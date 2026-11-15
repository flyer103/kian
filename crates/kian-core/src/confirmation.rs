//! The pending-confirmation projection (CONFIRMATION-MODEL §4, ADR-033).
//!
//! An `ask` is a `tool/call` whose permission decision is `evaluated:"ask"`
//! and which has no answering `tool/result`. The pending set is a **pure
//! projection of the stream** — no table, no inbox status column, no
//! in-memory wait — so a restart loses nothing and a crashed drain cannot
//! strand the decision.
//!
//! The provenance key is non-context (`derive_model_context` reads
//! `role`/`content` only), so the fold reads [`Event`]s, never the derived
//! messages. The core stream invariant already treats an open call as a
//! legal state (`invariants::durable_before_execute`: "a call with no
//! result yet is not a violation"), so an `ask` does not widen legality —
//! it names the reason a stream may rest in that shape.
//!
//! Effect ordering (CONFIRMATION-MODEL F3): answers may *arrive* in any
//! order, but the drain applies them in **call order** — [`confirmable`]
//! yields the longest call-ordered prefix whose decisions all exist, so a
//! later decision never lands before an earlier sibling's result.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::control::{ConfirmDecision, ConfirmIntent};
use crate::events::{Event, EventKind};

/// One `tool/call` awaiting a human decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCall {
    /// The tool-call id (`tool/call.content.id`), the decision's unit.
    pub call_id: String,
    /// The tool name (`tool/call.content.name`).
    pub tool: String,
    /// The call arguments, verbatim from the frozen payload.
    pub arguments: Value,
    /// The `tool/call` event's seq.
    pub call_seq: u64,
    /// The `tool/call` event's `ts` — when the ask was raised.
    pub requested_at_ms: u64,
}

/// The pending set, in call order (CONFIRMATION-MODEL F1).
///
/// A `tool/call` is pending iff its provenance says `evaluated:"ask"` and
/// no `tool/result` answers its `call_id`. Malformed payloads are skipped
/// honestly (they cannot be an ask).
#[must_use]
pub fn pending_calls(events: &[Event]) -> Vec<PendingCall> {
    let mut calls: Vec<PendingCall> = Vec::new();
    let mut answered: BTreeSet<String> = BTreeSet::new();
    for ev in events {
        match &ev.kind {
            EventKind::ToolCall => {
                if let Some(call) = ask_call(ev) {
                    calls.push(call);
                }
            }
            EventKind::ToolResult => {
                if let Some(id) = ev.bytes.pointer("/content/call_id").and_then(Value::as_str) {
                    answered.insert(id.to_owned());
                }
            }
            _ => {}
        }
    }
    calls.retain(|c| !answered.contains(&c.call_id));
    calls
}

/// The `tool/call` payload of an ask, or `None`.
fn ask_call(ev: &Event) -> Option<PendingCall> {
    let evaluated = ev
        .bytes
        .pointer("/permission/evaluated")
        .and_then(Value::as_str)?;
    if evaluated != "ask" {
        return None;
    }
    let call_id = ev
        .bytes
        .pointer("/content/id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?
        .to_owned();
    let tool = ev
        .bytes
        .pointer("/content/name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?
        .to_owned();
    let arguments = ev
        .bytes
        .pointer("/content/arguments")
        .cloned()
        .unwrap_or(Value::Null);
    Some(PendingCall {
        call_id,
        tool,
        arguments,
        call_seq: ev.seq,
        requested_at_ms: ev.ts,
    })
}

/// The durable decisions on the stream, keyed by `call_id`
/// (`control/confirm` events; a later decision for the same call wins
/// only in malformed streams — the engine refuses a second answer).
#[must_use]
pub fn confirmations(events: &[Event]) -> BTreeMap<String, ConfirmIntent> {
    let mut out = BTreeMap::new();
    for ev in events {
        if ev.kind != EventKind::ControlConfirm {
            continue;
        }
        if let Some(intent) = ConfirmIntent::from_event_payload(&ev.bytes) {
            // First durable decision wins: the engine admits at most one
            // row per (session, call_id) and the audit is append-only, so
            // a second event is a malformed/duplicate stream — never a
            // silent override of the recorded answer.
            out.entry(intent.call_id.clone()).or_insert(intent);
        }
    }
    out
}

/// The longest call-ordered prefix of the pending set whose decisions are
/// all durable (CONFIRMATION-MODEL F3): `(call, decision)` pairs the drain
/// may apply now, in order. An earlier undecided call blocks later ones.
#[must_use]
pub fn confirmable(
    pending: &[PendingCall],
    decisions: &BTreeMap<String, ConfirmIntent>,
) -> Vec<(PendingCall, ConfirmIntent)> {
    let mut out = Vec::new();
    for call in pending {
        match decisions.get(&call.call_id) {
            Some(decision) => out.push((call.clone(), decision.clone())),
            None => break,
        }
    }
    out
}

/// Whether a pending call has lapsed under the configured TTL. `ttl_ms == 0`
/// means no expiry; `expired` is a read-time overlay (like `recoverable`),
/// never a stored state.
#[must_use]
pub fn is_expired(call: &PendingCall, ttl_ms: u64, now_ms: u64) -> bool {
    ttl_ms > 0 && now_ms.saturating_sub(call.requested_at_ms) > ttl_ms
}

/// The effective decision for an answer arriving now: a lapsed call is
/// transformed into a durable reject (`expired: true`) so the session
/// resumes and the model sees the honest error (CONFIRMATION-MODEL A6).
#[must_use]
pub fn effective_decision(
    call: &PendingCall,
    ttl_ms: u64,
    now_ms: u64,
    intent: &ConfirmIntent,
) -> ConfirmIntent {
    if is_expired(call, ttl_ms, now_ms) {
        ConfirmIntent {
            decision: ConfirmDecision::Reject,
            expired: true,
            ..intent.clone()
        }
    } else {
        intent.clone()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{confirmable, confirmations, effective_decision, is_expired, pending_calls};
    use crate::control::{ConfirmDecision, ConfirmIntent};
    use crate::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use crate::model::{Epoch, SessionId};

    fn ev(seq: u64, ts: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(1),
            ts,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    fn ask(seq: u64, ts: u64, id: &str, tool: &str, args: serde_json::Value) -> Event {
        ev(
            seq,
            ts,
            EventKind::ToolCall,
            json!({
                "role": "assistant",
                "content": {"id": id, "name": tool, "arguments": args},
                "permission": {"evaluated": "ask", "policy": "configured", "matched": tool}
            }),
        )
    }

    fn allow(seq: u64, ts: u64, id: &str) -> Event {
        ev(
            seq,
            ts,
            EventKind::ToolCall,
            json!({
                "role": "assistant",
                "content": {"id": id, "name": "read", "arguments": {}},
                "permission": {"evaluated": "allow", "policy": "configured", "matched": "read"}
            }),
        )
    }

    fn result(seq: u64, ts: u64, id: &str) -> Event {
        ev(
            seq,
            ts,
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": id, "output": {}, "is_error": false}}),
        )
    }

    fn decision(seq: u64, id: &str, d: ConfirmDecision) -> Event {
        let intent = ConfirmIntent {
            call_id: id.into(),
            decision: d,
            reason: None,
            requested_by: Some("alice".into()),
            answered_at_ms: 50,
            expired: false,
        };
        ev(seq, 50, EventKind::ControlConfirm, intent.event_payload())
    }

    #[test]
    fn pending_is_call_ordered_and_excludes_answered_and_allowed_calls() {
        let events = vec![
            ask(1, 10, "a", "send_email", json!({"to": "x"})),
            allow(2, 11, "b"),
            result(3, 12, "b"),
            ask(4, 13, "c", "delete", json!({})),
        ];
        let pending = pending_calls(&events);
        assert_eq!(
            pending
                .iter()
                .map(|p| p.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
        assert_eq!(pending[0].tool, "send_email");
        assert_eq!(pending[0].arguments, json!({"to": "x"}));
        assert_eq!(pending[0].requested_at_ms, 10);
        assert_eq!(pending[0].call_seq, 1);
    }

    #[test]
    fn an_unanswered_call_is_legal_and_stays_pending_across_an_end_write() {
        let events = vec![
            ask(1, 10, "a", "send_email", json!({})),
            ev(
                2,
                11,
                EventKind::SessionEnded {
                    reason: EndReason::AwaitingInput,
                },
                json!({}),
            ),
        ];
        assert_eq!(pending_calls(&events).len(), 1);
    }

    #[test]
    fn a_result_answers_exactly_its_own_call() {
        let events = vec![
            ask(1, 10, "a", "t", json!({})),
            ask(2, 11, "b", "t", json!({})),
            result(3, 12, "a"),
        ];
        let pending = pending_calls(&events);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].call_id, "b");
    }

    #[test]
    fn malformed_payloads_are_not_asks() {
        let events = vec![
            ev(1, 10, EventKind::ToolCall, json!({"role": "assistant"})),
            ev(
                2,
                11,
                EventKind::ToolCall,
                json!({"role": "assistant", "content": {"id": "x", "name": "t"},
                       "permission": {"evaluated": "maybe"}}),
            ),
            ev(
                3,
                12,
                EventKind::ToolCall,
                json!({"role": "assistant", "content": {"id": "", "name": "t"},
                       "permission": {"evaluated": "ask"}}),
            ),
        ];
        assert!(pending_calls(&events).is_empty());
    }

    #[test]
    fn confirmable_is_the_call_ordered_prefix() {
        let events = vec![
            ask(1, 10, "a", "t", json!({})),
            ask(2, 11, "b", "t", json!({})),
            decision(3, "b", ConfirmDecision::Approve),
        ];
        let pending = pending_calls(&events);
        let decisions = confirmations(&events);
        // b is decided but a is not: b's effect must wait (call order).
        assert!(confirmable(&pending, &decisions).is_empty());

        let events2 = vec![
            ask(1, 10, "a", "t", json!({})),
            decision(2, "a", ConfirmDecision::Reject),
        ];
        let pending2 = pending_calls(&events2);
        let decisions2 = confirmations(&events2);
        let ready = confirmable(&pending2, &decisions2);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0.call_id, "a");
        assert_eq!(ready[0].1.decision, ConfirmDecision::Reject);
    }

    #[test]
    fn ttl_overlay_and_effective_decision() {
        let call = pending_calls(&[ask(1, 10_000, "a", "t", json!({}))])
            .into_iter()
            .next()
            .unwrap();
        assert!(!is_expired(&call, 0, 99_999));
        assert!(!is_expired(&call, 5_000, 14_999));
        assert!(is_expired(&call, 5_000, 15_001));

        let approve = ConfirmIntent {
            call_id: "a".into(),
            decision: ConfirmDecision::Approve,
            reason: None,
            requested_by: None,
            answered_at_ms: 0,
            expired: false,
        };
        let applied = effective_decision(&call, 5_000, 20_000, &approve);
        assert_eq!(applied.decision, ConfirmDecision::Reject);
        assert!(applied.expired);
        // No TTL: the answer is applied as-is.
        let kept = effective_decision(&call, 0, 20_000, &approve);
        assert_eq!(kept.decision, ConfirmDecision::Approve);
        assert!(!kept.expired);
    }
}
