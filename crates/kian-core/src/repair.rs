//! Open tool-call recovery (CONFIRMATION-MODEL §15.6, DESIGN §2.3).
//!
//! A drain can die inside a tool batch: after the assistant message that
//! carries `tool_calls`, or after logging a `tool/call`, but before the
//! `tool/result`. The next claim must not send that context to a provider — a
//! tool block with a missing response is rejected by strict
//! OpenAI-compatible endpoints — and it must not silently re-run an effect.
//!
//! [`recoverable_calls`] names the calls a drain must close before its next
//! provider request: every assistant `tool_calls` entry that has no answer
//! and is **not** a pending ask. An `ask` with no durable decision is pending
//! (the confirmation plane waits for it); an `ask` with a durable decision is
//! left alone — its approval path re-invokes it (ADR-036 R4's documented
//! at-least-once class). Resolved calls are never touched.
//!
//! The recovered result is an honest error the model reacts to:
//!
//! - **`not_dispatched`** — no `tool/call` was logged, so the call never
//!   left the process. The repair logs the call (reconstructed from the
//!   assistant message, with an additive `recovered` marker and no
//!   fabricated permission provenance) and appends the error result.
//! - **`in_flight`** — the call was logged but never resolved, so its
//!   effect is unknown. Only the error result is appended.
//!
//! Both appends are ordinary events (no new kind): a reconstructed
//! `tool/call` is the same frozen payload shape as a real one plus the
//! additive non-context `recovered` key, and the `tool/result` is an
//! `is_error` result. A crash between the two appends converges: the next
//! claim sees the reconstructed call and only appends the result.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::events::{Event, EventKind};

/// Why a call needs recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// The assistant message carried the call, but no `tool/call` was
    /// logged: the process died before logging or dispatching it.
    NotDispatched,
    /// The `tool/call` was logged, but no `tool/result` answered it: the
    /// process died while the call was in flight.
    InFlight,
}

impl RecoveryPhase {
    /// The stable machine token written to the additive `recovered` key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotDispatched => "not_dispatched",
            Self::InFlight => "in_flight",
        }
    }

    /// The honest error the model sees.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::NotDispatched => {
                "recovery: the run ended before this call was dispatched; it did not execute"
            }
            Self::InFlight => {
                "recovery: the run ended while this call was in flight; the effect may or may \
                 not have landed"
            }
        }
    }
}

/// One call a drain must close before its next provider request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredCall {
    /// The `tool_calls[].id` carried by the assistant message.
    pub call_id: String,
    /// The tool name carried by the assistant message.
    pub tool: String,
    /// The arguments carried by the assistant message, verbatim.
    pub arguments: Value,
    /// Whether a `tool/call` for this id is already logged.
    pub logged: bool,
    /// Why it is open.
    pub phase: RecoveryPhase,
}

/// The calls the stream must close before the next provider request, in
/// call order.
///
/// Reads the durable stream only: the assistant message's `content.tool_calls`
/// is the authoritative call list (the `tool/call` event is a transcript
/// duplicate of it, per the wire dialect v2 contract), so a crash before the
/// log is still recoverable.
#[must_use]
pub fn recoverable_calls(events: &[Event]) -> Vec<RecoveredCall> {
    struct Logged {
        ask: bool,
        recovered: Option<RecoveryPhase>,
    }
    let mut logged: BTreeMap<String, Logged> = BTreeMap::new();
    let mut answered: BTreeSet<String> = BTreeSet::new();
    for ev in events {
        match ev.kind {
            EventKind::ToolCall => {
                let Some(id) = ev.bytes.pointer("/content/id").and_then(Value::as_str) else {
                    continue;
                };
                let evaluated = ev
                    .bytes
                    .pointer("/permission/evaluated")
                    .and_then(Value::as_str)
                    .unwrap_or("allow");
                // A call this module logged itself carries the phase
                // marker; a crash between its two appends must not
                // promote an un-dispatched call to "in flight".
                let recovered = match ev.bytes.get("recovered").and_then(Value::as_str) {
                    Some("not_dispatched") => Some(RecoveryPhase::NotDispatched),
                    Some("in_flight") => Some(RecoveryPhase::InFlight),
                    _ => None,
                };
                logged.insert(
                    id.to_owned(),
                    Logged {
                        ask: evaluated == "ask",
                        recovered,
                    },
                );
            }
            EventKind::ToolResult => {
                if let Some(id) = ev.bytes.pointer("/content/call_id").and_then(Value::as_str) {
                    answered.insert(id.to_owned());
                }
            }
            _ => {}
        }
    }

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for ev in events {
        if ev.kind != EventKind::AssistantMessage {
            continue;
        }
        let Some(calls) = ev
            .bytes
            .pointer("/content/tool_calls")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for call in calls {
            let Some(id) = call.get("id").and_then(Value::as_str) else {
                continue;
            };
            if !seen.insert(id.to_owned()) || answered.contains(id) {
                continue;
            }
            let Some(tool) = call.get("name").and_then(Value::as_str) else {
                continue;
            };
            let open = |phase, logged| RecoveredCall {
                call_id: id.to_owned(),
                tool: tool.to_owned(),
                arguments: call.get("arguments").cloned().unwrap_or(Value::Null),
                logged,
                phase,
            };
            match logged.get(id) {
                Some(rec) => match rec.recovered {
                    Some(phase) => out.push(open(phase, true)),
                    // A logged ask is never recovered: pending (no
                    // decision) waits for the person, and decided ones
                    // are re-driven by the approval path (CONFIRMATION-
                    // MODEL R4's at-least-once window). Synthesizing a
                    // result here would race that path.
                    None if rec.ask => continue,
                    None => out.push(open(RecoveryPhase::InFlight, true)),
                },
                None => out.push(open(RecoveryPhase::NotDispatched, false)),
            }
        }
    }
    out
}

/// The reconstructed `tool/call` payload: the frozen shape plus the
/// additive, non-context `recovered` marker. No `permission` key is
/// fabricated — no decision was recorded.
#[must_use]
pub fn recovered_tool_call_payload(call: &RecoveredCall) -> Value {
    json!({
        "role": "assistant",
        "content": {"id": call.call_id, "name": call.tool, "arguments": call.arguments},
        "recovered": call.phase.as_str(),
    })
}

/// The synthetic `tool/result` closing an open call: an honest error the
/// model sees, with the additive `recovered` marker outside `content`.
#[must_use]
pub fn recovered_tool_result_payload(call: &RecoveredCall) -> Value {
    json!({
        "role": "tool",
        "content": {
            "call_id": call.call_id,
            "output": {"error": call.phase.message()},
            "is_error": true,
        },
        "recovered": call.phase.as_str(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::SCHEMA_VERSION;
    use crate::model::{Epoch, SessionId};

    fn ev(seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId::new(),
            epoch: Epoch::GENESIS,
            ts: 1_800_000_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    fn assistant_with_calls(seq: u64, calls: Value) -> Event {
        ev(
            seq,
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": "", "tool_calls": calls}}),
        )
    }

    fn call(id: &str, name: &str) -> Value {
        json!({"id": id, "name": name, "arguments": {"argv": ["echo", id]}})
    }

    fn logged_call(seq: u64, id: &str, name: &str, evaluated: &str) -> Event {
        let mut payload = json!({
            "role": "assistant",
            "content": {"id": id, "name": name, "arguments": {"argv": ["echo", id]}}
        });
        if evaluated != "allow" {
            payload["permission"] = json!({"evaluated": evaluated, "policy": "configured",
                                           "matched": name});
        }
        ev(seq, EventKind::ToolCall, payload)
    }

    fn result(seq: u64, id: &str) -> Event {
        ev(
            seq,
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": id, "output": {"ok": true},
                                               "is_error": false}}),
        )
    }

    #[test]
    fn a_resolved_call_is_never_recovered() {
        let events = vec![
            assistant_with_calls(2, json!([call("a", "exec")])),
            logged_call(3, "a", "exec", "allow"),
            result(4, "a"),
        ];
        assert!(recoverable_calls(&events).is_empty());
    }

    #[test]
    fn a_crash_after_the_log_recovers_in_flight() {
        let events = vec![
            assistant_with_calls(2, json!([call("a", "exec")])),
            logged_call(3, "a", "exec", "allow"),
        ];
        let open = recoverable_calls(&events);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].call_id, "a");
        assert!(open[0].logged);
        assert_eq!(open[0].phase, RecoveryPhase::InFlight);
    }

    #[test]
    fn a_crash_before_the_log_recovers_not_dispatched() {
        let events = vec![assistant_with_calls(2, json!([call("a", "exec")]))];
        let open = recoverable_calls(&events);
        assert_eq!(open.len(), 1);
        assert!(!open[0].logged);
        assert_eq!(open[0].phase, RecoveryPhase::NotDispatched);
    }

    #[test]
    fn a_pending_ask_is_not_recovered() {
        let events = vec![
            assistant_with_calls(2, json!([call("a", "exec")])),
            logged_call(3, "a", "exec", "ask"),
        ];
        assert!(recoverable_calls(&events).is_empty());
    }

    #[test]
    fn a_decided_ask_without_a_result_is_left_to_the_approval_path() {
        let confirm = ev(
            4,
            EventKind::ControlConfirm,
            json!({"action": "confirm", "call_id": "a", "decision": "approve"}),
        );
        let events = vec![
            assistant_with_calls(2, json!([call("a", "exec")])),
            logged_call(3, "a", "exec", "ask"),
            confirm,
        ];
        // The approval path re-invokes (ADR-036 R4); recovery must not
        // race it with a synthetic result.
        assert!(recoverable_calls(&events).is_empty());
    }

    #[test]
    fn a_crash_between_the_reconstruction_appends_keeps_the_phase() {
        let events = vec![
            assistant_with_calls(2, json!([call("a", "exec")])),
            ev(
                3,
                EventKind::ToolCall,
                json!({"role": "assistant",
                       "content": {"id": "a", "name": "exec", "arguments": {"argv": ["echo", "a"]}},
                       "recovered": "not_dispatched"}),
            ),
        ];
        let open = recoverable_calls(&events);
        assert_eq!(open.len(), 1);
        assert!(open[0].logged);
        assert_eq!(open[0].phase, RecoveryPhase::NotDispatched);
    }

    #[test]
    fn recovered_payloads_keep_the_frozen_message_shapes() {
        let open = RecoveredCall {
            call_id: "a".into(),
            tool: "exec".into(),
            arguments: json!({"argv": ["echo", "a"]}),
            logged: false,
            phase: RecoveryPhase::NotDispatched,
        };
        let call = recovered_tool_call_payload(&open);
        assert_eq!(call["role"], "assistant");
        assert_eq!(call["content"]["id"], "a");
        assert_eq!(call["content"]["name"], "exec");
        assert_eq!(call["content"]["arguments"], json!({"argv": ["echo", "a"]}));
        assert_eq!(call["recovered"], "not_dispatched");
        assert!(
            call.get("permission").is_none(),
            "no decision was recorded, so none is fabricated"
        );

        let result = recovered_tool_result_payload(&open);
        assert_eq!(result["role"], "tool");
        assert_eq!(result["content"]["call_id"], "a");
        assert_eq!(result["content"]["is_error"], true);
        assert!(
            result["content"]["output"]["error"]
                .as_str()
                .unwrap_or_default()
                .contains("did not execute")
        );
        assert_eq!(result["recovered"], "not_dispatched");
    }
}
