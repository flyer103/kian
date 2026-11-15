//! The reserved control-envelope convention (ADR-014).
//!
//! Interrupt intent is *durable*: the control plane admits a reserved
//! inbox row instead of only flipping an in-memory flag, so the intent
//! survives a crash of the draining replica and crosses replicas as
//! data. The row's payload is exactly `{"kian": {...}}` — a row-level
//! convention like the steer wrapper and the spill envelope before
//! it: a plain payload of that precise single-key shape is read as an
//! envelope, never as message content.
//!
//! The layering is untouched (constraint 9): the control plane still
//! only writes the inbox — it never appends events. The owning drain
//! recognizes the envelope at consumption time, appends the
//! [`crate::events::EventKind::ControlInterrupt`] event (the audit
//! record), marks the row consumed, and honors the intent at its next
//! safe boundary. No wake is needed: a drain in flight consumes the row
//! at its next boundary take; a crashed owner's successor reaches it
//! through the takeover path (recovery sweep / startup inbox scan).
//!
//! Epoch targeting: the envelope carries `target_epoch` — the epoch of
//! the last event in the stream at admit time, i.e. The episode the
//! interrupt intends to stop (best-effort; `null` when unreadable).
//! Consumption compares it against the stream to decide staleness (see
//! ADR-014): a row whose target episode already *settled* (the stream
//! carries a `session/ended` at that epoch) is consumed for audit only
//! (`"stale": true`, ends nothing); a row for the current episode —
//! including one resumed by a takeover claim after the original owner
//! died mid-episode — always interrupts.

use serde_json::{Value, json};

use crate::model::SessionId;

/// The reserved envelope's single top-level key.
const ENVELOPE_KEY: &str = "kian";
/// The reserved control type discriminator.
const CONTROL_TYPE: &str = "control";
/// The interrupt action discriminator.
const INTERRUPT_ACTION: &str = "interrupt";
/// The confirmation action discriminator.
const CONFIRM_ACTION: &str = "confirm";

/// A parsed interrupt intent from a reserved control-envelope row.
///
/// Pure data: the same intent serializes to the envelope (row payload)
/// and to the [`crate::events::EventKind::ControlInterrupt`] event
/// payload, so the audit event reproduces exactly what was requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterruptIntent {
    /// Free-form operator reason, or `null`.
    pub reason: Option<String>,
    /// Who requested the interrupt (holder/replica identity), or `null`.
    pub requested_by: Option<String>,
    /// Unix epoch milliseconds at admit time.
    pub requested_at_ms: u64,
    /// The episode (fencing epoch of the last event) the intent targets;
    /// `null` when the stream was unreadable at admit time.
    pub target_epoch: Option<u64>,
}

impl InterruptIntent {
    /// Whether `payload` is the reserved control envelope (a single
    /// `{"kian": {"type": "control", "action": "interrupt",...}}` key).
    /// Like the steer wrapper and the spill envelope, the exact shape is
    /// reserved: any other payload — including a `kian` object that is
    /// not an interrupt control — is plain content.
    #[must_use]
    pub fn is_control_envelope(payload: &Value) -> bool {
        Self::parse(payload).is_some()
    }

    /// Parses the reserved control envelope, or `None` for any payload
    /// that is not exactly it. Fields the writer omitted default
    /// honestly (`reason`/`requested_by`/`requested_by` → null,
    /// `requested_at_ms` → 0) so cross-replica rows with older writers
    /// still consume.
    #[must_use]
    pub fn parse(payload: &Value) -> Option<Self> {
        let obj = payload.as_object()?;
        if obj.len() != 1 {
            return None;
        }
        let inner = obj.get(ENVELOPE_KEY)?.as_object()?;
        if inner.get("type").and_then(Value::as_str) != Some(CONTROL_TYPE)
            || inner.get("action").and_then(Value::as_str) != Some(INTERRUPT_ACTION)
        {
            return None;
        }
        Some(Self {
            reason: inner
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            requested_by: inner
                .get("requested_by")
                .and_then(Value::as_str)
                .map(str::to_owned),
            requested_at_ms: inner
                .get("requested_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            target_epoch: inner.get("target_epoch").and_then(Value::as_u64),
        })
    }

    /// The reserved envelope payload for this intent (the inbox row's
    /// `payload`).
    #[must_use]
    pub fn envelope(&self) -> Value {
        let mut inner = json!({
            "type": CONTROL_TYPE,
            "action": INTERRUPT_ACTION,
            "requested_at_ms": self.requested_at_ms,
        });
        let map = inner.as_object_mut().expect("literal object");
        match &self.reason {
            Some(r) => map.insert("reason".into(), json!(r)),
            None => map.insert("reason".into(), Value::Null),
        };
        match &self.requested_by {
            Some(r) => map.insert("requested_by".into(), json!(r)),
            None => map.insert("requested_by".into(), Value::Null),
        };
        map.insert(
            "target_epoch".into(),
            self.target_epoch.map_or(Value::Null, |e| json!(e)),
        );
        json!({ ENVELOPE_KEY: inner })
    }

    /// The `control/interrupt` event payload for this intent. `stale`
    /// rides only when set (additive key — legacy payloads never carry
    /// it, and a live intent never does).
    #[must_use]
    pub fn event_payload(&self, stale: bool) -> Value {
        let mut payload = json!({
            "action": INTERRUPT_ACTION,
            "requested_at_ms": self.requested_at_ms,
        });
        let map = payload.as_object_mut().expect("literal object");
        match &self.reason {
            Some(r) => map.insert("reason".into(), json!(r)),
            None => map.insert("reason".into(), Value::Null),
        };
        match &self.requested_by {
            Some(r) => map.insert("requested_by".into(), json!(r)),
            None => map.insert("requested_by".into(), Value::Null),
        };
        map.insert(
            "target_epoch".into(),
            self.target_epoch.map_or(Value::Null, |e| json!(e)),
        );
        if stale {
            map.insert("stale".into(), json!(true));
        }
        payload
    }

    /// The inbox idempotency key: repeated interrupts against the same
    /// episode (`target_epoch`) dedupe to exactly one row.
    #[must_use]
    pub fn idempotency_key(session: &SessionId, target_epoch: Option<u64>) -> String {
        match target_epoch {
            Some(e) => format!("interrupt:{}:{}", session.0, e),
            None => format!("interrupt:{}:null", session.0),
        }
    }
}

/// Whether `payload` is **any** reserved control envelope shape
/// (`{"kian": {"type": "control", ...}}`, any action). The public wire
/// refuses it: clients may never mint a control row, even one carrying
/// an action this build does not know.
#[must_use]
pub fn is_reserved_control_shape(payload: &Value) -> bool {
    let Some(obj) = payload.as_object() else {
        return false;
    };
    if obj.len() != 1 {
        return false;
    }
    let Some(inner) = obj.get(ENVELOPE_KEY).and_then(Value::as_object) else {
        return false;
    };
    inner.get("type").and_then(Value::as_str) == Some(CONTROL_TYPE)
}

/// A parsed confirmation intent from a reserved control-envelope row
/// (CONFIRMATION-MODEL §5, ADR-033): one human decision on one pending
/// ask. Pure data — the same intent serializes to the inbox row
/// (envelope), to the `control/confirm` audit event (event payload), and
/// to the `tool/result` provenance that records the applied decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmIntent {
    /// The pending call this decision answers (`tool/call.content.id`).
    pub call_id: String,
    /// The decision.
    pub decision: ConfirmDecision,
    /// Free-form human reason, or `null`.
    pub reason: Option<String>,
    /// Who answered (operator label), or `null`.
    pub requested_by: Option<String>,
    /// Unix epoch milliseconds at admit time (server-stamped).
    pub answered_at_ms: u64,
    /// The pending call had lapsed under `confirmation.ttl_ms`; the
    /// server transformed the answer into a durable reject so the
    /// session resumes honestly.
    pub expired: bool,
}

/// The closed decision vocabulary of the confirmation plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmDecision {
    Approve,
    Reject,
}

impl ConfirmDecision {
    /// The wire/event spelling (`approve` | `reject`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
        }
    }

    /// Parses the closed vocabulary; anything else is `None`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "approve" => Some(Self::Approve),
            "reject" => Some(Self::Reject),
            _ => None,
        }
    }
}

impl ConfirmIntent {
    /// Whether `payload` is the reserved confirmation envelope (a single
    /// `{"kian": {"type": "control", "action": "confirm", ...}}` key).
    #[must_use]
    pub fn is_control_envelope(payload: &Value) -> bool {
        Self::parse(payload).is_some()
    }

    /// Whether `payload` is **any** reserved control envelope shape
    /// (`{"kian": {"type": "control", ...}}`, any action). Used by the
    /// public wire guard: clients may never mint a control row, even one
    /// carrying an action this build does not know.
    #[must_use]
    pub fn is_reserved_shape(payload: &Value) -> bool {
        is_reserved_control_shape(payload)
    }

    /// Parses the reserved confirmation envelope, or `None` for any
    /// payload that is not exactly it. A missing/empty `call_id` or an
    /// unknown decision is not an envelope (it stays plain content).
    #[must_use]
    pub fn parse(payload: &Value) -> Option<Self> {
        let obj = payload.as_object()?;
        if obj.len() != 1 {
            return None;
        }
        let inner = obj.get(ENVELOPE_KEY)?.as_object()?;
        if inner.get("type").and_then(Value::as_str) != Some(CONTROL_TYPE)
            || inner.get("action").and_then(Value::as_str) != Some(CONFIRM_ACTION)
        {
            return None;
        }
        let call_id = inner
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?
            .to_owned();
        let decision = inner
            .get("decision")
            .and_then(Value::as_str)
            .and_then(ConfirmDecision::parse)?;
        Some(Self {
            call_id,
            decision,
            reason: inner
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            requested_by: inner
                .get("requested_by")
                .and_then(Value::as_str)
                .map(str::to_owned),
            answered_at_ms: inner
                .get("answered_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            expired: inner.get("expired").and_then(Value::as_bool) == Some(true),
        })
    }

    /// Parses the `control/confirm` **event** payload (the inner object,
    /// without the row envelope wrapper) — the audit record the drain
    /// appends on consumption.
    #[must_use]
    pub fn from_event_payload(payload: &Value) -> Option<Self> {
        let inner = payload.as_object()?;
        if inner.get("action").and_then(Value::as_str) != Some(CONFIRM_ACTION) {
            return None;
        }
        let call_id = inner
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?
            .to_owned();
        let decision = inner
            .get("decision")
            .and_then(Value::as_str)
            .and_then(ConfirmDecision::parse)?;
        Some(Self {
            call_id,
            decision,
            reason: inner
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            requested_by: inner
                .get("requested_by")
                .and_then(Value::as_str)
                .map(str::to_owned),
            answered_at_ms: inner
                .get("answered_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            expired: inner.get("expired").and_then(Value::as_bool) == Some(true),
        })
    }

    /// The reserved envelope payload for this intent (the inbox row's
    /// `payload`).
    #[must_use]
    pub fn envelope(&self) -> Value {
        let mut inner = json!({
            "type": CONTROL_TYPE,
            "action": CONFIRM_ACTION,
            "call_id": self.call_id,
            "decision": self.decision.as_str(),
            "reason": self.reason,
            "requested_by": self.requested_by,
            "answered_at_ms": self.answered_at_ms,
        });
        if self.expired {
            inner["expired"] = json!(true);
        }
        json!({ ENVELOPE_KEY: inner })
    }

    /// The `control/confirm` event payload for this intent.
    #[must_use]
    pub fn event_payload(&self) -> Value {
        let mut payload = json!({
            "action": CONFIRM_ACTION,
            "call_id": self.call_id,
            "decision": self.decision.as_str(),
            "reason": self.reason,
            "requested_by": self.requested_by,
            "answered_at_ms": self.answered_at_ms,
        });
        if self.expired {
            payload["expired"] = json!(true);
        }
        payload
    }

    /// The inbox idempotency key: repeated answers for the same call
    /// dedupe to exactly one row.
    #[must_use]
    pub fn idempotency_key(session: &SessionId, call_id: &str) -> String {
        format!("confirm:{}:{}", session.0, call_id)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{ConfirmDecision, ConfirmIntent, InterruptIntent};
    use crate::model::SessionId;

    fn intent() -> InterruptIntent {
        InterruptIntent {
            reason: Some("stop the deploy".into()),
            requested_by: Some("replica-a".into()),
            requested_at_ms: 1_762_000_000_000,
            target_epoch: Some(3),
        }
    }

    /// The envelope is exactly the reserved single-key shape and parses
    /// back to the same intent.
    #[test]
    fn envelope_round_trips_through_parse() {
        let i = intent();
        let envelope = i.envelope();
        assert_eq!(
            envelope,
            json!({
                "kian": {
                    "type": "control",
                    "action": "interrupt",
                    "reason": "stop the deploy",
                    "requested_by": "replica-a",
                    "requested_at_ms": 1_762_000_000_000_u64,
                    "target_epoch": 3
                }
            })
        );
        assert_eq!(InterruptIntent::parse(&envelope), Some(i));
        assert!(InterruptIntent::is_control_envelope(&envelope));
    }

    /// Null fields survive the round trip as nulls; a null target epoch
    /// (unreadable stream at admit) stays null.
    #[test]
    fn null_fields_round_trip() {
        let i = InterruptIntent {
            reason: None,
            requested_by: None,
            requested_at_ms: 7,
            target_epoch: None,
        };
        assert_eq!(InterruptIntent::parse(&i.envelope()), Some(i));
    }

    /// Omitted fields default honestly — a writer that sends only the
    /// discriminators still parses.
    #[test]
    fn minimal_envelope_parses_with_defaults() {
        let minimal = json!({"kian": {"type": "control", "action": "interrupt"}});
        assert_eq!(
            InterruptIntent::parse(&minimal),
            Some(InterruptIntent {
                reason: None,
                requested_by: None,
                requested_at_ms: 0,
                target_epoch: None,
            })
        );
    }

    /// Anything that is not exactly the reserved envelope is plain
    /// content: extra top-level keys, a non-control type, a non-interrupt
    /// action, a bare `kian` string, the steer wrapper, the spill
    /// envelope.
    #[test]
    fn only_the_exact_reserved_shape_is_an_envelope() {
        for payload in [
            json!({"kian": {"type": "control", "action": "interrupt"}, "extra": 1}),
            json!({"kian": {"type": "user", "action": "interrupt"}}),
            json!({"kian": {"type": "control", "action": "cancel"}}),
            json!({"kian": "not an object"}),
            json!({"synthetic": true, "content": "steer"}),
            json!({"content": "x", "spill": "y"}),
            json!("plain string"),
            json!({"role": "user", "content": "hi"}),
        ] {
            assert!(
                !InterruptIntent::is_control_envelope(&payload),
                "{payload} must be plain content"
            );
        }
    }

    /// The event payload carries the intent verbatim; `stale` rides only
    /// when set.
    #[test]
    fn event_payload_shape_and_stale_marker() {
        let live = intent().event_payload(false);
        assert_eq!(
            live,
            json!({
                "action": "interrupt",
                "reason": "stop the deploy",
                "requested_by": "replica-a",
                "requested_at_ms": 1_762_000_000_000_u64,
                "target_epoch": 3
            })
        );
        assert!(live.get("stale").is_none());
        let stale = intent().event_payload(true);
        assert_eq!(stale.get("stale"), Some(&json!(true)));
        // Everything else is unchanged by the marker.
        let mut without = stale.clone();
        without.as_object_mut().unwrap().remove("stale");
        assert_eq!(without, live);
    }

    /// The idempotency key is stable per (session, epoch) and spells a
    /// null target honestly.
    #[test]
    fn idempotency_key_is_deterministic() {
        let sid = SessionId(Uuid::nil());
        assert_eq!(
            InterruptIntent::idempotency_key(&sid, Some(3)),
            format!("interrupt:{}:3", sid.0)
        );
        assert_eq!(
            InterruptIntent::idempotency_key(&sid, None),
            format!("interrupt:{}:null", sid.0)
        );
    }

    /// The confirmation envelope round-trips; the reserved-shape guard
    /// also recognizes the interrupt shape and refuses nothing else.
    #[test]
    fn confirm_envelope_round_trips_and_reserved_shape_covers_both_actions() {
        let i = ConfirmIntent {
            call_id: "call-1".into(),
            decision: ConfirmDecision::Approve,
            reason: Some("looks right".into()),
            requested_by: Some("alice".into()),
            answered_at_ms: 1_762_000_000_000,
            expired: false,
        };
        let envelope = i.envelope();
        assert_eq!(
            envelope,
            json!({"kian": {"type": "control", "action": "confirm", "call_id": "call-1",
                            "decision": "approve", "reason": "looks right",
                            "requested_by": "alice", "answered_at_ms": 1_762_000_000_000_u64}})
        );
        assert_eq!(ConfirmIntent::parse(&envelope), Some(i));
        assert!(ConfirmIntent::is_control_envelope(&envelope));
        assert!(ConfirmIntent::is_reserved_shape(&envelope));

        let expired = ConfirmIntent {
            expired: true,
            decision: ConfirmDecision::Reject,
            reason: None,
            requested_by: None,
            answered_at_ms: 5,
            call_id: "call-2".into(),
        };
        assert_eq!(
            ConfirmIntent::parse(&expired.envelope()),
            Some(expired.clone())
        );
        assert_eq!(expired.event_payload()["expired"], json!(true));
        assert_eq!(expired.event_payload()["decision"], json!("reject"));

        // The interrupt envelope is a reserved control shape too (the
        // wire guard is action-agnostic), but not a confirmation.
        let interrupt = InterruptIntent {
            reason: None,
            requested_by: None,
            requested_at_ms: 1,
            target_epoch: None,
        }
        .envelope();
        assert!(ConfirmIntent::is_reserved_shape(&interrupt));
        assert!(!ConfirmIntent::is_control_envelope(&interrupt));

        for plain in [
            json!({"kian": {"type": "control", "action": "confirm"}}),
            json!({"kian": {"type": "control", "action": "confirm", "call_id": "c",
                            "decision": "maybe"}}),
            json!({"kian": {"type": "async_result", "job_id": "j"}}),
            json!({"kian": {"type": "control", "action": "confirm"}, "extra": 1}),
            json!({"kian": "string"}),
        ] {
            assert!(!ConfirmIntent::is_control_envelope(&plain), "{plain}");
        }
        assert!(!ConfirmIntent::is_reserved_shape(
            &json!({"kian": {"type": "async_result"}})
        ));
        assert!(!ConfirmIntent::is_reserved_shape(&json!("plain")));
    }

    /// The idempotency key is stable per (session, call).
    #[test]
    fn confirm_idempotency_key_is_deterministic() {
        let sid = SessionId(Uuid::nil());
        assert_eq!(
            ConfirmIntent::idempotency_key(&sid, "call-1"),
            format!("confirm:{}:call-1", sid.0)
        );
    }
}
