//! The reserved control-envelope convention (C68, ADR-014).
//!
//! Interrupt intent is *durable*: the control plane admits a reserved
//! inbox row instead of only flipping an in-memory flag, so the intent
//! survives a crash of the draining replica and crosses replicas as
//! data. The row's payload is exactly `{"kian": {...}}` — a row-level
//! convention like the steer wrapper and the C32 spill envelope before
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
//! the last event in the stream at admit time, i.e. the episode the
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
    /// `{"kian": {"type": "control", "action": "interrupt", ...}}` key).
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::InterruptIntent;
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
}
