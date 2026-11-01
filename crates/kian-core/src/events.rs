//! Event classification and envelope (DESIGN §3.1 "Event classification
//! (borrowing dsh's durable/live split)" and §3.3 "Schema versioning").
//!
//! The append-only `Event` envelope carries a schema `version` (v0, DESIGN
//! §3.3) and the durable `EventKind` set. Provider payloads stay opaque
//! `bytes` — no provider-specific field names inside `EventKind`. After C2
//! merges, event kinds are additions-only (constraint 3, cache-byte
//! stability).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{Epoch, SessionId};

/// Current event-schema version (DESIGN §3.3). v0 is frozen by C2; later
/// versions arrive exclusively as adjacent migrations — committed
/// generations are never renamed, replaced or deleted.
pub const SCHEMA_VERSION: u32 = 0;

/// The append-only event envelope — the sole truth (constraint 1). Events
/// are never modified; every projection is derived from a stream of these.
///
/// The exact serialized JSON form is frozen (golden-tested): field order and
/// byte shape are stable content, because envelopes feed replay and audit
/// exports (C14).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Monotone per-session sequence number (CAS fence input, ADR-005).
    pub seq: u64,
    pub session: SessionId,
    /// Fencing epoch of the drain that appended this event.
    pub epoch: Epoch,
    /// Appended at, unix epoch milliseconds.
    pub ts: u64,
    /// Flattened into the envelope: the JSON carries `"kind": "<name>"`
    /// (plus payload fields such as `reason` for `session/ended`) at the
    /// envelope's top level.
    #[serde(flatten)]
    pub kind: EventKind,
    /// Opaque payload. `EventKind` names the semantics, `bytes` carries
    /// them — no provider-specific schema is allowed inside the envelope
    /// (narrow waist; constraint 3). Serialized as a JSON value:
    /// deterministic re-serialization keeps replay byte-identical.
    pub bytes: Value,
    /// Envelope schema version. Defaults to 0 (v0) when absent (§3.3).
    #[serde(default)]
    pub version: u32,
}

impl Event {
    /// The current schema version, for constructors that want to be
    /// explicit.
    pub fn current_version() -> u32 {
        SCHEMA_VERSION
    }
}

/// Why a session ended. Honesty over completion rate (constraint 7):
/// `Interrupted` is deliberately distinct from `Failed` — an abort or crash
/// must never be mapped onto `Completed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndReason {
    Completed,
    Cancelled,
    Interrupted,
    Failed,
}

/// The durable event set (DESIGN §3.1). Live events (streaming chunks,
/// heartbeats, progress) are *not* here — they are never persisted.
///
/// Only `SessionEnded` carries structured data (`EndReason`) because the
/// terminal state transition must be projectable without parsing opaque
/// bytes (NoHalfTerminated, constraint 4). Every other kind's payload lives
/// in [`Event::bytes`] under a payload schema owned by the emitting
/// subsystem (e.g. `compact`'s `[a,b)` range + summary: C4).
///
/// **Additions-only after C2** (constraint 3): no renames, no reformats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EventKind {
    /// Session created (`idle`).
    #[serde(rename = "session/created")]
    SessionCreated,

    /// An input row became durable (admit-before-wake, invariant 1).
    #[serde(rename = "input/admitted")]
    InputAdmitted,

    /// A drain claimed the session (epoch issued, lease started).
    #[serde(rename = "drain/claimed")]
    DrainClaimed,

    /// A provider turn began.
    #[serde(rename = "turn/start")]
    TurnStart,

    /// A provider turn ended.
    #[serde(rename = "turn/end")]
    TurnEnd,

    /// Exact request bytes persisted *before* the request flew (A2).
    #[serde(rename = "step/request")]
    StepRequest,

    /// Assistant message — the sent bytes are the truth (A2/A3).
    #[serde(rename = "assistant/message")]
    AssistantMessage,

    /// Tool invocation requested (logged before execution, invariant 1).
    #[serde(rename = "tool/call")]
    ToolCall,

    /// Tool invocation result.
    #[serde(rename = "tool/result")]
    ToolResult,

    /// Compression — the only deliberate prefix-invalidation point (A3).
    /// Carries the pre-compression range `[a,b)` and the post-compression
    /// summary in its payload (schema owned by C4).
    #[serde(rename = "compact")]
    Compact,

    /// Slot → provider/model binding fixed at claim (ADR-006).
    #[serde(rename = "slot/binding")]
    SlotBinding,

    /// One fallback-chain switch within a bound slot (C8).
    #[serde(rename = "slot/fallback")]
    SlotFallback,

    /// Explicit slot migration — the only escape hatch from an immutable
    /// binding (ADR-006).
    #[serde(rename = "slot/migration")]
    SlotMigration,

    /// Operator reset of a session's recovery budget (card C14). The
    /// reset is itself an appended event — there is no silent counter
    /// wipe (constraint 1: every observable change is an append). The
    /// payload (schema owned by C14: `{"reason": string}`) records why.
    /// Neutral by design: the state machine maps it to no transition
    /// and the model-context projection skips it, so replay derives
    /// fresh budgets from config while the event remains the audit
    /// record that a reset happened.
    #[serde(rename = "recovery/budget-reset")]
    RecoveryBudgetReset,

    /// Terminal end write: session end + state transition in one atomic
    /// append (NoHalfTerminated, constraint 4).
    #[serde(rename = "session/ended")]
    SessionEnded { reason: EndReason },
}

impl EventKind {
    /// The durable kind name (e.g. `"session/created"`). Golden-tested to
    /// stay in sync with the serde tag.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SessionCreated => "session/created",
            Self::InputAdmitted => "input/admitted",
            Self::DrainClaimed => "drain/claimed",
            Self::TurnStart => "turn/start",
            Self::TurnEnd => "turn/end",
            Self::StepRequest => "step/request",
            Self::AssistantMessage => "assistant/message",
            Self::ToolCall => "tool/call",
            Self::ToolResult => "tool/result",
            Self::Compact => "compact",
            Self::SlotBinding => "slot/binding",
            Self::SlotFallback => "slot/fallback",
            Self::SlotMigration => "slot/migration",
            Self::RecoveryBudgetReset => "recovery/budget-reset",
            Self::SessionEnded { .. } => "session/ended",
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use crate::model::{Epoch, SessionId};

    fn sample_event(kind: EventKind) -> Event {
        Event {
            seq: 7,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(3),
            ts: 1_761_900_000_000,
            kind,
            bytes: json!({"opaque": true}),
            version: SCHEMA_VERSION,
        }
    }

    fn tag_of(golden: &str) -> String {
        serde_json::from_str::<serde_json::Value>(golden)
            .unwrap()
            .get("kind")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap()
    }

    /// Every EventKind has an exact golden JSON form and round-trips (C2
    /// acceptance). The `as_str` name must stay in sync with the serde tag.
    /// After C2 merges this set is additions-only.
    #[test]
    fn event_kind_golden_json_round_trip() {
        let cases = [
            (EventKind::SessionCreated, r#"{"kind":"session/created"}"#),
            (EventKind::InputAdmitted, r#"{"kind":"input/admitted"}"#),
            (EventKind::DrainClaimed, r#"{"kind":"drain/claimed"}"#),
            (EventKind::TurnStart, r#"{"kind":"turn/start"}"#),
            (EventKind::TurnEnd, r#"{"kind":"turn/end"}"#),
            (EventKind::StepRequest, r#"{"kind":"step/request"}"#),
            (
                EventKind::AssistantMessage,
                r#"{"kind":"assistant/message"}"#,
            ),
            (EventKind::ToolCall, r#"{"kind":"tool/call"}"#),
            (EventKind::ToolResult, r#"{"kind":"tool/result"}"#),
            (EventKind::Compact, r#"{"kind":"compact"}"#),
            (EventKind::SlotBinding, r#"{"kind":"slot/binding"}"#),
            (EventKind::SlotFallback, r#"{"kind":"slot/fallback"}"#),
            (EventKind::SlotMigration, r#"{"kind":"slot/migration"}"#),
            (
                EventKind::RecoveryBudgetReset,
                r#"{"kind":"recovery/budget-reset"}"#,
            ),
            (
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                r#"{"kind":"session/ended","reason":"completed"}"#,
            ),
        ];
        for (kind, golden) in &cases {
            assert_eq!(&serde_json::to_string(kind).unwrap(), golden);
            let back: EventKind = serde_json::from_str(golden).unwrap();
            assert_eq!(&back, kind);
            assert_eq!(kind.as_str(), tag_of(golden));
        }
        // The durable set is exactly the fifteen kinds above (the
        // fourteen frozen by C2 + the additive C14
        // `recovery/budget-reset`; additions-only, no renames).
        assert_eq!(cases.len(), 15);
    }

    /// `session/ended` carries every `EndReason`; `interrupted` is distinct
    /// from `failed` (honesty, constraint 7).
    #[test]
    fn session_ended_round_trips_every_end_reason() {
        for (reason, golden_reason) in [
            (EndReason::Completed, "completed"),
            (EndReason::Cancelled, "cancelled"),
            (EndReason::Interrupted, "interrupted"),
            (EndReason::Failed, "failed"),
        ] {
            let kind = EventKind::SessionEnded { reason };
            let ser = serde_json::to_string(&kind).unwrap();
            assert_eq!(
                ser,
                format!(r#"{{"kind":"session/ended","reason":"{golden_reason}"}}"#)
            );
            let back: EventKind = serde_json::from_str(&ser).unwrap();
            assert_eq!(back, kind);
        }
    }

    /// The v0 envelope has an exact, frozen JSON shape — field order
    /// included (cache-byte discipline starts at the envelope).
    #[test]
    fn event_envelope_golden_json_and_round_trip() {
        let ev = sample_event(EventKind::TurnStart);
        let golden = concat!(
            r#"{"seq":7,"session":"00000000-0000-0000-0000-000000000000","#,
            r#""epoch":3,"ts":1761900000000,"kind":"turn/start","#,
            r#""bytes":{"opaque":true},"version":0}"#
        );
        assert_eq!(serde_json::to_string(&ev).unwrap(), golden);
        let back: Event = serde_json::from_str(golden).unwrap();
        assert_eq!(back, ev);
    }

    /// `session/ended` flattens its `EndReason` into the envelope and the
    /// envelope still round-trips.
    #[test]
    fn event_envelope_session_ended_golden_json() {
        let ev = sample_event(EventKind::SessionEnded {
            reason: EndReason::Interrupted,
        });
        let golden = concat!(
            r#"{"seq":7,"session":"00000000-0000-0000-0000-000000000000","#,
            r#""epoch":3,"ts":1761900000000,"kind":"session/ended","#,
            r#""reason":"interrupted","bytes":{"opaque":true},"version":0}"#
        );
        assert_eq!(serde_json::to_string(&ev).unwrap(), golden);
        let back: Event = serde_json::from_str(golden).unwrap();
        assert_eq!(back, ev);
    }

    /// Envelope `version` defaults to 0 (v0) when absent (DESIGN §3.3).
    #[test]
    fn envelope_version_defaults_to_zero_when_absent() {
        let ev = sample_event(EventKind::Compact);
        let mut v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v.get("version"), Some(&json!(SCHEMA_VERSION)));
        assert_eq!(SCHEMA_VERSION, 0);
        v.as_object_mut().unwrap().remove("version");
        let back: Event = serde_json::from_value(v).unwrap();
        assert_eq!(back.version, 0);
        assert_eq!(back, ev);
    }
}
