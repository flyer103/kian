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

    /// Thread lineage start (C18, Phase 2): a Router's drain dispatched
    /// work to a worker session, or the worker's stream records that it
    /// was spawned by delegation. Payload schema (owned by C18):
    /// `{"thread_id": uuid, "parent": session-uuid, "worker":
    /// session-uuid, "pinned_version": u64, "task": <message payload>}`.
    /// State-preserving in `idle`/`running`/`recoverable` (same row as
    /// `input/admitted`); not context-bearing.
    #[serde(rename = "thread/started")]
    ThreadStarted,

    /// Thread lineage completion (C18, Phase 2): the worker's own drain
    /// appended its outcome before the end write. Payload schema (owned
    /// by C18): `{"thread_id": uuid, "status": "completed"|"failed"|
    /// "interrupted", "summary": <message payload>}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "thread/completed")]
    ThreadCompleted,

    /// Evidence-gate verdict (C18, Phase 2): the bounded, declarative
    /// stop-gate recorded its decision. Payload schema (owned by C18):
    /// `{"gate": str, "passed": bool, "checks": [{"name": str,
    /// "passed": bool, "detail"?: str}], "attempt": u64}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "evidence/verdict")]
    EvidenceVerdict,

    /// Channel delivery receipt (C18, Phase 2): a channel output policy
    /// delivered a message and recorded the honest receipt. Payload
    /// schema (owned by C18): `{"channel": str, "target": <value>,
    /// "policy": str, "words"?: u64, "truncated": bool}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "channel/delivered")]
    ChannelDelivered,

    /// Spilled raw channel payload (C31, Phase 3): the verbatim channel
    /// payload kept in-stream but out of context. Payload schema (owned
    /// by C31): `{"event_id": str, "channel": str, "topic": str|null,
    /// "raw": <verbatim channel payload>}`. Appended by the drain's
    /// consumption write in the same fenced batch as its sibling
    /// `input/admitted` (the Inline form is what the model sees; this
    /// event is what recall reads back from the stream — constraint 1:
    /// the stream stays the sole truth, no side store owns the raw).
    /// Drain activity (the additions-only default mapping); not
    /// context-bearing.
    #[serde(rename = "input/spilled")]
    InputSpilled,

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
            Self::ThreadStarted => "thread/started",
            Self::ThreadCompleted => "thread/completed",
            Self::EvidenceVerdict => "evidence/verdict",
            Self::ChannelDelivered => "channel/delivered",
            Self::InputSpilled => "input/spilled",
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
            (EventKind::ThreadStarted, r#"{"kind":"thread/started"}"#),
            (EventKind::ThreadCompleted, r#"{"kind":"thread/completed"}"#),
            (EventKind::EvidenceVerdict, r#"{"kind":"evidence/verdict"}"#),
            (
                EventKind::ChannelDelivered,
                r#"{"kind":"channel/delivered"}"#,
            ),
            (EventKind::InputSpilled, r#"{"kind":"input/spilled"}"#),
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
        // The durable set is exactly the twenty kinds above (the
        // fourteen frozen by C2 + the additive C14
        // `recovery/budget-reset` + the four additive C18 Phase-2 kinds
        // + the additive C31 `input/spilled`; additions-only, no
        // renames).
        assert_eq!(cases.len(), 20);
    }

    /// Additions-only regression (C18): every pre-existing kind's golden
    /// bytes are byte-identical to the Phase-1 frozen forms. This list is
    /// deliberately independent of the growing `cases` array above — it
    /// is the frozen Phase-1 contract, copied verbatim, so a future edit
    /// to an existing kind's serialization fails here even if `cases`
    /// was updated alongside it.
    #[test]
    fn additions_only_regression_phase1_kind_bytes_are_frozen() {
        let frozen_phase1: [(EventKind, &str); 15] = [
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
                    reason: EndReason::Failed,
                },
                r#"{"kind":"session/ended","reason":"failed"}"#,
            ),
        ];
        for (kind, golden) in &frozen_phase1 {
            assert_eq!(
                &serde_json::to_string(kind).unwrap(),
                golden,
                "pre-existing kind {:?} changed bytes (additions-only violated)",
                kind
            );
            let back: EventKind = serde_json::from_str(golden).unwrap();
            assert_eq!(&back, kind);
        }
    }

    /// Additions-only regression (C31): the four C18 Phase-2 kinds' golden
    /// bytes are frozen exactly as they landed in Phase 2. Together with
    /// the Phase-1 frozen set above this pins every one of the nineteen
    /// pre-C31 kinds, independently of the growing `cases` array, so the
    /// C31 addition cannot disturb a single existing serialization byte.
    #[test]
    fn additions_only_regression_phase2_kind_bytes_are_frozen() {
        let frozen_phase2: [(EventKind, &str); 4] = [
            (EventKind::ThreadStarted, r#"{"kind":"thread/started"}"#),
            (EventKind::ThreadCompleted, r#"{"kind":"thread/completed"}"#),
            (EventKind::EvidenceVerdict, r#"{"kind":"evidence/verdict"}"#),
            (
                EventKind::ChannelDelivered,
                r#"{"kind":"channel/delivered"}"#,
            ),
        ];
        for (kind, golden) in &frozen_phase2 {
            assert_eq!(
                &serde_json::to_string(kind).unwrap(),
                golden,
                "pre-existing kind {:?} changed bytes (additions-only violated)",
                kind
            );
            let back: EventKind = serde_json::from_str(golden).unwrap();
            assert_eq!(&back, kind);
        }
    }

    /// The C31 `input/spilled` kind round-trips through the full envelope
    /// with a payload of its tabled schema (§2 of the round-3 plan) riding
    /// opaquely in `bytes` (JSONL-export shape, the C14 replay path).
    #[test]
    fn input_spilled_envelope_round_trips_with_tabled_payload() {
        let payload = json!({
            "event_id": "evt-91",
            "channel": "mock-im",
            "topic": "worker-1",
            "raw": {"text": "@kian deploy", "attachments": [{"type": "file", "size": 4821}]}
        });
        let ev = Event {
            seq: 5,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(2),
            ts: 1_762_000_000_000,
            kind: EventKind::InputSpilled,
            bytes: payload.clone(),
            version: SCHEMA_VERSION,
        };
        let line = serde_json::to_string(&ev).unwrap();
        let back: Event = serde_json::from_str(&line).unwrap();
        assert_eq!(back, ev);
        assert_eq!(tag_of(&line), "input/spilled");
        // The verbatim raw survives the round trip byte-identically —
        // the spill is the audit copy the recall path reads back.
        assert_eq!(back.bytes, payload);
    }

    /// Each C18 Phase-2 kind round-trips through the full envelope with
    /// a payload of its tabled schema riding opaquely in `bytes`
    /// (JSONL-export shape, the C14 replay path).
    #[test]
    fn phase2_kind_envelopes_round_trip_with_tabled_payloads() {
        let payloads = [
            (
                EventKind::ThreadStarted,
                json!({
                    "thread_id": "0b6a18c4-0e52-4b8f-9d5a-2f4c9c1b7d01",
                    "parent": "0b6a18c4-0e52-4b8f-9d5a-2f4c9c1b7d02",
                    "worker": "0b6a18c4-0e52-4b8f-9d5a-2f4c9c1b7d03",
                    "pinned_version": 3,
                    "task": {"role": "user", "content": "refactor the parser"}
                }),
            ),
            (
                EventKind::ThreadCompleted,
                json!({
                    "thread_id": "0b6a18c4-0e52-4b8f-9d5a-2f4c9c1b7d01",
                    "status": "completed",
                    "summary": {"role": "assistant", "content": "done: 3 files"}
                }),
            ),
            (
                EventKind::EvidenceVerdict,
                json!({
                    "gate": "worker-stop",
                    "passed": true,
                    "checks": [
                        {"name": "final_message_present", "passed": true},
                        {"name": "no_error_tool_result_tail", "passed": true, "detail": "tail=2"}
                    ],
                    "attempt": 1
                }),
            ),
            (
                EventKind::ChannelDelivered,
                json!({
                    "channel": "mock-im",
                    "target": {"chat_id": 42},
                    "policy": "im/max-words",
                    "words": 87,
                    "truncated": false
                }),
            ),
        ];
        for (kind, payload) in &payloads {
            let ev = Event {
                seq: 4,
                session: SessionId(Uuid::nil()),
                epoch: Epoch(2),
                ts: 1_762_000_000_000,
                kind: *kind,
                bytes: payload.clone(),
                version: SCHEMA_VERSION,
            };
            let line = serde_json::to_string(&ev).unwrap();
            let back: Event = serde_json::from_str(&line).unwrap();
            assert_eq!(back, ev, "envelope round-trip for {}", kind.as_str());
            // The kind tag is exactly the tabled serde name.
            assert_eq!(tag_of(&line), kind.as_str());
        }
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
