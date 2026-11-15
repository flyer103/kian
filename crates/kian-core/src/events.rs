//! Event classification and envelope (DESIGN §3.1 "Event classification
//! (borrowing dsh's durable/live split)" and §3.3 "Schema versioning").
//!
//! The append-only `Event` envelope carries a schema `version` (v0, DESIGN
//! §3.3) and the durable `EventKind` set. Provider payloads stay opaque
//! `bytes` — no provider-specific field names inside `EventKind`. After
//! merges, event kinds are additions-only (constraint 3, cache-byte
//! stability).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{Epoch, SessionId};

/// Current event-schema version (DESIGN §3.3). v0 is frozen by; later
/// versions arrive exclusively as adjacent migrations — committed
/// generations are never renamed, replaced or deleted.
pub const SCHEMA_VERSION: u32 = 0;

/// The append-only event envelope — the sole truth (constraint 1). Events
/// are never modified; every projection is derived from a stream of these.
///
/// The exact serialized JSON form is frozen (golden-tested): field order and
/// byte shape are stable content, because envelopes feed replay and audit
/// exports.
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

/// The derived event id: `"evt_" + uuid5(session, "kian:event:<seq>")`
/// — RFC 4122 v5 with the session uuid as the namespace and the literal
/// `kian:event:<seq>` as the name.
///
/// Deterministic and derivable by anyone who knows the session id and
/// seq, so tooling (CLI `session export --with-event-ids`, the SDK
/// helper) can reference an event without storing an id. It is **not**
/// an envelope field and **not** stored: envelope bytes are frozen v0
/// (constraint 3), and `(session, seq)` is already unique — the id is a
/// naming convention over that pair, never a second source of truth.
pub fn event_id(session: &SessionId, seq: u64) -> String {
    format!(
        "evt_{}",
        uuid::Uuid::new_v5(&session.0, format!("kian:event:{seq}").as_bytes())
    )
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
    /// The drain stopped with a non-empty pending-confirmation set
    /// (CONFIRMATION-MODEL, ADR-033): `running -> idle` (resumable), not
    /// terminal. Deliberately distinct from `interrupted` (an owner
    /// stopped the drain) and `failed` (a failure).
    #[serde(rename = "awaiting_input")]
    AwaitingInput,
}

/// The durable event set (DESIGN §3.1). Live events (streaming chunks,
/// heartbeats, progress) are *not* here — they are never persisted.
///
/// Only `SessionEnded` carries structured data (`EndReason`) because the
/// terminal state transition must be projectable without parsing opaque
/// bytes (NoHalfTerminated, constraint 4). Every other kind's payload lives
/// in [`Event::bytes`] under a payload schema owned by the emitting
/// subsystem (e.g. `compact`'s `[a,b)` range + summary:).
///
/// **Additions-only after ** (constraint 3): no renames, no reformats.
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
    /// summary in its payload (schema owned ).
    #[serde(rename = "compact")]
    Compact,

    /// Slot → provider/model binding fixed at claim (ADR-006).
    #[serde(rename = "slot/binding")]
    SlotBinding,

    /// One fallback-chain switch within a bound slot.
    #[serde(rename = "slot/fallback")]
    SlotFallback,

    /// Explicit slot migration — the only escape hatch from an immutable
    /// binding (ADR-006).
    #[serde(rename = "slot/migration")]
    SlotMigration,

    /// Operator reset of a session's recovery budget. The
    /// reset is itself an appended event — there is no silent counter
    /// wipe (constraint 1: every observable change is an append). The
    /// payload (schema owned by: `{"reason": string}`) records why.
    /// Neutral by design: the state machine maps it to no transition
    /// and the model-context projection skips it, so replay derives
    /// fresh budgets from config while the event remains the audit
    /// record that a reset happened.
    #[serde(rename = "recovery/budget-reset")]
    RecoveryBudgetReset,

    /// Thread lineage start (bundle): a Router's drain dispatched
    /// work to a worker session, or the worker's stream records that it
    /// was spawned by delegation. Payload schema (owned ):
    /// `{"thread_id": uuid, "parent": session-uuid, "worker":
    /// session-uuid, "pinned_version": u64, "task": <message payload>}`.
    /// State-preserving in `idle`/`running`/`recoverable` (same row as
    /// `input/admitted`); not context-bearing.
    #[serde(rename = "thread/started")]
    ThreadStarted,

    /// Thread lineage completion (bundle): the worker's own drain
    /// appended its outcome before the end write. Payload schema (owned
    /// ): `{"thread_id": uuid, "status": "completed"|"failed"|
    /// "interrupted", "summary": <message payload>}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "thread/completed")]
    ThreadCompleted,

    /// Evidence-gate verdict (bundle): the bounded, declarative
    /// stop-gate recorded its decision. Payload schema (owned ):
    /// `{"gate": str, "passed": bool, "checks": [{"name": str,
    /// "passed": bool, "detail"?: str}], "attempt": u64}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "evidence/verdict")]
    EvidenceVerdict,

    /// Channel delivery receipt (bundle): a channel output policy
    /// delivered a message and recorded the honest receipt. Payload
    /// schema (owned ): `{"channel": str, "target": <value>,
    /// "policy": str, "words"?: u64, "truncated": bool}`. Drain activity
    /// (default mapping); not context-bearing.
    #[serde(rename = "channel/delivered")]
    ChannelDelivered,

    /// Spilled raw channel payload (ecosystem): the verbatim channel
    /// payload kept in-stream but out of context. Payload schema (owned
    /// ): `{"event_id": str, "channel": str, "topic": str|null,
    /// "raw": <verbatim channel payload>}`. Appended by the drain's
    /// consumption write in the same fenced batch as its sibling
    /// `input/admitted` (the Inline form is what the model sees; this
    /// event is what recall reads back from the stream — constraint 1:
    /// the stream stays the sole truth, no side store owns the raw).
    /// Drain activity (the additions-only default mapping); not
    /// context-bearing.
    #[serde(rename = "input/spilled")]
    InputSpilled,

    /// Durable interrupt intent (ADR-014): the control plane's
    /// interrupt request, consumed and appended by the owning drain at a
    /// safe boundary. Payload schema (owned ):
    /// `{"action": "interrupt", "reason": string|null,
    /// "requested_by": string|null, "requested_at_ms": u64,
    /// "target_epoch": u64|null, "stale"?: true}` — `stale` is present
    /// (and `true`) only when the row's target episode had already
    /// settled before consumption (the audit-only arm; ends nothing).
    /// Drain activity (default mapping); not context-bearing — the kind
    /// never enters a model request (constraint 3).
    #[serde(rename = "control/interrupt")]
    ControlInterrupt,

    /// A memory note (MEMORY-MODEL, ADR-016): a *claim* about the agent,
    /// extracted from a source session's transcript by the dreamer drain
    /// and appended to the memory session's stream. Payload schema
    /// (owned by the memory dreamer drain): `{"subject": <non-empty
    /// str>, "text": <str>, "status": "candidate"|"verified"|"stale",
    /// "derived_from": [{"session": <uuid-str>, "start_seq": <u64>,
    /// "end_seq": <u64>}], "verified_by"?: {"method":
    /// "transcript"|"evidence_gate"|"human", "ref": <str>}}` —
    /// `verified_by` rides only on verified notes. Non-context-bearing
    /// (a note is never a message, constraint 3); drain activity (the
    /// additions-only default) — legal only while `running`; the
    /// latest-per-subject fold is a projection in `kian-core::memory`,
    /// never state outside the stream.
    #[serde(rename = "memory/note")]
    MemoryNote,

    /// A human decision on one pending ask, consumed from the reserved
    /// confirm row (CONFIRMATION-MODEL §5, ADR-033): non-context drain
    /// activity, the durable record that the answer arrived, before any
    /// effect.
    #[serde(rename = "control/confirm")]
    ControlConfirm,

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
            Self::ControlInterrupt => "control/interrupt",
            Self::MemoryNote => "memory/note",
            Self::ControlConfirm => "control/confirm",
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

    /// Every EventKind has an exact golden JSON form and round-trips (
    /// acceptance). The `as_str` name must stay in sync with the serde tag.
    /// After merges this set is additions-only.
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
                EventKind::ControlInterrupt,
                r#"{"kind":"control/interrupt"}"#,
            ),
            (EventKind::MemoryNote, r#"{"kind":"memory/note"}"#),
            (EventKind::ControlConfirm, r#"{"kind":"control/confirm"}"#),
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
        // The durable set is exactly the twenty-three kinds above (the
        // fourteen v0-frozen kinds + the additive
        // `recovery/budget-reset` + the four additive bundle kinds
        // + the additive `input/spilled` + the additive
        // `control/interrupt` + the additive `memory/note` + the
        // additive `control/confirm`; additions-only, no renames).
        assert_eq!(cases.len(), 23);
    }

    /// Additions-only regression: every pre-existing kind's golden
    /// bytes are byte-identical to the built-in frozen forms. This list is
    /// deliberately independent of the growing `cases` array above — it
    /// is the frozen built-in contract, copied verbatim, so a future edit
    /// to an existing kind's serialization fails here even if `cases`
    /// was updated alongside it.
    #[test]
    fn additions_only_regression_initial_kind_bytes_are_frozen() {
        let frozen_initial: [(EventKind, &str); 15] = [
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
        for (kind, golden) in &frozen_initial {
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

    /// Additions-only regression: the four bundle kinds' golden
    /// bytes are frozen exactly as they landed in bundle. Together with
    /// the built-in frozen set above this pins every one of the nineteen
    /// legacy kinds, independently of the growing `cases` array, so the
    /// addition cannot disturb a single existing serialization byte.
    #[test]
    fn additions_only_regression_bundle_kind_bytes_are_frozen() {
        let frozen_bundle: [(EventKind, &str); 4] = [
            (EventKind::ThreadStarted, r#"{"kind":"thread/started"}"#),
            (EventKind::ThreadCompleted, r#"{"kind":"thread/completed"}"#),
            (EventKind::EvidenceVerdict, r#"{"kind":"evidence/verdict"}"#),
            (
                EventKind::ChannelDelivered,
                r#"{"kind":"channel/delivered"}"#,
            ),
        ];
        for (kind, golden) in &frozen_bundle {
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

    /// Additions-only regression: all twenty legacy kinds' golden
    /// bytes are frozen exactly as they stood at the end of.
    /// Like the built-in/bundle frozen lists above, this is
    /// deliberately independent of the growing `cases` array, so the
    /// `control/interrupt` addition cannot disturb a single
    /// existing serialization byte.
    #[test]
    fn additions_only_regression_all_kind_bytes_are_frozen() {
        let frozen_all: [(EventKind, &str); 20] = [
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
                    reason: EndReason::Failed,
                },
                r#"{"kind":"session/ended","reason":"failed"}"#,
            ),
        ];
        for (kind, golden) in &frozen_all {
            assert_eq!(
                &serde_json::to_string(kind).unwrap(),
                golden,
                "pre-existing kind {:?} changed bytes (additions-only violated)",
                kind
            );
            let back: EventKind = serde_json::from_str(golden).unwrap();
            assert_eq!(&back, kind);
        }
        assert_eq!(frozen_all.len(), 20);
    }

    /// The `control/interrupt` kind round-trips through the full
    /// envelope with its tabled payload riding opaquely in `bytes`,
    /// including the audit-only `stale` marker variant.
    #[test]
    fn control_interrupt_envelope_round_trips_with_tabled_payload() {
        for (payload, with_stale) in [
            (
                json!({
                    "action": "interrupt",
                    "reason": "operator requested",
                    "requested_by": "ops@replica-a",
                    "requested_at_ms": 1_762_000_000_000_u64,
                    "target_epoch": 3
                }),
                false,
            ),
            (
                json!({
                    "action": "interrupt",
                    "reason": null,
                    "requested_by": null,
                    "requested_at_ms": 1_762_000_000_001_u64,
                    "target_epoch": null,
                    "stale": true
                }),
                true,
            ),
        ] {
            let mut ev = sample_event(EventKind::ControlInterrupt);
            ev.bytes = payload.clone();
            let line = serde_json::to_string(&ev).unwrap();
            let back: Event = serde_json::from_str(&line).unwrap();
            assert_eq!(back, ev);
            assert_eq!(tag_of(&line), "control/interrupt");
            assert_eq!(back.bytes, payload);
            // The stale marker rides only when set (additive key).
            if !with_stale {
                assert!(back.bytes.get("stale").is_none());
            } else {
                assert_eq!(back.bytes.get("stale"), Some(&json!(true)));
            }
        }
    }

    /// The `memory/note` kind round-trips through the full envelope with
    /// its tabled payload (round-8 plan §2.1) riding opaquely in `bytes`;
    /// `verified_by` is the one optional key (absent for notes that have
    /// not been verified yet).
    #[test]
    fn memory_note_envelope_round_trips_with_tabled_payload() {
        let verified = json!({
            "subject": "deploys via the dsh bridge",
            "text": "The team deploys through the dsh bridge; verify the environment before suggesting commands.",
            "status": "verified",
            "derived_from": [{
                "session": "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                "start_seq": 4,
                "end_seq": 38
            }],
            "verified_by": {"method": "transcript", "ref": "seq range [4,38) read back from the source stream"}
        });
        let candidate = json!({
            "subject": "prefers concise replies",
            "text": "Keep answers short and to the point.",
            "status": "candidate",
            "derived_from": [{
                "session": "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                "start_seq": 4,
                "end_seq": 9
            }]
        });
        for (payload, has_verified_by) in [(verified, true), (candidate, false)] {
            let mut ev = sample_event(EventKind::MemoryNote);
            ev.bytes = payload.clone();
            let line = serde_json::to_string(&ev).unwrap();
            let back: Event = serde_json::from_str(&line).unwrap();
            assert_eq!(back, ev);
            assert_eq!(tag_of(&line), "memory/note");
            assert_eq!(back.bytes, payload);
            // The optional key rides only when set (additive key).
            if has_verified_by {
                assert!(back.bytes.get("verified_by").is_some());
            } else {
                assert!(back.bytes.get("verified_by").is_none());
            }
        }
    }

    /// Additions-only regression: all twenty-one pre-`memory/note` kinds'
    /// golden bytes are frozen exactly as they stood at the end of round
    /// 7 (the twenty of `frozen_all` plus `control/interrupt`). Like the
    /// frozen lists above, this is deliberately independent of the
    /// growing `cases` array, so the `memory/note` addition cannot
    /// disturb a single existing serialization byte.
    #[test]
    fn additions_only_regression_round8_kind_bytes_are_frozen() {
        let frozen_round7: [(EventKind, &str); 21] = [
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
                EventKind::ControlInterrupt,
                r#"{"kind":"control/interrupt"}"#,
            ),
            (
                EventKind::SessionEnded {
                    reason: EndReason::Failed,
                },
                r#"{"kind":"session/ended","reason":"failed"}"#,
            ),
        ];
        for (kind, golden) in &frozen_round7 {
            assert_eq!(
                &serde_json::to_string(kind).unwrap(),
                golden,
                "pre-existing kind {:?} changed bytes (additions-only violated)",
                kind
            );
            let back: EventKind = serde_json::from_str(golden).unwrap();
            assert_eq!(&back, kind);
        }
        assert_eq!(frozen_round7.len(), 21);
    }

    /// The `input/spilled` kind round-trips through the full envelope
    /// with a payload of its tabled schema (§2 of the plan) riding
    /// opaquely in `bytes` (JSONL-export shape, the replay path).
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

    /// Each bundle kind round-trips through the full envelope with
    /// a payload of its tabled schema riding opaquely in `bytes`
    /// (JSONL-export shape, the replay path).
    #[test]
    fn bundle_kind_envelopes_round_trip_with_tabled_payloads() {
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
            (EndReason::AwaitingInput, "awaiting_input"),
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

    // -- derived event id -------------------------------------------------

    /// Golden vectors for the derived event id rule: `evt_` + RFC 4122
    /// v5(session uuid, "kian:event:<seq>"). The literals are generated
    /// by an independent implementation (Python `uuid.uuid5`) so the
    /// Rust `uuid` crate and any mirror (SDK, external tooling) are
    /// pinned to the same bytes.
    #[test]
    fn event_id_golden_vectors_match_rfc4122_v5() {
        let ns = SessionId(Uuid::parse_str("0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b").unwrap());
        let cases: &[(u64, &str)] = &[
            (1, "evt_ec671a02-5108-5e7a-811a-3517bea635e6"),
            (7, "evt_42124c15-3948-56f1-9606-517dc61d2829"),
            (42, "evt_b1a35135-d4f4-50cd-b457-e8b96fb9e5dd"),
        ];
        for (seq, expected) in cases {
            assert_eq!(&super::event_id(&ns, *seq), expected, "seq {seq}");
        }
        // A second namespace pins the "distinct per session" half of the
        // rule with its own golden literal.
        let ns2 = SessionId(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
        assert_eq!(
            super::event_id(&ns2, 7),
            "evt_ac6b5bb7-fa8f-5314-868d-4c342a94d273"
        );
    }

    /// The derived id is deterministic, distinct per seq, distinct per
    /// session, and never collides across a (session × seq) grid.
    #[test]
    fn event_id_is_deterministic_and_distinct_per_seq_and_session() {
        let a = SessionId(Uuid::parse_str("0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b").unwrap());
        let b = SessionId(Uuid::new_v4());
        // Determinism.
        assert_eq!(super::event_id(&a, 1), super::event_id(&a, 1));
        // Distinct per seq within a session.
        let ids: Vec<String> = (0..64).map(|seq| super::event_id(&a, seq)).collect();
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(ids.len(), unique.len(), "ids collide across seq");
        // Distinct per session at the same seq.
        assert_ne!(super::event_id(&a, 7), super::event_id(&b, 7));
        // Every id carries the documented prefix and a v5-shaped uuid.
        for id in &ids {
            assert!(id.starts_with("evt_"));
            assert_eq!(id.len(), "evt_".len() + 36);
            assert_eq!(id.as_bytes()[18], b'5', "uuid version nibble must be 5");
        }
    }
}
