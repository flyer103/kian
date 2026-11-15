//! Model-context projection (DESIGN §3.2 "Model-context projection (the
//! implementation core of A2/A3)").
//!
//! [`derive_model_context`] projects the next model request's messages from
//! a session's event stream. This is the code constraints 2 and 3 exist to
//! protect:
//!
//! - **History is immutable**: the `bytes` of an `assistant/message` event
//!   *is* what was sent; the projection passes content through verbatim and
//!   never reformats, reorders or normalizes it.
//! - **Compression is the only deliberate invalidation point** (A3): a
//!   `compact` event carries the pre-compression range `[a,b)` and the
//!   post-compression summary; the projection skips exactly that range and
//!   inserts the summary at the range's position.
//! - **Synthetic messages are strippable** (A6): steer-injected messages
//!   carry a `synthetic=true` marker; [`strip_synthetic`] restores the
//!   "real" context for replay/export.
//!
//! Historical events are immutable through this API — there is no `&mut`
//! path from the projection to a stored event:
//!
//! ```compile_fail
//! use kian_core::events::Event;
//!
//! fn tamper(events: &[Event]) {
//!     // E0594: cannot assign to data in a `&` reference — history is
//!     // read-only through any borrowed stream.
//!     events[0].seq = 99;
//! }
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::{Event, EventKind, SCHEMA_VERSION};
use crate::state::ProjectionError;

/// One model-visible message — the projection output unit.
///
/// `content` passes through verbatim from the originating event's payload
/// (A2): it is never reformatted, reordered or normalized. `synthetic`
/// marks steer-injected messages (A6) so replay/export can strip them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Non-empty role string (provider-shaped, e.g. `user`/`assistant`/
    /// `tool`). Kept as a string deliberately: the core does not model
    /// provider role vocabularies (narrow waist).
    pub role: String,
    /// The exact payload content — the bytes are the truth (constraint 2).
    pub content: Value,
    /// `true` for steer-injected synthetic messages (strippable, A6).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub synthetic: bool,
}

/// Named context-projection errors.
///
/// Envelope discipline reuses [`ProjectionError`] so the two
/// projections cannot drift on envelope checks; payload and compact-range
/// errors are 's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ContextError {
    /// Envelope check failed (empty stream, non-monotone seq, foreign
    /// session, future schema version).
    #[error(transparent)]
    Envelope(#[from] ProjectionError),
    /// A context-bearing event's payload does not match the frozen message
    /// payload contract (§3.2).
    #[error("event seq {seq} ({kind}): malformed context payload: {reason}")]
    MalformedPayload {
        seq: u64,
        kind: &'static str,
        reason: &'static str,
    },
    /// `compact` range is empty or inverted.
    #[error("compact at seq {seq}: invalid range [{from},{to})")]
    InvalidCompactRange { seq: u64, from: u64, to: u64 },
    /// `compact` range extends to or past events appended after the compact
    /// itself — compression may only cover the past.
    #[error("compact at seq {seq}: range [{from},{to}) extends past the compact event")]
    CompactRangeNotInPast { seq: u64, from: u64, to: u64 },
    /// `compact` range overlaps an already-compacted range: skipped bytes
    /// are gone and cannot be compressed again.
    #[error(
        "compact at seq {seq}: range [{from},{to}) overlaps already-compacted range [{prev_from},{prev_to})"
    )]
    OverlappingCompactRange {
        seq: u64,
        from: u64,
        to: u64,
        prev_from: u64,
        prev_to: u64,
    },
}

/// The context-bearing event kinds (DESIGN §3.2): the kinds whose payload
/// is a message. Every other kind (session/drain/turn/step/slot) is
/// envelope or audit data — `step/request` is the A2 audit copy of a whole
/// request, not a message, so it never re-enters the context.
///
/// The bundle kinds are named *explicitly* in the skip branch: they
/// are lineage/audit/receipt data by contract, so a stream containing
/// them derives exactly the context of the same stream without them.
/// Skipping them is not silent inheritance of the catch-all — a future
/// context-bearing addition must edit this match.
fn is_context_bearing(kind: &EventKind) -> bool {
    match kind {
        EventKind::InputAdmitted
        | EventKind::AssistantMessage
        | EventKind::ToolCall
        | EventKind::ToolResult => true,
        // bundle additions: non-context-bearing (thread lineage,
        // evidence verdicts, delivery receipts).
        EventKind::ThreadStarted
        | EventKind::ThreadCompleted
        | EventKind::EvidenceVerdict
        | EventKind::ChannelDelivered => false,
        // later addition: the spilled raw is deliberately kept
        // out of the model context — the Inline form carried by its
        // sibling `input/admitted` is what the model sees. Named here
        // (not silently in the catch-all) per the guard pattern: a
        // future context-bearing addition must edit this match.
        EventKind::InputSpilled => false,
        // addition: `control/interrupt` is drain-activity audit
        // data (the durable interrupt intent), never a message — the
        // kind must not appear in a model request (constraint 3).
        EventKind::ControlInterrupt => false,
        // addition: a memory note is a *claim* about the agent
        // (MEMORY-MODEL), not a message — appended by the dreamer
        // drain and folded by `kian-core::memory`; it never re-enters
        // a model context (constraint 3).
        EventKind::MemoryNote => false,
        // addition: the confirmation decision is drain-activity audit
        // data (CONFIRMATION-MODEL §5) — the durable answer, never a
        // message the model sees (constraint 3).
        EventKind::ControlConfirm => false,
        _ => false,
    }
}

/// Parses the frozen message payload contract:
/// `{"role": <non-empty string>, "content": <any JSON>, "synthetic"?: <bool>}`.
/// Field order in the source object is irrelevant (serde_json uses ordered
/// maps); the *content* is preserved exactly.
fn parse_message_payload(
    seq: u64,
    kind: &'static str,
    bytes: &Value,
) -> Result<Message, ContextError> {
    let malformed = |reason: &'static str| ContextError::MalformedPayload { seq, kind, reason };
    let obj = bytes
        .as_object()
        .ok_or_else(|| malformed("payload must be a JSON object with 'role' and 'content'"))?;

    let role = obj
        .get("role")
        .ok_or_else(|| malformed("payload must be a JSON object with 'role' and 'content'"))?
        .as_str()
        .filter(|r| !r.is_empty())
        .ok_or_else(|| malformed("'role' must be a non-empty string"))?;

    let content = obj
        .get("content")
        .ok_or_else(|| malformed("payload must be a JSON object with 'role' and 'content'"))?
        .clone();

    let synthetic = match obj.get("synthetic") {
        None => false,
        Some(v) => v
            .as_bool()
            .ok_or_else(|| malformed("'synthetic' must be a boolean when present"))?,
    };

    Ok(Message {
        role: role.to_owned(),
        content,
        synthetic,
    })
}

/// One validated `compact` event: the pre-compression range `[from,to)`
/// (event seqs), the post-compression summary message, and the compact
/// event's own seq (the summary's provenance source).
struct CompactSpec {
    seq: u64,
    from: u64,
    to: u64,
    summary: Message,
}

/// Projects the next model request's messages from a session's event
/// stream (DESIGN §3.2, the implementation core of A2/A3).
///
/// - Messages appear in event order; content is passed through verbatim.
/// - A `compact` event's `[a,b)` range is skipped exactly; the summary is
///   inserted at the range's position. Ranges must satisfy
///   `a < b <= seq(compact)` and be pairwise disjoint.
/// - Steer-injected synthetic messages are included (the model must see
///   the steer); [`strip_synthetic`] restores the "real" context.
/// - State legality is [`crate::state::project`]'s concern, not this
///   function's; only envelope discipline is re-checked here.
///
/// Pure and total: no I/O, no clock, no allocation beyond the output.
pub fn derive_model_context(events: &[Event]) -> Result<Vec<Message>, ContextError> {
    Ok(derive_with_provenance(events)?.0)
}

/// The per-message provenance of a projected context (READ-MODEL §5):
/// the source event seq of every message [`derive_model_context`] emits,
/// position-aligned with the message list (a `compact` summary's source
/// is the compact event's own seq). Derived by the *same* fold — the two
/// outputs can never disagree — so a snapshot carrying messages plus
/// this provenance is exactly the projection the stream would rebuild.
///
/// Metadata only (constraint 3): provenance never enters a model request
/// and never changes [`derive_model_context`]'s bytes.
pub fn context_provenance(events: &[Event]) -> Result<Vec<u64>, ContextError> {
    Ok(derive_with_provenance(events)?.1)
}

/// The shared fold: messages + their source seqs in one pass. Splitting
/// the two public functions over this single implementation is what
/// keeps them aligned by construction.
fn derive_with_provenance(events: &[Event]) -> Result<(Vec<Message>, Vec<u64>), ContextError> {
    let Some(first) = events.first() else {
        return Err(ProjectionError::EmptyStream.into());
    };
    let session = first.session;

    // Pass 1: envelope discipline + compact collection and validation.
    let mut prev_seq: Option<u64> = None;
    let mut compacts: Vec<CompactSpec> = Vec::new();
    for ev in events {
        if ev.version > SCHEMA_VERSION {
            return Err(ProjectionError::UnsupportedVersion {
                got: ev.version,
                supported: SCHEMA_VERSION,
            }
            .into());
        }
        if ev.session != session {
            return Err(ProjectionError::SessionIdMismatch {
                expected: session,
                got: ev.session,
            }
            .into());
        }
        if let Some(prev) = prev_seq
            && ev.seq <= prev
        {
            return Err(ProjectionError::NonMonotoneSeq { prev, got: ev.seq }.into());
        }
        prev_seq = Some(ev.seq);

        if let EventKind::Compact = ev.kind {
            let malformed = |reason: &'static str| ContextError::MalformedPayload {
                seq: ev.seq,
                kind: "compact",
                reason,
            };
            let obj = ev.bytes.as_object().ok_or_else(|| {
                malformed("payload must be an object with 'from_seq', 'to_seq' and 'summary'")
            })?;
            let u64_field = |name: &str| -> Result<u64, ContextError> {
                obj.get(name)
                    .and_then(Value::as_u64)
                    .ok_or(ContextError::MalformedPayload {
                        seq: ev.seq,
                        kind: "compact",
                        reason: match name {
                            "from_seq" => "'from_seq' must be a u64",
                            _ => "'to_seq' must be a u64",
                        },
                    })
            };
            let from = u64_field("from_seq")?;
            let to = u64_field("to_seq")?;
            let summary_payload = obj.get("summary").ok_or_else(|| {
                malformed("payload must be an object with 'from_seq', 'to_seq' and 'summary'")
            })?;
            let summary = parse_message_payload(ev.seq, "compact/summary", summary_payload)?;

            if from >= to {
                return Err(ContextError::InvalidCompactRange {
                    seq: ev.seq,
                    from,
                    to,
                });
            }
            // A range may only cover events already appended: to <= the
            // compact event's own seq.
            if to > ev.seq {
                return Err(ContextError::CompactRangeNotInPast {
                    seq: ev.seq,
                    from,
                    to,
                });
            }
            if let Some(c) = compacts.iter().find(|c| from < c.to && c.from < to) {
                return Err(ContextError::OverlappingCompactRange {
                    seq: ev.seq,
                    from,
                    to,
                    prev_from: c.from,
                    prev_to: c.to,
                });
            }
            compacts.push(CompactSpec {
                seq: ev.seq,
                from,
                to,
                summary,
            });
        }
    }

    // Ranges are disjoint; ordering by `from` makes summary insertion
    // positional regardless of append order.
    compacts.sort_by_key(|c| c.from);

    // Pass 2: fold. Skipped-range events are never parsed; each summary is
    // emitted at the position of the first event with seq >= from. The
    // provenance rides the same push (a summary's source seq is the
    // compact event's own) — the two lists stay position-aligned.
    let mut messages: Vec<Message> = Vec::new();
    let mut provenance: Vec<u64> = Vec::new();
    let mut next_summary = 0usize;
    for ev in events {
        while next_summary < compacts.len() && compacts[next_summary].from <= ev.seq {
            messages.push(compacts[next_summary].summary.clone());
            provenance.push(compacts[next_summary].seq);
            next_summary += 1;
        }
        let compacted = compacts.iter().any(|c| c.from <= ev.seq && ev.seq < c.to);
        if compacted || !is_context_bearing(&ev.kind) {
            continue;
        }
        messages.push(parse_message_payload(ev.seq, ev.kind.as_str(), &ev.bytes)?);
        provenance.push(ev.seq);
    }
    // Every compact has from < to <= its own seq, so each summary flushed
    // at its own compact event at the latest — nothing can remain pending.
    debug_assert_eq!(next_summary, compacts.len());
    debug_assert_eq!(messages.len(), provenance.len());

    Ok((messages, provenance))
}

/// Removes synthetic (steer-injected) messages — the A6 strip that restores
/// the "real" context for replay/export. Position and bytes of every real
/// message are untouched.
#[must_use]
pub fn strip_synthetic(messages: &[Message]) -> Vec<Message> {
    messages.iter().filter(|m| !m.synthetic).cloned().collect()
}

/// The canonical byte form of a projected context.
///
/// **Frozen stable content (constraint 3):** `serde_json::to_vec` under the
/// workspace's default feature set — compact output, struct fields in
/// declaration order (`role`, `content`, then `synthetic` only when
/// `true`), object keys lexicographic (`serde_json::Map` is a `BTreeMap`;
/// `preserve_order` must never be enabled). The same logical context
/// always serializes to identical bytes — this is the basis of the A2
/// golden-bytes tests, the replay diff and the prefix-hash
/// comparison. Changing this form is a deliberate prefix-cache
/// invalidation and a constraint-3 event.
#[must_use]
pub fn context_to_bytes(messages: &[Message]) -> Vec<u8> {
    serde_json::to_vec(messages).expect("Message serializes infallibly (valid JSON only)")
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        ContextError, Message, context_provenance, context_to_bytes, derive_model_context,
        strip_synthetic,
    };
    use crate::events::{Event, EventKind, SCHEMA_VERSION};
    use crate::model::{Epoch, SessionId};
    use crate::state::ProjectionError;

    /// Stream fixture with explicit control over payloads and seqs
    /// (`push` returns the seq of the appended event so compact ranges can
    /// reference exact boundaries).
    struct CtxBuilder {
        session: SessionId,
        seq: u64,
        ts: u64,
        epoch: Epoch,
        events: Vec<Event>,
    }

    impl CtxBuilder {
        fn new() -> Self {
            Self {
                session: SessionId(Uuid::nil()),
                seq: 0,
                ts: 1_000,
                epoch: Epoch(1),
                events: Vec::new(),
            }
        }

        fn push(&mut self, kind: EventKind, bytes: Value) -> u64 {
            let seq = self.seq;
            self.events.push(Event {
                seq,
                session: self.session,
                epoch: self.epoch,
                ts: self.ts,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            });
            self.seq += 1;
            self.ts += 7;
            seq
        }

        fn create(&mut self) -> u64 {
            self.push(EventKind::SessionCreated, json!({}))
        }

        fn claim(&mut self) -> u64 {
            self.epoch = self.epoch.next();
            self.push(EventKind::DrainClaimed, json!({}))
        }

        fn user(&mut self, content: &str) -> u64 {
            self.push(
                EventKind::InputAdmitted,
                json!({"role": "user", "content": content}),
            )
        }

        /// Steer-injected synthetic user message (A6 marker).
        fn steer(&mut self, content: &str) -> u64 {
            self.push(
                EventKind::InputAdmitted,
                json!({"role": "user", "content": content, "synthetic": true}),
            )
        }

        fn assistant(&mut self, content: Value) -> u64 {
            self.push(
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": content}),
            )
        }

        fn tool_call(&mut self, content: Value) -> u64 {
            self.push(
                EventKind::ToolCall,
                json!({"role": "assistant", "content": content}),
            )
        }

        fn tool_result(&mut self, content: Value) -> u64 {
            self.push(
                EventKind::ToolResult,
                json!({"role": "tool", "content": content}),
            )
        }

        fn compact(&mut self, a: u64, b: u64, summary: &str) -> u64 {
            self.push(
                EventKind::Compact,
                json!({
                    "from_seq": a,
                    "to_seq": b,
                    "summary": {"role": "assistant", "content": summary},
                }),
            )
        }

        fn build(&self) -> Vec<Event> {
            self.events.clone()
        }
    }

    fn started() -> CtxBuilder {
        let mut b = CtxBuilder::new();
        b.create();
        b.claim();
        b
    }

    fn user_msg(content: &str) -> Message {
        Message {
            role: "user".into(),
            content: json!(content),
            synthetic: false,
        }
    }

    fn assistant_msg(content: Value) -> Message {
        Message {
            role: "assistant".into(),
            content,
            synthetic: false,
        }
    }

    // ------------------------------------------------------------------
    // Envelope discipline
    // ------------------------------------------------------------------

    #[test]
    fn empty_stream_is_an_error() {
        assert_eq!(
            derive_model_context(&[]),
            Err(ContextError::Envelope(ProjectionError::EmptyStream))
        );
    }

    #[test]
    fn envelope_errors_are_rejected() {
        let mut b = started();
        b.user("hi");
        let mut events = b.build();

        // Non-monotone seq.
        let mut repeat = events[events.len() - 1].clone();
        repeat.seq = 0;
        let mut with_repeat = events.clone();
        with_repeat.push(repeat);
        assert_eq!(
            derive_model_context(&with_repeat),
            Err(ContextError::Envelope(ProjectionError::NonMonotoneSeq {
                prev: 2,
                got: 0
            }))
        );

        // Foreign session.
        let mut stray = events[events.len() - 1].clone();
        stray.seq += 1;
        stray.session = SessionId::new();
        events.push(stray);
        assert!(matches!(
            derive_model_context(&events),
            Err(ContextError::Envelope(
                ProjectionError::SessionIdMismatch { .. }
            ))
        ));

        // Future schema version (on a stream without the stray above).
        events.pop();
        let mut future = events[events.len() - 1].clone();
        future.seq += 1;
        future.session = events[0].session;
        future.version = SCHEMA_VERSION + 1;
        events.push(future);
        assert_eq!(
            derive_model_context(&events),
            Err(ContextError::Envelope(
                ProjectionError::UnsupportedVersion {
                    got: SCHEMA_VERSION + 1,
                    supported: SCHEMA_VERSION,
                }
            ))
        );
    }

    // ------------------------------------------------------------------
    // Projection shape
    // ------------------------------------------------------------------

    #[test]
    fn projects_context_in_event_order_and_ignores_non_context_kinds() {
        let mut b = started();
        b.user("hello");
        b.push(EventKind::TurnStart, json!({}));
        b.push(
            EventKind::StepRequest,
            json!({"request": "audit copy, not a message"}),
        );
        b.assistant(json!("hi there"));
        b.push(EventKind::TurnEnd, json!({}));
        b.push(EventKind::SlotBinding, json!({"slot": "main"}));
        b.push(EventKind::SlotFallback, json!({}));
        b.push(EventKind::SlotMigration, json!({}));
        b.push(
            EventKind::SessionEnded {
                reason: crate::events::EndReason::Completed,
            },
            json!({}),
        );

        assert_eq!(
            derive_model_context(&b.build()).unwrap(),
            vec![user_msg("hello"), assistant_msg(json!("hi there"))]
        );
    }

    /// the four bundle kinds are non-context-bearing — a stream
    /// containing them derives a byte-identical context to the same
    /// stream without them (constraints 1 and 3: the additions are audit
    /// data, never a context mutation), and a stream containing *only*
    /// new kinds derives an empty context.
    #[test]
    fn bundle_kinds_leave_model_context_unchanged() {
        let mut with = started();
        with.user("hello");
        with.push(
            EventKind::ThreadStarted,
            json!({"thread_id": "t-1", "pinned_version": 1, "task": {"role": "user", "content": "work"}}),
        );
        with.assistant(json!("working"));
        with.push(
            EventKind::EvidenceVerdict,
            json!({"gate": "g", "passed": true, "checks": [], "attempt": 1}),
        );
        with.push(
            EventKind::ThreadCompleted,
            json!({"thread_id": "t-1", "status": "completed", "summary": {"role": "assistant", "content": "done"}}),
        );
        with.push(
            EventKind::ChannelDelivered,
            json!({"channel": "mock-im", "target": {}, "policy": "im", "truncated": false}),
        );

        let mut without = started();
        without.user("hello");
        without.assistant(json!("working"));

        let ctx_with = derive_model_context(&with.build()).unwrap();
        let ctx_without = derive_model_context(&without.build()).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(
            context_to_bytes(&ctx_with),
            context_to_bytes(&ctx_without),
            "non-baseline kinds must not change a single context byte"
        );

        // A stream of only new kinds (on top of create/claim) is context
        // empty — even when the payloads are message-shaped.
        let mut only_new = started();
        only_new.push(
            EventKind::ThreadStarted,
            json!({"role": "user", "content": "looks like a message, is not"}),
        );
        only_new.push(
            EventKind::ThreadCompleted,
            json!({"role": "assistant", "content": "no"}),
        );
        only_new.push(
            EventKind::EvidenceVerdict,
            json!({"role": "user", "content": "no"}),
        );
        only_new.push(
            EventKind::ChannelDelivered,
            json!({"role": "user", "content": "no"}),
        );
        assert_eq!(derive_model_context(&only_new.build()).unwrap(), Vec::new());
    }

    /// `input/spilled` is non-context-bearing — a stream containing
    /// it derives a byte-identical context to the same stream without it
    /// (the spilled raw is audit data for the recall path, never a
    /// context mutation; the Inline form in its sibling
    /// `input/admitted` is what the model sees), and its payload is
    /// never parsed even when message-shaped.
    #[test]
    fn input_spilled_leaves_model_context_unchanged() {
        let spill = json!({
            "event_id": "evt-91",
            "channel": "mock-im",
            "topic": "worker-1",
            "raw": {"text": "@kian deploy", "attachments": [{"type": "file", "size": 4821}]}
        });

        let mut with = started();
        with.user("hello");
        with.push(EventKind::InputSpilled, spill.clone());
        with.assistant(json!("working"));
        // A second spill directly between context-bearing messages —
        // position must not matter, only the kind does.
        with.push(EventKind::InputSpilled, spill);

        let mut without = started();
        without.user("hello");
        without.assistant(json!("working"));

        let ctx_with = derive_model_context(&with.build()).unwrap();
        let ctx_without = derive_model_context(&without.build()).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(
            context_to_bytes(&ctx_with),
            context_to_bytes(&ctx_without),
            "input/spilled must not change a single context byte"
        );

        // A message-shaped spill payload is skipped unparsed — even a
        // payload that would be malformed for a context-bearing kind
        // must not error here.
        let mut shaped = started();
        shaped.user("q");
        shaped.push(EventKind::InputSpilled, json!({"no_role": true}));
        let ctx = derive_model_context(&shaped.build()).unwrap();
        assert_eq!(ctx, vec![user_msg("q")]);
    }

    /// `control/interrupt` is non-context-bearing — a stream
    /// containing it derives a byte-identical context (and request
    /// bytes) to the same stream without it, whatever position it sits
    /// at and whatever its payload shape (constraint 3: the durable
    /// interrupt intent is audit data, never a context mutation).
    #[test]
    fn control_interrupt_leaves_model_context_unchanged() {
        let intent = json!({
            "action": "interrupt",
            "reason": "operator requested",
            "requested_by": "replica-a",
            "requested_at_ms": 1_762_000_000_000_u64,
            "target_epoch": 1
        });

        let mut with = started();
        with.user("hello");
        with.push(crate::events::EventKind::ControlInterrupt, intent);
        with.assistant(json!("working"));
        // The stale variant directly between context-bearing messages —
        // position must not matter, only the kind does.
        with.push(
            crate::events::EventKind::ControlInterrupt,
            json!({
                "action": "interrupt",
                "reason": null,
                "requested_by": null,
                "requested_at_ms": 1_762_000_000_001_u64,
                "target_epoch": null,
                "stale": true
            }),
        );

        let mut without = started();
        without.user("hello");
        without.assistant(json!("working"));

        let ctx_with = derive_model_context(&with.build()).unwrap();
        let ctx_without = derive_model_context(&without.build()).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(
            context_to_bytes(&ctx_with),
            context_to_bytes(&ctx_without),
            "control/interrupt must not change a single context/request byte"
        );

        // A message-shaped control payload is skipped unparsed — even a
        // payload that would be malformed for a context-bearing kind
        // must not error here.
        let mut shaped = started();
        shaped.user("q");
        shaped.push(
            crate::events::EventKind::ControlInterrupt,
            json!({"no_role": true}),
        );
        let ctx = derive_model_context(&shaped.build()).unwrap();
        assert_eq!(ctx, vec![user_msg("q")]);
    }

    /// `memory/note` is non-context-bearing — a stream containing
    /// notes derives a byte-identical context to the same stream
    /// without them (a note is a *claim* the dreamer drain appends,
    /// never a message; MEMORY-MODEL), whatever position they sit at
    /// and whatever their payload shape (constraint 3).
    #[test]
    fn memory_note_leaves_model_context_unchanged() {
        let note = json!({
            "subject": "deploys via the dsh bridge",
            "text": "Verify the environment before suggesting commands.",
            "status": "verified",
            "derived_from": [{
                "session": "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                "start_seq": 4,
                "end_seq": 38
            }],
            "verified_by": {"method": "transcript", "ref": "seq range [4,38)"}
        });
        let candidate = json!({
            "subject": "prefers concise replies",
            "text": "Keep answers short.",
            "status": "candidate",
            "derived_from": [{
                "session": "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                "start_seq": 4,
                "end_seq": 9
            }]
        });

        let mut with = started();
        with.user("hello");
        with.push(EventKind::MemoryNote, note);
        with.assistant(json!("working"));
        // A second note directly between context-bearing messages —
        // position must not matter, only the kind does.
        with.push(EventKind::MemoryNote, candidate);

        let mut without = started();
        without.user("hello");
        without.assistant(json!("working"));

        let ctx_with = derive_model_context(&with.build()).unwrap();
        let ctx_without = derive_model_context(&without.build()).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(
            context_to_bytes(&ctx_with),
            context_to_bytes(&ctx_without),
            "memory/note must not change a single context byte"
        );

        // A message-shaped note payload is skipped unparsed — even a
        // payload that would be malformed for a context-bearing kind
        // must not error here.
        let mut shaped = started();
        shaped.user("q");
        shaped.push(EventKind::MemoryNote, json!({"no_role": true}));
        let ctx = derive_model_context(&shaped.build()).unwrap();
        assert_eq!(ctx, vec![user_msg("q")]);
    }

    /// A2: assistant/message bytes are the truth — content passes through
    /// verbatim, byte-for-byte, however exotic its JSON shape.
    #[test]
    fn assistant_bytes_pass_through_verbatim() {
        let content = json!({
            "text": "héllo — wörld",
            "parts": [1, 2, {"x": null, "y": [true, false]}],
            "empty": "",
            "deep": {"a": {"b": {"c": [1.5, -0.25]}}},
        });
        let sent = serde_json::to_string(&content).unwrap();

        let mut b = started();
        b.assistant(content.clone());

        let ctx = derive_model_context(&b.build()).unwrap();
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].role, "assistant");
        assert_eq!(ctx[0].content, content);
        assert_eq!(serde_json::to_string(&ctx[0].content).unwrap(), sent);
    }

    // ------------------------------------------------------------------
    // Golden bytes (A2)
    // ------------------------------------------------------------------

    /// The canonical context byte form has an exact, frozen shape:
    /// serde_json compact output, message fields in declaration order
    /// (`role`, `content`), `synthetic` present only when true.
    #[test]
    fn context_bytes_have_frozen_golden_shape() {
        let messages = vec![
            user_msg("hi"),
            Message {
                role: "user".into(),
                content: json!("plan B"),
                synthetic: true,
            },
            assistant_msg(json!(42)),
        ];
        let golden = concat!(
            r#"[{"role":"user","content":"hi"},"#,
            r#"{"role":"user","content":"plan B","synthetic":true},"#,
            r#"{"role":"assistant","content":42}]"#,
        );
        assert_eq!(context_to_bytes(&messages), golden.as_bytes().to_vec());
    }

    /// A2 golden-bytes: the same stream yields byte-identical context on
    /// first pass, on rebuild, and on replay-from-export (each event
    /// serialized to JSONL and re-parsed, the export shape).
    #[test]
    fn golden_bytes_first_pass_rebuild_and_replay_from_export() {
        let mut b = started();
        b.user("q1");
        let a1 = b.assistant(json!({"answer": "a1"}));
        b.steer("plan B");
        b.tool_call(json!({"name": "fs.read", "arguments": {"path": "/tmp"}}));
        let tr = b.tool_result(json!({"content": "bytes"}));
        b.compact(a1, tr + 1, "summary of the middle");
        b.user("q2");
        b.assistant(json!("a2"));

        let events = b.build();
        let first = context_to_bytes(&derive_model_context(&events).unwrap());
        assert!(!first.is_empty());

        // Rebuild: a fresh pass over the same stream.
        let rebuild = context_to_bytes(&derive_model_context(&events).unwrap());
        assert_eq!(first, rebuild, "rebuild must be byte-identical");

        // Replay-from-export: JSONL round-trip through serde.
        let jsonl: Vec<String> = events
            .iter()
            .map(|ev| serde_json::to_string(ev).unwrap())
            .collect();
        let replayed: Vec<Event> = jsonl
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let replay = context_to_bytes(&derive_model_context(&replayed).unwrap());
        assert_eq!(first, replay, "replay-from-export must be byte-identical");
    }

    // ------------------------------------------------------------------
    // Compact (A3)
    // ------------------------------------------------------------------

    /// The skipped range is exactly `[a,b)`: `a` skipped, `b-1` skipped,
    /// `b` kept, everything before `a` kept, summary at `a`'s position.
    #[test]
    fn compact_skips_exactly_the_range() {
        let mut b = started();
        b.user("A"); // seq 2
        let asst_b = b.assistant(json!("B")); // seq 3
        b.user("C"); // seq 4 — inside the range too
        b.assistant(json!("D")); // seq 5 — first survivor after the range
        b.compact(asst_b, 5, "SUM"); // range [3,5)
        b.user("E"); // seq 7

        assert_eq!(
            derive_model_context(&b.build()).unwrap(),
            vec![
                user_msg("A"),
                assistant_msg(json!("SUM")),
                assistant_msg(json!("D")),
                user_msg("E"),
            ]
        );
    }

    /// A3: the unaffected prefix bytes are untouched — byte-prefix hash
    /// equality between the pre-compression context and the post-compression
    /// context, over every message originating before `from_seq`.
    #[test]
    fn compact_leaves_unaffected_prefix_bytes_untouched() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash(bytes: &[u8]) -> u64 {
            let mut h = DefaultHasher::new();
            bytes.hash(&mut h);
            h.finish()
        }

        let mut b = started();
        b.user("u1"); // seq 2
        b.assistant(json!("a1")); // seq 3
        let a2 = b.assistant(json!("a2")); // seq 4  <- from_seq
        b.assistant(json!("a3")); // seq 5
        b.user("u2"); // seq 6      <- to_seq (exclusive)
        let compact_at = b.compact(a2, 6, "compressed"); // seq 7
        b.user("u3"); // seq 8
        b.assistant(json!("a4")); // seq 9

        let events = b.build();

        // Pre-compression context: the stream truncated before the compact.
        let pre = derive_model_context(&events[..compact_at as usize]).unwrap();
        // Post-compression context: the full stream.
        let post = derive_model_context(&events).unwrap();

        // Messages originating before from_seq: the first two of each.
        let pre_prefix = context_to_bytes(&pre[..2]);
        let post_prefix = context_to_bytes(&post[..2]);
        assert_eq!(hash(&pre_prefix), hash(&post_prefix));
        assert_eq!(pre_prefix, post_prefix, "byte prefix must be identical");

        // And the compacted range is gone: exactly one summary; a2/a3
        // (seqs 4,5) skipped; u2 (seq 6 = to_seq, exclusive) survives.
        let rendered: Vec<String> = post
            .iter()
            .map(|m| serde_json::to_string(&m.content).unwrap())
            .collect();
        assert_eq!(
            rendered,
            vec![
                json!("u1").to_string(),
                json!("a1").to_string(),
                json!("compressed").to_string(),
                json!("u2").to_string(),
                json!("u3").to_string(),
                json!("a4").to_string(),
            ]
        );
    }

    /// Payloads inside the skipped range are never even parsed — skipping is
    /// positional, not interpretive.
    #[test]
    fn skipped_range_payloads_are_never_parsed() {
        let mut b = started();
        b.user("A");
        let bad = b.push(EventKind::AssistantMessage, json!({"no_role": true}));
        b.user("C");
        b.compact(bad, bad + 1, "SUM");
        b.user("E");

        assert_eq!(
            derive_model_context(&b.build()).unwrap(),
            vec![
                user_msg("A"),
                assistant_msg(json!("SUM")),
                user_msg("C"),
                user_msg("E"),
            ]
        );
    }

    #[test]
    fn compact_rejects_invalid_ranges() {
        // from >= to.
        let mut b = started();
        let s = b.user("A");
        let at = b.compact(s + 1, s, "SUM"); // [3,2)
        assert_eq!(
            derive_model_context(&b.build()),
            Err(ContextError::InvalidCompactRange {
                seq: at,
                from: s + 1,
                to: s,
            })
        );

        // Range extends past the compact event itself.
        let mut b = started();
        b.user("A"); // seq 2
        b.compact(2, 99, "SUM"); // to=99 > seq of the compact event (3)
        assert!(matches!(
            derive_model_context(&b.build()),
            Err(ContextError::CompactRangeNotInPast {
                seq: 3,
                from: 2,
                to: 99
            })
        ));

        // Overlapping ranges.
        let mut b = started();
        b.user("A"); // seq 2
        b.assistant(json!("B")); // seq 3
        b.user("C"); // seq 4
        b.compact(2, 4, "seg1"); // seq 5
        let at = b.compact(3, 5, "seg2"); // seq 6, overlaps [2,4)
        assert_eq!(
            derive_model_context(&b.build()),
            Err(ContextError::OverlappingCompactRange {
                seq: at,
                from: 3,
                to: 5,
                prev_from: 2,
                prev_to: 4,
            })
        );
    }

    /// Disjoint compacts apply at their own positions regardless of append
    /// order (a later append may compress an *older* range).
    #[test]
    fn multiple_disjoint_compacts_apply_in_range_order() {
        let mut b = started();
        b.user("u0"); // seq 2
        b.assistant(json!("a1")); // seq 3
        b.user("u1"); // seq 4
        b.assistant(json!("a2")); // seq 5
        b.user("u2"); // seq 6
        b.compact(5, 7, "NEW"); // seq 7 — the newer range, appended first
        b.compact(3, 5, "OLD"); // seq 8 — the older range, appended later

        assert_eq!(
            derive_model_context(&b.build()).unwrap(),
            vec![
                user_msg("u0"),
                assistant_msg(json!("OLD")),
                assistant_msg(json!("NEW")),
                // nothing after: seq 7,8 are the compacts themselves
            ]
        );
    }

    // ------------------------------------------------------------------
    // Synthetic messages (A6)
    // ------------------------------------------------------------------

    #[test]
    fn synthetic_messages_are_marked_and_strippable() {
        let mut b = started();
        b.user("real question"); // seq 2
        let steer_seq = b.steer("plan B"); // seq 3
        b.assistant(json!("answer")); // seq 4

        let events = b.build();
        let ctx = derive_model_context(&events).unwrap();
        assert_eq!(ctx.len(), 3);
        assert!(!ctx[0].synthetic);
        assert!(ctx[1].synthetic, "the steer message carries the marker");
        assert!(!ctx[2].synthetic);

        // Strip restores the "real" context — byte-identical to deriving
        // the stream with the synthetic event absent.
        let stripped = strip_synthetic(&ctx);
        assert_eq!(stripped.len(), 2);
        let without_steer: Vec<Event> = events
            .iter()
            .filter(|ev| ev.seq != steer_seq)
            .cloned()
            .collect();
        assert_eq!(
            context_to_bytes(&stripped),
            context_to_bytes(&derive_model_context(&without_steer).unwrap()),
            "synthetic-strip must restore the real context"
        );
    }

    // ------------------------------------------------------------------
    // Malformed payloads
    // ------------------------------------------------------------------

    #[test]
    fn malformed_payloads_are_named_errors() {
        let cases: [(Value, &str); 4] = [
            (
                json!({}),
                "payload must be a JSON object with 'role' and 'content'",
            ),
            (
                json!({"role": 3, "content": "x"}),
                "'role' must be a non-empty string",
            ),
            (
                json!({"role": "user"}),
                "payload must be a JSON object with 'role' and 'content'",
            ),
            (
                json!({"role": "user", "content": "x", "synthetic": "yes"}),
                "'synthetic' must be a boolean when present",
            ),
        ];
        for (payload, reason) in cases {
            let mut b = started();
            let at = b.push(EventKind::AssistantMessage, payload.clone());
            assert_eq!(
                derive_model_context(&b.build()),
                Err(ContextError::MalformedPayload {
                    seq: at,
                    kind: "assistant/message",
                    reason,
                }),
                "payload {payload}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Immutability
    // ------------------------------------------------------------------

    /// The projection output owns its data: mutating a derived message can
    /// never touch the historical events it came from.
    #[test]
    fn mutating_projection_output_cannot_touch_history() {
        let mut b = started();
        b.user("original");
        let events = b.build();

        let before = context_to_bytes(&derive_model_context(&events).unwrap());
        let mut ctx = derive_model_context(&events).unwrap();
        ctx[0].content = json!("TAMPERED");
        assert_eq!(
            serde_json::to_string(&events[2].bytes).unwrap(),
            serde_json::to_string(&json!({"role": "user", "content": "original"})).unwrap(),
            "the event must be untouched"
        );
        assert_eq!(
            context_to_bytes(&derive_model_context(&events).unwrap()),
            before,
            "re-deriving must still yield the original bytes"
        );
    }

    // ------------------------------------------------------------------
    // Provenance (READ-MODEL §5)
    // ------------------------------------------------------------------

    /// Provenance is position-aligned with the derived messages and
    /// carries each message's source event seq — derived by the same
    /// fold, so the two can never disagree.
    #[test]
    fn provenance_is_position_aligned_and_carries_source_seqs() {
        let mut b = started();
        let u1 = b.user("one");
        let u2 = b.user("two");
        let a1 = b.assistant(json!("mid"));
        let u3 = b.user("three");
        let events = b.build();

        let messages = derive_model_context(&events).unwrap();
        let prov = context_provenance(&events).unwrap();
        assert_eq!(messages.len(), prov.len());
        assert_eq!(prov, vec![u1, u2, a1, u3]);

        // Deterministic: the same stream, the same provenance.
        assert_eq!(context_provenance(&events).unwrap(), prov);
    }

    /// A compact summary's provenance is the compact event's own seq;
    /// compacted-away events contribute no provenance entries.
    #[test]
    fn provenance_of_a_compacted_context_sources_summaries_from_the_compact() {
        let mut b = started();
        let u1 = b.user("one");
        let u2 = b.user("two");
        let _u3 = b.user("three");
        let c = b.compact(u2, u2 + 2, "summarized");
        let u4 = b.user("four");
        let events = b.build();

        let messages = derive_model_context(&events).unwrap();
        let prov = context_provenance(&events).unwrap();
        // u1 survives; [u2, u3) compacted away; the summary sources from
        // the compact event; u4 follows.
        assert_eq!(prov, vec![u1, c, u4]);
        assert_eq!(messages.len(), prov.len());
        assert_eq!(messages[1].content, json!("summarized"));
    }

    /// The empty stream refusal is shared with `derive_model_context`.
    #[test]
    fn provenance_refuses_the_empty_stream_like_derive() {
        assert!(matches!(
            context_provenance(&[]),
            Err(ContextError::Envelope(ProjectionError::EmptyStream))
        ));
    }
}
