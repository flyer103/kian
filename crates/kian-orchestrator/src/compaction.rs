//! Compaction planner (ADR-058).
//!
//! Compression is the one deliberate prefix-invalidation point (constraint
//! 3): everything else in the model context is immutable history. This
//! module is the pure decision half — given the committed stream and the
//! pressure the next request would put on the context window, choose the
//! oldest contiguous range to fold into one summary, or refuse with a
//! documented reason.
//!
//! It writes nothing and calls no provider. The loop owns the request that
//! produces the summary and the compact append (persist-what-you-send: the
//! summary request is itself a step/request with purpose compact); the
//! engine owns the drain that carries it. Range selection is deterministic
//! because it is derived from the stream alone (constraint 1): the same
//! events and settings yield the same plan.
//!
//! Economics (approved P3 semantics): compact only under context pressure,
//! never to save money. At a 1M window with cache-hit pricing an eager
//! compression usually costs more than the prefix it invalidates; the
//! planner therefore refuses without a pressure reading.

use kian_core::context::{ContextError, Message, context_provenance, derive_model_context};
use kian_core::events::{Event, EventKind};

/// Keep this many most-recent provider turns untouched.
pub const DEFAULT_KEEP_RECENT_TURNS: usize = 4;

/// A fold that would reclaim fewer tokens than this is not worth a
/// provider call (and another deliberate prefix invalidation).
pub const DEFAULT_MIN_RECLAIM_TOKENS: u64 = 1_024;

/// Start considering compaction once the next request's worst-case token
/// estimate reaches this percentage of the declared context window.
pub const DEFAULT_TRIGGER_PERCENT: u64 = 90;

/// Bytes per token used by the planner's conservative arithmetic. It sits
/// inside the chars_v1 estimator's [len/5, len/3 + 256] envelope and
/// deliberately under-states reclaimed tokens (a fold that looks large
/// enough here is certainly large enough).
const BYTES_PER_TOKEN: u64 = 4;

/// Estimated summary size as a fraction of the folded transcript
/// (folded_bytes / 8), capped so a huge fold cannot pretend to compress to
/// nothing.
const SUMMARY_DIVISOR: u64 = 8;

/// The plan-time knobs. Immutable per allocation; the loop carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionSettings {
    /// Most-recent turns kept verbatim.
    pub keep_recent_turns: usize,
    /// Minimum estimate of reclaimed tokens for a plan to be worth it.
    pub min_reclaim_tokens: u64,
    /// Pressure percentage (1..=100) of the declared context window.
    pub trigger_percent: u64,
}

impl Default for CompactionSettings {
    fn default() -> Self {
        Self {
            keep_recent_turns: DEFAULT_KEEP_RECENT_TURNS,
            min_reclaim_tokens: DEFAULT_MIN_RECLAIM_TOKENS,
            trigger_percent: DEFAULT_TRIGGER_PERCENT,
        }
    }
}

impl CompactionSettings {
    /// Clamps to the meaningful domain so a config typo cannot disable the
    /// planner silently (keep at least one turn, trigger in 1..=100).
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            keep_recent_turns: self.keep_recent_turns.max(1),
            min_reclaim_tokens: self.min_reclaim_tokens.max(1),
            trigger_percent: self.trigger_percent.clamp(1, 100),
        }
    }
}

/// The pressure reading for the next request: what it would cost against
/// the declared window. incoming_estimate_tokens already includes the
/// output reserve — the worst case the request implies, never the best.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPressure {
    /// The model profile's declared context window.
    pub context_window_tokens: u64,
    /// Worst-case tokens the next request would occupy.
    pub incoming_estimate_tokens: u64,
}

impl CompactionPressure {
    /// Whether the next request is close enough to the window to compact
    /// before sending it.
    #[must_use]
    pub fn triggers(&self, settings: &CompactionSettings) -> bool {
        if self.context_window_tokens == 0 {
            return false;
        }
        self.incoming_estimate_tokens.saturating_mul(100)
            >= self
                .context_window_tokens
                .saturating_mul(settings.trigger_percent)
    }
}

/// Why compaction is being considered for this stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionTrigger {
    /// The next request would occupy too much of the declared window.
    Pressure(CompactionPressure),
    /// The provider already rejected a request as context overflow.
    Overflow,
    /// The provider refused a request as too large for its byte bound
    /// (ADR-068). Distinct from `Overflow` so the fold's reason names the
    /// byte verdict instead of claiming a window overflow.
    Oversized,
}

impl CompactionTrigger {
    /// Whether this trigger authorizes a fold under the given settings.
    #[must_use]
    pub fn authorizes(&self, settings: &CompactionSettings) -> bool {
        match self {
            Self::Pressure(pressure) => pressure.triggers(settings),
            Self::Overflow | Self::Oversized => true,
        }
    }

    fn reason(&self, settings: &CompactionSettings) -> String {
        match self {
            Self::Pressure(pressure) => format!(
                "context pressure: estimate {} >= {}% of window {}",
                pressure.incoming_estimate_tokens,
                settings.trigger_percent,
                pressure.context_window_tokens
            ),
            Self::Overflow => "provider reported context overflow".to_string(),
            Self::Oversized => "provider refused the request as too large (ADR-068)".to_string(),
        }
    }
}

/// One selected fold. [from_seq, to_seq) is the event range the compact
/// event will skip; the summary takes its place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    /// First event seq folded (inclusive).
    pub from_seq: u64,
    /// First event seq kept (exclusive end of the fold).
    pub to_seq: u64,
    /// How many projected messages the fold removes.
    pub folded_messages: usize,
    /// Bytes of those messages (the planner's evidence for its estimate).
    pub folded_bytes: u64,
    /// Conservative token estimate the fold frees.
    pub reclaimed_tokens: u64,
    /// Conservative token estimate of the post-compaction context.
    pub expected_after_tokens: u64,
    /// Why this plan exists (pressure reading).
    pub reason: String,
}

/// Chooses the oldest safe fold for this stream.
///
/// Rules (all derived from the stream, ADR-058):
/// - keep the last keep_recent_turns turns verbatim;
/// - never overlap an existing compact range and start after the last one,
///   so repeated planning advances monotonically;
/// - fold only messages that exist, and require a meaningful reclaim;
/// - without a trigger (or with a window this stream cannot pressure) refuse
///   — compression is not a cost-saving tactic.
pub fn plan_compaction(
    events: &[Event],
    settings: &CompactionSettings,
    trigger: Option<&CompactionTrigger>,
) -> Result<Option<CompactionPlan>, ContextError> {
    let settings = settings.normalized();
    if events.len() < 2 {
        return Ok(None);
    }
    let Some(trigger) = trigger else {
        return Ok(None);
    };
    if !trigger.authorizes(&settings) {
        return Ok(None);
    }

    let messages = derive_model_context(events)?;
    let provenance = context_provenance(events)?;
    let turn_starts: Vec<u64> = events
        .iter()
        .filter(|event| matches!(event.kind, EventKind::TurnStart))
        .map(|event| event.seq)
        .collect();
    if turn_starts.len() <= settings.keep_recent_turns {
        return Ok(None);
    }
    let to_seq = turn_starts[turn_starts.len() - settings.keep_recent_turns];

    let last_compacted_to = events
        .iter()
        .filter(|event| matches!(event.kind, EventKind::Compact))
        .filter_map(|event| {
            event
                .bytes
                .get("to_seq")
                .and_then(serde_json::Value::as_u64)
        })
        .max();
    let from_seq = match last_compacted_to {
        Some(to) => to,
        None => events.first().map(|event| event.seq).unwrap_or(0),
    };
    if from_seq >= to_seq {
        return Ok(None);
    }

    let mut folded_messages = 0usize;
    let mut folded_bytes = 0u64;
    let mut total_bytes = 0u64;
    for (message, src) in messages.iter().zip(provenance.iter()) {
        let bytes = message_bytes(message);
        total_bytes = total_bytes.saturating_add(bytes);
        if (from_seq..to_seq).contains(src) {
            folded_messages += 1;
            folded_bytes = folded_bytes.saturating_add(bytes);
        }
    }
    if folded_messages == 0 || folded_bytes == 0 {
        return Ok(None);
    }

    let reclaimed_tokens = folded_bytes / BYTES_PER_TOKEN;
    if reclaimed_tokens < settings.min_reclaim_tokens {
        return Ok(None);
    }
    let summary_bytes = (folded_bytes / SUMMARY_DIVISOR).min(folded_bytes);
    let remaining_bytes = total_bytes
        .saturating_sub(folded_bytes)
        .saturating_add(summary_bytes);
    let expected_after_tokens = remaining_bytes / BYTES_PER_TOKEN + 256;
    let reason = trigger.reason(&settings);

    Ok(Some(CompactionPlan {
        from_seq,
        to_seq,
        folded_messages,
        folded_bytes,
        reclaimed_tokens,
        expected_after_tokens,
        reason,
    }))
}

/// Deterministic byte size of one projected message (role + serialized
/// content) — the same quantity the chars_v1 estimator is built on.
#[must_use]
pub fn message_bytes(message: &Message) -> u64 {
    let content = serde_json::to_string(&message.content)
        .map(|s| s.len())
        .unwrap_or(0);
    (message.role.len() + content) as u64
}

/// Worst-case token estimate of a projected context under chars_v1's high
/// bound — what the loop feeds CompactionPressure.
#[must_use]
pub fn estimate_context_tokens(messages: &[Message]) -> u64 {
    let bytes: u64 = messages.iter().map(message_bytes).sum();
    bytes / 3 + 256
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::{Epoch, SessionId};

    fn event(seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(1),
            ts: 1_761_900_000_000,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    struct Stream {
        seq: u64,
        events: Vec<Event>,
    }

    impl Stream {
        fn new() -> Self {
            let mut stream = Self {
                seq: 1,
                events: Vec::new(),
            };
            stream.push(EventKind::SessionCreated, json!({}));
            stream.push(EventKind::DrainClaimed, json!({}));
            stream
        }

        fn push(&mut self, kind: EventKind, bytes: Value) -> u64 {
            let seq = self.seq;
            self.seq += 1;
            self.events.push(event(seq, kind, bytes));
            seq
        }

        fn turn(&mut self, text: &str) -> (u64, u64) {
            let start = self.push(EventKind::TurnStart, json!({"iteration": 1}));
            self.push(
                EventKind::InputAdmitted,
                json!({"role": "user", "content": text}),
            );
            self.push(
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": text}),
            );
            self.push(EventKind::TurnEnd, json!({"iteration": 1}));
            (start, self.seq - 1)
        }
    }

    fn pressure(incoming: u64, window: u64) -> CompactionTrigger {
        CompactionTrigger::Pressure(CompactionPressure {
            context_window_tokens: window,
            incoming_estimate_tokens: incoming,
        })
    }

    #[test]
    fn refuses_without_pressure_or_messages() {
        let settings = CompactionSettings::default();
        assert_eq!(plan_compaction(&[], &settings, None).unwrap(), None);
        let mut stream = Stream::new();
        stream.turn("one");
        assert_eq!(
            plan_compaction(&stream.events, &settings, None).unwrap(),
            None
        );
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        assert_eq!(
            plan_compaction(&stream.events, &settings, None).unwrap(),
            None
        );
    }

    #[test]
    fn folds_old_turns_and_keeps_the_most_recent_ones() {
        let mut stream = Stream::new();
        let mut starts = Vec::new();
        for i in 0..8 {
            starts.push(stream.turn(&format!("turn {i}")).0);
        }
        let settings = CompactionSettings {
            keep_recent_turns: 3,
            min_reclaim_tokens: 1,
            trigger_percent: 50,
        };
        let plan = plan_compaction(&stream.events, &settings, Some(&pressure(9_000, 10_000)))
            .unwrap()
            .expect("plan");
        assert_eq!(plan.from_seq, stream.events[0].seq);
        assert_eq!(plan.to_seq, starts[8 - 3]);
        assert_eq!(plan.folded_messages, 10, "ten folded messages");
        assert!(plan.folded_bytes > 0);
        assert!(plan.reclaimed_tokens >= settings.min_reclaim_tokens);
        assert!(plan.expected_after_tokens > 0);
        assert!(plan.reason.contains("context pressure"));
    }

    #[test]
    fn a_replan_starts_after_the_last_compaction() {
        let mut stream = Stream::new();
        let mut starts = Vec::new();
        for i in 0..10 {
            starts.push(stream.turn(&format!("turn {i}")).0);
        }
        let settings = CompactionSettings {
            keep_recent_turns: 4,
            min_reclaim_tokens: 1,
            trigger_percent: 50,
        };
        let first = plan_compaction(&stream.events, &settings, Some(&pressure(9_000, 10_000)))
            .unwrap()
            .expect("first plan");
        assert_eq!(first.to_seq, starts[6]);
        stream.push(
            EventKind::Compact,
            json!({
                "from_seq": first.from_seq,
                "to_seq": first.to_seq,
                "summary": {"role": "assistant", "content": {"text": "summary"}},
            }),
        );
        for i in 10..14 {
            starts.push(stream.turn(&format!("turn {i}")).0);
        }
        let second = plan_compaction(&stream.events, &settings, Some(&pressure(9_000, 10_000)))
            .unwrap()
            .expect("second plan");
        assert_eq!(second.from_seq, first.to_seq, "no overlap, no re-fold");
        assert_eq!(second.to_seq, starts[14 - 4]);
        assert!(second.to_seq > second.from_seq);
    }

    #[test]
    fn refuses_when_the_reclaim_is_too_small() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let settings = CompactionSettings {
            keep_recent_turns: 2,
            min_reclaim_tokens: u64::MAX,
            trigger_percent: 50,
        };
        assert_eq!(
            plan_compaction(&stream.events, &settings, Some(&pressure(9_000, 10_000))).unwrap(),
            None
        );
    }

    #[test]
    fn pressure_gate_uses_the_configured_percentage() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let settings = CompactionSettings {
            keep_recent_turns: 2,
            min_reclaim_tokens: 1,
            trigger_percent: 90,
        };
        assert_eq!(
            plan_compaction(&stream.events, &settings, Some(&pressure(8_999, 10_000))).unwrap(),
            None
        );
        assert!(
            plan_compaction(&stream.events, &settings, Some(&pressure(9_000, 10_000)))
                .unwrap()
                .is_some()
        );
        assert_eq!(
            plan_compaction(&stream.events, &settings, Some(&pressure(1, 0))).unwrap(),
            None
        );
    }

    #[test]
    fn overflow_trigger_authorizes_a_fold_without_a_window_reading() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let settings = CompactionSettings {
            keep_recent_turns: 3,
            min_reclaim_tokens: 1,
            trigger_percent: 50,
        };
        let plan = plan_compaction(
            &stream.events,
            &settings,
            Some(&CompactionTrigger::Overflow),
        )
        .unwrap()
        .expect("overflow authorizes a fold");
        assert_eq!(plan.reason, "provider reported context overflow");
        assert_eq!(plan.folded_messages, 10);
    }

    #[test]
    fn oversized_refusal_authorizes_a_fold_and_names_the_byte_verdict() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let settings = CompactionSettings {
            keep_recent_turns: 3,
            min_reclaim_tokens: 1,
            trigger_percent: 50,
        };
        let plan = plan_compaction(
            &stream.events,
            &settings,
            Some(&CompactionTrigger::Oversized),
        )
        .unwrap()
        .expect("a byte refusal authorizes a fold");
        assert_eq!(
            plan.reason,
            "provider refused the request as too large (ADR-068)"
        );
        assert_eq!(plan.folded_messages, 10);
    }

    #[test]
    fn refuses_when_too_few_turns_exist_to_keep_the_window() {
        let mut stream = Stream::new();
        stream.turn("only");
        let settings = CompactionSettings {
            keep_recent_turns: 4,
            min_reclaim_tokens: 1,
            trigger_percent: 50,
        };
        assert_eq!(
            plan_compaction(
                &stream.events,
                &settings,
                Some(&pressure(999_000, 1_000_000))
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn planning_is_pure_and_repeatable() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let settings = CompactionSettings {
            keep_recent_turns: 4,
            min_reclaim_tokens: 1,
            trigger_percent: 90,
        };
        let reading = pressure(9_500, 10_000);
        let first = plan_compaction(&stream.events, &settings, Some(&reading)).unwrap();
        let second = plan_compaction(&stream.events, &settings, Some(&reading)).unwrap();
        assert_eq!(first, second);
        assert!(first.is_some());
    }

    #[test]
    fn malformed_history_is_an_error_not_a_guess() {
        let mut stream = Stream::new();
        for i in 0..8 {
            stream.turn(&format!("turn {i}"));
        }
        let mut events = stream.events;
        events[2].seq = events[1].seq;
        let settings = CompactionSettings::default();
        assert!(plan_compaction(&events, &settings, Some(&pressure(9_500, 10_000))).is_err());
    }

    #[test]
    fn context_estimate_matches_the_chars_v1_high_bound() {
        let message = Message {
            role: "user".into(),
            content: json!("abcd"),
            synthetic: false,
        };
        assert_eq!(message_bytes(&message), 10);
        assert_eq!(estimate_context_tokens(&[message]), 10 / 3 + 256);
    }
}
