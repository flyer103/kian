//! The session token budget projection (ADR-054).
//!
//! Budgets are never authoritative state: the base is pinned on
//! `session/created`, raises are `budget/raised` events, and spend is the
//! provider usage carried by `turn/end`. This module folds that stream into
//! the one number the loop's pre-send gate needs.

use serde_json::Value;

use crate::events::{Event, EventKind};
use crate::model::Budgets;

/// Fraction of the token allowance withheld from ordinary turns so
/// maintenance work (compaction, ADR-057) stays affordable near the limit.
/// Without it the runtime can end up short of budget for the one call that
/// would have prevented a context overflow.
pub const DEFAULT_MAINTENANCE_RESERVE_PERCENT: u64 = 10;

/// The token usage one provider turn reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenUsage {
    /// Prompt/input tokens.
    pub input: u64,
    /// Completion/output tokens.
    pub output: u64,
}

impl TokenUsage {
    /// Total tokens billed for the turn.
    #[must_use]
    pub fn total(self) -> u64 {
        self.input.saturating_add(self.output)
    }
}

/// Parses the provider usage object under the two spellings the runtime
/// accepts (OpenAI chat completions and DeepSeek's native chat usage).
/// Returns `None` when neither side reports a count — an unknown usage is
/// never silently read as zero spend.
#[must_use]
pub fn usage_tokens(usage: &Value) -> Option<TokenUsage> {
    let input = first_u64(usage, &["prompt_tokens", "input_tokens"]);
    let output = first_u64(usage, &["completion_tokens", "output_tokens"]);
    if input.is_none() && output.is_none() {
        return None;
    }
    Some(TokenUsage {
        input: input.unwrap_or(0),
        output: output.unwrap_or(0),
    })
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

/// The folded budget facts of one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetProjection {
    /// The base token allowance pinned at creation (or the loop's agent
    /// fallback). `None` = no token budget: the gate always passes.
    pub tokens_limit: Option<u64>,
    /// Tokens added by `budget/raised` events.
    pub tokens_raised: u64,
    /// Tokens reported spent by `turn/end` usage.
    pub tokens_spent: u64,
    /// The base wall-clock allowance; recorded and raised, never enforced
    /// (ADR-054: idle waiting for a human must not consume a turn budget).
    pub wall_clock_limit_ms: Option<u64>,
    /// Wall-clock milliseconds added by raises.
    pub wall_clock_raised_ms: u64,
}

impl BudgetProjection {
    /// Remaining tokens after raises and known spend, or `None` when the
    /// session has no token budget.
    #[must_use]
    pub fn remaining_tokens(&self) -> Option<u64> {
        self.tokens_limit.map(|limit| {
            limit
                .saturating_add(self.tokens_raised)
                .saturating_sub(self.tokens_spent)
        })
    }

    /// Gross token allowance (base plus raises) before spend, or `None`
    /// when the session has no token budget.
    #[must_use]
    pub fn allowance_tokens(&self) -> Option<u64> {
        self.tokens_limit
            .map(|limit| limit.saturating_add(self.tokens_raised))
    }

    /// Tokens withheld from ordinary turns for maintenance work.
    #[must_use]
    pub fn maintenance_reserve_tokens(&self) -> Option<u64> {
        self.allowance_tokens()
            .map(|allowance| allowance.saturating_mul(DEFAULT_MAINTENANCE_RESERVE_PERCENT) / 100)
    }

    /// Whether an estimated turn size (prompt upper bound plus declared
    /// output cap) would cross the remaining allowance.
    #[must_use]
    pub fn would_exceed_tokens(&self, estimate: u64) -> bool {
        self.remaining_tokens()
            .is_some_and(|remaining| estimate > remaining)
    }

    /// Whether an estimate would cross what ordinary turns may spend: the
    /// remaining allowance minus the maintenance reserve. Maintenance calls
    /// themselves use `Self::would_exceed_tokens` and may draw the reserve.
    #[must_use]
    pub fn would_exceed_tokens_reserving(&self, estimate: u64, reserve: u64) -> bool {
        self.remaining_tokens()
            .is_some_and(|remaining| estimate > remaining.saturating_sub(reserve))
    }
}

/// Folds `session/created.budgets` (when pinned), `budget/raised` and
/// `turn/end.usage`.
///
/// A pinned `budgets` object is authoritative even when its fields are
/// `null` (an explicit "no budget" pin). A stream without the pin falls
/// back to the loop's own agent definition — the pre-pin compatibility path.
#[must_use]
pub fn project_budget(events: &[Event], fallback: &Budgets) -> BudgetProjection {
    let pinned = events
        .iter()
        .find(|event| matches!(event.kind, EventKind::SessionCreated))
        .and_then(|event| event.bytes.get("budgets"))
        .and_then(Value::as_object);
    let (tokens_limit, wall_clock_limit_ms) = match pinned {
        Some(budgets) => (
            budgets.get("tokens").and_then(Value::as_u64),
            budgets.get("wall_clock_ms").and_then(Value::as_u64),
        ),
        None => (fallback.tokens, fallback.wall_clock_ms),
    };

    let mut projection = BudgetProjection {
        tokens_limit,
        wall_clock_limit_ms,
        ..BudgetProjection::default()
    };
    for event in events {
        match event.kind {
            EventKind::BudgetRaised => {
                projection.tokens_raised = projection.tokens_raised.saturating_add(
                    event
                        .bytes
                        .get("tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
                projection.wall_clock_raised_ms = projection.wall_clock_raised_ms.saturating_add(
                    event
                        .bytes
                        .get("wall_clock_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
            }
            EventKind::TurnEnd => {
                if let Some(usage) = event.bytes.get("usage").and_then(usage_tokens) {
                    projection.tokens_spent = projection.tokens_spent.saturating_add(usage.total());
                }
            }
            _ => {}
        }
    }
    projection
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::events::{EndReason, SCHEMA_VERSION};
    use crate::model::{Epoch, SessionId};

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

    fn budgets(tokens: Option<u64>) -> Budgets {
        Budgets {
            tokens,
            wall_clock_ms: None,
        }
    }

    #[test]
    fn empty_stream_uses_the_agent_fallback() {
        let projection = project_budget(&[], &budgets(Some(1_000)));
        assert_eq!(projection.tokens_limit, Some(1_000));
        assert_eq!(projection.remaining_tokens(), Some(1_000));
    }

    #[test]
    fn a_pinned_budgets_object_overrides_the_fallback_even_when_null() {
        let events = vec![event(
            0,
            EventKind::SessionCreated,
            json!({"budgets": {"tokens": null, "wall_clock_ms": null}}),
        )];
        let projection = project_budget(&events, &budgets(Some(1_000)));
        assert_eq!(projection.tokens_limit, None);
        assert_eq!(projection.remaining_tokens(), None);
    }

    #[test]
    fn raises_and_turn_spend_fold_into_the_remaining_allowance() {
        let events = vec![
            event(
                0,
                EventKind::SessionCreated,
                json!({"budgets": {"tokens": 1_000}}),
            ),
            event(1, EventKind::BudgetRaised, json!({"tokens": 500})),
            event(
                2,
                EventKind::TurnEnd,
                json!({"usage": {"prompt_tokens": 300, "completion_tokens": 100}}),
            ),
            event(
                3,
                EventKind::TurnEnd,
                json!({"usage": {"input_tokens": 50, "output_tokens": 25}}),
            ),
        ];
        let projection = project_budget(&events, &budgets(None));
        assert_eq!(projection.tokens_limit, Some(1_000));
        assert_eq!(projection.tokens_raised, 500);
        assert_eq!(projection.tokens_spent, 475);
        assert_eq!(projection.remaining_tokens(), Some(1_025));
    }

    #[test]
    fn usage_without_a_recognized_spelling_is_unknown_not_zero() {
        assert_eq!(usage_tokens(&json!({"total_tokens": 9})), None);
        assert_eq!(
            usage_tokens(&json!({"prompt_tokens": 4, "completion_tokens": 6})),
            Some(TokenUsage {
                input: 4,
                output: 6
            })
        );
        assert_eq!(
            usage_tokens(&json!({"input_tokens": 4})),
            Some(TokenUsage {
                input: 4,
                output: 0
            })
        );
    }

    #[test]
    fn the_gate_compares_the_worst_case_against_remaining_tokens() {
        let events = vec![
            event(
                0,
                EventKind::SessionCreated,
                json!({"budgets": {"tokens": 1_000}}),
            ),
            event(
                1,
                EventKind::TurnEnd,
                json!({"usage": {"prompt_tokens": 300, "completion_tokens": 100}}),
            ),
        ];
        let projection = project_budget(&events, &budgets(None));
        assert!(!projection.would_exceed_tokens(600));
        assert!(projection.would_exceed_tokens(601));
    }

    #[test]
    fn ordinary_turns_reserve_a_share_for_maintenance() {
        let events = vec![
            event(
                0,
                EventKind::SessionCreated,
                json!({"budgets": {"tokens": 1_000}}),
            ),
            event(
                1,
                EventKind::TurnEnd,
                json!({"usage": {"prompt_tokens": 800, "completion_tokens": 50}}),
            ),
        ];
        let projection = project_budget(&events, &budgets(None));
        assert_eq!(projection.allowance_tokens(), Some(1_000));
        assert_eq!(projection.maintenance_reserve_tokens(), Some(100));
        // Remaining is 150: ordinary turns may spend 50, maintenance may
        // still draw the reserved 100.
        assert!(!projection.would_exceed_tokens_reserving(50, 100));
        assert!(projection.would_exceed_tokens_reserving(51, 100));
        assert!(!projection.would_exceed_tokens(150));
        assert!(projection.would_exceed_tokens(151));
    }

    #[test]
    fn an_unbudgeted_session_never_reserves_or_blocks() {
        let projection = project_budget(&[], &budgets(None));
        assert_eq!(projection.allowance_tokens(), None);
        assert_eq!(projection.maintenance_reserve_tokens(), None);
        assert!(!projection.would_exceed_tokens_reserving(u64::MAX, 10));
    }

    #[test]
    fn raises_enlarge_the_allowance_and_with_it_the_reserve() {
        let events = vec![
            event(
                0,
                EventKind::SessionCreated,
                json!({"budgets": {"tokens": 1_000}}),
            ),
            event(1, EventKind::BudgetRaised, json!({"tokens": 1_000})),
        ];
        let projection = project_budget(&events, &budgets(None));
        assert_eq!(projection.allowance_tokens(), Some(2_000));
        assert_eq!(projection.maintenance_reserve_tokens(), Some(200));
    }

    #[test]
    fn budget_raised_is_the_durable_kind_name() {
        assert_eq!(EventKind::BudgetRaised.as_str(), "budget/raised");
        let projection = project_budget(
            &[event(1, EventKind::BudgetRaised, json!({"tokens": 10}))],
            &budgets(Some(0)),
        );
        assert_eq!(projection.remaining_tokens(), Some(10));
        // The end-reason spelling is part of the frozen wire vocabulary.
        assert_eq!(
            serde_json::to_string(&EndReason::AwaitingBudget).unwrap(),
            "\"awaiting_budget\""
        );
    }
}
