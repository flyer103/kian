//! Off-peak deferral (ADR-059, P4): the pure planning half of
//! deferred admissions.
//!
//! Deferral is not a scheduler. The planner here answers one question -
//! given this policy and this instant, what eligibility instant should
//! this durable inbox row carry, and why - and the runtime's existing
//! eligible_at gate does the rest (the store refuses to hand out a row
//! before that instant; the due-wake sweep wakes sessions when their rows
//! become due). The price book is the same one the cost projection reads
//! (ADR-052), so peak windows and holidays have exactly one definition.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::pricing::PriceBook;

/// The wall-clock deferral policy one admission can request (ADR-059).
///
/// Interactive inputs (CLI runs, channel messages, steer) are always
/// Immediate; only explicit opt-ins - the API defer parameter and a cron
/// schedule's defer: off_peak - defer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeferPolicy {
    /// Admit now; the executor runs it as soon as it can.
    #[default]
    Immediate,
    /// Hold the durable row until the next off-peak instant (batch work).
    OffPeak,
}

impl DeferPolicy {
    /// Parses the wire spelling of a policy (the API defer value).
    /// Returns None for anything else: an out-of-contract policy is a
    /// client error, never a silent immediate admit.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "immediate" => Some(Self::Immediate),
            "off_peak" => Some(Self::OffPeak),
            _ => None,
        }
    }

    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::OffPeak => "off_peak",
        }
    }
}

/// The resolved deferral of one admission: the instant it becomes
/// eligible and the human-readable reason (durable in the admitted row
/// when the admission is deferred).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeferPlan {
    /// The policy the caller asked for.
    pub policy: DeferPolicy,
    /// The eligibility instant written on the inbox row.
    pub eligible_at_ms: u64,
    /// True when the row is held until eligible_at_ms.
    pub deferred: bool,
    /// Why this instant was chosen (recorded for the why-didn't-wait
    /// audit; never a promise of execution time).
    pub reason: String,
}

impl DeferPlan {
    /// An admission that runs now, with the honest reason it did not wait.
    #[must_use]
    pub fn immediate(policy: DeferPolicy, now_ms: u64, reason: impl Into<String>) -> Self {
        Self {
            policy,
            eligible_at_ms: now_ms,
            deferred: false,
            reason: reason.into(),
        }
    }
}

/// The off-peak planner: the price book's peak calendar plus the
/// operator's wait cap (ADR-059). A plan is pure - no clock read, no
/// store access.
#[derive(Debug, Clone)]
pub struct DeferCalendar {
    book: Arc<PriceBook>,
    max_wait_ms: u64,
}

impl DeferCalendar {
    /// A calendar over book that never waits longer than max_wait_ms.
    #[must_use]
    pub fn new(book: Arc<PriceBook>, max_wait_ms: u64) -> Self {
        Self { book, max_wait_ms }
    }

    /// The price book this calendar plans against.
    #[must_use]
    pub fn book(&self) -> &PriceBook {
        &self.book
    }

    /// The operator's wait cap, milliseconds.
    #[must_use]
    pub fn max_wait_ms(&self) -> u64 {
        self.max_wait_ms
    }

    /// Plans one admission at now_ms: off peak -> now; on peak -> the
    /// next off-peak instant when it is within the cap, else now with the
    /// cap recorded as the why-didn't-wait reason.
    #[must_use]
    pub fn plan(&self, policy: DeferPolicy, now_ms: u64) -> DeferPlan {
        match policy {
            DeferPolicy::Immediate => DeferPlan::immediate(policy, now_ms, "policy immediate"),
            DeferPolicy::OffPeak => match self.book.next_off_peak_ms(now_ms) {
                Some(at) if at <= now_ms => {
                    DeferPlan::immediate(policy, now_ms, "already off peak")
                }
                Some(at) => {
                    let wait = at.saturating_sub(now_ms);
                    if wait <= self.max_wait_ms {
                        DeferPlan {
                            policy,
                            eligible_at_ms: at,
                            deferred: true,
                            reason: format!("next off-peak at {at} ({wait} ms away)"),
                        }
                    } else {
                        DeferPlan::immediate(
                            policy,
                            now_ms,
                            format!(
                                "next off-peak at {at} is {wait} ms away, beyond defer.max_wait_ms {}: running now",
                                self.max_wait_ms
                            ),
                        )
                    }
                }
                None => DeferPlan::immediate(
                    policy,
                    now_ms,
                    "no off-peak instant inside the scan horizon",
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeferCalendar, DeferPlan, DeferPolicy};
    use crate::pricing::{PeakWindowConfig, PriceEntryConfig, PricingConfig, WeekdayName};
    use std::sync::Arc;

    fn book() -> Arc<crate::pricing::PriceBook> {
        let config = PricingConfig {
            models: [(
                "deepseek/deepseek-flash".to_string(),
                PriceEntryConfig {
                    cache_hit: 0.003,
                    cache_miss: 0.15,
                    output: 0.6,
                },
            )]
            .into_iter()
            .collect(),
            peak_windows: vec![PeakWindowConfig {
                weekdays: vec![WeekdayName::Mon],
                start: "01:00".to_string(),
                end: "04:00".to_string(),
                timezone: "UTC".to_string(),
            }],
            ..PricingConfig::default()
        };
        Arc::new(config.book().expect("book"))
    }

    fn at(minute: u64) -> u64 {
        1_767_571_200_000 + minute * 60_000
    }

    #[test]
    fn immediate_never_defers() {
        let calendar = DeferCalendar::new(book(), 86_400_000);
        let plan = calendar.plan(DeferPolicy::Immediate, at(120));
        assert_eq!(plan.policy, DeferPolicy::Immediate);
        assert!(!plan.deferred);
        assert_eq!(plan.eligible_at_ms, at(120));
        assert_eq!(plan.reason, "policy immediate");
    }

    #[test]
    fn off_peak_inside_the_window_waits_for_the_next_boundary() {
        let calendar = DeferCalendar::new(book(), 86_400_000);
        let plan = calendar.plan(DeferPolicy::OffPeak, at(150));
        assert!(plan.deferred);
        assert_eq!(plan.eligible_at_ms, at(240));
        assert_eq!(
            plan.reason,
            format!("next off-peak at {} (5400000 ms away)", at(240))
        );
    }

    #[test]
    fn the_wait_cap_runs_now_and_records_why() {
        let calendar = DeferCalendar::new(book(), 60_000);
        let plan = calendar.plan(DeferPolicy::OffPeak, at(150));
        assert!(!plan.deferred);
        assert_eq!(plan.eligible_at_ms, at(150));
        assert!(plan.reason.contains("beyond defer.max_wait_ms 60000"));
    }

    #[test]
    fn already_off_peak_needs_no_wait() {
        let calendar = DeferCalendar::new(book(), 86_400_000);
        let plan = calendar.plan(DeferPolicy::OffPeak, at(300));
        assert!(!plan.deferred);
        assert_eq!(plan.reason, "already off peak");
        assert_eq!(plan.eligible_at_ms, at(300));
    }

    #[test]
    fn policy_round_trips_through_its_wire_spelling() {
        assert_eq!(DeferPolicy::parse("off_peak"), Some(DeferPolicy::OffPeak));
        assert_eq!(
            DeferPolicy::parse("immediate"),
            Some(DeferPolicy::Immediate)
        );
        assert_eq!(DeferPolicy::parse("OFF_PEAK"), None);
        assert_eq!(DeferPolicy::OffPeak.as_str(), "off_peak");
        let wire = serde_json::to_string(&DeferPolicy::OffPeak).expect("serialize");
        assert!(wire.contains("off_peak"));
    }

    #[test]
    fn the_cap_is_inclusive_and_a_plan_serializes_its_reason() {
        let calendar = DeferCalendar::new(book(), 5_400_000);
        let plan = calendar.plan(DeferPolicy::OffPeak, at(150));
        assert!(plan.deferred);
        let wire = serde_json::to_value(&plan).expect("serialize");
        assert_eq!(wire["policy"], "off_peak");
        assert_eq!(wire["deferred"], true);
        assert_eq!(wire["eligible_at_ms"], at(240));
        assert!(
            wire["reason"]
                .as_str()
                .expect("reason")
                .contains("next off-peak")
        );
        assert_eq!(
            DeferPlan::immediate(DeferPolicy::Immediate, 7, "x").eligible_at_ms,
            7
        );
    }
}
