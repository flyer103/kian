//! Lateness classification and the `catch_up` policy (CRON-MODEL §5.2, §5.3), plus the
//! `{schedule_id}@{fire_time}` slot key (§9.1).
//!
//! One tick reads `now` once and asks [`plan_fire`] what to do; the plan is the whole decision:
//! which single slot (if any) to fire, which slots to record as skipped and with which reason,
//! and where the cursor goes next. The plan is data — the caller persists it, the caller never
//! re-derives a fire time from the wall clock (§4.3).
//!
//! # The two `catch_up` policies (§5.3)
//!
//! * [`CatchUp::Latest`] (default) — fire **at most once** for the most recent missed slot,
//!   record the rest as `skipped_misfire`. The reason distinguishes the two cases:
//!   [`SkipReason::Superseded`] when the whole missed window still fits inside the coalesce
//!   window (`now - last_fire_time <= coalesce`), [`SkipReason::OutsideCoalesce`] otherwise
//!   (the gap is still collapsed to one firing, but the reason says the window was blown).
//! * [`CatchUp::Skip`] — never fire for a past slot; record every missed slot as
//!   [`SkipReason::PolicySkip`] and advance the cursor to the first fire time after `now`.
//!
//! Neither policy replays a backlog: there is no unbounded catch-up in v1.

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use chrono_tz::Tz;

use crate::error::CronError;
use crate::expr::CronExpr;

/// Upper bound on how many skipped slots one plan materializes.
///
/// `missed` still counts every slot; past this cap the `skipped` vector is truncated (earliest
/// first) so a pathological cursor cannot allocate without bound. A cursor that stale is the
/// `cursor_reset` path's business (§12.3), not the tick's.
pub const MAX_SKIPPED_SLOTS: usize = 4096;

/// Durable slot key: `{schedule_id}@{fire_time}` (CRON-MODEL §9.1).
///
/// `fire_time` is rendered as UTC RFC 3339 with whole seconds, e.g.
/// `nightly@2026-11-08T09:00:00Z`. Fire times are whole minutes by construction (the grammar
/// has no seconds field), so the key is stable and safe as a primary key / dedupe key.
///
/// ```
/// use chrono::{TimeZone, Utc};
/// use kian_cron::firing_id;
///
/// let fire_time = Utc.with_ymd_and_hms(2026, 11, 8, 9, 0, 0).unwrap();
/// assert_eq!(firing_id("nightly", fire_time), "nightly@2026-11-08T09:00:00Z");
/// ```
pub fn firing_id(schedule_id: &str, fire_time: DateTime<Utc>) -> String {
    format!(
        "{schedule_id}@{}",
        fire_time.to_rfc3339_opts(SecondsFormat::Secs, true)
    )
}

/// The schedule's `catch_up` policy (§5.3). Default is [`CatchUp::Latest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CatchUp {
    /// Collapse every missed slot into one fire-latest-only firing.
    #[default]
    Latest,
    /// Never fire for a slot that is already past.
    Skip,
}

/// Why a slot was recorded as `skipped_misfire` (§9.1 `reason` machine tokens).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkipReason {
    /// A later slot of the same schedule is being fired instead of this one.
    Superseded,
    /// The missed window exceeded `coalesce_ms`; the gap is reported, not replayed.
    OutsideCoalesce,
    /// `catch_up: skip` — past slots are never fired.
    PolicySkip,
}

impl SkipReason {
    /// The stable machine token written to `cron_firings.reason` and emitted on events.
    pub const fn as_str(self) -> &'static str {
        match self {
            SkipReason::Superseded => "superseded",
            SkipReason::OutsideCoalesce => "outside_coalesce",
            SkipReason::PolicySkip => "policy_skip",
        }
    }
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One slot recorded as skipped, with the reason that decided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkippedSlot {
    /// The logical instants this slot was due at (never `now`).
    pub fire_time: DateTime<Utc>,
    /// Why it was skipped.
    pub reason: SkipReason,
}

/// What kind of due-ness decision a [`FirePlan`] represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DueKind {
    /// `next_fire_at > now`: the tick does nothing.
    NotDue,
    /// Lateness within `coalesce_ms`: a plain firing for the computed `fire_time`.
    OnTime,
    /// A late slot was honored as a single fire-latest-only catch-up firing.
    CatchUpLatest,
    /// A late slot was dropped by `catch_up: skip`.
    PolicySkip,
}

/// The complete decision for one schedule in one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirePlan {
    /// Which branch of §5.2/§5.3 decided this.
    pub kind: DueKind,
    /// The single slot to fire now, if any. Never `now` — always the computed fire time.
    pub fire: Option<DateTime<Utc>>,
    /// The cursor value to persist after applying this plan.
    pub next_fire_at: DateTime<Utc>,
    /// Slots recorded as `skipped_misfire`, each with its reason.
    pub skipped: Vec<SkippedSlot>,
    /// How many slots this decision superseded or skipped (exact, even when `skipped` is
    /// truncated at [`MAX_SKIPPED_SLOTS`]).
    pub missed: u32,
}

/// The tick-time inputs to [`plan_fire`] — the durable cursor plus the schedule's policies.
#[derive(Debug, Clone, Copy)]
pub struct DueInput<'a> {
    /// The parsed expression.
    pub expr: &'a CronExpr,
    /// The schedule's IANA timezone.
    pub tz: &'a Tz,
    /// The single `now` read by this tick (§5.2).
    pub now: DateTime<Utc>,
    /// The durable cursor: when the schedule is next due.
    pub next_fire_at: DateTime<Utc>,
    /// The durable cursor: when it last fired, if it ever has.
    pub last_fire_at: Option<DateTime<Utc>>,
    /// The `catch_up` policy.
    pub catch_up: CatchUp,
    /// The coalesce window (`coalesce_ms`).
    pub coalesce: TimeDelta,
}

/// Classify one schedule's due-ness and produce the whole firing decision.
///
/// `Ok(FirePlan { kind: DueKind::NotDue, .. })` means "nothing to do this tick". Every other
/// kind either fires exactly one slot or records skips, and always carries the cursor value to
/// persist.
pub fn plan_fire(input: DueInput<'_>) -> Result<FirePlan, CronError> {
    let DueInput {
        expr,
        tz,
        now,
        next_fire_at,
        last_fire_at,
        catch_up,
        coalesce,
    } = input;

    if next_fire_at > now {
        return Ok(FirePlan {
            kind: DueKind::NotDue,
            fire: None,
            next_fire_at,
            skipped: Vec::new(),
            missed: 0,
        });
    }

    // §5.2: lateness within the coalesce window is a *normal* firing whose fire_time is the
    // computed next_fire_at, not `now`, so the record stays reproducible.
    if now - next_fire_at <= coalesce {
        return Ok(FirePlan {
            kind: DueKind::OnTime,
            fire: Some(next_fire_at),
            next_fire_at: expr.next_fire_after(tz, next_fire_at)?,
            skipped: Vec::new(),
            missed: 0,
        });
    }

    // §5.3: misfire. Enumerate every slot in `( .., now]`.
    let mut slots: Vec<DateTime<Utc>> = Vec::new();
    let mut latest = next_fire_at;
    let mut total: usize = 0;
    let mut slot = next_fire_at;
    while slot <= now {
        latest = slot;
        if slots.len() < MAX_SKIPPED_SLOTS {
            slots.push(slot);
        }
        total += 1;
        slot = expr.next_fire_after(tz, slot)?;
    }

    let (kind, fire, reason) = match catch_up {
        CatchUp::Latest => {
            // The window is measured against the last actual firing (§5.3); with no history,
            // against the oldest missed slot.
            let reference = last_fire_at.unwrap_or(next_fire_at);
            let reason = if now - reference <= coalesce {
                SkipReason::Superseded
            } else {
                SkipReason::OutsideCoalesce
            };
            (DueKind::CatchUpLatest, Some(latest), reason)
        }
        CatchUp::Skip => (DueKind::PolicySkip, None, SkipReason::PolicySkip),
    };

    // Slots not honored by this plan are recorded as skipped; for `latest` that is everything
    // but the one firing, for `skip` that is all of them.
    let skipped: Vec<SkippedSlot> = slots
        .into_iter()
        .filter(|candidate| Some(*candidate) != fire)
        .map(|fire_time| SkippedSlot { fire_time, reason })
        .collect();
    let missed_slots = match catch_up {
        CatchUp::Latest => total.saturating_sub(1),
        CatchUp::Skip => total,
    };
    let missed = u32::try_from(missed_slots).unwrap_or(u32::MAX);

    let next_fire_at = match fire {
        Some(fire) => expr.next_fire_after(tz, fire)?,
        // `catch_up: skip`: jump the cursor past the gap.
        None => expr.next_fire_after(tz, now)?,
    };

    Ok(FirePlan {
        kind,
        fire,
        next_fire_at,
        skipped,
        missed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_timezone;
    use chrono::TimeZone;

    fn expr(source: &str) -> CronExpr {
        CronExpr::parse(source).expect("expression should parse")
    }

    fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, hh, mm, 0)
            .single()
            .expect("unambiguous UTC instant")
    }

    const UTC_TZ: fn() -> Tz = || parse_timezone("UTC").expect("UTC resolves");

    #[test]
    fn firing_id_is_schedule_at_rfc3339_utc() {
        assert_eq!(
            firing_id("nightly", utc(2026, 11, 8, 9, 0)),
            "nightly@2026-11-08T09:00:00Z"
        );
        // Same slot, same key — this is the dedupe property (§4.3).
        assert_eq!(
            firing_id("nightly", utc(2026, 11, 8, 9, 0)),
            firing_id("nightly", utc(2026, 11, 8, 9, 0))
        );
        assert_ne!(
            firing_id("nightly", utc(2026, 11, 8, 9, 0)),
            firing_id("other", utc(2026, 11, 8, 9, 0))
        );
    }

    #[test]
    fn not_due_before_the_cursor() {
        let expr = expr("*/15 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 0, 14),
            next_fire_at: utc(2026, 1, 1, 0, 15),
            last_fire_at: Some(utc(2026, 1, 1, 0, 0)),
            catch_up: CatchUp::Latest,
            coalesce: TimeDelta::minutes(5),
        })
        .expect("plan");
        assert_eq!(plan.kind, DueKind::NotDue);
        assert_eq!(plan.fire, None);
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 0, 15));
        assert!(plan.skipped.is_empty());
        assert_eq!(plan.missed, 0);
    }

    #[test]
    fn on_time_firing_keeps_the_computed_fire_time() {
        let expr = expr("*/15 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 0, 16),
            next_fire_at: utc(2026, 1, 1, 0, 15),
            last_fire_at: Some(utc(2026, 1, 1, 0, 0)),
            catch_up: CatchUp::Latest,
            coalesce: TimeDelta::minutes(5),
        })
        .expect("plan");
        assert_eq!(plan.kind, DueKind::OnTime);
        assert_eq!(
            plan.fire,
            Some(utc(2026, 1, 1, 0, 15)),
            "fire_time is not `now`"
        );
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 0, 30));
        assert_eq!(plan.missed, 0);
    }

    #[test]
    fn three_hour_jump_with_latest_fires_once_outside_coalesce() {
        // §13 test-matrix row: clock jumps 3h, catch_up: latest, 15m window ->
        // exactly one firing (the latest slot), missed: 11, outside_coalesce.
        let expr = expr("*/15 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 3, 0),
            next_fire_at: utc(2026, 1, 1, 0, 15),
            last_fire_at: Some(utc(2026, 1, 1, 0, 0)),
            catch_up: CatchUp::Latest,
            coalesce: TimeDelta::minutes(15),
        })
        .expect("plan");

        assert_eq!(plan.kind, DueKind::CatchUpLatest);
        assert_eq!(
            plan.fire,
            Some(utc(2026, 1, 1, 3, 0)),
            "one firing, latest slot"
        );
        assert_eq!(plan.missed, 11);
        assert_eq!(plan.skipped.len(), 11);
        assert!(
            plan.skipped
                .iter()
                .all(|slot| slot.reason == SkipReason::OutsideCoalesce)
        );
        assert_eq!(plan.skipped[0].fire_time, utc(2026, 1, 1, 0, 15));
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 3, 15));
    }

    #[test]
    fn within_window_missed_slots_are_superseded() {
        let expr = expr("*/15 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 3, 0),
            next_fire_at: utc(2026, 1, 1, 2, 15),
            // Fired recently relative to the coalesce window: the rest are merely superseded.
            last_fire_at: Some(utc(2026, 1, 1, 2, 40)),
            catch_up: CatchUp::Latest,
            coalesce: TimeDelta::minutes(30),
        })
        .expect("plan");

        assert_eq!(plan.kind, DueKind::CatchUpLatest);
        assert_eq!(plan.fire, Some(utc(2026, 1, 1, 3, 0)));
        assert_eq!(plan.missed, 3);
        assert!(
            plan.skipped
                .iter()
                .all(|slot| slot.reason == SkipReason::Superseded)
        );
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 3, 15));
    }

    #[test]
    fn catch_up_skip_never_fires_and_moves_the_cursor_past_the_gap() {
        // §13 test-matrix row: same 3h jump with catch_up: skip -> zero firings, one
        // skipped_misfire/policy_skip per slot, cursor past the gap.
        let expr = expr("*/15 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 3, 0),
            next_fire_at: utc(2026, 1, 1, 0, 15),
            last_fire_at: Some(utc(2026, 1, 1, 0, 0)),
            catch_up: CatchUp::Skip,
            coalesce: TimeDelta::minutes(15),
        })
        .expect("plan");

        assert_eq!(plan.kind, DueKind::PolicySkip);
        assert_eq!(plan.fire, None);
        assert_eq!(plan.missed, 12);
        assert_eq!(plan.skipped.len(), 12);
        assert!(
            plan.skipped
                .iter()
                .all(|slot| slot.reason == SkipReason::PolicySkip)
        );
        assert!(
            plan.next_fire_at > utc(2026, 1, 1, 3, 0),
            "cursor past the gap"
        );
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 3, 15));
    }

    #[test]
    fn one_missed_slot_still_fires_the_latest_only() {
        let expr = expr("0 * * * *");
        let tz = UTC_TZ();
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: utc(2026, 1, 1, 2, 5),
            next_fire_at: utc(2026, 1, 1, 1, 0),
            last_fire_at: Some(utc(2026, 1, 1, 0, 0)),
            catch_up: CatchUp::Latest,
            coalesce: TimeDelta::minutes(1),
        })
        .expect("plan");
        assert_eq!(plan.kind, DueKind::CatchUpLatest);
        // One slot was superseded (01:00), the latest one (02:00) is the single firing.
        assert_eq!(plan.fire, Some(utc(2026, 1, 1, 2, 0)));
        assert_eq!(plan.missed, 1);
        assert_eq!(
            plan.skipped,
            vec![SkippedSlot {
                fire_time: utc(2026, 1, 1, 1, 0),
                reason: SkipReason::OutsideCoalesce,
            }]
        );
        assert_eq!(plan.next_fire_at, utc(2026, 1, 1, 3, 0));
    }

    #[test]
    fn missed_count_is_exact_even_when_the_skip_list_is_truncated() {
        let expr = expr("* * * * *");
        let tz = UTC_TZ();
        let start = utc(2026, 1, 1, 0, 0);
        let plan = plan_fire(DueInput {
            expr: &expr,
            tz: &tz,
            now: start + TimeDelta::days(3),
            next_fire_at: start,
            last_fire_at: None,
            catch_up: CatchUp::Skip,
            coalesce: TimeDelta::minutes(1),
        })
        .expect("plan");
        assert_eq!(plan.missed, 4321);
        assert_eq!(plan.skipped.len(), MAX_SKIPPED_SLOTS);
        assert_eq!(plan.next_fire_at, utc(2026, 1, 4, 0, 1));
    }

    #[test]
    fn skip_reason_tokens_are_the_spec_tokens() {
        assert_eq!(SkipReason::Superseded.as_str(), "superseded");
        assert_eq!(SkipReason::OutsideCoalesce.as_str(), "outside_coalesce");
        assert_eq!(SkipReason::PolicySkip.as_str(), "policy_skip");
        assert_eq!(SkipReason::PolicySkip.to_string(), "policy_skip");
        assert_eq!(CatchUp::default(), CatchUp::Latest);
    }
}
