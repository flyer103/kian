//! Timezone resolution and next-fire computation (CRON-MODEL §2.3, §2.4, §2.5).
//!
//! [`CronExpr::next_fire_after`] is a **pure** function: no I/O, no clock read, no store access.
//! It answers "what is the earliest UTC instant strictly after `from` that matches this
//! expression in this timezone?".
//!
//! # DST rules (§2.4), fixed per schedule and not configurable in v1
//!
//! * **Gap** (wall time does not exist, e.g. `02:30` on `America/New_York` spring-forward):
//!   **skip**. The candidate is dropped; we never shift it forward to `03:00`, because that
//!   would run at an hour the operator did not ask for.
//! * **Overlap** (wall time occurs twice, e.g. `01:30` on fall-back): **run once**, at the
//!   *first* (pre-transition) occurrence. The second occurrence is suppressed because both
//!   occurrences resolve to the same wall time under the expression, and that slot has already
//!   fired: mapping an ambiguous local time back to UTC picks the earlier instant, so asking
//!   for the next fire after the pre-transition instant skips the duplicate.
//! * **Wildcard hour/minute**: each local time is evaluated independently, so an hourly
//!   schedule loses exactly the missing hours in a gap and does not double-fire in an overlap.

use chrono::{DateTime, Days, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use std::str::FromStr;

use crate::error::CronError;
use crate::expr::CronExpr;

/// Bounded forward-scan horizon in days (CRON-MODEL §2.5).
///
/// Exhausting it is a [`CronError::NoFireWithinHorizon`] config error, reachable only for
/// valid-but-never-matching shapes such as `0 0 30 2 *`.
///
/// Note: 366 days cannot reach a `29 2 *` leap-day schedule declared more than a year before
/// the next leap year; call [`CronExpr::next_fire_after_with_horizon`] with a longer horizon
/// (e.g. `2922`, eight years) if that shape must be supported. This is the only place where the
/// spec's 366-day bound shows through.
pub const MAX_SCAN_DAYS: i64 = 366;

/// Resolve an IANA timezone name (`Europe/Berlin`, `America/Los_Angeles`, `UTC`).
///
/// An unresolvable `tz` is a hard config error, never a fallback to UTC (§2.3).
pub fn parse_timezone(name: &str) -> Result<Tz, CronError> {
    Tz::from_str(name).map_err(|_| CronError::UnknownTimeZone {
        name: name.to_string(),
    })
}

/// Map a local wall time to UTC under the §2.4 rules, or `None` if it falls in a DST gap.
fn resolve_local(tz: &Tz, naive: NaiveDateTime) -> Option<DateTime<Utc>> {
    match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => {
            // Overlap: take the *earliest* UTC instant, i.e. the pre-transition occurrence.
            let first = first.with_timezone(&Utc);
            let second = second.with_timezone(&Utc);
            Some(if first <= second { first } else { second })
        }
        // Gap: the wall time does not exist. Skip it — never shift.
        LocalResult::None => None,
    }
}

impl CronExpr {
    /// Earliest UTC instant strictly after `from` matching this expression in `tz`.
    ///
    /// Uses the spec's [`MAX_SCAN_DAYS`] horizon. The result is the earliest matching instant,
    /// with DST gaps skipped and overlaps collapsed onto their pre-transition occurrence.
    ///
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// use kian_cron::{parse_timezone, CronExpr};
    ///
    /// let tz = parse_timezone("Asia/Shanghai").unwrap();
    /// let expr = CronExpr::parse("0 0 * * *").unwrap();
    /// let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    /// // Local midnight in a fixed-offset zone is 16:00Z the previous day.
    /// assert_eq!(
    ///     expr.next_fire_after(&tz, from).unwrap(),
    ///     Utc.with_ymd_and_hms(2026, 1, 1, 16, 0, 0).unwrap()
    /// );
    /// ```
    pub fn next_fire_after(
        &self,
        tz: &Tz,
        from: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, CronError> {
        self.next_fire_after_with_horizon(tz, from, MAX_SCAN_DAYS)
    }

    /// Same as [`CronExpr::next_fire_after`] with an explicit scan horizon in days.
    ///
    /// The scan is still bounded and terminating: at most `horizon_days` local calendar days
    /// after `from` are examined before [`CronError::NoFireWithinHorizon`] is returned.
    pub fn next_fire_after_with_horizon(
        &self,
        tz: &Tz,
        from: DateTime<Utc>,
        horizon_days: i64,
    ) -> Result<DateTime<Utc>, CronError> {
        let horizon_days = horizon_days.max(1);
        let local_from = from.with_timezone(tz).naive_local();
        // Start one *local* day early: with a fall-back transition an instant on the previous
        // local date can map to a later UTC instant than one on `from`'s local date (offsets
        // move by at most a couple of hours, always far less than a day). Candidates are
        // filtered by `> from` anyway, so starting early is safe and never adds a wrong result.
        let start = local_from
            .date()
            .checked_sub_days(Days::new(1))
            .unwrap_or_else(|| local_from.date());

        let mut best: Option<DateTime<Utc>> = None;
        let mut best_date: Option<NaiveDate> = None;
        let mut date = start;

        loop {
            if self.month_matches(date) && self.day_matches(date) {
                for hour in (0u32..24).filter(|h| self.hour_mask() & (1u64 << h) != 0) {
                    for minute in (0u32..60).filter(|m| self.minute_mask() & (1u64 << m) != 0) {
                        let Some(naive) = date.and_hms_opt(hour, minute, 0) else {
                            continue;
                        };
                        // `None` here is a DST-gap candidate: skipped, never shifted (§2.4).
                        let Some(utc) = resolve_local(tz, naive) else {
                            continue;
                        };
                        if utc <= from {
                            continue;
                        }
                        if best.is_none_or(|current| utc < current) {
                            best = Some(utc);
                            best_date = Some(date);
                        }
                    }
                }
            }

            // Two local days past the best candidate, no later local date can yield an earlier
            // UTC instant (offset deltas are orders of magnitude below a day).
            if let Some(found_on) = best_date
                && (date - found_on).num_days() >= 2
            {
                break;
            }
            if (date - start).num_days() >= horizon_days + 2 {
                break;
            }
            match date.succ_opt() {
                Some(next) => date = next,
                None => break,
            }
        }

        best.ok_or_else(|| CronError::NoFireWithinHorizon {
            expr: self.source().to_string(),
            horizon_days,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    fn expr(source: &str) -> CronExpr {
        CronExpr::parse(source).expect("expression should parse")
    }

    fn tz(name: &str) -> Tz {
        parse_timezone(name).expect("timezone should resolve")
    }

    fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, hh, mm, 0)
            .single()
            .expect("unambiguous UTC instant")
    }

    #[test]
    fn quarter_hours_across_an_hour_boundary() {
        let expr = expr("*/15 * * * *");
        let zone = tz("UTC");
        let mut cursor = utc(2026, 1, 1, 0, 7);
        let mut seen = Vec::new();
        for _ in 0..5 {
            let next = expr.next_fire_after(&zone, cursor).expect("next fire");
            seen.push(next);
            cursor = next;
        }
        assert_eq!(
            seen,
            vec![
                utc(2026, 1, 1, 0, 15),
                utc(2026, 1, 1, 0, 30),
                utc(2026, 1, 1, 0, 45),
                utc(2026, 1, 1, 1, 0),
                utc(2026, 1, 1, 1, 15),
            ]
        );
        assert!(seen.windows(2).all(|pair| pair[1] > pair[0]), "monotonic");
    }

    #[test]
    fn strictly_after_and_boundary_idempotent() {
        let expr = expr("0 0 * * *");
        let zone = tz("UTC");
        let fire = utc(2026, 3, 1, 0, 0);
        // Boundary idempotence: `next_fire(x - 1ns) == x` when `x` matches.
        assert_eq!(
            expr.next_fire_after(&zone, fire - TimeDelta::nanoseconds(1))
                .expect("next fire"),
            fire
        );
        // Strictly after: asking again from the fire time skips it.
        assert_eq!(
            expr.next_fire_after(&zone, fire).expect("next fire"),
            utc(2026, 3, 2, 0, 0)
        );
    }

    #[test]
    fn monotonic_across_a_long_sweep() {
        let expr = expr("0 0 * * 1");
        let zone = tz("Europe/Berlin");
        let mut cursor = utc(2026, 1, 1, 0, 0);
        let mut previous: Option<DateTime<Utc>> = None;
        for _ in 0..60 {
            let next = expr.next_fire_after(&zone, cursor).expect("next fire");
            if let Some(previous) = previous {
                assert!(next > previous, "monotonic");
            }
            previous = Some(next);
            cursor = next;
        }
    }

    #[test]
    fn dom_dow_union_in_next_fire() {
        // The 1st *or* any Monday, midnight UTC.
        let expr = expr("0 0 1 * 1");
        let zone = tz("UTC");
        // 2026-01-01 is a Thursday; the next match is Monday 2026-01-05.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 1, 1, 0, 0))
                .expect("next fire"),
            utc(2026, 1, 5, 0, 0)
        );
        // 2026-12-27 is a Sunday, so the next match is the following Monday.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 12, 27, 0, 0))
                .expect("next fire"),
            utc(2026, 12, 28, 0, 0)
        );
    }

    #[test]
    fn month_end_only_lands_on_existing_days() {
        let expr = expr("0 0 31 * *");
        let zone = tz("UTC");
        // April has 30 days: the next 31st after 2026-04-01 is 2026-05-31.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 4, 1, 0, 0))
                .expect("next fire"),
            utc(2026, 5, 31, 0, 0)
        );
        // June has 30 as well, so July 31 is next after May 31.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 5, 31, 0, 0))
                .expect("next fire"),
            utc(2026, 7, 31, 0, 0)
        );
    }

    #[test]
    fn leap_day_is_reachable_within_the_horizon() {
        let expr = expr("0 0 29 2 *");
        let zone = tz("UTC");
        assert_eq!(
            expr.next_fire_after(&zone, utc(2027, 12, 1, 0, 0))
                .expect("next fire"),
            utc(2028, 2, 29, 0, 0)
        );
        // From just after a leap day the next one is four years out: the default 366-day scan
        // refuses rather than looping forever.
        assert!(matches!(
            expr.next_fire_after(&zone, utc(2028, 3, 1, 0, 0)),
            Err(CronError::NoFireWithinHorizon { .. })
        ));
        // ... and a deliberate eight-year horizon finds it.
        assert_eq!(
            expr.next_fire_after_with_horizon(&zone, utc(2028, 3, 1, 0, 0), 2922)
                .expect("next fire"),
            utc(2032, 2, 29, 0, 0)
        );
    }

    #[test]
    fn never_matching_shape_terminates_with_an_error() {
        // February 30 never exists.
        assert!(matches!(
            expr("0 0 30 2 *").next_fire_after(&tz("UTC"), utc(2026, 1, 1, 0, 0)),
            Err(CronError::NoFireWithinHorizon { .. })
        ));
    }

    #[test]
    fn spring_gap_is_skipped_never_shifted_us() {
        // America/New_York 2026-03-08: 02:00 EST -> 03:00 EDT, so 02:30 does not exist.
        let expr = expr("30 2 * * *");
        let zone = tz("America/New_York");
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 3, 7, 12, 0))
                .expect("next fire"),
            // 2026-03-09 02:30 EDT == 06:30Z. The gap day produced no firing at all, and
            // 03:30 EDT on the gap day was *not* substituted.
            utc(2026, 3, 9, 6, 30)
        );
    }

    #[test]
    fn spring_gap_is_skipped_never_shifted_berlin() {
        // Europe/Berlin 2026-03-29: 02:00 CET -> 03:00 CEST, so 02:30 does not exist.
        let expr = expr("30 2 * * *");
        let zone = tz("Europe/Berlin");
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 3, 28, 12, 0))
                .expect("next fire"),
            // 2026-03-30 02:30 CEST == 00:30Z.
            utc(2026, 3, 30, 0, 30)
        );
    }

    #[test]
    fn fall_back_fires_once_at_the_pre_transition_instant() {
        // America/New_York 2026-11-01: 02:00 EDT -> 01:00 EST, so 01:30 happens twice.
        let expr = expr("30 1 * * *");
        let zone = tz("America/New_York");
        let first = expr
            .next_fire_after(&zone, utc(2026, 10, 31, 12, 0))
            .expect("next fire");
        // Pre-transition occurrence: 01:30 EDT == 05:30Z.
        assert_eq!(first, utc(2026, 11, 1, 5, 30));
        // The second occurrence (01:30 EST == 06:30Z) is suppressed by the slot key.
        assert_eq!(
            expr.next_fire_after(&zone, first).expect("next fire"),
            utc(2026, 11, 2, 6, 30)
        );
        // Even asking from *between* the two occurrences does not produce the duplicate.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 11, 1, 6, 0))
                .expect("next fire"),
            utc(2026, 11, 2, 6, 30)
        );
    }

    #[test]
    fn fall_back_fires_once_berlin() {
        // Europe/Berlin 2026-10-25: 03:00 CEST -> 02:00 CET, so 02:30 happens twice.
        let expr = expr("30 2 * * *");
        let zone = tz("Europe/Berlin");
        let first = expr
            .next_fire_after(&zone, utc(2026, 10, 24, 12, 0))
            .expect("next fire");
        // Pre-transition occurrence: 02:30 CEST == 00:30Z.
        assert_eq!(first, utc(2026, 10, 25, 0, 30));
        // Next day 02:30 CET == 01:30Z; 2026-10-25 02:30 CET == 01:30Z is suppressed.
        assert_eq!(
            expr.next_fire_after(&zone, first).expect("next fire"),
            utc(2026, 10, 26, 1, 30)
        );
    }

    #[test]
    fn half_hourly_schedule_fires_once_per_wall_time_in_the_overlap() {
        // Europe/Berlin 2026-10-25: the wall times 02:00..=02:59 happen twice. The repeated hour
        // is *suppressed*: ambiguous wall times resolve to their pre-transition instant, so
        // 02:00 and 02:30 fire once each and the second pass (01:00Z, 01:30Z) produces nothing.
        let expr = expr("*/30 * * * *");
        let zone = tz("Europe/Berlin");
        let mut cursor = utc(2026, 10, 25, 0, 0); // 02:00 CEST, the first occurrence
        let mut seen = Vec::new();
        for _ in 0..6 {
            let next = expr.next_fire_after(&zone, cursor).expect("next fire");
            seen.push(next);
            cursor = next;
        }
        assert_eq!(
            seen,
            vec![
                utc(2026, 10, 25, 0, 30), // 02:30 CEST (first occurrence)
                utc(2026, 10, 25, 2, 0),  // 03:00 CET — 02:00/02:30 CET suppressed
                utc(2026, 10, 25, 2, 30), // 03:30 CET
                utc(2026, 10, 25, 3, 0),  // 04:00 CET
                utc(2026, 10, 25, 3, 30), // 04:30 CET
                utc(2026, 10, 25, 4, 0),  // 05:00 CET
            ]
        );
        assert!(
            seen.iter()
                .all(|instant| !(*instant > utc(2026, 10, 25, 0, 30)
                    && *instant < utc(2026, 10, 25, 2, 0))),
            "the second pass of the repeated hour is suppressed"
        );
    }

    #[test]
    fn hourly_schedule_loses_only_the_gap_hours() {
        // Berlin 2026-03-29: 02:00 CET -> 03:00 CEST. Every 02:xx wall time is nonexistent, so
        // an hourly schedule simply has no firing there: 01:00 CET (00:00Z) then 03:00 CEST
        // (01:00Z). The shifted hour is never substituted.
        let expr = expr("0 * * * *");
        let zone = tz("Europe/Berlin");
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 3, 29, 0, 0))
                .expect("next fire"),
            utc(2026, 3, 29, 1, 0)
        );
    }

    #[test]
    fn non_dst_zone_keeps_a_fixed_offset() {
        let expr = expr("0 0 * * *");
        let zone = tz("Asia/Shanghai");
        let mut cursor = utc(2026, 1, 1, 0, 0);
        let mut seen = Vec::new();
        for _ in 0..5 {
            let next = expr.next_fire_after(&zone, cursor).expect("next fire");
            seen.push(next);
            cursor = next;
        }
        assert_eq!(
            seen,
            (1..=5)
                .map(|day| utc(2026, 1, day, 16, 0))
                .collect::<Vec<_>>()
        );
        assert!(
            seen.windows(2)
                .all(|pair| pair[1] - pair[0] == TimeDelta::hours(24)),
            "no DST: every step is exactly 24h"
        );
    }

    #[test]
    fn monthly_in_a_non_dst_zone() {
        let expr = expr("0 0 1 * *");
        let zone = tz("Asia/Shanghai");
        // 2026-02-01 00:00 CST == 2026-01-31 16:00Z.
        assert_eq!(
            expr.next_fire_after(&zone, utc(2026, 1, 1, 0, 0))
                .expect("next fire"),
            utc(2026, 1, 31, 16, 0)
        );
    }

    #[test]
    fn unknown_timezone_is_a_hard_error() {
        assert_eq!(
            parse_timezone("Europe/Berlinn"),
            Err(CronError::UnknownTimeZone {
                name: "Europe/Berlinn".to_string()
            })
        );
        assert!(parse_timezone("UTC").is_ok());
        assert!(parse_timezone("America/Los_Angeles").is_ok());
    }
}
