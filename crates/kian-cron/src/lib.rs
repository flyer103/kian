//! Kian cron scheduling core — the pure half of `design/CRON-MODEL.md`.
//!
//! This crate implements exactly two of the model's normative sections and nothing else:
//!
//! * **§2 Schedule model** — the five-field expression grammar (§2.1), the `@`-aliases (§2.2),
//!   timezone resolution (§2.3), the DST gap/overlap rules (§2.4) and the pure
//!   [`CronExpr::next_fire_after`] computation (§2.5).
//! * **§5 Firing semantics, the pure parts** — lateness classification and the `catch_up`
//!   policy plans ([`plan_fire`]), the skip-reason vocabulary (§9.1) and the
//!   `{schedule_id}@{fire_time}` slot key ([`firing_id`]).
//!
//! Deliberately **not** here (later increments): the tick loop and its clock reads, the cursors
//! in the store, config/declaration loading, the dsh/worktree execution path and the CLI. Every
//! function in this crate is pure: no I/O, no clock read, no store access, no panics on user
//! input — malformed expressions and unresolvable timezones are `Result`s.
//!
//! ```
//! use chrono::{TimeZone, Utc};
//! use kian_cron::{parse_timezone, CronExpr};
//!
//! let expr = CronExpr::parse("*/15 * * * *").unwrap();
//! let tz = parse_timezone("Europe/Berlin").unwrap();
//! let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 7, 0).unwrap();
//! let next = expr.next_fire_after(&tz, from).unwrap();
//! assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 0, 15, 0).unwrap());
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod expr;
mod misfire;
mod schedule;

pub use chrono_tz::Tz;
pub use error::CronError;
pub use expr::{ALIASES, CronExpr, Field};
pub use misfire::{
    CatchUp, DueInput, DueKind, FirePlan, MAX_SKIPPED_SLOTS, SkipReason, SkippedSlot, firing_id,
    plan_fire,
};
pub use schedule::{MAX_SCAN_DAYS, parse_timezone};
