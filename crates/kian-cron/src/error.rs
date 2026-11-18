//! The crate's error vocabulary. Every rejection is fail-closed and carries enough context to
//! name the offending field and token (CRON-MODEL §3.3: a malformed expression is a config
//! error at load, never a silent skip).

use crate::expr::Field;
use thiserror::Error;

/// Everything this crate can reject.
///
/// Errors are precise on purpose: an operator reading `kian cron list --why` (or a startup
/// failure) must be able to see *which* field and *which* token was wrong.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CronError {
    /// The expression was empty or whitespace only.
    #[error("cron expression is empty")]
    Empty,

    /// The expression did not have exactly five whitespace-separated fields.
    #[error(
        "cron expression `{expr}` has {found} field(s); exactly 5 are required \
         (minute hour day-of-month month day-of-week)"
    )]
    FieldCount {
        /// The expression as written (trimmed).
        expr: String,
        /// How many fields were found.
        found: usize,
    },

    /// An `@`-alias that is not in the v1 set (e.g. `@reboot`, which is explicitly out).
    #[error("unknown cron alias `{alias}`; supported: @hourly @daily @weekly @monthly @yearly")]
    UnknownAlias {
        /// The alias as written.
        alias: String,
    },

    /// An empty element inside a comma list, e.g. `1,,2` or a trailing comma.
    #[error("{field}: empty element in list `{expr}`")]
    EmptyElement {
        /// The field that failed to parse.
        field: Field,
        /// The whole field text, for context.
        expr: String,
    },

    /// A structurally wrong element (`1/`, `-5`, `1-2-3`, `a` in a numeric-only field, …).
    #[error("{field}: malformed element `{token}` (expected {expected})")]
    Malformed {
        /// The field that failed to parse.
        field: Field,
        /// The offending element.
        token: String,
        /// What would have been accepted.
        expected: &'static str,
    },

    /// A numeric value outside the field's range.
    #[error("{field}: {value} is out of range {min}..={max} in `{token}`")]
    ValueOutOfRange {
        /// The field that failed to parse.
        field: Field,
        /// The offending element.
        token: String,
        /// The rejected value.
        value: u32,
        /// Lowest legal value.
        min: u32,
        /// Highest legal value.
        max: u32,
    },

    /// A three-letter name that is not in the field's name table.
    #[error("{field}: unknown name `{token}` (expected {expected}, case-insensitive)")]
    UnknownName {
        /// The field that failed to parse.
        field: Field,
        /// The offending name.
        token: String,
        /// The accepted names, as a hint.
        expected: &'static str,
    },

    /// A step of zero (`*/0`), which would never terminate.
    #[error("{field}: step must be >= 1 in `{token}`")]
    ZeroStep {
        /// The field that failed to parse.
        field: Field,
        /// The offending element.
        token: String,
    },

    /// A descending range (`5-1`).
    #[error("{field}: range start {lo} is greater than end {hi} in `{token}`")]
    InvertedRange {
        /// The field that failed to parse.
        field: Field,
        /// The offending element.
        token: String,
        /// Range start.
        lo: u32,
        /// Range end.
        hi: u32,
    },

    /// The bounded forward scan found no matching instant. Reachable only for valid-but-
    /// never-matching shapes such as `0 0 30 2 *` (CRON-MODEL §2.5: the scan terminates).
    #[error("no fire time for `{expr}` within {horizon_days} days")]
    NoFireWithinHorizon {
        /// The normalized five-field expression.
        expr: String,
        /// The scan horizon that was exhausted.
        horizon_days: i64,
    },

    /// A `tz` that does not resolve in the bundled IANA database. Never a silent fallback to
    /// UTC (CRON-MODEL §2.3).
    #[error("unknown IANA timezone `{name}` (no UTC fallback)")]
    UnknownTimeZone {
        /// The name as written.
        name: String,
    },
}
