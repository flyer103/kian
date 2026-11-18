//! Expression grammar, aliases and day-matching rules (CRON-MODEL §2.1, §2.2).
//!
//! Grammar recap (five fields, classic order):
//!
//! ```text
//! ┌──────── minute        0-59
//! │ ┌────── hour          0-23
//! │ │ ┌──── day of month  1-31
//! │ │ │ ┌── month         1-12
//! │ │ │ │ ┌ day of week   0-6 (0 = Sunday; 7 also = Sunday)
//! * * * * *
//! ```
//!
//! Each field accepts `*`, a literal, a comma list, a range `a-b`, and the step forms `*/n`,
//! `a-b/n`, `a/n`. Month and day-of-week additionally accept three-letter English names,
//! case-insensitively. Seconds, `L`/`W`/`#`, `@reboot` and sub-minute intervals are out of v1.

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::hash::{Hash, Hasher};

use crate::error::CronError;

/// The `@`-aliases of CRON-MODEL §2.2 and the five-field expressions they expand to.
///
/// Aliases are pure sugar: [`CronExpr::parse`] rewrites them *before* anything is hashed or
/// compared, so `@daily` and `0 0 * * *` are the same schedule and [`CronExpr`] compares equal
/// for both.
pub const ALIASES: &[(&str, &str)] = &[
    ("@hourly", "0 * * * *"),
    ("@daily", "0 0 * * *"),
    ("@weekly", "0 0 * * 0"),
    ("@monthly", "0 0 1 * *"),
    ("@yearly", "0 0 1 1 *"),
];

const MONTH_NAMES: &[(&str, u32)] = &[
    ("JAN", 1),
    ("FEB", 2),
    ("MAR", 3),
    ("APR", 4),
    ("MAY", 5),
    ("JUN", 6),
    ("JUL", 7),
    ("AUG", 8),
    ("SEP", 9),
    ("OCT", 10),
    ("NOV", 11),
    ("DEC", 12),
];
const MONTH_NAMES_HINT: &str = "JAN..DEC";

const DOW_NAMES: &[(&str, u32)] = &[
    ("SUN", 0),
    ("MON", 1),
    ("TUE", 2),
    ("WED", 3),
    ("THU", 4),
    ("FRI", 5),
    ("SAT", 6),
];
const DOW_NAMES_HINT: &str = "SUN..SAT";

const fn mask_range(min: u32, max: u32) -> u64 {
    let mut mask = 0u64;
    let mut value = min;
    while value <= max {
        mask |= 1u64 << value;
        value += 1;
    }
    mask
}

const FULL_DOM: u64 = mask_range(1, 31);
/// Day-of-week `*`. Bit 7 (`7 == Sunday`) is folded onto bit 0 while parsing, so the
/// unrestricted mask is 0..=6.
const FULL_DOW: u64 = mask_range(0, 6);

/// One of the five cron fields. Used to make parse errors say *where* they failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Field {
    /// Field 1, `0-59`.
    Minute,
    /// Field 2, `0-23`.
    Hour,
    /// Field 3, `1-31`.
    DayOfMonth,
    /// Field 4, `1-12`, names `JAN`-`DEC`.
    Month,
    /// Field 5, `0-6` with `7` also Sunday, names `SUN`-`SAT`.
    DayOfWeek,
}

impl Field {
    const fn label(self) -> &'static str {
        match self {
            Field::Minute => "minute",
            Field::Hour => "hour",
            Field::DayOfMonth => "day-of-month",
            Field::Month => "month",
            Field::DayOfWeek => "day-of-week",
        }
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

const ELEMENT_HINT: &str = "a value, a range `a-b`, a list `a,b`, `*` or a step `/n`";

/// Per-field parse parameters: legal numeric range and (for month/day-of-week) the name table.
#[derive(Debug, Clone, Copy)]
struct FieldSpec {
    kind: Field,
    min: u32,
    max: u32,
    names: Option<&'static [(&'static str, u32)]>,
    names_hint: &'static str,
}

impl FieldSpec {
    const MINUTE: Self = Self {
        kind: Field::Minute,
        min: 0,
        max: 59,
        names: None,
        names_hint: "",
    };
    const HOUR: Self = Self {
        kind: Field::Hour,
        min: 0,
        max: 23,
        names: None,
        names_hint: "",
    };
    const DOM: Self = Self {
        kind: Field::DayOfMonth,
        min: 1,
        max: 31,
        names: None,
        names_hint: "",
    };
    const MONTH: Self = Self {
        kind: Field::Month,
        min: 1,
        max: 12,
        names: Some(MONTH_NAMES),
        names_hint: MONTH_NAMES_HINT,
    };
    /// Day-of-week: numeric range is `0..=7` so `7` can be accepted as Sunday; the mask is
    /// normalized to `0..=6` before it is stored.
    const DOW: Self = Self {
        kind: Field::DayOfWeek,
        min: 0,
        max: 7,
        names: Some(DOW_NAMES),
        names_hint: DOW_NAMES_HINT,
    };
}

/// Parse a whole field (a comma list of elements) into a value bitmask.
fn parse_field(spec: &FieldSpec, text: &str, expr: &str) -> Result<u64, CronError> {
    let mut mask = 0u64;
    for element in text.split(',') {
        if element.is_empty() {
            return Err(CronError::EmptyElement {
                field: spec.kind,
                expr: expr.to_string(),
            });
        }
        mask |= parse_element(spec, element)?;
    }
    if spec.kind == Field::DayOfWeek && mask & (1u64 << 7) != 0 {
        // §2.1: `7` is also Sunday — fold it onto 0 so the stored mask is canonical.
        mask |= 1;
        mask &= !(1u64 << 7);
    }
    Ok(mask)
}

/// Parse one element: `*`, `a`, `a-b`, optionally followed by `/n`.
fn parse_element(spec: &FieldSpec, element: &str) -> Result<u64, CronError> {
    let (body, step) = match element.split_once('/') {
        Some((body, step_text)) => (body, Some(parse_step(spec, step_text, element)?)),
        None => (element, None),
    };

    let (lo, hi) = if body == "*" {
        (spec.min, spec.max)
    } else if let Some((lo_text, hi_text)) = body.split_once('-') {
        let lo = parse_value(spec, lo_text, element)?;
        let hi = parse_value(spec, hi_text, element)?;
        if lo > hi {
            return Err(CronError::InvertedRange {
                field: spec.kind,
                token: element.to_string(),
                lo,
                hi,
            });
        }
        (lo, hi)
    } else {
        let value = parse_value(spec, body, element)?;
        // `a/n` means "from `a` to the field maximum, every `n`" (§2.1). Without a step it is
        // a single literal.
        let hi = if step.is_some() { spec.max } else { value };
        (value, hi)
    };

    let step = step.unwrap_or(1);
    let mut mask = 0u64;
    let mut value = lo;
    while value <= hi {
        mask |= 1u64 << value;
        value += step;
    }
    Ok(mask)
}

/// Parse a step denominator: digits only, `>= 1`.
fn parse_step(spec: &FieldSpec, token: &str, element: &str) -> Result<u32, CronError> {
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_digit()) {
        return Err(CronError::Malformed {
            field: spec.kind,
            token: element.to_string(),
            expected: "a step `n` with n >= 1 after `/`",
        });
    }
    let Ok(step) = token.parse::<u32>() else {
        return Err(CronError::Malformed {
            field: spec.kind,
            token: element.to_string(),
            expected: "a step that fits in 32 bits",
        });
    };
    if step == 0 {
        return Err(CronError::ZeroStep {
            field: spec.kind,
            token: element.to_string(),
        });
    }
    Ok(step)
}

/// Parse a single value: a decimal number in range, or (month/day-of-week) a three-letter name.
fn parse_value(spec: &FieldSpec, token: &str, element: &str) -> Result<u32, CronError> {
    if token.is_empty() {
        return Err(CronError::Malformed {
            field: spec.kind,
            token: element.to_string(),
            expected: ELEMENT_HINT,
        });
    }
    if let Some(names) = spec.names
        && !token.bytes().all(|b| b.is_ascii_digit())
    {
        return match names
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(token))
        {
            Some((_, value)) => Ok(*value),
            None => Err(CronError::UnknownName {
                field: spec.kind,
                token: token.to_string(),
                expected: spec.names_hint,
            }),
        };
    }
    let Ok(value) = token.parse::<u32>() else {
        return Err(CronError::Malformed {
            field: spec.kind,
            token: element.to_string(),
            expected: "a non-negative decimal value",
        });
    };
    if value < spec.min || value > spec.max {
        return Err(CronError::ValueOutOfRange {
            field: spec.kind,
            token: element.to_string(),
            value,
            min: spec.min,
            max: spec.max,
        });
    }
    Ok(value)
}

/// A parsed, validated cron expression (CRON-MODEL §2.1).
///
/// Value fields are stored as bitmasks, which makes matching O(1) per candidate instant and
/// makes equality/hashing canonical: `@daily`, `0 0 * * *` and `*/1 0 * * *`-style spellings
/// that denote the same set of instants compare equal.
#[derive(Debug, Clone)]
pub struct CronExpr {
    source: String,
    minute_mask: u64,
    hour_mask: u64,
    dom_mask: u64,
    month_mask: u64,
    dow_mask: u64,
    dom_restricted: bool,
    dow_restricted: bool,
}

impl CronExpr {
    /// Parse a five-field expression or an `@`-alias, failing closed on anything malformed.
    ///
    /// ```
    /// # use kian_cron::CronExpr;
    /// let expr = CronExpr::parse("@daily").unwrap();
    /// assert_eq!(expr.source(), "0 0 * * *");
    /// assert!(CronExpr::parse("0 0 * *").is_err());
    /// ```
    pub fn parse(input: &str) -> Result<Self, CronError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(CronError::Empty);
        }
        // §2.2: aliases are rewritten to the five-field form *before* hashing.
        let expanded = if trimmed.starts_with('@') {
            ALIASES
                .iter()
                .find(|(alias, _)| alias.eq_ignore_ascii_case(trimmed))
                .map(|(_, expansion)| *expansion)
                .ok_or_else(|| CronError::UnknownAlias {
                    alias: trimmed.to_string(),
                })?
        } else {
            trimmed
        };

        let fields: Vec<&str> = expanded.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronError::FieldCount {
                expr: trimmed.to_string(),
                found: fields.len(),
            });
        }

        let minute_mask = parse_field(&FieldSpec::MINUTE, fields[0], trimmed)?;
        let hour_mask = parse_field(&FieldSpec::HOUR, fields[1], trimmed)?;
        let dom_mask = parse_field(&FieldSpec::DOM, fields[2], trimmed)?;
        let month_mask = parse_field(&FieldSpec::MONTH, fields[3], trimmed)?;
        let dow_mask = parse_field(&FieldSpec::DOW, fields[4], trimmed)?;

        Ok(Self {
            // Canonical text: single spaces, aliases already expanded.
            source: fields.join(" "),
            minute_mask,
            hour_mask,
            dom_mask,
            month_mask,
            dow_mask,
            // §2.1 union rule hinges on "is this field restricted?", i.e. does it denote the
            // whole range. Comparing masks (not raw text) treats `1-31` and `*` alike.
            dom_restricted: dom_mask != FULL_DOM,
            dow_restricted: dow_mask != FULL_DOW,
        })
    }

    /// The canonical five-field text (aliases expanded, whitespace normalized).
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Does this expression match the given *local* wall time?
    ///
    /// Applies the day-of-month / day-of-week union rule: when both are restricted the day
    /// matches if **either** matches; when only one is restricted only that one constrains.
    pub fn matches_local(&self, local: NaiveDateTime) -> bool {
        self.minute_mask & (1u64 << local.minute()) != 0
            && self.hour_mask & (1u64 << local.hour()) != 0
            && self.month_matches(local.date())
            && self.day_matches(local.date())
    }

    /// Bitmask of matching minutes (`0..=59`).
    pub(crate) fn minute_mask(&self) -> u64 {
        self.minute_mask
    }

    /// Bitmask of matching hours (`0..=23`).
    pub(crate) fn hour_mask(&self) -> u64 {
        self.hour_mask
    }

    /// Month restriction, independent of the day rule.
    pub(crate) fn month_matches(&self, date: NaiveDate) -> bool {
        self.month_mask & (1u64 << date.month()) != 0
    }

    /// §2.1 day rule: union when both DOM and DOW are restricted, plain conjunction otherwise
    /// (an unrestricted field has a full mask, so `&&` is correct in that branch).
    pub(crate) fn day_matches(&self, date: NaiveDate) -> bool {
        let dom_hit = self.dom_mask & (1u64 << date.day()) != 0;
        let dow_hit = self.dow_mask & (1u64 << date.weekday().num_days_from_sunday()) != 0;
        match (self.dom_restricted, self.dow_restricted) {
            (true, true) => dom_hit || dow_hit,
            _ => dom_hit && dow_hit,
        }
    }

    fn signature(&self) -> [u64; 5] {
        [
            self.minute_mask,
            self.hour_mask,
            self.dom_mask,
            self.month_mask,
            self.dow_mask,
        ]
    }
}

impl PartialEq for CronExpr {
    fn eq(&self, other: &Self) -> bool {
        self.signature() == other.signature()
    }
}

impl Eq for CronExpr {}

impl Hash for CronExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature().hash(state);
    }
}

impl std::fmt::Display for CronExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn values(mask: u64) -> Vec<u32> {
        (0..64u32)
            .filter(|value| mask & (1u64 << value) != 0)
            .collect()
    }

    fn ok(source: &str) -> CronExpr {
        CronExpr::parse(source).expect("expression should parse")
    }

    fn local(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .and_then(|date| date.and_hms_opt(hh, mm, 0))
            .expect("valid local datetime")
    }

    #[test]
    fn wildcard_expands_to_full_ranges_and_restricts_nothing() {
        let expr = ok("* * * * *");
        assert_eq!(values(expr.minute_mask()), (0..60).collect::<Vec<u32>>());
        assert_eq!(values(expr.hour_mask()), (0..24).collect::<Vec<u32>>());
        assert_eq!(values(expr.dom_mask), (1..32).collect::<Vec<u32>>());
        assert_eq!(values(expr.month_mask), (1..13).collect::<Vec<u32>>());
        assert_eq!(values(expr.dow_mask), (0..7).collect::<Vec<u32>>());
        assert!(!expr.dom_restricted && !expr.dow_restricted);
    }

    #[test]
    fn steps_lists_ranges_and_open_ranges() {
        assert_eq!(
            values(ok("*/15 * * * *").minute_mask()),
            vec![0, 15, 30, 45]
        );
        assert_eq!(values(ok("1,15,30 * * * *").minute_mask()), vec![1, 15, 30]);
        assert_eq!(
            values(ok("1-5 9-17 * * *").minute_mask()),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(
            values(ok("10-50/20 * * * *").minute_mask()),
            vec![10, 30, 50]
        );
        // `a/n` = `a-max/n`
        assert_eq!(values(ok("5/20 * * * *").minute_mask()), vec![5, 25, 45]);
        assert_eq!(values(ok("0 0 * * */2").dow_mask), vec![0, 2, 4, 6]);
    }

    #[test]
    fn names_are_case_insensitive() {
        let expr = ok("0 0 * JAN,feb *");
        assert_eq!(values(expr.month_mask), vec![1, 2]);
        let weekdays = ok("0 0 * * MON-FRI");
        assert_eq!(values(weekdays.dow_mask), vec![1, 2, 3, 4, 5]);
        // Case-insensitive alias too.
        assert_eq!(ok("@DAILY"), ok("0 0 * * *"));
    }

    #[test]
    fn seven_is_sunday() {
        assert_eq!(values(ok("0 0 * * 7").dow_mask), vec![0]);
        assert_eq!(ok("0 0 * * 7"), ok("0 0 * * 0"));
        // `0-7` denotes every day, so it is not a restriction.
        assert!(!ok("0 0 * * 0-7").dow_restricted);
    }

    #[test]
    fn aliases_expand_to_five_fields() {
        for (alias, expansion) in ALIASES {
            let parsed = ok(alias);
            assert_eq!(parsed.source(), *expansion, "alias {alias}");
            assert_eq!(
                parsed,
                ok(expansion),
                "alias {alias} must equal its expansion"
            );
        }
        assert_eq!(ok("@daily").to_string(), "0 0 * * *");
    }

    #[test]
    fn whitespace_is_normalized() {
        assert_eq!(ok("  0   0  * *  * ").source(), "0 0 * * *");
    }

    #[test]
    fn error_empty() {
        assert_eq!(CronExpr::parse("   "), Err(CronError::Empty));
    }

    #[test]
    fn error_field_count() {
        assert!(matches!(
            CronExpr::parse("* * * *"),
            Err(CronError::FieldCount { found: 4, .. })
        ));
        assert!(matches!(
            CronExpr::parse("* * * * * *"),
            Err(CronError::FieldCount { found: 6, .. })
        ));
    }

    #[test]
    fn error_unknown_alias() {
        assert_eq!(
            CronExpr::parse("@reboot"),
            Err(CronError::UnknownAlias {
                alias: "@reboot".to_string()
            })
        );
        assert!(matches!(
            CronExpr::parse("@never"),
            Err(CronError::UnknownAlias { .. })
        ));
    }

    #[test]
    fn error_value_out_of_range_names_the_field() {
        assert_eq!(
            CronExpr::parse("60 * * * *"),
            Err(CronError::ValueOutOfRange {
                field: Field::Minute,
                token: "60".to_string(),
                value: 60,
                min: 0,
                max: 59,
            })
        );
        assert!(matches!(
            CronExpr::parse("* 24 * * *"),
            Err(CronError::ValueOutOfRange {
                field: Field::Hour,
                ..
            })
        ));
        assert!(matches!(
            CronExpr::parse("0 0 0 * *"),
            Err(CronError::ValueOutOfRange {
                field: Field::DayOfMonth,
                ..
            })
        ));
        assert!(matches!(
            CronExpr::parse("0 0 * 13 *"),
            Err(CronError::ValueOutOfRange {
                field: Field::Month,
                ..
            })
        ));
        assert!(matches!(
            CronExpr::parse("0 0 * * 8"),
            Err(CronError::ValueOutOfRange {
                field: Field::DayOfWeek,
                ..
            })
        ));
    }

    #[test]
    fn error_unknown_name() {
        assert!(matches!(
            CronExpr::parse("0 0 * JANU *"),
            Err(CronError::UnknownName {
                field: Field::Month,
                ..
            })
        ));
        assert!(matches!(
            CronExpr::parse("0 0 * * MONDAY"),
            Err(CronError::UnknownName {
                field: Field::DayOfWeek,
                ..
            })
        ));
    }

    #[test]
    fn error_zero_step_inverted_range_and_empty_element() {
        assert!(matches!(
            CronExpr::parse("*/0 * * * *"),
            Err(CronError::ZeroStep {
                field: Field::Minute,
                ..
            })
        ));
        assert!(matches!(
            CronExpr::parse("5-1 * * * *"),
            Err(CronError::InvertedRange { lo: 5, hi: 1, .. })
        ));
        assert!(matches!(
            CronExpr::parse("1,,2 * * * *"),
            Err(CronError::EmptyElement { .. })
        ));
        assert!(matches!(
            CronExpr::parse("1, * * * *"),
            Err(CronError::EmptyElement { .. })
        ));
    }

    #[test]
    fn error_malformed_tokens() {
        for bad in [
            "1/ * * * *",
            "1/2/3 * * * *",
            "-5 * * * *",
            "/5 * * * *",
            "1- * * * *",
        ] {
            assert!(
                matches!(CronExpr::parse(bad), Err(CronError::Malformed { .. })),
                "expected Malformed for `{bad}`, got {:?}",
                CronExpr::parse(bad)
            );
        }
        assert!(matches!(
            CronExpr::parse("abc * * * *"),
            Err(CronError::Malformed {
                field: Field::Minute,
                ..
            })
        ));
    }

    #[test]
    fn dom_dow_union_rule() {
        // Both restricted: the 1st *or* any Monday.
        let expr = ok("0 0 1 * 1");
        assert!(expr.matches_local(local(2026, 1, 5, 0, 0)), "Monday");
        assert!(expr.matches_local(local(2026, 2, 1, 0, 0)), "the 1st");
        assert!(!expr.matches_local(local(2026, 2, 3, 0, 0)), "neither");

        // Only DOM restricted: Mondays other than the 1st do not match.
        let dom_only = ok("0 0 1 * *");
        assert!(dom_only.matches_local(local(2026, 2, 1, 0, 0)));
        assert!(!dom_only.matches_local(local(2026, 1, 5, 0, 0)));

        // Only DOW restricted: the 1st that is not a Monday does not match.
        let dow_only = ok("0 0 * * 1");
        assert!(dow_only.matches_local(local(2026, 1, 5, 0, 0)));
        assert!(!dow_only.matches_local(local(2026, 2, 1, 0, 0)));

        // Minute/hour still bind.
        assert!(!expr.matches_local(local(2026, 1, 5, 0, 1)));
    }
}
