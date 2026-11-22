//! Pricing: the config pricing book and its normalized runtime form.
//!
//! The config carries decimal prices per million tokens (USD by default);
//! the runtime normalizes them once to integer micro-USD per million
//! tokens, so every cost is exact integer arithmetic. The book is a
//! deployment fact, not event state: only the projection reads it, and a
//! price edit re-renders history (ADR-052).
//!
//! Peak windows are wall-clock intervals in an IANA timezone; a timestamp
//! is peak when it falls in any window on a listed weekday and the local
//! date (in holiday_timezone) is not a listed holiday.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Config shapes (the pricing: and model_profiles: sections)
// ---------------------------------------------------------------------------

/// The pricing: section. Absent = no price book; kian cost reports
/// unavailable rather than pricing every request at zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    /// Display currency code (USD, CNY, ...).
    #[serde(default = "default_currency")]
    pub currency: String,
    /// The unit every price in models is expressed in.
    #[serde(default)]
    pub unit: PriceUnit,
    /// The multiplier applied to every price inside a peak window.
    #[serde(default = "default_peak_multiplier")]
    pub peak_multiplier: f64,
    /// Peak wall-clock windows. Empty = everything is off-peak.
    #[serde(default)]
    pub peak_windows: Vec<PeakWindowConfig>,
    /// Holiday dates (YYYY-MM-DD in holiday_timezone) that are off-peak
    /// regardless of the weekday.
    #[serde(default)]
    pub holidays: Vec<String>,
    /// The timezone holiday dates are interpreted in; required when
    /// holidays is non-empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holiday_timezone: Option<String>,
    /// provider/model to per-million-token prices (off-peak base).
    #[serde(default)]
    pub models: BTreeMap<String, PriceEntryConfig>,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            unit: PriceUnit::default(),
            peak_multiplier: default_peak_multiplier(),
            peak_windows: Vec::new(),
            holidays: Vec::new(),
            holiday_timezone: None,
            models: BTreeMap::new(),
        }
    }
}

impl PricingConfig {
    /// True when the section is absent-equivalent (nothing to serialize and
    /// nothing to price). Used by skip_serializing_if.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
            && self.peak_windows.is_empty()
            && self.holidays.is_empty()
            && self.holiday_timezone.is_none()
            && self.currency == default_currency()
            && self.unit == PriceUnit::PerMillionTokens
            && self.peak_multiplier == default_peak_multiplier()
    }

    /// Validates and normalizes the book. Every failure names the exact
    /// config path; the caller turns it into a boot refusal.
    pub fn book(&self) -> Result<PriceBook, String> {
        let currency = self.currency.trim();
        if currency.is_empty() {
            return Err("pricing.currency must be a non-empty currency code".into());
        }
        if !self.peak_multiplier.is_finite() || self.peak_multiplier <= 0.0 {
            return Err("pricing.peak_multiplier must be a finite positive number".into());
        }

        let mut windows = Vec::with_capacity(self.peak_windows.len());
        for (i, w) in self.peak_windows.iter().enumerate() {
            if w.weekdays.is_empty() {
                return Err(format!(
                    "pricing.peak_windows[{i}].weekdays must not be empty"
                ));
            }
            let mut weekdays = Vec::with_capacity(w.weekdays.len());
            for day in &w.weekdays {
                let day = day.to_chrono();
                if !weekdays.contains(&day) {
                    weekdays.push(day);
                }
            }
            let start = parse_hhmm(&w.start, false)
                .map_err(|e| format!("pricing.peak_windows[{i}].start: {e}"))?;
            let end = parse_hhmm(&w.end, true)
                .map_err(|e| format!("pricing.peak_windows[{i}].end: {e}"))?;
            if start >= end {
                return Err(format!(
                    "pricing.peak_windows[{i}]: start must be before end"
                ));
            }
            let tz: Tz = w.timezone.parse().map_err(|_| {
                format!(
                    "pricing.peak_windows[{i}].timezone '{}' is not an IANA timezone",
                    w.timezone
                )
            })?;
            windows.push(PeakWindow {
                weekdays,
                start_minute: start,
                end_minute: end,
                tz,
            });
        }

        let holiday_tz: Tz = match self.holiday_timezone.as_deref() {
            Some(name) => name.parse().map_err(|_| {
                format!("pricing.holiday_timezone '{name}' is not an IANA timezone")
            })?,
            None => {
                if !self.holidays.is_empty() {
                    return Err(
                        "pricing.holiday_timezone is required when pricing.holidays is non-empty"
                            .into(),
                    );
                }
                chrono_tz::UTC
            }
        };
        let mut holidays = BTreeSet::new();
        for date in &self.holidays {
            let parsed = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .map_err(|_| format!("pricing.holidays entry '{date}' must be YYYY-MM-DD"))?;
            holidays.insert(parsed);
        }

        let mut entries = BTreeMap::new();
        for (key, entry) in &self.models {
            let (provider, model) = split_model_key(key)?;
            let cache_hit = price_micros(entry.cache_hit, key, "cache_hit")?;
            let cache_miss = price_micros(entry.cache_miss, key, "cache_miss")?;
            let output = price_micros(entry.output, key, "output")?;
            entries.insert(
                format!("{provider}/{model}"),
                PriceEntry {
                    cache_hit_micros: cache_hit,
                    cache_miss_micros: cache_miss,
                    output_micros: output,
                },
            );
        }

        Ok(PriceBook {
            currency: currency.to_string(),
            peak_multiplier: self.peak_multiplier,
            windows,
            holidays,
            holiday_tz,
            entries,
        })
    }
}

fn default_currency() -> String {
    "USD".into()
}

fn default_peak_multiplier() -> f64 {
    2.0
}

/// The price unit. Only per-million-token prices are meaningful today.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceUnit {
    #[default]
    PerMillionTokens,
}

/// One peak window: weekdays + a minute range in one timezone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeakWindowConfig {
    pub weekdays: Vec<WeekdayName>,
    /// HH:MM, inclusive.
    pub start: String,
    /// HH:MM or 24:00, exclusive.
    pub end: String,
    /// IANA timezone (UTC, Asia/Shanghai, ...).
    pub timezone: String,
}

/// A weekday name in config. Three-letter lowercase is canonical; the full
/// English name is accepted as an alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WeekdayName {
    #[serde(alias = "monday")]
    Mon,
    #[serde(alias = "tuesday")]
    Tue,
    #[serde(alias = "wednesday")]
    Wed,
    #[serde(alias = "thursday")]
    Thu,
    #[serde(alias = "friday")]
    Fri,
    #[serde(alias = "saturday")]
    Sat,
    #[serde(alias = "sunday")]
    Sun,
}

impl WeekdayName {
    fn to_chrono(self) -> Weekday {
        match self {
            Self::Mon => Weekday::Mon,
            Self::Tue => Weekday::Tue,
            Self::Wed => Weekday::Wed,
            Self::Thu => Weekday::Thu,
            Self::Fri => Weekday::Fri,
            Self::Sat => Weekday::Sat,
            Self::Sun => Weekday::Sun,
        }
    }
}

/// One model's prices, in the unit declared by pricing.unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceEntryConfig {
    pub cache_hit: f64,
    pub cache_miss: f64,
    pub output: f64,
}

/// The model_profiles: section: declarative capability metadata, parsed and
/// validated now, consumed by the request planner from phase 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfileConfig {
    /// The wire dialect the model speaks.
    #[serde(default)]
    pub dialect: Dialect,
    /// Maximum context window in tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    /// Maximum output tokens in one response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output: Option<u64>,
    /// Whether the model supports a thinking/reasoning mode.
    #[serde(default)]
    pub reasoning: bool,
    /// Whether the model accepts image content.
    #[serde(default)]
    pub vision: bool,
    /// Whether the model supports strict JSON-schema tool validation.
    #[serde(default)]
    pub strict: bool,
    /// Whether the provider has a prefix cache worth preserving.
    #[serde(default = "default_true")]
    pub cache: bool,
}

impl Default for ModelProfileConfig {
    fn default() -> Self {
        Self {
            dialect: Dialect::default(),
            context_window: None,
            max_output: None,
            reasoning: false,
            vision: false,
            strict: false,
            cache: true,
        }
    }
}

fn default_true() -> bool {
    true
}

impl ModelProfileConfig {
    /// Validates one profile entry; key is the provider/model config key.
    pub fn validate(&self, key: &str) -> Result<(), String> {
        split_model_key(key)?;
        if let Some(0) = self.context_window {
            return Err(format!(
                "model_profiles.\"{key}\".context_window must be greater than zero"
            ));
        }
        if let Some(0) = self.max_output {
            return Err(format!(
                "model_profiles.\"{key}\".max_output must be greater than zero"
            ));
        }
        if let (Some(context), Some(max)) = (self.context_window, self.max_output)
            && max > context
        {
            return Err(format!(
                "model_profiles.\"{key}\".max_output ({max}) must not exceed context_window ({context})"
            ));
        }
        Ok(())
    }
}

/// The model's wire dialect. Only the OpenAI chat-completions dialect exists
/// today; phase 1 adds the versioned variant without changing the config
/// grammar.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dialect {
    #[default]
    #[serde(rename = "openai_chat")]
    OpenAiChat,
}

// ---------------------------------------------------------------------------
// Runtime book
// ---------------------------------------------------------------------------

/// The normalized, validated price book (integer micro-USD per million tokens).
#[derive(Debug, Clone)]
pub struct PriceBook {
    currency: String,
    peak_multiplier: f64,
    windows: Vec<PeakWindow>,
    holidays: BTreeSet<NaiveDate>,
    holiday_tz: Tz,
    entries: BTreeMap<String, PriceEntry>,
}

/// One model's normalized prices: micro-USD per million tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceEntry {
    pub cache_hit_micros: u64,
    pub cache_miss_micros: u64,
    pub output_micros: u64,
}

/// One model's effective prices for one attempt (peak multiplier applied).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub cache_hit_micros: u64,
    pub cache_miss_micros: u64,
    pub output_micros: u64,
    pub peak: bool,
}

#[derive(Debug, Clone)]
struct PeakWindow {
    weekdays: Vec<Weekday>,
    start_minute: u32,
    end_minute: u32,
    tz: Tz,
}

impl PriceBook {
    /// The display currency.
    #[must_use]
    pub fn currency(&self) -> &str {
        &self.currency
    }

    /// True when at least one model has a price.
    #[must_use]
    pub fn has_prices(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Number of priced models (reported by the CLI).
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// The effective quote for one attempt, or None when the model has no
    /// price. The peak window is evaluated at ts_ms (the attempt's event
    /// timestamp), never at projection time.
    #[must_use]
    pub fn quote(&self, provider: &str, model: &str, ts_ms: u64) -> Option<Quote> {
        let entry = self.entries.get(&format!("{provider}/{model}"))?;
        let peak = self.is_peak(ts_ms);
        let scale = |micros: u64| {
            if peak {
                scale_micros(micros, self.peak_multiplier)
            } else {
                micros
            }
        };
        Some(Quote {
            cache_hit_micros: scale(entry.cache_hit_micros),
            cache_miss_micros: scale(entry.cache_miss_micros),
            output_micros: scale(entry.output_micros),
            peak,
        })
    }

    /// True when ts_ms falls inside a peak window and is not a holiday.
    #[must_use]
    pub fn is_peak(&self, ts_ms: u64) -> bool {
        let Some(dt) = i64::try_from(ts_ms)
            .ok()
            .and_then(DateTime::<Utc>::from_timestamp_millis)
        else {
            return false;
        };
        if self
            .holidays
            .contains(&dt.with_timezone(&self.holiday_tz).date_naive())
        {
            return false;
        }
        self.windows.iter().any(|w| {
            let local = dt.with_timezone(&w.tz);
            if !w.weekdays.contains(&local.weekday()) {
                return false;
            }
            let minute = local.hour() * 60 + local.minute();
            minute >= w.start_minute && minute < w.end_minute
        })
    }

    /// The exact cost of one usage object under quote, in micro-USD.
    /// Rounds once, half-up, after summing the three components.
    #[must_use]
    pub fn cost_micros(&self, hit: u64, miss: u64, output: u64, quote: &Quote) -> u64 {
        let numerator = (hit as u128)
            .saturating_mul(quote.cache_hit_micros as u128)
            .saturating_add((miss as u128).saturating_mul(quote.cache_miss_micros as u128))
            .saturating_add((output as u128).saturating_mul(quote.output_micros as u128));
        let rounded = numerator.saturating_add(500_000) / 1_000_000;
        u64::try_from(rounded).unwrap_or(u64::MAX)
    }
}

/// Renders micro-USD as currency integer.six digits.
#[must_use]
pub fn format_micros(currency: &str, micros: u64) -> String {
    format!(
        "{currency} {}.{:06}",
        micros / 1_000_000,
        micros % 1_000_000
    )
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn parse_hhmm(raw: &str, allow_24: bool) -> Result<u32, String> {
    let (h, m) = raw
        .split_once(':')
        .ok_or_else(|| format!("'{raw}' must be HH:MM"))?;
    if h.len() != 2
        || m.len() != 2
        || !h.chars().all(|c| c.is_ascii_digit())
        || !m.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("'{raw}' must be HH:MM"));
    }
    let hour: u32 = h.parse().map_err(|_| format!("'{raw}' must be HH:MM"))?;
    let minute: u32 = m.parse().map_err(|_| format!("'{raw}' must be HH:MM"))?;
    if allow_24 && hour == 24 && minute == 0 {
        return Ok(24 * 60);
    }
    if hour > 23 || minute > 59 {
        return Err(format!("'{raw}' must be HH:MM"));
    }
    Ok(hour * 60 + minute)
}

pub(crate) fn split_model_key(key: &str) -> Result<(&str, &str), String> {
    if key.chars().any(char::is_whitespace) {
        return Err(format!("model key '{key}' must not contain whitespace"));
    }
    let (provider, model) = key
        .split_once('/')
        .ok_or_else(|| format!("model key '{key}' must be provider/model"))?;
    if provider.is_empty() || model.is_empty() {
        return Err(format!("model key '{key}' must be provider/model"));
    }
    Ok((provider, model))
}

fn price_micros(value: f64, key: &str, field: &str) -> Result<u64, String> {
    let micros = value * 1_000_000.0;
    if !value.is_finite() || value < 0.0 || !micros.is_finite() || micros > u64::MAX as f64 {
        return Err(format!(
            "pricing.models.\"{key}\".{field} must be a finite non-negative price"
        ));
    }
    Ok(micros.round() as u64)
}

fn scale_micros(micros: u64, multiplier: f64) -> u64 {
    let scaled = micros as f64 * multiplier;
    if !scaled.is_finite() || scaled >= u64::MAX as f64 {
        u64::MAX
    } else {
        scaled.round() as u64
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        Dialect, ModelProfileConfig, PriceEntryConfig, PricingConfig, format_micros,
        split_model_key,
    };

    fn base() -> PricingConfig {
        PricingConfig {
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
            peak_windows: vec![super::PeakWindowConfig {
                weekdays: vec![super::WeekdayName::Mon, super::WeekdayName::Fri],
                start: "01:00".into(),
                end: "04:00".into(),
                timezone: "UTC".into(),
            }],
            ..PricingConfig::default()
        }
    }

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> u64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn prices_normalize_to_micro_usd() {
        let book = base().book().unwrap();
        let q = book
            .quote("deepseek", "deepseek-flash", ts(2026, 11, 23, 5, 0))
            .unwrap();
        assert!(!q.peak);
        assert_eq!(q.cache_hit_micros, 3_000);
        assert_eq!(q.cache_miss_micros, 150_000);
        assert_eq!(q.output_micros, 600_000);
        assert_eq!(book.currency(), "USD");
        assert_eq!(book.entry_count(), 1);
    }

    #[test]
    fn peak_window_uses_the_attempt_timezone_and_bounds() {
        let book = base().book().unwrap();
        assert!(book.is_peak(ts(2026, 11, 23, 2, 0)));
        assert!(!book.is_peak(ts(2026, 11, 23, 4, 0)));
        assert!(!book.is_peak(ts(2026, 11, 23, 5, 0)));
        assert!(!book.is_peak(ts(2026, 11, 24, 2, 0)));
        let q = book
            .quote("deepseek", "deepseek-flash", ts(2026, 11, 23, 2, 0))
            .unwrap();
        assert!(q.peak);
        assert_eq!(q.cache_hit_micros, 6_000);
        assert_eq!(q.cache_miss_micros, 300_000);
        assert_eq!(q.output_micros, 1_200_000);
    }

    #[test]
    fn holiday_is_off_peak_in_the_holiday_timezone() {
        let mut cfg = base();
        cfg.holidays = vec!["2026-11-23".into()];
        cfg.holiday_timezone = Some("Asia/Shanghai".into());
        let book = cfg.book().unwrap();
        assert!(!book.is_peak(ts(2026, 11, 23, 2, 0)));
        assert!(!book.is_peak(ts(2026, 11, 24, 2, 0)));
    }

    #[test]
    fn cost_is_exact_integer_math() {
        let book = base().book().unwrap();
        let q = book
            .quote("deepseek", "deepseek-flash", ts(2026, 11, 23, 5, 0))
            .unwrap();
        assert_eq!(
            book.cost_micros(1_000_000, 1_000_000, 1_000_000, &q),
            753_000
        );
        assert_eq!(format_micros("USD", 753_000), "USD 0.753000");
        assert_eq!(format_micros("USD", 123), "USD 0.000123");
    }

    #[test]
    fn invalid_timezone_and_holiday_timezone_are_refused() {
        let mut cfg = base();
        cfg.peak_windows[0].timezone = "Mars/Olympus".into();
        assert!(cfg.book().unwrap_err().contains("IANA timezone"));

        let mut cfg = base();
        cfg.holidays = vec!["2026-11-23".into()];
        assert!(cfg.book().unwrap_err().contains("holiday_timezone"));
    }

    #[test]
    fn invalid_model_keys_and_prices_are_refused() {
        let mut cfg = base();
        cfg.models.insert(
            "no-slash".into(),
            PriceEntryConfig {
                cache_hit: 0.0,
                cache_miss: 0.0,
                output: 0.0,
            },
        );
        assert!(cfg.book().unwrap_err().contains("provider/model"));

        let mut cfg = base();
        cfg.models
            .get_mut("deepseek/deepseek-flash")
            .unwrap()
            .output = -1.0;
        assert!(cfg.book().unwrap_err().contains("non-negative"));
    }

    #[test]
    fn profile_dialect_parses_the_documented_spelling() {
        let profile: ModelProfileConfig =
            serde_yaml::from_str("dialect: openai_chat\nreasoning: true\n").unwrap();
        assert_eq!(profile.dialect, Dialect::OpenAiChat);
        assert!(profile.reasoning);
        assert!(serde_yaml::from_str::<ModelProfileConfig>("dialect: openai-chat\n").is_err());
    }

    #[test]
    fn profile_bounds_are_validated() {
        let mut p = ModelProfileConfig {
            dialect: Dialect::OpenAiChat,
            context_window: Some(1000),
            max_output: Some(2000),
            ..ModelProfileConfig::default()
        };
        assert!(
            p.validate("deepseek/deepseek-flash")
                .unwrap_err()
                .contains("exceed")
        );
        p.max_output = Some(1000);
        assert!(p.validate("deepseek/deepseek-flash").is_ok());
        assert!(split_model_key("x/y").is_ok());
        assert!(split_model_key("x /y").is_err());
    }

    #[test]
    fn empty_book_has_no_prices() {
        let book = PricingConfig::default().book().unwrap();
        assert!(!book.has_prices());
        assert_eq!(book.entry_count(), 0);
        assert!(book.quote("deepseek", "deepseek-flash", 0).is_none());
    }
}
