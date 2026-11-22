//! Cost projection — a pure read over the event stream and the config
//! price book (ADR-052, phase 0).
//!
//! Nothing here is stored: a session's cost is derived on read from each
//! usage-bearing turn/end, the step/request (or slot/fallback) that names
//! the provider/model that actually produced it, and the pricing section
//! of the config. Editing a price re-renders history; the event stream
//! never records a price. A turn whose usage is unrecognized, or whose
//! model has no price, is counted as unpriced, never silently priced at
//! zero.

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::InboxError;
use serde::Serialize;

use crate::config::ServeConfig;
use crate::pricing::{PriceBook, format_micros};
use crate::usage_metrics::usage_totals;

/// Per-session cost totals derived from the event stream.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SessionCostReport {
    pub session: SessionId,
    /// The price book's display currency; null when no book is configured.
    pub currency: Option<String>,
    /// True when the config carries at least one model price.
    pub pricing_configured: bool,
    pub requests_with_usage: u64,
    /// Usage-bearing turns priced by the book.
    pub priced_requests: u64,
    /// Usage-bearing turns no price could be attached to.
    pub unpriced_requests: u64,
    /// Usage-bearing turns whose usage spelling is unrecognized.
    pub unknown_usage_requests: u64,
    pub input_tokens: u64,
    pub cache_hit_tokens: u64,
    pub cache_miss_tokens: u64,
    pub cache_creation_tokens: u64,
    pub output_tokens: u64,
    /// Priced subtotal in micro-units of the currency; null when nothing
    /// was priced.
    pub cost_micros: Option<u64>,
    /// Sorted provider/model labels that had no price (or unknown when no
    /// step/request named one).
    pub unpriced_models: Vec<String>,
    pub peak_requests: u64,
    pub off_peak_requests: u64,
}

/// Whole-store aggregate (the kian cost default shape).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct CostStatsReport {
    pub sessions: usize,
    pub currency: Option<String>,
    pub pricing_configured: bool,
    pub requests_with_usage: u64,
    pub priced_requests: u64,
    pub unpriced_requests: u64,
    pub unknown_usage_requests: u64,
    pub input_tokens: u64,
    pub cache_hit_tokens: u64,
    pub cache_miss_tokens: u64,
    pub cache_creation_tokens: u64,
    pub output_tokens: u64,
    pub cost_micros: Option<u64>,
    pub unpriced_models: Vec<String>,
    pub peak_requests: u64,
    pub off_peak_requests: u64,
}

impl CostStatsReport {
    /// Folds one session report into the aggregate.
    pub fn add_session(&mut self, s: &SessionCostReport) {
        self.sessions += 1;
        if self.currency.is_none() {
            self.currency.clone_from(&s.currency);
        }
        self.pricing_configured |= s.pricing_configured;
        self.requests_with_usage += s.requests_with_usage;
        self.priced_requests += s.priced_requests;
        self.unpriced_requests += s.unpriced_requests;
        self.unknown_usage_requests += s.unknown_usage_requests;
        self.input_tokens += s.input_tokens;
        self.cache_hit_tokens += s.cache_hit_tokens;
        self.cache_miss_tokens += s.cache_miss_tokens;
        self.cache_creation_tokens += s.cache_creation_tokens;
        self.output_tokens += s.output_tokens;
        if let Some(cost) = s.cost_micros {
            self.cost_micros = Some(self.cost_micros.unwrap_or(0).saturating_add(cost));
        }
        for model in &s.unpriced_models {
            if !self.unpriced_models.contains(model) {
                self.unpriced_models.push(model.clone());
            }
        }
        self.unpriced_models.sort();
        self.peak_requests += s.peak_requests;
        self.off_peak_requests += s.off_peak_requests;
    }
}

#[derive(Debug, Clone)]
struct Target {
    provider: String,
    model: String,
    /// The attempt's own event timestamp: peak/off-peak is decided when the
    /// request flew, not when the turn happened to settle.
    ts: u64,
}

impl Target {
    fn label(&self) -> String {
        format!("{}/{}", self.provider, self.model)
    }
}

fn target_from(value: &serde_json::Value, ts: u64) -> Option<Target> {
    Some(Target {
        provider: value.get("provider")?.as_str()?.to_string(),
        model: value.get("model")?.as_str()?.to_string(),
        ts,
    })
}

/// Derives one session's cost report from its event stream (pure; events
/// must be in seq order). The quote is evaluated at the attempt's own
/// timestamp, so a replay prices peak/off-peak exactly as it happened.
#[must_use]
pub fn session_cost_report(
    session: SessionId,
    events: &[Event],
    book: Option<&PriceBook>,
) -> SessionCostReport {
    let mut report = SessionCostReport {
        session,
        currency: book.map(|b| b.currency().to_string()),
        pricing_configured: book.is_some_and(PriceBook::has_prices),
        ..SessionCostReport::default()
    };
    let mut target: Option<Target> = None;
    for ev in events {
        match ev.kind {
            EventKind::StepRequest => {
                if let Some(t) = target_from(&ev.bytes, ev.ts) {
                    target = Some(t);
                }
            }
            EventKind::SlotFallback => {
                if let Some(t) = ev.bytes.get("to").and_then(|v| target_from(v, ev.ts)) {
                    target = Some(t);
                }
            }
            EventKind::TurnEnd => {
                let Some(usage) = ev.bytes.get("usage").filter(|u| !u.is_null()) else {
                    continue;
                };
                report.requests_with_usage += 1;
                let totals = usage_totals(usage);
                if !totals.recognized() {
                    report.unknown_usage_requests += 1;
                    continue;
                }
                report.input_tokens += totals.input_tokens;
                report.cache_hit_tokens += totals.cache_read_input_tokens;
                report.cache_miss_tokens += totals.cache_miss_input_tokens;
                report.cache_creation_tokens += totals.cache_creation_input_tokens;
                report.output_tokens += totals.output_tokens;

                let Some(t) = target.as_ref() else {
                    report.unpriced_requests += 1;
                    if !report.unpriced_models.iter().any(|m| m == "unknown") {
                        report.unpriced_models.push("unknown".to_string());
                    }
                    continue;
                };
                let Some(quote) = book.and_then(|b| b.quote(&t.provider, &t.model, t.ts)) else {
                    report.unpriced_requests += 1;
                    let label = t.label();
                    if !report.unpriced_models.contains(&label) {
                        report.unpriced_models.push(label);
                    }
                    continue;
                };
                let cost = book.expect("quote implies a book").cost_micros(
                    totals.cache_read_input_tokens,
                    totals.cache_miss_input_tokens,
                    totals.output_tokens,
                    &quote,
                );
                report.cost_micros = Some(report.cost_micros.unwrap_or(0).saturating_add(cost));
                report.priced_requests += 1;
                if quote.peak {
                    report.peak_requests += 1;
                } else {
                    report.off_peak_requests += 1;
                }
            }
            _ => {}
        }
    }
    report.unpriced_models.sort();
    report
}

/// Derives the whole-store aggregate: every session with events, rebuilt
/// and folded into one report. Optionally restricted to one session.
pub async fn cost_stats_report(
    store: &kian_store::EventStore,
    only: Option<SessionId>,
    book: Option<&PriceBook>,
) -> Result<CostStatsReport, InboxError> {
    let sessions = match only {
        Some(s) => vec![s],
        None => crate::cache_metrics::sessions_with_events(store).await?,
    };
    let mut report = CostStatsReport::default();
    for session in sessions {
        let events = store.rebuild(session).await?;
        report.add_session(&session_cost_report(session, &events, book));
    }
    Ok(report)
}

/// Renders the human-readable kian cost output (deterministic: a pure
/// function of the report).
#[must_use]
pub fn render_cost_stats(report: &CostStatsReport) -> String {
    let cost = match (&report.currency, report.cost_micros) {
        (Some(currency), Some(micros)) => format_micros(currency, micros),
        _ if !report.pricing_configured => "n/a (no pricing configured)".to_string(),
        _ => "n/a (no priced requests)".to_string(),
    };
    let mut out = format!(
        "sessions                {}\nrequests with usage     {}\npriced requests         {}\nunpriced requests       {}\nunknown usage requests  {}\ninput tokens            {}\ncache hit tokens        {}\ncache miss tokens       {}\ncache creation tokens   {}\noutput tokens           {}\npeak requests           {}\noff-peak requests       {}\ncost                    {}\n",
        report.sessions,
        report.requests_with_usage,
        report.priced_requests,
        report.unpriced_requests,
        report.unknown_usage_requests,
        report.input_tokens,
        report.cache_hit_tokens,
        report.cache_miss_tokens,
        report.cache_creation_tokens,
        report.output_tokens,
        report.peak_requests,
        report.off_peak_requests,
        cost,
    );
    if !report.unpriced_models.is_empty() {
        out.push_str(&format!(
            "unpriced models         {}\n",
            report.unpriced_models.join(",")
        ));
    }
    out
}

/// Runs kian cost: loads the config (fail-closed), opens the configured
/// store for reading, derives the report and renders it (human-readable,
/// or JSON with json = true). Mirrors run_usage_stats.
pub async fn run_cost(
    config_path: &std::path::Path,
    session: Option<SessionId>,
    json: bool,
) -> Result<String, String> {
    let config = ServeConfig::load(config_path).map_err(|e| format!("config: {e}"))?;
    let book = config.price_book().map_err(|e| format!("config: {e}"))?;
    let book = book.has_prices().then_some(&book);
    let store = if config.storage.is_postgres() {
        let url_env = &config
            .storage
            .postgres
            .as_ref()
            .expect("validated: postgres mode requires the section")
            .url_env;
        let url = std::env::var(url_env)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| format!("{url_env} (the postgres URL) is not set"))?;
        kian_store::EventStore::open_postgres_read_only(&url)
            .await
            .map_err(|e| format!("store: {e}"))?
    } else {
        kian_store::EventStore::open(&config.storage.sqlite)
            .await
            .map_err(|e| format!("store: {e}"))?
    };
    if let Some(session) = session
        && let Some(m) = store
            .archived_session(session)
            .await
            .map_err(|e| format!("archive marker read: {e}"))?
    {
        store.close().await;
        return Err(format!(
            "session {} is archived (online rows removed; package {}):              its usage history lives in the package — restore it first —              kian session import {} --db <fresh.sqlite>",
            session.0, m.package_path, m.package_path
        ));
    }
    let report = cost_stats_report(&store, session, book)
        .await
        .map_err(|e| format!("cost: {e}"))?;
    store.close().await;

    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(render_cost_stats(&report))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;
    use uuid::Uuid;

    use super::{CostStatsReport, cost_stats_report, render_cost_stats, session_cost_report};
    use crate::pricing::{PriceEntryConfig, PricingConfig};
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};

    struct B {
        session: SessionId,
        seq: u64,
        events: Vec<Event>,
    }

    impl B {
        fn new() -> Self {
            Self {
                session: SessionId(Uuid::nil()),
                seq: 1,
                events: Vec::new(),
            }
        }

        fn push_at(&mut self, kind: EventKind, ts: u64, bytes: serde_json::Value) {
            self.events.push(Event {
                seq: self.seq,
                session: self.session,
                epoch: Epoch(1),
                ts,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            });
            self.seq += 1;
        }

        fn step(&mut self, provider: &str, model: &str) {
            let ts = 1_000 + self.seq;
            self.step_at(ts, provider, model);
        }

        fn step_at(&mut self, ts: u64, provider: &str, model: &str) {
            self.push_at(
                EventKind::StepRequest,
                ts,
                json!({"provider": provider, "model": model, "body": {}}),
            );
        }

        fn fallback(&mut self, provider: &str, model: &str) {
            self.push_at(
                EventKind::SlotFallback,
                1_000 + self.seq,
                json!({"slot": "main", "from": {"provider": "a", "model": "b"}, "to": {"provider": provider, "model": model}}),
            );
        }

        fn turn(&mut self, ts: u64, usage: serde_json::Value) {
            self.push_at(
                EventKind::TurnEnd,
                ts,
                json!({"iteration": 0, "outcome": "final_answer", "usage": usage}),
            );
        }

        fn build(&self) -> Vec<Event> {
            self.events.clone()
        }
    }

    fn peak_book() -> crate::pricing::PriceBook {
        let cfg = PricingConfig {
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
            peak_windows: vec![crate::pricing::PeakWindowConfig {
                weekdays: vec![crate::pricing::WeekdayName::Mon],
                start: "01:00".into(),
                end: "04:00".into(),
                timezone: "UTC".into(),
            }],
            ..PricingConfig::default()
        };
        cfg.book().unwrap()
    }

    fn book() -> crate::pricing::PriceBook {
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
            ..PricingConfig::default()
        }
        .book()
        .unwrap()
    }

    #[test]
    fn off_peak_usage_is_priced_exactly() {
        let mut b = B::new();
        b.step("deepseek", "deepseek-flash");
        b.turn(
            1_000,
            json!({"prompt_tokens": 1000, "completion_tokens": 10,
                   "prompt_cache_hit_tokens": 800, "prompt_cache_miss_tokens": 200}),
        );
        let book = book();
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), Some(&book));
        assert_eq!(r.priced_requests, 1);
        assert_eq!(r.unpriced_requests, 0);
        // 800 * $0.003/M + 200 * $0.15/M + 10 * $0.6/M = $0.000038.
        assert_eq!(r.cost_micros, Some(38));
        assert_eq!(r.cache_hit_tokens, 800);
        assert_eq!(r.cache_miss_tokens, 200);
        assert_eq!(r.off_peak_requests, 1);
        assert!(r.unpriced_models.is_empty());
    }

    #[test]
    fn peak_window_doubles_the_quote_at_the_attempt_timestamp() {
        let mut b = B::new();
        // 2026-11-23 02:00 UTC, Monday, inside 01:00-04:00.
        let ts = chrono::Utc
            .with_ymd_and_hms(2026, 11, 23, 2, 0, 0)
            .unwrap()
            .timestamp_millis() as u64;
        b.step_at(ts, "deepseek", "deepseek-flash");
        // The turn settles after the window closed; the attempt still owns
        // the peak quote.
        let settled = ts + 4 * 60 * 60 * 1_000;
        b.turn(
            settled,
            json!({"prompt_tokens": 1000, "completion_tokens": 10,
                   "prompt_cache_hit_tokens": 800, "prompt_cache_miss_tokens": 200}),
        );
        let book = peak_book();
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), Some(&book));
        assert_eq!(r.cost_micros, Some(77));
        assert_eq!(r.peak_requests, 1);
        assert_eq!(r.off_peak_requests, 0);
    }

    #[test]
    fn an_off_peak_attempt_stays_off_peak_even_if_the_turn_settles_in_a_window() {
        let mut b = B::new();
        // 2026-11-22 12:00 UTC, Sunday, outside every window.
        let attempt = chrono::Utc
            .with_ymd_and_hms(2026, 11, 22, 12, 0, 0)
            .unwrap()
            .timestamp_millis() as u64;
        // 2026-11-23 02:00 UTC, Monday, inside 01:00-04:00.
        let settled = chrono::Utc
            .with_ymd_and_hms(2026, 11, 23, 2, 0, 0)
            .unwrap()
            .timestamp_millis() as u64;
        b.step_at(attempt, "deepseek", "deepseek-flash");
        b.turn(
            settled,
            json!({"prompt_tokens": 1000, "completion_tokens": 10,
                   "prompt_cache_hit_tokens": 800, "prompt_cache_miss_tokens": 200}),
        );
        let book = peak_book();
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), Some(&book));
        assert_eq!(r.cost_micros, Some(38));
        assert_eq!(r.peak_requests, 0);
        assert_eq!(r.off_peak_requests, 1);
    }

    #[test]
    fn fallback_target_owns_the_turn_cost() {
        let mut b = B::new();
        b.step("primary", "unpriced");
        b.fallback("deepseek", "deepseek-flash");
        b.turn(
            1_000,
            json!({"prompt_tokens": 10, "completion_tokens": 1,
                   "prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 10}),
        );
        let book = book();
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), Some(&book));
        assert_eq!(r.priced_requests, 1);
        assert_eq!(r.unpriced_requests, 0);
        assert_eq!(r.cost_micros, Some(2));
    }

    #[test]
    fn unpriced_and_unknown_usage_are_counted_not_priced() {
        let mut b = B::new();
        b.step("deepseek", "deepseek-flash");
        b.turn(
            1_000,
            json!({"prompt_tokens": 10, "completion_tokens": 1,
                             "prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 10}),
        );
        b.step("other", "model");
        b.turn(1_000, json!({"prompt_tokens": 10, "completion_tokens": 1}));
        b.turn(1_000, json!({"weird": true}));
        let book = book();
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), Some(&book));
        assert_eq!(r.requests_with_usage, 3);
        assert_eq!(r.priced_requests, 1);
        assert_eq!(r.unpriced_requests, 1);
        assert_eq!(r.unknown_usage_requests, 1);
        assert_eq!(r.unpriced_models, vec!["other/model".to_string()]);
        assert_eq!(r.cost_micros, Some(2));
    }

    #[test]
    fn absent_book_reports_no_cost() {
        let mut b = B::new();
        b.step("deepseek", "deepseek-flash");
        b.turn(1_000, json!({"prompt_tokens": 10, "completion_tokens": 1}));
        let r = session_cost_report(SessionId(Uuid::nil()), &b.build(), None);
        assert_eq!(r.requests_with_usage, 1);
        assert_eq!(r.unpriced_requests, 1);
        assert_eq!(r.cost_micros, None);
        assert!(!r.pricing_configured);
        let rendered = render_cost_stats(&CostStatsReport {
            sessions: 1,
            ..CostStatsReport::default()
        });
        assert!(rendered.contains("n/a (no pricing configured)"));
    }

    #[tokio::test]
    async fn aggregate_over_a_real_store_folds_sessions() {
        let dir = std::env::temp_dir().join(format!("kian-cost-unit-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = kian_store::EventStore::open(dir.join("cost.db"))
            .await
            .unwrap();
        let a = SessionId::new();
        let mut b = B::new();
        b.session = a;
        b.push_at(EventKind::SessionCreated, 1, json!({}));
        b.step("deepseek", "deepseek-flash");
        b.turn(
            1_000,
            json!({"prompt_tokens": 10, "completion_tokens": 1,
                             "prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 10}),
        );
        for ev in b.build() {
            let head = store.head(a).await.unwrap();
            store
                .append(
                    a,
                    ev,
                    kian_store::AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: head.next_seq,
                    },
                )
                .await
                .unwrap();
        }
        let book = book();
        let report = cost_stats_report(&store, None, Some(&book)).await.unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.priced_requests, 1);
        assert_eq!(report.cost_micros, Some(2));
        assert_eq!(report.currency.as_deref(), Some("USD"));
        store.close().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
