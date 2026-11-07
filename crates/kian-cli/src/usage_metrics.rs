//! Provider token-usage metrics — a pure projection over
//! `turn/end` events, patterned on `cache_metrics.rs`.
//!
//! The loop persists the provider's `usage` object **verbatim** on
//! `turn/end` when observed (drain-activity, never context-bearing).
//! This module derives per-session and whole-store totals from those
//! payloads on read: nothing here bumps a live counter from a drain
//! task, the report never lives outside the event log, and pricing /
//! cost conversion is deliberately out of scope (the plan's §4: usage
//! is persisted verbatim; pricing is a projection-layer decision
//! outside core).
//!
//! Field spellings accepted from the verbatim usage object (the
//! openai-compat provider emits openai spellings; anthropic-style keys
//! are accepted so future providers need no core change):
//!
//! | total | primary key | accepted alias |
//! |------------------------------|------------------------------|-----------------------------------------|
//! | `input_tokens` | `input_tokens` | `prompt_tokens` |
//! | `output_tokens` | `output_tokens` | `completion_tokens` |
//! | `cache_read_input_tokens` | `cache_read_input_tokens` | `prompt_tokens_details.cached_tokens` |
//! | `cache_creation_input_tokens`| `cache_creation_input_tokens`| — |
//!
//! A `turn/end` whose usage object yields **none** of the four kinds is
//! counted under `unknown_usage_requests` (still counted under
//! `requests_with_usage` — the provider did report something, kian just
//! does not recognize the spelling).

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::InboxError;

/// Per-session usage totals derived from `turn/end` payloads.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct SessionUsageReport {
    pub session: SessionId,
    /// `turn/end` events carrying a non-null `usage` object.
    pub requests_with_usage: u64,
    /// Of those, the ones whose usage object matched none of the four
    /// recognized token kinds.
    pub unknown_usage_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
}

/// Whole-store aggregate of [`SessionUsageReport`]s (the serve
/// `/metrics` and `kian usage-stats` shape).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct UsageStatsReport {
    pub sessions: usize,
    pub requests_with_usage: u64,
    pub unknown_usage_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
}

impl UsageStatsReport {
    /// Folds one session report into the aggregate.
    pub fn add_session(&mut self, s: &SessionUsageReport) {
        self.sessions += 1;
        self.requests_with_usage += s.requests_with_usage;
        self.unknown_usage_requests += s.unknown_usage_requests;
        self.input_tokens += s.input_tokens;
        self.output_tokens += s.output_tokens;
        self.cache_read_input_tokens += s.cache_read_input_tokens;
        self.cache_creation_input_tokens += s.cache_creation_input_tokens;
    }
}

/// The recognized token kinds of one verbatim usage object. `None` when
/// the object matches none of them (an unknown spelling).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct UsageTotals {
    input_tokens: u64,
    output_tokens: u64,
    cache_read_input_tokens: u64,
    cache_creation_input_tokens: u64,
}

impl UsageTotals {
    fn recognized(&self) -> bool {
        *self != Self::default()
    }
}

/// Extracts the four recognized token kinds from a verbatim usage
/// object (see the module table for the accepted spellings). Saturating:
/// no field is summed twice from two spellings — the primary key wins.
fn usage_totals(usage: &serde_json::Value) -> UsageTotals {
    let get = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| usage.get(*k).and_then(serde_json::Value::as_u64))
    };
    UsageTotals {
        input_tokens: get(&["input_tokens", "prompt_tokens"]).unwrap_or(0),
        output_tokens: get(&["output_tokens", "completion_tokens"]).unwrap_or(0),
        cache_read_input_tokens: usage
            .get("cache_read_input_tokens")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                usage
                    .pointer("/prompt_tokens_details/cached_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or(0),
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    }
}

/// Derives one session's usage totals from its event stream (pure;
/// events must be in seq order, as read from the store).
#[must_use]
pub fn session_usage_report(session: SessionId, events: &[Event]) -> SessionUsageReport {
    let mut report = SessionUsageReport {
        session,
        ..SessionUsageReport::default()
    };
    for ev in events {
        if !matches!(ev.kind, EventKind::TurnEnd) {
            continue;
        }
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
        report.output_tokens += totals.output_tokens;
        report.cache_read_input_tokens += totals.cache_read_input_tokens;
        report.cache_creation_input_tokens += totals.cache_creation_input_tokens;
    }
    report
}

/// Derives the whole-store aggregate: every session with events,
/// rebuilt and folded into one [`UsageStatsReport`]. Optionally
/// restricted to a single session.
///
/// With `use_views` (READ-MODEL §6, P2a) the fresh `session_usage`
/// view is consulted first — identical numbers by construction (the
/// view rows ARE `session_usage_report` outputs, published atomically
/// at the store key); stale, corrupt, absent or disabled → the honest
/// full-scan fold below, which stays the truth path.
pub async fn usage_stats_report(
    store: &kian_store::EventStore,
    only: Option<SessionId>,
    use_views: bool,
) -> Result<UsageStatsReport, InboxError> {
    if use_views && let Some(entries) = crate::views::fresh_usage(store).await {
        return Ok(match only {
            None => {
                let mut report = UsageStatsReport::default();
                for entry in &entries {
                    report.add_session(&entry.report());
                }
                report
            }
            Some(session) => {
                // Same shape as the honest single-session fold:
                // one session counted, zero row when unknown.
                let mut report = UsageStatsReport::default();
                report.add_session(
                    &entries
                        .iter()
                        .find(|e| e.session == session.0.to_string())
                        .map(|e| e.report())
                        .unwrap_or_else(|| SessionUsageReport {
                            session,
                            ..SessionUsageReport::default()
                        }),
                );
                report
            }
        });
    }
    let sessions = match only {
        Some(s) => vec![s],
        None => crate::cache_metrics::sessions_with_events(store).await?,
    };
    let mut report = UsageStatsReport::default();
    for session in sessions {
        let events = store.rebuild(session).await?;
        report.add_session(&session_usage_report(session, &events));
    }
    Ok(report)
}

/// Renders the human-readable `kian usage-stats` output (deterministic:
/// a pure function of the report).
#[must_use]
pub fn render_usage_stats(report: &UsageStatsReport) -> String {
    format!(
        "sessions                {}\nrequests with usage     {}\nunknown usage requests  {}\ninput tokens            {}\noutput tokens           {}\ncache read tokens       {}\ncache creation tokens   {}\n",
        report.sessions,
        report.requests_with_usage,
        report.unknown_usage_requests,
        report.input_tokens,
        report.output_tokens,
        report.cache_read_input_tokens,
        report.cache_creation_input_tokens,
    )
}

/// Runs `kian usage-stats`: loads the config (fail-closed), opens the
/// configured store read-only-in-intent, derives the report and
/// renders it (human-readable, or JSON with `json = true`). Mirrors
/// `run_cache_stats`. Returns the printed output so callers (tests)
/// can assert on it.
pub async fn run_usage_stats(
    config_path: &std::path::Path,
    session: Option<SessionId>,
    json: bool,
) -> Result<String, String> {
    let config =
        crate::config::ServeConfig::load(config_path).map_err(|e| format!("config: {e}"))?;
    // the store follows the config's backend (read intent — no
    // mutations happen here).
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
            .ok_or_else(|| format!("`{url_env}` (the postgres URL) is not set"))?;
        kian_store::EventStore::open_postgres_read_only(&url)
            .await
            .map_err(|e| format!("store: {e}"))?
    } else {
        kian_store::EventStore::open(&config.storage.sqlite)
            .await
            .map_err(|e| format!("store: {e}"))?
    };
    // Archived first (R7, R9-08 F3): a per-session read must never
    // silently zero-report a session whose data left the online store.
    if let Some(session) = session
        && let Some(m) = store
            .archived_session(session)
            .await
            .map_err(|e| format!("archive marker read: {e}"))?
    {
        store.close().await;
        return Err(format!(
            "session {} is archived (online rows removed; package {}): \
             its usage history lives in the package — restore it first — \
             kian session import {} --db <fresh.sqlite>",
            session.0, m.package_path, m.package_path
        ));
    }
    let report = usage_stats_report(&store, session, config.read_model.views.enabled)
        .await
        .map_err(|e| format!("usage stats: {e}"))?;
    store.close().await;

    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(render_usage_stats(&report))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{SessionUsageReport, UsageStatsReport, session_usage_report, usage_stats_report};
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};

    /// Minimal event-stream builder (the cache_metrics pattern).
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

        fn push(&mut self, kind: EventKind, bytes: Value) {
            self.events.push(Event {
                seq: self.seq,
                session: self.session,
                epoch: Epoch(1),
                ts: 1_000 + self.seq,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            });
            self.seq += 1;
        }

        fn turn_end(&mut self, extra: Value) {
            let mut payload = json!({"iteration": 0, "outcome": "final_answer"});
            if let (Some(obj), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
                for (k, v) in extra {
                    obj.insert(k.clone(), v.clone());
                }
            }
            self.push(EventKind::TurnEnd, payload);
        }

        fn build(&self) -> Vec<Event> {
            self.events.clone()
        }
    }

    /// Anthropic-style and openai-style spellings both sum into the
    /// four named totals; unknown spellings count as unknown requests;
    /// turn/end events without usage never count.
    #[test]
    fn session_report_sums_recognized_usage_spellings() {
        let mut b = B::new();
        // openai spelling (what the built-in provider emits), with the
        // nested cached-tokens alias.
        b.turn_end(json!({"usage": {
            "prompt_tokens": 11, "completion_tokens": 4,
            "prompt_tokens_details": {"cached_tokens": 3}
        }}));
        // anthropic spelling.
        b.turn_end(json!({"usage": {
            "input_tokens": 100, "output_tokens": 50,
            "cache_read_input_tokens": 60, "cache_creation_input_tokens": 7
        }}));
        // A turn with no usage key: invisible here.
        b.turn_end(json!({}));
        // A usage object with an unrecognized spelling.
        b.turn_end(json!({"usage": {"tokens": "many"}}));
        // Non-turn events and null usage are skipped.
        b.push(EventKind::StepRequest, json!({"body": {}}));
        b.turn_end(json!({"usage": serde_json::Value::Null}));

        let r = session_usage_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.requests_with_usage, 3);
        assert_eq!(r.unknown_usage_requests, 1);
        assert_eq!(r.input_tokens, 111);
        assert_eq!(r.output_tokens, 54);
        assert_eq!(r.cache_read_input_tokens, 63);
        assert_eq!(r.cache_creation_input_tokens, 7);
    }

    /// The primary spelling wins — an object carrying both spellings is
    /// not double-counted.
    #[test]
    fn primary_spelling_wins_over_the_alias() {
        let mut b = B::new();
        b.turn_end(json!({"usage": {
            "input_tokens": 10, "prompt_tokens": 999,
            "output_tokens": 2, "completion_tokens": 888
        }}));
        let r = session_usage_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.input_tokens, 10);
        assert_eq!(r.output_tokens, 2);
        assert_eq!(r.unknown_usage_requests, 0);
    }

    /// The aggregate folds per-session reports across a real
    /// multi-session store (the scripted-store acceptance shape).
    #[tokio::test]
    async fn aggregate_over_a_real_multi_session_store() {
        let dir = std::env::temp_dir().join(format!("kian-c67-unit-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = kian_store::EventStore::open(dir.join("c67.db"))
            .await
            .unwrap();

        let append_all = |store: kian_store::EventStore, session: SessionId, events: Vec<Event>| async move {
            for ev in events {
                let head = store.head(session).await.unwrap();
                store
                    .append(
                        session,
                        ev,
                        kian_store::AppendGuard {
                            epoch: kian_core::model::Epoch(1),
                            expected_seq: head.next_seq,
                        },
                    )
                    .await
                    .unwrap();
            }
        };

        // Session A: two usage-bearing turns.
        let a = SessionId::new();
        let mut b = B::new();
        b.session = a;
        b.push(EventKind::SessionCreated, json!({}));
        b.turn_end(json!({"usage": {"prompt_tokens": 5, "completion_tokens": 1}}));
        b.turn_end(json!({"usage": {"input_tokens": 20, "output_tokens": 9}}));
        append_all(store.clone(), a, b.build()).await;

        // Session B: one unknown-spelling turn only.
        let c = SessionId::new();
        let mut b = B::new();
        b.session = c;
        b.push(EventKind::SessionCreated, json!({}));
        b.turn_end(json!({"usage": {"weird": true}}));
        append_all(store.clone(), c, b.build()).await;

        let report = usage_stats_report(&store, None, false).await.unwrap();
        assert_eq!(report.sessions, 2);
        assert_eq!(report.requests_with_usage, 3);
        assert_eq!(report.unknown_usage_requests, 1);
        assert_eq!(report.input_tokens, 25);
        assert_eq!(report.output_tokens, 10);
        assert_eq!(report.cache_read_input_tokens, 0);
        assert_eq!(report.cache_creation_input_tokens, 0);

        // Single-session restriction folds exactly that session.
        let one = usage_stats_report(&store, Some(a), false).await.unwrap();
        assert_eq!(
            one,
            UsageStatsReport {
                sessions: 1,
                requests_with_usage: 2,
                unknown_usage_requests: 0,
                input_tokens: 25,
                output_tokens: 10,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            }
        );
        assert_eq!(
            session_usage_report(a, &store.rebuild(a).await.unwrap()),
            SessionUsageReport {
                session: a,
                requests_with_usage: 2,
                unknown_usage_requests: 0,
                input_tokens: 25,
                output_tokens: 10,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            }
        );

        store.close().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
