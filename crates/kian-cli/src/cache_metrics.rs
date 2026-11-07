//! cache-byte stability metrics.
//!
//! The provider-independent proxy for a prefix-cache hit: within one
//! session, consecutive model requests (`step/request` audit copies, the
//! A2 bytes) must share a byte-identical message prefix. The *only*
//! thing allowed to break prefix identity is a `compact` event
//! (constraint 3: compression is the one deliberate invalidation point)
//! — any other prefix change is a bug and lands in the
//! `unexpected_prefix_change` counter.
//!
//! Pure projection over events (constraint 1): nothing here bumps a
//! live counter from a drain task; the report is *derived* from the
//! event stream on read, so it survives restarts and never lives
//! outside the log. Observability belongs to this crate (kian-cli) and
//! the orchestrator — never the core crates ("never touch").
//!
//! Comparison granularity: one canonical byte string per message
//! (`serde_json::to_vec` of the request-body message object — compact,
//! lexicographic keys, the same frozen form as
//! `kian_core::context::context_to_bytes`). Request N+1 is
//! prefix-stable against request N iff every message of N serializes to
//! identical bytes at the same position in N+1. Comparing
//! message-by-message (instead of whole-array byte prefixes) is what
//! makes "N+1 = N plus appended turns" observable: a JSON array's
//! closing `]` would otherwise defeat a raw byte-prefix check.
//!
//! Scope note: a `slot/fallback` switch changes `body.model` but not
//! the message prefix; that is a provider-side cache miss the byte
//! proxy cannot see and is deliberately not counted as an
//! invalidation cause here.

use std::collections::BTreeMap;

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::InboxError;

/// Invalidation cause: a `compact` event between two requests (the one
/// deliberate, observable prefix invalidation — constraint 3).
pub const CAUSE_COMPACT: &str = "compact";
/// Invalidation cause: the message prefix changed with **no** `compact`
/// in between — a constraint-3 violation; must stay zero.
pub const CAUSE_UNEXPECTED: &str = "unexpected_prefix_change";

/// Prefix-stability report for one session, derived from its events.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct SessionCacheReport {
    pub session: SessionId,
    /// `step/request` events observed.
    pub requests: usize,
    /// Consecutive request pairs compared (requests − 1).
    pub compared_pairs: usize,
    /// Pairs whose message prefix is byte-identical (the hit-rate
    /// numerator).
    pub prefix_stable_pairs: usize,
    /// `compact` events observed in the stream.
    pub compacts: usize,
    /// Counted invalidation causes (`compact`, `unexpected_prefix_change`).
    pub invalidation_causes: BTreeMap<String, u64>,
    /// Seq of the first unexplained prefix change, if any.
    pub first_unexpected_at: Option<u64>,
    /// `step/request` payloads without a parseable `body.messages`
    /// array (a corrupt audit copy; must stay zero).
    pub parse_errors: u64,
}

impl SessionCacheReport {
    /// The prefix-cache hit-rate proxy: stable pairs over compared
    /// pairs. `None` when fewer than two requests were observed.
    #[must_use]
    pub fn prefix_hit_rate(&self) -> Option<f64> {
        if self.compared_pairs == 0 {
            None
        } else {
            Some(self.prefix_stable_pairs as f64 / self.compared_pairs as f64)
        }
    }
}

/// Whole-store aggregate of [`SessionCacheReport`]s (the serve
/// `/metrics` and `kian cache-stats` shape).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct CacheStatsReport {
    pub sessions: usize,
    pub requests: usize,
    pub compared_pairs: usize,
    pub prefix_stable_pairs: usize,
    pub compacts: usize,
    pub invalidation_causes: BTreeMap<String, u64>,
    pub parse_errors: u64,
}

impl CacheStatsReport {
    /// Folds one session report into the aggregate.
    pub fn add_session(&mut self, s: &SessionCacheReport) {
        self.sessions += 1;
        self.requests += s.requests;
        self.compared_pairs += s.compared_pairs;
        self.prefix_stable_pairs += s.prefix_stable_pairs;
        self.compacts += s.compacts;
        self.parse_errors += s.parse_errors;
        for (cause, n) in &s.invalidation_causes {
            *self.invalidation_causes.entry(cause.clone()).or_insert(0) += n;
        }
    }

    /// The aggregate prefix-cache hit-rate proxy.
    #[must_use]
    pub fn prefix_hit_rate(&self) -> Option<f64> {
        if self.compared_pairs == 0 {
            None
        } else {
            Some(self.prefix_stable_pairs as f64 / self.compared_pairs as f64)
        }
    }
}

/// Canonical per-message byte strings of one request body, or `None`
/// when the audit copy has no `body.messages` array (counted as a
/// parse error by the caller).
fn request_message_bytes(ev: &Event) -> Option<Vec<Vec<u8>>> {
    let messages = ev.bytes.pointer("/body/messages")?.as_array()?;
    Some(
        messages
            .iter()
            .map(|m| serde_json::to_vec(m).expect("message Value serializes infallibly"))
            .collect(),
    )
}

/// True iff `prev` is an element-wise byte-identical prefix of `next`.
fn is_prefix_stable(prev: &[Vec<u8>], next: &[Vec<u8>]) -> bool {
    prev.len() <= next.len() && prev.iter().zip(next.iter()).all(|(a, b)| a == b)
}

/// Derives one session's prefix-stability report from its event
/// stream (pure; events must be in seq order, as read from the store).
#[must_use]
pub fn session_cache_report(session: SessionId, events: &[Event]) -> SessionCacheReport {
    let mut report = SessionCacheReport {
        session,
        ..SessionCacheReport::default()
    };

    // Previous request's per-message canonical bytes (if any), and
    // whether a compact landed since it.
    let mut prev: Option<Vec<Vec<u8>>> = None;
    let mut prev_seq: Option<u64> = None;
    let mut compact_since_prev = false;

    for ev in events {
        match ev.kind {
            EventKind::Compact => {
                report.compacts += 1;
                compact_since_prev = true;
            }
            EventKind::StepRequest => {
                report.requests += 1;
                let Some(cur) = request_message_bytes(ev) else {
                    report.parse_errors += 1;
                    continue;
                };
                if let (Some(prev_bytes), Some(_)) = (&prev, prev_seq) {
                    report.compared_pairs += 1;
                    if is_prefix_stable(prev_bytes, &cur) {
                        report.prefix_stable_pairs += 1;
                    } else if compact_since_prev {
                        // The one deliberate invalidation point —
                        // observable, counted, expected.
                        *report
                            .invalidation_causes
                            .entry(CAUSE_COMPACT.into())
                            .or_insert(0) += 1;
                    } else {
                        // Nothing but a compact may break the prefix
                        // (constraint 3). This counter must stay zero.
                        if report.first_unexpected_at.is_none() {
                            report.first_unexpected_at = Some(ev.seq);
                        }
                        *report
                            .invalidation_causes
                            .entry(CAUSE_UNEXPECTED.into())
                            .or_insert(0) += 1;
                    }
                }
                prev = Some(cur);
                prev_seq = Some(ev.seq);
                compact_since_prev = false;
            }
            // Everything else (turns, tool calls/results, assistant
            // messages, steer, slot events, drain/session lifecycle)
            // appends to the tail; it must never touch the prefix.
            _ => {}
        }
    }

    report
}

/// Every session with events, ascending — a listing query over the
/// events table (not metrics code in a core crate), backend-dispatched
/// since (the same statement on both dialects; only the executor
/// differs).
pub async fn sessions_with_events(
    store: &kian_store::EventStore,
) -> Result<Vec<SessionId>, InboxError> {
    let rows: Vec<(String,)> = match store {
        kian_store::EventStore::Sqlite(pool) => {
            sqlx::query_as("SELECT DISTINCT session FROM events ORDER BY session ASC")
                .fetch_all(pool)
                .await?
        }
        kian_store::EventStore::Postgres(pool) => {
            sqlx::query_as("SELECT DISTINCT session FROM events ORDER BY session ASC")
                .fetch_all(pool)
                .await?
        }
    };
    rows.into_iter()
        .map(|(s,)| {
            s.parse::<uuid::Uuid>()
                .map(SessionId)
                // A non-uuid session column means the stream is corrupt;
                // fail closed rather than skipping half a store.
                .map_err(|_| InboxError::CorruptRow {
                    session: SessionId(uuid::Uuid::nil()),
                    input_id: uuid::Uuid::nil(),
                    reason: "session column is not a uuid".into(),
                })
        })
        .collect()
}

/// Derives the whole-store aggregate: every session with events,
/// rebuilt and folded into one [`CacheStatsReport`]. Optionally
/// restricted to a single session.
///
/// With `use_views` (READ-MODEL §6, P2a) the fresh `session_cache`
/// view is consulted first — identical numbers by construction; stale,
/// corrupt, absent or disabled → the honest full-scan fold below.
pub async fn cache_stats_report(
    store: &kian_store::EventStore,
    only: Option<SessionId>,
    use_views: bool,
) -> Result<CacheStatsReport, InboxError> {
    if use_views && let Some(entries) = crate::views::fresh_cache(store).await {
        let mut report = CacheStatsReport::default();
        match only {
            None => {
                for entry in &entries {
                    report.add_session(&entry.report());
                }
            }
            Some(session) => {
                // Same shape as the honest single-session fold:
                // one session counted, zero row when unknown.
                let zero = crate::views::CacheEntry {
                    session: session.0.to_string(),
                    requests: 0,
                    compared_pairs: 0,
                    prefix_stable_pairs: 0,
                    compacts: 0,
                    invalidation_causes: BTreeMap::new(),
                    first_unexpected_at: None,
                    parse_errors: 0,
                };
                report.add_session(
                    &entries
                        .iter()
                        .find(|e| e.session == session.0.to_string())
                        .unwrap_or(&zero)
                        .report(),
                );
            }
        }
        return Ok(report);
    }
    let sessions = match only {
        Some(s) => vec![s],
        None => sessions_with_events(store).await?,
    };
    let mut report = CacheStatsReport::default();
    for session in sessions {
        let events = store.rebuild(session).await?;
        report.add_session(&session_cache_report(session, &events));
    }
    Ok(report)
}

/// Runs `kian cache-stats`: loads the config (fail-closed), opens the
/// configured sqlite store read-only-in-intent, derives the report
/// and renders it (human-readable, or JSON with `json = true`).
/// Returns the printed output so callers (tests) can assert on it.
pub async fn run_cache_stats(
    config_path: &std::path::Path,
    session: Option<SessionId>,
    json: bool,
) -> Result<String, String> {
    let config =
        crate::config::ServeConfig::load(config_path).map_err(|e| format!("config: {e}"))?;
    // the store follows the config's backend. sqlite opens the
    // configured file (read intent — no mutations happen here); postgres
    // resolves the env-only URL and opens without migrating (the report
    // is a pure projection).
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
             its cache history lives in the package — restore it first — \
             kian session import {} --db <fresh.sqlite>",
            session.0, m.package_path, m.package_path
        ));
    }
    let report = cache_stats_report(&store, session, config.read_model.views.enabled)
        .await
        .map_err(|e| format!("cache stats: {e}"))?;
    store.close().await;

    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    let rate = report
        .prefix_hit_rate()
        .map_or_else(|| "n/a".to_string(), |r| format!("{r:.3}"));
    let causes = if report.invalidation_causes.is_empty() {
        "(none)".to_string()
    } else {
        report
            .invalidation_causes
            .iter()
            .map(|(c, n)| format!("{c}={n}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Ok(format!(
        "sessions                {}\nrequests                {}\ncompared pairs          {}\nprefix-stable pairs     {} (hit rate {})\ncompacts                {}\ninvalidation causes     {}\nparse errors            {}\n",
        report.sessions,
        report.requests,
        report.compared_pairs,
        report.prefix_stable_pairs,
        rate,
        report.compacts,
        causes,
        report.parse_errors,
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        CAUSE_COMPACT, CAUSE_UNEXPECTED, cache_stats_report, is_prefix_stable, session_cache_report,
    };
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};

    /// Minimal event-stream builder for prefix-stability fixtures.
    struct B {
        session: SessionId,
        seq: u64,
        events: Vec<Event>,
    }

    impl B {
        fn new() -> Self {
            Self {
                session: SessionId(Uuid::nil()),
                // Store seqs start at 1 (head of a fresh session).
                seq: 1,
                events: Vec::new(),
            }
        }

        fn push(&mut self, kind: EventKind, bytes: Value) -> u64 {
            let seq = self.seq;
            self.events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: 1_000 + seq,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            });
            self.seq += 1;
            seq
        }

        fn request(&mut self, messages: Value) -> u64 {
            self.push(
                EventKind::StepRequest,
                json!({
                    "slot": "main", "provider": "p", "model": "m",
                    "iteration": 0,
                    "body": {"model": "m", "messages": messages, "stream": true},
                }),
            )
        }

        fn compact(&mut self, from: u64, to: u64) -> u64 {
            self.push(
                EventKind::Compact,
                json!({"from_seq": from, "to_seq": to, "summary": {"role": "assistant", "content": "s"}}),
            )
        }

        fn build(&self) -> Vec<Event> {
            self.events.clone()
        }
    }

    fn msgs(items: &[(&str, &str)]) -> Value {
        Value::Array(
            items
                .iter()
                .map(|(role, content)| json!({"role": role, "content": content}))
                .collect(),
        )
    }

    #[test]
    fn prefix_check_is_element_wise() {
        let a = vec![b"[0]".to_vec(), b"[1]".to_vec()];
        let b = vec![b"[0]".to_vec(), b"[1]".to_vec(), b"[2]".to_vec()];
        let c = vec![b"[9]".to_vec(), b"[1]".to_vec(), b"[2]".to_vec()];
        let d = vec![b"[0]".to_vec()];
        assert!(is_prefix_stable(&a, &b), "extension keeps the prefix");
        assert!(!is_prefix_stable(&a, &c), "changed head breaks it");
        assert!(!is_prefix_stable(&a, &d), "shrunk history breaks it");
    }

    /// happy path: consecutive requests with unchanged (only
    /// extended) history share the byte prefix; nothing counts as an
    /// invalidation.
    #[test]
    fn consecutive_requests_share_the_byte_prefix() {
        let mut b = B::new();
        b.request(msgs(&[("user", "q1")]));
        // Anything may land between requests: assistant replies, tool
        // transcripts, turns, slot events — all tail appends.
        b.push(EventKind::TurnStart, json!({"iteration": 0}));
        b.push(
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": "calling a tool"}}),
        );
        b.push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c1", "name": "fs.read"}}),
        );
        b.push(
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c1", "output": "data"}}),
        );
        b.push(EventKind::TurnEnd, json!({"iteration": 0}));
        b.request(msgs(&[
            ("user", "q1"),
            ("assistant", "calling a tool"),
            ("tool", "data"),
        ]));
        b.request(msgs(&[
            ("user", "q1"),
            ("assistant", "calling a tool"),
            ("tool", "data"),
            ("assistant", "final answer"),
        ]));

        let r = session_cache_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.requests, 3);
        assert_eq!(r.compared_pairs, 2);
        assert_eq!(r.prefix_stable_pairs, 2);
        assert_eq!(r.prefix_hit_rate(), Some(1.0));
        assert!(
            r.invalidation_causes.is_empty(),
            "{:?}",
            r.invalidation_causes
        );
        assert_eq!(r.first_unexpected_at, None);
        assert_eq!(r.parse_errors, 0);
    }

    /// A `compact` event changes the prefix and is counted under the
    /// `compact` cause — and only under it.
    #[test]
    fn compact_breaks_the_prefix_and_counts_as_the_sole_cause() {
        let mut b = B::new();
        b.request(msgs(&[("user", "q1"), ("assistant", "a1"), ("user", "q2")]));
        let from = 1u64; // event seqs; range [1,4) covers the middle
        b.compact(from, 4);
        b.request(msgs(&[
            ("user", "q1"),
            ("assistant", "summary"),
            ("assistant", "a3"),
        ]));

        let r = session_cache_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.compared_pairs, 1);
        assert_eq!(r.prefix_stable_pairs, 0);
        assert_eq!(r.compacts, 1);
        assert_eq!(r.invalidation_causes.get(CAUSE_COMPACT), Some(&1));
        assert_eq!(r.invalidation_causes.get(CAUSE_UNEXPECTED), None);
        assert_eq!(
            r.first_unexpected_at, None,
            "compact is expected, not a bug"
        );
    }

    /// Any prefix change without a compact is a constraint-3 violation
    /// and must surface in the `unexpected_prefix_change` counter.
    #[test]
    fn prefix_change_without_compact_is_counted_unexpected() {
        let mut b = B::new();
        b.request(msgs(&[("user", "q1")]));
        b.request(msgs(&[("user", "REWRITTEN"), ("user", "q1")]));

        let r = session_cache_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.compared_pairs, 1);
        assert_eq!(r.prefix_stable_pairs, 0);
        assert_eq!(r.invalidation_causes.get(CAUSE_UNEXPECTED), Some(&1));
        assert!(r.first_unexpected_at.is_some());
    }

    /// A compact *before* the previous request does not excuse a later
    /// prefix change: only compacts landing between the compared pair
    /// are the cause of that pair's invalidation.
    #[test]
    fn stale_compact_does_not_excuse_a_later_change() {
        let mut b = B::new();
        b.request(msgs(&[("user", "q1")]));
        b.request(msgs(&[("user", "q1"), ("assistant", "a1")])); // stable
        let from = 1u64;
        b.compact(from, 3);
        b.request(msgs(&[("user", "s1"), ("assistant", "a1")]));
        b.request(msgs(&[("user", "REWRITTEN-AGAIN")])); // no compact since

        let r = session_cache_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.compared_pairs, 3);
        assert_eq!(r.prefix_stable_pairs, 1);
        assert_eq!(r.invalidation_causes.get(CAUSE_COMPACT), Some(&1));
        assert_eq!(r.invalidation_causes.get(CAUSE_UNEXPECTED), Some(&1));
    }

    /// A request whose audit copy lacks `body.messages` counts as a
    /// parse error instead of being silently skipped.
    #[test]
    fn malformed_audit_copy_counts_a_parse_error() {
        let mut b = B::new();
        b.request(msgs(&[("user", "q1")]));
        b.push(EventKind::StepRequest, json!({"slot": "main"}));
        let r = session_cache_report(SessionId(Uuid::nil()), &b.build());
        assert_eq!(r.requests, 2);
        assert_eq!(r.compared_pairs, 0);
        assert_eq!(r.parse_errors, 1);
    }

    /// The aggregate folds per-session reports and rates.
    #[tokio::test]
    async fn aggregate_over_a_real_store() {
        let dir = std::env::temp_dir().join(format!("kian-c17-unit-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = kian_store::EventStore::open(dir.join("s3.db"))
            .await
            .unwrap();

        // A session with two stable requests via the real append path.
        let session = SessionId::new();
        let mut b = B::new();
        b.session = session;
        b.push(EventKind::SessionCreated, json!({}));
        b.request(msgs(&[("user", "q1")]));
        b.push(
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": "answer"}}),
        );
        b.request(msgs(&[("user", "q1"), ("assistant", "answer")]));
        for ev in b.build() {
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

        let report = cache_stats_report(&store, None, false).await.unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.requests, 2);
        assert_eq!(report.compared_pairs, 1);
        assert_eq!(report.prefix_stable_pairs, 1);
        assert_eq!(report.prefix_hit_rate(), Some(1.0));
        assert!(report.invalidation_causes.is_empty());

        let one = cache_stats_report(&store, Some(session), false)
            .await
            .unwrap();
        assert_eq!(one, report);

        let missing = cache_stats_report(&store, Some(SessionId::new()), false)
            .await
            .unwrap();
        assert_eq!(missing.sessions, 1, "empty stream still folds in");
        assert_eq!(missing.requests, 0);

        store.close().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
