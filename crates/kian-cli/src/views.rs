//! Materialized views for the operator surfaces (READ-MODEL §6, P2a).
//!
//! A view is a **disposable observability accelerator, never truth**
//! (R1/R8): the four views declared in READ-MODEL §6 —
//! `session_directory`, `session_usage`, `ticket_stage` and (additively
//! declared by this change, see the deviation note in §6)
//! `session_cache` — are derived by ONE rebuild pass over the store's
//! streams and published atomically per view
//! ([`kian_store::EventStore::view_publish`]: rows + freshness key +
//! digest in one transaction).
//!
//! ## Read rule (honest fallback, always)
//!
//! A view is served only when **all three** checks pass:
//!
//! 1. the stored store key equals the live store key (no append since
//!    the rebuild — exact-version freshness, not a TTL);
//! 2. the sha256 digest over the read rows equals the stored digest
//!    (a tampered or partially lost table is **discarded**, never
//!    repaired by guessing);
//! 3. every payload decodes.
//!
//! Any failure — including "the v4 tables do not exist on this
//! read-only store" — returns `None` and the caller falls back to full
//! stream derivation, which is the truth path (READ-MODEL §8: falling
//! back to the stream is always allowed; falling back from it never
//! is). The `enabled` gate (config `read_model.views.enabled`, default
//! on) hides every view the same way P1's snapshot gate does.
//!
//! ## Build rule (maintenance, under a lease)
//!
//! [`rebuild_views`] runs outside every drain under the `views:all`
//! maintenance lease (liveness only, READ-MODEL §9 — safety comes from
//! the atomic publication shape). It pins the store key FIRST, derives
//! every view from the streams read after the pin, and publishes at the
//! pinned key: an append racing the build makes the live key diverge,
//! so the just-built version reads stale and is never served.
//!
//! ## No control read consumes a view (R8)
//!
//! Views live here and in `kian-store` only. The control plane
//! (`kian-orchestrator`: append guards, claim, promotion, steer
//! eligibility, the P1 snapshot loader) never touches them — pinned by
//! `no_control_read_consumes_a_view`.
//!
//! ## Declared staleness
//!
//! Event-derived fields are exact-version fresh (check 1). Two
//! `ticket_stage` inputs are not event-derived and carry the declared
//! observability bound (§6): `pending_inbox` (an inbox row can become
//! eligible with no append, via `eligible_at`) and the derived stage's
//! dependence on it — both are as of the last rebuild. The rendered
//! board is therefore ≤ one rebuild interval old on those two fields.

use std::collections::BTreeMap;

use kian_core::events::Event;
use kian_core::model::SessionId;
use kian_core::state::project;
use kian_store::{EventStore, StoreKey, ViewRow, view_rows_digest};
use serde::{Deserialize, Serialize};

use crate::commands::CmdResult;

/// The declared view names (READ-MODEL §6; the physical rows live in
/// the v4 `view_rows` table keyed by these names).
pub const VIEW_DIRECTORY: &str = "session_directory";
pub const VIEW_USAGE: &str = "session_usage";
pub const VIEW_TICKETS: &str = "ticket_stage";
pub const VIEW_CACHE: &str = "session_cache";

/// The rebuild lease scope (READ-MODEL §9: one rebuilder per store).
const VIEW_SCOPE: &str = "views:all";
/// Liveness-only lease TTL for the rebuilder.
const VIEW_LEASE_TTL_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Payload types (the view's row shapes — serde JSON in `view_rows.payload`)
// ---------------------------------------------------------------------------

/// One `session_directory` row: the per-session facts an operator list
/// needs, from the events table (version key: the session's head).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub session: String,
    /// The projected session state (serde name, e.g. `"running"`).
    pub state: String,
    pub epoch: u64,
    pub first_ts: u64,
    pub last_ts: u64,
    pub events: u64,
    /// The session's head version this row was derived at.
    pub version: u64,
}

/// One `session_usage` row: [`crate::usage_metrics::SessionUsageReport`]'s
/// totals per session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEntry {
    pub session: String,
    pub requests_with_usage: u64,
    pub unknown_usage_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
}

/// One `session_cache` row: [`crate::cache_metrics::SessionCacheReport`]'s
/// per-session prefix-stability counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub session: String,
    pub requests: u64,
    pub compared_pairs: u64,
    pub prefix_stable_pairs: u64,
    pub compacts: u64,
    pub invalidation_causes: BTreeMap<String, u64>,
    /// Seq of the first unexplained prefix change, if any (the
    /// constraint-3 detector's pointer).
    pub first_unexpected_at: Option<u64>,
    pub parse_errors: u64,
}

/// One `ticket_stage` row: a worker session's board line. Event-derived
/// fields are exact-version fresh; `pending_inbox` (and the stage's
/// dependence on it) is as of the rebuild — the declared bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketEntry {
    pub session: String,
    pub thread: Option<String>,
    pub topic: Option<String>,
    /// The stage name (TicketStage's stable serde string).
    pub stage: String,
    pub thread_status: String,
    pub pending_inbox: u64,
    /// First-event ts; the board renders `age = now - first_ts` at
    /// read time (same formula as the honest path).
    pub first_ts: u64,
    pub events: u64,
    pub parked: Vec<String>,
}

impl TicketEntry {
    /// Converts a served view row into the board's [`TicketRow`] at
    /// read time (age recomputed from `first_ts`, exactly the honest
    /// path's formula). Fallible: a row that fails to decode into the
    /// board types is a doubt, and the caller falls back to the honest
    /// derivation — never a panic, never a guess.
    pub fn to_row(&self, now_ms: u64) -> Result<crate::commands::TicketRow, String> {
        let session: SessionId = self
            .session
            .parse()
            .map(SessionId)
            .map_err(|e| format!("view ticket row session `{}`: {e}", self.session))?;
        let thread = self
            .thread
            .as_deref()
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|e| format!("view ticket row thread `{:?}`: {e}", self.thread))?;
        let stage = kian_superagent::TicketStage::from_str_name(&self.stage)
            .ok_or_else(|| format!("view ticket row stage `{}` is not a stage name", self.stage))?;
        Ok(crate::commands::TicketRow {
            session,
            thread,
            topic: self.topic.clone(),
            stage,
            thread_status: self.thread_status.clone(),
            pending_inbox: self.pending_inbox as usize,
            age_ms: now_ms.saturating_sub(self.first_ts),
            events: self.events as usize,
            parked: self.parked.clone(),
        })
    }
}

impl UsageEntry {
    /// The entry as the report type it was derived from (the fold input
    /// of `usage_stats_report`'s view path — the same values the honest
    /// path's `session_usage_report` produces).
    #[must_use]
    pub fn report(&self) -> crate::usage_metrics::SessionUsageReport {
        crate::usage_metrics::SessionUsageReport {
            session: SessionId(self.session.parse().unwrap_or_else(|_| uuid::Uuid::nil())),
            requests_with_usage: self.requests_with_usage,
            unknown_usage_requests: self.unknown_usage_requests,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
        }
    }
}

impl CacheEntry {
    /// The entry as the report type it was derived from (the fold input
    /// of `cache_stats_report`'s view path).
    #[must_use]
    pub fn report(&self) -> crate::cache_metrics::SessionCacheReport {
        crate::cache_metrics::SessionCacheReport {
            session: SessionId(self.session.parse().unwrap_or_else(|_| uuid::Uuid::nil())),
            requests: self.requests as usize,
            compared_pairs: self.compared_pairs as usize,
            prefix_stable_pairs: self.prefix_stable_pairs as usize,
            compacts: self.compacts as usize,
            invalidation_causes: self.invalidation_causes.clone(),
            first_unexpected_at: self.first_unexpected_at,
            parse_errors: self.parse_errors,
        }
    }
}

// ---------------------------------------------------------------------------
// Derivation (pure folds shared with the honest paths — equivalence by
// construction: the builder and the fallback call the same functions)
// ---------------------------------------------------------------------------

fn directory_entry(session: SessionId, events: &[Event]) -> CmdResult<DirectoryEntry> {
    let last = events
        .last()
        .ok_or_else(|| format!("no events for {session:?}"))?;
    let state = project(events).map_err(|e| format!("directory projection: {e}"))?;
    Ok(DirectoryEntry {
        session: session.0.to_string(),
        state: serde_json::to_value(state)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default(),
        epoch: last.epoch.0,
        first_ts: events.first().expect("non-empty").ts,
        last_ts: last.ts,
        events: events.len() as u64,
        version: last.seq,
    })
}

fn usage_entry(report: &crate::usage_metrics::SessionUsageReport) -> UsageEntry {
    UsageEntry {
        session: report.session.0.to_string(),
        requests_with_usage: report.requests_with_usage,
        unknown_usage_requests: report.unknown_usage_requests,
        input_tokens: report.input_tokens,
        output_tokens: report.output_tokens,
        cache_read_input_tokens: report.cache_read_input_tokens,
        cache_creation_input_tokens: report.cache_creation_input_tokens,
    }
}

fn cache_entry(report: &crate::cache_metrics::SessionCacheReport) -> CacheEntry {
    CacheEntry {
        session: report.session.0.to_string(),
        requests: report.requests as u64,
        compared_pairs: report.compared_pairs as u64,
        prefix_stable_pairs: report.prefix_stable_pairs as u64,
        compacts: report.compacts as u64,
        invalidation_causes: report.invalidation_causes.clone(),
        first_unexpected_at: report.first_unexpected_at,
        parse_errors: report.parse_errors,
    }
}

// ---------------------------------------------------------------------------
// Rebuild (maintenance; outside drains, under the lease)
// ---------------------------------------------------------------------------

/// What one [`rebuild_views`] run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildOutcome {
    /// Sessions read and derived.
    pub sessions: usize,
    /// Views published atomically (always all four on success).
    pub views: usize,
    /// Another holder's lease was live — nothing rebuilt (R9:
    /// idempotent under retry).
    pub lease_held: bool,
}

/// Rebuilds and publishes every view under the `views:all` maintenance
/// lease (READ-MODEL §6/§9). The store key is pinned **before** any
/// stream is read; publication records that pinned key, so a racing
/// append makes this whole version stale and it is never served.
pub async fn rebuild_views(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
) -> CmdResult<RebuildOutcome> {
    if !store
        .maintenance_lease_try_acquire(VIEW_SCOPE, holder, VIEW_LEASE_TTL_MS, now_ms)
        .await
        .map_err(|e| format!("view lease: {e}"))?
    {
        return Ok(RebuildOutcome {
            sessions: 0,
            views: 0,
            lease_held: true,
        });
    }
    let outcome = rebuild_under_lease(store, now_ms).await;
    let _ = store.maintenance_lease_release(VIEW_SCOPE, holder).await;
    outcome
}

async fn rebuild_under_lease(store: &EventStore, now_ms: u64) -> CmdResult<RebuildOutcome> {
    // Pin the freshness key first: rows are only ever served at exactly
    // this store version.
    let key = store
        .view_store_key()
        .await
        .map_err(|e| format!("view store key: {e}"))?;
    let sessions = crate::cache_metrics::sessions_with_events(store)
        .await
        .map_err(|e| format!("view session listing: {e}"))?;

    let mut streams: BTreeMap<SessionId, Vec<Event>> = BTreeMap::new();
    let mut directory = Vec::with_capacity(sessions.len());
    let mut usage = Vec::with_capacity(sessions.len());
    let mut cache = Vec::with_capacity(sessions.len());
    for session in &sessions {
        let events = store
            .rebuild(*session)
            .await
            .map_err(|e| format!("view read events for {session:?}: {e}"))?;
        directory.push(row(
            &session.0.to_string(),
            directory_entry(*session, &events)?,
            &events,
        ));
        usage.push(row(
            &session.0.to_string(),
            usage_entry(&crate::usage_metrics::session_usage_report(
                *session, &events,
            )),
            &events,
        ));
        cache.push(row(
            &session.0.to_string(),
            cache_entry(&crate::cache_metrics::session_cache_report(
                *session, &events,
            )),
            &events,
        ));
        streams.insert(*session, events);
    }

    // ticket_stage: the same derivation `kian tickets` runs on its
    // honest path (equivalence by construction).
    let tickets = crate::commands::ticket_rows(store, &streams, now_ms).await?;
    let mut ticket_rows_out = Vec::with_capacity(tickets.len());
    for t in &tickets {
        let first_ts = streams
            .get(&t.session)
            .and_then(|events| events.first())
            .map(|e| e.ts)
            .ok_or_else(|| format!("ticket session {} missing from streams", t.session.0))?;
        let entry = TicketEntry {
            session: t.session.0.to_string(),
            thread: t.thread.map(|th| th.to_string()),
            topic: t.topic.clone(),
            stage: t.stage.as_str().to_string(),
            thread_status: t.thread_status.clone(),
            pending_inbox: t.pending_inbox as u64,
            first_ts,
            events: t.events as u64,
            parked: t.parked.clone(),
        };
        ticket_rows_out.push(ViewRow {
            session: t.session.0.to_string(),
            row_version: first_ts_version(&streams, &t.session),
            payload: serde_json::to_string(&entry).map_err(|e| e.to_string())?,
        });
    }

    for (view, rows) in [
        (VIEW_DIRECTORY, directory),
        (VIEW_USAGE, usage),
        (VIEW_CACHE, cache),
        (VIEW_TICKETS, ticket_rows_out),
    ] {
        store
            .view_publish(view, rows, key, now_ms)
            .await
            .map_err(|e| format!("view publish `{view}`: {e}"))?;
    }
    Ok(RebuildOutcome {
        sessions: sessions.len(),
        views: 4,
        lease_held: false,
    })
}

/// The head version of one session's stream (its row_version).
fn first_ts_version(streams: &BTreeMap<SessionId, Vec<Event>>, session: &SessionId) -> u64 {
    streams
        .get(session)
        .and_then(|events| events.last())
        .map(|e| e.seq)
        .unwrap_or(0)
}

fn row<T: Serialize>(session: &str, entry: T, events: &[Event]) -> ViewRow {
    ViewRow {
        session: session.to_string(),
        row_version: events.last().map(|e| e.seq).unwrap_or(0),
        payload: serde_json::to_string(&entry).expect("view payload serializes"),
    }
}

/// One maintenance tick (the serve-side rebuilder's cadence): rebuild
/// every view; errors are logged and skipped — the tick is bounded and
/// idempotent under retry (R9).
pub async fn views_maintenance_tick(store: &EventStore, holder: &str, now_ms: u64) {
    match rebuild_views(store, holder, now_ms).await {
        Ok(out) if out.lease_held => {}
        Ok(out) => {
            tracing::debug!(
                sessions = out.sessions,
                views = out.views,
                "view maintenance tick"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "view maintenance tick failed");
        }
    }
}

// ---------------------------------------------------------------------------
// Fresh reads (the three checks; None = honest fallback)
// ---------------------------------------------------------------------------

/// The served rows of `view` when fresh, intact and decodable — `None`
/// on ANY doubt (stale key, digest mismatch, decode failure, absent
/// tables, store error). The caller falls back to stream derivation.
async fn fresh_entries<T: for<'de> Deserialize<'de>>(
    store: &EventStore,
    view: &str,
) -> Option<Vec<T>> {
    let state = store.view_state(view).await.ok()??;
    let current = store.view_store_key().await.ok()?;
    let built = StoreKey {
        events_total: state.events_total,
        max_seq: state.max_seq,
    };
    if !built.is_current(current) {
        return None;
    }
    let rows = store.view_rows(view).await.ok()??;
    // Corruption check: the digest over exactly the bytes just read.
    if view_rows_digest(view, &rows) != state.digest {
        tracing::warn!(view, "view digest mismatch — discarding the view");
        return None;
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match serde_json::from_str(&row.payload) {
            Ok(entry) => out.push(entry),
            Err(_) => {
                tracing::warn!(view, "view payload failed to decode — discarding the view");
                return None;
            }
        }
    }
    Some(out)
}

/// The `session_directory` rows when fresh.
pub async fn fresh_directory(store: &EventStore) -> Option<Vec<DirectoryEntry>> {
    fresh_entries(store, VIEW_DIRECTORY).await
}

/// The `session_usage` rows when fresh.
pub async fn fresh_usage(store: &EventStore) -> Option<Vec<UsageEntry>> {
    fresh_entries(store, VIEW_USAGE).await
}

/// The `session_cache` rows when fresh.
pub async fn fresh_cache(store: &EventStore) -> Option<Vec<CacheEntry>> {
    fresh_entries(store, VIEW_CACHE).await
}

/// The `ticket_stage` rows when fresh.
pub async fn fresh_tickets(store: &EventStore) -> Option<Vec<TicketEntry>> {
    fresh_entries(store, VIEW_TICKETS).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    /// R8 (READ-MODEL §6): no control read may consume a view. The
    /// control plane is `kian-orchestrator` (append guards, claim,
    /// promotion, steer eligibility) plus the store's control paths;
    /// views live in `kian-store::views` and this module only. This
    /// pins that boundary at the source level: the orchestrator never
    /// references the view surface.
    #[test]
    fn no_control_read_consumes_a_view() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let orch = root.join("../kian-orchestrator/src");
        let mut offenders = Vec::new();
        let visit = |offenders: &mut Vec<String>, dir: &std::path::Path| {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "rs") {
                    let Ok(text) = std::fs::read_to_string(&path) else {
                        continue;
                    };
                    for needle in [
                        "view_rows",
                        "view_state",
                        "view_publish",
                        "fresh_tickets",
                        "fresh_usage",
                        "fresh_cache",
                        "fresh_directory",
                        "VIEW_TICKETS",
                        "VIEW_USAGE",
                        "VIEW_CACHE",
                        "VIEW_DIRECTORY",
                    ] {
                        if text.contains(needle) {
                            offenders.push(format!("{} references `{needle}`", path.display()));
                        }
                    }
                }
            }
        };
        visit(&mut offenders, &orch);
        assert!(
            offenders.is_empty(),
            "control-plane code must not consume views: {offenders:?}"
        );
    }
}
