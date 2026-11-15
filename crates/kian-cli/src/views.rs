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
//! R9-10 made the rebuild **incremental**: a session whose head is
//! unchanged since the last publication is reused, not re-derived (a
//! row whose `row_version` equals the live head was derived from an
//! identical stream — streams are append-only under CAS guards, so the
//! head determines the stream). An unchanged store is a no-op tick;
//! per-tick work is proportional to the *changed* sessions. Any doubt
//! about the stored views (absent state, digest mismatch, undecodable
//! ticket payload) falls back to [`rebuild_views_full`] — the self-heal
//! a per-tick full rebuild used to give. The `ticket_stage` view is
//! all-or-nothing (its global folds read board-kind events across
//! every session, and `pending_inbox` moves with `eligible_at` alone):
//! a board-kind change, a deletion, or a pending drift re-derives the
//! whole board from a kind-filtered read plus the worker streams —
//! never from every session's full stream.
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
//! board is therefore ≤ one rebuild interval old on those two fields
//! (the incremental tick compares live pending counts with the stored
//! rows every beat, so the bound holds without a full re-derivation).

use std::collections::BTreeMap;

use kian_core::events::{Event, EventKind};
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
/// The task board (R17 C4, READ-MODEL §6's additive row,
/// MULTIAGENT-MODEL §4.2): one row per delegation task. Published
/// only while `superagent.board.enabled && read_model.views.enabled`
/// (pin P3-1); rows found while the switch is off are deleted by the
/// next rebuild — a disposable view, so a later re-enable cannot serve
/// a stale era.
pub const VIEW_TASK_BOARD: &str = "task_board";

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

/// One `task_board` row (R17 C4, pin P3-3): the stored form of
/// [`crate::commands::TaskBoardRow`], keyed by `task_id`. Event-derived
/// columns are exact-version fresh; `input_required` (derived from the
/// parent's durable inbox report row, the one non-event input — pin
/// P3-5) is as of the last rebuild: the declared bound
/// `B = read_model.views.interval_ms`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskBoardEntry {
    pub task_id: String,
    pub parent: String,
    pub child: String,
    pub thread: String,
    pub assignee: Option<String>,
    pub attempt: u64,
    pub outcome: String,
    pub thread_status: String,
    pub gate: String,
    pub evidence: Option<crate::commands::TaskEvidence>,
    pub evidence_provenance: String,
    pub group: Option<String>,
    pub depends_on: Vec<String>,
}

impl TaskBoardEntry {
    /// Builds the stored entry from the derivation's row (the build
    /// side of the equivalence; the fields are the same facts).
    #[must_use]
    pub fn from_row(row: crate::commands::TaskBoardRow) -> Self {
        Self {
            task_id: row.task_id,
            parent: row.parent,
            child: row.child,
            thread: row.thread,
            assignee: row.assignee,
            attempt: row.attempt,
            outcome: row.outcome,
            thread_status: row.thread_status,
            gate: row.gate,
            evidence: row.evidence,
            evidence_provenance: row.evidence_provenance,
            group: row.group,
            depends_on: row.depends_on,
        }
    }

    /// Converts a served view row into the board's
    /// [`crate::commands::TaskBoardRow`]. Fallible: a row whose
    /// vocabulary values do not decode (an unknown outcome, gate or
    /// provenance spelling — a tampered or future-shaped payload) is a
    /// doubt, and the caller falls back to the honest derivation —
    /// never a panic, never a guess.
    pub fn to_row(&self) -> Result<crate::commands::TaskBoardRow, String> {
        let known_outcome = self.outcome == "outstanding"
            || kian_superagent::TaskOutcome::parse(&self.outcome).is_some();
        if !known_outcome {
            return Err(format!(
                "view task row outcome `{}` is not a vocabulary spelling",
                self.outcome
            ));
        }
        if !matches!(self.gate.as_str(), "pass" | "fail" | "absent") {
            return Err(format!(
                "view task row gate `{}` is not pass/fail/absent",
                self.gate
            ));
        }
        if self.evidence_provenance != "derived" {
            return Err(format!(
                "view task row evidence_provenance `{}` is not `derived` (P3)",
                self.evidence_provenance
            ));
        }
        Ok(crate::commands::TaskBoardRow {
            task_id: self.task_id.clone(),
            parent: self.parent.clone(),
            child: self.child.clone(),
            thread: self.thread.clone(),
            assignee: self.assignee.clone(),
            attempt: self.attempt,
            outcome: self.outcome.clone(),
            thread_status: self.thread_status.clone(),
            gate: self.gate.clone(),
            evidence: self.evidence.clone(),
            evidence_provenance: self.evidence_provenance.clone(),
            group: self.group.clone(),
            depends_on: self.depends_on.clone(),
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
    /// Sessions covered by the published views (the live row set).
    pub sessions: usize,
    /// Sessions actually read and folded this tick (R9-10): unchanged
    /// sessions are reused, not re-derived — the per-tick cost is
    /// proportional to this number, not to total history.
    pub derived: usize,
    /// Views published atomically (all four on a real tick; `0` when
    /// the tick was a no-op — nothing changed, nothing published).
    pub views: usize,
    /// Whether the ticket board was re-derived this tick (a board-kind
    /// change, a deletion, or a `pending_inbox` drift).
    pub board_refreshed: bool,
    /// Another holder's lease was live — nothing rebuilt (R9:
    /// idempotent under retry).
    pub lease_held: bool,
}

/// Rebuilds and publishes every view under the `views:all` maintenance
/// lease (READ-MODEL §6/§9), **incrementally** (R9-10): sessions whose
/// head moved since the last publication are re-derived; every other
/// stored row is reused verbatim (a row whose `row_version` equals the
/// live head was derived from an identical stream — streams are
/// append-only under CAS guards, so the head determines the stream).
/// An unchanged store is a no-op tick. Any doubt about the stored
/// views (absent state, digest mismatch, undecodable ticket payload)
/// falls back to [`rebuild_views_full`] — the self-heal a per-tick
/// full rebuild used to give.
///
/// The store key is pinned **before** any read; publication records
/// that pinned key, so an append racing the build makes this whole
/// version stale and it is never served. Read semantics are unchanged
/// (R1/R8): the merged row set is byte-identical to a full rebuild's.
///
/// Board off (the default): the `task_board` view is not published,
/// and any rows from an earlier enabled era are deleted in the same
/// tick (pin P3-1 — a disposable view).
pub async fn rebuild_views(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
) -> CmdResult<RebuildOutcome> {
    rebuild_views_with(store, holder, now_ms, false).await
}

/// [`rebuild_views`] with the task-board switch carried in (R17 C4,
/// pin P3-1): `board_enabled` is `superagent.board.enabled &&
/// read_model.views.enabled` as the caller (the serve-side
/// `ViewsMaintenance` plan) computed it — the machinery switch
/// already gated the ticker, so this arm only composes the two.
pub async fn rebuild_views_with(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
    board_enabled: bool,
) -> CmdResult<RebuildOutcome> {
    if !store
        .maintenance_lease_try_acquire(VIEW_SCOPE, holder, VIEW_LEASE_TTL_MS, now_ms)
        .await
        .map_err(|e| format!("view lease: {e}"))?
    {
        return Ok(RebuildOutcome {
            sessions: 0,
            derived: 0,
            views: 0,
            board_refreshed: false,
            lease_held: true,
        });
    }
    let outcome = incremental_under_lease(store, now_ms, board_enabled).await;
    let _ = store.maintenance_lease_release(VIEW_SCOPE, holder).await;
    outcome
}

/// The pre-R9-10 rebuild: re-derive every session's whole stream and
/// republish all four views. Still the first-build path, the fallback
/// on any doubt about the stored views, and the equivalence baseline
/// for the incremental path's tests and benchmarks.
pub async fn rebuild_views_full(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
) -> CmdResult<RebuildOutcome> {
    rebuild_views_full_with(store, holder, now_ms, false).await
}

/// [`rebuild_views_full`] with the task-board switch carried in
/// (R17 C4): the fifth view publishes only while the switch is on;
/// off, any stored rows are deleted (pin P3-1).
pub async fn rebuild_views_full_with(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
    board_enabled: bool,
) -> CmdResult<RebuildOutcome> {
    if !store
        .maintenance_lease_try_acquire(VIEW_SCOPE, holder, VIEW_LEASE_TTL_MS, now_ms)
        .await
        .map_err(|e| format!("view lease: {e}"))?
    {
        return Ok(RebuildOutcome {
            sessions: 0,
            derived: 0,
            views: 0,
            board_refreshed: false,
            lease_held: true,
        });
    }
    let outcome = rebuild_under_lease(store, now_ms, board_enabled).await;
    let _ = store.maintenance_lease_release(VIEW_SCOPE, holder).await;
    outcome
}

/// Reads one view's stored rows and verifies the digest over exactly
/// the bytes read. `None` on any doubt (absent tables/state, digest
/// mismatch, store error) — the caller falls back to the full rebuild.
async fn stored_view_verified(store: &EventStore, view: &str) -> Option<Vec<ViewRow>> {
    let state = store.view_state(view).await.ok()??;
    let rows = store.view_rows(view).await.ok()??;
    if view_rows_digest(view, &rows) != state.digest {
        tracing::warn!(view, "stored view digest mismatch — full rebuild");
        return None;
    }
    Some(rows)
}

/// One incremental tick under the lease. See [`rebuild_views`].
async fn incremental_under_lease(
    store: &EventStore,
    now_ms: u64,
    board_enabled: bool,
) -> CmdResult<RebuildOutcome> {
    // Pin the freshness key first: rows are only ever served at exactly
    // this store version.
    let key = store
        .view_store_key()
        .await
        .map_err(|e| format!("view store key: {e}"))?;

    // The reuse base: all four views must be present and digest-clean.
    // Any doubt → the full rebuild (self-heal).
    let (Some(directory), Some(usage), Some(cache), Some(tickets)) = (
        stored_view_verified(store, VIEW_DIRECTORY).await,
        stored_view_verified(store, VIEW_USAGE).await,
        stored_view_verified(store, VIEW_CACHE).await,
        stored_view_verified(store, VIEW_TICKETS).await,
    ) else {
        return rebuild_under_lease(store, now_ms, board_enabled).await;
    };

    // The task board's own reuse base (R17 C4, pin P3-1): board off ⇒
    // the view is disposable — any rows from an earlier enabled era
    // are deleted in this same tick, so a later re-enable cannot serve
    // a stale era. Board on but absent/corrupt stored rows ⇒ the
    // board re-derives (the self-heal the four views just got).
    let task_board_stored = if board_enabled {
        stored_view_verified(store, VIEW_TASK_BOARD).await
    } else {
        drop_task_board_if_present(store).await?;
        None
    };

    // The change detector: live heads vs the stored row versions (all
    // three per-session views carry the same head as row_version; a
    // disagreement anywhere counts as a change — the set must be
    // internally consistent to be a reuse base).
    let heads: BTreeMap<String, u64> = store
        .session_heads()
        .await
        .map_err(|e| format!("view session heads: {e}"))?
        .into_iter()
        .collect();
    let stored_head = |rows: &Vec<ViewRow>, session: &str| {
        rows.binary_search_by(|r| r.session.as_str().cmp(session))
            .ok()
            .map(|i| rows[i].row_version)
    };
    let changed: Vec<String> = heads
        .iter()
        .filter(|(session, head)| {
            stored_head(&directory, session) != Some(**head)
                || stored_head(&usage, session) != Some(**head)
                || stored_head(&cache, session) != Some(**head)
        })
        .map(|(session, _)| session.clone())
        .collect();
    let gone: Vec<String> = directory
        .iter()
        .filter(|r| !heads.contains_key(&r.session))
        .map(|r| r.session.clone())
        .collect();

    // `pending_inbox` drift (the §6 declared bound): an inbox row can
    // become eligible with no append, so the live per-worker counts are
    // compared with the stored ticket rows every tick.
    let pending: BTreeMap<String, u64> = store
        .pending_input_counts(now_ms)
        .await
        .map_err(|e| format!("view pending counts: {e}"))?
        .into_iter()
        .collect();
    let mut tickets_drift = false;
    for row in &tickets {
        let entry: TicketEntry = match serde_json::from_str(&row.payload) {
            Ok(entry) => entry,
            Err(_) => {
                tickets_drift = true; // undecodable → force the board refresh
                break;
            }
        };
        let live = pending.get(&row.session).copied().unwrap_or(0);
        if live != entry.pending_inbox {
            tickets_drift = true;
            break;
        }
    }

    // The idle tick: nothing changed anywhere and the board inputs are
    // as stored — publish nothing (the views are already fresh at this
    // key; the default-on ticker costs one heads scan, one pending
    // scan and four digest verifications). The task board's own doubt
    // (absent stored rows while the switch is on) keeps the tick busy.
    let mut task_board_touched = board_enabled && (task_board_stored.is_none() || !gone.is_empty());
    if changed.is_empty() && gone.is_empty() && !tickets_drift && !task_board_touched {
        return Ok(RebuildOutcome {
            sessions: directory.len(),
            derived: 0,
            views: 0,
            board_refreshed: false,
            lease_held: false,
        });
    }

    // Derive ONLY the changed sessions (yield between sessions so a
    // large change set cannot monopolize the runtime worker — R9-10).
    // Board trigger, refined to the DELTA: the board's global folds
    // read board-kind events only, so a plain session growing by
    // non-board events cannot move the board; but a session already on
    // the board has row fields derived from its whole stream (`events`
    // count, stage inputs), so ANY append to it refreshes the board.
    let mut board_touched = !gone.is_empty() || tickets_drift;
    let mut new_directory = Vec::with_capacity(changed.len());
    let mut new_usage = Vec::with_capacity(changed.len());
    let mut new_cache = Vec::with_capacity(changed.len());
    for session in &changed {
        let sid: SessionId = session
            .parse()
            .map(SessionId)
            .map_err(|e| format!("view session id `{session}`: {e}"))?;
        let events = store
            .rebuild(sid)
            .await
            .map_err(|e| format!("view read events for {sid:?}: {e}"))?;
        let prev_head = stored_head(&directory, session).unwrap_or(0);
        let on_board = stored_head(&tickets, session).is_some();
        if on_board
            || events
                .iter()
                .any(|e| e.seq > prev_head && crate::commands::is_board_event(e))
        {
            board_touched = true;
        }
        // The task board's own trigger (R17 C4): its folds read the
        // five task-board kinds, so only a delta carrying one of them
        // can move the board (the outcome's non-trigger inputs —
        // `turn/end` error classes, `control/interrupt` — only matter
        // beside a completion or an end, which are themselves
        // triggers; `input_required` drift is the declared bound B).
        if board_enabled
            && events
                .iter()
                .any(|e| e.seq > prev_head && crate::commands::is_task_board_event(e))
        {
            task_board_touched = true;
        }
        new_directory.push(row(session, directory_entry(sid, &events)?, &events));
        new_usage.push(row(
            session,
            usage_entry(&crate::usage_metrics::session_usage_report(sid, &events)),
            &events,
        ));
        new_cache.push(row(
            session,
            cache_entry(&crate::cache_metrics::session_cache_report(sid, &events)),
            &events,
        ));
        tokio::task::yield_now().await;
    }

    // Merge: reused rows (unchanged, minus gone) ++ the new rows, then
    // publish each view as ONE delta transaction at the pinned key —
    // upserts for the changed sessions, deletes for the gone ones, and
    // the state flip to the merged set's digest.
    let merged = |stored: Vec<ViewRow>, fresh: Vec<ViewRow>| {
        let mut rows: Vec<ViewRow> = stored
            .into_iter()
            .filter(|r| !gone.contains(&r.session) && !changed.iter().any(|c| c == &r.session))
            .collect();
        rows.extend(fresh);
        rows.sort_by(|a, b| a.session.cmp(&b.session));
        rows
    };
    let merged_directory = merged(directory, new_directory);
    let merged_usage = merged(usage, new_usage);
    let merged_cache = merged(cache, new_cache);
    for (view, all_rows) in [
        (VIEW_DIRECTORY, &merged_directory),
        (VIEW_USAGE, &merged_usage),
        (VIEW_CACHE, &merged_cache),
    ] {
        store
            .view_publish_delta(
                view,
                // upserts: only the changed sessions' rows (unchanged
                // rows stay physically in place; the merged set's
                // digest covers exactly the bytes that remain)
                &all_rows
                    .iter()
                    .filter(|r| changed.iter().any(|c| c == &r.session))
                    .cloned()
                    .collect::<Vec<_>>(),
                &gone,
                &view_rows_digest(view, all_rows),
                key,
                now_ms,
            )
            .await
            .map_err(|e| format!("view publish delta `{view}`: {e}"))?;
    }

    // The ticket board: an all-or-nothing view (its global folds read
    // board-kind events across every session). Board inputs moved, a
    // session went away, or pending drifted → re-derive the whole
    // board from the kind-filtered read + worker streams. Otherwise
    // the stored rows are exact — flip ONLY the state to the new key
    // (a delta publication with no row writes; the rows and their
    // digest are unchanged) so the freshness gate keeps holding.
    let board_refreshed = board_touched;
    if board_touched {
        let kinds = crate::commands::board_event_kind_names();
        let board_events = store
            .events_where_kinds(&kinds)
            .await
            .map_err(|e| format!("view board events: {e}"))?;
        let mut worker_streams = BTreeMap::new();
        for worker in kian_superagent::worker_sessions(&board_events) {
            worker_streams.insert(
                worker,
                store
                    .rebuild(worker)
                    .await
                    .map_err(|e| format!("view read events for {worker:?}: {e}"))?,
            );
            tokio::task::yield_now().await;
        }
        let mut ticket_rows_out = Vec::new();
        for t in
            crate::commands::ticket_rows_from(store, &board_events, &worker_streams, now_ms).await?
        {
            let first_ts = worker_streams
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
                row_version: first_ts_version(&worker_streams, &t.session),
                payload: serde_json::to_string(&entry).map_err(|e| e.to_string())?,
            });
        }
        store
            .view_publish(VIEW_TICKETS, ticket_rows_out, key, now_ms)
            .await
            .map_err(|e| format!("view publish `{VIEW_TICKETS}`: {e}"))?;
    } else {
        store
            .view_publish_delta(
                VIEW_TICKETS,
                &[],
                &[],
                &view_rows_digest(VIEW_TICKETS, &tickets),
                key,
                now_ms,
            )
            .await
            .map_err(|e| format!("view publish delta `{VIEW_TICKETS}`: {e}"))?;
    }

    // The task board (R17 C4, pin P3-3): an all-or-nothing view over
    // the same shape of inputs as the ticket board — its global folds
    // read the five task-board kinds across every session, plus the
    // parents' durable inbox report rows (pin P3-5, read live inside
    // the derivation). Touched ⇒ re-derive the whole board from the
    // kind-filtered read plus the discovered children's FULL streams
    // (the outcome fold reads `turn/end` classes and interrupts);
    // otherwise the stored rows are exact — flip ONLY the state to the
    // new key so the freshness gate keeps holding.
    if board_enabled {
        if task_board_touched {
            let kinds = crate::commands::task_board_event_kind_names();
            let task_events = store
                .events_where_kinds(&kinds)
                .await
                .map_err(|e| format!("view task board events: {e}"))?;
            let mut filtered: BTreeMap<SessionId, Vec<Event>> = BTreeMap::new();
            for event in task_events {
                filtered.entry(event.session).or_default().push(event);
            }
            // The discovered children need their FULL streams; every
            // other involved session (the parents) contributes its
            // kind-filtered subset — an equivalent input for discovery
            // and the attempt ordinal, which read thread/started only.
            let mut children = std::collections::BTreeSet::new();
            for events in filtered.values() {
                for event in events.iter().filter(|e| e.kind == EventKind::ThreadStarted) {
                    let carries_identity = event
                        .bytes
                        .get("task")
                        .and_then(|t| t.get("content"))
                        .and_then(|w| kian_superagent::worker::spawn_identity_of(w).map(|_| ()))
                        .is_some();
                    if !carries_identity {
                        continue;
                    }
                    if let Some(worker) = event
                        .bytes
                        .get("worker")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
                    {
                        children.insert(SessionId(worker));
                    }
                }
            }
            let mut task_streams: BTreeMap<SessionId, Vec<Event>> = BTreeMap::new();
            for child in children {
                task_streams.insert(
                    child,
                    store
                        .rebuild(child)
                        .await
                        .map_err(|e| format!("view read events for {child:?}: {e}"))?,
                );
                tokio::task::yield_now().await;
            }
            for (session, events) in filtered {
                task_streams.entry(session).or_insert(events);
            }
            publish_task_board(store, &task_streams, key, now_ms).await?;
        } else {
            let rows = task_board_stored.unwrap_or_default();
            store
                .view_publish_delta(
                    VIEW_TASK_BOARD,
                    &[],
                    &[],
                    &view_rows_digest(VIEW_TASK_BOARD, &rows),
                    key,
                    now_ms,
                )
                .await
                .map_err(|e| format!("view publish delta `{VIEW_TASK_BOARD}`: {e}"))?;
        }
    }

    Ok(RebuildOutcome {
        sessions: merged_directory.len(),
        derived: changed.len(),
        views: 4 + usize::from(board_enabled),
        board_refreshed,
        lease_held: false,
    })
}

/// Derives the task board over `streams` and publishes it atomically
/// at the pinned key (R17 C4, pin P3-3): one row per task, keyed by
/// `task_id`, `row_version` = the child stream's head. The derivation
/// is exactly the honest path's ([`crate::commands::task_board_rows_from`])
/// — equivalence by construction.
async fn publish_task_board(
    store: &EventStore,
    streams: &BTreeMap<SessionId, Vec<Event>>,
    key: StoreKey,
    now_ms: u64,
) -> CmdResult<()> {
    let rows = crate::commands::task_board_rows_from(store, streams).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let child: uuid::Uuid = row
            .child
            .parse()
            .map_err(|e| format!("task board child id `{}`: {e}", row.child))?;
        let version = streams
            .get(&SessionId(child))
            .and_then(|events| events.last())
            .map(|e| e.seq)
            .unwrap_or(0);
        out.push(ViewRow {
            session: row.task_id.clone(),
            row_version: version,
            payload: serde_json::to_string(&TaskBoardEntry::from_row(row))
                .map_err(|e| e.to_string())?,
        });
    }
    store
        .view_publish(VIEW_TASK_BOARD, out, key, now_ms)
        .await
        .map_err(|e| format!("view publish `{VIEW_TASK_BOARD}`: {e}"))?;
    Ok(())
}

/// Deletes any stored `task_board` rows + state (R17 C4, pin P3-1): a
/// disabled board is a disposable view — nothing is published and
/// nothing is served, and a later re-enable cannot serve a stale era.
/// No-op when the view was never built (including a pre-v4 store,
/// where `view_state` reads `None`).
async fn drop_task_board_if_present(store: &EventStore) -> CmdResult<()> {
    let present = store
        .view_state(VIEW_TASK_BOARD)
        .await
        .map_err(|e| format!("view state `{VIEW_TASK_BOARD}`: {e}"))?
        .is_some();
    if present {
        store
            .view_drop(VIEW_TASK_BOARD)
            .await
            .map_err(|e| format!("view drop `{VIEW_TASK_BOARD}`: {e}"))?;
        tracing::info!(
            view = VIEW_TASK_BOARD,
            "task board disabled — stored rows deleted (disposable view)"
        );
    }
    Ok(())
}

async fn rebuild_under_lease(
    store: &EventStore,
    now_ms: u64,
    board_enabled: bool,
) -> CmdResult<RebuildOutcome> {
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
    // The fifth view (R17 C4, pin P3-3): the task board over the same
    // full streams just read — the derivation the honest path runs,
    // published at the pinned key. Off: any stored rows from an
    // earlier enabled era are deleted (pin P3-1).
    if board_enabled {
        publish_task_board(store, &streams, key, now_ms).await?;
    } else {
        drop_task_board_if_present(store).await?;
    }
    Ok(RebuildOutcome {
        sessions: sessions.len(),
        derived: sessions.len(),
        views: 4 + usize::from(board_enabled),
        board_refreshed: true,
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
    views_maintenance_tick_with(store, holder, now_ms, false).await;
}

/// [`views_maintenance_tick`] with the task-board switch carried in
/// (R17 C4): the serve-side `ViewsMaintenance` plan computes
/// `superagent.board.enabled && read_model.views.enabled` (the ticker
/// itself exists only while the machinery switch is on) and passes the
/// conjunction here.
pub async fn views_maintenance_tick_with(
    store: &EventStore,
    holder: &str,
    now_ms: u64,
    board_enabled: bool,
) {
    match rebuild_views_with(store, holder, now_ms, board_enabled).await {
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

/// The `task_board` rows when fresh (R17 C4) — the same three checks
/// (exact store key, digest over the read bytes, every payload
/// decodes); `None` on any doubt, and the caller falls back to the
/// honest derivation. Note the caller composes BOTH switches before
/// even consulting this (pin P3-1): a disabled board never reads a
/// view, however fresh.
pub async fn fresh_task_board(store: &EventStore) -> Option<Vec<TaskBoardEntry>> {
    fresh_entries(store, VIEW_TASK_BOARD).await
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
                        "fresh_task_board",
                        "VIEW_TICKETS",
                        "VIEW_USAGE",
                        "VIEW_CACHE",
                        "VIEW_DIRECTORY",
                        "VIEW_TASK_BOARD",
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
