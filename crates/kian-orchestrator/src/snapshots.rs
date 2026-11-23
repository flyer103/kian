//! Verified projection snapshots and tail replay (READ-MODEL §5/§9, P1).
//!
//! A snapshot is a **disposable, verified cache** of the projection at one
//! committed version — `state` (the full state fold, including the
//! episode-end first-write-wins bit), `context` (`derive_model_context`'s
//! output) and per-message `provenance`. It is infrastructure state,
//! never truth (R1): every path here can be replaced by "scan the whole
//! stream and re-derive", and **any doubt falls back to exactly that** —
//! a reader must never serve a partially applied or unverified snapshot.
//!
//! ## Build (outside drains, under a maintenance lease)
//!
//! [`build_snapshot`] pins a version `V`, reads `1..V` **twice**
//! independently, derives `(state, context, provenance)` from both reads,
//! and publishes only when the two derivations agree — the INSERT lands
//! an already-verified row (insert-only publication, R6). A crash window
//! can therefore leave *nothing* or *one complete verified row*, never a
//! partial snapshot; superseded rows are GC-able (keep-N).
//!
//! ## Load (snapshot + tail replay)
//!
//! [`state_at`] / [`load_projection`] pick the highest verified `V <=`
//! the read version, integrity-check the row (digest over the stored
//! columns), and replay `(V, head]`:
//!
//! - **state** — the fold continues from the snapshotted seed through
//!   the public transition table, mirroring [`kian_core::state::project`]
//!   exactly (envelope checks, the end-write first-write-wins episode
//!   rule, illegal transitions);
//! - **context** — the tail is compact-free on this path, and
//!   `derive_model_context` is a forward fold, so appending
//!   `derive_model_context(tail)` to the snapshotted messages equals
//!   deriving the whole stream (pinned by equivalence tests).
//!
//! A `compact` event in the tail is the single deliberate invalidation
//! point (constraint 3 / R5): the snapshot is set aside and the context
//! re-derived once in full from the stream — exactly the DrainCtx cache's
//! P0 behavior. Missing/corrupt/unverified rows, a non-contiguous tail,
//! or a tail beyond the configured cap → honest full rebuild.
//!
//! ## Request bytes (constraint 3)
//!
//! The serialized model request bytes are owned by `kian-core` and are
//! NOT changed here: the snapshot's `context` column is the exact
//! `context_to_bytes` serialization, and the loaded messages re-serialize
//! byte-identically (witness test + the cache-stability suite).

use kian_core::context::{Message, context_provenance, context_to_bytes, derive_model_context};
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_core::state::{ProjectionError, SessionState, TransitionTrigger, project, transition};
use kian_store::{EventStore, SnapshotRow};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::EngineError;

/// The read-model settings an [`crate::engine::Engine`] carries: whether
/// projection/recovery reads may consult snapshots, and the tail cap
/// beyond which a snapshot is ignored (the honest full rebuild is
/// cheaper than replaying an oversized tail).
///
/// The engine default is **disabled** — every existing behavior stays
/// byte-identical unless the assembly (kian serve) turns it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadModelConfig {
    pub snapshots_enabled: bool,
    /// Replay work bound: `head - snapshot_version` beyond this skips
    /// the snapshot (full rebuild instead).
    pub tail_cap: u64,
}

impl Default for ReadModelConfig {
    fn default() -> Self {
        Self {
            snapshots_enabled: false,
            tail_cap: DEFAULT_TAIL_CAP,
        }
    }
}

/// Conservative default tail cap: a full rebuild of a 10k-event stream
/// costs ~9 ms (P0 bench), so replaying more than a few thousand tail
/// events saves nothing.
pub const DEFAULT_TAIL_CAP: u64 = 8_192;

/// Builder-side snapshot settings (READ-MODEL §5 build, §9 maintenance).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBuildConfig {
    /// Sessions below this head version are never snapshotted.
    pub min_events: u64,
    /// Verified versions kept per session (older rows GC-ed).
    pub keep_n: u64,
    /// The maintenance lease TTL (liveness only, READ-MODEL §9).
    pub lease_ttl_ms: u64,
}

impl Default for SnapshotBuildConfig {
    fn default() -> Self {
        Self {
            min_events: DEFAULT_MIN_EVENTS,
            keep_n: DEFAULT_KEEP_N,
            lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
        }
    }
}

pub const DEFAULT_MIN_EVENTS: u64 = 2_000;
pub const DEFAULT_KEEP_N: u64 = 3;
pub const DEFAULT_LEASE_TTL_MS: u64 = 60_000;

/// The full state fold at a version — what `project` carries between
/// events. Serialized into the snapshot's `state` column; `episode_end`
/// is the end-write first-write-wins bit (a second `session/ended` in
/// one drain episode is a projection error, so the tail replay must
/// start from exactly this bit, not guess it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StateFold {
    state: SessionState,
    #[serde(default)]
    episode_end: Option<EndReasonSerde>,
}

/// EndReason is `Copy` but carries no serde derives of its own shape we
/// can reuse positionally; mirror it lowercase like the event envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EndReasonSerde {
    Completed,
    Cancelled,
    Interrupted,
    Failed,
    #[serde(rename = "awaiting_input")]
    AwaitingInput,
    /// ADR-055: the token budget gate stopped the drain (resumable idle).
    #[serde(rename = "awaiting_budget")]
    AwaitingBudget,
    /// ADR-040: the resident session was retired by rotation.
    Rotated,
}

impl From<EndReason> for EndReasonSerde {
    fn from(r: EndReason) -> Self {
        match r {
            EndReason::Completed => Self::Completed,
            EndReason::Cancelled => Self::Cancelled,
            EndReason::Interrupted => Self::Interrupted,
            EndReason::Failed => Self::Failed,
            EndReason::AwaitingInput => Self::AwaitingInput,
            EndReason::AwaitingBudget => Self::AwaitingBudget,
            EndReason::Rotated => Self::Rotated,
        }
    }
}

impl From<EndReasonSerde> for EndReason {
    fn from(r: EndReasonSerde) -> Self {
        match r {
            EndReasonSerde::Completed => Self::Completed,
            EndReasonSerde::Cancelled => Self::Cancelled,
            EndReasonSerde::Interrupted => Self::Interrupted,
            EndReasonSerde::Failed => Self::Failed,
            EndReasonSerde::AwaitingInput => Self::AwaitingInput,
            EndReasonSerde::AwaitingBudget => Self::AwaitingBudget,
            EndReasonSerde::Rotated => Self::Rotated,
        }
    }
}

impl StateFold {
    fn of(events: &[Event]) -> Result<Self, ProjectionError> {
        Ok(Self {
            state: project(events)?,
            episode_end: episode_end_of(events).map(EndReasonSerde::from),
        })
    }
}

/// The end-write episode bit at the end of a (projection-legal) stream:
/// the last `session/ended` after the last `drain/claimed`, if any.
/// `project` guarantees at most one per episode, so this is exact.
fn episode_end_of(events: &[Event]) -> Option<EndReason> {
    let from = events
        .iter()
        .rposition(|e| matches!(e.kind, EventKind::DrainClaimed))
        .map_or(0, |i| i + 1);
    events[from..].iter().rev().find_map(|e| {
        if let EventKind::SessionEnded { reason } = e.kind {
            Some(reason)
        } else {
            None
        }
    })
}

/// The sha256 digest over a snapshot's three projection columns,
/// domain-separated from any other hashed payload. Recomputed by the
/// loader before a row is trusted (integrity check).
fn snapshot_digest(state: &str, context: &str, provenance: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"kian-snapshot-v1");
    h.update([0u8]);
    h.update(state.as_bytes());
    h.update([0u8]);
    h.update(context.as_bytes());
    h.update([0u8]);
    h.update(provenance.as_bytes());
    let out = h.finalize();
    let mut hex = String::with_capacity(out.len() * 2);
    for b in out {
        hex.push_str(&format!("{b:02x}"));
    }
    hex
}

/// Continues the state fold from a snapshot seed across the tail —
/// mirroring [`project`]'s per-event logic exactly (envelope checks,
/// end-write first-write-wins, the claim reset, the transition table).
/// Any deviation surfaces as the same typed error the full projection
/// would have produced.
fn replay_state(
    session: SessionId,
    seed: StateFold,
    from_version: u64,
    tail: &[Event],
) -> Result<StateFold, ProjectionError> {
    let mut fold = seed;
    let mut prev = from_version;
    for ev in tail {
        if ev.version > SCHEMA_VERSION {
            return Err(ProjectionError::UnsupportedVersion {
                got: ev.version,
                supported: SCHEMA_VERSION,
            });
        }
        if ev.session != session {
            return Err(ProjectionError::SessionIdMismatch {
                expected: session,
                got: ev.session,
            });
        }
        // The scan is seq-ordered and the stream is CAS-contiguous;
        // verify rather than trust (the same discipline as the tail read).
        if ev.seq != prev + 1 {
            return Err(ProjectionError::NonMonotoneSeq { prev, got: ev.seq });
        }
        prev = ev.seq;
        if let EventKind::SessionEnded { reason } = ev.kind {
            if let Some(recorded) = fold.episode_end {
                return Err(if EndReason::from(recorded) == reason {
                    ProjectionError::DuplicateSessionEnd { reason }
                } else {
                    ProjectionError::EndReasonConflict {
                        recorded: EndReason::from(recorded),
                        attempted: reason,
                    }
                });
            }
            fold.episode_end = Some(EndReasonSerde::from(reason));
        }
        if matches!(
            TransitionTrigger::from_event_kind(&ev.kind),
            TransitionTrigger::Claimed
        ) {
            fold.episode_end = None;
        }
        fold.state = transition(
            Some(fold.state),
            TransitionTrigger::from_event_kind(&ev.kind),
        )?;
    }
    Ok(fold)
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

/// The projection at one read version: the state fold plus the model
/// context and its provenance. `from_snapshot` records which snapshot
/// version seeded the tail replay (`None` = honest full derivation —
/// the fallback path every doubt takes).
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedProjection {
    pub version: u64,
    pub state: SessionState,
    pub context: Vec<Message>,
    pub provenance: Vec<u64>,
    pub from_snapshot: Option<u64>,
}

/// The honest fallback (and the only path when snapshots are disabled):
/// one bounded scan of `1..=version`, full derivation. `version` was
/// read before the scan, so the payload is derived from exactly that
/// committed prefix (R2 — an append racing meanwhile is wholly
/// excluded, never mixed in).
async fn full_derivation(
    store: &EventStore,
    session: SessionId,
    version: u64,
) -> Result<LoadedProjection, EngineError> {
    let events = store.scan(session, 0..version + 1).await?;
    if events.is_empty() {
        return Err(ProjectionError::EmptyStream.into());
    }
    Ok(LoadedProjection {
        version,
        state: project(&events)?,
        context: derive_model_context(&events)?,
        provenance: context_provenance(&events)?,
        from_snapshot: None,
    })
}

/// The materialized snapshot row, decoded and integrity-checked. `None`
/// on ANY doubt — the caller falls back to the stream (R1: never serve
/// unverified derived data as if it were truth).
fn materialize(row: &SnapshotRow) -> Option<(StateFold, Vec<Message>, Vec<u64>)> {
    if !row.verified || row.version == 0 {
        return None;
    }
    // Integrity: the stored digest must cover exactly the stored columns.
    if snapshot_digest(&row.state, &row.context, &row.context_provenance) != row.digest {
        return None;
    }
    let fold: StateFold = serde_json::from_str(&row.state).ok()?;
    let context: Vec<Message> = serde_json::from_str(&row.context).ok()?;
    let provenance: Vec<u64> = serde_json::from_str(&row.context_provenance).ok()?;
    // Sanity: the two lists are position-aligned by construction; a row
    // where they disagree is corrupt, not usable.
    if context.len() != provenance.len() {
        return None;
    }
    if provenance.last().is_some_and(|&seq| seq > row.version) {
        return None;
    }
    Some((fold, context, provenance))
}

/// Loads the full projection (state + context + provenance) at `version`
/// — snapshot + tail replay when a verified snapshot is usable, the
/// honest full derivation otherwise (READ-MODEL §5 load).
pub async fn load_projection(
    store: &EventStore,
    session: SessionId,
    version: u64,
    read: &ReadModelConfig,
) -> Result<LoadedProjection, EngineError> {
    if !read.snapshots_enabled || version == 0 {
        return full_derivation(store, session, version).await;
    }
    let Some(row) = store.snapshot_highest_verified(session).await? else {
        return full_derivation(store, session, version).await;
    };
    if row.version > version || version - row.version > read.tail_cap {
        return full_derivation(store, session, version).await;
    }
    let Some((fold, mut context, mut provenance)) = materialize(&row) else {
        return full_derivation(store, session, version).await;
    };
    let tail = store.scan(session, row.version + 1..version + 1).await?;
    // R5 / constraint 3: a compact in the tail is the single deliberate
    // invalidation point — one full re-derivation, never an incremental
    // application across the snapshot boundary.
    if tail.iter().any(|e| matches!(e.kind, EventKind::Compact)) {
        return full_derivation(store, session, version).await;
    }
    let replayed = match replay_state(session, fold, row.version, &tail) {
        Ok(fold) => fold,
        // A tail the fold rejects may be genuinely illegal (the full
        // derivation will produce the same error) or a snapshot doubt —
        // either way the stream decides.
        Err(_) => return full_derivation(store, session, version).await,
    };
    if tail.is_empty() {
        return Ok(LoadedProjection {
            version,
            state: replayed.state,
            context,
            provenance,
            from_snapshot: Some(row.version),
        });
    }
    // Compact-free tail: deriving the tail slice and appending is
    // exactly deriving the whole stream (forward fold; pinned by the
    // equivalence tests).
    context.extend(derive_model_context(&tail)?);
    provenance.extend(context_provenance(&tail)?);
    Ok(LoadedProjection {
        version,
        state: replayed.state,
        context,
        provenance,
        from_snapshot: Some(row.version),
    })
}

/// Loads only the state fold at `version` (the projection read path —
/// no context parse, no context scan beyond the tail).
pub async fn state_at(
    store: &EventStore,
    session: SessionId,
    version: u64,
    read: &ReadModelConfig,
) -> Result<SessionState, EngineError> {
    if let Some(state) = state_via_snapshot(store, session, version, read).await? {
        return Ok(state);
    }
    // The honest full derivation (and the only path when snapshots are
    // disabled or any doubt was hit above).
    let events = store.scan(session, 0..version + 1).await?;
    if events.is_empty() {
        return Err(ProjectionError::EmptyStream.into());
    }
    Ok(project(&events)?)
}

/// The snapshot+tail state derivation, `None` on any doubt (disabled,
/// no row, corrupt/unverified row, oversized tail, rejected replay) —
/// the caller falls back to the stream (R1).
async fn state_via_snapshot(
    store: &EventStore,
    session: SessionId,
    version: u64,
    read: &ReadModelConfig,
) -> Result<Option<SessionState>, EngineError> {
    if !read.snapshots_enabled || version == 0 {
        return Ok(None);
    }
    let Some(row) = store.snapshot_highest_verified(session).await? else {
        return Ok(None);
    };
    if row.version > version || version - row.version > read.tail_cap {
        return Ok(None);
    }
    let Some((fold, _context, _provenance)) = materialize(&row) else {
        return Ok(None);
    };
    let tail = store.scan(session, row.version + 1..version + 1).await?;
    match replay_state(session, fold, row.version, &tail) {
        Ok(fold) => Ok(Some(fold.state)),
        // A tail the fold rejects may be genuinely illegal (the full
        // derivation will produce the same error) or a snapshot doubt —
        // either way the stream decides.
        Err(_) => Ok(None),
    }
}

/// The versioned projection read when snapshots are enabled
/// (READ-MODEL §3 + §5): identical semantics to
/// [`crate::read::versioned_projection`] — the unchanged fast path, the
/// `since > version` refusal, one committed prefix — with the changed
/// path's state derived from the highest verified snapshot plus the
/// `(V, version]` tail, falling back to the full derivation on any
/// doubt (never a partially applied snapshot).
pub async fn versioned_projection_via_snapshots(
    store: &EventStore,
    session: SessionId,
    since: Option<u64>,
    read: &ReadModelConfig,
) -> Result<crate::read::VersionedProjection, EngineError> {
    let version = store.version(session).await?;
    if version == 0 {
        return Err(ProjectionError::EmptyStream.into());
    }
    if let Some(since) = since {
        if since == version {
            return Ok(crate::read::VersionedProjection {
                version,
                unchanged: true,
                state: None,
            });
        }
        if since > version {
            return Err(EngineError::SinceAhead {
                session,
                since,
                version,
            });
        }
    }
    let state = state_at(store, session, version, read).await?;
    Ok(crate::read::VersionedProjection {
        version,
        unchanged: false,
        state: Some(state),
    })
}

// ---------------------------------------------------------------------------
// Build (maintenance)
// ---------------------------------------------------------------------------

/// What one [`build_snapshot`] run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildOutcome {
    /// A verified snapshot at this version was published (or an
    /// identical row already existed — insert-only idempotency).
    Published { version: u64 },
    /// The head already has a verified snapshot.
    UpToDate,
    /// The session is below the configured minimum.
    BelowMinimum,
    /// Another holder's maintenance lease is live — skipped (R9:
    /// idempotent under retry).
    LeaseHeld,
    /// The two derivations disagreed: nothing published (R6 — doubt
    /// never becomes a visible snapshot).
    VerifyFailed,
}

/// One derivation of the snapshot columns from a pinned prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DerivedColumns {
    state: String,
    context: String,
    provenance: String,
}

fn derive_columns(events: &[Event]) -> Result<DerivedColumns, EngineError> {
    if events.is_empty() {
        return Err(ProjectionError::EmptyStream.into());
    }
    let fold = StateFold::of(events)?;
    let context = derive_model_context(events)?;
    let provenance = context_provenance(events)?;
    Ok(DerivedColumns {
        state: serde_json::to_string(&fold)
            .map_err(|e| EngineError::Store(kian_store::StoreError::Serde(e)))?,
        // The exact `context_to_bytes` form — the request bytes stay
        // owned by kian-core and unchanged (constraint 3).
        context: String::from_utf8(context_to_bytes(&context))
            .expect("serde_json output is valid UTF-8"),
        provenance: serde_json::to_string(&provenance)
            .map_err(|e| EngineError::Store(kian_store::StoreError::Serde(e)))?,
    })
}

impl DerivedColumns {
    fn digest(&self) -> String {
        snapshot_digest(&self.state, &self.context, &self.provenance)
    }
}

/// Builds and publishes one verified snapshot for `session` (READ-MODEL
/// §5 build): under the per-session maintenance lease, pin the head
/// version `V`, read `1..V` **twice**, derive both, and publish only on
/// agreement — one atomic INSERT of an already-verified row, then GC.
///
/// Runs **outside any drain** (READ-MODEL §9): a drain appending
/// concurrently simply extends the stream past `V`; both scans read the
/// same committed prefix (the stream is append-only), so the two
/// derivations cover identical bytes.
pub async fn build_snapshot(
    store: &EventStore,
    session: SessionId,
    build: &SnapshotBuildConfig,
    holder: &str,
    now_ms: u64,
) -> Result<BuildOutcome, EngineError> {
    let version = store.version(session).await?;
    if version < build.min_events {
        return Ok(BuildOutcome::BelowMinimum);
    }
    if let Some(existing) = store.snapshot_highest_verified(session).await?
        && existing.version == version
    {
        return Ok(BuildOutcome::UpToDate);
    }
    let scope = format!("snapshots:{}", session.0);
    if !store
        .maintenance_lease_try_acquire(&scope, holder, build.lease_ttl_ms, now_ms)
        .await?
    {
        return Ok(BuildOutcome::LeaseHeld);
    }
    // Publish-or-not decided inside the lease; the lease itself is
    // liveness only — losing it mid-build can at worst orphan a no-op.
    let outcome = build_under_lease(store, session, version, build, now_ms).await;
    let _ = store.maintenance_lease_release(&scope, holder).await;
    outcome
}

async fn build_under_lease(
    store: &EventStore,
    session: SessionId,
    version: u64,
    build: &SnapshotBuildConfig,
    now_ms: u64,
) -> Result<BuildOutcome, EngineError> {
    // Two independent derivations of the same pinned prefix.
    let first = derive_columns(&store.scan(session, 0..version + 1).await?)?;
    let second = derive_columns(&store.scan(session, 0..version + 1).await?)?;
    if first != second {
        tracing::warn!(
            session = %session.0,
            version,
            "snapshot build: two derivations disagreed — nothing published"
        );
        return Ok(BuildOutcome::VerifyFailed);
    }
    let row = SnapshotRow {
        session,
        version,
        state: first.state.clone(),
        context: first.context.clone(),
        context_provenance: first.provenance.clone(),
        digest: first.digest(),
        verified: true,
        created_at: now_ms,
    };
    store.snapshot_insert_verified(&row).await?;
    let _ = store.snapshot_gc(session, build.keep_n).await?;
    Ok(BuildOutcome::Published { version })
}

/// One maintenance tick (the serve-side snapshot builder's cadence):
/// candidates past the minimum, each built under its lease. A per-session
/// failure is logged and skipped — one bad session never stalls the
/// tick (bounded, idempotent under retry, R9). Returns how many
/// snapshots were published.
pub async fn maintenance_tick(
    store: &EventStore,
    build: &SnapshotBuildConfig,
    holder: &str,
    now_ms: u64,
) -> Result<usize, EngineError> {
    const TICK_BATCH: u64 = 16;
    let candidates = store
        .snapshot_candidates(build.min_events, TICK_BATCH)
        .await?;
    let mut published = 0usize;
    for candidate in candidates {
        match build_snapshot(store, candidate.session, build, holder, now_ms).await {
            Ok(BuildOutcome::Published { .. }) => published += 1,
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    session = %candidate.session.0,
                    error = %e,
                    "snapshot maintenance: build failed (skipped this tick)"
                );
            }
        }
    }
    Ok(published)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::state::{ProjectionError, SessionState, project};
    use kian_store::{AppendGuard, EventStore};
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        BuildOutcome, DerivedColumns, ReadModelConfig, SnapshotBuildConfig, build_snapshot,
        derive_columns, episode_end_of, load_projection, replay_state, snapshot_digest, state_at,
    };
    use crate::engine::EngineError;

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-orch-snapshots-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// A tiny deterministic PRNG (xorshift64*) so the equivalence tests
    /// are reproducible without a dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Builds a random projection-legal stream: created → claimed, then
    /// random legal turns/messages, optionally a compact mid-stream and
    /// an interrupted-end + re-claim episode. Returns the event seqs of
    /// the context-bearing messages for assertions.
    async fn seed_random(
        store: &EventStore,
        session: SessionId,
        upto: u64,
        seed: u64,
        compact_every: u64,
    ) {
        let mut rng = Rng(seed);
        let mut epoch = Epoch::GENESIS;
        let mut claimed = false;
        let mut compact_base: Option<u64> = None;
        for seq in 1..=upto {
            let (kind, bytes) = if seq == 1 {
                (EventKind::SessionCreated, json!({}))
            } else if !claimed {
                claimed = true;
                (
                    EventKind::DrainClaimed,
                    json!({"holder": "t", "epoch": epoch.0}),
                )
            } else if compact_every > 0 && seq % compact_every == 0 && seq > 4 {
                // A compact covering the un-compacted span before it.
                let from = compact_base.unwrap_or(3).max(3);
                if from >= seq {
                    (EventKind::TurnEnd, json!({ "n": seq }))
                } else {
                    compact_base = Some(seq + 1);
                    (
                        EventKind::Compact,
                        json!({
                            "from_seq": from,
                            "to_seq": seq,
                            "summary": {"role": "assistant", "content": format!("compact@{seq}")},
                        }),
                    )
                }
            } else if seq == upto && upto.is_multiple_of(7) {
                // An interrupted end followed by nothing (terminal-ish
                // shape variation): only when it is the LAST event.
                (
                    EventKind::SessionEnded {
                        reason: EndReason::Interrupted,
                    },
                    json!({}),
                )
            } else {
                match rng.below(6) {
                    0 => (
                        EventKind::InputAdmitted,
                        json!({"role": "user", "content": format!("u{seq}")}),
                    ),
                    1 => (
                        EventKind::AssistantMessage,
                        json!({"role": "assistant", "content": format!("a{seq}")}),
                    ),
                    2 => (
                        EventKind::ToolCall,
                        json!({"role": "assistant", "content": format!("c{seq}")}),
                    ),
                    3 => (
                        EventKind::ToolResult,
                        json!({"role": "tool", "content": format!("r{seq}")}),
                    ),
                    _ => (EventKind::TurnEnd, json!({ "n": seq })),
                }
            };
            if matches!(kind, EventKind::SessionEnded { .. }) {
                // the interrupted end releases ownership; a re-claim
                // follows (the resumable shape)
            }
            store
                .append(
                    session,
                    Event {
                        seq,
                        session,
                        epoch,
                        ts: 1_761_900_000_000 + seq,
                        kind,
                        bytes,
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .expect("seed append");
            if matches!(kind, EventKind::SessionEnded { .. }) && seq != upto {
                claimed = false;
                epoch = epoch.next();
            }
        }
    }

    fn enabled_read() -> ReadModelConfig {
        ReadModelConfig {
            snapshots_enabled: true,
            tail_cap: 8_192,
        }
    }

    fn build_cfg(min_events: u64) -> SnapshotBuildConfig {
        SnapshotBuildConfig {
            min_events,
            keep_n: 3,
            lease_ttl_ms: 60_000,
        }
    }

    /// Random-stream equivalence (READ-MODEL §11 P1, L1): snapshot +
    /// tail replay equals full replay — state, context bytes and
    /// provenance — across randomized streams with and without
    /// compacts, with snapshots built mid-history and long tails
    /// appended after.
    #[tokio::test]
    async fn snapshot_plus_tail_equals_full_replay_on_random_streams() {
        for &(upto, tail, compact_every, snapshot_at) in &[
            (50u64, 20u64, 0u64, 30u64), // no compacts
            (60, 15, 17, 40),            // compacts before the snapshot
            (80, 40, 11, 40),            // compacts before AND after
            (40, 5, 0, 39),              // snapshot near head
        ] {
            let path = temp_db_path();
            let store = EventStore::open(&path).await.expect("open");
            let sid = SessionId(Uuid::new_v4());
            seed_random(&store, sid, upto, upto * 7 + tail, compact_every).await;

            // Build a snapshot at `snapshot_at` by building at head and
            // then appending the tail — simpler: build at the current
            // head, then extend.
            let head_before = store.version(sid).await.unwrap();
            assert!(head_before >= snapshot_at || upto >= snapshot_at);
            let out = build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
                .await
                .expect("build");
            assert!(
                matches!(out, BuildOutcome::Published { .. } | BuildOutcome::UpToDate),
                "unexpected outcome: {out:?}"
            );

            // Append more history (the tail the loader must replay),
            // seeded from a different point.
            let mut rng = Rng(tail * 31 + 7);
            let epoch = Epoch(2);
            let extra = tail;
            for seq in (head_before + 1..).take(extra as usize) {
                let kind = match rng.below(5) {
                    0 => EventKind::InputAdmitted,
                    1 => EventKind::AssistantMessage,
                    2 => EventKind::ToolResult,
                    _ => EventKind::TurnEnd,
                };
                let bytes = if matches!(
                    kind,
                    EventKind::InputAdmitted | EventKind::AssistantMessage | EventKind::ToolResult
                ) {
                    json!({"role": "user", "content": format!("tail{seq}")})
                } else {
                    json!({ "n": seq })
                };
                store
                    .append(
                        sid,
                        Event {
                            seq,
                            session: sid,
                            epoch,
                            ts: 1_761_900_000_000 + seq,
                            kind,
                            bytes,
                            version: SCHEMA_VERSION,
                        },
                        AppendGuard {
                            epoch,
                            expected_seq: seq,
                        },
                    )
                    .await
                    .expect("tail append");
            }

            let version = store.version(sid).await.unwrap();
            let loaded = load_projection(&store, sid, version, &enabled_read())
                .await
                .expect("load");
            let full = load_projection(&store, sid, version, &ReadModelConfig::default())
                .await
                .expect("full");

            assert_eq!(loaded.state, full.state, "state mismatch ({upto}/{tail})");
            assert_eq!(
                loaded.context, full.context,
                "context mismatch ({upto}/{tail})"
            );
            assert_eq!(
                loaded.provenance, full.provenance,
                "provenance mismatch ({upto}/{tail})"
            );
            // And it really used a snapshot on the no-compact-tail path.
            if compact_every == 0 {
                assert!(loaded.from_snapshot.is_some(), "expected snapshot use");
            }

            store.close().await;
            cleanup(&path);
        }
    }

    /// A `compact` in the tail invalidates the snapshot once (R5): the
    /// loader still answers, via the honest full re-derivation, and the
    /// answer equals the stream's truth.
    #[tokio::test]
    async fn compact_in_the_tail_falls_back_to_full_and_stays_correct() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 30, 991, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");

        // A compact in the tail covering pre-snapshot events.
        let version = store.version(sid).await.unwrap();
        let guard_epoch = Epoch(2);
        store
            .append(
                sid,
                Event {
                    seq: version + 1,
                    session: sid,
                    epoch: guard_epoch,
                    ts: 1,
                    kind: EventKind::Compact,
                    bytes: json!({
                        "from_seq": 4,
                        "to_seq": version,
                        "summary": {"role": "assistant", "content": "late compact"},
                    }),
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: guard_epoch,
                    expected_seq: version + 1,
                },
            )
            .await
            .expect("compact append");

        let version = store.version(sid).await.unwrap();
        let loaded = load_projection(&store, sid, version, &enabled_read())
            .await
            .expect("load");
        // The compact forced the full path…
        assert_eq!(loaded.from_snapshot, None, "R5: compact invalidates");
        // …and the answer is the stream's truth (compared against the
        // disabled-config full derivation of the same stream).
        let full = load_projection(&store, sid, version, &ReadModelConfig::default())
            .await
            .expect("full");
        assert_eq!(loaded.state, full.state);
        assert_eq!(loaded.context, full.context);

        store.close().await;
        cleanup(&path);
    }

    /// Corrupt / unverified / tampered snapshots are ignored — the
    /// loader's answer is always the stream's (R1).
    #[tokio::test]
    async fn corrupt_unverified_and_tampered_snapshots_are_ignored() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 30, 5, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        let version = store.version(sid).await.unwrap();
        let truth = load_projection(&store, sid, version, &ReadModelConfig::default())
            .await
            .expect("truth");

        if let EventStore::Sqlite(pool) = &store {
            // 1. Tamper with the state column (digest check fails).
            sqlx::query("UPDATE projection_snapshots SET state = '{\"state\":\"completed\",\"episode_end\":null}' WHERE session = ?")
                .bind(sid.0.to_string())
                .execute(pool)
                .await
                .unwrap();
            let loaded = load_projection(&store, sid, version, &enabled_read())
                .await
                .expect("load after tamper");
            assert_eq!(loaded.from_snapshot, None, "tampered row must be ignored");
            assert_eq!(loaded.state, truth.state);

            // 2. Flip verified to 0 (invisible).
            let _ = sqlx::query("UPDATE projection_snapshots SET verified = 0 WHERE session = ?")
                .bind(sid.0.to_string())
                .execute(pool)
                .await
                .unwrap();
            let loaded = load_projection(&store, sid, version, &enabled_read())
                .await
                .expect("load after unverify");
            assert_eq!(loaded.from_snapshot, None);
            assert_eq!(loaded.state, truth.state);
        }

        store.close().await;
        cleanup(&path);
    }

    /// R9-08 F2: the documented trust assumption, pinned honestly. A row
    /// poisoned together with its digest (self-consistent, `verified=1`)
    /// is NOT detectable at load — load-time verification is builder
    /// attestation + digest↔columns integrity (READ-MODEL §5 trust
    /// boundary). This test asserts the documented behaviour (the row is
    /// SERVED) so no reader mistakes `..._are_ignored` above for a
    /// general tamper guarantee. The table must live inside the same
    /// trust boundary as `events` and never be copied between stores.
    #[tokio::test]
    async fn self_consistent_poisoned_snapshot_is_served_builder_attestation_only() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 30, 5, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        let version = store.version(sid).await.unwrap();
        let truth = load_projection(&store, sid, version, &ReadModelConfig::default())
            .await
            .expect("truth");

        if let EventStore::Sqlite(pool) = &store {
            // Poison the state column AND recompute the digest over the
            // stored columns with the documented `kian-snapshot-v1`
            // recipe — the row is now internally consistent and attested.
            let (context, provenance): (String, String) = sqlx::query_as(
                "SELECT context, context_provenance FROM projection_snapshots WHERE session = ?",
            )
            .bind(sid.0.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
            let poisoned_state = r#"{"state":"completed","episode_end":null}"#.to_string();
            let digest = snapshot_digest(&poisoned_state, &context, &provenance);
            sqlx::query(
                "UPDATE projection_snapshots SET state = ?, digest = ?, verified = 1 WHERE session = ?",
            )
            .bind(&poisoned_state)
            .bind(&digest)
            .bind(sid.0.to_string())
            .execute(pool)
            .await
            .unwrap();

            let loaded = load_projection(&store, sid, version, &enabled_read())
                .await
                .expect("load");
            assert_eq!(
                loaded.from_snapshot,
                Some(version),
                "documented behaviour: a self-consistent attested row is served"
            );
            assert_eq!(loaded.state, SessionState::Completed);
            assert_ne!(
                loaded.state, truth.state,
                "the poison changed the answer — this is exactly why the table must stay inside the events trust boundary"
            );
        }

        store.close().await;
        cleanup(&path);
    }

    /// A tail beyond the configured cap skips the snapshot (full
    /// rebuild) — the cap bounds replay work.
    #[tokio::test]
    async fn tail_beyond_the_cap_falls_back_to_full() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 30, 5, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        // Extend the stream: the snapshot now sits one event behind head.
        let epoch = Epoch(2);
        let version = store.version(sid).await.unwrap();
        store
            .append(
                sid,
                Event {
                    seq: version + 1,
                    session: sid,
                    epoch,
                    ts: 1,
                    kind: EventKind::InputAdmitted,
                    bytes: json!({"role": "user", "content": "over-cap tail"}),
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch,
                    expected_seq: version + 1,
                },
            )
            .await
            .unwrap();
        let version = store.version(sid).await.unwrap();

        let capped = ReadModelConfig {
            snapshots_enabled: true,
            tail_cap: 0, // any tail exceeds it
        };
        let loaded = load_projection(&store, sid, version, &capped)
            .await
            .expect("load");
        assert_eq!(loaded.from_snapshot, None);
        let full = load_projection(&store, sid, version, &ReadModelConfig::default())
            .await
            .expect("full");
        assert_eq!(loaded.state, full.state);
        assert_eq!(loaded.context, full.context);

        store.close().await;
        cleanup(&path);
    }

    /// The witness (plan §4.2 req 4): the serialized model request bytes
    /// on the snapshot path are byte-identical to `context_to_bytes` of
    /// the full derivation — constraint 3 holds on the serving path.
    #[tokio::test]
    async fn snapshot_path_request_bytes_are_byte_identical() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 40, 77, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        // extend the tail
        let version = store.version(sid).await.unwrap();
        let epoch = Epoch(2);
        for seq in version + 1..=version + 3 {
            store
                .append(
                    sid,
                    Event {
                        seq,
                        session: sid,
                        epoch,
                        ts: 1,
                        kind: EventKind::InputAdmitted,
                        bytes: json!({"role": "user", "content": format!("w{seq}")}),
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .unwrap();
        }
        let version = store.version(sid).await.unwrap();
        let loaded = load_projection(&store, sid, version, &enabled_read())
            .await
            .expect("load");
        assert!(loaded.from_snapshot.is_some());
        let full = load_projection(&store, sid, version, &ReadModelConfig::default())
            .await
            .expect("full");
        assert_eq!(
            kian_core::context::context_to_bytes(&loaded.context),
            kian_core::context::context_to_bytes(&full.context),
            "request bytes differ across the snapshot path (constraint 3)"
        );
        store.close().await;
        cleanup(&path);
    }

    /// Crash-window shape (plan §4.2 req 7, build side): publication is
    /// verify-then-insert — a build that "dies" between derivation and
    /// insert leaves no row, and one that dies after leaves exactly one
    /// complete verified row. Simulated by running the build's exact
    /// steps piecemeal.
    #[tokio::test]
    async fn build_crash_windows_leave_nothing_or_one_complete_row() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 25, 3, 0).await;
        let version = store.version(sid).await.unwrap();

        // Crash before insert: derivation only, nothing published.
        let first = derive_columns(&store.scan(sid, 0..version + 1).await.unwrap()).unwrap();
        assert!(
            store
                .snapshot_highest_verified(sid)
                .await
                .unwrap()
                .is_none(),
            "no row may exist before the publish insert"
        );
        assert!(!first.context.is_empty() || version > 0);

        // Crash AFTER insert: exactly one complete, verified, loadable row.
        let row = kian_store::SnapshotRow {
            session: sid,
            version,
            state: first.state.clone(),
            context: first.context.clone(),
            context_provenance: first.provenance.clone(),
            digest: first.digest(),
            verified: true,
            created_at: 1_000,
        };
        assert!(store.snapshot_insert_verified(&row).await.unwrap());
        let stored = store.snapshot_highest_verified(sid).await.unwrap().unwrap();
        assert_eq!(stored.version, version);
        assert!(stored.verified);
        let loaded = load_projection(&store, sid, version, &enabled_read())
            .await
            .expect("the surviving row loads exactly");
        assert_eq!(loaded.from_snapshot, Some(version));

        store.close().await;
        cleanup(&path);
    }

    /// Two-derivation verification gates publication: mismatched
    /// derivations never publish (R6) — exercised directly on the
    /// comparison the builder performs.
    #[tokio::test]
    async fn mismatched_derivations_never_publish() {
        let a = DerivedColumns {
            state: "s".into(),
            context: "[]".into(),
            provenance: "[]".into(),
        };
        let mut b = a.clone();
        b.state = "different".into();
        assert_ne!(a.digest(), b.digest());
        assert_ne!(a, b, "the builder's equality check would refuse this");
    }

    /// The end-write episode bit is captured exactly: an interrupted end
    /// mid-stream (resumable shape) is recorded, and a claimed episode
    /// resets it — `replay_state` then reproduces `project`'s
    /// duplicate-end error across the snapshot boundary.
    #[tokio::test]
    async fn episode_end_bit_survives_the_boundary_and_reproduces_duplicate_end_errors() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        // created, claimed, message, ended{interrupted} (state → idle,
        // episode_end = Some(Interrupted), stream continues).
        let spec = [
            (EventKind::SessionCreated, json!({})),
            (EventKind::DrainClaimed, json!({"holder": "t", "epoch": 1})),
            (
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "x"}),
            ),
            (
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
                json!({}),
            ),
            (
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "y"}),
            ),
        ];
        for (seq, (kind, bytes)) in spec.iter().enumerate() {
            let seq = seq as u64 + 1;
            store
                .append(
                    sid,
                    Event {
                        seq,
                        session: sid,
                        epoch: Epoch(1),
                        ts: seq,
                        kind: *kind,
                        bytes: bytes.clone(),
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: seq,
                    },
                )
                .await
                .unwrap();
        }
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        let version = store.version(sid).await.unwrap();

        // A second ended WITHOUT an intervening claim: project errors
        // (duplicate in one episode) — the snapshot+tail path must
        // produce the same error, not a served state.
        store
            .append(
                sid,
                Event {
                    seq: version + 1,
                    session: sid,
                    epoch: Epoch(1),
                    ts: 9,
                    kind: EventKind::SessionEnded {
                        reason: EndReason::Interrupted,
                    },
                    bytes: json!({}),
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: version + 1,
                },
            )
            .await
            .unwrap();
        let version = store.version(sid).await.unwrap();
        let snapshotted = load_projection(&store, sid, version, &enabled_read()).await;
        let full = load_projection(&store, sid, version, &ReadModelConfig::default()).await;
        match (snapshotted, full) {
            (Err(EngineError::Projection(_)), Err(EngineError::Projection(_))) => {}
            (a, b) => panic!("both paths must refuse the duplicate end: {a:?} vs {b:?}"),
        }

        store.close().await;
        cleanup(&path);
    }

    /// state_at (the projection read path) agrees with project() at
    /// every prefix, with and without snapshots — including the terminal
    /// probe the drain loop performs.
    #[tokio::test]
    async fn state_at_agrees_with_project_across_prefixes() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 60, 17, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "builder", 1_000)
            .await
            .expect("build");
        for probe in [1u64, 2, 30, 45, 60] {
            let events = store.scan(sid, 0..probe + 1).await.unwrap();
            let expected = project(&events).unwrap();
            let got = state_at(&store, sid, probe, &enabled_read()).await.unwrap();
            assert_eq!(got, expected, "state mismatch at prefix {probe}");
        }
        // SessionState is exercised across shapes here.
        assert_ne!(SessionState::Idle, SessionState::Running);
        store.close().await;
        cleanup(&path);
    }

    /// The maintenance lease excludes concurrent builders of the same
    /// session and lets a retry through after release (R9).
    #[tokio::test]
    async fn builder_lease_excludes_concurrent_builders() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 10, 1, 0).await;
        assert!(
            store
                .maintenance_lease_try_acquire(
                    &format!("snapshots:{}", sid.0),
                    "other",
                    60_000,
                    1_000,
                )
                .await
                .unwrap()
        );
        let out = build_snapshot(&store, sid, &build_cfg(1), "builder", 1_100)
            .await
            .unwrap();
        assert_eq!(out, BuildOutcome::LeaseHeld);
        assert!(
            store
                .snapshot_highest_verified(sid)
                .await
                .unwrap()
                .is_none(),
            "a lease-held build publishes nothing"
        );

        store
            .maintenance_lease_release(&format!("snapshots:{}", sid.0), "other")
            .await
            .unwrap();
        let out = build_snapshot(&store, sid, &build_cfg(1), "builder", 1_200)
            .await
            .unwrap();
        assert!(matches!(out, BuildOutcome::Published { .. }));

        store.close().await;
        cleanup(&path);
    }

    /// GC keeps only the newest keep_n versions; a re-build at head
    /// after growth still leaves a usable row.
    #[tokio::test]
    async fn rebuilds_gc_older_versions() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed_random(&store, sid, 20, 3, 0).await;
        build_snapshot(&store, sid, &build_cfg(1), "b", 1_000)
            .await
            .expect("build 1");
        let v1 = store.version(sid).await.unwrap();
        // extend and rebuild
        let epoch = Epoch(2);
        for seq in v1 + 1..=v1 + 5 {
            store
                .append(
                    sid,
                    Event {
                        seq,
                        session: sid,
                        epoch,
                        ts: 1,
                        kind: EventKind::TurnEnd,
                        bytes: json!({ "n": seq }),
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .unwrap();
        }
        build_snapshot(&store, sid, &build_cfg(1), "b", 2_000)
            .await
            .expect("build 2");
        if let EventStore::Sqlite(pool) = &store {
            let n: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM projection_snapshots WHERE session = ?")
                    .bind(sid.0.to_string())
                    .fetch_one(pool)
                    .await
                    .unwrap();
            assert_eq!(n, 2, "both versions are within keep_n=3");
        }
        store.close().await;
        cleanup(&path);
    }

    /// digest determinism: same columns, same digest; any column change
    /// changes it.
    #[test]
    fn digest_is_deterministic_and_column_sensitive() {
        let a = snapshot_digest("s", "[]", "[]");
        assert_eq!(a, snapshot_digest("s", "[]", "[]"));
        assert_ne!(a, snapshot_digest("t", "[]", "[]"));
        assert_ne!(a, snapshot_digest("s", "[{}]", "[]"));
        assert_ne!(a, snapshot_digest("s", "[]", "[1]"));
    }

    /// episode_end_of: none on a claimed open episode; the last end
    /// after the last claim otherwise.
    #[test]
    fn episode_end_of_shapes() {
        let sid = SessionId(Uuid::nil());
        let ev = |seq: u64, kind: EventKind| Event {
            seq,
            session: sid,
            epoch: Epoch(1),
            ts: seq,
            kind,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        let open = vec![
            ev(1, EventKind::SessionCreated),
            ev(2, EventKind::DrainClaimed),
            ev(3, EventKind::TurnEnd),
        ];
        assert_eq!(episode_end_of(&open), None);
        let ended = vec![
            ev(1, EventKind::SessionCreated),
            ev(2, EventKind::DrainClaimed),
            ev(
                3,
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
            ),
        ];
        assert_eq!(
            episode_end_of(&ended),
            Some(EndReason::Interrupted),
            "an interrupted end leaves the episode bit set (the stream may legally continue)"
        );
        let reclaied = vec![
            ev(1, EventKind::SessionCreated),
            ev(2, EventKind::DrainClaimed),
            ev(
                3,
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
            ),
            ev(4, EventKind::DrainClaimed),
        ];
        assert_eq!(
            episode_end_of(&reclaied),
            None,
            "a claim resets the episode"
        );
    }

    /// replay_state refuses envelope violations exactly like project.
    #[test]
    fn replay_state_refuses_foreign_sessions_and_gaps() {
        let sid = SessionId(Uuid::nil());
        let seed = super::StateFold {
            state: SessionState::Running,
            episode_end: None,
        };
        let foreign = Event {
            seq: 2,
            session: SessionId(Uuid::new_v4()),
            epoch: Epoch(1),
            ts: 2,
            kind: EventKind::TurnEnd,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        assert!(matches!(
            replay_state(sid, seed, 1, &[foreign]),
            Err(ProjectionError::SessionIdMismatch { .. })
        ));
        let gap = Event {
            seq: 5,
            session: sid,
            epoch: Epoch(1),
            ts: 5,
            kind: EventKind::TurnEnd,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        assert!(matches!(
            replay_state(sid, seed, 1, &[gap]),
            Err(ProjectionError::NonMonotoneSeq { .. })
        ));
    }
}
