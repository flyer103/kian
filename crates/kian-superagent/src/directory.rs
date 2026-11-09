//! The bundle-owned read-only port over the runtime worker directory
//! (R12, spec §5.1/S4 — the [`crate::recall::RecallPort`] /
//! [`crate::worker::WorkerStreamsPort`] precedent).
//!
//! Membership is liveness, not truth (I2): the port answers *read-only*
//! liveness/catalog questions — which worker ids are known, and each
//! member's observed state. It carries **no** binding fields: the live
//! bound session/thread for `list_workers` is composed by the tool
//! itself from [`crate::worker::WorkerStreamsPort`] (the existing
//! stream-derived exclusivity machinery), so this port stays a plain
//! liveness read.
//!
//! Assembly (`kian-cli`) implements the trait over `kian-exec`'s runtime
//! directory plus the durable registry read and the static config; this
//! crate names no store/engine symbol (`tests/iron_rule.rs` — zero
//! exemptions).
//!
//! Prompt-prefix neutrality (I6): nothing returned here may ever render
//! into a prompt or a tool schema. Membership data flows only through
//! `list_workers` tool **results** (post-prefix) and spawn validation
//! refusals (tool errors, also post-prefix).

use futures::future::BoxFuture;

/// Failures of a [`WorkerDirectoryPort`] read. The bundle's honest
/// "cannot decide" — a spawn that cannot consult the directory is
/// refused (fail-closed), never guessed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("worker directory port: {0}")]
pub struct WorkerDirectoryError(pub String);

/// A member's observed state — computed, never stored (spec M3), the
/// same rule every consumer (`list_workers`, the spawn echo,
/// `kian workers`) applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemberState {
    /// Live connection **and** fresh heartbeat.
    Online,
    /// Teardown window (bye/replace began, route not yet detached).
    Draining,
    /// Everything else — including known-but-absent workers.
    Offline,
}

impl MemberState {
    /// The wire spelling (`online`/`offline`/`draining`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            MemberState::Online => "online",
            MemberState::Draining => "draining",
            MemberState::Offline => "offline",
        }
    }
}

/// One member's liveness view: a disposable snapshot of the runtime
/// directory (plus, for ids known only from the durable registry, the
/// registry row's facts — post-restart honesty per spec M4/M5).
///
/// `description` and `version` are runtime facts: absent for an id that
/// has not (re-)registered since this boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    pub id: String,
    pub description: Option<String>,
    /// The worker's declared protocol revision — `None` when only the
    /// durable registry row knows the id (no migration, spec M4).
    pub version: Option<u32>,
    pub state: MemberState,
    /// The liveness clock the state was computed against (server
    /// clock, ms since epoch; 0 when unknown).
    pub last_seen_ms: u64,
}

/// The bundle-owned directory read port (spec §5.1): the spawn-side
/// unknown-id check, the spawn result's member-state echo and the
/// `list_workers` tool all read through this seam and nothing else.
///
/// Implementations must be read-only: no events, no rows, no wakes.
pub trait WorkerDirectoryPort: Send + Sync {
    /// Every known worker id, id-ascending: static config ids ∪ live
    /// directory ids ∪ durable registry row ids (the registry half is
    /// what keeps a known-but-offline worker bindable after a kian
    /// restart — rows are never pruned, spec M5).
    fn known_ids(&self) -> BoxFuture<'static, Result<Vec<String>, WorkerDirectoryError>>;

    /// Every known member's view, sorted by `id` ascending (byte
    /// order) — the `list_workers` result set and the spawn-side
    /// known-id/state snapshot (one read per spawn: no torn view).
    fn members(&self) -> BoxFuture<'static, Result<Vec<MemberView>, WorkerDirectoryError>>;

    /// One member's view, when the id is known.
    fn member(
        &self,
        id: &str,
    ) -> BoxFuture<'static, Result<Option<MemberView>, WorkerDirectoryError>>;
}
