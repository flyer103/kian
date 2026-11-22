//! `kian-store` — EventStore (sqlite v0 → postgres), Inbox, expectedSeq CAS,
//! epoch fencing, projection rebuild.
//!
//! DESIGN §7 assigns this crate the key interfaces `append(carrying epoch,
//! expected_seq)`, `scan`, `project`. It depends on [`kian_core`] only.
//!
//! Storage is dual-backend since: sqlite (WAL over a sqlx pool) and
//! postgres, dispatched by the [`event_store::EventStore`] enum with the
//! same method names and signatures on both. sqlite access never blocks
//! a tokio worker (AGENTS.md environment gotcha). Correctness lives in
//! the store, not in careful callers: appends are single-statement CAS
//! transactions that reject stale epochs and sequence mismatches
//! (ADR-005), and the end write lands as one atomic append.
//!
//! Modules:
//! - [`event_store`] — the dispatching EventStore, CAS, fencing
//! - [`sqlite`] / [`postgres`] — per-backend SQL (private; dialect lives
//!   here and only here)
//! - [`inbox`] — durable input rows, claims/leases, replay source, startup
//!   scan
//! - [`snapshots`] — read-model snapshot rows + maintenance leases
//!   (READ-MODEL §5/§9, migration v3): infrastructure state, never truth
//! - [`views`] — materialized-view rows + per-view freshness/corruption
//!   state (READ-MODEL §6, migration v4): disposable observability
//!   accelerators, never truth
//! - [`archive`] — export-only retention (READ-MODEL §7, migration v5):
//!   the archived marker, the terminal-only eligibility scan and the
//!   single-transaction marker-CAS + online-row delete behind a
//!   verified archive package
//! - [`artifact`] — the content-addressed tool-result blob tree (ADR-055:
//!   disposable payload cache, retention GC, never truth)
//! - [`wake`] — the postgres LISTEN/NOTIFY wake transport (advisory;
//!   /ADR-009)
//! - [`migrations`] — adjacent migration chains, one per backend (DESIGN
//!   §3.3)

pub mod archive;
pub mod artifact;
pub mod cron;
pub mod event_store;
pub mod inbox;
pub mod migrations;
pub mod snapshots;
pub mod views;
pub mod wake;

mod postgres;
mod sqlite;

pub use archive::{
    ArchiveCandidate, ArchiveCommitOutcome, ArchiveError, ArchivedInput, ArchivedMarker,
};
pub use artifact::{
    ArtifactError, ArtifactRef, ArtifactStats, ArtifactStore, ArtifactWindow, GcReport,
};
pub use cron::{CronError, CronFiringRow, CronScheduleRow, FiringState, ScheduleUpsert};
pub use event_store::{AppendGuard, EventStore, Head, StoreError};
pub use inbox::{AdmitOutcome, InboxError, SessionInbox};
pub use snapshots::{SnapshotCandidate, SnapshotRow};
pub use views::{StoreKey, ViewRow, ViewState, view_rows_digest};
