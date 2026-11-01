//! `kian-store` — EventStore (sqlite v0 → postgres), Inbox, expectedSeq CAS,
//! epoch fencing, projection rebuild.
//!
//! DESIGN §7 assigns this crate the key interfaces `append(carrying epoch,
//! expected_seq)`, `scan`, `project`. It depends on [`kian_core`] only.
//!
//! Storage in Phase 1 is sqlite over a sqlx pool (WAL, `busy_timeout`);
//! sqlite access never blocks a tokio worker (AGENTS.md environment
//! gotcha). Correctness lives in the store, not in careful callers:
//! appends are single-statement CAS transactions that reject stale epochs
//! and sequence mismatches (ADR-005), and the end write lands as one
//! atomic append.
//!
//! Modules (skeletons, to be filled by cards C5–C6):
//! - [`event_store`] — append-only events table, CAS, fencing, migrations
//! - [`inbox`] — durable input rows, claims/leases, replay source, startup
//!   scan
//! - [`migrations`] — adjacent migration chain (DESIGN §3.3)

pub mod event_store;
pub mod inbox;
pub mod migrations;

pub use event_store::{AppendGuard, EventStore, Head, StoreError};
pub use inbox::{AdmitOutcome, InboxError, SessionInbox};
