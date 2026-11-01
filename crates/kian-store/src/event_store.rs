//! Append-only events table (card C5) and its dual-backend dispatch (C50).
//!
//! `append(session, event, { epoch, expected_seq })` as a single atomic
//! CAS write rejecting stale epochs and seq mismatches (ADR-005);
//! `scan(session, range)` reads events back. No statement that could
//! rewrite or remove a committed event row may ever exist here — the
//! stream is the sole truth (constraint 1), enforced by a crate-level
//! review assertion test below.
//!
//! The end write (`append_end`) lands `session/ended` as exactly one
//! atomic append: the terminal state transition is *projected* from that
//! single row, never written separately (NoHalfTerminated, constraint 4).
//!
//! C50: the public [`EventStore`] is a small dispatching enum —
//! `Sqlite(..) | Postgres(..)` — exposing the same method names and
//! signatures on both backends (sqlx cannot usefully genericize over the
//! two dialects; dispatch, not generics, is the plan of record). The
//! per-backend SQL lives in [`crate::sqlite`] and [`crate::postgres`];
//! guard/event coherence and envelope serialization are shared here so
//! both backends see byte-identical inputs. `open(path)` stays sqlite
//! construction; `open_postgres(url)` is the pg entry point.
//!
//! Schema versioning: the table carries the envelope `version` column and
//! is created by the adjacent migration chains in [`crate::migrations`]
//! (DESIGN §3.3) — one chain per backend, same versions and names.

use std::ops::Range;
use std::path::Path;
use std::time::Duration;

use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};

use crate::migrations;

/// CAS fence inputs (ADR-005): the drain's fencing epoch and the seq the
/// caller asserts the new event takes. The store — not the caller's
/// carefulness — decides whether the write may land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendGuard {
    pub epoch: Epoch,
    pub expected_seq: u64,
}

/// Store-observed stream position: the next free seq and the epoch floor
/// (the highest epoch any committed event of the session carries).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub next_seq: u64,
    pub epoch: Epoch,
}

/// Store failures as typed, matchable facts — a rejected CAS is not an
/// opaque error string (ADR-005).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The write's epoch is behind the session's epoch floor: a zombie
    /// drain from before a newer claim (NoSplitBrainWrite).
    #[error(
        "stale epoch {event_epoch:?} writing session {session:?}: epoch floor is {current_epoch:?}"
    )]
    StaleEpoch {
        session: SessionId,
        event_epoch: Epoch,
        current_epoch: Epoch,
    },
    /// The write's expected_seq does not match the stream head.
    #[error(
        "seq mismatch writing session {session:?}: expected head {expected_seq}, actual next seq is {actual_next}"
    )]
    SeqMismatch {
        session: SessionId,
        expected_seq: u64,
        actual_next: u64,
    },
    /// The guard disagrees with the event it accompanies — a programming
    /// error in the caller, rejected before touching sqlite.
    #[error("append guard mismatch: {0}")]
    GuardMismatch(&'static str),
    /// `append_end` accepts only `session/ended` (constraint 4).
    #[error("end write must be session/ended, got {kind}")]
    NotAnEndWrite { kind: &'static str },
    /// Envelope version beyond the head of the migration chain.
    #[error("unsupported envelope version {found}, chain head is {current}")]
    UnsupportedVersion { found: u32, current: u32 },
    /// A stored row disagrees with its envelope — the stream is corrupt.
    #[error("corrupt event row (session {session:?}, seq {seq})")]
    CorruptRow { session: SessionId, seq: u64 },
    #[error("u64 field {field} overflows sqlite's i64")]
    IntOverflow { field: &'static str },
    #[error("sqlite error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("envelope serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Classifies a refused CAS write from the committed head: which fence
/// tripped. Shared by both backends — on sqlite it explains zero rows
/// from the WHERE fence; on postgres it additionally explains the
/// PK-race outcomes (`ON CONFLICT DO NOTHING` zero rows / sqlstate
/// 23505) that sqlite's writer serialization makes impossible (C50 §3).
pub(crate) fn classify_cas_rejection(
    session: SessionId,
    guard: AppendGuard,
    epoch_floor: i64,
    max_seq: i64,
) -> StoreError {
    if guard.epoch.0 < epoch_floor as u64 {
        StoreError::StaleEpoch {
            session,
            event_epoch: guard.epoch,
            current_epoch: Epoch(epoch_floor as u64),
        }
    } else {
        StoreError::SeqMismatch {
            session,
            expected_seq: guard.expected_seq,
            actual_next: max_seq as u64 + 1,
        }
    }
}

pub(crate) fn to_i64(v: u64, field: &'static str) -> Result<i64, StoreError> {
    i64::try_from(v).map_err(|_| StoreError::IntOverflow { field })
}

/// The dual-backend EventStore (C50): a small dispatching enum over the
/// sqlite and postgres implementations. Every method carries the same
/// name and signature the pre-C50 sqlite store had, so callers and tests
/// are unchanged; construction picks the backend (`open` = sqlite,
/// `open_postgres` = postgres).
///
/// All access goes through the sqlx pool — both drivers own their
/// background connection handling, so no tokio worker ever blocks on a
/// query (AGENTS environment gotcha).
#[derive(Clone)]
pub enum EventStore {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

impl EventStore {
    /// Opens (creating if needed) a sqlite database file, applies the
    /// sqlite migration chain, and returns a pooled store. This stays
    /// *the* `open` — sqlite remains the default backend.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            // Generous timeout: WAL serializes writers and concurrent
            // drains queue up — never tighten locking to force
            // interleavings (round-1 plan, risk #7).
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;
        migrations::migrate(&pool).await?;
        Ok(Self::Sqlite(pool))
    }

    /// Opens a postgres database by URL, applies the pg migration chain
    /// (same versions and names as the sqlite chain, pg DDL — DESIGN
    /// §3.3), and returns a pooled store. The URL is env-sourced by the
    /// caller (C51 assembly); it may carry a password, but never lands in
    /// a config value (the C13 secrets rule).
    pub async fn open_postgres(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        migrations::migrate_pg(&pool).await?;
        Ok(Self::Postgres(pool))
    }

    /// Adopts an already-open postgres pool and applies the pg migration
    /// chain — the counterpart of [`EventStore::from_pool`]-style
    /// adoption, used by the C51 assembly so the event store and the
    /// exec store share ONE pool (one server, one database).
    pub async fn from_pg_pool(pool: PgPool) -> Result<Self, StoreError> {
        migrations::migrate_pg(&pool).await?;
        Ok(Self::Postgres(pool))
    }

    /// Opens an existing sqlite database file **read-only** (card C14: the
    /// audit reader behind `kian session inspect/replay/export` and
    /// `kian lease inspect`).
    ///
    /// No migrations run — a read-only connection cannot (and must not)
    /// write schema. The file must already exist; a missing file is a
    /// typed error, never an empty store silently created. Readers ride
    /// the same WAL (a live writer's `-wal`/`-shm` files serve
    /// consistent snapshots), so embedded reads against a running
    /// `serve` are safe.
    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await?;
        Ok(Self::Sqlite(pool))
    }

    /// Opens a postgres database by URL **without migrating** — the pg
    /// counterpart of [`EventStore::open_read_only`]. Read-only-ness is a
    /// property of the role behind the URL, not a client flag, on
    /// postgres: point this at a role without schema privileges and the
    /// store can read what it is granted and refuses every mutation
    /// (including migrations — see the C50 pg suite).
    pub async fn open_postgres_read_only(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
        Ok(Self::Postgres(pool))
    }

    /// The underlying sqlite pool (C6's inbox/lease tables share it).
    ///
    /// C50 note: this is the *sqlite-only* accessor. The postgres variant
    /// carries no sqlite pool by construction, so calling it there is a
    /// programming error and panics with this message. Production code
    /// paths are backend-dispatched since C51; remaining callers are
    /// sqlite-only by construction (tests and sqlite-file readers).
    pub fn pool(&self) -> &SqlitePool {
        match self {
            EventStore::Sqlite(pool) => pool,
            EventStore::Postgres(_) => panic!(
                "EventStore::pool() is the sqlite-only pool accessor; this store is postgres-backed (C50; C51 migrates the remaining sqlite-only surfaces)"
            ),
        }
    }

    /// The underlying postgres pool when this store is postgres-backed.
    pub fn pg_pool(&self) -> Option<&PgPool> {
        match self {
            EventStore::Sqlite(_) => None,
            EventStore::Postgres(pool) => Some(pool),
        }
    }

    /// Closes the pool; queued work is dropped. Reopening the same file
    /// (or URL) rebuilds everything from the committed stream.
    pub async fn close(self) {
        match self {
            EventStore::Sqlite(pool) => pool.close().await,
            EventStore::Postgres(pool) => pool.close().await,
        }
    }

    /// Appends one event as a single-statement CAS transaction. Returns
    /// `Ok` only when the row is durably committed; a rejected write
    /// leaves the stream untouched.
    pub async fn append(
        &self,
        session: SessionId,
        event: Event,
        guard: AppendGuard,
    ) -> Result<(), StoreError> {
        Self::validate_append(&session, &event, guard)?;
        // Persist-what-you-send: the envelope column holds the exact
        // serialized bytes (frozen v0 shape from C2), so replay and audit
        // exports are byte-identical — on either backend (the C50 export
        // parity golden).
        let envelope = serde_json::to_string(&event)?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::append(pool, session, &event, &envelope, guard).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::append(pool, session, &event, &envelope, guard).await
            }
        }
    }

    /// Guard/event coherence is backend-independent: checked once here,
    /// rejected before any driver is touched.
    fn validate_append(
        session: &SessionId,
        event: &Event,
        guard: AppendGuard,
    ) -> Result<(), StoreError> {
        if event.version > SCHEMA_VERSION {
            return Err(StoreError::UnsupportedVersion {
                found: event.version,
                current: SCHEMA_VERSION,
            });
        }
        if event.session != *session {
            return Err(StoreError::GuardMismatch(
                "event.session disagrees with the session argument",
            ));
        }
        if event.epoch != guard.epoch {
            return Err(StoreError::GuardMismatch(
                "guard.epoch disagrees with event.epoch",
            ));
        }
        if event.seq != guard.expected_seq {
            return Err(StoreError::GuardMismatch(
                "guard.expected_seq disagrees with event.seq",
            ));
        }
        Ok(())
    }

    /// The end write: session end + state transition as exactly one
    /// atomic append (NoHalfTerminated, constraint 4). The terminal state
    /// is projected from this single row by callers — the store performs
    /// no second write. `EndReason` first-write-wins lives in the C3
    /// projection, so a resumed session's later end event appends
    /// normally under the same fences.
    pub async fn append_end(
        &self,
        session: SessionId,
        event: Event,
        guard: AppendGuard,
    ) -> Result<(), StoreError> {
        if !matches!(event.kind, EventKind::SessionEnded { .. }) {
            return Err(StoreError::NotAnEndWrite {
                kind: event.kind.as_str(),
            });
        }
        self.append(session, event, guard).await
    }

    /// Appends a contiguous batch of events as ONE atomic CAS write
    /// (C32): the batch shares a single fence decision — the first event
    /// takes the CAS position and every event carries the guard's epoch —
    /// so a rejected write (stale epoch, seq mismatch) commits NOTHING.
    /// Used when two events are one logical fact (the spill sidecar's
    /// `input/admitted` + `input/spilled` pair): half a pair must never
    /// become durable (NoSplitBrainWrite over the whole batch).
    ///
    /// C50: on sqlite the batch is a guarded first-row CAS + tail inserts
    /// in one transaction; on postgres it is a single CTE statement —
    /// all rows land or none on both backends.
    pub async fn append_batch(
        &self,
        session: SessionId,
        events: &[Event],
        guard: AppendGuard,
    ) -> Result<(), StoreError> {
        if events.is_empty() {
            return Err(StoreError::GuardMismatch("empty batch append"));
        }
        for (i, event) in events.iter().enumerate() {
            if event.version > SCHEMA_VERSION {
                return Err(StoreError::UnsupportedVersion {
                    found: event.version,
                    current: SCHEMA_VERSION,
                });
            }
            if event.session != session {
                return Err(StoreError::GuardMismatch(
                    "event.session disagrees with the session argument",
                ));
            }
            if event.epoch != guard.epoch {
                return Err(StoreError::GuardMismatch(
                    "guard.epoch disagrees with event.epoch",
                ));
            }
            if event.seq != guard.expected_seq + i as u64 {
                return Err(StoreError::GuardMismatch(
                    "batch seqs must be contiguous from guard.expected_seq",
                ));
            }
        }
        let envelopes = events
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::append_batch(pool, session, events, &envelopes, guard).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::append_batch(pool, session, events, &envelopes, guard).await
            }
        }
    }

    /// Reads a session's events back in seq order over `[start, end)`.
    /// The stored envelope is deserialized as-is; the store keeps no
    /// projection state (projections are computed by callers).
    pub async fn scan(
        &self,
        session: SessionId,
        range: Range<u64>,
    ) -> Result<Vec<Event>, StoreError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::scan(pool, session, range).await,
            EventStore::Postgres(pool) => crate::postgres::scan(pool, session, range).await,
        }
    }

    /// The session's stream position: next free seq and epoch floor.
    pub async fn head(&self, session: SessionId) -> Result<Head, StoreError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::head(pool, session).await,
            EventStore::Postgres(pool) => crate::postgres::head(pool, session).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;
    use sqlx::sqlite::SqliteConnection;
    use uuid::Uuid;

    use super::{AppendGuard, EventStore, Head, StoreError};

    // -- helpers ---------------------------------------------------------

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-store-test-{}.sqlite", Uuid::new_v4()))
    }

    async fn test_store() -> (EventStore, PathBuf) {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        (store, path)
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn ev(session: SessionId, seq: u64, epoch: Epoch, kind: EventKind) -> Event {
        Event {
            seq,
            session,
            epoch,
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes: json!({ "n": seq }),
            version: SCHEMA_VERSION,
        }
    }

    /// Seeds a session stream of `upto` events (seq 1..=upto, all under
    /// `epoch`), the way a lawful drain would.
    async fn seed(store: &EventStore, session: SessionId, upto: u64, epoch: Epoch) {
        for seq in 1..=upto {
            let kind = if seq == 1 {
                EventKind::SessionCreated
            } else {
                EventKind::TurnStart
            };
            store
                .append(
                    session,
                    ev(session, seq, epoch, kind),
                    AppendGuard {
                        epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .expect("seed append");
        }
    }

    fn all() -> std::ops::Range<u64> {
        0..i64::MAX as u64
    }

    /// The migration chain's head version, so reopen assertions track the
    /// chain instead of hardcoding its Phase-1 length (C6 extended it to
    /// v1; future generations extend it further).
    fn kian_store_chain_head() -> u32 {
        crate::migrations::chain()
            .last()
            .map(|m| m.version)
            .unwrap_or(0)
    }

    fn ser(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect()
    }

    async fn raw_connection(path: &Path) -> SqliteConnection {
        use sqlx::Connection;
        let opts = sqlx::sqlite::SqliteConnectOptions::new().filename(path);
        SqliteConnection::connect_with(&opts)
            .await
            .expect("raw sqlite connection")
    }

    // -- NoSplitBrainWrite -------------------------------------------------

    /// A stale-epoch write (a zombie drain from before a newer claim) is
    /// rejected even when its seq guess is correct (ADR-005).
    #[tokio::test]
    async fn stale_epoch_append_is_rejected() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;

        let err = store
            .append(
                sid,
                ev(sid, 4, Epoch(3), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(3),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::StaleEpoch {
                    event_epoch: Epoch(3),
                    current_epoch: Epoch(5),
                    ..
                }
            ),
            "expected StaleEpoch, got {err:?}"
        );

        // The rejected write left the stream untouched.
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        assert_eq!(
            store.head(sid).await.unwrap(),
            Head {
                next_seq: 4,
                epoch: Epoch(5)
            }
        );
        store.close().await;
        cleanup(&path);
    }

    // -- C32 batch append: one logical fact, one fenced write --------------

    /// A batch append commits every event contiguously, in order, under
    /// one CAS — the happy path of the spill pair.
    #[tokio::test]
    async fn append_batch_commits_the_whole_batch_contiguously() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(1)).await;

        let batch = [
            ev(sid, 4, Epoch(1), EventKind::InputAdmitted),
            ev(sid, 5, Epoch(1), EventKind::InputSpilled),
        ];
        store
            .append_batch(
                sid,
                &batch,
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 4,
                },
            )
            .await
            .expect("batch append");

        let events = store.scan(sid, all()).await.unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[3], batch[0]);
        assert_eq!(events[4], batch[1]);
        assert_eq!(store.head(sid).await.unwrap().next_seq, 6);
        store.close().await;
        cleanup(&path);
    }

    /// A stale-epoch batch writes NEITHER event: the pair is one logical
    /// fact, so a fenced-out writer must not leave half of it behind.
    #[tokio::test]
    async fn append_batch_stale_epoch_writes_neither_event() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;

        let batch = [
            ev(sid, 4, Epoch(3), EventKind::InputAdmitted),
            ev(sid, 5, Epoch(3), EventKind::InputSpilled),
        ];
        let err = store
            .append_batch(
                sid,
                &batch,
                AppendGuard {
                    epoch: Epoch(3),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::StaleEpoch {
                    event_epoch: Epoch(3),
                    ..
                }
            ),
            "expected StaleEpoch, got {err:?}"
        );
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        assert_eq!(
            store.head(sid).await.unwrap(),
            Head {
                next_seq: 4,
                epoch: Epoch(5)
            }
        );
        store.close().await;
        cleanup(&path);
    }

    /// A seq-mismatched batch writes neither event either — the batch
    /// aborts before any row is committed.
    #[tokio::test]
    async fn append_batch_seq_mismatch_writes_neither_event() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(1)).await;

        let batch = [
            ev(sid, 5, Epoch(1), EventKind::InputAdmitted),
            ev(sid, 6, Epoch(1), EventKind::InputSpilled),
        ];
        let err = store
            .append_batch(
                sid,
                &batch,
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 5,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::SeqMismatch {
                    expected_seq: 5,
                    ..
                }
            ),
            "expected SeqMismatch, got {err:?}"
        );
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        store.close().await;
        cleanup(&path);
    }

    /// Malformed batches (non-contiguous seqs, mixed epochs, wrong
    /// session) are rejected before touching sqlite.
    #[tokio::test]
    async fn append_batch_rejects_malformed_batches() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let other = SessionId::new();

        let guard = AppendGuard {
            epoch: Epoch(1),
            expected_seq: 1,
        };
        let err = store.append_batch(sid, &[], guard).await.unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        let non_contiguous = [
            ev(sid, 1, Epoch(1), EventKind::SessionCreated),
            ev(sid, 3, Epoch(1), EventKind::InputAdmitted),
        ];
        let err = store
            .append_batch(sid, &non_contiguous, guard)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        let mixed_epochs = [
            ev(sid, 1, Epoch(1), EventKind::SessionCreated),
            ev(sid, 2, Epoch(2), EventKind::InputAdmitted),
        ];
        let err = store
            .append_batch(sid, &mixed_epochs, guard)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        let wrong_session = [ev(other, 1, Epoch(1), EventKind::SessionCreated)];
        let err = store
            .append_batch(sid, &wrong_session, guard)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 0);
        store.close().await;
        cleanup(&path);
    }

    /// An append whose expected_seq does not match the stream head is
    /// rejected, on a fresh session and on an established one.
    #[tokio::test]
    async fn wrong_expected_seq_is_rejected() {
        let (store, path) = test_store().await;

        // Fresh session: the first event must claim seq 1.
        let fresh = SessionId::new();
        let err = store
            .append(
                fresh,
                ev(fresh, 0, Epoch(1), EventKind::SessionCreated),
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 0,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::SeqMismatch {
                    expected_seq: 0,
                    actual_next: 1,
                    ..
                }
            ),
            "expected SeqMismatch, got {err:?}"
        );

        // Established session at head 3.
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;
        let err = store
            .append(
                sid,
                ev(sid, 7, Epoch(5), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 7,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::SeqMismatch {
                    expected_seq: 7,
                    actual_next: 4,
                    ..
                }
            ),
            "expected SeqMismatch, got {err:?}"
        );
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        store.close().await;
        cleanup(&path);
    }

    /// Two concurrent appends racing for the same seq: exactly one wins
    /// (NoSplitBrainWrite), the loser gets a typed CAS error, and the
    /// stream stays contiguous.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_exactly_one_wins() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 2, Epoch(5)).await;

        let a = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        sid,
                        ev(sid, 3, Epoch(5), EventKind::TurnStart),
                        AppendGuard {
                            epoch: Epoch(5),
                            expected_seq: 3,
                        },
                    )
                    .await
            })
        };
        let b = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        sid,
                        ev(sid, 3, Epoch(5), EventKind::ToolCall),
                        AppendGuard {
                            epoch: Epoch(5),
                            expected_seq: 3,
                        },
                    )
                    .await
            })
        };
        let ra = a.await.unwrap();
        let rb = b.await.unwrap();
        assert!(
            ra.is_ok() ^ rb.is_ok(),
            "exactly one append must win, got {ra:?} and {rb:?}"
        );

        let events = store.scan(sid, all()).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].seq, 3);
        assert_eq!(events[2].epoch, Epoch(5));
        assert!(
            matches!(events[2].kind, EventKind::TurnStart | EventKind::ToolCall),
            "the winner's event is the one stored"
        );
        store.close().await;
        cleanup(&path);
    }

    // -- NoHalfTerminated ----------------------------------------------------

    /// The end write is one atomic append: a concurrent reader never
    /// observes a half-terminated stream — every snapshot is a contiguous
    /// prefix, `session/ended` appears at most once and only at the tail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn end_write_is_one_atomic_append_with_no_half_terminated_state() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 5, Epoch(5)).await;

        let scanner = {
            let store = store.clone();
            tokio::spawn(async move {
                let mut snapshots = Vec::new();
                for _ in 0..400 {
                    let evs = store.scan(sid, all()).await.unwrap();
                    snapshots.push(evs);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                snapshots
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .append_end(
                sid,
                ev(
                    sid,
                    6,
                    Epoch(5),
                    EventKind::SessionEnded {
                        reason: EndReason::Completed,
                    },
                ),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 6,
                },
            )
            .await
            .expect("end write");

        let snapshots = scanner.await.unwrap();
        assert!(!snapshots.is_empty());
        for snap in &snapshots {
            // No torn multi-row writes: seqs are always a contiguous
            // 1..=len prefix of the final stream.
            for (i, e) in snap.iter().enumerate() {
                assert_eq!(e.seq, i as u64 + 1, "gap or duplicate in snapshot");
            }
            // The end write is visible only as one whole row at the tail —
            // never a bare marker row, never mid-stream.
            let ended: Vec<usize> = snap
                .iter()
                .enumerate()
                .filter(|(_, e)| matches!(e.kind, EventKind::SessionEnded { .. }))
                .map(|(i, _)| i)
                .collect();
            assert!(ended.len() <= 1, "duplicate session/ended in one snapshot");
            if let Some(&i) = ended.first() {
                assert_eq!(i, snap.len() - 1, "session/ended must be the tail event");
            }
        }

        let final_events = store.scan(sid, all()).await.unwrap();
        assert_eq!(final_events.len(), 6);
        assert!(matches!(
            final_events[5].kind,
            EventKind::SessionEnded {
                reason: EndReason::Completed
            }
        ));
        store.close().await;
        cleanup(&path);
    }

    // -- durability --------------------------------------------------------

    /// A connection that dies with an open transaction leaves zero
    /// partial rows: sqlite rolls back on close, so nothing uncommitted
    /// is ever observable.
    #[tokio::test]
    async fn connection_dropped_mid_transaction_leaves_zero_partial_rows() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();

        let mut conn = raw_connection(&path).await;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        for seq in 1..=2i64 {
            sqlx::query(
                "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(sid.0.to_string())
            .bind(seq)
            .bind(5i64)
            .bind(0i64)
            .bind("turn/start")
            .bind(0i64)
            .bind("{}")
            .execute(&mut conn)
            .await
            .unwrap();
        }
        // The client dies before COMMIT: close with the transaction open.
        sqlx::Connection::close(conn).await.unwrap();

        assert!(
            store.scan(sid, all()).await.unwrap().is_empty(),
            "uncommitted rows must not be observable"
        );
        assert_eq!(
            store.head(sid).await.unwrap(),
            Head {
                next_seq: 1,
                epoch: Epoch::GENESIS
            }
        );
        store.close().await;
        cleanup(&path);
    }

    /// An old DB holding a committed v0 stream reopens cleanly: the
    /// migration chain is a no-op, every generation stays applied exactly
    /// once, and the events read back byte-identically.
    #[tokio::test]
    async fn reopen_old_db_with_v0_stream_migrates_cleanly() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(2)).await;
        let before = store.scan(sid, all()).await.unwrap();
        store.close().await;

        let reopened = EventStore::open(&path).await.expect("reopen");
        let after = reopened.scan(sid, all()).await.unwrap();
        assert_eq!(ser(&after), ser(&before));
        assert_eq!(
            crate::migrations::applied_versions(reopened.pool())
                .await
                .unwrap(),
            (0..=kian_store_chain_head()).collect::<Vec<_>>(),
            "reopen applies every unapplied generation exactly once"
        );
        reopened.close().await;
        cleanup(&path);
    }

    // -- read path -----------------------------------------------------------

    #[tokio::test]
    async fn append_then_scan_round_trips_events() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 4, Epoch(5)).await;

        let events = store.scan(sid, all()).await.unwrap();
        let expected: Vec<Event> = (1..=4)
            .map(|seq| {
                let kind = if seq == 1 {
                    EventKind::SessionCreated
                } else {
                    EventKind::TurnStart
                };
                ev(sid, seq, Epoch(5), kind)
            })
            .collect();
        assert_eq!(ser(&events), ser(&expected));
        store.close().await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn scan_returns_requested_seq_range() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 5, Epoch(5)).await;

        let seqs = |events: &[Event]| events.iter().map(|e| e.seq).collect::<Vec<_>>();
        assert_eq!(seqs(&store.scan(sid, 2..4).await.unwrap()), vec![2, 3]);
        // Half-open Range semantics: [1, 2) is exactly the first event.
        assert_eq!(seqs(&store.scan(sid, 1..2).await.unwrap()), vec![1]);
        // [0, 1) is empty — seqs start at 1.
        assert!(store.scan(sid, 0..1).await.unwrap().is_empty());
        assert!(store.scan(sid, 9..12).await.unwrap().is_empty());
        store.close().await;
        cleanup(&path);
    }

    #[tokio::test]
    async fn head_reports_next_seq_and_epoch_floor() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        assert_eq!(
            store.head(sid).await.unwrap(),
            Head {
                next_seq: 1,
                epoch: Epoch::GENESIS
            }
        );
        seed(&store, sid, 3, Epoch(7)).await;
        assert_eq!(
            store.head(sid).await.unwrap(),
            Head {
                next_seq: 4,
                epoch: Epoch(7)
            }
        );
        store.close().await;
        cleanup(&path);
    }

    // -- guard hygiene ---------------------------------------------------------

    /// The store, not the caller's carefulness, enforces guard/event
    /// coherence; a mismatched guard is a programming error, not a race.
    #[tokio::test]
    async fn append_rejects_guard_event_mismatch() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;

        // guard.epoch disagrees with event.epoch
        let err = store
            .append(
                sid,
                ev(sid, 4, Epoch(5), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(4),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        // guard.expected_seq disagrees with event.seq
        let err = store
            .append(
                sid,
                ev(sid, 4, Epoch(5), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 5,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        // event.session disagrees with the session argument
        let err = store
            .append(
                sid,
                ev(SessionId::new(), 4, Epoch(5), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::GuardMismatch(_)), "{err:?}");

        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        store.close().await;
        cleanup(&path);
    }

    /// An envelope from a future schema version is refused: it must wait
    /// for the adjacent migration that understands it (DESIGN §3.3).
    #[tokio::test]
    async fn append_rejects_unsupported_envelope_version() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;

        let mut future = ev(sid, 4, Epoch(5), EventKind::TurnStart);
        future.version = SCHEMA_VERSION + 1;
        let err = store
            .append(
                sid,
                future,
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::UnsupportedVersion {
                    found: 1,
                    current: 0
                }
            ),
            "{err:?}"
        );
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        store.close().await;
        cleanup(&path);
    }

    /// The end-write API accepts only `session/ended`.
    #[tokio::test]
    async fn append_end_rejects_non_terminal_event() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed(&store, sid, 3, Epoch(5)).await;

        let err = store
            .append_end(
                sid,
                ev(sid, 4, Epoch(5), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::NotAnEndWrite { .. }), "{err:?}");
        assert_eq!(store.scan(sid, all()).await.unwrap().len(), 3);
        store.close().await;
        cleanup(&path);
    }
}

/// Review assertions (card C5): properties enforced by scanning the crate
/// source itself, so a violation fails `cargo test` instead of waiting
/// for human review.
#[cfg(test)]
mod review {
    use std::fs;
    use std::path::{Path, PathBuf};

    fn rust_sources_under(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                rust_sources_under(&p, out);
            } else if p.extension().is_some_and(|e| e == "rs") {
                out.push(p);
            }
        }
    }

    /// The events table is append-only by construction: no statement that
    /// could rewrite or remove a committed row may exist anywhere in this
    /// crate. The forbidden fragments are assembled from split pieces so
    /// this file itself cannot contain them literally.
    #[test]
    fn no_rewrite_statement_against_events_table_in_crate_source() {
        let forbidden: [String; 4] = [
            ["upd", "ate ev", "ents"].concat(),
            ["del", "ete from ev", "ents"].concat(),
            ["ins", "ert or replace into ev", "ents"].concat(),
            ["dr", "op table ev", "ents"].concat(),
        ];
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        rust_sources_under(&src, &mut files);
        assert!(
            !files.is_empty(),
            "no sources found under {}",
            src.display()
        );
        for f in files {
            let content = fs::read_to_string(&f).unwrap_or_default().to_lowercase();
            for pat in &forbidden {
                assert!(
                    !content.contains(pat),
                    "append-only violation: {pat:?} found in {}",
                    f.display()
                );
            }
        }
    }
}
