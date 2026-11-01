//! Append-only events table (card C5).
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
//! Schema versioning: the table carries the envelope `version` column and
//! is created by the adjacent migration chain in [`crate::migrations`]
//! (DESIGN §3.3).

use std::ops::Range;
use std::path::Path;
use std::time::Duration;

use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
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

/// The single-statement CAS append (ADR-005: "a sqlite transaction / a
/// single postgres statement suffices").
///
/// The insert lands only when both fences hold at statement time:
/// - epoch fence: the session's epoch floor (MAX(epoch) over committed
///   events) must not exceed the write's epoch — stale-epoch writes are
///   always rejected, lease state is never consulted (lease = liveness
///   only);
/// - seq fence: the stream head must be exactly `expected_seq - 1`, so
///   the primary key can never collide and seqs stay contiguous.
///
/// Anything else inserts zero rows and is classified into a typed error.
const APPEND_SQL: &str = "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope)\nSELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7\nWHERE (SELECT COALESCE(MAX(epoch), 0) FROM events WHERE session = ?8) <= ?9\n  AND (SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = ?10) = ?11 - 1";

const HEAD_SQL: &str =
    "SELECT COALESCE(MAX(epoch), 0), COALESCE(MAX(seq), 0) FROM events WHERE session = ?";

const SCAN_SQL: &str = "SELECT version, envelope FROM events WHERE session = ? AND seq >= ? AND seq < ? ORDER BY seq ASC";

fn to_i64(v: u64, field: &'static str) -> Result<i64, StoreError> {
    i64::try_from(v).map_err(|_| StoreError::IntOverflow { field })
}

/// The sqlite EventStore (Phase 1; postgres is Phase 2+ and will mirror
/// these semantics in single statements).
///
/// All access goes through the sqlx pool — the sqlite driver owns its
/// background thread, so no tokio worker ever blocks on a query (AGENTS
/// environment gotcha).
#[derive(Clone)]
pub struct EventStore {
    pool: SqlitePool,
}

impl EventStore {
    /// Opens (creating if needed) a database file, applies the migration
    /// chain, and returns a pooled store.
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
        let store = Self { pool };
        migrations::migrate(&store.pool).await?;
        Ok(store)
    }

    /// The underlying pool (C6's inbox/lease tables share it).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Opens an existing database file **read-only** (card C14: the
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
        Ok(Self { pool })
    }

    /// Closes the pool; queued work is dropped. Reopening the same file
    /// rebuilds everything from the committed stream.
    pub async fn close(self) {
        self.pool.close().await;
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
        if event.seq != guard.expected_seq {
            return Err(StoreError::GuardMismatch(
                "guard.expected_seq disagrees with event.seq",
            ));
        }
        // Persist-what-you-send: the envelope column holds the exact
        // serialized bytes (frozen v0 shape from C2), so replay and audit
        // exports are byte-identical.
        let envelope = serde_json::to_string(&event)?;
        let session_key = session.0.to_string();
        let result = sqlx::query(APPEND_SQL)
            .bind(&session_key)
            .bind(to_i64(event.seq, "seq")?)
            .bind(to_i64(event.epoch.0, "epoch")?)
            .bind(to_i64(event.ts, "ts")?)
            .bind(event.kind.as_str())
            .bind(i64::from(event.version))
            .bind(&envelope)
            // Fence inputs are bound again: sqlite parameters are
            // positional per index, and repeating ?N inline reads worse
            // than rebinding.
            .bind(&session_key)
            .bind(to_i64(event.epoch.0, "epoch")?)
            .bind(&session_key)
            .bind(to_i64(event.seq, "seq")?)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        // The CAS refused the write: classify which fence tripped.
        let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
            .bind(&session_key)
            .fetch_one(&self.pool)
            .await?;
        if guard.epoch.0 < epoch_floor as u64 {
            Err(StoreError::StaleEpoch {
                session,
                event_epoch: guard.epoch,
                current_epoch: Epoch(epoch_floor as u64),
            })
        } else {
            Err(StoreError::SeqMismatch {
                session,
                expected_seq: guard.expected_seq,
                actual_next: max_seq as u64 + 1,
            })
        }
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

    /// Reads a session's events back in seq order over `[start, end)`.
    /// The stored envelope is deserialized as-is; the store keeps no
    /// projection state (projections are computed by callers).
    pub async fn scan(
        &self,
        session: SessionId,
        range: Range<u64>,
    ) -> Result<Vec<Event>, StoreError> {
        let start = range.start;
        // Clamp the upper bound to sqlite's i64 domain so "everything"
        // can be expressed as 0..i64::MAX.
        let end = range.end.min(i64::MAX as u64);
        let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_SQL)
            .bind(session.0.to_string())
            .bind(to_i64(start, "seq")?)
            .bind(to_i64(end, "seq")?)
            .fetch_all(&self.pool)
            .await?;
        let mut events = Vec::with_capacity(rows.len());
        for (version_col, envelope) in rows {
            let event: Event = serde_json::from_str(&envelope)?;
            if i64::from(event.version) != version_col || event.session != session {
                return Err(StoreError::CorruptRow {
                    session,
                    seq: event.seq,
                });
            }
            events.push(event);
        }
        Ok(events)
    }

    /// The session's stream position: next free seq and epoch floor.
    pub async fn head(&self, session: SessionId) -> Result<Head, StoreError> {
        let (epoch, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
            .bind(session.0.to_string())
            .fetch_one(&self.pool)
            .await?;
        Ok(Head {
            next_seq: max_seq as u64 + 1,
            epoch: Epoch(epoch as u64),
        })
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
