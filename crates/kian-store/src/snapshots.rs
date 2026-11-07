//! Read-model snapshot storage (READ-MODEL §5/§9, migration v3).
//!
//! Snapshot rows are **infrastructure state, never truth** (R1): they are
//! a disposable, verified cache of the projection (`state` + model
//! `context` + per-message provenance) at one committed version. The
//! table can be dropped and rebuilt from the event stream at any time;
//! no reader may treat a row as authority — the loader in
//! `kian-orchestrator` verifies and falls back to full stream derivation
//! on any doubt.
//!
//! Publication shape (R6): a builder writes a row only **after** a second
//! independent derivation verified `(state, context, provenance)` against
//! the first, so the INSERT lands an already-verified row and a crash
//! window can leave nothing or one complete row — never a partial
//! snapshot. `INSERT … ON CONFLICT DO NOTHING` keeps publication
//! insert-only: a previously visible version is never overwritten, and a
//! racing replica's identical build is an idempotent no-op.
//!
//! `maintenance_leases` carries the builder's liveness lease (per-scope
//! TTL + heartbeat). Like the claims lease (ADR-005) it is **liveness
//! only**: safety comes from the insert-only publication shape, never
//! from the lease.
//!
//! The dialect SQL lives in [`crate::sqlite`] / [`crate::postgres`]; this
//! module owns the row types, the dual-backend dispatch and the
//! u64→i64 boundary (both chains use the same versions and names —
//! enforced by the migration drift tests).

use kian_core::model::SessionId;

use crate::event_store::{EventStore, StoreError, to_i64};

/// A stored snapshot row. `state`/`context`/`context_provenance` are the
/// exact serialized projections (JSON strings); `digest` is the hex
/// sha256 over the three, computed by the builder and re-checked by the
/// loader before a row is trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub session: SessionId,
    pub version: u64,
    /// JSON: the full state fold at `version`
    /// (`{"state": …, "episode_end": …}` — the fold seed the loader
    /// replays the tail from).
    pub state: String,
    /// JSON: `derive_model_context`'s output at `version` (the exact
    /// `context_to_bytes` form — the request bytes stay owned by
    /// `kian-core` and unchanged).
    pub context: String,
    /// JSON: per-message source event seqs at `version` (the provenance
    /// a later `compact` is applied against).
    pub context_provenance: String,
    pub digest: String,
    pub verified: bool,
    pub created_at: u64,
}

/// A snapshot-builder candidate: a session whose head version reached the
/// configured minimum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCandidate {
    pub session: SessionId,
    pub version: u64,
}

fn decode_session(raw: &str) -> SessionId {
    // Candidate session keys come from the events table, where they were
    // written by the store from a SessionId's uuid string.
    SessionId(uuid::Uuid::parse_str(raw).unwrap_or_else(|_| {
        panic!("snapshot candidate row carries a non-uuid session key: {raw:?}")
    }))
}

/// The publication payload the dispatch layer hands the backend
/// (clippy-friendly: one struct instead of a positional column list).
pub(crate) struct SnapshotInsert<'a> {
    pub session_key: &'a str,
    pub version: i64,
    pub state: &'a str,
    pub context: &'a str,
    pub provenance: &'a str,
    pub digest: &'a str,
    pub created_at: i64,
}

impl EventStore {
    /// Publishes one verified snapshot row (insert-only, R6). Returns
    /// `true` when this call published the row; `false` when an identical
    /// version already exists (a racing replica's build — an idempotent
    /// no-op, never an overwrite).
    pub async fn snapshot_insert_verified(&self, row: &SnapshotRow) -> Result<bool, StoreError> {
        let payload = SnapshotInsert {
            session_key: &row.session.0.to_string(),
            version: to_i64(row.version, "snapshot version")?,
            state: &row.state,
            context: &row.context,
            provenance: &row.context_provenance,
            digest: &row.digest,
            created_at: to_i64(row.created_at, "snapshot created_at")?,
        };
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::snapshot_insert(pool, &payload).await,
            EventStore::Postgres(pool) => crate::postgres::snapshot_insert(pool, &payload).await,
        }
    }

    /// The highest **verified** snapshot row of the session, if any.
    /// Unverified rows are never visible to readers.
    pub async fn snapshot_highest_verified(
        &self,
        session: SessionId,
    ) -> Result<Option<SnapshotRow>, StoreError> {
        let cols = match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::snapshot_highest(pool, &session.0.to_string()).await?
            }
            EventStore::Postgres(pool) => {
                crate::postgres::snapshot_highest(pool, &session.0.to_string()).await?
            }
        };
        Ok(cols.map(
            |(version, state, context, provenance, digest, verified, created_at)| SnapshotRow {
                session,
                version: version.max(0) as u64,
                state,
                context,
                context_provenance: provenance,
                digest,
                verified: verified == 1,
                created_at: created_at.max(0) as u64,
            },
        ))
    }

    /// GC (R6: orphans are GC-able): keeps the newest `keep_n` versions
    /// of the session and deletes everything older — verified or not.
    /// The just-published row is always among the newest, so GC never
    /// undoes a publication. Returns the number of rows deleted.
    pub async fn snapshot_gc(&self, session: SessionId, keep_n: u64) -> Result<u64, StoreError> {
        let keep_n = to_i64(keep_n, "snapshot keep_n")?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::snapshot_gc(pool, &session.0.to_string(), keep_n).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::snapshot_gc(pool, &session.0.to_string(), keep_n).await
            }
        }
    }

    /// Snapshot-builder candidates: sessions whose head version reached
    /// `min_events`, with that version. One aggregate over the events PK
    /// — maintenance cadence only, never the serving path.
    pub async fn snapshot_candidates(
        &self,
        min_events: u64,
        limit: u64,
    ) -> Result<Vec<SnapshotCandidate>, StoreError> {
        let min_events = to_i64(min_events, "snapshot min_events")?;
        let limit = to_i64(limit, "snapshot limit")?;
        let rows = match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::snapshot_candidates(pool, min_events, limit).await?
            }
            EventStore::Postgres(pool) => {
                crate::postgres::snapshot_candidates(pool, min_events, limit).await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|(session, version)| SnapshotCandidate {
                session: decode_session(&session),
                version: version.max(0) as u64,
            })
            .collect())
    }

    /// Tries to acquire the maintenance lease for `scope` (liveness
    /// only, READ-MODEL §9). `false` = another holder's lease is still
    /// live; the caller skips (idempotent under retry).
    pub async fn maintenance_lease_try_acquire(
        &self,
        scope: &str,
        holder: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let ttl_ms = to_i64(ttl_ms, "maintenance ttl_ms")?;
        let now_ms = to_i64(now_ms, "maintenance now_ms")?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::maintenance_acquire(pool, scope, holder, ttl_ms, now_ms).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::maintenance_acquire(pool, scope, holder, ttl_ms, now_ms).await
            }
        }
    }

    /// Extends the caller's maintenance lease. `false` = the lease was
    /// lost (expired and taken over); liveness-only, so the caller just
    /// stops — safety never depended on the lease.
    pub async fn maintenance_lease_heartbeat(
        &self,
        scope: &str,
        holder: &str,
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let now_ms = to_i64(now_ms, "maintenance now_ms")?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::maintenance_heartbeat(pool, scope, holder, now_ms).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::maintenance_heartbeat(pool, scope, holder, now_ms).await
            }
        }
    }

    /// Releases the caller's maintenance lease (a no-op when already
    /// lost — idempotent under retry, R9).
    pub async fn maintenance_lease_release(
        &self,
        scope: &str,
        holder: &str,
    ) -> Result<(), StoreError> {
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::maintenance_release(pool, scope, holder).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::maintenance_release(pool, scope, holder).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{SnapshotCandidate, SnapshotRow};
    use crate::{AppendGuard, EventStore};

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-snapshots-test-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
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

    async fn seed(store: &EventStore, session: SessionId, upto: u64) {
        for seq in 1..=upto {
            let kind = if seq == 1 {
                EventKind::SessionCreated
            } else if seq == 2 {
                EventKind::DrainClaimed
            } else {
                EventKind::TurnEnd
            };
            store
                .append(
                    session,
                    ev(session, seq, Epoch::GENESIS, kind),
                    AppendGuard {
                        epoch: Epoch::GENESIS,
                        expected_seq: seq,
                    },
                )
                .await
                .expect("seed append");
        }
    }

    fn row(session: SessionId, version: u64) -> SnapshotRow {
        SnapshotRow {
            session,
            version,
            state: r#"{"state":"running","episode_end":null}"#.to_string(),
            context: "[]".into(),
            context_provenance: "[]".into(),
            digest: format!("digest-{version}"),
            verified: true,
            created_at: 42,
        }
    }

    /// Insert-only publication: the first insert lands, a re-publish of
    /// the same version is an idempotent no-op (never an overwrite — a
    /// racing replica's different digest cannot replace a visible row).
    #[tokio::test]
    async fn publication_is_insert_only_and_idempotent() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId::new();

        assert!(
            store
                .snapshot_insert_verified(&row(sid, 5))
                .await
                .expect("publish")
        );
        // Same version again: no-op, and the stored row is untouched.
        let mut other = row(sid, 5);
        other.digest = "forged".into();
        assert!(
            !store
                .snapshot_insert_verified(&other)
                .await
                .expect("re-publish")
        );
        let stored = store
            .snapshot_highest_verified(sid)
            .await
            .expect("read")
            .expect("row exists");
        assert_eq!(stored.digest, "digest-5");

        store.close().await;
        cleanup(&path);
    }

    /// The highest-verified query surfaces only verified rows and picks
    /// the greatest version.
    #[tokio::test]
    async fn highest_verified_picks_the_greatest_version() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId::new();
        assert!(
            store
                .snapshot_highest_verified(sid)
                .await
                .expect("read")
                .is_none()
        );

        for v in [10u64, 20, 30] {
            assert!(
                store
                    .snapshot_insert_verified(&row(sid, v))
                    .await
                    .expect("p")
            );
        }
        let top = store
            .snapshot_highest_verified(sid)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(top.version, 30);

        // An unverified row (written behind the API's back, simulating a
        // foreign/corrupt writer) is never visible.
        if let EventStore::Sqlite(pool) = &store {
            sqlx::query(
                "INSERT INTO projection_snapshots (session, version, state, context, context_provenance, digest, verified, created_at) \
                 VALUES (?, ?, '{}', '[]', '[]', 'x', 0, 0)",
            )
            .bind(sid.0.to_string())
            .bind(40i64)
            .execute(pool)
            .await
            .expect("plant unverified");
        }
        let top = store
            .snapshot_highest_verified(sid)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(top.version, 30, "an unverified row must stay invisible");

        store.close().await;
        cleanup(&path);
    }

    /// GC keeps the newest `keep_n` and deletes older orphans (verified
    /// or not) — and never touches the just-published head.
    #[tokio::test]
    async fn gc_keeps_the_newest_rows_only() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId::new();
        for v in [1u64, 2, 3, 4, 5] {
            store
                .snapshot_insert_verified(&row(sid, v))
                .await
                .expect("publish");
        }
        assert_eq!(store.snapshot_gc(sid, 2).await.expect("gc"), 3);
        let top = store
            .snapshot_highest_verified(sid)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(top.version, 5);

        store.close().await;
        cleanup(&path);
    }

    /// Candidates: sessions past the configured minimum, with their head
    /// version; smaller sessions are skipped.
    #[tokio::test]
    async fn candidates_list_sessions_past_the_minimum() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let big = SessionId::new();
        let small = SessionId::new();
        seed(&store, big, 7).await;
        seed(&store, small, 3).await;

        let cands = store.snapshot_candidates(5, 100).await.expect("candidates");
        assert_eq!(
            cands,
            vec![SnapshotCandidate {
                session: big,
                version: 7
            }]
        );

        store.close().await;
        cleanup(&path);
    }

    /// The maintenance lease: mutual exclusion while live, takeover
    /// after expiry, heartbeat extension, idempotent release (R9).
    #[tokio::test]
    async fn maintenance_lease_liveness_semantics() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let scope = "snapshots:test";

        assert!(
            store
                .maintenance_lease_try_acquire(scope, "a", 1000, 10_000)
                .await
                .expect("acquire a")
        );
        // A different live holder is refused…
        assert!(
            !store
                .maintenance_lease_try_acquire(scope, "b", 1000, 10_500)
                .await
                .expect("acquire b refused")
        );
        // …the owner re-acquiring (heartbeat-shaped) succeeds…
        assert!(
            store
                .maintenance_lease_try_acquire(scope, "a", 1000, 10_500)
                .await
                .expect("re-acquire a")
        );
        // …and the explicit heartbeat extends it.
        assert!(
            store
                .maintenance_lease_heartbeat(scope, "a", 11_000)
                .await
                .expect("heartbeat")
        );
        // Still live for b at 11_500 (ttl 1000 from 11_000).
        assert!(
            !store
                .maintenance_lease_try_acquire(scope, "b", 1000, 11_500)
                .await
                .expect("b still refused")
        );
        // Past the TTL, b takes over (liveness only — expiry grants no
        // safety decision, just the next builder turn).
        assert!(
            store
                .maintenance_lease_try_acquire(scope, "b", 1000, 13_000)
                .await
                .expect("b takeover")
        );
        // a's heartbeat now reports the lost lease…
        assert!(
            !store
                .maintenance_lease_heartbeat(scope, "a", 13_100)
                .await
                .expect("a heartbeat lost")
        );
        // …and releasing a lost lease is an idempotent no-op.
        store
            .maintenance_lease_release(scope, "a")
            .await
            .expect("release a");
        assert!(
            store
                .maintenance_lease_heartbeat(scope, "b", 13_200)
                .await
                .expect("b still holds")
        );
        store
            .maintenance_lease_release(scope, "b")
            .await
            .expect("release b");
        // Released: anyone may take it again.
        assert!(
            store
                .maintenance_lease_try_acquire(scope, "a", 1000, 13_500)
                .await
                .expect("re-acquire after release")
        );

        store.close().await;
        cleanup(&path);
    }
}
