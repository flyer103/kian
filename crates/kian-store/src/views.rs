//! Materialized-view storage (READ-MODEL §6, migration v4).
//!
//! View rows are **infrastructure state, never truth** (R1): a view is
//! a disposable, derived accelerator for observability reads. The
//! physical store is one row table (`view_rows`, keyed by the declared
//! logical view name + session, payload = the view's canonical JSON row
//! type) plus one `view_state` row per view recording:
//!
//! - the **store key** (`events_total`, `max_seq`) the rows were
//!   derived at — the freshness key. `seq` is per-session, so the row
//!   count is part of the key: appending to a short session leaves the
//!   global `MAX(seq)` untouched but bumps `events_total`, and a view
//!   built before that append must not be served.
//! - a **sha256 digest over the row set** (session-ascending) — the
//!   corruption check. A reader recomputes the digest over the rows it
//!   just read; any mismatch means the table was tampered with or
//!   partially lost, and the view is discarded (READ-MODEL §6: a
//!   corrupt view is discarded/rebuilt, never repaired by guessing).
//!
//! Publication is **one transaction per view**: delete the view's rows,
//! insert the new set, upsert `view_state`. Atomic on both backends, so
//! a crash leaves the old complete version or the new complete version
//! — never half a view ("a view is visible only when complete for its
//! version"). Unlike snapshots (R6 requires insert-only publication) a
//! view *replaces* its predecessor; that is safe because the whole
//! version flips in one transaction and no reader may treat a view as
//! authority anyway.
//!
//! Readers on stores opened **read-only** (no migrations run there) see
//! `Ok(None)` when the v4 tables do not exist — "no view" is the
//! honest answer, and the caller falls back to stream derivation (the
//! truth path; READ-MODEL §8: falling back to the stream is always
//! allowed, falling back from the stream never is).
//!
//! The dialect SQL lives in [`crate::sqlite`] / [`crate::postgres`].

use kian_core::events::Event;
use sha2::{Digest, Sha256};

use crate::event_store::{EventStore, StoreError, to_i64};

/// The freshness key of the whole store: `(total event rows, MAX(seq))`.
/// Both components are needed — `seq` is per-session, so two sessions
/// can share seq values and an append to the shorter one changes only
/// the count. Two aggregate columns read in one query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreKey {
    pub events_total: u64,
    pub max_seq: u64,
}

/// One stored view row: the session it describes, the session's head
/// version at derivation time, and the canonical JSON payload of the
/// view's row type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRow {
    pub session: String,
    pub row_version: u64,
    pub payload: String,
}

/// The published state of one view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewState {
    pub events_total: u64,
    pub max_seq: u64,
    pub digest: String,
    pub built_at: u64,
}

impl StoreKey {
    /// A view built at `self` is fresh against `current` iff the store
    /// has not changed since (exact equality — a view is served only at
    /// the version it was derived at; anything newer is stale and the
    /// read falls back to the stream).
    #[must_use]
    pub fn is_current(self, current: StoreKey) -> bool {
        self == current
    }
}

/// The row-payload schema version of every materialized view.
///
/// The digest domain below carries it, so a bump invalidates every
/// stored view at once: the next maintenance tick cannot reuse the old
/// bytes and derives the whole set again. Views are disposable
/// accelerators — serving rows written for another shape is not an
/// option, whether they still decode or not.
pub const VIEW_SCHEMA_VERSION: u32 = 1;

/// The sha256 digest over a view's row set, domain-separated per view
/// and per row-payload schema version. Rows are taken session-ascending
/// (the read order), so the digest is order-independent for any row set
/// a publisher sorted the same way.
#[must_use]
pub fn view_rows_digest(view: &str, rows: &[ViewRow]) -> String {
    view_rows_digest_in(view, rows, VIEW_SCHEMA_VERSION)
}

/// [`view_rows_digest`] at an explicit schema version (the version is
/// part of the domain, so two versions never collide).
fn view_rows_digest_in(view: &str, rows: &[ViewRow], version: u32) -> String {
    let mut h = Sha256::new();
    h.update(b"kian-view");
    h.update([0u8]);
    h.update(version.to_string().as_bytes());
    h.update([0u8]);
    h.update(view.as_bytes());
    h.update([0u8]);
    for row in rows {
        h.update(row.session.as_bytes());
        h.update([0u8]);
        h.update(row.row_version.to_string().as_bytes());
        h.update([0u8]);
        h.update(row.payload.as_bytes());
        h.update([0u8]);
    }
    let out = h.finalize();
    let mut hex = String::with_capacity(out.len() * 2);
    for b in out {
        hex.push_str(&format!("{b:02x}"));
    }
    hex
}

/// True when a read error means "the v4 tables do not exist on this
/// store" (a read-only open of a store last migrated before v4). Only
/// recognized shapes map to "no view"; anything else is a real error
/// and propagates.
fn tables_absent(e: &StoreError) -> bool {
    let msg = e.to_string();
    // sqlite: "no such table: view_rows"; pg: `relation "view_rows"
    // does not exist` (SQLSTATE 42P01).
    msg.contains("no such table") || msg.contains("does not exist")
}

impl EventStore {
    /// The store's freshness key (one aggregate query over the events
    /// table; v0 exists on every store, so this works read-only too).
    pub async fn view_store_key(&self) -> Result<StoreKey, StoreError> {
        let (total, max_seq): (i64, i64) = match self {
            EventStore::Sqlite(pool) => {
                sqlx::query_as(crate::sqlite::STORE_KEY_SQL)
                    .fetch_one(pool)
                    .await?
            }
            EventStore::Postgres(pool) => {
                sqlx::query_as(crate::postgres::STORE_KEY_SQL)
                    .fetch_one(pool)
                    .await?
            }
        };
        Ok(StoreKey {
            events_total: total.max(0) as u64,
            max_seq: max_seq.max(0) as u64,
        })
    }

    /// The published state of `view`, `None` when the view was never
    /// built (or the v4 tables do not exist on this store).
    pub async fn view_state(&self, view: &str) -> Result<Option<ViewState>, StoreError> {
        let row = match self {
            EventStore::Sqlite(pool) => {
                sqlx::query_as::<_, (i64, i64, String, i64)>(crate::sqlite::VIEW_STATE_SQL)
                    .bind(view)
                    .fetch_optional(pool)
                    .await
            }
            EventStore::Postgres(pool) => {
                sqlx::query_as::<_, (i64, i64, String, i64)>(crate::postgres::VIEW_STATE_SQL)
                    .bind(view)
                    .fetch_optional(pool)
                    .await
            }
        };
        let out = match row {
            Err(e) => {
                // A read-only open of a store last migrated before v4:
                // no tables, no view — the honest fallback input.
                let se = StoreError::Sql(e);
                if tables_absent(&se) {
                    return Ok(None);
                }
                return Err(se);
            }
            Ok(row) => row,
        };
        Ok(
            out.map(|(events_total, max_seq, digest, built_at)| ViewState {
                events_total: events_total.max(0) as u64,
                max_seq: max_seq.max(0) as u64,
                digest,
                built_at: built_at.max(0) as u64,
            }),
        )
    }

    /// The stored rows of `view`, session-ascending — the exact bytes
    /// the stored digest covers. `None` when the v4 tables do not exist
    /// on this store (a never-built view is `Some(vec![])` + a state
    /// row; callers check `view_state` for freshness).
    pub async fn view_rows(&self, view: &str) -> Result<Option<Vec<ViewRow>>, StoreError> {
        let rows = match self {
            EventStore::Sqlite(pool) => {
                sqlx::query_as::<_, (String, i64, String)>(crate::sqlite::VIEW_ROWS_SQL)
                    .bind(view)
                    .fetch_all(pool)
                    .await
            }
            EventStore::Postgres(pool) => {
                sqlx::query_as::<_, (String, i64, String)>(crate::postgres::VIEW_ROWS_SQL)
                    .bind(view)
                    .fetch_all(pool)
                    .await
            }
        };
        match rows {
            Err(e) => {
                let se = StoreError::Sql(e);
                if tables_absent(&se) {
                    return Ok(None);
                }
                Err(se)
            }
            Ok(rows) => Ok(Some(
                rows.into_iter()
                    .map(|(session, row_version, payload)| ViewRow {
                        session,
                        row_version: row_version.max(0) as u64,
                        payload,
                    })
                    .collect(),
            )),
        }
    }

    /// Publishes one view version atomically (READ-MODEL §6): sort the
    /// rows session-ascending (the read order the digest assumes), then
    /// — in ONE transaction — delete the view's previous rows, insert
    /// the new set and upsert `view_state` with the store key and the
    /// digest over exactly the bytes written. A crash leaves the old
    /// complete version or the new one, never half a view. Idempotent
    /// under retry: republishing the same set is a byte-identical
    /// replacement (R9).
    pub async fn view_publish(
        &self,
        view: &str,
        mut rows: Vec<ViewRow>,
        key: StoreKey,
        built_at: u64,
    ) -> Result<(), StoreError> {
        rows.sort_by(|a, b| a.session.cmp(&b.session));
        let digest = view_rows_digest(view, &rows);
        let payload = crate::views::publish::Publish {
            view,
            rows: &rows,
            events_total: to_i64(key.events_total, "view events_total")?,
            max_seq: to_i64(key.max_seq, "view max_seq")?,
            digest: &digest,
            built_at: to_i64(built_at, "view built_at")?,
        };
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::view_publish(pool, &payload).await,
            EventStore::Postgres(pool) => crate::postgres::view_publish(pool, &payload).await,
        }
    }

    /// Drops one view entirely (rows + state). Used when the view
    /// builder is disabled by config — a disabled builder hides every
    /// view — and by the maintenance path to discard a corrupt view
    /// before rebuilding it.
    pub async fn view_drop(&self, view: &str) -> Result<(), StoreError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::view_drop(pool, view).await,
            EventStore::Postgres(pool) => crate::postgres::view_drop(pool, view).await,
        }
    }

    /// R9-10 (incremental rebuild): every live session's head seq in
    /// one index-only aggregate — the change detector's whole-store
    /// read. A stored view row whose `row_version` equals the live
    /// head covers an identical stream (streams are append-only under
    /// CAS guards, so the head determines the stream), which is what
    /// makes row reuse sound.
    pub async fn session_heads(&self) -> Result<Vec<(String, u64)>, StoreError> {
        let heads = match self {
            EventStore::Sqlite(pool) => crate::sqlite::session_heads(pool).await,
            EventStore::Postgres(pool) => crate::postgres::session_heads(pool).await,
        }?;
        Ok(heads
            .into_iter()
            .map(|(session, head)| (session, head.max(0) as u64))
            .collect())
    }

    /// R9-10: pending-eligible inbox counts per session in one grouped
    /// read — the `ticket_stage` drift check (`pending_inbox` moves
    /// with `eligible_at` alone, no append).
    pub async fn pending_input_counts(
        &self,
        now_ms: u64,
    ) -> Result<Vec<(String, u64)>, StoreError> {
        let counts = match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::pending_input_counts(pool, to_i64(now_ms, "pending counts now_ms")?)
                    .await
            }
            EventStore::Postgres(pool) => crate::postgres::pending_input_counts(pool, now_ms).await,
        }?;
        Ok(counts
            .into_iter()
            .map(|(session, count)| (session, count.max(0) as u64))
            .collect())
    }

    /// R9-10: every event of `kinds` across the whole store,
    /// `(session, seq)`-ascending — the ticket board's global fold
    /// input (the board folds ignore every other kind, so deriving
    /// from this read equals deriving from all full streams). `kinds`
    /// is the board's compile-time kind set, never user input.
    pub async fn events_where_kinds(&self, kinds: &[&str]) -> Result<Vec<Event>, StoreError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::events_where_kinds(pool, kinds).await,
            EventStore::Postgres(pool) => crate::postgres::events_where_kinds(pool, kinds).await,
        }
    }

    /// R9-10: the incremental publication — ONE transaction that
    /// deletes the gone sessions' rows, upserts the changed sessions'
    /// rows and flips `view_state` to the digest over the MERGED row
    /// set at the pinned key. Same atomic version-flip guarantee as
    /// [`EventStore::view_publish`] (a crash leaves the previous
    /// complete version or the new one); the caller owns computing the
    /// merged digest, exactly the bytes a full publish of the same set
    /// would record.
    pub async fn view_publish_delta(
        &self,
        view: &str,
        rows: &[ViewRow],
        gone_sessions: &[String],
        digest: &str,
        key: StoreKey,
        built_at: u64,
    ) -> Result<(), StoreError> {
        let payload = crate::views::publish::PublishDelta {
            view,
            rows,
            gone_sessions,
            events_total: to_i64(key.events_total, "view events_total")?,
            max_seq: to_i64(key.max_seq, "view max_seq")?,
            digest,
            built_at: to_i64(built_at, "view built_at")?,
        };
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::view_publish_delta(pool, &payload).await,
            EventStore::Postgres(pool) => crate::postgres::view_publish_delta(pool, &payload).await,
        }
    }
}

/// The cross-backend publication payload (dialect functions bind it).
pub(crate) mod publish {
    use super::ViewRow;

    pub(crate) struct Publish<'a> {
        pub view: &'a str,
        pub rows: &'a [ViewRow],
        pub events_total: i64,
        pub max_seq: i64,
        pub digest: &'a str,
        pub built_at: i64,
    }

    /// The incremental publication payload (R9-10): only the changed
    /// rows are upserted and the gone sessions' rows deleted, in one
    /// transaction with the state flip.
    pub(crate) struct PublishDelta<'a> {
        pub view: &'a str,
        pub rows: &'a [ViewRow],
        pub gone_sessions: &'a [String],
        pub events_total: i64,
        pub max_seq: i64,
        pub digest: &'a str,
        pub built_at: i64,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{StoreKey, VIEW_SCHEMA_VERSION, ViewRow, view_rows_digest, view_rows_digest_in};
    use crate::{AppendGuard, EventStore};

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-views-test-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn ev(session: SessionId, seq: u64, kind: EventKind) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch::GENESIS,
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes: json!({ "n": seq }),
            version: SCHEMA_VERSION,
        }
    }

    async fn seed_one(store: &EventStore, session: SessionId, seq: u64, kind: EventKind) {
        store
            .append(
                session,
                ev(session, seq, kind),
                AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: seq,
                },
            )
            .await
            .expect("seed append");
    }

    fn row(session: &str, version: u64, payload: &str) -> ViewRow {
        ViewRow {
            session: session.into(),
            row_version: version,
            payload: payload.into(),
        }
    }

    /// Publication is one atomic version flip: the rows and the state
    /// land together, a re-publish replaces the previous version, and
    /// the stored digest covers exactly the stored bytes.
    #[tokio::test]
    async fn publication_is_atomic_versioned_and_digest_covered() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        assert!(
            store
                .view_state("session_directory")
                .await
                .expect("state")
                .is_none(),
            "never-built view has no state"
        );

        let a = SessionId::new();
        seed_one(&store, a, 1, EventKind::SessionCreated).await;
        let key = store.view_store_key().await.expect("key");
        let rows = vec![row(&a.0.to_string(), 1, r#"{"events":1}"#)];
        store
            .view_publish("session_directory", rows.clone(), key, 42)
            .await
            .expect("publish");

        let state = store
            .view_state("session_directory")
            .await
            .expect("state")
            .expect("published");
        assert_eq!(state.events_total, key.events_total);
        assert_eq!(state.max_seq, key.max_seq);
        assert_eq!(state.built_at, 42);
        let stored = store
            .view_rows("session_directory")
            .await
            .expect("rows")
            .expect("table exists");
        assert_eq!(stored, rows, "rows read back in session order");
        assert_eq!(state.digest, view_rows_digest("session_directory", &stored));

        // A re-publish with different rows replaces the version whole:
        // old rows gone, state matches the new set (rows read back in
        // session-ascending order — the digest's order).
        let b = SessionId::new();
        let mut rows2 = vec![
            row(&a.0.to_string(), 2, r#"{"events":2}"#),
            row(&b.0.to_string(), 1, r#"{"events":1}"#),
        ];
        rows2.sort_by(|x, y| x.session.cmp(&y.session));
        store
            .view_publish("session_directory", rows2.clone(), key, 43)
            .await
            .expect("republish");
        let stored = store
            .view_rows("session_directory")
            .await
            .expect("rows")
            .expect("table exists");
        assert_eq!(stored, rows2);
        assert_eq!(
            store
                .view_state("session_directory")
                .await
                .expect("state")
                .expect("published")
                .digest,
            view_rows_digest("session_directory", &stored)
        );

        store.close().await;
        cleanup(&path);
    }

    /// The freshness key changes on ANY append — including one to a
    /// short session whose seq never reaches the global MAX (the row
    /// count is the second axis; MAX(seq) alone would miss it).
    #[tokio::test]
    async fn store_key_changes_on_any_append() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let long = SessionId::new();
        let short = SessionId::new();
        for seq in 1..=5 {
            seed_one(&store, long, seq, EventKind::TurnEnd).await;
        }
        let before = store.view_store_key().await.expect("key");
        assert_eq!(before.events_total, 5);
        assert_eq!(before.max_seq, 5);

        // Append to the SHORT session: max_seq stays 5, count moves.
        seed_one(&store, short, 1, EventKind::SessionCreated).await;
        let after = store.view_store_key().await.expect("key");
        assert_eq!(after.max_seq, 5, "global max untouched");
        assert_ne!(before, after, "the key must move on any append");
        assert!(!before.is_current(after));

        store.close().await;
        cleanup(&path);
    }

    /// A tampered row breaks the digest: the recomputed digest over the
    /// read rows disagrees with the stored state — the corruption
    /// signal every view read checks before serving.
    #[tokio::test]
    async fn tampered_rows_break_the_digest() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let a = SessionId::new();
        seed_one(&store, a, 1, EventKind::SessionCreated).await;
        let key = store.view_store_key().await.expect("key");
        store
            .view_publish(
                "session_usage",
                vec![row(&a.0.to_string(), 1, r#"{"input_tokens":1}"#)],
                key,
                1,
            )
            .await
            .expect("publish");

        if let EventStore::Sqlite(pool) = &store {
            sqlx::query("UPDATE view_rows SET payload = ? WHERE view = ? AND session = ?")
                .bind(r#"{"input_tokens":999}"#)
                .bind("session_usage")
                .bind(a.0.to_string())
                .execute(pool)
                .await
                .expect("tamper");
        }
        let state = store
            .view_state("session_usage")
            .await
            .expect("state")
            .expect("published");
        let rows = store
            .view_rows("session_usage")
            .await
            .expect("rows")
            .expect("table exists");
        assert_ne!(
            state.digest,
            view_rows_digest("session_usage", &rows),
            "a tampered row must fail the corruption check"
        );

        // view_drop discards it (the discard-never-guess rule's hammer).
        store.view_drop("session_usage").await.expect("drop");
        assert!(
            store
                .view_state("session_usage")
                .await
                .expect("state")
                .is_none()
        );

        store.close().await;
        cleanup(&path);
    }

    /// The digest domain is (view, row-payload schema version, rows):
    /// two views never share a digest, and a schema-version bump moves
    /// every digest even over byte-identical rows — the invalidation
    /// that makes a changed payload shape un-reusable.
    #[test]
    fn the_digest_domain_carries_the_view_and_the_schema_version() {
        let rows = vec![
            row("a", 1, r#"{"events":1}"#),
            row("b", 3, r#"{"events":7}"#),
        ];
        assert_eq!(
            view_rows_digest("session_usage", &rows),
            view_rows_digest_in("session_usage", &rows, VIEW_SCHEMA_VERSION),
            "the public digest is the current schema version's digest"
        );
        assert_ne!(
            view_rows_digest("session_usage", &rows),
            view_rows_digest("session_directory", &rows),
            "the view name is part of the domain"
        );
        assert_ne!(
            view_rows_digest_in("session_usage", &rows, VIEW_SCHEMA_VERSION),
            view_rows_digest_in("session_usage", &rows, VIEW_SCHEMA_VERSION + 1),
            "a schema bump invalidates the stored bytes"
        );
        assert_ne!(
            view_rows_digest("session_usage", &rows),
            view_rows_digest(
                "session_usage",
                &[
                    row("a", 1, r#"{"events":1}"#),
                    row("b", 3, r#"{"events":8}"#)
                ]
            ),
            "the payload is part of the domain"
        );
    }

    /// A read-only store last migrated before v4 answers "no view"
    /// (`Ok(None)`) — the honest fallback input, never an error.
    #[tokio::test]
    async fn absent_tables_read_as_no_view() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        // Simulate a pre-v4 database by dropping the v4 tables behind
        // the API's back (a read-only reader of an old store sees
        // exactly this shape).
        if let EventStore::Sqlite(pool) = &store {
            sqlx::query("DROP TABLE view_rows")
                .execute(pool)
                .await
                .expect("drop rows");
            sqlx::query("DROP TABLE view_state")
                .execute(pool)
                .await
                .expect("drop state");
        }
        assert!(
            store
                .view_state("session_directory")
                .await
                .expect("no error")
                .is_none()
        );
        assert!(
            store
                .view_rows("session_directory")
                .await
                .expect("no error")
                .is_none()
        );
        // The freshness key still works (v0 events table).
        let key = store.view_store_key().await.expect("key");
        assert_eq!(
            key,
            StoreKey {
                events_total: 0,
                max_seq: 0
            }
        );

        store.close().await;
        cleanup(&path);
    }
}
