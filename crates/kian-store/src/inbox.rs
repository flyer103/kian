//! Inbox, claim/lease records, replay source, startup scan.
//!
//! Inputs are durable rows before any wake fires (AGENTS.md constraint 6);
//! admit is idempotent by key; claims issue monotonic epochs with
//! lease TTL + heartbeat as *liveness* only — lease expiry is never
//! permission to accept stale writes (ADR-005).
//!
//! Consumption is event-driven: an input stops being eligible only when
//! [`EventStore::mark_consumed`] ties it to an event that is *already*
//! committed in the session's stream. There is no operation that takes
//! eligibility away while destroying the row — the inbox row itself is
//! durable and audit-visible in the online store, and remains
//! audit-visible inside the verified archive package after an opt-in
//! retention archive (READ-MODEL §7, P2b; [`crate::archive`]).
//!
//! the inbox surface dispatches over the same
//! [`EventStore`](crate::event_store::EventStore) enum as the event
//! store — identical method names and signatures, per-backend SQL in
//! [`crate::sqlite`] and [`crate::postgres`], shared decode and typed
//! error classification here.
//!
//! The store keeps zero background tasks: every time-sensitive query
//! takes `now_ms` from the caller (clocks, timers and wake registries
//! belong to the orchestrator, +).

use kian_core::events::Event;
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use uuid::Uuid;

use crate::event_store::{EventStore, StoreError};

/// Outcome of an [`EventStore::admit`]: the durable row as it stands
/// after the call, and whether this call deduplicated against an
/// existing row (same idempotency key — first write wins).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitOutcome {
    pub input: InputRow,
    pub deduplicated: bool,
}

/// One session's pending work as seen by the startup inbox scan: every
/// input that is eligible (`eligible_at <= now`) and not yet consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInbox {
    pub session: SessionId,
    pub eligible: Vec<InputRow>,
}

/// Typed inbox/lease failures (mirrors the discipline: a rejected
/// operation is a matchable fact, not an opaque string).
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("heartbeat rejected: no lease row for session {session:?}")]
    NoLease { session: SessionId },
    #[error(
        "cannot consume input {input_id}: no committed event at seq {seq} for session {session:?}"
    )]
    MissingConsumptionEvent {
        session: SessionId,
        input_id: Uuid,
        seq: u64,
    },
    #[error(
        "input {input_id} already consumed by event seq {consumed_by_seq} (attempted seq {seq})"
    )]
    AlreadyConsumed {
        session: SessionId,
        input_id: Uuid,
        consumed_by_seq: u64,
        seq: u64,
    },
    #[error("no input row {input_id} in session {session:?}")]
    UnknownInput { session: SessionId, input_id: Uuid },
    #[error("corrupt inbox row (session {session:?}, input {input_id}): {reason}")]
    CorruptRow {
        session: SessionId,
        input_id: Uuid,
        reason: String,
    },
    #[error("projection of session {session:?} failed while filtering expired leases")]
    Projection {
        session: SessionId,
        source: kian_core::state::ProjectionError,
    },
    /// Conditional claim admission (ADR-009): the claims row holds a
    /// *fresh* lease owned by a *different* holder. Nothing was written —
    /// the store's epoch floor is unchanged — and the live holder's sweep
    /// or wake transport will service the input. A typed fact for the
    /// caller, never a session event (a rejected claim is not session
    /// state, DESIGN §2 additions-only contract).
    #[error(
        "claim rejected: session {session:?} is freshly held by {holder:?} at epoch {epoch:?} until {expires_at_ms} ms"
    )]
    ClaimRejected {
        session: SessionId,
        holder: String,
        epoch: Epoch,
        expires_at_ms: u64,
    },
    #[error("u64 field {field} overflows sqlite's i64")]
    IntOverflow { field: &'static str },
    #[error("sqlite error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("json error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

pub(crate) fn to_i64(v: u64, field: &'static str) -> Result<i64, InboxError> {
    i64::try_from(v).map_err(|_| InboxError::IntOverflow { field })
}

type InputCols = (String, String, String, i64, i64, Option<String>);

/// Decodes one stored inbox row (identical column shape on both
/// backends). Any decode failure is a typed [`InboxError::CorruptRow`] —
/// never a panic, never silently skipped.
pub(crate) fn decode_input(row: InputCols) -> Result<InputRow, InboxError> {
    let (id, session, payload, admitted_at, eligible_at, key) = row;
    let corrupt = |reason: String| {
        let sid = Uuid::parse_str(&session).unwrap_or_else(|_| Uuid::nil());
        let iid = Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil());
        InboxError::CorruptRow {
            session: SessionId(sid),
            input_id: iid,
            reason,
        }
    };
    let id = Uuid::parse_str(&id).map_err(|e| corrupt(format!("input id: {e}")))?;
    let sid = Uuid::parse_str(&session).map_err(|e| corrupt(format!("session id: {e}")))?;
    let u64col = |v: i64, name: &str| {
        u64::try_from(v).map_err(|_| corrupt(format!("{name} column is negative")))
    };
    Ok(InputRow {
        id,
        session: SessionId(sid),
        payload: serde_json::from_str(&payload).map_err(|e| corrupt(format!("payload: {e}")))?,
        admitted_at: u64col(admitted_at, "admitted_at")?,
        eligible_at: u64col(eligible_at, "eligible_at")?,
        idempotency_key: key,
    })
}

impl EventStore {
    /// Durably admits one input row (constraint 6: the row exists before
    /// any wake may fire). Idempotent by `(session, idempotency_key)`:
    /// re-admitting the same key returns the original row with
    /// `deduplicated: true` — first write wins, exactly one row exists.
    /// Rows without a key are always inserted.
    pub async fn admit(&self, input: InputRow) -> Result<AdmitOutcome, InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::admit(pool, input).await,
            EventStore::Postgres(pool) => crate::postgres::admit(pool, input).await,
        }
    }

    /// The session's inputs that are eligible (`eligible_at <= now_ms`)
    /// and not yet consumed, oldest first. Pure read — eligibility is
    /// never "taken" here.
    pub async fn eligible_inputs(
        &self,
        session: SessionId,
        now_ms: u64,
    ) -> Result<Vec<InputRow>, InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::eligible_inputs(pool, session, now_ms).await,
            EventStore::Postgres(pool) => {
                crate::postgres::eligible_inputs(pool, session, now_ms).await
            }
        }
    }

    /// Marks one input consumed *by an already-committed event*: the
    /// write is refused unless `(session, seq)` exists in the events
    /// table, so eligibility can never be destroyed before the
    /// consumption event exists. The row itself is never deleted.
    /// Re-marking with the same seq is idempotent; a different seq is a
    /// typed conflict.
    pub async fn mark_consumed(
        &self,
        session: SessionId,
        input_id: Uuid,
        consumed_by_seq: u64,
    ) -> Result<(), InboxError> {
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::mark_consumed(pool, session, input_id, consumed_by_seq).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::mark_consumed(pool, session, input_id, consumed_by_seq).await
            }
        }
    }

    /// Claims the session: issues the next epoch and (re)writes the
    /// lease. The issued epoch is `max(claims.epoch, events epoch
    /// floor) + 1`, so epochs strictly increase per session across
    /// process restarts (the claims row is a durable high-water mark)
    /// and across the claim-issued-but-not-yet-appended window
    /// flagged. One statement — concurrent claims serialize and get
    /// consecutive epochs. Liveness only (ADR-005).
    ///
    /// (ADR-009) — conditional admission: the claim lands only when
    /// (a) no claims row exists, (b) the row's lease is expired
    /// (`now - ttl > last_heartbeat`), or (c) `holder` already owns the
    /// row (same process re-claiming for its next drain episode — every
    /// single-replica behavior, including back-to-back drains, is
    /// byte-identical to legacy). A fresh lease held by a different
    /// holder is refused with the typed [`InboxError::ClaimRejected`]
    /// carrying the live holder; nothing is written, so the store's
    /// epoch floor is unchanged. This conditions *claiming* only —
    /// write acceptance never consults lease state (ADR-005,
    /// rule 10).
    pub async fn claim(
        &self,
        session: SessionId,
        holder: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<LeaseRecord, InboxError> {
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::claim(pool, session, holder, ttl_ms, now_ms).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::claim(pool, session, holder, ttl_ms, now_ms).await
            }
        }
    }

    /// Extends the lease (liveness only). Typed [`InboxError::NoLease`]
    /// when no claims row exists.
    pub async fn heartbeat(&self, session: SessionId, now_ms: u64) -> Result<(), InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::heartbeat(pool, session, now_ms).await,
            EventStore::Postgres(pool) => crate::postgres::heartbeat(pool, session, now_ms).await,
        }
    }

    /// Leases past their TTL (`now_ms - ttl_ms > last_heartbeat`) whose
    /// session *projects* to `running`. This is the liveness input for
    /// `recoverable` detection — explicitly not a safety mechanism
    /// (ADR-005): expiry never grants permission to accept stale
    /// writes. `running` is derived from the event projection
    /// (constraint 1), never from a column.
    pub async fn expired_leases(&self, now_ms: u64) -> Result<Vec<LeaseRecord>, InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::expired_leases(pool, now_ms).await,
            EventStore::Postgres(pool) => crate::postgres::expired_leases(pool, now_ms).await,
        }
    }

    /// The full-replay source: every event of the session, seq order,
    /// deserialized from the stored envelope bytes.
    pub async fn rebuild(&self, session: SessionId) -> Result<Vec<Event>, InboxError> {
        Ok(self.scan(session, 0..i64::MAX as u64).await?)
    }

    /// Every claims row, ordered by epoch (the read-only data
    /// source behind `kian lease inspect` — epoch/TTL/holder visibility).
    /// Unlike [`EventStore::expired_leases`] this applies no liveness
    /// filtering: an operational reader sees the lease table as it is,
    /// including leases of sessions that are no longer draining (the
    /// claims row is the durable epoch high-water mark, ADR-005).
    pub async fn leases(&self) -> Result<Vec<LeaseRecord>, InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::leases(pool).await,
            EventStore::Postgres(pool) => crate::postgres::leases(pool).await,
        }
    }

    /// The startup inbox scan: every session holding at least one
    /// eligible-but-unconsumed input, with those inputs, ordered by
    /// earliest `eligible_at`. This is the data source the
    /// orchestrator's wake-registry rebuild consumes so NoLostWork
    /// holds across restarts.
    pub async fn startup_scan(&self, now_ms: u64) -> Result<Vec<SessionInbox>, InboxError> {
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::startup_scan(pool, now_ms).await,
            EventStore::Postgres(pool) => crate::postgres::startup_scan(pool, now_ms).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{AdmitOutcome, InboxError, SessionInbox};
    use crate::event_store::{AppendGuard, EventStore};

    const T0: u64 = 1_761_900_000_000;

    // -- helpers -----------------------------------------------------------

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
            ts: T0 + seq,
            kind,
            bytes: json!({ "n": seq }),
            version: SCHEMA_VERSION,
        }
    }

    async fn append_ok(
        store: &EventStore,
        session: SessionId,
        seq: u64,
        epoch: Epoch,
        kind: EventKind,
    ) {
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

    /// Seeds a stream that projects to `running` (created + claimed).
    async fn seed_running(store: &EventStore, session: SessionId, epoch: Epoch) {
        append_ok(store, session, 1, epoch, EventKind::SessionCreated).await;
        append_ok(store, session, 2, epoch, EventKind::DrainClaimed).await;
    }

    /// Seeds a stream that projects to `completed`.
    async fn seed_completed(store: &EventStore, session: SessionId, epoch: Epoch) {
        seed_running(store, session, epoch).await;
        append_ok(
            store,
            session,
            3,
            epoch,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            },
        )
        .await;
    }

    /// Seeds a stream that projects back to `idle` (interrupted end
    /// write releases ownership cleanly).
    async fn seed_interrupted(store: &EventStore, session: SessionId, epoch: Epoch) {
        seed_running(store, session, epoch).await;
        append_ok(
            store,
            session,
            3,
            epoch,
            EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            },
        )
        .await;
    }

    fn input(
        session: SessionId,
        key: Option<&str>,
        admitted_at: u64,
        eligible_at: u64,
        payload: serde_json::Value,
    ) -> InputRow {
        InputRow {
            id: Uuid::new_v4(),
            session,
            payload,
            admitted_at,
            eligible_at,
            idempotency_key: key.map(String::from),
        }
    }

    async fn row_count(store: &EventStore, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(store.pool())
            .await
            .expect("count query")
    }

    // -- admit ---------------------------------------------------------------

    /// Admit idempotency: the same (session, idempotency key) yields
    /// exactly one durable row; the duplicate call returns the original
    /// row — first write wins (acceptance).
    #[tokio::test]
    async fn admit_is_idempotent_by_key() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();

        let first = input(sid, Some("key-1"), T0, T0, json!({ "text": "hello" }));
        let out = store.admit(first.clone()).await.expect("first admit");
        assert_eq!(
            out,
            AdmitOutcome {
                input: first.clone(),
                deduplicated: false
            }
        );

        // Same key, different id and payload: one row, first write wins.
        let dup = InputRow {
            id: Uuid::new_v4(),
            payload: json!({ "text": "changed" }),
            ..first.clone()
        };
        let out = store.admit(dup).await.expect("duplicate admit");
        assert_eq!(
            out,
            AdmitOutcome {
                input: first,
                deduplicated: true
            },
            "duplicate admit must return the original row"
        );

        assert_eq!(row_count(&store, "SELECT COUNT(*) FROM inputs").await, 1);
        store.close().await;
        cleanup(&path);
    }

    /// Without an idempotency key every admit is a new durable row.
    #[tokio::test]
    async fn admit_without_key_always_inserts() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        store
            .admit(input(sid, None, T0, T0, json!(1)))
            .await
            .unwrap();
        store
            .admit(input(sid, None, T0, T0, json!(2)))
            .await
            .unwrap();
        assert_eq!(row_count(&store, "SELECT COUNT(*) FROM inputs").await, 2);
        store.close().await;
        cleanup(&path);
    }

    /// Eligibility gates on `eligible_at` and on event-driven
    /// consumption — a future input and a consumed input are both
    /// invisible; ordering is oldest-first.
    #[tokio::test]
    async fn eligible_inputs_gate_on_eligible_at_and_consumption() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();

        let past = input(sid, Some("a"), T0, T0 - 100, json!("past"));
        let future = input(sid, Some("b"), T0, T0 + 5_000, json!("future"));
        let consumed = input(sid, Some("c"), T0, T0 - 200, json!("consumed"));
        store.admit(past.clone()).await.unwrap();
        store.admit(future).await.unwrap();
        store.admit(consumed.clone()).await.unwrap();

        // Seed a running stream and consume `consumed` via its claimed
        // event (seq 2).
        seed_running(&store, sid, Epoch(1)).await;
        store
            .mark_consumed(sid, consumed.id, 2)
            .await
            .expect("consume");

        let eligible = store.eligible_inputs(sid, T0).await.unwrap();
        assert_eq!(
            eligible,
            vec![past],
            "only the past-eligible unconsumed row"
        );
        store.close().await;
        cleanup(&path);
    }

    // -- consumption is event-driven -------------------------------------------

    /// Taking eligibility never destroys the row before its consumption
    /// event exists: marking against a seq that is not committed is a
    /// typed error and the input stays eligible (acceptance).
    #[tokio::test]
    async fn mark_consumed_requires_committed_consumption_event() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let row = input(sid, Some("k"), T0, T0, json!("work"));
        store.admit(row.clone()).await.unwrap();

        // No events at all yet.
        let err = store.mark_consumed(sid, row.id, 5).await.unwrap_err();
        assert!(
            matches!(
                &err,
                InboxError::MissingConsumptionEvent { seq: 5, input_id, .. } if *input_id == row.id
            ),
            "expected MissingConsumptionEvent, got {err:?}"
        );
        // The row survived and is still eligible.
        assert_eq!(
            store.eligible_inputs(sid, T0).await.unwrap(),
            vec![row.clone()]
        );

        // Commit a real event and consume through it.
        seed_running(&store, sid, Epoch(1)).await; // seq 1, 2
        store
            .mark_consumed(sid, row.id, 2)
            .await
            .expect("consume via seq 2");
        assert!(store.eligible_inputs(sid, T0).await.unwrap().is_empty());
        // The durable row itself still exists — audit-visible, not
        // deleted (an archived session's rows live on in its verified
        // package, READ-MODEL §7).
        assert_eq!(row_count(&store, "SELECT COUNT(*) FROM inputs").await, 1);
        let consumed_by: Option<i64> =
            sqlx::query_scalar("SELECT consumed_by_seq FROM inputs WHERE id = ?")
                .bind(row.id.to_string())
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(consumed_by, Some(2));
        store.close().await;
        cleanup(&path);
    }

    /// Re-consuming with the same event seq is idempotent; a different
    /// seq is a typed conflict; an unknown input id is a typed error.
    #[tokio::test]
    async fn mark_consumed_is_idempotent_and_rejects_conflicting_seq() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let row = input(sid, Some("k"), T0, T0, json!("work"));
        store.admit(row.clone()).await.unwrap();
        seed_running(&store, sid, Epoch(1)).await; // seqs 1, 2
        append_ok(&store, sid, 3, Epoch(1), EventKind::TurnStart).await;

        store.mark_consumed(sid, row.id, 2).await.expect("consume");
        store
            .mark_consumed(sid, row.id, 2)
            .await
            .expect("re-consume is idempotent");

        let err = store.mark_consumed(sid, row.id, 3).await.unwrap_err();
        assert!(
            matches!(
                &err,
                InboxError::AlreadyConsumed {
                    consumed_by_seq: 2,
                    seq: 3,
                    ..
                }
            ),
            "expected AlreadyConsumed, got {err:?}"
        );

        let err = store
            .mark_consumed(sid, Uuid::new_v4(), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, InboxError::UnknownInput { .. }), "{err:?}");
        store.close().await;
        cleanup(&path);
    }

    // -- claims / leases ------------------------------------------------------

    /// Epochs strictly increase per session, including across a
    /// simulated process restart (close + reopen the DB file) — the
    /// claims row is a durable high-water mark (acceptance).
    /// note: successive claims use *different* holders, so each must
    /// arrive past the previous lease's TTL (expired admission) — the
    /// takeover path — which is exactly how distinct replicas or a
    /// restarted successor take over.
    #[tokio::test]
    async fn claim_issues_strictly_increasing_epochs_across_restart() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let ttl = 1_000u64;

        let e1 = store.claim(sid, "holder-a", ttl, T0).await.unwrap().epoch;
        // Past holder-a's TTL the lease is expired: holder-b takes over.
        let e2 = store
            .claim(sid, "holder-b", ttl, T0 + ttl + 1)
            .await
            .unwrap()
            .epoch;
        assert!(e2 > e1, "epochs must strictly increase ({e1:?} → {e2:?})");

        // Simulated restart: same file, fresh pool.
        store.close().await;
        let reopened = EventStore::open(&path).await.expect("reopen");
        let e3 = reopened
            .claim(sid, "holder-c", ttl, T0 + 2 * ttl + 2)
            .await
            .unwrap()
            .epoch;
        assert!(
            e3 > e2,
            "epoch must keep increasing after restart ({e2:?} → {e3:?})"
        );

        // Restart again with *no* claims usage in between but events
        // committed at a higher epoch: the events floor must also hold.
        append_ok(&reopened, sid, 1, e3, EventKind::SessionCreated).await;
        reopened.close().await;
        let reopened2 = EventStore::open(&path).await.expect("reopen 2");
        let e4 = reopened2
            .claim(sid, "holder-d", ttl, T0 + 3 * ttl + 3)
            .await
            .unwrap()
            .epoch;
        assert!(
            e4 > e3,
            "epoch floor includes committed events ({e3:?} → {e4:?})"
        );
        reopened2.close().await;
        cleanup(&path);
    }

    /// The epoch floor includes committed events: claiming a session
    /// whose stream already carries epoch 7 issues 8 — this closes the
    /// claim-issued-but-not-yet-appended window flagged.
    #[tokio::test]
    async fn claim_epoch_floor_includes_committed_events() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        append_ok(&store, sid, 1, Epoch(7), EventKind::SessionCreated).await;
        append_ok(&store, sid, 2, Epoch(7), EventKind::DrainClaimed).await;

        let lease = store.claim(sid, "holder", 1_000, T0).await.unwrap();
        assert_eq!(lease.epoch, Epoch(8));
        assert_eq!(lease.holder, "holder");
        assert_eq!(lease.ttl_ms, 1_000);
        assert_eq!(lease.last_heartbeat, T0);
        store.close().await;
        cleanup(&path);
    }

    /// Two concurrent claims serialize: both succeed with consecutive,
    /// distinct epochs — exactly one holder can be current. note:
    /// both claims carry the *same* holder (a single process's racing
    /// drain episodes — condition (c)); a cross-holder claim against a
    /// fresh lease is `ClaimRejected` by the conditional admission tests
    /// below, so the serialization/monotonicity property is what this
    /// test pins.
    #[tokio::test]
    async fn concurrent_claims_get_consecutive_distinct_epochs() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let a = store.clone();
        let b = store.clone();
        let (ra, rb) = futures::join!(
            async move { a.claim(sid, "h1", 1_000, T0).await },
            async move { b.claim(sid, "h1", 1_000, T0).await }
        );
        let ea = ra.expect("claim a").epoch;
        let eb = rb.expect("claim b").epoch;
        assert_ne!(ea, eb, "concurrent claims must not share an epoch");
        assert!(
            ea.0.abs_diff(eb.0) == 1,
            "expected consecutive epochs, got {ea:?} and {eb:?}"
        );
        store.close().await;
        cleanup(&path);
    }

    // -- conditional claim admission (ADR-009) --------------------------

    /// Admission condition (a): no claims row exists — the claim lands
    /// and issues epoch 1 (or the events-floor epoch when the stream
    /// already carries higher).
    #[tokio::test]
    async fn claim_of_session_with_no_claims_row_succeeds() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let lease = store.claim(sid, "first", 1_000, T0).await.unwrap();
        assert_eq!(lease.epoch, Epoch(1));
        assert_eq!(lease.holder, "first");

        // No claims row, but events committed at epoch 7: the events
        // floor holds (issuance arithmetic unchanged).
        let other = SessionId::new();
        append_ok(&store, other, 1, Epoch(7), EventKind::SessionCreated).await;
        let lease = store.claim(other, "first", 1_000, T0).await.unwrap();
        assert_eq!(lease.epoch, Epoch(8));
        store.close().await;
        cleanup(&path);
    }

    /// Admission condition (b): an expired lease admits a *different*
    /// holder with the next epoch — the takeover path, unchanged
    /// from legacy (the sweep's semantics rest on it).
    #[tokio::test]
    async fn claim_takeover_of_expired_lease_by_different_holder_issues_next_epoch() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let ttl = 1_000u64;
        let first = store.claim(sid, "zombie", ttl, T0).await.unwrap();
        assert_eq!(first.epoch, Epoch(1));

        // Inside the TTL window a different holder is refused…
        let err = store
            .claim(sid, "recoverer", ttl, T0 + ttl)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, InboxError::ClaimRejected { holder, .. } if holder == "zombie"),
            "expected ClaimRejected from the zombie, got {err:?}"
        );
        // …past it (elapsed ttl+1 > ttl) the takeover lands with epoch+1.
        let takeover = store
            .claim(sid, "recoverer", ttl, T0 + ttl + 1)
            .await
            .unwrap();
        assert_eq!(takeover.epoch, Epoch(2));
        assert_eq!(takeover.holder, "recoverer");
        assert_eq!(takeover.last_heartbeat, T0 + ttl + 1);
        store.close().await;
        cleanup(&path);
    }

    /// Admission refusal: a *fresh* lease held by a *different* holder
    /// is `ClaimRejected` carrying the live holder, its epoch and its
    /// lease expiry — and the store's epoch floor is unchanged (the
    /// refused claim wrote nothing).
    #[tokio::test]
    async fn claim_rejected_while_lease_fresh_and_holder_differs() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let ttl = 1_000u64;
        let live = store.claim(sid, "replica-a", ttl, T0).await.unwrap();
        assert_eq!(live.epoch, Epoch(1));
        // A heartbeat pushes the deadline out: still fresh later.
        store.heartbeat(sid, T0 + 500).await.unwrap();

        let err = store
            .claim(sid, "replica-b", ttl, T0 + 900)
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                InboxError::ClaimRejected {
                    session: s,
                    holder,
                    epoch: Epoch(1),
                    expires_at_ms,
                } if *s == sid && holder == "replica-a" && *expires_at_ms == T0 + 500 + ttl
            ),
            "expected ClaimRejected carrying the live holder, got {err:?}"
        );

        // Nothing was written: the claims row is byte-for-byte the live
        // holder's (epoch floor unchanged).
        assert_eq!(
            store.leases().await.unwrap(),
            vec![LeaseRecord {
                session: sid,
                epoch: Epoch(1),
                holder: "replica-a".into(),
                ttl_ms: ttl,
                last_heartbeat: T0 + 500,
            }]
        );
        store.close().await;
        cleanup(&path);
    }

    /// Admission condition (c): the *same* holder re-claiming a fresh
    /// lease succeeds and still bumps the epoch — the single-replica
    /// back-to-back-drain behavior, byte-identical to legacy.
    #[tokio::test]
    async fn claim_by_same_holder_on_fresh_lease_succeeds_and_bumps_epoch() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let ttl = 1_000u64;
        let first = store
            .claim(sid, "kian-orchestrator", ttl, T0)
            .await
            .unwrap();
        let second = store
            .claim(sid, "kian-orchestrator", ttl, T0 + 100)
            .await
            .unwrap();
        assert_eq!(first.epoch, Epoch(1));
        assert_eq!(
            second.epoch,
            Epoch(2),
            "same-holder claims still bump the epoch"
        );
        assert_eq!(second.last_heartbeat, T0 + 100);
        store.close().await;
        cleanup(&path);
    }

    /// The expired-lease query returns only leases past TTL whose
    /// session projects to `running` — terminal, idle and freshly
    /// heartbeated sessions never surface. Liveness input only
    /// (ADR-005, acceptance).
    #[tokio::test]
    async fn expired_leases_return_only_running_sessions_past_ttl() {
        let (store, path) = test_store().await;
        let ttl = 1_000u64;
        let now = T0 + 2_000;

        // (1) running + expired → included.
        let running = SessionId::new();
        seed_running(&store, running, Epoch(1)).await;
        store.claim(running, "zombie", ttl, T0).await.unwrap();

        // (2) running + heartbeat inside the TTL window → excluded.
        let fresh = SessionId::new();
        seed_running(&store, fresh, Epoch(1)).await;
        store.claim(fresh, "alive", ttl, T0).await.unwrap();
        store.heartbeat(fresh, T0 + 1_500).await.unwrap();

        // (3) completed (terminal) + stale lease → excluded.
        let done = SessionId::new();
        seed_completed(&store, done, Epoch(1)).await;
        store.claim(done, "zombie", ttl, T0).await.unwrap();

        // (4) interrupted back to idle + stale lease → excluded.
        let aborted = SessionId::new();
        seed_interrupted(&store, aborted, Epoch(1)).await;
        store.claim(aborted, "zombie", ttl, T0).await.unwrap();

        // (5) never-claimed running session → excluded (no lease row).
        let nolease = SessionId::new();
        seed_running(&store, nolease, Epoch(1)).await;

        let expired = store.expired_leases(now).await.unwrap();
        assert_eq!(
            expired.len(),
            1,
            "exactly the running+expired lease: {expired:?}"
        );
        assert_eq!(
            expired[0],
            LeaseRecord {
                session: running,
                epoch: Epoch(2), // claim floor = seeded stream epoch 1
                holder: "zombie".into(),
                ttl_ms: ttl,
                last_heartbeat: T0,
            }
        );
        store.close().await;
        cleanup(&path);
    }

    /// Heartbeat extends the lease; a heartbeat with no claims row is a
    /// typed error.
    #[tokio::test]
    async fn heartbeat_extends_lease_and_requires_one() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed_running(&store, sid, Epoch(1)).await;
        store.claim(sid, "holder", 1_000, T0).await.unwrap();

        // Inside the TTL window nothing is expired (elapsed 500 < 1000)...
        assert!(store.expired_leases(T0 + 500).await.unwrap().is_empty());
        // ...past it the lease is expired (elapsed 1500 > 1000)...
        assert_eq!(store.expired_leases(T0 + 1_500).await.unwrap().len(), 1);
        // ...and a heartbeat pushes the deadline out again (elapsed from
        // the new heartbeat: 600 < 1000).
        store.heartbeat(sid, T0 + 1_400).await.unwrap();
        assert!(store.expired_leases(T0 + 2_000).await.unwrap().is_empty());
        assert_eq!(store.expired_leases(T0 + 2_600).await.unwrap().len(), 1);

        // Heartbeat without a claims row: typed NoLease.
        let other = SessionId::new();
        let err = store.heartbeat(other, T0).await.unwrap_err();
        assert!(
            matches!(err, InboxError::NoLease { session } if session == other),
            "{err:?}"
        );
        store.close().await;
        cleanup(&path);
    }

    // -- replay source ---------------------------------------------------------

    /// `rebuild` returns the committed stream in seq order,
    /// byte-identical to `scan` over the full range.
    #[tokio::test]
    async fn rebuild_returns_the_full_stream() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed_completed(&store, sid, Epoch(4)).await;

        let rebuilt = store.rebuild(sid).await.unwrap();
        let scanned = store.scan(sid, 0..i64::MAX as u64).await.unwrap();
        let ser = |evs: &[Event]| {
            evs.iter()
                .map(|e| serde_json::to_string(e).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(ser(&rebuilt), ser(&scanned));
        assert_eq!(rebuilt.len(), 3);
        assert_eq!(rebuilt[0].seq, 1);
        assert_eq!(rebuilt[2].seq, 3);
        store.close().await;
        cleanup(&path);
    }

    // -- startup scan -----------------------------------------------------------

    /// FIFO inside one `eligible_at` millisecond follows admission
    /// order, never the random-uuid tie-break. live-run finding:
    /// the inbox ordered same-ms rows by `id` (a random uuid), so a
    /// task/steer pair admitted in the same millisecond could consume
    /// the steer first — and the worker-template classification (which
    /// reads the *first* input) misrouted the session to the default
    /// agent. The ids here are crafted so the uuid tie-break orders
    /// them backwards: admission order must win.
    #[tokio::test]
    async fn same_millisecond_inputs_consume_in_admission_order() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();

        let first = InputRow {
            id: Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap(),
            session: sid,
            payload: json!({ "n": 1 }),
            admitted_at: T0,
            eligible_at: T0,
            idempotency_key: Some("first".to_owned()),
        };
        let second = InputRow {
            id: Uuid::parse_str("00000000-0000-4000-8000-000000000000").unwrap(),
            session: sid,
            payload: json!({ "n": 2 }),
            admitted_at: T0,
            eligible_at: T0,
            idempotency_key: Some("second".to_owned()),
        };
        store.admit(first.clone()).await.expect("admit first");
        store.admit(second.clone()).await.expect("admit second");

        let rows = store.eligible_inputs(sid, T0).await.expect("eligible");
        assert_eq!(rows.len(), 2, "both same-ms inputs are eligible");
        assert_eq!(
            rows[0].id, first.id,
            "same-ms inputs must consume in admission order (not uuid order)"
        );
        assert_eq!(rows[1].id, second.id);

        // The startup inbox rebuild keeps the same order per session.
        let scan = store.startup_scan(T0).await.expect("startup scan");
        let ours = scan
            .iter()
            .find(|s| s.session == sid)
            .expect("session in startup scan");
        assert_eq!(
            ours.eligible.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![first.id, second.id],
            "startup scan must list same-ms inputs in admission order"
        );

        store.close().await;
        cleanup(&path);
    }

    /// The startup inbox scan finds every session with
    /// eligible-but-unconsumed inputs in a DB seeded with half-consumed
    /// sessions, and skips future-eligible and fully-consumed ones
    /// (acceptance; the data source for NoLostWork).
    #[tokio::test]
    async fn startup_scan_finds_all_eligible_unconsumed_inputs() {
        let (store, path) = test_store().await;

        // A: two eligible unconsumed inputs.
        let a = SessionId::new();
        let a1 = input(a, Some("a1"), T0, T0 - 300, json!("a1"));
        let a2 = input(a, Some("a2"), T0, T0 - 200, json!("a2"));
        store.admit(a1.clone()).await.unwrap();
        store.admit(a2.clone()).await.unwrap();

        // B: half-consumed — one input consumed via a committed event,
        // one still pending.
        let b = SessionId::new();
        let b_done = input(b, Some("bd"), T0, T0 - 400, json!("bd"));
        let b_pend = input(b, Some("bp"), T0, T0 - 100, json!("bp"));
        store.admit(b_done.clone()).await.unwrap();
        store.admit(b_pend.clone()).await.unwrap();
        seed_running(&store, b, Epoch(1)).await;
        store.mark_consumed(b, b_done.id, 2).await.unwrap();

        // C: eligible only in the future.
        let c = SessionId::new();
        store
            .admit(input(c, Some("c"), T0, T0 + 60_000, json!("c")))
            .await
            .unwrap();

        // D: fully consumed.
        let d = SessionId::new();
        let d1 = input(d, Some("d1"), T0, T0 - 500, json!("d1"));
        store.admit(d1.clone()).await.unwrap();
        seed_running(&store, d, Epoch(1)).await;
        store.mark_consumed(d, d1.id, 2).await.unwrap();

        // E: one eligible unconsumed input, newest of the pending set.
        let e = SessionId::new();
        let e1 = input(e, Some("e1"), T0, T0 - 50, json!("e1"));
        store.admit(e1.clone()).await.unwrap();

        let scan = store.startup_scan(T0).await.unwrap();
        // Order: by earliest eligible input — B (-400? no: B's remaining
        // pending input is -100; B's earliest *unconsumed* eligible is
        // bp at -100). Earliest pending per session: A=-300, B=-100,
        // E=-50.
        assert_eq!(
            scan,
            vec![
                SessionInbox {
                    session: a,
                    eligible: vec![a1, a2]
                },
                SessionInbox {
                    session: b,
                    eligible: vec![b_pend]
                },
                SessionInbox {
                    session: e,
                    eligible: vec![e1]
                },
            ],
            "half-consumed and pending sessions surface; future and fully-consumed do not"
        );
        store.close().await;
        cleanup(&path);
    }

    /// The scan is time-parameterized: as the clock crosses an
    /// `eligible_at`, the session surfaces — delayed inputs are never
    /// lost, only not-yet-visible.
    #[tokio::test]
    async fn startup_scan_respects_eligible_at_boundary() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        store
            .admit(input(sid, Some("k"), T0, T0 + 1_000, json!("later")))
            .await
            .unwrap();

        assert!(store.startup_scan(T0).await.unwrap().is_empty());
        assert_eq!(store.startup_scan(T0 + 1_000).await.unwrap().len(), 1);
        store.close().await;
        cleanup(&path);
    }

    /// Admitted inputs survive a restart (durable rows, constraint 6):
    /// the reopened store's startup scan still finds them.
    #[tokio::test]
    async fn startup_scan_survives_restart() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let row = input(sid, Some("k"), T0, T0, json!("persisted"));
        store.admit(row.clone()).await.unwrap();
        store.close().await;

        let reopened = EventStore::open(&path).await.expect("reopen");
        let scan = reopened.startup_scan(T0).await.unwrap();
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].eligible, vec![row]);
        reopened.close().await;
        cleanup(&path);
    }

    /// `leases()` surfaces every claims row with epoch/TTL/holder, no
    /// liveness filtering — the `kian lease inspect` data source.
    /// Rows survive the writer closing: the claims table is the durable
    /// epoch high-water mark.
    #[tokio::test]
    async fn leases_lists_every_claim_with_epoch_ttl_holder() {
        let (store, path) = test_store().await;
        let a = SessionId::new();
        let b = SessionId::new();
        let la = store.claim(a, "holder-a", 30_000, T0).await.unwrap();
        let lb = store.claim(b, "holder-b", 5_000, T0).await.unwrap();
        assert_eq!(la.epoch, Epoch(1));
        assert_eq!(lb.epoch, Epoch(1));

        let leases = store.leases().await.unwrap();
        assert_eq!(leases.len(), 2, "one row per claimed session");
        assert!(leases.contains(&LeaseRecord {
            session: a,
            epoch: Epoch(1),
            holder: "holder-a".into(),
            ttl_ms: 30_000,
            last_heartbeat: T0,
        }));
        assert!(leases.contains(&LeaseRecord {
            session: b,
            epoch: Epoch(1),
            holder: "holder-b".into(),
            ttl_ms: 5_000,
            last_heartbeat: T0,
        }));

        // A takeover claim rewrites the row with the next epoch.
        // note: holder-a2 differs from holder-a, so the claim arrives
        // past the 30 s TTL — the expired-lease takeover path.
        let takeover = store
            .claim(a, "holder-a2", 30_000, T0 + 30_001)
            .await
            .unwrap();
        assert_eq!(takeover.epoch, Epoch(2));
        let leases = store.leases().await.unwrap();
        assert_eq!(leases.len(), 2, "claims are keyed by session");
        assert!(leases.iter().any(|l| l.session == a && l.epoch == Epoch(2)));

        store.close().await;
        cleanup(&path);
    }

    /// A read-only reopen sees the same events and leases a writer left
    /// behind ('s embedded audit reader), and refuses writes.
    #[tokio::test]
    async fn read_only_reopen_reads_stream_and_refuses_writes() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        append_ok(&store, sid, 1, Epoch(0), EventKind::SessionCreated).await;
        store.claim(sid, "w", 30_000, T0).await.unwrap();
        append_ok(&store, sid, 2, Epoch(1), EventKind::InputAdmitted).await;
        store.close().await;

        let reader = EventStore::open_read_only(&path)
            .await
            .expect("read-only open");
        let events = reader.rebuild(sid).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(reader.leases().await.unwrap().len(), 1);

        // The audit reader cannot mutate: an append is rejected by
        // sqlite itself, not by convention.
        let denied = reader
            .append(
                sid,
                ev(sid, 3, Epoch(1), EventKind::TurnStart),
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 3,
                },
            )
            .await;
        assert!(denied.is_err(), "read-only store must refuse appends");
        reader.close().await;
        cleanup(&path);
    }

    /// `open_read_only` never creates a missing file (fail-closed).
    #[tokio::test]
    async fn read_only_open_of_missing_file_is_an_error() {
        let missing = temp_db_path();
        assert!(EventStore::open_read_only(&missing).await.is_err());
        assert!(!missing.exists(), "read-only open must not create the file");
    }
}

/// Review assertions: the store owns zero background tasks —
/// no task spaw*ning, no timers, no sleeps. Everything time-dependent
/// takes `now_ms` from the caller.
#[cfg(test)]
mod review {
    use std::fs;
    use std::path::PathBuf;

    /// This module's source must not create tasks, timers or sleeping
    /// constructs: the store is a passive query surface (
    /// "never touch" list). The forbidden fragments are assembled from
    /// split pieces so this file cannot contain them literally.
    #[test]
    fn no_background_tasks_or_timers_in_inbox_module() {
        let forbidden: [String; 4] = [
            ["tokio::sp", "awn"].concat(),
            ["std::thr", "ead"].concat(),
            ["inter", "val"].concat(),
            ["::sl", "eep"].concat(),
        ];
        let file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/inbox.rs");
        let src = fs::read_to_string(&file).expect("read inbox.rs");
        let lower = src.to_lowercase();
        for pat in &forbidden {
            assert!(
                !lower.contains(pat),
                "background-task violation: {pat:?} found in inbox.rs"
            );
        }
    }
}
