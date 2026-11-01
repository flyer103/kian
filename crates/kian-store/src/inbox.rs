//! Inbox, claim/lease records, replay source, startup scan (card C6).
//!
//! Inputs are durable rows before any wake fires (AGENTS.md constraint 6);
//! admit is idempotent by key; claims issue monotonic epochs with
//! lease TTL + heartbeat as *liveness* only — lease expiry is never
//! permission to accept stale writes (ADR-005).
//!
//! Consumption is event-driven: an input stops being eligible only when
//! [`EventStore::mark_consumed`] ties it to an event that is *already
//! committed* in the session's stream. There is no operation that takes
//! eligibility away while destroying the row — the inbox row itself is
//! durable and audit-visible forever.
//!
//! The store keeps zero background tasks: every time-sensitive query
//! takes `now_ms` from the caller (clocks, timers and wake registries
//! belong to the orchestrator, cards C9+).

use kian_core::events::Event;
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use kian_core::state::{ProjectionError, SessionState, project};
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

/// Typed inbox/lease failures (mirrors the C5 discipline: a rejected
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
        source: ProjectionError,
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

fn to_i64(v: u64, field: &'static str) -> Result<i64, InboxError> {
    i64::try_from(v).map_err(|_| InboxError::IntOverflow { field })
}

/// Durable admit keyed by `(session, idempotency_key)`: the unique
/// index makes the insert a no-op on a key collision; NULL keys never
/// collide, so keyless admits always insert.
const ADMIT_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key)\nVALUES (?1, ?2, ?3, ?4, ?5, ?6)\nON CONFLICT (session, idempotency_key) DO NOTHING";

const INPUT_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = ?1 AND idempotency_key = ?2";

const ELIGIBLE_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = ?1 AND consumed_by_seq IS NULL AND eligible_at <= ?2 ORDER BY eligible_at ASC, id ASC";

const STARTUP_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE consumed_by_seq IS NULL AND eligible_at <= ?1 ORDER BY eligible_at ASC, id ASC";

/// Consumption is event-driven by construction: the row is touched only
/// when the referenced event row already exists in the same session's
/// stream. The input row is never removed.
const CONSUME_SQL: &str = "UPDATE inputs SET consumed_by_seq = ?3\nWHERE session = ?1 AND id = ?2\n  AND consumed_by_seq IS NULL\n  AND EXISTS (SELECT 1 FROM events WHERE session = ?1 AND seq = ?3)";

const EVENT_EXISTS_SQL: &str = "SELECT COUNT(*) FROM events WHERE session = ?1 AND seq = ?2";

const CONSUMED_STATE_SQL: &str =
    "SELECT consumed_by_seq FROM inputs WHERE session = ?1 AND id = ?2";

/// Monotonic epoch issuance in one statement (sqlite serializes
/// writers, so concurrent claims execute this atomically in order).
/// The floor is max(claims.epoch, events epoch floor): the claims row
/// is the durable high-water mark across restarts, and the events floor
/// closes the claim-issued-but-not-yet-appended window C5 flagged.
/// In the conflict branch `excluded.epoch` was computed against the
/// pre-statement claims row, so it is strictly greater than the stored
/// epoch — epochs never repeat.
const CLAIM_SQL: &str = "INSERT INTO claims (session, epoch, holder, ttl_ms, last_heartbeat)\nVALUES (?1,\n        MAX(COALESCE((SELECT MAX(epoch) FROM claims WHERE session = ?2), 0),\n             COALESCE((SELECT MAX(epoch) FROM events WHERE session = ?3), 0)) + 1,\n        ?4, ?5, ?6)\nON CONFLICT(session) DO UPDATE SET\n    epoch = excluded.epoch,\n    holder = excluded.holder,\n    ttl_ms = excluded.ttl_ms,\n    last_heartbeat = excluded.last_heartbeat\nRETURNING epoch";

const HEARTBEAT_SQL: &str = "UPDATE claims SET last_heartbeat = ?2 WHERE session = ?1";

/// Expired = `now - ttl > last_heartbeat`, written as a subtraction so
/// neither side of the comparison can overflow i64.
const EXPIRED_SQL: &str = "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims WHERE (?1 - ttl_ms) > last_heartbeat ORDER BY session ASC";

/// Every claims row (C14 `lease inspect`): no filtering, epoch order.
const ALL_LEASES_SQL: &str =
    "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims ORDER BY epoch ASC";

type InputCols = (String, String, String, i64, i64, Option<String>);

/// Decodes one stored inbox row. Any decode failure is a typed
/// [`InboxError::CorruptRow`] — never a panic, never silently skipped.
fn decode_input(row: InputCols) -> Result<InputRow, InboxError> {
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
        let payload = serde_json::to_string(&input.payload)?;
        let session_key = input.session.0.to_string();
        let result = sqlx::query(ADMIT_SQL)
            .bind(input.id.to_string())
            .bind(&session_key)
            .bind(&payload)
            .bind(to_i64(input.admitted_at, "admitted_at")?)
            .bind(to_i64(input.eligible_at, "eligible_at")?)
            .bind(input.idempotency_key.as_deref())
            .execute(self.pool())
            .await?;
        if result.rows_affected() == 1 {
            return Ok(AdmitOutcome {
                input,
                deduplicated: false,
            });
        }
        // Zero rows: the (session, idempotency_key) index already held a
        // row. Read it back — first write wins.
        let Some(key) = input.idempotency_key.clone() else {
            return Err(InboxError::CorruptRow {
                session: input.session,
                input_id: input.id,
                reason: "insert skipped without an idempotency key".into(),
            });
        };
        let existing: Option<InputCols> = sqlx::query_as(INPUT_BY_KEY_SQL)
            .bind(&session_key)
            .bind(&key)
            .fetch_optional(self.pool())
            .await?;
        existing
            .map(decode_input)
            .transpose()?
            .map(|input| AdmitOutcome {
                input,
                deduplicated: true,
            })
            .ok_or(InboxError::CorruptRow {
                session: SessionId(Uuid::parse_str(&session_key).unwrap_or_else(|_| Uuid::nil())),
                input_id: Uuid::nil(),
                reason: "unique index reported a collision but no row was found".into(),
            })
    }

    /// The session's inputs that are eligible (`eligible_at <= now_ms`)
    /// and not yet consumed, oldest first. Pure read — eligibility is
    /// never "taken" here.
    pub async fn eligible_inputs(
        &self,
        session: SessionId,
        now_ms: u64,
    ) -> Result<Vec<InputRow>, InboxError> {
        let rows: Vec<InputCols> = sqlx::query_as(ELIGIBLE_SQL)
            .bind(session.0.to_string())
            .bind(to_i64(now_ms, "now_ms")?)
            .fetch_all(self.pool())
            .await?;
        rows.into_iter().map(decode_input).collect()
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
        let session_key = session.0.to_string();
        let seq = to_i64(consumed_by_seq, "consumed_by_seq")?;
        let result = sqlx::query(CONSUME_SQL)
            .bind(&session_key)
            .bind(input_id.to_string())
            .bind(seq)
            .execute(self.pool())
            .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        // The guarded write refused: classify which condition failed.
        let committed: i64 = sqlx::query_scalar(EVENT_EXISTS_SQL)
            .bind(&session_key)
            .bind(seq)
            .fetch_one(self.pool())
            .await?;
        if committed == 0 {
            return Err(InboxError::MissingConsumptionEvent {
                session,
                input_id,
                seq: consumed_by_seq,
            });
        }
        let state: Option<Option<i64>> = sqlx::query_scalar(CONSUMED_STATE_SQL)
            .bind(&session_key)
            .bind(input_id.to_string())
            .fetch_optional(self.pool())
            .await?;
        match state {
            None => Err(InboxError::UnknownInput { session, input_id }),
            Some(Some(prev)) if prev == seq => Ok(()),
            Some(Some(prev)) => Err(InboxError::AlreadyConsumed {
                session,
                input_id,
                consumed_by_seq: prev as u64,
                seq: consumed_by_seq,
            }),
            Some(None) => Err(InboxError::CorruptRow {
                session,
                input_id,
                reason: "guarded consume refused on an unconsumed row with a committed event"
                    .into(),
            }),
        }
    }

    /// Claims the session: issues the next epoch and (re)writes the
    /// lease. The issued epoch is `max(claims.epoch, events epoch
    /// floor) + 1`, so epochs strictly increase per session across
    /// process restarts (the claims row is a durable high-water mark)
    /// and across the claim-issued-but-not-yet-appended window C5
    /// flagged. Single statement — concurrent claims serialize and get
    /// consecutive epochs. Liveness only (ADR-005).
    pub async fn claim(
        &self,
        session: SessionId,
        holder: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<LeaseRecord, InboxError> {
        let session_key = session.0.to_string();
        let epoch: i64 = sqlx::query_scalar(CLAIM_SQL)
            .bind(&session_key)
            .bind(&session_key)
            .bind(&session_key)
            .bind(holder)
            .bind(to_i64(ttl_ms, "ttl_ms")?)
            .bind(to_i64(now_ms, "now_ms")?)
            .fetch_one(self.pool())
            .await?;
        Ok(LeaseRecord {
            session,
            epoch: Epoch(u64::try_from(epoch).map_err(|_| InboxError::CorruptRow {
                session,
                input_id: Uuid::nil(),
                reason: format!("claims table returned a negative epoch ({epoch})"),
            })?),
            holder: holder.to_string(),
            ttl_ms,
            last_heartbeat: now_ms,
        })
    }

    /// Extends the lease (liveness only). Typed [`InboxError::NoLease`]
    /// when no claims row exists.
    pub async fn heartbeat(&self, session: SessionId, now_ms: u64) -> Result<(), InboxError> {
        let result = sqlx::query(HEARTBEAT_SQL)
            .bind(session.0.to_string())
            .bind(to_i64(now_ms, "now_ms")?)
            .execute(self.pool())
            .await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(InboxError::NoLease { session })
        }
    }

    /// Leases past their TTL (`now_ms - ttl_ms > last_heartbeat`) whose
    /// session *projects* to `running`. This is the liveness input for
    /// `recoverable` detection — explicitly not a safety mechanism
    /// (ADR-005): expiry never grants permission to accept stale
    /// writes. `running` is derived from the event projection
    /// (constraint 1), never from a column.
    pub async fn expired_leases(&self, now_ms: u64) -> Result<Vec<LeaseRecord>, InboxError> {
        let rows: Vec<(String, i64, String, i64, i64)> = sqlx::query_as(EXPIRED_SQL)
            .bind(to_i64(now_ms, "now_ms")?)
            .fetch_all(self.pool())
            .await?;
        let mut out = Vec::new();
        for (session_key, epoch, holder, ttl_ms, last_heartbeat) in rows {
            let sid = Uuid::parse_str(&session_key)
                .map_err(|e| InboxError::CorruptRow {
                    session: SessionId(Uuid::nil()),
                    input_id: Uuid::nil(),
                    reason: format!("claims row session id: {e}"),
                })
                .map(SessionId)?;
            // State comes from the event projection only (constraint 1):
            // terminal, idle and recoverable sessions never surface as
            // expired leases.
            let events = self.rebuild(sid).await?;
            let state = project(&events).map_err(|source| InboxError::Projection {
                session: sid,
                source,
            })?;
            if state != SessionState::Running {
                continue;
            }
            out.push(LeaseRecord {
                session: sid,
                epoch: Epoch(epoch as u64),
                holder,
                ttl_ms: ttl_ms as u64,
                last_heartbeat: last_heartbeat as u64,
            });
        }
        Ok(out)
    }

    /// The full-replay source: every event of the session, seq order,
    /// deserialized from the stored envelope bytes.
    pub async fn rebuild(&self, session: SessionId) -> Result<Vec<Event>, InboxError> {
        Ok(self.scan(session, 0..i64::MAX as u64).await?)
    }

    /// Every claims row, ordered by epoch (card C14: the read-only data
    /// source behind `kian lease inspect` — epoch/TTL/holder visibility).
    /// Unlike [`EventStore::expired_leases`] this applies no liveness
    /// filtering: an operational reader sees the lease table as it is,
    /// including leases of sessions that are no longer draining (the
    /// claims row is the durable epoch high-water mark, ADR-005).
    pub async fn leases(&self) -> Result<Vec<LeaseRecord>, InboxError> {
        let rows: Vec<(String, i64, String, i64, i64)> = sqlx::query_as(ALL_LEASES_SQL)
            .fetch_all(self.pool())
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (session_key, epoch, holder, ttl_ms, last_heartbeat) in rows {
            let session = Uuid::parse_str(&session_key)
                .map_err(|e| InboxError::CorruptRow {
                    session: SessionId(Uuid::nil()),
                    input_id: Uuid::nil(),
                    reason: format!("claims row session id: {e}"),
                })
                .map(SessionId)?;
            out.push(LeaseRecord {
                session,
                epoch: Epoch(u64::try_from(epoch).map_err(|_| InboxError::CorruptRow {
                    session,
                    input_id: Uuid::nil(),
                    reason: format!("claims table returned a negative epoch ({epoch})"),
                })?),
                holder,
                ttl_ms: ttl_ms.max(0) as u64,
                last_heartbeat: last_heartbeat.max(0) as u64,
            });
        }
        Ok(out)
    }

    /// The startup inbox scan: every session holding at least one
    /// eligible-but-unconsumed input, with those inputs, ordered by
    /// earliest `eligible_at`. This is the data source the
    /// orchestrator's wake-registry rebuild consumes so NoLostWork
    /// holds across restarts.
    pub async fn startup_scan(&self, now_ms: u64) -> Result<Vec<SessionInbox>, InboxError> {
        let rows: Vec<InputCols> = sqlx::query_as(STARTUP_SQL)
            .bind(to_i64(now_ms, "now_ms")?)
            .fetch_all(self.pool())
            .await?;
        // Rows arrive ordered by (eligible_at, id); sessions may
        // interleave, so group by first-seen session, preserving that
        // order across the groups.
        let mut inboxes: Vec<SessionInbox> = Vec::new();
        for row in rows {
            let input = decode_input(row)?;
            match inboxes.iter_mut().find(|i| i.session == input.session) {
                Some(inbox) => inbox.eligible.push(input),
                None => inboxes.push(SessionInbox {
                    session: input.session,
                    eligible: vec![input],
                }),
            }
        }
        Ok(inboxes)
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
    /// row — first write wins (card C6 acceptance).
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
    /// typed error and the input stays eligible (card C6 acceptance).
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
        // The durable row itself still exists — audit-visible, not deleted.
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
    /// claims row is a durable high-water mark (card C6 acceptance).
    #[tokio::test]
    async fn claim_issues_strictly_increasing_epochs_across_restart() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();

        let e1 = store.claim(sid, "holder-a", 1_000, T0).await.unwrap().epoch;
        let e2 = store.claim(sid, "holder-b", 1_000, T0).await.unwrap().epoch;
        assert!(e2 > e1, "epochs must strictly increase ({e1:?} → {e2:?})");

        // Simulated restart: same file, fresh pool.
        store.close().await;
        let reopened = EventStore::open(&path).await.expect("reopen");
        let e3 = reopened
            .claim(sid, "holder-c", 1_000, T0)
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
            .claim(sid, "holder-d", 1_000, T0)
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
    /// claim-issued-but-not-yet-appended window C5 flagged.
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
    /// distinct epochs — exactly one holder can be current.
    #[tokio::test]
    async fn concurrent_claims_get_consecutive_distinct_epochs() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let a = store.clone();
        let b = store.clone();
        let (ra, rb) = futures::join!(
            async move { a.claim(sid, "h1", 1_000, T0).await },
            async move { b.claim(sid, "h2", 1_000, T0).await }
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

    /// The expired-lease query returns only leases past TTL whose
    /// session projects to `running` — terminal, idle and freshly
    /// heartbeated sessions never surface. Liveness input only
    /// (ADR-005, card C6 acceptance).
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

    /// The startup inbox scan finds every session with
    /// eligible-but-unconsumed inputs in a DB seeded with half-consumed
    /// sessions, and skips future-eligible and fully-consumed ones
    /// (card C6 acceptance; the data source for NoLostWork).
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
    /// liveness filtering — the `kian lease inspect` data source (C14).
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
        let takeover = store.claim(a, "holder-a2", 30_000, T0 + 100).await.unwrap();
        assert_eq!(takeover.epoch, Epoch(2));
        let leases = store.leases().await.unwrap();
        assert_eq!(leases.len(), 2, "claims are keyed by session");
        assert!(leases.iter().any(|l| l.session == a && l.epoch == Epoch(2)));

        store.close().await;
        cleanup(&path);
    }

    /// A read-only reopen sees the same events and leases a writer left
    /// behind (C14's embedded audit reader), and refuses writes.
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

/// Review assertions (card C6): the store owns zero background tasks —
/// no task spaw*ning, no timers, no sleeps. Everything time-dependent
/// takes `now_ms` from the caller.
#[cfg(test)]
mod review {
    use std::fs;
    use std::path::PathBuf;

    /// This module's source must not create tasks, timers or sleeping
    /// constructs: the store is a passive query surface (card C6
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
