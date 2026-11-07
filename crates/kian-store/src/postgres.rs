//! The postgres backend: mirrors the sqlite statements with `$N`
//! placeholders and pg dialect, sharing the typed error surface
//! ([`StoreError`]/[`InboxError`]) and the HEAD-query CAS classification
//! path. pg ≥ 14 assumed.
//!
//! pg specifics pinned by this module (DESIGN §3.3 notes):
//! - every parameter carries an explicit cast (`$1::text`, `$2::bigint`):
//!   pg's parameter-type inference is context-driven and a bare `$N - 1`
//!   can infer `integer`, which would reject an int8 bind;
//! - `GREATEST(...)` for the scalar claim-floor max (pg `MAX` is
//!   aggregate-only; sqlite's two-argument `MAX` is scalar);
//! - `append` rides `ON CONFLICT (session, seq) DO NOTHING`: postgres has
//!   no sqlite-style writer serialization, so two concurrent same-session
//!   CAS appends can both pass the WHERE snapshot and one then hits the
//!   primary key — the conflict becomes zero rows and flows through the
//!   *same* HEAD re-read classification (`StaleEpoch` vs `SeqMismatch`),
//!   never a leaked unique-violation error;
//! - `append_batch` is ONE CTE statement (`WITH guarded AS … INSERT …
//!   SELECT … CROSS JOIN guarded`): all rows land or none — atomicity by
//!   statement, stronger than sqlite's transaction. A PK race raises
//!   sqlstate 23505, which is caught and classified through the same
//!   HEAD path.

use std::ops::Range;

use kian_core::events::Event;
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use kian_core::state::{SessionState, project};
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::event_store::{AppendGuard, Head, StoreError, classify_cas_rejection, to_i64 as se_i64};
use crate::inbox::{AdmitOutcome, InboxError, SessionInbox, decode_input, to_i64 as ie_i64};

// -- events -----------------------------------------------------------------

/// The single-statement CAS append, pg shape (constraint 4 holds as one
/// statement on both backends — no transaction needed on either). The
/// `ON CONFLICT DO NOTHING` arm exists purely for the PK race: a fence
/// snapshot taken before a concurrent writer's commit turns a would-be
/// unique violation into zero rows, which the shared HEAD re-read then
/// classifies exactly like sqlite does.
const APPEND_SQL: &str = "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope)\nSELECT $1::text, $2::bigint, $3::bigint, $4::bigint, $5::text, $6::bigint, $7::text\nWHERE (SELECT COALESCE(MAX(epoch), 0) FROM events WHERE session = $1::text) <= $3::bigint\n  AND (SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = $1::text) = $2::bigint - 1\nON CONFLICT (session, seq) DO NOTHING";

/// The batch append as ONE statement: `guarded` is non-empty only when
/// both fences hold at snapshot time; the INSERT selects the batch rows
/// cross-joined against it, so either every row lands or none does. No
/// `ON CONFLICT` here — skipping only the racing row would commit the
/// tail without its head; a genuine race raises 23505 and is classified.
const BATCH_SQL: &str = "WITH guarded AS (\n    SELECT 1\n    WHERE (SELECT COALESCE(MAX(epoch), 0) FROM events WHERE session = $1::text) <= $2::bigint\n      AND (SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = $1::text) = $3::bigint - 1\n), batch AS (\n    SELECT * FROM unnest($4::text[], $5::bigint[], $6::bigint[], $7::bigint[], $8::text[], $9::bigint[], $10::text[])\n        AS u(session, seq, epoch, ts, kind, version, envelope)\n)\nINSERT INTO events (session, seq, epoch, ts, kind, version, envelope)\nSELECT b.session, b.seq, b.epoch, b.ts, b.kind, b.version, b.envelope\nFROM batch b CROSS JOIN guarded g";

const HEAD_SQL: &str =
    "SELECT COALESCE(MAX(epoch), 0), COALESCE(MAX(seq), 0) FROM events WHERE session = $1::text";

const SCAN_SQL: &str = "SELECT version, envelope FROM events WHERE session = $1::text AND seq >= $2::bigint AND seq < $3::bigint ORDER BY seq ASC";

/// Postgres sqlstate for a unique-constraint violation — the observable
/// form of the concurrent same-session CAS race this backend must
/// classify rather than leak.
fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

/// The same HEAD re-read classification sqlite uses: fence refusal and
/// PK race alike land here and are told apart by the committed head.
async fn classify(pool: &PgPool, session: SessionId, guard: AppendGuard) -> Result<(), StoreError> {
    let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await?;
    Err(classify_cas_rejection(session, guard, epoch_floor, max_seq))
}

pub(crate) async fn append(
    pool: &PgPool,
    session: SessionId,
    event: &Event,
    envelope: &str,
    guard: AppendGuard,
) -> Result<(), StoreError> {
    let result = sqlx::query(APPEND_SQL)
        .bind(session.0.to_string())
        .bind(se_i64(event.seq, "seq")?)
        .bind(se_i64(event.epoch.0, "epoch")?)
        .bind(se_i64(event.ts, "ts")?)
        .bind(event.kind.as_str())
        .bind(i64::from(event.version))
        .bind(envelope)
        .execute(pool)
        .await?;
    if result.rows_affected() == 1 {
        return Ok(());
    }
    // Zero rows: either fence refused the write, or a concurrent writer
    // won the same (session, seq) after this statement's snapshot. Both
    // classify through the committed head — never a raw error.
    classify(pool, session, guard).await
}

pub(crate) async fn append_batch(
    pool: &PgPool,
    session: SessionId,
    events: &[Event],
    envelopes: &[String],
    guard: AppendGuard,
) -> Result<(), StoreError> {
    let first = &events[0];
    let sessions = vec![session.0.to_string(); events.len()];
    let seqs = events
        .iter()
        .map(|e| se_i64(e.seq, "seq"))
        .collect::<Result<Vec<_>, _>>()?;
    let epochs = events
        .iter()
        .map(|e| se_i64(e.epoch.0, "epoch"))
        .collect::<Result<Vec<_>, _>>()?;
    let ts = events
        .iter()
        .map(|e| se_i64(e.ts, "ts"))
        .collect::<Result<Vec<_>, _>>()?;
    let kinds = events.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>();
    let versions = events
        .iter()
        .map(|e| i64::from(e.version))
        .collect::<Vec<_>>();
    let result = sqlx::query(BATCH_SQL)
        .bind(session.0.to_string())
        .bind(se_i64(first.epoch.0, "epoch")?)
        .bind(se_i64(first.seq, "seq")?)
        .bind(sessions)
        .bind(seqs)
        .bind(epochs)
        .bind(ts)
        .bind(kinds)
        .bind(versions)
        .bind(envelopes)
        .execute(pool)
        .await;
    match result {
        // All rows landed (the CTE is all-or-nothing by construction).
        Ok(res) if res.rows_affected() == events.len() as u64 => Ok(()),
        // Fence refused: `guarded` was empty, zero rows inserted.
        Ok(_) => classify(pool, session, guard).await,
        // The PK race: a concurrent writer committed this statement's
        // first seq after its snapshot was taken. Statement atomicity
        // guarantees nothing landed; classify through the head instead of
        // leaking the unique violation.
        Err(e) if is_unique_violation(&e) => classify(pool, session, guard).await,
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn scan(
    pool: &PgPool,
    session: SessionId,
    range: Range<u64>,
) -> Result<Vec<Event>, StoreError> {
    // The same i64 clamp as sqlite: "everything" is 0..i64::MAX.
    let end = range.end.min(i64::MAX as u64);
    let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_SQL)
        .bind(session.0.to_string())
        .bind(se_i64(range.start, "seq")?)
        .bind(se_i64(end, "seq")?)
        .fetch_all(pool)
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

pub(crate) async fn head(pool: &PgPool, session: SessionId) -> Result<Head, StoreError> {
    let (epoch, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await?;
    Ok(Head {
        next_seq: max_seq as u64 + 1,
        epoch: Epoch(epoch as u64),
    })
}

// -- inbox ------------------------------------------------------------------

/// Same shape as sqlite's ADMIT (NULL keys never collide: pg unique
/// indexes treat NULLs as distinct, exactly like sqlite). The admission
/// ordinal (`admitted_seq`, v2) is the identity column's sequence — the
/// database assigns it, so the take queries' same-millisecond
/// tie-break is the admission order, never the random uuid.
const ADMIT_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key)\nVALUES ($1::text, $2::text, $3::text, $4::bigint, $5::bigint, $6::text)\nON CONFLICT (session, idempotency_key) DO NOTHING";

const INPUT_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = $1::text AND idempotency_key = $2::text";

const ELIGIBLE_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = $1::text AND consumed_by_seq IS NULL AND eligible_at <= $2::bigint ORDER BY eligible_at ASC, admitted_seq ASC";

const STARTUP_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE consumed_by_seq IS NULL AND eligible_at <= $1::bigint ORDER BY eligible_at ASC, admitted_seq ASC";

/// Dialect-identical to sqlite's CONSUME modulo placeholders (pg reuses
/// `$1`/`$3` positionally, so each distinct parameter binds once).
const CONSUME_SQL: &str = "UPDATE inputs SET consumed_by_seq = $3::bigint\nWHERE session = $1::text AND id = $2::text\n  AND consumed_by_seq IS NULL\n  AND EXISTS (SELECT 1 FROM events WHERE session = $1::text AND seq = $3::bigint)";

const EVENT_EXISTS_SQL: &str =
    "SELECT COUNT(*) FROM events WHERE session = $1::text AND seq = $2::bigint";

const CONSUMED_STATE_SQL: &str =
    "SELECT consumed_by_seq FROM inputs WHERE session = $1::text AND id = $2::text";

/// Same conditional admission as sqlite's CLAIM (ADR-009), with
/// `GREATEST` for the scalar floor max and `$N` placeholders (one bind
/// per distinct parameter). The DO UPDATE predicate qualifies its
/// columns with the table name: unlike sqlite, postgres resolves
/// unqualified names there against both the target row and `excluded`.
/// `RETURNING` yields zero rows when the UPDATE refuses — the
/// fresh-lease-different-holder case.
const CLAIM_SQL: &str = "INSERT INTO claims (session, epoch, holder, ttl_ms, last_heartbeat)\nVALUES ($1::text,\n        GREATEST(COALESCE((SELECT MAX(epoch) FROM claims WHERE session = $1::text), 0),\n                 COALESCE((SELECT MAX(epoch) FROM events WHERE session = $1::text), 0)) + 1,\n        $2::text, $3::bigint, $4::bigint)\nON CONFLICT(session) DO UPDATE SET\n    epoch = excluded.epoch,\n    holder = excluded.holder,\n    ttl_ms = excluded.ttl_ms,\n    last_heartbeat = excluded.last_heartbeat\nWHERE ($4::bigint - claims.ttl_ms) > claims.last_heartbeat\n   OR claims.holder = $2::text\nRETURNING epoch";

const CLAIM_ROW_SQL: &str =
    "SELECT epoch, holder, ttl_ms, last_heartbeat FROM claims WHERE session = $1::text";

const HEARTBEAT_SQL: &str =
    "UPDATE claims SET last_heartbeat = $2::bigint WHERE session = $1::text";

const EXPIRED_SQL: &str = "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims WHERE ($1::bigint - ttl_ms) > last_heartbeat ORDER BY session ASC";

const ALL_LEASES_SQL: &str =
    "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims ORDER BY epoch ASC";

type InputCols = (String, String, String, i64, i64, Option<String>);

pub(crate) async fn admit(pool: &PgPool, input: InputRow) -> Result<AdmitOutcome, InboxError> {
    let payload = serde_json::to_string(&input.payload)?;
    let session_key = input.session.0.to_string();
    let result = sqlx::query(ADMIT_SQL)
        .bind(input.id.to_string())
        .bind(&session_key)
        .bind(&payload)
        .bind(ie_i64(input.admitted_at, "admitted_at")?)
        .bind(ie_i64(input.eligible_at, "eligible_at")?)
        .bind(input.idempotency_key.as_deref())
        .execute(pool)
        .await?;
    if result.rows_affected() == 1 {
        return Ok(AdmitOutcome {
            input,
            deduplicated: false,
        });
    }
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
        .fetch_optional(pool)
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

pub(crate) async fn eligible_inputs(
    pool: &PgPool,
    session: SessionId,
    now_ms: u64,
) -> Result<Vec<InputRow>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(ELIGIBLE_SQL)
        .bind(session.0.to_string())
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(decode_input).collect()
}

pub(crate) async fn mark_consumed(
    pool: &PgPool,
    session: SessionId,
    input_id: Uuid,
    consumed_by_seq: u64,
) -> Result<(), InboxError> {
    let session_key = session.0.to_string();
    let seq = ie_i64(consumed_by_seq, "consumed_by_seq")?;
    let result = sqlx::query(CONSUME_SQL)
        .bind(&session_key)
        .bind(input_id.to_string())
        .bind(seq)
        .execute(pool)
        .await?;
    if result.rows_affected() == 1 {
        return Ok(());
    }
    let committed: i64 = sqlx::query_scalar(EVENT_EXISTS_SQL)
        .bind(&session_key)
        .bind(seq)
        .fetch_one(pool)
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
        .fetch_optional(pool)
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
            reason: "guarded consume refused on an unconsumed row with a committed event".into(),
        }),
    }
}

pub(crate) async fn claim(
    pool: &PgPool,
    session: SessionId,
    holder: &str,
    ttl_ms: u64,
    now_ms: u64,
) -> Result<LeaseRecord, InboxError> {
    let session_key = session.0.to_string();
    let issued: Option<i64> = sqlx::query_scalar(CLAIM_SQL)
        .bind(&session_key)
        .bind(holder)
        .bind(ie_i64(ttl_ms, "ttl_ms")?)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_optional(pool)
        .await?;
    let Some(epoch) = issued else {
        let live: Option<(i64, String, i64, i64)> = sqlx::query_as(CLAIM_ROW_SQL)
            .bind(&session_key)
            .fetch_optional(pool)
            .await?;
        let Some((epoch, live_holder, live_ttl, last_heartbeat)) = live else {
            return Err(InboxError::CorruptRow {
                session,
                input_id: Uuid::nil(),
                reason: "conditional claim refused but no claims row exists".into(),
            });
        };
        return Err(InboxError::ClaimRejected {
            session,
            holder: live_holder,
            epoch: Epoch(u64::try_from(epoch).map_err(|_| InboxError::CorruptRow {
                session,
                input_id: Uuid::nil(),
                reason: format!("claims table returned a negative epoch ({epoch})"),
            })?),
            expires_at_ms: u64::try_from(last_heartbeat)
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(live_ttl).unwrap_or(u64::MAX)),
        });
    };
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

pub(crate) async fn heartbeat(
    pool: &PgPool,
    session: SessionId,
    now_ms: u64,
) -> Result<(), InboxError> {
    let result = sqlx::query(HEARTBEAT_SQL)
        .bind(session.0.to_string())
        .bind(ie_i64(now_ms, "now_ms")?)
        .execute(pool)
        .await?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(InboxError::NoLease { session })
    }
}

pub(crate) async fn expired_leases(
    pool: &PgPool,
    now_ms: u64,
) -> Result<Vec<LeaseRecord>, InboxError> {
    let rows: Vec<(String, i64, String, i64, i64)> = sqlx::query_as(EXPIRED_SQL)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_all(pool)
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
        let events = scan(pool, sid, 0..i64::MAX as u64).await?;
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

pub(crate) async fn leases(pool: &PgPool) -> Result<Vec<LeaseRecord>, InboxError> {
    let rows: Vec<(String, i64, String, i64, i64)> =
        sqlx::query_as(ALL_LEASES_SQL).fetch_all(pool).await?;
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

pub(crate) async fn startup_scan(
    pool: &PgPool,
    now_ms: u64,
) -> Result<Vec<SessionInbox>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(STARTUP_SQL)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_all(pool)
        .await?;
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
