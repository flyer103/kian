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

use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use kian_core::state::{SessionState, project};
use sqlx::postgres::PgPool;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::event_store::{AppendGuard, Head, StoreError, classify_cas_rejection, to_i64 as se_i64};
use crate::inbox::{
    ExpiredLeases, InboxError, SessionInbox, UnreadableLease, decode_input, to_i64 as ie_i64,
};

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

const LAST_EVENT_SQL: &str =
    "SELECT version, envelope FROM events WHERE session = $1::text ORDER BY seq DESC LIMIT 1";

const SESSION_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock($1::bigint)";

/// The seq axis only (the unchanged-read fast path — see `version`).
const VERSION_SQL: &str = "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = $1::text";

const SCAN_SQL: &str = "SELECT version, envelope FROM events WHERE session = $1::text AND seq >= $2::bigint AND seq < $3::bigint ORDER BY seq ASC";

/// The same kind-filtered session read as the sqlite twin (memory
/// P3b-1): served by the v6 `idx_events_session_kind_seq` index on
/// postgres too.
const SCAN_KIND_SQL: &str = "SELECT version, envelope FROM events WHERE session = $1::text AND kind = $2::text ORDER BY seq ASC";

const SCAN_KIND_ACROSS_SESSIONS_SQL: &str = "SELECT version, envelope FROM events WHERE kind = $1::text ORDER BY session ASC, seq ASC LIMIT $2::bigint";

/// The multi-kind session read behind `EventStore::scan_kinds` (the same
/// index, an array key set).
const SCAN_KINDS_SQL: &str =
    "SELECT version, envelope FROM events WHERE session = $1::text AND kind = ANY($2::text[])";

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
    let mut tx = pool.begin().await?;
    append_batch_tx(&mut tx, session, events, envelopes, guard).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn append_batch_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
    events: &[Event],
    envelopes: &[String],
    guard: AppendGuard,
) -> Result<(), StoreError> {
    sqlx::query("SAVEPOINT kian_session_write_batch")
        .execute(&mut **tx)
        .await?;
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
        .execute(&mut **tx)
        .await;
    match result {
        // All rows landed (the CTE is all-or-nothing by construction).
        Ok(res) if res.rows_affected() == events.len() as u64 => {
            sqlx::query("RELEASE SAVEPOINT kian_session_write_batch")
                .execute(&mut **tx)
                .await?;
            Ok(())
        }
        // Fence refused: `guarded` was empty, zero rows inserted.
        Ok(_) => {
            rollback_batch_savepoint(tx).await?;
            let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
                .bind(session.0.to_string())
                .fetch_one(&mut **tx)
                .await?;
            Err(classify_cas_rejection(session, guard, epoch_floor, max_seq))
        }
        // The PK race: a concurrent writer committed this statement's
        // first seq after its snapshot was taken. Statement atomicity
        // guarantees nothing landed; classify through the head instead of
        // leaking the unique violation.
        Err(e) if is_unique_violation(&e) => {
            rollback_batch_savepoint(tx).await?;
            let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
                .bind(session.0.to_string())
                .fetch_one(&mut **tx)
                .await?;
            Err(classify_cas_rejection(session, guard, epoch_floor, max_seq))
        }
        Err(e) => {
            rollback_batch_savepoint(tx).await?;
            Err(e.into())
        }
    }
}

async fn rollback_batch_savepoint(tx: &mut Transaction<'_, Postgres>) -> Result<(), StoreError> {
    sqlx::query("ROLLBACK TO SAVEPOINT kian_session_write_batch")
        .execute(&mut **tx)
        .await?;
    sqlx::query("RELEASE SAVEPOINT kian_session_write_batch")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Transaction-scoped serialization for one session. FNV-1a over the UUID
/// text is stable across processes and Rust releases (unlike `DefaultHasher`).
pub(crate) async fn lock_session_write(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
) -> Result<(), StoreError> {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in session.0.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    sqlx::query(SESSION_LOCK_SQL)
        .bind(hash as i64)
        .execute(&mut **tx)
        .await?;
    Ok(())
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

pub(crate) async fn scan_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
    range: Range<u64>,
) -> Result<Vec<Event>, StoreError> {
    let end = range.end.min(i64::MAX as u64);
    let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_SQL)
        .bind(session.0.to_string())
        .bind(se_i64(range.start, "seq")?)
        .bind(se_i64(end, "seq")?)
        .fetch_all(&mut **tx)
        .await?;
    decode_scan_rows(session, rows)
}

fn decode_scan_rows(
    session: SessionId,
    rows: Vec<(i64, String)>,
) -> Result<Vec<Event>, StoreError> {
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

pub(crate) async fn last_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
) -> Result<Option<Event>, StoreError> {
    let row: Option<(i64, String)> = sqlx::query_as(LAST_EVENT_SQL)
        .bind(session.0.to_string())
        .fetch_optional(&mut **tx)
        .await?;
    row.map(|(_, envelope)| serde_json::from_str(&envelope))
        .transpose()
        .map_err(StoreError::from)
}

/// The kind-filtered session read — the postgres arm of
/// `EventStore::scan_kind` (see the sqlite twin).
pub(crate) async fn scan_kind(
    pool: &PgPool,
    session: SessionId,
    kind: EventKind,
) -> Result<Vec<Event>, StoreError> {
    let kind_key = kind.as_str();
    let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_KIND_SQL)
        .bind(session.0.to_string())
        .bind(kind_key)
        .fetch_all(pool)
        .await?;
    let mut events = Vec::with_capacity(rows.len());
    for (version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col
            || event.session != session
            || event.kind.as_str() != kind_key
        {
            return Err(StoreError::CorruptRow {
                session,
                seq: event.seq,
            });
        }
        // The `kind` column stores `as_str()`, which collapses
        // `SessionEnded`'s four reasons into one key. The decoded kind is
        // the exact value: a row whose full kind differs is not the
        // requested kind (it is not corruption — same kind string) and is
        // skipped, so the result is exactly `scan`-then-`e.kind == kind`.
        if event.kind != kind {
            continue;
        }
        events.push(event);
    }
    Ok(events)
}

pub(crate) async fn scan_kind_across_sessions(
    pool: &PgPool,
    kind: EventKind,
    limit: u64,
) -> Result<Vec<Event>, StoreError> {
    let kind_key = kind.as_str();
    let limit = se_i64(limit, "limit")?;
    let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_KIND_ACROSS_SESSIONS_SQL)
        .bind(kind_key)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    let mut events = Vec::with_capacity(rows.len());
    for (version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col || event.kind.as_str() != kind_key {
            return Err(StoreError::CorruptRow {
                session: event.session,
                seq: event.seq,
            });
        }
        if event.kind != kind {
            continue;
        }
        events.push(event);
    }
    Ok(events)
}

/// The multi-kind session read — the postgres arm of
/// `EventStore::scan_kinds` (see the sqlite twin). One array bind keeps the
/// statement static; `kind = ANY($2)` rides the same `session, kind, seq`
/// index.
pub(crate) async fn scan_kinds(
    pool: &PgPool,
    session: SessionId,
    kinds: &[EventKind],
) -> Result<Vec<Event>, StoreError> {
    let mut keys: Vec<String> = kinds.iter().map(|kind| kind.as_str().to_string()).collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(i64, String)> = sqlx::query_as(SCAN_KINDS_SQL)
        .bind(session.0.to_string())
        .bind(&keys)
        .fetch_all(pool)
        .await?;
    let mut events = Vec::with_capacity(rows.len());
    for (version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col
            || event.session != session
            || !keys.iter().any(|key| key == event.kind.as_str())
        {
            return Err(StoreError::CorruptRow {
                session,
                seq: event.seq,
            });
        }
        events.push(event);
    }
    events.sort_by_key(|event| event.seq);
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

pub(crate) async fn head_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
) -> Result<Head, StoreError> {
    let (epoch, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
        .bind(session.0.to_string())
        .fetch_one(&mut **tx)
        .await?;
    Ok(Head {
        next_seq: max_seq as u64 + 1,
        epoch: Epoch(epoch as u64),
    })
}

/// READ-MODEL §3 (P0): the session's version (`head.next_seq - 1`)
/// alone — the seq axis only, mirroring [`crate::sqlite::version`]
/// (same names, same semantics on both backends).
pub(crate) async fn version(pool: &PgPool, session: SessionId) -> Result<u64, StoreError> {
    let max_seq: i64 = sqlx::query_scalar(VERSION_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await?;
    Ok(max_seq as u64)
}

// -- inbox ------------------------------------------------------------------

/// Same shape as sqlite's ADMIT (NULL keys never collide: pg unique
/// indexes treat NULLs as distinct, exactly like sqlite). The admission
/// ordinal (`admitted_seq`, v2) is the identity column's sequence — the
/// database assigns it, so the take queries' same-millisecond
/// tie-break is the admission order, never the random uuid.
const ADMIT_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key)\nVALUES ($1::text, $2::text, $3::text, $4::bigint, $5::bigint, $6::text)\nON CONFLICT (session, idempotency_key) DO NOTHING";

const INPUT_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = $1::text AND idempotency_key = $2::text";

/// Channel-scoped admit (ADR-081): dialect-identical to sqlite's
/// `ADMIT_CHANNEL_SQL` — refused when a row with this key is already
/// *consumed* in any session or the receipts ledger holds it (ADR-082),
/// one statement, `NOT EXISTS` probes served by `idx_inputs_channel_key`
/// (migration v9) and the receipts primary key (migration v10).
const ADMIT_CHANNEL_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key)\nSELECT $1::text, $2::text, $3::text, $4::bigint, $5::bigint, $6::text\nWHERE NOT EXISTS (SELECT 1 FROM inputs p WHERE p.idempotency_key = $6::text AND p.consumed_by_seq IS NOT NULL)\n  AND NOT EXISTS (SELECT 1 FROM channel_receipts r WHERE r.idempotency_key = $6::text)\nON CONFLICT (session, idempotency_key) DO NOTHING";

/// The suppressing row of a refused channel admit: the consumed row with
/// this key in *any* session (ADR-081 decision 6).
const CONSUMED_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE idempotency_key = $1::text AND consumed_by_seq IS NOT NULL ORDER BY admitted_at ASC LIMIT 1";

/// The suppressing row after the consuming session was archived
/// (ADR-082); column order matches `InputCols`.
const RECEIPT_BY_KEY_SQL: &str = "SELECT input_id, session, payload, admitted_at, eligible_at, idempotency_key FROM channel_receipts WHERE idempotency_key = $1::text";

const ELIGIBLE_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = $1::text AND consumed_by_seq IS NULL AND eligible_at <= $2::bigint ORDER BY eligible_at ASC, admitted_seq ASC";

const HAS_UNCONSUMED_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM inputs WHERE session = $1::text AND consumed_by_seq IS NULL)";

/// (R17 C4, pin P3-5) the read-only per-session inbox read behind the
/// task board's `input_required` arm: EVERY row of the session,
/// consumed rows included (the row is durable and audit-visible after
/// `mark_consumed`). Dialect-identical to sqlite's INPUT_ROWS_SQL.
const INPUT_ROWS_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = $1::text ORDER BY eligible_at ASC, admitted_seq ASC";

const INPUT_ROWS_ACROSS_SESSIONS_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs ORDER BY admitted_at DESC, admitted_seq DESC LIMIT $1::bigint";

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

pub(crate) async fn input_by_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
    key: &str,
) -> Result<Option<InputRow>, InboxError> {
    let row: Option<InputCols> = sqlx::query_as(INPUT_BY_KEY_SQL)
        .bind(session.0.to_string())
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(decode_input).transpose()
}

pub(crate) async fn consumed_input_by_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<Option<InputRow>, InboxError> {
    let row: Option<InputCols> = sqlx::query_as(CONSUMED_BY_KEY_SQL)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(decode_input).transpose()
}

pub(crate) async fn receipt_by_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<Option<InputRow>, InboxError> {
    let row: Option<InputCols> = sqlx::query_as(RECEIPT_BY_KEY_SQL)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(decode_input).transpose()
}

pub(crate) async fn insert_input_tx(
    tx: &mut Transaction<'_, Postgres>,
    input: &InputRow,
    payload: &str,
) -> Result<bool, InboxError> {
    let result = sqlx::query(ADMIT_SQL)
        .bind(input.id.to_string())
        .bind(input.session.0.to_string())
        .bind(payload)
        .bind(ie_i64(input.admitted_at, "admitted_at")?)
        .bind(ie_i64(input.eligible_at, "eligible_at")?)
        .bind(input.idempotency_key.as_deref())
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn insert_channel_input_tx(
    tx: &mut Transaction<'_, Postgres>,
    input: &InputRow,
    payload: &str,
) -> Result<bool, InboxError> {
    let result = sqlx::query(ADMIT_CHANNEL_SQL)
        .bind(input.id.to_string())
        .bind(input.session.0.to_string())
        .bind(payload)
        .bind(ie_i64(input.admitted_at, "admitted_at")?)
        .bind(ie_i64(input.eligible_at, "eligible_at")?)
        .bind(input.idempotency_key.as_deref())
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn has_unconsumed_inputs_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: SessionId,
) -> Result<bool, InboxError> {
    sqlx::query_scalar(HAS_UNCONSUMED_SQL)
        .bind(session.0.to_string())
        .fetch_one(&mut **tx)
        .await
        .map_err(InboxError::from)
}

pub(crate) async fn has_unconsumed_inputs(
    pool: &PgPool,
    session: SessionId,
) -> Result<bool, InboxError> {
    sqlx::query_scalar(HAS_UNCONSUMED_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await
        .map_err(InboxError::from)
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

/// (R17 C4, pin P3-5): every inbox row of the session — read-only,
/// consumed rows included, oldest first. No new table, no migration.
pub(crate) async fn input_rows(
    pool: &PgPool,
    session: SessionId,
) -> Result<Vec<InputRow>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(INPUT_ROWS_SQL)
        .bind(session.0.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(decode_input).collect()
}

pub(crate) async fn input_rows_across_sessions(
    pool: &PgPool,
    limit: u64,
) -> Result<Vec<InputRow>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(INPUT_ROWS_ACROSS_SESSIONS_SQL)
        .bind(ie_i64(limit, "limit")?)
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

pub(crate) async fn claim_tx(
    tx: &mut Transaction<'_, Postgres>,
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
        .fetch_optional(&mut **tx)
        .await?;
    let Some(epoch) = issued else {
        let live: Option<(i64, String, i64, i64)> = sqlx::query_as(CLAIM_ROW_SQL)
            .bind(&session_key)
            .fetch_optional(&mut **tx)
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
) -> Result<ExpiredLeases, InboxError> {
    let rows: Vec<(String, i64, String, i64, i64)> = sqlx::query_as(EXPIRED_SQL)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_all(pool)
        .await?;
    let mut out = Vec::new();
    let mut unreadable = Vec::new();
    for (session_key, epoch, holder, ttl_ms, last_heartbeat) in rows {
        let sid = Uuid::parse_str(&session_key)
            .map_err(|e| InboxError::CorruptRow {
                session: SessionId(Uuid::nil()),
                input_id: Uuid::nil(),
                reason: format!("claims row session id: {e}"),
            })
            .map(SessionId)?;
        // State comes from the event projection only (constraint 1). A
        // session whose stream cannot be read is skipped, not fatal: one
        // corrupt row must not stop recovery for every other session.
        let events = match scan(pool, sid, 0..i64::MAX as u64).await {
            Ok(events) => events,
            Err(err) => {
                let err = InboxError::from(err);
                if crate::inbox::is_backend_failure(&err) {
                    return Err(err);
                }
                unreadable.push(UnreadableLease {
                    session: sid,
                    reason: err.to_string(),
                });
                continue;
            }
        };
        let state = match project(&events) {
            Ok(state) => state,
            Err(source) => {
                unreadable.push(UnreadableLease {
                    session: sid,
                    reason: source.to_string(),
                });
                continue;
            }
        };
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
    Ok(ExpiredLeases {
        expired: out,
        unreadable,
    })
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

// -- read-model snapshots (READ-MODEL §5/§9, v3) ----------------------------
// Same statements as the sqlite twins, `$N` placeholders with explicit
// casts (the pg dialect traps); the DO UPDATE predicate qualifies its
// columns with the table name (pg resolves unqualified names there
// against `excluded` too — sqlstate 42702).

pub(crate) const SNAPSHOT_INSERT_SQL: &str = "INSERT INTO projection_snapshots (session, version, state, context, context_provenance, digest, verified, created_at)\nVALUES ($1::text, $2::bigint, $3::text, $4::text, $5::text, $6::text, 1, $7::bigint)\nON CONFLICT (session, version) DO NOTHING";

pub(crate) const SNAPSHOT_HIGHEST_SQL: &str = "SELECT version, state, context, context_provenance, digest, verified, created_at FROM projection_snapshots WHERE session = $1::text AND verified = 1 ORDER BY version DESC LIMIT 1";

pub(crate) const SNAPSHOT_GC_SQL: &str = "DELETE FROM projection_snapshots WHERE session = $1::text AND version NOT IN (SELECT version FROM projection_snapshots WHERE session = $1::text ORDER BY version DESC LIMIT $2::bigint)";

pub(crate) const SNAPSHOT_CANDIDATES_SQL: &str = "SELECT session, MAX(seq) FROM events GROUP BY session HAVING MAX(seq) >= $1::bigint ORDER BY session ASC LIMIT $2::bigint";

pub(crate) const MAINTENANCE_ACQUIRE_SQL: &str = "INSERT INTO maintenance_leases (scope, holder, ttl_ms, last_heartbeat)\nVALUES ($1::text, $2::text, $3::bigint, $4::bigint)\nON CONFLICT (scope) DO UPDATE SET\n    holder = excluded.holder,\n    ttl_ms = excluded.ttl_ms,\n    last_heartbeat = excluded.last_heartbeat\nWHERE maintenance_leases.holder = excluded.holder\n   OR ($4::bigint - maintenance_leases.ttl_ms) > maintenance_leases.last_heartbeat";

pub(crate) const MAINTENANCE_HEARTBEAT_SQL: &str = "UPDATE maintenance_leases SET last_heartbeat = $3::bigint WHERE scope = $1::text AND holder = $2::text";

pub(crate) const MAINTENANCE_RELEASE_SQL: &str =
    "DELETE FROM maintenance_leases WHERE scope = $1::text AND holder = $2::text";

/// One snapshot row as stored (column order matches the SELECTs above).
pub(crate) type SnapshotCols = (i64, String, String, String, String, i64, i64);

pub(crate) async fn snapshot_insert(
    pool: &PgPool,
    p: &crate::snapshots::SnapshotInsert<'_>,
) -> Result<bool, StoreError> {
    let res = sqlx::query(SNAPSHOT_INSERT_SQL)
        .bind(p.session_key)
        .bind(p.version)
        .bind(p.state)
        .bind(p.context)
        .bind(p.provenance)
        .bind(p.digest)
        .bind(p.created_at)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn snapshot_highest(
    pool: &PgPool,
    session_key: &str,
) -> Result<Option<SnapshotCols>, StoreError> {
    let row: Option<SnapshotCols> = sqlx::query_as(SNAPSHOT_HIGHEST_SQL)
        .bind(session_key)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn snapshot_gc(
    pool: &PgPool,
    session_key: &str,
    keep_n: i64,
) -> Result<u64, StoreError> {
    let res = sqlx::query(SNAPSHOT_GC_SQL)
        .bind(session_key)
        .bind(keep_n)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

pub(crate) async fn snapshot_candidates(
    pool: &PgPool,
    min_events: i64,
    limit: i64,
) -> Result<Vec<(String, i64)>, StoreError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(SNAPSHOT_CANDIDATES_SQL)
        .bind(min_events)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub(crate) async fn maintenance_acquire(
    pool: &PgPool,
    scope: &str,
    holder: &str,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<bool, StoreError> {
    let res = sqlx::query(MAINTENANCE_ACQUIRE_SQL)
        .bind(scope)
        .bind(holder)
        .bind(ttl_ms)
        .bind(now_ms)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn maintenance_heartbeat(
    pool: &PgPool,
    scope: &str,
    holder: &str,
    now_ms: i64,
) -> Result<bool, StoreError> {
    let res = sqlx::query(MAINTENANCE_HEARTBEAT_SQL)
        .bind(scope)
        .bind(holder)
        .bind(now_ms)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn maintenance_release(
    pool: &PgPool,
    scope: &str,
    holder: &str,
) -> Result<(), StoreError> {
    sqlx::query(MAINTENANCE_RELEASE_SQL)
        .bind(scope)
        .bind(holder)
        .execute(pool)
        .await?;
    Ok(())
}

// -- materialized views (READ-MODEL §6, v4) ----------------------------------

/// Same shapes as the sqlite module, `$N` placeholders, pg types.
pub(crate) const STORE_KEY_SQL: &str = "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM events";

pub(crate) const VIEW_STATE_SQL: &str =
    "SELECT events_total, max_seq, digest, built_at FROM view_state WHERE view = $1";

pub(crate) const VIEW_ROWS_SQL: &str =
    "SELECT session, row_version, payload FROM view_rows WHERE view = $1 ORDER BY session ASC";

pub(crate) const VIEW_ROW_DELETE_SQL: &str = "DELETE FROM view_rows WHERE view = $1";
pub(crate) const VIEW_ROW_INSERT_SQL: &str =
    "INSERT INTO view_rows (view, session, row_version, payload) VALUES ($1, $2, $3, $4)";
pub(crate) const VIEW_STATE_UPSERT_SQL: &str = "INSERT INTO view_state (view, events_total, max_seq, digest, built_at)\nVALUES ($1, $2, $3, $4, $5)\nON CONFLICT(view) DO UPDATE SET\n    events_total = excluded.events_total,\n    max_seq = excluded.max_seq,\n    digest = excluded.digest,\n    built_at = excluded.built_at";
pub(crate) const VIEW_STATE_DELETE_SQL: &str = "DELETE FROM view_state WHERE view = $1";

pub(crate) async fn view_publish(
    pool: &PgPool,
    p: &crate::views::publish::Publish<'_>,
) -> Result<(), StoreError> {
    let mut tx = pool.begin().await?;
    sqlx::query(VIEW_ROW_DELETE_SQL)
        .bind(p.view)
        .execute(&mut *tx)
        .await?;
    for row in p.rows {
        sqlx::query(VIEW_ROW_INSERT_SQL)
            .bind(p.view)
            .bind(&row.session)
            .bind(se_i64(row.row_version, "view row_version")?)
            .bind(&row.payload)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(VIEW_STATE_UPSERT_SQL)
        .bind(p.view)
        .bind(p.events_total)
        .bind(p.max_seq)
        .bind(p.digest)
        .bind(p.built_at)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn view_drop(pool: &PgPool, view: &str) -> Result<(), StoreError> {
    let mut tx = pool.begin().await?;
    sqlx::query(VIEW_ROW_DELETE_SQL)
        .bind(view)
        .execute(&mut *tx)
        .await?;
    sqlx::query(VIEW_STATE_DELETE_SQL)
        .bind(view)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// incremental maintenance reads/writes (R9-10)
// ---------------------------------------------------------------------------

/// R9-10: one line per live session with its head seq — the view
/// rebuilder's change detector. Index-only over the events PK.
pub(crate) const SESSION_HEADS_SQL: &str =
    "SELECT session, MAX(seq) FROM events GROUP BY session ORDER BY session ASC";

/// R9-10: pending-eligible inbox counts per session in one grouped read
/// (the `ticket_stage` view's `pending_inbox` drift check).
pub(crate) const PENDING_COUNTS_SQL: &str = "SELECT session, COUNT(*) FROM inputs WHERE consumed_by_seq IS NULL AND eligible_at <= $1::bigint GROUP BY session ORDER BY session ASC";

/// R9-10: per-row upsert for the incremental publication.
pub(crate) const VIEW_ROW_UPSERT_SQL: &str = "INSERT INTO view_rows (view, session, row_version, payload)\nVALUES ($1, $2, $3, $4)\nON CONFLICT(view, session) DO UPDATE SET\n    row_version = excluded.row_version,\n    payload = excluded.payload";

pub(crate) const VIEW_ROW_DELETE_ONE_SQL: &str =
    "DELETE FROM view_rows WHERE view = $1 AND session = $2";

/// Every live session's head seq, session-ascending.
pub(crate) async fn session_heads(pool: &PgPool) -> Result<Vec<(String, i64)>, StoreError> {
    Ok(sqlx::query_as(SESSION_HEADS_SQL).fetch_all(pool).await?)
}

/// Pending-eligible input counts per session at `now_ms`.
pub(crate) async fn pending_input_counts(
    pool: &PgPool,
    now_ms: u64,
) -> Result<Vec<(String, i64)>, StoreError> {
    Ok(sqlx::query_as(PENDING_COUNTS_SQL)
        .bind(se_i64(now_ms, "pending counts now_ms")?)
        .fetch_all(pool)
        .await?)
}

/// Every event of the given kinds across the whole store, `(session,
/// seq)`-ascending (the board's global fold input; see the sqlite twin).
pub(crate) async fn events_where_kinds(
    pool: &PgPool,
    kinds: &[&str],
) -> Result<Vec<Event>, StoreError> {
    let rows: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT session, version, envelope FROM events \
         WHERE kind = ANY($1::text[]) ORDER BY session ASC, seq ASC",
    )
    .bind(kinds.iter().map(|k| k.to_string()).collect::<Vec<_>>())
    .fetch_all(pool)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for (session_col, version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col || event.session.0.to_string() != session_col {
            return Err(StoreError::CorruptRow {
                session: event.session,
                seq: event.seq,
            });
        }
        events.push(event);
    }
    Ok(events)
}

/// Every event of the given kinds whose recorded `ts` falls in
/// `[since_ms, until_ms]`, ordered by `(ts, session, seq)` (the
/// sqlite twin's contract).
pub(crate) async fn events_in_window(
    pool: &PgPool,
    kinds: &[&str],
    since_ms: u64,
    until_ms: u64,
) -> Result<Vec<Event>, StoreError> {
    let rows: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT session, version, envelope FROM events WHERE kind = ANY($1::text[]) AND ts >= $2 AND ts <= $3 ORDER BY ts ASC, session ASC, seq ASC",
    )
    .bind(kinds.iter().map(|k| k.to_string()).collect::<Vec<_>>())
    .bind(se_i64(since_ms, "self inspect since_ms")?)
    .bind(se_i64(until_ms, "self inspect until_ms")?)
    .fetch_all(pool)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for (session_col, version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col || event.session.0.to_string() != session_col {
            return Err(StoreError::CorruptRow {
                session: event.session,
                seq: event.seq,
            });
        }
        events.push(event);
    }
    Ok(events)
}

/// Event counts per kind whose recorded `ts` falls in
/// `[since_ms, until_ms]`, kind-ascending (the sqlite twin's contract).
pub(crate) async fn event_kind_counts(
    pool: &PgPool,
    since_ms: u64,
    until_ms: u64,
) -> Result<Vec<(String, u64)>, StoreError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind, COUNT(*) FROM events WHERE ts >= $1 AND ts <= $2 GROUP BY kind ORDER BY kind ASC",
    )
    .bind(se_i64(since_ms, "self inspect since_ms")?)
    .bind(se_i64(until_ms, "self inspect until_ms")?)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(kind, count)| (kind, count.max(0) as u64))
        .collect())
}

/// The incremental publication (R9-10): ONE transaction — delete the
/// gone sessions' rows, upsert the changed rows, flip `view_state`.
/// Same atomic version-flip shape as [`view_publish`], writes
/// proportional to the change.
pub(crate) async fn view_publish_delta(
    pool: &PgPool,
    p: &crate::views::publish::PublishDelta<'_>,
) -> Result<(), StoreError> {
    let mut tx = pool.begin().await?;
    for session in p.gone_sessions {
        sqlx::query(VIEW_ROW_DELETE_ONE_SQL)
            .bind(p.view)
            .bind(session)
            .execute(&mut *tx)
            .await?;
    }
    for row in p.rows {
        sqlx::query(VIEW_ROW_UPSERT_SQL)
            .bind(p.view)
            .bind(&row.session)
            .bind(se_i64(row.row_version, "view row_version")?)
            .bind(&row.payload)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(VIEW_STATE_UPSERT_SQL)
        .bind(p.view)
        .bind(p.events_total)
        .bind(p.max_seq)
        .bind(p.digest)
        .bind(p.built_at)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

// -- durable cron state (CRON-MODEL §4/§9, v7-v8) ---------------------------
// Same statements as the sqlite twins, `$N` placeholders with explicit
// casts (every nullable parameter needs one — pg cannot infer a bare
// NULL). Fire times stay TEXT in the canonical UTC RFC 3339 `Z` form, so
// the cursor comparison and the firing ordering are the same text
// comparisons on both backends.

pub(crate) const CRON_SCHEDULE_UPSERT_SQL: &str = "INSERT INTO cron_schedules (id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at)\nVALUES ($1::text, $2::text, $3::text, $4::text, NULL, NULL, $5::bigint, $6::bigint, $6::bigint)\nON CONFLICT (id) DO UPDATE SET\n    expr = excluded.expr,\n    tz = excluded.tz,\n    enabled = excluded.enabled,\n    cursor_fire_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        THEN cron_schedules.cursor_fire_at ELSE NULL END,\n    next_fire_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        THEN cron_schedules.next_fire_at ELSE NULL END,\n    declaration_digest = excluded.declaration_digest,\n    updated_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        AND cron_schedules.expr = excluded.expr\n        AND cron_schedules.tz = excluded.tz\n        AND cron_schedules.enabled = excluded.enabled\n        THEN cron_schedules.updated_at ELSE excluded.updated_at END";

pub(crate) const CRON_SCHEDULE_DIGEST_SQL: &str =
    "SELECT declaration_digest FROM cron_schedules WHERE id = $1::text";

pub(crate) const CRON_SCHEDULE_CURSOR_SQL: &str =
    "SELECT cursor_fire_at FROM cron_schedules WHERE id = $1::text";

pub(crate) const CRON_SCHEDULES_SQL: &str = "SELECT id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at FROM cron_schedules ORDER BY id ASC";

pub(crate) const CRON_SCHEDULE_SQL: &str = "SELECT id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at FROM cron_schedules WHERE id = $1::text";

/// Insert-if-absent. Two racers reach the unique index together; one
/// commits and the other blocks, then reports zero rows — exactly one
/// winner (`rows_affected() == 1`).
pub(crate) const CRON_DELETE_SCHEDULE_SQL: &str = "DELETE FROM cron_schedules WHERE id = $1::text";

pub(crate) const CRON_FIRING_CLAIM_SQL: &str = "INSERT INTO cron_firings (firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session)\nVALUES ($1::text, $2::text, $3::text, $4::bigint, $5::text, $6::text, $7::text, $8::bigint, $9::text, $10::text)\nON CONFLICT DO NOTHING";

pub(crate) const CRON_FIRING_FINISH_SQL: &str = "UPDATE cron_firings\nSET state = $2::text,\n    worktree_path = COALESCE($3::text, worktree_path),\n    finished_at = $4::bigint,\n    skip_reason = COALESCE($5::text, skip_reason),\n    error = $6::text\nWHERE firing_id = $1::text AND state IN ('admitted', 'running')";

pub(crate) const CRON_ADVANCE_CURSOR_SQL: &str = "UPDATE cron_schedules\nSET cursor_fire_at = $2::text,\n    next_fire_at = $3::text,\n    updated_at = $4::bigint\nWHERE id = $1::text\n  AND (cursor_fire_at IS NULL OR cursor_fire_at < $2::text)\n  AND EXISTS (SELECT 1 FROM cron_firings WHERE firing_id = $5::text)";

pub(crate) const CRON_LIST_FIRINGS_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings\nWHERE ($1::text IS NULL OR schedule_id = $1::text)\nORDER BY fire_time DESC, firing_id DESC\nLIMIT $2::bigint";

pub(crate) const CRON_LOAD_FIRING_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings WHERE firing_id = $1::text";

/// The settlement sweep's open-firing read (CRON-MODEL §9.1) — the sqlite
/// twin's comment explains why a newest-page scan is not enough.
pub(crate) const CRON_OPEN_FIRINGS_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings\nWHERE schedule_id = $1::text AND state IN ('admitted', 'running')\nORDER BY fire_time ASC, firing_id ASC";

pub(crate) async fn cron_upsert_schedule(
    pool: &PgPool,
    p: &crate::cron::ScheduleInsert<'_>,
) -> Result<Option<String>, StoreError> {
    // `FOR UPDATE` would be the paranoid form; the reset decision is made
    // by the UPSERT itself, so the pre-read only feeds the caller's
    // report (documented on the sqlite twin).
    let previous: Option<String> = sqlx::query_scalar(CRON_SCHEDULE_DIGEST_SQL)
        .bind(p.id)
        .fetch_optional(pool)
        .await?;
    sqlx::query(CRON_SCHEDULE_UPSERT_SQL)
        .bind(p.id)
        .bind(p.expr)
        .bind(p.tz)
        .bind(p.digest)
        .bind(p.enabled)
        .bind(p.now)
        .execute(pool)
        .await?;
    Ok(previous)
}

pub(crate) async fn cron_load_schedules(
    pool: &PgPool,
) -> Result<Vec<crate::cron::ScheduleCols>, StoreError> {
    let rows: Vec<crate::cron::ScheduleCols> =
        sqlx::query_as(CRON_SCHEDULES_SQL).fetch_all(pool).await?;
    Ok(rows)
}

pub(crate) async fn cron_load_schedule(
    pool: &PgPool,
    id: &str,
) -> Result<Option<crate::cron::ScheduleCols>, StoreError> {
    let row: Option<crate::cron::ScheduleCols> = sqlx::query_as(CRON_SCHEDULE_SQL)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn cron_delete_schedule(pool: &PgPool, id: &str) -> Result<bool, StoreError> {
    let res = sqlx::query(CRON_DELETE_SCHEDULE_SQL)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn cron_claim_firing(
    pool: &PgPool,
    p: &crate::cron::FiringInsert<'_>,
) -> Result<bool, crate::cron::CronError> {
    let res = sqlx::query(CRON_FIRING_CLAIM_SQL)
        .bind(p.firing_id)
        .bind(p.schedule_id)
        .bind(p.fire_time)
        .bind(p.admitted_at)
        .bind(p.state)
        .bind(p.skip_reason)
        .bind(p.worktree_path)
        .bind(p.finished_at)
        .bind(p.error)
        .bind(p.session)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn cron_finish_firing(
    pool: &PgPool,
    firing_id: &str,
    state: &str,
    finished_at: Option<i64>,
    worktree_path: Option<&str>,
    skip_reason: Option<&str>,
    error: Option<&str>,
) -> Result<bool, crate::cron::CronError> {
    let res = sqlx::query(CRON_FIRING_FINISH_SQL)
        .bind(firing_id)
        .bind(state)
        .bind(worktree_path)
        .bind(finished_at)
        .bind(skip_reason)
        .bind(error)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn cron_advance_cursor(
    pool: &PgPool,
    id: &str,
    firing_id: &str,
    admitted_fire_at: &str,
    next_fire_at: &str,
    now: i64,
) -> Result<bool, crate::cron::CronError> {
    let res = sqlx::query(CRON_ADVANCE_CURSOR_SQL)
        .bind(id)
        .bind(admitted_fire_at)
        .bind(next_fire_at)
        .bind(now)
        .bind(firing_id)
        .execute(pool)
        .await?;
    if res.rows_affected() == 1 {
        return Ok(true);
    }
    let current: Option<Option<String>> = sqlx::query_scalar(CRON_SCHEDULE_CURSOR_SQL)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    match current {
        None => Err(crate::cron::CronError::UnknownSchedule { id: id.to_owned() }),
        Some(cursor) if cursor.as_deref().is_some_and(|c| c >= admitted_fire_at) => Ok(false),
        Some(_) => Err(crate::cron::CronError::UnrecordedFiring {
            firing_id: firing_id.to_owned(),
        }),
    }
}

pub(crate) async fn cron_list_firings(
    pool: &PgPool,
    schedule_id: Option<&str>,
    limit: i64,
) -> Result<Vec<crate::cron::FiringCols>, StoreError> {
    let rows: Vec<crate::cron::FiringCols> = sqlx::query_as(CRON_LIST_FIRINGS_SQL)
        .bind(schedule_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub(crate) async fn cron_load_firing(
    pool: &PgPool,
    firing_id: &str,
) -> Result<Option<crate::cron::FiringCols>, StoreError> {
    let row: Option<crate::cron::FiringCols> = sqlx::query_as(CRON_LOAD_FIRING_SQL)
        .bind(firing_id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn cron_open_firings(
    pool: &PgPool,
    schedule_id: &str,
) -> Result<Vec<crate::cron::FiringCols>, StoreError> {
    let rows: Vec<crate::cron::FiringCols> = sqlx::query_as(CRON_OPEN_FIRINGS_SQL)
        .bind(schedule_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Record where a claimed firing is dispatched (CRON-MODEL §3.2/§9.1) — the
/// sqlite twin's comment explains the guarded no-op semantics.
pub(crate) const CRON_SET_FIRING_SESSION_SQL: &str = "UPDATE cron_firings SET session = $2::text WHERE firing_id = $1::text AND state IN ('admitted', 'running')";

pub(crate) async fn cron_set_firing_session(
    pool: &PgPool,
    firing_id: &str,
    session: &str,
) -> Result<bool, crate::cron::CronError> {
    let res = sqlx::query(CRON_SET_FIRING_SESSION_SQL)
        .bind(firing_id)
        .bind(session)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}
