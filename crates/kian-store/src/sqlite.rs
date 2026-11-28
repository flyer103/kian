//! The sqlite backend: the legacy statements and statement logic,
//! extracted verbatim from the single-backend store so the dispatching
//! [`EventStore`](crate::event_store::EventStore) can expose the same
//! surface on postgres. Dialect SQL lives here and only here.
//!
//! sqlite specifics pinned by this module:
//! - `?N` placeholders, bound per occurrence (positional per index);
//! - scalar `MAX(a, b)` in CLAIM_SQL (sqlite's two-argument MAX is scalar);
//! - `append_batch` = guarded first-row CAS + plain tail inserts inside one
//!   transaction: the first insert holds sqlite's writer lock until commit,
//!   so no concurrent writer can interleave between the fence check and the
//!   tail rows (the guarantee postgres gets from a single CTE statement).

use std::ops::Range;

use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use kian_core::state::{SessionState, project};
use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

use crate::event_store::{AppendGuard, Head, StoreError, classify_cas_rejection, to_i64 as se_i64};
use crate::inbox::{AdmitOutcome, InboxError, SessionInbox, decode_input, to_i64 as ie_i64};

// -- events -----------------------------------------------------------------

/// The single-statement CAS append (ADR-005). The insert lands only when
/// both fences hold at statement time: the session's epoch floor must not
/// exceed the write's epoch, and the stream head must be exactly
/// `expected_seq - 1` (so the primary key can never collide and seqs stay
/// contiguous). Anything else inserts zero rows and is classified into a
/// typed error by the HEAD re-read in [`classify`].
const APPEND_SQL: &str = "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope)\nSELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7\nWHERE (SELECT COALESCE(MAX(epoch), 0) FROM events WHERE session = ?8) <= ?9\n  AND (SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = ?10) = ?11 - 1";

/// The tail rows of a batch append ride the first row's CAS (see module
/// docs: the writer lock covers the whole transaction).
const BATCH_TAIL_SQL: &str = "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope)\nVALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

const HEAD_SQL: &str =
    "SELECT COALESCE(MAX(epoch), 0), COALESCE(MAX(seq), 0) FROM events WHERE session = ?";

/// The seq axis only (the unchanged-read fast path — see `version`).
const VERSION_SQL: &str = "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session = ?";

const SCAN_SQL: &str = "SELECT version, envelope FROM events WHERE session = ? AND seq >= ? AND seq < ? ORDER BY seq ASC";

/// The kind-filtered session read behind `EventStore::scan_kind`
/// (memory P3b-1): served by the v6 `idx_events_session_kind_seq` index
/// (`session, kind, seq` — equality on the first two, so the index order
/// already satisfies the `ORDER BY seq`).
const SCAN_KIND_SQL: &str =
    "SELECT version, envelope FROM events WHERE session = ?1 AND kind = ?2 ORDER BY seq ASC";

/// The CAS refused the write: re-read the head and classify which fence
/// tripped — the same classification both backends share (§3: pg's
/// PK-race zero-row outcome flows through here too).
async fn classify(
    pool: &SqlitePool,
    session_key: &str,
    session: SessionId,
    guard: AppendGuard,
) -> Result<(), StoreError> {
    let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
        .bind(session_key)
        .fetch_one(pool)
        .await?;
    Err(classify_cas_rejection(session, guard, epoch_floor, max_seq))
}

pub(crate) async fn append(
    pool: &SqlitePool,
    session: SessionId,
    event: &Event,
    envelope: &str,
    guard: AppendGuard,
) -> Result<(), StoreError> {
    let session_key = session.0.to_string();
    let result = sqlx::query(APPEND_SQL)
        .bind(&session_key)
        .bind(se_i64(event.seq, "seq")?)
        .bind(se_i64(event.epoch.0, "epoch")?)
        .bind(se_i64(event.ts, "ts")?)
        .bind(event.kind.as_str())
        .bind(i64::from(event.version))
        .bind(envelope)
        // Fence inputs are bound again: sqlite parameters are positional
        // per index, and repeating?N inline reads worse than rebinding.
        .bind(&session_key)
        .bind(se_i64(event.epoch.0, "epoch")?)
        .bind(&session_key)
        .bind(se_i64(event.seq, "seq")?)
        .execute(pool)
        .await?;
    if result.rows_affected() == 1 {
        return Ok(());
    }
    classify(pool, &session_key, session, guard).await
}

pub(crate) async fn append_batch(
    pool: &SqlitePool,
    session: SessionId,
    events: &[Event],
    envelopes: &[String],
    guard: AppendGuard,
) -> Result<(), StoreError> {
    let first = &events[0];
    let session_key = session.0.to_string();
    let mut tx = pool.begin().await?;
    // The first row is the same single-statement CAS as `append`: either
    // both fences hold and the batch proceeds (holding the writer lock
    // until commit), or zero rows land and the typed error classifies
    // which fence tripped.
    let result = sqlx::query(APPEND_SQL)
        .bind(&session_key)
        .bind(se_i64(first.seq, "seq")?)
        .bind(se_i64(first.epoch.0, "epoch")?)
        .bind(se_i64(first.ts, "ts")?)
        .bind(first.kind.as_str())
        .bind(i64::from(first.version))
        .bind(envelopes[0].as_str())
        .bind(&session_key)
        .bind(se_i64(first.epoch.0, "epoch")?)
        .bind(&session_key)
        .bind(se_i64(first.seq, "seq")?)
        .execute(&mut *tx)
        .await?;
    if result.rows_affected() != 1 {
        let (epoch_floor, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
            .bind(&session_key)
            .fetch_one(&mut *tx)
            .await?;
        return Err(classify_cas_rejection(session, guard, epoch_floor, max_seq));
    }
    for (event, envelope) in events[1..].iter().zip(&envelopes[1..]) {
        sqlx::query(BATCH_TAIL_SQL)
            .bind(&session_key)
            .bind(se_i64(event.seq, "seq")?)
            .bind(se_i64(event.epoch.0, "epoch")?)
            .bind(se_i64(event.ts, "ts")?)
            .bind(event.kind.as_str())
            .bind(i64::from(event.version))
            .bind(envelope.as_str())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn scan(
    pool: &SqlitePool,
    session: SessionId,
    range: Range<u64>,
) -> Result<Vec<Event>, StoreError> {
    // Clamp the upper bound to sqlite's i64 domain so "everything" can be
    // expressed as 0..i64::MAX.
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

/// The kind-filtered session read (`WHERE session = ? AND kind = ?`,
/// seq-ascending) — the sqlite arm of `EventStore::scan_kind`. The same
/// envelope/coherence decoding as [`scan`]; a `kind` projection column
/// that disagrees with its envelope is a corrupt row, exactly like a
/// drifting `version` or `session`.
pub(crate) async fn scan_kind(
    pool: &SqlitePool,
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

/// The multi-kind session read behind `EventStore::scan_kinds` — the
/// [`scan_kind`] index with an `IN` list (`session` equality first, so
/// the index order still satisfies `ORDER BY seq`). Matching is by the
/// `kind` projection column's key, which collapses `session/ended`'s
/// reasons into one key: the decoded variants come back as stored.
pub(crate) async fn scan_kinds(
    pool: &SqlitePool,
    session: SessionId,
    kinds: &[EventKind],
) -> Result<Vec<Event>, StoreError> {
    let mut keys: Vec<&str> = kinds.iter().map(EventKind::as_str).collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    // No `ORDER BY seq` here on purpose: with an `IN` list sqlite prefers
    // the primary key (session, seq) and filters every row of the session,
    // whereas the unordered form seeks the per-session kind index once per
    // key. The seq order is restored in memory below.
    let placeholders = vec!["?"; keys.len()].join(", ");
    let sql = format!(
        "SELECT version, envelope FROM events WHERE session = ? AND kind IN ({placeholders})"
    );
    let mut query = sqlx::query_as(&sql).bind(session.0.to_string());
    for key in &keys {
        query = query.bind(*key);
    }
    let rows: Vec<(i64, String)> = query.fetch_all(pool).await?;
    let mut events = Vec::with_capacity(rows.len());
    for (version_col, envelope) in rows {
        let event: Event = serde_json::from_str(&envelope)?;
        if i64::from(event.version) != version_col
            || event.session != session
            || !keys.contains(&event.kind.as_str())
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

pub(crate) async fn head(pool: &SqlitePool, session: SessionId) -> Result<Head, StoreError> {
    let (epoch, max_seq): (i64, i64) = sqlx::query_as(HEAD_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await?;
    Ok(Head {
        next_seq: max_seq as u64 + 1,
        epoch: Epoch(epoch as u64),
    })
}

/// READ-MODEL §3 (P0): the session's version (`head.next_seq - 1`)
/// alone. Unlike [`head`] this touches only the seq axis — the PK
/// index's min/max lookup, no per-row epoch read — so the
/// unchanged-read fast path stays one cheap query at any history
/// size. (`MAX(epoch)` in [`HEAD_SQL`] has no index and would scan
/// the session's rows every call.)
pub(crate) async fn version(pool: &SqlitePool, session: SessionId) -> Result<u64, StoreError> {
    let max_seq: i64 = sqlx::query_scalar(VERSION_SQL)
        .bind(session.0.to_string())
        .fetch_one(pool)
        .await?;
    Ok(max_seq as u64)
}

// -- inbox ------------------------------------------------------------------

/// Durable admit keyed by `(session, idempotency_key)`: the unique index
/// makes the insert a no-op on a key collision; NULL keys never collide,
/// so keyless admits always insert. The admission ordinal
/// (`admitted_seq`, v2) is `max+1` computed inside the same statement —
/// sqlite serializes whole write statements, so concurrent admits
/// cannot observe the same max, and the take queries' same-millisecond
/// tie-break is the admission order, never the random uuid.
const ADMIT_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key, admitted_seq)\nVALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(admitted_seq), 0) + 1 FROM inputs))\nON CONFLICT (session, idempotency_key) DO NOTHING";

const INPUT_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = ?1 AND idempotency_key = ?2";

/// Channel-scoped admit (ADR-081): the same durable write as [`ADMIT_SQL`]
/// plus the cross-incarnation guard — the insert is refused when a row
/// with this idempotency key is already *consumed* in any session. A row
/// that was merely admitted never blocks, so a replay still rescues a
/// pending row out of a frozen predecessor. Still one statement (the
/// `NOT EXISTS` probe cannot race a check-then-insert window), served by
/// `idx_inputs_channel_key` (migration v9).
const ADMIT_CHANNEL_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, eligible_at, idempotency_key, admitted_seq)\nSELECT ?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(admitted_seq), 0) + 1 FROM inputs)\nWHERE NOT EXISTS (SELECT 1 FROM inputs p WHERE p.idempotency_key = ?6 AND p.consumed_by_seq IS NOT NULL)\nON CONFLICT (session, idempotency_key) DO NOTHING";

/// The suppressing row of a refused channel admit: the consumed row with
/// this key in *any* session (ADR-081 decision 6 — the receipt names the
/// row that already answered the event, wherever its incarnation lives).
const CONSUMED_BY_KEY_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE idempotency_key = ?1 AND consumed_by_seq IS NOT NULL ORDER BY admitted_at ASC LIMIT 1";

const ELIGIBLE_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = ?1 AND consumed_by_seq IS NULL AND eligible_at <= ?2 ORDER BY eligible_at ASC, admitted_seq ASC";

/// (R17 C4, pin P3-5) the read-only per-session inbox read behind the
/// task board's `input_required` arm: EVERY row of the session,
/// consumed rows included — the row is durable and audit-visible after
/// `mark_consumed`, so a report row that was already consumed by the
/// parent's drain still carries its `state:"waiting"` fact. No
/// eligibility filter, no clock: the caller reads facts, not work.
const INPUT_ROWS_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE session = ?1 ORDER BY eligible_at ASC, admitted_seq ASC";

const STARTUP_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, idempotency_key FROM inputs WHERE consumed_by_seq IS NULL AND eligible_at <= ?1 ORDER BY eligible_at ASC, admitted_seq ASC";

/// Consumption is event-driven by construction: the row is touched only
/// when the referenced event row already exists in the same session's
/// stream. The input row is never removed.
const CONSUME_SQL: &str = "UPDATE inputs SET consumed_by_seq = ?3\nWHERE session = ?1 AND id = ?2\n  AND consumed_by_seq IS NULL\n  AND EXISTS (SELECT 1 FROM events WHERE session = ?1 AND seq = ?3)";

const EVENT_EXISTS_SQL: &str = "SELECT COUNT(*) FROM events WHERE session = ?1 AND seq = ?2";

const CONSUMED_STATE_SQL: &str =
    "SELECT consumed_by_seq FROM inputs WHERE session = ?1 AND id = ?2";

/// Monotonic epoch issuance in one statement (sqlite serializes writers,
/// so concurrent claims execute this atomically in order). The floor is
/// max(claims.epoch, events epoch floor); the conflict branch's
/// `excluded.epoch` is strictly greater than the stored epoch, so epochs
/// never repeat. (ADR-009) — conditional claim admission: the conflict
/// branch lands only when the existing lease is expired or the claiming
/// holder already owns the row; otherwise zero rows return and `claim`
/// reports the typed `ClaimRejected`.
const CLAIM_SQL: &str = "INSERT INTO claims (session, epoch, holder, ttl_ms, last_heartbeat)\nVALUES (?1,\n        MAX(COALESCE((SELECT MAX(epoch) FROM claims WHERE session = ?2), 0),\n             COALESCE((SELECT MAX(epoch) FROM events WHERE session = ?3), 0)) + 1,\n        ?4, ?5, ?6)\nON CONFLICT(session) DO UPDATE SET\n    epoch = excluded.epoch,\n    holder = excluded.holder,\n    ttl_ms = excluded.ttl_ms,\n    last_heartbeat = excluded.last_heartbeat\nWHERE (?7 - ttl_ms) > last_heartbeat\n   OR holder = ?4\nRETURNING epoch";

/// Reads the live claims row after a refused claim, to report *who* holds
/// the fresh lease (`ClaimRejected` diagnostics).
const CLAIM_ROW_SQL: &str =
    "SELECT epoch, holder, ttl_ms, last_heartbeat FROM claims WHERE session = ?1";

const HEARTBEAT_SQL: &str = "UPDATE claims SET last_heartbeat = ?2 WHERE session = ?1";

/// Expired = `now - ttl > last_heartbeat`, written as a subtraction so
/// neither side of the comparison can overflow i64.
const EXPIRED_SQL: &str = "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims WHERE (?1 - ttl_ms) > last_heartbeat ORDER BY session ASC";

/// Every claims row (`lease inspect`): no filtering, epoch order.
const ALL_LEASES_SQL: &str =
    "SELECT session, epoch, holder, ttl_ms, last_heartbeat FROM claims ORDER BY epoch ASC";

type InputCols = (String, String, String, i64, i64, Option<String>);

pub(crate) async fn admit(pool: &SqlitePool, input: InputRow) -> Result<AdmitOutcome, InboxError> {
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
    // Zero rows: the (session, idempotency_key) index already held a row.
    // Read it back — first write wins.
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

/// The cross-incarnation channel admission (ADR-081). Identical to
/// [`admit`] except the insert additionally refuses a key that is already
/// consumed in any session; `deduplicated: true` then names the consumed
/// row (this session's row is read back first, the suppressing row
/// second).
pub(crate) async fn admit_channel(
    pool: &SqlitePool,
    input: InputRow,
) -> Result<AdmitOutcome, InboxError> {
    let payload = serde_json::to_string(&input.payload)?;
    let session_key = input.session.0.to_string();
    let result = sqlx::query(ADMIT_CHANNEL_SQL)
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
            reason: "channel insert skipped without an idempotency key".into(),
        });
    };
    // Zero rows: this session already holds the key (first write wins), or
    // the cross-incarnation guard refused a key consumed elsewhere.
    let existing: Option<InputCols> = sqlx::query_as(INPUT_BY_KEY_SQL)
        .bind(&session_key)
        .bind(&key)
        .fetch_optional(pool)
        .await?;
    if let Some(existing) = existing {
        return decode_input(existing).map(|input| AdmitOutcome {
            input,
            deduplicated: true,
        });
    }
    let suppressed: Option<InputCols> = sqlx::query_as(CONSUMED_BY_KEY_SQL)
        .bind(&key)
        .fetch_optional(pool)
        .await?;
    suppressed
        .map(decode_input)
        .transpose()?
        .map(|input| AdmitOutcome {
            input,
            deduplicated: true,
        })
        .ok_or(InboxError::CorruptRow {
            session: SessionId(Uuid::parse_str(&session_key).unwrap_or_else(|_| Uuid::nil())),
            input_id: Uuid::nil(),
            reason: "channel admit was refused but neither this session's row nor a consumed row was found".into(),
        })
}

pub(crate) async fn eligible_inputs(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
    session: SessionId,
) -> Result<Vec<InputRow>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(INPUT_ROWS_SQL)
        .bind(session.0.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(decode_input).collect()
}

pub(crate) async fn mark_consumed(
    pool: &SqlitePool,
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
    // The guarded write refused: classify which condition failed.
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
    pool: &SqlitePool,
    session: SessionId,
    holder: &str,
    ttl_ms: u64,
    now_ms: u64,
) -> Result<LeaseRecord, InboxError> {
    let session_key = session.0.to_string();
    let issued: Option<i64> = sqlx::query_scalar(CLAIM_SQL)
        .bind(&session_key)
        .bind(&session_key)
        .bind(&session_key)
        .bind(holder)
        .bind(ie_i64(ttl_ms, "ttl_ms")?)
        .bind(ie_i64(now_ms, "now_ms")?)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_optional(pool)
        .await?;
    let Some(epoch) = issued else {
        // Conditional admission refused: read the live row and report who
        // holds the fresh lease. Claims rows are never deleted, so a
        // missing row here is corruption.
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
        // State comes from the event projection only (constraint 1):
        // terminal, idle and recoverable sessions never surface as
        // expired leases.
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

pub(crate) async fn leases(pool: &SqlitePool) -> Result<Vec<LeaseRecord>, InboxError> {
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
    pool: &SqlitePool,
    now_ms: u64,
) -> Result<Vec<SessionInbox>, InboxError> {
    let rows: Vec<InputCols> = sqlx::query_as(STARTUP_SQL)
        .bind(ie_i64(now_ms, "now_ms")?)
        .fetch_all(pool)
        .await?;
    // Rows arrive ordered by (eligible_at, id); sessions may interleave,
    // so group by first-seen session, preserving that order across the
    // groups.
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

/// Insert-only publication (R6): the row is born verified (the builder's
/// second derivation compared `(state, context, provenance)` before this
/// statement ran), and a conflict with an existing row is a no-op — a
/// previously visible snapshot version is never overwritten.
pub(crate) const SNAPSHOT_INSERT_SQL: &str = "INSERT INTO projection_snapshots (session, version, state, context, context_provenance, digest, verified, created_at)\nVALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)\nON CONFLICT (session, version) DO NOTHING";

/// The loader's single snapshot query: the highest verified row for the
/// session (the partial index carries it).
pub(crate) const SNAPSHOT_HIGHEST_SQL: &str = "SELECT version, state, context, context_provenance, digest, verified, created_at FROM projection_snapshots WHERE session = ?1 AND verified = 1 ORDER BY version DESC LIMIT 1";

/// GC (R6: orphans are GC-able): keep the newest `keep_n` versions of the
/// session, delete everything older — verified or not. The just-published
/// row is always among the newest, so publication is never undone by GC.
pub(crate) const SNAPSHOT_GC_SQL: &str = "DELETE FROM projection_snapshots WHERE session = ?1 AND version NOT IN (SELECT version FROM projection_snapshots WHERE session = ?2 ORDER BY version DESC LIMIT ?3)";

/// Snapshot-builder candidates: every session whose head version has
/// reached the configured minimum, with that version. One aggregate over
/// the events PK — maintenance cadence only, never the serving path.
pub(crate) const SNAPSHOT_CANDIDATES_SQL: &str = "SELECT session, MAX(seq) FROM events GROUP BY session HAVING MAX(seq) >= ?1 ORDER BY session ASC LIMIT ?2";

/// Maintenance-lease acquisition (liveness only, READ-MODEL §9): the
/// conflict branch lands when the caller already holds the scope or the
/// recorded lease has expired past its TTL. Zero rows = someone else
/// holds a live lease — the builder skips (idempotent under retry).
pub(crate) const MAINTENANCE_ACQUIRE_SQL: &str = "INSERT INTO maintenance_leases (scope, holder, ttl_ms, last_heartbeat)\nVALUES (?1, ?2, ?3, ?4)\nON CONFLICT (scope) DO UPDATE SET\n    holder = excluded.holder,\n    ttl_ms = excluded.ttl_ms,\n    last_heartbeat = excluded.last_heartbeat\nWHERE maintenance_leases.holder = excluded.holder\n   OR (?4 - maintenance_leases.ttl_ms) > maintenance_leases.last_heartbeat";

pub(crate) const MAINTENANCE_HEARTBEAT_SQL: &str =
    "UPDATE maintenance_leases SET last_heartbeat = ?3 WHERE scope = ?1 AND holder = ?2";

pub(crate) const MAINTENANCE_RELEASE_SQL: &str =
    "DELETE FROM maintenance_leases WHERE scope = ?1 AND holder = ?2";

/// One snapshot row as stored (column order matches the SELECTs above).
pub(crate) type SnapshotCols = (i64, String, String, String, String, i64, i64);

pub(crate) async fn snapshot_insert(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
    session_key: &str,
) -> Result<Option<SnapshotCols>, StoreError> {
    let row: Option<SnapshotCols> = sqlx::query_as(SNAPSHOT_HIGHEST_SQL)
        .bind(session_key)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn snapshot_gc(
    pool: &SqlitePool,
    session_key: &str,
    keep_n: i64,
) -> Result<u64, StoreError> {
    let res = sqlx::query(SNAPSHOT_GC_SQL)
        .bind(session_key)
        .bind(session_key)
        .bind(keep_n)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

pub(crate) async fn snapshot_candidates(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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

/// The store's freshness key: `(total event rows, MAX(seq))`. `seq` is
/// per-session, so the count is load-bearing — an append to a session
/// shorter than the global max moves only the count.
pub(crate) const STORE_KEY_SQL: &str = "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM events";

pub(crate) const VIEW_STATE_SQL: &str =
    "SELECT events_total, max_seq, digest, built_at FROM view_state WHERE view = ?1";

pub(crate) const VIEW_ROWS_SQL: &str =
    "SELECT session, row_version, payload FROM view_rows WHERE view = ?1 ORDER BY session ASC";

/// One-transaction publication (READ-MODEL §6): the whole view version
/// — rows and state — flips atomically, so a crash leaves the old
/// complete version or the new one, never half a view.
pub(crate) const VIEW_ROW_DELETE_SQL: &str = "DELETE FROM view_rows WHERE view = ?1";
pub(crate) const VIEW_ROW_INSERT_SQL: &str =
    "INSERT INTO view_rows (view, session, row_version, payload) VALUES (?1, ?2, ?3, ?4)";
pub(crate) const VIEW_STATE_UPSERT_SQL: &str = "INSERT INTO view_state (view, events_total, max_seq, digest, built_at)\nVALUES (?1, ?2, ?3, ?4, ?5)\nON CONFLICT(view) DO UPDATE SET\n    events_total = excluded.events_total,\n    max_seq = excluded.max_seq,\n    digest = excluded.digest,\n    built_at = excluded.built_at";
pub(crate) const VIEW_STATE_DELETE_SQL: &str = "DELETE FROM view_state WHERE view = ?1";

pub(crate) async fn view_publish(
    pool: &SqlitePool,
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

pub(crate) async fn view_drop(pool: &SqlitePool, view: &str) -> Result<(), StoreError> {
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
/// rebuilder's change detector. Index-only over the events PK (`session,
/// seq`): no envelope decode, no per-session queries.
pub(crate) const SESSION_HEADS_SQL: &str =
    "SELECT session, MAX(seq) FROM events GROUP BY session ORDER BY session ASC";

/// R9-10: pending-eligible inbox counts per session in one grouped read
/// (the `ticket_stage` view's `pending_inbox` drift check — an inbox row
/// can become eligible with no append, so heads alone cannot detect it).
pub(crate) const PENDING_COUNTS_SQL: &str = "SELECT session, COUNT(*) FROM inputs WHERE consumed_by_seq IS NULL AND eligible_at <= ?1 GROUP BY session ORDER BY session ASC";

/// R9-10: per-row upsert for the incremental publication (replaces only
/// the changed sessions' rows; unchanged rows stay physically in place).
pub(crate) const VIEW_ROW_UPSERT_SQL: &str = "INSERT INTO view_rows (view, session, row_version, payload)\nVALUES (?1, ?2, ?3, ?4)\nON CONFLICT(view, session) DO UPDATE SET\n    row_version = excluded.row_version,\n    payload = excluded.payload";

pub(crate) const VIEW_ROW_DELETE_ONE_SQL: &str =
    "DELETE FROM view_rows WHERE view = ?1 AND session = ?2";

/// Every live session's head seq, session-ascending.
pub(crate) async fn session_heads(pool: &SqlitePool) -> Result<Vec<(String, i64)>, StoreError> {
    Ok(sqlx::query_as(SESSION_HEADS_SQL).fetch_all(pool).await?)
}

/// Pending-eligible input counts per session at `now_ms`.
pub(crate) async fn pending_input_counts(
    pool: &SqlitePool,
    now_ms: i64,
) -> Result<Vec<(String, i64)>, StoreError> {
    Ok(sqlx::query_as(PENDING_COUNTS_SQL)
        .bind(now_ms)
        .fetch_all(pool)
        .await?)
}

/// Every event of the given kinds across the whole store, `(session,
/// seq)`-ascending — the ticket board's global fold input without
/// reading every full stream (the folds ignore all other kinds, so the
/// derived board is identical by construction). The kind list is a
/// compile-time constant of the board, never user input.
pub(crate) async fn events_where_kinds(
    pool: &SqlitePool,
    kinds: &[&str],
) -> Result<Vec<Event>, StoreError> {
    let placeholders = vec!["?"; kinds.len()].join(", ");
    let sql = format!(
        "SELECT session, version, envelope FROM events WHERE kind IN ({placeholders}) \
         ORDER BY session ASC, seq ASC"
    );
    let mut query = sqlx::query_as::<_, (String, i64, String)>(&sql);
    for kind in kinds {
        query = query.bind(kind);
    }
    let rows = query.fetch_all(pool).await?;
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
/// `[since_ms, until_ms]`, ordered by `(ts, session, seq)` — the
/// `kian self inspect` window read: one bounded slice of the stream,
/// never the full history. The kind list is the report's compile-time
/// set, never user input.
pub(crate) async fn events_in_window(
    pool: &SqlitePool,
    kinds: &[&str],
    since_ms: u64,
    until_ms: u64,
) -> Result<Vec<Event>, StoreError> {
    let placeholders = vec!["?"; kinds.len()].join(", ");
    let sql = format!(
        "SELECT session, version, envelope FROM events WHERE kind IN ({placeholders}) AND ts >= ? AND ts <= ? ORDER BY ts ASC, session ASC, seq ASC"
    );
    let mut query = sqlx::query_as::<_, (String, i64, String)>(&sql);
    for kind in kinds {
        query = query.bind(kind);
    }
    let rows = query
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
/// `[since_ms, until_ms]`, kind-ascending — the report's volume
/// section without decoding a single envelope (the `(kind, ts)` index
/// serves it).
pub(crate) async fn event_kind_counts(
    pool: &SqlitePool,
    since_ms: u64,
    until_ms: u64,
) -> Result<Vec<(String, u64)>, StoreError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind, COUNT(*) FROM events WHERE ts >= ? AND ts <= ? GROUP BY kind ORDER BY kind ASC",
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

/// The incremental publication (R9-10): ONE transaction that deletes
/// the gone sessions' rows, upserts the changed sessions' rows and
/// flips `view_state` to the new digest + pinned key. Same atomic
/// shape as [`view_publish`] — a crash leaves the previous complete
/// version or the new one, never half a view — but the writes are
/// proportional to the change, not to the row count.
pub(crate) async fn view_publish_delta(
    pool: &SqlitePool,
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

// -- durable cron state (CRON-MODEL §4/§9, migrations v7-v8) ----------------
// The sqlite twin of the pg statements below. Fire times are canonical
// UTC RFC 3339 `Z` TEXT, so `<` on them is chronological and the
// monotonic cursor guard needs no calendar math.

/// Idempotent by `declaration_digest`: the reset decision is made inside
/// this one statement against the *live* row, so a racing redeclaration
/// cannot resurrect a stale cursor. Target-table references on the right
/// of `DO UPDATE SET` are the pre-update row values (sqlite and pg agree).
const CRON_SCHEDULE_UPSERT_SQL: &str = "INSERT INTO cron_schedules (id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at)\nVALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?6)\nON CONFLICT (id) DO UPDATE SET\n    expr = excluded.expr,\n    tz = excluded.tz,\n    enabled = excluded.enabled,\n    cursor_fire_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        THEN cron_schedules.cursor_fire_at ELSE NULL END,\n    next_fire_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        THEN cron_schedules.next_fire_at ELSE NULL END,\n    declaration_digest = excluded.declaration_digest,\n    updated_at = CASE WHEN cron_schedules.declaration_digest = excluded.declaration_digest\n        AND cron_schedules.expr = excluded.expr\n        AND cron_schedules.tz = excluded.tz\n        AND cron_schedules.enabled = excluded.enabled\n        THEN cron_schedules.updated_at ELSE excluded.updated_at END";

const CRON_SCHEDULE_DIGEST_SQL: &str = "SELECT declaration_digest FROM cron_schedules WHERE id = ?";

const CRON_SCHEDULE_CURSOR_SQL: &str = "SELECT cursor_fire_at FROM cron_schedules WHERE id = ?";

const CRON_SCHEDULES_SQL: &str = "SELECT id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at FROM cron_schedules ORDER BY id ASC";

const CRON_SCHEDULE_SQL: &str = "SELECT id, expr, tz, declaration_digest, cursor_fire_at, next_fire_at, enabled, created_at, updated_at FROM cron_schedules WHERE id = ?";

/// Insert-if-absent: the PK on `firing_id` is the idempotency rule, so
/// `rows_affected() == 1` is exactly "this caller won the slot".
const CRON_DELETE_SCHEDULE_SQL: &str = "DELETE FROM cron_schedules WHERE id = ?1";

const CRON_FIRING_CLAIM_SQL: &str = "INSERT INTO cron_firings (firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session)\nVALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)\nON CONFLICT DO NOTHING";

/// Non-terminal rows only: a settled slot is never rewritten.
const CRON_FIRING_FINISH_SQL: &str = "UPDATE cron_firings\nSET state = ?2,\n    worktree_path = COALESCE(?3, worktree_path),\n    finished_at = ?4,\n    skip_reason = COALESCE(?5, skip_reason),\n    error = ?6\nWHERE firing_id = ?1 AND state IN ('admitted', 'running')";

/// The admit-then-advance guard (§4.3): the cursor only moves when it is
/// unset or strictly behind the slot, and only when that slot has a
/// firing record — the cursor may lag the truth, never lead it.
const CRON_ADVANCE_CURSOR_SQL: &str = "UPDATE cron_schedules\nSET cursor_fire_at = ?2,\n    next_fire_at = ?3,\n    updated_at = ?4\nWHERE id = ?1\n  AND (cursor_fire_at IS NULL OR cursor_fire_at < ?2)\n  AND EXISTS (SELECT 1 FROM cron_firings WHERE firing_id = ?5)";

const CRON_LIST_FIRINGS_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings\nWHERE (?1 IS NULL OR schedule_id = ?1)\nORDER BY fire_time DESC, firing_id DESC\nLIMIT ?2";

const CRON_LOAD_FIRING_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings WHERE firing_id = ?";

/// The settlement sweep's read (CRON-MODEL §9.1): every firing of one
/// schedule that is still open, oldest first. A bounded-by-age read is not
/// enough — the pre-fix wedge left one `admitted` row stranded behind an
/// unbounded run of later `skipped` rows, so the sweep must find it wherever
/// it is in the history, not only within the newest page.
const CRON_OPEN_FIRINGS_SQL: &str = "SELECT firing_id, schedule_id, fire_time, admitted_at, state, skip_reason, worktree_path, finished_at, error, session\nFROM cron_firings\nWHERE schedule_id = ?1 AND state IN ('admitted', 'running')\nORDER BY fire_time ASC, firing_id ASC";

/// One idempotent declaration upsert. The pre-read digest is for the
/// caller's *report* only (Inserted / Unchanged / Redeclared): the state
/// change is decided by the statement itself, so a race can only make
/// the report approximate, never the cursor wrong.
pub(crate) async fn cron_upsert_schedule(
    pool: &SqlitePool,
    p: &crate::cron::ScheduleInsert<'_>,
) -> Result<Option<String>, StoreError> {
    let mut tx = pool.begin().await?;
    let previous: Option<String> = sqlx::query_scalar(CRON_SCHEDULE_DIGEST_SQL)
        .bind(p.id)
        .fetch_optional(&mut *tx)
        .await?;
    sqlx::query(CRON_SCHEDULE_UPSERT_SQL)
        .bind(p.id)
        .bind(p.expr)
        .bind(p.tz)
        .bind(p.digest)
        .bind(p.enabled)
        .bind(p.now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(previous)
}

pub(crate) async fn cron_load_schedules(
    pool: &SqlitePool,
) -> Result<Vec<crate::cron::ScheduleCols>, StoreError> {
    let rows: Vec<crate::cron::ScheduleCols> =
        sqlx::query_as(CRON_SCHEDULES_SQL).fetch_all(pool).await?;
    Ok(rows)
}

pub(crate) async fn cron_load_schedule(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<crate::cron::ScheduleCols>, StoreError> {
    let row: Option<crate::cron::ScheduleCols> = sqlx::query_as(CRON_SCHEDULE_SQL)
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn cron_delete_schedule(pool: &SqlitePool, id: &str) -> Result<bool, StoreError> {
    let res = sqlx::query(CRON_DELETE_SCHEDULE_SQL)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() == 1)
}

pub(crate) async fn cron_claim_firing(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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

/// Advances the cursor, then explains a refusal: an unknown schedule is a
/// caller bug, an already-at-or-past cursor is an idempotent no-op, and a
/// slot with no firing record is the admit-then-advance violation.
pub(crate) async fn cron_advance_cursor(
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
    firing_id: &str,
) -> Result<Option<crate::cron::FiringCols>, StoreError> {
    let row: Option<crate::cron::FiringCols> = sqlx::query_as(CRON_LOAD_FIRING_SQL)
        .bind(firing_id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub(crate) async fn cron_open_firings(
    pool: &SqlitePool,
    schedule_id: &str,
) -> Result<Vec<crate::cron::FiringCols>, StoreError> {
    let rows: Vec<crate::cron::FiringCols> = sqlx::query_as(CRON_OPEN_FIRINGS_SQL)
        .bind(schedule_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Record where a claimed firing is dispatched (CRON-MODEL §3.2/§9.1). The
/// `state IN ('admitted','running')` guard makes it a no-op on an already
/// settled row — a settlement race, never an error.
const CRON_SET_FIRING_SESSION_SQL: &str = "UPDATE cron_firings SET session = ?2 WHERE firing_id = ?1 AND state IN ('admitted', 'running')";

pub(crate) async fn cron_set_firing_session(
    pool: &SqlitePool,
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
