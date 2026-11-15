//! Export-only retention: the archive marker, the eligibility scan, and
//! the single-transaction marker-CAS + online-row delete (READ-MODEL
//! §7, P2b).
//!
//! Retention is **opt-in and default off**; there is no transparent
//! cold tier — a session leaves the online store only through a
//! verified archive package (`events.jsonl` + `inputs.jsonl` +
//! `manifest.json`, built and verified by the caller in `kian-cli`) and
//! comes back only through `kian session import`. The events and inputs
//! rows are deleted in exactly one place,
//! [`EventStore::archive_commit`], which:
//!
//! 1. runs only for a **terminal** session (the eligibility scan
//!    [`EventStore::archive_candidates`] lists sessions whose last
//!    committed event is `session/ended`);
//! 2. takes the marker insert and the online-row delete as **one
//!    transaction** — a crash leaves the online rows and no marker, or
//!    the marker and no online rows, never anything in between;
//! 3. CAS-fences the online stream: the delete proceeds only when the
//!    online event count still equals the count the caller verified
//!    against the package (a session that appended after verification —
//!    a resume — aborts the commit with
//!    [`ArchiveError::OnlineMoved`]);
//! 4. never deletes before a verified package exists — the store API
//!    takes the package's hashes as part of the marker, and the delete
//!    statements below exist only inside this guarded transaction.
//!
//! The marker row is infrastructure, not truth (R1): it names the
//! package; the package carries the data. Reads of an archived session
//! answer the typed `archived` error (`-32007`) instead of scanning
//! empty tables — an explicit refusal, never a silent truncation (R4).
//!
//! The inbox doctrine is amended, not violated: an input row stays
//! audit-visible in the online store **or inside the verified archive
//! package** (READ-MODEL §7's supersession of the old "audit-visible
//! forever in the store" wording).

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use sqlx::PgPool;
use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

use crate::event_store::{EventStore, StoreError};

/// The archived marker row (migration v5): the verified package's
/// identity and the online range it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedMarker {
    pub session: SessionId,
    /// The last committed seq the package covers (`head - 1` at archive
    /// time).
    pub last_seq: u64,
    pub event_count: u64,
    pub input_count: u64,
    /// Absolute path of the verified package directory.
    pub package_path: String,
    /// sha256 hex over the package's `events.jsonl`.
    pub events_sha256: String,
    /// sha256 hex over the package's `inputs.jsonl`.
    pub inputs_sha256: String,
    /// sha256 hex over the package's `manifest.json` bytes.
    pub manifest_sha256: String,
    /// unix epoch ms the marker landed.
    pub archived_at: u64,
}

/// One terminal session the retention job may consider (READ-MODEL §7:
/// eligibility is terminal-only; the online window by age and/or bytes
/// is the caller's policy over these facts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveCandidate {
    pub session: SessionId,
    /// The `session/ended` event's ts — the age anchor.
    pub ended_ts: u64,
    pub last_seq: u64,
    pub event_count: u64,
    pub input_count: u64,
    /// Bytes of envelope storage the session's events occupy online.
    pub event_bytes: u64,
    /// Bytes of payload storage the session's input rows occupy online.
    pub input_bytes: u64,
    /// The terminal state string (`completed` / `cancelled` /
    /// `interrupted` / `failed`).
    pub terminal_state: String,
}

/// One full inbox row as archived: everything the `inputs` table holds,
/// including the audit columns the public [`kian_core::model::InputRow`]
/// does not carry (`consumed_by_seq`, `admitted_seq`), so an import
/// restores the row exactly as it was.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArchivedInput {
    pub id: Uuid,
    pub session: SessionId,
    pub payload: serde_json::Value,
    pub admitted_at: u64,
    pub eligible_at: u64,
    pub idempotency_key: Option<String>,
    pub consumed_by_seq: Option<u64>,
    pub admitted_seq: u64,
}

/// Typed archive failures — a refused retention step is a matchable
/// fact, not an opaque string (the store discipline).
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The online stream moved between verification and commit (the
    /// session resumed and appended): nothing was deleted.
    #[error(
        "session {session:?} moved online after verification: expected {expected_count} events, found {actual_count}"
    )]
    OnlineMoved {
        session: SessionId,
        expected_count: u64,
        actual_count: u64,
    },
    /// A different package already archived this session (first marker
    /// wins); nothing was deleted.
    #[error(
        "session {session:?} already archived by a different package (marker {existing_manifest}, attempted {attempted_manifest})"
    )]
    MarkerConflict {
        session: SessionId,
        existing_manifest: String,
        attempted_manifest: String,
    },
    /// Commit called with no online rows and no matching marker — an
    /// inconsistent store state, refused rather than guessed at.
    #[error(
        "archive commit for session {session:?}: no online rows and no marker — nothing to commit"
    )]
    NothingToCommit { session: SessionId },
    #[error("u64 field {field} overflows the backend integer")]
    IntOverflow { field: &'static str },
    #[error("corrupt archived input row (session {session:?}): {reason}")]
    CorruptInput { session: SessionId, reason: String },
}

/// The outcome of [`EventStore::archive_commit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveCommitOutcome {
    /// Marker landed and the online rows were deleted in the same
    /// transaction.
    Committed,
    /// The session is already archived by this exact package (idempotent
    /// retry) — a no-op.
    AlreadyArchived,
}

/// sqlx failures flow through the store's typed Sql error, so a plain
/// `?` on a query maps into the archive error space unchanged.
impl From<sqlx::Error> for ArchiveError {
    fn from(e: sqlx::Error) -> Self {
        ArchiveError::Store(StoreError::Sql(e))
    }
}

fn to_i64(v: u64, field: &'static str) -> Result<i64, ArchiveError> {
    i64::try_from(v).map_err(|_| ArchiveError::IntOverflow { field })
}

fn to_u64(v: i64, field: &'static str) -> Result<u64, ArchiveError> {
    u64::try_from(v).map_err(|_| ArchiveError::IntOverflow { field })
}

/// True when a read error means "the v5 table does not exist on this
/// store" (a read-only open of a store last migrated before v5) — the
/// same shapes [`crate::views`] recognizes, applied to
/// `archived_sessions`: "no table" reads as "not archived", anything
/// else is a real error.
fn marker_table_absent(e: &StoreError) -> bool {
    let msg = e.to_string();
    msg.contains("no such table") || msg.contains("does not exist")
}

/// The `session/ended` payload's reason as the terminal-state string.
fn terminal_state(reason: &EndReason) -> &'static str {
    match reason {
        EndReason::Completed => "completed",
        EndReason::Cancelled => "cancelled",
        EndReason::Interrupted => "interrupted",
        EndReason::Failed => "failed",
        EndReason::AwaitingInput => "awaiting_input",
    }
}

impl EventStore {
    /// The session's archived marker, `None` when the session is not
    /// archived (or the v5 table does not exist on this read-only
    /// store). This is the read surfaces' first check: an archived
    /// session answers the typed `archived` error, never an empty scan.
    pub async fn archived_session(
        &self,
        session: SessionId,
    ) -> Result<Option<ArchivedMarker>, StoreError> {
        let key = session.0.to_string();
        const MARKER_SQL: &str = "SELECT last_seq, event_count, input_count, archived_at, \
             package_path, events_sha256, inputs_sha256, manifest_sha256 \
             FROM archived_sessions WHERE session = ";
        let row = match self {
            EventStore::Sqlite(pool) => {
                sqlx::query_as::<_, (i64, i64, i64, i64, String, String, String, String)>(
                    &(MARKER_SQL.to_string() + "?1"),
                )
                .bind(&key)
                .fetch_optional(pool)
                .await
            }
            EventStore::Postgres(pool) => {
                sqlx::query_as::<_, (i64, i64, i64, i64, String, String, String, String)>(
                    &(MARKER_SQL.to_string() + "$1"),
                )
                .bind(&key)
                .fetch_optional(pool)
                .await
            }
        };
        match row {
            Ok(Some(r)) => Ok(Some(decode_marker(session, r))),
            Ok(None) => Ok(None),
            Err(e) => {
                let se = StoreError::Sql(e);
                if marker_table_absent(&se) {
                    Ok(None)
                } else {
                    Err(se)
                }
            }
        }
    }

    /// Every session eligible for retention consideration: terminal
    /// (last committed event is `session/ended`) and still online.
    /// Sessions already archived (marker present, rows gone) no longer
    /// appear — the events table has nothing to list. The age/bytes
    /// window is the caller's policy over [`ArchiveCandidate`] facts.
    pub async fn archive_candidates(&self) -> Result<Vec<ArchiveCandidate>, ArchiveError> {
        /// Terminal sessions: the last committed event per session is
        /// `session/ended` (the `kind` projection column; the envelope
        /// is parsed only for the reason).
        const TERMINAL_SQL: &str = "SELECT e.session, e.ts, e.seq, e.envelope FROM events e \
            WHERE e.kind = 'session/ended' \
            AND e.seq = (SELECT MAX(x.seq) FROM events x WHERE x.session = e.session) \
            ORDER BY e.session ASC";
        let rows: Vec<(String, i64, i64, String)> = match self {
            EventStore::Sqlite(pool) => sqlx::query_as(TERMINAL_SQL).fetch_all(pool).await?,
            EventStore::Postgres(pool) => sqlx::query_as(TERMINAL_SQL).fetch_all(pool).await?,
        };
        let mut candidates = Vec::with_capacity(rows.len());
        for (session, ts, seq, envelope) in rows {
            let uuid = Uuid::parse_str(&session).map_err(|e| ArchiveError::CorruptInput {
                session: SessionId(Uuid::nil()),
                reason: format!("terminal scan session id: {e}"),
            })?;
            let sid = SessionId(uuid);
            let event: Event = serde_json::from_str(&envelope)
                .map_err(|e| ArchiveError::Store(StoreError::Serde(e)))?;
            let reason = match &event.kind {
                EventKind::SessionEnded { reason } => reason,
                // The kind column and the envelope disagreed — corrupt.
                _ => {
                    return Err(ArchiveError::Store(StoreError::CorruptRow {
                        session: sid,
                        seq: event.seq,
                    }));
                }
            };
            let (event_count, event_bytes) = self.archive_row_stats(&sid, true).await?;
            let (input_count, input_bytes) = self.archive_row_stats(&sid, false).await?;
            candidates.push(ArchiveCandidate {
                session: sid,
                ended_ts: to_u64(ts, "ended_ts")?,
                last_seq: to_u64(seq, "last_seq")?,
                event_count,
                input_count,
                event_bytes,
                input_bytes,
                terminal_state: terminal_state(reason).to_string(),
            });
        }
        Ok(candidates)
    }

    /// `(count, bytes)` of a session's online events or input rows.
    async fn archive_row_stats(
        &self,
        session: &SessionId,
        events_table: bool,
    ) -> Result<(u64, u64), ArchiveError> {
        let key = session.0.to_string();
        const EVENTS_SQL: &str =
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(envelope)), 0) FROM events WHERE session = ";
        const INPUTS_SQL: &str =
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM inputs WHERE session = ";
        let row: (i64, i64) = match self {
            EventStore::Sqlite(pool) => {
                if events_table {
                    sqlx::query_as(&(EVENTS_SQL.to_string() + "?1"))
                        .bind(&key)
                        .fetch_one(pool)
                        .await?
                } else {
                    sqlx::query_as(&(INPUTS_SQL.to_string() + "?1"))
                        .bind(&key)
                        .fetch_one(pool)
                        .await?
                }
            }
            EventStore::Postgres(pool) => {
                if events_table {
                    sqlx::query_as(&(EVENTS_SQL.to_string() + "$1"))
                        .bind(&key)
                        .fetch_one(pool)
                        .await?
                } else {
                    sqlx::query_as(&(INPUTS_SQL.to_string() + "$1"))
                        .bind(&key)
                        .fetch_one(pool)
                        .await?
                }
            }
        };
        Ok((to_u64(row.0, "count")?, to_u64(row.1, "bytes")?))
    }

    /// Every input row of the session — the full table row, including
    /// the audit columns — in admission order. This is the package
    /// writer's source; the read is also legal on a read-only store.
    pub async fn archived_inputs_all(
        &self,
        session: SessionId,
    ) -> Result<Vec<ArchivedInput>, ArchiveError> {
        let key = session.0.to_string();
        /// The raw inputs-table row (column order matches the SELECT).
        type InputCols = (
            String,
            String,
            String,
            i64,
            i64,
            Option<String>,
            Option<i64>,
            i64,
        );
        const INPUTS_SQL: &str = "SELECT id, session, payload, admitted_at, eligible_at, \
             idempotency_key, consumed_by_seq, admitted_seq FROM inputs WHERE session = ";
        let rows: Vec<InputCols> = match self {
            EventStore::Sqlite(pool) => {
                sqlx::query_as(&(INPUTS_SQL.to_string() + "?1 ORDER BY admitted_seq ASC"))
                    .bind(&key)
                    .fetch_all(pool)
                    .await?
            }
            EventStore::Postgres(pool) => {
                sqlx::query_as(&(INPUTS_SQL.to_string() + "$1 ORDER BY admitted_seq ASC"))
                    .bind(&key)
                    .fetch_all(pool)
                    .await?
            }
        };
        rows.into_iter()
            .map(|row| decode_archived_input(row, session))
            .collect()
    }

    /// Restores full input rows verbatim (the `session import` path).
    /// The caller must have refused an existing session first; the
    /// insert preserves every column, so a restored row is byte-equal
    /// to the archived one.
    pub async fn archive_inputs_insert(&self, rows: &[ArchivedInput]) -> Result<u64, ArchiveError> {
        let mut inserted = 0u64;
        for r in rows {
            let payload = serde_json::to_string(&r.payload)
                .map_err(|e| ArchiveError::Store(StoreError::Serde(e)))?;
            let id = r.id.to_string();
            let session = r.session.0.to_string();
            let admitted_at = to_i64(r.admitted_at, "admitted_at")?;
            let eligible_at = to_i64(r.eligible_at, "eligible_at")?;
            let admitted_seq = to_i64(r.admitted_seq, "admitted_seq")?;
            let consumed = r
                .consumed_by_seq
                .map(|v| to_i64(v, "consumed_by_seq"))
                .transpose()?;
            const INSERT_SQL: &str = "INSERT INTO inputs (id, session, payload, admitted_at, \
                 eligible_at, idempotency_key, consumed_by_seq, admitted_seq) VALUES (";
            match self {
                EventStore::Sqlite(pool) => {
                    sqlx::query(&(INSERT_SQL.to_string() + "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"))
                        .bind(&id)
                        .bind(&session)
                        .bind(&payload)
                        .bind(admitted_at)
                        .bind(eligible_at)
                        .bind(&r.idempotency_key)
                        .bind(consumed)
                        .bind(admitted_seq)
                        .execute(pool)
                        .await?;
                }
                EventStore::Postgres(pool) => {
                    sqlx::query(&(INSERT_SQL.to_string() + "$1, $2, $3, $4, $5, $6, $7, $8)"))
                        .bind(&id)
                        .bind(&session)
                        .bind(&payload)
                        .bind(admitted_at)
                        .bind(eligible_at)
                        .bind(&r.idempotency_key)
                        .bind(consumed)
                        .bind(admitted_seq)
                        .execute(pool)
                        .await?;
                }
            }
            inserted += 1;
        }
        Ok(inserted)
    }

    /// The two-phase commit's atomic core (READ-MODEL §7): insert the
    /// archived marker under CAS and delete the session's online rows
    /// in ONE transaction. `expected_event_count` is the count the
    /// caller's verified package covers — a session that appended since
    /// (a resume) aborts with [`ArchiveError::OnlineMoved`] and nothing
    /// is deleted. Retrying a completed archive with the same package
    /// is the idempotent [`ArchiveCommitOutcome::AlreadyArchived`].
    ///
    /// This is the ONLY code in the crate that removes `events`/`inputs`
    /// rows; it exists solely behind the verified-package guard (the
    /// review assertion in `event_store.rs` carves out exactly this
    /// module for it).
    pub async fn archive_commit(
        &self,
        marker: &ArchivedMarker,
        expected_event_count: u64,
    ) -> Result<ArchiveCommitOutcome, ArchiveError> {
        let key = marker.session.0.to_string();
        let args = CommitArgs {
            key: &key,
            last_seq: to_i64(marker.last_seq, "last_seq")?,
            event_count: to_i64(marker.event_count, "event_count")?,
            input_count: to_i64(marker.input_count, "input_count")?,
            archived_at: to_i64(marker.archived_at, "archived_at")?,
            expected: to_i64(expected_event_count, "expected_event_count")?,
            package_path: &marker.package_path,
            events_sha256: &marker.events_sha256,
            inputs_sha256: &marker.inputs_sha256,
            manifest_sha256: &marker.manifest_sha256,
            session: marker.session,
        };
        match self {
            EventStore::Sqlite(pool) => commit_sqlite(pool, &args).await,
            EventStore::Postgres(pool) => commit_pg(pool, &args).await,
        }
    }
}

/// Everything the commit transaction needs, backend-independent.
struct CommitArgs<'a> {
    key: &'a str,
    last_seq: i64,
    event_count: i64,
    input_count: i64,
    archived_at: i64,
    expected: i64,
    package_path: &'a str,
    events_sha256: &'a str,
    inputs_sha256: &'a str,
    manifest_sha256: &'a str,
    session: SessionId,
}

fn decode_marker(
    session: SessionId,
    row: (i64, i64, i64, i64, String, String, String, String),
) -> ArchivedMarker {
    let u = |v: i64| u64::try_from(v).unwrap_or(0);
    ArchivedMarker {
        session,
        last_seq: u(row.0),
        event_count: u(row.1),
        input_count: u(row.2),
        archived_at: u(row.3),
        package_path: row.4,
        events_sha256: row.5,
        inputs_sha256: row.6,
        manifest_sha256: row.7,
    }
}

fn decode_archived_input(
    row: (
        String,
        String,
        String,
        i64,
        i64,
        Option<String>,
        Option<i64>,
        i64,
    ),
    expect_session: SessionId,
) -> Result<ArchivedInput, ArchiveError> {
    let (id, session, payload, admitted_at, eligible_at, key, consumed, admitted_seq) = row;
    let corrupt = |reason: String| ArchiveError::CorruptInput {
        session: expect_session,
        reason,
    };
    let id = Uuid::parse_str(&id).map_err(|e| corrupt(format!("input id: {e}")))?;
    let sid =
        SessionId(Uuid::parse_str(&session).map_err(|e| corrupt(format!("session id: {e}")))?);
    Ok(ArchivedInput {
        id,
        session: sid,
        payload: serde_json::from_str(&payload).map_err(|e| corrupt(format!("payload: {e}")))?,
        admitted_at: to_u64(admitted_at, "admitted_at")?,
        eligible_at: to_u64(eligible_at, "eligible_at")?,
        idempotency_key: key,
        consumed_by_seq: consumed.map(|v| to_u64(v, "consumed_by_seq")).transpose()?,
        admitted_seq: to_u64(admitted_seq, "admitted_seq")?,
    })
}

/// The sqlite commit: one transaction, marker CAS first, deletes last.
async fn commit_sqlite(
    pool: &SqlitePool,
    a: &CommitArgs<'_>,
) -> Result<ArchiveCommitOutcome, ArchiveError> {
    let mut tx = pool.begin().await?;
    let (online,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE session = ?1")
        .bind(a.key)
        .fetch_one(&mut *tx)
        .await?;
    if online == 0 {
        let out = finish_idempotent_sqlite(&mut tx, a).await?;
        tx.commit().await?;
        return Ok(out);
    }
    if online != a.expected {
        return Err(ArchiveError::OnlineMoved {
            session: a.session,
            expected_count: a.expected.max(0) as u64,
            actual_count: online.max(0) as u64,
        });
    }
    let inserted = sqlx::query(
        "INSERT INTO archived_sessions (session, last_seq, event_count, input_count, \
         package_path, events_sha256, inputs_sha256, manifest_sha256, archived_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
         ON CONFLICT(session) DO NOTHING",
    )
    .bind(a.key)
    .bind(a.last_seq)
    .bind(a.event_count)
    .bind(a.input_count)
    .bind(a.package_path)
    .bind(a.events_sha256)
    .bind(a.inputs_sha256)
    .bind(a.manifest_sha256)
    .bind(a.archived_at)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        // First marker wins: a different package's marker is the typed
        // conflict (raised by marker_matches_*); ours means a prior
        // commit finished — fall through and complete the delete.
        marker_matches_sqlite(&mut tx, a).await?;
    }
    delete_online_rows_sqlite(&mut tx, a.key).await?;
    tx.commit().await?;
    Ok(ArchiveCommitOutcome::Committed)
}

/// The postgres commit: same shape, `$n` placeholders.
async fn commit_pg(
    pool: &PgPool,
    a: &CommitArgs<'_>,
) -> Result<ArchiveCommitOutcome, ArchiveError> {
    let mut tx = pool.begin().await?;
    let (online,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE session = $1")
        .bind(a.key)
        .fetch_one(&mut *tx)
        .await?;
    if online == 0 {
        let out = finish_idempotent_pg(&mut tx, a).await?;
        tx.commit().await?;
        return Ok(out);
    }
    if online != a.expected {
        return Err(ArchiveError::OnlineMoved {
            session: a.session,
            expected_count: a.expected.max(0) as u64,
            actual_count: online.max(0) as u64,
        });
    }
    let inserted = sqlx::query(
        "INSERT INTO archived_sessions (session, last_seq, event_count, input_count, \
         package_path, events_sha256, inputs_sha256, manifest_sha256, archived_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT(session) DO NOTHING",
    )
    .bind(a.key)
    .bind(a.last_seq)
    .bind(a.event_count)
    .bind(a.input_count)
    .bind(a.package_path)
    .bind(a.events_sha256)
    .bind(a.inputs_sha256)
    .bind(a.manifest_sha256)
    .bind(a.archived_at)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        marker_matches_pg(&mut tx, a).await?;
    }
    delete_online_rows_pg(&mut tx, a.key).await?;
    tx.commit().await?;
    Ok(ArchiveCommitOutcome::Committed)
}

/// True when the existing marker names this exact package. On mismatch
/// the caller builds the typed conflict from a fresh read below.
async fn marker_matches_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    a: &CommitArgs<'_>,
) -> Result<bool, ArchiveError> {
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT manifest_sha256 FROM archived_sessions WHERE session = ?1")
            .bind(a.key)
            .fetch_optional(&mut **tx)
            .await?;
    match existing {
        Some((h,)) if h == a.manifest_sha256 => Ok(true),
        Some((h,)) => Err(ArchiveError::MarkerConflict {
            session: a.session,
            existing_manifest: h,
            attempted_manifest: a.manifest_sha256.to_string(),
        }),
        // insert reported zero rows yet no marker exists — impossible
        // inside this transaction; treat as inconsistent.
        None => Err(ArchiveError::NothingToCommit { session: a.session }),
    }
}

async fn marker_matches_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    a: &CommitArgs<'_>,
) -> Result<bool, ArchiveError> {
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT manifest_sha256 FROM archived_sessions WHERE session = $1")
            .bind(a.key)
            .fetch_optional(&mut **tx)
            .await?;
    match existing {
        Some((h,)) if h == a.manifest_sha256 => Ok(true),
        Some((h,)) => Err(ArchiveError::MarkerConflict {
            session: a.session,
            existing_manifest: h,
            attempted_manifest: a.manifest_sha256.to_string(),
        }),
        None => Err(ArchiveError::NothingToCommit { session: a.session }),
    }
}

/// The online-row delete. Reachable ONLY from the two commit functions
/// above — after the marker CAS inside the same open transaction. The
/// disposable projection tables (snapshots, views) ride the same
/// transaction; the claims row stays (its epoch is a durable
/// high-water mark, not session data).
async fn delete_online_rows_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key: &str,
) -> Result<(), ArchiveError> {
    sqlx::query("DELETE FROM events WHERE session = ?1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM inputs WHERE session = ?1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM projection_snapshots WHERE session = ?1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM view_rows WHERE session = ?1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn delete_online_rows_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &str,
) -> Result<(), ArchiveError> {
    sqlx::query("DELETE FROM events WHERE session = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM inputs WHERE session = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM projection_snapshots WHERE session = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM view_rows WHERE session = $1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Zero online rows: idempotent no-op iff the marker matches this
/// package; a different package's marker is a conflict; no marker at
/// all is an inconsistent store, refused.
async fn finish_idempotent_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    a: &CommitArgs<'_>,
) -> Result<ArchiveCommitOutcome, ArchiveError> {
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT manifest_sha256 FROM archived_sessions WHERE session = ?1")
            .bind(a.key)
            .fetch_optional(&mut **tx)
            .await?;
    match existing {
        Some((h,)) if h == a.manifest_sha256 => Ok(ArchiveCommitOutcome::AlreadyArchived),
        Some((h,)) => Err(ArchiveError::MarkerConflict {
            session: a.session,
            existing_manifest: h,
            attempted_manifest: a.manifest_sha256.to_string(),
        }),
        None => Err(ArchiveError::NothingToCommit { session: a.session }),
    }
}

async fn finish_idempotent_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    a: &CommitArgs<'_>,
) -> Result<ArchiveCommitOutcome, ArchiveError> {
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT manifest_sha256 FROM archived_sessions WHERE session = $1")
            .bind(a.key)
            .fetch_optional(&mut **tx)
            .await?;
    match existing {
        Some((h,)) if h == a.manifest_sha256 => Ok(ArchiveCommitOutcome::AlreadyArchived),
        Some((h,)) => Err(ArchiveError::MarkerConflict {
            session: a.session,
            existing_manifest: h,
            attempted_manifest: a.manifest_sha256.to_string(),
        }),
        None => Err(ArchiveError::NothingToCommit { session: a.session }),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, InputRow, SessionId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{ArchiveCommitOutcome, ArchiveError, ArchivedMarker, EventStore};

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-arch-{}.sqlite", Uuid::new_v4()))
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

    fn guard(epoch: Epoch, seq: u64) -> crate::event_store::AppendGuard {
        crate::event_store::AppendGuard {
            epoch,
            expected_seq: seq,
        }
    }

    /// A terminal stream: created, one turn, then `session/ended`.
    async fn seed_terminal(store: &EventStore, sid: SessionId) {
        let e = Epoch(1);
        store
            .append(sid, ev(sid, 1, e, EventKind::SessionCreated), guard(e, 1))
            .await
            .unwrap();
        store
            .append(sid, ev(sid, 2, e, EventKind::TurnStart), guard(e, 2))
            .await
            .unwrap();
        store
            .append_end(
                sid,
                ev(
                    sid,
                    3,
                    e,
                    EventKind::SessionEnded {
                        reason: EndReason::Completed,
                    },
                ),
                guard(e, 3),
            )
            .await
            .unwrap();
    }

    fn marker(sid: SessionId, manifest: &str) -> ArchivedMarker {
        ArchivedMarker {
            session: sid,
            last_seq: 3,
            event_count: 3,
            input_count: 0,
            package_path: format!("/tmp/pkg/{}", sid.0),
            events_sha256: "e".repeat(64),
            inputs_sha256: "i".repeat(64),
            manifest_sha256: manifest.to_string(),
            archived_at: 1_761_900_000_123,
        }
    }

    async fn row_count(store: &EventStore, sql: &str, key: &str) -> i64 {
        let (n,): (i64,) = sqlx::query_as(sql)
            .bind(key)
            .fetch_one(store.pool())
            .await
            .unwrap();
        n
    }

    /// Terminal sessions (last event `session/ended`) and only they are
    /// candidates, with the full count/bytes facts.
    #[tokio::test]
    async fn candidates_lists_terminal_sessions_only() {
        let (store, path) = test_store().await;
        let terminal = SessionId::new();
        let live = SessionId::new();
        seed_terminal(&store, terminal).await;
        let e = Epoch(1);
        store
            .append(live, ev(live, 1, e, EventKind::SessionCreated), guard(e, 1))
            .await
            .unwrap();

        let candidates = store.archive_candidates().await.unwrap();
        assert_eq!(candidates.len(), 1, "only the terminal session");
        let c = &candidates[0];
        assert_eq!(c.session, terminal);
        assert_eq!(c.last_seq, 3);
        assert_eq!(c.event_count, 3);
        assert_eq!(c.terminal_state, "completed");
        assert_eq!(c.ended_ts, 1_761_900_000_003);
        assert!(c.event_bytes > 0);

        store.close().await;
        cleanup(&path);
    }

    /// A mid-stream `session/ended` (not the tail) is NOT terminal: the
    /// end write is always the tail by construction (NoHalfTerminated),
    /// and the scan honors that.
    #[tokio::test]
    async fn candidates_ignores_a_mid_stream_end_event() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let e = Epoch(1);
        for (seq, kind) in [
            (1u64, EventKind::SessionCreated),
            (
                2,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
            ),
            (3, EventKind::TurnStart),
        ] {
            store
                .append(sid, ev(sid, seq, e, kind), guard(e, seq))
                .await
                .unwrap();
        }
        assert!(store.archive_candidates().await.unwrap().is_empty());
        store.close().await;
        cleanup(&path);
    }

    /// The happy two-phase commit: marker + delete in one transaction,
    /// and the marker reads back through `archived_session`.
    #[tokio::test]
    async fn commit_deletes_online_rows_and_writes_the_marker_atomically() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed_terminal(&store, sid).await;
        let key = sid.0.to_string();

        let m = marker(sid, "aa");
        assert_eq!(
            store.archive_commit(&m, 3).await.unwrap(),
            ArchiveCommitOutcome::Committed
        );
        assert_eq!(
            row_count(
                &store,
                "SELECT COUNT(*) FROM events WHERE session = ?1",
                &key
            )
            .await,
            0
        );
        let read = store.archived_session(sid).await.unwrap().expect("marker");
        assert_eq!(read.manifest_sha256, "aa");
        assert_eq!(read.last_seq, 3);
        // The candidate scan no longer lists it (nothing online).
        assert!(store.archive_candidates().await.unwrap().is_empty());

        // Retry with the same package: the idempotent no-op.
        assert_eq!(
            store.archive_commit(&m, 3).await.unwrap(),
            ArchiveCommitOutcome::AlreadyArchived
        );
        store.close().await;
        cleanup(&path);
    }

    /// The CAS fence: a session that appended after the package was
    /// verified (a resume) is never deleted.
    #[tokio::test]
    async fn commit_refuses_an_online_stream_that_moved() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed_terminal(&store, sid).await;
        // The "verified" package covered 2 events; the stream has 3.
        let err = store
            .archive_commit(&marker(sid, "aa"), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, ArchiveError::OnlineMoved { .. }), "{err:?}");
        let key = sid.0.to_string();
        assert_eq!(
            row_count(
                &store,
                "SELECT COUNT(*) FROM events WHERE session = ?1",
                &key
            )
            .await,
            3,
            "nothing was deleted"
        );
        assert!(store.archived_session(sid).await.unwrap().is_none());
        store.close().await;
        cleanup(&path);
    }

    /// First marker wins: a different package's marker is a typed
    /// conflict and deletes nothing.
    #[tokio::test]
    async fn commit_refuses_a_conflicting_marker() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        seed_terminal(&store, sid).await;
        store
            .archive_commit(&marker(sid, "first"), 3)
            .await
            .unwrap();
        let err = store
            .archive_commit(&marker(sid, "second"), 0)
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                ArchiveError::MarkerConflict {
                    existing_manifest,
                    attempted_manifest,
                    ..
                } if existing_manifest == "first" && attempted_manifest == "second"
            ),
            "{err:?}"
        );
        // The original marker is untouched.
        assert_eq!(
            store
                .archived_session(sid)
                .await
                .expect("marker read")
                .expect("marker row")
                .manifest_sha256,
            "first"
        );
        store.close().await;
        cleanup(&path);
    }

    /// Commit with no online rows and no marker is refused — an honest
    /// error, never a silent no-op.
    #[tokio::test]
    async fn commit_without_online_rows_or_marker_is_refused() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let err = store
            .archive_commit(&marker(sid, "aa"), 0)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ArchiveError::NothingToCommit { .. }),
            "{err:?}"
        );
        store.close().await;
        cleanup(&path);
    }

    /// Full input rows round-trip: every audit column survives
    /// `archived_inputs_all` → `archive_inputs_insert`.
    #[tokio::test]
    async fn input_rows_round_trip_with_audit_columns() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        let row = InputRow {
            id: Uuid::new_v4(),
            session: sid,
            payload: json!({"hello": "world"}),
            admitted_at: 100,
            eligible_at: 100,
            idempotency_key: Some("k1".into()),
        };
        store.admit(row.clone()).await.unwrap();
        store
            .append(
                sid,
                ev(sid, 1, Epoch(1), EventKind::SessionCreated),
                guard(Epoch(1), 1),
            )
            .await
            .unwrap();
        store.mark_consumed(sid, row.id, 1).await.unwrap();

        let archived = store.archived_inputs_all(sid).await.unwrap();
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].id, row.id);
        assert_eq!(archived[0].consumed_by_seq, Some(1));
        assert!(archived[0].admitted_seq >= 1);

        // Restore into a fresh store: byte-equal rows.
        let (fresh, fresh_path) = test_store().await;
        let n = fresh.archive_inputs_insert(&archived).await.unwrap();
        assert_eq!(n, 1);
        let back = fresh.archived_inputs_all(sid).await.unwrap();
        assert_eq!(back, archived);
        fresh.close().await;
        cleanup(&fresh_path);
        store.close().await;
        cleanup(&path);
    }

    /// A read-only reader of a pre-v5 store (marker table absent) reads
    /// "not archived" — the views module's absent-table doctrine.
    /// The fixture is a hand-built v0-only database (the archived
    /// marker table never existed there).
    #[tokio::test]
    async fn absent_marker_table_reads_as_not_archived() {
        use sqlx::Connection;
        let path = temp_db_path();
        {
            let opts = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let mut conn = sqlx::sqlite::SqliteConnection::connect_with(&opts)
                .await
                .unwrap();
            let v0 = crate::migrations::chain()[0].up;
            sqlx::raw_sql(v0).execute(&mut conn).await.unwrap();
            conn.close().await.unwrap();
        }
        let sid = SessionId::new();
        let reader = EventStore::open_read_only(&path).await.unwrap();
        assert!(reader.archived_session(sid).await.unwrap().is_none());
        reader.close().await;
        cleanup(&path);
    }

    /// The marker reads back with every column intact.
    #[tokio::test]
    async fn marker_round_trips_every_column() {
        let (store, path) = test_store().await;
        let sid = SessionId::new();
        sqlx::query(
            "INSERT INTO archived_sessions (session, last_seq, event_count, input_count, \
             package_path, events_sha256, inputs_sha256, manifest_sha256, archived_at) \
             VALUES (?1, 7, 5, 2, '/pkg', 'ee', 'ii', 'mm', 42)",
        )
        .bind(sid.0.to_string())
        .execute(store.pool())
        .await
        .unwrap();
        let m = store.archived_session(sid).await.unwrap().expect("marker");
        assert_eq!(m.last_seq, 7);
        assert_eq!(m.event_count, 5);
        assert_eq!(m.input_count, 2);
        assert_eq!(m.package_path, "/pkg");
        assert_eq!(m.events_sha256, "ee");
        assert_eq!(m.inputs_sha256, "ii");
        assert_eq!(m.manifest_sha256, "mm");
        assert_eq!(m.archived_at, 42);
        store.close().await;
        cleanup(&path);
    }
}
