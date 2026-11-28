//! Durable command queue + endpoint registry (DESIGN §4.3/§7;
//! dual-backend since).
//!
//! Queue rows are **pre-event durable state**, exactly like inbox rows: the
//! row exists in the server's database before any dispatch flies
//! (admit-before-execute), and results become session events only through
//! the normal `tool/call`/`tool/result` loop path — never by writing the
//! events table from here.
//!
//! One server, one database: when the event store is postgres,
//! `endpoint_registry`/`exec_queue`/`async_jobs` live in the same postgres
//! database. The public [`ExecStore`] is a small dispatching enum —
//! `Sqlite(..) | Postgres(..)` — with the same method names and signatures
//! on both backends (the pattern: dispatch, not generics — sqlx cannot
//! usefully genericize over the two dialects). Per-backend SQL lives in
//! [`crate::sqlite`] and [`crate::postgres`]; `open`/`from_pool` stay
//! sqlite construction, `from_pg_pool`/`open_postgres` are the postgres
//! entry points.
//!
//! Statuses form a monotone little state machine:
//!
//! ```text
//! pending ──▶ inflight ──▶ done
//!    │ │
//!    └───────────┴──▶ abandoned (queue timeout / exhausted re-drive; terminal)
//! ```
//!
//! `done` and `abandoned` are terminal: every transition statement carries a
//! status guard (`WHERE status IN (...)`) so a committed `done` row is never
//! updated again and no row is ever deleted (asserted by
//! `no_delete_and_done_rows_are_never_mutated` and
//! `backend_modules_keep_guards_and_never_delete` — the review pattern).
//!
//! Re-drive is at-least-once and therefore **bounded** (ADR-033): every claim
//! increments `dispatch_attempts`, and a driver may only claim a row already
//! `inflight` while that count is below the caller's bound (`1 +
//! max_redrives`). A row whose result was lost to a transport loss gets its
//! retry; a row that keeps losing its result because the command itself
//! destroys the ack path (restarting the server, killing the worker) reaches
//! [`ExecStore::abandon_exhausted`] instead of looping forever.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use uuid::Uuid;

use kian_core::fs::{FsCommand, FsResult};
use kian_core::seams::{ExecCommand, ExecError, ExecResult};

use crate::proto::{CommandError, ErrorCode};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failures of the durable queue store itself (never a command outcome).
#[derive(Debug, thiserror::Error)]
pub enum QueueStoreError {
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("corrupt queue row: {0}")]
    Corrupt(String),
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// Lifecycle of one queued command. Monotone: `Done` and `Abandoned` are
/// terminal (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum QueueStatus {
    Pending,
    Inflight,
    Done,
    Abandoned,
}

impl QueueStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Inflight => "inflight",
            Self::Done => "done",
            Self::Abandoned => "abandoned",
        }
    }

    fn from_str(s: &str) -> Result<Self, QueueStoreError> {
        match s {
            "pending" => Ok(Self::Pending),
            "inflight" => Ok(Self::Inflight),
            "done" => Ok(Self::Done),
            "abandoned" => Ok(Self::Abandoned),
            other => Err(QueueStoreError::Corrupt(format!(
                "unknown queue status {other:?}"
            ))),
        }
    }
}

/// The correlated outcome of one command, as sent by the endpoint. Stored
/// verbatim on the row when it settles (`done`), so a re-execution of the
/// same command id after a restart is answered from the row — no second
/// dispatch (ADR-007 command-id dedupe).
#[derive(Debug, Clone, PartialEq)]
pub enum CommandOutcome {
    Ok { result: ExecResult },
    Fs { result: FsResult },
    Err { error: CommandError },
}

/// The two command families the durable queue carries (ADR-023). The
/// stored `kind` column disambiguates the payload and result JSON; rows
/// written before the v3 migration carry the default `exec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Exec,
    Fs,
}

impl CommandKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exec => "exec",
            Self::Fs => "fs",
        }
    }

    fn from_str(raw: &str) -> Result<Self, QueueStoreError> {
        match raw {
            "exec" => Ok(Self::Exec),
            "fs" => Ok(Self::Fs),
            other => Err(QueueStoreError::Corrupt(format!(
                "unknown command kind {other:?}"
            ))),
        }
    }
}

/// One queued worker command: the `exec` family (unchanged wire shape) or
/// the v2 filesystem family.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerCommand {
    Exec(ExecCommand),
    Fs(FsCommand),
}

impl From<ExecCommand> for WorkerCommand {
    fn from(cmd: ExecCommand) -> Self {
        Self::Exec(cmd)
    }
}

impl From<&ExecCommand> for WorkerCommand {
    fn from(cmd: &ExecCommand) -> Self {
        Self::Exec(cmd.clone())
    }
}

impl From<&WorkerCommand> for WorkerCommand {
    fn from(cmd: &WorkerCommand) -> Self {
        cmd.clone()
    }
}

impl WorkerCommand {
    #[must_use]
    pub fn kind(&self) -> CommandKind {
        match self {
            Self::Exec(_) => CommandKind::Exec,
            Self::Fs(_) => CommandKind::Fs,
        }
    }

    #[must_use]
    pub fn as_exec(&self) -> Option<&ExecCommand> {
        match self {
            Self::Exec(cmd) => Some(cmd),
            Self::Fs(_) => None,
        }
    }

    #[must_use]
    pub fn as_fs(&self) -> Option<&FsCommand> {
        match self {
            Self::Fs(cmd) => Some(cmd),
            Self::Exec(_) => None,
        }
    }

    fn to_cmd_json(&self) -> Result<String, QueueStoreError> {
        let value = match self {
            Self::Exec(cmd) => serde_json::to_value(cmd),
            Self::Fs(cmd) => serde_json::to_value(cmd),
        }
        .map_err(|e| QueueStoreError::Corrupt(format!("cmd not serializable: {e}")))?;
        serde_json::to_string(&value)
            .map_err(|e| QueueStoreError::Corrupt(format!("cmd not serializable: {e}")))
    }
}

impl CommandOutcome {
    /// Map onto the exec seam's error shape.
    pub fn to_exec(self) -> Result<ExecResult, ExecError> {
        match self {
            Self::Ok { result } => Ok(result),
            Self::Err { error } => Err(match error.code {
                ErrorCode::Spawn => ExecError::Spawn(error.message),
                ErrorCode::Timeout => ExecError::Timeout,
                ErrorCode::Io => ExecError::Io(error.message),
            }),
            Self::Fs { .. } => Err(ExecError::Io(
                "fs result on the exec path (internal)".into(),
            )),
        }
    }

    /// Map onto the fs tool's result shape (the fs family's counterpart).
    pub fn to_fs(self) -> Result<FsResult, ExecError> {
        match self {
            Self::Fs { result } => Ok(result),
            Self::Ok { .. } => Err(ExecError::Io(
                "exec result on the fs path (internal)".into(),
            )),
            Self::Err { error } => Err(match error.code {
                ErrorCode::Timeout => ExecError::Timeout,
                _ => ExecError::Io(error.message),
            }),
        }
    }

    pub(crate) fn to_json(&self) -> String {
        match self {
            Self::Ok { result } => serde_json::to_string(&serde_json::json!({ "ok": result }))
                .expect("exec results always serialize"),
            Self::Fs { result } => serde_json::to_string(&serde_json::json!({ "fs": result }))
                .expect("fs results always serialize"),
            Self::Err { error } => serde_json::to_string(&serde_json::json!({ "error": error }))
                .expect("command errors always serialize"),
        }
    }

    fn from_json(raw: &str, kind: CommandKind) -> Result<Self, QueueStoreError> {
        let value: serde_json::Value = serde_json::from_str(raw)
            .map_err(|e| QueueStoreError::Corrupt(format!("bad outcome json: {e}")))?;
        if let Some(error) = value.get("error") {
            return serde_json::from_value(error.clone())
                .map(|error| Self::Err { error })
                .map_err(|e| QueueStoreError::Corrupt(format!("bad err outcome: {e}")));
        }
        match kind {
            CommandKind::Exec => {
                let result = value.get("ok").ok_or_else(|| {
                    QueueStoreError::Corrupt("exec outcome carries no `ok`".into())
                })?;
                serde_json::from_value(result.clone())
                    .map(|result| Self::Ok { result })
                    .map_err(|e| QueueStoreError::Corrupt(format!("bad ok outcome: {e}")))
            }
            CommandKind::Fs => {
                let result = value
                    .get("fs")
                    .ok_or_else(|| QueueStoreError::Corrupt("fs outcome carries no `fs`".into()))?;
                serde_json::from_value(result.clone())
                    .map(|result| Self::Fs { result })
                    .map_err(|e| QueueStoreError::Corrupt(format!("bad fs outcome: {e}")))
            }
        }
    }
}

/// One `exec_queue` row.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedCommand {
    pub command_id: Uuid,
    pub endpoint_id: String,
    pub command: WorkerCommand,
    pub status: QueueStatus,
    pub tool_call_ref: Option<String>,
    /// Present iff `status == Done`.
    pub outcome: Option<CommandOutcome>,
    /// How many wire dispatches this row has been claimed for (ADR-033).
    /// `0` on a never-dispatched row; `1` after the first dispatch. A driver
    /// may only re-drive an `inflight` row while this is below its bound.
    pub dispatch_attempts: u32,
    pub created_at: i64,
    pub updated_at: i64,
}

/// One orphaned pending row converged by the queue's startup/sweep pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAbandonment {
    pub command_id: Uuid,
    pub endpoint_id: String,
    pub tool_call_ref: Option<String>,
    pub created_at: i64,
    pub dispatch_attempts: u32,
}

/// One `endpoint_registry` row (the watchdog reads this projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointRegistryRow {
    pub endpoint_id: String,
    pub addr: String,
    pub last_heartbeat_ms: u64,
    pub state: String,
    /// The worker's **last-declared** description, cached across restarts
    /// (ADR-025). `None` until the first registration declares one (or
    /// when the latest declaration omitted / was invalid). Never
    /// authoritative: the live directory wins while a worker is connected.
    pub description: Option<String>,
    /// The worker's last-declared protocol revision (ADR-025), cached for
    /// the cold-start catalog. `None` for ids known only from config.
    pub protocol_version: Option<u32>,
    /// The worker's last-declared **sanitized** capability list (v3,
    /// ADR-029), cached for the cold-start catalog. Empty means the latest
    /// declaration omitted it (or no declaration has happened yet).
    pub capabilities: Vec<String>,
}

/// Wall-clock milliseconds since the unix epoch (i64, matching the store's
/// timestamp columns).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Migrations (kian-exec's own adjacent chains, one per backend; the
// bookkeeping table `_kian_exec_migrations` keeps them independent of
// kian-store's chains in the same database)
// ---------------------------------------------------------------------------

/// One generation in the adjacent chain (same shape as `kian-store`'s
/// migrations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub up: &'static str,
}

/// v0 — the endpoint registry and the durable command queue.
/// Frozen golden: committed generations are never rewritten.
///
/// `exec_queue.result` stores the correlated outcome JSON once a row
/// settles, so command-id dedupe can answer re-executions from the row
/// across a restart (additive over the column list; see ADR-007).
const V0_REGISTRY_QUEUE: Migration = Migration {
    version: 0,
    name: "v0_endpoint_registry_exec_queue",
    up: "CREATE TABLE IF NOT EXISTS endpoint_registry (\n    endpoint_id TEXT PRIMARY KEY,\n    addr TEXT NOT NULL,\n    last_heartbeat_ms INTEGER NOT NULL,\n    state TEXT NOT NULL\n);\nCREATE TABLE IF NOT EXISTS exec_queue (\n    command_id TEXT PRIMARY KEY,\n    endpoint_id TEXT NOT NULL,\n    cmd TEXT NOT NULL,\n    status TEXT NOT NULL,\n    tool_call_ref TEXT,\n    result TEXT,\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_exec_queue_drive ON exec_queue (endpoint_id, status, created_at)",
};

/// v1 — the async job table (ADR-008). Frozen golden once
/// committed: the same append-only discipline as v0. Rows are
/// pre-event durable state per ADR-007's pattern — the row exists
/// before any dispatch, the completion reaches a session only as an
/// admitted input.
const V1_ASYNC_JOBS: Migration = Migration {
    version: 1,
    name: "v1_async_jobs",
    up: "CREATE TABLE IF NOT EXISTS async_jobs (\n    job_id TEXT PRIMARY KEY,\n    session TEXT NOT NULL,\n    endpoint_id TEXT NOT NULL,\n    cmd TEXT NOT NULL,\n    status TEXT NOT NULL,\n    tool_call_ref TEXT,\n    result TEXT,\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_async_jobs_drive ON async_jobs (status, created_at)",
};

/// v2 — queue-row lease columns (the designed chain extension
/// builds its row leases ): every queue table grows `claimed_by` /
/// `claimed_until_ms` so a single replica can *own* a row while it drives
/// it, and any replica can honestly re-drive after expiry (ADR-009: the
/// lease is liveness only — the command-id correlation stays the safety
/// net). Purely additive `ALTER TABLE`s: committed rows keep their shape,
/// existing columns are untouched, and the columns start `NULL`
/// (unclaimed) on both backends.
const V2_QUEUE_ROW_LEASES: Migration = Migration {
    version: 2,
    name: "v2_queue_row_leases",
    up: "ALTER TABLE exec_queue ADD COLUMN claimed_by TEXT;\nALTER TABLE exec_queue ADD COLUMN claimed_until_ms INTEGER;\nALTER TABLE async_jobs ADD COLUMN claimed_by TEXT;\nALTER TABLE async_jobs ADD COLUMN claimed_until_ms INTEGER",
};

/// v3 — the queue command family (ADR-023): `exec` rows keep their
/// payload and result bytes; `fs` rows (KiandProto v2) carry the fs
/// command/result JSON. `DEFAULT 'exec'` is what makes every pre-v3 row
/// decode as the exec family after the additive ALTER — no row rewrite,
/// no lost parked work.
const V3_EXEC_QUEUE_KIND: Migration = Migration {
    version: 3,
    name: "v3_exec_queue_kind",
    up: "ALTER TABLE exec_queue ADD COLUMN kind TEXT NOT NULL DEFAULT 'exec'",
};

/// v4 — the last-declared worker cache on the registry projection
/// (ADR-025): `description` / `protocol_version` survive a kian restart so
/// the Router catalog and `kian workers` are not id-only for a known
/// worker that has not re-registered yet. Additive nullable columns; a
/// pre-v4 row reads as "no declaration", and the live registration always
/// wins over the cache.
const V4_REGISTRY_DECLARATION: Migration = Migration {
    version: 4,
    name: "v4_registry_declaration",
    up: "ALTER TABLE endpoint_registry ADD COLUMN description TEXT;\nALTER TABLE endpoint_registry ADD COLUMN protocol_version INTEGER",
};

/// v5 — the worker-declared capability cache (KiandProto v3, ADR-029):
/// the sanitized list is JSON text on the existing registry projection.
/// Additive nullable column; a pre-v5 row reads as "no declaration".
const V5_REGISTRY_CAPABILITIES: Migration = Migration {
    version: 5,
    name: "v5_registry_capabilities",
    up: "ALTER TABLE endpoint_registry ADD COLUMN capabilities TEXT",
};

/// v6 — the bounded re-drive counter (ADR-033): `dispatch_attempts` counts
/// how many times a driver has claimed the row for a wire dispatch. The
/// count is what lets at-least-once re-drive stay honest *and* terminate: a
/// row whose result was lost to a transport loss gets its retry, while a row
/// that keeps losing its result because the command itself destroyed the ack
/// path (restarting the server, killing the worker) stops at the bound and is
/// abandoned. Additive `ALTER TABLE`; pre-v6 rows read as zero attempts.
const V6_QUEUE_DISPATCH_ATTEMPTS: Migration = Migration {
    version: 6,
    name: "v6_queue_dispatch_attempts",
    up: "ALTER TABLE exec_queue ADD COLUMN dispatch_attempts INTEGER NOT NULL DEFAULT 0",
};

/// The registered adjacent sqlite chain, ordered v0 → head. Extensible by
/// appending; committed entries are never edited.
pub fn chain() -> &'static [Migration] {
    &[
        V0_REGISTRY_QUEUE,
        V1_ASYNC_JOBS,
        V2_QUEUE_ROW_LEASES,
        V3_EXEC_QUEUE_KIND,
        V4_REGISTRY_DECLARATION,
        V5_REGISTRY_CAPABILITIES,
        V6_QUEUE_DISPATCH_ATTEMPTS,
    ]
}

/// v0 (postgres chain) — same version and name as the sqlite
/// generation, pg DDL (TEXT/BIGINT; DESIGN §3.3's per-backend typing).
const PG_V0_REGISTRY_QUEUE: Migration = Migration {
    version: 0,
    name: "v0_endpoint_registry_exec_queue",
    up: "CREATE TABLE IF NOT EXISTS endpoint_registry (\n    endpoint_id TEXT PRIMARY KEY,\n    addr TEXT NOT NULL,\n    last_heartbeat_ms BIGINT NOT NULL,\n    state TEXT NOT NULL\n);\nCREATE TABLE IF NOT EXISTS exec_queue (\n    command_id TEXT PRIMARY KEY,\n    endpoint_id TEXT NOT NULL,\n    cmd TEXT NOT NULL,\n    status TEXT NOT NULL,\n    tool_call_ref TEXT,\n    result TEXT,\n    created_at BIGINT NOT NULL,\n    updated_at BIGINT NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_exec_queue_drive ON exec_queue (endpoint_id, status, created_at)",
};

/// v1 (postgres chain) — the async job table, pg DDL.
const PG_V1_ASYNC_JOBS: Migration = Migration {
    version: 1,
    name: "v1_async_jobs",
    up: "CREATE TABLE IF NOT EXISTS async_jobs (\n    job_id TEXT PRIMARY KEY,\n    session TEXT NOT NULL,\n    endpoint_id TEXT NOT NULL,\n    cmd TEXT NOT NULL,\n    status TEXT NOT NULL,\n    tool_call_ref TEXT,\n    result TEXT,\n    created_at BIGINT NOT NULL,\n    updated_at BIGINT NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_async_jobs_drive ON async_jobs (status, created_at)",
};

/// v2 (postgres chain) — the queue-row lease columns, pg DDL. Same
/// version and name as the sqlite v2: the two chains track each other
/// generation for generation.
const PG_V2_QUEUE_ROW_LEASES: Migration = Migration {
    version: 2,
    name: "v2_queue_row_leases",
    up: "ALTER TABLE exec_queue ADD COLUMN claimed_by TEXT;\nALTER TABLE exec_queue ADD COLUMN claimed_until_ms BIGINT;\nALTER TABLE async_jobs ADD COLUMN claimed_by TEXT;\nALTER TABLE async_jobs ADD COLUMN claimed_until_ms BIGINT",
};

/// v3 (postgres chain) — the queue command family, pg DDL. Same version
/// and name as the sqlite v3.
const PG_V3_EXEC_QUEUE_KIND: Migration = Migration {
    version: 3,
    name: "v3_exec_queue_kind",
    up: "ALTER TABLE exec_queue ADD COLUMN kind TEXT NOT NULL DEFAULT 'exec'",
};

/// v4 (postgres chain) — the last-declared worker cache, pg DDL. Same
/// version and name as the sqlite v4.
const PG_V4_REGISTRY_DECLARATION: Migration = Migration {
    version: 4,
    name: "v4_registry_declaration",
    up: "ALTER TABLE endpoint_registry ADD COLUMN description TEXT;\nALTER TABLE endpoint_registry ADD COLUMN protocol_version BIGINT",
};

/// v5 (postgres chain) — the capability cache, pg DDL. Same version and
/// name as the sqlite v5.
const PG_V5_REGISTRY_CAPABILITIES: Migration = Migration {
    version: 5,
    name: "v5_registry_capabilities",
    up: "ALTER TABLE endpoint_registry ADD COLUMN capabilities TEXT",
};

/// v6 (postgres chain) — the bounded re-drive counter, pg DDL. Same
/// version and name as the sqlite v6.
const PG_V6_QUEUE_DISPATCH_ATTEMPTS: Migration = Migration {
    version: 6,
    name: "v6_queue_dispatch_attempts",
    up: "ALTER TABLE exec_queue ADD COLUMN dispatch_attempts BIGINT NOT NULL DEFAULT 0",
};

/// The postgres chain: same versions, same names, same ordering as
/// [`chain`] (enforced by `pg_chain_mirrors_sqlite_chain_versions_and_names`).
pub fn pg_chain() -> &'static [Migration] {
    &[
        PG_V0_REGISTRY_QUEUE,
        PG_V1_ASYNC_JOBS,
        PG_V2_QUEUE_ROW_LEASES,
        PG_V3_EXEC_QUEUE_KIND,
        PG_V4_REGISTRY_DECLARATION,
        PG_V5_REGISTRY_CAPABILITIES,
        PG_V6_QUEUE_DISPATCH_ATTEMPTS,
    ]
}

/// Applies every unapplied sqlite generation in order (same transactional
/// discipline as `kian_store::migrations::migrate`).
pub async fn migrate(pool: &SqlitePool) -> Result<(), QueueStoreError> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _kian_exec_migrations (\n    version INTEGER PRIMARY KEY,\n    name TEXT NOT NULL,\n    applied_at INTEGER NOT NULL\n)",
    )
    .execute(pool)
    .await?;
    for m in chain() {
        let applied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _kian_exec_migrations WHERE version = ?")
                .bind(i64::from(m.version))
                .fetch_one(pool)
                .await?;
        if applied != 0 {
            continue;
        }
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(m.up).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO _kian_exec_migrations (version, name, applied_at) VALUES (?, ?, ?)",
        )
        .bind(i64::from(m.version))
        .bind(m.name)
        .bind(now_ms())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// Applies every unapplied pg generation in order — the same runner logic
/// as [`migrate`], dispatched per backend (the pattern).
pub async fn migrate_pg(pool: &PgPool) -> Result<(), QueueStoreError> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _kian_exec_migrations (\n    version BIGINT PRIMARY KEY,\n    name TEXT NOT NULL,\n    applied_at BIGINT NOT NULL\n)",
    )
    .execute(pool)
    .await?;
    for m in pg_chain() {
        let applied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _kian_exec_migrations WHERE version = $1")
                .bind(i64::from(m.version))
                .fetch_one(pool)
                .await?;
        if applied != 0 {
            continue;
        }
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(m.up).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO _kian_exec_migrations (version, name, applied_at) VALUES ($1, $2, $3)",
        )
        .bind(i64::from(m.version))
        .bind(m.name)
        .bind(now_ms())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ExecStore
// ---------------------------------------------------------------------------

/// Durable queue + registry access (dual-backend since): a small
/// dispatching enum over the sqlite and postgres backends. Every method
/// carries the same name and signature the legacy sqlite store had, so
/// callers and tests are unchanged; construction picks the backend
/// (`open`/`from_pool` = sqlite, `open_postgres`/`from_pg_pool` =
/// postgres). All access goes through a sqlx pool — never blocks a tokio
/// worker.
#[derive(Clone)]
pub enum ExecStore {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

/// What [`ExecStore::enqueue`] found: a freshly inserted `pending` row, or
/// the already-present row for this command id (dedupe path).
#[derive(Debug)]
pub enum EnqueueOutcome {
    Inserted,
    Existing(Box<QueuedCommand>),
}

impl ExecStore {
    /// Opens (creating if needed) a sqlite database file and applies the
    /// exec migration chain. Same connection discipline as
    /// `EventStore::open`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, QueueStoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;
        let store = Self::Sqlite(pool);
        store.run_migrations().await?;
        Ok(store)
    }

    /// Opens a postgres database by URL, applies the pg exec migration
    /// chain, and returns a pooled store. The URL is env-sourced by the
    /// caller (the assembly); it may carry a password, but never
    /// lands in a config value (the secrets rule).
    pub async fn open_postgres(url: &str) -> Result<Self, QueueStoreError> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        let store = Self::Postgres(pool);
        store.run_migrations().await?;
        Ok(store)
    }

    /// Adopts an already-open sqlite pool (the server's shared
    /// `kian.sqlite` pool) and applies the exec migration chain to it.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self, QueueStoreError> {
        let store = Self::Sqlite(pool);
        store.run_migrations().await?;
        Ok(store)
    }

    /// Adopts an already-open postgres pool (the server's shared pg pool —
    /// one server, one database: the event store and the exec tables
    /// share it) and applies the pg exec migration chain to it.
    pub async fn from_pg_pool(pool: PgPool) -> Result<Self, QueueStoreError> {
        let store = Self::Postgres(pool);
        store.run_migrations().await?;
        Ok(store)
    }

    async fn run_migrations(&self) -> Result<(), QueueStoreError> {
        match self {
            Self::Sqlite(pool) => migrate(pool).await,
            Self::Postgres(pool) => migrate_pg(pool).await,
        }
    }

    /// The underlying sqlite pool (the shared-pool adoption path and the
    /// sqlite test suite). The sqlite-only accessor: a postgres-backed
    /// store carries no sqlite pool by construction, so calling it there
    /// is a programming error and panics with this message (the
    /// `EventStore::pool` pattern).
    pub fn pool(&self) -> &SqlitePool {
        match self {
            ExecStore::Sqlite(pool) => pool,
            ExecStore::Postgres(_) => panic!(
                "ExecStore::pool() is the sqlite-only pool accessor; this store is postgres-backed"
            ),
        }
    }

    /// The underlying postgres pool when this store is postgres-backed.
    pub fn pg_pool(&self) -> Option<&PgPool> {
        match self {
            ExecStore::Sqlite(_) => None,
            ExecStore::Postgres(pool) => Some(pool),
        }
    }

    /// Closes the pool; reopening the same file (or URL) rebuilds from
    /// the committed rows.
    pub async fn close(self) {
        match self {
            ExecStore::Sqlite(pool) => pool.close().await,
            ExecStore::Postgres(pool) => pool.close().await,
        }
    }

    /// Opens a sqlite database file **read-only** (no migrations, no
    /// writes): the operator verbs' view (`kian workers` — the `kian
    /// tickets` discipline). A file that never carried the exec tables
    /// reads as empty (the verb never refuses on a fresh store).
    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self, QueueStoreError> {
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

    /// All registry rows, read-only-tolerant: a store whose exec tables
    /// were never created (a database that predates remote exec, opened
    /// read-only) reads as **empty**, not as an error — the operator
    /// verbs never refuse on it.
    pub async fn endpoints_lenient(&self) -> Result<Vec<EndpointRegistryRow>, QueueStoreError> {
        match self.endpoints().await {
            Ok(rows) => Ok(rows),
            // "no such table" — a fresh/legacy file; honest empty.
            Err(QueueStoreError::Sql(e)) => {
                if e.to_string().contains("no such table")
                    || e.to_string().contains("does not exist")
                {
                    Ok(Vec::new())
                } else {
                    Err(QueueStoreError::Sql(e))
                }
            }
            other => other,
        }
    }

    // -- queue --------------------------------------------------------------

    /// Durably admit one command: the row exists (status `pending`) before
    /// any dispatch — admit-before-execute. A row for the same command id
    /// is never duplicated; the existing row is returned instead.
    pub async fn enqueue(
        &self,
        command_id: Uuid,
        endpoint_id: &str,
        command: impl Into<WorkerCommand>,
        tool_call_ref: Option<&str>,
    ) -> Result<EnqueueOutcome, QueueStoreError> {
        let command = command.into();
        let cmd_json = command.to_cmd_json()?;
        let kind = command.kind().as_str();
        let now = now_ms();
        let inserted = match self {
            Self::Sqlite(pool) => {
                crate::sqlite::enqueue(
                    pool,
                    command_id,
                    endpoint_id,
                    &cmd_json,
                    kind,
                    tool_call_ref,
                    now,
                )
                .await?
            }
            Self::Postgres(pool) => {
                crate::postgres::enqueue(
                    pool,
                    command_id,
                    endpoint_id,
                    &cmd_json,
                    kind,
                    tool_call_ref,
                    now,
                )
                .await?
            }
        };
        if inserted {
            Ok(EnqueueOutcome::Inserted)
        } else {
            Ok(EnqueueOutcome::Existing(Box::new(
                self.get(command_id).await?.ok_or_else(|| {
                    QueueStoreError::Corrupt("conflict reported but row absent".into())
                })?,
            )))
        }
    }

    /// Reads one row, if present.
    pub async fn get(&self, command_id: Uuid) -> Result<Option<QueuedCommand>, QueueStoreError> {
        let row = match self {
            Self::Sqlite(pool) => crate::sqlite::get(pool, command_id).await?,
            Self::Postgres(pool) => crate::postgres::get(pool, command_id).await?,
        };
        row.map(parse_row).transpose()
    }

    /// `pending → inflight`. Returns false when the row is no longer
    /// `pending` (already inflight from a previous life — the at-least-once
    /// re-drive case — or terminal).
    pub async fn mark_inflight(&self, command_id: Uuid) -> Result<bool, QueueStoreError> {
        let affected = match self {
            Self::Sqlite(pool) => crate::sqlite::mark_inflight(pool, command_id, now_ms()).await?,
            Self::Postgres(pool) => {
                crate::postgres::mark_inflight(pool, command_id, now_ms()).await?
            }
        };
        Ok(affected == 1)
    }

    /// `pending|inflight → done`, recording the correlated outcome.
    /// Returns false (and changes nothing) when the row is terminal —
    /// a committed `done` row is never mutated again, and an `abandoned`
    /// row keeps its honest timeout as the last word.
    pub async fn mark_done(
        &self,
        command_id: Uuid,
        outcome: &CommandOutcome,
    ) -> Result<bool, QueueStoreError> {
        let affected = match self {
            Self::Sqlite(pool) => {
                crate::sqlite::mark_done(pool, command_id, outcome, now_ms()).await?
            }
            Self::Postgres(pool) => {
                crate::postgres::mark_done(pool, command_id, outcome, now_ms()).await?
            }
        };
        Ok(affected == 1)
    }

    /// `pending → abandoned` (queue timeout). Returns false when
    /// the row is not pending — an inflight execution is never abandoned by
    /// a caller's wait expiry, and a settled row freezes.
    pub async fn mark_abandoned(&self, command_id: Uuid) -> Result<bool, QueueStoreError> {
        let affected = match self {
            Self::Sqlite(pool) => crate::sqlite::mark_abandoned(pool, command_id, now_ms()).await?,
            Self::Postgres(pool) => {
                crate::postgres::mark_abandoned(pool, command_id, now_ms()).await?
            }
        };
        Ok(affected == 1)
    }

    /// Converges aged `pending` rows whose caller disappeared. The guarded
    /// sweep only abandons rows that were never dispatched, have exceeded
    /// the deployment's dispatch wait, and have no live row lease.
    pub async fn abandon_aged_pending(
        &self,
        older_than_ms: i64,
        now_ms: i64,
    ) -> Result<Vec<PendingAbandonment>, QueueStoreError> {
        let rows = match self {
            Self::Sqlite(pool) => {
                crate::sqlite::abandon_aged_pending(pool, older_than_ms, now_ms).await?
            }
            Self::Postgres(pool) => {
                crate::postgres::abandon_aged_pending(pool, older_than_ms, now_ms).await?
            }
        };
        rows.into_iter()
            .map(
                |(command_id, endpoint_id, tool_call_ref, created_at, dispatch_attempts)| {
                    Ok(PendingAbandonment {
                        command_id: Uuid::parse_str(&command_id).map_err(|e| {
                            QueueStoreError::Corrupt(format!("bad command id: {e}"))
                        })?,
                        endpoint_id,
                        tool_call_ref,
                        created_at,
                        dispatch_attempts: u32::try_from(dispatch_attempts).unwrap_or(u32::MAX),
                    })
                },
            )
            .collect()
    }

    /// Rows this endpoint should (re-)drive: `pending` never sent, and
    /// `inflight` presumed lost (at-least-once; ADR-007). Oldest first.
    pub async fn drivable(&self, endpoint_id: &str) -> Result<Vec<QueuedCommand>, QueueStoreError> {
        let rows = match self {
            Self::Sqlite(pool) => crate::sqlite::drivable(pool, endpoint_id).await?,
            Self::Postgres(pool) => crate::postgres::drivable(pool, endpoint_id).await?,
        };
        rows.into_iter().map(parse_row).collect()
    }

    // -- queue-row leases (ADR-009 decision 5) -------------------------

    /// Claims one `exec_queue` row with a single-statement lease
    /// (`claimed_by`/`claimed_until_ms`, v2 columns) and counts the dispatch
    /// attempt (`dispatch_attempts`, v6). The claim lands iff the row is live
    /// (`pending`/`inflight` — terminal rows freeze), unclaimed, expired or
    /// already this holder's, **and** the bound admits it: a `pending` row's
    /// first dispatch always lands, while an `inflight` row is re-drivable
    /// only while `dispatch_attempts < max_dispatches` (ADR-033). The
    /// attempt itself commits later, after the frame is written. Returns
    /// `false` when a fresh lease is held by another replica *or* the
    /// re-drive bound is exhausted; [`ExecStore::abandon_exhausted`] tells
    /// the two apart.
    ///
    /// The lease is **liveness only**: it bounds duplicate dispatch
    /// across replicas and is never consulted for accepting a result —
    /// the command-id correlation (ADR-007) remains the safety net.
    pub async fn claim_queue_row(
        &self,
        command_id: Uuid,
        holder: &str,
        ttl_ms: i64,
        now_ms: i64,
        max_dispatches: u32,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::claim_queue_row(
                    pool,
                    command_id,
                    holder,
                    ttl_ms,
                    now_ms,
                    i64::from(max_dispatches),
                )
                .await
            }
            Self::Postgres(pool) => {
                crate::postgres::claim_queue_row(
                    pool,
                    command_id,
                    holder,
                    ttl_ms,
                    now_ms,
                    i64::from(max_dispatches),
                )
                .await
            }
        }
        .map_err(QueueStoreError::from)
    }

    /// Commits one dispatch attempt after its frame was written. The lease
    /// guard makes the commit idempotent-by-owner: a crash before this write
    /// leaves the attempt unspent, while a frame that reached the transport
    /// but not the row is conservatively re-driven at least once.
    pub async fn commit_dispatch_attempt(
        &self,
        command_id: Uuid,
        holder: &str,
    ) -> Result<bool, QueueStoreError> {
        let now = now_ms();
        let affected = match self {
            Self::Sqlite(pool) => {
                crate::sqlite::commit_dispatch_attempt(pool, command_id, holder, now).await?
            }
            Self::Postgres(pool) => {
                crate::postgres::commit_dispatch_attempt(pool, command_id, holder, now).await?
            }
        };
        Ok(affected == 1)
    }

    /// Converges an `inflight` row whose re-drive bound is spent (ADR-033):
    /// `inflight → abandoned` exactly when `dispatch_attempts >=
    /// max_dispatches` **and** no live lease is held. A fresh lease means a
    /// driver is dispatching the row right now (that attempt may still
    /// settle it), so the transition is refused and the caller retries on a
    /// later loop turn — never an abandonment race against a live dispatch.
    /// Returns `true` iff this call made the row terminal.
    pub async fn abandon_exhausted(
        &self,
        command_id: Uuid,
        max_dispatches: u32,
        now_ms: i64,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::abandon_exhausted(
                    pool,
                    command_id,
                    i64::from(max_dispatches),
                    now_ms,
                )
                .await
            }
            Self::Postgres(pool) => {
                crate::postgres::abandon_exhausted(
                    pool,
                    command_id,
                    i64::from(max_dispatches),
                    now_ms,
                )
                .await
            }
        }
        .map_err(QueueStoreError::from)
    }

    /// Refunds one dispatch attempt charged by [`Self::claim_queue_row`] for
    /// a frame that never left the process (the connection's writer was
    /// already closed). The endpoint provably never received the command, so
    /// the at-least-once bound must not count it: without the refund a worker
    /// that reconnects a moment later could find its row abandoned after a
    /// dispatch that never happened. Guarded on this holder's claim so a
    /// foreign replica's attempt is never touched. Returns `true` iff a unit
    /// was refunded.
    pub async fn refund_dispatch_attempt(
        &self,
        command_id: Uuid,
        holder: &str,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::refund_dispatch_attempt(pool, command_id, holder).await
            }
            Self::Postgres(pool) => {
                crate::postgres::refund_dispatch_attempt(pool, command_id, holder).await
            }
        }
        .map_err(QueueStoreError::from)
    }

    /// Extends this holder's lease on one `exec_queue` row. `false` =
    /// ownership lost (expired and reclaimed elsewhere) or the row
    /// settled — the caller stops heartbeating (a stray late result is
    /// still discarded by the terminal guard; the lease never gates
    /// correctness).
    pub async fn heartbeat_queue_row(
        &self,
        command_id: Uuid,
        holder: &str,
        ttl_ms: i64,
        now_ms: i64,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::heartbeat_queue_row(pool, command_id, holder, ttl_ms, now_ms).await
            }
            Self::Postgres(pool) => {
                crate::postgres::heartbeat_queue_row(pool, command_id, holder, ttl_ms, now_ms).await
            }
        }
        .map_err(QueueStoreError::from)
    }

    /// The [`ExecStore::claim_queue_row`] lease for `async_jobs` rows.
    /// A job's *dispatch* is additionally gated by the `exec_queue` row
    /// it rides (the job id is the command id); this lease gives the
    /// observer (the replica driving the job to its session) ownership
    /// of the bookkeeping row on the same discipline.
    pub async fn claim_async_row(
        &self,
        job_id: Uuid,
        holder: &str,
        ttl_ms: i64,
        now_ms: i64,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::claim_async_row(pool, job_id, holder, ttl_ms, now_ms).await
            }
            Self::Postgres(pool) => {
                crate::postgres::claim_async_row(pool, job_id, holder, ttl_ms, now_ms).await
            }
        }
        .map_err(QueueStoreError::from)
    }

    /// The [`ExecStore::heartbeat_queue_row`] twin for `async_jobs`.
    pub async fn heartbeat_async_row(
        &self,
        job_id: Uuid,
        holder: &str,
        ttl_ms: i64,
        now_ms: i64,
    ) -> Result<bool, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::heartbeat_async_row(pool, job_id, holder, ttl_ms, now_ms).await
            }
            Self::Postgres(pool) => {
                crate::postgres::heartbeat_async_row(pool, job_id, holder, ttl_ms, now_ms).await
            }
        }
        .map_err(QueueStoreError::from)
    }

    // -- registry -----------------------------------------------------------

    /// Projects the connection manager's current view of one endpoint.
    /// The registry is a disposable projection — rebuilt from live status,
    /// never authoritative.
    pub async fn upsert_endpoint(
        &self,
        endpoint_id: &str,
        addr: &str,
        last_heartbeat_ms: u64,
        state: &str,
    ) -> Result<(), QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::upsert_endpoint(pool, endpoint_id, addr, last_heartbeat_ms, state)
                    .await?
            }
            Self::Postgres(pool) => {
                crate::postgres::upsert_endpoint(pool, endpoint_id, addr, last_heartbeat_ms, state)
                    .await?
            }
        }
        Ok(())
    }

    /// Upserts the registry row **with the worker's last declaration**
    /// (ADR-025/ADR-029: description, protocol version, capabilities).
    /// Called on a successful registration before the route is attached;
    /// the declaration columns are overwritten by every registration (a
    /// registration that omits a declaration clears the cached value).
    /// Heartbeat/detach upserts never touch these columns.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_endpoint_with_declaration(
        &self,
        endpoint_id: &str,
        addr: &str,
        last_heartbeat_ms: u64,
        state: &str,
        description: Option<&str>,
        protocol_version: u32,
        capabilities: &[String],
    ) -> Result<(), QueueStoreError> {
        match self {
            Self::Sqlite(pool) => {
                crate::sqlite::upsert_endpoint_with_declaration(
                    pool,
                    endpoint_id,
                    addr,
                    last_heartbeat_ms,
                    state,
                    description,
                    protocol_version,
                    capabilities,
                )
                .await?
            }
            Self::Postgres(pool) => {
                crate::postgres::upsert_endpoint_with_declaration(
                    pool,
                    endpoint_id,
                    addr,
                    last_heartbeat_ms,
                    state,
                    description,
                    protocol_version,
                    capabilities,
                )
                .await?
            }
        }
        Ok(())
    }

    /// All registry rows (the watchdog's read API).
    pub async fn endpoints(&self) -> Result<Vec<EndpointRegistryRow>, QueueStoreError> {
        match self {
            Self::Sqlite(pool) => crate::sqlite::endpoints(pool).await,
            Self::Postgres(pool) => crate::postgres::endpoints(pool).await,
        }
        .map_err(QueueStoreError::from)
    }
}

/// Parses one queue row. `result` is stored as the raw outcome JSON;
/// its kind (ok / err) is recovered from the payload shape.
fn parse_row(row: crate::sqlite::QueueRow) -> Result<QueuedCommand, QueueStoreError> {
    let (
        command_id,
        endpoint_id,
        cmd,
        kind,
        status,
        tool_call_ref,
        result,
        created_at,
        updated_at,
        dispatch_attempts,
    ) = row;
    let command_id = Uuid::parse_str(&command_id)
        .map_err(|e| QueueStoreError::Corrupt(format!("bad command id: {e}")))?;
    let kind = CommandKind::from_str(&kind)?;
    let command = match kind {
        CommandKind::Exec => WorkerCommand::Exec(
            serde_json::from_str(&cmd)
                .map_err(|e| QueueStoreError::Corrupt(format!("bad cmd json: {e}")))?,
        ),
        CommandKind::Fs => WorkerCommand::Fs(
            serde_json::from_str(&cmd)
                .map_err(|e| QueueStoreError::Corrupt(format!("bad fs cmd json: {e}")))?,
        ),
    };
    let status = QueueStatus::from_str(&status)?;
    let outcome = match (&result, status) {
        (None, QueueStatus::Done) => {
            return Err(QueueStoreError::Corrupt("done row without result".into()));
        }
        (None, _) => None,
        (Some(raw), _) => Some(CommandOutcome::from_json(raw, kind)?),
    };
    Ok(QueuedCommand {
        command_id,
        endpoint_id,
        command,
        status,
        tool_call_ref,
        outcome,
        dispatch_attempts: u32::try_from(dispatch_attempts).unwrap_or(u32::MAX),
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::{Migration, chain, pg_chain};

    /// The chain must be adjacent and gapless starting at v0 (same rule as
    /// the store's chain, independently enforced for this one).
    #[test]
    fn migration_chain_is_adjacent_and_gapless_from_zero() {
        let chain = chain();
        assert!(!chain.is_empty(), "chain must contain v0");
        assert_eq!(chain[0].version, 0, "chain starts at v0");
        let versions: Vec<u32> = chain.iter().map(|m| m.version).collect();
        assert_eq!(versions, (0..chain.len() as u32).collect::<Vec<_>>());
    }

    /// v0 is a committed generation: pinned byte-for-byte so any rewrite of
    /// history trips this test instead of diverging from every
    /// already-created database (frozen-golden pattern).
    #[test]
    fn v0_generation_sql_is_frozen() {
        let v0: &Migration = &chain()[0];
        assert_eq!(v0.version, 0);
        assert_eq!(v0.name, "v0_endpoint_registry_exec_queue");
        let golden = concat!(
            "CREATE TABLE IF NOT EXISTS endpoint_registry (\n",
            "    endpoint_id TEXT PRIMARY KEY,\n",
            "    addr TEXT NOT NULL,\n",
            "    last_heartbeat_ms INTEGER NOT NULL,\n",
            "    state TEXT NOT NULL\n",
            ");\n",
            "CREATE TABLE IF NOT EXISTS exec_queue (\n",
            "    command_id TEXT PRIMARY KEY,\n",
            "    endpoint_id TEXT NOT NULL,\n",
            "    cmd TEXT NOT NULL,\n",
            "    status TEXT NOT NULL,\n",
            "    tool_call_ref TEXT,\n",
            "    result TEXT,\n",
            "    created_at INTEGER NOT NULL,\n",
            "    updated_at INTEGER NOT NULL\n",
            ");\n",
            "CREATE INDEX IF NOT EXISTS idx_exec_queue_drive ON exec_queue (endpoint_id, status, created_at)",
        );
        assert_eq!(v0.up, golden);
    }

    /// v2 is committed the moment this chain ships: pinned byte-for-byte
    /// (additive `ALTER TABLE`s only — the queue-lease columns, the
    /// designed chain extension).
    #[test]
    fn v2_generation_sql_is_frozen_and_additive() {
        let v2: &Migration = &chain()[2];
        assert_eq!(v2.version, 2);
        assert_eq!(v2.name, "v2_queue_row_leases");
        assert_eq!(
            v2.up,
            concat!(
                "ALTER TABLE exec_queue ADD COLUMN claimed_by TEXT;\n",
                "ALTER TABLE exec_queue ADD COLUMN claimed_until_ms INTEGER;\n",
                "ALTER TABLE async_jobs ADD COLUMN claimed_by TEXT;\n",
                "ALTER TABLE async_jobs ADD COLUMN claimed_until_ms INTEGER",
            )
        );
        // Additive only: no CREATE, no DROP, no column rewrite.
        assert!(!v2.up.contains("CREATE"));
        assert!(!v2.up.contains("DROP"));
        for stmt in v2.up.split(";") {
            assert!(
                stmt.trim_start().starts_with("ALTER TABLE"),
                "v2 must be ALTER TABLE only: {stmt}"
            );
        }
    }

    /// v6 is committed the moment this chain ships: pinned byte-for-byte
    /// (additive `ALTER TABLE` only — the bounded re-drive counter,
    /// ADR-033).
    #[test]
    fn v6_generation_sql_is_frozen_and_additive() {
        let v6: &Migration = &chain()[6];
        assert_eq!(v6.version, 6);
        assert_eq!(v6.name, "v6_queue_dispatch_attempts");
        assert_eq!(
            v6.up,
            "ALTER TABLE exec_queue ADD COLUMN dispatch_attempts INTEGER NOT NULL DEFAULT 0"
        );
        // Additive only: no CREATE, no DROP, no column rewrite, and a
        // default so every pre-v6 row reads as "never dispatched".
        assert!(!v6.up.contains("CREATE"));
        assert!(!v6.up.contains("DROP"));
        assert!(v6.up.starts_with("ALTER TABLE"));
        assert!(v6.up.contains("DEFAULT 0"));
    }

    /// The pg chain tracks the sqlite chain generation for generation:
    /// same versions, same names, same order (DESIGN §3.3).
    #[test]
    fn pg_chain_mirrors_sqlite_chain_versions_and_names() {
        let sqlite = chain();
        let pg = pg_chain();
        assert_eq!(
            sqlite.len(),
            pg.len(),
            "the two chains must stay the same length"
        );
        for (s, p) in sqlite.iter().zip(pg.iter()) {
            assert_eq!(s.version, p.version, "version mismatch at v{}", s.version);
            assert_eq!(s.name, p.name, "name mismatch at v{}", s.version);
        }
    }

    /// The pg chain is adjacent and gapless from v0, like the sqlite
    /// chain.
    #[test]
    fn pg_chain_is_adjacent_and_gapless_from_zero() {
        let pg = pg_chain();
        assert!(!pg.is_empty(), "pg chain must contain v0");
        assert_eq!(pg[0].version, 0, "pg chain starts at v0");
        let versions: Vec<u32> = pg.iter().map(|m| m.version).collect();
        assert_eq!(versions, (0..pg.len() as u32).collect::<Vec<_>>());
    }

    /// The pg generations carry pg types (TEXT/BIGINT), never sqlite's
    /// untyped INTEGER column spelling — except the ALTER statements,
    /// which name no type-bearing sqlite column (their BIGINT arms are
    /// checked by the frozen v2 golden below).
    #[test]
    fn pg_generations_use_pg_types() {
        for m in pg_chain() {
            for stmt in m.up.split(';') {
                if !stmt.contains("INTEGER") {
                    continue;
                }
                assert!(
                    stmt.trim_start().starts_with("ALTER TABLE"),
                    "pg migration v{} ({}) uses sqlite's INTEGER spelling",
                    m.version,
                    m.name
                );
            }
        }
    }

    /// v0/v1/v2 of the pg chain are committed generations: pinned
    /// byte-for-byte like their sqlite twins.
    #[test]
    fn pg_generations_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[0].version, 0);
        assert_eq!(pg[0].name, "v0_endpoint_registry_exec_queue");
        assert_eq!(
            pg[0].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS endpoint_registry (\n",
                "    endpoint_id TEXT PRIMARY KEY,\n",
                "    addr TEXT NOT NULL,\n",
                "    last_heartbeat_ms BIGINT NOT NULL,\n",
                "    state TEXT NOT NULL\n",
                ");\n",
                "CREATE TABLE IF NOT EXISTS exec_queue (\n",
                "    command_id TEXT PRIMARY KEY,\n",
                "    endpoint_id TEXT NOT NULL,\n",
                "    cmd TEXT NOT NULL,\n",
                "    status TEXT NOT NULL,\n",
                "    tool_call_ref TEXT,\n",
                "    result TEXT,\n",
                "    created_at BIGINT NOT NULL,\n",
                "    updated_at BIGINT NOT NULL\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_exec_queue_drive ON exec_queue (endpoint_id, status, created_at)",
            )
        );
        assert_eq!(pg[1].version, 1);
        assert_eq!(pg[1].name, "v1_async_jobs");
        assert_eq!(
            pg[1].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS async_jobs (\n",
                "    job_id TEXT PRIMARY KEY,\n",
                "    session TEXT NOT NULL,\n",
                "    endpoint_id TEXT NOT NULL,\n",
                "    cmd TEXT NOT NULL,\n",
                "    status TEXT NOT NULL,\n",
                "    tool_call_ref TEXT,\n",
                "    result TEXT,\n",
                "    created_at BIGINT NOT NULL,\n",
                "    updated_at BIGINT NOT NULL\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_async_jobs_drive ON async_jobs (status, created_at)",
            )
        );
        assert_eq!(pg[2].version, 2);
        assert_eq!(pg[2].name, "v2_queue_row_leases");
        assert_eq!(
            pg[2].up,
            concat!(
                "ALTER TABLE exec_queue ADD COLUMN claimed_by TEXT;\n",
                "ALTER TABLE exec_queue ADD COLUMN claimed_until_ms BIGINT;\n",
                "ALTER TABLE async_jobs ADD COLUMN claimed_by TEXT;\n",
                "ALTER TABLE async_jobs ADD COLUMN claimed_until_ms BIGINT",
            )
        );
    }

    /// Review assertion (pattern): the store never deletes queue rows,
    /// and every queue mutation carries a status guard so committed `done`
    /// rows are immutable.
    #[test]
    fn no_delete_and_done_rows_are_never_mutated() {
        let src = include_str!("store.rs");
        // Assembled at runtime so this test's own source is not a match.
        let delete = format!("{} {}", "DELETE", "FROM");
        assert!(
            !src.to_uppercase().contains(&delete.to_uppercase()),
            "queue rows are durable audit state — deletion is forbidden"
        );
        for line in src.lines() {
            let trimmed = line.trim_start();
            if trimmed.to_uppercase().starts_with("UPDATE EXEC_QUEUE") {
                assert!(
                    line.contains("AND status"),
                    "unguarded exec_queue mutation: {line}"
                );
            }
        }
    }

    /// The same review assertion over the backend modules, where the SQL
    /// lives since: no deletes anywhere, and every `exec_queue` /
    /// `async_jobs` mutation carries a status guard.
    #[test]
    fn backend_modules_keep_guards_and_never_delete() {
        for (name, src) in [
            ("sqlite.rs", include_str!("sqlite.rs")),
            ("postgres.rs", include_str!("postgres.rs")),
        ] {
            let delete = format!("{} {}", "DELETE", "FROM");
            assert!(
                !src.to_uppercase().contains(&delete.to_uppercase()),
                "{name}: queue rows are durable audit state — deletion is forbidden"
            );
            for line in src.lines() {
                let trimmed = line.trim_start();
                let t = trimmed.to_uppercase();
                if t.starts_with("UPDATE EXEC_QUEUE") || t.starts_with("UPDATE ASYNC_JOBS") {
                    assert!(
                        line.contains("AND status"),
                        "{name}: unguarded queue mutation: {line}"
                    );
                }
            }
        }
    }
}
