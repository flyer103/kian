//! Durable command queue + endpoint registry (card C20, DESIGN §4.3/§7).
//!
//! Queue rows are **pre-event durable state**, exactly like inbox rows: the
//! row exists in `kian.sqlite` before any dispatch flies (admit-before-
//! execute), and results become session events only through the normal
//! `tool/call`/`tool/result` loop path — never by writing the events table
//! from here.
//!
//! Statuses form a monotone little state machine:
//!
//! ```text
//! pending ──▶ inflight ──▶ done
//!    │           │
//!    └───────────┴──▶ abandoned   (queue timeout; terminal)
//! ```
//!
//! `done` and `abandoned` are terminal: every transition statement carries a
//! status guard (`WHERE status IN (...)`) so a committed `done` row is never
//! updated again and no row is ever deleted (asserted by
//! `no_delete_and_done_rows_are_never_mutated` — the C5 review pattern).

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use uuid::Uuid;

use kian_core::seams::{ExecCommand, ExecError, ExecResult};

use crate::proto::{CommandError, ErrorCode};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failures of the durable queue store itself (never a command outcome).
#[derive(Debug, thiserror::Error)]
pub enum QueueStoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] sqlx::Error),
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
    Err { error: CommandError },
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
        }
    }

    fn to_json(&self) -> String {
        match self {
            Self::Ok { result } => serde_json::to_string(&serde_json::json!({ "ok": result }))
                .expect("exec results always serialize"),
            Self::Err { error } => serde_json::to_string(&serde_json::json!({ "error": error }))
                .expect("command errors always serialize"),
        }
    }

    fn from_json(raw: &str) -> Result<Self, QueueStoreError> {
        let value: serde_json::Value = serde_json::from_str(raw)
            .map_err(|e| QueueStoreError::Corrupt(format!("bad outcome json: {e}")))?;
        if let Some(result) = value.get("ok") {
            serde_json::from_value(result.clone())
                .map(|result| Self::Ok { result })
                .map_err(|e| QueueStoreError::Corrupt(format!("bad ok outcome: {e}")))
        } else if let Some(error) = value.get("error") {
            serde_json::from_value(error.clone())
                .map(|error| Self::Err { error })
                .map_err(|e| QueueStoreError::Corrupt(format!("bad err outcome: {e}")))
        } else {
            Err(QueueStoreError::Corrupt(
                "outcome json carries neither ok nor error".into(),
            ))
        }
    }
}

/// One `exec_queue` row.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedCommand {
    pub command_id: Uuid,
    pub endpoint_id: String,
    pub cmd: ExecCommand,
    pub status: QueueStatus,
    pub tool_call_ref: Option<String>,
    /// Present iff `status == Done`.
    pub outcome: Option<CommandOutcome>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// One `endpoint_registry` row (the C21 watchdog reads this projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointRegistryRow {
    pub endpoint_id: String,
    pub addr: String,
    pub last_heartbeat_ms: u64,
    pub state: String,
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
// Migrations (kian-exec's own adjacent chain in the shared kian.sqlite)
// ---------------------------------------------------------------------------

/// One generation in the adjacent chain (same shape as `kian-store`'s
/// migrations; a separate bookkeeping table `_kian_exec_migrations` keeps
/// the two chains independent in one file).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub up: &'static str,
}

/// v0 — the endpoint registry and the durable command queue (card C20).
/// Frozen golden: committed generations are never rewritten.
///
/// `exec_queue.result` stores the correlated outcome JSON once a row
/// settles, so command-id dedupe can answer re-executions from the row
/// across a restart (additive over the card's column list; see ADR-007).
const V0_REGISTRY_QUEUE: Migration = Migration {
    version: 0,
    name: "v0_endpoint_registry_exec_queue",
    up: "CREATE TABLE IF NOT EXISTS endpoint_registry (\n    endpoint_id TEXT PRIMARY KEY,\n    addr TEXT NOT NULL,\n    last_heartbeat_ms INTEGER NOT NULL,\n    state TEXT NOT NULL\n);\nCREATE TABLE IF NOT EXISTS exec_queue (\n    command_id TEXT PRIMARY KEY,\n    endpoint_id TEXT NOT NULL,\n    cmd TEXT NOT NULL,\n    status TEXT NOT NULL,\n    tool_call_ref TEXT,\n    result TEXT,\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_exec_queue_drive ON exec_queue (endpoint_id, status, created_at)",
};

/// The registered adjacent chain, ordered v0 → head. Extensible by
/// appending; committed entries are never edited.
pub fn chain() -> &'static [Migration] {
    &[V0_REGISTRY_QUEUE]
}

/// Applies every unapplied generation in order (same transactional
/// discipline as `kian-store::migrations::migrate`).
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

// ---------------------------------------------------------------------------
// ExecStore
// ---------------------------------------------------------------------------

/// Durable queue + registry access over a sqlx sqlite pool (WAL,
/// `busy_timeout`; all access is async — never blocks a tokio worker).
#[derive(Clone)]
pub struct ExecStore {
    pool: SqlitePool,
}

/// What [`ExecStore::enqueue`] found: a freshly inserted `pending` row, or
/// the already-present row for this command id (dedupe path).
#[derive(Debug)]
pub enum EnqueueOutcome {
    Inserted,
    Existing(Box<QueuedCommand>),
}

impl ExecStore {
    /// Opens (creating if needed) a database file and applies the exec
    /// migration chain. Same connection discipline as `EventStore::open`.
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
        let store = Self { pool };
        migrate(&store.pool).await?;
        Ok(store)
    }

    /// Adopts an already-open pool (the server's shared `kian.sqlite` pool)
    /// and applies the exec migration chain to it.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self, QueueStoreError> {
        let store = Self { pool };
        migrate(&store.pool).await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Closes the pool; reopening the same file rebuilds from the committed
    /// rows.
    pub async fn close(self) {
        self.pool.close().await;
    }

    // -- queue --------------------------------------------------------------

    /// Durably admit one command: the row exists (status `pending`) before
    /// any dispatch — admit-before-execute. A row for the same command id
    /// is never duplicated; the existing row is returned instead.
    pub async fn enqueue(
        &self,
        command_id: Uuid,
        endpoint_id: &str,
        cmd: &ExecCommand,
        tool_call_ref: Option<&str>,
    ) -> Result<EnqueueOutcome, QueueStoreError> {
        let cmd_json = serde_json::to_string(cmd)
            .map_err(|e| QueueStoreError::Corrupt(format!("cmd not serializable: {e}")))?;
        let now = now_ms();
        let result = sqlx::query(
            "INSERT INTO exec_queue (command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at)\n             VALUES (?, ?, ?, 'pending', ?, NULL, ?, ?)\n             ON CONFLICT(command_id) DO NOTHING",
        )
        .bind(command_id.to_string())
        .bind(endpoint_id)
        .bind(&cmd_json)
        .bind(tool_call_ref)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
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
        let row = sqlx::query_as::<_, (String, String, String, String, Option<String>, Option<String>, i64, i64)>(
            "SELECT command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM exec_queue WHERE command_id = ?",
        )
        .bind(command_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(parse_row).transpose()
    }

    /// `pending → inflight`. Returns false when the row is no longer
    /// `pending` (already inflight from a previous life — the at-least-once
    /// re-drive case — or terminal).
    pub async fn mark_inflight(&self, command_id: Uuid) -> Result<bool, QueueStoreError> {
        let result = sqlx::query(
            "UPDATE exec_queue SET status = 'inflight', updated_at = ? WHERE command_id = ? AND status = 'pending'",
        )
        .bind(now_ms())
        .bind(command_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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
        let result = sqlx::query(
            "UPDATE exec_queue SET status = 'done', result = ?, updated_at = ? WHERE command_id = ? AND status IN ('pending', 'inflight')",
        )
        .bind(outcome.to_json())
        .bind(now_ms())
        .bind(command_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// `pending|inflight → abandoned` (queue timeout). Returns false when
    /// the row already settled — including when the real result won the
    /// race against the timer (the caller re-reads and prefers it).
    pub async fn mark_abandoned(&self, command_id: Uuid) -> Result<bool, QueueStoreError> {
        let result = sqlx::query(
            "UPDATE exec_queue SET status = 'abandoned', updated_at = ? WHERE command_id = ? AND status IN ('pending', 'inflight')",
        )
        .bind(now_ms())
        .bind(command_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Rows this endpoint should (re-)drive: `pending` never sent, and
    /// `inflight` presumed lost (at-least-once; ADR-007). Oldest first.
    pub async fn drivable(&self, endpoint_id: &str) -> Result<Vec<QueuedCommand>, QueueStoreError> {
        let rows = sqlx::query_as::<_, (String, String, String, String, Option<String>, Option<String>, i64, i64)>(
            "SELECT command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM exec_queue WHERE endpoint_id = ? AND status IN ('pending', 'inflight') ORDER BY created_at ASC, command_id ASC",
        )
        .bind(endpoint_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(parse_row).collect()
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
        sqlx::query(
            "INSERT INTO endpoint_registry (endpoint_id, addr, last_heartbeat_ms, state) VALUES (?, ?, ?, ?)\n             ON CONFLICT(endpoint_id) DO UPDATE SET addr = excluded.addr, last_heartbeat_ms = excluded.last_heartbeat_ms, state = excluded.state",
        )
        .bind(endpoint_id)
        .bind(addr)
        .bind(last_heartbeat_ms as i64)
        .bind(state)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// All registry rows (the C21 watchdog's read API).
    pub async fn endpoints(&self) -> Result<Vec<EndpointRegistryRow>, QueueStoreError> {
        let rows: Vec<(String, String, i64, String)> =
            sqlx::query_as("SELECT endpoint_id, addr, last_heartbeat_ms, state FROM endpoint_registry ORDER BY endpoint_id ASC")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|(endpoint_id, addr, hb, state)| EndpointRegistryRow {
                endpoint_id,
                addr,
                last_heartbeat_ms: hb.max(0) as u64,
                state,
            })
            .collect())
    }
}

/// Parses one queue row tuple. `result` is stored as the raw outcome JSON;
/// its kind (ok / err) is recovered from the payload shape.
fn parse_row(
    (command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at): (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        i64,
        i64,
    ),
) -> Result<QueuedCommand, QueueStoreError> {
    let command_id = Uuid::parse_str(&command_id)
        .map_err(|e| QueueStoreError::Corrupt(format!("bad command id: {e}")))?;
    let cmd: ExecCommand = serde_json::from_str(&cmd)
        .map_err(|e| QueueStoreError::Corrupt(format!("bad cmd json: {e}")))?;
    let status = QueueStatus::from_str(&status)?;
    let outcome = match (&result, status) {
        (None, QueueStatus::Done) => {
            return Err(QueueStoreError::Corrupt("done row without result".into()));
        }
        (None, _) => None,
        (Some(raw), _) => Some(CommandOutcome::from_json(raw)?),
    };
    Ok(QueuedCommand {
        command_id,
        endpoint_id,
        cmd,
        status,
        tool_call_ref,
        outcome,
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::{Migration, chain};

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
    /// already-created database (C5 frozen-golden pattern).
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

    /// Review assertion (C5 pattern): the store never deletes queue rows,
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
}
