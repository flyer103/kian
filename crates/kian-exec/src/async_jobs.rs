//! Async job rows — the durable bookkeeping behind the async
//! `start()`/`stop()` tool pair (card C37, ADR-008).
//!
//! An async job is a queued remote command **plus** the session that
//! must be told when it settles. The dispatch itself is owned by the
//! existing C20 machinery unchanged: the command rides the same
//! `exec_queue` through the same per-endpoint drivers, correlated by
//! the same command id (the job id **is** the command id, derived from
//! the calling tool-call id — dedupe across recovery comes free). This
//! table adds exactly one thing: *who to notify*.
//!
//! Rows are pre-event durable state per ADR-007's pattern: the job row
//! is committed before any dispatch flies (admit-before-execute), and
//! the completion reaches the session only as an **admitted input**
//! (the C23 admit→wake path, driven by the kian-cli completion
//! bridge) — never by writing session events from here.
//!
//! Statuses form the same monotone little state machine as
//! `exec_queue`, plus `cancelled`:
//!
//! ```text
//! pending ──▶ inflight ──▶ done
//!    │           │
//!    │           ├──▶ cancelled   (stop(); terminal)
//!    └───────────┴──▶ abandoned   (queue timeout; terminal)
//! ```
//!
//! `done`, `cancelled` and `abandoned` are terminal: every transition
//! carries a status guard, no row is ever deleted, and no committed
//! terminal row is ever mutated (asserted by
//! [`self::tests::async_rows_are_guarded_and_never_deleted`], the C5
//! review pattern applied to this table).

use kian_core::model::SessionId;
use kian_core::seams::ExecCommand;
use uuid::Uuid;

use crate::store::{ExecStore, QueueStoreError, now_ms};

/// Lifecycle of one async job. Monotone: `Done`, `Cancelled` and
/// `Abandoned` are terminal (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncJobStatus {
    Pending,
    Inflight,
    Done,
    Cancelled,
    Abandoned,
}

impl AsyncJobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Inflight => "inflight",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
        }
    }

    fn from_str(s: &str) -> Result<Self, QueueStoreError> {
        match s {
            "pending" => Ok(Self::Pending),
            "inflight" => Ok(Self::Inflight),
            "done" => Ok(Self::Done),
            "cancelled" => Ok(Self::Cancelled),
            "abandoned" => Ok(Self::Abandoned),
            other => Err(QueueStoreError::Corrupt(format!(
                "unknown async job status {other:?}"
            ))),
        }
    }

    /// Terminal statuses freeze the row: no transition may follow.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Abandoned)
    }
}

/// One `async_jobs` row. `result` is the JSON completion value, present
/// iff the job settled with something to say (`done` always; a
/// cancellation keeps `None` — the label is the event).
#[derive(Debug, Clone, PartialEq)]
pub struct AsyncJob {
    pub job_id: Uuid,
    pub session: SessionId,
    pub endpoint_id: String,
    pub cmd: ExecCommand,
    pub status: AsyncJobStatus,
    pub tool_call_ref: Option<String>,
    pub result: Option<serde_json::Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// What [`ExecStore::insert_async_job`] found: a freshly inserted
/// `pending` row, or the already-present row for this job id (the
/// dedupe path — replayed tool calls collapse onto one row).
#[derive(Debug)]
pub enum AsyncEnqueueOutcome {
    Inserted,
    Existing(Box<AsyncJob>),
}

impl ExecStore {
    /// Durably admits one async job: the row exists (status `pending`)
    /// before any dispatch — admit-before-execute. A row for the same
    /// job id is never duplicated; the existing row is returned
    /// instead (command-id dedupe across recovery, ADR-008).
    pub async fn insert_async_job(
        &self,
        job_id: Uuid,
        session: SessionId,
        endpoint_id: &str,
        cmd: &ExecCommand,
        tool_call_ref: Option<&str>,
    ) -> Result<AsyncEnqueueOutcome, QueueStoreError> {
        let cmd_json = serde_json::to_string(cmd)
            .map_err(|e| QueueStoreError::Corrupt(format!("cmd not serializable: {e}")))?;
        let now = now_ms();
        let result = sqlx::query(
            "INSERT INTO async_jobs (job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at)\n             VALUES (?, ?, ?, ?, 'pending', ?, NULL, ?, ?)\n             ON CONFLICT(job_id) DO NOTHING",
        )
        .bind(job_id.to_string())
        .bind(session.0.to_string())
        .bind(endpoint_id)
        .bind(&cmd_json)
        .bind(tool_call_ref)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 1 {
            Ok(AsyncEnqueueOutcome::Inserted)
        } else {
            Ok(AsyncEnqueueOutcome::Existing(Box::new(
                self.get_async_job(job_id).await?.ok_or_else(|| {
                    QueueStoreError::Corrupt("conflict reported but async row absent".into())
                })?,
            )))
        }
    }

    /// Reads one job row, if present.
    pub async fn get_async_job(&self, job_id: Uuid) -> Result<Option<AsyncJob>, QueueStoreError> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                i64,
                i64,
            ),
        >(
            "SELECT job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM async_jobs WHERE job_id = ?",
        )
        .bind(job_id.to_string())
        .fetch_optional(self.pool())
        .await?;
        row.map(parse_async_row).transpose()
    }

    /// `pending → inflight`. Returns false when the row is no longer
    /// `pending` (re-drive of an already-inflight row, or terminal).
    pub async fn mark_async_inflight(&self, job_id: Uuid) -> Result<bool, QueueStoreError> {
        let result = sqlx::query(
            "UPDATE async_jobs SET status = 'inflight', updated_at = ? WHERE job_id = ? AND status = 'pending'",
        )
        .bind(now_ms())
        .bind(job_id.to_string())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// `pending|inflight → done`, recording the completion value.
    /// Returns false (and changes nothing) when the row is terminal —
    /// a committed `done`/`cancelled`/`abandoned` row is never mutated.
    pub async fn mark_async_done(
        &self,
        job_id: Uuid,
        result: &serde_json::Value,
    ) -> Result<bool, QueueStoreError> {
        let result_json = serde_json::to_string(result)
            .map_err(|e| QueueStoreError::Corrupt(format!("result not serializable: {e}")))?;
        let outcome = sqlx::query(
            "UPDATE async_jobs SET status = 'done', result = ?, updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
        )
        .bind(result_json)
        .bind(now_ms())
        .bind(job_id.to_string())
        .execute(self.pool())
        .await?;
        Ok(outcome.rows_affected() == 1)
    }

    /// `pending|inflight → cancelled` (the `stop` tool). Honest
    /// cancellation: the row keeps the label forever; it never erases
    /// the job. Returns false when the row already settled.
    pub async fn mark_async_cancelled(&self, job_id: Uuid) -> Result<bool, QueueStoreError> {
        let result = sqlx::query(
            "UPDATE async_jobs SET status = 'cancelled', updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
        )
        .bind(now_ms())
        .bind(job_id.to_string())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// `pending|inflight → abandoned` (queue timeout). Returns false
    /// when the row already settled.
    pub async fn mark_async_abandoned(&self, job_id: Uuid) -> Result<bool, QueueStoreError> {
        let result = sqlx::query(
            "UPDATE async_jobs SET status = 'abandoned', updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
        )
        .bind(now_ms())
        .bind(job_id.to_string())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Job rows in any of `statuses`, oldest first (the startup
    /// re-drive / redelivery sweep's read).
    pub async fn async_jobs_with_status(
        &self,
        statuses: &[AsyncJobStatus],
    ) -> Result<Vec<AsyncJob>, QueueStoreError> {
        let placeholders = statuses
            .iter()
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM async_jobs WHERE status IN ({placeholders}) ORDER BY created_at ASC, job_id ASC"
        );
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                i64,
                i64,
            ),
        >(&sql)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(parse_async_row).collect()
    }
}

/// Parses one async job row tuple.
fn parse_async_row(
    (job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at): (
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        i64,
        i64,
    ),
) -> Result<AsyncJob, QueueStoreError> {
    let job_id = Uuid::parse_str(&job_id)
        .map_err(|e| QueueStoreError::Corrupt(format!("bad job id: {e}")))?;
    let session = kian_core::model::SessionId(
        Uuid::parse_str(&session)
            .map_err(|e| QueueStoreError::Corrupt(format!("bad session id: {e}")))?,
    );
    let cmd: ExecCommand = serde_json::from_str(&cmd)
        .map_err(|e| QueueStoreError::Corrupt(format!("bad cmd json: {e}")))?;
    let status = AsyncJobStatus::from_str(&status)?;
    let result = match (&result, status) {
        (None, AsyncJobStatus::Done) => {
            return Err(QueueStoreError::Corrupt(
                "done async row without result".into(),
            ));
        }
        (None, _) => None,
        (Some(raw), _) => Some(
            serde_json::from_str(raw)
                .map_err(|e| QueueStoreError::Corrupt(format!("bad result json: {e}")))?,
        ),
    };
    Ok(AsyncJob {
        job_id,
        session,
        endpoint_id,
        cmd,
        status,
        tool_call_ref,
        result,
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::seams::ExecCommand;
    use uuid::Uuid;

    use super::{AsyncEnqueueOutcome, AsyncJobStatus};
    use crate::store::{ExecStore, Migration, chain};

    fn temp_db() -> PathBuf {
        std::env::temp_dir().join(format!("kian-exec-async-{}.sqlite", Uuid::new_v4()))
    }

    fn cmd() -> ExecCommand {
        ExecCommand {
            argv: vec!["sleep".into(), "1".into()],
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        }
    }

    /// v1 is a committed generation: pinned byte-for-byte so any
    /// rewrite of history trips this test (C5 frozen-golden pattern).
    #[test]
    fn v1_generation_sql_is_frozen() {
        let v1: &Migration = &chain()[1];
        assert_eq!(v1.version, 1);
        assert_eq!(v1.name, "v1_async_jobs");
        let golden = concat!(
            "CREATE TABLE IF NOT EXISTS async_jobs (\n",
            "    job_id TEXT PRIMARY KEY,\n",
            "    session TEXT NOT NULL,\n",
            "    endpoint_id TEXT NOT NULL,\n",
            "    cmd TEXT NOT NULL,\n",
            "    status TEXT NOT NULL,\n",
            "    tool_call_ref TEXT,\n",
            "    result TEXT,\n",
            "    created_at INTEGER NOT NULL,\n",
            "    updated_at INTEGER NOT NULL\n",
            ");\n",
            "CREATE INDEX IF NOT EXISTS idx_async_jobs_drive ON async_jobs (status, created_at)",
        );
        assert_eq!(v1.up, golden);
    }

    /// Review assertion (C5 pattern, applied to this table): no
    /// deletes, and every mutation carries a status guard so committed
    /// terminal rows are immutable.
    #[test]
    fn async_rows_are_guarded_and_never_deleted() {
        let src = include_str!("async_jobs.rs");
        let delete = format!("{} {}", "DELETE", "FROM");
        assert!(
            !src.to_uppercase().contains(&delete.to_uppercase()),
            "async job rows are durable audit state — deletion is forbidden"
        );
        for line in src.lines() {
            let trimmed = line.trim_start();
            if trimmed.to_uppercase().starts_with("UPDATE ASYNC_JOBS") {
                assert!(
                    line.contains("AND status"),
                    "unguarded async_jobs mutation: {line}"
                );
            }
        }
    }

    #[tokio::test]
    async fn async_job_transitions_are_guarded_and_terminal_rows_freeze() {
        let db = temp_db();
        let store = ExecStore::open(&db).await.unwrap();
        let job = Uuid::new_v4();
        let session = kian_core::model::SessionId(Uuid::new_v4());

        // Admit-before-execute: the row is pending before anything else.
        assert!(matches!(
            store
                .insert_async_job(job, session, "ep1", &cmd(), Some("call-1"))
                .await
                .unwrap(),
            AsyncEnqueueOutcome::Inserted
        ));
        // Dedupe: the same job id never makes a second row.
        assert!(matches!(
            store
                .insert_async_job(job, session, "ep1", &cmd(), Some("call-1"))
                .await
                .unwrap(),
            AsyncEnqueueOutcome::Existing(_)
        ));

        assert!(store.mark_async_inflight(job).await.unwrap());
        assert!(!store.mark_async_inflight(job).await.unwrap());

        // done settles with the result.
        assert!(
            store
                .mark_async_done(job, &serde_json::json!({"stdout": "x"}))
                .await
                .unwrap()
        );
        let row = store.get_async_job(job).await.unwrap().unwrap();
        assert_eq!(row.status, AsyncJobStatus::Done);
        assert!(row.status.is_terminal());
        assert_eq!(row.result, Some(serde_json::json!({"stdout": "x"})));
        assert_eq!(row.tool_call_ref.as_deref(), Some("call-1"));
        assert_eq!(row.session, session);

        // Terminal: nothing may follow — not done again, not a
        // cancellation, not an abandonment.
        assert!(
            !store
                .mark_async_done(job, &serde_json::json!({"stdout": "y"}))
                .await
                .unwrap()
        );
        assert!(!store.mark_async_cancelled(job).await.unwrap());
        assert!(!store.mark_async_abandoned(job).await.unwrap());
        let row = store.get_async_job(job).await.unwrap().unwrap();
        assert_eq!(row.result, Some(serde_json::json!({"stdout": "x"})));

        store.close().await;
        let _ = std::fs::remove_file(&db);
    }

    #[tokio::test]
    async fn cancelled_is_terminal_and_sweeps_read_by_status() {
        let db = temp_db();
        let store = ExecStore::open(&db).await.unwrap();
        let session = kian_core::model::SessionId(Uuid::new_v4());

        let cancelled_job = Uuid::new_v4();
        store
            .insert_async_job(cancelled_job, session, "ep1", &cmd(), None)
            .await
            .unwrap();
        assert!(store.mark_async_cancelled(cancelled_job).await.unwrap());
        // Cancellation is an event, not an erasure: the row keeps the
        // label and no result was invented.
        let row = store.get_async_job(cancelled_job).await.unwrap().unwrap();
        assert_eq!(row.status, AsyncJobStatus::Cancelled);
        assert_eq!(row.result, None);

        let live_job = Uuid::new_v4();
        store
            .insert_async_job(live_job, session, "ep1", &cmd(), None)
            .await
            .unwrap();
        store.mark_async_inflight(live_job).await.unwrap();

        let pending: Vec<_> = store
            .async_jobs_with_status(&[AsyncJobStatus::Pending, AsyncJobStatus::Inflight])
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].job_id, live_job);

        let settled: Vec<_> = store
            .async_jobs_with_status(&[
                AsyncJobStatus::Done,
                AsyncJobStatus::Cancelled,
                AsyncJobStatus::Abandoned,
            ])
            .await
            .unwrap();
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].job_id, cancelled_job);

        store.close().await;
        let _ = std::fs::remove_file(&db);
    }
}
