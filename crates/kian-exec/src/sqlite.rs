//! The sqlite backend: the statements `ExecStore` shipped in
//! , extracted verbatim from the dispatching enum so the
//! dialect lives here and only here. No statement changes semantics —
//! sqlite behavior is byte-identical to the legacy store (the zero
//! sanctioned-edit goal).

use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

use crate::store::{CommandOutcome, EndpointRegistryRow};

/// One `exec_queue` row's raw columns (parsed by the shared
/// `store::parse_row`).
pub(crate) type QueueRow = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
    i64,
);

pub(crate) type PendingAbandonmentRow = (String, String, Option<String>, i64, i64);

/// One `endpoint_registry` row's raw columns (ADR-025 adds the
/// last-declared description/version).
pub(crate) type RegistryRow = (
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
);

/// One `async_jobs` row's raw columns (parsed by the shared
/// `async_jobs::parse_async_row`).
pub(crate) type AsyncRow = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
);

// -- exec_queue -------------------------------------------------------------

pub(crate) async fn enqueue(
    pool: &SqlitePool,
    command_id: Uuid,
    endpoint_id: &str,
    cmd_json: &str,
    kind: &str,
    tool_call_ref: Option<&str>,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO exec_queue (command_id, endpoint_id, cmd, kind, status, tool_call_ref, result, created_at, updated_at)\n             VALUES (?, ?, ?, ?, 'pending', ?, NULL, ?, ?)\n             ON CONFLICT(command_id) DO NOTHING",
    )
    .bind(command_id.to_string())
    .bind(endpoint_id)
    .bind(cmd_json)
    .bind(kind)
    .bind(tool_call_ref)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn get(
    pool: &SqlitePool,
    command_id: Uuid,
) -> Result<Option<QueueRow>, sqlx::Error> {
    sqlx::query_as::<_, QueueRow>(
        "SELECT command_id, endpoint_id, cmd, kind, status, tool_call_ref, result, created_at, updated_at, dispatch_attempts FROM exec_queue WHERE command_id = ?",
    )
    .bind(command_id.to_string())
    .fetch_optional(pool)
    .await
}

/// `pending → inflight`; returns rows affected (0 = guarded off).
pub(crate) async fn mark_inflight(
    pool: &SqlitePool,
    command_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'inflight', updated_at = ? WHERE command_id = ? AND status = 'pending'",
    )
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// `pending|inflight → done`; returns rows affected (0 = terminal).
pub(crate) async fn mark_done(
    pool: &SqlitePool,
    command_id: Uuid,
    outcome: &CommandOutcome,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'done', result = ?, updated_at = ? WHERE command_id = ? AND status IN ('pending', 'inflight')",
    )
    .bind(outcome.to_json())
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// `pending → abandoned`; returns rows affected (0 = not pending).
pub(crate) async fn mark_abandoned(
    pool: &SqlitePool,
    command_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'abandoned', updated_at = ? WHERE command_id = ? AND status = 'pending'",
    )
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn abandon_aged_pending(
    pool: &SqlitePool,
    created_before: i64,
    now: i64,
) -> Result<Vec<PendingAbandonmentRow>, sqlx::Error> {
    sqlx::query_as::<_, PendingAbandonmentRow>(
        "UPDATE exec_queue SET status = 'abandoned', updated_at = ? WHERE status = 'pending' AND created_at <= ? AND (claimed_by IS NULL OR claimed_until_ms < ?) RETURNING command_id, endpoint_id, tool_call_ref, created_at, dispatch_attempts",
    )
    .bind(now)
    .bind(created_before)
    .bind(now)
    .fetch_all(pool)
    .await
}

pub(crate) async fn drivable(
    pool: &SqlitePool,
    endpoint_id: &str,
) -> Result<Vec<QueueRow>, sqlx::Error> {
    sqlx::query_as::<_, QueueRow>(
        "SELECT command_id, endpoint_id, cmd, kind, status, tool_call_ref, result, created_at, updated_at, dispatch_attempts FROM exec_queue WHERE endpoint_id = ? AND status IN ('pending', 'inflight') ORDER BY created_at ASC, command_id ASC",
    )
    .bind(endpoint_id)
    .fetch_all(pool)
    .await
}

// -- queue-row leases ---------------------------------------------------

/// Single-statement row lease (ADR-009 decision 5): the claimant
/// takes ownership iff the row is live (`pending`/`inflight` — terminal
/// rows freeze) and the row is unclaimed, expired, or already this
/// holder's (same-holder re-drive keeps single-replica behavior
/// unchanged, the condition-(c) discipline). `RETURNING` yields
/// the row exactly when ownership landed. The lease is *liveness*
/// only — it bounds duplicate dispatch across replicas; the command-id
/// correlation (ADR-007) remains the safety net.
pub(crate) async fn claim_queue_row(
    pool: &SqlitePool,
    command_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
    max_dispatches: i64,
) -> Result<bool, sqlx::Error> {
    let claimed: Option<String> = sqlx::query_scalar(
        "UPDATE exec_queue SET claimed_by = ?, claimed_until_ms = ?, updated_at = ? WHERE command_id = ? AND status IN ('pending', 'inflight') AND (claimed_by IS NULL OR claimed_until_ms < ? OR claimed_by = ?) AND (status = 'pending' OR dispatch_attempts < ?) RETURNING command_id",
    )
    .bind(holder)
    .bind(now + ttl_ms)
    .bind(now)
    .bind(command_id.to_string())
    .bind(now)
    .bind(holder)
    .bind(max_dispatches)
    .fetch_optional(pool)
    .await?;
    Ok(claimed.is_some())
}

pub(crate) async fn commit_dispatch_attempt(
    pool: &SqlitePool,
    command_id: Uuid,
    holder: &str,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET dispatch_attempts = dispatch_attempts + 1, updated_at = ? WHERE command_id = ? AND claimed_by = ? AND status = 'inflight'",
    )
    .bind(now)
    .bind(command_id.to_string())
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Converges an exhausted re-drive (ADR-033): `inflight → abandoned` iff the
/// dispatch count is spent and no live lease exists. The `claimed_until_ms`
/// guard is what prevents an abandonment race against a driver that is
/// dispatching this row right now.
pub(crate) async fn abandon_exhausted(
    pool: &SqlitePool,
    command_id: Uuid,
    max_dispatches: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'abandoned', updated_at = ? WHERE command_id = ? AND status = 'inflight' AND dispatch_attempts >= ? AND (claimed_by IS NULL OR claimed_until_ms < ?)",
    )
    .bind(now)
    .bind(command_id.to_string())
    .bind(max_dispatches)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Refunds one dispatch attempt iff this holder claimed the row and one
/// attempt is on the books. See `ExecStore::refund_dispatch_attempt`.
pub(crate) async fn refund_dispatch_attempt(
    pool: &SqlitePool,
    command_id: Uuid,
    holder: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET dispatch_attempts = dispatch_attempts - 1 WHERE command_id = ? AND claimed_by = ? AND dispatch_attempts > 0",
    )
    .bind(command_id.to_string())
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Extends the lease iff this holder still owns the row and the row is
/// live. `false` = ownership was lost (expired and reclaimed by another
/// replica) or the row settled — the caller stops treating itself as
/// the owner (never a write permission: the lease never gates results).
pub(crate) async fn heartbeat_queue_row(
    pool: &SqlitePool,
    command_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET claimed_until_ms = ? WHERE command_id = ? AND claimed_by = ? AND status IN ('pending', 'inflight')",
    )
    .bind(now + ttl_ms)
    .bind(command_id.to_string())
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// The async-jobs twin of [`claim_queue_row`] (same single-statement
/// lease discipline; the job's *dispatch* is additionally gated by the
/// `exec_queue` row it rides — the job id is the command id).
pub(crate) async fn claim_async_row(
    pool: &SqlitePool,
    job_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let claimed: Option<String> = sqlx::query_scalar(
        "UPDATE async_jobs SET claimed_by = ?, claimed_until_ms = ? WHERE job_id = ? AND status IN ('pending', 'inflight') AND (claimed_by IS NULL OR claimed_until_ms < ? OR claimed_by = ?) RETURNING job_id",
    )
    .bind(holder)
    .bind(now + ttl_ms)
    .bind(job_id.to_string())
    .bind(now)
    .bind(holder)
    .fetch_optional(pool)
    .await?;
    Ok(claimed.is_some())
}

/// The async-jobs twin of [`heartbeat_queue_row`].
pub(crate) async fn heartbeat_async_row(
    pool: &SqlitePool,
    job_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET claimed_until_ms = ? WHERE job_id = ? AND claimed_by = ? AND status IN ('pending', 'inflight')",
    )
    .bind(now + ttl_ms)
    .bind(job_id.to_string())
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

// -- endpoint_registry ------------------------------------------------------

pub(crate) async fn upsert_endpoint(
    pool: &SqlitePool,
    endpoint_id: &str,
    addr: &str,
    last_heartbeat_ms: u64,
    state: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO endpoint_registry (endpoint_id, addr, last_heartbeat_ms, state) VALUES (?, ?, ?, ?)\n             ON CONFLICT(endpoint_id) DO UPDATE SET addr = excluded.addr, last_heartbeat_ms = excluded.last_heartbeat_ms, state = excluded.state",
    )
    .bind(endpoint_id)
    .bind(addr)
    .bind(last_heartbeat_ms as i64)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_endpoint_with_declaration(
    pool: &SqlitePool,
    endpoint_id: &str,
    addr: &str,
    last_heartbeat_ms: u64,
    state: &str,
    description: Option<&str>,
    protocol_version: u32,
    capabilities: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO endpoint_registry (endpoint_id, addr, last_heartbeat_ms, state, description, protocol_version, capabilities) VALUES (?, ?, ?, ?, ?, ?, ?)\n             ON CONFLICT(endpoint_id) DO UPDATE SET addr = excluded.addr, last_heartbeat_ms = excluded.last_heartbeat_ms, state = excluded.state, description = excluded.description, protocol_version = excluded.protocol_version, capabilities = excluded.capabilities",
    )
    .bind(endpoint_id)
    .bind(addr)
    .bind(last_heartbeat_ms as i64)
    .bind(state)
    .bind(description)
    .bind(i64::from(protocol_version))
    .bind(serde_json::to_string(capabilities).unwrap_or_else(|_| "[]".into()))
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn endpoints(pool: &SqlitePool) -> Result<Vec<EndpointRegistryRow>, sqlx::Error> {
    let rows: Vec<RegistryRow> =
        sqlx::query_as("SELECT endpoint_id, addr, last_heartbeat_ms, state, description, protocol_version, capabilities FROM endpoint_registry ORDER BY endpoint_id ASC")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(
            |(endpoint_id, addr, hb, state, description, protocol_version, capabilities)| {
                let capabilities = capabilities
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                EndpointRegistryRow {
                    endpoint_id,
                    addr,
                    last_heartbeat_ms: hb.max(0) as u64,
                    state,
                    description,
                    protocol_version: protocol_version.and_then(|v| u32::try_from(v).ok()),
                    capabilities,
                }
            },
        )
        .collect())
}

// -- async_jobs -------------------------------------------------------------

pub(crate) async fn insert_async_job(
    pool: &SqlitePool,
    job_id: Uuid,
    session: &str,
    endpoint_id: &str,
    cmd_json: &str,
    tool_call_ref: Option<&str>,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO async_jobs (job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at)\n             VALUES (?, ?, ?, ?, 'pending', ?, NULL, ?, ?)\n             ON CONFLICT(job_id) DO NOTHING",
    )
    .bind(job_id.to_string())
    .bind(session)
    .bind(endpoint_id)
    .bind(cmd_json)
    .bind(tool_call_ref)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn get_async_job(
    pool: &SqlitePool,
    job_id: Uuid,
) -> Result<Option<AsyncRow>, sqlx::Error> {
    sqlx::query_as::<_, AsyncRow>(
        "SELECT job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM async_jobs WHERE job_id = ?",
    )
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await
}

pub(crate) async fn mark_async_inflight(
    pool: &SqlitePool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'inflight', updated_at = ? WHERE job_id = ? AND status = 'pending'",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_done(
    pool: &SqlitePool,
    job_id: Uuid,
    result_json: &str,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'done', result = ?, updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
    )
    .bind(result_json)
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_cancelled(
    pool: &SqlitePool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'cancelled', updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_abandoned(
    pool: &SqlitePool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'abandoned', updated_at = ? WHERE job_id = ? AND status IN ('pending', 'inflight')",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Statuses come from the closed `AsyncJobStatus` enum (never caller
/// text), so the `IN` list is assembled from trusted literals.
pub(crate) async fn async_jobs_with_status(
    pool: &SqlitePool,
    statuses: &[&str],
) -> Result<Vec<AsyncRow>, sqlx::Error> {
    let list = statuses
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM async_jobs WHERE status IN ({list}) ORDER BY created_at ASC, job_id ASC"
    );
    sqlx::query_as::<_, AsyncRow>(&sql).fetch_all(pool).await
}
