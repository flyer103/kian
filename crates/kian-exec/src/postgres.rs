//! The postgres backend: mirrors the sqlite statements with `$N`
//! placeholders and explicit casts (pg parameter-type inference is
//! context-driven; a bare `$N` can infer `integer` and reject an int8
//! bind — the lesson). Semantics are the sqlite store's semantics,
//! transported: ADR-007's enqueue-first / at-least-once / command-id
//! correlation / honest timeout is unchanged, only the dialect is.

use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::store::{CommandOutcome, EndpointRegistryRow};

pub(crate) type QueueRow = super::sqlite::QueueRow;
pub(crate) type AsyncRow = super::sqlite::AsyncRow;

// -- exec_queue -------------------------------------------------------------

pub(crate) async fn enqueue(
    pool: &PgPool,
    command_id: Uuid,
    endpoint_id: &str,
    cmd_json: &str,
    tool_call_ref: Option<&str>,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO exec_queue (command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at)\n             VALUES ($1::text, $2::text, $3::text, 'pending', $4::text, NULL, $5::bigint, $6::bigint)\n             ON CONFLICT(command_id) DO NOTHING",
    )
    .bind(command_id.to_string())
    .bind(endpoint_id)
    .bind(cmd_json)
    .bind(tool_call_ref)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn get(pool: &PgPool, command_id: Uuid) -> Result<Option<QueueRow>, sqlx::Error> {
    sqlx::query_as::<_, QueueRow>(
        "SELECT command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM exec_queue WHERE command_id = $1::text",
    )
    .bind(command_id.to_string())
    .fetch_optional(pool)
    .await
}

/// `pending → inflight`; returns rows affected (0 = guarded off).
pub(crate) async fn mark_inflight(
    pool: &PgPool,
    command_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'inflight', updated_at = $1::bigint WHERE command_id = $2::text AND status = 'pending'",
    )
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// `pending|inflight → done`; returns rows affected (0 = terminal).
pub(crate) async fn mark_done(
    pool: &PgPool,
    command_id: Uuid,
    outcome: &CommandOutcome,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'done', result = $1::text, updated_at = $2::bigint WHERE command_id = $3::text AND status IN ('pending', 'inflight')",
    )
    .bind(outcome.to_json())
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// `pending|inflight → abandoned`; returns rows affected (0 = terminal).
pub(crate) async fn mark_abandoned(
    pool: &PgPool,
    command_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET status = 'abandoned', updated_at = $1::bigint WHERE command_id = $2::text AND status IN ('pending', 'inflight')",
    )
    .bind(now)
    .bind(command_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn drivable(
    pool: &PgPool,
    endpoint_id: &str,
) -> Result<Vec<QueueRow>, sqlx::Error> {
    sqlx::query_as::<_, QueueRow>(
        "SELECT command_id, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM exec_queue WHERE endpoint_id = $1::text AND status IN ('pending', 'inflight') ORDER BY created_at ASC, command_id ASC",
    )
    .bind(endpoint_id)
    .fetch_all(pool)
    .await
}

// -- queue-row leases (pg twins of the sqlite statements) ---------------

pub(crate) async fn claim_queue_row(
    pool: &PgPool,
    command_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let claimed: Option<String> = sqlx::query_scalar(
        "UPDATE exec_queue SET claimed_by = $1::text, claimed_until_ms = $2::bigint WHERE command_id = $3::text AND status IN ('pending', 'inflight') AND (claimed_by IS NULL OR claimed_until_ms < $4::bigint OR claimed_by = $1::text) RETURNING command_id",
    )
    .bind(holder)
    .bind(now + ttl_ms)
    .bind(command_id.to_string())
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(claimed.is_some())
}

pub(crate) async fn heartbeat_queue_row(
    pool: &PgPool,
    command_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE exec_queue SET claimed_until_ms = $1::bigint WHERE command_id = $2::text AND claimed_by = $3::text AND status IN ('pending', 'inflight')",
    )
    .bind(now + ttl_ms)
    .bind(command_id.to_string())
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn claim_async_row(
    pool: &PgPool,
    job_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let claimed: Option<String> = sqlx::query_scalar(
        "UPDATE async_jobs SET claimed_by = $1::text, claimed_until_ms = $2::bigint WHERE job_id = $3::text AND status IN ('pending', 'inflight') AND (claimed_by IS NULL OR claimed_until_ms < $4::bigint OR claimed_by = $1::text) RETURNING job_id",
    )
    .bind(holder)
    .bind(now + ttl_ms)
    .bind(job_id.to_string())
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(claimed.is_some())
}

pub(crate) async fn heartbeat_async_row(
    pool: &PgPool,
    job_id: Uuid,
    holder: &str,
    ttl_ms: i64,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET claimed_until_ms = $1::bigint WHERE job_id = $2::text AND claimed_by = $3::text AND status IN ('pending', 'inflight')",
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
    pool: &PgPool,
    endpoint_id: &str,
    addr: &str,
    last_heartbeat_ms: u64,
    state: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO endpoint_registry (endpoint_id, addr, last_heartbeat_ms, state) VALUES ($1::text, $2::text, $3::bigint, $4::text)\n             ON CONFLICT(endpoint_id) DO UPDATE SET addr = EXCLUDED.addr, last_heartbeat_ms = EXCLUDED.last_heartbeat_ms, state = EXCLUDED.state",
    )
    .bind(endpoint_id)
    .bind(addr)
    .bind(last_heartbeat_ms as i64)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn endpoints(pool: &PgPool) -> Result<Vec<EndpointRegistryRow>, sqlx::Error> {
    let rows: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT endpoint_id, addr, last_heartbeat_ms, state FROM endpoint_registry ORDER BY endpoint_id ASC")
            .fetch_all(pool)
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

// -- async_jobs -------------------------------------------------------------

pub(crate) async fn insert_async_job(
    pool: &PgPool,
    job_id: Uuid,
    session: &str,
    endpoint_id: &str,
    cmd_json: &str,
    tool_call_ref: Option<&str>,
    now: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO async_jobs (job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at)\n             VALUES ($1::text, $2::text, $3::text, $4::text, 'pending', $5::text, NULL, $6::bigint, $7::bigint)\n             ON CONFLICT(job_id) DO NOTHING",
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
    pool: &PgPool,
    job_id: Uuid,
) -> Result<Option<AsyncRow>, sqlx::Error> {
    sqlx::query_as::<_, AsyncRow>(
        "SELECT job_id, session, endpoint_id, cmd, status, tool_call_ref, result, created_at, updated_at FROM async_jobs WHERE job_id = $1::text",
    )
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await
}

pub(crate) async fn mark_async_inflight(
    pool: &PgPool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'inflight', updated_at = $1::bigint WHERE job_id = $2::text AND status = 'pending'",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_done(
    pool: &PgPool,
    job_id: Uuid,
    result_json: &str,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'done', result = $1::text, updated_at = $2::bigint WHERE job_id = $3::text AND status IN ('pending', 'inflight')",
    )
    .bind(result_json)
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_cancelled(
    pool: &PgPool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'cancelled', updated_at = $1::bigint WHERE job_id = $2::text AND status IN ('pending', 'inflight')",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub(crate) async fn mark_async_abandoned(
    pool: &PgPool,
    job_id: Uuid,
    now: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE async_jobs SET status = 'abandoned', updated_at = $1::bigint WHERE job_id = $2::text AND status IN ('pending', 'inflight')",
    )
    .bind(now)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Statuses come from the closed `AsyncJobStatus` enum (never caller
/// text), so the `IN` list is assembled from trusted literals — the
/// same list sqlite uses.
pub(crate) async fn async_jobs_with_status(
    pool: &PgPool,
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
