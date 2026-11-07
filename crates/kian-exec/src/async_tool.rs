//! The async `start()`/`stop()` tool pair (ADR-008, DESIGN
//! §4.3/§6.2) — the article's §4.1 per-session full-duplex in kian's
//! native shape.
//!
//! The input half of full-duplex *is* invariant 3 (inputs are durable
//! rows promoted at provider-turn boundaries — already true); the
//! completion half *is* an admit→wake→promote round-trip. The only
//! gap was the tool wrapper, and that is all this module adds:
//!
//! - [`AsyncExecTool`] (`start`) enqueues the command through the
//!   **unchanged** durable queue and returns **immediately** with
//!   `{"status":"started","job_id":…}` as an honest `tool/result` —
//!   the session's drain is never blocked by the job (the built-in
//!   loop in `kian-loop` is untouched; the tool simply resolves
//!   fast). The job id is the command id derived from the tool-call
//!   id ([`crate::tool::command_id_for_tool_call`]), so recovery and
//!   replay dedupe at the row level exactly like the sync tool.
//! - A detached observer task awaits the correlated result (the
//!   existing per-endpoint queue drivers do the dispatch), settles
//!   the `async_jobs` row, and hands the settled row to the
//!   [`AsyncResultSink`] — the kian-cli completion bridge, which
//!   admits it to the originating session's inbox as a
//!   provenance-wrapped `{"kian":{"type":"async_result",…}}` input
//!   and wakes it (invariant 1: row durable before the wake). The
//!   completion is consumed at the next safe boundary as a normal
//!   message — no loop change, no new event kind.
//! - [`StopTool`] (`stop`) cancels by job id: the row settles
//!   `cancelled` (honest cancellation — an event, not an erasure),
//!   the in-flight command is killed best-effort (the v0 wire has no
//!   kill: a not-yet-dispatched queue row is abandoned so it never
//!   flies; an already-running one is unkillable and its late result
//!   is discarded by the queue's terminal guard), and the completion
//!   input still lands, labeled `cancelled`.
//!
//! [`AsyncJobs::redrive_pending`] re-drives `pending`/`inflight` rows
//! after a server restart (observers re-attach through
//! `exec_tracked`, which answers settled command ids from the queue
//! row — one row, one dispatch, ever); [`AsyncJobs::redeliver_settled`]
//! re-offers settled rows to the sink so a crash between settle and
//! deliver cannot lose a completion (the bridge's idempotency key
//! dedupes the admit).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::{ExecError, ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema};
use serde_json::json;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::RemoteExecBackend;
use crate::async_jobs::{AsyncEnqueueOutcome, AsyncJob, AsyncJobStatus};
use crate::tool::command_id_for_tool_call;

/// The completion half of the bridge: hand one **settled** job row to
/// the originating session's world. Implemented by the kian-cli
/// assembly over the super-agent `InboxWriter` port — kian-exec
/// itself never names an inbox, an engine, or an event (the layered
/// iron rule; this crate's whole session-facing surface is this
/// callback).
pub trait AsyncResultSink: Send + Sync {
    /// Offers the settled row. `Err` means "not durable" — the caller
    /// logs honestly; nothing is retried in-process (the startup
    /// redelivery sweep is the recovery path).
    fn deliver(&self, job: AsyncJob) -> BoxFuture<'static, Result<(), String>>;
}

/// Resolves the session that issued a tool call, from the committed
/// `tool/call` event (the loop persists it **before** `invoke` runs,
/// constraint 2 — so the lookup always finds it). Assembly provides
/// the events-pool-backed implementation; kian-exec stays
/// store-agnostic.
pub type SessionForCall = Arc<dyn Fn(&str) -> BoxFuture<'static, Option<SessionId>> + Send + Sync>;

/// The shared async-jobs engine: one per process, mounted behind the
/// tool pair. Owns the observer tasks and their cancellation
/// notifiers.
pub struct AsyncJobs {
    backend: Arc<RemoteExecBackend>,
    endpoint_id: String,
    sink: Arc<dyn AsyncResultSink>,
    session_for_call: SessionForCall,
    /// job id → cancel notifier for the currently-running observer.
    running: Mutex<HashMap<Uuid, Arc<Notify>>>,
}

fn store_error(e: crate::QueueStoreError) -> ToolError {
    ToolError::Execution(format!("async job store: {e}"))
}

impl AsyncJobs {
    /// Builds the engine over the shared remote-exec backend. All
    /// jobs route through `endpoint_id` (the same route the sync
    /// `exec` tool uses).
    pub fn new(
        backend: Arc<RemoteExecBackend>,
        endpoint_id: impl Into<String>,
        sink: Arc<dyn AsyncResultSink>,
        session_for_call: SessionForCall,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            endpoint_id: endpoint_id.into(),
            sink,
            session_for_call,
            running: Mutex::new(HashMap::new()),
        })
    }

    /// The `start` half: admit the job row (durable before dispatch),
    /// make sure exactly one observer drives it, return immediately.
    pub async fn start(self: &Arc<Self>, call: ToolCall) -> Result<ToolResult, ToolError> {
        let cmd = crate::tool::parse_exec(&call.arguments)?;
        // The invoking session is derived from the committed
        // tool/call event — the row cannot be admitted without it
        // (an honest error, never a guess).
        let Some(session) = (self.session_for_call)(&call.id).await else {
            return Err(ToolError::Execution(
                "cannot resolve the invoking session for the async job".into(),
            ));
        };
        let job_id = command_id_for_tool_call(&call.id);
        let outcome = self
            .backend
            .store()
            .insert_async_job(job_id, session, &self.endpoint_id, &cmd, Some(&call.id))
            .await
            .map_err(store_error)?;
        if let AsyncEnqueueOutcome::Existing(row) = &outcome {
            tracing::debug!(
                job_id = %job_id,
                status = row.status.as_str(),
                "async start deduped onto the existing job row"
            );
        }
        self.ensure_observer(job_id);
        Ok(ToolResult {
            call_id: call.id,
            output: json!({
                "status": "started",
                "job_id": job_id.to_string(),
            }),
            // The ack is honest even on the dedupe path: the job IS
            // started (its row exists; an observer is attached).
            is_error: false,
            // No exec output here — nothing was redacted.
            redactions: Vec::new(),
        })
    }

    /// The `stop` half: honest cancellation by job id.
    pub async fn stop(self: &Arc<Self>, call: ToolCall) -> Result<ToolResult, ToolError> {
        let raw = call
            .arguments
            .get("job_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ToolError::InvalidCall("stop requires a string `job_id`".into()))?;
        let job_id = Uuid::parse_str(raw)
            .map_err(|_| ToolError::InvalidCall(format!("`job_id` {raw:?} is not a uuid")))?;
        let store = self.backend.store();
        let Some(row) = store.get_async_job(job_id).await.map_err(store_error)? else {
            return Err(ToolError::InvalidCall(format!("unknown job_id `{raw}`")));
        };
        if row.status.is_terminal() {
            // Honest already-settled answer — never a fake cancel.
            return Ok(ToolResult {
                call_id: call.id,
                output: json!({
                    "status": row.status.as_str(),
                    "job_id": job_id.to_string(),
                    "note": "job already settled; nothing to cancel",
                }),
                is_error: false,
                redactions: Vec::new(),
            });
        }
        let cancelled = store
            .mark_async_cancelled(job_id)
            .await
            .map_err(store_error)?;
        if !cancelled {
            // Settled between the read and the CAS: report what won.
            let row = store.get_async_job(job_id).await.map_err(store_error)?;
            return Ok(ToolResult {
                call_id: call.id,
                output: json!({
                    "status": row
                        .as_ref()
                        .map(|r| r.status.as_str())
                        .unwrap_or("gone"),
                    "job_id": job_id.to_string(),
                    "note": "job settled while cancelling",
                }),
                is_error: false,
                redactions: Vec::new(),
            });
        }
        // Wake the observer so the cancelled completion lands now
        // (the row is the truth; the notifier is just latency).
        if let Some(notify) = self
            .running
            .lock()
            .expect("async running map")
            .get(&job_id)
            .cloned()
        {
            notify.notify_one();
        }
        Ok(ToolResult {
            call_id: call.id,
            output: json!({
                "status": "cancelled",
                "job_id": job_id.to_string(),
            }),
            is_error: false,
            redactions: Vec::new(),
        })
    }

    /// Spawns the observer for `job_id` unless one is already
    /// running. One observer per job, ever, per process.
    fn ensure_observer(self: &Arc<Self>, job_id: Uuid) {
        let mut running = self.running.lock().expect("async running map");
        if running.contains_key(&job_id) {
            return;
        }
        let notify = Arc::new(Notify::new());
        running.insert(job_id, Arc::clone(&notify));
        drop(running);
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.observe(job_id, notify).await;
        });
    }

    /// Drives one job to a settled row and delivers it. Exits on any
    /// terminal state; the observer slot is released on the way out
    /// so a later replayed `start` may re-attach (and merely
    /// re-deliver the settled row — idempotent at the bridge).
    async fn observe(self: Arc<Self>, job_id: Uuid, notify: Arc<Notify>) {
        let store = self.backend.store().clone();
        let Some(row) = store.get_async_job(job_id).await.unwrap_or_else(|e| {
            tracing::error!(job_id = %job_id, error = %e, "async observer: row read failed");
            None
        }) else {
            self.finish(job_id);
            return;
        };
        if row.status.is_terminal() {
            // Restart redelivery / late replay: answer from the row.
            self.deliver(row).await;
            self.finish(job_id);
            return;
        }
        // row lease: own the job row before observing. A fresh lease
        // held by another replica means *its* observer drives this job —
        // exit honestly (delivery is idempotent by the bridge's admit
        // key, and that replica's startup redeliver covers the handoff).
        // Liveness only: the dispatch itself is gated by the
        // `exec_queue` row lease; command-id correlation is the net.
        let (holder, ttl) = self.backend.row_lease();
        let ttl_ms = ttl.as_millis().min(i64::MAX as u128) as i64;
        match store
            .claim_async_row(job_id, holder, ttl_ms, crate::store::now_ms())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // The claim's CAS matched nothing. Two causes with
                // opposite obligations: (a) a live foreign lease —
                // another replica's observer drives this job, exit;
                // (b) the row settled between the read above and
                // this claim (`stop` won the race) — the settled row
                // must be delivered HERE: in-process only this
                // observer would, and the startup redelivery sweep
                // is a recovery path, not this turn's (a
                // cancelled completion used to be lost here until
                // the next process restart).
                match store.get_async_job(job_id).await {
                    Ok(Some(row)) if row.status.is_terminal() => {
                        tracing::info!(
                            job_id = %job_id,
                            status = row.status.as_str(),
                            "async observer: row settled while claiming; delivering"
                        );
                        self.deliver(row).await;
                    }
                    Ok(_) => {
                        tracing::debug!(
                            job_id = %job_id,
                            holder,
                            "async job row leased by another replica; this observer exits"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "async observer: claim-refused row read failed"
                        );
                    }
                }
                self.finish(job_id);
                return;
            }
            Err(e) => {
                tracing::warn!(job_id = %job_id, error = %e, "async row lease claim failed");
            }
        }
        let _ = store.mark_async_inflight(job_id).await;
        // Register the cancel watch BEFORE re-reading the status, so
        // a stop that races the registration still surfaces (Notify
        // permits are not lost).
        let cancelled = notify.notified();
        tokio::pin!(cancelled);
        if let Ok(Some(row)) = store.get_async_job(job_id).await
            && row.status == AsyncJobStatus::Cancelled
        {
            self.cancel_path(job_id).await;
            self.finish(job_id);
            return;
        }
        let cmd = row.cmd.clone();
        let tool_call_ref = row.tool_call_ref.clone();
        let endpoint_id = self.endpoint_id.clone();
        let backend = Arc::clone(&self.backend);
        tokio::select! {
            result = backend.exec_tracked(job_id, &endpoint_id, cmd, tool_call_ref) => {
                let (status, value) = match result {
                    Ok(r) => (
                        AsyncJobStatus::Done,
                        json!({
                            "exit_code": r.exit_code,
                            "stdout": r.stdout,
                            "stderr": r.stderr,
                            "timed_out": r.timed_out,
                        }),
                    ),
                    // The queue's honest bound (config `queue_timeout_ms`).
                    Err(ExecError::Timeout) => {
                        (AsyncJobStatus::Abandoned, json!({"error": "queue timeout"}))
                    }
                    Err(e) => (AsyncJobStatus::Done, json!({"error": e.to_string()})),
                };
                let updated = match status {
                    AsyncJobStatus::Abandoned => store.mark_async_abandoned(job_id).await,
                    _ => store.mark_async_done(job_id, &value).await,
                };
                match updated {
                    Ok(_) => {}
                    // A lost CAS means cancellation won the race — the
                    // durable row below says so and is what we deliver.
                    Err(e) => {
                        tracing::error!(job_id = %job_id, error = %e, "async observer: settle failed")
                    }
                }
                // Deliver whatever the durable row now says (a
                // cancellation that won the race is the honest label).
                match store.get_async_job(job_id).await {
                    Ok(Some(row)) => self.deliver(row).await,
                    Ok(None) => {}
                    Err(e) => tracing::error!(job_id = %job_id, error = %e, "async observer: final read failed"),
                }
            }
            _ = &mut cancelled => {
                self.cancel_path(job_id).await;
            }
        }
        self.finish(job_id);
    }

    /// The cancellation exit: best-effort kill of the underlying
    /// command, then deliver the row (labeled `cancelled`).
    async fn cancel_path(&self, job_id: Uuid) {
        let store = self.backend.store();
        // Best-effort kill. The v0 wire has no cancel frame, so: a
        // queue row that has not flown yet is abandoned (it never
        // will fly — abandoned is terminal, the driver skips it); a
        // command already executing on the endpoint is unkillable and
        // its late result is discarded by the queue's terminal guard
        // ("late result for a settled command discarded").
        match store.mark_abandoned(job_id).await {
            Ok(true) => tracing::info!(
                job_id = %job_id,
                "async cancel: queued command abandoned before dispatch"
            ),
            Ok(false) => tracing::info!(
                job_id = %job_id,
                "async cancel: command already settled or executing; late result will be discarded"
            ),
            Err(e) => {
                tracing::warn!(job_id = %job_id, error = %e, "async cancel: queue abandon failed")
            }
        }
        match store.get_async_job(job_id).await {
            Ok(Some(row)) => self.deliver(row).await,
            Ok(None) => {}
            Err(e) => {
                tracing::error!(job_id = %job_id, error = %e, "async cancel: row read failed")
            }
        }
    }

    /// Hands one settled row to the bridge.
    async fn deliver(&self, row: AsyncJob) {
        if !row.status.is_terminal() {
            tracing::warn!(
                job_id = %row.job_id,
                status = row.status.as_str(),
                "async deliver: refusing a non-terminal row"
            );
            return;
        }
        if let Err(e) = self.sink.deliver(row).await {
            tracing::error!(
                error = %e,
                "async completion delivery failed (startup redelivery is the recovery path)"
            );
        }
    }

    /// Releases the observer slot.
    fn finish(&self, job_id: Uuid) {
        self.running
            .lock()
            .expect("async running map")
            .remove(&job_id);
    }

    /// Startup re-drive: one observer per `pending`/`inflight` row
    /// (a server restart drops the drivers and reopens the pool; the
    /// observers re-attach through `exec_tracked`, which dedupes
    /// against the queue rows — at-least-once to the endpoint, once
    /// to the job).
    pub async fn redrive_pending(self: &Arc<Self>) {
        let rows = match self
            .backend
            .store()
            .async_jobs_with_status(&[AsyncJobStatus::Pending, AsyncJobStatus::Inflight])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "async bridge: pending-job sweep read failed");
                return;
            }
        };
        for row in rows {
            tracing::info!(
                job_id = %row.job_id,
                status = row.status.as_str(),
                "async bridge: re-driving job after restart"
            );
            self.ensure_observer(row.job_id);
        }
    }

    /// Startup redelivery: re-offer every settled row whose observer
    /// is not running (a crash between settle and deliver loses
    /// nothing — the bridge's admit idempotency key dedupes).
    pub async fn redeliver_settled(self: &Arc<Self>) {
        let rows = match self
            .backend
            .store()
            .async_jobs_with_status(&[
                AsyncJobStatus::Done,
                AsyncJobStatus::Cancelled,
                AsyncJobStatus::Abandoned,
            ])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "async bridge: settled-job sweep read failed");
                return;
            }
        };
        let running: Vec<Uuid> = self
            .running
            .lock()
            .expect("async running map")
            .keys()
            .copied()
            .collect();
        for row in rows {
            if running.contains(&row.job_id) {
                continue;
            }
            tracing::debug!(
                job_id = %row.job_id,
                status = row.status.as_str(),
                "async bridge: redelivering settled job"
            );
            self.deliver(row).await;
        }
    }
}

// ---------------------------------------------------------------------------
// The model-visible tools
// ---------------------------------------------------------------------------

/// The `start` tool: async remote exec. Same argument contract as
/// the sync `exec` tool (argv vector, optional cwd/timeout_ms).
#[derive(Clone)]
pub struct AsyncExecTool {
    jobs: Arc<AsyncJobs>,
}

impl AsyncExecTool {
    #[must_use]
    pub fn new(jobs: Arc<AsyncJobs>) -> Self {
        Self { jobs }
    }
}

impl ToolProvider for AsyncExecTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "start".into(),
            description: "Start a long-running command on the execution endpoint and return \
                immediately with a job_id; the result arrives later as an input message \
                (kian async_result)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "argv": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "command and arguments"
                    },
                    "cwd": {"type": "string"},
                    "timeout_ms": {"type": "integer"}
                },
                "required": ["argv"]
            }),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let jobs = Arc::clone(&self.jobs);
        Box::pin(async move { jobs.start(call).await })
    }
}

/// The `stop` tool: honest cancellation by job id.
#[derive(Clone)]
pub struct StopTool {
    jobs: Arc<AsyncJobs>,
}

impl StopTool {
    #[must_use]
    pub fn new(jobs: Arc<AsyncJobs>) -> Self {
        Self { jobs }
    }
}

impl ToolProvider for StopTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "stop".into(),
            description: "Cancel a running async job by job_id (best-effort kill; the \
                cancellation itself still arrives as a result)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "the job_id returned by start"
                    }
                },
                "required": ["job_id"]
            }),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let jobs = Arc::clone(&self.jobs);
        Box::pin(async move { jobs.stop(call).await })
    }
}

/// The mounted pair, one provider (schemas in stable order:
/// `start` first, `stop` second — tool schemas are prompt-prefix
/// content, so the order is part of the contract).
#[derive(Clone)]
pub struct AsyncToolPair {
    start: AsyncExecTool,
    stop: StopTool,
}

impl AsyncToolPair {
    /// The pair over one shared engine.
    #[must_use]
    pub fn new(jobs: Arc<AsyncJobs>) -> Self {
        Self {
            start: AsyncExecTool::new(Arc::clone(&jobs)),
            stop: StopTool::new(jobs),
        }
    }
}

impl ToolProvider for AsyncToolPair {
    fn schemas(&self) -> Vec<ToolSchema> {
        let mut schemas = self.start.schemas();
        schemas.extend(self.stop.schemas());
        schemas
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        match call.name.as_str() {
            "start" => self.start.invoke(call),
            "stop" => self.stop.invoke(call),
            other => {
                let other = other.to_string();
                Box::pin(async move {
                    Err(ToolError::InvalidCall(format!(
                        "unknown async tool `{other}`"
                    )))
                })
            }
        }
    }
}
