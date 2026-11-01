//! The async-job completion bridge (card C37, ADR-008): the kian-cli
//! assembly half of the async `start()`/`stop()` pattern.
//!
//! When a job settles, the [`kian_exec::AsyncJobs`] observer hands the
//! settled row to [`CompletionBridge`] (the [`AsyncResultSink`]). The
//! bridge then does exactly one thing, in invariant-1 order:
//!
//! 1. **admit** the result to the originating session's inbox as a
//!    provenance-wrapped `{"kian":{"type":"async_result",…}}` content
//!    envelope (the same content-embedding discipline as channel
//!    provenance and the C21 endpoint-health payload — the frozen C4
//!    payload contract is untouched, and no new event kind exists);
//! 2. **wake** the session, strictly after the row is durable.
//!
//! The completion is promoted at the next safe boundary as a normal
//! message — the loop is untouched. The admit idempotency key is
//! `async-result:{job_id}`, so the startup redelivery sweep (and any
//! replayed `start`) dedupes at the inbox row.
//!
//! Honest no-op: if the target session **projects terminal** (or no
//! longer exists), the completion is dropped **with an audit line**
//! and a counter — never a silent swallow, never a zombie row claim.
//! The job row itself is already settled and immutable, so nothing
//! lingers in any wake registry (startup-rebuild idempotence).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::state::SessionState;
use kian_exec::{AsyncJob, AsyncResultSink, SessionForCall};
use kian_store::EventStore;
use kian_superagent::InboxWriter;

/// The provenance-wrapped completion envelope. `result` is the job
/// row's own durable value (exit/stdout/stderr for `done`, the
/// cancellation/timeout detail otherwise); `text` is the
/// model-readable summary.
#[must_use]
pub fn async_result_payload(job: &AsyncJob) -> serde_json::Value {
    let status = job.status.as_str();
    let result = job.result.clone().unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "kian": {
            "type": "async_result",
            "job_id": job.job_id.to_string(),
            "status": status,
            "result": result,
            "tool_call_ref": job.tool_call_ref,
        },
        "text": format!("async job {} finished with status `{status}`", job.job_id),
    })
}

/// The admit idempotency key for one job's completion. One job, one
/// completion input, ever — redelivery dedupes here.
#[must_use]
pub fn async_result_key(job: &AsyncJob) -> String {
    format!("async-result:{}", job.job_id)
}

/// Bridge outcome counters (the `/metrics` fragment + the audit
/// trail's quantitative half). Plain atomics, like the subscription
/// metrics beside them.
#[derive(Debug, Default)]
pub struct AsyncBridgeMetrics {
    admitted: AtomicU64,
    deduplicated: AtomicU64,
    dropped_terminal: AtomicU64,
    dropped_missing: AtomicU64,
    failures: AtomicU64,
}

impl AsyncBridgeMetrics {
    #[must_use]
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "admitted": self.admitted.load(Ordering::Relaxed),
            "deduplicated": self.deduplicated.load(Ordering::Relaxed),
            "dropped_terminal": self.dropped_terminal.load(Ordering::Relaxed),
            "dropped_missing": self.dropped_missing.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
        })
    }

    #[must_use]
    pub fn dropped_terminal(&self) -> u64 {
        self.dropped_terminal.load(Ordering::Relaxed)
    }
}

/// The [`AsyncResultSink`] over the engine-backed inbox port.
pub struct CompletionBridge {
    inbox: Arc<dyn InboxWriter>,
    store: EventStore,
    metrics: Arc<AsyncBridgeMetrics>,
}

/// The session's projected state, derived from its event stream
/// (constraint 1: session state is read only from the event
/// projection — there is no `sessions` status column to consult).
/// `Ok(None)` = no stream, i.e. no such session.
async fn projected_state(
    store: &EventStore,
    session: SessionId,
) -> Result<Option<SessionState>, String> {
    let events = store
        .rebuild(session)
        .await
        .map_err(|e| format!("async bridge: session stream read: {e}"))?;
    if events.is_empty() {
        return Ok(None);
    }
    let state = kian_core::state::project(&events)
        .map_err(|e| format!("async bridge: session projection: {e}"))?;
    Ok(Some(state))
}

impl CompletionBridge {
    #[must_use]
    pub fn new(
        inbox: Arc<dyn InboxWriter>,
        store: EventStore,
        metrics: Arc<AsyncBridgeMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inbox,
            store,
            metrics,
        })
    }
}

impl AsyncResultSink for CompletionBridge {
    fn deliver(&self, job: AsyncJob) -> BoxFuture<'static, Result<(), String>> {
        let this = Arc::new(Self {
            inbox: Arc::clone(&self.inbox),
            store: self.store.clone(),
            metrics: Arc::clone(&self.metrics),
        });
        Box::pin(async move {
            let session = job.session;
            match projected_state(&this.store, session).await {
                Ok(Some(state)) if state.is_terminal() => {
                    // Honest no-op: the completion has nowhere to go.
                    // The audit line + counter are the evidence; the
                    // settled job row is immutable, so nothing lingers.
                    this.metrics
                        .dropped_terminal
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        session = %session.0,
                        job_id = %job.job_id,
                        status = job.status.as_str(),
                        projected = ?state,
                        "async bridge: completion dropped — target session is terminal"
                    );
                    return Ok(());
                }
                Ok(None) => {
                    this.metrics.dropped_missing.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        session = %session.0,
                        job_id = %job.job_id,
                        status = job.status.as_str(),
                        "async bridge: completion dropped — target session no longer exists"
                    );
                    return Ok(());
                }
                Ok(Some(_)) => {}
                Err(e) => {
                    this.metrics.failures.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
            let payload = async_result_payload(&job);
            let key = async_result_key(&job);
            match this.inbox.admit(session, payload, Some(key)).await {
                Ok(receipt) => {
                    // Invariant 1: the row is durable when admit
                    // resolves; the wake is strictly after.
                    this.inbox.wake(session);
                    if receipt.deduplicated {
                        this.metrics.deduplicated.fetch_add(1, Ordering::Relaxed);
                    } else {
                        this.metrics.admitted.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(
                            session = %session.0,
                            job_id = %job.job_id,
                            status = job.status.as_str(),
                            "async bridge: completion admitted and woken"
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    this.metrics.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        session = %session.0,
                        job_id = %job.job_id,
                        error = %e,
                        "async bridge: completion admit failed (wake skipped)"
                    );
                    Err(format!("async completion admit: {e}"))
                }
            }
        })
    }
}

/// The tool-call → session resolver: the invoking session is derived
/// from the committed `tool/call` event (persist-what-you-send, and
/// the loop persists the call before invoking it), never from
/// in-memory routing state (constraint 1).
#[must_use]
pub fn session_for_call(store: EventStore) -> SessionForCall {
    Arc::new(move |call_id: &str| {
        let store = store.clone();
        let call_id = call_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String,)> = match sqlx::query_as(
                "SELECT DISTINCT session FROM events \
                 WHERE kind = 'tool/call' \
                   AND json_extract(envelope, '$.bytes.content.id') = ?",
            )
            .bind(&call_id)
            .fetch_all(store.pool())
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, call_id = %call_id, "async bridge: invoking-session lookup failed");
                    return None;
                }
            };
            rows.into_iter()
                .find_map(|(session,)| uuid::Uuid::parse_str(&session).ok().map(SessionId))
        }) as BoxFuture<'static, Option<SessionId>>
    })
}

#[cfg(test)]
mod tests {
    use super::{async_result_key, async_result_payload};
    use kian_exec::{AsyncJob, AsyncJobStatus};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn job(status: AsyncJobStatus) -> AsyncJob {
        AsyncJob {
            job_id: uuid::Uuid::nil(),
            session: kian_core::model::SessionId(uuid::Uuid::nil()),
            endpoint_id: "ep1".into(),
            cmd: kian_core::seams::ExecCommand {
                argv: vec!["echo".into()],
                env: Default::default(),
                cwd: None,
                timeout_ms: None,
            },
            status,
            tool_call_ref: Some("call-1".into()),
            result: Some(serde_json::json!({"exit_code": 0})),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            updated_at: 0,
        }
    }

    /// The completion envelope is provenance-wrapped: the kian marker
    /// carries type/job_id/status/result; the frozen payload contract
    /// is untouched (a normal message payload).
    #[test]
    fn completion_envelope_shape_is_the_provenance_contract() {
        let payload = async_result_payload(&job(AsyncJobStatus::Done));
        assert_eq!(payload["kian"]["type"], "async_result");
        assert_eq!(
            payload["kian"]["job_id"],
            uuid::Uuid::nil().to_string().as_str()
        );
        assert_eq!(payload["kian"]["status"], "done");
        assert_eq!(payload["kian"]["result"]["exit_code"], 0);
        assert_eq!(payload["kian"]["tool_call_ref"], "call-1");
        assert!(payload["text"].as_str().unwrap().contains("done"));
        assert_eq!(
            async_result_key(&job(AsyncJobStatus::Cancelled)),
            format!("async-result:{}", uuid::Uuid::nil())
        );
    }
}
