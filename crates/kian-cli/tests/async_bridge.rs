//! Completion-bridge integration tests (card C37, ADR-008), over the
//! **real** engine + event store + an engine-backed `InboxWriter`
//! port (the test-local, driver-generic twin of the assembly's
//! `EngineInbox`) — no in-process shortcuts for the admit path. The
//! exec side is replaced by hand-settled `AsyncJob` rows (the engine
//! half is proven by `kian-exec/tests/async_tools.rs`; the full
//! assembled loop by `async_tools_e2e.rs`).
//!
//! Proven here, against durable rows:
//! - a settled job's completion lands as an **inbox input row**
//!   (provenance-wrapped `async_result` envelope, idempotency key
//!   `async-result:{job_id}`) — invariant 1's durable-before-wake
//!   ordering is the engine's own `EngineInbox` behavior;
//! - redelivery of the same settled row dedupes at the inbox row
//!   (exactly one input, ever);
//! - a completion to a **terminal** session is dropped with the audit
//!   counter (honest no-op): no input row, no wake registered;
//! - a completion to a nonexistent session is dropped the same way.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_cli::async_bridge::{AsyncBridgeMetrics, CompletionBridge, async_result_key};
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_exec::{AsyncJob, AsyncJobStatus, AsyncResultSink};
use kian_orchestrator::{DrainCtx, Engine, EngineConfig};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::{AdmitReceipt, InboxWriteError, InboxWriter};
use serde_json::json;
use uuid::Uuid;

/// A drain that does nothing (the completion path under test is the
/// admit, not the loop).
struct NoopDriver;

impl LoopDriver for NoopDriver {
    type Ctx = DrainCtx;

    fn drive(&self, _ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        futures::stream::empty().boxed()
    }
}

/// The `InboxWriter` port over the real engine (the test-local,
/// driver-generic twin of the assembly's `EngineInbox`: durable admit
/// resolves before the wake, exactly the invariant-1 ordering).
struct TestInbox<D: LoopDriver<Ctx = DrainCtx> + Send + Sync + 'static>(Arc<Engine<D>>);

impl<D: LoopDriver<Ctx = DrainCtx> + Send + Sync + 'static> InboxWriter for TestInbox<D> {
    fn admit(
        &self,
        session: SessionId,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = Arc::clone(&self.0);
        Box::pin(async move {
            let outcome = engine
                .admit(session, payload, idempotency_key)
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> kian_orchestrator::WakeOutcome {
        self.0.wake(session)
    }
}

struct Rig {
    engine: Arc<Engine<NoopDriver>>,
    store: EventStore,
    bridge: Arc<CompletionBridge>,
    metrics: Arc<AsyncBridgeMetrics>,
}

async fn rig() -> Rig {
    let db = std::env::temp_dir().join(format!("kian-async-bridge-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&db).await.expect("open store");
    let engine = Engine::new(
        store.clone(),
        Arc::new(NoopDriver),
        EngineConfig {
            holder: "async-bridge-test".into(),
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 60_000,
        },
    );
    let inbox = Arc::new(TestInbox(engine.clone()));
    let metrics = Arc::new(AsyncBridgeMetrics::default());
    let bridge = CompletionBridge::new(
        inbox as Arc<dyn InboxWriter>,
        store.clone(),
        Arc::clone(&metrics),
    );
    Rig {
        engine,
        store,
        bridge,
        metrics,
    }
}

fn settled_job(session: SessionId, status: AsyncJobStatus) -> AsyncJob {
    AsyncJob {
        job_id: Uuid::new_v4(),
        session,
        endpoint_id: "ep1".into(),
        cmd: kian_core::seams::ExecCommand {
            argv: vec!["echo".into(), "hi".into()],
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        },
        status,
        tool_call_ref: Some("call-1".into()),
        result: Some(json!({"exit_code": 0, "stdout": "hi\n"})),
        created_at: 1,
        updated_at: 2,
    }
}

/// The durable inbox rows for one session: (idempotency_key, payload).
async fn inbox_rows(store: &EventStore, session: SessionId) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT idempotency_key, payload FROM inputs WHERE session = ? ORDER BY admitted_at",
    )
    .bind(session.0.to_string())
    .fetch_all(store.pool())
    .await
    .expect("read inbox")
}

#[tokio::test]
async fn completion_lands_as_a_durable_input_row_and_redelivery_dedupes() {
    let rig = rig().await;
    let session = rig.engine.create_session().await.expect("create session");
    let job = settled_job(session, AsyncJobStatus::Done);

    rig.bridge.deliver(job.clone()).await.expect("deliver");

    // Invariant 1: the row is durable, with the provenance-wrapped
    // envelope and the job-derived idempotency key.
    let rows = inbox_rows(&rig.store, session).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, async_result_key(&job));
    let payload: serde_json::Value = serde_json::from_str(&rows[0].1).expect("payload json");
    assert_eq!(payload["kian"]["type"], "async_result");
    assert_eq!(payload["kian"]["status"], "done");
    assert_eq!(payload["kian"]["job_id"], job.job_id.to_string().as_str());
    assert_eq!(payload["kian"]["result"]["stdout"], "hi\n");

    // Redelivery (the startup sweep's crash-window case) dedupes at
    // the inbox row — still exactly one input.
    rig.bridge.deliver(job.clone()).await.expect("redeliver");
    let rows = inbox_rows(&rig.store, session).await;
    assert_eq!(rows.len(), 1, "one completion input per job, ever");
    let snapshot = rig.metrics.snapshot();
    assert_eq!(snapshot["admitted"], 1);
    assert_eq!(snapshot["deduplicated"], 1);
}

#[tokio::test]
async fn completion_to_a_terminal_session_drops_with_audit_and_no_row() {
    let rig = rig().await;
    let session = rig.engine.create_session().await.expect("create session");

    // Drive the session terminal through its stream (the projection
    // is the only state read). The legal path is created → claimed →
    // ended (end writes are drain activity); these are plain fixture
    // appends — production end writes are the engine's atomic append.
    for kind in [
        EventKind::DrainClaimed,
        EventKind::SessionEnded {
            reason: EndReason::Completed,
        },
    ] {
        let head = rig.store.head(session).await.expect("head");
        let event = Event {
            seq: head.next_seq,
            session,
            epoch: Epoch::GENESIS,
            ts: 1,
            kind,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        rig.store
            .append(
                session,
                event,
                AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: head.next_seq,
                },
            )
            .await
            .expect("append fixture event");
    }
    let state = rig.engine.projection(session).await.expect("projection");
    assert!(state.is_terminal(), "fixture session is terminal");

    let job = settled_job(session, AsyncJobStatus::Done);
    rig.bridge.deliver(job).await.expect("honest no-op");

    // No input row, no wake registered, and the audit counter moved.
    let rows = inbox_rows(&rig.store, session).await;
    assert!(rows.is_empty(), "terminal session gets no completion row");
    assert_eq!(rig.metrics.dropped_terminal(), 1);
    assert!(!rig.engine.pending_wake_contains(&session));
    assert!(!rig.engine.is_draining(&session));
}

#[tokio::test]
async fn completion_to_a_missing_session_drops_without_a_zombie_row() {
    let rig = rig().await;
    let session = SessionId::new();
    let job = settled_job(session, AsyncJobStatus::Cancelled);
    rig.bridge.deliver(job).await.expect("honest no-op");
    let rows = inbox_rows(&rig.store, session).await;
    assert!(rows.is_empty());
    assert_eq!(rig.metrics.snapshot()["dropped_missing"], 1);
    assert!(!rig.engine.pending_wake_contains(&session));
}

/// A cancelled job's completion is admitted and **labeled cancelled**
/// (cancellation is an event, not an erasure).
#[tokio::test]
async fn cancelled_completion_lands_labelled_cancelled() {
    let rig = rig().await;
    let session = rig.engine.create_session().await.expect("create session");
    let job = settled_job(session, AsyncJobStatus::Cancelled);
    rig.bridge.deliver(job).await.expect("deliver");
    let rows = inbox_rows(&rig.store, session).await;
    assert_eq!(rows.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&rows[0].1).expect("payload json");
    assert_eq!(payload["kian"]["status"], "cancelled");
    // Give the noop drain a beat; nothing about the row changes.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(inbox_rows(&rig.store, session).await.len(), 1);
}
