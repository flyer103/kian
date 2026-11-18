//! (ADR-020 / R11-03, D11) The async-tools carry decision at the
//! store level: **one `AsyncJobs` engine per configured endpoint**,
//! each bound to its own endpoint id — a `start` invoked through
//! side B's pair writes an `async_jobs` row with
//! `endpoint_id = "B"`, and the single-endpoint shape keeps today's
//! default (the only endpoint) exactly.
//!
//! These are L1 rows of the spec §7 async matrix: no network, no
//! daemon — the managers are constructed but never started, so no
//! connection is dialed; only the durable row admission is asserted.
//! (The full per-side assembly and the two-daemon legs are R11-04.)

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::temp_dir;
use kian_core::model::SessionId;
use kian_core::seams::{ToolCall, ToolProvider as _};
use kian_exec::{
    AsyncJobs, AsyncToolPair, ConnectionConfig, ConnectionManager, ExecStore, RemoteExecBackend,
    RemoteExecConfig, tool::command_id_for_tool_call,
};
use uuid::Uuid;

/// A no-op completion sink: the observer's delivery is not under test
/// here (the async_bridge suite owns it); the row admission is.
struct NoopSink;

impl kian_exec::AsyncResultSink for NoopSink {
    fn deliver(
        &self,
        _job: kian_exec::AsyncJob,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// One backend over a fresh sqlite exec store with `ids` routes
/// (managers constructed, never started — nothing dials).
async fn backend_with(ids: &[&str]) -> (ExecStore, Arc<RemoteExecBackend>) {
    let dir = temp_dir("worker-binding");
    let store = ExecStore::open(dir.join("exec.sqlite"))
        .await
        .expect("exec store opens");
    let routes = ids
        .iter()
        .map(|id| {
            let addr: std::net::SocketAddr = "127.0.0.1:1".parse().expect("addr");
            (
                (*id).to_string(),
                Arc::new(ConnectionManager::new(ConnectionConfig::new(addr))),
            )
        })
        .collect();
    let backend = RemoteExecBackend::with_row_leases_redacting(
        store.clone(),
        routes,
        ids.first().copied().expect("at least one id"),
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(200),
            poll_interval: Duration::from_millis(50),
            max_redrives: 1,
        },
        "test-holder",
        Duration::from_millis(200),
        kian_core::secrets::SecretRedactor::default(),
    )
    .expect("backend builds");
    (store, Arc::new(backend))
}

fn start_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "start".into(),
        arguments: serde_json::json!({"argv": ["/bin/true"]}),
    }
}

fn session_for_call() -> kian_exec::SessionForCall {
    Arc::new(|_call_id: &str| Box::pin(async { Some(SessionId(Uuid::new_v4())) }))
}

/// Two endpoints ⇒ two distinct engines/pairs; a `start` on side B
/// writes an `async_jobs` row with `endpoint_id = "B"` (the persisted
/// column carries the binding — the carry, not refuse, decision).
#[tokio::test]
async fn per_side_async_pairs_bind_their_own_endpoint_id() {
    let (store, backend) = backend_with(&["A", "B"]).await;
    let jobs_a = AsyncJobs::new(
        Arc::clone(&backend),
        "A",
        Arc::new(NoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        session_for_call(),
    );
    let jobs_b = AsyncJobs::new(
        Arc::clone(&backend),
        "B",
        Arc::new(NoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        session_for_call(),
    );
    let pair_b = AsyncToolPair::new(Arc::clone(&jobs_b));

    // Two distinct engines (pointer-inequal) share one backend.
    assert!(!Arc::ptr_eq(&jobs_a, &jobs_b));

    // start on side B: the row lands with endpoint_id = "B".
    let result = pair_b.invoke(start_call("call-on-b")).await.unwrap();
    let job_id = Uuid::parse_str(result.output["job_id"].as_str().expect("job_id")).expect("uuid");
    assert_eq!(
        job_id,
        command_id_for_tool_call("call-on-b"),
        "the job id derives from the tool-call id (ADR-008 correlation)"
    );
    let row = store
        .get_async_job(job_id)
        .await
        .expect("row read")
        .expect("row exists");
    assert_eq!(row.endpoint_id, "B", "side B's pair writes endpoint_id B");
}

/// One endpoint (or a legacy unbound session): the default engine is
/// the only endpoint's — the pre-R11 behavior exactly.
#[tokio::test]
async fn single_endpoint_default_pair_keeps_the_only_endpoint() {
    let (store, backend) = backend_with(&["only"]).await;
    let jobs = AsyncJobs::new(
        Arc::clone(&backend),
        "only",
        Arc::new(NoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        session_for_call(),
    );
    let pair = AsyncToolPair::new(Arc::clone(&jobs));
    pair.invoke(start_call("call-default")).await.unwrap();
    let row = store
        .get_async_job(command_id_for_tool_call("call-default"))
        .await
        .expect("row read")
        .expect("row exists");
    assert_eq!(row.endpoint_id, "only");
}
