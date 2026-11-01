//! Async `start()`/`stop()` tool acceptance tests (card C37, ADR-008).
//!
//! Every test pairs the real durable queue + `RemoteExecBackend` +
//! `AsyncJobs` engine against the real endpoint-side crate over an
//! ephemeral-port TCP listener and a real sqlite file — no in-process
//! shortcuts, no credentials (same harness discipline as
//! `remote_tool.rs`). The completion bridge is a recording sink: it
//! captures exactly what the kian-cli assembly would admit, proving
//! the *engine* half of the pattern; the assembled-server half is
//! proven by `kian-cli/tests/async_tools_e2e.rs`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult, ToolCall, ToolProvider};
use kian_endpoint::Endpoint;
use kian_exec::async_jobs::AsyncJobStatus;
use kian_exec::store::ExecStore;
use kian_exec::{
    AsyncJob, AsyncJobs, AsyncResultSink, AsyncToolPair, BackoffConfig, ConnectionConfig,
    ConnectionManager, RemoteExecBackend, RemoteExecConfig,
};
use tokio::net::TcpListener;
use uuid::Uuid;

fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: written once, before any socket is opened.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-async-tool-{}.sqlite", Uuid::new_v4()))
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        initial: Duration::from_millis(20),
        max: Duration::from_millis(200),
        multiplier: 2.0,
    }
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}

/// The session every job originates from (the resolver stub — the
/// assembly resolves it from the committed tool/call event).
fn fixed_session() -> SessionId {
    SessionId(Uuid::from_u128(0xa51c_0000_0000_0000_0000_0000_0000_0001))
}

fn session_for_call() -> kian_exec::SessionForCall {
    Arc::new(|_call_id: &str| {
        Box::pin(async move { Some(fixed_session()) }) as BoxFuture<'static, Option<SessionId>>
    })
}

/// A recording completion sink: captures every settled row offered
/// for delivery (what the assembly bridge would admit + wake).
#[derive(Default)]
struct RecordingSink {
    delivered: Mutex<Vec<AsyncJob>>,
}

impl AsyncResultSink for RecordingSink {
    fn deliver(&self, job: AsyncJob) -> BoxFuture<'static, Result<(), String>> {
        self.delivered.lock().unwrap().push(job);
        Box::pin(async { Ok(()) })
    }
}

struct TestRig {
    manager: Arc<ConnectionManager>,
    backend: Arc<RemoteExecBackend>,
    pair: Arc<AsyncToolPair>,
    store: ExecStore,
    sink: Arc<RecordingSink>,
    db: PathBuf,
}

/// Boots endpoint + queue + async engine over one sqlite file.
async fn rig(db: &PathBuf) -> TestRig {
    let store = ExecStore::open(db).await.unwrap();
    let endpoint = Arc::new(Endpoint::new(
        "dev-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(endpoint.serve(listener));
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = Arc::new(
        RemoteExecBackend::new(
            store.clone(),
            vec![("dev-box".into(), manager.clone())],
            "dev-box",
            RemoteExecConfig {
                queue_timeout: Duration::from_secs(15),
                poll_interval: Duration::from_millis(40),
            },
        )
        .unwrap(),
    );
    let sink = Arc::new(RecordingSink::default());
    let jobs = AsyncJobs::new(backend.clone(), "dev-box", sink.clone(), session_for_call());
    let pair = Arc::new(AsyncToolPair::new(jobs));
    TestRig {
        manager,
        backend,
        pair,
        store,
        sink,
        db: db.clone(),
    }
}

async fn shutdown(rig: TestRig) {
    let db = rig.db.clone();
    shutdown_keep_db(rig).await;
    let _ = std::fs::remove_file(&db);
}

/// Tears down one "life" of the process but keeps the sqlite file —
/// the simulated restart reopens it (durability across the drop).
async fn shutdown_keep_db(rig: TestRig) {
    rig.backend.shutdown().await;
    rig.manager.shutdown().await;
    rig.store.close().await;
}

/// Poll until `pred` over the job row holds (bounded).
async fn wait_for_job(
    store: &ExecStore,
    job_id: Uuid,
    pred: impl Fn(&AsyncJob) -> bool,
) -> AsyncJob {
    for _ in 0..1_000 {
        if let Some(row) = store.get_async_job(job_id).await.unwrap()
            && pred(&row)
        {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for job {job_id} to settle");
}

/// The pair's schemas are exactly [start, stop], in that stable order
/// (tool schemas are prompt-prefix content — constraint 3's gate).
#[tokio::test]
async fn pair_schemas_are_start_then_stop_in_stable_order() {
    let db = temp_db();
    let rig = rig(&db).await;
    let names: Vec<String> = rig.pair.schemas().iter().map(|s| s.name.clone()).collect();
    assert_eq!(names, vec!["start".to_string(), "stop".to_string()]);
    shutdown(rig).await;
}

/// The core promise: `start` returns **immediately** (bounded well
/// under the command's runtime) while the command runs for seconds,
/// and the job row is durable (admit-before-execute) the whole time.
#[tokio::test]
async fn start_returns_immediately_while_the_command_runs_for_seconds() {
    no_proxy();
    let db = temp_db();
    let rig = rig(&db).await;

    let began = std::time::Instant::now();
    let result = rig
        .pair
        .invoke(call(
            "call-start-slow",
            "start",
            serde_json::json!({"argv": ["sleep", "3"]}),
        ))
        .await
        .unwrap();
    let elapsed = began.elapsed();
    assert!(
        elapsed < Duration::from_millis(1_500),
        "start must return immediately, took {elapsed:?}"
    );
    assert!(!result.is_error);
    assert_eq!(result.output["status"], "started");
    let job_id = Uuid::parse_str(result.output["job_id"].as_str().unwrap()).unwrap();
    assert_eq!(
        job_id,
        kian_exec::tool::command_id_for_tool_call("call-start-slow"),
        "the job id is the command id derived from the tool-call id"
    );

    // While the command runs: the row is durable and not done.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let row = rig.store.get_async_job(job_id).await.unwrap().unwrap();
    assert!(
        row.status == AsyncJobStatus::Pending || row.status == AsyncJobStatus::Inflight,
        "row must be durable and live while the command runs, got {:?}",
        row.status
    );

    // It settles done with the real endpoint outcome, delivered once.
    let row = wait_for_job(&rig.store, job_id, |r| r.status.is_terminal()).await;
    assert_eq!(row.status, AsyncJobStatus::Done);
    assert_eq!(row.result.as_ref().unwrap()["exit_code"], 0);
    let delivered = rig.sink.delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1, "exactly one completion delivery");
    assert_eq!(delivered[0].job_id, job_id);
    assert_eq!(delivered[0].status, AsyncJobStatus::Done);
    assert_eq!(delivered[0].session, fixed_session());

    shutdown(rig).await;
}

/// Honest errors: no session resolution → no row, an honest tool
/// error; invalid argv → rejected before any row exists.
#[tokio::test]
async fn start_without_a_session_or_with_bad_argv_fails_honestly() {
    no_proxy();
    let db = temp_db();
    let rig = rig(&db).await;

    let err = rig
        .pair
        .invoke(call("call-bad", "start", serde_json::json!({"argv": []})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("argv"));

    let err = rig
        .pair
        .invoke(call(
            "call-unknown-stop",
            "stop",
            serde_json::json!({"job_id": Uuid::new_v4().to_string()}),
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown job_id"));

    let err = rig
        .pair
        .invoke(call("call-x", "nope", serde_json::json!({})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown async tool"));

    shutdown(rig).await;
}

/// Job-id dedupe across a simulated recovery: the same tool-call id
/// replayed after the engine (and queue drivers) were dropped and the
/// pool reopened collapses onto the same rows — one dispatch ever,
/// and the completion still lands.
#[tokio::test]
async fn job_id_dedupes_across_a_simulated_recovery() {
    no_proxy();
    let db = temp_db();

    // Life 1: run the job to completion, then drop everything.
    let rig1 = rig(&db).await;
    let first = rig1
        .pair
        .invoke(call(
            "call-dedupe",
            "start",
            serde_json::json!({"argv": ["echo", "once"]}),
        ))
        .await
        .unwrap();
    let job_id = Uuid::parse_str(first.output["job_id"].as_str().unwrap()).unwrap();
    wait_for_job(&rig1.store, job_id, |r| r.status.is_terminal()).await;
    assert_eq!(rig1.sink.delivered.lock().unwrap().len(), 1);
    shutdown_keep_db(rig1).await;

    // Life 2: reopen the pool, rebuild drivers, replay the same call.
    let rig2 = rig(&db).await;
    let second = rig2
        .pair
        .invoke(call(
            "call-dedupe",
            "start",
            serde_json::json!({"argv": ["echo", "once"]}),
        ))
        .await
        .unwrap();
    assert!(!second.is_error, "the ack is honest on the dedupe path");
    assert_eq!(second.output["status"], "started");
    assert_eq!(
        second.output["job_id"].as_str().unwrap(),
        job_id.to_string(),
        "same job id — one row, ever"
    );
    // Exactly one async row exists.
    let rows = rig2
        .store
        .async_jobs_with_status(&[
            AsyncJobStatus::Pending,
            AsyncJobStatus::Inflight,
            AsyncJobStatus::Done,
            AsyncJobStatus::Cancelled,
            AsyncJobStatus::Abandoned,
        ])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].job_id, job_id);
    assert_eq!(rows[0].status, AsyncJobStatus::Done, "no re-execution");
    // The replayed observer redelivers the settled row (idempotent at
    // the bridge) — the row's own result, never a second dispatch.
    for _ in 0..200 {
        if !rig2.sink.delivered.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivered = rig2.sink.delivered.lock().unwrap().clone();
    assert!(!delivered.is_empty(), "settled row is re-offered");
    assert!(delivered.iter().all(|j| j.job_id == job_id));
    shutdown(rig2).await;
}

/// `stop` mid-run: the job settles `cancelled`, the completion input
/// is still delivered (labeled `cancelled` — cancellation is an
/// event, not an erasure), and the stop tool's own result is honest.
#[tokio::test]
async fn stop_mid_run_cancels_and_completion_lands_labelled_cancelled() {
    no_proxy();
    let db = temp_db();
    let rig = rig(&db).await;

    let started = rig
        .pair
        .invoke(call(
            "call-stop-me",
            "start",
            serde_json::json!({"argv": ["sleep", "10"]}),
        ))
        .await
        .unwrap();
    let job_id = Uuid::parse_str(started.output["job_id"].as_str().unwrap()).unwrap();
    // Let the command actually reach the endpoint (inflight).
    tokio::time::sleep(Duration::from_millis(400)).await;

    let stopped = rig
        .pair
        .invoke(call(
            "call-stop-1",
            "stop",
            serde_json::json!({"job_id": job_id.to_string()}),
        ))
        .await
        .unwrap();
    assert!(!stopped.is_error);
    assert_eq!(stopped.output["status"], "cancelled");
    assert_eq!(stopped.output["job_id"], job_id.to_string());

    let row = wait_for_job(&rig.store, job_id, |r| r.status.is_terminal()).await;
    assert_eq!(row.status, AsyncJobStatus::Cancelled);

    // The completion still lands, labeled cancelled.
    for _ in 0..300 {
        if !rig.sink.delivered.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivered = rig.sink.delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].status, AsyncJobStatus::Cancelled);
    assert_eq!(delivered[0].job_id, job_id);

    // Stopping again is honest: already settled, nothing faked.
    let again = rig
        .pair
        .invoke(call(
            "call-stop-2",
            "stop",
            serde_json::json!({"job_id": job_id.to_string()}),
        ))
        .await
        .unwrap();
    assert_eq!(again.output["status"], "cancelled");
    assert!(again.output["note"].as_str().unwrap().contains("settled"));

    shutdown(rig).await;
}

/// Server-restart semantics: `pending` and `inflight` rows are
/// re-driven by a fresh engine over the reopened pool (drop drivers,
/// reopen) — the completion still lands.
#[tokio::test]
async fn restart_redrives_pending_and_inflight_jobs() {
    no_proxy();
    let db = temp_db();

    // Life 1: two rows land in the table without a completing engine —
    // one parked `pending` (endpoint offline, manager never started)
    // and one `inflight` (crash between dispatch and settle).
    let store = ExecStore::open(&db).await.unwrap();
    let session = fixed_session();
    let cmd = ExecCommand {
        argv: vec!["echo".into(), "revived".into()],
        env: Default::default(),
        cwd: None,
        timeout_ms: None,
    };
    let pending = Uuid::new_v4();
    store
        .insert_async_job(pending, session, "dev-box", &cmd, Some("call-p"))
        .await
        .unwrap();
    let inflight = Uuid::new_v4();
    store
        .insert_async_job(inflight, session, "dev-box", &cmd, Some("call-i"))
        .await
        .unwrap();
    store.mark_async_inflight(inflight).await.unwrap();
    store.close().await;

    // Life 2 (the restart): fresh drivers over the reopened pool, then
    // the startup sweep.
    let rig = rig(&db).await;
    let jobs = AsyncJobs::new(
        rig.backend.clone(),
        "dev-box",
        rig.sink.clone(),
        session_for_call(),
    );
    jobs.redeliver_settled().await;
    jobs.redrive_pending().await;

    for (job, label) in [(pending, "pending"), (inflight, "inflight")] {
        let row = wait_for_job(&rig.store, job, |r| r.status.is_terminal()).await;
        assert_eq!(
            row.status,
            AsyncJobStatus::Done,
            "the {label} job must be re-driven to completion"
        );
        assert_eq!(
            row.result.as_ref().unwrap()["stdout"]
                .as_str()
                .unwrap()
                .trim(),
            "revived"
        );
    }
    let delivered = rig.sink.delivered.lock().unwrap().len();
    assert_eq!(delivered, 2, "both completions delivered");

    // Idempotence: the sweep again re-offers the settled rows but
    // spawns no second dispatch (rows stay done, no re-execution).
    jobs.redrive_pending().await;
    jobs.redeliver_settled().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rows = rig
        .store
        .async_jobs_with_status(&[AsyncJobStatus::Done])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    shutdown(rig).await;
}

/// A queue timeout (the honest bound) settles the job `abandoned` and
/// still delivers the completion, labeled as such.
#[tokio::test]
async fn queue_timeout_settles_abandoned_and_delivers_honestly() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();
    // No endpoint listener at all: the command parks pending until
    // the short queue timeout abandons it.
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new("127.0.0.1:1".parse().unwrap()).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = Arc::new(
        RemoteExecBackend::new(
            store.clone(),
            vec![("dev-box".into(), manager.clone())],
            "dev-box",
            RemoteExecConfig {
                queue_timeout: Duration::from_millis(400),
                poll_interval: Duration::from_millis(40),
            },
        )
        .unwrap(),
    );
    let sink = Arc::new(RecordingSink::default());
    let jobs = AsyncJobs::new(backend.clone(), "dev-box", sink.clone(), session_for_call());
    let pair = Arc::new(AsyncToolPair::new(jobs));

    let started = pair
        .invoke(call(
            "call-timeout",
            "start",
            serde_json::json!({"argv": ["echo", "never"]}),
        ))
        .await
        .unwrap();
    let job_id = Uuid::parse_str(started.output["job_id"].as_str().unwrap()).unwrap();
    let row = wait_for_job(&store, job_id, |r| r.status.is_terminal()).await;
    assert_eq!(row.status, AsyncJobStatus::Abandoned);

    for _ in 0..300 {
        if !sink.delivered.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivered = sink.delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].status, AsyncJobStatus::Abandoned);

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Compile-time anchor: the trait object shape assembly relies on.
#[allow(dead_code)]
fn sink_is_object_safe(_: &dyn AsyncResultSink) {}

// Silence unused warnings for helpers only some tests use.
#[allow(dead_code)]
fn _unused_backend_traits() {
    fn assert_exec_backend<T: ExecBackend>() {}
    assert_exec_backend::<kian_exec::RemoteExecBackend>();
    let _ = ExecResult {
        exit_code: Some(0),
        stdout: String::new(),
        stderr: String::new(),
        timed_out: false,
    };
    let _: Option<ExecError> = None;
}
