//! Durable command queue acceptance tests (card C20).
//!
//! Every test pairs the real queue + `RemoteExecBackend` against the real
//! endpoint-side crate over an ephemeral-port TCP listener and a real
//! sqlite file — no in-process shortcuts, no credentials. "Offline" is
//! simulated by binding the listener but not serving on it yet: the server
//! side's dial completes into the kernel backlog and parks in the
//! handshake, deterministically, until `Endpoint::serve` starts accepting.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};
use kian_endpoint::Endpoint;
use kian_exec::proto::CommandId;
use kian_exec::store::{EnqueueOutcome, ExecStore, QueueStatus};
use kian_exec::{
    BackoffConfig, ConnectionConfig, ConnectionManager, RemoteExecBackend, RemoteExecConfig,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

/// Local e2e discipline (round-2 plan §1 rule 6).
fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: no other thread in this test binary reads the
        // environment, and the write happens exactly once before any
        // socket is opened by the calling test.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-queue-{}.sqlite", Uuid::new_v4()))
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        initial: Duration::from_millis(20),
        max: Duration::from_millis(200),
        multiplier: 2.0,
    }
}

fn cmd(argv: &[&str]) -> ExecCommand {
    ExecCommand {
        argv: argv.iter().map(|s| s.to_string()).collect(),
        env: Default::default(),
        cwd: None,
        timeout_ms: None,
    }
}

async fn ephemeral_listener() -> (std::net::SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, listener)
}

/// Poll until `cond` holds; panics with `what` after ~10s.
async fn until(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 10s: {what}");
}

/// Async variant of [`until`] for conditions that must query the store.
async fn until_async<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..1_000 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 10s: {what}");
}

fn queue_config(queue_timeout_ms: u64) -> RemoteExecConfig {
    RemoteExecConfig {
        queue_timeout: Duration::from_millis(queue_timeout_ms),
        poll_interval: Duration::from_millis(40),
    }
}

/// An executor that counts executions and optionally gates them (for the
/// concurrency-cap test).
#[derive(Clone)]
struct CountingExecutor {
    count: Arc<AtomicUsize>,
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    gate: Option<watch::Receiver<bool>>,
}

impl CountingExecutor {
    fn counting() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            running: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            gate: None,
        }
    }

    fn gated(gate: watch::Receiver<bool>) -> Self {
        Self {
            gate: Some(gate),
            ..Self::counting()
        }
    }
}

impl ExecBackend for CountingExecutor {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        let count = self.count.clone();
        let running = self.running.clone();
        let peak = self.peak.clone();
        let gate = self.gate.clone();
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            if let Some(gate) = gate {
                let mut gate = gate;
                let _ = gate.wait_for(|open| *open).await;
            } else if let Some(secs) = cmd.argv.first().and_then(|a| a.parse::<f64>().ok()) {
                // "slow" mode: argv[0] is a number of seconds to sleep.
                tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            }
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: format!("ran-{}", cmd.argv.join("-")),
                stderr: String::new(),
                timed_out: false,
                redactions: Vec::new(),
            })
        })
    }
}

/// **S5 core semantic**: enqueue while the endpoint is offline parks
/// durably (`pending` row in sqlite, registry `offline`); when the endpoint
/// connects later, the caller's future resolves with the *real* result.
#[tokio::test]
async fn offline_enqueue_parks_durable_then_resolves_on_connect() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    // Offline: the listener is bound but nothing serves on it yet.
    let (addr, listener) = ephemeral_listener().await;
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr)
            .with_heartbeat(Duration::from_millis(30))
            .with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
    )
    .unwrap();

    let id: CommandId = Uuid::new_v4();
    let mut future =
        Box::pin(backend.exec_tracked(id, "box", cmd(&["echo", "late"]), Some("call-late".into())));
    // Parked, not failed, not resolved.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut future)
            .await
            .is_err(),
        "offline dispatch must park, not resolve or fail"
    );
    // Durable before any dispatch: the row exists as pending.
    let row = store.get(id).await.unwrap().expect("row durable");
    assert_eq!(row.status, QueueStatus::Pending);
    assert_eq!(row.tool_call_ref.as_deref(), Some("call-late"));
    // The registry projected the endpoint as offline.
    until_async("registry shows offline", || async {
        store
            .endpoints()
            .await
            .map(|rows| {
                rows.iter()
                    .any(|r| r.endpoint_id == "box" && r.state == "offline")
            })
            .unwrap_or(false)
    })
    .await;

    // The endpoint comes online: the same future resolves with the real
    // result, and the row settles done with that result recorded.
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(kian_endpoint::LocalExecutor)));
    tokio::spawn(endpoint.serve(listener));
    let result = future.await.unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.stdout.trim(), "late");

    let row = store.get(id).await.unwrap().expect("row present");
    assert_eq!(row.status, QueueStatus::Done);
    assert_eq!(
        row.outcome.unwrap().to_exec().unwrap().stdout.trim(),
        "late"
    );
    // Registry flipped online and heartbeat freshness is being projected.
    until_async("registry shows online with fresh heartbeat", || async {
        store
            .endpoints()
            .await
            .map(|rows| {
                rows.iter().any(|r| {
                    r.endpoint_id == "box" && r.state == "online" && r.last_heartbeat_ms > 0
                })
            })
            .unwrap_or(false)
    })
    .await;

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// `queue_timeout` expires to an honest `ExecError::Timeout` with the row
/// marked `abandoned` (terminal); a later duplicate command id gets the
/// same honest Timeout — never a fabricated result.
#[tokio::test]
async fn queue_timeout_is_honest_and_marks_abandoned() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    // Endpoint never comes up; the listener is never served.
    let (addr, _listener) = ephemeral_listener().await;
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(250),
    )
    .unwrap();

    let id: CommandId = Uuid::new_v4();
    let error = backend
        .exec_tracked(id, "box", cmd(&["echo", "never"]), None)
        .await
        .unwrap_err();
    assert_eq!(error, ExecError::Timeout);
    let row = store.get(id).await.unwrap().expect("row present");
    assert_eq!(row.status, QueueStatus::Abandoned);
    assert!(row.outcome.is_none());

    // Row-level idempotence: the duplicate gets the same honest answer and
    // no second dispatch can ever happen for this id.
    let again = backend
        .exec_tracked(id, "box", cmd(&["echo", "never"]), None)
        .await
        .unwrap_err();
    assert_eq!(again, ExecError::Timeout);

    backend.shutdown().await;
    // NOTE (C19 limitation, flagged to the orchestrator): a manager parked
    // in the handshake (endpoint bound but never serving) cannot be shut
    // down — the hello-wait has no shutdown branch, so `shutdown().await`
    // would deadlock. Tests that never connect drop the manager instead;
    // the test runtime reclaims its tasks. Connected managers shut down
    // gracefully as normal.
    drop(manager);
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Simulated server restart — connection manager dropped, pool reopened —
/// re-drives the parked `pending` rows once the endpoint (re)connects:
/// the row settles `done` with the real result even though the original
/// caller is gone (at-least-once, command-id correlation).
#[tokio::test]
async fn server_restart_redrives_pending_rows() {
    no_proxy();
    let db = temp_db();

    // Life 1: enqueue while offline, then "crash" (drop everything).
    let (addr, listener) = ephemeral_listener().await;
    {
        let store = ExecStore::open(&db).await.unwrap();
        let manager = Arc::new(ConnectionManager::new(
            ConnectionConfig::new(addr).with_backoff(fast_backoff()),
        ));
        manager.start();
        let backend = RemoteExecBackend::new(
            store.clone(),
            vec![("box".into(), manager.clone())],
            "box",
            queue_config(60_000),
        )
        .unwrap();
        let id: CommandId = Uuid::new_v4();
        let mut future = Box::pin(backend.exec_tracked(id, "box", cmd(&["echo", "reborn"]), None));
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut future)
                .await
                .is_err()
        );
        assert_eq!(
            store.get(id).await.unwrap().unwrap().status,
            QueueStatus::Pending
        );
        backend.shutdown().await;
        // Never-connected manager (C19 handshake-park limitation, see the
        // queue-timeout test note): dropping beats deadlocking on
        // `shutdown().await` here; the runtime reclaims the supervisor.
        drop(manager);
        store.close().await;
        drop(future);
    }

    // Life 2: fresh pool over the same file (the restart), a fresh
    // connection manager, and the endpoint finally serving.
    let executor = CountingExecutor::counting();
    let executed = executor.count.clone();
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(executor)));
    tokio::spawn(endpoint.serve(listener));
    let store = ExecStore::open(&db).await.unwrap();
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
    )
    .unwrap();

    // The pending row from the previous life is re-driven and settles done
    // with the real result — no waiter needed.
    until_async("restarted queue drains the pending row", || async {
        store
            .drivable("box")
            .await
            .map(|rows| rows.is_empty())
            .unwrap_or(false)
    })
    .await;
    let rows: Vec<_> = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM exec_queue")
        .fetch_all(store.pool())
        .await
        .unwrap();
    assert_eq!(rows[0], 1, "exactly one queue row across the restart");
    let row = sqlx::query_scalar::<_, String>(
        "SELECT cmd FROM exec_queue WHERE status = 'done' AND result LIKE '%reborn%'",
    )
    .fetch_optional(store.pool())
    .await
    .unwrap();
    assert!(row.is_some(), "row settled done with the real result");
    assert_eq!(executed.load(Ordering::SeqCst), 1, "executed exactly once");

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Same command id twice → one row, one dispatch: a concurrent duplicate
/// attaches to the in-flight execution instead of re-enqueueing.
#[tokio::test]
async fn same_command_id_twice_one_row_one_dispatch() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    let executor = CountingExecutor::counting();
    let executed = executor.count.clone();
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(executor)));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
    )
    .unwrap();

    let id: CommandId = Uuid::new_v4();
    let (a, b) = tokio::join!(
        backend.exec_tracked(id, "box", cmd(&["echo", "once"]), Some("call-a".into())),
        backend.exec_tracked(id, "box", cmd(&["echo", "once"]), Some("call-b".into())),
    );
    assert_eq!(a.unwrap().stdout, "ran-echo-once");
    assert_eq!(b.unwrap().stdout, "ran-echo-once");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exec_queue")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1, "one row for one command id");
    assert_eq!(
        executed.load(Ordering::SeqCst),
        1,
        "one dispatch on the wire"
    );

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// The per-endpoint in-flight cap is respected across a re-drive burst:
/// six queued commands against an endpoint announcing `max_concurrent = 2`
/// never exceed two concurrent executions, and all six resolve.
#[tokio::test]
async fn per_endpoint_inflight_cap_respected() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    let (gate_tx, gate_rx) = watch::channel(false);
    let executor = CountingExecutor::gated(gate_rx);
    let running = executor.running.clone();
    let peak = executor.peak.clone();
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(executor)).with_max_concurrent(2));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr)
            .with_heartbeat(Duration::from_millis(25))
            .with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
    )
    .unwrap();

    // Six distinct command ids flood the queue at once.
    let mut tasks = Vec::new();
    for i in 0..6 {
        let backend = backend.clone();
        tasks.push(tokio::spawn(async move {
            backend
                .exec_tracked(Uuid::new_v4(), "box", cmd(&["work", &i.to_string()]), None)
                .await
                .unwrap()
        }));
    }
    until("two commands start executing", || {
        running.load(Ordering::SeqCst) == 2
    })
    .await;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            running.load(Ordering::SeqCst),
            2,
            "cap must hold: exactly two concurrent executions"
        );
    }
    gate_tx.send(true).unwrap();
    for task in tasks {
        assert!(task.await.unwrap().exit_code == Some(0));
    }
    assert_eq!(peak.load(Ordering::SeqCst), 2, "peak concurrency == cap");

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// C11 liveness pattern, on a **single-threaded** runtime: while a slow
/// remote command executes through the durable queue, the runtime's other
/// tasks keep ticking — sqlite rides the async sqlx pool and nothing
/// blocks the worker.
#[tokio::test(flavor = "current_thread")]
async fn slow_remote_command_never_blocks_the_runtime() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    let executor = CountingExecutor::counting();
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(executor)));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::new(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
    )
    .unwrap();

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = {
        let ticks = ticks.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                ticks.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    // argv[0] = seconds to sleep inside the endpoint's executor.
    let result = backend
        .exec_tracked(Uuid::new_v4(), "box", cmd(&["0.5"]), None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    ticker.abort();
    assert!(
        ticks.load(Ordering::SeqCst) >= 20,
        "runtime starved during remote exec: only {} ticks in ~500ms",
        ticks.load(Ordering::SeqCst)
    );

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Store-level contract: enqueue dedupe, monotone status transitions, and
/// committed `done` rows are immutable (the runtime half of the C5 review
/// assertion).
#[tokio::test]
async fn store_enqueue_dedupe_and_status_transitions() {
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    let id: CommandId = Uuid::new_v4();
    assert!(matches!(
        store
            .enqueue(id, "box", &cmd(&["echo"]), None)
            .await
            .unwrap(),
        EnqueueOutcome::Inserted
    ));
    assert!(matches!(
        store
            .enqueue(id, "box", &cmd(&["echo"]), None)
            .await
            .unwrap(),
        EnqueueOutcome::Existing(_)
    ));
    assert!(store.mark_inflight(id).await.unwrap());
    assert!(!store.mark_inflight(id).await.unwrap());
    let outcome = kian_exec::store::CommandOutcome::Ok {
        result: ExecResult {
            exit_code: Some(0),
            stdout: "done".into(),
            stderr: String::new(),
            timed_out: false,
            redactions: Vec::new(),
        },
    };
    assert!(store.mark_done(id, &outcome).await.unwrap());
    let settled = store.get(id).await.unwrap().unwrap();
    assert_eq!(settled.status, QueueStatus::Done);
    let updated_at = settled.updated_at;

    // Terminal rows are immutable: no later transition touches a committed
    // done row, and settled rows leave the drivable set.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!store.mark_done(id, &outcome).await.unwrap());
    assert!(!store.mark_abandoned(id).await.unwrap());
    assert!(!store.mark_inflight(id).await.unwrap());
    assert_eq!(
        store.get(id).await.unwrap().unwrap().updated_at,
        updated_at,
        "committed done row was mutated"
    );
    assert!(store.drivable("box").await.unwrap().is_empty());

    // Abandoned is terminal too.
    let other: CommandId = Uuid::new_v4();
    store
        .enqueue(other, "box", &cmd(&["echo"]), None)
        .await
        .unwrap();
    assert!(store.mark_abandoned(other).await.unwrap());
    assert!(!store.mark_done(other, &outcome).await.unwrap());
    assert!(store.drivable("box").await.unwrap().is_empty());

    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Reopening the database is a no-op migration (the chain is idempotent)
/// and committed rows survive.
#[tokio::test]
async fn store_reopen_is_idempotent_and_rows_survive() {
    let db = temp_db();
    let id: CommandId = Uuid::new_v4();
    {
        let store = ExecStore::open(&db).await.unwrap();
        store
            .enqueue(id, "box", &cmd(&["echo"]), None)
            .await
            .unwrap();
        store.close().await;
    }
    {
        let store = ExecStore::open(&db).await.unwrap();
        let row = store.get(id).await.unwrap().expect("row survived");
        assert_eq!(row.status, QueueStatus::Pending);
        store.close().await;
    }
    let _ = std::fs::remove_file(&db);
}

// -- C52: queue-row leases ---------------------------------------------------

/// The single-statement row lease over sqlite (the same code path
/// postgres rides): claim → fresh-foreign refusal → heartbeat
/// extension → expiry takeover → same-holder re-claim → terminal
/// freeze. Liveness only — the lease never touches results.
#[tokio::test]
async fn row_lease_claims_expire_and_redrive_on_sqlite() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();
    let id: CommandId = Uuid::new_v4();
    store
        .enqueue(id, "box", &cmd(&["echo", "lease"]), None)
        .await
        .unwrap();

    // Unclaimed row: the first claimant takes ownership.
    assert!(
        store
            .claim_queue_row(id, "replica-a", 1_000, 0)
            .await
            .unwrap()
    );
    // Fresh lease, different holder: refused, nothing written.
    assert!(
        !store
            .claim_queue_row(id, "replica-b", 1_000, 500)
            .await
            .unwrap()
    );
    // The owner's heartbeat pushes the deadline out…
    assert!(
        store
            .heartbeat_queue_row(id, "replica-a", 1_000, 800)
            .await
            .unwrap()
    );
    // …so the foreign claim at 1500 is still inside (800+1000).
    assert!(
        !store
            .claim_queue_row(id, "replica-b", 1_000, 1_500)
            .await
            .unwrap()
    );
    // Past expiry (1801 > 1800) any replica takes over honestly.
    assert!(
        store
            .claim_queue_row(id, "replica-b", 1_000, 1_801)
            .await
            .unwrap()
    );
    // Same-holder re-claim always lands (single-replica re-drive).
    assert!(
        store
            .claim_queue_row(id, "replica-b", 1_000, 1_900)
            .await
            .unwrap()
    );
    // A foreign heartbeat never extends someone else's lease.
    assert!(
        !store
            .heartbeat_queue_row(id, "replica-a", 1_000, 1_950)
            .await
            .unwrap()
    );

    // Terminal rows freeze: once done, no claim ever lands again.
    let outcome = kian_exec::store::CommandOutcome::Ok {
        result: ExecResult {
            exit_code: Some(0),
            stdout: "ok".into(),
            stderr: String::new(),
            timed_out: false,
            redactions: Vec::new(),
        },
    };
    assert!(store.mark_done(id, &outcome).await.unwrap());
    assert!(
        !store
            .claim_queue_row(id, "replica-c", 1_000, 5_000)
            .await
            .unwrap()
    );
    assert!(
        !store
            .heartbeat_queue_row(id, "replica-b", 1_000, 5_000)
            .await
            .unwrap()
    );

    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// The async-jobs twin of the row lease on sqlite (claim / fresh-foreign
/// refusal / expiry takeover / terminal freeze).
#[tokio::test]
async fn async_row_lease_claims_and_expires_on_sqlite() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();
    let job = Uuid::new_v4();
    let session = kian_core::model::SessionId(Uuid::new_v4());
    store
        .insert_async_job(job, session, "box", &cmd(&["echo", "a"]), None)
        .await
        .unwrap();

    assert!(
        store
            .claim_async_row(job, "replica-a", 1_000, 0)
            .await
            .unwrap()
    );
    assert!(
        !store
            .claim_async_row(job, "replica-b", 1_000, 500)
            .await
            .unwrap()
    );
    assert!(
        store
            .heartbeat_async_row(job, "replica-a", 1_000, 800)
            .await
            .unwrap()
    );
    assert!(
        !store
            .claim_async_row(job, "replica-b", 1_000, 1_500)
            .await
            .unwrap()
    );
    assert!(
        store
            .claim_async_row(job, "replica-b", 1_000, 1_801)
            .await
            .unwrap()
    );

    // Terminal freeze: a settled job row is never claimable again.
    store
        .mark_async_done(job, &serde_json::json!({"stdout": "x"}))
        .await
        .unwrap();
    assert!(
        !store
            .claim_async_row(job, "replica-c", 1_000, 5_000)
            .await
            .unwrap()
    );

    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Drive-loop integration: a queue row freshly leased by *another*
/// replica is not dispatched by this backend's driver; once that lease
/// expires, this replica re-drives the row honestly to `done`
/// (at-least-once; command-id correlation is the safety net).
#[tokio::test]
async fn foreign_row_lease_blocks_dispatch_until_expiry() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();

    let (addr, listener) = ephemeral_listener().await;
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    // This replica is "replica-a"; its row-lease TTL is short (400ms)
    // so the foreign lease below expires within the test's bounds.
    let backend = RemoteExecBackend::with_row_leases(
        store.clone(),
        vec![("box".into(), manager.clone())],
        "box",
        queue_config(8_000),
        "replica-a",
        Duration::from_millis(400),
    )
    .unwrap();

    let id: CommandId = Uuid::new_v4();
    store
        .enqueue(id, "box", &cmd(&["echo", "leased"]), None)
        .await
        .unwrap();
    // Another replica owns the row, fresh for a long window — no race
    // with the "must not dispatch" observation below.
    assert!(
        store
            .claim_queue_row(id, "replica-b", 10_000, kian_exec::store::now_ms())
            .await
            .unwrap()
    );

    // Endpoint online — the driver loads the row but the claim refuses:
    // no dispatch may happen while the foreign lease is fresh.
    let endpoint = Arc::new(Endpoint::new("box", Arc::new(kian_endpoint::LocalExecutor)));
    tokio::spawn(endpoint.serve(listener));
    until_async("endpoint connected", || async {
        store
            .endpoints()
            .await
            .map(|rows| {
                rows.iter()
                    .any(|r| r.endpoint_id == "box" && r.state == "online")
            })
            .unwrap_or(false)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let row = store.get(id).await.unwrap().expect("row present");
    assert_eq!(
        row.status,
        QueueStatus::Pending,
        "a freshly foreign-leased row must not be dispatched"
    );

    // The foreign holder re-claims with a short TTL (its own liveness
    // shrinking — the deterministic expiry knob), after which this
    // replica's driver claims on its next poll and re-drives the row
    // to a real result.
    assert!(
        store
            .claim_queue_row(id, "replica-b", 300, kian_exec::store::now_ms())
            .await
            .unwrap(),
        "same-holder re-claim must land"
    );
    until_async("row re-driven after lease expiry", || async {
        store
            .get(id)
            .await
            .map(|row| matches!(row, Some(r) if r.status == QueueStatus::Done))
            .unwrap_or(false)
    })
    .await;
    let row = store.get(id).await.unwrap().unwrap();
    assert_eq!(
        row.outcome.unwrap().to_exec().unwrap().stdout.trim(),
        "leased"
    );

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}
