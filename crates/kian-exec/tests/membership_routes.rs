//! Membership + runtime routes + queue-first L1 tests (R12-04 /
//! kian-130): the exec-layer half of the R12 contract over the R12-03
//! listener — spec §4 (Q1–Q5), §3 (M1/M5/M7), §9 (A4/A15).
//!
//! The pure membership state table and the listener's handshake matrix
//! live in `membership.rs` (unit) and `registration.rs` (integration);
//! this suite covers what only the **wiring** can prove: enqueue parks
//! without a route, registration drains, generation-scoped detach,
//! boot rebuild, and the full register→dispatch→result loop over real
//! loopback TCP against the real worker half.
//!
//! Fake token value only — never a real secret.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use uuid::Uuid;

use kian_core::seams::{ExecBackend as _, ExecCommand, ExecError, ExecResult};
use kian_exec::proto::{EndpointFrame, ServerFrame};
use kian_exec::watchdog::SystemClock;
use kian_exec::{
    DefaultRoute, ExecStore, LeaveReason, RegisteredWorker, RegistrationConfig,
    RegistrationListener, RegistrationSink, RemoteExecBackend, RemoteExecConfig, RouteTransport,
    WorkerDirectory, WorkerToken,
};

const TOKEN: &str = "t0ken-not-a-secret";

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-routes-{}.sqlite", Uuid::new_v4()))
}

fn cmd(argv: &[&str]) -> ExecCommand {
    ExecCommand {
        argv: argv.iter().map(|s| s.to_string()).collect(),
        env: Default::default(),
        cwd: None,
        timeout_ms: None,
    }
}

fn queue_config(queue_timeout_ms: u64) -> RemoteExecConfig {
    RemoteExecConfig {
        queue_timeout: Duration::from_millis(queue_timeout_ms),
        poll_interval: Duration::from_millis(40),
        max_redrives: 1,
        redrive_grace: Duration::ZERO,
    }
}

/// A membership backend: zero static routes, a directory, dynamic
/// default (the `workers:`-only deployment shape, spec §4.1).
async fn membership_backend(timeout_ms: u64, directory: Arc<WorkerDirectory>) -> RemoteExecBackend {
    let store = ExecStore::open(temp_db()).await.expect("exec store");
    RemoteExecBackend::with_membership(
        store,
        Vec::new(),
        DefaultRoute::Dynamic,
        queue_config(timeout_ms),
        "kian-test",
        Duration::from_millis(timeout_ms),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(kian_exec::MembershipConfig {
            directory: Arc::clone(&directory),
            offline_after_ms: 30_000,
        }),
    )
    .expect("membership backend constructs with zero static routes")
}

/// A fabricated live registered connection (the listener's handoff
/// shape): the test owns both channel ends — it receives `Command`
/// frames through `writer_rx` and answers through `inbound_tx`.
struct FakeWorker {
    writer_rx: mpsc::Receiver<ServerFrame>,
    inbound_tx: mpsc::Sender<EndpointFrame>,
}

fn registered(id: &str, generation: u64) -> (RegisteredWorker, FakeWorker) {
    let (writer_tx, writer_rx) = mpsc::channel(64);
    let (inbound_tx, inbound_rx) = mpsc::channel(64);
    (
        RegisteredWorker {
            description: None,
            capabilities: Vec::new(),
            id: id.into(),
            generation,
            peer_addr: "127.0.0.1:50000".parse().unwrap(),
            version: kian_exec::PROTOCOL_VERSION,
            max_concurrent: 2,
            writer: writer_tx,
            inbound: inbound_rx,
        },
        FakeWorker {
            writer_rx,
            inbound_tx,
        },
    )
}

async fn until_async<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 5s: {what}");
}

fn ok_result(stdout: &str) -> ExecResult {
    ExecResult {
        exit_code: Some(0),
        stdout: stdout.into(),
        stderr: String::new(),
        timed_out: false,
        redactions: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Q1 — queue-first: enqueue never requires a live route
// ---------------------------------------------------------------------------

/// A command for an absent worker enqueues durably and parks `pending`
/// — never the pre-R12 "no endpoint route registered" error — and the
/// caller's `queue_timeout` abandons it honestly (Q1).
#[tokio::test]
async fn enqueue_with_no_route_parks_then_times_out_honestly() {
    let backend = membership_backend(300, WorkerDirectory::new()).await;
    let id = Uuid::new_v4();

    // Spawned: the enqueue must run (the row parks) while we observe.
    let caller = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(id, "ghost-worker", cmd(&["echo", "hi"]), None)
                .await
        })
    };
    // The row is durable and pending while no route exists.
    until_async("row pending", || async {
        matches!(
            backend.store().get(id).await,
            Ok(Some(row)) if matches!(row.status, kian_exec::QueueStatus::Pending)
        )
    })
    .await;

    let err = caller.await.expect("caller task").unwrap_err();
    assert!(matches!(err, ExecError::Timeout), "was: {err:?}");
    match backend.store().get(id).await {
        Ok(Some(row)) => {
            assert!(
                matches!(row.status, kian_exec::QueueStatus::Abandoned),
                "honest row"
            );
            assert_eq!(row.endpoint_id, "ghost-worker", "no cross-worker drift");
        }
        other => panic!("row missing: {other:?}"),
    }
    backend.shutdown().await;
}

// ---------------------------------------------------------------------------
// Q2 — registration drains; A4 — re-drive after loss
// ---------------------------------------------------------------------------

/// A row parked while the worker was absent is drained exactly once by
/// the attach (registration): one `Command` frame on the wire, one
/// correlated result, one settled row (command-id dedupe).
#[tokio::test]
async fn registration_drains_the_parked_row_exactly_once() {
    let backend = membership_backend(10_000, WorkerDirectory::new()).await;
    let id = Uuid::new_v4();

    // Park first: no route yet (spawned so the enqueue runs).
    let caller = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(id, "builder-1", cmd(&["echo", "parked"]), None)
                .await
        })
    };
    until_async("parked pending", || async {
        matches!(
            backend.store().get(id).await,
            Ok(Some(row)) if matches!(row.status, kian_exec::QueueStatus::Pending)
        )
    })
    .await;

    // The worker registers (generation 1).
    let (worker, mut fake) = registered("builder-1", 1);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            1,
        )
        .await;

    // The route's first driver iteration dispatches the parked row.
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("drained within 3s")
        .expect("a command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame, got {frame:?}");
    };
    assert_eq!(wire_id, id, "command id is the dedupe key");
    fake.inbound_tx
        .send(EndpointFrame::CommandResult {
            id: wire_id,
            result: Some(ok_result("parked\n")),
            error: None,
        })
        .await
        .unwrap();

    let result = caller
        .await
        .expect("caller task")
        .expect("caller resolves after the drain");
    assert!(result.stdout.contains("parked"), "was: {}", result.stdout);
    match backend.store().get(id).await {
        Ok(Some(row)) => assert!(matches!(row.status, kian_exec::QueueStatus::Done)),
        other => panic!("row missing: {other:?}"),
    }
    // No second dispatch ever flies (dedupe at the row).
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fake.writer_rx.recv())
            .await
            .is_err(),
        "no duplicate command frame"
    );
    backend.shutdown().await;
}

/// No cross-worker dispatch (Q4): with only `w-1` attached, `w-2`'s row
/// stays parked — nothing is ever dispatched to another worker.
#[tokio::test]
async fn no_cross_worker_dispatch() {
    let backend = membership_backend(10_000, WorkerDirectory::new()).await;

    let (worker, mut fake) = registered("w-1", 1);
    backend
        .attach_route(
            "w-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            1,
        )
        .await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let caller1 = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(id1, "w-1", cmd(&["echo", "one"]), None)
                .await
        })
    };
    let caller2 = {
        let backend = backend.clone();
        let _ = id2;
        tokio::spawn(async move {
            backend
                .exec_tracked(id2, "w-2", cmd(&["echo", "two"]), None)
                .await
        })
    };

    // w-1's command arrives on w-1's connection only.
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("w-1 dispatched")
        .expect("command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame");
    };
    assert_eq!(wire_id, id1);
    fake.inbound_tx
        .send(EndpointFrame::CommandResult {
            id: wire_id,
            result: Some(ok_result("one\n")),
            error: None,
        })
        .await
        .unwrap();
    caller1.await.expect("caller task").expect("w-1 settles");

    // w-2 stays parked — no frame ever, row still pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fake.writer_rx.recv())
            .await
            .is_err(),
        "w-2's command must never reach w-1"
    );
    match backend.store().get(id2).await {
        Ok(Some(row)) => assert_eq!(format!("{:?}", row.status), "Pending"),
        other => panic!("w-2 row missing: {other:?}"),
    }
    // (The parked caller keeps running to its own timeout; the row
    // stays durable — at-least-once.)
    drop(caller2);
    backend.shutdown().await;
}

/// Transport loss under a dispatch leaves the row parked (A4); the
/// re-registration (new generation, new connection) re-drives it on
/// the **same** id and it settles exactly once (Q2/A15).
#[tokio::test]
async fn loss_then_re_registration_redrives_on_the_same_id() {
    let backend = membership_backend(10_000, WorkerDirectory::new()).await;
    let id = Uuid::new_v4();

    // Generation 1 registers; the command flies; the worker dies
    // without answering (transport loss).
    let (worker1, mut fake1) = registered("builder-1", 1);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker1)),
            1,
        )
        .await;
    let caller = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(id, "builder-1", cmd(&["echo", "again"]), None)
                .await
        })
    };
    let frame = tokio::time::timeout(Duration::from_secs(3), fake1.writer_rx.recv())
        .await
        .expect("first dispatch")
        .expect("command frame");
    assert!(matches!(frame, ServerFrame::Command { .. }));
    // (No answer.) The connection dies: dropping both channel ends is
    // the listener's transport-loss shape.
    drop(fake1);

    // The leave detaches generation 1's route (the sink path).
    backend.detach_route("builder-1", 1).await;

    // Generation 2 registers; the parked row re-drives on the new
    // connection.
    let (worker2, mut fake2) = registered("builder-1", 2);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker2)),
            2,
        )
        .await;
    // Generous: the re-drive is evidence-polled (40 ms) by the backend,
    // and a 2-core CI runner has starved the old 3 s bound. A timeout
    // here means the row never moved, not that the runner was slow.
    let frame = tokio::time::timeout(Duration::from_secs(15), fake2.writer_rx.recv())
        .await
        .expect("re-drive on generation 2")
        .expect("command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame");
    };
    assert_eq!(wire_id, id, "same command id re-driven (at-least-once)");
    fake2
        .inbound_tx
        .send(EndpointFrame::CommandResult {
            id: wire_id,
            result: Some(ok_result("again\n")),
            error: None,
        })
        .await
        .unwrap();

    let result = caller
        .await
        .expect("caller task")
        .expect("caller survives the route swap (Q5)");
    assert!(result.stdout.contains("again"));
    match backend.store().get(id).await {
        Ok(Some(row)) => assert!(matches!(row.status, kian_exec::QueueStatus::Done)),
        other => panic!("row missing: {other:?}"),
    }
    backend.shutdown().await;
}

// ---------------------------------------------------------------------------
// A15 — generation-scoped detach
// ---------------------------------------------------------------------------

/// A stale generation's detach is a no-op: it can never remove its
/// successor's route; the current generation's detach removes it.
#[tokio::test]
async fn stale_generation_detach_never_removes_the_successor() {
    let backend = membership_backend(10_000, WorkerDirectory::new()).await;

    let (worker, _fake) = registered("builder-1", 2);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            2,
        )
        .await;
    assert!(backend.route_ids().contains(&"builder-1".to_string()));

    // The generation-1 loss handler arrives late (the A15 race, loser
    // side): no-op.
    backend.detach_route("builder-1", 1).await;
    assert!(
        backend.route_ids().contains(&"builder-1".to_string()),
        "generation 2's route survives"
    );

    // The owning generation's detach removes it; rows were none.
    backend.detach_route("builder-1", 2).await;
    assert!(!backend.route_ids().contains(&"builder-1".to_string()));
    backend.shutdown().await;
}

// ---------------------------------------------------------------------------
// Boot rebuild (M1/M5) + the durable projection
// ---------------------------------------------------------------------------

/// A kian boot after a previous run: the registry rows persist (never
/// pruned — M5), the runtime directory is empty (no forged joins), and
/// a re-registration fills the directory again. The registry row also
/// keeps the offline-known id bindable (spec §6.3 step 3).
#[tokio::test]
async fn boot_rebuild_registry_rows_survive_directory_empty() {
    // A previous incarnation's durable projection.
    let db = temp_db();
    let old = ExecStore::open(&db).await.expect("old store");
    old.upsert_endpoint("stale-worker", "127.0.0.1:55555", 1_000, "offline")
        .await
        .unwrap();
    old.close().await;

    // The new boot: same store file, empty directory, zero routes.
    let store = ExecStore::open(&db).await.expect("new store");
    let directory = WorkerDirectory::new();
    let backend = RemoteExecBackend::with_membership(
        store.clone(),
        Vec::new(),
        DefaultRoute::Dynamic,
        queue_config(10_000),
        "kian-test",
        Duration::from_millis(10_000),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(kian_exec::MembershipConfig {
            directory: Arc::clone(&directory),
            offline_after_ms: 30_000,
        }),
    )
    .expect("backend constructs over persisted rows");

    let rows = store.endpoints().await.unwrap();
    assert_eq!(rows.len(), 1, "registry rows are never pruned");
    assert_eq!(rows[0].endpoint_id, "stale-worker");
    assert!(directory.known_ids().is_empty(), "no forged joins at boot");
    assert!(backend.route_ids().is_empty());

    // The stale id is still *known*: a command parks against it.
    let id = Uuid::new_v4();
    let caller = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(id, "stale-worker", cmd(&["echo", "x"]), None)
                .await
        })
    };
    until_async("stale id parks", || async {
        matches!(
            backend.store().get(id).await,
            Ok(Some(row)) if row.endpoint_id == "stale-worker"
                && matches!(row.status, kian_exec::QueueStatus::Pending)
        )
    })
    .await;
    drop(caller);

    // Re-registration attaches the runtime route (the directory itself
    // fills through the listener — proven by the full-loop test below).
    let (worker, fake) = registered("stale-worker", 1);
    backend
        .attach_route(
            "stale-worker",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            1,
        )
        .await;
    assert!(backend.route_ids().contains(&"stale-worker".to_string()));
    // The registry row flipped online (the durable projection).
    until_async("registry online", || async {
        matches!(
            backend.store().endpoints().await,
            Ok(rows) if rows.iter().any(|r| r.endpoint_id == "stale-worker" && r.state == "online")
        )
    })
    .await;
    drop(fake);
    backend.shutdown().await;
}

/// The dynamic default resolution (spec §4.1): with nothing known the
/// untracked path errors honestly; it never invents an id.
#[tokio::test]
async fn dynamic_default_errors_honestly_when_nothing_is_known() {
    let backend = membership_backend(200, WorkerDirectory::new()).await;
    let err = backend.exec(cmd(&["echo", "x"])).await.unwrap_err();
    match err {
        ExecError::Io(text) => assert!(
            text.contains("no execution worker is configured"),
            "was: {text}"
        ),
        // A Timeout here would mean an id was invented and parked.
        other => panic!("expected the honest Io error, got {other:?}"),
    }
    backend.shutdown().await;
}

// ---------------------------------------------------------------------------
// The full loop over real TCP: listener + sink wiring + real worker half
// ---------------------------------------------------------------------------

/// The assembly sink shape: membership edges attach/detach routes.
struct AttachingSink {
    backend: Arc<RemoteExecBackend>,
}

impl RegistrationSink for AttachingSink {
    fn member_online(&self, worker: RegisteredWorker) -> futures::future::BoxFuture<'static, ()> {
        let backend = Arc::clone(&self.backend);
        Box::pin(async move {
            let id = worker.id.clone();
            let generation = worker.generation;
            backend
                .attach_route(
                    id,
                    RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
                    generation,
                )
                .await;
        })
    }

    fn member_offline(
        &self,
        id: &str,
        generation: u64,
        _reason: LeaveReason,
    ) -> futures::future::BoxFuture<'static, ()> {
        let backend = Arc::clone(&self.backend);
        let id = id.to_string();
        Box::pin(async move {
            backend.detach_route(&id, generation).await;
        })
    }
}

/// The end-to-end proof: a command parked **before** the worker exists
/// drains through a real registration (real TCP, real worker half, real
/// executor), the member reads `online` with a route attached (M7),
/// and the graceful `bye` detaches and offlines everything (metrics
/// counted, audit edges one per generation).
#[tokio::test]
async fn full_loop_register_drains_bye_detaches() {
    let directory = WorkerDirectory::new();
    let store = ExecStore::open(temp_db()).await.expect("store");
    let backend = Arc::new(
        RemoteExecBackend::with_membership(
            store,
            Vec::new(),
            DefaultRoute::Dynamic,
            queue_config(20_000),
            "kian-test",
            Duration::from_millis(20_000),
            kian_core::secrets::SecretRedactor::patterns_only(),
            Some(kian_exec::MembershipConfig {
                directory: Arc::clone(&directory),
                offline_after_ms: 30_000,
            }),
        )
        .unwrap(),
    );

    let config = RegistrationConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        WorkerToken::new(TOKEN.into()),
        30_000,
    );
    let listener = Arc::new(RegistrationListener::new(
        config,
        Arc::new(SystemClock),
        Arc::new(AttachingSink {
            backend: Arc::clone(&backend),
        }),
        Arc::clone(&directory),
    ));
    let l = listener.clone();
    tokio::spawn(async move {
        let _ = l.run().await;
    });
    for _ in 0..200 {
        if listener.local_addr().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let addr = listener.local_addr().expect("listener bound");

    // Park a command for the not-yet-registered worker.
    let id = Uuid::new_v4();
    let caller = {
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            backend
                .exec_tracked(id, "e2e-box", cmd(&["echo", "registered"]), None)
                .await
        })
    };
    until_async("parked", || async {
        matches!(
            backend.store().get(id).await,
            Ok(Some(row)) if matches!(row.status, kian_exec::QueueStatus::Pending)
        )
    })
    .await;

    // The worker dials in (the real endpoint client + local executor).
    let endpoint = Arc::new(kian_endpoint::Endpoint::new(
        "e2e-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let conn = tokio::spawn(kian_endpoint::connect_registered(
        endpoint,
        kian_endpoint::RegisterInfo {
            endpoint_id: "e2e-box".into(),
            description: Some("e2e box".into()),
            token: kian_endpoint::proto::RegisterToken::new(TOKEN.into()),
            capabilities: Vec::new(),
        },
        addr,
        shutdown_rx,
    ));

    // The parked command drains through the registration and executes
    // for real on the worker side.
    let result = tokio::time::timeout(Duration::from_secs(10), caller)
        .await
        .expect("drains within 10s")
        .expect("caller task")
        .expect("caller resolves");
    assert!(
        result.stdout.contains("registered"),
        "was: {}",
        result.stdout
    );

    // M7: online ⇒ route attached; the durable projection says online.
    assert!(backend.route_ids().contains(&"e2e-box".to_string()));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        listener.member_state("e2e-box", now),
        Some(kian_exec::MemberState::Online)
    );
    until_async("registry online", || async {
        matches!(
            backend.store().endpoints().await,
            Ok(rows) if rows.iter().any(|r| r.endpoint_id == "e2e-box" && r.state == "online")
        )
    })
    .await;
    let metrics = listener.metrics();
    assert!(metrics.joined.load(std::sync::atomic::Ordering::Relaxed) >= 1);

    // Graceful leave: the worker says bye; the route detaches, the
    // member reads offline, the registry row flips offline, the row is
    // never pruned.
    shutdown_tx.send_replace(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
    until_async("route detached", || async {
        !backend.route_ids().contains(&"e2e-box".to_string())
    })
    .await;
    until_async("registry offline", || async {
        matches!(
            backend.store().endpoints().await,
            Ok(rows) if rows.iter().any(|r| r.endpoint_id == "e2e-box" && r.state == "offline")
        )
    })
    .await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        listener.member_state("e2e-box", now),
        Some(kian_exec::MemberState::Offline),
        "bye = graceful leave → offline (teardown window closed)"
    );
    assert!(
        metrics.left_bye.load(std::sync::atomic::Ordering::Relaxed) >= 1,
        "the bye edge counted once"
    );
    backend.shutdown().await;
}

/// With a member online, the untracked `exec` path resolves the
/// dynamic default to it (spec §4.1: first online member at call
/// time).
#[tokio::test]
async fn dynamic_default_resolves_the_online_member() {
    let directory = WorkerDirectory::new();
    let backend = membership_backend(20_000, Arc::clone(&directory)).await;

    // No member: honest error.
    assert!(backend.exec(cmd(&["echo", "x"])).await.is_err());

    // A live member registers through the full stack (the directory
    // fills via the listener; the sink attaches the route).
    let config = RegistrationConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        WorkerToken::new(TOKEN.into()),
        30_000,
    );
    let listener = Arc::new(RegistrationListener::new(
        config,
        Arc::new(SystemClock),
        Arc::new(AttachingSink {
            backend: Arc::new(backend.clone()),
        }),
        Arc::clone(&directory),
    ));
    let l = listener.clone();
    tokio::spawn(async move {
        let _ = l.run().await;
    });
    for _ in 0..200 {
        if listener.local_addr().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let addr = listener.local_addr().expect("listener bound");

    let endpoint = Arc::new(kian_endpoint::Endpoint::new(
        "default-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let conn = tokio::spawn(kian_endpoint::connect_registered(
        endpoint,
        kian_endpoint::RegisterInfo {
            endpoint_id: "default-box".into(),
            description: None,
            token: kian_endpoint::proto::RegisterToken::new(TOKEN.into()),
            capabilities: Vec::new(),
        },
        addr,
        shutdown_rx,
    ));
    until_async("member online", || async {
        !directory.known_ids().is_empty()
            && backend.route_ids().contains(&"default-box".to_string())
    })
    .await;

    // The untracked path now resolves to the online member.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        backend.exec(cmd(&["echo", "default"])),
    )
    .await
    .expect("resolves within 10s")
    .expect("executes on the online member");
    assert!(result.stdout.contains("default"), "was: {}", result.stdout);

    drop(_shutdown_tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), conn).await;
    backend.shutdown().await;
}

// ---------------------------------------------------------------------------
// A4 + ADR-033 — bounded re-drive: a worker that dies before replying
// ---------------------------------------------------------------------------

/// The defect ADR-033 bounds, on the real driver/wire path: a command whose
/// own side effect destroys the ack path (it restarts the server or kills its
/// worker) is received and executed, and the worker dies before its result can
/// fly. Before the bound, every re-attach re-dispatched the row forever. Now
/// the worker sees `1 + max_redrives` command frames, the third attach gets
/// none, and the row converges to the honest terminal `abandoned`.
#[tokio::test]
async fn redrive_bound_converges_a_row_whose_result_is_never_delivered() {
    let store = ExecStore::open(temp_db()).await.expect("exec store");
    // A short row lease so convergence happens in test time; the caller's
    // wait is deliberately longer, so the abandonment observed is the bound's.
    let backend = RemoteExecBackend::with_membership(
        store.clone(),
        Vec::new(),
        DefaultRoute::Dynamic,
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(3_000),
            poll_interval: Duration::from_millis(40),
            max_redrives: 1,
            redrive_grace: Duration::ZERO,
        },
        "kian-test",
        Duration::from_millis(300),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(kian_exec::MembershipConfig {
            directory: WorkerDirectory::new(),
            offline_after_ms: 30_000,
        }),
    )
    .expect("membership backend");

    let id = Uuid::new_v4();
    let caller = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .exec_tracked(
                    id,
                    "builder-1",
                    cmd(&["systemctl", "restart", "kian"]),
                    Some("call-poison".into()),
                )
                .await
        })
    };
    until_async("parked pending", || async {
        matches!(
            store.get(id).await,
            Ok(Some(row)) if matches!(row.status, kian_exec::QueueStatus::Pending)
        )
    })
    .await;

    // Attempt 1: the worker receives the command and dies before replying.
    let (worker, mut fake) = registered("builder-1", 1);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            1,
        )
        .await;
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("first dispatch within 3s")
        .expect("a command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame, got {frame:?}");
    };
    assert_eq!(wire_id, id, "command id is the dedupe key");
    drop(fake);

    // Attempt 2: the at-least-once retry lands (`1 + max_redrives` = 2).
    let (worker, mut fake) = registered("builder-1", 2);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            2,
        )
        .await;
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("the re-drive lands within 3s")
        .expect("a command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame, got {frame:?}");
    };
    assert_eq!(wire_id, id);
    drop(fake);

    // Attempt 3: the bound refuses the claim — no third command frame ever
    // flies, and the row converges once the lease lapses.
    let (worker, mut fake) = registered("builder-1", 3);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            3,
        )
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1_500), fake.writer_rx.recv())
            .await
            .is_err(),
        "a replay past the bound is the bug this test exists for"
    );
    until_async("row converged to abandoned", || async {
        matches!(
            store.get(id).await,
            Ok(Some(row)) if matches!(row.status, kian_exec::QueueStatus::Abandoned)
        )
    })
    .await;
    let row = store.get(id).await.expect("row read").expect("row present");
    assert_eq!(
        row.dispatch_attempts, 2,
        "exactly 1 + max_redrives dispatches"
    );

    // The caller's honest answer: Timeout — never a fabricated result.
    let err = caller.await.expect("caller task").unwrap_err();
    assert!(matches!(err, ExecError::Timeout), "was: {err:?}");

    backend.shutdown().await;
}

/// ADR-037: a row whose last dispatch is younger than `redrive_grace` is
/// not re-driven the moment a route re-attaches — the driver waits, so it
/// cannot race an execution that may still be running on the worker. The
/// replay lands once the grace elapses; nothing is settled by the wait.
#[tokio::test]
async fn redrive_grace_postpones_a_recent_replay() {
    let store = ExecStore::open(temp_db()).await.expect("exec store");
    let backend = RemoteExecBackend::with_membership(
        store.clone(),
        Vec::new(),
        DefaultRoute::Dynamic,
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(5_000),
            poll_interval: Duration::from_millis(40),
            max_redrives: 1,
            redrive_grace: Duration::from_millis(800),
        },
        "kian-test",
        Duration::from_millis(300),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(kian_exec::MembershipConfig {
            directory: WorkerDirectory::new(),
            offline_after_ms: 30_000,
        }),
    )
    .expect("membership backend");

    let id = Uuid::new_v4();
    store
        .enqueue(
            id,
            "builder-1",
            &cmd(&["echo", "grace"]),
            Some("call-grace"),
        )
        .await
        .unwrap();

    // Attempt 1: dispatched, then the worker dies before replying.
    let (worker, mut fake) = registered("builder-1", 1);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            1,
        )
        .await;
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("first dispatch within 3s")
        .expect("a command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame, got {frame:?}");
    };
    assert_eq!(wire_id, id);
    drop(fake);

    // A reconnect right after the dispatch must NOT replay it: the grace
    // postpones the re-drive instead of racing the executing worker.
    let (worker, mut fake) = registered("builder-1", 2);
    backend
        .attach_route(
            "builder-1",
            RouteTransport::Registered(kian_exec::RegisteredTransport::new(worker)),
            2,
        )
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fake.writer_rx.recv())
            .await
            .is_err(),
        "a row dispatched within the grace must not be re-driven immediately"
    );

    // Once the grace elapses the replay lands (at-least-once is intact).
    let frame = tokio::time::timeout(Duration::from_secs(3), fake.writer_rx.recv())
        .await
        .expect("the replay lands after the grace")
        .expect("a command frame");
    let ServerFrame::Command { id: wire_id, .. } = frame else {
        panic!("expected a command frame, got {frame:?}");
    };
    assert_eq!(wire_id, id);

    backend.shutdown().await;
}
