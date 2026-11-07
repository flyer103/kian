//! Real-TCP pairing tests for the KiandProto server half (card C19
//! acceptance).
//!
//! Every test pairs `kian_exec::ConnectionManager` against the real
//! endpoint-side crate (`kian_endpoint::Endpoint`) over an ephemeral-port
//! TCP listener — no in-process channel shortcuts, no credentials. The
//! wire contract's two independent declarations are held together by the
//! golden tests in each crate plus the version-match assertion below.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::seams::{ExecBackend, ExecCommand, ExecResult};
use kian_endpoint::Endpoint;
use kian_exec::proto::ErrorCode;
use kian_exec::{BackoffConfig, ConnectionConfig, ConnectionError, ConnectionManager};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};

/// Local e2e discipline (round-2 plan §1 rule 6): raw TCP ignores proxies,
/// but the rule is cheap to honor and keeps every e2e suite uniform.
fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: no other thread in this test binary reads the
        // environment, and the write happens exactly once before any
        // socket is opened by the calling test.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

/// Fast, bounded reconnect backoff for tests.
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

fn cmd_with_timeout(argv: &[&str], timeout_ms: u64) -> ExecCommand {
    ExecCommand {
        timeout_ms: Some(timeout_ms),
        ..cmd(argv)
    }
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

/// Await `connected == true` on the status projection.
async fn await_connected(manager: &ConnectionManager) {
    until("manager connects", || manager.status().connected).await;
}

/// Executor that gates every execution on a shared watch flag and tracks
/// concurrent + peak executions (same shape as the endpoint crate's test
/// executor — reimplemented here against the public `ExecBackend` seam).
#[derive(Clone)]
struct GateExecutor {
    running: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    gate: watch::Receiver<bool>,
}

impl ExecBackend for GateExecutor {
    fn exec(
        &self,
        _cmd: ExecCommand,
    ) -> BoxFuture<'static, Result<ExecResult, kian_core::seams::ExecError>> {
        let running = self.running.clone();
        let peak = self.peak.clone();
        let mut gate = self.gate.clone();
        Box::pin(async move {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            let _ = gate.wait_for(|open| *open).await;
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "gated".into(),
                stderr: String::new(),
                timed_out: false,
                redactions: Vec::new(),
            })
        })
    }
}

/// Bind an ephemeral listener and return (addr, listener).
async fn ephemeral_listener() -> (std::net::SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, listener)
}

/// Both halves must pin the same provisional protocol version — the
/// two-declarations-one-contract rule.
#[test]
fn protocol_versions_of_both_halves_match() {
    assert_eq!(
        kian_exec::PROTOCOL_VERSION,
        kian_endpoint::proto::PROTOCOL_VERSION
    );
}

/// Card acceptance: real-TCP pairing against `Endpoint::serve` — hello →
/// heartbeat observed → command → result, including error and timed-out
/// results; ping answered; graceful shutdown disconnects.
#[tokio::test]
async fn real_tcp_pair_hello_heartbeat_command_result() {
    no_proxy();
    let endpoint = Arc::new(Endpoint::new(
        "dev-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = ConnectionManager::new(
        ConnectionConfig::new(addr)
            .with_heartbeat(Duration::from_millis(40))
            .with_backoff(fast_backoff()),
    );
    manager.start();

    // Hello observed: identity and announced cap projected.
    await_connected(&manager).await;
    let status = manager.status();
    assert_eq!(status.endpoint_id, "dev-box");
    assert_eq!(
        status.max_concurrent,
        kian_endpoint::endpoint::DEFAULT_MAX_CONCURRENT as u32
    );

    // Heartbeat observed: freshness advances (endpoint-reported ts).
    let first_seen = manager.status().last_seen_ms;
    until("heartbeat freshness advances", || {
        manager.status().last_seen_ms > first_seen
    })
    .await;

    // Command → successful result.
    let ok = manager.dispatch(cmd(&["echo", "over-tcp"])).await.unwrap();
    assert_eq!(ok.unwrap().stdout.trim(), "over-tcp");

    // Error result: a command the endpoint cannot spawn flows back
    // honestly as a command error (never a fake success).
    let failed = manager
        .dispatch(cmd(&["kian-definitely-missing-binary"]))
        .await
        .unwrap();
    let error = failed.expect_err("spawn failure must flow back");
    assert_eq!(error.code, ErrorCode::Spawn);

    // Timed-out result: the endpoint killed the child and says so.
    let timed_out = manager
        .dispatch(cmd_with_timeout(&["sleep", "5"], 100))
        .await
        .unwrap()
        .unwrap();
    assert!(timed_out.timed_out);
    assert_eq!(timed_out.exit_code, None);

    // Ping is answered by a heartbeat (which refreshes last_seen).
    let before = manager.status().last_seen_ms;
    manager.ping().await.unwrap();
    until("ping refreshed freshness", || {
        manager.status().last_seen_ms > before
    })
    .await;

    // Graceful shutdown: status leaves the connected state.
    manager.shutdown().await;
    assert!(!manager.status().connected);
}

/// Handshake against an endpoint announcing an incompatible protocol
/// version is rejected with the named error and is terminal: no welcome
/// is sent, no reconnect loop spins, every dispatch reports it.
#[tokio::test]
async fn version_mismatch_handshake_is_rejected_as_terminal() {
    no_proxy();
    // A raw fake endpoint that speaks the wrong version.
    let (addr, listener) = ephemeral_listener().await;
    let (welcome_seen_tx, welcome_seen_rx) = tokio::sync::oneshot::channel::<bool>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        // Announce v+1.
        let hello = r#"{"type":"hello","v":1,"endpoint_id":"future","max_concurrent":4}"#;
        writer.write_all(hello.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        // Watch whether the server (wrongly) welcomes us.
        let mut lines = BufReader::new(reader).lines();
        let welcomed = match lines.next_line().await.unwrap() {
            Some(line) => line.contains("welcome"),
            None => false, // server dropped the transport — correct
        };
        let _ = welcome_seen_tx.send(welcomed);
    });

    let manager = ConnectionManager::new(ConnectionConfig::new(addr).with_backoff(fast_backoff()));
    manager.start();

    let error = manager.dispatch(cmd(&["echo", "hi"])).await.unwrap_err();
    assert_eq!(
        error,
        ConnectionError::VersionMismatch {
            endpoint: 1,
            server: kian_exec::PROTOCOL_VERSION
        }
    );
    // Terminal and durable: the manager did not spin reconnecting.
    until("terminal error recorded", || {
        matches!(
            manager.terminal_error(),
            Some(ConnectionError::VersionMismatch { .. })
        )
    })
    .await;
    // No welcome ever hit the wire.
    assert!(
        !welcome_seen_rx.await.unwrap(),
        "server must not welcome a mismatched endpoint"
    );
    manager.shutdown().await;
}

/// Heartbeat freshness strictly advances while the connection is healthy
/// (two distinct increasing endpoint-reported timestamps), and `inflight`
/// is projected from the heartbeat body.
#[tokio::test]
async fn heartbeat_freshness_advances() {
    no_proxy();
    let endpoint = Arc::new(Endpoint::new(
        "hb-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = ConnectionManager::new(
        ConnectionConfig::new(addr)
            .with_heartbeat(Duration::from_millis(25))
            .with_backoff(fast_backoff()),
    );
    manager.start();
    await_connected(&manager).await;

    let mut status = manager.subscribe_status();
    let mut increasing = 0usize;
    let mut last = 0u64;
    for _ in 0..1_000 {
        if status.changed().await.is_err() {
            break;
        }
        let current = status.borrow_and_update().last_seen_ms;
        if current > last {
            increasing += 1;
            last = current;
            if increasing >= 2 {
                break;
            }
        }
    }
    assert!(
        increasing >= 2,
        "two strictly increasing heartbeats not observed"
    );
    assert_eq!(
        manager.status().inflight,
        0,
        "idle connection reports zero inflight"
    );
    manager.shutdown().await;
}

/// A transport loss under an in-flight command fails that command's future
/// with a transport error (never a fake result); the manager reconnects
/// with bounded backoff and the next command succeeds.
///
/// The endpoint side is a hand-rolled raw TCP peer (not `Endpoint::serve`)
/// because the injected failure is a **hard socket close mid-command** —
/// the one thing the real endpoint never does to itself. The manager's
/// pairing against the real `Endpoint::serve` is covered by the other
/// tests; this one owns the wire so the vanish is deterministic.
#[tokio::test]
async fn disconnect_fails_inflight_command_then_reconnect_succeeds() {
    no_proxy();
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(async move {
        // Connection 1: hello → welcome → take the command → vanish
        // without answering (the socket drops with the command in flight).
        {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            writer
                .write_all(br#"{"type":"hello","v":0,"endpoint_id":"disc","max_concurrent":2}"#)
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
            let mut lines = BufReader::new(reader).lines();
            let welcome = lines.next_line().await.unwrap().unwrap();
            assert!(welcome.contains("welcome"));
            let command = lines.next_line().await.unwrap().unwrap();
            assert!(
                command.contains(r#""type":"command""#),
                "must receive the command"
            );
            drop(writer);
            drop(lines); // socket dies with the command in flight
        }
        // Connection 2 (the automatic reconnect): serve the command
        // honestly this time.
        {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            writer
                .write_all(br#"{"type":"hello","v":0,"endpoint_id":"disc","max_concurrent":2}"#)
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
            let mut lines = BufReader::new(reader).lines();
            let welcome = lines.next_line().await.unwrap().unwrap();
            assert!(welcome.contains("welcome"));
            let command_line = lines.next_line().await.unwrap().unwrap();
            let frame: serde_json::Value = serde_json::from_str(&command_line).unwrap();
            let id = frame["id"].as_str().unwrap();
            let result = format!(
                r#"{{"type":"result","id":"{id}","result":{{"exit_code":0,"stdout":"again","stderr":"","timed_out":false}}}}"#
            );
            writer.write_all(result.as_bytes()).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
            // Drain until the server says bye (or the socket closes).
            while let Ok(Some(line)) = lines.next_line().await {
                if line.contains(r#""type":"bye""#) {
                    break;
                }
            }
        }
    });

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    await_connected(&manager).await;

    // A command whose answer will never come: the endpoint vanishes under
    // it. The dispatch future must fail with a transport error — never
    // hang, never fabricate a result.
    let dispatch_manager = manager.clone();
    let inflight = tokio::spawn(async move { dispatch_manager.dispatch(cmd(&["blocked"])).await });
    let outcome = inflight.await.unwrap().unwrap_err();
    assert_eq!(outcome, ConnectionError::TransportClosed);
    assert!(
        !manager.status().connected,
        "status must leave connected on loss"
    );

    // The reconnect is automatic (bounded backoff), and the next command
    // succeeds on the fresh connection.
    await_connected(&manager).await;
    let result = manager
        .dispatch(cmd(&["echo", "again"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.stdout, "again");
    manager.shutdown().await;
}

/// The manager never keeps more commands in flight than the endpoint's
/// announced `max_concurrent` (manager-side semaphore + heartbeat-reported
/// inflight never exceed it), and every pipelined result comes back.
#[tokio::test]
async fn concurrency_never_exceeds_announced_max_concurrent() {
    no_proxy();
    let (gate_tx, gate_rx) = watch::channel(false);
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let executor = GateExecutor {
        running: running.clone(),
        peak: peak.clone(),
        gate: gate_rx,
    };
    let endpoint = Arc::new(Endpoint::new("capped", Arc::new(executor)).with_max_concurrent(2));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr)
            .with_heartbeat(Duration::from_millis(25))
            .with_backoff(fast_backoff()),
    ));
    manager.start();
    await_connected(&manager).await;
    assert_eq!(manager.status().max_concurrent, 2);

    // Dispatch six commands concurrently; the manager pipelines them.
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let manager = manager.clone();
        tasks.push(tokio::spawn(async move {
            manager.dispatch(cmd(&["work"])).await
        }));
    }
    until("two commands start executing", || {
        running.load(Ordering::SeqCst) == 2
    })
    .await;
    // Give any over-cap execution ample time to (wrongly) start, and
    // sample heartbeat-reported inflight while saturated.
    let mut max_reported_inflight = 0u32;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        max_reported_inflight = max_reported_inflight.max(manager.status().inflight);
        assert_eq!(
            running.load(Ordering::SeqCst),
            2,
            "cap must hold: exactly two concurrent executions"
        );
    }
    assert!(
        max_reported_inflight <= 2,
        "heartbeat-reported inflight exceeded the cap"
    );

    gate_tx.send(true).unwrap();
    for task in tasks {
        let outcome = task.await.unwrap().unwrap().unwrap();
        assert_eq!(outcome.stdout, "gated");
    }
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    manager.shutdown().await;
}

/// Pipelined commands correlate by id regardless of completion order: the
/// fast command's result arrives first and lands on the fast caller.
#[tokio::test]
async fn pipelined_results_correlate_by_command_id_out_of_order() {
    no_proxy();
    let endpoint = Arc::new(Endpoint::new(
        "pipe-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(endpoint.serve(listener));

    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    await_connected(&manager).await;

    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let slow = {
        let manager = manager.clone();
        let order = order.clone();
        tokio::spawn(async move {
            let result = manager
                .dispatch(cmd(&["sleep", "0.4"]))
                .await
                .unwrap()
                .unwrap();
            order.lock().await.push("slow");
            assert_eq!(result.exit_code, Some(0));
        })
    };
    let fast = {
        let manager = manager.clone();
        let order = order.clone();
        tokio::spawn(async move {
            let result = manager
                .dispatch(cmd(&["echo", "fast"]))
                .await
                .unwrap()
                .unwrap();
            order.lock().await.push("fast");
            assert_eq!(result.stdout.trim(), "fast");
        })
    };
    fast.await.unwrap();
    slow.await.unwrap();
    assert_eq!(*order.lock().await, vec!["fast", "slow"]);
    manager.shutdown().await;
}

/// Shutdown is graceful on the wire: the server says `bye` (the raw frame
/// is observable at the socket) and everything parked afterwards fails
/// with `Shutdown`.
#[tokio::test]
async fn shutdown_sends_bye_on_the_wire() {
    no_proxy();
    let (addr, listener) = ephemeral_listener().await;
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        // The endpoint opens: hello at v0, then awaits the welcome.
        let mut lines = BufReader::new(reader).lines();
        writer
            .write_all(br#"{"type":"hello","v":0,"endpoint_id":"fake","max_concurrent":1}"#)
            .await
            .unwrap();
        writer.write_all(b"\n").await.unwrap();
        let welcome = lines.next_line().await.unwrap().unwrap();
        assert!(
            welcome.contains(r#""type":"welcome""#),
            "server must welcome, got: {welcome}"
        );
        // The next server frame must be bye.
        let bye = lines.next_line().await.unwrap().unwrap();
        let frame = kian_exec::proto::decode_server_frame(&bye).unwrap();
        assert_eq!(frame, kian_exec::proto::ServerFrame::Bye);
        // Then the server closes its side.
        assert!(lines.next_line().await.expect("clean eof").is_none());
    });

    let manager = ConnectionManager::new(ConnectionConfig::new(addr).with_backoff(fast_backoff()));
    manager.start();
    await_connected(&manager).await;
    manager.shutdown().await;
    // After shutdown every operation reports it honestly.
    assert_eq!(
        manager.dispatch(cmd(&["echo", "late"])).await.unwrap_err(),
        ConnectionError::Shutdown
    );
    assert_eq!(manager.ping().await.unwrap_err(), ConnectionError::Shutdown);
}
