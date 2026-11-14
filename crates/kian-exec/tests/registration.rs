//! Registration-listener L1 tests (kian-129 / R12-03): the accept path
//! validated over real loopback TCP against the real worker half
//! (`kian_endpoint`'s register client — the dev-dependency pairing), plus
//! a raw-socket probe for adversarial frames the client never sends.
//!
//! Fake token value only — never a real secret.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;

use kian_endpoint::proto::RegisterToken;
use kian_endpoint::{Endpoint, RegisterInfo, connect_registered};
use kian_exec::watchdog::{Clock, ManualClock, SystemClock};
use kian_exec::{
    LeaveReason, MemberState, RegisteredWorker, RegistrationConfig, RegistrationListener,
    RegistrationSink, WorkerDirectory, WorkerToken, constant_time_eq, is_private_bind_addr,
    validate_bind_addr,
};

const TOKEN: &str = "t0ken-not-a-secret";
const SERVER_TOKEN: &str = "t0ken-not-a-secret";

fn worker_token() -> WorkerToken {
    WorkerToken::new(SERVER_TOKEN.into())
}

/// Recording sink: membership edges in order.
#[derive(Default)]
struct RecSink {
    online: std::sync::Mutex<Vec<(String, u64)>>,
    offline: std::sync::Mutex<Vec<(String, u64, LeaveReason)>>,
}

impl RegistrationSink for RecSink {
    fn member_online(&self, worker: RegisteredWorker) -> futures::future::BoxFuture<'static, ()> {
        // Keep the route usable inside the test by parking the inbound
        // receiver on a task that just counts frames.
        let mut inbound = worker.inbound;
        let id = worker.id.clone();
        let generation = worker.generation;
        tokio::spawn(async move { while inbound.recv().await.is_some() {} });
        self.online.lock().unwrap().push((id, generation));
        Box::pin(async {})
    }

    fn member_offline(
        &self,
        id: &str,
        generation: u64,
        reason: LeaveReason,
    ) -> futures::future::BoxFuture<'static, ()> {
        self.offline
            .lock()
            .unwrap()
            .push((id.to_string(), generation, reason));
        Box::pin(async {})
    }
}

async fn start_listener(
    static_ids: Vec<String>,
    clock: Arc<dyn Clock>,
) -> (
    Arc<RegistrationListener>,
    Arc<RecSink>,
    std::net::SocketAddr,
) {
    let config = RegistrationConfig::new("127.0.0.1:0".parse().unwrap(), worker_token(), 30_000)
        .with_static_ids(static_ids);
    let sink = Arc::new(RecSink::default());
    let listener = Arc::new(RegistrationListener::new(
        config,
        clock,
        sink.clone(),
        WorkerDirectory::new(),
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
    (listener, sink, addr)
}

fn info(id: &str, token: &str) -> RegisterInfo {
    RegisterInfo {
        endpoint_id: id.into(),
        description: Some("linux builder".into()),
        token: RegisterToken::new(token.into()),
    }
}

fn endpoint() -> Arc<Endpoint> {
    Arc::new(Endpoint::new(
        "builder-1",
        Arc::new(kian_endpoint::LocalExecutor),
    ))
}

async fn until(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 5s: {what}");
}

// ---------------------------------------------------------------------------
// Unit: bind policy (C2/C3) and constant-time compare (I9)
// ---------------------------------------------------------------------------

#[test]
fn bind_policy_matrix() {
    for accepted in [
        "127.0.0.1:7400",
        "127.8.8.8:7400",       // 127/8, not just 127.0.0.1
        "10.1.2.3:7400",        // RFC1918
        "172.16.0.1:7400",      // RFC1918
        "172.31.255.255:7400",  // RFC1918 upper edge
        "192.168.1.4:7400",     // RFC1918
        "100.64.0.1:7400",      // CGNAT
        "100.127.255.255:7400", // CGNAT upper edge
        "[::1]:7400",           // IPv6 loopback
        "[fd00::1]:7400",       // IPv6 ULA
    ] {
        let addr: std::net::SocketAddr = accepted.parse().unwrap();
        assert!(is_private_bind_addr(addr), "{accepted} must be private");
        assert!(
            validate_bind_addr(addr, false).is_ok(),
            "{accepted} accepted"
        );
    }
    for rejected in [
        "0.0.0.0:7400",        // wildcard = every interface
        "8.8.8.8:7400",        // public
        "172.32.0.1:7400",     // just outside RFC1918
        "100.63.0.1:7400",     // just outside CGNAT
        "100.128.0.1:7400",    // just outside CGNAT
        "[::]:7400",           // IPv6 wildcard
        "[fe80::1]:7400",      // link-local is public here
        "[2606:4700::1]:7400", // public v6
    ] {
        let addr: std::net::SocketAddr = rejected.parse().unwrap();
        assert!(!is_private_bind_addr(addr), "{rejected} must be public");
        let err = validate_bind_addr(addr, false).unwrap_err();
        assert!(
            err.contains("SSH tunnel") || err.contains("Tailscale"),
            "was: {err}"
        );
        assert!(err.contains("allow_public"), "was: {err}");
        // The explicit override lifts it.
        assert!(validate_bind_addr(addr, true).is_ok());
    }
}

#[test]
fn constant_time_eq_properties() {
    assert!(constant_time_eq(b"secret", b"secret"));
    assert!(!constant_time_eq(b"secret", b"secreX"));
    // Every byte position participates — a flip anywhere flips the
    // result (the mutation witness for an early-exit rewrite is the
    // source-scan assertion below plus this full-coverage matrix).
    let a = b"0123456789abcdef";
    for i in 0..a.len() {
        let mut b = *a;
        b[i] ^= 1;
        assert!(!constant_time_eq(a, &b), "position {i}");
    }
    // Length differences never compare equal, in either direction.
    assert!(!constant_time_eq(b"secret", b"secre"));
    assert!(!constant_time_eq(b"secre", b"secret"));
    assert!(!constant_time_eq(b"", b"x"));
    assert!(constant_time_eq(b"", b""));
    // ... and never vanish modulo 256: the length mismatch folds as a
    // boolean, not a byte-truncated XOR. A JSON token string can carry
    // `\u0000` padding, so the real token plus 256k NULs must not
    // authenticate (B1: the byte-wise fold made exactly that compare
    // equal — this pair is its mutation witness).
    assert!(!constant_time_eq(
        b"secret",
        &[b"secret".as_slice(), &[0u8; 256]].concat()
    ));
    assert!(!constant_time_eq(
        b"secret",
        &[b"secret".as_slice(), &[0u8; 512]].concat()
    ));
    assert!(!constant_time_eq(
        &[b"secret".as_slice(), &[0u8; 256]].concat(),
        b"secret"
    ));
}

/// The compare stays constant-time: the fold loop must contain no
/// early exit (`return`/`break`/`if diff` short-circuit) — converting it
/// to a `!=` early return makes this scan red. This is the repo's
/// established source-assertion pattern (see `kian-endpoint`'s
/// zero-session-state scan).
#[test]
fn constant_time_fold_has_no_early_exit() {
    let src = include_str!("../src/registration.rs");
    let start = src.find("pub fn constant_time_eq").expect("fn present");
    let end = src[start..]
        .find("\n}")
        .map(|i| start + i)
        .expect("fn body ends");
    let body = &src[start..end];
    let fold_start = body.find("for i in 0..len").expect("scan loop present");
    let fold = &body[fold_start..];
    let fold_end = fold.find("}\n").expect("loop ends");
    let loop_body = &fold[..fold_end];
    assert!(
        !loop_body.contains("return") && !loop_body.contains("break"),
        "the constant-time fold must scan every byte: {loop_body}"
    );
}

// ---------------------------------------------------------------------------
// Integration: handshake accept/reject matrix over real TCP
// ---------------------------------------------------------------------------

/// Good token + fresh id → welcome; the member goes online and the sink
/// sees the join.
#[tokio::test]
async fn good_token_registers_online() {
    let (_listener, sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let conn = tokio::spawn(connect_registered(
        endpoint(),
        info("builder-1", TOKEN),
        addr,
        shutdown,
    ));
    until("sink sees join", || !sink.online.lock().unwrap().is_empty()).await;
    conn.abort();
}

/// Wrong token → typed `bad_token` refusal.
#[tokio::test]
async fn bad_token_refused() {
    let (_listener, _sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let err = connect_registered(endpoint(), info("builder-1", "wrong-token"), addr, shutdown)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        kian_endpoint::EndpointError::RegisterRejected {
            reason: kian_endpoint::RefusalReason::BadToken,
            detail: None,
        }
    );
}

/// A register announcing v0 → typed `version_mismatch` with the fixed
/// detail template; never a crash, never a half connection.
#[tokio::test]
async fn version_mismatch_refused_typed() {
    let (_listener, _sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"{\"type\":\"register\",\"endpoint_id\":\"old\",\"token\":\"t0ken-not-a-secret\",\"version\":0,\"max_concurrent\":2}\n",
        )
        .await
        .unwrap();
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(
        line.trim(),
        "{\"type\":\"rejected\",\"reason\":\"version_mismatch\",\"detail\":\"server speaks KiandProto 2; worker announced 0\"}"
    );
    // The connection is closed after the refusal — EOF follows.
    let mut trailing = String::new();
    let n = reader.read_line(&mut trailing).await.unwrap();
    assert_eq!(n, 0, "server must close after a refusal");
}

/// A register naming a static pin id → `duplicate_static` (C5).
#[tokio::test]
async fn duplicate_static_refused() {
    let (_listener, _sink, addr) =
        start_listener(vec!["vultr-builder".into()], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let err = connect_registered(endpoint(), info("vultr-builder", TOKEN), addr, shutdown)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        kian_endpoint::EndpointError::RegisterRejected {
            reason: kian_endpoint::RefusalReason::DuplicateStatic,
            ..
        }
    ));
}

/// A malformed endpoint_id → `id_conflict` (the shape-defence path).
#[tokio::test]
async fn malformed_id_refused_id_conflict() {
    let (_listener, _sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let err = connect_registered(endpoint(), info("   ", TOKEN), addr, shutdown)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        kian_endpoint::EndpointError::RegisterRejected {
            reason: kian_endpoint::RefusalReason::IdConflict,
            ..
        }
    ));
}

/// A non-register first frame → immediate close, no reply frame (P7).
#[tokio::test]
async fn non_register_first_frame_closed_silently() {
    let (_listener, _sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"{\"type\":\"hello\",\"v\":1,\"endpoint_id\":\"x\",\"max_concurrent\":1}\n")
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let n = tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
        .await
        .expect("closes within the deadline");
    assert_eq!(n.unwrap(), 0, "no reply frame; connection just closes");
}

/// Disabled listener → `disabled` (retryable on the worker side).
#[tokio::test]
async fn disabled_listener_refuses() {
    let (listener, _sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    listener.disable();
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let err = connect_registered(endpoint(), info("builder-1", TOKEN), addr, shutdown)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        kian_endpoint::EndpointError::RegisterRejected {
            reason: kian_endpoint::RefusalReason::Disabled,
            ..
        }
    ));
}

/// Replace-duplicate-id at the connection layer (M6): the second
/// authenticated registration for a live id closes the incumbent with a
/// server `bye`, audits `replaced`, and bumps the generation 1 → 2.
#[tokio::test]
async fn duplicate_dynamic_id_replaces_incumbent() {
    let (listener, sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx1, shutdown1) = tokio::sync::watch::channel(false);
    let conn1 = tokio::spawn(connect_registered(
        endpoint(),
        info("builder-1", TOKEN),
        addr,
        shutdown1,
    ));
    until("first join", || sink.online.lock().unwrap().len() == 1).await;

    // Second registration, same id.
    let (_tx2, shutdown2) = tokio::sync::watch::channel(false);
    let conn2 = tokio::spawn(connect_registered(
        endpoint(),
        info("builder-1", TOKEN),
        addr,
        shutdown2,
    ));
    until("replace recorded", || {
        sink.offline
            .lock()
            .unwrap()
            .iter()
            .any(|(id, _g, r)| id == "builder-1" && *r == LeaveReason::Replaced)
    })
    .await;
    until("generation 2", || {
        sink.online.lock().unwrap().iter().any(|(_, g)| *g == 2)
    })
    .await;

    // The incumbent connection observed a graceful end (server bye).
    let ended = tokio::time::timeout(Duration::from_secs(3), conn1)
        .await
        .expect("incumbent ends");
    assert!(ended.unwrap().is_ok(), "replaced incumbent ends gracefully");

    // The directory shows exactly one member, generation 2, online.
    let snap = listener.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap["builder-1"].generation, 2);
    let now = SystemClock.now_ms();
    assert_eq!(
        listener.member_state("builder-1", now),
        Some(MemberState::Online)
    );
    conn2.abort();
}

/// Worker `bye` marks the member offline through the sink (graceful
/// leave) — draining is transient, then offline.
#[tokio::test]
async fn worker_bye_marks_offline() {
    let (listener, sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
    let conn = tokio::spawn(connect_registered(
        endpoint(),
        info("bye-box", TOKEN),
        addr,
        shutdown,
    ));
    until("joined", || sink.online.lock().unwrap().len() == 1).await;
    // Trigger the worker-side graceful bye.
    shutdown_tx.send(true).unwrap();
    until("sink sees bye", || {
        sink.offline
            .lock()
            .unwrap()
            .iter()
            .any(|(id, _g, r)| id == "bye-box" && *r == LeaveReason::Bye)
    })
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(3), conn).await;
    let now = SystemClock.now_ms();
    assert_eq!(
        listener.member_state("bye-box", now),
        Some(MemberState::Offline)
    );
}

/// Per-peer rate bound (P9) with an injected clock: the 6th failed
/// attempt inside the window is closed without reading a frame.
#[tokio::test]
async fn rate_bound_closes_sixth_attempt_without_reading() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (_listener, _sink, addr) = start_listener(vec![], clock.clone()).await;
    for attempt in 0..5 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                b"{\"type\":\"register\",\"endpoint_id\":\"bad\",\"token\":\"wrong\",\"version\":2,\"max_concurrent\":1}\n",
            )
            .await
            .unwrap();
        let mut line = String::new();
        let n = BufReader::new(&mut stream)
            .read_line(&mut line)
            .await
            .unwrap();
        assert_eq!(n, line.len(), "attempt {attempt}: a typed refusal arrives");
        assert!(line.contains("bad_token"), "attempt {attempt}: {line}");
    }
    // 6th attempt from the same peer: closed without reading a frame.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"{\"type\":\"register\",\"endpoint_id\":\"bad\",\"token\":\"wrong\",\"version\":2,\"max_concurrent\":1}\n",
        )
        .await
        .unwrap();
    let mut line = String::new();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        BufReader::new(&mut stream).read_line(&mut line),
    )
    .await
    .expect("the rate-limited connection closes promptly");
    // No frame is read or answered: a clean EOF, or the RST the kernel
    // emits when the server drops the socket with the unread register
    // line still queued (macOS behavior).
    match outcome {
        Ok(0) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("expected close-without-reply, got {other:?} (line {line:?})"),
    }

    // After the window passes, the peer is unblocked again.
    clock.advance_by(61_000);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"{\"type\":\"register\",\"endpoint_id\":\"bad\",\"token\":\"wrong\",\"version\":2,\"max_concurrent\":1}\n",
        )
        .await
        .unwrap();
    let mut line = String::new();
    let n = BufReader::new(&mut stream)
        .read_line(&mut line)
        .await
        .unwrap();
    assert!(n > 0 && line.contains("bad_token"), "window reset: {line}");
}

/// An invalid description is dropped with an audit line; the
/// registration still succeeds (P11).
#[tokio::test]
async fn invalid_description_dropped_registration_kept() {
    let (listener, sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let register_info = RegisterInfo {
        endpoint_id: "builder-1".into(),
        description: Some("ctrl\u{7}bad".into()),
        token: RegisterToken::new(TOKEN.into()),
    };
    let conn = tokio::spawn(connect_registered(
        endpoint(),
        register_info,
        addr,
        shutdown,
    ));
    until("joined despite bad description", || {
        !sink.online.lock().unwrap().is_empty()
    })
    .await;
    let snap = listener.snapshot();
    assert_eq!(snap["builder-1"].description, None, "description dropped");
    conn.abort();
}

/// Shutdown: live members receive a server `bye`; the worker observes a
/// graceful end and the member goes offline (A14).
#[tokio::test]
async fn listener_shutdown_byes_live_members() {
    let (listener, sink, addr) = start_listener(vec![], Arc::new(SystemClock)).await;
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let conn = tokio::spawn(connect_registered(
        endpoint(),
        info("drain-box", TOKEN),
        addr,
        shutdown,
    ));
    until("joined", || !sink.online.lock().unwrap().is_empty()).await;
    listener.shutdown().await;
    let ended = tokio::time::timeout(Duration::from_secs(3), conn)
        .await
        .expect("worker ends after server bye");
    assert!(ended.unwrap().is_ok());
    until("sink sees shutdown leave", || {
        sink.offline
            .lock()
            .unwrap()
            .iter()
            .any(|(id, _g, r)| id == "drain-box" && *r == LeaveReason::Shutdown)
    })
    .await;
}

/// The welcome carries the server-directed heartbeat cadence
/// `max(1, offline_after_ms / 3)` (P6).
#[tokio::test]
async fn welcome_carries_directed_heartbeat_cadence() {
    let config = RegistrationConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        worker_token(),
        30_000, // → 10_000 ms cadence
    );
    let sink = Arc::new(RecSink::default());
    let listener = Arc::new(RegistrationListener::new(
        config,
        Arc::new(SystemClock),
        sink.clone(),
        WorkerDirectory::new(),
    ));
    let l = listener.clone();
    tokio::spawn(async move {
        let _ = l.run().await;
    });
    until("bound", || listener.local_addr().is_some()).await;
    let addr = listener.local_addr().unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"{\"type\":\"register\",\"endpoint_id\":\"cadence\",\"token\":\"t0ken-not-a-secret\",\"version\":2,\"max_concurrent\":1}\n",
        )
        .await
        .unwrap();
    let mut line = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut line)
        .await
        .unwrap();
    assert_eq!(
        line.trim(),
        "{\"type\":\"welcome\",\"v\":2,\"heartbeat_ms\":10000}"
    );
}
