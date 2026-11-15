//! Worker-dial register mode (KiandProto v1, R12): the endpoint dials the
//! server's registration listener, sends `register`, and — on `welcome` —
//! serves the ordinary steady-state frames on that same connection
//! (DESIGN §4.3; the worker-dial connection *is* the route).
//!
//! Reconnect policy (spec §1.5): bounded backoff, initial 500 ms ×2 up to
//! a 30 s cap, unbounded attempts while the process lives. Transport
//! loss/refusal and the retryable refusals (`id_conflict`, `disabled`)
//! retry; the terminal refusals (`version_mismatch`, `bad_token`,
//! `duplicate_static`) return an error for the daemon to log once and exit
//! non-zero — never a tight retry loop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

use crate::endpoint::{Endpoint, EndpointError};
use crate::proto::{
    EndpointFrame, PROTOCOL_VERSION, RegisterToken, ServerFrame, decode_server_frame,
    encode_endpoint_frame,
};

/// Bounded reconnect backoff for the register loop. The loop itself is
/// unbounded (independent lifecycles are the point) — only the delay is
/// bounded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegisterBackoff {
    /// Delay before the first reconnect attempt.
    pub initial: Duration,
    /// Upper bound on every reconnect delay.
    pub max: Duration,
    /// Growth factor between consecutive attempts (>= 1.0).
    pub multiplier: f64,
}

impl Default for RegisterBackoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

impl RegisterBackoff {
    /// Advance one attempt: returns the delay to sleep now and updates
    /// the state for the next attempt.
    fn next_delay(&self, delay: &mut Duration) -> Duration {
        let current = *delay;
        *delay = delay.mul_f64(self.multiplier).min(self.max);
        current
    }
}

/// What a worker announces in its `register` frame.
#[derive(Debug, Clone)]
pub struct RegisterInfo {
    pub endpoint_id: String,
    pub description: Option<String>,
    pub token: RegisterToken,
    /// The worker's declared capabilities (v3, ADR-029). Validated and
    /// bounded by the caller (`kiand`); the wire form omits an empty
    /// list.
    pub capabilities: Vec<String>,
}

/// One register-mode connection: dial, register, then serve the steady
/// state until the connection ends (server bye, transport loss, or local
/// shutdown — the last one drains in-flight commands and sends a worker
/// `bye` first).
pub async fn connect_registered(
    endpoint: Arc<Endpoint>,
    info: RegisterInfo,
    addr: SocketAddr,
    shutdown: watch::Receiver<bool>,
) -> Result<(), EndpointError> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|_| EndpointError::TransportClosed)?;
    let (inbound_tx, inbound) = mpsc::channel::<ServerFrame>(64);
    let (outbound, outbound_rx) = mpsc::channel::<EndpointFrame>(64);
    let writer = spawn_pumps(stream, inbound_tx, outbound_rx);
    let result = register_session(endpoint, info, inbound, outbound, shutdown).await;
    // The session future has dropped every sender by now, so the writer
    // pump drains what is still queued (the shutdown `bye`), flushes the
    // write half, and only then completes — a queued bye can never be
    // lost to process exit.
    let _ = writer.await;
    result
}

/// Handshake plus steady state on an already-pumped connection. Every
/// exit path (including the early handshake refusals) drops the outbound
/// sender, which is what lets the writer pump finish above.
async fn register_session(
    endpoint: Arc<Endpoint>,
    info: RegisterInfo,
    mut inbound: mpsc::Receiver<ServerFrame>,
    outbound: mpsc::Sender<EndpointFrame>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), EndpointError> {
    // Handshake: register first, then a matching welcome or a typed
    // refusal.
    outbound
        .send(EndpointFrame::Register {
            endpoint_id: info.endpoint_id.clone(),
            description: info.description.clone(),
            token: info.token.clone(),
            version: PROTOCOL_VERSION,
            max_concurrent: endpoint.max_concurrent() as u32,
            capabilities: info.capabilities.clone(),
        })
        .await
        .map_err(|_| EndpointError::TransportClosed)?;
    let heartbeat = match inbound.recv().await {
        Some(ServerFrame::Welcome { v, heartbeat_ms }) => {
            // The server-directed cadence wins over the local default
            // (P6: `welcome.heartbeat_ms = max(1, offline_after_ms / 3)`).
            if v != PROTOCOL_VERSION {
                return Err(EndpointError::VersionMismatch {
                    endpoint: PROTOCOL_VERSION,
                    server: v,
                });
            }
            heartbeat_ms.map(Duration::from_millis).unwrap_or_else(|| {
                Duration::from_millis(crate::endpoint::DEFAULT_HEARTBEAT.as_millis() as u64)
            })
        }
        Some(ServerFrame::Rejected { reason, detail }) => {
            return Err(EndpointError::RegisterRejected { reason, detail });
        }
        Some(other) => {
            return Err(EndpointError::Protocol(format!(
                "first server frame must be welcome or rejected, got {other:?}"
            )));
        }
        None => return Err(EndpointError::TransportClosed),
    };
    endpoint
        .run_steady(inbound, outbound, heartbeat, Some(shutdown))
        .await
}

/// The register supervisor: keep a worker registered against one server
/// for the life of the process, reconnecting with bounded backoff.
/// Terminal refusals return the error (the daemon logs one actionable
/// line and exits); everything else retries.
pub async fn register_loop(
    endpoint: Arc<Endpoint>,
    info: RegisterInfo,
    addr: SocketAddr,
    backoff: RegisterBackoff,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EndpointError> {
    let mut delay = backoff.initial;
    loop {
        if *shutdown.borrow_and_update() {
            return Ok(());
        }
        let attempt =
            connect_registered(endpoint.clone(), info.clone(), addr, shutdown.clone()).await;
        match attempt {
            // Graceful end (local shutdown said bye, or the server said
            // bye and ours already drained): exit or reconnect.
            Ok(()) => {
                if *shutdown.borrow_and_update() {
                    return Ok(());
                }
            }
            Err(EndpointError::RegisterRejected { reason, detail })
                if matches!(
                    reason,
                    crate::proto::RefusalReason::VersionMismatch
                        | crate::proto::RefusalReason::BadToken
                        | crate::proto::RefusalReason::DuplicateStatic
                ) =>
            {
                // Terminal: reconnecting cannot fix a wrong token, a
                // version gap, or a static pin. Surface it — never a
                // tight retry loop.
                return Err(EndpointError::RegisterRejected { reason, detail });
            }
            Err(error @ EndpointError::VersionMismatch { .. }) => return Err(error),
            // Retryable: transport loss, id_conflict, disabled.
            Err(_) => {}
        }
        let sleep = backoff.next_delay(&mut delay);
        tokio::select! {
            _ = tokio::time::sleep(sleep) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}

/// Wire the dialed TCP stream to the framed channel ends (the worker-dial
/// mirror of [`crate::tcp::attach`]). Returns the writer pump's handle so
/// the caller can prove every queued frame reached the socket before the
/// connection is considered finished.
fn spawn_pumps(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<ServerFrame>,
    mut outbound_rx: mpsc::Receiver<EndpointFrame>,
) -> tokio::task::JoinHandle<()> {
    let (reader, mut writer) = stream.into_split();
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match decode_server_frame(&line) {
                Ok(frame) => {
                    if inbound_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed KiandProto line");
                }
            }
        }
    });
    tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let line = encode_endpoint_frame(&frame);
            if writer.write_all(line.as_bytes()).await.is_err()
                || writer.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
        // Drain complete: flush the write half so the final frame (the
        // graceful `bye`) reaches the peer even as the process exits.
        let _ = writer.shutdown().await;
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    // The `as _` trait imports come in via `use super::*` below; only
    // `BufReader` needs a local name.
    use tokio::io::BufReader;
    use tokio::net::TcpListener;

    use super::*;
    use crate::executor::LocalExecutor;
    use crate::proto::RefusalReason;

    const TOKEN: &str = "t0ken-not-a-secret";

    fn info() -> RegisterInfo {
        RegisterInfo {
            endpoint_id: "builder-1".into(),
            description: Some("linux builder".into()),
            token: RegisterToken::new(TOKEN.into()),
            capabilities: Vec::new(),
        }
    }

    fn fast_backoff() -> RegisterBackoff {
        RegisterBackoff {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(200),
            multiplier: 2.0,
        }
    }

    /// A mock kian: one accepted peer with line reads/writes.
    struct Peer {
        reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
        writer: tokio::net::tcp::OwnedWriteHalf,
    }

    impl Peer {
        async fn accept(listener: &TcpListener) -> (Peer, std::net::SocketAddr) {
            let (stream, peer) = listener.accept().await.unwrap();
            let (reader, writer) = stream.into_split();
            (
                Peer {
                    reader: BufReader::new(reader),
                    writer,
                },
                peer,
            )
        }

        async fn line(&mut self) -> String {
            let mut line = String::new();
            self.reader
                .read_line(&mut line)
                .await
                .expect("read a frame line");
            line.trim_end().to_string()
        }

        async fn send(&mut self, frame: &ServerFrame) {
            let mut line = serde_json::to_string(frame).unwrap();
            line.push('\n');
            self.writer.write_all(line.as_bytes()).await.unwrap();
        }
    }

    fn welcome(heartbeat_ms: u64) -> ServerFrame {
        ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(heartbeat_ms),
        }
    }

    fn rejected(reason: RefusalReason) -> ServerFrame {
        ServerFrame::Rejected {
            reason,
            detail: None,
        }
    }

    /// The backoff delay grows 500 ms ×2 and caps at 30 s.
    #[test]
    fn backoff_sequence_is_bounded() {
        let backoff = RegisterBackoff::default();
        let mut delay = backoff.initial;
        let delays: Vec<Duration> = (0..9).map(|_| backoff.next_delay(&mut delay)).collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ]
        );
    }

    /// Accept path: register → welcome → command executes over the
    /// registered connection → result correlates.
    #[tokio::test]
    async fn register_handshake_accepts_and_executes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(connect_registered(endpoint, info(), addr, shutdown_rx));
        let (mut peer, _) = Peer::accept(&listener).await;

        let register = peer.line().await;
        assert!(
            register.contains("\"type\":\"register\""),
            "was: {register}"
        );
        assert!(register.contains("\"endpoint_id\":\"builder-1\""));
        assert!(register.contains("\"description\":\"linux builder\""));
        assert!(register.contains("\"token\":\"t0ken-not-a-secret\""));
        assert!(register.contains(&format!("\"version\":{}", PROTOCOL_VERSION)));
        peer.send(&welcome(10_000)).await;

        let id = uuid::Uuid::new_v4();
        peer.send(&ServerFrame::Command {
            id,
            cmd: kian_core::seams::ExecCommand {
                argv: vec!["echo".into(), "registered".into()],
                env: Default::default(),
                cwd: None,
                timeout_ms: None,
            },
        })
        .await;
        // Heartbeats flow past; wait for the correlated result line.
        let mut result = String::new();
        for _ in 0..20 {
            let line = peer.line().await;
            if line.contains("\"type\":\"result\"") {
                result = line;
                break;
            }
        }
        assert!(
            result.contains("\"type\":\"result\""),
            "no result frame arrived"
        );
        assert!(result.contains(&id.to_string()));
        assert!(result.contains("registered"));

        // Close from the server side: the connection ends gracefully.
        peer.send(&ServerFrame::Bye).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
    }

    /// A typed refusal surfaces as `RegisterRejected` with the reason.
    #[tokio::test]
    async fn register_rejection_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(connect_registered(endpoint, info(), addr, shutdown_rx));
        let (mut peer, _) = Peer::accept(&listener).await;
        let _ = peer.line().await; // register
        peer.send(&rejected(RefusalReason::BadToken)).await;

        let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("connect resolves after a refusal")
            .unwrap()
            .unwrap_err();
        assert_eq!(
            outcome,
            EndpointError::RegisterRejected {
                reason: RefusalReason::BadToken,
                detail: None,
            }
        );
    }

    /// A v0 welcome is a typed version mismatch, never a silent accept.
    #[tokio::test]
    async fn welcome_version_mismatch_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(connect_registered(endpoint, info(), addr, shutdown_rx));
        let (mut peer, _) = Peer::accept(&listener).await;
        let _ = peer.line().await; // register
        peer.send(&ServerFrame::Welcome {
            v: 0,
            heartbeat_ms: None,
        })
        .await;

        let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("connect resolves on mismatch")
            .unwrap()
            .unwrap_err();
        assert_eq!(
            outcome,
            EndpointError::VersionMismatch {
                endpoint: PROTOCOL_VERSION,
                server: 0,
            }
        );
    }

    /// Transport loss reconnects with backoff; local shutdown produces a
    /// graceful worker `bye` and a clean exit.
    #[tokio::test]
    async fn register_loop_reconnects_and_byes_on_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(register_loop(
            endpoint,
            info(),
            addr,
            fast_backoff(),
            shutdown_rx,
        ));

        // First connection: welcome, then hard transport loss.
        let (mut peer, _) = Peer::accept(&listener).await;
        assert!(peer.line().await.contains("\"type\":\"register\""));
        peer.send(&welcome(30_000)).await;
        drop(peer); // kill -9 the connection

        // The supervisor reconnects.
        let (mut peer2, _) = Peer::accept(&listener).await;
        assert!(peer2.line().await.contains("\"type\":\"register\""));
        peer2.send(&welcome(30_000)).await;

        // SIGTERM-equivalent: drain (nothing in flight) + worker bye.
        shutdown_tx.send(true).unwrap();
        let bye = tokio::time::timeout(Duration::from_secs(5), peer2.line())
            .await
            .expect("worker sends bye promptly");
        assert_eq!(bye, "{\"type\":\"bye\"}");

        let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("loop exits after shutdown")
            .unwrap();
        assert!(outcome.is_ok());
    }

    /// A terminal refusal (`bad_token`) exits the supervisor without
    /// retrying — exactly one connection, fast.
    #[tokio::test]
    async fn terminal_refusal_exits_without_retry_loop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(register_loop(
            endpoint,
            info(),
            addr,
            fast_backoff(),
            shutdown_rx,
        ));
        let (mut peer, _) = Peer::accept(&listener).await;
        assert!(peer.line().await.contains("\"type\":\"register\""));
        peer.send(&rejected(RefusalReason::BadToken)).await;

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("terminal refusal exits promptly")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            outcome,
            EndpointError::RegisterRejected {
                reason: RefusalReason::BadToken,
                ..
            }
        ));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "no tight retry loop: exited in {:?}",
            started.elapsed()
        );
        // No second connection was attempted.
        let second = tokio::time::timeout(Duration::from_millis(300), listener.accept()).await;
        assert!(
            second.is_err(),
            "server must not see a retry after bad_token"
        );
    }

    /// A retryable refusal (`disabled`) keeps trying with backoff and
    /// succeeds once the server recovers.
    #[tokio::test]
    async fn retryable_refusal_backs_off_and_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(Endpoint::new("builder-1", Arc::new(LocalExecutor)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(register_loop(
            endpoint,
            info(),
            addr,
            fast_backoff(),
            shutdown_rx,
        ));

        let (mut peer, _) = Peer::accept(&listener).await;
        assert!(peer.line().await.contains("\"type\":\"register\""));
        peer.send(&rejected(RefusalReason::Disabled)).await;

        // The retry lands; this time welcome.
        let (mut peer2, _) = Peer::accept(&listener).await;
        assert!(peer2.line().await.contains("\"type\":\"register\""));
        peer2.send(&welcome(30_000)).await;

        shutdown_tx.send(true).unwrap();
        let bye = tokio::time::timeout(Duration::from_secs(5), peer2.line())
            .await
            .expect("worker sends bye promptly");
        assert_eq!(bye, "{\"type\":\"bye\"}");
        let _ = tokio::time::timeout(Duration::from_secs(5), worker).await;
    }

    /// The worker honours the server-directed heartbeat cadence from the
    /// welcome over its own default (P6).
    #[tokio::test]
    async fn server_directed_heartbeat_cadence_wins() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = Arc::new(
            Endpoint::new("builder-1", Arc::new(LocalExecutor))
                .with_heartbeat(Duration::from_secs(30)),
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(connect_registered(endpoint, info(), addr, shutdown_rx));
        let (mut peer, _) = Peer::accept(&listener).await;
        let _ = peer.line().await; // register
        peer.send(&welcome(50)).await; // 50 ms cadence

        let first = tokio::time::timeout(Duration::from_secs(2), peer.line())
            .await
            .expect("a heartbeat arrives within the server cadence");
        assert!(first.contains("\"type\":\"heartbeat\""), "was: {first}");
        let second = tokio::time::timeout(Duration::from_secs(2), peer.line())
            .await
            .expect("heartbeats keep flowing");
        assert!(second.contains("\"type\":\"heartbeat\""));
        worker.abort();
    }
}
