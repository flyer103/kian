//! `Endpoint` — the KiandProto endpoint-side connection loop
//! (card C12, DESIGN §4.3).
//!
//! Accepts commands over a stream transport, executes them via a
//! configurable [`ExecBackend`] under a concurrency cap, returns results
//! and emits heartbeats. The endpoint keeps **zero session state**
//! (brain–hand separation): a connection holds only its in-flight
//! command ids; when the connection ends, nothing about it survives —
//! reconnects start from a clean handshake. Session logs live in Kian's
//! EventStore; delivery retries across disconnects are **server-side**
//! (Phase 2, `kian-exec`) — the endpoint drops undeliverable results with
//! a warning.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use uuid::Uuid;

use kian_core::seams::{ExecBackend, ExecCommand, ExecResult};

use crate::proto::{CommandError, CommandId, EndpointFrame, PROTOCOL_VERSION, ServerFrame};

/// Concurrency cap applied when [`Endpoint::new`] is not followed by
/// [`Endpoint::with_max_concurrent`].
pub const DEFAULT_MAX_CONCURRENT: usize = 8;

/// Default heartbeat interval when neither the builder nor the server's
/// welcome message overrides it.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);

/// Endpoint-side failures. Transport loss is *expected* (intermittent
/// endpoint presence is the norm, DESIGN §4.3) and never fatal to the
/// endpoint process — the accept loop simply waits for the next
/// connection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointError {
    #[error("transport closed")]
    TransportClosed,
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("protocol version mismatch: endpoint={endpoint} server={server}")]
    VersionMismatch { endpoint: u32, server: u32 },
}

/// What the endpoint announced in its [`EndpointFrame::Hello`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloInfo {
    pub endpoint_id: String,
    pub max_concurrent: u32,
}

/// One heartbeat the endpoint successfully sent, as seen by an optional
/// observer installed via [`Endpoint::with_heartbeat_tap`] (additive
/// builder hook, card C22): a hosting daemon logs liveness at its own
/// level without the library choosing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatInfo {
    pub endpoint_id: String,
    pub ts_ms: u64,
    pub inflight: u32,
}

/// Observer invoked after each heartbeat frame has been sent — both the
/// periodic emitter's beats and ping answers. Called inline on the
/// connection task: it must not block.
pub type HeartbeatTap = Arc<dyn Fn(HeartbeatInfo) + Send + Sync>;

/// An execution endpoint: identity + executor + concurrency cap.
///
/// Cheap to share: `serve`, `embed` and every spawned connection hold the
/// same `Arc<Endpoint>`, so the semaphore caps concurrency **across all
/// connections**, not per connection.
pub struct Endpoint {
    endpoint_id: String,
    executor: Arc<dyn ExecBackend>,
    max_concurrent: usize,
    heartbeat: Duration,
    heartbeat_tap: Option<HeartbeatTap>,
    semaphore: Arc<Semaphore>,
}

impl Endpoint {
    /// New endpoint with [`DEFAULT_MAX_CONCURRENT`] and
    /// [`DEFAULT_HEARTBEAT`].
    pub fn new(endpoint_id: impl Into<String>, executor: Arc<dyn ExecBackend>) -> Self {
        Self {
            endpoint_id: endpoint_id.into(),
            executor,
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            heartbeat: DEFAULT_HEARTBEAT,
            heartbeat_tap: None,
            semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT)),
        }
    }

    /// Cap concurrent executions (across all connections). Must be called
    /// before serving/embedding; must be at least 1.
    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        assert!(
            max_concurrent >= 1,
            "max_concurrent must be at least 1 (0 would deadlock every command)"
        );
        self.max_concurrent = max_concurrent;
        self.semaphore = Arc::new(Semaphore::new(max_concurrent));
        self
    }

    /// Default heartbeat interval; the server's welcome may override it
    /// per connection.
    pub fn with_heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// Install a heartbeat observer (additive builder hook, card C22).
    /// Called for every heartbeat frame successfully sent — both the
    /// periodic emitter's beats and ping answers.
    pub fn with_heartbeat_tap(mut self, tap: HeartbeatTap) -> Self {
        self.heartbeat_tap = Some(tap);
        self
    }

    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }

    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// Serve the stream transport: accept TCP connections until the
    /// listener fails. Each connection is an independent
    /// [`Endpoint::run_connection`]; a peer that disconnects and comes
    /// back is handled by simply accepting again — the endpoint holds no
    /// per-connection state to restore (zero session state).
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            tracing::debug!(%peer, endpoint = %self.endpoint_id, "connection accepted");
            let (inbound_tx, inbound) = mpsc::channel(64);
            let (outbound, outbound_rx) = mpsc::channel(64);
            crate::tcp::attach(stream, inbound_tx, outbound_rx);
            tokio::spawn(self.clone().run_connection(inbound, outbound));
        }
    }

    /// Embed: run one connection loop against an in-process peer and
    /// return the server-side handle. This is the "SDK embedded in any
    /// process" access form (the peer will be `kian-exec`'s client half
    /// in Phase 2; today it doubles as the in-process mock for tests).
    pub fn embed(self: &Arc<Self>) -> ServerPeer {
        let (inbound_tx, inbound) = mpsc::channel(64);
        let (outbound, outbound_rx) = mpsc::channel(64);
        tokio::spawn(self.clone().run_connection(inbound, outbound));
        ServerPeer::new(inbound_tx, outbound_rx)
    }

    /// One framed connection: handshake → heartbeat emitter + command
    /// dispatch until the peer goes away or says bye. Transport-agnostic
    /// extension point: any transport that can produce inbound
    /// [`ServerFrame`]s and accept outbound [`EndpointFrame`]s lands here
    /// (TCP via [`crate::tcp`], in-process via [`Endpoint::embed`]).
    pub async fn run_connection(
        self: Arc<Self>,
        mut inbound: mpsc::Receiver<ServerFrame>,
        outbound: mpsc::Sender<EndpointFrame>,
    ) -> Result<(), EndpointError> {
        // Handshake: announce ourselves, then require a matching welcome.
        outbound
            .send(EndpointFrame::Hello {
                v: PROTOCOL_VERSION,
                endpoint_id: self.endpoint_id.clone(),
                max_concurrent: self.max_concurrent as u32,
            })
            .await
            .map_err(|_| EndpointError::TransportClosed)?;
        let heartbeat = match inbound.recv().await {
            Some(ServerFrame::Welcome { v, heartbeat_ms }) => {
                if v != PROTOCOL_VERSION {
                    return Err(EndpointError::VersionMismatch {
                        endpoint: PROTOCOL_VERSION,
                        server: v,
                    });
                }
                heartbeat_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.heartbeat)
            }
            Some(other) => {
                return Err(EndpointError::Protocol(format!(
                    "first frame must be welcome, got {other:?}"
                )));
            }
            None => return Err(EndpointError::TransportClosed),
        };

        // Per-connection in-flight counter (reported in heartbeats).
        let inflight = Arc::new(AtomicUsize::new(0));
        // Shutdown flag for the heartbeat emitter.
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        {
            let outbound = outbound.clone();
            let endpoint_id = self.endpoint_id.clone();
            let inflight = inflight.clone();
            let tap = self.heartbeat_tap.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(heartbeat);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let info = HeartbeatInfo {
                                endpoint_id: endpoint_id.clone(),
                                ts_ms: now_ms(),
                                inflight: inflight.load(Ordering::SeqCst) as u32,
                            };
                            let frame = EndpointFrame::Heartbeat {
                                endpoint_id: info.endpoint_id.clone(),
                                ts_ms: info.ts_ms,
                                inflight: info.inflight,
                            };
                            if outbound.send(frame).await.is_err() {
                                break; // peer gone
                            }
                            if let Some(tap) = tap.as_ref() {
                                tap(info);
                            }
                        }
                        _ = shutdown_rx.changed() => break,
                    }
                }
            });
        }

        let result = loop {
            match inbound.recv().await {
                Some(ServerFrame::Command { id, cmd }) => {
                    self.spawn_execution(id, cmd, &outbound, &inflight).await;
                }
                Some(ServerFrame::Ping) => {
                    let info = HeartbeatInfo {
                        endpoint_id: self.endpoint_id.clone(),
                        ts_ms: now_ms(),
                        inflight: inflight.load(Ordering::SeqCst) as u32,
                    };
                    let frame = EndpointFrame::Heartbeat {
                        endpoint_id: info.endpoint_id.clone(),
                        ts_ms: info.ts_ms,
                        inflight: info.inflight,
                    };
                    if outbound.send(frame).await.is_err() {
                        break Err(EndpointError::TransportClosed);
                    }
                    if let Some(tap) = self.heartbeat_tap.as_ref() {
                        tap(info);
                    }
                }
                Some(ServerFrame::Welcome { .. }) => {
                    break Err(EndpointError::Protocol("duplicate welcome".into()));
                }
                Some(ServerFrame::Bye) => break Ok(()),
                None => break Ok(()), // peer went away — expected, not an error
            }
        };
        let _ = shutdown_tx.send(true);
        result
    }

    /// Acquire a concurrency permit and spawn the execution of one
    /// command. The permit is held until the result has been delivered
    /// (or dropped with a warning after connection loss).
    async fn spawn_execution(
        self: &Arc<Self>,
        id: CommandId,
        cmd: ExecCommand,
        outbound: &mpsc::Sender<EndpointFrame>,
        inflight: &Arc<AtomicUsize>,
    ) {
        // Awaiting here applies backpressure: while saturated, this
        // connection stops reading further commands until a permit frees.
        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return, // semaphore never closed; unreachable
        };
        let executor = self.executor.clone();
        let outbound = outbound.clone();
        let inflight = inflight.clone();
        inflight.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let frame = match executor.exec(cmd).await {
                Ok(result) => EndpointFrame::command_ok(id, result),
                Err(error) => EndpointFrame::command_failed(id, error.into()),
            };
            inflight.fetch_sub(1, Ordering::SeqCst);
            if outbound.send(frame).await.is_err() {
                tracing::warn!(
                    command_id = %id,
                    "connection lost before result was delivered; delivery retries are server-side (Phase 2)"
                );
            }
            drop(permit); // hold the permit through delivery
        });
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// The server-side half of one connection: send [`ServerFrame`]s, receive
/// [`EndpointFrame`]s. In-process mock of the Phase-2 `kian-exec` client;
/// also the reference client for the TCP transport ([`crate::tcp::connect`]).
#[derive(Debug)]
pub struct ServerPeer {
    outbound: mpsc::Sender<ServerFrame>,
    inbound: mpsc::Receiver<EndpointFrame>,
}

impl ServerPeer {
    pub(crate) fn new(
        outbound: mpsc::Sender<ServerFrame>,
        inbound: mpsc::Receiver<EndpointFrame>,
    ) -> Self {
        Self { outbound, inbound }
    }

    /// Complete the handshake: read the endpoint's hello, answer welcome.
    /// `heartbeat` optionally directs the endpoint's heartbeat cadence
    /// for this connection.
    pub async fn handshake(
        &mut self,
        heartbeat: Option<Duration>,
    ) -> Result<HelloInfo, EndpointError> {
        match self.inbound.recv().await {
            Some(EndpointFrame::Hello {
                v,
                endpoint_id,
                max_concurrent,
            }) => {
                if v != PROTOCOL_VERSION {
                    return Err(EndpointError::VersionMismatch {
                        endpoint: v,
                        server: PROTOCOL_VERSION,
                    });
                }
                self.send(ServerFrame::Welcome {
                    v: PROTOCOL_VERSION,
                    heartbeat_ms: heartbeat.map(|d| d.as_millis() as u64),
                })
                .await?;
                Ok(HelloInfo {
                    endpoint_id,
                    max_concurrent,
                })
            }
            Some(other) => Err(EndpointError::Protocol(format!(
                "first frame must be hello, got {other:?}"
            ))),
            None => Err(EndpointError::TransportClosed),
        }
    }

    /// Send one raw frame (handshake escape hatch for adversarial tests).
    pub async fn send(&mut self, frame: ServerFrame) -> Result<(), EndpointError> {
        self.outbound
            .send(frame)
            .await
            .map_err(|_| EndpointError::TransportClosed)
    }

    /// Execute one command and await its result (heartbeats flow past).
    pub async fn execute(
        &mut self,
        cmd: ExecCommand,
    ) -> Result<Result<ExecResult, CommandError>, EndpointError> {
        let id = Uuid::new_v4();
        self.send(ServerFrame::Command { id, cmd }).await?;
        while let Some(frame) = self.inbound.recv().await {
            if frame.command_id() == Some(id) {
                return match frame.outcome() {
                    Ok(result) => Ok(Ok(result.clone())),
                    Err(error) => Ok(Err(error.clone())),
                };
            }
            // Heartbeats and results of other commands flow past.
        }
        Err(EndpointError::TransportClosed)
    }

    /// Next frame from the endpoint, whatever it is; `None` once the
    /// endpoint closed the connection.
    pub async fn next_frame(&mut self) -> Option<EndpointFrame> {
        self.inbound.recv().await
    }

    /// Next heartbeat as `(endpoint_id, inflight)`.
    pub async fn next_heartbeat(&mut self) -> Option<(String, u32)> {
        loop {
            match self.inbound.recv().await? {
                EndpointFrame::Heartbeat {
                    endpoint_id,
                    inflight,
                    ..
                } => return Some((endpoint_id, inflight)),
                _ => continue,
            }
        }
    }

    /// Graceful close: say bye. The endpoint closes its side; subsequent
    /// [`ServerPeer::next_frame`] calls return `None` once it has.
    pub async fn bye(&mut self) -> Result<(), EndpointError> {
        self.send(ServerFrame::Bye).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};
    use tokio::sync::watch;

    use super::{DEFAULT_MAX_CONCURRENT, Endpoint, HeartbeatInfo, HeartbeatTap};
    use crate::endpoint::ServerPeer;
    use crate::proto::{EndpointFrame, ErrorCode, PROTOCOL_VERSION, ServerFrame};

    fn cmd(argv: &[&str]) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        }
    }

    fn ok_result(payload: &str) -> ExecResult {
        ExecResult {
            exit_code: Some(0),
            stdout: payload.into(),
            stderr: String::new(),
            timed_out: false,
        }
    }

    /// Executor that gates every execution on a shared watch flag and
    /// tracks concurrent + peak executions.
    #[derive(Clone)]
    struct GateExecutor {
        running: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        gate: watch::Receiver<bool>,
    }

    impl ExecBackend for GateExecutor {
        fn exec(&self, _cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
            let running = self.running.clone();
            let peak = self.peak.clone();
            let mut gate = self.gate.clone();
            Box::pin(async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Returns immediately when the gate is already open.
                let _ = gate.wait_for(|open| *open).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(ok_result("gated"))
            })
        }
    }

    #[derive(Clone)]
    struct FailExecutor;

    impl ExecBackend for FailExecutor {
        fn exec(&self, _cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
            Box::pin(async { Err(ExecError::Timeout) })
        }
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

    async fn assert_eventually_closed(peer: &mut ServerPeer) {
        let mut frames = 0;
        loop {
            if peer.next_frame().await.is_none() {
                return;
            }
            frames += 1;
            assert!(frames < 100, "peer still receiving frames after bye");
        }
    }

    /// The in-process pair test (card acceptance): connect (handshake),
    /// observe a heartbeat, execute a command, receive the result, close
    /// with bye.
    #[tokio::test]
    async fn in_process_pair_connect_heartbeat_execute_result() {
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (_gate_tx, gate_rx) = watch::channel(true); // gate open: exec runs through
        let executor = GateExecutor {
            running,
            peak,
            gate: gate_rx,
        };
        let endpoint = Arc::new(Endpoint::new("dev-box", Arc::new(executor)));

        let mut peer = endpoint.embed();
        let hello = peer
            .handshake(Some(Duration::from_millis(50)))
            .await
            .unwrap();
        assert_eq!(hello.endpoint_id, "dev-box");
        assert_eq!(hello.max_concurrent, DEFAULT_MAX_CONCURRENT as u32);

        // The heartbeat emitter ticks immediately after the handshake.
        let (endpoint_id, inflight) = peer.next_heartbeat().await.unwrap();
        assert_eq!(endpoint_id, "dev-box");
        assert_eq!(inflight, 0);

        let outcome = peer.execute(cmd(&["echo", "hi"])).await.unwrap();
        assert_eq!(outcome.unwrap(), ok_result("gated"));

        peer.bye().await.unwrap();
        assert_eventually_closed(&mut peer).await;
    }

    /// Disconnect (peer dropped mid-command) and reconnect: the endpoint
    /// is immediately usable, holds no poisoned state, and the orphaned
    /// command's permit is still respected (execution belongs to the
    /// executor, not the connection).
    #[tokio::test]
    async fn disconnect_reconnect_resumes_with_zero_state() {
        let (gate_tx, gate_rx) = watch::channel(false); // first command blocks
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let executor = GateExecutor {
            running: running.clone(),
            peak: peak.clone(),
            gate: gate_rx,
        };
        let endpoint =
            Arc::new(Endpoint::new("dev-box", Arc::new(executor)).with_max_concurrent(2));

        let mut peer = endpoint.embed();
        peer.handshake(None).await.unwrap();
        let orphan = tokio::spawn(async move { peer.execute(cmd(&["sleep", "10"])).await });
        until("first command starts executing", || {
            running.load(Ordering::SeqCst) == 1
        })
        .await;
        orphan.abort(); // drops the peer → connection closes mid-flight

        // Reconnect on the same endpoint: clean handshake, command works.
        let mut peer2 = endpoint.embed();
        peer2.handshake(None).await.unwrap();
        let second = tokio::spawn(async move { peer2.execute(cmd(&["echo", "again"])).await });
        until("second command starts executing", || {
            running.load(Ordering::SeqCst) == 2
        })
        .await;
        gate_tx.send(true).unwrap(); // release both gated executions

        let outcome = second.await.unwrap().unwrap().unwrap();
        assert_eq!(outcome.stdout, "gated");
        assert_eq!(peak.load(Ordering::SeqCst), 2); // cap held across the reconnect
    }

    /// Four parallel commands against a cap of two: exactly two execute
    /// concurrently (peak == 2), the rest wait for permits, all results
    /// come back.
    #[tokio::test]
    async fn concurrency_cap_enforced_under_parallel_commands() {
        let (gate_tx, gate_rx) = watch::channel(false);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let executor = GateExecutor {
            running: running.clone(),
            peak: peak.clone(),
            gate: gate_rx,
        };
        let endpoint = Arc::new(Endpoint::new("capped", Arc::new(executor)).with_max_concurrent(2));

        // One connection per command: the cap must hold across connections.
        let mut peers: Vec<_> = (0..4).map(|_| endpoint.embed()).collect();
        for peer in &mut peers {
            peer.handshake(None).await.unwrap();
        }
        let handles: Vec<_> = peers
            .into_iter()
            .map(|mut peer| tokio::spawn(async move { peer.execute(cmd(&["work"])).await }))
            .collect();

        until("two commands start executing", || {
            running.load(Ordering::SeqCst) == 2
        })
        .await;
        // Give any over-cap execution ample time to (wrongly) start.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            running.load(Ordering::SeqCst),
            2,
            "cap must hold: exactly two concurrent executions"
        );

        gate_tx.send(true).unwrap();
        for handle in handles {
            let outcome = handle.await.unwrap().unwrap().unwrap();
            assert_eq!(outcome.stdout, "gated");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    /// Executor failures flow back honestly as command errors — never as
    /// silent successes (constraint 7 spirit).
    #[tokio::test]
    async fn executor_error_flows_back_as_command_error() {
        let endpoint = Arc::new(Endpoint::new("failing", Arc::new(FailExecutor)));
        let mut peer = endpoint.embed();
        peer.handshake(None).await.unwrap();
        let outcome = peer.execute(cmd(&["boom"])).await.unwrap();
        let error = outcome.expect_err("executor error must flow back");
        assert_eq!(error.code, ErrorCode::Timeout);
    }

    /// A welcome with a wrong protocol version closes the connection.
    #[tokio::test]
    async fn version_mismatch_closes_connection() {
        let endpoint = Arc::new(Endpoint::new("vcheck", Arc::new(FailExecutor)));
        let mut peer = endpoint.embed();
        match peer.next_frame().await.unwrap() {
            EndpointFrame::Hello { .. } => {}
            other => panic!("expected hello, got {other:?}"),
        }
        peer.send(ServerFrame::Welcome {
            v: PROTOCOL_VERSION + 1,
            heartbeat_ms: None,
        })
        .await
        .unwrap();
        assert!(peer.next_frame().await.is_none(), "endpoint must close");
    }

    /// The heartbeat tap (additive hook, C22) observes every beat the
    /// emitter successfully sends — identity, timestamp, in-flight count.
    #[tokio::test]
    async fn heartbeat_tap_observes_periodic_beats() {
        use std::sync::Mutex;

        let beats: Arc<Mutex<Vec<HeartbeatInfo>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = beats.clone();
        let tap: HeartbeatTap = Arc::new(move |info| seen.lock().unwrap().push(info));
        let endpoint =
            Arc::new(Endpoint::new("tap-box", Arc::new(FailExecutor)).with_heartbeat_tap(tap));
        let mut peer = endpoint.embed();
        peer.handshake(Some(Duration::from_millis(20)))
            .await
            .unwrap();
        until("two taps observed", || beats.lock().unwrap().len() >= 2).await;
        let observed = beats.lock().unwrap().clone();
        assert!(
            observed.iter().all(|b| b.endpoint_id == "tap-box"),
            "every beat carries the endpoint id"
        );
        assert!(
            observed.iter().all(|b| b.ts_ms > 0),
            "every beat carries a wall-clock timestamp"
        );
        assert!(
            observed.iter().all(|b| b.inflight == 0),
            "idle endpoint reports zero in-flight"
        );
        peer.bye().await.unwrap();
    }
}
