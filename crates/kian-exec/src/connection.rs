//! `ConnectionManager` — the KiandProto server-side connection half
//! (DESIGN §4.3).
//!
//! Dials a configured endpoint address (the endpoint listens — matches
//! `kian_endpoints`' `Endpoint::serve`), performs the hello/welcome
//! handshake with version validation, tracks per-endpoint heartbeat
//! freshness (`last_seen`, `inflight`), pipelines `Command{id, cmd}`
//! dispatch and correlates `CommandResult` frames by command id, probes
//! liveness with ping, says bye on shutdown, and reconnects with bounded
//! backoff when the transport drops.
//!
//! This slice is deliberately **in-memory**: a transport loss fails the
//! in-flight command futures with a transport error (never a fake result —
//! honesty over completion rate). Durable offline queueing is the next
//! slice.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use kian_core::seams::{ExecCommand, ExecResult};

use crate::error::ConnectionError;
use crate::proto::{
    CommandError, CommandId, EndpointFrame, PROTOCOL_VERSION, ServerFrame, decode_endpoint_frame,
    encode_server_frame,
};

/// Frame channel capacity between the pumps and the connection loop
/// (mirrors the endpoint side's channel sizing).
const FRAME_CHANNEL_CAP: usize = 64;

/// What a dispatched command's oneshot carries: the correlated outcome, or
/// the connection failure that prevented one.
type DispatchOutcome = Result<Result<ExecResult, CommandError>, ConnectionError>;

/// Bounded reconnect backoff: delay grows geometrically up to `max` and
/// stays there. The reconnect loop itself is unbounded (intermittent
/// endpoint presence is the norm) — only the delay is bounded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffConfig {
    /// Delay before the first reconnect attempt.
    pub initial: Duration,
    /// Upper bound on every reconnect delay.
    pub max: Duration,
    /// Growth factor between consecutive attempts (>= 1.0).
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(5),
            multiplier: 2.0,
        }
    }
}

impl BackoffConfig {
    /// Advance one attempt: returns the delay to sleep now and updates the
    /// state for the next attempt.
    fn next_delay(&self, delay: &mut Duration) -> Duration {
        let current = *delay;
        *delay = delay.mul_f64(self.multiplier).min(self.max);
        current
    }
}

/// Configuration of one managed endpoint connection.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionConfig {
    /// Endpoint address to dial (the endpoint listens).
    pub addr: SocketAddr,
    /// Optional server-directed heartbeat cadence sent in the welcome.
    pub heartbeat: Option<Duration>,
    /// Reconnect backoff.
    pub backoff: BackoffConfig,
}

impl ConnectionConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            heartbeat: None,
            backoff: BackoffConfig::default(),
        }
    }

    /// Direct the endpoint's heartbeat cadence for this connection.
    pub fn with_heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    pub fn with_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = backoff;
        self
    }
}

/// Live view of the managed endpoint, projected from hello + heartbeats.
/// Exposed via [`ConnectionManager::status`] and a watch channel for
/// subscribers (the watchdog consumes the same projection).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointStatus {
    /// Endpoint id announced in the hello (empty before the first
    /// handshake).
    pub endpoint_id: String,
    /// Concurrency cap announced in the hello (0 before the first
    /// handshake). The manager never keeps more than this many commands
    /// in flight toward the endpoint.
    pub max_concurrent: u32,
    /// Whether a dialed connection is currently in its steady state.
    pub connected: bool,
    /// `ts_ms` of the most recent heartbeat (endpoint-reported clock; 0
    /// before the first heartbeat). Monotone per connection.
    pub last_seen_ms: u64,
    /// In-flight command count as reported by the latest heartbeat.
    pub inflight: u32,
}

/// Handle to one live connection, published to dispatchers via a watch
/// channel. Cloning is cheap; the writer fails once the transport drops.
#[derive(Clone)]
struct ConnHandle {
    writer: mpsc::Sender<ServerFrame>,
    /// Caps in-flight commands at the endpoint's announced
    /// `max_concurrent` (manager-side backpressure; the endpoint enforces
    /// its own cap too).
    permits: Arc<Semaphore>,
}

struct Shared {
    config: ConnectionConfig,
    status: watch::Sender<EndpointStatus>,
    conn: watch::Sender<Option<ConnHandle>>,
    pending: Mutex<HashMap<CommandId, oneshot::Sender<DispatchOutcome>>>,
    ping_waiters: Mutex<Vec<oneshot::Sender<()>>>,
    /// Set when reconnecting can never succeed (version mismatch). Stops
    /// the supervisor; every dispatch reports it.
    terminal: Mutex<Option<ConnectionError>>,
    shutdown: watch::Sender<bool>,
}

impl Shared {
    fn set_connected(&self, connected: bool) {
        self.status
            .send_modify(|status| status.connected = connected);
    }

    /// Resolve every waiting command future with `error`. Called when a
    /// connection dies (nothing in flight survives it) and at shutdown.
    fn fail_pending(&self, error: ConnectionError) {
        let pending: Vec<_> = self.pending.lock().unwrap().drain().collect();
        for (_, sender) in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }

    /// Wake (success) every ping waiter — a heartbeat arrived.
    fn wake_pings(&self) {
        let waiters: Vec<_> = self.ping_waiters.lock().unwrap().drain(..).collect();
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }

    /// Drop every ping waiter — the connection they probed is gone, so
    /// their receivers error out with a transport failure.
    fn fail_ping_waiters(&self) {
        self.ping_waiters.lock().unwrap().clear();
    }
}

/// The server half of a KiandProto pairing: dial one endpoint, keep it
/// alive, dispatch commands onto it.
///
/// Lifecycle: [`start`](ConnectionManager::start) spawns the supervisor
/// (dial → handshake → steady state → on loss: fail in-flight, bounded
/// backoff, redial); [`shutdown`](ConnectionManager::shutdown) says bye
/// and joins it. Dropping the manager without shutting it down leaves the
/// supervisor running (it owns its own `Arc` to the shared state) — always
/// shut down explicitly.
pub struct ConnectionManager {
    shared: Arc<Shared>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
}

impl ConnectionManager {
    pub fn new(config: ConnectionConfig) -> Self {
        let (status, _) = watch::channel(EndpointStatus::default());
        let (conn, _) = watch::channel(None);
        let (shutdown, _) = watch::channel(false);
        Self {
            shared: Arc::new(Shared {
                config,
                status,
                conn,
                pending: Mutex::new(HashMap::new()),
                ping_waiters: Mutex::new(Vec::new()),
                terminal: Mutex::new(None),
                shutdown,
            }),
            supervisor: Mutex::new(None),
        }
    }

    /// The configured endpoint address.
    pub fn addr(&self) -> SocketAddr {
        self.shared.config.addr
    }

    /// Spawn the supervisor. Call exactly once.
    ///
    /// # Panics
    /// If called twice.
    pub fn start(&self) {
        let mut supervisor = self.supervisor.lock().unwrap();
        assert!(
            supervisor.is_none(),
            "ConnectionManager::start must be called exactly once"
        );
        let shared = self.shared.clone();
        *supervisor = Some(tokio::spawn(supervise(shared)));
    }

    /// Graceful shutdown: the supervisor says bye on the live connection
    /// (if any), fails everything still parked with
    /// [`ConnectionError::Shutdown`], and joins.
    pub async fn shutdown(&self) {
        self.shared.shutdown.send_replace(true);
        let supervisor = self.supervisor.lock().unwrap().take();
        if let Some(handle) = supervisor {
            let _ = handle.await;
        }
    }

    /// Current endpoint status projection.
    pub fn status(&self) -> EndpointStatus {
        self.shared.status.borrow().clone()
    }

    /// Subscribe to status changes (heartbeats update `last_seen_ms` /
    /// `inflight`; connect/disconnect flips `connected`).
    pub fn subscribe_status(&self) -> watch::Receiver<EndpointStatus> {
        self.shared.status.subscribe()
    }

    /// The terminal error, if the manager stopped permanently (version
    /// mismatch).
    pub fn terminal_error(&self) -> Option<ConnectionError> {
        self.shared.terminal.lock().unwrap().clone()
    }

    /// Dispatch one command and await its correlated result. Commands
    /// pipeline: any number may be in flight concurrently, results
    /// correlate by command id (arrival order irrelevant), and the manager
    /// never sends more than the endpoint's announced `max_concurrent`.
    ///
    /// While no connection is live the future parks until a reconnect
    /// succeeds (or the manager stops). A transport loss under an
    /// in-flight command fails with [`ConnectionError::TransportClosed`] —
    /// the caller decides whether to retry; durability is 's slice.
    pub async fn dispatch(
        &self,
        cmd: ExecCommand,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        self.dispatch_with_id(Uuid::new_v4(), cmd).await
    }

    /// [`ConnectionManager::dispatch`] with a caller-chosen command id.
    ///
    /// The durable queue keys its rows by the command id derived
    /// from the calling tool-call id, and re-drives rows after a restart
    /// with the *same* id — at-least-once delivery with command-id
    /// correlation (ADR-007). Additive API; the random-id behavior of
    /// `dispatch` is unchanged.
    pub async fn dispatch_with_id(
        &self,
        id: CommandId,
        cmd: ExecCommand,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        let handle = self.live_conn().await?;
        // Hold a permit for the whole flight: caps our in-flight count at
        // the endpoint's announced concurrency.
        let permit = handle
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ConnectionError::TransportClosed)?;
        let (sender, receiver) = oneshot::channel();
        self.shared.pending.lock().unwrap().insert(id, sender);
        if handle
            .writer
            .send(ServerFrame::Command { id, cmd })
            .await
            .is_err()
        {
            self.shared.pending.lock().unwrap().remove(&id);
            drop(permit);
            return Err(ConnectionError::TransportClosed);
        }
        // The sender is resolved with the correlated result, or dropped
        // (mapped to a transport error) when the connection dies.
        let outcome = receiver
            .await
            .map_err(|_| ConnectionError::TransportClosed)?;
        drop(permit);
        outcome
    }

    /// Probe liveness: send ping, resolve on the endpoint's answering
    /// heartbeat (which also refreshes `last_seen_ms`).
    pub async fn ping(&self) -> Result<(), ConnectionError> {
        let handle = self.live_conn().await?;
        let (sender, receiver) = oneshot::channel();
        self.shared.ping_waiters.lock().unwrap().push(sender);
        if handle.writer.send(ServerFrame::Ping).await.is_err() {
            // The transport just died; the supervisor's cleanup drops the
            // waiter, which resolves us with a transport error.
            return receiver.await.map_err(|_| ConnectionError::TransportClosed);
        }
        receiver.await.map_err(|_| ConnectionError::TransportClosed)
    }

    /// A live connection handle, or the reason there can never be one
    /// (terminal error / shutdown). Parks while a reconnect is pending.
    async fn live_conn(&self) -> Result<ConnHandle, ConnectionError> {
        let mut conn = self.shared.conn.subscribe();
        loop {
            if let Some(error) = self.shared.terminal.lock().unwrap().as_ref() {
                return Err(error.clone());
            }
            if *self.shared.shutdown.borrow() {
                return Err(ConnectionError::Shutdown);
            }
            if let Some(handle) = conn.borrow_and_update().clone() {
                return Ok(handle);
            }
            if conn.changed().await.is_err() {
                return Err(ConnectionError::Shutdown);
            }
        }
    }
}

/// Supervisor: keep exactly one connection to the configured endpoint
/// alive, forever, with bounded backoff between attempts.
async fn supervise(shared: Arc<Shared>) {
    let backoff = shared.config.backoff;
    let mut delay = backoff.initial;
    loop {
        if *shared.shutdown.borrow() {
            break;
        }
        match run_connection(&shared).await {
            Ok(()) => tracing::info!(
                addr = %shared.config.addr,
                "connection ended"
            ),
            Err(error) => {
                tracing::warn!(%error, addr = %shared.config.addr, "connection failed");
                if matches!(error, ConnectionError::VersionMismatch { .. }) {
                    // Reconnecting cannot fix a version gap — stop, and
                    // report the named error to every caller.
                    *shared.terminal.lock().unwrap() = Some(error);
                    break;
                }
            }
        }
        // The connection is over: nothing in flight survives it.
        shared.set_connected(false);
        shared.fail_pending(ConnectionError::TransportClosed);
        shared.fail_ping_waiters();
        if *shared.shutdown.borrow() {
            break;
        }
        let delay_now = backoff.next_delay(&mut delay);
        tracing::debug!(?delay_now, addr = %shared.config.addr, "reconnecting");
        tokio::time::sleep(delay_now).await;
    }
    // Final cleanup: retire the handle (wakes dispatchers parked waiting
    // for a connection — they re-check the terminal error / shutdown flag
    // and fail honestly) and leave nothing parked.
    shared.conn.send_replace(None);
    shared.set_connected(false);
    shared.fail_pending(ConnectionError::Shutdown);
    shared.fail_ping_waiters();
    tracing::info!("connection manager supervisor exited");
}

/// One connection lifetime: dial → handshake → steady state. Returns when
/// the connection ends for any reason.
async fn run_connection(shared: &Arc<Shared>) -> Result<(), ConnectionError> {
    let addr = shared.config.addr;
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|error| ConnectionError::Dial {
            addr,
            message: error.to_string(),
        })?;
    let (writer_tx, writer_rx) = mpsc::channel::<ServerFrame>(FRAME_CHANNEL_CAP);
    let (frames_tx, mut frames_rx) = mpsc::channel::<EndpointFrame>(FRAME_CHANNEL_CAP);
    spawn_pumps(stream, frames_tx, writer_rx);

    // Handshake: hello first; its version must equal ours or we refuse to
    // welcome (and drop the transport by returning).
    let (endpoint_id, max_concurrent) = match frames_rx.recv().await {
        Some(EndpointFrame::Hello {
            v,
            endpoint_id,
            max_concurrent,
        }) => {
            if v != PROTOCOL_VERSION {
                return Err(ConnectionError::VersionMismatch {
                    endpoint: v,
                    server: PROTOCOL_VERSION,
                });
            }
            (endpoint_id, max_concurrent)
        }
        Some(other) => {
            return Err(ConnectionError::Protocol(format!(
                "first frame must be hello, got {other:?}"
            )));
        }
        None => return Err(ConnectionError::TransportClosed),
    };
    writer_tx
        .send(ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: shared
                .config
                .heartbeat
                .map(|duration| duration.as_millis() as u64),
        })
        .await
        .map_err(|_| ConnectionError::TransportClosed)?;

    if max_concurrent == 0 {
        tracing::warn!(endpoint = %endpoint_id, "announced max_concurrent=0; clamping to 1");
    }
    let cap = max_concurrent.max(1) as usize;
    let handle = ConnHandle {
        writer: writer_tx.clone(),
        permits: Arc::new(Semaphore::new(cap)),
    };
    shared.status.send_modify(|status| {
        status.endpoint_id = endpoint_id.clone();
        status.max_concurrent = max_concurrent;
        status.last_seen_ms = 0;
        status.inflight = 0;
        status.connected = true;
    });
    shared.conn.send_replace(Some(handle));
    tracing::info!(endpoint = %endpoint_id, %addr, "endpoint connected");

    let mut shutdown = shared.shutdown.subscribe();
    if *shutdown.borrow_and_update() {
        // Shutdown raced the handshake — close gracefully right away.
        let _ = writer_tx.send(ServerFrame::Bye).await;
        return Ok(());
    }

    let outcome = loop {
        tokio::select! {
            frame = frames_rx.recv() => match frame {
                Some(EndpointFrame::Heartbeat { ts_ms, inflight, .. }) => {
                    shared.status.send_modify(|status| {
                        if status.connected {
                            status.last_seen_ms = status.last_seen_ms.max(ts_ms);
                            status.inflight = inflight;
                        }
                    });
                    shared.wake_pings();
                }
                Some(EndpointFrame::CommandResult { id, result, error }) => {
                    let outcome = match (result, error) {
                        (Some(result), None) => Ok(Ok(result)),
                        (None, Some(error)) => Ok(Err(error)),
                        _ => break Err(ConnectionError::Protocol(format!(
                            "result frame for {id} must carry exactly one of result/error"
                        ))),
                    };
                    match shared.pending.lock().unwrap().remove(&id) {
                        Some(sender) => {
                            let _ = sender.send(outcome);
                        }
                        None => tracing::warn!(
                            command_id = %id,
                            "result for unknown command (already failed or duplicate)"
                        ),
                    }
                }
                Some(EndpointFrame::Hello { .. }) => {
                    break Err(ConnectionError::Protocol("duplicate hello".into()));
                }
                // v1: a register frame is only valid as the first frame of
                // a worker-dial connection (the registration listener); on
                // this server-dial connection it is a protocol violation.
                Some(EndpointFrame::Register { .. }) => {
                    break Err(ConnectionError::Protocol(
                        "register frame on a server-dialed connection".into(),
                    ));
                }
                // v1 (P12): a peer bye is a graceful transport end — the
                // same path as loss, just distinguishable in the log.
                Some(EndpointFrame::Bye) => {
                    tracing::info!(endpoint = %endpoint_id, "endpoint said bye");
                    break Err(ConnectionError::TransportClosed);
                }
                None => break Err(ConnectionError::TransportClosed),
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(endpoint = %endpoint_id, "shutdown: sending bye");
                    let _ = writer_tx.send(ServerFrame::Bye).await;
                    break Ok(());
                }
            }
        }
    };
    // Retire this connection's handle before failing anyone so no new
    // dispatch grabs a dead writer.
    shared.conn.send_replace(None);
    shared.set_connected(false);
    outcome
}

/// Wire the TCP stream to the framed channel ends: a read pump (lines →
/// endpoint frames, malformed lines logged and skipped) and a write pump
/// (server frames → lines). Both pumps exit when either side closes,
/// which ends the connection loop's frame stream.
fn spawn_pumps(
    stream: TcpStream,
    frames_tx: mpsc::Sender<EndpointFrame>,
    mut writer_rx: mpsc::Receiver<ServerFrame>,
) {
    let (reader, mut writer) = stream.into_split();
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match decode_endpoint_frame(&line) {
                Ok(frame) => {
                    if frames_tx.send(frame).await.is_err() {
                        break; // connection loop went away
                    }
                }
                Err(error) => tracing::warn!(%error, "skipping malformed KiandProto line"),
            }
        }
    });
    tokio::spawn(async move {
        while let Some(frame) = writer_rx.recv().await {
            let line = encode_server_frame(&frame);
            if writer.write_all(line.as_bytes()).await.is_err()
                || writer.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The backoff delay grows geometrically and never exceeds `max`.
    #[test]
    fn backoff_delay_is_bounded() {
        let backoff = BackoffConfig {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(80),
            multiplier: 2.0,
        };
        let mut delay = backoff.initial;
        let mut delays = Vec::new();
        for _ in 0..10 {
            delays.push(backoff.next_delay(&mut delay));
        }
        assert_eq!(
            delays,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
                Duration::from_millis(80),
                Duration::from_millis(80),
                Duration::from_millis(80),
                Duration::from_millis(80),
                Duration::from_millis(80),
                Duration::from_millis(80),
                Duration::from_millis(80),
            ]
        );
    }

    /// The status projection is serde round-trippable (persist and
    /// expose it).
    #[test]
    fn endpoint_status_round_trips() {
        let status = EndpointStatus {
            endpoint_id: "dev-box".into(),
            max_concurrent: 4,
            connected: true,
            last_seen_ms: 1234,
            inflight: 2,
        };
        let value = serde_json::to_value(&status).unwrap();
        let back: EndpointStatus = serde_json::from_value(value).unwrap();
        assert_eq!(back, status);
    }

    /// Shutdown before start is a no-op that parks nothing.
    #[tokio::test]
    async fn shutdown_without_start_is_a_noop() {
        let manager = ConnectionManager::new(ConnectionConfig::new("127.0.0.1:1".parse().unwrap()));
        manager.shutdown().await;
        assert!(!manager.status().connected);
        assert!(manager.terminal_error().is_none());
    }
}
