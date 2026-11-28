//! Route transports (R12-04): the two connection shapes a queue route
//! can dispatch over.
//!
//! - [`RouteTransport::Managed`] — the legacy server-dial path: a
//!   [`ConnectionManager`] that dials a configured static `endpoints:`
//!   address, performs the hello/welcome handshake and reconnects on
//!   loss. Its liveness timestamps are the endpoint-reported `ts_ms`
//!   (unchanged, spec M10).
//! - [`RouteTransport::Registered`] — the worker-dial path (KiandProto
//!   v1): the registration listener's live connection, wrapped by
//!   [`RegisteredTransport`]. Heartbeats and results arrive on the
//!   listener's inbound channel; commands go out through its writer.
//!   Liveness timestamps are the **server clock** (receipt time — M10).
//!
//! Both expose the same tiny surface the per-route queue driver reads
//! (`status`, `subscribe_status`, `addr`, `dispatch_with_id`), so
//! [`crate::backend`] drives static pins and dynamic members through
//! one identical loop.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::sync::{Semaphore, mpsc, oneshot, watch};

use kian_core::fs::{FsCommand, FsResult};
use kian_core::seams::{ExecCommand, ExecResult};

use crate::connection::{ConnectionManager, EndpointStatus};
use crate::error::ConnectionError;
use crate::proto::{CommandError, CommandId, EndpointFrame, ServerFrame};
use crate::registration::RegisteredWorker;
use crate::store::now_ms;

use crate::dispatch::{DispatchOutcome, DispatchReply};

/// The transport one queue route dispatches over.
pub enum RouteTransport {
    /// The static server-dial path (a configured `endpoints:` pin).
    Managed(Arc<ConnectionManager>),
    /// The dynamic worker-dial path (a registered member's live
    /// connection).
    Registered(Arc<RegisteredTransport>),
}

impl RouteTransport {
    /// The observed peer address (observability only — never identity).
    pub fn addr(&self) -> SocketAddr {
        match self {
            Self::Managed(manager) => manager.addr(),
            Self::Registered(registered) => registered.peer_addr(),
        }
    }

    /// Current liveness/status projection.
    pub fn status(&self) -> EndpointStatus {
        match self {
            Self::Managed(manager) => manager.status(),
            Self::Registered(registered) => registered.status(),
        }
    }

    /// Subscribe to status changes (connect/disconnect flips,
    /// heartbeat freshness).
    pub fn subscribe_status(&self) -> watch::Receiver<EndpointStatus> {
        match self {
            Self::Managed(manager) => manager.subscribe_status(),
            Self::Registered(registered) => registered.subscribe_status(),
        }
    }

    /// Dispatch one command and await its correlated result (by command
    /// id; at-least-once semantics live at the queue row, not here).
    pub async fn dispatch_with_id(
        &self,
        id: CommandId,
        cmd: ExecCommand,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        match self {
            Self::Managed(manager) => manager.dispatch_with_id(id, cmd).await,
            Self::Registered(registered) => registered.dispatch_with_id(id, cmd).await,
        }
    }

    pub async fn dispatch_with_id_after_write(
        &self,
        id: CommandId,
        cmd: ExecCommand,
        written: oneshot::Sender<()>,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        match self {
            Self::Managed(manager) => manager.dispatch_with_id_after_write(id, cmd, written).await,
            Self::Registered(registered) => {
                registered
                    .dispatch_with_id_after_write(id, cmd, written)
                    .await
            }
        }
    }

    /// Dispatch one filesystem command (v2, ADR-023) and await its
    /// correlated result.
    pub async fn dispatch_fs_with_id(
        &self,
        id: CommandId,
        cmd: FsCommand,
    ) -> Result<Result<FsResult, CommandError>, ConnectionError> {
        match self {
            Self::Managed(manager) => manager.dispatch_fs_with_id(id, cmd).await,
            Self::Registered(registered) => registered.dispatch_fs_with_id(id, cmd).await,
        }
    }

    pub async fn dispatch_fs_with_id_after_write(
        &self,
        id: CommandId,
        cmd: FsCommand,
        written: oneshot::Sender<()>,
    ) -> Result<Result<FsResult, CommandError>, ConnectionError> {
        match self {
            Self::Managed(manager) => {
                manager
                    .dispatch_fs_with_id_after_write(id, cmd, written)
                    .await
            }
            Self::Registered(registered) => {
                registered
                    .dispatch_fs_with_id_after_write(id, cmd, written)
                    .await
            }
        }
    }
}

/// The server-side half of one registered worker connection: the
/// listener's steady-state loop already peeled liveness bookkeeping off
/// the frame stream; this type consumes the forwarded frames
/// (heartbeats, results), correlates results by command id, and writes
/// commands back through the listener's writer channel.
///
/// Liveness is the **server clock** (receipt time of the last
/// heartbeat frame — M10); the heartbeat's own `ts_ms` is ignored on
/// this path. When the inbound channel closes (the listener's
/// steady-state task ended: bye, transport loss or replace) the
/// transport flips offline and fails every parked dispatch with a
/// transport error — never a fake result.
pub struct RegisteredTransport {
    id: String,
    generation: u64,
    peer_addr: SocketAddr,
    writer: mpsc::Sender<ServerFrame>,
    status: watch::Sender<EndpointStatus>,
    pending: Mutex<HashMap<CommandId, oneshot::Sender<DispatchOutcome>>>,
    permits: Arc<Semaphore>,
}

impl RegisteredTransport {
    /// Wraps a completed registration's live connection and spawns its
    /// inbound pump. The connection is live at construction (the
    /// listener hands it over after flushing `welcome`).
    pub fn new(worker: RegisteredWorker) -> Arc<Self> {
        let RegisteredWorker {
            id,
            generation,
            peer_addr,
            version,
            description: _,
            capabilities: _,
            max_concurrent,
            writer,
            mut inbound,
        } = worker;
        let (status, _) = watch::channel(EndpointStatus {
            endpoint_id: id.clone(),
            max_concurrent,
            connected: true,
            last_seen_ms: now_ms().max(0) as u64,
            inflight: 0,
        });
        let _ = version; // declared on the wire; the directory carries it
        let this = Arc::new(Self {
            id: id.clone(),
            generation,
            peer_addr,
            writer,
            status,
            pending: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_concurrent.max(1) as usize)),
        });
        let pump = Arc::clone(&this);
        tokio::spawn(async move {
            while let Some(frame) = inbound.recv().await {
                match frame {
                    // Server-clock liveness (M10): monotone max.
                    EndpointFrame::Heartbeat { inflight, .. } => {
                        pump.status.send_modify(|status| {
                            if status.connected {
                                let now = now_ms().max(0) as u64;
                                status.last_seen_ms = status.last_seen_ms.max(now);
                                status.inflight = inflight;
                            }
                        });
                    }
                    EndpointFrame::CommandResult { id, result, error } => {
                        let outcome = match (result, error) {
                            (Some(result), None) => Ok(DispatchReply::Exec(Ok(result))),
                            (None, Some(error)) => Ok(DispatchReply::Exec(Err(error))),
                            _ => {
                                tracing::warn!(
                                    endpoint = %pump.id,
                                    command_id = %id,
                                    "result frame must carry exactly one of result/error; dropping"
                                );
                                continue;
                            }
                        };
                        match pump.pending.lock().unwrap().remove(&id) {
                            Some(sender) => {
                                let _ = sender.send(outcome);
                            }
                            None => tracing::debug!(
                                endpoint = %pump.id,
                                command_id = %id,
                                "result for unknown command (already failed or duplicate)"
                            ),
                        }
                    }
                    EndpointFrame::FsCommandResult { id, result, error } => {
                        let outcome = match (result, error) {
                            (Some(result), None) => Ok(DispatchReply::Fs(Ok(result))),
                            (None, Some(error)) => Ok(DispatchReply::Fs(Err(error))),
                            _ => {
                                tracing::warn!(
                                    endpoint = %pump.id,
                                    command_id = %id,
                                    "fs result frame must carry exactly one of result/error; dropping"
                                );
                                continue;
                            }
                        };
                        match pump.pending.lock().unwrap().remove(&id) {
                            Some(sender) => {
                                let _ = sender.send(outcome);
                            }
                            None => tracing::debug!(
                                endpoint = %pump.id,
                                command_id = %id,
                                "fs result for unknown command (already failed or duplicate)"
                            ),
                        }
                    }
                    // Hello/register cannot reach steady state (the
                    // listener closes those); heartbeats above; bye ends
                    // the channel instead of travelling it.
                    other => {
                        tracing::debug!(
                            endpoint = %pump.id,
                            frame = ?other,
                            "unexpected frame on registered connection"
                        );
                    }
                }
            }
            // Inbound closed: the connection is gone. Retire the
            // transport, fail every parked dispatch honestly.
            pump.status.send_modify(|status| status.connected = false);
            let parked: Vec<_> = pump.pending.lock().unwrap().drain().collect();
            for (_, sender) in parked {
                let _ = sender.send(Err(ConnectionError::TransportClosed));
            }
            tracing::debug!(endpoint = %pump.id, "registered transport closed");
        });
        this
    }

    /// The registration's generation (the queue route's identity guard).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The observed peer address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Current status projection.
    pub fn status(&self) -> EndpointStatus {
        self.status.borrow().clone()
    }

    /// Subscribe to status changes.
    pub fn subscribe_status(&self) -> watch::Receiver<EndpointStatus> {
        self.status.subscribe()
    }

    /// Dispatch one command: park the correlation slot, write the
    /// command frame, await the correlated result (or the transport's
    /// honest failure).
    pub async fn dispatch_with_id(
        &self,
        id: CommandId,
        cmd: ExecCommand,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        match self
            .dispatch_frame(id, ServerFrame::Command { id, cmd }, None)
            .await?
        {
            DispatchReply::Exec(outcome) => Ok(outcome),
            DispatchReply::Fs(_) => Err(ConnectionError::Protocol(
                "fs result on an exec command id".into(),
            )),
        }
    }

    pub async fn dispatch_with_id_after_write(
        &self,
        id: CommandId,
        cmd: ExecCommand,
        written: oneshot::Sender<()>,
    ) -> Result<Result<ExecResult, CommandError>, ConnectionError> {
        match self
            .dispatch_frame(id, ServerFrame::Command { id, cmd }, Some(written))
            .await?
        {
            DispatchReply::Exec(outcome) => Ok(outcome),
            DispatchReply::Fs(_) => Err(ConnectionError::Protocol(
                "fs result on an exec command id".into(),
            )),
        }
    }

    /// Dispatch one filesystem command (v2) and await its correlated
    /// result.
    pub async fn dispatch_fs_with_id(
        &self,
        id: CommandId,
        cmd: FsCommand,
    ) -> Result<Result<FsResult, CommandError>, ConnectionError> {
        match self
            .dispatch_frame(id, ServerFrame::FsCommandFrame { id, cmd }, None)
            .await?
        {
            DispatchReply::Fs(outcome) => Ok(outcome),
            DispatchReply::Exec(_) => Err(ConnectionError::Protocol(
                "exec result on an fs command id".into(),
            )),
        }
    }

    pub async fn dispatch_fs_with_id_after_write(
        &self,
        id: CommandId,
        cmd: FsCommand,
        written: oneshot::Sender<()>,
    ) -> Result<Result<FsResult, CommandError>, ConnectionError> {
        match self
            .dispatch_frame(id, ServerFrame::FsCommandFrame { id, cmd }, Some(written))
            .await?
        {
            DispatchReply::Fs(outcome) => Ok(outcome),
            DispatchReply::Exec(_) => Err(ConnectionError::Protocol(
                "exec result on an fs command id".into(),
            )),
        }
    }

    /// Park the correlation slot, write one frame, await the correlated
    /// reply. The permit is held for the whole flight (the announced cap).
    async fn dispatch_frame(
        &self,
        id: CommandId,
        frame: ServerFrame,
        written: Option<oneshot::Sender<()>>,
    ) -> DispatchOutcome {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ConnectionError::TransportClosed)?;
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, sender);
        if self.writer.send(frame).await.is_err() {
            self.pending.lock().unwrap().remove(&id);
            drop(permit);
            // The writer channel is closed: the frame never left this
            // process. Retire the transport on the status projection (a
            // driver must stop dispatching into a dead connection) and
            // report it distinctly, so the queue layer refunds the attempt
            // it just charged instead of spending the row's re-drive bound
            // on a command the endpoint provably never received.
            self.status.send_modify(|status| status.connected = false);
            return Err(ConnectionError::NotWritten);
        }
        if let Some(written) = written {
            let _ = written.send(());
        }
        let outcome = receiver
            .await
            .map_err(|_| ConnectionError::TransportClosed)?;
        drop(permit);
        outcome
    }
}
