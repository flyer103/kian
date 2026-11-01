//! `RemoteExecBackend` — the durable remote-execution seam impl (card C20,
//! DESIGN §4.3/§6.2, ADR-007).
//!
//! Enqueue-first: the queue row is durable in `kian.sqlite` **before** any
//! dispatch flies (admit-before-execute, the same discipline as inbox
//! rows). Dispatch is owned by one per-endpoint driver task, never by the
//! calling tool: connected → send immediately; offline → the row parks as
//! `pending` and the driver re-drives on the next connect (a `server`
//! restart re-drives `inflight` rows too — at-least-once to the endpoint,
//! deduped by command id at the row level).
//!
//! The caller's future resolves from the correlated result; if
//! `queue_timeout` expires first the caller gets an honest
//! [`ExecError::Timeout`] and the row settles `abandoned` (terminal — a
//! later duplicate command id gets the same honest Timeout).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{Notify, Semaphore, oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};

use crate::connection::ConnectionManager;
use crate::proto::{CommandError, CommandId};
use crate::store::{
    CommandOutcome, EnqueueOutcome, ExecStore, QueueStatus, QueueStoreError, QueuedCommand,
};

/// Caller-visible failures of the queue machinery, mapped onto the exec
/// seam's error shape.
fn store_error(e: QueueStoreError) -> ExecError {
    ExecError::Io(format!("exec queue store: {e}"))
}

type Waiter = oneshot::Sender<Result<ExecResult, ExecError>>;

/// Per-endpoint queue driver state.
struct Route {
    endpoint_id: String,
    manager: Arc<ConnectionManager>,
    /// Caller → driver wake-up. `notify_one` is permit-based, so a wake
    /// arriving while the driver is busy loading rows is not lost.
    nudge: Notify,
    /// In-flight cap for re-drive bursts, sized from the endpoint's
    /// announced `max_concurrent` at each connect (the connection manager's
    /// own per-connection semaphore is the hard cap).
    permits: Mutex<Option<Arc<Semaphore>>>,
    /// Command ids this process currently has dispatched (prevents the
    /// driver re-dispatching a row whose dispatch is already parked).
    dispatching: Mutex<HashSet<CommandId>>,
}

/// Shared queue state.
struct QueueCore {
    store: ExecStore,
    routes: HashMap<String, Arc<Route>>,
    config: RemoteExecConfig,
    waiters: Mutex<HashMap<CommandId, Vec<(u64, Waiter)>>>,
    waiter_tokens: std::sync::atomic::AtomicU64,
    shutdown: watch::Sender<bool>,
    drivers: Mutex<Vec<JoinHandle<()>>>,
}

/// Tuning of the durable queue.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RemoteExecConfig {
    /// How long a caller waits for the correlated result before the row is
    /// honestly abandoned (`ExecError::Timeout`).
    pub queue_timeout: Duration,
    /// Idle poll of the queue table while connected (a safety net behind
    /// the wake-up; also refreshes the registry projection).
    pub poll_interval: Duration,
}

impl Default for RemoteExecConfig {
    fn default() -> Self {
        Self {
            queue_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(250),
        }
    }
}

/// The server-side remote [`ExecBackend`] over a durable sqlite queue
/// (card C20). Cloneable; share one per process.
///
/// The endpoint managers must be started by the caller
/// ([`ConnectionManager::start`]); this type only observes them.
pub struct RemoteExecBackend {
    core: Arc<QueueCore>,
    default_endpoint: String,
}

impl Clone for RemoteExecBackend {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            default_endpoint: self.default_endpoint.clone(),
        }
    }
}

impl RemoteExecBackend {
    /// Builds the backend over a migrated [`ExecStore`], routing commands
    /// by `endpoint_id` to the given connection managers, and spawns one
    /// queue driver per endpoint. `default_endpoint` names the route used
    /// by the plain [`ExecBackend::exec`] (untracked, random command id).
    pub fn new(
        store: ExecStore,
        routes: Vec<(String, Arc<ConnectionManager>)>,
        default_endpoint: impl Into<String>,
        config: RemoteExecConfig,
    ) -> std::result::Result<Self, String> {
        if routes.is_empty() {
            return Err("remote exec needs at least one endpoint route".into());
        }
        let default_endpoint = default_endpoint.into();
        let mut map = HashMap::new();
        for (endpoint_id, manager) in routes {
            let route = Arc::new(Route {
                endpoint_id: endpoint_id.clone(),
                manager,
                nudge: Notify::new(),
                permits: Mutex::new(None),
                dispatching: Mutex::new(HashSet::new()),
            });
            if map.insert(endpoint_id, route).is_some() {
                return Err("duplicate endpoint route".into());
            }
        }
        let (shutdown, _) = watch::channel(false);
        let core = Arc::new(QueueCore {
            store,
            routes: map.clone(),
            config,
            waiters: Mutex::new(HashMap::new()),
            waiter_tokens: std::sync::atomic::AtomicU64::new(0),
            shutdown,
            drivers: Mutex::new(Vec::new()),
        });
        // One driver per route, all sharing the final core (dispatch
        // completion resolves waiters through it).
        let mut drivers = Vec::new();
        for (_, route) in map {
            drivers.push(tokio::spawn(drive_route(core.clone(), route)));
        }
        *core.drivers.lock().unwrap() = drivers;
        Ok(Self {
            core,
            default_endpoint,
        })
    }

    /// The store behind the queue (registry reads for the C21 watchdog,
    /// row inspection for ops).
    pub fn store(&self) -> &ExecStore {
        &self.core.store
    }

    /// The configured default endpoint route.
    pub fn default_endpoint(&self) -> &str {
        &self.default_endpoint
    }

    /// Enqueue-first tracked execution (the [`crate::RemoteExecTool`] path):
    /// the row is durable before any dispatch, the command id is the
    /// caller's dedupe key, and the caller's future resolves from the
    /// correlated result — or an honest [`ExecError::Timeout`] with the row
    /// `abandoned`.
    pub async fn exec_tracked(
        &self,
        command_id: CommandId,
        endpoint_id: &str,
        cmd: ExecCommand,
        tool_call_ref: Option<String>,
    ) -> Result<ExecResult, ExecError> {
        let route = self.core.route(endpoint_id).ok_or_else(|| {
            ExecError::Io(format!("no endpoint route registered for {endpoint_id:?}"))
        })?;
        match self
            .core
            .store
            .enqueue(command_id, endpoint_id, &cmd, tool_call_ref.as_deref())
            .await
            .map_err(store_error)?
        {
            // Re-execution of a settled command id: answer from the row —
            // one row, one dispatch, ever (ADR-007 dedupe).
            EnqueueOutcome::Existing(row) => match row.status {
                QueueStatus::Done => {
                    return row
                        .outcome
                        .expect("done rows always carry an outcome")
                        .to_exec();
                }
                QueueStatus::Abandoned => return Err(ExecError::Timeout),
                QueueStatus::Pending | QueueStatus::Inflight => {
                    // Someone is already driving this command id; attach.
                }
            },
            EnqueueOutcome::Inserted => route.nudge.notify_one(),
        }
        self.core.await_outcome(command_id).await
    }

    /// Stops the queue drivers and joins them. Endpoint managers are owned
    /// by the caller and shut down separately.
    pub async fn shutdown(&self) {
        self.core.shutdown.send_replace(true);
        let drivers: Vec<_> = self.core.drivers.lock().unwrap().drain(..).collect();
        for driver in drivers {
            let _ = driver.await;
        }
    }
}

impl QueueCore {
    fn route(&self, endpoint_id: &str) -> Option<Arc<Route>> {
        self.routes.get(endpoint_id).cloned()
    }

    /// Resolve the caller's future from the correlated result; race the
    /// queue timeout honestly.
    async fn await_outcome(&self, command_id: CommandId) -> Result<ExecResult, ExecError> {
        let (token, receiver) = self.register_waiter(command_id);
        // Close the completion race: if the row settled between enqueue and
        // this registration, prefer the recorded outcome.
        if let Some(outcome) = self.settled_outcome(command_id).await? {
            self.drop_waiter(&command_id, token);
            return outcome.to_exec();
        }
        tokio::select! {
            outcome = receiver => {
                outcome.unwrap_or_else(|_| {
                    Err(ExecError::Io("queue driver stopped without a result".into()))
                })
            }
            _ = tokio::time::sleep(self.config.queue_timeout) => {
                self.drop_waiter(&command_id, token);
                match self.store.mark_abandoned(command_id).await {
                    Ok(true) => Err(ExecError::Timeout),
                    // The row settled concurrently — the real result (or a
                    // competing abandonment) won the race against the timer.
                    Ok(false) => {
                        if let Some(outcome) = self.settled_outcome(command_id).await? {
                            outcome.to_exec()
                        } else {
                            Err(ExecError::Timeout)
                        }
                    }
                    Err(e) => Err(store_error(e)),
                }
            }
        }
    }

    /// The recorded outcome iff the row is settled (`done`).
    async fn settled_outcome(
        &self,
        command_id: CommandId,
    ) -> Result<Option<CommandOutcome>, ExecError> {
        match self.store.get(command_id).await.map_err(store_error)? {
            Some(row) if row.status == QueueStatus::Done => Ok(row.outcome),
            _ => Ok(None),
        }
    }

    fn register_waiter(
        &self,
        command_id: CommandId,
    ) -> (u64, oneshot::Receiver<Result<ExecResult, ExecError>>) {
        let (sender, receiver) = oneshot::channel();
        let token = self
            .waiter_tokens
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.waiters
            .lock()
            .unwrap()
            .entry(command_id)
            .or_default()
            .push((token, sender));
        (token, receiver)
    }

    /// Remove exactly this caller's waiter — a timing-out duplicate must
    /// not sever another caller's future on the same command id.
    fn drop_waiter(&self, command_id: &CommandId, token: u64) {
        let mut waiters = self.waiters.lock().unwrap();
        if let Some(list) = waiters.get_mut(command_id) {
            list.retain(|(t, _)| *t != token);
            if list.is_empty() {
                waiters.remove(command_id);
            }
        }
    }

    /// Record a settled outcome durably, then wake every caller attached to
    /// this command id. Called only from a dispatch task.
    async fn complete(&self, command_id: CommandId, outcome: CommandOutcome) {
        match self.store.mark_done(command_id, &outcome).await {
            Ok(true) => {
                let mapped = outcome.to_exec();
                let waiters: Vec<Waiter> = self
                    .waiters
                    .lock()
                    .unwrap()
                    .remove(&command_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(_, waiter)| waiter)
                    .collect();
                for waiter in waiters {
                    let _ = waiter.send(mapped.clone());
                }
            }
            // Terminal already (timeout won): the row keeps its honest
            // abandonment; late results are discarded, never replayed.
            Ok(false) => {
                tracing::warn!(
                    %command_id,
                    "late result for a settled command discarded"
                );
            }
            Err(e) => tracing::error!(%command_id, error = %e, "queue row completion failed"),
        }
    }
}

/// Per-endpoint driver: the only component that turns queue rows into wire
/// dispatches.
///
/// - offline → park (rows stay durable `pending`), project the registry;
/// - on connect → load every `pending`/`inflight` row (a restart's
///   `inflight` rows are presumed lost — re-drive is at-least-once) and
///   dispatch under the endpoint's announced concurrency cap;
/// - on transport loss under a dispatch → the row stays parked for the
///   next connect; only a settled result or an honest abandonment ends it.
async fn drive_route(core: Arc<QueueCore>, route: Arc<Route>) {
    let mut shutdown = core.shutdown.subscribe();
    let mut status = route.manager.subscribe_status();
    loop {
        if *core.shutdown.borrow() {
            break;
        }
        let st = route.manager.status();
        // Registry projection (C21 reads it): disposable, rebuilt from live
        // status on every loop turn — heartbeats refresh it while idle.
        if let Err(e) = core
            .store
            .upsert_endpoint(
                &route.endpoint_id,
                &route.manager.addr().to_string(),
                st.last_seen_ms,
                if st.connected { "online" } else { "offline" },
            )
            .await
        {
            tracing::warn!(error = %e, "endpoint registry upsert failed");
        }
        if !st.connected {
            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        break; // manager dropped
                    }
                    continue;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            }
        }
        // Connected: refresh the re-drive cap from the announced
        // concurrency. In-flight dispatches keep their old permits — the
        // connection manager's semaphore remains the hard cap.
        *route.permits.lock().unwrap() =
            Some(Arc::new(Semaphore::new(st.max_concurrent.max(1) as usize)));
        // Register the wake *before* loading so an enqueue racing the load
        // still surfaces (Notify permits are not lost).
        let nudge = route.nudge.notified();
        let rows = match core.store.drivable(&route.endpoint_id).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "queue load failed");
                tokio::time::sleep(core.config.poll_interval).await;
                continue;
            }
        };
        let fresh: Vec<QueuedCommand> = {
            let mut dispatching = route.dispatching.lock().unwrap();
            rows.into_iter()
                .filter(|row| dispatching.insert(row.command_id))
                .collect()
        };
        if fresh.is_empty() {
            tokio::select! {
                _ = nudge => {},
                changed = status.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = tokio::time::sleep(core.config.poll_interval) => {},
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
            continue;
        }
        for row in fresh {
            let core = core.clone();
            let route = route.clone();
            tokio::spawn(async move {
                drive_row(core, route, row).await;
            });
        }
        // Loop immediately: a batch may have left more eligible rows.
    }
    tracing::debug!(endpoint = %route.endpoint_id, "queue driver stopped");
}

/// Dispatch one queued row. The dispatching slot is released on exit so the
/// driver may re-drive the row after a transport loss or a restart.
async fn drive_row(core: Arc<QueueCore>, route: Arc<Route>, row: QueuedCommand) {
    let id = row.command_id;
    let result = drive_row_inner(&core, &route, row).await;
    route.dispatching.lock().unwrap().remove(&id);
    if let Err(e) = result {
        tracing::warn!(command_id = %id, error = %e, "dispatch ended without a result; row stays parked for the next connect");
    }
}

async fn drive_row_inner(
    core: &QueueCore,
    route: &Route,
    row: QueuedCommand,
) -> Result<(), String> {
    // Snapshot-race guard: the driver's `drivable` read may have snapshotted
    // this row *before* a concurrent dispatch settled it (`done`), while the
    // `dispatching` slot was only freed *after* that settle — a stale row can
    // therefore pass the loop-side guard. This re-read happens strictly
    // after the guard was acquired (hence after any competing settle
    // committed), so a terminal row is dropped here instead of executing a
    // duplicate command on the endpoint.
    match core.store.get(row.command_id).await {
        Ok(Some(current)) => {
            if matches!(current.status, QueueStatus::Done | QueueStatus::Abandoned) {
                tracing::debug!(
                    command_id = %row.command_id,
                    "drivable snapshot was stale; row already settled"
                );
                return Ok(());
            }
        }
        Ok(None) => return Ok(()),
        Err(e) => return Err(format!("settle re-check failed: {e}")),
    }
    // Cap re-drive bursts at the announced concurrency. The semaphore is
    // always installed by the driver before it loads rows.
    let permit = {
        let semaphore = route
            .permits
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "route cap not installed".to_string())?;
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| "route cap closed".to_string())?
    };
    // pending → inflight. A false return means the row was already inflight
    // (presumed lost across a restart) — proceed, at-least-once.
    if let Err(e) = core.store.mark_inflight(row.command_id).await {
        return Err(format!("mark_inflight failed: {e}"));
    }
    let outcome: Result<Result<ExecResult, CommandError>, crate::error::ConnectionError> = route
        .manager
        .dispatch_with_id(row.command_id, row.cmd)
        .await;
    match outcome {
        Ok(Ok(result)) => {
            core.complete(row.command_id, CommandOutcome::Ok { result })
                .await
        }
        Ok(Err(error)) => {
            core.complete(row.command_id, CommandOutcome::Err { error })
                .await
        }
        // Transport loss / manager stop: fail honestly *without* settling
        // the row — the next connect re-drives it (at-least-once). The
        // caller's queue timeout remains the honesty bound.
        Err(e) => return Err(e.to_string()),
    }
    drop(permit);
    Ok(())
}

// The plain seam impl: untracked, random command id, default route. Tools
// use `exec_tracked` (via `RemoteExecTool`) to get command-id dedupe.
impl ExecBackend for RemoteExecBackend {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        let this = self.clone();
        Box::pin(async move {
            let id = Uuid::new_v4();
            this.exec_tracked(id, &this.default_endpoint.clone(), cmd, None)
                .await
        })
    }
}
