//! `RemoteExecBackend` — the durable remote-execution seam impl (
//! DESIGN §4.3/§6.2, ADR-007).
//!
//! Enqueue-first: the queue row is durable in `kian.sqlite` **before** any
//! dispatch flies (admit-before-execute, the same discipline as inbox
//! rows). Dispatch is owned by one per-endpoint driver task, never by the
//! calling tool: connected → send immediately; offline → the row parks as
//! `pending` and the driver re-drives on the next connect (a `server`
//! restart re-drives `inflight` rows too — at-least-once to the endpoint,
//! deduped by command id at the row level). Re-drive is **bounded**
//! (ADR-033): a row whose result was lost gets [`RemoteExecConfig::max_redrives`]
//! retries and is then abandoned, so a command that destroys its own ack
//! path (restarting the server or its worker) converges instead of looping.
//!
//! The caller's future resolves from the correlated result. How long it
//! waits is per phase (ADR-030): the *dispatch* wait is `queue_timeout`, and
//! expiry there abandons the row (terminal — a later duplicate command id
//! gets the same honest Timeout); the *execution* wait is the command's own
//! kill bound, and expiry there never mutates the row — only the watcher
//! stops. [`WaitPolicy`] selects which waits a caller takes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{Notify, Semaphore, oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use kian_core::fs::{FsCommand, FsResult};
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};
use kian_core::secrets::SecretRedactor;

use crate::connection::ConnectionManager;
use crate::membership::WorkerDirectory;
use crate::proto::CommandId;
use crate::route::RouteTransport;
use crate::store::{
    CommandOutcome, EnqueueOutcome, ExecStore, QueueStatus, QueueStoreError, QueuedCommand,
    WorkerCommand, now_ms,
};

/// Caller-visible failures of the queue machinery, mapped onto the exec
/// seam's error shape.
fn store_error(e: QueueStoreError) -> ExecError {
    ExecError::Io(format!("exec queue store: {e}"))
}

type Waiter = oneshot::Sender<Result<CommandOutcome, ExecError>>;

/// Which waits a caller takes over one durable queue row (ADR-030).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitPolicy {
    /// The synchronous `exec` tool: one *dispatch* wait, then abandon. A
    /// parked drain must not stay parked, so the caller never waits for a
    /// worker that is already executing.
    Caller,
    /// The async `start` observer: a dispatch wait, then an *execution*
    /// wait bounded by the command's own worker-side timeout. Nothing is
    /// parked, so waiting longer is free — and expiry never mutates the row.
    Job,
}

/// What a queue row was doing when a wait expired (ADR-030). The value is
/// observed, never guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiredRow {
    /// No route had taken the row: nothing ran, and the wait abandoned it.
    Pending,
    /// A worker is executing it: the caller stopped waiting; the row stays
    /// live and re-driveable.
    Inflight,
    /// A previous wait already abandoned this command id.
    Abandoned,
}

impl ExpiredRow {
    /// The vocabulary reported to the model, one word per observed state.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Inflight => "inflight",
            Self::Abandoned => "abandoned",
        }
    }

    /// Whether the command was (as far as the server knows) still running.
    #[must_use]
    pub fn still_running(self) -> bool {
        matches!(self, Self::Inflight)
    }
}

/// A watched wait's failures: the seam's own failures plus the honest
/// timeout that carries what the row was doing (ADR-030).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecWaitError {
    /// The wait bound expired. `row` says what was observed; `bound_ms` is
    /// the bound that was in force, so the caller can report it verbatim.
    #[error("execution timed out")]
    Timeout { row: ExpiredRow, bound_ms: u64 },
    /// Everything else is the exec seam's own failure, unchanged.
    #[error("{0}")]
    Failure(ExecError),
}

impl ExecWaitError {
    /// Collapses onto the seam's error shape: a timeout is the seam's
    /// honest [`ExecError::Timeout`], so pre-ADR-030 callers stay identical.
    #[must_use]
    pub fn into_exec_error(self) -> ExecError {
        match self {
            Self::Timeout { .. } => ExecError::Timeout,
            Self::Failure(error) => error,
        }
    }
}

/// The execution wait bound for one command (ADR-030):
/// `2 * exec_bound + queue_timeout` — one dispatch, one at-least-once
/// re-dispatch, and one dispatch-wait of slack.
fn execution_bound(dispatch_bound: Duration, exec_bound_ms: Option<u64>) -> Duration {
    let exec = exec_bound_ms
        .map(Duration::from_millis)
        .unwrap_or(dispatch_bound);
    exec * 2 + dispatch_bound
}

/// The row projection one wait consults, never a second source of truth.
enum RowNow {
    Done(Box<CommandOutcome>),
    Pending,
    Inflight,
    Abandoned,
    Gone,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Per-endpoint queue driver state. Static pins are born at boot with
/// generation 0; dynamic routes carry the registration's generation so
/// teardown is generation-scoped (A15: a stale loss handler can never
/// detach its successor's route).
struct Route {
    endpoint_id: String,
    transport: RouteTransport,
    /// Caller → driver wake-up. `notify_one` is permit-based, so a wake
    /// arriving while the driver is busy loading rows is not lost.
    nudge: Notify,
    /// Per-route stop signal (detach/shutdown). Also permit-based.
    stop: Notify,
    /// In-flight cap for re-drive bursts, sized from the endpoint's
    /// announced `max_concurrent` at each connect (the connection manager's
    /// own per-connection semaphore is the hard cap).
    permits: Mutex<Option<Arc<Semaphore>>>,
    /// Command ids this process currently has dispatched (prevents the
    /// driver re-dispatching a row whose dispatch is already parked).
    dispatching: Mutex<HashSet<CommandId>>,
    /// The registration generation this route belongs to (0 = static).
    generation: u64,
    /// The driver task (joined on detach/shutdown).
    driver: Mutex<Option<JoinHandle<()>>>,
}

/// Shared queue state.
struct QueueCore {
    store: ExecStore,
    /// Runtime-mutable route registry (Q2): static pins are inserted at
    /// construction; dynamic routes attach/detach with membership.
    routes: RwLock<HashMap<String, Arc<Route>>>,
    config: RemoteExecConfig,
    /// the exec output redactor. Every `Ok` outcome passes it
    /// once, at settlement ([`QueueCore::complete`]) — the single choke
    /// point the durable row, every waiter, the sync tool result and
    /// the async-jobs completion all read from.
    redactor: SecretRedactor,
    /// row-lease identity: which replica's drivers own the rows
    /// they drive. Defaults to a single-process holder; multi-replica
    /// assemblies (postgres mode) pass the config-validated unique
    /// `drain.holder` via [`RemoteExecBackend::with_row_leases`].
    row_lease_holder: String,
    /// row-lease TTL — how long a claimed row stays owned without a
    /// heartbeat before any replica may re-drive it.
    row_lease_ttl: Duration,
    /// The runtime membership directory (present iff dynamic membership
    /// is on) — the dynamic `default_endpoint` resolution's read.
    directory: Option<Arc<WorkerDirectory>>,
    /// Liveness bound for that resolution (workers.offline_after_ms).
    directory_offline_after_ms: u64,
    waiters: Mutex<HashMap<CommandId, Vec<(u64, Waiter)>>>,
    waiter_tokens: std::sync::atomic::AtomicU64,
    shutdown: watch::Sender<bool>,
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
    /// How many times an `inflight` row may be **re-driven** after its
    /// result was lost (ADR-033). The bound exists so at-least-once re-drive
    /// terminates: a command whose own side effect destroys the ack path
    /// (restarting the server, killing its worker) can never be
    /// acknowledged, so the driver abandons the row after `1 + max_redrives`
    /// dispatches instead of replaying it forever. `0` disables re-drive
    /// entirely (first dispatch only).
    pub max_redrives: u32,
    /// How recently a row must have been dispatched for its re-drive to be
    /// postponed (ADR-037). A route attach re-drives `inflight` rows
    /// immediately (ADR-007); when the row's last dispatch is younger than
    /// this grace, the driver waits instead of racing an execution that may
    /// still be running on the worker. The grace only postpones a dispatch:
    /// it never settles a row and never changes the caller's bound. `0`
    /// disables it (every `inflight` row re-drives on attach, as before).
    pub redrive_grace: Duration,
}

impl Default for RemoteExecConfig {
    fn default() -> Self {
        Self {
            queue_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(250),
            max_redrives: 1,
            redrive_grace: Duration::from_secs(5),
        }
    }
}

/// The dynamic-membership inputs of [`RemoteExecBackend::with_membership`]:
/// the shared runtime directory (zero static routes are allowed when it
/// is supplied) and its liveness bound for the dynamic default
/// resolution.
#[derive(Clone)]
pub struct MembershipConfig {
    pub directory: Arc<WorkerDirectory>,
    pub offline_after_ms: u64,
}

/// How the plain [`ExecBackend::exec`] (untracked, random command id)
/// picks its endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultRoute {
    /// A configured static pin (today's behavior: the first
    /// `endpoints:` entry).
    Static(String),
    /// Runtime membership: the first `online` member at call time, else
    /// the first known id, else the honest error (spec §4.1 — the
    /// untracked path must never invent an id).
    Dynamic,
}

/// The server-side remote [`ExecBackend`] over a durable sqlite queue
/// . Cloneable; share one per process.
///
/// The endpoint managers must be started by the caller
/// ([`ConnectionManager::start`]); this type only observes them.
pub struct RemoteExecBackend {
    core: Arc<QueueCore>,
    default_route: DefaultRoute,
}

impl Clone for RemoteExecBackend {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            default_route: self.default_route.clone(),
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
        // row-lease defaults: a single-process holder and the queue
        // timeout as the TTL (a row owed longer than that is re-drivable
        // — consistent with the honest-timeout bound). Single-process
        // behavior is unchanged: the same holder always re-claims.
        Self::with_row_leases(
            store,
            routes,
            default_endpoint,
            config,
            "kian-exec",
            config.queue_timeout,
        )
    }

    /// [`RemoteExecBackend::new`] with an explicit row-lease
    /// identity: `holder` names this replica (multi-replica postgres
    /// assemblies pass the config-validated unique `drain.holder`), `ttl`
    /// how long a claimed queue row stays owned without a heartbeat.
    /// Existing call sites stay on [`Self::new`] unchanged.
    pub fn with_row_leases(
        store: ExecStore,
        routes: Vec<(String, Arc<ConnectionManager>)>,
        default_endpoint: impl Into<String>,
        config: RemoteExecConfig,
        holder: impl Into<String>,
        ttl: Duration,
    ) -> std::result::Result<Self, String> {
        // No configured secret names → patterns-only redaction, the
        // same default the local exec path carries.
        Self::with_row_leases_redacting(
            store,
            routes,
            default_endpoint,
            config,
            holder,
            ttl,
            SecretRedactor::patterns_only(),
        )
    }

    /// [`RemoteExecBackend::with_row_leases`] with the exec
    /// output redactor. The redactor runs once per settled `Ok`
    /// outcome, before the durable row is written — the row, every
    /// waiter, `RemoteExecTool` results and async-job completions all
    /// observe scrubbed bytes.
    pub fn with_row_leases_redacting(
        store: ExecStore,
        routes: Vec<(String, Arc<ConnectionManager>)>,
        default_endpoint: impl Into<String>,
        config: RemoteExecConfig,
        holder: impl Into<String>,
        ttl: Duration,
        redactor: SecretRedactor,
    ) -> std::result::Result<Self, String> {
        Self::with_membership(
            store,
            routes,
            DefaultRoute::Static(default_endpoint.into()),
            config,
            holder,
            ttl,
            redactor,
            None,
        )
    }

    /// The membership constructor (spec §4.1): static routes may be
    /// **empty** iff a [`MembershipConfig`] is supplied (a `workers:`
    /// deployment with zero `endpoints:` still constructs — its routes
    /// attach at runtime as workers register). `default_route` picks the
    /// untracked `exec` path's endpoint: `Dynamic` resolves the first
    /// online member at call time.
    // The static-constructor chain (`new` → `with_row_leases` →
    // `with_row_leases_redacting`, whose signatures the spec pins) feeds
    // every lease/redactor argument through here — folding them into a
    // params struct would fork that chain, so the arity is deliberate.
    #[allow(clippy::too_many_arguments)]
    pub fn with_membership(
        store: ExecStore,
        routes: Vec<(String, Arc<ConnectionManager>)>,
        default_route: DefaultRoute,
        config: RemoteExecConfig,
        holder: impl Into<String>,
        ttl: Duration,
        redactor: SecretRedactor,
        membership: Option<MembershipConfig>,
    ) -> std::result::Result<Self, String> {
        if routes.is_empty() && membership.is_none() {
            return Err(
                "remote exec needs at least one endpoint route (or a `workers:` listener)".into(),
            );
        }
        let (directory, directory_offline_after_ms) = match &membership {
            Some(membership) => (
                Some(Arc::clone(&membership.directory)),
                membership.offline_after_ms,
            ),
            None => (None, 30_000),
        };
        let mut map = HashMap::new();
        for (endpoint_id, manager) in routes {
            let route = Arc::new(Route {
                endpoint_id: endpoint_id.clone(),
                transport: RouteTransport::Managed(manager),
                nudge: Notify::new(),
                stop: Notify::new(),
                permits: Mutex::new(None),
                dispatching: Mutex::new(HashSet::new()),
                generation: 0,
                driver: Mutex::new(None),
            });
            if map.insert(endpoint_id, route).is_some() {
                return Err("duplicate endpoint route".into());
            }
        }
        let (shutdown, _) = watch::channel(false);
        let core = Arc::new(QueueCore {
            store,
            routes: RwLock::new(map.clone()),
            config,
            redactor,
            row_lease_holder: holder.into(),
            row_lease_ttl: ttl,
            directory,
            directory_offline_after_ms,
            waiters: Mutex::new(HashMap::new()),
            waiter_tokens: std::sync::atomic::AtomicU64::new(0),
            shutdown,
        });
        // One driver per boot-created (static) route, all sharing the
        // final core (dispatch completion resolves waiters through it).
        // Dynamic routes spawn their drivers in `attach_route`.
        for (_, route) in map {
            let handle = tokio::spawn(drive_route(core.clone(), route.clone()));
            *route.driver.lock().unwrap() = Some(handle);
        }
        Ok(Self {
            core,
            default_route,
        })
    }

    /// Attach a runtime route (dynamic registration, spec Q2). The
    /// durable registry projection is upserted **before** the route
    /// becomes visible (the §9.1 step-3-before-step-4 ordering), then
    /// the per-route driver spawns — its first loop iteration re-drives
    /// this id's parked `pending`/`inflight` rows. Replace semantics:
    /// an existing route for the same id is stopped first.
    pub async fn attach_route(
        &self,
        endpoint_id: impl Into<String>,
        transport: RouteTransport,
        generation: u64,
    ) {
        self.attach_route_inner(endpoint_id.into(), transport, generation, None)
            .await;
    }

    /// [`RemoteExecBackend::attach_route`] carrying the worker's last
    /// declaration (ADR-025): the registration handoff caches
    /// `description`/`protocol_version` on the registry row in the same
    /// upsert that marks it online, so a kian restart does not lose the
    /// catalog text before the worker re-registers.
    pub async fn attach_route_with_declaration(
        &self,
        endpoint_id: impl Into<String>,
        transport: RouteTransport,
        generation: u64,
        description: Option<&str>,
        protocol_version: u32,
        capabilities: &[String],
    ) {
        self.attach_route_inner(
            endpoint_id.into(),
            transport,
            generation,
            Some((description, protocol_version, capabilities)),
        )
        .await;
    }

    async fn attach_route_inner(
        &self,
        id: String,
        transport: RouteTransport,
        generation: u64,
        declaration: Option<(Option<&str>, u32, &[String])>,
    ) {
        let status = transport.status();
        let registry = match declaration {
            Some((description, protocol_version, capabilities)) => {
                self.core
                    .store
                    .upsert_endpoint_with_declaration(
                        &id,
                        &transport.addr().to_string(),
                        status.last_seen_ms,
                        "online",
                        description,
                        protocol_version,
                        capabilities,
                    )
                    .await
            }
            None => {
                self.core
                    .store
                    .upsert_endpoint(
                        &id,
                        &transport.addr().to_string(),
                        status.last_seen_ms,
                        "online",
                    )
                    .await
            }
        };
        if let Err(e) = registry {
            tracing::warn!(error = %e, endpoint = %id, "registry upsert on attach failed");
        }
        // Replace: stop any incumbent driver for this id first (the map
        // lock is never held across the await).
        let incumbent = self.core.routes.write().unwrap().get(&id).cloned();
        if let Some(incumbent) = incumbent {
            stop_route(&incumbent).await;
        }
        let route = Arc::new(Route {
            endpoint_id: id.clone(),
            transport,
            nudge: Notify::new(),
            stop: Notify::new(),
            permits: Mutex::new(None),
            dispatching: Mutex::new(HashSet::new()),
            generation,
            driver: Mutex::new(None),
        });
        let handle = tokio::spawn(drive_route(self.core.clone(), route.clone()));
        *route.driver.lock().unwrap() = Some(handle);
        self.core.routes.write().unwrap().insert(id, route);
    }

    /// Detach a runtime route (leave), generation-scoped (A15): a no-op
    /// unless the registry still holds exactly this generation's route,
    /// so a stale loss handler never detaches its successor. The queue
    /// rows stay durable and parked (queue-first); the final registry
    /// projection reads `offline` at the transport's last-seen.
    pub async fn detach_route(&self, endpoint_id: &str, generation: u64) {
        let route = {
            let routes = self.core.routes.read().unwrap();
            match routes.get(endpoint_id) {
                Some(route) if route.generation == generation => Arc::clone(route),
                _ => return,
            }
        };
        {
            let mut routes = self.core.routes.write().unwrap();
            // Remove exactly this route instance: a concurrent replace
            // may already have swapped a newer one in (A15).
            if routes
                .get(endpoint_id)
                .is_some_and(|held| Arc::ptr_eq(held, &route))
            {
                routes.remove(endpoint_id);
            }
        }
        stop_route(&route).await;
        let status = route.transport.status();
        if let Err(e) = self
            .core
            .store
            .upsert_endpoint(
                endpoint_id,
                &route.transport.addr().to_string(),
                status.last_seen_ms,
                "offline",
            )
            .await
        {
            tracing::warn!(error = %e, endpoint = %endpoint_id, "registry upsert on detach failed");
        }
        tracing::debug!(endpoint = %endpoint_id, generation, "runtime route detached");
    }

    /// The runtime route ids currently attached (static + dynamic).
    pub fn route_ids(&self) -> Vec<String> {
        self.core.routes.read().unwrap().keys().cloned().collect()
    }

    /// The store behind the queue (registry reads for the watchdog,
    /// row inspection for ops).
    pub fn store(&self) -> &ExecStore {
        &self.core.store
    }

    /// The row-lease identity this backend's drivers claim queue
    /// rows under: `(holder, ttl)`.
    pub fn row_lease(&self) -> (&str, Duration) {
        (&self.core.row_lease_holder, self.core.row_lease_ttl)
    }

    /// The untracked `exec` path's configured default route.
    pub fn default_route(&self) -> &DefaultRoute {
        &self.default_route
    }

    /// Enqueue-first tracked execution (the [`crate::RemoteExecTool`] path):
    /// the row is durable before any dispatch, the command id is the
    /// caller's dedupe key, and the caller's future resolves from the
    /// correlated result — or an honest [`ExecError::Timeout`] with the row
    /// `abandoned`.
    ///
    /// Queue-first (Q1): a live route is **never** a precondition. The
    /// row enqueues durably by the bound worker id; with no route it
    /// parks `pending` until one attaches, the caller's
    /// [`RemoteExecConfig::queue_timeout`] abandons it honestly, or the
    /// row settles. Nothing is ever dispatched to a different worker.
    pub async fn exec_tracked(
        &self,
        command_id: CommandId,
        endpoint_id: &str,
        cmd: ExecCommand,
        tool_call_ref: Option<String>,
    ) -> Result<ExecResult, ExecError> {
        self.exec_tracked_watched(
            command_id,
            endpoint_id,
            cmd,
            tool_call_ref,
            WaitPolicy::Caller,
        )
        .await
        .map_err(ExecWaitError::into_exec_error)
    }

    /// Enqueue-first tracked execution with an explicit wait policy
    /// (ADR-030). [`WaitPolicy::Caller`] keeps the historical single
    /// dispatch wait; [`WaitPolicy::Job`] adds the execution wait bounded by
    /// the command's own worker-side `timeout_ms` and never mutates the row.
    pub async fn exec_tracked_watched(
        &self,
        command_id: CommandId,
        endpoint_id: &str,
        cmd: ExecCommand,
        tool_call_ref: Option<String>,
        policy: WaitPolicy,
    ) -> Result<ExecResult, ExecWaitError> {
        self.tracked(
            command_id,
            endpoint_id,
            WorkerCommand::Exec(cmd),
            tool_call_ref,
            policy,
        )
        .await?
        .to_exec()
        .map_err(ExecWaitError::Failure)
    }

    /// Enqueue-first tracked filesystem execution (the
    /// [`crate::RemoteFsTool`] path, ADR-023). Identical queue/correlation/
    /// timeout discipline as [`RemoteExecBackend::exec_tracked`]; the
    /// caller gets the fs result shape.
    pub async fn fs_tracked(
        &self,
        command_id: CommandId,
        endpoint_id: &str,
        cmd: FsCommand,
        tool_call_ref: Option<String>,
    ) -> Result<FsResult, ExecError> {
        self.tracked(
            command_id,
            endpoint_id,
            WorkerCommand::Fs(cmd),
            tool_call_ref,
            WaitPolicy::Caller,
        )
        .await
        .map_err(ExecWaitError::into_exec_error)?
        .to_fs()
    }

    /// The shared enqueue → park → correlate path for both command
    /// families. `policy` decides how long the caller waits after the row
    /// stops being `pending` (ADR-030); the enqueue half is identical.
    async fn tracked(
        &self,
        command_id: CommandId,
        endpoint_id: &str,
        command: WorkerCommand,
        tool_call_ref: Option<String>,
        policy: WaitPolicy,
    ) -> Result<CommandOutcome, ExecWaitError> {
        // The command's own worker-side kill bound, snapshotted before the
        // command moves into the row: policy `Job` derives its execution
        // wait from it.
        let exec_bound_ms = match &command {
            WorkerCommand::Exec(cmd) => cmd.timeout_ms,
            WorkerCommand::Fs(_) => None,
        };
        let route = self.core.route(endpoint_id);
        match self
            .core
            .store
            .enqueue(command_id, endpoint_id, &command, tool_call_ref.as_deref())
            .await
            .map_err(|e| ExecWaitError::Failure(store_error(e)))?
        {
            // Re-execution of a settled command id: answer from the row —
            // one row, one dispatch, ever (ADR-007 dedupe). Rows written
            // by legacy builds may carry raw output; redact the
            // read-back so a replayed tool call still emits scrubbed
            // bytes even against an old store.
            EnqueueOutcome::Existing(row) => match row.status {
                QueueStatus::Done => {
                    let settled = match row.outcome.expect("done rows always carry an outcome") {
                        // The row was redacted at settlement (report
                        // rides the durable result); re-redacting is
                        // idempotent (scrubbed bytes yield an empty
                        // report) and covers legacy rows whose raw
                        // output still scrubs — the stored report is
                        // kept and summed with anything newly redacted.
                        CommandOutcome::Ok { result } => {
                            let (stdout, stdout_report) =
                                self.core.redactor.redact_with_report(&result.stdout);
                            let (stderr, stderr_report) =
                                self.core.redactor.redact_with_report(&result.stderr);
                            let stored = result.redactions.clone();
                            Ok(CommandOutcome::Ok {
                                result: ExecResult {
                                    stdout,
                                    stderr,
                                    redactions: kian_core::secrets::merge_redaction_reports(
                                        stored
                                            .into_iter()
                                            .chain(stdout_report)
                                            .chain(stderr_report),
                                    ),
                                    ..result
                                },
                            })
                        }
                        other => Ok(other),
                    };
                    return settled.map_err(ExecWaitError::Failure);
                }
                QueueStatus::Abandoned => {
                    return Err(ExecWaitError::Timeout {
                        row: ExpiredRow::Abandoned,
                        bound_ms: millis(self.core.config.queue_timeout),
                    });
                }
                QueueStatus::Pending | QueueStatus::Inflight => {
                    // Someone is already driving this command id; attach.
                }
            },
            // No route ⇒ the row simply parks (Q1); a route nudges its
            // driver so an enqueue racing the loop still surfaces.
            EnqueueOutcome::Inserted => {
                if let Some(route) = route {
                    route.nudge.notify_one();
                }
            }
        }
        self.core
            .await_outcome(command_id, policy, exec_bound_ms)
            .await
    }

    /// The caller's wait bound: the dispatch wait, and (policy `Job` only)
    /// the execution wait derived from the command's own kill bound.
    #[must_use]
    pub fn queue_timeout(&self) -> Duration {
        self.core.config.queue_timeout
    }

    /// Stops every queue driver (static and runtime) and joins them.
    /// Endpoint managers are owned by the caller and shut down
    /// separately; registered transports die with their inbound
    /// channels (the listener's shutdown path).
    pub async fn shutdown(&self) {
        self.core.shutdown.send_replace(true);
        let routes: Vec<Arc<Route>> = self
            .core
            .routes
            .write()
            .unwrap()
            .drain()
            .map(|(_, route)| route)
            .collect();
        for route in routes {
            stop_route(&route).await;
        }
    }
}

/// Signals one route's driver to stop (stop + nudge wake it out of any
/// select) and joins it.
async fn stop_route(route: &Arc<Route>) {
    route.stop.notify_one();
    route.nudge.notify_one();
    let driver = route.driver.lock().unwrap().take();
    if let Some(handle) = driver {
        let _ = handle.await;
    }
}

impl QueueCore {
    fn route(&self, endpoint_id: &str) -> Option<Arc<Route>> {
        self.routes.read().unwrap().get(endpoint_id).cloned()
    }

    /// Resolve the untracked `exec` path's endpoint (spec §4.1): the
    /// first `online` member at call time, else the first known id
    /// (directory, else registry rows), else the honest error — never
    /// an invented id.
    async fn resolve_default(&self) -> Result<String, ExecError> {
        let now = now_ms().max(0) as u64;
        if let Some(directory) = &self.directory {
            if let Some(id) = directory.first_online(now, self.directory_offline_after_ms) {
                return Ok(id);
            }
            if let Some(id) = directory.known_ids().into_iter().next() {
                return Ok(id);
            }
        }
        // Directory empty (e.g. fresh boot): fall back to the durable
        // registry's known ids — an offline-but-known worker still
        // parks rows honestly.
        match self.store.endpoints().await {
            Ok(rows) if !rows.is_empty() => Ok(rows
                .into_iter()
                .map(|row| row.endpoint_id)
                .min()
                .expect("non-empty")),
            _ => Err(ExecError::Io(
                "no execution worker is configured on this server".into(),
            )),
        }
    }

    /// Resolve the caller's future from the correlated result; race the
    /// queue timeout honestly.
    async fn await_outcome(
        &self,
        command_id: CommandId,
        policy: WaitPolicy,
        exec_bound_ms: Option<u64>,
    ) -> Result<CommandOutcome, ExecWaitError> {
        let dispatch_bound = self.config.queue_timeout;
        // Phase A — the dispatch wait: the row may still be `pending`
        // (nobody took it), so the bound is the absent-worker bound.
        if let Some(outcome) = self.wait_settle(command_id, dispatch_bound).await? {
            return Ok(outcome);
        }
        let now = self
            .row_now(command_id)
            .await
            .map_err(ExecWaitError::Failure)?;
        match policy {
            WaitPolicy::Caller => {
                // The historical contract: the parked drain gives up and the
                // row is abandoned whatever it was doing.
                match self.store.mark_abandoned(command_id).await {
                    Ok(true) => Err(ExecWaitError::Timeout {
                        row: match now {
                            RowNow::Inflight => ExpiredRow::Inflight,
                            RowNow::Abandoned => ExpiredRow::Abandoned,
                            _ => ExpiredRow::Pending,
                        },
                        bound_ms: millis(dispatch_bound),
                    }),
                    // The row settled concurrently — the real result (or a
                    // competing abandonment) won the race against the timer.
                    Ok(false) => match self.settled_outcome(command_id).await {
                        Ok(Some(outcome)) => Ok(outcome),
                        Ok(None) => Err(ExecWaitError::Timeout {
                            row: ExpiredRow::Abandoned,
                            bound_ms: millis(dispatch_bound),
                        }),
                        Err(e) => Err(ExecWaitError::Failure(e)),
                    },
                    Err(e) => Err(ExecWaitError::Failure(store_error(e))),
                }
            }
            WaitPolicy::Job => match now {
                RowNow::Done(outcome) => Ok(*outcome),
                RowNow::Abandoned => Err(ExecWaitError::Timeout {
                    row: ExpiredRow::Abandoned,
                    bound_ms: millis(dispatch_bound),
                }),
                RowNow::Gone => Err(ExecWaitError::Failure(ExecError::Io(
                    "exec queue row vanished".into(),
                ))),
                RowNow::Pending => {
                    // Nothing ran: abandoning destroys no result.
                    match self.store.mark_abandoned(command_id).await {
                        Ok(_) => Err(ExecWaitError::Timeout {
                            row: ExpiredRow::Pending,
                            bound_ms: millis(dispatch_bound),
                        }),
                        Err(e) => Err(ExecWaitError::Failure(store_error(e))),
                    }
                }
                RowNow::Inflight => {
                    // Phase B — the execution wait: a worker is running the
                    // command, whose own kill bound terminates it. Expiry
                    // NEVER mutates the row (the queue keeps the truth and
                    // stays re-driveable); only the watcher stops.
                    let bound = execution_bound(dispatch_bound, exec_bound_ms);
                    if let Some(outcome) = self.wait_settle(command_id, bound).await? {
                        return Ok(outcome);
                    }
                    match self
                        .row_now(command_id)
                        .await
                        .map_err(ExecWaitError::Failure)?
                    {
                        RowNow::Done(outcome) => Ok(*outcome),
                        _ => Err(ExecWaitError::Timeout {
                            row: ExpiredRow::Inflight,
                            bound_ms: millis(bound),
                        }),
                    }
                }
            },
        }
    }

    /// Wait up to `bound` for the correlated result. `None` means the bound
    /// expired with the row unsettled — the caller decides what to do.
    async fn wait_settle(
        &self,
        command_id: CommandId,
        bound: Duration,
    ) -> Result<Option<CommandOutcome>, ExecWaitError> {
        let (token, receiver) = self.register_waiter(command_id);
        // Close the completion race: if the row settled between enqueue and
        // this registration, prefer the recorded outcome.
        match self.settled_outcome(command_id).await {
            Ok(Some(outcome)) => {
                self.drop_waiter(&command_id, token);
                return Ok(Some(outcome));
            }
            Ok(None) => {}
            Err(e) => {
                self.drop_waiter(&command_id, token);
                return Err(ExecWaitError::Failure(e));
            }
        }
        let settled = tokio::select! {
            outcome = receiver => Some(outcome.unwrap_or_else(|_| {
                Err(ExecError::Io("queue driver stopped without a result".into()))
            })),
            _ = tokio::time::sleep(bound) => {
                self.drop_waiter(&command_id, token);
                None
            }
        };
        match settled {
            Some(result) => result.map(Some).map_err(ExecWaitError::Failure),
            None => Ok(None),
        }
    }

    /// The row's current phase, read from the durable row (never a cache).
    async fn row_now(&self, command_id: CommandId) -> Result<RowNow, ExecError> {
        match self.store.get(command_id).await.map_err(store_error)? {
            Some(row) => Ok(match row.status {
                QueueStatus::Done => match row.outcome {
                    Some(outcome) => RowNow::Done(Box::new(outcome)),
                    None => RowNow::Gone,
                },
                QueueStatus::Pending => RowNow::Pending,
                QueueStatus::Inflight => RowNow::Inflight,
                QueueStatus::Abandoned => RowNow::Abandoned,
            }),
            None => Ok(RowNow::Gone),
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
    ) -> (u64, oneshot::Receiver<Result<CommandOutcome, ExecError>>) {
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
        // the one redaction point on the remote path — before
        // the row is written, so the durable copy (and everything that
        // reads it: waiters, tool results, async completions, replays)
        // only ever holds scrubbed bytes. The same pass emits its
        // provenance report, which rides the durable row so deduped
        // read-backs still answer with the report.
        let outcome = match outcome {
            CommandOutcome::Ok { result } => {
                let (stdout, stdout_report) = self.redactor.redact_with_report(&result.stdout);
                let (stderr, stderr_report) = self.redactor.redact_with_report(&result.stderr);
                CommandOutcome::Ok {
                    result: ExecResult {
                        stdout,
                        stderr,
                        redactions: kian_core::secrets::merge_redaction_reports(
                            stdout_report.into_iter().chain(stderr_report),
                        ),
                        ..result
                    },
                }
            }
            other => other,
        };
        match self.store.mark_done(command_id, &outcome).await {
            Ok(true) => {
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
                    let _ = waiter.send(Ok(outcome.clone()));
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
    let mut status = route.transport.subscribe_status();
    loop {
        if *core.shutdown.borrow() {
            break;
        }
        let st = route.transport.status();
        // Registry projection (reads it): disposable, rebuilt from live
        // status on every loop turn — heartbeats refresh it while idle.
        if let Err(e) = core
            .store
            .upsert_endpoint(
                &route.endpoint_id,
                &route.transport.addr().to_string(),
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
                        break; // transport dropped
                    }
                    continue;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                _ = route.stop.notified() => break,
            }
        }
        // Connected: refresh the re-drive cap from the announced
        // concurrency. In-flight dispatches keep their old permits — the
        // transport's own semaphore remains the hard cap.
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
            let now = now_ms();
            let grace_ms = duration_ms_i64(core.config.redrive_grace);
            rows.into_iter()
                .filter(|row| {
                    // ADR-037: an `inflight` row dispatched within the grace
                    // may still be running on the worker; re-driving it now
                    // would race that execution. Wait for a later loop turn.
                    let young = row.status == QueueStatus::Inflight
                        && grace_ms > 0
                        && now.saturating_sub(row.updated_at) < grace_ms;
                    !young && dispatching.insert(row.command_id)
                })
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
                _ = route.stop.notified() => break,
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
    // row lease: single-statement ownership of this row for the
    // dispatch's duration — claimed BEFORE any mutation of the row. A
    // fresh lease held by *another* replica means its driver owns the
    // row: skip honestly and leave the row parked untouched (the lease
    // is liveness only; command-id correlation is the safety net).
    // Same-holder claims always land, so single-process behavior is
    // unchanged.
    //
    // The same statement counts the dispatch attempt and enforces the
    // re-drive bound (ADR-033): a `pending` row always dispatches, an
    // `inflight` row only while its attempts are below `1 + max_redrives`.
    // At-least-once therefore terminates even for a command that destroys
    // its own ack path.
    let max_dispatches = 1u32.saturating_add(core.config.max_redrives);
    let lease_ttl_ms = duration_ms_i64(core.row_lease_ttl);
    match core
        .store
        .claim_queue_row(
            row.command_id,
            &core.row_lease_holder,
            lease_ttl_ms,
            now_ms(),
            max_dispatches,
        )
        .await
    {
        Ok(true) => {}
        // Two causes, told apart by the store: a fresh foreign lease (that
        // replica owns the row — leave it untouched), or a spent re-drive
        // bound (converge the row now). The convergence transition is
        // guarded on "no live lease", so it cannot abandon a row another
        // driver is dispatching right now.
        Ok(false) => {
            // Only a row that already looks spent is worth the convergence
            // write; the common case (a fresh foreign lease) stays a read-only
            // skip with no writer-lock churn on the poll path.
            let looks_exhausted = matches!(row.status, QueueStatus::Inflight)
                && row.dispatch_attempts >= max_dispatches;
            if looks_exhausted {
                match core
                    .store
                    .abandon_exhausted(row.command_id, max_dispatches, now_ms())
                    .await
                {
                    Ok(true) => {
                        tracing::warn!(
                            command_id = %row.command_id,
                            endpoint = %row.endpoint_id,
                            dispatch_attempts = row.dispatch_attempts,
                            max_dispatches,
                            max_redrives = core.config.max_redrives,
                            tool_call_ref = row.tool_call_ref.as_deref().unwrap_or(""),
                            "queue row abandoned: re-drive bound exhausted — the command ran without ever delivering a result (its side effect can destroy the ack path, e.g. restarting the server or its worker); at-least-once re-drive stops here"
                        );
                    }
                    // A live lease (or a concurrent settle) still owns the
                    // outcome; the next loop turn re-reads and converges.
                    Ok(false) => {}
                    Err(e) => return Err(format!("exhausted re-drive convergence failed: {e}")),
                }
                return Ok(());
            }
            tracing::debug!(
                command_id = %row.command_id,
                holder = %core.row_lease_holder,
                "queue row leased by another replica; this driver skips it"
            );
            return Ok(());
        }
        Err(e) => return Err(format!("row lease claim failed: {e}")),
    }
    // pending → inflight. A false return means the row was already inflight
    // (presumed lost across a restart) — proceed, at-least-once.
    if let Err(e) = core.store.mark_inflight(row.command_id).await {
        return Err(format!("mark_inflight failed: {e}"));
    }
    // Heartbeat the row lease while the dispatch flies (liveness only —
    // mirrors the engine's drain heartbeat). Losing the lease mid-flight
    // (expired and reclaimed after a long dispatch) is logged and the
    // dispatch still completes through the terminal guard: at-least-once
    // with command-id dedupe remains the net.
    let hb_stop = Arc::new(Notify::new());
    let heartbeat = {
        let store = core.store.clone();
        let holder = core.row_lease_holder.clone();
        let id = row.command_id;
        let ttl = core.row_lease_ttl;
        let stop = Arc::clone(&hb_stop);
        tokio::spawn(async move {
            row_lease_heartbeat(store, id, holder, ttl, stop).await;
        })
    };
    let outcome: Result<CommandOutcome, crate::error::ConnectionError> = match &row.command {
        WorkerCommand::Exec(cmd) => route
            .transport
            .dispatch_with_id(row.command_id, cmd.clone())
            .await
            .map(|reply| match reply {
                Ok(result) => CommandOutcome::Ok { result },
                Err(error) => CommandOutcome::Err { error },
            }),
        WorkerCommand::Fs(cmd) => route
            .transport
            .dispatch_fs_with_id(row.command_id, cmd.clone())
            .await
            .map(|reply| match reply {
                Ok(result) => CommandOutcome::Fs { result },
                Err(error) => CommandOutcome::Err { error },
            }),
    };
    hb_stop.notify_one();
    let _ = heartbeat.await;
    match outcome {
        Ok(outcome) => core.complete(row.command_id, outcome).await,
        // Transport loss / manager stop: fail honestly *without* settling
        // the row — the next connect re-drives it (at-least-once). The
        // caller's queue timeout remains the honesty bound.
        Err(e) => return Err(e.to_string()),
    }
    drop(permit);
    Ok(())
}

/// A `Duration` clamped into the store's i64 milliseconds.
fn duration_ms_i64(d: Duration) -> i64 {
    d.as_millis().min(i64::MAX as u128) as i64
}

/// Extends one queue row's lease every `ttl/3` until `stop` fires —
/// the owner's drive-loop heartbeat. Losing the lease (another
/// replica reclaimed after expiry) logs and ends the loop; the
/// in-flight dispatch itself is unaffected (the lease is liveness
/// only, never a write permission).
async fn row_lease_heartbeat(
    store: ExecStore,
    id: CommandId,
    holder: String,
    ttl: Duration,
    stop: Arc<Notify>,
) {
    let ttl_ms = duration_ms_i64(ttl);
    let interval = ttl.max(Duration::from_millis(1)) / 3;
    loop {
        tokio::select! {
            _ = stop.notified() => return,
            _ = tokio::time::sleep(interval) => {},
        }
        match store
            .heartbeat_queue_row(id, &holder, ttl_ms, now_ms())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(command_id = %id, "row lease lost mid-dispatch (liveness only)");
                return;
            }
            Err(e) => {
                tracing::warn!(command_id = %id, error = %e, "row lease heartbeat failed");
            }
        }
    }
}

// The plain seam impl: untracked, random command id, default route. Tools
// use `exec_tracked` (via `RemoteExecTool`) to get command-id dedupe.
// With dynamic membership the default resolves per call (the first
// online member, else the first known id, else the honest error).
impl ExecBackend for RemoteExecBackend {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        let this = self.clone();
        Box::pin(async move {
            let id = Uuid::new_v4();
            let endpoint = match &this.default_route {
                DefaultRoute::Static(endpoint) => endpoint.clone(),
                DefaultRoute::Dynamic => this.core.resolve_default().await?,
            };
            this.exec_tracked(id, &endpoint, cmd, None).await
        })
    }
}
