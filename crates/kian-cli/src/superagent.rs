//! Super-agent assembly: the wiring that mounts the
//! `kian-superagent` bundle and `kian-exec` server half onto the real
//! engine, behind the same fail-closed discipline as the built-in
//! assembly.
//!
//! This module is assembly-layer code — the peer of [`crate::server`],
//! not of the HTTP handler modules. It names the engine, the store and
//! the drivers on purpose (that is what assembly is ); the layered
//! iron rule still holds at its edges: HTTP handlers reach any of this
//! only through the [`crate::api::ControlPlane`] / [`crate::ops::OpsPlane`]
//! traits, and the bundle reaches the server only through its
//! [`InboxWriter`] port (implemented here over the engine — the
//! cycle breaker is a late-bound engine slot).
//!
//! Composition (one engine, one driver — the pattern):
//!
//! - the configured resident Router session runs under
//!   [`RouterDriver`] (port install + residency parking over a ReAct
//!   loop whose tools are the bundle's dispatch/channel tools);
//! - sessions with Thread lineage (a first input carrying the
//!   delegation wrapper, or an existing `thread/started`) run under
//!   [`WorkerLoopDriver`] (lineage + evidence gate + completion
//!   report);
//! - every other session is a plain built-in session and runs the
//!   default agent's loop unchanged.
//!
//! Startup order is load-bearing (the acceptance): inbox
//! rebuild → watchdog/subscription start → listener accepts traffic.
//! [`startup_order_log`] records the sequence so the integration test
//! can assert it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{
    ChannelAdapter, LoopDriver, LoopOutcome, ToolCall, ToolError, ToolProvider, ToolResult,
    ToolSchema, WatchdogSource,
};
use kian_exec::watchdog::{Clock as ExecClock, SystemClock as ExecSystemClock};
use kian_exec::{EndpointRegistryRow, ExecStore, HeartbeatWatchdog, WatchdogConfig};
use kian_loop::evidence::EvidenceGateConfig;
use kian_loop::react::ReActLoop;
use kian_orchestrator::{DrainCtx, Engine, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, LineagePlan, MockChannel, PassthroughRegistry,
    PortFor, RouterDriver, RouterInbox, RouterMetrics, RouterProfile, StreamPort, StreamPortError,
    SubscriptionService, WorkerBinding, WorkerLoopDriver, WorkerSpawner, WorkerStreamsError,
    lineage_plan, worker_binding_of, worker_sessions,
};

use crate::config::EndpointConfig;

// ---------------------------------------------------------------------------
// Startup order log (per-assembly; the acceptance assertion)
// ---------------------------------------------------------------------------

/// The recorded startup sequence of one assembly. Purely observable
/// state for `tests/superagent_serve.rs`; production code never reads
/// it beyond recording.
pub type StartupOrder = Arc<Mutex<Vec<&'static str>>>;

/// A fresh, empty order log for one `Server::from_config` run.
pub fn new_startup_order() -> StartupOrder {
    Arc::new(Mutex::new(Vec::new()))
}

fn record(order: &StartupOrder, step: &'static str) {
    order.lock().expect("startup order lock").push(step);
}

/// Records that `startup_rebuild` returned (called by
/// `Server::from_config` between the rebuild and any service start).
pub fn record_rebuild_done(order: &StartupOrder) {
    record(order, "startup_rebuild_done");
}

// ---------------------------------------------------------------------------
// The InboxWriter port over the real engine (the cycle breaker)
// ---------------------------------------------------------------------------

/// [`InboxWriter`] implemented over the engine's own admit + wake: the
/// row is durable when `admit` resolves (the engine resolves its admit
/// future only on commit — invariant 1), and the wake is strictly
/// after.
///
/// The engine is installed **after** the port exists (the engine needs
/// a driver, the driver's tools need this port): a late-bound slot,
/// filled exactly once by [`crate::server::Server::from_config`] before
/// the first wake can fire.
pub struct EngineInbox {
    engine: OnceLock<Arc<Engine<ServeLoopDriver>>>,
    /// (ADR-049) The per-spawn `session/created` class marker (HARNESS-BRIDGE §4.7): the default
    /// worker template's harness marker plus one per roster assignee. Unset = the historical `{}`.
    markers: OnceLock<BridgeMarkers>,
}

impl EngineInbox {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: OnceLock::new(),
            markers: OnceLock::new(),
        })
    }

    /// The installed engine, when the assembly has reached installation:
    /// the answer surface is constructed before the engine exists (the
    /// engine needs the driver, the driver needs the subscription
    /// service), so it resolves the handle at call time like the inbox
    /// does.
    pub fn engine_handle(&self) -> Option<Arc<Engine<ServeLoopDriver>>> {
        self.engine.get().cloned()
    }

    /// Installs the per-spawn marker map written on spawned worker sessions' `session/created`
    /// (first write only; a later call is ignored).
    pub fn set_bridge_markers(&self, markers: BridgeMarkers) {
        let _ = self.markers.set(markers);
    }

    /// Installs the engine. Fails (returns the engine back) if one is
    /// already installed — exactly one engine per port, ever.
    pub fn install(
        &self,
        engine: Arc<Engine<ServeLoopDriver>>,
    ) -> Result<(), Arc<Engine<ServeLoopDriver>>> {
        self.engine.set(engine)
    }
}

impl Default for EngineInbox {
    fn default() -> Self {
        Self {
            engine: OnceLock::new(),
            markers: OnceLock::new(),
        }
    }
}

impl kian_superagent::SpawnSessionWriter for EngineInbox {
    fn create_session(
        &self,
        session: SessionId,
        assignee: Option<&str>,
    ) -> BoxFuture<'static, Result<(), kian_superagent::InboxWriteError>> {
        let engine = self
            .engine
            .get()
            .expect("engine installed before the first spawn")
            .clone();
        // (ADR-049) the marker is resolved from the same spawn identity the side selection uses:
        // a resolved roster entry always carries its own class (a kian-loop child stays `{}`).
        let payload = self
            .markers
            .get()
            .cloned()
            .unwrap_or_default()
            .for_spawn(assignee);
        Box::pin(async move {
            engine
                .create_session_with(session, payload)
                .await
                .map(|_| ())
                .map_err(|e| kian_superagent::InboxWriteError::Write(e.to_string()))
        })
    }
}

impl InboxWriter for EngineInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = self
            .engine
            .get()
            .expect("engine installed before the first admit")
            .clone();
        Box::pin(async move {
            let outcome = engine
                .admit(session, payload, idempotency_key)
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.engine
            .get()
            .expect("engine installed before the first wake")
            .wake(session)
    }
}

// ---------------------------------------------------------------------------
// The per-hold StreamPort over DrainCtx (the shape)
// ---------------------------------------------------------------------------

/// Assembly's [`StreamPort`]: one per execution hold, built from the
/// hold's [`DrainCtx`]. Writes carry the hold's epoch, so a stale
/// port's writes are fenced out by the store (NoSplitBrainWrite).
struct DrainCtxPort(DrainCtx);

impl StreamPort for DrainCtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: serde_json::Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.events()
                .await
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }
}

/// The per-hold [`StreamPort`] factory (public: the plugins build the
/// Router/Worker decorators with it).
pub fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(DrainCtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

// ---------------------------------------------------------------------------
// The AnswerPort over the engine's confirmation plane
// ---------------------------------------------------------------------------

/// [`kian_superagent::answers::AnswerPort`] over the engine's own
/// confirmation write (CONFIRMATION-MODEL §14d phase 3a): an answer
/// arriving on an IM channel takes **exactly** the path a CLI/HTTP
/// answer takes — one reserved control row, the same pending-set check,
/// the same TTL rewrite and the same `not_pending` for a racing
/// decision. Nothing here can fabricate or bypass a decision.
///
/// The engine is resolved through the inbox at call time for the same
/// reason the inbox itself is late-bound: the surface is constructed
/// before the engine exists.
pub struct EngineAnswers {
    inbox: Arc<EngineInbox>,
    ttl_ms: u64,
}

impl EngineAnswers {
    pub fn new(inbox: Arc<EngineInbox>, ttl_ms: u64) -> Arc<Self> {
        Arc::new(Self { inbox, ttl_ms })
    }
}

impl kian_superagent::answers::AnswerPort for EngineAnswers {
    fn answer(
        &self,
        session: SessionId,
        call_id: String,
        decision: kian_core::control::ConfirmDecision,
        reason: Option<String>,
        requested_by: String,
    ) -> BoxFuture<
        'static,
        Result<kian_superagent::answers::AnswerOutcome, kian_superagent::answers::AnswerError>,
    > {
        let engine = self
            .inbox
            .engine_handle()
            .expect("engine installed before the first answer");
        let ttl_ms = self.ttl_ms;
        Box::pin(async move {
            match engine
                .confirm(
                    session,
                    &call_id,
                    decision,
                    reason,
                    Some(requested_by),
                    ttl_ms,
                )
                .await
            {
                Ok(kian_orchestrator::ConfirmOutcome::Accepted) => {
                    Ok(kian_superagent::answers::AnswerOutcome::Accepted)
                }
                Ok(kian_orchestrator::ConfirmOutcome::NotPending) => {
                    Ok(kian_superagent::answers::AnswerOutcome::NotPending)
                }
                Ok(kian_orchestrator::ConfirmOutcome::Expired) => {
                    Ok(kian_superagent::answers::AnswerOutcome::Expired)
                }
                Ok(kian_orchestrator::ConfirmOutcome::UnknownSession) => {
                    Ok(kian_superagent::answers::AnswerOutcome::UnknownSession)
                }
                Err(e) => Err(kian_superagent::answers::AnswerError::Write(e.to_string())),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// The RecallPort over the store's public scan (the read surface)
// ---------------------------------------------------------------------------

/// Assembly's [`kian_superagent::RecallPort`]: cross-session reads
/// served over the store's existing public scan API — reads only, no
/// execution surface, no writes. Holds the disposable spill index (a
/// location cache rebuilt at startup; the spilled raws themselves live
/// only in the streams — constraint 1).
pub struct StoreRecall {
    store: EventStore,
    spill: Mutex<kian_superagent::SpillIndex>,
}

impl StoreRecall {
    /// A fresh port over `store` with an empty spill index (the
    /// startup rebuild fills it).
    pub fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            spill: Mutex::new(kian_superagent::SpillIndex::new()),
        })
    }

    /// Rebuilds the spill index at startup: scan the Router stream,
    /// then every worker stream it names (the [`worker_sessions`]
    /// derivation — the same universe `PassthroughRegistry::rebuild`
    /// scans), and fold every `input/spilled` event into the index.
    /// Idempotent and disposable: drop the index and run it again over
    /// the same log and the same map comes back.
    ///
    /// v1 boundary: the scanned sessions are the bundle's whole
    /// channel-fed universe. `input/spilled` events are written only by
    /// the drain's consumption of a spill envelope row, and those rows
    /// are admitted only by the subscription service — to the Router
    /// or to spawned workers. A spill on a session unreachable from
    /// the Router stream therefore cannot exist today; if a future
    /// route widens admission, this scan list widens with it.
    ///
    /// `chain` is the re-arm chain — ended frozen predecessors
    /// first, then the live resident session (see
    /// [`RouterProfile::re_arm`]); predecessor streams still carry
    /// spill envelopes the index must serve.
    pub async fn rebuild_spill_index(&self, chain: &[SessionId]) -> Result<(), String> {
        let mut events = Vec::new();
        for session in chain {
            let part = self.store.rebuild(*session).await.map_err(|e| {
                format!(
                    "spill index rebuild (resident {session}): {e}",
                    session = session.0
                )
            })?;
            events.extend(part);
        }
        for worker in worker_sessions(&events) {
            let worker_events = self
                .store
                .rebuild(worker)
                .await
                .map_err(|e| format!("spill index rebuild (worker): {e}"))?;
            events.extend(worker_events);
        }
        let index = kian_superagent::SpillIndex::rebuild(&events);
        tracing::info!(
            spills = index.len(),
            "superagent: spill index rebuilt from streams"
        );
        *self.spill.lock().expect("spill index lock") = index;
        Ok(())
    }
}

impl kian_superagent::RecallPort for StoreRecall {
    fn read_events(
        &self,
        session: SessionId,
        since_seq: u64,
        until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::RecallPortError>> {
        let store = self.store.clone();
        Box::pin(async move {
            if since_seq >= until_seq {
                // An empty window is an honest empty result, not an
                // error — the port refuses nothing read-only.
                return Ok(Vec::new());
            }
            store
                .scan(session, since_seq..until_seq)
                .await
                .map_err(|e| kian_superagent::RecallPortError::Read(e.to_string()))
        })
    }

    fn list_threads(
        &self,
        parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, kian_superagent::RecallPortError>>
    {
        let store = self.store.clone();
        Box::pin(async move {
            let events = store
                .rebuild(parent)
                .await
                .map_err(|e| kian_superagent::RecallPortError::Read(e.to_string()))?;
            Ok(kian_superagent::thread_bindings(&events))
        })
    }

    fn spill_location(
        &self,
        event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, kian_superagent::RecallPortError>>
    {
        let located = self
            .spill
            .lock()
            .expect("spill index lock")
            .lookup(event_id);
        Box::pin(async move { Ok(located) })
    }
}

// ---------------------------------------------------------------------------
// The WorkerStreamsPort over the store's public rebuild (the D9 read
// surface — the StoreRecall precedent)
// ---------------------------------------------------------------------------

/// Assembly's [`kian_superagent::WorkerStreamsPort`]: worker-stream
/// reads served over the store's existing public rebuild API — reads
/// only, no execution surface, no writes. The Router tools'
/// exclusivity check (`busy_owner`) reads through this port and
/// nothing else.
pub struct StoreWorkerStreams {
    store: EventStore,
}

impl StoreWorkerStreams {
    /// A fresh port over `store`.
    pub fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self { store })
    }
}

impl kian_superagent::WorkerStreamsPort for StoreWorkerStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        let store = self.store.clone();
        Box::pin(async move {
            store
                .rebuild(session)
                .await
                .map_err(|e| WorkerStreamsError(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// The WorkerDirectoryPort over the runtime directory + the durable
// registry read (the R12 S4 read surface — the StoreWorkerStreams
// precedent)
// ---------------------------------------------------------------------------

/// Assembly's [`kian_superagent::WorkerDirectoryPort`]: static config
/// ids ∪ the runtime membership directory ∪ the durable
/// `endpoint_registry` rows (the registry half keeps a known-but-
/// offline worker bindable after a kian restart — rows are never
/// pruned). Reads only: no writes, no wakes, no membership mutation.
///
/// State derivation (spec M3/L2): a directory member computes through
/// `kian_exec`'s pure `member_state` (online iff connected AND fresh);
/// an id known only from its registry row uses the CLI conjunction
/// (online iff the row says `online` AND the heartbeat is fresh).
/// `description`/`version` are runtime facts: absent until the worker
/// (re-)registers (spec M4 — no migration, honest absence).
pub struct StoreWorkerDirectory {
    backend: Arc<kian_exec::RemoteExecBackend>,
    directory: Arc<kian_exec::WorkerDirectory>,
    /// The static pins (config order) — descriptions and per-id
    /// liveness bounds.
    endpoints: Vec<crate::config::EndpointConfig>,
    /// `workers.offline_after_ms` — the liveness bound for dynamic
    /// ids.
    workers_offline_after_ms: u64,
}

fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl StoreWorkerDirectory {
    /// A fresh port over the shared backend, the runtime directory
    /// and the static config.
    pub fn new(
        backend: Arc<kian_exec::RemoteExecBackend>,
        directory: Arc<kian_exec::WorkerDirectory>,
        endpoints: Vec<crate::config::EndpointConfig>,
        workers_offline_after_ms: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            directory,
            endpoints,
            workers_offline_after_ms,
        })
    }

    /// The per-id liveness bound: the static pin's own
    /// `offline_after_ms` when configured, else the `workers:` bound.
    fn offline_after(&self, id: &str) -> u64 {
        self.endpoints
            .iter()
            .find(|ep| ep.id == id)
            .map(|ep| ep.offline_after_ms)
            .unwrap_or(self.workers_offline_after_ms)
    }

    /// The one snapshot both `members()` and `member()` derive from.
    async fn snapshot(
        &self,
    ) -> Result<Vec<kian_superagent::MemberView>, kian_superagent::WorkerDirectoryError> {
        let rows = self
            .backend
            .store()
            .endpoints_lenient()
            .await
            .map_err(|e| kian_superagent::WorkerDirectoryError(format!("registry read: {e}")))?;
        let now = wall_clock_ms();
        let directory = self.directory.snapshot();
        let mut views: std::collections::BTreeMap<String, kian_superagent::MemberView> =
            std::collections::BTreeMap::new();
        // registry rows first (the durable floor: every id ever seen)
        for row in rows {
            let offline_after = self.offline_after(&row.endpoint_id);
            let fresh = now.saturating_sub(row.last_heartbeat_ms) <= offline_after;
            let state = if row.state == "online" && fresh {
                kian_superagent::MemberState::Online
            } else {
                kian_superagent::MemberState::Offline
            };
            views.insert(
                row.endpoint_id.clone(),
                kian_superagent::MemberView {
                    id: row.endpoint_id.clone(),
                    description: row.description.clone(),
                    version: row.protocol_version,
                    capabilities: row.capabilities,
                    declaration_stale: true,
                    state,
                    last_seen_ms: row.last_heartbeat_ms,
                },
            );
        }
        // the runtime directory wins where it knows the id (it also
        // carries description/version and the draining window)
        for (id, member) in directory {
            let offline_after = self.offline_after(&id);
            let state = match kian_exec::member_state(&member, now, offline_after) {
                kian_exec::MemberState::Online => kian_superagent::MemberState::Online,
                kian_exec::MemberState::Draining => kian_superagent::MemberState::Draining,
                kian_exec::MemberState::Offline => kian_superagent::MemberState::Offline,
            };
            let capabilities = member.capabilities;
            views.insert(
                id.clone(),
                kian_superagent::MemberView {
                    id,
                    description: member.description,
                    version: Some(member.version),
                    capabilities,
                    declaration_stale: false,
                    state,
                    last_seen_ms: member.last_seen_ms,
                },
            );
        }
        // static pins are known even before any registry row exists
        // (a fresh database): offline until their boot-seeded row says
        // otherwise — but their config description is the known text.
        for ep in &self.endpoints {
            views
                .entry(ep.id.clone())
                .and_modify(|view| {
                    if view.description.is_none() {
                        view.description = ep.description.clone();
                    }
                })
                .or_insert_with(|| kian_superagent::MemberView {
                    id: ep.id.clone(),
                    description: ep.description.clone(),
                    version: None,
                    capabilities: Vec::new(),
                    declaration_stale: false,
                    state: kian_superagent::MemberState::Offline,
                    last_seen_ms: 0,
                });
        }
        Ok(views.into_values().collect())
    }
}

impl kian_superagent::WorkerDirectoryPort for StoreWorkerDirectory {
    fn known_ids(
        &self,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Vec<String>, kian_superagent::WorkerDirectoryError>,
    > {
        let this = self.clone_shallow();
        Box::pin(async move {
            let views = this.snapshot().await?;
            Ok(views.into_iter().map(|v| v.id).collect())
        })
    }

    fn members(
        &self,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Vec<kian_superagent::MemberView>, kian_superagent::WorkerDirectoryError>,
    > {
        let this = self.clone_shallow();
        Box::pin(async move { this.snapshot().await })
    }

    fn member(
        &self,
        id: &str,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Option<kian_superagent::MemberView>, kian_superagent::WorkerDirectoryError>,
    > {
        let this = self.clone_shallow();
        let id = id.to_string();
        Box::pin(async move {
            let views = this.snapshot().await?;
            Ok(views.into_iter().find(|v| v.id == id))
        })
    }
}

// A plain clone helper over the Arc fields (the port methods need
// 'static futures; the struct itself is only ever held in an Arc).
impl StoreWorkerDirectory {
    fn clone_shallow(&self) -> Arc<Self> {
        Arc::new(Self {
            backend: Arc::clone(&self.backend),
            directory: Arc::clone(&self.directory),
            endpoints: self.endpoints.clone(),
            workers_offline_after_ms: self.workers_offline_after_ms,
        })
    }
}

// ---------------------------------------------------------------------------
// Per-worker routing selection (ADR-020, D8/D15/D16 — I2)
// ---------------------------------------------------------------------------

/// The pure routing decision over (stream binding, configured
/// worker ids) — the spec §4 selection table. A pure function: no
/// side effects, no reads, the same inputs always yield the same
/// decision (rebuild == replay).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionKind<'a> {
    /// Bound to a configured worker — its prebuilt side runs.
    Bound(&'a str),
    /// Legacy unbound on a workers-configured server — the FIRST
    /// endpoint (the legacy `CombinedTools` first-wins outcome), with
    /// one audit line per session; never registered as bound.
    LegacyFirst(&'a str),
    /// Bound to an id that is no longer configured — refused (D16):
    /// neither re-routed nor run locally.
    BoundUnconfigured(&'a str),
    /// Malformed binding on the stream — refused honestly.
    Malformed,
    /// Zero configured workers — today's local-exec path, byte-identical.
    NoWorkers,
}

/// Selects the routing decision for one worker session (spec §4's
/// selection table, pure). "No binding must not default to a bound
/// worker": an unbound session routes to the *first* endpoint
/// (legacy), never to whichever side happens to be prebuilt first.
#[must_use]
pub fn select_side<'a>(
    binding: &'a WorkerBinding<'_>,
    configured_ids: &'a [String],
) -> SelectionKind<'a> {
    match binding {
        WorkerBinding::Bound(id) => {
            let id: &str = id.as_ref();
            if configured_ids.iter().any(|configured| configured == id) {
                SelectionKind::Bound(id)
            } else {
                SelectionKind::BoundUnconfigured(id)
            }
        }
        WorkerBinding::Unbound => match configured_ids.first() {
            Some(first) => SelectionKind::LegacyFirst(first.as_str()),
            None => SelectionKind::NoWorkers,
        },
        WorkerBinding::Malformed => SelectionKind::Malformed,
    }
}

/// Builds one per-worker-id side on first use (R12, S1 — the lazy
/// factory replacing ADR-020's boot-prebuilt loop): the same tool set
/// R11 built at boot for a configured endpoint — that worker's
/// `RemoteExecTool` (one `exec`, unchanged schema), its own async pair
/// bound to that id (S3), the shared recall toolpack (when enabled),
/// the filtered local tools. The `description` parameter is part of
/// the pinned seam (spec §5.1); the side's tool surface does not vary
/// with it (S5), so the assembly passes it through unused or `None`.
pub type WorkerSideFactory = Arc<
    dyn Fn(&str, Option<&str>) -> WorkerLoopDriver<crate::superagent::WorkerInnerDriver>
        + Send
        + Sync,
>;

/// (R17-C3, pin P2-2) Builds one **per-assignee** side for a roster
/// entry id: the entry's resolved `AgentDef` (slots and budgets from
/// the `agents:` definition, pin P2-1) with
/// `tools = entry.agent.tools ∩ entry.tools_allow` — a structural
/// narrowing at the provider edge, so a denied tool is absent from
/// the request schema, never merely refused at call time (§5.1,
/// M3). An assignee with no live entry gets a **refusing** side (the
/// [`Refusal::VanishedAssignee`] row): no working tool, the honest
/// reason, never a silent fallback onto another agent's surface.
pub type AssigneeSideFactory =
    Arc<dyn Fn(&str) -> WorkerLoopDriver<crate::superagent::WorkerInnerDriver> + Send + Sync>;

/// The selection input for one drive (R12, S2): the static config ids
/// (config order — the legacy-unbound first rule) ∪ the directory
/// port's known ids, appended in the port's id-ascending order. One
/// snapshot per drive; the Bound row matches against the full set,
/// the legacy-unbound row against the static prefix only (see
/// `SuperAgentDriver::drive`).
#[must_use]
pub fn known_ids_snapshot(static_order: &[String], port_ids: &[String]) -> Vec<String> {
    let mut known = static_order.to_vec();
    for id in port_ids {
        if !known.contains(id) {
            known.push(id.clone());
        }
    }
    known
}

/// Why a worker session's `exec` is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Bound to a worker id that is no longer configured (D16). The
    /// message names the id.
    Unconfigured(String),
    /// The stream's binding is malformed (D2) — no id exists to name.
    Malformed,
    /// (R17-C3, pin P2-2) The committed wrapper names a roster
    /// assignee that has no live roster entry (§2.1.1:270-274). The
    /// message names the assignee; the side never widens onto any
    /// other agent's tools.
    VanishedAssignee(String),
}

impl Refusal {
    /// The honest tool error text (spec §4 / D16, verbatim shapes). The
    /// exec spelling is frozen; the fs family says "fs refused" and
    /// carries the same binding facts.
    #[must_use]
    pub fn message_for(&self, tool: &str) -> String {
        let family = if tool == "exec" { "exec" } else { "fs" };
        match self {
            Self::Unconfigured(id) => format!(
                "{family} refused: this session is bound to execution worker `{id}`, which is not \
                 configured on this server — no command was queued and nothing ran elsewhere"
            ),
            Self::Malformed => format!(
                "{family} refused: this session carries a malformed `exec_worker` binding on its \
                 stream — no command was queued and nothing ran elsewhere"
            ),
            Self::VanishedAssignee(id) => format!(
                "{family} refused: this session's committed delegation names roster assignee \
                 `{id}`, which has no live roster entry — no command was queued and nothing ran \
                 elsewhere"
            ),
        }
    }

    /// The frozen exec refusal text.
    #[must_use]
    pub fn message(&self) -> String {
        self.message_for("exec")
    }
}

/// Builds the refusing side for one drive (assembly glue: the factory
/// captures the loop ingredients; the driver calls it only on the
/// refusal rows, which are rare and never on the hot path).
pub type RefuseSideFactory =
    Arc<dyn Fn(&Refusal) -> WorkerLoopDriver<crate::superagent::WorkerInnerDriver> + Send + Sync>;

/// The refusing `exec` tool (assembly glue, D16/D2): mounts the
/// single name `exec` with the exact schema the remote tool exposes
/// (prompt-prefix neutral), and every invoke returns the honest
/// refusal. It holds **no backend handle** — structurally incapable
/// of enqueueing a command anywhere (no queue row, no endpoint
/// dispatch, no silent failover).
pub struct RefusingExecTool {
    refusal: Refusal,
}

impl RefusingExecTool {
    #[must_use]
    pub fn new(refusal: Refusal) -> Self {
        Self { refusal }
    }

    /// The refusal this tool answers with (tests).
    #[must_use]
    pub fn refusal(&self) -> &Refusal {
        &self.refusal
    }
}

impl ToolProvider for RefusingExecTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        // Byte-identical to the RemoteExecTool schema: one uniform
        // `exec` surface (name and schema unchanged — D8).
        vec![ToolSchema {
            name: "exec".into(),
            description: "Run a command on the remote execution endpoint (argv vector, no shell)."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "argv": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "command and arguments"
                    },
                    "cwd": {"type": "string"},
                    "timeout_ms": {"type": "integer"}
                },
                "required": ["argv"]
            }),
        }]
    }

    fn invoke(&self, _call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let message = self.refusal.message();
        Box::pin(async move { Err(ToolError::Execution(message)) })
    }
}

/// The refusing fs tool (assembly glue, D16/D2): mounts the three v2 fs
/// names with the exact schemas the remote fs tool exposes, and every
/// invoke returns the honest refusal. Structurally holds no backend handle
/// — nothing can be enqueued on a refusal row.
pub struct RefusingFsTool {
    refusal: Refusal,
}

impl RefusingFsTool {
    #[must_use]
    pub fn new(refusal: Refusal) -> Self {
        Self { refusal }
    }

    /// The refusal this tool answers with (tests).
    #[must_use]
    pub fn refusal(&self) -> &Refusal {
        &self.refusal
    }
}

impl ToolProvider for RefusingFsTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        kian_exec::RemoteFsTool::fs_schemas()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let message = self.refusal.message_for(&call.name);
        Box::pin(async move { Err(ToolError::Execution(message)) })
    }
}

// ---------------------------------------------------------------------------
// The assembled driver
// ---------------------------------------------------------------------------

/// The driver every assembled engine runs: the built-in plain loop, or
/// the super-agent three-way router (built-in behavior with the bundle
/// mounted), or the dream-routing memory assembly (a `memory:` section;
/// ordinary sessions keep the plain loop's exact handle). All variants
/// share `Ctx = DrainCtx`.
pub enum ServeLoopDriver {
    /// built-in: one loop, the default agent (unchanged behavior).
    Plain(Arc<ReActLoop>),
    /// Super-agent: resident Router / spawned workers / plain sessions.
    SuperAgent(Arc<SuperAgentDriver>),
    /// Agent memory (round 8): plain sessions unchanged, the dreamer
    /// agent's DreamDriver for sessions carrying a dream request.
    Memory(Arc<crate::memory::MemoryLoopDriver>),
}

// ---------------------------------------------------------------------------
// the dsh bridge mount — the DshCtx port over the real DrainCtx
// ---------------------------------------------------------------------------

/// Assembly's [`kian_dsh::DshCtx`] over the engine's `DrainCtx`: the
/// fenced append path, the safe-boundary input take, and the read-only
/// stream — the bridge reaches nothing else (no store, no end write).
/// A newtype because the orphan rule puts the port impl in the crate
/// that owns neither side's foreign halves — this one owns the
/// wrapper.
pub struct DrainDshCtx(DrainCtx);

impl kian_dsh::DshCtx for DrainDshCtx {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn epoch(&self) -> kian_core::model::Epoch {
        self.0.epoch()
    }

    fn in_turn(&self) -> bool {
        self.0.in_turn()
    }

    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, kian_dsh::DshCtxError>> {
        let fut = self.0.events();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn append(
        &self,
        kind: EventKind,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.append(kind, bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn begin_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.begin_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn end_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.end_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn take_eligible_inputs(
        &self,
    ) -> BoxFuture<'_, Result<Vec<kian_core::model::InputRow>, kian_dsh::DshCtxError>> {
        let fut = self.0.take_eligible_inputs();
        Box::pin(async move {
            fut.await.map_err(|e| match e {
                kian_orchestrator::EngineError::NotAtSafeBoundary { .. } => {
                    kian_dsh::DshCtxError::NotAtSafeBoundary
                }
                other => kian_dsh::DshCtxError::Failure(other.to_string()),
            })
        })
    }

    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, kian_dsh::DshCtxError>> {
        let fut = self.0.has_eligible_inputs();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    // interrupt_requested: the engine's interrupt flag is not visible
    // from DrainCtx; v1 relies on the engine honoring it at outcome
    // boundaries (the mounted bridge never forwards an `interrupt`
    // control frame mid-episode). The contract frame itself is
    // exercised by the kian-dsh driver suite.
}

/// The `kian_harness::BridgeCtx` impl over the real `DrainCtx` (HARNESS-BRIDGE §4): the shared
/// skeleton's drain port, mirroring the dsh port above.
pub struct DrainBridgeCtx(pub DrainCtx);

impl kian_harness::BridgeCtx for DrainBridgeCtx {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn epoch(&self) -> kian_core::model::Epoch {
        self.0.epoch()
    }

    fn in_turn(&self) -> bool {
        self.0.in_turn()
    }

    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, kian_harness::BridgeCtxError>> {
        let fut = self.0.events();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_harness::BridgeCtxError::Failure(e.to_string()))
        })
    }

    fn append(
        &self,
        kind: EventKind,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_harness::BridgeCtxError>> {
        let fut = self.0.append(kind, bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_harness::BridgeCtxError::Failure(e.to_string()))
        })
    }

    fn begin_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_harness::BridgeCtxError>> {
        let fut = self.0.begin_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_harness::BridgeCtxError::Failure(e.to_string()))
        })
    }

    fn end_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_harness::BridgeCtxError>> {
        let fut = self.0.end_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_harness::BridgeCtxError::Failure(e.to_string()))
        })
    }

    fn take_eligible_inputs(
        &self,
    ) -> BoxFuture<'_, Result<Vec<kian_core::model::InputRow>, kian_harness::BridgeCtxError>> {
        let fut = self.0.take_eligible_inputs();
        Box::pin(async move {
            fut.await.map_err(|e| match e {
                kian_orchestrator::EngineError::NotAtSafeBoundary { .. } => {
                    kian_harness::BridgeCtxError::NotAtSafeBoundary
                }
                other => kian_harness::BridgeCtxError::Failure(other.to_string()),
            })
        })
    }

    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, kian_harness::BridgeCtxError>> {
        let fut = self.0.has_eligible_inputs();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_harness::BridgeCtxError::Failure(e.to_string()))
        })
    }
}

/// The worker template's inner loop, config-gated: the built-in
/// ReAct loop (default), the dsh bridge when `superagent.harness.dsh`
/// is on, or the shared bridge driver when `harness.claude`/`harness.codex`
/// is on. The decorator (`WorkerLoopDriver`) and the plain/router sides
/// are unchanged either way.
pub enum WorkerInnerDriver {
    React(Arc<ReActLoop>),
    Dsh(Arc<kian_dsh::DshDriver>),
    Bridge(Arc<kian_harness::BridgeDriver>),
}

impl LoopDriver for WorkerInnerDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        match self {
            Self::React(loop_) => loop_.drive(ctx),
            Self::Dsh(driver) => {
                driver.drive(Arc::new(DrainDshCtx(ctx)) as Arc<dyn kian_dsh::DshCtx>)
            }
            Self::Bridge(driver) => {
                driver.drive(Arc::new(DrainBridgeCtx(ctx)) as Arc<dyn kian_harness::BridgeCtx>)
            }
        }
    }
}

/// (ADR-049) The per-spawn `session/created` class markers: the default worker template's choice
/// plus one per roster assignee (absent = the built-in kian loop's historical `{}`).
#[derive(Debug, Clone, Default)]
pub struct BridgeMarkers {
    /// The marker for a spawn with no roster entry (the default worker template).
    pub default: Option<serde_json::Value>,
    /// The marker for a spawn that resolved through a roster entry, keyed by entry id.
    pub by_assignee: std::collections::BTreeMap<String, serde_json::Value>,
}

impl BridgeMarkers {
    /// The `session/created` payload for one spawn. A resolved roster entry always decides its own
    /// class — a kian-loop child writes `{}`, never the default template's bridge marker (the
    /// pre-ADR-049 mixed-config lie this ADR fixes).
    #[must_use]
    pub fn for_spawn(&self, assignee: Option<&str>) -> serde_json::Value {
        match assignee {
            Some(id) => self
                .by_assignee
                .get(id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
            None => self
                .default
                .clone()
                .unwrap_or_else(|| serde_json::json!({})),
        }
    }
}

/// (ADR-049) The assembled worker-loop mounts: the built-in kian loop plus every declared
/// external harness, the default worker template's choice and each roster entry's choice.
#[derive(Clone)]
pub struct HarnessMounts {
    /// Every declared non-dsh bridge driver, keyed by the harness the templates select.
    pub bridges:
        std::collections::BTreeMap<crate::config::HarnessKey, Arc<kian_harness::BridgeDriver>>,
    /// The declared dsh driver (shared across every side that selects it).
    pub dsh: Option<Arc<kian_dsh::DshDriver>>,
    /// The default worker template's inner loop.
    pub worker: crate::config::HarnessKey,
    /// Each roster entry's inner loop, keyed by entry id.
    pub assignees: std::collections::BTreeMap<String, crate::config::HarnessKey>,
    /// The per-spawn class markers riding `session/created`.
    pub markers: BridgeMarkers,
    /// Whether any selection was explicit (per-agent mode; absent = the legacy single-harness
    /// shape, byte-compatible).
    pub per_agent: bool,
}

impl HarnessMounts {
    /// The external harness inner loop for one choice; `None` = the built-in kian loop (the
    /// caller supplies the per-side `ReActLoop`).
    #[must_use]
    pub fn bridged_inner(&self, key: crate::config::HarnessKey) -> Option<WorkerInnerDriver> {
        match key {
            crate::config::HarnessKey::Kian => None,
            crate::config::HarnessKey::Dsh => Some(WorkerInnerDriver::Dsh(Arc::clone(
                self.dsh
                    .as_ref()
                    .expect("a selected dsh harness has a build mount"),
            ))),
            crate::config::HarnessKey::Claude | crate::config::HarnessKey::Codex => {
                Some(WorkerInnerDriver::Bridge(Arc::clone(
                    self.bridges
                        .get(&key)
                        .expect("a selected bridge has a built mount"),
                )))
            }
        }
    }
}

impl LoopDriver for ServeLoopDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        match self {
            Self::Plain(loop_) => loop_.drive(ctx),
            Self::SuperAgent(driver) => driver.drive(ctx),
            Self::Memory(driver) => driver.drive(ctx),
        }
    }
}

/// One engine, one driver, three routings:
///
/// - the configured Router session → [`RouterDriver`] (residency);
/// - a session with Thread lineage → [`WorkerLoopDriver`] (lineage,
///   evidence gate, completion report);
/// - anything else → the default agent's loop (built-in semantics for
///   plain sessions).
///
/// Worker lineage is decided from the stream itself (constraint 1: no
/// resident routing state), so a restarted server classifies a
/// takeover hold identically.
pub struct SuperAgentDriver {
    /// ADR-040 phase 1: the live resident binding (routing reads it per
    /// drive, so a rotation routes the successor to the Router side).
    router: Arc<kian_superagent::ResidentBinding>,
    worker_version: u64,
    router_side: RouterDriver<ReActLoop>,
    worker_side: WorkerLoopDriver<crate::superagent::WorkerInnerDriver>,
    default_side: Arc<ReActLoop>,
    /// The resident watchdog session, when configured: routed to
    /// its own driver (the same residency shape as the Router's) so
    /// each admitted tick is one judge turn.
    watchdog: Option<SessionId>,
    watchdog_side: Option<RouterDriver<ReActLoop>>,
    /// (R12, S1 — supersedes ADR-020 D8's boot-prebuilt map) the lazy
    /// per-worker-id side cache: built on first use from the factory,
    /// selected per drive from the stream's binding, and **never
    /// evicted** (queue-first: a side's `exec` enqueues by id, so an
    /// offline worker's side keeps working; eviction would only risk
    /// a rebuild storm on a flapping worker — A13).
    worker_sides:
        Arc<Mutex<BTreeMap<String, WorkerLoopDriver<crate::superagent::WorkerInnerDriver>>>>,
    /// (R17-C3, pin P2-2) the per-**assignee** side cache: keyed by
    /// the roster entry id the committed wrapper names, built lazily
    /// on first use and cached forever — the same never-evict
    /// reasoning as `worker_sides` (the side's exec enqueues by id;
    /// eviction only risks a rebuild storm). The surface is therefore
    /// stable for the life of the child: post-spawn roster edits never
    /// re-resolve it (constraint 13, §2.1.1).
    assignee_sides:
        Arc<Mutex<BTreeMap<String, WorkerLoopDriver<crate::superagent::WorkerInnerDriver>>>>,
    /// The side factory (S1). `None` (zero `endpoints:` and no
    /// `workers:`) = the feature is off and `worker_side` above is
    /// today's exact path, byte-identical.
    side_factory: Option<WorkerSideFactory>,
    /// (R17-C3, pin P2-2) the per-assignee factory — installed iff the
    /// roster is non-empty (pin P2-6). `None` = no roster, and no
    /// wrapper ever carries an `assignee` key, so the selection below
    /// is unreachable (the arm degrades to a warn + today's path).
    assignee_side_factory: Option<AssigneeSideFactory>,
    /// The static pins in **config order** (`endpoints` file order).
    /// The legacy-unbound row routes to the first static pin (B1);
    /// a bound row matches the statics ∪ the port's known ids.
    worker_order: Vec<String>,
    /// (R12, S2): the bundle-owned directory port — the known-ids
    /// snapshot source per drive (static ∪ live directory ∪ registry
    /// rows). `None` = static ids only (tests without the wiring).
    known_ids_port: Option<Arc<dyn kian_superagent::WorkerDirectoryPort>>,
    /// (ADR-020, D16/D2) builds the refusing side (a worker loop
    /// whose only `exec` is the honest refusal) for the
    /// unconfigured-bound and malformed rows. `None` when no factory
    /// is installed (feature off).
    refuse_side: Option<RefuseSideFactory>,
    /// (ADR-020, D15) observability-only dedup so the
    /// `legacy unbound` audit line logs **once per session** per
    /// process. Never read for a routing decision — the selection
    /// itself stays a pure function of (stream binding,
    /// configured ids).
    legacy_unbound_audited: Arc<Mutex<BTreeSet<SessionId>>>,
    /// (R17-C6, pin P4-1): the `superagent.board.gating` class-A
    /// emission switch — passed to the child-side `lineage_plan` so
    /// a gated child emits its `thread/started` audit keys.
    board_gating: kian_superagent::BoardGating,
    /// The job conductor side (JOB-MODEL §5.3): a session whose stream
    /// carries a job marker on an admitted input runs under it. `None` =
    /// job runs are not mounted; such a stream settles honestly instead of
    /// falling through to the default agent loop over a marker payload.
    job_side: Option<Arc<kian_superagent::job::conduct::JobLoopDriver<DrainCtx>>>,
}

impl SuperAgentDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<kian_superagent::ResidentBinding>,
        worker_version: u64,
        router_side: RouterDriver<ReActLoop>,
        worker_side: WorkerLoopDriver<crate::superagent::WorkerInnerDriver>,
        default_side: Arc<ReActLoop>,
    ) -> Self {
        Self {
            router,
            worker_version,
            router_side,
            worker_side,
            default_side,
            watchdog: None,
            watchdog_side: None,
            worker_sides: Arc::new(Mutex::new(BTreeMap::new())),
            assignee_sides: Arc::new(Mutex::new(BTreeMap::new())),
            side_factory: None,
            assignee_side_factory: None,
            worker_order: Vec::new(),
            known_ids_port: None,
            refuse_side: None,
            legacy_unbound_audited: Arc::new(Mutex::new(BTreeSet::new())),
            board_gating: kian_superagent::BoardGating::default(),
            job_side: None,
        }
    }

    /// Installs the job conductor (JOB-MODEL §5.3). Additive: without it a
    /// job-marked stream settles with a failed run verdict rather than being
    /// mistaken for a plain session.
    #[must_use]
    pub fn with_job_runs(
        mut self,
        side: kian_superagent::job::conduct::JobLoopDriver<DrainCtx>,
    ) -> Self {
        self.job_side = Some(Arc::new(side));
        self
    }

    /// Attaches the resident watchdog session and its driver.
    /// Additive: unconfigured (the default) keeps the legacy routing
    /// exactly.
    #[must_use]
    pub fn with_watchdog(mut self, session: SessionId, side: RouterDriver<ReActLoop>) -> Self {
        self.watchdog = Some(session);
        self.watchdog_side = Some(side);
        self
    }

    /// (R12, S1/S2 — supersedes ADR-020 D8's prebuilt map) attaches
    /// the lazy per-worker-id side factory, the static pins in
    /// **config order** (`endpoints` file order — the legacy-unbound
    /// row routes to the first static pin, B1), the refusing-side
    /// factory, and the directory port (the known-ids snapshot source
    /// per drive: static ∪ live directory ∪ registry rows — the
    /// registry half keeps a bound offline worker's side buildable
    /// after a kian restart). Additive: with no factory installed the
    /// driver routes exactly as before (the entire feature is behind
    /// the endpoints-or-workers presence discriminator).
    #[must_use]
    pub fn with_lazy_worker_sides(
        mut self,
        side_factory: WorkerSideFactory,
        worker_order: Vec<String>,
        refuse_side: RefuseSideFactory,
        known_ids_port: Option<Arc<dyn kian_superagent::WorkerDirectoryPort>>,
    ) -> Self {
        self.side_factory = Some(side_factory);
        self.worker_order = worker_order;
        self.refuse_side = Some(refuse_side);
        self.known_ids_port = known_ids_port;
        self
    }

    /// (R17-C3, pin P2-2) attaches the per-assignee side factory —
    /// installed iff the roster is non-empty (pin P2-6). Additive:
    /// with no factory installed a wrapper never carries `assignee`
    /// (the roster wrote none), so routing is exactly as before.
    #[must_use]
    pub fn with_assignee_sides(mut self, assignee_side_factory: AssigneeSideFactory) -> Self {
        self.assignee_side_factory = Some(assignee_side_factory);
        self
    }

    /// (R17-C6, pin P4-1) installs the `superagent.board.gating`
    /// class-A emission switch — the child-side `lineage_plan` emits
    /// its audit keys only when it is on; the absent call (the
    /// default) keeps today's exact bytes.
    #[must_use]
    pub fn with_board_gating(mut self, gating: kian_superagent::BoardGating) -> Self {
        self.board_gating = gating;
        self
    }
}

impl LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        if ctx.session() == self.router.load() {
            return self.router_side.drive(ctx);
        }
        if let (Some(watchdog), Some(side)) = (self.watchdog, &self.watchdog_side)
            && ctx.session() == watchdog
        {
            return side.drive(ctx);
        }
        // Classify by stream content before the first provider turn:
        // worker streams carry the delegation wrapper (or already carry
        // thread/started); plain sessions see the default agent.
        //
        // (ADR-020, D8 → R12, S1/S2): a worker session selects ONE
        // side per drive from its own stream's binding — bound ⇒ that
        // worker's side, built lazily from the factory on first use
        // and cached forever (never evicted: an offline worker's side
        // enqueues by id — queue-first); legacy unbound ⇒ the first
        // STATIC pin with one audit line per session; unknown-bound
        // or malformed ⇒ the refusing side (never a fallback route,
        // never local exec); nothing known ⇒ today's local path.
        // With no factory installed (zero endpoints and no workers:)
        // the single `worker_side` below is today's exact path,
        // byte-identical.
        let worker = self.worker_side.clone();
        let job = self.job_side.clone();
        let default = Arc::clone(&self.default_side);
        let router = self.router.load();
        let worker_version = self.worker_version;
        let session = ctx.session();
        let sides = Arc::clone(&self.worker_sides);
        let side_factory = self.side_factory.clone();
        // (R17-C3, pin P2-2) the assignee-keyed halves: the cache and
        // the factory, cloned into the async selection block below.
        let assignee_sides = Arc::clone(&self.assignee_sides);
        let assignee_side_factory = self.assignee_side_factory.clone();
        // (B1) the legacy-unbound selection input is CONFIG order of
        // the STATIC pins — never alphabetical, never dynamic.
        let order = self.worker_order.clone();
        let port = self.known_ids_port.clone();
        let refuse_side = self.refuse_side.clone();
        let audited = Arc::clone(&self.legacy_unbound_audited);
        // (R17-C6, pin P4-1): the class-A switch, copied into the
        // async selection block below (the child-side keys).
        let board_gating = self.board_gating;
        futures::stream::once(async move {
            let events = ctx.events().await.unwrap_or_default();
            // (JOB-MODEL §5.3) A run is classified by the trusted marker on
            // its admitted input, never by content — so a run is decided
            // from the stream itself and a takeover classifies identically.
            // A malformed marker (`Err`) is still a run: the conductor owns
            // the honest `invalid_job_input` failure, so it must not fall
            // through to the agent loop either.
            if let Some(marker) = kian_superagent::job::status::marker(&events) {
                let digest = marker.ok().map(|marker| marker.plan_digest);
                return match job {
                    Some(side) => side.drive(ctx).boxed(),
                    None => crate::job_runtime::unmounted_run(ctx, digest),
                };
            }
            match lineage_plan(
                &events,
                session,
                router,
                worker_version,
                board_gating,
            ) {
                LineagePlan::PassThrough => default.drive(ctx).boxed(),
                plan @ (LineagePlan::Emit { .. } | LineagePlan::Resume { .. }) => {
                    // (R17-C3, pin P2-2) the assignee-keyed selection,
                    // BEFORE the endpoint binding: a wrapper carrying
                    // `assignee` (a roster spawn) routes to that
                    // assignee's side — built lazily from the factory
                    // on first use, cached forever (the surface is
                    // stable for the child's life; post-spawn roster
                    // edits never re-resolve it, constraint 13). The
                    // selection input is the **committed wrapper's**
                    // `assignee`, never a live-config re-resolution
                    // (§2.1.1); the factory itself owns the
                    // vanished-entry refusing row. A wrapper without
                    // `assignee` never enters this arm — today's path
                    // exactly (M8).
                    if let Some(assignee) = kian_superagent::stream_assignee_of(&events) {
                        let factory = match &assignee_side_factory {
                            Some(factory) => factory.clone(),
                            None => {
                                // Unreachable in production (a wrapper
                                // carries `assignee` only when the
                                // roster wrote it, and the factory is
                                // installed exactly then, pin P2-6) —
                                // but never widen silently: warn and
                                // take today's worker path.
                                tracing::warn!(
                                    target: "kian::superagent",
                                    session = %session.0,
                                    assignee = %assignee,
                                    "worker routing audit: assignee wrapper with no assignee \
                                     factory installed; routing to the worker template side"
                                );
                                return worker.drive(ctx).boxed();
                            }
                        };
                        let side = {
                            let mut cache = assignee_sides
                                .lock()
                                .expect("assignee side cache lock");
                            if !cache.contains_key(&assignee) {
                                cache.insert(assignee.clone(), factory(&assignee));
                            }
                            cache.get(&assignee).cloned().expect("just inserted")
                        };
                        tracing::debug!(
                            target: "kian::superagent",
                            session = %session.0,
                            assignee = %assignee,
                            resumed = matches!(plan, LineagePlan::Resume { .. }),
                            "worker routing audit: assignee side selected"
                        );
                        return side.drive(ctx).boxed();
                    }
                    let Some(factory) = side_factory else {
                        // Feature off: today's single worker side.
                        return worker.drive(ctx).boxed();
                    };
                    let binding = worker_binding_of(&events, session);
                    // (S2) one known-ids snapshot per drive: the
                    // static pins ∪ the directory port's known ids.
                    // A failed port read falls back to the static
                    // pins with one warn — a drive never refuses on
                    // an observability read.
                    let port_ids = match &port {
                        Some(port) => match port.known_ids().await {
                            Ok(ids) => ids,
                            Err(error) => {
                                tracing::warn!(
                                    target: "kian::superagent",
                                    %error,
                                    "worker routing audit: known-ids read failed; \
                                     selecting from the static pins only"
                                );
                                Vec::new()
                            }
                        },
                        None => Vec::new(),
                    };
                    let known = known_ids_snapshot(&order, &port_ids);
                    // Bound/malformed rows match the full known set;
                    // the legacy-unbound row matches the STATIC prefix
                    // only (S2: first static endpoint, or today's
                    // local path when nothing is configured).
                    let kind = match &binding {
                        WorkerBinding::Unbound => select_side(&binding, &order),
                        _ => select_side(&binding, &known),
                    };
                    // The never-evict cache lookup/build (S1): one
                    // side per id per process, built on first use.
                    let side_of = |id: &str| {
                        let mut cache = sides.lock().expect("worker side cache lock");
                        if !cache.contains_key(id) {
                            // The factory seam carries an optional
                            // description (spec §5.1); the side's tool
                            // surface does not vary with it (S5), so
                            // none is fetched here — no second port
                            // read on the drive path.
                            cache.insert(id.to_string(), factory(id, None));
                        }
                        cache.get(id).cloned().expect("just inserted")
                    };
                    match kind {
                        SelectionKind::Bound(id) => {
                            tracing::debug!(
                                target: "kian::superagent",
                                session = %session.0,
                                exec_worker = %id,
                                resumed = matches!(plan, LineagePlan::Resume { .. }),
                                "worker routing audit: bound side selected"
                            );
                            side_of(id).drive(ctx).boxed()
                        }
                        SelectionKind::LegacyFirst(id) => {
                            let first = audited
                                .lock()
                                .expect("legacy unbound audit lock")
                                .insert(session);
                            if first {
                                tracing::info!(
                                    target: "kian::superagent",
                                    session = %session.0,
                                    exec_worker = %id,
                                    "legacy unbound worker session routed to first endpoint \
                                     `{id}`; not a supported mode"
                                );
                            }
                            side_of(id).drive(ctx).boxed()
                        }
                        SelectionKind::BoundUnconfigured(id) => {
                            tracing::warn!(
                                target: "kian::superagent",
                                session = %session.0,
                                exec_worker = %id,
                                "worker routing audit: bound worker `{id}` is not known to this \
                                 server; exec refused"
                            );
                            let factory = refuse_side
                                .expect("the refusing factory is installed exactly when the side factory is");
                            factory(&Refusal::Unconfigured(id.to_string()))
                                .drive(ctx)
                                .boxed()
                        }
                        SelectionKind::Malformed => {
                            tracing::warn!(
                                target: "kian::superagent",
                                session = %session.0,
                                "worker routing audit: malformed exec_worker binding; exec refused"
                            );
                            let factory = refuse_side
                                .expect("the refusing factory is installed exactly when the side factory is");
                            factory(&Refusal::Malformed).drive(ctx).boxed()
                        }
                        // Unbound with nothing known (no statics, no
                        // dynamic ids): today's local path (S2).
                        SelectionKind::NoWorkers => worker.drive(ctx).boxed(),
                    }
                }
            }
        })
        .flatten()
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// Tool composition
// ---------------------------------------------------------------------------

/// A [`ToolProvider`] over an ordered list of providers: schemas are
/// the union with **first-wins** deduplication by tool name, and
/// invocations route to the first provider owning the name.
///
/// Assembly uses the ordering `[remote-exec…, local-tools]` for worker
/// agents, so the remote `exec` tool (same schema as the local built-in,
/// ) shadows the local one when endpoints are configured — one
/// uniform `exec` surface for the model either way.
pub struct CombinedTools {
    providers: Vec<Arc<dyn ToolProvider>>,
}

impl CombinedTools {
    pub fn new(providers: Vec<Arc<dyn ToolProvider>>) -> Self {
        Self { providers }
    }
}

impl ToolProvider for CombinedTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for provider in &self.providers {
            for schema in provider.schemas() {
                if seen.insert(schema.name.clone()) {
                    out.push(schema);
                }
            }
        }
        out
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        // First provider whose schema set contains the call's name.
        for provider in &self.providers {
            if provider
                .schemas()
                .iter()
                .any(|schema| schema.name == call.name)
            {
                return provider.invoke(call);
            }
        }
        Box::pin(async move {
            Err(ToolError::InvalidCall(format!(
                "no provider serves tool `{}`",
                call.name
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// The answer surface mount (CONFIRMATION-MODEL §14d phase 3a)
// ---------------------------------------------------------------------------

/// The answer surface's configuration as the assembly carries it: the
/// channel names allowed to answer (already validated fail-closed at
/// load) and the `confirmation.ttl_ms` the derived codes live under.
/// An empty channel set — the default — means the assembly installs no
/// interceptor and mounts no `pending_decisions` tool, so the Router's
/// prompt prefix is byte-identical to the pre-3a assembly.
#[derive(Debug, Clone, Default)]
pub struct ConfirmAnswerMount {
    pub channels: std::collections::BTreeSet<String>,
    pub ttl_ms: u64,
}

impl ConfirmAnswerMount {
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Subscription metrics (the consumer loop is assembly-owned so the
// outcomes are observable)
// ---------------------------------------------------------------------------

/// Counters for the subscription engine (routed/dropped/deduplicated).
/// Plain atomics — cheap to read from `/metrics`.
#[derive(Debug, Default)]
pub struct SubscriptionMetrics {
    routed: std::sync::atomic::AtomicU64,
    routed_deduplicated: std::sync::atomic::AtomicU64,
    dropped_no_rule: std::sync::atomic::AtomicU64,
    /// admission-class `ignore` drops (the DroppedNoRule shape).
    dropped_ignored: std::sync::atomic::AtomicU64,
    /// `explicit-only` drops without an explicit-request signal.
    dropped_not_explicit: std::sync::atomic::AtomicU64,
    failures: std::sync::atomic::AtomicU64,
    /// per-code intake-reason counters — one tick per decision,
    /// keyed by the closed [`kian_superagent::IntakeReason`] set's
    /// stable code. Every code is materialized in the snapshot, so
    /// the series shape is fixed (the histogram discipline).
    by_reason: ReasonCounters,
}

/// The closed per-code counter set (all 7 codes, zero-initialized).
#[derive(Debug)]
struct ReasonCounters {
    counts: std::collections::BTreeMap<&'static str, std::sync::atomic::AtomicU64>,
}

impl ReasonCounters {
    fn new() -> Self {
        Self {
            counts: kian_superagent::IntakeReason::ALL
                .iter()
                .map(|reason| (reason.code(), std::sync::atomic::AtomicU64::new(0)))
                .collect(),
        }
    }

    fn bump(&self, reason: kian_superagent::IntakeReason) {
        if let Some(counter) = self.counts.get(reason.code()) {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        for (code, counter) in &self.counts {
            object.insert(
                (*code).to_string(),
                serde_json::json!(counter.load(std::sync::atomic::Ordering::Relaxed)),
            );
        }
        serde_json::Value::Object(object)
    }
}

impl Default for ReasonCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionMetrics {
    fn bump(counter: &std::sync::atomic::AtomicU64) {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records one handled event's outcome: the legacy shape counters
    /// plus exactly one per-code tick per decision (— the codes
    /// come from the same mapping the audit lines stamp, so the two
    /// surfaces cannot drift).
    pub fn record_outcome(
        &self,
        outcome: &kian_superagent::RoutingOutcome,
        router: kian_core::model::SessionId,
    ) {
        match outcome {
            kian_superagent::RoutingOutcome::Routed { deduplicated, .. } => {
                Self::bump(&self.routed);
                if *deduplicated {
                    Self::bump(&self.routed_deduplicated);
                }
            }
            // fan-out: one routed counter tick per admitted
            // subscriber, dedup per row.
            kian_superagent::RoutingOutcome::FannedOut { admitted } => {
                for admission in admitted {
                    Self::bump(&self.routed);
                    if admission.deduplicated {
                        Self::bump(&self.routed_deduplicated);
                    }
                }
            }
            kian_superagent::RoutingOutcome::DroppedNoRule => {
                Self::bump(&self.dropped_no_rule);
            }
            // fast-path drops: audit line by the service, counter
            // here (the DroppedNoRule shape).
            kian_superagent::RoutingOutcome::DroppedIgnored => {
                Self::bump(&self.dropped_ignored);
            }
            kian_superagent::RoutingOutcome::DroppedNotExplicit => {
                Self::bump(&self.dropped_not_explicit);
            }
            // A hook veto at `admit/pre` (PLUGIN-SPEC §4.1): no admission
            // decision exists to count — the veto's own audit lines
            // (the service's and the bus's `hook-veto`) carry it, and
            // no legacy counter drifts.
            kian_superagent::RoutingOutcome::RefusedByHook { .. } => {}
            // The answer surface (CONFIRMATION-MODEL §14d) makes its own
            // decision: it either writes a `control/confirm` row or drops
            // the message. Neither is an admission shape, so no legacy
            // counter moves; all four codes ride the per-reason set
            // (`answer_applied` / `answer_no_match` / `answer_ambiguous` /
            // `answer_write_failed`), which is where a new shape belongs.
            kian_superagent::RoutingOutcome::Answered { .. }
            | kian_superagent::RoutingOutcome::DroppedAnswerNoMatch
            | kian_superagent::RoutingOutcome::DroppedAnswerAmbiguous
            | kian_superagent::RoutingOutcome::DroppedAnswerWriteFailed => {}
        }
        for reason in kian_superagent::outcome_reasons(outcome, router) {
            self.by_reason.bump(reason);
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering;
        serde_json::json!({
            "routed": self.routed.load(Ordering::Relaxed),
            "routed_deduplicated": self.routed_deduplicated.load(Ordering::Relaxed),
            "dropped_no_rule": self.dropped_no_rule.load(Ordering::Relaxed),
            "dropped_ignored": self.dropped_ignored.load(Ordering::Relaxed),
            "dropped_not_explicit": self.dropped_not_explicit.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
            "by_reason": self.by_reason.snapshot(),
        })
    }
}

/// Spawns one consumer per adapter, feeding the subscription service
/// and recording outcomes (the assembly-owned variant of
/// `SubscriptionService::spawn` — same semantics, observable).
pub fn spawn_subscription_consumers(
    service: Arc<SubscriptionService>,
    adapters: Vec<Arc<dyn ChannelAdapter>>,
    metrics: Arc<SubscriptionMetrics>,
    edge: Option<Arc<crate::media_edge::MediaEdge>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    adapters
        .into_iter()
        .map(|adapter| {
            let service = Arc::clone(&service);
            let metrics = Arc::clone(&metrics);
            let edge = edge.clone();
            let mut stream = adapter.subscribe();
            tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    // ADR-062: the media edge runs before routing, so an
                    // inbound image or voice note is already an artifact
                    // reference plus transcript when it is shaped and
                    // admitted. Absent = the event is untouched.
                    let event = match edge.as_ref() {
                        Some(edge) => edge.prepare(event).await,
                        None => event,
                    };
                    match service.handle_event(event).await {
                        Ok(outcome) => {
                            // Legacy shape counters plus the
                            // per-code ticks (once per decision, the
                            // audit lines' own mapping).
                            metrics.record_outcome(&outcome, service.router_session());
                        }
                        Err(_) => {
                            SubscriptionMetrics::bump(&metrics.failures);
                        }
                    }
                }
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Watchdog wiring: health edges → admitted inputs for affected sessions
// ---------------------------------------------------------------------------

/// The endpoint-health input payload admitted to a session holding
/// queued commands on the endpoint that went offline. A normal message
/// payload whose content embeds a `kian.endpoint_health` marker — the
/// same content-embedding discipline as channel provenance (the frozen
/// payload contract is untouched).
pub fn endpoint_health_payload(
    endpoint_id: &str,
    status: &str,
    detail: Option<&str>,
    queued: usize,
    ts_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "kian": {
            "type": "endpoint_health",
            "endpoint": endpoint_id,
            "status": status,
            "detail": detail,
            "queued_commands": queued,
            "ts_ms": ts_ms,
        },
        "text": format!(
            "execution endpoint `{endpoint_id}` is {status}; {queued} queued command(s) affected"
        ),
    })
}

/// Sessions whose streams carry a `tool/call` with one of the given
/// call ids — the sessions holding the queued commands. Derived purely
/// from the event log (constraint 1) via the store's pool; the `kind`
/// column and the envelope's call id do the matching.
async fn sessions_for_tool_calls(
    store: &EventStore,
    tool_call_refs: &BTreeSet<String>,
) -> Result<BTreeSet<SessionId>, String> {
    let mut sessions = BTreeSet::new();
    for call_id in tool_call_refs {
        // same lookup, per dialect (sqlite json_extract vs pg
        // jsonb path) — derived purely from the event log either way.
        let rows: Vec<(String,)> = match store {
            EventStore::Sqlite(pool) => sqlx::query_as(
                "SELECT DISTINCT session FROM events \
                 WHERE kind = 'tool/call' \
                   AND json_extract(envelope, '$.bytes.content.id') = ?",
            )
            .bind(call_id)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("endpoint-health session lookup: {e}"))?,
            EventStore::Postgres(pool) => sqlx::query_as(
                "SELECT DISTINCT session FROM events \
                 WHERE kind = 'tool/call' \
                   AND envelope::jsonb -> 'bytes' -> 'content' ->> 'id' = $1::text",
            )
            .bind(call_id)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("endpoint-health session lookup: {e}"))?,
        };
        for (session,) in rows {
            if let Ok(uuid) = uuid::Uuid::parse_str(&session) {
                sessions.insert(SessionId(uuid));
            }
        }
    }
    Ok(sessions)
}

/// Spawns the watchdog bridge: subscribes to the [`WatchdogSource`]
/// health stream to learn which endpoints are (still) offline, then
/// **periodically re-examines** every offline endpoint for sessions
/// holding queued commands — one durable health input row per affected
/// session, admitted and woken.
///
/// Why the tick beside the edges: an endpoint that is offline *before*
/// a command is enqueued (registered-but-stale at startup, or the
/// offline edge arriving between polls) would otherwise never be
/// re-examined — the edge fired once and the queue row landed after
/// it. The re-examination is idempotent per offline episode: the admit
/// key embeds the episode's `ts_ms`, so repeated sweeps deduplicate at
/// the inbox row. Online edges drop the endpoint from the sweep set
/// (reconnects re-drive the queue on their own, semantics).
pub fn spawn_watchdog_bridge(
    watchdog: Arc<HeartbeatWatchdog>,
    exec_store: ExecStore,
    event_store: EventStore,
    inbox: Arc<dyn InboxWriter>,
) -> tokio::task::JoinHandle<()> {
    use kian_core::seams::EndpointHealth;
    use std::collections::BTreeMap;
    tokio::spawn(async move {
        let mut health = watchdog.health();
        // endpoint → (offline since ts_ms, detail) — the live sweep set.
        let mut offline: BTreeMap<String, (u64, Option<String>)> = BTreeMap::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event = health.next() => {
                    let Some(event) = event else { break };
                    match event.status {
                        EndpointHealth::Offline => {
                            tracing::info!(
                                endpoint = %event.endpoint_id,
                                "watchdog: endpoint offline"
                            );
                            offline.insert(
                                event.endpoint_id.clone(),
                                (event.ts_ms, event.detail.clone()),
                            );
                        }
                        EndpointHealth::Online => {
                            tracing::info!(
                                endpoint = %event.endpoint_id,
                                "watchdog: endpoint back online; queue re-drives itself"
                            );
                            offline.remove(&event.endpoint_id);
                        }
                    }
                }
                _ = ticker.tick() => {
                    for (endpoint, (ts_ms, detail)) in offline.clone() {
                        sweep_offline_endpoint(
                            &exec_store,
                            &event_store,
                            inbox.as_ref(),
                            &endpoint,
                            ts_ms,
                            detail.as_deref(),
                        )
                        .await;
                    }
                }
            }
        }
    })
}

/// One idempotent sweep of a currently-offline endpoint: sessions
/// holding queued commands on it get one durable health input row.
async fn sweep_offline_endpoint(
    exec_store: &ExecStore,
    event_store: &EventStore,
    inbox: &dyn InboxWriter,
    endpoint_id: &str,
    offline_since_ms: u64,
    detail: Option<&str>,
) {
    let queued = match exec_store.drivable(endpoint_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, endpoint = %endpoint_id, "watchdog bridge: cannot read queued commands");
            return;
        }
    };
    let refs: BTreeSet<String> = queued
        .iter()
        .filter_map(|row| row.tool_call_ref.clone())
        .collect();
    if refs.is_empty() {
        return;
    }
    let sessions = match sessions_for_tool_calls(event_store, &refs).await {
        Ok(sessions) => sessions,
        Err(e) => {
            tracing::warn!(error = %e, "watchdog bridge: session lookup failed");
            return;
        }
    };
    for session in sessions {
        let payload =
            endpoint_health_payload(endpoint_id, "offline", detail, refs.len(), offline_since_ms);
        let key = format!("endpoint-health:{endpoint_id}:offline:{offline_since_ms}");
        match inbox.admit(session, payload, Some(key)).await {
            Ok(receipt) => {
                inbox.wake(session);
                if !receipt.deduplicated {
                    tracing::info!(
                        session = %session.0,
                        endpoint = %endpoint_id,
                        queued = refs.len(),
                        "watchdog bridge: health input admitted"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    session = %session.0,
                    "watchdog bridge: health input admit failed (wake skipped)"
                );
            }
        }
    }
}

/// Registers the configured endpoints in the durable registry so a
/// never-connected endpoint is observable (registered-but-stale is an
/// actionable offline edge — semantics).
async fn seed_endpoint_registry(
    exec_store: &ExecStore,
    endpoints: &[EndpointConfig],
) -> Result<(), String> {
    for ep in endpoints {
        exec_store
            .upsert_endpoint(&ep.id, &ep.addr, 0, "offline")
            .await
            .map_err(|e| format!("seed endpoint registry `{}`: {e}", ep.id))?;
    }
    Ok(())
}

/// Reads the registry back (used by assembly logging/tests).
pub async fn endpoint_registry(exec_store: &ExecStore) -> Result<Vec<EndpointRegistryRow>, String> {
    exec_store
        .endpoints()
        .await
        .map_err(|e| format!("endpoint registry read: {e}"))
}

// ---------------------------------------------------------------------------
// The assembled runtime
// ---------------------------------------------------------------------------

/// Builds one per-worker-id async jobs engine on demand (R12, S3):
/// `AsyncJobs` is a stateless handle over the shared durable queue, so
/// the lazy sides and the boot sweep each build their own instance
/// per id.
pub type AsyncJobsFactory = Arc<dyn Fn(&str) -> Arc<kian_exec::AsyncJobs> + Send + Sync>;

/// The running super-agent services the [`crate::server::Server`]
/// holds: everything with a background task or a metrics surface. The
/// server's `ControlPlane`/`OpsPlane` impls reach the bundle only
/// through this handle (mock-channel injection needs the channel map;
/// `/metrics` needs the counters).
pub struct SuperAgentRuntime {
    /// ADR-040: the live resident binding (reads at use time; a rotation
    /// re-points the runtime's own views).
    pub router: Arc<kian_superagent::ResidentBinding>,
    /// Channel name → mock adapter (the injection surface; v1 only
    /// mock channels exist).
    pub channels: BTreeMap<String, Arc<MockChannel>>,
    /// The worker's evidence gate (shared counters = the verdict
    /// metrics surface).
    pub worker_gate: EvidenceGateConfig,
    /// Subscription outcome counters.
    pub subscription_metrics: Arc<SubscriptionMetrics>,
    /// Router judge-turn batching metrics (batch size, wall
    /// duration and amortized per-decision latency — recorded by
    /// `RouterDriver`, read-only here).
    pub router_metrics: Arc<RouterMetrics>,
    /// The `superagent.policy_drift` counter: one tick per
    /// drift marked at boot.
    pub policy_drift: Arc<std::sync::atomic::AtomicU64>,
    /// The heartbeat watchdog (when endpoints are configured).
    pub watchdog: Option<Arc<HeartbeatWatchdog>>,
    /// The remote-exec services (when a worker surface is configured).
    pub remote: Option<Arc<crate::plugins::RemoteExecServices>>,
    /// The worker-membership metrics counters (the registration
    /// listener's edge counters — present iff `workers:` is
    /// configured; the `/metrics` superagent fragment reads them).
    pub worker_membership: Option<Arc<kian_exec::RegistrationMetrics>>,
    /// The completion bridge's counters (present when the async
    /// pair is mounted).
    pub async_metrics: Option<Arc<crate::async_bridge::AsyncBridgeMetrics>>,
    /// The cross-session read port: `read_events` /
    /// `list_threads` / `spill_location` over the store's public scan,
    /// with the startup-rebuilt spill index. Read-only by
    /// construction; the recall toolpack mounts on it.
    pub recall: Arc<StoreRecall>,
    /// GitHub channels by configured name: the webhook-fed
    /// adapters (at most one — the config validation enforces it).
    /// The `WebhookPlane` hand-off in `server.rs` reads this map.
    pub github_channels: BTreeMap<String, Arc<kian_superagent::GitHubChannel>>,
    /// Consumer + bridge tasks; kept alive for the server's lifetime.
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SuperAgentRuntime {
    /// The mock channel registered under `name`.
    pub fn mock_channel(&self, name: &str) -> Option<Arc<MockChannel>> {
        self.channels.get(name).cloned()
    }

    /// The `/metrics` superagent fragment: evidence-verdict,
    /// subscription, channel-policy (a pure projection of the Router
    /// stream — workers never deliver; policies are Router-side),
    /// router batching (the §7.2 series the resident Router's
    /// driver records) and watchdog counters.
    pub async fn metrics_report(&self, store: &EventStore) -> serde_json::Value {
        let verdicts = self.worker_gate.counters.snapshot();
        let subscriptions = self.subscription_metrics.snapshot();
        let router_batching = self.router_metrics.snapshot();

        // Channel-policy counts derive from the Router's committed
        // stream: `channel/delivered` events carry policy/words/truncated.
        let mut delivered = 0u64;
        let mut im = 0u64;
        let mut structured = 0u64;
        let mut truncated = 0u64;
        if let Ok(events) = store.rebuild(self.router.load()).await {
            for event in &events {
                if event.kind != EventKind::ChannelDelivered {
                    continue;
                }
                delivered += 1;
                match event.bytes.get("policy").and_then(|p| p.as_str()) {
                    Some("im") => im += 1,
                    Some("structured") => structured += 1,
                    _ => {}
                }
                if event.bytes.get("truncated") == Some(&serde_json::json!(true)) {
                    truncated += 1;
                }
            }
        }

        let watchdog = self.watchdog.as_ref().map(|watchdog| {
            let metrics = watchdog.metrics();
            serde_json::json!({
                "offline_transitions": metrics.offline_transitions(),
                "online_transitions": metrics.online_transitions(),
                "mean_detection_latency_ms": metrics.mean_detection_latency_ms(),
            })
        });

        // Worker-membership edge counters (present iff `workers:` is
        // configured — one per audited transition, deduped per
        // generation by construction).
        let worker_membership = self.worker_membership.as_ref().map(|m| {
            let load =
                |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
            serde_json::json!({
                "joined": load(&m.joined),
                "replaced": load(&m.replaced),
                "left_bye": load(&m.left_bye),
                "left_loss": load(&m.left_loss),
                "refused": load(&m.refused),
                "rate_limited": load(&m.rate_limited),
            })
        });

        serde_json::json!({
            "router_session": self.router.load().0,
            "evidence_verdicts": {
                "passed": verdicts.passed,
                "failed": verdicts.failed,
            },
            "subscriptions": subscriptions,
            "router_batching": router_batching,
            "policy_drift": self
                .policy_drift
                .load(std::sync::atomic::Ordering::Relaxed),
            "channel_policy": {
                "delivered": delivered,
                "im": im,
                "structured": structured,
                "truncated": truncated,
            },
            "watchdog": watchdog,
            "worker_membership": worker_membership,
            "async_bridge": self.async_metrics.as_ref().map(|m| m.snapshot()),
        })
    }
}

/// Everything the assembled server needs from the activated
/// `superagent` plugin before the engine exists.
pub struct SuperAgentServices {
    /// The assembled three-way driver.
    pub driver: Arc<SuperAgentDriver>,
    /// ADR-040: the shared live-resident binding (the rotation re-points
    /// it; every consumer reads it at use time).
    pub router_binding: Arc<kian_superagent::ResidentBinding>,
    /// The late-bound engine slot (server installs the engine).
    pub inbox: Arc<EngineInbox>,
    /// The mounted router inbox (residency signal; also the spawner's
    /// and the worker wrapper's port).
    pub router_inbox: Arc<RouterInbox>,
    /// The delegation spawner (ADR-038: `start_services` installs the
    /// session-bootstrap seam here before the first dispatch).
    pub spawner: Arc<WorkerSpawner>,
    /// The resident Router profile (bootstrap is called post-rebuild).
    pub profile: RouterProfile,
    /// The subscription service (registry installed post-rebuild).
    pub subscriptions: Arc<SubscriptionService>,
    /// Mock channels by configured name (injection surface).
    pub mock_channels: BTreeMap<String, Arc<MockChannel>>,
    /// Lark channels by configured name — the real Feishu
    /// adapters, registered alongside the mocks.
    pub lark_channels: BTreeMap<String, Arc<kian_superagent::LarkChannel>>,
    /// GitHub channels by configured name — the webhook-fed
    /// adapters. The receiver plane (server.rs) hands verified
    /// deliveries to the single channel through this map.
    pub github_channels: BTreeMap<String, Arc<kian_superagent::GitHubChannel>>,
    /// WeChat channels by configured name — the iLink long-poll
    /// adapters (phase 1: single account). Exposed for observability
    /// (paused/stale counters); the receive drivers self-start with
    /// the subscription consumers.
    pub wechat_channels: BTreeMap<String, Arc<kian_superagent::WechatChannel>>,
    /// iMessage channels by configured name — the macOS-bridge
    /// adapters (phase 1: single account). Exposed for observability
    /// (paused/gap/uncertain-send counters); the receive drivers
    /// self-start with the subscription consumers.
    pub imessage_channels: BTreeMap<String, Arc<kian_superagent::ImessageChannel>>,
    /// Adapters in config order (consumer spawn order).
    pub adapters: Vec<Arc<dyn ChannelAdapter>>,
    /// ADR-062: the channel-edge media processor, mounted only when
    /// `media:` is configured — every adapter event is prepared through
    /// it before `SubscriptionService::handle_event`. `None` = today's
    /// bytes on every channel.
    pub media: Option<Arc<crate::media_edge::MediaEdge>>,
    /// The worker's evidence gate config (shared counters).
    pub worker_gate: EvidenceGateConfig,
    /// Subscription outcome counters.
    pub subscription_metrics: Arc<SubscriptionMetrics>,
    /// Router judge-turn batching metrics — the handle the
    /// driver records into and `/metrics` snapshots.
    pub router_metrics: Arc<RouterMetrics>,
    /// the configured policy version
    /// (`superagent.policy.version`), rendered into the resident
    /// profiles' bootstrap prompts as a `[policy <label>]` header.
    /// `None` = today's bytes, byte-identical.
    pub policy: Option<kian_superagent::PolicyVersion>,
    /// `superagent.policy.notify_live` (default false) — deliver
    /// one synthetic policy-changed marker input to each drifted live
    /// resident session at boot (idempotent by a stable event id).
    pub policy_notify_live: bool,
    /// The `superagent.policy_drift` counter: one tick per drift
    /// marked at boot (live sessions are never rewritten — the counter
    /// is the audit surface's numeric twin).
    pub policy_drift: Arc<std::sync::atomic::AtomicU64>,
    /// the outbox bounded-retry driver, when
    /// `superagent.outbox` is configured — the same handle the Router
    /// tools deliver through; the boot re-drive uses it after the
    /// stream rebuild.
    pub outbox: Option<Arc<kian_superagent::OutboxDriver>>,
    /// The Router's shared hold slot (the boot re-drive waits
    /// for its first per-hold stream port before writing re-drive
    /// receipts).
    pub router_hold: Arc<kian_superagent::RouterHold>,
    /// Pre-opened exec services when endpoints are configured.
    pub remote: Option<Arc<crate::plugins::RemoteExecServices>>,
    /// The async-jobs engine factory when `superagent.async_tools` is
    /// on (R12, S3: one lazily-built engine per worker id — the lazy
    /// sides build their own instances; the startup sweep materializes
    /// one per known id and re-drives/redelivers after the inbox
    /// rebuild).
    pub async_jobs_factory: Option<crate::superagent::AsyncJobsFactory>,
    /// The completion bridge's counters, for `/metrics`.
    pub async_metrics: Option<Arc<crate::async_bridge::AsyncBridgeMetrics>>,
    /// The cross-session read port: reads over the store's
    /// public scan plus the disposable spill index, rebuilt by
    /// `start_services` after the inbox rebuild.
    pub recall: Arc<StoreRecall>,
    /// The timer input sources: their run tasks spawn in
    /// `start_services` after the subscription consumers (a timer with
    /// no consumer would emit ticks into the void).
    pub timers: Vec<Arc<kian_superagent::TimerChannel>>,
    /// The resident watchdog profile, when configured: bootstrapped
    /// (idempotent prompt admit + wake) in `start_services`.
    pub watchdog: Option<kian_superagent::WatchdogProfile>,
    /// the watchdog's mounted inbox (residency-counted), when
    /// configured — the drift notice admits through the mounted inbox
    /// a parked resident polls.
    pub watchdog_inbox: Option<Arc<RouterInbox>>,
    /// §13.3's alert port: the conductor admits one alert row per failed
    /// run through it. Mounted empty by the plugin; the config-resolved
    /// catalogue is installed here at boot by [`crate::server::Server::from_config`].
    pub job_alerts: Arc<crate::job_runtime::JobAlertPort>,
}

/// Post-`startup_rebuild` services start (the load-bearing
/// order): registry rebuild → subscription consumers → watchdog +
/// bridge → Router bootstrap. Called by [`crate::server::Server::from_config`]
/// **after** the inbox rebuild returned; every step lands in `order`.
pub async fn start_services(
    services: Arc<SuperAgentServices>,
    engine: Arc<Engine<ServeLoopDriver>>,
    store: EventStore,
    order: StartupOrder,
    session_bootstrap: Option<Arc<dyn kian_superagent::SessionBootstrap>>,
    rotation: Option<crate::rotation::BootRotation>,
) -> Result<SuperAgentRuntime, String> {
    let router = services.profile.session;
    // ADR-038: the spawner is built by the plugin (before the engine
    // exists), so the seam is installed late here; every worker admit
    // after this point bootstraps before its triggering row.
    if let Some(bootstrap) = session_bootstrap.as_ref() {
        services.spawner.install_bootstrap(Arc::clone(bootstrap));
    }

    // 1. Rebuild the passthrough registry from event streams (the
    //    NoLostWork analog for subscriptions; nothing authoritative
    //    survives only in memory). The scan covers the whole
    //    re-arm chain — ended predecessors still carry thread/started
    //    lineage and worker references the registry must serve.
    let resident_chain: Vec<SessionId> = services
        .profile
        .re_arm
        .iter()
        .map(|ended| ended.session)
        .chain(std::iter::once(router))
        .collect();
    let mut events = Vec::new();
    for session in &resident_chain {
        let part = store
            .rebuild(*session)
            .await
            .map_err(|e| format!("passthrough registry rebuild (resident {}): {e}", session.0))?;
        events.extend(part);
    }
    for worker in worker_sessions(&events) {
        let worker_events = store
            .rebuild(worker)
            .await
            .map_err(|e| format!("passthrough registry rebuild (worker): {e}"))?;
        events.extend(worker_events);
    }
    let registry = PassthroughRegistry::rebuild(&events);
    tracing::info!(
        topics = registry.len(),
        chain = resident_chain.len(),
        "superagent: passthrough registry rebuilt from streams"
    );
    services.subscriptions.install_registry(registry);

    // 1b. Rebuild the disposable spill index over the same
    //     scanned universe — a pure location projection, reads only.
    services
        .recall
        .rebuild_spill_index(&resident_chain)
        .await
        .map_err(|e| format!("spill index rebuild: {e}"))?;
    record(&order, "spill_index_rebuilt");

    // 1c. boot-time policy drift marking over the LIVE resident
    //     sessions (the Router and, when configured, the watchdog).
    //     The persisted bootstrap prompts are read-only here — a
    //     drifted session keeps its prompt byte-identically; only the
    //     next bootstrap renders the new header (cache-byte stability).
    //     Ended re-arm predecessors are frozen history and never
    //     marked or notified.
    let current_policy = services.policy.as_ref().map(|p| p.label().to_string());
    let router_stream: Vec<Event> = events
        .iter()
        .filter(|event| event.session == router)
        .cloned()
        .collect();
    let mut residents: Vec<(SessionId, Vec<Event>)> = vec![(router, router_stream)];
    if let Some(watchdog_profile) = &services.watchdog {
        let watchdog_events = store.rebuild(watchdog_profile.session).await.map_err(|e| {
            format!(
                "policy drift scan (watchdog {}): {e}",
                watchdog_profile.session.0
            )
        })?;
        residents.push((watchdog_profile.session, watchdog_events));
    }
    let marking = kian_superagent::run_drift_marking(
        residents.iter().map(|(s, evs)| (*s, evs.as_slice())),
        current_policy.as_deref(),
        services.policy_notify_live,
        |session| {
            // Each resident's marker admits through ITS OWN mounted
            // inbox — the residency counter a parked resident polls
            // . The Router's and the watchdog's mounted
            // inboxes; anything else falls back to the raw engine
            // inbox.
            if session == services.router_binding.load() {
                Arc::clone(&services.router_inbox) as Arc<dyn kian_superagent::InboxWriter>
            } else if services
                .watchdog
                .as_ref()
                .is_some_and(|profile| profile.session == session)
            {
                Arc::clone(
                    services
                        .watchdog_inbox
                        .as_ref()
                        .expect("the watchdog inbox is mounted exactly when the profile is"),
                ) as Arc<dyn kian_superagent::InboxWriter>
            } else {
                Arc::clone(&services.inbox) as Arc<dyn kian_superagent::InboxWriter>
            }
        },
    )
    .await
    .map_err(|e| format!("policy drift marking: {e}"))?;
    if !marking.marked.is_empty() {
        services.policy_drift.fetch_add(
            marking.marked.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        record(&order, "policy_drift_marked");
    }

    // 1d. The outbox boot re-drive — pending (in-flight,
    //     result-less) delivery intents rebuilt from the Router's
    //     stream are re-sent through the adapter (at-least-once; under
    //     per-key ordering the receipt lands once). Parked entries are
    //     NOT re-driven (003: a parked delivery waits for a person —
    //     visible via the `delivery-retry` stage and `kian tickets`).
    //     Receipts write through the Router's per-hold port, so the
    //     re-drive task waits for the hold's first port (the resident
    //     Router installs one on its first drain after boot); until
    //     then the intent stays pending and visible — never silently
    //     dropped.
    if let Some(driver) = services.outbox.clone() {
        // The Router's own events (the same universe scan above).
        let router_events: Vec<Event> = events
            .iter()
            .filter(|event| event.session == router)
            .cloned()
            .collect();
        let pending = kian_superagent::outbox_pending(&router_events);
        let redriven = pending
            .iter()
            .filter(|entry| entry.state == kian_superagent::OutboxState::Pending)
            .count();
        tracing::info!(
            target: "kian::outbox",
            pending = redriven,
            parked = pending.len() - redriven,
            "outbox audit: boot rebuild from the Router stream"
        );
        // each parked delivery rebuilt from the stream is
        // surfaced at boot with its identity — the park is durable
        // (the intent + park-shaped tool/result on the stream), so the
        // rebooted process re-states it instead of silently forgetting
        // (the run2 negative: parked=0 while two sat parked). Not
        // re-driven: a parked delivery waits for a person (003).
        for entry in pending
            .iter()
            .filter(|e| matches!(e.state, kian_superagent::OutboxState::Parked { .. }))
        {
            let attempts = match entry.state {
                kian_superagent::OutboxState::Parked { attempts } => attempts,
                _ => 0,
            };
            tracing::error!(
                target: "kian::outbox",
                ticket = %entry.ticket,
                conversation = %entry.conversation,
                attempts,
                "outbox audit: boot rebuild found a parked delivery — visible on `kian tickets`, \
                 released by a person, not re-driven"
            );
        }
        if redriven > 0 {
            let hold = services.router_hold.clone();
            let im = services.profile.im.clone();
            let structured = services.profile.structured.clone();
            tokio::spawn(async move {
                // Wait for the first Router hold port (poll tick well
                // under any drain cadence; a sleeping task, never a
                // blocked worker).
                let port = loop {
                    if let Some(port) = hold.port() {
                        break port;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                };
                let outcomes = driver
                    .redrive(&pending, &im, &structured, &router_events, &port)
                    .await;
                for (call_id, outcome) in outcomes {
                    match outcome {
                        Ok(delivered) => tracing::info!(
                            target: "kian::outbox",
                            call_id = %call_id,
                            attempt = delivered.attempt,
                            "outbox audit: boot re-drive delivered"
                        ),
                        Err(park) => tracing::error!(
                            target: "kian::outbox",
                            call_id = %call_id,
                            "outbox audit: boot re-drive parked: {}", park.error_message()
                        ),
                    }
                }
            });
        }
    }
    record(&order, "outbox_redrive_scheduled");

    // 2. Subscription consumers (AFTER the rebuild — no event is
    //    routed against an empty registry).
    let consumers = spawn_subscription_consumers(
        Arc::clone(&services.subscriptions),
        services.adapters.clone(),
        Arc::clone(&services.subscription_metrics),
        services.media.clone(),
    );
    record(&order, "subscriptions_started");

    // 2b. The timers' run tasks spawn in step 4b below —
    //     strictly after the consumers exist (a tick emitted before
    //     any subscriber is a tick nobody hears).

    // 3. Watchdog + bridge (needs the engine-backed inbox), and the
    //    dynamic-membership registration listener (R12): boot = registry
    //    rows exist, runtime directory empty — workers re-register; no
    //    dynamic seeding, no forged joins (spec §3.2).
    let mut watchdog = None;
    let mut bridge = Vec::new();
    let mut worker_membership = None;
    if let Some(remote) = &services.remote {
        seed_endpoint_registry(remote.backend.store(), &remote.endpoints).await?;
        let offline_after = remote
            .endpoints
            .iter()
            .map(|ep| ep.offline_after_ms)
            .min()
            .or_else(|| remote.workers.as_ref().map(|w| w.offline_after_ms))
            .unwrap_or(30_000);
        // Detection bound = offline_after + poll_interval; keep the
        // poll a fraction of the bound, capped at 1s.
        let poll = (offline_after / 4).clamp(50, 1_000);
        let config = WatchdogConfig {
            offline_after: Duration::from_millis(offline_after),
            poll_interval: Duration::from_millis(poll),
        };
        let clock: Arc<dyn ExecClock> = Arc::new(ExecSystemClock);
        let wd = HeartbeatWatchdog::start(remote.backend.store().clone(), config, clock);
        bridge.push(spawn_watchdog_bridge(
            Arc::clone(&wd),
            remote.backend.store().clone(),
            store.clone(),
            Arc::clone(&services.router_inbox) as Arc<dyn InboxWriter>,
        ));
        watchdog = Some(wd);
        record(&order, "watchdog_started");
        // The registration listener accepts worker connections and
        // attaches runtime routes through the sink (queue-first: rows
        // parked before any route exist drain on registration).
        if let Some(workers) = &remote.workers {
            let listener = Arc::clone(&workers.listener);
            worker_membership = Some(listener.metrics());
            tokio::spawn(async move {
                if let Err(error) = listener.run().await {
                    tracing::error!(%error, "worker registration listener failed");
                }
            });
            record(&order, "worker_listener_started");
        }
    }

    // 3b. The async bridge's startup sweep, strictly after the
    //    inbox rebuild (admitted completions must never race an empty
    //    wake registry): re-offer settled rows (a crash between settle
    //    and deliver loses nothing — the admit key dedupes), then
    //    re-drive pending/inflight jobs (observers re-attach through
    //    the queue; one row, one dispatch, ever).
    //    (R12, S3): the engines are lazy per worker id — the sweep
    //    materializes one instance per known id (static pins ∪
    //    durable registry rows), so a dynamic worker's pending rows
    //    re-drive after a kian restart too.
    if let Some(factory) = &services.async_jobs_factory {
        let mut known: Vec<String> = services
            .remote
            .as_ref()
            .map(|remote| remote.endpoints.iter().map(|ep| ep.id.clone()).collect())
            .unwrap_or_default();
        if let Some(remote) = &services.remote
            && let Ok(rows) = remote.backend.store().endpoints_lenient().await
        {
            for row in rows {
                if !known.contains(&row.endpoint_id) {
                    known.push(row.endpoint_id);
                }
            }
        }
        for id in &known {
            let jobs = factory(id);
            jobs.redeliver_settled().await;
            jobs.redrive_pending().await;
        }
        if !known.is_empty() {
            record(&order, "async_jobs_redriven");
        }
    }

    // 4. Bootstrap the resident Router (idempotent admit + wake; the
    //    first hold appends `session/created`). ADR-038: the session
    //    bootstrap runs first, so a fresh Router's first context-bearing
    //    event is the memory index, not the judge prompt.
    if let Some(bootstrap) = session_bootstrap.as_ref()
        && let Err(error) = bootstrap.bootstrap(router).await
    {
        tracing::warn!(session = %router.0, %error, "router session bootstrap skipped");
    }
    // ADR-040 phase 0: the rotated successor's context order is
    // memory index (above) → the predecessor's pending message rows →
    // the bounded handoff → the judge prompt (below). The rows are
    // quiet admits; the profile bootstrap's admit is the wake.
    if let Some(rotation) = rotation.as_ref() {
        if rotation.successor != router {
            return Err(format!(
                "rotation successor {} does not match the live router {}",
                rotation.successor.0, router.0
            ));
        }
        for (payload, key) in &rotation.migrated {
            if let Err(error) = engine
                .admit_quiet(router, payload.clone(), key.clone())
                .await
            {
                tracing::warn!(session = %router.0, %error, "rotated input migration skipped");
            }
        }
        let key = Some(format!("rotate-handoff:{}", rotation.predecessor.0));
        if let Err(error) = engine
            .admit_quiet(router, rotation.handoff.clone(), key)
            .await
        {
            tracing::warn!(session = %router.0, %error, "rotation handoff skipped");
        }
    }
    services
        .profile
        .bootstrap(services.router_inbox.as_ref())
        .await
        .map_err(|e| format!("router bootstrap: {e}"))?;
    engine.wake(router);
    record(&order, "router_bootstrapped");

    let mut tasks = consumers;

    // 4b. bootstrap the resident watchdog session (idempotent
    //     prompt admit + wake; the first hold appends
    //     `session/created`), then start the timers' run tasks —
    //     after this point every emitted tick reaches a live consumer
    //     and a bootstrapped judge.
    if let Some(watchdog_profile) = &services.watchdog {
        if let Some(bootstrap) = session_bootstrap.as_ref()
            && let Err(error) = bootstrap.bootstrap(watchdog_profile.session).await
        {
            tracing::warn!(
                session = %watchdog_profile.session.0,
                %error,
                "watchdog session bootstrap skipped"
            );
        }
        watchdog_profile
            .bootstrap(services.router_inbox.as_ref())
            .await
            .map_err(|e| format!("watchdog bootstrap: {e}"))?;
        engine.wake(watchdog_profile.session);
        record(&order, "watchdog_bootstrapped");
    }
    for timer in &services.timers {
        let poll = (timer.config().interval_ms / 20).clamp(10, 1_000);
        tasks.push(timer.spawn(Duration::from_millis(poll)));
    }
    if !services.timers.is_empty() {
        record(&order, "timers_started");
    }

    // 4c. The github poll drivers — strictly after the consumers
    //     (a polled event emitted before any subscriber is an event
    //     nobody hears) and after the Router bootstrap (the first poll
    //     waits one interval besides). Each poll-mode channel resumes
    //     from the stream-derived read position: the universe
    //     scan above covers the Router chain and every worker stream,
    //     so the newest admitted github event's `updated_at` cursor
    //     seeds the adapter and a restart never replays what a previous
    //     incarnation admitted (the durable backstop is `event_id`
    //     dedupe — ADR-013's no position table).
    let mut polls_spawned = 0usize;
    for github in services.github_channels.values() {
        if github.config().poll.is_none() {
            continue;
        }
        let mut tail: Vec<Event> = events
            .iter()
            .filter(|event| event.kind == EventKind::InputAdmitted)
            .cloned()
            .collect();
        // Newest-first (the read-position scan's contract): sort by
        // committed timestamp, descending.
        tail.sort_by_key(|a| std::cmp::Reverse(a.ts));
        let resume =
            kian_superagent::read_position(kian_superagent::SourceCursor::GitHubSince, &tail)
                .map(|position| position.cursor);
        if let Some(cursor) = &resume {
            tracing::info!(
                target: "kian::github_channel",
                channel = github.channel_name(),
                since = %cursor,
                "github poll: resume cursor derived from the admitted-event tail"
            );
        }
        github.seed_poll_cursor(resume);
        tasks.push(github.spawn_poll());
        polls_spawned += 1;
    }
    if polls_spawned > 0 {
        tracing::info!(
            polls = polls_spawned,
            "superagent: github poll drivers started"
        );
        record(&order, "github_poll_started");
    }

    tasks.extend(bridge);
    Ok(SuperAgentRuntime {
        router: Arc::clone(&services.router_binding),
        channels: services.mock_channels.clone(),
        worker_gate: clone_gate(&services.worker_gate),
        subscription_metrics: Arc::clone(&services.subscription_metrics),
        router_metrics: Arc::clone(&services.router_metrics),
        policy_drift: Arc::clone(&services.policy_drift),
        watchdog,
        remote: services.remote.clone(),
        worker_membership,
        async_metrics: services.async_metrics.clone(),
        recall: Arc::clone(&services.recall),
        github_channels: services.github_channels.clone(),
        tasks,
    })
}

/// Clones the gate config while sharing its counters (the metrics
/// surface is the same handle the worker wrapper records into).
pub fn clone_gate(gate: &EvidenceGateConfig) -> EvidenceGateConfig {
    EvidenceGateConfig {
        enabled: gate.enabled,
        checks: gate.checks.clone(),
        max_retries: gate.max_retries,
        counters: Arc::clone(&gate.counters),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (pure pieces only — the wiring is exercised by
// tests/superagent_serve.rs over the real engine)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn combined_tools_dedupe_first_wins_and_route_by_name() {
        use kian_core::seams::ToolSchema;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct FixedTools {
            names: Vec<&'static str>,
            hits: Arc<AtomicU64>,
        }
        impl ToolProvider for FixedTools {
            fn schemas(&self) -> Vec<ToolSchema> {
                self.names
                    .iter()
                    .map(|n| ToolSchema {
                        name: (*n).into(),
                        description: String::new(),
                        parameters: serde_json::json!({"type": "object"}),
                    })
                    .collect()
            }
            fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
                self.hits.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(ToolResult {
                        call_id: call.id,
                        output: serde_json::json!({"from": call.name}),
                        is_error: false,
                        redactions: Vec::new(),
                    })
                })
            }
        }

        let first_hits = Arc::new(AtomicU64::new(0));
        let second_hits = Arc::new(AtomicU64::new(0));
        let first = Arc::new(FixedTools {
            names: vec!["exec", "only_first"],
            hits: Arc::clone(&first_hits),
        });
        let second = Arc::new(FixedTools {
            names: vec!["exec", "fs.read"],
            hits: Arc::clone(&second_hits),
        });
        let combined = CombinedTools::new(vec![
            Arc::clone(&first) as Arc<dyn ToolProvider>,
            Arc::clone(&second) as Arc<dyn ToolProvider>,
        ]);

        // Schema union: one `exec`, plus each side's unique names.
        let mut names: Vec<String> = combined.schemas().iter().map(|s| s.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["exec", "fs.read", "only_first"]);

        // `exec` routes to the FIRST provider (remote shadows local).
        let result = combined
            .invoke(ToolCall {
                id: "c1".into(),
                name: "exec".into(),
                arguments: serde_json::json!({}),
            })
            .await
            .expect("exec routes");
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 0);
        assert_eq!(result.output["from"], "exec");

        // A name neither serves is an honest error.
        let err = combined
            .invoke(ToolCall {
                id: "c2".into(),
                name: "nope".into(),
                arguments: serde_json::json!({}),
            })
            .await
            .expect_err("unknown tool");
        assert!(err.to_string().contains("no provider serves tool `nope`"));
    }

    #[test]
    fn endpoint_health_payload_embeds_the_marker() {
        let payload = endpoint_health_payload("ep1", "offline", Some("stale"), 2, 42);
        assert_eq!(payload["kian"]["type"], "endpoint_health");
        assert_eq!(payload["kian"]["endpoint"], "ep1");
        assert_eq!(payload["kian"]["status"], "offline");
        assert_eq!(payload["kian"]["queued_commands"], 2);
        assert!(payload["text"].as_str().unwrap().contains("ep1"));
    }

    #[tokio::test]
    async fn engine_inbox_requires_install_before_use() {
        use kian_superagent::InboxWriter as _;
        let inbox = EngineInbox::new();
        let session = SessionId::new();
        // Before install: the admit future panics on use (documented);
        // wake would too. Use catch_unwind to assert the contract
        // without poisoning the test thread's output.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(inbox.admit(session, serde_json::json!(1), None))
        }));
        assert!(result.is_err(), "admit before install must panic");
    }

    /// the consumer loop counts admission-class drops in the
    /// `DroppedNoRule` shape — `ignore` bumps `dropped_ignored`,
    /// `explicit-only` without a signal bumps `dropped_not_explicit`,
    /// and nothing is routed.
    #[tokio::test]
    async fn admission_class_drops_bump_their_counters() {
        use kian_superagent::{
            AdmissionClass, AdmissionClassRule, AdmissionClassTable, Route, RuleMatcher,
            SubscriptionRule, SubscriptionRuleSet,
        };

        struct CountingInbox;
        impl InboxWriter for CountingInbox {
            fn admit(
                &self,
                _session: SessionId,
                _payload: serde_json::Value,
                _key: Option<String>,
            ) -> futures::future::BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>>
            {
                Box::pin(async {
                    Ok(AdmitReceipt {
                        deduplicated: false,
                    })
                })
            }
            fn wake(&self, _session: SessionId) -> WakeOutcome {
                WakeOutcome::Folded
            }
        }

        let channel = Arc::new(kian_superagent::MockChannel::new("mock-im"));
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        }]);
        let classes = AdmissionClassTable::new(vec![AdmissionClassRule {
            channel: "mock-im".into(),
            topic: None,
            sender: None,
            class: AdmissionClass::ExplicitOnly,
        }]);
        let service = Arc::new(kian_superagent::SubscriptionService::with_classes(
            kian_superagent::ResidentBinding::new(SessionId::new()),
            rules,
            classes,
            Arc::new(CountingInbox) as Arc<dyn InboxWriter>,
        ));
        let metrics = Arc::new(SubscriptionMetrics::default());
        let tasks = super::spawn_subscription_consumers(
            Arc::clone(&service),
            vec![Arc::clone(&channel) as Arc<dyn ChannelAdapter>],
            Arc::clone(&metrics),
            None,
        );

        // A non-mention event drops (explicit-only without a signal);
        // a mention is admitted.
        channel.inject(None, serde_json::json!({"text": "just chatting"}));
        channel.inject(None, serde_json::json!({"text": "hey @kian deploy"}));
        for _ in 0..200 {
            let snap = metrics.snapshot();
            if snap["dropped_not_explicit"] == 1 && snap["routed"] == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let snap = metrics.snapshot();
        assert_eq!(snap["dropped_not_explicit"], 1);
        assert_eq!(snap["routed"], 1);
        assert_eq!(snap["dropped_ignored"], 0);

        // Now an Ignore channel: the ignore counter bumps instead.
        let ignored_channel = Arc::new(kian_superagent::MockChannel::new("mock-im"));
        let ignored_service = Arc::new(kian_superagent::SubscriptionService::with_classes(
            kian_superagent::ResidentBinding::new(SessionId::new()),
            SubscriptionRuleSet::new(vec![]),
            kian_superagent::AdmissionClassTable::new(vec![kian_superagent::AdmissionClassRule {
                channel: "mock-im".into(),
                topic: None,
                sender: None,
                class: kian_superagent::AdmissionClass::Ignore,
            }]),
            Arc::new(CountingInbox) as Arc<dyn InboxWriter>,
        ));
        let ignored_metrics = Arc::new(SubscriptionMetrics::default());
        let ignored_tasks = super::spawn_subscription_consumers(
            Arc::clone(&ignored_service),
            vec![Arc::clone(&ignored_channel) as Arc<dyn ChannelAdapter>],
            Arc::clone(&ignored_metrics),
            None,
        );
        ignored_channel.inject(None, serde_json::json!({"text": "noise"}));
        for _ in 0..200 {
            if ignored_metrics.snapshot()["dropped_ignored"] == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(ignored_metrics.snapshot()["dropped_ignored"], 1);
        assert_eq!(ignored_metrics.snapshot()["routed"], 0);
        for task in tasks.into_iter().chain(ignored_tasks) {
            task.abort();
        }
    }

    /// ADR-062: the pump prepares every adapter event through the media
    /// edge before the subscription service sees it — an inbound photo
    /// reaches the admitted payload as an artifact reference plus
    /// transcript, and the base64 bytes never do.
    #[tokio::test]
    async fn the_channel_pump_transcribes_media_before_admission() {
        use kian_superagent::{
            AdmissionClass, AdmissionClassRule, AdmissionClassTable, InboxWriteError, InboxWriter,
            Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet,
        };
        use std::sync::Mutex;

        #[derive(Default)]
        struct RecordingInbox {
            payloads: Mutex<Vec<serde_json::Value>>,
        }
        impl InboxWriter for RecordingInbox {
            fn admit(
                &self,
                _session: SessionId,
                payload: serde_json::Value,
                _key: Option<String>,
            ) -> futures::future::BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>>
            {
                self.payloads.lock().unwrap().push(payload);
                Box::pin(async {
                    Ok(AdmitReceipt {
                        deduplicated: false,
                    })
                })
            }
            fn wake(&self, _session: SessionId) -> WakeOutcome {
                WakeOutcome::Folded
            }
        }

        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/tools/ocr"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"text": "Total 42"})),
            )
            .mount(&server)
            .await;
        let dir = std::env::temp_dir().join(format!("kian-pump-media-{}", uuid::Uuid::new_v4()));
        let store = Arc::new(kian_store::ArtifactStore::open(dir).unwrap());
        let tools = Arc::new(
            kian_media::MediaTools::new(
                kian_media::MediaToolsConfig {
                    base_url: server.uri(),
                    api_key: "test-key".into(),
                    timeout_ms: 5_000,
                    max_input_bytes: 1_048_576,
                    default_voice: None,
                    default_format: "mp3".into(),
                },
                Arc::clone(&store),
            )
            .unwrap(),
        );
        let edge = Arc::new(crate::media_edge::MediaEdge::new(tools, store));

        let channel = Arc::new(kian_superagent::MockChannel::new("mock-im"));
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        }]);
        let classes = AdmissionClassTable::new(vec![AdmissionClassRule {
            channel: "mock-im".into(),
            topic: None,
            sender: None,
            class: AdmissionClass::ExplicitOnly,
        }]);
        let inbox = Arc::new(RecordingInbox::default());
        let service = Arc::new(kian_superagent::SubscriptionService::with_classes(
            kian_superagent::ResidentBinding::new(SessionId::new()),
            rules,
            classes,
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        ));
        let metrics = Arc::new(SubscriptionMetrics::default());
        let tasks = super::spawn_subscription_consumers(
            Arc::clone(&service),
            vec![Arc::clone(&channel) as Arc<dyn ChannelAdapter>],
            Arc::clone(&metrics),
            Some(edge),
        );
        channel.inject(
            None,
            serde_json::json!({
                "text": "what is @kian on this invoice?",
                "media": [{"kind": "image", "media_type": "image/png", "base64": "aGk="}]
            }),
        );
        for _ in 0..200 {
            if !inbox.payloads.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let payloads = inbox.payloads.lock().unwrap();
        assert_eq!(payloads.len(), 1);
        let serialized = serde_json::to_string(&payloads[0]).unwrap();
        assert!(
            serialized.contains("[image transcript] Total 42"),
            "got {serialized}"
        );
        assert!(serialized.contains("\"artifact\""), "got {serialized}");
        assert!(
            !serialized.contains("aGk="),
            "base64 must not reach admission: {serialized}"
        );
        for task in tasks {
            task.abort();
        }
    }

    /// per-code counters increment exactly once per decision,
    /// keyed by the closed `IntakeReason` set, and the snapshot
    /// materializes every code (stable series shape). The legacy
    /// shape counters keep their semantics beside them.
    #[test]
    fn by_reason_counters_tick_once_per_decision() {
        use kian_superagent::FanoutAdmission;

        let router = SessionId::new();
        let worker_a = SessionId::new();
        let worker_b = SessionId::new();
        let metrics = SubscriptionMetrics::default();

        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::Routed {
                session: router,
                deduplicated: false,
            },
            router,
        );
        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::Routed {
                session: worker_a,
                deduplicated: true,
            },
            router,
        );
        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::FannedOut {
                admitted: vec![
                    FanoutAdmission {
                        session: worker_a,
                        deduplicated: false,
                    },
                    FanoutAdmission {
                        session: worker_b,
                        deduplicated: true,
                    },
                ],
            },
            router,
        );
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedNoRule, router);
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedIgnored, router);
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedNotExplicit, router);

        let snap = metrics.snapshot();
        let by_reason = &snap["by_reason"];
        // Exactly once per decision: 1 router admit, 1 passthrough
        // admit, 2 dedups, 1 per drop shape.
        assert_eq!(by_reason["admitted_router"], serde_json::json!(1));
        assert_eq!(by_reason["admitted_passthrough"], serde_json::json!(1));
        assert_eq!(by_reason["deduplicated"], serde_json::json!(2));
        assert_eq!(by_reason["no_rule"], serde_json::json!(1));
        assert_eq!(by_reason["class_ignored"], serde_json::json!(1));
        assert_eq!(by_reason["not_explicit"], serde_json::json!(1));
        // Reserved for: defined and materialized, never produced.
        assert_eq!(by_reason["released_to_eval"], serde_json::json!(0));
        // Every code of the closed set is materialized — the snapshot
        // shape is fixed regardless of what has happened.
        assert_eq!(
            by_reason.as_object().expect("by_reason object").len(),
            kian_superagent::IntakeReason::ALL.len()
        );
        // The legacy shape counters keep their own semantics.
        assert_eq!(snap["routed"], serde_json::json!(4));
        assert_eq!(snap["routed_deduplicated"], serde_json::json!(2));
        assert_eq!(snap["dropped_no_rule"], serde_json::json!(1));
        assert_eq!(snap["dropped_ignored"], serde_json::json!(1));
        assert_eq!(snap["dropped_not_explicit"], serde_json::json!(1));
        assert_eq!(snap["failures"], serde_json::json!(0));
    }

    // ------------------------------------------------------------------
    // (ADR-020) per-worker routing selection — the pure matrix (I2)
    // ------------------------------------------------------------------

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_string()).collect()
    }

    /// (R12, S2) The per-drive known-ids snapshot: the static config
    /// order first (the legacy-unbound rule), then the directory
    /// port's ids appended once each — the Bound row's membership set.
    #[test]
    fn known_ids_snapshot_unions_statics_and_port_ids_in_order() {
        let statics = ids(&["zeta", "alpha"]);
        let port = ids(&["alpha", "builder-1", "zeta"]);
        assert_eq!(
            known_ids_snapshot(&statics, &port),
            ids(&["zeta", "alpha", "builder-1"]),
            "statics keep config order; port ids append once each"
        );
        assert_eq!(known_ids_snapshot(&[], &[]), Vec::<String>::new());
    }

    /// The spec §4 selection table as a pure-function test: bound ⇒
    /// that worker; legacy unbound ⇒ the FIRST endpoint; a bound id
    /// missing from config ⇒ refusal, never a re-route; malformed ⇒
    /// refusal; zero workers ⇒ today's local path. "No binding must
    /// not default to a bound worker": with [mac-mini, vultr] the
    /// unbound session selects mac-mini because it is FIRST, and with
    /// the order flipped it selects vultr — the first endpoint, not a
    /// bound worker and not whichever side is prebuilt.
    #[test]
    fn select_side_is_the_pure_selection_table() {
        use kian_superagent::WorkerBinding;

        let configured = ids(&["mac-mini", "vultr"]);
        let bound = WorkerBinding::Bound(std::borrow::Cow::Borrowed("vultr"));
        assert_eq!(
            select_side(&bound, &configured),
            SelectionKind::Bound("vultr")
        );
        // Bound to the FIRST id: still that side.
        let first_bound = WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"));
        assert_eq!(
            select_side(&first_bound, &configured),
            SelectionKind::Bound("mac-mini")
        );
        // Unbound ⇒ first endpoint (config order).
        assert_eq!(
            select_side(&WorkerBinding::Unbound, &configured),
            SelectionKind::LegacyFirst("mac-mini")
        );
        let flipped = ids(&["vultr", "mac-mini"]);
        assert_eq!(
            select_side(&WorkerBinding::Unbound, &flipped),
            SelectionKind::LegacyFirst("vultr"),
            "the unbound route follows config order, not a bound worker"
        );
        // A bound id no longer configured: neither re-routed nor local.
        let gone = WorkerBinding::Bound(std::borrow::Cow::Borrowed("toaster"));
        assert_eq!(
            select_side(&gone, &configured),
            SelectionKind::BoundUnconfigured("toaster")
        );
        // Malformed: refusal on any config.
        assert_eq!(
            select_side(&WorkerBinding::Malformed, &configured),
            SelectionKind::Malformed
        );
        // Zero workers: today's local-exec path.
        assert_eq!(
            select_side(&WorkerBinding::Unbound, &[]),
            SelectionKind::NoWorkers
        );
    }

    /// (B1 regression) The legacy-unbound route must follow CONFIG
    /// order, not the `worker_sides` BTreeMap's (alphabetical) key
    /// order. Config `endpoints: [zeta, alpha]` — non-alphabetical —
    /// must select `zeta` (the pre-change `CombinedTools` first-wins
    /// outcome); the map-key input the driver used before the fix
    /// would have selected `alpha`, silently routing a legacy
    /// session to a different machine while the audit line claimed
    /// "first endpoint".
    #[test]
    fn legacy_unbound_selection_uses_config_order_not_map_key_order() {
        use kian_superagent::WorkerBinding;

        let endpoint = |id: &str| crate::config::EndpointConfig {
            id: id.to_string(),
            addr: "127.0.0.1:1".to_string(),
            queue_timeout_ms: 60_000,
            offline_after_ms: 30_000,
            description: None,
        };
        // Config order deliberately NOT alphabetical.
        let endpoints = [endpoint("zeta"), endpoint("alpha")];
        let input: Vec<String> = endpoints.iter().map(|ep| ep.id.clone()).collect();
        assert_eq!(input, ids(&["zeta", "alpha"]));

        // The unbound row selects the FIRST CONFIGURED endpoint.
        assert_eq!(
            select_side(&WorkerBinding::Unbound, &input),
            SelectionKind::LegacyFirst("zeta"),
            "the legacy-unbound route follows config order"
        );

        // The B1 witness, kept as the reason this test exists: the
        // driver's pre-fix input (`worker_sides.keys()`, i.e. the
        // sorted map keys) would have selected `alpha`.
        let mut map_keys: Vec<String> = input.clone();
        map_keys.sort();
        assert_eq!(map_keys, ids(&["alpha", "zeta"]));
        assert_eq!(
            select_side(&WorkerBinding::Unbound, &map_keys),
            SelectionKind::LegacyFirst("alpha"),
            "the pre-fix (BTreeMap key order) input selected the wrong machine"
        );
        assert_ne!(
            select_side(&WorkerBinding::Unbound, &input),
            select_side(&WorkerBinding::Unbound, &map_keys),
            "config order and key order must disagree on non-alphabetical configs"
        );

        // The order-sensitive change touches ONLY the unbound row:
        // bound (configured) and bound-unconfigured stay
        // order-independent on the same input.
        let bound = WorkerBinding::Bound(std::borrow::Cow::Borrowed("alpha"));
        assert_eq!(select_side(&bound, &input), SelectionKind::Bound("alpha"));
        let gone = WorkerBinding::Bound(std::borrow::Cow::Borrowed("toaster"));
        assert_eq!(
            select_side(&gone, &input),
            SelectionKind::BoundUnconfigured("toaster")
        );
        assert_eq!(
            select_side(&WorkerBinding::Malformed, &input),
            SelectionKind::Malformed
        );
    }

    /// The refusing `exec` tool (D16/D2): the schema is the uniform
    /// `exec` surface, every invoke is the honest refusal naming the
    /// missing worker id — and the tool holds no backend handle, so
    /// it is structurally incapable of enqueueing a command anywhere.
    #[tokio::test]
    async fn refusing_exec_tool_errors_honestly_without_a_backend() {
        let tool = RefusingExecTool::new(Refusal::Unconfigured("toaster".into()));
        // The schema matches the RemoteExecTool surface (name +
        // parameters byte-identical — prompt-prefix neutral).
        let schemas = tool.schemas();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "exec");
        assert_eq!(
            schemas[0].parameters["required"],
            serde_json::json!(["argv"])
        );
        assert_eq!(
            schemas[0].parameters["properties"]["argv"]["type"],
            serde_json::json!("array")
        );

        let err = tool
            .invoke(ToolCall {
                id: "c1".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["ls"]}),
            })
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("exec refused: this session is bound to execution worker `toaster`"),
            "{text}"
        );
        assert!(
            text.contains("no command was queued and nothing ran elsewhere"),
            "{text}"
        );

        // The malformed refusal names the malformed binding.
        let malformed = RefusingExecTool::new(Refusal::Malformed);
        let text = malformed
            .invoke(ToolCall {
                id: "c2".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["ls"]}),
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(text.contains("malformed `exec_worker` binding"), "{text}");
    }
}

// ---------------------------------------------------------------------------
// (R17-C3, pin P2-2) the per-assignee selection — drive-path probes
// over a real DrainCtx (sqlite store), with the side's completion
// inbox as the observable: whichever inbox receives the report names
// the side that drove.
// ---------------------------------------------------------------------------

/// A recording inbox for the selection probes: every admit is tagged
/// with the side that owns it (the observable under test).
#[cfg(test)]
mod assignee_selection_tests {
    use super::*;
    use kian_core::events::{Event, EventKind};
    use kian_core::model::Epoch;
    use kian_orchestrator::{DrainCtx, Metrics, SystemClock};
    use kian_store::{AppendGuard, EventStore};
    use kian_superagent::{
        InboxWriter, RouterDriver, RouterHold, RouterInbox, SpawnIdentity, SpawnRequest,
        WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner, worker_template,
    };
    use std::sync::Mutex as StdMutex;

    /// The tagged inbox: the `tag` names the side whose completion
    /// report landed here.
    struct TaggedInbox {
        tag: &'static str,
        admits: StdMutex<Vec<&'static str>>,
    }

    impl TaggedInbox {
        fn shared(tag: &'static str) -> Arc<Self> {
            Arc::new(Self {
                tag,
                admits: StdMutex::new(Vec::new()),
            })
        }

        fn count(&self) -> usize {
            self.admits.lock().unwrap().len()
        }
    }

    impl InboxWriter for TaggedInbox {
        fn admit(
            &self,
            _session: kian_core::model::SessionId,
            _payload: serde_json::Value,
            _idempotency_key: Option<String>,
        ) -> futures::future::BoxFuture<
            'static,
            Result<kian_superagent::AdmitReceipt, kian_superagent::InboxWriteError>,
        > {
            self.admits.lock().unwrap().push(self.tag);
            Box::pin(async {
                Ok(kian_superagent::AdmitReceipt {
                    deduplicated: false,
                })
            })
        }

        fn wake(&self, _session: kian_core::model::SessionId) -> kian_orchestrator::WakeOutcome {
            kian_orchestrator::WakeOutcome::Folded
        }
    }

    struct NoTools;
    impl kian_core::seams::ToolProvider for NoTools {
        fn schemas(&self) -> Vec<kian_core::seams::ToolSchema> {
            Vec::new()
        }
        fn invoke(
            &self,
            call: kian_core::seams::ToolCall,
        ) -> futures::future::BoxFuture<
            'static,
            Result<kian_core::seams::ToolResult, kian_core::seams::ToolError>,
        > {
            Box::pin(async move {
                Err(kian_core::seams::ToolError::InvalidCall(format!(
                    "no tool `{}`",
                    call.name
                )))
            })
        }
    }

    struct NoopPolicy;
    impl kian_core::seams::RecoveryPolicySource for NoopPolicy {
        fn table(&self) -> Vec<kian_core::seams::PolicyRule> {
            Vec::new()
        }
    }

    /// A minimal ReAct loop over an empty provider registry: driving it
    /// fails softly (no provider for the slot), which still drives the
    /// decorator's completion path — exactly the observable needed.
    fn minimal_loop() -> Arc<kian_loop::ReActLoop> {
        Arc::new(kian_loop::ReActLoop::new(
            worker_template("none", "none", 1),
            Arc::new(kian_llm::turn::ProviderRegistry::new()),
            Arc::new(NoTools),
            Arc::new(NoopPolicy) as Arc<dyn kian_core::seams::RecoveryPolicySource>,
        ))
    }

    /// One side over the minimal inner loop, reporting into `inbox`.
    fn tagged_side(
        inbox: Arc<TaggedInbox>,
    ) -> WorkerLoopDriver<crate::superagent::WorkerInnerDriver> {
        WorkerLoopDriver::new(
            Arc::new(crate::superagent::WorkerInnerDriver::React(minimal_loop())),
            inbox as Arc<dyn InboxWriter>,
            WorkerLoopConfig {
                router: kian_superagent::ResidentBinding::new(SessionId::new()),
                worker_version: 1,
                gate: kian_loop::EvidenceGateConfig::default(),
                multiagent: None,
                board_gating: kian_superagent::BoardGating::default(),
            },
            crate::superagent::port_for_ctx(),
        )
    }

    /// Seeds one roster child stream (created, wrapper with `assignee`,
    /// no thread/started yet — the first drive classifies and emits
    /// lineage) into a fresh sqlite store, and returns the store.
    async fn seed_child(assignee: &str) -> (EventStore, kian_core::model::SessionId, uuid::Uuid) {
        let dir = std::env::temp_dir().join(format!("kian-c3-selection-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let store = EventStore::open(&dir.join("kian.sqlite"))
            .await
            .expect("open store");
        let parent = SessionId::new();
        let identity = SpawnIdentity::derive_delegated(
            parent,
            1,
            "research",
            5,
            None,
            Some(assignee.to_string()),
        );
        let spawner = WorkerSpawner::new(
            kian_superagent::ResidentBinding::new(SessionId::new()),
            &worker_template("none", "none", 1),
            Arc::new(NoopInbox) as Arc<dyn InboxWriter>,
        );
        let spawned = spawner.prepare(&SpawnRequest {
            task: serde_json::json!("do the work"),
            identity: Some(identity),
            ..SpawnRequest::default()
        });
        let session = spawned.worker_session;
        let mut seq = 0u64;
        for (kind, bytes) in [
            (EventKind::SessionCreated, serde_json::json!({})),
            (
                EventKind::InputAdmitted,
                serde_json::json!({"content": spawned.task_message["content"].clone()}),
            ),
        ] {
            seq += 1;
            let event = Event {
                seq,
                session,
                epoch: Epoch(1),
                ts: 1_700_000_000_000 + seq,
                kind,
                bytes,
                version: 0,
            };
            store
                .append(
                    session,
                    event,
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: seq,
                    },
                )
                .await
                .expect("seed append");
        }
        (store, session, spawned.thread.id)
    }

    /// The assembled driver under test: today's worker side reports
    /// into `worker_inbox`; the assignee factory is the test's own.
    fn selection_driver(
        worker_inbox: Arc<TaggedInbox>,
        factory: crate::superagent::AssigneeSideFactory,
    ) -> Arc<SuperAgentDriver> {
        let router_session = SessionId::new();
        let loop_ = minimal_loop();
        let router_side = RouterDriver::new(
            loop_,
            RouterHold::shared(),
            crate::superagent::port_for_ctx(),
            RouterInbox::new(
                Arc::new(NoopInbox) as Arc<dyn InboxWriter>,
                kian_superagent::ResidentBinding::new(router_session),
            ),
        );
        let worker_side = tagged_side(worker_inbox);
        Arc::new(
            SuperAgentDriver::new(
                kian_superagent::ResidentBinding::new(router_session),
                1,
                router_side,
                worker_side,
                minimal_loop(),
            )
            .with_assignee_sides(factory),
        )
    }

    async fn drive_once(
        driver: &Arc<SuperAgentDriver>,
        store: &EventStore,
        session: kian_core::model::SessionId,
    ) {
        let ctx = DrainCtx::new(
            store.clone(),
            session,
            Epoch(2),
            Arc::new(SystemClock),
            Arc::new(Metrics::default()),
        )
        .await
        .expect("ctx");
        let _ = driver.drive(ctx).collect::<Vec<_>>().await;
    }

    struct NoopInbox;
    impl InboxWriter for NoopInbox {
        fn admit(
            &self,
            _session: kian_core::model::SessionId,
            _payload: serde_json::Value,
            _idempotency_key: Option<String>,
        ) -> futures::future::BoxFuture<
            'static,
            Result<kian_superagent::AdmitReceipt, kian_superagent::InboxWriteError>,
        > {
            Box::pin(async {
                Ok(kian_superagent::AdmitReceipt {
                    deduplicated: false,
                })
            })
        }
        fn wake(&self, _session: kian_core::model::SessionId) -> kian_orchestrator::WakeOutcome {
            kian_orchestrator::WakeOutcome::Folded
        }
    }

    /// 🔴 pin P2-2 / §2.1.1:270-274: an assignee whose entry has no
    /// live roster entry runs the REFUSING side and never widens —
    /// the completion report lands in the refusing side's inbox, not
    /// the worker template's (the widening mutation would put it in
    /// the worker inbox).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_assignee_whose_entry_vanished_runs_a_refusing_side_and_never_widens() {
        let worker_inbox = TaggedInbox::shared("worker");
        let known_inbox = TaggedInbox::shared("known");
        let refused_inbox = TaggedInbox::shared("refused");
        let refused = Arc::clone(&refused_inbox);
        let known = Arc::clone(&known_inbox);
        let factory: crate::superagent::AssigneeSideFactory = Arc::new(move |assignee: &str| {
            if assignee == "researcher" {
                tagged_side(Arc::clone(&known))
            } else {
                tagged_side(Arc::clone(&refused))
            }
        });
        let driver = selection_driver(Arc::clone(&worker_inbox), factory);
        let (store, session, _thread) = seed_child("ghost").await;
        drive_once(&driver, &store, session).await;
        assert_eq!(
            refused_inbox.count(),
            1,
            "the vanished assignee's completion lands on the refusing side"
        );
        assert_eq!(known_inbox.count(), 0, "no live entry was consulted");
        assert_eq!(
            worker_inbox.count(),
            0,
            "never a silent fallback onto the (wider) worker template side"
        );
    }

    /// 🔴 constraint 13 / §2.1.1: the assignee is read from the
    /// COMMITTED wrapper, and the side is cached never-evicted — a
    /// later drive of the same child reuses the identical surface even
    /// after the "live" roster map changed underneath. The factory's
    /// call log is the red lever: a re-resolving mutation would call
    /// it again with a live-config id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_assignee_is_read_from_the_committed_wrapper_not_live_config() {
        let worker_inbox = TaggedInbox::shared("worker");
        let a_inbox = TaggedInbox::shared("researcher");
        let b_inbox = TaggedInbox::shared("other");
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let calls_for_factory = Arc::clone(&calls);
        let a = Arc::clone(&a_inbox);
        let b = Arc::clone(&b_inbox);
        let factory: crate::superagent::AssigneeSideFactory = Arc::new(move |assignee: &str| {
            calls_for_factory.lock().unwrap().push(assignee.to_string());
            if assignee == "researcher" {
                tagged_side(Arc::clone(&a))
            } else {
                tagged_side(Arc::clone(&b))
            }
        });
        let driver = selection_driver(Arc::clone(&worker_inbox), factory);
        let (store, session, _thread) = seed_child("researcher").await;
        drive_once(&driver, &store, session).await;
        assert_eq!(a_inbox.count(), 1, "the wrapper's assignee side drove");
        // "Edit the roster after the spawn": a second drive of the
        // same child must neither re-resolve (the factory would be
        // called again — with whatever a live config re-read produced)
        // nor rebuild the side (a different inbox would serve).
        drive_once(&driver, &store, session).await;
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["researcher"],
            "the second drive neither re-resolved live config nor rebuilt the side"
        );
        assert_eq!(a_inbox.count(), 1, "idempotent: the surface is unchanged");
        assert_eq!(b_inbox.count(), 0, "the flipped live entry never served");
        assert_eq!(worker_inbox.count(), 0);
    }
}
