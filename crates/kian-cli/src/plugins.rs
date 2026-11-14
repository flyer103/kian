//! Built-in static plugins for `kian serve` (ADR-004).
//!
//! Five plugins assemble the seam implementations into the plugin
//! [`Ctx`] (kian-plugin):
//!
//! | plugin | registers |
//! |----------------|----------------------------------------------------------|
//! | `openai-compat`| [`ProviderRegistry`] of openai-compatible providers |
//! | `local-tools` | `Arc<dyn ToolProvider>` (built-in fs/exec, kian-loop) |
//! | `react-loop` | [`ReActLoop`] (`Arc<dyn LoopDriver>` built-) |
//! | `superagent` | [`SuperAgentServices`] (the bundle mounted on the |
//! | | Router/Worker/plain three-way driver + subscription |
//! | | engine + mock channels + watchdog wiring) |
//! | `remote-exec` | [`RemoteExecServices`] (the durable queue + |
//! | | per-endpoint `RemoteExecTool` for worker agents) |
//!
//! The `react-loop` plugin declares the first two as dependencies, so
//! activation is deterministically topological and the server
//! assembly simply resolves the loop out of the activated `Ctx`. The
//! plugins are presence-locked to their config sections
//! (fail-closed both directions, see `config.rs`).
//!
//! Secrets discipline: provider API keys are read from the environment
//! at assembly time (`api_key_env`); they never live in `kian.yaml` or
//! in this module.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kian_core::model::{AgentDef, Permission, SessionId};
use kian_core::seams::{ChannelAdapter, RecoveryPolicySource, ToolProvider};
use kian_exec::{
    ConnectionConfig, ConnectionManager, DefaultRoute, ExecStore, LeaveReason, MembershipConfig,
    RegisteredTransport, RegisteredWorker, RegistrationConfig, RegistrationListener,
    RegistrationSink, RemoteExecBackend, RemoteExecConfig, RemoteExecTool, RemoteFsTool,
    RouteTransport, SystemClock, WorkerDirectory, WorkerToken,
};
use kian_llm::provider::{
    OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink, PersistedRequest,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::evidence::EvidenceGateConfig;
use kian_loop::react::{ReActConfig, ReActLoop};
use kian_loop::tools::{FilteredTools, LocalTools, unmatched_tool_names};
use kian_orchestrator::recovery::PolicyTable;
use kian_plugin::{Ctx, Disposer, Plugin, PluginError};
use kian_superagent::{
    MockChannel, RouterInbox, RouterProfile, RouterTools, SubscriptionService, WorkerSpawner,
};

/// An apply-phase failure with the plugin name pre-filled.
fn apply_error(msg: impl Into<String>) -> PluginError {
    PluginError::Apply {
        plugin: "kian-cli builtin".into(),
        source: msg.into().into(),
    }
}

// ---------------------------------------------------------------------------
// openai-compat
// ---------------------------------------------------------------------------

/// Registers the openai-compatible [`ModelProvider`](kian_core::seams::ModelProvider)
/// endpoints (one per `providers:` entry in `kian.yaml`).
pub struct OpenAiCompatPlugin {
    /// (registry name, endpoint config) pairs from `kian.yaml`.
    pub providers: Vec<OpenAiCompatConfig>,
}

impl Plugin for OpenAiCompatPlugin {
    fn name(&self) -> String {
        "openai-compat".into()
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let mut registry = ProviderRegistry::new();
        for cfg in &self.providers {
            if registry.get(&cfg.name).is_some() {
                return Err(apply_error(format!(
                    "duplicate provider name `{}` in kian.yaml",
                    cfg.name
                )));
            }
            registry.register(
                cfg.name.clone(),
                Arc::new(OpenAiCompatProvider::new(
                    cfg.clone(),
                    Arc::new(AuditPersistSink),
                )),
            );
        }
        ctx.provide::<ProviderRegistry>(Arc::new(registry))
    }
}

/// The assembly-time [`PersistSink`] for provider requests.
///
/// A2 persist-what-you-send is enforced twice: the drain-level
/// `step/request` audit event (kian-loop) persists the exact primary
/// request body *before* the turn starts, and the provider hands its
/// bytes to this sink before dialing. This sink records at `debug` (it
/// has no session context — it is built once, at assembly); the durable
/// copy of every model-visible byte lives in the session event stream.
struct AuditPersistSink;

impl PersistSink for AuditPersistSink {
    fn persist_request(
        &self,
        record: PersistedRequest,
    ) -> futures::future::BoxFuture<'static, Result<(), PersistError>> {
        Box::pin(async move {
            tracing::debug!(
                slot = %record.slot.0,
                provider = %record.provider,
                model = %record.model,
                bytes = record.body.len(),
                "provider request bytes persisted-before-send (drain step/request holds the durable copy)"
            );
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// local-tools
// ---------------------------------------------------------------------------

/// Registers the built-in local fs/exec [`ToolProvider`] (kian-loop).
pub struct LocalToolsPlugin {
    /// Filesystem root the fs tools are confined to.
    pub root: std::path::PathBuf,
    /// Default exec timeout, milliseconds.
    pub exec_timeout_ms: u64,
    /// the exec child environment policy
    /// (`tools.exec_env`, default inherit).
    pub env_policy: kian_core::secrets::ExecEnvPolicy,
    /// the exec output redactor (`tools.exec_redact`, default
    /// on; built at assembly from every secret name kian.yaml names).
    pub redactor: kian_core::secrets::SecretRedactor,
}

impl Plugin for LocalToolsPlugin {
    fn name(&self) -> String {
        "local-tools".into()
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let tools = LocalTools::new(self.root)
            .with_exec_timeout(self.exec_timeout_ms)
            .with_env_policy(self.env_policy)
            .with_redactor(self.redactor);
        ctx.provide::<dyn ToolProvider>(Arc::new(tools) as Arc<dyn ToolProvider>)
    }
}

// ---------------------------------------------------------------------------
// react-loop
// ---------------------------------------------------------------------------

/// Assembles the built-in ReAct [`LoopDriver`](kian_core::seams::LoopDriver)
/// from the agent definition, the registered provider registry, the
/// registered tool provider and the declarative recovery-policy table.
pub struct ReActPlugin {
    pub agent: AgentDef,
    pub policy: Arc<PolicyTable>,
    pub config: ReActConfig,
    /// The default agent's evidence gate (wired ). The
    /// default (disabled) keeps built-in behavior identical.
    pub gate: EvidenceGateConfig,
}

impl Plugin for ReActPlugin {
    fn name(&self) -> String {
        "react-loop".into()
    }

    fn dependencies(&self) -> Vec<String> {
        vec!["openai-compat".into(), "local-tools".into()]
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let registry = ctx
            .resolve::<ProviderRegistry>()
            .ok_or_else(|| apply_error("react-loop: ProviderRegistry not registered"))?;
        let tools = ctx
            .resolve::<dyn ToolProvider>()
            .ok_or_else(|| apply_error("react-loop: ToolProvider not registered"))?;
        // the agent's `tools:` list is the mounted surface —
        // fail-closed against the registered provider, then enforced at
        // the tool edge (unlisted schemas never ride the request).
        let unmatched = unmatched_tool_names(&self.agent.tools, &tools.schemas());
        if !unmatched.is_empty() {
            return Err(apply_error(format!(
                "react-loop: agent `{}` lists tools not mounted by the tool provider: {unmatched:?}",
                self.agent.id
            )));
        }
        let tools = Arc::new(FilteredTools::new(tools, &self.agent.tools));
        let react = ReActLoop::with_evidence_gate(
            self.agent,
            registry,
            tools,
            self.policy as Arc<dyn RecoveryPolicySource>,
            self.config,
            self.gate,
        );
        ctx.provide::<ReActLoop>(Arc::new(react))
    }
}

// ---------------------------------------------------------------------------
// remote-exec: the durable queue + per-endpoint tools
// ---------------------------------------------------------------------------

/// The dynamic-membership services (built by [`RemoteExecPlugin`] when a
/// `workers:` section is configured): the registration listener and the
/// shared runtime directory.
pub struct WorkersServices {
    /// The registration listener (started by the superagent assembly's
    /// service start — after the inbox rebuild, like the watchdog).
    pub listener: Arc<RegistrationListener>,
    /// The runtime membership directory (shared with the backend's
    /// dynamic default resolution; snapshots + `member_state` live
    /// there).
    pub directory: Arc<WorkerDirectory>,
    /// The configured `workers.offline_after_ms` (the watchdog's bound
    /// when zero static endpoints exist).
    pub offline_after_ms: u64,
}

/// The running remote-exec services (built by [`RemoteExecPlugin`]):
/// one connection manager and one `RemoteExecTool` per configured
/// endpoint, all over one durable queue — plus, when `workers:` is
/// configured, the dynamic-membership listener.
pub struct RemoteExecServices {
    /// The shared backend (enqueue-first durable queue).
    pub backend: Arc<RemoteExecBackend>,
    /// One manager per endpoint (dial + reconnect; started at build).
    pub managers: Vec<Arc<ConnectionManager>>,
    /// The endpoint configs (registry seeding, watchdog bounds).
    pub endpoints: Vec<crate::config::EndpointConfig>,
    /// The dynamic-membership services (present iff `workers:` is
    /// configured).
    pub workers: Option<WorkersServices>,
}

impl RemoteExecServices {
    /// One [`RemoteExecTool`] per configured endpoint — the worker
    /// agents' exec surface (same schema as the local built-).
    pub fn tools(&self) -> Vec<Arc<RemoteExecTool>> {
        self.endpoints
            .iter()
            .map(|ep| {
                Arc::new(RemoteExecTool::new(
                    Arc::clone(&self.backend),
                    ep.id.clone(),
                ))
            })
            .collect()
    }
}

/// Registers [`RemoteExecServices`] over a pre-opened [`ExecStore`]
/// (the async store open happens in `Server::from_config`; plugin
/// `apply` is sync). Presence-locked to a non-empty `endpoints:` list
/// or a `workers:` section (C6).
pub struct RemoteExecPlugin {
    /// The pre-opened, migrated exec store (same `kian.sqlite`).
    pub store: ExecStore,
    /// The configured endpoints.
    pub endpoints: Vec<crate::config::EndpointConfig>,
    /// The `workers:` section (dynamic membership) — `None` is the
    /// feature off, byte-identical everywhere.
    pub workers: Option<crate::config::WorkersConfig>,
    /// row-lease identity: this replica's `drain.holder`. Under
    /// postgres the config validates it explicit-unique before we get
    /// here; sqlite passes the (defaulted) holder too — the lease is
    /// backend-portable and same-holder claims always land, so
    /// single-process behavior is unchanged.
    pub holder: String,
    /// the exec output redactor — runs once per settled
    /// outcome, before the durable queue row is written.
    pub redactor: kian_core::secrets::SecretRedactor,
}

/// The assembly's [`RegistrationSink`]: membership edges become runtime
/// routes (Q2) — `member_online` attaches the registered connection's
/// transport (its first driver iteration re-drives the id's parked
/// rows), `member_offline` detaches generation-scoped (A15; the rows
/// stay durable and parked).
struct ExecRegistrationSink {
    backend: Arc<RemoteExecBackend>,
}

impl RegistrationSink for ExecRegistrationSink {
    fn member_online(&self, worker: RegisteredWorker) -> futures::future::BoxFuture<'static, ()> {
        let backend = self.backend.clone();
        Box::pin(async move {
            let id = worker.id.clone();
            let generation = worker.generation;
            let description = worker.description.clone();
            let protocol_version = worker.version;
            let transport = RouteTransport::Registered(RegisteredTransport::new(worker));
            backend
                .attach_route_with_declaration(
                    id,
                    transport,
                    generation,
                    description.as_deref(),
                    protocol_version,
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
        let backend = self.backend.clone();
        let id = id.to_string();
        Box::pin(async move {
            backend.detach_route(&id, generation).await;
        })
    }
}

impl Plugin for RemoteExecPlugin {
    fn name(&self) -> String {
        "remote-exec".into()
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let mut routes = Vec::with_capacity(self.endpoints.len());
        let mut managers = Vec::with_capacity(self.endpoints.len());
        for ep in &self.endpoints {
            let addr: std::net::SocketAddr = ep
                .addr
                .parse()
                .map_err(|e| apply_error(format!("remote-exec: endpoint `{}` addr: {e}", ep.id)))?;
            let manager = Arc::new(ConnectionManager::new(ConnectionConfig::new(addr)));
            manager.start();
            routes.push((ep.id.clone(), Arc::clone(&manager)));
            managers.push(manager);
        }
        // Queue timeouts are per-endpoint in config; the backend takes
        // one — use the max so no endpoint's honest timeout is silently
        // shortened (the row's own abandonment timing stays the bound).
        // Dynamic workers use the same backend-level bound (Q3: no new
        // timeout knob).
        let max_timeout = self
            .endpoints
            .iter()
            .map(|ep| ep.queue_timeout_ms)
            .max()
            .unwrap_or(60_000);
        let config = RemoteExecConfig {
            queue_timeout: Duration::from_millis(max_timeout),
            poll_interval: Duration::from_millis(250),
        };
        // The dynamic-membership half (when `workers:` is configured):
        // read the env-only token (fail-closed — the value never
        // travels further than the redacting `WorkerToken`), build the
        // shared directory, the listener over it, and the sink that
        // attaches routes onto the backend below.
        let workers_services = match &self.workers {
            Some(workers) => {
                let value = std::env::var(workers.token_env.trim())
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        apply_error(format!(
                            "remote-exec: workers.token_env `{}` is not set (or blank) — \
                             the shared worker token is env-only; set the variable and \
                             restart `kian serve`",
                            workers.token_env.trim()
                        ))
                    })?;
                let bind = workers
                    .listen_addr()
                    .map_err(|e| apply_error(format!("remote-exec: {}", e)))?;
                let registration = RegistrationConfig::new(
                    bind,
                    WorkerToken::new(value),
                    workers.offline_after_ms,
                )
                .with_static_ids(self.endpoints.iter().map(|ep| ep.id.clone()).collect());
                let registration = RegistrationConfig {
                    allow_public: workers.allow_public,
                    ..registration
                };
                Some((registration, workers.offline_after_ms))
            }
            None => None,
        };
        // The backend is built with the directory (queue-first: zero
        // static routes are fine when the listener will attach them at
        // runtime); the listener then gets the sink holding that
        // backend. The untracked `exec` path defaults to the first
        // static pin when one exists, else the dynamic resolution.
        let directory = WorkerDirectory::new();
        let membership = workers_services
            .as_ref()
            .map(|(_, offline_after_ms)| MembershipConfig {
                directory: Arc::clone(&directory),
                offline_after_ms: *offline_after_ms,
            });
        let default_route = match self.endpoints.first() {
            Some(ep) => DefaultRoute::Static(ep.id.clone()),
            None => DefaultRoute::Dynamic,
        };
        let backend = Arc::new(
            RemoteExecBackend::with_membership(
                self.store,
                routes,
                default_route,
                config,
                self.holder,
                Duration::from_millis(max_timeout),
                self.redactor,
                membership,
            )
            .map_err(apply_error)?,
        );
        let workers = workers_services.map(|(registration, offline_after_ms)| WorkersServices {
            listener: Arc::new(RegistrationListener::new(
                registration,
                Arc::new(SystemClock),
                Arc::new(ExecRegistrationSink {
                    backend: Arc::clone(&backend),
                }),
                Arc::clone(&directory),
            )),
            directory,
            offline_after_ms,
        });
        let services = Arc::new(RemoteExecServices {
            backend,
            managers,
            endpoints: self.endpoints,
            workers,
        });
        ctx.provide::<RemoteExecServices>(services)
    }
}

// ---------------------------------------------------------------------------
// superagent: the bundle mounted on the three-way driver
// ---------------------------------------------------------------------------

/// Registers [`SuperAgentServices`]: the Router/Worker/plain three-way
/// [`SuperAgentDriver`](crate::superagent::SuperAgentDriver) over the
/// bundle's profiles, spawner, dispatch tools and subscription engine,
/// plus the mock channels and the late-bound engine inbox. Everything
/// that needs an async open or the engine happens outside `apply`
/// (`Server::from_config` opens the stores, installs the engine,
/// starts the services after the inbox rebuild).
pub struct SuperAgentPlugin {
    /// The Router's compiled agent definition (`agents.<superagent.router.agent>`).
    pub router_agent: AgentDef,
    /// The Worker's compiled agent definition.
    pub worker_agent: AgentDef,
    /// The default agent's compiled definition (plain sessions).
    pub default_agent: AgentDef,
    /// the worker template's per-call tool permission table
    /// (`agents.<superagent.worker.agent>.permissions`), evaluated on
    /// the worker loop before every `tool/call`.
    pub worker_permissions: BTreeMap<String, Permission>,
    /// the default agent's permission table, evaluated on the
    /// plain-session loop.
    pub default_permissions: BTreeMap<String, Permission>,
    /// The configured resident Router session id — already the
    /// re-arm walk's *live* session: the pinned id itself when its
    /// stream is non-terminal (or never created), else the deterministic
    /// successor at the end of the chain.
    pub router_session: SessionId,
    /// the ended, frozen predecessors of `router_session`
    /// (empty = generation 0). Flows into the Router profile's re-arm
    /// bootstrap (per-generation key + cause-carrying prompt).
    pub router_rearm: Vec<kian_superagent::rearm::EndedResident>,
    /// The Router's slot name.
    pub router_slot: String,
    /// The Router profile's speculation budget: `None` disables
    /// speculation (speculative spawns refused honestly at the tool).
    pub speculation: Option<kian_superagent::SpeculationPolicy>,
    /// The output-side speak-rules: the whether-to-send gate on
    /// `reply`/`deliver_structured`. Empty = ungated (today's
    /// behavior).
    pub speak: kian_superagent::SpeakPolicy,
    /// follow-up ownership (`superagent.followup_ownership`,
    /// default ): live-only fan-out + release-to-fresh-Eval on
    /// all-ended topics, and the `spawn_worker` topic-ownership
    /// refusal at the tool surface. `false` restores the legacy
    /// behavior.
    pub followup_ownership: bool,
    /// the configured policy version
    /// (`superagent.policy.version`), rendered into the resident
    /// profiles' bootstrap prompts as a `[policy <label>]` header.
    /// `None` = today's bytes, byte-identical.
    pub policy_version: Option<kian_superagent::PolicyVersion>,
    /// `superagent.policy.notify_live` (default false) — deliver
    /// one synthetic policy-changed marker input to each drifted live
    /// resident session at boot (idempotent by a stable event id).
    pub policy_notify_live: bool,
    /// (ADR-012): the mandate issuance policy
    /// (`superagent.mandate`), per-worker-template scope texts.
    /// `None` = no mandate issued (byte-identical legacy wrappers).
    pub mandate: Option<kian_superagent::MandatePolicy>,
    /// the outbox bounded-retry configuration
    /// (`superagent.outbox`). `None` = the legacy single-attempt send
    /// path, byte-identical.
    pub outbox: Option<kian_superagent::OutboxConfig>,
    /// Drain tuning shared by all three loops.
    pub max_iterations: usize,
    /// The declarative recovery-policy table.
    pub policy: Arc<PolicyTable>,
    /// The converted subscription rule set (config, fail-closed).
    pub rules: kian_superagent::SubscriptionRuleSet,
    /// The converted admission-class table (config, fail-closed;
    /// empty = today's behavior).
    pub classes: kian_superagent::AdmissionClassTable,
    /// /ADR-013: the converted thread-key derivations
    /// (`superagent.intake.thread_keys`). The derived key surfaces in
    /// spilled events' Inline `neighbors` block as recall context —
    /// correlation is context only, never a routing input. Empty =
    /// no derivation, byte-identical wrappers.
    pub thread_keys: kian_superagent::ThreadKeySet,
    /// admission-time shaping (infinite cap = legacy behavior).
    pub shaping: kian_superagent::ShapingConfig,
    /// untrusted-channel set (prompt-injection hygiene): events
    /// from these channels admit with the untrusted bit inside the
    /// provenance wrapper. Empty = every channel trusted.
    pub untrusted_channels: std::collections::BTreeSet<String>,
    /// Mock channels by configured name (v1's only adapter type).
    pub mock_channels: BTreeMap<String, Arc<MockChannel>>,
    /// Lark channels by configured name: built by the server
    /// from env-gated credentials, registered into the channel
    /// registry and the subscription consumers exactly like mocks.
    pub lark_channels: BTreeMap<String, Arc<kian_superagent::LarkChannel>>,
    /// GitHub channels by configured name: webhook-fed
    /// adapters built by the server (token env-only, fail-closed when
    /// send is configured), registered alongside the others. The
    /// receiver hands verified deliveries to the single channel
    /// through the `WebhookPlane` implementation in `server.rs`.
    pub github_channels: BTreeMap<String, Arc<kian_superagent::GitHubChannel>>,
    /// WeChat channels by configured name: the iLink long-poll
    /// adapters built by the server over the credential file
    /// (fail-closed at assembly), registered alongside the others.
    /// Their receive driver self-starts with the subscription
    /// consumers (`subscribe` starts it); nothing here knows WeChat
    /// exists beyond the registry entry.
    pub wechat_channels: BTreeMap<String, Arc<kian_superagent::WechatChannel>>,
    /// The worker's evidence gate (mounts on the worker wrapper).
    pub worker_gate: EvidenceGateConfig,
    /// The default agent's evidence gate (mounts on the plain loop).
    pub default_gate: EvidenceGateConfig,
    /// The late-bound engine inbox (shared with the server assembly).
    pub inbox: Arc<crate::superagent::EngineInbox>,
    /// Whether endpoints are configured (dependency on `remote-exec`).
    pub has_remote: bool,
    /// Mount the async `start()`/`stop()` pair (ADR-008) on the
    /// Router and Worker loops. Requires `has_remote` (fail-closed in
    /// config). Default off.
    pub async_tools: bool,
    /// The recall toolpack gates and caps: `router` default on,
    /// `worker` default off. The toolpack reads through the same
    /// `StoreRecall` port the runtime exposes (the shared instance is
    /// built once, below).
    pub recall: crate::config::RecallConfig,
    /// the resolved dsh bridge config
    /// (`superagent.harness.dsh`, default off). When present the
    /// worker template's inner loop is the `DshDriver`.
    pub dsh_harness: Option<kian_dsh::DshDriverConfig>,
    /// The timer input sources, built by the server over the
    /// system clock; registered into the channel registry and the
    /// consumer spawn exactly like mocks. Their run tasks start in
    /// `start_services`, after the consumers exist.
    pub timer_channels: Vec<Arc<kian_superagent::TimerChannel>>,
    /// The resident watchdog profile, when configured. The loop
    /// and driver are built below; the bootstrap happens in
    /// `start_services`.
    pub watchdog: Option<kian_superagent::WatchdogProfile>,
    /// The event store (the completion bridge reads session
    /// projections and resolves invoking sessions from committed
    /// `tool/call` events). Only used when `async_tools` is on.
    pub event_store: kian_store::EventStore,
}

impl Plugin for SuperAgentPlugin {
    fn name(&self) -> String {
        "superagent".into()
    }

    fn dependencies(&self) -> Vec<String> {
        let mut deps = vec!["openai-compat".into(), "local-tools".into()];
        if self.has_remote {
            deps.push("remote-exec".into());
        }
        deps
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let registry = ctx
            .resolve::<ProviderRegistry>()
            .ok_or_else(|| apply_error("superagent: ProviderRegistry not registered"))?;
        let local_tools = ctx
            .resolve::<dyn ToolProvider>()
            .ok_or_else(|| apply_error("superagent: ToolProvider not registered"))?;
        let remote = if self.has_remote {
            Some(
                ctx.resolve::<RemoteExecServices>()
                    .ok_or_else(|| apply_error("superagent: RemoteExecServices not registered"))?,
            )
        } else {
            None
        };

        // the async `start()`/`stop()` pair, config-gated (default
        // off). The engine shares the backend (no second dispatch
        // system); the completion bridge admits through the same
        // late-bound inbox port the bundle uses, and resolves invoking
        // sessions from committed `tool/call` events.
        //
        // (ADR-020, D11 → R12, S3): carry, not refuse — a **jobs
        // factory** keyed by worker id (`async_jobs.endpoint_id` is
        // already a persisted column; dispatch is `exec_tracked(job_id,
        // &endpoint_id, …)`), with one shared metrics handle and one
        // shared completion sink. The lazy side factory builds each
        // id's pair on first use; the Router keeps the first static
        // endpoint's pair (the legacy behavior — the Router is not a
        // bound worker); a workers-only deployment honestly mounts no
        // Router pair. The boot sweep (`start_services`) materializes
        // one instance per known id.
        let mut async_metrics = None;
        let mut async_pair: Option<Arc<dyn ToolProvider>> = None;
        let mut async_validation_pair: Option<Arc<dyn ToolProvider>> = None;
        let mut async_jobs_factory: Option<crate::superagent::AsyncJobsFactory> = None;
        if self.async_tools {
            let remote = remote.as_ref().ok_or_else(|| {
                apply_error("superagent.async_tools requires the remote-exec plugin")
            })?;
            // The presence lock (C6) guarantees `endpoints:` non-empty
            // or `workers:` present; a workers-only deployment is
            // valid (per-side pairs ride the lazy sides).
            let metrics = Arc::new(crate::async_bridge::AsyncBridgeMetrics::default());
            let sink = crate::async_bridge::CompletionBridge::new(
                Arc::clone(&self.inbox) as Arc<dyn kian_superagent::InboxWriter>,
                self.event_store.clone(),
                Arc::clone(&metrics),
            );
            let session_for_call = crate::async_bridge::session_for_call(self.event_store.clone());
            let jobs_backend = Arc::clone(&remote.backend);
            let jobs_sink = Arc::clone(&sink) as Arc<dyn kian_exec::AsyncResultSink>;
            let jobs_session_for_call = Arc::clone(&session_for_call);
            let jobs_factory = Arc::new(move |id: &str| {
                kian_exec::AsyncJobs::new(
                    Arc::clone(&jobs_backend),
                    id.to_string(),
                    Arc::clone(&jobs_sink),
                    Arc::clone(&jobs_session_for_call),
                )
            });
            // The Router's default pair is the first static
            // endpoint's — the legacy behavior with one endpoint
            // (default == only worker), byte-identical.
            if let Some(first) = remote.endpoints.first() {
                async_pair = Some(
                    Arc::new(kian_exec::AsyncToolPair::new(jobs_factory(&first.id)))
                        as Arc<dyn ToolProvider>,
                );
            }
            // A representative pair for the worker-template surface
            // validation below (every pair's schemas are identical —
            // S5: one uniform tool surface per side).
            let representative = remote
                .endpoints
                .first()
                .map(|ep| ep.id.as_str())
                .unwrap_or("worker");
            async_validation_pair = Some(Arc::new(kian_exec::AsyncToolPair::new(jobs_factory(
                representative,
            ))) as Arc<dyn ToolProvider>);
            async_jobs_factory = Some(jobs_factory);
            async_metrics = Some(metrics);
        }

        // The structural profile rules are assembly-time assertions:
        // a worker smuggling a channel tool or a router without the
        // dispatch tool refuses to start.
        let profile = kian_superagent::SuperAgentProfile::new(
            self.router_agent.clone(),
            self.worker_agent.clone(),
        )
        .map_err(|e| apply_error(format!("superagent profile: {e}")))?;
        // the Router profile carries the configured speculation
        // budget (None = disabled, speculative spawns refused honestly).
        let mut profile = RouterProfile::new(self.router_session, profile.router);
        profile.speculation = self.speculation.clone();
        profile.speak = self.speak.clone();
        // the follow-up-ownership gate rides the profile into the
        // tool surface.
        profile.followup_ownership = self.followup_ownership;
        // the policy version rides the profile into the bootstrap
        // render (header over the rendered prompt).
        profile.policy_version = self.policy_version.clone();
        // (ADR-012): the mandate issuance policy rides the profile
        // into the spawn tool (the single issuance point).
        profile.mandate = self.mandate.clone();
        // (R12, R1/R2 — supersedes ADR-020 D5/D7): membership on iff
        // `endpoints:` is non-empty or `workers:` is configured — the
        // bootstrap then appends the frozen membership block (a
        // constant; ids never render). Off = today's bytes.
        if let Some(remote) = &remote {
            profile.worker_membership = !remote.endpoints.is_empty() || remote.workers.is_some();
        }
        // the outbox bounded-retry config rides the profile (an
        // explicitly-built driver below overrides the auto-built one,
        // sharing one handle with the boot re-drive).
        profile.outbox = self.outbox;
        // the re-arm chain — empty for generation 0.
        profile.re_arm = self.router_rearm;

        // The mounted inbox: admits counted for Router residency.
        let router_inbox = RouterInbox::new(
            Arc::clone(&self.inbox) as Arc<dyn kian_superagent::InboxWriter>,
            self.router_session,
        );

        // Dispatch tools over the spawner + channel registry + hold.
        let spawner = Arc::new(WorkerSpawner::new(
            self.router_session,
            &self.worker_agent,
            Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>,
        ));
        let mut channels = kian_superagent::ChannelRegistry::new();
        let mut adapters = Vec::with_capacity(
            self.mock_channels.len()
                + self.lark_channels.len()
                + self.github_channels.len()
                + self.wechat_channels.len()
                + self.timer_channels.len(),
        );
        for (name, mock) in &self.mock_channels {
            channels.register(name.clone(), Arc::clone(mock) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(mock) as Arc<dyn ChannelAdapter>);
        }
        // real lark adapters ride the same registry + consumer
        // spawn (the adapter is a plugin over the seam — nothing in
        // the subscription engine knows Feishu exists).
        for (name, lark) in &self.lark_channels {
            channels.register(name.clone(), Arc::clone(lark) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(lark) as Arc<dyn ChannelAdapter>);
        }
        // the github adapter rides the same registry +
        // consumer spawn; its receive half is fed exclusively by the
        // webhook receiver's verified hand-off (nothing here knows
        // HTTP delivery exists).
        for (name, github) in &self.github_channels {
            channels.register(name.clone(), Arc::clone(github) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(github) as Arc<dyn ChannelAdapter>);
        }
        // the wechat iLink adapter rides the same registry + consumer
        // spawn; its receive half is its own long-poll loop (started
        // by `subscribe` inside the consumer spawn — consumers first,
        // so no polled event lands before a subscriber exists).
        for (name, wechat) in &self.wechat_channels {
            channels.register(name.clone(), Arc::clone(wechat) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(wechat) as Arc<dyn ChannelAdapter>);
        }
        // the timers ride the same registry + consumer spawn (a
        // timer is a channel — nothing here knows a watchdog exists).
        for timer in &self.timer_channels {
            let name = timer.channel_name().to_string();
            channels.register(name, Arc::clone(timer) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(timer) as Arc<dyn ChannelAdapter>);
        }
        let hold = kian_superagent::RouterHold::shared();
        // one shared registry projection for the tool surface and
        // the subscription service — the spawn-time ownership check and
        // the fan-out resolution read the same map (still disposable:
        // `start_services` rebuilds its contents from streams at boot).
        let followup_registry = Arc::new(std::sync::RwLock::new(
            kian_superagent::PassthroughRegistry::new(),
        ));
        // one outbox driver shared by the tool surface (retry +
        // ordering + park) and the boot re-drive (`start_services`
        // rebuilds pending intents from the Router's stream and
        // re-drives them through this handle).
        let outbox_driver = self.outbox.map(|config| {
            Arc::new(kian_superagent::OutboxDriver::new(
                config,
                Arc::new(kian_superagent::SystemRetryClock),
                channels.clone(),
            ))
        });
        let mut router_tools = RouterTools::new(spawner, channels.clone(), &profile, hold.clone())
            .with_followup_registry(Arc::clone(&followup_registry));
        if let Some(driver) = &outbox_driver {
            router_tools = router_tools.with_outbox_driver(Arc::clone(driver));
        }
        // (R12, R1/R4/S4): the membership surface — static pin ids
        // (config order), per-id queue timeouts, the worker-streams
        // read port and the read-only directory port (the runtime
        // directory + the durable registry rows + the static config).
        // Installed only when the remote-exec services exist (the C6
        // presence lock guarantees `endpoints:` or `workers:`); zero
        // config installs nothing — feature off, no port reads.
        if let Some(remote) = &remote {
            let static_ids: Vec<String> = remote.endpoints.iter().map(|ep| ep.id.clone()).collect();
            let timeouts: std::collections::BTreeMap<String, u64> = remote
                .endpoints
                .iter()
                .map(|ep| (ep.id.clone(), ep.queue_timeout_ms))
                .collect();
            let worker_streams =
                crate::superagent::StoreWorkerStreams::new(self.event_store.clone());
            // An empty directory stand-in for a static-only deployment
            // (no listener exists to fill it; the registry rows and
            // the static config still flow through the port).
            let directory = remote
                .workers
                .as_ref()
                .map(|w| Arc::clone(&w.directory))
                .unwrap_or_else(kian_exec::WorkerDirectory::new);
            let directory_port = crate::superagent::StoreWorkerDirectory::new(
                Arc::clone(&remote.backend),
                directory,
                remote.endpoints.clone(),
                remote
                    .workers
                    .as_ref()
                    .map(|w| w.offline_after_ms)
                    .unwrap_or(30_000),
            );
            router_tools = router_tools
                .with_worker_membership(
                    static_ids,
                    timeouts,
                    Some(directory_port as Arc<dyn kian_superagent::WorkerDirectoryPort>),
                )
                .with_worker_streams(worker_streams);
        }
        let router_tools = Arc::new(router_tools);
        // the recall toolpack reads through the one `StoreRecall`
        // port the runtime also exposes (the spill index it holds is
        // rebuilt by `start_services`). Config-gated: default on for
        // the Router, opt-in per worker template.
        let recall_port = crate::superagent::StoreRecall::new(self.event_store.clone());
        let recall_tools = kian_superagent::RecallTools::new(
            recall_port.clone() as Arc<dyn kian_superagent::RecallPort>,
            self.router_session,
            self.shaping.clone(),
            self.recall.to_runtime(),
        );
        // +: the Router's combined provider — async pair (when
        // ), dispatch/channel tools, then recall. All names are
        // unique, so the first-wins union shadows nothing; the request
        // prefix order is the loop's name sort (stable either way).
        let mut router_providers: Vec<Arc<dyn ToolProvider>> = Vec::new();
        if let Some(pair) = &async_pair {
            router_providers.push(Arc::clone(pair));
        }
        router_providers.push(router_tools as Arc<dyn ToolProvider>);
        if self.recall.router {
            router_providers.push(Arc::new(recall_tools.clone()) as Arc<dyn ToolProvider>);
        }
        let router_tools: Arc<dyn ToolProvider> = match router_providers.len() {
            0 | 1 => router_providers
                .into_iter()
                .next()
                .expect("the dispatch provider is always present"),
            _ => Arc::new(crate::superagent::CombinedTools::new(router_providers)),
        };

        // Router loop: cheap slot, dispatch tools, ungated (a resident
        // judge never settles; a gate would be meaningless).
        let router_loop = Arc::new(ReActLoop::with_config(
            self.router_agent,
            registry.clone(),
            router_tools,
            Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
            ReActConfig {
                slot: kian_core::model::SlotName(self.router_slot.clone()),
                max_iterations: self.max_iterations,
                budget_scope: kian_loop::BudgetScope::PerEpisode,
                permissions: BTreeMap::new(),
            },
        ));

        // Worker loop: strong main slot, local tools shadowed by the
        // per-endpoint remote exec tools when configured, ungated here
        // (the worker wrapper owns the gate).
        // the worker template's `tools:` list filters the local
        // built-in surface (the remote/async/recall surfaces are already
        // config-gated) and is validated fail-closed against the loop's
        // combined mounted surface — a typo'd name refuses boot.
        let mut worker_mounted: Vec<kian_core::seams::ToolSchema> = Vec::new();
        if let Some(remote) = &remote {
            for tool in remote.tools() {
                worker_mounted.extend(tool.schemas());
            }
            // v2: the worker fs surface rides the same (static or
            // dynamic) endpoints; every worker side mounts it filtered
            // by the agent's `tools:` list.
            worker_mounted.extend(kian_exec::RemoteFsTool::fs_schemas());
        }
        if let Some(pair) = &async_validation_pair {
            worker_mounted.extend(pair.schemas());
        }
        if self.recall.worker {
            worker_mounted.extend(recall_tools.schemas());
        }
        worker_mounted.extend(local_tools.schemas());
        let unmatched = unmatched_tool_names(&self.worker_agent.tools, &worker_mounted);
        if !unmatched.is_empty() {
            return Err(apply_error(format!(
                "superagent worker agent `{}` lists tools not mounted on the worker loop: {unmatched:?}",
                self.worker_agent.id
            )));
        }
        let mut worker_tools: Vec<Arc<dyn ToolProvider>> = Vec::new();
        if let Some(remote) = &remote {
            for tool in remote.tools() {
                let endpoint_id = tool.endpoint_id().to_string();
                let provider: Arc<dyn ToolProvider> = tool;
                worker_tools.push(provider);
                worker_tools.push(Arc::new(FilteredTools::new(
                    Arc::new(RemoteFsTool::new(Arc::clone(&remote.backend), endpoint_id)),
                    &self.worker_agent.tools,
                )));
            }
        }
        // the single legacy worker side (feature off, or the
        // nothing-known fallback) never mounts the async pair: a
        // pair is bound to one worker id, and this side is not a
        // bound worker. With membership on, the lazy sides carry
        // their own per-id pairs (S3).
        // the worker template mounts the recall toolpack when
        // opted in (config-gated, default off — after the async pair,
        // before the local tools; stable order, unique names).
        if self.recall.worker {
            worker_tools.push(Arc::new(recall_tools.clone()) as Arc<dyn ToolProvider>);
        }
        worker_tools.push(Arc::new(FilteredTools::new(
            local_tools.clone(),
            &self.worker_agent.tools,
        )));
        let worker_loop = Arc::new(ReActLoop::with_config(
            self.worker_agent.clone(),
            registry.clone(),
            Arc::new(crate::superagent::CombinedTools::new(worker_tools)),
            Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
            ReActConfig {
                slot: kian_core::model::SlotName("main".into()),
                max_iterations: self.max_iterations,
                budget_scope: kian_loop::BudgetScope::Lifetime,
                permissions: self.worker_permissions.clone(),
            },
        ));
        // the worker template's inner loop is config-gated — the
        // dsh bridge replaces the built-in ReAct loop when
        // `superagent.harness.dsh` is on (default off; the decorator,
        // evidence gate and completion report above it are unchanged).
        // The config is resolved fail-closed by the server assembly
        // (`DshHarnessConfig::to_runtime`: command from config or
        // `KIAN_DSH_BIN`); the mount just builds the driver from it.
        let worker_inner = match &self.dsh_harness {
            Some(config) => crate::superagent::WorkerInnerDriver::Dsh(Arc::new(
                kian_dsh::DshDriver::new(config.clone()),
            )),
            None => crate::superagent::WorkerInnerDriver::React(worker_loop),
        };

        // Plain-session loop: the default agent, with its own gate. The
        // agent's `tools:` list filters the local surface (
        // fail-closed like the worker's).
        let default_agent = self.default_agent;
        let unmatched = unmatched_tool_names(&default_agent.tools, &local_tools.schemas());
        if !unmatched.is_empty() {
            return Err(apply_error(format!(
                "superagent default agent `{}` lists tools not mounted on the plain-session loop: {unmatched:?}",
                default_agent.id
            )));
        }
        let default_tools = Arc::new(FilteredTools::new(
            local_tools.clone(),
            &default_agent.tools,
        ));
        let default_loop = Arc::new(ReActLoop::with_evidence_gate(
            default_agent,
            registry.clone(),
            default_tools,
            Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
            ReActConfig {
                slot: kian_core::model::SlotName("main".into()),
                max_iterations: self.max_iterations,
                budget_scope: kian_loop::BudgetScope::Lifetime,
                permissions: self.default_permissions.clone(),
            },
            self.default_gate,
        ));

        // the resident watchdog loop (when configured) — a
        // worker-template agent whose tools are exactly the
        // read/intervene surface, over the same recall port, the
        // engine inbox, the shared channel registry (escalation) and
        // its own hold. Driven by the Router's residency shape: one
        // judge turn per admitted tick, parked between ticks.
        // its mounted inbox is hoisted for the services (the
        // drift notice admits through it).
        let mut mounted_watchdog_inbox: Option<Arc<RouterInbox>> = None;
        let watchdog_side = self.watchdog.as_ref().map(|profile| {
            // One hold shared by the driver (per-hold port install)
            // and the tools (the channel/delivered surface).
            let watchdog_hold = kian_superagent::RouterHold::shared();
            let tools = kian_superagent::WatchdogTools::new(
                recall_port.clone() as Arc<dyn kian_superagent::RecallPort>,
                self.router_session,
                Arc::clone(&self.inbox) as Arc<dyn kian_superagent::InboxWriter>,
                channels.clone(),
                kian_superagent::ImPolicy::default(),
                watchdog_hold.clone(),
                Arc::new(kian_superagent::SystemTurnClock),
            );
            let watchdog_inbox = RouterInbox::new(
                Arc::clone(&self.inbox) as Arc<dyn kian_superagent::InboxWriter>,
                profile.session,
            );
            // the mounted inbox rides the services so the boot-time
            // policy drift notice admits through the residency counter
            // a parked resident polls (a raw-engine admit would strand
            // behind the park).
            mounted_watchdog_inbox = Some(watchdog_inbox.clone());
            let watchdog_loop = Arc::new(ReActLoop::with_config(
                profile.agent.clone(),
                registry.clone(),
                Arc::new(tools) as Arc<dyn ToolProvider>,
                Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
                ReActConfig {
                    slot: kian_core::model::SlotName("main".into()),
                    max_iterations: self.max_iterations,
                    budget_scope: kian_loop::BudgetScope::PerEpisode,
                    permissions: BTreeMap::new(),
                },
            ));
            kian_superagent::RouterDriver::new(
                watchdog_loop,
                watchdog_hold,
                crate::superagent::port_for_ctx(),
                watchdog_inbox,
            )
        });

        // The three-way driver. The Router side mounts judge-turn
        // metrics: batch size + wall duration per judge turn,
        // bundle-owned counters the /metrics route snapshots.
        let router_metrics = kian_superagent::RouterMetrics::shared();
        let router_side = kian_superagent::RouterDriver::with_metrics(
            router_loop,
            hold.clone(),
            crate::superagent::port_for_ctx(),
            router_inbox.clone(),
            kian_superagent::RouterTurnRecorder::new(
                Arc::clone(&router_metrics),
                Arc::new(kian_superagent::SystemTurnClock),
            ),
        );
        let worker_side = kian_superagent::WorkerLoopDriver::new(
            Arc::new(worker_inner),
            Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>,
            kian_superagent::WorkerLoopConfig {
                router: self.router_session,
                worker_version: self.worker_agent.version,
                gate: crate::superagent::clone_gate(&self.worker_gate),
            },
            crate::superagent::port_for_ctx(),
        );

        // (R12, S1/S3/S5 — supersedes ADR-020 D8's boot-prebuilt
        // loop): the lazy per-worker-id side factory. Each id's side
        // mounts EXACTLY ONE `exec` (its id's `RemoteExecTool`), its
        // own async pair bound to the same id, the shared recall
        // toolpack and the filtered local tools. Tool names and
        // schemas are identical across sides ⇒ prompt-prefix neutral
        // across sessions (and across joins/leaves — a side is built
        // the same way whatever the membership state). The driver
        // caches the built sides forever (never evicted, queue-first).
        // The refusing-side factory covers the unconfigured-bound and
        // malformed rows (D16/D2): a worker loop whose only `exec` is
        // the honest refusal — no backend handle, zero enqueues.
        // Zero remote services installs nothing: the single
        // `worker_side` above is today's exact path.
        let mut side_factory: Option<crate::superagent::WorkerSideFactory> = None;
        let mut worker_order: Vec<String> = Vec::new();
        let mut refuse_side: Option<crate::superagent::RefuseSideFactory> = None;
        let mut directory_for_sides: Option<Arc<dyn kian_superagent::WorkerDirectoryPort>> = None;
        if let Some(remote) = &remote {
            worker_order = remote.endpoints.iter().map(|ep| ep.id.clone()).collect();
            let worker_react_config = ReActConfig {
                slot: kian_core::model::SlotName("main".into()),
                max_iterations: self.max_iterations,
                budget_scope: kian_loop::BudgetScope::Lifetime,
                permissions: self.worker_permissions.clone(),
            };
            let side_backend = Arc::clone(&remote.backend);
            let side_registry = registry.clone();
            let side_agent = self.worker_agent.clone();
            let side_policy: Arc<dyn RecoveryPolicySource> =
                Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>;
            let side_config = worker_react_config.clone();
            let side_local = local_tools.clone();
            let side_recall = self.recall.worker;
            let side_recall_tools = recall_tools.clone();
            let side_inbox = Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>;
            let side_gate = crate::superagent::clone_gate(&self.worker_gate);
            let side_session = self.router_session;
            let side_version = self.worker_agent.version;
            let side_jobs_factory = async_jobs_factory.clone();
            side_factory = Some(Arc::new(move |id: &str, _description: Option<&str>| {
                let mut tools: Vec<Arc<dyn ToolProvider>> = Vec::new();
                tools.push(Arc::new(kian_exec::RemoteExecTool::new(
                    Arc::clone(&side_backend),
                    id.to_string(),
                )));
                tools.push(Arc::new(FilteredTools::new(
                    Arc::new(kian_exec::RemoteFsTool::new(
                        Arc::clone(&side_backend),
                        id.to_string(),
                    )),
                    &side_agent.tools,
                )));
                if let Some(jobs_factory) = &side_jobs_factory {
                    tools.push(Arc::new(kian_exec::AsyncToolPair::new(jobs_factory(id)))
                        as Arc<dyn ToolProvider>);
                }
                if side_recall {
                    tools.push(Arc::new(side_recall_tools.clone()) as Arc<dyn ToolProvider>);
                }
                tools.push(Arc::new(FilteredTools::new(
                    side_local.clone(),
                    &side_agent.tools,
                )));
                let side_loop = Arc::new(ReActLoop::with_config(
                    side_agent.clone(),
                    side_registry.clone(),
                    Arc::new(crate::superagent::CombinedTools::new(tools)),
                    Arc::clone(&side_policy),
                    side_config.clone(),
                ));
                kian_superagent::WorkerLoopDriver::new(
                    Arc::new(crate::superagent::WorkerInnerDriver::React(side_loop)),
                    Arc::clone(&side_inbox),
                    kian_superagent::WorkerLoopConfig {
                        router: side_session,
                        worker_version: side_version,
                        gate: crate::superagent::clone_gate(&side_gate),
                    },
                    crate::superagent::port_for_ctx(),
                )
            }));
            // The refusing-side factory (built per drive — the refusal
            // rows are rare and never on the hot path; a fresh loop per
            // drive is deterministic, no resident routing state).
            let refuse_registry = registry.clone();
            let refuse_agent = self.worker_agent.clone();
            let refuse_policy: Arc<dyn RecoveryPolicySource> =
                Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>;
            let refuse_config = worker_react_config;
            let refuse_local = local_tools.clone();
            let refuse_recall = self.recall.worker;
            let refuse_recall_tools = recall_tools.clone();
            let refuse_inbox = Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>;
            let refuse_gate = crate::superagent::clone_gate(&self.worker_gate);
            let refuse_session = self.router_session;
            let refuse_version = self.worker_agent.version;
            refuse_side = Some(Arc::new(move |refusal: &crate::superagent::Refusal| {
                let mut tools: Vec<Arc<dyn ToolProvider>> = Vec::new();
                tools.push(Arc::new(crate::superagent::RefusingExecTool::new(
                    refusal.clone(),
                )));
                tools.push(Arc::new(FilteredTools::new(
                    Arc::new(crate::superagent::RefusingFsTool::new(refusal.clone())),
                    &refuse_agent.tools,
                )));
                if refuse_recall {
                    tools.push(Arc::new(refuse_recall_tools.clone()) as Arc<dyn ToolProvider>);
                }
                tools.push(Arc::new(FilteredTools::new(
                    refuse_local.clone(),
                    &refuse_agent.tools,
                )));
                let refuse_loop = Arc::new(ReActLoop::with_config(
                    refuse_agent.clone(),
                    refuse_registry.clone(),
                    Arc::new(crate::superagent::CombinedTools::new(tools)),
                    Arc::clone(&refuse_policy),
                    refuse_config.clone(),
                ));
                kian_superagent::WorkerLoopDriver::new(
                    Arc::new(crate::superagent::WorkerInnerDriver::React(refuse_loop)),
                    Arc::clone(&refuse_inbox),
                    kian_superagent::WorkerLoopConfig {
                        router: refuse_session,
                        worker_version: refuse_version,
                        gate: crate::superagent::clone_gate(&refuse_gate),
                    },
                    crate::superagent::port_for_ctx(),
                )
            }));
            // The driver's known-ids source: the same read-only
            // directory port the Router tools read (static ∪ live
            // directory ∪ registry rows).
            let directory = remote
                .workers
                .as_ref()
                .map(|w| Arc::clone(&w.directory))
                .unwrap_or_else(kian_exec::WorkerDirectory::new);
            directory_for_sides = Some(crate::superagent::StoreWorkerDirectory::new(
                Arc::clone(&remote.backend),
                directory,
                remote.endpoints.clone(),
                remote
                    .workers
                    .as_ref()
                    .map(|w| w.offline_after_ms)
                    .unwrap_or(30_000),
            )
                as Arc<dyn kian_superagent::WorkerDirectoryPort>);
        }
        let mut driver = crate::superagent::SuperAgentDriver::new(
            self.router_session,
            self.worker_agent.version,
            router_side,
            worker_side,
            default_loop,
        );
        if let Some(profile) = &self.watchdog {
            driver = driver.with_watchdog(
                profile.session,
                watchdog_side
                    .expect("the watchdog side is built exactly when the profile is present"),
            );
        }
        // (R12, S1/S2): the lazy side factory + static order + the
        // refusing factory + the directory port ride the driver —
        // with none installed (zero endpoints and no workers:) the
        // driver routes exactly as before.
        if let (Some(side_factory), Some(refuse_side)) = (side_factory, refuse_side) {
            driver = driver.with_lazy_worker_sides(
                side_factory,
                worker_order,
                refuse_side,
                directory_for_sides,
            );
        }
        let driver = Arc::new(driver);

        let subscriptions = Arc::new(
            SubscriptionService::with_classes(
                self.router_session,
                self.rules,
                self.classes,
                Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>,
            )
            .with_shaping(self.shaping)
            .with_untrusted_channels(self.untrusted_channels)
            // /ADR-013: the derived thread key joins spilled
            // events' Inline neighbors as recall context (context
            // only — routing stays topic-keyed).
            .with_thread_keys(self.thread_keys.clone())
            // the fan-out and the spawn tool share one registry
            // projection, and the ownership gate rides the same flag.
            .with_shared_registry(Arc::clone(&followup_registry))
            .with_followup_ownership(self.followup_ownership)
            // (ADR-012): the fan-out's mandate revalidation compares
            // against the same current pointer the bootstrap renders and
            // the spawn tool stamps — one configured version everywhere.
            .with_policy_pointer(
                self.policy_version
                    .as_ref()
                    .map(|version| version.label().to_string()),
            ),
        );
        let subscription_metrics = Arc::new(crate::superagent::SubscriptionMetrics::default());

        let services = Arc::new(crate::superagent::SuperAgentServices {
            driver,
            inbox: self.inbox,
            router_inbox,
            profile,
            subscriptions,
            mock_channels: self.mock_channels,
            lark_channels: self.lark_channels,
            github_channels: self.github_channels,
            wechat_channels: self.wechat_channels,
            adapters,
            worker_gate: self.worker_gate,
            subscription_metrics,
            router_metrics,
            // boot-time policy drift marking (audit + counter +
            // the config-gated marker notice).
            policy: self.policy_version.clone(),
            policy_notify_live: self.policy_notify_live,
            policy_drift: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            // the outbox driver + the Router hold (the boot
            // re-drive waits for the hold's first port, then re-sends
            // the rebuilt pending intents through this handle).
            outbox: outbox_driver,
            router_hold: hold,
            watchdog_inbox: mounted_watchdog_inbox,
            remote,
            // (R12, S3): the async jobs factory — `start_services`
            // materializes one instance per known id (static pins ∪
            // registry rows) for the boot sweep; the lazy sides build
            // their own instances on first use.
            async_jobs_factory,
            async_metrics,
            // The one shared recall port: the toolpack and the runtime
            // surface (and the startup spill-index rebuild) all use it.
            recall: recall_port,
            timers: self.timer_channels,
            watchdog: self.watchdog.clone(),
        });
        ctx.provide::<crate::superagent::SuperAgentServices>(services)
    }
}
