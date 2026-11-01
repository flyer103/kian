//! Built-in static plugins for `kian serve` (card C13, ADR-004).
//!
//! Five plugins assemble the seam implementations into the plugin
//! [`Ctx`] (kian-plugin C7):
//!
//! | plugin         | registers                                                |
//! |----------------|----------------------------------------------------------|
//! | `openai-compat`| [`ProviderRegistry`] of openai-compatible providers       |
//! | `local-tools`  | `Arc<dyn ToolProvider>` (built-in fs/exec, kian-loop C11) |
//! | `react-loop`   | [`ReActLoop`] (`Arc<dyn LoopDriver>` built-in, C11)       |
//! | `superagent`   | [`SuperAgentServices`] (C27: the bundle mounted on the    |
//! |                | Router/Worker/plain three-way driver + subscription       |
//! |                | engine + mock channels + watchdog wiring)                |
//! | `remote-exec`  | [`RemoteExecServices`] (C27: the C20 durable queue +      |
//! |                | per-endpoint `RemoteExecTool` for worker agents)         |
//!
//! The `react-loop` plugin declares the first two as dependencies, so
//! activation is deterministically topological (C7) and the server
//! assembly simply resolves the loop out of the activated `Ctx`. The
//! C27 plugins are presence-locked to their config sections
//! (fail-closed both directions, see `config.rs`).
//!
//! Secrets discipline: provider API keys are read from the environment
//! at assembly time (`api_key_env`); they never live in `kian.yaml` or
//! in this module.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kian_core::model::{AgentDef, SessionId};
use kian_core::seams::{ChannelAdapter, RecoveryPolicySource, ToolProvider};
use kian_exec::{
    ConnectionConfig, ConnectionManager, ExecStore, RemoteExecBackend, RemoteExecConfig,
    RemoteExecTool,
};
use kian_llm::provider::{
    OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink, PersistedRequest,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::evidence::EvidenceGateConfig;
use kian_loop::react::{ReActConfig, ReActLoop};
use kian_loop::tools::LocalTools;
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
/// `step/request` audit event (kian-loop C11) persists the exact primary
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

/// Registers the built-in local fs/exec [`ToolProvider`] (kian-loop C11).
pub struct LocalToolsPlugin {
    /// Filesystem root the fs tools are confined to.
    pub root: std::path::PathBuf,
    /// Default exec timeout, milliseconds.
    pub exec_timeout_ms: u64,
}

impl Plugin for LocalToolsPlugin {
    fn name(&self) -> String {
        "local-tools".into()
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let tools = LocalTools::new(self.root).with_exec_timeout(self.exec_timeout_ms);
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
    /// The default agent's evidence gate (C24, wired by C27). The
    /// default (disabled) keeps Phase-1 behavior identical.
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
// remote-exec (C27): the C20 durable queue + per-endpoint tools
// ---------------------------------------------------------------------------

/// The running remote-exec services (built by [`RemoteExecPlugin`]):
/// one connection manager and one `RemoteExecTool` per configured
/// endpoint, all over one durable queue.
pub struct RemoteExecServices {
    /// The shared backend (enqueue-first durable queue).
    pub backend: Arc<RemoteExecBackend>,
    /// One manager per endpoint (dial + reconnect; started at build).
    pub managers: Vec<Arc<ConnectionManager>>,
    /// The endpoint configs (registry seeding, watchdog bounds).
    pub endpoints: Vec<crate::config::EndpointConfig>,
}

impl RemoteExecServices {
    /// One [`RemoteExecTool`] per configured endpoint — the worker
    /// agents' exec surface (same schema as the local built-in).
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
/// `apply` is sync). Presence-locked to a non-empty `endpoints:` list.
pub struct RemoteExecPlugin {
    /// The pre-opened, migrated exec store (same `kian.sqlite`).
    pub store: ExecStore,
    /// The configured endpoints.
    pub endpoints: Vec<crate::config::EndpointConfig>,
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
        let default_endpoint = self
            .endpoints
            .first()
            .map(|ep| ep.id.clone())
            .ok_or_else(|| apply_error("remote-exec: no endpoints configured"))?;
        let backend = RemoteExecBackend::new(self.store, routes, default_endpoint, config)
            .map_err(apply_error)?;
        let services = Arc::new(RemoteExecServices {
            backend: Arc::new(backend),
            managers,
            endpoints: self.endpoints,
        });
        ctx.provide::<RemoteExecServices>(services)
    }
}

// ---------------------------------------------------------------------------
// superagent (C27): the bundle mounted on the three-way driver
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
    /// The configured resident Router session id.
    pub router_session: SessionId,
    /// The Router's slot name.
    pub router_slot: String,
    /// Drain tuning shared by all three loops.
    pub max_iterations: usize,
    /// The declarative recovery-policy table.
    pub policy: Arc<PolicyTable>,
    /// The converted subscription rule set (config, fail-closed).
    pub rules: kian_superagent::SubscriptionRuleSet,
    /// Mock channels by configured name (v1's only adapter type).
    pub mock_channels: BTreeMap<String, Arc<MockChannel>>,
    /// The worker's evidence gate (mounts on the worker wrapper).
    pub worker_gate: EvidenceGateConfig,
    /// The default agent's evidence gate (mounts on the plain loop).
    pub default_gate: EvidenceGateConfig,
    /// The late-bound engine inbox (shared with the server assembly).
    pub inbox: Arc<crate::superagent::EngineInbox>,
    /// Whether endpoints are configured (dependency on `remote-exec`).
    pub has_remote: bool,
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

        // The structural profile rules are assembly-time assertions:
        // a worker smuggling a channel tool or a router without the
        // dispatch tool refuses to start.
        let profile = kian_superagent::SuperAgentProfile::new(
            self.router_agent.clone(),
            self.worker_agent.clone(),
        )
        .map_err(|e| apply_error(format!("superagent profile: {e}")))?;
        let profile = RouterProfile::new(self.router_session, profile.router);

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
        let mut adapters = Vec::with_capacity(self.mock_channels.len());
        for (name, mock) in &self.mock_channels {
            channels.register(name.clone(), Arc::clone(mock) as Arc<dyn ChannelAdapter>);
            adapters.push(Arc::clone(mock) as Arc<dyn ChannelAdapter>);
        }
        let hold = kian_superagent::RouterHold::shared();
        let router_tools = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));

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
            },
        ));

        // Worker loop: strong main slot, local tools shadowed by the
        // per-endpoint remote exec tools when configured, ungated here
        // (the worker wrapper owns the gate).
        let mut worker_tools: Vec<Arc<dyn ToolProvider>> = Vec::new();
        if let Some(remote) = &remote {
            for tool in remote.tools() {
                let provider: Arc<dyn ToolProvider> = tool;
                worker_tools.push(provider);
            }
        }
        worker_tools.push(local_tools.clone());
        let worker_loop = Arc::new(ReActLoop::with_config(
            self.worker_agent.clone(),
            registry.clone(),
            Arc::new(crate::superagent::CombinedTools::new(worker_tools)),
            Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
            ReActConfig {
                slot: kian_core::model::SlotName("main".into()),
                max_iterations: self.max_iterations,
            },
        ));

        // Plain-session loop: the default agent, with its own gate.
        let default_loop = Arc::new(ReActLoop::with_evidence_gate(
            self.default_agent,
            registry,
            local_tools,
            Arc::clone(&self.policy) as Arc<dyn RecoveryPolicySource>,
            ReActConfig {
                slot: kian_core::model::SlotName("main".into()),
                max_iterations: self.max_iterations,
            },
            self.default_gate,
        ));

        // The three-way driver.
        let router_side = kian_superagent::RouterDriver::new(
            router_loop,
            hold,
            crate::superagent::port_for_ctx(),
            router_inbox.clone(),
        );
        let worker_side = kian_superagent::WorkerLoopDriver::new(
            worker_loop,
            Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>,
            kian_superagent::WorkerLoopConfig {
                router: self.router_session,
                worker_version: self.worker_agent.version,
                gate: crate::superagent::clone_gate(&self.worker_gate),
            },
            crate::superagent::port_for_ctx(),
        );
        let driver = Arc::new(crate::superagent::SuperAgentDriver::new(
            self.router_session,
            self.worker_agent.version,
            router_side,
            worker_side,
            default_loop,
        ));

        let subscriptions = Arc::new(SubscriptionService::new(
            self.router_session,
            self.rules,
            Arc::clone(&router_inbox) as Arc<dyn kian_superagent::InboxWriter>,
        ));
        let subscription_metrics = Arc::new(crate::superagent::SubscriptionMetrics::default());

        let services = Arc::new(crate::superagent::SuperAgentServices {
            driver,
            inbox: self.inbox,
            router_inbox,
            profile,
            subscriptions,
            mock_channels: self.mock_channels,
            adapters,
            worker_gate: self.worker_gate,
            subscription_metrics,
            remote,
        });
        ctx.provide::<crate::superagent::SuperAgentServices>(services)
    }
}
