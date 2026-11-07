//! Server assembly and the iron-rule boundary.
//!
//! [`Server`] owns everything the HTTP layer must NOT touch directly:
//! the plugin `Ctx` activations (via their [`Disposer`]), the sqlite
//! [`EventStore`], and the execution [`Engine`]. It deliberately sits
//! *behind* the [`ControlPlane`] trait implemented here: `api` handlers
//! can only write the inbox and read projections/events through it
//! (AGENTS.md constraint 9 — the layered iron rule).
//!
//! Startup order is load-bearing:
//! 1. load + validate `kian.yaml` (fail-closed),
//! 2. activate plugins (`kian_plugins![...]`) and resolve the loop,
//! 3. open the event store,
//! 4. build the engine,
//! 5. **run the inbox rebuild** so NoLostWork holds across restarts
//!    (constraint 6) before the listener accepts traffic.

use std::sync::Arc;

use futures::future::BoxFuture;
use kian_core::events::Event;
use kian_core::model::SessionId;
use kian_core::state::SessionState;
use kian_exec::ExecStore;
use kian_llm::provider::OpenAiCompatConfig;
use kian_loop::react::{ReActConfig, ReActLoop};
use kian_orchestrator::recovery::PolicyTable;
use kian_orchestrator::{Engine, EngineConfig, EngineError, InterruptOutcome};
use kian_plugin::{Ctx as PluginCtx, Disposer, PluginError};
use kian_store::EventStore;

use crate::api::{ControlError, ControlPlane, EventStreamMetrics};
use crate::config::{DshHarnessConfig, ServeConfig};
use crate::memory::{MemoryAssembly, MemoryLoopDriver, render_transcript};
use crate::ops::{
    DreamAdmitReceipt, MockSendReceipt, OpsError, OpsPlane, ResetReceipt, WakeReceipt,
};
use crate::plugins::{
    LocalToolsPlugin, OpenAiCompatPlugin, ReActPlugin, RemoteExecPlugin, SuperAgentPlugin,
};
use crate::superagent::{ServeLoopDriver, SuperAgentRuntime, SuperAgentServices};
use crate::webhook::{WebhookError, WebhookLimits, WebhookPlane};

/// Everything assembling `kian serve` can fail with. All of them refuse
/// to start — none degrade to defaults.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("config: {0}")]
    Config(#[from] crate::config::ConfigError),
    #[error("plugins: {0}")]
    Plugin(#[from] PluginError),
    #[error("assembly: {0}")]
    Assembly(String),
    #[error("store: {0}")]
    Store(#[from] kian_store::StoreError),
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
}

/// The assembled single-machine server.
pub struct Server {
    engine: Arc<Engine<ServeLoopDriver>>,
    store: EventStore,
    sse_poll_ms: u64,
    /// Recovery-sweep period. Equals `drain.heartbeat_interval_ms`;
    /// see [`Server::serve`] for the documented bound.
    recovery_tick_ms: u64,
    /// The shared pg pool under postgres mode: the wake-transport
    /// listener's dedicated connection is checked out of this pool in
    /// [`Server::serve`]. `None` under sqlite (no transport — the
    /// in-process wake registry is the whole story).
    pg_pool: Option<sqlx::postgres::PgPool>,
    /// The super-agent runtime (present iff the `superagent:` section
    /// is configured): mock channels, metrics surfaces, watchdog.
    superagent: Option<Arc<SuperAgentRuntime>>,
    /// The assembled agent-memory plan (present iff a `memory:`
    /// section is configured): the scope, the caps and the shared
    /// dream driver the `memory_dream` ops method and the `memory`
    /// metrics fragment read.
    memory: Option<Arc<MemoryAssembly>>,
    /// The webhook receiver secret (present iff a `type: github`
    /// channel is configured — the route is configured). Env-only:
    /// `KIAN_GITHUB_WEBHOOK_SECRET`, fail-closed at assembly.
    webhook_secret: Option<String>,
    /// The receiver limits from the (single) github section.
    webhook_limits: WebhookLimits,
    /// This assembly's recorded startup sequence (the test hook).
    startup_order: crate::superagent::StartupOrder,
    /// The snapshot maintenance plan (READ-MODEL §9, P1): present iff
    /// `read_model.snapshots.enabled`. [`Server::serve`] spawns its
    /// ticker — the builder runs outside every drain, under a
    /// per-session maintenance lease.
    snapshot_maintenance: Option<SnapshotMaintenance>,
    /// The view maintenance plan (READ-MODEL §6/§9, P2a): present iff
    /// `read_model.views.enabled`. [`Server::serve`] spawns its ticker
    /// — the rebuilder runs outside every drain under the `views:all`
    /// maintenance lease.
    views_maintenance: Option<ViewsMaintenance>,
    /// Whether observability reads on this server (/metrics aggregates)
    /// may consult fresh materialized views (READ-MODEL §6; a disabled
    /// gate hides every view).
    views_enabled: bool,
    /// The fan-in event-stream metrics (READ-MODEL §4.2, P3): the
    /// shared counters the `/metrics` `event_stream` fragment reports
    /// and every open `GET /events` connection bumps.
    event_stream: Arc<EventStreamMetrics>,
    /// Whether `GET /events` (the fan-in stream) is mounted —
    /// `api.event_stream.enabled`, default true. Disabled mounts no
    /// route: the path 404s.
    event_stream_enabled: bool,
    /// Keeps the plugin-registered services alive for the server's
    /// lifetime; dropping it would unregister them (disposer).
    _plugin_disposer: Disposer,
}

/// The serve-side snapshot builder plan (READ-MODEL §5/§9).
#[derive(Clone)]
struct SnapshotMaintenance {
    build: kian_orchestrator::SnapshotBuildConfig,
    interval_ms: u64,
    holder: String,
}

/// The serve-side view rebuilder plan (READ-MODEL §6/§9, P2a).
#[derive(Clone)]
struct ViewsMaintenance {
    interval_ms: u64,
    holder: String,
}

/// Reads one lark credential from the environment, treating blank as
/// absent (a blank KIAN_LARK_APP_ID is a missing credential, not an
/// empty-string app id).
fn lark_env(key: &str) -> Option<String> {
    env_credential(key)
}

/// Reads one credential from the environment, treating blank as
/// absent (a blank value is a missing credential, never an
/// empty-string secret).
fn env_credential(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

impl Server {
    /// Full assembly from a config file (fail-closed on every step).
    pub async fn from_config_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<(Self, ServeConfig), ServeError> {
        let config = ServeConfig::load(path)?;
        let server = Self::from_config(config.clone()).await?;
        Ok((server, config))
    }

    /// Full assembly from an already-loaded config.
    ///
    /// Startup order is load-bearing: config → providers →
    /// plugins → store → engine → **inbox rebuild** → superagent
    /// services (registry rebuild, subscription consumers, watchdog) →
    /// the caller binds the listener. The subscription engine is never
    /// started before `startup_rebuild` returned (asserted by
    /// `tests/superagent_serve.rs` via [`Server::startup_order`]).
    pub async fn from_config(config: ServeConfig) -> Result<Self, ServeError> {
        // The startup-order log for THIS assembly (test hook).
        let startup_order = crate::superagent::new_startup_order();

        // -- 1. providers: resolve secrets from the environment ----------
        // (fail-closed: a configured provider without its key refuses to
        // start; secrets never live in kian.yaml)
        let mut providers = Vec::new();
        for (name, pcfg) in &config.providers {
            let api_key = std::env::var(&pcfg.api_key_env).map_err(|_| {
                ServeError::Assembly(format!(
                    "provider `{name}`: environment variable `{}` (api key) is not set",
                    pcfg.api_key_env
                ))
            })?;
            providers.push(OpenAiCompatConfig {
                name: name.clone(),
                base_url: pcfg.base_url.clone(),
                api_key,
                chat_completions_path: pcfg.chat_completions_path.clone(),
            });
        }

        // -- 2. plugin activation (ADR-004: static, compile-time) --------
        let policy = Arc::new(config.policy()?.table());
        let superagent_cfg = config.superagent.as_ref();
        let has_remote = !config.endpoints.is_empty();

        // the webhook receiver's env-only secret + limits (filled
        // by the github arm below; present iff the route is
        // configured, consumed by the Server construction below).
        let mut webhook_secret: Option<String> = None;
        let mut webhook_limits = WebhookLimits::default();

        // The exec store opens before activation (plugin apply is sync).
        // one server, one database — under `event_store: postgres`
        // the event store and the exec store share ONE pg pool, opened
        // here before either adopts it. The URL is env-only (the
        // secrets rule); config validation already refused the default
        // drain.holder for postgres (condition (c)).
        let pg_pool = if config.storage.is_postgres() {
            let url_env = &config
                .storage
                .postgres
                .as_ref()
                .expect("validated: postgres mode requires the section")
                .url_env;
            let url = std::env::var(url_env)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| {
                    ServeError::Assembly(format!(
                        "storage.event_store: postgres but environment variable `{url_env}` \
                         (the database URL) is not set — URLs are env-only, never a config value"
                    ))
                })?;
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(16)
                .connect(&url)
                .await
                .map_err(|e| ServeError::Assembly(format!("postgres pool ({url_env}): {e}")))?;
            Some(pool)
        } else {
            None
        };
        let exec_store = if has_remote {
            let store = match pg_pool.as_ref() {
                Some(pool) => ExecStore::from_pg_pool(pool.clone())
                    .await
                    .map_err(|e| ServeError::Assembly(format!("exec store: {e}")))?,
                None => ExecStore::open(&config.storage.sqlite)
                    .await
                    .map_err(|e| ServeError::Assembly(format!("exec store: {e}")))?,
            };
            Some(store)
        } else {
            None
        };

        let mut ctx = PluginCtx::new();
        // the exec secret hygiene pair, built once and shared
        // by the local tool surface and the remote-exec queue:
        //
        // - the redactor's known values are the *configured* secrets —
        //   every env name kian.yaml itself names (provider keys, the
        //   github send token + webhook secret, lark credentials, the
        //   postgres URL) plus operator-listed extras. Unset names are
        //   skipped by the redactor itself.
        // - the env policy is fail-closed on an unknown mode; the
        //   default (`inherit`) keeps today's behavior.
        let mut secret_env_names: Vec<String> = config
            .providers
            .values()
            .map(|p| p.api_key_env.clone())
            .collect();
        secret_env_names.push("KIAN_GITHUB_TOKEN".into());
        secret_env_names.push("KIAN_GITHUB_WEBHOOK_SECRET".into());
        secret_env_names.push("KIAN_LARK_APP_SECRET".into());
        if let Some(pg) = config.storage.postgres.as_ref() {
            secret_env_names.push(pg.url_env.clone());
        }
        secret_env_names.extend(config.tools.exec_redact.env_names.clone());
        let exec_redactor = if config.tools.exec_redact.enabled {
            kian_core::secrets::SecretRedactor::from_env_names(&secret_env_names)
        } else {
            // Explicit config opt-out: fully disabled (no patterns).
            kian_core::secrets::SecretRedactor::default()
        };
        // The memory redactor (kian-91, review M-1): note text passes
        // the secret redactor before append. It shares the exec
        // redactor's configured secret names but is NOT bound to the
        // exec opt-out — that config is about byte-exact exec OUTPUT;
        // memory notes are durable claims, so pattern scanning stays on
        // (the safe default for a surface with no configured names).
        let memory_redactor = kian_core::secrets::SecretRedactor::from_env_names(&secret_env_names);
        let exec_env_policy = config
            .tools
            .exec_env
            .to_policy()
            .map_err(ServeError::Config)?;
        let mut plugins = kian_plugin::kian_plugins![
            OpenAiCompatPlugin { providers },
            LocalToolsPlugin {
                root: config.tools.root.clone().into(),
                exec_timeout_ms: config.tools.exec_timeout_ms,
                env_policy: exec_env_policy,
                redactor: exec_redactor.clone(),
            },
        ];
        if let Some(store) = exec_store {
            plugins.register(RemoteExecPlugin {
                store,
                endpoints: config.endpoints.clone(),
                holder: config.drain.holder.clone(),
                redactor: exec_redactor,
            });
        }
        // The event store opens before activation too: the
        // completion bridge (inside the superagent plugin) reads
        // session projections and committed `tool/call` events.
        let store = match pg_pool.as_ref() {
            Some(pool) => EventStore::from_pg_pool(pool.clone()).await?,
            None => EventStore::open(&config.storage.sqlite).await?,
        };
        let superagent_services: Option<()> = if let Some(sa) = superagent_cfg {
            // Super-agent mode: the bundle plugin builds all three
            // loops (router/worker/plain); `react-loop` is absent.
            let router_agent = config
                .named_agent_def(&sa.router.agent)
                .ok_or_else(|| ServeError::Assembly("router agent missing".into()))?;
            let worker_agent = config
                .named_agent_def(&sa.worker.agent)
                .ok_or_else(|| ServeError::Assembly("worker agent missing".into()))?;
            // the async pair mounts on BOTH profiles, config-gated
            // (default off — the AgentDefs are untouched when off).
            let mut router_agent = router_agent;
            let mut worker_agent = worker_agent;
            kian_superagent::mount_async_tools(&mut router_agent, sa.async_tools);
            kian_superagent::mount_async_tools(&mut worker_agent, sa.async_tools);
            // the recall toolpack — default on for the Router
            // profile, opt-in per worker template (config-gated).
            kian_superagent::mount_recall_tools(&mut router_agent, sa.recall.router);
            kian_superagent::mount_recall_tools(&mut worker_agent, sa.recall.worker);
            let worker_gate = config
                .named_agent(&sa.worker.agent)
                .map(|agent| agent.evidence_gate.clone())
                .ok_or_else(|| ServeError::Assembly("worker agent missing".into()))?;
            let router_session = kian_core::model::SessionId(
                uuid::Uuid::parse_str(&sa.router.session_id)
                    .map_err(|_| ServeError::Assembly("router session id is not a uuid".into()))?,
            );
            // the boot-time re-arm walk. A resident Router that
            // ended terminally (session/ended{completed|cancelled} —
            // e.g. The loop's lifetime iteration budget exhausting) is
            // frozen; the consumed bootstrap row can never re-wake it.
            // The walk resolves the live session down the deterministic
            // successor chain (uuid5) and assembly binds THAT session
            // everywhere the pinned id used to go. Soft-failed sessions
            // project idle and stay generation 0 — the next admitted
            // input re-opens them, no successor needed.
            let router_re_arm = kian_superagent::rearm::resolve_re_arm(router_session, {
                let store = store.clone();
                move |session| {
                    let store = store.clone();
                    Box::pin(async move { store.rebuild(session).await })
                }
            })
            .await
            .map_err(|e| {
                ServeError::Assembly(format!(
                    "router re-arm resolution (pinned {}): {e}",
                    router_session.0
                ))
            })?;
            let router_session = router_re_arm.live;
            if router_re_arm.is_succession() {
                tracing::info!(
                    pinned = %sa.router.session_id,
                    live = %router_session.0,
                    ended = router_re_arm.ended.len(),
                    "superagent: resident Router re-armed onto a successor session"
                );
            }
            // the resident watchdog binding (optional). The agent
            // is worker-template shaped; the read/intervene surface
            // mounts on it the same way the async/recall pairs mount.
            let mut watchdog = match &sa.watchdog {
                Some(watchdog) => {
                    let pinned = kian_core::model::SessionId(
                        uuid::Uuid::parse_str(&watchdog.session_id).map_err(|_| {
                            ServeError::Assembly("watchdog session id is not a uuid".into())
                        })?,
                    );
                    // the same re-arm walk for the resident
                    // watchdog (it can terminally end exactly like the
                    // Router; the timer rule's Route::Session target
                    // becomes the resolved live session).
                    let re_arm = kian_superagent::rearm::resolve_re_arm(pinned, {
                        let store = store.clone();
                        move |session| {
                            let store = store.clone();
                            Box::pin(async move { store.rebuild(session).await })
                        }
                    })
                    .await
                    .map_err(|e| {
                        ServeError::Assembly(format!(
                            "watchdog re-arm resolution (pinned {}): {e}",
                            pinned.0
                        ))
                    })?;
                    if re_arm.is_succession() {
                        tracing::info!(
                            pinned = %watchdog.session_id,
                            live = %re_arm.live.0,
                            ended = re_arm.ended.len(),
                            "superagent: resident watchdog re-armed onto a successor session"
                        );
                    }
                    let mut agent = config
                        .named_agent_def(&watchdog.agent)
                        .ok_or_else(|| ServeError::Assembly("watchdog agent missing".into()))?;
                    kian_superagent::mount_watchdog_tools(&mut agent, true);
                    let mut profile = kian_superagent::WatchdogProfile::new(re_arm.live, agent);
                    profile.re_arm = re_arm.ended;
                    Some(profile)
                }
                None => None,
            };
            let rules = sa.subscription_rules().map_err(ServeError::Config)?;
            let classes = sa.admission_classes().map_err(ServeError::Config)?;
            // the configured policy version (`superagent.policy
            // .version`), fingerprinted over the resident prompt
            // templates; `None` = today's bytes, byte-identical.
            let policy_version = sa.policy_version();
            if let Some(watchdog_profile) = watchdog.as_mut() {
                watchdog_profile.policy_version = policy_version.clone();
            }
            // +: the configured list plus every lark-typed
            // channel (real channels are untrusted by default).
            let untrusted_channels = sa.runtime_untrusted_set().map_err(ServeError::Config)?;
            let inbox = crate::superagent::EngineInbox::new();

            // Mock channels by configured name; lark channels read
            // their credentials from the environment here (fail-closed:
            // a lark channel without KIAN_LARK_APP_ID/KIAN_LARK_APP_SECRET
            // refuses to start — tokens never live in kian.yaml).
            let mut mock_channels = std::collections::BTreeMap::new();
            let mut lark_channels = std::collections::BTreeMap::new();
            // the webhook-fed github channels (config
            // validation caps them at one — the single receiver route
            // feeds exactly one).
            let mut github_channels = std::collections::BTreeMap::new();
            // the timer input sources (a channel like any other —
            // the system clock times the cadence).
            let mut timer_channels: Vec<Arc<kian_superagent::TimerChannel>> = Vec::new();
            for (name, channel) in &sa.channels {
                match channel.channel_type.as_str() {
                    "mock-im" => {
                        mock_channels.insert(
                            name.clone(),
                            Arc::new(kian_superagent::MockChannel::new(name.clone())),
                        );
                    }
                    "github" => {
                        let section = channel.github.as_ref().ok_or_else(|| {
                            ServeError::Config(crate::config::ConfigError::Invalid(format!(
                                "superagent.channels `{name}` (type github) requires a `github:` section"
                            )))
                        })?;
                        // Send token env-only: `send: true` without
                        // KIAN_GITHUB_TOKEN refuses to start
                        // (fail-closed, never a degraded send);
                        // receive-only needs none.
                        let runtime = section
                            .to_runtime(name, env_credential("KIAN_GITHUB_TOKEN"))
                            .map_err(ServeError::Config)?;
                        let adapter = kian_superagent::GitHubChannel::new(name.clone(), runtime)
                            .map_err(|e| {
                                ServeError::Assembly(format!("github channel `{name}`: {e}"))
                            })?;
                        webhook_limits = section.webhook_limits();
                        github_channels.insert(name.clone(), Arc::new(adapter));
                    }
                    "timer" => {
                        let section = channel.timer.as_ref().ok_or_else(|| {
                            ServeError::Config(crate::config::ConfigError::Invalid(format!(
                                "superagent.channels `{name}` (type timer) requires a `timer:` section"
                            )))
                        })?;
                        let runtime = section.to_runtime(name).map_err(ServeError::Config)?;
                        timer_channels.push(Arc::new(kian_superagent::TimerChannel::new(
                            name.clone(),
                            runtime,
                            Arc::new(kian_superagent::SystemTurnClock),
                        )));
                    }
                    "lark" => {
                        let section = channel.lark.as_ref().ok_or_else(|| {
                            ServeError::Config(crate::config::ConfigError::Invalid(format!(
                                "superagent.channels `{name}` (type lark) requires a `lark:` section"
                            )))
                        })?;
                        let runtime = section
                            .to_runtime(
                                name,
                                lark_env("KIAN_LARK_APP_ID"),
                                lark_env("KIAN_LARK_APP_SECRET"),
                            )
                            .map_err(ServeError::Config)?;
                        let adapter = kian_superagent::LarkChannel::new(name.clone(), runtime)
                            .map_err(|e| {
                                ServeError::Assembly(format!("lark channel `{name}`: {e}"))
                            })?;
                        lark_channels.insert(name.clone(), Arc::new(adapter));
                    }
                    // Unknown types were rejected at parse time.
                    _ => {}
                }
            }

            // a webhook-**receiving** github channel configures the
            // webhook receiver route — and a configured route without
            // its env-only secret refuses to start (fail-closed, the
            // secrets discipline: never a signature-less receiver,
            // never a secret in kian.yaml). a poll-only channel
            // (`webhook_receive: false`) mounts no receiver route and
            // needs no secret — poll mode is beside the webhook
            // surface, and a deployment that receives no webhooks
            // mounts no signature-verifying route.
            let webhook_fed = sa.channels.values().any(|channel| {
                channel.channel_type == kian_superagent::GITHUB_CHANNEL_TYPE
                    && channel
                        .github
                        .as_ref()
                        .is_some_and(|section| section.webhook_receive)
            });
            if webhook_fed {
                webhook_secret = Some(
                    env_credential(crate::webhook::GITHUB_WEBHOOK_SECRET_ENV).ok_or_else(|| {
                        ServeError::Assembly(format!(
                            "superagent.channels declares a `type: github` channel (the \
                                 POST /webhooks/github route is configured) but `{}` is \
                                 absent/blank in the environment — webhook secrets never live \
                                 in kian.yaml",
                            crate::webhook::GITHUB_WEBHOOK_SECRET_ENV
                        ))
                    })?,
                );
            }

            plugins.register(SuperAgentPlugin {
                router_agent,
                worker_agent,
                default_agent: config.agent_def(),
                // (ADR-015): the worker template's and the default
                // agent's permission tables ride their loops; the judge
                // loops (router/watchdog) refuse a table at validation.
                worker_permissions: config
                    .named_agent(&sa.worker.agent)
                    .map(|a| a.permissions.clone())
                    .unwrap_or_default(),
                default_permissions: config.agent.permissions.clone(),
                router_session,
                router_rearm: router_re_arm.ended,
                router_slot: sa.router.slot.clone(),
                speculation: sa
                    .router
                    .speculation
                    .as_ref()
                    .map(|s| s.to_policy())
                    .transpose()
                    .map_err(ServeError::Config)?,
                speak: sa.speak_policy().map_err(ServeError::Config)?,
                followup_ownership: sa.followup_ownership,
                policy_version,
                policy_notify_live: sa.policy.notify_live,
                mandate: sa.mandate_policy(),
                outbox: sa.outbox_config().map_err(ServeError::Config)?,
                // /ADR-013: the thread-key derivations (validated at
                // parse; converted again here — one shape, both gates).
                thread_keys: sa
                    .intake
                    .to_runtime(&sa.channels.keys().cloned().collect())
                    .map_err(ServeError::Config)?,
                max_iterations: config.drain.max_iterations,
                policy: Arc::clone(&policy),
                rules,
                classes,
                shaping: sa
                    .shaping
                    .as_ref()
                    .map(|shaping| shaping.to_runtime())
                    .unwrap_or_default(),
                untrusted_channels,
                mock_channels,
                lark_channels,
                github_channels,
                timer_channels,
                watchdog,
                worker_gate,
                default_gate: config.agent.evidence_gate.clone(),
                inbox: Arc::clone(&inbox),
                has_remote,
                async_tools: sa.async_tools,
                recall: sa.recall.clone(),
                dsh_harness: sa
                    .harness
                    .dsh
                    .as_ref()
                    .map(DshHarnessConfig::to_runtime)
                    .transpose()?,
                event_store: store.clone(),
            });
            None // services resolve from the Ctx after activation
        } else {
            // built-in mode: exactly the loop plugin.
            plugins.register(ReActPlugin {
                agent: config.agent_def(),
                policy: Arc::clone(&policy) as Arc<PolicyTable>,
                config: ReActConfig {
                    slot: kian_core::model::SlotName(config.drain.slot.clone()),
                    max_iterations: config.drain.max_iterations,
                    // the default agent's per-call permission table
                    // (absent = empty = allow-all, byte-identical).
                    permissions: config.agent.permissions.clone(),
                },
                gate: config.agent.evidence_gate.clone(),
            });
            None
        };
        let plugin_disposer = plugins.activate(&mut ctx)?;

        // -- 3. driver resolution -----------------------------------------
        let (mut driver, services) = if superagent_cfg.is_some() {
            let services = ctx
                .resolve::<SuperAgentServices>()
                .ok_or_else(|| ServeError::Assembly("superagent plugin did not register".into()))?;
            (
                Arc::new(ServeLoopDriver::SuperAgent(Arc::clone(&services.driver))),
                Some(services),
            )
        } else {
            let loop_ = ctx
                .resolve::<ReActLoop>()
                .ok_or_else(|| ServeError::Assembly("react-loop plugin did not register".into()))?;
            (Arc::new(ServeLoopDriver::Plain(loop_)), None)
        };
        // The memory assembly (a `memory:` section): ordinary sessions
        // keep the exact plain-loop handle above — the router only
        // adds the dream side for streams carrying a dream request
        // (config-gated; with no section the driver is untouched).
        let memory = match (&config.memory, driver.as_ref()) {
            (Some(section), ServeLoopDriver::Plain(plain)) => {
                let dream = build_dream_driver(
                    &config,
                    section,
                    &ctx,
                    Arc::clone(&policy),
                    memory_redactor.clone(),
                )?;
                let caps = section.dreamer.caps();
                let scope = section.scope.clone();
                let assembly = Arc::new(MemoryAssembly::new(scope, caps, Arc::clone(&dream)));
                let router = Arc::new(MemoryLoopDriver::new(Arc::clone(plain), dream));
                driver = Arc::new(ServeLoopDriver::Memory(router));
                Some(assembly)
            }
            (Some(_), _) => {
                // Unreachable: validation refuses memory + superagent.
                return Err(ServeError::Assembly(
                    "memory: the section is only supported on the built-in assembly".into(),
                ));
            }
            (None, _) => None,
        };
        let _ = superagent_services;

        // -- 4. engine ------------------------------------------------------
        let engine_config = EngineConfig {
            holder: config.drain.holder.clone(),
            lease_ttl_ms: config.drain.lease_ttl_ms,
            heartbeat_interval_ms: config.drain.heartbeat_interval_ms,
        };
        // READ-MODEL §5 (P1): the read-model config routes
        // projection/recovery state reads through the verified-snapshot
        // loader when enabled (the honest full-derivation fallback is
        // built into the loader — the wire never changes).
        let read_model = kian_orchestrator::ReadModelConfig {
            snapshots_enabled: config.read_model.snapshots.enabled,
            tail_cap: config.read_model.snapshots.tail_cap,
        };
        let snapshot_maintenance =
            config
                .read_model
                .snapshots
                .enabled
                .then(|| SnapshotMaintenance {
                    build: kian_orchestrator::SnapshotBuildConfig {
                        min_events: config.read_model.snapshots.min_events,
                        keep_n: config.read_model.snapshots.keep_n,
                        lease_ttl_ms: kian_orchestrator::DEFAULT_LEASE_TTL_MS,
                    },
                    interval_ms: config.read_model.snapshots.interval_ms,
                    holder: config.drain.holder.clone(),
                });
        // READ-MODEL §6 (P2a): the view maintenance plan — present iff
        // `read_model.views.enabled`. [`Server::serve`] spawns its
        // ticker; the rebuilder runs outside every drain under the
        // `views:all` maintenance lease. The gate also decides whether
        // the /metrics aggregates may consult views (a disabled gate
        // hides every view — honest full derivation).
        let views_maintenance = config.read_model.views.enabled.then(|| ViewsMaintenance {
            interval_ms: config.read_model.views.interval_ms,
            holder: config.drain.holder.clone(),
        });
        let views_enabled = config.read_model.views.enabled;
        let event_stream_enabled = config.api.event_stream.enabled;
        let engine = Engine::new_with_read_model(store.clone(), driver, engine_config, read_model);

        // The bundle's inbox port learns the engine (the cycle
        // breaker closes here, before any wake can fire).
        let superagent = match services {
            Some(services) => {
                if services.inbox.install(Arc::clone(&engine)).is_err() {
                    return Err(ServeError::Assembly(
                        "engine inbox already installed".into(),
                    ));
                }
                Some(Arc::clone(&services))
            }
            None => None,
        };

        let server = Self {
            engine,
            store,
            sse_poll_ms: config.sse.poll_interval_ms,
            recovery_tick_ms: config.drain.heartbeat_interval_ms,
            pg_pool: pg_pool.clone(),
            superagent: None,
            memory,
            webhook_secret,
            webhook_limits,
            startup_order: Arc::clone(&startup_order),
            snapshot_maintenance,
            views_maintenance,
            views_enabled,
            event_stream: Arc::new(EventStreamMetrics::new()),
            event_stream_enabled,
            _plugin_disposer: plugin_disposer,
        };

        // -- 5. NoLostWork: inbox rebuild BEFORE anything else runs ------
        let sessions = server.engine.startup_rebuild().await?;
        tracing::info!(sessions, "startup inbox rebuild complete");
        crate::superagent::record_rebuild_done(&startup_order);

        // -- 6. superagent services start (registry → subscriptions →
        //       watchdog → router bootstrap), strictly AFTER the rebuild.
        let superagent = match superagent {
            Some(services) => Some(
                crate::superagent::start_services(
                    services,
                    Arc::clone(&server.engine),
                    server.store.clone(),
                    Arc::clone(&startup_order),
                )
                .await
                .map_err(ServeError::Assembly)?,
            ),
            None => None,
        };
        let mut server = server;
        server.superagent = superagent.map(Arc::new);

        Ok(server)
    }

    /// This assembly's recorded startup sequence (the acceptance
    /// hook: subscription/watchdog starts must follow
    /// `startup_rebuild_done`).
    pub fn startup_order(&self) -> Vec<&'static str> {
        self.startup_order
            .lock()
            .expect("startup order lock")
            .clone()
    }

    /// The super-agent runtime handle (present iff a `superagent:`
    /// section assembled this server). Embedders and the e2e tests
    /// reach the mock channels (delivery capture) through this — the
    /// same handle the ops plane's `mock_channel_send` uses.
    pub fn superagent_runtime(&self) -> Option<&Arc<SuperAgentRuntime>> {
        self.superagent.as_ref()
    }

    /// The HTTP router for this server. The control-plane handlers see
    /// only the [`ControlPlane`] trait object — never the engine or
    /// store. The merged `/metrics` route (observability) lives in
    /// this module, not `api`: it reads counters and derives the cache
    /// report from the store, which the iron-rule handler module must
    /// not even name. The `/ops/rpc` plane (operator actions:
    /// `wake_now`, `recovery_budget_reset`) is merged here too, behind
    /// its own [`OpsPlane`] trait — the control plane's built-in wire
    /// surface stays untouched.
    pub fn router(self: &Arc<Self>) -> axum::Router {
        let plane: Arc<dyn ControlPlane> = self.clone();
        let ops: Arc<dyn OpsPlane> = self.clone();
        let router = crate::api::router_with_event_stream(
            plane,
            self.sse_poll_ms,
            Arc::clone(&self.event_stream),
            self.event_stream_enabled,
        )
        .merge(metrics_router(self.clone()))
        .merge(crate::ops::router(ops));
        // the fourth surface — the inbound webhook receiver,
        // mounted **only** when the route is configured (a `type:
        // github` channel exists; assembly already refused to start
        // without the env-only secret). The wire surface, the ops
        // plane and the metrics route are untouched.
        match self.webhook_secret.clone() {
            Some(secret) => {
                let webhook: Arc<dyn WebhookPlane> = self.clone();
                router.merge(crate::webhook::router(
                    webhook,
                    secret,
                    self.webhook_limits.clone(),
                ))
            }
            None => router,
        }
    }

    /// Serves on `addr` until the process is stopped.
    ///
    /// Also spawns the recovery-detection ticker (DESIGN §13
    /// availability target): every `recovery_tick_ms` the engine sweeps
    /// expired leases and wakes `running` sessions whose drain owner
    /// died — including killed-mid-turn sessions whose inbox is empty
    /// (the startup inbox scan cannot see those). Constant relationship
    /// (documented, not tuned): the tick equals
    /// `drain.heartbeat_interval_ms`, so detection is bounded by
    /// `last_heartbeat + lease_ttl_ms + heartbeat_interval_ms` — the
    /// designed "lease TTL + heartbeat interval" bound plus at most one
    /// heartbeat of kill-to-last-heartbeat skew. Safety does not depend
    /// on the timing: the takeover claim's epoch fences any lingering
    /// old owner's writes (NoSplitBrainWrite, ADR-005).
    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> Result<(), ServeError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| ServeError::Assembly(format!("cannot bind {addr}: {e}")))?;
        tracing::info!(%addr, "kian serve listening");
        {
            let engine = Arc::clone(&self.engine);
            let tick = std::time::Duration::from_millis(self.recovery_tick_ms.max(1));
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    match engine.recovery_sweep().await {
                        Ok(woken) if !woken.is_empty() => {
                            tracing::info!(
                                sessions = ?woken,
                                "recovery sweep: took over expired drains"
                            );
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "recovery sweep failed");
                        }
                    }
                }
            });
        }
        // Snapshot maintenance (READ-MODEL §9, P1): a ticker whose each
        // beat builds verified snapshots for candidate sessions under a
        // per-session maintenance lease — strictly outside every drain
        // (never on a drain's critical path). Publication is
        // verify-then-insert (R6): a crash between beats can orphan a
        // no-op, never a partial snapshot. Idempotent under retry (R9),
        // so a missed or doubled beat is harmless.
        if let Some(plan) = self.snapshot_maintenance.clone() {
            let store = self.store.clone();
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_millis(plan.interval_ms));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    match kian_orchestrator::maintenance_tick(
                        &store,
                        &plan.build,
                        &plan.holder,
                        now_ms,
                    )
                    .await
                    {
                        Ok(n) if n > 0 => {
                            tracing::info!(published = n, "snapshot maintenance tick");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "snapshot maintenance tick failed");
                        }
                    }
                }
            });
        }
        // View maintenance (READ-MODEL §6/§9, P2a): a ticker whose each
        // beat rebuilds every materialized view under the `views:all`
        // maintenance lease — strictly outside every drain. Publication
        // is one atomic version flip per view (rows + freshness key +
        // digest in one transaction): a crash between beats leaves the
        // previous complete version, never half a view. Idempotent
        // under retry (R9). Views are observability accelerators only —
        // no control read ever consumes one (R8).
        if let Some(plan) = self.views_maintenance.clone() {
            let store = self.store.clone();
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_millis(plan.interval_ms));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    crate::views::views_maintenance_tick(&store, &plan.holder, now_ms).await;
                }
            });
        }
        // wake transport (postgres mode only): a relay task holds
        // one dedicated pooled connection LISTENing on `kian_wake` and
        // fires the local engine `wake` per notification — the
        // cross-replica accelerator. Advisory by contract (ADR-009
        // decision 4): a listener gap (reconnect, error) logs, backs
        // off and re-listens, and every notification missed in the gap
        // is bounded by the recovery sweep ticker above plus this
        // replica's startup inbox scan — the same rebuild that already
        // makes in-process wakes losable. sqlite runs nothing here.
        if let Some(pool) = self.pg_pool.clone() {
            let engine = Arc::clone(&self.engine);
            tokio::spawn(async move {
                let mut backoff = std::time::Duration::from_millis(100);
                loop {
                    match kian_store::wake::listen_for_wakes(&pool).await {
                        Ok(mut listener) => {
                            backoff = std::time::Duration::from_millis(100);
                            loop {
                                match listener.recv().await {
                                    Ok(notification) => {
                                        let payload = notification.payload();
                                        match uuid::Uuid::parse_str(payload) {
                                            Ok(id) => {
                                                engine.wake(kian_core::model::SessionId(id));
                                            }
                                            Err(e) => {
                                                // Foreign payloads are logged and
                                                // dropped — never a panic, never a
                                                // relay death (wakes are advisory).
                                                tracing::warn!(
                                                    payload,
                                                    error = %e,
                                                    "wake notification payload is not a session uuid; ignored"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "wake listener dropped; re-listening (gap is bounded by the sweep)"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "wake listener connect failed; retrying with backoff"
                            );
                        }
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
                }
            });
        }
        axum::serve(
            listener,
            self.router()
                // peer connect info feeds the webhook receiver's
                // per-source rate-limit buckets (additive — every
                // other surface ignores it).
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .map_err(|e| ServeError::Assembly(format!("server error: {e}")))
    }
}

// ---------------------------------------------------------------------------
// ControlPlane: the ONLY surface the HTTP layer may call (iron rule)
// ---------------------------------------------------------------------------

impl ControlPlane for Server {
    fn create_session(&self) -> BoxFuture<'_, Result<SessionId, ControlError>> {
        Box::pin(async move {
            self.engine
                .create_session()
                .await
                .map_err(internal("create_session"))
        })
    }

    fn submit(
        &self,
        session: SessionId,
        content: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>> {
        Box::pin(async move {
            // Inbox write first (constraint 6); the engine folds the wake.
            self.engine
                .admit(session, content, idempotency_key)
                .await
                .map(|o| o.deduplicated)
                .map_err(internal("submit"))
        })
    }

    fn steer(
        &self,
        session: SessionId,
        content: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>> {
        Box::pin(async move {
            self.engine
                .steer(session, content, idempotency_key)
                .await
                .map(|o| o.deduplicated)
                .map_err(internal("steer"))
        })
    }

    fn queue(
        &self,
        session: SessionId,
        content: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>> {
        // `queue` differs from `submit` only in intent: it lands in the
        // same durable inbox; a drain in flight folds the wake and
        // consumes it in the same sweep (batch semantics).
        Box::pin(async move {
            self.engine
                .admit(session, content, idempotency_key)
                .await
                .map(|o| o.deduplicated)
                .map_err(internal("queue"))
        })
    }

    fn interrupt(&self, session: SessionId) -> BoxFuture<'_, Result<String, ControlError>> {
        Box::pin(async move {
            match self.engine.interrupt(session).await {
                Ok(InterruptOutcome::Requested) => Ok("requested".into()),
                Ok(InterruptOutcome::NotDraining { .. }) => Ok("not_draining".into()),
                Ok(InterruptOutcome::UnknownSession) => {
                    Err(ControlError::UnknownSession { session })
                }
                Err(e) => Err(internal("interrupt")(e)),
            }
        })
    }

    fn projection(&self, session: SessionId) -> BoxFuture<'_, Result<SessionState, ControlError>> {
        Box::pin(async move {
            self.refuse_archived(session).await?;
            self.engine.projection(session).await.map_err(|e| match e {
                EngineError::Projection(kian_core::state::ProjectionError::EmptyStream) => {
                    ControlError::UnknownSession { session }
                }
                other => ControlError::Internal(format!("projection: {other}")),
            })
        })
    }

    fn session_events(
        &self,
        session: SessionId,
    ) -> BoxFuture<'_, Result<Vec<Event>, ControlError>> {
        Box::pin(async move {
            self.refuse_archived(session).await?;
            let events = self
                .store
                .rebuild(session)
                .await
                .map_err(|e| ControlError::Internal(format!("events: {e}")))?;
            if events.is_empty() {
                return Err(ControlError::UnknownSession { session });
            }
            Ok(events)
        })
    }

    fn projection_versioned(
        &self,
        session: SessionId,
        since: Option<u64>,
    ) -> BoxFuture<'_, Result<crate::api::ProjectionRead, ControlError>> {
        Box::pin(async move {
            self.refuse_archived(session).await?;
            let read = self
                .engine
                .projection_versioned(session, since)
                .await
                .map_err(|e| match e {
                    EngineError::SinceAhead {
                        session,
                        since,
                        version,
                    } => ControlError::SinceAhead {
                        session,
                        since,
                        version,
                    },
                    EngineError::Projection(kian_core::state::ProjectionError::EmptyStream) => {
                        ControlError::UnknownSession { session }
                    }
                    other => ControlError::Internal(format!("projection: {other}")),
                })?;
            Ok(crate::api::ProjectionRead {
                version: read.version,
                unchanged: read.unchanged,
                state: read.state,
            })
        })
    }

    fn session_events_after(
        &self,
        session: SessionId,
        after: u64,
    ) -> BoxFuture<'_, Result<crate::api::EventTailRead, ControlError>> {
        Box::pin(async move {
            self.refuse_archived(session).await?;
            let tail = kian_orchestrator::read::events_after(&self.store, session, after)
                .await
                .map_err(|e| match e {
                    EngineError::SinceAhead {
                        session,
                        since,
                        version,
                    } => ControlError::SinceAhead {
                        session,
                        since,
                        version,
                    },
                    EngineError::Projection(kian_core::state::ProjectionError::EmptyStream) => {
                        ControlError::UnknownSession { session }
                    }
                    other => ControlError::Internal(format!("events: {other}")),
                })?;
            Ok(crate::api::EventTailRead {
                version: tail.version,
                events: tail.events,
                terminal: tail.terminal,
            })
        })
    }

    /// READ-MODEL §4.2 (P3): the fan-in's tail-only start point — one
    /// indexed head query (the pre-connect history is never replayed
    /// nor scanned). An event-less session is `unknown_session`; an
    /// archived one answers the typed `-32007` refusal.
    fn session_version(&self, session: SessionId) -> BoxFuture<'_, Result<u64, ControlError>> {
        Box::pin(async move {
            self.refuse_archived(session).await?;
            let version = self
                .store
                .version(session)
                .await
                .map_err(|e| ControlError::Internal(format!("version: {e}")))?;
            if version == 0 {
                return Err(ControlError::UnknownSession { session });
            }
            Ok(version)
        })
    }
}

impl Server {
    /// READ-MODEL §7 (P2b): the archived guard shared by every session
    /// read on the plane — an archived session answers the typed
    /// `archived` error (`-32007`), never an empty-scan
    /// `unknown_session`. One indexed PK lookup on the marker table
    /// (absent table = pre-v5 store = never archived).
    async fn refuse_archived(&self, session: SessionId) -> Result<(), ControlError> {
        match self.store.archived_session(session).await {
            Ok(Some(m)) => Err(ControlError::Archived {
                session,
                archived_at: m.archived_at,
            }),
            Ok(None) => Ok(()),
            Err(e) => Err(ControlError::Internal(format!("archive marker read: {e}"))),
        }
    }
}

/// Maps engine/store failures onto `ControlError::Internal` with the
/// operation name for a clear message.
fn internal(op: &'static str) -> impl Fn(EngineError) -> ControlError + Send + Sync + 'static {
    move |e| {
        // Inbox failures carry their own context; keep the message honest.
        ControlError::Internal(format!("{op}: {e}"))
    }
}

// ---------------------------------------------------------------------------
// OpsPlane: the operator surface (wake-now, recovery budget reset)
// ---------------------------------------------------------------------------

/// Wall-clock milliseconds for operator event timestamps (the engine's
/// clock is private; ops events happen outside a drain, so the system
/// clock is the honest source).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl OpsPlane for Server {
    fn wake_now(&self, session: SessionId) -> BoxFuture<'_, Result<WakeReceipt, OpsError>> {
        Box::pin(async move {
            // Read-before-act: refuse unknown and terminal sessions
            // (a terminal stream is frozen — constraint: terminal states
            // never restart; an unknown id is a typo, not a wake target).
            let events = self
                .store
                .rebuild(session)
                .await
                .map_err(|e| OpsError::Internal(format!("wake_now: {e}")))?;
            if events.is_empty() {
                return Err(OpsError::UnknownSession { session });
            }
            let state = kian_core::state::project(&events)
                .map_err(|e| OpsError::Internal(format!("wake_now: {e}")))?;
            if state.is_terminal() {
                return Err(OpsError::SessionTerminal { session });
            }
            match self.engine.wake(session) {
                kian_orchestrator::WakeOutcome::Started => Ok(WakeReceipt::Started),
                kian_orchestrator::WakeOutcome::Folded => Ok(WakeReceipt::Folded),
            }
        })
    }

    fn recovery_budget_reset(
        &self,
        session: SessionId,
        reason: Option<String>,
    ) -> BoxFuture<'_, Result<ResetReceipt, OpsError>> {
        Box::pin(async move {
            // The reset is an explicit appended event — there is no
            // silent counter wipe (constraint 1: every observable change
            // is an append; the budget counters a fresh drain derives
            // come from config, the event is the audit record).
            let events = self
                .store
                .rebuild(session)
                .await
                .map_err(|e| OpsError::Internal(format!("budget reset: {e}")))?;
            if events.is_empty() {
                return Err(OpsError::UnknownSession { session });
            }
            let state = kian_core::state::project(&events)
                .map_err(|e| OpsError::Internal(format!("budget reset: {e}")))?;
            if state.is_terminal() {
                return Err(OpsError::SessionTerminal { session });
            }
            if state == kian_core::state::SessionState::Idle {
                // The table maps the (additive) reset kind to drain
                // activity — legal only while running. An idle session
                // never consumed a recovery budget anyway.
                return Err(OpsError::SessionIdle { session });
            }
            if self.engine.is_draining(&session) {
                // Refuse rather than race: a concurrent drain owns the
                // stream head and the CAS would reject us mid-turn.
                return Err(OpsError::SessionBusy { session });
            }
            let head = self
                .store
                .head(session)
                .await
                .map_err(|e| OpsError::Internal(format!("budget reset: {e}")))?;
            let event = Event {
                seq: head.next_seq,
                session,
                epoch: head.epoch,
                ts: now_ms(),
                kind: kian_core::events::EventKind::RecoveryBudgetReset,
                bytes: serde_json::json!({
                    "reason": reason.unwrap_or_else(|| "operator reset".into())
                }),
                version: kian_core::events::SCHEMA_VERSION,
            };
            let seq = event.seq;
            let epoch = event.epoch.0;
            let ts = event.ts;
            self.store
                .append(
                    session,
                    event,
                    kian_store::AppendGuard {
                        epoch: head.epoch,
                        expected_seq: head.next_seq,
                    },
                )
                .await
                .map_err(|e| OpsError::Internal(format!("budget reset append: {e}")))?;
            tracing::info!(session = %session.0, seq, "recovery budget reset appended");
            Ok(ResetReceipt { seq, epoch, ts })
        })
    }

    fn mock_channel_send(
        &self,
        channel: String,
        topic: Option<String>,
        payload: serde_json::Value,
    ) -> BoxFuture<'_, Result<MockSendReceipt, OpsError>> {
        Box::pin(async move {
            // Demo/ injection: one event onto a configured mock
            // channel; the subscription engine does the rest. Only the
            // superagent assembly carries channels — anything else is
            // an honest error, never a silent no-op.
            let runtime = self.superagent.as_ref().ok_or_else(|| {
                OpsError::Internal(
                    "mock_channel_send requires the superagent assembly \
                     (no `superagent:` section in kian.yaml)"
                        .into(),
                )
            })?;
            let mock = runtime.mock_channel(&channel).ok_or_else(|| {
                OpsError::Internal(format!(
                    "channel `{channel}` is not a configured mock channel (configured: {})",
                    runtime
                        .channels
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
            let event = mock.inject(topic.as_deref(), payload);
            Ok(MockSendReceipt {
                event_id: event.event_id.clone(),
                channel,
                topic,
            })
        })
    }

    fn memory_dream(
        &self,
        session: SessionId,
        source: SessionId,
        from_seq: u64,
        to_seq: u64,
    ) -> BoxFuture<'_, Result<DreamAdmitReceipt, OpsError>> {
        Box::pin(async move {
            // Memory off is a closed refusal, never a silent no-op.
            let memory = self.memory.as_ref().ok_or(OpsError::MemoryNotConfigured)?;

            // Read-before-act on the memory session: an unknown id is a
            // typo; a terminal stream is frozen (no new dreams land).
            let events = self
                .store
                .rebuild(session)
                .await
                .map_err(|e| OpsError::Internal(format!("memory_dream: {e}")))?;
            if events.is_empty() {
                return Err(OpsError::UnknownSession { session });
            }

            // Idempotency, first window: the dream prompt's
            // `input/admitted` row IS the durable record of the
            // request (its header carries source + range), so a
            // repeated range is derivable from the stream alone —
            // including on a frozen (completed) memory session, where
            // it stays an honest no-op forever. A range still pending
            // in the inbox (admitted but not yet consumed) dedupes on
            // the idempotency key at the admit below (second window).
            for ev in &events {
                if let Some(header) = dream_header_of_stream(ev)
                    && header.source_session == source
                    && header.start_seq == from_seq
                    && header.end_seq == to_seq
                {
                    tracing::info!(
                        session = %session.0,
                        source = %source.0,
                        "dream request already in the stream — no-op"
                    );
                    return Ok(DreamAdmitReceipt {
                        session,
                        source,
                        from_seq,
                        to_seq,
                        deduplicated: true,
                        bytes_read: 0,
                        prompt_bytes: 0,
                    });
                }
            }

            let state = kian_core::state::project(&events)
                .map_err(|e| OpsError::Internal(format!("memory_dream: {e}")))?;
            if state.is_terminal() {
                // A dream settles its memory session (the kian-86
                // contract: notes land, then the end write). A NEW
                // range on the frozen session refuses honestly — the
                // operator dreams it on a fresh memory session.
                return Err(OpsError::SessionTerminal { session });
            }

            // Read the source range and render the transcript under the
            // cap — an over-cap range is refused honestly, never
            // truncated (round-8 plan D3).
            let source_events = self
                .store
                .rebuild(source)
                .await
                .map_err(|e| OpsError::Internal(format!("memory_dream source read: {e}")))?;
            if source_events.is_empty() {
                return Err(OpsError::UnknownSource {
                    source_session: source,
                });
            }
            // B-1 (kian-91): the range must lie inside the source
            // stream, bounded by its head — the LAST event's seq + 1,
            // read from the rebuilt stream itself (constraint 1: never
            // a column). A `to_seq` past the head would silently clip
            // the transcript (MEMORY-MODEL D3) while every note's
            // `derived_from` keeps the inflated range — provenance
            // naming events that do not exist (L2). Closed refusal, no
            // admission, no partial state.
            let head_next_seq = source_events
                .last()
                .map(|ev| ev.seq + 1)
                .expect("non-empty source");
            if from_seq < 1 || from_seq >= to_seq || to_seq > head_next_seq {
                return Err(OpsError::SourceRangeOutOfBounds {
                    source_session: source,
                    from_seq,
                    to_seq,
                    head_next_seq,
                });
            }
            let range: Vec<kian_core::events::Event> = source_events
                .iter()
                .filter(|ev| ev.seq >= from_seq && ev.seq < to_seq)
                .cloned()
                .collect();
            if range.is_empty() {
                return Err(OpsError::EmptySourceRange {
                    source_session: source,
                    from_seq,
                    to_seq,
                });
            }
            let transcript = render_transcript(&range);
            let bytes_read = transcript.len() as u64;
            if bytes_read > memory.config().max_bytes_read {
                return Err(OpsError::SourceRangeTooLarge {
                    source_session: source,
                    from_seq,
                    to_seq,
                    bytes: bytes_read,
                    cap: memory.config().max_bytes_read,
                });
            }

            // ONE durable inbox row under the dream idempotency key; the
            // admit wakes. B-2 (kian-91): the row is the ops-plane
            // dream envelope — the rendered prompt as `content` plus
            // the trusted, server-set `dream` marker (exactly those two
            // keys). The drain consumes it to one `input/admitted`
            // carrying the marker; routing and provenance read the
            // marker, NEVER the message text, so a forged
            // `[kian-dream: ...]` header in a plain submit cannot flip
            // a session onto the dreamer. A repeated range dedupes onto
            // the existing row (no-op).
            let prompt = kian_memory::render_dream_prompt(
                memory.scope(),
                source,
                from_seq,
                to_seq,
                &transcript,
            );
            let prompt_bytes = prompt.len() as u64;
            let payload =
                kian_memory::dream_admit_payload(memory.scope(), source, from_seq, to_seq, &prompt);
            let key = kian_memory::dream_idempotency_key(source, from_seq, to_seq);
            let outcome = self
                .engine
                .admit(session, payload, Some(key))
                .await
                .map_err(|e| OpsError::Internal(format!("memory_dream admit: {e}")))?;
            tracing::info!(
                session = %session.0,
                source = %source.0,
                range = ?(from_seq, to_seq),
                deduplicated = outcome.deduplicated,
                "dream request admitted"
            );
            Ok(DreamAdmitReceipt {
                session,
                source,
                from_seq,
                to_seq,
                deduplicated: outcome.deduplicated,
                bytes_read,
                prompt_bytes,
            })
        })
    }
}

/// The dream header of one event, if that event is a dream-prompt
/// `input/admitted` row (thin per-event view of
/// [`kian_memory::dream_request_in`]).
fn dream_header_of_stream(ev: &kian_core::events::Event) -> Option<kian_memory::DreamHeader> {
    kian_memory::dream_request_in(std::slice::from_ref(ev))
}

/// Builds the dreamer side of the memory assembly: the configured
/// dreamer agent's own ReAct loop (its `main` slot, its tools, its
/// permission table, its evidence gate) wrapped in the
/// [`kian_memory::DreamDriver`]. Fail-closed like every other loop
/// build (unlisted tools refuse to boot). `redactor` is the assembly's
/// configured secret redactor — note text passes it before append
/// (kian-91, review M-1 / MEMORY-MODEL N4).
fn build_dream_driver(
    config: &ServeConfig,
    section: &crate::config::MemorySection,
    ctx: &kian_plugin::Ctx,
    policy: Arc<kian_orchestrator::recovery::PolicyTable>,
    redactor: kian_core::secrets::SecretRedactor,
) -> Result<Arc<kian_memory::DreamDriver>, ServeError> {
    let registry = ctx
        .resolve::<kian_llm::turn::ProviderRegistry>()
        .ok_or_else(|| ServeError::Assembly("memory: ProviderRegistry not registered".into()))?;
    let tools = ctx
        .resolve::<dyn kian_core::seams::ToolProvider>()
        .ok_or_else(|| ServeError::Assembly("memory: ToolProvider not registered".into()))?;
    let agent_cfg = config
        .dreamer_agent(&section.dreamer.agent)
        .ok_or_else(|| {
            ServeError::Assembly(format!(
                "memory.dreamer.agent `{}` names no configured agent",
                section.dreamer.agent
            ))
        })?;
    // The dreamer's `tools:` list filters the mounted surface —
    // fail-closed against typos, exactly like the default agent's.
    let unmatched = kian_loop::tools::unmatched_tool_names(&agent_cfg.tools, &tools.schemas());
    if !unmatched.is_empty() {
        return Err(ServeError::Assembly(format!(
            "memory.dreamer agent `{}` lists tools not mounted by the tool provider: {unmatched:?}",
            agent_cfg.id
        )));
    }
    let tools = Arc::new(kian_loop::tools::FilteredTools::new(
        tools,
        &agent_cfg.tools,
    ));
    let agent = config
        .dreamer_agent_def(&section.dreamer.agent)
        .expect("validated: the agent id resolves");
    let react = ReActLoop::with_evidence_gate(
        agent,
        registry,
        tools,
        policy as Arc<dyn kian_core::seams::RecoveryPolicySource>,
        ReActConfig {
            slot: kian_core::model::SlotName("main".into()),
            max_iterations: config.drain.max_iterations,
            permissions: agent_cfg.permissions.clone(),
        },
        agent_cfg.evidence_gate.clone(),
    );
    Ok(Arc::new(
        kian_memory::DreamDriver::new(react, section.dreamer.caps()).with_redactor(redactor),
    ))
}

// ---------------------------------------------------------------------------
// WebhookPlane: the inbound surface's sole capability (verify +
// hand the verified delivery to the subscription pipeline — an inbox
// write, control-plane-legal)
// ---------------------------------------------------------------------------

impl WebhookPlane for Server {
    fn ingest_verified_github(
        &self,
        event_type: String,
        delivery_id: String,
        body: serde_json::Value,
    ) -> BoxFuture<'_, Result<usize, WebhookError>> {
        Box::pin(async move {
            // The receiver module authenticated the bytes; the channel
            // maps + dedupes them, and the subscription consumers
            // (started after the inbox rebuild) do the admitting. This
            // implementation only routes between the two — it touches
            // no execution surface itself.
            let Some(runtime) = self.superagent.as_ref() else {
                return Err(WebhookError(
                    "github webhook delivered but the superagent assembly is not running".into(),
                ));
            };
            let Some(channel) = runtime.github_channels.values().next() else {
                return Err(WebhookError(
                    "github webhook delivered but no github channel is configured".into(),
                ));
            };
            Ok(channel.ingest_delivery(&event_type, &delivery_id, &body))
        })
    }
}

// ---------------------------------------------------------------------------
// GET /metrics — observability (engine counters + cache report)
// ---------------------------------------------------------------------------

fn metrics_router(server: Arc<Server>) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(server)
}

/// Read-only observability: engine counters plus the
/// cache-byte stability report derived from the event stream —
/// a pure projection, so it is as durable as the log itself. With the
/// superagent assembly mounted, the fragment adds evidence-verdict,
/// subscription, channel-policy and watchdog counters.
async fn metrics_handler(
    axum::extract::State(server): axum::extract::State<Arc<Server>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let engine =
        serde_json::to_value(server.engine.metrics().snapshot()).unwrap_or(serde_json::Value::Null);
    let cache =
        match crate::cache_metrics::cache_stats_report(&server.store, None, server.views_enabled)
            .await
        {
            Ok(report) => serde_json::to_value(report).unwrap_or(serde_json::Value::Null),
            Err(e) => serde_json::json!({"error": e.to_string()}),
        };
    // the usage aggregate, derived on read from `turn/end` payloads
    // (pure projection — no drain ever bumps a counter).
    let usage =
        match crate::usage_metrics::usage_stats_report(&server.store, None, server.views_enabled)
            .await
        {
            Ok(report) => serde_json::to_value(report).unwrap_or(serde_json::Value::Null),
            Err(e) => serde_json::json!({"error": e.to_string()}),
        };
    let superagent = match &server.superagent {
        Some(runtime) => runtime.metrics_report(&server.store).await,
        None => serde_json::Value::Null,
    };
    // the dream counters (present iff a `memory:` section assembled;
    // pure reads of the shared driver's atomics).
    let memory = match &server.memory {
        Some(assembly) => assembly.metrics_report(),
        None => serde_json::Value::Null,
    };
    // the fan-in event-stream gauges (READ-MODEL §4.2 / API-CONTRACT
    // §6): open `GET /events` connections, the sessions they still
    // watch, and the frames they have emitted (process-local, like
    // `engine`).
    let event_stream =
        serde_json::to_value(server.event_stream.snapshot()).unwrap_or(serde_json::Value::Null);
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "engine": engine,
            "pending_wake_depth": server.engine.pending_wake_depth(),
            "cache": cache,
            "usage": usage,
            "superagent": superagent,
            "memory": memory,
            "event_stream": event_stream,
        })),
    )
        .into_response()
}
