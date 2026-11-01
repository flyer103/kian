//! Server assembly and the iron-rule boundary (card C13).
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

use crate::api::{ControlError, ControlPlane};
use crate::config::ServeConfig;
use crate::ops::{MockSendReceipt, OpsError, OpsPlane, ResetReceipt, WakeReceipt};
use crate::plugins::{
    LocalToolsPlugin, OpenAiCompatPlugin, ReActPlugin, RemoteExecPlugin, SuperAgentPlugin,
};
use crate::superagent::{ServeLoopDriver, SuperAgentRuntime, SuperAgentServices};

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
    /// Recovery-sweep period (C15). Equals `drain.heartbeat_interval_ms`;
    /// see [`Server::serve`] for the documented bound.
    recovery_tick_ms: u64,
    /// The super-agent runtime (present iff the `superagent:` section
    /// is configured): mock channels, metrics surfaces, watchdog.
    superagent: Option<Arc<SuperAgentRuntime>>,
    /// This assembly's recorded startup sequence (the C27 test hook).
    startup_order: crate::superagent::StartupOrder,
    /// Keeps the plugin-registered services alive for the server's
    /// lifetime; dropping it would unregister them (C7 disposer).
    _plugin_disposer: Disposer,
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
    /// Startup order is load-bearing (C27): config → providers →
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

        // The exec store opens before activation (plugin apply is sync).
        let exec_store = if has_remote {
            let store = ExecStore::open(&config.storage.sqlite)
                .await
                .map_err(|e| ServeError::Assembly(format!("exec store: {e}")))?;
            Some(store)
        } else {
            None
        };

        let mut ctx = PluginCtx::new();
        let mut plugins = kian_plugin::kian_plugins![
            OpenAiCompatPlugin { providers },
            LocalToolsPlugin {
                root: config.tools.root.clone().into(),
                exec_timeout_ms: config.tools.exec_timeout_ms,
            },
        ];
        if let Some(store) = exec_store {
            plugins.register(RemoteExecPlugin {
                store,
                endpoints: config.endpoints.clone(),
            });
        }
        let superagent_services: Option<()> = if let Some(sa) = superagent_cfg {
            // Super-agent mode: the bundle plugin builds all three
            // loops (router/worker/plain); `react-loop` is absent.
            let router_agent = config
                .named_agent_def(&sa.router.agent)
                .ok_or_else(|| ServeError::Assembly("router agent missing".into()))?;
            let worker_agent = config
                .named_agent_def(&sa.worker.agent)
                .ok_or_else(|| ServeError::Assembly("worker agent missing".into()))?;
            let worker_gate = config
                .named_agent(&sa.worker.agent)
                .map(|agent| agent.evidence_gate.clone())
                .ok_or_else(|| ServeError::Assembly("worker agent missing".into()))?;
            let router_session = kian_core::model::SessionId(
                uuid::Uuid::parse_str(&sa.router.session_id)
                    .map_err(|_| ServeError::Assembly("router session id is not a uuid".into()))?,
            );
            let rules = sa.subscription_rules().map_err(ServeError::Config)?;
            let inbox = crate::superagent::EngineInbox::new();

            // Mock channels by configured name (v1's only adapter type).
            let mut mock_channels = std::collections::BTreeMap::new();
            for (name, channel) in &sa.channels {
                if channel.channel_type == "mock-im" {
                    mock_channels.insert(
                        name.clone(),
                        Arc::new(kian_superagent::MockChannel::new(name.clone())),
                    );
                }
            }

            plugins.register(SuperAgentPlugin {
                router_agent,
                worker_agent,
                default_agent: config.agent_def(),
                router_session,
                router_slot: sa.router.slot.clone(),
                max_iterations: config.drain.max_iterations,
                policy: Arc::clone(&policy),
                rules,
                mock_channels,
                worker_gate,
                default_gate: config.agent.evidence_gate.clone(),
                inbox: Arc::clone(&inbox),
                has_remote,
            });
            None // services resolve from the Ctx after activation
        } else {
            // Phase-1 mode: exactly the round-1 loop plugin.
            plugins.register(ReActPlugin {
                agent: config.agent_def(),
                policy: Arc::clone(&policy) as Arc<PolicyTable>,
                config: ReActConfig {
                    slot: kian_core::model::SlotName(config.drain.slot.clone()),
                    max_iterations: config.drain.max_iterations,
                },
                gate: config.agent.evidence_gate.clone(),
            });
            None
        };
        let plugin_disposer = plugins.activate(&mut ctx)?;

        // -- 3. driver resolution -----------------------------------------
        let (driver, services) = if superagent_cfg.is_some() {
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
        let _ = superagent_services;

        // -- 4. store, engine ----------------------------------------------
        let store = EventStore::open(&config.storage.sqlite).await?;
        let engine_config = EngineConfig {
            holder: config.drain.holder.clone(),
            lease_ttl_ms: config.drain.lease_ttl_ms,
            heartbeat_interval_ms: config.drain.heartbeat_interval_ms,
        };
        let engine = Engine::new(store.clone(), driver, engine_config);

        // The bundle's inbox port learns the engine (the C23 cycle
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
            superagent: None,
            startup_order: Arc::clone(&startup_order),
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

    /// This assembly's recorded startup sequence (the C27 acceptance
    /// hook: subscription/watchdog starts must follow
    /// `startup_rebuild_done`).
    pub fn startup_order(&self) -> Vec<&'static str> {
        self.startup_order
            .lock()
            .expect("startup order lock")
            .clone()
    }

    /// The super-agent runtime handle (present iff a `superagent:`
    /// section assembled this server). Embedders and the S4 e2e tests
    /// reach the mock channels (delivery capture) through this — the
    /// same handle the ops plane's `mock_channel_send` uses.
    pub fn superagent_runtime(&self) -> Option<&Arc<SuperAgentRuntime>> {
        self.superagent.as_ref()
    }

    /// The HTTP router for this server. The control-plane handlers see
    /// only the [`ControlPlane`] trait object — never the engine or
    /// store. The merged `/metrics` route (C17 observability) lives in
    /// this module, not `api`: it reads counters and derives the cache
    /// report from the store, which the iron-rule handler module must
    /// not even name. The `/ops/rpc` plane (C14 operator actions:
    /// `wake_now`, `recovery_budget_reset`) is merged here too, behind
    /// its own [`OpsPlane`] trait — the control plane's Phase-1 wire
    /// surface stays untouched.
    pub fn router(self: &Arc<Self>) -> axum::Router {
        let plane: Arc<dyn ControlPlane> = self.clone();
        let ops: Arc<dyn OpsPlane> = self.clone();
        crate::api::router(plane, self.sse_poll_ms)
            .merge(metrics_router(self.clone()))
            .merge(crate::ops::router(ops))
    }

    /// Serves on `addr` until the process is stopped.
    ///
    /// Also spawns the S1 recovery-detection ticker (C15, DESIGN §13
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
        axum::serve(listener, self.router())
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
        // consumes it in the same sweep (C16 batch semantics).
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
// OpsPlane: the C14 operator surface (wake-now, recovery budget reset)
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
                // The C3 table maps the (additive) reset kind to drain
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
            // Demo/S4 injection (C27): one event onto a configured mock
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
}

// ---------------------------------------------------------------------------
// GET /metrics — C17 observability (engine counters + S3 cache report)
// ---------------------------------------------------------------------------

fn metrics_router(server: Arc<Server>) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(server)
}

/// Read-only observability: engine counters (C9) plus the S3
/// cache-byte stability report (C17) derived from the event stream —
/// a pure projection, so it is as durable as the log itself. With the
/// superagent assembly mounted, the C27 fragment adds evidence-verdict,
/// subscription, channel-policy and watchdog counters.
async fn metrics_handler(
    axum::extract::State(server): axum::extract::State<Arc<Server>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let engine =
        serde_json::to_value(server.engine.metrics().snapshot()).unwrap_or(serde_json::Value::Null);
    let cache = match crate::cache_metrics::cache_stats_report(&server.store, None).await {
        Ok(report) => serde_json::to_value(report).unwrap_or(serde_json::Value::Null),
        Err(e) => serde_json::json!({"error": e.to_string()}),
    };
    let superagent = match &server.superagent {
        Some(runtime) => runtime.metrics_report(&server.store).await,
        None => serde_json::Value::Null,
    };
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "engine": engine,
            "pending_wake_depth": server.engine.pending_wake_depth(),
            "cache": cache,
            "superagent": superagent,
        })),
    )
        .into_response()
}
