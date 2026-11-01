//! `kian.yaml` configuration loading (card C13).
//!
//! Storage (sqlite), model slots, session/drain budgets, recovery policy
//! tables, providers and the plugin list. Loading is **fail-closed**: a
//! malformed file, an unknown key, an unknown provider reference or an
//! unparseable `listen` address refuses to start with a clear error
//! (never a silent default).

use std::collections::BTreeMap;
use std::net::SocketAddr;

use kian_core::model::{AgentDef, Budgets, SlotName, SlotSpec, SlotTarget};
use kian_loop::evidence::EvidenceGateConfig;
use kian_orchestrator::recovery::PolicyConfig;
use kian_superagent::rules::{
    CorrelateBy, Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet,
};
use serde::{Deserialize, Serialize};

/// Exec timeout default mirrors the built-in tools crate.
pub const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Typed configuration errors (fail-closed, message-first)
// ---------------------------------------------------------------------------

/// Every way loading `kian.yaml` can refuse to start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("malformed kian.yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid kian.yaml: {0}")]
    Invalid(String),
}

fn invalid(msg: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(msg.into())
}

// ---------------------------------------------------------------------------
// Raw document shape (serde; unknown keys are errors, not surprises)
// ---------------------------------------------------------------------------

/// The whole `kian.yaml` document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeConfig {
    /// Bind address, `"host:port"` (parsed and validated).
    pub listen: String,
    /// Storage (Phase 1: sqlite only).
    pub storage: StorageConfig,
    /// The agent definition (id, model slots, tools, session budgets).
    pub agent: AgentConfig,
    /// openai-compatible endpoints the slots may target.
    pub providers: BTreeMap<String, ProviderConfig>,
    /// Enabled built-in plugins (validated against the known set).
    #[serde(default = "default_plugins")]
    pub plugins: Vec<String>,
    /// Drain/lease tuning and loop budgets.
    #[serde(default)]
    pub drain: DrainConfig,
    /// Local tool settings (fs root, exec timeout).
    #[serde(default)]
    pub tools: ToolsConfig,
    /// SSE poll interval.
    #[serde(default)]
    pub sse: SseConfig,
    /// The `recovery:` section, parsed fail-closed by C10's
    /// [`PolicyConfig`]. Kept as raw YAML so this crate never forks the
    /// policy schema.
    pub recovery: serde_yaml::Value,
    /// Additional named agent definitions (C27; id → definition). The
    /// single `agent:` section remains the **default agent** shorthand —
    /// plain sessions always run it. Router/Worker definitions live here
    /// and are referenced from the `superagent:` section.
    #[serde(default)]
    pub agents: BTreeMap<String, AgentConfig>,
    /// The `superagent:` section (C27). `None` (the section absent) is
    /// the Phase-1 configuration: no bundle is assembled and every
    /// existing behavior is unchanged.
    #[serde(default)]
    pub superagent: Option<SuperAgentConfig>,
    /// Remote execution endpoints (C27 over the C20 queue). Empty = no
    /// remote exec.
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
}

fn default_plugins() -> Vec<String> {
    vec![
        "openai-compat".into(),
        "local-tools".into(),
        "react-loop".into(),
    ]
}

/// Storage selection. Phase 1 is sqlite-only by design.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Path of the sqlite database file.
    pub sqlite: String,
}

/// The `agent:` section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub id: String,
    #[serde(default = "default_agent_version")]
    pub version: u64,
    /// Slot name → slot spec (string keys; converted to `SlotName`).
    #[serde(default)]
    pub model_slots: BTreeMap<String, SlotSpecDef>,
    /// Tool names the agent may call.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Session budgets (A5; `null` = unlimited).
    #[serde(default)]
    pub budgets: BudgetsDef,
    /// Evidence-gate tuning (C24, wired by the C27 assembly): the
    /// default agent's gate mounts on the built-in loop's stop path; a
    /// superagent worker agent's gate mounts on the worker wrapper.
    /// Default is disabled — Phase-1 behavior unchanged.
    #[serde(default)]
    pub evidence_gate: EvidenceGateConfig,
}

fn default_agent_version() -> u64 {
    1
}

/// Wire shape of [`kian_core::model::SlotSpec`] (identical fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotSpecDef {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub fallback: Vec<SlotTargetDef>,
}

/// Wire shape of [`kian_core::model::SlotTarget`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotTargetDef {
    pub provider: String,
    pub model: String,
}

/// Wire shape of [`kian_core::model::Budgets`] (adds `Default`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BudgetsDef {
    #[serde(default)]
    pub tokens: Option<u64>,
    #[serde(default)]
    pub wall_clock_ms: Option<u64>,
}

/// One openai-compatible endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Base URL without trailing slash, e.g. `https://api.openai.com/v1`.
    pub base_url: String,
    /// Environment variable holding the API key (resolved at startup;
    /// a missing variable refuses to start — secrets never live in the
    /// config file).
    pub api_key_env: String,
    /// Chat-completions path appended to `base_url`.
    #[serde(default = "default_chat_path")]
    pub chat_completions_path: String,
}

fn default_chat_path() -> String {
    "/chat/completions".into()
}

/// The `drain:` section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainConfig {
    /// Claim holder identity.
    #[serde(default = "default_holder")]
    pub holder: String,
    /// Lease TTL, milliseconds (liveness only, ADR-005).
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_ms: u64,
    /// Heartbeat period, milliseconds.
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_ms: u64,
    /// ReAct iteration budget per session.
    #[serde(default = "default_max_iterations")]
    pub max_iterations: usize,
    /// The model slot `serve` drives.
    #[serde(default = "default_slot")]
    pub slot: String,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            holder: default_holder(),
            lease_ttl_ms: default_lease_ttl(),
            heartbeat_interval_ms: default_heartbeat(),
            max_iterations: default_max_iterations(),
            slot: default_slot(),
        }
    }
}

fn default_holder() -> String {
    "kian-serve".into()
}
fn default_lease_ttl() -> u64 {
    30_000
}
fn default_heartbeat() -> u64 {
    10_000
}
fn default_max_iterations() -> usize {
    kian_loop::DEFAULT_MAX_ITERATIONS
}
fn default_slot() -> String {
    "main".into()
}

/// The `tools:` section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolsConfig {
    /// Filesystem root the built-in fs tools are confined to.
    #[serde(default = "default_tools_root")]
    pub root: String,
    /// Default local exec timeout, milliseconds.
    #[serde(default = "default_exec_timeout")]
    pub exec_timeout_ms: u64,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            root: default_tools_root(),
            exec_timeout_ms: default_exec_timeout(),
        }
    }
}

fn default_tools_root() -> String {
    ".".into()
}
fn default_exec_timeout() -> u64 {
    DEFAULT_EXEC_TIMEOUT_MS
}

/// The `sse:` section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SseConfig {
    /// How often the SSE `events` endpoint polls the event store.
    #[serde(default = "default_poll_ms")]
    pub poll_interval_ms: u64,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: default_poll_ms(),
        }
    }
}

fn default_poll_ms() -> u64 {
    100
}

// ---------------------------------------------------------------------------
// C27: super-agent + endpoint configuration
// ---------------------------------------------------------------------------

/// The `superagent:` section. Presence switches `serve` into the
/// super-agent assembly (bundle plugins, resident Router, subscription
/// engine); absence keeps the Phase-1 single-agent server byte-for-byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuperAgentConfig {
    pub router: SuperAgentRouterConfig,
    pub worker: SuperAgentWorkerConfig,
    /// Declarative subscription rules (DESIGN §4.1). Wire form:
    /// `{channel, match: always | {mentions: t} | {topic: t},
    /// route: router | {passthrough: {correlate_by: topic}}}`; converted
    /// fail-closed to the bundle's [`SubscriptionRuleSet`] by
    /// [`SuperAgentConfig::subscription_rules`].
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionRuleDef>,
    /// Channel name → channel config. Every rule's `channel` must be a
    /// key here (fail-closed cross-reference).
    #[serde(default)]
    pub channels: BTreeMap<String, ChannelConfig>,
}

/// The resident Router binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuperAgentRouterConfig {
    /// The configured resident Router session id (a uuid). The session
    /// itself is bootstrapped by admitting to this id (first hold appends
    /// `session/created`).
    pub session_id: String,
    /// Id of the Router's agent definition in `agents:`.
    pub agent: String,
    /// The Router's cheap fast slot (must exist in the Router agent's
    /// `model_slots`).
    #[serde(default = "default_router_slot")]
    pub slot: String,
}

fn default_router_slot() -> String {
    "router".into()
}

/// The Worker binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuperAgentWorkerConfig {
    /// Id of the worker's agent definition in `agents:`.
    pub agent: String,
    /// The version worker threads pin (ADR-006). Default: the referenced
    /// agent's `version`. An explicit value that contradicts the agent's
    /// version refuses to start (the `AgentDef.version` **is** the pin).
    pub pinned_version: Option<u64>,
}

/// One channel entry. The map key is the channel name (`mock-im` in the
/// DESIGN §4.1 examples); `type` selects the adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfig {
    #[serde(rename = "type")]
    pub channel_type: String,
}

/// Wire form of a subscription rule's match condition (the natural
/// YAML/JSON spelling of the plan's `always|mentions(str)|topic(str)`):
/// the bare keyword `always`, or a one-key map `{mentions: t}` /
/// `{topic: t}`. Anything else fails closed in
/// [`SuperAgentConfig::subscription_rules`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RuleMatcherDef {
    Keyword(String),
    Mentions { mentions: String },
    Topic { topic: String },
}

/// Wire form of a rule's route: the bare keyword `router`, or
/// `{passthrough: {correlate_by: topic}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RouteDef {
    Keyword(String),
    Passthrough { passthrough: CorrelateByWire },
}

/// Wire form of `{correlate_by: topic}` (v1 vocabulary: `topic` only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub struct CorrelateByWire {
    pub correlate_by: CorrelateByDef,
}

/// Wire form of the correlation key (v1 vocabulary: `topic` only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CorrelateByDef {
    Topic,
}

/// One declarative subscription rule (wire form).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionRuleDef {
    pub channel: String,
    #[serde(rename = "match")]
    pub matcher: RuleMatcherDef,
    pub route: RouteDef,
}

impl SuperAgentConfig {
    /// Converts the wire-form rules into the bundle's
    /// [`SubscriptionRuleSet`], fail-closed: unknown keywords, unknown
    /// correlation keys and empty strings are errors, never defaults.
    pub fn subscription_rules(&self) -> Result<SubscriptionRuleSet, ConfigError> {
        let mut rules = Vec::with_capacity(self.subscriptions.len());
        for (i, def) in self.subscriptions.iter().enumerate() {
            let matcher = match &def.matcher {
                RuleMatcherDef::Keyword(k) if k == "always" => RuleMatcher::Always,
                RuleMatcherDef::Mentions { mentions } if !mentions.is_empty() => {
                    RuleMatcher::Mentions(mentions.clone())
                }
                RuleMatcherDef::Topic { topic } if !topic.is_empty() => {
                    RuleMatcher::Topic(topic.clone())
                }
                other => {
                    return Err(invalid(format!(
                        "superagent.subscriptions[{i}].match `{other:?}` is not one of \
                         always | {{mentions: token}} | {{topic: name}}"
                    )));
                }
            };
            let route = match &def.route {
                RouteDef::Keyword(k) if k == "router" => Route::Router,
                RouteDef::Passthrough {
                    passthrough:
                        CorrelateByWire {
                            correlate_by: CorrelateByDef::Topic,
                        },
                } => Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
                other => {
                    return Err(invalid(format!(
                        "superagent.subscriptions[{i}].route `{other:?}` is not one of \
                         router | {{passthrough: {{correlate_by: topic}}}}"
                    )));
                }
            };
            rules.push(SubscriptionRule {
                channel: def.channel.clone(),
                matcher,
                route,
            });
        }
        Ok(SubscriptionRuleSet::new(rules))
    }
}

/// Every channel adapter type `serve` knows how to build (C27 v1: the
/// first-class mock only — real lark/GitHub adapters stay deferred).
pub const KNOWN_CHANNEL_TYPES: [&str; 1] = ["mock-im"];

/// One remote execution endpoint (over the C20 durable queue).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    pub id: String,
    /// `host:port` the server dials (endpoint listens).
    pub addr: String,
    /// How long a queued command may wait for its result before the row
    /// is honestly abandoned (`ExecError::Timeout`).
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
    /// Heartbeat staleness bound feeding the watchdog (offline iff
    /// `now - last_heartbeat > offline_after`).
    #[serde(default = "default_offline_after_ms")]
    pub offline_after_ms: u64,
}

fn default_queue_timeout_ms() -> u64 {
    60_000
}
fn default_offline_after_ms() -> u64 {
    30_000
}

// ---------------------------------------------------------------------------
// Loading + validation
// ---------------------------------------------------------------------------

/// Every built-in plugin name `serve` knows.
pub const KNOWN_PLUGINS: [&str; 5] = [
    "openai-compat",
    "local-tools",
    "react-loop",
    "superagent",
    "remote-exec",
];

impl ServeConfig {
    /// Loads and validates the config file. Fail-closed: any structural,
    /// reference or semantic problem is an error, never a default.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&text)
    }

    /// Parses and validates a config document.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_yaml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-reference validation (references resolve, semantics sane).
    pub fn validate(&self) -> Result<(), ConfigError> {
        // listen must parse as a socket address.
        self.listen().map_err(|e| {
            invalid(format!(
                "listen `{}` is not a valid host:port: {e}",
                self.listen
            ))
        })?;

        // At least one provider and one slot.
        if self.providers.is_empty() {
            return Err(invalid("providers must list at least one endpoint"));
        }
        if self.agent.model_slots.is_empty() {
            return Err(invalid("agent.model_slots must define at least one slot"));
        }

        // The driven slot must exist.
        if !self.agent.model_slots.contains_key(&self.drain.slot) {
            return Err(invalid(format!(
                "drain.slot `{}` is not defined in agent.model_slots",
                self.drain.slot
            )));
        }

        // Every provider referenced by any agent's slot (primary or
        // fallback) must be configured — a slot pointing nowhere refuses
        // to start.
        self.validate_agent_slots(&self.agent, "agent")?;

        // Plugin list: known names only; the loop implies its deps.
        for name in &self.plugins {
            if !KNOWN_PLUGINS.contains(&name.as_str()) {
                return Err(invalid(format!(
                    "unknown plugin `{name}` (known: {})",
                    KNOWN_PLUGINS.join(", ")
                )));
            }
        }
        if self.plugins.iter().any(|p| p == "react-loop") {
            for dep in ["openai-compat", "local-tools"] {
                if !self.plugins.iter().any(|p| p == dep) {
                    return Err(invalid(format!(
                        "plugin `react-loop` requires plugin `{dep}`"
                    )));
                }
            }
        }

        // C27: the bundle plugins are presence-locked to their config
        // sections — fail-closed in BOTH directions so neither a stale
        // plugin list nor a stray section boots half-assembled.
        let has_superagent_plugin = self.plugins.iter().any(|p| p == "superagent");
        if self.superagent.is_some() != has_superagent_plugin {
            return Err(invalid(
                "plugin `superagent` and the `superagent:` config section \
                 must be enabled together",
            ));
        }
        let has_remote_exec_plugin = self.plugins.iter().any(|p| p == "remote-exec");
        if !self.endpoints.is_empty() != has_remote_exec_plugin {
            return Err(invalid(
                "plugin `remote-exec` and a non-empty `endpoints:` list \
                 must be enabled together",
            ));
        }
        if has_superagent_plugin && self.plugins.iter().any(|p| p == "react-loop") {
            return Err(invalid(
                "plugin `react-loop` is the Phase-1 loop; with the \
                 `superagent` plugin the assembly builds the loop itself",
            ));
        }

        // Named agents: same reference discipline as the default agent.
        if let Some(default_id) = self.agents.get(&self.agent.id) {
            let _ = default_id;
            return Err(invalid(format!(
                "agents map must not shadow the default agent id `{}`",
                self.agent.id
            )));
        }
        for (name, agent) in &self.agents {
            if agent.id != *name {
                return Err(invalid(format!(
                    "agents `{name}` carries mismatched inner id `{}`",
                    agent.id
                )));
            }
            self.validate_agent_slots(agent, &format!("agents.{name}"))?;
        }

        // Endpoints: unique ids, dialable addresses, sane timeouts.
        let mut seen_endpoint = std::collections::BTreeSet::new();
        for ep in &self.endpoints {
            if !seen_endpoint.insert(ep.id.clone()) {
                return Err(invalid(format!("duplicate endpoint id `{}`", ep.id)));
            }
            ep.addr.parse::<SocketAddr>().map_err(|e| {
                invalid(format!(
                    "endpoint `{}` addr `{}` invalid: {e}",
                    ep.id, ep.addr
                ))
            })?;
            if ep.queue_timeout_ms == 0 {
                return Err(invalid(format!(
                    "endpoint `{}` queue_timeout_ms must be > 0",
                    ep.id
                )));
            }
            if ep.offline_after_ms == 0 {
                return Err(invalid(format!(
                    "endpoint `{}` offline_after_ms must be > 0",
                    ep.id
                )));
            }
        }

        // The superagent section's cross-references.
        if let Some(sa) = &self.superagent {
            if sa.channels.is_empty() {
                return Err(invalid(
                    "superagent.channels must declare at least one channel",
                ));
            }
            for (name, channel) in &sa.channels {
                if !KNOWN_CHANNEL_TYPES.contains(&channel.channel_type.as_str()) {
                    return Err(invalid(format!(
                        "superagent.channels `{name}` has unknown type `{}` (known: {})",
                        channel.channel_type,
                        KNOWN_CHANNEL_TYPES.join(", ")
                    )));
                }
            }
            uuid::Uuid::parse_str(&sa.router.session_id).map_err(|_| {
                invalid(format!(
                    "superagent.router.session_id `{}` is not a uuid",
                    sa.router.session_id
                ))
            })?;
            let router_agent = self.agents.get(&sa.router.agent).ok_or_else(|| {
                invalid(format!(
                    "superagent.router.agent `{}` is not defined in agents",
                    sa.router.agent
                ))
            })?;
            if !router_agent.model_slots.contains_key(&sa.router.slot) {
                return Err(invalid(format!(
                    "superagent.router.slot `{}` is not defined in agents.{}.model_slots",
                    sa.router.slot, sa.router.agent
                )));
            }
            let worker_agent = self.agents.get(&sa.worker.agent).ok_or_else(|| {
                invalid(format!(
                    "superagent.worker.agent `{}` is not defined in agents",
                    sa.worker.agent
                ))
            })?;
            if let Some(pin) = sa.worker.pinned_version
                && pin != worker_agent.version
            {
                return Err(invalid(format!(
                    "superagent.worker.pinned_version {pin} contradicts agents.{}.version {} \
                     (the AgentDef version IS the pin; drop the override or align them)",
                    sa.worker.agent, worker_agent.version
                )));
            }
            for (i, rule) in sa.subscriptions.iter().enumerate() {
                if !sa.channels.contains_key(&rule.channel) {
                    return Err(invalid(format!(
                        "superagent.subscriptions[{i}] references channel `{}` which is not \
                         declared in superagent.channels",
                        rule.channel
                    )));
                }
            }
            // Wire-form rules must convert cleanly (unknown matcher/route
            // spellings refuse to start).
            sa.subscription_rules()?;
        }

        // Degenerate timing constants that would make liveness meaningless.
        if self.drain.lease_ttl_ms == 0 {
            return Err(invalid("drain.lease_ttl_ms must be > 0"));
        }
        if self.drain.heartbeat_interval_ms == 0 {
            return Err(invalid("drain.heartbeat_interval_ms must be > 0"));
        }
        if self.drain.max_iterations == 0 {
            return Err(invalid("drain.max_iterations must be > 0"));
        }
        if self.sse.poll_interval_ms == 0 {
            return Err(invalid("sse.poll_interval_ms must be > 0"));
        }

        // The recovery section must parse through C10's fail-closed
        // loader (unknown classes / malformed actions are errors).
        self.policy()?;
        Ok(())
    }

    /// The validated bind address.
    pub fn listen(&self) -> Result<SocketAddr, ConfigError> {
        self.listen
            .parse()
            .map_err(|e| invalid(format!("bad listen address: {e}")))
    }

    /// Fail-closed provider-reference check for one agent definition
    /// (shared by the default `agent:` and every `agents:` entry).
    fn validate_agent_slots(&self, agent: &AgentConfig, label: &str) -> Result<(), ConfigError> {
        if agent.model_slots.is_empty() {
            return Err(invalid(format!(
                "{label}.model_slots must define at least one slot"
            )));
        }
        for (slot, spec) in &agent.model_slots {
            if !self.providers.contains_key(&spec.provider) {
                return Err(invalid(format!(
                    "{label} slot `{slot}` targets provider `{}` which is not defined in providers",
                    spec.provider
                )));
            }
            for fb in &spec.fallback {
                if !self.providers.contains_key(&fb.provider) {
                    return Err(invalid(format!(
                        "{label} slot `{slot}` fallback targets provider `{}` which is not defined in providers",
                        fb.provider
                    )));
                }
            }
        }
        Ok(())
    }

    /// The named agent's definition, if configured.
    #[must_use]
    pub fn named_agent(&self, id: &str) -> Option<&AgentConfig> {
        self.agents.get(id)
    }

    /// The recovery policy section, parsed fail-closed by C10.
    pub fn policy(&self) -> Result<PolicyConfig, ConfigError> {
        // Re-serialize just the recovery node; PolicyConfig accepts the
        // bare `budget:`/`rules:` section shape.
        let text = serde_yaml::to_string(&self.recovery)
            .map_err(|e| invalid(format!("cannot re-serialize recovery section: {e}")))?;
        PolicyConfig::from_yaml(&text).map_err(|e| invalid(format!("recovery section: {e}")))
    }

    /// The compiled agent definition handed to the loop.
    #[must_use]
    pub fn agent_def(&self) -> AgentDef {
        agent_def_of(&self.agent)
    }

    /// The compiled definition of a named agent (C27; `None` when the id
    /// is not configured).
    #[must_use]
    pub fn named_agent_def(&self, id: &str) -> Option<AgentDef> {
        self.agents.get(id).map(agent_def_of)
    }
}

// ---------------------------------------------------------------------------
// AgentDef conversion
// ---------------------------------------------------------------------------

/// Compiles one wire-form agent definition into the core [`AgentDef`]
/// (shared by the default `agent:` section and every `agents:` entry).
fn agent_def_of(agent: &AgentConfig) -> AgentDef {
    AgentDef {
        id: agent.id.clone(),
        version: agent.version,
        model_slots: agent
            .model_slots
            .iter()
            .map(|(name, spec)| {
                (
                    SlotName(name.clone()),
                    SlotSpec {
                        provider: spec.provider.clone(),
                        model: spec.model.clone(),
                        fallback: spec
                            .fallback
                            .iter()
                            .map(|t| SlotTarget {
                                provider: t.provider.clone(),
                                model: t.model.clone(),
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
        tools: agent.tools.clone(),
        budgets: Budgets {
            tokens: agent.budgets.tokens,
            wall_clock_ms: agent.budgets.wall_clock_ms,
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal-but-valid config used as the fixture basis.
    fn valid_yaml() -> String {
        r#"listen: "127.0.0.1:7331"
storage:
  sqlite: "/tmp/kian-test.sqlite"
agent:
  id: default
  model_slots:
    main:
      provider: openai
      model: gpt-test
      fallback:
        - provider: openai
          model: gpt-fallback
  tools: ["fs.read", "fs.write", "exec"]
  budgets:
    tokens: 100000
providers:
  openai:
    base_url: "http://127.0.0.1:9/v1"
    api_key_env: KIAN_API_KEY
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: rate_limited
      action: {backoff_retry: {delay_ms: 500}}
    - class: unknown
      action: fail
"#
        .into()
    }

    #[test]
    fn valid_config_parses_and_validates() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert_eq!(cfg.listen().unwrap().port(), 7331);
        assert_eq!(cfg.drain.max_iterations, 8);
        let agent = cfg.agent_def();
        assert_eq!(agent.model_slots.len(), 1);
        let policy = cfg.policy().unwrap();
        assert_eq!(policy.rules.len(), 2);
    }

    #[test]
    fn malformed_yaml_is_rejected_with_clear_error() {
        let err = ServeConfig::parse("listen: [broken").unwrap_err();
        assert!(err.to_string().contains("malformed kian.yaml"), "{err}");
    }

    #[test]
    fn unknown_top_level_key_is_rejected() {
        let mut yaml = valid_yaml();
        yaml.push_str("not_a_key: 1\n");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not_a_key") || msg.contains("malformed"),
            "{msg}"
        );
    }

    #[test]
    fn slot_referencing_unknown_provider_refuses_to_start() {
        let yaml = valid_yaml().replace("provider: openai", "provider: nowhere");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("not defined in providers"),
            "{err}"
        );
    }

    #[test]
    fn drain_slot_must_exist() {
        let yaml = valid_yaml().replace("drain:\n", "drain:\n  slot: missing\n");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[test]
    fn unknown_plugin_name_is_rejected() {
        let yaml = valid_yaml() + "plugins: [\"openai-compat\", \"local-tools\", \"nope\"]\n";
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("unknown plugin"), "{err}");
    }

    #[test]
    fn react_loop_requires_its_dependencies() {
        let yaml = valid_yaml() + "plugins: [\"openai-compat\", \"react-loop\"]\n";
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("requires plugin"), "{err}");
    }

    #[test]
    fn bad_listen_address_is_rejected() {
        let yaml = valid_yaml().replace("127.0.0.1:7331", "not-an-address");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("listen"), "{err}");
    }

    #[test]
    fn missing_recovery_section_is_rejected() {
        // `recovery` is required: no silent defaults for policy tables.
        let yaml = valid_yaml();
        let without = yaml.split("recovery:").next().unwrap().to_string();
        let err = ServeConfig::parse(&without).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("recovery") || msg.contains("malformed") || msg.contains("missing field"),
            "{msg}"
        );
    }

    #[test]
    fn malformed_recovery_rules_refuse_to_start() {
        let yaml = valid_yaml().replace("class: unknown", "class: not_a_class");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("recovery"), "{err}");
    }

    #[test]
    fn zero_lease_ttl_is_rejected() {
        let yaml = valid_yaml().replace("drain:\n", "drain:\n  lease_ttl_ms: 0\n");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("lease_ttl_ms"), "{err}");
    }

    #[test]
    fn missing_file_reports_path() {
        let err = ServeConfig::load("/nonexistent/kian.yaml").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("/nonexistent/kian.yaml"), "{msg}");
        assert!(msg.contains("cannot read config file"), "{msg}");
    }

    // -----------------------------------------------------------------------
    // C27: super-agent configuration
    // -----------------------------------------------------------------------

    const ROUTER_SESSION: &str = "11111111-2222-3333-4444-555555555555";

    /// A full super-agent profile (the C27 golden shape): named router
    /// and worker agents, resident router session, subscriptions over a
    /// mock-im channel, and one remote endpoint.
    fn superagent_yaml() -> String {
        format!(
            r#"listen: "127.0.0.1:7331"
storage:
  sqlite: "/tmp/kian-test-superagent.sqlite"
agent:
  id: default
  model_slots:
    main:
      provider: openai
      model: gpt-test
  tools: ["fs.read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: openai
        model: gpt-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: openai
        model: gpt-worker
    tools: ["fs.read", "fs.write", "exec"]
    evidence_gate:
      enabled: true
      checks:
        - type: final_message_present
      max_retries: 2
superagent:
  router:
    session_id: "{ROUTER_SESSION}"
    agent: router
    slot: router
  worker:
    agent: worker
    pinned_version: 7
  subscriptions:
    - channel: mock-im
      match: always
      route: router
    - channel: ci
      match:
        topic: pr-9
      route:
        passthrough:
          correlate_by: topic
  channels:
    mock-im:
      type: mock-im
    ci:
      type: mock-im
endpoints:
  - id: ep1
    addr: "127.0.0.1:7341"
    queue_timeout_ms: 5000
    offline_after_ms: 500
providers:
  openai:
    base_url: "http://127.0.0.1:9/v1"
    api_key_env: KIAN_API_KEY
plugins: ["openai-compat", "local-tools", "superagent", "remote-exec"]
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#
        )
    }

    #[test]
    fn superagent_golden_profile_parses() {
        let cfg = ServeConfig::parse(&superagent_yaml()).expect("golden superagent config");
        let sa = cfg.superagent.as_ref().expect("superagent section");

        // Router binding: session, agent ref, slot.
        assert_eq!(sa.router.session_id, ROUTER_SESSION);
        assert_eq!(sa.router.agent, "router");
        assert_eq!(sa.router.slot, "router", "explicit slot parses");

        // Worker binding + pin.
        assert_eq!(sa.worker.agent, "worker");
        assert_eq!(sa.worker.pinned_version, Some(7));

        // Subscriptions keep declaration order and convert to the
        // bundle's rule shapes (first router, second passthrough).
        assert_eq!(sa.subscriptions.len(), 2);
        assert_eq!(sa.subscriptions[0].channel, "mock-im");
        let rules = sa.subscription_rules().expect("rules convert");
        assert_eq!(rules.len(), 2);
        assert!(!rules.0[0].is_passthrough());
        assert!(rules.0[1].is_passthrough());
        assert_eq!(rules.0[1].channel, "ci");

        // Channels and endpoints.
        assert_eq!(sa.channels.len(), 2);
        assert_eq!(sa.channels["mock-im"].channel_type, "mock-im");
        assert_eq!(cfg.endpoints.len(), 1);
        assert_eq!(cfg.endpoints[0].id, "ep1");
        assert_eq!(cfg.endpoints[0].queue_timeout_ms, 5000);
        assert_eq!(cfg.endpoints[0].offline_after_ms, 500);

        // Named agents compile to AgentDefs; the worker gate parsed.
        let worker = cfg.named_agent_def("worker").expect("worker agent");
        assert_eq!(worker.version, 7);
        assert!(worker.tools.contains(&"exec".into()));
        let worker_cfg = cfg.named_agent("worker").unwrap();
        assert!(worker_cfg.evidence_gate.enabled);
        assert_eq!(worker_cfg.evidence_gate.max_retries, 2);

        // The default agent is untouched shorthand.
        assert_eq!(cfg.agent_def().id, "default");
    }

    #[test]
    fn superagent_defaults_slot_and_pin() {
        let yaml = superagent_yaml()
            .replace("    slot: router\n", "")
            .replace("    pinned_version: 7\n", "");
        let cfg = ServeConfig::parse(&yaml).expect("defaults parse");
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(sa.router.slot, "router", "slot defaults to `router`");
        assert_eq!(sa.worker.pinned_version, None);
        let worker = cfg.named_agent("worker").unwrap();
        assert_eq!(worker.version, 7, "agent version is the pin");
    }

    #[test]
    fn superagent_unknown_field_is_rejected() {
        let yaml = superagent_yaml().replace(
            "  worker:\n    agent: worker",
            "  worker:\n    agent: worker\n    bogus: 1",
        );
        let err = ServeConfig::parse(&yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("malformed") || msg.contains("bogus"), "{msg}");
    }

    #[test]
    fn superagent_unknown_agent_reference_is_rejected() {
        let err = ServeConfig::parse(&superagent_yaml().replace("agent: worker", "agent: ghost"))
            .unwrap_err();
        assert!(
            err.to_string().contains("`ghost` is not defined in agents"),
            "{}",
            err
        );
    }

    #[test]
    fn superagent_unknown_channel_type_is_rejected() {
        let err = ServeConfig::parse(&superagent_yaml().replace("type: mock-im", "type: lark"))
            .unwrap_err();
        assert!(err.to_string().contains("unknown type `lark`"), "{}", err);
    }

    #[test]
    fn superagent_rule_referencing_undeclared_channel_is_rejected() {
        // Remove the `ci` channel entry; the passthrough rule now dangles.
        let yaml = superagent_yaml().replace("    ci:\n      type: mock-im\n", "");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("not declared in superagent.channels"),
            "{}",
            err
        );
    }

    #[test]
    fn superagent_bad_router_session_uuid_is_rejected() {
        let err = ServeConfig::parse(&superagent_yaml().replace(ROUTER_SESSION, "not-a-uuid"))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("session_id `not-a-uuid` is not a uuid"),
            "{}",
            err
        );
    }

    #[test]
    fn superagent_missing_router_slot_is_rejected() {
        let err = ServeConfig::parse(
            &superagent_yaml().replace("    slot: router\n", "    slot: main\n"),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("slot `main` is not defined"),
            "{}",
            err
        );
    }

    #[test]
    fn superagent_pinned_version_conflict_is_rejected() {
        let err = ServeConfig::parse(
            &superagent_yaml().replace("pinned_version: 7", "pinned_version: 8"),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("contradicts agents.worker.version"),
            "{}",
            err
        );
    }

    #[test]
    fn endpoint_bad_addr_and_duplicate_ids_are_rejected() {
        let err = ServeConfig::parse(&superagent_yaml().replace("127.0.0.1:7341", "not-an-addr"))
            .unwrap_err();
        assert!(
            err.to_string().contains("addr `not-an-addr` invalid"),
            "{}",
            err
        );

        // A second entry reusing `ep1`: duplicate ids refuse to start.
        let doubled = superagent_yaml().replace(
            "    offline_after_ms: 500\n",
            "    offline_after_ms: 500\n  - id: ep1\n    addr: \"127.0.0.1:7342\"\n",
        );
        let err = ServeConfig::parse(&doubled).unwrap_err();
        assert!(err.to_string().contains("duplicate endpoint id"), "{}", err);
    }

    #[test]
    fn superagent_plugin_section_mismatch_is_rejected_both_ways() {
        // Section without the plugin.
        let err =
            ServeConfig::parse(&superagent_yaml().replace(", \"superagent\"", "")).unwrap_err();
        assert!(
            err.to_string().contains("must be enabled together"),
            "{}",
            err
        );

        // Plugin without the section: splice the whole `superagent:` block
        // out of the document (from its key to the `endpoints:` key).
        let full = superagent_yaml();
        let start = full.find("superagent:\n").expect("superagent block");
        let end = full.find("endpoints:\n").expect("endpoints key");
        let plugin_only = format!("{}{}", &full[..start], &full[end..]);
        let err = ServeConfig::parse(&plugin_only).unwrap_err();
        assert!(
            err.to_string().contains("must be enabled together"),
            "{}",
            err
        );

        // Same lock for remote-exec: endpoints without the plugin.
        let err =
            ServeConfig::parse(&superagent_yaml().replace(", \"remote-exec\"", "")).unwrap_err();
        assert!(
            err.to_string()
                .contains("plugin `remote-exec` and a non-empty `endpoints:`"),
            "{}",
            err
        );
    }

    #[test]
    fn superagent_plugin_conflicts_with_react_loop() {
        let yaml = superagent_yaml().replace(
            "plugins: [\"openai-compat\", \"local-tools\", \"superagent\", \"remote-exec\"]",
            "plugins: [\"openai-compat\", \"local-tools\", \"react-loop\", \"superagent\", \"remote-exec\"]",
        );
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("react-loop` is the Phase-1 loop"),
            "{}",
            err
        );
    }

    #[test]
    fn named_agent_shadowing_default_agent_id_is_rejected() {
        let yaml =
            superagent_yaml().replace("  router:\n    id: router", "  default:\n    id: default");
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("must not shadow the default agent id"),
            "{}",
            err
        );
    }

    #[test]
    fn named_agent_with_unknown_provider_is_rejected() {
        let yaml = superagent_yaml().replace(
            "        provider: openai\n        model: gpt-router",
            "        provider: nowhere\n        model: gpt-router",
        );
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string()
                .contains("agents.router slot `router` targets provider `nowhere`"),
            "{}",
            err
        );
    }

    #[test]
    fn phase1_config_without_superagent_section_still_parses() {
        // The round-1 fixture (no agents/superagent/endpoints keys at all)
        // must parse identically — the additions are all optional.
        let cfg = ServeConfig::parse(&valid_yaml()).expect("phase-1 config");
        assert!(cfg.superagent.is_none());
        assert!(cfg.agents.is_empty());
        assert!(cfg.endpoints.is_empty());
        assert!(!cfg.agent.evidence_gate.enabled);
    }
}
