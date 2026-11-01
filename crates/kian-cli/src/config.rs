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
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, CorrelateBy, Route, RuleMatcher,
    SubscriptionRule, SubscriptionRuleSet,
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
    /// Storage (sqlite default; postgres since C51).
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

/// Storage selection (C51): `event_store: sqlite` (the default — Phase-1
/// behavior, byte-identical) or `event_store: postgres` (the hosted
/// substrate; one server, one database). The sqlite path and the pg URL
/// env are mutually exclusive — a config that names storage the active
/// backend ignores is refused (fail-closed), never silently skipped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// The event-store backend: `sqlite` (default) or `postgres`.
    #[serde(default = "default_event_store")]
    pub event_store: String,
    /// Path of the sqlite database file (required iff `event_store:
    /// sqlite`; must be absent/empty under postgres).
    #[serde(default)]
    pub sqlite: String,
    /// The postgres section (required iff `event_store: postgres`).
    #[serde(default)]
    pub postgres: Option<PostgresStorageConfig>,
}

fn default_event_store() -> String {
    "sqlite".into()
}

impl StorageConfig {
    /// True when the event store is postgres-backed.
    pub fn is_postgres(&self) -> bool {
        self.event_store == "postgres"
    }
}

/// The `storage.postgres` section (C51). The database URL — which may
/// carry a password — is read from the named environment variable at
/// startup: env-only, never a config value (the C13 secrets rule).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStorageConfig {
    /// Environment variable holding the postgres URL (e.g.
    /// `postgres://user:pass@host:5432/kian`). Missing at startup
    /// refuses to start.
    pub url_env: String,
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
// C54: the `superagent.harness` section (the S6 dsh bridge mount)
// ---------------------------------------------------------------------------

/// The `superagent.harness:` section. Every harness is default-off;
/// the built-in worker loop stays the default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HarnessConfig {
    /// The S6 dsh bridge (C54, ADR-010 / DESIGN §16): the worker
    /// template's inner loop becomes the `DshDriver`, driving a
    /// `dsh --profile sdk` child process; dsh keeps its interaction
    /// model, kian owns durability.
    #[serde(default)]
    pub dsh: Option<DshHarnessConfig>,
}

/// The `superagent.harness.dsh:` section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DshHarnessConfig {
    /// Path to the dsh binary. When absent, the `KIAN_DSH_BIN` env var
    /// is used (env-only, like every runtime-resolved path); neither
    /// present refuses to start (fail-closed).
    #[serde(default)]
    pub command: Option<String>,
    /// Child argv. Default `["--profile", "sdk"]` (DESIGN §16).
    #[serde(default = "default_dsh_args")]
    pub args: Vec<String>,
    /// The slot fact stamped into mapped `step/request` envelopes.
    #[serde(default = "default_dsh_slot")]
    pub slot: String,
    /// The provider fact (audit label of the binding).
    #[serde(default = "default_dsh_provider")]
    pub provider: String,
    /// The model fact (audit label of the binding).
    #[serde(default = "default_dsh_model")]
    pub model: String,
    /// Mapped-step budget across the whole session.
    #[serde(default = "default_dsh_max_steps")]
    pub max_steps: u64,
}

fn default_dsh_args() -> Vec<String> {
    kian_dsh::driver::DEFAULT_DSH_ARGS
        .iter()
        .map(|s| (*s).to_owned())
        .collect()
}

fn default_dsh_slot() -> String {
    "main".to_owned()
}

fn default_dsh_provider() -> String {
    "openai-compat".to_owned()
}

fn default_dsh_model() -> String {
    "dsh".to_owned()
}

fn default_dsh_max_steps() -> u64 {
    kian_dsh::driver::DEFAULT_MAX_STEPS
}

impl DshHarnessConfig {
    /// Resolves the runtime driver config, fail-closed: the command
    /// comes from config or `KIAN_DSH_BIN` and nothing else.
    pub fn to_runtime(&self) -> Result<kian_dsh::DshDriverConfig, ConfigError> {
        let command = self
            .command
            .clone()
            .or_else(|| std::env::var("KIAN_DSH_BIN").ok())
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| {
                invalid(
                    "`superagent.harness.dsh` needs a `command` or the \
                     `KIAN_DSH_BIN` env var (fail-closed)",
                )
            })?;
        if self.args.is_empty() {
            return Err(invalid("`superagent.harness.dsh.args` must not be empty"));
        }
        Ok(kian_dsh::DshDriverConfig {
            command,
            args: self.args.clone(),
            map: kian_dsh::MapCtx {
                slot: self.slot.clone(),
                provider: self.provider.clone(),
                model: self.model.clone(),
            },
            max_steps: self.max_steps,
            startup_timeout: kian_dsh::driver::DEFAULT_STARTUP_TIMEOUT,
        })
    }
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
    /// Admission classes (C36, the article §3.1 Fast Path): a
    /// provenance-keyed filter evaluated *before* the rule set and
    /// before any model call. Wire form: `{channel, topic?, sender?,
    /// class: candidate | explicit-only | ignore}`; converted
    /// fail-closed to the bundle's [`AdmissionClassTable`] by
    /// [`SuperAgentConfig::admission_classes`]. Absent = today's
    /// behavior, unchanged.
    #[serde(default)]
    pub classes: Vec<AdmissionClassDef>,
    /// C33 admission-time Inline/Spill shaping. Absent preserves the
    /// pre-C33 infinite cap and byte-identical legacy wrapper.
    #[serde(default)]
    pub shaping: Option<ChannelShapingConfig>,
    /// C43 untrusted-input marking (prompt-injection hygiene):
    /// channels whose events admit with `"untrusted": true` inside
    /// the provenance wrapper's `content`. Honest labeling only —
    /// never a drop/allow decision. Real channels (lark, C44) belong
    /// here; absent = every channel trusted (today's behavior).
    /// Every entry must be declared in `channels:` (fail-closed
    /// cross-reference).
    #[serde(default)]
    pub untrusted_channels: Vec<String>,
    /// Channel name → channel config. Every rule's `channel` must be a
    /// key here (fail-closed cross-reference).
    #[serde(default)]
    pub channels: BTreeMap<String, ChannelConfig>,
    /// Mount the async `start()`/`stop()` tool pair on the Router
    /// profile and the worker template (C37, ADR-008). Default off —
    /// the article's own "not default-on". Requires a non-empty
    /// `endpoints:` list (the pair routes through the C20 queue);
    /// enabling it without endpoints refuses to start (fail-closed).
    #[serde(default)]
    pub async_tools: bool,
    /// C54: the harness section — external agent harnesses the worker
    /// template can run instead of the built-in loop. Default off;
    /// absence keeps the built-in worker loop byte-for-byte.
    #[serde(default)]
    pub harness: HarnessConfig,
    /// The recall toolpack (C35, the Chain of Recall): read-only
    /// `recall_event` / `recall_neighbors` / `recall_thread` tools.
    /// Default: mounted on the Router profile, opt-in per worker
    /// template, with bounded tool-output caps.
    #[serde(default)]
    pub recall: RecallConfig,
    /// Watchdog Session v2 (C40): the resident model-in-the-loop
    /// progress judge. Absent (the default) = no watchdog session, no
    /// change to today's behavior. Present = assembly mounts the
    /// watchdog loop on the fixed session and bootstraps its judge
    /// prompt; ticks reach it through an ordinary subscription rule
    /// (`route: {session: <uuid>}`) over a `type: timer` channel.
    #[serde(default)]
    pub watchdog: Option<WatchdogConfig>,
}

/// The additive `superagent.watchdog:` section (C40): the resident
/// watchdog session binding. The timer input source itself is a
/// channel (`superagent.channels.<name>` with `type: timer`); this
/// section names the judge session and its agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchdogConfig {
    /// The configured fixed watchdog session id (a uuid — the
    /// `Route::Session` target of the timer rule). Bootstrapped by
    /// admitting to this id; must differ from the Router session.
    pub session_id: String,
    /// Id of the watchdog's agent definition in `agents:` (a
    /// worker-template shape: its own strong `main` slot; the
    /// read/intervene tool surface is mounted by assembly).
    pub agent: String,
}

/// The additive `superagent.recall:` section (C35): the recall
/// toolpack's mount gates and caps. `router` defaults on (the Router
/// is the global-observability seat and rides the event stream via
/// recall, not via its own context), `worker` defaults off (opt-in
/// per worker template). The caps bound what one recall tool output
/// may re-inject verbatim — recall reuses the C33 shaping vocabulary
/// for excerpts and association ids, but its verbatim bound is its
/// own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecallConfig {
    /// Mount the toolpack on the Router profile (default true).
    #[serde(default = "default_recall_router")]
    pub router: bool,
    /// Mount the toolpack on the worker template (default false).
    #[serde(default)]
    pub worker: bool,
    /// Maximum serialized bytes of one value surfaced verbatim in a
    /// recall output; above it the output carries the Inline view
    /// (excerpt + continuation). Default 8192.
    #[serde(default = "default_recall_output_cap_bytes")]
    pub output_cap_bytes: usize,
    /// Channel events within this many seq positions on either side
    /// of an anchor (same topic) form the locality view. Default 3.
    #[serde(default = "default_recall_neighbor_window")]
    pub neighbor_window: u64,
}

impl Default for RecallConfig {
    fn default() -> Self {
        Self {
            router: default_recall_router(),
            worker: false,
            output_cap_bytes: default_recall_output_cap_bytes(),
            neighbor_window: default_recall_neighbor_window(),
        }
    }
}

impl RecallConfig {
    /// Converts to the bundle's runtime caps (fail-closed shape only —
    /// the mount booleans are consumed by assembly wiring).
    pub fn to_runtime(&self) -> kian_superagent::RecallToolConfig {
        kian_superagent::RecallToolConfig {
            output_cap_bytes: self.output_cap_bytes,
            neighbor_window: self.neighbor_window,
        }
    }
}

fn default_recall_router() -> bool {
    true
}

fn default_recall_output_cap_bytes() -> usize {
    kian_superagent::DEFAULT_OUTPUT_CAP_BYTES
}

fn default_recall_neighbor_window() -> u64 {
    kian_superagent::DEFAULT_NEIGHBOR_WINDOW
}

/// The additive `superagent.shaping:` section (C33). The cap applies to
/// the serialized raw channel payload at admission, never to committed
/// context. Omitted cap = infinity; omitted excerpt length = 50 chars.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelShapingConfig {
    #[serde(default)]
    pub inline_cap_bytes: Option<usize>,
    #[serde(default = "default_shaping_first_chars")]
    pub first_chars: usize,
    #[serde(default)]
    pub neighbor_fields: Vec<String>,
}

fn default_shaping_first_chars() -> usize {
    50
}

impl ChannelShapingConfig {
    pub fn to_runtime(&self) -> kian_superagent::ShapingConfig {
        kian_superagent::ShapingConfig {
            inline_cap_bytes: self.inline_cap_bytes,
            first_chars: self.first_chars,
            neighbor_fields: self.neighbor_fields.clone(),
        }
    }
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
    /// The Router profile's speculation budget (C39). Absent (the
    /// default) disables speculation: `spawn_worker` with
    /// `speculative: true` is refused with an honest tool error.
    #[serde(default)]
    pub speculation: Option<SpeculationDef>,
}

/// The `superagent.router.speculation` section (C39):
/// `{max_outstanding, label, silent_repro?}` — the Router profile's
/// speculation budget plus the silent-repro prompt template (a config
/// artifact: prose, editable without code).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpeculationDef {
    /// Maximum outstanding (started, unresolved) threads a speculative
    /// spawn is admitted at. Must be ≥ 1 (fail-closed).
    pub max_outstanding: usize,
    /// Budget label, surfaced in the honest cap refusal and the audit.
    /// Must be non-empty (fail-closed).
    pub label: String,
    /// Optional override of the bundle's default silent-repro prompt
    /// template.
    #[serde(default)]
    pub silent_repro: Option<String>,
}

impl SpeculationDef {
    /// Converts to the bundle policy (C39), fail-closed: a zero cap or
    /// an empty label is a config error, never a silent default.
    pub fn to_policy(&self) -> Result<kian_superagent::SpeculationPolicy, ConfigError> {
        if self.max_outstanding == 0 {
            return Err(invalid(
                "superagent.router.speculation.max_outstanding must be > 0",
            ));
        }
        if self.label.trim().is_empty() {
            return Err(invalid(
                "superagent.router.speculation.label must be non-empty",
            ));
        }
        let mut policy =
            kian_superagent::SpeculationPolicy::new(self.max_outstanding, self.label.clone());
        if let Some(silent_repro) = &self.silent_repro {
            if silent_repro.trim().is_empty() {
                return Err(invalid(
                    "superagent.router.speculation.silent_repro must be non-empty when set",
                ));
            }
            policy.silent_repro = silent_repro.clone();
        }
        Ok(policy)
    }
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

/// One channel entry. The map key is the channel name (`mock-im` in
/// the DESIGN §4.1 examples); `type` selects the adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfig {
    #[serde(rename = "type")]
    pub channel_type: String,
    /// The C44 lark section — required when (and only when)
    /// `type: lark` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]).
    #[serde(default)]
    pub lark: Option<LarkChannelSection>,
    /// The C40 timer section — required when (and only when)
    /// `type: timer` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]).
    #[serde(default)]
    pub timer: Option<TimerChannelSection>,
    /// The C48/C49 github section — required when (and only when)
    /// `type: github` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]). Configuring a github channel is what
    /// mounts the `POST /webhooks/github` receiver route.
    #[serde(default)]
    pub github: Option<GitHubChannelSection>,
}

/// The `superagent.channels.<name>.github:` section (C48 adapter +
/// C49 receiver): the topic-mapping allowlist, the events filter and
/// the webhook receiver limits. Credentials are deliberately absent:
/// the send token comes from `KIAN_GITHUB_TOKEN` and the webhook
/// secret from `KIAN_GITHUB_WEBHOOK_SECRET` (assembly, fail-closed) —
/// tokens never live in `kian.yaml` (the C13 secrets discipline).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubChannelSection {
    /// REST API base (api.github.com or a GitHub Enterprise server).
    #[serde(default = "default_github_api_base")]
    pub api_base: String,
    /// Topic mapping table: `owner/repo` → topic, optionally
    /// narrowed per PR with an exact `owner/repo#N` → topic key. The
    /// keys are also the receive allowlist: a delivery from an
    /// unmapped repo is an honest drop (the C48 contract).
    pub topics: BTreeMap<String, String>,
    /// Webhook event-type filter (empty = every supported type).
    #[serde(default)]
    pub events: std::collections::BTreeSet<String>,
    /// Is send configured? Requires `KIAN_GITHUB_TOKEN` at assembly
    /// (fail-closed); receive-only needs none.
    #[serde(default)]
    pub send: bool,
    /// Webhook receiver limits (C49). Defaults are the receiver
    /// module's (`WebhookLimits::default`).
    #[serde(default)]
    pub webhook: GitHubWebhookLimitsSection,
}

fn default_github_api_base() -> String {
    kian_superagent::GITHUB_DEFAULT_API_BASE.to_string()
}

/// The `superagent.channels.<name>.github.webhook:` limits (C49):
/// body cap + per-source token bucket. All fields optional (the
/// manual `Default` mirrors the per-field serde defaults — a derived
/// `Default` would silently zero every limit); zero values refuse to
/// start (a disabled limit is not a limit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubWebhookLimitsSection {
    /// Maximum raw body bytes (413 above).
    #[serde(default = "default_webhook_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Per-source token-bucket capacity.
    #[serde(default = "default_webhook_rate_capacity")]
    pub rate_capacity: u64,
    /// Per-source bucket refill, tokens per second.
    #[serde(default = "default_webhook_rate_refill")]
    pub rate_refill_per_second: u64,
}

impl Default for GitHubWebhookLimitsSection {
    fn default() -> Self {
        Self {
            max_body_bytes: crate::webhook::DEFAULT_MAX_BODY_BYTES,
            rate_capacity: crate::webhook::DEFAULT_RATE_CAPACITY,
            rate_refill_per_second: crate::webhook::DEFAULT_RATE_REFILL_PER_SECOND,
        }
    }
}

fn default_webhook_max_body_bytes() -> usize {
    crate::webhook::DEFAULT_MAX_BODY_BYTES
}
fn default_webhook_rate_capacity() -> u64 {
    crate::webhook::DEFAULT_RATE_CAPACITY
}
fn default_webhook_rate_refill() -> u64 {
    crate::webhook::DEFAULT_RATE_REFILL_PER_SECOND
}

impl GitHubChannelSection {
    /// Converts to the bundle's runtime config, fail-closed on every
    /// degenerate field (the C48 validation). `token` is the env-only
    /// credential filled by assembly (`KIAN_GITHUB_TOKEN`; `send:
    /// true` without it is a startup error, receive-only needs none).
    pub fn to_runtime(
        &self,
        name: &str,
        token: Option<String>,
    ) -> Result<kian_superagent::GitHubConfig, ConfigError> {
        let config = kian_superagent::GitHubConfig {
            api_base: self.api_base.clone(),
            topics: self.topics.clone(),
            events: self.events.clone(),
            // C43: GitHub payloads are attacker-influible text — the
            // channel is untrusted, no opt-out (the lark discipline).
            untrusted: true,
            send: self.send,
            token: token
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        };
        config
            .validate()
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type github): {e}")))?;
        Ok(config)
    }

    /// Validates the pure-config shape at parse time (env credentials
    /// are an assembly concern): topics, events filter and api base
    /// through the bundle's validation with `send` neutralized.
    fn validate_shape(&self, name: &str) -> Result<(), ConfigError> {
        let probe = kian_superagent::GitHubConfig {
            api_base: self.api_base.clone(),
            topics: self.topics.clone(),
            events: self.events.clone(),
            untrusted: true,
            send: false,
            token: None,
        };
        probe
            .validate()
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type github): {e}")))?;
        Ok(())
    }

    /// The C49 receiver limits (defaults where unset).
    pub fn webhook_limits(&self) -> crate::webhook::WebhookLimits {
        crate::webhook::WebhookLimits {
            max_body_bytes: self.webhook.max_body_bytes,
            rate_capacity: self.webhook.rate_capacity,
            rate_refill_per_second: self.webhook.rate_refill_per_second,
        }
    }
}

/// The `superagent.channels.<name>.timer:` section (C40): the timer
/// input source's cadence. The adapter emits
/// `ChannelEvent{channel:<name>, topic, event_id:"tick-<n>",
/// payload:{"tick":n}}` every `interval_ms`; `emit_on_start` fires one
/// immediate tick after startup (an immediate first progress check
/// after a restart).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerChannelSection {
    /// Milliseconds between ticks (must be > 0, fail-closed).
    pub interval_ms: u64,
    /// One immediate tick at startup. Default false.
    #[serde(default)]
    pub emit_on_start: bool,
    /// The routing topic every tick carries (subscription rules match
    /// on it; the watchdog convention is `watchdog`).
    #[serde(default = "default_timer_topic")]
    pub topic: String,
}

fn default_timer_topic() -> String {
    kian_superagent::DEFAULT_TIMER_TOPIC.to_string()
}

impl TimerChannelSection {
    /// Converts to the bundle's runtime config, fail-closed on the
    /// cadence: a zero interval or an empty topic is a config error,
    /// never a silent default.
    pub fn to_runtime(&self, name: &str) -> Result<kian_superagent::TimerConfig, ConfigError> {
        if self.interval_ms == 0 {
            return Err(invalid(format!(
                "superagent.channels `{name}` (type timer).timer.interval_ms must be > 0"
            )));
        }
        if self.topic.trim().is_empty() {
            return Err(invalid(format!(
                "superagent.channels `{name}` (type timer).timer.topic must be non-empty"
            )));
        }
        Ok(kian_superagent::TimerConfig {
            interval_ms: self.interval_ms,
            topic: self.topic.clone(),
            emit_on_start: self.emit_on_start,
        })
    }
}

/// The `superagent.channels.<name>.lark:` section (C44). Credentials
/// are deliberately absent: `app_id`/`app_secret` come from the
/// environment (`KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET`) at assembly
/// — tokens never live in `kian.yaml` (the C13 secrets discipline).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LarkChannelSection {
    /// Feishu open-apis base (overridable for private deployments).
    #[serde(default = "default_lark_api_base")]
    pub api_base: String,
    /// Receive poll cadence, milliseconds (must be > 0).
    #[serde(default = "default_lark_poll_ms")]
    pub poll_interval_ms: u64,
    /// chat id → routing topic. The keys are exactly the chats the
    /// adapter polls; at least one entry is required (fail-closed).
    pub topics: BTreeMap<String, String>,
}

fn default_lark_api_base() -> String {
    kian_superagent::DEFAULT_API_BASE.to_string()
}

fn default_lark_poll_ms() -> u64 {
    kian_superagent::DEFAULT_POLL_INTERVAL.as_millis() as u64
}

impl LarkChannelSection {
    /// Converts to the bundle's runtime config, fail-closed on
    /// credentials: absent or blank `KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET`
    /// is a startup error naming the env vars — never a silent default
    /// and never a config-file token (C13/C44).
    pub fn to_runtime(
        &self,
        name: &str,
        app_id: Option<String>,
        app_secret: Option<String>,
    ) -> Result<kian_superagent::LarkConfig, ConfigError> {
        let missing = |what: &str| {
            invalid(format!(
                "superagent.channels `{name}` (type lark) requires {what} in the environment \
                 (KIAN_LARK_APP_ID / KIAN_LARK_APP_SECRET) — lark credentials never live in \
                 kian.yaml"
            ))
        };
        let app_id = app_id
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| missing("KIAN_LARK_APP_ID"))?;
        let app_secret = app_secret
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| missing("KIAN_LARK_APP_SECRET"))?;
        let config = kian_superagent::LarkConfig {
            app_id,
            app_secret,
            api_base: self.api_base.clone(),
            topics: self.topics.clone(),
            poll_interval: std::time::Duration::from_millis(self.poll_interval_ms),
        };
        config
            .validate()
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type lark): {e}")))?;
        Ok(config)
    }
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

/// Wire form of a rule's route: the bare keyword `router`,
/// `{passthrough: {correlate_by: topic}}`, or — C40, additive —
/// `{session: <uuid>}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RouteDef {
    Keyword(String),
    Passthrough { passthrough: CorrelateByWire },
    Session { session: String },
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

/// One admission-class entry (C36 wire form): a provenance key
/// `(channel, topic?, sender?)` → `class`. `topic`/`sender` are
/// optional pins; the class keyword is validated fail-closed in
/// [`SuperAgentConfig::admission_classes`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionClassDef {
    pub channel: String,
    #[serde(default)]
    pub topic: Option<String>,
    /// Adapter-declared sender provenance (any JSON value, compared by
    /// equality at resolution).
    #[serde(default)]
    pub sender: Option<serde_json::Value>,
    /// `candidate | explicit-only | ignore` (validated fail-closed).
    pub class: String,
}

impl SuperAgentConfig {
    /// Converts the wire-form admission classes into the bundle's
    /// [`AdmissionClassTable`], fail-closed: unknown class keywords
    /// and empty channel/topic strings are errors, never defaults.
    pub fn admission_classes(&self) -> Result<AdmissionClassTable, ConfigError> {
        let mut rules = Vec::with_capacity(self.classes.len());
        for (i, def) in self.classes.iter().enumerate() {
            if def.channel.is_empty() {
                return Err(invalid(format!(
                    "superagent.classes[{i}].channel must not be empty"
                )));
            }
            if def.topic.as_deref().is_some_and(str::is_empty) {
                return Err(invalid(format!(
                    "superagent.classes[{i}].topic must not be empty"
                )));
            }
            let class = match def.class.as_str() {
                "candidate" => AdmissionClass::Candidate,
                "explicit-only" => AdmissionClass::ExplicitOnly,
                "ignore" => AdmissionClass::Ignore,
                other => {
                    return Err(invalid(format!(
                        "superagent.classes[{i}].class `{other}` is not one of \
                         candidate | explicit-only | ignore"
                    )));
                }
            };
            rules.push(AdmissionClassRule {
                channel: def.channel.clone(),
                topic: def.topic.clone(),
                sender: def.sender.clone(),
                class,
            });
        }
        Ok(AdmissionClassTable::new(rules))
    }

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
                // C40: a config-addressed fixed session (the timer's
                // ticks → the watchdog). Fail-closed on the uuid.
                RouteDef::Session { session } => Route::Session(kian_core::model::SessionId(
                    uuid::Uuid::parse_str(session).map_err(|_| {
                        invalid(format!(
                            "superagent.subscriptions[{i}].route.session `{session}` is not a uuid"
                        ))
                    })?,
                )),
                other => {
                    return Err(invalid(format!(
                        "superagent.subscriptions[{i}].route `{other:?}` is not one of \
                         router | {{passthrough: {{correlate_by: topic}}}} | {{session: uuid}}"
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

    /// Converts the C43 `untrusted_channels` list into the runtime
    /// set, fail-closed: empty entries, duplicates and channels not
    /// declared in `channels` are errors, never defaults.
    pub fn untrusted_channel_set(&self) -> Result<std::collections::BTreeSet<String>, ConfigError> {
        let mut set = std::collections::BTreeSet::new();
        for (i, channel) in self.untrusted_channels.iter().enumerate() {
            if channel.trim().is_empty() {
                return Err(invalid(format!(
                    "superagent.untrusted_channels[{i}] must not be empty"
                )));
            }
            if !self.channels.contains_key(channel) {
                return Err(invalid(format!(
                    "superagent.untrusted_channels[{i}] references channel `{channel}` which is \
                     not declared in superagent.channels"
                )));
            }
            if !set.insert(channel.clone()) {
                return Err(invalid(format!(
                    "superagent.untrusted_channels[{i}] duplicates channel `{channel}`"
                )));
            }
        }
        Ok(set)
    }

    /// The runtime untrusted set (C43 + C44 + C48): the configured
    /// `untrusted_channels` list **plus every lark- and github-typed
    /// channel** — real inbound text is attacker-controllable, so a
    /// real channel is untrusted by default (listing it again is
    /// harmless; opting out is deliberately not offered — trust must
    /// be a considered change, not a missing default).
    pub fn runtime_untrusted_set(&self) -> Result<std::collections::BTreeSet<String>, ConfigError> {
        let mut set = self.untrusted_channel_set()?;
        for (name, channel) in &self.channels {
            if channel.channel_type == kian_superagent::LARK_CHANNEL_TYPE
                || channel.channel_type == kian_superagent::GITHUB_CHANNEL_TYPE
            {
                set.insert(name.clone());
            }
        }
        Ok(set)
    }
}

/// Every channel adapter type `serve` knows how to build (C27 v1:
/// the first-class mock; C44 adds the real lark adapter; C40 the
/// timer input source; C48/C49 the github channel + webhook
/// receiver).
pub const KNOWN_CHANNEL_TYPES: [&str; 4] = ["mock-im", "lark", "timer", "github"];

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
        // Storage (C51): the backend picks its required section, and any
        // storage config the active backend would IGNORE is refused —
        // fail-closed, never a silently skipped path or credential.
        match self.storage.event_store.as_str() {
            "sqlite" => {
                if self.storage.sqlite.trim().is_empty() {
                    return Err(invalid(
                        "storage.sqlite (the database path) is required when storage.event_store is sqlite",
                    ));
                }
                if self.storage.postgres.is_some() {
                    return Err(invalid(
                        "storage.postgres is set but storage.event_store is sqlite — one server, one database; remove the unused section",
                    ));
                }
            }
            "postgres" => {
                if !self.storage.sqlite.trim().is_empty() {
                    return Err(invalid(
                        "storage.sqlite is set but storage.event_store is postgres — one server, one database; remove the unused path",
                    ));
                }
                let pg = self.storage.postgres.as_ref().ok_or_else(|| {
                    invalid(
                        "storage.event_store: postgres requires a storage.postgres section with url_env (env-only URLs, the C13 secrets rule)",
                    )
                })?;
                if pg.url_env.trim().is_empty() {
                    return Err(invalid(
                        "storage.postgres.url_env must name the environment variable holding the database URL",
                    ));
                }
                // C47 condition (c) depends on this: two replicas sharing
                // a holder string would defeat same-holder claim
                // admission, so the default holder refuses to start.
                if self.drain.holder.trim().is_empty() {
                    return Err(invalid(
                        "storage.event_store: postgres requires an explicit drain.holder (an empty holder cannot claim)",
                    ));
                }
                if self.drain.holder == default_holder() {
                    return Err(invalid(format!(
                        "storage.event_store: postgres requires an explicit unique drain.holder — the default `{}` is refused: two replicas sharing a holder would defeat same-holder claim admission (C47/ADR-009)",
                        default_holder()
                    )));
                }
            }
            other => {
                return Err(invalid(format!(
                    "storage.event_store must be `sqlite` or `postgres`, got `{other}`"
                )));
            }
        }

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
        // C37: the async pair routes through the C20 queue — gating it
        // on without endpoints would mount tools with nowhere to run.
        if self.superagent.as_ref().is_some_and(|sa| sa.async_tools) && self.endpoints.is_empty() {
            return Err(invalid(
                "`superagent.async_tools` requires a non-empty `endpoints:` list \
                 (the async start/stop pair routes through the remote-exec queue)",
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
                // C44: the `lark:` section is presence-locked to
                // `type: lark` in both directions, and its contents
                // validate fail-closed (a lark channel is unusable
                // without a real topic map and a positive cadence).
                match (&channel.lark, channel.channel_type.as_str()) {
                    (Some(section), "lark") => {
                        if section.topics.is_empty() {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.lark.topics must declare at \
                                 least one chat id → topic entry"
                            )));
                        }
                        for (chat_id, topic) in &section.topics {
                            if chat_id.trim().is_empty() || topic.trim().is_empty() {
                                return Err(invalid(format!(
                                    "superagent.channels `{name}`.lark.topics entries must be \
                                     non-empty (`{chat_id}`)"
                                )));
                            }
                        }
                        if section.poll_interval_ms == 0 {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.lark.poll_interval_ms must be > 0"
                            )));
                        }
                    }
                    (Some(_), other) => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}`: the `lark:` section belongs to \
                             `type: lark` channels only (this channel is `type: {other}`)"
                        )));
                    }
                    (None, "lark") => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}` (type lark) requires a `lark:` section \
                             (api_base?, poll_interval_ms?, topics)"
                        )));
                    }
                    (None, _) => {}
                }
                // C40: the `timer:` section is presence-locked to
                // `type: timer` in both directions; its cadence
                // validates fail-closed.
                match (&channel.timer, channel.channel_type.as_str()) {
                    (Some(section), "timer") => {
                        section.to_runtime(name)?;
                    }
                    (Some(_), other) => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}`: the `timer:` section belongs to \
                             `type: timer` channels only (this channel is `type: {other}`)"
                        )));
                    }
                    (None, "timer") => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}` (type timer) requires a `timer:` section \
                             (interval_ms, emit_on_start?, topic?)"
                        )));
                    }
                    (None, _) => {}
                }
                // C48/C49: the `github:` section is presence-locked
                // to `type: github` in both directions; its pure
                // shape validates fail-closed, and the webhook
                // limits must be real limits (zero refuses).
                match (&channel.github, channel.channel_type.as_str()) {
                    (Some(section), "github") => {
                        section.validate_shape(name)?;
                        let limits = section.webhook_limits();
                        if limits.max_body_bytes == 0 {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.github.webhook.max_body_bytes \
                                 must be > 0 (a disabled cap is not a cap)"
                            )));
                        }
                        if limits.rate_capacity == 0 || limits.rate_refill_per_second == 0 {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.github.webhook.rate_capacity and \
                                 rate_refill_per_second must be > 0 (a disabled limit is not a \
                                 limit)"
                            )));
                        }
                    }
                    (Some(_), other) => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}`: the `github:` section belongs to \
                             `type: github` channels only (this channel is `type: {other}`)"
                        )));
                    }
                    (None, "github") => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}` (type github) requires a `github:` \
                             section (api_base?, topics, events?, send?, webhook?)"
                        )));
                    }
                    (None, _) => {}
                }
            }
            // C49: at most one `type: github` channel — the single
            // `POST /webhooks/github` route feeds exactly one channel
            // (two channels would make delivery routing ambiguous).
            let github_count = sa
                .channels
                .values()
                .filter(|channel| channel.channel_type == kian_superagent::GITHUB_CHANNEL_TYPE)
                .count();
            if github_count > 1 {
                return Err(invalid(
                    "superagent.channels may declare at most one `type: github` channel (the \
                     single POST /webhooks/github route feeds exactly one)",
                ));
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
            // The speculation section must convert cleanly (C39:
            // fail-closed — a zero cap or empty label refuses to start).
            if let Some(speculation) = &sa.router.speculation {
                speculation.to_policy()?;
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
            // C36: classes cross-reference declared channels and convert
            // cleanly (unknown class keywords refuse to start).
            for (i, class) in sa.classes.iter().enumerate() {
                if !sa.channels.contains_key(&class.channel) {
                    return Err(invalid(format!(
                        "superagent.classes[{i}] references channel `{}` which is not \
                         declared in superagent.channels",
                        class.channel
                    )));
                }
            }
            sa.admission_classes()?;
            // C43: the untrusted-channel list cross-references the
            // declared channels and converts cleanly (fail-closed).
            sa.untrusted_channel_set()?;
            // C40: the watchdog binding cross-references convert
            // cleanly — a fixed session uuid distinct from the Router
            // session, an agent that exists and carries a strong
            // `main` slot (the worker-template shape).
            if let Some(watchdog) = &sa.watchdog {
                let session = uuid::Uuid::parse_str(&watchdog.session_id).map_err(|_| {
                    invalid(format!(
                        "superagent.watchdog.session_id `{}` is not a uuid",
                        watchdog.session_id
                    ))
                })?;
                if session.to_string() == sa.router.session_id {
                    return Err(invalid(
                        "superagent.watchdog.session_id must differ from \
                         superagent.router.session_id (the watchdog judges the Router's \
                         threads — it cannot be the Router)",
                    ));
                }
                let watchdog_agent = self.agents.get(&watchdog.agent).ok_or_else(|| {
                    invalid(format!(
                        "superagent.watchdog.agent `{}` is not defined in agents",
                        watchdog.agent
                    ))
                })?;
                if !watchdog_agent.model_slots.contains_key("main") {
                    return Err(invalid(format!(
                        "superagent.watchdog.agent `{}` has no `main` model slot (the \
                         watchdog is worker-template shaped: one strong main slot)",
                        watchdog.agent
                    )));
                }
            }
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
  tools: ["fs_read", "fs_write", "exec"]
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

    /// C51 golden fixture: a config with NO `event_store:` key parses to
    /// exactly today's sqlite storage — default backend, path read
    /// verbatim, no postgres section — and serializes back to today's
    /// bytes. The sqlite default is byte-identical, not merely similar.
    #[test]
    fn sqlite_default_config_parses_byte_identically_to_today() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert_eq!(
            cfg.storage,
            super::StorageConfig {
                event_store: "sqlite".into(),
                sqlite: "/tmp/kian-test.sqlite".into(),
                postgres: None,
            }
        );
        assert!(!cfg.storage.is_postgres());
        assert_eq!(
            serde_yaml::to_string(&cfg.storage).unwrap(),
            "event_store: sqlite\nsqlite: /tmp/kian-test.sqlite\npostgres: null\n"
        );
    }

    /// C51: sqlite mode without a path refuses (the pre-C51 required
    /// field's behavior, now enforced at validation with a clearer
    /// message).
    #[test]
    fn sqlite_mode_requires_the_sqlite_path() {
        let yaml = valid_yaml().replace(
            "storage:\n  sqlite: \"/tmp/kian-test.sqlite\"\n",
            "storage:\n  event_store: sqlite\n",
        );
        let err = ServeConfig::parse(&yaml).expect_err("missing path must refuse");
        assert!(
            err.to_string().contains("storage.sqlite"),
            "{}",
            err.to_string()
        );
    }

    /// C51: an unknown backend name refuses, never falls back.
    #[test]
    fn unknown_event_store_backend_refuses() {
        let yaml = valid_yaml().replace("storage:\n", "storage:\n  event_store: mysql\n");
        let err = ServeConfig::parse(&yaml).expect_err("unknown backend must refuse");
        assert!(err.to_string().contains("mysql"), "{}", err.to_string());
    }

    /// C51: postgres mode without the `storage.postgres` section refuses.
    #[test]
    fn postgres_mode_requires_the_postgres_section() {
        let yaml = valid_yaml().replace(
            "storage:\n  sqlite: \"/tmp/kian-test.sqlite\"\n",
            "storage:\n  event_store: postgres\n",
        );
        let err = ServeConfig::parse(&yaml).expect_err("missing section must refuse");
        assert!(
            err.to_string().contains("storage.postgres"),
            "{}",
            err.to_string()
        );
    }

    /// C51: postgres mode parses with an explicit holder and an env-only
    /// URL name; the URL itself never appears in the config.
    #[test]
    fn postgres_mode_parses_with_explicit_holder() {
        let yaml = valid_yaml()
            .replace(
                "storage:\n  sqlite: \"/tmp/kian-test.sqlite\"\n",
                "storage:\n  event_store: postgres\n  postgres:\n    url_env: KIAN_PG_URL\n",
            )
            .replace(
                "drain:\n  max_iterations: 8\n",
                "drain:\n  holder: replica-a\n  max_iterations: 8\n",
            );
        let cfg = ServeConfig::parse(&yaml).expect("valid postgres config");
        assert!(cfg.storage.is_postgres());
        assert_eq!(
            cfg.storage.postgres.as_ref().unwrap().url_env,
            "KIAN_PG_URL"
        );
        assert_eq!(cfg.drain.holder, "replica-a");
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
  tools: ["fs_read"]
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
    tools: ["fs_read", "fs_write", "exec"]
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

    // -------------------------------------------------------------------
    // C40: watchdog + timer configuration
    // -------------------------------------------------------------------

    const WATCHDOG_SESSION: &str = "99999999-8888-7777-6666-555555555555";

    /// The golden profile plus the C40 surface: a watchdog agent and
    /// binding, a `type: timer` channel, and a session-routed rule.
    fn watchdog_yaml() -> String {
        superagent_yaml()
            .replacen(
                "agents:\n",
                "agents:\n  watchdog:\n    id: watchdog\n    version: 1\n    model_slots:\n      main:\n        provider: openai\n        model: gpt-watchdog\n    tools: []\n",
                1,
            )
            .replacen(
                "superagent:\n",
                format!(
                    "superagent:\n  watchdog:\n    session_id: \"{WATCHDOG_SESSION}\"\n    agent: watchdog\n"
                )
                .as_str(),
                1,
            )
            .replacen(
                "  subscriptions:",
                // The timer rule routes the watchdog topic to the fixed
                // session (interpolated — this arm needs the uuid).
                format!(
                    "  subscriptions:\n    - channel: timer\n      match:\n        topic: watchdog\n      route:\n        session: \"{WATCHDOG_SESSION}\""
                )
                .as_str(),
                1,
            )
            .replacen(
                "  channels:",
                "  channels:\n    timer:\n      type: timer\n      timer:\n        interval_ms: 60000\n        emit_on_start: true",
                1,
            )
    }

    #[test]
    fn watchdog_and_timer_config_parse_and_convert() {
        let cfg = ServeConfig::parse(&watchdog_yaml()).expect("watchdog config parses");
        let sa = cfg.superagent.as_ref().expect("superagent section");

        // The watchdog binding.
        let watchdog = sa.watchdog.as_ref().expect("watchdog binding");
        assert_eq!(watchdog.session_id, WATCHDOG_SESSION);
        assert_eq!(watchdog.agent, "watchdog");

        // The timer channel: type + section, with defaults filled.
        let timer = sa.channels.get("timer").expect("timer channel");
        assert_eq!(timer.channel_type, "timer");
        let section = timer.timer.as_ref().expect("timer section");
        assert_eq!(section.interval_ms, 60_000);
        assert!(section.emit_on_start);
        assert_eq!(
            section.topic,
            kian_superagent::DEFAULT_TIMER_TOPIC,
            "topic defaults to `watchdog`"
        );
        let runtime = section.to_runtime("timer").expect("converts");
        assert_eq!(runtime.interval_ms, 60_000);
        assert!(runtime.emit_on_start);
        assert_eq!(runtime.topic, "watchdog");

        // The session-routed rule converts to Route::Session.
        let rules = sa.subscription_rules().expect("rules convert");
        let session_rule = rules
            .0
            .iter()
            .find(|r| r.channel == "timer")
            .expect("the timer rule");
        assert_eq!(
            session_rule.route,
            kian_superagent::Route::Session(kian_core::model::SessionId(
                uuid::Uuid::parse_str(WATCHDOG_SESSION).unwrap()
            ))
        );

        // The golden profile without the additions still parses
        // (additive: absence is today's behavior).
        let plain = ServeConfig::parse(&superagent_yaml()).expect("plain golden parses");
        assert!(plain.superagent.as_ref().unwrap().watchdog.is_none());
    }

    #[test]
    fn timer_channel_section_is_fail_closed() {
        // Zero interval refuses to start.
        let zero = watchdog_yaml().replace("interval_ms: 60000", "interval_ms: 0");
        let err = ServeConfig::parse(&zero).unwrap_err();
        assert!(err.to_string().contains("interval_ms must be > 0"), "{err}");

        // A timer channel without its section refuses to start.
        let missing = watchdog_yaml().replacen(
            "    timer:\n      type: timer\n      timer:\n        interval_ms: 60000\n        emit_on_start: true",
            "    timer:\n      type: timer",
            1,
        );
        let err = ServeConfig::parse(&missing).unwrap_err();
        assert!(
            err.to_string().contains("requires a `timer:` section"),
            "{err}"
        );

        // The section on a non-timer channel refuses to start.
        let misapplied = watchdog_yaml().replacen(
            "    mock-im:\n      type: mock-im",
            "    mock-im:\n      type: mock-im\n      timer:\n        interval_ms: 1000",
            1,
        );
        let err = ServeConfig::parse(&misapplied).unwrap_err();
        assert!(
            err.to_string()
                .contains("belongs to `type: timer` channels only"),
            "{err}"
        );
    }

    #[test]
    fn watchdog_binding_is_fail_closed() {
        // A malformed session uuid (the binding's occurrence only —
        // the route references the same literal later in the file).
        let bad_uuid = watchdog_yaml().replacen(WATCHDOG_SESSION, "not-a-uuid", 1);
        let err = ServeConfig::parse(&bad_uuid).unwrap_err();
        assert!(
            err.to_string()
                .contains("session_id `not-a-uuid` is not a uuid"),
            "{err}"
        );

        // The watchdog session must differ from the Router session.
        let same = watchdog_yaml().replace(WATCHDOG_SESSION, ROUTER_SESSION);
        let err = ServeConfig::parse(&same).unwrap_err();
        assert!(
            err.to_string()
                .contains("must differ from superagent.router.session_id"),
            "{err}"
        );

        // An unknown agent reference.
        let unknown_agent = watchdog_yaml().replace("agent: watchdog", "agent: ghost");
        let err = ServeConfig::parse(&unknown_agent).unwrap_err();
        assert!(
            err.to_string().contains("`ghost` is not defined in agents"),
            "{err}"
        );

        // An agent without a main slot (router-shaped, not worker-shaped).
        let no_main = watchdog_yaml().replacen(
            "  watchdog:\n    id: watchdog\n    version: 1\n    model_slots:\n      main:",
            "  watchdog:\n    id: watchdog\n    version: 1\n    model_slots:\n      judge:",
            1,
        );
        let err = ServeConfig::parse(&no_main).unwrap_err();
        assert!(
            err.to_string().contains("has no `main` model slot"),
            "{err}"
        );
    }

    #[test]
    fn session_route_with_a_bad_uuid_is_rejected() {
        let bad = watchdog_yaml().replacen(
            &format!("        session: \"{WATCHDOG_SESSION}\""),
            "        session: \"nope\"",
            1,
        );
        let err = ServeConfig::parse(&bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("route.session `nope` is not a uuid"),
            "{err}"
        );
    }

    #[test]
    fn superagent_shaping_is_additive_and_defaults_to_legacy_behavior() {
        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  shaping:\n    inline_cap_bytes: 2048\n    first_chars: 80\n    neighbor_fields: [reply_to, thread_id]\n  channels:",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("shaping config parses");
        let shaping = cfg.superagent.unwrap().shaping.unwrap();
        assert_eq!(shaping.inline_cap_bytes, Some(2048));
        assert_eq!(shaping.first_chars, 80);
        assert_eq!(shaping.neighbor_fields, ["reply_to", "thread_id"]);

        let legacy = ServeConfig::parse(&superagent_yaml()).expect("legacy config parses");
        assert!(legacy.superagent.unwrap().shaping.is_none());

        let defaults = ServeConfig::parse(&superagent_yaml().replacen(
            "  channels:",
            "  shaping: {}\n  channels:",
            1,
        ))
        .expect("defaulted shaping parses");
        let shaping = defaults.superagent.unwrap().shaping.unwrap();
        assert_eq!(shaping.inline_cap_bytes, None);
        assert_eq!(shaping.first_chars, 50);
        assert!(shaping.neighbor_fields.is_empty());
    }

    /// C35: the `superagent.recall:` section is additive — a legacy
    /// config parses with the toolpack default-on for the Router and
    /// off for workers (the Default impl agrees with the serde
    /// defaults), and an explicit section converts to the bundle caps.
    #[test]
    fn superagent_recall_section_is_additive_with_router_default_on() {
        // Legacy (no recall section): defaults, not absence.
        let legacy = ServeConfig::parse(&superagent_yaml()).expect("legacy config parses");
        let recall = legacy.superagent.unwrap().recall;
        assert_eq!(recall, super::RecallConfig::default());
        assert!(recall.router, "the Router mounts recall by default");
        assert!(!recall.worker, "worker templates opt in");
        assert_eq!(
            recall.output_cap_bytes,
            kian_superagent::DEFAULT_OUTPUT_CAP_BYTES
        );
        assert_eq!(
            recall.neighbor_window,
            kian_superagent::DEFAULT_NEIGHBOR_WINDOW
        );

        // Explicit section: worker opt-in plus custom caps.
        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  recall:\n    worker: true\n    output_cap_bytes: 512\n    neighbor_window: 7\n  channels:",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("recall config parses");
        let recall = cfg.superagent.unwrap().recall;
        assert!(recall.router);
        assert!(recall.worker);
        let runtime = recall.to_runtime();
        assert_eq!(runtime.output_cap_bytes, 512);
        assert_eq!(runtime.neighbor_window, 7);

        // Router opt-out is explicit and parseable.
        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  recall:\n    router: false\n  channels:",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("router opt-out parses");
        assert!(!cfg.superagent.unwrap().recall.router);

        // Unknown keys still refuse (fail-closed).
        let bad = superagent_yaml().replacen(
            "  channels:",
            "  recall:\n    neighbor_windowx: 3\n  channels:",
            1,
        );
        assert!(ServeConfig::parse(&bad).is_err());
    }

    #[test]
    fn superagent_shaping_rejects_unknown_fields() {
        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  shaping:\n    inline_cap_byte: 1024\n  channels:",
            1,
        );
        let error = ServeConfig::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("unknown field"), "unexpected error: {error}");
        assert!(error.contains("inline_cap_byte"));
    }

    /// C43: the untrusted-channel list parses, defaults to empty
    /// (every channel trusted = today's behavior), and converts to
    /// the runtime set.
    #[test]
    fn superagent_untrusted_channels_parse_and_convert() {
        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  untrusted_channels: [mock-im, ci]\n  channels:",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("untrusted channels parse");
        let sa = cfg.superagent.unwrap();
        assert_eq!(sa.untrusted_channels, ["mock-im", "ci"]);
        assert_eq!(
            sa.untrusted_channel_set().unwrap(),
            std::collections::BTreeSet::from(["ci".to_string(), "mock-im".to_string()])
        );

        // Absent = empty = every channel trusted.
        let legacy = ServeConfig::parse(&superagent_yaml()).unwrap();
        let sa = legacy.superagent.unwrap();
        assert!(sa.untrusted_channels.is_empty());
        assert!(sa.untrusted_channel_set().unwrap().is_empty());
    }

    /// C43 fail-closed: an entry naming an undeclared channel refuses
    /// to start, as do empty entries and duplicates.
    #[test]
    fn superagent_untrusted_channels_cross_reference_declared_channels() {
        let undeclared = superagent_yaml().replacen(
            "  channels:",
            "  untrusted_channels: [lark]\n  channels:",
            1,
        );
        let error = ServeConfig::parse(&undeclared).unwrap_err().to_string();
        assert!(
            error.contains("untrusted_channels[0] references channel `lark`"),
            "unexpected error: {error}"
        );

        let duplicate = superagent_yaml().replacen(
            "  channels:",
            "  untrusted_channels: [mock-im, mock-im]\n  channels:",
            1,
        );
        let error = ServeConfig::parse(&duplicate).unwrap_err().to_string();
        assert!(
            error.contains("duplicates channel `mock-im`"),
            "unexpected error: {error}"
        );

        let empty = superagent_yaml().replacen(
            "  channels:",
            "  untrusted_channels: [\"\"]\n  channels:",
            1,
        );
        assert!(ServeConfig::parse(&empty).is_err());
    }

    // -----------------------------------------------------------------------
    // C44: the lark channel wire layer
    // -----------------------------------------------------------------------

    /// A lark channel declaration spliced into the golden fixture (the
    /// section carries no credentials — they are env-only).
    fn lark_yaml(extra: &str) -> String {
        superagent_yaml().replacen(
            "  channels:\n    mock-im:",
            &format!("  channels:\n    lark:\n      type: lark\n{extra}    mock-im:"),
            1,
        )
    }

    #[test]
    fn lark_channel_config_parses_with_defaults() {
        let yaml = lark_yaml(
            "      lark:\n        topics:\n          oc_chat1: ci\n          oc_chat2: ops\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("lark config parses");
        let sa = cfg.superagent.as_ref().unwrap();
        let lark = sa.channels["lark"].lark.as_ref().expect("lark section");
        assert_eq!(lark.api_base, "https://open.feishu.cn");
        assert_eq!(lark.poll_interval_ms, 5_000);
        assert_eq!(
            lark.topics,
            BTreeMap::from([
                ("oc_chat1".to_string(), "ci".to_string()),
                ("oc_chat2".to_string(), "ops".to_string())
            ])
        );

        // Explicit overrides parse.
        let yaml = lark_yaml(
            "      lark:\n        api_base: \"https://feishu.private\"\n        poll_interval_ms: 500\n        topics:\n          oc_chat1: ci\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("lark overrides parse");
        let lark = cfg.superagent.as_ref().unwrap().channels["lark"]
            .lark
            .as_ref()
            .unwrap();
        assert_eq!(lark.api_base, "https://feishu.private");
        assert_eq!(lark.poll_interval_ms, 500);
    }

    #[test]
    fn lark_channel_config_fails_closed_both_directions() {
        // `type: lark` without a `lark:` section.
        let error = ServeConfig::parse(&lark_yaml("")).unwrap_err().to_string();
        assert!(
            error.contains("requires a `lark:` section"),
            "unexpected error: {error}"
        );

        // A `lark:` section on a non-lark channel.
        let misplaced = superagent_yaml().replacen(
            "    mock-im:\n      type: mock-im",
            "    mock-im:\n      type: mock-im\n      lark:\n        topics:\n          oc_chat1: ci",
            1,
        );
        let error = ServeConfig::parse(&misplaced).unwrap_err().to_string();
        assert!(
            error.contains("belongs to `type: lark` channels only"),
            "unexpected error: {error}"
        );

        // Empty topic table / empty entries / zero cadence.
        for (extra, needle) in [
            ("      lark:\n        topics: {}\n", "at least one chat id"),
            (
                "      lark:\n        topics:\n          \"\": ci\n",
                "must be non-empty",
            ),
            (
                "      lark:\n        poll_interval_ms: 0\n        topics:\n          oc_chat1: ci\n",
                "poll_interval_ms must be > 0",
            ),
        ] {
            let error = ServeConfig::parse(&lark_yaml(extra))
                .unwrap_err()
                .to_string();
            assert!(error.contains(needle), "unexpected error: {error}");
        }
    }

    #[test]
    fn lark_credentials_fail_closed_at_startup() {
        let cfg = ServeConfig::parse(&lark_yaml(
            "      lark:\n        topics:\n          oc_chat1: ci\n",
        ))
        .expect("config parses (credentials are checked at conversion)");
        let sa = cfg.superagent.as_ref().unwrap();
        let section = sa.channels["lark"].lark.as_ref().unwrap();

        // Absent credentials → a startup error naming the env vars.
        let error = section
            .to_runtime("lark", None, None)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("KIAN_LARK_APP_ID") && error.contains("never live in kian.yaml"),
            "unexpected error: {error}"
        );
        // Blank app id is a missing credential; same for the secret.
        let error = section
            .to_runtime("lark", Some("  ".into()), Some("s".into()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("KIAN_LARK_APP_ID"),
            "unexpected error: {error}"
        );
        let error = section
            .to_runtime("lark", Some("cli_x".into()), Some("".into()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("KIAN_LARK_APP_SECRET"),
            "unexpected error: {error}"
        );

        // Present credentials convert to a validated runtime config.
        let runtime = section
            .to_runtime("lark", Some("cli_x".into()), Some("sec".into()))
            .expect("credentials build");
        assert_eq!(runtime.app_id, "cli_x");
        assert_eq!(runtime.topics["oc_chat1"], "ci");
        assert_eq!(runtime.poll_interval.as_millis(), 5_000);
    }

    #[test]
    fn lark_channels_are_untrusted_by_default() {
        let yaml = lark_yaml("      lark:\n        topics:\n          oc_chat1: ci\n");
        let cfg = ServeConfig::parse(&yaml).expect("lark config parses");
        let sa = cfg.superagent.as_ref().unwrap();

        // Not listed in untrusted_channels…
        assert!(sa.untrusted_channels.is_empty());
        // …but the runtime set (what assembly passes to the bundle)
        // includes the lark-typed channel, and only it.
        let set = sa.runtime_untrusted_set().expect("runtime set converts");
        assert_eq!(
            set,
            std::collections::BTreeSet::from(["lark".to_string()]),
            "real channels are untrusted by default (C43+C44)"
        );

        // Without any lark channel the runtime set equals the legacy
        // configured set exactly (C43 behavior unchanged).
        let plain = ServeConfig::parse(&superagent_yaml()).expect("golden parses");
        let sa = plain.superagent.as_ref().unwrap();
        assert!(sa.runtime_untrusted_set().unwrap().is_empty());
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

    /// C36: the `classes:` wire layer parses, converts fail-closed to
    /// the bundle's table, resolves most-specific-wins through the
    /// converted form, and unknown spellings/undeclared channels
    /// refuse to start.
    #[test]
    fn admission_classes_parse_convert_and_fail_closed() {
        use kian_superagent::rules::AdmissionClass;

        let yaml = superagent_yaml().replacen(
            "  channels:",
            "  classes:\n\
             \x20   - channel: mock-im\n\
             \x20     class: explicit-only\n\
             \x20   - channel: ci\n\
             \x20     topic: ci-noise\n\
             \x20     class: ignore\n\
             \x20   - channel: ci\n\
             \x20     topic: ci-noise\n\
             \x20     sender:\n\
             \x20       id: u-1\n\
             \x20     class: candidate\n\
             \x20 channels:",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("classes parse");
        let sa = cfg.superagent.as_ref().expect("superagent section");
        cfg.validate().expect("classes validate");
        let table = sa.admission_classes().expect("classes convert");
        assert_eq!(table.0.len(), 3);
        assert_eq!(table.0[0].class, AdmissionClass::ExplicitOnly);
        assert_eq!(table.0[0].topic, None);
        assert_eq!(table.0[1].topic.as_deref(), Some("ci-noise"));
        assert_eq!(table.0[2].sender, Some(serde_json::json!({"id": "u-1"})));

        // Resolution sanity through the converted table: the pinned
        // sender in the pinned topic wins over the topic-only ignore.
        let event = kian_core::seams::ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-cfg".into(),
            topic: Some("ci-noise".into()),
            sender: Some(serde_json::json!({"id": "u-1"})),
            group: None,
            payload: serde_json::json!({}),
            ts_ms: 1,
        };
        assert_eq!(table.resolve(&event), AdmissionClass::Candidate);

        // Fail-closed: an unknown class keyword refuses to load.
        let bad = superagent_yaml().replacen(
            "  channels:",
            "  classes:\n    - channel: mock-im\n      class: silence\n  channels:",
            1,
        );
        let err = ServeConfig::parse(&bad).unwrap_err().to_string();
        assert!(
            err.contains("not one of candidate | explicit-only | ignore"),
            "{err}"
        );

        // Fail-closed: a class keying an undeclared channel refuses
        // validation.
        let undeclared = superagent_yaml().replacen(
            "  channels:",
            "  classes:\n    - channel: lark\n      class: ignore\n  channels:",
            1,
        );
        let err = ServeConfig::parse(&undeclared).unwrap_err().to_string();
        assert!(
            err.contains("superagent.classes[0] references channel `lark`"),
            "{err}"
        );
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
        // C44 note: the fixture originally used `lark` as the unknown
        // type; lark is a real adapter now. C49 note: same story for
        // `github` — the probe rides a still-unknown spelling so the
        // unknown-type arm keeps its subject.
        let err =
            ServeConfig::parse(&superagent_yaml().replace("type: mock-im", "type: carrier-pigeon"))
                .unwrap_err();
        assert!(
            err.to_string().contains("unknown type `carrier-pigeon`"),
            "{}",
            err
        );
    }

    // -------------------------------------------------------------------
    // C48/C49: the github channel + webhook receiver wire layer
    // -------------------------------------------------------------------

    /// A github channel declaration spliced into the golden fixture
    /// (no credentials in the section — token and webhook secret are
    /// env-only).
    fn github_yaml(extra: &str) -> String {
        superagent_yaml().replacen(
            "  channels:\n    mock-im:",
            &format!("  channels:\n    github:\n      type: github\n{extra}    mock-im:"),
            1,
        )
    }

    const GITHUB_SECTION: &str = "      github:\n        topics:\n          flyer103/kian: ci\n";

    #[test]
    fn github_channel_config_parses_with_defaults() {
        let cfg = ServeConfig::parse(&github_yaml(GITHUB_SECTION)).expect("github config parses");
        cfg.validate().expect("github config validates");
        let sa = cfg.superagent.as_ref().unwrap();
        let github = sa.channels["github"].github.as_ref().expect("section");
        assert_eq!(github.api_base, "https://api.github.com");
        assert_eq!(
            github.topics,
            BTreeMap::from([("flyer103/kian".to_string(), "ci".to_string())])
        );
        assert!(github.events.is_empty());
        assert!(!github.send, "send defaults off (receive-only)");

        // Webhook limits default to the receiver module's constants.
        let limits = github.webhook_limits();
        assert_eq!(limits, crate::webhook::WebhookLimits::default());

        // The runtime config converts (receive-only needs no token)
        // and is untrusted by construction (C43/C48, no opt-out).
        let runtime = github.to_runtime("github", None).expect("converts");
        assert!(runtime.untrusted);
        assert_eq!(runtime.token, None);

        // C43: github-typed channels join the runtime untrusted set
        // automatically, like lark.
        let untrusted = sa.runtime_untrusted_set().unwrap();
        assert!(untrusted.contains("github"));
    }

    #[test]
    fn github_channel_section_is_fail_closed() {
        // No section on a github channel.
        let err = ServeConfig::parse(&github_yaml("")).unwrap_err();
        assert!(
            err.to_string().contains("requires a `github:` section"),
            "{err}"
        );

        // The section on a non-github channel.
        let misapplied = superagent_yaml().replacen(
            "    mock-im:\n      type: mock-im",
            "    mock-im:\n      type: mock-im\n      github:\n        topics:\n          a/b: ci",
            1,
        );
        let err = ServeConfig::parse(&misapplied).unwrap_err();
        assert!(
            err.to_string()
                .contains("belongs to `type: github` channels only"),
            "{err}"
        );

        // Degenerate shapes refuse (through the bundle's validation).
        let empty_topics = "      github:\n        topics: {}\n";
        let err = ServeConfig::parse(&github_yaml(empty_topics)).unwrap_err();
        assert!(err.to_string().contains("at least one repo"), "{err}");

        let bad_event =
            "      github:\n        topics:\n          a/b: ci\n        events: [frobnicate]\n";
        let err = ServeConfig::parse(&github_yaml(bad_event)).unwrap_err();
        assert!(
            err.to_string().contains("not a supported event type"),
            "{err}"
        );

        // Zero webhook limits refuse (a disabled limit is not a limit).
        let zero_cap = "      github:\n        topics:\n          a/b: ci\n        webhook:\n          max_body_bytes: 0\n";
        let err = ServeConfig::parse(&github_yaml(zero_cap)).unwrap_err();
        assert!(err.to_string().contains("max_body_bytes"), "{err}");

        let zero_rate = "      github:\n        topics:\n          a/b: ci\n        webhook:\n          rate_capacity: 0\n";
        let err = ServeConfig::parse(&github_yaml(zero_rate)).unwrap_err();
        assert!(err.to_string().contains("rate_capacity"), "{err}");
    }

    #[test]
    fn at_most_one_github_channel_mounts_the_single_route() {
        let doubled = superagent_yaml().replacen(
            "  channels:\n    mock-im:",
            "  channels:\n    gh1:\n      type: github\n      github:\n        topics:\n          a/b: ci\n    gh2:\n      type: github\n      github:\n        topics:\n          c/d: ops\n    mock-im:",
            1,
        );
        let err = ServeConfig::parse(&doubled).unwrap_err();
        assert!(
            err.to_string()
                .contains("at most one `type: github` channel"),
            "{err}"
        );
    }

    #[test]
    fn github_send_without_token_fails_closed_at_conversion() {
        let send = "      github:\n        topics:\n          a/b: ci\n        send: true\n";
        let cfg = ServeConfig::parse(&github_yaml(send)).expect("parses (env is assembly's)");
        let superagent = cfg.superagent.unwrap();
        let github = superagent.channels["github"].github.as_ref().unwrap();
        // No token in the environment contract: conversion refuses.
        let err = github.to_runtime("github", None).unwrap_err();
        assert!(err.to_string().contains("KIAN_GITHUB_TOKEN"), "{err}");
        // With the token it converts; the token rides the config.
        let runtime = github
            .to_runtime("github", Some("ghp_x".into()))
            .expect("converts with token");
        assert_eq!(runtime.token.as_deref(), Some("ghp_x"));
        // A blank token is a missing token.
        let err = github.to_runtime("github", Some("  ".into())).unwrap_err();
        assert!(err.to_string().contains("KIAN_GITHUB_TOKEN"), "{err}");
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

    // ------------------------------------------------------------------
    // C39: the speculation section
    // ------------------------------------------------------------------

    #[test]
    fn speculation_section_parses_and_converts() {
        let yaml = superagent_yaml().replace(
            "  router:\n    session_id:",
            "  router:\n    speculation:\n      max_outstanding: 3\n      label: weak-signal\n    session_id:",
        );
        let cfg = ServeConfig::parse(&yaml).expect("speculation config");
        let sa = cfg.superagent.as_ref().unwrap();
        let policy = sa.router.speculation.as_ref().unwrap().to_policy().unwrap();
        assert_eq!(policy.max_outstanding, 3);
        assert_eq!(policy.label, "weak-signal");
        assert_eq!(
            policy.silent_repro,
            kian_superagent::SILENT_REPRO_TEMPLATE,
            "silent-repro defaults to the bundle template"
        );

        // An override replaces the template (a config artifact).
        let yaml = yaml.replace(
            "      label: weak-signal\n",
            "      label: weak-signal\n      silent_repro: custom policy prose\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("speculation override");
        let policy = cfg
            .superagent
            .as_ref()
            .unwrap()
            .router
            .speculation
            .as_ref()
            .unwrap()
            .to_policy()
            .unwrap();
        assert_eq!(policy.silent_repro, "custom policy prose");
    }

    #[test]
    fn speculation_defaults_to_absent_and_degenerate_values_fail_closed() {
        // The unmodified fixture: no speculation section, spec disabled.
        let cfg = ServeConfig::parse(&superagent_yaml()).unwrap();
        assert!(cfg.superagent.unwrap().router.speculation.is_none());

        // Zero cap / empty label refuse to start (never a silent default).
        let zero = superagent_yaml().replace(
            "  router:\n    session_id:",
            "  router:\n    speculation:\n      max_outstanding: 0\n      label: x\n    session_id:",
        );
        let err = ServeConfig::parse(&zero).unwrap_err();
        assert!(
            err.to_string()
                .contains("speculation.max_outstanding must be > 0"),
            "{err}"
        );

        let empty_label = superagent_yaml().replace(
            "  router:\n    session_id:",
            "  router:\n    speculation:\n      max_outstanding: 2\n      label: \"\"\n    session_id:",
        );
        let err = ServeConfig::parse(&empty_label).unwrap_err();
        assert!(
            err.to_string()
                .contains("speculation.label must be non-empty"),
            "{err}"
        );

        // Unknown keys inside speculation fail closed like the rest of
        // the config surface.
        let unknown = superagent_yaml().replace(
            "  router:\n    session_id:",
            "  router:\n    speculation:\n      max_outstanding: 2\n      label: x\n      budget: 9\n    session_id:",
        );
        assert!(ServeConfig::parse(&unknown).is_err());
    }
}
