//! `kian.yaml` configuration loading.
//!
//! Storage (sqlite), model slots, session/drain budgets, recovery policy
//! tables, providers and the plugin list. Loading is **fail-closed**: a
//! malformed file, an unknown key, an unknown provider reference or an
//! unparseable `listen` address refuses to start with a clear error
//! (never a silent default).

use std::collections::BTreeMap;
use std::net::SocketAddr;

use kian_core::model::{AgentDef, Budgets, Permission, SlotName, SlotSpec, SlotTarget};
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
    /// Storage (sqlite default; postgres since).
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
    /// The `recovery:` section, parsed fail-closed by 's
    /// [`PolicyConfig`]. Kept as raw YAML so this crate never forks the
    /// policy schema.
    pub recovery: serde_yaml::Value,
    /// Additional named agent definitions (id → definition). The
    /// single `agent:` section remains the **default agent** shorthand —
    /// plain sessions always run it. Router/Worker definitions live here
    /// and are referenced from the `superagent:` section.
    #[serde(default)]
    pub agents: BTreeMap<String, AgentConfig>,
    /// The `superagent:` section. `None` (the section absent) is
    /// the built-in configuration: no bundle is assembled and every
    /// existing behavior is unchanged.
    #[serde(default)]
    pub superagent: Option<SuperAgentConfig>,
    /// Remote execution endpoints (over the queue). Empty = no
    /// remote exec.
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    /// The `memory:` section (round-8 plan, kian-87). `None` (the
    /// section absent) is memory **off**: the assembly, the ops plane
    /// and every existing behavior are byte-identical. Present parses
    /// fail-closed (unknown keys refuse) and switches the plain
    /// assembly onto the dream-routing driver.
    #[serde(default)]
    pub memory: Option<MemorySection>,
    /// The `read_model:` section (READ-MODEL §5/§9, P1). Absent = the
    /// defaults: verified snapshots enabled with a conservative
    /// threshold. Disabling (`read_model.snapshots.enabled: false`)
    /// hides every snapshot — the honest full-derivation fallback.
    #[serde(default)]
    pub read_model: ReadModelSection,
    /// The `retention:` section (READ-MODEL §7, P2b). Absent = retention
    /// **off**: nothing is ever archived or deleted, byte-identical to
    /// pre-P2b. Present parses fail-closed; enabling requires an
    /// archive directory and at least one online-window threshold.
    #[serde(default)]
    pub retention: RetentionSection,
}

fn default_plugins() -> Vec<String> {
    vec![
        "openai-compat".into(),
        "local-tools".into(),
        "react-loop".into(),
    ]
}

/// Storage selection: `event_store: sqlite` (the default — built-in
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

/// The `storage.postgres` section. The database URL — which may
/// carry a password — is read from the named environment variable at
/// startup: env-only, never a config value (the secrets rule).
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
    /// Per-call tool permission table (ADR-015): tool name →
    /// `allow`/`deny`, closed vocabulary — an unknown value refuses to
    /// parse. Every key must be present in [`AgentConfig::tools`]
    /// (fail-closed, validated in [`ServeConfig::validate`]). Absent =
    /// empty = allow-all with the legacy byte-identical event stream.
    /// Evaluated on the built-in loop, the plain-session (default) loop
    /// and the superagent worker loop; a table on the Router/Watchdog
    /// judge agents refuses boot (never silently ignored).
    #[serde(default)]
    pub permissions: BTreeMap<String, Permission>,
    /// Session budgets (A5; `null` = unlimited).
    #[serde(default)]
    pub budgets: BudgetsDef,
    /// Evidence-gate tuning (wired by the assembly): the
    /// default agent's gate mounts on the built-in loop's stop path; a
    /// superagent worker agent's gate mounts on the worker wrapper.
    /// Default is disabled — built-in behavior unchanged.
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

// ---------------------------------------------------------------------------
// memory section (round-8 plan, kian-87)
// ---------------------------------------------------------------------------

/// The `memory:` section: the memory scope plus the dreamer caps.
/// Absent = memory off (the caller's "off", never an error); present is
/// validated fail-closed in [`ServeConfig::validate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySection {
    /// The memory scope: the agent identity whose memory this is
    /// (MEMORY-MODEL — scope = Agent identity). Must name a configured
    /// agent (the default `agent:` id or an `agents:` entry); it rides
    /// the dream prompt header verbatim.
    pub scope: String,
    /// The dreamer assembly.
    pub dreamer: DreamerSection,
}

/// The `memory.dreamer` block: which agent dreams, and the caps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DreamerSection {
    /// The agent that runs the dreaming drain (a named `agents:` entry
    /// or the default `agent:` id). Its loop is wrapped in the
    /// `DreamDriver`; its `main` slot drives the dream turns.
    pub agent: String,
    /// Max bytes of source transcript one dream may read (default
    /// 256 KiB, `kian_memory::DEFAULT_MAX_BYTES_READ`). Must be > 0.
    #[serde(default)]
    pub max_bytes_read: Option<u64>,
    /// Max notes one dream answer may carry (default 20,
    /// `kian_memory::DEFAULT_MAX_NOTES`); over-cap is refused. Must
    /// be > 0.
    #[serde(default)]
    pub max_notes: Option<u64>,
}

impl DreamerSection {
    /// The caps in force (explicit values or the bundle defaults).
    /// The values were already validated positive; this only fills
    /// defaults.
    #[must_use]
    pub fn caps(&self) -> kian_memory::MemoryConfig {
        kian_memory::MemoryConfig {
            max_bytes_read: self
                .max_bytes_read
                .unwrap_or(kian_memory::DEFAULT_MAX_BYTES_READ),
            max_notes: self.max_notes.unwrap_or(kian_memory::DEFAULT_MAX_NOTES),
        }
    }
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
    /// the exec child environment policy (`tools.exec_env`).
    /// Default `inherit` = today's behavior (compatibility clause).
    #[serde(default)]
    pub exec_env: ExecEnvSection,
    /// the exec output redaction config (`tools.exec_redact`).
    /// Redaction is ON by default — it only scrubs what is persisted,
    /// never what the child does.
    #[serde(default)]
    pub exec_redact: ExecRedactSection,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            root: default_tools_root(),
            exec_timeout_ms: default_exec_timeout(),
            exec_env: ExecEnvSection::default(),
            exec_redact: ExecRedactSection::default(),
        }
    }
}

/// The `tools.exec_env:` section (fix ①: secrets never reach
/// spawned children).
///
/// ```yaml
/// tools:
///   exec_env:
///     mode: allowlist
///     allow: [PATH, HOME, LANG, TMPDIR]
/// ```
///
/// `mode: allowlist` clears the child environment and passes only the
/// listed names — plus `DRY_RUN` when the parent has it set (the
/// dry-run contract rides the environment). `mode: inherit` (the
/// default) keeps today's full passthrough; output redaction
/// (`tools.exec_redact`) is the backstop either way.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecEnvSection {
    /// `inherit` (default) or `allowlist`; anything else refuses boot.
    #[serde(default = "default_exec_env_mode")]
    pub mode: String,
    /// Variable names an allowlisted child may see. Names only —
    /// values are read from the parent environment at spawn time and
    /// never live in kian.yaml.
    #[serde(default)]
    pub allow: Vec<String>,
}

impl Default for ExecEnvSection {
    fn default() -> Self {
        Self {
            mode: default_exec_env_mode(),
            allow: Vec::new(),
        }
    }
}

fn default_exec_env_mode() -> String {
    "inherit".into()
}

impl ExecEnvSection {
    /// Converts to the runtime policy, fail-closed on an unknown mode.
    pub fn to_policy(&self) -> Result<kian_core::secrets::ExecEnvPolicy, ConfigError> {
        match self.mode.as_str() {
            "inherit" => Ok(kian_core::secrets::ExecEnvPolicy::Inherit),
            "allowlist" => Ok(kian_core::secrets::ExecEnvPolicy::allowlist(&self.allow)),
            other => Err(ConfigError::Invalid(format!(
                "tools.exec_env.mode: unknown mode {other:?} (expected \"inherit\" or \"allowlist\")"
            ))),
        }
    }
}

/// The `tools.exec_redact:` section (fix ②: no configured
/// secret persists into the event stream).
///
/// ```yaml
/// tools:
///   exec_redact:
///     enabled: true # default
///     env_names: [MY_EXTRA_TOKEN] # beyond the auto-collected ones
/// ```
///
/// The assembly auto-collects every secret name kian.yaml itself
/// configures (`api_key_env`, `KIAN_GITHUB_TOKEN`, webhook/lark
/// secrets, the postgres URL env); `env_names` lists extras the
/// operator knows about (e.g. a `gh` CLI token the workers use).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecRedactSection {
    /// Redaction on/off. Default on.
    #[serde(default = "default_exec_redact_enabled")]
    pub enabled: bool,
    /// Extra secret environment variable names to redact by value.
    #[serde(default)]
    pub env_names: Vec<String>,
}

impl Default for ExecRedactSection {
    fn default() -> Self {
        Self {
            enabled: default_exec_redact_enabled(),
            env_names: Vec::new(),
        }
    }
}

fn default_exec_redact_enabled() -> bool {
    true
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
// the `read_model` section (READ-MODEL §5/§9, P1)
// ---------------------------------------------------------------------------

/// The `read_model:` section. Default: verified snapshots **enabled**
/// with a conservative threshold (a session must reach `min_events`
/// before a snapshot pays for itself); disabling hides every snapshot —
/// the loader falls back to full stream derivation and behavior is
/// byte-identical to pre-P1. Materialized views (P2a) default enabled
/// the same way: disabling hides every view — operator surfaces fall
/// back to full stream derivation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ReadModelSection {
    /// The `read_model.snapshots:` subsection.
    #[serde(default)]
    pub snapshots: SnapshotSection,
    /// The `read_model.views:` subsection (READ-MODEL §6, P2a).
    #[serde(default)]
    pub views: ViewsSection,
}

/// Materialized-view settings (READ-MODEL §6): enable plus the
/// maintenance rebuild cadence. Views are observability accelerators;
/// a disabled builder hides every view and every operator surface
/// serves the honest full derivation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewsSection {
    /// Whether operator surfaces may consult materialized views.
    /// `false` hides every view (honest full derivation everywhere).
    #[serde(default = "default_views_enabled")]
    pub enabled: bool,
    /// The maintenance rebuild cadence (the rebuilder runs outside
    /// drains, under a maintenance lease — READ-MODEL §9).
    #[serde(default = "default_views_interval_ms")]
    pub interval_ms: u64,
}

impl Default for ViewsSection {
    fn default() -> Self {
        Self {
            enabled: default_views_enabled(),
            interval_ms: default_views_interval_ms(),
        }
    }
}

fn default_views_enabled() -> bool {
    true
}
fn default_views_interval_ms() -> u64 {
    60_000
}

/// Verified-snapshot settings (READ-MODEL §5): enable/min-events/tail
/// cap/keep-N plus the maintenance tick cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSection {
    /// Whether projection/recovery reads may consult verified
    /// snapshots. `false` hides all snapshots (honest full derivation).
    #[serde(default = "default_snapshots_enabled")]
    pub enabled: bool,
    /// Sessions below this head version are never snapshotted (and the
    /// loader never consults one — none exists yet).
    #[serde(default = "default_snapshots_min_events")]
    pub min_events: u64,
    /// Replay bound: a tail longer than this skips the snapshot.
    #[serde(default = "default_snapshots_tail_cap")]
    pub tail_cap: u64,
    /// Verified snapshot versions kept per session (older GC-ed).
    #[serde(default = "default_snapshots_keep_n")]
    pub keep_n: u64,
    /// The maintenance tick cadence (the builder runs outside drains,
    /// READ-MODEL §9).
    #[serde(default = "default_snapshots_interval_ms")]
    pub interval_ms: u64,
}

impl Default for SnapshotSection {
    fn default() -> Self {
        Self {
            enabled: default_snapshots_enabled(),
            min_events: default_snapshots_min_events(),
            tail_cap: default_snapshots_tail_cap(),
            keep_n: default_snapshots_keep_n(),
            interval_ms: default_snapshots_interval_ms(),
        }
    }
}

fn default_snapshots_enabled() -> bool {
    true
}
fn default_snapshots_min_events() -> u64 {
    kian_orchestrator::DEFAULT_MIN_EVENTS
}
fn default_snapshots_tail_cap() -> u64 {
    kian_orchestrator::DEFAULT_TAIL_CAP
}
fn default_snapshots_keep_n() -> u64 {
    kian_orchestrator::DEFAULT_KEEP_N
}
fn default_snapshots_interval_ms() -> u64 {
    30_000
}

// ---------------------------------------------------------------------------
// the `retention` section (READ-MODEL §7, P2b)
// ---------------------------------------------------------------------------

/// The `retention:` section. Default: **disabled** — retention is
/// strictly opt-in (the owner gate: export-only archive, terminal
/// sessions only, no transparent cold tier). With the section absent
/// or `enabled: false`, behavior is byte-identical to pre-P2b: nothing
/// is ever archived or deleted. Enabling parses fail-closed: an
/// archive directory and at least one online-window threshold must be
/// named explicitly — an enabled-but-vague retention never starts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RetentionSection {
    /// Whether `kian archive run` may archive anything at all.
    /// Default `false` (opt-in).
    #[serde(default)]
    pub enabled: bool,
    /// The directory archive packages are written under (one
    /// subdirectory per session). Required when enabled.
    #[serde(default)]
    pub archive_dir: String,
    /// Online-window threshold: terminal sessions younger than this
    /// (ms since their `session/ended`) stay online. At least one of
    /// `min_age_ms` / `min_bytes` must be set when enabled.
    #[serde(default)]
    pub min_age_ms: Option<u64>,
    /// Online-window threshold: terminal sessions smaller than this
    /// (bytes of online event + input storage) stay online.
    #[serde(default)]
    pub min_bytes: Option<u64>,
}

// ---------------------------------------------------------------------------
// the `superagent.harness` section (the dsh bridge mount)
// ---------------------------------------------------------------------------

/// The `superagent.harness:` section. Every harness is default-off;
/// the built-in worker loop stays the default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HarnessConfig {
    /// The dsh bridge (ADR-010 / DESIGN §16): the worker
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
// super-agent + endpoint configuration
// ---------------------------------------------------------------------------

/// The `superagent:` section. Presence switches `serve` into the
/// super-agent assembly (bundle plugins, resident Router, subscription
/// engine); absence keeps the built-in single-agent server byte-for-byte.
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
    /// Admission classes (the article §3.1 Fast Path): a
    /// provenance-keyed filter evaluated *before* the rule set and
    /// before any model call. Wire form: `{channel, topic?, sender?,
    /// class: candidate | explicit-only | ignore}`; converted
    /// fail-closed to the bundle's [`AdmissionClassTable`] by
    /// [`SuperAgentConfig::admission_classes`]. Absent = today's
    /// behavior, unchanged.
    #[serde(default)]
    pub classes: Vec<AdmissionClassDef>,
    /// admission-time Inline/Spill shaping. Absent preserves the
    /// legacy infinite cap and byte-identical legacy wrapper.
    #[serde(default)]
    pub shaping: Option<ChannelShapingConfig>,
    /// untrusted-input marking (prompt-injection hygiene):
    /// channels whose events admit with `"untrusted": true` inside
    /// the provenance wrapper's `content`. Honest labeling only —
    /// never a drop/allow decision. Real channels (lark) belong
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
    /// profile and the worker template (ADR-008). Default off —
    /// the article's own "not default-on". Requires a non-empty
    /// `endpoints:` list (the pair routes through the queue);
    /// enabling it without endpoints refuses to start (fail-closed).
    #[serde(default)]
    pub async_tools: bool,
    /// the harness section — external agent harnesses the worker
    /// template can run instead of the built-in loop. Default off;
    /// absence keeps the built-in worker loop byte-for-byte.
    #[serde(default)]
    pub harness: HarnessConfig,
    /// The recall toolpack (the Chain of Recall): read-only
    /// `recall_event` / `recall_neighbors` / `recall_thread` tools.
    /// Default: mounted on the Router profile, opt-in per worker
    /// template, with bounded tool-output caps.
    #[serde(default)]
    pub recall: RecallConfig,
    /// Watchdog Session v2: the resident model-in-the-loop
    /// progress judge. Absent (the default) = no watchdog session, no
    /// change to today's behavior. Present = assembly mounts the
    /// watchdog loop on the fixed session and bootstraps its judge
    /// prompt; ticks reach it through an ordinary subscription rule
    /// (`route: {session: <uuid>}`) over a `type: timer` channel.
    #[serde(default)]
    pub watchdog: Option<WatchdogConfig>,
    /// output-side speak-rules: the *whether-to-send* gate on the
    /// Router's `reply`/`deliver_structured` tools (attention
    /// economics — DESIGN §4.2). Absent (the default) = every send
    /// passes, byte-identical to today. Converted fail-closed by
    /// [`SuperAgentConfig::speak_policy`]; every rule's `channel`
    /// must be declared in `channels:` (fail-closed cross-reference).
    #[serde(default)]
    pub speak_rules: Vec<SpeakRuleDef>,
    /// follow-up ownership: a person's follow-up belongs to the
    /// live Ticket's Executor (reference 002). On (the default), the
    /// fan-out admits to live subscribers only — an all-ended topic
    /// releases to the Router as a fresh evaluation
    /// (`released_to_eval`) — and `spawn_worker` refuses a topic
    /// correlation a live worker already owns with an honest tool
    /// error naming the owner (the Router may only stop it).
    /// `false` restores the legacy behavior.
    #[serde(default = "default_followup_ownership")]
    pub followup_ownership: bool,
    /// policy/prompt versioning: the policy revision the resident
    /// profiles render into their bootstrap prompts as a
    /// `[policy <label>]` header, plus the drift-notice gate. Absent
    /// (`version` unset) = today's bytes, byte-identical.
    #[serde(default)]
    pub policy: PolicyVersionConfig,
    /// (ADR-012) mandate issuance: per-worker-template scope
    /// texts bounding every delegation `spawn_worker` issues. Absent
    /// (the default) = no mandate — wrappers stay byte-identical
    /// legacy. Converted fail-closed by
    /// [`SuperAgentConfig::mandate_policy`].
    #[serde(default)]
    pub mandate: MandateConfig,
    /// outbox bounded retry (`superagent.outbox`): stored work
    /// for the write side — bounded backoff, per-`(ticket,
    /// conversation)` ordering, park with a visible stage. Absent
    /// (the default) = the legacy single-attempt send path,
    /// byte-identical. Present (even empty) enables it; converted
    /// fail-closed by [`SuperAgentConfig::outbox_config`].
    #[serde(default)]
    pub outbox: Option<OutboxConfigDef>,
    /// /ADR-013 thread-key intake (`superagent.intake`): the
    /// config-owned canonical thread-key derivations. The derived key
    /// surfaces in the Inline `neighbors` block (`thread_key`) as
    /// recall context — correlation is context only, never a routing
    /// input (routing stays topic-keyed; poll-mode github topics are
    /// the derived keys themselves, produced at adapter mapping time).
    /// Absent = no derivation, byte-identical wrappers.
    #[serde(default)]
    pub intake: IntakeConfig,
}

/// 's default: follow-up ownership is **on** — the honest default
/// is the rule, opt-out is explicit.
fn default_followup_ownership() -> bool {
    true
}

impl SuperAgentConfig {
    /// Converts the speak-rule table to the bundle policy,
    /// fail-closed: an unknown condition spelling or an empty channel
    /// refuses to start, never silently defaults. Errors carry the
    /// rule's index.
    pub fn speak_policy(&self) -> Result<kian_superagent::SpeakPolicy, ConfigError> {
        let rules = self
            .speak_rules
            .iter()
            .enumerate()
            .map(|(i, def)| {
                def.to_rule_inner()
                    .map_err(|message| invalid(format!("superagent.speak_rules[{i}].{message}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(kian_superagent::SpeakPolicy::from_rules(rules))
    }

    /// Converts the policy-version section to the bundle's
    /// [`kian_superagent::PolicyVersion`], fingerprinted against the
    /// resident profiles' prompt templates (the judge and watchdog
    /// defaults) so a label always names the template bytes it was
    /// pinned against. `None` when no version is configured (today's
    /// bytes, byte-identical).
    pub fn policy_version(&self) -> Option<kian_superagent::PolicyVersion> {
        self.policy.version.as_deref().map(|label| {
            kian_superagent::PolicyVersion::from_templates(
                label,
                &[
                    ("judge", kian_superagent::JUDGE_PROMPT_TEMPLATE),
                    ("watchdog", kian_superagent::WATCHDOG_PROMPT_TEMPLATE),
                ],
            )
        })
    }

    /// Converts the mandate section to the bundle's
    /// [`kian_superagent::MandatePolicy`]. `None` when nothing is
    /// configured (no mandate issued — byte-identical legacy
    /// wrappers).
    pub fn mandate_policy(&self) -> Option<kian_superagent::MandatePolicy> {
        self.mandate.to_policy()
    }

    /// Converts the outbox section to the bundle's
    /// [`kian_superagent::OutboxConfig`], fail-closed (a broken
    /// schedule refuses to start). `Ok(None)` when nothing is
    /// configured (the legacy single-attempt send path,
    /// byte-identical).
    pub fn outbox_config(&self) -> Result<Option<kian_superagent::OutboxConfig>, ConfigError> {
        let Some(config) = self.outbox.as_ref() else {
            return Ok(None);
        };
        let converted = kian_superagent::OutboxConfig {
            max_attempts: config
                .max_attempts
                .unwrap_or(kian_superagent::DEFAULT_MAX_ATTEMPTS),
            backoff_start_ms: config
                .backoff_start_ms
                .unwrap_or(kian_superagent::DEFAULT_BACKOFF_START_MS),
            backoff_cap_ms: config
                .backoff_cap_ms
                .unwrap_or(kian_superagent::DEFAULT_BACKOFF_CAP_MS),
        };
        converted
            .validate()
            .map_err(|message| invalid(format!("superagent.outbox: {message}")))?;
        Ok(Some(converted))
    }
}

/// The `superagent.outbox` section:
/// `{max_attempts?, backoff_start_ms?, backoff_cap_ms?}` — every
/// field optional; an empty section enables the outbox with the
/// defaults (6 attempts, backoff 2s doubling, cap 60s).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OutboxConfigDef {
    pub max_attempts: Option<u32>,
    pub backoff_start_ms: Option<u64>,
    pub backoff_cap_ms: Option<u64>,
}

/// The `superagent.intake` section (ADR-013):
/// `{thread_keys: [{channel, key: [group?, <payload-field>…]}]}`.
///
/// The derived key (`<channel>:<group>#<key>`, percent-encoded
/// components — collision-free by construction) surfaces in the
/// Inline `neighbors` block as `thread_key` recall context. Keys are
/// **config-owned**: a config change is a new key, and old streams
/// keep their topics (cache-stable, no rewrite). Absent section = no
/// derivation, byte-identical admission wrappers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct IntakeConfig {
    #[serde(default)]
    pub thread_keys: Vec<ThreadKeyDef>,
}

/// One `superagent.intake.thread_keys` entry: `{channel, key: […]}`.
/// `key` entries: `"group"` fills the group slot from the event's
/// group provenance; any other name is a dotted payload-field path
/// whose leaf must be a scalar. The wire form is the bundle's
/// [`kian_superagent::ThreadKeyRule`] verbatim (one shape, two
/// readers — this def adds only fail-closed config paths).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadKeyDef {
    /// Channel name the rule applies to (must be declared in
    /// `superagent.channels`, fail-closed cross-reference).
    pub channel: String,
    /// The derivation fields, config order (see the type doc).
    pub key: Vec<String>,
}

impl IntakeConfig {
    /// Converts to the bundle's [`kian_superagent::ThreadKeySet`],
    /// fail-closed: blank channels, empty key lists and undeclared
    /// channel references refuse to start (the caller supplies the
    /// declared channel names for the cross-reference).
    pub fn to_runtime(
        &self,
        declared_channels: &std::collections::BTreeSet<String>,
    ) -> Result<kian_superagent::ThreadKeySet, ConfigError> {
        let mut rules = Vec::with_capacity(self.thread_keys.len());
        for (i, def) in self.thread_keys.iter().enumerate() {
            if def.channel.trim().is_empty() {
                return Err(invalid(format!(
                    "superagent.intake.thread_keys[{i}].channel must not be empty"
                )));
            }
            if def.key.is_empty() {
                return Err(invalid(format!(
                    "superagent.intake.thread_keys[{i}].key must name at least one field"
                )));
            }
            if def.key.iter().any(|field| field.trim().is_empty()) {
                return Err(invalid(format!(
                    "superagent.intake.thread_keys[{i}].key entries must not be empty"
                )));
            }
            if !declared_channels.contains(&def.channel) {
                return Err(invalid(format!(
                    "superagent.intake.thread_keys[{i}] references channel `{}` which is not \
                     declared in superagent.channels",
                    def.channel
                )));
            }
            rules.push(kian_superagent::ThreadKeyRule {
                channel: def.channel.clone(),
                key: def.key.clone(),
            });
        }
        Ok(kian_superagent::ThreadKeySet::new(rules))
    }
}

/// The `superagent.policy` section:
/// `{version: <label>, notify_live: bool}`.
///
/// - `version` (optional) — the policy revision label rendered into
///   the resident Router/watchdog bootstrap prompts as a
///   `[policy <label>]` header. Absent = today's bytes. Labels are
///   restricted to `[A-Za-z0-9._+-]+` (fail-closed at validation) so
///   the header parses back unambiguously.
/// - `notify_live` (default `false`) — when a boot-time drift is
///   detected, deliver one synthetic policy-changed marker input to
///   each affected live session (idempotent by a stable event id).
///   Live sessions are never rewritten either way (cache-byte
///   stability).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PolicyVersionConfig {
    pub version: Option<String>,
    #[serde(default)]
    pub notify_live: bool,
}

impl PolicyVersionConfig {
    /// Fail-closed label validation: non-empty and restricted to
    /// header-safe ASCII (no `]`, no whitespace — the
    /// `[policy <label>]` header must parse back unambiguously from
    /// persisted prompts).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(label) = &self.version {
            let safe = !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'));
            if !safe {
                return Err(invalid(format!(
                    "superagent.policy.version `{label}` is not a valid label (non-empty, \
                     ASCII letters/digits/`. _ - + :` only — it rides the `[policy <label>]` \
                     prompt header and must parse back unambiguously)"
                )));
            }
        }
        Ok(())
    }
}

/// The `superagent.mandate` section (ADR-012):
/// `{scopes: {<worker-template-id>: <scope text>}, default_scope: <text>}`.
///
/// - `scopes` — worker template id (`agents.<id>`) → the
///   natural-language bounds stamped on every mandate issued for
///   workers of that template. Non-empty template keys and scope
///   texts (fail-closed at validation).
/// - `default_scope` (optional) — bounds templates with no `scopes`
///   entry. Absent: an unlisted template is issued no mandate
///   (unconfigured authority is never silently defaulted).
///
/// Absent section = no mandate at all (byte-identical legacy
/// delegation wrappers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MandateConfig {
    #[serde(default)]
    pub scopes: BTreeMap<String, String>,
    #[serde(default)]
    pub default_scope: Option<String>,
}

impl MandateConfig {
    /// Fail-closed validation: template keys non-empty, scope texts
    /// non-blank (an empty authority string is never issued).
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (template, scope) in &self.scopes {
            if template.trim().is_empty() {
                return Err(invalid(
                    "superagent.mandate.scopes has an empty worker-template key (keys are \
                     `agents.<id>` template ids)",
                ));
            }
            if scope.trim().is_empty() {
                return Err(invalid(format!(
                    "superagent.mandate.scopes[`{template}`] has a blank scope — a mandate \
                     scope is the worker's authority bounds and is never empty"
                )));
            }
        }
        if self
            .default_scope
            .as_ref()
            .is_some_and(|scope| scope.trim().is_empty())
        {
            return Err(invalid(
                "superagent.mandate.default_scope is blank — a mandate scope is the worker's \
                 authority bounds and is never empty",
            ));
        }
        Ok(())
    }

    /// Converts to the bundle policy; `None` when nothing is
    /// configured.
    pub fn to_policy(&self) -> Option<kian_superagent::MandatePolicy> {
        if self.scopes.is_empty() && self.default_scope.is_none() {
            return None;
        }
        Some(kian_superagent::MandatePolicy {
            scopes: self.scopes.clone(),
            default_scope: self.default_scope.clone(),
        })
    }
}

/// One `superagent.speak_rules` entry:
/// `{channel, when: always | ack-first-only | decision-needed |
/// result-ready | never, per_topic_ack?}` — the condition under which
/// a send may leave the channel, evaluated before the §4.2 shaping
/// policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpeakRuleDef {
    /// The channel this rule gates. Must be declared in
    /// `superagent.channels` (fail-closed cross-reference) and
    /// non-empty.
    pub channel: String,
    /// The closed condition vocabulary's code spelling. Unknown
    /// spellings refuse to start.
    pub when: String,
    /// For `ack-first-only`: key the confirm-once memory per topic
    /// rather than per channel. Default false.
    #[serde(default)]
    pub per_topic_ack: bool,
}

impl SpeakRuleDef {
    /// Converts to the bundle rule, fail-closed. Messages are
    /// index-free; [`SuperAgentConfig::speak_policy`] prefixes them.
    pub fn to_rule(&self) -> Result<kian_superagent::SpeakRule, ConfigError> {
        self.to_rule_inner().map_err(invalid)
    }

    fn to_rule_inner(&self) -> Result<kian_superagent::SpeakRule, String> {
        if self.channel.trim().is_empty() {
            return Err("channel must be non-empty".into());
        }
        let when = kian_superagent::SpeakCondition::from_code(&self.when).ok_or_else(|| {
            format!(
                "when `{}` is not one of always | ack-first-only | decision-needed | \
                     result-ready | never",
                self.when
            )
        })?;
        Ok(kian_superagent::SpeakRule {
            channel: self.channel.clone(),
            when,
            per_topic_ack: self.per_topic_ack,
        })
    }
}

/// The additive `superagent.watchdog:` section: the resident
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

/// The additive `superagent.recall:` section: the recall
/// toolpack's mount gates and caps. `router` defaults on (the Router
/// is the global-observability seat and rides the event stream via
/// recall, not via its own context), `worker` defaults off (opt-in
/// per worker template). The caps bound what one recall tool output
/// may re-inject verbatim — recall reuses the shaping vocabulary
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

/// The additive `superagent.shaping:` section. The cap applies to
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
    /// The Router profile's speculation budget. Absent (the
    /// default) disables speculation: `spawn_worker` with
    /// `speculative: true` is refused with an honest tool error.
    #[serde(default)]
    pub speculation: Option<SpeculationDef>,
}

/// The `superagent.router.speculation` section:
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
    /// Converts to the bundle policy, fail-closed: a zero cap or
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
    /// The lark section — required when (and only when)
    /// `type: lark` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]).
    #[serde(default)]
    pub lark: Option<LarkChannelSection>,
    /// The timer section — required when (and only when)
    /// `type: timer` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]).
    #[serde(default)]
    pub timer: Option<TimerChannelSection>,
    /// The github section — required when (and only when)
    /// `type: github` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]). Configuring a github channel is what
    /// mounts the `POST /webhooks/github` receiver route.
    #[serde(default)]
    pub github: Option<GitHubChannelSection>,
}

/// The `superagent.channels.<name>.github:` section (adapter +
/// receiver): the topic-mapping allowlist, the events filter and
/// the webhook receiver limits. Credentials are deliberately absent:
/// the send token comes from `KIAN_GITHUB_TOKEN` and the webhook
/// secret from `KIAN_GITHUB_WEBHOOK_SECRET` (assembly, fail-closed) —
/// tokens never live in `kian.yaml` (the secrets discipline).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubChannelSection {
    /// REST API base (api.github.com or a GitHub Enterprise server).
    #[serde(default = "default_github_api_base")]
    pub api_base: String,
    /// Topic mapping table: `owner/repo` → topic, optionally
    /// narrowed per PR with an exact `owner/repo#N` → topic key. The
    /// keys are also the receive allowlist: a delivery from an
    /// unmapped repo is an honest drop (the contract). In
    /// poll-receive mode the table keys are still the allowlist (the
    /// polled repo must be mapped); the routing topic of a polled
    /// event is its derived thread key, not the table value.
    pub topics: BTreeMap<String, String>,
    /// Webhook event-type filter (empty = every supported type).
    #[serde(default)]
    pub events: std::collections::BTreeSet<String>,
    /// Is send configured? Requires `KIAN_GITHUB_TOKEN` at assembly
    /// (fail-closed); receive-only needs none.
    #[serde(default)]
    pub send: bool,
    /// Webhook receiver limits. Defaults are the receiver
    /// module's (`WebhookLimits::default`).
    #[serde(default)]
    pub webhook: GitHubWebhookLimitsSection,
    /// is webhook receive mounted for this channel? Default
    /// `true` (the shape); `false` on a poll-only channel
    /// keeps the webhook route — and its env-only secret
    /// requirement — unconfigured (poll mode is *beside* the webhook
    /// surface, and a deployment that receives no webhooks mounts no
    /// signature-verifying route).
    #[serde(default = "default_github_webhook_receive")]
    pub webhook_receive: bool,
    /// the poll-receive mode. Present (with `webhook_receive` at
    /// its default) the channel receives webhooks **and** polls;
    /// absent the channel is exactly the shape.
    #[serde(default)]
    pub poll: Option<GitHubPollSection>,
}

fn default_github_webhook_receive() -> bool {
    true
}

fn default_github_api_base() -> String {
    kian_superagent::GITHUB_DEFAULT_API_BASE.to_string()
}

/// The default bounded first-poll backfill window: 24 hours. A
/// fresh deployment admits at most the issues/comments updated in
/// this window — never the repo's whole history.
pub const DEFAULT_POLL_BACKFILL_MS: u64 = 86_400_000;

/// The `superagent.channels.<name>.github.poll:` section:
/// `{repo, interval_ms, kinds, since_field?, backfill_ms?}` — the
/// poll-receive mode beside webhook receive. `repo` must be a
/// `owner/repo` key of the channel's `topics` table (the receive
/// allowlist); `kinds` names the REST list endpoints
/// (`issues` | `issue_comments`); `since_field` defaults to
/// `updated_at` (the only honest v1 cursor) and `backfill_ms` to 24h.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubPollSection {
    /// The repo polled (`owner/repo`; must be a `topics` key).
    pub repo: String,
    /// Milliseconds between polls (> 0).
    pub interval_ms: u64,
    /// The list endpoints to drive (non-empty; `issues` |
    /// `issue_comments`).
    pub kinds: std::collections::BTreeSet<String>,
    /// The cursor field (default `updated_at`).
    #[serde(default)]
    pub since_field: Option<String>,
    /// The bounded first-poll window, milliseconds (default 24h).
    #[serde(default)]
    pub backfill_ms: Option<u64>,
}

/// The `superagent.channels.<name>.github.webhook:` limits:
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
    /// degenerate field (the validation). `token` is the env-only
    /// credential filled by assembly (`KIAN_GITHUB_TOKEN`; `send:
    /// true` without it is a startup error, receive/poll need none).
    pub fn to_runtime(
        &self,
        name: &str,
        token: Option<String>,
    ) -> Result<kian_superagent::GitHubConfig, ConfigError> {
        let config = kian_superagent::GitHubConfig {
            api_base: self.api_base.clone(),
            topics: self.topics.clone(),
            events: self.events.clone(),
            // GitHub payloads are attacker-influible text — the
            // channel is untrusted, no opt-out (the lark discipline).
            untrusted: true,
            send: self.send,
            token: token
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            poll: self.poll_runtime(name)?,
        };
        config
            .validate()
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type github): {e}")))?;
        Ok(config)
    }

    /// The poll section as the bundle's runtime config,
    /// fail-closed on kind spellings and degenerate bounds.
    fn poll_runtime(&self, name: &str) -> Result<Option<kian_superagent::PollConfig>, ConfigError> {
        let Some(section) = &self.poll else {
            return Ok(None);
        };
        let mut kinds = std::collections::BTreeSet::new();
        for kind in &section.kinds {
            let parsed = match kind.as_str() {
                "issues" => kian_superagent::PollKind::Issues,
                "issue_comments" => kian_superagent::PollKind::IssueComments,
                other => {
                    return Err(invalid(format!(
                        "superagent.channels `{name}`.github.poll.kinds entry `{other}` is not \
                         one of [issues, issue_comments]"
                    )));
                }
            };
            kinds.insert(parsed);
        }
        Ok(Some(kian_superagent::PollConfig {
            repo: section.repo.clone(),
            interval_ms: section.interval_ms,
            kinds,
            since_field: section
                .since_field
                .clone()
                .unwrap_or_else(|| kian_superagent::POLL_SINCE_FIELD.to_string()),
            backfill_ms: section.backfill_ms.unwrap_or(DEFAULT_POLL_BACKFILL_MS),
        }))
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
            poll: self.poll_runtime(name)?,
        };
        probe
            .validate()
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type github): {e}")))?;
        Ok(())
    }

    /// The receiver limits (defaults where unset).
    pub fn webhook_limits(&self) -> crate::webhook::WebhookLimits {
        crate::webhook::WebhookLimits {
            max_body_bytes: self.webhook.max_body_bytes,
            rate_capacity: self.webhook.rate_capacity,
            rate_refill_per_second: self.webhook.rate_refill_per_second,
        }
    }
}

/// The `superagent.channels.<name>.timer:` section: the timer
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

/// The `superagent.channels.<name>.lark:` section. Credentials
/// are deliberately absent: `app_id`/`app_secret` come from the
/// environment (`KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET`) at assembly
/// — tokens never live in `kian.yaml` (the secrets discipline).
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
    /// and never a config-file token.
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
/// `{passthrough: {correlate_by: topic}}`, or —, additive —
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

/// One admission-class entry (wire form): a provenance key
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
                // a config-addressed fixed session (the timer's
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

    /// Converts the `untrusted_channels` list into the runtime
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

    /// The runtime untrusted set (+ +): the configured
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

/// Every channel adapter type `serve` knows how to build (v1:
/// the first-class mock; adds the real lark adapter; the
/// timer input source; the github channel + webhook
/// receiver).
pub const KNOWN_CHANNEL_TYPES: [&str; 4] = ["mock-im", "lark", "timer", "github"];

/// One remote execution endpoint (over the durable queue).
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
        // Storage: the backend picks its required section, and any
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
                        "storage.event_store: postgres requires a storage.postgres section with url_env (env-only URLs, the secrets rule)",
                    )
                })?;
                if pg.url_env.trim().is_empty() {
                    return Err(invalid(
                        "storage.postgres.url_env must name the environment variable holding the database URL",
                    ));
                }
                // condition (c) depends on this: two replicas sharing
                // a holder string would defeat same-holder claim
                // admission, so the default holder refuses to start.
                if self.drain.holder.trim().is_empty() {
                    return Err(invalid(
                        "storage.event_store: postgres requires an explicit drain.holder (an empty holder cannot claim)",
                    ));
                }
                if self.drain.holder == default_holder() {
                    return Err(invalid(format!(
                        "storage.event_store: postgres requires an explicit unique drain.holder — the default `{}` is refused: two replicas sharing a holder would defeat same-holder claim admission (ADR-009)",
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

        // read_model.snapshots: sane bounds (fail-closed, never a
        // silently-degenerate maintenance loop).
        {
            let s = &self.read_model.snapshots;
            if s.enabled {
                if s.min_events == 0 {
                    return Err(invalid(
                        "read_model.snapshots.min_events must be >= 1 when snapshots are enabled",
                    ));
                }
                if s.tail_cap == 0 {
                    return Err(invalid(
                        "read_model.snapshots.tail_cap must be >= 1 when snapshots are enabled",
                    ));
                }
                if s.keep_n == 0 {
                    return Err(invalid(
                        "read_model.snapshots.keep_n must be >= 1 when snapshots are enabled",
                    ));
                }
                if s.interval_ms < 100 {
                    return Err(invalid(
                        "read_model.snapshots.interval_ms must be >= 100 (a tighter cadence only burns the maintenance lease)",
                    ));
                }
            }
        }

        // read_model.views: sane bounds (fail-closed, same doctrine as
        // the snapshot section).
        {
            let v = &self.read_model.views;
            if v.enabled && v.interval_ms < 100 {
                return Err(invalid(
                    "read_model.views.interval_ms must be >= 100 (a tighter cadence only burns the maintenance lease)",
                ));
            }
        }

        // retention (READ-MODEL §7, P2b): opt-in and fail-closed. An
        // enabled retention without an archive directory, or without
        // any online-window threshold at all, refuses to start — an
        // ambiguous "archive everything now" is never a default.
        {
            let r = &self.retention;
            if r.enabled {
                if r.archive_dir.trim().is_empty() {
                    return Err(invalid(
                        "retention.archive_dir is required when retention is enabled",
                    ));
                }
                if r.min_age_ms.is_none() && r.min_bytes.is_none() {
                    return Err(invalid(
                        "retention requires at least one online-window threshold \
                         (min_age_ms and/or min_bytes) when enabled",
                    ));
                }
            }
        }

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
        self.validate_agent_permissions(&self.agent, "agent")?;

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

        // the bundle plugins are presence-locked to their config
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
        // the async pair routes through the queue — gating it
        // on without endpoints would mount tools with nowhere to run.
        if self.superagent.as_ref().is_some_and(|sa| sa.async_tools) && self.endpoints.is_empty() {
            return Err(invalid(
                "`superagent.async_tools` requires a non-empty `endpoints:` list \
                 (the async start/stop pair routes through the remote-exec queue)",
            ));
        }
        if has_superagent_plugin && self.plugins.iter().any(|p| p == "react-loop") {
            return Err(invalid(
                "plugin `react-loop` is the baseline loop; with the \
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
            self.validate_agent_permissions(agent, &format!("agents.{name}"))?;
        }

        // The memory section (round-8 plan, kian-87): present parses
        // fail-closed and cross-references configured agents. Absent is
        // memory off — nothing below runs.
        if let Some(memory) = &self.memory {
            if self.superagent.is_some() {
                return Err(invalid(
                    "memory: the agent-memory bundle (Phase 1) assembles on the \
                     built-in assembly; a `superagent:` section alongside `memory:` \
                     is refused — fail-closed, never half-assembled",
                ));
            }
            if memory.scope.trim().is_empty() || memory.scope.contains('"') {
                return Err(invalid(
                    "memory.scope must be a non-empty agent id without double quotes \
                     (it rides the dream prompt header verbatim)",
                ));
            }
            if self.dreamer_agent(&memory.scope).is_none() {
                return Err(invalid(format!(
                    "memory.scope `{}` names no configured agent (the default agent id or an `agents:` entry)",
                    memory.scope
                )));
            }
            let dreamer_id = &memory.dreamer.agent;
            let Some(dreamer) = self.dreamer_agent(dreamer_id) else {
                return Err(invalid(format!(
                    "memory.dreamer.agent `{dreamer_id}` names no configured agent (the default agent id or an `agents:` entry)"
                )));
            };
            if !dreamer.model_slots.contains_key("main") {
                return Err(invalid(format!(
                    "memory.dreamer.agent `{dreamer_id}` must define slot `main` (the dream-turn slot)"
                )));
            }
            if memory.dreamer.max_bytes_read == Some(0) {
                return Err(invalid("memory.dreamer.max_bytes_read must be > 0"));
            }
            if memory.dreamer.max_notes == Some(0) {
                return Err(invalid("memory.dreamer.max_notes must be > 0"));
            }
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
                // the `lark:` section is presence-locked to
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
                // the `timer:` section is presence-locked to
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
                // the `github:` section is presence-locked
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
            // at most one `type: github` channel — the single
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
            // the Router judge loop does not evaluate permissions
            // this round (ADR-015) — a table on it would be silently
            // ignored, so it refuses boot.
            if !router_agent.permissions.is_empty() {
                return Err(invalid(format!(
                    "agents.{} carries a permissions table but the Router judge loop does not \
                     evaluate permissions (ADR-015) — remove the section or move it to the \
                     worker/default agent",
                    sa.router.agent
                )));
            }
            // The speculation section must convert cleanly (
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
            // classes cross-reference declared channels and convert
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
            // speak rules convert cleanly and cross-reference
            // declared channels (fail-closed — an unknown condition
            // spelling or an undeclared channel refuses to start).
            sa.speak_policy()?;
            for (i, rule) in sa.speak_rules.iter().enumerate() {
                if !sa.channels.contains_key(&rule.channel) {
                    return Err(invalid(format!(
                        "superagent.speak_rules[{i}] references channel `{}` which is not \
                         declared in superagent.channels",
                        rule.channel
                    )));
                }
            }
            // the policy-version label is header-safe (fail-closed
            // — it rides persisted bootstrap prompts and must parse
            // back unambiguously).
            sa.policy.validate()?;
            // the mandate scope texts are non-blank (fail-closed —
            // an empty authority string is never issued).
            sa.mandate.validate()?;
            // the outbox schedule converts cleanly (fail-closed —
            // a broken retry bound or backoff refuses to start).
            sa.outbox_config()?;
            // the untrusted-channel list cross-references the
            // declared channels and converts cleanly (fail-closed).
            sa.untrusted_channel_set()?;
            // /ADR-013: the intake thread-keys cross-reference the
            // declared channels and convert cleanly (fail-closed —
            // an undeclared channel or an empty key list refuses to
            // start, never a silently dead derivation).
            sa.intake
                .to_runtime(&sa.channels.keys().cloned().collect())?;
            // the watchdog binding cross-references convert
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
                // same as the Router — the watchdog judge loop does
                // not evaluate permissions; a table refuses boot.
                if !watchdog_agent.permissions.is_empty() {
                    return Err(invalid(format!(
                        "agents.{} carries a permissions table but the Watchdog judge loop \
                         does not evaluate permissions (ADR-015) — remove the section",
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

        // The recovery section must parse through 's fail-closed
        // loader (unknown classes / malformed actions are errors).
        self.policy()?;
        Ok(())
    }

    /// Fail-closed permission check for one agent definition: every
    /// `permissions:` key must be present in the agent's `tools:` list —
    /// a policy for a tool the agent cannot call is a config error, never
    /// a silent no-op.
    fn validate_agent_permissions(
        &self,
        agent: &AgentConfig,
        label: &str,
    ) -> Result<(), ConfigError> {
        for key in agent.permissions.keys() {
            if !agent.tools.iter().any(|t| t == key) {
                return Err(invalid(format!(
                    "{label}.permissions key `{key}` is not in {label}.tools — permission keys \
                     must name a tool the agent may call",
                )));
            }
        }
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

    /// Resolves an agent id the memory section may reference: a named
    /// `agents:` entry, or the default `agent:` section's own id.
    /// Anything else is `None` (validation refuses it fail-closed).
    #[must_use]
    pub fn dreamer_agent(&self, id: &str) -> Option<&AgentConfig> {
        self.named_agent(id).or({
            if id == self.agent.id {
                Some(&self.agent)
            } else {
                None
            }
        })
    }

    /// The recovery policy section, parsed fail-closed by.
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

    /// The compiled definition of a named agent (`None` when the id
    /// is not configured).
    #[must_use]
    pub fn named_agent_def(&self, id: &str) -> Option<AgentDef> {
        self.agents.get(id).map(agent_def_of)
    }

    /// The compiled definition an agent id the memory section may
    /// reference resolves to (`None` when it names no configured
    /// agent) — [`Self::named_agent_def`] plus the default agent.
    #[must_use]
    pub fn dreamer_agent_def(&self, id: &str) -> Option<AgentDef> {
        if id == self.agent.id {
            return Some(self.agent_def());
        }
        self.named_agent_def(id)
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
    // -- tools.exec_env / tools.exec_redact -----------------------

    /// Defaults: inherit (compatibility clause) + redaction on.
    #[test]
    fn exec_env_and_redact_defaults_are_inherit_and_enabled() {
        let cfg = ToolsConfig::default();
        assert_eq!(cfg.exec_env.mode, "inherit");
        assert!(cfg.exec_env.allow.is_empty());
        assert!(cfg.exec_redact.enabled);
        assert!(cfg.exec_redact.env_names.is_empty());
        assert_eq!(
            cfg.exec_env.to_policy().unwrap(),
            kian_core::secrets::ExecEnvPolicy::Inherit
        );
    }

    /// An allowlist section parses and converts to the closed-set policy.
    #[test]
    fn exec_env_allowlist_parses_and_converts() {
        let cfg: ToolsConfig = serde_yaml::from_str(
            "root: \".\"\nexec_timeout_ms: 30000\nexec_env:\n  mode: allowlist\n  allow: [PATH, HOME]\n",
        )
        .unwrap();
        assert_eq!(
            cfg.exec_env.to_policy().unwrap(),
            kian_core::secrets::ExecEnvPolicy::allowlist(&["PATH".to_string(), "HOME".to_string()])
        );
    }

    /// Unknown mode is fail-closed, never a silent inherit.
    #[test]
    fn exec_env_unknown_mode_refuses() {
        let cfg = ExecEnvSection {
            mode: "allowlst".into(),
            allow: vec![],
        };
        let err = cfg.to_policy().unwrap_err();
        assert!(err.to_string().contains("allowlst"), "{err}");
    }

    /// deny_unknown_fields holds on both new sections (typo'd keys
    /// refuse boot instead of silently doing nothing).
    #[test]
    fn exec_env_and_redact_reject_unknown_fields() {
        assert!(serde_yaml::from_str::<ExecEnvSection>("mode: inherit\nalow: [PATH]").is_err());
        assert!(serde_yaml::from_str::<ExecRedactSection>("enabled: true\nenvnames: []").is_err());
    }

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

    /// golden fixture: a config with NO `event_store:` key parses to
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

    /// sqlite mode without a path refuses (the legacy required
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

    /// an unknown backend name refuses, never falls back.
    #[test]
    fn unknown_event_store_backend_refuses() {
        let yaml = valid_yaml().replace("storage:\n", "storage:\n  event_store: mysql\n");
        let err = ServeConfig::parse(&yaml).expect_err("unknown backend must refuse");
        assert!(err.to_string().contains("mysql"), "{}", err.to_string());
    }

    /// postgres mode without the `storage.postgres` section refuses.
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

    /// postgres mode parses with an explicit holder and an env-only
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
    // per-call tool permissions (ADR-015)
    // -----------------------------------------------------------------------

    /// an agent with no `permissions:` section parses to an empty
    /// table (= allow-all, byte-identical behavior).
    #[test]
    fn permissions_absent_defaults_to_empty() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert!(cfg.agent.permissions.is_empty(), "absent = empty = allow");
        let sa = ServeConfig::parse(&superagent_yaml()).expect("superagent config");
        assert!(sa.named_agent("worker").unwrap().permissions.is_empty());
    }

    /// `allow`/`deny` parse into the closed `Permission` vocabulary.
    #[test]
    fn permissions_allow_and_deny_parse() {
        let yaml = valid_yaml().replacen(
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n  permissions:\n    fs_read: allow\n    fs_write: deny\n",
            1,
        );
        let cfg = ServeConfig::parse(&yaml).expect("permissions parse");
        assert_eq!(
            cfg.agent.permissions,
            BTreeMap::from([
                ("fs_read".to_string(), Permission::Allow),
                ("fs_write".to_string(), Permission::Deny),
            ])
        );
    }

    /// (fail-closed): an unknown permission value refuses boot —
    /// the vocabulary is closed; `ask` is deliberately not in it this
    /// round (ADR-015).
    #[test]
    fn permissions_unknown_value_refuses_boot() {
        for bad in ["ask", "maybe", "ALLOW"] {
            let yaml = valid_yaml().replacen(
                "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
                format!("  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n  permissions:\n    fs_read: {bad}\n").as_str(),
                1,
            );
            let err = ServeConfig::parse(&yaml).expect_err("unknown value must refuse boot");
            assert!(
                err.to_string().contains("permissions"),
                "unexpected error: {err}"
            );
        }
    }

    /// (fail-closed): a permission key not present in the agent's
    /// `tools:` list refuses boot — a policy for a tool the agent cannot
    /// call is a config error, never a silent no-op.
    #[test]
    fn permissions_key_absent_from_agent_tools_refuses_boot() {
        let yaml = valid_yaml().replacen(
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n  permissions:\n    not_a_tool: deny\n",
            1,
        );
        let err = ServeConfig::parse(&yaml).expect_err("unknown tool key must refuse boot");
        let msg = err.to_string();
        assert!(msg.contains("permissions"), "unexpected error: {msg}");
        assert!(
            msg.contains("not_a_tool"),
            "must name the offending key: {msg}"
        );
        assert!(msg.contains("tools"), "must point at the tools list: {msg}");

        // Same discipline for a named agent (the worker template path).
        let sa = superagent_yaml().replacen(
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n    permissions:\n      exec: deny\n",
            1,
        );
        // (exec IS a worker tool — the parse must succeed; the refusal
        // case below uses a name the worker does not list.)
        let parsed = ServeConfig::parse(&sa).expect("worker permissions on a listed tool parse");
        assert_eq!(
            parsed.named_agent("worker").unwrap().permissions["exec"],
            Permission::Deny
        );
        let sa_bad = superagent_yaml().replacen(
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n    permissions:\n      spawn_worker: deny\n",
            1,
        );
        let err = ServeConfig::parse(&sa_bad).expect_err("unlisted key must refuse boot");
        assert!(err.to_string().contains("spawn_worker"), "{err}");
    }

    /// (fail-closed): the Router and Watchdog judge loops do not
    /// evaluate permissions this round (ADR-015) — a permissions table
    /// on their agents would be silently ignored, so it refuses boot
    /// instead.
    #[test]
    fn permissions_on_judge_agents_refuse_boot() {
        let router = superagent_yaml().replacen(
            "    tools: [\"spawn_worker\", \"reply\", \"deliver_structured\"]\n",
            "    tools: [\"spawn_worker\", \"reply\", \"deliver_structured\"]\n    permissions:\n      reply: deny\n",
            1,
        );
        let err = ServeConfig::parse(&router).expect_err("router permissions must refuse boot");
        assert!(err.to_string().contains("router"), "{err}");

        let watchdog = watchdog_yaml().replacen(
            "  watchdog:\n    id: watchdog\n    version: 1\n    model_slots:\n      main:\n        provider: openai\n        model: gpt-watchdog\n    tools: []\n",
            "  watchdog:\n    id: watchdog\n    version: 1\n    model_slots:\n      main:\n        provider: openai\n        model: gpt-watchdog\n    tools: []\n    permissions:\n      noop: allow\n",
            1,
        );
        let err = ServeConfig::parse(&watchdog).expect_err("watchdog permissions must refuse boot");
        assert!(err.to_string().contains("watchdog"), "{err}");
    }

    // -----------------------------------------------------------------------
    // super-agent configuration
    // -----------------------------------------------------------------------

    const ROUTER_SESSION: &str = "11111111-2222-3333-4444-555555555555";

    /// A full super-agent profile (the golden shape): named router
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
    // watchdog + timer configuration
    // -------------------------------------------------------------------

    const WATCHDOG_SESSION: &str = "99999999-8888-7777-6666-555555555555";

    /// The golden profile plus the surface: a watchdog agent and
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

    /// the `superagent.recall:` section is additive — a legacy
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

    /// the untrusted-channel list parses, defaults to empty
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

    /// fail-closed: an entry naming an undeclared channel refuses
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
    // the lark channel wire layer
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
            "real channels are untrusted by default"
        );

        // Without any lark channel the runtime set equals the legacy
        // configured set exactly (behavior unchanged).
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

    /// the `classes:` wire layer parses, converts fail-closed to
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
        // note: the fixture originally used `lark` as the unknown
        // type; lark is a real adapter now. note: same story for
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
    // the github channel + webhook receiver wire layer
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
        // and is untrusted by construction (no opt-out).
        let runtime = github.to_runtime("github", None).expect("converts");
        assert!(runtime.untrusted);
        assert_eq!(runtime.token, None);

        // github-typed channels join the runtime untrusted set
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
            err.to_string().contains("react-loop` is the baseline loop"),
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
    fn baseline_config_without_superagent_section_still_parses() {
        // The fixture (no agents/superagent/endpoints keys at all)
        // must parse identically — the additions are all optional.
        let cfg = ServeConfig::parse(&valid_yaml()).expect("phase-1 config");
        assert!(cfg.superagent.is_none());
        assert!(cfg.agents.is_empty());
        assert!(cfg.endpoints.is_empty());
        assert!(!cfg.agent.evidence_gate.enabled);
    }

    // ------------------------------------------------------------------
    // the speculation section
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

    // ------------------------------------------------------------------
    // the superagent.speak_rules section
    // ------------------------------------------------------------------

    #[test]
    fn speak_rules_section_parses_converts_and_defaults_absent() {
        let yaml = superagent_yaml().replace(
            "  subscriptions:\n",
            "  speak_rules:\n    - channel: mock-im\n      when: ack-first-only\n      per_topic_ack: true\n    - channel: ci\n      when: result-ready\n  subscriptions:\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("speak rules config");
        let sa = cfg.superagent.as_ref().unwrap();
        let policy = sa.speak_policy().expect("converts");
        let mock_rule = policy.rule_for("mock-im").expect("mock-im gated");
        assert_eq!(
            mock_rule.when,
            kian_superagent::SpeakCondition::AckFirstOnly
        );
        assert!(mock_rule.per_topic_ack);
        let ci_rule = policy.rule_for("ci").expect("ci gated");
        assert_eq!(ci_rule.when, kian_superagent::SpeakCondition::ResultReady);
        assert!(!ci_rule.per_topic_ack);
        assert!(policy.rule_for("elsewhere").is_none());

        // Absent table = empty policy: every send passes (today's
        // behavior, byte-identical).
        let cfg = ServeConfig::parse(&superagent_yaml()).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(
            sa.speak_policy().unwrap(),
            kian_superagent::SpeakPolicy::default()
        );

        // Unknown condition spelling refuses to start (fail-closed,
        // vocabulary named in the error).
        let bad = superagent_yaml().replace(
            "  subscriptions:\n",
            "  speak_rules:\n    - channel: mock-im\n      when: sometimes\n  subscriptions:\n",
        );
        let err = ServeConfig::parse(&bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("superagent.speak_rules[0].when `sometimes`"),
            "{err}"
        );

        // Empty channel spelling refuses to start.
        let bad = superagent_yaml().replace(
            "  subscriptions:\n",
            "  speak_rules:\n    - channel: \" \"\n      when: never\n  subscriptions:\n",
        );
        let err = ServeConfig::parse(&bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("superagent.speak_rules[0].channel must be non-empty"),
            "{err}"
        );

        // Unknown keys inside a rule fail closed like the rest of the
        // config surface.
        let bad = superagent_yaml().replace(
            "  subscriptions:\n",
            "  speak_rules:\n    - channel: mock-im\n      when: never\n      volume: 3\n  subscriptions:\n",
        );
        assert!(ServeConfig::parse(&bad).is_err());
    }

    #[test]
    fn speak_rules_cross_reference_declared_channels() {
        // A rule gating a channel that is not declared refuses to
        // start (the subscriptions/classes cross-ref discipline).
        let bad = superagent_yaml().replace(
            "  subscriptions:\n",
            "  speak_rules:\n    - channel: nope\n      when: never\n  subscriptions:\n",
        );
        let err = ServeConfig::parse(&bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("superagent.speak_rules[0] references channel `nope`"),
            "{err}"
        );
    }

    // ------------------------------------------------------------------
    // the superagent.policy section
    // ------------------------------------------------------------------

    #[test]
    fn policy_section_parses_converts_and_defaults_absent() {
        // Present: the label converts to a version fingerprinted over
        // the resident prompt templates.
        let yaml = superagent_yaml().replace(
            "  subscriptions:\n",
            "  policy:\n    version: 2026-11.1\n    notify_live: true\n  subscriptions:\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("policy config");
        let sa = cfg.superagent.as_ref().unwrap();
        assert!(sa.policy.notify_live);
        let version = sa.policy_version().expect("version converts");
        assert_eq!(version.label(), "2026-11.1");
        let hashes = version.prompt_hashes();
        assert_eq!(hashes.len(), 2, "judge + watchdog templates");
        assert!(hashes.contains_key("judge"));
        assert!(hashes.contains_key("watchdog"));

        // Absent: no version (today's bytes, byte-identical), notices
        // off.
        let cfg = ServeConfig::parse(&superagent_yaml()).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(sa.policy, super::PolicyVersionConfig::default());
        assert!(sa.policy_version().is_none());
    }

    #[test]
    fn policy_version_label_validation_fails_closed() {
        for bad in ["", "v1 x", "v1]x", "new\nline"] {
            let yaml = superagent_yaml().replace(
                "  subscriptions:\n",
                &format!("  policy:\n    version: {bad:?}\n  subscriptions:\n"),
            );
            let err = ServeConfig::parse(&yaml).unwrap_err();
            assert!(
                err.to_string().contains("superagent.policy.version"),
                "label {bad:?} must refuse: {err}"
            );
        }
        // Every header-safe spelling is accepted.
        for good in ["v1", "2026-11.1", "a-b_c.d+e"] {
            let yaml = superagent_yaml().replace(
                "  subscriptions:\n",
                &format!("  policy:\n    version: {good}\n  subscriptions:\n"),
            );
            let parsed = ServeConfig::parse(&yaml);
            assert!(
                parsed.is_ok(),
                "label {good} must parse: {:?}",
                parsed.err()
            );
        }
    }

    // ------------------------------------------------------------------
    // the superagent.mandate section
    // ------------------------------------------------------------------

    #[test]
    fn mandate_section_parses_converts_and_defaults_absent() {
        // Present: the per-template scopes convert to the issuance
        // policy.
        let yaml = superagent_yaml().replace(
            "  subscriptions:\n",
            concat!(
                "  mandate:\n",
                "    scopes:\n",
                "      kian-worker: draft a triage comment; never post\n",
                "    default_scope: bounded default work only\n",
                "  subscriptions:\n",
            ),
        );
        let cfg = ServeConfig::parse(&yaml).expect("mandate config");
        let sa = cfg.superagent.as_ref().unwrap();
        let policy = sa.mandate_policy().expect("policy converts");
        assert_eq!(
            policy.scope_for("kian-worker"),
            Some("draft a triage comment; never post")
        );
        assert_eq!(
            policy.scope_for("any-other-template"),
            Some("bounded default work only")
        );

        // Absent: no mandate issued (byte-identical legacy wrappers).
        let cfg = ServeConfig::parse(&superagent_yaml()).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(sa.mandate, super::MandateConfig::default());
        assert!(sa.mandate_policy().is_none());
    }

    #[test]
    fn mandate_scope_validation_fails_closed() {
        for bad in [
            ("      kian-worker: \"  \"\n", "blank scope text"),
            ("      \" \": do the work\n", "blank template key"),
        ] {
            let yaml = superagent_yaml().replace(
                "  subscriptions:\n",
                &format!("  mandate:\n    scopes:\n{}  subscriptions:\n", bad.0),
            );
            let err = ServeConfig::parse(&yaml).unwrap_err();
            assert!(
                err.to_string().contains("superagent.mandate"),
                "{} must refuse: {err}",
                bad.1
            );
        }
        // A blank default scope refuses too.
        let yaml = superagent_yaml().replace(
            "  subscriptions:\n",
            "  mandate:\n    default_scope: \" \"\n  subscriptions:\n",
        );
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("superagent.mandate.default_scope"),
            "{err}"
        );
    }

    // -- memory section (round 8, kian-87) ------------------------

    /// A minimal memory block: the default agent as both scope and
    /// dreamer, caps defaulting.
    fn memory_yaml() -> String {
        valid_yaml().replace(
            "drain:",
            "memory:\n  scope: default\n  dreamer:\n    agent: default\ndrain:",
        )
    }

    /// Absent block = memory off (None), byte-identical legacy shape.
    #[test]
    fn memory_absent_block_is_off() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert!(cfg.memory.is_none());
    }

    /// A memory block naming configured agents parses and resolves.
    #[test]
    fn memory_block_parses_and_resolves_agents() {
        let cfg = ServeConfig::parse(&memory_yaml()).expect("valid config");
        let memory = cfg.memory.as_ref().expect("memory on");
        assert_eq!(memory.scope, "default");
        assert_eq!(memory.dreamer.agent, "default");
        assert_eq!(memory.dreamer.caps(), kian_memory::MemoryConfig::default());
        assert!(cfg.dreamer_agent("default").is_some());
        assert!(cfg.dreamer_agent("nope").is_none());
    }

    /// Explicit caps ride through; a named dreamer agent resolves too.
    #[test]
    fn memory_explicit_caps_and_named_dreamer_parse() {
        let yaml = valid_yaml().replace(
            "drain:",
            "agents:\n  dreamer:\n    id: dreamer\n    model_slots:\n      main:\n        provider: openai\n        model: gpt-cheap\nmemory:\n  scope: default\n  dreamer:\n    agent: dreamer\n    max_bytes_read: 1024\n    max_notes: 5\ndrain:",
        );
        let cfg = ServeConfig::parse(&yaml).expect("valid config");
        let caps = cfg.memory.as_ref().expect("memory on").dreamer.caps();
        assert_eq!(caps.max_bytes_read, 1024);
        assert_eq!(caps.max_notes, 5);
    }

    /// The fail-closed matrix: unknown keys, blank/quoted scope,
    /// unresolvable agents, missing `main` slot, zero caps, and the
    /// superagent mix — every one refuses.
    #[test]
    fn memory_validation_is_fail_closed() {
        let cases = [
            (
                "unknown key",
                "memory:\n  scope: default\n  cadence: hourly\n  dreamer:\n    agent: default\n",
                "cadence",
            ),
            (
                "unknown dreamer key",
                "memory:\n  scope: default\n  dreamer:\n    agent: default\n    model: x\n",
                "model",
            ),
            (
                "blank scope",
                "memory:\n  scope: \" \"\n  dreamer:\n    agent: default\n",
                "memory.scope",
            ),
            (
                "quoted scope",
                "memory:\n  scope: \"no\\\"good\"\n  dreamer:\n    agent: default\n",
                "memory.scope",
            ),
            (
                "scope names no agent",
                "memory:\n  scope: ghost\n  dreamer:\n    agent: default\n",
                "memory.scope `ghost`",
            ),
            (
                "dreamer names no agent",
                "memory:\n  scope: default\n  dreamer:\n    agent: ghost\n",
                "memory.dreamer.agent `ghost`",
            ),
            (
                "dreamer without main slot",
                "agents:\n  dreamer:\n    id: dreamer\n    model_slots:\n      side:\n        provider: openai\n        model: gpt-cheap\nmemory:\n  scope: default\n  dreamer:\n    agent: dreamer\n",
                "slot `main`",
            ),
            (
                "zero max_bytes_read",
                "memory:\n  scope: default\n  dreamer:\n    agent: default\n    max_bytes_read: 0\n",
                "max_bytes_read",
            ),
            (
                "zero max_notes",
                "memory:\n  scope: default\n  dreamer:\n    agent: default\n    max_notes: 0\n",
                "max_notes",
            ),
        ];
        for (label, block, needle) in cases {
            let yaml = format!("{block}\n{}", valid_yaml());
            let err = ServeConfig::parse(&yaml).unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "{label} must refuse with `{needle}`: {err}"
            );
        }
    }

    /// `memory:` alongside `superagent:` refuses (the P1 bundle
    /// assembles on the built-in assembly only — fail-closed, never
    /// half-assembled).
    #[test]
    fn memory_refuses_the_superagent_mix() {
        // Append the memory block to a full superagent config.
        let yaml = format!(
            "{}\nmemory:\n  scope: kian-router\n  dreamer:\n    agent: kian-router\n",
            superagent_yaml()
        );
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("superagent"),
            "the mix must refuse: {err}"
        );
    }

    // -- read_model.snapshots (READ-MODEL P1) ----------------------

    /// Defaults: snapshots enabled with the conservative threshold
    /// (READ-MODEL plan §4.2 req 5); absent section = defaults.
    #[test]
    fn read_model_defaults_are_enabled_and_conservative() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        let s = &cfg.read_model.snapshots;
        assert!(s.enabled);
        assert_eq!(s.min_events, kian_orchestrator::DEFAULT_MIN_EVENTS);
        assert_eq!(s.tail_cap, kian_orchestrator::DEFAULT_TAIL_CAP);
        assert_eq!(s.keep_n, kian_orchestrator::DEFAULT_KEEP_N);
        assert!(s.interval_ms >= 100);
        cfg.validate().expect("defaults validate");
    }

    /// An explicit section parses; disabling validates (the gate hides
    /// every snapshot); unknown keys refuse (fail-closed).
    #[test]
    fn read_model_section_parses_and_gates() {
        let yaml =
            valid_yaml() + "read_model:\n  snapshots:\n    enabled: false\n    min_events: 5\n";
        let cfg = ServeConfig::parse(&yaml).expect("parse");
        assert!(!cfg.read_model.snapshots.enabled);
        assert_eq!(cfg.read_model.snapshots.min_events, 5);
        // Disabling skips the numeric sanity gates entirely.
        cfg.validate().expect("disabled validates");

        let bad =
            valid_yaml() + "read_model:\n  snapshots:\n    enabled: true\n    min_events: 0\n";
        let err = ServeConfig::parse(&bad).expect_err("min_events 0 must refuse");
        assert!(err.to_string().contains("min_events"), "{err}");

        let typo = valid_yaml() + "read_model:\n  snapshots:\n    enbled: true\n";
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "deny_unknown_fields must refuse the typo'd key"
        );
    }

    // -- read_model.views (READ-MODEL P2a) --------------------------

    /// Defaults: views enabled with a sane cadence; absent section =
    /// defaults (operator surfaces consult fresh views, honest fallback
    /// otherwise).
    #[test]
    fn read_model_views_default_enabled() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        let v = &cfg.read_model.views;
        assert!(v.enabled);
        assert!(v.interval_ms >= 100);
        cfg.validate().expect("defaults validate");
    }

    /// An explicit `read_model.views` section parses; disabling
    /// validates (the gate hides every view); unknown keys refuse;
    /// an enabled-but-too-tight cadence refuses (fail-closed).
    #[test]
    fn read_model_views_section_parses_and_gates() {
        let yaml = format!(
            "{}read_model:
  views:
    enabled: false
    interval_ms: 5000
",
            valid_yaml()
        );
        let cfg = ServeConfig::parse(&yaml).expect("parse");
        assert!(!cfg.read_model.views.enabled);
        assert_eq!(cfg.read_model.views.interval_ms, 5000);
        cfg.validate().expect("disabled validates");

        let bad = format!(
            "{}read_model:
  views:
    enabled: true
    interval_ms: 1
",
            valid_yaml()
        );
        let err = ServeConfig::parse(&bad).expect_err("interval_ms 1 must refuse while enabled");
        assert!(
            err.to_string().contains("read_model.views.interval_ms"),
            "{err}"
        );

        let typo = format!(
            "{}read_model:
  views:
    enbled: true
",
            valid_yaml()
        );
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "deny_unknown_fields must refuse the typo'd key"
        );
    }

    // -- retention (READ-MODEL §7, P2b) ------------------------------

    /// Default: retention OFF — nothing is archived, nothing is
    /// deleted, byte-identical to pre-P2b (the owner gate).
    #[test]
    fn retention_defaults_to_off() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert!(!cfg.retention.enabled);
        assert!(cfg.retention.archive_dir.is_empty());
        assert!(cfg.retention.min_age_ms.is_none());
        assert!(cfg.retention.min_bytes.is_none());
        cfg.validate().expect("defaults validate");
    }

    /// An enabled retention requires an archive dir and at least one
    /// online-window threshold — both refusals are fail-closed, and an
    /// unknown key refuses too.
    #[test]
    fn retention_enabled_parses_fail_closed() {
        // No archive dir: refuses (parse validates fail-closed).
        let bad = valid_yaml() + "retention:\n  enabled: true\n  min_age_ms: 1000\n";
        let err = ServeConfig::parse(&bad).expect_err("enabled without archive_dir must refuse");
        assert!(err.to_string().contains("archive_dir"), "{err}");

        // No threshold at all: refuses.
        let bad = valid_yaml() + "retention:\n  enabled: true\n  archive_dir: /tmp/a\n";
        let err = ServeConfig::parse(&bad).expect_err("no threshold must refuse");
        assert!(err.to_string().contains("threshold"), "{err}");

        // Fully specified: validates.
        let good = valid_yaml()
            + "retention:\n  enabled: true\n  archive_dir: /tmp/kian-archive\n  min_age_ms: 86400000\n  min_bytes: 10485760\n";
        let cfg = ServeConfig::parse(&good).expect("parse");
        assert!(cfg.retention.enabled);
        assert_eq!(cfg.retention.min_age_ms, Some(86_400_000));
        assert_eq!(cfg.retention.min_bytes, Some(10_485_760));
        cfg.validate().expect("fully specified validates");

        // A disabled retention with vague values still validates (the
        // gate is `enabled`, and off means off).
        let off = valid_yaml() + "retention:\n  archive_dir: \"\"\n";
        let cfg = ServeConfig::parse(&off).expect("parse");
        assert!(!cfg.retention.enabled);
        cfg.validate().expect("off validates regardless");

        // Unknown key: refuses (deny_unknown_fields).
        let typo = valid_yaml() + "retention:\n  enbled: true\n";
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "deny_unknown_fields must refuse the typo'd key"
        );
    }
}
