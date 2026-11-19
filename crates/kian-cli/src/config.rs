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
    /// The confirmation plane policy (CONFIRMATION-MODEL §8, ADR-033):
    /// token-independent settings for `ask`. Absent = no expiry.
    #[serde(default)]
    pub confirmation: ConfirmationConfig,
    /// Remote execution endpoints (over the queue). Empty = no
    /// remote exec.
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    /// The `workers:` section (R12): the dynamic-membership
    /// registration listener. `None` (the section absent) is dynamic
    /// membership **off** — no listener, no directory, no runtime
    /// routes, byte-identical pre-R12 behavior everywhere (including
    /// with `endpoints:` present: static pins behave exactly as
    /// before). Present parses fail-closed (unknown keys refuse) and
    /// gates the fail-closed startup validations (C2: private bind,
    /// env-only token, positive `offline_after_ms`).
    #[serde(default)]
    pub workers: Option<WorkersConfig>,
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
    /// The `sidecars:` section (PLUGIN-SPEC §8.1/§13.1). `None` (the
    /// section absent) is sidecars **off**: no runner spawns, no bus
    /// runtime installs, no ports gain a subscriber — byte-identical
    /// behavior (§13.3). Present parses the closed two-scope grammar
    /// fail-closed (an unknown scope key — `harness:`, `profile:` —
    /// refuses boot naming the key, §8.1/§6.2 boundary case 2) and gates
    /// the boot checks: every set entry a declared instance, `use ⊆
    /// scopes.server`, limits naming receivable instances and narrowing
    /// only (§8.2; the widening check runs in the serve assembly, where
    /// the manifests are loaded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecars: Option<kian_plugin::hooks::resolve::SidecarConfig>,
    /// The `api:` section (READ-MODEL §4.2, P3). Absent = the defaults:
    /// the fan-in event stream enabled, explicit session allowlists
    /// only (no wildcard in OSS).
    #[serde(default)]
    pub api: ApiConfig,
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
    /// The source memory session (P2 plan §2.1): the session whose
    /// `memory/note` stream is rendered into new sessions at creation
    /// time (MEMORY-MODEL §6 injection). A uuid string; **absent =
    /// injection off** — sessions are byte-identical to a memory-off
    /// server (I9).
    #[serde(default)]
    pub session: Option<String>,
    /// The injection caps (MEMORY-MODEL §6 I4). Requires `session` (a
    /// caps block without a source session is a configuration mistake
    /// and refuses, never silently ignored).
    #[serde(default)]
    pub inject: Option<InjectSection>,
}

/// The `memory.inject` block (P2 plan §2.1): the injection caps.
/// Absent keys default to the bundle defaults
/// (`kian_memory::MemoryIndexConfig::default()`); present values must
/// be positive (fail-closed in [`ServeConfig::validate`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectSection {
    /// Maximum entries a rendered index carries (default 20); over cap
    /// the renderer degrades to subject + one-line excerpt. Must be > 0.
    #[serde(default)]
    pub max_items: Option<u64>,
    /// Hard byte ceiling of the rendered index text (default 4096).
    /// Must be > 0.
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

impl InjectSection {
    /// The caps in force (explicit values or the bundle defaults).
    /// The values were already validated positive; this only fills
    /// defaults — the same discipline as [`DreamerSection::caps`].
    #[must_use]
    pub fn caps(&self) -> kian_memory::MemoryIndexConfig {
        kian_memory::MemoryIndexConfig {
            max_items: self
                .max_items
                .unwrap_or(kian_memory::DEFAULT_INJECT_MAX_ITEMS),
            max_bytes: self
                .max_bytes
                .unwrap_or(kian_memory::DEFAULT_INJECT_MAX_BYTES),
        }
    }
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
    /// ReAct iteration budget per session; resident loops (Router/watchdog) budget it per
    /// episode instead (ADR-024).
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

/// The confirmation plane (CONFIRMATION-MODEL §8, ADR-033): how long a
/// pending ask may wait before an answer degrades honestly to `expired`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmationConfig {
    /// Maximum age of a pending ask, milliseconds. Absent or 0 = no
    /// expiry (the operator answers whenever). A late answer is
    /// admitted as a durable reject with `expired: true` so the
    /// session resumes; it is never silently dropped.
    #[serde(default)]
    pub ttl_ms: u64,
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
// the `api` section (READ-MODEL §4.2, P3 — the fan-in event stream)
// ---------------------------------------------------------------------------

/// The `api:` section. The `event_stream` gate is default **on**: the
/// fan-in stream is a read-only observability surface behind the same
/// v0 trust boundary (API-CONTRACT §2.4 — the deployment is the
/// boundary; the allowlist is explicit session ids only, never a
/// wildcard). Disabling mounts no `/events` route — the path 404s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// The `api.event_stream:` subsection.
    #[serde(default)]
    pub event_stream: EventStreamSection,
}

/// The `api.event_stream:` subsection (READ-MODEL §4.2 / API-CONTRACT
/// §5.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventStreamSection {
    /// Whether `GET /events` (the explicit-allowlist fan-in stream) is
    /// mounted. Default `true`.
    #[serde(default = "default_event_stream_enabled")]
    pub enabled: bool,
}

impl Default for EventStreamSection {
    fn default() -> Self {
        Self {
            enabled: default_event_stream_enabled(),
        }
    }
}

fn default_event_stream_enabled() -> bool {
    true
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
    /// (R15) the multiagent delegation bounds
    /// (`superagent.multiagent`): `max_children`/`max_depth`/
    /// `child_timeout_ms`, every field optional with the §5.3
    /// defaults. **Absent (the default) = the delegation feature is
    /// off** — today's spawn bytes, byte-identical (M8, §8.1).
    /// Present (even empty) enables it; converted fail-closed by
    /// [`SuperAgentConfig::multiagent_config`].
    #[serde(default)]
    pub multiagent: Option<MultiagentConfigDef>,
    /// (R17-C1) the roster (`superagent.roster`, MULTIAGENT-MODEL
    /// §3.1/§7.1): the named agents the Router may delegate to.
    /// Absent/empty (the default) = the roster feature is **off** —
    /// no `list_agents` tool, no roster validation, today's exact
    /// bytes (pin P2-6, §3.1:366-367). Converted fail-closed by
    /// [`ServeConfig::roster_config`].
    #[serde(default)]
    pub roster: Vec<RosterEntryDef>,
    /// (R17 C4) the task-board switch block
    /// (`superagent.board`): `enabled`/`gating`, **both default
    /// `false`**. Absent block = board off: no `task_board` view is
    /// published or served and any rows from an earlier era are
    /// deleted by the next rebuild (a disposable view,
    /// `anchors.md` rule 4). Converted fail-closed by
    /// [`SuperAgentConfig::board_config`] — `gating: true` is a P4
    /// key and refuses to boot in P3.
    #[serde(default)]
    pub board: BoardConfigDef,
    /// /ADR-013 thread-key intake (`superagent.intake`): the
    /// config-owned canonical thread-key derivations. The derived key
    /// surfaces in the Inline `neighbors` block (`thread_key`) as
    /// recall context — correlation is context only, never a routing
    /// input (routing stays topic-keyed; poll-mode github topics are
    /// the derived keys themselves, produced at adapter mapping time).
    /// Absent = no derivation, byte-identical wrappers.
    #[serde(default)]
    pub intake: IntakeConfig,
    /// (CRON-MODEL §3) the declared cron schedules (`superagent.cron`).
    /// Absent (the default) = no cron schedules declared, nothing to
    /// fire. Validated fail-closed at load by [`CronSection::to_runtime`]
    /// (expression, timezone, id uniqueness, target cross-reference,
    /// bounds, the unsupported-policy gates) and by
    /// [`reject_cron_targeting_the_watchdog`].
    #[serde(default)]
    pub cron: CronSection,
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

    /// Converts the multiagent section to the bundle's
    /// [`kian_superagent::MultiagentLimits`], fail-closed (a zero
    /// bound refuses to start, naming the key). `Ok(None)` when the
    /// block is absent — the delegation feature is off, today's
    /// spawn bytes byte-identical (M8, §8.1).
    pub fn multiagent_config(
        &self,
    ) -> Result<Option<kian_superagent::MultiagentLimits>, ConfigError> {
        let Some(config) = self.multiagent.as_ref() else {
            return Ok(None);
        };
        let converted = kian_superagent::MultiagentLimits {
            max_children: config
                .max_children
                .unwrap_or(kian_superagent::DEFAULT_MAX_CHILDREN),
            max_depth: config
                .max_depth
                .unwrap_or(kian_superagent::DEFAULT_MAX_DEPTH),
            child_timeout_ms: config
                .child_timeout_ms
                .unwrap_or(kian_superagent::DEFAULT_CHILD_TIMEOUT_MS),
        };
        converted
            .validate()
            .map_err(|message| invalid(format!("superagent.multiagent: {message}")))?;
        // (R17-C7, pin P4-5) the `join` fan-in policy: v1 evaluates
        // only `all` — the one shape with an evaluation site (one
        // spawn, one parent). `any`/`quorum:<n>` refuse to boot
        // naming the missing evaluation site and the §9 deferral;
        // anything else is an unknown value naming the accepted
        // forms. Never accepted-and-ignored.
        if let Some(join) = config.join.as_deref() {
            match join {
                "all" => {}
                "any" => {
                    return Err(invalid(
                        "superagent.multiagent: join: `any` has no evaluation site in v1 (one \
                         spawn, one parent — only `all` is evaluated); fan-in is deferred with \
                         its own ADR (MULTIAGENT-MODEL §9) — refused rather than \
                         accepted-and-ignored"
                            .to_string(),
                    ));
                }
                value if value.starts_with("quorum:") => {
                    let n = &value["quorum:".len()..];
                    // n ≥ 1: empty, non-numeric and zero are all
                    // typos of the same bound (fail-closed).
                    if !matches!(n.parse::<u32>(), Ok(v) if v >= 1) {
                        return Err(invalid(format!(
                            "superagent.multiagent: join: `{value}` — a quorum needs n ≥ 1 \
                             (got `{n}`)"
                        )));
                    }
                    return Err(invalid(format!(
                        "superagent.multiagent: join: `{value}` has no evaluation site in v1 \
                         (one spawn, one parent — only `all` is evaluated); fan-in is deferred \
                         with its own ADR (MULTIAGENT-MODEL §9) — refused rather than \
                         accepted-and-ignored"
                    )));
                }
                other => {
                    return Err(invalid(format!(
                        "superagent.multiagent: join: `{other}` is not one of the accepted \
                         values `all` | `any` | `quorum:<n>` (n ≥ 1); v1 evaluates `all` only"
                    )));
                }
            }
        }
        Ok(Some(converted))
    }

    /// Converts the board section to the runtime switch, fail-closed
    /// (R17 C4, pin P3-1; R17 C6 made `gating` live). The block
    /// defaults to **off** (`enabled: false`, `gating: false` — an
    /// absent block is the same shape), so absence keeps P3
    /// byte-identical: no `task_board` view, no board output, no
    /// class-A key. `gating: true` arms the P4 emission switch
    /// (§8.1) — but only when the `multiagent` block is present
    /// (§7.1:737-741: neither board key can enable anything
    /// `multiagent` has not already enabled — a refusal, never a
    /// silent no-op).
    pub fn board_config(&self) -> Result<BoardConfig, ConfigError> {
        let converted = BoardConfig {
            enabled: self.board.enabled,
            gating: self.board.gating,
        };
        converted
            .validate()
            .map_err(|message| invalid(format!("superagent.board: {message}")))?;
        if converted.gating && self.multiagent.is_none() {
            return Err(invalid(
                "superagent.board: gating: true requires the superagent.multiagent block — \
                 neither board key can enable anything multiagent has not already enabled \
                 (MULTIAGENT-MODEL §7.1)"
                    .to_string(),
            ));
        }
        Ok(converted)
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

/// The `superagent.multiagent` section (MULTIAGENT-MODEL §7.1):
/// `{max_children?, max_depth?, child_timeout_ms?, join?}` — every
/// field optional with the §5.3 defaults (4 children, 1 hop, 15
/// minutes, `join: all`); an empty section enables the feature with
/// the defaults. The spelling is **`child_timeout_ms`** (pin P4): the
/// anchors' A2 error would name `timeout_ms`, and this struct's
/// `#[serde(deny_unknown_fields)]` makes that typo a boot refusal,
/// never a silent default. Absent section = the feature is off.
///
/// `join` (R17-C7, pin P4-5) is the fan-in policy: `all` (the
/// default) is the only value v1 has an evaluation site for (one
/// spawn, one parent); `any`/`quorum:<n>` **refuse to boot** naming
/// the missing evaluation site and the deferral — never
/// accepted-and-ignored, which would imply any-of success (§5.3:588,
/// §9:882-888).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MultiagentConfigDef {
    pub max_children: Option<u32>,
    pub max_depth: Option<u32>,
    pub child_timeout_ms: Option<u64>,
    pub join: Option<String>,
}

/// One `superagent.roster` entry (MULTIAGENT-MODEL §3.1:357-362,
/// §7.1): `{id, description, agent, mandate_template, tools_allow,
/// limits?, output_schema?}`. `id`/`description`/`agent`/
/// `mandate_template`/`tools_allow` are required — a missing
/// `tools_allow` must never silently mean "all tools" (that would be
/// a widening). `limits` defaults to the §5.3 `max_children` (4);
/// `output_schema` is the optional per-entry default expectation
/// (§3.2.1) and must sit inside the closed subset (pin P2-3).
///
/// `agent` names an **`agents:` entry** (resolved by
/// `ServeConfig::named_agent_def`, pin P2-1) — not the default
/// `agent:` and never an invented id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterEntryDef {
    /// The name the model uses; unique across the roster, non-empty.
    pub id: String,
    /// The one line used for selection (≤200 chars, no control
    /// characters — the `WorkerEntry` precedent).
    pub description: String,
    /// An `agents:` entry id — the `AgentDef` reference and the
    /// memory scope (pin P2-1).
    pub agent: String,
    /// The advisory mandate template: `{scope, strength}` with
    /// `strength ∈ {explicit, inferred}` (ADR-012 vocabulary).
    pub mandate_template: RosterMandateTemplateDef,
    /// The tool fence: intersected with the agent's surface — an
    /// entry listing a tool the `AgentDef` lacks **refuses boot**
    /// (M3, §5.1:551-552; never a widening).
    pub tools_allow: Vec<String>,
    /// Per-entry limits; `max_children: 0` is a **leaf** — legal
    /// (§3.1:360-361).
    #[serde(default)]
    pub limits: RosterLimitsDef,
    /// The optional `output_schema` (pin P2-3 subset, ≤8192
    /// canonical-JSON bytes). Absent = no schema gate for this
    /// entry's children.
    #[serde(default)]
    pub output_schema: Option<serde_json::Value>,
}

/// The `mandate_template` block of a roster entry: the advisory
/// scope text (recorded and audited, never a permission — §5.1) and
/// the strength. `strength` is converted fail-closed so a typo
/// refuses boot naming the key, never a silent default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterMandateTemplateDef {
    /// The advisory scope text (never blank — an empty authority
    /// string is never issued).
    pub scope: String,
    /// `explicit` | `inferred` (ADR-012).
    pub strength: String,
}

/// The `limits` block of a roster entry — the per-entry bound the
/// contract defines (§3.1:360-361): `max_children` only, optional
/// with the §5.3 default (4); `0` names a leaf and is legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RosterLimitsDef {
    pub max_children: Option<u32>,
}

/// The `superagent.board` section (R17 C4, MULTIAGENT-MODEL §7.1):
/// `{enabled?, gating?}` — **both keys default `false`**, and an
/// absent section is the same shape as an all-false one (feature-off,
/// byte-identical, `anchors.md` rule 4). `enabled` gates publication
/// of the `task_board` view and (with `read_model.views.enabled`) the
/// P3 verbs; `gating` is P4's class-A emission switch — live since
/// R17 C6, and requiring the `multiagent` block (checked in
/// [`SuperAgentConfig::board_config`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BoardConfigDef {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub gating: bool,
}

/// The converted runtime board switch (R17 C4, pin P3-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoardConfig {
    /// Whether the `task_board` view may be published/served (the
    /// verbs' own gate composes this with `read_model.views.enabled`).
    pub enabled: bool,
    /// P4's class-A key-emission switch (live since R17 C6, pin
    /// P4-1: `false` ⇒ P3 bytes exactly).
    pub gating: bool,
}

impl BoardConfig {
    /// Fail-closed validation of the switch pair itself. Since R17
    /// C6 landed the key groups, `gating: true` boots — the
    /// §7.1 "board cannot enable what multiagent has not" refusal
    /// lives in [`SuperAgentConfig::board_config`], where the
    /// multiagent block is visible.
    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }
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
///
/// (ADR-021 supersedes ADR-020 D7) deployment note: turning
/// membership on (a non-empty `endpoints:` list or a `workers:`
/// section) is prompt-prefix content — the judge prompt gains the
/// frozen worker-membership block and `spawn_worker`'s schema gains
/// the required plain-string `exec_worker` property — so a deployment
/// that enables (or disables) either must pin a **new** `version`
/// label: new sessions bootstrap the new prefix, live sessions keep
/// their persisted prompts byte-identically and the boot drift scan
/// reports the mismatch. Membership *changes* (a worker joining or
/// leaving) never touch the prefix — the block is a constant and the
/// catalog reaches the model only via `list_workers` results.
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
    /// The wechat section — required when (and only when)
    /// `type: wechat` (fail-closed both directions, validated in
    /// [`ServeConfig::parse`]). The bot token lives only in the
    /// credential file written by `kian wechat login` — never here.
    #[serde(default)]
    pub wechat: Option<WechatChannelSection>,
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

/// The `superagent.channels.<name>.wechat:` section
/// (CHANNEL-WECHAT §7). Credentials are deliberately absent: the bot
/// token is issued by the QR flow and lives only in the file at
/// `credentials_file` — written by `kian wechat login`, never in
/// `kian.yaml`, never an env var (ADR-018's recorded deviation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WechatChannelSection {
    /// iLink API base (overridable for private deployments and local
    /// mocks; must be an http(s) URL).
    #[serde(default = "default_wechat_api_base")]
    pub api_base: String,
    /// Path of the credential file written by `kian wechat login`
    /// (required, fail-closed at serve assembly: missing/unreadable/
    /// malformed refuses startup naming the command).
    pub credentials_file: String,
    /// `from_user_id` → routing topic. The keys are exactly the
    /// senders the receive loop admits (the allowlist); at least one
    /// entry is required (fail-closed).
    pub topics: BTreeMap<String, String>,
    /// Client long-poll deadline, milliseconds (the server holds the
    /// request ~35 s; the client waits longer).
    #[serde(default = "default_wechat_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    /// Optional best-effort reply-token state file (ADR-026); defaults
    /// to `<credentials_file>.context-state.json`.
    #[serde(default)]
    pub context_state_file: Option<String>,
    /// How long a persisted reply token stays usable (default 1 h).
    #[serde(default = "default_wechat_context_ttl_ms")]
    pub context_ttl_ms: u64,
}

fn default_wechat_api_base() -> String {
    kian_superagent::WECHAT_DEFAULT_API_BASE.to_string()
}

fn default_wechat_poll_timeout_ms() -> u64 {
    kian_superagent::wechat::DEFAULT_POLL_TIMEOUT.as_millis() as u64
}

fn default_wechat_context_ttl_ms() -> u64 {
    kian_superagent::wechat::context_state::DEFAULT_CONTEXT_TTL.as_millis() as u64
}

impl WechatChannelSection {
    /// Converts to the bundle's runtime config, fail-closed on the
    /// credential file: a missing/unreadable/malformed file is a
    /// startup error naming `kian wechat login` (the operator action)
    /// — never a silent default, never a token in `kian.yaml`.
    pub fn to_runtime(&self, name: &str) -> Result<kian_superagent::WechatConfig, ConfigError> {
        let mut config = kian_superagent::WechatConfig::new(
            self.api_base.clone(),
            self.credentials_file.trim(),
            self.topics.clone(),
        );
        config.poll_timeout = std::time::Duration::from_millis(self.poll_timeout_ms);
        config.context_ttl = std::time::Duration::from_millis(self.context_ttl_ms);
        if let Some(path) = self
            .context_state_file
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            config.context_state_file = path.into();
        }
        config
            .validate()
            .map(|_| ())
            .map_err(|e| invalid(format!("superagent.channels `{name}` (type wechat): {e}")))?;
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
                || channel.channel_type == kian_superagent::WECHAT_CHANNEL_TYPE
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
/// receiver; the wechat iLink channel).
pub const KNOWN_CHANNEL_TYPES: [&str; 5] = ["mock-im", "lark", "timer", "github", "wechat"];

/// The `workers:` section (R12, spec C1): the dynamic-membership
/// registration listener. Workers dial in and self-register; joining or
/// leaving never requires editing this file or restarting `kian serve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersConfig {
    /// The private bind (`host:port`) of the registration listener.
    /// Private ranges only (loopback, RFC1918, CGNAT, `::1`, ULA) —
    /// reached over an SSH tunnel or Tailscale; a public bind refuses
    /// startup unless `allow_public: true` is set explicitly.
    pub listen: String,
    /// The **name** of the environment variable holding the shared
    /// worker token. The value is env-only: it never appears in this
    /// file, argv, logs, events or error text. Optional when `tokens`
    /// enrolls every admitted worker id; `kian serve` refuses to start
    /// when the named variable is absent/blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    /// Liveness bound: a worker is offline once its last heartbeat is
    /// older than this (also the welcome cadence source: /3).
    #[serde(default = "default_workers_offline_after_ms")]
    pub offline_after_ms: u64,
    /// Explicit public-bind override. Never set in any doc example —
    /// it exposes a command-execution surface.
    #[serde(default)]
    pub allow_public: bool,
    /// Backend-level queue bound for dynamic workers
    /// (`workers.queue_timeout_ms`). The backend takes one bound (the
    /// max over static pins and this value), so raising it lengthens
    /// the honest timeout of every queued command.
    #[serde(default = "default_workers_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
    /// How many times a queued command whose result was lost may be
    /// **re-driven** before the row is abandoned (ADR-033). The re-drive is
    /// at-least-once, and this bound is what makes it terminate: a command
    /// that destroys its own ack path (a `systemctl restart kian`, a
    /// `pkill kiand`) can never deliver a result, so the server stops
    /// replaying it after `1 + queue_max_redrives` dispatches and records one
    /// audit line. `0` disables re-drive (first dispatch only). Global by
    /// construction, like the queue timeout: no per-endpoint override.
    #[serde(default = "default_workers_queue_max_redrives")]
    pub queue_max_redrives: u32,
    /// How recently a row must have been dispatched for its re-drive to be
    /// postponed (ADR-037). A route attach re-drives `inflight` rows
    /// immediately; within this grace the driver waits instead, so it does
    /// not race an execution that may still be running on the worker. The
    /// grace only postpones a dispatch — it never settles a row and never
    /// changes the honest queue timeout. `0` disables it.
    #[serde(default = "default_workers_queue_redrive_grace_ms")]
    pub queue_redrive_grace_ms: u64,
    /// How long a worker thread that already ended `interrupted`/`failed`
    /// keeps holding its 1:1 worker slot before the maintenance tick closes
    /// it (`session/ended{cancelled}`, ADR-035). The explicit
    /// `kian workers release <id>` verb frees the slot immediately; `0`
    /// disables the automatic reclaim (the thread then holds the slot until
    /// an operator releases it). The grace exists so an ended thread can
    /// still be inspected/resumed through the API before it is closed.
    #[serde(default = "default_workers_release_after_ms")]
    pub release_after_ms: u64,
    /// Admission mode (phase 1 of the per-worker-identity work): when
    /// true, only ids in `tokens` may register. Requires a non-empty
    /// `tokens` map and forbids `token_env` (no shared fallback — a
    /// half-enrolled deployment must not look strict while accepting a
    /// shared token).
    #[serde(default)]
    pub enrolled_only: bool,
    /// Per-worker credentials: `id -> environment variable name`. Each
    /// listed id authenticates with its own token; a listed id never
    /// accepts the shared token. Values stay env-only.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, String>,
}

fn default_workers_offline_after_ms() -> u64 {
    30_000
}

fn default_workers_queue_timeout_ms() -> u64 {
    60_000
}

fn default_workers_queue_max_redrives() -> u32 {
    1
}

/// The default `workers.queue_redrive_grace_ms` (ADR-037): a row
/// dispatched within the last 5 seconds is not re-driven yet.
pub const DEFAULT_WORKER_QUEUE_REDRIVE_GRACE_MS: u64 = 5_000;

fn default_workers_queue_redrive_grace_ms() -> u64 {
    DEFAULT_WORKER_QUEUE_REDRIVE_GRACE_MS
}

/// The default `workers.release_after_ms` (ADR-035): an idle-ended worker
/// thread is closed after five minutes. `0` disables the automatic reclaim.
pub const DEFAULT_WORKER_RELEASE_AFTER_MS: u64 = 300_000;

fn default_workers_release_after_ms() -> u64 {
    DEFAULT_WORKER_RELEASE_AFTER_MS
}

/// The ceiling on `workers.queue_max_redrives`: the knob bounds a recovery
/// loop, so a typo must not quietly restore unbounded replay.
const MAX_QUEUE_REDRIVES: u32 = 16;

/// The ceiling on `workers.queue_redrive_grace_ms`: a large value would
/// delay every recovery re-drive, so a typo must not quietly park rows.
const MAX_QUEUE_REDRIVE_GRACE_MS: u64 = 60_000;

impl WorkersConfig {
    /// The parsed listener bind (validated fail-closed).
    pub fn listen_addr(&self) -> Result<std::net::SocketAddr, ConfigError> {
        self.listen.parse().map_err(|e| {
            invalid(format!(
                "workers.listen `{}` is not a valid host:port: {e} \
                     (expected `host:port`, e.g. `127.0.0.1:7400`)",
                self.listen
            ))
        })
    }

    /// The C2 validations that are pure config (bind shape, private
    /// range, positive timeouts, token **names**). The token **values**
    /// (env present and non-blank) are startup checks in the serve
    /// assembly, not config validations, so read-only verbs can still
    /// load the file.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let addr = self.listen_addr()?;
        if self.offline_after_ms == 0 {
            return Err(invalid("workers.offline_after_ms must be > 0"));
        }
        if self.queue_timeout_ms == 0 {
            return Err(invalid("workers.queue_timeout_ms must be > 0"));
        }
        if self.queue_redrive_grace_ms > MAX_QUEUE_REDRIVE_GRACE_MS {
            return Err(invalid(format!(
                "workers.queue_redrive_grace_ms must be <= {MAX_QUEUE_REDRIVE_GRACE_MS} (it delays every recovery re-drive)"
            )));
        }
        if self.queue_max_redrives > MAX_QUEUE_REDRIVES {
            return Err(invalid(format!(
                "workers.queue_max_redrives must be <= {MAX_QUEUE_REDRIVES} (it bounds a re-drive loop; a larger value is unbounded replay in practice)"
            )));
        }
        if let Some(name) = &self.token_env
            && name.trim().is_empty()
        {
            return Err(invalid(
                "workers.token_env must name the environment variable holding the worker token",
            ));
        }
        let has_shared = self
            .token_env
            .as_ref()
            .is_some_and(|n| !n.trim().is_empty());
        if self.enrolled_only {
            if self.tokens.is_empty() {
                return Err(invalid(
                    "workers.enrolled_only requires a non-empty `tokens` map",
                ));
            }
            if has_shared {
                return Err(invalid(
                    "workers.enrolled_only forbids `token_env`: a strict deployment must not \
                     also accept a shared token",
                ));
            }
        }
        if !has_shared && self.tokens.is_empty() {
            return Err(invalid(
                "workers: configure `token_env` (shared token) or a non-empty `tokens` map \
                 (per-worker tokens); one is required",
            ));
        }
        for (id, token_env) in &self.tokens {
            if id.trim() != id || !kian_exec::valid_worker_id(id) {
                return Err(invalid(format!(
                    "workers.tokens `{id}`: worker ids are 1..=200 bytes after trim, \
                     no control characters"
                )));
            }
            if token_env.trim().is_empty() {
                return Err(invalid(format!(
                    "workers.tokens `{id}` must name a non-empty environment variable"
                )));
            }
        }
        kian_exec::validate_bind_addr(addr, self.allow_public).map_err(invalid)
    }
}

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
    /// (ADR-020, D5): the optional human-readable description of the
    /// worker — prompt-prefix content (it renders into the Router's
    /// judge prompt and the `spawn_worker` catalog), so it validates
    /// fail-closed: non-empty after trim, at most 200 characters, no
    /// control characters. Absent parses and serializes byte-identically
    /// to the legacy entry.
    #[serde(default)]
    pub description: Option<String>,
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

    /// (ADR-020, D5): the execution-worker catalog handed to the
    /// super-agent assembly — `endpoints[].{id, description}` in
    /// **config order** (the order is the spawn-enum order and the
    /// judge-prompt block order — deterministic). Empty = the feature
    /// is off, nothing changes anywhere.
    #[must_use]
    pub fn worker_catalog(&self) -> Vec<kian_superagent::WorkerEntry> {
        self.endpoints
            .iter()
            .map(|ep| kian_superagent::WorkerEntry {
                id: ep.id.clone(),
                description: ep.description.clone(),
            })
            .collect()
    }

    /// (ADR-020, D5): per-worker-id queue timeouts — the exclusivity
    /// check's never-started release bound.
    #[must_use]
    pub fn worker_queue_timeouts(&self) -> std::collections::BTreeMap<String, u64> {
        self.endpoints
            .iter()
            .map(|ep| (ep.id.clone(), ep.queue_timeout_ms))
            .collect()
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

        // The `sidecars:` section (PLUGIN-SPEC §8.1/§13.1): `plugins:`
        // stays KNOWN_PLUGINS-only (D7) — an instance key colliding
        // with a known plugin name refuses boot (§6.2 rule 3), and the
        // closed two-scope grammar's name-only boot checks run here so
        // a read-only verb catches a typo. The widening check needs the
        // manifests and runs in the serve assembly.
        if let Some(sidecars) = &self.sidecars {
            for name in sidecars.plugins.keys() {
                if KNOWN_PLUGINS.contains(&name.as_str()) {
                    return Err(invalid(format!(
                        "sidecars.plugins key `{name}` collides with a known plugin name \
                         (code: name_collision, §6.2 rule 3)"
                    )));
                }
            }
            if let Err(errors) = kian_plugin::hooks::resolve::validate_shape(sidecars) {
                let rendered: Vec<String> = errors
                    .iter()
                    .map(|e| format!("{} (code: {})", e.message, e.code.as_str()))
                    .collect();
                return Err(invalid(format!("sidecars: {}", rendered.join("; "))));
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
        // (C6) the remote-exec plugin's presence lock now covers the
        // dynamic-membership half too: `workers:` on its own (zero
        // `endpoints:`) still routes commands through the queue.
        let has_workers_surface = !self.endpoints.is_empty() || self.workers.as_ref().is_some();
        if has_workers_surface != has_remote_exec_plugin {
            return Err(invalid(
                "plugin `remote-exec` and a non-empty `endpoints:` list (or a `workers:` \
                 section) must be enabled together",
            ));
        }
        // the async pair routes through the queue — gating it
        // on without any worker surface would mount tools with nowhere
        // to run (C6: the same disjunction).
        if self.superagent.as_ref().is_some_and(|sa| sa.async_tools) && !has_workers_surface {
            return Err(invalid(
                "`superagent.async_tools` requires a non-empty `endpoints:` list or a \
                 `workers:` section (the async start/stop pair routes through the \
                 remote-exec queue)",
            ));
        }
        // (ADR-020, D12; C6): the dsh harness and a worker surface are
        // mutually exclusive — the dsh child never mounts the kian
        // `exec` tool, so a worker binding could not route any command
        // (recording one there would be audit fiction). Zero-endpoint
        // dsh configs boot exactly as before.
        if self
            .superagent
            .as_ref()
            .is_some_and(|sa| sa.harness.dsh.is_some())
            && has_workers_surface
        {
            return Err(invalid(
                "`superagent.harness.dsh` and a worker surface (a non-empty `endpoints:` \
                 list or a `workers:` section) are mutually exclusive: the dsh harness \
                 runs the worker loop inside the dsh child and never uses the kian \
                 `exec` tool, so a worker binding could not route any command — drop \
                 one of the two sections",
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
            // P2 (kian-101): the injection source and caps. `session`
            // absent = injection off (nothing below runs); present must
            // be a parseable uuid — a typo'd session id would otherwise
            // silently degrade every creation into a skip.
            if let Some(session) = &memory.session
                && uuid::Uuid::parse_str(session).is_err()
            {
                return Err(invalid(
                    "memory.session must be a uuid string (the source memory session)",
                ));
            }
            if let Some(inject) = &memory.inject {
                if memory.session.is_none() {
                    return Err(invalid(
                        "memory.inject requires memory.session — injection caps without a \
                         source session are a configuration mistake, never silently ignored",
                    ));
                }
                if inject.max_items == Some(0) {
                    return Err(invalid("memory.inject.max_items must be > 0"));
                }
                if inject.max_bytes == Some(0) {
                    return Err(invalid("memory.inject.max_bytes must be > 0"));
                }
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
            // (ADR-020, D5): the description is prompt-prefix content —
            // it renders into the Router's judge prompt and the
            // spawn_worker catalog, so it validates fail-closed
            // (non-empty, ≤200 chars, no control characters).
            if let Some(description) = &ep.description {
                if description.trim().is_empty() {
                    return Err(invalid(format!(
                        "endpoint `{}` description must be non-empty (it renders into the \
                         Router prompt)",
                        ep.id
                    )));
                }
                if description.chars().count() > 200 {
                    return Err(invalid(format!(
                        "endpoint `{}` description must be at most 200 characters (it renders \
                         into the Router prompt)",
                        ep.id
                    )));
                }
                if description.chars().any(char::is_control) {
                    return Err(invalid(format!(
                        "endpoint `{}` description must not contain control characters (it \
                         renders into the Router prompt)",
                        ep.id
                    )));
                }
            }
        }

        // The `workers:` section (R12, C2): the pure-config validations
        // (bind shape + private range, positive `offline_after_ms`,
        // non-blank `token_env` name). The token **value** check is a
        // serve-startup check (the assembly reads the env once).
        if let Some(workers) = &self.workers {
            workers.validate()?;
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
                // the `wechat:` section is presence-locked to
                // `type: wechat` in both directions; its shape
                // validates fail-closed (the credential FILE is
                // deliberately not checked here — `kian wechat login`
                // runs before it exists; serve assembly's
                // `to_runtime` is the fail-closed credential gate).
                match (&channel.wechat, channel.channel_type.as_str()) {
                    (Some(section), "wechat") => {
                        if section.credentials_file.trim().is_empty() {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.credentials_file is \
                                 required (the path `kian wechat login` writes; the bot token \
                                 never lives in kian.yaml)"
                            )));
                        }
                        if section.topics.is_empty() {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.topics must declare at \
                                 least one user id → topic entry (the receive allowlist)"
                            )));
                        }
                        for (user_id, topic) in &section.topics {
                            if user_id.trim().is_empty() || topic.trim().is_empty() {
                                return Err(invalid(format!(
                                    "superagent.channels `{name}`.wechat.topics entries must \
                                     be non-empty (`{user_id}`)"
                                )));
                            }
                        }
                        if section.poll_timeout_ms == 0 {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.poll_timeout_ms must be \
                                 > 0 (above the server's ~35 s hold)"
                            )));
                        }
                        if section.context_ttl_ms == 0 {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.context_ttl_ms must be > 0"
                            )));
                        }
                        if section
                            .context_state_file
                            .as_deref()
                            .is_some_and(|p| p.trim().is_empty())
                        {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.context_state_file must \
                                 not be blank when present (omit it to derive the default)"
                            )));
                        }
                        let api_base = section.api_base.trim();
                        if !kian_superagent::wechat::is_http_base(api_base) {
                            return Err(invalid(format!(
                                "superagent.channels `{name}`.wechat.api_base must be an \
                                 http(s):// URL with a non-empty host (https in production; \
                                 http:// is accepted for local mocks), got `{}`",
                                section.api_base
                            )));
                        }
                    }
                    (Some(_), other) => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}`: the `wechat:` section belongs to \
                             `type: wechat` channels only (this channel is `type: {other}`)"
                        )));
                    }
                    (None, "wechat") => {
                        return Err(invalid(format!(
                            "superagent.channels `{name}` (type wechat) requires a `wechat:` \
                             section (api_base?, credentials_file, topics, poll_timeout_ms?)"
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
            // at most one `type: wechat` channel — phase 1 is a
            // single-account channel (CHANNEL-WECHAT §12; the
            // credential file names one bot token).
            let wechat_count = sa
                .channels
                .values()
                .filter(|channel| channel.channel_type == kian_superagent::WECHAT_CHANNEL_TYPE)
                .count();
            if wechat_count > 1 {
                return Err(invalid(
                    "superagent.channels may declare at most one `type: wechat` channel \
                     (phase 1 is single-account: one credential file, one bot token)",
                ));
            }
            // (CRON-MODEL §3.3) the cron declarations validate
            // fail-closed at load: expression + horizon, timezone, id
            // shape/uniqueness, target cross-reference, bounds, the
            // unsupported-policy gates, and the watchdog cross-reference
            // (rule 7: a wall-clock judge must not fight the interval
            // watchdog for the same session).
            let resolved_cron = sa.cron.to_runtime(&sa.channels, chrono::Utc::now())?;
            reject_cron_targeting_the_watchdog(
                &resolved_cron,
                sa.watchdog.as_ref().map(|w| w.session_id.as_str()),
            )?;
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
            // CONFIRMATION-MODEL C2 (ADR-033): phase 1 has no waiting
            // path for a worker — ending one `awaiting_input` without
            // `thread/completed` would strand its ticket, so `ask` on a
            // worker agent refuses boot (fail-closed, never silently
            // ignored). Phase 2 removes this refusal.
            if worker_agent
                .permissions
                .values()
                .any(|p| matches!(p, Permission::Ask))
            {
                return Err(invalid(format!(
                    "agents.{}: `permissions: ask` is not supported on a superagent worker agent \
                     in phase 1 (the worker has no waiting path yet — CONFIRMATION-MODEL C2); \
                     use it on the default agent or wait for phase 2",
                    sa.worker.agent
                )));
            }
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
            // (R17 C4) the board block converts cleanly (fail-closed —
            // `gating: true` is a P4 key and refuses to boot in P3,
            // naming the deferral).
            sa.board_config()?;
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

    /// Converts the `superagent.roster` section to the bundle's
    /// [`kian_superagent::Roster`], fail-closed. `Ok(None)` when the
    /// section is absent **or empty** — the roster feature is off,
    /// today's tool set byte-identical (pin P2-6, §3.1:366-367).
    ///
    /// Validation is the contract's (§3.1), errors naming the key:
    /// duplicate ids; an `agent` naming no configured `agents:`
    /// entry (the refusal names the known ids — pin P2-1, resolved
    /// exactly through [`Self::named_agent_def`]); `tools_allow`
    /// listing a tool the AgentDef lacks (a widening refuses boot —
    /// M3/§5.1:551-552); an `output_schema` outside the closed
    /// subset (pin P2-3, naming the keyword). `limits.max_children:
    /// 0` is legal — a leaf (§3.1:360-361).
    pub fn roster_config(&self) -> Result<Option<kian_superagent::Roster>, ConfigError> {
        let Some(sa) = self.superagent.as_ref() else {
            return Ok(None);
        };
        if sa.roster.is_empty() {
            return Ok(None);
        }
        // Whole-set check first: duplicate ids (the per-entry pass
        // below cannot see them).
        let mut seen = std::collections::BTreeSet::new();
        for def in &sa.roster {
            if !seen.insert(def.id.as_str()) {
                return Err(invalid(format!(
                    "superagent.roster: duplicate id `{}` — roster ids are unique",
                    def.id
                )));
            }
        }
        // The pin P2-1 resolution seam: exactly `named_agent_def`,
        // and the known `agents:` ids for the refusal text.
        let known: Vec<String> = self.agents.keys().cloned().collect();
        let lookup = |id: &str| {
            self.named_agent_def(id)
                .map(|def| kian_superagent::AgentSurface {
                    id: def.id,
                    tools: def.tools,
                })
        };
        let mut entries = Vec::with_capacity(sa.roster.len());
        for (i, def) in sa.roster.iter().enumerate() {
            let strength = match def.mandate_template.strength.as_str() {
                "explicit" => kian_superagent::MandateStrength::Explicit,
                "inferred" => kian_superagent::MandateStrength::Inferred,
                other => {
                    return Err(invalid(format!(
                        "superagent.roster[{i}].mandate_template.strength: unknown `{other}` \
                         (explicit | inferred)"
                    )));
                }
            };
            let entry = kian_superagent::RosterEntry {
                id: def.id.clone(),
                description: def.description.clone(),
                agent: def.agent.clone(),
                mandate_template: kian_superagent::MandateTemplate {
                    scope: def.mandate_template.scope.clone(),
                    strength,
                },
                tools_allow: def.tools_allow.clone(),
                limits: kian_superagent::RosterLimits {
                    max_children: def
                        .limits
                        .max_children
                        .unwrap_or_else(kian_superagent::default_roster_max_children),
                },
                output_schema: def.output_schema.clone(),
                // (R17-C2, pin P2-1) the resolution is stamped here,
                // once, at conversion — the spawn side reads this
                // recorded fact and never re-consults live config
                // (§2.1.1 constraint 13). `named_agent_def` resolved
                // it for the surface check below; the same call
                // supplies the identity the wrapper will carry.
                resolved_agent: self.named_agent_def(&def.agent).map(|agent| {
                    kian_superagent::ResolvedAgent {
                        def_id: agent.id,
                        version: agent.version,
                    }
                }),
            };
            kian_superagent::Roster {
                entries: vec![entry.clone()],
            }
            .validate(&lookup, &known)
            .map_err(|e| invalid(format!("superagent.roster[{i}]: {e}")))?;
            entries.push(entry);
        }
        Ok(Some(kian_superagent::Roster { entries }))
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
// superagent.cron (CRON-MODEL §3)
// ---------------------------------------------------------------------------

/// Default `coalesce_ms` (CRON-MODEL §3.1): 15 minutes of lateness honored
/// by `catch_up: latest`.
pub const DEFAULT_CRON_COALESCE_MS: u64 = 900_000;
/// Default `max_concurrent` (CRON-MODEL §3.1): one firing at a time.
pub const DEFAULT_CRON_MAX_CONCURRENT: u32 = 1;
/// Default `timeout_ms` (CRON-MODEL §3.1): a 30-minute per-firing ceiling.
pub const DEFAULT_CRON_TIMEOUT_MS: u64 = 1_800_000;
/// The longest schedule `id` (§3.1: `[a-z0-9][a-z0-9_-]{0,63}`).
pub const CRON_SCHEDULE_ID_MAX_LEN: usize = 64;

/// The `superagent.cron:` section (CRON-MODEL §3.1): the declared
/// schedules, validated fail-closed at load and resolved into
/// [`CronRuntimeSchedule`]s by [`CronSection::to_runtime`]. The tick loop
/// (§7.3), the durable cursor (§4), the dsh/worktree execution path (§6) and
/// the CLI (§10) consume these resolutions. `sandbox` (§8.2) is designed, not
/// v1: an unknown key still refuses boot here (`deny_unknown_fields`), never a
/// silently ignored policy field.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronSection {
    /// The declared schedules, in declaration order. Order is the
    /// `kian cron list` order and nothing else — firing order is the
    /// schedule's own fire time (§2.5).
    #[serde(default)]
    pub schedules: Vec<CronScheduleDef>,

    /// The §6.4 disk guards: cron must never fill the host disk. Absent = the
    /// documented defaults; an explicit `0` refuses boot rather than silently
    /// disabling a guard.
    #[serde(default)]
    pub disk: CronDiskSection,

    /// The §6/§7 harness invocation: *how* a firing's payload is handed
    /// to `dsh`. Absent = the documented default (the shipped `headless`
    /// profile, the payload as that app's one positional task argument).
    #[serde(default)]
    pub harness: CronHarnessSection,
}

/// Default `superagent.cron.disk.min_free_gib` (CRON-MODEL §6.4).
pub const CRON_DEFAULT_MIN_FREE_GIB: u64 = 80;
/// Default `superagent.cron.disk.min_free_percent` (CRON-MODEL §6.4).
pub const CRON_DEFAULT_MIN_FREE_PERCENT: u64 = 25;
/// Default `superagent.cron.disk.max_worktree_mib` (CRON-MODEL §6.4).
pub const CRON_DEFAULT_MAX_WORKTREE_MIB: u64 = 2048;
/// Default `superagent.cron.disk.total_cap_mib` (CRON-MODEL §6.4).
pub const CRON_DEFAULT_TOTAL_CAP_MIB: u64 = 20480;
/// Default `superagent.cron.disk.max_output_kib` (CRON-MODEL §6.4).
pub const CRON_DEFAULT_MAX_OUTPUT_KIB: u64 = 256;

/// The `superagent.cron.disk:` section (CRON-MODEL §6.4): the guards that keep
/// cron from filling the host disk. Every field is `Option` — absent means the
/// documented default, an explicit `0` refuses boot
/// ([`CronDiskSection::collect_errors`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronDiskSection {
    /// Free-space preflight floor, GiB. Default 80, sized for Rust build artefacts
    /// (a `target/` tree is GiB, not MiB). The floor actually enforced
    /// is the larger of this and `min_free_percent` of the filesystem.
    #[serde(default)]
    pub min_free_gib: Option<u64>,
    /// Free-space preflight floor, percent of the filesystem. Default 25.
    #[serde(default)]
    pub min_free_percent: Option<u64>,
    /// Cap on a *single* worktree, MiB. Default 2048.
    #[serde(default)]
    pub max_worktree_mib: Option<u64>,
    /// Cap across *all* cron worktrees, MiB. Default 20480. `max_total_worktree_mib`
    /// is accepted as a synonym (the same knob under the longer name).
    #[serde(default, alias = "max_total_worktree_mib")]
    pub total_cap_mib: Option<u64>,
    /// Bound on each stored stdout/stderr/diff of a firing, KiB. Default 256.
    #[serde(default)]
    pub max_output_kib: Option<u64>,
}

impl CronDiskSection {
    /// The section with §6.4's defaults applied — what
    /// [`crate::cron_worktree::WorktreePolicy`] is built from.
    pub fn resolved(&self) -> CronDiskLimits {
        CronDiskLimits {
            min_free_gib: self.min_free_gib.unwrap_or(CRON_DEFAULT_MIN_FREE_GIB),
            min_free_percent: self
                .min_free_percent
                .unwrap_or(CRON_DEFAULT_MIN_FREE_PERCENT),
            max_worktree_mib: self
                .max_worktree_mib
                .unwrap_or(CRON_DEFAULT_MAX_WORKTREE_MIB),
            total_cap_mib: self.total_cap_mib.unwrap_or(CRON_DEFAULT_TOTAL_CAP_MIB),
            max_output_kib: self.max_output_kib.unwrap_or(CRON_DEFAULT_MAX_OUTPUT_KIB),
        }
    }

    /// §3.3 fail-closed: a knob set to `0` (or a percent outside `1..=100`)
    /// refuses boot rather than silently disabling a disk guard.
    pub fn collect_errors(&self, errors: &mut Vec<String>) {
        let sized = [
            ("min_free_gib", self.min_free_gib),
            ("max_worktree_mib", self.max_worktree_mib),
            ("total_cap_mib", self.total_cap_mib),
            ("max_output_kib", self.max_output_kib),
        ];
        for (name, value) in sized {
            if value == Some(0) {
                errors.push(format!(
                    "superagent.cron.disk.{name} must be > 0 (omit it for the default)"
                ));
            }
        }
        if let Some(percent) = self.min_free_percent
            && !(1..=100).contains(&percent)
        {
            errors.push(
                "superagent.cron.disk.min_free_percent must be 1..=100 (omit it for the default 25)"
                    .to_owned(),
            );
        }
        // §6.4: the total cap bounds all worktrees, so a per-worktree cap larger
        // than it can never be reached — the config is contradictory, refuse it.
        let limits = self.resolved();
        if limits.min_free_gib > 0
            && limits.max_worktree_mib > 0
            && limits.total_cap_mib > 0
            && limits.max_worktree_mib > limits.total_cap_mib
        {
            errors.push(format!(
                "superagent.cron.disk.total_cap_mib ({}) must be >= max_worktree_mib ({})",
                limits.total_cap_mib, limits.max_worktree_mib
            ));
        }
    }
}

/// [`CronDiskSection`] with §6.4's defaults applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronDiskLimits {
    /// Free-space preflight floor, GiB.
    pub min_free_gib: u64,
    /// Free-space preflight floor, percent of the filesystem.
    pub min_free_percent: u64,
    /// Cap on a single worktree, MiB.
    pub max_worktree_mib: u64,
    /// Cap across all cron worktrees, MiB.
    pub total_cap_mib: u64,
    /// Bound on each stored stdout/stderr/diff, KiB.
    pub max_output_kib: u64,
}
/// Default `superagent.cron.harness.profile` (CRON-MODEL §6.2): the shipped
/// `headless` dsh profile — `dsh headless "run the tests"` answers one task,
/// prints the result and exits, which is exactly one cron firing's turn.
pub const CRON_DEFAULT_DSH_PROFILE: &str = "headless";

/// The `superagent.cron.harness:` section (CRON-MODEL §6.2): how a firing's turn
/// is handed to the harness.
///
/// dsh's own contract is `dsh [--profile] <name> [options] [app-args...]`:
/// the profile selects the app, and the scheduled payload is that app's one
/// positional task argument (`dsh headless "run the tests"`). An argv with no
/// `--profile` makes dsh parse the payload *as the profile name* and refuse
/// (`profile "say hello" does not exist`), which is the bug this section fixes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronHarnessSection {
    /// The dsh profile to boot — `dsh --profile <name>`. Default `headless`.
    #[serde(default)]
    pub profile: Option<String>,
    /// The dsh binary. Absent = `KIAN_DSH_BIN`, then `dsh` on `PATH`.
    #[serde(default)]
    pub command: Option<String>,
    /// Extra environment for the harness child: peer/model/session selects and
    /// credential *references*. Values come from config or the environment and
    /// are never echoed into a firing, a log line or an error.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

impl CronHarnessSection {
    /// The section with the documented defaults applied — what
    /// [`crate::cron_worktree::DshTurnRunner`] is built from.
    pub fn resolved(&self) -> CronHarnessSettings {
        let profile = self
            .profile
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or(CRON_DEFAULT_DSH_PROFILE)
            .to_owned();
        let command = self
            .command
            .as_deref()
            .map(str::trim)
            .filter(|cmd| !cmd.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                std::env::var("KIAN_DSH_BIN")
                    .ok()
                    .map(|cmd| cmd.trim().to_owned())
                    .filter(|cmd| !cmd.is_empty())
            })
            .unwrap_or_else(|| "dsh".to_owned());
        CronHarnessSettings {
            profile,
            command,
            env: self.env.clone(),
        }
    }

    /// §3.3 fail-closed: a whitespace-only knob refuses boot rather than
    /// silently falling back to a default the operator did not choose.
    pub fn collect_errors(&self, errors: &mut Vec<String>) {
        if self
            .profile
            .as_deref()
            .is_some_and(|profile| profile.trim().is_empty())
        {
            errors.push(
                "superagent.cron.harness.profile must be non-empty (omit it for `headless`)"
                    .to_owned(),
            );
        }
        if self
            .command
            .as_deref()
            .is_some_and(|command| command.trim().is_empty())
        {
            errors.push(
                "superagent.cron.harness.command must be non-empty (omit it for                  `KIAN_DSH_BIN` or `dsh` on PATH)"
                    .to_owned(),
            );
        }
        for name in self.env.keys() {
            if name.trim().is_empty() {
                errors.push("superagent.cron.harness.env keys must be non-empty".to_owned());
            }
        }
    }
}

/// [`CronHarnessSection`] with the documented defaults applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronHarnessSettings {
    /// The dsh profile to boot.
    pub profile: String,
    /// The dsh binary.
    pub command: String,
    /// Extra environment for the harness child (never logged).
    pub env: std::collections::BTreeMap<String, String>,
}

impl Default for CronHarnessSettings {
    fn default() -> Self {
        CronHarnessSection::default().resolved()
    }
}

/// One declared cron schedule (CRON-MODEL §3.1), wire form.
///
/// `Option<_>` marks every field with a §3.1 default: absent means the
/// documented default, an explicit value is validated (`0` where the spec
/// requires `> 0` refuses boot rather than silently defaulting).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronScheduleDef {
    /// Stable identity — the cursor key and log namespace (§3.1). Shape
    /// and uniqueness are validated in [`CronSection::to_runtime`].
    pub id: String,
    /// Five-field expression or `@`-alias (§2.1, §2.2).
    pub expr: String,
    /// IANA timezone (§2.3). Omitted = the host timezone, resolved once
    /// at load and frozen into the resolved schedule.
    #[serde(default)]
    pub tz: Option<String>,
    /// Declared-but-off schedules are validated and listed, never fired.
    /// Default `true`.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Where the firing's turn lands (§3.2): exactly one of
    /// `{session: <uuid>}` or `{channel: <name>, topic: <topic>}`.
    pub target: CronTargetDef,
    /// The firing turn's user message (§3.4). `payload` is accepted as a
    /// synonym (the same string under either name).
    #[serde(alias = "payload")]
    pub prompt: String,
    /// Overlap policy (§5.1). Default `skip`.
    #[serde(default)]
    pub overlap: Option<CronOverlap>,
    /// Missed-slot policy (§5.3). Default `latest`.
    #[serde(default)]
    pub catch_up: Option<CronCatchUpDef>,
    /// Lateness honored by `latest`, milliseconds (§5.3). Default 15 min.
    #[serde(default)]
    pub coalesce_ms: Option<u64>,
    /// Ceiling for this schedule (§5.4). Default `1`.
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    /// Per-firing ceiling, milliseconds (§5.5). Default 30 min.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Worktree isolation (§6). Absent = the firing runs on the resident
    /// engine; present = the schedule's firings are dispatched to the
    /// worktree-backed `WorktreeDispatcher` (a bare `true` takes §6's
    /// defaults, an object sets `repo`/`root`/`base`/`keep_on_failure`).
    #[serde(default)]
    pub worktree: Option<CronWorktreeDef>,
}

/// Wire form of a schedule's `worktree:` policy (§6): `true` for §6's
/// defaults, or an object with the fields below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CronWorktreeDef {
    /// `worktree: true` / `worktree: false`.
    Flag(bool),
    /// `worktree: {repo, root, base, keep_on_failure}`.
    Policy(CronWorktreePolicyDef),
}

/// `worktree: {...}` (§6.2/§6.3); every field optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronWorktreePolicyDef {
    /// The repo the per-firing trees are cut from. Default: the process cwd.
    #[serde(default)]
    pub repo: Option<String>,
    /// `worktree.root`: where the trees live. Default
    /// `<git-common-dir>/kian-worktrees` (resolved by git, so a linked
    /// worktree's gitfile still lands in the shared git dir).
    #[serde(default)]
    pub root: Option<String>,
    /// `worktree.base`: the git rev a tree starts from. Default `HEAD`.
    #[serde(default)]
    pub base: Option<String>,
    /// Leave a failed firing's tree behind for inspection (§6.2). Default true.
    #[serde(default)]
    pub keep_on_failure: Option<bool>,
}

/// One schedule's resolved isolation plan (§6): the repo to cut from and the
/// policy (whose disk knobs come from `superagent.cron.disk`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronWorktreePlan {
    /// `worktree.repo`; `None` = the process's working directory.
    pub repo: Option<std::path::PathBuf>,
    /// The resolved §6.2/§6.4 policy.
    pub policy: crate::cron_worktree::WorktreePolicy,
}

/// Wire form of a schedule's `target` (CRON-MODEL §3.2): the untagged pair
/// of the two legal shapes. The inner structs carry `deny_unknown_fields`
/// so a stray key is a parse error, and `topic` is `Option` so a channel
/// target missing its topic fails with a named §3.2 message rather than
/// serde's untagged "did not match any variant".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CronTargetDef {
    /// `{session: <uuid>}` — fire into a fixed session.
    Session(CronSessionTargetDef),
    /// `{channel: <name>, topic: <topic>}` — fire as a channel event.
    Channel(CronChannelTargetDef),
}

/// `target: {session: <uuid>}` (§3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronSessionTargetDef {
    /// The fixed session id (a uuid, validated).
    pub session: String,
}

/// `target: {channel: <name>, topic: <topic>}` (§3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronChannelTargetDef {
    /// A channel declared in `superagent.channels` (fail-closed
    /// cross-reference).
    pub channel: String,
    /// The routing topic the firing carries.
    #[serde(default)]
    pub topic: Option<String>,
}

/// A resolved target (CRON-MODEL §3.2), every field validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronTarget {
    /// Fire into this session.
    Session {
        /// A validated uuid.
        session: String,
    },
    /// Fire as a channel event on this declared channel/topic.
    Channel {
        /// A declared channel name.
        channel: String,
        /// A non-empty routing topic.
        topic: String,
    },
}

/// The overlap policy (§5.1), wire and resolved form.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronOverlap {
    /// Do not fire while the previous firing still runs (the default).
    #[default]
    Skip,
    /// Queue the slot behind the running firing.
    Queue,
    /// Kill the running firing, then run this one.
    #[serde(alias = "kill_previous")]
    KillPrevious,
}

/// The `catch_up` policy (§5.3), wire form. Converted to the scheduling
/// core's [`kian_cron::CatchUp`] in [`CronSection::to_runtime`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronCatchUpDef {
    /// Fire at most once for the most recent missed slot (the default).
    #[default]
    Latest,
    /// Never fire for a slot that is already past.
    Skip,
}

/// One fully resolved schedule: everything a tick loop needs to compute the
/// next fire time and to describe the firing, and nothing that still needs
/// validation.
///
/// The next-fire *inputs* are the parsed [`kian_cron::CronExpr`] and the
/// frozen [`kian_cron::Tz`]; [`CronRuntimeSchedule::next_fire_after`] is the
/// pure §2.5 computation over them. There is no clock read and no store
/// access here — the cursor, the tick loop and the firing path are later
/// increments.
#[derive(Debug, Clone)]
pub struct CronRuntimeSchedule {
    /// The declared id (cursor key, §4.1).
    pub id: String,
    /// The parsed expression (aliases already expanded, §2.2).
    pub expr: kian_cron::CronExpr,
    /// The effective timezone: explicit, or the host timezone frozen at
    /// load (§2.3).
    pub tz: kian_cron::Tz,
    /// The effective timezone's IANA name, for `kian cron list` to echo.
    pub tz_name: String,
    /// Declared-but-off schedules are listed, never fired.
    pub enabled: bool,
    /// The resolved target (§3.2).
    pub target: CronTarget,
    /// The firing turn's user message.
    pub prompt: String,
    /// The overlap policy (§5.1).
    pub overlap: CronOverlap,
    /// The missed-slot policy (§5.3).
    pub catch_up: kian_cron::CatchUp,
    /// Lateness honored by `latest`, milliseconds.
    pub coalesce_ms: u64,
    /// This schedule's concurrency ceiling.
    pub max_concurrent: u32,
    /// The per-firing ceiling, milliseconds.
    pub timeout_ms: u64,
    /// The §6 worktree isolation plan; `None` = resident-engine dispatch.
    pub worktree: Option<CronWorktreePlan>,
}

impl CronRuntimeSchedule {
    /// The earliest fire time strictly after `from` (§2.5) — pure: no
    /// clock read, no store access, DST gaps skipped and overlaps
    /// collapsed (§2.4). The tick loop is the only caller-to-be.
    pub fn next_fire_after(
        &self,
        from: chrono::DateTime<chrono::Utc>,
    ) -> Result<chrono::DateTime<chrono::Utc>, kian_cron::CronError> {
        self.expr.next_fire_after(&self.tz, from)
    }
}

/// The host's IANA timezone, resolved **once at load** (CRON-MODEL §2.3):
/// `TZ` first, then the `/etc/localtime` symlink, then UTC.
///
/// The name is returned beside the zone so the effective value can be
/// frozen into the resolved schedule and echoed by `kian cron list`; a
/// later host timezone change never moves an existing schedule. An
/// explicit `tz:` that does not resolve is still a config error (§2.3) —
/// this fallback only applies to the *omitted* default.
pub fn host_timezone() -> (kian_cron::Tz, String) {
    if let Ok(raw) = std::env::var("TZ") {
        let name = raw.trim().trim_start_matches(':');
        if !name.is_empty()
            && let Ok(tz) = kian_cron::parse_timezone(name)
        {
            return (tz, name.to_string());
        }
    }
    if let Ok(link) = std::fs::read_link("/etc/localtime") {
        let text = link.to_string_lossy().into_owned();
        if let Some(pos) = text.find("zoneinfo/") {
            let name = text[pos + "zoneinfo/".len()..].trim_end_matches('/');
            if !name.is_empty()
                && let Ok(tz) = kian_cron::parse_timezone(name)
            {
                return (tz, name.to_string());
            }
        }
    }
    (kian_cron::Tz::UTC, "UTC".to_string())
}

/// Is `id` a legal schedule id (§3.1: `[a-z0-9][a-z0-9_-]{0,63}`)?
fn is_valid_cron_id(id: &str) -> bool {
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && id.len() <= CRON_SCHEDULE_ID_MAX_LEN
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

impl CronSection {
    /// Converts the declarations into resolved schedules (the timer's
    /// `to_runtime` pattern), fail-closed on every degenerate field
    /// (§3.3): a malformed expression, an unresolvable timezone, a
    /// duplicate or malformed id, a target that does not resolve, an
    /// empty prompt, a zero bound or a contradictory or unsupported
    /// policy is a config error — never a silent skip and never a silent
    /// default.
    ///
    /// All *declaration* errors are collected (each naming its schedule
    /// id) and reported together, so an operator fixes one load rather
    /// than one error per restart.
    ///
    /// §3.3 rule 7 (`cron_targets_watchdog`) is enforced by
    /// [`reject_cron_targeting_the_watchdog`] at `ServeConfig` validation,
    /// where the `superagent.watchdog` session is visible.
    pub fn to_runtime(
        &self,
        channels: &std::collections::BTreeMap<String, ChannelConfig>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<CronRuntimeSchedule>, ConfigError> {
        let (host_tz, host_name) = host_timezone();
        let mut errors: Vec<String> = Vec::new();
        self.disk.collect_errors(&mut errors);
        self.harness.collect_errors(&mut errors);
        let mut seen: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
        let mut resolved = Vec::with_capacity(self.schedules.len());

        for (i, def) in self.schedules.iter().enumerate() {
            let before = errors.len();
            let mut fail = |message: String| {
                errors.push(format!(
                    "superagent.cron.schedules[{i}] `{}`: {message}",
                    def.id
                ));
            };

            // §3.1 id shape + §3.3 rule 2 (unique, and not a channel name).
            if def.id.trim() != def.id || !is_valid_cron_id(&def.id) {
                fail(
                    "id must match [a-z0-9][a-z0-9_-]{0,63} (lowercase, at most 64 \
                     characters)"
                        .to_string(),
                );
            } else if let Some(first) = seen.insert(def.id.clone(), i) {
                fail(format!(
                    "duplicate id (already declared at superagent.cron.schedules[{first}]; \
                     the id is the cursor key and must be unique)"
                ));
            } else if channels.contains_key(&def.id) {
                fail(format!(
                    "id `{}` collides with a declared channel name (§3.3 rule 2)",
                    def.id
                ));
            }

            // §2.3 timezone: explicit, or the host timezone frozen at load.
            let (tz, tz_name) = match def.tz.as_deref() {
                None => (host_tz, host_name.clone()),
                Some(raw) => {
                    let name = raw.trim();
                    if name.is_empty() {
                        fail(
                            "tz must be non-empty when set (omit the key to freeze the host \
                             timezone)"
                                .to_string(),
                        );
                        (host_tz, host_name.clone())
                    } else {
                        match kian_cron::parse_timezone(name) {
                            Ok(tz) => (tz, name.to_string()),
                            Err(e) => {
                                fail(format!("tz `{name}` does not resolve: {e}"));
                                (host_tz, host_name.clone())
                            }
                        }
                    }
                }
            };

            // §3.3 rule 1: the expression parses and has a fire time inside
            // the §2.5 horizon (a valid-but-never-matching shape refuses).
            let expr = match kian_cron::CronExpr::parse(&def.expr) {
                Ok(expr) => match expr.next_fire_after(&tz, now) {
                    Ok(_) => Some(expr),
                    Err(e) => {
                        fail(format!(
                            "expr `{}` yields no fire time within the {} day horizon: {e}",
                            def.expr.trim(),
                            kian_cron::MAX_SCAN_DAYS
                        ));
                        None
                    }
                },
                Err(e) => {
                    fail(format!("expr `{}` is invalid: {e}", def.expr.trim()));
                    None
                }
            };

            // §3.2 target: exactly one of session | channel.
            let target = match &def.target {
                CronTargetDef::Session(session) => {
                    let value = session.session.trim();
                    if value.is_empty() {
                        fail("target.session must be non-empty".to_string());
                        None
                    } else if let Err(e) = uuid::Uuid::parse_str(value) {
                        fail(format!("target.session `{value}` is not a uuid ({e})"));
                        None
                    } else {
                        Some(CronTarget::Session {
                            session: value.to_string(),
                        })
                    }
                }
                CronTargetDef::Channel(channel) => {
                    let name = channel.channel.trim();
                    let topic = channel.topic.as_deref().map(str::trim).unwrap_or("");
                    if name.is_empty() {
                        fail("target.channel must be non-empty".to_string());
                        None
                    } else if topic.is_empty() {
                        fail(
                            "target.topic must be non-empty for a channel target (§3.2 \
                             {channel: <name>, topic: <topic>})"
                                .to_string(),
                        );
                        None
                    } else if !channels.contains_key(name) {
                        fail(format!(
                            "target.channel `{name}` is not declared in superagent.channels \
                             (fail-closed cross-reference, §3.2)"
                        ));
                        None
                    } else {
                        Some(CronTarget::Channel {
                            channel: name.to_string(),
                            topic: topic.to_string(),
                        })
                    }
                }
            };

            // §3.4: the prompt is the turn's user message — an empty one is
            // an error, never an empty turn.
            if def.prompt.trim().is_empty() {
                fail("prompt must be non-empty (it is the firing turn's user message)".to_string());
            }

            // §3.3 rules 4 and 5: real bounds, no contradiction.
            let overlap = def.overlap.unwrap_or_default();
            let catch_up = match def.catch_up.unwrap_or_default() {
                CronCatchUpDef::Latest => kian_cron::CatchUp::Latest,
                CronCatchUpDef::Skip => kian_cron::CatchUp::Skip,
            };
            let coalesce_ms = def.coalesce_ms.unwrap_or(DEFAULT_CRON_COALESCE_MS);
            let max_concurrent = def.max_concurrent.unwrap_or(DEFAULT_CRON_MAX_CONCURRENT);
            let timeout_ms = def.timeout_ms.unwrap_or(DEFAULT_CRON_TIMEOUT_MS);
            if coalesce_ms == 0 {
                fail("coalesce_ms must be > 0 (a zero window honors no lateness)".to_string());
            }
            if max_concurrent == 0 {
                fail("max_concurrent must be >= 1".to_string());
            }
            if timeout_ms == 0 {
                fail("timeout_ms must be > 0".to_string());
            }
            // v1 honesty gate (CRON-MODEL §5.1/§5.5): `queue` and
            // `kill-previous` are designed, not implemented — the v1
            // scheduler has no running-set to queue behind and no
            // cross-replica kill path, so accepting the key would lie
            // about the policy. Fail closed at load instead.
            if overlap != CronOverlap::Skip {
                fail(format!(
                    "overlap: {} is designed, not implemented in v1 (CRON-MODEL §5.1) — omit \
                     the key or set `overlap: skip`",
                    match overlap {
                        CronOverlap::Queue => "queue",
                        CronOverlap::KillPrevious => "kill-previous",
                        CronOverlap::Skip => "skip",
                    }
                ));
            }
            // v1 scope gate (CRON-MODEL §5.5): the per-firing timeout is
            // enforced only by the worktree arm's runner. A resident
            // firing's turn has no cron-owned deadline (ADR-030 wait
            // bounds and the exec timeout still apply), so an explicit
            // `timeout_ms` on a resident schedule would be a silent no-op
            // — refuse it. Omitting the key keeps the documented default
            // for worktree schedules and means "no cron deadline" for a
            // resident one.
            let wants_worktree = matches!(
                def.worktree.as_ref(),
                Some(CronWorktreeDef::Policy(_)) | Some(CronWorktreeDef::Flag(true))
            );
            if def.timeout_ms.is_some() && !wants_worktree {
                fail(
                    "timeout_ms is only enforced for a worktree-isolated firing in v1 \
                     (CRON-MODEL §5.5) — a resident-arm firing has no cron-owned deadline; \
                     omit the key or declare `worktree:`"
                        .to_string(),
                );
            }
            if overlap == CronOverlap::KillPrevious && max_concurrent > 1 {
                fail(format!(
                    "overlap: kill-previous contradicts max_concurrent: {max_concurrent} \
                     (§3.3 rule 5)"
                ));
            }

            // §6 isolation: a declared `worktree` policy selects the
            // worktree-backed dispatcher for this schedule's firings.
            let worktree = match def.worktree.as_ref() {
                None => None,
                Some(CronWorktreeDef::Flag(false)) => None,
                Some(declared) => {
                    let mut policy = crate::cron_worktree::WorktreePolicy::from_section(self);
                    let mut repo: Option<std::path::PathBuf> = None;
                    if let CronWorktreeDef::Policy(declared) = declared {
                        if let Some(raw) = declared.repo.as_deref() {
                            let raw = raw.trim();
                            if raw.is_empty() {
                                fail("worktree.repo must be non-empty when set".to_string());
                            } else {
                                let path = std::path::PathBuf::from(raw);
                                repo = Some(if path.is_absolute() {
                                    path
                                } else {
                                    std::env::current_dir()
                                        .unwrap_or_else(|_| std::path::PathBuf::from("."))
                                        .join(path)
                                });
                            }
                        }
                        if let Some(raw) = declared.root.as_deref() {
                            let raw = raw.trim();
                            if raw.is_empty() {
                                fail("worktree.root must be non-empty when set".to_string());
                            } else {
                                policy.root = Some(std::path::PathBuf::from(raw));
                            }
                        }
                        if let Some(raw) = declared.base.as_deref() {
                            let raw = raw.trim();
                            if raw.is_empty() {
                                fail("worktree.base must be non-empty when set".to_string());
                            } else {
                                policy.base = Some(raw.to_string());
                            }
                        }
                        if let Some(keep) = declared.keep_on_failure {
                            policy.keep_on_failure = keep;
                        }
                    }
                    Some(CronWorktreePlan { repo, policy })
                }
            };

            if errors.len() > before {
                continue;
            }
            let (Some(expr), Some(target)) = (expr, target) else {
                continue;
            };
            resolved.push(CronRuntimeSchedule {
                id: def.id.clone(),
                expr,
                tz,
                tz_name,
                enabled: def.enabled.unwrap_or(true),
                target,
                prompt: def.prompt.clone(),
                overlap,
                catch_up,
                coalesce_ms,
                max_concurrent,
                timeout_ms,
                worktree,
            });
        }

        if !errors.is_empty() {
            return Err(invalid(errors.join("; ")));
        }
        Ok(resolved)
    }
}

/// CRON-MODEL §3.3 rule 7 (`cron_targets_watchdog`): a wall-clock schedule
/// must not fire into the interval watchdog's session — the two judge loops
/// would fight over the same stream. Enforced at load with the resolved
/// declarations (the watchdog session is a `superagent.watchdog`
/// cross-reference).
pub fn reject_cron_targeting_the_watchdog(
    schedules: &[CronRuntimeSchedule],
    watchdog_session: Option<&str>,
) -> Result<(), ConfigError> {
    let Some(watchdog) = watchdog_session else {
        return Ok(());
    };
    for schedule in schedules {
        if let CronTarget::Session { session } = &schedule.target
            && session == watchdog
        {
            return Err(invalid(format!(
                "superagent.cron schedule `{}` targets the watchdog session `{watchdog}` \
                 (cron_targets_watchdog, CRON-MODEL §3.3 rule 7)",
                schedule.id
            )));
        }
    }
    Ok(())
}

/// The worktree arm(s) the resident ticker arms: one dispatcher per
/// schedule that asked for isolation, keyed by schedule id. Each schedule
/// keeps its own `worktree.repo` — a global "first schedule wins" rule
/// silently ran one repo's audit in another repo's tree.
pub fn cron_worktree_plans(
    schedules: &[CronRuntimeSchedule],
) -> std::collections::BTreeMap<String, (std::path::PathBuf, crate::cron_worktree::WorktreePolicy)>
{
    schedules
        .iter()
        .filter_map(|schedule| {
            schedule.worktree.as_ref().map(|plan| {
                let repo = plan
                    .repo
                    .clone()
                    .or_else(|| std::env::current_dir().ok())
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                (schedule.id.clone(), (repo, plan.policy.clone()))
            })
        })
        .collect()
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

    // -- workers: (R12, C1–C6) -------------------------------------

    /// ADR-033: `workers.queue_max_redrives` is the bounded re-drive knob —
    /// explicit values parse, `0` is meaningful (no re-drive), and the
    /// fail-closed ceiling refuses a value that would restore unbounded
    /// replay in practice.
    #[test]
    fn workers_queue_max_redrives_parses_and_is_fail_closed() {
        let base = valid_yaml();
        let with = |max: u32| {
            format!(
                "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
                 workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n  \
                 queue_max_redrives: {max}\n"
            )
        };

        let cfg = ServeConfig::parse(&with(0)).expect("explicit zero parses");
        assert_eq!(
            cfg.workers
                .clone()
                .expect("section present")
                .queue_max_redrives,
            0
        );
        assert!(cfg.validate().is_ok(), "zero = first dispatch only");

        let cfg = ServeConfig::parse(&with(3)).expect("explicit bound parses");
        assert_eq!(
            cfg.workers
                .clone()
                .expect("section present")
                .queue_max_redrives,
            3
        );
        assert!(cfg.validate().is_ok());

        // `parse` validates: the ceiling refuses at load time, not later.
        let err = ServeConfig::parse(&with(17)).expect_err("ceiling must fail closed");
        assert!(err.to_string().contains("queue_max_redrives"), "was: {err}");
    }

    /// A config with a `workers:` section and zero `endpoints:` boots
    /// (C6): the remote-exec plugin's presence lock covers the dynamic
    /// half. Absent section parses to `None` (feature off).
    #[test]
    fn workers_section_parses_with_defaults() {
        let base = valid_yaml();
        let with_workers = format!(
            "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
             workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n"
        );
        let cfg = ServeConfig::parse(&with_workers).expect("workers config");
        let workers = cfg.workers.clone().expect("section present");
        assert_eq!(workers.listen, "127.0.0.1:7400");
        assert_eq!(workers.token_env.as_deref(), Some("KIAN_WORKER_TOKEN"));
        assert_eq!(workers.offline_after_ms, 30_000, "default");
        assert_eq!(workers.queue_timeout_ms, 60_000, "default");
        assert_eq!(workers.queue_max_redrives, 1, "default");
        assert_eq!(
            workers.queue_redrive_grace_ms, DEFAULT_WORKER_QUEUE_REDRIVE_GRACE_MS,
            "default"
        );
        assert_eq!(workers.release_after_ms, DEFAULT_WORKER_RELEASE_AFTER_MS);
        assert_eq!(workers.release_after_ms, 300_000, "5-minute default");
        assert!(!workers.allow_public, "default");
        assert!(!workers.enrolled_only, "default");
        assert!(workers.tokens.is_empty(), "default");
        // No endpoints + workers + the plugin = valid (C6).
        assert!(cfg.validate().is_ok(), "was: {:?}", cfg.validate().err());

        let plain = ServeConfig::parse(&base).expect("valid config");
        assert!(plain.workers.is_none(), "absent section = feature off");
    }

    /// ADR-035: `workers.release_after_ms` parses; `0` is an explicit
    /// "disabled", never a typo-refused value.
    #[test]
    fn workers_release_after_ms_parses_including_disabled() {
        let base = valid_yaml();
        let with = |value: u64| {
            format!(
                "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
                 workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n  \
                 release_after_ms: {value}\n"
            )
        };
        let cfg = ServeConfig::parse(&with(600_000)).expect("explicit release_after_ms parses");
        assert_eq!(
            cfg.workers
                .clone()
                .expect("section present")
                .release_after_ms,
            600_000
        );
        let off = ServeConfig::parse(&with(0)).expect("0 = disabled parses");
        assert_eq!(
            off.workers
                .clone()
                .expect("section present")
                .release_after_ms,
            0
        );
        assert!(off.validate().is_ok());
    }

    /// The enrolled form parses: per-worker token env names, the
    /// strict-mode flag and the dynamic queue bound.
    #[test]
    fn workers_enrolled_form_parses() {
        let base = valid_yaml();
        let cfg_text = format!(
            "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
             workers:\n  listen: \"127.0.0.1:7400\"\n  enrolled_only: true\n  \
             queue_timeout_ms: 600000\n  offline_after_ms: 120000\n  \
             tokens:\n    iphone: KIAN_IPHONE_TOKEN\n    build-box: KIAN_BUILD_TOKEN\n"
        );
        let cfg = ServeConfig::parse(&cfg_text).expect("enrolled workers config");
        let workers = cfg.workers.clone().expect("section present");
        assert!(workers.token_env.is_none());
        assert!(workers.enrolled_only);
        assert_eq!(workers.queue_timeout_ms, 600_000);
        assert_eq!(workers.queue_max_redrives, 1, "default when omitted");
        assert_eq!(workers.offline_after_ms, 120_000);
        assert_eq!(
            workers.tokens.get("iphone").map(String::as_str),
            Some("KIAN_IPHONE_TOKEN")
        );
        assert!(cfg.validate().is_ok(), "was: {:?}", cfg.validate().err());
    }

    /// Unknown keys refuse (deny_unknown_fields, C1).
    #[test]
    fn workers_section_refuses_unknown_keys() {
        let base = valid_yaml();
        let cfg_text = format!(
            "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
             workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n  bogus: 1\n"
        );
        assert!(ServeConfig::parse(&cfg_text).is_err());
    }

    /// The C2 matrix, one refusal per row: unparseable listen, public
    /// bind without the explicit override, zero timeouts, no token
    /// source, blank token names, strict mode without enrollment, and
    /// invalid token ids.
    #[test]
    fn workers_validations_refuse_fail_closed() {
        fn workers(
            listen: &str,
            token_env: Option<&str>,
            offline: u64,
            queue: u64,
        ) -> WorkersConfig {
            WorkersConfig {
                listen: listen.into(),
                token_env: token_env.map(str::to_string),
                offline_after_ms: offline,
                allow_public: false,
                queue_timeout_ms: queue,
                queue_max_redrives: 1,
                queue_redrive_grace_ms: DEFAULT_WORKER_QUEUE_REDRIVE_GRACE_MS,
                release_after_ms: DEFAULT_WORKER_RELEASE_AFTER_MS,
                enrolled_only: false,
                tokens: BTreeMap::new(),
            }
        }
        let case = |listen: &str, token_env: Option<&str>, offline: u64| {
            workers(listen, token_env, offline, 60_000).validate()
        };
        let err = case("not-an-addr", Some("T"), 30_000).unwrap_err();
        assert!(err.to_string().contains("host:port"), "was: {err}");
        let err = case("8.8.8.8:7400", Some("T"), 30_000).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("public address"), "was: {text}");
        assert!(text.contains("Tailscale") || text.contains("SSH tunnel"));
        assert!(text.contains("allow_public"));
        // The explicit override lifts it (validated at the WorkersConfig
        // level; the serve assembly still gates the env value).
        let lifted = WorkersConfig {
            allow_public: true,
            ..workers("8.8.8.8:7400", Some("T"), 30_000, 60_000)
        };
        assert!(lifted.validate().is_ok());
        let err = case("127.0.0.1:7400", Some("T"), 0).unwrap_err();
        assert!(err.to_string().contains("offline_after_ms must be > 0"));
        let err = case("127.0.0.1:7400", Some("  "), 30_000).unwrap_err();
        assert!(err.to_string().contains("token_env"));
        // No source at all refuses.
        let err = case("127.0.0.1:7400", None, 30_000).unwrap_err();
        assert!(err.to_string().contains("required"), "was: {err}");
        // Zero queue timeout refuses.
        let err = workers("127.0.0.1:7400", Some("T"), 30_000, 0)
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("queue_timeout_ms must be > 0"));
        // Strict mode needs enrollment and no shared fallback.
        let strict_no_tokens = WorkersConfig {
            enrolled_only: true,
            ..workers("127.0.0.1:7400", None, 30_000, 60_000)
        };
        assert!(
            strict_no_tokens
                .validate()
                .unwrap_err()
                .to_string()
                .contains("enrolled_only requires")
        );
        let strict_with_shared = WorkersConfig {
            enrolled_only: true,
            tokens: BTreeMap::from([("iphone".into(), "IPHONE_TOKEN".into())]),
            ..workers("127.0.0.1:7400", Some("T"), 30_000, 60_000)
        };
        assert!(
            strict_with_shared
                .validate()
                .unwrap_err()
                .to_string()
                .contains("forbids `token_env`")
        );
        // Token declarations validate their ids.
        let bad_id = WorkersConfig {
            tokens: BTreeMap::from([("bad\nid".into(), "T".into())]),
            ..workers("127.0.0.1:7400", None, 30_000, 60_000)
        };
        assert!(
            bad_id
                .validate()
                .unwrap_err()
                .to_string()
                .contains("worker ids")
        );
        // Wildcards count as public (C3).
        for wildcard in ["0.0.0.0:7400", "[::]:7400"] {
            assert!(case(wildcard, Some("T"), 30_000).is_err(), "{wildcard}");
        }
    }

    /// The presence locks (C6): `workers:` without the remote-exec
    /// plugin refuses; `async_tools` on workers-only boots; dsh plus
    /// `workers:` refuses.
    #[test]
    fn workers_presence_locks() {
        const WORKERS: &str =
            "workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n";
        const ENDPOINTS: &str = "endpoints:\n  - id: ep1\n    addr: \"127.0.0.1:7341\"\n    queue_timeout_ms: 5000\n    offline_after_ms: 500\n";

        // workers: without the remote-exec plugin → refuse.
        let no_plugin = superagent_yaml()
            .replace(ENDPOINTS, WORKERS)
            .replace(", \"remote-exec\"", "");
        let err = ServeConfig::parse(&no_plugin).unwrap_err().to_string();
        assert!(err.contains("remote-exec"), "was: {err}");

        // `async_tools` with workers-only (zero endpoints) is now legal.
        let with_async = superagent_yaml()
            .replace(ENDPOINTS, WORKERS)
            .replace("superagent:\n", "superagent:\n  async_tools: true\n");
        let cfg = ServeConfig::parse(&with_async).expect("async_tools on workers-only");
        assert!(cfg.validate().is_ok(), "was: {:?}", cfg.validate().err());

        // dsh + workers → refuse (the D12 rationale extends to the
        // dynamic surface).
        let with_dsh = superagent_yaml().replace(ENDPOINTS, WORKERS).replace(
            "superagent:\n",
            "superagent:\n  harness:\n    dsh:\n      command: /usr/bin/true\n",
        );
        let err = ServeConfig::parse(&with_dsh).unwrap_err().to_string();
        assert!(err.contains("mutually exclusive"), "was: {err}");
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
        for bad in ["maybe", "ALLOW", "ask_user"] {
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

    /// The confirmation arm parses, but `ask` on a superagent worker
    /// agent refuses boot in phase 1 (CONFIRMATION-MODEL C2) — the
    /// worker has no waiting path yet.
    #[test]
    fn ask_on_a_worker_agent_refuses_boot() {
        // The default agent accepts `ask` (plain sessions).
        let sa = valid_yaml().replace(
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n",
            "  tools: [\"fs_read\", \"fs_write\", \"exec\"]\n  permissions:\n    fs_read: ask\n",
        );
        ServeConfig::parse(&sa).expect("ask on the default agent parses");
        // A superagent worker agent refuses it.
        let worker = superagent_yaml().replace(
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n    evidence_gate:",
            "    tools: [\"fs_read\", \"fs_write\", \"exec\"]\n    permissions:\n      fs_read: ask\n    evidence_gate:",
        );
        let err = ServeConfig::parse(&worker).expect_err("ask on a worker agent must refuse boot");
        assert!(
            err.to_string().contains("ask") && err.to_string().contains("worker"),
            "unexpected error: {err}"
        );
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
    // the join fan-in policy (R17 C7, pin P4-5)
    // -----------------------------------------------------------------------

    #[test]
    fn any_and_quorum_join_values_refuse_to_boot_naming_the_deferral() {
        // (R17-C7, pin P4-5) v1 evaluates only `all` — the one shape
        // with an evaluation site (one spawn, one parent). `any` and
        // `quorum:<n>` refuse to boot naming the missing evaluation
        // site and the §9 deferral; they are never
        // accepted-and-ignored (which would imply any-of success,
        // §5.3:588).
        for join in ["any", "quorum:2"] {
            let yaml = superagent_yaml().replacen(
                "superagent:\n",
                format!("superagent:\n  multiagent:\n    join: {join}\n").as_str(),
                1,
            );
            let cfg = ServeConfig::parse(&yaml).unwrap();
            let sa = cfg.superagent.as_ref().unwrap();
            let err = sa
                .multiagent_config()
                .expect_err("join any/quorum must refuse to boot");
            let msg = err.to_string();
            assert!(msg.contains("superagent.multiagent"), "{msg}");
            assert!(msg.contains("join"), "{msg}");
            assert!(
                msg.contains("no evaluation site"),
                "names the missing evaluation site: {msg}"
            );
            assert!(
                msg.contains("deferred") || msg.contains("deferral"),
                "names the deferral: {msg}"
            );
        }
        // A quorum of zero is a malformed bound, not a deferral.
        let zero = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  multiagent:\n    join: quorum:0\n",
            1,
        );
        let cfg = ServeConfig::parse(&zero).unwrap();
        let err = cfg
            .superagent
            .as_ref()
            .unwrap()
            .multiagent_config()
            .expect_err("quorum:0 must refuse");
        assert!(err.to_string().contains("n ≥ 1"), "names the bound: {err}");
    }

    #[test]
    fn join_all_and_absent_boot_and_unknown_values_refuse_naming_the_forms() {
        // Absent: the default policy (`all`) — the block still boots.
        let absent = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  multiagent:\n    max_children: 2\n",
            1,
        );
        let cfg = ServeConfig::parse(&absent).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert!(
            sa.multiagent_config().unwrap().is_some(),
            "absent join boots with the default"
        );
        // Explicit `all`: accepted.
        let all = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  multiagent:\n    join: all\n",
            1,
        );
        let cfg = ServeConfig::parse(&all).unwrap();
        assert!(
            cfg.superagent
                .as_ref()
                .unwrap()
                .multiagent_config()
                .unwrap()
                .is_some(),
            "join: all boots"
        );
        // An unknown value names the accepted forms.
        let bogus = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  multiagent:\n    join: sometimes\n",
            1,
        );
        let cfg = ServeConfig::parse(&bogus).unwrap();
        let err = cfg
            .superagent
            .as_ref()
            .unwrap()
            .multiagent_config()
            .expect_err("unknown join value must refuse");
        assert!(
            err.to_string().contains("`all` | `any` | `quorum:<n>`"),
            "names the accepted forms: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // the task-board switch block (R17 C4, pin P3-1)
    // -----------------------------------------------------------------------

    #[test]
    fn board_block_defaults_off_and_parses_both_keys() {
        // Absent block: both keys false — feature-off, byte-identical
        // (`anchors.md` rule 4), and the conversion is simply off.
        let cfg = ServeConfig::parse(&superagent_yaml()).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(sa.board, BoardConfigDef::default());
        assert_eq!(
            sa.board_config().unwrap(),
            BoardConfig {
                enabled: false,
                gating: false
            }
        );

        // The block parses both keys; `enabled` alone is legal P3.
        let on = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  board:\n    enabled: true\n",
            1,
        );
        let cfg = ServeConfig::parse(&on).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(
            sa.board_config().unwrap(),
            BoardConfig {
                enabled: true,
                gating: false
            }
        );
    }

    #[test]
    fn board_gating_true_boots_with_the_multiagent_block_and_refuses_without_it() {
        // (R17-C6, pin P4-1 / §7.1:737-741) `gating: true` is live
        // since the key groups landed: it converts cleanly when (and
        // only when) the `multiagent` block is present — neither board
        // key can enable anything multiagent has not already enabled.
        let with_multiagent = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  multiagent:\n    max_children: 2\n  board:\n    gating: true\n",
            1,
        );
        let cfg = ServeConfig::parse(&with_multiagent).unwrap();
        let sa = cfg.superagent.as_ref().unwrap();
        assert_eq!(
            sa.board_config().unwrap(),
            BoardConfig {
                enabled: false,
                gating: true
            }
        );

        // Without the multiagent block: a refusal naming the block,
        // never a silent no-op (§7.1:737-741).
        let gating = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  board:\n    gating: true\n",
            1,
        );
        let err = ServeConfig::parse(&gating)
            .expect_err("board.gating without multiagent must refuse to boot");
        let msg = err.to_string();
        assert!(msg.contains("superagent.board"), "{msg}");
        assert!(msg.contains("multiagent"), "{msg}");
    }

    #[test]
    fn board_unknown_keys_refuse_boot() {
        let typo = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  board:\n    enabeld: true\n",
            1,
        );
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "deny_unknown_fields must refuse a mistyped board key"
        );
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

    // -----------------------------------------------------------------------
    // the wechat channel wire layer (CHANNEL-WECHAT §7)
    // -----------------------------------------------------------------------

    fn wechat_yaml(extra: &str) -> String {
        superagent_yaml().replacen(
            "  channels:\n    mock-im:",
            &format!("  channels:\n    wechat:\n      type: wechat\n{extra}    mock-im:"),
            1,
        )
    }

    /// A wechat section over a temp credential path (the file itself is
    /// written by the caller — `to_runtime` loads it fail-closed).
    fn wechat_section_with_credentials(tag: &str) -> (String, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "kian-wechat-cfg-{tag}-{}.json",
            uuid::Uuid::new_v4()
        ));
        kian_superagent::wechat::credentials::save(
            &path,
            &kian_superagent::wechat::credentials::WechatCredentials::new(
                "https://ilinkai.weixin.qq.com",
                "fake-bot-token-cfg",
                "fake-bot-1",
                "fake-user-1",
                1730000000000,
            ),
        )
        .expect("write credential fixture");
        (
            format!(
                "      wechat:\n        credentials_file: \"{}\"\n        topics:\n          oUSER@im.wechat: wechat\n",
                path.display()
            ),
            path,
        )
    }

    #[test]
    fn wechat_channel_config_parses_with_defaults() {
        let (section, cred) = wechat_section_with_credentials("defaults");
        let cfg = ServeConfig::parse(&wechat_yaml(&section)).expect("wechat config parses");
        let sa = cfg.superagent.as_ref().unwrap();
        let wechat = sa.channels["wechat"].wechat.as_ref().expect("section");
        assert_eq!(wechat.api_base, "https://ilinkai.weixin.qq.com");
        assert_eq!(wechat.poll_timeout_ms, 45_000);
        assert_eq!(wechat.context_ttl_ms, 3_600_000);
        assert_eq!(
            wechat.topics,
            BTreeMap::from([("oUSER@im.wechat".to_string(), "wechat".to_string())])
        );
        let runtime = wechat.to_runtime("wechat").expect("credentials exist");
        assert_eq!(runtime.context_ttl, std::time::Duration::from_secs(3600));
        assert_eq!(
            runtime.context_state_file,
            kian_superagent::wechat::context_state::default_state_path(&cred),
            "default state file sits beside the credential file"
        );

        // Explicit overrides parse and reach the runtime config.
        let state = format!("{}.state", cred.display());
        let overrides = section.replace(
            "      wechat:\n",
            &format!(
                "      wechat:\n        api_base: \"http://127.0.0.1:9\"\n        poll_timeout_ms: 5000\n        context_ttl_ms: 60000\n        context_state_file: \"{state}\"\n"
            ),
        );
        let cfg = ServeConfig::parse(&wechat_yaml(&overrides)).expect("overrides parse");
        let wechat = cfg.superagent.as_ref().unwrap().channels["wechat"]
            .wechat
            .as_ref()
            .unwrap();
        assert_eq!(wechat.api_base, "http://127.0.0.1:9");
        assert_eq!(wechat.poll_timeout_ms, 5_000);
        assert_eq!(wechat.context_ttl_ms, 60_000);
        let runtime = wechat.to_runtime("wechat").expect("credentials exist");
        assert_eq!(runtime.context_ttl, std::time::Duration::from_secs(60));
        assert_eq!(runtime.context_state_file, std::path::PathBuf::from(&state));
    }

    #[test]
    fn wechat_channel_section_is_fail_closed() {
        // `type: wechat` without a `wechat:` section.
        let err = ServeConfig::parse(&wechat_yaml("")).unwrap_err();
        assert!(
            err.to_string().contains("requires a `wechat:` section"),
            "{err}"
        );

        // A `wechat:` section on a non-wechat channel.
        let misplaced = superagent_yaml().replacen(
            "    mock-im:\n      type: mock-im",
            "    mock-im:\n      type: mock-im\n      wechat:\n        credentials_file: ./x.json\n        topics:\n          oU: wechat",
            1,
        );
        let err = ServeConfig::parse(&misplaced).unwrap_err();
        assert!(
            err.to_string()
                .contains("belongs to `type: wechat` channels only"),
            "{err}"
        );

        // Degenerate shapes refuse (fail-closed, never defaults).
        for (extra, needle) in [
            (
                "      wechat:\n        topics:\n          oUSER@im.wechat: wechat\n",
                "credentials_file",
            ),
            (
                "      wechat:\n        credentials_file: \"  \"\n        topics:\n          oU: wechat\n",
                "credentials_file",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        topics: {}\n",
                "at least one user id",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        topics:\n          \"\": wechat\n",
                "must be non-empty",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        topics:\n          oU: \"  \"\n",
                "must be non-empty",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        poll_timeout_ms: 0\n        topics:\n          oU: wechat\n",
                "poll_timeout_ms must be > 0",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        api_base: \"ftp://bad\"\n        topics:\n          oU: wechat\n",
                "http(s):// URL",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        api_base: \"http://\"\n        topics:\n          oU: wechat\n",
                "non-empty host",
            ),
        ] {
            let err = ServeConfig::parse(&wechat_yaml(extra)).unwrap_err();
            assert!(err.to_string().contains(needle), "unexpected: {err}");
        }
    }

    #[test]
    fn wechat_context_state_is_fail_closed() {
        // Parse-level: a zero TTL and a blank explicit path refuse.
        for (extra, needle) in [
            (
                "      wechat:\n        credentials_file: ./x.json\n        context_ttl_ms: 0\n        topics:\n          oU: wechat\n",
                "context_ttl_ms must be > 0",
            ),
            (
                "      wechat:\n        credentials_file: ./x.json\n        context_state_file: \"  \"\n        topics:\n          oU: wechat\n",
                "context_state_file must",
            ),
        ] {
            let err = ServeConfig::parse(&wechat_yaml(extra)).unwrap_err();
            assert!(err.to_string().contains(needle), "{err}");
        }
        // Runtime-level: the state file may not be the credential file.
        let (section, cred) = wechat_section_with_credentials("state-same");
        let same = format!(
            "      wechat:\n        credentials_file: \"{}\"\n        context_state_file: \"{}\"\n        topics:\n          oUSER@im.wechat: wechat\n",
            cred.display(),
            cred.display()
        );
        let cfg = ServeConfig::parse(&wechat_yaml(&same)).expect("shape parses");
        let err = cfg.superagent.as_ref().unwrap().channels["wechat"]
            .wechat
            .as_ref()
            .unwrap()
            .to_runtime("wechat")
            .unwrap_err();
        assert!(err.to_string().contains("must differ"), "{err}");
        let _ = section;
    }

    #[test]
    fn at_most_one_wechat_channel_per_config() {
        let doubled = superagent_yaml().replacen(
            "  channels:\n    mock-im:",
            "  channels:\n    wx1:\n      type: wechat\n      wechat:\n        credentials_file: ./a.json\n        topics:\n          oU: wechat\n    wx2:\n      type: wechat\n      wechat:\n        credentials_file: ./b.json\n        topics:\n          oV: wechat\n    mock-im:",
            1,
        );
        let err = ServeConfig::parse(&doubled).unwrap_err();
        assert!(
            err.to_string()
                .contains("at most one `type: wechat` channel"),
            "{err}"
        );
    }

    #[test]
    fn wechat_credentials_fail_closed_at_conversion_naming_the_command() {
        // Parse is credential-agnostic (login runs before the file
        // exists); `to_runtime` (serve assembly) is the fail-closed
        // gate.
        let missing = "      wechat:\n        credentials_file: ./does-not-exist-wechat.json\n        topics:\n          oUSER@im.wechat: wechat\n";
        let cfg =
            ServeConfig::parse(&wechat_yaml(missing)).expect("config parses without the file");
        let sa = cfg.superagent.as_ref().unwrap();
        let section = sa.channels["wechat"].wechat.as_ref().unwrap();
        let err = section.to_runtime("wechat").unwrap_err().to_string();
        assert!(
            err.contains("kian wechat login"),
            "the startup error must name the command: {err}"
        );

        // A present, valid file converts to the runtime config with
        // the contract fields.
        let (section_yaml, path) = wechat_section_with_credentials("converts");
        let cfg = ServeConfig::parse(&wechat_yaml(&section_yaml)).expect("parses");
        let section = cfg.superagent.as_ref().unwrap().channels["wechat"]
            .wechat
            .as_ref()
            .unwrap();
        let runtime = section.to_runtime("wechat").expect("converts");
        assert_eq!(runtime.api_base, "https://ilinkai.weixin.qq.com");
        assert_eq!(runtime.credentials_file, path);
        assert_eq!(runtime.poll_timeout.as_millis(), 45_000);
        assert_eq!(runtime.topics["oUSER@im.wechat"], "wechat");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn wechat_channels_are_untrusted_by_default_and_cross_reference() {
        let (section, path) = wechat_section_with_credentials("untrusted");
        // A subscription + speak rule + untrusted_channels listing all
        // reference the wechat channel by name and validate.
        let yaml = wechat_yaml(&section)
            .replace(
                "  subscriptions:",
                "  subscriptions:\n    - channel: wechat\n      match: always\n      route: router",
            )
            .replace("  channels:", "  untrusted_channels: [wechat]\n  channels:");
        let cfg = ServeConfig::parse(&yaml).expect("cross-references validate");
        let sa = cfg.superagent.as_ref().unwrap();
        // Forced untrusted: listed or not, the wechat channel is in
        // the runtime set (no opt-out).
        let set = sa.runtime_untrusted_set().expect("set converts");
        assert!(set.contains("wechat"), "wechat is untrusted by default");

        // An unknown cross-reference still refuses.
        let bad = wechat_yaml(&section).replace(
            "  subscriptions:",
            "  subscriptions:\n    - channel: wechat-ghost\n      match: always\n      route: router",
        );
        let err = ServeConfig::parse(&bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("not declared in superagent.channels"),
            "{err}"
        );
        std::fs::remove_file(&path).ok();
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

    // -- memory injection config (P2, kian-101) ----------------------

    /// The injection source parses: `memory.session` rides through, the
    /// caps default to the bundle defaults; absent `session` (and
    /// absent `inject`) is injection off.
    #[test]
    fn memory_injection_config_parses_with_defaults() {
        let cfg = ServeConfig::parse(&memory_yaml()).expect("valid config");
        let memory = cfg.memory.as_ref().expect("memory on");
        assert!(memory.session.is_none(), "absent session = injection off");
        assert!(memory.inject.is_none());

        let uuid = uuid::Uuid::new_v4().to_string();
        let yaml = valid_yaml().replace(
            "drain:",
            &format!("memory:\n  scope: default\n  session: {uuid}\n  dreamer:\n    agent: default\ndrain:"),
        );
        let cfg = ServeConfig::parse(&yaml).expect("valid config");
        let memory = cfg.memory.as_ref().expect("memory on");
        assert_eq!(memory.session.as_deref(), Some(uuid.as_str()));
        assert!(memory.inject.is_none(), "no inject block = default caps");
    }

    /// Explicit injection caps ride through to `MemoryIndexConfig`.
    #[test]
    fn memory_injection_explicit_caps_parse() {
        let uuid = uuid::Uuid::new_v4().to_string();
        let yaml = valid_yaml().replace(
            "drain:",
            &format!(
                "memory:\n  scope: default\n  session: {uuid}\n  inject:\n    max_items: 5\n    max_bytes: 1024\n  dreamer:\n    agent: default\ndrain:"
            ),
        );
        let cfg = ServeConfig::parse(&yaml).expect("valid config");
        let inject = cfg
            .memory
            .as_ref()
            .expect("memory on")
            .inject
            .as_ref()
            .expect("inject block");
        assert_eq!(inject.max_items, Some(5));
        assert_eq!(inject.max_bytes, Some(1024));
        assert_eq!(
            inject.caps(),
            kian_memory::MemoryIndexConfig {
                max_items: 5,
                max_bytes: 1024,
            }
        );
        // The bundle defaults fill absent keys.
        let partial = valid_yaml().replace(
            "drain:",
            &format!(
                "memory:\n  scope: default\n  session: {uuid}\n  inject:\n    max_items: 3\n  dreamer:\n    agent: default\ndrain:"
            ),
        );
        let cfg = ServeConfig::parse(&partial).expect("valid config");
        let caps = cfg
            .memory
            .as_ref()
            .expect("memory on")
            .inject
            .as_ref()
            .expect("inject block")
            .caps();
        assert_eq!(caps.max_items, 3);
        assert_eq!(caps.max_bytes, kian_memory::DEFAULT_INJECT_MAX_BYTES);
    }

    /// The fail-closed matrix for the injection block: a non-uuid
    /// session, caps without a session, zero caps, unknown inject keys.
    #[test]
    fn memory_injection_validation_is_fail_closed() {
        let cases = [
            (
                "session not a uuid",
                "memory:\n  scope: default\n  session: not-a-uuid\n  dreamer:\n    agent: default\n",
                "memory.session",
            ),
            (
                "inject without session",
                "memory:\n  scope: default\n  inject:\n    max_items: 5\n  dreamer:\n    agent: default\n",
                "memory.inject requires memory.session",
            ),
            (
                "zero max_items",
                "memory:\n  scope: default\n  session: 00000000-0000-0000-0000-000000000000\n  inject:\n    max_items: 0\n  dreamer:\n    agent: default\n",
                "memory.inject.max_items",
            ),
            (
                "zero max_bytes",
                "memory:\n  scope: default\n  session: 00000000-0000-0000-0000-000000000000\n  inject:\n    max_bytes: 0\n  dreamer:\n    agent: default\n",
                "memory.inject.max_bytes",
            ),
            (
                "unknown inject key",
                "memory:\n  scope: default\n  session: 00000000-0000-0000-0000-000000000000\n  inject:\n    cadence: hourly\n  dreamer:\n    agent: default\n",
                "unknown field",
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

    // -- api.event_stream (READ-MODEL §4.2, P3) ---------------------

    /// Default: the fan-in event stream is ON (the section is absent —
    /// an explicit session allowlist only, never a wildcard).
    #[test]
    fn event_stream_defaults_to_enabled() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid config");
        assert!(cfg.api.event_stream.enabled);
        cfg.validate().expect("defaults validate");
    }

    /// The gate parses explicitly in both directions and refuses
    /// unknown keys (deny_unknown_fields at both levels).
    #[test]
    fn event_stream_gate_parses_fail_closed() {
        let off = valid_yaml() + "api:\n  event_stream:\n    enabled: false\n";
        let cfg = ServeConfig::parse(&off).expect("parse");
        assert!(!cfg.api.event_stream.enabled);
        cfg.validate().expect("explicit off validates");

        let on = valid_yaml() + "api:\n  event_stream:\n    enabled: true\n";
        let cfg = ServeConfig::parse(&on).expect("parse");
        assert!(cfg.api.event_stream.enabled);

        let typo = valid_yaml() + "api:\n  event_stream:\n  enbled: true\n";
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "a typo'd gate key must refuse, not silently default"
        );
        let typo = valid_yaml() + "api:\n  wildcard: true\n";
        assert!(
            ServeConfig::parse(&typo).is_err(),
            "an unknown api key must refuse"
        );
    }

    // ------------------------------------------------------------------
    // (ADR-020) endpoints[].description + the worker catalog (D5) and
    // the dsh × endpoints refusal (D12)
    // ------------------------------------------------------------------

    #[test]
    fn endpoint_description_is_optional_and_validates_fail_closed() {
        // Absent parses (byte-identical legacy entry) and yields no
        // catalog description.
        let cfg = ServeConfig::parse(&superagent_yaml()).expect("parses");
        assert_eq!(cfg.endpoints.len(), 1);
        assert_eq!(cfg.endpoints[0].description, None);
        assert_eq!(
            cfg.worker_catalog(),
            vec![kian_superagent::WorkerEntry::new("ep1")]
        );
        assert_eq!(
            cfg.worker_queue_timeouts(),
            std::collections::BTreeMap::from([("ep1".to_string(), 5_000u64)])
        );

        // A sane description parses and flows into the catalog in
        // config order.
        let described = superagent_yaml().replace(
            "  - id: ep1\n",
            "  - id: ep1\n    description: \"local macOS dev machine — cargo, git\"\n",
        );
        let cfg = ServeConfig::parse(&described).expect("description parses");
        assert_eq!(
            cfg.worker_catalog(),
            vec![kian_superagent::WorkerEntry::with_description(
                "ep1",
                "local macOS dev machine — cargo, git"
            )]
        );

        // Fail-closed: empty / whitespace / over-200-chars / control
        // characters all refuse to start (prompt-prefix content).
        for bad in [
            "\"\"",
            "\"   \"",
            &format!("\"{}\"", "x".repeat(201)),
            "\"line one\\nline two\"",
            "\"tab\\there\"",
        ] {
            let yaml = superagent_yaml().replace(
                "  - id: ep1\n",
                &format!("  - id: ep1\n    description: {bad}\n"),
            );
            let err = match ServeConfig::parse(&yaml) {
                Err(err) => err,
                Ok(_) => panic!("description {bad:?} must refuse"),
            };
            assert!(err.to_string().contains("description"), "{bad:?}: {err}");
        }

        // Exactly 200 characters parse (the bound is inclusive).
        let exact = superagent_yaml().replace(
            "  - id: ep1\n",
            &format!("  - id: ep1\n    description: \"{}\"\n", "y".repeat(200)),
        );
        assert!(ServeConfig::parse(&exact).is_ok());
    }

    #[test]
    fn dsh_harness_with_endpoints_refuses_boot() {
        // D12: the combination is a fail-closed config error naming
        // both sections; zero-endpoints dsh configs are unchanged.
        let combined = superagent_yaml().replace(
            "superagent:\n  router:",
            "superagent:\n  harness:\n    dsh:\n      command: /usr/bin/true\n  router:",
        );
        let err = ServeConfig::parse(&combined).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("`superagent.harness.dsh` and a worker surface"),
            "{text}"
        );
        assert!(
            text.contains("non-empty `endpoints:` list or a `workers:` section"),
            "{text}"
        );
        assert!(text.contains("mutually exclusive"), "{text}");
        assert!(text.contains("drop one of the two sections"), "{text}");

        // Zero endpoints + dsh boots as before (the refusal is
        // additive): splice the endpoints list out and drop the
        // remote-exec plugin.
        let dsh_only = superagent_yaml()
            .replace(
                "superagent:\n  router:",
                "superagent:\n  harness:\n    dsh:\n      command: /usr/bin/true\n  router:",
            )
            .replace(", \"remote-exec\"", "");
        let start = dsh_only.find("endpoints:\n").expect("endpoints block");
        let end = dsh_only.find("providers:\n").expect("providers key");
        let spliced = format!("{}{}", &dsh_only[..start], &dsh_only[end..]);
        assert!(
            ServeConfig::parse(&spliced).is_ok(),
            "zero-endpoints dsh config boots exactly as before"
        );
    }

    // -- superagent.roster: (R17-C1) --------------------------------

    /// The superagent fixture plus a `research` agent (the roster's
    /// resolution target) — the base every roster case splices onto.
    fn roster_base_yaml() -> String {
        superagent_yaml().replace(
            "superagent:\n  router:",
            "  research:\n    id: research\n    version: 1\n    model_slots:\n      \
             main:\n        provider: openai\n        model: gpt-research\n    tools: \
             [\"fs_read\", \"recall_event\"]\nsuperagent:\n  router:",
        )
    }

    /// The base fixture with `roster_block` spliced under
    /// `superagent:` (the caller indents to the entry level).
    fn roster_yaml(roster_block: &str) -> String {
        roster_base_yaml().replace(
            "  worker:\n    agent: worker\n    pinned_version: 7\n",
            &format!("  worker:\n    agent: worker\n    pinned_version: 7\n{roster_block}"),
        )
    }

    /// One valid roster entry yaml (agent `research`, a leaf).
    const VALID_ENTRY: &str = "  roster:\n    - id: researcher\n      description: Finds and \
         cites sources\n      agent: research\n      mandate_template:\n        scope: read-only \
         research\n        strength: inferred\n      tools_allow: [\"fs_read\"]\n      limits: \
         {max_children: 0}\n";

    #[test]
    fn roster_absent_or_empty_converts_to_none_feature_off() {
        // Pin P2-6: absent/empty roster ⇒ `None` ⇒ no `list_agents`,
        // no validation, today's bytes.
        let base = ServeConfig::parse(&roster_base_yaml()).expect("base config");
        assert_eq!(base.roster_config().expect("absent roster"), None);
        let empty = roster_yaml("  roster: []\n");
        let cfg = ServeConfig::parse(&empty).expect("empty roster config");
        assert_eq!(cfg.roster_config().expect("empty roster"), None);
    }

    #[test]
    fn an_entry_naming_no_configured_agent_refuses_boot_naming_the_known_ids() {
        // Pin P2-1: discovery is exactly `named_agent_def` — an
        // unknown `agent` refuses boot, naming the known `agents:`
        // ids (not the default `agent:`).
        let yaml = roster_yaml(
            "  roster:\n    - id: researcher\n      description: Ghost\n      agent: nope\n      \
             mandate_template: {scope: s, strength: inferred}\n      tools_allow: []\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("parses");
        let err = cfg.roster_config().expect_err("unknown agent refuses");
        let text = err.to_string();
        assert!(text.contains("nope"), "names the unknown id: {text}");
        assert!(
            text.contains("router") && text.contains("worker") && text.contains("research"),
            "names the known agents: ids: {text}"
        );
        assert!(
            text.contains("superagent.roster[0]"),
            "names the key: {text}"
        );
    }

    #[test]
    fn duplicate_roster_ids_refuse_boot_at_the_config_gate() {
        let block = format!(
            "{VALID_ENTRY}    - id: researcher\n      description: Again\n      agent: research\n      \
             mandate_template: {{scope: s, strength: inferred}}\n      tools_allow: []\n"
        );
        let cfg = ServeConfig::parse(&roster_yaml(&block)).expect("parses");
        let err = cfg.roster_config().expect_err("duplicate ids refuse");
        let text = err.to_string();
        assert!(
            text.contains("duplicate") && text.contains("researcher"),
            "{text}"
        );
        assert!(text.contains("superagent.roster"), "{text}");
    }

    #[test]
    fn a_widening_tools_allow_refuses_boot_at_the_config_gate() {
        // M3 / §5.1:551-552 — `exec` is not on the `research` agent's
        // surface, so listing it refuses boot.
        let yaml = roster_yaml(
            "  roster:\n    - id: researcher\n      description: Widening\n      agent: \
             research\n      mandate_template: {scope: s, strength: inferred}\n      \
             tools_allow: [\"fs_read\", \"exec\"]\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("parses");
        let err = cfg.roster_config().expect_err("widening refuses");
        let text = err.to_string();
        assert!(text.contains("exec"), "names the tool: {text}");
        assert!(
            text.contains("superagent.roster[0]"),
            "names the key: {text}"
        );
    }

    #[test]
    fn an_out_of_subset_output_schema_refuses_boot_naming_the_key_and_keyword() {
        // Pin P2-3: `oneOf` is outside the closed subset — the
        // refusal names the keyword and the key.
        let yaml = roster_yaml(
            "  roster:\n    - id: researcher\n      description: Scheming\n      agent: \
             research\n      mandate_template: {scope: s, strength: inferred}\n      \
             tools_allow: []\n      output_schema:\n        oneOf:\n          - type: string\n",
        );
        let cfg = ServeConfig::parse(&yaml).expect("parses");
        let err = cfg.roster_config().expect_err("outside the subset refuses");
        let text = err.to_string();
        assert!(text.contains("oneOf"), "names the keyword: {text}");
        assert!(
            text.contains("superagent.roster[0].output_schema") || {
                // the RosterError prefix composes as
                // `superagent.roster[0]: … output_schema: …`
                text.contains("superagent.roster[0]") && text.contains("output_schema")
            },
            "names the key: {text}"
        );
    }

    #[test]
    fn a_valid_roster_converts_in_config_order_with_leaf_legal() {
        // §3.1:360-361: `max_children: 0` is a legal leaf; a missing
        // `limits` takes the §5.3 default (4); config order holds.
        let block = format!(
            "{VALID_ENTRY}    - id: recaller\n      description: Recalls events\n      agent: \
             research\n      mandate_template: {{scope: s, strength: explicit}}\n      \
             tools_allow: [\"fs_read\", \"recall_event\"]\n      output_schema:\n        \
             type: object\n        required: [summary]\n        properties:\n          summary: \
             {{type: string}}\n"
        );
        let cfg = ServeConfig::parse(&roster_yaml(&block)).expect("parses");
        let roster = cfg
            .roster_config()
            .expect("valid roster converts")
            .expect("some");
        assert_eq!(roster.known_ids(), vec!["researcher", "recaller"]);
        let leaf = roster.resolve("researcher").expect("leaf resolves");
        assert_eq!(leaf.limits.max_children, 0, "the leaf fact");
        assert_eq!(leaf.agent, "research");
        let defaulted = roster.resolve("recaller").expect("defaulted resolves");
        assert_eq!(defaulted.limits.max_children, 4, "the §5.3 default");
        assert_eq!(
            defaulted.mandate_template.strength,
            kian_superagent::MandateStrength::Explicit
        );
        assert!(
            defaulted.output_schema.is_some(),
            "the schema rides the entry"
        );
    }

    // -- sidecars: the §8.1 config surface -------------------------

    const SIDECAR_SECTION: &str = r#"sidecars:
  plugins:
    approval-gate:
      manifest: "/etc/kian/plugins/approval-gate.toml"
    cost-audit:
      manifest: "/etc/kian/plugins/cost-audit.toml"
  scopes:
    server: ["approval-gate", "cost-audit"]
    worker:
      node-zulu:
        use: ["approval-gate"]
        limits:
          approval-gate: {deadline_ms: 200}
      node-yankee:
        limits:
          cost-audit: {max_payload_kib: 64}
"#;

    /// §8.1's example grammar parses and validates (the shape checks —
    /// the widening check runs in the assembly, where the manifests
    /// are loaded).
    #[test]
    fn sidecars_two_scope_grammar_parses() {
        let base = valid_yaml();
        let cfg = ServeConfig::parse(&format!("{base}{SIDECAR_SECTION}"))
            .expect("the §8.1 example is valid");
        let sidecars = cfg.sidecars.as_ref().expect("section present");
        assert_eq!(sidecars.scopes.server.len(), 2);
        let zulu = sidecars.scopes.worker.get("node-zulu").expect("zulu");
        assert_eq!(
            zulu.use_.as_deref(),
            Some(&["approval-gate".to_string()][..])
        );
        assert_eq!(
            zulu.limits["approval-gate"].deadline_ms,
            Some(200),
            "the worker-scope narrowing key"
        );
    }

    /// §13.3: an absent section is `None` — no sidecars, byte-identical.
    #[test]
    fn sidecars_absent_section_means_no_sidecars() {
        let cfg = ServeConfig::parse(&valid_yaml()).expect("valid");
        assert!(cfg.sidecars.is_none());
    }

    /// §8.1's closed grammar: `harness:` (or `profile:`) inside
    /// `scopes:` is an unknown key and refuses boot naming the key.
    #[test]
    fn sidecars_unknown_scope_key_refuses_naming_the_key() {
        let text = format!(
            "{base}sidecars:\n  scopes:\n    harness:\n      react: [\"x\"]\n",
            base = valid_yaml()
        );
        let err = ServeConfig::parse(&text).unwrap_err().to_string();
        assert!(err.contains("harness"), "names the offender: {err}");
        let text = format!(
            "{base}sidecars:\n  scopes:\n    profile:\n      router: [\"x\"]\n",
            base = valid_yaml()
        );
        let err = ServeConfig::parse(&text).unwrap_err().to_string();
        assert!(err.contains("profile"), "names the offender: {err}");
    }

    /// §8.1/§8.4: `use ⊆ scopes.server` is a static boot check — a
    /// subset violation names the worker and the name.
    #[test]
    fn sidecars_subset_violation_refuses_naming_worker_and_name() {
        let text = format!(
            "{base}sidecars:\n  plugins:\n    a:\n      manifest: \"/etc/a.toml\"\n    b:\n      manifest: \"/etc/b.toml\"\n  scopes:\n    server: [\"a\"]\n    worker:\n      node-bad:\n        use: [\"b\"]\n",
            base = valid_yaml()
        );
        let err = ServeConfig::parse(&text).unwrap_err().to_string();
        assert!(err.contains("node-bad") && err.contains("`b`"), "{err}");
        assert!(err.contains("subset_violation"), "carries the code: {err}");
    }

    /// §8.1: a set entry that is not a declared instance refuses boot
    /// (a typo cannot silently do nothing).
    #[test]
    fn sidecars_undeclared_instance_refuses() {
        let text = format!(
            "{base}sidecars:\n  plugins:\n    a:\n      manifest: \"/etc/a.toml\"\n  scopes:\n    server: [\"a\", \"typo-name\"]\n",
            base = valid_yaml()
        );
        let err = ServeConfig::parse(&text).unwrap_err().to_string();
        assert!(err.contains("typo-name"), "{err}");
        assert!(err.contains("undeclared_instance"), "{err}");
    }

    /// §6.2 rule 3 via §13.1: an instance key colliding with a
    /// KNOWN_PLUGINS name refuses boot.
    #[test]
    fn sidecars_known_plugin_name_collision_refuses() {
        let text = format!(
            "{base}sidecars:\n  plugins:\n    react-loop:\n      manifest: \"/etc/evil.toml\"\n",
            base = valid_yaml()
        );
        let err = ServeConfig::parse(&text).unwrap_err().to_string();
        assert!(err.contains("react-loop"), "{err}");
        assert!(err.contains("name_collision"), "{err}");
    }

    /// §8.1: an empty server list is legal ("no sidecars for anyone"),
    /// and so is an empty `use: []` at a worker.
    #[test]
    fn sidecars_empty_sets_are_legal() {
        let text = format!(
            "{base}sidecars:\n  plugins:\n    a:\n      manifest: \"/etc/a.toml\"\n  scopes:\n    server: []\n    worker:\n      w:\n        use: []\n",
            base = valid_yaml()
        );
        let cfg = ServeConfig::parse(&text).expect("empty sets are legal");
        let sidecars = cfg.sidecars.expect("present");
        assert!(sidecars.scopes.server.is_empty());
    }

    // -- superagent.cron (CRON-MODEL §3) --------------------------

    /// A channel map with one declared `cron-jobs` channel — the target
    /// every cron fixture below resolves against.
    fn cron_channels() -> std::collections::BTreeMap<String, ChannelConfig> {
        let mut channels = std::collections::BTreeMap::new();
        channels.insert(
            "cron-jobs".to_string(),
            ChannelConfig {
                channel_type: "mock-im".into(),
                lark: None,
                timer: None,
                github: None,
                wechat: None,
            },
        );
        channels
    }

    /// A fixed "now" (2027-01-15T08:00:00Z) — the §2.5 input is a pure
    /// function, so the fixtures never read the clock.
    fn cron_now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    /// Parses a `CronSection` document and resolves it against the fixture
    /// channels (`Err` is the fail-closed load path).
    fn cron_section_from(yaml: &str) -> Result<Vec<CronRuntimeSchedule>, ConfigError> {
        let section: CronSection = serde_yaml::from_str(yaml).unwrap();
        section.to_runtime(&cron_channels(), cron_now())
    }

    /// Two schedules round-trip through the wire form and resolve: one with
    /// an explicit `tz:` and non-default policies, one defaulting to the
    /// frozen host timezone with every §3.1 default.
    #[test]
    fn cron_section_round_trips_with_explicit_and_host_tz() {
        let yaml = r#"
schedules:
  - id: night-audit
    expr: "30 3 * * 1-5"
    tz: Europe/Berlin
    target: { channel: cron-jobs, topic: audit }
    prompt: "Audit .state/ for orphaned worktrees."
    worktree: true
    catch_up: skip
    coalesce_ms: 60000
    max_concurrent: 2
    timeout_ms: 120000
  - id: hourly-ping
    expr: "@hourly"
    enabled: false
    target: { session: "99999999-8888-7777-6666-555555555555" }
    payload: "ping"
"#;
        let section: CronSection = serde_yaml::from_str(yaml).unwrap();
        // the wire form survives a serialize → parse → serialize cycle.
        let round_tripped =
            serde_yaml::to_string(&section).expect("a parsed cron section serializes");
        let again: CronSection = serde_yaml::from_str(&round_tripped).unwrap();
        assert_eq!(again, section);

        let resolved = section.to_runtime(&cron_channels(), cron_now()).unwrap();
        assert_eq!(resolved.len(), 2);

        let audit = &resolved[0];
        assert_eq!(audit.id, "night-audit");
        assert_eq!(audit.expr.source(), "30 3 * * 1-5");
        assert_eq!(
            audit.tz,
            kian_cron::parse_timezone("Europe/Berlin").unwrap()
        );
        assert_eq!(audit.tz_name, "Europe/Berlin");
        assert!(audit.enabled);
        assert_eq!(
            audit.target,
            CronTarget::Channel {
                channel: "cron-jobs".into(),
                topic: "audit".into()
            }
        );
        assert_eq!(audit.overlap, CronOverlap::Skip);
        assert_eq!(audit.catch_up, kian_cron::CatchUp::Skip);
        assert_eq!(audit.coalesce_ms, 60_000);
        assert_eq!(audit.max_concurrent, 2);
        assert_eq!(audit.timeout_ms, 120_000);
        assert!(audit.worktree.is_some(), "worktree: true selects isolation");
        assert!(audit.next_fire_after(cron_now()).unwrap() > cron_now());

        // the second schedule freezes the host timezone (§2.3) and takes
        // every remaining §3.1 default.
        let ping = &resolved[1];
        assert_eq!(ping.id, "hourly-ping");
        assert_eq!(ping.expr.source(), "0 * * * *");
        let (host_tz, host_name) = host_timezone();
        assert_eq!(ping.tz, host_tz);
        assert_eq!(ping.tz_name, host_name);
        assert!(!ping.enabled, "`enabled: false` is carried, never fired");
        assert_eq!(
            ping.target,
            CronTarget::Session {
                session: "99999999-8888-7777-6666-555555555555".into()
            }
        );
        assert_eq!(ping.prompt, "ping");
        assert_eq!(ping.overlap, CronOverlap::Skip);
        assert_eq!(ping.catch_up, kian_cron::CatchUp::Latest);
        assert_eq!(ping.coalesce_ms, DEFAULT_CRON_COALESCE_MS);
        assert_eq!(ping.max_concurrent, DEFAULT_CRON_MAX_CONCURRENT);
        assert_eq!(ping.timeout_ms, DEFAULT_CRON_TIMEOUT_MS);
    }

    /// A malformed expression refuses load, naming the schedule id.
    #[test]
    fn cron_schedule_bad_expr_refuses_naming_the_id() {
        let err = cron_section_from(
            "schedules:\n  - id: night-audit\n    expr: \"0 0 * *\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("night-audit"), "{err}");
        assert!(err.contains("exactly 5 are required"), "{err}");
    }

    /// A valid-but-never-matching shape refuses load (§2.5 horizon).
    #[test]
    fn cron_never_firing_shape_refuses() {
        let err = cron_section_from(
            "schedules:\n  - id: never\n    expr: \"0 0 30 2 *\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`never`"), "{err}");
        assert!(err.contains("horizon"), "{err}");
    }

    /// An unknown `@`-alias refuses load (§2.2).
    #[test]
    fn cron_unknown_alias_refuses() {
        let err = cron_section_from(
            "schedules:\n  - id: boot\n    expr: \"@reboot\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("@reboot"), "{err}");
        assert!(err.contains("alias"), "{err}");
    }

    /// An unresolvable timezone refuses load — never a fallback to UTC
    /// (§2.3).
    #[test]
    fn cron_schedule_bad_tz_refuses() {
        let err = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    tz: Mars/Olympus\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("Mars/Olympus"), "{err}");
        assert!(err.contains("`audit`"), "{err}");
    }

    /// Duplicate ids refuse load (§3.3 rule 2) — the id is the cursor key.
    #[test]
    fn cron_duplicate_id_refuses() {
        let err = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n  \
             - id: audit\n    expr: \"@hourly\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("duplicate id"), "{err}");
        assert!(err.contains("schedules[1]"), "{err}");
    }

    /// A missing `target` / a misspelled key is a parse error
    /// (`deny_unknown_fields`), never a silently ignored field.
    #[test]
    fn cron_missing_target_and_unknown_fields_refuse_at_parse() {
        // no target at all
        assert!(
            serde_yaml::from_str::<CronSection>(
                "schedules:\n  - id: audit\n    expr: \"@daily\"\n    prompt: audit\n"
            )
            .is_err()
        );
        // unknown key inside a schedule
        assert!(
            serde_yaml::from_str::<CronSection>(
                "schedules:\n  - id: audit\n    expr: \"@daily\"\n    prompt: audit\n    \
                 tzname: UTC\n    target: { session: \"99999999-8888-7777-6666-555555555555\" }\n"
            )
            .is_err()
        );
        // unknown key inside a target
        assert!(
            serde_yaml::from_str::<CronSection>(
                "schedules:\n  - id: audit\n    expr: \"@daily\"\n    prompt: audit\n    \
                 target: { channel: cron-jobs, topic: audit, extra: x }\n"
            )
            .is_err()
        );
        // unknown key on the section itself
        assert!(serde_yaml::from_str::<CronSection>("schedulz: []\n").is_err());
    }

    /// The target resolves fail-closed (§3.2) and an empty prompt refuses.
    #[test]
    fn cron_target_and_prompt_are_fail_closed() {
        let undeclared = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { channel: nope, topic: audit }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(undeclared.contains("not declared"), "{undeclared}");

        let no_topic = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { channel: cron-jobs }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(no_topic.contains("target.topic"), "{no_topic}");

        let bad_uuid = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { session: not-a-uuid }\n    prompt: audit\n",
        )
        .unwrap_err()
        .to_string();
        assert!(bad_uuid.contains("not-a-uuid"), "{bad_uuid}");

        let empty_prompt = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: \"  \"\n",
        )
        .unwrap_err()
        .to_string();
        assert!(
            empty_prompt.contains("prompt must be non-empty"),
            "{empty_prompt}"
        );
    }

    /// Zero bounds and a contradictory overlap policy refuse, and every
    /// declaration's errors are collected in one load (§3.3 rules 4, 5).
    #[test]
    fn cron_bounds_and_contradictions_refuse_and_collect_all_errors() {
        let err = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: audit\n    \
             overlap: kill-previous\n    max_concurrent: 2\n    coalesce_ms: 0\n  \
             - id: sweep\n    expr: \"@hourly\"\n    tz: Nowhere/Land\n    \
             target: { channel: cron-jobs, topic: audit }\n    prompt: sweep\n    \
             timeout_ms: 0\n",
        )
        .unwrap_err()
        .to_string();
        // both schedules are named: the load reports one error list, not
        // one error per restart.
        assert!(err.contains("`audit`"), "{err}");
        assert!(err.contains("`sweep`"), "{err}");
        assert!(err.contains("coalesce_ms"), "{err}");
        assert!(err.contains("kill-previous"), "{err}");
        assert!(err.contains("timeout_ms"), "{err}");
        assert!(err.contains("Nowhere/Land"), "{err}");
    }

    /// v1 honesty gates (CRON-MODEL §5.1/§5.5): an unsupported overlap
    /// policy or an explicit resident-arm timeout refuses at load instead
    /// of being accepted and silently ignored.
    #[test]
    fn cron_unsupported_v1_policies_refuse_at_load() {
        for (policy, needle) in [
            ("queue", "queue is designed"),
            ("kill-previous", "kill-previous is designed"),
        ] {
            let err = cron_section_from(&format!(
                "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
                 target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit\n    \
                 overlap: {policy}\n"
            ))
            .unwrap_err()
            .to_string();
            assert!(err.contains(needle), "{policy}: {err}");
            assert!(err.contains("designed, not implemented"), "{policy}: {err}");
            assert!(err.contains("`audit`"), "{policy}: {err}");
        }

        let resident_timeout = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { session: \"99999999-8888-7777-6666-555555555555\" }\n    prompt: audit\n    \
             timeout_ms: 1000\n",
        )
        .unwrap_err()
        .to_string();
        assert!(
            resident_timeout.contains("timeout_ms"),
            "{resident_timeout}"
        );
        assert!(
            resident_timeout.contains("worktree-isolated"),
            "{resident_timeout}"
        );

        // The same explicit timeout is legal once isolation selects the arm
        // that enforces it.
        let isolated = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { session: \"99999999-8888-7777-6666-555555555555\" }\n    prompt: audit\n    \
             worktree: true\n    timeout_ms: 1000\n",
        )
        .expect("a worktree schedule may declare its per-firing timeout");
        assert_eq!(isolated[0].timeout_ms, 1000);
        assert!(isolated[0].worktree.is_some());
    }

    /// CRON-MODEL §3.3 rule 7 (`cron_targets_watchdog`): a wall-clock
    /// schedule must not target the interval watchdog's session.
    #[test]
    fn cron_targeting_the_watchdog_session_refuses() {
        let resolved = cron_section_from(
            "schedules:\n  - id: audit\n    expr: \"@daily\"\n    \
             target: { session: \"99999999-8888-7777-6666-555555555555\" }\n    prompt: audit\n",
        )
        .unwrap();
        let err = reject_cron_targeting_the_watchdog(
            &resolved,
            Some("99999999-8888-7777-6666-555555555555"),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("cron_targets_watchdog"), "{err}");
        assert!(err.contains("`audit`"), "{err}");
        assert!(reject_cron_targeting_the_watchdog(&resolved, Some("other")).is_ok());
        assert!(reject_cron_targeting_the_watchdog(&resolved, None).is_ok());
    }

    /// The schedule id shape is enforced (§3.1).
    #[test]
    fn cron_id_shape_is_enforced() {
        for bad in ["Night-Audit", "-audit", "audit!", ""] {
            let yaml = format!(
                "schedules:\n  - id: \"{bad}\"\n    expr: \"@daily\"\n    \
                 target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit\n"
            );
            let err = cron_section_from(&yaml).unwrap_err().to_string();
            assert!(err.contains("[a-z0-9][a-z0-9_-]{0,63}"), "{bad}: {err}");
        }
    }

    /// A declared `superagent.cron` block reaches the load-time gate from
    /// `ServeConfig::parse`, and a broken one refuses boot.
    #[test]
    fn serve_config_validates_cron_at_load() {
        let good = superagent_yaml().replacen(
            "superagent:\n",
            "superagent:\n  cron:\n    schedules:\n      - id: audit\n        expr: \"@daily\"\n        \
             target: { channel: mock-im, topic: audit }\n        prompt: audit\n",
            1,
        );
        let cfg = ServeConfig::parse(&good).expect("cron declarations parse");
        let sa = cfg.superagent.as_ref().expect("superagent section");
        assert_eq!(sa.cron.schedules.len(), 1);
        assert_eq!(sa.cron.schedules[0].id, "audit");

        // fail-closed at load: an unknown alias refuses boot from parse().
        let bad = good.replace("expr: \"@daily\"", "expr: \"@reboot\"");
        assert!(ServeConfig::parse(&bad).is_err());
    }

    /// ADR-037: `workers.queue_redrive_grace_ms` parses, defaults to 5 s and
    /// is fail-closed at its ceiling (a large value delays every recovery
    /// re-drive).
    #[test]
    fn workers_queue_redrive_grace_ms_parses_and_is_fail_closed() {
        let base = valid_yaml();
        let with = |grace: u64| {
            format!(
                "{base}plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\n\
                 workers:\n  listen: \"127.0.0.1:7400\"\n  token_env: KIAN_WORKER_TOKEN\n  \
                 queue_redrive_grace_ms: {grace}\n"
            )
        };
        let cfg = ServeConfig::parse(&with(0)).expect("explicit zero parses");
        assert_eq!(
            cfg.workers.expect("section present").queue_redrive_grace_ms,
            0
        );
        let cfg = ServeConfig::parse(&with(2_500)).expect("a positive grace parses");
        assert_eq!(
            cfg.workers.expect("section present").queue_redrive_grace_ms,
            2_500
        );
        let err = ServeConfig::parse(&with(60_001)).expect_err("over the ceiling refuses");
        assert!(
            err.to_string().contains("queue_redrive_grace_ms"),
            "was: {err}"
        );
        let cfg = ServeConfig::parse(&base).expect("valid config");
        assert!(cfg.workers.is_none());
    }
}
