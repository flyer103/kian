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
use kian_orchestrator::recovery::PolicyConfig;
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
// Loading + validation
// ---------------------------------------------------------------------------

/// Every built-in plugin name `serve` knows.
pub const KNOWN_PLUGINS: [&str; 3] = ["openai-compat", "local-tools", "react-loop"];

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

        // Every provider referenced by a slot (primary or fallback) must
        // be configured — a slot pointing nowhere refuses to start.
        for (slot, spec) in &self.agent.model_slots {
            if !self.providers.contains_key(&spec.provider) {
                return Err(invalid(format!(
                    "slot `{slot}` targets provider `{}` which is not defined in providers",
                    spec.provider
                )));
            }
            for fb in &spec.fallback {
                if !self.providers.contains_key(&fb.provider) {
                    return Err(invalid(format!(
                        "slot `{slot}` fallback targets provider `{}` which is not defined in providers",
                        fb.provider
                    )));
                }
            }
        }

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
        AgentDef {
            id: self.agent.id.clone(),
            version: self.agent.version,
            model_slots: self
                .agent
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
            tools: self.agent.tools.clone(),
            budgets: Budgets {
                tokens: self.agent.budgets.tokens,
                wall_clock_ms: self.agent.budgets.wall_clock_ms,
            },
        }
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
}
