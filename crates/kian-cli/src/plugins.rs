//! Built-in static plugins for `kian serve` (card C13, ADR-004).
//!
//! Three Phase-1 plugins assemble the seam implementations into the
//! plugin [`Ctx`] (kian-plugin C7):
//!
//! | plugin         | registers                                                |
//! |----------------|----------------------------------------------------------|
//! | `openai-compat`| [`ProviderRegistry`] of openai-compatible providers       |
//! | `local-tools`  | `Arc<dyn ToolProvider>` (built-in fs/exec, kian-loop C11) |
//! | `react-loop`   | [`ReActLoop`] (`Arc<dyn LoopDriver>` built-in, C11)       |
//!
//! The `react-loop` plugin declares the other two as dependencies, so
//! activation is deterministically topological (C7) and the server
//! assembly simply resolves the loop out of the activated `Ctx`.
//!
//! Secrets discipline: provider API keys are read from the environment
//! at assembly time (`api_key_env`); they never live in `kian.yaml` or
//! in this module.

use std::sync::Arc;

use kian_core::model::AgentDef;
use kian_core::seams::{RecoveryPolicySource, ToolProvider};
use kian_llm::provider::{
    OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink, PersistedRequest,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::react::{ReActConfig, ReActLoop};
use kian_loop::tools::LocalTools;
use kian_orchestrator::recovery::PolicyTable;
use kian_plugin::{Ctx, Disposer, Plugin, PluginError};

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
        let react = ReActLoop::with_config(
            self.agent,
            registry,
            tools,
            self.policy as Arc<dyn RecoveryPolicySource>,
            self.config,
        );
        ctx.provide::<ReActLoop>(Arc::new(react))
    }
}
