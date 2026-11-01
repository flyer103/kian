//! Router/Worker templates (card C25 item 4, DESIGN §4).
//!
//! Templates are plain [`AgentDef`]s — declarative, versioned config
//! (DESIGN §2.1). The worker template pins a strong `main` slot and
//! the full toolset (fs + exec; `exec` is the remote-exec tool schema
//! when assembly routes it to an endpoint, C20 — same schema either
//! way). The Router template references the worker template through
//! [`SuperAgentProfile`], whose constructor enforces the structural
//! profile rules:
//!
//! - the worker's toolset contains **no channel tools** (workers never
//!   reply on channels; that is the Router's surface, C26);
//! - the worker keeps its exec capability (full-toolset promise);
//! - the Router carries the dispatch tool (`spawn_worker`).
//!
//! The worker's `AgentDef.version` *is* the thread's `pinned_version`
//! (ADR-006: pinned for the thread's lifetime, no runtime swap).

use std::collections::BTreeMap;

use kian_core::model::{AgentDef, Budgets, SlotName, SlotSpec};
use serde_json::{Value, json};

/// The Router's (cheap, fast) model slot name.
pub const ROUTER_SLOT: &str = "router";

/// The dispatch tool the Router calls to spawn a worker (C25 mechanics
/// behind it, C26 wires the tool itself).
pub const SPAWN_WORKER_TOOL: &str = "spawn_worker";

/// Channel-facing tools — the Router's output surface (C26). Workers
/// never hold these.
pub const CHANNEL_TOOLS: [&str; 2] = ["reply", "deliver_structured"];

/// The worker's built-in toolset: fs + exec. `exec` is also the remote
/// endpoint's tool schema (C20 `RemoteExecTool` implements the same
/// schema), so "full tools incl. remote exec" is one name. Dotless
/// spellings (kian-61): strict OpenAI-compatible endpoints reject
/// dotted tool names.
pub const WORKER_TOOLS: [&str; 3] = ["fs_read", "fs_write", "exec"];

/// The async `start()`/`stop()` pair (C37, ADR-008). Mounted on the
/// Router profile and the worker template **config-gated, default
/// off** (the article's own "not default-on"); the sync `exec` tool
/// is untouched either way.
pub const ASYNC_TOOLS: [&str; 2] = ["start", "stop"];

/// Mounts (or leaves off) the async pair on an agent definition.
/// Idempotent, and the append order is stable (`start` then `stop`) —
/// tool lists are prompt-prefix content (constraint 3's gate), so the
/// same config always yields the same list. With `enabled = false`
/// the definition is returned byte-identical (the default-off
/// regression is structural, not incidental).
pub fn mount_async_tools(agent: &mut AgentDef, enabled: bool) {
    if !enabled {
        return;
    }
    for tool in ASYNC_TOOLS {
        if !agent.tools.iter().any(|t| t == tool) {
            agent.tools.push(tool.to_string());
        }
    }
}

/// Mounts (or leaves off) the C35 recall toolpack
/// ([`crate::toolpack::RECALL_TOOLS`]) on an agent definition. Same
/// discipline as [`mount_async_tools`]: idempotent, stable sorted
/// append order, byte-identical definition when `enabled = false`.
/// Assembly mounts it default-on for the Router profile and opt-in
/// per worker template (the `superagent.recall` config section) — the
/// mount itself is explicit so the default-off regression stays
/// structural. Recall tools are read-only; they are not channel tools
/// ([`is_channel_tool`]) and never violate the structural profile
/// rules.
pub fn mount_recall_tools(agent: &mut AgentDef, enabled: bool) {
    if !enabled {
        return;
    }
    for tool in crate::toolpack::RECALL_TOOLS {
        if !agent.tools.iter().any(|t| t == tool) {
            agent.tools.push(tool.to_string());
        }
    }
}

/// Mounts (or leaves off) the C40 watchdog surface
/// ([`crate::watchdog::WATCHDOG_TOOLS`]) on an agent definition. Same
/// discipline: idempotent, stable sorted append order, byte-identical
/// definition when `enabled = false`. Assembly mounts it on the
/// configured watchdog agent when `superagent.watchdog` is present.
pub fn mount_watchdog_tools(agent: &mut AgentDef, enabled: bool) {
    if !enabled {
        return;
    }
    for tool in crate::watchdog::WATCHDOG_TOOLS {
        if !agent.tools.iter().any(|t| t == tool) {
            agent.tools.push(tool.to_string());
        }
    }
}

/// Whether `name` belongs to the Router's dispatch/channel surface —
/// the tool names a worker profile must never carry.
#[must_use]
pub fn is_channel_tool(name: &str) -> bool {
    CHANNEL_TOOLS.contains(&name) || name == SPAWN_WORKER_TOOL || name.starts_with("channel.")
}

/// The worker template: strong `main` slot, full toolset, generous
/// budgets. `version` is the pinned version threads mint against
/// ([`crate::WorkerSpawner`] reads it).
#[must_use]
pub fn worker_template(provider: &str, model: &str, version: u64) -> AgentDef {
    AgentDef {
        id: "kian-worker".into(),
        version,
        model_slots: BTreeMap::from([(
            SlotName("main".into()),
            SlotSpec {
                provider: provider.into(),
                model: model.into(),
                fallback: Vec::new(),
            },
        )]),
        tools: WORKER_TOOLS.iter().map(|t| (*t).to_string()).collect(),
        budgets: Budgets {
            tokens: Some(1_000_000),
            wall_clock_ms: Some(3_600_000),
        },
    }
}

/// The Router template: cheap `router` slot, the dispatch + channel
/// toolset (C26), no exec — the Router judges and dispatches; workers
/// act.
#[must_use]
pub fn router_template(provider: &str, model: &str) -> AgentDef {
    let mut tools = vec![SPAWN_WORKER_TOOL.to_string()];
    tools.extend(CHANNEL_TOOLS.iter().map(|t| (*t).to_string()));
    AgentDef {
        id: "kian-router".into(),
        version: 1,
        model_slots: BTreeMap::from([(
            SlotName(ROUTER_SLOT.into()),
            SlotSpec {
                provider: provider.into(),
                model: model.into(),
                fallback: Vec::new(),
            },
        )]),
        tools,
        budgets: Budgets {
            tokens: Some(200_000),
            wall_clock_ms: Some(600_000),
        },
    }
}

/// Structural profile violations (the assertions behind
/// [`SuperAgentProfile::new`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileError {
    #[error("worker toolset contains a channel tool: {0}")]
    WorkerHasChannelTool(String),
    #[error("worker toolset lost its exec capability (full-toolset promise)")]
    WorkerMissingExec,
    #[error("router toolset lost the dispatch tool `{0}`")]
    RouterMissingDispatchTool(String),
}

/// The Router/Worker template pair — the Router's config *references*
/// the worker template it dispatches to, validated as one unit.
#[derive(Debug, Clone, PartialEq)]
pub struct SuperAgentProfile {
    pub router: AgentDef,
    pub worker: AgentDef,
}

impl SuperAgentProfile {
    /// Validates the pair against the structural profile rules and
    /// returns it. This is the assertion behind the card's "worker's
    /// toolset contains no channel tools" acceptance.
    pub fn new(router: AgentDef, worker: AgentDef) -> Result<Self, ProfileError> {
        for tool in &worker.tools {
            if is_channel_tool(tool) {
                return Err(ProfileError::WorkerHasChannelTool(tool.clone()));
            }
        }
        if !worker.tools.iter().any(|t| t == "exec") {
            return Err(ProfileError::WorkerMissingExec);
        }
        if !router.tools.iter().any(|t| t == SPAWN_WORKER_TOOL) {
            return Err(ProfileError::RouterMissingDispatchTool(
                SPAWN_WORKER_TOOL.into(),
            ));
        }
        Ok(Self { router, worker })
    }

    /// The version worker threads pin (ADR-006).
    #[must_use]
    pub fn worker_pinned_version(&self) -> u64 {
        self.worker.version
    }
}

/// A minimal empty JSON-Schema tool parameter description (the shape
/// `ToolSchema.parameters` expects; used by test fixtures and C26's
/// tool implementations).
#[must_use]
pub fn empty_tool_parameters() -> Value {
    json!({"type": "object", "properties": {}, "additionalProperties": false})
}

#[cfg(test)]
mod tests {
    use super::{
        CHANNEL_TOOLS, ProfileError, ROUTER_SLOT, SPAWN_WORKER_TOOL, SuperAgentProfile,
        WORKER_TOOLS, empty_tool_parameters, is_channel_tool, router_template, worker_template,
    };
    use kian_core::model::AgentDef;

    fn worker() -> AgentDef {
        worker_template("openai-compat", "kimi-k2", 7)
    }

    fn router() -> AgentDef {
        router_template("openai-compat", "kimi-flash")
    }

    #[test]
    fn worker_template_is_strong_main_slot_full_tools_pinned_version() {
        let w = worker();
        assert_eq!(w.version, 7);
        assert!(
            w.model_slots
                .contains_key(&kian_core::model::SlotName("main".into()))
        );
        assert_eq!(w.model_slots.len(), 1, "strong single main slot");
        for tool in WORKER_TOOLS {
            assert!(w.tools.contains(&tool.to_string()), "worker lacks {tool}");
        }
        assert!(
            w.tools.contains(&"exec".into()),
            "exec present (remote-exec schema)"
        );
    }

    #[test]
    fn worker_toolset_contains_no_channel_tools() {
        let w = worker();
        assert!(w.tools.iter().all(|t| !is_channel_tool(t)));
        for channel in CHANNEL_TOOLS {
            assert!(!w.tools.contains(&channel.to_string()));
            assert!(is_channel_tool(channel));
        }
        assert!(is_channel_tool(SPAWN_WORKER_TOOL));
        assert!(is_channel_tool("channel.send"));
        assert!(!is_channel_tool("exec"));
        assert!(!is_channel_tool("fs.read"));
    }

    #[test]
    fn router_template_references_and_dispatches() {
        let r = router();
        assert!(
            r.model_slots
                .contains_key(&kian_core::model::SlotName(ROUTER_SLOT.into()))
        );
        assert!(r.tools.contains(&SPAWN_WORKER_TOOL.to_string()));
        assert_eq!(r.id, "kian-router");
        assert_eq!(worker().id, "kian-worker");
    }

    #[test]
    fn profile_validates_the_structural_rules() {
        let profile = SuperAgentProfile::new(router(), worker()).expect("valid pair");
        assert_eq!(profile.worker_pinned_version(), 7);
        assert_eq!(profile.router.version, 1);

        // A worker with a channel tool is rejected.
        let mut bad = worker();
        bad.tools.push("reply".into());
        assert_eq!(
            SuperAgentProfile::new(router(), bad).unwrap_err(),
            ProfileError::WorkerHasChannelTool("reply".into())
        );

        // A worker without exec is rejected.
        let mut no_exec = worker();
        no_exec.tools.retain(|t| t != "exec");
        assert_eq!(
            SuperAgentProfile::new(router(), no_exec).unwrap_err(),
            ProfileError::WorkerMissingExec
        );

        // A router without the dispatch tool is rejected.
        let mut no_dispatch = router();
        no_dispatch.tools.retain(|t| t != SPAWN_WORKER_TOOL);
        assert!(matches!(
            SuperAgentProfile::new(no_dispatch, worker()).unwrap_err(),
            ProfileError::RouterMissingDispatchTool(_)
        ));
    }

    #[test]
    fn empty_tool_parameters_is_a_closed_empty_object_schema() {
        let p = empty_tool_parameters();
        assert_eq!(p["type"], "object");
        assert_eq!(p["additionalProperties"], false);
    }

    #[test]
    fn async_pair_mount_is_gated_idempotent_and_stable_ordered() {
        use super::{ASYNC_TOOLS, mount_async_tools};
        // Default off: byte-identical definitions (the regression is
        // structural, not incidental).
        let mut w = worker();
        let before = w.clone();
        mount_async_tools(&mut w, false);
        assert_eq!(w, before);
        let mut r = router();
        let before = r.clone();
        mount_async_tools(&mut r, false);
        assert_eq!(r, before);

        // On: the pair lands on BOTH profiles, start then stop, and
        // re-mounting changes nothing (idempotent, stable order).
        for mut agent in [worker(), router()] {
            mount_async_tools(&mut agent, true);
            for tool in ASYNC_TOOLS {
                assert!(agent.tools.contains(&tool.to_string()));
            }
            let start = agent.tools.iter().position(|t| t == "start").unwrap();
            let stop = agent.tools.iter().position(|t| t == "stop").unwrap();
            assert_eq!(stop, start + 1, "start immediately followed by stop");
            let once = agent.clone();
            mount_async_tools(&mut agent, true);
            assert_eq!(agent, once, "idempotent re-mount");
        }

        // The mounted worker still satisfies the structural profile
        // rules (start/stop are not channel tools).
        let mut w = worker();
        mount_async_tools(&mut w, true);
        SuperAgentProfile::new(router(), w).expect("async-mounted worker is a valid profile");
    }

    #[test]
    fn recall_mount_is_gated_idempotent_and_stable_ordered() {
        use super::mount_recall_tools;
        use crate::toolpack::RECALL_TOOLS;

        // Default off (opt-in): byte-identical definitions.
        for mut agent in [worker(), router()] {
            let before = agent.clone();
            mount_recall_tools(&mut agent, false);
            assert_eq!(agent, before);
        }

        // On: the three tools land in the stable sorted order, and
        // re-mounting changes nothing.
        for mut agent in [worker(), router()] {
            mount_recall_tools(&mut agent, true);
            for tool in RECALL_TOOLS {
                assert!(agent.tools.contains(&tool.to_string()), "lacks {tool}");
                assert!(!is_channel_tool(tool), "{tool} must not be a channel tool");
            }
            let positions: Vec<usize> = RECALL_TOOLS
                .iter()
                .map(|tool| agent.tools.iter().position(|t| t == tool).unwrap())
                .collect();
            let mut sorted = positions.clone();
            sorted.sort_unstable();
            assert_eq!(positions, sorted, "stable sorted append order");
            let once = agent.clone();
            mount_recall_tools(&mut agent, true);
            assert_eq!(agent, once, "idempotent re-mount");
        }

        // A recall-mounted worker still satisfies the structural
        // profile rules (recall tools are read-only, not channel tools).
        let mut w = worker();
        mount_recall_tools(&mut w, true);
        SuperAgentProfile::new(router(), w).expect("recall-mounted worker is a valid profile");
    }

    #[test]
    fn watchdog_mount_is_gated_idempotent_and_stable_sorted() {
        use super::mount_watchdog_tools;
        use crate::watchdog::WATCHDOG_TOOLS;

        // Default off: byte-identical definitions.
        for mut agent in [worker(), router()] {
            let before = agent.clone();
            mount_watchdog_tools(&mut agent, false);
            assert_eq!(agent, before);
        }

        // On: the four tools land in the stable sorted order, and
        // re-mounting changes nothing (idempotent).
        for mut agent in [worker(), router()] {
            mount_watchdog_tools(&mut agent, true);
            let tools: Vec<String> = WATCHDOG_TOOLS.iter().map(|t| (*t).to_string()).collect();
            assert_eq!(
                agent.tools[agent.tools.len() - WATCHDOG_TOOLS.len()..],
                tools
            );
            let once = agent.clone();
            mount_watchdog_tools(&mut agent, true);
            assert_eq!(agent, once, "idempotent re-mount");
        }
    }
}
