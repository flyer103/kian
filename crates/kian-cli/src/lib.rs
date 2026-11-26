//! `kian-cli` — CLI and embedded SDK: serve/run/inspect/replay/export.
//!
//! DESIGN §7 assigns this crate (together with the bundle `kian-sdk`) the
//! key interfaces `create_session`, `submit`, `steer`, `interrupt`,
//! `events`. It is the assembly point: it depends on every other kian
//! crate, builds the plugin `Ctx`, wires the seam implementations into the
//! orchestrator, and exposes the control-plane API (DESIGN §5).
//!
//! Control-plane handlers obey the layered iron rule (AGENTS.md constraint
//! 9): they only write the inbox and read projections — never execution.
//! The HTTP layer ([`api`]) talks exclusively to the [`api::ControlPlane`]
//! trait; [`server`] implements it over the engine.
//!
//! Modules:
//! - [`api`] — axum JSON-RPC + SSE control plane (DESIGN §5)
//! - [`archive`] — export-only retention (READ-MODEL §7, P2b): the
//!   archive package (`events.jsonl` + `inputs.jsonl` +
//!   `manifest.json`), the two-phase write → verify → commit flow and
//!   the `kian archive run|verify` / `kian session import` commands
//! - [`cache_metrics`] — cache-byte stability report, exposed
//!   at `GET /metrics` and `kian cache-stats`
//! - [`usage_metrics`] — provider token-usage report (pure
//!   projection over `turn/end` payloads), exposed at `GET /metrics`,
//!   `kian usage-stats` and the `session inspect` usage lines
//! - [`views`] — the materialized views of the operator surfaces
//!   (READ-MODEL §6, P2a): directory/usage/cache/ticket-stage rebuild
//!   under a maintenance lease + fresh, digest-checked reads with the
//!   honest full-derivation fallback
//! - [`client`] — thin JSON-RPC HTTP client the session commands use to
//!   talk to a running `serve` (re-exported from the wire-only
//!   `kian-sdk` crate)
//! - [`commands`] — the session commands (inspect / replay / export
//!   / wake-now / lease / recovery budget reset) with documented
//!   `--json` schemas
//! - [`config`] — `kian.yaml` loading, fail-closed on malformed config
//! - [`memory`] — the agent-memory assembly (round 8): the
//!   config-gated dream-routing driver and the source-transcript
//!   rendering behind the `memory_dream` ops method and the
//!   `kian memory` verbs
//! - [`ops`] — the operational plane (`POST /ops/rpc`: `wake_now`,
//!   `recovery_budget_reset`, `memory_dream`, the demo injection
//!   `mock_channel_send` and the worker binding `release_worker`),
//!   trait-isolated like the control plane
//! - [`plugins`] — built-in static plugins (openai-compat, local-tools,
//!   react-loop; adds superagent and remote-exec) registered via
//!   `kian_plugins![...]`
//! - [`server`] — assembly (plugins → Ctx → engine → inbox rebuild) and
//!   the `ControlPlane` / `OpsPlane` implementations
//! - [`superagent`] — the super-agent assembly: the three-way
//!   Router/Worker/plain driver, the `InboxWriter` port over the
//!   engine, subscription consumers, watchdog wiring and metrics
//! - [`webhook`] — the inbound webhook receiver plane
//!   (`POST /webhooks/github`: HMAC-verified GitHub deliveries handed
//!   to the subscription pipeline), trait-isolated like the control
//!   and ops planes (ADR-011)
//! - [`wechat_login`] — `kian wechat login`: the WeChat iLink QR flow
//!   that writes the channel's 0600 credential file (ids printed,
//!   never the token)
pub mod api;
pub mod archive;
pub mod artifacts;
pub mod async_bridge;
pub mod cache_metrics;
#[doc(hidden)]
pub mod capture;
pub mod client;
pub mod commands;
pub mod config;
pub mod cost;
pub mod cron_cli;
pub mod cron_engine;
pub mod cron_tick;
pub mod cron_worktree;
pub mod defer;
mod harness_hooks;
pub mod imessage_check;
pub mod job_cli;
pub mod job_config;
pub mod job_runtime;
pub mod media_edge;
pub mod memory;
pub mod multiagent_tick;
pub mod ops;
pub mod plugins;
pub mod pricing;
pub mod rotation;
pub mod server;
pub mod sidecars;
pub mod superagent;
pub mod usage_metrics;
pub mod views;
pub mod voice_edge;
pub mod webhook;
pub mod wechat_login;
pub mod worker_release;
pub mod worker_release_tick;

#[cfg(test)]
mod tests {
    /// The binary must print a `MAJOR.MINOR.PATCH`-shaped version.
    #[test]
    fn version_is_semver_shaped() {
        let v = env!("CARGO_PKG_VERSION");
        let parts: Vec<&str> = v.split('.').collect();
        assert!(parts.len() >= 3, "version {v} is not MAJOR.MINOR.PATCH");
        for p in &parts[..3] {
            assert!(
                p.chars().all(|c| c.is_ascii_digit()),
                "version component {p} is not numeric"
            );
        }
    }
}
