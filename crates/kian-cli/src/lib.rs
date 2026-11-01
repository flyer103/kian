//! `kian-cli` — CLI and embedded SDK: serve/run/inspect/replay/export.
//!
//! DESIGN §7 assigns this crate (together with the Phase-2 `kian-sdk`) the
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
//! - [`cache_metrics`] — S3 cache-byte stability report (C17), exposed
//!   at `GET /metrics` and `kian cache-stats`
//! - [`client`] — thin JSON-RPC HTTP client the session commands use to
//!   talk to a running `serve` (re-exported from the wire-only
//!   `kian-sdk` crate, card C46)
//! - [`commands`] — the C14 session commands (inspect / replay / export
//!   / wake-now / lease / recovery budget reset) with documented
//!   `--json` schemas
//! - [`config`] — `kian.yaml` loading, fail-closed on malformed config
//! - [`ops`] — the C14 operational plane (`POST /ops/rpc`: `wake_now`,
//!   `recovery_budget_reset`, and the C27 demo injection
//!   `mock_channel_send`), trait-isolated like the control plane
//! - [`plugins`] — built-in static plugins (openai-compat, local-tools,
//!   react-loop; C27 adds superagent and remote-exec) registered via
//!   `kian_plugins![...]`
//! - [`server`] — assembly (plugins → Ctx → engine → inbox rebuild) and
//!   the `ControlPlane` / `OpsPlane` implementations
//! - [`superagent`] — the C27 super-agent assembly: the three-way
//!   Router/Worker/plain driver, the `InboxWriter` port over the
//!   engine, subscription consumers, watchdog wiring and metrics
//! - [`webhook`] — the C49 inbound webhook receiver plane
//!   (`POST /webhooks/github`: HMAC-verified GitHub deliveries handed
//!   to the subscription pipeline), trait-isolated like the control
//!   and ops planes (ADR-011)
pub mod api;
pub mod async_bridge;
pub mod cache_metrics;
pub mod client;
pub mod commands;
pub mod config;
pub mod ops;
pub mod plugins;
pub mod server;
pub mod superagent;
pub mod webhook;

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
