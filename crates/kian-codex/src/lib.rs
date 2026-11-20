//! `kian-codex` — the Codex bridge adapter (HARNESS-BRIDGE §5.2, phase 1).
//!
//! Spawns `codex app-server` and speaks its JSON-RPC over stdio: `initialize`, `thread/start`,
//! `turn/start`, `turn/steer`, `turn/interrupt`, item notifications and server-initiated approval
//! requests. **Projection-only**: no `step/request`, no cache accounting.
//!
//! Implementation lands in the same change as its fixtures; the crate is a workspace member so
//! the narrow-waist and capability drift tests cover it from the start.

pub mod codec;
pub mod session;

pub use codec::{CodexConfig, CodexMapper};
pub use session::{CAPABILITIES, CodexAdapter, CodexSession};
