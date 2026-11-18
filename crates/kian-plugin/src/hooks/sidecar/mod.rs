//! The sidecar tier (PLUGIN-SPEC §6–§7, §10.3, §11) — P3a: the
//! manifest, the stdio ABI and the runner.
//!
//! - [`manifest`] — §6.1's closed-TOML manifest and §6.2's fourteen
//!   per-instance validation rules (fail-closed, no repair, no
//!   whole-section refusal)
//! - [`frame`] — §7.2's closed frame shapes with the epoch rule as
//!   their single source, plus the golden-checked encoders/decoders
//! - [`line`] — §7.6's bounded newline carrier (the byte bound is
//!   enforced while scanning, before any buffering or parse)
//! - [`runner`] — the spawn-hardened child (§10.3), the handshake
//!   identity check (§7.3), the dedicated std io thread, the §6.4
//!   failure window, §6.5's quarantine, deadlines via a group-kill
//!   watchdog, and the tier-0-shaped adapter that plugs a sidecar
//!   into the existing [`HookBus`](super::bus::HookBus) classifier
//!
//! The config surface, per-worker resolution, set digest and session
//! consistency are P3b; this module knows nothing about YAML config,
//! workers or sessions-as-such — a runner is per instance, for the
//! server's lifetime (R11).

pub mod frame;
pub mod line;
pub mod manifest;
pub mod runner;

pub use frame::{
    DecodeError, Hello, HookFrame, NOTE_MAX_BYTES, REASON_MAX_BYTES, Ready, ReplyFrame,
    decode_ready, decode_reply, encode_bye, encode_err,
};
pub use line::{BoundedLineReader, LineError, write_line};
pub use manifest::{
    ACCEPTED_ABI, GrantsSection, LimitsSection, Manifest, PluginSection, ProcessSection, Refusal,
    RefusalCode, Required, ValidationCtx, parse_manifest,
};
pub use runner::{Counters, FailureKind, QuarantineReason, ReplyOutcome, SidecarRunner};
