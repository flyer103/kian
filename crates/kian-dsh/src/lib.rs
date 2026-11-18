//! `kian-dsh` — the bridge (ADR-010 / DESIGN §6.2's
//! "dsh loop (being-integrated)" row, §16).
//!
//! `DshDriver` implements the [`LoopDriver`](kian_core::seams::LoopDriver)
//! seam by driving a `dsh --profile sdk` child process over stdio
//! JSON-lines. The dsh process keeps its interaction model ("dsh
//! interaction unchanged"); kian owns durability ("made durable"):
//!
//! - every mapped event is appended through the drain's ordinary fenced
//!   append path — the drain stays the sole writer (the layered iron
//!   rule; the bridge never touches a store);
//! - steers arrive as inbox rows promoted at safe boundaries exactly
//!   like any input and ride to the child as `steer` control frames;
//! - the mapping ([`mapping`]) is the only translation — zero new
//!   event kinds, mapped events ride the existing payloads verbatim.
//!
//! # Narrow waist
//!
//! This crate depends on `kian-core` only (seam types + the shared
//! event vocabulary the mapping selects from). The drain surface the
//! driver needs is [`DshCtx`] — a crate-local port mirroring exactly
//! what `DrainCtx` offers a loop. Assembly (kian-cli, behind
//! `superagent.harness.dsh`, default off) implements the port over the
//! real `DrainCtx`; the orphan rule is why the port lives here and the
//! impl lives there.
//!
//! # Crash honesty (constraint 7)
//!
//! A child that dies mid-step leaves the honest failure trace
//! fixed: `step/request` with no following `assistant/message`. The
//! driver reports a soft failure (`Failed`, never `completed`); a
//! server-side kill mid-drain leaves the stream `running` for the
//! takeover drain, which spawns a fresh child that replays its own
//! durable session log — the driver de-duplicates the replay against
//! the persisted stream ([`driver::Dedup`]) so only genuinely new
//! events are appended (semantics through the bridge).
//!
//! # Fixture peer
//!
//! [`fixture_peer`] is the scripted stdio peer as a library
//! function plus a thin `dsh-fixture-peer` bin, so CI proves the
//! driver with zero Node/dsh present. The live run (`KIAN_DSH_BIN`
//! pointing at a real dsh install) is env-gated, CI-excluded, and
//! recorded in the README run log (pattern).

pub mod ctx;
pub mod driver;
pub mod fixture_peer;
pub mod mapping;
pub mod toolpost;

pub use ctx::{DshCtx, DshCtxError};
pub use driver::{DshDriver, DshDriverConfig};
pub use mapping::{
    BRIDGE_CONTRACT_VERSION, ControlFrame, DshEvent, MapCtx, PeerFrame, PeerSession,
    SUPPORTED_LOGICAL_FORMAT,
};
