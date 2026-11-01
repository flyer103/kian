//! The S6 dsh mapping contract (ADR-010, DESIGN §16), as landed by
//! card C53 — **moved verbatim into `kian-dsh` (C54)** and re-exported
//! here so every existing `kian_superagent::dsh` import keeps
//! compiling unchanged. The mapping is the bridge's only translation;
//! see [`kian_dsh`] for the driver and the narrow-waist rule that
//! keeps this contract's home out of the core crates.

pub use kian_dsh::mapping::*;
pub use kian_dsh::{DshCtx, DshCtxError, DshDriver, DshDriverConfig};

/// The C53 scripted fixture peer as a library entry (the bin below is
/// a thin wrapper; kian-dsh's copy drives the C54 bridge tests).
pub use kian_dsh::fixture_peer;
