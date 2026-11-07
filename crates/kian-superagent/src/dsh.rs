//! The dsh mapping contract (ADR-010, DESIGN §16), as landed by
//! — **moved verbatim into `kian-dsh` ** and re-exported
//! here so every existing `kian_superagent::dsh` import keeps
//! compiling unchanged. The mapping is the bridge's only translation;
//! see [`kian_dsh`] for the driver and the narrow-waist rule that
//! keeps this contract's home out of the core crates.

pub use kian_dsh::mapping::*;
pub use kian_dsh::{DshCtx, DshCtxError, DshDriver, DshDriverConfig};

/// The scripted fixture peer as a library entry (the bin below is
/// a thin wrapper; kian-dsh's copy drives the bridge tests).
pub use kian_dsh::fixture_peer;
