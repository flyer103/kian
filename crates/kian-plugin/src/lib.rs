//! `kian-plugin` — Ctx registry, Plugin trait, five-mode dispatch, Disposer.
//!
//! DESIGN §7 assigns this crate the key interfaces `Ctx`, `Plugin`,
//! `Disposer`; the design mapping lives in DESIGN §6 ("Plugin system
//! (Cordis-inspired, Rust-ized)"). Static, compile-time registration only
//! in Phase 1 (ADR-004) — no dynamic loading.
//!
//! The seam *definitions* live in [`kian_core::seams`]; this crate consumes
//! them and never redefines them.
//!
//! Modules:
//! - [`ctx`] — typed service registry, `Plugin`/`PluginSet` activation,
//!   `Disposer`
//! - [`dispatch`] — five-mode typed-event dispatch (`emit` / `waterfall` /
//!   `parallel` / `serial` / `bail`)

pub mod ctx;
pub mod dispatch;

pub use ctx::{Ctx, Disposer, Plugin, PluginError, PluginSet, Service};
pub use dispatch::{
    BailHandler, Flow, Observer, SerialHandler, WaterfallMiddleware, WaterfallNext,
};

/// Compile-time plugin registration (ADR-004): assembles a [`PluginSet`]
/// from plugin values. The activation order is *not* the listing order —
/// [`PluginSet::activate`] derives a deterministic topological order from
/// the plugins' declared dependencies.
///
/// ```ignore
/// let set = kian_plugins![LoggingPlugin::new(), ToolsPlugin::new()];
/// let disposer = set.activate(&mut ctx)?;
/// ```
#[macro_export]
macro_rules! kian_plugins {
    ($($plugin:expr),+ $(,)?) => {{
        let mut set = $crate::PluginSet::new();
        $( set.register($plugin); )+
        set
    }};
}
