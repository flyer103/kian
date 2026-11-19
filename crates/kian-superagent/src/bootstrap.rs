//! The session-bootstrap seam (ADR-038): a hook every internal session
//! creator runs **before** it admits its triggering input.
//!
//! Creation-time memory injection (MEMORY-MODEL §6) is only reachable
//! from the control plane's `create_session`. The resident Router and
//! watchdog are bootstrapped by the first admit, delegated workers get
//! a derived `child_session_id`, and cron targets a derived
//! `channel_session` — none goes through `create_session`. They are the
//! session families the product runs unattended, so they are exactly the
//! ones that would never see the index.
//!
//! The seam is deliberately tiny and **ordering-bearing**: the caller
//! must invoke it before the admit that creates the session (the index
//! is the session's first context-bearing event). It is idempotent by
//! the injector's own key, fresh-only by the injector's own check, and a
//! `None` bootstrap is a no-op that keeps every byte unchanged.

use futures::future::BoxFuture;
use kian_core::model::SessionId;

/// Bootstraps one internally created session (ADR-038). Implemented by
/// the assembly (the memory bundle); the bundle crates only see the
/// trait.
pub trait SessionBootstrap: Send + Sync {
    /// Prepares `session` for its first turn. Must be called before the
    /// creator admits the triggering input. `Err` is recorded and
    /// ignored by the creator — a failed bootstrap never fails it.
    fn bootstrap(&self, session: SessionId) -> BoxFuture<'static, Result<(), String>>;
}
