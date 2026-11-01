//! The wake registry (DESIGN §2.3, card C9).
//!
//! Wakes are *advisory* (invariant 2): pendingWake entries are
//! - **foldable** — any number of wakes for one session collapse into a
//!   single pending entry (registering onto an existing entry folds);
//! - **losable** — dropping an entry loses nothing, because the drain
//!   re-evaluates the inbox before releasing ownership and the startup
//!   inbox scan fully rebuilds the registry (NoLostWork);
//! - **concurrency-safe** — backed by a lock-free sharded set.
//!
//! The registry holds no timers and no execution state: it only tracks
//! *which sessions have a pending wake*, never what to execute.

use dashmap::DashSet;
use std::sync::Arc;

use kian_core::model::SessionId;

use crate::metrics::Metrics;

/// Pending-wake set. `len()` is the pendingWake depth gauge.
#[derive(Debug, Default)]
pub struct WakeRegistry {
    pending: DashSet<SessionId>,
    metrics: Option<Arc<Metrics>>,
}

impl WakeRegistry {
    /// A registry without metrics (standalone/test use).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry that counts register/fold traffic into `metrics`.
    #[must_use]
    pub fn with_metrics(metrics: Arc<Metrics>) -> Self {
        Self {
            pending: DashSet::new(),
            metrics: Some(metrics),
        }
    }

    /// Registers a pending wake for `session`. Returns `true` when a new
    /// entry was created, `false` when the wake *folded* onto an
    /// existing pending entry.
    pub fn register(&self, session: &SessionId) -> bool {
        if self.pending.insert(*session) {
            if let Some(m) = &self.metrics {
                m.on_wake_registered();
            }
            true
        } else {
            if let Some(m) = &self.metrics {
                m.on_wake_folded();
            }
            false
        }
    }

    /// Consumes the pending wake for `session` (returns `true` when one
    /// existed). Deliberately losable: a lost take is repaired by the
    /// drain's inbox re-evaluation or the startup scan.
    pub fn take(&self, session: &SessionId) -> bool {
        self.pending.remove(session).is_some()
    }

    #[must_use]
    pub fn contains(&self, session: &SessionId) -> bool {
        self.pending.contains(session)
    }

    /// The pendingWake depth gauge.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use kian_core::model::SessionId;

    use super::WakeRegistry;

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    /// Wakes fold: repeated registers collapse to one pending entry,
    /// and folding is distinguishable from a fresh registration.
    #[test]
    fn wakes_fold_onto_a_pending_entry() {
        let r = WakeRegistry::new();
        let s = sid(1);
        assert!(r.register(&s));
        assert!(!r.register(&s));
        assert!(!r.register(&s));
        assert_eq!(r.len(), 1);
        assert!(r.contains(&s));
    }

    /// Wakes are losable and concurrency-safe: take removes exactly the
    /// one entry; distinct sessions stay independent.
    #[test]
    fn take_is_losable_and_per_session() {
        let r = WakeRegistry::new();
        let a = sid(1);
        let b = sid(2);
        r.register(&a);
        r.register(&b);
        assert!(r.take(&a));
        assert!(!r.take(&a));
        assert!(r.contains(&b));
        assert_eq!(r.len(), 1);
    }
}
