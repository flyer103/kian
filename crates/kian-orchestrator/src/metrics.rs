//! Orchestrator metrics counters.
//!
//! Observability lives at the orchestrator layer (plan:
//! "metrics code inside core crates" is forbidden — this crate is where
//! it belongs). The counters are plain atomics: cheap to bump from drain
//! tasks, snapshottable for serve/CLI exposure later.
//!
//! Counters:
//! - `state_transitions` — durable state-changing events appended
//!   (`session/created`, `drain/claimed`, `session/ended`).
//! - `drains_started` / `drain_depth` / `drain_duration_ms_total` —
//!   drain executor liveness and cost. `drain_depth` is a gauge
//!   (concurrent drains across sessions; per-session drains are serial).
//! - `wakes_registered` / `wakes_folded` — wake-registry traffic; the
//!   pendingWake *depth* gauge is [`crate::wake::WakeRegistry::len`].

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Atomic counter set owned by the engine (shared into drain contexts
/// and the wake registry through `Arc`).
#[derive(Debug, Default)]
pub struct Metrics {
    state_transitions: AtomicU64,
    drains_started: AtomicU64,
    drain_depth: AtomicI64,
    drain_duration_ms_total: AtomicU64,
    end_writes: AtomicU64,
    wakes_registered: AtomicU64,
    wakes_folded: AtomicU64,
}

/// A point-in-time copy of [`Metrics`] for exposure (serde-shaped for
/// the built-in serve metrics endpoint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct MetricsSnapshot {
    pub state_transitions: u64,
    pub drains_started: u64,
    /// Current concurrent-drain gauge (negative would be a bug).
    pub drain_depth: i64,
    pub drain_duration_ms_total: u64,
    pub end_writes: u64,
    pub wakes_registered: u64,
    pub wakes_folded: u64,
}

impl Metrics {
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            state_transitions: self.state_transitions.load(Ordering::Relaxed),
            drains_started: self.drains_started.load(Ordering::Relaxed),
            drain_depth: self.drain_depth.load(Ordering::Relaxed),
            drain_duration_ms_total: self.drain_duration_ms_total.load(Ordering::Relaxed),
            end_writes: self.end_writes.load(Ordering::Relaxed),
            wakes_registered: self.wakes_registered.load(Ordering::Relaxed),
            wakes_folded: self.wakes_folded.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn on_state_transition(&self) {
        self.state_transitions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn on_drain_started(&self) {
        self.drains_started.fetch_add(1, Ordering::Relaxed);
        self.drain_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn on_drain_finished(&self, duration_ms: u64) {
        self.drain_depth.fetch_sub(1, Ordering::Relaxed);
        self.drain_duration_ms_total
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub(crate) fn on_end_write(&self) {
        self.end_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn on_wake_registered(&self) {
        self.wakes_registered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn on_wake_folded(&self) {
        self.wakes_folded.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::{Metrics, MetricsSnapshot};

    /// Counters start at zero and a snapshot reflects every bump.
    #[test]
    fn counters_start_at_zero_and_track_bumps() {
        let m = Metrics::default();
        assert_eq!(m.snapshot(), MetricsSnapshot::default());
        m.on_state_transition();
        m.on_state_transition();
        m.on_drain_started();
        m.on_end_write();
        m.on_wake_folded();
        m.on_drain_finished(7);
        let s = m.snapshot();
        assert_eq!(s.state_transitions, 2);
        assert_eq!(s.drains_started, 1);
        assert_eq!(s.drain_depth, 0);
        assert_eq!(s.drain_duration_ms_total, 7);
        assert_eq!(s.end_writes, 1);
        assert_eq!(s.wakes_registered, 0);
        assert_eq!(s.wakes_folded, 1);
    }
}
