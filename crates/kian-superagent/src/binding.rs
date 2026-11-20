//! ADR-040 phase 1: one shared binding for the live resident session.
//!
//! Every router-facing surface reads the live Router id through this
//! binding instead of capturing a `SessionId` at construction, so a
//! rotation (phase 2) re-points the driver routing, the mounted
//! residency inbox, the spawner's thread parent, the worker completion
//! reports, the subscription `router` route, the Router tools/mandate
//! grant and the recall reader in one place. Phase 1 is
//! behavior-identical: with a single store the id never changes.
//!
//! The binding is deliberately small (no generation counter, no
//! persistence): the durable truth stays the event stream (the
//! deterministic uuid5 chain), and a process restart re-derives the live
//! session from it.

use std::sync::{Arc, RwLock};

use kian_core::model::SessionId;

/// The live resident session, shared by every consumer.
#[derive(Debug)]
pub struct ResidentBinding {
    live: RwLock<SessionId>,
}

impl ResidentBinding {
    /// A binding pinned to `session` (the boot resolution).
    #[must_use]
    pub fn new(session: SessionId) -> Arc<Self> {
        Arc::new(Self {
            live: RwLock::new(session),
        })
    }

    /// The current live resident session.
    #[must_use]
    pub fn load(&self) -> SessionId {
        *self
            .live
            .read()
            .expect("resident binding poisoned by a panicking writer")
    }

    /// Re-points the binding at the successor (rotation). Phase 1 never
    /// calls it; phase 2's rotation does, after the predecessor's
    /// terminal end lands.
    pub fn store(&self, session: SessionId) {
        *self
            .live
            .write()
            .expect("resident binding poisoned by a panicking writer") = session;
    }
}

#[cfg(test)]
mod tests {
    use kian_core::model::SessionId;

    use super::ResidentBinding;

    #[test]
    fn the_binding_loads_its_construction_and_follows_a_store() {
        let first = SessionId::new();
        let second = SessionId::new();
        let binding = ResidentBinding::new(first);
        assert_eq!(binding.load(), first);
        binding.store(second);
        assert_eq!(binding.load(), second);
        let shared = std::sync::Arc::clone(&binding);
        binding.store(first);
        assert_eq!(shared.load(), first);
    }
}
