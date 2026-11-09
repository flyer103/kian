//! Membership types shared by the registration listener and (R12-04) the
//! worker directory: the pure `member_state` projection and the identity
//! validators (spec §3.1, §1.3).
//!
//! Membership is **liveness, not truth** (I2): nothing here writes
//! sessions, events or bindings; the durable `endpoint_registry` row is a
//! disposable projection.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::proto::ServerFrame;

/// Computed member state — never stored, always derived (M3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemberState {
    /// Live connection **and** `now - last_seen_ms <= offline_after_ms`.
    Online,
    /// Teardown window: leave/replace began, route not yet detached.
    Draining,
    /// Everything else.
    Offline,
}

/// A disposable snapshot of one member (the runtime directory's entry;
/// `connection` presence is the only liveness fact carried).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSnapshot {
    pub id: String,
    pub description: Option<String>,
    /// The worker's declared protocol revision.
    pub version: u32,
    /// +1 on every accepted registration (replace included).
    pub generation: u64,
    pub max_concurrent: u32,
    /// Observed peer address — observability only, never identity.
    pub peer_addr: SocketAddr,
    /// Server clock (ms since epoch), monotone max.
    pub last_seen_ms: u64,
    /// Whether a live connection is attached.
    pub connected: bool,
    /// True between a leave/replace and the route detach.
    pub teardown: bool,
}

/// Pure state computation (spec §3.1):
/// - `Online` iff a live connection exists **and**
///   `now - last_seen_ms <= offline_after_ms`;
/// - `Draining` iff teardown began and no connection remains;
/// - `Offline` otherwise.
pub fn member_state(m: &MemberSnapshot, now_ms: u64, offline_after_ms: u64) -> MemberState {
    if m.connected && now_ms.saturating_sub(m.last_seen_ms) <= offline_after_ms {
        MemberState::Online
    } else if m.teardown && !m.connected {
        MemberState::Draining
    } else {
        MemberState::Offline
    }
}

/// Worker-id shape rule (spec §1.3): 1..=200 bytes after trim, no
/// control characters. The worker validates at startup; the server
/// re-checks and answers `id_conflict` on violation.
pub fn valid_worker_id(id: &str) -> bool {
    let trimmed = id.trim();
    let len_ok = (1..=200).contains(&trimmed.len());
    len_ok && !trimmed.bytes().any(|b| b.is_ascii_control())
}

/// Description rule (spec §1.3/P11): non-empty after trim, ≤200 chars,
/// no control characters. Server-side a violation drops the description
/// (audited, registration continues); worker-side it refuses startup.
pub fn valid_description(description: &str) -> bool {
    let trimmed = description.trim();
    let len_ok = (1..=200).contains(&trimmed.chars().count());
    len_ok && !trimmed.chars().any(|c| c.is_control())
}

/// The mutable liveness facts behind one member (R12-04). The `writer`
/// half is the listener's reach onto the live connection (the replace
/// `bye`); everything else is the spec §3.1 directory entry.
pub(crate) struct MemberEntry {
    pub(crate) description: Option<String>,
    pub(crate) version: u32,
    pub(crate) generation: u64,
    pub(crate) max_concurrent: u32,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) last_seen_ms: u64,
    pub(crate) connected: bool,
    pub(crate) teardown: bool,
    pub(crate) writer: Option<mpsc::Sender<ServerFrame>>,
}

impl MemberEntry {
    fn snapshot(&self, id: &str) -> MemberSnapshot {
        MemberSnapshot {
            id: id.to_string(),
            description: self.description.clone(),
            version: self.version,
            generation: self.generation,
            max_concurrent: self.max_concurrent,
            peer_addr: self.peer_addr,
            last_seen_ms: self.last_seen_ms,
            connected: self.connected,
            teardown: self.teardown,
        }
    }
}

/// The runtime membership directory (M1): `worker id → member facts`,
/// runtime-only, disposable, never a truth. The registration listener
/// is the sole writer (crate-internal mutators); every consumer reads
/// snapshots and computes state through [`member_state`] (M3).
///
/// A kian boot starts with an **empty** directory: registry rows persist
/// (the durable projection) but the runtime map fills only as workers
/// re-register (spec §3.2 "kian restart" row — no forged joins).
#[derive(Default)]
pub struct WorkerDirectory {
    members: Mutex<BTreeMap<String, MemberEntry>>,
}

impl WorkerDirectory {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot of every member, id-ascending.
    pub fn snapshot(&self) -> BTreeMap<String, MemberSnapshot> {
        self.members
            .lock()
            .unwrap()
            .iter()
            .map(|(id, entry)| (id.clone(), entry.snapshot(id)))
            .collect()
    }

    /// One member's snapshot, when known since boot.
    pub fn member(&self, id: &str) -> Option<MemberSnapshot> {
        self.members.lock().unwrap().get(id).map(|e| e.snapshot(id))
    }

    /// Every id that registered since boot (id-ascending).
    pub fn known_ids(&self) -> Vec<String> {
        self.members.lock().unwrap().keys().cloned().collect()
    }

    /// The first member computing `Online` at `now_ms` (id-ascending) —
    /// the dynamic `default_endpoint` resolution's preference.
    pub fn first_online(&self, now_ms: u64, offline_after_ms: u64) -> Option<String> {
        self.members.lock().unwrap().iter().find_map(|(id, entry)| {
            (matches!(
                member_state(&entry.snapshot(id), now_ms, offline_after_ms),
                MemberState::Online
            ))
            .then(|| id.clone())
        })
    }

    // -- listener-side mutators (crate-internal; §3.2 transitions) --------

    /// Insert/replace a registration's directory entry with no live
    /// connection yet (spec §9.1 step 2). Returns the new generation
    /// (`+1` on every accepted registration, replace included).
    pub(crate) fn register(
        &self,
        id: &str,
        description: Option<String>,
        version: u32,
        max_concurrent: u32,
        peer_addr: SocketAddr,
        last_seen_ms: u64,
    ) -> u64 {
        let mut members = self.members.lock().unwrap();
        let generation = members
            .get(id)
            .map(|entry| entry.generation + 1)
            .unwrap_or(1);
        members.insert(
            id.to_string(),
            MemberEntry {
                description,
                version,
                generation,
                max_concurrent,
                peer_addr,
                last_seen_ms,
                connected: false,
                teardown: false,
                writer: None,
            },
        );
        generation
    }

    /// Mark the entry live (spec §9.1 step 5).
    pub(crate) fn set_live(&self, id: &str, writer: mpsc::Sender<ServerFrame>) {
        if let Some(entry) = self.members.lock().unwrap().get_mut(id) {
            entry.connected = true;
            entry.writer = Some(writer);
        }
    }

    /// The live connection's writer (the replace `bye` path).
    pub(crate) fn writer_of(&self, id: &str) -> Option<mpsc::Sender<ServerFrame>> {
        self.members
            .lock()
            .unwrap()
            .get(id)
            .and_then(|e| e.writer.clone())
    }

    /// Heartbeat freshness (server clock, monotone max — M10).
    pub(crate) fn note_seen(&self, id: &str, now_ms: u64) {
        if let Some(entry) = self.members.lock().unwrap().get_mut(id) {
            entry.last_seen_ms = entry.last_seen_ms.max(now_ms);
        }
    }

    /// Begin teardown (bye/loss/replace/shutdown): teardown spans the
    /// drain window only. Returns the entry's generation (None when the
    /// id is unknown).
    pub(crate) fn begin_teardown(&self, id: &str) -> Option<u64> {
        let mut members = self.members.lock().unwrap();
        members.get_mut(id).map(|entry| {
            entry.teardown = true;
            entry.connected = false;
            entry.writer = None;
            entry.generation
        })
    }

    /// End the teardown window (draining → offline).
    pub(crate) fn end_teardown(&self, id: &str) {
        if let Some(entry) = self.members.lock().unwrap().get_mut(id) {
            entry.teardown = false;
        }
    }

    /// Ownership check for generation-scoped teardown (A15): true iff
    /// the directory still holds this generation with a live connection.
    pub(crate) fn is_owner(&self, id: &str, generation: u64) -> bool {
        self.members
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|entry| entry.generation == generation && entry.connected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(connected: bool, last_seen_ms: u64, teardown: bool) -> MemberSnapshot {
        MemberSnapshot {
            id: "builder-1".into(),
            description: Some("linux builder".into()),
            version: 1,
            generation: 1,
            max_concurrent: 4,
            peer_addr: "127.0.0.1:50000".parse().unwrap(),
            last_seen_ms,
            connected,
            teardown,
        }
    }

    /// The membership state table (spec §3.2 rows, pure half) with an
    /// injected clock.
    #[test]
    fn member_state_table() {
        let now = 100_000u64;
        let offline_after = 30_000u64;

        // register accepted: connected + fresh → online.
        assert_eq!(
            member_state(&snap(true, now, false), now, offline_after),
            MemberState::Online
        );
        // heartbeat freshens: exactly at the bound is still online.
        assert_eq!(
            member_state(&snap(true, now - offline_after, false), now, offline_after),
            MemberState::Online
        );
        // heartbeat absence past the bound → offline (connection or not).
        assert_eq!(
            member_state(
                &snap(true, now - offline_after - 1, false),
                now,
                offline_after
            ),
            MemberState::Offline
        );
        assert_eq!(
            member_state(
                &snap(false, now - offline_after - 1, false),
                now,
                offline_after
            ),
            MemberState::Offline
        );
        // bye / transport loss: no connection, teardown window → draining.
        assert_eq!(
            member_state(&snap(false, now, true), now, offline_after),
            MemberState::Draining
        );
        // after route detach (teardown cleared): offline.
        assert_eq!(
            member_state(&snap(false, now, false), now, offline_after),
            MemberState::Offline
        );
    }

    #[test]
    fn worker_id_shape() {
        assert!(valid_worker_id("builder-1"));
        assert!(valid_worker_id("  padded  ")); // trim applies
        assert!(!valid_worker_id(""));
        assert!(!valid_worker_id("   "));
        assert!(valid_worker_id(&"x".repeat(200)));
        assert!(!valid_worker_id(&"x".repeat(201)));
        assert!(!valid_worker_id("bad\u{1}id"));
    }

    #[test]
    fn description_shape() {
        assert!(valid_description("linux builder"));
        assert!(!valid_description(""));
        assert!(!valid_description("   "));
        assert!(!valid_description(&"d".repeat(201)));
        assert!(valid_description(&"d".repeat(200)));
        assert!(!valid_description("ctrl\u{7}"));
    }
}
