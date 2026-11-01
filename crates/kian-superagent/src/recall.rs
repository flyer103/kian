//! The `RecallPort` read port (card C34) — the bundle's cross-session
//! read surface.
//!
//! The bundle's other two ports are scoped: [`crate::inbox::InboxWriter`]
//! is the write surface (admit + wake), and [`crate::worker::StreamPort`]
//! is per-hold and own-session only — a decorator may read the session
//! it currently holds, nothing else. Recall needs more: a worker
//! reading the Router's stream, a thread reading a sibling thread, a
//! trigger reading a thread. That is this port: cross-session, outside
//! any hold, **reads only**.
//!
//! The trait is bundle-defined and **assembly-implemented** (kian-cli,
//! C34): assembly serves it over the store's public scan API; the
//! bundle crate itself never names a store (`tests/iron_rule.rs`
//! enforces that textually, exactly as for the other ports).
//!
//! The spill index ([`SpillIndex`]) is the disposable half: a map from
//! channel `event_id` to where its verbatim raw lives in the log — the
//! `(session, seq)` of the `input/spilled` event. It is a cache of
//! *locations*, never a copy of content (constraint 1 by construction:
//! the raw itself lives only in the stream; drop the index and replay
//! the streams and the same map comes back). Assembly rebuilds it at
//! startup; nothing authoritative ever depends on it surviving.

use std::collections::{BTreeMap, BTreeSet};

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;

/// Failure of a [`RecallPort`] read. Reads fail only on genuine
/// transport trouble; an unknown session, an empty window or an
/// unknown id is **not** a failure — it is an honest empty result (the
/// port refuses nothing that is read-only).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecallPortError {
    #[error("recall read failed: {0}")]
    Read(String),
}

/// The bundle's cross-session read port (C34), implemented by assembly
/// over the store's public scan. Implementations must stay read-only:
/// no execution surface, no writes, no projection mutated. The methods
/// resolve concurrently with drains — a read never waits on a drain
/// hold (the C11 liveness discipline).
pub trait RecallPort: Send + Sync {
    /// Reads one session's committed events in seq order over
    /// `[since_seq, until_seq)`. An unknown session or an empty window
    /// yields an empty vector — never an error.
    fn read_events(
        &self,
        session: SessionId,
        since_seq: u64,
        until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, RecallPortError>>;

    /// The threads spawned by `parent` (the Router): one
    /// `(thread_id, worker_session)` pair per `thread/started` event
    /// on the parent's stream, in stream order — the same source
    /// [`crate::registry::PassthroughRegistry::rebuild`] derives its
    /// worker set from.
    fn list_threads(
        &self,
        parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, RecallPortError>>;

    /// Where the verbatim raw of a spilled channel event lives: the
    /// `(session, seq)` of its `input/spilled` event, if the startup
    /// rebuild saw it. The location only — the raw itself is read back
    /// through [`RecallPort::read_events`], so the stream stays the
    /// sole copy of the content (constraint 1).
    fn spill_location(
        &self,
        event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>>;
}

/// The disposable spill index (C34): channel `event_id` →
/// `(session, seq)` of its `input/spilled` event. Rebuilt at startup
/// by scanning the streams; never authoritative — the events are. A
/// pure projection over the log, safe to drop at any moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpillIndex {
    entries: BTreeMap<String, (SessionId, u64)>,
}

impl SpillIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the index from event streams (any sessions, any
    /// order). Only `input/spilled` events contribute: the payload's
    /// `event_id` field keys the entry; the event's own
    /// `(session, seq)` is the value. First write wins for a repeated
    /// id (shaped-once admission dedupes upstream, so a duplicate here
    /// can only be a re-admission artifact — the earliest location is
    /// where the audit trail starts). Malformed payloads (missing or
    /// non-string `event_id`) are skipped, never fatal.
    pub fn rebuild<'a>(events: impl IntoIterator<Item = &'a Event>) -> Self {
        let mut entries = BTreeMap::new();
        for ev in events {
            if ev.kind != EventKind::InputSpilled {
                continue;
            }
            let Some(event_id) = ev.bytes.get("event_id").and_then(|v| v.as_str()) else {
                continue;
            };
            entries
                .entry(event_id.to_string())
                .or_insert((ev.session, ev.seq));
        }
        Self { entries }
    }

    /// The `(session, seq)` of the spilled raw for `event_id`, if
    /// indexed.
    pub fn lookup(&self, event_id: &str) -> Option<(SessionId, u64)> {
        self.entries.get(event_id).copied()
    }

    /// Sorted indexed ids (deterministic iteration for
    /// tests/inspection).
    pub fn event_ids(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Derives `(thread_id, worker_session)` pairs from `thread/started`
/// events — the derivation behind [`RecallPort::list_threads`], the
/// same event source [`crate::registry::PassthroughRegistry::rebuild`]
/// scans for its worker set. Pairs come back in stream order; a
/// repeated `thread_id` keeps its first appearance; malformed entries
/// (missing `thread_id` or an ill-formed `worker` id) are skipped,
/// never fatal.
pub fn thread_bindings<'a>(
    events: impl IntoIterator<Item = &'a Event>,
) -> Vec<(String, SessionId)> {
    let mut out = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for ev in events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        let (Some(thread_id), Some(worker)) = (
            ev.bytes.get("thread_id").and_then(|v| v.as_str()),
            ev.bytes
                .get("worker")
                .and_then(|w| w.as_str())
                .and_then(|w| uuid::Uuid::parse_str(w).ok()),
        ) else {
            continue;
        };
        if seen.insert(thread_id.to_string()) {
            out.push((thread_id.to_string(), SessionId(worker)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

    use super::{SpillIndex, thread_bindings};
    use kian_core::events::{Event, EventKind};

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    fn event(session: SessionId, seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(2),
            ts: 1_000 + seq,
            kind,
            bytes,
            version: 0,
        }
    }

    fn spilled(session: SessionId, seq: u64, event_id: &str) -> Event {
        event(
            session,
            seq,
            EventKind::InputSpilled,
            json!({
                "event_id": event_id,
                "channel": "mock-im",
                "topic": null,
                "raw": {"text": "verbatim"},
            }),
        )
    }

    #[test]
    fn spill_index_maps_event_id_to_its_stream_location() {
        let router = sid(1);
        let worker = sid(2);
        let index = SpillIndex::rebuild([
            &event(router, 1, EventKind::SessionCreated, json!({})),
            &spilled(router, 5, "evt-router"),
            &spilled(worker, 3, "evt-worker-a"),
            &spilled(worker, 7, "evt-worker-b"),
        ]);
        assert_eq!(index.len(), 3);
        assert_eq!(index.lookup("evt-router"), Some((router, 5)));
        assert_eq!(index.lookup("evt-worker-a"), Some((worker, 3)));
        assert_eq!(index.lookup("evt-worker-b"), Some((worker, 7)));
        assert_eq!(index.lookup("evt-unknown"), None);
        let mut ids = index.event_ids();
        ids.sort();
        assert_eq!(ids, vec!["evt-router", "evt-worker-a", "evt-worker-b"]);
    }

    #[test]
    fn spill_index_rebuild_is_deterministic_first_write_wins() {
        let a = sid(1);
        let b = sid(2);
        let events = vec![
            spilled(a, 4, "evt-dup"),
            spilled(b, 9, "evt-dup"), // re-admission artifact
            spilled(a, 5, "evt-only"),
        ];
        let first = SpillIndex::rebuild(&events);
        let second = SpillIndex::rebuild(&events);
        assert_eq!(first, second, "drop-and-rebuild equivalence");
        assert_eq!(first.lookup("evt-dup"), Some((a, 4)), "first write wins");
    }

    #[test]
    fn spill_index_skips_malformed_and_non_spilled_events() {
        let session = sid(1);
        let malformed = event(
            session,
            2,
            EventKind::InputSpilled,
            json!({"channel": "mock-im", "raw": {}}), // no event_id
        );
        let not_string = event(
            session,
            3,
            EventKind::InputSpilled,
            json!({"event_id": 42, "raw": {}}),
        );
        let admitted = event(
            session,
            4,
            EventKind::InputAdmitted,
            // A spilled-shaped payload under the wrong kind must not
            // index (the kind is the authority, not the shape).
            json!({"event_id": "evt-wrong-kind"}),
        );
        let index = SpillIndex::rebuild([&malformed, &not_string, &admitted]);
        assert!(index.is_empty(), "nothing indexable was found");
    }

    #[test]
    fn thread_bindings_extracts_pairs_and_skips_malformed() {
        let router = sid(1);
        let worker_a = sid(2);
        let worker_b = sid(3);
        let started = |seq: u64, thread_id: &str, worker: SessionId| {
            event(
                router,
                seq,
                EventKind::ThreadStarted,
                json!({
                    "thread_id": thread_id,
                    "parent": router.0.to_string(),
                    "worker": worker.0.to_string(),
                    "pinned_version": 1,
                    "task": {"role": "user", "content": "t"},
                }),
            )
        };
        let bad_worker = event(
            router,
            8,
            EventKind::ThreadStarted,
            json!({"thread_id": "t-bad", "worker": "not-a-uuid"}),
        );
        let events = vec![
            started(3, "t-a", worker_a),
            event(router, 4, EventKind::TurnStart, json!({})),
            started(5, "t-b", worker_b),
            started(6, "t-a", worker_b), // duplicate thread_id: first wins
            bad_worker,
        ];
        assert_eq!(
            thread_bindings(&events),
            vec![("t-a".to_string(), worker_a), ("t-b".to_string(), worker_b),]
        );
        assert!(thread_bindings(std::iter::empty::<&Event>()).is_empty());
    }
}
