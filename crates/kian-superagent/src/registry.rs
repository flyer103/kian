//! Passthrough worker registry: `topic → set of worker sessions`
//! (card C38: many-to-many fan-out).
//!
//! A **disposable projection** (constraint 1): nothing authoritative
//! lives here. At runtime the spawner registers fresh workers
//! (`register` *adds* to the set — workers sharing a topic observe each
//! other's events and self-resolve conflicts without peer links; the
//! Router stays the cross-task rendezvous); after a restart the mapping
//! is *rebuilt* from event streams alone by
//! [`PassthroughRegistry::rebuild`] — `thread/started` events name the
//! worker sessions (and may carry the spawning topic in the delegated
//! task's content), and the workers' own `input/admitted` payloads
//! carry the channel-event wrapper with the topic they served. Drop the
//! registry and replay the streams and you get the same map back.

use std::collections::{BTreeMap, BTreeSet};

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;

use crate::provenance::parse_channel_marker;

/// The runtime projection: routing topic → the *set* of worker sessions
/// subscribed to it (C38 fan-out). Guarded by a lock at the call site;
/// cheap to rebuild and safe to discard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassthroughRegistry {
    topics: BTreeMap<String, BTreeSet<SessionId>>,
}

impl PassthroughRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a subscriber to a topic's set. Called at runtime by the
    /// worker spawner (C25); a second worker on the same topic joins
    /// the set — both then observe the topic's events. Restarts
    /// re-derive the same entries via [`PassthroughRegistry::rebuild`]
    /// instead.
    pub fn register(&mut self, topic: impl Into<String>, worker: SessionId) {
        self.topics.entry(topic.into()).or_default().insert(worker);
    }

    /// The set of worker sessions subscribed to `topic` — empty means
    /// unbound (the subscription service's Router fallback).
    pub fn resolve(&self, topic: &str) -> BTreeSet<SessionId> {
        self.topics.get(topic).cloned().unwrap_or_default()
    }

    /// Sorted topic list (deterministic iteration for tests/inspection).
    pub fn topics(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.topics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }

    /// Rebuilds the mapping from event streams — the NoLostWork analog
    /// for subscriptions: a restarted server loses nothing because the
    /// map is a pure function of the log. `events` may interleave
    /// sessions arbitrarily; every [`Event`] carries its session id.
    ///
    /// Sources scanned, in order of authority:
    ///
    /// 1. `thread/started` — the `worker` field identifies worker
    ///    sessions; the delegated task's `content` may embed the
    ///    channel-event wrapper naming the topic the worker was spawned
    ///    to serve (binds immediately).
    /// 2. `input/admitted` on a *worker* session — a message content
    ///    carrying the channel-event wrapper proves that worker served
    ///    that topic (binds).
    ///
    /// Both sources *add* to each topic's set: every worker that ever
    /// served the topic is a subscriber (the fan-out semantics), and a
    /// topic bound by several workers rebuilds to exactly their set.
    pub fn rebuild<'a>(events: impl IntoIterator<Item = &'a Event>) -> Self {
        let events: Vec<&Event> = events.into_iter().collect();

        // Pass 1: which sessions are workers (named by thread/started)?
        let mut workers: BTreeSet<SessionId> = BTreeSet::new();
        for ev in &events {
            if ev.kind == EventKind::ThreadStarted
                && let Some(worker) = bytes_worker(&ev.bytes)
            {
                workers.insert(worker);
            }
        }

        let mut topics: BTreeMap<String, BTreeSet<SessionId>> = BTreeMap::new();
        for ev in &events {
            if ev.kind != EventKind::ThreadStarted {
                continue;
            }
            // The delegated task is a message payload; its content may
            // carry the channel-event wrapper with the spawning topic.
            let Some(worker) = bytes_worker(&ev.bytes) else {
                continue;
            };
            if let Some(marker) = ev
                .bytes
                .get("task")
                .and_then(|task| task.get("content"))
                .and_then(parse_channel_marker)
                && let Some(topic) = marker.topic
            {
                topics.entry(topic).or_default().insert(worker);
            }
        }
        for ev in &events {
            if ev.kind != EventKind::InputAdmitted || !workers.contains(&ev.session) {
                continue;
            }
            if let Some(marker) = ev.bytes.get("content").and_then(parse_channel_marker)
                && let Some(topic) = marker.topic
            {
                topics.entry(topic).or_default().insert(ev.session);
            }
        }
        Self { topics }
    }
}

/// The worker session ids named by a Router stream's `thread/started`
/// events — how a restarting server discovers which worker streams to
/// scan without any out-of-band state.
pub fn worker_sessions(router_events: &[Event]) -> Vec<SessionId> {
    let mut seen: BTreeSet<SessionId> = BTreeSet::new();
    for ev in router_events {
        if ev.kind == EventKind::ThreadStarted
            && let Some(worker) = bytes_worker(&ev.bytes)
        {
            seen.insert(worker);
        }
    }
    seen.into_iter().collect()
}

/// `bytes.worker` as a session id, if present and well-formed.
fn bytes_worker(bytes: &serde_json::Value) -> Option<SessionId> {
    let raw = bytes.get("worker").and_then(|w| w.as_str())?;
    Some(SessionId(uuid::Uuid::parse_str(raw).ok()?))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{PassthroughRegistry, worker_sessions};
    use crate::provenance::{channel_input_payload, wrap_channel_event};
    use kian_core::events::{Event, EventKind};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

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

    fn thread_started(router: SessionId, worker: SessionId, task: serde_json::Value) -> Event {
        event(
            router,
            3,
            EventKind::ThreadStarted,
            json!({
                "thread_id": "00000000-0000-0000-0000-0000000000aa",
                "parent": router.0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 1,
                "task": task,
            }),
        )
    }

    #[test]
    fn rebuild_binds_topic_from_thread_task_content() {
        let router = sid(1);
        let worker = sid(2);
        let marker = wrap_channel_event(&kian_core::seams::ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-0".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "ok"}),
            ts_ms: 1,
        });
        let task = json!({"role": "user", "content": marker});
        let registry = PassthroughRegistry::rebuild([&thread_started(router, worker, task)]);
        // C38: a single binding rebuilds to the singleton set.
        assert_eq!(registry.resolve("pr-9"), BTreeSet::from([worker]));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rebuild_binds_topic_from_worker_channel_input() {
        let router = sid(1);
        let worker = sid(2);
        let mut events = vec![thread_started(
            router,
            worker,
            json!({"role": "user", "content": "task"}),
        )];
        // A later channel input on the worker stream binds its topic.
        let payload = channel_input_payload(&kian_core::seams::ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-7".into(),
            topic: Some("pr-10".into()),
            sender: None,
            group: None,
            payload: json!({"build": "failed"}),
            ts_ms: 2,
        });
        events.push(event(
            worker,
            5,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": payload}),
        ));
        let registry = PassthroughRegistry::rebuild(&events);
        assert_eq!(registry.resolve("pr-10"), BTreeSet::from([worker]));
    }

    #[test]
    fn rebuild_derives_the_fanout_set_from_both_sources() {
        // C38: two workers on one topic — one bound by its delegation
        // task's channel marker (source 1), the other by a channel
        // input on its own stream (source 2). Rebuild yields the set
        // of both: every worker that served the topic is a subscriber.
        let router = sid(1);
        let worker_a = sid(2);
        let worker_b = sid(3);
        let marker = wrap_channel_event(&kian_core::seams::ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-spawn-a".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"spawn": true}),
            ts_ms: 1,
        });
        let mut events = vec![
            thread_started(router, worker_a, json!({"role": "user", "content": marker})),
            // B is a worker (thread/started names it) but its task
            // carries no topic — its binding comes from its own stream.
            thread_started(router, worker_b, json!({"role": "user", "content": "task"})),
        ];
        let payload = channel_input_payload(&kian_core::seams::ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-b".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "ok"}),
            ts_ms: 2,
        });
        events.push(event(
            worker_b,
            4,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": payload}),
        ));
        let registry = PassthroughRegistry::rebuild(&events);
        assert_eq!(
            registry.resolve("pr-9"),
            BTreeSet::from([worker_a, worker_b])
        );
        assert_eq!(registry.len(), 1, "one topic, two subscribers");
    }

    #[test]
    fn rebuild_ignores_non_worker_channel_inputs() {
        // A Router's own channel inputs carry topics but the Router is
        // not a worker — no binding may be created from its stream.
        let router = sid(1);
        let payload = channel_input_payload(&kian_core::seams::ChannelEvent {
            channel: "mock-im".into(),
            event_id: "evt-8".into(),
            topic: Some("chat".into()),
            sender: None,
            group: None,
            payload: json!({"text": "hi"}),
            ts_ms: 3,
        });
        let events = [event(
            router,
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": payload}),
        )];
        let registry = PassthroughRegistry::rebuild(events.iter());
        assert!(registry.is_empty());
    }

    #[test]
    fn rebuild_is_deterministic_and_disposable() {
        // Same events in any order → same map; the map carries no state
        // beyond its entries.
        let router = sid(1);
        let worker = sid(2);
        let task = json!({"role": "user", "content": "t"});
        let mut events = vec![
            thread_started(router, worker, task),
            event(router, 1, EventKind::SessionCreated, json!({})),
        ];
        let a = PassthroughRegistry::rebuild(&events);
        events.reverse();
        let b = PassthroughRegistry::rebuild(&events);
        assert_eq!(a, b);
        assert_eq!(worker_sessions(&events), vec![worker]);

        // Malformed worker ids are skipped, never fatal.
        let bad = event(
            router,
            9,
            EventKind::ThreadStarted,
            json!({"worker": "not-a-uuid"}),
        );
        assert_eq!(worker_sessions(&[bad]), Vec::<SessionId>::new());
    }

    #[test]
    fn runtime_register_accumulates_workers_into_a_set() {
        // C38: registering a second worker on a topic adds it to the
        // set (no re-bind, no eviction) — both subscribers stay.
        let mut r = PassthroughRegistry::new();
        assert!(r.is_empty());
        r.register("ci", sid(7));
        assert_eq!(r.resolve("ci"), BTreeSet::from([sid(7)]));
        r.register("ci", sid(8));
        assert_eq!(r.resolve("ci"), BTreeSet::from([sid(7), sid(8)]));
        // Re-registering an existing subscriber is idempotent.
        r.register("ci", sid(7));
        assert_eq!(r.resolve("ci"), BTreeSet::from([sid(7), sid(8)]));
        assert_eq!(r.topics(), vec!["ci".to_string()]);
        // An unknown topic resolves to the empty set (unbound → the
        // subscription service's Router fallback).
        assert!(r.resolve("unknown").is_empty());
    }
}
