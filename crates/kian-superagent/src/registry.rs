//! Passthrough worker registry: `topic → set of worker sessions`
//! (card C38: many-to-many fan-out), with the C59 liveness
//! projection layered on top.
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
//!
//! C59 (follow-up ownership): the registry also projects each worker's
//! **status** — `live` or `ended` — from the worker's own stream
//! (`thread/completed` or `session/ended`). [`PassthroughRegistry::resolve`]
//! keeps returning every subscriber (the historical record: a worker
//! that served a topic stays bound to it); the fan-out path resolves
//! through [`PassthroughRegistry::resolve_live`] instead, and an
//! all-ended set is the *release to fresh evaluation* signal (002's
//! Ticket-end rule) the subscription service turns into a Router
//! fallback.

use std::collections::{BTreeMap, BTreeSet};

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;

use crate::provenance::parse_channel_marker;

/// A worker's liveness (C59): `Live` until its own stream carries
/// `thread/completed` or `session/ended`, then `Ended` — terminal for
/// a kian Ticket v1 (one worker session's life; resumption is a fresh
/// Eval, never a same-session resume). A pure projection of the
/// stream: the runtime registry holds the same fact `register` /
/// [`PassthroughRegistry::mark_ended`] write, and a rebuild re-derives
/// it, so the two can be asserted equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    Live,
    Ended,
}

impl WorkerStatus {
    /// The stable machine spelling (audit lines, tests).
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Ended => "ended",
        }
    }
}

/// The runtime projection: routing topic → the *set* of worker sessions
/// subscribed to it (C38 fan-out), plus the C59 worker-status
/// projection. Guarded by a lock at the call site; cheap to rebuild and
/// safe to discard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassthroughRegistry {
    topics: BTreeMap<String, BTreeSet<SessionId>>,
    /// Workers whose streams ended (`thread/completed`/`session/ended`).
    /// Kept *beside* the topic sets, never removed from them: the sets
    /// are the historical binding record; `ended` is the liveness view
    /// the fan-out filters with.
    ended: BTreeSet<SessionId>,
}

impl PassthroughRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a subscriber to a topic's set. Called at runtime by the
    /// worker spawner (C25); a second worker on the same topic joins
    /// the set — both then observe the topic's events. Restarts
    /// re-derive the same entries via [`PassthroughRegistry::rebuild`]
    /// instead. Registration asserts liveness: a session registered
    /// here is `live` (an `ended` session id being re-registered is a
    /// caller bug, and the honest reading is the fresh assertion).
    pub fn register(&mut self, topic: impl Into<String>, worker: SessionId) {
        self.ended.remove(&worker);
        self.topics.entry(topic.into()).or_default().insert(worker);
    }

    /// Marks a worker's stream ended (C59). Idempotent; the topic sets
    /// are untouched — the worker stays a *historical* subscriber and
    /// [`PassthroughRegistry::resolve`] keeps naming it, but
    /// [`PassthroughRegistry::resolve_live`] stops returning it.
    pub fn mark_ended(&mut self, worker: SessionId) {
        self.ended.insert(worker);
    }

    /// The worker's projected status (C59). Unknown sessions are
    /// `live` — the registry only ever records the ended ones.
    #[must_use]
    pub fn status(&self, worker: &SessionId) -> WorkerStatus {
        if self.ended.contains(worker) {
            WorkerStatus::Ended
        } else {
            WorkerStatus::Live
        }
    }

    /// The set of worker sessions subscribed to `topic` — every worker
    /// that ever served it, live or ended (the historical record).
    /// Empty means unbound (the subscription service's Router
    /// fallback).
    pub fn resolve(&self, topic: &str) -> BTreeSet<SessionId> {
        self.topics.get(topic).cloned().unwrap_or_default()
    }

    /// The set of **live** subscribers of `topic` (C59): the historical
    /// set minus the ended workers. This is the fan-out admission set —
    /// an event never admits onto a terminal session where it would
    /// strand forever.
    #[must_use]
    pub fn resolve_live(&self, topic: &str) -> BTreeSet<SessionId> {
        let subs = self.resolve(topic);
        if self.ended.is_empty() {
            return subs;
        }
        subs.into_iter()
            .filter(|w| !self.ended.contains(w))
            .collect()
    }

    /// The deterministic first live subscriber of `topic` (C59's owner
    /// for the tool-surface refusal): session ids sort, so the owner
    /// named in a refusal is stable. `None` when no live worker owns
    /// the topic (unbound, or all subscribers ended — the release to
    /// fresh evaluation).
    #[must_use]
    pub fn live_owner(&self, topic: &str) -> Option<SessionId> {
        self.resolve_live(topic).into_iter().next()
    }

    /// The ended workers recorded by the projection (inspection/tests).
    #[must_use]
    pub fn ended_workers(&self) -> Vec<SessionId> {
        self.ended.iter().copied().collect()
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
    ///
    /// C59 status projection, derived in the same pass: a worker
    /// session whose own stream carries `thread/completed` or
    /// `session/ended` is `ended`. The terminal event must sit on the
    /// *worker's* stream — a Router-side `session/ended` (the re-arm
    /// chain) is not a worker fact and marks nothing. Rebuild is the
    /// ground truth the runtime registry's `register`/`mark_ended`
    /// writes must equal.
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

        let mut ended: BTreeSet<SessionId> = BTreeSet::new();
        for ev in &events {
            if !workers.contains(&ev.session) {
                continue;
            }
            match ev.kind {
                EventKind::ThreadCompleted => {
                    ended.insert(ev.session);
                }
                EventKind::SessionEnded { .. } => {
                    ended.insert(ev.session);
                }
                _ => {}
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
        Self { topics, ended }
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

    // -----------------------------------------------------------------
    // C59: the worker-status projection
    // -----------------------------------------------------------------

    #[test]
    fn resolve_live_filters_ended_workers_and_keeps_the_historical_set() {
        let mut r = PassthroughRegistry::new();
        r.register("pr-9", sid(7));
        r.register("pr-9", sid(8));
        assert_eq!(r.resolve_live("pr-9"), BTreeSet::from([sid(7), sid(8)]));
        r.mark_ended(sid(7));
        // The historical set is untouched; the live view drops the
        // ended worker.
        assert_eq!(r.resolve("pr-9"), BTreeSet::from([sid(7), sid(8)]));
        assert_eq!(r.resolve_live("pr-9"), BTreeSet::from([sid(8)]));
        assert_eq!(r.status(&sid(7)), super::WorkerStatus::Ended);
        assert_eq!(r.status(&sid(8)), super::WorkerStatus::Live);
        // Unknown sessions project live; the deterministic first live
        // subscriber is the sorted minimum.
        assert_eq!(r.status(&sid(99)), super::WorkerStatus::Live);
        assert_eq!(r.live_owner("pr-9"), Some(sid(8)));
        r.mark_ended(sid(8));
        // All ended: no live owner — the release-to-fresh-Eval shape.
        assert!(r.resolve_live("pr-9").is_empty());
        assert_eq!(r.live_owner("pr-9"), None);
        assert_eq!(r.ended_workers(), vec![sid(7), sid(8)]);
        // mark_ended is idempotent.
        r.mark_ended(sid(7));
        assert_eq!(r.ended_workers(), vec![sid(7), sid(8)]);
    }

    #[test]
    fn rebuild_derives_ended_status_from_worker_terminal_events() {
        // Mixed live/ended stream: two workers bound to one topic (the
        // delegation-marker source), one whose stream ended with
        // `thread/completed`, one still live; a third worker ended via
        // `session/ended` on another topic. The Router's own
        // `session/ended` (re-arm chain) must mark nothing.
        let router = sid(1);
        let live = sid(2);
        let done = sid(3);
        let session_ended_worker = sid(4);
        let marker = |topic: &str| {
            wrap_channel_event(&kian_core::seams::ChannelEvent {
                channel: "ci".into(),
                event_id: "evt-0".into(),
                topic: Some(topic.into()),
                sender: None,
                group: None,
                payload: json!({"build": "ok"}),
                ts_ms: 1,
            })
        };
        let mut events = vec![
            thread_started(
                router,
                live,
                json!({"role": "user", "content": marker("pr-9")}),
            ),
            thread_started(
                router,
                done,
                json!({"role": "user", "content": marker("pr-9")}),
            ),
            thread_started(
                router,
                session_ended_worker,
                json!({"role": "user", "content": marker("pr-10")}),
            ),
        ];
        events.push(event(
            done,
            7,
            EventKind::ThreadCompleted,
            json!({"status": "completed"}),
        ));
        events.push(event(
            session_ended_worker,
            8,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed,
            },
            json!({}),
        ));
        events.push(event(
            router,
            9,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed,
            },
            json!({}),
        ));

        let rebuilt = PassthroughRegistry::rebuild(&events);
        // Historical sets keep every subscriber.
        assert_eq!(rebuilt.resolve("pr-9"), BTreeSet::from([live, done]));
        // Live views drop the ended ones.
        assert_eq!(rebuilt.resolve_live("pr-9"), BTreeSet::from([live]));
        assert_eq!(rebuilt.status(&done), super::WorkerStatus::Ended);
        assert_eq!(
            rebuilt.status(&session_ended_worker),
            super::WorkerStatus::Ended
        );
        assert!(rebuilt.resolve_live("pr-10").is_empty());
        // The Router is not a worker: its session/ended marks nothing.
        assert_eq!(rebuilt.status(&router), super::WorkerStatus::Live);

        // The runtime map (register + mark_ended) equals the rebuild.
        let mut runtime = PassthroughRegistry::new();
        runtime.register("pr-9", live);
        runtime.register("pr-9", done);
        runtime.register("pr-10", session_ended_worker);
        runtime.mark_ended(done);
        runtime.mark_ended(session_ended_worker);
        assert_eq!(rebuilt, runtime);
    }
}
