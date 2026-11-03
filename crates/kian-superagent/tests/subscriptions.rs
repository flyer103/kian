//! Integration tests (card C23): the subscription framework against a
//! real sqlite `EventStore` and a real `Engine` — through the
//! `InboxWriter` port only, exactly the way assembly (C27) will wire
//! it. No credentials, no network; everything local.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{StreamExt, once};
use kian_core::events::{Event, EventKind};
use kian_core::model::{InputRow, SessionId};
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelEvent, LoopDriver, LoopOutcome};
use kian_orchestrator::{DrainCtx, Engine, EngineConfig, WakeOutcome};
use kian_store::{AdmitOutcome, EventStore};
use kian_superagent::provenance::{parse_channel_marker, wrap_channel_event};
use kian_superagent::{
    AdmitReceipt, CorrelateBy, InboxWriteError, InboxWriter, MockChannel, PassthroughRegistry,
    Route, RoutingOutcome, RuleMatcher, SubscriptionRule, SubscriptionRuleSet, SubscriptionService,
    worker_sessions,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    // The temp file outlives the test harmlessly; no test reopens it.
    std::mem::forget(path);
    store
}

/// Polls a session's committed stream until `pred` holds (bounded,
/// generous — never tight enough to flake).
async fn wait_for_events(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) {
    for _ in 0..400 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// What the test driver does when it gets the wheel for one session.
/// A session with no entry settles immediately.
#[derive(Clone)]
enum Behavior {
    /// Append a `thread/started` delegating to `worker` with a spawning
    /// topic inside the task's content (what C25's spawner will do),
    /// then settle.
    Dispatch {
        worker: SessionId,
        thread_id: Uuid,
        topic: String,
    },
}

/// Loop driver keyed by session: the router session dispatches a
/// delegation, every other session settles immediately. Test machinery
/// only — the bundle crate itself never sees these types.
struct TestDriver {
    behaviors: Mutex<HashMap<SessionId, Behavior>>,
}

impl LoopDriver for TestDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
        let behavior = self.behaviors.lock().unwrap().get(&ctx.session()).cloned();
        once(async move {
            if let Some(Behavior::Dispatch {
                worker,
                thread_id,
                topic,
            }) = behavior
            {
                let marker = wrap_channel_event(&ChannelEvent {
                    channel: "ci".into(),
                    event_id: format!("spawn-{thread_id}"),
                    topic: Some(topic),
                    sender: None,
                    group: None,
                    payload: json!({"spawn": true}),
                    ts_ms: 0,
                });
                ctx.append(
                    EventKind::ThreadStarted,
                    json!({
                        "thread_id": thread_id.to_string(),
                        "parent": ctx.session().0.to_string(),
                        "worker": worker.0.to_string(),
                        "pinned_version": 1,
                        "task": {"role": "user", "content": marker},
                    }),
                )
                .await
                .expect("append thread/started");
            }
            LoopOutcome::Settled
        })
        .boxed()
    }
}

/// The assembly-style `InboxWriter` over a real engine (what kian-cli
/// builds in C27). Lives in tests: the bundle crate never names the
/// engine (tests/iron_rule.rs).
struct EngineInbox<D>(Arc<Engine<D>>)
where
    D: LoopDriver<Ctx = DrainCtx> + 'static;

impl<D> InboxWriter for EngineInbox<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = Arc::clone(&self.0);
        Box::pin(async move {
            let AdmitOutcome { deduplicated, .. } = engine
                .admit(session, payload, idempotency_key)
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt { deduplicated })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.0.wake(session)
    }
}

/// Store-backed port that *omits* the engine's internal wake: `admit`
/// is the same durable `store.admit` write, and every `wake` is
/// intercepted to prove the just-admitted row was already durable at
/// that moment (invariant 1, observable ordering).
struct OrderingInbox {
    store: EventStore,
    last_admit: Mutex<Option<(SessionId, Value, String)>>,
    wake_saw_durable_row: Mutex<Vec<bool>>,
}

impl OrderingInbox {
    fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            last_admit: Mutex::new(None),
            wake_saw_durable_row: Mutex::new(Vec::new()),
        })
    }

    fn private_admit(
        &self,
        session: SessionId,
        payload: Value,
        key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitOutcome, InboxWriteError>> {
        let store = self.store.clone();
        Box::pin(async move {
            let now = now_ms();
            store
                .admit(InputRow {
                    id: Uuid::new_v4(),
                    session,
                    payload,
                    admitted_at: now,
                    eligible_at: now,
                    idempotency_key: key,
                })
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))
        })
    }
}

impl InboxWriter for OrderingInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let outcome = self.private_admit(session, payload.clone(), idempotency_key.clone());
        *self.last_admit.lock().unwrap() = idempotency_key.map(|k| (session, payload, k));
        Box::pin(async move {
            let AdmitOutcome { deduplicated, .. } = outcome.await?;
            Ok(AdmitReceipt { deduplicated })
        })
    }

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        // At the moment the wake fires, re-admitting the last row must
        // deduplicate against a committed row — the durability proof.
        // The probe runs on its own thread with its own small runtime
        // (a sync trait method cannot await on this thread's runtime).
        let durable = match self.last_admit.lock().unwrap().clone() {
            Some((session, payload, key)) => {
                let store = self.store.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("probe runtime");
                    rt.block_on(store.admit(InputRow {
                        id: Uuid::new_v4(),
                        session,
                        payload,
                        admitted_at: now_ms(),
                        eligible_at: now_ms(),
                        idempotency_key: Some(key),
                    }))
                    .map(|o| o.deduplicated)
                    .unwrap_or(false)
                })
                .join()
                .expect("durability probe thread")
            }
            None => false,
        };
        self.wake_saw_durable_row.lock().unwrap().push(durable);
        WakeOutcome::Folded
    }
}

fn router_rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic("pr-9".into()),
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        },
        SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
    ])
}

// ---------------------------------------------------------------------------
// Log capture (for the unmatched-drop audit line)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MakeShared(SharedWriter);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}

fn install_log_capture() -> Arc<Mutex<Vec<u8>>> {
    static INSTALL: Once = Once::new();
    let buf = Arc::new(Mutex::new(Vec::new()));
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(SharedWriter(Arc::clone(&buf))))
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
    buf
}

fn logs_snapshot(logs: &Arc<Mutex<Vec<u8>>>) -> String {
    let bytes = logs.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// Channel event → rule match → the inbox row is durable **before**
/// the wake fires (invariant 1, observable ordering). The
/// OrderingInbox proves durability at wake time by re-admitting the
/// same idempotency key — a dedup hit means the committed row already
/// existed at that instant.
#[tokio::test]
async fn inbox_row_is_durable_before_wake_fires() {
    let store = temp_store().await;
    let inbox = OrderingInbox::new(store.clone());
    let router = SessionId::new();
    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let outcome = service
        .handle_event(ChannelEvent {
            channel: "mock-im".into(),
            event_id: "evt-ordering".into(),
            topic: None,
            sender: None,
            group: None,
            payload: json!({"text": "hi"}),
            ts_ms: 1,
        })
        .await
        .expect("handled");

    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        }
    );
    let observed = inbox.wake_saw_durable_row.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![true],
        "wake fired before the row was durable"
    );
}

/// Passthrough routes to the worker session, not the Router; the
/// worker's inbox row (and only its stream) carries the channel-event
/// wrapper.
#[tokio::test]
async fn passthrough_routes_to_the_worker_not_the_router() {
    let store = temp_store().await;
    let driver = Arc::new(TestDriver {
        behaviors: Mutex::new(HashMap::new()),
    });
    let engine = Engine::new(store.clone(), Arc::clone(&driver), EngineConfig::default());

    let router = engine.create_session().await.unwrap();
    let worker = SessionId::new();
    driver.behaviors.lock().unwrap().insert(
        router,
        Behavior::Dispatch {
            worker,
            thread_id: Uuid::new_v4(),
            topic: "pr-9".into(),
        },
    );

    // The router drains once and dispatches the delegation
    // (thread/started names the worker and the spawning topic).
    engine
        .admit(router, json!({"role": "user", "content": "dispatch"}), None)
        .await
        .unwrap();
    wait_for_events(
        &store,
        router,
        "thread/started on the router stream",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadStarted),
    )
    .await;

    // Rebuild the passthrough registry from the streams and route a CI
    // event of the worker's own topic.
    let router_events = store.rebuild(router).await.unwrap();
    let worker_events = store.rebuild(worker).await.unwrap();
    let registry = PassthroughRegistry::rebuild(router_events.iter().chain(worker_events.iter()));
    // C38: resolve returns the subscriber set (singleton here).
    assert_eq!(registry.resolve("pr-9"), BTreeSet::from([worker]));

    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    );
    service.install_registry(registry);

    let outcome = service
        .handle_event(ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-ci".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "passed"}),
            ts_ms: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: worker,
            deduplicated: false
        }
    );

    // The worker's stream bootstraps via the C9 first-drain path and
    // consumes the wrapper; the router stream never sees this event.
    wait_for_events(
        &store,
        worker,
        "worker consumed the passthrough input",
        |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
    )
    .await;
    let worker_events = store.rebuild(worker).await.unwrap();
    let input = worker_events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .unwrap();
    let marker = input
        .bytes
        .get("content")
        .and_then(parse_channel_marker)
        .expect("channel-event wrapper in the worker's input");
    assert_eq!(marker.topic.as_deref(), Some("pr-9"));
    assert_eq!(marker.event_id, "evt-ci");

    let router_events = store.rebuild(router).await.unwrap();
    let router_channel_inputs = router_events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .filter(|e| {
            e.bytes
                .get("content")
                .and_then(parse_channel_marker)
                .is_some()
        })
        .count();
    assert_eq!(
        router_channel_inputs, 0,
        "the passthrough event must never reach the Router stream"
    );
}

/// Unmatched events are dropped with an audit log line and no row —
/// nothing durable, no wake.
#[tokio::test]
async fn unmatched_event_dropped_with_audit_log_and_no_row() {
    let logs = install_log_capture();
    let store = temp_store().await;
    let inbox = OrderingInbox::new(store.clone());
    let router = SessionId::new();
    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let outcome = service
        .handle_event(ChannelEvent {
            channel: "github".into(), // no rule for this channel
            event_id: "evt-unmatched".into(),
            topic: None,
            sender: None,
            group: None,
            payload: json!({"text": "irrelevant"}),
            ts_ms: 3,
        })
        .await
        .unwrap();
    assert_eq!(outcome, RoutingOutcome::DroppedNoRule);

    // No inbox row anywhere in the store…
    assert!(
        store.startup_scan(now_ms()).await.unwrap().is_empty(),
        "a dropped event must leave no durable row"
    );
    // …no wake was ever issued…
    assert!(inbox.wake_saw_durable_row.lock().unwrap().is_empty());
    // …no session stream materialized…
    assert!(store.rebuild(router).await.unwrap().is_empty());
    // …and the audit line is observable.
    let text = logs_snapshot(&logs);
    assert!(
        text.contains("subscription audit: event dropped, no matching rule"),
        "expected the unmatched-drop audit line in the log, got:\n{text}"
    );
}

/// Admit is idempotent per `event_id` key — the same event delivered
/// twice yields exactly one durable row; the second handling reports
/// the dedup.
#[tokio::test]
async fn admit_is_idempotent_per_event_id_key() {
    let store = temp_store().await;
    let inbox = OrderingInbox::new(store.clone());
    let router = SessionId::new();
    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let event = ChannelEvent {
        channel: "mock-im".into(),
        event_id: "evt-dup".into(),
        topic: None,
        sender: None,
        group: None,
        payload: json!({"text": "twice"}),
        ts_ms: 4,
    };
    let first = service.handle_event(event.clone()).await.unwrap();
    let second = service.handle_event(event).await.unwrap();
    assert_eq!(
        first,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        }
    );
    assert_eq!(
        second,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: true
        },
        "the second admit must deduplicate against the first row"
    );

    // The store itself holds exactly one row for the key: a third
    // direct admit deduplicates and returns the original row.
    let probe = inbox
        .private_admit(router, json!({"probe": true}), Some("evt-dup".into()))
        .await
        .unwrap();
    assert!(probe.deduplicated);
    assert_eq!(probe.input.idempotency_key.as_deref(), Some("evt-dup"));
}

/// NoLostWork analog: a restart rebuilds the passthrough registry
/// purely from event streams — the worker discovery (thread/started)
/// and the topic binding (channel-input payload) survive because they
/// live in the log, nowhere else.
#[tokio::test]
async fn restart_rebuilds_the_passthrough_registry_from_streams() {
    let store = temp_store().await;
    let driver = Arc::new(TestDriver {
        behaviors: Mutex::new(HashMap::new()),
    });
    let engine = Engine::new(store.clone(), Arc::clone(&driver), EngineConfig::default());

    let router = engine.create_session().await.unwrap();
    let worker = SessionId::new();
    driver.behaviors.lock().unwrap().insert(
        router,
        Behavior::Dispatch {
            worker,
            thread_id: Uuid::new_v4(),
            topic: "pr-9".into(),
        },
    );
    engine
        .admit(router, json!({"role": "user", "content": "dispatch"}), None)
        .await
        .unwrap();
    wait_for_events(
        &store,
        router,
        "thread/started on the router stream",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadStarted),
    )
    .await;

    // The worker serves one passthrough CI event; its stream then
    // carries the wrapper — the durable trace of the binding.
    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    );
    let runtime_registry =
        PassthroughRegistry::rebuild(store.rebuild(router).await.unwrap().iter());
    service.install_registry(runtime_registry);
    service
        .handle_event(ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-rebuild".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "ok"}),
            ts_ms: 5,
        })
        .await
        .unwrap();
    wait_for_events(
        &store,
        worker,
        "worker consumed the passthrough input",
        |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
    )
    .await;

    // "Restart": a brand-new registry, rebuilt from nothing but streams.
    let router_events = store.rebuild(router).await.unwrap();
    let workers = worker_sessions(&router_events);
    assert_eq!(
        workers,
        vec![worker],
        "worker discovery from thread/started"
    );
    let worker_events = store.rebuild(worker).await.unwrap();
    let rebuilt = PassthroughRegistry::rebuild(router_events.iter().chain(worker_events.iter()));
    // C38: resolve returns the subscriber set (singleton here) — the
    // historical record keeps the binding forever.
    assert_eq!(rebuilt.resolve("pr-9"), BTreeSet::from([worker]));
    assert_eq!(rebuilt.topics(), vec!["pr-9".to_string()]);
    // C59 (kian-68): the worker's session settled (its scripted driver
    // returned a terminal outcome, so its stream carries `session/ended`)
    // — the liveness projection reads it as `ended`, and a follow-up on
    // the topic releases to the Router as a fresh evaluation instead of
    // admitting onto the terminal session. (Pre-C59 this fixture relied
    // on ended-subscriber admission; rewritten to the new fallback and
    // named — plan C59's sanctioned-edit clause.)
    assert_eq!(
        rebuilt.status(&worker),
        kian_superagent::WorkerStatus::Ended
    );
    assert!(rebuilt.resolve_live("pr-9").is_empty());

    // The rebuilt registry routes the follow-up to the fresh evaluation.
    // (The release audit line itself is asserted in
    // `tests/followup_ownership.rs`, whose binary owns a sound
    // process-global log capture; this file's per-test buffer fixture
    // is order-parasitic, so this test stays outcome-observable only.)
    let fresh = SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    );
    fresh.install_registry(rebuilt);
    let outcome = fresh
        .handle_event(ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-after-restart".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "still ok"}),
            ts_ms: 6,
        })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        },
        "an all-ended topic releases to the Router (released_to_eval)"
    );
}

/// The resident Router session is bootstrapped by admitting to a
/// configured *not-yet-existing* session id — the C9 first-drain
/// `session/created` path. No orchestrator change is involved: the
/// full pipeline (MockChannel → spawned consumer → rule match →
/// durable admit → wake → first drain) creates the session.
#[tokio::test]
async fn router_bootstraps_via_admit_to_a_not_yet_existing_session() {
    let store = temp_store().await;
    let driver = Arc::new(TestDriver {
        behaviors: Mutex::new(HashMap::new()),
    });
    let engine = Engine::new(store.clone(), driver, EngineConfig::default());

    let router = SessionId::new(); // configured, never created
    assert!(store.rebuild(router).await.unwrap().is_empty());

    let channel = Arc::new(MockChannel::new("mock-im"));
    let service = Arc::new(SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    ));
    let tasks = service.spawn(vec![channel.clone() as Arc<dyn ChannelAdapter>]);
    assert_eq!(tasks.len(), 1);

    let event = channel.inject(None, json!({"text": "hello router"}));
    wait_for_events(
        &store,
        router,
        "session/created + input/admitted on the router stream",
        |events| {
            events.iter().any(|e| e.kind == EventKind::SessionCreated)
                && events.iter().any(|e| e.kind == EventKind::InputAdmitted)
        },
    )
    .await;

    let events = store.rebuild(router).await.unwrap();
    let input = events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .unwrap();
    let marker = input
        .bytes
        .get("content")
        .and_then(parse_channel_marker)
        .expect("wrapper in the bootstrap input");
    assert_eq!(marker.event_id, event.event_id);
    // Creation precedes the consumption write in the same stream.
    let created_at = events
        .iter()
        .position(|e| e.kind == EventKind::SessionCreated)
        .unwrap();
    let input_at = events
        .iter()
        .position(|e| e.kind == EventKind::InputAdmitted)
        .unwrap();
    assert!(created_at < input_at);
    for task in tasks {
        task.abort();
    }
}

/// MockChannel send/receive round trip with receipt, end to end through
/// the subscription consumer.
#[tokio::test]
async fn mock_channel_round_trip_through_the_subscription_pipeline() {
    let store = temp_store().await;
    let driver = Arc::new(TestDriver {
        behaviors: Mutex::new(HashMap::new()),
    });
    let engine = Engine::new(store.clone(), driver, EngineConfig::default());
    let router = SessionId::new();

    let channel = Arc::new(MockChannel::new("mock-im"));
    let service = Arc::new(SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    ));
    let _tasks = service.spawn(vec![channel.clone() as Arc<dyn ChannelAdapter>]);

    // Inbound round trip: inject → consumer admits (durable row, then
    // the first-drain bootstrap consumes it into the stream).
    channel.inject(None, json!({"text": "round trip"}));
    wait_for_events(
        &store,
        router,
        "the injected event reached the router",
        |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
    )
    .await;

    // Outbound round trip: send captures the delivery with a receipt.
    let delivery = ChannelDelivery {
        channel: "mock-im".into(),
        target: json!({"chat_id": 1}),
        content: json!({"text": "done"}),
    };
    let receipt = channel.send(delivery.clone()).await.unwrap();
    let captured = channel.deliveries();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].delivery, delivery);
    assert_eq!(captured[0].receipt, receipt);
    assert!(!receipt.message_id.is_empty());
}

// ---------------------------------------------------------------------------
// C38: many-to-many passthrough fan-out
// ---------------------------------------------------------------------------

/// C38-only driver: appends both delegations on the Router's drain and
/// settles every worker drain. The Phase-2 `TestDriver` stays untouched.
struct FanoutDriver {
    router: SessionId,
    workers: Vec<(SessionId, Uuid)>,
    topic: String,
}

impl LoopDriver for FanoutDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
        let router = self.router;
        let workers = self.workers.clone();
        let topic = self.topic.clone();
        once(async move {
            if ctx.session() == router {
                for (worker, thread_id) in workers {
                    let marker = wrap_channel_event(&ChannelEvent {
                        channel: "ci".into(),
                        event_id: format!("spawn-{thread_id}"),
                        topic: Some(topic.clone()),
                        sender: None,
                        group: None,
                        payload: json!({"spawn": true}),
                        ts_ms: 0,
                    });
                    ctx.append(
                        EventKind::ThreadStarted,
                        json!({
                            "thread_id": thread_id.to_string(),
                            "parent": ctx.session().0.to_string(),
                            "worker": worker.0.to_string(),
                            "pinned_version": 1,
                            "task": {"role": "user", "content": marker},
                        }),
                    )
                    .await
                    .expect("append fan-out thread/started");
                }
            }
            LoopOutcome::Settled
        })
        .boxed()
    }
}

/// An inbox that fails admits for one scripted session until the flag
/// is cleared — the mid-set write-failure shape (test machinery only).
struct FlakyInbox<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    inner: EngineInbox<D>,
    fail_for: Mutex<Option<SessionId>>,
}

impl<D> InboxWriter for FlakyInbox<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        if *self.fail_for.lock().unwrap() == Some(session) {
            return Box::pin(async {
                Err(InboxWriteError::Write("scripted mid-set flake".into()))
            });
        }
        self.inner.admit(session, payload, idempotency_key)
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.inner.wake(session)
    }
}

/// C38 acceptance: one topic, two registered workers — one channel
/// event yields one durable row per subscriber, each row carrying its
/// own provenance wrapper; the rebuild (and a drop-and-replay after
/// consumption) re-derives the same set from streams alone; the Router
/// stream never sees the event. (The audit-log naming of every
/// admission and drop is asserted in `tests/fanout_audit.rs`, which
/// owns its process-global log capture — thread-local `set_default`
/// capture is order-parasitic on whichever test touched the library
/// callsites first and is not used here.)
#[tokio::test]
async fn fanout_one_topic_two_workers_both_get_durable_rows() {
    let store = temp_store().await;
    let router = SessionId::new();
    let worker_a = SessionId::new();
    let worker_b = SessionId::new();
    let driver = Arc::new(FanoutDriver {
        router,
        workers: vec![(worker_a, Uuid::new_v4()), (worker_b, Uuid::new_v4())],
        topic: "pr-9".into(),
    });
    let engine = Engine::new(store.clone(), Arc::clone(&driver), EngineConfig::default());

    // One router drain dispatches delegations onto BOTH workers on
    // topic pr-9 (source 1 of the rebuild: thread/started markers).
    engine
        .admit(
            router,
            json!({"role": "user", "content": "dispatch both"}),
            None,
        )
        .await
        .unwrap();
    wait_for_events(
        &store,
        router,
        "thread/started for both workers",
        |events| {
            events
                .iter()
                .filter(|e| e.kind == EventKind::ThreadStarted)
                .count()
                == 2
        },
    )
    .await;

    // The rebuild derives the two-subscriber set from streams alone.
    let mut all = store.rebuild(router).await.unwrap();
    for worker in [worker_a, worker_b] {
        all.extend(store.rebuild(worker).await.unwrap());
    }
    let registry = PassthroughRegistry::rebuild(&all);
    assert_eq!(
        registry.resolve("pr-9"),
        BTreeSet::from([worker_a, worker_b])
    );

    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    );
    service.install_registry(registry);

    let outcome = service
        .handle_event(ChannelEvent {
            channel: "ci".into(),
            event_id: "evt-fanout".into(),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"build": "fan-out ok"}),
            ts_ms: 7,
        })
        .await
        .unwrap();
    let RoutingOutcome::FannedOut { admitted } = outcome else {
        panic!("expected a fan-out outcome, got {outcome:?}")
    };
    let mut sessions: Vec<SessionId> = admitted.iter().map(|a| a.session).collect();
    sessions.sort();
    let mut expected = vec![worker_a, worker_b];
    expected.sort();
    assert_eq!(sessions, expected);
    assert!(
        admitted.iter().all(|a| !a.deduplicated),
        "first delivery: {admitted:?}"
    );

    // Both workers get durable rows consumed on their own streams,
    // each with its own provenance wrapper naming the same event id.
    for worker in [worker_a, worker_b] {
        wait_for_events(
            &store,
            worker,
            "worker consumed the fan-out input",
            |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
        )
        .await;
        let events = store.rebuild(worker).await.unwrap();
        let input = events
            .iter()
            .find(|e| e.kind == EventKind::InputAdmitted)
            .unwrap();
        let marker = input
            .bytes
            .get("content")
            .and_then(parse_channel_marker)
            .expect("per-row provenance wrapper");
        assert_eq!(marker.event_id, "evt-fanout");
        assert_eq!(marker.topic.as_deref(), Some("pr-9"));
    }

    // The Router stream never sees the fan-out event.
    let router_events = store.rebuild(router).await.unwrap();
    assert_eq!(
        router_events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .filter(|e| {
                e.bytes
                    .get("content")
                    .and_then(parse_channel_marker)
                    .is_some_and(|m| m.event_id == "evt-fanout")
            })
            .count(),
        0,
        "the fan-out event must never reach the Router stream"
    );

    // The audit-log naming of both admissions lives in
    // `tests/fanout_audit.rs` (own process, own global capture).

    // Drop-and-replay equivalence: a fresh rebuild over the streams
    // that now ALSO carry both workers' channel inputs (source 2)
    // re-derives exactly the same set.
    let mut replay_source = store.rebuild(router).await.unwrap();
    for worker in [worker_a, worker_b] {
        replay_source.extend(store.rebuild(worker).await.unwrap());
    }
    let replayed = PassthroughRegistry::rebuild(&replay_source);
    assert_eq!(
        replayed.resolve("pr-9"),
        BTreeSet::from([worker_a, worker_b])
    );
}

/// C38 retry semantics against the real store: one subscriber's admit
/// fails mid-set — the other's row is durable and consumed anyway; the
/// idempotent retry fills only the gap (failed subscriber gets its
/// row, the successful one deduplicates); each stream ends with
/// exactly one wrapper for the event.
#[tokio::test]
async fn fanout_partial_write_failure_is_retried_per_subscriber() {
    let store = temp_store().await;
    let driver = Arc::new(TestDriver {
        behaviors: Mutex::new(HashMap::new()),
    });
    let engine = Engine::new(store.clone(), Arc::clone(&driver), EngineConfig::default());
    let router = engine.create_session().await.unwrap();
    let worker_a = SessionId::new();
    let worker_b = SessionId::new();

    let inbox = Arc::new(FlakyInbox {
        inner: EngineInbox(Arc::clone(&engine)),
        fail_for: Mutex::new(Some(worker_a)),
    });
    let service = SubscriptionService::new(
        router,
        router_rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );
    service.register_topic("pr-9", worker_a);
    service.register_topic("pr-9", worker_b);

    let event = ChannelEvent {
        channel: "ci".into(),
        event_id: "evt-partial-fanout".into(),
        topic: Some("pr-9".into()),
        sender: None,
        group: None,
        payload: json!({"build": "flake"}),
        ts_ms: 8,
    };
    let err = service.handle_event(event.clone()).await.unwrap_err();
    assert!(matches!(err, InboxWriteError::Write(_)));

    // B's row was admitted and consumed despite A's failure; A's
    // stream never materialized (no row, no drain, no wrapper).
    wait_for_events(
        &store,
        worker_b,
        "worker b consumed its row despite a's failure",
        |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
    )
    .await;
    assert!(
        store.rebuild(worker_a).await.unwrap().is_empty(),
        "the failed subscriber must hold no row before the retry"
    );

    // The adapter's retry (same event id): A's gap is filled, B
    // deduplicates against its committed row.
    *inbox.fail_for.lock().unwrap() = None;
    let outcome = service.handle_event(event).await.unwrap();
    let RoutingOutcome::FannedOut { admitted } = outcome else {
        panic!("expected a fan-out outcome, got {outcome:?}")
    };
    let find = |s: SessionId| {
        admitted
            .iter()
            .find(|a| a.session == s)
            .unwrap_or_else(|| panic!("missing admission for {s:?} in {admitted:?}"))
    };
    assert!(!find(worker_a).deduplicated);
    assert!(find(worker_b).deduplicated);

    wait_for_events(
        &store,
        worker_a,
        "worker a consumed its row after the retry",
        |events| events.iter().any(|e| e.kind == EventKind::InputAdmitted),
    )
    .await;

    // Exactly one wrapper per worker stream, both naming the event.
    for worker in [worker_a, worker_b] {
        let events = store.rebuild(worker).await.unwrap();
        let wrappers: Vec<_> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .filter(|e| {
                e.bytes
                    .get("content")
                    .and_then(parse_channel_marker)
                    .is_some_and(|m| m.event_id == "evt-partial-fanout")
            })
            .collect();
        assert_eq!(wrappers.len(), 1, "exactly one row per subscriber");
    }
}
