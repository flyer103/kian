//! Integration tests (card C23): the subscription framework against a
//! real sqlite `EventStore` and a real `Engine` — through the
//! `InboxWriter` port only, exactly the way assembly (C27) will wire
//! it. No credentials, no network; everything local.

use std::collections::HashMap;
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
    assert_eq!(registry.resolve("pr-9"), Some(worker));

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
    let rebuilt = PassthroughRegistry::rebuild(
        router_events
            .iter()
            .chain(store.rebuild(worker).await.unwrap().iter()),
    );
    assert_eq!(rebuilt.resolve("pr-9"), Some(worker));
    assert_eq!(rebuilt.topics(), vec!["pr-9".to_string()]);

    // The rebuilt registry routes exactly like the pre-restart one.
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
            payload: json!({"build": "still ok"}),
            ts_ms: 6,
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
