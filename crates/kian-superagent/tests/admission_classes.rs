//! Integration tests (card C36): the admission-class Fast Path — the
//! provenance-keyed filter evaluated **before** the rule set and before
//! any model call — against a real sqlite `EventStore` and a real
//! `Engine`, through the `InboxWriter` port exactly the way assembly
//! wires it. No credentials, no network; everything local.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{StreamExt, once};
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome, ModelProvider};
use kian_core::seams::{ModelChunk, ModelRequest};
use kian_orchestrator::{DrainCtx, Engine, EngineConfig, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, AdmitReceipt, InboxWriteError,
    InboxWriter, Route, RoutingOutcome, RuleMatcher, SubscriptionRule, SubscriptionRuleSet,
    SubscriptionService,
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
    let path = std::env::temp_dir().join(format!("kian-c36-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    std::mem::forget(path);
    store
}

/// Rules that would route everything the tests drop: `mock-im` always
/// to the Router, `ci`/`pr-9` passthrough. A dropped-by-class event
/// must never reach any of them.
fn eager_rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic("pr-9".into()),
            route: Route::Passthrough {
                correlate_by: kian_superagent::CorrelateBy::Topic,
            },
        },
        SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
        SubscriptionRule {
            channel: "lark".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
    ])
}

fn class(channel: &str, class: AdmissionClass) -> AdmissionClassTable {
    AdmissionClassTable::new(vec![AdmissionClassRule {
        channel: channel.into(),
        topic: None,
        sender: None,
        class,
    }])
}

/// Scripted port: records every admit (session, payload, key) and wake
/// so drops are observable as the absence of records.
struct ScriptedInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl ScriptedInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for ScriptedInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async move {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

fn event(channel: &str, event_id: &str, text: &str) -> kian_core::seams::ChannelEvent {
    kian_core::seams::ChannelEvent {
        channel: channel.into(),
        event_id: event_id.into(),
        topic: None,
        sender: Some(json!({"id": "u-1"})),
        group: None,
        payload: json!({"text": text}),
        ts_ms: 1,
    }
}

// ---------------------------------------------------------------------------
// Log capture (audit lines) — one shared buffer per test binary
// ---------------------------------------------------------------------------

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

struct MakeShared(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        SharedWriter(Arc::clone(&self.0))
    }
}

fn logs() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    BUF.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(Arc::clone(&buf)))
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        buf
    })
    .clone()
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// `Ignore` event: no inbox row (no admit at all), no wake, an audit
/// line — the `DroppedNoRule` shape, decided before the rule set.
#[tokio::test]
async fn ignore_event_no_row_no_wake_audit_line() {
    let logbuf = logs();
    let inbox = ScriptedInbox::new();
    let router = SessionId::new();
    let service = SubscriptionService::with_classes(
        router,
        eager_rules(),
        class("mock-im", AdmissionClass::Ignore),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let outcome = service
        .handle_event(event("mock-im", "evt-ign", "would have routed"))
        .await
        .unwrap();
    assert_eq!(outcome, RoutingOutcome::DroppedIgnored);
    assert!(inbox.admits.lock().unwrap().is_empty(), "no row, ever");
    assert!(inbox.wakes.lock().unwrap().is_empty(), "no wake, ever");

    let text = String::from_utf8(logbuf.lock().unwrap().clone()).unwrap();
    assert!(
        text.contains("dropped by admission class `ignore`"),
        "expected the ignore audit line, got:\n{text}"
    );
}

/// `ExplicitOnly` + a non-mention event: dropped with an audit line,
/// no row, no wake.
#[tokio::test]
async fn explicit_only_drops_non_mention_event_with_audit() {
    let logbuf = logs();
    let inbox = ScriptedInbox::new();
    let router = SessionId::new();
    let service = SubscriptionService::with_classes(
        router,
        eager_rules(),
        class("lark", AdmissionClass::ExplicitOnly),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let outcome = service
        .handle_event(event("lark", "evt-quiet", "just chatting, no mention"))
        .await
        .unwrap();
    assert_eq!(outcome, RoutingOutcome::DroppedNotExplicit);
    assert!(inbox.admits.lock().unwrap().is_empty());
    assert!(inbox.wakes.lock().unwrap().is_empty());

    let text = String::from_utf8(logbuf.lock().unwrap().clone()).unwrap();
    assert!(
        text.contains("dropped by admission class `explicit-only`"),
        "expected the explicit-only audit line, got:\n{text}"
    );
}

/// `ExplicitOnly` + an @-mention: admitted through the normal rule-set
/// path (durable row, then wake) — the mention is the explicit signal.
#[tokio::test]
async fn explicit_only_admits_mention_event() {
    let inbox = ScriptedInbox::new();
    let router = SessionId::new();
    let service = SubscriptionService::with_classes(
        router,
        eager_rules(),
        class("lark", AdmissionClass::ExplicitOnly),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let outcome = service
        .handle_event(event("lark", "evt-mention", "hey @kian please deploy"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(admits[0].0, router);
    assert_eq!(admits[0].2.as_deref(), Some("evt-mention"));
    assert_eq!(*inbox.wakes.lock().unwrap(), vec![router]);
}

/// `ExplicitOnly` + the adapter-declared `explicit: true` payload
/// field: admitted (the second explicit-request signal).
#[tokio::test]
async fn explicit_only_admits_adapter_declared_explicit_payload() {
    let inbox = ScriptedInbox::new();
    let router = SessionId::new();
    let service = SubscriptionService::with_classes(
        router,
        eager_rules(),
        class("lark", AdmissionClass::ExplicitOnly),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );

    let mut ev = event(
        "lark",
        "evt-declared",
        "structured payload, no text mention",
    );
    ev.payload = json!({"explicit": true, "state": "failed"});
    let outcome = service.handle_event(ev).await.unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );
    assert_eq!(inbox.admits.lock().unwrap().len(), 1);
}

/// `Candidate` flows to the rule set exactly as a classless config
/// does: same outcome, and the admitted payload bytes are identical.
#[tokio::test]
async fn candidate_rule_set_path_is_byte_identical_to_classless_config() {
    let router = SessionId::new();

    let classless_inbox = ScriptedInbox::new();
    let classless = SubscriptionService::new(
        router,
        eager_rules(),
        Arc::clone(&classless_inbox) as Arc<dyn InboxWriter>,
    );

    let candidate_inbox = ScriptedInbox::new();
    let candidate = SubscriptionService::with_classes(
        router,
        eager_rules(),
        class("mock-im", AdmissionClass::Candidate),
        Arc::clone(&candidate_inbox) as Arc<dyn InboxWriter>,
    );

    let a = classless
        .handle_event(event("mock-im", "evt-same", "hello"))
        .await
        .unwrap();
    let b = candidate
        .handle_event(event("mock-im", "evt-same", "hello"))
        .await
        .unwrap();
    assert_eq!(a, b);

    let (classless_admits, candidate_admits) = (
        classless_inbox.admits.lock().unwrap().clone(),
        candidate_inbox.admits.lock().unwrap().clone(),
    );
    assert_eq!(classless_admits.len(), 1);
    assert_eq!(candidate_admits.len(), 1);
    assert_eq!(
        serde_json::to_string(&classless_admits[0].1).unwrap(),
        serde_json::to_string(&candidate_admits[0].1).unwrap(),
        "the admitted payload bytes must be identical (compat)"
    );
    assert_eq!(*classless_inbox.wakes.lock().unwrap(), vec![router]);
    assert_eq!(*candidate_inbox.wakes.lock().unwrap(), vec![router]);
}

// ---------------------------------------------------------------------------
// The zero-provider-calls proof: a real Engine whose driver spends a
// model request on every judge turn.
// ---------------------------------------------------------------------------

/// A `ModelProvider` that records every request (the S4 ScriptedProvider
/// stand-in, both sides — no credentials).
struct ScriptedProvider {
    call_count: std::sync::atomic::AtomicUsize,
}

impl ScriptedProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            call_count: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> futures::stream::BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>>
    {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        once(async move {
            Ok(ModelChunk {
                delta: json!({"content": "ok"}),
            })
        })
        .boxed()
    }
}

/// A driver whose every drain is one judge turn: it spends exactly one
/// provider request, then settles. A drain happens only on a wake, and
/// a wake happens only after a durable admit — so dropped-by-class
/// events must leave the request count at zero.
struct JudgeDriver {
    provider: Arc<ScriptedProvider>,
}

impl LoopDriver for JudgeDriver {
    type Ctx = DrainCtx;

    fn drive(&self, _ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
        let provider = Arc::clone(&self.provider);
        once(async move {
            let mut stream = provider.complete(ModelRequest {
                slot: kian_core::model::SlotName("router".into()),
                body: json!({"messages": []}),
            });
            while (stream.next().await).is_some() {}
            LoopOutcome::Settled
        })
        .boxed()
    }
}

/// The assembly-style inbox over a real engine (the LateEngineInbox
/// shape): durable store admit, then the engine's wake.
struct EngineInbox<A: LoopDriver<Ctx = DrainCtx> + 'static>(Arc<Engine<A>>);

impl<A> InboxWriter for EngineInbox<A>
where
    A: LoopDriver<Ctx = DrainCtx> + 'static,
{
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = Arc::clone(&self.0);
        Box::pin(async move {
            let outcome = engine
                .admit(session, payload, idempotency_key)
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.0.wake(session)
    }
}

async fn wait_for_input(store: &EventStore, session: SessionId, desc: &str) {
    for _ in 0..400 {
        let events = store.rebuild(session).await.expect("rebuild");
        if events
            .iter()
            .any(|e| e.kind == kian_core::events::EventKind::InputAdmitted)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// Classes evaluate before any judge turn: `ignore` and
/// `explicit-only`-without-signal events — both carrying rules that
/// would route them to the Router — leave the ScriptedProvider at zero
/// requests; the control (an admitted mention) spends one.
#[tokio::test]
async fn classes_evaluate_before_any_judge_turn_zero_provider_calls() {
    let store = temp_store().await;
    let provider = ScriptedProvider::new();
    let engine = Engine::new(
        store.clone(),
        Arc::new(JudgeDriver {
            provider: Arc::clone(&provider),
        }),
        EngineConfig::default(),
    );
    let router = SessionId::new();

    let mut classes = AdmissionClassTable::new(vec![
        AdmissionClassRule {
            channel: "mock-im".into(),
            topic: None,
            sender: None,
            class: AdmissionClass::Ignore,
        },
        AdmissionClassRule {
            channel: "lark".into(),
            topic: None,
            sender: None,
            class: AdmissionClass::ExplicitOnly,
        },
    ]);
    // A sender-keyed Candidate carve-out so the control event admits.
    classes.0.push(AdmissionClassRule {
        channel: "lark".into(),
        topic: None,
        sender: Some(json!({"id": "u-2"})),
        class: AdmissionClass::Candidate,
    });

    let service = SubscriptionService::with_classes(
        router,
        eager_rules(),
        classes,
        Arc::new(EngineInbox(Arc::clone(&engine))) as Arc<dyn InboxWriter>,
    );

    // Dropped events: no row, no wake — hence no drain, no judge turn,
    // zero provider calls.
    let ignored = service
        .handle_event(event(
            "mock-im",
            "evt-judge-1",
            "@kian mention would still drop",
        ))
        .await
        .unwrap();
    assert_eq!(ignored, RoutingOutcome::DroppedIgnored);
    let not_explicit = service
        .handle_event(event("lark", "evt-judge-2", "no signal here"))
        .await
        .unwrap();
    assert_eq!(not_explicit, RoutingOutcome::DroppedNotExplicit);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        provider.request_count(),
        0,
        "dropped events must spend zero provider calls"
    );
    assert!(
        store.startup_scan(now_ms()).await.unwrap().is_empty(),
        "dropped events must leave no durable row"
    );

    // Control: an admitted event (sender carve-out → Candidate → rule
    // match → Router) does spend a judge turn.
    let mut admitted = event("lark", "evt-judge-3", "hello");
    admitted.sender = Some(json!({"id": "u-2"}));
    let outcome = service.handle_event(admitted).await.unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );
    wait_for_input(&store, router, "the admitted control event consumed").await;
    assert!(
        provider.request_count() >= 1,
        "the control event must reach a judge turn"
    );
}
