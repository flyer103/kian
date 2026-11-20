//! Integration tests: policy/prompt versioning + drift
//! marking, over a real sqlite `EventStore` and a real `Engine` — the
//! same fixture family as `router_profile.rs`. ScriptedProvider, no
//! credentials, no network egress.
//!
//! The four named gates:
//! 1. The `[policy <label>]` header is rendered at bootstrap and
//!    persisted (byte golden of a new-session bootstrap input);
//! 2. The same session replayed after a config version change keeps
//!    its original header byte-identically (live sessions are never
//!    rewritten — cache-byte stability);
//! 3. drift detection marks exactly the affected sessions;
//! 4. `notify_live: true` admits ONE marker input per affected session
//!    (idempotent by a stable event id), `false` admits none;
//!    plus: absent config ⇒ bootstrap bytes identical to today.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{InputRow, SessionId};
use kian_core::seams::{
    ModelChunk, ModelProvider, ModelRequest, PolicyRule, RecoveryPolicySource, ToolCall, ToolError,
    ToolProvider, ToolResult, ToolSchema,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::{ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, SystemClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, JUDGE_PROMPT_TEMPLATE, JudgePrompt, PolicyVersion,
    RouterDriver, RouterHold, RouterInbox, RouterProfile, WATCHDOG_BOOTSTRAP_KEY,
    WATCHDOG_PROMPT_TEMPLATE, WatchdogProfile, parse_policy_header, run_drift_marking, scan_drift,
    worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-c63-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
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
) -> Vec<Event> {
    for _ in 0..800 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn text_chunk(text: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

/// A `ModelProvider` replaying scripted turns in order (plenty of
/// plain answers — content never matters here).
struct ScriptedProvider {
    turns: Mutex<VecDeque<Vec<Result<ModelChunk, kian_core::seams::ProviderError>>>>,
}

impl ScriptedProvider {
    fn answers(n: usize) -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::new((0..n).map(|_| vec![text_chunk("ok")]).collect()),
        })
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        futures::stream::iter(turn).boxed()
    }
}

struct FakeTools;

impl ToolProvider for FakeTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "noop".into(),
            description: "test tool".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            Ok(ToolResult {
                call_id: call.id,
                output: json!({"ok": true}),
                is_error: false,
                redactions: Vec::new(),
            })
        })
    }
}

struct NoopPolicy;
impl RecoveryPolicySource for NoopPolicy {
    fn table(&self) -> Vec<PolicyRule> {
        Vec::new()
    }
}

/// The engine-backed inbox (the cycle-breaker shape): admits are
/// durable store writes with real `(session, idempotency_key)`
/// dedupe; wakes reach the engine through a late-bound slot.
struct LateEngineInbox {
    store: EventStore,
    clock: Arc<dyn Clock>,
    engine: OnceLock<Arc<Engine<SuperAgentDriver>>>,
}

impl InboxWriter for LateEngineInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let store = self.store.clone();
        let now = self.clock.now_ms();
        Box::pin(async move {
            let outcome = store
                .admit(InputRow {
                    id: Uuid::new_v4(),
                    session,
                    payload,
                    admitted_at: now,
                    eligible_at: now,
                    idempotency_key,
                })
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.engine
            .get()
            .expect("engine installed before first wake")
            .wake(session)
    }
}

/// Assembly's `StreamPort` over the execution-hold handle (shape).
struct CtxPort(DrainCtx);

impl kian_superagent::StreamPort for CtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), kian_superagent::StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|e| kian_superagent::StreamPortError(e.to_string()))
        })
    }

    fn read_stream(
        &self,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.events()
                .await
                .map_err(|e| kian_superagent::StreamPortError(e.to_string()))
        })
    }
}

fn port_for_ctx() -> kian_superagent::PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| {
        Arc::new(CtxPort(ctx.clone())) as Arc<dyn kian_superagent::StreamPort>
    })
}

/// One engine, one driver: the resident Router session routes to the
/// `RouterDriver` (residency parking — the session survives settling);
/// every other session routes to the plain loop.
struct SuperAgentDriver {
    router: SessionId,
    router_side: RouterDriver<ReActLoop>,
    other_side: ReActLoop,
}

impl kian_core::seams::LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, kian_core::seams::LoopOutcome> {
        if ctx.session() == self.router {
            self.router_side.drive(ctx)
        } else {
            self.other_side.drive(ctx)
        }
    }
}

struct Harness {
    store: EventStore,
    engine: Arc<Engine<SuperAgentDriver>>,
    inbox: Arc<LateEngineInbox>,
    router_inbox: Arc<RouterInbox>,
    router: SessionId,
}

async fn harness() -> Harness {
    let store = temp_store().await;
    let router = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let agent = worker_template("mock", "mock-worker", 7);
    let make_loop = || {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "mock",
            ScriptedProvider::answers(64) as Arc<dyn ModelProvider>,
        );
        Arc::new(ReActLoop::with_config(
            agent.clone(),
            Arc::new(registry),
            Arc::new(FakeTools),
            Arc::new(NoopPolicy),
            ReActConfig::default(),
        ))
    };
    let inbox = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    // The mounted inbox: admits counted for Router residency parking.
    let router_inbox = RouterInbox::new(
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        kian_superagent::ResidentBinding::new(router),
    );
    let router_side = RouterDriver::with_park_tick(
        make_loop(),
        RouterHold::shared(),
        port_for_ctx(),
        router_inbox.clone(),
        Duration::from_millis(10),
    );
    let engine = Engine::with_clock(
        store.clone(),
        Arc::new(SuperAgentDriver {
            router,
            router_side,
            other_side: make_loop().as_ref().clone(),
        }),
        EngineConfig::default(),
        clock,
    );
    assert!(inbox.engine.set(Arc::clone(&engine)).is_ok());
    Harness {
        store,
        engine,
        inbox,
        router_inbox,
        router,
    }
}

/// Bootstraps `session` under policy label `label` (None = today's
/// bytes), wakes it, and waits for the bootstrap input to be consumed
/// into the committed stream. Returns the stream. The resident Router
/// session bootstraps through the mounted (residency-counted) inbox;
/// other sessions through the raw engine inbox.
async fn bootstrap_and_wait(h: &Harness, session: SessionId, label: Option<&str>) -> Vec<Event> {
    let mut profile = RouterProfile::new(session, worker_template("mock", "mock-router", 7));
    profile.policy_version = label.map(PolicyVersion::new);
    let inbox: Arc<dyn InboxWriter> = if session == h.router {
        h.router_inbox.clone()
    } else {
        h.inbox.clone()
    };
    profile.bootstrap(inbox.as_ref()).await.expect("bootstrap");
    h.engine.wake(session);
    wait_for_events(&h.store, session, "bootstrap consumed", |events| {
        events.iter().any(|e| e.kind == EventKind::InputAdmitted)
    })
    .await
}

/// The first `input/admitted` payload's content of a stream (the
/// persisted bootstrap prompt — the consumption write wraps payloads
/// as `{"role":"user","content":…}`).
fn bootstrap_prompt(events: &[Event]) -> &str {
    events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .and_then(|e| e.bytes.get("content"))
        .and_then(|c| c.as_str())
        .expect("bootstrap prompt persisted")
}

// ---------------------------------------------------------------------------
// Gate 1 — header rendered at bootstrap and persisted (byte golden)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn header_rendered_at_bootstrap_and_persisted_byte_golden() {
    let h = harness().await;
    let router = h.router;
    let events = bootstrap_and_wait(&h, router, Some("2026-11")).await;

    // Byte golden: the persisted bootstrap input is EXACTLY the
    // header over the default judge template rendered with 0 inputs —
    // constructed independently of the render path under test.
    let golden = format!(
        "[policy 2026-11] {}",
        JUDGE_PROMPT_TEMPLATE.replace("{inputs}", "0")
    );
    assert_eq!(bootstrap_prompt(&events), golden);

    // The header parses back (the drift scan's read surface) and the
    // template text underneath is byte-identical (goldens stay valid).
    assert_eq!(
        parse_policy_header(bootstrap_prompt(&events)),
        Some("2026-11")
    );
    assert_eq!(
        bootstrap_prompt(&events).strip_prefix("[policy 2026-11] "),
        Some(JUDGE_PROMPT_TEMPLATE.replace("{inputs}", "0").as_str())
    );
}

#[tokio::test]
async fn watchdog_bootstrap_header_byte_golden() {
    let h = harness().await;
    let watchdog = SessionId::new();
    let profile = {
        let mut p = WatchdogProfile::new(watchdog, worker_template("mock", "mock-watchdog", 7));
        p.policy_version = Some(PolicyVersion::new("2026-11"));
        p
    };
    // Bootstrap admits (no wake needed to inspect the durable row's
    // payload through the admit path; the router test above proves
    // persistence).
    let inbox: Arc<dyn InboxWriter> = h.inbox.clone();
    let receipt = profile.bootstrap(inbox.as_ref()).await.expect("bootstrap");
    assert!(!receipt.deduplicated);
    // The durable row is in the store's inbox; its payload is the
    // byte-golden header over the watchdog template.
    let rows = h
        .store
        .eligible_inputs(watchdog, (i64::MAX as u64) - 1)
        .await
        .expect("eligible inputs");
    let row = rows
        .iter()
        .find(|r| r.idempotency_key.as_deref() == Some(WATCHDOG_BOOTSTRAP_KEY))
        .expect("bootstrap row");
    assert_eq!(
        row.payload,
        json!(format!("[policy 2026-11] {WATCHDOG_PROMPT_TEMPLATE}"))
    );
}

// ---------------------------------------------------------------------------
// Gate 2 — replay after a config version change keeps the original header
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replayed_session_after_version_change_keeps_its_original_header() {
    let h = harness().await;
    let router = h.router;
    let events = bootstrap_and_wait(&h, router, Some("v1")).await;

    // The operator bumps the config: v1 → v2. The same live session
    // re-bootstraps (assembly calls bootstrap at every startup).
    let mut upgraded = RouterProfile::new(router, worker_template("mock", "mock-router", 7));
    upgraded.policy_version = Some(PolicyVersion::new("v2"));
    let receipt = upgraded
        .bootstrap(h.inbox.as_ref() as &dyn InboxWriter)
        .await
        .expect("re-bootstrap");
    assert!(
        receipt.deduplicated,
        "the idempotent bootstrap key dedupes — the persisted prompt stands"
    );

    // The live session's stream is byte-identical: the v1 header is
    // never rewritten (cache-byte stability); only a FRESH session's
    // bootstrap would render v2.
    let replayed = h.store.rebuild(router).await.expect("rebuild");
    let original_prompt = bootstrap_prompt(&events).to_string();
    assert_eq!(
        bootstrap_prompt(&replayed),
        original_prompt,
        "the drifted session keeps its original header byte-identically"
    );
    let bootstrap_payload_count = replayed
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .filter(|e| {
            e.bytes.get("content").and_then(|c| c.as_str()) == Some(original_prompt.as_str())
        })
        .count();
    assert_eq!(bootstrap_payload_count, 1, "exactly one bootstrap input");
}

// ---------------------------------------------------------------------------
// Gate 3 — drift detection marks exactly the affected sessions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drift_detection_marks_exactly_the_affected_sessions() {
    let h = harness().await;
    let drifted = h.router;
    let aligned = SessionId::new();
    let fresh = SessionId::new(); // never bootstrapped
    let drifted_stream = bootstrap_and_wait(&h, drifted, Some("v1")).await;
    let aligned_stream = bootstrap_and_wait(&h, aligned, Some("v2")).await;

    // Config is now v2: only the v1 session drifted.
    let marked = scan_drift(
        vec![
            (drifted, drifted_stream.as_slice()),
            (aligned, aligned_stream.as_slice()),
            (fresh, [].as_slice()),
        ],
        Some("v2"),
    );
    assert_eq!(marked.len(), 1, "exactly the drifted session");
    assert_eq!(marked[0].0, drifted);
    assert_eq!(marked[0].1.persisted_label(), "v1");
    assert_eq!(marked[0].1.current_label(), "v2");

    // Unsetting the version is a drift too (the honest both-direction
    // rule); the aligned session is still not marked.
    let marked = scan_drift(
        vec![
            (drifted, drifted_stream.as_slice()),
            (aligned, aligned_stream.as_slice()),
        ],
        None,
    );
    assert_eq!(marked.len(), 2, "v1 and v2 persisted vs unversioned config");
}

// ---------------------------------------------------------------------------
// Gate 4 — notify_live: one idempotent marker per affected session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn notify_live_true_admits_one_marker_per_affected_session_idempotently() {
    let h = harness().await;
    let router = h.router;
    let stream = bootstrap_and_wait(&h, router, Some("v1")).await;

    // Boot with config v2 and notify_live: true. The marker admits
    // through the resident's MOUNTED inbox (the parked Router polls its
    // residency counter — the wiring assembly provides).
    let mounted = |session: SessionId| -> Arc<dyn InboxWriter> {
        if session == h.router {
            h.router_inbox.clone()
        } else {
            h.inbox.clone()
        }
    };
    let marking = run_drift_marking(vec![(router, stream.as_slice())], Some("v2"), true, mounted)
        .await
        .expect("drift marking");
    assert_eq!(marking.marked.len(), 1);
    assert_eq!(marking.notified.len(), 1, "one marker per affected session");
    assert!(!marking.notified[0].1, "the first delivery is fresh");

    // The marker lands as an ordinary admitted input — synthetic-marked,
    // strippable, replayable from the stream (no kind). (The explicit
    // wake mirrors assembly's post-admit wake; the notify's own wake is
    // advisory and may fold into the still-running bootstrap turn.)
    h.engine.wake(router);
    let events = wait_for_events(&h.store, router, "marker input consumed", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes
                    .get("content")
                    .and_then(|c| c.as_str())
                    .is_some_and(|c| c.starts_with("[policy v1→v2] defaults updated;"))
        })
    })
    .await;
    let marker = events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .filter_map(|e| e.bytes.get("content").and_then(|c| c.as_str()))
        .filter(|c| c.starts_with("[policy v1→v2] defaults updated;"))
        .count();
    assert_eq!(marker, 1, "exactly one marker input");

    // Re-boot: the stable event id dedupes onto the same row.
    let replayed = h.store.rebuild(router).await.expect("rebuild");
    let again = run_drift_marking(
        vec![(router, replayed.as_slice())],
        Some("v2"),
        true,
        mounted,
    )
    .await
    .expect("re-run");
    assert_eq!(again.marked.len(), 1);
    assert_eq!(again.notified.len(), 1);
    assert!(
        again.notified[0].1,
        "the re-boot re-derives the same stable event id — deduplicated"
    );

    // Still exactly one marker input in the stream.
    let settled = wait_for_events(&h.store, router, "stream settled", |events| {
        events.len() >= replayed.len()
    })
    .await;
    let marker = settled
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .filter_map(|e| e.bytes.get("content").and_then(|c| c.as_str()))
        .filter(|c| c.starts_with("[policy v1→v2] defaults updated;"))
        .count();
    assert_eq!(marker, 1, "idempotent: still exactly one marker input");
}

#[tokio::test]
async fn notify_live_false_marks_but_admits_none() {
    let h = harness().await;
    let router = h.router;
    let stream = bootstrap_and_wait(&h, router, Some("v1")).await;
    let before = stream.len();

    let marking = run_drift_marking(
        vec![(router, stream.as_slice())],
        Some("v2"),
        false, // the default: notice off
        |session| -> Arc<dyn InboxWriter> {
            if session == h.router {
                h.router_inbox.clone()
            } else {
                h.inbox.clone()
            }
        },
    )
    .await
    .expect("drift marking");
    assert_eq!(marking.marked.len(), 1, "the drift is still marked");
    assert!(
        marking.notified.is_empty(),
        "notify_live: false admits no marker input"
    );

    // Nothing was admitted: even after an explicit wake and a settle
    // window, no policy marker ever reaches the stream.
    h.engine.wake(router);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let events = h.store.rebuild(router).await.expect("rebuild");
    assert!(
        events.len() <= before,
        "no new inputs: {:?}",
        &events[before..]
    );
    assert!(
        !events.iter().any(|e| {
            e.bytes
                .get("content")
                .and_then(|c| c.as_str())
                .is_some_and(|c| c.contains("defaults updated"))
        }),
        "no policy-changed marker admitted"
    );
}

// ---------------------------------------------------------------------------
// Absent config ⇒ today's bytes (golden diff empty)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn absent_config_bootstrap_bytes_are_identical_to_today() {
    let h = harness().await;
    let router = h.router;
    let events = bootstrap_and_wait(&h, router, None).await;
    // Exactly what the legacy tests asserted (and still assert,
    // unmodified): the plain judge render, nothing added.
    assert_eq!(
        bootstrap_prompt(&events),
        JudgePrompt::default().render(0),
        "absent config renders today's bytes — the golden diff is empty"
    );
    assert!(!bootstrap_prompt(&events).contains("[policy "));
}
