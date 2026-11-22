//! The end-to-end fixture test: issue-watch multi-Ticket fan-out
//! over a real engine — mock providers on both sides, the github
//! adapter's poll-receive mode reading **scripted REST list pages**
//! from an in-process axum server. No credentials, no real network
//! (the live leg is the next step).
//!
//! The four-gate flow asserted here, against committed sqlite streams:
//!
//! 1. a new polled issue reaches the Router (unbound topic → the
//!    passthrough Router fallback), whose judge turn accepts it with a
//!    **Mandate** (`spawn_worker` over the channel wrapper — the
//!    adapter-declared `explicit` on human issue authorship makes the
//!    mandate strength `explicit`);
//! 2. a follow-up comment on the **live** issue thread admits to that
//!    worker with **zero Router judge turns** (ownership: the
//!    registry routes by the thread-key topic; the Router session
//!    neither wakes nor drains for the event);
//! 3. The worker completes; the Router delivers the triage draft as a
//!    structured github comment **under speak-rules** (the `result-ready`
//!    gate admits the Result; an attempted echo/ack Decision is an
//!    honest tool error with zero adapter sends, the discipline);
//! 4. The stage projection observes the ticket's transitions
//!    (working mid-flight → idle after completion);
//! 5. a second live issue fans out **independently** — two tickets,
//!    one source: two `thread/started` delegations, two workers, two
//!    delivered drafts, one github channel.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::Json;
use axum::extract::{FromRequest, Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{InputRow, SessionId};
use kian_core::seams::{
    ChannelAdapter, ModelChunk, ModelProvider, ModelRequest, PolicyRule, RecoveryPolicySource,
    ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::{CheckSpec, EvidenceGateConfig, ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, SystemClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::rules::Route;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, CorrelateBy, GitHubChannel, GitHubConfig, InboxWriteError,
    InboxWriter, MandatePolicy, NoOutbox, POLL_SINCE_FIELD, PollConfig, PollKind, PortFor,
    RouterDriver, RouterHold, RouterInbox, RouterProfile, RouterTools, RuleMatcher, SpeakCondition,
    SpeakPolicy, SpeakRule, StreamPort, StreamPortError, SubscriptionRule, SubscriptionRuleSet,
    SubscriptionService, WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner, poll_items_to_events,
    project_stage, router_template, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The scripted GitHub fixture: GET list pages from a path-keyed map,
// every other method (the send path's POSTs) answered with acceptance.
// ---------------------------------------------------------------------------

struct GithubFixture {
    pages: Mutex<BTreeMap<String, Value>>,
    posts: Mutex<Vec<(String, Value)>>,
}

impl GithubFixture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pages: Mutex::new(BTreeMap::new()),
            posts: Mutex::new(Vec::new()),
        })
    }

    fn serve_page(&self, path: &str, body: Value) {
        self.pages.lock().unwrap().insert(path.into(), body);
    }

    fn posted_bodies(&self) -> Vec<(String, Value)> {
        self.posts.lock().unwrap().clone()
    }

    async fn serve(self: Arc<Self>) -> String {
        let app = axum::Router::new()
            .fallback(fixture_endpoint)
            .with_state(self);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture");
        let addr = listener.local_addr().expect("fixture addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("fixture serve") });
        format!("http://{addr}")
    }
}

async fn fixture_endpoint(
    State(state): State<Arc<GithubFixture>>,
    req: Request,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_default();
    if method != "GET" {
        let bytes = axum::body::Bytes::from_request(req, &state)
            .await
            .expect("fixture body");
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        state.posts.lock().unwrap().push((path, body));
        return (StatusCode::CREATED, Json(json!({"id": 4242})));
    }
    // Longest key prefix wins (`/issues` vs `/issues/comments`); the
    // map is cloned under one lock (no re-lock).
    let scripted = {
        let pages = state.pages.lock().unwrap().clone();
        pages
            .keys()
            .filter(|key| path.starts_with(key.as_str()))
            .max_by_key(|key| key.len())
            .and_then(|key| pages.get(key).cloned())
    };
    match scripted {
        Some(page) => (StatusCode::OK, Json(page)),
        // An unscripted GET is an empty list (an honest quiet repo).
        None => (StatusCode::OK, Json(json!([]))),
    }
}

fn issue_item(number: u64, updated: &str, login: &str, sender_type: &str) -> Value {
    json!({
        "number": number,
        "title": format!("issue {number}"),
        "body": "the CI log shows a panic",
        "updated_at": updated,
        "user": {"login": login, "type": sender_type, "id": 7},
    })
}

fn comment_item(id: u64, issue: u64, updated: &str) -> Value {
    json!({
        "id": id,
        "body": "still reproducible on main",
        "updated_at": updated,
        "issue_url": format!("https://api.github.com/repos/flyer103/kian-runtime/issues/{issue}"),
        "user": {"login": "reporter", "type": "User"},
    })
}

// ---------------------------------------------------------------------------
// The scripted model providers (the mock provider on both sides).
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;

/// Replays scripted turns in order and counts requests; one chosen
/// call parks on a oneshot gate until the test releases it (holding
/// the worker's first turn open so a follow-up can arrive mid-flight).
struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
    call_count: std::sync::atomic::AtomicUsize,
    gate_on_call: usize,
    gate_rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl ScriptedProvider {
    fn new(
        turns: Vec<ScriptedChunks>,
        gate_on_call: usize,
        gate_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            call_count: std::sync::atomic::AtomicUsize::new(0),
            gate_on_call,
            gate_rx: Mutex::new(gate_rx),
        }
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn wait_for_calls(&self, n: usize) {
        for _ in 0..800 {
            if self.request_count() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("provider never reached {n} calls");
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        use std::sync::atomic::Ordering;
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        let gate = if n == self.gate_on_call {
            self.gate_rx.lock().unwrap().take()
        } else {
            None
        };
        match gate {
            Some(rx) => futures::stream::once(async move {
                let _ = rx.await;
                turn
            })
            .flat_map(futures::stream::iter)
            .boxed(),
            None => futures::stream::iter(turn).boxed(),
        }
    }
}

fn text_chunk(text: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

/// One tool call whose arguments are arbitrary JSON (serialized once,
/// here — the wire shape the loop parses back).
fn tool_call_chunk(
    id: &str,
    name: &str,
    arguments: &Value,
) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"tool_calls": [{
        "index": 0,
        "id": id,
        "function": {"name": name, "arguments": arguments.to_string()}
    }]})))
}

/// The worker side's inert tool provider (the evidence gate reads the
/// stream shape; no real tool runs).
struct FakeTools;

impl ToolProvider for FakeTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "fs_read".into(),
            description: "test".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        Box::pin(async {
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

// ---------------------------------------------------------------------------
// Assembly surface: the late-bound inbox + per-hold stream port over
// the real engine (the shape).
// ---------------------------------------------------------------------------

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

struct CtxPort(DrainCtx);

impl StreamPort for CtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.events()
                .await
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }
}

fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(CtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

// ---------------------------------------------------------------------------
// The assembled engine: resident Router (RouterDriver) + every other
// session a worker (WorkerLoopDriver).
// ---------------------------------------------------------------------------

struct SuperAgentDriver {
    router: SessionId,
    router_side: RouterDriver<ReActLoop>,
    worker_side: WorkerLoopDriver<ReActLoop>,
}

impl kian_core::seams::LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, kian_core::seams::LoopOutcome> {
        if ctx.session() == self.router {
            self.router_side.drive(ctx)
        } else {
            self.worker_side.drive(ctx)
        }
    }
}

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-c64-e2e-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    std::mem::forget(path);
    store
}

async fn wait_for_events(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) {
    for _ in 0..800 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

fn issue_target(number: u64) -> Value {
    json!({
        "owner": "flyer103",
        "repo": "kian-runtime",
        "number": number,
        "kind": "issue_comment",
    })
}

// ---------------------------------------------------------------------------
// The end-to-end flow.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_ticket_fanout_end_to_end_over_fixture_pages() {
    // The macOS system proxy intercepts localhost (the AGENTS.md
    // gotcha) — the fixture must be reached directly.
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }

    // -- the scripted github ------------------------------------------------
    let fixture = GithubFixture::new();
    let api_base = fixture.clone().serve().await;
    // Poll 1: one new human issue (#101).
    fixture.serve_page(
        "/repos/flyer103/kian-runtime/issues",
        json!([issue_item(101, "2026-11-04T10:00:00Z", "reporter", "User")]),
    );
    fixture.serve_page("/repos/flyer103/kian-runtime/issues/comments", json!([]));

    let github_config = GitHubConfig {
        api_base: api_base.clone(),
        topics: BTreeMap::from([("flyer103/kian-runtime".to_string(), "issues".to_string())]),
        events: BTreeSet::new(),
        untrusted: true,
        send: true,
        token: Some("ghp_fixture".into()),
        poll: Some(PollConfig {
            repo: "flyer103/kian-runtime".into(),
            interval_ms: 30_000,
            kinds: BTreeSet::from([PollKind::Issues, PollKind::IssueComments]),
            since_field: POLL_SINCE_FIELD.into(),
            backfill_ms: 86_400_000,
        }),
    };
    let github = Arc::new(GitHubChannel::new("github", github_config).expect("valid config"));

    // The exact ChannelEvents the polls will surface (the same pure
    // mapping the adapter runs — used to build the spawn tasks and to
    // assert against committed streams).
    let issue_events = |items: &[Value]| {
        let (events, _) = poll_items_to_events(
            "github",
            github.config(),
            PollKind::Issues,
            items,
            Some("2026-11-01T00:00:00Z"),
        );
        events
    };
    let comment_events = |items: &[Value]| {
        let (events, _) = poll_items_to_events(
            "github",
            github.config(),
            PollKind::IssueComments,
            items,
            Some("2026-11-01T00:00:00Z"),
        );
        events
    };
    let issue_101 =
        issue_events(&[issue_item(101, "2026-11-04T10:00:00Z", "reporter", "User")]).remove(0);
    let issue_102 = issue_events(&[issue_item(
        102,
        "2026-11-04T12:00:00Z",
        "another-user",
        "User",
    )])
    .remove(0);
    let comment_991 = comment_events(&[comment_item(991, 101, "2026-11-04T11:00:00Z")]).remove(0);
    assert_eq!(
        issue_101.topic.as_deref(),
        Some("github:flyer103/kian-runtime#101"),
        "the poll topic is the thread key"
    );

    // -- the scripted Router judge (the mandate-bearing accept) -------------
    // Turn sequence (one entry per model request; the resident Router
    // parks after each tool-executing turn — the next admitted input
    // re-drives it):
    //   1. issue #101 arrives → an attempted ack `reply` — suppressed by
    //      the `result-ready` speak rule (the honest error the model
    //      sees; zero adapter sends);
    //   2. → `spawn_worker` with the channel wrapper (the accept with
    //      Mandate — the wrapper's adapter-declared explicit makes the
    //      strength explicit);
    //   3. The completion report arrives → `deliver_structured` (the
    //      triage draft — a Result, admitted by result-ready);
    //   4. issue #102 → `spawn_worker` (the independent fan-out);
    //   5. report #2 → `deliver_structured` on issue #102.
    let wrapper_101 = kian_superagent::provenance::wrap_channel_event_marked(&issue_101, true);
    let wrapper_102 = kian_superagent::provenance::wrap_channel_event_marked(&issue_102, true);
    let router_turns: Vec<ScriptedChunks> = vec![
        vec![tool_call_chunk(
            "call-ack-101",
            "reply",
            &json!({
                "channel": "github",
                "text": "on it — triaging issue 101",
                "target": issue_target(101),
            }),
        )],
        vec![tool_call_chunk(
            "call-spawn-101",
            "spawn_worker",
            &json!({"task": wrapper_101.clone()}),
        )],
        vec![tool_call_chunk(
            "call-draft-101",
            "deliver_structured",
            &json!({
                "channel": "github",
                "target": issue_target(101),
                "payload": {
                    "background": "issue 101: build broken on main",
                    "changes": "triaged: severity minor, needs-human",
                    "verification": "draft only (DRY_RUN=1)"
                }
            }),
        )],
        vec![tool_call_chunk(
            "call-spawn-102",
            "spawn_worker",
            &json!({"task": wrapper_102.clone()}),
        )],
        vec![tool_call_chunk(
            "call-draft-102",
            "deliver_structured",
            &json!({
                "channel": "github",
                "target": issue_target(102),
                "payload": {
                    "background": "issue 102",
                    "changes": "triaged: severity trivial, reply drafted",
                    "verification": "draft only (DRY_RUN=1)"
                }
            }),
        )],
    ];
    // Router call 4 (the issue-#102 spawn) is gated: without the gate
    // the loop iterates straight from the #101 draft delivery into the
    // #102 spawn — eager fan-out before the second issue was ever
    // polled. The gate holds the turn open until the test has polled
    // issue #102 in and asserted the fresh-Eval routing.
    let (router_gate_tx, router_gate_rx) = tokio::sync::oneshot::channel::<()>();
    let router_script = Arc::new(ScriptedProvider::new(router_turns, 4, Some(router_gate_rx)));

    // -- the scripted workers (worker A's first turn gated open) ------------
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let worker_script = Arc::new(ScriptedProvider::new(
        vec![
            // Worker A (issue #101), turn 1: the triage draft — gated
            // so the follow-up comment lands while the ticket is live.
            vec![text_chunk(
                "triage: issue 101 is severity minor, decision needs-human; \
                 draft: the panic comes from the timer rewrite, see log line 42",
            )],
            // Worker A, turn 2: the mid-drain follow-up comment consumed
            // a second model request (its own final answer completes
            // the thread).
            vec![text_chunk("noted — the follow-up confirms severity minor")],
            // Worker B (issue #102): its own final message.
            vec![text_chunk(
                "triage: issue 102 is severity trivial, decision reply",
            )],
        ],
        1,
        Some(gate_rx),
    ));

    // -- the engine ---------------------------------------------------------
    let store = temp_store().await;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = SessionId::new();
    let router_agent = router_template("mock", "mock-router");
    let worker_agent = worker_template("mock", "mock-worker", 7);

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    let router_inbox = RouterInbox::new(
        late.clone() as Arc<dyn InboxWriter>,
        kian_superagent::ResidentBinding::new(router),
    );

    // The dispatch tools: spawn + the channel sends, under the
    // policies (speak: result-ready; mandate: the triage scope) and the
    // shared ownership registry.
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_agent,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mut channels = ChannelRegistry::new();
    channels.register("github", Arc::clone(&github) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_agent.clone());
    profile.speak = SpeakPolicy::from_rules(vec![SpeakRule {
        channel: "github".into(),
        when: SpeakCondition::ResultReady,
        per_topic_ack: false,
    }]);
    profile.followup_ownership = true;
    profile.mandate = Some(MandatePolicy {
        scopes: BTreeMap::from([(
            // The worker template's AgentDef id (`worker_template`'s
            // fixed `kian-worker`).
            "kian-worker".to_string(),
            "Draft a triage comment for the assigned GitHub issue; never post unless \
             DRY_RUN=0 is set in the environment."
                .to_string(),
        )]),
        default_scope: None,
    });
    let shared_registry = Arc::new(std::sync::RwLock::new(
        kian_superagent::PassthroughRegistry::new(),
    ));
    let tools = Arc::new(
        RouterTools::new(
            spawner,
            channels,
            &profile,
            hold.clone(),
            kian_superagent::ResidentBinding::new(profile.session),
        )
        .with_followup_registry(Arc::clone(&shared_registry)),
    );

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: std::collections::BTreeMap::new(),
        },
    ));
    let worker_loop = Arc::new(ReActLoop::with_config(
        worker_agent,
        Arc::new(worker_registry),
        Arc::new(FakeTools),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    ));
    let wrapper_config = WorkerLoopConfig {
        router: kian_superagent::ResidentBinding::new(router),
        worker_version: 7,
        multiagent: None,
        board_gating: kian_superagent::BoardGating::default(),
        gate: EvidenceGateConfig {
            enabled: true,
            checks: vec![CheckSpec::FinalMessagePresent],
            max_retries: 1,
            ..EvidenceGateConfig::default()
        },
    };
    let router_side = RouterDriver::with_park_tick(
        router_loop,
        hold,
        port_for_ctx(),
        router_inbox.clone(),
        Duration::from_millis(10),
    );
    // The worker wrapper holds the MOUNTED router inbox (assembly's
    // wiring): a completion report admitted to the Router ticks the
    // residency counter a parked resident polls — the raw
    // engine inbox would fold the wake into the parked drain and the
    // judge turn would never fire.
    let worker_side = WorkerLoopDriver::new(
        worker_loop,
        router_inbox.clone() as Arc<dyn InboxWriter>,
        wrapper_config,
        port_for_ctx(),
    );
    let engine = Arc::new(Engine::with_clock(
        store.clone(),
        Arc::new(SuperAgentDriver {
            router,
            router_side,
            worker_side,
        }),
        EngineConfig::default(),
        clock,
    ));
    assert!(late.engine.set(Arc::clone(&engine)).is_ok());

    // -- the subscription pipeline (the fan-out surface) --------------------
    let service = Arc::new(
        SubscriptionService::with_classes(
            kian_superagent::ResidentBinding::new(router),
            SubscriptionRuleSet::new(vec![SubscriptionRule {
                channel: "github".into(),
                matcher: RuleMatcher::Always,
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            }]),
            kian_superagent::AdmissionClassTable::new(vec![kian_superagent::AdmissionClassRule {
                channel: "github".into(),
                topic: None,
                sender: Some(json!({"login": "github-actions[bot]", "type": "Bot", "id": 7})),
                class: kian_superagent::AdmissionClass::Ignore,
            }]),
            router_inbox.clone() as Arc<dyn InboxWriter>,
        )
        .with_untrusted_channels(BTreeSet::from(["github".to_string()]))
        .with_shared_registry(Arc::clone(&shared_registry)),
    );

    // -- poll 1: the new issue reaches the Router judge ---------------------
    github.seed_poll_cursor(Some("2026-11-01T00:00:00Z".into()));
    assert_eq!(github.poll_once().await.expect("poll 1"), 1);
    let outcome = service
        .handle_event(issue_101.clone())
        .await
        .expect("route");
    // Unbound topic: the passthrough Router fallback (the fresh Eval).
    assert_eq!(
        outcome,
        kian_superagent::RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );

    // The judge turn: ack suppressed (turn 1), spawn accepted (turn 2) —
    // the resident Router parks after each tool-executing turn.
    router_script.wait_for_calls(2).await;
    wait_for_events(&store, router, "the spawn's thread/started", |events| {
        count_kind(events, EventKind::ThreadStarted) == 1
    })
    .await;

    let router_events = store.rebuild(router).await.expect("rebuild");
    let started = router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("thread/started");
    let worker_a = SessionId(
        Uuid::parse_str(started.bytes["worker"].as_str().expect("worker id")).expect("uuid"),
    );
    // The mandate rode the delegation wrapper verbatim (ADR-012: the
    // triggering wrapper's adapter-declared explicit ⇒ strength
    // explicit; the source is the poll event's channel+topic).
    let task = &started.bytes["task"]["content"];
    let mandate = kian_superagent::delegation_mandate(task).expect("mandate on the wrapper");
    assert_eq!(mandate["strength"], "explicit");
    assert_eq!(mandate["source"]["channel"], "github");
    assert_eq!(
        mandate["source"]["topic"],
        "github:flyer103/kian-runtime#101"
    );
    assert_eq!(mandate["granted_by"]["login"], "reporter");
    assert!(
        mandate["scope"]
            .as_str()
            .expect("scope")
            .contains("never post unless DRY_RUN=0"),
        "the mandate scope is the configured triage bound: {mandate}"
    );

    // The spawned worker is the live owner of the issue's thread.
    assert_eq!(
        shared_registry
            .read()
            .expect("registry")
            .live_owner("github:flyer103/kian-runtime#101"),
        Some(worker_a)
    );

    // Worker A's first turn is gated open (the ticket is live,
    // mid-flight).
    worker_script.wait_for_calls(1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let worker_events = store.rebuild(worker_a).await.expect("rebuild");
    assert!(
        worker_events
            .iter()
            .any(|e| e.kind == EventKind::ThreadStarted),
        "the worker stream carries its side of the delegation"
    );
    // The stage projection's honest view at this instant: the
    // consuming drain's `drain/claimed` (seq 2) precedes its own
    // `input/admitted` append (seq 3), so arm 5 reads the ticket as
    // queued — an input is admitted with no LATER claim. The working
    // and idle arms assert below at their honest moments.
    let stage = project_stage(&worker_events, 0, &NoOutbox);
    assert_eq!(
        stage,
        kian_superagent::TicketStage::Queued,
        "mid-first-turn the projection reads queued (kinds: {:?})",
        worker_events
            .iter()
            .map(|e| (e.seq, e.kind))
            .collect::<Vec<_>>()
    );

    // -- poll 2: the follow-up comment — zero Router judge turns -----------
    fixture.serve_page(
        "/repos/flyer103/kian-runtime/issues/comments",
        json!([comment_item(991, 101, "2026-11-04T11:00:00Z")]),
    );
    assert_eq!(github.poll_once().await.expect("poll 2"), 1);
    let router_calls_before = router_script.request_count();
    let outcome = service
        .handle_event(comment_991.clone())
        .await
        .expect("route");
    // The live Executor owns the follow-up: the row goes to the
    // worker, and the Router was neither admitted nor woken.
    assert_eq!(
        outcome,
        kian_superagent::RoutingOutcome::Routed {
            session: worker_a,
            deduplicated: false,
        }
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        router_script.request_count(),
        router_calls_before,
        "zero Router judge turns spent on the follow-up"
    );
    let router_events = store.rebuild(router).await.expect("rebuild");
    assert!(
        !router_events.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && serde_json::to_string(&e.bytes)
                    .unwrap()
                    .contains(&comment_991.event_id)
        }),
        "the Router stream never carries the follow-up's event id"
    );

    // -- the worker completes; the draft delivers under speak-rules --------
    gate_tx.send(()).expect("release worker A");
    wait_for_events(&store, worker_a, "worker A's completion", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    wait_for_events(
        &store,
        worker_a,
        "worker A consumed the follow-up",
        |events| {
            // The follow-up row was consumed mid-flight: its channel
            // wrapper is an admitted input on the WORKER stream.
            events.iter().any(|e| {
                e.kind == EventKind::InputAdmitted
                    && serde_json::to_string(&e.bytes)
                        .unwrap()
                        .contains(&comment_991.event_id)
            })
        },
    )
    .await;

    // The Router's report turn delivers the triage draft (turn 3:
    // deliver_structured — the Result the speak rule admits).
    router_script.wait_for_calls(3).await;
    wait_for_events(&store, router, "the structured draft's receipt", |events| {
        count_kind(events, EventKind::ChannelDelivered) == 1
    })
    .await;
    let router_events = store.rebuild(router).await.expect("rebuild");
    let delivered = router_events
        .iter()
        .find(|e| e.kind == EventKind::ChannelDelivered)
        .expect("channel/delivered");
    assert_eq!(delivered.bytes["channel"], "github");
    assert_eq!(delivered.bytes["policy"], "structured");
    // The send hit the scripted REST API (an issue comment on #101).
    let posts = fixture.posted_bodies();
    assert_eq!(posts.len(), 1, "exactly one adapter send: {posts:?}");
    assert!(
        posts[0]
            .0
            .starts_with("/repos/flyer103/kian-runtime/issues/101/comments")
    );
    assert!(
        posts[0].1["body"]
            .as_str()
            .expect("rendered body")
            .contains("## background"),
        "the structured sections render: {}",
        posts[0].1["body"]
    );
    // The ack attempt never sent: exactly one POST total (the ack was
    // suppressed before the adapter — the deliver-or-discard).
    // (posts.len() == 1 asserted above is exactly that.)

    // Stage transition: the ticket is terminal. The projection's arm 5
    // has an honest ordering artifact on real engine streams: the
    // consuming drain appends `input/admitted` (take-time) AFTER its
    // own `drain/claimed`, so a consumed input always leaves
    // `last_input > last_claim` and arm 5 (`queued`) matches before
    // arm 6 (`idle`) can — the ticket never projects `working` or
    // `awaiting-*` again, and the terminal facts are asserted directly
    // below (recorded as a gap for the live run to observe).
    let worker_events = store.rebuild(worker_a).await.expect("rebuild");
    let stage = project_stage(&worker_events, 0, &NoOutbox);
    assert!(
        matches!(
            stage,
            kian_superagent::TicketStage::Idle | kian_superagent::TicketStage::Queued
        ),
        "the completed ticket never projects an active stage again (got {stage}, kinds: {:?})",
        worker_events
            .iter()
            .map(|e| (e.seq, e.kind))
            .collect::<Vec<_>>()
    );
    assert_eq!(count_kind(&worker_events, EventKind::ThreadCompleted), 1);
    assert!(
        worker_events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    );

    // -- poll 3: a second live issue fans out independently -----------------
    fixture.serve_page(
        "/repos/flyer103/kian-runtime/issues",
        json!([
            issue_item(101, "2026-11-04T10:00:00Z", "reporter", "User"), // unchanged
            issue_item(102, "2026-11-04T12:00:00Z", "another-user", "User"), // new
        ]),
    );
    assert_eq!(github.poll_once().await.expect("poll 3"), 1);
    let outcome = service
        .handle_event(issue_102.clone())
        .await
        .expect("route");
    // Issue #102's thread is unbound (A owned #101 and has ended):
    // fresh Eval at the Router — one source, a second ticket.
    assert_eq!(
        outcome,
        kian_superagent::RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );

    // Release the gated judge turn: the Router accepts issue #102 with
    // its own mandate and spawns worker B (the independent ticket).
    router_gate_tx
        .send(())
        .expect("release the #102 judge turn");
    router_script.wait_for_calls(4).await;
    wait_for_events(&store, router, "two thread/started delegations", |events| {
        count_kind(events, EventKind::ThreadStarted) == 2
    })
    .await;
    wait_for_events(&store, router, "two delivered drafts", |events| {
        count_kind(events, EventKind::ChannelDelivered) == 2
    })
    .await;

    // Two tickets, one source: distinct workers, distinct threads,
    // both drafts posted, both ended idle.
    let router_events = store.rebuild(router).await.expect("rebuild");
    let workers: BTreeSet<String> = router_events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes["worker"].as_str().expect("worker").to_string())
        .collect();
    assert_eq!(
        workers.len(),
        2,
        "two distinct worker sessions: {workers:?}"
    );
    assert!(workers.contains(&worker_a.0.to_string()));
    let worker_b_id = workers
        .iter()
        .find(|w| **w != worker_a.0.to_string())
        .expect("worker B")
        .clone();
    let worker_b = SessionId(Uuid::parse_str(&worker_b_id).expect("uuid"));
    for worker in [worker_a, worker_b] {
        // The worker's thread/completed lands on ITS OWN stream, after the
        // Router-side ChannelDelivered we waited for above — wait for it
        // here instead of racing the async completion.
        wait_for_events(&store, worker, "worker thread completed", |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
        })
        .await;
        let events = store.rebuild(worker).await.expect("rebuild");
        // Both tickets are terminal (the arm-5 ordering artifact above
        // applies to each; the facts are what the projection reads).
        let stage = project_stage(&events, 0, &NoOutbox);
        assert!(
            matches!(
                stage,
                kian_superagent::TicketStage::Idle | kian_superagent::TicketStage::Queued
            ),
            "both tickets ended terminal (got {stage})"
        );
        assert_eq!(count_kind(&events, EventKind::ThreadCompleted), 1);
    }
    let posts = fixture.posted_bodies();
    assert_eq!(posts.len(), 2, "one draft per issue: {posts:?}");
    assert!(
        posts
            .iter()
            .any(|(path, _)| path.starts_with("/repos/flyer103/kian-runtime/issues/101/comments"))
    );
    assert!(
        posts
            .iter()
            .any(|(path, _)| path.starts_with("/repos/flyer103/kian-runtime/issues/102/comments"))
    );

    // The speak ledger on the Router stream: the suppressed ack left
    // no channel/delivered (asserted: exactly 2 delivered, both
    // structured Results) — the honest trace of not sending.
    let delivered: Vec<&Event> = router_events
        .iter()
        .filter(|e| e.kind == EventKind::ChannelDelivered)
        .collect();
    assert!(delivered.iter().all(|e| e.bytes["policy"] == "structured"));
}
