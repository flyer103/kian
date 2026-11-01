//! C28 integration tests: the S4 acceptance scenario, CI-safe.
//!
//! mock-IM work request → Router judges (scripted router model:
//! dispatch) → Worker spawns via Thread delegation → worker model
//! performs scripted tool calls → evidence gate passes → worker
//! completes → report lands in the Router inbox → Router replies on
//! the mock channel: an IM reply ≤100 words **and** a structured
//! delivery (background/changes/verification).
//!
//! Everything runs against the real assembled server (C27): the real
//! sqlite event store, the real engine, the real subscription engine,
//! the real Router/Worker drivers — with the two providers mocked at
//! the openai-compat wire level (the C13 `common` fixtures — no
//! credentials, no network egress) and the mock-IM channel injected
//! through the ops plane (`mock_channel_send`, the README demo path).
//! `NO_PROXY=127.0.0.1,localhost` is forced in every test (AGENTS.md
//! gotcha). Observations read the committed streams through a
//! read-only store handle, never the server's internals.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::commands::{import_jsonl, run_export};
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use kian_superagent::delegation_pairs;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

const WORK_TASK: &str = "Summarize the changelog and verify the build";

/// The over-limit IM reply (120 whitespace words) — proving the §4.2
/// cap is enforced on the assembled path, not just in unit tests.
fn over_limit_reply() -> String {
    (1..=120)
        .map(|i| format!("word{i}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

/// The S4 super-agent `kian.yaml`: two mock providers (router/worker
/// scripts stay independent), mock-IM channel routed to the Router,
/// and a configurable worker evidence gate. No endpoints, no
/// credentials beyond the shared dummy env var.
fn write_s4_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
    worker_gate: &str,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7340"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
  tools: ["fs.read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: router-mock
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs.read", "fs.write", "exec"]
{worker_gate}superagent:
  router:
    session_id: "{ROUTER_SESSION}"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: mock-im
      match: always
      route: router
  channels:
    mock-im:
      type: mock-im
providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 16
tools:
  root: "{dir}"
  exec_timeout_ms: 5000
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        dir = dir.display(),
        worker_gate = worker_gate,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write s4 config");
    path
}

/// Spawns the assembled server on an ephemeral port (the C27 pattern).
async fn spawn_server(config_path: &std::path::Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble s4 server")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (format!("http://{addr}"), server)
}

async fn ops_rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops json")
}

/// Polls a session's committed stream until `pred` holds (generous:
/// the full S4 flow is ~7 provider turns over the real wire).
async fn wait_for_session(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..2000 {
        let events = store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// The worker session a `thread/started` event names.
fn worker_of(started: &Event) -> SessionId {
    SessionId(uuid::Uuid::parse_str(started.bytes["worker"].as_str().unwrap()).unwrap())
}

fn thread_id_of(started: &Event) -> uuid::Uuid {
    uuid::Uuid::parse_str(started.bytes["thread_id"].as_str().unwrap()).unwrap()
}

/// Everything the named S4 assertions need after one full flow.
struct S4Run {
    dir: PathBuf,
    config: PathBuf,
    server: Arc<Server>,
    router_events: Vec<Event>,
    worker_session: SessionId,
    worker_events: Vec<Event>,
    injected_event_id: String,
}

/// Runs the full S4 scenario: inject → judge → dispatch → worker tool
/// calls → gate → completion → report → IM reply + structured
/// delivery. Waits until both `channel/delivered` events are committed
/// and the worker's `thread/completed` is in, then snapshots both
/// streams.
async fn run_s4_flow(
    tag: &str,
    router_script: Vec<ScriptedResponse>,
    worker_script: Vec<ScriptedResponse>,
    worker_gate: &str,
) -> S4Run {
    let dir = temp_dir(&format!("c28-{tag}"));
    let (router_base, _router_mock) = spawn_scripted_provider(router_script, 0).await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(worker_script, 0).await;
    let config = write_s4_config(&dir, &router_base, &worker_base, worker_gate);
    let (base, server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");

    // Wait for the resident Router's bootstrap judge turn.
    wait_for_session(
        &read_only,
        router_session(),
        "bootstrap judge turn",
        |events| count_kind(events, EventKind::TurnStart) >= 1,
    )
    .await;

    // The README demo path: one ops injection onto the mock channel.
    let client = reqwest::Client::new();
    let out = ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "please run the release checklist"}}),
    )
    .await;
    let injected_event_id = out["result"]["event_id"]
        .as_str()
        .expect("event id")
        .to_owned();
    assert_eq!(out["result"]["channel"], "mock-im");

    // The Router dispatched a worker.
    let router_events = wait_for_session(
        &read_only,
        router_session(),
        "router dispatches",
        |events| count_kind(events, EventKind::ThreadStarted) >= 1,
    )
    .await;
    let worker_session = worker_of(
        router_events
            .iter()
            .find(|e| e.kind == EventKind::ThreadStarted)
            .unwrap(),
    );

    // The worker ran its tool calls, passed the gate and completed;
    // the Router delivered both channel outputs and settled its final
    // answer (5 router turns: bootstrap, judge+spawn, reply,
    // deliver_structured, final).
    let worker_events =
        wait_for_session(&read_only, worker_session, "worker completes", |events| {
            count_kind(events, EventKind::ThreadCompleted) >= 1
                && events
                    .iter()
                    .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        })
        .await;
    wait_for_session(
        &read_only,
        router_session(),
        "router delivers both outputs",
        |events| count_kind(events, EventKind::ChannelDelivered) >= 2,
    )
    .await;

    // Let the router's final text turn commit before snapshotting
    // (the resident session parks afterward — no further writes).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let router_events = read_only
        .rebuild(router_session())
        .await
        .expect("final router rebuild");

    S4Run {
        dir,
        config,
        server,
        router_events,
        worker_session,
        worker_events,
        injected_event_id,
    }
}

/// The standard S4 scripts: judge dispatches, worker acts, router
/// replies over-limit (120 words — the cap must hold) and delivers the
/// structured report.
fn standard_scripts() -> (Vec<ScriptedResponse>, Vec<ScriptedResponse>) {
    let router = vec![
        ScriptedResponse::Text("bootstrap acknowledged".into()),
        ScriptedResponse::ToolCall {
            id: "call-spawn-1".into(),
            name: "spawn_worker".into(),
            arguments: serde_json::json!({"task": WORK_TASK}),
        },
        ScriptedResponse::ToolCall {
            id: "call-reply-1".into(),
            name: "reply".into(),
            arguments: serde_json::json!({
                "channel": "mock-im",
                "target": {"chat_id": 42},
                "text": over_limit_reply(),
            }),
        },
        ScriptedResponse::ToolCall {
            id: "call-deliver-1".into(),
            name: "deliver_structured".into(),
            arguments: serde_json::json!({
                "channel": "mock-im",
                "target": {"chat_id": 42},
                "payload": {
                    "background": "The user asked for a changelog summary and build verification.",
                    "changes": "Twelve changelog entries summarized.",
                    "verification": "Build verified with one exec tool call."
                }
            }),
        },
        ScriptedResponse::Text("Delivered.".into()),
    ];
    let worker = vec![
        ScriptedResponse::ToolCall {
            id: "call-exec-1".into(),
            name: "exec".into(),
            arguments: serde_json::json!({"argv": ["echo", "s4-ready"]}),
        },
        ScriptedResponse::Text("Done: changelog summarized and build verified.".into()),
    ];
    (router, worker)
}

/// The passing worker gate: a final message after the delegated task
/// and at least one tool result since it (the worker actually acted).
const PASSING_GATE: &str = "    evidence_gate:\n      enabled: true\n      checks:\n        - type: final_message_present\n        - type: tool_result_since_last_user_input\n      max_retries: 1\n";

// ---------------------------------------------------------------------------
// S4: the Router judges the mock-IM request and dispatches a worker
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_router_judges_and_dispatches() {
    force_no_proxy();
    let (router, worker) = standard_scripts();
    let run = run_s4_flow("judge", router, worker, PASSING_GATE).await;

    // The channel event was admitted to the Router with the injected
    // event id verbatim (routing provenance rides inside content).
    let admitted = run
        .router_events
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("channel_event"))
        })
        .expect("channel input admitted on the router stream");
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&serde_json::json!(run.injected_event_id)),
        "the input carries the injected event id verbatim"
    );

    // The judge turn saw the event (persist-what-you-send).
    let judged = run.router_events.iter().any(|e| {
        e.kind == EventKind::StepRequest && {
            let body = serde_json::to_string(&e.bytes["body"]).unwrap_or_default();
            body.contains(&run.injected_event_id)
        }
    });
    assert!(judged, "a judge request body contains the event id");

    // The Router-side lineage event names the worker and the task.
    let started = run
        .router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("router thread/started");
    let thread_id = thread_id_of(started);
    assert_eq!(started.bytes["parent"], serde_json::json!(ROUTER_SESSION));
    assert_eq!(
        started.bytes.pointer("/task/content/task"),
        Some(&serde_json::json!(WORK_TASK)),
        "the delegated task rides the lineage event: {started:?}"
    );

    // The worker-side pair: same thread id, same worker, and the
    // delegation wrapper is the worker's model-visible first input.
    let worker_started = run
        .worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("worker thread/started");
    assert_eq!(
        thread_id_of(worker_started),
        thread_id,
        "the pair shares the thread id"
    );
    assert_eq!(worker_started.bytes["worker"], started.bytes["worker"]);
    let first_input = run
        .worker_events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .expect("worker first input");
    assert_eq!(
        first_input.bytes.pointer("/content/thread_id"),
        Some(&serde_json::json!(thread_id.to_string()))
    );
    assert_eq!(
        first_input.bytes.pointer("/content/task"),
        Some(&serde_json::json!(WORK_TASK))
    );

    // A2 audit: the delegation pair is discoverable by scanning both
    // streams — and it is exactly one, causally linked.
    let pairs = delegation_pairs(&run.router_events, &run.worker_events);
    assert_eq!(pairs.len(), 1, "exactly one delegation pair: {pairs:?}");
    assert_eq!(pairs[0].thread_id, thread_id);
    assert_eq!(pairs[0].parent, router_session());
    assert_eq!(pairs[0].worker, run.worker_session);

    // The worker's first provider request saw the delegated task
    // (persist-what-you-send across the delegation boundary).
    let saw_task = run.worker_events.iter().any(|e| {
        e.kind == EventKind::StepRequest && {
            let body = serde_json::to_string(&e.bytes["body"]).unwrap_or_default();
            body.contains(WORK_TASK)
        }
    });
    assert!(
        saw_task,
        "the worker's first request body contains the task"
    );

    let _ = std::fs::remove_dir_all(&run.dir);
}

// ---------------------------------------------------------------------------
// S4: the worker completes with evidence
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_worker_completes_with_evidence() {
    force_no_proxy();
    let (router, worker) = standard_scripts();
    let run = run_s4_flow("evidence", router, worker, PASSING_GATE).await;

    // The worker acted: one exec tool call and a non-error result.
    assert!(
        run.worker_events
            .iter()
            .any(|e| e.kind == EventKind::ToolCall && e.bytes["content"]["name"] == "exec"),
        "the worker performed a scripted tool call"
    );
    let result = run
        .worker_events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .expect("exec tool result");
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(false)),
        "the local exec succeeded: {result:?}"
    );

    // The evidence gate passed and precedes thread/completed, which
    // precedes the single atomic end write.
    let verdict = run
        .worker_events
        .iter()
        .find(|e| e.kind == EventKind::EvidenceVerdict)
        .expect("evidence verdict");
    assert_eq!(verdict.bytes["gate"], serde_json::json!("worker"));
    assert_eq!(
        verdict.bytes["passed"],
        serde_json::json!(true),
        "{verdict:?}"
    );
    let verdict_seq = verdict.seq;
    let completed = run
        .worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .expect("thread/completed");
    assert_eq!(completed.bytes["status"], serde_json::json!("completed"));
    assert!(
        verdict_seq < completed.seq,
        "verdict precedes thread/completed"
    );
    let ended = run
        .worker_events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .expect("session/ended");
    assert!(
        matches!(
            ended.kind,
            EventKind::SessionEnded {
                reason: EndReason::Completed
            }
        ),
        "honest completion: {ended:?}"
    );
    assert!(
        completed.seq < ended.seq,
        "thread/completed precedes the end write"
    );

    // The completion report landed durably in the Router's inbox with
    // the matching thread id and honest status.
    let report = run
        .router_events
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/thread_id") == completed.bytes.pointer("/thread_id")
        })
        .expect("completion report admitted to the router inbox");
    assert_eq!(
        report.bytes.pointer("/content/status"),
        Some(&serde_json::json!("completed"))
    );

    let _ = std::fs::remove_dir_all(&run.dir);
}

// ---------------------------------------------------------------------------
// S4: the IM reply respects the ≤100-word limit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_reply_within_word_limit() {
    force_no_proxy();
    let (router, worker) = standard_scripts();
    let run = run_s4_flow("wordlimit", router, worker, PASSING_GATE).await;

    // The scripted reply was 120 words; the §4.2 cap held on the
    // assembled path: the audit event says 100 words, truncated.
    let delivered = run
        .router_events
        .iter()
        .find(|e| {
            e.kind == EventKind::ChannelDelivered && e.bytes["policy"] == serde_json::json!("im")
        })
        .expect("im channel/delivered");
    assert_eq!(
        delivered.bytes["words"],
        serde_json::json!(100),
        "{delivered:?}"
    );
    assert_eq!(delivered.bytes["truncated"], serde_json::json!(true));
    assert!(delivered.bytes["words"].as_u64().unwrap() <= 100);

    // The mock channel actually received ≤100 words, cut at the limit.
    let runtime = run
        .server
        .superagent_runtime()
        .expect("superagent runtime assembled");
    let mock = runtime.mock_channel("mock-im").expect("mock-im channel");
    let deliveries = mock.deliveries();
    let im = deliveries
        .iter()
        .find(|d| d.delivery.content.get("text").is_some())
        .expect("the im delivery reached the channel");
    let text = im.delivery.content["text"].as_str().unwrap();
    let words = text.split_whitespace().count();
    assert!(words <= 100, "delivered {words} words");
    assert_eq!(words, 100, "cut exactly at the limit");
    assert!(text.ends_with('…'), "truncation marker present");

    let _ = std::fs::remove_dir_all(&run.dir);
}

// ---------------------------------------------------------------------------
// S4: the structured delivery carries background/changes/verification
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_structured_delivery() {
    force_no_proxy();
    let (router, worker) = standard_scripts();
    let run = run_s4_flow("structured", router, worker, PASSING_GATE).await;

    // Both deliveries landed: one im, one structured — with word
    // counts on the im event.
    let delivered: Vec<&Event> = run
        .router_events
        .iter()
        .filter(|e| e.kind == EventKind::ChannelDelivered)
        .collect();
    assert_eq!(delivered.len(), 2, "channel/delivered ×2: {delivered:?}");
    let policies: Vec<&serde_json::Value> = delivered.iter().map(|e| &e.bytes["policy"]).collect();
    assert!(policies.contains(&&serde_json::json!("im")));
    assert!(policies.contains(&&serde_json::json!("structured")));

    // The structured payload reached the channel with every required
    // section and the requested target.
    let runtime = run
        .server
        .superagent_runtime()
        .expect("superagent runtime assembled");
    let mock = runtime.mock_channel("mock-im").expect("mock-im channel");
    let deliveries = mock.deliveries();
    let structured = deliveries
        .iter()
        .find(|d| d.delivery.content.get("background").is_some())
        .expect("the structured delivery reached the channel");
    for section in ["background", "changes", "verification"] {
        assert!(
            structured
                .delivery
                .content
                .get(section)
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty()),
            "section `{section}` present and non-empty: {}",
            structured.delivery.content
        );
    }
    assert_eq!(
        structured.delivery.target,
        serde_json::json!({"chat_id": 42})
    );
    assert_eq!(structured.delivery.channel, "mock-im");

    let _ = std::fs::remove_dir_all(&run.dir);
}

// ---------------------------------------------------------------------------
// S4: export → re-import replays; the delegation pair stays linked
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_delegation_audit_export_replays() {
    force_no_proxy();
    let (router, worker) = standard_scripts();
    let run = run_s4_flow("export", router, worker, PASSING_GATE).await;

    // Export both sessions through the C14 JSONL path.
    let router_out = run.dir.join("router-audit.jsonl");
    let worker_out = run.dir.join("worker-audit.jsonl");
    run_export(
        &run.config,
        None,
        router_session(),
        None,
        None,
        Some(&router_out),
    )
    .await
    .expect("export router");
    run_export(
        &run.config,
        None,
        run.worker_session,
        None,
        None,
        Some(&worker_out),
    )
    .await
    .expect("export worker");
    let router_jsonl = std::fs::read_to_string(&router_out).expect("router jsonl");
    let worker_jsonl = std::fs::read_to_string(&worker_out).expect("worker jsonl");
    assert!(!router_jsonl.is_empty() && !worker_jsonl.is_empty());

    // Re-import into a fresh store: identical projections.
    let fresh_path = run.dir.join("fresh.sqlite");
    let fresh = EventStore::open(&fresh_path).await.expect("fresh store");
    import_jsonl(&fresh, &router_jsonl)
        .await
        .expect("import router");
    import_jsonl(&fresh, &worker_jsonl)
        .await
        .expect("import worker");
    for (session, original) in [
        (router_session(), &run.router_events),
        (run.worker_session, &run.worker_events),
    ] {
        let rebuilt = fresh.rebuild(session).await.expect("rebuild imported");
        assert_eq!(
            rebuilt.len(),
            original.len(),
            "session {session:?}: imported projection has the same event count"
        );
        for (a, b) in original.iter().zip(rebuilt.iter()) {
            assert_eq!(
                serde_json::to_string(a).unwrap(),
                serde_json::to_string(b).unwrap(),
                "event {} of {session:?} replays byte-identically",
                a.seq
            );
        }
    }

    // The delegation pair remains causally linked across the bundle in
    // the re-imported store alone (A2/A1: the audit needs nothing but
    // the exported logs).
    let imported_router = fresh
        .rebuild(router_session())
        .await
        .expect("router from fresh");
    let imported_worker = fresh
        .rebuild(run.worker_session)
        .await
        .expect("worker from fresh");
    let pairs = delegation_pairs(&imported_router, &imported_worker);
    assert_eq!(
        pairs.len(),
        1,
        "one causally-linked delegation pair: {pairs:?}"
    );
    assert_eq!(pairs[0].parent, router_session());
    assert_eq!(pairs[0].worker, run.worker_session);
    fresh.close().await;

    let _ = std::fs::remove_dir_all(&run.dir);
}

// ---------------------------------------------------------------------------
// S4: the README demo path — the real `kian` CLI driving a running serve
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_readme_demo_cli_mock_channel_send_against_serve() {
    force_no_proxy();
    let dir = temp_dir("c28-cli-demo");

    // Router: ack the bootstrap, then a plain judge answer for the
    // CLI-injected batch (this test proves the demo path, not the
    // dispatch — the flow above owns that).
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let config = write_s4_config(&dir, &router_base, &worker_base, "");
    let (base, _server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");

    wait_for_session(
        &read_only,
        router_session(),
        "bootstrap judge turn",
        |events| count_kind(events, EventKind::TurnStart) >= 1,
    )
    .await;

    // The demo command from the README, pointed at the running serve.
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args([
            "mock-channel-send",
            "mock-im",
            "--text",
            "please summarize",
            "--url",
            &base,
            "--json",
        ])
        .output()
        .await
        .expect("spawn kian mock-channel-send");
    assert!(
        out.status.success(),
        "cli demo failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let cli_out: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("cli --json output not json ({e}): {stdout}"));
    let event_id = cli_out["event_id"].as_str().expect("event id").to_owned();
    assert_eq!(cli_out["channel"], serde_json::json!("mock-im"));

    // The injected event reached the Router through the subscription
    // engine and was judged (second provider turn).
    let events = wait_for_session(&read_only, router_session(), "cli batch judged", |events| {
        count_kind(events, EventKind::TurnEnd) >= 2
    })
    .await;
    let admitted = events
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("channel_event"))
        })
        .expect("channel input admitted on the router stream");
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&serde_json::json!(event_id)),
        "the CLI-injected event id lands verbatim"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S4 negative path: a gate-failed worker never reaches a delivered
// "done" reply (honesty mapping)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_gate_failed_worker_never_reaches_delivered_done_reply() {
    force_no_proxy();
    let dir = temp_dir("c28-negative");

    // Router: ack, dispatch, then — on the failed report — an honest
    // escalation text WITHOUT any reply/deliver tool call.
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::ToolCall {
                id: "call-spawn-1".into(),
                name: "spawn_worker".into(),
                arguments: serde_json::json!({"task": "ship it without testing"}),
            },
            ScriptedResponse::Text(
                "worker failed the evidence gate; escalating instead of replying done.".into(),
            ),
        ],
        0,
    )
    .await;
    // Worker: talks but never acts — the gate (min_tool_results 1)
    // can never pass.
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![ScriptedResponse::Text("I skipped the work.".into())],
        0,
    )
    .await;
    // A gate the scripted worker can never satisfy.
    let gate = "    evidence_gate:\n      enabled: true\n      checks:\n        - type: min_tool_results\n          n: 1\n      max_retries: 2\n";

    let config = write_s4_config(&dir, &router_base, &worker_base, gate);
    let (base, server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");

    wait_for_session(
        &read_only,
        router_session(),
        "bootstrap judge turn",
        |events| count_kind(events, EventKind::TurnStart) >= 1,
    )
    .await;

    let client = reqwest::Client::new();
    ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "ship it"}}),
    )
    .await;

    // The worker settles honestly: only failed verdicts, a failed
    // thread/completed, a `failed` session end.
    let router_events = wait_for_session(
        &read_only,
        router_session(),
        "router dispatches",
        |events| count_kind(events, EventKind::ThreadStarted) >= 1,
    )
    .await;
    let started = router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .unwrap();
    let worker_session = worker_of(started);
    let worker_events = wait_for_session(
        &read_only,
        worker_session,
        "worker fails honestly",
        |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
                && events
                    .iter()
                    .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        },
    )
    .await;
    for verdict in worker_events
        .iter()
        .filter(|e| e.kind == EventKind::EvidenceVerdict)
    {
        assert_eq!(
            verdict.bytes["passed"],
            serde_json::json!(false),
            "no verdict may pass: {verdict:?}"
        );
    }
    assert!(
        worker_events
            .iter()
            .any(|e| e.kind == EventKind::EvidenceVerdict),
        "the gate failure is on the record"
    );
    let completed = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .unwrap();
    assert_eq!(completed.bytes["status"], serde_json::json!("failed"));
    let ended = worker_events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .unwrap();
    assert!(
        matches!(
            ended.kind,
            EventKind::SessionEnded {
                reason: EndReason::Failed
            }
        ),
        "a gate-failed worker settles failed, never completed: {ended:?}"
    );

    // The Router consumed the failed report and finished its final
    // turn (3 provider turns) without any channel output.
    let router_events = wait_for_session(
        &read_only,
        router_session(),
        "router final turn done",
        |events| count_kind(events, EventKind::TurnEnd) >= 3,
    )
    .await;
    let report = router_events
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/status") == Some(&serde_json::json!("failed"))
        })
        .expect("the failed report reached the router inbox");
    assert_eq!(
        report.bytes.pointer("/content/thread_id"),
        completed.bytes.pointer("/thread_id")
    );

    // Honesty mapping: no delivered "done" reply anywhere.
    assert_eq!(
        count_kind(&router_events, EventKind::ChannelDelivered),
        0,
        "a gate-failed worker must never reach a delivered reply"
    );
    let runtime = server
        .superagent_runtime()
        .expect("superagent runtime assembled");
    let mock = runtime.mock_channel("mock-im").expect("mock-im channel");
    assert!(
        mock.deliveries().is_empty(),
        "nothing was delivered on the channel"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
