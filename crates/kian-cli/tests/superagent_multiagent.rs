//! (R15-C5) The `superagent.multiagent` config surface + assembly
//! wiring: absent block ⇒ off (the round's M8 gate — the pre-P1 spawn
//! bytes byte-identical end to end), a typo in the block refuses to
//! boot, malformed bounds refuse with the key named, and the assembly
//! installs the caps + child-stream port only when the block is on.
//!
//! Boot-refusal legs exercise the real `Server::from_config_file`
//! path (fail-closed discipline); the e2e legs drive a spawn through
//! the real server, scripted providers and sqlite store, and read the
//! committed streams through a read-only handle — never the server's
//! internals. All tests run with `NO_PROXY=127.0.0.1,localhost`
//! (AGENTS.md gotcha).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::config::ServeConfig;
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_core::model::{SessionId, Thread};
use kian_store::EventStore;
use kian_superagent::{MultiagentLimits, delegation_content, thread_started_payload};
use serde_json::{Value, json};
use uuid::Uuid;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
/// The worker agent `version:` in the fixture config — the
/// `thread/started` payload's `pinned_version`.
const WORKER_VERSION: u64 = 7;
const TASK: &str = "polish the readme";

fn router_session() -> SessionId {
    SessionId(Uuid::parse_str(ROUTER_SESSION).unwrap())
}

/// A super-agent `kian.yaml` over two scripted mock providers, no
/// endpoints (`endpoints:`/`workers:` empty — the assembly where the
/// multiagent-on leg must still install the child-stream port), with
/// the raw `multiagent:` yaml fragment appended when `multiagent` is
/// `Some`.
fn write_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
    multiagent: Option<&str>,
) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let multiagent = multiagent.unwrap_or("");
    let yaml = format!(
        r#"listen: "127.0.0.1:7342"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
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
    version: {WORKER_VERSION}
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
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
{multiagent}providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 8
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
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
}

/// Boots the assembled server (no listener — the spawn flows through
/// the resident Router's own drain, not the HTTP surface).
async fn boot(config_path: &std::path::Path) -> Arc<Server> {
    Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble superagent server")
            .0,
    )
}

/// The provider key env for the boot-refusal legs (assembly is
/// fail-closed on a missing credential; no server is contacted).
fn set_unused_key_env() {
    // SAFETY: process-global test env, set before any parallel
    // assertion can read it; server.rs's C6 tests use the same
    // variable.
    unsafe { std::env::set_var("KIAN_E2E_UNUSED", "unused") };
}

fn count_kind(events: &[kian_core::events::Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

/// Polls the committed router stream until `pred` holds.
async fn wait_for_router(
    store: &EventStore,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    let session = router_session();
    for _ in 0..1200 {
        let events = store.rebuild(session).await.expect("rebuild router");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// Polls an arbitrary session's committed stream until `pred` holds.
async fn wait_for_session(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    for _ in 0..1200 {
        let events = store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// The router script for a spawn turn: bootstrap ack, one
/// `spawn_worker` call, then a park note.
fn spawn_router_script() -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::Text("bootstrap acknowledged".into()),
        ScriptedResponse::ToolCall {
            id: "call-spawn-1".into(),
            name: "spawn_worker".into(),
            arguments: json!({"task": TASK}),
        },
        ScriptedResponse::Text("dispatched".into()),
    ]
}

/// Drives one spawn end to end (mock-channel input → Router judge →
/// `spawn_worker`), waits for the tool result to commit, and returns
/// (router events, worker session, the result's `output`).
async fn drive_spawn(
    server: &Arc<Server>,
    store: &EventStore,
) -> (Vec<kian_core::events::Event>, SessionId, Value) {
    let client = reqwest::Client::new();
    // Drive via the ops RPC on a throwaway listener (the demo path).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    let base = format!("http://{addr}");
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(
            &json!({"jsonrpc": "2.0", "id": 1, "method": "mock_channel_send",
            "params": {"channel": "mock-im", "payload": {"text": "please polish the readme"}}}),
        )
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);

    // The spawn turn committed: a spawn_worker tool call AND its
    // result (error or ack — the caller asserts which). The
    // `tool/result` payload is `{call_id, output, is_error}`; the
    // scripted call id is the discriminator.
    let events = wait_for_router(store, "spawn tool result", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-spawn-1"
        })
    })
    .await;
    let result = events
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-spawn-1"
        })
        .expect("spawn tool result");
    let output = result.bytes["content"]["output"].clone();
    let is_error = result.bytes["content"]["is_error"].as_bool().unwrap();
    let worker = if is_error {
        // A refusal creates nothing — no worker session exists.
        return (events, SessionId(Uuid::nil()), output);
    } else {
        SessionId(Uuid::parse_str(output["worker"].as_str().expect("worker key")).unwrap())
    };
    (events, worker, output)
}

// ---------------------------------------------------------------------------
// 1. M8: absent block ⇒ off ⇒ the pre-P1 spawn bytes, exactly
// ---------------------------------------------------------------------------

/// **The round's M8 gate** (§8.1): a superagent config without the
/// `multiagent:` block boots and spawns exactly as before P1 — the
/// wrapper JSON, the `thread/started` bytes and the tool result equal
/// the pre-P1 golden forms (the frozen landed builders) exactly, and
/// no new key (`spawn_uid` / `task_id` / `spawn_outcome` /
/// `agent_def*`) appears anywhere in either committed stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_multiagent_block_keeps_spawn_bytes_and_result_identical() {
    force_no_proxy();
    let dir = temp_dir("c5-m8-absent");

    let (router_base, _router_mock) = spawn_scripted_provider(spawn_router_script(), 0).await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("readme polished".into())], 0).await;

    let config = write_config(&dir, &router_base, &worker_base, None);
    let server = boot(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    let (router_events, worker, output) = drive_spawn(&server, &read_only).await;

    // The pre-P1 ack: exactly the landed three keys — no spawn_uid,
    // no task_id, no spawn_outcome.
    let thread_id = Uuid::parse_str(output["thread_id"].as_str().expect("thread_id key")).unwrap();
    assert_eq!(
        output,
        json!({
            "thread_id": thread_id.to_string(),
            "worker": worker.0.to_string(),
            "status": "dispatched",
        }),
        "the feature-off ack is exactly the landed three-key shape"
    );

    // The `thread/started` bytes are exactly the frozen pre-P1
    // payload builder over the observed ids (the golden form).
    let wrapper = delegation_content(thread_id, None, json!(TASK));
    let task_message = json!({"role": "user", "content": wrapper.clone()});
    let expected_started = thread_started_payload(
        &Thread {
            id: thread_id,
            parent_session: router_session(),
            pinned_version: WORKER_VERSION,
        },
        worker,
        &task_message,
    );
    let started: Vec<&kian_core::events::Event> = router_events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .collect();
    assert_eq!(started.len(), 1, "exactly one lineage event");
    assert_eq!(
        started[0].bytes, expected_started,
        "thread/started bytes equal the pre-P1 golden exactly"
    );

    // The wrapper the worker consumed is the same legacy bytes: the
    // worker session's first admitted input carries the task message
    // verbatim.
    let worker_events = wait_for_session(&read_only, worker, "worker first input", |events| {
        count_kind(events, EventKind::InputAdmitted) >= 1
    })
    .await;
    let admitted = worker_events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .expect("worker first input");
    assert_eq!(
        admitted.bytes["content"], wrapper,
        "the worker's first input is the legacy wrapper bytes"
    );

    // No new key anywhere: serialize every committed byte on both
    // streams and sweep for the P1 key spellings.
    for events in [&router_events, &worker_events] {
        let all = serde_json::to_string(events).expect("serialize events");
        for key in [
            "\"spawn_uid\"",
            "\"task_id\"",
            "\"spawn_outcome\"",
            "\"agent_def\"",
            "\"agent_def_version\"",
        ] {
            assert!(
                !all.contains(key),
                "feature-off streams must not carry `{key}`: {all}"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 2. A typo in the block refuses to boot (pin P4's trap)
// ---------------------------------------------------------------------------

/// `multiagent: {timeout_ms: 5}` must refuse to boot — the anchors'
/// wrong spelling (A2, pin P4) is never a silent default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_key_in_the_multiagent_block_refuses_to_boot() {
    force_no_proxy();
    set_unused_key_env();
    let dir = temp_dir("c5-unknown-key");
    let (router_base, _r) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;
    let (worker_base, _w) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;
    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        Some("  multiagent:\n    timeout_ms: 5\n"),
    );
    let err = Server::from_config_file(&config)
        .await
        .err()
        .expect("the wrong spelling must refuse to boot");
    let msg = err.to_string();
    assert!(
        msg.contains("unknown field") && msg.contains("timeout_ms"),
        "the refusal names the unknown key: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 3. Malformed / zero bounds refuse with the key named
// ---------------------------------------------------------------------------

/// `max_depth: 0`, `child_timeout_ms: 0` (validation refusals naming
/// the key) and `max_children: -1` (a type error) all refuse to boot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_or_zero_bounds_refuse_to_boot_with_the_key_named() {
    force_no_proxy();
    set_unused_key_env();
    let (router_base, _r) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;
    let (worker_base, _w) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;

    for (fragment, key) in [
        (
            "  multiagent:\n    max_depth: 0\n",
            "superagent.multiagent.max_depth",
        ),
        (
            "  multiagent:\n    child_timeout_ms: 0\n",
            "superagent.multiagent.child_timeout_ms",
        ),
        // A negative u32 is a type error at parse — fail-closed too.
        ("  multiagent:\n    max_children: -1\n", "max_children"),
    ] {
        let dir = temp_dir("c5-zero-bounds");
        let config = write_config(&dir, &router_base, &worker_base, Some(fragment));
        let err = Server::from_config_file(&config)
            .await
            .err()
            .expect("zero/malformed bounds must refuse to boot");
        let msg = err.to_string();
        assert!(
            msg.contains(key),
            "the refusal must name the key `{key}`: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// 4. The §5.3 defaults
// ---------------------------------------------------------------------------

/// An empty `multiagent:` block enables the feature with the §5.3
/// defaults: four children, one hop, fifteen minutes of inactivity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_defaults_are_four_children_one_hop_and_fifteen_minutes() {
    force_no_proxy();
    let dir = temp_dir("c5-defaults");
    let (router_base, _r) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;
    let (worker_base, _w) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("x".into())], 0).await;
    let config = write_config(&dir, &router_base, &worker_base, Some("  multiagent: {}\n"));
    let loaded = ServeConfig::load(&config).expect("parse config");
    let sa = loaded.superagent.as_ref().expect("superagent section");
    assert_eq!(
        sa.multiagent_config().expect("valid block"),
        Some(MultiagentLimits {
            max_children: 4,
            max_depth: 1,
            child_timeout_ms: 900_000,
        }),
        "§5.3 defaults: 4 children, 1 hop, 15 minutes"
    );
    // Explicit values override the defaults verbatim.
    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        Some("  multiagent:\n    max_children: 2\n    max_depth: 3\n    child_timeout_ms: 1000\n"),
    );
    let loaded = ServeConfig::load(&config).expect("parse config");
    let sa = loaded.superagent.as_ref().expect("superagent section");
    assert_eq!(
        sa.multiagent_config().expect("valid block"),
        Some(MultiagentLimits {
            max_children: 2,
            max_depth: 3,
            child_timeout_ms: 1000,
        }),
        "explicit bounds override the defaults verbatim"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 5. On: the caps + the child-stream port are installed
// ---------------------------------------------------------------------------

/// With the block on, a cap refusal is reachable — and the refusal
/// string proves the child-stream port was installed: the §5.4 chain
/// checks the port (fail-closed) BEFORE the caps, so reaching
/// `max_children` means the port read succeeded. `max_children: 0`
/// (a leaf-only parent, legal per §5.3) makes every spawn refuse
/// with the bound named and nothing created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiagent_on_installs_the_caps_and_the_child_stream_port() {
    force_no_proxy();
    let dir = temp_dir("c5-on-caps");
    let (router_base, _router_mock) = spawn_scripted_provider(spawn_router_script(), 0).await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        Some("  multiagent:\n    max_children: 0\n"),
    );
    let server = boot(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    let (router_events, refused_worker, output) = drive_spawn(&server, &read_only).await;

    // The refusal is the honest cap error naming the bound and count
    // — NOT the fail-closed port refusal (the port check precedes
    // the caps in §5.4's chain, so its absence would have refused
    // with a different message).
    assert_eq!(
        refused_worker,
        SessionId(Uuid::nil()),
        "a refusal creates no worker session"
    );
    let error = output["error"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| output.to_string());
    assert!(
        error.contains("max_children") && error.contains("0"),
        "the refusal names the bound and the count: {error}"
    );
    assert!(
        !error.contains("child-stream port is not installed"),
        "the port must be installed when multiagent is on: {error}"
    );

    // No side effect: no lineage, no child stream created.
    assert_eq!(count_kind(&router_events, EventKind::ThreadStarted), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 6. Off: no child-stream port — feature-off wiring unchanged
// ---------------------------------------------------------------------------

/// With the block absent at the endpoint-less assembly (the exact
/// configuration where the on-leg installs the child-stream port),
/// today's wiring is untouched: the spawn proceeds with today's ack
/// and lineage bytes. A port installed when off could only ever be
/// read by the multiagent branch — which is skipped — so the
/// observable content of "installs nothing new" is that the off-path
/// bytes are exactly today's (the M8 leg asserts the same shapes at
/// this assembly; here the point is the port's absence changes
/// nothing even where the on-leg needs it).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiagent_off_installs_no_child_stream_port() {
    force_no_proxy();
    let dir = temp_dir("c5-off-no-port");
    let (router_base, _router_mock) = spawn_scripted_provider(spawn_router_script(), 0).await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("readme polished".into())], 0).await;

    let config = write_config(&dir, &router_base, &worker_base, None);
    let server = boot(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    let (router_events, worker, output) = drive_spawn(&server, &read_only).await;

    // The off-path spawn proceeds (no fail-closed port refusal is
    // reachable when off) with exactly today's ack bytes.
    let thread_id = Uuid::parse_str(output["thread_id"].as_str().expect("thread_id key")).unwrap();
    assert_eq!(
        output,
        json!({
            "thread_id": thread_id.to_string(),
            "worker": worker.0.to_string(),
            "status": "dispatched",
        }),
        "off-path spawn: today's ack, unchanged"
    );
    // And today's lineage bytes via the frozen builder.
    let wrapper = delegation_content(thread_id, None, json!(TASK));
    let expected_started = thread_started_payload(
        &Thread {
            id: thread_id,
            parent_session: router_session(),
            pinned_version: WORKER_VERSION,
        },
        worker,
        &json!({"role": "user", "content": wrapper}),
    );
    let started: Vec<&kian_core::events::Event> = router_events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .collect();
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].bytes, expected_started);

    let _ = std::fs::remove_dir_all(&dir);
}
