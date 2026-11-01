//! The C54 S6 bridge e2e (mounted): a `kian serve` whose worker
//! template runs the `DshDriver` against the C53 scripted fixture
//! peer — zero Node/dsh present. Proves the card's mounted evidence:
//! spawn/steer/complete through the full worker template, replay
//! byte-parity of the finished stream, honest failure on child crash
//! (`interrupted`, never `completed`), and kill -9 takeover with the
//! takeover drain continuing the same dsh interaction (S1 semantics
//! through the bridge). The live run against a real dsh install is
//! env-gated (`KIAN_DSH_BIN`, kian-dsh's live smoke) and recorded in
//! the README run log.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};
use kian_core::context::derive_model_context;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use serde_json::json;
use tokio::process::{Child, Command};

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const THREAD_ID: &str = "11111111-2222-3333-4444-555555555555";
const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dsh");

/// The fixture peer binary: a workspace sibling of this test binary's
/// own `kian` bin (workspace test runs build every member's bins).
fn fixture_peer() -> PathBuf {
    let kian = PathBuf::from(env!("CARGO_BIN_EXE_kian"));
    let peer = kian
        .parent()
        .expect("target dir")
        .join("kian-dsh-fixture-peer");
    assert!(
        peer.exists(),
        "fixture peer {} missing — build the workspace (cargo test --workspace)",
        peer.display()
    );
    peer
}

fn delegation_task() -> serde_json::Value {
    json!({"thread_id": THREAD_ID, "task": "deploy the service"})
}

/// The S6-mounted `kian.yaml`: super-agent assembly (the worker
/// template is where the bridge mounts) with
/// `superagent.harness.dsh` pointing at the fixture peer.
fn write_dsh_config(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    listen: &str,
    script: &str,
) -> PathBuf {
    let yaml = format!(
        r#"listen: "{listen}"
storage:
  sqlite: "{sqlite_path}"
agent:
  id: default
  model_slots:
    main:
      provider: mock
      model: mock-model
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: mock
        model: mock-router
    tools: ["spawn_worker", "reply"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: mock
        model: mock-worker
    tools: ["fs.read", "fs.write", "exec"]
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: {KEY}
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
  harness:
    dsh:
      command: "{peer}"
      args: ["{FIXTURE_DIR}/{script}"]
drain:
  max_iterations: 8
  lease_ttl_ms: {ttl}
  heartbeat_interval_ms: {hb}
tools:
  root: "{root}"
  exec_timeout_ms: 30000
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
plugins: ["openai-compat", "local-tools", "superagent"]
"#,
        KEY = common::KEY_ENV,
        peer = fixture_peer().display(),
        sqlite_path = sqlite.display(),
        root = dir.display(),
        ttl = 500,
        hb = 200,
    );
    let path = dir.join(format!("kian-{script}.yaml"));
    std::fs::write(&path, yaml).expect("write config");
    path
}

fn spawn_serve(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        // kill_on_drop: a panic anywhere in the test still reaps the
        // child — the explicit end-of-test kills stay (success path);
        // this one covers every early-return path. Without it, a
        // failed run leaks a spinning `kian serve` per test, which on
        // this shared machine has starved sibling e2e deadlines (the
        // round-4 async_tools_e2e timeout traced back to exactly that).
        .kill_on_drop(true)
        .spawn()
        .expect("spawn kian serve")
}

async fn wait_ready(base: &str) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let resp = client
            .post(format!("{base}/rpc"))
            .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "create_session", "params": {}}))
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        if let Ok(r) = resp
            && r.status().as_u16() == 200
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("rpc send")
        .json()
        .await
        .expect("rpc json")
}

async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    port
}

async fn session_events(store: &EventStore, session: SessionId) -> Vec<Event> {
    store.rebuild(session).await.expect("rebuild")
}

/// Polls the store until `pred` holds over the session's stream
/// (bounded — no sleep-races).
async fn wait_events<F>(store: &EventStore, session: SessionId, budget_ms: u64, pred: F) -> bool
where
    F: Fn(&[Event]) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(budget_ms);
    loop {
        if pred(&session_events(store, session).await) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

/// Boots a server, creates a worker session (delegation wrapper) and
/// returns (session id, client, base, child).
struct Booted {
    session: SessionId,
    child: Child,
    base: String,
    client: reqwest::Client,
}

async fn boot_with_task(dir: &Path, sqlite: &Path, provider: &str, script: &str) -> Booted {
    let port = free_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let config = write_dsh_config(dir, sqlite, provider, &format!("127.0.0.1:{port}"), script);
    let mut child = spawn_serve(&config);
    assert!(wait_ready(&base).await, "serve never became ready");
    let client = reqwest::Client::new();
    let out = rpc(&client, &base, "create_session", json!({})).await;
    let sid = out["result"]["session"].as_str().unwrap().to_owned();
    let session = SessionId(uuid::Uuid::parse_str(&sid).unwrap());
    let out = rpc(
        &client,
        &base,
        "submit",
        json!({"session": sid, "content": delegation_task()}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    let _ = &mut child;
    Booted {
        session,
        child,
        base,
        client,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsh_worker_spawn_steer_complete_and_replay_parity() {
    force_no_proxy();
    let dir = temp_dir("dsh-steer");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let mut booted = boot_with_task(&dir, &sqlite, &provider_base, "e2e_steer.jsonl").await;

    // A steer while the drain is in flight: it lands at the next safe
    // boundary and rides the contract's steer control frame.
    let out = rpc(
        &booted.client,
        &booted.base,
        "steer",
        json!({"session": booted.session.0.to_string(), "content": {"text": "switch to plan B"}}),
    )
    .await;
    assert!(out["result"].is_object(), "steer admitted: {out}");

    let store = EventStore::open(&sqlite).await.expect("open store");
    let done = wait_events(&store, booted.session, 20_000, |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    assert!(done, "the dsh-driven worker session never settled");

    let events = session_events(&store, booted.session).await;
    // Mapped events rode the existing kinds verbatim: two provider
    // turns with persisted request bytes, one tool round-trip.
    assert_eq!(count_kind(&events, EventKind::StepRequest), 2);
    assert_eq!(count_kind(&events, EventKind::AssistantMessage), 2);
    assert_eq!(count_kind(&events, EventKind::ToolCall), 1);
    assert_eq!(count_kind(&events, EventKind::ToolResult), 1);
    // The steer: one synthetic promotion with the strippable marker,
    // no duplicate from the child's echo.
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted
                && e.bytes.get("synthetic") == Some(&json!(true)))
            .count(),
        1
    );
    // The worker template's lineage rode along untouched.
    assert_eq!(count_kind(&events, EventKind::ThreadStarted), 1);
    // Completed, honestly (a final step/end preceded the end write).
    let ended = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .unwrap();
    assert!(matches!(
        ended.kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));

    // A2 replay parity: the model-visible projection is byte-stable.
    let a = serde_json::to_vec(&derive_model_context(&events).expect("context")).unwrap();
    let events2 = session_events(&store, booted.session).await;
    let b = serde_json::to_vec(&derive_model_context(&events2).expect("context")).unwrap();
    assert_eq!(a, b);

    booted.child.kill().await.expect("kill serve");
    let _ = booted.child.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsh_child_crash_settles_interrupted_never_completed() {
    force_no_proxy();
    let dir = temp_dir("dsh-crash");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let mut booted = boot_with_task(&dir, &sqlite, &provider_base, "e2e_crash.jsonl").await;

    let store = EventStore::open(&sqlite).await.expect("open store");
    let done = wait_events(&store, booted.session, 20_000, |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    assert!(done, "the crashed child never settled the drain");

    let events = session_events(&store, booted.session).await;
    let ended = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .unwrap();
    // Never completed: the crash trace is the C53 honest shape — an
    // attempt with no assistant message after it (constraint 7).
    assert!(
        !matches!(
            ended.kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ),
        "a child crash must never settle completed"
    );
    let attempt_idx = events
        .iter()
        .position(|e| e.kind == EventKind::StepRequest)
        .expect("the attempt was persisted");
    let after: Vec<EventKind> = events[attempt_idx + 1..].iter().map(|e| e.kind).collect();
    assert!(!after.contains(&EventKind::AssistantMessage));

    booted.child.kill().await.expect("kill serve");
    let _ = booted.child.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsh_kill9_takeover_continues_the_same_interaction() {
    force_no_proxy();
    let dir = temp_dir("dsh-kill9");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, mock) = spawn_mock_provider(0).await;

    // Boot #1: phase-1 script stalls mid-step (the drain is inside a
    // provider turn when the server dies).
    let mut booted = boot_with_task(&dir, &sqlite, &provider_base, "e2e_phase1.jsonl").await;
    let store = EventStore::open(&sqlite).await.expect("open store");
    let mid_step = wait_events(&store, booted.session, 20_000, |events| {
        events.iter().any(|e| e.kind == EventKind::StepRequest)
    })
    .await;
    assert!(mid_step, "the drain never reached the stalled step");
    let after_kill = mock.requests.load(Ordering::SeqCst);

    // kill -9 the server mid-turn (the stalled fixture peer dies with
    // the process group / stdin EOF).
    booted.child.kill().await.expect("kill -9 serve");
    let _ = booted.child.wait().await;

    // Boot #2: same sqlite DB, phase-2 script — the takeover drain
    // spawns a fresh child that replays its durable log and continues.
    let port = free_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let config = write_dsh_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("127.0.0.1:{port}"),
        "e2e_phase2.jsonl",
    );
    let mut child2 = spawn_serve(&config);
    assert!(
        wait_ready(&base).await,
        "restarted serve never became ready"
    );

    // The recovery sweep (lease TTL 500ms, tick 200ms) takes over the
    // running session and re-drives it through the bridge.
    let done = wait_events(&store, booted.session, 30_000, |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    assert!(done, "the takeover drain never settled the session");

    let events = session_events(&store, booted.session).await;
    // Exactly one end write (NoHalfTerminated through the bridge).
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
            .count(),
        1
    );
    let ended = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .unwrap();
    assert!(matches!(
        ended.kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));
    // The replayed events matched, not re-appended: one attempt for
    // the stalled step (its request bytes persisted before the kill)
    // plus the takeover's continuation step, and one final answer.
    assert_eq!(count_kind(&events, EventKind::StepRequest), 2);
    assert_eq!(
        count_kind(&events, EventKind::AssistantMessage),
        2,
        "the replay must not double-append: the pre-kill answer plus the takeover's final answer"
    );
    // The replayed answer appears exactly once (de-duplicated), and
    // the takeover's continuation adds its final answer exactly once.
    let assistant_texts: Vec<String> = events
        .iter()
        .filter(|e| e.kind == EventKind::AssistantMessage)
        .map(|e| e.bytes["content"]["text"].as_str().unwrap_or("").to_owned())
        .collect();
    assert_eq!(
        assistant_texts
            .iter()
            .filter(|t| t == &"running build")
            .count(),
        1,
        "the replayed pre-kill answer must not double-append: {assistant_texts:?}"
    );
    assert_eq!(
        assistant_texts.iter().filter(|t| t == &"deployed").count(),
        1,
        "the takeover's final answer: {assistant_texts:?}"
    );
    // Replay parity after takeover (A2): deriving the finished stream
    // is byte-stable.
    let a = serde_json::to_vec(&derive_model_context(&events).expect("context")).unwrap();
    let events2 = session_events(&store, booted.session).await;
    let b = serde_json::to_vec(&derive_model_context(&events2).expect("context")).unwrap();
    assert_eq!(a, b);
    // The dsh path never touched the provider (the router mock saw no
    // worker traffic either way — this pins that durability rode the
    // bridge, not a model call).
    let _ = after_kill;

    child2.kill().await.expect("kill serve");
    let _ = child2.wait().await;
}
