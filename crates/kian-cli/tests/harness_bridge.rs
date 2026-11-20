//! The mounted bridge e2e: a `kian serve` whose worker template runs the shared
//! `BridgeDriver` over the Claude Code adapter against the scripted `stream-json` peer (zero
//! `claude`, zero credentials). Proves the assembly path end to end: the worker session's
//! mapped events, the absence of `step/request` (projection-only), and an honest end.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use serde_json::json;
use tokio::process::{Child, Command};

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const THREAD_ID: &str = "11111111-2222-3333-4444-555555555555";

fn fixture_peer() -> PathBuf {
    let kian = PathBuf::from(env!("CARGO_BIN_EXE_kian"));
    kian.parent()
        .expect("target dir")
        .join("kian-claude-fixture-peer")
}

fn delegation_task() -> serde_json::Value {
    json!({"thread_id": THREAD_ID, "task": "deploy the service"})
}

/// The mounted `kian.yaml`: the worker template's inner loop is the shared bridge driver over
/// the Claude Code adapter, pointed at the scripted peer.
fn write_config(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    listen: &str,
    script: &str,
    step_timeout_ms: Option<u64>,
) -> PathBuf {
    let step_bound = match step_timeout_ms {
        Some(ms) => format!("      step_timeout_ms: {ms}\n"),
        None => String::new(),
    };
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
    tools: ["fs_read", "fs_write", "exec"]
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
    claude:
      command: "{peer}"
      args: ["{script}"]
{step_bound}drain:
  max_iterations: 8
  lease_ttl_ms: 500
  heartbeat_interval_ms: 200
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
        step_bound = step_bound,
        sqlite_path = sqlite.display(),
        root = dir.display(),
    );
    let path = dir.join("kian.yaml");
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
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "create_session", "params": {}}))
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
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
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
    probe.local_addr().unwrap().port()
}

async fn wait_events<F>(store: &EventStore, session: SessionId, budget_ms: u64, pred: F) -> bool
where
    F: Fn(&[Event]) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(budget_ms);
    loop {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_script(dir: &Path) -> PathBuf {
    let script = dir.join("claude.jsonl");
    std::fs::write(
        &script,
        [
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ]
        .join("\n"),
    )
    .expect("write script");
    script
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_worker_chains_map_and_settle_without_step_request() {
    force_no_proxy();
    let dir = temp_dir("harness-claude-mount");
    let sqlite = dir.join("kian.sqlite");
    let script = write_script(&dir);
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let port = free_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let config = write_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("127.0.0.1:{port}"),
        script.to_str().unwrap(),
        None,
    );
    let child = spawn_serve(&config);
    assert!(wait_ready(&base).await, "serve never became ready");

    let client = reqwest::Client::new();
    let out = rpc(&client, &base, "create_session", json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let session = SessionId(uuid::Uuid::parse_str(&sid).unwrap());
    let out = rpc(
        &client,
        &base,
        "submit",
        json!({"session": sid, "content": delegation_task()}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false, "submit: {out}");

    let store = EventStore::open(&sqlite).await.expect("open store");
    let ended = wait_events(&store, session, 20_000, |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    assert!(ended, "the bridged worker session never settled");

    let events = store.rebuild(session).await.expect("rebuild");
    eprintln!(
        "EVENTS: {:?}",
        events
            .iter()
            .map(|e| (e.kind, e.bytes.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::AssistantMessage))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::TurnStart))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::TurnEnd))
            .count(),
        1
    );
    assert!(
        events.iter().all(|e| e.kind != EventKind::StepRequest),
        "projection-only: no step/request may appear"
    );
    // The session was created through the API's plain `create_session`, not the spawn seam, so
    // its `session/created` stays the historical `{}` (the marker test lives at the spawn seam).
    let created = events
        .iter()
        .find(|e| e.kind == EventKind::SessionCreated)
        .expect("created");
    assert_eq!(created.bytes, json!({}));
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. })),
        "honest end write"
    );
    drop(child);
}

fn write_hold_script(dir: &Path) -> PathBuf {
    let script = dir.join("hold.jsonl");
    std::fs::write(
        &script,
        [
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            // Blocks on stdin: the turn stays open until the server dies.
            r#"{"op":"expect_any"}"#,
        ]
        .join("\n"),
    )
    .expect("write hold script");
    script
}

async fn boot(
    dir: &Path,
    sqlite: &Path,
    provider: &str,
    script: &Path,
) -> (SessionId, Child, String, reqwest::Client) {
    let port = free_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let config = write_config(
        dir,
        sqlite,
        provider,
        &format!("127.0.0.1:{port}"),
        script.to_str().unwrap(),
        None,
    );
    let child = spawn_serve(&config);
    assert!(wait_ready(&base).await, "serve never became ready");
    let client = reqwest::Client::new();
    let out = rpc(&client, &base, "create_session", json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let session = SessionId(uuid::Uuid::parse_str(&sid).unwrap());
    let out = rpc(
        &client,
        &base,
        "submit",
        json!({"session": sid, "content": delegation_task()}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false, "submit: {out}");
    (session, child, base, client)
}

/// A4: kill -9 mid-turn → the takeover ends the bridged session honestly (`failed`, the mapping
/// for a lost turn — never `completed`), with no re-run and exactly one end write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kill_9_mid_turn_ends_the_bridged_session_honestly() {
    force_no_proxy();
    let dir = temp_dir("harness-kill9");
    let sqlite = dir.join("kian.sqlite");
    let hold = write_hold_script(&dir);
    let (provider_base, _mock) = spawn_mock_provider(0).await;

    let (session, mut child, _base, _client) = boot(&dir, &sqlite, &provider_base, &hold).await;
    let store = EventStore::open(&sqlite).await.expect("open store");
    let mid_turn = wait_events(&store, session, 20_000, |events| {
        events.iter().any(|e| e.kind == EventKind::TurnStart)
    })
    .await;
    assert!(mid_turn, "the bridged turn never opened");

    child.kill().await.expect("kill -9 serve");
    let _ = child.wait().await;

    // Boot #2: same sqlite; the recovery sweep takes over the running session. The bridge has no
    // undelivered input and no harness resume, so it ends as an honest soft failure.
    let (session2, mut child2, _base2, _client2) = {
        let port = free_port().await;
        let base = format!("http://127.0.0.1:{port}");
        let config = write_config(
            &dir,
            &sqlite,
            &provider_base,
            &format!("127.0.0.1:{port}"),
            hold.to_str().unwrap(),
            None,
        );
        let child = spawn_serve(&config);
        assert!(
            wait_ready(&base).await,
            "restarted serve never became ready"
        );
        (session, child, base, reqwest::Client::new())
    };

    let done = wait_events(&store, session2, 30_000, |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    assert!(done, "the takeover drain never settled the session");

    let events = store.rebuild(session2).await.expect("rebuild");
    let ends: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .collect();
    assert_eq!(ends.len(), 1, "exactly one end write");
    assert!(matches!(
        ends[0].kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Failed
        }
    ));
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::AssistantMessage)
            .count(),
        0,
        "no fabricated answer after a lost turn"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::TurnStart)
            .count(),
        1,
        "the takeover does not re-open the lost turn"
    );

    child2.kill().await.expect("kill serve");
    let _ = child2.wait().await;
}

/// ADR-044 L2: a mounted harness child that stops talking mid-turn ends the session honestly
/// within the configured open-step bound. Before the bound this parked the drain (and would have
/// held a bound worker's slot) until a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_harness_step_ends_the_session_within_the_bound() {
    force_no_proxy();
    let dir = temp_dir("harness-step-stall");
    let sqlite = dir.join("kian.sqlite");
    let script = dir.join("stall.jsonl");
    std::fs::write(
        &script,
        [
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working"}]}}}"#,
            r#"{"op":"sleep","ms":60000}"#,
        ]
        .join("\n"),
    )
    .expect("write stall script");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let port = free_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let config = write_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("127.0.0.1:{port}"),
        script.to_str().unwrap(),
        // The config floor is 10 s (ADR-044): a too-small bound refuses boot by design.
        Some(10_000),
    );
    let child = spawn_serve(&config);
    assert!(wait_ready(&base).await, "serve never became ready");

    let client = reqwest::Client::new();
    let out = rpc(&client, &base, "create_session", json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let session = SessionId(uuid::Uuid::parse_str(&sid).unwrap());
    let out = rpc(
        &client,
        &base,
        "submit",
        json!({"session": sid, "content": delegation_task()}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false, "submit: {out}");

    let store = EventStore::open(&sqlite).await.expect("open store");
    let ended = wait_events(&store, session, 20_000, |events| {
        matches!(
            events.last().map(|e| &e.kind),
            Some(EventKind::SessionEnded {
                reason: EndReason::Failed
            })
        )
    })
    .await;
    assert!(
        ended,
        "the stalled harness step must end the session honestly as failed"
    );
    let events = store.rebuild(session).await.expect("rebuild");
    assert!(
        events.iter().any(|e| e.kind == EventKind::AssistantMessage),
        "the pre-stall mapped message is kept: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::TurnEnd))
            .count(),
        0,
        "the driver must not fabricate a turn/end on the bound: {events:?}"
    );

    // The process stays healthy after the bound fired.
    let out = rpc(&client, &base, "create_session", json!({})).await;
    assert!(
        out["result"]["session"].as_str().is_some(),
        "server healthy: {out}"
    );
    drop(child);
}
