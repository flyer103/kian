//! iMessage CLI/config/assembly integration tests (CHANNEL-IMESSAGE L2):
//!
//! - a real `kian serve` + sqlite + a scripted bridge mock: one inbound
//!   message admits exactly once; the same `guid` replayed after a real
//!   process restart admits nothing new;
//! - the env-only bridge token is fail-closed at serve assembly;
//! - `kian imessage check` reports the bridge and fails closed without
//!   the token (the read-only preflight).
//!
//! Zero Macs, zero network egress, a fake bridge token only.
//! `NO_PROXY=127.0.0.1,localhost` for the localhost legs (the macOS
//! system-proxy gotcha).

mod common;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use serde_json::{Value, json};

const USER: &str = "+14155551212";
const TOKEN: &str = "e2e-bridge-token-0123456789abcdef";
const ROUTER_SESSION: &str = "5b1f1c4e-4f5f-4a3c-9d1a-2b3c4d5e6f70";

// ---------------------------------------------------------------------------
// The scripted bridge mock.
// ---------------------------------------------------------------------------

struct BridgeMock {
    updates: Mutex<VecDeque<Value>>,
    sends: Mutex<VecDeque<Value>>,
    update_requests: Mutex<u64>,
    since_rowids: Mutex<Vec<u64>>,
}

impl BridgeMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            updates: Mutex::new(VecDeque::new()),
            sends: Mutex::new(VecDeque::new()),
            update_requests: Mutex::new(0),
            since_rowids: Mutex::new(Vec::new()),
        })
    }

    fn push_updates(&self, body: Value) {
        self.updates.lock().unwrap().push_back(body);
    }

    fn update_requests(&self) -> u64 {
        *self.update_requests.lock().unwrap()
    }

    fn since_rowids(&self) -> Vec<u64> {
        self.since_rowids.lock().unwrap().clone()
    }

    async fn serve(self: &Arc<Self>) -> String {
        let app = axum::Router::new()
            .route("/v1/updates", axum::routing::get(updates_endpoint))
            .route("/v1/send", axum::routing::post(send_endpoint))
            .route("/v1/health", axum::routing::get(health_endpoint))
            .with_state(Arc::clone(self));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("mock serve") });
        format!("http://{addr}")
    }
}

fn authorized(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {TOKEN}"))
}

async fn updates_endpoint(
    State(state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    *state.update_requests.lock().unwrap() += 1;
    let since = query
        .get("since_rowid")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    state.since_rowids.lock().unwrap().push(since);
    let scripted = state.updates.lock().unwrap().pop_front();
    let body = scripted.unwrap_or_else(|| {
        json!({
            "protocol": 1, "generation": "gen-e2e", "messages": [],
            "next_rowid": since, "has_more": false, "gap": false
        })
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn send_endpoint(
    State(state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
    Json(_body): Json<Value>,
) -> axum::response::Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    let scripted = state.sends.lock().unwrap().pop_front().unwrap_or_else(|| {
        json!({"protocol": 1, "ok": true, "message_id": "G-SENT", "guid": "G-SENT",
               "rowid": 900, "service": "iMessage", "verified": true, "deduplicated": false})
    });
    (StatusCode::OK, Json(scripted)).into_response()
}

async fn health_endpoint(
    State(_state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "protocol": 1,
            "bridge_version": "0.1.0",
            "instance_id": "e2e-instance",
            "imsg": {"version": "0.11.1", "rpc": true, "private_api": false},
            "database": {"ready": true, "path": "/tmp/chat.db", "generation": "gen-e2e"},
            "automation": "unknown",
            "watch": {"active": true, "last_rowid": 0, "buffered": 0, "participants": [USER]},
            "counters": {"updates_served": 0, "sends": 0, "uncertain_sends": 0}
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Config + process helpers
// ---------------------------------------------------------------------------

fn bridge_message(guid: &str) -> Value {
    json!({
        "rowid": 101,
        "guid": guid,
        "chat_id": 3,
        "chat_guid": format!("iMessage;-;{USER}"),
        "chat_identifier": USER,
        "sender": USER,
        "text": "please summarize",
        "created_at": "2026-11-19T15:31:55Z",
        "created_at_ms": 1_795_102_315_000u64,
        "service": "iMessage",
        "is_from_me": false,
        "is_group": false,
        "is_reaction": false
    })
}

/// A row whose text renders one marker per attachment placeholder while
/// only some of the attachments have landed (chat.db commits the joins one
/// at a time; ADR-070).
fn multi_attachment_message(guid: &str, text: &str, attachments: usize) -> Value {
    let items: Vec<Value> = (0..attachments)
        .map(|index| {
            json!({
                "attachment_id": format!("101:{index}"),
                "filename": format!("IMG_000{index}.heic"),
                "mime_type": "image/heic",
                "transfer_name": format!("IMG_000{index}.heic"),
                "total_bytes": 272_218u64,
                "is_sticker": false
            })
        })
        .collect();
    let mut message = bridge_message(guid);
    message["text"] = json!(text);
    message["attachments"] = json!(items);
    message
}

fn updates_with(message: &Value) -> Value {
    json!({
        "protocol": 1, "generation": "gen-e2e", "messages": [message],
        "next_rowid": 101, "has_more": false, "gap": false
    })
}

fn write_imessage_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    bridge_url: &str,
    port: u16,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:{port}"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
  tools: ["fs_read"]
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
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  router:
    session_id: "{ROUTER_SESSION}"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: imessage
      match: always
      route: router
  channels:
    imessage:
      type: imessage
      imessage:
        bridge_url: "{bridge_url}"
        topics:
          "{USER}": imessage
        service: imessage
        wait_ms: 200
        poll_timeout_ms: 6000
        send_timeout_ms: 30000
providers:
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
    std::fs::write(&path, yaml).expect("write imessage config");
    path
}

fn spawn_kian_serve(config: &Path, token: Option<&str>) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(token) = token {
        command.env("KIAN_IMESSAGE_BRIDGE_TOKEN", token);
    } else {
        command.env_remove("KIAN_IMESSAGE_BRIDGE_TOKEN");
    }
    command.spawn().expect("spawn kian serve")
}

fn run_kian(args: &[&str], token: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(args)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key");
    if let Some(token) = token {
        command.env("KIAN_IMESSAGE_BRIDGE_TOKEN", token);
    } else {
        command.env_remove("KIAN_IMESSAGE_BRIDGE_TOKEN");
    }
    command.output().expect("run kian")
}

fn router_session() -> kian_core::model::SessionId {
    kian_core::model::SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn imessage_admits(events: &[kian_core::events::Event]) -> Vec<&kian_core::events::Event> {
    events
        .iter()
        .filter(|event| {
            event.kind == kian_core::events::EventKind::InputAdmitted
                && event.bytes.pointer("/content/kian/type") == Some(&json!("channel_event"))
                && event.bytes.pointer("/content/kian/channel") == Some(&json!("imessage"))
        })
        .collect()
}

async fn within_fut<T, F, Fut>(secs: u64, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        if let Some(value) = f().await {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn open_read_only_when_ready(dir: &Path) -> kian_store::EventStore {
    let sqlite = dir.join("kian.sqlite");
    within_fut(30, || {
        let sqlite = sqlite.clone();
        async move { sqlite.exists().then_some(()) }
    })
    .await
    .expect("the child server created the sqlite file");
    kian_store::EventStore::open_read_only(&sqlite)
        .await
        .expect("read-only store")
}

async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    probe.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// L2: admission exactly once across replay and restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_one_inbound_admits_exactly_once_across_replay_and_restart() {
    force_no_proxy();
    let dir = temp_dir("imessage-e2e-admit");
    let mock = BridgeMock::new();
    let bridge_url = mock.clone().serve().await;
    let (router_base, _router) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
            ScriptedResponse::Text("batch noted".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let port = free_port().await;
    let config = write_imessage_config(&dir, &router_base, &worker_base, &bridge_url, port);

    // One inbound message; the same guid is replayed later, including
    // after a real process restart.
    let message = bridge_message("G-E2E-1");
    mock.push_updates(updates_with(&message));

    let mut child = spawn_kian_serve(&config, Some(TOKEN));
    let read_only = open_read_only_when_ready(&dir).await;

    let events = within_fut(30, || {
        let store = read_only.clone();
        async move {
            let events = match store.rebuild(router_session()).await {
                Ok(events) => events,
                Err(_) => return None,
            };
            (imessage_admits(&events).len() == 1).then_some(events)
        }
    })
    .await
    .expect("one imessage admission");
    let admitted = imessage_admits(&events)[0];
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&json!("G-E2E-1")),
        "event_id is the Apple guid"
    );
    assert_eq!(
        admitted.bytes.pointer("/content/kian/channel"),
        Some(&json!("imessage"))
    );
    assert_eq!(
        admitted.bytes.pointer("/content/kian/untrusted"),
        Some(&json!(true)),
        "the forced untrusted marker rides the admitted input"
    );

    // Replay the same guid in-run: the ring absorbs it, nothing new.
    mock.push_updates(updates_with(&message));
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let events = read_only.rebuild(router_session()).await.expect("rebuild");
    assert_eq!(
        imessage_admits(&events).len(),
        1,
        "a replay admits nothing new"
    );

    // A real restart (kill -9 semantics), then the same guid once more:
    // the DURABLE `event_id` guard — not the in-memory ring — is what
    // keeps the admission count at one.
    child.kill().expect("kill serve");
    let _ = child.wait();
    mock.push_updates(updates_with(&message));
    let before = mock.update_requests();
    assert!(before > 0, "the pre-kill child polled at least once");
    let mut child = spawn_kian_serve(&config, Some(TOKEN));
    within_fut(30, || {
        let mock = mock.clone();
        async move {
            let count = mock.update_requests();
            (count > before).then_some(count - before)
        }
    })
    .await
    .expect("the restarted process polled the bridge");
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let events = read_only.rebuild(router_session()).await.expect("rebuild");
    assert_eq!(
        imessage_admits(&events).len(),
        1,
        "a guid replayed after a real restart admits nothing new"
    );
    child.kill().expect("kill serve");
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// L2: a row settles before it is admitted
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_a_multi_attachment_row_waits_for_its_late_attachments() {
    force_no_proxy();
    let dir = temp_dir("imessage-e2e-settle");
    let mock = BridgeMock::new();
    let bridge_url = mock.clone().serve().await;
    let (router_base, _router) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let port = free_port().await;
    let config = write_imessage_config(&dir, &router_base, &worker_base, &bridge_url, port);

    // The first page renders three placeholders but carries one
    // attachment; the second page (read after the cursor parked before
    // the row) carries all three.
    mock.push_updates(updates_with(&multi_attachment_message(
        "G-MULTI-1",
        "[image][attachment][attachment]",
        1,
    )));
    mock.push_updates(updates_with(&multi_attachment_message(
        "G-MULTI-1",
        "[image][image][image]",
        3,
    )));

    let mut child = spawn_kian_serve(&config, Some(TOKEN));
    let read_only = open_read_only_when_ready(&dir).await;

    // The unsettled row must not be admitted: the read position parks
    // just before it, so the next page re-reads row 101.
    within_fut(30, || {
        let mock = mock.clone();
        async move { mock.since_rowids().contains(&100).then_some(()) }
    })
    .await
    .expect("the adapter parks its cursor before the settling row");

    let events = within_fut(30, || {
        let store = read_only.clone();
        async move {
            let events = match store.rebuild(router_session()).await {
                Ok(events) => events,
                Err(_) => return None,
            };
            (imessage_admits(&events).len() == 1).then_some(events)
        }
    })
    .await
    .expect("one imessage admission for the settled row");
    let admitted = imessage_admits(&events)[0];
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&json!("G-MULTI-1")),
    );
    let attachments = admitted
        .bytes
        .pointer("/content/kian/raw/attachments")
        .and_then(Value::as_array)
        .expect("the admitted payload carries its attachments");
    assert_eq!(
        attachments.len(),
        3,
        "the row is admitted once, with every attachment chat.db committed"
    );
    assert_eq!(
        admitted
            .bytes
            .pointer("/content/kian/raw/attachments/2/filename"),
        Some(&json!("IMG_0002.heic")),
    );

    // After the admission the cursor moves past the row: no third read.
    within_fut(30, || {
        let mock = mock.clone();
        async move { mock.since_rowids().contains(&101).then_some(()) }
    })
    .await
    .expect("the cursor advances past the settled row");
    child.kill().expect("kill serve");
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Assembly fail-closed + the read-only preflight
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_refuses_to_start_without_the_bridge_token() {
    force_no_proxy();
    let dir = temp_dir("imessage-e2e-notoken");
    let mock = BridgeMock::new();
    let bridge_url = mock.clone().serve().await;
    let (router_base, _router) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let port = free_port().await;
    let config = write_imessage_config(&dir, &router_base, &worker_base, &bridge_url, port);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env_remove("KIAN_IMESSAGE_BRIDGE_TOKEN")
        .env(common::KEY_ENV, "test-key")
        .output()
        .expect("run serve");
    assert!(
        !output.status.success(),
        "serve must refuse a type: imessage channel without its token"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("KIAN_IMESSAGE_BRIDGE_TOKEN"),
        "the error names the env var: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_reports_the_bridge_and_fails_closed_without_the_token() {
    force_no_proxy();
    let dir = temp_dir("imessage-e2e-check");
    let mock = BridgeMock::new();
    let bridge_url = mock.clone().serve().await;
    let (router_base, _router) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let port = free_port().await;
    let config = write_imessage_config(&dir, &router_base, &worker_base, &bridge_url, port);

    let ok = run_kian(
        &["imessage", "check", "--config", config.to_str().unwrap()],
        Some(TOKEN),
    );
    assert!(
        ok.status.success(),
        "check should pass against a healthy mock bridge: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let stdout = String::from_utf8_lossy(&ok.stdout);
    assert!(stdout.contains("iMessage channel `imessage`"), "{stdout}");
    assert!(stdout.contains("OK"), "{stdout}");

    let json = run_kian(
        &[
            "imessage",
            "check",
            "--config",
            config.to_str().unwrap(),
            "--json",
        ],
        Some(TOKEN),
    );
    assert!(json.status.success());
    let report: Value = serde_json::from_slice(&json.stdout).expect("json report");
    assert_eq!(report["ok"], true);
    assert_eq!(report["protocol"], 1);
    assert_eq!(report["database_ready"], true);
    assert_eq!(report["participants"], json!([USER]));
    assert_eq!(report["topics"], json!([USER]));
    // The token never appears in any output.
    assert!(!String::from_utf8_lossy(&json.stdout).contains(TOKEN));

    let missing = run_kian(
        &["imessage", "check", "--config", config.to_str().unwrap()],
        None,
    );
    assert!(
        !missing.status.success(),
        "check must fail closed without the token"
    );
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(stderr.contains("KIAN_IMESSAGE_BRIDGE_TOKEN"), "{stderr}");
    let _ = std::fs::remove_dir_all(&dir);
}
