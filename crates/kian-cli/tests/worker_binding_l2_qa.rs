//! (R11-05 / kian-125) Independent QA L2 probes for the durable
//! execution-worker binding (ADR-020) — the real `kian serve` binary
//! plus real `kiand` daemons, per the spawn recipe documented in
//! `worker_binding_l2.rs` (loopback-only, `NO_PROXY`, observable-fact
//! waits, no sleep races). The harness is QA-authored; the scenarios
//! are deliberately **adversarial**: every binding names the SECOND
//! configured endpoint, so a default-to-first (or first-wins) bug in
//! the routing turns these tests red:
//!
//! 1. `qa_binding_names_the_second_endpoint_and_routes_only_there` —
//!    two bound sessions, one to each worker, proven by distinct
//!    endpoint-side effects: each marker file exists ONLY on its own
//!    daemon's filesystem view (I2), the wrapper gains exactly one
//!    additive key, and both streams' `thread/started` carry the same
//!    binding and identical task bytes (I1/I6).
//! 2. `qa_kill9_of_the_second_bound_daemon_redrives_same_worker_once`
//!    — `kill -9` the daemon of the SECOND endpoint mid-command (the
//!    queue row observably `inflight`), restart it, and the row
//!    re-drives on the SAME worker id with the binding unchanged, the
//!    session continues to `thread/completed`, the side effect lands
//!    exactly once, and the FIRST daemon's filesystem is never touched
//!    (I4, ADR-007 command-id correlation, no silent failover).

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_exec::tool::command_id_for_tool_call;
use kian_store::EventStore;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const ROUTER_SESSION: &str = "77777777-1212-3434-5656-787878787878";
const HEARTBEAT_MS: u64 = 50;
/// Config order: qa-w1 is FIRST, qa-w2 is SECOND. Every binding below
/// names qa-w2 unless it is deliberately covering qa-w1 too.
const W1: &str = "qa-w1";
const W2: &str = "qa-w2";

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

// ---------------------------------------------------------------------------
// Daemons + server (the documented recipe)
// ---------------------------------------------------------------------------

fn kiand_bin() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_kiand") {
        return path.into();
    }
    let mut candidates = Vec::new();
    if let Some(profile) = option_env!("CARGO_TARGET_TMPDIR")
        .map(Path::new)
        .and_then(Path::parent)
    {
        candidates.push(profile.join("kiand"));
    }
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(manifest_dir);
        candidates.push(manifest.join("../../target/debug/kiand"));
    }
    candidates
        .iter()
        .find(|p| p.exists())
        .cloned()
        .unwrap_or_else(|| panic!("kiand binary not found; searched {candidates:?}"))
}

fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    port
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(listen: &str, dir: &Path) -> Daemon {
        let mut command = Command::new(kiand_bin());
        command
            .args([
                "--listen",
                listen,
                "--heartbeat-ms",
                &HEARTBEAT_MS.to_string(),
                "--exec-timeout-ms",
                "60000",
            ])
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("KIAND_DEDUPE", "0")
            .env("no_proxy", "127.0.0.1,localhost")
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        Daemon {
            child: command.spawn().expect("spawn kiand"),
        }
    }

    async fn wait_listening(&mut self) {
        let stdout = self.child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match timeout(Duration::from_secs(15), lines.next_line()).await {
                Ok(Ok(Some(line))) => {
                    if line.contains(" listening on ") {
                        return;
                    }
                }
                Ok(Ok(None)) => panic!("kiand exited before printing its listening line"),
                Ok(Err(e)) => panic!("cannot read kiand stdout: {e}"),
                Err(_) => panic!("kiand did not print its listening line within 15s"),
            }
        }
    }

    async fn kill9(&mut self) {
        self.child.kill().await.expect("kill -9 kiand");
        let _ = self.child.wait().await;
    }
}

async fn daemon_on(listen: &str, dir: &Path) -> Daemon {
    let mut daemon = Daemon::spawn(listen, dir);
    daemon.wait_listening().await;
    daemon
}

fn spawn_kian_serve(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_DEDUPE", "0")
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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
    path: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}{path}"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

// ---------------------------------------------------------------------------
// The task-routed worker mock (a request routes by its task tag)
// ---------------------------------------------------------------------------

struct Route {
    tag: &'static str,
    script: Vec<ScriptedResponse>,
}

struct RouteState {
    tag: String,
    requests: Arc<AtomicUsize>,
    script: Vec<ScriptedResponse>,
}

async fn completions_routed(
    State(state): State<Arc<TaskRouted>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let text = String::from_utf8_lossy(&body).to_string();
    let route = state
        .routes
        .iter()
        .find(|r| text.contains(&r.tag))
        .unwrap_or(&state.routes[0]);
    let n = route.requests.fetch_add(1, Ordering::SeqCst) + 1;
    let body = match route
        .script
        .get(n - 1)
        .unwrap_or(route.script.last().expect("non-empty script"))
    {
        ScriptedResponse::Text(text) => common::sse_completion_body(text),
        ScriptedResponse::ToolCall {
            id,
            name,
            arguments,
        } => common::sse_tool_call_body(
            id,
            name,
            &serde_json::to_string(arguments).expect("tool arguments serialize"),
        ),
        ScriptedResponse::DieMidStream => {
            common::sse_completion_body("die-mid-stream is unused here")
        }
    };
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        "content-type",
        axum::http::HeaderValue::from_static("text/event-stream"),
    );
    response
}

struct TaskRouted {
    routes: Vec<RouteState>,
}

async fn spawn_task_routed_provider(routes: Vec<Route>) -> String {
    let mut states = Vec::new();
    for route in routes {
        states.push(RouteState {
            tag: route.tag.to_string(),
            requests: Arc::new(AtomicUsize::new(0)),
            script: route.script,
        });
    }
    let app = Router::new()
        .route("/chat/completions", post(completions_routed))
        .with_state(Arc::new(TaskRouted { routes: states }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind task-routed mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serve");
    });
    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// Config + fixture + observation helpers
// ---------------------------------------------------------------------------

fn write_qa_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    serve_port: u16,
    port_w1: u16,
    port_w2: u16,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:{serve_port}"
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
    - channel: mock-im
      match: always
      route: router
  channels:
    mock-im:
      type: mock-im
endpoints:
  - id: {W1}
    addr: "127.0.0.1:{port_w1}"
    description: "the first QA daemon"
    queue_timeout_ms: 60000
    offline_after_ms: 30000
  - id: {W2}
    addr: "127.0.0.1:{port_w2}"
    description: "the second QA daemon"
    queue_timeout_ms: 60000
    offline_after_ms: 30000
providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent", "remote-exec"]
drain:
  max_iterations: 8
  lease_ttl_ms: 30000
  heartbeat_interval_ms: 10000
tools:
  root: "{dir}"
  exec_timeout_ms: 60000
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
    std::fs::write(&path, yaml).expect("write qa l2 config");
    path
}

struct Fixture {
    base: String,
    store: EventStore,
    pool: sqlx::sqlite::SqlitePool,
}

async fn open_fixture(base: &str, dir: &Path) -> Fixture {
    let store = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(dir.join("kian.sqlite"))
                .read_only(true),
        )
        .await
        .expect("read-only pool");
    Fixture {
        base: base.to_string(),
        store,
        pool,
    }
}

async fn wait_for_session(
    fx: &Fixture,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    for _ in 0..3_000 {
        let events = fx.store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = fx.store.rebuild(session).await.expect("rebuild session");
    panic!("timed out waiting for: {desc}; stream: {events:?}");
}

async fn router_thread_started(fx: &Fixture) -> Vec<serde_json::Value> {
    let events = fx.store.rebuild(router_session()).await.expect("rebuild");
    events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .collect()
}

async fn nth_spawn(fx: &Fixture, n: usize) -> (SessionId, String, String) {
    for _ in 0..3_000 {
        let started = router_thread_started(fx).await;
        if started.len() > n {
            let payload = &started[n];
            let worker = SessionId(
                uuid::Uuid::parse_str(payload["worker"].as_str().unwrap())
                    .expect("worker session uuid"),
            );
            return (
                worker,
                payload["thread_id"].as_str().unwrap().to_string(),
                payload["exec_worker"]
                    .as_str()
                    .unwrap_or("<none>")
                    .to_string(),
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for spawn #{n} on the router stream");
}

fn worker_started_bytes(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .expect("the worker stream carries thread/started")
}

fn first_wrapper(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .and_then(|e| e.bytes.get("content"))
        .cloned()
        .expect("a first input/admitted with content")
}

async fn wait_for_file(path: &Path, timeout_ms: u64) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Ok(content) = std::fs::read_to_string(path) {
            return content;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for marker file {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn exec_queue_status(fx: &Fixture, command_id: uuid::Uuid) -> Option<(String, String)> {
    let text = command_id.to_string();
    sqlx::query_as::<_, (String, String)>(
        "SELECT endpoint_id, status FROM exec_queue WHERE command_id = ?",
    )
    .bind(&text)
    .fetch_optional(&fx.pool)
    .await
    .expect("read exec_queue")
}

async fn send_channel_event(client: &reqwest::Client, fx: &Fixture, text: &str) {
    rpc(
        client,
        &fx.base,
        "/ops/rpc",
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": text}}),
    )
    .await;
}

async fn wait_router_bootstrapped(fx: &Fixture) {
    wait_for_session(
        fx,
        router_session(),
        "router bootstrap answered",
        |events| events.iter().any(|e| e.kind == EventKind::AssistantMessage),
    )
    .await;
}

fn tool_call(id: &str, name: &str, arguments: serde_json::Value) -> ScriptedResponse {
    ScriptedResponse::ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}

fn text(message: &str) -> ScriptedResponse {
    ScriptedResponse::Text(message.into())
}

fn exec_call(id: &str, shell_command: &str) -> ScriptedResponse {
    tool_call(
        id,
        "exec",
        serde_json::json!({
            "argv": ["/bin/sh", "-c", shell_command],
            "timeout_ms": 30000,
        }),
    )
}

fn spawn_call(id: &str, task: &str, exec_worker: &str) -> ScriptedResponse {
    tool_call(
        id,
        "spawn_worker",
        serde_json::json!({"task": task, "exec_worker": exec_worker}),
    )
}

/// Waits for the router's judge turn to settle on a final assistant
/// message (so the next channel event is judged on its own turn).
async fn wait_turn_settled(fx: &Fixture, message: &str) {
    wait_for_session(fx, router_session(), "judge turn settled", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::AssistantMessage
                && e.bytes.pointer("/content/text") == Some(&serde_json::json!(message))
        })
    })
    .await;
}

// ---------------------------------------------------------------------------
// QA leg 1 — the binding names the SECOND endpoint; routing by side effect
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qa_binding_names_the_second_endpoint_and_routes_only_there() {
    force_no_proxy();
    let dir = temp_dir("qa-l2-routing");
    let dir_w1 = dir.join("daemon-w1");
    let dir_w2 = dir.join("daemon-w2");
    std::fs::create_dir_all(&dir_w1).expect("dir w1");
    std::fs::create_dir_all(&dir_w2).expect("dir w2");

    let port_w1 = free_port();
    let port_w2 = free_port();
    let serve_port = free_port();
    let mut daemon_w1 = daemon_on(&format!("127.0.0.1:{port_w1}"), &dir_w1).await;
    let mut daemon_w2 = daemon_on(&format!("127.0.0.1:{port_w2}"), &dir_w2).await;

    // The router binds the FIRST job to the SECOND endpoint and the
    // second job to the first endpoint — both orderings proven.
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            spawn_call(
                "qa-spawn-2",
                "qa-tag-job2: run the second-endpoint check",
                W2,
            ),
            text("job2 dispatched"),
            spawn_call(
                "qa-spawn-1",
                "qa-tag-job1: run the first-endpoint check",
                W1,
            ),
            text("job1 dispatched"),
        ],
        0,
    )
    .await;
    let worker_base = spawn_task_routed_provider(vec![
        Route {
            tag: "qa-tag-job2",
            script: vec![
                exec_call("qa-exec-2", "echo second > qa_marker_second.txt"),
                text("job2 done"),
            ],
        },
        Route {
            tag: "qa-tag-job1",
            script: vec![
                exec_call("qa-exec-1", "echo first > qa_marker_first.txt"),
                text("job1 done"),
            ],
        },
    ])
    .await;

    let config = write_qa_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        port_w1,
        port_w2,
    );
    let mut serve = {
        let mut child = spawn_kian_serve(&config);
        let base = format!("http://127.0.0.1:{serve_port}");
        assert!(
            wait_ready(&base).await,
            "kian serve never became ready (config: {})",
            config.display()
        );
        child.stdout.take();
        child.stderr.take();
        child
    };
    let fx = open_fixture(&format!("http://127.0.0.1:{serve_port}"), &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the job2 work").await;
    let (session_2, thread_2, worker_2) = nth_spawn(&fx, 0).await;
    assert_eq!(worker_2, W2, "the FIRST spawn binds the SECOND endpoint");
    wait_turn_settled(&fx, "job2 dispatched").await;
    send_channel_event(&client, &fx, "please run the job1 work").await;
    let (session_1, thread_1, worker_1) = nth_spawn(&fx, 1).await;
    assert_eq!(worker_1, W1);
    assert_ne!(session_2, session_1);
    assert_ne!(thread_2, thread_1);

    // Both worker sessions settle and BOTH streams carry the binding
    // with identical task bytes and exactly one additive wrapper key.
    for (session, expected_worker) in [(session_2, W2), (session_1, W1)] {
        let events = wait_for_session(&fx, session, "worker settles", |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
        })
        .await;
        let started = worker_started_bytes(&events);
        assert_eq!(started["exec_worker"].as_str(), Some(expected_worker));
        let wrapper = first_wrapper(&events);
        assert_eq!(
            wrapper.get("exec_worker").and_then(|v| v.as_str()),
            Some(expected_worker),
            "the worker's own first-input wrapper carries the binding: {wrapper}"
        );
        let mut keys: Vec<&str> = wrapper
            .as_object()
            .expect("wrapper object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["exec_worker", "task", "thread_id"],
            "exactly the legacy keys plus exec_worker: {wrapper}"
        );
        // Both sides' task bytes are identical (D4).
        let router_side = router_thread_started(&fx).await;
        let mine = router_side
            .iter()
            .find(|p| p["worker"] == json_str(&session))
            .expect("the router-side lineage for this worker");
        assert_eq!(mine["task"], started["task"], "identical task bytes");
    }

    // The routing proof: each marker exists ONLY on its own daemon.
    wait_for_file(&dir_w2.join("qa_marker_second.txt"), 20_000).await;
    wait_for_file(&dir_w1.join("qa_marker_first.txt"), 20_000).await;
    assert!(
        !dir_w1.join("qa_marker_second.txt").exists(),
        "the second-endpoint exec must NEVER land on the first daemon"
    );
    assert!(
        !dir_w2.join("qa_marker_first.txt").exists(),
        "the first-endpoint exec must NEVER land on the second daemon"
    );

    // The exec queue rows record the bound endpoint ids.
    for (call_id, expected) in [("qa-exec-2", W2), ("qa-exec-1", W1)] {
        let (endpoint, _status) = exec_queue_status(&fx, command_id_for_tool_call(call_id))
            .await
            .expect("queue row");
        assert_eq!(endpoint, expected, "queue row endpoint for {call_id}");
    }

    daemon_w1.kill9().await;
    daemon_w2.kill9().await;
    serve.kill().await.expect("kill serve");
    let _ = serve.wait().await;
    let _ = std::fs::remove_dir_all(&dir);
}

fn json_str(session: &SessionId) -> serde_json::Value {
    serde_json::json!(session.0.to_string())
}

// ---------------------------------------------------------------------------
// QA leg 2 — kill -9 the SECOND daemon mid-command; re-drive, same worker
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qa_kill9_of_the_second_bound_daemon_redrives_same_worker_once() {
    force_no_proxy();
    let dir = temp_dir("qa-l2-redrive");
    let dir_w1 = dir.join("daemon-w1");
    let dir_w2 = dir.join("daemon-w2");
    std::fs::create_dir_all(&dir_w1).expect("dir w1");
    std::fs::create_dir_all(&dir_w2).expect("dir w2");

    let port_w1 = free_port();
    let port_w2 = free_port();
    let serve_port = free_port();
    let mut daemon_w1 = daemon_on(&format!("127.0.0.1:{port_w1}"), &dir_w1).await;
    let mut daemon_w2 = daemon_on(&format!("127.0.0.1:{port_w2}"), &dir_w2).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            spawn_call("qa-spawn-redrive", "qa-tag-redrive: run the long check", W2),
            text("redrive dispatched"),
        ],
        0,
    )
    .await;
    let worker_base = spawn_task_routed_provider(vec![Route {
        tag: "qa-tag-redrive",
        script: vec![
            // A long-running command whose side effect lands only if
            // the command runs to completion; killed mid-flight, the
            // first execution writes NOTHING, so exactly one `r` line
            // proves exactly one completed execution.
            exec_call("qa-exec-redrive", "sleep 6 && printf r >> qa_redrive.txt"),
            text("redrive done"),
        ],
    }])
    .await;

    let config = write_qa_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        port_w1,
        port_w2,
    );
    let mut serve = {
        let mut child = spawn_kian_serve(&config);
        let base = format!("http://127.0.0.1:{serve_port}");
        assert!(
            wait_ready(&base).await,
            "kian serve never became ready (config: {})",
            config.display()
        );
        child.stdout.take();
        child.stderr.take();
        child
    };
    let fx = open_fixture(&format!("http://127.0.0.1:{serve_port}"), &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the redrive work").await;
    let (session, thread, worker) = nth_spawn(&fx, 0).await;
    assert_eq!(worker, W2, "bound to the SECOND endpoint");

    // Wait until the command is observably IN FLIGHT on the daemon,
    // then kill -9 THAT daemon mid-command.
    let command_id = command_id_for_tool_call("qa-exec-redrive");
    let mut inflight = false;
    for _ in 0..2_400 {
        if let Some((endpoint, status)) = exec_queue_status(&fx, command_id).await {
            assert_eq!(endpoint, W2, "the queued row names the bound worker");
            if status == "inflight" {
                inflight = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(inflight, "the exec row never reached inflight");
    daemon_w2.kill9().await;

    // Restart the SAME worker id at the same address: the row re-drives,
    // the session continues, the binding is unchanged.
    let mut daemon_w2 = daemon_on(&format!("127.0.0.1:{port_w2}"), &dir_w2).await;

    let marker = wait_for_file(&dir_w2.join("qa_redrive.txt"), 90_000).await;
    assert_eq!(
        marker, "r",
        "exactly one completed execution wrote the marker"
    );

    // The session continues to thread/completed with the binding intact.
    let events = wait_for_session(&fx, session, "worker completes after re-drive", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    let started = worker_started_bytes(&events);
    assert_eq!(
        started["exec_worker"].as_str(),
        Some(W2),
        "the binding is unchanged across the daemon kill/restart"
    );
    assert_eq!(
        started["thread_id"].as_str(),
        Some(thread.as_str()),
        "same thread"
    );

    // The queue row settled on the bound endpoint.
    let (_, status) = exec_queue_status(&fx, command_id)
        .await
        .expect("the queue row is durable");
    assert_eq!(status, "done", "the re-driven row completed");

    // The FIRST daemon's filesystem was never touched (no failover).
    assert!(
        !dir_w1.join("qa_redrive.txt").exists(),
        "no silent failover to the first endpoint"
    );

    daemon_w1.kill9().await;
    daemon_w2.kill9().await;
    serve.kill().await.expect("kill serve");
    let _ = serve.wait().await;
    let _ = std::fs::remove_dir_all(&dir);
}
