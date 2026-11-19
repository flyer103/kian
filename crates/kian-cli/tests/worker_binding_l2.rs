//! (ADR-020 / R11-04) Real two-daemon L2: the **real `kian serve`
//! binary** (a child process) plus **real `kiand` daemons** (child
//! processes), over loopback only, mock providers in-process — the
//! lifecycle legs the L1 suites cannot see:
//!
//! 1. `two_bound_sessions_route_exec_to_their_own_daemon` — per-worker
//!    routing proven by distinct endpoint-side effects (marker files in
//!    each daemon's own working directory), never by log text (I2).
//! 2. `kill9_of_bound_kiand_redrives_same_worker_and_session_continues`
//!    — the headline reattachment semantic (I4/ADR-007): kill -9 a
//!    bound daemon mid-command, restart it, the queued row re-drives on
//!    the SAME worker id, the session continues, the binding is
//!    unchanged, and the command completes exactly once.
//! 3. `serve_restart_rebuilds_binding_and_next_command_goes_to_same_worker`
//!    — kill -9 `kian serve` mid-drain, restart it, the binding is
//!    re-derived from the stream (the next command still lands on the
//!    same daemon), and no stream byte is rewritten (I1).
//! 4. `legacy_unbound_session_routes_to_first_endpoint_and_never_counts_busy`
//!    — a pre-change (unbound) worker session injected as a durable
//!    inbox row before boot routes to the FIRST endpoint and is not
//!    counted by the 1:1 exclusivity check (I5/D15). Its B1-regression
//!    companion `legacy_unbound_follows_config_order_when_ids_are_not_alphabetical`
//!    runs the same session shape with config order [w-zeta, w-alpha]
//!    (NOT alphabetical): the route must follow the FIRST CONFIGURED
//!    endpoint, never the BTreeMap's alphabetically-first key
//!    (spec §6 / ADR-020 §7).
//! 5. `zero_endpoints_serve_keeps_the_legacy_spawn_surface` — the
//!    feature-off discriminator at L2: no `exec_worker` byte anywhere
//!    (wrapper, payload, spawn result, judge request body) (I5).
//! 6. `async_tools_follow_the_bound_worker` — the D11 carry decision at
//!    L2: the async job row carries the bound endpoint id and the side
//!    effect appears only on the bound daemon's filesystem.
//! 7. `busy_worker_refusal_names_owner_then_other_worker_takes_over` —
//!    the 1:1 exclusivity refusal surfaced to a live Router, then the
//!    honest recovery (choose the other worker) (I3/I7).
//!
//! ## Spawn recipe (for QA reuse, R11-05)
//!
//! Every test runs, from the repo root, with `NO_PROXY=127.0.0.1,localhost`
//! and `KIAN_E2E_API_KEY=test-key` (the macOS system proxy intercepts
//! localhost — AGENTS.md gotcha):
//!
//! ```text
//! # daemon i (one per worker; its OWN temp dir is its cwd, so every
//! # command's relative-path side effect lands in that dir and nowhere
//! # else — the routing discriminator):
//! <target>/kiand --listen 127.0.0.1:<port-i> --heartbeat-ms 50 \
//!         --exec-timeout-ms 60000          # cwd: <dir-i>/
//! # stdout prints "kiand: endpoint <id> listening on <addr>" — poll
//! # for " listening on " before booting the server (no sleep races).
//!
//! # server (the real serve binary, one per test, killed/restarted in
//! # leg 3):
//! <target>/kian serve --config <dir>/kian.yaml   # listen: fixed port
//! # readiness: POST /rpc {"method":"create_session"} answers 200.
//! ```
//!
//! Providers are in-process axum mocks (the `common` fixtures); the
//! worker-side mock routes on the TASK TEXT inside the request body, so
//! two concurrently-draining worker sessions with different task tags
//! never race for script positions. All waits poll observable facts
//! (committed events via a read-only store handle, marker files
//! appearing, sqlite rows landing) — never log text, never sleeps.
//!
//! Queue placement: the durable exec queue is server-side
//! (`exec_queue` inside the server's `kian.sqlite`, ADR-007); each
//! daemon is stateless and gets only its own working directory.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use common::{
    ScriptedResponse, force_no_proxy, spawn_scripted_provider, spawn_scripted_provider_gated_at,
    temp_dir,
};
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_store::EventStore;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const LEGACY_SESSION: &str = "12345678-90ab-cdef-1234-567890abcdef";
const HEARTBEAT_MS: u64 = 50;

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn legacy_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(LEGACY_SESSION).unwrap())
}

// ---------------------------------------------------------------------------
// The real kiand child processes
// ---------------------------------------------------------------------------

struct Daemon {
    child: Child,
}

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
        candidates.push(manifest.join("../../target/release/kiand"));
    }
    candidates
        .iter()
        .find(|p| p.exists())
        .cloned()
        .unwrap_or_else(|| panic!("kiand binary not found; searched {candidates:?}"))
}

/// One unused 127.0.0.1 port (bound then released).
fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    port
}

impl Daemon {
    /// Spawns the real `kiand` on `listen` with `dir` as its working
    /// directory — every exec side effect with a relative path lands in
    /// `dir`, which is exactly how the routing legs discriminate.
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

    /// Waits for the daemon's stdout registration line (its listening
    /// announcement) — the deterministic readiness observable.
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

    /// kill -9 (SIGKILL) and reap.
    async fn kill9(&mut self) {
        self.child.kill().await.expect("kill -9 kiand");
        let _ = self.child.wait().await;
    }
}

/// Spawns a daemon and waits until it is listening.
async fn daemon_on(listen: &str, dir: &Path) -> Daemon {
    let mut daemon = Daemon::spawn(listen, dir);
    daemon.wait_listening().await;
    daemon
}

// ---------------------------------------------------------------------------
// The real `kian serve` child process
// ---------------------------------------------------------------------------

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

/// Waits until the server answers an RPC (readiness probe).
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
// The task-routed worker mock provider
// ---------------------------------------------------------------------------

/// One route: every request whose body contains `tag` gets this script,
/// positionally, with an optional gate on one request number.
struct Route {
    tag: &'static str,
    script: Vec<ScriptedResponse>,
    gate_at: usize,
}

struct RouteState {
    tag: String,
    requests: Arc<AtomicUsize>,
    script: Vec<ScriptedResponse>,
    gate_at: usize,
    released: Arc<AtomicBool>,
}

/// A handle onto one route (its request counter and gate release).
#[derive(Clone)]
struct RouteHandle {
    requests: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
}

impl RouteHandle {
    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

/// A mock openai-compat endpoint that routes each request to the script
/// whose tag appears in the request BODY (the task text rides every
/// message of its session's history, so a session's requests always
/// match its own tag — two concurrent worker sessions never race for
/// script positions). Requests that match no tag get the FIRST script
/// (defensive; tests always tag their tasks).
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
    if route.gate_at != 0 && n == route.gate_at && !route.released.load(Ordering::SeqCst) {
        // Hold (bounded well above any test timeout) until released or
        // the client disconnects.
        for _ in 0..900 {
            if route.released.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
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

/// Spawns the task-routed mock on an ephemeral port; returns its base
/// URL (`http://127.0.0.1:PORT/v1`) and one handle per route, in order.
async fn spawn_task_routed_provider(routes: Vec<Route>) -> (String, Vec<RouteHandle>) {
    assert!(!routes.is_empty(), "at least one route");
    let mut handles = Vec::new();
    let mut states = Vec::new();
    for route in routes {
        let requests = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        handles.push(RouteHandle {
            requests: Arc::clone(&requests),
            released: Arc::clone(&released),
        });
        states.push(RouteState {
            tag: route.tag.to_string(),
            requests,
            script: route.script,
            gate_at: route.gate_at,
            released,
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
    (format!("http://{addr}"), handles)
}

// ---------------------------------------------------------------------------
// Config + fixture
// ---------------------------------------------------------------------------

/// One configured endpoint (id + daemon listen port).
struct EndpointSpec {
    id: &'static str,
    port: u16,
    description: &'static str,
}

/// Writes the super-agent `kian.yaml`: two mock providers, optional
/// endpoints (the worker catalog), optional `superagent.async_tools`.
fn write_l2_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    serve_port: u16,
    endpoints: &[EndpointSpec],
    async_tools: bool,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let async_section = if async_tools {
        "  async_tools: true\n"
    } else {
        ""
    };
    let mut endpoints_yaml = String::new();
    for ep in endpoints {
        endpoints_yaml.push_str(&format!(
            "  - id: {id}\n    addr: \"127.0.0.1:{port}\"\n    description: \"{desc}\"\n    queue_timeout_ms: 60000\n    offline_after_ms: 30000\n",
            id = ep.id,
            port = ep.port,
            desc = ep.description,
        ));
    }
    let remote = if endpoints.is_empty() {
        ""
    } else {
        ", \"remote-exec\""
    };
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
{async_section}  router:
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
{endpoints_yaml}providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"{remote}]
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
        async_section = async_section,
        endpoints_yaml = if endpoints.is_empty() {
            "  []\n".to_string()
        } else {
            endpoints_yaml
        },
        remote = remote,
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write l2 config");
    path
}

/// The observation fixture: the server's committed streams through a
/// read-only store handle, plus a read-only sqlite pool for queue rows.
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

/// Boots the serve child on `port` and returns (child, base) once ready.
async fn boot_serve(config: &Path, port: u16) -> (Child, String) {
    let mut child = spawn_kian_serve(config);
    let base = format!("http://127.0.0.1:{port}");
    assert!(
        wait_ready(&base).await,
        "kian serve never became ready (config: {})",
        config.display()
    );
    child.stdout.take();
    child.stderr.take();
    (child, base)
}

/// Kills the serve child with SIGKILL and reaps it.
async fn kill_serve(child: &mut Child) {
    child.kill().await.expect("kill -9 serve");
    let _ = child.wait().await;
}

// ---------------------------------------------------------------------------
// Observation helpers (committed facts only)
// ---------------------------------------------------------------------------

/// Polls a session's committed stream until `pred` holds.
async fn wait_for_session(
    fx: &Fixture,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    for _ in 0..2_400 {
        let events = fx.store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = fx.store.rebuild(session).await.expect("rebuild session");
    panic!("timed out waiting for: {desc}; stream: {events:?}");
}

/// Dumps a compact view of a stream (for timeout diagnostics).
fn compact(events: &[kian_core::events::Event]) -> String {
    events
        .iter()
        .map(|e| {
            format!(
                "{}:{}",
                e.kind.as_str(),
                serde_json::to_string(&e.bytes).unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n  ")
}

/// The Router stream's `thread/started` events, in order.
async fn router_thread_started(fx: &Fixture) -> Vec<serde_json::Value> {
    let events = fx.store.rebuild(router_session()).await.expect("rebuild");
    events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .collect()
}

/// Waits for the Nth `thread/started` on the Router stream and returns
/// (worker session, thread id, exec_worker) from its payload.
async fn nth_spawn(fx: &Fixture, n: usize) -> (SessionId, String, Option<String>) {
    for _ in 0..2_400 {
        let started = router_thread_started(fx).await;
        if started.len() > n {
            let payload = &started[n];
            let worker = SessionId(
                uuid::Uuid::parse_str(payload["worker"].as_str().unwrap())
                    .expect("worker session uuid"),
            );
            let thread = payload["thread_id"].as_str().unwrap().to_string();
            let exec_worker = payload["exec_worker"].as_str().map(str::to_string);
            return (worker, thread, exec_worker);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = fx.store.rebuild(router_session()).await.expect("rebuild");
    panic!(
        "timed out waiting for spawn #{n} on the router stream; stream:\n  {}",
        compact(&events)
    );
}

/// The session's first `input/admitted` content — the delegation
/// wrapper as committed on the worker's own stream.
fn first_wrapper(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .and_then(|e| e.bytes.get("content"))
        .cloned()
        .expect("a first input/admitted with content")
}

/// The session's own `thread/started` payload bytes (the worker side
/// emits its own copy; D4 says both sides' task bytes are identical).
fn worker_started_bytes(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .expect("the worker stream carries thread/started")
}

/// `ToolResult` events on a stream, serialized (for substring checks).
fn tool_result_strings(events: &[kian_core::events::Event]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e.kind == EventKind::ToolResult)
        .map(|e| serde_json::to_string(&e.bytes).expect("serialize"))
        .collect()
}

/// Polls until `path` exists with content; returns the content.
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

/// Polls until a condition on a file holds (the file may not exist
/// yet); returns the final content or None if it never existed.
async fn poll_file_until(path: &Path, timeout_ms: u64, cond: impl Fn(&str) -> bool) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Ok(content) = std::fs::read_to_string(path)
            && cond(&content)
        {
            return content;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for content of {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One exec queue row: (endpoint_id, status).
async fn exec_queue_row(fx: &Fixture, command_id: uuid::Uuid) -> Option<(String, String)> {
    let text = command_id.to_string();
    sqlx::query_as::<_, (String, String)>(
        "SELECT endpoint_id, status FROM exec_queue WHERE command_id = ?",
    )
    .bind(&text)
    .fetch_optional(&fx.pool)
    .await
    .expect("read exec_queue")
}

/// Sends the mock channel event that triggers a Router judge turn.
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

/// Waits for the Router's bootstrap turn (the resident Router must be
/// up before a channel event is judged deterministically).
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

// ---------------------------------------------------------------------------
// 1. Two workers, two bound sessions — routing by side effect (I2)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_bound_sessions_route_exec_to_their_own_daemon() {
    force_no_proxy();
    let dir = temp_dir("l2-routing");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-alpha",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-alpha: run the alpha check",
                    "exec_worker": "w-alpha",
                }),
            ),
            text("alpha dispatched"),
            tool_call(
                "call-spawn-beta",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-beta: run the beta check",
                    "exec_worker": "w-beta",
                }),
            ),
            text("beta dispatched"),
        ],
        0,
    )
    .await;
    let (worker_base, _handles) = spawn_task_routed_provider(vec![
        Route {
            tag: "job-alpha",
            script: vec![
                exec_call("call-exec-alpha", "echo alpha > alpha.txt"),
                text("alpha done"),
            ],
            gate_at: 0,
        },
        Route {
            tag: "job-beta",
            script: vec![
                exec_call("call-exec-beta", "echo beta > beta.txt"),
                text("beta done"),
            ],
            gate_at: 0,
        },
    ])
    .await;

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the alpha daemon — one filesystem view",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the beta daemon — another filesystem view",
            },
        ],
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the alpha job").await;

    // Both spawns landed, each with its own binding on the Router side.
    let (alpha_session, alpha_thread, alpha_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(alpha_worker.as_deref(), Some("w-alpha"));
    // The judge turn for the first job settled with its final answer
    // (the ack text) before the second event is judged — otherwise the
    // two events merge into one batch and only one spawn happens.
    wait_for_session(
        &fx,
        router_session(),
        "first judge turn settled",
        |events| {
            events.iter().any(|e| {
                e.kind == EventKind::AssistantMessage
                    && e.bytes.pointer("/content/text")
                        == Some(&serde_json::json!("alpha dispatched"))
            })
        },
    )
    .await;
    send_channel_event(&client, &fx, "please run the beta job").await;
    let (beta_session, beta_thread, beta_worker) = nth_spawn(&fx, 1).await;
    assert_eq!(beta_worker.as_deref(), Some("w-beta"));
    assert_ne!(alpha_session, beta_session);
    assert_ne!(alpha_thread, beta_thread);

    // Both worker sessions settle (final answer + thread/completed).
    for (session, desc) in [(alpha_session, "alpha"), (beta_session, "beta")] {
        let events = wait_for_session(&fx, session, "worker settles", |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
        })
        .await;
        // I6: the binding is on BOTH streams, byte-identical bytes for
        // the same spawn (payload key + wrapper key).
        let started = worker_started_bytes(&events);
        assert_eq!(started["exec_worker"].as_str(), Some(desc_bound(desc)));
        let wrapper = first_wrapper(&events);
        assert_eq!(
            wrapper.get("exec_worker").and_then(|v| v.as_str()),
            Some(desc_bound(desc)),
            "the worker's own first-input wrapper carries the binding: {wrapper}"
        );
        // Leg-5 byte rule inside the L2 harness: the wrapper gained
        // EXACTLY ONE additive key over the legacy shape.
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
    }

    // The routing proof — distinct endpoint-side effects: each marker
    // exists ONLY on its own daemon's filesystem view.
    wait_for_file(&dir_a.join("alpha.txt"), 20_000).await;
    wait_for_file(&dir_b.join("beta.txt"), 20_000).await;
    assert!(
        !dir_b.join("alpha.txt").exists(),
        "alpha's exec must never land on the beta daemon"
    );
    assert!(
        !dir_a.join("beta.txt").exists(),
        "beta's exec must never land on the alpha daemon"
    );

    // Both sides' thread/started carry identical task bytes (D4).
    let alpha_started_router = router_thread_started(&fx).await[0].clone();
    let alpha_events = fx
        .store
        .rebuild(alpha_session)
        .await
        .expect("rebuild alpha");
    assert_eq!(
        alpha_started_router["task"],
        worker_started_bytes(&alpha_events)["task"],
        "both sides' thread/started task bytes are identical"
    );
    assert_eq!(
        alpha_started_router["thread_id"],
        worker_started_bytes(&alpha_events)["thread_id"]
    );

    // I6: the spawn tool result echoes the binding.
    let router_events = fx.store.rebuild(router_session()).await.expect("rebuild");
    let results = tool_result_strings(&router_events);
    assert!(
        results
            .iter()
            .any(|r| r.contains("\"exec_worker\":\"w-alpha\"")),
        "spawn result echoes the alpha binding: {results:?}"
    );
    assert!(
        results
            .iter()
            .any(|r| r.contains("\"exec_worker\":\"w-beta\"")),
        "spawn result echoes the beta binding: {results:?}"
    );

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The endpoint id a test's daemon tag binds to (kept as a fn so the
/// mapping between tags and ids lives in exactly one place per test).
fn desc_bound(which: &str) -> &str {
    match which {
        "alpha" => "w-alpha",
        "beta" => "w-beta",
        _ => panic!("unknown side {which}"),
    }
}

// ---------------------------------------------------------------------------
// 2. kill -9 a bound kiand mid-command — re-drive, same worker, no
//    duplicate completion (I4 / ADR-007) — the headline leg
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill9_of_bound_kiand_redrives_same_worker_and_session_continues() {
    force_no_proxy();
    let dir = temp_dir("l2-kill9");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let listen_a = format!("127.0.0.1:{port_a}");
    let mut daemon_a = daemon_on(&listen_a, &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-kill",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-kill: the reattachment check",
                    "exec_worker": "w-alpha",
                }),
            ),
            text("worker dispatched"),
        ],
        0,
    )
    .await;
    // One exec whose two phases are observable on the daemon's
    // filesystem: `started` proves it is mid-command (the kill point),
    // `finished` proves completion — appended, so a duplicate
    // completion would show as a second line. The shell also records
    // its pid: SIGKILL to kiand does not signal the command's children
    // (a same-machine harness artifact — on a real worker machine the
    // command dies with the machine), so the test reaps the orphaned
    // shell to keep the re-drive the only completion.
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![
            exec_call(
                "call-exec-kill",
                "echo $$ > sh.pid; echo started > kill.txt; sleep 8; echo finished >> kill.txt",
            ),
            text("survived the kill"),
        ],
        0,
    )
    .await;

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the doomed-and-restarted daemon",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the other daemon — nothing may fail over here",
            },
        ],
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the kill check").await;

    let (worker, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("w-alpha"));

    // The command is genuinely in flight: phase one is on disk.
    poll_file_until(&dir_a.join("kill.txt"), 20_000, |c| c.contains("started")).await;
    let orphan_sh: u32 = std::fs::read_to_string(dir_a.join("sh.pid"))
        .expect("the shell pid file")
        .trim()
        .parse()
        .expect("a pid");

    // Snapshot the binding before the kill (the immutability witness).
    let pre_kill = fx.store.rebuild(worker).await.expect("rebuild pre-kill");
    let pre_started = worker_started_bytes(&pre_kill);
    assert_eq!(pre_started["exec_worker"].as_str(), Some("w-alpha"));

    // kill -9 the bound daemon mid-command, then the orphaned shell
    // (see the script comment): the machine is gone, the command with it.
    daemon_a.kill9().await;
    let _ = std::process::Command::new("kill")
        .args(["-9", &orphan_sh.to_string()])
        .status()
        .expect("reap the orphaned shell");
    // …and restart it: SAME worker id (same endpoint id, same address),
    // SAME working directory — the reattachment shape.
    daemon_a = daemon_on(&listen_a, &dir_a).await;

    // The queued row re-drives after the reconnect; the session
    // continues to its honest completion.
    let events = wait_for_session(
        &fx,
        worker,
        "worker completes after daemon restart",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
    )
    .await;

    // The command completed exactly once: one `finished` line.
    let kill_txt =
        poll_file_until(&dir_a.join("kill.txt"), 30_000, |c| c.contains("finished")).await;
    assert_eq!(
        kill_txt.matches("finished").count(),
        1,
        "no duplicate completion (ADR-007 command-id correlation): {kill_txt}"
    );
    assert_eq!(
        kill_txt.matches("started").count(),
        1,
        "the re-drive overwrites phase one (at-least-once), it does not stack it: {kill_txt}"
    );

    // No silent failover (I4): the other daemon saw nothing at all.
    assert!(
        !dir_b.join("kill.txt").exists(),
        "the command must never execute on the other worker"
    );

    // ADR-007 correlation: exactly one queue row for the command id,
    // on the bound endpoint, settled done.
    let command_id = kian_exec::tool::command_id_for_tool_call("call-exec-kill");
    let row = exec_queue_row(&fx, command_id).await;
    assert_eq!(
        row,
        Some(("w-alpha".to_string(), "done".to_string())),
        "one row, the bound endpoint, settled done: {row:?}"
    );

    // Exactly one exec tool call/result pair in the session — the
    // re-drive is invisible to the loop (no phantom second result).
    let exec_calls = events
        .iter()
        .filter(|e| e.kind == EventKind::ToolCall && e.bytes["content"]["name"] == "exec")
        .count();
    let exec_results = events
        .iter()
        .filter(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-exec-kill"
        })
        .count();
    assert_eq!(exec_calls, 1, "one exec call: {events:?}");
    assert_eq!(exec_results, 1, "one exec result: {events:?}");
    let result = events
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-exec-kill"
        })
        .expect("the exec result");
    assert_eq!(
        result.bytes["content"]["is_error"],
        serde_json::json!(false),
        "the re-driven command succeeds honestly: {:?}",
        result.bytes["content"]
    );

    // The binding is unchanged (I1): same bytes, still exactly one
    // thread/started, same exec_worker.
    assert_eq!(
        worker_started_bytes(&events),
        pre_started,
        "the binding bytes are untouched by the daemon death/restart"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1
    );
    assert_eq!(
        first_wrapper(&events)
            .get("exec_worker")
            .and_then(|v| v.as_str()),
        Some("w-alpha")
    );

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 3. kill -9 kian serve mid-drain, restart — the binding is re-derived
//    from the stream, no rewrite (I1)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_restart_rebuilds_binding_and_next_command_goes_to_same_worker() {
    force_no_proxy();
    let dir = temp_dir("l2-serve-restart");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-restart",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-restart: the binding rebuild check",
                    "exec_worker": "w-alpha",
                }),
            ),
            text("worker dispatched"),
            text("noted"),
        ],
        0,
    )
    .await;
    // Request 1 runs the first command; request 2 (gated) parks the
    // drain mid-turn — the deterministic kill window. The restarted
    // drain re-issues that turn as request 3, which runs the SECOND
    // command (the same marker via a fresh call id), then settles.
    let (worker_base, worker_mock) = spawn_scripted_provider_gated_at(
        vec![
            exec_call("call-exec-r1", "echo one > restart1.txt"),
            exec_call("call-exec-r2a", "echo two > restart2.txt"),
            exec_call("call-exec-r2b", "echo two > restart2.txt"),
            text("restart survived"),
        ],
        0,
        2, // gate exactly request 2 (the mid-turn park)
    )
    .await;

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the bound daemon — must stay the route",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the other daemon — must never run anything",
            },
        ],
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the restart check").await;

    let (worker, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("w-alpha"));

    // Command one ran on the bound daemon, and its result is committed.
    wait_for_file(&dir_a.join("restart1.txt"), 20_000).await;
    wait_for_session(&fx, worker, "first exec result committed", |events| {
        events.iter().any(|e| e.kind == EventKind::ToolResult)
    })
    .await;

    // Park the drain mid-turn (request 2 gated), then queue a second
    // input — durable, unconsumed: the NoLostWork the restart must keep.
    for _ in 0..2_400 {
        if worker_mock.requests.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        worker_mock.requests.load(Ordering::SeqCst) >= 2,
        "the drain never reached its parked provider turn"
    );
    let out = rpc(
        &client,
        &fx.base,
        "/rpc",
        "queue",
        serde_json::json!({"session": worker.0, "content": "phase two please"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Snapshot the binding (the no-rewrite witness).
    let pre_kill = fx.store.rebuild(worker).await.expect("rebuild pre-kill");
    let pre_started = worker_started_bytes(&pre_kill);
    let pre_wrapper = first_wrapper(&pre_kill);

    // kill -9 the server mid-drain, restart it on the same store.
    kill_serve(&mut serve).await;
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;

    // Release the parked turn: the restarted assembly re-drove the
    // session from the stream — binding re-derived, not re-written.
    worker_mock.released.store(true, Ordering::SeqCst);

    let events = wait_for_session(
        &fx,
        worker,
        "worker completes after serve restart",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
    )
    .await;

    // The SECOND command ran, and it ran on the SAME daemon — the
    // routing decision after the restart comes from the stream.
    wait_for_file(&dir_a.join("restart2.txt"), 20_000).await;
    assert!(
        !dir_b.join("restart1.txt").exists() && !dir_b.join("restart2.txt").exists(),
        "no command may land on the other daemon across the restart"
    );

    // No rewrite/rebind: one thread/started, byte-identical payload,
    // unchanged wrapper.
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1
    );
    assert_eq!(
        worker_started_bytes(&events),
        pre_started,
        "the binding payload is byte-identical across the serve restart"
    );
    assert_eq!(
        first_wrapper(&events),
        pre_wrapper,
        "the first-input wrapper is byte-identical across the serve restart"
    );

    // NoLostWork: the queued phase-two input was consumed.
    assert!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .count()
            >= 2,
        "the queued input survives the restart: {:?}",
        events.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>()
    );

    // Epoch fencing held across the restart (monotone epochs).
    let epochs: Vec<u64> = events.iter().map(|e| e.epoch.0).collect();
    assert!(
        epochs.windows(2).all(|w| w[0] <= w[1]),
        "epochs are monotone across the restart: {epochs:?}"
    );

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4. Legacy unbound worker session — first endpoint, never counted busy
//    (I5 / D15)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_unbound_session_routes_to_first_endpoint_and_never_counts_busy() {
    force_no_proxy();
    let dir = temp_dir("l2-legacy");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-modern",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-modern: the exclusivity probe",
                    "exec_worker": "w-alpha",
                }),
            ),
            text("worker dispatched"),
        ],
        0,
    )
    .await;
    let (worker_base, handles) = spawn_task_routed_provider(vec![
        // The legacy session: exec, then park at request 2 (keeps it
        // LIVE for the exclusivity probe), then settle.
        Route {
            tag: "job-legacy",
            script: vec![
                exec_call("call-exec-legacy", "echo legacy > legacy.txt"),
                tool_call(
                    "call-read-legacy",
                    "fs_read",
                    serde_json::json!({"path": "note.txt"}),
                ),
                text("legacy done"),
            ],
            gate_at: 2,
        },
        Route {
            tag: "job-modern",
            script: vec![
                exec_call("call-exec-modern", "echo modern > modern.txt"),
                text("modern done"),
            ],
            gate_at: 0,
        },
    ])
    .await;
    let legacy_handle = &handles[0];

    // The pre-change fixture: a worker delegation WITHOUT a binding,
    // admitted as a plain durable inbox row BEFORE the server boots —
    // exactly the stream shape a pre-ADR-020 binary would have left.
    // The startup inbox scan wakes it; the drain classifies it from
    // its first input wrapper (legacy unbound).
    {
        let store = EventStore::open(&dir.join("kian.sqlite"))
            .await
            .expect("open store pre-boot");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        store
            .admit(kian_core::model::InputRow {
                id: uuid::Uuid::new_v4(),
                session: legacy_session(),
                payload: serde_json::json!({
                    "thread_id": "0e0e0e0e-1e1e-4e1e-9e1e-2e2e2e2e2e2e",
                    "task": {"text": "job-legacy: the first-endpoint check"},
                }),
                admitted_at: now_ms,
                eligible_at: now_ms,
                idempotency_key: Some("legacy-delegation".into()),
            })
            .await
            .expect("admit legacy row");
    }

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the first endpoint — the legacy route",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the second endpoint — never the legacy route",
            },
        ],
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    // The startup scan woke the legacy session; its exec ran on the
    // FIRST endpoint (side effect, not log text).
    wait_for_file(&dir_a.join("legacy.txt"), 30_000).await;
    assert!(
        !dir_b.join("legacy.txt").exists(),
        "the legacy route is the FIRST endpoint only"
    );

    let legacy_events =
        wait_for_session(&fx, legacy_session(), "legacy thread/started", |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadStarted)
        })
        .await;
    // The legacy session never acquires a binding (D15): its payload
    // and wrapper stay the legacy bytes.
    assert!(
        worker_started_bytes(&legacy_events)
            .get("exec_worker")
            .is_none(),
        "no binding may be synthesized for a legacy session: {:?}",
        worker_started_bytes(&legacy_events)
    );
    assert!(
        first_wrapper(&legacy_events).get("exec_worker").is_none(),
        "the wrapper is never rewritten"
    );

    // The legacy session is parked (live). The exclusivity probe: a
    // fresh spawn on the SAME first-endpoint worker must SUCCEED — a
    // legacy unbound session is not registered as bound (D15).
    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the modern job").await;
    let (modern_session, _thread, modern_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(
        modern_worker.as_deref(),
        Some("w-alpha"),
        "the spawn on the first-endpoint worker succeeded while the legacy session is live"
    );

    // And the modern (bound) session's exec also lands on the same
    // first endpoint — its own binding, honored.
    wait_for_file(&dir_a.join("modern.txt"), 20_000).await;
    assert!(!dir_b.join("modern.txt").exists());

    // Release the legacy park; it settles honestly.
    legacy_handle.release();
    wait_for_session(&fx, legacy_session(), "legacy session settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    wait_for_session(&fx, modern_session, "modern session settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4b. (B1 regression) legacy unbound, config order NOT alphabetical
// ---------------------------------------------------------------------------

/// (B1 regression, L2 leg) Config order [`w-zeta`, `w-alpha`] —
/// deliberately NOT alphabetical. A pre-change (unbound) worker
/// delegation must run on the FIRST **CONFIGURED** endpoint
/// (`w-zeta` = daemon A), reproducing the pre-change
/// `CombinedTools` first-wins outcome. Before the fix the driver fed
/// `select_side` the `worker_sides` BTreeMap's key order
/// ([`w-alpha`, `w-zeta`]) and the session silently ran on daemon B
/// while the audit line claimed "first endpoint" — the reviewer's B1
/// witness, pinned here by a side effect on the real daemon
/// filesystem.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_unbound_follows_config_order_when_ids_are_not_alphabetical() {
    force_no_proxy();
    let dir = temp_dir("l2-legacy-order");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) =
        spawn_scripted_provider(vec![text("bootstrap acknowledged")], 0).await;
    // The legacy session's only turn: one exec (the side effect under
    // test), then settle — no exclusivity probe needed here.
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![
            exec_call("call-exec-legacy", "echo legacy > legacy.txt"),
            text("legacy done"),
        ],
        0,
    )
    .await;

    // The pre-change stream shape (same fixture as test 4): a worker
    // delegation WITHOUT a binding, admitted as a durable inbox row
    // BEFORE the server boots; the startup scan wakes it and the
    // drain classifies it legacy unbound from its first wrapper.
    {
        let store = EventStore::open(&dir.join("kian.sqlite"))
            .await
            .expect("open store pre-boot");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        store
            .admit(kian_core::model::InputRow {
                id: uuid::Uuid::new_v4(),
                session: legacy_session(),
                payload: serde_json::json!({
                    "thread_id": "0e0e0e0e-1e1e-4e1e-9e1e-2e2e2e2e2e2e",
                    "task": {"text": "job-legacy: the config-order check"},
                }),
                admitted_at: now_ms,
                eligible_at: now_ms,
                idempotency_key: Some("legacy-delegation-order".into()),
            })
            .await
            .expect("admit legacy row");
    }

    // CONFIG ORDER [w-zeta, w-alpha]: `w-alpha` sorts first but is
    // the SECOND configured endpoint.
    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-zeta",
                port: port_a,
                description: "the first configured endpoint — the legacy route",
            },
            EndpointSpec {
                id: "w-alpha",
                port: port_b,
                description: "sorts first alphabetically — never the legacy route here",
            },
        ],
        false,
    );
    let (mut serve, _base) = boot_serve(&config, serve_port).await;

    // The exec side effect lands on daemon A (w-zeta, the FIRST
    // CONFIGURED endpoint) — never on daemon B (w-alpha).
    wait_for_file(&dir_a.join("legacy.txt"), 30_000).await;
    assert!(
        !dir_b.join("legacy.txt").exists(),
        "the legacy route is the first CONFIGURED endpoint (w-zeta), \
         not the alphabetically-first id (w-alpha)"
    );

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 5. Zero endpoints — the feature-off discriminator (I5)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_endpoints_serve_keeps_the_legacy_spawn_surface() {
    force_no_proxy();
    let dir = temp_dir("l2-zero-endpoints");

    let serve_port = free_port();
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            // The pre-change spawn call shape: NO exec_worker argument.
            tool_call(
                "call-spawn-plain",
                "spawn_worker",
                serde_json::json!({"task": "job-plain: the legacy spawn shape"}),
            ),
            text("worker dispatched"),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(vec![text("plain done")], 0).await;

    let config = write_l2_config(&dir, &router_base, &worker_base, serve_port, &[], false);
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the plain job").await;

    let (worker, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert!(
        exec_worker.is_none(),
        "zero endpoints ⇒ no binding anywhere: {exec_worker:?}"
    );

    let events = wait_for_session(&fx, worker, "plain worker settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;

    // The wrapper is exactly the legacy key set — no additive key.
    let wrapper = first_wrapper(&events);
    let mut keys: Vec<&str> = wrapper
        .as_object()
        .expect("wrapper object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["task", "thread_id"],
        "legacy wrapper bytes: {wrapper}"
    );
    assert!(
        worker_started_bytes(&events).get("exec_worker").is_none(),
        "the payload stays legacy"
    );

    // The judge request body (persist-what-you-send) carries NO
    // exec_worker byte — prompt and spawn schema are unchanged.
    let router_events = fx.store.rebuild(router_session()).await.expect("rebuild");
    for event in router_events
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
    {
        let body = serde_json::to_string(&event.bytes["body"]).expect("serialize");
        assert!(
            !body.contains("exec_worker"),
            "zero-endpoints request bytes must not mention exec_worker: {body}"
        );
    }
    // The spawn tool result carries no binding echo.
    for result in tool_result_strings(&router_events) {
        assert!(
            !result.contains("exec_worker"),
            "zero-endpoints spawn result must not echo a binding: {result}"
        );
    }

    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 6. async_tools follows the bound worker (D11 carry)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_tools_follow_the_bound_worker() {
    force_no_proxy();
    let dir = temp_dir("l2-async-bound");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-async",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-async: the bound async check",
                    "exec_worker": "w-beta",
                }),
            ),
            text("worker dispatched"),
        ],
        0,
    )
    .await;
    // The drain must still be LIVE when the job settles: the completion
    // bridge honestly drops a completion whose target session is
    // terminal (documented no-op). Gate the third provider turn (the
    // would-be final answer) so the drain parks while the job runs —
    // the async_tools_e2e discipline, deterministic without sleeps.
    let (worker_base, worker_mock) = spawn_scripted_provider_gated_at(
        vec![
            tool_call(
                "call-start-async",
                "start",
                serde_json::json!({
                    "argv": ["/bin/sh", "-c", "sleep 2; echo async-done > async.txt"],
                    "timeout_ms": 30000,
                }),
            ),
            tool_call(
                "call-read-async",
                "fs_read",
                serde_json::json!({"path": "note.txt"}),
            ),
            // GATED (request 3): the drain parks here while the job runs.
            text("async completion seen"),
        ],
        0,
        3,
    )
    .await;

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the first endpoint — NOT this session's worker",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the bound endpoint for the async job",
            },
        ],
        true,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the async job").await;

    let (worker, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("w-beta"));

    // The async job row carries the BOUND endpoint id (the carry).
    let job_id = kian_exec::tool::command_id_for_tool_call("call-start-async");
    let mut row = None;
    for _ in 0..2_400 {
        row = sqlx::query_as::<_, (String,)>("SELECT endpoint_id FROM async_jobs WHERE job_id = ?")
            .bind(job_id.to_string())
            .fetch_optional(&fx.pool)
            .await
            .expect("read async_jobs");
        if row.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        row,
        Some(("w-beta".to_string(),)),
        "the async row rides the bound endpoint: {row:?}"
    );

    // The command's side effect appears ONLY on the bound daemon.
    wait_for_file(&dir_b.join("async.txt"), 30_000).await;
    assert!(
        !dir_a.join("async.txt").exists(),
        "the async command must never run on the first endpoint"
    );

    // The completion lands as a durable input row while the drain is
    // parked (admitted before any wake — ADR-008), then the gate
    // releases and the session settles having seen it.
    let key = format!("async-result:{job_id}");
    let mut landed = false;
    for _ in 0..2_400 {
        let found: Option<(String,)> =
            sqlx::query_as("SELECT payload FROM inputs WHERE idempotency_key = ?")
                .bind(&key)
                .fetch_optional(&fx.pool)
                .await
                .expect("read inputs");
        if found.is_some() {
            landed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(landed, "the async completion lands as a durable input row");
    worker_mock.released.store(true, Ordering::SeqCst);
    wait_for_session(&fx, worker, "async worker settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 7. Busy refusal names the owner, then the other worker takes over
//    (I3 / I7)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_worker_refusal_names_owner_then_other_worker_takes_over() {
    force_no_proxy();
    let dir = temp_dir("l2-busy");
    let dir_a = dir.join("daemon-a");
    let dir_b = dir.join("daemon-b");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    std::fs::create_dir_all(&dir_b).expect("dir b");
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");

    let port_a = free_port();
    let port_b = free_port();
    let serve_port = free_port();
    let mut daemon_a = daemon_on(&format!("127.0.0.1:{port_a}"), &dir_a).await;
    let mut daemon_b = daemon_on(&format!("127.0.0.1:{port_b}"), &dir_b).await;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call(
                "call-spawn-occupy",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-alpha: occupy the first worker",
                    "exec_worker": "w-alpha",
                }),
            ),
            // The refusal probe: the SAME busy worker, must be refused.
            tool_call(
                "call-spawn-busy",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-alpha: second attempt on the busy worker",
                    "exec_worker": "w-alpha",
                }),
            ),
            // The honest recovery: choose the other worker.
            tool_call(
                "call-spawn-beta",
                "spawn_worker",
                serde_json::json!({
                    "task": "job-beta: take the other worker",
                    "exec_worker": "w-beta",
                }),
            ),
            text("all dispatched"),
        ],
        0,
    )
    .await;
    let (worker_base, handles) = spawn_task_routed_provider(vec![
        // The occupier: exec, then park at request 2 (stays LIVE — the
        // busy state the refusal must see), then settle on release.
        Route {
            tag: "job-alpha",
            script: vec![
                exec_call("call-exec-occupy", "echo alpha > busy-alpha.txt"),
                tool_call(
                    "call-read-occupy",
                    "fs_read",
                    serde_json::json!({"path": "note.txt"}),
                ),
                text("alpha done"),
            ],
            gate_at: 2,
        },
        Route {
            tag: "job-beta",
            script: vec![
                exec_call("call-exec-beta", "echo beta > busy-beta.txt"),
                text("beta done"),
            ],
            gate_at: 0,
        },
    ])
    .await;
    let alpha_handle = &handles[0];

    let config = write_l2_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[
            EndpointSpec {
                id: "w-alpha",
                port: port_a,
                description: "the busy worker",
            },
            EndpointSpec {
                id: "w-beta",
                port: port_b,
                description: "the free worker",
            },
        ],
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    send_channel_event(&client, &fx, "please run the busy check").await;

    // The occupier is bound to w-alpha; its exec ran and its drain is
    // parked at the gated second provider turn (LIVE — the busy state
    // the refusal must see; the gate self-releases after ~45s, so the
    // probe must run well inside that window).
    let (occupier, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("w-alpha"));
    wait_for_file(&dir_a.join("busy-alpha.txt"), 20_000).await;
    for _ in 0..2_400 {
        if alpha_handle.count() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        alpha_handle.count() >= 2,
        "the occupier never reached its parked provider turn"
    );

    // The resident Router's judge turn parks after a tool turn until
    // the next input arrives, so each probe is one channel event. The
    // busy attempt:
    send_channel_event(&client, &fx, "retry on the same worker please").await;

    // The second spawn on the busy worker is refused HONESTLY: the
    // tool error names the owning live worker session, and no
    // thread/started exists for it.
    let router_events = wait_for_session(
        &fx,
        router_session(),
        "busy refusal tool result",
        |events| {
            tool_result_strings(events)
                .iter()
                .any(|r| r.contains("is busy") && r.contains(&occupier.0.to_string()))
        },
    )
    .await;
    let refusal = tool_result_strings(&router_events)
        .into_iter()
        .find(|r| r.contains("is busy") && r.contains(&occupier.0.to_string()))
        .expect("the refusal result");
    assert!(
        refusal.contains("\"is_error\":true"),
        "the refusal is an honest tool error: {refusal}"
    );
    assert_eq!(
        router_events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1,
        "the refused spawn leaves no thread/started"
    );

    // The Router recovered honestly: a third probe event takes the
    // OTHER worker, and its exec landed there (side effects again).
    send_channel_event(&client, &fx, "use the other worker please").await;
    let (beta_session, _thread, beta_worker) = nth_spawn(&fx, 1).await;
    assert_eq!(beta_worker.as_deref(), Some("w-beta"));
    wait_for_file(&dir_b.join("busy-beta.txt"), 20_000).await;
    assert!(
        !dir_a.join("busy-beta.txt").exists(),
        "the recovery spawn must not run on the busy worker"
    );

    // Release the occupier; both sessions settle.
    alpha_handle.release();
    wait_for_session(&fx, occupier, "occupier settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    wait_for_session(&fx, beta_session, "beta settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    assert!(
        !dir_b.join("busy-alpha.txt").exists(),
        "the occupier's exec never landed on the other daemon"
    );

    daemon_a.kill9().await;
    daemon_b.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ADR-035: release_worker over the real server
// ---------------------------------------------------------------------------

/// Seeds one bound worker thread directly into the pre-boot store:
/// the Router-side lineage + the worker session in the requested shape.
async fn seed_bound_thread(
    store: &EventStore,
    root: SessionId,
    worker_session: SessionId,
    thread_id: uuid::Uuid,
    worker: &str,
    running: bool,
    ts_ms: u64,
) {
    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::Epoch;
    use kian_store::AppendGuard;
    use serde_json::json;

    async fn append(
        store: &EventStore,
        session: SessionId,
        seq: u64,
        epoch: u64,
        ts: u64,
        kind: EventKind,
        bytes: serde_json::Value,
    ) {
        store
            .append(
                session,
                Event {
                    seq,
                    session,
                    epoch: Epoch(epoch),
                    ts,
                    kind,
                    bytes,
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: Epoch(epoch),
                    expected_seq: seq,
                },
            )
            .await
            .expect("seed append");
    }

    append(
        store,
        root,
        1,
        0,
        ts_ms,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    append(
        store,
        root,
        2,
        0,
        ts_ms,
        EventKind::ThreadStarted,
        json!({
            "thread_id": thread_id.to_string(),
            "parent": root.0.to_string(),
            "worker": worker_session.0.to_string(),
            "pinned_version": 1,
            "task": {"role": "user", "content": format!("job-{worker}")},
            "exec_worker": worker,
        }),
    )
    .await;
    append(
        store,
        worker_session,
        1,
        0,
        ts_ms,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    append(
        store,
        worker_session,
        2,
        1,
        ts_ms,
        EventKind::DrainClaimed,
        json!({"holder": "seed"}),
    )
    .await;
    append(
        store,
        worker_session,
        3,
        1,
        ts_ms,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "delegated task"}),
    )
    .await;
    if !running {
        append(
            store,
            worker_session,
            4,
            1,
            ts_ms,
            EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            },
            json!({}),
        )
        .await;
    }
}

/// ADR-035 L2: a stuck (interrupted, idle) bound thread is closed through
/// the real `POST /ops/rpc release_worker`; the real store shows the
/// terminal `cancelled` close, a second call is the idempotent
/// `already_released`, a running thread is refused `-32002`, and an
/// unknown id is refused `-32008`. No Router model call is involved —
/// the binding is the committed stream, exactly as the resolver reads it.
#[tokio::test]
async fn release_worker_ops_closes_a_stuck_binding_and_frees_it() {
    use kian_core::events::EventKind as EK;
    use serde_json::json;
    force_no_proxy();
    let dir = temp_dir("release-worker");
    let serve_port = free_port();
    let db = dir.join("kian.sqlite");
    let config = dir.join("kian.yaml");
    std::fs::write(
        &config,
        format!(
            r#"listen: "127.0.0.1:{serve_port}"
storage:
  sqlite: "{db}"
agent:
  id: default
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "remote-exec"]
endpoints:
  - id: w-alpha
    addr: "127.0.0.1:1"
    description: "the stuck worker"
    queue_timeout_ms: 60000
    offline_after_ms: 30000
  - id: w-beta
    addr: "127.0.0.1:2"
    description: "the running worker"
    queue_timeout_ms: 60000
    offline_after_ms: 30000
drain:
  max_iterations: 2
tools:
  root: "{dir}"
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
            db = db.display(),
            dir = dir.display(),
        ),
    )
    .expect("write l2 config");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let stuck_session = SessionId::new();
    let running_session = SessionId::new();
    let stuck_thread = uuid::Uuid::new_v4();
    let running_thread = uuid::Uuid::new_v4();
    {
        let store = EventStore::open(&db).await.expect("pre-boot store");
        seed_bound_thread(
            &store,
            SessionId::new(),
            stuck_session,
            stuck_thread,
            "w-alpha",
            false,
            now_ms,
        )
        .await;
        seed_bound_thread(
            &store,
            SessionId::new(),
            running_session,
            running_thread,
            "w-beta",
            true,
            now_ms,
        )
        .await;
        store.close().await;
    }

    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let client = reqwest::Client::new();

    let released = rpc(
        &client,
        &base,
        "/ops/rpc",
        "release_worker",
        json!({"worker": "w-alpha", "reason": "stuck after interrupt"}),
    )
    .await;
    let result = released
        .get("result")
        .unwrap_or_else(|| panic!("release failed: {released}"));
    assert_eq!(result["outcome"], "released", "{released}");
    assert_eq!(
        result["session"].as_str(),
        Some(stuck_session.0.to_string().as_str())
    );
    assert_eq!(
        result["thread_id"].as_str(),
        Some(stuck_thread.to_string().as_str())
    );

    // The real store shows one terminal cancelled close that preserves
    // the earlier honest interrupted end.
    let store = EventStore::open_read_only(&db)
        .await
        .expect("read-only store");
    let events = store.rebuild(stuck_session).await.expect("rebuild");
    assert!(matches!(
        events.last().unwrap().kind,
        EK::SessionEnded {
            reason: kian_core::events::EndReason::Cancelled
        }
    ));
    assert_eq!(
        events.last().unwrap().bytes["released"]["worker"],
        json!("w-alpha")
    );
    assert_eq!(
        events.last().unwrap().bytes["released"]["by"],
        json!("operator")
    );
    assert!(events.iter().any(|e| matches!(
        e.kind,
        EK::SessionEnded {
            reason: kian_core::events::EndReason::Interrupted
        }
    )));
    assert_eq!(
        store.rebuild(running_session).await.expect("rebuild").len(),
        3,
        "the running thread was not touched"
    );
    store.close().await;

    // Idempotent: the slot is already free.
    let again = rpc(
        &client,
        &base,
        "/ops/rpc",
        "release_worker",
        json!({"worker": "w-alpha"}),
    )
    .await;
    assert_eq!(again["result"]["outcome"], "already_released", "{again}");

    // The operator verb is the same call through the client: a second
    // release reads `already_released` (the SDK + CLI leg).
    let cli = std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["workers", "release", "w-alpha", "--url", &base, "--json"])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_DEDUPE", "0")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .expect("run kian workers release");
    assert!(
        cli.status.success(),
        "kian workers release failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_json: serde_json::Value = serde_json::from_slice(&cli.stdout).expect("cli json");
    assert_eq!(cli_json["outcome"], "already_released", "{cli_json}");

    // A running thread is never force-closed.
    let busy = rpc(
        &client,
        &base,
        "/ops/rpc",
        "release_worker",
        json!({"worker": "w-beta"}),
    )
    .await;
    assert_eq!(busy["error"]["code"], -32002, "{busy}");

    // An unknown worker id is the honest not-bound refusal.
    let unknown = rpc(
        &client,
        &base,
        "/ops/rpc",
        "release_worker",
        json!({"worker": "nope"}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32008, "{unknown}");

    kill_serve(&mut serve).await;
}
