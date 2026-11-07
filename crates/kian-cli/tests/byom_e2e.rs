//! BYOM acceptance e2e: the **real `kiand` binary** as a
//! child process, paired with the full `kian serve` super-agent assembly.
//!
//! One remote endpoint is configured; a scripted worker issues an `exec`
//! tool call → durable `exec_queue` row → dial → kiand executes →
//! `tool/result` event → worker completes. The offline leg kills the
//! daemon with -9 while a command is queued: the watchdog detects the
//! offline transition within its `offline_after_ms + poll` bound, a
//! health input is admitted durably to the worker's session, and either
//! a restart drains the queue (result flows, completion) or the queue
//! timeout resolves honestly (`Timeout`, row `abandoned`).
//!
//! Timing discipline (plan §7 risk 5): every bound is seeded and
//! generous. `heartbeat_ms = 50`, `offline_after_ms = 600` (watchdog
//! poll = 600/4 = 150 ms, bridge tick 250 ms); detection is asserted
//! under `offline_after + poll + tick + slack`. Kill -9 is a real
//! SIGKILL; the restart reuses the reserved port deterministically.
//!
//! No credentials (mock providers over real wire + mock channel), and
//! `NO_PROXY=127.0.0.1,localhost` is forced in every test (AGENTS.md).

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_store::EventStore;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

// Seeded timing (discipline): heartbeat cadence, offline bound, and
// the derived watchdog poll (assembly: offline_after/4 clamped to
// [50, 1000] ms) plus the watchdog-bridge sweep tick.
const HEARTBEAT_MS: u64 = 50;
const OFFLINE_AFTER_MS: u64 = 600;
const WATCHDOG_POLL_MS: u64 = OFFLINE_AFTER_MS / 4; // 150
const BRIDGE_TICK_MS: u64 = 250;
/// Slack above the analytic detection bound — absorbs scheduling jitter
/// on a loaded macOS runner without weakening the bound assertion.
const DETECTION_SLACK_MS: u64 = 2_000;

fn router_session() -> kian_core::model::SessionId {
    kian_core::model::SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

// ---------------------------------------------------------------------------
// The real kiand child process (spawn/registration/kill -9/restart)
// ---------------------------------------------------------------------------

/// The spawned daemon. Killed on drop so a failing test never leaks a
/// process (same guard discipline as `crates/kiand/tests/daemon_e2e.rs`).
struct Daemon {
    child: Child,
}

/// Locates the real `kiand` binary. `CARGO_BIN_EXE_kiand` is only set
/// inside the `kiand` crate's own tests; from a sibling crate we resolve
/// the workspace target dir (the binary is built by
/// `cargo build/test --workspace`, which is also the gate invocation).
fn kiand_bin() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_kiand") {
        return path.into();
    }
    let mut candidates = Vec::new();
    // `CARGO_TARGET_TMPDIR` = <target>/<profile>/tmp for integration
    // tests, so its parent is the profile dir holding the binaries.
    if let Some(profile) = option_env!("CARGO_TARGET_TMPDIR")
        .map(std::path::Path::new)
        .and_then(std::path::Path::parent)
    {
        candidates.push(profile.join("kiand"));
    }
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let manifest = std::path::PathBuf::from(manifest_dir);
        candidates.push(manifest.join("../../target/debug/kiand"));
        candidates.push(manifest.join("../../target/release/kiand"));
    }
    candidates
        .iter()
        .find(|p| p.exists())
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "kiand binary not found — run `cargo build --workspace` first; searched {candidates:?}"
            )
        })
}

impl Daemon {
    /// Spawns the real binary on `listen` (deterministic port for the
    /// restart legs) with a fast heartbeat cadence.
    fn spawn(listen: &str) -> Daemon {
        let mut command = Command::new(kiand_bin());
        command
            .args([
                "--listen",
                listen,
                "--heartbeat-ms",
                &HEARTBEAT_MS.to_string(),
            ])
            .env("NO_PROXY", "127.0.0.1,localhost")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        Daemon {
            child: command.spawn().expect("spawn kiand"),
        }
    }

    /// Reads stdout lines until the registration line; parses the bound
    /// address.
    async fn wait_listening(&mut self) -> SocketAddr {
        let stdout = self.child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();
        loop {
            let line = match timeout(Duration::from_secs(15), lines.next_line()).await {
                Ok(Ok(Some(line))) => line,
                Ok(Ok(None)) => panic!("kiand exited before printing its listening line"),
                Ok(Err(e)) => panic!("cannot read kiand stdout: {e}"),
                Err(_) => panic!("kiand did not print its listening line within 15s"),
            };
            if let Some(rest) = line.strip_prefix("kiand: endpoint ")
                && let Some((_id, addr)) = rest.split_once(" listening on ")
            {
                return addr.parse().expect("bound address parses");
            }
        }
    }

    /// Hard crash: SIGKILL — no drain, no bye frame.
    async fn kill9(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Reserves an ephemeral port and releases it for the daemon (the
/// restart legs must bind the *same* address again).
fn reserve_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    port
}

// ---------------------------------------------------------------------------
// The serve fixture: super-agent assembly + one real kiand endpoint
// ---------------------------------------------------------------------------

struct Fixture {
    base: String,
    _server: Arc<Server>,
    store: EventStore,
    pool: sqlx::sqlite::SqlitePool,
    dir: PathBuf,
}

/// Router script: bootstrap ack, then dispatch a worker for the incoming
/// event, then an idle final answer (the resident Router parks).
fn router_script() -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::Text("bootstrap acknowledged".into()),
        ScriptedResponse::ToolCall {
            id: "call-spawn-1".into(),
            name: "spawn_worker".into(),
            arguments: serde_json::json!({"task": "run the deploy checklist"}),
        },
        ScriptedResponse::Text("worker dispatched".into()),
    ]
}

/// Worker script: one remote exec call, then an honest final answer.
fn worker_script(exec: serde_json::Value) -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::ToolCall {
            id: "call-exec-1".into(),
            name: "exec".into(),
            arguments: exec,
        },
        ScriptedResponse::Text("deploy checklist finished".into()),
    ]
}

/// The full super-agent `kian.yaml` (shape) with one **live** endpoint
/// (`ep1`) at `addr` and the timing seeds.
fn write_byom_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
    addr: &str,
    queue_timeout_ms: u64,
    worker_gate: bool,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let worker_gate = if worker_gate {
        "    evidence_gate:\n      enabled: true\n      checks:\n        - type: final_message_present\n      max_retries: 1\n"
    } else {
        ""
    };
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
endpoints:
  - id: ep1
    addr: "{addr}"
    queue_timeout_ms: {queue_timeout_ms}
    offline_after_ms: {OFFLINE_AFTER_MS}
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
    std::fs::write(&path, yaml).expect("write s5 config");
    path
}

/// Spawns the assembled server on an ephemeral HTTP port (harness
/// shape) and opens the read-only observation surfaces.
async fn spawn_server(config_path: &std::path::Path, dir: &std::path::Path) -> Fixture {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble s5 server")
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
        .expect("read-only exec pool");
    Fixture {
        base: format!("http://{addr}"),
        _server: server,
        store,
        pool,
        dir: dir.to_path_buf(),
    }
}

/// Boots the whole pairing: mock providers, real kiand on a reserved
/// port, assembled serve. Returns the fixture + the daemon.
async fn boot(
    tag: &str,
    exec: serde_json::Value,
    queue_timeout_ms: u64,
    worker_gate: bool,
) -> (Fixture, Daemon, u16) {
    force_no_proxy();
    let dir = temp_dir(tag);
    let (router_base, _router_mock) = spawn_scripted_provider(router_script(), 0).await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(worker_script(exec), 0).await;

    let port = reserve_port();
    let listen = format!("127.0.0.1:{port}");
    let mut daemon = Daemon::spawn(&listen);
    let addr = daemon.wait_listening().await;
    assert_eq!(addr.port(), port, "kiand binds the reserved port");

    let config = write_byom_config(
        &dir,
        &router_base,
        &worker_base,
        &listen,
        queue_timeout_ms,
        worker_gate,
    );
    let fixture = spawn_server(&config, &dir).await;
    (fixture, daemon, port)
}

// ---------------------------------------------------------------------------
// Observation helpers (committed streams + durable queue rows)
// ---------------------------------------------------------------------------

async fn ops_rpc(
    client: &reqwest::Client,
    base: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "mock_channel_send", "params": params}))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops json")
}

fn count_kind(events: &[kian_core::events::Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

async fn wait_for_router(
    fx: &Fixture,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    let session = router_session();
    for _ in 0..1_600 {
        let events = fx.store.rebuild(session).await.expect("rebuild router");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

async fn wait_for_session(
    fx: &Fixture,
    session: kian_core::model::SessionId,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    for _ in 0..1_600 {
        let events = fx.store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// The worker session the Router's `thread/started` names.
async fn worker_session(fx: &Fixture) -> kian_core::model::SessionId {
    let events = wait_for_router(fx, "router dispatches the worker", |events| {
        count_kind(events, EventKind::ThreadStarted) >= 1
    })
    .await;
    let started = events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .unwrap();
    let raw = started.bytes["worker"].as_str().unwrap();
    kian_core::model::SessionId(uuid::Uuid::parse_str(raw).unwrap())
}

/// One durable `exec_queue` row: (command_id, endpoint_id, status).
async fn queue_rows(fx: &Fixture) -> Vec<(String, String, String)> {
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT command_id, endpoint_id, status FROM exec_queue",
    )
    .fetch_all(&fx.pool)
    .await
    .expect("read exec_queue")
}

/// The registry projection for `ep1`: (state, last_heartbeat_ms).
async fn registry_ep1(fx: &Fixture) -> (String, i64) {
    let row: Option<(String, i64)> = sqlx::query_as(
        "SELECT state, last_heartbeat_ms FROM endpoint_registry WHERE endpoint_id = 'ep1'",
    )
    .fetch_optional(&fx.pool)
    .await
    .expect("read endpoint_registry");
    row.expect("ep1 registered")
}

/// Polls until the endpoint registry shows a fresh heartbeat (the
/// server connected and the queue driver projected it).
async fn wait_online(fx: &Fixture) {
    for _ in 0..800 {
        let (state, last) = registry_ep1(fx).await;
        if state == "online" && last > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("endpoint ep1 never came online");
}

/// Polls until the exec queue row is `inflight` — the command is durably
/// queued *and* dispatched on the live endpoint (the kill point the
/// scenario names: a command queued while the daemon runs).
async fn wait_row_inflight(fx: &Fixture) {
    for _ in 0..800 {
        let rows = queue_rows(fx).await;
        if rows.len() == 1 && rows[0].2 == "inflight" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the exec row never reached inflight on the live endpoint");
}

/// Polls until the registry state flips to offline (the driver observed
/// the transport loss).
async fn wait_registry_offline(fx: &Fixture) {
    for _ in 0..800 {
        let (state, _) = registry_ep1(fx).await;
        if state == "offline" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("endpoint ep1 never went offline");
}

/// Durable endpoint-health inbox rows for `ep1`: (session, payload).
/// The row is the *admission* (constraint 6: durable before any wake);
/// the worker appends the `input/admitted` event only when its drain
/// next sweeps the inbox, which — while it is parked in the tool future
/// across the offline window — is strictly later than the row.
async fn health_input_rows(fx: &Fixture) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT session, payload FROM inputs WHERE idempotency_key LIKE 'endpoint-health:ep1:%'",
    )
    .fetch_all(&fx.pool)
    .await
    .expect("read health inputs")
}

/// Polls until the watchdog bridge has admitted the offline health
/// input row for the given worker session; returns the payload JSON.
async fn wait_health_row(fx: &Fixture, worker: kian_core::model::SessionId) -> serde_json::Value {
    for _ in 0..1_600 {
        for (session, payload) in health_input_rows(fx).await {
            if session == worker.0.to_string() {
                return serde_json::from_str(&payload).expect("payload json");
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no durable endpoint-health input admitted for the worker");
}

/// The worker's exec tool-result event, once it exists.
fn exec_result(events: &[kian_core::events::Event]) -> Option<&kian_core::events::Event> {
    events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-exec-1")
}

/// Index of the exec tool result (honesty ordering assertions).
fn exec_result_index(events: &[kian_core::events::Event]) -> Option<usize> {
    events.iter().position(|e| {
        e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == "call-exec-1"
    })
}

fn ended_index(events: &[kian_core::events::Event]) -> Option<usize> {
    events
        .iter()
        .position(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
}

// ---------------------------------------------------------------------------
// round trip: exec on the real kiand → tool/result → completion
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byom_exec_on_kiand_round_trip() {
    let (fx, mut daemon, _port) = boot(
        "c29-roundtrip",
        serde_json::json!({"argv": ["echo", "s5-round-trip"]}),
        15_000,
        true,
    )
    .await;
    let client = reqwest::Client::new();

    // The resident Router's bootstrap judge turn must land first.
    wait_for_router(&fx, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    // The work request that triggers spawn → worker → remote exec.
    ops_rpc(
        &client,
        &fx.base,
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;

    let worker = worker_session(&fx).await;

    // The worker's exec ran on the real daemon and returned a real result.
    let events = wait_for_session(&fx, worker, "exec tool result", |events| {
        exec_result(events).is_some()
    })
    .await;
    let result = exec_result(&events).unwrap();
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(false)),
        "the remote exec succeeded: {result:?}"
    );
    assert_eq!(
        result.bytes.pointer("/content/output/exit_code"),
        Some(&serde_json::json!(0))
    );
    assert_eq!(
        result
            .bytes
            .pointer("/content/output/stdout")
            .and_then(|v| v.as_str())
            .map(str::trim),
        Some("s5-round-trip"),
        "stdout is the daemon's real echo: {result:?}"
    );

    // The worker completed honestly after the real result, and the
    // durable queue row settled done.
    let events = wait_for_session(&fx, worker, "worker completes", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    let (exec_idx, end_idx) = (exec_result_index(&events), ended_index(&events));
    assert!(
        exec_idx < end_idx,
        "completed strictly after the real result: exec@{exec_idx:?} end@{end_idx:?}"
    );
    assert_eq!(
        events[exec_idx.unwrap()].bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(false))
    );
    assert!(matches!(
        events[end_idx.unwrap()].kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));
    assert_eq!(count_kind(&events, EventKind::ThreadCompleted), 1);

    let rows = queue_rows(&fx).await;
    assert_eq!(rows.len(), 1, "one command, one row (ADR-007): {rows:?}");
    assert_eq!(rows[0].2, "done");

    daemon.kill9().await;
    let _ = std::fs::remove_dir_all(&fx.dir);
}

// ---------------------------------------------------------------------------
// offline leg, part 1: kill -9 while the command is queued → the row
// parks durably pending (no dispatch, no fake result)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byom_offline_queue_parks_durable() {
    let (fx, mut daemon, _port) = boot(
        "c29-park",
        serde_json::json!({"argv": ["echo", "s5-parked"]}),
        8_000,
        true,
    )
    .await;
    let client = reqwest::Client::new();

    wait_for_router(&fx, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    // Crash the daemon while it is healthy and connected.
    wait_online(&fx).await;
    daemon.kill9().await;
    wait_registry_offline(&fx).await;

    ops_rpc(
        &client,
        &fx.base,
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;

    let worker = worker_session(&fx).await;
    wait_for_session(&fx, worker, "worker issues the exec call", |events| {
        count_kind(events, EventKind::ToolCall) >= 1
    })
    .await;

    // The row is durable and parked: pending on the offline endpoint,
    // never dispatched, never resolved with a fake result.
    let mut saw_pending = false;
    for _ in 0..120 {
        let rows = queue_rows(&fx).await;
        assert_eq!(rows.len(), 1, "exactly one queue row: {rows:?}");
        assert_eq!(rows[0].1, "ep1");
        assert!(
            rows[0].2 == "pending" || rows[0].2 == "inflight",
            "row is parked, not settled: {rows:?}"
        );
        saw_pending = true;
        // The worker cannot have a tool result while the row is parked.
        let events = fx.store.rebuild(worker).await.expect("rebuild worker");
        assert!(
            exec_result(&events).is_none(),
            "no tool result while the endpoint is offline — the park is honest"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(saw_pending);

    let _ = std::fs::remove_dir_all(&fx.dir);
}

// ---------------------------------------------------------------------------
// offline leg, part 2: the watchdog detects the offline transition
// within offline_after_ms + poll and admits a durable health input
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byom_watchdog_detects_offline_within_bound() {
    let (fx, mut daemon, _port) = boot(
        "c29-watchdog",
        // A long-running command: at the kill point the row is durable
        // AND in flight on the live daemon (kill -9 while queued).
        serde_json::json!({"argv": ["sleep", "30"]}),
        8_000,
        true,
    )
    .await;
    let client = reqwest::Client::new();

    wait_for_router(&fx, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;
    wait_online(&fx).await;

    // The work request first: the command must be queued *before* the
    // crash so the offline episode has an affected session.
    ops_rpc(
        &client,
        &fx.base,
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;
    let worker = worker_session(&fx).await;
    wait_for_session(&fx, worker, "worker issues the exec call", |events| {
        count_kind(events, EventKind::ToolCall) >= 1
    })
    .await;
    // The command is durable and dispatched on the live daemon.
    wait_row_inflight(&fx).await;

    // Hard crash with the command queued; measure detection. The bound
    // applies to the *admission*: the durable inbox row lands as soon as
    // the watchdog's offline edge + bridge sweep fire. The worker
    // appends the `input/admitted` event only when its (parked) drain
    // next sweeps the inbox — that is later than the row by design.
    let killed_at = tokio::time::Instant::now();
    daemon.kill9().await;

    let payload = wait_health_row(&fx, worker).await;
    let detected_in = killed_at.elapsed();

    assert_eq!(
        payload.pointer("/kian/endpoint"),
        Some(&serde_json::json!("ep1"))
    );
    assert_eq!(
        payload.pointer("/kian/status"),
        Some(&serde_json::json!("offline"))
    );
    assert_eq!(
        payload.pointer("/kian/queued_commands"),
        Some(&serde_json::json!(1))
    );

    // The detection bound: offline_after + watchdog poll + bridge tick,
    // with seeded slack for scheduler jitter.
    let bound = Duration::from_millis(
        OFFLINE_AFTER_MS + WATCHDOG_POLL_MS + BRIDGE_TICK_MS + DETECTION_SLACK_MS,
    );
    assert!(
        detected_in <= bound,
        "offline detected in {detected_in:?}, bound {bound:?} \
         (offline_after={OFFLINE_AFTER_MS}ms, poll={WATCHDOG_POLL_MS}ms, tick={BRIDGE_TICK_MS}ms)"
    );

    // The row eventually becomes a committed `input/admitted` event on
    // the worker's stream (observed, not bounded — the worker is parked
    // in the tool future across the offline window).
    let events = wait_for_session(&fx, worker, "health input admitted", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("endpoint_health"))
        })
    })
    .await;
    let health = events
        .iter()
        .rev()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("endpoint_health"))
        })
        .unwrap();
    assert_eq!(
        health.bytes.pointer("/content/kian/endpoint"),
        Some(&serde_json::json!("ep1"))
    );
    assert_eq!(
        health.bytes.pointer("/content/kian/status"),
        Some(&serde_json::json!("offline"))
    );
    assert_eq!(
        health.bytes.pointer("/content/kian/queued_commands"),
        Some(&serde_json::json!(1))
    );

    // The watchdog's own counters agree (detection latency ≤ bound).
    let metrics: serde_json::Value = client
        .get(format!("{}/metrics", fx.base))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    let wd = &metrics["superagent"]["watchdog"];
    assert!(
        wd["offline_transitions"].as_u64().unwrap_or(0) >= 1,
        "offline transition counted: {wd}"
    );
    // `mean_detection_latency_ms` is not asserted: the mean includes the
    // registered-but-stale startup episode (last_heartbeat = 0, seeded
    // by the assembly), whose "latency" is epoch-scale noise. The bound
    // itself is proven above by the wall-clock measurement.

    let _ = std::fs::remove_dir_all(&fx.dir);
}

// ---------------------------------------------------------------------------
// offline leg, part 3: restart kiand → reconnect drains the queue →
// the real result flows → the worker completes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byom_reconnect_drains_queue() {
    let (fx, mut daemon, port) = boot(
        "c29-reconnect",
        // A command long enough to still be in flight at the kill point,
        // short enough that the post-restart re-drive finishes well
        // inside the queue timeout.
        serde_json::json!({"argv": ["sleep", "4"]}),
        20_000,
        true,
    )
    .await;
    let client = reqwest::Client::new();

    wait_for_router(&fx, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;
    wait_online(&fx).await;

    ops_rpc(
        &client,
        &fx.base,
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;
    let worker = worker_session(&fx).await;
    wait_for_session(&fx, worker, "worker issues the exec call", |events| {
        count_kind(events, EventKind::ToolCall) >= 1
    })
    .await;
    // The command is durable and in flight on the live daemon before
    // the crash (kill -9 while queued — not before it was ever sent).
    wait_row_inflight(&fx).await;

    // Crash with the command queued; wait for the offline episode (the
    // durable health row — the admission — not the parked drain's
    // later event).
    daemon.kill9().await;
    wait_registry_offline(&fx).await;
    wait_health_row(&fx, worker).await;

    // Restart on the same port; the manager reconnects on its own.
    let listen = format!("127.0.0.1:{port}");
    let mut daemon2 = Daemon::spawn(&listen);
    let addr2 = daemon2.wait_listening().await;
    assert_eq!(addr2.port(), port, "restart binds the same port");

    // Reconnect drains the parked row: the real result flows to the
    // worker and it completes.
    let events = wait_for_session(&fx, worker, "exec result after restart", |events| {
        exec_result(events).is_some()
    })
    .await;
    let result = exec_result(&events).unwrap();
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(false)),
        "the parked command executed after reconnect: {result:?}"
    );
    assert_eq!(
        result.bytes.pointer("/content/output/exit_code"),
        Some(&serde_json::json!(0))
    );

    let events = wait_for_session(&fx, worker, "worker completes", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    let (exec_idx, end_idx) = (exec_result_index(&events), ended_index(&events));
    assert!(
        exec_idx < end_idx,
        "completed strictly after the real result (never before): exec@{exec_idx:?} end@{end_idx:?}"
    );
    assert!(matches!(
        events[end_idx.unwrap()].kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));

    let rows = queue_rows(&fx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, "done", "the drained row settled done: {rows:?}");

    daemon2.kill9().await;
    let _ = std::fs::remove_dir_all(&fx.dir);
}

// ---------------------------------------------------------------------------
// honesty across the offline window: queue timeout first → honest
// Timeout tool result, row abandoned, completion only after the honest
// error — never a fake success, never completed-before-result
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byom_honesty_across_offline_window() {
    // queue_timeout (1200ms) fires well before any restart: the honest
    // bound resolves the parked caller.
    let (fx, mut daemon, _port) = boot(
        "c29-honesty",
        serde_json::json!({"argv": ["echo", "s5-honesty"]}),
        1_200,
        true,
    )
    .await;
    let client = reqwest::Client::new();

    wait_for_router(&fx, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;
    wait_online(&fx).await;
    daemon.kill9().await;
    wait_registry_offline(&fx).await;

    ops_rpc(
        &client,
        &fx.base,
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;
    let worker = worker_session(&fx).await;

    // The honest timeout resolves the exec call.
    let events = wait_for_session(&fx, worker, "honest timeout tool result", |events| {
        exec_result(events).is_some()
    })
    .await;
    let result = exec_result(&events).unwrap();
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(true)),
        "the offline exec is an honest error, never a fake success: {result:?}"
    );
    let error = result
        .bytes
        .pointer("/content/output/error")
        .and_then(|v| v.as_str())
        .expect("error field present");
    let lower = error.to_lowercase();
    assert!(
        lower.contains("timeout") || lower.contains("timed out"),
        "the honest error is the queue timeout: {error}"
    );

    // Recovery disposed of the row: abandoned (terminal) — a later
    // duplicate command id gets the same honest Timeout (ADR-007).
    let mut abandoned = false;
    for _ in 0..200 {
        let rows = queue_rows(&fx).await;
        if rows.len() == 1 && rows[0].2 == "abandoned" {
            abandoned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(abandoned, "queue row settles abandoned after the timeout");

    // The worker completes only after the honest error, with the
    // evidence gate passing its final message.
    let events = wait_for_session(&fx, worker, "worker settles", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    let (exec_idx, end_idx) = (exec_result_index(&events), ended_index(&events));
    assert!(
        exec_idx < end_idx,
        "completed strictly after the honest error result: exec@{exec_idx:?} end@{end_idx:?}"
    );
    // Never a fake success anywhere in the window.
    assert!(
        exec_result(&events)
            .unwrap()
            .bytes
            .pointer("/content/output/stdout")
            .is_none(),
        "the timeout result carries no stdout"
    );
    assert!(
        matches!(
            events[end_idx.unwrap()].kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ),
        "the worker settles completed on its honest final answer"
    );
    assert!(
        events.iter().any(|e| e.kind == EventKind::EvidenceVerdict
            && e.bytes["passed"] == serde_json::json!(true)),
        "the evidence gate passed the honest final answer"
    );
    assert_eq!(count_kind(&events, EventKind::ThreadCompleted), 1);

    let _ = std::fs::remove_dir_all(&fx.dir);
}
