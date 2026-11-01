//! Async `start()`/`stop()` assembled-server e2e (card C37, ADR-008):
//! the full `kian serve` super-agent assembly (real engine, real
//! store, real completion bridge) paired with the **real `kiand`
//! binary** as a child process, over scripted mock providers — no
//! credentials, `NO_PROXY=127.0.0.1,localhost` throughout.
//!
//! What the two tests prove end to end:
//!
//! 1. `start_returns_fast_steer_interleaves_and_completion_lands` —
//!    the worker's `start` resolves as an honest `tool/result` while
//!    the command runs for seconds; the session answers a **steer**
//!    during the job (the S2 path interleaved — loop-responsiveness);
//!    when the command settles, the completion lands as a durable
//!    inbox input row (`async-result:{job_id}`) and is consumed at the
//!    next drain as a visible `async_result` message.
//! 2. `stop_mid_run_labels_cancelled_end_to_end` — `stop` cancels by
//!    job id: the job row settles `cancelled`, the stop tool's own
//!    result is honest, and the cancelled completion still lands,
//!    labeled `cancelled`.
//!
//! Timing discipline (C15, plan §7 risk 5): seeded, generous bounds;
//! the "fast" assertion is measured against a 3-second command with a
//! bound well under it, never against the command's own completion.

mod common;

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    MockProvider, ScriptedResponse, force_no_proxy, spawn_scripted_provider,
    spawn_scripted_provider_gated_at, temp_dir,
};
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_store::EventStore;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const HEARTBEAT_MS: u64 = 50;
const OFFLINE_AFTER_MS: u64 = 600;

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

// ---------------------------------------------------------------------------
// The real kiand child process (same harness discipline as s5_byom_e2e)
// ---------------------------------------------------------------------------

struct Daemon {
    child: Child,
}

fn kiand_bin() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_kiand") {
        return path.into();
    }
    let mut candidates = Vec::new();
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
        .unwrap_or_else(|| panic!("kiand binary not found; searched {candidates:?}"))
}

impl Daemon {
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
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// The serve fixture: super-agent assembly + async pair + one live endpoint
// ---------------------------------------------------------------------------

struct Fixture {
    base: String,
    _server: Arc<Server>,
    store: EventStore,
    pool: sqlx::sqlite::SqlitePool,
    dir: PathBuf,
}

fn router_script() -> Vec<ScriptedResponse> {
    vec![
        ScriptedResponse::Text("bootstrap acknowledged".into()),
        ScriptedResponse::ToolCall {
            id: "call-spawn-1".into(),
            name: "spawn_worker".into(),
            arguments: serde_json::json!({"task": "run the async job"}),
        },
        ScriptedResponse::Text("worker dispatched".into()),
    ]
}

/// The C27 config shape plus `superagent.async_tools: true` (the C37
/// gate) and one live endpoint.
fn write_async_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
    addr: &str,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7341"
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
  async_tools: true
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
    queue_timeout_ms: 20000
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
    std::fs::write(&path, yaml).expect("write async config");
    path
}

async fn spawn_server(config_path: &std::path::Path, dir: &std::path::Path) -> Fixture {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble async server")
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
        .expect("read-only pool");
    Fixture {
        base: format!("http://{addr}"),
        _server: server,
        store,
        pool,
        dir: dir.to_path_buf(),
    }
}

/// Boots the assembly. `gate_at` parks the worker provider's Nth
/// request until the test releases it (the deterministic mid-drain
/// window in which the async completion lands). Returns the worker
/// mock so the test can release the gate on durable evidence.
async fn boot(
    tag: &str,
    worker_script: Vec<ScriptedResponse>,
    gate_at: usize,
) -> (Fixture, Daemon, Arc<MockProvider>, Arc<MockProvider>) {
    force_no_proxy();
    let dir = temp_dir(tag);
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");
    let (router_base, router_mock) = spawn_scripted_provider(router_script(), 0).await;
    let (worker_base, worker_mock) =
        spawn_scripted_provider_gated_at(worker_script, 0, gate_at).await;

    // Reserve then release the port for the daemon.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let listen = format!("127.0.0.1:{}", probe.local_addr().unwrap().port());
    drop(probe);
    let mut daemon = Daemon::spawn(&listen);
    daemon.wait_listening().await;

    let config = write_async_config(&dir, &router_base, &worker_base, &listen);
    let fixture = spawn_server(&config, &dir).await;
    (fixture, daemon, worker_mock, router_mock)
}

// ---------------------------------------------------------------------------
// Observation helpers
// ---------------------------------------------------------------------------

async fn rpc(
    client: &reqwest::Client,
    fx: &Fixture,
    path: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{}{path}", fx.base))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

async fn wait_for_session(
    fx: &Fixture,
    session: SessionId,
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

async fn worker_session(fx: &Fixture) -> SessionId {
    let router = router_session();
    for _ in 0..1_600 {
        let events = fx.store.rebuild(router).await.expect("rebuild router");
        if let Some(started) = events.iter().find(|e| e.kind == EventKind::ThreadStarted) {
            let raw = started.bytes["worker"].as_str().unwrap();
            return SessionId(uuid::Uuid::parse_str(raw).unwrap());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = fx.store.rebuild(router).await.expect("rebuild router");
    panic!("timed out waiting for the worker spawn; router stream: {events:?}");
}

/// The `tool/result` payloads in a stream, as JSON values.
fn tool_results(events: &[kian_core::events::Event]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter(|e| e.kind == EventKind::ToolResult)
        .map(|e| e.bytes.clone())
        .collect()
}

/// The async job row: (status, result).
async fn async_job_row(fx: &Fixture, job_id: &str) -> Option<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT status, COALESCE(result, '') FROM async_jobs WHERE job_id = ?",
    )
    .bind(job_id)
    .fetch_optional(&fx.pool)
    .await
    .expect("read async_jobs")
}

/// The completion input row for one job (durable before any wake).
async fn completion_input(fx: &Fixture, job_id: &str) -> Option<serde_json::Value> {
    let key = format!("async-result:{job_id}");
    let row: Option<(String,)> =
        sqlx::query_as("SELECT payload FROM inputs WHERE idempotency_key = ?")
            .bind(key)
            .fetch_optional(&fx.pool)
            .await
            .expect("read inputs");
    row.map(|(payload,)| serde_json::from_str(&payload).expect("payload json"))
}

/// Waits until the completion input is durable; returns its payload.
async fn wait_completion_input(fx: &Fixture, job_id: &str) -> serde_json::Value {
    for _ in 0..1_600 {
        if let Some(payload) = completion_input(fx, job_id).await {
            return payload;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for the completion input of {job_id}");
}

async fn dispatch_worker(client: &reqwest::Client, fx: &Fixture) -> SessionId {
    // Send the channel event only after the Router's bootstrap turn
    // completed: an earlier send can be batched WITH the bootstrap
    // input (one hold sweep, one judge response), and the scripted
    // spawn would never happen. Waiting makes the turn order
    // deterministic: bootstrap ack → channel event → spawn.
    wait_for_session(
        fx,
        router_session(),
        "router bootstrap answered",
        |events| events.iter().any(|e| e.kind == EventKind::AssistantMessage),
    )
    .await;
    rpc(
        client,
        fx,
        "/ops/rpc",
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "run the async job"}}),
    )
    .await;
    worker_session(fx).await
}

// ---------------------------------------------------------------------------
// 1. start: immediate ack + steer interleave + admitted completion
// ---------------------------------------------------------------------------

/// The drain's shape here is deliberate: a kian session's drain ends
/// (terminal `completed`) when the loop settles on a final answer, so
/// the full-duplex story plays out **inside one drain** — the model
/// keeps working (tool turns) while the job runs, the completion is
/// admitted mid-drain and promoted at the next provider-turn safe
/// boundary. The fourth provider request is gated: the drain parks
/// mid-turn until the completion input is durable, making the whole
/// interleaving deterministic (no sleeps, no races).
#[tokio::test]
async fn start_returns_fast_steer_interleaves_and_completion_lands() {
    let worker_script = vec![
        ScriptedResponse::ToolCall {
            id: "call-start-1".into(),
            name: "start".into(),
            arguments: serde_json::json!({"argv": ["sleep", "3"]}),
        },
        ScriptedResponse::ToolCall {
            id: "call-read-1".into(),
            name: "fs_read".into(),
            arguments: serde_json::json!({"path": "note.txt"}),
        },
        ScriptedResponse::ToolCall {
            id: "call-read-2".into(),
            name: "fs_read".into(),
            arguments: serde_json::json!({"path": "note.txt"}),
        },
        // GATED (request 4): the drain parks here while the job runs.
        ScriptedResponse::ToolCall {
            id: "call-read-3".into(),
            name: "fs_read".into(),
            arguments: serde_json::json!({"path": "note.txt"}),
        },
        ScriptedResponse::Text("completion seen".into()),
    ];
    let (fx, _daemon, worker_mock, router_mock) = boot("async-start", worker_script, 4).await;
    let _ = &router_mock;
    let client = reqwest::Client::new();
    let worker = dispatch_worker(&client, &fx).await;

    // The start tool resolves as an honest `tool/result` with a job id,
    // well before the 3-second command could have finished.
    let began = Instant::now();
    let events = wait_for_session(&fx, worker, "start tool result", |events| {
        !tool_results(events).is_empty()
    })
    .await;
    let results = tool_results(&events);
    let output = results[0]["content"]["output"].clone();
    assert_eq!(output["status"], "started", "honest start ack: {output}");
    let job_id = output["job_id"].as_str().expect("job id").to_string();
    assert_eq!(
        job_id,
        kian_exec::tool::command_id_for_tool_call("call-start-1").to_string()
    );
    assert!(
        began.elapsed() < Duration::from_millis(1_500),
        "start must resolve while the 3s command runs (took {:?})",
        began.elapsed()
    );

    // Wait for the drain to park in the gated fourth provider turn.
    for _ in 0..1_600 {
        if worker_mock
            .requests
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 4
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The S2 path interleaved: a steer is queued **during the job**
    // (the command is still running; the drain is alive).
    rpc(
        &client,
        &fx,
        "/rpc",
        "steer",
        serde_json::json!({"session": worker.0, "content": "status?"}),
    )
    .await;
    assert!(
        completion_input(&fx, &job_id).await.is_none(),
        "the steer was queued while the job was still running"
    );

    // The completion lands as a durable input row (invariant 1: the
    // row exists before the wake that promotes it), provenance-wrapped.
    let payload = wait_completion_input(&fx, &job_id).await;
    assert_eq!(payload["kian"]["type"], "async_result");
    assert_eq!(payload["kian"]["status"], "done");
    assert_eq!(payload["kian"]["job_id"], job_id.as_str());

    // The job row itself settled done with the real outcome.
    let (status, result) = async_job_row(&fx, &job_id)
        .await
        .expect("async job row exists");
    assert_eq!(status, "done");
    assert!(
        result.contains("exit_code"),
        "real outcome recorded: {result}"
    );

    // Release the gate: the next provider turn promotes BOTH the
    // queued steer and the completion as visible messages (the exact
    // request bytes are the proof — persist-what-you-send).
    worker_mock
        .released
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let events = wait_for_session(
        &fx,
        worker,
        "steer + completion promoted and answered",
        |events| {
            let visible = events.iter().any(|e| {
                e.kind == EventKind::StepRequest
                    && e.bytes.to_string().contains("async_result")
                    && e.bytes.to_string().contains("status?")
            });
            let answered = events.iter().any(|e| {
                e.kind == EventKind::AssistantMessage
                    && e.bytes.to_string().contains("completion seen")
            });
            visible && answered
        },
    )
    .await;
    // Exactly one completion input, and it is consumed (the durable
    // consumption marker references a committed event of this stream).
    let consumed: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT consumed_by_seq FROM inputs WHERE idempotency_key = ?")
            .bind(format!("async-result:{job_id}"))
            .fetch_optional(&fx.pool)
            .await
            .expect("read consumption");
    assert!(
        consumed.and_then(|(seq,)| seq).is_some(),
        "the completion input is consumed by the drain"
    );
    let _ = (events, fx.dir);
}

// ---------------------------------------------------------------------------
// 2. stop: honest cancellation end to end
// ---------------------------------------------------------------------------

/// Same single-drain shape: the model calls `stop` on the running job,
/// the cancelled completion lands while the third provider turn is
/// gated, and it is promoted — labeled `cancelled` — at the next
/// boundary.
#[tokio::test]
async fn stop_mid_run_labels_cancelled_end_to_end() {
    // The model "learns" the job id deterministically: it is the
    // command id derived from the start call's id (ADR-008).
    let job_id = kian_exec::tool::command_id_for_tool_call("call-stop-start").to_string();
    let worker_script = vec![
        ScriptedResponse::ToolCall {
            id: "call-stop-start".into(),
            name: "start".into(),
            arguments: serde_json::json!({"argv": ["sleep", "30"]}),
        },
        ScriptedResponse::ToolCall {
            id: "call-stop-1".into(),
            name: "stop".into(),
            arguments: serde_json::json!({"job_id": job_id}),
        },
        // GATED (request 3): the cancelled completion lands meanwhile.
        ScriptedResponse::ToolCall {
            id: "call-read-1".into(),
            name: "fs_read".into(),
            arguments: serde_json::json!({"path": "note.txt"}),
        },
        ScriptedResponse::Text("cancel completion seen".into()),
    ];
    let (fx, _daemon, worker_mock, router_mock) = boot("async-stop", worker_script, 3).await;
    let _ = &router_mock;
    let client = reqwest::Client::new();
    let worker = dispatch_worker(&client, &fx).await;

    // The stop tool's own result is honest: status cancelled.
    let events = wait_for_session(&fx, worker, "stop tool result", |events| {
        tool_results(events)
            .iter()
            .any(|r| r["content"]["output"]["status"] == "cancelled")
    })
    .await;
    let results = tool_results(&events);
    assert!(results.len() >= 2, "start + stop results: {results:?}");

    // The job row settles cancelled…
    for _ in 0..1_600 {
        if let Some((status, _)) = async_job_row(&fx, &job_id).await {
            assert_eq!(status, "cancelled");
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // …and the cancelled completion still lands, labeled cancelled
    // (cancellation is an event, not an erasure).
    let payload = wait_completion_input(&fx, &job_id).await;
    assert_eq!(payload["kian"]["status"], "cancelled");

    // Release the gate: the cancelled completion becomes a visible
    // message and the worker answers.
    worker_mock
        .released
        .store(true, std::sync::atomic::Ordering::SeqCst);
    wait_for_session(&fx, worker, "cancel completion consumed", |events| {
        let visible = events.iter().any(|e| {
            e.kind == EventKind::StepRequest
                && e.bytes.to_string().contains("async_result")
                && e.bytes.to_string().contains("cancelled")
        });
        let answered = events.iter().any(|e| {
            e.kind == EventKind::AssistantMessage
                && e.bytes.to_string().contains("cancel completion seen")
        });
        visible && answered
    })
    .await;

    let _ = fx.dir;
}
