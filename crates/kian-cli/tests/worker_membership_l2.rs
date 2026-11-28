//! (R12-06 / kian-132, spec `shared/round12/spec.md` §8/§9) Real
//! two-process lifecycle L2: the **real `kian serve`** binary (a child
//! process) with the `workers:` registration listener, plus **real
//! `kiand`** child processes in `--register-to` (worker-dial) mode —
//! the runtime-membership legs the L1 suites cannot see. Fake token
//! values only, loopback only, `NO_PROXY=127.0.0.1,localhost` on every
//! local client (AGENTS.md gotcha).
//!
//! Legs (card §1–§8):
//!
//! 1. `join_without_restart_cli_list_spawn_exec_prefix` — join with NO
//!    kian restart and NO config edit → the real `kian workers --json`
//!    verb shows the worker online; the Router `list_workers` tool
//!    sees it; `spawn_worker` binds it; `exec` runs on the worker
//!    (endpoint-side marker file). Also captures the Router
//!    prompt+tool-schema prefix bytes across the join (leg 8).
//! 2. `bye_and_kill9_offline_watchdog_park_redrive_prefix` — SIGTERM →
//!    bye → offline; kill -9 → offline within `offline_after_ms`; the
//!    watchdog health input reaches the bound session; a queued
//!    command parks; an offline-known busy refusal still names the
//!    owner; reconnect re-drives with NO duplicate execution; prefix
//!    bytes identical across join AND leave (leg 8).
//! 3. `serve_kill9_restart_reregister_rebuild_one_end_write` — kian
//!    kill -9 mid-command → the worker re-registers (bounded backoff,
//!    no worker-side config change); the binding rebuilds from the
//!    stream (no rewrite); the row re-drives; exactly one end write;
//!    no cross-worker dispatch (a second dynamic worker stays idle).
//! 4. `worker_restart_same_id_reattaches_binding_unchanged` — the same
//!    worker id restarts (graceful) → registration reattaches; the
//!    live binding is unchanged; subsequent commands go to the same id.
//! 5. `refusals_bad_token_version_mismatch_static_collision` — wrong
//!    token, v0 version mismatch, static-id collision: refused
//!    honestly, no partial connection, no side effects, the worker
//!    logs an actionable (token-free) error.
//! 6. `static_and_dynamic_coexist_no_cross_dispatch` — a static
//!    `endpoints:` pin and a dynamic registration share one catalog;
//!    each executes only its own commands.
//! 7. `async_tools_follow_the_dynamic_worker` — the S3 carry on a
//!    dynamic side: the async job row carries the bound dynamic id and
//!    the side effect appears only on that worker's filesystem.
//!
//! ## Spawn recipe (for QA reuse, R12-07)
//!
//! ```text
//! # register-mode worker (its OWN temp dir is its cwd — the routing
//! # discriminator; readiness is the durable endpoint_registry row,
//! # observed through a read-only pool or `kian workers --json`):
//! KIAN_WORKER_TOKEN=<fake> NO_PROXY=127.0.0.1,localhost \
//!   <target>/kiand --register-to 127.0.0.1:<workers.listen> \
//!          --endpoint-id dyn-1 --description "…" \
//!          --heartbeat-ms 200 --exec-timeout-ms 60000   # cwd: <dir>/
//!
//! # server (token value env-only, named by workers.token_env):
//! KIAN_E2E_WORKER_TOKEN=<fake> NO_PROXY=127.0.0.1,localhost \
//!   <target>/kian serve --config <dir>/kian.yaml
//! ```
//!
//! All waits poll observable facts (committed events via a read-only
//! store handle, registry/queue rows, marker files, real CLI output) —
//! never log text, never sleeps.

mod common;

use std::os::fd::{FromRawFd as _, IntoRawFd as _};
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
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
/// Fake credential material for loopback tests only — never a real one.
const WORKER_TOKEN: &str = "l2-fake-worker-token-0123456789abcdef";
/// The env var `workers.token_env` names on the kian side.
const SERVER_TOKEN_ENV: &str = "KIAN_E2E_WORKER_TOKEN";
/// The env var the register-mode kiand reads by default (the
/// implementation's `DEFAULT_TOKEN_ENV`; the spec's §2.3 spelling
/// `KIAND_WORKER_TOKEN` was normalized to the server-side name when
/// R12-03 landed — recorded in this card's deviations).
const KIAND_TOKEN_ENV: &str = "KIAN_WORKER_TOKEN";

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Child processes
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

fn child_env(command: &mut Command) {
    command
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_DEDUPE", "0")
        .env("no_proxy", "127.0.0.1,localhost")
        .stdin(Stdio::null())
        .kill_on_drop(true);
}

/// A register-mode (worker-dial) kiand: cwd `dir` so every exec side
/// effect with a relative path lands there — the routing
/// discriminator. Readiness is the durable registry row, not stdout
/// (register mode prints no listening line).
struct RegisterDaemon {
    child: Child,
}

impl RegisterDaemon {
    fn spawn(register_to: &str, id: &str, dir: &Path, token: &str) -> RegisterDaemon {
        Self::spawn_with_capabilities(register_to, id, dir, token, &[])
    }

    fn spawn_with_capabilities(
        register_to: &str,
        id: &str,
        dir: &Path,
        token: &str,
        capabilities: &[&str],
    ) -> RegisterDaemon {
        // stderr → a file in the worker dir: register-mode logs go to
        // stderr, and keeping them makes L2 failures diagnosable.
        let stderr_path = dir.join(format!("{id}.kiand.stderr.log"));
        let stderr = std::fs::File::create(&stderr_path)
            .unwrap_or_else(|e| panic!("create {}: {e}", stderr_path.display()));
        let stderr =
            unsafe { Stdio::from(std::os::fd::OwnedFd::from_raw_fd(stderr.into_raw_fd())) };
        let mut command = Command::new(kiand_bin());
        command.args([
            "--register-to",
            register_to,
            "--endpoint-id",
            id,
            "--description",
            "l2 membership daemon",
            "--heartbeat-ms",
            "200",
            "--exec-timeout-ms",
            "60000",
        ]);
        for capability in capabilities {
            command.args(["--capability", capability]);
        }
        command
            .env(KIAND_TOKEN_ENV, token)
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(stderr);
        child_env(&mut command);
        RegisterDaemon {
            child: command.spawn().expect("spawn register-mode kiand"),
        }
    }

    /// kill -9 (SIGKILL) and reap.
    async fn kill9(&mut self) {
        self.child.kill().await.expect("kill -9 register kiand");
        let _ = self.child.wait().await;
    }

    /// SIGTERM → the daemon drains, sends `bye`, exits; returns its status.
    async fn sigterm_and_wait(&mut self) -> std::process::ExitStatus {
        let pid = self.child.id().expect("live register kiand pid");
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("send SIGTERM");
        self.child.wait().await.expect("register kiand exits")
    }
}

/// A listen-mode kiand (the static `endpoints:` shape, unchanged from
/// R11 — the server dials it).
struct ListenDaemon {
    child: Child,
}

impl ListenDaemon {
    fn spawn(listen: &str, dir: &Path) -> ListenDaemon {
        let mut command = Command::new(kiand_bin());
        command
            .args([
                "--listen",
                listen,
                "--heartbeat-ms",
                "100",
                "--exec-timeout-ms",
                "60000",
            ])
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        child_env(&mut command);
        ListenDaemon {
            child: command.spawn().expect("spawn listen-mode kiand"),
        }
    }

    /// Waits for the daemon's stdout listening line (deterministic readiness).
    async fn wait_listening(&mut self) {
        let stdout = self.child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match timeout(Duration::from_secs(15), lines.next_line()).await {
                Ok(Ok(Some(line))) if line.contains(" listening on ") => return,
                Ok(Ok(Some(_))) => continue,
                Ok(Ok(None)) => panic!("listen kiand exited before its listening line"),
                Ok(Err(e)) => panic!("cannot read listen kiand stdout: {e}"),
                Err(_) => panic!("listen kiand did not print its listening line within 15s"),
            }
        }
    }

    async fn kill9(&mut self) {
        self.child.kill().await.expect("kill -9 listen kiand");
        let _ = self.child.wait().await;
    }
}

async fn listen_daemon_on(listen: &str, dir: &Path) -> ListenDaemon {
    let mut daemon = ListenDaemon::spawn(listen, dir);
    daemon.wait_listening().await;
    daemon
}

fn spawn_kian_serve(config: &Path) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .env(common::KEY_ENV, "test-key")
        .env(SERVER_TOKEN_ENV, WORKER_TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    child_env(&mut command);
    command.spawn().expect("spawn kian serve")
}

/// Runs the real `kian workers --config <config> --json` (the operator
/// verb, a separate process) and parses its board.
async fn kian_workers_json(config: &Path) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["workers", "--config", config.to_str().unwrap(), "--json"])
        .env(common::KEY_ENV, "test-key")
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run kian workers");
    assert!(
        output.status.success(),
        "kian workers never refuses: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("kian workers --json parses")
}

/// One board row's `(state)` from the real CLI output.
fn cli_state<'a>(board: &'a serde_json::Value, id: &str) -> Option<&'a str> {
    board["workers"]
        .as_array()
        .expect("workers array")
        .iter()
        .find(|row| row["id"].as_str() == Some(id))
        .and_then(|row| row["state"].as_str())
}

// ---------------------------------------------------------------------------
// Config + fixture
// ---------------------------------------------------------------------------

/// One static `endpoints:` entry (id + listen-mode daemon port).
struct StaticSpec {
    id: &'static str,
    port: u16,
    description: &'static str,
}

/// The `workers:` section (spec C1).
struct WorkersSpec {
    listen_port: u16,
    offline_after_ms: u64,
}

/// Writes the super-agent `kian.yaml` with optional static endpoints
/// and an optional `workers:` membership section. `remote-exec` is
/// present iff either surface is configured (C6).
fn write_membership_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    serve_port: u16,
    statics: &[StaticSpec],
    workers: Option<&WorkersSpec>,
    async_tools: bool,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let async_section = if async_tools {
        "  async_tools: true\n"
    } else {
        ""
    };
    let mut endpoints_yaml = String::new();
    for ep in statics {
        endpoints_yaml.push_str(&format!(
            "  - id: {id}\n    addr: \"127.0.0.1:{port}\"\n    description: \"{desc}\"\n    queue_timeout_ms: 60000\n    offline_after_ms: 8000\n",
            id = ep.id,
            port = ep.port,
            desc = ep.description,
        ));
    }
    let workers_yaml = workers
        .map(|w| {
            format!(
                "workers:\n  listen: \"127.0.0.1:{port}\"\n  token_env: {SERVER_TOKEN_ENV}\n  offline_after_ms: {offline}\n",
                port = w.listen_port,
                offline = w.offline_after_ms,
            )
        })
        .unwrap_or_default();
    let remote = if statics.is_empty() && workers.is_none() {
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
{workers_yaml}endpoints:
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
        workers_yaml = workers_yaml,
        endpoints_yaml = if statics.is_empty() && workers.is_some() {
            "  []\n".to_string()
        } else {
            endpoints_yaml
        },
        remote = remote,
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write membership config");
    path
}

/// The observation fixture: committed streams through a read-only
/// store handle plus a read-only sqlite pool for registry/queue rows.
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

async fn boot_serve(config: &Path, port: u16) -> (Child, String) {
    let mut child = spawn_kian_serve(config);
    let base = format!("http://127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let client = reqwest::Client::new();
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
            child.stdout.take();
            child.stderr.take();
            return (child, base);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "kian serve never became ready (config: {})",
            config.display()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn kill_serve(child: &mut Child) {
    child.kill().await.expect("kill -9 serve");
    let _ = child.wait().await;
}

// ---------------------------------------------------------------------------
// Observation helpers (committed facts only)
// ---------------------------------------------------------------------------

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
    panic!("timed out waiting for: {desc}; stream:\n  {events:?}");
}

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

async fn router_thread_started(fx: &Fixture) -> Vec<serde_json::Value> {
    let events = fx.store.rebuild(router_session()).await.expect("rebuild");
    events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .collect()
}

/// Waits for the Nth `thread/started` on the Router stream and returns
/// (worker session, thread id, exec_worker).
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

fn first_wrapper(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .and_then(|e| e.bytes.get("content"))
        .cloned()
        .expect("a first input/admitted with content")
}

fn worker_started_bytes(events: &[kian_core::events::Event]) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| e.bytes.clone())
        .expect("the worker stream carries thread/started")
}

fn tool_result_strings(events: &[kian_core::events::Event]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e.kind == EventKind::ToolResult)
        .map(|e| serde_json::to_string(&e.bytes).expect("serialize"))
        .collect()
}

/// Every accepted spawn on the Router stream in order: the judged task and
/// the thread it minted. One per accepted `spawn_worker` call, none for a
/// refused one, so a count above the accepted calls means an input was judged
/// again after its worker slot had been released.
fn router_lineages(events: &[kian_core::events::Event]) -> Vec<(String, String)> {
    events
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .map(|e| {
            let content = &e.bytes["task"]["content"];
            (
                content["task"].as_str().unwrap_or("<unset>").to_string(),
                content["thread_id"]
                    .as_str()
                    .unwrap_or("<unset>")
                    .to_string(),
            )
        })
        .collect()
}

/// Replays what the Router decided: which input each turn judged, which tool
/// calls it made, what they answered and which spawns were accepted. Printed
/// on a lineage-count failure so the extra spawn names its own task.
fn router_decision_trail(events: &[kian_core::events::Event]) -> String {
    fn short(value: &str) -> String {
        value.chars().take(160).collect()
    }
    let mut trail = Vec::new();
    for e in events {
        let line = match e.kind {
            EventKind::StepRequest => {
                let judged = e.bytes["body"]["messages"]
                    .as_array()
                    .and_then(|messages| messages.iter().rev().find(|m| m["role"] == "user"))
                    .map(|m| short(m["content"].as_str().unwrap_or("")))
                    .unwrap_or_default();
                format!("{:>3} step/request judged: {judged}", e.seq)
            }
            EventKind::ToolCall => format!(
                "{:>3} tool/call {}: {}",
                e.seq,
                e.bytes["content"]["id"].as_str().unwrap_or("?"),
                short(&e.bytes["content"]["arguments"].to_string())
            ),
            EventKind::ToolResult => format!(
                "{:>3} tool/result {}: {}",
                e.seq,
                e.bytes["content"]["call_id"].as_str().unwrap_or("?"),
                short(&e.bytes["content"].to_string())
            ),
            EventKind::ThreadStarted => {
                let (task, thread) = router_lineages(std::slice::from_ref(e)).remove(0);
                format!("{:>3} thread/started {thread}: {task}", e.seq)
            }
            _ => continue,
        };
        trail.push(line);
    }
    trail.join("\n")
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

/// One registry row `(state, last_heartbeat_ms)`.
async fn registry_row(fx: &Fixture, id: &str) -> Option<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT state, last_heartbeat_ms FROM endpoint_registry WHERE endpoint_id = ?",
    )
    .bind(id)
    .fetch_optional(&fx.pool)
    .await
    .expect("read endpoint_registry")
}

/// Polls until the registry row for `id` reads `online`/`offline`
/// through the D-L2 conjunction (row state AND freshness).
async fn wait_registry_view(fx: &Fixture, id: &str, want_online: bool, offline_after_ms: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(offline_after_ms + 10_000);
    loop {
        if let Some((state, last)) = registry_row(fx, id).await {
            let online =
                state == "online" && now_ms().saturating_sub(last as u64) <= offline_after_ms;
            if online == want_online {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "registry view for `{id}` never became {}; last row: {:?}",
            if want_online { "online" } else { "offline" },
            registry_row(fx, id).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One exec queue row status for a tool call id.
async fn exec_queue_status(fx: &Fixture, call_id: &str) -> Option<String> {
    let command_id = kian_exec::tool::command_id_for_tool_call(call_id);
    let text = command_id.to_string();
    sqlx::query_as::<_, (String,)>("SELECT status FROM exec_queue WHERE command_id = ?")
        .bind(&text)
        .fetch_optional(&fx.pool)
        .await
        .expect("read exec_queue")
        .map(|(status,)| status)
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

// ---------------------------------------------------------------------------
// Script builders + the prefix fingerprint (leg 8)
// ---------------------------------------------------------------------------

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

/// The membership-sensitive part of a judge request prefix: the tool
/// schemas and the leading (system) message — the bytes a join/leave
/// must never touch (I6/leg 8). Membership data lives only in tool
/// RESULTS, which are message-tail content, never in this prefix.
fn prefix_fingerprint(body: &serde_json::Value) -> (String, String) {
    let tools = serde_json::to_string(body.get("tools").unwrap_or(&serde_json::Value::Null))
        .expect("serialize tools");
    let head = serde_json::to_string(
        body.pointer("/messages/0")
            .unwrap_or(&serde_json::Value::Null),
    )
    .expect("serialize first message");
    (tools, head)
}

/// The Router stream's per-request prefix fingerprints, in order.
async fn router_prefixes(fx: &Fixture) -> Vec<(String, String)> {
    let events = fx.store.rebuild(router_session()).await.expect("rebuild");
    events
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
        .filter_map(|e| e.bytes.get("body"))
        .map(prefix_fingerprint)
        .collect()
}

// ---------------------------------------------------------------------------
// The task-routed worker mock (per-session scripts keyed by task tag)
// ---------------------------------------------------------------------------

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
        ScriptedResponse::DieMidStream => common::sse_completion_body("die-mid-stream unused here"),
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

// ---------------------------------------------------------------------------
// The user-routed router mock (test 2): judge turns are keyed by their
// NEWEST input (the last user message of the request body), so each
// probe turn gets its own scripted response regardless of how turns
// batch or merge — positional scripts race when a worker completion
// report merges with a probe event (observed).
// ---------------------------------------------------------------------------

struct UserRoute {
    tag: &'static str,
    response: ScriptedResponse,
}

async fn completions_user_routed(
    State(state): State<Arc<Vec<UserRoute>>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let text = String::from_utf8_lossy(&body).to_string();
    // The newest judged input: the LAST user-role message of the body.
    let last_user = serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.pointer("/messages").and_then(|m| m.as_array()).cloned())
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|m| m.get("role") == Some(&serde_json::json!("user")))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default();
    let route = state
        .iter()
        .find(|r| last_user.contains(r.tag))
        .map(|r| &r.response);
    let body = match route {
        Some(ScriptedResponse::Text(text)) => common::sse_completion_body(text),
        Some(ScriptedResponse::ToolCall {
            id,
            name,
            arguments,
        }) => common::sse_tool_call_body(
            id,
            name,
            &serde_json::to_string(arguments).expect("tool arguments serialize"),
        ),
        _ => common::sse_completion_body("noted"),
    };
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        "content-type",
        axum::http::HeaderValue::from_static("text/event-stream"),
    );
    response
}

/// A mock whose responses key on the newest judged input (deterministic
/// per probe turn, immune to batch merging).
async fn spawn_user_routed_provider(routes: Vec<UserRoute>) -> String {
    let app = Router::new()
        .route("/chat/completions", post(completions_user_routed))
        .with_state(Arc::new(routes));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind user-routed mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serve");
    });
    format!("http://{addr}")
}

async fn spawn_task_routed_provider(routes: Vec<Route>) -> String {
    assert!(!routes.is_empty(), "at least one route");
    let mut states = Vec::new();
    for route in routes {
        states.push(RouteState {
            tag: route.tag.to_string(),
            requests: Arc::new(AtomicUsize::new(0)),
            script: route.script,
            gate_at: route.gate_at,
            released: Arc::new(AtomicBool::new(false)),
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
// 1. Join — no restart, no config edit (leg 1) + prefix across join (leg 8)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_without_restart_cli_list_spawn_exec_prefix() {
    force_no_proxy();
    let dir = temp_dir("l2m-join");
    let dir_w = dir.join("worker-dyn");
    std::fs::create_dir_all(&dir_w).expect("worker dir");

    let serve_port = free_port();
    let listen_port = free_port();
    let offline_after = 4_000u64;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call("call-list", "list_workers", serde_json::json!({})),
            spawn_call("call-spawn-join", "job-join: run the join check", "dyn-1"),
            text("join dispatched"),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![
            exec_call("call-exec-join", "echo joined > joined.txt"),
            text("join done"),
        ],
        0,
    )
    .await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: offline_after,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    // The bootstrap judge request is persisted BEFORE any worker joins
    // — the pre-join prefix snapshot (leg 8).
    wait_router_bootstrapped(&fx).await;
    let pre_join = router_prefixes(&fx).await;
    assert!(!pre_join.is_empty(), "the bootstrap judge request exists");

    // JOIN: start the register-mode kiand. No kian restart, no config
    // edit — the whole point of leg 1.
    let mut worker = RegisterDaemon::spawn(
        &format!("127.0.0.1:{listen_port}"),
        "dyn-1",
        &dir_w,
        WORKER_TOKEN,
    );

    // The real operator verb shows the worker online (durable view).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let board = kian_workers_json(&config).await;
        if cli_state(&board, "dyn-1") == Some("online") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "kian workers never showed dyn-1 online; board: {board}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;

    // The Router's `list_workers` tool sees it (R4) and `spawn_worker`
    // binds it (R1: plain-string required exec_worker honored).
    send_channel_event(&client, &fx, "please run the join job").await;
    let router_events = wait_for_session(
        &fx,
        router_session(),
        "list_workers result naming dyn-1 online",
        |events| {
            tool_result_strings(events)
                .iter()
                .any(|r| r.contains("\"id\":\"dyn-1\"") && r.contains("\"state\":\"online\""))
        },
    )
    .await;
    let listed = tool_result_strings(&router_events)
        .into_iter()
        .find(|r| r.contains("\"id\":\"dyn-1\""))
        .expect("the list_workers result");
    assert!(
        listed.contains("\"description\":\"l2 membership daemon\""),
        "the registered description is model-visible via the tool result: {listed}"
    );

    let (worker_session, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("dyn-1"));

    // The spawn result echoes the binding AND the observed member
    // state at spawn (R6).
    wait_for_session(
        &fx,
        router_session(),
        "spawn result echoes worker_state online",
        |events| {
            tool_result_strings(events).iter().any(|r| {
                r.contains("\"exec_worker\":\"dyn-1\"") && r.contains("\"worker_state\":\"online\"")
            })
        },
    )
    .await;

    // exec ran ON THE WORKER — asserted by the endpoint-side marker in
    // the daemon's own working directory, not by any success code.
    wait_for_file(&dir_w.join("joined.txt"), 20_000).await;
    let events = wait_for_session(&fx, worker_session, "worker settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;
    assert_eq!(
        worker_started_bytes(&events)["exec_worker"].as_str(),
        Some("dyn-1")
    );
    assert_eq!(
        first_wrapper(&events)
            .get("exec_worker")
            .and_then(|v| v.as_str()),
        Some("dyn-1"),
        "the worker's own wrapper carries the binding"
    );

    // Leg 8 (join half): the post-join judge requests carry the exact
    // pre-join prefix bytes — membership data never touched the prefix.
    let post_join = router_prefixes(&fx).await;
    assert!(post_join.len() > pre_join.len());
    for fingerprint in &post_join {
        assert_eq!(
            fingerprint, &pre_join[0],
            "membership changes never rewrite the prompt+tool-schema prefix"
        );
    }

    worker.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 2. Leave (bye + kill -9), watchdog, parked queue, busy-offline,
//    reconnect re-drive without duplicate execution (leg 2 + leg 8)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bye_and_kill9_offline_watchdog_park_redrive_prefix() {
    force_no_proxy();
    let dir = temp_dir("l2m-leave");
    let dir_w = dir.join("worker-dyn");
    std::fs::create_dir_all(&dir_w).expect("worker dir");
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");

    let serve_port = free_port();
    let listen_port = free_port();
    let offline_after = 4_000u64;
    let register_to = format!("127.0.0.1:{listen_port}");

    let router_base = spawn_user_routed_provider(vec![
        UserRoute {
            tag: "run the leave job",
            response: spawn_call("call-spawn-leave", "job-leave: the leave check", "dyn-1"),
        },
        // The busy probe: the SAME worker, offline but live-bound —
        // must still be refused (I8: busy is live-session based).
        UserRoute {
            tag: "probe the offline worker",
            response: spawn_call(
                "call-spawn-busy-offline",
                "job-leave: busy probe on the offline worker",
                "dyn-1",
            ),
        },
        // The end-of-test offline-known probe: allowed, honest queue
        // note (R6's offline echo at L2).
        UserRoute {
            tag: "probe the offline note",
            response: spawn_call(
                "call-spawn-offline-note",
                "job-leave: the offline-note probe",
                "dyn-1",
            ),
        },
    ])
    .await;
    // Request 1: exec one (completes while the worker is up). Request 2
    // (gated) parks the drain before the second exec is even issued —
    // the deterministic kill window; released only once the offline
    // facts are observed. Request 3: the final answer.
    let (worker_base, worker_mock) = spawn_scripted_provider_gated_at(
        vec![
            exec_call("call-exec-leave-1", "echo one > leave1.txt"),
            exec_call("call-exec-leave-2", "echo two > leave2.txt"),
            text("leave done"),
        ],
        0,
        2,
    )
    .await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: offline_after,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    let pre_join_prefixes = router_prefixes(&fx).await;

    let mut worker = RegisterDaemon::spawn(&register_to, "dyn-1", &dir_w, WORKER_TOKEN);
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;

    send_channel_event(&client, &fx, "please run the leave job").await;
    let (worker_session, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("dyn-1"));

    // Command one completed on the worker; the drain reached (and parks
    // at) its gated second provider turn.
    wait_for_file(&dir_w.join("leave1.txt"), 20_000).await;
    for _ in 0..2_400 {
        if worker_mock.requests.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        worker_mock.requests.load(Ordering::SeqCst) >= 2,
        "the drain never reached its gated provider turn"
    );

    // Post-join prefix snapshot (before the leave) — identical bytes.
    for _ in 0..2_400 {
        if router_prefixes(&fx).await.len() > pre_join_prefixes.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let joined_prefixes = router_prefixes(&fx).await;
    for fingerprint in &joined_prefixes {
        assert_eq!(
            fingerprint, &pre_join_prefixes[0],
            "the join never rewrote the prefix bytes"
        );
    }

    // LEAVE (crash flavor): kill -9 the worker mid-park. Offline within
    // offline_after_ms (the honest bound; transport loss is faster).
    worker.kill9().await;
    let kill_instant = tokio::time::Instant::now();
    wait_registry_view(&fx, "dyn-1", false, offline_after).await;
    let board = kian_workers_json(&config).await;
    assert_eq!(
        cli_state(&board, "dyn-1"),
        Some("offline"),
        "the real CLI board shows the crashed worker offline: {board}"
    );
    assert!(
        kill_instant.elapsed() <= Duration::from_millis(offline_after + 5_000),
        "offline observed within offline_after_ms (+ polling margin)"
    );

    // Release the gate: the drain issues the second exec NOW, with no
    // route anywhere — the row must PARK (queue-first, I4), the caller
    // keeps awaiting, and no error reaches the model.
    worker_mock.released.store(true, Ordering::SeqCst);
    let mut parked = None;
    for _ in 0..2_400 {
        parked = exec_queue_status(&fx, "call-exec-leave-2").await;
        if parked.as_deref() == Some("pending") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        parked,
        Some("pending".to_string()),
        "no route ⇒ the row parks, never errors away"
    );

    // The watchdog health input reaches the BOUND session (M8: the
    // existing bridge, no new kind) — asserted on the DURABLE INPUT
    // ROW (admit-before-wake): the InputAdmitted event only projects
    // when the parked drain next resumes, so the row is the honest
    // early observable (the R11 async-completion discipline).
    let mut health_landed = false;
    for _ in 0..2_400 {
        let found: Option<(String,)> =
            sqlx::query_as("SELECT idempotency_key FROM inputs WHERE idempotency_key LIKE ?")
                .bind("endpoint-health:dyn-1:offline:%")
                .fetch_optional(&fx.pool)
                .await
                .expect("read inputs");
        if found.is_some() {
            health_landed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        health_landed,
        "the watchdog health input lands as a durable input row on the bound session"
    );

    // Busy is live-session based (I8): the session holding the parked
    // row is LIVE, so a second spawn on the offline worker is refused
    // naming the owner — never a silent succeed.
    send_channel_event(&client, &fx, "probe the offline worker please").await;
    let router_events = wait_for_session(
        &fx,
        router_session(),
        "offline-known busy refusal names the owner",
        |events| {
            tool_result_strings(events)
                .iter()
                .any(|r| r.contains("is busy") && r.contains(&worker_session.0.to_string()))
        },
    )
    .await;
    assert_eq!(
        router_events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1,
        "the refused probe leaves no thread/started"
    );

    // Leg 8 (leave half): the post-leave judge request prefix is still
    // byte-identical — a leave never rewrites the prefix either.
    let post_leave = router_prefixes(&fx).await;
    assert!(post_leave.len() > joined_prefixes.len());
    for fingerprint in &post_leave {
        assert_eq!(
            fingerprint, &pre_join_prefixes[0],
            "the leave never rewrote the prefix bytes"
        );
    }

    // RECONNECT: the same id re-registers; the parked row re-drives on
    // it (no cross-worker dispatch possible — dyn-1 is the only
    // worker) and executes EXACTLY ONCE (endpoint-side idempotency
    // witness: a single line in the marker file).
    let mut worker = RegisterDaemon::spawn(&register_to, "dyn-1", &dir_w, WORKER_TOKEN);
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;
    let leave2 = poll_file_until(&dir_w.join("leave2.txt"), 30_000, |c| !c.is_empty()).await;
    assert_eq!(
        leave2.lines().count(),
        1,
        "no duplicate execution across the park/re-drive: {leave2:?}"
    );
    let row = exec_queue_status(&fx, "call-exec-leave-2").await;
    assert_eq!(
        row.as_deref(),
        Some("done"),
        "the row settles done: {row:?}"
    );

    let events = wait_for_session(
        &fx,
        worker_session,
        "worker settles after re-drive",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
    )
    .await;
    // One exec call/result pair for the re-driven command — no phantom
    // duplicates visible to the loop (command-id dedupe).
    for call_id in ["call-exec-leave-1", "call-exec-leave-2"] {
        let results = events
            .iter()
            .filter(|e| e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == call_id)
            .count();
        assert_eq!(results, 1, "one result for {call_id}: {results}");
    }
    // The binding is unchanged across the whole leave/rejoin cycle.
    assert_eq!(
        worker_started_bytes(&events)["exec_worker"].as_str(),
        Some("dyn-1")
    );

    // LEAVE (graceful flavor): SIGTERM → bye → offline.
    let status = worker.sigterm_and_wait().await;
    assert!(status.success(), "graceful bye exits 0: {status}");
    wait_registry_view(&fx, "dyn-1", false, offline_after).await;
    let board = kian_workers_json(&config).await;
    assert_eq!(cli_state(&board, "dyn-1"), Some("offline"), "bye ⇒ offline");

    // The offline-known spawn (session 1 has completed, so dyn-1 is
    // free — just offline): ALLOWED, with the honest queue note and
    // the observed state echoed (R6). The spawned session's commands
    // park until the worker returns; the test ends here (the kill
    // below tears everything down, rows parked by design).
    send_channel_event(&client, &fx, "probe the offline note please").await;
    wait_for_session(
        &fx,
        router_session(),
        "offline-known spawn echoes the queue note",
        |events| {
            tool_result_strings(events).iter().any(|r| {
                r.contains("\"exec_worker\":\"dyn-1\"")
                    && r.contains("\"worker_state\":\"offline\"")
                    && r.contains("offline — commands will queue")
            })
        },
    )
    .await;
    let router_events = fx.store.rebuild(router_session()).await.expect("rebuild");
    let lineages = router_lineages(&router_events);
    let spawned = |task: &str| {
        lineages
            .iter()
            .filter(|(spawned_task, _)| spawned_task == task)
            .count()
    };
    assert!(
        lineages.len() == 2
            && spawned("job-leave: the leave check") == 1
            && spawned("job-leave: busy probe on the offline worker") == 0
            && spawned("job-leave: the offline-note probe") == 1,
        "one lineage per accepted spawn: the leave probe and the offline-known probe \
         spawn, the refused busy probe leaves none, and no input is judged twice. \
         Got {} lineages:\n{}",
        lineages.len(),
        router_decision_trail(&router_events)
    );

    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 3. kian kill -9 / restart mid-command (leg 3)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_kill9_restart_reregister_rebuild_one_end_write() {
    force_no_proxy();
    let dir = temp_dir("l2m-serve-restart");
    let dir_1 = dir.join("worker-dyn-1");
    let dir_2 = dir.join("worker-dyn-2");
    std::fs::create_dir_all(&dir_1).expect("dir 1");
    std::fs::create_dir_all(&dir_2).expect("dir 2");

    let serve_port = free_port();
    let listen_port = free_port();
    let offline_after = 8_000u64;
    let register_to = format!("127.0.0.1:{listen_port}");

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            spawn_call(
                "call-spawn-restart",
                "job-restart: the kian restart check",
                "dyn-1",
            ),
            text("restart dispatched"),
        ],
        0,
    )
    .await;
    // A two-phase command observable on the daemon's filesystem:
    // `started` proves it is mid-command (the kill point), `finished`
    // proves completion. NOTE (A3): at-least-once means the endpoint
    // MAY execute twice across a kian restart — the honest dedupe
    // witnesses are session-level (one tool result, one settled row,
    // one end write), asserted below.
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![
            exec_call(
                "call-exec-restart",
                "echo $$ > sh.pid; echo started > mid.txt; sleep 8; echo finished >> mid.txt",
            ),
            text("restart survived"),
        ],
        0,
    )
    .await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: offline_after,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;

    // Both dynamic workers register (dyn-2 is the no-cross-dispatch
    // witness: nothing of dyn-1's may ever land there).
    let mut worker_1 = RegisterDaemon::spawn(&register_to, "dyn-1", &dir_1, WORKER_TOKEN);
    let mut worker_2 = RegisterDaemon::spawn(&register_to, "dyn-2", &dir_2, WORKER_TOKEN);
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;
    wait_registry_view(&fx, "dyn-2", true, offline_after).await;

    send_channel_event(&client, &fx, "please run the restart job").await;
    let (worker_session, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("dyn-1"));

    // Genuinely mid-command: phase one is on disk.
    poll_file_until(&dir_1.join("mid.txt"), 20_000, |c| c.contains("started")).await;
    let pre_kill = fx.store.rebuild(worker_session).await.expect("rebuild");
    let pre_started = worker_started_bytes(&pre_kill);
    let pre_wrapper = first_wrapper(&pre_kill);

    // kill -9 kian mid-command, then restart it on the same store. The
    // workers are untouched: no config change, no restart — dyn-1's
    // connection dies with kian and it re-registers with bounded
    // backoff (I3).
    kill_serve(&mut serve).await;
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;
    wait_registry_view(&fx, "dyn-2", true, offline_after).await;

    // The row re-drove and the session completed; the binding rebuilt
    // from the stream (byte-identical, never rewritten). NOTE the
    // honest A3 shape at model level: the re-armed drain continues
    // from the DANGLING exec tool call (it does not re-execute the
    // tool as a new call — asserted below); the re-drive happens at
    // the queue layer and settles the durable row (asserted below).
    let events = wait_for_session(
        &fx,
        worker_session,
        "worker completes after kian restart",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
    )
    .await;
    poll_file_until(&dir_1.join("mid.txt"), 30_000, |c| c.contains("finished")).await;
    assert_eq!(
        worker_started_bytes(&events),
        pre_started,
        "the binding payload is byte-identical across the kian restart"
    );
    assert_eq!(
        first_wrapper(&events),
        pre_wrapper,
        "the wrapper is byte-identical across the kian restart"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1
    );

    // Exactly one end write (the single atomic session end; a restart
    // must never produce a second one).
    for _ in 0..2_400 {
        let events = fx.store.rebuild(worker_session).await.expect("rebuild");
        let ends = events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
            .count();
        if ends >= 1 {
            assert_eq!(ends, 1, "exactly one end write: {ends}");
            break;
        }
        assert!(
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
            "the session completed without an end write?"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The dangling exec call is never re-issued as a NEW model-level
    // tool call (no phantom duplicate at the loop level).
    let exec_calls = events
        .iter()
        .filter(|e| {
            e.kind == EventKind::ToolCall
                && e.bytes["content"]["name"] == "exec"
                && e.bytes["content"]["id"] == "call-exec-restart"
        })
        .count();
    assert_eq!(
        exec_calls, 1,
        "the dangling call is not re-issued: {exec_calls}"
    );

    // The queue-layer re-drive (A3 at-least-once): the re-registered
    // route re-drives the `inflight` row and the second execution
    // settles it `done` — the honest durable witness that the row
    // re-drove (its result has no live caller after the restart, by
    // design: the waiter died with the old process).
    let mut row = None;
    for _ in 0..2_400 {
        row = exec_queue_status(&fx, "call-exec-restart").await;
        if row.as_deref() == Some("done") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        row,
        Some("done".to_string()),
        "the re-driven row settles done on dyn-1: {row:?}"
    );

    // The mid-command marker completed (>=1 `finished`: at-least-once
    // may append one per actual execution — A3 records this honestly;
    // dedupe is asserted at the row/settlement level above).
    let mid_txt = std::fs::read_to_string(dir_1.join("mid.txt")).expect("mid.txt");
    assert!(
        mid_txt.matches("finished").count() >= 1,
        "the command completed on the worker: {mid_txt:?}"
    );

    // No cross-worker dispatch: dyn-2 saw nothing at all.
    assert!(
        !dir_2.join("mid.txt").exists() && !dir_2.join("sh.pid").exists(),
        "dyn-1's command must never land on dyn-2"
    );

    worker_1.kill9().await;
    worker_2.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4. Worker restart / re-register with the same id (leg 4)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_restart_same_id_reattaches_binding_unchanged() {
    force_no_proxy();
    let dir = temp_dir("l2m-same-id");
    let dir_w = dir.join("worker-dyn");
    std::fs::create_dir_all(&dir_w).expect("worker dir");

    let serve_port = free_port();
    let listen_port = free_port();
    let offline_after = 4_000u64;
    let register_to = format!("127.0.0.1:{listen_port}");

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            spawn_call(
                "call-spawn-same-id",
                "job-same-id: the reattach check",
                "dyn-1",
            ),
            text("same-id dispatched"),
        ],
        0,
    )
    .await;
    // Request 1: command one (worker up). Request 2 (gated): the drain
    // parks here while the worker restarts under it; on release it
    // issues command two — which must run on the RE-REGISTERED same-id
    // worker. Request 3: settle.
    let (worker_base, worker_mock) = spawn_scripted_provider_gated_at(
        vec![
            exec_call("call-exec-same-1", "echo one > same1.txt"),
            exec_call("call-exec-same-2", "echo two > same2.txt"),
            text("same-id done"),
        ],
        0,
        2,
    )
    .await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: offline_after,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    let mut worker = RegisterDaemon::spawn(&register_to, "dyn-1", &dir_w, WORKER_TOKEN);
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;

    send_channel_event(&client, &fx, "please run the same-id job").await;
    let (worker_session, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("dyn-1"));

    // Command one ran; the drain parks at the gated second turn (the
    // session stays LIVE through the restart window).
    wait_for_file(&dir_w.join("same1.txt"), 20_000).await;
    for _ in 0..2_400 {
        if worker_mock.requests.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(worker_mock.requests.load(Ordering::SeqCst) >= 2);
    let pre_restart = fx.store.rebuild(worker_session).await.expect("rebuild");
    let pre_started = worker_started_bytes(&pre_restart);

    // The upgrade/restart window: the same id leaves gracefully and
    // comes back — a fresh registration that REATTACHES (the registry
    // row goes offline then online; never a new id).
    let status = worker.sigterm_and_wait().await;
    assert!(status.success());
    wait_registry_view(&fx, "dyn-1", false, offline_after).await;
    let mut worker = RegisterDaemon::spawn(&register_to, "dyn-1", &dir_w, WORKER_TOKEN);
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;

    // Phase two (a queued input consumed by the still-live drain) and
    // the released gate: the second command runs on the same id.
    let out = rpc(
        &client,
        &fx.base,
        "/rpc",
        "queue",
        serde_json::json!({"session": worker_session.0, "content": "phase two please"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    worker_mock.released.store(true, Ordering::SeqCst);

    let events = wait_for_session(
        &fx,
        worker_session,
        "worker settles after restart",
        |events| events.iter().any(|e| e.kind == EventKind::ThreadCompleted),
    )
    .await;
    wait_for_file(&dir_w.join("same2.txt"), 20_000).await;

    // The live binding is UNCHANGED: one thread/started, identical
    // bytes, subsequent commands went to the same id.
    assert_eq!(worker_started_bytes(&events), pre_started);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        1
    );
    let row = exec_queue_status(&fx, "call-exec-same-2").await;
    assert_eq!(
        row,
        Some("done".to_string()),
        "command two settled on the same id: {row:?}"
    );

    worker.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 5. Refusals — wrong token, version mismatch, static-id collision (leg 5)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_bad_token_version_mismatch_static_collision() {
    force_no_proxy();
    let dir = temp_dir("l2m-refusals");
    let dir_static = dir.join("worker-static");
    std::fs::create_dir_all(&dir_static).expect("static dir");

    let serve_port = free_port();
    let listen_port = free_port();
    let static_port = free_port();

    let (router_base, _router_mock) =
        spawn_scripted_provider(vec![text("bootstrap acknowledged")], 0).await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(vec![text("unused")], 0).await;

    // A static pin exists: `w-static` is server-dial, so a dynamic
    // registration of that id must be refused duplicate_static (C5).
    let static_listen = format!("127.0.0.1:{static_port}");
    let mut static_daemon = listen_daemon_on(&static_listen, &dir_static).await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[StaticSpec {
            id: "w-static",
            port: static_port,
            description: "the static pin — the collision target",
        }],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: 4_000,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let register_to = format!("127.0.0.1:{listen_port}");

    // (a) Wrong token: the real register-mode kiand must exit non-zero
    // with an actionable, token-free error naming the env variable.
    let mut wrong = Command::new(kiand_bin());
    wrong
        .args([
            "--register-to",
            register_to.as_str(),
            "--endpoint-id",
            "bad-token-worker",
            "--heartbeat-ms",
            "200",
        ])
        .env(KIAND_TOKEN_ENV, "definitely-the-wrong-token")
        .env("KIAND_DEDUPE", "0")
        .current_dir(&dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let wrong: tokio::process::Child = wrong.spawn().expect("spawn wrong-token kiand");
    let output = match timeout(Duration::from_secs(15), wrong.wait_with_output()).await {
        Ok(output) => output.expect("wrong-token kiand exits"),
        Err(_) => panic!("wrong-token kiand did not exit within 15s"),
    };
    assert_ne!(output.status.code(), Some(0), "refused honestly, non-zero");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("registration failed") && stderr.contains(KIAND_TOKEN_ENV),
        "the worker logs an actionable error: {stderr}"
    );
    assert!(
        !stderr.contains(WORKER_TOKEN),
        "no token material in the worker's error: {stderr}"
    );

    // No partial connection / no side effects: no registry row for the
    // refused id.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        registry_row(&fx, "bad-token-worker").await.is_none(),
        "a refused registration leaves no member"
    );

    // (b) Version mismatch (A9): a hand-rolled v0 registration frame —
    // the honest stand-in for an old worker build (no v0 binary exists
    // to spawn; the L1 golden pins the same refusal). The server must
    // answer the typed refusal and close — never a half-connection.
    let mut stream = tokio::net::TcpStream::connect(&register_to)
        .await
        .expect("dial the registration listener");
    let v0_frame = serde_json::json!({
        "type": "register",
        "endpoint_id": "v0-ghost",
        "token": WORKER_TOKEN,
        "version": 0,
        "max_concurrent": 1,
    });
    stream
        .write_all(format!("{}\n", v0_frame).as_bytes())
        .await
        .expect("send v0 register");
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    match timeout(Duration::from_secs(10), reader.read_line(&mut line)).await {
        Ok(Ok(n)) if n > 0 => {}
        other => panic!("no rejection frame arrived for a v0 register: {other:?}"),
    }
    let frame: serde_json::Value = serde_json::from_str(&line).expect("rejection parses");
    assert_eq!(frame["type"], "rejected", "typed refusal: {frame}");
    assert_eq!(frame["reason"], "version_mismatch", "reason: {frame}");
    // The connection is closed by the server (no partial connection).
    let mut tail = Vec::new();
    let closed = timeout(Duration::from_secs(5), reader.read_to_end(&mut tail))
        .await
        .expect("read to EOF")
        .expect("clean EOF");
    assert_eq!(closed, 0, "the server closed the connection: {tail:?}");
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        registry_row(&fx, "v0-ghost").await.is_none(),
        "a v0 registration leaves no member"
    );

    // (c) Static-id collision: a correctly-authenticated dynamic
    // registration of the static pin's id is refused duplicate_static,
    // the worker exits non-zero naming the pin, and the static entry
    // stays the pin (one registry row, no side effects on its daemon).
    let mut collide = Command::new(kiand_bin());
    collide
        .args([
            "--register-to",
            register_to.as_str(),
            "--endpoint-id",
            "w-static",
            "--heartbeat-ms",
            "200",
        ])
        .env(KIAND_TOKEN_ENV, WORKER_TOKEN)
        .env("KIAND_DEDUPE", "0")
        .current_dir(&dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let collide: tokio::process::Child = collide.spawn().expect("spawn colliding kiand");
    let output = match timeout(Duration::from_secs(15), collide.wait_with_output()).await {
        Ok(output) => output.expect("colliding kiand exits"),
        Err(_) => panic!("colliding kiand did not exit within 15s"),
    };
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("w-static") && stderr.contains("statically configured"),
        "the collision error names the static pin: {stderr}"
    );
    // The static pin keeps its single row and never executed anything
    // for the impostor (its working directory holds no marker files).
    assert!(registry_row(&fx, "w-static").await.is_some());
    assert!(
        std::fs::read_dir(&dir_static).expect("static dir").count() == 0,
        "the static daemon executed nothing for the refused registration"
    );

    static_daemon.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 6. Static + dynamic coexist (leg 6)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_and_dynamic_coexist_no_cross_dispatch() {
    force_no_proxy();
    let dir = temp_dir("l2m-coexist");
    let dir_static = dir.join("worker-static");
    let dir_dyn = dir.join("worker-dyn");
    std::fs::create_dir_all(&dir_static).expect("static dir");
    std::fs::create_dir_all(&dir_dyn).expect("dyn dir");

    let serve_port = free_port();
    let listen_port = free_port();
    let static_port = free_port();

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            tool_call("call-list-coexist", "list_workers", serde_json::json!({})),
            spawn_call(
                "call-spawn-static",
                "job-static: run the static job",
                "w-static",
            ),
            text("static dispatched"),
            spawn_call("call-spawn-dyn", "job-dyn: run the dynamic job", "dyn-1"),
            text("dyn dispatched"),
        ],
        0,
    )
    .await;
    let worker_base = spawn_task_routed_provider(vec![
        Route {
            tag: "job-static",
            script: vec![
                exec_call("call-exec-static", "echo static > static.txt"),
                text("static done"),
            ],
            gate_at: 0,
        },
        Route {
            tag: "job-dyn",
            script: vec![
                exec_call("call-exec-dyn", "echo dyn > dyn.txt"),
                text("dyn done"),
            ],
            gate_at: 0,
        },
    ])
    .await;

    let static_listen = format!("127.0.0.1:{static_port}");
    let mut static_daemon = listen_daemon_on(&static_listen, &dir_static).await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[StaticSpec {
            id: "w-static",
            port: static_port,
            description: "the static pin",
        }],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: 4_000,
        }),
        false,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;

    // The dynamic worker joins the same catalog as the static pin.
    let mut dyn_worker = RegisterDaemon::spawn(
        &format!("127.0.0.1:{listen_port}"),
        "dyn-1",
        &dir_dyn,
        WORKER_TOKEN,
    );
    wait_registry_view(&fx, "dyn-1", true, 4_000).await;
    wait_registry_view(&fx, "w-static", true, 8_000).await;

    // list_workers carries BOTH — one catalog (I10).
    send_channel_event(&client, &fx, "please run both jobs").await;
    wait_for_session(
        &fx,
        router_session(),
        "list_workers shows both ids",
        |events| {
            let Some(listed) = tool_result_strings(events)
                .into_iter()
                .find(|r| r.contains("\"id\":\"w-static\""))
            else {
                return false;
            };
            listed.contains("\"id\":\"dyn-1\"")
        },
    )
    .await;

    // The static spawn settled first (its judge turn precedes the
    // dynamic one in the script).
    let (static_session, _t1, static_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(static_worker.as_deref(), Some("w-static"));
    wait_for_session(
        &fx,
        router_session(),
        "static judge turn settled",
        |events| {
            events.iter().any(|e| {
                e.kind == EventKind::AssistantMessage
                    && e.bytes.pointer("/content/text")
                        == Some(&serde_json::json!("static dispatched"))
            })
        },
    )
    .await;
    send_channel_event(&client, &fx, "now the dynamic job please").await;
    let (dyn_session, _t2, dyn_worker_id) = nth_spawn(&fx, 1).await;
    assert_eq!(dyn_worker_id.as_deref(), Some("dyn-1"));
    assert_ne!(static_session, dyn_session);

    // Each command ran ONLY on its own daemon (side effects, not logs).
    wait_for_file(&dir_static.join("static.txt"), 20_000).await;
    wait_for_file(&dir_dyn.join("dyn.txt"), 20_000).await;
    assert!(
        !dir_dyn.join("static.txt").exists(),
        "the static job must never run on the dynamic worker"
    );
    assert!(
        !dir_static.join("dyn.txt").exists(),
        "the dynamic job must never run on the static worker"
    );
    for (session, id) in [(static_session, "w-static"), (dyn_session, "dyn-1")] {
        let events = wait_for_session(&fx, session, "session settles", |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
        })
        .await;
        assert_eq!(
            worker_started_bytes(&events)["exec_worker"].as_str(),
            Some(id)
        );
    }

    dyn_worker.kill9().await;
    static_daemon.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 7. async_tools × dynamic worker — the S3 carry (leg 7)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_tools_follow_the_dynamic_worker() {
    force_no_proxy();
    let dir = temp_dir("l2m-async");
    let dir_w = dir.join("worker-dyn");
    std::fs::create_dir_all(&dir_w).expect("worker dir");
    std::fs::write(dir.join("note.txt"), "a file to read between turns").expect("note file");

    let serve_port = free_port();
    let listen_port = free_port();
    let offline_after = 4_000u64;

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            text("bootstrap acknowledged"),
            spawn_call(
                "call-spawn-async",
                "job-async: the dynamic async check",
                "dyn-1",
            ),
            text("async dispatched"),
        ],
        0,
    )
    .await;
    // The drain must stay LIVE while the job settles (the completion
    // bridge honestly drops a completion for a terminal session): gate
    // the third provider turn — the would-be final answer.
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
            text("async completion seen"),
        ],
        0,
        3,
    )
    .await;

    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: offline_after,
        }),
        true,
    );
    let (mut serve, base) = boot_serve(&config, serve_port).await;
    let fx = open_fixture(&base, &dir).await;
    let client = reqwest::Client::new();

    wait_router_bootstrapped(&fx).await;
    let mut worker = RegisterDaemon::spawn(
        &format!("127.0.0.1:{listen_port}"),
        "dyn-1",
        &dir_w,
        WORKER_TOKEN,
    );
    wait_registry_view(&fx, "dyn-1", true, offline_after).await;

    send_channel_event(&client, &fx, "please run the async job").await;
    let (worker_session, _thread, exec_worker) = nth_spawn(&fx, 0).await;
    assert_eq!(exec_worker.as_deref(), Some("dyn-1"));

    // The per-side async pair carries the BOUND dynamic id (S3): the
    // job row names dyn-1, and the command's side effect appears only
    // on the registering worker's filesystem.
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
        Some(("dyn-1".to_string(),)),
        "the async row rides the dynamic side's own id: {row:?}"
    );
    wait_for_file(&dir_w.join("async.txt"), 30_000).await;

    // The completion lands as a durable input row while the drain is
    // parked, then the session settles having seen it.
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
    wait_for_session(&fx, worker_session, "async worker settles", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
    })
    .await;

    worker.kill9().await;
    kill_serve(&mut serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// L2 (v3, ADR-029): a real `kiand --register-to --capability` declares
/// its capabilities on the wire; the real `kian workers --json` board
/// shows the cached declaration while the worker is online.
#[tokio::test]
async fn worker_declared_capabilities_reach_the_operator_board() {
    force_no_proxy();
    let dir = temp_dir("l2m-caps");
    let dir_w = dir.join("worker-caps");
    std::fs::create_dir_all(&dir_w).expect("worker dir");

    let serve_port = free_port();
    let listen_port = free_port();
    let (router_base, _router_mock) =
        spawn_scripted_provider(vec![text("bootstrap acknowledged")], 0).await;
    let (worker_base, _worker_mock) = spawn_scripted_provider(vec![text("idle")], 0).await;
    let config = write_membership_config(
        &dir,
        &router_base,
        &worker_base,
        serve_port,
        &[],
        Some(&WorkersSpec {
            listen_port,
            offline_after_ms: 8_000,
        }),
        false,
    );
    let (mut serve, _base) = boot_serve(&config, serve_port).await;

    let mut worker = RegisterDaemon::spawn_with_capabilities(
        &format!("127.0.0.1:{listen_port}"),
        "dyn-caps",
        &dir_w,
        WORKER_TOKEN,
        &["exec", "apple-calendar", "apple-health"],
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let board = kian_workers_json(&config).await;
        let row = board["workers"]
            .as_array()
            .expect("workers array")
            .iter()
            .find(|row| row["id"].as_str() == Some("dyn-caps"));
        if let Some(row) = row
            && row["state"].as_str() == Some("online")
            && row["capabilities"] == serde_json::json!(["exec", "apple-calendar", "apple-health"])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "capabilities never reached the board; board: {board}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    worker.kill9().await;
    kill_serve(&mut serve).await;
}
