//! P5 end-to-end: the in-tree reference sidecar
//! (`tests/fixtures/plugins/approval-gate-sidecar.py`, PLUGIN-SPEC §2's
//! language-neutrality paragraph made executable) driven through the
//! **real** serve assembly — `Server::from_config_file` over a written
//! `kian.yaml` whose `sidecars:` section names a generated manifest
//! pointing at the real Python child. No fixture binary, no in-process
//! stub: the hardened spawn, env_clear + env_allow, the digest check,
//! the hello→ready identity restatement (§7.3) and the stdio ABI all
//! apply to the operator's actual shape (`argv[0]` = interpreter, the
//! script as `argv[1]`).
//!
//! The interpreter resolves `KIAN_TEST_SIDECAR_PYTHON` → `python3` on
//! `PATH` and **fails loudly** when neither exists — the reference
//! sidecar is a deliverable of the contract, never a silent skip.
//!
//! Audit lines are asserted by capturing `tracing` output in-process:
//! this file is its own test binary, so the global subscriber installed
//! here owns the process's single global slot (the fanout_audit
//! precedent — a thread-local `set_default` capture would be
//! order-parasitic and is never used).
//!
//! The `APPROVAL_GATE_*` environment is child-process-global (the
//! sidecar inherits it through `env_allow`, including at every respawn
//! after a deadline kill), so every test holds the gate env lock for
//! its whole duration — the sidecar_runner.rs precedent.

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use futures::StreamExt;
use kian_cli::server::Server;

// ---------------------------------------------------------------------------
// Audit capture (process-global, this binary only)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MakeShared(SharedWriter);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}

/// This binary's one capture buffer: the subscriber is installed once
/// (first call) and **every** test asserts against the same buffer —
/// wiring a fresh buffer per call would hand later tests a dead one
/// (only the first caller's writer is ever attached to the global
/// subscriber).
fn install_audit_capture() -> Arc<Mutex<Vec<u8>>> {
    static BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    let buf = BUFFER
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone();
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(SharedWriter(Arc::clone(&buf))))
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("hook_sidecar_e2e owns its process's global subscriber");
    });
    buf
}

/// The captured audit text so far.
fn audit_text(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

/// Captured audit lines that contain every needle.
fn audit_lines<'a>(text: &'a str, needles: &[&str]) -> Vec<&'a str> {
    text.lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .collect()
}

// ---------------------------------------------------------------------------
// The gate environment (child-process-global → serialized tests)
// ---------------------------------------------------------------------------

async fn gate_env_lock() -> tokio::sync::OwnedMutexGuard<()> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
        .lock_owned()
        .await
}

/// Sets the sidecar's `env_allow` surface (the caller holds the env
/// lock for the test's duration — the values must survive respawns).
fn set_gate_env(frames_log: &Path, mode: Option<&str>) {
    unsafe {
        // SAFETY: serialized by the gate env lock, held for the child's
        // whole (incl. respawned) lifetime; nothing else reads these.
        std::env::set_var(
            "APPROVAL_GATE_SESSIONS",
            // A session uuid that is never the one the test creates:
            // every live session is therefore unapproved for fs_write.
            "11111111-1111-1111-1111-111111111111",
        );
        std::env::set_var("APPROVAL_GATE_FRAMES", frames_log.display().to_string());
        match mode {
            Some(mode) => std::env::set_var("APPROVAL_GATE_MODE", mode),
            None => std::env::remove_var("APPROVAL_GATE_MODE"),
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures: interpreter, manifest, config, server, session
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()
        .expect("repo root")
}

fn sidecar_script() -> PathBuf {
    let script =
        repo_root().join("crates/kian-cli/tests/fixtures/plugins/approval-gate-sidecar.py");
    assert!(
        script.is_file(),
        "the reference sidecar must exist at {}",
        script.display()
    );
    script
}

/// The interpreter resolution ladder (plan K15): explicit env override,
/// then `python3` on `PATH`, then a **loud failure** — never a skip.
fn python_interpreter() -> PathBuf {
    if let Ok(from_env) = std::env::var("KIAN_TEST_SIDECAR_PYTHON")
        && !from_env.trim().is_empty()
    {
        let p = PathBuf::from(from_env);
        assert!(
            p.is_file(),
            "KIAN_TEST_SIDECAR_PYTHON={p:?} is not a file — fix the override"
        );
        return p.canonicalize().unwrap_or(p);
    }
    let path = std::env::var("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path) {
        let cand = dir.join("python3");
        if cand.is_file() {
            return cand.canonicalize().expect("canonicalize python3");
        }
    }
    panic!(
        "the P5 reference sidecar is Python 3 (stdlib only, PLUGIN-SPEC §2) \
         but no interpreter was found: set KIAN_TEST_SIDECAR_PYTHON or put \
         a python3 on PATH. Refusing to silently skip the contract's e2e."
    );
}

/// Writes the `approval-gate` manifest with the real interpreter as
/// `argv[0]` (digest = the interpreter's own sha256, §6.2 rule 6) and
/// the real script as `argv[1]` — the operator's actual shape.
fn write_gate_manifest(dir: &Path, python: &Path, deadline_ms: u64) -> PathBuf {
    let digest = kian_plugin::hooks::sha256_hex(&std::fs::read(python).expect("read interpreter"));
    let toml = format!(
        concat!(
            "[plugin]\n",
            "name = \"approval-gate\"\n",
            "version = \"0.1.0\"\n",
            "abi = 1\n",
            "required = true\n",
            "\n",
            "[process]\n",
            "argv = [\"{python}\", \"{script}\"]\n",
            "sha256 = \"{digest}\"\n",
            "env_allow = [\"APPROVAL_GATE_SESSIONS\", \"APPROVAL_GATE_FRAMES\", \"APPROVAL_GATE_MODE\"]\n",
            "startup_timeout_ms = 30000\n",
            "\n",
            "[grants]\n",
            "hooks = [\"tool/pre\", \"tool/post\"]\n",
            "capabilities = [\"observe:tool\", \"veto:tool\"]\n",
            "\n",
            "[limits]\n",
            "deadline_ms = {deadline_ms}\n"
        ),
        python = python.display(),
        script = sidecar_script().display(),
        digest = digest,
        deadline_ms = deadline_ms,
    );
    let path = dir.join("approval-gate.toml");
    std::fs::write(&path, toml).expect("write gate manifest");
    path
}

/// Writes a `kian.yaml` over the mock provider with the fs tools and,
/// when given, the `sidecars:` section naming the manifest.
fn write_config(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    sidecars: Option<&Path>,
) -> PathBuf {
    let mut yaml = format!(
        r#"listen: "127.0.0.1:7331"
storage:
  sqlite: "{sqlite}"
agent:
  id: e2e-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: ["fs_read", "fs_write"]
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: KIAN_E2E_API_KEY
drain:
  max_iterations: 8
  lease_ttl_ms: 30000
  heartbeat_interval_ms: 10000
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
"#,
        sqlite = sqlite.display(),
        provider_base = provider_base,
        root = dir.display(),
    );
    if let Some(manifest) = sidecars {
        yaml.push_str(&format!(
            "sidecars:\n  plugins:\n    approval-gate:\n      manifest: \"{}\"\n  scopes:\n    server: [\"approval-gate\"]\n",
            manifest.display()
        ));
    }
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

/// The spawned server on its own dedicated runtime, owned by a plain
/// thread. Two facts force this shape:
///
/// 1. the hook registry behind `HookBus::new()` is **process-global**
///    (correct for the one-server process the spec assumes), so a live
///    `Server` from an earlier test would receive this test's
///    dispatches and answer with a stopping runner ("crash" denials);
/// 2. the assembly spawns tasks that hold `Arc<Server>` — on the
///    *test's* runtime, which is dropped only after the test fn
///    returns, i.e. after the gate env lock is already released.
///
/// [`TestServer::shutdown`] therefore runs while the test still holds
/// the lock: signal the owner thread, which drops its runtime (every
/// server-side task's future drops, releasing its `Arc`s), join it,
/// then drop the last `Arc` so `SidecarMount::drop` stops the sidecar
/// and the disposers unregister — deterministically before the next
/// test mounts into the same global registry.
struct TestServer {
    server: Option<Arc<Server>>,
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TestServer {
    fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        drop(self.server.take());
    }
}

/// Spawns the assembled server on an ephemeral port, on a runtime the
/// test controls the lifetime of. `from_config_file` performs the
/// sidecar mount — a refused required instance is a boot failure, so
/// assembly success already says the handshake restated the identity
/// (§7.3 rules 12/13).
fn spawn_server(config_path: &Path) -> (String, TestServer) {
    let config_path = config_path.to_path_buf();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<(String, Arc<Server>)>();
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("kian-e2e-server".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()
                .expect("e2e server runtime");
            let server = Arc::new(
                rt.block_on(Server::from_config_file(&config_path))
                    .expect("assemble server")
                    .0,
            );
            let listener = rt
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let router = server.router();
            rt.spawn(async move {
                axum::serve(listener, router).await.expect("serve");
            });
            ready_tx
                .send((format!("http://{addr}"), Arc::clone(&server)))
                .expect("e2e server channel alive");
            // Hold the runtime (and its tasks) alive until shutdown;
            // dropping it here cancels every server-side task and
            // releases their Arcs while the test thread waits on join.
            let _ = shutdown_rx.recv();
        })
        .expect("spawn e2e server thread");
    let (base, server) = ready_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("e2e server ready");
    (
        base,
        TestServer {
            server: Some(server),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        },
    )
}

/// Creates a session, opens its SSE stream, submits one input and reads
/// until the `done` frame — returning the session id and every `kian`
/// event frame seen (the full envelope: `kind`, `bytes`, …).
async fn run_session(
    base: &str,
    client: &reqwest::Client,
    content: &str,
) -> (String, Vec<serde_json::Value>) {
    let out = rpc(client, base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();

    let sse = client
        .get(format!("{base}/sessions/{sid}/events"))
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("sse open");
    assert_eq!(sse.status(), 200);

    let out = rpc(
        client,
        base,
        "submit",
        serde_json::json!({"session": sid, "content": content}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    let mut events: Vec<serde_json::Value> = Vec::new();
    let mut saw_done = false;
    let mut stream = sse.bytes_stream();
    let mut buf = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("sse timeout")
            .expect("sse ended early")
            .expect("sse chunk");
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buf.find("\n\n") {
            let frame: String = buf.drain(..pos + 2).collect();
            let mut event_name = "";
            let mut data = String::new();
            for line in frame.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    event_name = v.trim();
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data.push_str(v.trim());
                }
            }
            match event_name {
                "kian" => {
                    let ev: serde_json::Value = serde_json::from_str(&data)
                        .unwrap_or_else(|e| panic!("bad kian frame {data}: {e}"));
                    events.push(ev);
                }
                "done" => saw_done = true,
                "error" => panic!("sse error frame: {data}"),
                _ => {}
            }
        }
        if saw_done {
            break;
        }
    }
    (sid, events)
}

fn of_kind<'a>(events: &'a [serde_json::Value], kind: &str) -> Vec<&'a serde_json::Value> {
    events.iter().filter(|e| e["kind"] == kind).collect()
}

/// The frames the sidecar received, as logged by the script itself
/// (one JSON object per line: the `hello`, then every `hook` frame).
fn frames_log_lines(dir: &Path) -> Vec<serde_json::Value> {
    let text =
        std::fs::read_to_string(dir.join("frames.log")).expect("the sidecar wrote its frames log");
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("reply:"))
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad frames line {l}: {e}")))
        .collect()
}

// ---------------------------------------------------------------------------
// The six acceptance tests (PLUGIN-SPEC §14 P5, plan C6)
// ---------------------------------------------------------------------------

/// The mount itself: over a real `sidecars:` config the plugin mounts
/// (a required instance whose `ready` failed to restate the identity
/// refuses boot, §7.3 rules 12/13), the assembly audit line is
/// emitted, the first drain records the `hook-set` line, and the
/// `hello` the script logged restates this manifest's identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_ready_over_real_config() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let audit = install_audit_capture();
    let dir = temp_dir("gate-hello");
    set_gate_env(&dir.join("frames.log"), None);

    let python = python_interpreter();
    let manifest = write_gate_manifest(&dir, &python, 500);
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("All done.".into())], 0).await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        Some(&manifest),
    );
    let (base, server) = spawn_server(&config);

    let (_sid, events) = run_session(&base, &reqwest::Client::new(), "go").await;
    assert!(
        of_kind(&events, "session/ended").len() == 1,
        "the session settled: {:?}",
        events
            .iter()
            .map(|e| e["kind"].as_str())
            .collect::<Vec<_>>()
    );

    // The assembly's mount line, and no refusal anywhere (a refusal of
    // this required instance would have failed boot before this point).
    let text = audit_text(&audit);
    assert!(
        audit_lines(&text, &["hook-refuse"]).is_empty(),
        "no instance was refused: {}",
        audit_lines(&text, &["hook-refuse"]).join("; ")
    );
    assert!(
        !audit_lines(&text, &["sidecars mounted"]).is_empty(),
        "the mount line is on the kian::hook sink"
    );
    // §9.2 step 1: the first drain records the effective set.
    assert!(
        !audit_lines(&text, &["hook-set", "approval-gate"]).is_empty(),
        "hook-set names the mounted instance"
    );

    // §7.3: the hello the child received carries this manifest's
    // canonical identity — and the child that answered `ready` was
    // accepted, which is the identity restating itself being *checked*
    // (any mismatch is a typed, fatal refusal).
    let frames = frames_log_lines(&dir);
    let hello = frames
        .iter()
        .find(|f| f.get("manifest").is_some() && f.get("abi").is_some())
        .expect("the script logged the hello");
    assert_eq!(hello["v"], 1);
    assert_eq!(hello["abi"], 1);
    assert_eq!(hello["plugin"]["name"], "approval-gate");
    assert_eq!(hello["plugin"]["version"], "0.1.0");
    let canonical = manifest.canonicalize().expect("canonical manifest path");
    assert_eq!(
        hello["manifest"].as_str().unwrap(),
        canonical.display().to_string()
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// §4.4 + §16.1: a denied `fs_write` yields the honest-error
/// `tool/result` with `policy:"hook"`, the attempted `tool/call`
/// carries the hook provenance naming `<plugin>:<hook>`, the file is
/// never written, and the `hook-veto` audit line exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deny_yields_honest_error_with_policy_hook() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let audit = install_audit_capture();
    let dir = temp_dir("gate-deny");
    set_gate_env(&dir.join("frames.log"), None);

    let python = python_interpreter();
    let manifest = write_gate_manifest(&dir, &python, 500);
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-w".into(),
                name: "fs_write".into(),
                arguments: serde_json::json!({"path": "out.txt", "content": "must not land"}),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        Some(&manifest),
    );
    let (base, server) = spawn_server(&config);

    let (sid, events) = run_session(&base, &reqwest::Client::new(), "write the file").await;
    let calls = of_kind(&events, "tool/call");
    assert_eq!(calls.len(), 1, "the denied call is still logged");
    let reason = calls[0]["bytes"]["permission"]["reason"]
        .as_str()
        .expect("the denial reason rides the provenance")
        .to_owned();
    assert_eq!(
        calls[0]["bytes"]["permission"],
        serde_json::json!({
            "evaluated": "deny",
            "policy": "hook",
            "matched": "approval-gate:tool/pre",
            "reason": reason,
        }),
        "§4.4 step 3: hook provenance names the plugin:hook"
    );
    assert!(
        reason.contains("fs_write"),
        "the reason names the gated tool: {reason}"
    );

    let results = of_kind(&events, "tool/result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["bytes"]["content"]["is_error"], true);
    assert_eq!(
        results[0]["bytes"]["content"]["output"],
        serde_json::json!({
            "error": format!("denied by plugin approval-gate: {reason}"),
            "policy": "hook",
        }),
        "§4.4 step 4: the fixed honest-error template"
    );

    // §4.4 step 4: a denied call never reaches invoke.
    assert!(
        !dir.join("out.txt").exists(),
        "the side effect never happened"
    );
    // §10.6: the veto audit line names hook and plugin.
    let text = audit_text(&audit);
    assert!(
        !audit_lines(&text, &["hook-veto", "tool/pre", "approval-gate", &sid]).is_empty(),
        "hook-veto names the hook, plugin and target session"
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// §3.3's `bail:null`: an abstaining verdict leaves the declarative
/// gate's decision standing — the fs_read executes, and the
/// `tool/call` payload is the legacy byte-identical shape (no
/// `permission` key at all, ADR-015 / constraint 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abstain_path_allows_execution() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let audit = install_audit_capture();
    let dir = temp_dir("gate-abstain");
    set_gate_env(&dir.join("frames.log"), None);
    std::fs::write(dir.join("in.txt"), "hello-gate").expect("seed the read target");

    let python = python_interpreter();
    let manifest = write_gate_manifest(&dir, &python, 500);
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-r".into(),
                name: "fs_read".into(),
                arguments: serde_json::json!({"path": "in.txt"}),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        Some(&manifest),
    );
    let (base, server) = spawn_server(&config);

    let (_sid, events) = run_session(&base, &reqwest::Client::new(), "read the file").await;
    let diag = || {
        audit_text(&audit)
            .lines()
            .filter(|l| l.contains("kian::hook") || l.contains("hook-"))
            .take(40)
            .collect::<Vec<_>>()
            .join("\n")
    };
    let calls = of_kind(&events, "tool/call");
    assert_eq!(calls.len(), 1, "audit trail:\n{}", diag());
    assert_eq!(
        serde_json::to_string(&calls[0]["bytes"]).unwrap(),
        r#"{"content":{"arguments":{"path":"in.txt"},"id":"call-r","name":"fs_read"},"role":"assistant"}"#,
        "an abstained call is the legacy payload — no permission key; audit trail:\n{}",
        diag()
    );
    let results = of_kind(&events, "tool/result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["bytes"]["content"]["is_error"],
        false,
        "the abstained call executed; audit trail:\n{}",
        diag()
    );
    assert_eq!(
        results[0]["bytes"]["content"]["output"],
        serde_json::json!({"content": "hello-gate"}),
        "the call executed"
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// §11.1 row 1: a wedged `veto:tool` holder (the script's `sleep`
/// failure-injection mode) misses the deadline and the hook fails
/// CLOSED — the honest error names the cause, the `hook-default` and
/// `default-deny` audit lines exist, and the write still never
/// happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_miss_fails_closed_deny() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let audit = install_audit_capture();
    let dir = temp_dir("gate-deadline");
    set_gate_env(&dir.join("frames.log"), Some("sleep"));

    let python = python_interpreter();
    let manifest = write_gate_manifest(&dir, &python, 60);
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-w".into(),
                name: "fs_write".into(),
                arguments: serde_json::json!({"path": "out.txt", "content": "nope"}),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        Some(&manifest),
    );
    let (base, server) = spawn_server(&config);

    let (_sid, events) = run_session(&base, &reqwest::Client::new(), "write the file").await;
    let results = of_kind(&events, "tool/result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["bytes"]["content"]["output"],
        serde_json::json!({
            "error": "denied by plugin approval-gate: hook unavailable (deadline)",
            "policy": "hook",
        }),
        "§11.1: the class default denies with the honest cause"
    );
    assert_eq!(results[0]["bytes"]["content"]["is_error"], true);
    let calls = of_kind(&events, "tool/call");
    assert_eq!(calls[0]["bytes"]["permission"]["policy"], "hook");
    assert!(
        !dir.join("out.txt").exists(),
        "fail-closed means no side effect"
    );

    let text = audit_text(&audit);
    assert!(
        !audit_lines(
            &text,
            &["hook-default", "tool/pre", "approval-gate", "deadline"]
        )
        .is_empty(),
        "hook-default records the class-default cause"
    );
    assert!(
        !audit_lines(&text, &["hook-dispatch", "default-deny"]).is_empty(),
        "the dispatch outcome says default-deny"
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// §7.2: the `hook` frame the real sidecar received (logged by the
/// script) is exactly the closed shape — key set, the epoch rule
/// (drain-phase hooks carry it), the mode, and the §3.2.5 payload
/// catalog with its digest pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_frame_golden() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let _audit = install_audit_capture();
    let dir = temp_dir("gate-golden");
    set_gate_env(&dir.join("frames.log"), None);

    let python = python_interpreter();
    let manifest = write_gate_manifest(&dir, &python, 500);
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-w".into(),
                name: "fs_write".into(),
                arguments: serde_json::json!({"path": "out.txt", "content": "x"}),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        Some(&manifest),
    );
    let (base, server) = spawn_server(&config);

    let (sid, events) = run_session(&base, &reqwest::Client::new(), "write the file").await;
    assert_eq!(of_kind(&events, "tool/result").len(), 1, "scenario ran");

    let frames = frames_log_lines(&dir);
    let hook = frames
        .iter()
        .find(|f| f.get("hook").and_then(|h| h.as_str()) == Some("tool/pre"))
        .expect("the script logged the tool/pre frame");
    let keys: Vec<&str> = hook
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    // `serde_json::Value` objects iterate sorted — compare the closed
    // §7.2 key SET (presence/absence is the contract; the wire order is
    // pinned by the runner's own byte goldens).
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![
            "deadline_ms",
            "epoch",
            "hook",
            "id",
            "mode",
            "payload",
            "seq",
            "session",
            "worker"
        ],
        "§7.2's closed hook-frame key set (actual: {keys:?})"
    );
    assert!(
        hook["epoch"].is_u64(),
        "tool/pre is drain-phase: the frame carries a real epoch (§7.2)"
    );
    assert_eq!(hook["mode"], "bail", "tool/pre's reply mode");
    assert_eq!(
        hook["deadline_ms"], 500,
        "the manifest's per-dispatch deadline"
    );
    assert_eq!(
        hook["session"].as_str().unwrap(),
        sid,
        "the frame's target session"
    );
    assert!(
        !hook["id"].as_str().unwrap().is_empty(),
        "a fresh dispatch id"
    );

    let payload = &hook["payload"];
    let pkeys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let mut psorted = pkeys.clone();
    psorted.sort_unstable();
    assert_eq!(
        psorted,
        vec![
            "arguments",
            "arguments_bytes",
            "arguments_sha256",
            "arguments_truncated",
            "call_id",
            "iteration",
            "name",
            "permission",
            "surface"
        ],
        "§3.2.5's closed payload catalog (actual: {pkeys:?})"
    );
    assert_eq!(payload["call_id"], "call-w");
    assert_eq!(payload["name"], "fs_write");
    assert_eq!(
        payload["arguments"],
        serde_json::json!({"path": "out.txt", "content": "x"})
    );
    assert_eq!(payload["arguments_truncated"], false);
    assert_eq!(payload["surface"], "react", "the only v1 surface");
    // The digest pair describes the complete unredacted serialization.
    let bytes = serde_json::to_vec(&payload["arguments"]).expect("serialize arguments");
    assert_eq!(
        payload["arguments_sha256"].as_str().unwrap(),
        kian_plugin::hooks::sha256_hex(&bytes),
        "arguments_sha256 pins the exact serialized arguments"
    );
    assert_eq!(payload["arguments_bytes"], bytes.len() as u64);
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13.3: deleting the `sidecars:` section returns byte-identical
/// behavior — the abstain run and the no-sidecar run produce the same
/// `tool/call` and `tool/result` payload bytes for the same scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_absent_is_byte_identical() {
    force_no_proxy();
    let _env = gate_env_lock().await;
    let _audit = install_audit_capture();

    async fn scenario(dir: &Path, sidecars: bool) -> Vec<serde_json::Value> {
        std::fs::write(dir.join("in.txt"), "hello-gate").expect("seed the read target");
        let python = python_interpreter();
        let manifest = write_gate_manifest(dir, &python, 500);
        let (provider_base, _mock) = spawn_scripted_provider(
            vec![
                ScriptedResponse::ToolCall {
                    id: "call-r".into(),
                    name: "fs_read".into(),
                    arguments: serde_json::json!({"path": "in.txt"}),
                },
                ScriptedResponse::Text("done".into()),
            ],
            0,
        )
        .await;
        let config = write_config(
            dir,
            &dir.join("kian.sqlite"),
            &provider_base,
            sidecars.then_some(&manifest),
        );
        let (base, server) = spawn_server(&config);
        let events = run_session(&base, &reqwest::Client::new(), "read the file")
            .await
            .1;
        // Stop this server before the next scenario mounts its own
        // instance into the same process-global hook registry.
        server.shutdown();
        events
    }

    let with_dir = temp_dir("gate-absent-with");
    set_gate_env(&with_dir.join("frames.log"), None);
    let with_sidecars = scenario(&with_dir, true).await;
    let without_dir = temp_dir("gate-absent-without");
    let without = scenario(&without_dir, false).await;

    let shape = |events: &[serde_json::Value]| -> Vec<String> {
        events
            .iter()
            .filter(|e| e["kind"] == "tool/call" || e["kind"] == "tool/result")
            .map(|e| serde_json::to_string(&e["bytes"]).unwrap())
            .collect()
    };
    assert_eq!(
        shape(&with_sidecars),
        shape(&without),
        "§13.3: an abstaining (or absent) sidecar changes no model-visible bytes"
    );
    let _ = std::fs::remove_dir_all(&with_dir);
    let _ = std::fs::remove_dir_all(&without_dir);
}
