//! (R12-07 / kian-133) Independent QA L2 acceptance legs for the
//! runtime worker membership — real `kian serve` and real `kiand`
//! child processes over loopback, QA-authored fixtures only.
//!
//! Complementary to (not a re-run of) the R12-06 suite: QA re-derives
//! the brief §7 L2 rows it can exercise independently of the
//! implementer's scripted-provider harness:
//!
//! - fail-closed `kian serve` startup refusals through the REAL
//!   binary: absent token env, blank token value, public `listen`
//!   without `allow_public` (C2/C4);
//! - runtime membership against the real serve process: a hand-rolled
//!   cross-process `register` (the test process IS a worker) joins
//!   with NO restart/config edit, heartbeats, `bye` → offline, and
//!   re-registers — every state observed through the real
//!   `kian workers --json` child;
//! - the wrong-token refusal over the real wire, plus the real
//!   `kiand --register-to` with a wrong token exiting non-zero with an
//!   actionable, token-free error;
//! - the captured serve stderr contains NO token material after all
//!   of the above (I9).
//!
//! Fake token values only; loopback only; `NO_PROXY` forced on every
//! child (AGENTS.md gotcha).

mod common;

use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use common::{force_no_proxy, temp_dir};
use kian_exec::proto::{
    EndpointFrame, RegisterToken, ServerFrame, decode_server_frame, encode_endpoint_frame,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::process::{Child, Command};

/// Fake credential material for loopback tests only — never a real one.
const QA_TOKEN: &str = "qa-l2-fake-token-zzz-not-real-42";
const QA_TOKEN_ENV: &str = "KIAN_QA_L2_TOKEN";
const KIAND_TOKEN_ENV: &str = "KIAN_WORKER_TOKEN";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A free loopback port (bind :0, read it, drop the listener).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn child_env(command: &mut Command) {
    command
        .env(common::KEY_ENV, "test-key")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_DEDUPE", "0")
        .env("no_proxy", "127.0.0.1,localhost");
    force_no_proxy();
}

/// The kiand binary (not a kian-cli bin target: locate it like the
/// repo's other cross-binary tests do).
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

/// The QA serve config: a `workers:` membership section and NO static
/// endpoints (the pure-dynamic deployment shape). Providers point at a
/// closed port — QA drives no model calls.
fn qa_config(dir: &Path, serve_port: u16, listen_port: u16, offline_after_ms: u64) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:{serve_port}"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: qa-dead
      model: mock-model
  tools: ["fs_read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: qa-dead
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: qa-dead
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  router:
    session_id: "99999999-9999-9999-9999-999999999999"
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
workers:
  listen: "127.0.0.1:{listen_port}"
  token_env: {QA_TOKEN_ENV}
  offline_after_ms: {offline_after_ms}
endpoints: []
providers:
  qa-dead:
    base_url: "http://127.0.0.1:1"
    api_key_env: {key_env}
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
        key_env = common::KEY_ENV,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write qa l2 config");
    path
}

/// Like [`qa_config`] but with a PUBLIC membership bind (no override).
fn qa_public_bind_config(dir: &Path, serve_port: u16, listen_port: u16) -> PathBuf {
    let path = qa_config(dir, serve_port, listen_port, 8_000);
    let yaml = std::fs::read_to_string(&path).unwrap();
    let yaml = yaml.replace(
        &format!("listen: \"127.0.0.1:{listen_port}\""),
        &format!("listen: \"0.0.0.0:{listen_port}\""),
    );
    std::fs::write(&path, yaml).unwrap();
    path
}

async fn run_serve_once(config: &Path, token_value: Option<&str>) -> (bool, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    child_env(&mut command);
    match token_value {
        Some(value) => {
            command.env(QA_TOKEN_ENV, value);
        }
        None => {
            command.env_remove(QA_TOKEN_ENV);
        }
    }
    let child = command.spawn().expect("spawn kian serve");
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("serve child settles within 30s (refusal is fail-closed at startup)")
        .expect("collect serve output");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// C2/C4 through the real binary: the listener configured with an
/// absent or blank token refuses startup; a public bind without the
/// explicit override refuses startup — every refusal names the policy,
/// none prints a token value.
#[tokio::test]
async fn qa_serve_startup_refusals_are_fail_closed() {
    let dir = temp_dir("qa-l2-refuse");

    // (1) token env ABSENT.
    let config = qa_config(&dir, free_port(), free_port(), 8_000);
    let (ok, _, stderr) = run_serve_once(&config, None).await;
    assert!(!ok, "absent token env must refuse startup");
    assert!(
        stderr.contains(QA_TOKEN_ENV),
        "the refusal names the variable: {stderr}"
    );
    assert!(!stderr.contains(QA_TOKEN), "no token material: {stderr}");

    // (2) token env set but BLANK.
    let (ok, _, stderr) = run_serve_once(&config, Some("   ")).await;
    assert!(!ok, "blank token value must refuse startup");
    assert!(
        stderr.contains(QA_TOKEN_ENV),
        "names the variable: {stderr}"
    );
    assert!(!stderr.contains(QA_TOKEN), "no token material: {stderr}");

    // (3) public bind without allow_public (token correctly set).
    let public = qa_public_bind_config(&dir, free_port(), free_port());
    let (ok, _, stderr) = run_serve_once(&public, Some(QA_TOKEN)).await;
    assert!(!ok, "public bind must refuse startup");
    assert!(
        stderr.contains("public") && stderr.contains("allow_public"),
        "the refusal names the policy and the override: {stderr}"
    );
    assert!(!stderr.contains(QA_TOKEN), "no token material: {stderr}");

    // (4) the honest control: the same config with the token set and a
    // private bind starts and stays up (killed by the test).
    let config = qa_config(&dir, free_port(), free_port(), 8_000);
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    child_env(&mut command);
    command.env(QA_TOKEN_ENV, QA_TOKEN);
    let mut child = command.spawn().expect("control serve starts");
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "the control config must stay up (the refusals above are the listeners', not the config's)"
    );
    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A raw cross-process registration client of the REAL serve listener.
struct QaRawWorker {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl QaRawWorker {
    async fn dial(addr: SocketAddr, id: &str, token: &str, version: u32) -> Option<Self> {
        let stream = AsyncTcpStream::connect(addr).await.expect("connect serve");
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        let mut this = Self {
            reader: BufReader::new(reader),
            writer,
        };
        let register = EndpointFrame::Register {
            endpoint_id: id.into(),
            description: Some("the QA raw worker".into()),
            token: RegisterToken::new(token.into()),
            version,
            max_concurrent: 2,
            capabilities: Vec::new(),
        };
        let mut line = encode_endpoint_frame(&register);
        line.push('\n');
        this.writer.write_all(line.as_bytes()).await.unwrap();
        match this.recv().await {
            Some(ServerFrame::Welcome { v: 3, .. }) => Some(this),
            other => {
                // Surface the refusal frame through `last_refusal`.
                if let Some(ServerFrame::Rejected { reason, .. }) = other {
                    eprintln!("qa raw worker refused: {reason:?}");
                }
                None
            }
        }
    }

    async fn recv(&mut self) -> Option<ServerFrame> {
        let mut line = String::new();
        match tokio::time::timeout(Duration::from_secs(5), self.reader.read_line(&mut line)).await {
            Err(_) => panic!("no frame within 5s"),
            Ok(Err(_)) | Ok(Ok(0)) => None,
            Ok(Ok(_)) => {
                Some(decode_server_frame(line.trim_end()).expect("decodable server frame"))
            }
        }
    }

    async fn heartbeat(&mut self, id: &str) {
        let frame = EndpointFrame::Heartbeat {
            endpoint_id: id.into(),
            ts_ms: now_ms(),
            inflight: 0,
        };
        let mut line = encode_endpoint_frame(&frame);
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn bye(mut self) {
        let mut line = encode_endpoint_frame(&EndpointFrame::Bye);
        line.push('\n');
        let _ = self.writer.write_all(line.as_bytes()).await;
        let _ = self.writer.shutdown().await;
        // Drain until the server closes (the graceful path).
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            let mut junk = [0u8; 64];
            loop {
                match self.reader.read(&mut junk).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        })
        .await;
    }
}

async fn kian_workers(config: &Path) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["workers", "--config", config.to_str().unwrap(), "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    child_env(&mut command);
    let output = command.output().await.expect("run kian workers");
    assert!(
        output.status.success(),
        "kian workers never refuses: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("board parses")
}

async fn wait_cli_state(config: &Path, id: &str, want: &str, deadline_ms: u64) {
    let start = now_ms();
    loop {
        let board = kian_workers(config).await;
        let state = board["workers"]
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row["id"].as_str() == Some(id))
                    .and_then(|row| row["state"].as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "<absent>".into());
        if state == want {
            return;
        }
        assert!(
            now_ms() - start < deadline_ms,
            "worker {id} never reached {want} (last: {state})"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Runtime membership against the real serve process: join without
/// restart/config edit, heartbeats, bye → offline, re-register →
/// online; the wrong token is typed-refused over the real wire; the
/// real register-mode kiand with a wrong token exits non-zero with an
/// actionable error; the serve stderr carries no token material.
#[tokio::test]
async fn qa_runtime_membership_over_real_processes() {
    let dir = temp_dir("qa-l2-membership");
    let listen_port = free_port();
    let config = qa_config(&dir, free_port(), listen_port, 8_000);

    // serve with stderr captured to a file (the I9 scan reads it).
    let stderr_path = dir.join("serve.stderr.log");
    let stderr_file = std::fs::File::create(&stderr_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .args(["serve", "--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(std::process::Stdio::from(stderr_file));
    child_env(&mut command);
    command.env(QA_TOKEN_ENV, QA_TOKEN);
    let mut serve: Child = command.spawn().expect("spawn serve");
    let addr: SocketAddr = format!("127.0.0.1:{listen_port}").parse().unwrap();

    // Wait for the listener: dial with a deliberate bad token first —
    // a typed rejection proves the listener is up AND exercises the
    // refusal over the real wire.
    let mut listener_up = false;
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            drop(stream);
            let probe = QaRawWorker::dial(
                addr,
                "qa-probe",
                "definitely-wrong-token",
                kian_exec::PROTOCOL_VERSION,
            )
            .await;
            match probe {
                None => {
                    listener_up = true;
                    break;
                }
                Some(_) => panic!("a wrong token must never register"),
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(listener_up, "the registration listener never came up");
    // The board already knows the refused probe id? It must NOT: a
    // refused registration creates no member and no row.
    let board = kian_workers(&config).await;
    assert!(
        board["workers"].as_array().unwrap().is_empty(),
        "a refused registration leaves no row: {board}"
    );

    // JOIN: no kian restart, no config edit — the test process is the
    // worker. Heartbeat keeps it fresh while we probe.
    let worker = QaRawWorker::dial(addr, "qa-raw-1", QA_TOKEN, kian_exec::PROTOCOL_VERSION)
        .await
        .expect("correct token registers");
    wait_cli_state(&config, "qa-raw-1", "online", 5_000).await;

    // Wrong token against the live listener (typed, terminal).
    let refused =
        QaRawWorker::dial(addr, "qa-raw-2", "wrong-again", kian_exec::PROTOCOL_VERSION).await;
    assert!(refused.is_none(), "wrong token refused");
    wait_cli_state(&config, "qa-raw-1", "online", 5_000).await;

    // BYE: graceful leave → offline on the board (no restart).
    worker.bye().await;
    wait_cli_state(&config, "qa-raw-1", "offline", 8_000).await;

    // RE-REGISTER the same id: online again (same id binding shape).
    let mut worker = QaRawWorker::dial(addr, "qa-raw-1", QA_TOKEN, kian_exec::PROTOCOL_VERSION)
        .await
        .expect("re-register accepted");
    worker.heartbeat("qa-raw-1").await;
    wait_cli_state(&config, "qa-raw-1", "online", 5_000).await;
    drop(worker);

    // The REAL register-mode kiand with a WRONG token: exits non-zero,
    // actionable stderr naming the env var, no token material.
    let out = Command::new(kiand_bin())
        .args([
            "--register-to",
            &format!("127.0.0.1:{listen_port}"),
            "--endpoint-id",
            "qa-kiand-bad",
            "--heartbeat-ms",
            "200",
        ])
        .env(KIAND_TOKEN_ENV, "kiand-wrong-token-xyz")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_DEDUPE", "0")
        .env(common::KEY_ENV, "test-key")
        .output()
        .await
        .expect("run kiand wrong-token");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(!out.status.success(), "wrong-token kiand exits non-zero");
    assert!(
        stderr.contains(KIAND_TOKEN_ENV) || stderr.contains("token"),
        "actionable error: {stderr}"
    );
    assert!(
        !stderr.contains(QA_TOKEN),
        "no server token material: {stderr}"
    );
    // And it never joined the board.
    let board = kian_workers(&config).await;
    assert!(
        board["workers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["id"].as_str() != Some("qa-kiand-bad")),
        "refused kiand leaves no row"
    );

    // I9: scan the serve stderr for the fake token value.
    let _ = serve.kill().await;
    let serve_stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    assert!(
        !serve_stderr.contains(QA_TOKEN),
        "the serve stderr contains token material:\n{serve_stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
