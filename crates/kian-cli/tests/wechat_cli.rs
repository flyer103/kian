//! wechat CLI/config/assembly integration tests (R10-04):
//!
//! - serve assembly mounts the `type: wechat` channel and fails closed
//!   without a usable credential file (naming `kian wechat login`);
//! - `kian wechat login` against a scripted in-process mock iLink
//!   server: QR flow → atomic 0600 credential file, ids printed, token
//!   never printed, `--json` shape, non-zero exit on failure/expiry;
//! - end-to-end (real `Server` + sqlite + mock iLink): one inbound →
//!   admitted exactly once; replay after a real process restart →
//!   still once; a Router reply produces the exact §2.4 sendmessage
//!   payload; a channel failure leaves session state untouched.
//!
//! Zero credentials, zero network egress, fake tokens only
//! (`fake-bot-token-…`). `NO_PROXY=127.0.0.1,localhost` for the
//! localhost legs (AGENTS.md gotcha).

mod common;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Query, State};
use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The scripted iLink mock (pattern: kian-superagent/tests/wechat_channel.rs).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Captured {
    path: String,
    authorization: String,
    authorization_type: String,
    body: Value,
}

struct IlinkMock {
    /// Scripted `getupdates` response bodies, in order; the default
    /// after the script runs dry is an empty long-poll result.
    updates_script: Mutex<VecDeque<Value>>,
    /// Scripted `sendmessage` responses, in order; default: accepted.
    send_script: Mutex<VecDeque<Value>>,
    /// Scripted `get_qrcode_status` bodies, in order; default `wait`.
    qr_status_script: Mutex<VecDeque<Value>>,
    requests: Mutex<Vec<Captured>>,
    qr_fetches: Mutex<u64>,
}

impl IlinkMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            updates_script: Mutex::new(VecDeque::new()),
            send_script: Mutex::new(VecDeque::new()),
            qr_status_script: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
            qr_fetches: Mutex::new(0),
        })
    }

    fn push_updates(&self, body: Value) {
        self.updates_script.lock().unwrap().push_back(body);
    }

    #[allow(dead_code)] // kept for scripted send-failure legs
    fn push_send(&self, body: Value) {
        self.send_script.lock().unwrap().push_back(body);
    }

    fn push_qr_status(&self, body: Value) {
        self.qr_status_script.lock().unwrap().push_back(body);
    }

    fn requests_for(&self, path: &str) -> Vec<Captured> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path)
            .cloned()
            .collect()
    }

    fn request_count(&self, path: &str) -> usize {
        self.requests_for(path).len()
    }

    async fn serve(self: Arc<Self>) -> String {
        let app = axum::Router::new()
            .route(
                "/ilink/bot/getupdates",
                axum::routing::post(getupdates_endpoint),
            )
            .route(
                "/ilink/bot/sendmessage",
                axum::routing::post(sendmessage_endpoint),
            )
            .route(
                "/ilink/bot/get_bot_qrcode",
                axum::routing::get(bot_qrcode_endpoint),
            )
            .route(
                "/ilink/bot/get_qrcode_status",
                axum::routing::get(qr_status_endpoint),
            )
            .with_state(self);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("mock serve") });
        format!("http://{addr}")
    }
}

fn capture(state: &IlinkMock, path: &str, headers: &axum::http::HeaderMap, body: Value) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    state.requests.lock().unwrap().push(Captured {
        path: path.to_string(),
        authorization: header("authorization"),
        authorization_type: header("authorizationtype"),
        body,
    });
}

async fn getupdates_endpoint(
    State(state): State<Arc<IlinkMock>>,
    headers: axum::http::HeaderMap,
    body: Json<Value>,
) -> Json<Value> {
    capture(&state, "/ilink/bot/getupdates", &headers, body.0);
    let scripted = state.updates_script.lock().unwrap().pop_front();
    Json(scripted.unwrap_or(json!({
        "ret": 0, "errcode": 0, "msgs": [], "get_updates_buf": "mock-cursor"
    })))
}

async fn sendmessage_endpoint(
    State(state): State<Arc<IlinkMock>>,
    headers: axum::http::HeaderMap,
    body: Json<Value>,
) -> Json<Value> {
    capture(&state, "/ilink/bot/sendmessage", &headers, body.0);
    let scripted = state.send_script.lock().unwrap().pop_front();
    Json(scripted.unwrap_or(json!({
        "message_id": u64::MAX, "ret": 0, "errmsg": "ok"
    })))
}

async fn bot_qrcode_endpoint(
    State(state): State<Arc<IlinkMock>>,
    headers: axum::http::HeaderMap,
) -> Json<Value> {
    capture(&state, "/ilink/bot/get_bot_qrcode", &headers, json!(null));
    let n = {
        let mut fetches = state.qr_fetches.lock().unwrap();
        *fetches += 1;
        *fetches
    };
    Json(json!({
        "ret": 0, "errcode": 0,
        "qrcode": format!("qr-{n}"),
        "qrcode_img_content": format!("https://qr.example/qr-{n}"),
    }))
}

async fn qr_status_endpoint(
    State(state): State<Arc<IlinkMock>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Json<Value> {
    capture(
        &state,
        "/ilink/bot/get_qrcode_status",
        &headers,
        json!(query.clone()),
    );
    let scripted = state.qr_status_script.lock().unwrap().pop_front();
    Json(scripted.unwrap_or(json!({"status": "wait"})))
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

const USER: &str = "oUSER@im.wechat";
const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn credential_path(dir: &Path) -> PathBuf {
    dir.join("wechat-credentials.json")
}

fn write_credentials(path: &Path, token: &str) {
    kian_superagent::wechat::credentials::save(
        path,
        &kian_superagent::wechat::credentials::WechatCredentials::new(
            "https://ilinkai.weixin.qq.com",
            token,
            "fake-bot-1",
            "fake-user-1",
            1730000000000,
        ),
    )
    .expect("write credential fixture");
}

/// One raw `WeixinMessage` fixture (the CHANNEL-WECHAT §2.3 shape).
fn wx_message(message_id: Value, from: &str, text: &str, context_token: &str) -> Value {
    json!({
        "seq": 7,
        "message_id": message_id,
        "from_user_id": from,
        "to_user_id": "ilink-bot",
        "client_id": "client-1",
        "create_time_ms": 1730000000123u64,
        "session_id": "sess-1",
        "group_id": "",
        "message_type": 1,
        "message_state": 1,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
        "context_token": context_token,
        "run_id": "run-1",
    })
}

/// A full super-agent `kian.yaml` with a `type: wechat` channel pointed
/// at the mock iLink base. The credential file's existence is the
/// caller's business (the fail-closed leg deliberately omits it).
fn write_wechat_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    ilink_base: &str,
    credentials: &Path,
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
  router:
    session_id: "{ROUTER_SESSION}"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: wechat
      match: always
      route: router
  channels:
    wechat:
      type: wechat
      wechat:
        api_base: "{ilink_base}"
        credentials_file: "{credentials}"
        topics:
          "{USER}": wechat
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
        ilink_base = ilink_base,
        credentials = credentials.display(),
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write wechat config");
    path
}

fn router_session() -> kian_core::model::SessionId {
    kian_core::model::SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

/// The wechat events admitted on the router stream (channel wrapper
/// with the event id verbatim).
fn wechat_admits(events: &[kian_core::events::Event]) -> Vec<&kian_core::events::Event> {
    events
        .iter()
        .filter(|e| {
            e.kind == kian_core::events::EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("channel_event"))
                && e.bytes.pointer("/content/kian/channel") == Some(&json!("wechat"))
        })
        .collect()
}

/// Polls `f` at 25 ms until it yields `Some` or `secs` elapse.
async fn within<T>(secs: u64, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        if let Some(value) = f() {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// The async twin of [`within`] (awaiting `f` each round).
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

// ---------------------------------------------------------------------------
// Assembly: fail-closed without credentials; mounted with them.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_refuses_to_start_without_a_credential_file_naming_the_command() {
    force_no_proxy();
    let dir = temp_dir("wechat-fail-closed");
    let (router_base, _router) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    // No credential file at the configured path.
    let credentials = credential_path(&dir);
    let config = write_wechat_config(
        &dir,
        &router_base,
        &worker_base,
        "https://ilinkai.weixin.qq.com",
        &credentials,
    );
    let error = kian_cli::server::Server::from_config_file(&config)
        .await
        .map(|_| ())
        .expect_err("serve must refuse a wechat channel without credentials");
    let message = error.to_string();
    assert!(
        message.contains("kian wechat login"),
        "the startup error must name the command: {message}"
    );
    assert!(
        message.contains("wechat-credentials.json"),
        "the startup error must name the credential path: {message}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_mounts_the_wechat_channel_and_polls_ilink() {
    force_no_proxy();
    let dir = temp_dir("wechat-mounted");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
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

    let credentials = credential_path(&dir);
    write_credentials(&credentials, "fake-bot-token-mounted");
    let config = write_wechat_config(&dir, &router_base, &worker_base, &base, &credentials);
    let server = kian_cli::server::Server::from_config_file(&config)
        .await
        .expect("assemble with a valid credential file")
        .0;

    // The adapter's long-poll driver starts with the subscription
    // consumers and reaches the mock with the credential token's
    // Authorization header (never logged — asserted here on the wire).
    let observed = within(15, || {
        let sent = mock.request_count("/ilink/bot/getupdates");
        (sent > 0).then_some(sent)
    })
    .await;
    assert!(
        observed.is_some(),
        "the mounted channel must poll getupdates"
    );
    let polls = mock.requests_for("/ilink/bot/getupdates");
    assert!(
        polls
            .iter()
            .all(|r| r.authorization.contains("fake-bot-token-mounted")),
        "every poll carries the credential token's Authorization header"
    );
    assert!(
        polls
            .iter()
            .all(|r| r.authorization_type == "ilink_bot_token"),
        "every poll carries the AuthorizationType header"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// `kian wechat login` against the mock iLink server (real binary).
// ---------------------------------------------------------------------------

use std::process::Command;

fn run_kian(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kian"));
    command.args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("run kian binary")
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout utf8")
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr utf8")
}

/// A login-shaped config over the mock iLink (the credential file is
/// deliberately absent — login is what writes it).
fn write_login_config(dir: &Path, ilink_base: &str, credentials: &Path) -> PathBuf {
    write_wechat_config(
        dir,
        "http://127.0.0.1:1",
        "http://127.0.0.1:1",
        ilink_base,
        credentials,
    )
}

fn confirmed_body() -> Value {
    json!({
        "status": "confirmed",
        "bot_token": "fake-bot-token-login",
        "ilink_bot_id": "fake-bot-9",
        "ilink_user_id": "fake-user-9",
        "baseurl": "https://ilinkai.weixin.qq.com",
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_walks_the_qr_states_and_writes_a_0600_file() {
    force_no_proxy();
    let dir = temp_dir("wechat-login");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    // The redirect host points back at the same mock; the states walk
    // wait → redirect → need_verifycode → confirmed.
    let redirect_host = base.trim_start_matches("http://").to_string();
    mock.push_qr_status(json!({"status": "wait"}));
    mock.push_qr_status(json!({
        "status": "scaned_but_redirect", "redirect_host": redirect_host
    }));
    mock.push_qr_status(json!({"status": "need_verifycode"}));
    mock.push_qr_status(confirmed_body());

    let credentials = dir.join("login-credentials.json");
    let config = write_login_config(&dir, &base, &credentials);
    let out = run_kian(
        &["wechat", "login", "--config", config.to_str().unwrap()],
        &[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
        ],
    );
    assert!(out.status.success(), "exit 0; stderr: {}", stderr(&out));

    let text = stdout(&out);
    assert!(
        text.contains("https://qr.example/qr-1"),
        "QR URL printed: {text}"
    );
    assert!(
        text.contains("verification code"),
        "the operator is prompted for the code: {text}"
    );
    assert!(text.contains("fake-bot-9"), "account id printed: {text}");
    assert!(text.contains("fake-user-9"), "user id printed: {text}");
    assert!(
        !text.contains("fake-bot-token-login"),
        "the token is NEVER printed: {text}{}",
        stderr(&out)
    );
    assert!(
        !stderr(&out).contains("fake-bot-token-login"),
        "the token is never on stderr either"
    );

    // The credential file: exists, 0600, ids + token inside (the file
    // is the token's only home).
    let saved = std::fs::read_to_string(&credentials).expect("credential file written");
    let value: Value = serde_json::from_str(&saved).expect("credential file is JSON");
    assert_eq!(value["version"], json!(1));
    assert_eq!(value["account_id"], json!("fake-bot-9"));
    assert_eq!(value["user_id"], json!("fake-user-9"));
    assert_eq!(value["token"], json!("fake-bot-token-login"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&credentials)
            .expect("meta")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the credential file is 0600");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_json_mode_is_machine_readable_without_the_token() {
    force_no_proxy();
    let dir = temp_dir("wechat-login-json");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    mock.push_qr_status(json!({"status": "scaned"}));
    mock.push_qr_status(confirmed_body());

    let credentials = dir.join("login-credentials.json");
    let config = write_login_config(&dir, &base, &credentials);
    let out = run_kian(
        &[
            "wechat",
            "login",
            "--config",
            config.to_str().unwrap(),
            "--json",
        ],
        &[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
        ],
    );
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let report: Value = serde_json::from_str(&stdout(&out)).expect("stdout is one JSON object");
    assert_eq!(report["status"], json!("ok"));
    assert_eq!(report["account_id"], json!("fake-bot-9"));
    assert_eq!(report["user_id"], json!("fake-user-9"));
    assert_eq!(
        report["credentials_file"].as_str().unwrap(),
        credentials.display().to_string()
    );
    assert!(
        report.get("token").is_none() && !stdout(&out).contains("fake-bot-token-login"),
        "no token in --json output: {report}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_fails_with_a_nonzero_exit_when_the_qr_keeps_expiring() {
    force_no_proxy();
    let dir = temp_dir("wechat-login-expired");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    // `expired` more times than the refresh budget: the bounded
    // re-fetch exhausts and the flow fails honestly.
    for _ in 0..6 {
        mock.push_qr_status(json!({"status": "expired"}));
    }

    let credentials = dir.join("login-credentials.json");
    let config = write_login_config(&dir, &base, &credentials);
    let out = run_kian(
        &["wechat", "login", "--config", config.to_str().unwrap()],
        &[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
        ],
    );
    assert!(!out.status.success(), "non-zero exit on expiry");
    let text = format!("{}{}", stdout(&out), stderr(&out));
    assert!(
        text.contains("expired") || text.contains("refresh"),
        "a clear expiry message: {text}"
    );
    assert!(!credentials.exists(), "no credential file on failure");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_channel_flag_selects_and_rejects() {
    force_no_proxy();
    let dir = temp_dir("wechat-login-channel");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    mock.push_qr_status(confirmed_body());
    let credentials = dir.join("login-credentials.json");
    let config = write_login_config(&dir, &base, &credentials);

    // The named channel works.
    let out = run_kian(
        &[
            "wechat",
            "login",
            "--config",
            config.to_str().unwrap(),
            "--channel",
            "wechat",
        ],
        &[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
        ],
    );
    assert!(out.status.success(), "stderr: {}", stderr(&out));

    // An unknown name is a usage error, non-zero exit.
    let out = run_kian(
        &[
            "wechat",
            "login",
            "--config",
            config.to_str().unwrap(),
            "--channel",
            "ghost",
        ],
        &[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("no_proxy", "127.0.0.1,localhost"),
        ],
    );
    assert!(!out.status.success());
    assert!(stderr(&out).contains("ghost"), "names the bad channel");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// End-to-end: real `Server` (+ a real child-process restart) + sqlite +
// mock iLink.
// ---------------------------------------------------------------------------

use std::process::{Child, Stdio};

fn spawn_kian_serve(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kian serve")
}

/// A `kian.yaml` like [`write_wechat_config`] but with an explicit
/// listen port (the child-process legs need a real listener).
fn write_wechat_config_on_port(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    ilink_base: &str,
    credentials: &Path,
    port: u16,
) -> PathBuf {
    let config = write_wechat_config(
        dir,
        router_provider,
        worker_provider,
        ilink_base,
        credentials,
    );
    let yaml = std::fs::read_to_string(&config).expect("read config");
    std::fs::write(
        &config,
        yaml.replace("127.0.0.1:7341", &format!("127.0.0.1:{port}")),
    )
    .expect("rewrite listen port");
    config
}

/// Opens the read-only store once the sqlite file exists (a child
/// server creates it at boot).
async fn open_read_only_when_ready(dir: &Path) -> kian_store::EventStore {
    let sqlite = dir.join("kian.sqlite");
    within(30, || sqlite.exists().then_some(())).await;
    kian_store::EventStore::open_read_only(&sqlite)
        .await
        .expect("read-only store")
}

/// One inbound `getupdates` batch carrying a single message.
fn updates_with(message: &Value) -> Value {
    json!({"ret": 0, "errcode": 0, "msgs": [message], "get_updates_buf": "mock-cursor"})
}

/// A message id well past 2^53 — the lossless uint64 discipline.
const BIG_ID: u64 = 9_876_543_210_987_654_321;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_one_inbound_admits_exactly_once_across_replay_and_restart() {
    force_no_proxy();
    let dir = temp_dir("wechat-e2e-admit");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
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

    let credentials = credential_path(&dir);
    write_credentials(&credentials, "fake-bot-token-e2e");
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let config =
        write_wechat_config_on_port(&dir, &router_base, &worker_base, &base, &credentials, port);

    // One inbound message; the same batch is replayed later, and the
    // whole server is killed and restarted after that.
    let message = wx_message(json!(BIG_ID), USER, "please summarize", "ctx-e2e-1");
    mock.push_updates(updates_with(&message));

    let mut child = spawn_kian_serve(&config);
    let read_only = open_read_only_when_ready(&dir).await;

    // The poll loop reached the mock and the message was admitted
    // exactly once, with the lossless string id and the forced
    // untrusted marker.
    let events = within_fut(30, || {
        let store = read_only.clone();
        async move {
            let events = match store.rebuild(router_session()).await {
                Ok(events) => events,
                // The child may still be running migrations when the
                // sqlite file first appears — retry inside the window
                // instead of panicking on "no such table".
                Err(_) => return None,
            };
            (wechat_admits(&events).len() == 1).then_some(events)
        }
    })
    .await
    .expect("one wechat admission");
    let admitted = wechat_admits(&events)[0];
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&json!(BIG_ID.to_string())),
        "event_id is the decimal message_id string, lossless (u64 > 2^53)"
    );
    assert_eq!(
        admitted.bytes.pointer("/content/kian/channel"),
        Some(&json!("wechat"))
    );
    assert_eq!(
        admitted.bytes.pointer("/content/kian/untrusted"),
        Some(&json!(true)),
        "the forced untrusted marker rides the admitted input"
    );

    // Replay the same message_id: no second admission (the durable
    // event_id guard, not just the in-memory ring).
    mock.push_updates(updates_with(&message));
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let events = read_only.rebuild(router_session()).await.expect("rebuild");
    assert_eq!(
        wechat_admits(&events).len(),
        1,
        "a replayed message_id admits nothing new"
    );

    // A REAL restart (kill -9 semantics: child.kill), then the same
    // message once more — still exactly one admission for the id.
    child.kill().expect("kill serve");
    let _ = child.wait();
    mock.push_updates(updates_with(&message));
    // Baseline taken BEFORE the respawn: the pre-kill child necessarily
    // already false here — only captures NEWER than this baseline belong
    // to the restarted process.
    let before = mock.request_count("/ilink/bot/getupdates");
    assert!(before > 0, "the pre-kill child polled at least once");
    let mut child = spawn_kian_serve(&config);
    // Wait until the RESTARTED process has (a) produced its own
    // getupdates captures and (b) consumed the replay batch we just
    // pushed (the batch carries the replayed message_id — getupdates
    // request bodies carry only the cursor, so script consumption is the
    // observable proof the replay was served).
    let new_polls = within_fut(30, || {
        let mock = mock.clone();
        async move {
            let count = mock.request_count("/ilink/bot/getupdates");
            let replay_pending = !mock.updates_script.lock().unwrap().is_empty();
            (count > before && !replay_pending).then_some(count - before)
        }
    })
    .await
    .expect("the restarted process polled and consumed the replay batch");
    eprintln!("restart-dedupe: baseline {before} polls, restarted process added {new_polls}");
    // Settle: whatever the restarted process does with the replay
    // (dedupe) happens after the poll returns.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let events = read_only.rebuild(router_session()).await.expect("rebuild");
    let admits_after_restart = wechat_admits(&events).len();
    assert_eq!(
        admits_after_restart, 1,
        "replay after a real restart (baseline {before} polls, restarted process added \
         {new_polls}) admits nothing new"
    );
    child.kill().expect("kill serve");
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_router_reply_produces_the_exact_sendmessage_payload() {
    force_no_proxy();
    let dir = temp_dir("wechat-e2e-reply");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    // Router: ack the bootstrap, then REPLY on the wechat channel with
    // the documented target shape, then settle.
    let (router_base, _router) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::ToolCall {
                id: "call-reply-1".into(),
                name: "reply".into(),
                arguments: json!({
                    "channel": "wechat",
                    "text": "hello back",
                    "target": {"user_id": USER},
                }),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    let credentials = credential_path(&dir);
    write_credentials(&credentials, "fake-bot-token-send");
    let config = write_wechat_config(&dir, &router_base, &worker_base, &base, &credentials);
    let server = std::sync::Arc::new(
        kian_cli::server::Server::from_config_file(&config)
            .await
            .expect("assemble")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let router = server.router();
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });

    // Wait for the resident Router's bootstrap judge turn BEFORE the
    // inbound lands (otherwise the poll loop can fold the wechat
    // input into the bootstrap batch and the scripted reply tool call
    // would land on the wrong turn).
    let read_only = open_read_only_when_ready(&dir).await;
    let _bootstrap = within_fut(30, || {
        let store = read_only.clone();
        async move {
            let events = store
                .rebuild(router_session())
                .await
                .expect("rebuild router");
            (events
                .iter()
                .filter(|e| e.kind == kian_core::events::EventKind::TurnStart)
                .count()
                >= 1)
                .then_some(events)
        }
    })
    .await
    .expect("bootstrap judge turn");

    // One inbound message carries the context_token the reply must
    // echo (cached before fan-out).
    mock.push_updates(updates_with(&wx_message(
        json!(42),
        USER,
        "hi there",
        "ctx-inbound-42",
    )));

    let sends = within(30, || {
        let sends = mock.requests_for("/ilink/bot/sendmessage");
        (sends.len() == 1).then_some(sends)
    })
    .await
    .expect("exactly one sendmessage");

    // The exact CHANNEL-WECHAT §2.4 payload shape.
    let body = &sends[0].body;
    let msg = &body["msg"];
    assert_eq!(msg["from_user_id"], json!(""), "from_user_id is empty");
    assert_eq!(msg["to_user_id"], json!(USER));
    assert_eq!(msg["message_type"], json!(2));
    assert_eq!(msg["message_state"], json!(2));
    assert_eq!(
        msg["context_token"],
        json!("ctx-inbound-42"),
        "the reply echoes the LATEST inbound context_token"
    );
    assert_eq!(
        msg["item_list"],
        json!([{"type": 1, "text_item": {"text": "hello back"}}])
    );
    assert!(
        msg["client_id"].as_str().is_some_and(|id| !id.is_empty()),
        "client_id generated per delivery: {msg}"
    );
    assert!(
        body["base_info"]["channel_version"]
            .as_str()
            .is_some_and(|v| v.starts_with("kian/")),
        "base_info.channel_version is kian/<version>: {}",
        body["base_info"]
    );
    assert!(
        sends[0].authorization.contains("fake-bot-token-send"),
        "the send carries the credential token's Authorization header"
    );
    assert_eq!(sends[0].authorization_type, "ilink_bot_token");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_channel_failure_leaves_session_state_untouched() {
    force_no_proxy();
    let dir = temp_dir("wechat-e2e-failure");
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
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

    let credentials = credential_path(&dir);
    write_credentials(&credentials, "fake-bot-token-fail");
    let config = write_wechat_config(&dir, &router_base, &worker_base, &base, &credentials);
    let server = std::sync::Arc::new(
        kian_cli::server::Server::from_config_file(&config)
            .await
            .expect("assemble")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let router = server.router();
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });

    let read_only = kian_store::EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");

    // One good admission first…
    mock.push_updates(updates_with(&wx_message(
        json!(7),
        USER,
        "before the failure",
        "ctx-7",
    )));
    let events = within_fut(30, || {
        let store = read_only.clone();
        async move {
            let events = store
                .rebuild(router_session())
                .await
                .expect("rebuild router");
            (wechat_admits(&events).len() == 1).then_some(events)
        }
    })
    .await
    .expect("the first message admits");
    let state_events_before = events
        .iter()
        .filter(|e| {
            e.kind == kian_core::events::EventKind::SessionCreated
                || matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. })
        })
        .count();

    // …then the channel breaks hard: errcode -14 (stale token) pauses
    // the channel for an hour. The session stream must not move.
    mock.push_updates(json!({"ret": 0, "errcode": -14, "errmsg": "stale token", "msgs": []}));
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let after = read_only.rebuild(router_session()).await.expect("rebuild");
    assert_eq!(
        wechat_admits(&after).len(),
        1,
        "the paused channel admits nothing"
    );
    let state_events_after = after
        .iter()
        .filter(|e| {
            e.kind == kian_core::events::EventKind::SessionCreated
                || matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. })
        })
        .count();
    assert_eq!(
        state_events_before, state_events_after,
        "a channel failure never terminates or mutates the resident session"
    );
    assert!(
        !after
            .iter()
            .any(|e| matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. })),
        "no honest session is mapped onto `completed` by a channel failure"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
