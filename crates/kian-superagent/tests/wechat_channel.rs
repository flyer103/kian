//! wechat-channel integration tests: a scripted in-process axum mock of
//! the iLink Bot API (login QR endpoints, getupdates long-poll,
//! sendmessage) — zero credentials, zero network, fake tokens only
//! (`fake-bot-token-…`). Pattern: `tests/lark_channel.rs`.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use futures::StreamExt;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError};
use kian_superagent::wechat::credentials::{self, WechatCredentials};
use kian_superagent::wechat::login::{LoginError, QrLoginFlow, QrStep, run_login};
use kian_superagent::wechat::protocol::IlinkClient;
use kian_superagent::{WechatChannel, WechatConfig};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The scripted iLink mock.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Captured {
    method: String,
    path: String,
    authorization: String,
    authorization_type: String,
    wechat_uin: String,
    ilink_app_id: String,
    bot_agent: String,
    body: Value,
}

struct IlinkMock {
    /// Scripted `getupdates` response bodies, served in order; the
    /// default after the script runs dry is an empty long-poll result.
    updates_script: Mutex<VecDeque<Value>>,
    /// Scripted `sendmessage` response bodies, in order; default is an
    /// accepted send with a u64::MAX message id (lossless-parse check).
    send_script: Mutex<VecDeque<Value>>,
    /// Scripted `get_qrcode_status` bodies, in order; default `wait`.
    qr_status_script: Mutex<VecDeque<Value>>,
    /// Optional hold before answering `get_qrcode_status` (the
    /// login-timeout leg: the server holds past the client deadline).
    qr_status_hold: Mutex<Duration>,
    /// Optional hold before answering `getupdates` (timeout + shutdown
    /// tests).
    updates_hold: Mutex<Duration>,
    /// Optional hold before answering `sendmessage`.
    send_hold: Mutex<Duration>,
    requests: Mutex<Vec<Captured>>,
    qr_fetches: Mutex<u64>,
    status_polls: Mutex<u64>,
}

impl IlinkMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            updates_script: Mutex::new(VecDeque::new()),
            send_script: Mutex::new(VecDeque::new()),
            qr_status_script: Mutex::new(VecDeque::new()),
            qr_status_hold: Mutex::new(Duration::ZERO),
            updates_hold: Mutex::new(Duration::ZERO),
            send_hold: Mutex::new(Duration::ZERO),
            requests: Mutex::new(Vec::new()),
            qr_fetches: Mutex::new(0),
            status_polls: Mutex::new(0),
        })
    }

    fn push_updates(&self, body: Value) {
        self.updates_script.lock().unwrap().push_back(body);
    }

    fn push_send(&self, body: Value) {
        self.send_script.lock().unwrap().push_back(body);
    }

    fn push_qr_status(&self, body: Value) {
        self.qr_status_script.lock().unwrap().push_back(body);
    }

    fn hold_updates(&self, hold: Duration) {
        *self.updates_hold.lock().unwrap() = hold;
    }

    fn hold_qr_status(&self, hold: Duration) {
        *self.qr_status_hold.lock().unwrap() = hold;
    }

    fn hold_sends(&self, hold: Duration) {
        *self.send_hold.lock().unwrap() = hold;
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

    fn qr_fetches(&self) -> u64 {
        *self.qr_fetches.lock().unwrap()
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

fn capture(
    state: &IlinkMock,
    method: &str,
    path: &str,
    headers: &axum::http::HeaderMap,
    body: Value,
) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    state.requests.lock().unwrap().push(Captured {
        method: method.to_string(),
        path: path.to_string(),
        authorization: header("authorization"),
        authorization_type: header("authorizationtype"),
        wechat_uin: header("x-wechat-uin"),
        ilink_app_id: header("ilink-app-id"),
        bot_agent: header("bot_agent"),
        body,
    });
}

async fn getupdates_endpoint(
    State(state): State<Arc<IlinkMock>>,
    headers: axum::http::HeaderMap,
    body: Json<Value>,
) -> Json<Value> {
    capture(&state, "POST", "/ilink/bot/getupdates", &headers, body.0);
    let hold = *state.updates_hold.lock().unwrap();
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
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
    capture(&state, "POST", "/ilink/bot/sendmessage", &headers, body.0);
    let hold = *state.send_hold.lock().unwrap();
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
    let scripted = state.send_script.lock().unwrap().pop_front();
    Json(scripted.unwrap_or(json!({
        "message_id": u64::MAX, "ret": 0, "errmsg": "ok"
    })))
}

async fn bot_qrcode_endpoint(
    State(state): State<Arc<IlinkMock>>,
    headers: axum::http::HeaderMap,
) -> Json<Value> {
    capture(
        &state,
        "GET",
        "/ilink/bot/get_bot_qrcode",
        &headers,
        json!(null),
    );
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
        "GET",
        "/ilink/bot/get_qrcode_status",
        &headers,
        json!(query.clone()),
    );
    *state.status_polls.lock().unwrap() += 1;
    let hold = *state.qr_status_hold.lock().unwrap();
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
    let scripted = state.qr_status_script.lock().unwrap().pop_front();
    Json(scripted.unwrap_or(json!({"status": "wait"})))
}

// ---------------------------------------------------------------------------
// Fixtures and helpers.
// ---------------------------------------------------------------------------

const USER: &str = "oUSER@im.wechat";

/// The macOS system proxy intercepts localhost (AGENTS.md gotcha).
fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }
}

fn temp_credential_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "kian-wechat-it-{tag}-{}-wechat-credentials.json",
        uuid::Uuid::new_v4()
    ))
}

fn write_credentials(path: &std::path::Path, token: &str) {
    credentials::save(
        path,
        &WechatCredentials::new(
            "https://ilinkai.weixin.qq.com",
            token,
            "fake-bot-1",
            "fake-user-1",
            1730000000000,
        ),
    )
    .expect("write credential fixture");
}

/// One raw `WeixinMessage` fixture (the §2.3 shape).
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

/// Test-tuned timings: fast polls, tiny backoff, a stale pause that
/// will not elapse inside a test, and a tight credential watch.
fn wechat_config(base: &str, credentials_file: &std::path::Path) -> WechatConfig {
    let mut config = WechatConfig::new(
        base,
        credentials_file,
        BTreeMap::from([(USER.to_string(), "wechat".to_string())]),
    );
    config.poll_interval = Duration::from_millis(15);
    config.poll_timeout = Duration::from_secs(5);
    config.backoff_start = Duration::from_millis(20);
    config.backoff_cap = Duration::from_millis(100);
    config.stale_pause = Duration::from_secs(60);
    config.credential_poll_interval = Duration::from_millis(25);
    config
}

/// Awaits `f` for at most `secs` — never unbounded wall-clock waits.
async fn within<T>(secs: u64, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(value) = f() {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Login state machine (mock QR endpoints).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn login_walks_wait_redirect_to_confirmed() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    // The redirect host points back at the same mock (a real flow would
    // switch to Tencent's redirect host).
    let redirect_host = base.trim_start_matches("http://").to_string();
    mock.push_qr_status(json!({"status": "wait"}));
    mock.push_qr_status(json!({"status": "scaned"}));
    mock.push_qr_status(json!({"status": "scaned_but_redirect", "redirect_host": redirect_host}));
    mock.push_qr_status(json!({
        "status": "confirmed",
        "bot_token": "fake-bot-token-login",
        "ilink_bot_id": "bot-9",
        "ilink_user_id": "user-9",
        "baseurl": "https://ilinkai.weixin.qq.com",
    }));

    let client = IlinkClient::new().expect("client");
    let mut flow = QrLoginFlow::new(client, &base, 3);
    let img = flow.fetch_qrcode().await.expect("qr fetch");
    assert_eq!(img, "https://qr.example/qr-1");

    // Step through the machine and observe every state.
    assert_eq!(flow.poll_status().await.expect("wait"), QrStep::Wait);
    assert_eq!(flow.poll_status().await.expect("scaned"), QrStep::Scanned);
    let QrStep::Redirected {
        redirect_host: moved,
    } = flow.poll_status().await.expect("redirect")
    else {
        panic!("redirect step");
    };
    assert_eq!(moved, redirect_host);
    assert_eq!(
        flow.poll_base(),
        base,
        "the redirect host resolves to the same mock base"
    );
    let QrStep::Success(material) = flow.poll_status().await.expect("confirmed") else {
        panic!("success step");
    };
    assert_eq!(material.bot_token, "fake-bot-token-login");
    assert_eq!(material.account_id, "bot-9");
    assert_eq!(material.user_id, "user-9");
    assert_eq!(material.base_url, "https://ilinkai.weixin.qq.com");

    // And the convenience driver reaches the same place from scratch.
    mock.push_qr_status(json!({"status": "confirmed", "bot_token": "fake-bot-token-login-2"}));
    let mut flow = QrLoginFlow::new(IlinkClient::new().expect("client"), &base, 3);
    let material = run_login(&mut flow, 8).await.expect("run to completion");
    assert_eq!(material.bot_token, "fake-bot-token-login-2");
}

/// M3: a client-side timeout on the ~35 s status long-poll is normal
/// and expected (§11) — the flow must treat it as `wait` and keep
/// polling, never abort the operator's login.
#[tokio::test]
async fn login_treats_a_status_longpoll_timeout_as_wait_then_confirms() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    // The server holds the first status poll far past the client
    // deadline (the real endpoint holds ~35 s).
    mock.hold_qr_status(Duration::from_secs(30));
    let client = IlinkClient::new().expect("client");
    let mut flow =
        QrLoginFlow::new(client, &base, 3).with_status_poll_timeout(Duration::from_millis(80));
    flow.fetch_qrcode().await.expect("qr fetch");

    // The first poll outlasts the 80 ms deadline: wait, not death.
    assert_eq!(
        flow.poll_status()
            .await
            .expect("a timeout is a wait, not a fatal error"),
        QrStep::Wait
    );

    // The hold released, the next poll confirms.
    mock.hold_qr_status(Duration::ZERO);
    mock.push_qr_status(json!({
        "status": "confirmed",
        "bot_token": "fake-bot-token-after-timeout",
        "ilink_bot_id": "bot-1",
        "ilink_user_id": "user-1",
    }));
    let QrStep::Success(material) = flow
        .poll_status()
        .await
        .expect("confirmed after the timeout")
    else {
        panic!("success step");
    };
    assert_eq!(material.bot_token, "fake-bot-token-after-timeout");
}

#[tokio::test]
async fn login_expired_refresh_is_bounded() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    for _ in 0..8 {
        mock.push_qr_status(json!({"status": "expired"}));
    }

    let mut flow = QrLoginFlow::new(IlinkClient::new().expect("client"), &base, 2);
    flow.fetch_qrcode().await.expect("qr fetch");
    // Two refreshes are allowed (each issuing a fresh QR)…
    assert!(matches!(
        flow.poll_status().await,
        Ok(QrStep::QrRefreshed { .. })
    ));
    assert!(matches!(
        flow.poll_status().await,
        Ok(QrStep::QrRefreshed { .. })
    ));
    // …the third expiry is the honest, bounded error.
    let error = flow.poll_status().await.expect_err("budget exhausts");
    assert!(
        matches!(error, LoginError::QrExpiredOut { budget: 2 }),
        "{error:?}"
    );
    assert_eq!(mock.qr_fetches(), 3, "initial fetch + 2 bounded refreshes");
}

#[tokio::test]
async fn login_surfaces_verifycode_states_and_binds_without_a_token() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    mock.push_qr_status(json!({"status": "need_verifycode"}));
    mock.push_qr_status(json!({"status": "verify_code_blocked"}));
    mock.push_qr_status(json!({"status": "binded_redirect"}));
    mock.push_qr_status(json!({
        "status": "binded_redirect",
        "bot_token": "fake-bot-token-binded",
        "ilink_bot_id": "bot-1", "ilink_user_id": "user-1",
    }));

    let mut flow = QrLoginFlow::new(IlinkClient::new().expect("client"), &base, 1);
    flow.fetch_qrcode().await.expect("qr fetch");
    assert_eq!(
        flow.poll_status().await.expect("code"),
        QrStep::NeedVerifyCode
    );
    assert_eq!(
        flow.poll_status().await.expect("blocked"),
        QrStep::VerifyCodeBlocked
    );
    // binded_redirect WITHOUT a token is the honest error.
    let error = flow.poll_status().await.expect_err("bare bind");
    assert!(matches!(error, LoginError::BindedWithoutToken), "{error:?}");
    // binded_redirect WITH a token is a success.
    let QrStep::Success(material) = flow.poll_status().await.expect("bound with token") else {
        panic!("success");
    };
    assert_eq!(material.bot_token, "fake-bot-token-binded");
}

// ---------------------------------------------------------------------------
// Receive: mapping, dedupe, allowlist, cursor echo (mock getupdates).
// ---------------------------------------------------------------------------

/// A sender that is NOT a key of `topics` (the allowlist-rejection case).
const ATTACKER: &str = "oATTACKER@im.wechat";

#[tokio::test]
async fn receive_maps_a_text_message_to_exactly_one_channel_event() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let message = wx_message(
        json!(18446744073709551615u64),
        USER,
        "build broken",
        "ctx-token-1",
    );
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [message],
        "get_updates_buf": "cursor-2",
        "longpolling_timeout_ms": 35000,
    }));
    // The same message again on the next poll (server replay): it must
    // not resurface (the in-memory ring; admission's event_id guard is
    // the durable backstop downstream).
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!(18446744073709551615u64), USER, "build broken", "ctx-token-1")],
        "get_updates_buf": "cursor-3",
    }));
    let base = mock.clone().serve().await;

    let cred = temp_credential_path("recv");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let mut stream = channel.subscribe();

    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event within 5s")
        .expect("stream open");
    // The full §3 mapping contract.
    assert_eq!(event.channel, "wechat");
    assert_eq!(
        event.event_id, "18446744073709551615",
        "uint64 id, lossless"
    );
    assert_eq!(event.topic.as_deref(), Some("wechat"));
    assert_eq!(event.sender, Some(json!({"user_id": USER})));
    assert_eq!(event.group, None);
    assert_eq!(
        event.payload,
        json!({"text": "build broken", "create_time_ms": 1730000000123u64, "item_types": [1], "from_user_id": USER})
    );
    assert_eq!(event.ts_ms, 1730000000123);

    // The replayed copy never arrives…
    let none = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        none.is_err() || none.unwrap().is_none(),
        "no second fan-out"
    );

    // …and the cursor is echoed on the next poll (empty at start, then
    // the server-issued opaque value — carried in-process only).
    let updates = mock.requests_for("/ilink/bot/getupdates");
    assert_eq!(updates[0].body["get_updates_buf"], "");
    assert!(
        updates.len() >= 2,
        "the loop kept polling after the scripted responses: {updates:?}"
    );
    assert_eq!(
        updates[1].body["get_updates_buf"], "cursor-2",
        "the returned cursor feeds the next request"
    );
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn unknown_senders_bot_echoes_and_non_text_never_surface() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let mut stranger = wx_message(json!("m1"), "oSTRANGER@im.wechat", "let me in", "ctx-a");
    stranger["from_user_id"] = json!("oSTRANGER@im.wechat");
    let mut echo = wx_message(json!("m2"), USER, "the bot's own reply", "ctx-b");
    echo["message_type"] = json!(2);
    let mut image = wx_message(json!("m3"), USER, "", "ctx-c");
    image["item_list"] = json!([{"type": 2, "image_item": {"key": "k"}}]);
    mock.push_updates(json!({
        "ret": 0, "errcode": 0, "msgs": [stranger, echo, image], "get_updates_buf": "c1",
    }));
    let base = mock.clone().serve().await;

    let cred = temp_credential_path("drops");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let mut stream = channel.subscribe();
    let none = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        none.is_err() || none.unwrap().is_none(),
        "allowlist drop, echo drop and non-text drop: nothing surfaces"
    );
    assert_eq!(channel.dropped_full(), 0);
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn receive_api_errors_back_off_and_then_recover() {
    force_no_proxy();
    let mock = IlinkMock::new();
    mock.push_updates(json!({"ret": 7, "errcode": 0, "errmsg": "server says no"}));
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-late"), USER, "after the storm", "ctx-late")],
        "get_updates_buf": "c9",
    }));
    let base = mock.clone().serve().await;

    let cred = temp_credential_path("backoff");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let mut stream = channel.subscribe();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the message surfaces after the backoff window")
        .expect("stream open");
    assert_eq!(event.event_id, "m-late");
    assert!(
        !channel.paused(),
        "a ret!=0 error is a backoff, not a pause"
    );
    std::fs::remove_file(cred).ok();
}

// ---------------------------------------------------------------------------
// The -14 pause and the credential-mtime resume.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_token_pauses_then_resumes_on_credential_change() {
    force_no_proxy();
    let mock = IlinkMock::new();
    mock.push_updates(json!({"ret": 0, "errcode": -14, "errmsg": "token expired"}));
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-resume"), USER, "fresh after relogin", "ctx-r")],
        "get_updates_buf": "c-resume",
    }));
    let base = mock.clone().serve().await;

    let cred = temp_credential_path("stale");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    assert!(!channel.paused());
    let mut stream = channel.subscribe();

    // The pause lands…
    within(5, || channel.paused().then_some(()))
        .await
        .expect("paused after -14");
    assert!(channel.stale_pauses() >= 1, "the pause is counted (metric)");
    // …and a send while paused is honestly unavailable, naming the fix.
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("held"),
        })
        .await
        .expect_err("paused channel does not send");
    assert!(
        matches!(error, ChannelError::Unavailable(ref why) if why.contains("kian wechat login"))
    );

    // The operator re-runs login: the file's mtime moves, the channel
    // re-reads and resumes — no restart.
    credentials::save(
        &cred,
        &WechatCredentials::new(
            "https://ilinkai.weixin.qq.com",
            "fake-bot-token-2",
            "fake-bot-1",
            "fake-user-1",
            1730000009999,
        ),
    )
    .expect("re-login write");

    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event after resume")
        .expect("stream open");
    assert_eq!(event.event_id, "m-resume");
    assert!(!channel.paused(), "resumed");

    // The resumed poll used the NEW bearer token (never logged, but
    // visible on the wire to the mock).
    let updates = mock.requests_for("/ilink/bot/getupdates");
    let last = updates.last().expect("polls captured");
    assert_eq!(last.authorization, "Bearer fake-bot-token-2");
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn malformed_credential_replacement_keeps_the_pause() {
    force_no_proxy();
    let mock = IlinkMock::new();
    mock.push_updates(json!({"ret": 0, "errcode": -14, "errmsg": "token expired"}));
    let base = mock.clone().serve().await;

    let cred = temp_credential_path("stale-bad");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let _stream = channel.subscribe();
    within(5, || channel.paused().then_some(()))
        .await
        .expect("paused");

    // A garbage replacement: fail-closed, still paused.
    std::fs::write(&cred, "{ not json").expect("garbage write");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(channel.paused(), "a malformed replacement keeps the pause");
    std::fs::remove_file(cred).ok();
}

// ---------------------------------------------------------------------------
// Send: exact payload, tokens, split, error mapping.
// ---------------------------------------------------------------------------

/// Subscribe, capture one inbound message (seeding the context-token
/// cache), return the channel.
async fn channel_with_one_inbound(
    mock: &Arc<IlinkMock>,
    base: &str,
    cred: &std::path::Path,
) -> WechatChannel {
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-seed"), USER, "hello bot", "ctx-token-1")],
        "get_updates_buf": "c-seed",
    }));
    let channel = WechatChannel::new("wechat", wechat_config(base, cred)).expect("channel");
    let mut stream = channel.subscribe();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("seed event")
        .expect("stream open");
    assert_eq!(event.event_id, "m-seed");
    assert_eq!(
        channel.cached_context_token(USER).as_deref(),
        Some("ctx-token-1"),
        "the token cache updated BEFORE fan-out"
    );
    channel
}

#[tokio::test]
async fn send_builds_the_exact_sendmessage_payload() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("send-shape");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;

    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("done in 3 words"),
        })
        .await
        .expect("send accepted");
    // The default mock answers with u64::MAX — parsed losslessly.
    assert_eq!(receipt.message_id, "18446744073709551615");
    assert!(receipt.ts_ms > 0);

    let sends = mock.requests_for("/ilink/bot/sendmessage");
    assert_eq!(sends.len(), 1);
    let body = &sends[0].body;
    // The exact §2.4 shape, field by field.
    assert_eq!(body.pointer("/msg/from_user_id"), Some(&json!("")));
    assert_eq!(body.pointer("/msg/to_user_id"), Some(&json!(USER)));
    assert_eq!(body.pointer("/msg/message_type"), Some(&json!(2)));
    assert_eq!(body.pointer("/msg/message_state"), Some(&json!(2)));
    assert_eq!(
        body.pointer("/msg/context_token"),
        Some(&json!("ctx-token-1"))
    );
    assert_eq!(
        body.pointer("/msg/item_list"),
        Some(&json!([{"type": 1, "text_item": {"text": "done in 3 words"}}]))
    );
    let client_id = body
        .pointer("/msg/client_id")
        .and_then(Value::as_str)
        .unwrap();
    assert!(!client_id.is_empty(), "client_id is generated per delivery");
    assert_eq!(body["base_info"], json!({"channel_version": "2.4.10"}));
    // Exactly the two top-level keys — no extra fields.
    let mut keys: Vec<&str> = body
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["base_info", "msg"]);

    // Headers: the full §2.2 set on the send (and on every poll). The
    // QR endpoints are GETs; the poll/send endpoints are POSTs.
    for captured in mock.requests.lock().unwrap().iter() {
        assert_eq!(captured.authorization, "Bearer fake-bot-token-1");
        assert_eq!(captured.authorization_type, "ilink_bot_token");
        assert!(!captured.wechat_uin.is_empty(), "X-WECHAT-UIN present");
        assert_eq!(captured.ilink_app_id, "bot");
        assert!(
            captured.bot_agent.starts_with("kian/"),
            "bot_agent UA-style"
        );
        let expected_method = if captured.path.ends_with("get_bot_qrcode")
            || captured.path.ends_with("get_qrcode_status")
        {
            "GET"
        } else {
            "POST"
        };
        assert_eq!(captured.method, expected_method);
    }
    // The uin is fresh per request: every captured request carries a
    // distinct one.
    let all = mock.requests.lock().unwrap().clone();
    let uins: std::collections::BTreeSet<String> =
        all.iter().map(|r| r.wechat_uin.clone()).collect();
    assert!(!uins.is_empty());
    assert_eq!(
        uins.len(),
        all.len(),
        "one fresh X-WECHAT-UIN per request: {uins:?} vs {} requests",
        all.len()
    );
    std::fs::remove_file(cred).ok();
}

/// The model may emit the channel-defined target as a JSON-encoded
/// string (one tool-argument dialect); the adapter parses the string
/// form before the allowlist checks, so a well-formed target is never
/// rejected over its wire spelling.
#[tokio::test]
async fn a_json_string_target_is_accepted_for_send() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("send-string-target");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;

    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!(format!("{{\"user_id\": \"{USER}\"}}")),
            content: json!("string-shaped target"),
        })
        .await
        .expect("a JSON-encoded target string is accepted");
    assert_eq!(receipt.message_id, "18446744073709551615");

    let sends = mock.requests_for("/ilink/bot/sendmessage");
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].body.pointer("/msg/to_user_id"), Some(&json!(USER)));
    std::fs::remove_file(cred).ok();
}

/// B1: the send path must not reach a sender the allowlist rejected.
/// An inbound from a non-allowlisted user carries a `context_token`, is
/// dropped as `unknown_sender`, and must NOT become a reachable send
/// target — neither through the token cache nor through the send gate.
#[tokio::test]
async fn non_allowlisted_senders_never_become_reachable_send_targets() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("allowlist-send");
    write_credentials(&cred, "fake-bot-token-1");
    // One admitted inbound from the allowlisted USER plus one dropped
    // inbound from an attacker who DOES carry a context_token.
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [
            wx_message(json!("m-ok"), USER, "hello", "ctx-allowlisted"),
            wx_message(json!("m-bad"), ATTACKER, "exfiltrate via reply", "ctx-attacker"),
        ],
        "get_updates_buf": "c-allow",
    }));
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let mut stream = channel.subscribe();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the allowlisted message surfaces")
        .expect("stream open");
    assert_eq!(event.event_id, "m-ok");
    // The allowlisted sender's token IS cached before fan-out…
    assert_eq!(
        channel.cached_context_token(USER).as_deref(),
        Some("ctx-allowlisted")
    );
    // …and the attacker's never was: the cache is admit-only.
    assert_eq!(
        channel.cached_context_token(ATTACKER),
        None,
        "a dropped inbound's context_token must never be cached"
    );

    // A send to the attacker is refused before any wire traffic, even
    // though an inbound from them carried a context_token.
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": ATTACKER}),
            content: json!("leak"),
        })
        .await
        .expect_err("a non-allowlisted target is never sendable");
    assert!(
        matches!(&error, ChannelError::DeliveryFailed(why) if why.contains("allowlist")),
        "the refusal names the allowlist: {error}"
    );
    assert_eq!(
        mock.request_count("/ilink/bot/sendmessage"),
        0,
        "no wire traffic to a non-allowlisted recipient"
    );

    // The allowlisted user still receives — the gate is not a blanket
    // refusal.
    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("legitimate reply"),
        })
        .await
        .expect("an allowlisted target sends");
    assert!(!receipt.message_id.is_empty());
    assert_eq!(mock.request_count("/ilink/bot/sendmessage"), 1);
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn a_send_without_a_cached_token_is_honestly_unavailable() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("no-token");
    write_credentials(&cred, "fake-bot-token-1");
    // No inbound ⇒ no context_token for the user.
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("proactive"),
        })
        .await
        .expect_err("no token ⇒ no send");
    assert!(
        matches!(&error, ChannelError::Unavailable(why) if why.contains("proactive push")),
        "the honest reason: {error}"
    );
    assert_eq!(
        mock.request_count("/ilink/bot/sendmessage"),
        0,
        "no wire traffic"
    );
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn degenerate_targets_contents_and_channel_names_fail_before_the_wire() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("degenerate");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;

    for bad in [
        json!("oUSER"),
        json!({}),
        json!({"user_id": ""}),
        json!({"chat_id": "x"}),
    ] {
        let error = channel
            .send(ChannelDelivery {
                channel: "wechat".into(),
                target: bad.clone(),
                content: json!("x"),
            })
            .await
            .expect_err("bad target errors");
        assert!(
            matches!(error, ChannelError::DeliveryFailed(ref why) if why.contains("user_id")),
            "names the expected shape: {error}"
        );
    }
    for bad in [
        json!(42),
        json!(null),
        json!({"text": 7}),
        json!({"other": "x"}),
    ] {
        let error = channel
            .send(ChannelDelivery {
                channel: "wechat".into(),
                target: json!({"user_id": USER}),
                content: bad,
            })
            .await
            .expect_err("bad content errors");
        assert!(matches!(error, ChannelError::DeliveryFailed(_)), "{error}");
    }
    let error = channel
        .send(ChannelDelivery {
            channel: "lark".into(),
            target: json!({"user_id": USER}),
            content: json!("x"),
        })
        .await
        .expect_err("cross-channel delivery errors");
    assert!(matches!(error, ChannelError::DeliveryFailed(_)), "{error}");
    assert_eq!(
        mock.request_count("/ilink/bot/sendmessage"),
        0,
        "no wire traffic"
    );

    // The object form {"text": …} is accepted content.
    channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!({"text": "object form"}),
        })
        .await
        .expect("object content accepted");
    let sends = mock.requests_for("/ilink/bot/sendmessage");
    assert_eq!(
        sends[0].body.pointer("/msg/item_list/0/text_item/text"),
        Some(&json!("object form"))
    );
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn long_text_splits_into_ordered_deliveries() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("split");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;

    // 4500 chars ⇒ 3 ordered parts (2000 + 2000 + 500).
    let text = format!(
        "{}{}{}",
        "a".repeat(2000),
        "b".repeat(2000),
        "c".repeat(500)
    );
    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!(text),
        })
        .await
        .expect("split send accepted");
    assert!(receipt.ts_ms > 0, "the final part's receipt resolves");

    let sends = mock.requests_for("/ilink/bot/sendmessage");
    assert_eq!(sends.len(), 3, "three parts, each its own delivery");
    let texts: Vec<String> = sends
        .iter()
        .map(|s| {
            s.body
                .pointer("/msg/item_list/0/text_item/text")
                .and_then(Value::as_str)
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(texts[0].chars().count(), 2000);
    assert!(texts[0].chars().all(|c| c == 'a'));
    assert_eq!(texts[1].chars().count(), 2000);
    assert!(texts[1].chars().all(|c| c == 'b'));
    assert_eq!(texts[2].chars().count(), 500);
    assert!(texts[2].chars().all(|c| c == 'c'));
    // Distinct client ids per delivery.
    let ids: std::collections::BTreeSet<&str> = sends
        .iter()
        .map(|s| {
            s.body
                .pointer("/msg/client_id")
                .and_then(Value::as_str)
                .unwrap()
        })
        .collect();
    assert_eq!(ids.len(), 3);
    // Every part echoes the same inbound context token.
    assert!(
        sends
            .iter()
            .all(|s| s.body.pointer("/msg/context_token") == Some(&json!("ctx-token-1")))
    );

    // A multibyte text splits at char boundaries (never mid-codepoint).
    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("é".repeat(2500)),
        })
        .await
        .expect("multibyte split send accepted");
    assert!(receipt.ts_ms > 0);
    let sends = mock.requests_for("/ilink/bot/sendmessage");
    let multibyte: Vec<&str> = sends[3..]
        .iter()
        .map(|s| {
            s.body
                .pointer("/msg/item_list/0/text_item/text")
                .and_then(Value::as_str)
                .unwrap()
        })
        .collect();
    assert_eq!(
        multibyte.len(),
        2,
        "2500 chars at a 2000-char limit: {multibyte:?}"
    );
    assert_eq!(multibyte[0].chars().count(), 2000);
    assert_eq!(multibyte[1].chars().count(), 500);
    assert_eq!(multibyte.concat(), "é".repeat(2500), "lossless");
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn send_error_mapping_is_honest_per_the_degradation_table() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("send-errors");
    write_credentials(&cred, "fake-bot-token-1");

    // ret != 0 ⇒ DeliveryFailed naming ret.
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-e1"), USER, "hi", "ctx-e1")],
        "get_updates_buf": "c-e1",
    }));
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    let mut stream = channel.subscribe();
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("seed event")
        .expect("stream open");
    mock.push_send(json!({"message_id": 0, "ret": 500, "errmsg": "denied"}));
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("x"),
        })
        .await
        .expect_err("ret != 0 fails the delivery");
    assert!(
        matches!(error, ChannelError::DeliveryFailed(ref why) if why.contains("ret=500")),
        "the honest error names ret: {error}"
    );
    assert!(!channel.paused(), "a plain ret != 0 is not a pause");
    std::fs::remove_file(cred).ok();
}

/// M4 (settled by the live L3 wire): the real `sendmessage` success
/// body omits `ret`, so an absent ret is success (official-client
/// parity); a non-object body is still malformed, and `ret: 0` with a
/// failing `errcode` is still a failure — nothing synthesized (§11).
#[tokio::test]
async fn a_missing_ret_or_a_failing_errcode_is_never_a_success() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("ret-honesty");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;

    // (a) `ret` absent is the live wire's success shape: a receipt is
    // minted, falling back to our client_id when no message_id exists.
    mock.push_send(json!({"message_id": 1, "errmsg": "ok"}));
    let receipt = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("probe missing ret"),
        })
        .await
        .expect("a ret-less success body is accepted");
    assert_eq!(receipt.message_id, "1");

    // (a2) a non-object body is malformed, never a success.
    mock.push_send(json!([]));
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("probe non-object"),
        })
        .await
        .expect_err("a non-object body is never a success");
    assert!(
        matches!(&error, ChannelError::Unavailable(why) if why.contains("malformed")),
        "the honest error names the malformed body: {error}"
    );

    // (b) `ret: 0` with a non-zero, non--14 errcode is a failure too
    // (symmetric with the receive path's classify).
    mock.push_send(json!({"message_id": 2, "ret": 0, "errcode": -1, "errmsg": "denied"}));
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("probe errcode"),
        })
        .await
        .expect_err("ret 0 with errcode -1 is never a success");
    assert!(
        matches!(&error, ChannelError::Unavailable(why) if why.contains("errcode=-1")),
        "the honest error names the errcode: {error}"
    );
    assert!(
        !channel.paused(),
        "errcode -1 is an api error, not a -14 pause"
    );
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn send_timeout_maps_to_channel_timeout() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("send-timeout");
    write_credentials(&cred, "fake-bot-token-1");
    let mut config = wechat_config(&base, &cred);
    config.send_timeout = Duration::from_millis(80);
    // Seed the token cache with one inbound through the wire.
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-t"), USER, "hi", "ctx-t")],
        "get_updates_buf": "c-t",
    }));
    let channel = WechatChannel::new("wechat", config).expect("channel");
    let mut stream = channel.subscribe();
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("seed")
        .expect("stream open");
    // The send is held past its 80 ms deadline ⇒ ChannelError::Timeout.
    mock.hold_sends(Duration::from_millis(600));
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("slow"),
        })
        .await
        .expect_err("the held send times out");
    assert!(matches!(error, ChannelError::Timeout), "{error}");
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn send_observing_stale_token_pauses_the_channel() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("send-stale");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = channel_with_one_inbound(&mock, &base, &cred).await;
    mock.push_send(json!({"message_id": 0, "ret": 0, "errcode": -14, "errmsg": "stale"}));
    let error = channel
        .send(ChannelDelivery {
            channel: "wechat".into(),
            target: json!({"user_id": USER}),
            content: json!("x"),
        })
        .await
        .expect_err("-14 on send");
    assert!(
        matches!(&error, ChannelError::Unavailable(why) if why.contains("kian wechat login")),
        "{error}"
    );
    assert!(channel.paused(), "a send-side -14 pauses the channel");
    assert_eq!(channel.stale_pauses(), 1);
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn network_failures_surface_as_transport_errors() {
    force_no_proxy();
    // Port 9 (discard) is closed on macOS: connection refused — a real
    // transport failure, no mock needed.
    let client = IlinkClient::new().expect("client");
    let error = client
        .send_message(
            "http://127.0.0.1:9",
            "fake-bot-token-1",
            &json!({"from_user_id": ""}),
            Duration::from_secs(2),
        )
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            kian_superagent::wechat::protocol::IlinkError::Transport(_)
        ),
        "{error}"
    );
    // …and the adapter maps transport to Unavailable (unit-checked in
    // src; asserted here through the public mapper for completeness).
    assert!(matches!(
        kian_superagent::wechat::ilink_error_to_channel_error(error),
        ChannelError::Unavailable(_)
    ));
}

// ---------------------------------------------------------------------------
// Long-poll timeout handling and graceful shutdown.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_side_longpoll_timeouts_are_normal_and_not_pauses() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("lp-timeout");
    write_credentials(&cred, "fake-bot-token-1");
    let mut config = wechat_config(&base, &cred);
    config.poll_timeout = Duration::from_millis(80);
    // The mock holds longer than the client deadline: every poll times
    // out — normal, no backoff escalation, no pause.
    mock.hold_updates(Duration::from_millis(400));
    let channel = WechatChannel::new("wechat", config).expect("channel");
    let mut stream = channel.subscribe();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!channel.paused(), "a client timeout is normal, not a pause");
    assert!(
        mock.request_count("/ilink/bot/getupdates") >= 2,
        "the loop kept polling"
    );

    // And when a message finally lands (hold released), it surfaces.
    mock.hold_updates(Duration::ZERO);
    mock.push_updates(json!({
        "ret": 0, "errcode": 0,
        "msgs": [wx_message(json!("m-after-timeout"), USER, "late", "ctx-after")],
        "get_updates_buf": "c-after",
    }));
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event after the hold clears")
        .expect("stream open");
    assert_eq!(event.event_id, "m-after-timeout");
    std::fs::remove_file(cred).ok();
}

#[tokio::test]
async fn graceful_shutdown_aborts_the_inflight_longpoll() {
    force_no_proxy();
    let mock = IlinkMock::new();
    let base = mock.clone().serve().await;
    let cred = temp_credential_path("shutdown");
    write_credentials(&cred, "fake-bot-token-1");
    let channel = WechatChannel::new("wechat", wechat_config(&base, &cred)).expect("channel");
    // A long-held request the shutdown must abort (not wait out).
    mock.hold_updates(Duration::from_secs(10));

    let driver = channel.clone();
    let shutdown = channel.shutdown_signal();
    let task = tokio::spawn(async move {
        driver.run(shutdown).await;
    });
    within(5, || {
        (mock.request_count("/ilink/bot/getupdates") >= 1).then_some(())
    })
    .await
    .expect("the in-flight poll started");
    channel.shutdown();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the driver exits promptly, aborting the in-flight request")
        .expect("task joined");
    std::fs::remove_file(cred).ok();
}

// ---------------------------------------------------------------------------
// Config validation (fail-closed construction).
// ---------------------------------------------------------------------------

#[test]
fn degenerate_configs_fail_closed() {
    let dir = std::env::temp_dir().join(format!("kian-wechat-cfg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let cred = dir.join("wechat-credentials.json");
    write_credentials(&cred, "fake-bot-token-1");

    let topics = BTreeMap::from([(USER.to_string(), "wechat".to_string())]);
    let config = WechatConfig::new("http://127.0.0.1:1", &cred, topics.clone());
    assert!(
        config.validate().is_ok(),
        "a valid config with a live credential file passes"
    );

    let mut bad = config.clone();
    bad.api_base = "ftp://x".into();
    assert!(matches!(
        bad.validate(),
        Err(kian_superagent::WechatError::BadApiBase(_))
    ));

    let mut bad = config.clone();
    bad.topics.clear();
    assert!(matches!(
        bad.validate(),
        Err(kian_superagent::WechatError::NoTopics)
    ));

    let mut bad = config.clone();
    bad.topics.insert("".into(), "t".into());
    assert!(matches!(
        bad.validate(),
        Err(kian_superagent::WechatError::EmptyTopicEntry(_))
    ));

    // Missing credential file: fail-closed naming the command.
    let missing = WechatConfig::new("http://127.0.0.1:1", dir.join("nope.json"), topics.clone());
    let error = missing.validate().expect_err("missing file");
    assert!(
        error.to_string().contains("kian wechat login"),
        "names the fix: {error}"
    );

    // Malformed credential file: fail-closed, contents never quoted.
    let garbage = dir.join("garbage.json");
    std::fs::write(&garbage, "not json at all").expect("garbage");
    let malformed = WechatConfig::new("http://127.0.0.1:1", &garbage, topics);
    let error = malformed.validate().expect_err("garbage file");
    assert!(
        !error.to_string().contains("not json"),
        "no contents leaked: {error}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
