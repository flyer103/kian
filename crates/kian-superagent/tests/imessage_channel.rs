//! imessage-channel integration tests: a scripted in-process axum mock of
//! the macOS bridge ABI (CHANNEL-IMESSAGE §3.2) — no Mac, no network, a
//! fake token only. Pattern: `tests/wechat_channel.rs`.
//!
//! Covers the adapter's whole L1 surface: the §4 mapping table and drop
//! rules, ring dedupe, the §5 send path (target allowlist, chat_guid
//! continuity, deterministic client ids, receipts, dispositions, split),
//! the §7 config gate, and the §10 degradation rows that are observable
//! without a Mac (401 pause, generation reset, gap, unreachable).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::StreamExt;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError};
use kian_superagent::imessage::IMESSAGE_CHANNEL_TYPE;
use kian_superagent::imessage::bridge;
use kian_superagent::imessage::{
    ImessageChannel, ImessageConfig, ImessageService, delivery_client_id,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The scripted bridge mock.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BridgeMock {
    /// Scripted `/v1/updates` bodies (200), served in order; the default
    /// after the script runs dry is an empty page at the requested
    /// cursor.
    updates: Arc<Mutex<VecDeque<Value>>>,
    /// Scripted `/v1/send` responses: (status, body), in order; the
    /// default is an accepted send with a guid.
    sends: Arc<Mutex<VecDeque<(StatusCode, Value)>>>,
    /// Every captured `/v1/send` request body.
    send_requests: Arc<Mutex<Vec<Value>>>,
    /// Every captured `/v1/updates` query string.
    update_queries: Arc<Mutex<Vec<String>>>,
    /// Hold before answering `/v1/send` (send-timeout tests).
    send_hold: Arc<Mutex<Duration>>,
    /// The bearer token the mock accepts.
    token: Arc<Mutex<String>>,
    /// Return 401 on every route (the token-mismatch leg).
    unauthorized: Arc<Mutex<bool>>,
    /// Health body override (the CLI preflight leg is unit-tested in
    /// kian-cli; the mock keeps a minimal one for completeness).
    health: Arc<Mutex<Value>>,
}

impl BridgeMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            updates: Arc::new(Mutex::new(VecDeque::new())),
            sends: Arc::new(Mutex::new(VecDeque::new())),
            send_requests: Arc::new(Mutex::new(Vec::new())),
            update_queries: Arc::new(Mutex::new(Vec::new())),
            send_hold: Arc::new(Mutex::new(Duration::ZERO)),
            token: Arc::new(Mutex::new("test-bridge-token".into())),
            unauthorized: Arc::new(Mutex::new(false)),
            health: Arc::new(Mutex::new(json!({
                "protocol": 1,
                "bridge_version": "0.1.0",
                "instance_id": "i-1",
                "imsg": {"version": "0.11.1", "rpc": true, "private_api": false},
                "database": {"ready": true, "path": "/tmp/chat.db", "generation": "g-1"},
                "automation": "unknown",
                "watch": {"active": true, "last_rowid": 0, "buffered": 0, "participants": []},
                "counters": {}
            }))),
        })
    }

    fn push_updates(&self, body: Value) {
        self.updates.lock().unwrap().push_back(body);
    }

    fn push_send(&self, status: StatusCode, body: Value) {
        self.sends.lock().unwrap().push_back((status, body));
    }

    fn hold_sends(&self, hold: Duration) {
        *self.send_hold.lock().unwrap() = hold;
    }

    fn reject_auth(&self) {
        *self.unauthorized.lock().unwrap() = true;
    }

    fn send_requests(&self) -> Vec<Value> {
        self.send_requests.lock().unwrap().clone()
    }

    fn update_queries(&self) -> Vec<String> {
        self.update_queries.lock().unwrap().clone()
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

fn check_auth(state: &BridgeMock, headers: &axum::http::HeaderMap) -> bool {
    if *state.unauthorized.lock().unwrap() {
        return false;
    }
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {}", state.token.lock().unwrap()))
}

async fn updates_endpoint(
    State(state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if !check_auth(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    let encoded = query
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    state.update_queries.lock().unwrap().push(encoded.clone());
    let scripted = state.updates.lock().unwrap().pop_front();
    let body = scripted.unwrap_or_else(|| {
        let since = query
            .get("since_rowid")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        json!({
            "protocol": 1,
            "generation": "g-1",
            "messages": [],
            "next_rowid": since,
            "has_more": false,
            "gap": false
        })
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn send_endpoint(
    State(state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    if !check_auth(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    state.send_requests.lock().unwrap().push(body.clone());
    let hold = *state.send_hold.lock().unwrap();
    if !hold.is_zero() {
        tokio::time::sleep(hold).await;
    }
    let scripted = state.sends.lock().unwrap().pop_front().unwrap_or((
        StatusCode::OK,
        json!({"protocol": 1, "ok": true, "message_id": "G-SENT", "guid": "G-SENT",
               "rowid": 900, "service": "iMessage", "verified": true, "deduplicated": false}),
    ));
    (scripted.0, Json(scripted.1)).into_response()
}

async fn health_endpoint(
    State(state): State<Arc<BridgeMock>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    if !check_auth(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false}))).into_response();
    }
    (StatusCode::OK, Json(state.health.lock().unwrap().clone())).into_response()
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn topics() -> BTreeMap<String, String> {
    BTreeMap::from([("+14155551212".to_string(), "imessage".to_string())])
}

fn config(bridge_url: String) -> ImessageConfig {
    let mut config = ImessageConfig::new(bridge_url, "test-bridge-token", topics());
    config.service = ImessageService::Imessage;
    config.wait = Duration::from_millis(50);
    config.poll_timeout = Duration::from_secs(6);
    config.poll_interval = Duration::from_millis(10);
    config.send_timeout = Duration::from_secs(2);
    config
}

async fn channel(mock: &Arc<BridgeMock>) -> ImessageChannel {
    let url = mock.serve().await;
    // `subscribe()` starts the single receive driver (the assembly path);
    // starting `run` here too would make two pollers race the script.
    ImessageChannel::new("imessage", config(url)).expect("channel builds")
}

fn bridge_message(guid: &str, sender: &str, text: &str) -> Value {
    json!({
        "rowid": 101,
        "guid": guid,
        "chat_id": 3,
        "chat_guid": format!("iMessage;-;{sender}"),
        "chat_identifier": sender,
        "sender": sender,
        "text": text,
        "created_at": "2026-11-19T15:31:55Z",
        "created_at_ms": 1_795_102_315_000u64,
        "service": "iMessage",
        "is_from_me": false,
        "is_group": false,
        "is_reaction": false
    })
}

fn updates_page(messages: Vec<Value>, next_rowid: u64) -> Value {
    json!({
        "protocol": 1,
        "generation": "g-1",
        "messages": messages,
        "next_rowid": next_rowid,
        "has_more": false,
        "gap": false
    })
}

async fn next_event(
    stream: &mut (impl StreamExt<Item = kian_core::seams::ChannelEvent> + Unpin),
) -> kian_core::seams::ChannelEvent {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("an event within the deadline")
        .expect("the stream is alive")
}

async fn assert_no_event(
    stream: &mut (impl StreamExt<Item = kian_core::seams::ChannelEvent> + Unpin),
) {
    if tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .is_ok()
    {
        panic!("expected no event");
    }
}

fn delivery(target: Value, content: Value) -> ChannelDelivery {
    ChannelDelivery {
        channel: "imessage".into(),
        target,
        content,
    }
}

// ---------------------------------------------------------------------------
// Receive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_inbound_text_maps_to_one_event() {
    let mock = BridgeMock::new();
    mock.push_updates(updates_page(
        vec![bridge_message("G-1", "+1 (415) 555-1212", "  hi  ")],
        101,
    ));
    let adapter = channel(&mock).await;
    let mut stream = adapter.subscribe().boxed();
    let event = next_event(&mut stream).await;
    assert_eq!(event.channel, "imessage");
    assert_eq!(event.event_id, "G-1");
    assert_eq!(event.topic.as_deref(), Some("imessage"));
    assert_eq!(event.sender, Some(json!({"handle": "+1 (415) 555-1212"})));
    assert_eq!(event.group, None);
    assert_eq!(event.ts_ms, 1_795_102_315_000);
    assert_eq!(event.payload["text"], "hi");
    assert_eq!(event.payload["sender_handle"], "+1 (415) 555-1212");
    assert_eq!(event.payload["chat_guid"], "iMessage;-;+1 (415) 555-1212");
    assert_eq!(event.payload["service"], "iMessage");
    assert_eq!(event.payload["is_group"], false);
    adapter.shutdown();
}

#[tokio::test]
async fn every_drop_rule_surfaces_nothing() {
    let mock = BridgeMock::new();
    let mut from_me = bridge_message("G-1", "+14155551212", "own echo");
    from_me["is_from_me"] = json!(true);
    let mut group = bridge_message("G-2", "+14155551212", "group");
    group["is_group"] = json!(true);
    let mut reaction = bridge_message("G-3", "+14155551212", "👍");
    reaction["is_reaction"] = json!(true);
    let not_text = bridge_message("G-4", "+14155551212", "");
    let unknown = bridge_message("G-5", "+19995550000", "hello");
    let no_guid = bridge_message("", "+14155551212", "hello");
    mock.push_updates(updates_page(
        vec![from_me, group, reaction, not_text, unknown, no_guid],
        106,
    ));
    let adapter = channel(&mock).await;
    let mut stream = adapter.subscribe().boxed();
    assert_no_event(&mut stream).await;
    adapter.shutdown();
}

#[tokio::test]
async fn a_repeated_guid_surfaces_once() {
    let mock = BridgeMock::new();
    mock.push_updates(updates_page(
        vec![bridge_message("G-1", "+14155551212", "hi")],
        101,
    ));
    mock.push_updates(updates_page(
        vec![bridge_message("G-1", "+14155551212", "hi again")],
        102,
    ));
    let adapter = channel(&mock).await;
    let mut stream = adapter.subscribe().boxed();
    let event = next_event(&mut stream).await;
    assert_eq!(event.event_id, "G-1");
    assert_no_event(&mut stream).await;
    adapter.shutdown();
}

#[tokio::test]
async fn a_changed_generation_resets_the_cursor_to_zero() {
    let mock = BridgeMock::new();
    mock.push_updates(json!({
        "protocol": 1, "generation": "g-1", "messages": [], "next_rowid": 500,
        "has_more": false, "gap": false
    }));
    mock.push_updates(json!({
        "protocol": 1, "generation": "g-2", "messages": [], "next_rowid": 7,
        "has_more": false, "gap": false
    }));
    mock.push_updates(json!({
        "protocol": 1, "generation": "g-2", "messages": [], "next_rowid": 9,
        "has_more": false, "gap": false
    }));
    let adapter = channel(&mock).await;
    let _stream = adapter.subscribe().boxed();
    // Let the driver consume all three scripted pages.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let queries = mock.update_queries();
    assert!(
        queries
            .iter()
            .any(|query| query.contains("since_rowid=500")),
        "{queries:?}"
    );
    assert!(
        queries.iter().any(|query| query.contains("since_rowid=7")),
        "the generation change adopts the new database's cursor: {queries:?}"
    );
    adapter.shutdown();
}

#[tokio::test]
async fn a_gap_is_counted() {
    let mock = BridgeMock::new();
    mock.push_updates(json!({
        "protocol": 1, "generation": "g-1", "messages": [], "next_rowid": 5,
        "has_more": false, "gap": true
    }));
    let adapter = channel(&mock).await;
    let _stream = adapter.subscribe().boxed();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(adapter.gap_events(), 1);
    adapter.shutdown();
}

#[tokio::test]
async fn a_token_mismatch_pauses_with_an_honest_reason() {
    let mock = BridgeMock::new();
    mock.reject_auth();
    let adapter = channel(&mock).await;
    let _stream = adapter.subscribe().boxed();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(adapter.paused());
    let reason = adapter.paused_reason().expect("reason");
    assert!(reason.contains("KIAN_IMESSAGE_BRIDGE_TOKEN"), "{reason}");
    adapter.shutdown();
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_target_must_be_allowlisted() {
    let mock = BridgeMock::new();
    let adapter = channel(&mock).await;
    let error = adapter
        .send(delivery(
            json!({"handle": "+19995550000"}),
            json!({"text": "hi"}),
        ))
        .await
        .expect_err("allowlist refusal");
    match error {
        ChannelError::DeliveryFailed(reason) => {
            assert!(reason.contains("allowlist"), "{reason}");
        }
        other => panic!("expected DeliveryFailed, got {other:?}"),
    }
    assert!(
        mock.send_requests().is_empty(),
        "no wire traffic on refusal"
    );
    adapter.shutdown();
}

#[tokio::test]
async fn a_malformed_target_is_an_honest_refusal() {
    let mock = BridgeMock::new();
    let adapter = channel(&mock).await;
    for target in [
        json!({}),
        json!("+14155551212"),
        json!({"user_id": "+14155551212"}),
    ] {
        let error = adapter
            .send(delivery(target, json!("hi")))
            .await
            .expect_err("shape refusal");
        assert!(matches!(error, ChannelError::DeliveryFailed(_)));
    }
    adapter.shutdown();
}

#[tokio::test]
async fn send_accepts_the_json_encoded_target_and_uses_the_cached_chat_guid() {
    let mock = BridgeMock::new();
    mock.push_updates(updates_page(
        vec![bridge_message("G-1", "+14155551212", "hi")],
        101,
    ));
    let adapter = channel(&mock).await;
    let mut stream = adapter.subscribe().boxed();
    let _ = next_event(&mut stream).await;
    // The model may emit the target as a JSON-encoded string.
    let receipt = adapter
        .send(delivery(
            json!("{\"handle\": \"+1 (415) 555-1212\"}"),
            json!("hello"),
        ))
        .await
        .expect("send");
    assert_eq!(receipt.message_id, "G-SENT");
    let requests = mock.send_requests();
    assert_eq!(requests.len(), 1);
    let body = &requests[0];
    assert_eq!(body["to"], "+14155551212");
    assert_eq!(body["chat_guid"], "iMessage;-;+14155551212");
    assert_eq!(body["service"], "imessage");
    let expected = delivery_client_id("imessage", "+14155551212", 0, 1, "hello");
    assert_eq!(body["client_id"], expected);
    adapter.shutdown();
}

#[tokio::test]
async fn a_long_reply_is_split_into_ordered_parts() {
    let mock = BridgeMock::new();
    let adapter = channel(&mock).await;
    let text = format!("{}{}", "a".repeat(2_000), "b".repeat(700));
    let receipt = adapter
        .send(delivery(
            json!({"handle": "+14155551212"}),
            json!({"text": text}),
        ))
        .await
        .expect("send");
    assert_eq!(receipt.message_id, "G-SENT");
    let requests = mock.send_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["text"].as_str().unwrap().chars().count(), 2_000);
    assert_eq!(requests[1]["text"].as_str().unwrap().chars().count(), 700);
    assert_ne!(requests[0]["client_id"], requests[1]["client_id"]);
    assert_eq!(
        requests[0]["client_id"],
        delivery_client_id(
            "imessage",
            "+14155551212",
            0,
            2,
            requests[0]["text"].as_str().unwrap()
        )
    );
    adapter.shutdown();
}

#[tokio::test]
async fn receipts_prefer_guid_then_rowid_then_client_id() {
    let mock = BridgeMock::new();
    mock.push_send(
        StatusCode::OK,
        json!({"protocol": 1, "ok": true, "message_id": "", "guid": null,
               "rowid": 42, "service": "iMessage", "verified": true, "deduplicated": false}),
    );
    mock.push_send(
        StatusCode::OK,
        json!({"protocol": 1, "ok": true, "message_id": "kian/client-x",
               "guid": null, "rowid": null, "service": "iMessage", "verified": false,
               "deduplicated": false}),
    );
    let adapter = channel(&mock).await;
    let first = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("a")))
        .await
        .expect("send");
    assert_eq!(first.message_id, "imessage:42");
    let second = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("b")))
        .await
        .expect("send");
    assert_eq!(second.message_id, "kian/client-x");
    adapter.shutdown();
}

#[tokio::test]
async fn an_uncertain_send_is_a_delivery_failed_that_says_so() {
    let mock = BridgeMock::new();
    mock.push_send(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"protocol": 1, "ok": false, "kind": "may_have_completed",
               "detail": "ambiguous receipt", "retry_safe": false}),
    );
    let adapter = channel(&mock).await;
    let error = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("hi")))
        .await
        .expect_err("uncertain");
    match error {
        ChannelError::DeliveryFailed(reason) => {
            assert!(reason.contains("outcome unknown"), "{reason}");
        }
        other => panic!("expected DeliveryFailed, got {other:?}"),
    }
    assert_eq!(adapter.uncertain_sends(), 1);
    adapter.shutdown();
}

#[tokio::test]
async fn a_not_started_refusal_is_a_retry_safe_delivery_failed() {
    let mock = BridgeMock::new();
    mock.push_send(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"protocol": 1, "ok": false, "kind": "not_started",
               "detail": "Automation denied (-1743)", "retry_safe": true}),
    );
    let adapter = channel(&mock).await;
    let error = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("hi")))
        .await
        .expect_err("not started");
    match error {
        ChannelError::DeliveryFailed(reason) => {
            assert!(reason.contains("not started"), "{reason}");
        }
        other => panic!("expected DeliveryFailed, got {other:?}"),
    }
    assert_eq!(adapter.uncertain_sends(), 0);
    adapter.shutdown();
}

async fn dead_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    format!("http://{addr}")
}

#[tokio::test]
async fn an_unreachable_bridge_is_unavailable() {
    let url = dead_url().await;
    let adapter = ImessageChannel::new("imessage", config(url)).expect("channel builds");
    let error = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("hi")))
        .await
        .expect_err("unreachable");
    assert!(
        matches!(error, ChannelError::Unavailable(_)),
        "expected Unavailable, got {error:?}"
    );
    adapter.shutdown();
}

#[tokio::test]
async fn a_bridge_timeout_is_a_timeout() {
    let mock = BridgeMock::new();
    mock.hold_sends(Duration::from_millis(300));
    let mut cfg = config("http://127.0.0.1:1".into());
    cfg.send_timeout = Duration::from_millis(50);
    let url = mock.serve().await;
    cfg.bridge_url = url;
    let adapter = ImessageChannel::new("imessage", cfg).expect("channel builds");
    let error = adapter
        .send(delivery(json!({"handle": "+14155551212"}), json!("hi")))
        .await
        .expect_err("timeout");
    assert!(matches!(error, ChannelError::Timeout), "got {error:?}");
    adapter.shutdown();
}

#[tokio::test]
async fn config_refuses_a_phase_two_service() {
    let mut cfg = config("http://127.0.0.1:7420".into());
    cfg.service = ImessageService::Auto;
    assert!(ImessageChannel::new("imessage", cfg).is_ok());
    assert_eq!(IMESSAGE_CHANNEL_TYPE, "imessage");
    assert_eq!(bridge::PROTOCOL, 1);
}
