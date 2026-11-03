//! C44 lark-channel integration tests: a scripted in-process HTTP mock
//! serving canned Feishu open-API responses (token, chat history, send)
//! — zero credentials, zero network. The live smoke test at the bottom
//! is env-gated (`KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET`), `#[ignore]`d
//! out of CI, and recorded in README.md's live-run log.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::{ChannelAdapter, ChannelDelivery};
use kian_orchestrator::WakeOutcome;
use kian_superagent::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
use kian_superagent::rules::{CorrelateBy, Route, RuleMatcher};
use kian_superagent::{
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, LarkChannel, LarkConfig,
    SubscriptionRule, SubscriptionRuleSet, SubscriptionService,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The scripted Feishu mock: an in-process axum server with mutable
// canned state (no credentials — the "token" is a fixture string).
// ---------------------------------------------------------------------------

struct FeishuMock {
    /// Chat id → messages served by the history endpoint (served
    /// verbatim every poll until the test mutates them — history, not
    /// a queue, exactly like the real API).
    chats: Mutex<BTreeMap<String, Vec<Value>>>,
    /// What the send endpoint "accepted": (receive_id_type, body).
    sent: Mutex<Vec<(String, Value)>>,
    /// The (code, msg) the send endpoint rejects with, when set.
    send_reject: Mutex<Option<(i64, String)>>,
    /// The (code, msg) the token endpoint rejects with, when set.
    token_reject: Mutex<Option<(i64, String)>>,
    token_calls: Mutex<u64>,
}

impl FeishuMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            chats: Mutex::new(BTreeMap::new()),
            sent: Mutex::new(Vec::new()),
            send_reject: Mutex::new(None),
            token_reject: Mutex::new(None),
            token_calls: Mutex::new(0),
        })
    }

    fn put_message(&self, chat_id: &str, message: Value) {
        self.chats
            .lock()
            .unwrap()
            .entry(chat_id.to_string())
            .or_default()
            .push(message);
    }

    fn reject_sends_with(&self, code: i64, msg: &str) {
        *self.send_reject.lock().unwrap() = Some((code, msg.to_string()));
    }

    fn reject_tokens_with(&self, code: i64, msg: &str) {
        *self.token_reject.lock().unwrap() = Some((code, msg.to_string()));
    }

    fn token_calls(&self) -> u64 {
        *self.token_calls.lock().unwrap()
    }

    async fn serve(self: Arc<Self>) -> String {
        let app = axum::Router::new()
            .route(
                "/open-apis/auth/v3/tenant_access_token/internal",
                axum::routing::post(token_endpoint),
            )
            .route(
                "/open-apis/im/v1/messages",
                axum::routing::get(history_endpoint).post(send_endpoint),
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

async fn token_endpoint(State(state): State<Arc<FeishuMock>>) -> Json<Value> {
    *state.token_calls.lock().unwrap() += 1;
    if let Some((code, msg)) = state.token_reject.lock().unwrap().clone() {
        return Json(json!({"code": code, "msg": msg}));
    }
    Json(json!({
        "code": 0,
        "msg": "ok",
        "tenant_access_token": "t-mock",
        "expire": 7200,
    }))
}

async fn history_endpoint(
    State(state): State<Arc<FeishuMock>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let chat_id = query.get("container_id").cloned().unwrap_or_default();
    let items = state
        .chats
        .lock()
        .unwrap()
        .get(&chat_id)
        .cloned()
        .unwrap_or_default();
    Json(json!({
        "code": 0,
        "msg": "ok",
        "data": {"items": items, "has_more": false},
    }))
}

async fn send_endpoint(
    State(state): State<Arc<FeishuMock>>,
    Query(query): Query<HashMap<String, String>>,
    body: Json<Value>,
) -> Json<Value> {
    if let Some((code, msg)) = state.send_reject.lock().unwrap().clone() {
        return Json(json!({"code": code, "msg": msg}));
    }
    let receive_id_type = query.get("receive_id_type").cloned().unwrap_or_default();
    state.sent.lock().unwrap().push((receive_id_type, body.0));
    Json(json!({
        "code": 0,
        "msg": "ok",
        "data": {"message_id": "om_sent_1"},
    }))
}

fn user_message(message_id: &str, chat_id: &str, text: &str) -> Value {
    json!({
        "message_id": message_id,
        "chat_id": chat_id,
        "msg_type": "text",
        "create_time": "1730000000",
        "deleted": false,
        "sender": {
            "id": "ou_sender",
            "id_type": "open_id",
            "sender_type": "user",
            "name": "Yifei"
        },
        "content": json!({"text": text}).to_string(),
    })
}

fn lark_config(base: &str, topics: &[(&str, &str)]) -> LarkConfig {
    LarkConfig {
        app_id: "cli_mock".into(),
        app_secret: "mock-secret".into(),
        api_base: base.to_string(),
        topics: topics
            .iter()
            .map(|(c, t)| ((*c).to_string(), (*t).to_string()))
            .collect(),
        poll_interval: Duration::from_millis(20),
    }
}

/// The macOS system proxy intercepts localhost (AGENTS.md gotcha).
fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }
}

/// Awaits `f` for at most `secs` — polls must be observed promptly,
/// never with unbounded wall-clock waits.
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
// Receive → ChannelEvent mapping (scripted mock).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn receive_maps_feishu_history_to_channel_events() {
    force_no_proxy();
    let mock = FeishuMock::new();
    mock.put_message("oc_chat1", user_message("om_1", "oc_chat1", "build broken"));
    // An unmapped second chat is never polled (its messages never surface).
    mock.put_message("oc_other", user_message("om_x", "oc_other", "noise"));
    let base = mock.clone().serve().await;

    let channel = LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
        .expect("valid lark config");
    let mut stream = channel.subscribe();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event within 5s")
        .expect("stream open");

    // The full mapping contract: channel, idempotency key, topic from
    // the mapping table, chat id → group, sender object, verbatim
    // payload, API create_time → ts_ms.
    assert_eq!(event.channel, "lark");
    assert_eq!(event.event_id, "om_1");
    assert_eq!(event.topic.as_deref(), Some("ci"));
    assert_eq!(event.group.as_deref(), Some("oc_chat1"));
    assert_eq!(
        event.sender,
        Some(json!({
            "id": "ou_sender",
            "id_type": "open_id",
            "sender_type": "user",
            "name": "Yifei"
        }))
    );
    assert_eq!(
        event.payload.get("content").and_then(Value::as_str),
        Some("{\"text\":\"build broken\"}")
    );
    assert_eq!(event.ts_ms, 1_730_000_000_000);

    // The unmapped chat's message never arrives.
    let none = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    assert!(none.is_err() || none.unwrap().is_none());
}

#[tokio::test]
async fn event_id_idempotency_across_polls_and_subscribers() {
    force_no_proxy();
    let mock = FeishuMock::new();
    // The same message id stays in the history across polls (Feishu
    // serves history, not a queue) — it must surface exactly once.
    mock.put_message("oc_chat1", user_message("om_dup", "oc_chat1", "again"));
    let base = mock.clone().serve().await;

    let channel = LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
        .expect("valid lark config");
    let mut s1 = channel.subscribe();
    let mut s2 = channel.subscribe();

    let first = tokio::time::timeout(Duration::from_secs(5), s1.next())
        .await
        .expect("first within 5s")
        .expect("s1 open");
    assert_eq!(first.event_id, "om_dup");
    let second = tokio::time::timeout(Duration::from_secs(5), s2.next())
        .await
        .expect("second within 5s")
        .expect("s2 open");
    assert_eq!(second.event_id, "om_dup");

    // Both streams now stay quiet across further poll ticks: the
    // adapter-side dedupe (and admission's durable event_id guard
    // downstream) means one row per event, never two.
    for stream in [&mut s1, &mut s2] {
        let none = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
        assert!(
            none.is_err() || none.unwrap().is_none(),
            "the same message id must not resurface on later polls"
        );
    }

    // A genuinely new message id surfaces on the next tick.
    mock.put_message("oc_chat1", user_message("om_new", "oc_chat1", "fresh"));
    let fresh = tokio::time::timeout(Duration::from_secs(5), s1.next())
        .await
        .expect("fresh within 5s")
        .expect("s1 open");
    assert_eq!(fresh.event_id, "om_new");
}

#[tokio::test]
async fn app_sent_and_recalled_messages_never_surface() {
    force_no_proxy();
    let mock = FeishuMock::new();
    mock.put_message("oc_chat1", user_message("om_ok", "oc_chat1", "hello"));
    mock.put_message(
        "oc_chat1",
        json!({
            "message_id": "om_self",
            "chat_id": "oc_chat1",
            "sender": {"id": "cli_mock", "id_type": "app_id", "sender_type": "app"},
            "content": "{\"text\":\"the bot's own reply\"}",
        }),
    );
    mock.put_message(
        "oc_chat1",
        json!({
            "message_id": "om_gone",
            "chat_id": "oc_chat1",
            "deleted": true,
            "sender": {"sender_type": "user"},
        }),
    );
    let base = mock.clone().serve().await;

    let channel = LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
        .expect("valid lark config");
    let mut stream = channel.subscribe();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("an event within 5s")
        .expect("stream open");
    // The only surfacing message is the genuine user one.
    assert_eq!(event.event_id, "om_ok");
    let none = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
    assert!(none.is_err() || none.unwrap().is_none());
}

#[tokio::test]
async fn poll_failures_are_honest_but_not_fatal() {
    force_no_proxy();
    let mock = FeishuMock::new();
    // The token endpoint rejects everything: the poll loop must log
    // and keep trying (a later recovery picks the messages up).
    mock.reject_tokens_with(99991663, "token stale");
    mock.put_message("oc_chat1", user_message("om_1", "oc_chat1", "hi"));
    let base = mock.clone().serve().await;

    let channel = LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
        .expect("valid lark config");
    let mut stream = channel.subscribe();
    let none = tokio::time::timeout(Duration::from_millis(150), stream.next()).await;
    assert!(none.is_err() || none.unwrap().is_none());
    assert!(mock.token_calls() >= 1, "the poll loop did try the token");

    // Recovery: the token endpoint heals and the message surfaces.
    *mock.token_reject.lock().unwrap() = None;
    let healed = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event after token recovery")
        .expect("stream open");
    assert_eq!(healed.event_id, "om_1");
}

// ---------------------------------------------------------------------------
// Send (honest receipts only).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_resolves_only_after_the_api_accepts() {
    force_no_proxy();
    let mock = FeishuMock::new();
    let base = mock.clone().serve().await;
    let channel = LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
        .expect("valid lark config");

    // chat_id target → receive_id_type=chat_id; content passes through
    // as the policy-shaped text content object.
    let receipt = channel
        .send(ChannelDelivery {
            channel: "lark".into(),
            target: json!({"chat_id": "oc_chat1"}),
            content: json!({"text": "done in 3 words"}),
        })
        .await
        .expect("send accepted");
    assert_eq!(receipt.message_id, "om_sent_1");
    assert!(receipt.ts_ms > 0);
    let sent = mock.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "chat_id");
    assert_eq!(sent[0].1["receive_id"], "oc_chat1");
    assert_eq!(sent[0].1["msg_type"], "text");
    assert_eq!(
        sent[0].1["content"].as_str().unwrap(),
        "{\"text\":\"done in 3 words\"}"
    );

    // open_id target derives receive_id_type=open_id.
    let receipt = channel
        .send(ChannelDelivery {
            channel: "lark".into(),
            target: json!({"open_id": "ou_user"}),
            content: json!({"text": "direct"}),
        })
        .await
        .expect("send accepted");
    assert_eq!(receipt.message_id, "om_sent_1");
    assert_eq!(mock.sent.lock().unwrap()[1].0, "open_id");

    // Rejection → an honest error naming the API's code, never a
    // fabricated receipt.
    mock.reject_sends_with(230034, "receive_id is invalid");
    let err = channel
        .send(ChannelDelivery {
            channel: "lark".into(),
            target: json!({"chat_id": "oc_wrong"}),
            content: json!({"text": "x"}),
        })
        .await
        .expect_err("rejection must error");
    assert!(
        err.to_string().contains("230034"),
        "the honest error names the API code: {err}"
    );

    // Degenerate targets/contents are honest errors before any wire
    // traffic.
    for bad in [
        json!("oc_chat1"),
        json!({}),
        json!({"chat_id": ""}),
        json!({"unknown": "x"}),
    ] {
        let err = channel
            .send(ChannelDelivery {
                channel: "lark".into(),
                target: bad.clone(),
                content: json!({"text": "x"}),
            })
            .await
            .expect_err("bad target must error");
        assert!(matches!(
            err,
            kian_core::seams::ChannelError::DeliveryFailed(_)
        ));
    }
    let err = channel
        .send(ChannelDelivery {
            channel: "lark".into(),
            target: json!({"chat_id": "oc_chat1"}),
            content: json!("not an object"),
        })
        .await
        .expect_err("non-object content must error");
    assert!(matches!(
        err,
        kian_core::seams::ChannelError::DeliveryFailed(_)
    ));
    // No wire traffic was spent on the degenerate shapes.
    assert_eq!(mock.sent.lock().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// The full subscription path: classes + rules + provenance, with a
// real lark event flowing through the adapter.
// ---------------------------------------------------------------------------

/// The scripted inbox from the subscriptions tests: records every
/// admit (session, payload, idempotency key) and wake.
struct RecordingInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl RecordingInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

#[tokio::test]
async fn lark_event_flows_through_classes_rules_and_provenance_untrusted() {
    force_no_proxy();
    let mock = FeishuMock::new();
    // A mention-carrying message (the explicit-request signal) — the
    // adapter declares `explicit: true` from the @-token in the text.
    mock.put_message(
        "oc_chat1",
        user_message("om_1", "oc_chat1", "please @kian fix"),
    );
    // A second message with no mention: explicit-only must drop it
    // before any rule, any row, any wake.
    mock.put_message(
        "oc_chat1",
        user_message("om_2", "oc_chat1", "just chatting"),
    );
    let base = mock.clone().serve().await;

    let router = SessionId(uuid::Uuid::new_v4());
    let inbox = RecordingInbox::new();
    let service = Arc::new(
        SubscriptionService::with_classes(
            router,
            SubscriptionRuleSet::new(vec![SubscriptionRule {
                channel: "lark".into(),
                matcher: RuleMatcher::Topic("ci".into()),
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            }]),
            AdmissionClassTable::new(vec![AdmissionClassRule {
                channel: "lark".into(),
                topic: Some("ci".into()),
                sender: None,
                class: AdmissionClass::ExplicitOnly,
            }]),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        // C43/C44: lark is default-untrusted — assembly passes the
        // channel name in; the wrapper gains the bit.
        .with_untrusted_channels(BTreeSet::from(["lark".to_string()])),
    );

    let channel = Arc::new(
        LarkChannel::new("lark", lark_config(&base, &[("oc_chat1", "ci")]))
            .expect("valid lark config"),
    );
    let _consumers = service.spawn(vec![Arc::clone(&channel) as Arc<dyn ChannelAdapter>]);

    // Exactly one admit must land (the mention) — wait for it, then
    // give the second (mentionless) event a clear window to be wrongly
    // admitted before freezing the assertions.
    within(5, || {
        let admits = inbox.admits.lock().unwrap();
        (!admits.is_empty()).then(|| admits[0].clone())
    })
    .await
    .expect("one admit within 5s");
    tokio::time::sleep(Duration::from_millis(250)).await;

    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(
        admits.len(),
        1,
        "explicit-only dropped the mentionless message before any row: {admits:?}"
    );
    let (target, payload, key) = &admits[0];
    assert_eq!(*target, router); // empty-set passthrough fallback
    assert_eq!(key.as_deref(), Some("om_1")); // event_id idempotency key
    assert_eq!(inbox.wakes.lock().unwrap().len(), 1);

    // Provenance: the wrapper is the C33/C43 `{"kian":…}` channel-event
    // wrapper with the untrusted bit set, and the verbatim lark raw
    // (with the adapter-declared explicit flag) inside.
    let kian = &payload["kian"];
    assert_eq!(kian["type"], "channel_event");
    assert_eq!(kian["channel"], "lark");
    assert_eq!(kian["event_id"], "om_1");
    assert_eq!(kian["topic"], "ci");
    assert_eq!(kian["untrusted"], json!(true));
    assert_eq!(kian["raw"]["chat_id"], "oc_chat1");
    assert_eq!(kian["raw"]["sender"]["name"], "Yifei");
    assert_eq!(kian["raw"]["explicit"], json!(true));
}

// ---------------------------------------------------------------------------
// Live smoke — env-gated, CI-excluded, recorded in README.md.
// ---------------------------------------------------------------------------

/// One manual pass against the real Feishu open APIs. Skips (does not
/// fail) without credentials:
///
/// ```text
/// KIAN_LARK_APP_ID=cli_xxx KIAN_LARK_APP_SECRET=xxx \
/// KIAN_LARK_CHAT_ID=oc_xxx [KIAN_LARK_POLL_CHAT=oc_xxx:topic] \
/// cargo test -p kian-superagent --test lark_channel -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "env-gated: set KIAN_LARK_APP_ID / KIAN_LARK_APP_SECRET (and KIAN_LARK_CHAT_ID) and run with --ignored"]
async fn lark_live_smoke_env_gated() {
    let (Ok(app_id), Ok(app_secret)) = (
        std::env::var("KIAN_LARK_APP_ID"),
        std::env::var("KIAN_LARK_APP_SECRET"),
    ) else {
        eprintln!(
            "skipping: KIAN_LARK_APP_ID / KIAN_LARK_APP_SECRET not set (manual, recorded run)"
        );
        return;
    };
    let chat_id = std::env::var("KIAN_LARK_CHAT_ID").ok();
    let poll_chat = std::env::var("KIAN_LARK_POLL_CHAT").ok();

    let mut topics = BTreeMap::new();
    if let Some(spec) = poll_chat.as_ref() {
        // "oc_xxx:topic" — live receive is opt-in per chat.
        let (chat, topic) = spec
            .split_once(':')
            .expect("KIAN_LARK_POLL_CHAT=oc_x:topic");
        topics.insert(chat.to_string(), topic.to_string());
    }
    let channel = LarkChannel::new(
        "lark",
        LarkConfig {
            app_id,
            app_secret,
            api_base: kian_superagent::DEFAULT_API_BASE.into(),
            topics,
            poll_interval: Duration::from_secs(2),
        },
    )
    .expect("live config validates");

    if let Some(chat) = chat_id.as_deref() {
        let receipt = channel
            .send(ChannelDelivery {
                channel: "lark".into(),
                target: json!({"chat_id": chat}),
                content: json!({"text": "kian lark live smoke (C44)"}),
            })
            .await
            .expect("live send accepted");
        eprintln!("live send ok: message_id={}", receipt.message_id);
    } else {
        eprintln!("live send skipped: KIAN_LARK_CHAT_ID not set");
    }

    if poll_chat.is_some() {
        let mut stream = channel.subscribe();
        let event = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("a live event within 30s")
            .expect("stream open");
        eprintln!(
            "live receive ok: event_id={} topic={:?} group={:?}",
            event.event_id, event.topic, event.group
        );
    } else {
        eprintln!("live receive skipped: KIAN_LARK_POLL_CHAT not set");
    }
}

// ---------------------------------------------------------------------------
// C61: the chat-history cursor helper (read-position derivation, fixture
// payloads only)
// ---------------------------------------------------------------------------

/// `payload_message_cursor` reads the top-level `message_id` off a
/// mapped history item — the offset marker a restart's poll resumes
/// after. Missing or empty ids are honest `None`s (the caller re-polls
/// under `event_id` dedupe).
#[test]
fn payload_message_cursor_reads_the_message_id_offset() {
    use kian_superagent::payload_message_cursor;

    let message = user_message("om_8812", "oc_7f3a", "please review");
    assert_eq!(payload_message_cursor(&message).as_deref(), Some("om_8812"));

    // Missing id: honestly underivable.
    let mut no_id = user_message("om_x", "oc_1", "hi");
    no_id
        .as_object_mut()
        .unwrap()
        .remove("message_id")
        .expect("fixture carries the id");
    assert_eq!(payload_message_cursor(&no_id), None);

    // Empty id is not a cursor.
    let empty = json!({"message_id": "", "chat_id": "oc_1"});
    assert_eq!(payload_message_cursor(&empty), None);

    // A non-string id is not a cursor (never a guessed render).
    let numeric = json!({"message_id": 42, "chat_id": "oc_1"});
    assert_eq!(payload_message_cursor(&numeric), None);
}
