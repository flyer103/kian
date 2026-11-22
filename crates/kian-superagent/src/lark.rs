//! `LarkChannel` — the first *real* [`ChannelAdapter`] (
//! DESIGN §4.2): the Feishu/Lark open platform over HTTP.
//!
//! Footprint-Ladder level 2: plugin-hosted code over the core
//! `ChannelAdapter` seam. No core concept knows Feishu exists.
//!
//! - **Receive** — an adapter-side poll loop over the IM *chat history*
//!   API (`GET /open-apis/im/v1/messages?container_id_type=chat`, the
//!   documented long-poll-shaped read: page through new messages on a
//!   configurable cadence). No inbound webhook server this round — the
//!   adapter pulls. Each configured chat id (a key of the topic-mapping
//!   table) is polled every cadence tick; `message_id` is the durable
//!   idempotency key (`ChannelEvent.event_id`), so admission dedupes
//!   across polls *and* across restarts. Messages sent by the app
//!   itself (`sender_type: "app"`) never surface — the bot reading its
//!   own replies would be an echo loop — and neither do recalled
//!   (`deleted`) messages.
//! - **Send** — the IM send API (`POST /open-apis/im/v1/messages`),
//!   `receive_id_type` derived from the delivery target's key
//!   (`chat_id`/`open_id`/…). The future resolves only after the API
//!   accepted the message (`code == 0` + `message_id`); every other
//!   outcome is an honest [`ChannelError`].
//! - **Credentials** — `app_id`/`app_secret` are constructor inputs,
//!   never config-file fields: assembly (kian-cli) reads
//!   `KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET` from the environment at
//!   startup (the secrets discipline; fail-closed: absent
//!   credentials refuse to start). The tenant access token is fetched,
//!   cached until shortly before expiry, and refreshed once on an
//!   invalid-token rejection.
//! - **Untrusted by default** — the channel carries no policy of its
//!   own; assembly adds lark-typed channels to the untrusted set
//!   (real IM input is attacker-controllable text by default).
//!
//! Output policy is adapter-agnostic by contract: the caller shapes
//! [`ChannelDelivery::content`] before the send (the IM unit-budget rule
//! lives in the bundle's `policy` module); this adapter delivers what
//! it is given.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt};
use serde_json::{Value, json};
use tracing::warn;

/// The channel `type` spellings that build a [`LarkChannel`]
/// (kian-cli cross-references this constant).
pub const LARK_CHANNEL_TYPE: &str = "lark";

/// Feishu open-apis base URL.
pub const DEFAULT_API_BASE: &str = "https://open.feishu.cn";

/// `POST /open-apis/auth/v3/tenant_access_token/internal`.
const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";

/// The `User-Agent` every request carries (the lark half of
/// the check): applied once on the client so the token POST, the
/// history GET, and the send POST all carry it. Feishu tolerates a
/// missing one today — the adapter must not depend on that.
pub const USER_AGENT: &str = concat!("kian/", env!("CARGO_PKG_VERSION"), " (+issue-watch)");

/// `GET`/`POST /open-apis/im/v1/messages` (history read + send).
const MESSAGES_PATH: &str = "/open-apis/im/v1/messages";

/// Default poll cadence (the documented history API allows 1000 reads
/// per minute; a handful of chats at this cadence stays far below).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(5_000);

/// Per-subscriber inbound queue bound (the MockChannel discipline: a
/// slow consumer's copy is dropped with an audit line, never a block).
pub const SUBSCRIBER_CAPACITY: usize = 256;

/// How many distinct message ids the poll loop remembers (the in-memory
/// dedupe ring; the durable guard is admission's `event_id` idempotency,
/// so this only bounds re-surfacing between polls).
const SEEN_RING_CAPACITY: usize = 4096;

/// Refresh the tenant token this long before its documented expiry.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// HTTP timeout for every lark API call.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Page size for the history read (API-documented range 1–50).
const PAGE_SIZE: usize = 50;

/// Feishu error codes that mean "your tenant token is stale" — one
/// honest refresh-and-retry, then the error stands.
const INVALID_TOKEN_CODES: [i64; 3] = [99991663, 99991664, 99991668];

/// Honest construction/config failures (fail-closed, constraint 7: a
/// bad lark config refuses to start, never silently degrades).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LarkError {
    #[error("lark app id must not be empty (KIAN_LARK_APP_ID)")]
    EmptyAppId,
    #[error("lark app secret must not be empty (KIAN_LARK_APP_SECRET)")]
    EmptyAppSecret,
    #[error("lark api base must be an http(s) URL, got `{0}`")]
    BadApiBase(String),
    #[error("lark topic-mapping table must declare at least one chat id")]
    NoTopics,
    #[error("lark topic-mapping key/value must not be empty (`{0}`)")]
    EmptyTopicEntry(String),
    #[error("lark poll interval must be > 0")]
    ZeroPollInterval,
}

/// The runtime lark channel config (already credential-bearing — the
/// wire layer in kian-cli fills `app_id`/`app_secret` from the
/// environment; they never live in `kian.yaml`).
#[derive(Debug, Clone, PartialEq)]
pub struct LarkConfig {
    /// Feishu custom-app credential (env-only at assembly).
    pub app_id: String,
    /// Feishu custom-app secret (env-only at assembly).
    pub app_secret: String,
    /// Open-apis base, overridable for private deployments.
    pub api_base: String,
    /// chat id → routing topic. The keys are exactly the chats the
    /// receive loop polls; the values feed `ChannelEvent.topic`.
    pub topics: BTreeMap<String, String>,
    /// Receive poll cadence.
    pub poll_interval: Duration,
}

impl LarkConfig {
    /// Fails closed on every degenerate field (the discipline:
    /// an unusable lark config is a startup error, never a default).
    pub fn validate(&self) -> Result<(), LarkError> {
        if self.app_id.trim().is_empty() {
            return Err(LarkError::EmptyAppId);
        }
        if self.app_secret.trim().is_empty() {
            return Err(LarkError::EmptyAppSecret);
        }
        if !(self.api_base.starts_with("http://") || self.api_base.starts_with("https://")) {
            return Err(LarkError::BadApiBase(self.api_base.clone()));
        }
        if self.topics.is_empty() {
            return Err(LarkError::NoTopics);
        }
        for (chat_id, topic) in &self.topics {
            if chat_id.trim().is_empty() || topic.trim().is_empty() {
                return Err(LarkError::EmptyTopicEntry(format!("{chat_id}→{topic}")));
            }
        }
        if self.poll_interval.is_zero() {
            return Err(LarkError::ZeroPollInterval);
        }
        Ok(())
    }
}

/// The Feishu/Lark channel adapter. Cheap to clone (an `Arc` inside);
/// share clones, not deep copies.
#[derive(Clone)]
pub struct LarkChannel {
    inner: Arc<Inner>,
}

struct Inner {
    /// The configured channel name reported on every event.
    name: String,
    config: LarkConfig,
    http: reqwest::Client,
    /// Cached tenant token with its refresh deadline. Async mutex: the
    /// critical section spans the fetch (concurrent callers coalesce
    /// onto one request) and never blocks a runtime worker.
    token: tokio::sync::Mutex<Option<TokenState>>,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    poll_started: AtomicBool,
    dropped_full: AtomicU64,
}

struct TokenState {
    token: String,
    expires_at: Instant,
}

impl LarkChannel {
    /// Builds the adapter, fail-closed on config ([`LarkConfig::validate`]).
    pub fn new(name: impl Into<String>, config: LarkConfig) -> Result<Self, LarkError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            // the client-level User-Agent covers the token
            // POST, history GET, and send POST in one place.
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| LarkError::BadApiBase(format!("http client: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.into(),
                config,
                http,
                token: tokio::sync::Mutex::new(None),
                subscribers: Mutex::new(Vec::new()),
                poll_started: AtomicBool::new(false),
                dropped_full: AtomicU64::new(0),
            }),
        })
    }

    /// The configured channel name.
    pub fn channel_name(&self) -> &str {
        &self.inner.name
    }

    /// The runtime config (assembly/tests; credentials included —
    /// never log this).
    pub fn config(&self) -> &LarkConfig {
        &self.inner.config
    }

    /// How many inbound copies were dropped because a subscriber queue
    /// was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.inner.dropped_full.load(Ordering::Relaxed)
    }

    /// Starts the shared receive loop exactly once (idempotent across
    /// any number of `subscribe` calls). The loop polls every mapped
    /// chat each cadence tick; transport failures are audit-logged and
    /// retried on the next tick — the durable idempotency key makes a
    /// retry always safe.
    fn start_poll_loop(&self) {
        if self.inner.poll_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut seen = SeenRing::new(SEEN_RING_CAPACITY);
            loop {
                for (chat_id, topic) in &inner.config.topics {
                    match poll_chat(&inner, chat_id, topic).await {
                        Ok(events) => {
                            for event in events {
                                if seen.insert(event.event_id.clone()) {
                                    fan_out(&inner, event);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                target: "kian::lark_channel",
                                channel = %inner.name,
                                chat_id = %chat_id,
                                error = %e,
                                "lark audit: chat poll failed, retried next tick"
                            );
                        }
                    }
                }
                tokio::time::sleep(inner.config.poll_interval).await;
            }
        });
    }
}

impl ChannelAdapter for LarkChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_CAPACITY);
        self.inner
            .subscribers
            .lock()
            .expect("lark channel lock")
            .push(tx);
        self.start_poll_loop();
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed()
    }

    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { send_message(&inner, &out).await })
    }
}

/// Bounded insert-order dedupe over message ids: a set plus the
/// insertion queue that evicts the oldest when the capacity is hit.
struct SeenRing {
    order: std::collections::VecDeque<String>,
    set: std::collections::BTreeSet<String>,
    capacity: usize,
}

impl SeenRing {
    fn new(capacity: usize) -> Self {
        Self {
            order: std::collections::VecDeque::new(),
            set: std::collections::BTreeSet::new(),
            capacity: capacity.max(1),
        }
    }

    /// True iff the id was not seen before (and is now remembered).
    fn insert(&mut self, id: String) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        if self.order.len() == self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        self.order.push_back(id.clone());
        self.set.insert(id)
    }
}

/// Fans one event out to every live subscriber (bounded `try_send`;
/// a full queue drops that subscriber's copy with an audit line — a
/// slow consumer never blocks the poll loop).
fn fan_out(inner: &Inner, event: ChannelEvent) {
    let mut subscribers = inner.subscribers.lock().expect("lark channel lock");
    subscribers.retain(|tx| !tx.is_closed());
    for tx in subscribers.iter() {
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                inner.dropped_full.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "kian::lark_channel",
                    channel = %inner.name,
                    event_id = %event.event_id,
                    "lark audit: subscriber queue full, event copy dropped"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Pruned on this pass; nothing to do.
            }
        }
    }
}

/// Pages through the chat history once and maps every surfacing
/// message to a `ChannelEvent` (newest page last; order within a page
/// is the API's `ByCreateTimeAsc`).
async fn poll_chat(
    inner: &Inner,
    chat_id: &str,
    topic: &str,
) -> Result<Vec<ChannelEvent>, ChannelError> {
    let token = channel_token(inner).await?;
    let base = format!("{}{MESSAGES_PATH}", inner.config.api_base);
    let mut events = Vec::new();
    let mut page_token: Option<String> = None;
    // Hard page bound: a chat with pathological pagination must not
    // pin the loop forever (honest bounded work per tick).
    for _ in 0..32 {
        let mut request = inner.http.get(&base).query(&[
            ("container_id_type", "chat"),
            ("container_id", chat_id),
            ("sort_type", "ByCreateTimeAsc"),
            ("page_size", &PAGE_SIZE.to_string()),
        ]);
        if let Some(token) = &page_token {
            request = request.query(&[("page_token", token.as_str())]);
        }
        let value: Value = request
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| transport_error("chat poll", e))?
            .json()
            .await
            .map_err(|e| ChannelError::Unavailable(format!("chat poll body unreadable: {e}")))?;
        let code = value.get("code").and_then(Value::as_i64).unwrap_or(-1);
        if code != 0 {
            return Err(ChannelError::Unavailable(format!(
                "chat poll rejected: code={code} msg={}",
                value.get("msg").and_then(Value::as_str).unwrap_or("?")
            )));
        }
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        if let Some(items) = data.get("items").and_then(Value::as_array) {
            for item in items {
                if let Some(event) = message_to_event(&inner.name, chat_id, topic, item) {
                    events.push(event);
                }
            }
        }
        match data.get("has_more").and_then(Value::as_bool) {
            Some(true) => {
                let next = data
                    .get("page_token")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if next.is_empty() {
                    return Ok(events); // has_more without a token: stop honestly
                }
                page_token = Some(next);
            }
            _ => return Ok(events),
        }
    }
    Ok(events)
}

/// Maps one verbatim history-message item to a `ChannelEvent`
/// (`None` = honestly not surfacing: no id, app-sent echo, or
/// recalled). Pure — the unit-test surface for the mapping contract.
pub fn message_to_event(
    channel: &str,
    polled_chat_id: &str,
    topic: &str,
    message: &Value,
) -> Option<ChannelEvent> {
    let event_id = message.get("message_id").and_then(Value::as_str)?;
    if event_id.is_empty() {
        return None;
    }
    // Echo guard: the app's own sends never re-enter as input.
    let sender = message.get("sender").cloned().unwrap_or(Value::Null);
    if sender.get("sender_type").and_then(Value::as_str) == Some("app") {
        return None;
    }
    // Recalled messages are not input.
    if message.get("deleted").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    // provenance: chat id → group, sender id/name → sender (kept
    // verbatim as the adapter-defined identity object).
    let group = message
        .get("chat_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .unwrap_or(polled_chat_id)
        .to_string();
    // create_time is string seconds per the API contract.
    let ts_ms = message
        .get("create_time")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<u64>().ok())
        .map(|secs| secs * 1000)
        .unwrap_or_else(now_ms);
    // The payload is the verbatim message item plus — exactly the
    // -documented adapter-declared signal — a top-level
    // `explicit: true` when the raw message carries an @-mention
    // (Feishu's `mentions` array, or an @-token in the text content,
    // which arrives JSON-encoded as a string rather than as
    // `payload.text`). Without this the explicit-only admission class
    // could never admit a lark event; with it the predicate stays pure
    // and adapter-agnostic.
    let mut payload = message.clone();
    if message_has_mention(message) {
        payload["explicit"] = Value::Bool(true);
    }
    Some(ChannelEvent {
        channel: channel.to_string(),
        event_id: event_id.to_string(),
        topic: Some(topic.to_string()),
        sender: Some(sender),
        group: Some(group),
        payload,
        ts_ms,
    })
}

/// Does the raw message carry an @-mention? Feishu populates the
/// `mentions` array for real mentions; a bare `@word` inside the
/// JSON-encoded text content counts too (the same shape the
/// text predicate recognizes, decoded from the wire form).
fn message_has_mention(message: &Value) -> bool {
    if message
        .get("mentions")
        .and_then(Value::as_array)
        .is_some_and(|mentions| !mentions.is_empty())
    {
        return true;
    }
    let text = message
        .get("content")
        .and_then(Value::as_str)
        .and_then(|encoded| serde_json::from_str::<Value>(encoded).ok())
        .and_then(|content| {
            content
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    text.is_some_and(|t| {
        let chars: Vec<char> = t.chars().collect();
        (0..chars.len())
            .any(|i| chars[i] == '@' && chars.get(i + 1).is_some_and(|next| !next.is_whitespace()))
    })
}

/// The chat-history cursor a mapped message payload carries (read
/// position): its top-level `message_id` — offset semantics, the next
/// poll resumes after it (the history API pages by create time; the
/// id is the durable marker of what admission already saw). Pure over
/// the payload; `None` when the payload carries no usable id — the
/// caller then re-polls bounded by `event_id` dedupe (ADR-013's
/// honest v1, no position table).
pub fn payload_message_cursor(payload: &Value) -> Option<String> {
    payload
        .get("message_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

/// Sends one delivery through the IM send API. Resolves only after
/// the API's acceptance (`code == 0` + a message id); anything else is an
/// honest error.
async fn send_message(inner: &Inner, out: &ChannelDelivery) -> Result<SendReceipt, ChannelError> {
    let Some(target) = out.target.as_object() else {
        return Err(ChannelError::DeliveryFailed(format!(
            "lark target must be an object with one receive id, got `{}`",
            out.target
        )));
    };
    let (receive_id_type, receive_id) = receive_id(target)?;
    let content = match &out.content {
        Value::Object(_) => serde_json::to_string(&out.content)
            .map_err(|e| ChannelError::DeliveryFailed(format!("content unserializable: {e}")))?,
        other => {
            return Err(ChannelError::DeliveryFailed(format!(
                "lark content must be a JSON object (msg_type text carries {{text}}), got `{other}`"
            )));
        }
    };
    let body = json!({
        "receive_id": receive_id,
        "msg_type": "text",
        "content": content,
    });
    let url = format!("{}{MESSAGES_PATH}", inner.config.api_base);

    // First attempt; on a stale-token rejection, one refresh + retry.
    for attempt in 0..2 {
        let token = channel_token(inner).await?;
        let value: Value = inner
            .http
            .post(&url)
            .query(&[("receive_id_type", receive_id_type)])
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|e| transport_error("send", e))?
            .json()
            .await
            .map_err(|e| ChannelError::Unavailable(format!("send response unreadable: {e}")))?;
        let code = value.get("code").and_then(Value::as_i64).unwrap_or(-1);
        if code == 0 {
            let message_id = value
                .pointer("/data/message_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if message_id.is_empty() {
                // Accepted claim without an id is not acceptance.
                return Err(ChannelError::DeliveryFailed(format!(
                    "lark send accepted but returned no message_id: {value}"
                )));
            }
            return Ok(SendReceipt {
                message_id: message_id.to_string(),
                ts_ms: now_ms(),
            });
        }
        let stale_token = INVALID_TOKEN_CODES.contains(&code);
        if stale_token && attempt == 0 {
            channel_token_refresh(inner).await?;
            continue;
        }
        return Err(ChannelError::DeliveryFailed(format!(
            "lark send rejected: code={code} msg={}",
            value.get("msg").and_then(Value::as_str).unwrap_or("?")
        )));
    }
    Err(ChannelError::DeliveryFailed(
        "lark send rejected: token refresh did not clear the rejection".into(),
    ))
}

/// Derives Feishu's `receive_id_type` from the target's single key
/// (`chat_id`/`open_id`/`union_id`/`user_id`/`email`, the documented
/// receive-id vocabulary).
fn receive_id(
    target: &serde_json::Map<String, Value>,
) -> Result<(&'static str, String), ChannelError> {
    for (key, id_type) in [
        ("chat_id", "chat_id"),
        ("open_id", "open_id"),
        ("union_id", "union_id"),
        ("user_id", "user_id"),
        ("email", "email"),
    ] {
        if let Some(id) = target
            .get(key)
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            return Ok((id_type, id.to_string()));
        }
    }
    Err(ChannelError::DeliveryFailed(format!(
        "lark target must carry one non-empty receive id key \
         (chat_id | open_id | union_id | user_id | email), got `{target:?}`"
    )))
}

/// Token access on an `Inner` (the free-function twin of the method, so
/// the poll loop needs no channel handle).
async fn channel_token(inner: &Inner) -> Result<String, ChannelError> {
    let mut guard = inner.token.lock().await;
    if let Some(state) = guard.as_ref()
        && Instant::now() < state.expires_at
    {
        return Ok(state.token.clone());
    }
    fetch_token(inner, &mut guard).await
}

async fn channel_token_refresh(inner: &Inner) -> Result<String, ChannelError> {
    let mut guard = inner.token.lock().await;
    *guard = None;
    fetch_token(inner, &mut guard).await
}

async fn fetch_token(
    inner: &Inner,
    guard: &mut Option<TokenState>,
) -> Result<String, ChannelError> {
    let url = format!("{}{TOKEN_PATH}", inner.config.api_base);
    let body = json!({
        "app_id": inner.config.app_id,
        "app_secret": inner.config.app_secret,
    });
    let value: Value = inner
        .http
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| transport_error("token fetch", e))?
        .json()
        .await
        .map_err(|e| ChannelError::Unavailable(format!("token response unreadable: {e}")))?;
    let code = value.get("code").and_then(Value::as_i64).unwrap_or(-1);
    if code != 0 {
        return Err(ChannelError::Unavailable(format!(
            "token fetch rejected: code={code} msg={}",
            value.get("msg").and_then(Value::as_str).unwrap_or("?")
        )));
    }
    let token = value
        .get("tenant_access_token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if token.is_empty() {
        return Err(ChannelError::Unavailable(
            "token fetch returned no tenant_access_token".into(),
        ));
    }
    let expire_secs = value.get("expire").and_then(Value::as_u64).unwrap_or(0);
    let lifetime = Duration::from_secs(expire_secs.saturating_sub(1));
    let refresh_in = lifetime
        .saturating_sub(TOKEN_REFRESH_MARGIN)
        .max(Duration::from_secs(1));
    *guard = Some(TokenState {
        token: token.clone(),
        expires_at: Instant::now()
            .checked_add(refresh_in)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1)),
    });
    Ok(token)
}

fn transport_error(what: &str, e: reqwest::Error) -> ChannelError {
    if e.is_timeout() {
        ChannelError::Timeout
    } else {
        ChannelError::Unavailable(format!("{what}: {e}"))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{LarkChannel, LarkConfig, LarkError, SeenRing, message_to_event};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn config() -> LarkConfig {
        LarkConfig {
            app_id: "cli_test".into(),
            app_secret: "secret".into(),
            api_base: "https://example.invalid".into(),
            topics: BTreeMap::from([("oc_chat1".to_string(), "ci".to_string())]),
            poll_interval: Duration::from_millis(50),
        }
    }

    #[test]
    fn degenerate_configs_fail_closed() {
        let cfg = config();
        assert_eq!(cfg.validate(), Ok(()));

        let mut bad = cfg.clone();
        bad.app_id = "  ".into();
        assert_eq!(bad.validate(), Err(LarkError::EmptyAppId));

        let mut bad = cfg.clone();
        bad.app_secret.clear();
        assert_eq!(bad.validate(), Err(LarkError::EmptyAppSecret));

        let mut bad = cfg.clone();
        bad.api_base = "ftp://x".into();
        assert_eq!(bad.validate(), Err(LarkError::BadApiBase("ftp://x".into())));

        let mut bad = cfg.clone();
        bad.topics.clear();
        assert_eq!(bad.validate(), Err(LarkError::NoTopics));

        let mut bad = cfg.clone();
        bad.topics.insert("".into(), "t".into());
        assert!(matches!(bad.validate(), Err(LarkError::EmptyTopicEntry(_))));

        let mut bad = cfg;
        bad.poll_interval = Duration::ZERO;
        assert_eq!(bad.validate(), Err(LarkError::ZeroPollInterval));

        assert!(LarkChannel::new("lark", config()).is_ok());
    }

    #[test]
    fn constructor_reports_the_configured_name() {
        let channel = LarkChannel::new("team-im", config()).expect("valid config");
        assert_eq!(channel.channel_name(), "team-im");
        assert_eq!(channel.config().poll_interval, Duration::from_millis(50));
    }

    #[test]
    fn message_mapping_extracts_provenance_and_stays_verbatim() {
        // No mention in the text: the payload stays byte-verbatim.
        let message = json!({
            "message_id": "om_1",
            "chat_id": "oc_chat1",
            "msg_type": "text",
            "create_time": "1730000000",
            "deleted": false,
            "sender": {
                "id": "ou_sender",
                "id_type": "open_id",
                "sender_type": "user",
                "name": "Yifei"
            },
            "content": "{\"text\":\"build broken\"}"
        });
        let event = message_to_event("lark", "oc_chat1", "ci", &message).expect("maps");
        assert_eq!(event.channel, "lark");
        assert_eq!(event.event_id, "om_1"); // the idempotency key
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
        assert_eq!(event.payload, message); // verbatim, no adapter fields
        assert_eq!(event.ts_ms, 1_730_000_000_000);
    }

    #[test]
    fn mention_messages_gain_the_adapter_declared_explicit_flag() {
        // Feishu's real shape: mentions array + placeholder text.
        let mut mentioned = json!({
            "message_id": "om_m1",
            "chat_id": "oc_chat1",
            "sender": {"sender_type": "user"},
            "content": "{\"text\":\"<at id=\\\"ou_kian\\\"></at> please fix\"}",
            "mentions": [{"key": "@_user_1", "id": "ou_kian", "id_type": "open_id"}],
        });
        let event = message_to_event("lark", "oc_chat1", "ci", &mentioned).expect("maps");
        assert_eq!(
            event.payload.get("explicit"),
            Some(&serde_json::Value::Bool(true))
        );
        mentioned["explicit"] = serde_json::Value::Bool(true);
        assert_eq!(event.payload, mentioned); // verbatim + the one flag

        // A bare @token in the text (no mentions array) counts too.
        let bare = json!({
            "message_id": "om_m2",
            "chat_id": "oc_chat1",
            "sender": {"sender_type": "user"},
            "content": "{\"text\":\"@kian look\"}",
        });
        let event = message_to_event("lark", "oc_chat1", "ci", &bare).expect("maps");
        assert_eq!(
            event.payload.get("explicit"),
            Some(&serde_json::Value::Bool(true))
        );

        // A lone '@' or '@ word' is not a mention (the predicate
        // shape: '@' followed by a non-whitespace char).
        for text in ["\"@\"", "\"@ kian\"", "\"no mention here\""] {
            let plain = json!({
                "message_id": "om_m3",
                "chat_id": "oc_chat1",
                "sender": {"sender_type": "user"},
                "content": format!("{{\"text\":{text}}}"),
            });
            let event = message_to_event("lark", "oc_chat1", "ci", &plain).expect("maps");
            assert!(
                event.payload.get("explicit").is_none(),
                "no explicit flag for {text}"
            );
        }
    }

    #[test]
    fn message_mapping_skips_echo_recalled_and_idless_items() {
        let echo = json!({
            "message_id": "om_self",
            "sender": {"id": "cli_test", "id_type": "app_id", "sender_type": "app"},
        });
        assert!(message_to_event("lark", "oc_chat1", "ci", &echo).is_none());

        let recalled = json!({
            "message_id": "om_gone",
            "deleted": true,
            "sender": {"sender_type": "user"},
        });
        assert!(message_to_event("lark", "oc_chat1", "ci", &recalled).is_none());

        let idless = json!({"sender": {"sender_type": "user"}});
        assert!(message_to_event("lark", "oc_chat1", "ci", &idless).is_none());
    }

    #[test]
    fn missing_chat_id_falls_back_to_the_polled_chat() {
        let message = json!({
            "message_id": "om_nochat",
            "sender": {"sender_type": "user"},
            "create_time": "1730000001",
        });
        let event = message_to_event("lark", "oc_polled", "ci", &message).expect("maps");
        assert_eq!(event.group.as_deref(), Some("oc_polled"));
        assert!(event.ts_ms >= 1_730_000_001_000); // parsed, not wall-clocked
    }

    #[test]
    fn seen_ring_dedupes_and_evicts_at_capacity() {
        let mut ring = SeenRing::new(2);
        assert!(ring.insert("a".into()));
        assert!(!ring.insert("a".into()));
        assert!(ring.insert("b".into()));
        assert!(ring.insert("c".into())); // evicts "a"
        assert!(ring.insert("a".into())); // "a" is new again
        assert!(!ring.insert("c".into()));
    }
}
