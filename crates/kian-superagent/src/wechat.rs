//! `WechatChannel` — the WeChat (Tencent ClawBot **iLink Bot API**)
//! channel adapter (CHANNEL-WECHAT, ADR-018).
//!
//! Footprint-Ladder level 2: plugin-hosted code over the core
//! `ChannelAdapter` seam; no core concept knows WeChat exists, and this
//! adapter reaches no execution surface (the crate iron rule).
//!
//! - **Receive** — one long-poll loop over `POST /ilink/bot/getupdates`
//!   (the server holds ~35 s; a client-side timeout is NORMAL: empty
//!   batch, re-poll, no backoff). `topics` is the receive allowlist: a
//!   message from a `from_user_id` that is not a key of the table is an
//!   honest drop with an audit line, never a fallback topic. Text-only
//!   (`message_type == 1` and a type-1 item); bot echoes never re-enter.
//!   `event_id = message_id` (uint64 parsed losslessly, kept a decimal
//!   string) is the idempotency key; a bounded in-memory ring fronts the
//!   durable admission guard. The `get_updates_buf` cursor starts empty
//!   every process start and is never persisted — no position table
//!   (§10's honest bound).
//! - **Send** — `POST /ilink/bot/sendmessage` echoing the **latest
//!   `context_token`** cached for the target user (cached only from
//!   ADMITTED inbound messages — a sender the allowlist rejected never
//!   becomes reachable; transport state, memory only). The target
//!   `user_id` must itself be a `topics` key (the recipient allowlist,
//!   §4/§9); anything else is an honest `DeliveryFailed` naming the
//!   allowlist. No token ⇒ `Unavailable` with the honest reason:
//!   proactive push is unsupported by the protocol in phase 1. Text
//!   above 2000 chars splits into ordered deliveries at char
//!   boundaries; a mid-split failure is a `DeliveryFailed` naming how
//!   many parts landed.
//! - **Credentials** — a QR-issued bot token in a 0600 file
//!   (`wechat::credentials`, the only place the token lives). On
//!   `errcode == -14` the whole channel pauses (audit + counter), sends
//!   return an honest `Unavailable` naming `kian wechat login`, and the
//!   driver polls the credential file's mtime (≤5 s) and resumes on
//!   change — no restart. A malformed replacement keeps the pause.
//! - **Untrusted by default** — the channel carries no policy of its
//!   own; assembly adds wechat-typed channels to the forced untrusted
//!   set (WeChat text is attacker-controllable by definition).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::warn;

pub mod credentials;
pub mod login;
pub mod protocol;

use credentials::WechatCredentials;
use protocol::{IlinkClient, IlinkError};

/// The channel `type` spelling that builds a [`WechatChannel`]
/// (kian-cli cross-references this constant).
pub const WECHAT_CHANNEL_TYPE: &str = "wechat";

/// The iLink Bot API base (configurable as `api_base`).
pub const DEFAULT_API_BASE: &str = "https://ilinkai.weixin.qq.com";

/// Per-subscriber inbound queue bound (the lark/mock discipline: a slow
/// consumer's copy is dropped with an audit line, never a block).
pub const SUBSCRIBER_CAPACITY: usize = 256;

/// How many distinct message ids the poll loop remembers (the in-memory
/// dedupe ring; the durable guard is admission's `event_id`
/// idempotency, so this only bounds re-surfacing between polls).
pub const SEEN_RING_CAPACITY: usize = 4096;

/// Default client long-poll deadline: above the server's ~35 s hold.
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(45_000);

/// Default one-shot send deadline.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(15);

/// Backoff after a receive error: 2 s doubling to a 30 s cap.
pub const DEFAULT_BACKOFF_START: Duration = Duration::from_secs(2);
pub const DEFAULT_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// How long a `-14` pause holds before retrying the same token
/// (the official client's 1 h behaviour).
pub const DEFAULT_STALE_PAUSE: Duration = Duration::from_secs(3600);

/// Credential-file mtime poll cadence while paused (contract: ≤5 s).
pub const DEFAULT_CREDENTIAL_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Defensive pacing between immediate responses (a real server holds
/// the long-poll ~35 s; this only stops a hot loop against a broken
/// server or a test mock).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Text is split into ordered deliveries above this many chars (the
/// real server-side limit is UNVERIFIED — defensive split, §14.5).
pub const SEND_SPLIT_CHARS: usize = 2000;

/// Honest construction/config failures (fail-closed: a bad wechat
/// config refuses to start, never silently degrades).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WechatError {
    #[error("wechat api base must be an http(s) URL, got `{0}`")]
    BadApiBase(String),
    #[error("wechat topic-mapping table must declare at least one user id")]
    NoTopics,
    #[error("wechat topic-mapping key/value must not be empty (`{0}`)")]
    EmptyTopicEntry(String),
    #[error("{0}")]
    Credentials(#[from] credentials::CredentialError),
    #[error("wechat http client: {0}")]
    Client(#[from] protocol::IlinkError),
}

/// The runtime wechat channel config. The YAML contract (CHANNEL-WECHAT
/// §7) carries `api_base`/`credentials_file`/`topics`/`poll_timeout_ms`;
/// the timing knobs below are assembly/test tuning with contract
/// defaults — they are not operator-facing config surface.
#[derive(Debug, Clone, PartialEq)]
pub struct WechatConfig {
    /// iLink API base, overridable for local mocks.
    pub api_base: String,
    /// Path of the credential file written by `kian wechat login`.
    pub credentials_file: PathBuf,
    /// `from_user_id` → routing topic. The keys are exactly the senders
    /// the receive loop admits (the allowlist); the values feed
    /// `ChannelEvent.topic`.
    pub topics: BTreeMap<String, String>,
    /// Client long-poll deadline.
    pub poll_timeout: Duration,
    /// One-shot send deadline.
    pub send_timeout: Duration,
    /// Backoff start after a receive error.
    pub backoff_start: Duration,
    /// Backoff cap.
    pub backoff_cap: Duration,
    /// How long a `-14` pause holds before retrying the same token.
    pub stale_pause: Duration,
    /// Credential mtime poll cadence while paused (≤5 s).
    pub credential_poll_interval: Duration,
    /// Pacing between immediately-answered polls.
    pub poll_interval: Duration,
}

/// `api_base` shape: an `http(s)://` URL with a non-empty host
/// (CHANNEL-WECHAT §7 — `https` in production; plain `http://` is
/// accepted deliberately so local mock-iLink tests work, but a bare
/// scheme or a hostless path is not a base).
pub fn is_http_base(url: &str) -> bool {
    match url.trim().split_once("://") {
        Some(("http" | "https", rest)) => !rest.is_empty() && !rest.starts_with('/'),
        _ => false,
    }
}

impl WechatConfig {
    /// The contract-shaped fields with timing defaults.
    pub fn new(
        api_base: impl Into<String>,
        credentials_file: impl Into<PathBuf>,
        topics: BTreeMap<String, String>,
    ) -> Self {
        Self {
            api_base: api_base.into(),
            credentials_file: credentials_file.into(),
            topics,
            poll_timeout: DEFAULT_POLL_TIMEOUT,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            backoff_start: DEFAULT_BACKOFF_START,
            backoff_cap: DEFAULT_BACKOFF_CAP,
            stale_pause: DEFAULT_STALE_PAUSE,
            credential_poll_interval: DEFAULT_CREDENTIAL_POLL_INTERVAL,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Fails closed on every degenerate field, including the credential
    /// file (missing/unreadable/malformed ⇒ startup error naming
    /// `kian wechat login`, per the config contract §7).
    pub fn validate(&self) -> Result<WechatCredentials, WechatError> {
        if !is_http_base(&self.api_base) {
            return Err(WechatError::BadApiBase(self.api_base.clone()));
        }
        if self.topics.is_empty() {
            return Err(WechatError::NoTopics);
        }
        for (user_id, topic) in &self.topics {
            if user_id.trim().is_empty() || topic.trim().is_empty() {
                return Err(WechatError::EmptyTopicEntry(format!("{user_id}→{topic}")));
            }
        }
        Ok(credentials::load(&self.credentials_file)?)
    }
}

/// The WeChat channel adapter. Cheap to clone (an `Arc` inside); share
/// clones, not deep copies.
#[derive(Clone)]
pub struct WechatChannel {
    inner: Arc<Inner>,
}

struct Inner {
    /// The configured channel name reported on every event and checked
    /// on every delivery.
    name: String,
    config: WechatConfig,
    client: IlinkClient,
    /// The current credential snapshot (rotates on resume after a
    /// re-login). Short critical sections; never held across an await.
    credentials: Mutex<WechatCredentials>,
    /// Latest `context_token` per ADMITTED user — transport state,
    /// memory only, updated before fan-out and only for senders the
    /// allowlist admitted (§5).
    context_tokens: Mutex<HashMap<String, String>>,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    poll_started: AtomicBool,
    paused: AtomicBool,
    dropped_full: AtomicU64,
    /// Metric: how many times a `-14` paused the channel (assembly
    /// snapshots it; no new metrics key is required by the contract).
    stale_pauses: AtomicU64,
    shutdown_tx: watch::Sender<bool>,
}

impl WechatChannel {
    /// Builds the adapter, fail-closed on config + credentials
    /// ([`WechatConfig::validate`]).
    pub fn new(name: impl Into<String>, config: WechatConfig) -> Result<Self, WechatError> {
        let credentials = config.validate()?;
        if let Some(mode) = credentials::insecure_permissions(&config.credentials_file) {
            warn!(
                target: "kian::wechat_channel",
                credentials_file = %config.credentials_file.display(),
                mode = format!("{mode:o}"),
                "wechat audit: credential file is group/world-readable; run `kian wechat login` to rewrite it 0600"
            );
        }
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.into(),
                config,
                client: IlinkClient::new().map_err(WechatError::from)?,
                credentials: Mutex::new(credentials),
                context_tokens: Mutex::new(HashMap::new()),
                subscribers: Mutex::new(Vec::new()),
                poll_started: AtomicBool::new(false),
                paused: AtomicBool::new(false),
                dropped_full: AtomicU64::new(0),
                stale_pauses: AtomicU64::new(0),
                shutdown_tx,
            }),
        })
    }

    /// The configured channel name.
    pub fn channel_name(&self) -> &str {
        &self.inner.name
    }

    /// The runtime config (assembly/tests; the credential token inside
    /// `Inner` is separate — never log that one).
    pub fn config(&self) -> &WechatConfig {
        &self.inner.config
    }

    /// Is the channel currently paused on a stale bot token (`-14`)?
    pub fn paused(&self) -> bool {
        self.inner.paused.load(Ordering::SeqCst)
    }

    /// How many inbound copies were dropped because a subscriber queue
    /// was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.inner.dropped_full.load(Ordering::Relaxed)
    }

    /// How many times a `-14` paused the channel.
    pub fn stale_pauses(&self) -> u64 {
        self.inner.stale_pauses.load(Ordering::Relaxed)
    }

    /// A fresh receiver for the shutdown signal (spawning the driver
    /// directly — tests and assembly mounts).
    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.inner.shutdown_tx.subscribe()
    }

    /// Signals graceful shutdown; the driver aborts its in-flight
    /// long-poll and exits.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.send(true);
    }

    /// The latest cached `context_token` for a user (transport state;
    /// test/diagnostic surface — the token is credentials-adjacent and
    /// must not be logged).
    pub fn cached_context_token(&self, user_id: &str) -> Option<String> {
        self.inner
            .context_tokens
            .lock()
            .expect("wechat channel lock")
            .get(user_id)
            .cloned()
    }

    /// Starts the shared receive driver exactly once (idempotent across
    /// any number of `subscribe` calls).
    fn start_driver(&self) {
        if self.inner.poll_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let channel = self.clone();
        let shutdown = self.shutdown_signal();
        tokio::spawn(async move {
            channel.run(shutdown).await;
        });
    }

    /// The long-poll receive driver. Runs until [`Self::shutdown`];
    /// transport/API failures back off 2 s → 30 s; `-14` pauses and
    /// watches the credential file's mtime. Never panics, never loops
    /// unboundedly without progress.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let inner = Arc::clone(&self.inner);
        let mut seen = SeenRing::new(SEEN_RING_CAPACITY);
        // Start empty every process start; carried within this run only
        // (never persisted — §10's honest bound).
        let mut cursor: Option<String> = None;
        let mut backoff = Backoff::new(inner.config.backoff_start, inner.config.backoff_cap);
        loop {
            if *shutdown.borrow() {
                break;
            }
            let token = {
                let guard = inner.credentials.lock().expect("wechat channel lock");
                guard.token.clone()
            };
            let request = inner.client.get_updates(
                &inner.config.api_base,
                &token,
                cursor.as_deref().unwrap_or(""),
                inner.config.poll_timeout,
            );
            let outcome = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                result = request => result,
            };
            match outcome {
                Ok(batch) => {
                    backoff.reset();
                    if let Some(next) = batch.cursor.filter(|c| !c.is_empty()) {
                        cursor = Some(next);
                    }
                    let received_at = now_ms();
                    for message in batch.msgs {
                        match map_message(&inner.name, &inner.config.topics, &message, received_at)
                        {
                            Mapping::Event(event) => {
                                // Cache only ADMITTED senders (§5.1): the
                                // mapping decision above is the allowlist,
                                // and every admitted event's sender is a
                                // `topics` key. Caching before the decision
                                // would make an allowlist-rejected sender a
                                // reachable send target.
                                cache_context_token(&inner, &message);
                                if seen.insert(event.event_id.clone()) {
                                    fan_out(&inner, event);
                                }
                            }
                            Mapping::Drop(reason) => {
                                let dropped_id = protocol::lossless_id(&message, "message_id")
                                    .unwrap_or_default();
                                let dropped_from = message
                                    .get("from_user_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string();
                                warn!(
                                    target: "kian::wechat_channel",
                                    channel = %inner.name,
                                    reason = reason.as_str(),
                                    message_id = %dropped_id,
                                    from_user_id = %dropped_from,
                                    "wechat audit: inbound message dropped"
                                );
                            }
                        }
                    }
                    // Defensive pacing (the real server holds the
                    // request ~35 s; a mock or broken server may not).
                    if !sleep_or_shutdown(inner.config.poll_interval, &mut shutdown).await {
                        break;
                    }
                }
                Err(IlinkError::Timeout) => {
                    // Normal long-poll cadence: empty retry, no
                    // backoff, no audit noise above debug.
                }
                Err(IlinkError::StaleToken) => {
                    inner.paused.store(true, Ordering::SeqCst);
                    inner.stale_pauses.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "kian::wechat_channel",
                        channel = %inner.name,
                        "wechat audit: bot token stale (errcode -14), channel paused; \
                         run `kian wechat login`, watching the credential file for a change"
                    );
                    if !wait_out_stale_pause(&inner, &mut shutdown).await {
                        break;
                    }
                }
                Err(error) => {
                    warn!(
                        target: "kian::wechat_channel",
                        channel = %inner.name,
                        error = %error,
                        "wechat audit: getupdates failed, backing off and retrying"
                    );
                    if !sleep_or_shutdown(backoff.next(), &mut shutdown).await {
                        break;
                    }
                }
            }
        }
    }
}

impl ChannelAdapter for WechatChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_CAPACITY);
        self.inner
            .subscribers
            .lock()
            .expect("wechat channel lock")
            .push(tx);
        self.start_driver();
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed()
    }

    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { send_delivery(&inner, &out).await })
    }
}

// ---------------------------------------------------------------------------
// Receive mapping (pure — the unit-test surface for §3)
// ---------------------------------------------------------------------------

/// Why a raw inbound message did not become a `ChannelEvent` (each drop
/// produces exactly one audit line with this code; nothing is
/// synthesized).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WechatDrop {
    /// `message_id` missing/empty — no idempotency key ⇒ no admission.
    NoMessageId,
    /// `message_type != 1` — a bot message (an echo loop if admitted).
    BotEcho,
    /// `from_user_id` empty/absent — no identity, no allowlist decision.
    NoSender,
    /// `from_user_id` not a key of `topics` — the honest allowlist drop.
    UnknownSender,
    /// No type-1 item with non-empty text — image/voice/file/video are
    /// phase 2.
    NotText,
}

impl WechatDrop {
    pub fn as_str(&self) -> &'static str {
        match self {
            WechatDrop::NoMessageId => "no_message_id",
            WechatDrop::BotEcho => "bot_echo",
            WechatDrop::NoSender => "no_sender",
            WechatDrop::UnknownSender => "unknown_sender",
            WechatDrop::NotText => "not_text",
        }
    }
}

/// The mapping decision for one raw message.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapping {
    Event(ChannelEvent),
    Drop(WechatDrop),
}

/// Maps one raw `WeixinMessage` per CHANNEL-WECHAT §3. Pure: a function
/// of the message plus the config's topic table and the receive-time
/// fallback. Only `message_type == 1` (user message) with a text item
/// from an allowlisted sender maps; everything else is an honest drop.
pub fn map_message(
    channel: &str,
    topics: &BTreeMap<String, String>,
    message: &Value,
    received_at_ms: u64,
) -> Mapping {
    let Some(event_id) = protocol::lossless_id(message, "message_id") else {
        return Mapping::Drop(WechatDrop::NoMessageId);
    };
    if message.get("message_type").and_then(Value::as_i64) != Some(1) {
        return Mapping::Drop(WechatDrop::BotEcho);
    }
    let Some(from_user_id) = message
        .get("from_user_id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
    else {
        return Mapping::Drop(WechatDrop::NoSender);
    };
    let Some(topic) = topics.get(from_user_id) else {
        return Mapping::Drop(WechatDrop::UnknownSender);
    };
    let item_list = message
        .get("item_list")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let item_types: Vec<Value> = item_list
        .iter()
        .map(|item| Value::from(item.get("type").and_then(Value::as_u64).unwrap_or(0)))
        .collect();
    let text = item_list
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_u64) == Some(1))
        .find_map(|item| {
            item.pointer("/text_item/text")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
        });
    let Some(text) = text else {
        return Mapping::Drop(WechatDrop::NotText);
    };
    let create_time_ms = message
        .get("create_time_ms")
        .and_then(Value::as_u64)
        .unwrap_or(received_at_ms);
    Mapping::Event(ChannelEvent {
        channel: channel.to_string(),
        event_id,
        topic: Some(topic.clone()),
        sender: Some(json!({"user_id": from_user_id})),
        group: None, // direct chats only in phase 1
        payload: json!({
            "text": text,
            "create_time_ms": create_time_ms,
            "item_types": item_types,
        }),
        ts_ms: create_time_ms,
    })
}

/// The lark-shaped thin wrapper: `Some(event)` or `None`.
pub fn message_to_event(
    channel: &str,
    topics: &BTreeMap<String, String>,
    message: &Value,
    received_at_ms: u64,
) -> Option<ChannelEvent> {
    match map_message(channel, topics, message, received_at_ms) {
        Mapping::Event(event) => Some(event),
        Mapping::Drop(_) => None,
    }
}

/// Caches the latest `context_token` per ADMITTED user (§5: transport
/// state, memory only, updated from every admitted inbound BEFORE
/// fan-out so a reply can always echo the freshest token). The caller
/// invokes this only after the allowlist mapping admitted the message —
/// a dropped inbound's token is never cached, so a sender the allowlist
/// rejected never becomes a reachable recipient. Bot echoes carry no
/// `from_user_id`, so our own sends never poison the cache.
fn cache_context_token(inner: &Inner, message: &Value) {
    if let (Some(user), Some(token)) = (
        message
            .get("from_user_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty()),
        message
            .get("context_token")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty()),
    ) {
        inner
            .context_tokens
            .lock()
            .expect("wechat channel lock")
            .insert(user.to_string(), token.to_string());
    }
}

// ---------------------------------------------------------------------------
// Send (CHANNEL-WECHAT §4/§5)
// ---------------------------------------------------------------------------

/// Sends one delivery. See the module docs for the pause/token/split
/// rules; every failure is an honest [`ChannelError`] with redacted
/// text (no token, no headers).
async fn send_delivery(inner: &Inner, out: &ChannelDelivery) -> Result<SendReceipt, ChannelError> {
    if out.channel != inner.name {
        return Err(ChannelError::DeliveryFailed(format!(
            "delivery named channel `{}` handed to the wechat channel `{}`",
            out.channel, inner.name
        )));
    }
    if inner.paused.load(Ordering::SeqCst) {
        return Err(ChannelError::Unavailable(
            "wechat channel paused: bot token stale (errcode -14); run `kian wechat login` \
             — the channel resumes when the credential file changes"
                .into(),
        ));
    }
    let Some(user_id) = out
        .target
        .get("user_id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
    else {
        return Err(ChannelError::DeliveryFailed(format!(
            "wechat target must be {{\"user_id\": \"<recipient>\"}}, got `{}`",
            out.target
        )));
    };
    // The recipient allowlist (§4/§9): the reply target is
    // model-controlled, so it may only ever name an operator-approved
    // user. Checked before any token lookup or wire traffic.
    if !inner.config.topics.contains_key(user_id) {
        return Err(ChannelError::DeliveryFailed(format!(
            "wechat send target `{user_id}` is not in the channel's topics allowlist \
             (a `from_user_id` key of superagent.channels.<name>.wechat.topics) — refused"
        )));
    }
    let text = match &out.content {
        Value::String(s) => s.clone(),
        Value::Object(object) => match object.get("text") {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(ChannelError::DeliveryFailed(format!(
                    "wechat content must be a string or {{\"text\": …}}, got `{}`",
                    out.content
                )));
            }
        },
        _ => {
            return Err(ChannelError::DeliveryFailed(format!(
                "wechat content must be a string or {{\"text\": …}}, got `{}`",
                out.content
            )));
        }
    };
    let Some(context_token) = inner
        .context_tokens
        .lock()
        .expect("wechat channel lock")
        .get(user_id)
        .cloned()
    else {
        return Err(ChannelError::Unavailable(format!(
            "wechat send to `{user_id}` has no cached context_token: proactive push is \
             unsupported (reply-only channel; the token arrives with an inbound message)"
        )));
    };
    let auth_token = {
        let guard = inner.credentials.lock().expect("wechat channel lock");
        guard.token.clone()
    };
    let parts = split_text(&text, SEND_SPLIT_CHARS);
    let total = parts.len();
    let mut last_receipt = None;
    for (index, part) in parts.into_iter().enumerate() {
        let client_id = uuid::Uuid::new_v4().to_string();
        // The exact §2.4 shape: from_user_id empty, message_type/state 2.
        let msg = json!({
            "from_user_id": "",
            "to_user_id": user_id,
            "client_id": client_id,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token,
            "item_list": [{"type": 1, "text_item": {"text": part}}],
        });
        let outcome = inner
            .client
            .send_message(
                &inner.config.api_base,
                &auth_token,
                &msg,
                inner.config.send_timeout,
            )
            .await;
        match outcome {
            Ok(outcome) if outcome.ret == 0 => {
                // Numeric ids are parsed losslessly; a ret==0 without
                // one falls back to OUR client_id — never an invented
                // server id.
                last_receipt = Some(SendReceipt {
                    message_id: outcome.message_id.unwrap_or(client_id),
                    ts_ms: now_ms(),
                });
            }
            Ok(outcome) => {
                return Err(plain_or_mid(
                    index,
                    total,
                    ChannelError::DeliveryFailed(format!(
                        "wechat send rejected: ret={} errmsg={}",
                        outcome.ret, outcome.errmsg
                    )),
                ));
            }
            Err(IlinkError::StaleToken) => {
                pause_for_stale_token(inner);
                return Err(plain_or_mid(
                    index,
                    total,
                    ChannelError::Unavailable(
                        "stale bot token (errcode -14); run `kian wechat login`".into(),
                    ),
                ));
            }
            Err(IlinkError::Timeout) => {
                return Err(plain_or_mid(index, total, ChannelError::Timeout));
            }
            Err(error) => {
                return Err(plain_or_mid(
                    index,
                    total,
                    ChannelError::Unavailable(error.to_string()),
                ));
            }
        }
    }
    last_receipt.ok_or_else(|| ChannelError::DeliveryFailed("wechat send produced no parts".into()))
}

/// A failure on the first part keeps its plain seam shape (`Timeout`
/// stays `Timeout`, `ret != 0` stays `DeliveryFailed`, transport stays
/// `Unavailable`); a failure after parts landed is always a
/// `DeliveryFailed` naming how many landed (§4's mid-split rule).
fn plain_or_mid(index: usize, total: usize, error: ChannelError) -> ChannelError {
    if index == 0 {
        return error;
    }
    let cause = match error {
        ChannelError::Timeout => "delivery timed out".to_string(),
        ChannelError::Unavailable(reason) | ChannelError::DeliveryFailed(reason) => reason,
    };
    ChannelError::DeliveryFailed(format!(
        "wechat send failed after {index} of {total} parts landed: {cause}"
    ))
}

/// Maps a typed protocol error to the seam vocabulary (the unit-test
/// surface for the §11 send-error rows).
pub fn ilink_error_to_channel_error(error: IlinkError) -> ChannelError {
    match error {
        IlinkError::Timeout => ChannelError::Timeout,
        IlinkError::StaleToken => ChannelError::Unavailable(
            "stale bot token (errcode -14); run `kian wechat login`".into(),
        ),
        other => ChannelError::Unavailable(other.to_string()),
    }
}

/// Splits text into ordered parts of at most `limit_chars` characters —
/// never mid-codepoint (chars, not bytes), never dropping characters.
/// Empty text yields one empty part (the server decides).
pub fn split_text(text: &str, limit_chars: usize) -> Vec<String> {
    let limit = limit_chars.max(1);
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut count = 0usize;
    for ch in text.chars() {
        if count == limit {
            parts.push(std::mem::take(&mut current));
            count = 0;
        }
        current.push(ch);
        count += 1;
    }
    if !current.is_empty() || parts.is_empty() {
        parts.push(current);
    }
    parts
}

/// The `-14` pause: audit once (the caller did), then poll the
/// credential file's mtime at ≤5 s cadence. A change re-reads the file:
/// valid ⇒ adopt + resume; malformed ⇒ keep paused with an audit line
/// (fail-closed). If the pause elapses first, retry the same token (the
/// official client's behaviour). Returns false only on shutdown.
async fn wait_out_stale_pause(inner: &Inner, shutdown: &mut watch::Receiver<bool>) -> bool {
    let started = Instant::now();
    let mut baseline = credentials::mtime(&inner.config.credentials_file);
    loop {
        if *shutdown.borrow() {
            return false;
        }
        let elapsed = started.elapsed();
        let remaining = inner.config.stale_pause.saturating_sub(elapsed);
        if remaining.is_zero() {
            warn!(
                target: "kian::wechat_channel",
                channel = %inner.name,
                "wechat audit: stale-token pause elapsed, retrying the same token"
            );
            inner.paused.store(false, Ordering::SeqCst);
            return true;
        }
        let tick = inner.config.credential_poll_interval.min(remaining);
        let woken = tokio::select! {
            biased;
            _ = shutdown.changed() => None,
            _ = tokio::time::sleep(tick) => Some(()),
        };
        if woken.is_none() {
            return false;
        }
        let current = credentials::mtime(&inner.config.credentials_file);
        if current != baseline && current.is_some() {
            baseline = current;
            match credentials::load(&inner.config.credentials_file) {
                Ok(fresh) => {
                    *inner.credentials.lock().expect("wechat channel lock") = fresh;
                    inner.paused.store(false, Ordering::SeqCst);
                    warn!(
                        target: "kian::wechat_channel",
                        channel = %inner.name,
                        "wechat audit: credential file changed, new token adopted, channel resumed"
                    );
                    return true;
                }
                Err(error) => {
                    warn!(
                        target: "kian::wechat_channel",
                        channel = %inner.name,
                        error = %error,
                        "wechat audit: credential file changed but is invalid; staying paused"
                    );
                }
            }
        }
    }
}

/// Marks the channel paused after a send observed `-14` (audit + metric
/// from the receive path's helper).
fn pause_for_stale_token(inner: &Inner) {
    inner.paused.store(true, Ordering::SeqCst);
    inner.stale_pauses.fetch_add(1, Ordering::Relaxed);
    warn!(
        target: "kian::wechat_channel",
        channel = %inner.name,
        "wechat audit: send rejected with errcode -14, channel paused; run `kian wechat login`"
    );
}

// ---------------------------------------------------------------------------
// Shared plumbing (the lark pattern)
// ---------------------------------------------------------------------------

/// Fans one event out to every live subscriber (bounded `try_send`; a
/// full queue drops that subscriber's copy with an audit line — a slow
/// consumer never blocks the poll loop).
fn fan_out(inner: &Inner, event: ChannelEvent) {
    let mut subscribers = inner.subscribers.lock().expect("wechat channel lock");
    subscribers.retain(|tx| !tx.is_closed());
    for tx in subscribers.iter() {
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                inner.dropped_full.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "kian::wechat_channel",
                    channel = %inner.name,
                    event_id = %event.event_id,
                    "wechat audit: subscriber queue full, event copy dropped"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Pruned on this pass; nothing to do.
            }
        }
    }
}

/// Bounded insert-order dedupe over message ids: a set plus the
/// insertion queue that evicts the oldest when the capacity is hit.
struct SeenRing {
    order: VecDeque<String>,
    set: std::collections::BTreeSet<String>,
    capacity: usize,
}

impl SeenRing {
    fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::new(),
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

/// Exponential backoff with a cap and a reset on success.
struct Backoff {
    start: Duration,
    current: Duration,
    cap: Duration,
}

impl Backoff {
    fn new(start: Duration, cap: Duration) -> Self {
        Self {
            start,
            current: start,
            cap,
        }
    }

    fn next(&mut self) -> Duration {
        let now = self.current;
        self.current = self
            .current
            .saturating_mul(2)
            .min(self.cap)
            .max(Duration::from_millis(1));
        now
    }

    fn reset(&mut self) {
        self.current = self.start;
    }
}

/// Sleeps, or returns false the moment shutdown is signalled.
async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return false;
    }
    let woken = tokio::select! {
        biased;
        _ = shutdown.changed() => None,
        _ = tokio::time::sleep(duration) => Some(()),
    };
    woken.is_some()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics() -> BTreeMap<String, String> {
        BTreeMap::from([("oUSER@im.wechat".to_string(), "wechat".to_string())])
    }

    fn user_message(message_id: Value, from: &str, text: &str) -> Value {
        json!({
            "message_id": message_id,
            "from_user_id": from,
            "to_user_id": "bot",
            "create_time_ms": 1730000000123u64,
            "message_type": 1,
            "message_state": 1,
            "item_list": [{"type": 1, "text_item": {"text": text}}],
            "context_token": "ctx-token-1",
        })
    }

    #[test]
    fn text_message_maps_to_the_full_channel_event() {
        // uint64 max on the wire: the id must survive losslessly.
        let raw = user_message(json!(u64::MAX), "oUSER@im.wechat", "build broken");
        let Mapping::Event(event) = map_message("wechat", &topics(), &raw, 1730000009999) else {
            panic!("a text message from an allowlisted sender maps");
        };
        assert_eq!(event.channel, "wechat");
        assert_eq!(event.event_id, "18446744073709551615");
        assert_eq!(event.topic.as_deref(), Some("wechat"));
        assert_eq!(event.sender, Some(json!({"user_id": "oUSER@im.wechat"})));
        assert_eq!(event.group, None, "direct chats carry no group");
        assert_eq!(
            event.payload,
            json!({
                "text": "build broken",
                "create_time_ms": 1730000000123u64,
                "item_types": [1],
            })
        );
        assert_eq!(event.ts_ms, 1730000000123);
    }

    #[test]
    fn string_message_ids_pass_through_verbatim() {
        let raw = user_message(json!("id-string-9"), "oUSER@im.wechat", "x");
        assert_eq!(
            message_to_event("wechat", &topics(), &raw, 0)
                .expect("maps")
                .event_id,
            "id-string-9"
        );
    }

    #[test]
    fn drop_rules_match_the_contract() {
        let table = topics();
        // bot echo (message_type 2)
        let mut echo = user_message(json!("id1"), "oUSER@im.wechat", "x");
        echo["message_type"] = json!(2);
        assert_eq!(
            map_message("wechat", &table, &echo, 0),
            Mapping::Drop(WechatDrop::BotEcho)
        );
        // unknown sender (allowlist)
        assert_eq!(
            map_message(
                "wechat",
                &table,
                &user_message(json!("id2"), "oSTRANGER@im.wechat", "x"),
                0
            ),
            Mapping::Drop(WechatDrop::UnknownSender)
        );
        // empty/absent sender
        assert_eq!(
            map_message("wechat", &table, &user_message(json!("id3"), "", "x"), 0),
            Mapping::Drop(WechatDrop::NoSender)
        );
        // non-text item
        let mut image = user_message(json!("id4"), "oUSER@im.wechat", "x");
        image["item_list"] = json!([{"type": 2, "image_item": {"key": "k"}}]);
        assert_eq!(
            map_message("wechat", &table, &image, 0),
            Mapping::Drop(WechatDrop::NotText)
        );
        // text item with empty text is not text
        let mut empty = user_message(json!("id5"), "oUSER@im.wechat", "");
        empty["item_list"] = json!([{"type": 1, "text_item": {"text": ""}}]);
        assert_eq!(
            map_message("wechat", &table, &empty, 0),
            Mapping::Drop(WechatDrop::NotText)
        );
        // missing message id
        let mut idless = user_message(json!("id6"), "oUSER@im.wechat", "x");
        idless.as_object_mut().unwrap().remove("message_id");
        assert_eq!(
            map_message("wechat", &table, &idless, 0),
            Mapping::Drop(WechatDrop::NoMessageId)
        );
        // missing create_time falls back to the receive time
        let mut no_ts = user_message(json!("id7"), "oUSER@im.wechat", "x");
        no_ts.as_object_mut().unwrap().remove("create_time_ms");
        let Mapping::Event(event) = map_message("wechat", &table, &no_ts, 1730000099999) else {
            panic!("maps");
        };
        assert_eq!(event.ts_ms, 1730000099999);
    }

    #[test]
    fn mixed_items_map_with_all_item_types_recorded() {
        let mut mixed = user_message(json!("id-mixed"), "oUSER@im.wechat", "the caption");
        mixed["item_list"] = json!([
            {"type": 2, "image_item": {"key": "k"}},
            {"type": 1, "text_item": {"text": "the caption"}},
        ]);
        let Mapping::Event(event) = map_message("wechat", &topics(), &mixed, 0) else {
            panic!("a text-bearing mixed message maps");
        };
        assert_eq!(
            event.payload,
            json!({
                "text": "the caption",
                "create_time_ms": 1730000000123u64,
                "item_types": [2, 1],
            })
        );
    }

    #[test]
    fn split_text_is_ordered_char_safe_and_lossless() {
        assert_eq!(split_text("abc", 5), vec!["abc"]);
        assert_eq!(split_text("abcde", 2), vec!["ab", "cd", "e"]);
        assert_eq!(split_text("", 2), vec![""]);
        // multibyte: char boundaries, never byte indices
        let parts = split_text(&"é".repeat(5), 2);
        assert_eq!(parts, vec!["éé", "éé", "é"]);
        // lossless round-trip at scale
        let text: String = "αβγ".repeat(2000);
        let parts = split_text(&text, 700);
        assert_eq!(parts.len(), 9); // 6000 chars / 700
        assert!(parts.iter().all(|p| p.chars().count() <= 700));
        assert_eq!(parts.concat(), text);
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

    #[test]
    fn backoff_doubles_to_the_cap_and_is_bounded() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(500));
        assert_eq!(backoff.next(), Duration::from_millis(100));
        assert_eq!(backoff.next(), Duration::from_millis(200));
        assert_eq!(backoff.next(), Duration::from_millis(400));
        assert_eq!(backoff.next(), Duration::from_millis(500));
        assert_eq!(backoff.next(), Duration::from_millis(500));
    }

    #[test]
    fn is_http_base_requires_scheme_and_host() {
        assert!(is_http_base("https://ilinkai.weixin.qq.com"));
        assert!(is_http_base("http://127.0.0.1:8080"));
        assert!(is_http_base("http://x"), "a short host is still a host");
        assert!(!is_http_base("http://"), "bare scheme has no host");
        assert!(!is_http_base("https:///path"), "hostless");
        assert!(!is_http_base("ftp://x"));
        assert!(!is_http_base("ilinkai.weixin.qq.com"));
    }

    #[test]
    fn error_mapping_follows_the_degradation_table() {
        assert_eq!(
            ilink_error_to_channel_error(IlinkError::Timeout),
            ChannelError::Timeout
        );
        assert!(matches!(
            ilink_error_to_channel_error(IlinkError::StaleToken),
            ChannelError::Unavailable(_)
        ));
        assert!(matches!(
            ilink_error_to_channel_error(IlinkError::Transport("refused".into())),
            ChannelError::Unavailable(_)
        ));
    }
}
