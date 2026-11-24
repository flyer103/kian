//! `ImessageChannel` — the iMessage channel adapter over the macOS
//! bridge ABI (CHANNEL-IMESSAGE, ADR-042).
//!
//! Footprint-Ladder level 2: plugin-hosted code over the core
//! `ChannelAdapter` seam; no core concept knows iMessage exists, and this
//! adapter reaches no execution surface (the crate iron rule). The
//! adapter talks only to a loopback bridge over HTTP; the bridge is a
//! separate daemon on the Messages Mac (CHANNEL-IMESSAGE §3).
//!
//! - **Receive** — one long-poll loop over `GET /v1/updates` (the
//!   bridge holds the request up to `wait_ms`). `topics` is the receive
//!   allowlist: a message from a handle that is not a key of the table
//!   is an honest drop with an audit line, never a fallback topic.
//!   Text-only direct chats; outgoing rows (`is_from_me`), groups,
//!   reactions, mutation updates and non-text rows never enter.
//!   `event_id = guid` (Apple's message GUID) is the idempotency key; a
//!   bounded in-memory ring fronts the durable admission guard. The
//!   `rowid` cursor is in-memory only and starts at 0 = the bridge's
//!   bounded tail replay (§9) — kian persists no position table.
//! - **Send** — `POST /v1/send` to the recipient handle the inbound
//!   event's `raw.sender_handle` named, validated against the same
//!   `topics` table (the recipient allowlist, §5/§8), with the latest
//!   inbound `chat_guid` for thread continuity. A deterministic
//!   `client_id` per part makes the outbox's bounded retry effectively
//!   once (the bridge's observed-result cache, §3.2). Text above
//!   `max_send_chars` splits into ordered deliveries; a mid-split
//!   failure is a `DeliveryFailed` naming how many parts landed.
//! - **Degradation** — bridge unreachable/unauthorized/unreadable and
//!   send refusals are honest audit lines plus the seam's closed error
//!   vocabulary (§10); a send whose outcome the bridge cannot prove is a
//!   `DeliveryFailed` that says so, never a fabricated receipt.
//! - **Untrusted by default** — the channel carries no policy of its
//!   own; assembly adds imessage-typed channels to the forced untrusted
//!   set (anyone who learns the bot handle can text it).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::warn;

pub mod bridge;
pub mod handle;

use bridge::{BridgeAttachment, BridgeClient, BridgeError, BridgeMessage, SendRequest};

/// The channel `type` spelling that builds an [`ImessageChannel`]
/// (kian-cli cross-references this constant).
pub const IMESSAGE_CHANNEL_TYPE: &str = "imessage";

/// The environment variable holding the bridge token (env-only, never
/// `kian.yaml`, never logged; CHANNEL-IMESSAGE §7).
pub const BRIDGE_TOKEN_ENV: &str = "KIAN_IMESSAGE_BRIDGE_TOKEN";

/// Per-subscriber inbound queue bound (the lark/mock discipline: a slow
/// consumer's copy is dropped with an audit line, never a block).
pub const SUBSCRIBER_CAPACITY: usize = 256;

/// How many distinct message guids the receive loop remembers (the
/// in-memory dedupe ring; the durable guard is admission's `event_id`
/// idempotency, so this only bounds re-surfacing between polls).
pub const SEEN_RING_CAPACITY: usize = 4096;

/// How many of our own sent guids the echo guard remembers (§5): an
/// inbound row whose guid matches is dropped even before its
/// `is_from_me` correction lands.
pub const ECHO_RING_CAPACITY: usize = 512;

/// Default client long-poll deadline (above the bridge's 25 s hold).
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(35_000);

/// Default long-poll hold requested from the bridge (the bridge caps at
/// 25 s).
pub const DEFAULT_WAIT: Duration = Duration::from_millis(20_000);

/// Default one-shot send deadline (above `imsg`'s ~8 s row verify).
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Pacing between immediately-answered polls (a healthy long-poll holds
/// `wait_ms`, so this only stops a hot loop against a broken bridge).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Backoff after a receive error: 2 s doubling to a 30 s cap.
pub const DEFAULT_BACKOFF_START: Duration = Duration::from_secs(2);
pub const DEFAULT_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Text is split into ordered deliveries above this many chars (the
/// real limit is UNVERIFIED — a defensive split, CHANNEL-IMESSAGE §5).
pub const SEND_SPLIT_CHARS: usize = 2000;

/// The page size requested from `/v1/updates` (the bridge's cap is 500).
pub const UPDATES_PAGE_LIMIT: u32 = 50;

/// The `service` selector (CHANNEL-IMESSAGE §7): phase 1 accepts `auto`
/// and `imessage`; an explicit `sms` belongs to phase 2 and refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImessageService {
    Auto,
    Imessage,
}

impl ImessageService {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ImessageService::Auto => "auto",
            ImessageService::Imessage => "imessage",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, ImessageError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(ImessageService::Auto),
            "imessage" => Ok(ImessageService::Imessage),
            "sms" => Err(ImessageError::PhaseTwoService("sms".into())),
            other => Err(ImessageError::BadService(other.to_string())),
        }
    }
}

/// `attachments: metadata | bytes` (ADR-064). `metadata` (the default)
/// never calls `/v1/attachment`, so a payload is exactly the metadata
/// projection; `bytes` fetches the row's image/audio items and hands the
/// media edge the ADR-062 envelope, whose transcripts the edge appends to
/// the admitted text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ImessageAttachments {
    #[default]
    Metadata,
    Bytes,
}

impl ImessageAttachments {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ImessageAttachments::Metadata => "metadata",
            ImessageAttachments::Bytes => "bytes",
        }
    }
}

/// How many attachments of one row the adapter hands the media edge.
pub const MAX_MEDIA_ITEMS_PER_ROW: usize = 4;

/// The largest single attachment the adapter fetches (the bridge's own
/// default cap; a bigger item is skipped before the request).
pub const MAX_MEDIA_ATTACHMENT_BYTES: u64 = 16 * 1024 * 1024;

/// One `/v1/attachment` deadline.
pub const ATTACHMENT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Honest construction/config failures (fail-closed: a bad imessage
/// config refuses to start, never silently degrades).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ImessageError {
    #[error("imessage bridge url must be an http(s) URL, got `{0}`")]
    BadBridgeUrl(String),
    #[error("imessage topic-mapping table must declare at least one handle")]
    NoTopics,
    #[error("imessage topic-mapping entry refused: {0}")]
    BadTopicEntry(String),
    #[error("imessage bridge token must not be empty (KIAN_IMESSAGE_BRIDGE_TOKEN)")]
    EmptyToken,
    #[error("imessage service `{0}` is not supported in phase 1 (auto | imessage)")]
    PhaseTwoService(String),
    #[error("imessage service must be auto or imessage, got `{0}`")]
    BadService(String),
    #[error("imessage timing: {0}")]
    BadTiming(String),
    #[error("imessage max_send_chars must be at least 100, got {0}")]
    BadSplit(usize),
}

/// `bridge_url` shape: an `http(s)://` URL with a non-empty host. Plain
/// `http://` is the loopback/tunnel norm; `https` is accepted for a
/// future TLS bridge (CHANNEL-IMESSAGE §7).
#[must_use]
pub fn is_http_base(url: &str) -> bool {
    match url.trim().split_once("://") {
        Some(("http" | "https", rest)) => !rest.is_empty() && !rest.starts_with('/'),
        _ => false,
    }
}

/// The runtime iMessage channel config. The YAML contract
/// (CHANNEL-IMESSAGE §7) carries `bridge_url`/`topics`/`service` and the
/// timing knobs; the token is env-only at assembly and never operator
/// config.
#[derive(Debug, Clone, PartialEq)]
pub struct ImessageConfig {
    /// The bridge base URL (loopback/tunnel).
    pub bridge_url: String,
    /// `KIAN_IMESSAGE_BRIDGE_TOKEN` — never logged, never in an event.
    pub token: String,
    /// Normalized handle → routing topic. The keys are exactly the
    /// senders the receive loop admits (the allowlist) and the only
    /// handles a delivery may target; the values feed
    /// `ChannelEvent.topic`.
    pub topics: BTreeMap<String, String>,
    /// Service selection for sends.
    pub service: ImessageService,
    /// Client long-poll deadline.
    pub poll_timeout: Duration,
    /// Long-poll hold requested from the bridge.
    pub wait: Duration,
    /// Pacing between immediately-answered polls.
    pub poll_interval: Duration,
    /// One-shot send deadline.
    pub send_timeout: Duration,
    /// Defensive split point for outbound text.
    pub max_send_chars: usize,
    /// `attachments: metadata | bytes` (ADR-064).
    pub attachments: ImessageAttachments,
}

impl ImessageConfig {
    /// The contract-shaped fields with timing defaults.
    pub fn new(
        bridge_url: impl Into<String>,
        token: impl Into<String>,
        topics: BTreeMap<String, String>,
    ) -> Self {
        Self {
            bridge_url: bridge_url.into(),
            token: token.into(),
            topics,
            service: ImessageService::Auto,
            poll_timeout: DEFAULT_POLL_TIMEOUT,
            wait: DEFAULT_WAIT,
            poll_interval: DEFAULT_POLL_INTERVAL,
            send_timeout: DEFAULT_SEND_TIMEOUT,
            max_send_chars: SEND_SPLIT_CHARS,
            attachments: ImessageAttachments::Metadata,
        }
    }

    /// Fails closed on every degenerate field and normalizes the
    /// topic table (CHANNEL-IMESSAGE §6/§7).
    pub fn validate(self) -> Result<Self, ImessageError> {
        if !is_http_base(&self.bridge_url) {
            return Err(ImessageError::BadBridgeUrl(self.bridge_url.clone()));
        }
        if self.token.trim().is_empty() {
            return Err(ImessageError::EmptyToken);
        }
        if self.topics.is_empty() {
            return Err(ImessageError::NoTopics);
        }
        let topics =
            handle::normalize_topics(&self.topics).map_err(ImessageError::BadTopicEntry)?;
        if self.wait > Duration::from_millis(bridge::MAX_WAIT_MS) {
            return Err(ImessageError::BadTiming(format!(
                "wait must be at most {} ms (the bridge's cap)",
                bridge::MAX_WAIT_MS
            )));
        }
        if self.poll_timeout <= self.wait + Duration::from_secs(5) {
            return Err(ImessageError::BadTiming(
                "poll_timeout must exceed wait by more than 5 s".into(),
            ));
        }
        if self.send_timeout.is_zero() {
            return Err(ImessageError::BadTiming(
                "send_timeout must be greater than zero".into(),
            ));
        }
        if self.max_send_chars < 100 {
            return Err(ImessageError::BadSplit(self.max_send_chars));
        }
        Ok(Self { topics, ..self })
    }
}

/// The iMessage channel adapter. Cheap to clone (an `Arc` inside);
/// share clones, not deep copies.
#[derive(Clone)]
pub struct ImessageChannel {
    inner: Arc<Inner>,
}

struct Inner {
    /// The configured channel name reported on every event and checked
    /// on every delivery.
    name: String,
    config: ImessageConfig,
    client: BridgeClient,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    started: AtomicBool,
    paused: AtomicBool,
    paused_reason: Mutex<Option<String>>,
    dropped_full: AtomicU64,
    /// Metric: how many `/v1/updates` pages reported a replay gap.
    gaps: AtomicU64,
    /// Metric: sends whose outcome the bridge could not prove.
    uncertain_sends: AtomicU64,
    /// The echo guard: guids of our own sends, bounded (§5).
    echo: Mutex<VecDeque<String>>,
    /// Latest inbound `chat_guid` per admitted (normalized) handle —
    /// transport state, memory only, reply-thread continuity (§5).
    chat_guids: Mutex<HashMap<String, String>>,
    shutdown_tx: watch::Sender<bool>,
}

impl ImessageChannel {
    /// Builds the adapter, fail-closed on config (§7).
    pub fn new(name: impl Into<String>, config: ImessageConfig) -> Result<Self, ImessageError> {
        let config = config.validate()?;
        let client = BridgeClient::new(config.bridge_url.clone(), config.token.clone())
            .map_err(|e| ImessageError::BadBridgeUrl(e.to_string()))?;
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.into(),
                config,
                client,
                subscribers: Mutex::new(Vec::new()),
                started: AtomicBool::new(false),
                paused: AtomicBool::new(false),
                paused_reason: Mutex::new(None),
                dropped_full: AtomicU64::new(0),
                gaps: AtomicU64::new(0),
                uncertain_sends: AtomicU64::new(0),
                echo: Mutex::new(VecDeque::new()),
                chat_guids: Mutex::new(HashMap::new()),
                shutdown_tx,
            }),
        })
    }

    /// The configured channel name.
    pub fn channel_name(&self) -> &str {
        &self.inner.name
    }

    /// The runtime config (assembly/tests). The token inside is
    /// credentials-adjacent — callers must not log it.
    pub fn config(&self) -> &ImessageConfig {
        &self.inner.config
    }

    /// Is the receive loop currently paused (bridge unreachable/
    /// unauthorized/DATABASE unreadable)? The reason is diagnostic.
    pub fn paused(&self) -> bool {
        self.inner.paused.load(Ordering::SeqCst)
    }

    /// The last pause reason (audit/diagnostic surface; never a token).
    pub fn paused_reason(&self) -> Option<String> {
        self.inner
            .paused_reason
            .lock()
            .expect("imessage channel lock")
            .clone()
    }

    /// How many inbound copies were dropped because a subscriber queue
    /// was full.
    pub fn dropped_full(&self) -> u64 {
        self.inner.dropped_full.load(Ordering::Relaxed)
    }

    /// How many `/v1/updates` pages reported a replay gap.
    pub fn gap_events(&self) -> u64 {
        self.inner.gaps.load(Ordering::Relaxed)
    }

    /// How many sends had an unprovable outcome (the honest counter).
    pub fn uncertain_sends(&self) -> u64 {
        self.inner.uncertain_sends.load(Ordering::Relaxed)
    }

    /// A fresh receiver for the shutdown signal (tests and assembly).
    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.inner.shutdown_tx.subscribe()
    }

    /// Signals graceful shutdown; the driver aborts its in-flight
    /// long-poll and exits.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.send(true);
    }

    /// The latest cached `chat_guid` for an admitted handle (transport
    /// state; test surface).
    pub fn cached_chat_guid(&self, handle: &str) -> Option<String> {
        let normalized = handle::normalize_handle(handle)?;
        self.inner
            .chat_guids
            .lock()
            .expect("imessage channel lock")
            .get(&normalized)
            .cloned()
    }

    /// Starts the shared receive driver exactly once (idempotent across
    /// any number of `subscribe` calls).
    fn start_driver(&self) {
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let channel = self.clone();
        let shutdown = self.shutdown_signal();
        tokio::spawn(async move {
            channel.run(shutdown).await;
        });
    }

    /// The long-poll receive driver. Runs until [`Self::shutdown`];
    /// transport failures back off 2 s → 30 s. Never panics, never
    /// loops unboundedly without progress.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let inner = Arc::clone(&self.inner);
        let mut seen = SeenRing::new(SEEN_RING_CAPACITY);
        // 0 = the bridge's bounded tail replay on startup only (§9).
        let mut cursor: u64 = 0;
        let mut generation = String::new();
        let mut backoff = Backoff::new(DEFAULT_BACKOFF_START, DEFAULT_BACKOFF_CAP);
        loop {
            if *shutdown.borrow() {
                break;
            }
            let request = inner.client.updates(
                cursor,
                UPDATES_PAGE_LIMIT,
                inner.config.wait.as_millis() as u64,
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
                    inner.paused.store(false, Ordering::SeqCst);
                    *inner.paused_reason.lock().expect("imessage channel lock") = None;
                    // A different chat.db is a new history: rowids are
                    // per database generation (CHANNEL-IMESSAGE §3.4).
                    if !generation.is_empty() && batch.generation != generation {
                        warn!(
                            target: "kian::imessage_channel",
                            channel = %inner.name,
                            previous = %generation,
                            current = %batch.generation,
                            "imessage audit: bridge database generation changed; resetting the rowid cursor"
                        );
                        cursor = 0;
                    }
                    if batch.gap {
                        inner.gaps.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            target: "kian::imessage_channel",
                            channel = %inner.name,
                            cursor = cursor,
                            next_rowid = batch.next_rowid,
                            "imessage audit: bridge reported a replay gap; continuing from next_rowid"
                        );
                    }
                    generation = batch.generation.clone();
                    // Advance the cursor before any early return: a page
                    // the adapter will drop (or a generation reset) must
                    // still move the read position forward, never re-read
                    // the same rows forever.
                    cursor = cursor.max(batch.next_rowid);
                    if batch.messages.is_empty() {
                        if !sleep_or_shutdown(inner.config.poll_interval, &mut shutdown).await {
                            break;
                        }
                        continue;
                    }
                    let received_at = now_ms();
                    for message in &batch.messages {
                        if let Some(guid) = own_echo(&inner, &message.guid) {
                            warn!(
                                target: "kian::imessage_channel",
                                channel = %inner.name,
                                guid = %guid,
                                "imessage audit: inbound row matches our own sent guid, dropped"
                            );
                            continue;
                        }
                        match map_message(&inner.name, &inner.config.topics, message, received_at) {
                            Mapping::Event(mut event) => {
                                if inner.config.attachments == ImessageAttachments::Bytes {
                                    attach_media(&inner, message, &batch.generation, &mut event)
                                        .await;
                                }
                                cache_chat_guid(&inner, message);
                                if seen.insert(event.event_id.clone()) {
                                    fan_out(&inner, event);
                                }
                            }
                            Mapping::Drop(reason) => {
                                warn!(
                                    target: "kian::imessage_channel",
                                    channel = %inner.name,
                                    reason = reason.as_str(),
                                    rowid = message.rowid,
                                    guid = %message.guid,
                                    sender = message.sender.as_deref().unwrap_or(""),
                                    "imessage audit: inbound message dropped"
                                );
                            }
                        }
                    }
                    if !batch.has_more
                        && !sleep_or_shutdown(inner.config.poll_interval, &mut shutdown).await
                    {
                        break;
                    }
                }
                Err(BridgeError::Timeout) => {
                    // Normal long-poll cadence: the bridge held the
                    // request to `wait_ms` and answered empty.
                }
                Err(error) => {
                    let reason = match &error {
                        BridgeError::Unauthorized => {
                            "imessage bridge unauthorized: KIAN_IMESSAGE_BRIDGE_TOKEN does not \
                             match the bridge's token_file"
                                .to_string()
                        }
                        BridgeError::DatabaseUnreadable(detail) => format!(
                            "imessage bridge database unreadable ({detail}): grant Full Disk \
                             Access to the bridge binary under System Settings → Privacy & \
                             Security → Full Disk Access"
                        ),
                        BridgeError::Unreachable(detail) => format!(
                            "imessage bridge unreachable ({detail}): check the bridge process and \
                             the SSH remote-forward to the server"
                        ),
                        other => other.to_string(),
                    };
                    if !inner.paused.swap(true, Ordering::SeqCst) {
                        warn!(
                            target: "kian::imessage_channel",
                            channel = %inner.name,
                            reason = %reason,
                            "imessage audit: receive paused, backing off"
                        );
                    }
                    *inner.paused_reason.lock().expect("imessage channel lock") = Some(reason);
                    if !sleep_or_shutdown(backoff.next(), &mut shutdown).await {
                        break;
                    }
                }
            }
        }
    }
}

impl ChannelAdapter for ImessageChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_CAPACITY);
        self.inner
            .subscribers
            .lock()
            .expect("imessage channel lock")
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
// Receive mapping (pure — the unit-test surface for §4)
// ---------------------------------------------------------------------------

/// Why a bridge message did not become a `ChannelEvent` (each drop
/// produces exactly one audit line with this code; nothing is
/// synthesized).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImessageDrop {
    /// `is_from_me` — the Mac records our own sends as outgoing rows.
    FromMe,
    /// `is_group` — phase 1 is direct chats only.
    GroupChat,
    /// `is_reaction` — a tapback is not input.
    Reaction,
    /// No text item — an attachment-only row reaches this only when the
    /// bridge runs with `watch.attachments` off (it synthesizes a marker
    /// otherwise); the bytes of what arrived are phase 2.
    NotText,
    /// `sender` empty/absent — no identity, no allowlist decision.
    NoSender,
    /// The sender handle is not a `topics` key — the honest drop.
    UnknownSender,
    /// `guid` missing/empty — no idempotency key ⇒ no admission.
    NoGuid,
}

impl ImessageDrop {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ImessageDrop::FromMe => "from_me",
            ImessageDrop::GroupChat => "group_chat",
            ImessageDrop::Reaction => "reaction",
            ImessageDrop::NotText => "not_text",
            ImessageDrop::NoSender => "no_sender",
            ImessageDrop::UnknownSender => "unknown_sender",
            ImessageDrop::NoGuid => "no_guid",
        }
    }
}

/// The mapping decision for one bridge message.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapping {
    Event(ChannelEvent),
    Drop(ImessageDrop),
}

/// Maps one bridge message per CHANNEL-IMESSAGE §4. Pure: a function of
/// the message plus the config's normalized topic table and the
/// receive-time fallback. Only a new, text-bearing, direct inbound row
/// from an allowlisted handle maps; everything else is an honest drop. A
/// row that carries attachments also contributes their metadata to the
/// payload, so the model sees what arrived even though it cannot open it.
#[must_use]
pub fn map_message(
    channel: &str,
    topics: &BTreeMap<String, String>,
    message: &BridgeMessage,
    received_at_ms: u64,
) -> Mapping {
    if message.is_from_me {
        return Mapping::Drop(ImessageDrop::FromMe);
    }
    if message.is_group {
        return Mapping::Drop(ImessageDrop::GroupChat);
    }
    if message.is_reaction {
        return Mapping::Drop(ImessageDrop::Reaction);
    }
    let Some(text) = message
        .text
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
    else {
        return Mapping::Drop(ImessageDrop::NotText);
    };
    let Some(sender) = message
        .sender
        .as_deref()
        .map(str::trim)
        .filter(|sender| !sender.is_empty())
    else {
        return Mapping::Drop(ImessageDrop::NoSender);
    };
    let Some(normalized) = handle::normalize_handle(sender) else {
        return Mapping::Drop(ImessageDrop::NoSender);
    };
    let Some(topic) = topics.get(&normalized) else {
        return Mapping::Drop(ImessageDrop::UnknownSender);
    };
    if message.guid.trim().is_empty() {
        return Mapping::Drop(ImessageDrop::NoGuid);
    }
    let created_at_ms = message.created_at_ms.unwrap_or(received_at_ms);
    let mut payload = json!({
        "text": text,
        "created_at_ms": created_at_ms,
        "sender_handle": sender,
        "chat_guid": message.chat_guid,
        "chat_identifier": message.chat_identifier,
        "service": message.service,
        "is_group": false,
    });
    if !message.attachments.is_empty() {
        payload["attachments"] = attachment_metadata(&message.attachments);
    }
    Mapping::Event(ChannelEvent {
        channel: channel.to_string(),
        event_id: message.guid.clone(),
        topic: Some(topic.clone()),
        sender: Some(json!({"handle": sender})),
        group: None, // direct chats only in phase 1
        payload,
        ts_ms: created_at_ms,
    })
}

/// The `attachments` items the payload carries: what the model can reason
/// about, and nothing that encodes a physical scan position — a durable
/// payload must not name an item by `rowid` (CHANNEL-IMESSAGE §4).
fn attachment_metadata(attachments: &[BridgeAttachment]) -> serde_json::Value {
    serde_json::Value::Array(
        attachments
            .iter()
            .map(|attachment| {
                json!({
                    "filename": attachment.filename,
                    "mime_type": attachment.mime_type,
                    "total_bytes": attachment.total_bytes,
                    "is_sticker": attachment.is_sticker,
                })
            })
            .collect(),
    )
}

/// The thin wrapper: `Some(event)` or `None`.
#[must_use]
pub fn message_to_event(
    channel: &str,
    topics: &BTreeMap<String, String>,
    message: &BridgeMessage,
    received_at_ms: u64,
) -> Option<ChannelEvent> {
    match map_message(channel, topics, message, received_at_ms) {
        Mapping::Event(event) => Some(event),
        Mapping::Drop(_) => None,
    }
}

/// Caches the latest inbound `chat_guid` per ADMITTED handle (§5:
/// transport state, memory only, updated before fan-out). The caller
/// invokes this only after the allowlist mapping admitted the message —
/// a dropped inbound never becomes a reply target's thread hint.
fn cache_chat_guid(inner: &Inner, message: &BridgeMessage) {
    let (Some(sender), Some(chat_guid)) = (
        message.sender.as_deref().and_then(handle::normalize_handle),
        message
            .chat_guid
            .as_deref()
            .filter(|guid| !guid.trim().is_empty()),
    ) else {
        return;
    };
    inner
        .chat_guids
        .lock()
        .expect("imessage channel lock")
        .insert(sender, chat_guid.to_string());
}

/// Records one of our own sent guids in the bounded echo guard (§5).
fn remember_own_echo(inner: &Inner, guid: &str) {
    if guid.trim().is_empty() {
        return;
    }
    let mut echo = inner.echo.lock().expect("imessage channel lock");
    if echo.len() == ECHO_RING_CAPACITY {
        echo.pop_front();
    }
    echo.push_back(guid.to_string());
}

/// `Some(guid)` iff the guid is one we sent (the echo guard).
fn own_echo(inner: &Inner, guid: &str) -> Option<String> {
    let echo = inner.echo.lock().expect("imessage channel lock");
    echo.iter().find(|known| known.as_str() == guid).cloned()
}

// ---------------------------------------------------------------------------
// Send (CHANNEL-IMESSAGE §5)
// ---------------------------------------------------------------------------

/// Sends one delivery. Every failure is an honest [`ChannelError`] with
/// redacted text (no token, no headers).
async fn send_delivery(inner: &Inner, out: &ChannelDelivery) -> Result<SendReceipt, ChannelError> {
    if out.channel != inner.name {
        return Err(ChannelError::DeliveryFailed(format!(
            "delivery named channel `{}` handed to the imessage channel `{}`",
            out.channel, inner.name
        )));
    }
    // The channel-defined target may arrive as a JSON object or as a
    // JSON-encoded string (tool-argument dialects differ); parse the
    // string form before the allowlist checks so a well-formed target
    // is never rejected over its wire spelling.
    let target = match &out.target {
        Value::String(raw) => {
            serde_json::from_str::<Value>(raw).unwrap_or_else(|_| out.target.clone())
        }
        other => other.clone(),
    };
    let Some(raw_handle) = target
        .get("handle")
        .and_then(Value::as_str)
        .filter(|handle| !handle.trim().is_empty())
    else {
        return Err(ChannelError::DeliveryFailed(format!(
            "imessage target must be {{\"handle\": \"<recipient>\"}}, got `{}`",
            out.target
        )));
    };
    let Some(handle) = handle::normalize_handle(raw_handle) else {
        return Err(ChannelError::DeliveryFailed(format!(
            "imessage target handle `{raw_handle}` is empty after normalization"
        )));
    };
    // The recipient allowlist (§5/§8): the reply target is
    // model-controlled, so it may only ever name an operator-approved
    // handle. Checked before any wire traffic.
    if !inner.config.topics.contains_key(&handle) {
        return Err(ChannelError::DeliveryFailed(format!(
            "imessage send target `{handle}` is not in the channel's topics allowlist \
             (a handle key of superagent.channels.<name>.imessage.topics) — refused"
        )));
    }
    let text = match &out.content {
        Value::String(s) => s.clone(),
        Value::Object(object) => match object.get("text") {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(ChannelError::DeliveryFailed(format!(
                    "imessage content must be a string or {{\"text\": …}}, got `{}`",
                    out.content
                )));
            }
        },
        _ => {
            return Err(ChannelError::DeliveryFailed(format!(
                "imessage content must be a string or {{\"text\": …}}, got `{}`",
                out.content
            )));
        }
    };
    let chat_guid = inner
        .chat_guids
        .lock()
        .expect("imessage channel lock")
        .get(&handle)
        .cloned();
    let parts = split_text(&text, inner.config.max_send_chars);
    let total = parts.len();
    let mut last_receipt = None;
    for (index, part) in parts.into_iter().enumerate() {
        let client_id = delivery_client_id(&inner.name, &handle, index, total, &part);
        let request = SendRequest::new(
            client_id.clone(),
            handle.clone(),
            chat_guid.clone(),
            part,
            inner.config.service.as_str(),
        );
        match inner.client.send(&request, inner.config.send_timeout).await {
            Ok(ok) => {
                let message_id = if !ok.message_id.trim().is_empty() {
                    ok.message_id
                } else if let Some(guid) = ok.guid.clone() {
                    guid
                } else if let Some(rowid) = ok.rowid {
                    format!("imessage:{rowid}")
                } else {
                    client_id
                };
                if let Some(guid) = ok.guid.as_deref().or(Some(message_id.as_str())) {
                    remember_own_echo(inner, guid);
                }
                if ok.deduplicated {
                    tracing::debug!(
                        target: "kian::imessage_channel",
                        channel = %inner.name,
                        handle = %handle,
                        message_id = %message_id,
                        "imessage: bridge served a repeated client_id from its observed cache"
                    );
                }
                last_receipt = Some(SendReceipt {
                    message_id,
                    ts_ms: now_ms(),
                });
            }
            Err(error) => {
                return Err(plain_or_mid(index, total, disposition_error(inner, error)));
            }
        }
    }
    last_receipt
        .ok_or_else(|| ChannelError::DeliveryFailed("imessage send produced no parts".into()))
}

/// Maps the bridge's delivery disposition onto the seam vocabulary
/// (CHANNEL-IMESSAGE §5/§10). `not_started` is a real retry; an
/// unprovable outcome is a `DeliveryFailed` that says "do not retry
/// blindly" (the outbox still retries by design — the bridge's
/// `client_id` cache is what makes the retry safe).
fn disposition_error(inner: &Inner, error: BridgeError) -> ChannelError {
    match error {
        BridgeError::SendRejected {
            kind,
            detail,
            retry_safe,
        } => match kind.as_str() {
            "not_started" | "target_unreachable" | "bridge_busy" | "bad_request" => {
                ChannelError::DeliveryFailed(format!(
                    "imessage send not started ({kind}, retry-safe={retry_safe}): {detail}"
                ))
            }
            "may_have_completed" | "still_in_flight" => {
                inner.uncertain_sends.fetch_add(1, Ordering::Relaxed);
                ChannelError::DeliveryFailed(format!(
                    "imessage send outcome unknown ({kind}): the message may have been delivered; \
                     do not retry automatically (a retry can duplicate) — {detail}"
                ))
            }
            "unauthorized" => ChannelError::Unavailable(
                "imessage bridge unauthorized (check KIAN_IMESSAGE_BRIDGE_TOKEN)".into(),
            ),
            "bridge_unavailable" => {
                ChannelError::Unavailable(format!("imessage bridge unavailable: {detail}"))
            }
            other => ChannelError::DeliveryFailed(format!(
                "imessage bridge refused the send ({other}): {detail}"
            )),
        },
        BridgeError::Unauthorized => ChannelError::Unavailable(
            "imessage bridge unauthorized: KIAN_IMESSAGE_BRIDGE_TOKEN does not match the \
             bridge's token_file"
                .into(),
        ),
        BridgeError::DatabaseUnreadable(detail) => ChannelError::Unavailable(format!(
            "imessage bridge database unreadable ({detail}); grant Full Disk Access to the bridge"
        )),
        BridgeError::Unreachable(detail) => {
            ChannelError::Unavailable(format!("imessage bridge unreachable: {detail}"))
        }
        BridgeError::AttachmentUnavailable(detail) => {
            ChannelError::Unavailable(format!("imessage attachment unavailable ({detail})"))
        }
        BridgeError::Timeout => ChannelError::Timeout,
        BridgeError::Malformed(detail) => {
            // The response could not be read after dispatch: the
            // outcome is unprovable by definition.
            inner.uncertain_sends.fetch_add(1, Ordering::Relaxed);
            ChannelError::DeliveryFailed(format!(
                "imessage send outcome unknown: the bridge response was malformed ({detail}); \
                 do not retry automatically"
            ))
        }
    }
}

/// A failure on the first part keeps its plain seam shape; a failure
/// after parts landed is always a `DeliveryFailed` naming how many
/// landed (§5's mid-split rule).
fn plain_or_mid(index: usize, total: usize, error: ChannelError) -> ChannelError {
    if index == 0 {
        return error;
    }
    let cause = match error {
        ChannelError::Timeout => "delivery timed out".to_string(),
        ChannelError::Unavailable(reason) | ChannelError::DeliveryFailed(reason) => reason,
    };
    ChannelError::DeliveryFailed(format!(
        "imessage send failed after {index} of {total} parts landed: {cause}"
    ))
}

/// The deterministic delivery idempotency key (§3.2): the same shaped
/// delivery (channel + handle + part index/total + part bytes) always
/// yields the same key, so the bridge can answer a retry from its
/// observed-result cache instead of sending twice.
#[must_use]
pub fn delivery_client_id(
    channel: &str,
    handle: &str,
    index: usize,
    total: usize,
    part: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(channel.as_bytes());
    hasher.update([0x1f]);
    hasher.update(handle.as_bytes());
    hasher.update([0x1f]);
    hasher.update(index.to_string().as_bytes());
    hasher.update([0x1f]);
    hasher.update(total.to_string().as_bytes());
    hasher.update([0x1f]);
    hasher.update(part.as_bytes());
    let digest = hasher.finalize();
    let mut id = String::with_capacity(32);
    for byte in &digest[..16] {
        id.push_str(&format!("{byte:02x}"));
    }
    id
}

/// Splits text into ordered parts of at most `limit_chars` characters —
/// never mid-codepoint (chars, not bytes), never dropping characters.
/// Empty text yields one empty part (the bridge decides).
#[must_use]
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

// ---------------------------------------------------------------------------
// Shared plumbing (the wechat pattern)
// ---------------------------------------------------------------------------

/// ADR-064: hand the media edge the ADR-062 `media` envelope for the
/// row's image/audio items. Every failure is one audit line for one
/// item — the row is still admitted with its metadata, never dropped and
/// never given a fabricated transcript.
async fn attach_media(
    inner: &Inner,
    message: &BridgeMessage,
    generation: &str,
    event: &mut ChannelEvent,
) {
    let mut items = Vec::new();
    for attachment in message
        .attachments
        .iter()
        .filter(|attachment| attachment.media_kind().is_some())
        .take(MAX_MEDIA_ITEMS_PER_ROW)
    {
        let Some(id) = attachment.attachment_id.as_deref() else {
            continue;
        };
        let kind = attachment.media_kind().unwrap_or("image");
        if attachment.total_bytes.unwrap_or(0) > MAX_MEDIA_ATTACHMENT_BYTES {
            warn!(
                target: "kian::imessage_channel",
                channel = %inner.name,
                attachment_id = %id,
                total_bytes = attachment.total_bytes.unwrap_or(0),
                "imessage audit: attachment skipped before the fetch (over the adapter's bound)"
            );
            continue;
        }
        match inner
            .client
            .attachment(id, generation, ATTACHMENT_FETCH_TIMEOUT)
            .await
        {
            Ok(bytes) => items.push(json!({
                "kind": kind,
                "media_type": bytes.media_type,
                "base64": base64_encode(&bytes.bytes),
            })),
            Err(error) => warn!(
                target: "kian::imessage_channel",
                channel = %inner.name,
                attachment_id = %id,
                kind = kind,
                error = %error,
                "imessage audit: attachment fetch failed; the row keeps its metadata"
            ),
        }
    }
    if !items.is_empty()
        && let Some(payload) = event.payload.as_object_mut()
    {
        payload.insert("media".into(), Value::Array(items));
    }
}

/// Base64 for the ADR-062 envelope (`kian-media`'s `base64_decode` reads
/// it on the edge; the adapter never touches bytes otherwise).
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Fans one event out to every live subscriber (bounded `try_send`; a
/// full queue drops that subscriber's copy with an audit line — a slow
/// consumer never blocks the poll loop).
fn fan_out(inner: &Inner, event: ChannelEvent) {
    let mut subscribers = inner.subscribers.lock().expect("imessage channel lock");
    subscribers.retain(|tx| !tx.is_closed());
    for tx in subscribers.iter() {
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                inner.dropped_full.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "kian::imessage_channel",
                    channel = %inner.name,
                    event_id = %event.event_id,
                    "imessage audit: subscriber queue full, event copy dropped"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
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
        BTreeMap::from([("+14155551212".to_string(), "imessage".to_string())])
    }

    fn message(guid: &str) -> BridgeMessage {
        BridgeMessage {
            rowid: 42,
            guid: guid.to_string(),
            chat_id: Some(7),
            chat_guid: Some("iMessage;-;+14155551212".into()),
            chat_identifier: Some("+14155551212".into()),
            sender: Some("+1 (415) 555-1212".into()),
            text: Some("  hi  ".into()),
            created_at: Some("2026-11-19T15:31:55Z".into()),
            created_at_ms: Some(1_794_718_315_000),
            service: Some("iMessage".into()),
            is_from_me: false,
            is_group: false,
            is_reaction: false,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn inbound_text_from_an_allowlisted_handle_maps() {
        let Mapping::Event(event) = map_message("imessage", &topics(), &message("g-1"), 9) else {
            panic!("expected an event");
        };
        assert_eq!(event.channel, "imessage");
        assert_eq!(event.event_id, "g-1");
        assert_eq!(event.topic.as_deref(), Some("imessage"));
        assert_eq!(event.sender, Some(json!({"handle": "+1 (415) 555-1212"})));
        assert_eq!(event.group, None);
        assert_eq!(event.ts_ms, 1_794_718_315_000);
        assert_eq!(event.payload["text"], "hi");
        assert_eq!(event.payload["sender_handle"], "+1 (415) 555-1212");
        assert_eq!(event.payload["chat_guid"], "iMessage;-;+14155551212");
        assert_eq!(event.payload["service"], "iMessage");
        assert_eq!(event.payload["is_group"], false);
        assert!(
            event.payload.get("attachments").is_none(),
            "a text-only row's payload keeps its exact keys"
        );
    }

    #[test]
    fn attachment_metadata_reaches_the_payload_without_the_row_position() {
        let mut photo = message("g-photo");
        photo.text = Some("look at this [image]".into());
        photo.attachments = vec![BridgeAttachment {
            attachment_id: Some("42:0".into()),
            filename: Some("IMG_0001.heic".into()),
            mime_type: Some("image/heic".into()),
            transfer_name: Some("IMG_0001.heic".into()),
            total_bytes: Some(2_345_678),
            is_sticker: false,
        }];
        let Mapping::Event(event) = map_message("imessage", &topics(), &photo, 9) else {
            panic!("expected an event");
        };
        assert_eq!(
            event.payload,
            json!({
                "text": "look at this [image]",
                "created_at_ms": 1_794_718_315_000u64,
                "sender_handle": "+1 (415) 555-1212",
                "chat_guid": "iMessage;-;+14155551212",
                "chat_identifier": "+14155551212",
                "service": "iMessage",
                "is_group": false,
                "attachments": [{
                    "filename": "IMG_0001.heic",
                    "mime_type": "image/heic",
                    "total_bytes": 2_345_678u64,
                    "is_sticker": false,
                }],
            })
        );
    }

    #[test]
    fn every_drop_rule_is_honest_and_named() {
        let mut from_me = message("g");
        from_me.is_from_me = true;
        assert_eq!(
            map_message("imessage", &topics(), &from_me, 0),
            Mapping::Drop(ImessageDrop::FromMe)
        );

        let mut group = message("g");
        group.is_group = true;
        assert_eq!(
            map_message("imessage", &topics(), &group, 0),
            Mapping::Drop(ImessageDrop::GroupChat)
        );

        let mut reaction = message("g");
        reaction.is_reaction = true;
        assert_eq!(
            map_message("imessage", &topics(), &reaction, 0),
            Mapping::Drop(ImessageDrop::Reaction)
        );

        let mut not_text = message("g");
        not_text.text = None;
        assert_eq!(
            map_message("imessage", &topics(), &not_text, 0),
            Mapping::Drop(ImessageDrop::NotText)
        );

        let mut blank = message("g");
        blank.text = Some("   \n".into());
        assert_eq!(
            map_message("imessage", &topics(), &blank, 0),
            Mapping::Drop(ImessageDrop::NotText)
        );

        let mut no_sender = message("g");
        no_sender.sender = Some("".into());
        assert_eq!(
            map_message("imessage", &topics(), &no_sender, 0),
            Mapping::Drop(ImessageDrop::NoSender)
        );

        let mut unknown = message("g");
        unknown.sender = Some("+19995550000".into());
        assert_eq!(
            map_message("imessage", &topics(), &unknown, 0),
            Mapping::Drop(ImessageDrop::UnknownSender)
        );

        let mut no_guid = message("   ");
        no_guid.guid = "".into();
        assert_eq!(
            map_message("imessage", &topics(), &no_guid, 0),
            Mapping::Drop(ImessageDrop::NoGuid)
        );
    }

    #[test]
    fn receiver_time_is_the_fallback_timestamp() {
        let mut unsynced = message("g-ts");
        unsynced.created_at_ms = None;
        let Mapping::Event(event) = map_message("imessage", &topics(), &unsynced, 1234) else {
            panic!("expected an event");
        };
        assert_eq!(event.ts_ms, 1234);
        assert_eq!(event.payload["created_at_ms"], 1234);
    }

    #[test]
    fn split_text_is_ordered_and_lossless() {
        let text = "ab".repeat(2_500);
        let parts = split_text(&text, 2_000);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].chars().count(), 2_000);
        assert_eq!(parts[2].chars().count(), 1_000);
        assert_eq!(parts.concat(), text);
        assert!(split_text("", 10).len() == 1);
    }

    #[test]
    fn split_text_never_breaks_a_codepoint() {
        let text = "你".repeat(2_001);
        let parts = split_text(&text, 2_000);
        assert_eq!(parts.concat(), text);
        assert_eq!(parts[0].chars().count(), 2_000);
    }

    #[test]
    fn delivery_client_id_is_deterministic_and_part_scoped() {
        let a = delivery_client_id("imessage", "+14155551212", 0, 2, "hello");
        let b = delivery_client_id("imessage", "+14155551212", 0, 2, "hello");
        let c = delivery_client_id("imessage", "+14155551212", 1, 2, "hello");
        let d = delivery_client_id("imessage", "+14155551212", 0, 2, "hell0");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn config_validation_fails_closed() {
        let bad_url = ImessageConfig::new("not-a-url", "t", topics());
        assert!(matches!(
            bad_url.validate(),
            Err(ImessageError::BadBridgeUrl(_))
        ));

        let no_token = ImessageConfig::new("http://127.0.0.1:7420", "  ", topics());
        assert!(matches!(
            no_token.validate(),
            Err(ImessageError::EmptyToken)
        ));

        let no_topics = ImessageConfig::new("http://127.0.0.1:7420", "t", BTreeMap::new());
        assert!(matches!(no_topics.validate(), Err(ImessageError::NoTopics)));

        let mut bad_wait = ImessageConfig::new("http://127.0.0.1:7420", "t", topics());
        bad_wait.wait = Duration::from_secs(30);
        assert!(matches!(
            bad_wait.validate(),
            Err(ImessageError::BadTiming(_))
        ));

        let mut bad_timeout = ImessageConfig::new("http://127.0.0.1:7420", "t", topics());
        bad_timeout.poll_timeout = bad_timeout.wait;
        assert!(matches!(
            bad_timeout.validate(),
            Err(ImessageError::BadTiming(_))
        ));

        let mut bad_split = ImessageConfig::new("http://127.0.0.1:7420", "t", topics());
        bad_split.max_send_chars = 99;
        assert!(matches!(
            bad_split.validate(),
            Err(ImessageError::BadSplit(99))
        ));
    }

    #[test]
    fn config_normalizes_the_allowlist() {
        let config = ImessageConfig::new(
            "http://127.0.0.1:7420",
            "token",
            BTreeMap::from([("+1 (415) 555-1212".to_string(), "imessage".to_string())]),
        )
        .validate()
        .expect("valid");
        assert!(config.topics.contains_key("+14155551212"));
    }

    #[test]
    fn service_selection_is_phase_one_closed() {
        assert_eq!(
            ImessageService::parse("auto").expect("auto"),
            ImessageService::Auto
        );
        assert_eq!(
            ImessageService::parse("IMESSAGE").expect("imessage"),
            ImessageService::Imessage
        );
        assert!(matches!(
            ImessageService::parse("sms"),
            Err(ImessageError::PhaseTwoService(_))
        ));
        assert!(matches!(
            ImessageService::parse("x"),
            Err(ImessageError::BadService(_))
        ));
    }

    #[test]
    fn base64_encodes_the_edge_envelope_bytes() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"heic-bytes"), "aGVpYy1ieXRlcw==");
        assert_eq!(base64_encode(&[0xff, 0x00, 0x7f]), "/wB/");
    }

    #[test]
    fn http_base_shape_accepts_loopback_and_rejects_bare_schemes() {
        assert!(is_http_base("http://127.0.0.1:7420"));
        assert!(is_http_base("https://bridge.example"));
        assert!(!is_http_base("http://"));
        assert!(!is_http_base("http:///path"));
        assert!(!is_http_base("127.0.0.1:7420"));
    }
}
