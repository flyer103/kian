//! `GitHubChannel` — the second *real* [`ChannelAdapter`] (card C48,
//! DESIGN §4.2 GitHub row): GitHub over webhook-shaped deliveries
//! (receive) and the REST API (send) — the S4 "group-chat message +
//! CI events" family.
//!
//! Footprint-Ladder level 2, beside `lark.rs`: plugin-hosted code over
//! the core `ChannelAdapter` seam. No core concept knows GitHub
//! exists.
//!
//! - **Receive** — push-shaped, not poll-shaped: GitHub POSTs a
//!   webhook delivery, the receiver surface (C49) verifies it
//!   (HMAC-SHA256 over the raw body — verification precedes any
//!   parsing, the untrusted-by-default discipline), and hands the
//!   *verified* triple (event type, delivery id, parsed body) to
//!   [`GitHubChannel::ingest_delivery`]. The adapter maps it to a
//!   `ChannelEvent` with `event_id = <X-GitHub-Delivery id>` — the
//!   durable idempotency key, so a redelivered webhook can never
//!   double-admit (one row per delivery, exactly like lark's message
//!   id). One delivery maps to **one** event (the payload is the
//!   verbatim body); drops are honest no-ops with an audit line:
//!   unsupported event type, filtered event type, unmapped repo.
//! - **Send** — the reply half over the REST API: PR review comment
//!   (`POST /repos/{o}/{r}/pulls/{n}/reviews`), issue comment
//!   (`POST /repos/{o}/{r}/issues/{n}/comments` — issue *or* PR
//!   conversation), and PR description update (`PATCH
//!   /repos/{o}/{r}/pulls/{n}`). The future resolves only after the
//!   API accepted (2xx + an id); every other outcome is an honest
//!   [`ChannelError`].
//! - **Credentials** — the token is a constructor input, never a
//!   config-file field: assembly reads `KIAN_GITHUB_TOKEN` from the
//!   environment (the C13/C44 secrets discipline). Receive needs no
//!   token (webhook reads are unauthenticated; trust comes from the
//!   receiver's HMAC check). `send: true` in the config without a
//!   token refuses to start — fail-closed, never a degraded send.
//! - **Untrusted by default** — `untrusted: true` is the config
//!   default (C43): GitHub payloads are attacker-influible text (issue
//!   bodies, commit messages). The bit itself is applied by the
//!   subscription service's channel list at admission time
//!   (labeling, never filtering); [`GitHubChannel::untrusted`] exposes
//!   the configured default for assembly.
//! - **Provenance (C36)** — `repo.full_name` → `group`, the config
//!   topic table → `topic` (`owner/repo` key, or an exact
//!   `owner/repo#N` PR override when the payload is PR-scoped), the
//!   verbatim `sender` object → `sender`, and an adapter-declared
//!   top-level `explicit: true` when a human-authored text field
//!   (comment/issue/PR body or title, commit message) carries an
//!   @-mention — the machine-only events (push metadata, check_run,
//!   status) never declare explicit.
//!
//! Output policy is adapter-agnostic by contract: the DESIGN §4.2
//! GitHub row (structured problem/changes/verification) shapes
//! [`ChannelDelivery::content`] upstream through the existing
//! `StructuredPolicy` gate; this adapter renders what it is given
//! (object content renders each key as a markdown section) and
//! delivers it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt};
use serde_json::Value;
use tracing::warn;

/// The channel `type` spelling that builds a [`GitHubChannel`]
/// (kian-cli cross-references this constant).
pub const GITHUB_CHANNEL_TYPE: &str = "github";

/// GitHub REST API base, overridable for GitHub Enterprise Server
/// (`https://ghe.example.com/api/v3`).
pub const DEFAULT_API_BASE: &str = "https://api.github.com";

/// The REST API version pin (`X-GitHub-Api-Version`).
pub const API_VERSION: &str = "2022-11-28";

/// The webhook event types this adapter maps (the S4 family). Any
/// other `X-GitHub-Event` is an honest drop with an audit line —
/// never a best-effort parse.
pub const SUPPORTED_EVENT_TYPES: [&str; 6] = [
    "issues",
    "issue_comment",
    "pull_request",
    "push",
    "check_run",
    "status",
];

/// Per-subscriber inbound queue bound (the MockChannel discipline: a
/// slow consumer's copy is dropped with an audit line, never a block).
pub const SUBSCRIBER_CAPACITY: usize = 256;

/// How many distinct delivery ids the adapter remembers (the in-memory
/// dedupe ring; the durable guard is admission's `event_id`
/// idempotency, so this only bounds re-surfacing between ingests).
const SEEN_RING_CAPACITY: usize = 4096;

/// HTTP timeout for every GitHub API call.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// The send-target `kind` vocabulary (adapter-defined; opaque to
/// everything upstream).
const KIND_ISSUE_COMMENT: &str = "issue_comment";
const KIND_PR_REVIEW: &str = "pr_review";
const KIND_PR_DESCRIPTION: &str = "pr_description";
const SEND_KINDS: [&str; 3] = [KIND_ISSUE_COMMENT, KIND_PR_REVIEW, KIND_PR_DESCRIPTION];

/// Honest construction/config failures (fail-closed, constraint 7: a
/// bad github config refuses to start, never silently degrades).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GitHubError {
    #[error("github api base must be an http(s) URL, got `{0}`")]
    BadApiBase(String),
    #[error("github topic-mapping table must declare at least one repo")]
    NoTopics,
    #[error("github topic-mapping key/value must not be empty (`{0}`)")]
    EmptyTopicEntry(String),
    #[error(
        "github events filter entry `{0}` is not a supported event type (supported: {SUPPORTED_EVENT_TYPES:?})"
    )]
    UnknownEventFilter(String),
    #[error(
        "github send is configured but KIAN_GITHUB_TOKEN is absent/blank — send requires a token, receive does not (fail-closed, never a degraded send)"
    )]
    SendWithoutToken,
}

/// The runtime github channel config (token-bearing by construction —
/// the wire layer in kian-cli fills `token` from the environment; it
/// never lives in `kian.yaml`).
#[derive(Debug, Clone, PartialEq)]
pub struct GitHubConfig {
    /// REST API base (api.github.com or a GHE server).
    pub api_base: String,
    /// Topic mapping table: `owner/repo` → topic, optionally
    /// narrowed per PR with an exact `owner/repo#N` → topic key (the
    /// PR override wins when the payload is PR-scoped). The keys are
    /// also the receive allowlist: a delivery from an unmapped repo
    /// is an honest drop.
    pub topics: BTreeMap<String, String>,
    /// Webhook event-type filter. Empty (the default) = every
    /// supported type surfaces; a non-empty set admits only those
    /// types (fail-closed on unknown entries).
    pub events: BTreeSet<String>,
    /// C43 default `true`: GitHub payloads are attacker-influible
    /// text. Advisory to assembly (it builds the subscription
    /// service's untrusted channel list); the adapter itself never
    /// filters on it.
    pub untrusted: bool,
    /// Is send configured? When true the token is required at
    /// construction (fail-closed); a receive-only channel constructs
    /// fine without one and `send` errors honestly if called.
    pub send: bool,
    /// Env-only credential (`KIAN_GITHUB_TOKEN`, filled by assembly).
    /// Receive needs none.
    pub token: Option<String>,
}

impl GitHubConfig {
    /// Fails closed on every degenerate field (the C13/C44/C48
    /// discipline: an unusable github config is a startup error,
    /// never a default).
    pub fn validate(&self) -> Result<(), GitHubError> {
        if !(self.api_base.starts_with("http://") || self.api_base.starts_with("https://")) {
            return Err(GitHubError::BadApiBase(self.api_base.clone()));
        }
        if self.topics.is_empty() {
            return Err(GitHubError::NoTopics);
        }
        for (repo, topic) in &self.topics {
            if repo.trim().is_empty() || topic.trim().is_empty() {
                return Err(GitHubError::EmptyTopicEntry(format!("{repo}→{topic}")));
            }
        }
        for event in &self.events {
            if !SUPPORTED_EVENT_TYPES.contains(&event.as_str()) {
                return Err(GitHubError::UnknownEventFilter(event.clone()));
            }
        }
        if self.send && !self.token.as_deref().is_some_and(|t| !t.trim().is_empty()) {
            return Err(GitHubError::SendWithoutToken);
        }
        Ok(())
    }

    /// Does the events filter admit this webhook event type? (Empty
    /// filter = all supported types.)
    fn event_allowed(&self, event_type: &str) -> bool {
        self.events.is_empty() || self.events.contains(event_type)
    }

    /// Resolves the routing topic for a repo: the exact
    /// `owner/repo#N` PR override when the payload is PR-scoped and
    /// mapped, else the `owner/repo` entry, else `None` (an honest
    /// drop — the table is the receive allowlist).
    fn topic_for(&self, repo: &str, pr_number: Option<u64>) -> Option<&str> {
        if let Some(number) = pr_number
            && let Some(topic) = self.topics.get(&format!("{repo}#{number}"))
        {
            return Some(topic.as_str());
        }
        self.topics.get(repo).map(String::as_str)
    }
}

/// The GitHub channel adapter. Cheap to clone (an `Arc` inside);
/// share clones, not deep copies.
#[derive(Clone)]
pub struct GitHubChannel {
    inner: Arc<Inner>,
}

struct Inner {
    /// The configured channel name reported on every event.
    name: String,
    config: GitHubConfig,
    http: reqwest::Client,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    dropped_full: AtomicU64,
    /// In-memory delivery-id dedupe (bounded); the durable guard is
    /// admission's `event_id` idempotency.
    seen: Mutex<DedupeRing>,
}

impl GitHubChannel {
    /// Builds the adapter, fail-closed on config
    /// ([`GitHubConfig::validate`]).
    pub fn new(name: impl Into<String>, config: GitHubConfig) -> Result<Self, GitHubError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| GitHubError::BadApiBase(format!("http client: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.into(),
                config,
                http,
                subscribers: Mutex::new(Vec::new()),
                dropped_full: AtomicU64::new(0),
                seen: Mutex::new(DedupeRing::new(SEEN_RING_CAPACITY)),
            }),
        })
    }

    /// The configured channel name.
    pub fn channel_name(&self) -> &str {
        &self.inner.name
    }

    /// The runtime config (assembly/tests; the token is included —
    /// never log this).
    pub fn config(&self) -> &GitHubConfig {
        &self.inner.config
    }

    /// The configured C43 untrusted default (advisory to assembly:
    /// it builds the subscription service's untrusted channel list).
    pub fn untrusted(&self) -> bool {
        self.inner.config.untrusted
    }

    /// How many inbound copies were dropped because a subscriber
    /// queue was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.inner.dropped_full.load(Ordering::Relaxed)
    }

    /// Ingests one **verified** webhook delivery (the C49 receiver
    /// surface calls this after its HMAC check — parse only what
    /// cryptographically authenticated). Maps the delivery to at most
    /// one `ChannelEvent` and fans it out to every live subscriber.
    /// Returns how many events surfaced (0 or 1); every drop is an
    /// audit line, never a silent swallow.
    pub fn ingest_delivery(&self, event_type: &str, delivery_id: &str, body: &Value) -> usize {
        let inner = &self.inner;
        let audit = |reason: &str| {
            warn!(
                target: "kian::github_channel",
                channel = %inner.name,
                event_type = %event_type,
                delivery_id = %delivery_id,
                reason,
                "github audit: delivery dropped"
            );
        };
        if !SUPPORTED_EVENT_TYPES.contains(&event_type) {
            audit("unsupported event type");
            return 0;
        }
        if !inner.config.event_allowed(event_type) {
            audit("filtered event type");
            return 0;
        }
        // Adapter-side dedupe first: a redelivered webhook never even
        // re-maps (admission's event_id guard is the durable backstop).
        if !inner
            .seen
            .lock()
            .expect("github channel lock")
            .insert(delivery_id)
        {
            audit("duplicate delivery id");
            return 0;
        }
        let Some(event) =
            delivery_to_event(&inner.name, &inner.config, event_type, delivery_id, body)
        else {
            audit("repo not in the topic-mapping table (or repo unreadable)");
            return 0;
        };
        fan_out(inner, event);
        1
    }
}

impl ChannelAdapter for GitHubChannel {
    /// Receive is push-shaped: the stream stays quiet until
    /// [`GitHubChannel::ingest_delivery`] fans events in (no poll
    /// loop — GitHub delivers to us).
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_CAPACITY);
        self.inner
            .subscribers
            .lock()
            .expect("github channel lock")
            .push(tx);
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

/// Maps one verified webhook delivery to a `ChannelEvent` (`None` =
/// honest drop: repo unreadable or unmapped). Pure — the unit-test
/// surface for the mapping contract (the event-type filter and the
/// unsupported-type guard live one level up, in
/// [`GitHubChannel::ingest_delivery`]).
pub fn delivery_to_event(
    channel: &str,
    config: &GitHubConfig,
    event_type: &str,
    delivery_id: &str,
    body: &Value,
) -> Option<ChannelEvent> {
    let repo = body
        .pointer("/repository/full_name")
        .and_then(Value::as_str)
        .filter(|r| !r.is_empty())?;
    let pr_number = payload_pr_number(event_type, body);
    let topic = config.topic_for(repo, pr_number)?;
    // C36 provenance: the verbatim webhook `sender` object (login,
    // type, …) — machine events without one stay `None`.
    let sender = body.get("sender").filter(|s| s.is_object()).cloned();
    let group = repo.to_string();
    // The adapter-declared explicit signal: an @-mention in a
    // human-authored text field. CI-train events never declare it.
    let mut payload = body.clone();
    if payload_has_mention(event_type, body) {
        payload["explicit"] = Value::Bool(true);
    }
    Some(ChannelEvent {
        channel: channel.to_string(),
        event_id: delivery_id.to_string(),
        topic: Some(topic.to_string()),
        sender,
        group: Some(group),
        payload,
        ts_ms: now_ms(),
    })
}

/// Is this payload PR-scoped, and with which number? `pull_request`
/// events carry `pull_request.number`; an `issues`/`issue_comment`
/// payload belongs to a PR when its `issue` carries the nested
/// `pull_request` object.
fn payload_pr_number(event_type: &str, body: &Value) -> Option<u64> {
    match event_type {
        "pull_request" => body.pointer("/pull_request/number").and_then(Value::as_u64),
        "issues" | "issue_comment" if body.pointer("/issue/pull_request").is_some() => {
            body.pointer("/issue/number").and_then(Value::as_u64)
        }
        _ => None,
    }
}

/// Does any human-authored text field of this payload carry an
/// @-mention ('@' followed by at least one non-whitespace character —
/// the C36 predicate shape)? Machine-only events (push metadata,
/// check_run, status) never declare explicit.
fn payload_has_mention(event_type: &str, body: &Value) -> bool {
    let mut texts: Vec<&str> = Vec::new();
    match event_type {
        "issue_comment" => {
            if let Some(body) = body.pointer("/comment/body").and_then(Value::as_str) {
                texts.push(body);
            }
        }
        "issues" | "pull_request" => {
            let root = if event_type == "pull_request" {
                "/pull_request"
            } else {
                "/issue"
            };
            for field in ["title", "body"] {
                let pointer = format!("{root}/{field}");
                if let Some(text) = body.pointer(&pointer).and_then(Value::as_str) {
                    texts.push(text);
                }
            }
        }
        "push" => {
            // Commit messages are author-written; a "@bot please look"
            // there is an explicit request too.
            if let Some(commits) = body.get("commits").and_then(Value::as_array) {
                for commit in commits {
                    if let Some(message) = commit.get("message").and_then(Value::as_str) {
                        texts.push(message);
                    }
                }
            }
        }
        _ => {}
    }
    texts.iter().any(|text| text_has_at_mention(text))
}

/// `@` followed by at least one non-whitespace character (the shared
/// C36 predicate shape).
fn text_has_at_mention(text: &str) -> bool {
    let chars: Vec<char> = text.chars().collect();
    (0..chars.len())
        .any(|i| chars[i] == '@' && chars.get(i + 1).is_some_and(|next| !next.is_whitespace()))
}

/// Fans one event out to every live subscriber (bounded `try_send`;
/// a full queue drops that subscriber's copy with an audit line — a
/// slow consumer never blocks the ingest path).
fn fan_out(inner: &Inner, event: ChannelEvent) {
    let mut subscribers = inner.subscribers.lock().expect("github channel lock");
    subscribers.retain(|tx| !tx.is_closed());
    for tx in subscribers.iter() {
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                inner.dropped_full.fetch_add(1, Ordering::Relaxed);
                warn!(
                    target: "kian::github_channel",
                    channel = %inner.name,
                    event_id = %event.event_id,
                    "github audit: subscriber queue full, event copy dropped"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Pruned on this pass; nothing to do.
            }
        }
    }
}

/// Sends one delivery through the REST API. Resolves only on the
/// API's acceptance (2xx + an id); anything else is an honest error.
async fn send_delivery(inner: &Inner, out: &ChannelDelivery) -> Result<SendReceipt, ChannelError> {
    let Some(token) = inner
        .config
        .token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    else {
        return Err(ChannelError::DeliveryFailed(
            "github channel is receive-only: KIAN_GITHUB_TOKEN is not set (send needs a token, \
             receive does not)"
                .into(),
        ));
    };
    let Some(target) = out.target.as_object() else {
        return Err(ChannelError::DeliveryFailed(format!(
            "github target must be an object {{owner, repo, number, kind}}, got `{}`",
            out.target
        )));
    };
    let owner = target.get("owner").and_then(Value::as_str).unwrap_or("");
    let repo = target.get("repo").and_then(Value::as_str).unwrap_or("");
    let number = target.get("number").and_then(Value::as_u64).unwrap_or(0);
    let kind = target.get("kind").and_then(Value::as_str).unwrap_or("");
    if owner.is_empty() || repo.is_empty() || number == 0 {
        return Err(ChannelError::DeliveryFailed(format!(
            "github target needs non-empty `owner`/`repo` and a positive `number`, got `{target:?}`"
        )));
    }
    if !SEND_KINDS.contains(&kind) {
        return Err(ChannelError::DeliveryFailed(format!(
            "github target `kind` must be one of {SEND_KINDS:?}, got `{kind}`"
        )));
    }
    let body_text = render_markdown(&out.content)?;

    // (method, path, request body) per the send-kind vocabulary.
    let (method, path, request_body) = match kind {
        KIND_PR_REVIEW => (
            reqwest::Method::POST,
            format!("/repos/{owner}/{repo}/pulls/{number}/reviews"),
            serde_json::json!({"body": body_text, "event": "COMMENT"}),
        ),
        KIND_PR_DESCRIPTION => (
            reqwest::Method::PATCH,
            format!("/repos/{owner}/{repo}/pulls/{number}"),
            serde_json::json!({"body": body_text}),
        ),
        // The issue-comment endpoint serves both issues and PR
        // conversations (GitHub's documented mapping).
        _ => (
            reqwest::Method::POST,
            format!("/repos/{owner}/{repo}/issues/{number}/comments"),
            serde_json::json!({"body": body_text}),
        ),
    };

    let url = format!("{}{path}", inner.config.api_base);
    let response = inner
        .http
        .request(method, &url)
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", API_VERSION)
        .json(&request_body)
        .send()
        .await
        .map_err(|e| transport_error("send", e))?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .map_err(|e| ChannelError::Unavailable(format!("send response unreadable: {e}")))?;
    if !status.is_success() {
        return Err(ChannelError::DeliveryFailed(format!(
            "github send rejected: HTTP {status} path={path} message={}",
            value.get("message").and_then(Value::as_str).unwrap_or("?")
        )));
    }
    let id = value.get("id").and_then(Value::as_u64);
    let Some(id) = id else {
        // Accepted status without an id is not acceptance.
        return Err(ChannelError::DeliveryFailed(format!(
            "github send accepted but returned no id: path={path} body={value}"
        )));
    };
    Ok(SendReceipt {
        message_id: id.to_string(),
        ts_ms: now_ms(),
    })
}

/// Renders the policy-shaped content to the markdown string the REST
/// API carries: a string passes through verbatim; an object renders
/// each key as a `## key` section (the StructuredPolicy
/// problem/changes/verification shape becomes a three-section
/// comment); every other shape is an honest error before any wire
/// traffic.
fn render_markdown(content: &Value) -> Result<String, ChannelError> {
    match content {
        Value::String(text) => Ok(text.clone()),
        Value::Object(map) => {
            let mut sections: Vec<String> = Vec::with_capacity(map.len());
            for (key, value) in map {
                let text = match value {
                    Value::String(s) => s.clone(),
                    other => serde_json::to_string(other).map_err(|e| {
                        ChannelError::DeliveryFailed(format!("content unserializable: {e}"))
                    })?,
                };
                sections.push(format!("## {key}\n\n{text}"));
            }
            Ok(sections.join("\n\n"))
        }
        other => Err(ChannelError::DeliveryFailed(format!(
            "github content must be a string or a section object, got `{other}`"
        ))),
    }
}

/// Bounded insert-order dedupe over delivery ids: a set plus the
/// insertion queue that evicts the oldest when the capacity is hit.
struct DedupeRing {
    order: std::collections::VecDeque<String>,
    set: BTreeSet<String>,
    capacity: usize,
}

impl DedupeRing {
    fn new(capacity: usize) -> Self {
        Self {
            order: std::collections::VecDeque::new(),
            set: BTreeSet::new(),
            capacity: capacity.max(1),
        }
    }

    /// True iff the id was not seen before (and is now remembered).
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        if self.order.len() == self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        self.order.push_back(id.to_string());
        self.set.insert(id.to_string())
    }
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
    use super::{
        DedupeRing, GitHubChannel, GitHubConfig, GitHubError, SUPPORTED_EVENT_TYPES,
        delivery_to_event,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn config() -> GitHubConfig {
        GitHubConfig {
            api_base: "https://api.github.com".into(),
            topics: BTreeMap::from([("flyer103/kian".to_string(), "ci".to_string())]),
            events: BTreeSet::new(),
            untrusted: true,
            send: false,
            token: None,
        }
    }

    #[test]
    fn degenerate_configs_fail_closed() {
        let cfg = config();
        assert_eq!(cfg.validate(), Ok(()));
        assert!(GitHubChannel::new("github", config()).is_ok());

        let mut bad = cfg.clone();
        bad.api_base = "ftp://ghe".into();
        assert_eq!(
            bad.validate(),
            Err(GitHubError::BadApiBase("ftp://ghe".into()))
        );

        let mut bad = cfg.clone();
        bad.topics.clear();
        assert_eq!(bad.validate(), Err(GitHubError::NoTopics));

        let mut bad = cfg.clone();
        bad.topics.insert("".into(), "ci".into());
        assert!(matches!(
            bad.validate(),
            Err(GitHubError::EmptyTopicEntry(_))
        ));

        let mut bad = cfg.clone();
        bad.events.insert("pull_request_review".into());
        assert_eq!(
            bad.validate(),
            Err(GitHubError::UnknownEventFilter(
                "pull_request_review".into()
            ))
        );

        // The C48 fail-closed rule: send configured without a token
        // refuses to start…
        let mut bad = cfg.clone();
        bad.send = true;
        assert_eq!(bad.validate(), Err(GitHubError::SendWithoutToken));

        let mut bad = cfg.clone();
        bad.send = true;
        bad.token = Some("  ".into());
        assert_eq!(bad.validate(), Err(GitHubError::SendWithoutToken));

        // …while a token unblocks it and receive-only stays tokenless.
        let mut send_cfg = bad;
        send_cfg.token = Some("ghp_x".into());
        assert_eq!(send_cfg.validate(), Ok(()));
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn constructor_reports_name_and_untrusted_default() {
        let channel = GitHubChannel::new("github", config()).expect("valid config");
        assert_eq!(channel.channel_name(), "github");
        assert!(channel.untrusted(), "C43: github defaults to untrusted");
        assert_eq!(channel.dropped_full(), 0);
    }

    fn issue_comment_delivery(body_text: &str) -> (String, serde_json::Value) {
        (
            "issue_comment".into(),
            json!({
                "action": "created",
                "issue": {
                    "number": 9,
                    "title": "build broken on main",
                    "body": format!("the CI log shows {body_text}"),
                    "user": {"login": "reporter"}
                },
                "comment": {
                    "id": 99,
                    "body": body_text,
                    "user": {"login": "reporter"}
                },
                "repository": {
                    "id": 1,
                    "full_name": "flyer103/kian",
                    "name": "kian"
                },
                "sender": {
                    "login": "reporter",
                    "type": "User",
                    "id": 7
                }
            }),
        )
    }

    #[test]
    fn issue_comment_fixture_maps_to_a_provenance_carrying_event() {
        let (event_type, body) = issue_comment_delivery("build broke after #123 landed");
        let cfg = config();
        let event =
            delivery_to_event("github", &cfg, &event_type, "d-abc123", &body).expect("maps");
        assert_eq!(event.channel, "github");
        // The delivery id is the idempotency key (never the comment
        // id — one delivery, one durable row).
        assert_eq!(event.event_id, "d-abc123");
        assert_eq!(event.topic.as_deref(), Some("ci"));
        assert_eq!(event.group.as_deref(), Some("flyer103/kian"));
        assert_eq!(
            event.sender,
            Some(json!({"login": "reporter", "type": "User", "id": 7}))
        );
        // Verbatim payload, no adapter fields (no mention in body).
        assert_eq!(event.payload, body);
        assert!(event.payload.get("explicit").is_none());
        assert!(
            event.ts_ms > 0,
            "webhook bodies carry no clock; ts is ingest time"
        );
    }

    #[test]
    fn mention_bodies_gain_the_adapter_declared_explicit_flag() {
        for text in ["@kian-agent please fix", "please @ kian no", "no mention"] {
            let (event_type, body) = issue_comment_delivery(text);
            let event =
                delivery_to_event("github", &config(), &event_type, "d-m", &body).expect("maps");
            let expected = text.contains("@kian-agent");
            assert_eq!(
                event.payload.get("explicit") == Some(&serde_json::Value::Bool(true)),
                expected,
                "explicit signal for {text:?}"
            );
        }
        // An @-mention in a commit message (push) counts too.
        let push = json!({
            "commits": [{"message": "revert: @kian-agent take a look"}],
            "repository": {"full_name": "flyer103/kian"},
            "sender": {"login": "pusher", "type": "User"}
        });
        let event = delivery_to_event("github", &config(), "push", "d-push", &push).expect("maps");
        assert_eq!(event.payload["explicit"], json!(true));
    }

    #[test]
    fn machine_events_never_declare_explicit() {
        for event_type in ["push", "check_run", "status"] {
            let body = json!({
                "repository": {"full_name": "flyer103/kian"},
                "sender": {"login": "github-actions", "type": "Bot"}
            });
            let event =
                delivery_to_event("github", &config(), event_type, "d-ci", &body).expect("maps");
            assert!(event.payload.get("explicit").is_none(), "{event_type}");
        }
        // A status delivery without a sender stays sender-less (None),
        // never a fabricated identity.
        let status = json!({
            "state": "success",
            "context": "ci/linux",
            "repository": {"full_name": "flyer103/kian"}
        });
        let event =
            delivery_to_event("github", &config(), "status", "d-st", &status).expect("maps");
        assert_eq!(event.sender, None);
        assert_eq!(event.topic.as_deref(), Some("ci"));
        assert_eq!(event.group.as_deref(), Some("flyer103/kian"));
    }

    #[test]
    fn pr_scoped_payload_resolves_the_repo_hash_pr_topic_override() {
        let mut cfg = config();
        cfg.topics
            .insert("flyer103/kian#9".to_string(), "pr-9".to_string());
        let pr_event = json!({
            "action": "synchronize",
            "pull_request": {"number": 9, "title": "fix", "body": ""},
            "repository": {"full_name": "flyer103/kian"},
            "sender": {"login": "contributor", "type": "User"}
        });
        let event =
            delivery_to_event("github", &cfg, "pull_request", "d-1", &pr_event).expect("maps");
        assert_eq!(event.topic.as_deref(), Some("pr-9"), "the #9 override wins");

        // A PR conversation comment (issue_comment on a PR) resolves
        // the same override via the nested pull_request object.
        let pr_comment = json!({
            "action": "created",
            "issue": {"number": 9, "pull_request": {"url": "https://api.github.com/repos/flyer103/kian/pulls/9"}},
            "comment": {"body": "rebased"},
            "repository": {"full_name": "flyer103/kian"},
            "sender": {"login": "contributor", "type": "User"}
        });
        let event =
            delivery_to_event("github", &cfg, "issue_comment", "d-2", &pr_comment).expect("maps");
        assert_eq!(event.topic.as_deref(), Some("pr-9"));

        // An unmapped PR number falls back to the repo topic.
        let other_pr = json!({
            "action": "opened",
            "pull_request": {"number": 10, "title": "other", "body": ""},
            "repository": {"full_name": "flyer103/kian"},
            "sender": {"login": "contributor", "type": "User"}
        });
        let event =
            delivery_to_event("github", &cfg, "pull_request", "d-3", &other_pr).expect("maps");
        assert_eq!(event.topic.as_deref(), Some("ci"));

        // A plain issue comment never resolves a PR override (its
        // issue has no nested pull_request).
        let (event_type, plain) = issue_comment_delivery("plain issue");
        let event = delivery_to_event("github", &cfg, &event_type, "d-4", &plain).expect("maps");
        assert_eq!(event.topic.as_deref(), Some("ci"));
    }

    #[test]
    fn unmapped_repo_and_unreadable_payload_drop_honestly() {
        let cfg = config();
        let stranger = json!({
            "action": "opened",
            "issue": {"number": 1, "title": "t", "body": "b"},
            "repository": {"full_name": "someone/else"},
            "sender": {"login": "x", "type": "User"}
        });
        assert!(delivery_to_event("github", &cfg, "issues", "d-x1", &stranger).is_none());

        let no_repo = json!({"action": "opened", "sender": {"login": "x"}});
        assert!(delivery_to_event("github", &cfg, "issues", "d-x2", &no_repo).is_none());

        let repo_only = json!({"repository": {"name": "kian"}});
        assert!(
            delivery_to_event("github", &cfg, "issues", "d-x3", &repo_only).is_none(),
            "a repository without full_name is unreadable, not guessed"
        );
    }

    #[test]
    fn supported_event_types_are_the_s4_family() {
        assert_eq!(
            SUPPORTED_EVENT_TYPES,
            [
                "issues",
                "issue_comment",
                "pull_request",
                "push",
                "check_run",
                "status"
            ]
        );
    }

    #[test]
    fn dedupe_ring_dedupes_and_evicts_at_capacity() {
        let mut ring = DedupeRing::new(2);
        assert!(ring.insert("a"));
        assert!(!ring.insert("a"));
        assert!(ring.insert("b"));
        assert!(ring.insert("c")); // evicts "a"
        assert!(ring.insert("a")); // "a" is new again
        assert!(!ring.insert("c"));
    }
}
