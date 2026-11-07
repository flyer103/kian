//! `GitHubChannel` — the second *real* [`ChannelAdapter`] (
//! DESIGN §4.2 GitHub row): GitHub over webhook-shaped deliveries
//! (receive) and the REST API (send) — the "group-chat message +
//! CI events" family.
//!
//! Footprint-Ladder level 2, beside `lark.rs`: plugin-hosted code over
//! the core `ChannelAdapter` seam. No core concept knows GitHub
//! exists.
//!
//! - **Receive** — push-shaped, not poll-shaped: GitHub POSTs a
//!   webhook delivery, the receiver surface verifies it
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
//!   environment (the secrets discipline). Receive needs no
//!   token (webhook reads are unauthenticated; trust comes from the
//!   receiver's HMAC check). `send: true` in the config without a
//!   token refuses to start — fail-closed, never a degraded send.
//! - **Untrusted by default** — `untrusted: true` is the config
//!   default: GitHub payloads are attacker-influible text (issue
//!   bodies, commit messages). The bit itself is applied by the
//!   subscription service's channel list at admission time
//!   (labeling, never filtering); [`GitHubChannel::untrusted`] exposes
//!   the configured default for assembly.
//! - **Provenance ** — `repo.full_name` → `group`, the config
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
use serde::{Deserialize, Serialize};
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

/// The `User-Agent` every request carries: api.github.com
/// answers UA-less requests with HTTP 403 + an HTML body, which the
/// JSON decode then fails opaquely ("poll response unreadable").
/// Applied once on the client so both the poll GETs and the send
/// POST/PATCHs (and any future request path) carry it.
pub const USER_AGENT: &str = concat!("kian/", env!("CARGO_PKG_VERSION"), " (+issue-watch)");

/// The webhook event types this adapter maps (the family). Any
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

/// The poll kind vocabulary: which REST list endpoints the
/// poll-receive mode drives. Serde names are the config spelling
/// (`issues` | `issue_comments`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollKind {
    /// `GET /repos/{o}/{r}/issues?since=…` — issues (and PRs, which
    /// the endpoint folds ) updated since the cursor.
    Issues,
    /// `GET /repos/{o}/{r}/issues/comments?since=…` — repo-wide issue
    /// comments updated since the cursor.
    IssueComments,
}

impl PollKind {
    /// The webhook event-type spelling the mapper expects (the
    /// poll items are normalized into webhook-shaped bodies, so the
    /// existing mapping — mention scan, provenance, PR detection —
    /// applies unchanged).
    pub fn event_type(self) -> &'static str {
        match self {
            Self::Issues => "issues",
            Self::IssueComments => "issue_comment",
        }
    }

    /// The REST list path (query string without `since`; the cursor
    /// parameter is appended by the poll driver).
    pub fn list_path(self, repo: &str) -> String {
        match self {
            Self::Issues => {
                format!("/repos/{repo}/issues?state=all&sort=updated&direction=desc&per_page=100")
            }
            Self::IssueComments => {
                format!("/repos/{repo}/issues/comments?sort=updated&direction=desc&per_page=100")
            }
        }
    }
}

/// The honest v1 cursor field: the REST list endpoints filter on
/// `updated_at`, and any other spelling is a config error (never a
/// silent default to a different field).
pub const POLL_SINCE_FIELD: &str = "updated_at";

/// The poll-receive configuration: `poll: {repo, interval_ms,
/// kinds, since_field, backfill_ms}` beside webhook receive. When
/// present the adapter gains a poll driver (assembly spawns it after
/// the subscription consumers): every `interval_ms` it reads the REST
/// list endpoints with a `since` cursor, maps each updated item
/// through the mapping, and fans the events out — idempotent by
/// the synthetic delivery id (`poll:<kind>:<repo>#<id>:<updated_at>`)
/// and bounded by the cursor (nothing at or before it is emitted).
///
/// Read position (ADR-013): the cursor is seedable from the
/// stream-derived resume position (`read_position` /
/// `payload_since_cursor`) at boot; a first poll with no cursor is the
/// bounded backfill window (`now - backfill_ms`). There is no
/// position table — the durable backstop is `event_id` dedupe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollConfig {
    /// The repo polled, `owner/repo`. Must be a key of the topic
    /// mapping table (the receive allowlist applies to polls too).
    pub repo: String,
    /// Milliseconds between polls. Must be > 0.
    pub interval_ms: u64,
    /// Which list endpoints to drive. Must be non-empty.
    pub kinds: BTreeSet<PollKind>,
    /// The cursor field the endpoints filter on. Must be
    /// `updated_at` (the honest v1 vocabulary).
    pub since_field: String,
    /// The bounded first-poll backfill window, milliseconds. Must be
    /// greater than 0; a fresh deployment admits at most the items
    /// updated in this window, never the repo's whole history.
    pub backfill_ms: u64,
}

impl PollConfig {
    /// Fail-closed shape validation (the topics cross-reference lives
    /// in [`GitHubConfig::validate`], where the table is in scope).
    pub fn validate(&self) -> Result<(), GitHubError> {
        let owner_repo = self.repo.split('/').collect::<Vec<_>>();
        if self.repo.trim().is_empty()
            || owner_repo.len() != 2
            || owner_repo.iter().any(|part| part.trim().is_empty())
        {
            return Err(GitHubError::InvalidPoll(format!(
                "repo must be `owner/repo`, got `{}`",
                self.repo
            )));
        }
        if self.interval_ms == 0 {
            return Err(GitHubError::InvalidPoll(
                "interval_ms must be > 0".to_string(),
            ));
        }
        if self.kinds.is_empty() {
            return Err(GitHubError::InvalidPoll(
                "kinds must name at least one of [issues, issue_comments]".to_string(),
            ));
        }
        if self.since_field != POLL_SINCE_FIELD {
            return Err(GitHubError::InvalidPoll(format!(
                "since_field must be `{POLL_SINCE_FIELD}` (the REST list endpoints' only \
                 honest v1 cursor), got `{}`",
                self.since_field
            )));
        }
        if self.backfill_ms == 0 {
            return Err(GitHubError::InvalidPoll(
                "backfill_ms must be > 0 (the first poll is bounded, never the whole \
                 repo history)"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

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
    #[error("github poll config invalid: {0}")]
    InvalidPoll(String),
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
    /// default `true`: GitHub payloads are attacker-influible
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
    /// The poll-receive mode, beside webhook receive: when
    /// present, assembly spawns the adapter's poll driver after the
    /// subscription consumers. `None` (the default) keeps the adapter
    /// exactly the shape — webhook-fed only.
    pub poll: Option<PollConfig>,
}

impl GitHubConfig {
    /// Fails closed on every degenerate field (the
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
        if let Some(poll) = &self.poll {
            poll.validate()?;
            // The polled repo must be in the topic-mapping table — the
            // receive allowlist applies to polls exactly as it does to
            // webhook deliveries (never a poll of an unmapped repo).
            if !self.topics.contains_key(&poll.repo) {
                return Err(GitHubError::InvalidPoll(format!(
                    "repo `{}` is not a key of the topic-mapping table (the table is the \
                     receive allowlist, for polls exactly as for webhooks)",
                    poll.repo
                )));
            }
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
    /// The poll-receive mode's resume cursor: the `since`
    /// timestamp of the last poll (seeded at boot from the
    /// stream-derived read position; `None` until the first poll,
    /// which then bounds itself by the backfill window). In-memory
    /// only — the durable guard is `event_id` dedupe (ADR-013's no
    /// position table).
    poll_cursor: Mutex<Option<String>>,
    /// Polls run (the poll-mode observability point).
    polls: AtomicU64,
    /// Poll events surfaced (post-dedupe).
    poll_surfaced: AtomicU64,
}

impl GitHubChannel {
    /// Builds the adapter, fail-closed on config
    /// ([`GitHubConfig::validate`]).
    pub fn new(name: impl Into<String>, config: GitHubConfig) -> Result<Self, GitHubError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            // api.github.com 403s UA-less requests — the
            // client-level default covers poll, send, and every
            // future request path in one place.
            .user_agent(USER_AGENT)
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
                poll_cursor: Mutex::new(None),
                polls: AtomicU64::new(0),
                poll_surfaced: AtomicU64::new(0),
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

    /// The configured untrusted default (advisory to assembly:
    /// it builds the subscription service's untrusted channel list).
    pub fn untrusted(&self) -> bool {
        self.inner.config.untrusted
    }

    /// How many inbound copies were dropped because a subscriber
    /// queue was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.inner.dropped_full.load(Ordering::Relaxed)
    }

    // -----------------------------------------------------------------
    // Poll-receive mode — beside webhook receive, never through
    // it: the poll driver reads the REST list endpoints with a
    // `since` cursor and fans the mapped events out through the same
    // subscribe surface the webhook path feeds.
    // -----------------------------------------------------------------

    /// The poll mode's current `since` cursor (read position), when a
    /// poll has run (or a cursor was seeded). In-memory only — the
    /// durable dedupe is admission's `event_id` idempotency.
    pub fn poll_since_cursor(&self) -> Option<String> {
        self.inner
            .poll_cursor
            .lock()
            .expect("github channel lock")
            .clone()
    }

    /// Seeds the resume cursor at boot (the stream-derived read
    /// position from the last admitted event's payload — assembly
    /// calls this before spawning the poll driver). Keeps an existing
    /// cursor when one is already set (the max wins: a restart must
    /// never replay what a previous incarnation admitted).
    pub fn seed_poll_cursor(&self, cursor: Option<String>) {
        let mut slot = self.inner.poll_cursor.lock().expect("github channel lock");
        *slot = match (&*slot, cursor) {
            (Some(held), Some(seed)) => Some(if *held >= seed { held.clone() } else { seed }),
            (held, seed) => held.clone().or(seed),
        };
    }

    /// Polls run / events surfaced (the observability pair).
    pub fn poll_counts(&self) -> (u64, u64) {
        (
            self.inner.polls.load(Ordering::Relaxed),
            self.inner.poll_surfaced.load(Ordering::Relaxed),
        )
    }

    /// Runs one poll (the poll driver's body, public so tests and
    /// assembly drive it directly): reads every configured list
    /// endpoint with the current `since` cursor, maps the updated
    /// items through the mapping with the thread-key topic,
    /// and fans each genuinely new event out. Returns how many events
    /// surfaced; the cursor advances to the newest `updated_at` seen
    /// (admitted or already-known — both bound the next poll).
    ///
    /// A first poll with no cursor bounds itself by the backfill
    /// window (`now - backfill_ms`): a fresh deployment admits at
    /// most that window, never the repo's history.
    pub async fn poll_once(&self) -> Result<usize, ChannelError> {
        let inner = &self.inner;
        let Some(poll) = inner.config.poll.as_ref() else {
            return Err(ChannelError::Unavailable(
                "github channel has no poll config — poll_once is poll-mode only".into(),
            ));
        };
        inner.polls.fetch_add(1, Ordering::Relaxed);
        // The bounded first poll: with no cursor (a fresh deployment,
        // or one whose stream derived no position) the window is
        // `now - backfill_ms`, rendered in the endpoints' RFC3339
        // shape. Subsequent polls resume at the last cursor.
        // Every kind in THIS poll reads and filters against the
        // poll-start cursor — one kind's newest item never masks
        // another kind's (the endpoints are independent lists).
        let poll_since = inner
            .poll_cursor
            .lock()
            .expect("github channel lock")
            .clone()
            .or_else(|| Some(rfc3339_ms(now_ms().saturating_sub(poll.backfill_ms))));

        let mut surfaced = 0usize;
        let mut newest = poll_since.clone();
        for kind in &poll.kinds {
            let mut path = kind.list_path(&poll.repo);
            if let Some(cursor) = &poll_since {
                path.push_str("&since=");
                path.push_str(&url_encode(cursor));
            }
            let items = fetch_list(inner, &path).await?;
            let (events, kind_newest) = poll_items_to_events(
                &inner.name,
                &inner.config,
                *kind,
                &items,
                poll_since.as_deref(),
            );
            for event in events {
                // The same dedupe ring the webhook path uses: a
                // re-polled page (unchanged `updated_at` ⇒ same event
                // id) surfaces nothing; admission's `event_id`
                // idempotency is the durable backstop.
                if !inner
                    .seen
                    .lock()
                    .expect("github channel lock")
                    .insert(&event.event_id)
                {
                    warn!(
                        target: "kian::github_channel",
                        channel = %inner.name,
                        event_id = %event.event_id,
                        "github audit: poll item re-delivered (duplicate event id), dropped"
                    );
                    continue;
                }
                fan_out(inner, event);
                surfaced += 1;
            }
            if let Some(kind_newest) = kind_newest {
                newest = Some(match newest {
                    Some(current) if current >= kind_newest => current,
                    _ => kind_newest,
                });
            }
        }
        *inner.poll_cursor.lock().expect("github channel lock") = newest;
        if surfaced > 0 {
            inner
                .poll_surfaced
                .fetch_add(surfaced as u64, Ordering::Relaxed);
        }
        Ok(surfaced)
    }

    /// Spawns the poll run loop (assembly mounts the handle for the
    /// server's lifetime; the loop is beside the webhook receiver —
    /// the surface is untouched). The first poll waits one
    /// interval (the TimerChannel discipline: assembly lands the
    /// Router bootstrap before the first event fires). A poll error
    /// is an audit line, never a crash: the next interval re-polls,
    /// and `event_id` dedupe absorbs any overlap.
    pub fn spawn_poll(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let channel = Arc::clone(self);
        let interval = Duration::from_millis(
            channel
                .inner
                .config
                .poll
                .as_ref()
                .map(|poll| poll.interval_ms)
                .unwrap_or_default()
                .max(1),
        );
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match channel.poll_once().await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(
                        target: "kian::github_channel",
                        channel = %channel.inner.name,
                        surfaced = n,
                        since = channel.poll_since_cursor().as_deref().unwrap_or(""),
                        "github poll: new events surfaced"
                    ),
                    Err(e) => warn!(
                        target: "kian::github_channel",
                        channel = %channel.inner.name,
                        error = %e,
                        "github audit: poll failed, next interval re-polls (event_id dedupe absorbs overlap)"
                    ),
                }
            }
        })
    }

    /// Ingests one **verified** webhook delivery (the receiver
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
    // provenance: the verbatim webhook `sender` object (login,
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

/// The REST `since` cursor a mapped payload carries (read
/// position): the `updated_at` timestamp of the issue or PR the
/// payload belongs to — exactly the field the REST list endpoints
/// filter on, so a restart's poll resumes at the last admitted
/// event's timestamp instead of replaying. Pure over the payload;
/// `None` when the payload belongs to no issue/PR (push, check_run,
/// status) or carries no timestamp — the caller then re-polls bounded
/// by `event_id` dedupe (ADR-013's honest v1, no position table).
pub fn payload_since_cursor(payload: &Value) -> Option<String> {
    ["/issue/updated_at", "/pull_request/updated_at"]
        .into_iter()
        .find_map(|pointer| {
            payload
                .pointer(pointer)
                .and_then(Value::as_str)
                .filter(|ts| !ts.is_empty())
                .map(str::to_string)
        })
}

// ---------------------------------------------------------------------
// Poll-receive mapping — pure functions over the REST list
// pages' items, normalized into webhook-shaped bodies so the existing
// mapping (mention scan, provenance, PR detection, allowlist)
// applies unchanged. The delivery id is synthetic but deterministic:
// `poll:<kind>:<repo>#<item id>:<updated_at>` — an unchanged item
// re-polled derives the same id and dedupes; an updated item derives
// a new one and surfaces.
// ---------------------------------------------------------------------

/// The poll-mode thread-key shape (ADR-013): the canonical topic
/// is derived from `group` + `issue.number`, so every event of one
/// issue — its opening, its comments, its edits — carries one routing
/// topic (`<channel>:<owner/repo>#<number>`) and fans out to exactly
/// the workers subscribed to that thread.
pub fn poll_thread_key_rule(channel: &str) -> crate::intake::ThreadKeyRule {
    crate::intake::ThreadKeyRule {
        channel: channel.to_string(),
        key: vec![
            crate::intake::GROUP_FIELD.to_string(),
            "issue.number".to_string(),
        ],
    }
}

/// Derives the outbox conversation keys a thread-key topic covers
/// a topic of the shape `<channel>:<owner/repo>#<number>`
/// names exactly one GitHub conversation, and every send the Router
/// makes for it targets `{kind, number, owner, repo}` for one of
/// [`SEND_KINDS`] — the same `(channel, speak_key(target))` key the
/// outbox intent carries, so the board can attribute a pending/parked
/// delivery to a worker from the topic alone (both sides derive it;
/// no freeform `correlation` needs to match). Any topic not of that
/// shape derives nothing — honest empty, never a guess.
#[must_use]
pub fn topic_conversation_keys(topic: &str) -> Vec<String> {
    let Some((channel, rest)) = topic.split_once(':') else {
        return Vec::new();
    };
    if channel.is_empty() {
        return Vec::new();
    }
    let Some((repo, number)) = rest.rsplit_once('#') else {
        return Vec::new();
    };
    let Some(number) = number.parse::<u64>().ok().filter(|n| *n > 0) else {
        return Vec::new();
    };
    let Some((owner, repo_name)) = repo.split_once('/') else {
        return Vec::new();
    };
    if owner.is_empty() || repo_name.is_empty() {
        return Vec::new();
    }
    SEND_KINDS
        .iter()
        .map(|kind| {
            crate::outbox::conversation_key(
                channel,
                &serde_json::json!({
                    "kind": kind, "number": number, "owner": owner, "repo": repo_name
                }),
            )
        })
        .collect()
}

/// The `updated_at` timestamp of one list item (`None` = the item
/// carries none — honestly skipped, never guessed).
pub fn poll_item_since(item: &Value) -> Option<&str> {
    item.get("updated_at")
        .and_then(Value::as_str)
        .filter(|ts| !ts.is_empty())
}

/// Is this item's author a human (not a Bot, not a `[bot]`-suffixed
/// app login)? The adapter-declared `explicit` signal on issue
/// authorship — a human filing an issue is an explicit request
/// for the watch's attention.
pub fn poll_author_is_human(item: &Value) -> bool {
    let Some(user) = item.get("user").and_then(Value::as_object) else {
        return false;
    };
    let is_bot_type = user.get("type").and_then(Value::as_str) == Some("Bot");
    let is_bot_login = user
        .get("login")
        .and_then(Value::as_str)
        .is_some_and(|login| login.ends_with("[bot]"));
    !is_bot_type && !is_bot_login
}

/// Normalizes one issues-list item into the webhook-shaped `issues`
/// body the mapper consumes. The action is honestly `poll` (a
/// list page cannot know opened/edited/closed — the topic thread key,
/// not the action, carries the correlation). A human author's item
/// carries the adapter-declared `explicit: true` (issue authorship is
/// the explicit signal; bot/CI senders never get it — the classes
/// table is the config-side filter).
pub fn poll_issue_body(repo_full_name: &str, item: &Value) -> Value {
    let mut body = json_body(repo_full_name, item);
    body["action"] = Value::String("poll".into());
    body["issue"] = item.clone();
    if poll_author_is_human(item) {
        body["explicit"] = Value::Bool(true);
    }
    body
}

/// Normalizes one issue-comments-list item into the webhook-shaped
/// `issue_comment` body. `None` when the item carries no parsable
/// `issue_url` (the comment's issue number is underivable — an honest
/// skip, never a guess). Comments never declare authorship-explicit:
/// a human comment is a *candidate*; only an @-mention (the existing
/// mention scan over `comment.body`) makes it explicit.
pub fn poll_comment_body(repo_full_name: &str, item: &Value) -> Option<Value> {
    let number = issue_number_from_url(
        item.get("issue_url")
            .and_then(Value::as_str)?
            .rsplit('/')
            .next()?,
    )?;
    let mut body = json_body(repo_full_name, item);
    body["action"] = Value::String("poll".into());
    body["issue"] = serde_json::json!({"number": number});
    body["comment"] = item.clone();
    Some(body)
}

/// The shared skeleton of a poll body: repository + sender.
fn json_body(repo_full_name: &str, item: &Value) -> Value {
    serde_json::json!({
        "repository": {"full_name": repo_full_name},
        "sender": item.get("user").filter(|u| u.is_object()).cloned().unwrap_or(Value::Null),
    })
}

fn issue_number_from_url(trailing: &str) -> Option<u64> {
    trailing.parse::<u64>().ok().filter(|n| *n > 0)
}

/// The deterministic delivery id of one polled item
/// (`poll:<kind>:<repo>#<item id>:<updated_at>`; the item id is the
/// issue number for the issues kind, the comment id for comments).
/// `None` when the id-bearing field or the timestamp is missing.
pub fn poll_event_id(kind: PollKind, repo: &str, item: &Value) -> Option<String> {
    let updated = poll_item_since(item)?;
    let item_id = match kind {
        PollKind::Issues => item.get("number").and_then(Value::as_u64)?,
        PollKind::IssueComments => item.get("id").and_then(Value::as_u64)?,
    };
    Some(format!(
        "poll:{}:{repo}#{item_id}:{updated}",
        kind.event_type()
    ))
}

/// Maps one REST list page's items to `ChannelEvent`s under the
/// mapping with the thread-key topic. Pure — the fixture-test
/// surface for the whole poll mapping contract:
///
/// - items at or before the `since` cursor are **not** emitted
///   (nothing older than the cursor surfaces — the cursor is honored
///   here, before any idempotency is spent);
/// - each item's topic is the derived thread key
///   (`<channel>:<owner/repo>#<number>`, one topic per issue thread);
/// - items without `updated_at`, underivable comments, unmapped repos
///   and underivable keys are honestly skipped;
/// - events return in chronological order (oldest first — the
///   conversation order of one thread);
/// - the second return value is the newest `updated_at` among the
///   page's non-skipped items (the candidate cursor).
pub fn poll_items_to_events(
    channel: &str,
    config: &GitHubConfig,
    kind: PollKind,
    items: &[Value],
    since: Option<&str>,
) -> (Vec<ChannelEvent>, Option<String>) {
    let rule = poll_thread_key_rule(channel);
    // The polled repo (validated present whenever a poll config rides
    // the config; the empty fallback never maps — `delivery_to_event`
    // drops it against the allowlist).
    let repo = config
        .poll
        .as_ref()
        .map(|poll| poll.repo.as_str())
        .unwrap_or_default();
    let mut mapped: Vec<(String, ChannelEvent)> = Vec::new();
    let mut newest: Option<String> = None;
    for item in items {
        let Some(updated) = poll_item_since(item) else {
            continue;
        };
        // The cursor is honored here: nothing at or before it is
        // emitted (GitHub's `since` filter is inclusive of ties; the
        // strictly-newer rule makes ties idempotent by construction).
        if since.is_some_and(|cursor| updated <= cursor) {
            continue;
        }
        let newest_ref = newest.get_or_insert_with(|| updated.to_string());
        if updated > newest_ref.as_str() {
            *newest_ref = updated.to_string();
        }
        let Some(event_id) = poll_event_id(kind, repo, item) else {
            continue;
        };
        let body = match kind {
            PollKind::Issues => poll_issue_body(repo, item),
            PollKind::IssueComments => match poll_comment_body(repo, item) {
                Some(body) => body,
                None => continue,
            },
        };
        let Some(mut event) =
            delivery_to_event(channel, config, kind.event_type(), &event_id, &body)
        else {
            // Unmapped repo / unreadable payload: the same honest drop
            // the webhook path takes.
            continue;
        };
        // The topic IS the derived thread key (keys are the
        // topics after derivation) — one canonical topic per issue
        // thread, regardless of what the static table maps the repo
        // to.
        match crate::intake::derive_thread_key(&rule, &event) {
            Some(key) => event.topic = Some(key.topic().to_string()),
            None => continue,
        }
        mapped.push((updated.to_string(), event));
    }
    mapped.sort_by(|a, b| a.0.cmp(&b.0));
    (mapped.into_iter().map(|(_, event)| event).collect(), newest)
}

/// Fetches one REST list page (a JSON array). Resolves only on 2xx +
/// an array body; every other outcome is an honest error.
async fn fetch_list(inner: &Inner, path: &str) -> Result<Vec<Value>, ChannelError> {
    let url = format!("{}{path}", inner.config.api_base);
    let mut request = inner
        .http
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", API_VERSION);
    // The token is optional for polls (public repos poll fine
    // anonymously); when the env provided one it rides, and a private
    // repo without one fails honestly at the API's 403/404.
    if let Some(token) = inner
        .config
        .token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| transport_error("poll", e))?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .map_err(|e| ChannelError::Unavailable(format!("poll response unreadable: {e}")))?;
    if !status.is_success() {
        return Err(ChannelError::Unavailable(format!(
            "github poll rejected: HTTP {status} path={path} message={}",
            value.get("message").and_then(Value::as_str).unwrap_or("?")
        )));
    }
    match value {
        Value::Array(items) => Ok(items),
        other => Err(ChannelError::Unavailable(format!(
            "github poll page must be a JSON array, got `{other}`"
        ))),
    }
}

/// Renders epoch milliseconds as the RFC3339 UTC timestamp the REST
/// `since` parameter expects (`2026-11-04T12:34:56Z`). Pure civil
/// arithmetic (days-from-epoch → y/m/d, Howard Hinnant's algorithm) —
/// no clock dependency, no new crate.
pub fn rfc3339_ms(epoch_ms: u64) -> String {
    let secs = epoch_ms / 1000;
    let days = (secs / 86_400) as i64;
    let time_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60
    )
}

/// Days-since-epoch → (year, month, day), the proleptic Gregorian
/// calendar (valid for any epoch value u64 can hold).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Percent-encodes the query value (RFC3339 timestamps only carry
/// `:`, which must not ride raw in a query string).
fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(ch),
            _ => out.push_str(&format!("%{:02X}", ch as u32)),
        }
    }
    out
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
/// the predicate shape)? Machine-only events (push metadata,
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
/// predicate shape).
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
            poll: None,
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

        // The fail-closed rule: send configured without a token
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
        assert!(channel.untrusted(), "github defaults to untrusted");
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
    fn supported_event_types_are_the_bundle_family() {
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

    // -----------------------------------------------------------------
    // poll-receive mode — config validation, the pure mapping,
    // and the RFC3339 cursor rendering.
    // -----------------------------------------------------------------

    fn poll_config() -> super::PollConfig {
        super::PollConfig {
            repo: "flyer103/kian".into(),
            interval_ms: 30_000,
            kinds: BTreeSet::from([super::PollKind::Issues]),
            since_field: super::POLL_SINCE_FIELD.into(),
            backfill_ms: 86_400_000,
        }
    }

    #[test]
    fn poll_configs_validate_fail_closed() {
        assert_eq!(poll_config().validate(), Ok(()));

        let mut bad = poll_config();
        bad.repo = "flyer103".into();
        assert!(matches!(
            bad.validate(),
            Err(super::GitHubError::InvalidPoll(_))
        ));

        let mut bad = poll_config();
        bad.interval_ms = 0;
        assert!(bad.validate().is_err());

        let mut bad = poll_config();
        bad.kinds.clear();
        assert!(bad.validate().is_err());

        let mut bad = poll_config();
        bad.since_field = "created_at".into();
        assert!(bad.validate().is_err());

        let mut bad = poll_config();
        bad.backfill_ms = 0;
        assert!(bad.validate().is_err());

        // The receive allowlist cross-reference: polling an unmapped
        // repo refuses to start.
        let mut cfg = config();
        let mut poll = poll_config();
        poll.repo = "someone/else".into();
        cfg.poll = Some(poll);
        assert!(cfg.validate().is_err());

        // A mapped poll config passes the whole config validation.
        let mut cfg = config();
        cfg.poll = Some(poll_config());
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn poll_items_map_through_the_c48_mapping_with_the_thread_key_topic() {
        let mut cfg = config();
        cfg.poll = Some(poll_config());
        let items = vec![
            json!({
                "number": 101,
                "title": "build broken",
                "body": "the CI log shows a panic",
                "updated_at": "2026-11-04T10:00:00Z",
                "user": {"login": "reporter", "type": "User", "id": 7},
            }),
            json!({
                "number": 102,
                "title": "bot-filed",
                "body": "automated report",
                "updated_at": "2026-11-04T10:30:00Z",
                "user": {"login": "github-actions[bot]", "type": "Bot"},
            }),
        ];
        let (events, newest) =
            super::poll_items_to_events("github", &cfg, super::PollKind::Issues, &items, None);
        assert_eq!(events.len(), 2);
        // Chronological order (oldest first).
        assert_eq!(
            events[0].event_id,
            "poll:issues:flyer103/kian#101:2026-11-04T10:00:00Z"
        );
        assert_eq!(
            events[1].event_id,
            "poll:issues:flyer103/kian#102:2026-11-04T10:30:00Z"
        );
        // The topic is the derived thread key: one canonical
        // topic per issue thread, not the static table's repo topic.
        assert_eq!(events[0].topic.as_deref(), Some("github:flyer103/kian#101"));
        assert_eq!(events[1].topic.as_deref(), Some("github:flyer103/kian#102"));
        assert_eq!(events[0].group.as_deref(), Some("flyer103/kian"));
        // The adapter-declared explicit signal on (human) issue
        // authorship; the bot's item never carries it.
        assert_eq!(events[0].payload["explicit"], json!(true));
        assert!(events[1].payload.get("explicit").is_none());
        // The sender provenance is the item's author, verbatim.
        assert_eq!(events[0].sender.as_ref().unwrap()["login"], "reporter");
        // The candidate cursor is the newest updated_at.
        assert_eq!(newest.as_deref(), Some("2026-11-04T10:30:00Z"));
    }

    #[test]
    fn poll_comments_map_to_the_issue_thread_topic() {
        let mut cfg = config();
        cfg.poll = Some(poll_config());
        let items = vec![json!({
            "id": 991,
            "body": "still reproducible on main",
            "updated_at": "2026-11-04T11:00:00Z",
            "issue_url": "https://api.github.com/repos/flyer103/kian/issues/101",
            "user": {"login": "reporter", "type": "User"},
        })];
        let (events, _) = super::poll_items_to_events(
            "github",
            &cfg,
            super::PollKind::IssueComments,
            &items,
            None,
        );
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            event.event_id,
            "poll:issue_comment:flyer103/kian#991:2026-11-04T11:00:00Z"
        );
        // The comment joins its issue's thread: the same canonical
        // topic the issue's own event carried.
        assert_eq!(event.topic.as_deref(), Some("github:flyer103/kian#101"));
        // A human comment is a candidate, never authorship-explicit.
        assert!(event.payload.get("explicit").is_none());
        // A mention in the comment body is explicit (the existing
        // mention scan over the normalized webhook shape).
        let mention = json!({
            "id": 992,
            "body": "@kian-agent please look",
            "updated_at": "2026-11-04T11:05:00Z",
            "issue_url": "https://api.github.com/repos/flyer103/kian/issues/101",
            "user": {"login": "reporter", "type": "User"},
        });
        let (events, _) = super::poll_items_to_events(
            "github",
            &cfg,
            super::PollKind::IssueComments,
            &[mention],
            None,
        );
        assert_eq!(events[0].payload["explicit"], json!(true));

        // An underivable comment (no issue_url) is honestly skipped.
        let orphan = vec![json!({
            "id": 993,
            "body": "?",
            "updated_at": "2026-11-04T11:06:00Z",
        })];
        let (events, newest) = super::poll_items_to_events(
            "github",
            &cfg,
            super::PollKind::IssueComments,
            &orphan,
            None,
        );
        assert!(events.is_empty());
        // The cursor candidate still advances (the item was seen, it
        // just maps to nothing).
        assert_eq!(newest.as_deref(), Some("2026-11-04T11:06:00Z"));
    }

    #[test]
    fn rfc3339_rendering_matches_the_github_shape() {
        assert_eq!(super::rfc3339_ms(0), "1970-01-01T00:00:00Z");
        // 2026-11-04T00:00:00Z == 1793750400 (UTC), spot-checked.
        assert_eq!(super::rfc3339_ms(1_793_750_400_000), "2026-11-04T00:00:00Z");
        assert_eq!(
            super::rfc3339_ms(1_793_750_400_123),
            "2026-11-04T00:00:00Z",
            "sub-second precision is truncated (the endpoints filter on seconds)"
        );
        // Leap-year day: 2024-02-29T12:00:00Z == 1709208000.
        assert_eq!(super::rfc3339_ms(1_709_208_000_000), "2024-02-29T12:00:00Z");
    }

    #[test]
    fn thread_key_topic_derives_the_conversation_keys_both_sides_share() {
        // a thread-key topic `<channel>:<owner/repo>#<n>`
        // names one GitHub conversation; every send the Router makes
        // for it targets `{kind, number, owner, repo}` for one of the
        // send kinds — the exact `conversation_key` the outbox intent
        // carries. The live run2 conversation string is asserted
        // byte-for-byte.
        let keys = super::topic_conversation_keys("issue-github:flyer103/kian#4");
        let conv = |kind: &str| {
            crate::outbox::conversation_key(
                "issue-github",
                &json!({"kind": kind, "number": 4, "owner": "flyer103", "repo": "kian"}),
            )
        };
        assert_eq!(
            keys,
            vec![
                conv("issue_comment"),
                conv("pr_review"),
                conv("pr_description"),
            ],
            "{keys:?}"
        );
        // The live byte shape (run2 serve.log, issue #4's parked
        // delivery) — serde_json's canonical object rendering, no
        // preserve_order, matches what the model's target serialized
        // to on the Router stream.
        assert_eq!(
            keys[0],
            "issue-github:{\"kind\":\"issue_comment\",\"number\":4,\"owner\":\"flyer103\",\"repo\":\"kian\"}"
        );
        // Anything not of the thread-key shape derives nothing —
        // honest empty, never a guess.
        for topic in [
            "ci",
            "issue-github:flyer103/kian",
            "issue-github:flyer103/kian#zero",
            "issue-github:flyer103#4",
            "issue-github:/kian#4",
            "issue-github:flyer103/#4",
            ":flyer103/kian#4",
            "issue-github:flyer103/kian#4:extra",
        ] {
            assert!(
                super::topic_conversation_keys(topic).is_empty(),
                "`{topic}` must derive no conversation"
            );
        }
    }
}
