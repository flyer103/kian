//! Outbox bounded retry (card C62, DESIGN §4.2 + §4.7): stored work
//! for the **write side** — reference 003's rule ("outgoing messages
//! are stored and sent as stored work, retried with backoff … a
//! message waits while an earlier one of the same Ticket to the same
//! conversation has not gone out, until that one has failed three
//! times in a row and shows as a problem").
//!
//! ## Delivery-kind reuse audit (the card's Step-1 gate)
//!
//! Three retry surfaces already exist; none is reusable here:
//!
//! 1. `lark.rs` refreshes a stale tenant token **once** and retries
//!    inside the adapter (auth-boundary, transport-specific, no
//!    schedule, no ordering, no park). The outbox wraps the *adapter
//!    boundary* — reuse would embed Feishu auth in the delivery
//!    domain.
//! 2. The loop-level model backoff (`kian-loop` recovery policy,
//!    `BackoffRetry`) retries *provider turns*, not tool sends; it is
//!    per-turn and disposable.
//! 3. `subscriptions.rs` does not retry a failed mid-set admit — it
//!    leaves the subscriber to the **adapter's idempotent
//!    re-delivery** (deduped per `(session, event_id)`). The read
//!    side has a re-sender; the write side has none — that absence is
//!    exactly what this module supplies.
//!
//! **Verdict (the expected one): the stream already carries intent
//! and receipt.** `tool/call` for `reply`/`deliver_structured` is
//! persisted *before* execution (log-before-execute, the loop's
//! invariant 1) with `content.{id, name, arguments}` verbatim;
//! `channel/delivered` lands only after a successful send; and
//! `tool/result` pairs back by `content.call_id`. The outbox is
//! therefore a **disposable projection over that triple** plus a
//! bounded in-process retry driver — no new durable state, no new
//! event kinds (a durable outbox table would be pre-event state
//! needing ADR-014 and a plan amendment; explicitly not taken).
//!
//! ## Rebuild classification (why `tool/result` is load-bearing)
//!
//! A bare intent + error result is ambiguous between a C58
//! speak-suppression (never sent, never outbox work) and a parked
//! delivery. The park tool error carries the stable prefix
//! [`PARK_ERROR_PREFIX`] (the C57 stable-vocabulary precedent), so a
//! rebuild classifies each intent exactly:
//!
//! | `tool/result` for the intent | State | Re-driven at boot? |
//! |---|---|---|
//! | none yet | [`OutboxState::Pending`] (in-flight; kill -9 mid-retry) | **yes** (at-least-once) |
//! | error, output starts `outbox park:` | [`OutboxState::Parked`] | no — visible, a person releases |
//! | error, other text | suppressed / invalid args / legacy single failure | no (re-driving would duplicate suppressed sends) |
//! | ok | delivered (its `channel/delivered` exists) | no |
//!
//! ## Ordering and identity
//!
//! - **Conversation** = `(channel, speak_key(target))` — the C58
//!   canonicalization reused verbatim.
//! - **Ticket** = the intent's `correlation` tool argument (optional,
//!   additive — the same vocabulary `spawn_worker` documents), the
//!   only ticket identity that survives a restart rebuild without new
//!   durable state; absent ⇒ the Router session itself is the ticket.
//! - Per `(ticket, conversation)` the driver serializes with a fair
//!   per-key mutex held across one [`OutboxDriver::deliver_with_retry`]
//!   call: a later message of the same key waits for the earlier one
//!   (its ack precedes its result, 003's rule); every other key —
//!   other tickets, other conversations — never waits.
//!
//! ## Attempt accounting
//!
//! New deliveries under an enabled outbox stamp additive `attempt`
//! and `first_attempt_ms` keys on the `channel/delivered` payload
//! (the C39 additive-marker precedent; the C4/C18 schemas stay frozen
//! — legacy receipts parse unchanged, and with the outbox off the
//! payload bytes are identical to today's).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::seams::{ChannelDelivery, ChannelError, SendReceipt};
use serde_json::Value;
use uuid::Uuid;

use crate::StreamPort;
use crate::policy::{
    ImPolicy, StructuredPolicy, im_delivered_payload, speak_key, structured_delivered_payload,
};
use crate::router::{DELIVER_STRUCTURED_TOOL, REPLY_TOOL};
use crate::stage::OutboxView;

/// Default attempt bound (the first attempt plus 5 retries).
pub const DEFAULT_MAX_ATTEMPTS: u32 = 6;

/// Backoff starts here after the first failure (003: "2 seconds,
/// doubling").
pub const DEFAULT_BACKOFF_START_MS: u64 = 2_000;

/// Backoff ceiling (003: "up to a minute").
pub const DEFAULT_BACKOFF_CAP_MS: u64 = 60_000;

/// The stable textual prefix of a park tool error — the rebuild's
/// disambiguator between "parked after bounded retries" and every
/// other honest send error (speak-suppression, invalid args, the
/// legacy single-attempt failure). Parse nothing else out of it.
pub const PARK_ERROR_PREFIX: &str = "outbox park:";

/// The outbox retry configuration (`superagent.outbox`). An **absent**
/// section is a disabled outbox — the send path is byte-identical to
/// the pre-C62 behavior (one attempt, the model may retry in-turn).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxConfig {
    /// Total attempts per delivery before parking (≥ 1).
    pub max_attempts: u32,
    /// Backoff after the first failure, milliseconds (≥ 1).
    pub backoff_start_ms: u64,
    /// Backoff ceiling, milliseconds (≥ start).
    pub backoff_cap_ms: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            backoff_start_ms: DEFAULT_BACKOFF_START_MS,
            backoff_cap_ms: DEFAULT_BACKOFF_CAP_MS,
        }
    }
}

impl OutboxConfig {
    /// Fail-closed validation (assembly refuses to start on a broken
    /// schedule, never silently repairs it).
    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts < 1 {
            return Err("max_attempts must be at least 1".into());
        }
        if self.backoff_start_ms == 0 {
            return Err("backoff_start_ms must be at least 1".into());
        }
        if self.backoff_cap_ms < self.backoff_start_ms {
            return Err("backoff_cap_ms must be at least backoff_start_ms".into());
        }
        Ok(())
    }

    /// The backoff after attempt `n` fails: `start · 2^(n-1)`, capped
    /// (`2s, 4s, 8s, …` up to the cap). Pure.
    #[must_use]
    pub fn delay_after(&self, failed_attempt: u32) -> u64 {
        let shift = failed_attempt.saturating_sub(1).min(63);
        self.backoff_start_ms
            .saturating_mul(1u64 << shift)
            .min(self.backoff_cap_ms)
    }
}

/// Clock + sleeper for the retry driver — the C41 `TurnClock`
/// discipline applied to waiting: assembly injects the system clock,
/// tests a manual one that **records** every sleep instead of
/// sleeping, so the backoff schedule is asserted exactly (the
/// `ManualTurnClock` pattern, plus capture).
pub trait RetryClock: Send + Sync {
    /// Current time, unix epoch milliseconds.
    fn now_ms(&self) -> u64;
    /// Waits `ms` (the system clock really sleeps; the manual clock
    /// records and returns).
    fn sleep_ms(&self, ms: u64) -> BoxFuture<'static, ()>;
}

/// The real clock: `now_ms` from the wall, sleeps via tokio.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRetryClock;

impl RetryClock for SystemRetryClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
    fn sleep_ms(&self, ms: u64) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
        })
    }
}

/// The manual clock: deterministic time plus the captured sleep
/// schedule (the assertion surface for "backoff 2s, 4s, …").
#[derive(Debug, Default)]
pub struct ManualRetryClock {
    now: std::sync::atomic::AtomicU64,
    sleeps: Mutex<Vec<u64>>,
}

impl ManualRetryClock {
    #[must_use]
    pub fn new(start_ms: u64) -> Self {
        Self {
            now: std::sync::atomic::AtomicU64::new(start_ms),
            sleeps: Mutex::new(Vec::new()),
        }
    }

    /// Advances the clock, returning the new time.
    pub fn advance(&self, by_ms: u64) -> u64 {
        self.now
            .fetch_add(by_ms, std::sync::atomic::Ordering::Relaxed)
            + by_ms
    }

    /// Every sleep issued so far, in order (the backoff schedule).
    #[must_use]
    pub fn sleeps(&self) -> Vec<u64> {
        self.sleeps.lock().expect("manual clock lock").clone()
    }
}

impl RetryClock for ManualRetryClock {
    fn now_ms(&self) -> u64 {
        self.now.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn sleep_ms(&self, ms: u64) -> BoxFuture<'static, ()> {
        self.sleeps.lock().expect("manual clock lock").push(ms);
        self.advance(ms);
        Box::pin(async {})
    }
}

/// The canonical conversation key: `(channel, speak_key(target))` —
/// the same canonicalization the C58 ack-once memory keys on, reused
/// so the two projections can never disagree about identity.
#[must_use]
pub fn conversation_key(channel: &str, target: &Value) -> String {
    format!("{channel}:{}", speak_key(target))
}

/// The ticket identity of an intent: the optional `correlation`
/// argument (a non-empty string — the `spawn_worker` vocabulary),
/// else `default` (the Router session itself). Pure.
#[must_use]
pub fn intent_ticket(arguments: &Value, default: &str) -> String {
    arguments
        .get("correlation")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| default.to_string())
}

/// One intent/receipt pairing — the projection's unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    /// Ticket identity (`correlation` argument or the Router default).
    pub ticket: String,
    /// `(channel, speak_key(target))`.
    pub conversation: String,
    /// `reply` or `deliver_structured`.
    pub tool: String,
    /// The intent `tool/call`'s `content.id` — the `tool/result`
    /// pairing key.
    pub call_id: String,
    /// The verbatim tool arguments (the rebuild input for a re-drive:
    /// channel, target, text/payload, correlation).
    pub arguments: Value,
    /// The intent `tool/call`'s stream seq (FIFO order within a key).
    pub seq: u64,
    /// Classification from the pairing rule (see the module table).
    pub state: OutboxState,
}

/// The rebuild classification of one intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxState {
    /// No `tool/result` yet — in-flight (a kill -9 mid-retry). The
    /// boot re-drive sends it again (at-least-once; the receipt
    /// eventually lands once).
    Pending,
    /// The driver exhausted its attempts and said so honestly (the
    /// `outbox park:` error). Terminal for the driver — visible via
    /// the stage arm, the audit line and the counter; a person's
    /// message releases it (003).
    Parked { attempts: u32 },
    /// The send succeeded (`tool/result` ok; its `channel/delivered`
    /// exists).
    Delivered,
}

/// Whether `content.name` names a delivery intent.
fn is_delivery_tool(name: &str) -> bool {
    name == REPLY_TOOL || name == DELIVER_STRUCTURED_TOOL
}

/// The rebuild: every `reply`/`deliver_structured` intent on a Router
/// stream, paired with its `tool/result` (by `call_id`) and — for a
/// result-less intent — with its `channel/delivered` receipt (FIFO per
/// conversation, receipt seq after intent seq). Pure over events;
/// returns only entries that are **not** delivered — a pending
/// (re-drive at boot) or parked (visible, held) delivery.
///
/// Intents whose result is an error without the park prefix are
/// dropped here on purpose: they are speak-suppressions, invalid
/// calls or legacy single-attempt failures — never outbox work, and
/// re-driving them would duplicate sends that were honestly not made.
///
/// The receipt pass exists for the re-drive path: a reboot re-drive
/// sends and writes the receipt but can never write a `tool/result`
/// (the turn that owned it is gone) — the receipt alone must satisfy
/// the intent, or every boot would re-drive it again. Known edge,
/// accepted: a *different* ticket's receipt on the same conversation
/// can satisfy a result-less intent — under concurrent tickets that
/// trades one bounded duplicate send (at-least-once, the documented
/// C62 semantics) against unbounded re-drives.
#[must_use]
pub fn outbox_pending(events: &[Event]) -> Vec<OutboxEntry> {
    // call_id → position in `intents` (results may follow any later
    // event on the stream — the loop writes them after execution).
    let mut intents: Vec<OutboxEntry> = Vec::new();
    let mut index: BTreeMap<String, usize> = BTreeMap::new();
    // Receipts per conversation, in seq order (the pairing pool).
    let mut receipts: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for ev in events {
        match ev.kind {
            EventKind::ToolCall => {
                let Some(content) = ev.bytes.get("content") else {
                    continue;
                };
                let name = content.get("name").and_then(Value::as_str).unwrap_or("");
                if !is_delivery_tool(name) {
                    continue;
                }
                let call_id = content
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let arguments = content.get("arguments").cloned().unwrap_or(Value::Null);
                let channel = arguments
                    .get("channel")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let target = arguments.get("target").cloned().unwrap_or(Value::Null);
                index.insert(call_id.clone(), intents.len());
                intents.push(OutboxEntry {
                    // The default ticket is the stream's own session —
                    // the Router itself made the delivery.
                    ticket: intent_ticket(&arguments, &ev.session.0.to_string()),
                    conversation: conversation_key(channel, &target),
                    tool: name.to_string(),
                    call_id,
                    arguments,
                    seq: ev.seq,
                    state: OutboxState::Pending,
                });
            }
            EventKind::ToolResult => {
                let Some(content) = ev.bytes.get("content") else {
                    continue;
                };
                let Some(call_id) = content.get("call_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(&i) = index.get(call_id) else {
                    continue;
                };
                let is_error = content
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let error_text = content
                    .get("output")
                    .and_then(|o| o.get("error"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                intents[i].state = if !is_error {
                    OutboxState::Delivered
                } else if let Some(rest) = error_text.strip_prefix(PARK_ERROR_PREFIX) {
                    // "… parked after N attempts" — N recorded on the
                    // park error itself (see OutboxPark::error_message).
                    let attempts = rest
                        .split_whitespace()
                        .find_map(|w| w.parse::<u32>().ok())
                        .unwrap_or(1);
                    OutboxState::Parked { attempts }
                } else {
                    // Suppressed / invalid / legacy single failure —
                    // not outbox work; filtered below with the
                    // delivered set.
                    OutboxState::Delivered
                };
            }
            EventKind::ChannelDelivered => {
                let Some(channel) = ev.bytes.get("channel").and_then(Value::as_str) else {
                    continue;
                };
                let target = ev.bytes.get("target").cloned().unwrap_or(Value::Null);
                receipts
                    .entry(conversation_key(channel, &target))
                    .or_default()
                    .push(ev.seq);
            }
            _ => {}
        }
    }
    // The receipt pass: a still-Pending intent is satisfied by the
    // first unconsumed receipt of its conversation with a later seq.
    for entry in &mut intents {
        if entry.state != OutboxState::Pending {
            continue;
        }
        let Some(pool) = receipts.get_mut(&entry.conversation) else {
            continue;
        };
        if let Some(pos) = pool.iter().position(|seq| *seq > entry.seq) {
            pool.remove(pos);
            entry.state = OutboxState::Delivered;
        }
    }
    intents
        .into_iter()
        .filter(|e| e.state != OutboxState::Delivered)
        .collect()
}

/// The rebuild snapshot answering the read side: the C56 stage arm
/// and the `kian tickets` board.
#[derive(Debug, Clone, Default)]
pub struct OutboxStatus {
    entries: Vec<OutboxEntry>,
}

impl OutboxStatus {
    /// Rebuilds the status from a committed Router stream (pure).
    #[must_use]
    pub fn from_events(events: &[Event]) -> Self {
        Self {
            entries: outbox_pending(events),
        }
    }

    /// The pending and parked entries (already filtered; kept as a
    /// named accessor for the board).
    #[must_use]
    pub fn entries(&self) -> &[OutboxEntry] {
        &self.entries
    }

    /// Whether any delivery is pending or parked for `ticket`.
    #[must_use]
    pub fn pending_for(&self, ticket: &str) -> bool {
        self.entries.iter().any(|e| e.ticket == ticket)
    }

    /// One board line per parked delivery: `<conversation> (parked
    /// after N attempts)` — the "parked message named in kian
    /// tickets".
    #[must_use]
    pub fn parked_lines(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter_map(|e| match e.state {
                OutboxState::Parked { attempts } => Some(format!(
                    "{} (parked after {attempts} attempts)",
                    e.conversation
                )),
                _ => None,
            })
            .collect()
    }
}

/// The real [`OutboxView`] (C56's arm 1, wired by this card): a
/// pending/parked delivery lights the `delivery-retry` stage for the
/// ticket it belongs to. The ticket aliases per thread are supplied by
/// the caller — the thread's own id string plus the topics its worker
/// owns in the registry projection (a `correlation` may name either).
pub struct ThreadOutboxView {
    status: OutboxStatus,
    aliases: BTreeMap<Uuid, Vec<String>>,
}

impl ThreadOutboxView {
    /// `aliases`: thread → the ticket identities that name it.
    #[must_use]
    pub fn new(status: OutboxStatus, aliases: BTreeMap<Uuid, Vec<String>>) -> Self {
        Self { status, aliases }
    }
}

impl OutboxView for ThreadOutboxView {
    fn has_pending_delivery(&self, thread_id: Uuid) -> bool {
        let Some(aliases) = self.aliases.get(&thread_id) else {
            return false;
        };
        aliases.iter().any(|a| self.status.pending_for(a))
    }
}

/// Counters for the retry driver (the C58 `SpeakMetrics` pattern —
/// plain atomics, audit-visible, snapshot-stable).
#[derive(Debug, Default)]
pub struct OutboxMetrics {
    retries: std::sync::atomic::AtomicU64,
    parks: std::sync::atomic::AtomicU64,
    redrives: std::sync::atomic::AtomicU64,
}

impl OutboxMetrics {
    fn shared_static() -> &'static Arc<Self> {
        static SHARED: std::sync::OnceLock<Arc<OutboxMetrics>> = std::sync::OnceLock::new();
        SHARED.get_or_init(|| Arc::new(Self::default()))
    }

    /// The process-global instance (the `SpeakMetrics::shared` shape).
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::clone(Self::shared_static())
    }

    /// One tick per retry attempt actually taken (attempt ≥ 2).
    pub fn record_retry(&self) {
        self.retries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// One tick per parked delivery.
    pub fn record_park(&self) {
        self.parks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// One tick per boot re-drive (in-flight intents re-sent).
    pub fn record_redrive(&self) {
        self.redrives
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// The snapshot (all fields materialized, monotone).
    #[must_use]
    pub fn snapshot(&self) -> Value {
        serde_json::json!({
            "retries": self.retries.load(std::sync::atomic::Ordering::Relaxed),
            "parks": self.parks.load(std::sync::atomic::Ordering::Relaxed),
            "redrives": self.redrives.load(std::sync::atomic::Ordering::Relaxed),
        })
    }
}

/// A successful delivery through the driver: the adapter receipt plus
/// the attempt accounting the `channel/delivered` payload stamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivered {
    pub receipt: SendReceipt,
    /// 1-based attempt that succeeded.
    pub attempt: u32,
    /// `RetryClock::now_ms` at the first attempt of this delivery.
    pub first_attempt_ms: u64,
    /// The conversation key (for audits and error messages).
    pub conversation: String,
}

/// A parked delivery: attempts exhausted, nothing more will be sent.
/// Its [`OutboxPark::error_message`] starts with
/// [`PARK_ERROR_PREFIX`] — that prefix is the rebuild contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxPark {
    pub attempts: u32,
    pub conversation: String,
    pub last_error: String,
}

impl OutboxPark {
    /// The honest park error (tool-facing and rebuild-parseable).
    #[must_use]
    pub fn error_message(&self) -> String {
        format!(
            "{PARK_ERROR_PREFIX} delivery to {} parked after {} attempts: {} — \
             no further sends; the ticket shows delivery-retry until released",
            self.conversation, self.attempts, self.last_error
        )
    }
}

/// The bounded retry driver. Wraps the send closure the Router send
/// path already uses — **retry wraps the adapter boundary**; adapter
/// internals are untouched. All state is per-call and per-key locks:
/// nothing here is durable, everything is rebuilt from the stream
/// (constraint 1).
pub struct OutboxDriver {
    config: OutboxConfig,
    clock: Arc<dyn RetryClock>,
    channels: crate::router::ChannelRegistry,
    /// One fair mutex per `(ticket, conversation)` — held across a
    /// whole `deliver_with_retry`, so a later message of the same key
    /// waits and everything else proceeds.
    keys: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
    metrics: Arc<OutboxMetrics>,
}

impl OutboxDriver {
    #[must_use]
    pub fn new(
        config: OutboxConfig,
        clock: Arc<dyn RetryClock>,
        channels: crate::router::ChannelRegistry,
    ) -> Self {
        Self {
            config,
            clock,
            channels,
            keys: Mutex::new(BTreeMap::new()),
            metrics: OutboxMetrics::shared(),
        }
    }

    /// The driver's counters (the assembly's observability surface).
    #[must_use]
    pub fn metrics(&self) -> Arc<OutboxMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The configuration (read-side).
    #[must_use]
    pub fn config(&self) -> &OutboxConfig {
        &self.config
    }

    fn key_lock(&self, ticket: &str, conversation: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("{ticket}\u{1}{conversation}");
        let mut keys = self.keys.lock().expect("outbox key lock");
        keys.entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Bounded retry around one send: `send` is re-executed per
    /// attempt (the same policy-shaped `ChannelDelivery` bytes every
    /// time — persist-what-you-send holds across retries), with
    /// backoff `start · 2^(n-1)` capped, at most `max_attempts`
    /// total. Serialized per `(ticket, conversation)`: the caller's
    /// later message of the same key waits here; every other key
    /// never waits.
    pub async fn deliver_with_retry<F, Fut>(
        &self,
        ticket: &str,
        conversation: &str,
        mut send: F,
    ) -> Result<Delivered, OutboxPark>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<SendReceipt, ChannelError>>,
    {
        let key = self.key_lock(ticket, conversation);
        let _guard = key.lock().await;
        let first_attempt_ms = self.clock.now_ms();
        let mut last_error = String::new();
        for attempt in 1..=self.config.max_attempts {
            match send().await {
                Ok(receipt) => {
                    return Ok(Delivered {
                        receipt,
                        attempt,
                        first_attempt_ms,
                        conversation: conversation.to_string(),
                    });
                }
                Err(e) => {
                    last_error = e.to_string();
                    if attempt < self.config.max_attempts {
                        self.metrics.record_retry();
                        tracing::warn!(
                            target: "kian::outbox",
                            ticket = ticket,
                            conversation = conversation,
                            attempt = attempt,
                            backoff_ms = self.config.delay_after(attempt),
                            error = %last_error,
                            "outbox audit: send failed, backing off"
                        );
                        let delay = self.config.delay_after(attempt);
                        self.clock.sleep_ms(delay).await;
                    }
                }
            }
        }
        self.metrics.record_park();
        tracing::error!(
            target: "kian::outbox",
            ticket = ticket,
            conversation = conversation,
            attempts = self.config.max_attempts,
            error = %last_error,
            "outbox audit: delivery parked after bounded retries"
        );
        Err(OutboxPark {
            attempts: self.config.max_attempts,
            conversation: conversation.to_string(),
            last_error,
        })
    }

    /// The boot re-drive (restart, deliverable 3): every **pending**
    /// (in-flight, result-less) intent from the rebuilt stream is
    /// sent again through the adapter — at-least-once; under the
    /// per-key ordering the receipt eventually lands exactly once for
    /// each conversation. Parked entries are **not** re-driven (003: a
    /// parked delivery waits for a person).
    ///
    /// The delivery is reconstructed from the intent's verbatim
    /// arguments plus the live output policies — the same pure
    /// shaping the original tool path applied, so the re-driven bytes
    /// equal the intent's bytes. Returns one `(call_id, outcome)` per
    /// re-driven entry.
    pub async fn redrive(
        &self,
        entries: &[OutboxEntry],
        im: &ImPolicy,
        structured: &StructuredPolicy,
        port: &Arc<dyn StreamPort>,
    ) -> Vec<(String, Result<Delivered, OutboxPark>)> {
        let mut out = Vec::new();
        for entry in entries.iter().filter(|e| e.state == OutboxState::Pending) {
            self.metrics.record_redrive();
            let Some(delivery) = reconstruct_delivery(entry, im, structured) else {
                tracing::warn!(
                    target: "kian::outbox",
                    call_id = %entry.call_id,
                    "outbox audit: re-drive skipped an unreconstructable intent"
                );
                continue;
            };
            let channel = delivery.channel.clone();
            let target = delivery.target.clone();
            let base_payload = if entry.tool == REPLY_TOOL {
                im_delivered_payload(
                    &channel,
                    &target,
                    &im.enforce(&arguments_text(&entry.arguments)),
                )
            } else {
                structured_delivered_payload(&channel, &target)
            };
            let adapter = self.channels.get(&channel);
            let result = match adapter {
                Some(adapter) => {
                    let d = delivery.clone();
                    self.deliver_with_retry(&entry.ticket, &entry.conversation, move || {
                        adapter.send(d.clone())
                    })
                    .await
                }
                None => Err(OutboxPark {
                    attempts: 0,
                    conversation: entry.conversation.clone(),
                    last_error: format!("unknown channel `{channel}` at re-drive"),
                }),
            };
            if let Ok(delivered) = &result {
                let payload = with_attempt_keys(base_payload, delivered);
                if let Err(e) = port.emit(EventKind::ChannelDelivered, payload).await {
                    tracing::warn!(
                        target: "kian::outbox",
                        call_id = %entry.call_id,
                        error = %e,
                        "outbox audit: re-drive delivered but the receipt write failed"
                    );
                }
            }
            out.push((entry.call_id.clone(), result));
        }
        out
    }
}

/// Stamps the additive attempt keys on a `channel/delivered` payload
/// (only ever called on the outbox-enabled path — legacy receipts
/// never carry them, and parse unchanged).
#[must_use]
pub fn with_attempt_keys(mut payload: Value, delivered: &Delivered) -> Value {
    payload["attempt"] = Value::from(delivered.attempt);
    payload["first_attempt_ms"] = Value::from(delivered.first_attempt_ms);
    payload
}

/// Rebuilds the `ChannelDelivery` an intent's arguments describe: the
/// same pure shaping the tool path applied (`reply` → ImPolicy word
/// cap; `deliver_structured` → the payload verbatim, re-validated).
/// `None` when the intent cannot be reconstructed (unknown tool,
/// missing payload, failed validation) — the re-drive says so, never
/// guesses.
#[must_use]
pub fn reconstruct_delivery(
    entry: &OutboxEntry,
    im: &ImPolicy,
    structured: &StructuredPolicy,
) -> Option<ChannelDelivery> {
    let channel = entry.arguments.get("channel")?.as_str()?.to_string();
    let target = entry
        .arguments
        .get("target")
        .cloned()
        .unwrap_or(Value::Null);
    let content = if entry.tool == REPLY_TOOL {
        serde_json::json!({"text": im.enforce(&arguments_text(&entry.arguments)).text})
    } else {
        let payload = entry.arguments.get("payload")?.clone();
        structured.validate(&payload).ok()?;
        payload
    };
    Some(ChannelDelivery {
        channel,
        target,
        content,
    })
}

/// The `text` argument of a `reply` intent (empty on missing — the
/// ImPolicy shape of the empty string is the honest reconstruction).
fn arguments_text(arguments: &Value) -> String {
    arguments
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn backoff_schedule_doubles_from_start_to_cap() {
        let cfg = OutboxConfig {
            max_attempts: 10,
            backoff_start_ms: 2_000,
            backoff_cap_ms: 60_000,
        };
        assert_eq!(cfg.delay_after(1), 2_000);
        assert_eq!(cfg.delay_after(2), 4_000);
        assert_eq!(cfg.delay_after(3), 8_000);
        assert_eq!(cfg.delay_after(4), 16_000);
        assert_eq!(cfg.delay_after(5), 32_000);
        assert_eq!(cfg.delay_after(6), 60_000, "capped, never past the cap");
        assert_eq!(cfg.delay_after(9), 60_000);
    }

    #[test]
    fn config_validation_fail_closed() {
        assert!(OutboxConfig::default().validate().is_ok());
        assert!(
            OutboxConfig {
                max_attempts: 0,
                ..OutboxConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            OutboxConfig {
                backoff_start_ms: 0,
                ..OutboxConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            OutboxConfig {
                backoff_cap_ms: 1,
                ..OutboxConfig::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn park_error_message_is_the_rebuild_contract() {
        let park = OutboxPark {
            attempts: 6,
            conversation: "mock-im:{\"chat_id\":1}".into(),
            last_error: "delivery failed: upstream 503".into(),
        };
        let msg = park.error_message();
        assert!(msg.starts_with(PARK_ERROR_PREFIX), "{msg}");
        // The rebuild parses the attempt count back out.
        let rest = msg.strip_prefix(PARK_ERROR_PREFIX).unwrap();
        let attempts = rest.split_whitespace().find_map(|w| w.parse::<u32>().ok());
        assert_eq!(attempts, Some(6));
    }

    #[test]
    fn ticket_identity_prefers_the_correlation_argument() {
        let args = json!({"channel": "mock-im", "correlation": "acme/repo#7"});
        assert_eq!(intent_ticket(&args, "router-default"), "acme/repo#7");
        let plain = json!({"channel": "mock-im"});
        assert_eq!(intent_ticket(&plain, "router-default"), "router-default");
        let empty = json!({"correlation": ""});
        assert_eq!(intent_ticket(&empty, "router-default"), "router-default");
    }

    #[test]
    fn rebuild_classifies_intents_by_their_results() {
        use kian_core::events::Event;
        use kian_core::model::{Epoch, SessionId};
        let session = SessionId(uuid::Uuid::nil());
        let mut events = Vec::new();
        let mut seq = 0u64;
        let mut push = |kind: EventKind, bytes: Value| {
            seq += 1;
            events.push(Event {
                seq,
                session,
                epoch: Epoch(1),
                ts: 1_000 + seq,
                kind,
                bytes,
                version: 0,
            });
        };
        let target = json!({"chat_id": 1});
        // 1: pending (no result) — kill -9 mid-retry.
        push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c1", "name": "reply",
                "arguments": {"channel": "mock-im", "target": target, "text": "hi",
                              "correlation": "t-1"}}}),
        );
        // 2: parked (the park prefix).
        push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c2", "name": "deliver_structured",
                "arguments": {"channel": "mock-im", "target": target, "payload": {}}}}),
        );
        push(
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c2", "is_error": true,
                "output": {"error": format!("{} delivery to x parked after 6 attempts: boom", PARK_ERROR_PREFIX)}}}),
        );
        // 3: delivered (ok result).
        push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c3", "name": "reply",
                "arguments": {"channel": "mock-im", "target": json!({"chat_id": 2}), "text": "ok"}}}),
        );
        push(
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c3", "is_error": false,
                "output": {"message_id": "m"}}}),
        );
        // 4: suppressed (error without the prefix) — dropped, never
        // re-driven.
        push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c4", "name": "reply",
                "arguments": {"channel": "mock-im", "target": json!({"chat_id": 3}), "text": "no"}}}),
        );
        push(
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c4", "is_error": true,
                "output": {"error": "send suppressed by the speak rule for channel `mock-im`"}}}),
        );
        // 5: a non-delivery tool call — ignored.
        push(
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c5", "name": "spawn_worker",
                "arguments": {"task": "x"}}}),
        );

        let pending = outbox_pending(&events);
        assert_eq!(pending.len(), 2, "{pending:?}");
        assert_eq!(pending[0].call_id, "c1");
        assert_eq!(pending[0].state, OutboxState::Pending);
        assert_eq!(pending[0].ticket, "t-1");
        assert_eq!(pending[1].call_id, "c2");
        assert_eq!(pending[1].state, OutboxState::Parked { attempts: 6 });
        // The uncorrelated intent falls back to the stream's session
        // (the Router itself).
        let defaulted = outbox_pending(&events)
            .into_iter()
            .find(|e| e.call_id == "c2")
            .unwrap();
        assert_eq!(defaulted.ticket, uuid::Uuid::nil().to_string());

        let status = OutboxStatus::from_events(&events);
        assert!(status.pending_for("t-1"));
        assert!(status.pending_for(&uuid::Uuid::nil().to_string()));
        assert!(!status.pending_for("nobody"));
        assert_eq!(
            status.parked_lines(),
            vec![format!(
                "{} (parked after 6 attempts)",
                pending[1].conversation
            )]
        );
    }

    #[test]
    fn receipt_satisfies_a_result_less_intent_the_re_drive_pairing() {
        use kian_core::events::Event;
        use kian_core::model::{Epoch, SessionId};
        let session = SessionId(uuid::Uuid::nil());
        let mk = |seq, kind, bytes| Event {
            seq,
            session,
            epoch: Epoch(1),
            ts: seq,
            kind,
            bytes,
            version: 0,
        };
        let target = json!({"chat_id": 5});
        // A result-less intent …
        let stream = vec![
            mk(
                1,
                EventKind::ToolCall,
                json!({"role": "assistant", "content": {"id": "c1", "name": "reply",
                    "arguments": {"channel": "mock-im", "target": target, "text": "x"}}}),
            ),
            // … satisfied by its receipt alone (the re-drive shape: no
            // tool/result can ever follow).
            mk(
                2,
                EventKind::ChannelDelivered,
                json!({"channel": "mock-im", "target": target, "policy": "im",
                       "words": 1, "truncated": false, "attempt": 2, "first_attempt_ms": 9}),
            ),
        ];
        assert!(
            outbox_pending(&stream).is_empty(),
            "the receipt satisfies the intent — the next boot re-drives nothing"
        );
        // A receipt that PREDATES the intent satisfies nothing: it is
        // an earlier delivery's receipt, never this intent's.
        let earlier_receipt = vec![
            mk(
                1,
                EventKind::ChannelDelivered,
                json!({"channel": "mock-im", "target": target, "policy": "im",
                       "words": 1, "truncated": false}),
            ),
            mk(
                3,
                EventKind::ToolCall,
                json!({"role": "assistant", "content": {"id": "c1", "name": "reply",
                    "arguments": {"channel": "mock-im", "target": target, "text": "x"}}}),
            ),
        ];
        assert_eq!(
            outbox_pending(&earlier_receipt).len(),
            1,
            "an earlier receipt never satisfies a later intent"
        );
    }

    #[test]
    fn thread_view_matches_aliases() {
        use kian_core::events::Event;
        use kian_core::model::{Epoch, SessionId};
        let thread = uuid::Uuid::new_v4();
        let session = SessionId(uuid::Uuid::nil());
        let events = vec![Event {
            seq: 1,
            session,
            epoch: Epoch(1),
            ts: 1,
            kind: EventKind::ToolCall,
            bytes: json!({"role": "assistant", "content": {"id": "c1", "name": "reply",
                "arguments": {"channel": "mock-im", "target": {"chat_id": 1}, "text": "hi",
                              "correlation": "acme/repo#7"}}}),
            version: 0,
        }];
        let status = OutboxStatus::from_events(&events);
        let mut aliases = BTreeMap::new();
        aliases.insert(thread, vec![thread.to_string(), "acme/repo#7".to_string()]);
        let view = ThreadOutboxView::new(status, aliases);
        assert!(view.has_pending_delivery(thread));
        assert!(!view.has_pending_delivery(uuid::Uuid::new_v4()));
    }

    #[test]
    fn delivery_reconstruction_reshapes_like_the_tool_path() {
        let im = ImPolicy::default();
        let structured = StructuredPolicy::default();
        let reply = OutboxEntry {
            ticket: "t".into(),
            conversation: "mock-im:x".into(),
            tool: REPLY_TOOL.into(),
            call_id: "c1".into(),
            arguments: json!({"channel": "mock-im", "target": {"chat_id": 1},
                              "text": "one two three"}),
            seq: 1,
            state: OutboxState::Pending,
        };
        let d = reconstruct_delivery(&reply, &im, &structured).expect("reply reconstructs");
        assert_eq!(d.channel, "mock-im");
        assert_eq!(d.content, json!({"text": im.enforce("one two three").text}));

        let structured_entry = OutboxEntry {
            tool: DELIVER_STRUCTURED_TOOL.into(),
            arguments: json!({"channel": "mock-im", "target": {"issue": 7},
                              "payload": {"background": "b", "changes": "c", "verification": "v"}}),
            ..reply
        };
        let d2 = reconstruct_delivery(&structured_entry, &im, &structured)
            .expect("structured reconstructs");
        assert_eq!(
            d2.content,
            json!({"background": "b", "changes": "c", "verification": "v"})
        );

        // A payload that now fails validation never reconstructs —
        // the re-drive says so instead of sending something else.
        let invalid = OutboxEntry {
            arguments: json!({"channel": "mock-im", "target": {"issue": 7}, "payload": {"background": "b"}}),
            ..structured_entry
        };
        assert!(reconstruct_delivery(&invalid, &im, &structured).is_none());
    }

    #[test]
    fn attempt_keys_are_additive() {
        let delivered = Delivered {
            receipt: SendReceipt {
                message_id: "m".into(),
                ts_ms: 42,
            },
            attempt: 3,
            first_attempt_ms: 1_000,
            conversation: "c".into(),
        };
        let mut base = im_delivered_payload(
            "mock-im",
            &json!({"chat_id": 1}),
            &ImPolicy::default().enforce("hi"),
        );
        let legacy = base.clone();
        let stamped = with_attempt_keys(base.clone(), &delivered);
        base["attempt"] = json!(3);
        base["first_attempt_ms"] = json!(1_000);
        assert_eq!(stamped, base);
        // The legacy payload parses unchanged: same object minus the
        // two additive keys.
        assert_eq!(legacy.as_object().map(|o| o.len()), Some(5));
        assert!(stamped.as_object().unwrap().len() == legacy.as_object().unwrap().len() + 2);
    }
}
