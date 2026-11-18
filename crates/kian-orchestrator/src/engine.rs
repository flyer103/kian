//! Admit → Wake → Drain → Promote (DESIGN §2.3).
//!
//! Admit before wake (constraint 6): the input row is durable before any
//! wake fires. Wakes are advisory: foldable, losable, fully rebuilt from
//! the startup inbox scan (NoLostWork). Claim issues a monotonic
//! epoch via the store; every drain write carries it (ADR-005 fencing);
//! the lease TTL + heartbeat task is *liveness only*. Drains are
//! per-session serial (one `DashMap<SessionId, DrainHandle>` entry per
//! draining session) and cross-session concurrent (one tokio task per
//! session).
//!
//! The layered iron rule (constraint 9): execution here only appends
//! events — every state is re-derived from the event projection; the
//! control-plane surface (`admit`/`steer`) only writes the inbox.
//!
//! Input-consumption convention (consumption is event-driven): the
//! drain is the only writer of `input/admitted` *events*. It appends one
//! — carrying the message payload contract from DESIGN §3.2
//! (`{"role","content","synthetic"?}`) — at a safe boundary when it
//! takes the row, then binds the row to that event's seq via
//! `mark_consumed`. Payload convention: a plain input row's payload IS
//! the message content; a steer row wraps it as
//! `{"synthetic": true, "content": <content>}` so the synthetic
//! (strippable) marker survives the inbox round-trip. The spill
//! envelope extends this row-level convention: a row payload of exactly
//! `{"content": <message content>, "spill": <sidecar value>}` consumes
//! as TWO events in one fenced write batch — `input/admitted` carrying
//! `{"role": "user", "content": <content>}` exactly as the bare content
//! would have produced, followed immediately by `input/spilled` carrying
//! the opaque sidecar verbatim. The envelope exists only at the row
//! level: the frozen event payload contracts are untouched, the sidecar
//! never enters model context (`input/spilled` is non-context-bearing),
//! and rows without the envelope consume byte-identically to before.
//! The dream envelope (kian-91, review B-2) extends the convention the
//! same way: a row payload of exactly
//! `{"content": <rendered dream prompt>, "dream": <trusted marker>}`
//! consumes as ONE `input/admitted` carrying
//! `{"role": "user", "content": <content>, "dream": <marker>}` — the
//! marker is additive and non-context (dreamer routing and provenance
//! read it, never message text; `derive_model_context` output is
//! unchanged). The memory envelope (kian-99, MEMORY-MODEL §6) is the
//! same shape for the creation-time injection: a row payload of exactly
//! `{"content": <rendered index>, "memory": <provenance marker>}`,
//! admitted only by the server's own creation path via
//! [`Engine::admit_quiet`], consumes as ONE `input/admitted` carrying
//! `{"role": "system", "content": <content>, "synthetic": true,
//! "memory": <marker>}` — additive and non-context the same way.
//!
//! extends the same row-level convention with the reserved control
//! envelope `{"kian": {"type": "control", "action": "interrupt", …}}`
//! (exactly that single key): the control plane admits it as the
//! durable interrupt intent, and the drain consumes it to a
//! `control/interrupt` audit event — never a message — flipping the
//! shared interrupt flag when the row targets the current episode
//! (ADR-014; the stale arm is audit-only).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::{DashMap, Entry};
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::sync::Notify;

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, LeaseRecord, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_core::state::{SessionState, project};
use kian_store::{AdmitOutcome, AppendGuard, EventStore, StoreError};
use uuid::Uuid;

use crate::metrics::Metrics;
use crate::wake::WakeRegistry;

/// Wall-clock source. Injectable so tests (lease TTL, heartbeats) run
/// deterministically; the store itself never owns a clock.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The real clock (unix epoch milliseconds).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A manually advanced clock for deterministic lease/heartbeat tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    #[must_use]
    pub fn new(start_ms: u64) -> Self {
        Self(AtomicU64::new(start_ms))
    }

    /// Advances the clock, returning the new time.
    pub fn advance(&self, by_ms: u64) -> u64 {
        self.0.fetch_add(by_ms, Ordering::Relaxed) + by_ms
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Engine tuning (holder identity, lease TTL, heartbeat cadence).
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Drain-holder identity written into claims (process/node id).
    pub holder: String,
    /// Lease time-to-live, milliseconds (liveness only, ADR-005).
    pub lease_ttl_ms: u64,
    /// Heartbeat task period, milliseconds.
    pub heartbeat_interval_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            holder: "kian-orchestrator".into(),
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 10_000,
        }
    }
}

/// Typed engine failures. Rejected CAS writes surface as
/// [`StoreError::StaleEpoch`] / [`StoreError::SeqMismatch`] — a fenced
/// drain abandons; it never retries against a newer epoch.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("inbox: {0}")]
    Inbox(#[from] kian_store::InboxError),
    #[error("projection: {0}")]
    Projection(#[from] kian_core::state::ProjectionError),
    #[error(
        "cannot take inputs for {session:?}: a provider turn is in flight (promote only at safe boundaries)"
    )]
    NotAtSafeBoundary { session: SessionId },
    #[error("append retry budget exhausted for {session:?}")]
    AppendRetryExhausted { session: SessionId },
    /// READ-MODEL §3: the caller's `since` is ahead of the durable
    /// head — the client is ahead or pointed at another store. An
    /// explicit refusal, never a silent resync.
    #[error(
        "since {since} is ahead of session {session:?} version {version} (client ahead or another store)"
    )]
    SinceAhead {
        session: SessionId,
        since: u64,
        version: u64,
    },
    /// Context projection failure (READ-MODEL §5 load path: the tail
    /// slice the loader appends must parse under the frozen message
    /// contract; a failure is the same error the full derivation would
    /// produce).
    #[error("context: {0}")]
    Context(#[from] kian_core::context::ContextError),
}

/// What a [`Engine::wake`] call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// No drain was in flight: one was spawned.
    Started,
    /// A drain was already in flight: the wake folded onto pendingWake.
    Folded,
}

/// What an [`Engine::interrupt`] call did. Interrupting an idle or
/// unknown session is a no-op (invariant 5: interrupt targets only the
/// active ownership chain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptOutcome {
    /// A drain is in flight; it will settle at its next safe boundary.
    Requested,
    /// No drain is in flight; the session's projected state is given.
    NotDraining { state: SessionState },
    /// No such session (empty event stream).
    UnknownSession,
}

/// What an [`Engine::release_idle`] call did (ADR-035).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The idle, already-ended session was closed: the claim + terminal
    /// `cancelled` end landed atomically.
    Released { seq: u64, epoch: u64 },
    /// The session is already terminal — nothing was written
    /// (idempotent).
    AlreadyTerminal,
    /// A drain holds the session, or a fresh lease on another replica
    /// blocked the claim — nothing was written; retry later.
    Busy,
    /// The session is not an idle, already-ended session (running,
    /// never ended, or holding eligible work) — nothing was written.
    NotEligible,
    /// No event stream exists for the session.
    Unknown,
}

/// The in-map handle for one draining session. Presence in
/// `DashMap<SessionId, DrainHandle>` *is* "a drain is in flight".
#[derive(Debug)]
pub struct DrainHandle {
    /// The draining session.
    pub session: SessionId,
    /// When the drain task started (unix epoch ms).
    pub started_at_ms: u64,
    interrupt_flag: Arc<AtomicBool>,
}

impl DrainHandle {
    fn new(session: SessionId, started_at_ms: u64) -> Self {
        Self {
            session,
            started_at_ms,
            interrupt_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    fn interrupt_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.interrupt_flag)
    }

    /// Requests an interrupt; honored at the next safe boundary.
    pub fn interrupt(&self) {
        self.interrupt_flag.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_interrupted(&self) -> bool {
        self.interrupt_flag.load(Ordering::Acquire)
    }
}

/// Bound on seq-CAS retry when a concurrent (fenced-out) writer moved
/// the stream head between our read and append. Epoch-stale writes are
/// never retried.
const APPEND_ATTEMPTS: usize = 4;

/// Shared mutable drain state: the fencing epoch, the next free seq,
/// and the provider-turn boundary flag. One core per drain attempt; the
/// ctx handed to the driver is a cheap clone of it.
struct DrainCtxCore {
    store: EventStore,
    session: SessionId,
    epoch: Epoch,
    /// Next free seq (matches the store head; CAS-repaired on mismatch).
    seq: AtomicU64,
    /// True while a provider turn is in flight (promote boundary gate).
    in_turn: AtomicBool,
    /// The drain's committed-event cache (READ-MODEL §3, P0): the one
    /// event Vec this episode derives from, tail-extended by
    /// [`DrainCtx::events`]. Disposable by construction — every entry
    /// is a committed stream event and a full re-derivation is always
    /// one `rebuild` away (R1: the stream stays the truth).
    events: tokio::sync::Mutex<Vec<Event>>,
    /// The drain's interrupt flag. Shared with the engine's
    /// `DrainHandle` when the ctx is built by `drain_episode`, so a
    /// consumed current-epoch control row and an `interrupt` RPC flip
    /// the *same* flag and `supervise` honors both identically. A ctx
    /// built via [`DrainCtx::new`] owns a detached flag (tests,
    /// recovery helpers).
    interrupt: Arc<AtomicBool>,
    clock: Arc<dyn Clock>,
    metrics: Arc<Metrics>,
}

/// The handle surface the engine hands to a [`LoopDriver`]
/// (`LoopDriver::Ctx`). This is the ONLY path through which inputs
/// become `input/admitted` events, and it refuses to promote while a
/// provider turn is in flight (invariant 3).
#[derive(Clone)]
pub struct DrainCtx {
    core: Arc<DrainCtxCore>,
}

impl DrainCtxCore {
    /// Single append attempt with the CAS guard.
    async fn append_once(&self, event: &Event) -> Result<(), StoreError> {
        if matches!(event.kind, EventKind::SessionEnded { .. }) {
            self.store
                .append_end(
                    self.session,
                    event.clone(),
                    AppendGuard {
                        epoch: self.epoch,
                        expected_seq: event.seq,
                    },
                )
                .await
        } else {
            self.store
                .append(
                    self.session,
                    event.clone(),
                    AppendGuard {
                        epoch: self.epoch,
                        expected_seq: event.seq,
                    },
                )
                .await
        }
    }

    /// The bounded-retry append: on `SeqMismatch` (a fenced-out
    /// concurrent writer moved the head) the seq is repaired from the
    /// store head and the append retried. A stale-epoch rejection
    /// propagates immediately — a fenced drain never fights the fence.
    async fn append_event(&self, kind: EventKind, bytes: Value) -> Result<Event, EngineError> {
        for _ in 0..APPEND_ATTEMPTS {
            let seq = self.seq.load(Ordering::Acquire);
            let event = Event {
                seq,
                session: self.session,
                epoch: self.epoch,
                ts: self.clock.now_ms(),
                kind,
                bytes: bytes.clone(),
                version: SCHEMA_VERSION,
            };
            match self.append_once(&event).await {
                Ok(()) => {
                    self.seq.store(seq + 1, Ordering::Release);
                    if matches!(
                        kind,
                        EventKind::SessionCreated
                            | EventKind::DrainClaimed
                            | EventKind::SessionEnded { .. }
                    ) {
                        self.metrics.on_state_transition();
                        if matches!(kind, EventKind::SessionEnded { .. }) {
                            self.metrics.on_end_write();
                        }
                    }
                    return Ok(event);
                }
                Err(StoreError::SeqMismatch { .. }) => {
                    let head = self.store.head(self.session).await?;
                    self.seq.store(head.next_seq, Ordering::Release);
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(EngineError::AppendRetryExhausted {
            session: self.session,
        })
    }

    /// The spill consumption write: `input/admitted` immediately
    /// followed by `input/spilled` as ONE fenced write batch (the
    /// store's `append_batch`), so the pair either lands whole or not at
    /// all — a stale-epoch rejection writes neither event. Retry
    /// semantics mirror [`DrainCtxCore::append_event`]: a `SeqMismatch`
    /// from a fenced-out concurrent writer is repaired from the head and
    /// retried; a stale-epoch rejection propagates immediately.
    async fn append_input_pair(
        &self,
        admitted: Value,
        spill: Value,
    ) -> Result<(Event, Event), EngineError> {
        for _ in 0..APPEND_ATTEMPTS {
            let seq = self.seq.load(Ordering::Acquire);
            let ts = self.clock.now_ms();
            let admitted_ev = Event {
                seq,
                session: self.session,
                epoch: self.epoch,
                ts,
                kind: EventKind::InputAdmitted,
                bytes: admitted.clone(),
                version: SCHEMA_VERSION,
            };
            let spilled_ev = Event {
                seq: seq + 1,
                session: self.session,
                epoch: self.epoch,
                ts,
                kind: EventKind::InputSpilled,
                bytes: spill.clone(),
                version: SCHEMA_VERSION,
            };
            let batch = [admitted_ev.clone(), spilled_ev.clone()];
            let guard = AppendGuard {
                epoch: self.epoch,
                expected_seq: seq,
            };
            match self.store.append_batch(self.session, &batch, guard).await {
                Ok(()) => {
                    self.seq.store(seq + 2, Ordering::Release);
                    return Ok((admitted_ev, spilled_ev));
                }
                Err(StoreError::SeqMismatch { .. }) => {
                    let head = self.store.head(self.session).await?;
                    self.seq.store(head.next_seq, Ordering::Release);
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(EngineError::AppendRetryExhausted {
            session: self.session,
        })
    }
}

impl DrainCtx {
    /// Builds a context for `session` under fencing `epoch`, with the
    /// seq counter initialized from the store head and a detached
    /// interrupt flag.
    pub async fn new(
        store: EventStore,
        session: SessionId,
        epoch: Epoch,
        clock: Arc<dyn Clock>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, EngineError> {
        Self::with_interrupt(
            store,
            session,
            epoch,
            clock,
            metrics,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    /// [`DrainCtx::new`] sharing `interrupt` — the engine's
    /// `drain_episode` passes the `DrainHandle` flag so a consumed
    /// control row interrupts the live drain.
    pub async fn with_interrupt(
        store: EventStore,
        session: SessionId,
        epoch: Epoch,
        clock: Arc<dyn Clock>,
        metrics: Arc<Metrics>,
        interrupt: Arc<AtomicBool>,
    ) -> Result<Self, EngineError> {
        let head = store.head(session).await?;
        Ok(Self {
            core: Arc::new(DrainCtxCore {
                store,
                session,
                epoch,
                seq: AtomicU64::new(head.next_seq),
                in_turn: AtomicBool::new(false),
                events: tokio::sync::Mutex::new(Vec::new()),
                interrupt,
                clock,
                metrics,
            }),
        })
    }

    #[must_use]
    pub fn session(&self) -> SessionId {
        self.core.session
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.core.epoch
    }

    /// Whether an interrupt has been requested for this drain — via the
    /// `interrupt` RPC flag or a consumed current-epoch control row
    /// both flip the same flag.
    #[must_use]
    pub fn interrupt_requested(&self) -> bool {
        self.core.interrupt.load(Ordering::Acquire)
    }

    /// Whether a provider turn is in flight (promote boundary closed).
    #[must_use]
    pub fn in_turn(&self) -> bool {
        self.core.in_turn.load(Ordering::Acquire)
    }

    /// Appends one event carrying the drain's epoch under expectedSeq
    /// CAS. See [`DrainCtxCore::append_event`].
    pub async fn append(&self, kind: EventKind, bytes: Value) -> Result<Event, EngineError> {
        self.core.append_event(kind, bytes).await
    }

    /// Opens a provider turn (`turn/start`). Closes the promote
    /// boundary until [`DrainCtx::end_turn`].
    pub async fn begin_turn(&self) -> Result<Event, EngineError> {
        self.begin_turn_with(json!({})).await
    }

    /// [`DrainCtx::begin_turn`] with an audit payload (e.g. The loop's
    /// iteration counter). The payload rides the `turn/start` event's
    /// `bytes`; boundary semantics are identical.
    pub async fn begin_turn_with(&self, bytes: Value) -> Result<Event, EngineError> {
        let ev = self.core.append_event(EventKind::TurnStart, bytes).await?;
        self.core.in_turn.store(true, Ordering::Release);
        Ok(ev)
    }

    /// Closes the provider turn (`turn/end`), reopening the promote
    /// boundary.
    pub async fn end_turn(&self) -> Result<Event, EngineError> {
        self.end_turn_with(json!({})).await
    }

    /// [`DrainCtx::end_turn`] with an audit payload on the `turn/end`
    /// event.
    pub async fn end_turn_with(&self, bytes: Value) -> Result<Event, EngineError> {
        let ev = self.core.append_event(EventKind::TurnEnd, bytes).await?;
        self.core.in_turn.store(false, Ordering::Release);
        Ok(ev)
    }

    /// Read-only accessor: the session's full committed event stream —
    /// the projection source a [`LoopDriver`](kian_core::seams::LoopDriver)
    /// derives its model context from (the event log is the sole truth,
    /// constraint 1; the loop is a passenger and never mutates
    /// projections).
    ///
    /// READ-MODEL §3 (P0): the drain keeps ONE committed-event Vec per
    /// episode; each call extends it with only the events committed
    /// since the last — a bounded tail scan, never a full `rebuild`
    /// per loop iteration (per-poll work is O(new events)). A
    /// `compact` event in the tail is the single deliberate
    /// invalidation point (constraint 3 / R5): the cache is dropped
    /// and re-derived from the stream once, in full. Event bytes are
    /// never rewritten, reordered or reformatted here — the returned
    /// Vec is byte-identical to a fresh `rebuild` (the cache-stability
    /// witness suites pin the derived request bytes).
    pub async fn events(&self) -> Result<Vec<Event>, EngineError> {
        let mut cache = self.core.events.lock().await;
        // The seq axis alone: this refresh must stay cheap at any
        // history size (the epoch-floor aggregate of `head` is not
        // needed for reads).
        let version = self.core.store.version(self.core.session).await?;
        match cache.last().map(|e| e.seq) {
            // Up to date: one version query, no scan (R3's fast path).
            Some(seq) if version == seq => {}
            // The durable head behind the cache: the stream cannot
            // legitimately shrink. Treat as skew and re-derive in full —
            // honesty over cleverness (never serve a skew as truth).
            Some(seq) if version < seq => {
                *cache = self.core.store.rebuild(self.core.session).await?;
            }
            _ => {
                let start = cache.last().map(|e| e.seq + 1).unwrap_or(0);
                let fresh = self
                    .core
                    .store
                    .scan(self.core.session, start..version + 1)
                    .await?;
                if fresh.iter().any(|e| matches!(e.kind, EventKind::Compact)) {
                    // R5: compact is the ONLY deliberate invalidation
                    // point — one full re-derivation from the stream.
                    *cache = self.core.store.rebuild(self.core.session).await?;
                } else if fresh.first().is_none_or(|e| e.seq == start) {
                    cache.extend(fresh);
                } else {
                    // A non-contiguous tail (store bug or reset):
                    // re-derive rather than append a gap.
                    *cache = self.core.store.rebuild(self.core.session).await?;
                }
            }
        }
        Ok(cache.clone())
    }

    /// Takes every eligible input at this safe boundary: appends the
    /// consumption event(s) per row (the `input/admitted` message event,
    /// plus — for the spill envelope — its `input/spilled` sibling in
    /// the same fenced write batch) and binds the row to the admitted
    /// event's seq via `mark_consumed`. Refused while a provider turn is
    /// in flight (invariant 3) — steer inputs land only at turn
    /// boundaries.
    ///
    /// reserved control-envelope rows are consumed here too — they
    /// append a `control/interrupt` event (the audit record) instead of
    /// a message, and a current-epoch row sets the drain's interrupt
    /// flag. Control rows are never returned: they are never wrapped
    /// into model messages.
    pub async fn take_eligible_inputs(&self) -> Result<Vec<InputRow>, EngineError> {
        Ok(self.consume_inputs(false).await?.messages)
    }

    /// The final take before the end write: consumes ONLY pending
    /// control rows (regular inputs stay eligible for the resumer) and
    /// returns whether a current-epoch control row fired the interrupt.
    pub async fn take_control_inputs(&self) -> Result<bool, EngineError> {
        Ok(self.consume_inputs(true).await?.control_interrupt)
    }

    /// The shared consumption loop. `control_only` selects control rows
    /// (the pre-end-write final take); otherwise every eligible row is
    /// consumed in inbox order.
    async fn consume_inputs(&self, control_only: bool) -> Result<TakeReport, EngineError> {
        if self.core.in_turn.load(Ordering::Acquire) {
            return Err(EngineError::NotAtSafeBoundary {
                session: self.core.session,
            });
        }
        let mut report = TakeReport::default();
        let now = self.core.clock.now_ms();
        let rows = self
            .core
            .store
            .eligible_inputs(self.core.session, now)
            .await?;
        for r in rows {
            match consumption_plan(&r) {
                Consumption::Control(intent) => {
                    // The audit append first, then the binding — the
                    // row may only lose eligibility once its
                    // `control/interrupt` event exists (the same
                    // admit-before-consume ordering as message rows).
                    let stale = self.control_intent_is_stale(&intent).await?;
                    let ev = self
                        .core
                        .append_event(EventKind::ControlInterrupt, intent.event_payload(stale))
                        .await?;
                    if stale {
                        tracing::info!(
                            session = %self.core.session.0,
                            target_epoch = ?intent.target_epoch,
                            "stale control/interrupt row consumed (audit only)"
                        );
                    } else {
                        // A row for the current episode always
                        // interrupts (ADR-014): flip the shared
                        // drain flag — `supervise` and `drain_episode`
                        // settle at the next safe boundary.
                        report.control_interrupt = true;
                        self.core.interrupt.store(true, Ordering::Release);
                    }
                    self.bind_consumed(&r, ev.seq).await?;
                }
                plan => {
                    if control_only {
                        continue; // the final take leaves message rows eligible
                    }
                    let admitted_seq = match plan {
                        Consumption::Single(payload) => {
                            let ev = self
                                .core
                                .append_event(EventKind::InputAdmitted, payload)
                                .await?;
                            ev.seq
                        }
                        Consumption::SpillPair { admitted, spill } => {
                            let (admitted_ev, _spilled_ev) =
                                self.core.append_input_pair(admitted, spill).await?;
                            admitted_ev.seq
                        }
                        Consumption::Control(_) => unreachable!("matched above"),
                    };
                    if self.bind_consumed(&r, admitted_seq).await? {
                        report.messages.push(r);
                    }
                }
            }
        }
        Ok(report)
    }

    /// Binds one row to its committed consumption event's seq. A
    /// concurrent drain binding the row first is a duplicate take the
    /// epoch fence will shortly fence out — skipped, never fatal.
    /// Returns whether this drain owns the binding.
    async fn bind_consumed(&self, r: &InputRow, seq: u64) -> Result<bool, EngineError> {
        match self
            .core
            .store
            .mark_consumed(self.core.session, r.id, seq)
            .await
        {
            Ok(()) => Ok(true),
            // A concurrent drain bound this row first: our event is a
            // duplicate take that the epoch fence will shortly fence
            // out entirely; skip the row, don't fail the drain.
            Err(kian_store::InboxError::AlreadyConsumed { .. }) => {
                tracing::debug!(
                    session = %self.core.session.0,
                    input = %r.id,
                    "input already consumed by a concurrent drain"
                );
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The stale rule (ADR-014): a control row is *stale* when the
    /// episode it targeted has already settled — the stream carries a
    /// `session/ended` at the row's `target_epoch`. A row for this
    /// drain's own claiming epoch can never be stale (no end write
    /// exists at it yet), and neither can a row targeting an episode a
    /// takeover resumed after its owner died mid-episode (that episode
    /// never settled — the resumed work is the work the interrupt
    /// intended to stop). A `null` target (unreadable stream at admit)
    /// always interrupts.
    async fn control_intent_is_stale(
        &self,
        intent: &kian_core::control::InterruptIntent,
    ) -> Result<bool, EngineError> {
        let Some(target) = intent.target_epoch else {
            return Ok(false);
        };
        if target == self.core.epoch.0 {
            return Ok(false); // targets this drain's own claim
        }
        let events = self.events().await?;
        Ok(events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }) && e.epoch.0 == target))
    }

    /// Pure read: does the session hold eligible-but-unconsumed input?
    /// Legal at any time (it takes nothing).
    pub async fn has_eligible_inputs(&self) -> Result<bool, EngineError> {
        let now = self.core.clock.now_ms();
        Ok(!self
            .core
            .store
            .eligible_inputs(self.core.session, now)
            .await?
            .is_empty())
    }

    /// The end write (constraint 4): `session/ended` as one atomic
    /// append via `append_end`.
    pub async fn end_session(&self, reason: EndReason) -> Result<Event, EngineError> {
        let ev = self
            .core
            .append_event(EventKind::SessionEnded { reason }, json!({}))
            .await?;
        self.core.in_turn.store(false, Ordering::Release);
        Ok(ev)
    }
}

/// What one drain attempt concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainOutcome {
    /// No work existed (spurious wake): released without any event.
    ReleasedClean,
    /// The end write landed with this reason.
    Ended { reason: EndReason },
    /// The stream projects terminal; nothing may be appended.
    Terminal,
}

/// What episode supervision concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Supervised {
    Interrupted,
    Settled,
    Stopped {
        reason: String,
    },
    SoftFailed {
        reason: String,
    },
    /// The driver stream ended without a terminal outcome — treated as
    /// an honest failure, never `completed` (constraint 7).
    StreamEnded,
}

/// The Admit → Wake → Drain → Promote engine (DESIGN §2.3).
pub struct Engine<D: LoopDriver> {
    store: EventStore,
    driver: Arc<D>,
    config: EngineConfig,
    clock: Arc<dyn Clock>,
    wakes: WakeRegistry,
    /// One entry per draining session: per-session serial,
    /// cross-session concurrent.
    drains: DashMap<SessionId, DrainHandle>,
    metrics: Arc<Metrics>,
    weak: std::sync::OnceLock<Weak<Engine<D>>>,
    /// READ-MODEL §5 (P1): whether projection/recovery state reads may
    /// consult verified snapshots (and the tail cap). Default
    /// `ReadModelConfig::default()` keeps every pre-P1 behavior
    /// byte-identical; the kian serve assembly turns it on from
    /// `read_model.snapshots`.
    read_model: crate::snapshots::ReadModelConfig,
}

impl<D> Engine<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    /// Engine with the system clock.
    #[must_use]
    pub fn new(store: EventStore, driver: Arc<D>, config: EngineConfig) -> Arc<Self> {
        Self::with_clock(store, driver, config, Arc::new(SystemClock))
    }

    /// Engine with an injectable clock (deterministic lease tests).
    #[must_use]
    pub fn with_clock(
        store: EventStore,
        driver: Arc<D>,
        config: EngineConfig,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Self::assemble(
            store,
            driver,
            config,
            clock,
            crate::snapshots::ReadModelConfig::default(),
        )
    }

    /// Engine with the system clock and an explicit read-model config
    /// (READ-MODEL §5): `read_model.snapshots_enabled` routes
    /// projection/recovery state reads through the verified-snapshot
    /// loader; the default (disabled) keeps every behavior identical.
    #[must_use]
    pub fn new_with_read_model(
        store: EventStore,
        driver: Arc<D>,
        config: EngineConfig,
        read_model: crate::snapshots::ReadModelConfig,
    ) -> Arc<Self> {
        Self::assemble(store, driver, config, Arc::new(SystemClock), read_model)
    }

    fn assemble(
        store: EventStore,
        driver: Arc<D>,
        config: EngineConfig,
        clock: Arc<dyn Clock>,
        read_model: crate::snapshots::ReadModelConfig,
    ) -> Arc<Self> {
        let metrics = Arc::new(Metrics::default());
        let engine = Arc::new(Self {
            store,
            driver,
            config,
            clock,
            wakes: WakeRegistry::with_metrics(Arc::clone(&metrics)),
            drains: DashMap::new(),
            metrics,
            weak: std::sync::OnceLock::new(),
            read_model,
        });
        engine
            .weak
            .set(Arc::downgrade(&engine))
            .expect("weak set exactly once at construction");
        engine
    }

    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The pendingWake depth gauge.
    #[must_use]
    pub fn pending_wake_depth(&self) -> usize {
        self.wakes.len()
    }

    #[must_use]
    pub fn pending_wake_contains(&self, session: &SessionId) -> bool {
        self.wakes.contains(session)
    }

    /// Whether a drain is in flight for `session`.
    #[must_use]
    pub fn is_draining(&self, session: &SessionId) -> bool {
        self.drains.contains_key(session)
    }

    /// Appends `session/created` (epoch genesis — no claim involved).
    pub async fn create_session(&self) -> Result<SessionId, EngineError> {
        let sid = SessionId::new();
        let head = self.store.head(sid).await?;
        let event = Event {
            seq: head.next_seq,
            session: sid,
            epoch: Epoch::GENESIS,
            ts: self.clock.now_ms(),
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        self.store
            .append(
                sid,
                event,
                AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: head.next_seq,
                },
            )
            .await?;
        self.metrics.on_state_transition();
        Ok(sid)
    }

    /// Closes an **idle, already-ended** session terminally (ADR-035):
    /// one claim + `drain/claimed` + `session/ended{cancelled}` in one
    /// atomic batch, under a fresh claim epoch. This is what frees a
    /// worker's 1:1 slot after its thread ended `interrupted`/`failed`:
    /// the state machine has no `idle → terminal` edge, so the close
    /// must take ownership first, and `cancelled` is what makes the
    /// release safe (a terminal session can never resume into the slot
    /// a newer thread now holds).
    ///
    /// Fail-closed gates, all before the claim: the projection is
    /// `idle`, the last event is `session/ended{interrupted|failed}`
    /// (a session that never ended is not releasable), no eligible
    /// inbox row exists (queued/resumable work is never discarded), and
    /// no drain is in flight. The claim then fences a live holder on
    /// another replica. The batch is one transaction: a crash before it
    /// leaves the projection `idle` (retryable), never a half-close.
    ///
    /// The caller owns the domain meaning of `end_bytes` (the worker
    /// release payload); the engine only needs a JSON object.
    pub async fn release_idle(
        &self,
        session: SessionId,
        end_bytes: Value,
    ) -> Result<ReleaseOutcome, EngineError> {
        match self.drains.entry(session) {
            Entry::Occupied(_) => return Ok(ReleaseOutcome::Busy),
            Entry::Vacant(v) => {
                v.insert(DrainHandle::new(session, self.clock.now_ms()));
            }
        }
        let outcome = self.release_idle_inner(session, end_bytes).await;
        self.drains.remove(&session);
        if !matches!(outcome, Ok(ReleaseOutcome::Released { .. })) && self.wakes.take(&session) {
            // A wake folded onto this release (e.g. a submit raced it):
            // the session is still open, so re-drive it — a folded wake
            // must never be lost (constraint 6).
            self.wake(session);
        }
        outcome
    }

    async fn release_idle_inner(
        &self,
        session: SessionId,
        end_bytes: Value,
    ) -> Result<ReleaseOutcome, EngineError> {
        let events = self.store.rebuild(session).await?;
        if events.is_empty() {
            return Ok(ReleaseOutcome::Unknown);
        }
        let state = project(&events)?;
        if state.is_terminal() {
            return Ok(ReleaseOutcome::AlreadyTerminal);
        }
        if state != SessionState::Idle {
            return Ok(ReleaseOutcome::Busy);
        }
        let ended = matches!(
            events.last().map(|e| &e.kind),
            Some(EventKind::SessionEnded {
                reason: EndReason::Interrupted | EndReason::Failed
            })
        );
        if !ended {
            return Ok(ReleaseOutcome::NotEligible);
        }
        let now = self.clock.now_ms();
        if !self.store.eligible_inputs(session, now).await?.is_empty() {
            return Ok(ReleaseOutcome::NotEligible);
        }
        let lease = match self
            .store
            .claim(session, &self.config.holder, self.config.lease_ttl_ms, now)
            .await
        {
            Ok(lease) => lease,
            Err(kian_store::InboxError::ClaimRejected { .. }) => return Ok(ReleaseOutcome::Busy),
            Err(e) => return Err(e.into()),
        };
        let head = self.store.head(session).await?;
        let claim = Event {
            seq: head.next_seq,
            session,
            epoch: lease.epoch,
            ts: now,
            kind: EventKind::DrainClaimed,
            bytes: json!({"holder": self.config.holder, "epoch": lease.epoch.0}),
            version: SCHEMA_VERSION,
        };
        let end = Event {
            seq: head.next_seq + 1,
            session,
            epoch: lease.epoch,
            ts: now,
            kind: EventKind::SessionEnded {
                reason: EndReason::Cancelled,
            },
            bytes: end_bytes,
            version: SCHEMA_VERSION,
        };
        self.store
            .append_batch(
                session,
                &[claim, end],
                AppendGuard {
                    epoch: lease.epoch,
                    expected_seq: head.next_seq,
                },
            )
            .await?;
        // Mirror the drain's accounting for the two events in the batch:
        // the claim and the terminal end are two state transitions, and
        // the end is one end write.
        self.metrics.on_state_transition();
        self.metrics.on_state_transition();
        self.metrics.on_end_write();
        Ok(ReleaseOutcome::Released {
            seq: head.next_seq + 1,
            epoch: lease.epoch.0,
        })
    }

    /// Projects the session from its event stream (constraint 1: state
    /// is only ever read here, never cached). Equivalent to
    /// [`Engine::projection_versioned`] with no `since`.
    pub async fn projection(&self, session: SessionId) -> Result<SessionState, EngineError> {
        let read = self.projection_versioned(session, None).await?;
        Ok(read
            .state
            .expect("a changed versioned read always carries state"))
    }

    /// The versioned projection read (READ-MODEL §3, P0): `version`
    /// is the last committed seq (`head.next_seq - 1`); `since ==
    /// version` answers `unchanged` with no state and no scan beyond
    /// the head check; `since > version` is
    /// [`EngineError::SinceAhead`]. Delegates to
    /// [`crate::read::versioned_projection`], or — when the read model
    /// is enabled (READ-MODEL §5, P1) — to
    /// [`crate::snapshots::versioned_projection_via_snapshots`]:
    /// identical wire semantics, state from the highest verified
    /// snapshot + tail replay, honest full-derivation fallback.
    pub async fn projection_versioned(
        &self,
        session: SessionId,
        since: Option<u64>,
    ) -> Result<crate::read::VersionedProjection, EngineError> {
        if self.read_model.snapshots_enabled {
            crate::snapshots::versioned_projection_via_snapshots(
                &self.store,
                session,
                since,
                &self.read_model,
            )
            .await
        } else {
            crate::read::versioned_projection(&self.store, session, since).await
        }
    }

    /// Admits one input: **durable row first**, then wake (constraint
    /// 6). On any admit failure the wake registry is untouched.
    pub async fn admit(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<AdmitOutcome, EngineError> {
        let outcome = self.admit_row(session, content, idempotency_key).await?;
        // wake transport (ADR-009 decision 4): publish the advisory
        // cross-replica hint. The admit INSERT above is a single
        // statement, so it has *committed* by the time we get here —
        // the notify can never be observed before its row is visible
        // (durable-before-wake holds for remote replicas too). The
        // notify is best-effort: a failure is logged and skipped (a
        // wake is losable by invariant; the sweep ticker and each
        // replica's startup scan bound the miss). sqlite is a no-op —
        // the in-process registry below is the single-process
        // transport.
        if let Err(e) = self.store.notify_wake(session).await {
            tracing::warn!(
                session = %session.0,
                error = %e,
                "wake notify failed (advisory — sweep/startup scan bounds the miss)"
            );
        }
        self.wake(session);
        Ok(outcome)
    }

    /// Admits one input **quietly** (MEMORY-MODEL §6, P2 plan §2.2):
    /// the durable row lands exactly like [`Engine::admit`] — same
    /// idempotency semantics, constraint 6's durable-row-first
    /// ordering — but **no wake fires and no cross-replica NOTIFY hint
    /// is published**. Wakes are advisory (invariant 2): the next
    /// `submit`'s wake discovers the row and drains it first (the
    /// inbox scan orders by admission), and the startup scan / sweep
    /// bound the miss across restarts. The creation-time memory
    /// injection uses this so the injected index row is durable
    /// before any client input without arming a drain on a session
    /// that has nothing to run yet.
    pub async fn admit_quiet(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<AdmitOutcome, EngineError> {
        self.admit_row(session, content, idempotency_key).await
    }

    /// The shared durable-row write behind [`Engine::admit`] and
    /// [`Engine::admit_quiet`]: build the row, persist it, stop there.
    /// Any wake/notify is the caller's decision.
    async fn admit_row(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<AdmitOutcome, EngineError> {
        let now = self.clock.now_ms();
        let input = InputRow {
            id: Uuid::new_v4(),
            session,
            payload: content,
            admitted_at: now,
            eligible_at: now,
            idempotency_key,
        };
        // Constraint 6: the row is durable BEFORE any wake may fire.
        // On any store failure we return without touching the registry.
        Ok(self.store.admit(input).await?)
    }

    /// Steers a running session: a synthetic (strippable) input that
    /// lands at the next provider-turn safe boundary.
    pub async fn steer(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<AdmitOutcome, EngineError> {
        self.admit(
            session,
            json!({"synthetic": true, "content": content}),
            idempotency_key,
        )
        .await
    }

    /// Wakes the session. If a drain is in flight the wake folds onto
    /// pendingWake (the drain re-evaluates the inbox before releasing);
    /// otherwise a drain task is spawned. Concurrency-safe: racing
    /// wakes yield exactly one `Started`.
    pub fn wake(&self, session: SessionId) -> WakeOutcome {
        match self.drains.entry(session) {
            Entry::Occupied(_) => {
                self.wakes.register(&session);
                WakeOutcome::Folded
            }
            Entry::Vacant(v) => {
                let handle = DrainHandle::new(session, self.clock.now_ms());
                let interrupt = handle.interrupt_flag();
                v.insert(handle);
                match self.weak.get().and_then(Weak::upgrade) {
                    Some(engine) => {
                        tokio::spawn(async move {
                            engine.drain_task(session, interrupt).await;
                        });
                        WakeOutcome::Started
                    }
                    None => {
                        // Engine being torn down; the map entry still
                        // guarantees a later wake re-spawns.
                        tracing::debug!("engine dropped before drain spawn");
                        WakeOutcome::Folded
                    }
                }
            }
        }
    }

    /// Requests an interrupt of the draining session (honored at the
    /// next safe boundary; the drain then settles on abort). Interrupt
    /// of an idle or missing session is a no-op (invariant 5).
    ///
    /// (ADR-014): the intent is *durable*. Whenever work may still
    /// be in flight — a local drain handle exists, or the projection is
    /// `running`/`recoverable` (the owner is another replica or dead) —
    /// the control plane admits a reserved control-envelope inbox row
    /// (idempotent per `(session, target_epoch)`, so repeated RPCs in
    /// one episode dedupe to one row) *in addition to* the in-memory
    /// flag. The row survives a crash and crosses replicas as data; the
    /// owning drain consumes it into a `control/interrupt` event at its
    /// next boundary. The control plane still never appends events
    /// itself (constraint 9), and no wake is fired: a drain in flight
    /// reaches the row at its next boundary take; a dead owner's
    /// successor reaches it via the takeover path (recovery sweep /
    /// startup scan). Idle/terminal sessions keep the exact
    /// `NotDraining`/`UnknownSession` behavior and admit nothing.
    pub async fn interrupt(&self, session: SessionId) -> Result<InterruptOutcome, EngineError> {
        self.interrupt_as(session, &self.config.holder).await
    }

    /// [`Engine::interrupt`] with a caller-supplied requester identity:
    /// the durable intent row — and the `control/interrupt` audit event
    /// it consumes into — carries `requested_by` verbatim instead of
    /// `EngineConfig::holder`. The child inactivity bound names
    /// `requested_by:"child_timeout"` so the join derives `stalled`
    /// (MULTIAGENT-MODEL §5.3:587, §6.2 row 6). Idempotency is
    /// structural and unchanged: the row keys on
    /// `interrupt:<session>:<target_epoch>` (EVENT-MODEL:203-205), so a
    /// second request for the same episode commits nothing. Additive:
    /// [`Engine::interrupt`] keeps its signature and behavior (the
    /// holder remains its requester).
    pub async fn interrupt_as(
        &self,
        session: SessionId,
        requested_by: &str,
    ) -> Result<InterruptOutcome, EngineError> {
        let has_handle = if let Some(handle) = self.drains.get(&session) {
            handle.interrupt();
            true
        } else {
            false
        };

        // Target epoch = the last event's epoch (the episode the intent
        // aims to stop); null when the stream is unreadable — the local
        // flag (if any) still carries the request.
        let events = match self.store.rebuild(session).await {
            Ok(events) => Some(events),
            Err(e) if has_handle => {
                tracing::warn!(
                    session = %session.0,
                    error = %e,
                    "interrupt: stream unreadable — durable row targets null epoch"
                );
                None
            }
            Err(e) => return Err(e.into()),
        };
        let Some(events) = events else {
            self.admit_interrupt_row(session, None, requested_by)
                .await?;
            return Ok(InterruptOutcome::Requested);
        };
        if events.is_empty() {
            return Ok(InterruptOutcome::UnknownSession);
        }
        let target_epoch = events.last().map(|e| e.epoch.0);
        if has_handle {
            // Best-effort terminal check: a handle on a frozen stream
            // is a finishing drain — the flag path settles it; no row.
            let terminal = project(&events).map(|s| s.is_terminal()).unwrap_or(false);
            if !terminal {
                self.admit_interrupt_row(session, target_epoch, requested_by)
                    .await?;
            }
            return Ok(InterruptOutcome::Requested);
        }
        let state = project(&events)?;
        if state.is_terminal() || state == SessionState::Idle {
            return Ok(InterruptOutcome::NotDraining { state });
        }
        // Running/Recoverable with no local drain: the owner is another
        // replica or dead. The durable row IS the interrupt — the
        // consuming drain (takeover or remote owner) honors it.
        self.admit_interrupt_row(session, target_epoch, requested_by)
            .await?;
        Ok(InterruptOutcome::Requested)
    }

    /// Admits the durable interrupt-intent row. Durable-first
    /// (constraint 6): the row exists before this returns; no wake —
    /// see [`Engine::interrupt`]. `requested_by` names the requester
    /// written into the row (the holder for [`Engine::interrupt`], the
    /// caller's identity for [`Engine::interrupt_as`]).
    async fn admit_interrupt_row(
        &self,
        session: SessionId,
        target_epoch: Option<u64>,
        requested_by: &str,
    ) -> Result<(), EngineError> {
        let now = self.clock.now_ms();
        let intent = kian_core::control::InterruptIntent {
            reason: None,
            requested_by: Some(requested_by.to_string()),
            requested_at_ms: now,
            target_epoch,
        };
        let input = InputRow {
            id: Uuid::new_v4(),
            session,
            payload: intent.envelope(),
            admitted_at: now,
            eligible_at: now,
            idempotency_key: Some(kian_core::control::InterruptIntent::idempotency_key(
                &session,
                target_epoch,
            )),
        };
        let outcome = self.store.admit(input).await?;
        if outcome.deduplicated {
            tracing::debug!(
                session = %session.0,
                target_epoch = ?target_epoch,
                "interrupt row deduplicated (one durable intent per episode)"
            );
        }
        Ok(())
    }

    /// Startup NoLostWork rebuild: registers a pending wake for every
    /// session the inbox scan reports with eligible-but-unconsumed
    /// input, then drains them. Returns the number of sessions found.
    pub async fn startup_rebuild(&self) -> Result<usize, EngineError> {
        let inboxes = self.store.startup_scan(self.clock.now_ms()).await?;
        let sessions: Vec<SessionId> = inboxes.iter().map(|i| i.session).collect();
        let count = sessions.len();
        for s in &sessions {
            self.wakes.register(s);
        }
        for s in sessions {
            self.wake(s);
        }
        Ok(count)
    }

    /// Lease-expiry recovery sweep (recovery detection, DESIGN §2.4
    /// R4 and the §13 availability target "recoverable detection ≤ lease
    /// TTL + heartbeat interval"): wakes every session whose lease is
    /// past its TTL *and* whose stream projects `running` — the previous
    /// drain owner lost liveness. The wake is advisory and safe: the
    /// takeover claim issues a fresh epoch, so any lingering old owner
    /// (a zombie process across the kill window) has every write
    /// rejected by the store fence (NoSplitBrainWrite, ADR-005); a
    /// drain that is still live keeps its lease fresh via heartbeat and
    /// never surfaces here. Sessions recovered this way are re-driven
    /// by the takeover path in [`Engine::drain_episode`] — including
    /// killed-mid-turn sessions whose inbox is already empty. Returns
    /// the woken session ids.
    pub async fn recovery_sweep(&self) -> Result<Vec<SessionId>, EngineError> {
        let expired = self.store.expired_leases(self.clock.now_ms()).await?;
        let sessions: Vec<SessionId> = expired.iter().map(|l| l.session).collect();
        for s in &sessions {
            self.wake(*s);
        }
        Ok(sessions)
    }

    /// Per-session drain task: claim → heartbeat → episodes → end
    /// write → release, repeating while folded wakes remain. On
    /// interrupt, ownership is released with pendingWake deliberately
    /// left registered (settle-on-abort, invariant 6). A rejected claim
    /// (fresh lease held by another holder, /ADR-009) is an honest
    /// no-op exit: nothing was appended and the folded wake is left for
    /// the live holder's sweep — never a retry loop (constraint 8).
    async fn drain_task(self: Arc<Self>, session: SessionId, interrupt: Arc<AtomicBool>) {
        let started_at = self.clock.now_ms();
        self.metrics.on_drain_started();
        let mut terminal_seen = false;
        let mut claim_rejected = false;
        loop {
            match self.drain_once(&session, &interrupt).await {
                Ok(DrainOutcome::Terminal) => terminal_seen = true,
                Ok(_) => {}
                Err(e) => {
                    if matches!(
                        &e,
                        EngineError::Inbox(kian_store::InboxError::ClaimRejected { .. })
                    ) {
                        // (ADR-009): another holder owns a fresh
                        // lease. Honest no-op exit — the claim failed
                        // before any append, so this drain wrote
                        // nothing, and the live holder's sweep or wake
                        // transport will service the input. Retrying
                        // would ping-pong the epoch between replicas
                        // (constraint 8: never a retry loop), so the
                        // folded wake is deliberately left registered
                        // and not re-taken here.
                        claim_rejected = true;
                        tracing::info!(
                            session = %session.0,
                            error = %e,
                            "claim rejected: session freshly held elsewhere — no-op drain exit"
                        );
                    } else {
                        // A fenced-out (stale-epoch) or failed attempt:
                        // the stream is left to the epoch winner —
                        // never guessed.
                        tracing::warn!(session = %session.0, error = %e, "drain attempt ended");
                    }
                }
            }
            if terminal_seen {
                // Terminal sessions freeze; a pending wake for one is
                // spurious (its inputs stay explicitly pending).
                self.wakes.take(&session);
            }
            self.drains.remove(&session);
            if interrupt.load(Ordering::Acquire) {
                break; // keep pendingWake registered for the resumer
            }
            if claim_rejected {
                break; // advisory pending wake stays for the holder's sweep
            }
            if !terminal_seen && self.wakes.take(&session) {
                tracing::debug!(session = %session.0, "folded wake: re-draining");
                continue;
            }
            break;
        }
        self.metrics
            .on_drain_finished(self.clock.now_ms().saturating_sub(started_at));
    }

    /// One drain attempt under a fresh claim, with a heartbeat task
    /// extending the lease for its duration (liveness only).
    async fn drain_once(
        &self,
        session: &SessionId,
        interrupt: &Arc<AtomicBool>,
    ) -> Result<DrainOutcome, EngineError> {
        let lease = self
            .store
            .claim(
                *session,
                &self.config.holder,
                self.config.lease_ttl_ms,
                self.clock.now_ms(),
            )
            .await?;
        let hb_stop = Arc::new(Notify::new());
        let heartbeat = {
            let store = self.store.clone();
            let clock = Arc::clone(&self.clock);
            let stop = Arc::clone(&hb_stop);
            let s = *session;
            let interval = self.config.heartbeat_interval_ms;
            tokio::spawn(async move {
                heartbeat_loop(store, s, clock, interval, stop).await;
            })
        };
        let result = self.drain_episode(session, lease, interrupt).await;
        hb_stop.notify_one();
        let _ = heartbeat.await;
        result
    }

    /// The drain loop body (DESIGN §2.3): take eligible inputs only at
    /// safe boundaries, run driver episodes, promote only at
    /// provider-turn boundaries (each folded wake batch resets the
    /// allowance once), settle on abort, and release via the atomic end
    /// write.
    async fn drain_episode(
        &self,
        session: &SessionId,
        lease: LeaseRecord,
        interrupt: &Arc<AtomicBool>,
    ) -> Result<DrainOutcome, EngineError> {
        // the ctx shares the drain's interrupt flag — an
        // `interrupt` RPC and a consumed current-epoch control row flip
        // the same switch.
        let ctx = DrainCtx::with_interrupt(
            self.store.clone(),
            *session,
            lease.epoch,
            Arc::clone(&self.clock),
            Arc::clone(&self.metrics),
            Arc::clone(interrupt),
        )
        .await?;
        let mut claimed = false;
        // takeover (DESIGN §2.4 R4/R5): a stream that already
        // projects `running` when we claim belongs to a drain that died
        // mid-episode (process death; its lease expired or it lost
        // liveness). The takeover claim re-owns the session and re-drives
        // the loop to an honest settle *even when the inbox is empty* —
        // the killed episode's inputs were already consumed, so only a
        // re-drive can finish their work. Without this, a killed-mid-turn
        // session would sit in `running` forever (the startup inbox scan
        // sees nothing for it) and the expired-lease sweep would churn.
        //
        // READ-MODEL §5 (P1): the state read goes through the
        // verified-snapshot loader when the read model is enabled —
        // snapshot + tail replay with the honest full-derivation
        // fallback; a recoverable session's resume cost is O(tail),
        // not O(history).
        let mut takeover = false;
        let mut end_reason: EndReason;
        'episodes: loop {
            if interrupt.load(Ordering::Acquire) && claimed {
                end_reason = EndReason::Interrupted;
                break 'episodes;
            }
            let version = self.store.version(*session).await?;
            let state = if version == 0 {
                None
            } else {
                Some(
                    crate::snapshots::state_at(&self.store, *session, version, &self.read_model)
                        .await?,
                )
            };
            if state.is_none() {
                if !ctx.has_eligible_inputs().await? {
                    return Ok(DrainOutcome::ReleasedClean);
                }
                ctx.append(EventKind::SessionCreated, json!({})).await?;
            } else if state.is_some_and(|s| s.is_terminal()) {
                return Ok(DrainOutcome::Terminal);
            }
            if !claimed {
                takeover = state == Some(SessionState::Running);
                if !ctx.has_eligible_inputs().await? && !takeover {
                    // Spurious wake: release without any event write.
                    return Ok(DrainOutcome::ReleasedClean);
                }
                ctx.append(
                    EventKind::DrainClaimed,
                    json!({"holder": self.config.holder, "epoch": lease.epoch.0}),
                )
                .await?;
                claimed = true;
                if interrupt.load(Ordering::Acquire) {
                    end_reason = EndReason::Interrupted;
                    break 'episodes;
                }
            }
            let taken = ctx.take_eligible_inputs().await?;
            // a current-epoch control row consumed by that take
            // flips the shared flag — honor it now, before the
            // empty-inbox branch below can settle `completed` past it.
            if ctx.interrupt_requested() && claimed {
                end_reason = EndReason::Interrupted;
                break 'episodes;
            }
            if taken.is_empty() && !takeover {
                if !claimed {
                    return Ok(DrainOutcome::ReleasedClean);
                }
                // The supervision continued but nothing new became
                // eligible: settle completed with an empty inbox.
                end_reason = EndReason::Completed;
                break 'episodes;
            }
            // Only the first episode of this attempt is the takeover
            // resume; later iterations settle on a normally empty inbox.
            takeover = false;
            match self
                .supervise(self.driver.drive(ctx.clone()), interrupt)
                .await
            {
                Supervised::Interrupted => {
                    end_reason = EndReason::Interrupted;
                    break 'episodes;
                }
                Supervised::Settled => {
                    if ctx.has_eligible_inputs().await? {
                        // Steer allowance reset: one more episode per
                        // newly eligible batch.
                        continue 'episodes;
                    }
                    end_reason = EndReason::Completed;
                    break 'episodes;
                }
                Supervised::Stopped { reason } => {
                    tracing::info!(session = %session.0, reason, "loop stopped intentionally");
                    end_reason = EndReason::Completed;
                    break 'episodes;
                }
                Supervised::SoftFailed { reason } => {
                    // Honest failure (constraint 7); 's policy table
                    // will intercept this before the end write.
                    tracing::warn!(session = %session.0, reason, "soft failure");
                    end_reason = EndReason::Failed;
                    break 'episodes;
                }
                Supervised::StreamEnded => {
                    tracing::warn!(session = %session.0, "driver stream ended without settling");
                    end_reason = EndReason::Failed;
                    break 'episodes;
                }
            }
        }
        // Settle on abort (invariant 6): re-evaluate the inbox BEFORE
        // releasing ownership; pending work keeps pendingWake registered.
        if ctx.has_eligible_inputs().await? {
            self.wakes.register(session);
        }
        // final take: one last control-only pass before the end
        // write — a current-epoch control row that raced the settle is
        // consumed here and flips the end write to `interrupted` (the
        // honest "stop before done"; DESIGN §2.4 records the race). A
        // stale row is appended for audit and ends nothing. Regular
        // inputs stay eligible for the resumer (the registered wake).
        // A settle that broke mid-turn (crash / soft-failure paths)
        // has no safe boundary: the final take is skipped — the
        // interrupt flag owns that case, and any pending control row
        // stays for the resumer.
        match ctx.take_control_inputs().await {
            Ok(fired) => {
                if fired {
                    end_reason = EndReason::Interrupted;
                }
            }
            Err(EngineError::NotAtSafeBoundary { .. }) => {}
            Err(e) => return Err(e),
        }
        ctx.end_session(end_reason).await?;
        Ok(DrainOutcome::Ended { reason: end_reason })
    }

    /// Consumes driver outcomes; the interrupt flag is honored at every
    /// outcome (each is a safe boundary by construction).
    async fn supervise(
        &self,
        mut stream: BoxStream<'static, LoopOutcome>,
        interrupt: &AtomicBool,
    ) -> Supervised {
        let interrupted = || interrupt.load(Ordering::Acquire);
        while let Some(outcome) = stream.next().await {
            let supervised = match outcome {
                LoopOutcome::TurnDone => {
                    if interrupted() {
                        Supervised::Interrupted
                    } else {
                        continue;
                    }
                }
                LoopOutcome::Settled => {
                    if interrupted() {
                        Supervised::Interrupted
                    } else {
                        Supervised::Settled
                    }
                }
                LoopOutcome::Stopped { reason } => {
                    if interrupted() {
                        Supervised::Interrupted
                    } else {
                        Supervised::Stopped { reason }
                    }
                }
                LoopOutcome::SoftFailed { reason } => {
                    if interrupted() {
                        Supervised::Interrupted
                    } else {
                        Supervised::SoftFailed { reason }
                    }
                }
            };
            return supervised;
        }
        Supervised::StreamEnded
    }
}

/// The lease heartbeat task (liveness only, ADR-005): extends the lease
/// every interval until stopped. The stop signal interrupts the tick
/// wait immediately so ownership release is never delayed by a tick.
/// A rejected heartbeat is logged and retried on the next tick — it
/// never aborts the drain (safety is the epoch fence's job).
async fn heartbeat_loop(
    store: EventStore,
    session: SessionId,
    clock: Arc<dyn Clock>,
    interval_ms: u64,
    stop: Arc<Notify>,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = stop.notified() => break,
        }
        if let Err(e) = store.heartbeat(session, clock.now_ms()).await {
            tracing::debug!(session = %session.0, error = %e, "heartbeat rejected (liveness only)");
        }
    }
}

/// What one consumption pass took. Message rows are returned to
/// the caller; control rows are consumed for audit and — when current
/// — flip the drain's interrupt flag instead.
#[derive(Default)]
struct TakeReport {
    messages: Vec<InputRow>,
    control_interrupt: bool,
}

/// The engine's input-row convention: what one row consumes to. A plain
/// row's payload IS the message content; a steer row wraps it as
/// `{"synthetic": true, "content": X}` (exactly those two keys —
/// kian-96, review N-1: a superset stays ordinary content, so the
/// public wire cannot mint the strippable marker through it) so the
/// marker survives
/// the inbox round-trip (DESIGN §3.2 payload contract); the spill
/// envelope `{"content": X, "spill": Y}` (exactly those two keys)
/// consumes to the admitted message plus its non-context-bearing sidecar
/// in one fenced write batch; the dream envelope
/// `{"content": X, "dream": M}` (exactly those two keys — kian-91,
/// review B-2) consumes to ONE admitted message carrying the trusted,
/// `dream` marker alongside `role`/`content` (the marker is
/// additive and non-context: dreamer routing and provenance read it,
/// never the message text); the memory envelope
/// `{"content": X, "memory": M}` (exactly those two keys — kian-99,
/// MEMORY-MODEL §6 I1) consumes to ONE admitted **system** message
/// carrying the creation-time injection provenance marker alongside
/// `role`/`content`/`synthetic:true` (the marker is additive and
/// non-context: `derive_model_context` reads role/content/synthetic
/// only). The envelopes are row-level conventions
/// only — the committed event payload contracts are unchanged (the
/// marker rides EVENT-MODEL §4.2's additive provenance keys). Like the
/// steer wrapper before it, each exact two-key shape is reserved: a
/// plain payload of that precise shape is read as an envelope, not
/// content.
enum Consumption {
    /// One `input/admitted` event (the entire legacy surface).
    Single(Value),
    /// The pair: `input/admitted` then `input/spilled`, appended
    /// atomically under one CAS (`append_batch`).
    SpillPair { admitted: Value, spill: Value },
    /// The reserved control envelope: a `control/interrupt` audit
    /// event, never a message. The row is bound to the event's seq like
    /// any other consumption.
    Control(kian_core::control::InterruptIntent),
}

fn consumption_plan(row: &InputRow) -> Consumption {
    if let Some(intent) = kian_core::control::InterruptIntent::parse(&row.payload) {
        return Consumption::Control(intent);
    }
    if let Some(obj) = row.payload.as_object() {
        // kian-96 (kian-95 review N-1): the steer wrapper is exactly
        // two keys, mirroring the spill/dream arms. A superset
        // (`synthetic:true` + `content` + extra keys) is ordinary
        // client content: the wire guard refuses only exact key-set
        // equality, so consuming a superset here would let the public
        // wire mint the strippable marker itself.
        if obj.len() == 2
            && obj.get("synthetic").and_then(Value::as_bool) == Some(true)
            && let Some(content) = obj.get("content")
        {
            return Consumption::Single(
                json!({"role": "user", "content": content, "synthetic": true}),
            );
        }
        if obj.len() == 2
            && let (Some(content), Some(spill)) = (obj.get("content"), obj.get("spill"))
        {
            return Consumption::SpillPair {
                admitted: json!({"role": "user", "content": content}),
                spill: spill.clone(),
            };
        }
        if obj.len() == 2
            && let (Some(content), Some(dream)) = (obj.get("content"), obj.get("dream"))
        {
            // kian-91 (review B-2): the ops plane's dream admit. The
            // marker rides the admitted payload verbatim — additive,
            // non-context (derive_model_context reads role/content
            // only, so request bytes are unchanged).
            return Consumption::Single(
                json!({"role": "user", "content": content, "dream": dream}),
            );
        }
        if obj.len() == 2
            && let (Some(content), Some(memory)) = (obj.get("content"), obj.get("memory"))
        {
            // kian-99 (MEMORY-MODEL §6 I1): the server's creation-time
            // memory injection admit (`admit_quiet`). ONE admitted
            // system message, strippable (`synthetic:true`), the
            // marker riding verbatim — additive, non-context
            // (derive_model_context reads role/content/synthetic
            // only, so request bytes are unchanged).
            return Consumption::Single(
                json!({"role": "system", "content": content, "synthetic": true, "memory": memory}),
            );
        }
        if obj.len() == 2
            && let (Some(content), Some(cron)) = (obj.get("content"), obj.get("cron"))
        {
            // CRON-MODEL §6.1: the scheduler's admit — ONE `input/admitted`
            // with the prompt as the message and the firing proof riding as
            // an additive, non-context key (derive_model_context reads
            // role/content only, so every firing sends the same prompt
            // bytes).
            return Consumption::Single(json!({"role": "user", "content": content, "cron": cron}));
        }
    }
    Consumption::Single(json!({"role": "user", "content": row.payload}))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::StreamExt;
    use serde_json::json;
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::{
        Clock, DrainCtx, Engine, EngineConfig, EngineError, InterruptOutcome, ManualClock,
        ReleaseOutcome, WakeOutcome,
    };
    use crate::metrics::Metrics;
    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, InputRow, SessionId};
    use kian_core::seams::{LoopDriver, LoopOutcome};
    use kian_core::state::{SessionState, project};
    use kian_store::AppendGuard;
    use kian_store::EventStore;

    const T0: u64 = 1_761_900_000_000;

    // -- helpers -----------------------------------------------------------

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-orch-test-{}.sqlite", Uuid::new_v4()))
    }

    async fn open_store() -> (EventStore, PathBuf) {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        (store, path)
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// Polls `cond` every 10 ms until it holds or `timeout_ms` elapses.
    async fn wait_for<F, Fut>(timeout_ms: u64, mut cond: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            if cond().await {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn events_of(store: &EventStore, sid: SessionId) -> Vec<Event> {
        store.rebuild(sid).await.expect("rebuild")
    }

    async fn projection_of(store: &EventStore, sid: SessionId) -> SessionState {
        let events = store.rebuild(sid).await.expect("rebuild");
        if events.is_empty() {
            // Not yet bootstrapped — indistinguishable from idle for the
            // polling assertions in these tests.
            return SessionState::Idle;
        }
        project(&events).expect("projection")
    }

    fn row(sid: SessionId, id: u128, payload: serde_json::Value) -> InputRow {
        InputRow {
            id: Uuid::from_u128(id),
            session: sid,
            payload,
            admitted_at: T0,
            eligible_at: T0,
            idempotency_key: Some(format!("key-{id}")),
        }
    }

    // -- the scripted driver ------------------------------------------------

    #[derive(Clone)]
    enum Step {
        TakeInputs,
        BeginTurn,
        EndTurn,
        Message(&'static str),
        Gate(Arc<Notify>),
        Outcome(LoopOutcome),
    }

    /// One script per `drive()` call, popped in call order.
    #[derive(Clone, Default)]
    struct ScriptedDriver {
        scripts: Arc<Mutex<VecDeque<Vec<Step>>>>,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedDriver {
        fn scripted(scripts: Vec<Vec<Step>>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(scripts.into())),
                log: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn happy_steps() -> Vec<Step> {
            vec![
                Step::BeginTurn,
                Step::Message("ok"),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::TurnDone),
                Step::Outcome(LoopOutcome::Settled),
            ]
        }

        fn default_happy() -> Self {
            Self::scripted(vec![Self::happy_steps()])
        }

        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    impl LoopDriver for ScriptedDriver {
        type Ctx = DrainCtx;

        fn drive(&self, ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
            let steps: VecDeque<Step> = self
                .scripts
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(Self::happy_steps)
                .into();
            let log = Arc::clone(&self.log);
            futures::stream::unfold((ctx, steps, log), |(ctx, mut steps, log)| async move {
                loop {
                    let step = steps.pop_front()?;
                    match step {
                        Step::TakeInputs => {
                            let taken = ctx.take_eligible_inputs().await;
                            log.lock().unwrap().push(format!("take:{taken:?}"));
                        }
                        Step::BeginTurn => {
                            let r = ctx.begin_turn().await;
                            log.lock().unwrap().push(format!("begin:{r:?}"));
                        }
                        Step::EndTurn => {
                            let r = ctx.end_turn().await;
                            log.lock().unwrap().push(format!("end:{r:?}"));
                        }
                        Step::Message(m) => {
                            let r = ctx
                                .append(
                                    EventKind::AssistantMessage,
                                    json!({"role": "assistant", "content": m}),
                                )
                                .await;
                            log.lock().unwrap().push(format!("msg:{r:?}"));
                        }
                        Step::Gate(g) => g.notified().await,
                        Step::Outcome(o) => return Some((o, (ctx, steps, log))),
                    }
                }
            })
            .boxed()
        }
    }

    fn engine(store: &EventStore, driver: Arc<ScriptedDriver>) -> Arc<Engine<ScriptedDriver>> {
        Engine::with_clock(
            store.clone(),
            driver,
            EngineConfig {
                holder: "test-holder".into(),
                lease_ttl_ms: 5_000,
                heartbeat_interval_ms: 25,
            },
            Arc::new(ManualClock::new(T0)),
        )
    }

    // -- invariant 1: durable before execute --------------------------------

    /// (1) No wake fires before the input row is durable: a failing
    /// admit (timestamp overflow at the store boundary) leaves the wake
    /// registry untouched and no drain spawned; a successful admit is
    /// durable (startup scan sees the row) before the drain runs.
    #[tokio::test]
    async fn no_wake_fires_before_the_input_row_is_durable() {
        let (store, path) = open_store().await;

        // Broken clock near u64::MAX: admit overflows sqlite's i64.
        let broken = Engine::with_clock(
            store.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig::default(),
            Arc::new(ManualClock::new(u64::MAX - 10)),
        );
        let sid = SessionId::new();
        let err = broken
            .admit(sid, json!("task"), Some("k1".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Inbox(_)), "{err}");
        assert_eq!(broken.pending_wake_depth(), 0);
        assert!(!broken.is_draining(&sid));

        // Same store, sane clock: admit succeeds, row is durable, drain runs.
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![vec![Step::Gate(
                Arc::clone(&gate),
            )]])),
        );
        let outcome = eng.admit(sid, json!("task"), Some("k1".into())).await;
        assert!(outcome.is_ok());
        assert!(eng.is_draining(&sid), "wake fired after durable admit");
        let scan = store.startup_scan(T0).await.expect("scan");
        assert_eq!(scan.len(), 1, "row must be durable");
        assert_eq!(scan[0].eligible.len(), 1);
        gate.notify_one();

        cleanup(&path);
    }

    /// kian-99: `admit_quiet` lands a durable row but arms nothing —
    /// no drain spawns and the wake registry stays empty (wake is
    /// advisory; the next submit's wake discovers the row).
    #[tokio::test]
    async fn admit_quiet_does_not_wake() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = eng.create_session().await.expect("session");

        let outcome = eng
            .admit_quiet(
                sid,
                json!({"content": "index", "memory": {"scope": "a", "items": 0}}),
                Some(format!("memory-inject:{}", sid.0)),
            )
            .await
            .expect("quiet admit");
        assert!(!outcome.deduplicated);

        assert!(
            !eng.is_draining(&sid),
            "no drain may spawn from admit_quiet"
        );
        assert_eq!(eng.pending_wake_depth(), 0, "the wake registry stays empty");
        // The row IS durable: the startup scan sees it (admit semantics
        // unchanged — only the wake is suppressed).
        let scan = store.startup_scan(T0).await.expect("scan");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].eligible.len(), 1);
        cleanup(&path);
    }

    /// kian-99 (P2 plan §2.2): a `submit` after a quiet admit drains
    /// BOTH rows in one drain, in admission order — the injected index
    /// event lands before the user's message event.
    #[tokio::test]
    async fn submit_after_admit_quiet_drains_both_rows_in_admission_order() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = eng.create_session().await.expect("session");

        let marker = json!({"scope": "ops-agent", "notes": [], "bytes": 32, "items": 1,
                            "truncated": false, "source_seq": 3});
        eng.admit_quiet(
            sid,
            json!({"content": "rendered index", "memory": marker.clone()}),
            Some(format!("memory-inject:{}", sid.0)),
        )
        .await
        .expect("quiet admit");

        // The quiet row sits dormant until this submit's wake drains.
        eng.admit(
            sid,
            json!("what changed since yesterday?"),
            Some("k2".into()),
        )
        .await
        .expect("submit");

        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
            })
            .await,
            "the session settles through the one woken drain"
        );
        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 2, "both rows consumed: {events:#?}");
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "system", "content": "rendered index", "synthetic": true,
                   "memory": marker}),
            "the injected index lands first"
        );
        assert_eq!(
            admitted[1].bytes,
            json!({"role": "user", "content": "what changed since yesterday?"}),
            "the user's message lands second"
        );
        cleanup(&path);
    }

    /// kian-99: `admit_quiet` shares `admit`'s idempotency semantics —
    /// a repeated key deduplicates (first write wins, one row).
    #[tokio::test]
    async fn admit_quiet_dedupes_on_a_repeated_key() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = eng.create_session().await.expect("session");
        let key = format!("memory-inject:{}", sid.0);

        let first = eng
            .admit_quiet(sid, json!("index-a"), Some(key.clone()))
            .await
            .expect("first quiet admit");
        assert!(!first.deduplicated);
        let second = eng
            .admit_quiet(sid, json!("index-b"), Some(key))
            .await
            .expect("repeated quiet admit");
        assert!(second.deduplicated, "first write wins");

        let scan = store.startup_scan(T0).await.expect("scan");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].eligible.len(), 1, "exactly one durable row");
        assert_eq!(scan[0].eligible[0].payload, json!("index-a"));
        cleanup(&path);
    }

    // -- invariant 2: wake is advisory, inbox scan repairs ------------------

    /// (2) Dropping every wake still drains everything after the startup
    /// inbox scan: rows admitted straight through the store (no wake
    /// ever fired) are found by `startup_rebuild` and drained to
    /// completion in both sessions.
    #[tokio::test]
    async fn dropped_wakes_still_drain_everything_after_startup_inbox_scan() {
        let (store, path) = open_store().await;
        let a = SessionId::new();
        let b = SessionId::new();
        store.admit(row(a, 1, json!("a1"))).await.unwrap();
        store.admit(row(b, 2, json!("b1"))).await.unwrap();
        store.admit(row(b, 3, json!("b2"))).await.unwrap();

        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let found = eng.startup_rebuild().await.expect("rebuild");
        assert_eq!(found, 2, "both sessions with eligible inputs");

        assert!(
            wait_for(5_000, || async {
                projection_of(&store, a).await == SessionState::Completed
                    && projection_of(&store, b).await == SessionState::Completed
            })
            .await,
            "both sessions complete"
        );
        assert!(
            store.startup_scan(T0).await.unwrap().is_empty(),
            "nothing eligible remains (NoLostWork)"
        );
        // Teardown race: the projection reports Completed as soon as the
        // final event is appended, but the drain task clears its draining
        // flag after that append — poll until the flags settle rather than
        // asserting immediately.
        assert!(
            wait_for(5_000, || async {
                !eng.is_draining(&a) && !eng.is_draining(&b)
            })
            .await,
            "draining flags cleared after both sessions complete"
        );
        cleanup(&path);
    }

    // -- invariant 3: promote only at safe boundaries -----------------------

    /// (3) A steer input's events never interleave inside an in-flight
    /// provider turn: `take_eligible_inputs` — the only path that
    /// appends `input/admitted` — refuses while a turn is open; the
    /// steer lands after `turn/end` with its synthetic marker intact.
    #[tokio::test]
    async fn steer_events_never_interleave_inside_an_inflight_provider_turn() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        // Bootstrap the stream with session/created (epoch genesis).
        let created = Event {
            seq: 1,
            session: sid,
            epoch: Epoch::GENESIS,
            ts: T0,
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        store
            .append(
                sid,
                created,
                kian_store::AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: 1,
                },
            )
            .await
            .unwrap();

        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();
        assert!(ctx.append(EventKind::DrainClaimed, json!({})).await.is_ok());

        store.admit(row(sid, 1, json!("task"))).await.unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let turn_start = ctx.begin_turn().await.unwrap();
        assert!(ctx.in_turn());
        // The steer arrives mid-turn and MUST NOT interleave.
        store
            .admit(row(sid, 2, json!({"synthetic": true, "content": "plan B"})))
            .await
            .unwrap();
        let err = ctx.take_eligible_inputs().await.unwrap_err();
        assert!(
            matches!(err, EngineError::NotAtSafeBoundary { .. }),
            "steer take refused mid-turn: {err}"
        );
        // In-turn appends (assistant output) are fine.
        assert!(
            ctx.append(
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": "working"}),
            )
            .await
            .is_ok()
        );
        let turn_end = ctx.end_turn().await.unwrap();
        assert!(!ctx.in_turn());

        let taken = ctx.take_eligible_inputs().await.unwrap();
        assert_eq!(taken.len(), 1, "steer lands after the boundary");

        let events = events_of(&store, sid).await;
        for ev in &events {
            if matches!(ev.kind, EventKind::InputAdmitted) {
                assert!(
                    ev.seq <= turn_start.seq || ev.seq > turn_end.seq,
                    "input event at seq {} inside turn [{},{}]",
                    ev.seq,
                    turn_start.seq,
                    turn_end.seq
                );
            }
        }
        let steer_ev = events
            .iter()
            .find(|e| {
                e.kind == EventKind::InputAdmitted
                    && e.bytes.get("synthetic").and_then(|v| v.as_bool()) == Some(true)
            })
            .expect("steer event with synthetic marker");
        assert_eq!(steer_ev.bytes["content"], json!("plan B"));
        assert!(steer_ev.seq > turn_end.seq);
        cleanup(&path);
    }

    // -- the spill sidecar consumption-write convention ---------------

    /// Bootstraps `sid` (session/created + drain/claimed) and returns a
    /// drain context under `epoch` — the safe-boundary surface the spill
    /// tests drive directly.
    async fn spill_ctx(store: &EventStore, sid: SessionId, epoch: Epoch) -> DrainCtx {
        let created = Event {
            seq: 1,
            session: sid,
            epoch: Epoch::GENESIS,
            ts: T0,
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        store
            .append(
                sid,
                created,
                kian_store::AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: 1,
                },
            )
            .await
            .unwrap();
        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            epoch,
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();
        ctx.append(EventKind::DrainClaimed, json!({}))
            .await
            .unwrap();
        ctx
    }

    fn spill_sidecar() -> serde_json::Value {
        json!({
            "event_id": "evt-1",
            "channel": "mock",
            "topic": null,
            "raw": {"text": "the full verbatim channel payload"}
        })
    }

    fn pair_of(events: &[Event]) -> (Event, Event) {
        let admitted = events
            .iter()
            .find(|e| e.kind == EventKind::InputAdmitted)
            .expect("input/admitted event")
            .clone();
        let spilled = events
            .iter()
            .find(|e| e.kind == EventKind::InputSpilled)
            .expect("input/spilled event")
            .clone();
        (admitted, spilled)
    }

    /// The envelope row consumes to the exact two-event pair: admitted
    /// carries the bare message payload, spilled carries the sidecar
    /// verbatim, contiguously, and the row binds to the admitted seq.
    #[tokio::test]
    async fn spill_envelope_consumes_to_the_exact_two_event_pair() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let sidecar = spill_sidecar();
        store
            .admit(row(
                sid,
                7,
                json!({"content": "inline excerpt", "spill": sidecar.clone()}),
            ))
            .await
            .unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let (admitted, spilled) = pair_of(&events);
        assert_eq!(
            admitted.bytes,
            json!({"role": "user", "content": "inline excerpt"})
        );
        assert_eq!(spilled.bytes, sidecar, "the sidecar rides verbatim");
        assert_eq!(spilled.seq, admitted.seq + 1, "immediately after admitted");
        assert_eq!(spilled.epoch, admitted.epoch, "one fenced write batch");
        // The row is consumed against the admitted event's seq, not the
        // spilled sibling's.
        assert!(store.startup_scan(T0).await.unwrap().is_empty());
        assert!(store.eligible_inputs(sid, T0).await.unwrap().is_empty());
        // The pair projects legally (drain-activity default) and the
        // sidecar never enters model context.
        let projected = project(&events).expect("pair projects");
        assert_eq!(projected, SessionState::Running);
        let messages = kian_core::context::derive_model_context(&events)
            .expect("context derivation over the pair");
        let context_blob = serde_json::to_string(&messages).unwrap();
        assert!(context_blob.contains("inline excerpt"));
        assert!(
            !context_blob.contains("the full verbatim channel payload"),
            "the sidecar must never enter model context"
        );
        cleanup(&path);
    }

    /// kian-91 (review B-2): the dream envelope row — exactly
    /// `{"content": X, "dream": M}` — consumes to ONE `input/admitted`
    /// carrying `role`+`content`+the marker verbatim (no sibling event),
    /// and the additive marker leaves the projected model context
    /// byte-identical to the same stream without it (constraint 3).
    #[tokio::test]
    async fn dream_envelope_consumes_to_one_admitted_with_marker() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let marker = json!({
            "source_session": "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
            "start_seq": 1,
            "end_seq": 4,
            "scope": "ops-agent",
        });
        let prompt = "[kian-dream: scope=\"ops-agent\" range=[1,4)]\n\ndream body";
        store
            .admit(row(
                sid,
                7,
                json!({"content": prompt, "dream": marker.clone()}),
            ))
            .await
            .unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 1, "exactly one input/admitted event");
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "user", "content": prompt, "dream": marker}),
            "the marker rides the admitted payload verbatim"
        );
        // Constraint-3 witness: the additive, non-context marker leaves
        // derive_model_context byte-identical (the same stream without
        // the key derives the same context).
        let mut bare = events.clone();
        for ev in &mut bare {
            if ev.kind == EventKind::InputAdmitted {
                ev.bytes.as_object_mut().unwrap().remove("dream");
            }
        }
        let with_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&events).unwrap())
                .unwrap();
        let without_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&bare).unwrap())
                .unwrap();
        assert_eq!(with_marker, without_marker);
        assert!(with_marker.contains("dream body"));
        assert!(
            !with_marker.contains("source_session"),
            "the marker must never enter model context"
        );
        cleanup(&path);
    }

    /// CRON-MODEL §6.1: the cron envelope row — exactly
    /// `{"content": X, "cron": M}` — consumes to ONE `input/admitted`
    /// carrying `role`+`content`+the firing marker verbatim, and the
    /// additive marker leaves the projected model context byte-identical
    /// to the same stream without it (constraint 3).
    #[tokio::test]
    async fn cron_envelope_consumes_to_one_admitted_with_marker() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let marker = json!({
            "schedule_id": "night-audit",
            "firing_id": "night-audit@2026-11-18T03:30:00Z",
            "fire_time": "2026-11-18T03:30:00Z",
            "target": {"channel": "cron-jobs", "topic": "audit"},
        });
        let prompt = "audit .state/";
        store
            .admit(row(
                sid,
                7,
                json!({"content": prompt, "cron": marker.clone()}),
            ))
            .await
            .unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 1, "exactly one input/admitted event");
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "user", "content": prompt, "cron": marker}),
            "the firing proof rides the admitted payload verbatim"
        );
        // Constraint-3 witness: the additive, non-context marker leaves
        // derive_model_context byte-identical (the same stream without
        // the key derives the same context).
        let mut bare = events.clone();
        for ev in &mut bare {
            if ev.kind == EventKind::InputAdmitted {
                ev.bytes.as_object_mut().unwrap().remove("cron");
            }
        }
        let with_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&events).unwrap())
                .unwrap();
        let without_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&bare).unwrap())
                .unwrap();
        assert_eq!(with_marker, without_marker);
        assert!(with_marker.contains("audit .state/"));
        assert!(
            !with_marker.contains("night-audit@"),
            "the firing marker must never enter model context"
        );
        cleanup(&path);
    }

    /// kian-99 (MEMORY-MODEL §6 I1): the memory envelope row — exactly
    /// `{"content": X, "memory": M}` — consumes to ONE `input/admitted`
    /// system message carrying `synthetic:true` + the marker verbatim
    /// (no sibling event), and the additive marker leaves the projected
    /// model context byte-identical to the same stream without it
    /// (constraint 3).
    #[tokio::test]
    async fn memory_envelope_consumes_to_one_admitted_system_message() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let marker = json!({
            "scope": "ops-agent",
            "notes": [
                "evt_00000000-0000-0000-0000-000000000001",
                "evt_00000000-0000-0000-0000-000000000002"
            ],
            "bytes": 512,
            "items": 2,
            "truncated": false,
            "source_seq": 41,
        });
        let index = "[kian-memory: scope=\"ops-agent\" items=2]\n\n- deploy: use pnpm\n- test: cargo nextest";
        store
            .admit(row(
                sid,
                7,
                json!({"content": index, "memory": marker.clone()}),
            ))
            .await
            .unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 1, "exactly one input/admitted event");
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "system", "content": index, "synthetic": true, "memory": marker}),
            "the marker rides the admitted payload verbatim"
        );
        // Constraint-3 witness: the additive, non-context marker leaves
        // derive_model_context byte-identical (the same stream without
        // the key derives the same context).
        let mut bare = events.clone();
        for ev in &mut bare {
            if ev.kind == EventKind::InputAdmitted {
                ev.bytes.as_object_mut().unwrap().remove("memory");
            }
        }
        let with_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&events).unwrap())
                .unwrap();
        let without_marker =
            serde_json::to_string(&kian_core::context::derive_model_context(&bare).unwrap())
                .unwrap();
        assert_eq!(with_marker, without_marker);
        assert!(with_marker.contains("cargo nextest"));
        assert!(
            !with_marker.contains("source_seq"),
            "the marker must never enter model context"
        );
        // The synthetic marker survives the projection, so export can
        // strip the injection (I6).
        assert!(
            with_marker.contains("\"synthetic\":true"),
            "the injected index is strippable: {with_marker}"
        );
        cleanup(&path);
    }

    /// A superset (`memory` + extra keys) is ordinary content, never an
    /// envelope (kian-99, the kian-96 review N-1 discipline): the wire
    /// guard refuses only exact key-set equality, so the consumption
    /// plan must not mint a system injection from public content.
    #[tokio::test]
    async fn memory_superset_row_is_ordinary_content() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let forged = json!({
            "content": "totally ordinary user text",
            "memory": {"scope": "ghost", "items": 1, "bytes": 10, "truncated": false, "source_seq": 1},
            "extra": true,
        });
        store.admit(row(sid, 7, forged.clone())).await.unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 1);
        // Ordinary user content: the whole payload rides as content,
        // no top-level `memory`/`synthetic` key was minted.
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "user", "content": forged}),
            "a superset must consume as ordinary content"
        );
        assert!(admitted[0].bytes.get("memory").is_none());
        assert!(admitted[0].bytes.get("synthetic").is_none());
        cleanup(&path);
    }

    /// Byte-comparison: the envelope row's `input/admitted` payload is
    /// byte-identical to what the bare content would have produced.
    #[tokio::test]
    async fn spill_admitted_bytes_identical_to_bare_content_path() {
        let (store, path) = open_store().await;
        let bare = SessionId::new();
        let envelope = SessionId::new();
        let ctx_bare = spill_ctx(&store, bare, Epoch(1)).await;
        let ctx_env = spill_ctx(&store, envelope, Epoch(1)).await;

        let content = json!({"kian": {"type": "channel_event"}, "text": "hi"});
        store.admit(row(bare, 1, content.clone())).await.unwrap();
        store
            .admit(row(
                envelope,
                2,
                json!({"content": content, "spill": spill_sidecar()}),
            ))
            .await
            .unwrap();
        ctx_bare.take_eligible_inputs().await.unwrap();
        ctx_env.take_eligible_inputs().await.unwrap();

        let bare_events = events_of(&store, bare).await;
        let bare_admitted = bare_events
            .iter()
            .find(|e| e.kind == EventKind::InputAdmitted)
            .expect("bare input/admitted");
        let events = events_of(&store, envelope).await;
        let (env_admitted, _) = pair_of(&events);
        assert_eq!(
            serde_json::to_string(&bare_admitted.bytes).unwrap(),
            serde_json::to_string(&env_admitted.bytes).unwrap(),
            "envelope admitted bytes == bare-content admitted bytes"
        );
        cleanup(&path);
    }

    /// Fencing: a stale-epoch drain attempting to consume an envelope row
    /// writes NEITHER event of the pair, and the row stays eligible for
    /// the rightful owner.
    #[tokio::test]
    async fn spill_pair_stale_epoch_writes_neither_event() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        // A newer claim raises the epoch floor to 2.
        let _rightful = spill_ctx(&store, sid, Epoch(2)).await;
        store
            .admit(row(
                sid,
                3,
                json!({"content": "x", "spill": spill_sidecar()}),
            ))
            .await
            .unwrap();

        // The zombie drain from epoch 1 tries to consume the same row.
        let zombie = DrainCtx::new(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();
        let err = zombie.take_eligible_inputs().await.unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Store(kian_store::StoreError::StaleEpoch { .. })
            ),
            "stale-epoch rejection, got {err}"
        );

        let events = events_of(&store, sid).await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.kind, EventKind::InputAdmitted | EventKind::InputSpilled)),
            "a stale-epoch attempt writes neither event of the pair"
        );
        assert_eq!(
            store.eligible_inputs(sid, T0).await.unwrap().len(),
            1,
            "the row was never marked consumed"
        );
        cleanup(&path);
    }

    /// A2: crash between enqueue and consumption. The durable envelope
    /// row outlives the crashed drain; the resumed drain consumes it to
    /// the same pair the no-crash path would have produced.
    #[tokio::test]
    async fn spill_row_replays_to_the_same_pair_after_crash_between_enqueue_and_consumption() {
        let (store, path) = open_store().await;
        let envelope = json!({"content": "inline excerpt", "spill": spill_sidecar()});

        // Baseline: no crash — one drain consumes the row to its pair.
        let baseline = SessionId::new();
        let ctx = spill_ctx(&store, baseline, Epoch(1)).await;
        store
            .admit(row(baseline, 1, envelope.clone()))
            .await
            .unwrap();
        ctx.take_eligible_inputs().await.unwrap();
        let baseline_events = events_of(&store, baseline).await;
        let (baseline_admitted, baseline_spilled) = pair_of(&baseline_events);

        // Crash leg: the row is enqueued (durable) but never consumed.
        let crashed = SessionId::new();
        let doomed = spill_ctx(&store, crashed, Epoch(1)).await;
        store.admit(row(crashed, 2, envelope)).await.unwrap();
        drop(doomed); // crash before consumption; the row survives.

        // Recovery: a fresh drain (new epoch) finds the row via the
        // inbox scan and consumes it.
        assert_eq!(store.startup_scan(T0).await.unwrap().len(), 1);
        let resumed = DrainCtx::new(
            store.clone(),
            crashed,
            Epoch(2),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();
        resumed
            .append(EventKind::DrainClaimed, json!({}))
            .await
            .unwrap();
        assert_eq!(resumed.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, crashed).await;
        let (admitted, spilled) = pair_of(&events);
        assert_eq!(
            serde_json::to_string(&admitted.bytes).unwrap(),
            serde_json::to_string(&baseline_admitted.bytes).unwrap()
        );
        assert_eq!(
            serde_json::to_string(&spilled.bytes).unwrap(),
            serde_json::to_string(&baseline_spilled.bytes).unwrap()
        );
        assert_eq!(spilled.seq, admitted.seq + 1);
        cleanup(&path);
    }

    /// Compat: the envelope is exactly two keys. Content objects that
    /// merely carry a `content` key (or extra keys alongside `spill`)
    /// remain plain payloads — byte-identical to the legacy surface.
    #[tokio::test]
    async fn content_objects_with_extra_keys_are_not_treated_as_spill_envelopes() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let payloads = [
            json!({"content": "x", "other": 1}),
            json!({"content": "x", "spill": 1, "extra": 2}),
            json!({"spill": 1}),
            json!({"synthetic": true, "content": "plan B"}),
        ];
        for (i, payload) in payloads.iter().enumerate() {
            store
                .admit(row(sid, i as u128 + 1, payload.clone()))
                .await
                .unwrap();
        }
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 4);

        let events = events_of(&store, sid).await;
        assert!(
            !events.iter().any(|e| e.kind == EventKind::InputSpilled),
            "none of these rows is an envelope"
        );
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 4);
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "user", "content": payloads[0]})
        );
        assert_eq!(
            admitted[1].bytes,
            json!({"role": "user", "content": payloads[1]})
        );
        assert_eq!(
            admitted[2].bytes,
            json!({"role": "user", "content": payloads[2]})
        );
        assert_eq!(
            admitted[3].bytes,
            json!({"role": "user", "content": "plan B", "synthetic": true}),
            "the steer wrapper is unchanged"
        );
        cleanup(&path);
    }

    /// kian-96 (kian-95 review N-1): the steer wrapper is EXACTLY two
    /// keys. A superset (`synthetic:true` + `content` + extra keys) is
    /// ordinary client content — the wire guard refuses only exact
    /// key-set equality, so consuming a superset as a steer row would
    /// let the public wire mint the strippable marker itself.
    #[tokio::test]
    async fn synthetic_superset_key_sets_stay_ordinary_content() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let ctx = spill_ctx(&store, sid, Epoch(1)).await;

        let superset = json!({"synthetic": true, "content": "forged steer marker", "extra": 1});
        store.admit(row(sid, 1, superset.clone())).await.unwrap();
        assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

        let events = events_of(&store, sid).await;
        let admitted: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .collect();
        assert_eq!(admitted.len(), 1);
        // The whole superset object rides as ordinary nested content;
        // no top-level `synthetic` marker is minted from it.
        assert_eq!(
            admitted[0].bytes,
            json!({"role": "user", "content": superset})
        );
        assert!(
            admitted[0].bytes.get("synthetic").is_none(),
            "a superset key set must not land the strippable marker: {}",
            admitted[0].bytes
        );
        cleanup(&path);
    }

    // -- invariant 4: drain has no durable identity -------------------------

    /// (4) Simulated crash (runtime dropped mid-provider-turn, no end
    /// write, lease left stale) → a fresh orchestrator, purely from
    /// replayed events + the startup inbox scan, claims a higher epoch
    /// and completes the session.
    #[tokio::test]
    async fn fresh_orchestrator_continues_purely_from_replayed_events_after_crash() {
        let (reader, path) = open_store().await;
        let sid = SessionId::new();

        // Engine A on its own runtime (in a plain thread; block_on needs no
        // Send, dodging the sqlx HRTB spawn limitation): drains input-1,
        // hangs mid-turn until the crash flag drops the whole runtime.
        let crash_now = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&crash_now);
        let path_a = path.clone();
        let zombie = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let store_a = EventStore::open(&path_a).await.expect("open A");
                let driver = Arc::new(ScriptedDriver::scripted(vec![vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::new(Notify::new())), // hangs forever
                ]]));
                let eng = Engine::with_clock(
                    store_a,
                    driver,
                    EngineConfig::default(),
                    Arc::new(ManualClock::new(T0)),
                );
                // Admit control-plane style: the drain bootstraps the
                // stream (session/created) for the pre-agreed sid.
                eng.admit(sid, json!("input-1"), Some("i1".into()))
                    .await
                    .unwrap();
                eng.wake(sid);
                while !flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }); // rt dropped here: every task on it dies mid-turn — the crash
        });
        // Wait until A is mid-provider-turn (turn/start committed).
        assert!(
            wait_for(5_000, || async {
                events_of(&reader, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await,
            "A reached the provider turn"
        );
        // A second input is admitted control-plane style (its wake dies
        // with the crash — NoLostWork must not depend on it).
        reader.admit(row(sid, 9, json!("input-2"))).await.unwrap();
        // Crash: the runtime (and every task on it) dies.
        crash_now.store(true, Ordering::Relaxed);
        zombie.join().expect("zombie thread finished");

        // Engine B on a fresh pool: NoLostWork via the inbox scan.
        let store_b = EventStore::open(&path).await.expect("open B");
        let eng_b = Engine::with_clock(
            store_b.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig::default(),
            Arc::new(ManualClock::new(T0)),
        );
        let found = eng_b.startup_rebuild().await.expect("rebuild");
        assert_eq!(found, 1, "crashed session rediscovered from the inbox");
        assert!(
            wait_for(5_000, || async {
                projection_of(&store_b, sid).await == SessionState::Completed
            })
            .await,
            "B completes the session"
        );

        let events = events_of(&store_b, sid).await;
        // Exactly one end write, honest reason, monotone seqs, replay legal.
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ));
        for w in events.windows(2) {
            assert!(w[0].seq + 1 == w[1].seq, "contiguous seqs");
        }
        // B's events carry a strictly higher epoch than A's.
        let claims: Vec<Epoch> = events
            .iter()
            .filter(|e| e.kind == EventKind::DrainClaimed)
            .map(|e| e.epoch)
            .collect();
        assert_eq!(claims.len(), 2, "A then B claimed");
        assert!(claims[1] > claims[0], "takeover epoch strictly higher");
        assert!(store_b.startup_scan(T0).await.unwrap().is_empty());
        cleanup(&path);
    }

    // -- recovery: lease-expiry sweep + takeover re-drive -----------------

    /// a drain killed mid-provider-turn with its input already
    /// consumed leaves an EMPTY inbox — the startup scan sees nothing.
    /// The recovery sweep must detect the expired lease on the
    /// `running` stream, take over with a higher epoch, and re-drive the
    /// loop to an honest `completed`. Before the takeover path existed
    /// this session sat in `running` forever while the sweep churned.
    #[tokio::test]
    async fn recovery_sweep_takes_over_killed_midturn_drain_with_empty_inbox() {
        let (reader, path) = open_store().await;
        let sid = SessionId::new();
        let clock = Arc::new(ManualClock::new(T0));

        // Engine A on its own runtime: consumes input-1, hangs
        // mid-provider-turn until the crash flag drops the runtime.
        let crash_now = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&crash_now);
        let path_a = path.clone();
        let clock_a = Arc::clone(&clock);
        let zombie = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let store_a = EventStore::open(&path_a).await.expect("open A");
                let driver = Arc::new(ScriptedDriver::scripted(vec![vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::new(Notify::new())), // hangs forever
                ]]));
                let eng = Engine::with_clock(
                    store_a,
                    driver,
                    EngineConfig {
                        holder: "doomed".into(),
                        lease_ttl_ms: 100,
                        heartbeat_interval_ms: 10,
                    },
                    clock_a,
                );
                eng.admit(sid, json!("input-1"), Some("i1".into()))
                    .await
                    .unwrap();
                eng.wake(sid);
                while !flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }); // runtime dropped: the drain dies mid-turn
        });
        assert!(
            wait_for(5_000, || async {
                events_of(&reader, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await,
            "A reached the provider turn"
        );
        // The killed drain's input is already consumed: the inbox is empty
        // and the stream projects `running` — the pure shape.
        assert!(
            reader
                .eligible_inputs(sid, clock.now_ms())
                .await
                .unwrap()
                .is_empty(),
            "input consumed before the crash (mid-turn kill)"
        );
        crash_now.store(true, Ordering::Relaxed);
        zombie.join().expect("zombie thread finished");

        // Engine B: the startup inbox scan finds NOTHING (empty inbox)…
        let store_b = EventStore::open(&path).await.expect("open B");
        let eng_b = Engine::with_clock(
            store_b.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig {
                holder: "recoverer".into(),
                lease_ttl_ms: 100,
                heartbeat_interval_ms: 10,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        assert_eq!(
            eng_b.startup_rebuild().await.unwrap(),
            0,
            "empty inbox: nothing for the startup scan — only the sweep can find it"
        );

        // …the lease is still fresh (heartbeat wrote T0 with the frozen
        // clock): a sweep now is a no-op.
        clock.advance(50);
        assert!(
            eng_b.recovery_sweep().await.unwrap().is_empty(),
            "lease not yet expired"
        );

        // Past the TTL the sweep detects the dead owner and takes over.
        clock.advance(200);
        let woken = eng_b.recovery_sweep().await.unwrap();
        assert_eq!(woken, vec![sid], "expired running session woken");
        assert!(
            wait_for(5_000, || async {
                projection_of(&store_b, sid).await == SessionState::Completed
            })
            .await,
            "takeover re-drive settles honestly"
        );

        let events = events_of(&store_b, sid).await;
        let claims: Vec<kian_core::model::Epoch> = events
            .iter()
            .filter(|e| e.kind == EventKind::DrainClaimed)
            .map(|e| e.epoch)
            .collect();
        assert_eq!(claims.len(), 2, "A's claim + B's takeover claim");
        assert!(claims[1] > claims[0], "takeover epoch strictly higher");
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
                .count(),
            1,
            "exactly one end write"
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ));
        assert!(
            store_b
                .startup_scan(clock.now_ms())
                .await
                .unwrap()
                .is_empty()
        );
        cleanup(&path);
    }

    // -- durable interrupt intent (ADR-014) -----------------------------

    /// Unit: a reserved control-envelope row consumes to a
    /// `control/interrupt` audit event — never a message row — and a
    /// current-epoch row sets the drain's interrupt flag. The final
    /// control-only take leaves message rows eligible.
    #[tokio::test]
    async fn control_row_consumes_to_an_audit_event_and_flips_the_shared_flag() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        let flag = Arc::new(AtomicBool::new(false));
        let ctx = DrainCtx::with_interrupt(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
            Arc::clone(&flag),
        )
        .await
        .unwrap();
        ctx.append(EventKind::SessionCreated, json!({}))
            .await
            .unwrap();

        let intent = kian_core::control::InterruptIntent {
            reason: Some("stop the deploy".into()),
            requested_by: Some("ops".into()),
            requested_at_ms: T0,
            target_epoch: Some(1), // this drain's own claim — current
        };
        store.admit(row(sid, 1, intent.envelope())).await.unwrap();
        store
            .admit(row(sid, 2, json!("a plain message")))
            .await
            .unwrap();

        // The final control-only take consumes just the control row.
        assert!(ctx.take_control_inputs().await.unwrap());
        assert!(flag.load(Ordering::Acquire), "current-epoch row interrupts");
        assert_eq!(
            store.eligible_inputs(sid, T0).await.unwrap().len(),
            1,
            "the message row stays eligible"
        );

        // The normal take returns ONLY message rows.
        let taken = ctx.take_eligible_inputs().await.unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].payload, json!("a plain message"));

        let events = events_of(&store, sid).await;
        let control: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::ControlInterrupt)
            .collect();
        assert_eq!(control.len(), 1, "exactly one audit event");
        assert_eq!(
            control[0].bytes,
            intent.event_payload(false),
            "the event payload reproduces the requested intent"
        );
        assert!(control[0].bytes.get("stale").is_none());
        // The control row is bound consumed (inbox empty).
        assert!(store.eligible_inputs(sid, T0).await.unwrap().is_empty());
        cleanup(&path);
    }

    /// Dedupe: repeated `interrupt` RPCs against one episode admit
    /// exactly one durable row (idempotent by
    /// `interrupt:<session>:<target_epoch>`) and the drained stream
    /// carries exactly one `control/interrupt` event.
    #[tokio::test]
    async fn repeated_interrupts_in_one_epoch_dedupe_to_one_row_and_one_event() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![vec![
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::Settled),
            ]])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );

        // Two interrupts while the drain is hung mid-turn (same epoch).
        assert_eq!(
            eng.interrupt(sid).await.unwrap(),
            InterruptOutcome::Requested
        );
        assert_eq!(
            eng.interrupt(sid).await.unwrap(),
            InterruptOutcome::Requested
        );
        let eligible = store.eligible_inputs(sid, T0).await.unwrap();
        let control_rows: Vec<_> = eligible
            .iter()
            .filter(|r| kian_core::control::InterruptIntent::is_control_envelope(&r.payload))
            .collect();
        assert_eq!(
            control_rows.len(),
            1,
            "one durable intent row per epoch, not two"
        );
        assert_eq!(
            control_rows[0].idempotency_key.as_deref(),
            Some(kian_core::control::InterruptIntent::idempotency_key(&sid, Some(1)).as_str()),
            "the idempotency key targets the draining epoch"
        );

        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                !eng.is_draining(&sid) && projection_of(&store, sid).await == SessionState::Idle
            })
            .await
        );
        let events = events_of(&store, sid).await;
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::ControlInterrupt)
                .count(),
            1,
            "exactly one control/interrupt event"
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Interrupted
            }
        ));
        cleanup(&path);
    }

    /// crash leg: the interrupt RPC on a replica whose projection is
    /// `running` (owner elsewhere/dead) records the durable intent; the
    /// row outlives the crashed owner; the takeover drain consumes it
    /// and settles `interrupted` — the audit event in the stream.
    #[tokio::test]
    async fn durable_interrupt_intent_survives_crash_and_takeover_settles_interrupted() {
        let (reader, path) = open_store().await;
        let sid = SessionId::new();

        // Engine A on its own runtime: drains input-1, hangs mid-turn
        // until the crash flag drops the whole runtime.
        let crash_now = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&crash_now);
        let path_a = path.clone();
        let zombie = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let store_a = EventStore::open(&path_a).await.expect("open A");
                let driver = Arc::new(ScriptedDriver::scripted(vec![vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::new(Notify::new())), // hangs forever
                ]]));
                let eng = Engine::with_clock(
                    store_a,
                    driver,
                    EngineConfig::default(),
                    Arc::new(ManualClock::new(T0)),
                );
                eng.admit(sid, json!("input-1"), Some("i1".into()))
                    .await
                    .unwrap();
                eng.wake(sid);
                while !flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }); // rt dropped: every task on it dies mid-turn — the crash
        });
        assert!(
            wait_for(5_000, || async {
                events_of(&reader, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );

        // Replica B holds no drain handle: the projection is `running`,
        // so the interrupt is DURABLE — a control row targeting epoch 1.
        let store_b = EventStore::open(&path).await.expect("open B");
        let eng_b = Engine::with_clock(
            store_b.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig::default(),
            Arc::new(ManualClock::new(T0)),
        );
        assert_eq!(
            eng_b.interrupt(sid).await.unwrap(),
            InterruptOutcome::Requested,
            "running-with-no-local-drain records durable intent"
        );
        let eligible = reader.eligible_inputs(sid, T0).await.unwrap();
        assert_eq!(eligible.len(), 1, "the durable intent row exists");
        let intent =
            kian_core::control::InterruptIntent::parse(&eligible[0].payload).expect("envelope");
        assert_eq!(intent.target_epoch, Some(1), "targets the dead episode");

        // Crash: the runtime (and every task on it) dies before
        // consuming the row.
        crash_now.store(true, Ordering::Relaxed);
        zombie.join().expect("zombie thread finished");
        assert!(
            !events_of(&reader, sid)
                .await
                .iter()
                .any(|e| e.kind == EventKind::ControlInterrupt),
            "A died before consuming the intent"
        );

        // B's startup scan finds the row; the takeover drain consumes it
        // and settles `interrupted`.
        let found = eng_b.startup_rebuild().await.expect("rebuild");
        assert_eq!(found, 1, "the durable intent row rediscovered");
        assert!(
            wait_for(5_000, || async {
                projection_of(&store_b, sid).await == SessionState::Idle
            })
            .await
        );

        let events = events_of(&store_b, sid).await;
        let control: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::ControlInterrupt)
            .collect();
        assert_eq!(control.len(), 1, "the takeover consumed the intent");
        assert_eq!(control[0].bytes.get("target_epoch"), Some(&json!(1)));
        assert!(control[0].bytes.get("stale").is_none());
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Interrupted
            }
        ));
        // The takeover's claim is a strictly higher epoch than the row's
        // target — the un-settled target still interrupts (ADR-014).
        let last_claim = events
            .iter()
            .filter(|e| e.kind == EventKind::DrainClaimed)
            .map(|e| e.epoch)
            .next_back()
            .unwrap();
        assert!(last_claim.0 > 1);
        cleanup(&path);
    }

    /// Stale rule (ADR-014): a control row targeting an episode that
    /// already settled (`session/ended` at the target epoch) is consumed
    /// for audit with `"stale": true` and does NOT end the new episode's
    /// session — the later work the interrupt never aimed at survives.
    #[tokio::test]
    async fn stale_epoch_control_row_is_audit_only_and_does_not_end_the_session() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![
                // episode 1 → soft-failed @1 (failed maps back to Idle,
                // so a second episode may claim; the ended write at
                // epoch 1 is what makes a later row stale)
                vec![
                    Step::BeginTurn,
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::SoftFailed {
                        reason: "boom".into(),
                    }),
                ],
                vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::clone(&gate)),
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::TurnDone),
                    Step::Outcome(LoopOutcome::Settled),
                ],
            ])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                let events = events_of(&store, sid).await;
                events.iter().any(|e| {
                    matches!(
                        e.kind,
                        EventKind::SessionEnded {
                            reason: kian_core::events::EndReason::Failed
                        }
                    )
                }) && projection_of(&store, sid).await == SessionState::Idle
            })
            .await
        );
        let events = events_of(&store, sid).await;
        let tail: Vec<String> = events
            .iter()
            .map(|e| format!("{}@{}", e.kind.as_str(), e.epoch.0))
            .collect();
        assert!(
            matches!(events.last().unwrap().kind, EventKind::SessionEnded { .. }),
            "tail = {tail:?}"
        );
        assert_eq!(
            events.last().unwrap().epoch.0,
            1,
            "episode 1 settled at epoch 1"
        );

        // Episode 2 on new input; the drain hangs mid-turn.
        eng.admit(sid, json!("input-2"), Some("i2".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart && e.epoch.0 == 2)
            })
            .await
        );

        // A late cross-replica row still targeting the settled epoch 1.
        let late = kian_core::control::InterruptIntent {
            reason: Some("too late".into()),
            requested_by: Some("replica-b".into()),
            requested_at_ms: T0,
            target_epoch: Some(1),
        };
        store.admit(row(sid, 99, late.envelope())).await.unwrap();

        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                let events = events_of(&store, sid).await;
                events.iter().any(|e| e.kind == EventKind::ControlInterrupt)
                    && matches!(
                        events.last().unwrap().kind,
                        EventKind::SessionEnded {
                            reason: kian_core::events::EndReason::Completed
                        }
                    )
            })
            .await
        );

        let events = events_of(&store, sid).await;
        let control: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::ControlInterrupt)
            .collect();
        assert_eq!(control.len(), 1);
        assert_eq!(
            control[0].bytes.get("stale"),
            Some(&json!(true)),
            "stale rows are audit-only"
        );
        assert_eq!(control[0].bytes.get("target_epoch"), Some(&json!(1)));
        // The stale row was still consumed (bound to its audit event).
        assert!(store.eligible_inputs(sid, T0).await.unwrap().is_empty());
        // And the drain released without an interrupt break.
        assert!(!eng.is_draining(&sid));
        assert_eq!(projection_of(&store, sid).await, SessionState::Completed);
        cleanup(&path);
    }

    /// The stop-before-done semantics: a current-epoch control row
    /// that becomes eligible while the loop is finishing its work is
    /// honored — the session settles `interrupted`, never `completed`
    /// (the completed-interrupt race, DESIGN §2.4). Deterministic: the
    /// row lands while the drain is gated mid-turn; the boundary take
    /// (or, if it slips past, the final take before the end write)
    /// consumes it and flips the settle.
    #[tokio::test]
    async fn control_row_racing_the_settle_flips_the_end_write_to_interrupted() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![vec![
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::Settled),
            ]])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );

        // The loop's work is done the moment the gate opens — but the
        // interrupt intent for THIS epoch is already durable and
        // eligible. The settle must honor it, not complete past it.
        let intent = kian_core::control::InterruptIntent {
            reason: None,
            requested_by: Some("racer".into()),
            requested_at_ms: T0,
            target_epoch: Some(1),
        };
        store.admit(row(sid, 77, intent.envelope())).await.unwrap();
        gate.notify_one();

        assert!(
            wait_for(5_000, || async {
                !eng.is_draining(&sid) && projection_of(&store, sid).await == SessionState::Idle
            })
            .await
        );
        let events = events_of(&store, sid).await;
        assert!(
            events.iter().any(|e| e.kind == EventKind::ControlInterrupt),
            "the racing intent was consumed"
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Interrupted
            }
        ));
        assert!(store.eligible_inputs(sid, T0).await.unwrap().is_empty());
        cleanup(&path);
    }

    /// + invariant 5: interrupt of an idle, terminal or missing
    /// session changes nothing — no events AND no durable intent row.
    #[tokio::test]
    async fn interrupt_of_idle_or_terminal_session_admits_no_control_row() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));

        let missing = SessionId::new();
        assert_eq!(
            eng.interrupt(missing).await.unwrap(),
            InterruptOutcome::UnknownSession
        );
        assert!(store.eligible_inputs(missing, T0).await.unwrap().is_empty());

        let idle = eng.create_session().await.unwrap();
        assert_eq!(
            eng.interrupt(idle).await.unwrap(),
            InterruptOutcome::NotDraining {
                state: SessionState::Idle
            }
        );
        assert!(
            store.eligible_inputs(idle, T0).await.unwrap().is_empty(),
            "idle sessions admit no durable intent"
        );

        // Terminal: run one episode to completion, then interrupt.
        eng.admit(idle, json!("work"), Some("w1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, idle).await == SessionState::Completed
            })
            .await
        );
        assert_eq!(
            eng.interrupt(idle).await.unwrap(),
            InterruptOutcome::NotDraining {
                state: SessionState::Completed
            }
        );
        assert!(
            store.eligible_inputs(idle, T0).await.unwrap().is_empty(),
            "terminal sessions admit no durable intent"
        );
        cleanup(&path);
    }

    // -- conditional claim admission at the engine (ADR-009) -----------

    /// A second engine with a *different* holder waking a session whose
    /// fresh lease is held by a live drain (mid-provider-turn, frozen
    /// clock so the lease never expires) is an honest no-op exit: the
    /// rejected claim precedes every append (nothing is written), the
    /// drain task releases promptly — an unbounded retry loop would
    /// keep the drain entry alive forever — and the live holder still
    /// completes the work afterwards.
    #[tokio::test]
    async fn claim_rejected_drain_exits_as_noop_without_appending_or_retrying() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let clock = Arc::new(ManualClock::new(T0));
        let driver = Arc::new(ScriptedDriver::scripted(vec![vec![
            Step::BeginTurn,
            Step::Gate(Arc::clone(&gate)),
            Step::EndTurn,
            Step::Outcome(LoopOutcome::TurnDone),
            Step::TakeInputs,
            Step::Outcome(LoopOutcome::Settled),
        ]]));
        let eng_a = Engine::with_clock(
            store.clone(),
            Arc::clone(&driver),
            EngineConfig {
                holder: "replica-a".into(),
                lease_ttl_ms: 30_000,
                heartbeat_interval_ms: 10_000,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let sid = eng_a.create_session().await.unwrap();
        eng_a
            .admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await,
            "A in provider turn with a fresh lease"
        );

        // Engine B — a different holder on the same DB — wakes. The
        // lease is fresh (the clock is frozen), so admission refuses.
        let eng_b = Engine::with_clock(
            store.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig {
                holder: "replica-b".into(),
                lease_ttl_ms: 30_000,
                heartbeat_interval_ms: 10_000,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let before = events_of(&store, sid).await.len();
        assert_eq!(eng_b.wake(sid), WakeOutcome::Started);
        assert!(
            wait_for(5_000, || async { !eng_b.is_draining(&sid) }).await,
            "rejected claim exits the drain — never a retry loop"
        );
        assert_eq!(
            events_of(&store, sid).await.len(),
            before,
            "the rejected drain appended nothing"
        );

        // The live holder's drain finishes the work: no lost input.
        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
            })
            .await,
            "the live holder serviced the input"
        );
        assert!(
            !eng_a.is_draining(&sid) && !eng_b.is_draining(&sid),
            "both engines released"
        );
        cleanup(&path);
    }

    // -- invariant 5: interrupt targets only the active chain ---------------
    /// (5) Interrupt of an idle or missing session is a no-op: no
    /// events, no drain, no state change.
    #[tokio::test]
    async fn interrupt_of_idle_or_missing_session_is_a_noop() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let missing = SessionId::new();
        assert_eq!(
            eng.interrupt(missing).await.unwrap(),
            InterruptOutcome::UnknownSession
        );
        let idle = eng.create_session().await.unwrap();
        assert_eq!(
            eng.interrupt(idle).await.unwrap(),
            InterruptOutcome::NotDraining {
                state: SessionState::Idle
            }
        );
        assert_eq!(store.head(idle).await.unwrap().next_seq, 2, "no new events");
        assert!(!eng.is_draining(&idle));
        cleanup(&path);
    }

    // -- ADR-035: the release of an idle-ended session ----------------------

    /// Seeds a session in the worker-release shape: claimed once, an
    /// input admitted, then `session/ended{interrupted}` — projection
    /// `idle`, resumable, no drain.
    async fn seed_interrupted_idle(store: &EventStore, eng: &Engine<ScriptedDriver>) -> SessionId {
        let sid = eng.create_session().await.unwrap();
        let lease = store
            .claim(sid, "test-holder", 5_000, T0)
            .await
            .expect("claim");
        let events = [
            (
                EventKind::DrainClaimed,
                json!({"holder": "test-holder", "epoch": lease.epoch.0}),
            ),
            (
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "task"}),
            ),
            (
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
                json!({}),
            ),
        ];
        for (i, (kind, bytes)) in events.into_iter().enumerate() {
            let seq = 2 + i as u64;
            store
                .append(
                    sid,
                    Event {
                        seq,
                        session: sid,
                        epoch: lease.epoch,
                        ts: T0,
                        kind,
                        bytes,
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch: lease.epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .expect("append seed event");
        }
        assert_eq!(projection_of(store, sid).await, SessionState::Idle);
        sid
    }

    /// (ADR-035) An idle-ended session is closed by ONE atomic batch: a
    /// fresh claim + `drain/claimed` + `session/ended{cancelled}` with
    /// the caller's release payload. The result projects `cancelled`
    /// (terminal), satisfies NoHalfTerminated and invariant 5, and a
    /// second call is the idempotent `already_terminal`.
    #[tokio::test]
    async fn release_idle_closes_an_interrupted_session_with_one_atomic_batch() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = seed_interrupted_idle(&store, &eng).await;
        let before = store.head(sid).await.unwrap();
        let payload = json!({
            "released": {
                "worker": "mac-mini",
                "thread": Uuid::new_v4().to_string(),
                "by": "operator",
                "reason": "stuck",
            }
        });

        let out = eng.release_idle(sid, payload.clone()).await.unwrap();
        let ReleaseOutcome::Released { seq, epoch } = out else {
            panic!("expected Released, got {out:?}");
        };
        assert_eq!(seq, before.next_seq + 1, "the end is the batch's last seq");
        assert!(
            epoch > before.epoch.0,
            "the close takes a fresh claim epoch"
        );

        let events = events_of(&store, sid).await;
        assert_eq!(projection_of(&store, sid).await, SessionState::Cancelled);
        assert!(
            matches!(events[events.len() - 2].kind, EventKind::DrainClaimed),
            "the close claims before it ends"
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: EndReason::Cancelled
            }
        ));
        assert_eq!(events.last().unwrap().bytes, payload);
        assert_eq!(events.last().unwrap().seq, seq);
        assert_eq!(events.last().unwrap().epoch.0, epoch);
        kian_core::invariants::no_half_terminated(&events).expect("one terminal end");
        kian_core::invariants::interrupt_targets_active_chain(&events).expect("invariant 5");

        assert_eq!(
            eng.release_idle(sid, json!({})).await.unwrap(),
            ReleaseOutcome::AlreadyTerminal,
            "idempotent: a released thread stays closed"
        );
        assert_eq!(
            events_of(&store, sid).await.len(),
            events.len(),
            "no extra write"
        );
        cleanup(&path);
    }

    /// (ADR-035) The gates refuse everything that is not an idle, ended,
    /// drain-free, work-free session — before any claim or append.
    #[tokio::test]
    async fn release_idle_refuses_non_eligible_sessions() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));

        assert_eq!(
            eng.release_idle(SessionId::new(), json!({})).await.unwrap(),
            ReleaseOutcome::Unknown
        );

        let created = eng.create_session().await.unwrap();
        assert_eq!(
            eng.release_idle(created, json!({})).await.unwrap(),
            ReleaseOutcome::NotEligible,
            "a session that never ended is not releasable"
        );

        let running = eng.create_session().await.unwrap();
        let lease = store
            .claim(running, "test-holder", 5_000, T0)
            .await
            .unwrap();
        store
            .append(
                running,
                Event {
                    seq: 2,
                    session: running,
                    epoch: lease.epoch,
                    ts: T0,
                    kind: EventKind::DrainClaimed,
                    bytes: json!({"holder": "test-holder", "epoch": lease.epoch.0}),
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: lease.epoch,
                    expected_seq: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            eng.release_idle(running, json!({})).await.unwrap(),
            ReleaseOutcome::Busy,
            "a running projection is never force-closed"
        );

        let done = eng.create_session().await.unwrap();
        let lease = store.claim(done, "test-holder", 5_000, T0).await.unwrap();
        for (seq, kind) in [
            (2, EventKind::DrainClaimed),
            (
                3,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
            ),
        ] {
            store
                .append(
                    done,
                    Event {
                        seq,
                        session: done,
                        epoch: lease.epoch,
                        ts: T0,
                        kind,
                        bytes: json!({}),
                        version: SCHEMA_VERSION,
                    },
                    AppendGuard {
                        epoch: lease.epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            eng.release_idle(done, json!({})).await.unwrap(),
            ReleaseOutcome::AlreadyTerminal
        );
        cleanup(&path);
    }

    /// (ADR-035) Eligible work is never discarded: an inbox row makes
    /// the release refuse and write nothing.
    #[tokio::test]
    async fn release_idle_never_discards_eligible_work() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = seed_interrupted_idle(&store, &eng).await;
        eng.admit_quiet(sid, json!("resume me"), None)
            .await
            .unwrap();
        let before = store.head(sid).await.unwrap();

        assert_eq!(
            eng.release_idle(sid, json!({})).await.unwrap(),
            ReleaseOutcome::NotEligible
        );
        assert_eq!(
            store.head(sid).await.unwrap().next_seq,
            before.next_seq,
            "nothing was appended"
        );
        assert_eq!(projection_of(&store, sid).await, SessionState::Idle);
        cleanup(&path);
    }

    /// (ADR-035) A live drain is never raced: the release is refused
    /// while the drain holds the session, and the drain finishes
    /// untouched afterwards.
    #[tokio::test]
    async fn release_idle_never_touches_a_live_drain() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![vec![
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::TurnDone),
                Step::Outcome(LoopOutcome::Settled),
            ]])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("work"), None).await.unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await,
            "the drain is mid-turn"
        );

        assert_eq!(
            eng.release_idle(sid, json!({})).await.unwrap(),
            ReleaseOutcome::Busy
        );
        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                !eng.is_draining(&sid)
                    && projection_of(&store, sid).await == SessionState::Completed
            })
            .await,
            "the untouched drain settles"
        );
        cleanup(&path);
    }

    // -- invariant 6: settle on abort ---------------------------------------

    /// (6) Aborting a drain with a non-empty inbox leaves pendingWake
    /// registered: the interrupt is honored at the next safe boundary,
    /// the end write is honestly `interrupted` (session resumable —
    /// Idle), the pending input stays eligible, and a later wake
    /// resumes and completes the session.
    #[tokio::test]
    async fn aborting_drain_with_nonempty_inbox_leaves_pendingwake_registered() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let resume_gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![
                vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::clone(&gate)),
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::TurnDone),
                    Step::TakeInputs,
                    Step::Outcome(LoopOutcome::Settled),
                ],
                vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::clone(&resume_gate)),
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::TurnDone),
                    Step::Outcome(LoopOutcome::Settled),
                ],
            ])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );
        // Steer arrives mid-turn → its wake folds onto pendingWake.
        eng.steer(sid, json!("input-2"), Some("i2".into()))
            .await
            .unwrap();
        assert!(eng.pending_wake_contains(&sid), "folded steer wake");
        assert_eq!(
            eng.interrupt(sid).await.unwrap(),
            InterruptOutcome::Requested
        );
        gate.notify_one(); // the turn completes → boundary → interrupt honored

        assert!(
            wait_for(5_000, || async {
                !eng.is_draining(&sid) && projection_of(&store, sid).await == SessionState::Idle
            })
            .await,
            "interrupted drain released ownership (Idle, resumable)"
        );
        let events = events_of(&store, sid).await;
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Interrupted
            }
        ));
        // settle-on-abort: pendingWake still registered, input-2 eligible.
        assert!(eng.pending_wake_contains(&sid), "pendingWake kept on abort");
        let eligible = store.eligible_inputs(sid, T0).await.unwrap();
        assert!(
            eligible
                .iter()
                .any(|r| r.payload["content"] == json!("input-2")),
            "steer input not consumed: {eligible:?}"
        );

        // Resume: the pending input drains to completion under a new claim.
        assert_eq!(eng.wake(sid), WakeOutcome::Started);
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart && e.epoch.0 > 1)
            })
            .await,
            "resume drain reached a new provider turn"
        );
        resume_gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
            })
            .await,
            "interrupted-then-resumed completes honestly"
        );
        cleanup(&path);
    }

    // -- claims: one wins, losers are fenced --------------------------------

    /// Two concurrently claiming tasks → one wins, all loser writes are
    /// rejected by the store: engine A hangs mid-turn (a zombie holding
    /// the lower epoch with an unconsumed second input); engine B
    /// claims the higher epoch, finishes the work and ends the session;
    /// A's post-hang writes are all rejected (`turn/end` count stays 1;
    /// every event after B's claim carries B's epoch).
    #[tokio::test]
    async fn concurrent_claims_one_wins_and_loser_writes_are_rejected() {
        let (reader, path) = open_store().await;
        let gate_a = Arc::new(Notify::new());
        let gate_b = Arc::new(Notify::new());
        let driver = Arc::new(ScriptedDriver::scripted(vec![
            vec![
                // A's episode: takes input-1, hangs mid-turn.
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate_a)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::TurnDone),
                Step::TakeInputs,
                Step::Outcome(LoopOutcome::Settled),
            ],
            vec![
                // B's takeover episode on input-2.
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate_b)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::TurnDone),
                Step::TakeInputs,
                Step::Outcome(LoopOutcome::Settled),
            ],
        ]));

        let store_a = EventStore::open(&path).await.unwrap();
        let store_b = EventStore::open(&path).await.unwrap();
        let eng_a = Engine::with_clock(
            store_a.clone(),
            Arc::clone(&driver),
            EngineConfig::default(),
            Arc::new(ManualClock::new(T0)),
        );
        let eng_b = Engine::with_clock(
            store_b.clone(),
            Arc::clone(&driver),
            EngineConfig::default(),
            Arc::new(ManualClock::new(T0)),
        );

        let sid = eng_a.create_session().await.unwrap();
        eng_a
            .admit(sid, json!("input-1"), Some("i1".into()))
            .await
            .unwrap(); // admit wakes: A's drain is now in flight
        assert!(
            wait_for(5_000, || async {
                events_of(&reader, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await,
            "A in provider turn"
        );
        eng_a
            .admit(sid, json!("input-2"), Some("i2".into()))
            .await
            .unwrap(); // folds onto A's pendingWake

        // B claims concurrently and takes over the pending input.
        assert_eq!(eng_b.wake(sid), WakeOutcome::Started);
        assert!(
            wait_for(5_000, || async {
                events_of(&reader, sid)
                    .await
                    .iter()
                    .filter(|e| e.kind == EventKind::DrainClaimed)
                    .count()
                    >= 2
            })
            .await,
            "B claimed (two drain/claimed events)"
        );
        gate_b.notify_one();
        assert!(
            wait_for(5_000, || async {
                projection_of(&reader, sid).await == SessionState::Completed
            })
            .await,
            "B ended the session"
        );

        // Zombie A wakes: every write it attempts is fenced out.
        gate_a.notify_one();
        assert!(
            wait_for(5_000, || async { !eng_a.is_draining(&sid) }).await,
            "A abandoned after stale-epoch rejection; driver log: {:?}",
            driver.log()
        );
        assert!(wait_for(5_000, || async { !eng_b.is_draining(&sid) }).await);

        let events = events_of(&reader, sid).await;
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
                .count(),
            1,
            "exactly one end write"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::TurnEnd)
                .count(),
            1,
            "A's mid-turn turn/end was rejected by the store"
        );
        let claims: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::DrainClaimed)
            .collect();
        assert_eq!(claims.len(), 2);
        assert!(claims[0].epoch < claims[1].epoch, "consecutive epochs");
        let winner_epoch = claims[1].epoch;
        for ev in events.iter().filter(|e| e.seq > claims[1].seq) {
            assert_eq!(
                ev.epoch, winner_epoch,
                "loser write landed after the takeover claim"
            );
        }
        assert!(reader.startup_scan(T0).await.unwrap().is_empty());
        cleanup(&path);
    }

    // -- lease liveness -----------------------------------------------------

    /// Heartbeat extends the lease: while a drain is long in flight
    /// (with heartbeats and an advancing clock), the session never
    /// surfaces in `expired_leases`; an un-heartbeated claimed session
    /// past its TTL does.
    #[tokio::test]
    async fn heartbeat_extends_the_lease_during_a_long_drain() {
        let (store, path) = open_store().await;
        let clock = Arc::new(ManualClock::new(T0));
        let gate = Arc::new(Notify::new());
        let eng = Engine::with_clock(
            store.clone(),
            Arc::new(ScriptedDriver::scripted(vec![vec![
                Step::BeginTurn,
                Step::Gate(Arc::clone(&gate)),
                Step::EndTurn,
                Step::Outcome(LoopOutcome::TurnDone),
                Step::TakeInputs,
                Step::Outcome(LoopOutcome::Settled),
            ]])),
            EngineConfig {
                holder: "hb-test".into(),
                lease_ttl_ms: 60,
                heartbeat_interval_ms: 10,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );

        // Control: a claimed, running session that never heartbeats.
        let control = eng.create_session().await.unwrap();
        let lease = store
            .claim(control, "zombie", 60, clock.now_ms())
            .await
            .unwrap();
        store
            .append(
                control,
                Event {
                    seq: 2,
                    session: control,
                    epoch: lease.epoch,
                    ts: clock.now_ms(),
                    kind: EventKind::DrainClaimed,
                    bytes: json!({}),
                    version: SCHEMA_VERSION,
                },
                kian_store::AppendGuard {
                    epoch: lease.epoch,
                    expected_seq: 2,
                },
            )
            .await
            .unwrap();

        // Drained session with the heartbeat task alive.
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("long task"), None).await.unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );

        // Advance the clock while heartbeats tick: the lease extends.
        let adv_stop = Arc::new(AtomicBool::new(false));
        let advancer = {
            let clock = Arc::clone(&clock);
            let stop = Arc::clone(&adv_stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    clock.advance(5);
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Freeze the clock before asserting: on a starved runner the
        // heartbeat task can lag the advancer (CI runs many tests in
        // parallel), and the fixed sleep below is not a guarantee. With the
        // clock frozen, wait until a heartbeat has fired *after* the freeze
        // — then expiry is evaluated against a now the heartbeat has
        // already covered, making the assertion starvation-proof. The
        // un-heartbeated control stays expired regardless (its last beat is
        // at claim time, long before the frozen now).
        adv_stop.store(true, Ordering::Relaxed);
        advancer.await.unwrap();
        let frozen_now = clock.now_ms();
        assert!(
            wait_for(5_000, || async {
                store
                    .expired_leases(frozen_now)
                    .await
                    .unwrap()
                    .iter()
                    .any(|l| l.session == control)
            })
            .await
        );

        let now = clock.now_ms();
        let expired = store.expired_leases(now).await.unwrap();
        assert!(
            !expired.iter().any(|l| l.session == sid),
            "heartbeat kept the live drain's lease fresh: {expired:?}"
        );
        assert!(
            expired.iter().any(|l| l.session == control),
            "un-heartbeated control session expired"
        );

        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
            })
            .await
        );
        cleanup(&path);
    }

    // -- wake folding / concurrency ----------------------------------------

    /// A wake for an in-flight drain folds; two racing wakes yield
    /// exactly one `Started`; the folded wake re-triggers the drain
    /// (which then drains the second input in the same ownership).
    #[tokio::test]
    async fn wakes_fold_onto_an_inflight_drain_and_retrigger() {
        let (store, path) = open_store().await;
        let gate = Arc::new(Notify::new());
        let eng = engine(
            &store,
            Arc::new(ScriptedDriver::scripted(vec![
                vec![
                    Step::BeginTurn,
                    Step::Gate(Arc::clone(&gate)),
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::TurnDone),
                    Step::TakeInputs,
                    Step::Outcome(LoopOutcome::Settled),
                ],
                vec![
                    Step::BeginTurn,
                    Step::EndTurn,
                    Step::Outcome(LoopOutcome::TurnDone),
                    Step::Outcome(LoopOutcome::Settled),
                ],
            ])),
        );
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("input-1"), None).await.unwrap();
        assert!(
            wait_for(5_000, || async {
                events_of(&store, sid)
                    .await
                    .iter()
                    .any(|e| e.kind == EventKind::TurnStart)
            })
            .await
        );

        // Fold: the drain is in flight (admit already woke it).
        let folded = eng.admit(sid, json!("input-2"), None).await.unwrap();
        assert!(!folded.deduplicated);
        assert!(eng.pending_wake_contains(&sid));

        // The folded wake retriggers: the drain consumes input-2 too.
        gate.notify_one();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
                    && !eng.is_draining(&sid)
            })
            .await,
            "both inputs drained under folded wake"
        );
        let inputs = events_of(&store, sid)
            .await
            .into_iter()
            .filter(|e| e.kind == EventKind::InputAdmitted)
            .count();
        assert_eq!(inputs, 2);
        assert!(!eng.is_draining(&sid));
        assert_eq!(eng.pending_wake_depth(), 0);

        // Racing wakes on an idle session: exactly one Started (the
        // concurrency-safe fold; the entry lock serializes the racers).
        let other = SessionId::new();
        let (a, b) = (Arc::clone(&eng), Arc::clone(&eng));
        let (r1, r2) = tokio::join!(async move { a.wake(other) }, async move { b.wake(other) });
        assert_ne!(r1, r2, "exactly one Started, one Folded: {r1:?} {r2:?}");
        assert!(
            wait_for(5_000, || async { !eng.is_draining(&other) }).await,
            "no-work drain released cleanly"
        );
        cleanup(&path);
    }

    // -- metrics ------------------------------------------------------------

    /// The counters move: state transitions, drain depth/duration,
    /// end writes, wake traffic.
    #[tokio::test]
    async fn metrics_counters_track_transitions_depth_duration_and_wakes() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = eng.create_session().await.unwrap();
        let before = eng.metrics().snapshot();
        assert_eq!(before.state_transitions, 1, "create_session counted");

        eng.admit(sid, json!("task"), None).await.unwrap();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
                    && !eng.is_draining(&sid)
            })
            .await,
            "completed and drain task fully released"
        );
        let after = eng.metrics().snapshot();
        assert_eq!(
            after.state_transitions - before.state_transitions,
            2,
            "claimed + ended"
        );
        assert_eq!(after.drains_started - before.drains_started, 1);
        assert_eq!(after.drain_depth, 0, "depth gauge back to zero");
        assert_eq!(after.end_writes - before.end_writes, 1);
        assert!(after.drain_duration_ms_total >= before.drain_duration_ms_total);
        assert_eq!(eng.pending_wake_depth(), 0);
        cleanup(&path);
    }

    /// Waking a terminal session releases without any event: the
    /// terminal projection freezes the stream and the spurious pending
    /// wake is dropped.
    #[tokio::test]
    async fn waking_a_terminal_session_releases_without_end_write() {
        let (store, path) = open_store().await;
        let eng = engine(&store, Arc::new(ScriptedDriver::default_happy()));
        let sid = eng.create_session().await.unwrap();
        eng.admit(sid, json!("task"), None).await.unwrap();
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
                    && !eng.is_draining(&sid)
            })
            .await,
            "first drain completed and released"
        );
        let head = store.head(sid).await.unwrap();

        eng.wakes.register(&sid);
        assert_eq!(eng.wake(sid), WakeOutcome::Started);
        assert!(
            wait_for(5_000, || async { !eng.is_draining(&sid) }).await,
            "terminal drain released"
        );
        assert_eq!(store.head(sid).await.unwrap(), head, "no events appended");
        assert_eq!(eng.pending_wake_depth(), 0, "spurious wake dropped");
        cleanup(&path);
    }

    // -- wake transport (postgres, env-gated) --------------------------

    /// True when the env-gated postgres tests run (the pattern:
    /// `KIAN_PG_URL` absent → SKIP with a log line, never fail).
    fn pg_gated() -> bool {
        match std::env::var("KIAN_PG_URL") {
            Ok(url) if !url.is_empty() => true,
            _ => {
                eprintln!(
                    "SKIP: KIAN_PG_URL is not set — postgres wake-transport tests disabled (env gate)"
                );
                false
            }
        }
    }

    /// `postgres://…/db?opts` → `postgres://…/<other-db>?opts`.
    fn pg_with_db(base: &str, db: &str) -> String {
        let (path_part, query) = match base.split_once('?') {
            Some((p, q)) => (p, format!("?{q}")),
            None => (base, String::new()),
        };
        let scheme_end = path_part.find("://").map(|i| i + 3).unwrap_or(0);
        let rest = &path_part[scheme_end..];
        let root = match rest.find('/') {
            Some(i) => path_part[..scheme_end + i].to_string(),
            None => path_part.to_string(),
        };
        format!("{root}/{db}{query}")
    }

    /// One throwaway postgres database per gated test, opened as an
    /// [`EventStore`] and dropped on finish.
    async fn pg_test_store() -> Option<(EventStore, sqlx::postgres::PgPool, String)> {
        if !pg_gated() {
            return None;
        }
        let base = std::env::var("KIAN_PG_URL").unwrap();
        let db = format!("kian_orch_c52_{}", Uuid::new_v4().simple());
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&base)
            .await
            .expect("connect KIAN_PG_URL");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .expect("create throwaway database");
        let url = pg_with_db(&base, &db);
        let store = EventStore::open_postgres(&url)
            .await
            .expect("open pg store");
        Some((store, admin, url))
    }

    /// Drops the throwaway database behind [`pg_test_store`].
    async fn pg_finish(admin: &sqlx::postgres::PgPool, url: &str) {
        let db = url.rsplit('/').next().unwrap_or("");
        let db = db.split('?').next().unwrap_or("");
        let _ = sqlx::query(&format!("DROP DATABASE {db}"))
            .execute(admin)
            .await;
    }

    /// Seeds one event directly through the store (the dead replica's
    /// own writes, replayed for the test).
    async fn pg_seed_event(store: &EventStore, sid: SessionId, seq: u64, epoch: Epoch) {
        let event = Event {
            seq,
            session: sid,
            epoch,
            ts: T0 + seq,
            kind: if seq == 1 {
                EventKind::SessionCreated
            } else {
                EventKind::DrainClaimed
            },
            bytes: json!({}),
            version: SCHEMA_VERSION,
        };
        store
            .append(
                sid,
                event,
                AppendGuard {
                    epoch,
                    expected_seq: seq,
                },
            )
            .await
            .expect("seed append");
    }

    /// A notify the listener never saw (a simulated listener gap —
    /// reconnect window, or the recipient replica was down) is still
    /// serviced within one sweep period: the recovery sweep wakes the
    /// expired-lease session, the conditional claim admits the
    /// takeover, and the drain completes the work. Correctness never
    /// depends on the transport (invariant 6; ADR-009 decision 4) —
    /// this is the named listener-gap recovery test, on postgres, with
    /// a manual clock (no sleep-races).
    #[tokio::test]
    async fn pg_dropped_notify_is_serviced_within_one_sweep_period() {
        let Some((store, admin, url)) = pg_test_store().await else {
            return;
        };
        let ttl = 1_000u64;
        let clock = Arc::new(ManualClock::new(T0));
        let eng = Engine::with_clock(
            store.clone(),
            Arc::new(ScriptedDriver::default_happy()),
            EngineConfig {
                holder: "replica-b".into(),
                lease_ttl_ms: ttl,
                heartbeat_interval_ms: 25,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );

        // Replica A's world, seeded directly through the store: a
        // running session (created + claimed), a durable input its
        // admit parked, and a claim whose lease will expire. A's notify
        // to replica B was dropped — B never listens in this test.
        let sid = SessionId::new();
        pg_seed_event(&store, sid, 1, Epoch(1)).await;
        pg_seed_event(&store, sid, 2, Epoch(1)).await;
        store
            .admit(row(sid, 9001, json!("task-A")))
            .await
            .expect("replica A's durable admit");
        store
            .claim(sid, "replica-a", ttl, T0)
            .await
            .expect("replica A's claim");

        // A dies silently. One sweep period later (clock-past-TTL — the
        // exact ManualClock lease control the design asks ) B sweeps.
        clock.advance(ttl + 1);
        let woken = eng.recovery_sweep().await.expect("sweep");
        assert_eq!(
            woken,
            vec![sid],
            "the sweep surfaces the dead owner's session"
        );

        // Within that same sweep the takeover drain finishes the work.
        assert!(
            wait_for(5_000, || async {
                projection_of(&store, sid).await == SessionState::Completed
                    && !eng.is_draining(&sid)
            })
            .await,
            "the dropped notify's work must complete via the sweep takeover"
        );
        let events = events_of(&store, sid).await;
        let ended: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
            .collect();
        assert_eq!(ended.len(), 1, "exactly one end write: {:?}", events);
        assert!(matches!(
            ended[0].kind,
            EventKind::SessionEnded {
                reason: EndReason::Completed
            }
        ));
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::InputAdmitted)
                .count(),
            1,
            "the parked input was consumed (NoLostWork): {:?}",
            events
        );
        let epochs: Vec<u64> = events.iter().map(|e| e.epoch.0).collect();
        assert!(
            epochs.windows(2).all(|w| w[0] <= w[1]),
            "epochs monotone across the takeover: {epochs:?}"
        );

        store.close().await;
        pg_finish(&admin, &url).await;
        admin.close().await;
    }

    // -- READ-MODEL P0: the DrainCtx committed-event cache ---------------

    /// Builds a raw event and appends it straight to the store (the
    /// "foreign writer" path — same epoch, lawful CAS).
    async fn raw_append(store: &EventStore, sid: SessionId, seq: u64, kind: EventKind) {
        let event = Event {
            seq,
            session: sid,
            epoch: Epoch(1),
            ts: T0 + seq,
            kind,
            bytes: json!({ "n": seq }),
            version: SCHEMA_VERSION,
        };
        store
            .append(
                sid,
                event,
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: seq,
                },
            )
            .await
            .expect("raw append");
    }

    /// The cache is transparent: across self-appends, foreign appends
    /// and repeated reads, `ctx.events()` equals a fresh full rebuild
    /// byte-for-byte (the cache can never serve a skew).
    #[tokio::test]
    async fn drain_ctx_events_cache_equals_rebuild_across_self_and_foreign_appends() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        raw_append(&store, sid, 1, EventKind::SessionCreated).await;

        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();

        // First read: full (the cache starts empty) — and equals rebuild.
        assert_eq!(
            ctx.events().await.unwrap(),
            store.rebuild(sid).await.unwrap()
        );

        // Self-appends land; the tail refresh picks them up.
        ctx.append(EventKind::DrainClaimed, json!({}))
            .await
            .unwrap();
        ctx.begin_turn().await.unwrap();
        assert_eq!(
            ctx.events().await.unwrap(),
            store.rebuild(sid).await.unwrap()
        );

        // A foreign append (same epoch, straight to the store) is seen
        // by the next tail scan — the cache tracks the stream, not just
        // this ctx's own writes.
        raw_append(&store, sid, 4, EventKind::AssistantMessage).await;
        assert_eq!(
            ctx.events().await.unwrap(),
            store.rebuild(sid).await.unwrap()
        );

        // Repeated reads with nothing new are stable.
        assert_eq!(
            ctx.events().await.unwrap(),
            store.rebuild(sid).await.unwrap()
        );

        store.close().await;
        cleanup(&path);
    }

    /// A `compact` event in the tail forces exactly one full
    /// re-derivation (R5's single invalidation point): the cache is
    /// rebuilt from the stream, still byte-identical to `rebuild`, and
    /// reads after it return to tail-only work.
    #[tokio::test]
    async fn compact_in_the_tail_forces_one_full_rederivation() {
        let (store, path) = open_store().await;
        let sid = SessionId::new();
        raw_append(&store, sid, 1, EventKind::SessionCreated).await;
        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .unwrap();
        ctx.append(EventKind::DrainClaimed, json!({}))
            .await
            .unwrap();
        // Warm the cache.
        assert_eq!(ctx.events().await.unwrap().len(), 2);

        // The compaction lands (as recovery would append it).
        raw_append(&store, sid, 3, EventKind::Compact).await;
        let events = ctx.events().await.unwrap();
        assert_eq!(events, store.rebuild(sid).await.unwrap());
        assert_eq!(events.len(), 3);
        assert!(matches!(events[2].kind, EventKind::Compact));

        // Post-compact reads are ordinary tail reads again.
        raw_append(&store, sid, 4, EventKind::AssistantMessage).await;
        assert_eq!(
            ctx.events().await.unwrap(),
            store.rebuild(sid).await.unwrap()
        );

        store.close().await;
        cleanup(&path);
    }
}
