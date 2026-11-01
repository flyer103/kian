//! Admit → Wake → Drain → Promote (DESIGN §2.3, card C9).
//!
//! Admit before wake (constraint 6): the input row is durable before any
//! wake fires. Wakes are advisory: foldable, losable, fully rebuilt from
//! the C6 startup inbox scan (NoLostWork). Claim issues a monotonic
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
//! Input-consumption convention (C6 consumption is event-driven): the
//! drain is the only writer of `input/admitted` *events*. It appends one
//! — carrying the message payload contract from DESIGN §3.2
//! (`{"role","content","synthetic"?}`) — at a safe boundary when it
//! takes the row, then binds the row to that event's seq via
//! `mark_consumed`. Payload convention: a plain input row's payload IS
//! the message content; a steer row wraps it as
//! `{"synthetic": true, "content": <content>}` so the synthetic
//! (strippable) marker survives the inbox round-trip.

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
/// deterministically; the store itself never owns a clock (C6).
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
}

impl DrainCtx {
    /// Builds a context for `session` under fencing `epoch`, with the
    /// seq counter initialized from the store head.
    pub async fn new(
        store: EventStore,
        session: SessionId,
        epoch: Epoch,
        clock: Arc<dyn Clock>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, EngineError> {
        let head = store.head(session).await?;
        Ok(Self {
            core: Arc::new(DrainCtxCore {
                store,
                session,
                epoch,
                seq: AtomicU64::new(head.next_seq),
                in_turn: AtomicBool::new(false),
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

    /// [`DrainCtx::begin_turn`] with an audit payload (e.g. the loop's
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
    pub async fn events(&self) -> Result<Vec<Event>, EngineError> {
        Ok(self.core.store.rebuild(self.core.session).await?)
    }

    /// Takes every eligible input at this safe boundary: appends one
    /// `input/admitted` event per row (the consumption event, DESIGN
    /// §3.2 message payload) and binds the row to it via
    /// `mark_consumed`. Refused while a provider turn is in flight
    /// (invariant 3) — steer inputs land only at turn boundaries.
    pub async fn take_eligible_inputs(&self) -> Result<Vec<InputRow>, EngineError> {
        if self.core.in_turn.load(Ordering::Acquire) {
            return Err(EngineError::NotAtSafeBoundary {
                session: self.core.session,
            });
        }
        let now = self.core.clock.now_ms();
        let rows = self
            .core
            .store
            .eligible_inputs(self.core.session, now)
            .await?;
        let mut taken = Vec::new();
        for r in rows {
            let payload = message_payload(&r);
            let ev = self
                .core
                .append_event(EventKind::InputAdmitted, payload)
                .await?;
            match self
                .core
                .store
                .mark_consumed(self.core.session, r.id, ev.seq)
                .await
            {
                Ok(()) => taken.push(r),
                // A concurrent drain bound this row first: our event is a
                // duplicate take that the epoch fence will shortly fence
                // out entirely; skip the row, don't fail the drain.
                Err(kian_store::InboxError::AlreadyConsumed { .. }) => {
                    tracing::debug!(
                        session = %self.core.session.0,
                        input = %r.id,
                        "input already consumed by a concurrent drain"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(taken)
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

    /// Projects the session from its event stream (constraint 1: state
    /// is only ever read here, never cached).
    pub async fn projection(&self, session: SessionId) -> Result<SessionState, EngineError> {
        let events = self.store.rebuild(session).await?;
        if events.is_empty() {
            return Err(kian_core::state::ProjectionError::EmptyStream.into());
        }
        Ok(project(&events)?)
    }

    /// Admits one input: **durable row first**, then wake (constraint
    /// 6). On any admit failure the wake registry is untouched.
    pub async fn admit(
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
        // Constraint 6: the row is durable BEFORE the wake fires. On
        // any store failure we return without touching the registry.
        let outcome = self.store.admit(input).await?;
        self.wake(session);
        Ok(outcome)
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
    pub async fn interrupt(&self, session: SessionId) -> Result<InterruptOutcome, EngineError> {
        if let Some(handle) = self.drains.get(&session) {
            handle.interrupt();
            return Ok(InterruptOutcome::Requested);
        }
        let events = self.store.rebuild(session).await?;
        if events.is_empty() {
            return Ok(InterruptOutcome::UnknownSession);
        }
        Ok(InterruptOutcome::NotDraining {
            state: project(&events)?,
        })
    }

    /// Startup NoLostWork rebuild: registers a pending wake for every
    /// session the C6 inbox scan reports with eligible-but-unconsumed
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

    /// Lease-expiry recovery sweep (S1 recovery detection, DESIGN §2.4
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
    /// left registered (settle-on-abort, invariant 6).
    async fn drain_task(self: Arc<Self>, session: SessionId, interrupt: Arc<AtomicBool>) {
        let started_at = self.clock.now_ms();
        self.metrics.on_drain_started();
        let mut terminal_seen = false;
        loop {
            match self.drain_once(&session, &interrupt).await {
                Ok(DrainOutcome::Terminal) => terminal_seen = true,
                Ok(_) => {}
                Err(e) => {
                    // A fenced-out (stale-epoch) or failed attempt: the
                    // stream is left to the epoch winner — never guessed.
                    tracing::warn!(session = %session.0, error = %e, "drain attempt ended");
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
        interrupt: &AtomicBool,
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
        interrupt: &AtomicBool,
    ) -> Result<DrainOutcome, EngineError> {
        let ctx = DrainCtx::new(
            self.store.clone(),
            *session,
            lease.epoch,
            Arc::clone(&self.clock),
            Arc::clone(&self.metrics),
        )
        .await?;
        let mut claimed = false;
        // S1 takeover (DESIGN §2.4 R4/R5, C15): a stream that already
        // projects `running` when we claim belongs to a drain that died
        // mid-episode (process death; its lease expired or it lost
        // liveness). The takeover claim re-owns the session and re-drives
        // the loop to an honest settle *even when the inbox is empty* —
        // the killed episode's inputs were already consumed, so only a
        // re-drive can finish their work. Without this, a killed-mid-turn
        // session would sit in `running` forever (the startup inbox scan
        // sees nothing for it) and the expired-lease sweep would churn.
        let mut takeover = false;
        let end_reason: EndReason;
        'episodes: loop {
            if interrupt.load(Ordering::Acquire) && claimed {
                end_reason = EndReason::Interrupted;
                break 'episodes;
            }
            let events = self.store.rebuild(*session).await?;
            if events.is_empty() {
                if !ctx.has_eligible_inputs().await? {
                    return Ok(DrainOutcome::ReleasedClean);
                }
                ctx.append(EventKind::SessionCreated, json!({})).await?;
            } else if project(&events)?.is_terminal() {
                return Ok(DrainOutcome::Terminal);
            }
            if !claimed {
                takeover = !events.is_empty() && project(&events)? == SessionState::Running;
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
                    // Honest failure (constraint 7); C10's policy table
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

/// The engine's input-payload convention: a plain row's payload IS the
/// message content; a steer row wraps it as
/// `{"synthetic": true, "content": X}` so the strippable marker
/// survives the inbox round-trip (DESIGN §3.2 payload contract).
fn message_payload(row: &InputRow) -> Value {
    if let Some(obj) = row.payload.as_object()
        && obj.get("synthetic").and_then(Value::as_bool) == Some(true)
        && let Some(content) = obj.get("content")
    {
        return json!({"role": "user", "content": content, "synthetic": true});
    }
    json!({"role": "user", "content": row.payload})
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
        WakeOutcome,
    };
    use crate::metrics::Metrics;
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, InputRow, SessionId};
    use kian_core::seams::{LoopDriver, LoopOutcome};
    use kian_core::state::{SessionState, project};
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
        assert!(!eng.is_draining(&a) && !eng.is_draining(&b));
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

    // -- S1 recovery: lease-expiry sweep + takeover re-drive -----------------

    /// S1 (C15): a drain killed mid-provider-turn with its input already
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
        // and the stream projects `running` — the pure S1 shape.
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

        adv_stop.store(true, Ordering::Relaxed);
        advancer.await.unwrap();
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

    /// The C9 counters move: state transitions, drain depth/duration,
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
}
