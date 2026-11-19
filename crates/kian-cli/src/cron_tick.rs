//! The cron tick loop (CRON-MODEL §5.2/§5.3) — the *decision* half of the
//! scheduler, kept pure and store-agnostic.
//!
//! [`tick_once`] takes the clock as an argument, the declared schedules as a
//! slice, and a narrow [`CronTickStore`] for the four durable primitives the
//! tick needs (load the cursor, claim a slot, advance the cursor, settle a
//! firing). It performs no I/O of its own, so the whole §5 decision table
//! (on-time / catch-up-latest / policy-skip / outside-coalesce / not-due) is
//! unit-testable against an in-memory store — no sqlite, no postgres, no
//! tokio runtime.
//!
//! The rules this module encodes, and where they come from:
//!
//! - **the calendar math is not here**: `kian_cron::plan_fire` decides
//!   due-ness and the exact `fire_time` to record (§5.2). A `fire_time` is
//!   always a *computed* instant, never `now`, so a record replays
//!   identically;
//! - **admit, then advance** (§4.3): the firing record is claimed *before*
//!   the cursor moves. A crash between the two leaves the cursor behind a
//!   recorded slot, whose replay is the `false` from [`CronTickStore::claim_firing`]
//!   — "already fired" — not a second firing;
//! - **never admit twice** for one `(schedule_id, fire_time)`: the claim is
//!   the single-writer gate; a lost race still advances the cursor but
//!   admits nothing;
//! - **a skip is a record, not silence** (§9.1): every dropped slot is
//!   claimed with `state: Skipped` and its stable machine token
//!   (`superseded` / `outside_coalesce` / `policy_skip`).

use std::cell::RefCell;
use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};

use crate::config::CronRuntimeSchedule;

/// The durable cursor of one schedule, as the tick loop reads it.
///
/// Mirrors the two cursor columns of the `cron_schedules` row (§4.1). Both
/// are `None` until the schedule's first firing: a never-fired schedule has
/// no next fire recorded yet, and [`tick_once`] seeds it from
/// [`CronRuntimeSchedule::next_fire_after`] (nothing is due at declaration
/// time).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cursor {
    /// When the schedule is next due.
    pub next_fire_at: Option<DateTime<Utc>>,
    /// When it last fired, if it ever has.
    pub last_fire_at: Option<DateTime<Utc>>,
}

/// The state a firing record is claimed in.
///
/// A local mirror of `kian_store::FiringState` on purpose: the tick loop
/// must not depend on the storage crate's type surface (that dependency is
/// the *adapter*'s job, see [`CronTickStore`] implementors downstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiringState {
    /// Claimed, not yet started.
    Queued,
    /// A turn is running for this slot.
    Running,
    /// The turn finished cleanly.
    Succeeded,
    /// The turn failed (dispatched error, timeout, panic).
    Failed,
    /// The slot was recorded as dropped, with a skip reason.
    Skipped,
}

/// One firing slot handed to [`CronTickStore::claim_firing`].
///
/// `firing_id` is the store's slot key, `"{schedule_id}@{fire_time}"` — see
/// `kian_cron::firing_id`, which is the only producer of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiringClaim {
    /// The slot key, `{schedule_id}@{fire_time}`.
    pub firing_id: String,
    /// The owning schedule.
    pub schedule_id: String,
    /// The *computed* instant this slot was due at (never `now`).
    pub fire_time: DateTime<Utc>,
    /// When this tick claimed it.
    pub admitted_at: DateTime<Utc>,
    /// The state to claim it in.
    pub state: FiringState,
    /// Why it was skipped — `Some` iff `state == Skipped` (§9.1 reason).
    pub skip_reason: Option<&'static str>,
}

/// The four durable primitives a tick needs — everything else in the store
/// is someone else's problem.
///
/// Every method is fallible and idempotent: `claim_firing` reports "someone
/// else already recorded this slot" as `Ok(false)`, `advance_cursor` reports
/// "the cursor is already at/after this slot" as `Ok(false)`, and
/// `finish_firing` reports "already settled" as `Ok(false)`. A tick treats
/// all three as normal outcomes, never as errors.
pub trait CronTickStore {
    /// The schedule's durable cursor; `Cursor::default()` when the schedule
    /// has no row yet.
    fn load_cursor(&self, schedule_id: &str) -> Result<Cursor, String>;

    /// Claim one slot. `Ok(true)` = this caller won and owns the slot;
    /// `Ok(false)` = the slot was already recorded (a replay or a lost race).
    fn claim_firing(&self, claim: &FiringClaim) -> Result<bool, String>;

    /// Move the schedule's cursor to `admitted_fire_at` — **after** that
    /// slot was recorded — and set the next computed fire time with it.
    fn advance_cursor(
        &self,
        schedule_id: &str,
        admitted_fire_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    /// Settle a claimed firing (`Succeeded` / `Failed`, or a `Running`
    /// progress write). `finished_at` is `None` only for non-terminal
    /// states. `worktree_path` and `skip_reason` are additive: absent
    /// keeps whatever the claim recorded (the skip reason is part of the
    /// firing's public record — CRON-MODEL §9.1).
    fn finish_firing(
        &self,
        firing_id: &str,
        state: FiringState,
        finished_at: Option<DateTime<Utc>>,
        worktree_path: Option<&str>,
        skip_reason: Option<&str>,
        error: Option<&str>,
    ) -> Result<bool, String>;

    /// Settle resident-arm firings whose target session reached an end
    /// (CRON-MODEL §9.1). The resident arm accepts asynchronously, so the
    /// ticker reads the target session's stream and records the terminal
    /// state. Called once per schedule per tick; implementations throttle
    /// and the default is a no-op.
    fn settle_from_target(&self, _schedule: &CronRuntimeSchedule) -> Result<(), String> {
        Ok(())
    }
}

/// A terminal outcome the dispatcher already knows and the ticker must
/// persist on the firing row (CRON-MODEL §9.1). The worktree arm runs the
/// turn synchronously, so it knows the outcome; the resident arm accepts
/// asynchronously and settles through the target session's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutcome {
    /// The terminal state to record.
    pub state: FiringState,
    /// The worktree the turn ran in, when one was created.
    pub worktree_path: Option<String>,
    /// The machine skip reason, when `state == Skipped`.
    pub skip_reason: Option<String>,
    /// The bounded failure detail.
    pub error: Option<String>,
}

/// What a dispatch is asked to do with an admitted firing.
///
/// **Contract only — this increment deliberately ships no implementation.**
/// [`tick_and_dispatch`] calls [`CronDispatcher::dispatch`] for every
/// admitted slot and turns a returned `Err` into a `Failed` report entry
/// plus a `Failed` settle of the firing record; a returned `Ok` means the
/// firing has been *accepted for execution* (not that the turn finished).
///
/// Two implementations are expected, in this order:
///
/// 1. **resident-engine wake** — *this increment's default*: take the
///    payload, write it into the running super-agent runtime's inbox /
///    wake path so the already-resident engine runs the cron turn on the
///    schedule's target channel. Cheap (no process, no worktree), and it is
///    what the cron ticker in `server.rs` is wired for.
/// 2. **worktree-backed `dsh` execution** — *next increment (B4)*: create
///    (or reuse) a worktree for the slot, spawn `dsh` there with the
///    payload as its opening prompt, and record the worktree path on the
///    firing (`finish_firing`'s `worktree_path`). This is what buys cron
///    turns their isolation from the resident session; it is *not* wired
///    here, so `CronDispatcher` stays impl-free and the tick loop is
///    unaware of which one it is talking to.
///
/// The resident arm must be cheap (it only writes an inbox row); the
/// worktree arm runs its child **synchronously** in v1 — the tick loop
/// calls implementations inline, one admitted slot at a time, so a long
/// worktree turn delays the other schedules' ticks (CRON-MODEL §5.4/§7.3
///; a non-blocking execution pool is a P2 item).
pub trait CronDispatcher {
    /// Hand one admitted firing to the executor. `payload` is the JSON
    /// rendering of [`Admitted::payload`] — `{schedule_id, firing_id,
    /// fire_time, prompt, target}` — so both future impls read the same
    /// shape.
    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
    ) -> Result<(), String>;

    /// The terminal outcome the last [`CronDispatcher::dispatch`] already
    /// produced for `firing_id`, if it ran synchronously. `None` (the
    /// resident arm, and the default) means "accepted, not settled".
    fn take_outcome(&self, _firing_id: &str) -> Option<DispatchOutcome> {
        None
    }
}

/// A slot this tick admitted for execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    /// The owning schedule.
    pub schedule_id: String,
    /// The slot key, `{schedule_id}@{fire_time}`.
    pub firing_id: String,
    /// The computed instant the slot was due at.
    pub fire_time: DateTime<Utc>,
    /// The cursor value persisted with this admission.
    pub next_fire_at: DateTime<Utc>,
    /// Which §5.2/§5.3 branch admitted it (`OnTime` / `CatchUpLatest`).
    pub kind: kian_cron::DueKind,
    /// The dispatch payload (JSON, see [`CronDispatcher::dispatch`]).
    pub payload: String,
}

/// A slot this tick recorded as dropped, with the reason that decided it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// The owning schedule.
    pub schedule_id: String,
    /// The slot key, `{schedule_id}@{fire_time}`.
    pub firing_id: String,
    /// The computed instant the dropped slot was due at.
    pub fire_time: DateTime<Utc>,
    /// `superseded` / `outside_coalesce` / `policy_skip` (§9.1).
    pub reason: kian_cron::SkipReason,
}

/// One schedule whose tick could not be decided or recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failed {
    /// The owning schedule.
    pub schedule_id: String,
    /// The slot key, when a slot was already chosen (`""` when the failure
    /// happened before any slot existed, e.g. cursor read).
    pub firing_id: String,
    /// The slot's computed instant, or `now` for a pre-slot failure.
    pub fire_time: DateTime<Utc>,
    /// The error, as reported by the store or the dispatcher.
    pub error: String,
}

/// Everything one tick did, in three buckets.
#[derive(Debug, Default, PartialEq)]
pub struct TickReport {
    /// Slots claimed and handed on for execution, id order.
    pub admitted: Vec<Admitted>,
    /// Slots recorded as dropped, id order.
    pub skipped: Vec<Skipped>,
    /// Schedules whose tick failed (store error / dispatcher error).
    pub failed: Vec<Failed>,
}

/// One tick of the scheduler: decide, claim, advance — for every enabled
/// schedule, in id order.
///
/// Pure with respect to the world: the clock is `now`, the declarations are
/// `schedules`, and the only side effects are the [`CronTickStore`] calls.
/// Disabled schedules are ignored entirely (declared-but-off is a *listing*
/// state, §3.1). A store error on one schedule is recorded in
/// [`TickReport::failed`] and does not stop the others.
pub fn tick_once(
    now: DateTime<Utc>,
    schedules: &[CronRuntimeSchedule],
    store: &dyn CronTickStore,
) -> TickReport {
    let mut report = TickReport::default();
    let mut ordered: Vec<&CronRuntimeSchedule> = schedules.iter().filter(|s| s.enabled).collect();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    for schedule in ordered {
        tick_schedule(now, schedule, store, &mut report);
    }
    report
}

/// [`tick_once`], then hand every admitted slot to `dispatcher`.
///
/// A dispatch error settles the firing as `Failed` and moves the slot from
/// `admitted` to `failed` — the tick still ends with the cursor advanced, so
/// a failing executor cannot wedge the schedule into a re-fire loop.
pub fn tick_and_dispatch(
    now: DateTime<Utc>,
    schedules: &[CronRuntimeSchedule],
    store: &dyn CronTickStore,
    dispatcher: &dyn CronDispatcher,
) -> TickReport {
    let mut report = tick_once(now, schedules, store);
    let mut admitted = Vec::with_capacity(report.admitted.len());
    for entry in report.admitted.drain(..) {
        let Some(schedule) = schedules.iter().find(|s| s.id == entry.schedule_id) else {
            // Cannot happen: admitted entries are produced from `schedules`.
            admitted.push(entry);
            continue;
        };
        match dispatcher.dispatch(schedule, &entry.firing_id, &entry.payload) {
            Ok(()) => admitted.push(entry),
            Err(error) => {
                let _ = store.finish_firing(
                    &entry.firing_id,
                    FiringState::Failed,
                    Some(now),
                    None,
                    None,
                    Some(&error),
                );
                report.failed.push(Failed {
                    schedule_id: entry.schedule_id,
                    firing_id: entry.firing_id,
                    fire_time: entry.fire_time,
                    error,
                });
            }
        }
    }
    report.admitted = admitted;
    report
}

/// One schedule's tick — the §5.2/§5.3 decision table, applied.
fn tick_schedule(
    now: DateTime<Utc>,
    schedule: &CronRuntimeSchedule,
    store: &dyn CronTickStore,
    report: &mut TickReport,
) {
    let cursor = match store.load_cursor(&schedule.id) {
        Ok(cursor) => cursor,
        Err(error) => {
            report.failed.push(Failed {
                schedule_id: schedule.id.clone(),
                firing_id: String::new(),
                fire_time: now,
                error,
            });
            return;
        }
    };
    // A schedule that has never fired has no cursor yet: seed it with the
    // first computed fire time *after* `now`, so declaring a schedule never
    // back-fires a slot from before it existed.
    let next_fire_at = match cursor.next_fire_at {
        Some(next) => next,
        None => match schedule.next_fire_after(now) {
            Ok(next) => next,
            Err(error) => {
                report.failed.push(Failed {
                    schedule_id: schedule.id.clone(),
                    firing_id: String::new(),
                    fire_time: now,
                    error: error.to_string(),
                });
                return;
            }
        },
    };
    let plan = match kian_cron::plan_fire(kian_cron::DueInput {
        expr: &schedule.expr,
        tz: &schedule.tz,
        now,
        next_fire_at,
        last_fire_at: cursor.last_fire_at,
        catch_up: schedule.catch_up,
        coalesce: TimeDelta::milliseconds(schedule.coalesce_ms as i64),
    }) {
        Ok(plan) => plan,
        Err(error) => {
            report.failed.push(Failed {
                schedule_id: schedule.id.clone(),
                firing_id: String::new(),
                fire_time: next_fire_at,
                error: error.to_string(),
            });
            return;
        }
    };

    // 1. The dropped slots this plan decided on: each is a *record*, with
    //    its machine token. Recording happens before the fire slot so a
    //    crash mid-tick leaves at worst extra "this slot was superseded"
    //    rows, never a double firing.
    let mut last_skipped: Option<DateTime<Utc>> = None;
    for slot in &plan.skipped {
        let firing_id = kian_cron::firing_id(&schedule.id, slot.fire_time);
        let claim = FiringClaim {
            firing_id: firing_id.clone(),
            schedule_id: schedule.id.clone(),
            fire_time: slot.fire_time,
            admitted_at: now,
            state: FiringState::Skipped,
            skip_reason: Some(slot.reason.as_str()),
        };
        match store.claim_firing(&claim) {
            Ok(true) => report.skipped.push(Skipped {
                schedule_id: schedule.id.clone(),
                firing_id,
                fire_time: slot.fire_time,
                reason: slot.reason,
            }),
            // Already recorded (this tick's predecessor, or a crash replay):
            // nothing to add, the slot counts as handled either way.
            Ok(false) => {}
            Err(error) => {
                report.failed.push(Failed {
                    schedule_id: schedule.id.clone(),
                    firing_id,
                    fire_time: slot.fire_time,
                    error,
                });
                return;
            }
        }
        last_skipped = Some(match last_skipped {
            Some(prev) if prev >= slot.fire_time => prev,
            _ => slot.fire_time,
        });
    }

    // 2. The slot to fire, if this plan chose one.
    let Some(fire_time) = plan.fire else {
        // No firing: a pure-skip plan still advances the cursor, over its
        // last dropped slot, or the schedule would re-decide the same
        // missed window on every tick forever.
        if let Some(anchor) = last_skipped {
            let _ = store.advance_cursor(&schedule.id, anchor, plan.next_fire_at, now);
        }
        return;
    };

    let firing_id = kian_cron::firing_id(&schedule.id, fire_time);
    let claim = FiringClaim {
        firing_id: firing_id.clone(),
        schedule_id: schedule.id.clone(),
        fire_time,
        admitted_at: now,
        state: FiringState::Queued,
        skip_reason: None,
    };
    match store.claim_firing(&claim) {
        Ok(true) => {}
        Ok(false) => {
            // The slot is already recorded — a replay after a crash between
            // "admit" and "advance", or a lost race. Admit nothing, but do
            // the advance the crash interrupted (§4.3).
            let _ = store.advance_cursor(&schedule.id, fire_time, plan.next_fire_at, now);
            return;
        }
        Err(error) => {
            report.failed.push(Failed {
                schedule_id: schedule.id.clone(),
                firing_id,
                fire_time,
                error,
            });
            return;
        }
    }

    let payload = dispatch_payload(schedule, &firing_id, fire_time);
    report.admitted.push(Admitted {
        schedule_id: schedule.id.clone(),
        firing_id: firing_id.clone(),
        fire_time,
        next_fire_at: plan.next_fire_at,
        kind: plan.kind,
        payload,
    });

    // 3. Admit *then* advance, never the other way round (§4.3).
    if let Err(error) = store.advance_cursor(&schedule.id, fire_time, plan.next_fire_at, now) {
        report.failed.push(Failed {
            schedule_id: schedule.id.clone(),
            firing_id,
            fire_time,
            error,
        });
    }
}

/// The dispatch payload: one JSON object, the same shape for both future
/// dispatchers (resident-engine wake, worktree-backed `dsh`).
pub fn dispatch_payload(
    schedule: &CronRuntimeSchedule,
    firing_id: &str,
    fire_time: DateTime<Utc>,
) -> String {
    // The resolved target, in the shape it was declared (§3.2): a fixed
    // session, or a declared channel plus its routing topic.
    let target = match &schedule.target {
        crate::config::CronTarget::Session { session } => {
            serde_json::json!({ "session": session })
        }
        crate::config::CronTarget::Channel { channel, topic } => {
            serde_json::json!({ "channel": channel, "topic": topic })
        }
    };
    serde_json::json!({
        "schedule_id": schedule.id,
        "firing_id": firing_id,
        "fire_time": fire_time.to_rfc3339(),
        "prompt": schedule.prompt,
        "target": target,
    })
    .to_string()
}

/// The ticker cadence `server.rs` runs the loop at: one decision pass per
/// second. Cron expressions are minute-granular, so a second keeps every
/// honest schedule inside its coalesce window, and a missed beat is
/// absorbed by `MissedTickBehavior::Delay` plus the §5.3 catch-up policy —
/// the cadence never has to be exact for correctness.
pub const TICK_INTERVAL_MS: u64 = 1_000;

/// The structured events one tick report corresponds to: exactly one per
/// outcome, in report order — `cron/fired`, `cron/skipped`, `cron/failed`.
///
/// One event per outcome, never a summary, so a dropped slot is as
/// greppable as a firing. The ticker hands these to the engine's append
/// path; the shape here is the payload only, the loop owns the sink.
pub fn report_events(report: &TickReport, now: DateTime<Utc>) -> Vec<serde_json::Value> {
    let mut events = Vec::with_capacity(report.admitted.len() + report.skipped.len());
    for admitted in &report.admitted {
        events.push(serde_json::json!({
            "event": "cron/fired",
            "at": now.to_rfc3339(),
            "schedule_id": admitted.schedule_id,
            "firing_id": admitted.firing_id,
            "fire_time": admitted.fire_time.to_rfc3339(),
            "next_fire_at": admitted.next_fire_at.to_rfc3339(),
            "kind": format!("{:?}", admitted.kind),
            "payload": admitted.payload,
        }));
    }
    for skipped in &report.skipped {
        events.push(serde_json::json!({
            "event": "cron/skipped",
            "at": now.to_rfc3339(),
            "schedule_id": skipped.schedule_id,
            "firing_id": skipped.firing_id,
            "fire_time": skipped.fire_time.to_rfc3339(),
            "reason": skipped.reason.as_str(),
        }));
    }
    for failed in &report.failed {
        events.push(serde_json::json!({
            "event": "cron/failed",
            "at": now.to_rfc3339(),
            "schedule_id": failed.schedule_id,
            "firing_id": failed.firing_id,
            "fire_time": failed.fire_time.to_rfc3339(),
            "error": failed.error,
        }));
    }
    events
}

/// An in-memory [`CronTickStore`]: the unit tests' fake, and the shape the
/// `server.rs` ticker uses to collect a tick's intents before flushing them
/// to the real store.
///
/// It is *not* a cache of the durable world: `claim_firing` here is
/// last-writer-wins against its own map, so a [`tick_once`] run against it
/// only knows the slots that this process recorded. Cross-process claims are
/// still decided by the real store on flush — `INSERT … ON CONFLICT DO
/// NOTHING` — so the tick's own dedup is an optimisation, never the
/// guarantee.
/// One `advance_cursor` call recorded by [`MemoryStore`]: the schedule id
/// and the `(from, to)` cursor pair it moved between.
type Advance = (String, DateTime<Utc>, DateTime<Utc>);

#[derive(Debug, Default)]
pub struct MemoryStore {
    cursors: RefCell<BTreeMap<String, Cursor>>,
    firings: RefCell<BTreeMap<String, FiringClaim>>,
    advances: RefCell<Vec<Advance>>,
    settles: RefCell<Vec<(String, FiringState, Option<String>)>>,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a schedule's cursor (the durable `cron_schedules` read).
    pub fn set_cursor(&self, schedule_id: &str, cursor: Cursor) {
        self.cursors
            .borrow_mut()
            .insert(schedule_id.to_owned(), cursor);
    }

    /// The cursor the store currently holds for `schedule_id`.
    pub fn cursor(&self, schedule_id: &str) -> Cursor {
        self.cursors
            .borrow()
            .get(schedule_id)
            .copied()
            .unwrap_or_default()
    }

    /// Every claimed slot, by firing id.
    pub fn firings(&self) -> BTreeMap<String, FiringClaim> {
        self.firings.borrow().clone()
    }

    /// Every cursor move applied, in call order.
    pub fn advances(&self) -> Vec<Advance> {
        self.advances.borrow().clone()
    }

    /// Every settle applied, in call order.
    pub fn settles(&self) -> Vec<(String, FiringState, Option<String>)> {
        self.settles.borrow().clone()
    }
}

impl CronTickStore for MemoryStore {
    fn load_cursor(&self, schedule_id: &str) -> Result<Cursor, String> {
        Ok(self.cursor(schedule_id))
    }

    fn claim_firing(&self, claim: &FiringClaim) -> Result<bool, String> {
        let mut firings = self.firings.borrow_mut();
        if firings.contains_key(&claim.firing_id) {
            return Ok(false);
        }
        firings.insert(claim.firing_id.clone(), claim.clone());
        Ok(true)
    }

    fn advance_cursor(
        &self,
        schedule_id: &str,
        admitted_fire_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        if next_fire_at <= admitted_fire_at {
            return Err(format!(
                "cursor not advancing: next_fire_at {next_fire_at} <= admitted {admitted_fire_at}"
            ));
        }
        let mut cursors = self.cursors.borrow_mut();
        let entry = cursors.entry(schedule_id.to_owned()).or_default();
        if matches!(entry.next_fire_at, Some(current) if current > admitted_fire_at) {
            return Ok(false);
        }
        if !self
            .firings
            .borrow()
            .contains_key(&kian_cron::firing_id(schedule_id, admitted_fire_at))
        {
            return Err(format!(
                "cursor advance for an unrecorded slot {schedule_id}@{admitted_fire_at}"
            ));
        }
        entry.next_fire_at = Some(next_fire_at);
        entry.last_fire_at = Some(admitted_fire_at);
        self.advances
            .borrow_mut()
            .push((schedule_id.to_owned(), admitted_fire_at, next_fire_at));
        Ok(true)
    }

    fn finish_firing(
        &self,
        firing_id: &str,
        state: FiringState,
        finished_at: Option<DateTime<Utc>>,
        _worktree_path: Option<&str>,
        _skip_reason: Option<&str>,
        error: Option<&str>,
    ) -> Result<bool, String> {
        let terminal = matches!(
            state,
            FiringState::Succeeded | FiringState::Failed | FiringState::Skipped
        );
        if terminal && finished_at.is_none() {
            return Err(format!(
                "{firing_id}: terminal state {state:?} needs finished_at"
            ));
        }
        if !self.firings.borrow().contains_key(firing_id) {
            return Ok(false);
        }
        self.settles
            .borrow_mut()
            .push((firing_id.to_owned(), state, error.map(str::to_owned)));
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 6, 3, 12, 0, 0).unwrap()
    }

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    /// A resolved schedule from the real config path (so the test exercises
    /// the same `CronRuntimeSchedule` the ticker sees at runtime).
    fn schedule(id: &str, catch_up: &str, coalesce_ms: u64, enabled: bool) -> CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    catch_up: {catch_up}\n    \
             coalesce_ms: {coalesce_ms}\n    enabled: {enabled}\n    \
             target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit\n"
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the cron fixture parses");
        section
            .to_runtime(&channels(), now())
            .expect("the cron fixture resolves")
            .into_iter()
            .next()
            .expect("exactly one schedule")
    }

    #[test]
    fn on_time_fires_once() {
        let store = MemoryStore::new();
        store.set_cursor(
            "audit",
            Cursor {
                next_fire_at: Some(now()),
                last_fire_at: None,
            },
        );
        let schedules = vec![schedule("audit", "latest", 60_000, true)];

        let report = tick_once(now(), &schedules, &store);

        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.admitted.len(), 1, "{:?}", report.admitted);
        assert!(report.skipped.is_empty(), "{:?}", report.skipped);
        let fired = &report.admitted[0];
        assert_eq!(fired.schedule_id, "audit");
        assert_eq!(fired.firing_id, kian_cron::firing_id("audit", now()));
        assert_eq!(fired.fire_time, now());
        assert_eq!(fired.kind, kian_cron::DueKind::OnTime);
        assert_eq!(fired.next_fire_at, now() + TimeDelta::minutes(1));
        assert_eq!(store.firings().len(), 1);
        assert_eq!(store.cursor("audit").next_fire_at, Some(fired.next_fire_at));
    }

    #[test]
    fn catch_up_latest_inside_coalesce_window() {
        let store = MemoryStore::new();
        // 7 minutes late with a 5-minute coalesce window, last fired 2 minutes
        // ago: a misfire (§5.2: lateness > coalesce_ms) collapsed to one
        // catch-up firing, the rest recorded as superseded (§5.3).
        store.set_cursor(
            "audit",
            Cursor {
                next_fire_at: Some(now() - TimeDelta::minutes(7)),
                last_fire_at: Some(now() - TimeDelta::minutes(2)),
            },
        );
        let schedules = vec![schedule("audit", "latest", 300_000, true)];

        let report = tick_once(now(), &schedules, &store);

        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.admitted.len(), 1, "{:?}", report.admitted);
        assert_eq!(report.admitted[0].kind, kian_cron::DueKind::CatchUpLatest);
        // Every covered-but-dropped slot says *why* it was dropped.
        for skipped in &report.skipped {
            assert_eq!(skipped.reason, kian_cron::SkipReason::Superseded);
            assert_eq!(
                skipped.firing_id,
                kian_cron::firing_id("audit", skipped.fire_time)
            );
        }
    }

    #[test]
    fn skip_outside_coalesce_records_reason() {
        let store = MemoryStore::new();
        // A day late with a 60s window: the gap is reported, not replayed.
        store.set_cursor(
            "audit",
            Cursor {
                next_fire_at: Some(now() - TimeDelta::days(1)),
                last_fire_at: None,
            },
        );
        let schedules = vec![schedule("audit", "latest", 60_000, true)];

        let report = tick_once(now(), &schedules, &store);

        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.reason == kian_cron::SkipReason::OutsideCoalesce),
            "expected an outside_coalesce skip, got {:?}",
            report.skipped
        );
        // Whatever else the plan did, it never admitted two slots.
        assert!(report.admitted.len() <= 1, "{:?}", report.admitted);
    }

    #[test]
    fn disabled_schedule_never_fires() {
        let store = MemoryStore::new();
        store.set_cursor(
            "audit",
            Cursor {
                next_fire_at: Some(now() - TimeDelta::hours(2)),
                last_fire_at: None,
            },
        );
        let schedules = vec![schedule("audit", "latest", 60_000, false)];

        let report = tick_once(now(), &schedules, &store);

        assert_eq!(
            report,
            TickReport::default(),
            "a disabled schedule ticks nothing"
        );
        assert!(store.firings().is_empty());
        assert!(store.advances().is_empty());
    }

    #[test]
    fn second_tick_is_idempotent() {
        let store = MemoryStore::new();
        let cursor = Cursor {
            next_fire_at: Some(now()),
            last_fire_at: None,
        };
        store.set_cursor("audit", cursor);
        let schedules = vec![schedule("audit", "latest", 60_000, true)];

        let first = tick_once(now(), &schedules, &store);
        assert_eq!(first.admitted.len(), 1);

        // (a) a plain second tick at the same instant: nothing is due.
        let second = tick_once(now(), &schedules, &store);
        assert_eq!(second, TickReport::default(), "{second:?}");

        // (b) a crash between admit and advance: the cursor is rewound, the
        //     slot is already recorded, so the replay admits nothing but
        //     still advances the cursor it interrupted.
        store.set_cursor("audit", cursor);
        let replay = tick_once(now(), &schedules, &store);
        assert!(replay.admitted.is_empty(), "{:?}", replay.admitted);
        assert!(replay.skipped.is_empty(), "{:?}", replay.skipped);
        assert_eq!(store.firings().len(), 1, "one slot, recorded once");
        assert_eq!(
            store.cursor("audit").next_fire_at,
            Some(now() + TimeDelta::minutes(1)),
            "the interrupted advance is completed by the replay"
        );

        // (c) and a third tick is a no-op again.
        assert_eq!(tick_once(now(), &schedules, &store), TickReport::default());
    }

    struct FailingDispatcher;

    impl CronDispatcher for FailingDispatcher {
        fn dispatch(
            &self,
            _schedule: &CronRuntimeSchedule,
            firing_id: &str,
            _payload: &str,
        ) -> Result<(), String> {
            Err(format!("{firing_id}: resident engine is not running"))
        }
    }

    #[test]
    fn dispatch_error_marks_failed() {
        let store = MemoryStore::new();
        store.set_cursor(
            "audit",
            Cursor {
                next_fire_at: Some(now()),
                last_fire_at: None,
            },
        );
        let schedules = vec![schedule("audit", "latest", 60_000, true)];

        let report = tick_and_dispatch(now(), &schedules, &store, &FailingDispatcher);

        assert!(report.admitted.is_empty(), "{:?}", report.admitted);
        assert_eq!(report.failed.len(), 1, "{:?}", report.failed);
        assert_eq!(report.failed[0].schedule_id, "audit");
        assert_eq!(
            report.failed[0].firing_id,
            kian_cron::firing_id("audit", now())
        );
        assert!(
            report.failed[0]
                .error
                .contains("resident engine is not running")
        );
        let settles = store.settles();
        assert_eq!(settles.len(), 1, "{settles:?}");
        assert_eq!(settles[0].1, FiringState::Failed);
        // The cursor still moved: a broken executor cannot wedge the schedule.
        assert_eq!(
            store.cursor("audit").next_fire_at,
            Some(now() + TimeDelta::minutes(1))
        );
    }
}
