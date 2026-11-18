//! The resident half of the cron scheduler (increment B7): the durable
//! [`CronTickStore`] the *server* tick runs on, and the dispatchers it hands
//! admitted slots to.
//!
//! Before this module the resident loop decided its slots against
//! `cron_tick::MemoryStore`: every intent was recorded in a process-local map
//! and nothing was ever dispatched, so a `superagent.cron` schedule that fired
//! while `kian serve` was up ran no turn. Two seams close that gap:
//!
//! - **[`DurableCronTickStore`]** maps the four [`CronTickStore`] primitives
//!   onto the real store (`claim_firing` / `advance_cursor` /
//!   `finish_firing` / `load_schedule`), so admissions and settlements land in
//!   `cron_schedules` / `cron_firings` and survive a restart. The two worlds
//!   differ in shape: the tick loop is synchronous and clock-injected, the
//!   store is async — the adapter holds a [`tokio::runtime::Handle`] and
//!   blocks on it, which is why the resident ticker runs on its own OS thread
//!   rather than on a runtime worker;
//! - **[`ScheduleDispatcher`]** is the selection rule between the two
//!   executions CRON-MODEL allows: a schedule that asks for isolation
//!   (`superagent.cron.schedules[].worktree`) goes to the worktree-backed
//!   [`WorktreeDispatcher`], everything else to the resident engine
//!   ([`ResidentDispatcher`], which writes the payload as a durable inbox row
//!   and wakes its target session). Both remain [`CronDispatcher`]s, so tests
//!   inject fakes at the same seam.
//!
//! Failure semantics are the tick loop's, with one classification added here:
//! a dispatch `Err` settles the firing `failed` with the error text and never
//! advances silently (the cursor move already happened in `tick_once`, which
//! is the §4.3 "admit, then advance" order), while a *guard refusal*
//! (`disk_pressure: …`, `skipped_overlap: …`) settles it `skipped` with the
//! matching §9.1 token instead of counting as a failure.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use kian_core::model::SessionId;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::CronRuntimeSchedule;
use crate::cron_tick::{
    self, Admitted, CronDispatcher, CronTickStore, Cursor, Failed, FiringClaim, FiringState,
    TickReport,
};
use crate::cron_worktree::WorktreeDispatcher;
use kian_store::EventStore;

/// The §9.1 token a disk-guard refusal is recorded under.
pub const SKIP_DISK_PRESSURE: &str = "skipped_disk_pressure";
/// The §5.4 token an over-ceiling slot is recorded under.
pub const SKIP_OVERLAP: &str = "skipped_overlap";

/// One `fire_time` as the store spells it — the same rendering
/// `kian_cron::firing_id` embeds, so the row's key and its columns agree.
fn fire_time_text(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Epoch milliseconds, the store's timestamp unit.
fn epoch_ms(at: DateTime<Utc>) -> u64 {
    u64::try_from(at.timestamp_millis()).unwrap_or(0)
}

fn parse_instant(raw: &str, what: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|e| format!("cron {what} `{raw}` is not RFC 3339: {e}"))
}

/// The tick loop's state vocabulary on the store's (they differ in exactly one
/// name: the tick says `queued`, the row says `admitted`).
fn store_state(state: FiringState) -> kian_store::FiringState {
    match state {
        FiringState::Queued => kian_store::FiringState::Admitted,
        FiringState::Running => kian_store::FiringState::Running,
        FiringState::Succeeded => kian_store::FiringState::Succeeded,
        FiringState::Failed => kian_store::FiringState::Failed,
        FiringState::Skipped => kian_store::FiringState::Skipped,
    }
}

/// The four durable primitives, over the real store (CRON-MODEL §4.1/§4.3).
///
/// Held by the resident ticker's own thread; every write is the store's
/// single-statement conditional, so a second replica racing the same slot
/// gets `Ok(false)` from `claim_firing` and admits nothing.
pub struct DurableCronTickStore {
    store: EventStore,
    handle: tokio::runtime::Handle,
    /// The declarations this adapter serves, by id — `load_cursor` needs the
    /// expression to compute a *never-fired* schedule's first due time.
    schedules: BTreeMap<String, CronRuntimeSchedule>,
    /// The declaration anchor of every never-fired schedule: the store keeps
    /// `next_fire_at` NULL until the first recorded slot, so the first due
    /// time lives here until a firing writes it. Pinned per id (not
    /// recomputed per tick) so a schedule cannot slide its own due time
    /// forward and never fire.
    anchors: Mutex<BTreeMap<String, DateTime<Utc>>>,
}

impl DurableCronTickStore {
    /// An adapter over `store` for `schedules`, driving futures on `handle`.
    pub fn new(
        store: EventStore,
        handle: tokio::runtime::Handle,
        schedules: Vec<CronRuntimeSchedule>,
    ) -> Self {
        Self {
            store,
            handle,
            schedules: schedules.into_iter().map(|s| (s.id.clone(), s)).collect(),
            anchors: Mutex::new(BTreeMap::new()),
        }
    }

    /// Run one store future to completion on the server's runtime. Only ever
    /// called from the ticker's own thread (never from a runtime worker).
    fn block<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        self.handle.block_on(future)
    }

    /// Syncs `cron_schedules` with the declaration (§4.1/§12.3). One
    /// conditional upsert decides every case against the live row: absent ⇒
    /// insert, same digest ⇒ cursor untouched, different digest ⇒ adopt the
    /// declaration and reset the cursor. A reset is logged with both digests
    /// so an edited schedule is never indistinguishable from a restart.
    fn declare(&self, schedule: &CronRuntimeSchedule) -> Result<(), String> {
        let digest = declaration_digest(schedule);
        let updated = self.block(self.store.upsert_schedule(
            &schedule.id,
            &schedule.expr.to_string(),
            &schedule.tz_name,
            &digest,
            schedule.enabled,
            epoch_ms(Utc::now()),
        ));
        match updated {
            Ok(kian_store::ScheduleUpsert::Redeclared { previous_digest }) => {
                tracing::info!(
                    schedule_id = %schedule.id,
                    previous_digest = %previous_digest,
                    digest = %digest,
                    "cron declaration changed; cursor reset"
                );
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(error) => Err(format!("cron_schedules `{}`: {error}", schedule.id)),
        }
    }
}

/// The declaration identity of §4.1 — sha256 over the normalized expression,
/// the effective IANA zone and the resolved target. Everything else a
/// schedule declares (`prompt`, `overlap`, `catch_up`, timeouts) is policy
/// that must not restart the cursor (§12.3).
pub fn declaration_digest(schedule: &CronRuntimeSchedule) -> String {
    let target = match &schedule.target {
        crate::config::CronTarget::Session { session } => format!("session:{session}"),
        crate::config::CronTarget::Channel { channel, topic } => {
            format!("channel:{channel}/{topic}")
        }
    };
    let mut hasher = Sha256::new();
    for part in [schedule.expr.to_string(), schedule.tz_name.clone(), target] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

impl CronTickStore for DurableCronTickStore {
    fn load_cursor(&self, schedule_id: &str) -> Result<Cursor, String> {
        // The declaration is synced on every tick, *before* anything is
        // decided (§4.1/§12.3): the row is created from the config when it
        // is absent (a fresh deployment, or a `kian cron rm` behind the
        // running server), a changed declaration resets the cursor in the
        // same atomic statement, and an unchanged one leaves it untouched.
        // Reading a row that was never written was the old failure: no
        // declaration ever reached the store, so every tick reported
        // `no row` and no slot could be admitted.
        let schedule = self
            .schedules
            .get(schedule_id)
            .ok_or_else(|| format!("cron_schedules `{schedule_id}`: undeclared"))?;
        self.declare(schedule)?;
        let Some(row) = self
            .block(self.store.load_schedule(schedule_id))
            .map_err(|e| format!("cron_schedules `{schedule_id}`: {e}"))?
        else {
            return Ok(Cursor::default());
        };
        let last_fire_at = match row.cursor_fire_at.as_deref() {
            Some(raw) => Some(parse_instant(raw, "cursor_fire_at")?),
            None => None,
        };
        if let Some(raw) = row.next_fire_at.as_deref() {
            return Ok(Cursor {
                next_fire_at: Some(parse_instant(raw, "next_fire_at")?),
                last_fire_at,
            });
        }
        // Never fired: seed the anchor once, from the declaration itself.
        if let Some(anchor) = self
            .anchors
            .lock()
            .map_err(|_| "cron anchor map poisoned".to_string())?
            .get(schedule_id)
            .copied()
        {
            return Ok(Cursor {
                next_fire_at: Some(anchor),
                last_fire_at,
            });
        }
        let seed = schedule
            .next_fire_after(Utc::now())
            .map_err(|e| format!("cron_schedules `{schedule_id}`: {e}"))?;
        self.anchors
            .lock()
            .map_err(|_| "cron anchor map poisoned".to_string())?
            .insert(schedule_id.to_owned(), seed);
        Ok(Cursor {
            next_fire_at: Some(seed),
            last_fire_at,
        })
    }

    fn claim_firing(&self, claim: &FiringClaim) -> Result<bool, String> {
        let row = kian_store::CronFiringRow {
            firing_id: claim.firing_id.clone(),
            schedule_id: claim.schedule_id.clone(),
            fire_time: fire_time_text(claim.fire_time),
            admitted_at: epoch_ms(claim.admitted_at),
            state: store_state(claim.state),
            skip_reason: claim.skip_reason.map(str::to_owned),
            worktree_path: None,
            finished_at: None,
            error: None,
        };
        self.block(self.store.claim_firing(&row))
            .map_err(|e| format!("cron_firings `{}`: {e}", claim.firing_id))
    }

    fn advance_cursor(
        &self,
        schedule_id: &str,
        admitted_fire_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        self.block(self.store.advance_cursor(
            schedule_id,
            &fire_time_text(admitted_fire_at),
            &fire_time_text(next_fire_at),
            epoch_ms(now),
        ))
        .map_err(|e| format!("cron_schedules `{schedule_id}` cursor: {e}"))
    }

    fn finish_firing(
        &self,
        firing_id: &str,
        state: FiringState,
        finished_at: Option<DateTime<Utc>>,
        error: Option<&str>,
    ) -> Result<bool, String> {
        self.block(self.store.finish_firing(
            firing_id,
            store_state(state),
            finished_at.map(epoch_ms),
            None,
            error,
        ))
        .map_err(|e| format!("cron_firings `{firing_id}` settle: {e}"))
    }
}

/// The resident engine, as a dispatcher sees it: hand the payload to a session
/// and let the engine run it. Injected, so a test never touches an engine.
pub trait ResidentTurn: Send + Sync {
    /// Write `content` as a durable inbox row for `session` and wake it.
    /// `Err` means the row could not be written — the firing has not started.
    fn deliver(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<(), String>;
}

/// The deterministic resident session of a channel target.
///
/// A `{channel, topic}` schedule has no session id in its declaration, and the
/// resident engine addresses turns by session. The mapping is therefore a
/// stable hash of `cron:<channel>/<topic>`: every firing of that channel/topic
/// lands in the same session (one ongoing conversation, like a real channel
/// thread) and a restart re-derives the same id — no registry needed.
fn channel_session(channel: &str, topic: &str) -> SessionId {
    let name = format!("cron:{channel}/{topic}");
    let mut bytes = [0u8; 16];
    for (i, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ (i as u64);
        for byte in name.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        chunk.copy_from_slice(&hash.to_le_bytes());
    }
    SessionId(uuid::Uuid::from_bytes(bytes))
}

/// The session a schedule's firing belongs to (CRON-MODEL §3.2).
pub fn target_session(schedule: &CronRuntimeSchedule) -> Result<SessionId, String> {
    match &schedule.target {
        crate::config::CronTarget::Session { session } => uuid::Uuid::parse_str(session)
            .map(SessionId)
            .map_err(|e| format!("target.session `{session}` is not a uuid: {e}")),
        crate::config::CronTarget::Channel { channel, topic } => {
            Ok(channel_session(channel, topic))
        }
    }
}

/// The resident-engine [`CronDispatcher`]: admitted slots become durable inbox
/// rows on their target session, which the already-resident engine drains.
pub struct ResidentDispatcher {
    turn: Arc<dyn ResidentTurn>,
}

impl ResidentDispatcher {
    /// A dispatcher over the injected resident turn sink.
    pub fn new(turn: Arc<dyn ResidentTurn>) -> Self {
        Self { turn }
    }
}

impl CronDispatcher for ResidentDispatcher {
    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
    ) -> Result<(), String> {
        let session = target_session(schedule)?;
        let cron: Value = serde_json::from_str(payload)
            .map_err(|e| format!("dispatch payload is not JSON: {e}"))?;
        // `content` is the turn's user message; `cron` carries the routing
        // proof (schedule_id / firing_id / fire_time / target) as an additive
        // non-context key, so the model request is the prompt alone.
        let content = serde_json::json!({ "content": schedule.prompt, "cron": cron });
        self.turn
            .deliver(session, content, Some(format!("cron:{firing_id}")))
    }
}

/// The dispatcher selection rule: worktree isolation when the schedule asks for
/// it, the resident engine otherwise.
///
/// Both arms stay [`CronDispatcher`]s, so the choice is one branch and a test
/// can arm either with a fake.
pub struct ScheduleDispatcher {
    resident: Option<Arc<dyn CronDispatcher + Send + Sync>>,
    worktree: Option<Arc<WorktreeDispatcher>>,
}

impl ScheduleDispatcher {
    /// A dispatcher over the two execution paths (either may be absent: an
    /// absent arm makes a schedule that selects it fail loudly).
    pub fn new(
        resident: Option<Arc<dyn CronDispatcher + Send + Sync>>,
        worktree: Option<Arc<WorktreeDispatcher>>,
    ) -> Self {
        Self { resident, worktree }
    }

    /// The resident-only selection (no schedule asked for isolation).
    pub fn resident(resident: Arc<dyn CronDispatcher + Send + Sync>) -> Self {
        Self::new(Some(resident), None)
    }

    /// Add the worktree-backed arm.
    pub fn with_worktree(mut self, worktree: Arc<WorktreeDispatcher>) -> Self {
        self.worktree = Some(worktree);
        self
    }

    /// Which arm `schedule` selects.
    pub fn selects_worktree(schedule: &CronRuntimeSchedule) -> bool {
        schedule.worktree.is_some()
    }
}

impl CronDispatcher for ScheduleDispatcher {
    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
    ) -> Result<(), String> {
        if Self::selects_worktree(schedule) {
            return match self.worktree.as_ref() {
                Some(worktree) => worktree.dispatch(schedule, firing_id, payload),
                None => Err(format!(
                    "schedule `{}` asks for worktree isolation but no worktree dispatcher is \
                     armed (no `worktree.repo` resolved)",
                    schedule.id
                )),
            };
        }
        match self.resident.as_ref() {
            Some(resident) => resident.dispatch(schedule, firing_id, payload),
            None => Err(format!(
                "schedule `{}` selects the resident engine but no resident dispatcher is armed",
                schedule.id
            )),
        }
    }
}

/// The §9.1 skip token a guard refusal maps to, or `None` for a real failure.
fn guard_skip_token(error: &str) -> Option<&'static str> {
    if error.starts_with("disk_pressure:") {
        Some(SKIP_DISK_PRESSURE)
    } else if error.starts_with("skipped_overlap") || error.starts_with("skipped_overlap:") {
        Some(SKIP_OVERLAP)
    } else {
        None
    }
}

/// [`cron_tick::tick_and_dispatch`], with guard refusals classified as skips.
///
/// A dispatch error settles the slot `failed` with the error text (and is
/// reported); a guard refusal — the §6.4 disk preflight, the §5.4 concurrency
/// ceiling — settles it `skipped` under its machine token, because the slot was
/// consumed without a turn by policy, not by breakage. Either way the cursor
/// already advanced (`tick_once` admits then advances), so a refusing executor
/// cannot wedge the schedule into a re-fire loop.
///
/// Returns the report plus the extra `cron/skipped` events the refusal paths
/// produce (`cron_tick::report_events` renders the report's own buckets).
pub fn tick_and_dispatch_classified(
    now: DateTime<Utc>,
    schedules: &[CronRuntimeSchedule],
    store: &dyn CronTickStore,
    dispatcher: &dyn CronDispatcher,
) -> (TickReport, Vec<Value>) {
    let mut report = cron_tick::tick_once(now, schedules, store);
    let mut events: Vec<Value> = Vec::new();
    let mut admitted: Vec<Admitted> = Vec::with_capacity(report.admitted.len());
    for entry in report.admitted.drain(..) {
        let Some(schedule) = schedules.iter().find(|s| s.id == entry.schedule_id) else {
            admitted.push(entry);
            continue;
        };
        match dispatcher.dispatch(schedule, &entry.firing_id, &entry.payload) {
            Ok(()) => admitted.push(entry),
            Err(error) => match guard_skip_token(&error) {
                Some(token) => {
                    let _ = store.finish_firing(
                        &entry.firing_id,
                        FiringState::Skipped,
                        Some(now),
                        Some(&error),
                    );
                    events.push(serde_json::json!({
                        "event": "cron/skipped",
                        "at": now.to_rfc3339(),
                        "schedule_id": entry.schedule_id,
                        "firing_id": entry.firing_id,
                        "fire_time": entry.fire_time.to_rfc3339(),
                        "reason": token,
                    }));
                }
                None => {
                    let _ = store.finish_firing(
                        &entry.firing_id,
                        FiringState::Failed,
                        Some(now),
                        Some(&error),
                    );
                    report.failed.push(Failed {
                        schedule_id: entry.schedule_id,
                        firing_id: entry.firing_id,
                        fire_time: entry.fire_time,
                        error,
                    });
                }
            },
        }
    }
    report.admitted = admitted;
    (report, events)
}

/// The declared coalesce default, so the durable path's lateness matches the
/// config default rather than an accident (`CronSection::to_runtime` applies the
/// same value; this constant only documents the tests' fixture instant).
pub const SEED_SLACK: TimeDelta = TimeDelta::seconds(1);

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::cron_worktree::{TurnOutput, TurnRequest, TurnRunner, WorktreePolicy};

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    fn schedule_yaml(id: &str, extra: &str) -> crate::config::CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    max_concurrent: 1\n    \
             timeout_ms: 60000\n{extra}    target: {{ channel: cron-jobs, topic: audit }}\n    \
             prompt: audit .state/\n"
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the cron fixture parses");
        section
            .to_runtime(&channels(), Utc::now())
            .expect("the cron fixture resolves")
            .into_iter()
            .next()
            .expect("exactly one schedule")
    }

    fn schedule(id: &str) -> crate::config::CronRuntimeSchedule {
        schedule_yaml(id, "")
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = std::env::temp_dir()
                .join(format!("kian-cron-b7-{tag}-{}-{stamp}", std::process::id()));
            std::fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The fake resident sink: counts deliveries and keeps the payloads.
    #[derive(Default)]
    struct FakeResident {
        calls: AtomicUsize,
        seen: Mutex<Vec<(SessionId, Value)>>,
    }

    impl ResidentTurn for FakeResident {
        fn deliver(
            &self,
            session: SessionId,
            content: Value,
            _idempotency_key: Option<String>,
        ) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut seen) = self.seen.lock() {
                seen.push((session, content));
            }
            Ok(())
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn durable_adapter_admits_once_across_two_ticks_and_dispatches_once() {
        let rt = runtime();
        let dir = TempDir::new("durable");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");

        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let fake = Arc::new(FakeResident::default());
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(fake.clone())));

        // The declaration anchor: the first due time after the declaration.
        let anchor = schedule
            .next_fire_after(Utc::now())
            .expect("a minute-granular expression has a next fire");

        // Tick 1 — one second past the anchor: due, admitted, dispatched.
        let at = anchor + SEED_SLACK;
        let (first, first_events) = tick_and_dispatch_classified(
            at,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(first.failed.is_empty(), "{:?}", first.failed);
        assert_eq!(first.admitted.len(), 1, "the due slot is admitted once");
        assert_eq!(
            first.admitted[0].firing_id,
            kian_cron::firing_id("audit", anchor)
        );
        assert!(first_events.is_empty());
        assert_eq!(
            fake.calls.load(Ordering::SeqCst),
            1,
            "the payload reached the engine once"
        );

        // Tick 2 — the same instant again: the durable claim says "already
        // recorded", so nothing is admitted and nothing is dispatched.
        let (second, _) = tick_and_dispatch_classified(
            at,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(second.admitted.is_empty(), "{:?}", second.admitted);
        assert!(second.failed.is_empty(), "{:?}", second.failed);
        assert_eq!(
            fake.calls.load(Ordering::SeqCst),
            1,
            "a replay of the same slot dispatches nothing"
        );

        // The row is durable, in the store, exactly once.
        let firings = rt
            .block_on(store.list_firings(Some("audit"), 10))
            .expect("the firing rows read");
        assert_eq!(firings.len(), 1, "{firings:?}");
        assert_eq!(firings[0].firing_id, kian_cron::firing_id("audit", anchor));
        assert_eq!(firings[0].state.as_str(), "admitted");
        let row = rt
            .block_on(store.load_schedule("audit"))
            .expect("the schedule row reads")
            .expect("the schedule row exists");
        assert_eq!(
            row.cursor_fire_at.as_deref(),
            Some(fire_time_text(anchor).as_str())
        );
        assert!(
            row.next_fire_at.is_some(),
            "the cursor advanced to the next fire"
        );
    }

    #[test]
    fn a_failing_dispatch_settles_the_firing_failed_with_the_error_text() {
        struct Refusing;
        impl ResidentTurn for Refusing {
            fn deliver(&self, _: SessionId, _: Value, _: Option<String>) -> Result<(), String> {
                Err("engine refused the admit".to_string())
            }
        }
        let rt = runtime();
        let dir = TempDir::new("failed");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(Refusing))));
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let (report, _) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.admitted.is_empty());
        assert_eq!(report.failed.len(), 1, "{report:?}");
        assert_eq!(report.failed[0].error, "engine refused the admit");
        let firings = rt
            .block_on(store.list_firings(Some("audit"), 10))
            .expect("the firing rows read");
        assert_eq!(firings[0].state.as_str(), "failed");
        assert_eq!(
            firings[0].error.as_deref(),
            Some("engine refused the admit")
        );
    }

    #[test]
    fn a_disk_guard_refusal_is_recorded_as_skipped_disk_pressure() {
        struct DiskGuard;
        impl ResidentTurn for DiskGuard {
            fn deliver(&self, _: SessionId, _: Value, _: Option<String>) -> Result<(), String> {
                Err("disk_pressure: 12 GiB free, floor 80 GiB".to_string())
            }
        }
        let rt = runtime();
        let dir = TempDir::new("disk");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(DiskGuard))));
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let (report, events) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["reason"], SKIP_DISK_PRESSURE);
        let firings = rt
            .block_on(store.list_firings(Some("audit"), 10))
            .expect("the firing rows read");
        assert_eq!(firings[0].state.as_str(), "skipped");
        assert!(
            firings[0]
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("disk_pressure"),
            "{firings:?}"
        );
    }

    /// A fake harness: spawns nothing, records that it ran.
    struct CountingRunner {
        calls: AtomicUsize,
    }

    impl TurnRunner for CountingRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.worktree.exists(),
                "the turn runs in a real worktree: {}",
                request.worktree.display()
            );
            Ok(TurnOutput {
                stdout: "did the thing\n".to_string(),
                stderr: String::new(),
                exit_code: Some(0),
                timed_out: false,
                too_large: false,
            })
        }
    }

    /// Hands the injected runner's counter out again after the box is moved.
    struct SharedRunner(Arc<CountingRunner>);

    impl TurnRunner for SharedRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.0.run(request)
        }
    }

    fn init_repo(dir: &Path) {
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .expect("git runs");
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        std::fs::create_dir_all(dir).expect("repo dir");
        git(&["init", "-q"]);
        std::fs::write(dir.join("notes.txt"), "one\n").expect("write");
        git(&[
            "-c",
            "user.email=cron@example.invalid",
            "-c",
            "user.name=cron",
            "add",
            ".",
        ]);
        git(&[
            "-c",
            "user.email=cron@example.invalid",
            "-c",
            "user.name=cron",
            "commit",
            "-q",
            "-m",
            "one",
        ]);
    }

    #[test]
    fn a_worktree_isolation_schedule_routes_to_the_worktree_dispatcher() {
        let dir = TempDir::new("worktree");
        let repo = dir.path().join("repo");
        init_repo(&repo);

        let schedule = schedule_yaml("isolated", "    worktree: { root: kian-worktrees }\n");
        assert!(
            ScheduleDispatcher::selects_worktree(&schedule),
            "the schedule's `worktree` policy selects isolation"
        );

        let runner = Arc::new(CountingRunner {
            calls: AtomicUsize::new(0),
        });
        let worktree = Arc::new(
            WorktreeDispatcher::new(repo.clone())
                .with_policy(WorktreePolicy {
                    min_free_mib: 0,
                    min_free_percent: 0,
                    ..WorktreePolicy::default()
                })
                .with_runner(Box::new(SharedRunner(Arc::clone(&runner)))),
        );

        // The resident arm is armed with a fake that must never be called.
        let resident_fake = Arc::new(FakeResident::default());
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(resident_fake.clone())))
                .with_worktree(Arc::clone(&worktree));

        let fire_time = DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc);
        let firing_id = kian_cron::firing_id("isolated", fire_time);
        let payload = cron_tick::dispatch_payload(&schedule, &firing_id, fire_time);
        dispatcher
            .dispatch(&schedule, &firing_id, &payload)
            .expect("the worktree arm accepts the firing");

        assert_eq!(
            resident_fake.calls.load(Ordering::SeqCst),
            0,
            "an isolation schedule never reaches the resident engine"
        );
        assert_eq!(
            runner.calls.load(Ordering::SeqCst),
            1,
            "the worktree turn ran once"
        );
        assert!(
            worktree.firing(&firing_id).is_some(),
            "the firing is on the worktree ledger"
        );
        // The tree itself is removed after a clean turn (the runner above
        // asserted it existed while the turn ran); the root it was cut into
        // stays as the durable proof the worktree path was taken.
        let root = worktree.worktree_root();
        assert!(
            root.exists(),
            "the fixture repo's worktree root was created: {}",
            root.display()
        );
    }

    #[test]
    fn declaration_digest_tracks_the_declaration_and_not_the_policy() {
        let base = schedule("audit");
        let digest = declaration_digest(&base);

        let mut policy = base.clone();
        policy.prompt = "a different prompt".to_string();
        policy.timeout_ms += 1;
        policy.coalesce_ms += 1;
        assert_eq!(
            declaration_digest(&policy),
            digest,
            "policy is not identity"
        );

        let mut expr = base.clone();
        expr.expr = kian_cron::CronExpr::parse("*/5 * * * *").expect("expr parses");
        assert_ne!(
            declaration_digest(&expr),
            digest,
            "a new expr is a new declaration"
        );

        let mut zone = base.clone();
        zone.tz_name = "Europe/Berlin".to_string();
        assert_ne!(
            declaration_digest(&zone),
            digest,
            "a new zone is a new declaration"
        );

        let mut target = base.clone();
        target.target = crate::config::CronTarget::Channel {
            channel: "cron-jobs".to_string(),
            topic: "other".to_string(),
        };
        assert_ne!(
            declaration_digest(&target),
            digest,
            "a new target is a new declaration"
        );
    }

    #[test]
    fn the_first_tick_declares_the_row_instead_of_failing_no_row() {
        let rt = runtime();
        let dir = TempDir::new("declare");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);

        assert!(
            rt.block_on(store.load_schedule("audit"))
                .expect("the row reads")
                .is_none(),
            "nothing declares the schedule before the tick"
        );

        let cursor = durable
            .load_cursor("audit")
            .expect("the tick declares the row");
        assert!(
            cursor.next_fire_at.is_some(),
            "a freshly declared schedule is seeded from its declaration"
        );

        let row = rt
            .block_on(store.load_schedule("audit"))
            .expect("the row reads")
            .expect("the tick wrote the declaration row");
        assert_eq!(row.expr, schedule.expr.to_string());
        assert_eq!(row.tz, schedule.tz_name);
        assert_eq!(row.declaration_digest, declaration_digest(&schedule));
        assert!(row.enabled);
        assert!(row.cursor_fire_at.is_none(), "a fresh row has no cursor");
    }

    #[test]
    fn an_unchanged_declaration_keeps_the_cursor_and_a_changed_one_resets_it() {
        let rt = runtime();
        let dir = TempDir::new("redeclare");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let fake = Arc::new(FakeResident::default());
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(fake.clone())));

        let anchor = schedule
            .next_fire_after(Utc::now())
            .expect("a minute-granular expression has a next fire");
        let (report, _) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.admitted.len(), 1);
        let fired = rt
            .block_on(store.load_schedule("audit"))
            .expect("the row reads")
            .expect("the row exists");
        assert_eq!(
            fired.cursor_fire_at.as_deref(),
            Some(fire_time_text(anchor).as_str())
        );

        // A restart with the same declaration: the cursor survives.
        let same =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        same.load_cursor("audit").expect("the redeclare reads");
        let kept = rt
            .block_on(store.load_schedule("audit"))
            .expect("the row reads")
            .expect("the row exists");
        assert_eq!(
            kept.cursor_fire_at, fired.cursor_fire_at,
            "an unchanged declaration never moves the cursor"
        );
        assert_eq!(kept.next_fire_at, fired.next_fire_at);

        // An edited expression: the row adopts it and the cursor resets.
        let mut edited = schedule.clone();
        edited.expr = kian_cron::CronExpr::parse("*/5 * * * *").expect("expr parses");
        let changed = DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![edited]);
        changed.load_cursor("audit").expect("the redeclare reads");
        let reset = rt
            .block_on(store.load_schedule("audit"))
            .expect("the row reads")
            .expect("the row exists");
        assert_eq!(reset.expr, "*/5 * * * *");
        assert!(
            reset.cursor_fire_at.is_none(),
            "a changed declaration resets the cursor"
        );
        assert!(reset.next_fire_at.is_none());
    }
}
