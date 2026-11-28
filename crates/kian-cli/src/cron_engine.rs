//! The resident half of the cron scheduler (increment B7): the durable
//! [`CronTickStore`] the *server* tick runs on, and the dispatchers it hands
//! admitted slots to. Beyond the four decision primitives the tick store also
//! carries the settlement sweep and the per-firing session record
//! (CRON-MODEL §3.2/§9.1).
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
//!   ([`ResidentDispatcher`], which resolves the delivery session and writes the
//!   payload as a durable inbox row there). Both remain [`CronDispatcher`]s, so
//!   tests inject fakes at the same seam.
//!
//! Failure semantics are the tick loop's, with one classification added here:
//! a dispatch `Err` settles the firing `failed` with the error text and never
//! advances silently (the cursor move already happened in `tick_once`, which
//! is the §4.3 "admit, then advance" order), while a *guard refusal*
//! (`disk_pressure: …`, `skipped_overlap: …`) settles it `skipped` with the
//! matching §9.1 token instead of counting as a failure.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use kian_core::events::{EndReason, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::LoopDriver;
use kian_core::state::project;
use kian_orchestrator::{DrainCtx, Engine};
use kian_superagent::SessionBootstrap;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::CronRuntimeSchedule;
use crate::cron_tick::{
    self, Admitted, CronDispatcher, CronTickStore, Cursor, DispatchOutcome, Failed, FiringClaim,
    FiringState, TickReport,
};
use crate::cron_worktree::WorktreeDispatcher;
use kian_store::EventStore;

/// The §9.1 token a disk-guard refusal is recorded under.
pub const SKIP_DISK_PRESSURE: &str = "skipped_disk_pressure";
/// The §5.4 token an over-ceiling slot is recorded under.
pub const SKIP_OVERLAP: &str = "skipped_overlap";

/// The `cron_firings.error` prefix for a resident firing that can never run:
/// a resolved session frozen before the admit (a race), a stranded pre-v8 /
/// pre-succession record, a recorded session that never took the row (a crash
/// between the session record and the admit), or a corrupt `session` column.
/// Never a policy skip (CRON-MODEL §5.1/§9.3).
pub const TARGET_TERMINAL: &str = "target_terminal";

/// The stranded-heal grace (CRON-MODEL §9.1). A firing row without a recorded
/// session (pre-v8) or whose recorded session never took it is settled
/// `failed` only once this much wall-clock has passed since `admitted_at`:
/// another replica may be inside its claim → resolve → record → admit window,
/// and replica clock skew is far below this bound.
pub const STRANDED_GRACE_MS: u64 = 60_000;

/// The frozen v5 name input of the per-slot firing-session derivation
/// (CRON-MODEL §3.2). Changing it re-derives every existing firing session id
/// and orphans them: contract, like the resident re-arm name (ADR-040).
pub const FIRING_SESSION_V5_NAME: &str = "kian:cron:firing";

/// The session a firing runs in once its configured target is frozen:
/// `uuid5(target, "kian:cron:firing:<firing_id>")` — one fresh session per
/// slot, derivable by every replica from durable facts alone (no registry,
/// no chain walk). A plain target ends after its turn, so recurring schedules
/// must not deliver to it twice (CRON-MODEL §3.2/§9.4).
#[must_use]
pub fn firing_session(target: SessionId, firing_id: &str) -> SessionId {
    SessionId(uuid::Uuid::new_v5(
        &target.0,
        format!("{FIRING_SESSION_V5_NAME}:{firing_id}").as_bytes(),
    ))
}

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
    /// Per-schedule last settlement sweep (CRON-MODEL §9.1): the target
    /// session's stream is read at most once per window, not per tick.
    sweeps: Mutex<BTreeMap<String, Instant>>,
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
            sweeps: Mutex::new(BTreeMap::new()),
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

    /// The firing's row is still waiting in `session`'s inbox (not consumed
    /// yet). Settlement uses this to distinguish "the admit never landed"
    /// (stranded, heal) from "the row is pending and can still be taken"
    /// (leave the firing open), including the empty-session case where both
    /// look like an empty stream.
    fn inbox_holds_firing(&self, session: SessionId, firing_id: &str) -> Result<bool, String> {
        let rows = self
            .block(self.store.eligible_inputs(session, epoch_ms(Utc::now())))
            .map_err(|e| format!("inbox `{}`: {e}", session.0))?;
        Ok(rows.iter().any(|row| {
            row.payload
                .pointer("/cron/firing_id")
                .and_then(Value::as_str)
                == Some(firing_id)
        }))
    }

    /// Settle one stranded firing `failed` (CRON-MODEL §9.1). A store error is
    /// logged and retried by the next sweep; it never aborts the others.
    fn settle_stranded(&self, firing_id: &str, finished_at: u64, error: &str) {
        if let Err(settle_error) = self.block(self.store.finish_firing(
            firing_id,
            kian_store::FiringState::Failed,
            Some(finished_at),
            None,
            None,
            Some(error),
        )) {
            tracing::warn!(
                firing_id,
                %settle_error,
                "cron stranded settlement failed; the next sweep retries"
            );
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
        crate::config::CronTarget::Job { job } => format!("job:{job}"),
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
        // Read-first: the steady state does not write. A tick used to
        // upsert the declaration every second; under sqlite that write
        // raced the drain/assembly writers and one boot tick failed with
        // `database is locked`. Only an absent row, a changed digest or a
        // changed `enabled` needs the upsert (which is still the same
        // atomic statement: same digest = cursor untouched, changed =
        // cursor reset).
        let digest = declaration_digest(schedule);
        let existing = self
            .block(self.store.load_schedule(schedule_id))
            .map_err(|e| format!("cron_schedules `{schedule_id}`: {e}"))?;
        let needs_declare = existing
            .as_ref()
            .is_none_or(|row| row.declaration_digest != digest || row.enabled != schedule.enabled);
        if needs_declare {
            self.declare(schedule)?;
        }
        let row = if needs_declare || existing.is_none() {
            self.block(self.store.load_schedule(schedule_id))
                .map_err(|e| format!("cron_schedules `{schedule_id}`: {e}"))?
        } else {
            existing
        };
        let Some(row) = row else {
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
            session: None,
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
        worktree_path: Option<&str>,
        skip_reason: Option<&str>,
        error: Option<&str>,
    ) -> Result<bool, String> {
        self.block(self.store.finish_firing(
            firing_id,
            store_state(state),
            finished_at.map(epoch_ms),
            worktree_path,
            skip_reason,
            error,
        ))
        .map_err(|e| format!("cron_firings `{firing_id}` settle: {e}"))
    }

    fn set_firing_session(&self, firing_id: &str, session: SessionId) -> Result<bool, String> {
        self.block(self.store.set_firing_session(firing_id, session))
            .map_err(|e| format!("cron_firings `{firing_id}` session record: {e}"))
    }

    fn settle_from_target(&self, schedule: &CronRuntimeSchedule) -> Result<(), String> {
        // The worktree arm settles from its synchronous outcome; only the
        // resident arm needs the session stream.
        if schedule.worktree.is_some() {
            return Ok(());
        }
        // The fallback for a firing row that recorded no session. A job target
        // has none: its run session is derived from the firing's own slot, so
        // such a row is left to the stranded grace path below.
        let fallback: Option<SessionId> = match target_session(schedule, None) {
            Ok(session) => Some(session),
            Err(_) if matches!(schedule.target, crate::config::CronTarget::Job { .. }) => None,
            // A target that cannot resolve was rejected at load; a
            // later regression settles nothing rather than mis-settling.
            Err(_) => return Ok(()),
        };
        {
            let mut sweeps = self
                .sweeps
                .lock()
                .map_err(|_| "cron sweep map poisoned".to_string())?;
            let now = Instant::now();
            if let Some(last) = sweeps.get(&schedule.id)
                && now.duration_since(*last) < Duration::from_secs(15)
            {
                return Ok(());
            }
            sweeps.insert(schedule.id.clone(), now);
        }
        // Every still-open firing of this schedule, wherever it sits in the
        // history (CRON-MODEL §9.1).
        let open = self
            .block(self.store.open_firings(&schedule.id))
            .map_err(|e| format!("cron_firings `{}`: {e}", schedule.id))?;
        if open.is_empty() {
            return Ok(());
        }
        let now_ms = epoch_ms(Utc::now());
        // The sweep may have several open firings for one schedule; replay each
        // distinct session's stream at most once per sweep.
        let mut streams: BTreeMap<SessionId, Vec<kian_core::events::Event>> = BTreeMap::new();
        for firing in open {
            // The settlement reads the session the tick recorded before
            // dispatch; pre-v8 rows have no record and fall back to the
            // configured target, where they were delivered.
            let session = match firing.session.as_deref() {
                Some(raw) => match uuid::Uuid::parse_str(raw) {
                    Ok(uuid) => SessionId(uuid),
                    Err(error) => {
                        if now_ms.saturating_sub(firing.admitted_at) >= STRANDED_GRACE_MS {
                            self.settle_stranded(
                                &firing.firing_id,
                                now_ms.max(firing.admitted_at),
                                &format!(
                                    "{TARGET_TERMINAL}: cron_firings.session `{raw}` is not a \
                                     session id ({error}); the firing cannot be settled"
                                ),
                            );
                        }
                        continue;
                    }
                },
                None => match fallback {
                    Some(session) => session,
                    None => continue,
                },
            };
            if let std::collections::btree_map::Entry::Vacant(slot) = streams.entry(session) {
                match self.block(self.store.rebuild(session)) {
                    Ok(events) => {
                        slot.insert(events);
                    }
                    // Unreadable stream: retry on the next sweep (never guess).
                    Err(_) => continue,
                }
            }
            let Some(admitted) = streams[&session].iter().find(|ev| {
                ev.kind == EventKind::InputAdmitted
                    && ev.bytes.pointer("/cron/firing_id").and_then(Value::as_str)
                        == Some(firing.firing_id.as_str())
            }) else {
                // Never consumed. It can still run only when its row is
                // pending in the inbox AND the session is not frozen. Anything
                // else (the admit never landed, or the freeze makes a turn
                // impossible) is a stranded firing — settled only after the
                // grace so a concurrently claiming replica's in-flight
                // dispatch is never mis-settled (CRON-MODEL §9.1).
                if now_ms.saturating_sub(firing.admitted_at) < STRANDED_GRACE_MS {
                    continue;
                }
                let frozen =
                    matches!(project(&streams[&session]), Ok(state) if state.is_terminal());
                let legacy = firing.session.is_none();
                // A pre-v8 row has no recorded session, so the configured target
                // may no longer be the session it was delivered to (a re-pointed
                // schedule resets the cursor but keeps history): never infer a
                // legacy row's fate from a live target. Only a frozen target is
                // proof it can never run (the pre-succession wedge).
                if legacy && !frozen {
                    continue;
                }
                let pending = match self.inbox_holds_firing(session, &firing.firing_id) {
                    Ok(pending) => pending,
                    // Cannot prove what happened: retry next sweep.
                    Err(_) => continue,
                };
                if pending && !frozen {
                    continue; // durable and still takeable
                }
                if !frozen {
                    // The admit may have landed between the two reads above:
                    // re-read before declaring the firing never-admitted.
                    let reread = match self.block(self.store.rebuild(session)) {
                        Ok(events) => events,
                        Err(_) => continue,
                    };
                    streams.insert(session, reread);
                    let admitted_now = streams[&session].iter().any(|ev| {
                        ev.kind == EventKind::InputAdmitted
                            && ev.bytes.pointer("/cron/firing_id").and_then(Value::as_str)
                                == Some(firing.firing_id.as_str())
                    });
                    if admitted_now {
                        continue; // next sweep settles from its end
                    }
                }
                let (finished_at, detail) = if frozen {
                    let what = if legacy {
                        "the target session is terminal"
                    } else {
                        "the recorded session is terminal"
                    };
                    (
                        streams[&session]
                            .last()
                            .map_or(0, |ev| ev.ts)
                            .max(firing.admitted_at),
                        format!("{what}; the firing was never consumed and can never run"),
                    )
                } else {
                    (
                        now_ms.max(firing.admitted_at),
                        "the firing was never admitted (claim recorded, no dispatch); \
                         it can never run"
                            .to_string(),
                    )
                };
                self.settle_stranded(
                    &firing.firing_id,
                    finished_at,
                    &format!("{TARGET_TERMINAL}: {detail}"),
                );
                continue;
            };
            // The firing's episode ends at the FIRST end after its admission
            // that is not the resumable `awaiting_input` end: the answer to
            // that ask resumes the same turn, so the sweep keeps scanning
            // forward across it. It must stop at the first *settling* end —
            // a later episode (a retry's new input) is not this firing's
            // outcome, and reading the last end would mis-attribute it.
            let Some(end_at) = streams[&session].iter().position(|ev| {
                ev.seq > admitted.seq
                    && matches!(
                        ev.kind,
                        EventKind::SessionEnded {
                            reason: r
                        } if r != EndReason::AwaitingInput && r != EndReason::AwaitingBudget
                    )
            }) else {
                continue;
            };
            let EventKind::SessionEnded { reason } = streams[&session][end_at].kind else {
                continue;
            };
            // (JOB-MODEL §5.5, §11.1, §18.2 amendment 1) A job run's end
            // reason is not its outcome: an `unverified` run ends `completed`
            // by construction (the work happened, the evidence did not), and a
            // failed step ends `failed` with the step named. The engine owns
            // the end write, so the conductor records the outcome as ONE
            // `evidence/verdict` with gate `run` before it yields its final
            // outcome — that verdict, at or before the settling end, is
            // authoritative for a job schedule. A non-job firing keeps the
            // bare-end mapping unchanged.
            let job_verdict = matches!(schedule.target, crate::config::CronTarget::Job { .. })
                .then(|| kian_superagent::job::status::run_verdict(&streams[&session][..=end_at]))
                .flatten();
            let (state, error) = if let Some(verdict) = job_verdict {
                match verdict.outcome {
                    kian_superagent::job::outcome::RunOutcome::Succeeded => {
                        (kian_store::FiringState::Succeeded, None)
                    }
                    kian_superagent::job::outcome::RunOutcome::Unverified => (
                        kian_store::FiringState::Failed,
                        Some(
                            "the run ended unverified: at least one step produced no evidence \
                             of success"
                                .to_string(),
                        ),
                    ),
                    kian_superagent::job::outcome::RunOutcome::Cancelled => (
                        kian_store::FiringState::Failed,
                        Some("cancelled".to_string()),
                    ),
                    kian_superagent::job::outcome::RunOutcome::Failed => (
                        kian_store::FiringState::Failed,
                        Some(job_failure_text(&verdict.payload["job"])),
                    ),
                }
            } else if matches!(schedule.target, crate::config::CronTarget::Job { .. }) {
                (
                    kian_store::FiringState::Failed,
                    Some(
                        "the run ended without an outcome (the conductor never wrote one)"
                            .to_string(),
                    ),
                )
            } else {
                match reason {
                    EndReason::Completed => (kian_store::FiringState::Succeeded, None),
                    EndReason::Failed => (
                        kian_store::FiringState::Failed,
                        Some("the scheduled turn failed".to_string()),
                    ),
                    EndReason::Interrupted => (
                        kian_store::FiringState::Failed,
                        Some("the scheduled turn was interrupted".to_string()),
                    ),
                    EndReason::Cancelled => (
                        kian_store::FiringState::Failed,
                        Some("cancelled".to_string()),
                    ),
                    // Excluded by the scan above; kept exhaustive.
                    EndReason::AwaitingInput | EndReason::AwaitingBudget => continue,
                    // A rotated resident firing never happens (rotation is a
                    // boot-time Router action, not a cron target); record an
                    // honest failure if it ever does.
                    EndReason::Rotated => (
                        kian_store::FiringState::Failed,
                        Some("the target session was rotated".to_string()),
                    ),
                }
            };
            if let Err(settle_error) = self.block(self.store.finish_firing(
                &firing.firing_id,
                state,
                Some(streams[&session][end_at].ts),
                None,
                None,
                error.as_deref(),
            )) {
                tracing::warn!(
                    firing_id = %firing.firing_id,
                    %settle_error,
                    "cron firing settlement failed; the next sweep retries"
                );
            }
        }
        Ok(())
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

    /// Plans this firing's deferral (ADR-060). The default is immediate: a
    /// fake sink or a worktree-only dispatcher never defers.
    fn plan_deferral(
        &self,
        policy: crate::defer::DeferPolicy,
    ) -> Result<crate::defer::DeferPlan, String> {
        Ok(crate::defer::DeferPlan::immediate(
            policy,
            0,
            "deferral not supported by this sink",
        ))
    }

    /// Delivers the firing at an explicit eligibility instant (ADR-060). The
    /// default ignores the instant and delivers now, so a fake sink keeps its
    /// old behavior and a plain deliver call site is unchanged.
    fn deliver_at(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
        _eligible_at_ms: Option<u64>,
    ) -> Result<(), String> {
        self.deliver(session, content, idempotency_key)
    }

    /// Refuses a job run whose derived session already names another stream.
    fn ensure_job_identity(&self, _session: SessionId, _marker: &Value) -> Result<(), String> {
        Ok(())
    }

    /// The default `overlap: skip` judgement (CRON-MODEL §5.1): a firing
    /// must not pile onto a session that is still working. `Ok(Some(reason))`
    /// records the slot as `skipped_overlap`; `Err` is a real failure
    /// (fail-closed: unattended work does not start when "is it busy?"
    /// cannot be answered).
    fn should_skip(&self, _session: SessionId) -> Result<Option<String>, String> {
        Ok(None)
    }

    /// The session this firing runs in (CRON-MODEL §3.2): the configured
    /// target while it is empty/live, or its deterministic firing session when
    /// the target is frozen. Called before `deliver`, and its answer is
    /// recorded on the firing row before the admit. The default keeps the
    /// target (a fake sink); `Err` = the read failed (fail-closed).
    fn resolve(&self, session: SessionId, _firing_id: &str) -> Result<SessionId, String> {
        Ok(session)
    }
}

/// The resident-engine turn sink the cron dispatcher writes through: one
/// durable inbox row for the schedule's target session (the engine folds the
/// wake itself). Blocking on the serve runtime's handle is safe here because the
/// cron ticker runs on its own OS thread, never on a runtime worker.
///
/// [`EngineTurn::resolve`] replaces a **frozen** (terminal) target with its
/// deterministic firing session before anything is written: a frozen stream can
/// never consume another row (the drain projects terminal and appends nothing,
/// DESIGN §2.3/§4.6), so the firing runs in a fresh per-slot session instead
/// (CRON-MODEL §3.2). `deliver` still fails closed with the [`TARGET_TERMINAL`]
/// prefix if the *resolved* session turns out terminal — a race, never the
/// normal path.
///
/// Generic over the loop driver so the sink is testable with a scripted driver;
/// assembly arms it with `ServeLoopDriver`.
pub struct EngineTurn<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    engine: Arc<Engine<D>>,
    handle: tokio::runtime::Handle,
    /// ADR-038: the session bootstrap runs before the firing row, so a
    /// fresh cron target's first context event is the memory index.
    bootstrap: Option<Arc<dyn SessionBootstrap>>,
    /// ADR-060: the off-peak planner, armed only when the serve config
    /// declares a defer: section. None keeps every firing immediate.
    deferral: Option<crate::defer::DeferCalendar>,
    /// The job identity preflight reads the derived session's committed
    /// stream. Absent in unit fakes; production arms it at assembly.
    store: Option<EventStore>,
}

impl<D> EngineTurn<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    /// A sink over `engine`, driving futures on `handle` (the serve runtime).
    pub fn new(
        engine: Arc<Engine<D>>,
        handle: tokio::runtime::Handle,
        bootstrap: Option<Arc<dyn SessionBootstrap>>,
    ) -> Self {
        Self {
            engine,
            handle,
            bootstrap,
            deferral: None,
            store: None,
        }
    }

    /// Arms the job identity preflight with the store the scheduler admits
    /// through.
    #[must_use]
    pub fn with_store(mut self, store: EventStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Arms the off-peak planner (ADR-060). Without it every firing is
    /// immediate, and config validation refused any schedule that declared
    /// defer: off_peak without a defer: section.
    #[must_use]
    pub fn with_deferral(mut self, calendar: crate::defer::DeferCalendar) -> Self {
        self.deferral = Some(calendar);
        self
    }

    /// The one admit path shared by deliver (immediate) and deliver_at
    /// (deferred): the terminal/overlap/bootstrap checks run identically and
    /// only the eligibility instant of the durable row differs (ADR-060).
    fn admit(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
        eligible_at_ms: Option<u64>,
    ) -> Result<(), String> {
        self.handle.block_on(async {
            if let Some(state) = self
                .engine
                .terminal_state(session)
                .await
                .map_err(|error| format!("target state read: {error}"))?
            {
                return Err(format!(
                    "{TARGET_TERMINAL}: resolved session {} is terminal ({state:?}); it froze \
                     before the admit (a race — the next slot derives its own firing session)",
                    session.0
                ));
            }
            // The overlap judgement ran before this sink was reached (and
            // before the session was recorded), but resolution + the durable
            // session record widen that window. Re-check at the admit boundary:
            // a drain that started meanwhile must not get a queued duplicate
            // (CRON-MODEL §5.1). A trip here settles the slot skipped_overlap
            // and keeps the recorded session as "the session it tried".
            if self
                .engine
                .has_open_work(session)
                .await
                .map_err(|error| format!("overlap re-check: {error}"))?
            {
                return Err(format!(
                    "skipped_overlap: session {} became busy before the admit",
                    session.0
                ));
            }
            let job_row = content.get(kian_superagent::job::status::JOB_KEY).is_some();
            if !job_row
                && let Some(bootstrap) = &self.bootstrap
                && let Err(error) = bootstrap.bootstrap(session).await
            {
                tracing::warn!(
                    session = %session.0,
                    %error,
                    "cron target session bootstrap skipped"
                );
            }
            let admitted = match eligible_at_ms {
                Some(at) => {
                    self.engine
                        .admit_at(session, content, idempotency_key, at)
                        .await
                }
                None => self.engine.admit(session, content, idempotency_key).await,
            };
            admitted.map(|_| ()).map_err(|e| format!("admit: {e}"))
        })
    }
}

impl<D> ResidentTurn for EngineTurn<D>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    fn deliver(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> Result<(), String> {
        self.admit(session, content, idempotency_key, None)
    }

    fn plan_deferral(
        &self,
        policy: crate::defer::DeferPolicy,
    ) -> Result<crate::defer::DeferPlan, String> {
        let now_ms = self.engine.now_ms();
        match &self.deferral {
            Some(calendar) => Ok(calendar.plan(policy, now_ms)),
            None => Ok(crate::defer::DeferPlan::immediate(
                policy,
                now_ms,
                "deferral is not configured",
            )),
        }
    }

    fn deliver_at(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
        eligible_at_ms: Option<u64>,
    ) -> Result<(), String> {
        self.admit(session, content, idempotency_key, eligible_at_ms)
    }

    fn ensure_job_identity(&self, session: SessionId, marker: &Value) -> Result<(), String> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let expected = read_job_marker(marker)?;
        self.handle.block_on(async {
            let events = store
                .rebuild(session)
                .await
                .map_err(|error| format!("run identity read: {error}"))?;
            if events.is_empty() {
                let rows = store
                    .input_rows(session)
                    .await
                    .map_err(|error| format!("run identity inbox read: {error}"))?;
                let Some(row) = rows.first() else {
                    return Ok(());
                };
                let Some(existing) = row.payload.get(kian_superagent::job::status::JOB_KEY) else {
                    return Err(run_identity_conflict(session));
                };
                return same_job_identity(existing, &expected, session);
            }
            match kian_superagent::job::status::marker(&events) {
                Some(Ok(existing)) if existing.same_frozen_identity(&expected) => Ok(()),
                Some(Ok(_)) => Err(run_identity_conflict(session)),
                Some(Err(error)) => Err(format!(
                    "run_identity_conflict: derived session {} carries an invalid job marker: {error}",
                    session.0
                )),
                None => Err(run_identity_conflict(session)),
            }
        })
    }

    fn should_skip(&self, session: SessionId) -> Result<Option<String>, String> {
        self.handle
            .block_on(async {
                self.engine
                    .has_open_work(session)
                    .await
                    .map(|busy| busy.then(|| format!("session {} still has open work", session.0)))
            })
            .map_err(|e| format!("overlap check: {e}"))
    }

    fn resolve(&self, session: SessionId, firing_id: &str) -> Result<SessionId, String> {
        self.handle.block_on(async {
            match self.engine.terminal_state(session).await {
                Ok(None) => Ok(session),
                Ok(Some(_)) => Ok(firing_session(session, firing_id)),
                Err(error) => Err(format!("target state read: {error}")),
            }
        })
    }
}

/// The deterministic resident session of a channel target.
///
/// A `{channel, topic}` schedule has no session id in its declaration, and the
/// resident engine addresses turns by session. The mapping is therefore a
/// stable hash of `cron:<channel>/<topic>`: firings target the same session
/// (one ongoing conversation, like a real channel thread) and a restart
/// re-derives the same id — no registry needed. Once that session freezes, the
/// per-slot firing session takes over (CRON-MODEL §3.2).
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

/// The slot token of a firing id `{schedule_id}@{slot}` (CRON-MODEL §4.1).
///
/// The slot is what a job run is addressed by (`design/JOB-MODEL.md` §6): one
/// firing, one slot, one run session — a re-derivation never moves it.
pub fn slot_of_firing(firing_id: &str) -> Result<&str, String> {
    let (_, slot) = firing_id
        .split_once('@')
        .ok_or_else(|| format!("firing id `{firing_id}` carries no slot suffix"))?;
    if slot.is_empty() {
        return Err(format!("firing id `{firing_id}` carries an empty slot"));
    }
    Ok(slot)
}

fn read_job_marker(marker: &Value) -> Result<kian_superagent::job::status::JobMarker, String> {
    kian_superagent::job::status::parse_marker(marker)
        .map_err(|error| format!("invalid job marker: {error}"))
}

fn same_job_identity(
    existing: &Value,
    expected: &kian_superagent::job::status::JobMarker,
    session: SessionId,
) -> Result<(), String> {
    let existing = read_job_marker(existing)?;
    if existing.same_frozen_identity(expected) {
        Ok(())
    } else {
        Err(run_identity_conflict(session))
    }
}

fn run_identity_conflict(session: SessionId) -> String {
    format!(
        "run_identity_conflict: derived session {} already exists without the same job marker",
        session.0
    )
}

/// The operator-facing reason a job run failed (JOB-MODEL §5.5).
///
/// The conductor records the failing step and the verdict's own words under
/// the additive `job` key, so the firing row names the step instead of saying
/// only "the scheduled turn failed". Absent keys degrade to what is known —
/// never to a fabricated detail.
fn job_failure_text(job: &serde_json::Value) -> String {
    let step = job.get("failed_step").and_then(|value| value.as_str());
    let detail = job.get("reason").and_then(|value| value.as_str());
    match (step, detail) {
        (Some(step), Some(detail)) => format!("step `{step}` failed: {detail}"),
        (Some(step), None) => format!("step `{step}` failed"),
        (None, Some(detail)) => detail.to_string(),
        (None, None) => "the run failed".to_string(),
    }
}

/// The session a schedule's firing belongs to (CRON-MODEL §3.2).
///
/// A job target's session is derived from the job id and the firing's slot
/// (`design/JOB-MODEL.md` §6), so it needs the firing id; the other two shapes
/// do not.
pub fn target_session(
    schedule: &CronRuntimeSchedule,
    firing_id: Option<&str>,
) -> Result<SessionId, String> {
    match &schedule.target {
        crate::config::CronTarget::Session { session } => uuid::Uuid::parse_str(session)
            .map(SessionId)
            .map_err(|e| format!("target.session `{session}` is not a uuid: {e}")),
        crate::config::CronTarget::Channel { channel, topic } => {
            Ok(channel_session(channel, topic))
        }
        crate::config::CronTarget::Job { job } => {
            let firing_id = firing_id.ok_or_else(|| {
                format!("job target `{job}` needs a firing id to derive its slot")
            })?;
            let slot = slot_of_firing(firing_id)?;
            Ok(kian_superagent::job::plan::run_session(job, slot))
        }
    }
}

/// The resident-engine [`CronDispatcher`]: admitted slots become durable inbox
/// rows on the session resolution picks (the configured target while it is
/// live/empty, else that slot's firing session — CRON-MODEL §3.2), which the
/// already-resident engine drains.
pub struct ResidentDispatcher {
    turn: Arc<dyn ResidentTurn>,
    /// The declared jobs a `CronTarget::Job` firing admits from
    /// (`design/JOB-MODEL.md` §7.1a). Absent = no job target can fire.
    jobs: Option<Arc<crate::job_config::JobCatalog>>,
}

impl ResidentDispatcher {
    /// A dispatcher over the injected resident turn sink.
    pub fn new(turn: Arc<dyn ResidentTurn>) -> Self {
        Self { turn, jobs: None }
    }

    /// Mounts the job catalog (`design/JOB-MODEL.md` §7.1a).
    #[must_use]
    pub fn with_jobs(mut self, catalog: Arc<crate::job_config::JobCatalog>) -> Self {
        self.jobs = Some(catalog);
        self
    }
}

impl CronDispatcher for ResidentDispatcher {
    fn resolve_session(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
    ) -> Result<Option<SessionId>, String> {
        let target = target_session(schedule, Some(firing_id))?;
        // Resolve first (§3.2): a live/empty target is delivered to as before;
        // a frozen one becomes this slot's deterministic firing session. A job
        // run is already slot-addressed (`design/JOB-MODEL.md` §6): a terminal
        // run is a settled slot, so there is nothing to fall back to.
        let session = match &schedule.target {
            crate::config::CronTarget::Job { .. } => target,
            _ => self.turn.resolve(target, firing_id)?,
        };
        // CRON-MODEL §5.1's default `skip`: an overlapping firing is a
        // recorded skip, never a queued duplicate. The judgement is a pure
        // projection of the **resolved** session (`Engine::has_open_work`); it
        // runs before the session is recorded, so a skipped slot never claims
        // a session it did not run in.
        if let Some(reason) = self.turn.should_skip(session)? {
            return Err(format!("skipped_overlap: {reason}"));
        }
        Ok(Some(session))
    }

    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
        session: Option<SessionId>,
    ) -> Result<(), String> {
        // The tick recorded this exact session before calling us; never
        // re-resolve (a second resolution could disagree after a race).
        let Some(session) = session else {
            return Err("resident arm dispatched without a resolved session".to_string());
        };
        let cron: Value = serde_json::from_str(payload)
            .map_err(|e| format!("dispatch payload is not JSON: {e}"))?;
        // content is the turn's user message; cron carries the routing proof
        // (schedule_id / firing_id / fire_time / target) as an additive
        // non-context key, so the model request is the prompt alone.
        if let crate::config::CronTarget::Job { job } = &schedule.target {
            // A job firing admits a **run**: the same input row, plus the
            // frozen plan under the additive `job` key and the run-scoped
            // idempotency key (`design/JOB-MODEL.md` §5.2). The conductor
            // derives everything from that key, never from the content line.
            let slot = slot_of_firing(firing_id)?;
            let catalog = self
                .jobs
                .as_ref()
                .ok_or_else(|| format!("job target `{job}` fired without a mounted job catalog"))?;
            let admission = catalog
                .admission(job, slot, crate::job_config::TRIGGER_CRON)
                .ok_or_else(|| format!("job `{job}` is not declared in the mounted catalog"))?;
            let content = serde_json::json!({
                "content": admission.content,
                "cron": cron,
                "job": admission.marker,
            });
            self.turn.ensure_job_identity(session, &content["job"])?;
            return self.turn.deliver(session, content, Some(admission.key));
        }
        let mut content = serde_json::json!({ "content": schedule.prompt, "cron": cron });
        // ADR-060: a schedule that declared defer: off_peak admits its durable
        // row with a future eligibility instant instead of dropping it or
        // delaying outside the event log. The decision rides on the row payload
        // as an additive non-context key (derive_model_context keeps only
        // content), so an operator can answer why it did not run now. An
        // immediate firing keeps the exact bytes it had before this change.
        if schedule.defer == crate::defer::DeferPolicy::OffPeak {
            let plan = self.turn.plan_deferral(schedule.defer)?;
            if let Some(object) = content.as_object_mut() {
                object.insert("defer".to_string(), serde_json::json!(plan));
            }
            return self.turn.deliver_at(
                session,
                content,
                Some(format!("cron:{firing_id}")),
                plan.deferred.then_some(plan.eligible_at_ms),
            );
        }
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
    /// Per-schedule isolated arms: a schedule with its own
    /// `worktree.repo` gets its own dispatcher (CRON-MODEL §6, one repo
    /// per declaration), keyed by schedule id.
    per_schedule: BTreeMap<String, Arc<WorktreeDispatcher>>,
    /// Run worktree turns off the ticker thread (production): the tick
    /// only claims and hands off, so one long turn cannot stall every
    /// other schedule (CRON-MODEL §5.4/§7.3).
    async_worktree: bool,
    /// The §5.4 global ceiling: concurrent worktree turns across all
    /// schedules. Held by a spawned thread for the turn's duration.
    global: Arc<GlobalCap>,
}

/// The process-wide worktree concurrency ceiling (§5.4). A slot is taken
/// only when a turn is *spawned* and released when its thread finishes;
/// an over-ceiling slot is a recorded `skipped_overlap`, never a burst.
struct GlobalCap {
    cap: u32,
    running: Mutex<u32>,
}

/// One held global slot; released on drop (the spawned turn's end).
struct GlobalSlot {
    cap: Arc<GlobalCap>,
}

impl Drop for GlobalSlot {
    fn drop(&mut self) {
        if let Ok(mut running) = self.cap.running.lock() {
            *running = running.saturating_sub(1);
        }
    }
}

impl ScheduleDispatcher {
    /// A dispatcher over the two execution paths (either may be absent: an
    /// absent arm makes a schedule that selects it fail loudly).
    pub fn new(
        resident: Option<Arc<dyn CronDispatcher + Send + Sync>>,
        worktree: Option<Arc<WorktreeDispatcher>>,
    ) -> Self {
        Self {
            resident,
            worktree,
            per_schedule: BTreeMap::new(),
            async_worktree: false,
            global: Arc::new(GlobalCap {
                cap: 4,
                running: Mutex::new(0),
            }),
        }
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

    /// Add one schedule's isolated arm (its own `worktree.repo`).
    pub fn with_schedule_worktree(
        mut self,
        schedule_id: impl Into<String>,
        worktree: Arc<WorktreeDispatcher>,
    ) -> Self {
        self.per_schedule.insert(schedule_id.into(), worktree);
        self
    }

    /// Run worktree turns on their own threads, bounded by `cap`
    /// (CRON-MODEL §5.4). The tick then only claims and hands off.
    pub fn with_async_worktree(mut self, cap: u32) -> Self {
        self.async_worktree = true;
        self.global = Arc::new(GlobalCap {
            cap: cap.max(1),
            running: Mutex::new(0),
        });
        self
    }

    /// Try to take one global worktree slot (§5.4); `None` = over the
    /// ceiling.
    fn try_global_slot(&self) -> Option<GlobalSlot> {
        let mut running = self.global.running.lock().ok()?;
        if *running >= self.global.cap {
            return None;
        }
        *running += 1;
        Some(GlobalSlot {
            cap: Arc::clone(&self.global),
        })
    }

    /// Which arm `schedule` selects.
    pub fn selects_worktree(schedule: &CronRuntimeSchedule) -> bool {
        schedule.worktree.is_some()
    }
}

impl CronDispatcher for ScheduleDispatcher {
    fn resolve_session(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
    ) -> Result<Option<SessionId>, String> {
        if Self::selects_worktree(schedule) {
            // The worktree arm runs the turn outside any session.
            return Ok(None);
        }
        match self.resident.as_ref() {
            Some(resident) => resident.resolve_session(schedule, firing_id),
            None => Err(format!(
                "schedule `{}` selects the resident engine but no resident dispatcher is armed",
                schedule.id
            )),
        }
    }

    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
        session: Option<SessionId>,
    ) -> Result<(), String> {
        if Self::selects_worktree(schedule) {
            let isolated = self
                .per_schedule
                .get(&schedule.id)
                .or(self.worktree.as_ref());
            let Some(worktree) = isolated else {
                return Err(format!(
                    "schedule `{}` asks for worktree isolation but no worktree dispatcher is \
                     armed (no `worktree.repo` resolved)",
                    schedule.id
                ));
            };
            if !self.async_worktree {
                return worktree.dispatch(schedule, firing_id, payload, None);
            }
            // Production: hand the turn to its own thread so the ticker
            // keeps ticking (CRON-MODEL §5.4/§7.3). The global slot is
            // held for the turn's whole lifecycle (run + capture +
            // cleanup) and released by `GlobalSlot::drop`.
            let Some(slot) = self.try_global_slot() else {
                return Err(format!(
                    "skipped_overlap: global concurrency ceiling {} reached (CRON-MODEL §5.4)",
                    self.global.cap
                ));
            };
            let worktree = Arc::clone(worktree);
            let schedule = schedule.clone();
            let firing_id = firing_id.to_owned();
            let payload = payload.to_owned();
            return std::thread::Builder::new()
                .name("kian-cron-turn".to_string())
                .spawn(move || {
                    let _slot = slot;
                    let _ = worktree.dispatch(&schedule, &firing_id, &payload, None);
                })
                .map(|_| ())
                .map_err(|error| format!("cannot spawn the cron turn: {error}"));
        }
        match self.resident.as_ref() {
            Some(resident) => resident.dispatch(schedule, firing_id, payload, session),
            None => Err(format!(
                "schedule `{}` selects the resident engine but no resident dispatcher is armed",
                schedule.id
            )),
        }
    }

    fn take_outcome(&self, firing_id: &str) -> Option<DispatchOutcome> {
        self.per_schedule
            .values()
            .find_map(|worktree| worktree.take_outcome(firing_id))
            .or_else(|| {
                self.worktree
                    .as_ref()
                    .and_then(|worktree| worktree.take_outcome(firing_id))
            })
    }

    fn drain_settled(&self) -> Vec<(String, DispatchOutcome)> {
        let mut settled = Vec::new();
        for worktree in self.per_schedule.values() {
            settled.extend(worktree.drain_settled());
        }
        if let Some(worktree) = self.worktree.as_ref() {
            settled.extend(worktree.drain_settled());
        }
        settled
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

/// One `cron/skipped` audit line (CRON-MODEL §9.2): a slot consumed
/// without a turn, naming the decisive reason.
fn skip_event(entry: &Admitted, now: DateTime<Utc>, reason: &str) -> Value {
    serde_json::json!({
        "event": "cron/skipped",
        "at": now.to_rfc3339(),
        "schedule_id": entry.schedule_id,
        "firing_id": entry.firing_id,
        "fire_time": entry.fire_time.to_rfc3339(),
        "reason": reason,
    })
}

/// Epoch milliseconds as a `DateTime<Utc>` (the store's settlement unit).
fn from_epoch_ms(ms: u64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(ms).ok()?)
}

/// Settle an admitted slot whose dispatch could not start — the session
/// resolution failed, the session record could not be written, or the executor
/// refused. Mirrors the dispatch-error path exactly, so the firing report and
/// `cron/skipped` line are identical whichever pre-step refused.
fn settle_dispatch_failure(
    store: &dyn CronTickStore,
    dispatcher: &dyn CronDispatcher,
    entry: &Admitted,
    now: DateTime<Utc>,
    error: &str,
    events: &mut Vec<Value>,
    failed: &mut Vec<Failed>,
) {
    let outcome = dispatcher.take_outcome(&entry.firing_id);
    let token = guard_skip_token(error);
    let state = outcome.as_ref().map_or_else(
        || {
            if token.is_some() {
                FiringState::Skipped
            } else {
                FiringState::Failed
            }
        },
        |settled| settled.state,
    );
    let error_text = outcome
        .as_ref()
        .and_then(|settled| settled.error.clone())
        .unwrap_or_else(|| error.to_owned());
    let skip_reason = outcome
        .as_ref()
        .and_then(|settled| settled.skip_reason.clone())
        .or_else(|| token.map(str::to_owned));
    let worktree_path = outcome
        .as_ref()
        .and_then(|settled| settled.worktree_path.as_deref());
    let _ = store.finish_firing(
        &entry.firing_id,
        state,
        Some(now),
        worktree_path,
        skip_reason.as_deref(),
        Some(&error_text),
    );
    if state == FiringState::Skipped {
        events.push(skip_event(
            entry,
            now,
            skip_reason.as_deref().unwrap_or("skipped"),
        ));
    } else {
        failed.push(Failed {
            schedule_id: entry.schedule_id.clone(),
            firing_id: entry.firing_id.clone(),
            fire_time: entry.fire_time,
            error: error_text,
        });
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
    // Settle the outcomes the asynchronous worktree turns produced since
    // the last tick (CRON-MODEL §5.4/§9.1). The firing row is the record;
    // no extra log line is invented for a settlement.
    for (firing_id, settled) in dispatcher.drain_settled() {
        let finished = settled.finished_at_ms.and_then(from_epoch_ms).or(Some(now));
        let _ = store.finish_firing(
            &firing_id,
            settled.state,
            finished,
            settled.worktree_path.as_deref(),
            settled.skip_reason.as_deref(),
            settled.error.as_deref(),
        );
    }
    let mut admitted: Vec<Admitted> = Vec::with_capacity(report.admitted.len());
    for entry in report.admitted.drain(..) {
        let Some(schedule) = schedules.iter().find(|s| s.id == entry.schedule_id) else {
            admitted.push(entry);
            continue;
        };
        // Resolve the session this firing runs in and record it **before**
        // dispatch (CRON-MODEL §3.2/§9.1): the settlement sweep on another
        // replica must read where the firing runs, never infer it from the
        // configured target. A refusal here settles the slot like a dispatch
        // failure (a `skipped_overlap` resolution settles it skipped).
        let resolved = match dispatcher.resolve_session(schedule, &entry.firing_id) {
            Ok(resolved) => resolved,
            Err(error) => {
                settle_dispatch_failure(
                    store,
                    dispatcher,
                    &entry,
                    now,
                    &error,
                    &mut events,
                    &mut report.failed,
                );
                continue;
            }
        };
        if let Some(session) = resolved {
            match store.set_firing_session(&entry.firing_id, session) {
                Ok(true) => {}
                // The row settled between claim and dispatch (a sweep on
                // another replica won the race): delivering would run a turn
                // recorded against a settled firing, so refuse.
                Ok(false) => {
                    settle_dispatch_failure(
                        store,
                        dispatcher,
                        &entry,
                        now,
                        "the firing row is no longer open (settled concurrently)",
                        &mut events,
                        &mut report.failed,
                    );
                    continue;
                }
                Err(error) => {
                    settle_dispatch_failure(
                        store,
                        dispatcher,
                        &entry,
                        now,
                        &format!("record firing session: {error}"),
                        &mut events,
                        &mut report.failed,
                    );
                    continue;
                }
            }
        }
        // The worktree arm may run the turn synchronously (tests,
        // `run-now`) and know the terminal outcome immediately; the async
        // production arm records it later and the drain above settles it.
        // A known outcome is persisted on the firing row so
        // `cron list`/`cron logs` answer "did it run" — CRON-MODEL §9.1.
        match dispatcher.dispatch(schedule, &entry.firing_id, &entry.payload, resolved) {
            Ok(()) => match dispatcher.take_outcome(&entry.firing_id) {
                Some(settled) => {
                    let _ = store.finish_firing(
                        &entry.firing_id,
                        settled.state,
                        settled.finished_at_ms.and_then(from_epoch_ms).or(Some(now)),
                        settled.worktree_path.as_deref(),
                        settled.skip_reason.as_deref(),
                        settled.error.as_deref(),
                    );
                    if settled.state == FiringState::Skipped {
                        events.push(skip_event(
                            &entry,
                            now,
                            settled.skip_reason.as_deref().unwrap_or("skipped"),
                        ));
                    } else {
                        admitted.push(entry);
                    }
                }
                None => admitted.push(entry),
            },
            Err(error) => {
                settle_dispatch_failure(
                    store,
                    dispatcher,
                    &entry,
                    now,
                    &error,
                    &mut events,
                    &mut report.failed,
                );
            }
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
    use kian_core::events::{Event, SCHEMA_VERSION};
    use kian_core::model::{Epoch, InputRow};
    use kian_core::seams::{LoopDriver, LoopOutcome};
    use kian_core::state::SessionState;
    use kian_orchestrator::{DrainCtx, Engine, EngineConfig};
    use kian_store::AppendGuard;
    use kian_superagent::SessionBootstrap;

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    fn schedule_yaml(id: &str, extra: &str) -> crate::config::CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    max_concurrent: 1\n{extra}    target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit .state/\n"
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

    /// A schedule that fires into one fixed session (`target: {session}`),
    /// the shape the frozen-target tests need.
    fn session_schedule(id: &str, session: SessionId) -> crate::config::CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    target: {{ session: \"{}\" }}\n    prompt: audit\n",
            session.0
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the session-target fixture parses");
        section
            .to_runtime(&channels(), Utc::now())
            .expect("the session-target fixture resolves")
            .remove(0)
    }

    fn cron_event(sid: SessionId, seq: u64, epoch: Epoch, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: sid,
            epoch,
            ts: 1_800_000_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    /// A minimal **valid** frozen stream: created → claimed → ended with
    /// `reason`. `project` must see a terminal state here — the sink/sweep
    /// guards read the projection, not just the tail kind.
    async fn seed_frozen_session(store: &EventStore, sid: SessionId, reason: EndReason) {
        append_created(store, sid).await;
        append_claim(store, sid, 2, Epoch(1)).await;
        append_end(store, sid, 3, Epoch(1), reason).await;
    }

    async fn append_created(store: &EventStore, sid: SessionId) {
        store
            .append(
                sid,
                cron_event(
                    sid,
                    1,
                    Epoch::GENESIS,
                    EventKind::SessionCreated,
                    serde_json::json!({}),
                ),
                AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: 1,
                },
            )
            .await
            .expect("session/created");
    }

    async fn append_claim(store: &EventStore, sid: SessionId, seq: u64, epoch: Epoch) {
        store
            .append(
                sid,
                cron_event(
                    sid,
                    seq,
                    epoch,
                    EventKind::DrainClaimed,
                    serde_json::json!({"holder": "cron-test", "epoch": epoch.0}),
                ),
                AppendGuard {
                    epoch,
                    expected_seq: seq,
                },
            )
            .await
            .expect("drain/claimed");
    }

    async fn append_end(
        store: &EventStore,
        sid: SessionId,
        seq: u64,
        epoch: Epoch,
        reason: EndReason,
    ) {
        store
            .append_end(
                sid,
                cron_event(
                    sid,
                    seq,
                    epoch,
                    EventKind::SessionEnded { reason },
                    serde_json::json!({}),
                ),
                AppendGuard {
                    epoch,
                    expected_seq: seq,
                },
            )
            .await
            .expect("session/ended");
    }

    async fn append_firing_input(
        store: &EventStore,
        sid: SessionId,
        seq: u64,
        epoch: Epoch,
        schedule_id: &str,
        firing_id: &str,
    ) {
        store
            .append(
                sid,
                cron_event(
                    sid,
                    seq,
                    epoch,
                    EventKind::InputAdmitted,
                    serde_json::json!({"role": "user", "content": "audit",
                                       "cron": {"firing_id": firing_id, "schedule_id": schedule_id}}),
                ),
                AppendGuard {
                    epoch,
                    expected_seq: seq,
                },
            )
            .await
            .expect("input/admitted");
    }

    /// Claim one firing row (`{schedule_id}@{fire_time}`) in `admitted`,
    /// stamped far enough in the past for the stranded grace to have elapsed
    /// (the legacy / crash shapes).
    async fn claim_firing_row(store: &EventStore, schedule_id: &str, fire_time: &str) {
        claim_firing_row_at(store, schedule_id, fire_time, 1_700_000_000_000).await;
    }

    async fn claim_firing_row_at(
        store: &EventStore,
        schedule_id: &str,
        fire_time: &str,
        admitted_at: u64,
    ) {
        let firing_id = format!("{schedule_id}@{fire_time}");
        assert!(
            store
                .claim_firing(&kian_store::CronFiringRow {
                    firing_id,
                    schedule_id: schedule_id.to_string(),
                    fire_time: fire_time.to_string(),
                    session: None,
                    admitted_at,
                    state: kian_store::FiringState::Admitted,
                    skip_reason: None,
                    worktree_path: None,
                    finished_at: None,
                    error: None,
                })
                .await
                .expect("claim firing")
        );
    }

    /// A multi-thread runtime: the resident sink's `Handle::block_on` runs the
    /// admit while spawned drains execute on workers (production shape).
    fn multi_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime")
    }

    /// Drive the runtime until `sid` projects `want` (a spawned drain ran).
    fn wait_for_state(
        rt: &tokio::runtime::Runtime,
        store: &EventStore,
        sid: SessionId,
        want: SessionState,
    ) -> bool {
        rt.block_on(async {
            for _ in 0..500 {
                let events = store.rebuild(sid).await.expect("rebuild");
                if !events.is_empty() && project(&events).expect("project") == want {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            false
        })
    }

    /// The sink tests need an engine, not a provider: any drain the engine
    /// spawns settles immediately (the session ends completed).
    struct SettlingDriver;

    impl LoopDriver for SettlingDriver {
        type Ctx = DrainCtx;

        fn drive(&self, _ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
            use futures::StreamExt;
            futures::stream::iter(vec![LoopOutcome::Settled]).boxed()
        }
    }

    /// Counts bootstrap calls: the frozen-target refusal must run *before* the
    /// ADR-038 bootstrap (a bootstrap admit onto a frozen stream would strand
    /// yet another row).
    #[derive(Default)]
    struct CountingBootstrap {
        calls: AtomicUsize,
    }

    impl SessionBootstrap for CountingBootstrap {
        fn bootstrap(
            &self,
            _session: SessionId,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    /// A bootstrap that records whether the target's inbox already held a row
    /// when it ran: ADR-038 requires bootstrap-before-admit.
    struct ObservingBootstrap {
        store: EventStore,
        calls: Arc<AtomicUsize>,
        saw_row: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SessionBootstrap for ObservingBootstrap {
        fn bootstrap(
            &self,
            session: SessionId,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            let store = self.store.clone();
            let calls = Arc::clone(&self.calls);
            let saw_row = Arc::clone(&self.saw_row);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if let Ok(rows) = store.input_rows(session).await {
                    saw_row.store(!rows.is_empty(), Ordering::SeqCst);
                }
                Ok(())
            })
        }
    }

    fn engine_for(store: &EventStore) -> Arc<Engine<SettlingDriver>> {
        Engine::new(
            store.clone(),
            Arc::new(SettlingDriver),
            EngineConfig::default(),
        )
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

    /// A resident sink that records the deferred admission path (ADR-060).
    #[derive(Default)]
    struct DeferResident {
        plans: AtomicUsize,
        deliveries: Mutex<Vec<(SessionId, Value, Option<u64>)>>,
    }

    impl ResidentTurn for DeferResident {
        fn deliver(
            &self,
            session: SessionId,
            content: Value,
            _idempotency_key: Option<String>,
        ) -> Result<(), String> {
            if let Ok(mut seen) = self.deliveries.lock() {
                seen.push((session, content, None));
            }
            Ok(())
        }

        fn plan_deferral(
            &self,
            policy: crate::defer::DeferPolicy,
        ) -> Result<crate::defer::DeferPlan, String> {
            self.plans.fetch_add(1, Ordering::SeqCst);
            Ok(crate::defer::DeferPlan {
                policy,
                eligible_at_ms: 1_800_000_600_000,
                deferred: true,
                reason: "scheduled off peak".to_string(),
            })
        }

        fn deliver_at(
            &self,
            session: SessionId,
            content: Value,
            _idempotency_key: Option<String>,
            eligible_at_ms: Option<u64>,
        ) -> Result<(), String> {
            if let Ok(mut seen) = self.deliveries.lock() {
                seen.push((session, content, eligible_at_ms));
            }
            Ok(())
        }
    }

    fn dispatch_at(
        schedule: &crate::config::CronRuntimeSchedule,
        dispatcher: &ScheduleDispatcher,
        session: SessionId,
    ) {
        let fire_time = DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc);
        let firing_id = kian_cron::firing_id(&schedule.id, fire_time);
        let payload = cron_tick::dispatch_payload(schedule, &firing_id, fire_time);
        dispatcher
            .dispatch(schedule, &firing_id, &payload, Some(session))
            .expect("the resident arm accepts the firing");
    }

    #[test]
    fn an_off_peak_schedule_defers_through_deliver_at_and_records_the_decision() {
        let fake = Arc::new(DeferResident::default());
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(fake.clone())));
        let schedule = schedule_yaml(
            "audit",
            "    defer: off_peak
",
        );
        let session = SessionId(uuid::Uuid::new_v4());

        dispatch_at(&schedule, &dispatcher, session);

        assert_eq!(
            fake.plans.load(Ordering::SeqCst),
            1,
            "the planner ran exactly once for the deferred firing"
        );
        let seen = fake.deliveries.lock().expect("deliveries");
        assert_eq!(seen.len(), 1);
        let (seen_session, content, eligible_at) = &seen[0];
        assert_eq!(*seen_session, session);
        assert_eq!(
            *eligible_at,
            Some(1_800_000_600_000),
            "the sink admits at the planned eligibility instant"
        );
        let decision = content
            .get("defer")
            .expect("the decision rides the row payload as a non-context key");
        assert_eq!(decision["policy"], "off_peak");
        assert_eq!(decision["deferred"], true);
        assert_eq!(decision["eligible_at_ms"], 1_800_000_600_000_u64);
        assert_eq!(decision["reason"], "scheduled off peak");
        assert_eq!(content["content"], "audit .state/");
        assert!(content.get("cron").is_some(), "the routing proof stays");
    }

    #[test]
    fn an_immediate_schedule_keeps_the_old_payload_bytes() {
        let fake = Arc::new(DeferResident::default());
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(fake.clone())));
        let schedule = schedule("audit");
        let session = SessionId(uuid::Uuid::new_v4());

        dispatch_at(&schedule, &dispatcher, session);

        assert_eq!(
            fake.plans.load(Ordering::SeqCst),
            0,
            "an immediate schedule never asks the deferral planner"
        );
        let seen = fake.deliveries.lock().expect("deliveries");
        assert_eq!(seen.len(), 1);
        let (_, content, eligible_at) = &seen[0];
        assert_eq!(*eligible_at, None);
        assert!(
            content.get("defer").is_none(),
            "an immediate firing keeps the exact payload it had before"
        );
        assert_eq!(content["content"], "audit .state/");
        assert!(content.get("cron").is_some());
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
        assert!(
            firings[0].session.is_some(),
            "the tick records the resolved session before dispatch: {firings:?}"
        );
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

    /// CRON-MODEL §9.1: a resident firing settles from its target
    /// session's end — without this the row stayed `admitted` forever and
    /// `cron list` could not answer "did it run".
    #[test]
    fn the_settlement_sweep_records_the_target_sessions_end() {
        use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
        use kian_core::model::Epoch;
        use kian_store::AppendGuard;

        let rt = runtime();
        let dir = TempDir::new("settle");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let sid = SessionId(uuid::Uuid::new_v4());
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("settle@{fire_time}");

        let yaml = format!(
            "schedules:\n  - id: settle\n    expr: \"*/5 * * * *\"\n    target: {{ session: \"{}\" }}\n    prompt: audit\n",
            sid.0
        );
        let section: crate::config::CronSection = serde_yaml::from_str(&yaml).unwrap();
        let schedule = section
            .to_runtime(&channels(), Utc::now())
            .expect("the sweep fixture resolves")
            .remove(0);

        let event = |seq: u64, epoch: Epoch, kind: EventKind, bytes: Value| Event {
            seq,
            session: sid,
            epoch,
            ts: 1_800_000_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        rt.block_on(async {
            store
                .append(
                    sid,
                    event(1, Epoch::GENESIS, EventKind::SessionCreated, serde_json::json!({})),
                    AppendGuard {
                        epoch: Epoch::GENESIS,
                        expected_seq: 1,
                    },
                )
                .await
                .expect("session/created");
            store
                .append(
                    sid,
                    event(
                        2,
                        Epoch(1),
                        EventKind::DrainClaimed,
                        serde_json::json!({"holder": "cron-test", "epoch": 1}),
                    ),
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: 2,
                    },
                )
                .await
                .expect("drain/claimed");
            store
                .append(
                    sid,
                    event(
                        3,
                        Epoch(1),
                        EventKind::InputAdmitted,
                        serde_json::json!({"role": "user", "content": "audit",
                                           "cron": {"firing_id": firing_id, "schedule_id": "settle"}}),
                    ),
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: 3,
                    },
                )
                .await
                .expect("input/admitted");
            store
                .append_end(
                    sid,
                    event(
                        4,
                        Epoch(1),
                        EventKind::SessionEnded {
                            reason: kian_core::events::EndReason::Completed,
                        },
                        serde_json::json!({}),
                    ),
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: 4,
                    },
                )
                .await
                .expect("session/ended");
            assert!(
                store
                    .claim_firing(&kian_store::CronFiringRow {
                        firing_id: firing_id.clone(),
                        schedule_id: "settle".to_string(),
                        fire_time: fire_time.to_string(),
                        session: None,
                        admitted_at: 1_800_000_000_000,
                        state: kian_store::FiringState::Admitted,
                        skip_reason: None,
                        worktree_path: None,
                        finished_at: None,
                        error: None,
                    })
                    .await
                    .expect("claim firing")
            );
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads the session");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "succeeded", "{row:?}");
        assert!(row.finished_at.is_some(), "{row:?}");
    }

    /// CRON-MODEL §3.2: the per-slot firing session is deterministic from
    /// durable facts alone (target + firing id) and distinct per slot/target.
    /// The golden literal pins the frozen v5 name contract: renaming
    /// [`FIRING_SESSION_V5_NAME`] would orphan every derived session id, so it
    /// must fail here first (the same doctrine as the resident re-arm name).
    #[test]
    fn firing_session_is_deterministic_and_distinct_per_slot() {
        let target =
            SessionId(uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let other = SessionId(uuid::Uuid::new_v4());
        let first = firing_session(target, "audit@2027-01-15T08:05:00Z");
        assert_eq!(
            first,
            SessionId(uuid::Uuid::parse_str("71ba09b2-5ac5-5425-bd8d-2f8285fbef4a").unwrap()),
            "the derived session id is frozen contract"
        );
        assert_eq!(first, firing_session(target, "audit@2027-01-15T08:05:00Z"));
        assert_ne!(first, firing_session(target, "audit@2027-01-15T08:06:00Z"));
        assert_ne!(first, firing_session(other, "audit@2027-01-15T08:05:00Z"));
    }

    /// CRON-MODEL §3.2: resolution keeps an empty/live target (continuity,
    /// resident-shaped targets) and replaces a terminal target with this
    /// slot's firing session.
    #[test]
    fn resolve_keeps_a_live_target_and_moves_a_terminal_one() {
        let rt = runtime();
        let dir = TempDir::new("resolve");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let turn = EngineTurn::new(engine_for(&store), rt.handle().clone(), None);

        let empty = SessionId::new();
        assert_eq!(
            turn.resolve(empty, "s@a").expect("resolve empty"),
            empty,
            "a never-created target is delivered to"
        );
        let live = SessionId::new();
        rt.block_on(async {
            append_created(&store, live).await;
            append_claim(&store, live, 2, Epoch(1)).await;
        });
        assert_eq!(
            turn.resolve(live, "s@a").expect("resolve live"),
            live,
            "a running target keeps its conversation"
        );
        for reason in [
            EndReason::Completed,
            EndReason::Cancelled,
            EndReason::Rotated,
        ] {
            let frozen = SessionId::new();
            rt.block_on(seed_frozen_session(&store, frozen, reason));
            assert_eq!(
                turn.resolve(frozen, "s@a").expect("resolve frozen"),
                firing_session(frozen, "s@a"),
                "{reason:?}"
            );
        }
    }

    /// The conjunct: a live target still accepts the firing, and the bootstrap
    /// runs before the durable row (ADR-038's ordering claim — the bootstrap
    /// observes an empty inbox at call time, not merely a call count).
    #[test]
    fn a_live_target_accepts_the_firing_after_the_bootstrap() {
        let rt = runtime();
        let dir = TempDir::new("live-target");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let sid = SessionId(uuid::Uuid::new_v4());

        let engine = engine_for(&store);
        let bootstrap = Arc::new(ObservingBootstrap {
            store: store.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
            saw_row: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let turn = EngineTurn::new(
            Arc::clone(&engine),
            rt.handle().clone(),
            Some(bootstrap.clone()),
        );

        assert_eq!(
            turn.should_skip(sid).expect("the overlap judgement reads"),
            None,
            "a never-created target is not busy"
        );
        turn.deliver(
            sid,
            serde_json::json!({"content": "audit"}),
            Some("cron:live".into()),
        )
        .expect("a live target accepts the firing");
        assert_eq!(
            bootstrap.calls.load(Ordering::SeqCst),
            1,
            "the bootstrap runs before the firing row"
        );
        assert!(
            !bootstrap.saw_row.load(Ordering::SeqCst),
            "the inbox was empty when the bootstrap ran"
        );
        let rows = rt.block_on(store.input_rows(sid)).expect("input rows read");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].idempotency_key.as_deref(), Some("cron:live"));
    }

    /// CRON-MODEL §3.2/§5.1: a due slot on a frozen target runs in the derived
    /// firing session — never skipped, never stranded on the frozen stream —
    /// and the firing row records that session. The target's leftover row stays
    /// the audit fact it is.
    #[test]
    fn a_frozen_target_runs_in_its_firing_session_instead_of_skipping() {
        let rt = multi_runtime();
        let dir = TempDir::new("frozen-run");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId(uuid::Uuid::new_v4());
        rt.block_on(async {
            seed_frozen_session(&store, target, EndReason::Completed).await;
            let error = store
                .admit(InputRow {
                    id: uuid::Uuid::new_v4(),
                    session: target,
                    payload: serde_json::json!({"role": "user", "content": "leftover"}),
                    admitted_at: 1_700_000_000_000,
                    eligible_at: 1_700_000_000_000,
                    idempotency_key: Some("leftover".into()),
                })
                .await
                .expect_err("a terminal session refuses a new row");
            assert!(matches!(
                error,
                kian_store::InboxError::SessionTerminal { session } if session == target
            ));
        });
        let target_events_before = rt.block_on(store.rebuild(target)).expect("rebuild").len();

        let schedule = session_schedule("frozen-run", target);
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let firing_id = kian_cron::firing_id("frozen-run", anchor);
        let derived = firing_session(target, &firing_id);
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let turn = Arc::new(EngineTurn::new(
            engine_for(&store),
            rt.handle().clone(),
            None,
        ));
        let dispatcher = ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(turn)));

        let (report, events) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.failed.is_empty(), "{report:?}");
        assert_eq!(report.admitted.len(), 1, "{report:?}");
        assert!(events.is_empty(), "no skip: {events:?}");
        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(
            row.session.as_deref(),
            Some(derived.0.to_string().as_str()),
            "the firing records its derived session"
        );
        assert!(
            wait_for_state(&rt, &store, derived, SessionState::Completed),
            "the firing ran in its own session"
        );
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads the recorded session");
        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "succeeded", "{row:?}");

        // The frozen target is untouched: its attempted leftover row was refused.
        assert_eq!(
            rt.block_on(store.rebuild(target)).expect("rebuild").len(),
            target_events_before,
            "nothing was appended to the frozen target"
        );
        assert_eq!(
            rt.block_on(store.eligible_inputs(target, 1_900_000_000_000))
                .expect("eligible read")
                .len(),
            0,
            "the terminal target admitted no leftover row"
        );
        let fired = rt.block_on(store.rebuild(derived)).expect("rebuild");
        assert!(
            fired.iter().any(|ev| {
                ev.kind == EventKind::InputAdmitted
                    && ev.bytes.pointer("/cron/firing_id").and_then(Value::as_str)
                        == Some(firing_id.as_str())
            }),
            "the derived session consumed the firing"
        );
    }

    /// The production sequence: slot 1 runs in the empty target (which then
    /// freezes completed); slots 2 and 3 each run in their own derived firing
    /// session. Recurring schedules keep working with no operator action, and
    /// the frozen target is never re-created or re-admitted.
    #[test]
    fn a_successful_firing_freezes_the_target_and_the_next_slots_run_derived() {
        let rt = multi_runtime();
        let dir = TempDir::new("slots");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId(uuid::Uuid::new_v4());
        let schedule = session_schedule("slots", target);
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let turn = Arc::new(EngineTurn::new(
            engine_for(&store),
            rt.handle().clone(),
            None,
        ));
        let dispatcher = ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(turn)));

        let mut sessions: Vec<Option<String>> = Vec::new();
        let mut target_len_after_first = 0usize;
        for slot in 0..3i64 {
            let at = anchor + TimeDelta::minutes(slot);
            let firing_id = kian_cron::firing_id("slots", at);
            let expected = if slot == 0 {
                target
            } else {
                firing_session(target, &firing_id)
            };
            let (report, events) = tick_and_dispatch_classified(
                at + SEED_SLACK,
                std::slice::from_ref(&schedule),
                &durable,
                &dispatcher,
            );
            assert!(report.failed.is_empty(), "slot {slot}: {report:?}");
            assert!(events.is_empty(), "slot {slot} must not skip: {events:?}");
            assert!(
                wait_for_state(&rt, &store, expected, SessionState::Completed),
                "slot {slot} ran in {expected:?}"
            );
            // A fresh adapter per sweep: the per-schedule sweep throttle is
            // wall-clock based (15 s in production), and this loop runs in
            // microseconds.
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()])
                .settle_from_target(&schedule)
                .expect("the sweep settles");
            let row = rt
                .block_on(store.load_firing(&firing_id))
                .expect("load firing")
                .expect("row");
            assert_eq!(row.state.as_str(), "succeeded", "slot {slot}: {row:?}");
            assert_eq!(
                row.session.as_deref(),
                Some(expected.0.to_string().as_str()),
                "slot {slot} records the session it ran in"
            );
            sessions.push(row.session.clone());
            if slot == 0 {
                target_len_after_first = rt.block_on(store.rebuild(target)).expect("rebuild").len();
            }
        }
        assert_eq!(
            sessions[0].as_deref(),
            Some(target.0.to_string().as_str()),
            "the first firing uses the configured target"
        );
        assert_ne!(sessions[1], sessions[2], "each later slot derives its own");
        assert_eq!(
            rt.block_on(store.rebuild(target)).expect("rebuild").len(),
            target_len_after_first,
            "the frozen target is never re-created or re-admitted"
        );
        assert!(
            rt.block_on(store.open_firings("slots"))
                .expect("open read")
                .is_empty(),
            "every slot settled"
        );
    }

    /// CRON-MODEL §9.1: the sweep settles from the session recorded on the
    /// firing, even when the configured target is a different (never created)
    /// session.
    #[test]
    fn the_settlement_sweep_reads_the_recorded_session() {
        let rt = runtime();
        let dir = TempDir::new("recorded-session");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        let session = SessionId::new();
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("rec@{fire_time}");
        rt.block_on(async {
            append_created(&store, session).await;
            append_claim(&store, session, 2, Epoch(1)).await;
            append_firing_input(&store, session, 3, Epoch(1), "rec", &firing_id).await;
            append_end(&store, session, 4, Epoch(1), EndReason::Completed).await;
            claim_firing_row(&store, "rec", fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, session)
                    .await
                    .expect("record session")
            );
        });

        let schedule = session_schedule("rec", target);
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "succeeded", "{row:?}");
    }

    /// One `jobs:` declaration resolved exactly the way the loader resolves
    /// it (JOB-MODEL §7.1(a)): the lowered schedule plus the frozen plan the
    /// conductor would carry.
    fn job_declaration(
        id: &str,
        job: &str,
    ) -> (
        crate::config::CronRuntimeSchedule,
        kian_superagent::job::plan::JobPlan,
    ) {
        let yaml = format!(
            "jobs:\n  - id: {job}\n    plan:\n      - id: fetch\n        prompt: fetch the news\n        produces: {{ name: raw, max_bytes: 1024 }}\n        verify: [{{ check: final_message_present }}]\ntriggers:\n  - id: {id}\n    job: {job}\n    expr: \"* * * * *\"\n"
        );
        let section: crate::job_config::JobsSection =
            serde_yaml::from_str(&yaml).expect("the job fixture parses");
        let mut resolved = section
            .resolve(&channels(), &std::collections::BTreeSet::new(), Utc::now())
            .expect("the job fixture resolves");
        let plan = resolved
            .catalog
            .job(job)
            .expect("the declared job")
            .plan
            .clone();
        (resolved.schedules.remove(0), plan)
    }

    fn job_catalog(
        id: &str,
        job: &str,
    ) -> (
        crate::config::CronRuntimeSchedule,
        Arc<crate::job_config::JobCatalog>,
    ) {
        let yaml = format!(
            "jobs:\n  - id: {job}\n    plan:\n      - id: fetch\n        prompt: fetch the news\n        produces: {{ name: raw, max_bytes: 1024 }}\n        verify: [{{ check: final_message_present }}]\ntriggers:\n  - id: {id}\n    job: {job}\n    expr: \"* * * * *\"\n"
        );
        let section: crate::job_config::JobsSection =
            serde_yaml::from_str(&yaml).expect("the job fixture parses");
        let mut resolved = section
            .resolve(&channels(), &std::collections::BTreeSet::new(), Utc::now())
            .expect("the job fixture resolves");
        (resolved.schedules.remove(0), Arc::new(resolved.catalog))
    }

    #[test]
    fn a_job_run_refuses_a_derived_session_from_another_stream() {
        let rt = runtime();
        let dir = TempDir::new("job-identity-conflict");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let (schedule, catalog) = job_catalog("digest-morning", "digest");
        let firing_id = format!("{}@2027-01-15T08:05:00Z", schedule.id);
        let slot = slot_of_firing(&firing_id).expect("slot");
        let session = kian_superagent::job::plan::run_session("digest", slot);
        rt.block_on(append_created(&store, session));

        let turn = Arc::new(
            EngineTurn::new(engine_for(&store), rt.handle().clone(), None)
                .with_store(store.clone()),
        );
        let dispatcher = ResidentDispatcher::new(turn).with_jobs(catalog);
        let error = dispatcher
            .dispatch(&schedule, &firing_id, "{}", Some(session))
            .expect_err("another stream owns the derived identity");
        assert!(error.contains("run_identity_conflict"), "{error}");
        let rows = rt.block_on(store.input_rows(session)).expect("rows");
        assert!(rows.is_empty(), "nothing may be admitted into the conflict");
    }

    #[test]
    fn a_job_run_accepts_a_derived_session_with_the_same_marker() {
        let rt = runtime();
        let dir = TempDir::new("job-identity-same");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let (schedule, catalog) = job_catalog("digest-morning", "digest");
        let firing_id = format!("{}@2027-01-15T08:05:00Z", schedule.id);
        let slot = slot_of_firing(&firing_id).expect("slot");
        let session = kian_superagent::job::plan::run_session("digest", slot);
        let declared = catalog.job("digest").expect("job").plan.clone();
        let marker =
            kian_superagent::job::status::marker_payload("digest", slot, "cron", &declared);
        rt.block_on(async {
            append_created(&store, session).await;
            store
                .append(
                    session,
                    cron_event(
                        session,
                        2,
                        Epoch(1),
                        EventKind::InputAdmitted,
                        serde_json::json!({"role": "user", "content": "run", "job": marker}),
                    ),
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: 2,
                    },
                )
                .await
                .expect("existing marker");
        });

        let turn = Arc::new(
            EngineTurn::new(engine_for(&store), rt.handle().clone(), None)
                .with_store(store.clone()),
        );
        let dispatcher = ResidentDispatcher::new(turn).with_jobs(catalog);
        dispatcher
            .dispatch(&schedule, &firing_id, "{}", Some(session))
            .expect("the same marker names the same run");
        let rows = rt.block_on(store.input_rows(session)).expect("rows");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].payload.get("job").is_some());
    }

    /// Seed a job run stream through its settling end: created, claimed, the
    /// marker-carrying firing input, optionally one run verdict, then
    /// `completed` (the end reason a job run always has when it finishes).
    async fn seed_job_run(
        store: &EventStore,
        session: SessionId,
        schedule_id: &str,
        firing_id: &str,
        plan: &kian_superagent::job::plan::JobPlan,
        outcome: Option<kian_superagent::job::outcome::RunOutcome>,
    ) {
        append_created(store, session).await;
        append_claim(store, session, 2, Epoch(1)).await;
        let slot = slot_of_firing(firing_id).expect("the firing names its slot");
        let marker = kian_superagent::job::status::marker_payload(&plan.id, slot, "cron", plan);
        store
            .append(
                session,
                cron_event(
                    session,
                    3,
                    Epoch(1),
                    EventKind::InputAdmitted,
                    serde_json::json!({"role": "user", "content": "run the job",
                                       "cron": {"firing_id": firing_id,
                                                "schedule_id": schedule_id},
                                       "job": marker}),
                ),
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 3,
                },
            )
            .await
            .expect("input/admitted");
        let mut seq = 4;
        if let Some(outcome) = outcome {
            let payload = kian_superagent::job::status::run_verdict_payload(
                None,
                outcome,
                &plan.digest(),
                &[],
                None,
                None,
            );
            store
                .append(
                    session,
                    cron_event(session, seq, Epoch(1), EventKind::EvidenceVerdict, payload),
                    AppendGuard {
                        epoch: Epoch(1),
                        expected_seq: seq,
                    },
                )
                .await
                .expect("evidence/verdict");
            seq += 1;
        }
        append_end(store, session, seq, Epoch(1), EndReason::Completed).await;
    }

    /// (JOB-MODEL §5.5, §18.2 amendment 1) A job run ends `completed` whether
    /// it succeeded or merely went unverified, so the sweep must settle from
    /// the run verdict; the bare end reason would call both a success.
    #[test]
    fn a_job_firing_sets_succeeded_from_its_run_verdict() {
        use kian_superagent::job::outcome::RunOutcome;

        let rt = runtime();
        let dir = TempDir::new("job-succeeded");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let (schedule, plan) = job_declaration("digest-morning", "digest");
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("{}@{fire_time}", schedule.id);
        let slot = slot_of_firing(&firing_id).expect("slot");
        let session = kian_superagent::job::plan::run_session("digest", slot);
        rt.block_on(async {
            seed_job_run(
                &store,
                session,
                &schedule.id,
                &firing_id,
                &plan,
                Some(RunOutcome::Succeeded),
            )
            .await;
            claim_firing_row(&store, &schedule.id, fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, session)
                    .await
                    .expect("record session")
            );
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep settles");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "succeeded", "{row:?}");
        assert_eq!(row.error, None, "{row:?}");
    }

    /// The honest pair: an unverified run is NOT a success even though its
    /// end reason is `completed`.
    #[test]
    fn an_unverified_job_firing_settles_failed() {
        use kian_superagent::job::outcome::RunOutcome;

        let rt = runtime();
        let dir = TempDir::new("job-unverified");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let (schedule, plan) = job_declaration("digest-morning", "digest");
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("{}@{fire_time}", schedule.id);
        let slot = slot_of_firing(&firing_id).expect("slot");
        let session = kian_superagent::job::plan::run_session("digest", slot);
        rt.block_on(async {
            seed_job_run(
                &store,
                session,
                &schedule.id,
                &firing_id,
                &plan,
                Some(RunOutcome::Unverified),
            )
            .await;
            claim_firing_row(&store, &schedule.id, fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, session)
                    .await
                    .expect("record session")
            );
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep settles");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert!(
            row.error
                .as_deref()
                .unwrap_or_default()
                .contains("unverified"),
            "{row:?}"
        );
    }

    /// A run stream that ends without a run verdict can never be read as a
    /// success: the conductor's silence is not evidence (J7).
    #[test]
    fn a_job_firing_without_a_run_verdict_settles_failed() {
        let rt = runtime();
        let dir = TempDir::new("job-no-verdict");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let (schedule, plan) = job_declaration("digest-morning", "digest");
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("{}@{fire_time}", schedule.id);
        let slot = slot_of_firing(&firing_id).expect("slot");
        let session = kian_superagent::job::plan::run_session("digest", slot);
        rt.block_on(async {
            seed_job_run(&store, session, &schedule.id, &firing_id, &plan, None).await;
            claim_firing_row(&store, &schedule.id, fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, session)
                    .await
                    .expect("record session")
            );
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep settles");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert!(
            row.error
                .as_deref()
                .unwrap_or_default()
                .contains("without an outcome"),
            "{row:?}"
        );
    }

    /// CRON-MODEL §9.1: a pre-v8 row (no recorded session) stranded on a frozen
    /// target settles `failed`/`target_terminal` after the grace — the wedge
    /// heal, unchanged.
    #[test]
    fn the_settlement_sweep_fails_a_legacy_firing_stranded_on_a_frozen_target() {
        let rt = runtime();
        let dir = TempDir::new("legacy-strand");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        rt.block_on(seed_frozen_session(&store, target, EndReason::Completed));
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("legacy@{fire_time}");
        let schedule = session_schedule("legacy", target);
        rt.block_on(claim_firing_row(&store, "legacy", fire_time));

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert!(
            row.error
                .as_deref()
                .unwrap_or_default()
                .starts_with(TARGET_TERMINAL),
            "{row:?}"
        );
    }

    /// CRON-MODEL §9.1: the stranded heal waits for the grace, so a second
    /// replica's freshly claimed row is never mis-settled as failed.
    #[test]
    fn the_stranded_heal_waits_for_the_grace() {
        let rt = runtime();
        let dir = TempDir::new("grace");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        rt.block_on(seed_frozen_session(&store, target, EndReason::Completed));
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("fresh@{fire_time}");
        let schedule = session_schedule("fresh", target);
        rt.block_on(claim_firing_row_at(
            &store,
            "fresh",
            fire_time,
            epoch_ms(Utc::now()),
        ));

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(
            row.state.as_str(),
            "admitted",
            "a fresh claim inside the grace is not settled: {row:?}"
        );
    }

    /// CRON-MODEL §9.1: a recorded row that is still pending in a non-frozen
    /// session can be taken later, so the sweep leaves the firing open — the
    /// pending check is what keeps the stranded heal from mis-settling it.
    #[test]
    fn the_stranded_heal_waits_while_a_recorded_row_is_still_pending() {
        let rt = runtime();
        let dir = TempDir::new("pending-row");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        let live = SessionId::new();
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("pending@{fire_time}");
        let schedule = session_schedule("pending", target);
        rt.block_on(async {
            append_created(&store, live).await;
            append_claim(&store, live, 2, Epoch(1)).await;
            claim_firing_row(&store, "pending", fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, live)
                    .await
                    .expect("record session")
            );
            store
                .admit(InputRow {
                    id: uuid::Uuid::new_v4(),
                    session: live,
                    payload: serde_json::json!({"content": "audit",
                                               "cron": {"firing_id": firing_id, "schedule_id": "pending"}}),
                    admitted_at: 1_700_000_000_000,
                    eligible_at: 1_700_000_000_000,
                    idempotency_key: Some("cron:pending".into()),
                })
                .await
                .expect("durable pending row");
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(
            row.state.as_str(),
            "admitted",
            "a pending row on a live session may still run: {row:?}"
        );
    }

    /// The admit guard: a session that became busy after resolution must not
    /// receive a queued duplicate; `deliver` refuses it as `skipped_overlap`
    /// (CRON-MODEL §5.1).
    #[test]
    fn a_session_busy_at_the_admit_guard_is_refused() {
        let rt = runtime();
        let dir = TempDir::new("admit-guard");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let sid = SessionId::new();
        rt.block_on(async {
            append_created(&store, sid).await;
            append_claim(&store, sid, 2, Epoch(1)).await;
            store
                .admit(InputRow {
                    id: uuid::Uuid::new_v4(),
                    session: sid,
                    payload: serde_json::json!({"role": "user", "content": "busy work"}),
                    admitted_at: 1_700_000_000_000,
                    eligible_at: 1_700_000_000_000,
                    idempotency_key: Some("busy".into()),
                })
                .await
                .expect("pending row makes the session busy");
        });
        let turn = EngineTurn::new(engine_for(&store), rt.handle().clone(), None);
        let error = turn
            .deliver(
                sid,
                serde_json::json!({"content": "audit"}),
                Some("cron:busy".into()),
            )
            .expect_err("a busy session refuses the admit");
        assert!(error.starts_with("skipped_overlap"), "{error}");
    }

    /// CRON-MODEL §9.1: a recorded session that never took the row (a crash
    /// between the session record and the admit) heals `failed` after the
    /// grace — it can never run.
    #[test]
    fn a_recorded_session_that_never_took_the_row_heals_after_the_grace() {
        let rt = runtime();
        let dir = TempDir::new("never-admitted");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        let never = SessionId::new();
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("crash@{fire_time}");
        let schedule = session_schedule("crash", target);
        rt.block_on(async {
            claim_firing_row(&store, "crash", fire_time).await;
            assert!(
                store
                    .set_firing_session(&firing_id, never)
                    .await
                    .expect("record session")
            );
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert!(
            row.error
                .as_deref()
                .unwrap_or_default()
                .starts_with(TARGET_TERMINAL),
            "{row:?}"
        );
    }

    /// A pre-v8 stranded row hidden behind ≥20 later settled slots is still
    /// found by the sweep's open-firing read.
    #[test]
    fn the_settlement_sweep_finds_a_firing_stranded_behind_many_settled_slots() {
        let rt = runtime();
        let dir = TempDir::new("stranded-wide");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        rt.block_on(seed_frozen_session(&store, target, EndReason::Completed));
        let schedule = session_schedule("wide", target);
        let base = DateTime::parse_from_rfc3339("2027-01-15T08:05:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc);
        let stranded = fire_time_text(base);
        rt.block_on(async {
            claim_firing_row(&store, "wide", &stranded).await;
            for i in 1..=25i64 {
                let text = fire_time_text(base + TimeDelta::minutes(i));
                claim_firing_row(&store, "wide", &text).await;
                assert!(
                    store
                        .finish_firing(
                            &format!("wide@{text}"),
                            kian_store::FiringState::Skipped,
                            Some(1_700_000_000_000 + i as u64),
                            None,
                            Some(SKIP_OVERLAP),
                            None,
                        )
                        .await
                        .expect("settle a skipped slot")
                );
            }
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&format!("wide@{stranded}")))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert!(
            row.error
                .as_deref()
                .unwrap_or_default()
                .starts_with(TARGET_TERMINAL),
            "{row:?}"
        );
    }

    /// The sweep skips over the resumable `awaiting_input` end: the answer
    /// resumes the same turn, so only a settling end (completed here) decides
    /// the firing's outcome.
    #[test]
    fn the_settlement_sweep_skips_the_resumable_awaiting_input_end() {
        let rt = runtime();
        let dir = TempDir::new("await-input-end");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("multi@{fire_time}");
        let schedule = session_schedule("multi", target);
        rt.block_on(async {
            append_created(&store, target).await;
            append_claim(&store, target, 2, Epoch(1)).await;
            append_firing_input(&store, target, 3, Epoch(1), "multi", &firing_id).await;
            append_end(&store, target, 4, Epoch(1), EndReason::AwaitingInput).await;
            append_claim(&store, target, 5, Epoch(2)).await;
            append_end(&store, target, 6, Epoch(2), EndReason::Completed).await;
            claim_firing_row(&store, "multi", fire_time).await;
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "succeeded", "{row:?}");
    }

    /// The sweep stops at the **first settling** end after the admission: a
    /// firing whose own episode failed must settle `failed` even when a later
    /// episode of the same session completed.
    #[test]
    fn the_settlement_sweep_stops_at_the_first_settling_end() {
        let rt = runtime();
        let dir = TempDir::new("first-settling-end");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let target = SessionId::new();
        let fire_time = "2027-01-15T08:05:00Z";
        let firing_id = format!("first@{fire_time}");
        let schedule = session_schedule("first", target);
        rt.block_on(async {
            append_created(&store, target).await;
            append_claim(&store, target, 2, Epoch(1)).await;
            append_firing_input(&store, target, 3, Epoch(1), "first", &firing_id).await;
            append_end(&store, target, 4, Epoch(1), EndReason::Failed).await;
            append_claim(&store, target, 5, Epoch(2)).await;
            append_end(&store, target, 6, Epoch(2), EndReason::Completed).await;
            claim_firing_row(&store, "first", fire_time).await;
        });

        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        durable
            .settle_from_target(&schedule)
            .expect("the sweep reads");

        let row = rt
            .block_on(store.load_firing(&firing_id))
            .expect("load firing")
            .expect("row");
        assert_eq!(row.state.as_str(), "failed", "{row:?}");
        assert_eq!(row.error.as_deref(), Some("the scheduled turn failed"));
    }

    /// Race guard: every terminal end reason freezes the stream, and `deliver`
    /// refuses a session passed to it directly (dispatch resolves first, so
    /// this is the only remaining path to `target_terminal` delivery).
    #[test]
    fn delivering_to_a_terminal_session_fails_closed() {
        let rt = runtime();
        let dir = TempDir::new("terminal-reasons");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let bootstrap = Arc::new(CountingBootstrap::default());
        let turn = EngineTurn::new(
            engine_for(&store),
            rt.handle().clone(),
            Some(bootstrap.clone()),
        );
        for reason in [
            EndReason::Completed,
            EndReason::Cancelled,
            EndReason::Rotated,
        ] {
            let sid = SessionId(uuid::Uuid::new_v4());
            rt.block_on(seed_frozen_session(&store, sid, reason));
            assert_eq!(
                turn.should_skip(sid).expect("the overlap judgement reads"),
                None,
                "{reason:?}"
            );
            let error = turn
                .deliver(
                    sid,
                    serde_json::json!({"content": "audit"}),
                    Some(format!("cron:{reason:?}")),
                )
                .expect_err("a terminal session refuses the firing");
            assert!(error.starts_with(TARGET_TERMINAL), "{reason:?}: {error}");
            assert!(
                rt.block_on(store.input_rows(sid))
                    .expect("input rows read")
                    .is_empty(),
                "{reason:?}: nothing is admitted"
            );
        }
        assert_eq!(
            bootstrap.calls.load(Ordering::SeqCst),
            0,
            "the bootstrap never runs for a terminal session"
        );
    }

    /// CRON-MODEL §5.1's default `skip`: a firing whose target session is
    /// still working is recorded `skipped_overlap` with its reason and is
    /// never delivered (the resident arm used to queue it behind the
    /// session's single-flight drain, producing a duplicate run).
    #[test]
    fn a_busy_target_session_skips_the_slot_instead_of_queueing_it() {
        struct Busy;
        impl ResidentTurn for Busy {
            fn deliver(&self, _: SessionId, _: Value, _: Option<String>) -> Result<(), String> {
                panic!("a busy target must not receive the firing");
            }
            fn should_skip(&self, _: SessionId) -> Result<Option<String>, String> {
                Ok(Some("still running".to_string()))
            }
        }
        let rt = runtime();
        let dir = TempDir::new("busy");
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule("audit");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let dispatcher =
            ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(Busy))));
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let (report, events) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.admitted.is_empty(), "{:?}", report.admitted);
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["event"], "cron/skipped");
        assert_eq!(events[0]["reason"], SKIP_OVERLAP);

        let firings = rt
            .block_on(store.list_firings(Some("audit"), 10))
            .expect("firing rows");
        assert_eq!(firings.len(), 1, "{firings:?}");
        assert_eq!(firings[0].state.as_str(), "skipped");
        assert_eq!(firings[0].skip_reason.as_deref(), Some(SKIP_OVERLAP));
        assert_eq!(
            firings[0].session, None,
            "a skip decided before the session record claims no session"
        );
        assert!(firings[0].finished_at.is_some(), "{firings:?}");
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
            let out = crate::cron_worktree::git_command()
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
            .dispatch(&schedule, &firing_id, &payload, None)
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

    /// CRON-MODEL §9.1: a *synchronous* worktree turn (tests, `run-now`)
    /// settles on the same tick through `take_outcome` — the immediate
    /// outcome must not be silently dropped, or the row would stay
    /// `admitted` until the next tick's drain (and the async arm's
    /// contract would hide a synchronous regression).
    #[test]
    fn a_synchronous_worktree_tick_settles_the_firing_succeeded() {
        let rt = runtime();
        let dir = TempDir::new("sync-settle");
        let repo = dir.path().join("repo");
        init_repo(&repo);
        let sqlite = dir.path().join("cron.sqlite").to_string_lossy().to_string();
        let store = rt.block_on(EventStore::open(&sqlite)).expect("store opens");
        let schedule = schedule_yaml("iso", "    worktree: true\n");
        let durable =
            DurableCronTickStore::new(store.clone(), rt.handle().clone(), vec![schedule.clone()]);
        let worktree = Arc::new(
            WorktreeDispatcher::new(repo.clone())
                .with_policy(WorktreePolicy {
                    min_free_mib: 0,
                    min_free_percent: 0,
                    ..WorktreePolicy::default()
                })
                .with_runner(Box::new(SharedRunner(Arc::new(CountingRunner {
                    calls: AtomicUsize::new(0),
                })))),
        );
        let dispatcher = ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(
            FakeResident::default(),
        ))))
        .with_worktree(worktree);
        let anchor = schedule.next_fire_after(Utc::now()).expect("next fire");
        let (report, _) = tick_and_dispatch_classified(
            anchor + SEED_SLACK,
            std::slice::from_ref(&schedule),
            &durable,
            &dispatcher,
        );
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.admitted.len(), 1, "{:?}", report.admitted);
        let firings = rt
            .block_on(store.list_firings(Some("iso"), 5))
            .expect("firing rows");
        assert_eq!(firings.len(), 1, "{firings:?}");
        assert_eq!(firings[0].state.as_str(), "succeeded", "{firings:?}");
        assert!(firings[0].finished_at.is_some(), "{firings:?}");
        assert!(firings[0].worktree_path.is_some(), "{firings:?}");
    }

    /// A turn runner that holds its slot for a moment, so the async
    /// dispatch's non-blocking property and the global ceiling are
    /// observable.
    struct SlowRunner {
        delay: std::time::Duration,
        calls: AtomicUsize,
    }

    impl TurnRunner for SlowRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.worktree.exists(),
                "the turn runs in a real worktree: {}",
                request.worktree.display()
            );
            std::thread::sleep(self.delay);
            Ok(TurnOutput {
                stdout: "slow turn done\n".to_string(),
                stderr: String::new(),
                exit_code: Some(0),
                timed_out: false,
                too_large: false,
            })
        }
    }

    struct SharedSlowRunner(Arc<SlowRunner>);

    impl TurnRunner for SharedSlowRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.0.run(request)
        }
    }

    /// CRON-MODEL §5.4/§7.3: the production ticker hands a worktree turn
    /// to its own thread, so one long turn cannot stall every other
    /// schedule. `dispatch` returns before the turn finishes and the
    /// outcome is drained (and settled) on a later tick.
    #[test]
    fn async_worktree_dispatch_returns_before_the_turn_finishes() {
        let dir = TempDir::new("async-worktree");
        let repo = dir.path().join("repo");
        init_repo(&repo);
        let schedule = schedule_yaml("isolated", "    worktree: { root: kian-worktrees }\n");
        let runner = Arc::new(SlowRunner {
            delay: std::time::Duration::from_millis(300),
            calls: AtomicUsize::new(0),
        });
        let worktree = Arc::new(
            WorktreeDispatcher::new(repo.clone())
                .with_policy(WorktreePolicy {
                    min_free_mib: 0,
                    min_free_percent: 0,
                    ..WorktreePolicy::default()
                })
                .with_runner(Box::new(SharedSlowRunner(Arc::clone(&runner)))),
        );
        let dispatcher = ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(
            FakeResident::default(),
        ))))
        .with_worktree(Arc::clone(&worktree))
        .with_async_worktree(4);

        let fire_time = DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc);
        let firing_id = kian_cron::firing_id("isolated", fire_time);
        let payload = cron_tick::dispatch_payload(&schedule, &firing_id, fire_time);

        let started = std::time::Instant::now();
        dispatcher
            .dispatch(&schedule, &firing_id, &payload, None)
            .expect("the async arm accepts the firing");
        assert!(
            started.elapsed() < std::time::Duration::from_millis(150),
            "dispatch returned after {:?}, not before the turn finished",
            started.elapsed()
        );
        assert!(
            dispatcher.drain_settled().is_empty(),
            "the turn is still running, so nothing settled yet"
        );

        // The spawned thread records its outcome; the ticker drains it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let settled = loop {
            let settled = dispatcher.drain_settled();
            if !settled.is_empty() {
                break settled;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the async turn never recorded an outcome"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        assert_eq!(settled.len(), 1, "{settled:?}");
        assert_eq!(settled[0].0, firing_id);
        assert_eq!(settled[0].1.state, FiringState::Succeeded, "{settled:?}");
        assert!(settled[0].1.worktree_path.is_some(), "{settled:?}");
        assert!(settled[0].1.finished_at_ms.is_some(), "{settled:?}");
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    }

    /// CRON-MODEL §5.4: the process-wide ceiling refuses an over-cap
    /// firing as a recorded overlap, never a burst.
    #[test]
    fn the_global_worktree_ceiling_refuses_a_second_turn() {
        let dir = TempDir::new("global-cap");
        let repo = dir.path().join("repo");
        init_repo(&repo);
        let first = schedule_yaml("cap-a", "    worktree: { root: kian-worktrees }\n");
        let second = schedule_yaml("cap-b", "    worktree: { root: kian-worktrees }\n");
        let runner = Arc::new(SlowRunner {
            delay: std::time::Duration::from_millis(250),
            calls: AtomicUsize::new(0),
        });
        let worktree = Arc::new(
            WorktreeDispatcher::new(repo.clone())
                .with_policy(WorktreePolicy {
                    min_free_mib: 0,
                    min_free_percent: 0,
                    ..WorktreePolicy::default()
                })
                .with_runner(Box::new(SharedSlowRunner(Arc::clone(&runner)))),
        );
        let dispatcher = ScheduleDispatcher::resident(Arc::new(ResidentDispatcher::new(Arc::new(
            FakeResident::default(),
        ))))
        .with_worktree(Arc::clone(&worktree))
        .with_async_worktree(1);

        let fire_time = DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc);
        let first_id = kian_cron::firing_id("cap-a", fire_time);
        let second_id = kian_cron::firing_id("cap-b", fire_time);
        dispatcher
            .dispatch(
                &first,
                &first_id,
                &cron_tick::dispatch_payload(&first, &first_id, fire_time),
                None,
            )
            .expect("the first turn takes the only global slot");
        let refused = dispatcher
            .dispatch(
                &second,
                &second_id,
                &cron_tick::dispatch_payload(&second, &second_id, fire_time),
                None,
            )
            .expect_err("the ceiling refuses the second turn");
        assert!(refused.contains("global concurrency ceiling"), "{refused}");
        assert!(refused.starts_with("skipped_overlap"), "{refused}");

        // Let the first turn finish and drain its outcome before the temp
        // repo is removed.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let settled = dispatcher.drain_settled();
            if !settled.is_empty() {
                assert_eq!(settled.len(), 1, "{settled:?}");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the first async turn never recorded an outcome"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
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

        // A restart with the same declaration: the cursor survives, and
        // the steady-state tick writes nothing (read-first declaration
        // sync — the per-tick upsert used to race sqlite writers).
        std::thread::sleep(std::time::Duration::from_millis(10));
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
        assert_eq!(
            kept.updated_at, fired.updated_at,
            "an unchanged declaration writes nothing per tick"
        );

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
