//! Durable cron state (CRON-MODEL §4/§9, migrations v7–v8).
//!
//! Two tables, both **state of record for the scheduler, never event
//! truth**: a firing is an ordinary admitted turn whose record is an
//! ordinary event row (`EVENT-MODEL.md`); these rows are the scheduler's
//! own bookkeeping, which is why they live in the same store as the rest
//! of kian state (CRON-MODEL §4.1: `storage.sqlite`).
//!
//! - `cron_schedules` — one row per declared schedule: the normalized
//!   declaration identity (`expr`, `tz`, `declaration_digest`), the
//!   durable cursor (`cursor_fire_at` = last *admitted* fire time,
//!   `next_fire_at`) and `enabled`. A changed `declaration_digest` resets
//!   the cursor (§12.3) — visibly: the row's cursor columns go `NULL` in
//!   the same statement that adopts the new declaration.
//! - `cron_firings` — one row per **slot** `(schedule_id, fire_time)`,
//!   keyed by `firing_id = "{schedule_id}@{fire_time}"` (§4.3). The
//!   primary key *is* the idempotency mechanism: claiming a slot is
//!   `INSERT … ON CONFLICT DO NOTHING`, so of any number of racing
//!   callers exactly one learns it won (the single-writer rule of §4.2
//!   enforced by the store, not by careful callers).
//!
//! ## Order: admit, then advance (§4.3)
//!
//! The cursor is only ever **behind**, never ahead: a caller must
//! [`EventStore::claim_firing`] (or record a skip) before
//! [`EventStore::advance_cursor`], and `advance_cursor` refuses to move
//! the cursor to a fire time that has no firing record, refuses to move
//! it backwards, and is a monotonic no-op when the slot was already
//! consumed. A crash between admit and advance therefore re-attempts a
//! slot that is already recorded (the claim loses, the retry advances);
//! it can neither skip nor duplicate one.
//!
//! ## Time columns
//!
//! Fire times are UTC RFC 3339 with an explicit `Z` and whole seconds —
//! exactly the shape `"{id}@{fire_time}"` and §2.3's example
//! (`2026-11-08T09:00:00Z`) pin. Because that form is fixed-width,
//! lexicographic order on the TEXT column *is* chronological order, so
//! the monotonic cursor guard ([`EventStore::advance_cursor`]) and the
//! observability ordering ([`EventStore::list_firings`]) are plain
//! string comparisons on both backends. Admin timestamps
//! (`admitted_at`, `finished_at`, `created_at`, `updated_at`) are
//! epoch-milliseconds integers, like every other kian row timestamp.
//!
//! The dialect SQL lives in [`crate::sqlite`] / [`crate::postgres`]; this
//! module owns the row types, validation and the dual-backend dispatch.

use kian_core::model::SessionId;

use crate::event_store::{EventStore, StoreError};

/// The state machine of one firing record (§9.1 `outcome`, narrowed to
/// the durable lifecycle this generation owns). `admitted` → `running` →
/// one of `succeeded`/`failed`; `skipped` is terminal from birth (the
/// slot was consumed without a turn — overlap, misfire, worktree
/// unavailable). The store enforces "a terminal row is never rewritten".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiringState {
    /// The slot is this scheduler's; the turn has not started yet.
    Admitted,
    /// The dsh turn is in flight (a worktree may exist).
    Running,
    /// The turn settled cleanly.
    Succeeded,
    /// The turn settled with a failure (or a timeout).
    Failed,
    /// The slot was consumed without a turn; `skip_reason` says why.
    Skipped,
}

impl FiringState {
    /// The stable machine token written to `cron_firings.state`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            FiringState::Admitted => "admitted",
            FiringState::Running => "running",
            FiringState::Succeeded => "succeeded",
            FiringState::Failed => "failed",
            FiringState::Skipped => "skipped",
        }
    }

    /// Decodes a stored token; `None` for an unknown one (a corrupt row
    /// is reported, never guessed at).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "admitted" => Some(FiringState::Admitted),
            "running" => Some(FiringState::Running),
            "succeeded" => Some(FiringState::Succeeded),
            "failed" => Some(FiringState::Failed),
            "skipped" => Some(FiringState::Skipped),
            _ => None,
        }
    }

    /// Terminal states are written once and never moved (§4.3: a settled
    /// slot is not retried in place).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            FiringState::Succeeded | FiringState::Failed | FiringState::Skipped
        )
    }
}

impl std::fmt::Display for FiringState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One declared schedule's durable row (CRON-MODEL §4.1, narrowed to the
/// columns the scheduler owns; the declaration's policies live in the
/// effective config and are re-read every tick).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronScheduleRow {
    /// `${id}` — the cursor key and log namespace (§3.1).
    pub id: String,
    /// The normalized five-field expression (aliases already rewritten,
    /// §2.2 — so `@daily` and `0 0 * * *` declare the same schedule).
    pub expr: String,
    /// The IANA zone frozen at load time (§2.3).
    pub tz: String,
    /// sha256 over the normalized declaration (expr + tz + target,
    /// §4.1). A change resets the cursor.
    pub declaration_digest: String,
    /// UTC RFC 3339 `Z` of the last **admitted** fire time; `None` before
    /// the first admission (and immediately after a redeclaration).
    pub cursor_fire_at: Option<String>,
    /// UTC RFC 3339 `Z` of the next due fire time; `None` before the
    /// next-fire computation has run.
    pub next_fire_at: Option<String>,
    /// Declared-but-off schedules are listed, never fired (§3.1).
    pub enabled: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// One firing record (CRON-MODEL §9.1, narrowed to the columns this
/// generation writes; `session_id`/`diff`/`missed`/`detail` arrive with
/// the execution wiring).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronFiringRow {
    /// `"{schedule_id}@{fire_time}"` — the idempotency key (§4.3).
    pub firing_id: String,
    pub schedule_id: String,
    /// UTC RFC 3339 `Z`, whole seconds.
    pub fire_time: String,
    /// The session the firing was dispatched to (CRON-MODEL §3.2/§9.1): the
    /// configured target while it was live/empty, or the derived firing
    /// session when the target was frozen at dispatch. Written before the
    /// admit; a failed delivery keeps the session it tried. `None` on rows
    /// recorded before v8.
    pub session: Option<String>,
    /// Epoch ms the slot was recorded (not necessarily the fire time:
    /// §5.2 keeps `fire_time` reproducible, admission may be late).
    pub admitted_at: u64,
    pub state: FiringState,
    /// Machine token for a skip (`skipped_overlap`, `superseded`,
    /// `outside_coalesce`, `policy_skip`, `queue_starved`, `disk_cap`, …).
    pub skip_reason: Option<String>,
    /// The per-firing worktree, when one was created (§6.2).
    pub worktree_path: Option<String>,
    pub finished_at: Option<u64>,
    /// Bounded, credential-redacted failure detail.
    pub error: Option<String>,
}

/// Outcome of [`EventStore::upsert_schedule`]: what this call did to the
/// row. The outcome is advisory under a racing redeclaration, while the
/// cursor-reset *state* is exact — the reset decision is made inside the
/// single UPSERT statement against the live row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleUpsert {
    /// The schedule was not in the store.
    Inserted,
    /// Same `declaration_digest`: the row (and its cursor) is unchanged
    /// apart from `enabled`/`updated_at` when the declaration's enable
    /// flag moved.
    Unchanged,
    /// A different `declaration_digest`: the row now carries the new
    /// declaration and the cursor is reset (§12.3).
    Redeclared { previous_digest: String },
}

/// Typed cron-state failures (a rejected scheduler operation is a
/// matchable fact, never an opaque string).
#[derive(Debug, thiserror::Error)]
pub enum CronError {
    /// The fire time is not canonical UTC RFC 3339 with whole seconds —
    /// the only shape the slot key and the string-ordered cursor guard
    /// can be sound on.
    #[error(
        "fire time {value:?} is not canonical UTC RFC 3339 with whole seconds (YYYY-MM-DDTHH:MM:SSZ)"
    )]
    BadFireTime { value: String },
    /// A schedule id must be a non-empty token without the `@` slot
    /// separator (`[a-z0-9][a-z0-9_-]{0,63}`, §3.1).
    #[error("schedule id {value:?} is not a valid id (empty, or contains '@' or whitespace)")]
    BadScheduleId { value: String },
    /// `firing_id` must be exactly `"{schedule_id}@{fire_time}"` (§4.3):
    /// the key is the idempotency guarantee, so it is checked, not
    /// trusted.
    #[error("firing_id {firing_id:?} is not \"{schedule_id}@{fire_time}\"")]
    FiringIdMismatch {
        firing_id: String,
        schedule_id: String,
        fire_time: String,
    },
    /// A slot record cannot be born inconsistent: a `skipped` record must
    /// carry a `skip_reason`, and a non-skipped one cannot carry one.
    #[error("firing {firing_id}: state {state} is inconsistent with skip_reason {skip_reason:?}")]
    SkipReasonMismatch {
        firing_id: String,
        state: FiringState,
        skip_reason: Option<String>,
    },
    /// No schedule with that id — an advance or finish for a schedule the
    /// store has never seen is a caller bug, reported, never created.
    #[error("no cron schedule {id:?} in the store")]
    UnknownSchedule { id: String },
    /// `advance_cursor` refused: the cursor is behind the requested fire
    /// time but no firing record exists for that slot. Admit first, then
    /// advance (§4.3) — the cursor may lag the truth, never lead it.
    #[error(
        "cursor advance for {firing_id:?} refused: the slot has no firing record (admit, then advance)"
    )]
    UnrecordedFiring { firing_id: String },
    /// A terminal settle must say when it settled.
    #[error("firing {firing_id} cannot settle as {state} without finished_at")]
    MissingFinishedAt {
        firing_id: String,
        state: FiringState,
    },
    /// `next_fire_at` must be strictly after the admitted fire time — the
    /// cursor pair is ordered by construction (§2.5 computes it).
    #[error(
        "next_fire_at {next_fire_at:?} is not after the admitted fire time {admitted_fire_at:?}"
    )]
    CursorNotAdvancing {
        admitted_fire_at: String,
        next_fire_at: String,
    },
    #[error("a stored cron row is corrupt: {reason}")]
    CorruptRow { reason: String },
    #[error("u64 field {field} overflows the column's i64")]
    IntOverflow { field: &'static str },
    #[error("sqlite/postgres error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

fn to_i64_checked(v: u64, field: &'static str) -> Result<i64, CronError> {
    i64::try_from(v).map_err(|_| CronError::IntOverflow { field })
}

/// True when `s` is exactly `YYYY-MM-DDTHH:MM:SSZ` with in-range fields.
/// No `chrono` dependency in this crate: the shape is validated directly,
/// which is all the slot key and the lexicographic cursor guard need.
fn is_canonical_fire_time(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 20 {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| b[range].iter().all(u8::is_ascii_digit);
    if !(digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && b[10] == b'T'
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19)
        && b[19] == b'Z')
    {
        return false;
    }
    let num = |range: std::ops::Range<usize>| -> u32 { s[range].parse().unwrap_or(u32::MAX) };
    let (year, month, day) = (num(0..4), num(5..7), num(8..10));
    let (hour, minute, second) = (num(11..13), num(14..16), num(17..19));
    (1000..10_000).contains(&year)
        && (1..=12).contains(&month)
        && (1..=31).contains(&day)
        && hour < 24
        && minute < 60
        // 60 is a leap second — legal in RFC 3339, and ordered after 59.
        && second <= 60
}

fn validate_fire_time(value: &str) -> Result<(), CronError> {
    if is_canonical_fire_time(value) {
        Ok(())
    } else {
        Err(CronError::BadFireTime {
            value: value.to_owned(),
        })
    }
}

fn validate_schedule_id(value: &str) -> Result<(), CronError> {
    let bad = value.is_empty()
        || value.len() > 64
        || value.contains('@')
        || value.chars().any(char::is_whitespace);
    if bad {
        Err(CronError::BadScheduleId {
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

/// The claim payload the dispatch layer hands the backend (one struct
/// instead of a ten-positional column list).
pub(crate) struct FiringInsert<'a> {
    pub firing_id: &'a str,
    pub schedule_id: &'a str,
    pub fire_time: &'a str,
    /// Normally `None` at claim time: the tick loop records the resolved
    /// session with [`EventStore::set_firing_session`] before dispatch.
    pub session: Option<&'a str>,
    pub admitted_at: i64,
    pub state: &'a str,
    pub skip_reason: Option<&'a str>,
    pub worktree_path: Option<&'a str>,
    pub finished_at: Option<i64>,
    pub error: Option<&'a str>,
}

/// The schedule payload for the idempotent upsert.
pub(crate) struct ScheduleInsert<'a> {
    pub id: &'a str,
    pub expr: &'a str,
    pub tz: &'a str,
    pub digest: &'a str,
    pub enabled: i64,
    pub now: i64,
}

/// Stored column shapes (identical on both backends; decoded here).
pub(crate) type ScheduleCols = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
    i64,
);

pub(crate) type FiringCols = (
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

fn decode_schedule(row: ScheduleCols) -> Result<CronScheduleRow, CronError> {
    let (id, expr, tz, digest, cursor, next, enabled, created_at, updated_at) = row;
    Ok(CronScheduleRow {
        id,
        expr,
        tz,
        declaration_digest: digest,
        cursor_fire_at: cursor,
        next_fire_at: next,
        enabled: enabled != 0,
        created_at: u64::try_from(created_at).map_err(|_| CronError::CorruptRow {
            reason: "cron_schedules.created_at is negative".into(),
        })?,
        updated_at: u64::try_from(updated_at).map_err(|_| CronError::CorruptRow {
            reason: "cron_schedules.updated_at is negative".into(),
        })?,
    })
}

fn decode_firing(row: FiringCols) -> Result<CronFiringRow, CronError> {
    let (
        firing_id,
        schedule_id,
        fire_time,
        admitted_at,
        state,
        skip_reason,
        worktree,
        finished,
        err,
        session,
    ) = row;
    let state = FiringState::parse(&state).ok_or_else(|| CronError::CorruptRow {
        reason: format!("cron_firings.state {state:?} is not a known state"),
    })?;
    let admitted_at = u64::try_from(admitted_at).map_err(|_| CronError::CorruptRow {
        reason: "cron_firings.admitted_at is negative".into(),
    })?;
    let finished_at = match finished {
        Some(v) => Some(u64::try_from(v).map_err(|_| CronError::CorruptRow {
            reason: "cron_firings.finished_at is negative".into(),
        })?),
        None => None,
    };
    Ok(CronFiringRow {
        firing_id,
        schedule_id,
        fire_time,
        admitted_at,
        state,
        skip_reason,
        worktree_path: worktree,
        finished_at,
        error: err,
        session,
    })
}

impl EventStore {
    /// Declares (or redeclares) one schedule — idempotent on
    /// `declaration_digest` (CRON-MODEL §4.1/§12.3).
    ///
    /// A tick calls this for every declared schedule before firing
    /// anything, so the row always reflects the effective config. Same
    /// digest ⇒ the cursor is preserved untouched (a restart, or a second
    /// process reading the same config, never resets a schedule). New
    /// digest ⇒ the row adopts the new declaration and the cursor is
    /// reset to `NULL` **in the same statement** (the reset is decided
    /// against the live row, so a race cannot resurrect a stale cursor).
    /// `enabled` is declaration-adjacent state and is always synced.
    pub async fn upsert_schedule(
        &self,
        id: &str,
        expr: &str,
        tz: &str,
        declaration_digest: &str,
        enabled: bool,
        now_ms: u64,
    ) -> Result<ScheduleUpsert, CronError> {
        validate_schedule_id(id)?;
        let payload = ScheduleInsert {
            id,
            expr,
            tz,
            digest: declaration_digest,
            enabled: i64::from(enabled),
            now: to_i64_checked(now_ms, "cron updated_at")?,
        };
        let previous = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_upsert_schedule(pool, &payload).await?,
            EventStore::Postgres(pool) => {
                crate::postgres::cron_upsert_schedule(pool, &payload).await?
            }
        };
        Ok(match previous {
            None => ScheduleUpsert::Inserted,
            Some(previous_digest) if previous_digest == declaration_digest => {
                ScheduleUpsert::Unchanged
            }
            Some(previous_digest) => ScheduleUpsert::Redeclared { previous_digest },
        })
    }

    /// Every declared schedule, id-ascending — the tick loop's `§5.2`
    /// read (it filters `enabled` and `next_fire_at <= now` itself, so
    /// the caller sees the whole declared set, including the
    /// declared-but-off ones it must still list).
    pub async fn load_schedules(&self) -> Result<Vec<CronScheduleRow>, CronError> {
        let rows = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_load_schedules(pool).await?,
            EventStore::Postgres(pool) => crate::postgres::cron_load_schedules(pool).await?,
        };
        rows.into_iter().map(decode_schedule).collect()
    }

    /// One schedule's durable row, if declared.
    pub async fn load_schedule(&self, id: &str) -> Result<Option<CronScheduleRow>, CronError> {
        let row = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_load_schedule(pool, id).await?,
            EventStore::Postgres(pool) => crate::postgres::cron_load_schedule(pool, id).await?,
        };
        row.map(decode_schedule).transpose()
    }

    /// Removes one schedule's durable row (`kian cron rm`, CRON-MODEL
    /// the CLI section).
    ///
    /// The config declaration stays the source of truth: this deletes the
    /// row, it does not edit `superagent.cron`, and the next tick that sees
    /// the declaration again re-creates the row. Firing records are
    /// history and are kept for `kian cron logs`. `false` = there was no
    /// such row (nothing removed, nothing invented).
    pub async fn delete_schedule(&self, id: &str) -> Result<bool, CronError> {
        validate_schedule_id(id)?;
        let removed = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_delete_schedule(pool, id).await?,
            EventStore::Postgres(pool) => crate::postgres::cron_delete_schedule(pool, id).await?,
        };
        Ok(removed)
    }

    /// Claims one slot — the single-writer/idempotency rule of §4.2/§4.3
    /// as a store primitive.
    ///
    /// `true` = this caller won: the firing record is now durable and the
    /// caller owns the slot (run the turn, or, with `state: Skipped`,
    /// record why it did not). `false` = the slot was already recorded —
    /// by this same process before a crash, or by a racing claim — so the
    /// caller does *nothing* to the slot but may still advance the cursor
    /// (§4.3: an existing `firing_id` "is a no-op that still counts as
    /// already fired"). The insert is one `INSERT … ON CONFLICT DO
    /// NOTHING` statement, so exactly one of any number of racers wins on
    /// both backends, and a replayed claim can never duplicate a slot.
    pub async fn claim_firing(&self, firing: &CronFiringRow) -> Result<bool, CronError> {
        let expected = format!("{}@{}", firing.schedule_id, firing.fire_time);
        if firing.firing_id != expected {
            return Err(CronError::FiringIdMismatch {
                firing_id: firing.firing_id.clone(),
                schedule_id: firing.schedule_id.clone(),
                fire_time: firing.fire_time.clone(),
            });
        }
        validate_schedule_id(&firing.schedule_id)?;
        validate_fire_time(&firing.fire_time)?;
        let skipped = firing.state == FiringState::Skipped;
        if skipped != firing.skip_reason.is_some() {
            return Err(CronError::SkipReasonMismatch {
                firing_id: firing.firing_id.clone(),
                state: firing.state,
                skip_reason: firing.skip_reason.clone(),
            });
        }
        let payload = FiringInsert {
            firing_id: &firing.firing_id,
            schedule_id: &firing.schedule_id,
            fire_time: &firing.fire_time,
            session: firing.session.as_deref(),
            admitted_at: to_i64_checked(firing.admitted_at, "cron admitted_at")?,
            state: firing.state.as_str(),
            skip_reason: firing.skip_reason.as_deref(),
            worktree_path: firing.worktree_path.as_deref(),
            finished_at: firing
                .finished_at
                .map(|v| to_i64_checked(v, "cron finished_at"))
                .transpose()?,
            error: firing.error.as_deref(),
        };
        match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_claim_firing(pool, &payload).await,
            EventStore::Postgres(pool) => crate::postgres::cron_claim_firing(pool, &payload).await,
        }
    }

    /// Records the session a claimed firing is being dispatched to
    /// (CRON-MODEL §3.2/§9.1). The tick loop writes it **before** dispatch so a
    /// concurrently sweeping replica reads where the firing runs instead of
    /// inferring it from the configured target; the settlement sweep then
    /// reads exactly that stream. Idempotent (`UPDATE … WHERE state IN
    /// ('admitted','running')`): `false` = the row already settled or is
    /// absent (a settlement race), never an error.
    pub async fn set_firing_session(
        &self,
        firing_id: &str,
        session: SessionId,
    ) -> Result<bool, CronError> {
        let session = session.0.to_string();
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::cron_set_firing_session(pool, firing_id, &session).await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::cron_set_firing_session(pool, firing_id, &session).await
            }
        }
    }

    /// Moves a firing to its next state (or settles it).
    ///
    /// `true` = the transition landed. `false` = the row is already
    /// terminal (or absent), i.e. this settle is a replay: a crash
    /// between "the turn ended" and "the record says so" re-settles into
    /// an idempotent no-op instead of rewriting a finished slot. The
    /// `finished_at` of a non-terminal `running` write is left `NULL`, so
    /// "started, still going" stays distinguishable from "settled".
    pub async fn finish_firing(
        &self,
        firing_id: &str,
        state: FiringState,
        finished_at: Option<u64>,
        worktree_path: Option<&str>,
        skip_reason: Option<&str>,
        error: Option<&str>,
    ) -> Result<bool, CronError> {
        if state.is_terminal() && finished_at.is_none() {
            return Err(CronError::MissingFinishedAt {
                firing_id: firing_id.to_owned(),
                state,
            });
        }
        let finished_at = finished_at
            .map(|v| to_i64_checked(v, "cron finished_at"))
            .transpose()?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::cron_finish_firing(
                    pool,
                    firing_id,
                    state.as_str(),
                    finished_at,
                    worktree_path,
                    skip_reason,
                    error,
                )
                .await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::cron_finish_firing(
                    pool,
                    firing_id,
                    state.as_str(),
                    finished_at,
                    worktree_path,
                    skip_reason,
                    error,
                )
                .await
            }
        }
    }

    /// Advances a schedule's cursor to `admitted_fire_at` — **after** the
    /// slot was recorded (§4.3: admit, then advance).
    ///
    /// The move is conditional in a single statement: the cursor only
    /// moves when it is `NULL` or strictly behind `admitted_fire_at`, and
    /// only when a firing record exists for that slot. So
    ///
    /// - `true` = the cursor moved forward to the slot;
    /// - `false` = the cursor is already at/after it — a replayed advance
    ///   (a crash between admit and advance, or two ticks racing on the
    ///   same slot) is an idempotent no-op, and the cursor can **never**
    ///   move backwards or skip past an unrecorded slot.
    ///
    /// `next_fire_at` is written with the advance: it is the next
    /// computed fire time *after* `admitted_fire_at` (the caller computes
    /// it, §2.5 — the store owns ordering, never calendar math).
    pub async fn advance_cursor(
        &self,
        id: &str,
        admitted_fire_at: &str,
        next_fire_at: &str,
        now_ms: u64,
    ) -> Result<bool, CronError> {
        validate_schedule_id(id)?;
        validate_fire_time(admitted_fire_at)?;
        validate_fire_time(next_fire_at)?;
        let next_fire_at = if next_fire_at > admitted_fire_at {
            next_fire_at
        } else {
            return Err(CronError::CursorNotAdvancing {
                admitted_fire_at: admitted_fire_at.to_owned(),
                next_fire_at: next_fire_at.to_owned(),
            });
        };
        let firing_id = format!("{id}@{admitted_fire_at}");
        let now = to_i64_checked(now_ms, "cron updated_at")?;
        match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::cron_advance_cursor(
                    pool,
                    id,
                    &firing_id,
                    admitted_fire_at,
                    next_fire_at,
                    now,
                )
                .await
            }
            EventStore::Postgres(pool) => {
                crate::postgres::cron_advance_cursor(
                    pool,
                    id,
                    &firing_id,
                    admitted_fire_at,
                    next_fire_at,
                    now,
                )
                .await
            }
        }
    }

    /// Firing records for observability — the `kian cron logs` read
    /// (§9.1/§9.3), newest slot first. `schedule_id: None` lists every
    /// schedule's slots (the global "what has the scheduler been doing"
    /// view). `limit` bounds the read (a `--limit`-shaped argument); the
    /// caller renders, the store does not paginate.
    pub async fn list_firings(
        &self,
        schedule_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<CronFiringRow>, CronError> {
        if let Some(id) = schedule_id {
            validate_schedule_id(id)?;
        }
        let limit = to_i64_checked(limit, "cron list_firings limit")?;
        let rows = match self {
            EventStore::Sqlite(pool) => {
                crate::sqlite::cron_list_firings(pool, schedule_id, limit).await?
            }
            EventStore::Postgres(pool) => {
                crate::postgres::cron_list_firings(pool, schedule_id, limit).await?
            }
        };
        rows.into_iter().map(decode_firing).collect()
    }

    /// The one firing record of a slot, if it was recorded.
    pub async fn load_firing(&self, firing_id: &str) -> Result<Option<CronFiringRow>, CronError> {
        let rows = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_load_firing(pool, firing_id).await?,
            EventStore::Postgres(pool) => {
                crate::postgres::cron_load_firing(pool, firing_id).await?
            }
        };
        rows.map(decode_firing).transpose()
    }

    /// Every still-open (`admitted`/`running`) firing of one schedule, oldest
    /// slot first — the settlement sweep's read (CRON-MODEL §9.1). Unlike
    /// [`EventStore::list_firings`] (the newest-N observability page) this is
    /// not bounded by recency: a firing the wedge left stranded sits behind
    /// arbitrarily many later settled slots and must still be found.
    pub async fn open_firings(&self, schedule_id: &str) -> Result<Vec<CronFiringRow>, CronError> {
        validate_schedule_id(schedule_id)?;
        let rows = match self {
            EventStore::Sqlite(pool) => crate::sqlite::cron_open_firings(pool, schedule_id).await?,
            EventStore::Postgres(pool) => {
                crate::postgres::cron_open_firings(pool, schedule_id).await?
            }
        };
        rows.into_iter().map(decode_firing).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::{CronFiringRow, FiringState, ScheduleUpsert};
    use crate::EventStore;
    use kian_core::model::SessionId;

    const T0: u64 = 1_761_900_000_000;

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-cron-test-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    async fn seed_schedule(store: &EventStore, id: &str, digest: &str) {
        store
            .upsert_schedule(id, "*/5 * * * *", "UTC", digest, true, T0)
            .await
            .expect("upsert schedule");
    }

    fn firing(schedule_id: &str, fire_time: &str) -> CronFiringRow {
        CronFiringRow {
            firing_id: format!("{schedule_id}@{fire_time}"),
            schedule_id: schedule_id.to_owned(),
            fire_time: fire_time.to_owned(),
            session: None,
            admitted_at: T0,
            state: FiringState::Admitted,
            skip_reason: None,
            worktree_path: None,
            finished_at: None,
            error: None,
        }
    }

    /// §4.3 idempotency as a concurrency rule: two callers racing the
    /// same slot — one wins the insert, the other learns the slot is
    /// taken. Never two winners, never zero.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_claims_admit_exactly_one_winner() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        seed_schedule(&store, "night-audit", "d1").await;
        let slot = firing("night-audit", "2026-11-09T02:30:00Z");

        let mut handles = Vec::new();
        for _ in 0..4 {
            // A clone shares the same pool: every racer hits the same file.
            let store = store.clone();
            let slot = slot.clone();
            handles.push(tokio::spawn(async move {
                store.claim_firing(&slot).await.expect("claim")
            }));
        }
        let mut winners = 0;
        for h in handles {
            if h.await.expect("join") {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one racer may own the slot");

        // The loser's replay is still an idempotent no-op, and the slot
        // is visible exactly once.
        assert!(!store.claim_firing(&slot).await.expect("replay claim"));
        let rows = store
            .list_firings(Some("night-audit"), 10)
            .await
            .expect("list");
        assert_eq!(rows.len(), 1, "one row per slot");
        assert_eq!(rows[0].state, FiringState::Admitted);

        store.close().await;
        cleanup(&path);
    }

    /// CRON-MODEL §3.2/§9.1 (v8): the firing records where it actually runs.
    /// A claim starts with no session (the pre-v8 / legacy shape), the tick
    /// records the resolved session before dispatch, and the guarded write
    /// never rewrites a settled row.
    #[tokio::test]
    async fn set_firing_session_round_trips_and_never_touches_a_settled_row() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        seed_schedule(&store, "night-audit", "d1").await;
        let slot = firing("night-audit", "2026-11-09T02:30:00Z");
        assert!(store.claim_firing(&slot).await.expect("claim"));
        let loaded = store
            .load_firing(&slot.firing_id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(loaded.session, None, "a claim records no session yet");

        let session = SessionId::new();
        let session_text = session.0.to_string();
        assert!(
            store
                .set_firing_session(&slot.firing_id, session)
                .await
                .expect("record session")
        );
        let loaded = store
            .load_firing(&slot.firing_id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(loaded.session.as_deref(), Some(session_text.as_str()));
        let listed = store
            .list_firings(Some("night-audit"), 10)
            .await
            .expect("list");
        assert_eq!(listed[0].session.as_deref(), Some(session_text.as_str()));
        let open = store.open_firings("night-audit").await.expect("open");
        assert_eq!(open[0].session.as_deref(), Some(session_text.as_str()));

        // A settled row is frozen: a late session write is a guarded no-op.
        assert!(
            store
                .finish_firing(
                    &slot.firing_id,
                    FiringState::Succeeded,
                    Some(T0 + 1_000),
                    None,
                    None,
                    None
                )
                .await
                .expect("settle")
        );
        assert!(
            !store
                .set_firing_session(&slot.firing_id, SessionId::new())
                .await
                .expect("late record is a no-op")
        );
        let loaded = store
            .load_firing(&slot.firing_id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(
            loaded.session.as_deref(),
            Some(session_text.as_str()),
            "a settled row's session is never rewritten"
        );
        store.close().await;
        cleanup(&path);
    }

    /// The cursor is monotonic: it moves forward once per consumed slot
    /// and a late/duplicate advance never drags it back (§4.3 "the cursor
    /// is only ever behind, never ahead").
    #[tokio::test]
    async fn advance_cursor_is_monotonic_and_never_moves_backwards() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        seed_schedule(&store, "night-audit", "d1").await;

        for slot in ["2026-11-09T02:30:00Z", "2026-11-09T02:35:00Z"] {
            assert!(
                store
                    .claim_firing(&firing("night-audit", slot))
                    .await
                    .expect("claim")
            );
        }

        assert!(
            store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:30:00Z",
                    "2026-11-09T02:35:00Z",
                    T0
                )
                .await
                .expect("advance")
        );
        let row = store
            .load_schedule("night-audit")
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));
        assert_eq!(row.next_fire_at.as_deref(), Some("2026-11-09T02:35:00Z"));

        // A replayed advance of the consumed slot is a no-op…
        assert!(
            !store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:30:00Z",
                    "2026-11-09T02:35:00Z",
                    T0 + 1
                )
                .await
                .expect("replay advance")
        );
        // …and an older slot can never drag the cursor back.
        assert!(
            !store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:25:00Z",
                    "2026-11-09T02:35:00Z",
                    T0 + 2
                )
                .await
                .expect("backwards advance is a no-op")
        );
        let row = store
            .load_schedule("night-audit")
            .await
            .expect("load")
            .expect("row");
        assert_eq!(
            row.cursor_fire_at.as_deref(),
            Some("2026-11-09T02:30:00Z"),
            "the cursor never moves backwards"
        );

        // Forward again: the next recorded slot advances.
        assert!(
            store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:35:00Z",
                    "2026-11-09T02:40:00Z",
                    T0 + 3
                )
                .await
                .expect("advance forward")
        );

        // Admit-then-advance is enforced: an unrecorded slot is refused.
        let err = store
            .advance_cursor(
                "night-audit",
                "2026-11-09T02:45:00Z",
                "2026-11-09T02:50:00Z",
                T0 + 4,
            )
            .await
            .expect_err("unrecorded slot");
        assert!(
            matches!(err, super::CronError::UnrecordedFiring { .. }),
            "got {err:?}"
        );

        store.close().await;
        cleanup(&path);
    }

    /// Restart safety: the cursor is durable, so a reopened store reads
    /// the same cursor and re-advancing the same slot is still a no-op
    /// (§7.4 "never re-derive a fire time from the wall clock").
    #[tokio::test]
    async fn cursor_and_firings_survive_a_store_reopen() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        seed_schedule(&store, "night-audit", "d1").await;
        assert!(
            store
                .claim_firing(&firing("night-audit", "2026-11-09T02:30:00Z"))
                .await
                .expect("claim")
        );
        assert!(
            store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:30:00Z",
                    "2026-11-09T02:35:00Z",
                    T0
                )
                .await
                .expect("advance")
        );
        store.close().await;

        let reopened = EventStore::open(&path).await.expect("reopen");
        let row = reopened
            .load_schedule("night-audit")
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));
        assert_eq!(row.next_fire_at.as_deref(), Some("2026-11-09T02:35:00Z"));
        assert_eq!(
            row.created_at, T0,
            "a redeclare of the same digest is a no-op"
        );
        // The slot record survived too, and the same slot cannot be
        // claimed again after a restart (§7.4: no double-fire).
        assert!(
            !reopened
                .claim_firing(&firing("night-audit", "2026-11-09T02:30:00Z"))
                .await
                .expect("replay claim")
        );
        // Nor re-advanced.
        assert!(
            !reopened
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:30:00Z",
                    "2026-11-09T02:35:00Z",
                    T0
                )
                .await
                .expect("replay advance")
        );
        // Redeclaring the same declaration leaves the cursor alone.
        assert_eq!(
            reopened
                .upsert_schedule("night-audit", "*/5 * * * *", "UTC", "d1", true, T0 + 10)
                .await
                .expect("re-upsert"),
            ScheduleUpsert::Unchanged
        );
        let row = reopened
            .load_schedule("night-audit")
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));

        reopened.close().await;
        cleanup(&path);
    }

    /// The declaration lifecycle: insert, unchanged, redeclared (with a
    /// visible cursor reset, §12.3), and the enabled flag synced.
    #[tokio::test]
    async fn upsert_schedule_is_idempotent_and_resets_on_digest_change() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");

        assert_eq!(
            store
                .upsert_schedule("night-audit", "@daily", "UTC", "d1", true, T0)
                .await
                .expect("insert"),
            ScheduleUpsert::Inserted
        );
        assert!(
            store
                .claim_firing(&firing("night-audit", "2026-11-09T02:30:00Z"))
                .await
                .expect("claim")
        );
        assert!(
            store
                .advance_cursor(
                    "night-audit",
                    "2026-11-09T02:30:00Z",
                    "2026-11-10T02:30:00Z",
                    T0
                )
                .await
                .expect("advance")
        );
        // Same digest, enable flag flipped: cursor kept, enabled synced.
        assert_eq!(
            store
                .upsert_schedule("night-audit", "@daily", "UTC", "d1", false, T0 + 1)
                .await
                .expect("toggle"),
            ScheduleUpsert::Unchanged
        );
        let row = store.load_schedule("night-audit").await.unwrap().unwrap();
        assert!(!row.enabled);
        assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));

        // New digest: the cursor resets visibly.
        assert_eq!(
            store
                .upsert_schedule("night-audit", "0 3 * * *", "UTC", "d2", true, T0 + 2)
                .await
                .expect("redeclare"),
            ScheduleUpsert::Redeclared {
                previous_digest: "d1".into()
            }
        );
        let row = store.load_schedule("night-audit").await.unwrap().unwrap();
        assert_eq!(row.expr, "0 3 * * *");
        assert_eq!(row.declaration_digest, "d2");
        assert_eq!(
            row.cursor_fire_at, None,
            "a redeclaration resets the cursor"
        );
        assert_eq!(row.next_fire_at, None);
        assert_eq!(row.created_at, T0, "created_at is the creation");
        assert_eq!(row.updated_at, T0 + 2);

        // `load_schedules` lists the declared set, id-ascending.
        store
            .upsert_schedule(
                "weekly-report",
                "0 0 * * 0",
                "Europe/Berlin",
                "w1",
                true,
                T0,
            )
            .await
            .expect("second schedule");
        let all = store.load_schedules().await.expect("load all");
        let ids: Vec<&str> = all.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["night-audit", "weekly-report"]);

        store.close().await;
        cleanup(&path);
    }

    /// A settle lands once; a replayed settle and a settle of a skipped
    /// slot never rewrite history (§9.1, §5.5 "never retried in place").
    #[tokio::test]
    async fn finish_firing_settles_once_and_keeps_skip_records_immutable() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        seed_schedule(&store, "night-audit", "d1").await;

        let slot = firing("night-audit", "2026-11-09T02:30:00Z");
        assert!(store.claim_firing(&slot).await.expect("claim"));
        assert!(
            store
                .finish_firing(
                    &slot.firing_id,
                    FiringState::Running,
                    None,
                    Some("/tmp/wt/run-1"),
                    None,
                    None
                )
                .await
                .expect("running")
        );
        let row = store
            .load_firing(&slot.firing_id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.state, FiringState::Running);
        assert_eq!(row.worktree_path.as_deref(), Some("/tmp/wt/run-1"));
        assert_eq!(row.finished_at, None);

        assert!(
            store
                .finish_firing(
                    &slot.firing_id,
                    FiringState::Failed,
                    Some(T0 + 60_000),
                    None,
                    None,
                    Some("turn timed out")
                )
                .await
                .expect("settle")
        );
        // A replayed settle is an idempotent no-op — the terminal row is
        // never rewritten.
        assert!(
            !store
                .finish_firing(
                    &slot.firing_id,
                    FiringState::Succeeded,
                    Some(T0 + 99_000),
                    None,
                    None,
                    None
                )
                .await
                .expect("replay settle")
        );
        let row = store
            .load_firing(&slot.firing_id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.state, FiringState::Failed);
        assert_eq!(row.error.as_deref(), Some("turn timed out"));
        assert_eq!(row.finished_at, Some(T0 + 60_000));
        assert_eq!(row.worktree_path.as_deref(), Some("/tmp/wt/run-1"));

        // A skipped slot is born terminal with its reason, and settles
        // nowhere.
        let skipped = CronFiringRow {
            state: FiringState::Skipped,
            skip_reason: Some("skipped_overlap".into()),
            finished_at: Some(T0),
            ..firing("night-audit", "2026-11-09T02:35:00Z")
        };
        assert!(store.claim_firing(&skipped).await.expect("skip claim"));
        assert!(
            !store
                .finish_firing(
                    &skipped.firing_id,
                    FiringState::Succeeded,
                    Some(T0 + 1),
                    None,
                    None,
                    None
                )
                .await
                .expect("settle a skip")
        );

        // Observability read: newest slot first, both states visible.
        let rows = store.list_firings(None, 10).await.expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].fire_time, "2026-11-09T02:35:00Z");
        assert_eq!(rows[0].skip_reason.as_deref(), Some("skipped_overlap"));
        assert_eq!(rows[1].state, FiringState::Failed);

        store.close().await;
        cleanup(&path);
    }
}
