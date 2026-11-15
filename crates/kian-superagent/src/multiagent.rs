//! Pure multiagent core: identity, vocabulary, limits, reconciliation.
//!
//! The P1 (delegation core) pure functions of
//! `design/MULTIAGENT-MODEL.md`, implemented per the R15 plan's pins
//! (`round15/plan.md` §0.2): every item here is a pure function of
//! committed events and explicit arguments — no I/O, no port, no
//! `Arc<dyn …>`, no ambient config. The impulsive consumers (spawn
//! side, join, caps, timeout tick) live in other modules and call into
//! this one, so the decisions below are the single source every phase
//! of the derivation agrees with.
//!
//! Pin map (plan §0.2, binding):
//! - **P1** — the three identity derivations ([`spawn_uid`],
//!   [`child_thread_id`], [`child_session_id`]): derived, never minted,
//!   so a replay of the same committed `tool/call` recomputes the same
//!   child (M4) and lineage is findable by derivation, not by scanning.
//! - **P3** — idempotency keys: the report key is `spawn:<spawn_uid>`
//!   for a wrapper carrying a `spawn_uid`, else the landed legacy
//!   `thread-report:<thread_id>`; the task-input key stays
//!   `thread:<thread_id>` unchanged. The legacy fallback is the
//!   migration hinge: an upgraded child re-admitting a pre-upgrade
//!   completion keeps its own key, so no migration and no double admit.
//! - **P6** — the class-C report keys (`spawn_uid`, `task_id`,
//!   `contract` iff a gate verdict was committed).
//! - **P7** — `task_id` equals the derived thread id (no `task_key`
//!   before the P2 roster).
//! - **P8** — the outcome derivation table, row for row (§6.2).

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::worker::{
    ReportState, ThreadLiveness, ThreadReport, WORKER_GATE_NAME, thread_report_content,
};

/// The `turn/end` error class of an empty model response (constraint 7;
/// the loop's constant, `kian-loop/src/react.rs` `CLASS_EMPTY_RESPONSE`).
pub const EMPTY_RESPONSE_CLASS: &str = "empty_response";

/// The `turn/end` error class of an exhausted budget/turn/token bound
/// (the classifier's spelling, `kian-orchestrator/src/recovery.rs`).
pub const BUDGET_EXHAUSTED_CLASS: &str = "budget_exhausted";

/// The `control/interrupt` requester the child inactivity bound stamps
/// (§5.3:587) — the one requester that derives [`TaskOutcome::Stalled`]
/// rather than [`TaskOutcome::Interrupted`] (§6.2 rows 6-7, M6).
pub const CHILD_TIMEOUT_REQUESTED_BY: &str = "child_timeout";

// ---------------------------------------------------------------------------
// Identity (pins P1, P7)
// ---------------------------------------------------------------------------

/// The derived identity of one spawn *request* (pin P1):
/// `uuid5(parent_session, "kian:spawn:<seq>:<index>")`, where `seq` is
/// the committed seq of the `tool/call` that requested the spawn and
/// `index` is the 0-based position of the request inside that call
/// (always `0` in P1 — §1:165-166 — the parameter exists so batching
/// later does not change the derivation). Derived, never minted: the
/// same discipline as the derived event id (EVENT-MODEL §4.4).
#[must_use]
pub fn spawn_uid(parent: SessionId, seq: u64, index: u32) -> Uuid {
    Uuid::new_v5(&parent.0, format!("kian:spawn:{seq}:{index}").as_bytes())
}

/// The child thread id derived from the spawn identity (pin P1):
/// `uuid5(parent_session, "kian:thread:<spawn_uid>")` (§2.1:229), so
/// lineage is findable by derivation and a second spawn of the same
/// committed call cannot produce a different child.
#[must_use]
pub fn child_thread_id(parent: SessionId, spawn_uid: Uuid) -> Uuid {
    Uuid::new_v5(&parent.0, format!("kian:thread:{spawn_uid}").as_bytes())
}

/// The child session id derived from the thread id (pin P1):
/// `SessionId(uuid5(&thread_id, "kian:session"))` — the shape mirrors
/// the landed derived-session-id precedent (`rearm.rs`).
#[must_use]
pub fn child_session_id(thread_id: Uuid) -> SessionId {
    SessionId(Uuid::new_v5(&thread_id, "kian:session".as_bytes()))
}

/// The durable task identity of one delegation (§2.1.1: the wrapper's
/// class-B bytes record the resolution so a replay rehydrates rather
/// than re-consults live config).
///
/// P2 (pin P2-8) splits the two ids that P1 conflated: `thread_id` is
/// **always** `child_thread_id(parent, spawn_uid)` (the child's own
/// event stream), while `task_id` is `uuid5(parent,
/// "kian:task:<task_key>")` **when a `task_key` was given**, else it
/// stays equal to `thread_id` (the P1 no-`task_key` case, pin P7).
/// Without the split, two spawns sharing a `task_key` would collide on
/// one thread id — a duplicate-child bug, not a style choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnIdentity {
    /// The derived spawn-request identity ([`spawn_uid`]).
    pub spawn_uid: Uuid,
    /// The derived child thread id — the stream the child's own
    /// events land on (pin P2-8: taken from here in `prepare`,
    /// never from `task_id`).
    pub thread_id: Uuid,
    /// The durable task identity: `task_id_for_key(parent, task_key)`
    /// when a `task_key` was given, else equal to `thread_id` (pin
    /// P7) — the id that survives attempts (§1:169-176).
    pub task_id: Uuid,
    /// The `AgentDef` reference the spawn resolved to (recorded, never
    /// re-resolved — §2.1.1). With the roster (pin P2-1) this is the
    /// **resolved entry's** `AgentDef` id, not the worker template.
    pub agent_def: String,
    /// The version of `agent_def` at resolution time.
    pub agent_def_version: u64,
    /// The roster entry id the spawn resolved through (§2.1.1:264-269,
    /// the wrapper's `assignee` key). `None` on the P1 form — the
    /// roster does not exist for that spawn.
    pub assignee: Option<String>,
}

impl SpawnIdentity {
    /// Derives the identity from the parent session and the requesting
    /// `tool/call`'s own committed `seq` (pin P2 supplies that seq).
    /// The spawn index is always `0` in P1 (§1:165-166). This is the
    /// P1 form: no `task_key` (so `task_id == thread_id`, pin P7) and
    /// no roster resolution (`assignee` absent).
    #[must_use]
    pub fn derive(parent: SessionId, seq: u64, agent_def: &str, agent_def_version: u64) -> Self {
        Self::derive_delegated(parent, seq, agent_def, agent_def_version, None, None)
    }

    /// (R17-C2, §7.3/pin P2-8) the roster form: the same derivation
    /// plus the `task_key` (parent-scoped — the derivation itself is
    /// what scopes it) and the resolved roster entry id that rides
    /// the wrapper as `assignee` (pin P2-1: `agent_def` /
    /// `agent_def_version` carry the **resolved entry's** `AgentDef`,
    /// never the worker template, when an `agent` was passed).
    #[must_use]
    pub fn derive_delegated(
        parent: SessionId,
        seq: u64,
        agent_def: &str,
        agent_def_version: u64,
        task_key: Option<&str>,
        assignee: Option<String>,
    ) -> Self {
        let uid = spawn_uid(parent, seq, 0);
        let thread_id = child_thread_id(parent, uid);
        let task_id = match task_key {
            Some(key) => task_id_for_key(parent, key),
            None => thread_id,
        };
        Self {
            spawn_uid: uid,
            thread_id,
            task_id,
            agent_def: agent_def.to_string(),
            agent_def_version,
            assignee,
        }
    }

    /// The `task_id` wire form (a uuid string).
    #[must_use]
    pub fn task_id_str(&self) -> String {
        self.task_id.to_string()
    }
}

/// The durable task identity of a `task_key` (pin P2-8 / §1:169-176):
/// `uuid5(parent_session, "kian:task:<task_key>")` — parent-scoped by
/// construction (two parents using the same key derive different
/// tasks), and stable across attempts (the same key re-derives the
/// same task id, so a retry is a new attempt of one task, not a new
/// task). P4's readiness resolution uses the same derivation.
#[must_use]
pub fn task_id_for_key(parent: SessionId, task_key: &str) -> Uuid {
    Uuid::new_v5(&parent.0, format!("kian:task:{task_key}").as_bytes())
}

// ---------------------------------------------------------------------------
// Idempotency keys (pin P3)
// ---------------------------------------------------------------------------

/// The P1 report key: `spawn:<spawn_uid>` — 1:1 with the legacy
/// [`legacy_report_key`] under derived ids (§3.2:400-402), so a legacy
/// row and a new row can never double-admit the same completion and an
/// upgrade needs no migration.
#[must_use]
pub fn report_key(identity: &SpawnIdentity) -> String {
    format!("spawn:{}", identity.spawn_uid)
}

/// The landed legacy report key (`worker.rs`): `thread-report:<thread_id>`.
#[must_use]
pub fn legacy_report_key(thread_id: Uuid) -> String {
    format!("thread-report:{thread_id}")
}

/// The landed task-input key (pin P3: unchanged): `thread:<thread_id>`.
/// Changing it would risk a double-admit of a task row on upgrade, so
/// it stays byte-identical to `worker.rs`'s private helper.
#[must_use]
pub fn task_input_key(thread_id: Uuid) -> String {
    format!("thread:{thread_id}")
}

/// The report key for a wrapper: `spawn:<spawn_uid>` when the wrapper
/// carries a `spawn_uid` string, else the legacy key (pin P3).
///
/// **This is the migration-safety hinge**: a legacy child re-admitting
/// a completion after the upgrade keeps its own (legacy) key — the two
/// key families are 1:1 under derived ids and never collide.
#[must_use]
pub fn report_key_for(wrapper: &Value, thread_id: Uuid) -> String {
    match wrapper.get("spawn_uid").and_then(Value::as_str) {
        Some(uid) => format!("spawn:{uid}"),
        None => legacy_report_key(thread_id),
    }
}

// ---------------------------------------------------------------------------
// The task outcome vocabulary (§6.1.1, pin P8)
// ---------------------------------------------------------------------------

/// The closed nine-value task-outcome vocabulary (§6.1.1:620-621).
/// Derived in the join from the child's own committed events — never
/// typed by the child, never persisted before P4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskOutcome {
    /// Normal end, the evidence gate passed (§6.2 row 1).
    CompletedVerified,
    /// Normal end with no gate verdict (§6.2 row 2).
    CompletedUnverified,
    /// Contract miss against a declared `output_schema` — reachable
    /// from R17-C3 (P2) **only** through a committed
    /// `evidence/verdict{gate:"output_schema",passed:false}` beside a
    /// `thread/completed{failed}` (pin P2-5); before that verdict
    /// existed the value was structurally unreachable, and it is
    /// still never typed by the child nor inferred from prose.
    InvalidOutput,
    /// Empty final answer despite tool work — a soft failure, never a
    /// success at any policy setting (M6, §6.1.1:635-638).
    EmptyOutput,
    /// Budget/turn/token exhausted (§6.2 row 5).
    BudgetExhausted,
    /// Interrupted by the child inactivity bound — a timing fact,
    /// distinct from `Failed` (§6.2 row 6, M6).
    Stalled,
    /// Explicit stop by a person or the parent (§6.2 row 7).
    Interrupted,
    /// A terminal failure no softer carrier names.
    Failed,
    /// The child waits on a decision only the parent can give — a live
    /// child, never a terminal one (§6.2 row 9, §6.2.1).
    InputRequired,
}

impl TaskOutcome {
    /// Every value, in the contract's §6.1.1 order.
    pub const ALL: [TaskOutcome; 9] = [
        TaskOutcome::CompletedVerified,
        TaskOutcome::CompletedUnverified,
        TaskOutcome::InvalidOutput,
        TaskOutcome::EmptyOutput,
        TaskOutcome::BudgetExhausted,
        TaskOutcome::Stalled,
        TaskOutcome::Interrupted,
        TaskOutcome::Failed,
        TaskOutcome::InputRequired,
    ];

    /// The exact §6.1.1 spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TaskOutcome::CompletedVerified => "completed_verified",
            TaskOutcome::CompletedUnverified => "completed_unverified",
            TaskOutcome::InvalidOutput => "invalid_output",
            TaskOutcome::EmptyOutput => "empty_output",
            TaskOutcome::BudgetExhausted => "budget_exhausted",
            TaskOutcome::Stalled => "stalled",
            TaskOutcome::Interrupted => "interrupted",
            TaskOutcome::Failed => "failed",
            TaskOutcome::InputRequired => "input_required",
        }
    }

    /// Parses a wire spelling; unknown values are `None` (a future
    /// vocabulary value never breaks a legacy reader).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        TaskOutcome::ALL.into_iter().find(|v| v.as_str() == raw)
    }

    /// Success membership. The base set is exactly
    /// [`SUCCESS_SET`]; policy may admit `CompletedUnverified`
    /// (`accept_unverified`), and **`EmptyOutput` is never a member at
    /// any policy setting** (M6, §6.1.1:635-638).
    #[must_use]
    pub fn is_success(self, accept_unverified: bool) -> bool {
        match self {
            TaskOutcome::CompletedVerified => true,
            TaskOutcome::CompletedUnverified => accept_unverified,
            // Excluded from the success set at every policy setting —
            // the honesty test of §6.2 row 3.
            TaskOutcome::EmptyOutput => false,
            _ => false,
        }
    }

    /// (R17-C6, pin P4-3) Whether the value has a
    /// `thread/completed` carrier — the six completion-borne values
    /// of §6.1.1:623-633 (all but `stalled`, `interrupted` and
    /// `input_required`). Exactly these are writable by
    /// [`outcome_for_completion`]; the other three stay derived on
    /// demand.
    #[must_use]
    pub fn is_completion_borne(self) -> bool {
        !matches!(
            self,
            TaskOutcome::Stalled | TaskOutcome::Interrupted | TaskOutcome::InputRequired
        )
    }
}

/// The success set: exactly `CompletedVerified` (§6.1.1). Policy may
/// admit `CompletedUnverified` via [`TaskOutcome::is_success`];
/// `EmptyOutput` is never a member.
pub const SUCCESS_SET: [TaskOutcome; 1] = [TaskOutcome::CompletedVerified];

// ---------------------------------------------------------------------------
// Outcome derivation (pin P8, §6.2 row for row)
// ---------------------------------------------------------------------------

/// The last `turn/end` error class on the stream, if any (the loop's
/// audit object: `bytes.error.class`, `react.rs`).
fn last_error_class(events: &[Event]) -> Option<&str> {
    events.iter().rev().find_map(|ev| match ev.kind {
        EventKind::TurnEnd => ev
            .bytes
            .get("error")
            .and_then(|e| e.get("class"))
            .and_then(Value::as_str),
        _ => None,
    })
}

/// The last `evidence/verdict` committed by the worker gate
/// (`gate == "worker"`), if any. The builtin loop gate never counts.
fn last_worker_verdict(events: &[Event]) -> Option<&Event> {
    events.iter().rev().find(|ev| {
        ev.kind == EventKind::EvidenceVerdict
            && ev.bytes.get("gate").and_then(Value::as_str) == Some(WORKER_GATE_NAME)
    })
}

/// The last `evidence/verdict` committed by the roster's
/// `output_schema` gate (R17-C3, pin P2-5), if any — the only carrier
/// that derives [`TaskOutcome::InvalidOutput`].
fn last_schema_verdict(events: &[Event]) -> Option<&Event> {
    events.iter().rev().find(|ev| {
        ev.kind == EventKind::EvidenceVerdict
            && ev.bytes.get("gate").and_then(Value::as_str) == Some(crate::worker::SCHEMA_GATE_NAME)
    })
}

/// The interrupt-derived outcome (§6.2 rows 6-7): present only when the
/// session actually ended `interrupted`. The **last** non-stale
/// `control/interrupt` names the requester; `child_timeout` derives
/// `Stalled`, anything else `Interrupted`. A stale (audit-only)
/// interrupt ends nothing and never counts.
fn interrupt_outcome(events: &[Event]) -> Option<TaskOutcome> {
    let ended_interrupted = events.iter().any(|ev| {
        matches!(
            ev.kind,
            EventKind::SessionEnded {
                reason: EndReason::Interrupted
            }
        )
    });
    if !ended_interrupted {
        return None;
    }
    let requester = events
        .iter()
        .rev()
        .filter(|ev| ev.kind == EventKind::ControlInterrupt)
        // `stale: true` is the audit-only arm (ADR-014): it ended nothing.
        .find(|ev| ev.bytes.get("stale") != Some(&json!(true)))
        .and_then(|ev| ev.bytes.get("requested_by").and_then(Value::as_str));
    match requester {
        Some(CHILD_TIMEOUT_REQUESTED_BY) => Some(TaskOutcome::Stalled),
        _ => Some(TaskOutcome::Interrupted),
    }
}

/// Derives the task outcome from the child's own committed events
/// (pin P8, §6.2 row for row). `own_report` is the child's own stated
/// report row (if one was admitted) — the `state:"waiting"` carrier of
/// `input_required`. Returns `None` for *outstanding*: no outcome is
/// ever synthesized from absence (§6.3:705-708).
///
/// [`TaskOutcome::InvalidOutput`] (row 4, R17-C3 pin P2-5) is derived
/// **only** from a committed `evidence/verdict{gate:"output_schema",
/// passed:false}` beside a `thread/completed{failed}` — checked before
/// the `turn/end` class rows. Without that verdict it is never
/// returned: the P1 unreachability held until the schema gate existed
/// to commit one (§3.2.1:432-437, §6.1.1:640-644).
#[must_use]
pub fn derive_outcome(
    child_events: &[Event],
    own_report: Option<&ThreadReport>,
) -> Option<TaskOutcome> {
    // Rows 1-3, 5 (first half): a completion exists — the outcome
    // refines `thread/completed.status`, it never redefines it.
    if let Some(done) = child_events
        .iter()
        .rev()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
    {
        return completion_outcome(
            done.bytes.get("status").and_then(Value::as_str),
            child_events,
        );
    }

    // Rows 6-7 (no completion): control/interrupt + session/ended{
    // interrupted} carry `stalled` / `interrupted`.
    if let Some(outcome) = interrupt_outcome(child_events) {
        return Some(outcome);
    }

    // Row 5 (second half): session/ended{failed} under the budget
    // class, with no completion carrier.
    let ended_failed = child_events.iter().any(|ev| {
        matches!(
            ev.kind,
            EventKind::SessionEnded {
                reason: EndReason::Failed
            }
        )
    });
    if ended_failed && last_error_class(child_events) == Some(BUDGET_EXHAUSTED_CLASS) {
        return Some(TaskOutcome::BudgetExhausted);
    }

    // Row 9: the child's own report row with state "waiting" and no
    // completion and no end — a live, resumable child (§6.2.1).
    let ended = child_events
        .iter()
        .any(|ev| matches!(ev.kind, EventKind::SessionEnded { .. }));
    if !ended && own_report.map(|r| r.state) == Some(Some(ReportState::Waiting)) {
        return Some(TaskOutcome::InputRequired);
    }

    // Row 8 / §6.3: outstanding — a missing completion means
    // *outstanding*, never `failed`.
    None
}

/// The §6.2 table applied to ONE completion: the outcome a
/// `thread/completed` with this `status` refines to, read off the
/// events the completion rides beside (the verdicts and error classes
/// are all committed before it). Shared by [`derive_outcome`] (the
/// committed-stream reader) and [`outcome_for_completion`] (the
/// at-write helper) so the two can never disagree.
fn completion_outcome(status: Option<&str>, events: &[Event]) -> Option<TaskOutcome> {
    match status {
        Some("completed") => {
            // Row 1 vs row 2: a *passed* worker-gate verdict that
            // was committed on this stream.
            let verified = last_worker_verdict(events)
                .and_then(|v| v.bytes.get("passed"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(if verified {
                TaskOutcome::CompletedVerified
            } else {
                TaskOutcome::CompletedUnverified
            })
        }
        Some("failed") => {
            // Row 4 (R17-C3, pin P2-5): a failed completion WITH a
            // committed `output_schema` verdict is `invalid_output` —
            // checked BEFORE the `turn/end` class rows, and reachable
            // only through the committed verdict (never typed by the
            // child, never inferred from prose).
            let schema_failed = last_schema_verdict(events)
                .and_then(|v| v.bytes.get("passed"))
                .and_then(Value::as_bool)
                == Some(false);
            if schema_failed {
                return Some(TaskOutcome::InvalidOutput);
            }
            Some(match last_error_class(events) {
                // Row 3: an empty final answer is a soft failure.
                Some(EMPTY_RESPONSE_CLASS) => TaskOutcome::EmptyOutput,
                // Row 5: an exhausted budget is honest non-success.
                Some(BUDGET_EXHAUSTED_CLASS) => TaskOutcome::BudgetExhausted,
                // No softer carrier names this failure: `failed`,
                // never a guess (plan §0.3 gap 3 — if the loop ever
                // stops recording the class, the derivation degrades
                // to `failed`, it does not invent `empty_output`).
                _ => TaskOutcome::Failed,
            })
        }
        // The reserved `interrupted` completion (and any future
        // spelling): the interrupt rows refine it when their carriers
        // exist, else it is an honest failure — an abort is never
        // mapped onto `completed` (constraint 7).
        _ => interrupt_outcome(events).or(Some(TaskOutcome::Failed)),
    }
}

/// (R17-C6, pin P4-3) The at-write helper behind the P4 `outcome` key:
/// the §6.2 table applied to the completion the child is **about to
/// append** — `status` is that completion's status, `events` the
/// stream as it stands before the append (every verdict and error
/// class the table reads is already committed). It agrees with
/// [`derive_outcome`] on the committed stream by construction (both
/// route through [`completion_outcome`]).
///
/// The three no-carrier values (§6.1.1:626-633) are **unwritable**:
/// `stalled`, `interrupted` and `input_required` have no
/// `thread/completed` carrier, so the helper filters them to `None` —
/// they stay derived on demand (the board has them in P3; the key
/// adds nothing there). `own_report` mirrors [`derive_outcome`]'s
/// signature; the completion arm never reads it (a completion exists
/// by hypothesis, so the waiting-report row is not consulted).
#[must_use]
pub fn outcome_for_completion(
    status: &str,
    events: &[Event],
    own_report: Option<&ThreadReport>,
) -> Option<TaskOutcome> {
    let _ = own_report;
    completion_outcome(Some(status), events).filter(|outcome| outcome.is_completion_borne())
}

/// (R17-C6, §7.2:771) The committed `evidence/verdict` an outcome was
/// decided by, as a `{session, seq}` pointer — never a copy. Present
/// iff a verdict decided the value: the passed worker-gate verdict
/// behind `completed_verified`, the failed schema verdict behind
/// `invalid_output`; every other value was decided by carriers the
/// stream itself names (status, error class), so there is nothing to
/// point at.
#[must_use]
pub fn outcome_evidence_ref(
    events: &[Event],
    session: SessionId,
    outcome: TaskOutcome,
) -> Option<crate::audit::EvidenceRef> {
    let decided_by = match outcome {
        TaskOutcome::CompletedVerified => last_worker_verdict(events)
            .filter(|v| v.bytes.get("passed").and_then(Value::as_bool) == Some(true)),
        TaskOutcome::InvalidOutput => last_schema_verdict(events)
            .filter(|v| v.bytes.get("passed").and_then(Value::as_bool) == Some(false)),
        _ => None,
    };
    decided_by.map(|verdict| crate::audit::EvidenceRef {
        session,
        seq: verdict.seq,
    })
}

// ---------------------------------------------------------------------------
// Readiness over `depends_on` (R17-C7, pin P4-4, §4.4)
// ---------------------------------------------------------------------------

/// The readiness verdict for one spawn's declared `depends_on`
/// (pin P4-4, §4.4:512-527): evaluated **at the spawn**, as a pure
/// function of the parent's own committed events plus an explicit
/// child-stream lookup — never a view, never live config (M7 /
/// READ-MODEL R8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepReadiness {
    /// Every declared dependency has a completion whose derived task
    /// outcome is in the success set — the spawn may proceed.
    Ready,
    /// The named task keys (every unmet one, not just the first) have
    /// no successful completion yet: never spawned, still
    /// outstanding, or completed outside the success set. A
    /// not-yet-spawned referent is **not-ready, not an error**
    /// (§5.4:607-608).
    Unmet(Vec<String>),
    /// The declaration closes a cycle over the parent's recorded
    /// dependency graph (a self-reference included) — a refusal
    /// naming the cycle's members in walk order (§7.3:791-793).
    Cycle(Vec<String>),
}

/// The task ids one `thread/started`'s declared `depends_on` names,
/// resolved in `parent`'s namespace — `None` when the event carries
/// no P4 audit group (pre-P4 bytes, or gating was off).
fn started_dep_ids(parent: SessionId, ev: &Event) -> Option<Vec<Uuid>> {
    let keys = ev
        .bytes
        .get(crate::audit::MULTIAGENT_KEY)?
        .get("depends_on")?
        .as_array()?;
    Some(
        keys.iter()
            .filter_map(|k| k.as_str())
            .map(|k| task_id_for_key(parent, k))
            .collect(),
    )
}

/// (R17-C7, pin P4-4 / §4.4:512-516) Readiness as a pure function of
/// the primary. A declared key releases its dependent iff the parent's
/// stream carries a `thread/started` for that `task_id` whose child's
/// derived outcome ([`derive_outcome`]) is in the success set
/// ([`TaskOutcome::is_success`] — `completed_verified` always,
/// `completed_unverified` iff `accept_unverified`, **`empty_output`
/// never, at any policy setting**). A referent with no spawn at all is
/// `Unmet`, not an error (§5.4:607-608); a declaration that closes a
/// cycle over the parent's **recorded** graph (the `depends_on` arrays
/// already committed on its `thread/started` events) is `Cycle`
/// naming the members (§7.3:791-793).
///
/// `own_task_id`/`own_key` name the spawn being evaluated (its event
/// is not yet on the stream — readiness runs before any side effect);
/// `child_events` supplies one child's committed stream by worker
/// session, and is only asked for sessions the parent's own lineage
/// already names. The cycle walk follows **recorded** edges only: a
/// task whose earlier attempts declared dependencies contributes those
/// edges even when a later attempt declared none — the refusal is
/// conservative and honest, never a silent accept.
#[must_use]
pub fn ready_deps(
    parent: SessionId,
    parent_events: &[Event],
    own_task_id: Uuid,
    own_key: Option<&str>,
    keys: &[String],
    accept_unverified: bool,
    child_events: &dyn Fn(SessionId) -> Vec<Event>,
) -> DepReadiness {
    if keys.is_empty() {
        return DepReadiness::Ready;
    }
    // The declared edges of this spawn, resolved in the parent's
    // namespace (§1:169-176 — the same derivation `task_key` uses).
    let own_deps: Vec<Uuid> = keys.iter().map(|k| task_id_for_key(parent, k)).collect();
    // The parent's recorded dependency graph: task_id → dep task_ids,
    // off the committed `multiagent.depends_on` audit groups. Only
    // this spawn's own event is absent (it does not exist yet).
    let mut graph: std::collections::BTreeMap<Uuid, Vec<Uuid>> = std::collections::BTreeMap::new();
    // id → the key that named it, for honest cycle/refusal text.
    let mut names: std::collections::BTreeMap<Uuid, String> = std::collections::BTreeMap::new();
    for ev in parent_events
        .iter()
        .filter(|ev| ev.kind == EventKind::ThreadStarted)
    {
        let Some(recorded) = started_dep_ids(parent, ev) else {
            continue;
        };
        let Some(task) = crate::audit::thread_started_task_id(&ev.bytes)
            .and_then(|raw| Uuid::parse_str(&raw).ok())
        else {
            continue;
        };
        graph
            .entry(task)
            .or_default()
            .extend(recorded.iter().copied());
        if let Some(list) = ev
            .bytes
            .get(crate::audit::MULTIAGENT_KEY)
            .and_then(|m| m.get("depends_on"))
            .and_then(Value::as_array)
        {
            for key in list.iter().filter_map(Value::as_str) {
                names
                    .entry(task_id_for_key(parent, key))
                    .or_insert_with(|| key.to_string());
            }
        }
    }
    for key in keys {
        names
            .entry(task_id_for_key(parent, key))
            .or_insert_with(|| key.clone());
    }
    // Cycle (checked first — a graph-level defect outranks a timing
    // fact): can any of this spawn's own deps reach the spawn's own
    // task id over the recorded edges? A self-reference is the
    // one-edge case.
    let name_of = |id: Uuid| -> String {
        if id == own_task_id {
            own_key
                .map(str::to_string)
                .unwrap_or_else(|| id.to_string())
        } else {
            names.get(&id).cloned().unwrap_or_else(|| id.to_string())
        }
    };
    let mut seen: std::collections::BTreeSet<Uuid> = std::collections::BTreeSet::new();
    let mut stack: Vec<(Uuid, Vec<Uuid>)> = own_deps.iter().map(|&d| (d, vec![d])).collect();
    while let Some((node, trail)) = stack.pop() {
        if node == own_task_id {
            let cycle: Vec<String> = trail.iter().map(|id| name_of(*id)).collect();
            return DepReadiness::Cycle(cycle);
        }
        if !seen.insert(node) {
            continue;
        }
        if let Some(next) = graph.get(&node) {
            for &succ in next {
                let mut trail = trail.clone();
                trail.push(succ);
                stack.push((succ, trail));
            }
        }
    }
    // Unmet (§4.4:512-516): every declared key with no successful
    // completion, named — never just the first.
    let mut unmet: Vec<String> = Vec::new();
    for key in keys {
        let id = task_id_for_key(parent, key);
        let expected = id.to_string();
        let released = parent_events
            .iter()
            .filter(|ev| ev.kind == EventKind::ThreadStarted)
            .any(|ev| {
                crate::audit::thread_started_task_id(&ev.bytes).as_deref()
                    == Some(expected.as_str())
                    && event_worker_session(ev)
                        .map(|worker| {
                            let events = child_events(worker);
                            derive_outcome(&events, None)
                                .is_some_and(|outcome| outcome.is_success(accept_unverified))
                        })
                        .unwrap_or(false)
            });
        if !released {
            unmet.push(key.clone());
        }
    }
    if unmet.is_empty() {
        DepReadiness::Ready
    } else {
        DepReadiness::Unmet(unmet)
    }
}

// ---------------------------------------------------------------------------
// Join target (F17, §3.2:409-413)
// ---------------------------------------------------------------------------

/// The join target: `parent` read off the `thread/started` payload that
/// names `self_session` as `worker` — from the **child's own stream**,
/// never ambient Router configuration (F17). There is deliberately no
/// fallback argument: `None` means the caller must refuse, not guess.
#[must_use]
pub fn join_target(child_events: &[Event], self_session: SessionId) -> Option<SessionId> {
    let worker = self_session.0.to_string();
    child_events
        .iter()
        .filter(|ev| ev.kind == EventKind::ThreadStarted)
        .find(|ev| ev.bytes.get("worker").and_then(Value::as_str) == Some(worker.as_str()))
        .and_then(|ev| ev.bytes.get("parent").and_then(Value::as_str))
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .map(SessionId)
}

// ---------------------------------------------------------------------------
// Limits and caps (§5.3)
// ---------------------------------------------------------------------------

/// (R17-C6, pin P4-1) The `superagent.board.gating` switch — the P4
/// class-A key-emission flag, plumbed to every site that decides
/// whether an audit key is written (the Router's parent-side
/// `thread/started` builder, the worker loop's child-side builders).
/// `false` (the default, and what an absent block produces) ⇒ **no
/// class-A key is emitted at all** — P3 bytes exactly [anchors.md
/// rule 4]. `true` ⇒ the two key groups are emitted (readiness
/// evaluation itself is C7's). Rollback is flipping back to `false`:
/// new emissions stop, history is never rewritten (§8.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BoardGating(pub bool);

impl BoardGating {
    /// Whether the gate is on.
    #[must_use]
    pub fn is_on(self) -> bool {
        self.0
    }
}

/// The P1 delegation bounds (§5.3:585-589, §7.1). Projection-derived
/// policy, validated fail-closed at config parse (C5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiagentLimits {
    /// Per parent session: children that still *hold* the parent.
    pub max_children: u32,
    /// Spawn-chain hops from the root session.
    pub max_depth: u32,
    /// **Inactivity** bound on a child (not total runtime).
    pub child_timeout_ms: u64,
}

/// Default `max_children` (§5.3:585).
pub const DEFAULT_MAX_CHILDREN: u32 = 4;
/// Default `max_depth` (§5.3:586): a child is a leaf; an orchestrator
/// child needs an explicit raise.
pub const DEFAULT_MAX_DEPTH: u32 = 1;
/// Default `child_timeout_ms` (§5.3:587): fifteen minutes of
/// *inactivity*, not total runtime.
pub const DEFAULT_CHILD_TIMEOUT_MS: u64 = 900_000;

impl MultiagentLimits {
    /// Fail-closed validation (the config surface maps every error to a
    /// boot refusal naming the key):
    ///
    /// - `max_children == 0` is **legal** — a leaf-only parent is the
    ///   §5.3/§3.1 idiom (`limits.max_children: 0` names a leaf), an
    ///   explicit policy, not a silently-disabled spawn surface.
    /// - `max_depth == 0` is refused — a spawn chain of zero hops
    ///   cannot produce the child being spawned, so the value can only
    ///   ever be a typo.
    /// - `child_timeout_ms == 0` is refused — it would interrupt every
    ///   child instantly.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_depth == 0 {
            return Err(
                "superagent.multiagent.max_depth must be at least 1 (a spawn chain of zero hops \
                 cannot produce the child being spawned); got 0"
                    .to_string(),
            );
        }
        if self.child_timeout_ms == 0 {
            return Err(
                "superagent.multiagent.child_timeout_ms must be at least 1 (zero would interrupt \
                 every child instantly); got 0"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// `bytes.worker` as a session id, if present and well-formed.
pub(crate) fn event_worker_session(ev: &Event) -> Option<SessionId> {
    let raw = ev.bytes.get("worker").and_then(Value::as_str)?;
    Some(SessionId(Uuid::parse_str(raw).ok()?))
}

/// Counts the children that still **hold** the parent (§5.3:585): the
/// `thread/started` entries on the parent's stream whose worker is not
/// [`ThreadLiveness::Released`]. `Live` and `NeverStarted` hold (a
/// waiting or queued child is honest work in flight); `Released` frees
/// the slot. Pure: the caller supplies the per-child liveness lookup.
#[must_use]
pub fn holding_children(
    parent_events: &[Event],
    liveness: &dyn Fn(SessionId) -> ThreadLiveness,
) -> usize {
    let mut held: Vec<SessionId> = Vec::new();
    for ev in parent_events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        let Some(worker) = event_worker_session(ev) else {
            continue;
        };
        if liveness(worker) != ThreadLiveness::Released && !held.contains(&worker) {
            held.push(worker);
        }
    }
    held.len()
}

/// The honest cap-refusal payload (§5.3, §8.3:864): names the bound and
/// the current count — never truncation, never a silent drop. Checked
/// in §5.4's order: `max_depth` before `max_children`.
#[must_use]
pub fn cap_refusal(limits: &MultiagentLimits, holding: u32, depth: u32) -> Option<CapRefusal> {
    if depth >= limits.max_depth {
        Some(CapRefusal {
            bound: "max_depth",
            current: u64::from(depth),
            max: u64::from(limits.max_depth),
        })
    } else if holding >= limits.max_children {
        Some(CapRefusal {
            bound: "max_children",
            current: u64::from(holding),
            max: u64::from(limits.max_children),
        })
    } else {
        None
    }
}

/// One cap refusal: which bound fired, at what current count against
/// what maximum. `current` may exceed `max` — the count is reported
/// honestly, never truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapRefusal {
    /// `"max_children"` or `"max_depth"`.
    pub bound: &'static str,
    /// The current count at the refusing spawn.
    pub current: u64,
    /// The configured maximum.
    pub max: u64,
}

/// Hops from the root session, one stream at a time (the stream-walk
/// shape of pins P5/A3): `0` for a root stream (no `thread/started`
/// lineage of its own), `1` for a spawned session's stream (it carries
/// `thread/started.parent`). Deeper chains are composed by the caller —
/// re-invoking this on each named parent's stream and summing — over
/// the streams the caller already has; P1's default `max_depth: 1`
/// needs exactly one call. No store scan is involved.
#[must_use]
pub fn depth_of(parent_events: &[Event]) -> u32 {
    let is_child = parent_events.iter().any(|ev| {
        ev.kind == EventKind::ThreadStarted
            && ev.bytes.get("parent").and_then(Value::as_str).is_some()
    });
    u32::from(is_child)
}

// ---------------------------------------------------------------------------
// Reconciliation (§2.2.1, the pure half of F2)
// ---------------------------------------------------------------------------

/// The pin-P6 class-C `contract` payload off a committed worker-gate
/// verdict: `{"passed": bool, "errors": [<failed check names>]}`.
fn contract_payload(verdict: &Value) -> Value {
    let passed = verdict
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let errors: Vec<String> = verdict
        .get("checks")
        .and_then(Value::as_array)
        .map(|checks| {
            checks
                .iter()
                .filter(|c| c.get("passed").and_then(Value::as_bool) == Some(false))
                .filter_map(|c| c.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    json!({ "passed": passed, "errors": errors })
}

/// The pure decision behind §2.2.1's pair-of-writes reconciliation
/// (F2): when `thread/completed` is already present on the child's
/// stream but the parent's row cannot be proven committed, the child
/// re-issues the admit with the same idempotency key and byte-stable
/// report — admission is idempotent per `(session, key)`, so the
/// re-issue is a no-op when the row is already there.
#[derive(Debug, Clone, PartialEq)]
pub enum Reconcile {
    /// No completion carrier exists: nothing to re-admit (today's
    /// early return stands).
    Nothing,
    /// `thread/completed` is present: re-admit with the pin-P3 key and
    /// the byte-stable report (re-derived from the same committed
    /// events — a genuine no-op when the row already exists).
    ReAdmit { key: String, report: Value },
}

/// Decides the reconciliation for one completion: `thread/completed`
/// present ⇒ [`Reconcile::ReAdmit`] with the byte-stable report
/// ([`thread_report_content`]-shaped plus the pin-P6 class-C keys:
/// `spawn_uid`/`task_id` when the wrapper carries the identity, and
/// `contract` iff a worker-gate verdict was committed); absent ⇒
/// [`Reconcile::Nothing`]. Pure: the caller performs the admit.
#[must_use]
pub fn reconcile_completion(
    child_events: &[Event],
    wrapper: &Value,
    thread_id: Uuid,
    status: &str,
    summary: &Value,
) -> Reconcile {
    let completed = child_events
        .iter()
        .any(|ev| ev.kind == EventKind::ThreadCompleted);
    if !completed {
        return Reconcile::Nothing;
    }
    let key = report_key_for(wrapper, thread_id);
    let mut report = thread_report_content(thread_id, status, summary.clone());
    // Pin P6 class-C keys ride only when the wrapper carries the
    // identity (absent, never null, on the legacy path).
    if let Some(uid) = wrapper.get("spawn_uid").and_then(Value::as_str) {
        report["spawn_uid"] = Value::String(uid.to_string());
        let task_id = wrapper
            .get("task_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            // Pin P7: the task id equals the derived thread id, so this
            // fallback is the same value by construction.
            .unwrap_or_else(|| thread_id.to_string());
        report["task_id"] = Value::String(task_id);
    }
    if let Some(verdict) = last_worker_verdict(child_events) {
        report["contract"] = contract_payload(&verdict.bytes);
    }
    Reconcile::ReAdmit { key, report }
}
