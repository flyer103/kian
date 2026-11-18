//! (R17-C6) The P4 class-A audit payload builders — the two key
//! groups of `design/MULTIAGENT-MODEL.md` §7.2, in one place.
//!
//! Class A (contract §0.3): new keys on an existing event payload,
//! **outside** `content`, non-context, omitted when unset, gated by
//! `superagent.board.gating` (pin P4-1: off ⇒ no key is emitted at
//! all — P3 bytes exactly). Both key groups land here so the two
//! sides' key sets can never drift apart:
//!
//! - the **parent's** `thread/started` (pin P4-2):
//!   `{spawn_uid, multiagent{task_id, assignee, attempt,
//!   depends_on[], group?}}` — the parent-side bookkeeping included;
//! - the **child's** `thread/started` (pin P4-2): `{spawn_uid,
//!   multiagent{task_id, assignee}}` — the derivable subset only
//!   (`depends_on`/`group`/`attempt` are **absent, never null**);
//! - the child's `thread/completed`: `{spawn_uid,
//!   multiagent{task_id, outcome, evidence_ref?, contract?}}` —
//!   `outcome` written by [`crate::multiagent::outcome_for_completion`]
//!   (pin P4-3: the six completion-borne values only), `evidence_ref`
//!   a `{session, seq}` pointer at the committed verdict (never a
//!   copy), `contract` the pin-P6 twin of a committed worker-gate
//!   verdict.
//!
//! Every builder takes the composed **base** payload (the frozen
//! legacy/bound builders' own bytes) and only adds the class-A keys,
//! so the additive-marker pattern composes with every earlier
//! additive key (`exec_worker`) exactly as it landed.

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::multiagent::TaskOutcome;

/// The namespaced task-fact group key (§7.2 "Key naming": the payload
/// already carries `"task"` — the wrapper itself — so the audit facts
/// ride under `"multiagent"`, mirroring the config block).
pub const MULTIAGENT_KEY: &str = "multiagent";

/// The durable pointer at a committed `evidence/verdict` (§7.2:771):
/// `{session, seq}` of the verdict the outcome was decided by — a
/// pointer, never a copy of the verdict's content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRef {
    /// The stream the verdict is committed on (the child's own).
    pub session: SessionId,
    /// The committed `seq` of the verdict event.
    pub seq: u64,
}

impl EvidenceRef {
    /// The JSON shape: exactly `{"session": <uuid-str>, "seq": <u64>}`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "session": self.session.0.to_string(),
            "seq": self.seq,
        })
    }
}

/// The parent-side `thread/started` audit facts (pin P4-2). Built at
/// the spawn site from the spawn's own identity plus the parent
/// stream's attempt ordinal; `depends_on`/`group` arrive with the P4
/// spawn arguments (C7) and stay empty/unset until then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentThreadAudit {
    /// The derived spawn-request identity (§2.1).
    pub spawn_uid: Uuid,
    /// The durable task identity (§2.1.1) — the id that survives
    /// attempts.
    pub task_id: Uuid,
    /// The roster entry id the spawn resolved through, iff one did
    /// (§2.1.1) — omitted when unset.
    pub assignee: Option<String>,
    /// `1 +` the parent stream's prior `thread/started` carrying the
    /// same `task_id` (§1:177-179).
    pub attempt: u64,
    /// The declared dependency keys — empty until the P4 `depends_on`
    /// argument exists (C7); an empty array is the honest "declared
    /// none" on the gated path.
    pub depends_on: Vec<String>,
    /// The join group — omitted when unset (C7).
    pub group: Option<String>,
}

/// The parent's `thread/started` class-A keys over the composed base
/// payload: `spawn_uid` + the full `multiagent` group (pin P4-2).
/// `assignee`/`group` are omitted when unset, never `null`.
#[must_use]
pub fn parent_thread_started_payload(base: Value, audit: &ParentThreadAudit) -> Value {
    let mut multiagent = json!({
        "task_id": audit.task_id.to_string(),
        "attempt": audit.attempt,
        "depends_on": audit.depends_on,
    });
    if let Some(assignee) = &audit.assignee {
        multiagent["assignee"] = json!(assignee);
    }
    if let Some(group) = &audit.group {
        multiagent["group"] = json!(group);
    }
    let mut payload = base;
    payload["spawn_uid"] = json!(audit.spawn_uid.to_string());
    payload[MULTIAGENT_KEY] = multiagent;
    payload
}

/// The child's `thread/started` class-A keys over the composed base
/// payload: `spawn_uid` + the **derivable subset** only (pin P4-2).
/// `depends_on`/`group`/`attempt` are parent-side bookkeeping and are
/// absent, never `null`. `assignee` is omitted when unset.
#[must_use]
pub fn child_thread_started_payload(
    base: Value,
    spawn_uid: Uuid,
    task_id: Uuid,
    assignee: Option<&str>,
) -> Value {
    let mut multiagent = json!({
        "task_id": task_id.to_string(),
    });
    if let Some(assignee) = assignee.filter(|a| !a.is_empty()) {
        multiagent["assignee"] = json!(assignee);
    }
    let mut payload = base;
    payload["spawn_uid"] = json!(spawn_uid.to_string());
    payload[MULTIAGENT_KEY] = multiagent;
    payload
}

/// The child's `thread/completed` class-A keys over the composed base
/// payload (§7.2): `spawn_uid` + `multiagent{task_id, outcome,
/// evidence_ref?, contract?}`. `outcome` is omitted exactly when the
/// completion's status refines to one of the three no-carrier values
/// (§6.1.1 — `stalled`/`interrupted`/`input_required` stay derived on
/// demand); `evidence_ref` is present iff a committed verdict decided
/// the outcome; `contract` iff a worker-gate verdict was committed.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn thread_completed_payload_keyed(
    base: Value,
    spawn_uid: Uuid,
    task_id: Uuid,
    outcome: Option<TaskOutcome>,
    evidence_ref: Option<EvidenceRef>,
    contract: Option<Value>,
) -> Value {
    let mut multiagent = json!({
        "task_id": task_id.to_string(),
    });
    if let Some(outcome) = outcome {
        multiagent["outcome"] = json!(outcome.as_str());
    }
    if let Some(evidence_ref) = evidence_ref {
        multiagent["evidence_ref"] = evidence_ref.to_json();
    }
    if let Some(contract) = contract {
        multiagent["contract"] = contract;
    }
    let mut payload = base;
    payload["spawn_uid"] = json!(spawn_uid.to_string());
    payload[MULTIAGENT_KEY] = multiagent;
    payload
}

/// The `task_id` one committed `thread/started` carries — the keyed
/// form's `multiagent.task_id` first, else the wrapper's class-B
/// `task_id` copy (the pre-P4 spelling the board already reads).
pub(crate) fn thread_started_task_id(bytes: &Value) -> Option<String> {
    let keyed = bytes
        .get(MULTIAGENT_KEY)
        .and_then(|m| m.get("task_id"))
        .and_then(Value::as_str);
    let wrapper = bytes
        .get("task")
        .and_then(|t| t.get("content"))
        .and_then(|c| c.get("task_id"))
        .and_then(Value::as_str);
    keyed.or(wrapper).map(str::to_string)
}

/// The attempt ordinal (§1:177-179): `1 +` the count of the parent
/// stream's `thread/started` events carrying the same `task_id`. A
/// pure function of committed events (constraint 13's pattern) — the
/// same derivation the board and the readiness walk (C7) use.
#[must_use]
pub fn attempt_of(parent_events: &[Event], task_id: Uuid) -> u64 {
    let expected = task_id.to_string();
    let priors = parent_events
        .iter()
        .filter(|ev| {
            ev.kind == EventKind::ThreadStarted
                && thread_started_task_id(&ev.bytes).as_deref() == Some(expected.as_str())
        })
        .count();
    1 + u64::try_from(priors).expect("a stream's event count fits u64")
}
