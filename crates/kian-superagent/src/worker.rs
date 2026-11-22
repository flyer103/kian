//! Thread delegation: `WorkerSpawner` + `WorkerLoopDriver`.
//!
//! The Router/Worker composition mechanics over existing primitives
//! (DESIGN §4, ADR-003): a `Thread` pins a parent (Router) session and a
//! worker `AgentDef` version; the worker is a *fresh session* created by
//! admitting its task input (the first execution hold appends
//! `session/created` — the bootstrap-admit path); the loop below is a
//! decorator around any inner loop that adds the thread lineage events
//! and the evidence-gated completion report.
//!
//! ## The delegation wrapper (model-visible by design)
//!
//! The task input's message `content` is the wrapper
//! `{"thread_id", "correlation"?, "task": <actual user content>,
//! "speculative"?}`. The worker's first user message honestly says what
//! spawned it and why; it is verbatim-passed JSON that is never
//! rewritten afterward (constraint 3 — rewriting it would destroy
//! provider prefix cache).
//!
//! The `"speculative": true` marker is additive and optional:
//! [`delegation_content`] never emits it (legacy bytes are unchanged,
//! golden-tested) and [`speculative_delegation_content`] adds exactly
//! one key. The marker is model-visible by design — the worker's first
//! message *is* the wrapper, so a speculatively spawned worker reads
//! that it was spawned on a weak signal — and audit-visible forever:
//! the wrapper bytes live in both streams (the Router-side and
//! worker-side `thread/started` payloads embed the task message, and
//! the worker's `input/admitted` carries the wrapper itself).
//!
//! ## Lineage events (the additions-only kinds)
//!
//! - Router side: the Router's own execution hold appends
//!   `thread/started` at dispatch, built by [`thread_started_payload`]
//!   (the `spawn_worker` tool does this in assembly; tests and the
//!   audit helper consume the same bytes).
//! - Worker side: [`WorkerLoopDriver`] appends `thread/started` at its
//!   first drive when the stream lacks one and the initial input
//!   carries the wrapper — **idempotent by stream scan**, so a takeover
//!   after a killed hold never duplicates lineage (constraint 1).
//! - Completion: on the inner loop's terminal outcome the wrapper runs
//!   the evidence gate (pure predicates, no model call), appends
//!   `evidence/verdict` + `thread/completed{status, summary}`, admits
//!   the completion report to the Router's inbox via [`InboxWriter`]
//!   (a durable row — no cross-stream claim, fencing untouched), then
//!   propagates the outcome. Soft failures record `status: "failed"`
//!   honestly; nothing maps a failed gate or a soft failure onto
//!   `completed` (constraint 7).
//!
//! ## Ports, not execution APIs (layered iron rule)
//!
//! This module implements the core `LoopDriver` seam (a kian-core
//! type) — that is bundle code composing loops, not an execution
//! client. Every stream write/read goes through the narrow
//! [`StreamPort`] (assembly implements it over the server's execution
//! hold); every inbox write goes through [`InboxWriter`]. The crate
//! still names no engine, store, or claim symbol
//! (`tests/iron_rule.rs`).
//!
//! ## Interrupts
//!
//! The `LoopDriver` seam carries no interrupt signal and the server
//! owns the interrupt end write; an interrupted worker session
//! therefore ends *without* `thread/completed` — the thread stays
//! honestly "in progress" in the audit view rather than recording a
//! status nobody observed. The `interrupted` status value remains
//! reserved for callers that can observe it.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, SessionId, Thread};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_loop::{EvidenceGateConfig, evaluate_evidence_gate, verdict_payload};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::inbox::{InboxWriteError, InboxWriter};
use crate::multiagent::{
    BoardGating, MultiagentLimits, Reconcile, SpawnIdentity, child_session_id, derive_outcome,
    join_target, outcome_evidence_ref, outcome_for_completion, reconcile_completion,
    report_key_for,
};

/// Gate name the worker wrapper stamps on its verdicts (distinct from
/// the built-in loop's `builtin` so both can coexist on one stream).
pub const WORKER_GATE_NAME: &str = "worker";

/// (R17-C3, pin P2-4) Gate name the roster's `output_schema` gate
/// stamps on its verdicts — the only gate name that derives
/// [`crate::multiagent::TaskOutcome::InvalidOutput`] (pin P2-5).
pub const SCHEMA_GATE_NAME: &str = "output_schema";

/// Idempotency key for a spawned worker's task input row (one row per
/// thread, never two).
fn task_input_key(thread_id: Uuid) -> String {
    format!("thread:{thread_id}")
}

// ---------------------------------------------------------------------------
// The delegation wrapper
// ---------------------------------------------------------------------------

/// A parsed delegation wrapper found inside a message `content`.
#[derive(Debug, Clone, PartialEq)]
pub struct Delegation {
    pub thread_id: Uuid,
    pub correlation: Option<Value>,
    /// The actual user content — what the Router asked the worker to do.
    pub task: Value,
}

/// The delegation wrapper that becomes the worker's first user message
/// `content`: `{"thread_id", "correlation"?, "task"}`. `correlation` is
/// omitted entirely when `None` (absent, never `null`).
#[must_use]
pub fn delegation_content(thread_id: Uuid, correlation: Option<Value>, task: Value) -> Value {
    let mut wrapper = json!({
        "thread_id": thread_id.to_string(),
        "task": task,
    });
    if let Some(correlation) = correlation {
        wrapper["correlation"] = correlation;
    }
    wrapper
}

/// The speculative marker key: present (and `true`) iff the
/// delegation was spawned on a weak signal. An additive optional field
/// inside the wrapper `content` — the frozen fields
/// (`thread_id`/`correlation`/`task`) and every legacy wrapper byte are
/// unchanged.
pub const SPECULATIVE_MARKER: &str = "speculative";

/// Parses a delegation wrapper out of a message `content` (the inverse
/// of [`delegation_content`]). Returns `None` for any content that is
/// not a delegation wrapper — plain user text and channel-event
/// wrappers are the common case and are never misread.
#[must_use]
pub fn parse_delegation(content: &Value) -> Option<Delegation> {
    let obj = content.as_object()?;
    let thread_id = Uuid::parse_str(obj.get("thread_id")?.as_str()?).ok()?;
    let task = obj.get("task")?.clone();
    if task.is_null() {
        return None;
    }
    Some(Delegation {
        thread_id,
        correlation: obj.get("correlation").cloned(),
        task,
    })
}

/// True iff `content` is a delegation wrapper carrying the
/// speculative marker (`"speculative": true`). The companion of
/// [`parse_delegation`] for the one additive wrapper field: the
/// [`Delegation`] struct itself stays exactly its frozen shape, so
/// every existing parse path (and test) is byte-identical.
#[must_use]
pub fn delegation_is_speculative(content: &Value) -> bool {
    content
        .as_object()
        .is_some_and(|obj| obj.get(SPECULATIVE_MARKER) == Some(&Value::Bool(true)))
}

/// The speculative delegation wrapper: exactly
/// [`delegation_content`] plus `"speculative": true` — the marker that
/// tells the worker (model-visible, in its first message) that it was
/// spawned on a weak signal and may be discarded without delivery.
#[must_use]
pub fn speculative_delegation_content(
    thread_id: Uuid,
    correlation: Option<Value>,
    task: Value,
) -> Value {
    let mut wrapper = delegation_content(thread_id, correlation, task);
    wrapper[SPECULATIVE_MARKER] = Value::Bool(true);
    wrapper
}

// ---------------------------------------------------------------------------
// The mandate object (ADR-012)
// ---------------------------------------------------------------------------

/// The mandate key (ADR-012): present iff the delegation carries
/// an authority object. An additive optional field inside the wrapper
/// `content` — the frozen fields (`thread_id`/`correlation`/`task`),
/// the marker, and every legacy wrapper byte are unchanged. The
/// markers compose: a speculative delegation may carry a mandate too
/// (both are additive wrapper keys).
pub const MANDATE_KEY: &str = "mandate";

/// The mandate delegation wrapper (ADR-012): exactly
/// [`delegation_content`] plus `"mandate"` — the marker mechanism
/// applied to an object. The mandate is the authority the Router
/// granted: `{strength, scope, granted_by, source{channel,topic},
/// policy_version, matched_rules?}` (schema owned by ADR-012). It is
/// **immutable** — carried verbatim inside `thread/started`'s task
/// payload (durable + audit-visible forever) and model-visible as the
/// worker's first message, because the wrapper *is* that message.
#[must_use]
pub fn mandate_delegation_content(
    thread_id: Uuid,
    correlation: Option<Value>,
    task: Value,
    mandate: &Value,
) -> Value {
    let mut wrapper = delegation_content(thread_id, correlation, task);
    wrapper[MANDATE_KEY] = mandate.clone();
    wrapper
}

/// Reads the additive mandate object off a delegation wrapper — the
/// companion of [`delegation_is_speculative`] for this wrapper field:
/// the [`Delegation`] struct stays exactly its frozen shape, and every
/// existing parse path is byte-identical. `None` on legacy wrappers
/// (the key is absent) and on non-wrapper content — never guessed,
/// never misread.
#[must_use]
pub fn delegation_mandate(content: &Value) -> Option<&Value> {
    content.as_object().and_then(|obj| obj.get(MANDATE_KEY))
}

// ---------------------------------------------------------------------------
// The derived task identity keys (R15 plan §4 C2, pin P6 class-B)
// ---------------------------------------------------------------------------

/// Reads the derived task identity off a delegation wrapper — the
/// inverse of the additive keys [`WorkerSpawner::prepare`] appends on
/// the multiagent path (`spawn_uid`, `task_id`, `agent_def`,
/// `agent_def_version`, and — R17-C2 — `assignee`, pin P6/P2-8). The
/// child side and the completion reconciliation recover the identity
/// from the wrapper bytes alone (the resolution is recorded, never
/// re-resolved, §2.1.1), so a replay rehydrates rather than
/// re-consults.
///
/// `thread_id` is read from the wrapper's own frozen `thread_id` key
/// (pin P2-8: it is no longer derivable from `task_id` once a
/// `task_key` splits the two). `assignee` is `None` on wrappers
/// written before the roster resolved one (the P1 form) — absent,
/// never guessed. `None` on legacy wrappers (the keys are absent —
/// the legacy path never writes them) and on partially-written or
/// malformed ones: the identity is never guessed, never defaulted,
/// never reconstructed from `thread_id` alone (a malformed writer is
/// a bug to surface, not a value to paper over).
#[must_use]
pub fn spawn_identity_of(wrapper: &Value) -> Option<SpawnIdentity> {
    let obj = wrapper.as_object()?;
    Some(SpawnIdentity {
        spawn_uid: Uuid::parse_str(obj.get("spawn_uid")?.as_str()?).ok()?,
        thread_id: Uuid::parse_str(obj.get("thread_id")?.as_str()?).ok()?,
        task_id: Uuid::parse_str(obj.get("task_id")?.as_str()?).ok()?,
        agent_def: obj.get("agent_def")?.as_str()?.to_string(),
        agent_def_version: obj.get("agent_def_version")?.as_u64()?,
        assignee: obj
            .get("assignee")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

// ---------------------------------------------------------------------------
// The execution-worker binding key (ADR-020, spec D1/D2)
// ---------------------------------------------------------------------------

/// The execution-worker binding key (D1): **one spelling on every
/// surface** — the delegation wrapper, the `thread/started` payload
/// top level, the `spawn_worker` tool argument, and the spawn result
/// echo. The value is the configured `endpoints[].id`, an opaque
/// non-empty string. `worker` stays the worker **session** uuid and
/// is never reused for the id. The constant lives in
/// `kian-plugin::hooks` (frame identity's crate) and is re-exported
/// here so the two spellings cannot drift.
pub const EXEC_WORKER_KEY: &str = kian_plugin::hooks::EXEC_WORKER_KEY;

/// The binding state of one surface (D2): the key is absent (legacy
/// unbound, byte-identical), present with a non-empty string value
/// (bound to that worker id), or present with anything else
/// (malformed — refused honestly at use time, never guessed, never
/// defaulted, never fallback-routed). [`Cow`] so stream-derived
/// readers can return an owned id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerBinding<'a> {
    /// The key is absent — a legacy unbound delegation.
    Unbound,
    /// The key names the bound execution worker.
    Bound(std::borrow::Cow<'a, str>),
    /// The key exists but is not a non-empty string.
    Malformed,
}

impl WorkerBinding<'_> {
    /// The same binding with an owned id (stream readers hand the
    /// value out of the events' lifetime).
    #[must_use]
    pub fn into_owned(self) -> WorkerBinding<'static> {
        match self {
            Self::Unbound => WorkerBinding::Unbound,
            Self::Bound(id) => {
                WorkerBinding::Bound(std::borrow::Cow::Owned(id.as_ref().to_string()))
            }
            Self::Malformed => WorkerBinding::Malformed,
        }
    }
}

/// One surface's binding off a JSON value: `None` (key absent) ⇒
/// [`WorkerBinding::Unbound`], a non-empty string ⇒ `Bound`, anything
/// else ⇒ `Malformed` (D2).
fn binding_of_value(value: Option<&Value>) -> WorkerBinding<'_> {
    match value {
        None => WorkerBinding::Unbound,
        Some(Value::String(id)) if !id.is_empty() => {
            WorkerBinding::Bound(std::borrow::Cow::Borrowed(id))
        }
        Some(_) => WorkerBinding::Malformed,
    }
}

/// Reads the additive execution-worker binding off a delegation
/// wrapper (the message `content`). The companion of
/// [`delegation_mandate`]: the [`Delegation`] struct stays exactly
/// its frozen shape, and every legacy parse path is byte-identical.
#[must_use]
pub fn delegation_worker_binding(content: &Value) -> WorkerBinding<'_> {
    binding_of_value(content.as_object().and_then(|obj| obj.get(EXEC_WORKER_KEY)))
}

/// Reads the additive execution-worker binding off a `thread/started`
/// payload's top level (the same key, the same spelling).
#[must_use]
pub fn thread_started_worker_binding(bytes: &Value) -> WorkerBinding<'_> {
    binding_of_value(bytes.get(EXEC_WORKER_KEY))
}

/// The session's own binding, derived from its own stream alone (I1:
/// no resident routing state; rebuild == replay). The stream's
/// `thread/started` payload (naming this session as the worker) and
/// the **first** `input/admitted` delegation wrapper must agree; a
/// disagreement — or a malformed value on either side — reads
/// [`WorkerBinding::Malformed`] and the routing layer refuses rather
/// than picking one (D4).
#[must_use]
pub fn worker_binding_of(events: &[Event], session: SessionId) -> WorkerBinding<'static> {
    let payload = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted && event_worker(ev) == Some(session))
        .map(|ev| thread_started_worker_binding(&ev.bytes));
    let wrapper = events
        .iter()
        .find(|ev| ev.kind == EventKind::InputAdmitted)
        .and_then(|ev| ev.bytes.get("content"))
        .map(delegation_worker_binding);
    match (payload, wrapper) {
        (None, None) => WorkerBinding::Unbound,
        (Some(binding), None) | (None, Some(binding)) => binding.into_owned(),
        (Some(payload), Some(wrapper)) => {
            if payload == wrapper {
                payload.into_owned()
            } else {
                WorkerBinding::Malformed
            }
        }
    }
}

/// The result of revalidating a live worker's mandate before a
/// follow-up routes to it (ADR-012's revalidation rule, composed with
/// the live-routing path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MandateRevalidation {
    /// The mandate authorizes the follow-up: `strength: explicit`
    /// always passes; `inferred` passes while its `policy_version` is
    /// still the current pointer. Legacy delegations (no mandate
    /// object) pass — legacy authority is never retroactively
    /// narrowed.
    Pass,
    /// `strength: inferred` under a `policy_version` that is no longer
    /// the current pointer: the follow-up releases to fresh
    /// evaluation (the `released_to_eval` path) — honestly
    /// audited, never a silent drop. `policy_version` is the mandate's
    /// persisted label, for the audit line.
    Release { policy_version: String },
}

/// Pure mandate revalidation (ADR-012): `strength: explicit` passes;
/// `inferred` passes iff its `policy_version` equals the current
/// policy pointer (the label; `None` on either side reads as the
/// honest `none` label, so an unversioned deployment never releases).
/// A missing `strength` is treated as `inferred` (the safe reading:
/// only an explicit statement is explicit).
#[must_use]
pub fn revalidate_mandate(
    mandate: Option<&Value>,
    current_policy_label: Option<&str>,
) -> MandateRevalidation {
    use crate::policy_version::NO_POLICY_LABEL;
    let Some(mandate) = mandate else {
        return MandateRevalidation::Pass;
    };
    if mandate.get("strength").and_then(Value::as_str) == Some("explicit") {
        return MandateRevalidation::Pass;
    }
    let stamped = mandate
        .get("policy_version")
        .and_then(Value::as_str)
        .unwrap_or(NO_POLICY_LABEL);
    let current = current_policy_label.unwrap_or(NO_POLICY_LABEL);
    if stamped == current {
        MandateRevalidation::Pass
    } else {
        MandateRevalidation::Release {
            policy_version: stamped.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Lineage payload builders (golden forms, frozen like all event bytes)
// ---------------------------------------------------------------------------

/// The `thread/started` payload (§2 schema):
/// `{"thread_id", "parent", "worker", "pinned_version", "task"}`.
/// `task` is the *message payload* (`{"role","content":<wrapper>}`) the
/// worker consumed — identical bytes on both sides' events.
#[must_use]
pub fn thread_started_payload(thread: &Thread, worker: SessionId, task_message: &Value) -> Value {
    json!({
        "thread_id": thread.id.to_string(),
        "parent": thread.parent_session.0.to_string(),
        "worker": worker.0.to_string(),
        "pinned_version": thread.pinned_version,
        "task": task_message,
    })
}

/// The bound `thread/started` payload (spec D4): exactly
/// [`thread_started_payload`] plus **one** top-level
/// [`EXEC_WORKER_KEY`] key — the additive-marker pattern. The payload
/// stays frozen for legacy spawns (the sibling builder above is
/// unchanged); both sides of one bound delegation render these same
/// bytes because both embed the identical task message.
#[must_use]
pub fn thread_started_payload_bound(
    thread: &Thread,
    worker: SessionId,
    task_message: &Value,
    exec_worker: &str,
) -> Value {
    let mut payload = thread_started_payload(thread, worker, task_message);
    payload[EXEC_WORKER_KEY] = Value::String(exec_worker.to_string());
    payload
}

/// The `thread/completed` payload (§2 schema):
/// `{"thread_id", "status": "completed"|"failed"|"interrupted",
/// "summary": <message payload>}`.
#[must_use]
pub fn thread_completed_payload(thread_id: Uuid, status: &str, summary: Value) -> Value {
    json!({
        "thread_id": thread_id.to_string(),
        "status": status,
        "summary": summary,
    })
}

/// The completion-report content admitted to the Router's inbox (a
/// normal message payload content, verbatim-passed JSON).
#[must_use]
pub fn thread_report_content(thread_id: Uuid, status: &str, summary: Value) -> Value {
    json!({
        "thread_id": thread_id.to_string(),
        "status": status,
        "summary": summary,
    })
}

/// The structured report state: the closed vocabulary the stage
/// projection reads (the prototype 001's Report row: progress ·
/// waiting · ready · blocked · failed · dropped). Carried by the
/// **additive** `"state"` key on stated completion reports
/// ([`thread_report_content_stated`]) — the frozen
/// `thread_id`/`status`/`summary` triple and every legacy report byte
/// are unchanged; 001's rule holds by construction: "waiting on a
/// person" is a structured field, never inferred from free text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportState {
    Progress,
    Waiting,
    Ready,
    Blocked,
    Failed,
    Dropped,
}

impl ReportState {
    /// The stable wire name (the serde name).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::Waiting => "waiting",
            Self::Ready => "ready",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Dropped => "dropped",
        }
    }

    /// Parses the wire name. Unknown values are `None` — a future
    /// state never breaks a legacy reader (additions-only discipline).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "progress" => Some(Self::Progress),
            "waiting" => Some(Self::Waiting),
            "ready" => Some(Self::Ready),
            "blocked" => Some(Self::Blocked),
            "failed" => Some(Self::Failed),
            "dropped" => Some(Self::Dropped),
            _ => None,
        }
    }
}

/// The additive state key on stated completion reports. Omitted
/// entirely by [`thread_report_content`] (legacy bytes unchanged, the
/// marker precedent).
pub const REPORT_STATE_KEY: &str = "state";

/// The stated completion-report content: exactly
/// [`thread_report_content`] plus the additive `"state"` key — the
/// structured fact the Ticket stage projection reads (arm 2 `waiting`,
/// arm 3 `ready`). The key is present (never `null`) precisely because
/// this builder is only called with a state to state.
#[must_use]
pub fn thread_report_content_stated(
    thread_id: Uuid,
    status: &str,
    summary: Value,
    state: ReportState,
) -> Value {
    let mut report = thread_report_content(thread_id, status, summary);
    report[REPORT_STATE_KEY] = Value::String(state.as_str().to_string());
    report
}

/// The multiagent completion-report content (R15 plan §4 C2, pin P6
/// class-C): exactly [`thread_report_content`] (whose frozen triple
/// and bytes are golden-tested and **not** modified) plus the additive
/// `spawn_uid` and `task_id` keys, and the `contract` object **iff**
/// the child committed a gate verdict — `None` means no verdict was
/// committed and the key is absent (never `null`). The `contract`
/// shape is `{"passed": bool, "errors": [<failed check names>]}`.
///
/// Pure: the caller (the join, C4) supplies committed facts only, so
/// the same events always re-derive the same bytes — the precondition
/// for §2.2.1's re-admit being a genuine no-op.
#[must_use]
pub fn thread_report_content_ident(
    identity: &SpawnIdentity,
    status: &str,
    summary: Value,
    contract: Option<Value>,
) -> Value {
    let mut report = thread_report_content(identity.thread_id, status, summary);
    report["spawn_uid"] = Value::String(identity.spawn_uid.to_string());
    report["task_id"] = Value::String(identity.task_id_str());
    if let Some(contract) = contract {
        report["contract"] = contract;
    }
    report
}

/// A parsed completion report found inside a message `content`.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadReport {
    pub thread_id: Uuid,
    pub status: String,
    pub summary: Value,
    /// The additive structured state: `None` on legacy reports
    /// (the key is absent) and on unknown future values — the report
    /// still parses, the state is never guessed.
    pub state: Option<ReportState>,
}

/// Parses a completion report out of a message `content`. Strict:
/// anything without a well-formed `thread_id`/`status`/`summary` triple
/// is not a report (user text is never misread). The `state` key is
/// additive: absent (legacy) ⇒ `None`.
#[must_use]
pub fn parse_thread_report(content: &Value) -> Option<ThreadReport> {
    let obj = content.as_object()?;
    Some(ThreadReport {
        thread_id: Uuid::parse_str(obj.get("thread_id")?.as_str()?).ok()?,
        status: obj.get("status")?.as_str()?.to_string(),
        summary: obj.get("summary")?.clone(),
        state: obj
            .get(REPORT_STATE_KEY)
            .and_then(Value::as_str)
            .and_then(ReportState::parse),
    })
}

// ---------------------------------------------------------------------------
// StreamPort — the decorator's narrow write/read surface
// ---------------------------------------------------------------------------

/// Failures of a [`StreamPort`] call. The server's typed errors are
/// mapped at the assembly boundary; the wrapper only needs "the write
/// did not land" — it then reports honestly and never guesses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("stream port: {0}")]
pub struct StreamPortError(pub String);

/// The decorator's only request surface toward the running hold:
/// append one event (carrying the hold's fencing epoch — assembly
/// guarantees that), read the session's committed stream, and name the
/// session. Assembly (kian-cli) builds one `Arc<dyn StreamPort>`
/// per execution hold from the hold handle (the handle is foreign to
/// this crate, so the impl lives with assembly — the same shape as the
/// `InboxWriter` port impl); the bundle never names that type.
pub trait StreamPort: Send + Sync {
    /// The session whose stream this port writes.
    fn session(&self) -> SessionId;
    /// Appends one event. Resolves only after the write is committed.
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>>;
    /// The session's full committed event stream (the sole truth).
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>>;
}

/// Builds the [`StreamPort`] for one execution hold from its context —
/// what assembly hands the wrapper at construction. Called once per
/// drive (each hold carries a fresh fencing epoch).
pub type PortFor<Ctx> = Arc<dyn Fn(&Ctx) -> Arc<dyn StreamPort> + Send + Sync>;

// ---------------------------------------------------------------------------
// WorkerSpawner
// ---------------------------------------------------------------------------

/// What [`WorkerSpawner::spawn`] produced: the minted [`Thread`], the
/// fresh worker session, and the task *message payload* the Router
/// embeds in its own `thread/started` event
/// ([`thread_started_payload`]).
#[derive(Debug, Clone, PartialEq)]
pub struct SpawnedWorker {
    pub thread: Thread,
    pub worker_session: SessionId,
    /// `{"role":"user","content":<delegation wrapper>}` — the exact
    /// message the worker consumes (identical bytes go into both
    /// streams' `thread/started` events).
    pub task_message: Value,
}

/// Failures of [`WorkerSpawner::spawn`]. On any error no wake was
/// issued and nothing durable claims the thread id.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("worker spawn: inbox admit failed: {0}")]
    Inbox(#[from] InboxWriteError),
    /// The Router-profile speculation budget is exhausted: the
    /// honest refusal the Router model sees and reacts to — never a
    /// silent drop. The outstanding count behind it is stream-derived
    /// (unmatched `thread/started`), so the decision survives Router
    /// restarts.
    #[error(
        "worker spawn: speculation cap `{label}` reached: \
             {outstanding} outstanding thread(s), max {max} — wait for a \
             thread completion before speculating again"
    )]
    SpeculationCap {
        label: String,
        outstanding: usize,
        max: usize,
    },
}

/// The optional eager-creation seam for a spawned worker session (HARNESS-BRIDGE §4.7).
///
/// Kept **off** [`InboxWriter`] on purpose: that port is exactly admit+wake (the iron-rule
/// invariant). A deployment with a mounted harness bridge supplies this port so
/// `session/created` can carry the bridge marker the first drain cannot derive.
pub trait SpawnSessionWriter: Send + Sync {
    /// Creates the worker session eagerly (HARNESS-BRIDGE §4.7), so the first event can carry the
    /// class marker the spawn resolved. `assignee` is the roster entry id the spawn resolved
    /// through (`None` on a non-roster spawn) — (ADR-049) the marker is per spawn, so the writer
    /// resolves the bridged class from the same identity the side selection uses.
    fn create_session(
        &self,
        session: SessionId,
        assignee: Option<&str>,
    ) -> BoxFuture<'static, Result<(), InboxWriteError>>;
}

/// Mints a `Thread` and creates the worker session by admitting its
/// task input (a fresh session id; the first hold appends
/// `session/created`), wakes it, and returns the thread id. The
/// `pinned_version` comes from the worker template's `AgentDef`
/// version (ADR-006: pinned for the thread's lifetime, never swapped).
pub struct WorkerSpawner {
    /// ADR-040: the live resident session (the thread parent for new
    /// workers and the completion-report target). Read at use time so a
    /// rotation re-points new workers; phase 1 never changes it.
    router: Arc<crate::binding::ResidentBinding>,
    pinned_version: u64,
    /// The worker template's id: the key the Router's mandate
    /// policy resolves per-template scope text against.
    worker_template_id: String,
    inbox: Arc<dyn InboxWriter>,
    /// ADR-038: the session-bootstrap seam, installed by the assembly
    /// once the engine exists (the spawner itself is built before it).
    /// `None` (the default, all tests) keeps every byte unchanged.
    bootstrap: std::sync::OnceLock<Option<Arc<dyn crate::bootstrap::SessionBootstrap>>>,
    /// HARNESS-BRIDGE §4.7: the eager-creation seam (absent = the historical lazy creation).
    session_creation: Option<Arc<dyn SpawnSessionWriter>>,
}

impl WorkerSpawner {
    /// `worker_template` supplies the pinned `AgentDef` version.
    pub fn new(
        router: Arc<crate::binding::ResidentBinding>,
        worker_template: &AgentDef,
        inbox: Arc<dyn InboxWriter>,
    ) -> Self {
        Self {
            router,
            pinned_version: worker_template.version,
            worker_template_id: worker_template.id.clone(),
            inbox,
            bootstrap: std::sync::OnceLock::new(),
            session_creation: None,
        }
    }

    /// Installs the session-bootstrap seam (ADR-038). Idempotent: the
    /// first install wins, later calls are no-ops.
    pub fn install_bootstrap(&self, bootstrap: Arc<dyn crate::bootstrap::SessionBootstrap>) {
        let _ = self.bootstrap.set(Some(bootstrap));
    }

    /// Mounts the eager-creation seam (HARNESS-BRIDGE §4.7); absent = the historical lazy
    /// creation by the first drain.
    #[must_use]
    pub fn with_session_creation(mut self, port: Arc<dyn SpawnSessionWriter>) -> Self {
        self.session_creation = Some(port);
        self
    }

    /// The worker template's id — the mandate scope key (
    /// `superagent.mandate.scopes`).
    #[must_use]
    pub fn worker_template_id(&self) -> &str {
        &self.worker_template_id
    }

    /// Spawns one worker for `task` (the actual user content).
    /// The row is durable before the wake fires (invariant 1); the
    /// admit is idempotent per thread id, so a retried dispatch never
    /// double-admits.
    pub async fn spawn(
        &self,
        task: Value,
        correlation: Option<Value>,
    ) -> Result<SpawnedWorker, SpawnError> {
        self.spawn_inner(task, correlation, false, None).await
    }

    /// [`WorkerSpawner::spawn`] plus the authority object (
    /// ADR-012): the wrapper is exactly the legacy bytes plus the
    /// additive `"mandate"` key, carried verbatim into the worker's
    /// first message and both streams' `thread/started` payloads.
    pub async fn spawn_with_mandate(
        &self,
        task: Value,
        correlation: Option<Value>,
        mandate: Value,
    ) -> Result<SpawnedWorker, SpawnError> {
        self.spawn_inner(task, correlation, false, Some(mandate))
            .await
    }

    /// Spawns one worker **speculatively**: identical mechanics
    /// to [`WorkerSpawner::spawn`], except the delegation wrapper
    /// carries `"speculative": true` — the model-visible, audit-forever
    /// marker that this thread was minted on a weak signal and may be
    /// discarded without delivery (the worker's own stream still audits
    /// every token spent). The caller (the Router profile's tool layer)
    /// enforces the speculation cap *before* calling this — the spawn
    /// itself is honest and uncapped so the refusal always happens
    /// where the Router model can see it.
    pub async fn spawn_speculative(
        &self,
        task: Value,
        correlation: Option<Value>,
    ) -> Result<SpawnedWorker, SpawnError> {
        self.spawn_inner(task, correlation, true, None).await
    }

    /// [`WorkerSpawner::spawn_speculative`] plus the authority object
    /// the and markers compose — the wrapper carries
    /// both additive keys, so even a speculatively spawned worker runs
    /// under a recorded mandate (weak signal, bounded authority).
    pub async fn spawn_speculative_with_mandate(
        &self,
        task: Value,
        correlation: Option<Value>,
        mandate: Value,
    ) -> Result<SpawnedWorker, SpawnError> {
        self.spawn_inner(task, correlation, true, Some(mandate))
            .await
    }

    /// The struct-carrying spawn entry point (spec D3): the four
    /// legacy methods above stay byte-identical and combinatorial
    /// explosion is refused — every caller that needs the
    /// execution-worker binding (and any future additive spawn field)
    /// goes through one [`SpawnRequest`]. Admits immediately; the
    /// bound Router path uses [`WorkerSpawner::prepare`] +
    /// [`WorkerSpawner::admit`] so the Router-side `thread/started`
    /// can land **before** the task row (D10).
    pub async fn spawn_bound(&self, request: SpawnRequest) -> Result<SpawnedWorker, SpawnError> {
        let spawned = self.prepare(&request);
        self.admit(&spawned).await?;
        Ok(spawned)
    }

    /// The pure half of a spawn (D10 step 7): mint the thread id, the
    /// fresh worker session, the delegation wrapper (every additive
    /// key included) and the task message. No IO, no side effects —
    /// the caller emits its Router-side lineage first, then admits.
    ///
    /// When `request.identity` is `Some` (the multiagent path, R15
    /// plan §4 C2) both ids are **derived from the identity** (pin
    /// P1): the thread id is the identity's `thread_id` (pin P2-8 —
    /// `child_thread_id(parent, spawn_uid)`, **never** `task_id`,
    /// which a shared `task_key` would collide across spawns) and the
    /// worker session is `child_session_id(thread_id)`. Derived,
    /// never minted: a replay of the same committed `tool/call`
    /// recomputes the same child instead of minting a second one
    /// (M4). When `None`, the two existing random mints below stay
    /// byte-identically in place — the legacy path is untouched.
    #[must_use]
    pub fn prepare(&self, request: &SpawnRequest) -> SpawnedWorker {
        let (thread_id, worker_session) = match &request.identity {
            // Pin P1/P2-8: the identity carries the resolution — the
            // thread id is taken from it (never re-minted) and the
            // worker session is derived from that thread id, so
            // preparing the same identity twice yields identical ids
            // and identical wrapper bytes.
            Some(identity) => (identity.thread_id, child_session_id(identity.thread_id)),
            None => (Uuid::new_v4(), SessionId::new()),
        };
        let thread = Thread {
            id: thread_id,
            parent_session: self.router.load(),
            pinned_version: self.pinned_version,
        };
        // The wrapper composes the public builders: the mandate
        // key lands through mandate_delegation_content (exactly
        // delegation_content plus one key), the speculative marker
        // through the spelling — both additive, both optional, and
        // absent together the bytes are the frozen legacy form.
        let mut wrapper = match &request.mandate {
            Some(mandate) => mandate_delegation_content(
                thread.id,
                request.correlation.clone(),
                request.task.clone(),
                mandate,
            ),
            None => {
                delegation_content(thread.id, request.correlation.clone(), request.task.clone())
            }
        };
        if request.speculative {
            wrapper[SPECULATIVE_MARKER] = Value::Bool(true);
        }
        // (ADR-020, D1/D2): the binding is the last additive key —
        // inserted only on the bound path, never replaced, so legacy
        // spawns stay byte-identical (absent, never null).
        if let Some(exec_worker) = &request.exec_worker {
            wrapper[EXEC_WORKER_KEY] = Value::String(exec_worker.clone());
        }
        // (R15 plan §4 C2, pin P6 class-B): on the multiagent path the
        // derived task identity rides as four more additive keys,
        // appended after the existing additive keys — new wrappers
        // only; the legacy path writes none of them (absent, never
        // null), so every landed wrapper's byte order is untouched.
        // (R17-C2, §2.1.1:264-269) two more appended keys, in the
        // same discipline: `assignee` (the roster entry id the spawn
        // resolved through — present iff a resolution happened) and
        // `output_schema` (the class-B copy of the declared
        // expectation, §3.2.1:438-441 — present iff one was declared;
        // the child is thereby "told about" it, so a restart
        // reconstructs the gate from its own stream).
        if let Some(identity) = &request.identity {
            wrapper["spawn_uid"] = Value::String(identity.spawn_uid.to_string());
            wrapper["task_id"] = Value::String(identity.task_id_str());
            wrapper["agent_def"] = Value::String(identity.agent_def.clone());
            wrapper["agent_def_version"] = json!(identity.agent_def_version);
            if let Some(assignee) = &identity.assignee {
                wrapper["assignee"] = Value::String(assignee.clone());
            }
            if let Some(schema) = &request.output_schema {
                wrapper["output_schema"] = schema.clone();
            }
        }
        let task_message = json!({"role": "user", "content": wrapper});
        SpawnedWorker {
            thread,
            worker_session,
            task_message,
        }
    }

    /// The durable half of a spawn (D10 step 7): admit the task row
    /// (idempotent per thread id — a retried dispatch never
    /// double-admits), then wake. Durable row first, wake strictly
    /// after (invariant 1).
    pub async fn admit(&self, spawned: &SpawnedWorker) -> Result<(), SpawnError> {
        // Eager creation (HARNESS-BRIDGE §4.7): the first event can then carry the bridge marker
        // the first drain could not derive. Absent the port, the lazy path is unchanged.
        if let Some(port) = &self.session_creation {
            // (ADR-049) the resolved roster entry rides the wrapper; the writer resolves the
            // per-spawn class marker from it (a kian-loop spawn stays the historical `{}`).
            let assignee = spawned.task_message["content"]["assignee"].as_str();
            port.create_session(spawned.worker_session, assignee)
                .await?;
        }
        // ADR-038: the bootstrap runs BEFORE the triggering row, so the
        // index is the worker session's first context-bearing event.
        // A failure is recorded and never fails the spawn.
        if let Some(bootstrap) = self.bootstrap.get().and_then(|b| b.as_ref())
            && let Err(error) = bootstrap.bootstrap(spawned.worker_session).await
        {
            tracing::warn!(
                session = %spawned.worker_session.0,
                %error,
                "worker session bootstrap skipped"
            );
        }
        let wrapper = spawned.task_message["content"].clone();
        self.inbox
            .admit(
                spawned.worker_session,
                wrapper,
                Some(task_input_key(spawned.thread.id)),
            )
            .await?;
        self.inbox.wake(spawned.worker_session);
        Ok(())
    }

    async fn spawn_inner(
        &self,
        task: Value,
        correlation: Option<Value>,
        speculative: bool,
        mandate: Option<Value>,
    ) -> Result<SpawnedWorker, SpawnError> {
        self.spawn_bound(SpawnRequest {
            task,
            correlation,
            speculative,
            mandate,
            exec_worker: None,
            identity: None,
            output_schema: None,
        })
        .await
    }
}

/// The struct-carrying spawn request (spec D3): `task`, the optional
/// `correlation`, the `speculative` marker, the (ADR-012) `mandate`,
/// and the (ADR-020) execution-worker binding. One entry point, no
/// combinatorial `spawn_*` explosion.
#[derive(Debug, Clone, Default)]
pub struct SpawnRequest {
    /// The actual user content — what the Router asked the worker to do.
    pub task: Value,
    /// Optional ticket correlation (verbatim into the wrapper).
    pub correlation: Option<Value>,
    /// The speculative marker (`"speculative": true` in the wrapper).
    pub speculative: bool,
    /// The mandate object (ADR-012), carried verbatim.
    pub mandate: Option<Value>,
    /// The execution-worker binding (ADR-020, D1): the configured
    /// `endpoints[].id`. `None` = legacy unbound, byte-identical.
    pub exec_worker: Option<String>,
    /// The derived task identity (R15 plan §4 C2): `Some` selects the
    /// multiagent path — the thread id and worker session are derived
    /// from it (pin P1) and the wrapper carries the four pin-P6
    /// class-B keys. `None` (the `Default`) is today's path,
    /// byte-identical in every respect. The single additive field is
    /// the seam: every future spawn-side fact composes here, never as
    /// loose fields.
    pub identity: Option<SpawnIdentity>,
    /// (R17-C2, §3.2.1:438-441) the declared `output_schema` — the
    /// call's, or the resolved roster entry's default when the call
    /// declared none. Copied verbatim into the wrapper as the class-B
    /// `output_schema` key **iff** one was declared: the child is
    /// thereby "told about" the expectation, so a restart
    /// reconstructs the gate from the child's own stream (constraint
    /// 2). `None` (the `Default`) writes no key — absent, never
    /// `null` — and the legacy path never sets it.
    pub output_schema: Option<Value>,
}

// ---------------------------------------------------------------------------
// Lineage planning (pure — unit-tested without any port)
// ---------------------------------------------------------------------------

/// What [`WorkerLoopDriver`] must do about lineage for one stream.
#[derive(Debug, Clone, PartialEq)]
pub enum LineagePlan {
    /// Not a spawned worker (no lineage event, no completion report):
    /// the decorator is a transparent pass-through.
    PassThrough,
    /// The stream already carries this worker's `thread/started`
    /// (takeover / later episode): append nothing.
    Resume { thread_id: Uuid },
    /// The initial input carries the wrapper and no lineage exists:
    /// append these exact `thread/started` bytes first.
    Emit {
        thread_id: Uuid,
        started_bytes: Value,
    },
}

/// Decides the lineage plan from the stream alone (constraint 1: no
/// resident state). A stream is a worker stream iff it already carries
/// a `thread/started` naming this session, or its **first** input
/// carries the delegation wrapper.
///
/// (R17-C6, pin P4-1/P4-2) `gating` is the `superagent.board.gating`
/// class-A switch: off (the default) the emitted bytes are exactly
/// today's; on, a wrapper carrying the spawn identity gains the
/// child-side keys — `spawn_uid` + the derivable `multiagent` subset
/// (`task_id`, `assignee`) — over the same base payload. A wrapper
/// without a readable identity writes no key (never guessed), and the
/// wrapper itself never changes: the keys ride outside `content`.
#[must_use]
pub fn lineage_plan(
    events: &[Event],
    self_session: SessionId,
    router: SessionId,
    worker_version: u64,
    gating: BoardGating,
) -> LineagePlan {
    if let Some(existing) = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted && event_worker(ev) == Some(self_session))
    {
        return match event_thread_id(existing) {
            Some(thread_id) => LineagePlan::Resume { thread_id },
            // A lineage event naming us without a usable thread id is
            // malformed history; never fabricate a second one.
            None => LineagePlan::PassThrough,
        };
    }
    let Some(first_input) = events.iter().find(|ev| ev.kind == EventKind::InputAdmitted) else {
        return LineagePlan::PassThrough;
    };
    let Some(content) = first_input.bytes.get("content") else {
        return LineagePlan::PassThrough;
    };
    let Some(delegation) = parse_delegation(content) else {
        return LineagePlan::PassThrough;
    };
    let thread = Thread {
        id: delegation.thread_id,
        parent_session: router,
        pinned_version: worker_version,
    };
    // (ADR-020, D4): the worker side derives its binding from the
    // FIRST input wrapper — when bound, the emitted `thread/started`
    // is the sibling bound payload (legacy + the one top-level key),
    // byte-identical to the Router side's event for the same spawn.
    // A malformed binding has no id to record; the payload stays the
    // legacy builder and `worker_binding_of` reads the stream
    // Malformed (the routing layer refuses).
    let started_bytes = match delegation_worker_binding(content) {
        WorkerBinding::Bound(id) => {
            thread_started_payload_bound(&thread, self_session, &first_input.bytes, &id)
        }
        WorkerBinding::Unbound | WorkerBinding::Malformed => {
            thread_started_payload(&thread, self_session, &first_input.bytes)
        }
    };
    // (R17-C6, pin P4-2): the child-side class-A keys — only when the
    // gate is on AND the wrapper carries a readable identity (all
    // four pin-P6 class-B keys; a partial identity is malformed
    // history and writes no key, never a guess). The keys compose
    // over the base payload above, so the bound form keeps its
    // `exec_worker` key and the wrapper bytes never move.
    let started_bytes = match (gating.is_on(), spawn_identity_of(content)) {
        (true, Some(identity)) => crate::audit::child_thread_started_payload(
            started_bytes,
            identity.spawn_uid,
            identity.task_id,
            identity.assignee.as_deref(),
        ),
        _ => started_bytes,
    };
    LineagePlan::Emit {
        thread_id: delegation.thread_id,
        started_bytes,
    }
}

/// `bytes.worker` as a session id, if present and well-formed.
fn event_worker(ev: &Event) -> Option<SessionId> {
    let raw = ev.bytes.get("worker").and_then(Value::as_str)?;
    Some(SessionId(Uuid::parse_str(raw).ok()?))
}

/// `bytes.thread_id` as a uuid, if present and well-formed.
fn event_thread_id(ev: &Event) -> Option<Uuid> {
    let raw = ev.bytes.get("thread_id").and_then(Value::as_str)?;
    Uuid::parse_str(raw).ok()
}

/// The `(status, summary)` one worker outcome reports (ADR-030).
///
/// The loop's only intentional-stop producer is iteration-budget exhaustion,
/// where the last assistant turn is by construction a pending tool call with
/// no text: recording that as `completed` would hand the Router an empty
/// success, so a stop reports `interrupted` and its reason *is* the summary
/// (the full context stays on the stream).
#[must_use]
pub fn reported_outcome(outcome: &LoopOutcome, events: &[Event]) -> (&'static str, Value) {
    match outcome {
        LoopOutcome::SoftFailed { reason } => ("failed", completion_summary(events, reason)),
        LoopOutcome::Stopped { reason } => ("interrupted", json!({ "text": reason })),
        _ => ("completed", completion_summary(events, "")),
    }
}

/// The summary payload for `thread/completed`: the last assistant
/// message's content, or `{"text": fallback}` when the stream has none
/// (a failed worker that never produced an assistant message still
/// reports honestly — the fallback carries the failure reason).
#[must_use]
pub fn completion_summary(events: &[Event], fallback_text: &str) -> Value {
    for ev in events.iter().rev() {
        if ev.kind == EventKind::AssistantMessage
            && let Some(content) = ev.bytes.get("content")
        {
            return content.clone();
        }
    }
    json!({"text": fallback_text})
}

// ---------------------------------------------------------------------------
// The output_schema gate (R17-C3, pin P2-4 — pure planning half)
// ---------------------------------------------------------------------------

/// The roster assignee of one committed stream (pin P2-2): the
/// **first** `input/admitted` wrapper's additive `assignee` key — the
/// recorded resolution (§2.1.1), never a live-config re-resolution.
/// `None` on legacy wrappers and non-wrapper streams.
#[must_use]
pub fn stream_assignee_of(events: &[Event]) -> Option<String> {
    let assignee = events
        .iter()
        .find(|ev| ev.kind == EventKind::InputAdmitted)?
        .bytes
        .get("content")?
        .get("assignee")
        .and_then(Value::as_str)?;
    if assignee.is_empty() {
        return None;
    }
    Some(assignee.to_string())
}

/// The final assistant answer text, if the stream carries one (the
/// same read [`completion_summary`] performs). `None` when there is no
/// assistant message or its text is blank — an **empty** final answer
/// never enters the schema gate (`empty_output` owns that case,
/// §6.2 row 3).
#[must_use]
pub fn final_assistant_answer(events: &[Event]) -> Option<String> {
    for ev in events.iter().rev() {
        if ev.kind == EventKind::AssistantMessage
            && let Some(text) = ev
                .bytes
                .get("content")
                .and_then(|content| content.get("text"))
                .and_then(Value::as_str)
        {
            return if text.trim().is_empty() {
                None
            } else {
                Some(text.to_string())
            };
        }
    }
    None
}

/// The idempotency key of the schema-gate correction input (pin P2-4,
/// §13 pattern): `schema-correction:<spawn_uid>:<attempt>` — derived
/// from the committed wrapper's `spawn_uid` and the stream-derived
/// attempt, so a replay of the same committed events re-derives the
/// same key (byte-stable) and a retried drive never double-appends.
#[must_use]
pub fn schema_correction_key(spawn_uid: &str, attempt: u64) -> String {
    format!("schema-correction:{spawn_uid}:{attempt}")
}

/// The correction input's committed bytes (pin P2-4): a synthetic
/// input whose `kian_schema_check` object carries the failed checks
/// with their honest detail lines, plus the attempt the key derives
/// from. Pure: same failed checks, same bytes.
#[must_use]
pub fn schema_correction_content(attempt: u64, checks: &[kian_loop::CheckOutcome]) -> Value {
    let errors: Vec<Value> = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| {
            let mut row = json!({"name": check.name});
            if let Some(detail) = &check.detail {
                row["detail"] = Value::String(detail.clone());
            }
            row
        })
        .collect();
    json!({
        "synthetic": true,
        "content": {"kian_schema_check": {"attempt": attempt, "errors": errors}},
    })
}

/// Counts the correction inputs already committed on the stream (the
/// schema gate's stream-derived attempt counter — the same discipline
/// as `evaluate_evidence_gate`'s verdict count: the budget survives
/// episodes and takeover because it is read off the committed bytes).
#[must_use]
pub fn schema_correction_count(events: &[Event]) -> u64 {
    events
        .iter()
        .filter(|ev| {
            ev.kind == EventKind::InputAdmitted
                && ev.bytes.get("synthetic") == Some(&json!(true))
                && ev.bytes.pointer("/content/kian_schema_check").is_some()
        })
        .count() as u64
}

/// One roster-subset keyword check rendered as a gate-verdict check
/// row (the `{keyword, passed, detail?}` shape mapped onto the verdict
/// payload's `{name, passed, detail?}`).
fn check_row(check: &crate::roster::CheckOutcome) -> kian_loop::CheckOutcome {
    kian_loop::CheckOutcome {
        name: check.keyword.clone(),
        passed: check.passed,
        detail: check.detail.clone(),
    }
}

/// Evaluates the final answer text against the declared subset schema
/// (pin P2-3): an unparseable answer is the `json_parse` violation;
/// a parseable one is validated keyword for keyword. Pure.
#[must_use]
pub fn schema_gate_checks(schema: &Value, answer: &str) -> Vec<kian_loop::CheckOutcome> {
    match serde_json::from_str::<Value>(answer.trim()) {
        Err(error) => vec![kian_loop::CheckOutcome {
            name: "json_parse".to_string(),
            passed: false,
            detail: Some(format!("the final answer is not valid JSON: {error}")),
        }],
        Ok(instance) => crate::roster::validate_instance(schema, &instance)
            .iter()
            .map(check_row)
            .collect(),
    }
}

/// The schema gate's plan for one terminal outcome (pin P2-4), a pure
/// function of the committed stream:
///
/// - [`SchemaGatePlan::Skip`] — no `output_schema` on the wrapper, or
///   the final answer is empty (never a schema miss, §6.2 row 3);
/// - [`SchemaGatePlan::Pass`] — every check passed (the gate stays
///   silent: no verdict is committed, `completed_unverified` keeps the
///   label unless the *worker* gate verifies it);
/// - [`SchemaGatePlan::Miss`] — the failed checks, the stream-derived
///   attempt (committed corrections), and the wrapper's `spawn_uid`
///   (the correction-key derivation; `None` on a wrapper without one —
///   no key can be derived, so the miss goes straight to the verdict).
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaGatePlan {
    /// No declared schema, or an empty final answer: the gate appends
    /// nothing at all (M8 — a wrapper without `output_schema` keeps
    /// today's bytes exactly).
    Skip,
    /// Every check passed: the honest silence — no verdict, no bytes.
    Pass,
    /// The failed checks, the stream-derived attempt, and the wrapper's
    /// `spawn_uid` for the correction-key derivation.
    Miss {
        checks: Vec<kian_loop::CheckOutcome>,
        attempt: u64,
        spawn_uid: Option<String>,
    },
}

/// Plans the schema gate for one drive off the committed events alone.
#[must_use]
pub fn schema_gate_plan(events: &[Event]) -> SchemaGatePlan {
    let wrapper = first_input_content(events);
    let Some(schema) = wrapper.get("output_schema") else {
        return SchemaGatePlan::Skip;
    };
    let Some(answer) = final_assistant_answer(events) else {
        return SchemaGatePlan::Skip;
    };
    let checks = schema_gate_checks(schema, &answer);
    if checks.iter().all(|check| check.passed) {
        return SchemaGatePlan::Pass;
    }
    SchemaGatePlan::Miss {
        checks,
        attempt: schema_correction_count(events),
        spawn_uid: wrapper
            .get("spawn_uid")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

// ---------------------------------------------------------------------------
// Audit helper
// ---------------------------------------------------------------------------

/// One delegation causal pair discovered by scanning both streams: the
/// Router-side `thread/started` (parent side) matched with the
/// worker-side `thread/started` of the same thread id.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegationPair {
    pub thread_id: Uuid,
    pub parent: SessionId,
    pub worker: SessionId,
    /// The delegated task message payload (from the Router-side event).
    pub task: Value,
}

/// Finds every delegation pair across the two streams (A2 audit: the
/// causal link lives in the logs, nowhere else). Router-side events
/// contribute `(thread_id, parent, worker, task)`; a pair exists when
/// the worker-side stream carries a `thread/started` of the same
/// thread id naming the same worker. Deterministic: ordered by thread
/// id.
#[must_use]
pub fn delegation_pairs(router_events: &[Event], worker_events: &[Event]) -> Vec<DelegationPair> {
    use std::collections::BTreeMap;
    let mut worker_side: BTreeMap<Uuid, SessionId> = BTreeMap::new();
    for ev in worker_events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        if let (Some(thread_id), Some(worker)) = (event_thread_id(ev), event_worker(ev)) {
            worker_side.insert(thread_id, worker);
        }
    }
    let mut pairs = BTreeMap::new();
    for ev in router_events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        let (Some(thread_id), Some(worker)) = (event_thread_id(ev), event_worker(ev)) else {
            continue;
        };
        if worker_side.get(&thread_id) == Some(&worker) {
            pairs.insert(
                thread_id,
                DelegationPair {
                    thread_id,
                    parent: ev.session,
                    worker,
                    task: ev.bytes.get("task").cloned().unwrap_or(Value::Null),
                },
            );
        }
    }
    pairs.into_values().collect()
}

// ---------------------------------------------------------------------------
// Worker exclusivity (ADR-020, I3/D9) — stream-derived, pure
// ---------------------------------------------------------------------------

/// Failures of a [`WorkerStreamsPort`] read. The bundle's honest
/// "cannot decide" — the spawn is refused (fail-closed), never
/// guessed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("worker streams port: {0}")]
pub struct WorkerStreamsError(pub String);

/// The bundle-owned read-only port over worker streams (spec D9, the
/// [`crate::recall::RecallPort`] precedent): the exclusivity check
/// reads the candidate bound workers' committed streams through this
/// seam and nothing else. Assembly (`kian-cli`) implements it over
/// the event store; this crate names no store symbol
/// (`tests/iron_rule.rs`).
pub trait WorkerStreamsPort: Send + Sync {
    /// One worker session's full committed event stream.
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>>;
}

/// One delegation bound to an execution worker, projected off the
/// Router's own `thread/started` events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundThread {
    pub thread_id: Uuid,
    pub worker_session: SessionId,
    /// The lineage event's `ts` — the clock the never-started
    /// reservation's release bound (D10) measures from.
    pub started_ts: u64,
}

/// Candidates per execution-worker id, from the ROUTER stream's
/// `thread/started` payloads (top-level [`EXEC_WORKER_KEY`]). Legacy
/// unbound threads contribute nothing — an unbound session is never
/// registered as bound (D15).
#[must_use]
pub fn bound_workers(
    router_events: &[Event],
) -> std::collections::BTreeMap<String, Vec<BoundThread>> {
    let mut map = std::collections::BTreeMap::new();
    for ev in router_events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        let WorkerBinding::Bound(id) = thread_started_worker_binding(&ev.bytes) else {
            continue;
        };
        let (Some(thread_id), Some(worker_session)) = (event_thread_id(ev), event_worker(ev))
        else {
            continue;
        };
        map.entry(id.as_ref().to_string())
            .or_insert_with(Vec::new)
            .push(BoundThread {
                thread_id,
                worker_session,
                started_ts: ev.ts,
            });
    }
    map
}

/// The liveness of one worker stream — status-aware, over the frozen
/// vocabulary (I3): `completed`/`cancelled` release the worker;
/// `failed`/`interrupted` are resumable and still hold it; a stream
/// with neither `session/created` nor `input/admitted` never started
/// (a lineage-only reservation — D10's crash window).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadLiveness {
    /// Started and not terminally released — the worker is held.
    Live,
    /// `thread/completed{completed}` or `session/ended{completed |
    /// cancelled}`: the worker is free.
    Released,
    /// No `session/created` and no `input/admitted` — the reservation
    /// never became a session; released only after the queue timeout
    /// from the lineage `ts`.
    NeverStarted,
}

/// Projects one worker stream's liveness (the table above). The last
/// terminal event decides: a `thread/completed{failed}` followed by
/// `session/ended{completed}` reads Released; the reverse order reads
/// what the final terminal says.
#[must_use]
pub fn thread_liveness(worker_events: &[Event]) -> ThreadLiveness {
    let started = worker_events.iter().any(|ev| {
        matches!(
            ev.kind,
            EventKind::SessionCreated | EventKind::InputAdmitted
        )
    });
    if !started {
        return ThreadLiveness::NeverStarted;
    }
    let mut liveness = ThreadLiveness::Live;
    for ev in worker_events {
        match &ev.kind {
            EventKind::ThreadCompleted => {
                let status = ev.bytes.get("status").and_then(Value::as_str);
                liveness = if status == Some("completed") {
                    ThreadLiveness::Released
                } else {
                    // `failed` (and the reserved `interrupted`):
                    // resumable — the worker is still held.
                    ThreadLiveness::Live
                };
            }
            EventKind::SessionEnded { reason } => {
                liveness = match reason {
                    kian_core::events::EndReason::Completed
                    | kian_core::events::EndReason::Cancelled => ThreadLiveness::Released,
                    kian_core::events::EndReason::Failed
                    | kian_core::events::EndReason::Interrupted
                    | kian_core::events::EndReason::AwaitingInput
                    | kian_core::events::EndReason::AwaitingBudget => ThreadLiveness::Live,
                    // A rotated session is frozen (ADR-040): nothing can
                    // resume it, so the thread is released.
                    kian_core::events::EndReason::Rotated => ThreadLiveness::Released,
                };
            }
            _ => {}
        }
    }
    liveness
}

/// The live owner a busy refusal names (I3): the owning worker
/// session, its thread, and why it holds the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusyOwner {
    pub thread_id: Uuid,
    pub worker_session: SessionId,
    /// Why the owner still holds the worker (the refusal text).
    pub reason: &'static str,
}

/// Why one bound session still holds the worker.
const BUSY_REASON_LIVE: &str = "live worker session";
const BUSY_REASON_NEVER_STARTED: &str = "never-started reservation inside the queue timeout";

/// The exclusivity decision `spawn_worker` consumes before any side
/// effect (I3/D9): pure over the Router's committed events plus the
/// candidate worker streams read through the port. `Ok(None)` = the
/// worker is free; `Ok(Some(owner))` = busy (the refusal names the
/// owner); `Err` = a stream could not be read — fail-closed, the
/// caller refuses honestly and never spawns on an undecidable fact.
///
/// A never-started reservation (the D10 crash window: lineage landed,
/// the task row never did) is released once `now_ms - started_ts`
/// reaches `ttl_ms` (`endpoints[].queue_timeout_ms`), with an audit
/// line — before that it honestly holds the worker.
pub async fn busy_owner(
    router_events: &[Event],
    port: &dyn WorkerStreamsPort,
    exec_worker: &str,
    now_ms: u64,
    ttl_ms: u64,
) -> Result<Option<BusyOwner>, WorkerStreamsError> {
    let candidates = bound_workers(router_events);
    let Some(candidates) = candidates.get(exec_worker) else {
        return Ok(None);
    };
    for candidate in candidates {
        let events = port.read_stream(candidate.worker_session).await?;
        match thread_liveness(&events) {
            ThreadLiveness::Released => continue,
            ThreadLiveness::NeverStarted
                if now_ms.saturating_sub(candidate.started_ts) >= ttl_ms =>
            {
                tracing::info!(
                    target: "kian::router",
                    worker = %exec_worker,
                    worker_session = %candidate.worker_session.0,
                    thread_id = %candidate.thread_id,
                    "worker exclusivity audit: never-started reservation released after the queue timeout"
                );
                continue;
            }
            ThreadLiveness::NeverStarted => {
                return Ok(Some(BusyOwner {
                    thread_id: candidate.thread_id,
                    worker_session: candidate.worker_session,
                    reason: BUSY_REASON_NEVER_STARTED,
                }));
            }
            ThreadLiveness::Live => {
                return Ok(Some(BusyOwner {
                    thread_id: candidate.thread_id,
                    worker_session: candidate.worker_session,
                    reason: BUSY_REASON_LIVE,
                }));
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// WorkerLoopDriver
// ---------------------------------------------------------------------------

/// Tuning for [`WorkerLoopDriver`].
#[derive(Debug, Clone)]
pub struct WorkerLoopConfig {
    /// The live resident session: where completion reports are admitted
    /// (ADR-040 binding; read at report time so a rotation re-points an
    /// in-flight worker's report to the successor).
    pub router: Arc<crate::binding::ResidentBinding>,
    /// The worker template's pinned `AgentDef` version (ADR-006) —
    /// stamped into the worker-side `thread/started`.
    pub worker_version: u64,
    /// The evidence gate evaluated at the terminal outcome.
    /// Default (disabled) appends no verdict and completes ungated —
    /// built-in inner behavior unchanged.
    pub gate: EvidenceGateConfig,
    /// The P1 delegation gate (R15 plan §4 C4): `Some` turns the join
    /// on — the completion report's admit target becomes the parent
    /// read off the child's own `thread/started` (F17: never ambient
    /// Router config), the §2.2. pair-of-writes reconciliation heals
    /// the `thread/completed`-without-parent-row crash window (F2),
    /// and the task outcome is derived and logged (never persisted —
    /// §7.2 is P4). The limits themselves bound the *spawn* side
    /// (C3); the join consumes no bound, but the one field keeps the
    /// feature gate single-sourced. `None` is today's path
    /// byte-identically (M8): the ambient [`WorkerLoopConfig::router`]
    /// admits the legacy report under the legacy key.
    pub multiagent: Option<MultiagentLimits>,
    /// (R17-C6, pin P4-1) The `superagent.board.gating` class-A
    /// emission switch: `false` (the default) ⇒ the child-side
    /// `thread/started` and `thread/completed` payloads keep their
    /// P3 bytes exactly; `true` ⇒ each gains `spawn_uid` + the
    /// `multiagent` audit group (the derivable subset on `started`,
    /// the outcome group on `completed` — `crates/kian-superagent/
    /// src/audit.rs`). Wiring only; the config block is C4's.
    pub board_gating: BoardGating,
}

/// A decorator over any inner [`LoopDriver`] that adds Thread lineage
/// `thread/started` at first drive (idempotent by stream scan —
/// safe across takeover), the evidence gate + `evidence/verdict` +
/// `thread/completed` at the terminal outcome, and the completion
/// report admitted to the Router's inbox before the outcome propagates.
///
/// Non-worker streams (no lineage) are a transparent pass-through: the
/// Router itself may run under this decorator unchanged.
pub struct WorkerLoopDriver<D: LoopDriver> {
    inner: Arc<DriverCore<D>>,
}

struct DriverCore<D: LoopDriver> {
    driver: Arc<D>,
    inbox: Arc<dyn InboxWriter>,
    config: WorkerLoopConfig,
    port_for: PortFor<D::Ctx>,
    /// Port (e) — `settle/pre` (PLUGIN-SPEC §4.6): the hook bus this
    /// gate consults before propagating `LoopOutcome::Settled` on a
    /// worker stream. `None` mounts no gate — today's behavior,
    /// byte-identical. Worker-lineage only by construction (§3.4.4):
    /// the gate sits on the `Some(thread_id)` path, which a non-worker
    /// stream never reaches.
    hooks: Option<kian_plugin::hooks::HookBus>,
}

impl<D: LoopDriver> Clone for WorkerLoopDriver<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: LoopDriver> WorkerLoopDriver<D> {
    /// Wraps `driver`. `inbox` is used only for the completion report
    /// (admit, then wake — both via the port); `port_for` builds
    /// the [`StreamPort`] for each execution hold from its context
    /// (assembly's job — see [`PortFor`]).
    pub fn new(
        driver: Arc<D>,
        inbox: Arc<dyn InboxWriter>,
        config: WorkerLoopConfig,
        port_for: PortFor<D::Ctx>,
    ) -> Self {
        Self::with_hooks(driver, inbox, config, port_for, None)
    }

    /// [`WorkerLoopDriver::new`] with the settle-gate bus mounted
    /// (PLUGIN-SPEC §4.6): the completion gate exists exactly where
    /// this wrapper is mounted **and** the stream is a worker stream —
    /// `RouterDriver` and `ServeLoopDriver` never consult (§3.4.4).
    pub fn with_hooks(
        driver: Arc<D>,
        inbox: Arc<dyn InboxWriter>,
        config: WorkerLoopConfig,
        port_for: PortFor<D::Ctx>,
        hooks: Option<kian_plugin::hooks::HookBus>,
    ) -> Self {
        Self {
            inner: Arc::new(DriverCore {
                driver,
                inbox,
                config,
                port_for,
                hooks,
            }),
        }
    }
}

/// The decorator's stream phases. `Lineage` classifies the stream;
/// `Inner` delegates; `Terminal` runs the gate, writes the completion
/// pair, admits the report, then yields the (possibly remapped)
/// outcome.
enum Phase<Ctx> {
    Lineage(Ctx, Arc<dyn StreamPort>),
    Inner {
        ctx: Ctx,
        port: Arc<dyn StreamPort>,
        thread_id: Option<Uuid>,
        stream: BoxStream<'static, LoopOutcome>,
    },
    Terminal {
        ctx: Ctx,
        port: Arc<dyn StreamPort>,
        thread_id: Uuid,
        outcome: LoopOutcome,
    },
    Done,
}

fn is_terminal(outcome: &LoopOutcome) -> bool {
    !matches!(outcome, LoopOutcome::TurnDone)
}

impl<D> LoopDriver for WorkerLoopDriver<D>
where
    D: LoopDriver + 'static,
    D::Ctx: Clone + Send + Sync + 'static,
{
    type Ctx = D::Ctx;

    fn drive(&self, ctx: D::Ctx) -> BoxStream<'static, LoopOutcome> {
        let core = Arc::clone(&self.inner);
        // One port per execution hold: each drive runs under a fresh
        // fencing epoch, and the port's writes carry it.
        let port = (core.port_for)(&ctx);
        futures::stream::unfold(Phase::Lineage(ctx, port), move |phase| {
            let core = Arc::clone(&core);
            async move {
                let mut phase = phase;
                loop {
                    match phase {
                        Phase::Done => return None,
                        Phase::Lineage(ctx, port) => {
                            match begin_lineage(&core, Arc::clone(&port)).await {
                                Ok((thread_id, events)) => {
                                    // §9.2 steps 1–4 at the drain/claim
                                    // boundary (PLUGIN-SPEC): when the
                                    // settle-gate bus is mounted, the same
                                    // instance's session-consistency guard
                                    // records the set digest at first sight
                                    // and re-checks it on every claim this
                                    // process serves. The frame carries the
                                    // session's durable `exec_worker`
                                    // binding (§7.3/§8.2) — derived from the
                                    // same stream read the lineage
                                    // classification just made, never from
                                    // resident state — so a worker-bound
                                    // session resolves its narrowed set and
                                    // the digest records that set, not the
                                    // server scope's.
                                    if let Some(bus) = core.hooks.as_ref() {
                                        bus.session_begin(kian_plugin::hooks::FrameInfo {
                                            session: port.session(),
                                            worker: settle_view_worker(&events, port.session()),
                                            seq: 0,
                                            epoch: None,
                                        });
                                    }
                                    let stream = core.driver.drive(ctx.clone());
                                    phase = Phase::Inner {
                                        ctx,
                                        port,
                                        thread_id,
                                        stream,
                                    };
                                }
                                Err(reason) => {
                                    return Some((LoopOutcome::SoftFailed { reason }, Phase::Done));
                                }
                            }
                        }
                        Phase::Inner {
                            ctx,
                            port,
                            thread_id,
                            mut stream,
                        } => {
                            return match stream.next().await {
                                Some(outcome) if !is_terminal(&outcome) => Some((
                                    outcome,
                                    Phase::Inner {
                                        ctx,
                                        port,
                                        thread_id,
                                        stream,
                                    },
                                )),
                                Some(outcome) => match thread_id {
                                    // Not a worker stream: transparent
                                    // pass-through of the terminal outcome.
                                    None => Some((outcome, Phase::Done)),
                                    Some(thread_id) => {
                                        // CONFIRMATION-MODEL phase 2: a
                                        // worker that stopped on a
                                        // pending `ask` is waiting, not
                                        // completed. No `thread/completed`
                                        // and no parent report: the
                                        // answer's wake resumes the same
                                        // session, which completes the
                                        // pair later. The pending set is
                                        // the stream's own fact (the ask
                                        // was appended before this
                                        // settle), never process state.
                                        if matches!(&outcome, LoopOutcome::Settled)
                                            && let Ok(events) = port.read_stream().await
                                            && !kian_core::confirmation::pending_calls(&events)
                                                .is_empty()
                                        {
                                            return Some((outcome, Phase::Done));
                                        }
                                        // Port (e) — `settle/pre`
                                        // (§4.6 step 2): consulted ONLY
                                        // for `LoopOutcome::Settled` (the
                                        // loop's own completion decision);
                                        // `SoftFailed`, `Stopped` and every
                                        // interrupt path propagate
                                        // untouched (§3.4.2's protected
                                        // classes never reach the port).
                                        if matches!(outcome, LoopOutcome::Settled)
                                            && let Some(bus) = core.hooks.clone()
                                            && let Some((yielded, next)) = settle_gate(
                                                &core,
                                                &bus,
                                                Arc::clone(&port),
                                                thread_id,
                                                outcome.clone(),
                                            )
                                            .await
                                        {
                                            return Some((yielded, next));
                                        }
                                        phase = Phase::Terminal {
                                            ctx,
                                            port,
                                            thread_id,
                                            outcome,
                                        };
                                        continue;
                                    }
                                },
                                // The inner stream ended without a terminal
                                // outcome — honest failure, never a silent
                                // completion (constraint 7).
                                None => Some((
                                    LoopOutcome::SoftFailed {
                                        reason: "worker loop ended without a terminal outcome"
                                            .into(),
                                    },
                                    Phase::Done,
                                )),
                            };
                        }
                        Phase::Terminal {
                            ctx,
                            port,
                            thread_id,
                            outcome,
                        } => {
                            return Some(
                                finish_thread(&core, &ctx, port, thread_id, outcome).await,
                            );
                        }
                    }
                }
            }
        })
        .boxed()
    }
}

/// built-in: classify the stream and, for a fresh worker, append the
/// `thread/started` lineage event. Pure decision via [`lineage_plan`].
/// Returns the classified thread id **and the events read** — the
/// drain-boundary §9.2 guard derives the session's durable worker
/// binding from the same read, never a second one.
async fn begin_lineage<D>(
    core: &DriverCore<D>,
    port: Arc<dyn StreamPort>,
) -> Result<(Option<Uuid>, Vec<Event>), String>
where
    D: LoopDriver,
{
    let events = port
        .read_stream()
        .await
        .map_err(|e| format!("worker lineage: stream read failed: {e}"))?;
    match lineage_plan(
        &events,
        port.session(),
        core.config.router.load(),
        core.config.worker_version,
        core.config.board_gating,
    ) {
        LineagePlan::PassThrough => Ok((None, events)),
        LineagePlan::Resume { thread_id } => Ok((Some(thread_id), events)),
        LineagePlan::Emit {
            thread_id,
            started_bytes,
        } => {
            port.emit(EventKind::ThreadStarted, started_bytes)
                .await
                .map_err(|e| format!("worker lineage: thread/started write failed: {e}"))?;
            Ok((Some(thread_id), events))
        }
    }
}

/// Port (e) — the §4.6 settle gate, one consultation of a mounted bus
/// before `LoopOutcome::Settled` propagates on a worker stream.
///
/// Returns `Some((yielded, next_phase))` when the gate decides the
/// drive must not reach the terminal decoration:
/// - `Deny` — the marker row is durable; the outcome propagates
///   unchanged and the engine's own check re-arms the loop
///   (`Supervised::Settled` + eligible input → `continue 'episodes`),
///   so the completion pair is written only when the session really
///   settles (§4.6 step 3);
/// - `AdmitFailed` — the three-step failure path: the gate does NOT
///   settle; the append error propagates as the driver's honest
///   failure through the existing route (step 1 of §4.6's path), and
///   the budget was consumed anyway via the bus's in-process counter.
///
/// `None` (Allow, Override, or an unreadable stream) proceeds to the
/// terminal decoration exactly as today.
async fn settle_gate<D>(
    core: &DriverCore<D>,
    bus: &kian_plugin::hooks::HookBus,
    port: Arc<dyn StreamPort>,
    _thread_id: uuid::Uuid,
    outcome: LoopOutcome,
) -> Option<(LoopOutcome, Phase<D::Ctx>)>
where
    D: LoopDriver,
    D::Ctx: Clone + Send + Sync + 'static,
{
    // The same read tolerance `finish_thread` already applies: an
    // unreadable stream cannot count a budget, and failing the session
    // over the gate's own read would turn an observer into an outage.
    let events = port.read_stream().await.unwrap_or_default();
    let session = port.session();
    let view = settle_view(&events, session);
    let ctl = std::sync::Arc::new(GateCtl {
        session,
        inbox: Arc::clone(&core.inbox) as Arc<dyn InboxWriter>,
        events,
    });
    match bus.settle(&view, ctl.as_ref()).await {
        kian_plugin::hooks::SettleVerdict::Allow
        | kian_plugin::hooks::SettleVerdict::Override { .. } => None,
        // §4.6 step 3: the marker is durable; propagate unchanged. No
        // terminal decoration this drive — the re-armed episode owns
        // the completion pair when it truly settles.
        kian_plugin::hooks::SettleVerdict::Deny { .. } => Some((outcome, Phase::Done)),
        // §4.6's append-failure path, step 1: the outcome becomes the
        // driver's honest failure — never `Settled`, never `completed`;
        // the session stays recoverable and resumable.
        kian_plugin::hooks::SettleVerdict::AdmitFailed { error, .. } => Some((
            LoopOutcome::SoftFailed {
                reason: format!("settle gate: veto marker admit failed: {error}"),
            },
            Phase::Done,
        )),
    }
}

/// The session's durable `exec_worker` binding as a frame `worker`
/// string (§7.3): the bound id, or the honest empty floor for an
/// unbound or malformed binding. Derived from the stream alone — the
/// wrapper holds no resident binding, so replay rebuilds it.
fn settle_view_worker(events: &[Event], session: SessionId) -> String {
    match worker_binding_of(events, session) {
        WorkerBinding::Bound(id) => id.into_owned(),
        WorkerBinding::Unbound | WorkerBinding::Malformed => String::new(),
    }
}

/// The §3.2.8 `settle/pre` view from the session's own stream.
fn settle_view(events: &[Event], session: SessionId) -> kian_plugin::hooks::SettleView {
    let turns = events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
        .count() as u64;
    let tool_calls = events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::ToolCall))
        .count() as u64;
    // Episodes completed before this one: the stream is the sole truth,
    // and a resumed session's prior `session/ended`s are exactly that.
    let episodes = events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::SessionEnded { .. }))
        .count() as u64;
    let evidence = events
        .iter()
        .rev()
        .find(|ev| matches!(ev.kind, EventKind::EvidenceVerdict))
        .and_then(|ev| ev.bytes.get("passed").and_then(Value::as_bool))
        .map_or("none".to_string(), |passed| {
            if passed { "passed" } else { "failed" }.to_string()
        });
    let frame = kian_plugin::hooks::FrameInfo {
        session,
        worker: settle_view_worker(events, session),
        seq: events.last().map_or(0, |ev| ev.seq),
        epoch: events.last().map(|ev| Some(ev.epoch.0)).unwrap_or(None),
    };
    kian_plugin::hooks::SettleView {
        reason: "completed".to_string(),
        turns,
        tool_calls,
        evidence,
        // §3.2.8's `vetoes_used`/`vetoes_remaining` are per plugin: the
        // bus rebuilds each subscriber's view with its own durable
        // count and its own §6.1 limit before dispatch — the zeros here
        // are the base view, never the numbers a gate reads.
        vetoes_used: 0,
        vetoes_remaining: 0,
        episodes,
        frame: Some(frame),
    }
}

/// The `SettleCtl` the gate injects (§4.6): the two operations over
/// what the wrapper already holds — the `InboxWriter` for the marker
/// row (the existing, honest steer shape: `{"synthetic": true,
/// "content": …}`, persisted before the model can see it) and the
/// committed stream for the per-plugin veto count.
struct GateCtl {
    session: SessionId,
    inbox: Arc<dyn InboxWriter>,
    events: Vec<Event>,
}

impl kian_plugin::hooks::SettleCtl for GateCtl {
    fn admit_synthetic(
        &self,
        content: String,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        let inbox = Arc::clone(&self.inbox);
        let session = self.session;
        Box::pin(async move {
            // Durable row first (persist-what-you-send), then the
            // advisory wake — which folds, since this drain is in
            // flight; the engine re-evaluates the inbox at Settled.
            let receipt = inbox
                .admit(
                    session,
                    serde_json::json!({"synthetic": true, "content": content}),
                    None,
                )
                .await
                .map_err(|e| e.to_string())?;
            let _ = inbox.wake(session);
            let _ = receipt;
            Ok(())
        })
    }

    fn veto_count(&self, plugin: &str) -> u64 {
        // §4.6's budget predicate: `synthetic` + the plugin's marker
        // prefix, never `role` — a projection of the durable stream.
        self.events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted))
            .filter(|ev| {
                ev.bytes.get("synthetic").and_then(Value::as_bool) == Some(true)
                    && ev
                        .bytes
                        .get("content")
                        .and_then(Value::as_str)
                        .is_some_and(|content| {
                            kian_plugin::hooks::is_veto_marker(plugin, true, content)
                        })
            })
            .count() as u64
    }
}

/// ecosystem: the terminal outcome. Gate → verdict → `thread/completed`
/// → report (durable row, then wake) → propagate. Gate failures with
/// retry budget left re-drive the inner loop (yielding `TurnDone` as
/// the continue signal); everything else yields a terminal outcome.
async fn finish_thread<D>(
    core: &DriverCore<D>,
    ctx: &D::Ctx,
    port: Arc<dyn StreamPort>,
    thread_id: Uuid,
    outcome: LoopOutcome,
) -> (LoopOutcome, Phase<D::Ctx>)
where
    D: LoopDriver,
    D::Ctx: Clone + Send + Sync + 'static,
{
    let mut final_outcome = outcome;
    if matches!(final_outcome, LoopOutcome::SoftFailed { .. }) {
        // Honest failure: no gate, no verdict — the thread records
        // `failed` (constraint 7: never completed).
    } else {
        // (R17-C3, pin P2-4) the output_schema gate — BEFORE the
        // worker evidence gate, off the committed stream alone: the
        // wrapper's class-B `output_schema` key declares the
        // expectation, the final assistant text is the instance.
        // A pass stays silent (no verdict — `completed_unverified`
        // keeps the label); miss #0 appends exactly one correction
        // input and re-drives; miss #1 commits the verdict and fails
        // the thread with the raw answer preserved. An empty final
        // answer never enters the gate (`empty_output` owns it).
        let events = match port.read_stream().await {
            Ok(events) => events,
            Err(e) => {
                return (
                    LoopOutcome::SoftFailed {
                        reason: format!("schema gate: stream read failed: {e}"),
                    },
                    Phase::Done,
                );
            }
        };
        match schema_gate_plan(&events) {
            SchemaGatePlan::Skip | SchemaGatePlan::Pass => {}
            SchemaGatePlan::Miss {
                checks,
                attempt,
                spawn_uid,
            } if attempt == 0 && spawn_uid.is_some() => {
                // The one correction (§3.2.1:442-445): an input/admitted
                // event on the child's OWN stream (an inbox row would
                // strand behind the engine's per-episode take), under
                // the byte-stable idempotency key
                // `schema-correction:<spawn_uid>:<attempt>` — a retried
                // or takeover drive re-derives the same key from the
                // same committed bytes, so the correction is exactly
                // one per attempt.
                let spawn_uid = spawn_uid.expect("checked by the guard");
                if let Err(e) = port
                    .emit(
                        EventKind::InputAdmitted,
                        schema_correction_content(attempt, &checks),
                    )
                    .await
                {
                    return (
                        LoopOutcome::SoftFailed {
                            reason: format!("schema gate: correction write failed: {e}"),
                        },
                        Phase::Done,
                    );
                }
                tracing::info!(
                    target: "kian::worker",
                    key = %schema_correction_key(&spawn_uid, attempt),
                    failed = checks.iter().filter(|c| !c.passed).count(),
                    "schema gate: one correction admitted; re-driving the inner loop"
                );
                let stream = core.driver.drive(ctx.clone());
                return (
                    LoopOutcome::TurnDone,
                    Phase::Inner {
                        ctx: ctx.clone(),
                        port,
                        thread_id: Some(thread_id),
                        stream,
                    },
                );
            }
            SchemaGatePlan::Miss {
                checks, attempt, ..
            } => {
                // Miss #1 (or a miss with no derivable key): commit the
                // verdict and fail the thread. The completion below
                // preserves the RAW answer as the summary — the parent
                // sees what the child actually said, never a summary of
                // the failure alone (§3.2.1:445-451).
                let verdict = kian_loop::GateVerdict {
                    passed: false,
                    checks,
                    attempt,
                };
                if let Err(e) = port
                    .emit(
                        EventKind::EvidenceVerdict,
                        verdict_payload(SCHEMA_GATE_NAME, &verdict),
                    )
                    .await
                {
                    return (
                        LoopOutcome::SoftFailed {
                            reason: format!("schema gate: verdict write failed: {e}"),
                        },
                        Phase::Done,
                    );
                }
                final_outcome = LoopOutcome::SoftFailed {
                    reason: format!(
                        "output_schema gate `{SCHEMA_GATE_NAME}` failed after {} attempt(s)",
                        attempt + 1
                    ),
                };
            }
        }
    }
    if matches!(final_outcome, LoopOutcome::SoftFailed { .. }) {
        // The schema gate's terminal miss lands here too: no worker
        // gate for an already-failed thread (one honest reason, not
        // two competing verdicts).
    } else if core.config.gate.enabled {
        let events = match port.read_stream().await {
            Ok(events) => events,
            Err(e) => {
                return (
                    LoopOutcome::SoftFailed {
                        reason: format!("worker gate: stream read failed: {e}"),
                    },
                    Phase::Done,
                );
            }
        };
        let verdict = evaluate_evidence_gate(WORKER_GATE_NAME, &core.config.gate.checks, &events);
        if let Err(e) = port
            .emit(
                EventKind::EvidenceVerdict,
                verdict_payload(WORKER_GATE_NAME, &verdict),
            )
            .await
        {
            return (
                LoopOutcome::SoftFailed {
                    reason: format!("worker gate: verdict write failed: {e}"),
                },
                Phase::Done,
            );
        }
        if verdict.passed {
            core.config.gate.counters.record_passed();
        } else {
            core.config.gate.counters.record_failed();
            if verdict.attempt < core.config.gate.max_retries {
                // Bounded retry (constraint 8: the attempt counter is
                // stream-derived, so the budget survives episodes and
                // takeover): re-drive the inner loop. The fabricated
                // `TurnDone` is the continue signal, exactly like the
                // built-in gate's retry flow.
                let stream = core.driver.drive(ctx.clone());
                return (
                    LoopOutcome::TurnDone,
                    Phase::Inner {
                        ctx: ctx.clone(),
                        port,
                        thread_id: Some(thread_id),
                        stream,
                    },
                );
            }
            final_outcome = LoopOutcome::SoftFailed {
                reason: format!(
                    "evidence gate `{WORKER_GATE_NAME}` failed after {} attempt(s)",
                    verdict.attempt + 1
                ),
            };
        }
    }

    // The completion pair is idempotent too: a takeover racing this
    // write must not produce a second thread/completed or a second
    // report row.
    let events = port.read_stream().await.unwrap_or_default();
    // Join target (F17, §3.2:409-413): with the multiagent gate on,
    // the parent is read off the child's OWN `thread/started` — never
    // ambient Router config. Feature-off keeps the ambient `router`
    // exactly as before (M8, byte-identical).
    let join = match core.config.multiagent {
        Some(_) => join_target(&events, port.session()),
        None => None,
    };
    if core.config.multiagent.is_some() && join.is_none() {
        // Refuse, never guess: a worker stream with no readable
        // `thread/started.parent` has no join target, and falling back
        // to the ambient Router would deliver the report to a session
        // that never asked for it. No report is emitted and no
        // completion is fabricated for an unknowable target — the
        // child stays honestly in progress.
        return (
            LoopOutcome::SoftFailed {
                reason: "worker completion: join target unknown \
                         (child stream carries no thread/started.parent)"
                    .into(),
            },
            Phase::Done,
        );
    }
    let target = join.unwrap_or(core.config.router.load());
    // Derived once, before the reconciliation block and the fresh path
    // below, which both consume it (the reconciliation's unwrap_or
    // fallback and the fresh emit read the same pair).
    let (status, summary) = reported_outcome(&final_outcome, &events);
    if events
        .iter()
        .any(|ev| ev.kind == EventKind::ThreadCompleted && event_thread_id(ev) == Some(thread_id))
    {
        // Feature-off keeps today's early return unchanged (M8): the
        // legacy path never re-admits.
        if core.config.multiagent.is_none() {
            return (final_outcome, Phase::Done);
        }
        // §2.2.1 pair-of-writes reconciliation (F2): the crash window
        // — `thread/completed` committed, the parent row unprovable —
        // heals by re-deriving the report from the same committed
        // events (byte-stable, so the re-admit is a genuine no-op
        // when the row already landed) and re-admitting under the
        // same idempotency key.
        let wrapper = first_input_content(&events);
        let committed = events
            .iter()
            .rev()
            .find(|ev| {
                ev.kind == EventKind::ThreadCompleted && event_thread_id(ev) == Some(thread_id)
            })
            .expect("presence checked above");
        let committed_status = committed
            .bytes
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or(status);
        let committed_summary = committed
            .bytes
            .get("summary")
            .cloned()
            .unwrap_or_else(|| completion_summary(&events, ""));
        match reconcile_completion(
            &events,
            &wrapper,
            thread_id,
            committed_status,
            &committed_summary,
        ) {
            Reconcile::Nothing => {
                // A stream with no completion carrier after all: today's
                // early return, unchanged.
                return (final_outcome, Phase::Done);
            }
            Reconcile::ReAdmit { key, report } => {
                if let Err(e) = core
                    .inbox
                    .admit(target, report.clone(), Some(key.clone()))
                    .await
                {
                    // The honest signal: the completion is committed but
                    // the parent row is still not durable. The settle is
                    // monotone — `session/ended{failed}` returns the
                    // child to `idle` (EVENT-MODEL:123), so the next
                    // claim re-enters this branch and reconciles again.
                    return (
                        LoopOutcome::SoftFailed {
                            reason: format!(
                                "worker completion reconciliation: re-admit failed: {e}"
                            ),
                        },
                        Phase::Done,
                    );
                }
                // Durable row first, wake strictly after (invariant 1).
                core.inbox.wake(target);
                // The task outcome is DERIVED HERE, from the committed
                // events alone, and is never persisted (§6.1.1:639-643;
                // persisting is P4, §7.2). This audit line is its only
                // observable channel — observable, not silent (§2.2.1).
                let derived = derive_outcome(&events, parse_thread_report(&report).as_ref());
                tracing::info!(
                    target: "kian::worker",
                    thread_id = %thread_id,
                    key = %key,
                    outcome = %derived.as_ref().map_or("outstanding", |o| o.as_str()),
                    "worker completion reconciliation: report re-admitted \
                     (thread/completed already committed)"
                );
                return (final_outcome, Phase::Done);
            }
        }
    }
    // (R17-C6, pins P4-1/P4-3): the class-A completion keys —
    // `spawn_uid` + the `multiagent` outcome group — ride only when
    // the gate is on AND the wrapper carries a readable identity.
    // `outcome` is written by the at-write helper (the six
    // completion-borne values; `None` means the status refines to a
    // no-carrier value, which stays derived on demand), `evidence_ref`
    // points at the committed verdict that decided the outcome,
    // `contract` is the pin-P6 twin of a committed worker-gate
    // verdict. Gating off (or no identity): exactly today's bytes.
    let wrapper = first_input_content(&events);
    let completed_payload = match (
        core.config.board_gating.is_on(),
        spawn_identity_of(&wrapper),
    ) {
        (true, Some(identity)) => {
            let outcome = outcome_for_completion(status, &events, None);
            let evidence_ref =
                outcome.and_then(|outcome| outcome_evidence_ref(&events, port.session(), outcome));
            crate::audit::thread_completed_payload_keyed(
                thread_completed_payload(thread_id, status, summary.clone()),
                identity.spawn_uid,
                identity.task_id,
                outcome,
                evidence_ref,
                worker_contract(&events),
            )
        }
        _ => thread_completed_payload(thread_id, status, summary.clone()),
    };
    if let Err(e) = port
        .emit(EventKind::ThreadCompleted, completed_payload)
        .await
    {
        return (
            LoopOutcome::SoftFailed {
                reason: format!("worker completion: thread/completed write failed: {e}"),
            },
            Phase::Done,
        );
    }
    // Report content (pin P6): the C2 ident builder when the wrapper
    // carries an identity (plus `contract` iff a worker-gate verdict
    // was committed), the frozen legacy builder otherwise; the key is
    // the pin-P3 key for the wrapper (`spawn:<spawn_uid>`, else the
    // legacy `thread-report:` key). Feature-off derives exactly the
    // legacy pair (M8).
    let key = report_key_for(&wrapper, thread_id);
    let report = match spawn_identity_of(&wrapper) {
        Some(identity) => {
            thread_report_content_ident(&identity, status, summary, worker_contract(&events))
        }
        None => thread_report_content(thread_id, status, summary),
    };
    if let Err(e) = core.inbox.admit(target, report, Some(key)).await {
        // The thread completed but the report is not durable: the
        // delegation contract is unmet — report honestly, never let
        // the session settle completed on a silent parent miss.
        return (
            LoopOutcome::SoftFailed {
                reason: format!("worker completion: report admit failed: {e}"),
            },
            Phase::Done,
        );
    }
    // Durable row first, wake strictly after (invariant 1).
    core.inbox.wake(target);
    (final_outcome, Phase::Done)
}

/// The stream's first `input/admitted` `content` — the same read
/// `lineage_plan` performs, so the join and the lineage agree on
/// which bytes are the delegation wrapper. `Null` when the stream
/// carries no admitted input (readers treat it as "no wrapper").
fn first_input_content(events: &[Event]) -> Value {
    events
        .iter()
        .find(|ev| ev.kind == EventKind::InputAdmitted)
        .and_then(|ev| ev.bytes.get("content"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// The pin-P6 class-C `contract` payload off the last committed
/// worker-gate verdict: `{"passed": bool, "errors": [<failed check
/// names>]}`, `None` when no verdict was committed (the key is then
/// absent, never `null`). Byte-identical to the pure core's builder
/// (`multiagent.rs`'s private `contract_payload` — kept in lockstep by
/// `fresh_report_builder_agrees_byte_for_byte_with_reconcile_completion`).
fn worker_contract(events: &[Event]) -> Option<Value> {
    let verdict = events.iter().rev().find(|ev| {
        ev.kind == EventKind::EvidenceVerdict
            && ev.bytes.get("gate").and_then(Value::as_str) == Some(WORKER_GATE_NAME)
    })?;
    let passed = verdict
        .bytes
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let errors: Vec<String> = verdict
        .bytes
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
    Some(json!({ "passed": passed, "errors": errors }))
}

#[cfg(test)]
mod tests {
    use crate::multiagent;
    use kian_core::model::Epoch;
    use kian_core::seams::LoopOutcome;
    use serde_json::json;
    use std::sync::Arc;

    use super::{
        Delegation, DelegationPair, LineagePlan, MANDATE_KEY, MandateRevalidation,
        SPECULATIVE_MARKER, SpawnError, StreamPortError, WorkerSpawner, completion_summary,
        delegation_content, delegation_is_speculative, delegation_mandate, delegation_pairs,
        lineage_plan, mandate_delegation_content, parse_delegation, parse_thread_report,
        reported_outcome, revalidate_mandate, speculative_delegation_content,
        thread_completed_payload, thread_report_content, thread_started_payload,
    };
    use crate::multiagent::BoardGating;
    use crate::templates::worker_template;
    use kian_core::events::{Event, EventKind};
    use kian_core::model::{SessionId, Thread};

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    fn tid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    // ------------------------------------------------------------------
    // Delegation wrapper
    // ------------------------------------------------------------------

    #[test]
    fn delegation_content_golden_form_round_trips() {
        let content = delegation_content(tid(0xaa), Some(json!("evt-9")), json!("do the thing"));
        assert_eq!(
            content,
            json!({
                "thread_id": tid(0xaa).to_string(),
                "correlation": "evt-9",
                "task": "do the thing",
            })
        );
        let back = parse_delegation(&content).expect("parses");
        assert_eq!(
            back,
            Delegation {
                thread_id: tid(0xaa),
                correlation: Some(json!("evt-9")),
                task: json!("do the thing"),
            }
        );

        // Correlation is omitted (absent, never null) when None.
        let bare = delegation_content(tid(0xbb), None, json!(42));
        assert!(bare.get("correlation").is_none());
        let back = parse_delegation(&bare).expect("parses");
        assert_eq!(back.correlation, None);
        assert_eq!(back.task, json!(42));
    }

    #[test]
    fn non_delegation_content_is_never_misread() {
        assert!(parse_delegation(&json!("plain text")).is_none());
        assert!(parse_delegation(&json!({"text": "hi"})).is_none());
        // Channel-event wrappers are not delegations.
        assert!(parse_delegation(&json!({"kian": {"type": "channel_event"}})).is_none());
        // A wrapper without a real task or with a malformed thread id.
        assert!(parse_delegation(&json!({"thread_id": "not-a-uuid", "task": 1})).is_none());
        assert!(parse_delegation(&json!({"thread_id": tid(1).to_string()})).is_none());
        assert!(parse_delegation(&serde_json::Value::Null).is_none());
    }

    // ------------------------------------------------------------------
    // Speculative delegation wrapper
    // ------------------------------------------------------------------

    #[test]
    fn speculative_wrapper_round_trips_with_the_marker() {
        let content = speculative_delegation_content(
            tid(0x5a),
            Some(json!("weak-evt-1")),
            json!("maybe fix the flaky signal"),
        );
        // Exactly delegation_content + one additive key: the frozen
        // fields round-trip unchanged and the marker reads back true.
        assert_eq!(
            content,
            json!({
                "thread_id": tid(0x5a).to_string(),
                "correlation": "weak-evt-1",
                "task": "maybe fix the flaky signal",
                "speculative": true,
            })
        );
        let back = parse_delegation(&content).expect("parses as a delegation");
        assert_eq!(back.thread_id, tid(0x5a));
        assert_eq!(back.correlation, Some(json!("weak-evt-1")));
        assert_eq!(back.task, json!("maybe fix the flaky signal"));
        assert!(delegation_is_speculative(&content));
    }

    #[test]
    fn legacy_wrapper_bytes_are_unchanged_additive_golden() {
        // The legacy builder emits no marker key at all (absent, never
        // false/null) and its serialized bytes are exactly the
        // golden — the addition is purely additive.
        let content = delegation_content(tid(0x5b), Some(json!("evt-9")), json!("do the thing"));
        assert!(content.get(SPECULATIVE_MARKER).is_none());
        assert!(!delegation_is_speculative(&content));
        let serialized = content.to_string();
        let expected = format!(
            "{{\"correlation\":\"evt-9\",\"task\":\"do the thing\",\"thread_id\":\"{}\"}}",
            tid(0x5b)
        );
        assert_eq!(serialized, expected);
        // An explicit false marker is not a speculative delegation.
        let explicit_false = json!({
            "thread_id": tid(0x5c).to_string(),
            "task": "x",
            "speculative": false,
        });
        assert!(parse_delegation(&explicit_false).is_some());
        assert!(!delegation_is_speculative(&explicit_false));
    }

    /// An inbox capturing full admit payloads (the wrapper bytes).
    struct CapturingInbox {
        admits: std::sync::Mutex<Vec<(SessionId, serde_json::Value, Option<String>)>>,
    }

    impl CapturingInbox {
        fn shared() -> Arc<Self> {
            Arc::new(Self {
                admits: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    impl crate::InboxWriter for CapturingInbox {
        fn admit(
            &self,
            session: SessionId,
            payload: serde_json::Value,
            idempotency_key: Option<String>,
        ) -> futures::future::BoxFuture<'static, Result<crate::AdmitReceipt, crate::InboxWriteError>>
        {
            self.admits
                .lock()
                .unwrap()
                .push((session, payload, idempotency_key));
            Box::pin(async {
                Ok(crate::AdmitReceipt {
                    deduplicated: false,
                })
            })
        }
        fn wake(&self, _session: SessionId) -> kian_orchestrator::WakeOutcome {
            kian_orchestrator::WakeOutcome::Folded
        }
    }

    #[tokio::test]
    async fn spawn_speculative_admits_the_marked_wrapper_durably() {
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        let spawned = spawner
            .spawn_speculative(json!("check the anomaly"), Some(json!("evt-1")))
            .await
            .expect("speculative spawn");

        // The durable row (the admitted payload) IS the marked wrapper,
        // keyed idempotently per thread (invariant 1 shape unchanged).
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(admits[0].0, spawned.worker_session);
        assert_eq!(
            admits[0].2.as_deref(),
            Some(format!("thread:{}", spawned.thread.id).as_str())
        );
        assert!(delegation_is_speculative(&admits[0].1));
        assert_eq!(
            parse_delegation(&admits[0].1).map(|d| d.task),
            Some(json!("check the anomaly"))
        );
        // The task message embedded in both streams' thread/started
        // carries the marker too (audit-visible forever).
        assert!(delegation_is_speculative(&spawned.task_message["content"]));
    }

    /// ADR-038: the session bootstrap runs BEFORE the triggering row —
    /// the injected index must be the worker session's first
    /// context-bearing event.
    #[tokio::test]
    async fn the_session_bootstrap_precedes_the_worker_admit() {
        struct OrderingBootstrap {
            inbox: Arc<CapturingInbox>,
            seen: Arc<std::sync::Mutex<Option<SessionId>>>,
        }

        impl crate::bootstrap::SessionBootstrap for OrderingBootstrap {
            fn bootstrap(
                &self,
                session: SessionId,
            ) -> futures::future::BoxFuture<'static, Result<(), String>> {
                // The triggering row must not exist yet.
                assert!(
                    self.inbox.admits.lock().unwrap().is_empty(),
                    "the bootstrap must run before the spawner admits the wrapper"
                );
                *self.seen.lock().unwrap() = Some(session);
                Box::pin(async { Ok(()) })
            }
        }

        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        let seen = Arc::new(std::sync::Mutex::new(None));
        spawner.install_bootstrap(Arc::new(OrderingBootstrap {
            inbox: Arc::clone(&inbox),
            seen: Arc::clone(&seen),
        }));

        let spawned = spawner
            .spawn(json!("inject then admit"), None)
            .await
            .expect("spawn");
        assert_eq!(*seen.lock().unwrap(), Some(spawned.worker_session));
        assert_eq!(inbox.admits.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn plain_spawn_still_admits_an_unmarked_wrapper() {
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        spawner.spawn(json!("plain work"), None).await.unwrap();
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert!(!delegation_is_speculative(&admits[0].1));
    }

    #[test]
    fn speculation_cap_error_is_honest_data() {
        let e = SpawnError::SpeculationCap {
            label: "weak-signal".into(),
            outstanding: 2,
            max: 2,
        };
        let text = e.to_string();
        assert!(
            text.contains("speculation cap `weak-signal` reached"),
            "{text}"
        );
        assert!(text.contains("2 outstanding thread(s), max 2"), "{text}");
        assert!(text.contains("wait for a thread completion"), "{text}");
    }

    // ------------------------------------------------------------------
    // The mandate wrapper (ADR-012)
    // ------------------------------------------------------------------

    fn mandate() -> serde_json::Value {
        json!({
            "strength": "inferred",
            "scope": "draft a triage comment for the issue; never post",
            "granted_by": {"login": "reporter", "type": "User"},
            "source": {"channel": "github", "topic": "acme/repo#7"},
            "policy_version": "v1",
        })
    }

    #[test]
    fn mandate_wrapper_is_exactly_legacy_plus_one_key() {
        let mandate = mandate();
        let content = mandate_delegation_content(
            tid(0x6a),
            Some(json!("evt-9")),
            json!("do the thing"),
            &mandate,
        );
        // Exactly delegation_content + "mandate": the frozen fields are
        // byte-identical, one additive key carries the object verbatim.
        let legacy = delegation_content(tid(0x6a), Some(json!("evt-9")), json!("do the thing"));
        assert_eq!(content.as_object().map(serde_json::Map::len), Some(4));
        for (key, value) in legacy.as_object().expect("legacy is an object") {
            assert_eq!(
                content.get(key),
                Some(value),
                "frozen field `{key}` unchanged"
            );
        }
        assert_eq!(delegation_mandate(&content), Some(&mandate));
        // The wrapper still parses as a delegation (the struct is frozen).
        let back = parse_delegation(&content).expect("mandate wrapper parses");
        assert_eq!(back.thread_id, tid(0x6a));
        assert_eq!(back.task, json!("do the thing"));
    }

    #[test]
    fn legacy_wrappers_parse_with_no_mandate() {
        // Legacy, speculative, and non-wrapper content: the key is
        // absent and the companion parser returns None — never guessed.
        let legacy = delegation_content(tid(0x6b), None, json!("x"));
        assert!(legacy.get(MANDATE_KEY).is_none());
        assert_eq!(delegation_mandate(&legacy), None);
        let speculative = speculative_delegation_content(tid(0x6c), None, json!("x"));
        assert_eq!(delegation_mandate(&speculative), None);
        assert_eq!(delegation_mandate(&json!("plain text")), None);
        assert_eq!(delegation_mandate(&serde_json::Value::Null), None);
    }

    #[test]
    fn mandate_wrapper_bytes_are_a_stable_golden() {
        let content = mandate_delegation_content(tid(0x6d), None, json!("t"), &mandate());
        let serialized = content.to_string();
        let expected = format!(
            "{{\"mandate\":{{\"granted_by\":{{\"login\":\"reporter\",\"type\":\"User\"}},\
             \"policy_version\":\"v1\",\"scope\":\"draft a triage comment for the issue; never \
             post\",\"source\":{{\"channel\":\"github\",\"topic\":\"acme/repo#7\"}},\
             \"strength\":\"inferred\"}},\"task\":\"t\",\"thread_id\":\"{}\"}}",
            tid(0x6d)
        );
        assert_eq!(serialized, expected);
    }

    #[tokio::test]
    async fn speculative_and_mandate_markers_compose() {
        // + compose: one wrapper carries both additive keys, so
        // even a weak-signal spawn runs under recorded authority.
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        let mandate = mandate();
        let spawned = spawner
            .spawn_speculative_with_mandate(json!("check the anomaly"), None, mandate.clone())
            .await
            .expect("speculative mandated spawn");
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        let wrapper = &admits[0].1;
        assert!(delegation_is_speculative(wrapper));
        assert_eq!(delegation_mandate(wrapper), Some(&mandate));
        // The same composed bytes ride both streams' thread/started.
        assert!(delegation_is_speculative(&spawned.task_message["content"]));
        assert_eq!(
            delegation_mandate(&spawned.task_message["content"]),
            Some(&mandate)
        );
    }

    #[tokio::test]
    async fn mandated_spawn_admits_the_wrapper_and_pins_the_template_id() {
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        assert_eq!(spawner.worker_template_id(), "kian-worker");
        let mandate = mandate();
        spawner
            .spawn_with_mandate(json!("plain work"), None, mandate.clone())
            .await
            .unwrap();
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(delegation_mandate(&admits[0].1), Some(&mandate));
        assert!(!delegation_is_speculative(&admits[0].1));
    }

    // ------------------------------------------------------------------
    // Mandate revalidation (ADR-012)
    // ------------------------------------------------------------------

    #[test]
    fn revalidation_passes_explicit_legacy_and_matching_versions() {
        let explicit = json!({"strength": "explicit", "policy_version": "v1"});
        let inferred = json!({"strength": "inferred", "policy_version": "v1"});
        // Explicit always passes — even under a changed version.
        assert_eq!(
            revalidate_mandate(Some(&explicit), Some("v9")),
            MandateRevalidation::Pass
        );
        // Legacy delegations (no object) pass; legacy authority is
        // never retroactively narrowed.
        assert_eq!(
            revalidate_mandate(None, Some("v9")),
            MandateRevalidation::Pass
        );
        // Inferred passes while its version is the current pointer
        // (both sides unversioned read as the honest `none` label).
        assert_eq!(
            revalidate_mandate(Some(&inferred), Some("v1")),
            MandateRevalidation::Pass
        );
        assert_eq!(
            revalidate_mandate(Some(&json!({"strength": "inferred"})), None),
            MandateRevalidation::Pass
        );
        assert_eq!(
            revalidate_mandate(
                Some(&json!({"strength": "inferred", "policy_version": "none"})),
                None
            ),
            MandateRevalidation::Pass
        );
    }

    #[test]
    fn revalidation_releases_inferred_under_a_changed_version() {
        let inferred = json!({"strength": "inferred", "policy_version": "v1"});
        assert_eq!(
            revalidate_mandate(Some(&inferred), Some("v2")),
            MandateRevalidation::Release {
                policy_version: "v1".into()
            }
        );
        // A versioned mandate under an unversioned deployment drifts
        // too (the pointer moved — unset is a drift, 's rule).
        assert_eq!(
            revalidate_mandate(Some(&inferred), None),
            MandateRevalidation::Release {
                policy_version: "v1".into()
            }
        );
        // A missing strength is the safe inferred reading.
        assert_eq!(
            revalidate_mandate(Some(&json!({"policy_version": "v1"})), Some("v2")),
            MandateRevalidation::Release {
                policy_version: "v1".into()
            }
        );
    }

    // ------------------------------------------------------------------
    // Payload golden forms
    // ------------------------------------------------------------------

    #[test]
    fn thread_payloads_match_the_c18_schema() {
        let thread = Thread {
            id: tid(0xcc),
            parent_session: sid(1),
            pinned_version: 7,
        };
        let task = json!({"role": "user", "content": {"task": "x"}});
        assert_eq!(
            thread_started_payload(&thread, sid(2), &task),
            json!({
                "thread_id": tid(0xcc).to_string(),
                "parent": sid(1).0.to_string(),
                "worker": sid(2).0.to_string(),
                "pinned_version": 7,
                "task": task,
            })
        );
        assert_eq!(
            thread_completed_payload(tid(0xcc), "completed", json!({"text": "done"})),
            json!({
                "thread_id": tid(0xcc).to_string(),
                "status": "completed",
                "summary": {"text": "done"},
            })
        );
    }

    #[test]
    fn thread_report_round_trips_strictly() {
        let content = thread_report_content(tid(0xdd), "failed", json!({"text": "boom"}));
        assert_eq!(
            parse_thread_report(&content),
            Some(super::ThreadReport {
                thread_id: tid(0xdd),
                status: "failed".to_string(),
                summary: json!({"text": "boom"}),
                state: None,
            })
        );
        assert!(parse_thread_report(&json!("nope")).is_none());
        assert!(parse_thread_report(&json!({"thread_id": "x", "status": "ok"})).is_none());
    }

    #[test]
    fn stated_report_is_exactly_legacy_plus_the_state_key() {
        // The additive key (the marker precedent): the legacy
        // builder emits no key at all, the stated builder adds exactly
        // one, and legacy bytes are unchanged.
        let legacy = thread_report_content(tid(0xe1), "completed", json!({"text": "done"}));
        assert!(legacy.get(super::REPORT_STATE_KEY).is_none());
        assert_eq!(
            legacy.to_string(),
            format!(
                "{{\"status\":\"completed\",\"summary\":{{\"text\":\"done\"}},\"thread_id\":\"{}\"}}",
                tid(0xe1)
            )
        );
        let stated = super::thread_report_content_stated(
            tid(0xe1),
            "completed",
            json!({"text": "done"}),
            super::ReportState::Ready,
        );
        assert_eq!(stated.get("status"), legacy.get("status"));
        assert_eq!(stated.get("summary"), legacy.get("summary"));
        assert_eq!(stated.get("thread_id"), legacy.get("thread_id"));
        assert_eq!(
            stated.as_object().map(serde_json::Map::len),
            Some(4),
            "exactly one additive key"
        );
        // Round-trip: the parsed state is the structured fact.
        let back = parse_thread_report(&stated).expect("stated report parses");
        assert_eq!(back.state, Some(super::ReportState::Ready));
        assert_eq!(back.status, "completed");
    }

    #[test]
    fn legacy_and_unknown_states_parse_to_none() {
        // Legacy reports (no key) parse with state None — never
        // misread, never rejected.
        let legacy = thread_report_content(tid(0xe2), "failed", json!({"text": "x"}));
        assert_eq!(parse_thread_report(&legacy).unwrap().state, None);
        // A future/unknown state value parses as a report with state
        // None (additions-only: the reader survives new vocabulary).
        let future = json!({
            "thread_id": tid(0xe2).to_string(),
            "status": "completed",
            "summary": {"text": "x"},
            "state": "teleported",
        });
        assert_eq!(parse_thread_report(&future).unwrap().state, None);
        // The vocabulary is closed and round-trips.
        for state in [
            super::ReportState::Progress,
            super::ReportState::Waiting,
            super::ReportState::Ready,
            super::ReportState::Blocked,
            super::ReportState::Failed,
            super::ReportState::Dropped,
        ] {
            assert_eq!(super::ReportState::parse(state.as_str()), Some(state));
            let serde_name = serde_json::to_value(state).unwrap();
            assert_eq!(serde_name, json!(state.as_str()));
        }
        assert_eq!(super::ReportState::parse("nope"), None);
    }

    // ------------------------------------------------------------------
    // Lineage planning
    // ------------------------------------------------------------------

    fn event(session: SessionId, seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(2),
            ts: 1_000 + seq,
            kind,
            bytes,
            version: 0,
        }
    }

    fn worker_stream_first_input(worker: SessionId) -> Vec<Event> {
        let wrapper = delegation_content(tid(0xee), None, json!("the task"));
        vec![
            event(
                worker,
                0,
                EventKind::SessionCreated,
                serde_json::Value::Null,
            ),
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": wrapper}),
            ),
        ]
    }

    #[test]
    fn lineage_plan_emits_for_fresh_worker_stream() {
        let router = sid(10);
        let worker = sid(20);
        let events = worker_stream_first_input(worker);
        let plan = lineage_plan(&events, worker, router, 7, BoardGating::default());
        let LineagePlan::Emit {
            thread_id,
            started_bytes,
        } = plan
        else {
            panic!("expected Emit, got {plan:?}");
        };
        assert_eq!(thread_id, tid(0xee));
        assert_eq!(started_bytes["parent"], router.0.to_string());
        assert_eq!(started_bytes["worker"], worker.0.to_string());
        assert_eq!(started_bytes["pinned_version"], 7);
        // The task embedded in the event is the full message payload
        // the worker consumed.
        assert_eq!(started_bytes["task"]["content"]["task"], json!("the task"));
    }

    #[test]
    fn lineage_plan_resumes_when_started_already_present() {
        let router = sid(10);
        let worker = sid(20);
        let mut events = worker_stream_first_input(worker);
        events.push(event(
            worker,
            2,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(0xee).to_string(),
                "parent": router.0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 7,
                "task": {},
            }),
        ));
        assert_eq!(
            lineage_plan(&events, worker, router, 7, BoardGating::default()),
            LineagePlan::Resume {
                thread_id: tid(0xee)
            },
            "takeover must not append a second thread/started"
        );
    }

    #[test]
    fn lineage_plan_passes_non_worker_streams_through() {
        let router = sid(10);
        // A router stream: first input is plain user text, and its own
        // thread/started events name *children*, not itself.
        let events = vec![
            event(
                router,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "judge this"}),
            ),
            event(
                router,
                2,
                EventKind::ThreadStarted,
                json!({"thread_id": tid(1).to_string(), "parent": router.0.to_string(), "worker": sid(99).0.to_string()}),
            ),
        ];
        assert_eq!(
            lineage_plan(&events, router, router, 1, BoardGating::default()),
            LineagePlan::PassThrough
        );
        // A worker-side event naming another session is not our lineage.
        let other = event(
            router,
            3,
            EventKind::ThreadStarted,
            json!({"thread_id": tid(2).to_string(), "worker": sid(98).0.to_string()}),
        );
        assert_eq!(
            lineage_plan(&[other], router, router, 1, BoardGating::default()),
            LineagePlan::PassThrough
        );
    }

    // ------------------------------------------------------------------
    // Completion summary
    // ------------------------------------------------------------------

    /// ADR-030: a budget stop is `interrupted` with its reason as the
    /// summary — never `completed` over a pending tool call with no text.
    #[test]
    fn a_budget_stop_reports_interrupted_with_its_reason() {
        let worker = sid(21);
        // Exactly the observed shape: the last assistant turn carries a
        // tool call and no text (the run was cut mid-tool).
        let events = vec![
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "x"}),
            ),
            event(
                worker,
                2,
                EventKind::AssistantMessage,
                json!({"content": {"text": "", "tool_calls": [{"id": "c1"}]}}),
            ),
        ];
        let (status, summary) = reported_outcome(
            &LoopOutcome::Stopped {
                reason: "iteration budget exhausted after 32 turns".into(),
            },
            &events,
        );
        assert_eq!(status, "interrupted");

        assert_eq!(
            summary,
            json!({"text": "iteration budget exhausted after 32 turns"})
        );
    }

    /// A soft failure is still `failed`, and a settle is still `completed`
    /// with the last assistant message — the ADR only moves the budget stop.
    #[test]
    fn a_soft_failure_and_a_settle_keep_their_status() {
        let worker = sid(22);
        let events = vec![event(
            worker,
            2,
            EventKind::AssistantMessage,
            json!({"content": {"text": "final answer"}}),
        )];
        let (failed, failed_summary) = reported_outcome(
            &LoopOutcome::SoftFailed {
                reason: "empty model response".into(),
            },
            &events,
        );
        assert_eq!(failed, "failed");
        assert_eq!(failed_summary, json!({"text": "final answer"}));

        let (settled, settled_summary) = reported_outcome(&LoopOutcome::Settled, &events);
        assert_eq!(settled, "completed");
        assert_eq!(settled_summary, json!({"text": "final answer"}));
    }

    #[test]
    fn completion_summary_uses_last_assistant_message_with_fallback() {
        let worker = sid(20);
        let events = vec![
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "x"}),
            ),
            event(
                worker,
                2,
                EventKind::AssistantMessage,
                json!({"content": {"text": "first"}}),
            ),
            event(worker, 3, EventKind::ToolResult, json!({})),
            event(
                worker,
                4,
                EventKind::AssistantMessage,
                json!({"content": {"text": "final answer"}}),
            ),
        ];
        assert_eq!(
            completion_summary(&events, "unused"),
            json!({"text": "final answer"})
        );
        // No assistant message: the honest fallback carries the reason.
        assert_eq!(
            completion_summary(&[], "empty response"),
            json!({"text": "empty response"})
        );
    }

    // ------------------------------------------------------------------
    // Audit helper
    // ------------------------------------------------------------------

    #[test]
    fn delegation_pairs_match_by_thread_id_across_streams() {
        let router = sid(10);
        let worker = sid(20);
        let task = json!({"role": "user", "content": {"task": "build"}});
        let started = json!({
            "thread_id": tid(0x99).to_string(),
            "parent": router.0.to_string(),
            "worker": worker.0.to_string(),
            "pinned_version": 3,
            "task": task,
        });
        let router_events = vec![event(router, 5, EventKind::ThreadStarted, started.clone())];
        let worker_events = vec![event(worker, 2, EventKind::ThreadStarted, started)];

        assert_eq!(
            delegation_pairs(&router_events, &worker_events),
            vec![DelegationPair {
                thread_id: tid(0x99),
                parent: router,
                worker,
                task,
            }]
        );

        // No worker-side event: no pair (half a delegation is not a
        // causal link).
        assert!(delegation_pairs(&router_events, &[]).is_empty());
        // Worker-side event naming a different worker: no pair.
        let stranger = event(
            worker,
            6,
            EventKind::ThreadStarted,
            json!({"thread_id": tid(0x99).to_string(), "worker": sid(77).0.to_string()}),
        );
        assert!(delegation_pairs(&router_events, &[stranger]).is_empty());
    }

    // ------------------------------------------------------------------
    // Plain-data checks
    // ------------------------------------------------------------------

    #[test]
    fn spawner_keys_and_errors_are_plain_data() {
        assert_eq!(super::task_input_key(tid(1)), format!("thread:{}", tid(1)));
        assert_eq!(
            crate::multiagent::legacy_report_key(tid(1)),
            format!("thread-report:{}", tid(1))
        );
        let e = SpawnError::from(crate::InboxWriteError::Write("boom".into()));
        assert_eq!(
            e.to_string(),
            "worker spawn: inbox admit failed: inbox write failed: boom"
        );
        let p = StreamPortError("x".into());
        assert_eq!(p.to_string(), "stream port: x");
        // The spawner pins the template version (ADR-006).
        let t = worker_template("p", "m", 42);
        assert_eq!(t.version, 42);
    }

    // ------------------------------------------------------------------
    // The execution-worker binding (ADR-020, I1/I7)
    // ------------------------------------------------------------------

    /// The bound wrapper is exactly the legacy wrapper plus ONE
    /// additive key; without the binding the bytes are byte-identical
    /// (the additive-marker discipline, I5).
    #[test]
    fn bound_wrapper_is_exactly_legacy_plus_one_additive_key() {
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            inbox.clone(),
        );
        let legacy = spawner.prepare(&super::SpawnRequest {
            task: json!("do the thing"),
            correlation: Some(json!("evt-9")),
            speculative: false,
            mandate: None,
            exec_worker: None,
            identity: None,
            output_schema: None,
        });
        let bound = spawner.prepare(&super::SpawnRequest {
            task: json!("do the thing"),
            correlation: Some(json!("evt-9")),
            speculative: false,
            mandate: None,
            exec_worker: Some("mac-mini".into()),
            identity: None,
            output_schema: None,
        });
        // The same frozen fields over the BOUND spawn's own thread id
        // (two prepares mint two threads — the id is the only
        // per-spawn difference).
        let legacy_wrapper =
            delegation_content(bound.thread.id, Some(json!("evt-9")), json!("do the thing"));
        let bound_wrapper = &bound.task_message["content"];
        // Every frozen/additive legacy field is byte-identical.
        for (key, value) in legacy_wrapper.as_object().expect("wrapper object") {
            assert_eq!(
                bound_wrapper.get(key),
                Some(value),
                "field `{key}` unchanged by the binding"
            );
        }
        // Exactly one additive key, carrying the id verbatim.
        assert_eq!(
            bound_wrapper.as_object().map(serde_json::Map::len),
            Some(
                legacy_wrapper
                    .as_object()
                    .map(serde_json::Map::len)
                    .unwrap()
                    + 1
            ),
            "exactly one additive key"
        );
        assert_eq!(
            super::delegation_worker_binding(bound_wrapper),
            super::WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"))
        );
        // The legacy builder emits no key at all (absent, never null)
        // and serializes to the frozen golden bytes.
        assert!(legacy_wrapper.get(super::EXEC_WORKER_KEY).is_none());
        assert_eq!(
            super::delegation_worker_binding(&legacy_wrapper),
            super::WorkerBinding::Unbound
        );
        assert_eq!(
            legacy_wrapper.to_string(),
            format!(
                "{{\"correlation\":\"evt-9\",\"task\":\"do the thing\",\"thread_id\":\"{}\"}}",
                bound.thread.id
            )
        );
        assert_ne!(legacy.thread.id, bound.thread.id);
    }

    /// The bound `thread/started` payload is the frozen legacy payload
    /// plus exactly one top-level key (spec D4), and both sides render
    /// identical bytes for the same spawn.
    #[test]
    fn thread_started_payload_bound_is_legacy_plus_one_top_level_key() {
        let thread = Thread {
            id: tid(0xcc),
            parent_session: sid(1),
            pinned_version: 7,
        };
        let task = json!({"role": "user", "content": {"task": "x", "exec_worker": "mac-mini"}});
        let legacy = thread_started_payload(&thread, sid(2), &task);
        assert!(legacy.get(super::EXEC_WORKER_KEY).is_none());
        let bound = super::thread_started_payload_bound(&thread, sid(2), &task, "mac-mini");
        for (key, value) in legacy.as_object().expect("payload object") {
            assert_eq!(bound.get(key), Some(value), "field `{key}` unchanged");
        }
        assert_eq!(
            bound.as_object().map(serde_json::Map::len),
            Some(legacy.as_object().map(serde_json::Map::len).unwrap() + 1),
            "exactly one top-level additive key"
        );
        assert_eq!(
            super::thread_started_worker_binding(&bound),
            super::WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"))
        );
        // The task message rides verbatim — both sides embed the same
        // bytes, so the worker-side derivation matches (I1).
        assert_eq!(bound["task"], task);
    }

    /// Reader tolerance (D2): absent = legacy unbound; a non-empty
    /// string = bound; "" / non-strings = Malformed — never guessed.
    #[test]
    fn binding_readers_tolerate_absent_and_refuse_malformed() {
        use super::{WorkerBinding, delegation_worker_binding, thread_started_worker_binding};
        let cases: Vec<serde_json::Value> = vec![
            json!(""),          // empty string
            json!(42),          // number
            json!(null),        // explicit null
            json!(true),        // boolean
            json!({"id": "x"}), // object
        ];
        for malformed in cases {
            let wrapper = json!({"thread_id": tid(1).to_string(), "task": "x", "exec_worker": malformed.clone()});
            assert_eq!(
                delegation_worker_binding(&wrapper),
                WorkerBinding::Malformed,
                "wrapper value {malformed} must read Malformed"
            );
            let payload = json!({"thread_id": tid(1).to_string(), "exec_worker": malformed});
            assert_eq!(
                thread_started_worker_binding(&payload),
                WorkerBinding::Malformed,
                "payload value must read Malformed"
            );
        }
        // Absent (legacy) and non-wrapper content read Unbound.
        let legacy = delegation_content(tid(2), None, json!("x"));
        assert_eq!(delegation_worker_binding(&legacy), WorkerBinding::Unbound);
        assert_eq!(
            delegation_worker_binding(&json!("plain text")),
            WorkerBinding::Unbound
        );
        let payload = thread_started_payload(
            &Thread {
                id: tid(3),
                parent_session: sid(1),
                pinned_version: 1,
            },
            sid(2),
            &json!({}),
        );
        assert_eq!(
            thread_started_worker_binding(&payload),
            WorkerBinding::Unbound
        );
    }

    /// `worker_binding_of` (I1): the session's own stream decides;
    /// both surfaces must agree or the stream reads Malformed.
    #[test]
    fn worker_binding_of_is_stream_derived_and_agrees_or_refuses() {
        use super::{WorkerBinding, worker_binding_of};
        let worker = sid(20);
        let started = |binding: serde_json::Value| {
            event(
                worker,
                5,
                EventKind::ThreadStarted,
                json!({
                    "thread_id": tid(0xee).to_string(),
                    "parent": sid(10).0.to_string(),
                    "worker": worker.0.to_string(),
                    "pinned_version": 7,
                    "task": {},
                    "exec_worker": binding,
                }),
            )
        };
        let input = |binding: serde_json::Value| {
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": {
                    "thread_id": tid(0xee).to_string(),
                    "task": "x",
                    "exec_worker": binding,
                }}),
            )
        };
        // Both surfaces bound and agreeing (rebuild == replay).
        let events = vec![input(json!("mac-mini")), started(json!("mac-mini"))];
        assert_eq!(
            worker_binding_of(&events, worker),
            WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"))
        );
        // Both legacy: unbound.
        let legacy = vec![
            input(serde_json::Value::Null.clone()),
            started(serde_json::Value::Null.clone()),
        ];
        // (null is present-but-malformed; use truly-absent instead)
        let mut legacy_stream = worker_stream_first_input(worker);
        legacy_stream.push(event(
            worker,
            2,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(0xee).to_string(),
                "parent": sid(10).0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 7,
                "task": {},
            }),
        ));
        assert_eq!(
            worker_binding_of(&legacy_stream, worker),
            WorkerBinding::Unbound
        );
        // Wrapper-only (fresh worker before its first drive): the
        // wrapper decides.
        let fresh = vec![input(json!("mac-mini"))];
        assert_eq!(
            worker_binding_of(&fresh, worker),
            WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"))
        );
        // Disagreeing copies: Malformed — never a pick.
        let disagree = vec![input(json!("mac-mini")), started(json!("vultr"))];
        assert_eq!(
            worker_binding_of(&disagree, worker),
            WorkerBinding::Malformed
        );
        let disagree_unbound = vec![input(json!("mac-mini")), started(serde_json::Value::Null)];
        assert_eq!(
            worker_binding_of(&disagree_unbound, worker),
            WorkerBinding::Malformed
        );
        // Malformed wrapper value: Malformed even alone.
        let malformed = vec![input(json!(""))];
        assert_eq!(
            worker_binding_of(&malformed, worker),
            WorkerBinding::Malformed
        );
        // A lineage event naming ANOTHER session is not ours (the
        // router-side event on the router stream never binds us).
        let router_side = vec![event(
            sid(10),
            1,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(0xee).to_string(),
                "worker": worker.0.to_string(),
                "exec_worker": "mac-mini",
            }),
        )];
        assert_eq!(
            worker_binding_of(&router_side, sid(99)),
            WorkerBinding::Unbound
        );
        let _ = legacy; // (shape fixture kept explicit above)
    }

    /// n1: the TWO stream readers of the same `exec_worker` binding —
    /// this crate's routing-layer `worker_binding_of` and
    /// `kian_plugin::hooks::durable_worker_binding` (the hook
    /// resolver's §7.3 floor reader; the crate edges only run
    /// superagent → plugin, so the duplication is by design and pinned
    /// HERE, where both are visible). They agree on every Bound and
    /// Unbound stream; the deliberate divergence is malformed input —
    /// the routing layer refuses (`Malformed`), the floor reader folds
    /// an unreadable value into "absent" and keeps the readable side's
    /// id. Any drift in either reader fails the shape it drifted on.
    #[test]
    fn durable_worker_binding_agrees_with_the_routing_reader() {
        use super::{WorkerBinding, worker_binding_of};
        use kian_plugin::hooks::durable_worker_binding;
        let worker = sid(21);
        let started = |binding: serde_json::Value| {
            event(
                worker,
                5,
                EventKind::ThreadStarted,
                json!({
                    "thread_id": tid(0xee).to_string(),
                    "parent": sid(10).0.to_string(),
                    "worker": worker.0.to_string(),
                    "pinned_version": 7,
                    "task": {},
                    "exec_worker": binding,
                }),
            )
        };
        let input = |binding: serde_json::Value| {
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": {
                    "thread_id": tid(0xee).to_string(),
                    "task": "x",
                    "exec_worker": binding,
                }}),
            )
        };
        // Bound: both readers name the same id.
        let bound = vec![input(json!("mac-mini")), started(json!("mac-mini"))];
        assert_eq!(
            worker_binding_of(&bound, worker),
            WorkerBinding::Bound(std::borrow::Cow::Borrowed("mac-mini"))
        );
        assert_eq!(
            durable_worker_binding(&bound, &worker),
            Some("mac-mini".to_string())
        );
        // Unbound: both read no binding (legacy stream, no keys).
        let mut unbound = worker_stream_first_input(worker);
        unbound.push(event(
            worker,
            2,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(0xee).to_string(),
                "parent": sid(10).0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 7,
                "task": {},
            }),
        ));
        assert_eq!(worker_binding_of(&unbound, worker), WorkerBinding::Unbound);
        assert_eq!(durable_worker_binding(&unbound, &worker), None);
        // Disagreement (two readable, different ids): the routing
        // layer refuses; the floor reader maps it to the honest floor.
        let disagree = vec![input(json!("mac-mini")), started(json!("vultr"))];
        assert_eq!(
            worker_binding_of(&disagree, worker),
            WorkerBinding::Malformed
        );
        assert_eq!(durable_worker_binding(&disagree, &worker), None);
        // Malformed payload key (non-string) + valid wrapper: the
        // deliberate divergence — routing refuses (`Malformed`), the
        // floor reader folds the malformed key into "absent" and keeps
        // the wrapper's id.
        let malformed = vec![input(json!("mac-mini")), started(json!(7))];
        assert_eq!(
            worker_binding_of(&malformed, worker),
            WorkerBinding::Malformed
        );
        assert_eq!(
            durable_worker_binding(&malformed, &worker),
            Some("mac-mini".to_string())
        );
        // Malformed wrapper alone: routing still refuses; the floor
        // reader has no readable id on either side → the floor.
        let malformed_wrapper = vec![input(json!(""))];
        assert_eq!(
            worker_binding_of(&malformed_wrapper, worker),
            WorkerBinding::Malformed
        );
        assert_eq!(durable_worker_binding(&malformed_wrapper, &worker), None);
    }

    /// The worker-side lineage derives the bound payload from the
    /// FIRST input wrapper (D4): identical bytes to the Router side.
    #[test]
    fn lineage_plan_emits_the_bound_payload_from_the_first_wrapper() {
        let router = sid(10);
        let worker = sid(20);
        let mut wrapper = delegation_content(tid(0xee), None, json!("the task"));
        wrapper[super::EXEC_WORKER_KEY] = json!("mac-mini");
        let events = vec![
            event(
                worker,
                0,
                EventKind::SessionCreated,
                serde_json::Value::Null,
            ),
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": wrapper}),
            ),
        ];
        let plan = lineage_plan(&events, worker, router, 7, BoardGating::default());
        let LineagePlan::Emit { started_bytes, .. } = plan else {
            panic!("expected Emit, got {plan:?}");
        };
        // The top-level key carries the wrapper's value verbatim and
        // the task message is the full first input — byte-identical to
        // what the Router side emitted for the same spawn.
        assert_eq!(started_bytes[super::EXEC_WORKER_KEY], json!("mac-mini"));
        assert_eq!(
            started_bytes["task"]["content"][super::EXEC_WORKER_KEY],
            json!("mac-mini")
        );
    }

    // ------------------------------------------------------------------
    // Worker exclusivity (ADR-020, I3/D9)
    // ------------------------------------------------------------------

    #[test]
    fn bound_workers_projects_only_bound_lineage_per_id() {
        let router = sid(10);
        let bound = |t: uuid::Uuid, w: SessionId, id: &str, ts: u64| {
            event(
                router,
                ts,
                EventKind::ThreadStarted,
                json!({
                    "thread_id": t.to_string(),
                    "parent": router.0.to_string(),
                    "worker": w.0.to_string(),
                    "pinned_version": 7,
                    "task": {},
                    "exec_worker": id,
                }),
            )
        };
        let legacy = event(
            router,
            3,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(3).to_string(),
                "worker": sid(33).0.to_string(),
                "task": {},
            }),
        );
        let events = vec![
            bound(tid(1), sid(21), "mac-mini", 100),
            bound(tid(2), sid(22), "vultr", 200),
            bound(tid(4), sid(23), "mac-mini", 300),
            legacy,
        ];
        let map = super::bound_workers(&events);
        assert_eq!(map.len(), 2, "legacy unbound lineage contributes nothing");
        assert_eq!(map["mac-mini"].len(), 2);
        assert_eq!(map["mac-mini"][0].thread_id, tid(1));
        assert_eq!(map["mac-mini"][0].worker_session, sid(21));
        // event() stamps ts = 1_000 + seq, so the seq-100 lineage reads
        // ts 1_100 — the release bound measures from the event's `ts`.
        assert_eq!(map["mac-mini"][0].started_ts, 1_100);
        assert_eq!(map["vultr"][0].thread_id, tid(2));
    }

    #[test]
    fn thread_liveness_follows_the_release_table() {
        use super::ThreadLiveness;
        use kian_core::events::EndReason;
        let worker = sid(20);
        let started = vec![
            event(
                worker,
                0,
                EventKind::SessionCreated,
                serde_json::Value::Null,
            ),
            event(
                worker,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "x"}),
            ),
        ];
        let completed = |seq: u64| {
            event(
                worker,
                seq,
                EventKind::ThreadCompleted,
                json!({"thread_id": tid(1).to_string(), "status": "completed", "summary": {}}),
            )
        };
        let failed = |seq: u64| {
            event(
                worker,
                seq,
                EventKind::ThreadCompleted,
                json!({"thread_id": tid(1).to_string(), "status": "failed", "summary": {}}),
            )
        };
        let ended = |seq: u64, reason: EndReason| {
            event(worker, seq, EventKind::SessionEnded { reason }, json!({}))
        };
        // Started, nothing terminal: live (worker held).
        assert_eq!(super::thread_liveness(&started), ThreadLiveness::Live);
        // thread/completed{completed}: released.
        let mut stream = started.clone();
        stream.push(completed(2));
        assert_eq!(super::thread_liveness(&stream), ThreadLiveness::Released);
        // session/ended{completed} and {cancelled}: released.
        for reason in [EndReason::Completed, EndReason::Cancelled] {
            let mut stream = started.clone();
            stream.push(ended(2, reason));
            assert_eq!(super::thread_liveness(&stream), ThreadLiveness::Released);
        }
        // session/ended{failed} and {interrupted}: still live (resumable).
        for reason in [EndReason::Failed, EndReason::Interrupted] {
            let mut stream = started.clone();
            stream.push(ended(2, reason));
            assert_eq!(super::thread_liveness(&stream), ThreadLiveness::Live);
        }
        // thread/completed{failed} with nothing terminal after: live.
        let mut stream = started.clone();
        stream.push(failed(2));
        assert_eq!(super::thread_liveness(&stream), ThreadLiveness::Live);
        // ...and the last terminal decides: a later completed releases.
        stream.push(ended(3, EndReason::Completed));
        assert_eq!(super::thread_liveness(&stream), ThreadLiveness::Released);
        // No session/created and no input/admitted: never started.
        assert_eq!(super::thread_liveness(&[]), ThreadLiveness::NeverStarted);
    }

    /// A scripted [`super::WorkerStreamsPort`] over fixed streams.
    struct FakeWorkerStreams {
        streams: std::sync::Mutex<std::collections::BTreeMap<SessionId, Vec<Event>>>,
        fail: std::sync::atomic::AtomicBool,
    }

    impl FakeWorkerStreams {
        fn shared() -> Arc<Self> {
            Arc::new(Self {
                streams: std::sync::Mutex::new(std::collections::BTreeMap::new()),
                fail: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn set(&self, session: SessionId, events: Vec<Event>) {
            self.streams.lock().unwrap().insert(session, events);
        }
    }

    impl super::WorkerStreamsPort for FakeWorkerStreams {
        fn read_stream(
            &self,
            session: SessionId,
        ) -> futures::future::BoxFuture<'static, Result<Vec<Event>, super::WorkerStreamsError>>
        {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Box::pin(async {
                    Err(super::WorkerStreamsError("stream unreadable".into()))
                });
            }
            let events = self
                .streams
                .lock()
                .unwrap()
                .get(&session)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(events) })
        }
    }

    #[tokio::test]
    async fn busy_owner_holds_releases_and_fails_closed() {
        let router = sid(10);
        let worker = sid(21);
        let router_events = vec![event(
            router,
            100,
            EventKind::ThreadStarted,
            json!({
                "thread_id": tid(7).to_string(),
                "parent": router.0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 7,
                "task": {},
                "exec_worker": "mac-mini",
            }),
        )];
        let port = FakeWorkerStreams::shared();

        // Live worker session: busy, naming the owner session + thread.
        port.set(
            worker,
            vec![
                event(
                    worker,
                    0,
                    EventKind::SessionCreated,
                    serde_json::Value::Null,
                ),
                event(
                    worker,
                    1,
                    EventKind::InputAdmitted,
                    json!({"role": "user", "content": "x"}),
                ),
            ],
        );
        let owner = super::busy_owner(&router_events, port.as_ref(), "mac-mini", 1_000, 60_000)
            .await
            .expect("decidable")
            .expect("busy");
        assert_eq!(owner.worker_session, worker);
        assert_eq!(owner.thread_id, tid(7));

        // Released (thread/completed{completed}): the worker is free.
        port.set(
            worker,
            vec![
                event(
                    worker,
                    0,
                    EventKind::SessionCreated,
                    serde_json::Value::Null,
                ),
                event(
                    worker,
                    1,
                    EventKind::InputAdmitted,
                    json!({"role": "user", "content": "x"}),
                ),
                event(
                    worker,
                    2,
                    EventKind::ThreadCompleted,
                    json!({"thread_id": tid(7).to_string(), "status": "completed", "summary": {}}),
                ),
            ],
        );
        assert!(
            super::busy_owner(&router_events, port.as_ref(), "mac-mini", 1_000, 60_000)
                .await
                .expect("decidable")
                .is_none()
        );

        // A never-started reservation inside the ttl honestly holds
        // (the lineage event's ts is 1_100 — event() stamps 1_000+seq).
        port.set(worker, Vec::new());
        let owner = super::busy_owner(&router_events, port.as_ref(), "mac-mini", 1_500, 60_000)
            .await
            .expect("decidable")
            .expect("never-started holds inside the ttl");
        assert_eq!(owner.worker_session, worker);

        // Past the ttl (ts=1_100, now=2_000, ttl=500): released.
        assert!(
            super::busy_owner(&router_events, port.as_ref(), "mac-mini", 2_000, 500)
                .await
                .expect("decidable")
                .is_none()
        );

        // An unreadable stream is fail-closed: an honest error, never
        // a spawn.
        port.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            super::busy_owner(&router_events, port.as_ref(), "mac-mini", 1_000, 60_000)
                .await
                .is_err()
        );

        // Another worker's candidates are untouched.
        port.fail.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(
            super::busy_owner(&router_events, port.as_ref(), "vultr", 1_000, 60_000)
                .await
                .expect("decidable")
                .is_none()
        );
    }

    // ------------------------------------------------------------------
    // The derived task identity (R15 plan §4 C2, pins P1/P6)
    // ------------------------------------------------------------------

    /// The identity wrapper composes additively over the fully-composed
    /// legacy form: exactly four more keys (pin P6), every legacy /
    /// mandate / speculative / binding byte identical.
    #[test]
    fn identity_wrapper_is_exactly_the_composed_form_plus_four_keys() {
        let router = sid(11);
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            CapturingInbox::shared(),
        );
        let identity = crate::multiagent::SpawnIdentity::derive(router, 12, "mock-worker", 7);
        let mandate = json!({"strength": "explicit", "scope": "qa"});
        let spawned = spawner.prepare(&super::SpawnRequest {
            task: json!("do the thing"),
            correlation: Some(json!("evt-9")),
            speculative: true,
            mandate: Some(mandate.clone()),
            exec_worker: Some("mac-mini".into()),
            identity: Some(identity.clone()),
            output_schema: None,
        });
        let wrapper = &spawned.task_message["content"];
        // The composed legacy form over the SAME (derived) thread id.
        let mut expected = super::mandate_delegation_content(
            spawned.thread.id,
            Some(json!("evt-9")),
            json!("do the thing"),
            &mandate,
        );
        expected[SPECULATIVE_MARKER] = serde_json::Value::Bool(true);
        expected[super::EXEC_WORKER_KEY] = json!("mac-mini");
        for (key, value) in expected.as_object().expect("composed object") {
            assert_eq!(
                wrapper.get(key),
                Some(value),
                "field `{key}` unchanged by the identity keys"
            );
        }
        // Exactly the four pin-P6 keys added, with the identity values.
        assert_eq!(
            wrapper.as_object().map(serde_json::Map::len),
            Some(expected.as_object().map(serde_json::Map::len).unwrap() + 4),
            "exactly four additive identity keys"
        );
        assert_eq!(wrapper["spawn_uid"], json!(identity.spawn_uid.to_string()));
        assert_eq!(wrapper["task_id"], json!(identity.task_id_str()));
        assert_eq!(wrapper["agent_def"], json!("mock-worker"));
        assert_eq!(wrapper["agent_def_version"], json!(7));
        // `assignee` is P2 (roster) — meaningless before the roster
        // exists, so the P1 wrapper never carries it.
        assert!(wrapper.get("assignee").is_none());
    }

    /// The identity reader refuses partial or malformed identity bytes
    /// — the identity is never guessed, never defaulted.
    #[test]
    fn spawn_identity_of_refuses_partial_or_malformed_identity_bytes() {
        let router = sid(12);
        let identity = crate::multiagent::SpawnIdentity::derive(router, 3, "mock-worker", 7);
        let good = json!({
            "spawn_uid": identity.spawn_uid.to_string(),
            "thread_id": identity.thread_id.to_string(),
            "task_id": identity.task_id.to_string(),
            "agent_def": "mock-worker",
            "agent_def_version": 7,
        });
        assert_eq!(super::spawn_identity_of(&good), Some(identity.clone()));
        // Each missing key alone breaks the round trip.
        for key in [
            "spawn_uid",
            "thread_id",
            "task_id",
            "agent_def",
            "agent_def_version",
        ] {
            let mut partial = good.clone();
            partial.as_object_mut().unwrap().remove(key);
            assert_eq!(super::spawn_identity_of(&partial), None, "missing `{key}`");
        }
        // Malformed values: non-uuid ids, non-string agent_def,
        // fractional version, and null in any slot.
        assert!(
            super::spawn_identity_of(&json!({
                "spawn_uid": "not-a-uuid",
                "task_id": identity.task_id.to_string(),
                "agent_def": "mock-worker",
                "agent_def_version": 7,
            }))
            .is_none()
        );
        assert!(
            super::spawn_identity_of(&json!({
                "spawn_uid": identity.spawn_uid.to_string(),
                "task_id": "not-a-uuid",
                "agent_def": "mock-worker",
                "agent_def_version": 7,
            }))
            .is_none()
        );
        assert!(
            super::spawn_identity_of(&json!({
                "spawn_uid": identity.spawn_uid.to_string(),
                "task_id": identity.task_id.to_string(),
                "agent_def": 42,
                "agent_def_version": 7,
            }))
            .is_none()
        );
        assert!(
            super::spawn_identity_of(&json!({
                "spawn_uid": identity.spawn_uid.to_string(),
                "task_id": identity.task_id.to_string(),
                "agent_def": "mock-worker",
                "agent_def_version": 7.5,
            }))
            .is_none()
        );
        assert!(
            super::spawn_identity_of(&json!({
                "spawn_uid": serde_json::Value::Null,
                "task_id": identity.task_id.to_string(),
                "agent_def": "mock-worker",
                "agent_def_version": 7,
            }))
            .is_none()
        );
        // Not an object at all.
        assert!(super::spawn_identity_of(&json!("plain text")).is_none());
        assert!(super::spawn_identity_of(&serde_json::Value::Null).is_none());
    }

    // ------------------------------------------------------------------
    // The join (R15 plan §4 C4, pins P3/P6, F2)
    // ------------------------------------------------------------------

    /// The fresh-completion report (the C2 ident builder + this
    /// module's `worker_contract`) is byte-for-byte what
    /// `multiagent::reconcile_completion` re-derives from the same
    /// committed events — the precondition for §2.2.1's re-admit being
    /// a genuine no-op. This pins the two derivations together forever.
    #[test]
    fn fresh_report_builder_agrees_byte_for_byte_with_reconcile_completion() {
        let router = sid(13);
        let identity = crate::multiagent::SpawnIdentity::derive(router, 5, "mock-worker", 7);
        let spawner = WorkerSpawner::new(
            crate::binding::ResidentBinding::new(router),
            &crate::templates::worker_template("mock", "mock-worker", 7),
            CapturingInbox::shared(),
        );
        let spawned = spawner.prepare(&super::SpawnRequest {
            task: json!("do the thing"),
            identity: Some(identity.clone()),
            ..super::SpawnRequest::default()
        });
        let wrapper = spawned.task_message["content"].clone();

        let mk = |verdict: Option<bool>| {
            let session = crate::multiagent::child_session_id(identity.task_id);
            let mut events = vec![
                event(session, 1, EventKind::SessionCreated, json!({})),
                event(
                    session,
                    2,
                    EventKind::InputAdmitted,
                    json!({"content": wrapper.clone()}),
                ),
            ];
            if let Some(passed) = verdict {
                events.push(event(
                    session,
                    3,
                    EventKind::EvidenceVerdict,
                    json!({
                        "gate": "worker",
                        "passed": passed,
                        "checks": [
                            {"name": "final_message_present", "passed": true},
                            {"name": "no_error_tool_result_tail", "passed": passed, "detail": "tail=2"},
                        ],
                        "attempt": 1,
                    }),
                ));
            }
            let seq = events.len() as u64 + 1;
            events.push(event(
                session,
                seq,
                EventKind::ThreadCompleted,
                super::thread_completed_payload(
                    identity.task_id,
                    "completed",
                    json!({"text": "all done"}),
                ),
            ));
            events
        };

        for verdict in [None, Some(true), Some(false)] {
            let events = mk(verdict);
            let fresh = super::thread_report_content_ident(
                &identity,
                "completed",
                json!({"text": "all done"}),
                super::worker_contract(&events),
            );
            let crate::multiagent::Reconcile::ReAdmit { key, report } =
                crate::multiagent::reconcile_completion(
                    &events,
                    &wrapper,
                    identity.task_id,
                    "completed",
                    &json!({"text": "all done"}),
                )
            else {
                panic!("a completion-bearing stream must re-admit");
            };
            assert_eq!(
                key,
                format!("spawn:{}", identity.spawn_uid),
                "the pin-P3 key on both paths"
            );
            assert_eq!(
                report, fresh,
                "fresh admit and crash-window re-admit are byte-identical (verdict: {verdict:?})"
            );
        }
    }

    /// `worker_contract` is absent without a committed worker verdict
    /// and ignores the built-in loop gate's verdicts.
    #[test]
    fn worker_contract_reads_only_the_worker_gate_verdict() {
        assert_eq!(super::worker_contract(&[]), None);
        let builtin = event(
            sid(14),
            1,
            EventKind::EvidenceVerdict,
            json!({"gate": "builtin", "passed": true, "checks": [], "attempt": 0}),
        );
        assert_eq!(super::worker_contract(&[builtin]), None);
        let worker = event(
            sid(14),
            2,
            EventKind::EvidenceVerdict,
            json!({
                "gate": "worker",
                "passed": false,
                "checks": [
                    {"name": "final_message_present", "passed": true},
                    {"name": "no_error_tool_result_tail", "passed": false},
                ],
                "attempt": 1,
            }),
        );
        assert_eq!(
            super::worker_contract(&[worker]),
            Some(json!({"passed": false, "errors": ["no_error_tool_result_tail"]}))
        );
    }

    // -- port (e): the settle gate (PLUGIN-SPEC §4.6) -----------------------

    use futures::{FutureExt, StreamExt};
    use kian_core::seams::LoopDriver;
    use kian_plugin::hooks::{
        HandlerId as HookHandlerId, HookBus, HookId, HookPayload, HookReply, Registry,
        SettleVerdict, veto_marker,
    };

    /// An inner driver that replays scripted outcomes (here: always
    /// `Settled` — the loop's own completion decision).
    struct SettledDriver;

    impl LoopDriver for SettledDriver {
        type Ctx = ();

        fn drive(&self, _ctx: ()) -> futures::stream::BoxStream<'static, LoopOutcome> {
            futures::stream::iter(vec![LoopOutcome::Settled]).boxed()
        }
    }

    /// A stream port over a shared, mutable event list.
    #[derive(Clone)]
    struct FakePort {
        session: SessionId,
        events: Arc<std::sync::Mutex<Vec<Event>>>,
    }

    impl FakePort {
        fn new(session: SessionId) -> Self {
            Self {
                session,
                events: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        /// Seeds the first `input/admitted` with the delegation
        /// wrapper — what makes `lineage_plan` classify this stream as
        /// a worker stream (Emit/Resume) rather than a pass-through.
        fn seed_delegation(&self) {
            let mut events = self.events.lock().unwrap();
            let seq = events.len() as u64 + 1;
            events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: seq,
                kind: EventKind::InputAdmitted,
                bytes: json!({
                    "role": "user",
                    "content": super::delegation_content(tid(0xaa), None, json!("do the thing")),
                }),
                version: 0,
            });
        }

        /// Seeds an unanswered `ask`: the assistant turn's `tool_calls`
        /// plus the logged `tool/call` with the ask provenance, and no
        /// `tool/result`. This is exactly the stream a worker leaves
        /// when it ends `awaiting_input` (CONFIRMATION-MODEL phase 2).
        fn seed_pending_ask(&self) {
            let mut events = self.events.lock().unwrap();
            let seq = events.len() as u64 + 1;
            events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: seq,
                kind: EventKind::AssistantMessage,
                bytes: json!({
                    "role": "assistant",
                    "content": {"text": "", "tool_calls": [{
                        "id": "call-ask", "name": "exec",
                        "arguments": {"argv": ["echo", "approve-me"]}
                    }]},
                }),
                version: 0,
            });
            events.push(Event {
                seq: seq + 1,
                session: self.session,
                epoch: Epoch(1),
                ts: seq + 1,
                kind: EventKind::ToolCall,
                bytes: json!({
                    "role": "assistant",
                    "content": {"id": "call-ask", "name": "exec",
                                "arguments": {"argv": ["echo", "approve-me"]}},
                    "permission": {"evaluated": "ask", "policy": "configured",
                                   "matched": "exec"},
                }),
                version: 0,
            });
        }

        /// A **bound** delegation (D4): the wrapper carries the additive
        /// `exec_worker` key naming `exec_worker` — the durable binding
        /// §7.3/§8.2 frames derive.
        fn seed_bound_delegation(&self, exec_worker: &str) {
            let mut events = self.events.lock().unwrap();
            let seq = events.len() as u64 + 1;
            let mut content = super::delegation_content(tid(0xab), None, json!("do the thing"));
            content[super::EXEC_WORKER_KEY] = json!(exec_worker);
            events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: seq,
                kind: EventKind::InputAdmitted,
                bytes: json!({
                    "role": "user",
                    "content": content,
                }),
                version: 0,
            });
        }

        fn seed_marker(&self, plugin: &str) {
            let mut events = self.events.lock().unwrap();
            let seq = events.len() as u64 + 1;
            events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: seq,
                kind: EventKind::InputAdmitted,
                bytes: json!({
                    "role": "user",
                    "synthetic": true,
                    "content": veto_marker(plugin, "settle/pre", "not done yet"),
                }),
                version: 0,
            });
        }

        fn kinds(&self) -> Vec<&'static str> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .map(|e| e.kind.as_str())
                .collect()
        }
    }

    impl super::StreamPort for FakePort {
        fn session(&self) -> SessionId {
            self.session
        }

        fn emit(
            &self,
            kind: EventKind,
            bytes: serde_json::Value,
        ) -> futures::future::BoxFuture<'static, Result<(), super::StreamPortError>> {
            let mut events = self.events.lock().unwrap();
            let seq = events.len() as u64 + 1;
            events.push(Event {
                seq,
                session: self.session,
                epoch: Epoch(1),
                ts: seq,
                kind,
                bytes,
                version: 0,
            });
            Box::pin(async { Ok(()) })
        }

        fn read_stream(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<Vec<Event>, super::StreamPortError>>
        {
            let events = self.events.lock().unwrap().clone();
            Box::pin(async move { Ok(events) })
        }
    }

    /// A bus whose one `settle/pre` subscriber always denies.
    fn deny_bus(hits: Arc<std::sync::Mutex<u32>>) -> (HookBus, kian_plugin::Disposer) {
        let registry = Arc::new(Registry::new());
        let keep = registry.register(
            HookHandlerId {
                plugin: "quality-gate",
                hook: HookId::SettlePre,
                grants: kian_plugin::hooks::GrantSet::from_names(&[
                    "observe:settle",
                    "veto:settle",
                ])
                .unwrap(),
                order: 0,
            },
            Arc::new(move |_payload: &HookPayload| {
                let hits = Arc::clone(&hits);
                async move {
                    *hits.lock().unwrap() += 1;
                    HookReply::BailDeny {
                        reason: "not done yet".to_string(),
                    }
                }
                .boxed()
            }),
        );
        (HookBus::with_registry(registry), keep)
    }

    fn gate_config() -> super::WorkerLoopConfig {
        super::WorkerLoopConfig {
            router: crate::binding::ResidentBinding::new(sid(10)),
            worker_version: 1,
            gate: kian_loop::evidence::EvidenceGateConfig::default(),
            multiagent: None,
            board_gating: multiagent::BoardGating(false),
        }
    }

    async fn drive_once(driver: &super::WorkerLoopDriver<SettledDriver>) -> Vec<LoopOutcome> {
        driver.drive(()).collect().await
    }

    /// One process-wide audit capture for the lib-test binary (no
    /// other test here installs a subscriber). `set_global_default`
    /// rebuilds the callsite Interest cache, so library `tracing`
    /// events are observed regardless of test order — the thread-local
    /// `set_default` pattern would silently lose them.
    fn audit_capture() -> Arc<std::sync::Mutex<String>> {
        use std::io::Write;
        use std::sync::{Arc, Mutex, OnceLock};

        #[derive(Clone)]
        struct Buffer(Arc<Mutex<String>>);
        impl Write for Buffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(buf));
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        static BUFFER: OnceLock<Arc<Mutex<String>>> = OnceLock::new();
        BUFFER
            .get_or_init(|| {
                let buffer = Arc::new(Mutex::new(String::new()));
                let subscriber = tracing_subscriber::fmt()
                    .with_writer({
                        let buffer = Buffer(Arc::clone(&buffer));
                        move || buffer.clone()
                    })
                    .with_ansi(false)
                    .finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("this process installs its global subscriber once");
                buffer
            })
            .clone()
    }

    /// The P2 acceptance: a settle veto re-arms the loop once (the
    /// marker row is admitted, the outcome propagates unchanged, no
    /// completion pair is written), and budget exhaustion overrides —
    /// settlement proceeds WITH the completion pair (§4.6 steps 3–4).
    #[tokio::test]
    async fn settle_veto_rearms_once_then_budget_exhausts_to_override() {
        let audit = audit_capture();
        let worker_session = sid(0x77);
        let port = FakePort::new(worker_session);
        port.seed_delegation();
        let inbox = CapturingInbox::shared();
        let hits = Arc::new(std::sync::Mutex::new(0u32));
        let (bus, _keep_gate) = deny_bus(Arc::clone(&hits));

        let port_for = {
            let port = port.clone();
            Arc::new(move |_: &()| Arc::new(port.clone()) as Arc<dyn super::StreamPort>)
                as Arc<dyn Fn(&()) -> Arc<dyn super::StreamPort> + Send + Sync>
        };
        let driver = super::WorkerLoopDriver::with_hooks(
            Arc::new(SettledDriver),
            inbox.clone(),
            gate_config(),
            port_for,
            Some(bus),
        );

        // Drive 1 — budget empty: deny re-arms. The marker row is the
        // §4.6 step-3 shape, admitted durably; NO thread/completed.
        let outcomes = drive_once(&driver).await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled]);
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1, "one synthetic marker input, exactly");
        assert_eq!(
            admits[0].1,
            json!({
                "synthetic": true,
                "content": veto_marker("quality-gate", "settle/pre", "not done yet"),
            }),
            "the marker is the §4.6 step 3 template through the steer shape"
        );
        assert_eq!(admits[0].0, worker_session);
        assert!(
            !port.kinds().contains(&"thread/completed"),
            "a re-armed settle writes no completion pair"
        );
        assert_eq!(*hits.lock().unwrap(), 1);

        // The re-armed episode consumed the marker (the consumption
        // write persists it as `input/admitted`) and settled again.
        port.seed_marker("quality-gate");

        // Drive 2 — budget 1 of 3: deny re-arms again.
        let outcomes = drive_once(&driver).await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled]);
        assert_eq!(inbox.admits.lock().unwrap().len(), 2);
        assert!(!port.kinds().contains(&"thread/completed"));
        assert_eq!(*hits.lock().unwrap(), 2);

        // Drives 3+4 consume and settle twice more: four markers total.
        port.seed_marker("quality-gate");
        port.seed_marker("quality-gate");

        // Drive 5 — budget exhausted (3 markers in the stream): the
        // veto is recorded but settlement PROCEEDS — the completion
        // pair is written and the outcome is still an honest Settled.
        let outcomes = drive_once(&driver).await;
        assert_eq!(
            outcomes,
            vec![LoopOutcome::Settled],
            "an override never launders the outcome"
        );
        assert!(
            port.kinds().contains(&"thread/completed"),
            "budget exhaustion lets the completion pair land"
        );
        assert_eq!(
            inbox.admits.lock().unwrap().len(),
            3,
            "an override admits no marker — plus the completion report"
        );
        let report = inbox.admits.lock().unwrap()[2].1.clone();
        assert!(
            report.get("status").is_some(),
            "the third admit is the completion report, not a marker"
        );
        assert_eq!(*hits.lock().unwrap(), 3, "the gate was consulted each time");

        // §4.6 step 4 + §10.6: the override emits ONE `hook-override`
        // audit line naming the hook, the plugin and its own count.
        let log = audit.lock().unwrap();
        let line = log
            .lines()
            .find(|l| l.contains("hook-override"))
            .expect("the budget-exhaustion override is audited");
        assert!(line.contains("settle/pre"), "{line}");
        assert!(line.contains("quality-gate"), "{line}");
        assert!(line.contains("vetoes=3"), "{line}");
        assert_eq!(
            log.lines().filter(|l| l.contains("hook-override")).count(),
            1,
            "exactly one override line across the five drives"
        );
    }

    /// §4.6's append-failure path: the gate does NOT settle — the
    /// outcome becomes the driver's honest failure (never `Settled`,
    /// never `completed`), and the session stays recoverable.
    #[tokio::test]
    async fn settle_marker_admit_failure_does_not_settle() {
        struct FailingInbox;

        impl crate::InboxWriter for FailingInbox {
            fn admit(
                &self,
                _session: SessionId,
                _payload: serde_json::Value,
                _key: Option<String>,
            ) -> futures::future::BoxFuture<
                'static,
                Result<crate::AdmitReceipt, crate::InboxWriteError>,
            > {
                Box::pin(async { Err(crate::InboxWriteError::Write("disk full".into())) })
            }

            fn wake(&self, _session: SessionId) -> kian_orchestrator::WakeOutcome {
                kian_orchestrator::WakeOutcome::Folded
            }
        }

        let worker_session = sid(0x78);
        let port = FakePort::new(worker_session);
        port.seed_delegation();
        let hits = Arc::new(std::sync::Mutex::new(0u32));
        let (bus, _keep_gate) = deny_bus(Arc::clone(&hits));
        let port_for = {
            let port = port.clone();
            Arc::new(move |_: &()| Arc::new(port.clone()) as Arc<dyn super::StreamPort>)
                as Arc<dyn Fn(&()) -> Arc<dyn super::StreamPort> + Send + Sync>
        };
        let driver = super::WorkerLoopDriver::with_hooks(
            Arc::new(SettledDriver),
            Arc::new(FailingInbox),
            gate_config(),
            port_for,
            Some(bus),
        );

        let outcomes = drive_once(&driver).await;
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            LoopOutcome::SoftFailed { reason } => assert!(
                reason.contains("settle gate: veto marker admit failed")
                    && reason.contains("disk full"),
                "the honest failure names the append error: {reason}"
            ),
            other => panic!("expected SoftFailed, got {other:?}"),
        }
        assert!(
            !port.kinds().contains(&"thread/completed"),
            "no completion on a failed marker append"
        );
    }

    /// §3.4.4's fourth row: a stream that is NOT a worker stream is a
    /// transparent pass-through — the gate is never consulted, even
    /// mounted, even with a denying subscriber registered.
    #[tokio::test]
    async fn non_worker_stream_never_reaches_the_gate() {
        // The Router's own session: lineage classifies it as
        // pass-through (thread_id None).
        let router_session = sid(10);
        let port = FakePort::new(router_session);
        let hits = Arc::new(std::sync::Mutex::new(0u32));
        let (bus, _keep_gate) = deny_bus(Arc::clone(&hits));
        let port_for = {
            let port = port.clone();
            Arc::new(move |_: &()| Arc::new(port.clone()) as Arc<dyn super::StreamPort>)
                as Arc<dyn Fn(&()) -> Arc<dyn super::StreamPort> + Send + Sync>
        };
        let driver = super::WorkerLoopDriver::with_hooks(
            Arc::new(SettledDriver),
            CapturingInbox::shared(),
            gate_config(),
            port_for,
            Some(bus),
        );

        let outcomes = drive_once(&driver).await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled]);
        assert_eq!(
            *hits.lock().unwrap(),
            0,
            "a non-worker stream never reaches Phase::Terminal's gate"
        );
    }

    /// `SettleVerdict` is reachable from this module's surface (the
    /// gate's decision vocabulary, §4.6 steps 3–5).
    #[test]
    fn settle_verdict_shape() {
        assert_eq!(SettleVerdict::Allow, SettleVerdict::Allow);
    }

    /// §4.6's trust boundary, at the [`GateCtl`] the wrapper injects:
    /// the budget predicate is `synthetic` + the plugin's marker
    /// prefix, **never** `role` — a steer-capable caller can mint a
    /// marker-shaped row, and it counts against the named plugin's
    /// budget (forgeability is the documented behaviour, §17).
    #[test]
    fn steer_minted_marker_consumes_budget() {
        let session = sid(0x7b);
        let marker = |plugin: &str| {
            (
                "user",
                true,
                veto_marker(plugin, "settle/pre", "not done yet"),
            )
        };
        let rows = [
            marker("quality-gate"),
            // An arbitrary role mints equally — the predicate never
            // reads `role`.
            ("assistant", true, marker("quality-gate").2),
            // Not synthetic: never counted, whatever the content.
            ("user", false, marker("quality-gate").2),
            // Another plugin's marker: counts for that plugin only.
            ("user", true, marker("other-gate").2),
            // A plain synthetic row without the prefix: no budget.
            ("user", true, "just a steer note".to_string()),
        ];
        let events: Vec<Event> = rows
            .iter()
            .enumerate()
            .map(|(i, (role, synthetic, content))| Event {
                seq: i as u64 + 1,
                session,
                epoch: Epoch(1),
                ts: i as u64 + 1,
                kind: EventKind::InputAdmitted,
                bytes: json!({
                    "role": role,
                    "synthetic": synthetic,
                    "content": content,
                }),
                version: 0,
            })
            .collect();
        let ctl = super::GateCtl {
            session,
            inbox: CapturingInbox::shared(),
            events,
        };
        assert_eq!(
            kian_plugin::hooks::SettleCtl::veto_count(&ctl, "quality-gate"),
            2
        );
        assert_eq!(
            kian_plugin::hooks::SettleCtl::veto_count(&ctl, "other-gate"),
            1
        );
        assert_eq!(kian_plugin::hooks::SettleCtl::veto_count(&ctl, "absent"), 0);
    }

    /// (B1/§8.2/§9.2 end-to-end): the wrapper's drain-boundary frame
    /// derives the session's durable `exec_worker` binding from the
    /// stream, so a worker-scoped `use:` narrowing **actually removes**
    /// a mounted instance from this session's dispatches, and the §9.2
    /// digest the guard records is the narrowed set's — not the server
    /// scope's.
    #[tokio::test]
    async fn worker_scoped_narrowing_removes_a_mounted_instance_and_records_the_narrowed_digest() {
        let worker_session = sid(0x79);
        let port = FakePort::new(worker_session);
        port.seed_bound_delegation("node-zulu");

        // A mounted "gate" instance, deny-on-settle, registered under
        // the sidecar's instance name so the §8.2 dispatch filter owns
        // its fate.
        let registry = Arc::new(Registry::new());
        let hits = Arc::new(std::sync::Mutex::new(0u32));
        let counter = Arc::clone(&hits);
        let keep_gate = registry.register(
            HookHandlerId {
                plugin: "gate",
                hook: HookId::SettlePre,
                grants: kian_plugin::hooks::GrantSet::from_names(&[
                    "observe:settle",
                    "veto:settle",
                ])
                .unwrap(),
                order: 0,
            },
            Arc::new(move |_payload: &HookPayload| {
                let counter = Arc::clone(&counter);
                async move {
                    *counter.lock().unwrap() += 1;
                    HookReply::BailDeny {
                        reason: "not done yet".to_string(),
                    }
                }
                .boxed()
            }),
        );
        let bus = HookBus::with_registry(registry);
        // Boot snapshot: the server scope mounts `gate`, but
        // `node-zulu`'s `use:` narrows it away (§8.2's closed grammar).
        let cfg: kian_plugin::hooks::resolve::SidecarConfig = serde_json::from_value(json!({
            "plugins": {"gate": {"manifest": "/etc/kian/plugins/gate.toml"}},
            "scopes": {
                "server": ["gate"],
                "worker": {"node-zulu": {"use": []}}
            }
        }))
        .unwrap();
        let manifest_text = format!(
            "[plugin]\nname = \"gate\"\nversion = \"0.1.0\"\nabi = 1\n\
             [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
             [grants]\nhooks = [\"settle/pre\"]\ncapabilities = [\"observe:settle\", \"veto:settle\"]\n\
             [limits]\ndeadline_ms = 500\n",
            "a".repeat(64)
        );
        let mctx = kian_plugin::hooks::sidecar::ValidationCtx {
            instance_key: "gate",
            ..Default::default()
        };
        let manifest =
            kian_plugin::hooks::sidecar::parse_manifest(&manifest_text, &mctx).expect("manifest");
        let mut manifests = std::collections::BTreeMap::new();
        manifests.insert("gate".to_string(), manifest);
        bus.install_sidecars(cfg, manifests).expect("boot install");

        let port_for = {
            let port = port.clone();
            Arc::new(move |_: &()| Arc::new(port.clone()) as Arc<dyn super::StreamPort>)
                as Arc<dyn Fn(&()) -> Arc<dyn super::StreamPort> + Send + Sync>
        };
        let driver = super::WorkerLoopDriver::with_hooks(
            Arc::new(SettledDriver),
            CapturingInbox::shared(),
            gate_config(),
            port_for,
            Some(bus.clone()),
        );

        // One drive: the §9.2 guard opens the session through the
        // stream-derived binding and the settle gate consults with the
        // narrowed set — the gate is absent, so the honest Settled
        // propagates to the terminal decoration.
        let outcomes = drive_once(&driver).await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled]);
        assert_eq!(
            *hits.lock().unwrap(),
            0,
            "the narrowed-away instance was never consulted"
        );

        // §9.2: the recorded digest is the NARROWED set's — a
        // server-scope session on the same bus records a different one.
        let narrowed = bus
            .session_set_digest(&worker_session)
            .expect("the guard recorded the worker session");
        let plain_session = sid(0x7a);
        bus.session_begin(kian_plugin::hooks::FrameInfo::new(plain_session));
        let server = bus
            .session_set_digest(&plain_session)
            .expect("the guard recorded the plain session");
        assert_ne!(
            narrowed, server,
            "the worker session's digest is its narrowed set's, not the server scope's"
        );

        // §8.2 through the production view: settle_view derives the
        // binding from the same stream, and a dispatch under that frame
        // skips the removed instance — while the same dispatch under
        // the server scope consults and denies.
        let events = port.events.lock().unwrap().clone();
        let view = super::settle_view(&events, worker_session);
        assert_eq!(
            view.frame.as_ref().expect("frame").worker,
            "node-zulu",
            "the settle frame carries the durable binding"
        );
        let ctl = std::sync::Arc::new(super::GateCtl {
            session: worker_session,
            inbox: CapturingInbox::shared(),
            events: events.clone(),
        });
        let verdict = bus.settle(&view, ctl.as_ref()).await;
        assert!(
            matches!(verdict, SettleVerdict::Allow),
            "the removed instance cannot veto: {verdict:?}"
        );
        assert_eq!(*hits.lock().unwrap(), 0);

        // Control: the same gate, consulted under a SERVER-scope session
        // (a fresh session with no worker binding — §8.4 memoizes the
        // worker session's set, so the control rides its own identity),
        // denies as designed.
        let mut server_frame_view = super::settle_view(&events, worker_session);
        server_frame_view.frame.as_mut().unwrap().session = plain_session;
        server_frame_view.frame.as_mut().unwrap().worker = String::new();
        let verdict = bus.settle(&server_frame_view, ctl.as_ref()).await;
        assert!(
            matches!(verdict, SettleVerdict::Deny { .. }),
            "the server scope still consults the gate: {verdict:?}"
        );
        assert_eq!(*hits.lock().unwrap(), 1);
        drop(keep_gate);
    }

    /// CONFIRMATION-MODEL phase 2: a worker that ends on a pending `ask`
    /// writes NO completion pair (`thread/completed` + parent report) —
    /// it is waiting on a person, not done — and the same session later
    /// completes the pair after the answer.
    #[tokio::test]
    async fn a_worker_waiting_on_an_ask_writes_no_completion_pair() {
        let worker_session = sid(0x88);
        let port = FakePort::new(worker_session);
        port.seed_delegation();
        port.seed_pending_ask();
        let inbox = CapturingInbox::shared();
        let port_for = {
            let port = port.clone();
            Arc::new(move |_: &()| Arc::new(port.clone()) as Arc<dyn super::StreamPort>)
                as Arc<dyn Fn(&()) -> Arc<dyn super::StreamPort> + Send + Sync>
        };
        let driver = super::WorkerLoopDriver::new(
            Arc::new(SettledDriver),
            inbox.clone(),
            gate_config(),
            port_for,
        );

        let outcomes: Vec<LoopOutcome> = driver.drive(()).collect().await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled], "{outcomes:?}");
        assert!(
            inbox.admits.lock().unwrap().is_empty(),
            "a waiting worker writes no parent completion report"
        );
        assert!(
            port.events
                .lock()
                .unwrap()
                .iter()
                .all(|ev| ev.kind != EventKind::ThreadCompleted),
            "a waiting worker writes no thread/completed"
        );
    }
}
