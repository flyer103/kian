//! Thread delegation (card C25): `WorkerSpawner` + `WorkerLoopDriver`.
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
//! The `"speculative": true` marker (C39) is additive and optional:
//! [`delegation_content`] never emits it (legacy bytes are unchanged,
//! golden-tested) and [`speculative_delegation_content`] adds exactly
//! one key. The marker is model-visible by design — the worker's first
//! message *is* the wrapper, so a speculatively spawned worker reads
//! that it was spawned on a weak signal — and audit-visible forever:
//! the wrapper bytes live in both streams (the Router-side and
//! worker-side `thread/started` payloads embed the task message, and
//! the worker's `input/admitted` carries the wrapper itself).
//!
//! ## Lineage events (the C18 additions-only kinds)
//!
//! - Router side: the Router's own execution hold appends
//!   `thread/started` at dispatch, built by [`thread_started_payload`]
//!   (the C26 `spawn_worker` tool does this in assembly; tests and the
//!   audit helper consume the same bytes).
//! - Worker side: [`WorkerLoopDriver`] appends `thread/started` at its
//!   first drive when the stream lacks one and the initial input
//!   carries the wrapper — **idempotent by stream scan**, so a takeover
//!   after a killed hold never duplicates lineage (constraint 1).
//! - Completion: on the inner loop's terminal outcome the wrapper runs
//!   the C24 evidence gate (pure predicates, no model call), appends
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

/// Gate name the worker wrapper stamps on its verdicts (distinct from
/// the built-in loop's `builtin` so both can coexist on one stream).
pub const WORKER_GATE_NAME: &str = "worker";

/// Idempotency key for a spawned worker's task input row (one row per
/// thread, never two).
fn task_input_key(thread_id: Uuid) -> String {
    format!("thread:{thread_id}")
}

/// Idempotency key for the completion report row the worker admits to
/// the Router's inbox.
fn report_key(thread_id: Uuid) -> String {
    format!("thread-report:{thread_id}")
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

/// The speculative marker key (C39): present (and `true`) iff the
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

/// True iff `content` is a delegation wrapper carrying the C39
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

/// The speculative delegation wrapper (C39): exactly
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
// The mandate object (C60 / ADR-012)
// ---------------------------------------------------------------------------

/// The mandate key (C60, ADR-012): present iff the delegation carries
/// an authority object. An additive optional field inside the wrapper
/// `content` — the frozen fields (`thread_id`/`correlation`/`task`),
/// the C39 marker, and every legacy wrapper byte are unchanged. The
/// markers compose: a speculative delegation may carry a mandate too
/// (both are additive wrapper keys).
pub const MANDATE_KEY: &str = "mandate";

/// The mandate delegation wrapper (C60, ADR-012): exactly
/// [`delegation_content`] plus `"mandate"` — the C39 marker mechanism
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

/// The result of revalidating a live worker's mandate before a
/// follow-up routes to it (ADR-012's revalidation rule, composed with
/// the C59 live-routing path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MandateRevalidation {
    /// The mandate authorizes the follow-up: `strength: explicit`
    /// always passes; `inferred` passes while its `policy_version` is
    /// still the current pointer. Legacy delegations (no mandate
    /// object) pass — pre-C60 authority is never retroactively
    /// narrowed.
    Pass,
    /// `strength: inferred` under a `policy_version` that is no longer
    /// the current pointer: the follow-up releases to fresh
    /// evaluation (the C59 `released_to_eval` path) — honestly
    /// audited, never a silent drop. `policy_version` is the mandate's
    /// persisted label, for the audit line.
    Release { policy_version: String },
}

/// Pure mandate revalidation (ADR-012): `strength: explicit` passes;
/// `inferred` passes iff its `policy_version` equals the current
/// policy pointer (the C63 label; `None` on either side reads as the
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

/// The `thread/started` payload (C18 §2 schema):
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

/// The `thread/completed` payload (C18 §2 schema):
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

/// The structured report state (C56): the closed vocabulary the stage
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

/// The additive state key on stated completion reports (C56). Omitted
/// entirely by [`thread_report_content`] (legacy bytes unchanged, the
/// C39 marker precedent).
pub const REPORT_STATE_KEY: &str = "state";

/// The stated completion-report content (C56): exactly
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

/// A parsed completion report found inside a message `content`.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadReport {
    pub thread_id: Uuid,
    pub status: String,
    pub summary: Value,
    /// The additive structured state (C56): `None` on legacy reports
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
/// session. Assembly (kian-cli, C27) builds one `Arc<dyn StreamPort>`
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
    /// The Router-profile speculation budget is exhausted (C39): the
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

/// Mints a `Thread` and creates the worker session by admitting its
/// task input (a fresh session id; the first hold appends
/// `session/created`), wakes it, and returns the thread id. The
/// `pinned_version` comes from the worker template's `AgentDef`
/// version (ADR-006: pinned for the thread's lifetime, never swapped).
pub struct WorkerSpawner {
    router: SessionId,
    pinned_version: u64,
    /// The worker template's id (C60): the key the Router's mandate
    /// policy resolves per-template scope text against.
    worker_template_id: String,
    inbox: Arc<dyn InboxWriter>,
}

impl WorkerSpawner {
    /// `worker_template` supplies the pinned `AgentDef` version.
    pub fn new(router: SessionId, worker_template: &AgentDef, inbox: Arc<dyn InboxWriter>) -> Self {
        Self {
            router,
            pinned_version: worker_template.version,
            worker_template_id: worker_template.id.clone(),
            inbox,
        }
    }

    /// The worker template's id — the mandate scope key (C60,
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

    /// [`WorkerSpawner::spawn`] plus the authority object (C60,
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

    /// Spawns one worker **speculatively** (C39): identical mechanics
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
    /// (C60): the C39 and C60 markers compose — the wrapper carries
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

    async fn spawn_inner(
        &self,
        task: Value,
        correlation: Option<Value>,
        speculative: bool,
        mandate: Option<Value>,
    ) -> Result<SpawnedWorker, SpawnError> {
        let thread = Thread {
            id: Uuid::new_v4(),
            parent_session: self.router,
            pinned_version: self.pinned_version,
        };
        let worker_session = SessionId::new();
        // The C60 wrapper composes the public builders: the mandate
        // key lands through mandate_delegation_content (exactly
        // delegation_content plus one key), the speculative marker
        // through the C39 spelling — both additive, both optional, and
        // absent together the bytes are the frozen legacy form.
        let mut wrapper = match &mandate {
            Some(mandate) => mandate_delegation_content(thread.id, correlation, task, mandate),
            None => delegation_content(thread.id, correlation, task),
        };
        if speculative {
            wrapper[SPECULATIVE_MARKER] = Value::Bool(true);
        }
        let task_message = json!({"role": "user", "content": wrapper});
        // The admitted payload *is* the message content (the server's
        // consumption write wraps it as {"role","content"}).
        self.inbox
            .admit(worker_session, wrapper, Some(task_input_key(thread.id)))
            .await?;
        self.inbox.wake(worker_session);
        Ok(SpawnedWorker {
            thread,
            worker_session,
            task_message,
        })
    }
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
#[must_use]
pub fn lineage_plan(
    events: &[Event],
    self_session: SessionId,
    router: SessionId,
    worker_version: u64,
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
    let Some(delegation) = first_input.bytes.get("content").and_then(parse_delegation) else {
        return LineagePlan::PassThrough;
    };
    let thread = Thread {
        id: delegation.thread_id,
        parent_session: router,
        pinned_version: worker_version,
    };
    LineagePlan::Emit {
        thread_id: delegation.thread_id,
        started_bytes: thread_started_payload(&thread, self_session, &first_input.bytes),
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
// WorkerLoopDriver
// ---------------------------------------------------------------------------

/// Tuning for [`WorkerLoopDriver`].
#[derive(Debug, Clone)]
pub struct WorkerLoopConfig {
    /// The resident Router session: where completion reports are
    /// admitted (the configured id from assembly, single-Router v1).
    pub router: SessionId,
    /// The worker template's pinned `AgentDef` version (ADR-006) —
    /// stamped into the worker-side `thread/started`.
    pub worker_version: u64,
    /// The C24 evidence gate evaluated at the terminal outcome.
    /// Default (disabled) appends no verdict and completes ungated —
    /// Phase-1 inner behavior unchanged.
    pub gate: EvidenceGateConfig,
}

/// A decorator over any inner [`LoopDriver`] that adds Thread lineage
/// (C25): `thread/started` at first drive (idempotent by stream scan —
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
    /// (admit, then wake — both via the C23 port); `port_for` builds
    /// the [`StreamPort`] for each execution hold from its context
    /// (assembly's job — see [`PortFor`]).
    pub fn new(
        driver: Arc<D>,
        inbox: Arc<dyn InboxWriter>,
        config: WorkerLoopConfig,
        port_for: PortFor<D::Ctx>,
    ) -> Self {
        Self {
            inner: Arc::new(DriverCore {
                driver,
                inbox,
                config,
                port_for,
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
                                Ok(thread_id) => {
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

/// Phase 1: classify the stream and, for a fresh worker, append the
/// `thread/started` lineage event. Pure decision via [`lineage_plan`].
async fn begin_lineage<D>(
    core: &DriverCore<D>,
    port: Arc<dyn StreamPort>,
) -> Result<Option<Uuid>, String>
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
        core.config.router,
        core.config.worker_version,
    ) {
        LineagePlan::PassThrough => Ok(None),
        LineagePlan::Resume { thread_id } => Ok(Some(thread_id)),
        LineagePlan::Emit {
            thread_id,
            started_bytes,
        } => {
            port.emit(EventKind::ThreadStarted, started_bytes)
                .await
                .map_err(|e| format!("worker lineage: thread/started write failed: {e}"))?;
            Ok(Some(thread_id))
        }
    }
}

/// Phase 3: the terminal outcome. Gate → verdict → `thread/completed`
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
    let mut status = "completed";
    let mut final_outcome = outcome;

    if matches!(final_outcome, LoopOutcome::SoftFailed { .. }) {
        // Honest failure: no gate, no verdict — the thread records
        // `failed` (constraint 7: never completed).
        status = "failed";
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
            status = "failed";
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
    if events
        .iter()
        .any(|ev| ev.kind == EventKind::ThreadCompleted && event_thread_id(ev) == Some(thread_id))
    {
        return (final_outcome, Phase::Done);
    }
    let fallback = match &final_outcome {
        LoopOutcome::SoftFailed { reason } => reason.clone(),
        _ => String::new(),
    };
    let summary = completion_summary(&events, &fallback);
    if let Err(e) = port
        .emit(
            EventKind::ThreadCompleted,
            thread_completed_payload(thread_id, status, summary.clone()),
        )
        .await
    {
        return (
            LoopOutcome::SoftFailed {
                reason: format!("worker completion: thread/completed write failed: {e}"),
            },
            Phase::Done,
        );
    }
    let report = thread_report_content(thread_id, status, summary);
    if let Err(e) = core
        .inbox
        .admit(core.config.router, report, Some(report_key(thread_id)))
        .await
    {
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
    core.inbox.wake(core.config.router);
    (final_outcome, Phase::Done)
}

#[cfg(test)]
mod tests {
    use kian_core::model::Epoch;
    use serde_json::json;
    use std::sync::Arc;

    use super::{
        Delegation, DelegationPair, LineagePlan, MANDATE_KEY, MandateRevalidation,
        SPECULATIVE_MARKER, SpawnError, StreamPortError, WorkerSpawner, completion_summary,
        delegation_content, delegation_is_speculative, delegation_mandate, delegation_pairs,
        lineage_plan, mandate_delegation_content, parse_delegation, parse_thread_report,
        revalidate_mandate, speculative_delegation_content, thread_completed_payload,
        thread_report_content, thread_started_payload,
    };
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
    // Speculative delegation wrapper (C39)
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
        // false/null) and its serialized bytes are exactly the C25
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
            router,
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

    #[tokio::test]
    async fn plain_spawn_still_admits_an_unmarked_wrapper() {
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            router,
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
    // The mandate wrapper (C60 / ADR-012)
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
        // C39 + C60 compose: one wrapper carries both additive keys, so
        // even a weak-signal spawn runs under recorded authority.
        let router = sid(10);
        let inbox = CapturingInbox::shared();
        let spawner = WorkerSpawner::new(
            router,
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
            router,
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
        // Legacy delegations (no object) pass; pre-C60 authority is
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
        // too (the pointer moved — unset is a drift, C63's rule).
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
        // The C56 additive key (the C39 marker precedent): the legacy
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
        let plan = lineage_plan(&events, worker, router, 7);
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
            lineage_plan(&events, worker, router, 7),
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
            lineage_plan(&events, router, router, 1),
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
            lineage_plan(&[other], router, router, 1),
            LineagePlan::PassThrough
        );
    }

    // ------------------------------------------------------------------
    // Completion summary
    // ------------------------------------------------------------------

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
            super::report_key(tid(1)),
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
}
