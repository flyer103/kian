//! The frozen plan — the declarative form of one job, its canonical
//! bytes, its digests, and the identity derivations every replica
//! recomputes (`design/JOB-MODEL.md` §6, §7).
//!
//! Pure by construction: no I/O, no clock, no store, no ambient config.
//! The config loader (`kian-cli`) validates an operator's `jobs:`
//! declaration, resolves defaults and freezes the result into a run's
//! first `input/admitted` row; the conductor re-reads that frozen value,
//! and from then on the *stream* is the plan's home — editing the config
//! never changes an in-flight run (§4 J2).
//!
//! Two properties are load-bearing and tested here:
//!
//! - `canonical_json` (recursively sorted keys, no insignificant
//!   whitespace) makes `plan_digest` a function of meaning rather than of
//!   declaration order (§7.3);
//! - every identifier is `uuid5` over a logical triple
//!   (`(job, slot)`, `(run, step, attempt)`), so a dispatch replayed after
//!   a crash recomputes exactly the same ids (§4 J4).

use std::collections::{BTreeMap, BTreeSet};

use kian_core::model::SessionId;
use kian_core::worker::{MAX_CAPABILITIES, valid_capability_name};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The pinned namespace every job identifier lives under (§6):
/// `uuid5(Uuid::NAMESPACE_URL, "kian.dev/ns/job")`, frozen as a constant
/// because changing it re-derives every run session id and orphans every
/// in-flight run.
pub const KIAN_NS: u128 = 0x99be60b5_c4d7_52ec_8563_6de7b8697856;

/// Prefix of every digest this contract writes (§7.3).
pub const DIGEST_PREFIX: &str = "sha256:";

/// Inbox idempotency-key prefix of a run's first input (`run_input_key`).
pub const RUN_INPUT_KEY_PREFIX: &str = "kian:job:run:";

/// The v5 name a step's spawn identity is derived from (§6).
pub const STEP_SPAWN_V5_PREFIX: &str = "kian:job:step:";

/// The slot prefix of an alert run (§13.3).
pub const ALERT_SLOT_PREFIX: &str = "alert:";

/// One attempt's deadline when a step does not declare `timeout_ms`.
pub const DEFAULT_STEP_TIMEOUT_MS: u64 = 300_000;

/// A run's wall-clock budget when `limits.deadline_ms` is not declared.
pub const DEFAULT_DEADLINE_MS: u64 = 1_800_000;

/// How long a run may wait with every candidate busy before failing.
pub const DEFAULT_DEFER_BUDGET_MS: u64 = 300_000;

/// Above this size an artifact may not be inlined into a consumer's task
/// message (§12.2).
pub const DEFAULT_INLINE_MAX_BYTES: u64 = 65_536;

/// The largest artifact this contract accepts (§12.1): 256 KiB.
pub const MAX_ARTIFACT_BYTES: u64 = 262_144;

/// Extra attempts beyond the first (§7.2).
pub const MAX_RETRIES: u8 = 5;

/// A plan's step ceiling (§7.4 V3).
pub const MAX_STEPS: usize = 32;

/// The shortest honest step budget: below it a provider turn cannot
/// finish, so the value would encode failure (§7.4 V8).
pub const MIN_STEP_TIMEOUT_MS: u64 = 1_000;

/// The shortest run budget (§7.4 V9).
pub const MIN_DEADLINE_MS: u64 = 60_000;

/// `limits.max_parallel` in v1 (§7.4 V10).
pub const MAX_PARALLEL_V1: u32 = 1;

/// How many consecutive failed runs close the manual-trigger breaker when a
/// job does not declare `limits.consecutive_failures`
/// (SELF-EVOLUTION-MODEL §10.2).
pub const DEFAULT_CONSECUTIVE_FAILURES: u32 = 3;

/// The breaker ceiling: a larger streak is a deployment problem, not a knob
/// (SELF-EVOLUTION-MODEL §14 V25).
pub const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// The branch a `publish` step's pull request targets when it declares none
/// (SELF-EVOLUTION-MODEL §14 V31, A15).
pub const DEFAULT_PUBLISH_BASE: &str = "main";

/// A job's plan, frozen: every default filled in, every id validated
/// (§7.3). Serialising this struct *is* the canonical frozen form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobPlan {
    /// The job id (§7.4 V1).
    pub id: String,
    /// Run-wide limits; every field has an operator-visible default.
    #[serde(default)]
    pub limits: JobLimits,
    /// Steps in declaration order — the dispatch order (§8.2).
    pub steps: Vec<StepPlan>,
}

/// Run-wide limits (§7.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobLimits {
    /// Wall-clock budget for the whole run, measured from the run's
    /// input ts (§8.7).
    #[serde(default = "default_deadline_ms")]
    pub deadline_ms: u64,
    /// Steps dispatched at once. v1 is serial (§7.4 V10).
    #[serde(default = "default_max_parallel")]
    pub max_parallel: u32,
    /// Soft worker preference inherited by steps that declare none (§9.3).
    #[serde(default)]
    pub prefer: Vec<String>,
    /// Steps whose `unverified` outcome counts as success for readiness
    /// (§8.1). The run's own outcome stays `unverified`.
    #[serde(default)]
    pub accept_unverified: Vec<String>,
    /// How long "every candidate is busy" may last before the step fails
    /// `no_candidate` instead of waiting (§9.4).
    #[serde(default = "default_defer_budget_ms")]
    pub defer_budget_ms: u64,
    /// The largest artifact carried inline in a consumer's task message
    /// (§12.2).
    #[serde(default = "default_inline_max_bytes")]
    pub inline_max_bytes: u64,
    /// How many consecutive settled runs of this job may fail before a manual
    /// trigger is refused (SELF-EVOLUTION-MODEL §10.2). Zero disables the
    /// breaker; the value is part of the frozen plan, so a run cannot raise
    /// it.
    ///
    /// The default is omitted from the frozen bytes, which keeps every
    /// plan_digest written before this key existed valid: the frozen form
    /// still spells out what the plan means, and "absent" means exactly the
    /// default.
    #[serde(
        default = "default_consecutive_failures",
        skip_serializing_if = "is_default_consecutive_failures"
    )]
    pub consecutive_failures: u32,
}

impl Default for JobLimits {
    fn default() -> Self {
        Self {
            deadline_ms: DEFAULT_DEADLINE_MS,
            max_parallel: MAX_PARALLEL_V1,
            prefer: Vec::new(),
            accept_unverified: Vec::new(),
            defer_budget_ms: DEFAULT_DEFER_BUDGET_MS,
            inline_max_bytes: DEFAULT_INLINE_MAX_BYTES,
            consecutive_failures: DEFAULT_CONSECUTIVE_FAILURES,
        }
    }
}

/// One step of a plan (§7.2).
///
/// A single flat struct, not an enum per action: the frozen form is the
/// digest's input, so its key set must not depend on the action. The
/// deliver-only fields are meaningful only when
/// `action == StepAction::Deliver`, and §7.4 V16 enforces exactly that.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepPlan {
    /// Step id, unique in the plan (§7.4 V1/V3).
    pub id: String,
    /// What kind of step this is.
    #[serde(default)]
    pub action: StepAction,
    /// The task message. Required for `agent` (V3), absent for `deliver`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Capabilities the worker must declare (§7.4 V6). Empty = any worker.
    #[serde(default)]
    pub requires: Vec<String>,
    /// Exact worker id; a hard constraint, unlike `prefer` (§9.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<String>,
    /// Admit an `Offline` worker as the last-ranked candidate (§9.2).
    #[serde(default)]
    pub allow_offline: bool,
    /// Soft worker preference for *this* step (§9.3).
    #[serde(default)]
    pub prefer: Vec<String>,
    /// Steps that must reach a success outcome first (§8.1).
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Artifact references this step consumes (§12).
    #[serde(default)]
    pub inputs: Vec<ArtifactInput>,
    /// The artifact this step produces, if any (§12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produces: Option<ArtifactSpec>,
    /// Structured-output schema handed to the child (§8.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// One attempt's budget (§8.5).
    #[serde(default = "default_step_timeout_ms")]
    pub timeout_ms: u64,
    /// Extra attempts: total = `retries + 1` (§8.6).
    #[serde(default)]
    pub retries: u8,
    /// Whether repeating the step is harmless (§10).
    #[serde(default)]
    pub effect: Effect,
    /// Declares that repeating the effect is harmless (§10.1).
    #[serde(default)]
    pub idempotent: bool,
    /// Declarative evidence predicates (§11).
    #[serde(default)]
    pub verify: Vec<JobCheck>,
    /// What the run does when this step finally fails (§11.3).
    #[serde(default)]
    pub on_failure: OnFailure,
    /// A failed optional step does not fail the run (§11.3).
    #[serde(default)]
    pub optional: bool,
    /// `deliver` only: the channel adapter to send through (§13).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// `deliver` only: the adapter-specific target (§13.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<Value>,
    /// `deliver` only: what to send (§13.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<DeliverContent>,
    /// `publish` only: the frozen publish shape (SELF-EVOLUTION-MODEL §9.4,
    /// A15). The field serializes only when present, so a plan written before
    /// the action existed keeps its digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<PublishSpec>,
}

impl StepPlan {
    /// Whether this step is executed by the conductor rather than placed
    /// on a worker (§13.1).
    #[must_use]
    pub fn is_deliver(&self) -> bool {
        matches!(self.action, StepAction::Deliver)
    }

    /// Whether this step publishes an artifact as a conductor action
    /// (SELF-EVOLUTION-MODEL §9.4).
    #[must_use]
    pub fn is_publish(&self) -> bool {
        matches!(self.action, StepAction::Publish)
    }

    /// The publish facts, present exactly when the step is a well-formed
    /// `publish` step (V27 validated them at load).
    #[must_use]
    pub fn publish(&self) -> Option<&PublishSpec> {
        if !self.is_publish() {
            return None;
        }
        self.publish.as_ref()
    }

    /// The deliver facts, present exactly when the step is a well-formed
    /// `deliver` step (V16 validated them at load).
    #[must_use]
    pub fn deliver(&self) -> Option<DeliverSpecRef<'_>> {
        if !self.is_deliver() {
            return None;
        }
        Some(DeliverSpecRef {
            channel: self.channel.as_deref()?,
            target: self.target.as_ref()?,
            content: self.content.as_ref()?,
        })
    }

    /// The artifact name this step produces, if it declares one.
    #[must_use]
    pub fn produces_name(&self) -> Option<&str> {
        self.produces.as_ref().map(|p| p.name.as_str())
    }

    /// The frozen canonical bytes of this step, as its digest's input.
    #[must_use]
    pub fn frozen_json(&self) -> Value {
        to_value_or_null(self)
    }

    /// `step_digest` (§6): the step's canonical bytes, hashed.
    #[must_use]
    pub fn digest(&self) -> String {
        digest_json(&self.frozen_json())
    }
}

/// The borrowed deliver facts of a `deliver` step.
#[derive(Debug, Clone, Copy)]
pub struct DeliverSpecRef<'a> {
    /// The channel adapter name.
    pub channel: &'a str,
    /// The adapter-specific target, exactly as the adapter receives it.
    pub target: &'a Value,
    /// The frozen content — a literal or an artifact reference.
    pub content: &'a DeliverContent,
}

impl JobPlan {
    /// The step with this id, if the plan declares it.
    #[must_use]
    pub fn step(&self, id: &str) -> Option<&StepPlan> {
        self.steps.iter().find(|s| s.id == id)
    }

    /// The frozen plan's canonical JSON — the exact value a run freezes
    /// in its first input row (§5.2).
    #[must_use]
    pub fn frozen_json(&self) -> Value {
        to_value_or_null(self)
    }

    /// `plan_digest` (§6, §7.3).
    #[must_use]
    pub fn digest(&self) -> String {
        digest_json(&self.frozen_json())
    }

    /// Whether the plan declares a patch surface, which is what makes its runs
    /// self-dev runs that require a verified evidence pack
    /// (SELF-EVOLUTION-MODEL §14 V24).
    #[must_use]
    pub fn declares_surface(&self) -> bool {
        self.steps.iter().any(|step| {
            step.verify
                .iter()
                .any(|check| matches!(check, JobCheck::PatchPathsAllowed { .. }))
        })
    }
}

/// What a step does (§2.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepAction {
    /// A worker-bound session turn, placed by capability (§2.3).
    #[default]
    Agent,
    /// A conductor-executed send through the channel adapter (§13.1).
    Deliver,
    /// A conductor-executed publish of one artifact (SELF-EVOLUTION-MODEL
    /// §9.4): a derived worktree, a commit, a gate and a receipt.
    Publish,
}

/// A step's retry class (§10).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// Repeating it is harmless.
    #[default]
    Read,
    /// Repeating it may duplicate the effect.
    Write,
}

/// The run's behaviour when a step finally fails (§11.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// The run fails at this step and dispatches nothing more.
    #[default]
    Halt,
    /// Independent steps still run; the run still ends `failed` unless
    /// the step is `optional`.
    Continue,
}

/// How an artifact is carried (§12.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactMedia {
    /// A slice of the producer's own event bytes — a reference, no store,
    /// no TTL (§12).
    #[default]
    Text,
    /// A content-addressed blob under the artifact root. Designed, not
    /// v1 (§21.2).
    Binary,
}

/// One artifact a step consumes (§7.2 `inputs`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
    /// The producer step id.
    pub step: String,
    /// The producer's `produces.name`.
    pub name: String,
}

/// One artifact a step produces (§7.2 `produces`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSpec {
    /// Artifact name, unique within its producer.
    pub name: String,
    /// How the bytes are carried; v1 accepts `text` only (V11).
    #[serde(default)]
    pub media: ArtifactMedia,
    /// The cap: breaching it fails the step, never truncates (§12.2).
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
}

/// What a `deliver` step sends (§7.2, §13.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverContent {
    /// Literal text, shaped by the channel's IM policy before sending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// A reference to an artifact produced by an earlier step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_artifact: Option<ArtifactInput>,
}

/// The frozen shape of a `publish` step (SELF-EVOLUTION-MODEL §9.4, A15).
///
/// The plan chooses *what* is published — its single input — and the branch a
/// pull request would target. It cannot choose the repository, the gate or the
/// credential: those are the operator's configuration (§9.5), so a run cannot
/// pick a trivial gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishSpec {
    /// The branch a pull request targets (S2b); validated by V31.
    #[serde(default = "default_publish_base")]
    pub base: String,
    /// Whether the configured gate runs before the receipt (§9.4 step 8).
    #[serde(default = "default_publish_gates")]
    pub gates: bool,
    /// S2b: push the branch to the configured remote. S2a leaves it false.
    #[serde(default)]
    pub push: bool,
    /// S2b: open the pull request. Requires `push` (V31).
    #[serde(default)]
    pub pull_request: bool,
}

fn default_publish_base() -> String {
    DEFAULT_PUBLISH_BASE.to_string()
}

const fn default_publish_gates() -> bool {
    true
}

/// One declarative evidence check (§11.2).
///
/// The first four reuse `kian_loop::evidence::CheckSpec` semantics
/// verbatim ([`JobCheck::to_check_spec`]); the last two are job-owned
/// because they are predicates over the *run's* facts, not the child's
/// stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "check", rename_all = "snake_case")]
pub enum JobCheck {
    /// The child ended with a final answer (§11.2).
    FinalMessagePresent,
    /// The child actually acted after its last input (§11.2).
    ToolResultSinceLastUserInput,
    /// The trailing `k` tool results contain no error (§11.2).
    NoErrorToolResultTail {
        /// Window size.
        k: u64,
    },
    /// The child produced at least `n` tool results (§11.2).
    MinToolResults {
        /// Minimum count.
        n: u64,
    },
    /// The named artifact exists and is within its declared cap (§12.2).
    ArtifactWithinCap {
        /// The artifact name the producer declared.
        name: String,
    },
    /// A `channel/delivered` receipt exists for this attempt (§13.2).
    DeliveryReceipt {
        /// The channel the receipt must name.
        channel: String,
        /// The frozen target the receipt must name.
        #[serde(default)]
        target: Value,
    },
    /// The publish receipt on this attempt satisfies §9.4 (A17).
    PublishOk,
    /// The patch artifact changes only paths inside the declared surface
    /// (SELF-EVOLUTION-MODEL §5.3, A13).
    PatchPathsAllowed {
        /// The artifact holding the unified diff; it must be produced by the
        /// step that declares the check.
        artifact: String,
        /// Repository-relative globs every changed path must match at least
        /// one of.
        #[serde(default)]
        allow: Vec<String>,
        /// Repository-relative globs no changed path may match.
        #[serde(default)]
        deny: Vec<String>,
    },
}

impl JobCheck {
    /// The check's name as it appears inside an `evidence/verdict` (§11.1).
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::FinalMessagePresent => "final_message_present",
            Self::ToolResultSinceLastUserInput => "tool_result_since_last_user_input",
            Self::NoErrorToolResultTail { .. } => "no_error_tool_result_tail",
            Self::MinToolResults { .. } => "min_tool_results",
            Self::ArtifactWithinCap { .. } => "artifact_within_cap",
            Self::DeliveryReceipt { .. } => "delivery_receipt",
            Self::PublishOk => "publish_ok",
            Self::PatchPathsAllowed { .. } => "patch_paths_allowed",
        }
    }

    /// Whether the existing gate evaluator owns this check. Job-owned
    /// checks return `None` and are evaluated by the conductor.
    #[must_use]
    pub fn to_check_spec(&self) -> Option<kian_loop::evidence::CheckSpec> {
        match *self {
            Self::FinalMessagePresent => Some(kian_loop::evidence::CheckSpec::FinalMessagePresent),
            Self::ToolResultSinceLastUserInput => {
                Some(kian_loop::evidence::CheckSpec::ToolResultSinceLastUserInput)
            }
            Self::NoErrorToolResultTail { k } => {
                Some(kian_loop::evidence::CheckSpec::NoErrorToolResultTail { k })
            }
            Self::MinToolResults { n } => {
                Some(kian_loop::evidence::CheckSpec::MinToolResults { n })
            }
            Self::ArtifactWithinCap { .. }
            | Self::DeliveryReceipt { .. }
            | Self::PublishOk
            | Self::PatchPathsAllowed { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical bytes and digests (§7.3)
// ---------------------------------------------------------------------------

/// The canonical bytes of a value: every object key sorted, no
/// insignificant whitespace. Two plans that mean the same thing have the
/// same digest regardless of declaration order — and defaults are part of
/// the meaning because the frozen form always spells them out.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    match serde_json::to_string(&sorted_value(value)) {
        Ok(text) => text,
        Err(error) => {
            debug_assert!(false, "a JSON value is always serializable: {error}");
            String::new()
        }
    }
}

/// Recursively rebuild a value with sorted object keys.
///
/// Deliberately explicit rather than relying on `serde_json::Map` being a
/// `BTreeMap` today: enabling the `preserve_order` feature anywhere in the
/// workspace would silently reorder every frozen plan, change every
/// digest and re-derive every run's identity.
#[must_use]
pub fn sorted_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<&str, Value> = map
                .iter()
                .map(|(k, v)| (k.as_str(), sorted_value(v)))
                .collect();
            let mut out = serde_json::Map::with_capacity(sorted.len());
            for (key, value) in sorted {
                out.insert(key.to_string(), value);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted_value).collect()),
        other => other.clone(),
    }
}

/// `sha256:<hex>` over the canonical bytes of `value`.
#[must_use]
pub fn digest_json(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_json(value).as_bytes());
    let digest = hasher.finalize();
    format!("{DIGEST_PREFIX}{}", hex(digest.as_ref()))
}

/// `sha256:<hex>` over the raw bytes of `text`. An artifact digest covers the
/// exact bytes its producer committed, never a re-serialization (§12.1).
#[must_use]
pub fn digest_text(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    format!("{DIGEST_PREFIX}{}", hex(digest.as_ref()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn to_value_or_null<T: Serialize>(value: &T) -> Value {
    match serde_json::to_value(value) {
        Ok(value) => value,
        Err(error) => {
            debug_assert!(false, "a job plan is always serializable: {error}");
            Value::Null
        }
    }
}

// ---------------------------------------------------------------------------
// Identity derivations (§6)
// ---------------------------------------------------------------------------
//
// Every identifier is a pure function of a logical triple. That is what
// makes dispatch idempotent across a crash and a takeover (J4): the
// conductor never stores an id, it recomputes it.

fn uuid5(namespace: Uuid, name: &str) -> Uuid {
    Uuid::new_v5(&namespace, name.as_bytes())
}

/// The namespace a job's runs live under.
#[must_use]
pub fn job_namespace(job_id: &str) -> Uuid {
    uuid5(Uuid::from_u128(KIAN_NS), &format!("kian:job:{job_id}"))
}

/// `{job}@{slot}` — the run's stable name in operator surfaces (§5.2).
#[must_use]
pub fn run_id(job_id: &str, slot: &str) -> String {
    format!("{job_id}@{slot}")
}

/// The session that carries one run (§5.1).
#[must_use]
pub fn run_session(job_id: &str, slot: &str) -> SessionId {
    SessionId(uuid5(job_namespace(job_id), &format!("run:{slot}")))
}

/// The run's first input row's idempotency key: one run per slot, ever.
#[must_use]
pub fn run_input_key(run: &str) -> String {
    format!("{RUN_INPUT_KEY_PREFIX}{run}")
}

/// A step attempt's spawn identity, seeded by the logical triple — never
/// by a sequence number, sequence numbers change under re-drive.
#[must_use]
pub fn spawn_uid(run: SessionId, step: &str, attempt: u32) -> Uuid {
    uuid5(run.0, &format!("{STEP_SPAWN_V5_PREFIX}{step}:{attempt}"))
}

/// The child thread id of one attempt.
#[must_use]
pub fn child_thread_id(run: SessionId, spawn: Uuid) -> Uuid {
    uuid5(run.0, &format!("kian:thread:{spawn}"))
}

/// The child session id of one attempt.
#[must_use]
pub fn child_session_id(thread: Uuid) -> SessionId {
    SessionId(uuid5(thread, "kian:session"))
}

/// The child's task id, the identity its spawn roots at.
#[must_use]
pub fn step_task_id(run: SessionId, step: &str, attempt: u32) -> Uuid {
    uuid5(run.0, &format!("kian:task:{step}:{attempt}"))
}

/// The slot of the alert run that reports `failed_run_id` (§13.3).
#[must_use]
pub fn alert_slot(failed_run: &str) -> String {
    format!("{ALERT_SLOT_PREFIX}{failed_run}")
}

/// The run id of the alert that reports `failed_run_id` (§13.3).
#[must_use]
pub fn alert_run_id(alert_job: &str, failed_run: &str) -> String {
    run_id(alert_job, &alert_slot(failed_run))
}

// ---------------------------------------------------------------------------
// Load-time validation (§7.4)
// ---------------------------------------------------------------------------
//
// Every rule here is plan-local: it can decide without the config file, the
// channel registry or the worker catalog. The config-context rules (V2,
// V6's pin half, V12, V13, V15, V16's registry half, V18-V20) live in the
// loader that owns those facts. A violation names its rule so an operator
// sees the contract, not a parser message.

/// One refused declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanViolation {
    /// The rule that refused it, e.g. `V4`.
    pub rule: &'static str,
    /// The field path, e.g. `steps[2].depends_on[0]`.
    pub path: String,
    /// What is wrong, in operator language.
    pub message: String,
}

impl std::fmt::Display for PlanViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} ({})", self.path, self.message, self.rule)
    }
}

fn violation(
    rule: &'static str,
    path: impl Into<String>,
    message: impl Into<String>,
) -> PlanViolation {
    PlanViolation {
        rule,
        path: path.into(),
        message: message.into(),
    }
}

/// Validate a frozen plan. Returns every violation, not just the first:
/// an operator fixing a config should see the whole list.
#[must_use]
pub fn validate_plan(plan: &JobPlan) -> Vec<PlanViolation> {
    let mut out = Vec::new();

    if plan.steps.is_empty() {
        out.push(violation(
            "V3",
            "steps",
            "a plan declares at least one step",
        ));
    }
    if plan.steps.len() > MAX_STEPS {
        out.push(violation(
            "V3",
            "steps",
            format!(
                "a plan declares at most {MAX_STEPS} steps, found {}",
                plan.steps.len()
            ),
        ));
    }

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (index, step) in plan.steps.iter().enumerate() {
        let path = format!("steps[{index}]");
        if !seen.insert(step.id.as_str()) {
            out.push(violation(
                "V3",
                format!("{path}.id"),
                format!("duplicate step id {:?}", step.id),
            ));
        }
        validate_step(plan, index, step, &mut out);
    }

    let publish_steps = plan.steps.iter().filter(|step| step.is_publish()).count();
    if publish_steps > 1 {
        out.push(violation(
            "V28",
            "steps",
            "invalid publish step: a plan declares at most one publish step",
        ));
    }
    if publish_steps > 0 && !plan.declares_surface() {
        out.push(violation(
            "V29",
            "steps",
            "publish requires a declared surface: no step declares patch_paths_allowed",
        ));
    }

    validate_limits(plan, &mut out);
    validate_graph(plan, &mut out);

    let any_check = plan.steps.iter().any(|s| !s.verify.is_empty());
    let any_deliver = plan.steps.iter().any(StepPlan::is_deliver);
    if !any_check && !any_deliver {
        out.push(violation(
            "V21",
            "steps",
            "no step declares a verify check: a run that cannot be checked cannot succeed",
        ));
    }

    if plan.limits.max_parallel != MAX_PARALLEL_V1 {
        out.push(violation(
            "V10",
            "limits.max_parallel",
            format!(
                "v1 admits one step at a time; found {}",
                plan.limits.max_parallel
            ),
        ));
    }

    out
}

fn validate_limits(plan: &JobPlan, out: &mut Vec<PlanViolation>) {
    let limits = &plan.limits;
    if limits.deadline_ms < MIN_DEADLINE_MS {
        out.push(violation(
            "V9",
            "limits.deadline_ms",
            format!("a run budget is at least {MIN_DEADLINE_MS} ms"),
        ));
    }
    for (index, worker) in limits.prefer.iter().enumerate() {
        validate_prefer_entry(&format!("limits.prefer[{index}]"), worker, out);
    }
    if limits.max_parallel == 0 {
        out.push(violation(
            "V10",
            "limits.max_parallel",
            "must be at least 1",
        ));
    }
    if limits.inline_max_bytes == 0 || limits.inline_max_bytes > MAX_ARTIFACT_BYTES {
        out.push(violation(
            "V11",
            "limits.inline_max_bytes",
            format!("must be in 1..={MAX_ARTIFACT_BYTES}"),
        ));
    }
    if limits.consecutive_failures > MAX_CONSECUTIVE_FAILURES {
        out.push(violation(
            "V25",
            "limits.consecutive_failures",
            format!(
                "a breaker streak is at most {MAX_CONSECUTIVE_FAILURES}, found {}",
                limits.consecutive_failures
            ),
        ));
    }
}

fn validate_step(plan: &JobPlan, index: usize, step: &StepPlan, out: &mut Vec<PlanViolation>) {
    let path = format!("steps[{index}]");

    if !valid_step_id(&step.id) {
        out.push(violation(
            "V3",
            format!("{path}.id"),
            format!("invalid step id {:?}", step.id),
        ));
    }

    match step.action {
        StepAction::Agent => {
            if step.prompt.as_deref().unwrap_or("").trim().is_empty() {
                out.push(violation(
                    "V3",
                    format!("{path}.prompt"),
                    "an agent step declares a prompt",
                ));
            }
        }
        StepAction::Deliver => validate_deliver(&path, plan, step, out),
        StepAction::Publish => validate_publish(&path, step, out),
    }

    if step.action != StepAction::Publish && step.publish.is_some() {
        out.push(violation(
            "V27",
            format!("{path}.publish"),
            "invalid publish step: the publish field belongs to a publish step",
        ));
    }

    if step.requires.len() > MAX_CAPABILITIES {
        out.push(violation(
            "V6",
            format!("{path}.requires"),
            format!(
                "at most {MAX_CAPABILITIES} capabilities, found {}",
                step.requires.len()
            ),
        ));
    }
    for (at, capability) in step.requires.iter().enumerate() {
        if !valid_capability_name(capability) {
            out.push(violation(
                "V6",
                format!("{path}.requires[{at}]"),
                format!("invalid capability name {capability:?}"),
            ));
        }
    }

    if matches!(step.effect, Effect::Write) && step.retries > 0 && !step.idempotent {
        out.push(violation(
            "V7",
            format!("{path}.retries"),
            "a write step retries only when it declares idempotent: true",
        ));
    }

    if step.retries > MAX_RETRIES {
        out.push(violation(
            "V8",
            format!("{path}.retries"),
            format!("at most {MAX_RETRIES} retries, found {}", step.retries),
        ));
    }
    if step.timeout_ms < MIN_STEP_TIMEOUT_MS {
        out.push(violation(
            "V8",
            format!("{path}.timeout_ms"),
            format!("a step budget is at least {MIN_STEP_TIMEOUT_MS} ms"),
        ));
    }
    let worst_case = step.timeout_ms.saturating_mul(u64::from(step.retries) + 1);
    if worst_case > plan.limits.deadline_ms {
        out.push(violation(
            "V8",
            format!("{path}.timeout_ms"),
            format!(
                "timeout_ms * (retries + 1) = {worst_case} exceeds limits.deadline_ms = {}",
                plan.limits.deadline_ms
            ),
        ));
    }

    if let Some(produces) = &step.produces {
        if produces.name.trim().is_empty() {
            out.push(violation(
                "V11",
                format!("{path}.produces.name"),
                "an artifact needs a name",
            ));
        }
        if produces.max_bytes == 0 || produces.max_bytes > MAX_ARTIFACT_BYTES {
            out.push(violation(
                "V11",
                format!("{path}.produces.max_bytes"),
                format!("must be in 1..={MAX_ARTIFACT_BYTES}"),
            ));
        }
        if matches!(produces.media, ArtifactMedia::Binary) {
            out.push(violation(
                "V11",
                format!("{path}.produces.media"),
                "binary artifacts are P3 (§21.2); v1 carries text only",
            ));
        }
    }

    for (at, input) in step.inputs.iter().enumerate() {
        let at_path = format!("{path}.inputs[{at}]");
        match plan.step(&input.step) {
            None => out.push(violation(
                "V5",
                &at_path,
                format!("step {:?} is not in this plan", input.step),
            )),
            Some(producer) => {
                if !producer
                    .produces_name()
                    .iter()
                    .any(|name| *name == input.name)
                {
                    out.push(violation(
                        "V5",
                        &at_path,
                        format!(
                            "step {:?} does not produce artifact {:?}",
                            input.step, input.name
                        ),
                    ));
                }
            }
        }
    }

    if let Some(schema) = &step.output_schema {
        if !schema.is_object() {
            out.push(violation(
                "V17",
                format!("{path}.output_schema"),
                "must be a JSON object",
            ));
        }
        if canonical_json(schema).len() > 16 * 1024 {
            out.push(violation(
                "V17",
                format!("{path}.output_schema"),
                "an output schema is at most 16 KiB",
            ));
        }
    }

    let mut patch_checks = 0_usize;
    for (at, check) in step.verify.iter().enumerate() {
        let at_path = format!("{path}.verify[{at}]");
        match check {
            JobCheck::ArtifactWithinCap { name } if name.trim().is_empty() => out.push(violation(
                "V21",
                &at_path,
                "artifact_within_cap names the artifact to check",
            )),
            JobCheck::DeliveryReceipt { channel, .. } if channel.trim().is_empty() => {
                out.push(violation(
                    "V21",
                    &at_path,
                    "delivery_receipt names the channel that must have delivered",
                ))
            }
            JobCheck::PatchPathsAllowed {
                artifact,
                allow,
                deny,
            } => {
                patch_checks += 1;
                validate_surface(&at_path, artifact, allow, deny, step, out);
            }
            _ => {}
        }
    }

    if patch_checks > 1 {
        out.push(violation(
            "V23",
            format!("{path}.verify"),
            "a step declares at most one patch_paths_allowed check",
        ));
    }

    for (at, worker) in step.prefer.iter().enumerate() {
        validate_prefer_entry(&format!("{path}.prefer[{at}]"), worker, out);
    }
}

fn validate_surface(
    path: &str,
    artifact: &str,
    allow: &[String],
    deny: &[String],
    step: &StepPlan,
    out: &mut Vec<PlanViolation>,
) {
    let produced = step.produces.as_ref().map(|spec| spec.name.as_str());
    if produced != Some(artifact) {
        out.push(violation(
            "V23",
            format!("{path}.artifact"),
            format!("patch_paths_allowed names {artifact:?}, which this step does not produce"),
        ));
    }
    if allow.is_empty() {
        out.push(violation(
            "V23",
            format!("{path}.allow"),
            "the surface declares at least one allow glob",
        ));
    }
    if deny.is_empty() {
        out.push(violation(
            "V23",
            format!("{path}.deny"),
            "the surface declares its deny globs, so the forbidden paths are frozen with the plan",
        ));
    }
    for (at, entry) in allow.iter().enumerate() {
        if !valid_surface_glob(entry) {
            out.push(violation(
                "V23",
                format!("{path}.allow[{at}]"),
                format!("not a repository-relative glob: {entry:?}"),
            ));
        }
    }
    for (at, entry) in deny.iter().enumerate() {
        if !valid_surface_glob(entry) {
            out.push(violation(
                "V23",
                format!("{path}.deny[{at}]"),
                format!("not a repository-relative glob: {entry:?}"),
            ));
        }
    }
}

fn valid_surface_glob(entry: &str) -> bool {
    if entry.trim().is_empty() || entry.starts_with('/') || entry.contains('\\') {
        return false;
    }
    entry
        .split('/')
        .all(|segment| !segment.is_empty() && segment != ".." && segment != ".")
}

fn validate_publish(path: &str, step: &StepPlan, out: &mut Vec<PlanViolation>) {
    if step.publish.is_none() {
        out.push(violation(
            "V27",
            format!("{path}.publish"),
            "invalid publish step: a publish step declares its publish block",
        ));
    }
    if step.inputs.len() != 1 {
        out.push(violation(
            "V27",
            format!("{path}.inputs"),
            "invalid publish step: exactly one input names the artifact to publish",
        ));
    }
    if step.retries != 0 {
        out.push(violation(
            "V27",
            format!("{path}.retries"),
            "invalid publish step: a publish is never retried, so retries is 0",
        ));
    }
    if !matches!(step.effect, Effect::Write) {
        out.push(violation(
            "V27",
            format!("{path}.effect"),
            "invalid publish step: a publish writes, so effect is write",
        ));
    }
    if step.idempotent {
        out.push(violation(
            "V27",
            format!("{path}.idempotent"),
            "invalid publish step: a publish is not idempotent",
        ));
    }
    let forbidden = |field: &str, present: bool, out: &mut Vec<PlanViolation>| {
        if present {
            out.push(violation(
                "V27",
                format!("{path}.{field}"),
                format!("invalid publish step: a publish step declares no {field}"),
            ));
        }
    };
    forbidden("prompt", step.prompt.is_some(), out);
    forbidden("requires", !step.requires.is_empty(), out);
    forbidden("pin", step.pin.is_some(), out);
    forbidden("prefer", !step.prefer.is_empty(), out);
    forbidden("allow_offline", step.allow_offline, out);
    forbidden("produces", step.produces.is_some(), out);
    forbidden("channel", step.channel.is_some(), out);
    forbidden("target", step.target.is_some(), out);
    forbidden("content", step.content.is_some(), out);

    if let Some(input) = step.inputs.first()
        && !step.depends_on.iter().any(|dep| dep == &input.step)
    {
        out.push(violation(
            "V27",
            format!("{path}.depends_on"),
            format!(
                "invalid publish step: depends_on names the producer {:?}",
                input.step
            ),
        ));
    }

    let Some(publish) = &step.publish else {
        return;
    };
    if publish.pull_request && !publish.push {
        out.push(violation(
            "V31",
            format!("{path}.publish.pull_request"),
            "invalid publish target: pull_request needs push",
        ));
    }
    if publish.base.trim().is_empty() || publish.base.chars().any(char::is_whitespace) {
        out.push(violation(
            "V31",
            format!("{path}.publish.base"),
            "invalid publish target: a branch name without whitespace",
        ));
    }
}

fn validate_deliver(path: &str, plan: &JobPlan, step: &StepPlan, out: &mut Vec<PlanViolation>) {
    if step.channel.as_deref().unwrap_or("").trim().is_empty() {
        out.push(violation(
            "V16",
            format!("{path}.channel"),
            "a deliver step names a channel",
        ));
    }
    if step.target.is_none() {
        out.push(violation(
            "V16",
            format!("{path}.target"),
            "a deliver step names a target",
        ));
    }
    match (&step.content, step.prompt.is_some()) {
        (None, _) => out.push(violation(
            "V16",
            format!("{path}.content"),
            "a deliver step declares content",
        )),
        (Some(content), _) => {
            let has_text = content.text.is_some();
            let has_ref = content.from_artifact.is_some();
            if has_text == has_ref {
                out.push(violation(
                    "V16",
                    format!("{path}.content"),
                    "content is exactly one of text or from_artifact",
                ));
            }
        }
    }
    if step.prompt.is_some() {
        out.push(violation(
            "V16",
            format!("{path}.prompt"),
            "a deliver step has no prompt",
        ));
    }
    if !step.requires.is_empty()
        || step.pin.is_some()
        || step.retries > 0
        || step.output_schema.is_some()
    {
        out.push(violation(
            "V16",
            path,
            "a deliver step declares no requires, pin, retries or output_schema: it is never placed",
        ));
    }
    if let Some(content) = &step.content
        && let Some(reference) = &content.from_artifact
    {
        let at_path = format!("{path}.content.from_artifact");
        match plan.step(&reference.step) {
            None => out.push(violation(
                "V5",
                &at_path,
                format!("step {:?} is not in this plan", reference.step),
            )),
            Some(producer) => {
                if !producer
                    .produces_name()
                    .iter()
                    .any(|name| *name == reference.name)
                {
                    out.push(violation(
                        "V5",
                        &at_path,
                        format!(
                            "step {:?} does not produce artifact {:?}",
                            reference.step, reference.name
                        ),
                    ));
                }
            }
        }
    }
}

fn validate_prefer_entry(path: &str, worker: &str, out: &mut Vec<PlanViolation>) {
    if worker.trim().is_empty() || worker.len() > 128 {
        out.push(violation(
            "V22",
            path,
            "a prefer entry is 1..=128 bytes and non-empty",
        ));
    }
}

fn validate_graph(plan: &JobPlan, out: &mut Vec<PlanViolation>) {
    let ids: BTreeSet<&str> = plan.steps.iter().map(|s| s.id.as_str()).collect();

    for (index, step) in plan.steps.iter().enumerate() {
        let path = format!("steps[{index}].depends_on");
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for (at, dep) in step.depends_on.iter().enumerate() {
            if !seen.insert(dep.as_str()) {
                out.push(violation(
                    "V4",
                    format!("{path}[{at}]"),
                    format!("duplicate dependency {dep:?}"),
                ));
                continue;
            }
            if dep == &step.id {
                out.push(violation(
                    "V4",
                    format!("{path}[{at}]"),
                    "a step cannot depend on itself",
                ));
            } else if !ids.contains(dep.as_str()) {
                out.push(violation(
                    "V4",
                    format!("{path}[{at}]"),
                    format!("step {dep:?} is not in this plan"),
                ));
            }
        }
    }

    if let Some(cycle) = find_cycle(plan) {
        out.push(violation(
            "V4",
            "steps",
            format!("dependency cycle through {}", cycle.join(" -> ")),
        ));
        return;
    }

    let reached = reachable_steps(plan);
    for (index, step) in plan.steps.iter().enumerate() {
        if !reached.contains(step.id.as_str()) {
            out.push(violation(
                "V14",
                format!("steps[{index}]"),
                "no root step reaches this one: it could never be dispatched",
            ));
        }
    }
}

/// Kahn's algorithm; returns the ids still holding a degree, which is a
/// cycle (and names it for the operator).
fn find_cycle(plan: &JobPlan) -> Option<Vec<String>> {
    let mut indegree: BTreeMap<&str, usize> = BTreeMap::new();
    for step in &plan.steps {
        indegree.insert(step.id.as_str(), step.depends_on.len());
    }
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut settled = 0usize;
    while let Some(id) = ready.pop() {
        settled += 1;
        for step in &plan.steps {
            let dependents = step.depends_on.iter().filter(|dep| *dep == id).count();
            if dependents == 0 {
                continue;
            }
            if let Some(degree) = indegree.get_mut(step.id.as_str()) {
                *degree = degree.saturating_sub(dependents);
                if *degree == 0 {
                    ready.push(step.id.as_str());
                }
            }
        }
    }
    if settled == plan.steps.len() {
        return None;
    }
    let mut cycle: Vec<String> = indegree
        .into_iter()
        .filter(|(_, degree)| *degree > 0)
        .map(|(id, _)| id.to_string())
        .collect();
    cycle.sort();
    Some(cycle)
}

fn reachable_steps(plan: &JobPlan) -> BTreeSet<&str> {
    let mut reached: BTreeSet<&str> = plan
        .steps
        .iter()
        .filter(|s| s.depends_on.is_empty())
        .map(|s| s.id.as_str())
        .collect();
    loop {
        let mut changed = false;
        for step in &plan.steps {
            if reached.contains(step.id.as_str()) {
                continue;
            }
            if step
                .depends_on
                .iter()
                .all(|dep| reached.contains(dep.as_str()))
            {
                reached.insert(step.id.as_str());
                changed = true;
            }
        }
        if !changed {
            return reached;
        }
    }
}

fn valid_step_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 64 {
        return false;
    }
    let mut chars = id.chars();
    let first = chars.next().unwrap_or(' ');
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' || c == '.')
}

const fn default_deadline_ms() -> u64 {
    DEFAULT_DEADLINE_MS
}

const fn default_max_parallel() -> u32 {
    MAX_PARALLEL_V1
}

const fn default_defer_budget_ms() -> u64 {
    DEFAULT_DEFER_BUDGET_MS
}

const fn default_consecutive_failures() -> u32 {
    DEFAULT_CONSECUTIVE_FAILURES
}

const fn is_default_consecutive_failures(value: &u32) -> bool {
    *value == DEFAULT_CONSECUTIVE_FAILURES
}

const fn default_inline_max_bytes() -> u64 {
    DEFAULT_INLINE_MAX_BYTES
}

const fn default_step_timeout_ms() -> u64 {
    DEFAULT_STEP_TIMEOUT_MS
}

const fn default_max_bytes() -> u64 {
    DEFAULT_INLINE_MAX_BYTES
}

// ---------------------------------------------------------------------------
// Tests (L1): canonical bytes, digests, identities, rules
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plan(value: serde_json::Value) -> JobPlan {
        match serde_json::from_value(value) {
            Ok(plan) => plan,
            Err(error) => panic!("the fixture is a valid plan: {error}"),
        }
    }

    /// A minimal valid plan: one agent step, one verify check.
    fn minimal() -> JobPlan {
        plan(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "verify": [{"check": "final_message_present"}]
            }]
        }))
    }

    fn rules(plan: &JobPlan) -> Vec<&'static str> {
        validate_plan(plan).into_iter().map(|v| v.rule).collect()
    }

    #[test]
    fn canonical_json_sorts_keys_recursively() {
        let value = json!({"z": 1, "a": {"y": 2, "b": 3}, "m": [{"q": 1, "c": 2}]});
        assert_eq!(
            canonical_json(&value),
            r#"{"a":{"b":3,"y":2},"m":[{"c":2,"q":1}],"z":1}"#
        );
    }

    #[test]
    fn canonical_json_is_idempotent() {
        let value = json!({"b": {"d": 1, "c": 2}, "a": [1, 2, {"z": 0, "y": 1}]});
        let once = canonical_json(&value);
        assert_eq!(once, canonical_json(&sorted_value(&value)));
    }

    #[test]
    fn digest_text_hashes_the_raw_bytes() {
        assert_eq!(
            digest_text("abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(
            digest_text("abc"),
            digest_json(&json!("abc")),
            "an artifact digest covers its bytes, not their JSON encoding"
        );
    }

    #[test]
    fn plan_digest_ignores_key_order_but_not_meaning() {
        let a = minimal();
        let b = plan(json!({
            "steps": [{
                "verify": [{"check": "final_message_present"}],
                "prompt": "fetch the news",
                "action": "agent",
                "id": "fetch"
            }],
            "id": "digest"
        }));
        assert_eq!(a.digest(), b.digest());

        let c = plan(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "timeout_ms": 300001,
                "verify": [{"check": "final_message_present"}]
            }]
        }));
        assert_ne!(a.digest(), c.digest());
    }

    #[test]
    fn defaults_are_part_of_the_frozen_digest() {
        let explicit_default = plan(json!({
            "id": "digest",
            "limits": {"deadline_ms": 1_800_000, "max_parallel": 1, "defer_budget_ms": 300_000,
                       "inline_max_bytes": 65_536, "prefer": [], "accept_unverified": []},
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "requires": [],
                "allow_offline": false,
                "prefer": [],
                "depends_on": [],
                "inputs": [],
                "timeout_ms": 300_000,
                "retries": 0,
                "effect": "read",
                "idempotent": false,
                "on_failure": "halt",
                "optional": false,
                "verify": [{"check": "final_message_present"}]
            }]
        }));
        assert_eq!(minimal().digest(), explicit_default.digest());
        assert_eq!(minimal().frozen_json(), explicit_default.frozen_json());
    }

    #[test]
    fn step_digest_is_stable_and_distinguishes_steps() {
        let one = minimal();
        let two = plan(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "verify": [{"check": "final_message_present"}]
            }, {
                "id": "summarise",
                "action": "agent",
                "prompt": "summarise it",
                "depends_on": ["fetch"],
                "verify": [{"check": "final_message_present"}]
            }]
        }));
        let fetch = two.step("fetch").expect("declared");
        assert_eq!(
            fetch.digest(),
            one.step("fetch").expect("declared").digest()
        );
        assert_ne!(
            fetch.digest(),
            two.step("summarise").expect("declared").digest()
        );
        assert!(fetch.digest().starts_with(DIGEST_PREFIX));
        assert_eq!(fetch.digest().len(), DIGEST_PREFIX.len() + 64);
    }

    #[test]
    fn run_identities_are_deterministic_and_slot_scoped() {
        let run = run_session("digest", "2026-11-23T01:00:00Z");
        assert_eq!(run, run_session("digest", "2026-11-23T01:00:00Z"));
        assert_ne!(run, run_session("digest", "2026-11-24T01:00:00Z"));
        assert_ne!(run, run_session("other", "2026-11-23T01:00:00Z"));
        assert_eq!(run_id("digest", "manual:abc"), "digest@manual:abc");
        assert_eq!(
            run_input_key("digest@manual:abc"),
            "kian:job:run:digest@manual:abc"
        );
    }

    #[test]
    fn step_identities_are_attempt_scoped_and_replayable() {
        let run = run_session("digest", "slot");
        let first = spawn_uid(run, "fetch", 1);
        assert_eq!(first, spawn_uid(run, "fetch", 1));
        assert_ne!(first, spawn_uid(run, "fetch", 2));
        assert_ne!(first, spawn_uid(run, "summarise", 1));

        let thread = child_thread_id(run, first);
        assert_eq!(thread, child_thread_id(run, first));
        let session = child_session_id(thread);
        assert_eq!(session, child_session_id(thread));
        assert_ne!(session, run_session("digest", "slot"));

        assert_eq!(step_task_id(run, "fetch", 1), step_task_id(run, "fetch", 1));
    }

    #[test]
    fn alert_runs_have_their_own_slot() {
        let failed = "digest@2026-11-23T01:00:00Z";
        assert_eq!(alert_slot(failed), format!("alert:{failed}"));
        assert_eq!(
            alert_run_id("alert:ops", failed),
            format!("alert:ops@alert:{failed}")
        );
    }

    #[test]
    fn a_minimal_plan_is_valid() {
        assert_eq!(validate_plan(&minimal()), Vec::new());
    }

    #[test]
    fn explicit_defaults_load_and_validate() {
        let explicit = plan(json!({
            "id": "digest",
            "limits": {"max_parallel": 1},
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "verify": [{"check": "final_message_present"}],
                "timeout_ms": 300_000,
                "retries": 0
            }]
        }));
        assert_eq!(validate_plan(&explicit), Vec::new());
    }

    #[test]
    fn v3_refuses_an_empty_plan_and_a_missing_prompt() {
        let empty = plan(json!({"id": "digest", "steps": []}));
        assert!(rules(&empty).contains(&"V3"));

        let no_prompt = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&no_prompt).contains(&"V3"));
    }

    #[test]
    fn v3_refuses_duplicate_step_ids() {
        let duplicate = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "fetch", "action": "agent", "prompt": "a", "verify": [{"check": "final_message_present"}]},
                {"id": "fetch", "action": "agent", "prompt": "b", "verify": [{"check": "final_message_present"}]}
            ]
        }));
        assert!(rules(&duplicate).contains(&"V3"));
    }

    #[test]
    fn v4_refuses_unknown_self_duplicate_and_cyclic_dependencies() {
        let unknown = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a", "depends_on": ["nope"],
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&unknown).contains(&"V4"));

        let itself = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a", "depends_on": ["fetch"],
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&itself).contains(&"V4"));

        let duplicated = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "a", "action": "agent", "prompt": "a", "verify": [{"check": "final_message_present"}]},
                {"id": "b", "action": "agent", "prompt": "b", "depends_on": ["a", "a"],
                 "verify": [{"check": "final_message_present"}]}
            ]
        }));
        assert!(rules(&duplicated).contains(&"V4"));

        let cycle = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "a", "action": "agent", "prompt": "a", "depends_on": ["b"],
                 "verify": [{"check": "final_message_present"}]},
                {"id": "b", "action": "agent", "prompt": "b", "depends_on": ["a"],
                 "verify": [{"check": "final_message_present"}]}
            ]
        }));
        assert!(rules(&cycle).contains(&"V4"));
        assert!(
            !rules(&cycle).contains(&"V14"),
            "the cycle is the better report; no reachability noise"
        );
    }

    #[test]
    fn v5_refuses_an_input_without_a_producer() {
        let dangling = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "summarise", "action": "agent", "prompt": "a",
                 "inputs": [{"step": "fetch", "name": "news"}],
                 "verify": [{"check": "final_message_present"}]},
                {"id": "fetch", "action": "agent", "prompt": "b",
                 "verify": [{"check": "final_message_present"}]}
            ]
        }));
        assert!(rules(&dangling).contains(&"V5"));

        let wrong_name = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "fetch", "action": "agent", "prompt": "b",
                 "produces": {"name": "news"},
                 "verify": [{"check": "final_message_present"}]},
                {"id": "summarise", "action": "agent", "prompt": "a",
                 "inputs": [{"step": "fetch", "name": "digest"}],
                 "verify": [{"check": "final_message_present"}]}
            ]
        }));
        assert!(rules(&wrong_name).contains(&"V5"));

        let wired = plan(json!({
            "id": "digest",
            "steps": [
                {"id": "fetch", "action": "agent", "prompt": "b",
                 "produces": {"name": "news", "max_bytes": 65536},
                 "verify": [{"check": "final_message_present"}]},
                {"id": "summarise", "action": "agent", "prompt": "a",
                 "inputs": [{"step": "fetch", "name": "news"}],
                 "verify": [{"check": "artifact_within_cap", "name": "news"}]}
            ]
        }));
        assert!(rules(&wired).is_empty());
    }

    #[test]
    fn v6_refuses_invalid_capability_names() {
        let invalid = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a",
                       "requires": ["Net.Unrestricted"],
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&invalid).contains(&"V6"));
    }

    #[test]
    fn v7_refuses_retries_on_a_non_idempotent_write() {
        let unsafe_write = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "agent", "prompt": "a",
                       "effect": "write", "retries": 1,
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&unsafe_write).contains(&"V7"));

        let idempotent = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "agent", "prompt": "a",
                       "effect": "write", "retries": 1, "idempotent": true,
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&idempotent).is_empty());
    }

    #[test]
    fn v8_refuses_a_step_budget_larger_than_the_run_budget() {
        let too_big = plan(json!({
            "id": "digest",
            "limits": {"deadline_ms": 60_000},
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a", "timeout_ms": 300_000,
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&too_big).contains(&"V8"));
    }

    #[test]
    fn v11_refuses_binary_artifacts_and_an_oversized_cap() {
        let binary = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a",
                       "produces": {"name": "news", "media": "binary"},
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&binary).contains(&"V11"));

        let oversized = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a",
                       "produces": {"name": "news", "max_bytes": 1_000_000},
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&oversized).contains(&"V11"));
    }

    #[test]
    fn v16_refuses_a_malformed_deliver_step() {
        let no_target = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "deliver", "channel": "imessage",
                       "content": {"text": "hello"}}]
        }));
        assert!(rules(&no_target).contains(&"V16"));

        let both_contents = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "deliver", "channel": "imessage",
                       "target": {"handle": "+1"},
                       "content": {"text": "hello", "from_artifact": {"step": "s", "name": "n"}}}]
        }));
        assert!(rules(&both_contents).contains(&"V16"));

        let well_formed = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "deliver", "channel": "imessage",
                       "target": {"handle": "+1"}, "content": {"text": "hello"}}]
        }));
        assert!(rules(&well_formed).is_empty());
    }

    #[test]
    fn v17_refuses_a_non_object_output_schema() {
        let scalar = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a",
                       "output_schema": "string",
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&scalar).contains(&"V17"));
    }

    #[test]
    fn v21_refuses_a_plan_with_no_evidence() {
        let unverifiable = plan(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a"}]
        }));
        assert!(rules(&unverifiable).contains(&"V21"));
    }

    #[test]
    fn v22_refuses_an_empty_or_duplicated_prefer_entry() {
        let empty_entry = plan(json!({
            "id": "digest",
            "limits": {"prefer": [""]},
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a",
                       "prefer": ["a", "a"],
                       "verify": [{"check": "final_message_present"}]}]
        }));
        assert!(rules(&empty_entry).contains(&"V22"));
    }

    #[test]
    fn violations_render_with_path_rule_and_reason() {
        let empty = plan(json!({"id": "digest", "steps": []}));
        let rendered: Vec<String> = validate_plan(&empty)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert!(
            rendered
                .iter()
                .any(|line| line == "steps: a plan declares at least one step (V3)")
        );
    }

    #[test]
    fn unknown_fields_are_refused() {
        let unknown = serde_json::from_value::<JobPlan>(json!({
            "id": "digest",
            "steps": [{"id": "fetch", "action": "agent", "prompt": "a", "nonsense": true}]
        }));
        assert!(unknown.is_err());
    }

    #[test]
    fn deliver_helpers_expose_the_frozen_facts() {
        let deliver = plan(json!({
            "id": "digest",
            "steps": [{"id": "send", "action": "deliver", "channel": "imessage",
                       "target": {"handle": "+1"}, "content": {"text": "hello"}}]
        }));
        let step = deliver.step("send").expect("declared");
        assert!(step.is_deliver());
        let spec = step.deliver().expect("well formed");
        assert_eq!(spec.channel, "imessage");
        assert_eq!(spec.target, &json!({"handle": "+1"}));
        assert_eq!(spec.content.text.as_deref(), Some("hello"));
        assert!(spec.content.from_artifact.is_none());

        let agent = minimal();
        assert!(!agent.step("fetch").expect("declared").is_deliver());
        assert!(agent.step("fetch").expect("declared").deliver().is_none());
    }

    #[test]
    fn check_names_match_the_verdict_vocabulary() {
        let cases = [
            (JobCheck::FinalMessagePresent, "final_message_present"),
            (
                JobCheck::ToolResultSinceLastUserInput,
                "tool_result_since_last_user_input",
            ),
            (
                JobCheck::NoErrorToolResultTail { k: 2 },
                "no_error_tool_result_tail",
            ),
            (JobCheck::MinToolResults { n: 3 }, "min_tool_results"),
            (
                JobCheck::ArtifactWithinCap {
                    name: "news".into(),
                },
                "artifact_within_cap",
            ),
            (
                JobCheck::DeliveryReceipt {
                    channel: "imessage".into(),
                    target: json!({"handle": "+1"}),
                },
                "delivery_receipt",
            ),
        ];
        for (check, name) in cases {
            assert_eq!(check.name(), name);
        }
        assert!(JobCheck::FinalMessagePresent.to_check_spec().is_some());
        assert!(
            JobCheck::ArtifactWithinCap {
                name: "news".into()
            }
            .to_check_spec()
            .is_none()
        );
    }

    /// The same plan's frozen bytes as they ship today: the fixture the
    /// digest is pinned against.
    const GOLDEN_CANONICAL: &str = r#"{"id":"digest","limits":{"accept_unverified":[],"deadline_ms":1800000,"defer_budget_ms":300000,"inline_max_bytes":65536,"max_parallel":1,"prefer":[]},"steps":[{"action":"agent","allow_offline":false,"depends_on":[],"effect":"read","id":"fetch","idempotent":false,"inputs":[],"on_failure":"halt","optional":false,"prefer":[],"prompt":"fetch the news","requires":[],"retries":0,"timeout_ms":300000,"verify":[{"check":"final_message_present"}]}]}"#;

    fn patch_check(allow: serde_json::Value, deny: serde_json::Value) -> serde_json::Value {
        json!({"check": "patch_paths_allowed", "artifact": "patch", "allow": allow, "deny": deny})
    }

    #[test]
    fn v23_accepts_a_frozen_surface() {
        let plan = plan(json!({
            "id": "self-dev",
            "steps": [{
                "id": "patch",
                "action": "agent",
                "prompt": "write the patch",
                "produces": {"name": "patch"},
                "verify": [patch_check(
                    json!(["crates/**", "docs/**"]),
                    json!(["design/decisions/**", "**/*.pem"])
                )]
            }]
        }));
        assert!(
            validate_plan(&plan).is_empty(),
            "{:?}",
            validate_plan(&plan)
        );
        assert_eq!(plan.steps[0].verify[0].name(), "patch_paths_allowed");
        assert!(plan.steps[0].verify[0].to_check_spec().is_none());
        assert!(plan.frozen_json()["steps"][0]["verify"][0]["allow"].is_array());
    }

    #[test]
    fn v23_refuses_a_surface_that_names_an_unproduced_artifact() {
        let plan = plan(json!({
            "id": "self-dev",
            "steps": [{
                "id": "patch",
                "action": "agent",
                "prompt": "write the patch",
                "produces": {"name": "diff"},
                "verify": [patch_check(json!(["crates/**"]), json!(["**/*.pem"]))]
            }]
        }));
        assert_eq!(rules(&plan), vec!["V23"]);
        assert!(
            validate_plan(&plan)[0]
                .to_string()
                .contains("does not produce")
        );
    }

    #[test]
    fn v23_requires_both_glob_lists_and_repository_relative_entries() {
        for (allow, deny) in [
            (json!([]), json!(["**/*.pem"])),
            (json!(["crates/**"]), json!([])),
            (json!(["/etc/**"]), json!(["**/*.pem"])),
            (json!(["crates/../etc/**"]), json!(["**/*.pem"])),
            (json!(["crates/**"]), json!(["../**"])),
        ] {
            let plan = plan(json!({
                "id": "self-dev",
                "steps": [{
                    "id": "patch",
                    "action": "agent",
                    "prompt": "write the patch",
                    "produces": {"name": "patch"},
                    "verify": [patch_check(allow.clone(), deny.clone())]
                }]
            }));
            assert_eq!(rules(&plan), vec!["V23"], "{allow} {deny}");
        }
    }

    #[test]
    fn v23_allows_one_surface_per_step() {
        let plan = plan(json!({
            "id": "self-dev",
            "steps": [{
                "id": "patch",
                "action": "agent",
                "prompt": "write the patch",
                "produces": {"name": "patch"},
                "verify": [
                    patch_check(json!(["crates/**"]), json!(["**/*.pem"])),
                    patch_check(json!(["docs/**"]), json!(["**/*.pem"]))
                ]
            }]
        }));
        assert_eq!(rules(&plan), vec!["V23"]);
        assert!(validate_plan(&plan)[0].message.contains("at most one"));
    }

    #[test]
    fn v25_bounds_the_breaker_streak() {
        let with_breaker = |value: u32| {
            plan(json!({
                "id": "self-dev",
                "limits": {"consecutive_failures": value},
                "steps": [{
                    "id": "patch",
                    "action": "agent",
                    "prompt": "p",
                    "verify": [{"check": "final_message_present"}]
                }]
            }))
        };
        assert!(validate_plan(&with_breaker(0)).is_empty());
        assert!(validate_plan(&with_breaker(10)).is_empty());
        assert_eq!(rules(&with_breaker(11)), vec!["V25"]);

        assert_eq!(
            minimal().limits.consecutive_failures,
            DEFAULT_CONSECUTIVE_FAILURES
        );
        assert!(
            minimal().frozen_json()["limits"]
                .get("consecutive_failures")
                .is_none()
        );
        assert_eq!(
            with_breaker(4).frozen_json()["limits"]["consecutive_failures"],
            json!(4)
        );
    }

    /// JOB-MODEL §18.4: the golden fixture. Frozen bytes are a compatibility
    /// surface: a stored `plan_digest` must keep meaning the same plan, so an
    /// accidental encoder change (a reordered key, a dropped default) fails
    /// here instead of silently re-labelling every run. A deliberate schema
    /// change updates this fixture and says so in the change.
    #[test]
    fn the_frozen_bytes_of_a_known_plan_match_their_golden_fixture() {
        let plan = minimal();
        assert_eq!(
            plan.digest(),
            "sha256:b130bbb784b4c7ea2ec46146d6d1d1b0c34eba03303e9192765758b3bb504b78"
        );
        assert_eq!(canonical_json(&plan.frozen_json()), GOLDEN_CANONICAL);
    }

    /// A well-formed self-dev plan: one patch producer, one publish step.
    fn proposal() -> JobPlan {
        plan(json!({
            "id": "self-dev",
            "limits": {"deadline_ms": 3_600_000},
            "steps": [{
                "id": "patch",
                "action": "agent",
                "prompt": "propose a patch",
                "produces": {"name": "patch", "max_bytes": 262_144},
                "verify": [
                    {"check": "artifact_within_cap", "name": "patch"},
                    {"check": "patch_paths_allowed", "artifact": "patch",
                     "allow": ["crates/**"], "deny": ["AGENTS.md"]}
                ]
            }, {
                "id": "publish",
                "action": "publish",
                "depends_on": ["patch"],
                "inputs": [{"step": "patch", "name": "patch"}],
                "publish": {"base": "main", "gates": true},
                "effect": "write",
                "verify": [{"check": "publish_ok"}]
            }]
        }))
    }

    fn rules_of(mutate: impl FnOnce(&mut serde_json::Value)) -> Vec<&'static str> {
        let mut value = proposal().frozen_json();
        mutate(&mut value);
        rules(&plan(value))
    }

    fn publish_step(value: &mut serde_json::Value) -> &mut serde_json::Value {
        &mut value["steps"][1]
    }

    #[test]
    fn a_well_formed_publish_plan_validates() {
        assert_eq!(validate_plan(&proposal()), Vec::new());
    }

    #[test]
    fn publish_defaults_are_frozen_into_the_step() {
        let held = proposal();
        let step = held.step("publish").expect("declared");
        assert!(step.is_publish());
        assert!(!step.is_deliver());
        let publish = step.publish().expect("the publish block");
        assert_eq!(publish.base, DEFAULT_PUBLISH_BASE);
        assert!(publish.gates);
        assert!(!publish.push);
        assert!(!publish.pull_request);

        let frozen = held.frozen_json();
        assert_eq!(frozen["steps"][1]["publish"]["base"], json!("main"));
        assert_eq!(frozen["steps"][1]["publish"]["gates"], json!(true));
        assert_eq!(
            frozen["steps"][1]["action"],
            json!("publish"),
            "the action is part of the frozen bytes"
        );
    }

    #[test]
    fn a_plan_without_a_publish_field_freezes_the_same_bytes() {
        let plain = minimal();
        assert!(plain.frozen_json()["steps"][0].get("publish").is_none());
        let explicit_null = plan(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "action": "agent",
                "prompt": "fetch the news",
                "publish": null,
                "verify": [{"check": "final_message_present"}]
            }]
        }));
        assert_eq!(plain.digest(), explicit_null.digest());
    }

    #[test]
    fn publish_ok_names_itself_and_is_owned_by_the_conductor() {
        assert_eq!(JobCheck::PublishOk.name(), "publish_ok");
        assert!(JobCheck::PublishOk.to_check_spec().is_none());
    }

    #[test]
    fn v27_rejects_a_publish_step_that_breaks_the_fixed_shape() {
        assert!(
            rules_of(|value| {
                publish_step(value)
                    .as_object_mut()
                    .unwrap()
                    .remove("publish");
            })
            .contains(&"V27")
        );
        assert!(
            rules_of(|value| {
                publish_step(value)["inputs"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"step": "patch", "name": "patch"}));
            })
            .contains(&"V27")
        );
        assert!(rules_of(|value| publish_step(value)["retries"] = json!(1)).contains(&"V27"));
        assert!(rules_of(|value| publish_step(value)["effect"] = json!("read")).contains(&"V27"));
        assert!(rules_of(|value| publish_step(value)["idempotent"] = json!(true)).contains(&"V27"));
        assert!(rules_of(|value| publish_step(value)["prompt"] = json!("do it")).contains(&"V27"));
        assert!(
            rules_of(|value| publish_step(value)["produces"] = json!({"name": "again"}))
                .contains(&"V27")
        );
        assert!(rules_of(|value| publish_step(value)["depends_on"] = json!([])).contains(&"V27"));
        assert!(
            rules_of(|value| publish_step(value)["channel"] = json!("mock-im")).contains(&"V27")
        );
        assert!(
            rules_of(|value| publish_step(value)["requires"] = json!(["repo"])).contains(&"V27")
        );
    }

    #[test]
    fn v27_rejects_the_publish_field_on_another_action() {
        let rules = rules_of(|value| value["steps"][0]["publish"] = json!({"gates": true}));
        assert!(rules.contains(&"V27"), "{rules:?}");
    }

    #[test]
    fn v28_allows_at_most_one_publish_step() {
        let rules = rules_of(|value| {
            let copy = value["steps"][1].clone();
            value["steps"].as_array_mut().unwrap()[1]["id"] = json!("publish-again");
            value["steps"].as_array_mut().unwrap().push(copy);
        });
        assert!(rules.contains(&"V28"), "{rules:?}");
    }

    #[test]
    fn v29_requires_a_declared_surface() {
        let rules = rules_of(|value| {
            value["steps"][0]["verify"] =
                json!([{"check": "artifact_within_cap", "name": "patch"}]);
        });
        assert!(rules.contains(&"V29"), "{rules:?}");
    }

    #[test]
    fn v31_rejects_a_pull_request_without_a_push_and_a_blank_base() {
        let rules = rules_of(|value| {
            publish_step(value)["publish"]["pull_request"] = json!(true);
        });
        assert!(rules.contains(&"V31"), "{rules:?}");
        let rules = rules_of(|value| {
            publish_step(value)["publish"]["base"] = json!("release 1");
        });
        assert!(rules.contains(&"V31"), "{rules:?}");
        let rules = rules_of(|value| {
            publish_step(value)["publish"]["base"] = json!("");
        });
        assert!(rules.contains(&"V31"), "{rules:?}");
        assert!(
            !rules_of(|value| {
                publish_step(value)["publish"]["push"] = json!(true);
                publish_step(value)["publish"]["pull_request"] = json!(true);
            })
            .contains(&"V31")
        );
    }

    #[test]
    fn a_publish_step_freezes_into_the_plan_digest() {
        let without = proposal();
        let mut value = without.frozen_json();
        publish_step(&mut value)["publish"]["gates"] = json!(false);
        assert_ne!(plan(value).digest(), without.digest());
    }
}
