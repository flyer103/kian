//! Evidence gate v1 (card C24, SPEC P4/A6): a declarative, bounded,
//! **no-model-call** gate over the worker's own event stream.
//!
//! The gate is a pure predicate evaluation: it never calls the provider,
//! never mutates context (verdicts are non-context-bearing by C18), and
//! only *reports* — recovery disposition stays owned by the engine
//! (a failed gate surfaces as a soft failure; the engine settles it).
//!
//! Semantics (wired into the built-in loop's stop path by
//! [`crate::react::ReActLoop::with_evidence_gate`]):
//!
//! - On the loop's terminal stop — `Settled` (final answer, empty inbox:
//!   the loop's "done" claim) or `Stopped` (intentional stop, e.g. the
//!   iteration budget) — the gate evaluates every check as a pure
//!   predicate over the session's event stream.
//! - **Pass**: append `evidence/verdict{passed:true}` and let the stop
//!   proceed — the verdict is the last event before the end write.
//! - **Fail**: append `evidence/verdict{passed:false, attempt}` and send
//!   the loop back for another provider turn while `attempt <
//!   max_retries`; past the budget the loop reports a soft failure, which
//!   the engine settles as `failed` — never `completed` with a failed
//!   gate (constraint 7).
//! - `attempt` is derived from the stream (the number of prior failed
//!   verdicts of the same gate), so the retry budget survives episodes,
//!   crashes and takeover (constraint 1 in spirit).
//! - Retry turns reuse the identical context prefix: verdict events are
//!   non-context-bearing, so a gate retry never invalidates the provider
//!   prefix cache (constraint 3).
//!
//! Default-off: [`EvidenceGateConfig::default`] is disabled; Phase-1
//! behavior is unchanged until a caller explicitly mounts a gate.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kian_core::events::{Event, EventKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Gate name the built-in loop stamps on its verdicts. Custom wrappers
/// (C25's worker driver) pass their own name so multiple gates can coexist
/// on one stream.
pub const BUILTIN_GATE_NAME: &str = "builtin";

// ---------------------------------------------------------------------------
// Check vocabulary (closed v1 — additions require a plan amendment)
// ---------------------------------------------------------------------------

/// One declarative evidence check. Each variant is a **pure predicate over
/// `&[Event]`** — no provider call, no context mutation, no clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CheckSpec {
    /// The last assistant message after the last user input is a final
    /// answer: non-empty text, no tool calls.
    FinalMessagePresent,
    /// At least one `tool/result` exists after the last user input (the
    /// loop actually acted on the task, not just talked).
    ToolResultSinceLastUserInput,
    /// The trailing window of (up to) `k` tool results contains no error.
    /// Vacuously true when the stream has fewer (or zero) tool results —
    /// pair with [`CheckSpec::MinToolResults`] when presence is required.
    NoErrorToolResultTail { k: u64 },
    /// The stream holds at least `n` tool results in total.
    MinToolResults { n: u64 },
}

impl CheckSpec {
    /// The check's name as it appears in the verdict payload.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::FinalMessagePresent => "final_message_present",
            Self::ToolResultSinceLastUserInput => "tool_result_since_last_user_input",
            Self::NoErrorToolResultTail { .. } => "no_error_tool_result_tail",
            Self::MinToolResults { .. } => "min_tool_results",
        }
    }

    fn evaluate(&self, events: &[Event]) -> CheckOutcome {
        let (passed, detail) = match *self {
            Self::FinalMessagePresent => {
                let after_input = slice_after_last_input(events);
                let found = after_input.iter().any(|ev| {
                    matches!(ev.kind, EventKind::AssistantMessage) && is_final_message(ev)
                });
                (
                    found,
                    Some("no assistant final message after the last user input".to_string()),
                )
            }
            Self::ToolResultSinceLastUserInput => {
                let after_input = slice_after_last_input(events);
                let found = after_input
                    .iter()
                    .any(|ev| matches!(ev.kind, EventKind::ToolResult));
                (
                    found,
                    Some("no tool/result after the last user input".to_string()),
                )
            }
            Self::NoErrorToolResultTail { k } => {
                let results = tool_results(events);
                let window = (k as usize).min(results.len());
                let errors = results[results.len() - window..]
                    .iter()
                    .filter(|ev| ev.bytes.pointer("/content/is_error") == Some(&json!(true)))
                    .count();
                let passed = errors == 0;
                let detail = if passed {
                    None
                } else {
                    Some(format!(
                        "the trailing {window} tool result(s) include {errors} error(s)"
                    ))
                };
                (passed, detail)
            }
            Self::MinToolResults { n } => {
                let count = tool_results(events).len();
                let passed = count >= n as usize;
                let detail = if passed {
                    None
                } else {
                    Some(format!("expected >= {n} tool results, found {count}"))
                };
                (passed, detail)
            }
        };
        CheckOutcome {
            name: self.name().to_string(),
            passed,
            detail: if passed { None } else { detail },
        }
    }
}

/// One evaluated check inside a verdict. `detail` is present exactly when
/// the check failed (observable why, no noise on passes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckOutcome {
    pub name: String,
    pub passed: bool,
    pub detail: Option<String>,
}

/// The gate's verdict over one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateVerdict {
    pub passed: bool,
    pub checks: Vec<CheckOutcome>,
    /// 0-based retry counter, derived from prior *failed* verdicts of the
    /// same gate in the stream.
    pub attempt: u64,
}

/// Evaluates `checks` against `events`. Pure: same inputs, same verdict.
///
/// Public and reusable — C25's worker-loop wrapper consumes this directly
/// instead of reimplementing gate semantics.
#[must_use]
pub fn evaluate_evidence_gate(gate: &str, checks: &[CheckSpec], events: &[Event]) -> GateVerdict {
    let attempt = events
        .iter()
        .filter(|ev| {
            matches!(ev.kind, EventKind::EvidenceVerdict)
                && ev.bytes.get("gate") == Some(&json!(gate))
                && ev.bytes.get("passed") == Some(&json!(false))
        })
        .count() as u64;
    let checks: Vec<CheckOutcome> = checks.iter().map(|c| c.evaluate(events)).collect();
    let passed = checks.iter().all(|c| c.passed);
    GateVerdict {
        passed,
        checks,
        attempt,
    }
}

/// Renders a verdict into the frozen C18 `evidence/verdict` payload:
/// `{"gate": str, "passed": bool, "checks": [{"name": str, "passed": bool,
/// "detail"?: str}], "attempt": u64}`. Field order in serialized form is
/// lexicographic (frozen serialization discipline, constraint 3).
#[must_use]
pub fn verdict_payload(gate: &str, verdict: &GateVerdict) -> Value {
    let checks: Vec<Value> = verdict
        .checks
        .iter()
        .map(|c| {
            let mut v = json!({"name": c.name, "passed": c.passed});
            if let Some(detail) = &c.detail {
                v["detail"] = Value::String(detail.clone());
            }
            v
        })
        .collect();
    json!({
        "gate": gate,
        "passed": verdict.passed,
        "checks": checks,
        "attempt": verdict.attempt,
    })
}

// ---------------------------------------------------------------------------
// Predicate support (pure stream slices)
// ---------------------------------------------------------------------------

/// The events after the last `input/admitted`; empty when no user input
/// exists (nothing has been asked — presence checks fail).
fn slice_after_last_input(events: &[Event]) -> &[Event] {
    match events
        .iter()
        .rposition(|ev| matches!(ev.kind, EventKind::InputAdmitted))
    {
        Some(i) => &events[i + 1..],
        None => &[],
    }
}

fn tool_results(events: &[Event]) -> Vec<&Event> {
    events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::ToolResult))
        .collect()
}

/// A context-bearing assistant/message is a *final* message when its text
/// is non-empty and it carries no tool calls (C4 payload convention).
fn is_final_message(ev: &Event) -> bool {
    let Some(content) = ev.bytes.get("content") else {
        return false;
    };
    let text_present = content
        .get("text")
        .and_then(Value::as_str)
        .is_some_and(|t| !t.is_empty());
    let no_tool_calls = content
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_none_or(|calls| calls.is_empty());
    text_present && no_tool_calls
}

// ---------------------------------------------------------------------------
// Configuration + metrics counters
// ---------------------------------------------------------------------------

/// Verdict counters for the metrics surface (SPEC §3.1: evidence-gate
/// verdicts). Mounted into `GET /metrics` by the C27 assembly; cheap to
/// read at any time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictCounters {
    pub passed: u64,
    pub failed: u64,
}

/// Shared, lock-free verdict counters. Cloning the containing
/// [`EvidenceGateConfig`] shares the same counters (an `Arc`).
#[derive(Debug, Default)]
pub struct EvidenceGateCounters {
    passed: AtomicU64,
    failed: AtomicU64,
}

impl EvidenceGateCounters {
    pub fn record_passed(&self) {
        self.passed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> VerdictCounters {
        VerdictCounters {
            passed: self.passed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// Evidence-gate tuning. Default is **disabled** — Phase-1 behavior is
/// unchanged until a caller mounts a gate explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceGateConfig {
    /// Default `false`: no gate runs, no verdict events are appended.
    #[serde(default)]
    pub enabled: bool,
    /// The closed v1 check vocabulary; an empty list is a vacuous pass.
    #[serde(default)]
    pub checks: Vec<CheckSpec>,
    /// How many extra loop iterations a failed gate may buy (bounded
    /// recovery, constraint 8). `0` = one failure settles immediately.
    #[serde(default)]
    pub max_retries: u64,
    /// Shared verdict counters for the metrics surface; not configuration.
    #[serde(skip)]
    pub counters: Arc<EvidenceGateCounters>,
}

impl Default for EvidenceGateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            checks: Vec::new(),
            max_retries: 0,
            counters: Arc::new(EvidenceGateCounters::default()),
        }
    }
}

/// Config identity ignores the shared counters handle (runtime state, not
/// configuration).
impl PartialEq for EvidenceGateConfig {
    fn eq(&self, other: &Self) -> bool {
        self.enabled == other.enabled
            && self.checks == other.checks
            && self.max_retries == other.max_retries
    }
}
