//! Three-layer recovery (DESIGN §2.4) — A4/A5.
//!
//! Layer 1 (in-loop): a pure `classify` function mapping typed errors to
//! [`ErrorClass`], a declarative policy table (`class → action`) parsed
//! from `kian.yaml`, a one-shot guard per recovery kind, and a recovery
//! budget (tokens + wall-clock) with refund semantics. Layer 2
//! (session-layer): exhaustion *escalates* — the caller releases the
//! claim without an end write so the session lands in `recoverable` via
//! lease-expiry detection (liveness only, ADR-005); never an unbounded
//! loop. Layer 3 (durable-state) is store-side repair, out of this
//! module's scope.
//!
//! Disposition is owned by the caller (A4): side-effecting actions are
//! executed through the caller-supplied [`RecoveryReactor`] hooks; this
//! module only classifies, consults the table, guards, budgets, and
//! bounds the loop.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use kian_core::seams::{
    PolicyRule, ProviderError, RecoveryAction, RecoveryPolicySource, ToolError,
};
use kian_store::{InboxError, StoreError};

use crate::engine::Clock;

// ---------------------------------------------------------------------------
// Layer 1a — pure error classification
// ---------------------------------------------------------------------------

/// The classification vocabulary. `name()` strings are the policy-table
/// class keys (`kian.yaml` `recovery.rules[].class`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    ProviderDown,
    RateLimited,
    ContextOverflow,
    /// The request itself is too large (ADR-067): refused locally by
    /// `providers.<name>.max_request_bytes` or by the provider's 413.
    /// Never retried implicitly — the same bytes cannot succeed.
    RequestTooLarge,
    ToolFailed,
    StoreFailure,
    BudgetExhausted,
    ValidationFailed,
    /// A soft failure (constraint 7): e.g. an empty model response —
    /// routable, never mappable onto `completed`.
    EmptyResponse,
    Unknown,
}

impl ErrorClass {
    /// Every class — the one-shot guard's worst-case bound is one fire
    /// per class, so any recovery loop is bounded by `ALL.len()`.
    pub const ALL: [ErrorClass; 10] = [
        ErrorClass::ProviderDown,
        ErrorClass::RateLimited,
        ErrorClass::ContextOverflow,
        ErrorClass::RequestTooLarge,
        ErrorClass::ToolFailed,
        ErrorClass::StoreFailure,
        ErrorClass::BudgetExhausted,
        ErrorClass::ValidationFailed,
        ErrorClass::EmptyResponse,
        ErrorClass::Unknown,
    ];

    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            ErrorClass::ProviderDown => "provider_down",
            ErrorClass::RateLimited => "rate_limited",
            ErrorClass::ContextOverflow => "context_overflow",
            ErrorClass::RequestTooLarge => "request_too_large",
            ErrorClass::ToolFailed => "tool_failed",
            ErrorClass::StoreFailure => "store_failure",
            ErrorClass::BudgetExhausted => "budget_exhausted",
            ErrorClass::ValidationFailed => "validation_failed",
            ErrorClass::EmptyResponse => "empty_response",
            ErrorClass::Unknown => "unknown",
        }
    }

    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        ErrorClass::ALL.into_iter().find(|c| c.name() == name)
    }
}

/// Every failure the recovery subsystem routes. Wraps the typed seam /
/// store errors so classification never parses error strings.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("provider: {0}")]
    Provider(#[from] ProviderError),
    #[error("tool: {0}")]
    Tool(#[from] ToolError),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("inbox: {0}")]
    Inbox(#[from] InboxError),
    /// A drain-context append failed inside a recovery action.
    #[error("engine: {0}")]
    Engine(#[from] crate::engine::EngineError),
    #[error("recovery budget exhausted")]
    BudgetExhausted,
    #[error("validation failed: {0}")]
    ValidationFailed(String),
    #[error("empty response")]
    EmptyResponse,
    #[error("unclassified: {0}")]
    Unknown(String),
}

/// Pure classification context. Currently only the (zero-based) retry
/// attempt; the parameter exists so attempt-dependent refinements stay
/// pure when they arrive. No clock, no store, no I/O ever enters here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassifyCtx {
    pub attempt: u64,
}

/// Classify one error (A4, layer 1). **Pure and total**: a synchronous
/// function of its arguments only — any I/O inside `classify` is a
/// defect ("never touch").
#[must_use]
pub fn classify(error: &RecoveryError, _ctx: &ClassifyCtx) -> ErrorClass {
    match error {
        RecoveryError::Provider(e) => match e {
            ProviderError::RateLimited => ErrorClass::RateLimited,
            ProviderError::ContextOverflow => ErrorClass::ContextOverflow,
            ProviderError::RequestTooLarge { .. } => ErrorClass::RequestTooLarge,
            ProviderError::Unavailable(_) | ProviderError::Transport(_) => ErrorClass::ProviderDown,
            ProviderError::EmptyResponse => ErrorClass::EmptyResponse,
            ProviderError::InvalidResponse(_) => ErrorClass::ValidationFailed,
            ProviderError::Other(_) => ErrorClass::Unknown,
        },
        RecoveryError::Tool(_) => ErrorClass::ToolFailed,
        RecoveryError::Store(_) | RecoveryError::Inbox(_) | RecoveryError::Engine(_) => {
            ErrorClass::StoreFailure
        }
        RecoveryError::BudgetExhausted => ErrorClass::BudgetExhausted,
        RecoveryError::ValidationFailed(_) => ErrorClass::ValidationFailed,
        RecoveryError::EmptyResponse => ErrorClass::EmptyResponse,
        RecoveryError::Unknown(_) => ErrorClass::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Layer 1b — the declarative policy table
// ---------------------------------------------------------------------------

/// Fail-closed policy errors: a malformed `kian.yaml` never starts.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("unknown error class in policy table: {0:?}")]
    UnknownClass(String),
    #[error("policy rule has an empty class")]
    EmptyClass,
}

/// Recovery budget sizing (the `recovery.budget` section of kian.yaml).
/// `wall_clock_ms == 0` means "no wall-clock limit".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BudgetConfig {
    pub tokens: u64,
    pub wall_clock_ms: u64,
    /// Token cost charged per executed retry-class action.
    pub retry_cost_tokens: u64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            tokens: 16,
            wall_clock_ms: 300_000,
            retry_cost_tokens: 1,
        }
    }
}

/// The `recovery:` section of `kian.yaml`, parsed.
#[derive(Debug, Clone, PartialEq)]
pub struct PolicyConfig {
    pub budget: BudgetConfig,
    pub rules: Vec<PolicyRule>,
}

fn rule(class: &str, action: RecoveryAction) -> PolicyRule {
    PolicyRule {
        class: class.into(),
        action,
    }
}

impl PolicyConfig {
    /// Parses the recovery section. Accepts either the bare section
    /// (`budget:`, `rules:`) or a full document carrying it under a
    /// top-level `recovery:` key. Fail-closed: unknown class names and
    /// malformed YAML are errors, never silently dropped rules.
    pub fn from_yaml(text: &str) -> Result<Self, PolicyError> {
        let value: serde_yaml::Value = serde_yaml::from_str(text)?;
        // Accept either the bare section or a full kian.yaml document
        // carrying it under a top-level `recovery:` key. The node is
        // re-serialized because serde_yaml 0.9 cannot deserialize enum
        // fields (the action shapes) directly from a `Value` — external
        // tags only survive a real string parse.
        let node = value.get("recovery").unwrap_or(&value);
        let doc: PolicyDoc = serde_yaml::from_str(&serde_yaml::to_string(node)?)?;
        let rules = doc
            .rules
            .iter()
            .map(RuleDef::to_rule)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            budget: doc.budget,
            rules,
        })
    }

    /// The config-default table (the only policies engine code may
    /// hardcode; callers override via kian.yaml).
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            budget: BudgetConfig::default(),
            rules: vec![
                rule(
                    "rate_limited",
                    RecoveryAction::BackoffRetry { delay_ms: 500 },
                ),
                rule(
                    "empty_response",
                    RecoveryAction::BackoffRetry { delay_ms: 250 },
                ),
                rule("context_overflow", RecoveryAction::CompressThenRetry),
                rule("provider_down", RecoveryAction::Failover),
                rule("tool_failed", RecoveryAction::InterruptAsk),
                rule("store_failure", RecoveryAction::Fail),
                rule("budget_exhausted", RecoveryAction::Fail),
                rule("validation_failed", RecoveryAction::Fail),
                rule("request_too_large", RecoveryAction::Fail),
                rule("unknown", RecoveryAction::Fail),
            ],
        }
    }

    #[must_use]
    pub fn table(&self) -> PolicyTable {
        PolicyTable {
            rules: self.rules.clone(),
        }
    }
}

/// The compiled declarative table: first matching rule (declaration
/// order) wins — deterministic application by construction.
#[derive(Debug, Clone, Default)]
pub struct PolicyTable {
    rules: Vec<PolicyRule>,
}

impl PolicyTable {
    #[must_use]
    pub fn new(rules: Vec<PolicyRule>) -> Self {
        Self { rules }
    }

    /// First-match resolution; `None` = no rule covers the class (the
    /// runner fails honestly — it never invents a default action).
    #[must_use]
    pub fn resolve(&self, class: ErrorClass) -> Option<&RecoveryAction> {
        self.rules
            .iter()
            .find(|r| r.class == class.name())
            .map(|r| &r.action)
    }
}

/// The built-in `RecoveryPolicySource` provider (DESIGN §6.2): pure
/// data — disposition stays with the caller.
impl RecoveryPolicySource for PolicyTable {
    fn table(&self) -> Vec<PolicyRule> {
        self.rules.clone()
    }
}

/// YAML-facing rule shape (snake_case actions for kian.yaml ergonomics);
/// converts into the frozen `kian-core` seam types.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RuleDef {
    class: String,
    action: ActionDef,
}

/// Accepted YAML shapes: a bare name (`failover`) or a single-key map
/// (`backoff_retry: { delay_ms: 250 }`). Hand-rolled because serde_yaml
/// 0.9 cannot drive externally-tagged enum variants through its Value
/// type.
#[derive(Debug, Clone, PartialEq)]
enum ActionDef {
    BackoffRetry { delay_ms: u64 },
    CompressThenRetry,
    Failover,
    InterruptAsk,
    Fail,
}

impl<'de> Deserialize<'de> for ActionDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        let (name, fields) = match &value {
            serde_yaml::Value::String(s) => (s.as_str(), None),
            serde_yaml::Value::Mapping(m) if m.len() == 1 => {
                let (k, v) = m.iter().next().expect("len checked");
                let key = k
                    .as_str()
                    .ok_or_else(|| serde::de::Error::custom("action key must be a string"))?;
                (key, Some(v))
            }
            _ => {
                return Err(serde::de::Error::custom(
                    "action must be a name or a single-key map",
                ));
            }
        };
        match name {
            "backoff_retry" => {
                let delay_ms = fields
                    .and_then(|v| v.get("delay_ms"))
                    .and_then(serde_yaml::Value::as_u64)
                    .ok_or_else(|| {
                        serde::de::Error::custom("backoff_retry requires a numeric delay_ms")
                    })?;
                Ok(ActionDef::BackoffRetry { delay_ms })
            }
            "compress_then_retry" => Ok(ActionDef::CompressThenRetry),
            "failover" => Ok(ActionDef::Failover),
            "interrupt_ask" => Ok(ActionDef::InterruptAsk),
            "fail" => Ok(ActionDef::Fail),
            other => Err(serde::de::Error::custom(format!("unknown action: {other}"))),
        }
    }
}

impl RuleDef {
    fn to_rule(&self) -> Result<PolicyRule, PolicyError> {
        if self.class.is_empty() {
            return Err(PolicyError::EmptyClass);
        }
        let class = ErrorClass::from_name(&self.class)
            .ok_or_else(|| PolicyError::UnknownClass(self.class.clone()))?;
        let action = match self.action {
            ActionDef::BackoffRetry { delay_ms } => RecoveryAction::BackoffRetry { delay_ms },
            ActionDef::CompressThenRetry => RecoveryAction::CompressThenRetry,
            ActionDef::Failover => RecoveryAction::Failover,
            ActionDef::InterruptAsk => RecoveryAction::InterruptAsk,
            ActionDef::Fail => RecoveryAction::Fail,
        };
        Ok(PolicyRule {
            class: class.name().into(),
            action,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PolicyDoc {
    #[serde(default)]
    budget: BudgetConfig,
    #[serde(default)]
    rules: Vec<RuleDef>,
}

// ---------------------------------------------------------------------------
// Layer 1c — the one-shot guard
// ---------------------------------------------------------------------------

/// One fire per recovery kind within one iteration (constraint 8): the
/// recovery loop itself cannot run away — a repeated class escalates.
#[derive(Debug, Default)]
pub struct OneShotGuard {
    fired: BTreeSet<ErrorClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    /// First fire of this kind — proceed.
    Fired,
    /// This kind already fired within the iteration — escalate.
    Rejected,
}

impl OneShotGuard {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn try_fire(&mut self, class: ErrorClass) -> GuardDecision {
        if self.fired.insert(class) {
            GuardDecision::Fired
        } else {
            GuardDecision::Rejected
        }
    }

    /// Iteration boundary: re-arm every kind.
    pub fn reset(&mut self) {
        self.fired.clear();
    }

    #[must_use]
    pub fn fired_kinds(&self) -> Vec<ErrorClass> {
        self.fired.iter().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Layer 1d / 2 — the recovery budget (A5)
// ---------------------------------------------------------------------------

/// Why a [`RecoveryBudget::try_consume`] was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetDenied {
    /// True when the wall-clock deadline (not the token pool) denied it.
    pub wall_clock: bool,
}

/// Token + wall-clock budget with refund semantics (A5): retries draw
/// tokens down; unused tokens are refunded on demand and the unused
/// remainder is *returned* on task completion (`settle`). Counters are
/// observable for the `recovery budget reset` command.
pub struct RecoveryBudget {
    limit_tokens: u64,
    deadline_ms: u64,
    consumed: AtomicU64,
    refunded: AtomicU64,
    returned: AtomicU64,
    settled: AtomicBool,
    clock: Arc<dyn Clock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetSnapshot {
    pub limit_tokens: u64,
    pub consumed_tokens: u64,
    pub refunded_tokens: u64,
    pub returned_tokens: u64,
    pub remaining_tokens: u64,
    pub wall_clock_deadline_ms: u64,
    pub exhausted: bool,
}

impl RecoveryBudget {
    #[must_use]
    pub fn new(limit_tokens: u64, wall_clock_ms: u64, clock: Arc<dyn Clock>) -> Self {
        Self {
            limit_tokens,
            deadline_ms: if wall_clock_ms == 0 {
                u64::MAX
            } else {
                clock.now_ms().saturating_add(wall_clock_ms)
            },
            consumed: AtomicU64::new(0),
            refunded: AtomicU64::new(0),
            returned: AtomicU64::new(0),
            settled: AtomicBool::new(false),
            clock,
        }
    }

    /// Draws `cost` tokens. Denied when the pool cannot cover the cost
    /// or the wall-clock deadline passed — both escalate to the session
    /// layer, neither loops.
    pub fn try_consume(&self, cost: u64) -> Result<(), BudgetDenied> {
        if self.settled.load(Ordering::Acquire) || self.is_wall_clock_exhausted() {
            return Err(BudgetDenied {
                wall_clock: !self.settled.load(Ordering::Acquire),
            });
        }
        // CAS loop: concurrent drains on one budget never over-draw.
        let mut current = self.consumed.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(cost) else {
                return Err(BudgetDenied { wall_clock: false });
            };
            if next > self.limit_tokens {
                return Err(BudgetDenied { wall_clock: false });
            }
            match self.consumed.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Refunds previously consumed tokens back into the pool.
    pub fn refund(&self, amount: u64) {
        if self.settled.load(Ordering::Acquire) {
            return; // a settled budget is closed; nothing left to refund into
        }
        let mut current = self.consumed.load(Ordering::Acquire);
        loop {
            // Never refund more than was consumed — refunds cannot mint.
            let dec = amount.min(current);
            if dec == 0 {
                return;
            }
            match self.consumed.compare_exchange_weak(
                current,
                current - dec,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.refunded.fetch_add(dec, Ordering::AcqRel);
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Task completion (A5): returns the unused remainder to the
    /// caller's pool exactly once; the amount is observable via
    /// [`RecoveryBudget::snapshot`].
    pub fn settle(&self) -> u64 {
        if self.settled.swap(true, Ordering::AcqRel) {
            return 0;
        }
        let remaining = self.limit_tokens - self.consumed.load(Ordering::Acquire);
        self.returned.fetch_add(remaining, Ordering::AcqRel);
        remaining
    }

    #[must_use]
    pub fn snapshot(&self) -> BudgetSnapshot {
        let consumed = self.consumed.load(Ordering::Acquire);
        BudgetSnapshot {
            limit_tokens: self.limit_tokens,
            consumed_tokens: consumed,
            refunded_tokens: self.refunded.load(Ordering::Acquire),
            returned_tokens: self.returned.load(Ordering::Acquire),
            remaining_tokens: self.limit_tokens - consumed,
            wall_clock_deadline_ms: self.deadline_ms,
            exhausted: self.is_exhausted(),
        }
    }

    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.consumed.load(Ordering::Acquire) >= self.limit_tokens
            || self.is_wall_clock_exhausted()
            || self.settled.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_wall_clock_exhausted(&self) -> bool {
        self.clock.now_ms() >= self.deadline_ms
    }
}

// ---------------------------------------------------------------------------
// The bounded recovery runner (layer 1 → layer 2 escalation)
// ---------------------------------------------------------------------------

/// What the runner concluded when it did NOT complete the operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscalationCause {
    /// A recovery kind fired twice within one iteration — the one-shot
    /// guard refused; escalate instead of looping.
    OneShotViolated {
        already_fired: ErrorClass,
        incoming: ErrorClass,
    },
    TokenBudgetExhausted,
    WallClockExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTerminal {
    /// The policy said `fail` (or no rule covers the class): an honest
    /// failure — never `completed` (constraint 7).
    Failed { class: ErrorClass },
    /// The policy said `interrupt_ask`; the reactor surfaced it.
    Ask { class: ErrorClass },
    /// Guard or budget exhausted → session-layer recovery: the caller
    /// releases the claim *without* an end write, so the session lands
    /// in `recoverable` via lease-expiry detection (ADR-005 liveness),
    /// and a fresh drain replays events and continues.
    EscalatedToSession { cause: EscalationCause },
}

/// What a successful `compact` did: the appended `compact` event's seq
/// and the pre-compression range `[from_seq, to_seq)` it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactOutcome {
    pub event_seq: u64,
    pub from_seq: u64,
    pub to_seq: u64,
}

/// Caller-owned disposition of side-effecting actions (A4): the runner
/// orchestrates, the reactor executes. `compact` MUST append the
/// observable `compact` event (DESIGN §3.2 payload
/// `{from_seq, to_seq, summary}`) **before** invalidating the prefix —
/// compression is the only deliberate invalidation point (constraint 3).
pub trait RecoveryReactor: Send + Sync {
    fn compact(&self) -> BoxFuture<'static, Result<CompactOutcome, RecoveryError>>;
    fn failover(&self) -> BoxFuture<'static, Result<(), RecoveryError>>;
    fn ask(&self, class: ErrorClass) -> BoxFuture<'static, Result<(), RecoveryError>>;
}

/// Assembled layer-1 environment for one supervised operation.
pub struct RecoveryEnv {
    pub policy: PolicyTable,
    pub budget: RecoveryBudget,
    pub retry_cost_tokens: u64,
}

impl RecoveryEnv {
    #[must_use]
    pub fn new(config: &PolicyConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            policy: config.table(),
            budget: RecoveryBudget::new(config.budget.tokens, config.budget.wall_clock_ms, clock),
            retry_cost_tokens: config.budget.retry_cost_tokens,
        }
    }
}

/// Runs `op` under the recovery subsystem. Each failure is classified,
/// resolved through the policy table, one-shot-guarded and budgeted;
/// retry-class actions (backoff / compress / failover) consume budget
/// and re-invoke `op(attempt + 1)`. Bounded by construction: at most
/// one fire per [`ErrorClass`] (≤ `ALL.len() + 1` op invocations) and
/// at most `tokens / retry_cost` retry actions — an adversarial
/// always-failing provider terminates in
/// `min(guard bound, budget bound)` steps, never an unbounded loop.
pub async fn run_with_recovery<T, F, Fut>(
    env: &RecoveryEnv,
    reactor: &dyn RecoveryReactor,
    mut op: F,
) -> Result<T, RecoveryTerminal>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<T, RecoveryError>>,
{
    let mut guard = OneShotGuard::new();
    let mut attempt: u64 = 0;
    // Defense in depth: the one-shot guard already bounds the loop to
    // one fire per class; this explicit ceiling makes the bound local
    // and obvious.
    let max_attempts = ErrorClass::ALL.len() as u64 + 1;
    loop {
        debug_assert!(
            attempt < max_attempts,
            "the one-shot guard must bound recovery"
        );
        match op(attempt).await {
            Ok(value) => {
                // A5: task completion returns the unused remainder.
                env.budget.settle();
                return Ok(value);
            }
            Err(err) => {
                let class = classify(&err, &ClassifyCtx { attempt });
                let Some(action) = env.policy.resolve(class) else {
                    // No rule covers the class: fail honestly — the
                    // runner never invents a default action.
                    return Err(RecoveryTerminal::Failed { class });
                };
                // Guard before budget: a repeated kind escalates
                // regardless of remaining tokens.
                match guard.try_fire(class) {
                    GuardDecision::Fired => {}
                    GuardDecision::Rejected => {
                        return Err(RecoveryTerminal::EscalatedToSession {
                            cause: EscalationCause::OneShotViolated {
                                already_fired: class,
                                incoming: class,
                            },
                        });
                    }
                }
                // Every retry-class action is budgeted first; denial
                // escalates to the session layer (never loops).
                let budgeted = || {
                    env.budget.try_consume(env.retry_cost_tokens).map_err(|d| {
                        RecoveryTerminal::EscalatedToSession {
                            cause: if d.wall_clock {
                                EscalationCause::WallClockExhausted
                            } else {
                                EscalationCause::TokenBudgetExhausted
                            },
                        }
                    })
                };
                match action {
                    RecoveryAction::BackoffRetry { delay_ms } => {
                        budgeted()?;
                        tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                    }
                    RecoveryAction::CompressThenRetry => {
                        budgeted()?;
                        if let Err(e) = reactor.compact().await {
                            let class = classify(&e, &ClassifyCtx { attempt });
                            return Err(RecoveryTerminal::Failed { class });
                        }
                    }
                    RecoveryAction::Failover => {
                        budgeted()?;
                        if let Err(e) = reactor.failover().await {
                            let class = classify(&e, &ClassifyCtx { attempt });
                            return Err(RecoveryTerminal::Failed { class });
                        }
                    }
                    RecoveryAction::InterruptAsk => {
                        if let Err(e) = reactor.ask(class).await {
                            let class = classify(&e, &ClassifyCtx { attempt });
                            return Err(RecoveryTerminal::Failed { class });
                        }
                        return Err(RecoveryTerminal::Ask { class });
                    }
                    RecoveryAction::Fail => return Err(RecoveryTerminal::Failed { class }),
                }
                attempt += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use futures::FutureExt;
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        BudgetDenied, BudgetSnapshot, ClassifyCtx, CompactOutcome, ErrorClass, GuardDecision,
        OneShotGuard, PolicyConfig, PolicyError, PolicyTable, RecoveryBudget, RecoveryEnv,
        RecoveryError, RecoveryReactor, RecoveryTerminal, classify, run_with_recovery,
    };
    use crate::engine::{Clock, DrainCtx, ManualClock};
    use crate::metrics::Metrics;
    use kian_core::context::{Message, derive_model_context};
    use kian_core::events::{EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::seams::{
        PolicyRule, ProviderError, RecoveryAction, RecoveryPolicySource, ToolError,
    };
    use kian_store::{EventStore, InboxError, StoreError};

    const T0: u64 = 1_761_900_000_000;

    // -- helpers -----------------------------------------------------------

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-rec-test-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn sample_error(class: ErrorClass) -> RecoveryError {
        match class {
            ErrorClass::ProviderDown => ProviderError::Transport("conn reset".into()).into(),
            ErrorClass::RateLimited => ProviderError::RateLimited.into(),
            ErrorClass::ContextOverflow => ProviderError::ContextOverflow.into(),
            ErrorClass::RequestTooLarge => ProviderError::RequestTooLarge {
                size: 18_235_916,
                limit: Some(33_554_432),
                detail: "over the outbound bound; nothing was sent".into(),
            }
            .into(),
            ErrorClass::ToolFailed => ToolError::Execution("boom".into()).into(),
            ErrorClass::StoreFailure => StoreError::GuardMismatch("test").into(),
            ErrorClass::BudgetExhausted => RecoveryError::BudgetExhausted,
            ErrorClass::ValidationFailed => {
                ProviderError::InvalidResponse("bad json".into()).into()
            }
            ErrorClass::EmptyResponse => ProviderError::EmptyResponse.into(),
            ErrorClass::Unknown => ProviderError::Other("mystery".into()).into(),
        }
    }

    fn rule(class: &str, action: RecoveryAction) -> PolicyRule {
        PolicyRule {
            class: class.into(),
            action,
        }
    }

    /// A tiny deterministic LCG — the property tests need seeded
    /// pseudo-randomness without a rand dependency.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }

        fn class(&mut self) -> ErrorClass {
            ErrorClass::ALL[(self.next() % ErrorClass::ALL.len() as u64) as usize]
        }
    }

    /// The mock provider used by the bounded-recovery property test:
    /// adversarial, always failing, cycling classes from a seed.
    struct AlwaysFailing {
        seq: Mutex<Lcg>,
        fixed: Option<ErrorClass>,
        attempts: AtomicU64,
    }

    impl AlwaysFailing {
        fn fixed(class: ErrorClass) -> Self {
            Self {
                seq: Mutex::new(Lcg(0)),
                fixed: Some(class),
                attempts: AtomicU64::new(0),
            }
        }

        fn seeded(seed: u64) -> Self {
            Self {
                seq: Mutex::new(Lcg(seed)),
                fixed: None,
                attempts: AtomicU64::new(0),
            }
        }

        fn attempts(&self) -> u64 {
            self.attempts.load(Ordering::SeqCst)
        }

        fn next_class(&self) -> ErrorClass {
            match self.fixed {
                Some(class) => class,
                None => self.seq.lock().unwrap().class(),
            }
        }
    }

    /// Records every disposition it executes; its `compact` appends the
    /// real `compact` event through a `DrainCtx` *before* flipping the
    /// prefix-invalidation flag (constraint 3 ordering).
    #[derive(Clone)]
    struct RecordingReactor {
        store: EventStore,
        session: SessionId,
        ctx: DrainCtx,
        log: Arc<Mutex<Vec<String>>>,
        prefix_invalidated: Arc<AtomicBool>,
    }

    impl RecoveryReactor for RecordingReactor {
        fn compact(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<CompactOutcome, RecoveryError>> {
            let reactor = self.clone();
            async move {
                // 1. The observable compact event FIRST.
                let events = reactor.store.rebuild(reactor.session).await?;
                let from = events
                    .iter()
                    .find(|e| {
                        matches!(
                            e.kind,
                            EventKind::InputAdmitted
                                | EventKind::AssistantMessage
                                | EventKind::ToolResult
                        )
                    })
                    .map(|e| e.seq)
                    .ok_or_else(|| RecoveryError::Unknown("no compressible range".into()))?;
                let to = events.last().map(|e| e.seq + 1).unwrap_or(from);
                let ev = reactor
                    .ctx
                    .append(
                        EventKind::Compact,
                        json!({
                            "from_seq": from,
                            "to_seq": to,
                            "summary": {"role": "assistant", "content": "compacted summary"},
                        }),
                    )
                    .await?;
                reactor
                    .log
                    .lock()
                    .unwrap()
                    .push(format!("compact:{}", ev.seq));
                // 2. Only THEN invalidate the prefix.
                reactor.prefix_invalidated.store(true, Ordering::Release);
                reactor.log.lock().unwrap().push("invalidated".into());
                Ok(CompactOutcome {
                    event_seq: ev.seq,
                    from_seq: from,
                    to_seq: to,
                })
            }
            .boxed()
        }

        fn failover(&self) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            let log = Arc::clone(&self.log);
            async move {
                log.lock().unwrap().push("failover".into());
                Ok(())
            }
            .boxed()
        }

        fn ask(
            &self,
            class: ErrorClass,
        ) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            let log = Arc::clone(&self.log);
            async move {
                log.lock().unwrap().push(format!("ask:{}", class.name()));
                Ok(())
            }
            .boxed()
        }
    }

    // -- classification ----------------------------------------------------

    /// Table-driven, pure: every seam/store error shape maps to its
    /// class; the function is synchronous by construction (no clock,
    /// no store, no I/O can enter it).
    #[test]
    fn classify_is_table_driven_and_pure() {
        let ctx = ClassifyCtx { attempt: 0 };
        let table = vec![
            (
                ProviderError::Unavailable("upstream 503".into()).into(),
                ErrorClass::ProviderDown,
            ),
            (
                ProviderError::Transport("timed out".into()).into(),
                ErrorClass::ProviderDown,
            ),
            (ProviderError::RateLimited.into(), ErrorClass::RateLimited),
            (
                ProviderError::ContextOverflow.into(),
                ErrorClass::ContextOverflow,
            ),
            (
                ProviderError::RequestTooLarge {
                    size: 18_235_916,
                    limit: Some(33_554_432),
                    detail: "over providers.vadis.max_request_bytes".into(),
                }
                .into(),
                ErrorClass::RequestTooLarge,
            ),
            (
                ProviderError::EmptyResponse.into(),
                ErrorClass::EmptyResponse,
            ),
            (
                ProviderError::InvalidResponse("bad json".into()).into(),
                ErrorClass::ValidationFailed,
            ),
            (
                ProviderError::Other("mystery".into()).into(),
                ErrorClass::Unknown,
            ),
            (
                ToolError::Execution("boom".into()).into(),
                ErrorClass::ToolFailed,
            ),
            (ToolError::Timeout.into(), ErrorClass::ToolFailed),
            (
                ToolError::InvalidCall("no such tool".into()).into(),
                ErrorClass::ToolFailed,
            ),
            (
                StoreError::StaleEpoch {
                    session: SessionId::new(),
                    event_epoch: Epoch(1),
                    current_epoch: Epoch(2),
                }
                .into(),
                ErrorClass::StoreFailure,
            ),
            (
                InboxError::NoLease {
                    session: SessionId::new(),
                }
                .into(),
                ErrorClass::StoreFailure,
            ),
            (RecoveryError::BudgetExhausted, ErrorClass::BudgetExhausted),
            (
                RecoveryError::ValidationFailed("input shape".into()),
                ErrorClass::ValidationFailed,
            ),
            (RecoveryError::EmptyResponse, ErrorClass::EmptyResponse),
            (
                RecoveryError::Unknown("anything else".into()),
                ErrorClass::Unknown,
            ),
        ];
        for (error, expected) in table {
            assert_eq!(classify(&error, &ctx), expected, "for {error:?}");
            // Determinism: same inputs, same class — purity's observable
            // contract.
            assert_eq!(classify(&error, &ClassifyCtx { attempt: 3 }), expected);
        }
    }

    /// Every class's `name()` is the policy-table key string and
    /// round-trips through `from_name`.
    #[test]
    fn error_class_names_round_trip_through_policy_strings() {
        for class in ErrorClass::ALL {
            let name = class.name();
            assert_eq!(ErrorClass::from_name(name), Some(class), "name: {name}");
        }
        assert_eq!(
            ErrorClass::from_name("provider_down"),
            Some(ErrorClass::ProviderDown)
        );
        assert_eq!(ErrorClass::from_name("no_such_class"), None);
    }

    // -- policy table ------------------------------------------------------

    const POLICY_YAML: &str = r#"
recovery:
  budget:
    tokens: 8
    wall_clock_ms: 120000
    retry_cost_tokens: 2
  rules:
    - class: rate_limited
      action:
        backoff_retry:
          delay_ms: 250
    - class: context_overflow
      action: compress_then_retry
    - class: provider_down
      action: failover
    - class: tool_failed
      action: interrupt_ask
    - class: budget_exhausted
      action: fail
    # duplicate: FIRST match must win (deterministic application)
    - class: rate_limited
      action: fail
"#;

    #[test]
    fn policy_table_parses_from_yaml_and_resolves_deterministically() {
        let config = PolicyConfig::from_yaml(POLICY_YAML).expect("parse");
        assert_eq!(
            config.budget,
            super::BudgetConfig {
                tokens: 8,
                wall_clock_ms: 120_000,
                retry_cost_tokens: 2,
            }
        );
        assert_eq!(config.rules.len(), 6);
        let table = config.table();
        assert_eq!(
            table.resolve(ErrorClass::RateLimited),
            Some(&RecoveryAction::BackoffRetry { delay_ms: 250 }),
            "first matching rule must win over the later 'fail'"
        );
        assert_eq!(
            table.resolve(ErrorClass::ContextOverflow),
            Some(&RecoveryAction::CompressThenRetry)
        );
        assert_eq!(
            table.resolve(ErrorClass::ProviderDown),
            Some(&RecoveryAction::Failover)
        );
        assert_eq!(
            table.resolve(ErrorClass::ToolFailed),
            Some(&RecoveryAction::InterruptAsk)
        );
        assert_eq!(
            table.resolve(ErrorClass::BudgetExhausted),
            Some(&RecoveryAction::Fail)
        );
        // No rule for these classes: honest None, never an invented default.
        assert_eq!(table.resolve(ErrorClass::StoreFailure), None);
        assert_eq!(table.resolve(ErrorClass::Unknown), None);
    }

    /// The bare section (no `recovery:` wrapper) parses identically —
    /// may hand either shape.
    #[test]
    fn policy_section_parses_without_the_recovery_wrapper() {
        let bare = r#"
budget:
  tokens: 3
  wall_clock_ms: 0
rules:
  - class: provider_down
    action:
      backoff_retry:
        delay_ms: 10
"#;
        let config = PolicyConfig::from_yaml(bare).expect("parse");
        assert_eq!(config.budget.tokens, 3);
        assert_eq!(config.budget.wall_clock_ms, 0);
        assert_eq!(
            config.table().resolve(ErrorClass::ProviderDown),
            Some(&RecoveryAction::BackoffRetry { delay_ms: 10 })
        );
    }

    /// Fail-closed: unknown class names and malformed YAML refuse to
    /// load; missing budget fields take the documented defaults.
    #[test]
    fn policy_yaml_fails_closed_on_unknown_class_and_malformed_input() {
        let unknown_class = "rules:\n  - class: not_a_class\n    action: fail\n";
        match PolicyConfig::from_yaml(unknown_class) {
            Err(PolicyError::UnknownClass(name)) => assert_eq!(name, "not_a_class"),
            other => panic!("expected UnknownClass, got {other:?}"),
        }
        let empty_class = "rules:\n  - class: \"\"\n    action: fail\n";
        assert!(matches!(
            PolicyConfig::from_yaml(empty_class),
            Err(PolicyError::EmptyClass)
        ));
        let malformed = "rules: [ this is: not: valid\n";
        assert!(matches!(
            PolicyConfig::from_yaml(malformed),
            Err(PolicyError::Yaml(_))
        ));
        // Defaults: omitted budget fields fall back per-field.
        let defaults = PolicyConfig::from_yaml("rules: []").expect("parse");
        assert_eq!(defaults.budget, super::BudgetConfig::default());
    }

    /// The config-default table is total: every class resolves (no
    /// silent None — a default-less class would fail honestly).
    #[test]
    fn policy_defaults_cover_every_class() {
        let defaults = PolicyConfig::defaults();
        for class in ErrorClass::ALL {
            assert!(
                defaults.table().resolve(class).is_some(),
                "defaults must cover {}",
                class.name()
            );
        }
        // The soft failure (constraint 7) is retried, never dropped, and
        // store failure fails honestly rather than looping.
        let table = defaults.table();
        assert_eq!(
            table.resolve(ErrorClass::EmptyResponse),
            Some(&RecoveryAction::BackoffRetry { delay_ms: 250 })
        );
        assert_eq!(
            table.resolve(ErrorClass::StoreFailure),
            Some(&RecoveryAction::Fail)
        );
        // ADR-067: the same bytes cannot succeed on a retry, and a failover
        // would resend them — the default is an honest fail.
        assert_eq!(
            table.resolve(ErrorClass::RequestTooLarge),
            Some(&RecoveryAction::Fail)
        );
    }

    /// The compiled table IS the built-in `RecoveryPolicySource`
    /// provider (DESIGN §6.2): usable as the seam trait object
    /// wires into the plugin context.
    #[test]
    fn policy_table_is_a_recovery_policy_source() {
        let rules = vec![
            rule(
                "rate_limited",
                RecoveryAction::BackoffRetry { delay_ms: 100 },
            ),
            rule("provider_down", RecoveryAction::Failover),
        ];
        let table = PolicyTable::new(rules.clone());
        let source: &dyn RecoveryPolicySource = &table;
        assert_eq!(source.table(), rules);
    }

    // -- one-shot guard ----------------------------------------------------

    /// The same recovery kind cannot fire twice within one iteration;
    /// a different kind still may; reset re-arms for the next iteration.
    #[test]
    fn one_shot_guard_blocks_a_second_fire_within_one_iteration() {
        let mut guard = OneShotGuard::new();
        assert_eq!(
            guard.try_fire(ErrorClass::ProviderDown),
            GuardDecision::Fired
        );
        assert_eq!(
            guard.try_fire(ErrorClass::ProviderDown),
            GuardDecision::Rejected,
            "same kind twice within one iteration is forbidden"
        );
        assert_eq!(
            guard.try_fire(ErrorClass::RateLimited),
            GuardDecision::Fired
        );
        assert_eq!(
            guard.fired_kinds(),
            vec![ErrorClass::ProviderDown, ErrorClass::RateLimited]
        );
        guard.reset();
        assert!(guard.fired_kinds().is_empty());
        assert_eq!(
            guard.try_fire(ErrorClass::ProviderDown),
            GuardDecision::Fired
        );
    }

    // -- budget ------------------------------------------------------------

    /// Consumption is counted and drawn against the limit; an
    /// over-draw is denied without partial consumption.
    #[test]
    fn budget_consumption_on_retry_is_counted() {
        let clock = Arc::new(ManualClock::new(T0));
        let budget = RecoveryBudget::new(10, 60_000, clock);
        for _ in 0..3 {
            budget.try_consume(2).expect("within limit");
        }
        let snap = budget.snapshot();
        let BudgetSnapshot {
            limit_tokens,
            consumed_tokens,
            refunded_tokens,
            returned_tokens,
            remaining_tokens,
            ..
        } = snap;
        assert_eq!(
            (
                limit_tokens,
                consumed_tokens,
                refunded_tokens,
                returned_tokens
            ),
            (10, 6, 0, 0)
        );
        assert_eq!(remaining_tokens, 4);
        // Over-draw denied, nothing partially consumed.
        assert_eq!(
            budget.try_consume(5),
            Err(BudgetDenied { wall_clock: false }),
            "token pool cannot cover the cost"
        );
        assert_eq!(budget.snapshot().consumed_tokens, 6);
        assert!(!budget.is_exhausted());
    }

    /// Refund returns consumed tokens to the pool; settle (task
    /// completion, A5) returns the unused remainder exactly once.
    #[test]
    fn budget_refund_and_settle_on_completion_are_counted() {
        let clock = Arc::new(ManualClock::new(T0));
        let budget = RecoveryBudget::new(10, 60_000, clock);
        budget.try_consume(3).expect("consume");
        budget.refund(1);
        let snap = budget.snapshot();
        assert_eq!(
            (
                snap.consumed_tokens,
                snap.refunded_tokens,
                snap.remaining_tokens
            ),
            (2, 1, 8)
        );
        // Refund never exceeds what was consumed (no minting tokens).
        budget.refund(100);
        let snap = budget.snapshot();
        assert_eq!((snap.consumed_tokens, snap.refunded_tokens), (0, 3));
        budget.try_consume(4).expect("pool restored by refunds");
        // Completion: the unused remainder (10 - 4) is returned once.
        assert_eq!(budget.settle(), 6);
        assert_eq!(budget.snapshot().returned_tokens, 6);
        assert_eq!(budget.settle(), 0, "settle is one-shot");
        assert_eq!(budget.snapshot().returned_tokens, 6);
        assert_eq!(
            budget.try_consume(1),
            Err(BudgetDenied { wall_clock: false }),
            "a settled budget spends nothing more"
        );
    }

    /// Wall-clock exhaustion denies consumption (and is distinguishable
    /// from token exhaustion for the escalation cause).
    #[test]
    fn budget_wall_clock_exhaustion_denies_consumption() {
        let clock = Arc::new(ManualClock::new(T0));
        let budget = RecoveryBudget::new(100, 1_000, Arc::clone(&clock) as Arc<dyn Clock>);
        budget.try_consume(1).expect("before deadline");
        clock.advance(2_000);
        assert!(budget.is_wall_clock_exhausted());
        assert!(budget.is_exhausted());
        assert_eq!(
            budget.try_consume(1),
            Err(BudgetDenied { wall_clock: true })
        );
        assert_eq!(
            budget.snapshot().consumed_tokens,
            1,
            "denied draw consumes nothing"
        );
    }

    // -- the bounded runner --------------------------------------------------

    /// Bounded-recovery property: for any seeded adversarial
    /// always-failing provider, the runner terminates within the
    /// one-shot-guard bound (≤ ALL.len() + 1 op invocations) and
    /// escalates to the session layer — never an unbounded loop.
    #[tokio::test]
    async fn bounded_recovery_property_always_failing_provider_terminates() {
        // Every class retries: the guard is the only bound.
        let yaml = "budget:\n  tokens: 1024\n  wall_clock_ms: 0\nrules:\n";
        let all_backoff = format!(
            "{}{}",
            yaml,
            ErrorClass::ALL
                .iter()
                .map(|c| format!(
                    "  - class: {}\n    action:\n      backoff_retry:\n        delay_ms: 0\n",
                    c.name()
                ))
                .collect::<String>()
        );
        let config = PolicyConfig::from_yaml(&all_backoff).expect("parse");
        let guard_bound = ErrorClass::ALL.len() as u64 + 1;

        for seed in 0..128u64 {
            let clock = Arc::new(ManualClock::new(T0));
            let env = RecoveryEnv::new(&config, clock);
            let reactor = NullReactor;
            let provider = Arc::new(AlwaysFailing::seeded(seed));
            let fail = Arc::clone(&provider);
            let op = move |_attempt: u64| {
                let fail = Arc::clone(&fail);
                async move {
                    fail.attempts.fetch_add(1, Ordering::SeqCst);
                    let class = fail.next_class();
                    Err::<(), _>(sample_error(class))
                }
            };
            let terminal = run_with_recovery(&env, &reactor, op)
                .await
                .expect_err("terminates");
            assert!(
                matches!(terminal, RecoveryTerminal::EscalatedToSession { .. }),
                "seed {seed}: expected session-layer escalation, got {terminal:?}"
            );
            assert!(
                provider.attempts() <= guard_bound,
                "seed {seed}: {} attempts exceeds the guard bound {}",
                provider.attempts(),
                guard_bound
            );
        }
    }

    /// The fixed single-class adversarial case: exactly two attempts —
    /// the second identical class is refused by the one-shot guard.
    #[tokio::test]
    async fn single_class_failure_escalates_after_guard_in_two_attempts() {
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 100\n  wall_clock_ms: 0\nrules:\n  - class: provider_down\n    action:\n      backoff_retry:\n        delay_ms: 0\n",
        )
        .expect("parse");
        let env = RecoveryEnv::new(&config, Arc::new(ManualClock::new(T0)));
        let provider = Arc::new(AlwaysFailing::fixed(ErrorClass::ProviderDown));
        let fail = Arc::clone(&provider);
        let op = move |_attempt: u64| {
            let fail = Arc::clone(&fail);
            async move {
                fail.attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(sample_error(fail.next_class()))
            }
        };
        let terminal = run_with_recovery(&env, &NullReactor, op)
            .await
            .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::EscalatedToSession {
                cause: super::EscalationCause::OneShotViolated {
                    already_fired: ErrorClass::ProviderDown,
                    incoming: ErrorClass::ProviderDown,
                }
            }
        );
        assert_eq!(provider.attempts(), 2);
    }

    /// Budget exhaustion (guard satisfied — distinct classes each
    /// fire once) escalates to the session layer with the token cause,
    /// and nothing is refunded on a failed task.
    #[tokio::test]
    async fn token_budget_exhaustion_escalates_to_session_layer() {
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 1\n  wall_clock_ms: 0\n  retry_cost_tokens: 1\nrules:\n  - class: rate_limited\n    action:\n      backoff_retry:\n        delay_ms: 0\n  - class: provider_down\n    action:\n      backoff_retry:\n        delay_ms: 0\n",
        )
        .expect("parse");
        let env = RecoveryEnv::new(&config, Arc::new(ManualClock::new(T0)));
        let provider = Arc::new(AtomicU64::new(0));
        let fail = Arc::clone(&provider);
        let op = move |_attempt: u64| {
            let fail = Arc::clone(&fail);
            async move {
                let attempt = fail.fetch_add(1, Ordering::SeqCst);
                let err = if attempt == 0 {
                    sample_error(ErrorClass::RateLimited)
                } else {
                    sample_error(ErrorClass::ProviderDown)
                };
                Err::<(), _>(err)
            }
        };
        let terminal = run_with_recovery(&env, &NullReactor, op)
            .await
            .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::EscalatedToSession {
                cause: super::EscalationCause::TokenBudgetExhausted
            }
        );
        assert_eq!(provider.load(Ordering::SeqCst), 2);
        assert_eq!(
            env.budget.snapshot().returned_tokens,
            0,
            "no refund on failure"
        );
        assert_eq!(env.budget.snapshot().consumed_tokens, 1);
    }

    /// Wall-clock exhaustion is distinguishable in the escalation cause.
    #[tokio::test]
    async fn wall_clock_exhaustion_escalates_with_its_own_cause() {
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 100\n  wall_clock_ms: 1000\nrules:\n  - class: rate_limited\n    action:\n      backoff_retry:\n        delay_ms: 0\n  - class: provider_down\n    action:\n      backoff_retry:\n        delay_ms: 0\n",
        )
        .expect("parse");
        let clock = Arc::new(ManualClock::new(T0));
        let env = RecoveryEnv::new(&config, Arc::clone(&clock) as Arc<dyn Clock>);
        let op = move |_attempt: u64| {
            let clock = Arc::clone(&clock);
            async move {
                clock.advance(2_000);
                let err = if _attempt == 0 {
                    sample_error(ErrorClass::RateLimited)
                } else {
                    sample_error(ErrorClass::ProviderDown)
                };
                Err::<(), _>(err)
            }
        };
        let terminal = run_with_recovery(&env, &NullReactor, op)
            .await
            .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::EscalatedToSession {
                cause: super::EscalationCause::WallClockExhausted
            }
        );
    }

    /// `compress_then_retry` emits the observable `compact` event
    /// through the real event store BEFORE the prefix is invalidated
    /// (constraint 3), and the projection skips exactly `[a,b)`.
    #[tokio::test]
    async fn compress_then_retry_emits_observable_compact_event_before_prefix_invalidation() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        let sid = SessionId::new();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(T0));
        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            Epoch::GENESIS,
            Arc::clone(&clock),
            Arc::new(Metrics::default()),
        )
        .await
        .expect("ctx");

        // Seed a stream: created, input, two assistant messages (seq 0..4).
        ctx.append(EventKind::SessionCreated, json!({}))
            .await
            .expect("created");
        ctx.append(
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "please help"}),
        )
        .await
        .expect("input");
        ctx.append(
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": "first answer"}),
        )
        .await
        .expect("msg1");
        ctx.append(
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": "second answer"}),
        )
        .await
        .expect("msg2");

        let reactor = RecordingReactor {
            store: store.clone(),
            session: sid,
            ctx: ctx.clone(),
            log: Arc::new(Mutex::new(Vec::new())),
            prefix_invalidated: Arc::new(AtomicBool::new(false)),
        };
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 8\n  wall_clock_ms: 0\nrules:\n  - class: context_overflow\n    action: compress_then_retry\n",
        )
        .expect("parse");
        let env = RecoveryEnv::new(&config, clock);

        let log = Arc::clone(&reactor.log);
        let invalidated = Arc::clone(&reactor.prefix_invalidated);
        let store2 = store.clone();
        let sid2 = sid;
        let op = move |attempt: u64| {
            let store = store2.clone();
            let log = Arc::clone(&log);
            let invalidated = Arc::clone(&invalidated);
            async move {
                if attempt == 0 {
                    return Err(sample_error(ErrorClass::ContextOverflow));
                }
                // On the retried attempt: the compact event is already
                // durable AND the invalidation already happened — event
                // first, invalidation second.
                let events = store.rebuild(sid2).await.expect("rebuild");
                assert!(
                    events.iter().any(|e| e.kind == EventKind::Compact),
                    "compact event must exist before the retry runs"
                );
                assert!(invalidated.load(Ordering::Acquire));
                let log = log.lock().unwrap();
                let compact_at = log
                    .iter()
                    .position(|e| e.starts_with("compact:"))
                    .expect("compact disposition logged");
                let invalidated_at = log
                    .iter()
                    .position(|e| e == "invalidated")
                    .expect("invalidated");
                assert!(compact_at < invalidated_at, "event before invalidation");
                Ok("recovered")
            }
        };

        let result = run_with_recovery(&env, &reactor, op)
            .await
            .expect("recovers");
        assert_eq!(result, "recovered");

        // A3/: the projection skips exactly [1,4) — only the summary
        // message remains, from the compact event's payload.
        let events = store.rebuild(sid).await.expect("rebuild");
        assert_eq!(events[0].version, SCHEMA_VERSION);
        let context = derive_model_context(&events).expect("context");
        assert_eq!(
            context,
            vec![Message {
                role: "assistant".into(),
                content: json!("compacted summary"),
                synthetic: false,
            }]
        );

        // Budget was consumed for the one compress-retry and the unused
        // remainder returned on completion (A5).
        let snap = env.budget.snapshot();
        assert_eq!(snap.consumed_tokens, 1);
        assert_eq!(snap.returned_tokens, 7);

        cleanup(&path);
    }

    /// `interrupt_ask` surfaces through the reactor and terminates
    /// honestly as `Ask`; `fail` never invokes the reactor at all.
    #[tokio::test]
    async fn interrupt_ask_and_fail_dispositions_are_honest() {
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 8\n  wall_clock_ms: 0\nrules:\n  - class: tool_failed\n    action: interrupt_ask\n  - class: validation_failed\n    action: fail\n",
        )
        .expect("parse");
        let env = RecoveryEnv::new(&config, Arc::new(ManualClock::new(T0)));

        // tool_failed → ask (an OkReactor: the disposition succeeds)
        let asked = Arc::new(AtomicU64::new(0));
        let asked2 = Arc::clone(&asked);
        let op = move |_attempt: u64| {
            let asked = Arc::clone(&asked2);
            async move {
                asked.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(RecoveryError::from(ToolError::Timeout))
            }
        };
        let reactor = OkReactor::default();
        let terminal = run_with_recovery(&env, &reactor, op)
            .await
            .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::Ask {
                class: ErrorClass::ToolFailed
            }
        );
        assert_eq!(asked.load(Ordering::SeqCst), 1, "ask does not retry");
        assert_eq!(
            reactor.dispositions.lock().unwrap().as_slice(),
            ["ask:tool_failed".to_string()].as_slice()
        );

        // validation_failed → fail, no reactor disposition
        let terminal = run_with_recovery(&env, &reactor, |_a| async {
            Err::<(), _>(RecoveryError::from(ProviderError::InvalidResponse(
                "unparseable".into(),
            )))
        })
        .await
        .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::Failed {
                class: ErrorClass::ValidationFailed
            }
        );

        // No rule covers the class → honest fail, no invented default.
        let terminal = run_with_recovery(&env, &reactor, |_a| async {
            Err::<(), _>(RecoveryError::from(StoreError::GuardMismatch("x")))
        })
        .await
        .expect_err("terminates");
        assert_eq!(
            terminal,
            RecoveryTerminal::Failed {
                class: ErrorClass::StoreFailure
            }
        );
    }

    /// A successful run settles the budget: the retry consumed, the
    /// unused remainder returned (A5 refund semantics).
    #[tokio::test]
    async fn successful_run_settles_the_budget_refunding_the_remainder() {
        let config = PolicyConfig::from_yaml(
            "budget:\n  tokens: 5\n  wall_clock_ms: 0\n  retry_cost_tokens: 1\nrules:\n  - class: rate_limited\n    action:\n      backoff_retry:\n        delay_ms: 0\n",
        )
        .expect("parse");
        let env = RecoveryEnv::new(&config, Arc::new(ManualClock::new(T0)));
        let op = |attempt: u64| async move {
            if attempt == 0 {
                Err(sample_error(ErrorClass::RateLimited))
            } else {
                Ok(7_u32)
            }
        };
        let result = run_with_recovery(&env, &NullReactor, op)
            .await
            .expect("succeeds");
        assert_eq!(result, 7);
        let snap = env.budget.snapshot();
        assert_eq!(snap.consumed_tokens, 1, "one retry was charged");
        assert_eq!(snap.returned_tokens, 4, "5 - 1 returned on completion");
        assert_eq!(snap.refunded_tokens, 0);
    }

    /// A no-op reactor for paths whose dispositions are not under test.
    struct NullReactor;

    impl RecoveryReactor for NullReactor {
        fn compact(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<CompactOutcome, RecoveryError>> {
            async { Err(RecoveryError::Unknown("compact not configured".into())) }.boxed()
        }
        fn failover(&self) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            async { Err(RecoveryError::Unknown("failover not configured".into())) }.boxed()
        }
        fn ask(
            &self,
            _class: ErrorClass,
        ) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            async { Err(RecoveryError::Unknown("ask not configured".into())) }.boxed()
        }
    }

    /// The opposite default: every disposition succeeds (records it).
    #[derive(Default)]
    struct OkReactor {
        dispositions: Mutex<Vec<String>>,
    }

    impl RecoveryReactor for OkReactor {
        fn compact(
            &self,
        ) -> futures::future::BoxFuture<'static, Result<CompactOutcome, RecoveryError>> {
            async {
                Ok(CompactOutcome {
                    event_seq: 0,
                    from_seq: 0,
                    to_seq: 0,
                })
            }
            .boxed()
        }
        fn failover(&self) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            self.dispositions.lock().unwrap().push("failover".into());
            async { Ok(()) }.boxed()
        }
        fn ask(
            &self,
            class: ErrorClass,
        ) -> futures::future::BoxFuture<'static, Result<(), RecoveryError>> {
            self.dispositions
                .lock()
                .unwrap()
                .push(format!("ask:{}", class.name()));
            async { Ok(()) }.boxed()
        }
    }
}
