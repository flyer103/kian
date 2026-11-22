//! Closed decision-reason vocabularies (plan): the
//! machine-stable reason codes stamped on every intake audit line and
//! the assembly metrics snapshot, plus the judge-side decision-tag
//! convention rendered into the Router's prompt template.
//!
//! Today every intake decision is auditable but its reason is prose;
//! the prototype's panel reads Decision reasons from a closed
//! vocabulary (reference 005). The codes ride **audit lines and
//! counters only** — [`crate::subscriptions::RoutingOutcome`]'s shape
//! is frozen (no ripple), and no new event kind exists: the durable
//! trace of a Router decision stays the existing
//! `tool/call`/`thread/started`/`channel/delivered` triple.
//!
//! Two vocabularies:
//!
//! - [`IntakeReason`] — the *intake* side, one code per decision the
//!   subscription service makes (drop or admit), stamped as
//!   `reason=<code>` into the audit line and counted per code by the
//!   assembly snapshot (`kian-cli` `SubscriptionMetrics`).
//! - [`DecisionKind`] — the *judge* side, the closed tag convention the
//!   Router is instructed to spell in its reply text
//!   (`decision:<accept|stop|ignore|unavailable>`). This is the soft
//!   half: model-visible convention, not a parsed contract; the hard
//!   half is the durable event triple, unchanged.
//!
//! Both enums are **closed**: consumers match on the enum, never parse
//! the string form upstream. The serde names and display strings are
//! golden (see `tests/reason_codes.rs`); changing either is a
//! deliberate, visible contract edit.

use kian_core::model::SessionId;
use serde::{Deserialize, Serialize};

use crate::subscriptions::RoutingOutcome;

/// The closed intake-reason vocabulary. One code per decision;
/// `code()` is the stable machine spelling used in audit lines and
/// metric keys, `display()` the human table.
///
/// `ReleasedToEval` is defined now and lands with the follow-up
/// ownership card: an all-ended subscriber set releases the
/// topic to a fresh evaluation. Until then no code path produces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntakeReason {
    /// No subscription rule matched the event.
    NoRule,
    /// The admission class resolved to `ignore`.
    ClassIgnored,
    /// The admission class is `explicit-only` and the event
    /// carried no explicit-request signal.
    NotExplicit,
    /// Admitted to the Router (a `router` rule, or the empty-set
    /// passthrough fallback) for judgment.
    AdmittedRouter,
    /// Admitted directly to a target without Router judgment: a
    /// passthrough subscriber, or a `Route::Session` fixed target
    /// (the watchdog ticks) — the shared "no judge turn spent"
    /// code, the closed set has no per-route variant.
    AdmittedPassthrough,
    /// The `(session, event_id)` row already existed — exactly one
    /// durable row either way (idempotent admission).
    Deduplicated,
    /// Reserved for: an all-ended subscriber set falls back to the
    /// Router as a *release to fresh evaluation*, not a plain admit.
    ReleasedToEval,
    /// A channel answer resolved a pending `ask` and was submitted to
    /// the confirmation plane (CONFIRMATION-MODEL §14d).
    AnswerApplied,
    /// Command-shaped input that matched no live pending decision
    /// (wrong code, a lapsed conversation, or a malformed code).
    AnswerNoMatch,
    /// Command-shaped input whose code matched several pending calls
    /// (a derivation collision — never guessed at).
    AnswerAmbiguous,
    /// The code resolved but the confirmation write failed; nothing was
    /// decided.
    AnswerWriteFailed,
}

impl IntakeReason {
    /// The closed set, in declaration order (snapshot shape).
    pub const ALL: [Self; 11] = [
        Self::NoRule,
        Self::ClassIgnored,
        Self::NotExplicit,
        Self::AdmittedRouter,
        Self::AdmittedPassthrough,
        Self::Deduplicated,
        Self::ReleasedToEval,
        Self::AnswerApplied,
        Self::AnswerNoMatch,
        Self::AnswerAmbiguous,
        Self::AnswerWriteFailed,
    ];

    /// The stable machine code — identical to the serde name, and the
    /// key audit lines and counters are stamped with.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NoRule => "no_rule",
            Self::ClassIgnored => "class_ignored",
            Self::NotExplicit => "not_explicit",
            Self::AdmittedRouter => "admitted_router",
            Self::AdmittedPassthrough => "admitted_passthrough",
            Self::Deduplicated => "deduplicated",
            Self::ReleasedToEval => "released_to_eval",
            Self::AnswerApplied => "answer_applied",
            Self::AnswerNoMatch => "answer_no_match",
            Self::AnswerAmbiguous => "answer_ambiguous",
            Self::AnswerWriteFailed => "answer_write_failed",
        }
    }

    /// The golden display text (the panel/CLI table).
    #[must_use]
    pub const fn display(self) -> &'static str {
        match self {
            Self::NoRule => "dropped: no subscription rule matched",
            Self::ClassIgnored => "dropped: admission class ignore",
            Self::NotExplicit => "dropped: explicit-only without an explicit request",
            Self::AdmittedRouter => "admitted to the Router for judgment",
            Self::AdmittedPassthrough => "admitted directly without Router judgment",
            Self::Deduplicated => "deduplicated against the existing row",
            Self::ReleasedToEval => "released to a fresh evaluation",
            Self::AnswerApplied => "answer applied to a pending confirmation",
            Self::AnswerNoMatch => "dropped: command-shaped, no pending decision matched",
            Self::AnswerAmbiguous => "dropped: command-shaped, several pending decisions matched",
            Self::AnswerWriteFailed => "dropped: the confirmation write failed",
        }
    }
}

/// The closed judge-side decision-tag vocabulary: the four tags
/// the Router is instructed to spell in its reply text, one per batch
/// decision. Convention, not contract — the durable trace of a Router
/// decision is the existing event triple; the tag makes the *model's*
/// stated decision cheaply visible to readers of the reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    /// Act on the input (dispatch a worker, or reply directly).
    Accept,
    /// End the watch/ticket on purpose.
    Stop,
    /// No action needed for this input.
    Ignore,
    /// Cannot act (missing capability, blocked dependency).
    Unavailable,
}

impl DecisionKind {
    /// The closed set, in declaration order.
    pub const ALL: [Self; 4] = [Self::Accept, Self::Stop, Self::Ignore, Self::Unavailable];

    /// The stable tag spelling (`decision:<code>` in reply text).
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Stop => "stop",
            Self::Ignore => "ignore",
            Self::Unavailable => "unavailable",
        }
    }

    /// The golden display text.
    #[must_use]
    pub const fn display(self) -> &'static str {
        match self {
            Self::Accept => "accept: act on the input",
            Self::Stop => "stop: end the watch",
            Self::Ignore => "ignore: no action needed",
            Self::Unavailable => "unavailable: cannot act",
        }
    }

    /// The reply-text tag form the Router spells (`decision:accept`,
    /// …). Boundary helper for readers of reply text; upstream
    /// consumers match the enum.
    #[must_use]
    pub fn tag(self) -> String {
        format!("decision:{}", self.code())
    }
}

/// The reason for one admitted row: dedup wins over the admit code
/// (the row already existed — no new decision was spent), then the
/// Router target vs a direct target. This is the single source of
/// truth for admit-path codes: the audit line in
/// [`crate::subscriptions`] and the assembly counters both render
/// through it, so they cannot drift.
#[must_use]
pub fn routed_reason(target: SessionId, router: SessionId, deduplicated: bool) -> IntakeReason {
    if deduplicated {
        IntakeReason::Deduplicated
    } else if target == router {
        IntakeReason::AdmittedRouter
    } else {
        IntakeReason::AdmittedPassthrough
    }
}

/// Exactly one [`IntakeReason`] per decision inside a
/// [`RoutingOutcome`]: single for drops and `Routed`, one per
/// admission for `FannedOut`. The assembly metrics snapshot counts
/// through this, so "once per decision" holds by construction.
#[must_use]
pub fn outcome_reasons(outcome: &RoutingOutcome, router: SessionId) -> Vec<IntakeReason> {
    match outcome {
        RoutingOutcome::DroppedNoRule => vec![IntakeReason::NoRule],
        RoutingOutcome::DroppedIgnored => vec![IntakeReason::ClassIgnored],
        RoutingOutcome::DroppedNotExplicit => vec![IntakeReason::NotExplicit],
        // A hook veto refuses the event for every target before any
        // admission decision exists — there is no per-decision reason
        // to count (the veto's own audit line on `kian::subscriptions`
        // and the bus's `hook-veto` line on `kian::hook` carry it).
        RoutingOutcome::RefusedByHook { .. } => Vec::new(),
        RoutingOutcome::Routed {
            session,
            deduplicated,
        } => {
            vec![routed_reason(*session, router, *deduplicated)]
        }
        RoutingOutcome::FannedOut { admitted } => admitted
            .iter()
            .map(|a| routed_reason(a.session, router, a.deduplicated))
            .collect(),
        RoutingOutcome::Answered { .. } => vec![IntakeReason::AnswerApplied],
        RoutingOutcome::DroppedAnswerNoMatch => vec![IntakeReason::AnswerNoMatch],
        RoutingOutcome::DroppedAnswerAmbiguous => vec![IntakeReason::AnswerAmbiguous],
        RoutingOutcome::DroppedAnswerWriteFailed => vec![IntakeReason::AnswerWriteFailed],
    }
}
