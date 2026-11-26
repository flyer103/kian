//! The subscription service: consume `ChannelEvent` streams
//! from registered [`ChannelAdapter`]s, evaluate the declarative rule
//! set, admit a provenance-wrapped input to the routed target — the row
//! durable **before** the wake fires (invariant 1) — then wake it.
//!
//! Routing (DESIGN §4.1): `router` rules admit to the configured
//! resident Router session; `passthrough` rules correlate the event's
//! topic through [`PassthroughRegistry`], admitting straight into the
//! owning worker's inbox without Router judgment. Since the
//! registry is many-to-many: **one row per subscriber** is admitted
//! (idempotent per `(session, event_id)` — the store's dedupe index is
//! per-session), so workers sharing a topic observe each other's
//! events. A mid-set write failure does not prevent the remaining
//! subscribers' rows: the loop continues, the error is returned for the
//! adapter's idempotent retry, and every admission and every drop is
//! named in the audit log. A passthrough topic with an **empty**
//! subscriber set falls back to the Router (the bootstrap path: the
//! Router judges the first event of an unknown topic and the spawner
//! binds the worker; both transitions leave an audit log line).
//!
//! (follow-up ownership): fan-out admits to **live** subscribers
//! only — the registry's liveness projection filters ended workers out
//! of the admission set, so an event never strands on a terminal
//! session. A topic whose subscribers have *all* ended falls back to
//! the Router as a **release to fresh evaluation** (002's Ticket-end
//! rule), audited with the closed `released_to_eval` reason code and
//! naming the released topic. Config-gated by
//! `superagent.followup_ownership` (default ); off restores the
//! legacy behavior (every historical subscriber admits).
//!
//! (ADR-012 mandate revalidation): before a follow-up admits to a
//! live subscriber, its mandate is revalidated — `strength: explicit`
//! passes; `inferred` passes while its `policy_version` is still the
//! current pointer (`superagent.policy.version`). A released
//! worker drops out of the admission set with an honest audit line
//! (`released_to_eval`), and a set emptied entirely by releases is the
//! same release-to-fresh-eval as an all-ended topic. Legacy workers
//! (no mandate object) always pass — legacy authority is never
//! retroactively narrowed.
//!
//! Events matching no rule are dropped with an audit log line and no
//! inbox row — an honest no-op, never a silent swallow.
//!
//! every drop/admit audit line stamps a machine-stable
//! `reason=<code>` from the closed [`crate::reasons::IntakeReason`]
//! vocabulary (single source of truth: the admit lines and the
//! assembly counters both render through
//! [`crate::reasons::routed_reason`], so they cannot drift).

use std::sync::{Arc, RwLock};

use futures::StreamExt;
use kian_core::model::SessionId;
use kian_core::seams::{ChannelAdapter, ChannelEvent};
use tracing::info;
use tracing::warn;

use crate::inbox::{InboxWriteError, InboxWriter};
use crate::provenance::{ShapingConfig, shape_channel_input_marked};
use crate::reasons::{IntakeReason, routed_reason};
use crate::registry::PassthroughRegistry;
use crate::rules::{
    AdmissionClass, AdmissionClassTable, Route, SubscriptionRuleSet, event_has_explicit_signal,
};

/// What handling one channel event concluded — the auditable decision,
/// returned to callers and asserted in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingOutcome {
    /// Admitted to `session` (durable row, then wake). `deduplicated`
    /// is true when the event's id had already been admitted — exactly
    /// one row exists either way. The single-target outcome: `router`
    /// routes, the empty-set Router fallback, and a passthrough topic
    /// with exactly one subscriber.
    Routed {
        session: SessionId,
        deduplicated: bool,
    },
    /// Fan-out: the event was admitted to every subscriber of
    /// the passthrough topic — one record per durable row, in
    /// subscriber-set (sorted) order. Returned only for sets of two or
    /// more.
    FannedOut { admitted: Vec<FanoutAdmission> },
    /// No rule matched: audit-logged, no inbox row, no wake.
    DroppedNoRule,
    /// admission class `ignore`: the event's provenance resolved
    /// to Ignore — audit-logged and counted, no inbox row, no wake,
    /// decided before the rule set and before any model call.
    DroppedIgnored,
    /// admission class `explicit-only` without an explicit-request
    /// signal (no @-mention, no adapter-declared `explicit: true`):
    /// audit-logged and counted, no inbox row, no wake.
    DroppedNotExplicit,
    /// A hook veto at `admit/pre` (PLUGIN-SPEC §4.1): a valid `stop`
    /// (or the fail-closed default of an unreachable `veto:admit`
    /// holder at channel origin) refused the event for **every**
    /// target — no inbox row anywhere, one audit line, and the sender
    /// is not told in v1 (§16.15). A redelivery is re-vetoed (§8.6).
    RefusedByHook { plugin: String, reason: String },
    /// An inbound message on an answers-enabled channel resolved to a
    /// pending call and was submitted to the confirmation plane
    /// (CONFIRMATION-MODEL §14d): exactly one `control/confirm` row
    /// when `applied` is `accepted`; no row at all when the race (or
    /// the TTL) already settled the call. Never an inbox row — the
    /// model never sees an answer.
    Answered {
        session: SessionId,
        call_id: String,
        decision: kian_core::control::ConfirmDecision,
        applied: crate::answers::AnswerOutcome,
    },
    /// Command-shaped input (a closed verb plus a code-shaped token)
    /// that matched no live pending decision: dropped and audited,
    /// never admitted — a typo cannot become a Router instruction.
    DroppedAnswerNoMatch,
    /// Command-shaped input whose code matched two or more pending
    /// calls (a derivation collision): dropped and audited, never
    /// guessed at.
    DroppedAnswerAmbiguous,
    /// The code resolved but the confirmation write failed; nothing
    /// was decided. The audit line carries the error; a retry is
    /// idempotent at the `(session, call_id)` key.
    DroppedAnswerWriteFailed,
}

/// One subscriber's admission inside a [`RoutingOutcome::FannedOut`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutAdmission {
    /// The subscriber whose inbox holds the durable row.
    pub session: SessionId,
    /// True when this subscriber already held a row for the event id
    /// (idempotent per `(session, event_id)`).
    pub deduplicated: bool,
}

/// The subscription service over one rule set and one inbox port.
///
/// Holds the passthrough registry behind a lock: it is runtime-mutable
/// (the spawner registers workers) yet disposable — a restart replaces
/// it wholesale with the projection rebuilt from event streams
/// ([`SubscriptionService::install_registry`]). Since the lock
/// lives behind a shared `Arc` ([`SubscriptionService::shared_registry`])
/// so the Router's tool surface (the `spawn_worker` ownership check)
/// and the fan-out path read the *same* projection — one ownership
/// truth, still disposable.
pub struct SubscriptionService {
    /// ADR-040 binding: the `router` route resolves the live session at
    /// dispatch time, so a rotation re-points every fallback.
    router_session: Arc<crate::binding::ResidentBinding>,
    rules: SubscriptionRuleSet,
    classes: AdmissionClassTable,
    shaping: ShapingConfig,
    /// channels whose events wrap with the untrusted bit. Empty
    /// (the default) marks nothing — every wrapper stays byte-for-byte
    /// the legacy form.
    untrusted_channels: std::collections::BTreeSet<String>,
    /// the follow-up-ownership gate (`superagent.followup_ownership`,
    /// default ). On, fan-out admits to live subscribers only and an
    /// all-ended set releases the topic to the Router (fresh Eval);
    /// off, every historical subscriber admits — the legacy behavior.
    followup_ownership: bool,
    /// (ADR-012): the current policy-version pointer (the
    /// label, `superagent.policy.version`) the live-subscriber
    /// revalidation compares each mandate against. `None` (the
    /// default) is the honest `none` label: an unversioned deployment
    /// never releases on version drift, and a `none`-stamped mandate
    /// matches it.
    policy_pointer: Option<String>,
    /// /ADR-013: the configured thread-key derivations
    /// (`superagent.intake.thread_keys`). Correlation is context
    /// only: the derived key surfaces in the Inline `neighbors`
    /// block (`thread_key`) so the Router and workers reach sibling
    /// threads through the recall toolpack — it never routes (the
    /// subscription rules stay topic-keyed; a poll-mode adapter's
    /// topics are the derived keys already, produced at mapping
    /// time). Empty (the default) stamps nothing — byte-identical
    /// wrappers.
    thread_keys: crate::intake::ThreadKeySet,
    registry: Arc<RwLock<PassthroughRegistry>>,
    inbox: Arc<dyn InboxWriter>,
    /// Port (a) — `admit/pre` + `admit/post` (PLUGIN-SPEC §4.1): the
    /// hook bus consulted at this shaping boundary, after the payload
    /// is shaped and before the first inbox row. `None` (default)
    /// consults nothing — byte-identical fan-out behavior.
    hooks: Option<kian_plugin::hooks::HookBus>,
    /// the answer surface (CONFIRMATION-MODEL §14d). `None` (the
    /// default) keeps the intake byte-identical: no channel may answer
    /// a pending `ask` and no interceptor runs.
    answers: Option<crate::answers::AnswerSurface>,
}

impl SubscriptionService {
    pub fn new(
        router_session: Arc<crate::binding::ResidentBinding>,
        rules: SubscriptionRuleSet,
        inbox: Arc<dyn InboxWriter>,
    ) -> Self {
        Self {
            router_session,
            rules,
            classes: AdmissionClassTable::default(),
            shaping: ShapingConfig::default(),
            untrusted_channels: std::collections::BTreeSet::new(),
            followup_ownership: true,
            policy_pointer: None,
            thread_keys: crate::intake::ThreadKeySet::default(),
            registry: Arc::new(RwLock::new(PassthroughRegistry::new())),
            inbox,
            hooks: None,
            answers: None,
        }
    }

    /// [`SubscriptionService::new`] plus the admission-class
    /// table: the provenance-keyed Fast Path evaluated **before** the
    /// rule set. An empty table is exactly `new` (today's behavior).
    pub fn with_classes(
        router_session: Arc<crate::binding::ResidentBinding>,
        rules: SubscriptionRuleSet,
        classes: AdmissionClassTable,
        inbox: Arc<dyn InboxWriter>,
    ) -> Self {
        Self {
            router_session,
            rules,
            classes,
            shaping: ShapingConfig::default(),
            untrusted_channels: std::collections::BTreeSet::new(),
            followup_ownership: true,
            policy_pointer: None,
            thread_keys: crate::intake::ThreadKeySet::default(),
            registry: Arc::new(RwLock::new(PassthroughRegistry::new())),
            inbox,
            hooks: None,
            answers: None,
        }
    }

    /// Mounts the hook bus at the admission boundary (PLUGIN-SPEC §4
    /// row (a)): `admit/pre` is evaluated once per inbound event over
    /// the complete target set before any inbox row exists, and
    /// `admit/post` fires per admitted target after its row is durable.
    pub fn with_hook_bus(mut self, hooks: kian_plugin::hooks::HookBus) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Mounts the answer surface (CONFIRMATION-MODEL §14d): on the
    /// configured channels, a message whose whole text is a closed
    /// command (`approve <code>` / `reject <code> [reason]`) is
    /// intercepted here — before the admission class, before the rule
    /// set, before any model call — and written as a `control/confirm`
    /// row through the assembly's [`crate::answers::AnswerPort`].
    pub fn with_answers(mut self, answers: crate::answers::AnswerSurface) -> Self {
        self.answers = Some(answers);
        self
    }

    /// Sets the follow-up-ownership gate (`superagent.followup_ownership`,
    /// default ). `false` restores the legacy behavior: the fan-out
    /// admission set is every historical subscriber, live or ended.
    pub fn with_followup_ownership(mut self, enabled: bool) -> Self {
        self.followup_ownership = enabled;
        self
    }

    /// Sets the current policy-version pointer
    /// (`superagent.policy.version`): the label the live-subscriber
    /// mandate revalidation compares against on every follow-up
    /// (ADR-012: `explicit` passes; `inferred` passes only while its
    /// stamped version is still current — otherwise the worker
    /// releases to fresh evaluation, audited `released_to_eval`).
    pub fn with_policy_pointer(mut self, label: Option<String>) -> Self {
        self.policy_pointer = label;
        self
    }

    /// Shares the registry: the handle returned here is the one
    /// this service reads and writes — installing it into the Router's
    /// tool surface gives both sides one ownership projection. A
    /// startup [`SubscriptionService::install_registry`] through either
    /// side re-derives the same shared state.
    pub fn shared_registry(&self) -> Arc<RwLock<PassthroughRegistry>> {
        Arc::clone(&self.registry)
    }

    /// Adopts an already-shared registry handle (assembly wiring):
    /// after this the service and every other holder of the Arc read
    /// and write one projection. Call before any
    /// [`SubscriptionService::shared_registry`] handle is taken, so no
    /// detached copies exist.
    pub fn with_shared_registry(mut self, shared: Arc<RwLock<PassthroughRegistry>>) -> Self {
        self.registry = shared;
        self
    }

    /// Runtime transition of the liveness projection: marks a
    /// worker's stream ended. The topic sets keep the worker as a
    /// historical subscriber; the fan-out live view drops it.
    pub fn mark_worker_ended(&self, worker: SessionId) {
        self.registry
            .write()
            .expect("passthrough registry lock")
            .mark_ended(worker);
    }

    /// Sets the admission-time shaping policy. The default (infinite
    /// cap) is byte-identical to the legacy single-row wrapper.
    pub fn with_shaping(mut self, shaping: ShapingConfig) -> Self {
        self.shaping = shaping;
        self
    }

    /// Sets the untrusted-channel list (prompt-injection
    /// hygiene): events from these channels admit with
    /// `"untrusted": true` inside the wrapper's `content`. Honest
    /// labeling only — routing, classes and rules are unchanged.
    pub fn with_untrusted_channels(mut self, channels: std::collections::BTreeSet<String>) -> Self {
        self.untrusted_channels = channels;
        self
    }

    /// Sets the thread-key derivations
    /// (`superagent.intake.thread_keys`): the derived key of an
    /// over-cap (spilled) event surfaces in its Inline `neighbors`
    /// block as `thread_key` recall context. Context only — never a
    /// routing input. An empty set (the default) changes nothing.
    pub fn with_thread_keys(mut self, keys: crate::intake::ThreadKeySet) -> Self {
        self.thread_keys = keys;
        self
    }

    /// The configured resident Router session.
    pub fn router_session(&self) -> SessionId {
        self.router_session.load()
    }

    /// Runtime registration of a passthrough binding (the worker
    /// spawner). Restart-equivalent state comes from
    /// [`SubscriptionService::install_registry`] instead.
    pub fn register_topic(&self, topic: impl Into<String>, worker: SessionId) {
        self.registry
            .write()
            .expect("passthrough registry lock")
            .register(topic, worker);
    }

    /// Installs a registry rebuilt from event streams at startup
    /// (constraint 1: the projection is disposable and re-derived, so
    /// no worker binding survives only in memory).
    pub fn install_registry(&self, rebuilt: PassthroughRegistry) {
        *self.registry.write().expect("passthrough registry lock") = rebuilt;
    }

    /// Read-only snapshot (inspection/tests).
    pub fn registry_snapshot(&self) -> PassthroughRegistry {
        self.registry
            .read()
            .expect("passthrough registry lock")
            .clone()
    }

    /// Tries the answer surface (CONFIRMATION-MODEL §14d). `None` means
    /// "not an answer": the event falls through to the ordinary intake
    /// path unchanged. `Some` means the event was consumed here — it
    /// never becomes an inbox row.
    async fn answer_attempt(
        &self,
        event: &ChannelEvent,
    ) -> Option<Result<RoutingOutcome, InboxWriteError>> {
        let surface = self.answers.as_ref()?;
        if !surface.covers(&event.channel) {
            return None;
        }
        let text = crate::answers::message_text(event)?;
        let command = match crate::answers::parse_answer(text) {
            crate::answers::ParseOutcome::None => return None,
            crate::answers::ParseOutcome::Command(command) => command,
        };
        // The answer is scoped to the conversation the message arrived
        // on: the candidates are that topic's live subscribers, rebuilt
        // from the stream at startup (constraint 1). No topic, no
        // candidates — fail closed, never a global scan.
        let Some(topic) = event.topic.as_deref() else {
            info!(
                target: "kian::subscriptions",
                channel = %event.channel,
                event_id = %event.event_id,
                reason = %IntakeReason::AnswerNoMatch.code(),
                "subscription audit: command-shaped message dropped (no topic)"
            );
            return Some(Ok(RoutingOutcome::DroppedAnswerNoMatch));
        };
        let workers: Vec<SessionId> = {
            let registry = self.registry.read().expect("passthrough registry lock");
            registry.resolve_live(topic).into_iter().collect()
        };
        let mut hits: Vec<(SessionId, String)> = Vec::new();
        for worker in workers {
            match surface.matches(worker, &command.code).await {
                Ok(call_ids) => {
                    hits.extend(call_ids.into_iter().map(|call_id| (worker, call_id)));
                }
                Err(error) => warn!(
                    target: "kian::subscriptions",
                    worker = %worker.0,
                    error = %error,
                    "answer surface: pending-set read failed on this worker"
                ),
            }
        }
        match hits.len() {
            0 => {
                info!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    topic = %topic,
                    reason = %IntakeReason::AnswerNoMatch.code(),
                    "subscription audit: command-shaped message matched no pending decision"
                );
                Some(Ok(RoutingOutcome::DroppedAnswerNoMatch))
            }
            1 => {
                let (worker, call_id) = hits.remove(0);
                let principal = answer_principal(event);
                let applied = match surface
                    .submit(
                        worker,
                        call_id.clone(),
                        command.decision,
                        command.reason,
                        principal.clone(),
                    )
                    .await
                {
                    Ok(applied) => applied,
                    Err(error) => {
                        warn!(
                            target: "kian::subscriptions",
                            worker = %worker.0,
                            call_id = %call_id,
                            error = %error,
                            "answer surface: confirmation write failed"
                        );
                        info!(
                            target: "kian::subscriptions",
                            channel = %event.channel,
                            event_id = %event.event_id,
                            call_id = %call_id,
                            reason = %IntakeReason::AnswerWriteFailed.code(),
                            "subscription audit: channel answer could not be written"
                        );
                        return Some(Ok(RoutingOutcome::DroppedAnswerWriteFailed));
                    }
                };
                info!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    worker = %worker.0,
                    call_id = %call_id,
                    decision = command.decision.as_str(),
                    applied = applied.code(),
                    principal = %principal,
                    reason = %IntakeReason::AnswerApplied.code(),
                    "subscription audit: channel answer submitted to the confirmation plane"
                );
                Some(Ok(RoutingOutcome::Answered {
                    session: worker,
                    call_id,
                    decision: command.decision,
                    applied,
                }))
            }
            _ => {
                info!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    matches = hits.len(),
                    reason = %IntakeReason::AnswerAmbiguous.code(),
                    "subscription audit: command-shaped message matched several pending decisions"
                );
                Some(Ok(RoutingOutcome::DroppedAnswerAmbiguous))
            }
        }
    }

    /// Handles one inbound channel event: rule match → route →
    /// durable admit (idempotent per `(session, event_id)`) → wake.
    ///
    /// Ordering is the point (invariant 1): each subscriber's `admit`
    /// future is awaited to completion — the row durably committed —
    /// *before* its `wake` is invoked. A failed admit wakes nothing;
    /// in a fan-out the remaining subscribers are still admitted (the
    /// per-`(session, event_id)` idempotency means the adapter's retry
    /// fills only the gap) and the first error is returned.
    pub async fn handle_event(
        &self,
        event: ChannelEvent,
    ) -> Result<RoutingOutcome, InboxWriteError> {
        // The answer surface runs first (CONFIRMATION-MODEL §14d): a
        // command-shaped message on a channel that opted in is either
        // applied to a pending call or dropped — it is never admitted,
        // so no prompt can reinterpret an answer. The admission-class
        // gate below cannot be reached by an answer: the boot
        // validation refuses a channel that is both an answer channel
        // and `class: ignore`.
        if let Some(outcome) = self.answer_attempt(&event).await {
            return outcome;
        }

        // Fast Path first: the admission class over provenance
        // decides in code, before the rule set and before any model
        // call could be spent. `Candidate` (and an absent table) falls
        // through unchanged; both drop shapes are honest no-ops —
        // audit line, no row, no wake (the DroppedNoRule discipline).
        match self.classes.resolve(&event) {
            AdmissionClass::Ignore => {
                info!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    topic = event.topic.as_deref().unwrap_or(""),
                    reason = %IntakeReason::ClassIgnored.code(),
                    "subscription audit: event dropped by admission class `ignore`"
                );
                return Ok(RoutingOutcome::DroppedIgnored);
            }
            AdmissionClass::ExplicitOnly if !event_has_explicit_signal(&event) => {
                info!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    topic = event.topic.as_deref().unwrap_or(""),
                    reason = %IntakeReason::NotExplicit.code(),
                    "subscription audit: event dropped by admission class `explicit-only` \
                     (no explicit-request signal)"
                );
                return Ok(RoutingOutcome::DroppedNotExplicit);
            }
            AdmissionClass::Candidate | AdmissionClass::ExplicitOnly => {}
        }

        let Some(rule) = self.rules.first_match(&event) else {
            info!(
                target: "kian::subscriptions",
                channel = %event.channel,
                event_id = %event.event_id,
                reason = %IntakeReason::NoRule.code(),
                "subscription audit: event dropped, no matching rule"
            );
            return Ok(RoutingOutcome::DroppedNoRule);
        };

        // The subscriber set for the matched rule. `router` routes (and
        // the empty-set fallback) are the single target; passthrough
        // resolves the topic's set — zero (fallback), one, or many.
        // `session` pins the event to one config-addressed fixed
        // session — the timer's ticks reach the watchdog this way.
        // with follow-up ownership on (the default), the
        // admission set is the *live* subscriber set — an all-ended
        // topic releases to the Router as a fresh evaluation
        // (`released_to_eval`), the bootstrap-empty fallback keeps its
        // `admitted_router` shape.
        // (ADR-012): each live subscriber's mandate is
        // revalidated under the same lock — `explicit` passes,
        // `inferred` passes while its `policy_version` is the current
        // pointer, and a release drops the worker from the admission
        // set (honestly audited below; a set emptied by releases is
        // the same release-to-fresh-eval as an all-ended one).
        let targets: Vec<SessionId> = match &rule.route {
            Route::Router => vec![self.router_session.load()],
            Route::Session(session) => vec![*session],
            Route::Passthrough { .. } => {
                let resolution = event.topic.as_deref().map(|t| {
                    let registry = self.registry.read().expect("passthrough registry lock");
                    if self.followup_ownership {
                        let released: Vec<(SessionId, String)> = registry
                            .resolve_live(t)
                            .iter()
                            .filter_map(|worker| {
                                match crate::worker::revalidate_mandate(
                                    registry.mandate_of(worker),
                                    self.policy_pointer.as_deref(),
                                ) {
                                    crate::worker::MandateRevalidation::Pass => None,
                                    crate::worker::MandateRevalidation::Release {
                                        policy_version,
                                    } => Some((*worker, policy_version)),
                                }
                            })
                            .collect();
                        let live = registry
                            .resolve_live(t)
                            .into_iter()
                            .filter(|worker| {
                                !released.iter().any(|(released, _)| released == worker)
                            })
                            .collect::<std::collections::BTreeSet<_>>();
                        (live, registry.resolve(t), released)
                    } else {
                        let all = registry.resolve(t);
                        (all.clone(), all, Vec::new())
                    }
                });
                match resolution {
                    Some((live, historical, released)) if !historical.is_empty() => {
                        if self.followup_ownership {
                            // the honest release audit — one line
                            // per released worker, naming the stale
                            // version and the closed reason code.
                            for (worker, from_version) in &released {
                                warn!(
                                    target: "kian::subscriptions",
                                    channel = %event.channel,
                                    event_id = %event.event_id,
                                    topic = event.topic.as_deref().unwrap_or(""),
                                    session = %worker.0,
                                    from_policy = %from_version,
                                    reason = %IntakeReason::ReleasedToEval.code(),
                                    "subscription audit: mandate revalidation released live \
                                     subscriber (inferred strength, policy version no longer \
                                     current), follow-up goes to fresh evaluation"
                                );
                            }
                            if live.is_empty() {
                                // Release to fresh evaluation (002's
                                // Ticket-end rule; adds the mandate
                                // path into the same release): every
                                // live subscriber is gone — ended, or
                                // released by revalidation — the
                                // follow-up belongs to a *new* Ticket,
                                // judged by the Router. The audit line
                                // names the cause and carries the
                                // closed reason code.
                                let cause = if released.is_empty() {
                                    "all subscribers ended"
                                } else {
                                    "mandate revalidation released every live subscriber"
                                };
                                warn!(
                                    target: "kian::subscriptions",
                                    channel = %event.channel,
                                    event_id = %event.event_id,
                                    topic = event.topic.as_deref().unwrap_or(""),
                                    reason = %IntakeReason::ReleasedToEval.code(),
                                    "subscription audit: topic released to fresh evaluation \
                                     ({cause}), falling back to router"
                                );
                                vec![self.router_session.load()]
                            } else {
                                live.into_iter().collect()
                            }
                        } else {
                            historical.into_iter().collect()
                        }
                    }
                    _ => {
                        warn!(
                            target: "kian::subscriptions",
                            channel = %event.channel,
                            event_id = %event.event_id,
                            topic = event.topic.as_deref().unwrap_or(""),
                            reason = %IntakeReason::AdmittedRouter.code(),
                            "subscription audit: passthrough topic unbound, falling back to router"
                        );
                        vec![self.router_session.load()]
                    }
                }
            }
        };

        // shape exactly once here, before any inbox row exists.
        // Committed events are never re-read or re-shaped. The
        // untrusted bit is decided by channel config at this same
        // pre-admission point — labeling, never filtering.
        let untrusted = self.untrusted_channels.contains(&event.channel);
        let mut payload = shape_channel_input_marked(&event, &self.shaping, untrusted);
        // (ADR-013): correlation is context only — the derived
        // thread key joins the over-cap Inline `neighbors` block for
        // recall hops. An under-cap wrapper carries the raw verbatim
        // (no neighbors block to stamp); an empty config stamps
        // nothing. Never a routing input.
        if !self.thread_keys.is_empty()
            && let Some(key) = self.thread_keys.derive(&event)
            && let Some(neighbors) = payload
                .pointer_mut("/kian/inline/neighbors")
                .and_then(serde_json::Value::as_object_mut)
        {
            neighbors.insert(
                crate::intake::THREAD_KEY_FIELD.to_string(),
                serde_json::Value::String(key.topic().to_string()),
            );
        }
        // Port (a) — `admit/pre` (PLUGIN-SPEC §4.1): one evaluation at
        // this channel-policy boundary, over the complete target set,
        // after shaping and before the first inbox row. The write loop
        // below starts only after the grid resolves; a `stop` refuses
        // the event for every target (no row, no wake — audit-only,
        // the sender is not told in v1).
        if let Some(bus) = self.hooks.as_ref() {
            let draft = self.admit_draft(&event, &targets, &payload);
            if let kian_plugin::hooks::AdmitVerdict::Stop { plugin, reason } =
                bus.admit(&draft).await
            {
                warn!(
                    target: "kian::subscriptions",
                    channel = %event.channel,
                    event_id = %event.event_id,
                    plugin = %plugin,
                    reason = %reason,
                    targets = targets.len(),
                    "subscription audit: event refused by hook veto (no inbox row for any target)"
                );
                return Ok(RoutingOutcome::RefusedByHook { plugin, reason });
            }
        }
        // Invariant 1 per subscriber: the admit future resolves only
        // on commit, and that subscriber's wake strictly after. The
        // set is sorted, so a fan-out admits in deterministic order.
        let mut admitted: Vec<FanoutAdmission> = Vec::new();
        let mut first_err: Option<InboxWriteError> = None;
        for target in targets {
            match self
                .inbox
                .admit(target, payload.clone(), Some(event.event_id.clone()))
                .await
            {
                Ok(receipt) => {
                    self.inbox.wake(target);
                    // the admit line's code comes from the same
                    // mapping the assembly counters use — audit and
                    // metrics cannot disagree.
                    let reason =
                        routed_reason(target, self.router_session.load(), receipt.deduplicated);
                    info!(
                        target: "kian::subscriptions",
                        session = %target.0,
                        event_id = %event.event_id,
                        deduplicated = receipt.deduplicated,
                        reason = %reason.code(),
                        "subscription audit: event admitted"
                    );
                    admitted.push(FanoutAdmission {
                        session: target,
                        deduplicated: receipt.deduplicated,
                    });
                    // `admit/post` (§4.1 step 4): fires per admitted
                    // target after its row is durable — single-target
                    // by construction, with that target's receipt. The
                    // `seq` field is the floor 0: the bundle's
                    // `InboxWriter` receipt carries no sequence (the
                    // inbox row is not an event), and inventing one
                    // would need a wire D3 forbids this round.
                    if let Some(bus) = self.hooks.as_ref() {
                        let content_bytes = serde_json::to_string(&payload)
                            .map(|s| s.len() as u64)
                            .unwrap_or(0);
                        bus.admit_post(&kian_plugin::hooks::AdmitReceiptView {
                            session: target,
                            event_id: event.event_id.clone(),
                            seq: 0,
                            origin: kian_plugin::hooks::Origin::Channel,
                            deduplicated: receipt.deduplicated,
                            input: kian_plugin::hooks::AdmitPostInput {
                                role: "user".to_string(),
                                content_bytes,
                                spilled: payload.get("spill").is_some(),
                            },
                            frame: Some(kian_plugin::hooks::FrameInfo::new(target)),
                        })
                        .await;
                    }
                }
                Err(e) => {
                    // The audit log names the drop; the subscriber is
                    // left to the adapter's idempotent retry.
                    warn!(
                        target: "kian::subscriptions",
                        session = %target.0,
                        event_id = %event.event_id,
                        error = %e,
                        "subscription audit: admit failed, subscriber left to the retry"
                    );
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        if admitted.len() == 1 {
            let only = admitted.pop().expect("len checked above");
            Ok(RoutingOutcome::Routed {
                session: only.session,
                deduplicated: only.deduplicated,
            })
        } else {
            Ok(RoutingOutcome::FannedOut { admitted })
        }
    }

    /// Builds the §3.2.1 `admit/pre` view for one shaped event: the
    /// complete fan-out set (sorted by session id), the payload every
    /// target would receive, and floor frames (§7.3 — this boundary
    /// holds no worker bindings or per-session seqs).
    fn admit_draft(
        &self,
        event: &ChannelEvent,
        targets: &[SessionId],
        payload: &serde_json::Value,
    ) -> kian_plugin::hooks::AdmitDraft {
        use kian_plugin::hooks::{
            AdmissionInfo, AdmitInput, AdmitTarget, ChannelProvenance, FrameInfo,
        };
        let content =
            serde_json::to_string(payload).expect("serializing a serde_json::Value cannot fail");
        let (content_preview, content_truncated) = kian_plugin::hooks::preview(&content);
        // The over-cap spill shape is `{content, spill}` — the row then
        // carries only the Inline view (§3.2.1 `inline_view`).
        let spilled = payload.get("spill").is_some();
        let mut sorted: Vec<AdmitTarget> = targets
            .iter()
            .map(|&session| AdmitTarget {
                session,
                router: session == self.router_session.load(),
            })
            .collect();
        sorted.sort_by_key(|t| t.session);
        let frames = sorted.iter().map(|t| FrameInfo::new(t.session)).collect();
        kian_plugin::hooks::AdmitDraft {
            origin: kian_plugin::hooks::Origin::Channel,
            targets: sorted,
            input: AdmitInput {
                role: "user".to_string(),
                synthetic: false,
                content_preview,
                content_bytes: content.len() as u64,
                content_sha256: kian_plugin::hooks::sha256_hex(content.as_bytes()),
                content_truncated,
                inline_view: spilled,
            },
            channel: Some(ChannelProvenance {
                name: event.channel.clone(),
                event_id: event.event_id.clone(),
                topic: event.topic.clone(),
            }),
            admission: AdmissionInfo {
                policy: match self.shaping.inline_cap_bytes {
                    Some(cap) => format!("inline_cap_bytes={cap}"),
                    None => "inline_cap_bytes=unbounded".to_string(),
                },
                spilled,
            },
            frames,
        }
    }

    /// Spawns one consumer task per adapter: each subscribes and feeds
    /// events to [`SubscriptionService::handle_event`] until the
    /// adapter's stream ends. Handling errors are logged and the
    /// consumer keeps going — the durable idempotency key means a
    /// retry of the same event can never double-admit.
    pub fn spawn(
        self: &Arc<Self>,
        adapters: Vec<Arc<dyn ChannelAdapter>>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        adapters
            .into_iter()
            .map(|adapter| {
                let service = Arc::clone(self);
                let mut stream = adapter.subscribe();
                tokio::spawn(async move {
                    while let Some(event) = stream.next().await {
                        if let Err(e) = service.handle_event(event).await {
                            warn!(
                                target: "kian::subscriptions",
                                error = %e,
                                "subscription audit: handling failed, event left to the adapter"
                            );
                        }
                    }
                })
            })
            .collect()
    }
}

/// The provenance of an answer, recorded as `control/confirm`'s
/// `requested_by` (CONFIRMATION-MODEL §14d X4): the channel, the
/// adapter-reported sender and the channel event id. The message text
/// itself is never persisted.
fn answer_principal(event: &ChannelEvent) -> String {
    let sender = match event.sender.as_ref() {
        Some(serde_json::Value::String(sender)) => sender.clone(),
        Some(other) => other.to_string(),
        None => "unknown".to_string(),
    };
    format!("{}:{}:{}", event.channel, sender, event.event_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::{RoutingOutcome, SubscriptionService};
    use crate::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
    use crate::registry::PassthroughRegistry;
    use crate::rules::{CorrelateBy, Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet};
    use futures::future::BoxFuture;
    use kian_core::model::SessionId;
    use kian_core::seams::ChannelEvent;
    use kian_orchestrator::WakeOutcome;
    use serde_json::{Value, json};

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    /// Scripted port: records the ordered call log so ordering and
    /// routing are observable without any server.
    struct ScriptedInbox {
        admits: Mutex<Vec<(SessionId, Option<String>)>>,
        wakes: Mutex<Vec<SessionId>>,
        dedup: bool,
        fail: bool,
    }

    impl ScriptedInbox {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                admits: Mutex::new(Vec::new()),
                wakes: Mutex::new(Vec::new()),
                dedup: false,
                fail: false,
            })
        }
    }

    impl InboxWriter for ScriptedInbox {
        fn admit(
            &self,
            session: SessionId,
            _payload: Value,
            idempotency_key: Option<String>,
        ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
            self.admits.lock().unwrap().push((session, idempotency_key));
            let dedup = self.dedup;
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    Err(InboxWriteError::Write("scripted".into()))
                } else {
                    Ok(AdmitReceipt {
                        deduplicated: dedup,
                    })
                }
            })
        }

        fn wake(&self, session: SessionId) -> WakeOutcome {
            self.wakes.lock().unwrap().push(session);
            WakeOutcome::Folded
        }
    }

    fn event(channel: &str, event_id: &str, topic: Option<&str>) -> ChannelEvent {
        ChannelEvent {
            channel: channel.into(),
            event_id: event_id.into(),
            topic: topic.map(str::to_string),
            sender: None,
            group: None,
            payload: json!({"text": "hello"}),
            ts_ms: 1,
        }
    }

    fn rules() -> SubscriptionRuleSet {
        SubscriptionRuleSet::new(vec![
            SubscriptionRule {
                channel: "ci".into(),
                matcher: RuleMatcher::Topic("pr-9".into()),
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            },
            SubscriptionRule {
                channel: "mock-im".into(),
                matcher: RuleMatcher::Always,
                route: Route::Router,
            },
        ])
    }

    #[tokio::test]
    async fn router_rule_admits_to_router_then_wakes_in_order() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let outcome = service
            .handle_event(event("mock-im", "evt-1", None))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
        // Invariant 1, observable: admit completes before wake fires.
        let admits = inbox.admits.lock().unwrap().clone();
        let wakes = inbox.wakes.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(wakes, vec![router]);
        assert_eq!(admits[0].0, router);
        assert_eq!(admits[0].1.as_deref(), Some("evt-1"));
    }

    #[tokio::test]
    async fn passthrough_routes_to_the_registered_worker_not_the_router() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let worker = sid(2);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service.register_topic("pr-9", worker);
        let outcome = service
            .handle_event(event("ci", "evt-2", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: worker,
                deduplicated: false
            }
        );
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(admits[0].0, worker);
        let wakes = inbox.wakes.lock().unwrap().clone();
        assert_eq!(wakes, vec![worker]);
    }

    #[tokio::test]
    async fn unbound_passthrough_topic_falls_back_to_router() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        // The rule matches every event on the channel, but the registry
        // holds no binding for the event's topic — the bootstrap
        // fallback admits to the Router (with an audit line).
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Always,
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        }]);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules,
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let outcome = service
            .handle_event(event("ci", "evt-3", Some("pr-404")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
    }

    #[tokio::test]
    async fn unmatched_event_is_dropped_with_no_admit_and_no_wake() {
        let inbox = ScriptedInbox::new();
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(sid(1)),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let outcome = service
            .handle_event(event("github", "evt-4", None))
            .await
            .unwrap();
        assert_eq!(outcome, RoutingOutcome::DroppedNoRule);
        assert!(inbox.admits.lock().unwrap().is_empty());
        assert!(inbox.wakes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_admit_never_wakes() {
        let inbox = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
            dedup: false,
            fail: true,
        });
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(sid(1)),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let err = service
            .handle_event(event("mock-im", "evt-5", None))
            .await
            .unwrap_err();
        assert!(matches!(err, InboxWriteError::Write(_)));
        assert!(inbox.wakes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn registry_install_replaces_runtime_state() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let worker = sid(9);
        let mut rebuilt = PassthroughRegistry::new();
        rebuilt.register("pr-9", worker);
        service.install_registry(rebuilt);
        // resolve returns the subscriber set (singleton here).
        assert_eq!(
            service.registry_snapshot().resolve("pr-9"),
            std::collections::BTreeSet::from([worker])
        );
        let outcome = service
            .handle_event(event("ci", "evt-6", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: worker,
                deduplicated: false
            }
        );
        assert_eq!(service.router_session(), router);
    }

    /// -only port: per-`(session, event_id)` dedup plus a mutable
    /// single-subscriber failure, leaving the bundle `ScriptedInbox`
    /// fixture unchanged.
    struct FanoutInbox {
        admits: Mutex<Vec<(SessionId, Option<String>)>>,
        wakes: Mutex<Vec<SessionId>>,
        committed: Mutex<std::collections::BTreeSet<(SessionId, Option<String>)>>,
        fail_for: Mutex<Option<SessionId>>,
    }

    impl FanoutInbox {
        fn new() -> Arc<Self> {
            Self::failing(None)
        }

        fn failing(fail_for: Option<SessionId>) -> Arc<Self> {
            Arc::new(Self {
                admits: Mutex::new(Vec::new()),
                wakes: Mutex::new(Vec::new()),
                committed: Mutex::new(std::collections::BTreeSet::new()),
                fail_for: Mutex::new(fail_for),
            })
        }
    }

    impl InboxWriter for FanoutInbox {
        fn admit(
            &self,
            session: SessionId,
            _payload: Value,
            idempotency_key: Option<String>,
        ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
            self.admits
                .lock()
                .unwrap()
                .push((session, idempotency_key.clone()));
            let fail = *self.fail_for.lock().unwrap() == Some(session);
            let deduplicated = if fail {
                false
            } else {
                !self
                    .committed
                    .lock()
                    .unwrap()
                    .insert((session, idempotency_key))
            };
            Box::pin(async move {
                if fail {
                    Err(InboxWriteError::Write("scripted fan-out failure".into()))
                } else {
                    Ok(AdmitReceipt { deduplicated })
                }
            })
        }

        fn wake(&self, session: SessionId) -> WakeOutcome {
            self.wakes.lock().unwrap().push(session);
            WakeOutcome::Folded
        }
    }

    #[tokio::test]
    async fn fanout_admits_one_durable_row_per_subscriber() {
        // one topic, two registered workers — one event admits one
        // row per subscriber (same event id key, per session), each
        // with its own wake, in deterministic set order; a single
        // registered worker keeps the Routed outcome (asserted above).
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let worker_a = sid(20);
        let worker_b = sid(21);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service.register_topic("pr-9", worker_a);
        service.register_topic("pr-9", worker_b);
        let outcome = service
            .handle_event(event("ci", "evt-fan", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::FannedOut {
                admitted: vec![
                    super::FanoutAdmission {
                        session: worker_a,
                        deduplicated: false
                    },
                    super::FanoutAdmission {
                        session: worker_b,
                        deduplicated: false
                    }
                ]
            }
        );
        // One admit + wake per subscriber, key shared, order sorted.
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(
            admits,
            vec![
                (worker_a, Some("evt-fan".into())),
                (worker_b, Some("evt-fan".into())),
            ]
        );
        assert_eq!(*inbox.wakes.lock().unwrap(), vec![worker_a, worker_b]);
    }

    #[tokio::test]
    async fn fanout_partial_failure_still_admits_and_wakes_the_others() {
        // retry semantics: subscriber A's admit fails mid-set — the
        // loop continues, B's row is durable and B wakes, A never
        // wakes, and the error surfaces for the adapter's idempotent
        // retry (which then admits only A's missing row).
        let inbox = FanoutInbox::failing(Some(sid(20)));
        let router = sid(1);
        let worker_a = sid(20);
        let worker_b = sid(21);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service.register_topic("pr-9", worker_a);
        service.register_topic("pr-9", worker_b);
        let err = service
            .handle_event(event("ci", "evt-partial", Some("pr-9")))
            .await
            .unwrap_err();
        assert!(matches!(err, InboxWriteError::Write(_)));
        // Both admits were attempted (A failed, B committed)…
        assert_eq!(inbox.admits.lock().unwrap().len(), 2);
        // …but only B woke.
        assert_eq!(*inbox.wakes.lock().unwrap(), vec![worker_b]);

        // The retry (failure cleared) fills A's gap; B deduplicates.
        *inbox.fail_for.lock().unwrap() = None;
        let outcome = service
            .handle_event(event("ci", "evt-partial", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::FannedOut {
                admitted: vec![
                    super::FanoutAdmission {
                        session: worker_a,
                        deduplicated: false
                    },
                    super::FanoutAdmission {
                        session: worker_b,
                        deduplicated: true
                    }
                ]
            }
        );
        assert_eq!(
            *inbox.wakes.lock().unwrap(),
            vec![worker_b, worker_a, worker_b]
        );
    }

    // -----------------------------------------------------------------
    // follow-up ownership — live-only fan-out + release fallback
    // -----------------------------------------------------------------

    /// A follow-up on a topic with one live subscriber admits to that
    /// worker (and to the worker only — the Router session never sees a
    /// row, so no judge turn can be spent on the event).
    #[tokio::test]
    async fn live_subscriber_owns_the_follow_up_not_the_router() {
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let worker = sid(30);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service.register_topic("pr-9", worker);
        let outcome = service
            .handle_event(event("ci", "evt-live", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: worker,
                deduplicated: false
            }
        );
        // Exactly one durable row, for the worker — the Router was
        // never admitted, never woken.
        assert_eq!(
            inbox.admits.lock().unwrap().clone(),
            vec![(worker, Some("evt-live".into()))]
        );
        assert_eq!(*inbox.wakes.lock().unwrap(), vec![worker]);
    }

    /// All subscribers ended: the topic releases to the Router (fresh
    /// evaluation). A mixed set admits to the live remainder only — an
    /// ended subscriber's terminal session never receives a row.
    #[tokio::test]
    async fn all_ended_topic_releases_to_the_router_mixed_admits_live_only() {
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let ended = sid(31);
        let live = sid(32);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service.register_topic("pr-9", ended);
        service.register_topic("pr-9", live);
        // The first worker's stream ended (its thread/completed landed
        // before this follow-up arrived).
        service.mark_worker_ended(ended);
        let outcome = service
            .handle_event(event("ci", "evt-mixed", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: live,
                deduplicated: false
            }
        );
        assert_eq!(
            inbox.admits.lock().unwrap().clone(),
            vec![(live, Some("evt-mixed".into()))]
        );

        // All ended: the release — the Router is the single target.
        service.mark_worker_ended(live);
        let outcome = service
            .handle_event(event("ci", "evt-release", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.last(), Some(&(router, Some("evt-release".into()))));
        assert_eq!(
            admits.iter().filter(|(s, _)| *s == ended).count(),
            0,
            "the ended subscriber never receives a row"
        );
    }

    /// `superagent.followup_ownership: false` restores the legacy
    /// behavior: every historical subscriber — ended or not — admits.
    #[tokio::test]
    async fn ownership_off_restores_ended_subscriber_admission() {
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let ended = sid(33);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        .with_followup_ownership(false);
        service.register_topic("pr-9", ended);
        service.mark_worker_ended(ended);
        let outcome = service
            .handle_event(event("ci", "evt-off", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: ended,
                deduplicated: false
            }
        );
    }

    /// The shared-registry handle (assembly wiring): the service
    /// and an outside holder observe one projection — a registration
    /// through the service is visible through the Arc, and a startup
    /// `install_registry` through the service reaches the Arc holder.
    #[tokio::test]
    async fn shared_registry_is_one_projection_across_holders() {
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let shared = service.shared_registry();
        service.register_topic("pr-9", sid(34));
        assert!(
            shared
                .read()
                .expect("shared registry lock")
                .live_owner("pr-9")
                .is_some()
        );
        let mut rebuilt = PassthroughRegistry::new();
        rebuilt.register("pr-9", sid(35));
        service.install_registry(rebuilt);
        assert_eq!(
            shared.read().expect("shared registry lock").resolve("pr-9"),
            std::collections::BTreeSet::from([sid(35)])
        );
    }

    // -----------------------------------------------------------------
    // untrusted-input marking end-to-end through the service
    // -----------------------------------------------------------------

    /// -only port: records the admitted payload bytes (the bundle
    /// `ScriptedInbox` fixture discards them and stays untouched).
    struct MarkingInbox {
        payloads: Mutex<Vec<(SessionId, Value)>>,
    }

    impl MarkingInbox {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                payloads: Mutex::new(Vec::new()),
            })
        }
    }

    impl InboxWriter for MarkingInbox {
        fn admit(
            &self,
            session: SessionId,
            payload: Value,
            _idempotency_key: Option<String>,
        ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
            self.payloads.lock().unwrap().push((session, payload));
            Box::pin(async move {
                Ok(AdmitReceipt {
                    deduplicated: false,
                })
            })
        }

        fn wake(&self, _session: SessionId) -> WakeOutcome {
            WakeOutcome::Folded
        }
    }

    /// Table-driven (config): an event from a listed channel admits
    /// with the bit inside `content`; every unlisted channel admits
    /// byte-identical to the legacy wrapper — the field is omitted,
    /// not spelled `false`.
    #[tokio::test]
    async fn untrusted_channels_admit_with_the_bit_and_trusted_ones_do_not() {
        struct Case {
            channel: &'static str,
            listed: bool,
        }
        let cases = [
            Case {
                channel: "lark",
                listed: true,
            },
            Case {
                channel: "webhook-feed",
                listed: true,
            },
            Case {
                channel: "mock-im",
                listed: false,
            },
        ];
        for case in cases {
            let inbox = MarkingInbox::new();
            let mut set = std::collections::BTreeSet::new();
            set.insert("lark".to_string());
            set.insert("webhook-feed".to_string());
            // A router rule for the case's own channel, so every case
            // admits (the marking axis is under test, not routing).
            let case_rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
                channel: case.channel.into(),
                matcher: RuleMatcher::Always,
                route: Route::Router,
            }]);
            let service = SubscriptionService::new(
                crate::binding::ResidentBinding::new(sid(1)),
                case_rules,
                Arc::clone(&inbox) as Arc<dyn InboxWriter>,
            )
            .with_untrusted_channels(set);
            service
                .handle_event(event(case.channel, "evt-mark", None))
                .await
                .unwrap();
            let payloads = inbox.payloads.lock().unwrap().clone();
            assert_eq!(payloads.len(), 1, "{}", case.channel);
            let payload = &payloads[0].1;
            if case.listed {
                assert_eq!(
                    payload["kian"]["untrusted"],
                    json!(true),
                    "{}: listed channels wrap with the bit",
                    case.channel
                );
            } else {
                assert!(
                    payload["kian"].get("untrusted").is_none(),
                    "{}: unlisted channels omit the field",
                    case.channel
                );
                // Byte-identical to the legacy wrapper bytes.
                assert_eq!(
                    serde_json::to_vec(payload).unwrap(),
                    serde_json::to_vec(&crate::provenance::channel_input_payload(&{
                        let mut ev = event(case.channel, "evt-mark", None);
                        ev.channel = case.channel.into();
                        ev
                    }))
                    .unwrap(),
                    "{}: the trusted wrapper bytes are the legacy bytes",
                    case.channel
                );
            }
        }
    }

    /// The default service (no list configured) marks nothing — the
    /// legacy behavior, byte-for-byte.
    #[tokio::test]
    async fn absent_untrusted_config_is_byte_identical_to_legacy() {
        let inbox = MarkingInbox::new();
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(sid(1)),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        service
            .handle_event(event("mock-im", "evt-legacy", None))
            .await
            .unwrap();
        let payloads = inbox.payloads.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1);
        assert!(payloads[0].1["kian"].get("untrusted").is_none());
    }

    /// Marking is labeling, not filtering: a listed channel's event
    /// routes exactly where the same event routes unlisted (same
    /// outcome, same target, same wake) — the only difference is the
    /// bit inside `content`.
    #[tokio::test]
    async fn marking_never_changes_the_drop_or_allow_decision() {
        for listed in [false, true] {
            let inbox = MarkingInbox::new();
            let mut set = std::collections::BTreeSet::new();
            if listed {
                set.insert("mock-im".to_string());
            }
            let service = SubscriptionService::new(
                crate::binding::ResidentBinding::new(sid(1)),
                rules(),
                Arc::clone(&inbox) as Arc<dyn InboxWriter>,
            )
            .with_untrusted_channels(set);
            let outcome = service
                .handle_event(event("mock-im", "evt-same", None))
                .await
                .unwrap();
            assert_eq!(
                outcome,
                RoutingOutcome::Routed {
                    session: sid(1),
                    deduplicated: false
                },
                "listed = {listed}"
            );
            // And an unmatched channel still drops, listed or not.
            let dropped = service
                .handle_event(event("github", "evt-drop", None))
                .await
                .unwrap();
            assert_eq!(dropped, RoutingOutcome::DroppedNoRule, "listed = {listed}");
            assert_eq!(inbox.payloads.lock().unwrap().len(), 1);
        }
    }

    /// Over-cap + listed channel: the Inline view carries the bit and
    /// the spill sidecar stays the exact shape with the raw
    /// verbatim (the marker rides content only).
    #[tokio::test]
    async fn over_cap_untrusted_event_marks_the_inline_view_and_keeps_the_sidecar_verbatim() {
        let inbox = MarkingInbox::new();
        let mut set = std::collections::BTreeSet::new();
        set.insert("mock-im".to_string());
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(sid(1)),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        .with_untrusted_channels(set)
        .with_shaping(crate::provenance::ShapingConfig {
            inline_cap_bytes: Some(4),
            first_chars: 2,
            neighbor_fields: vec![],
            vision: false,
        });
        service
            .handle_event(event("mock-im", "evt-spill", None))
            .await
            .unwrap();
        let payloads = inbox.payloads.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1);
        let payload = &payloads[0].1;
        assert_eq!(payload["content"]["kian"]["untrusted"], json!(true));
        assert_eq!(payload["content"]["kian"]["spill"], json!(true));
        assert_eq!(
            payload["spill"]["raw"],
            json!({"text": "hello"}),
            "the sidecar raw is verbatim"
        );
        assert!(payload["spill"].get("untrusted").is_none());
    }

    // -----------------------------------------------------------------
    // the session route
    // -----------------------------------------------------------------

    /// A `Route::Session` rule admits to the configured fixed session
    /// (not the Router), provenance-wrapped, durable before the wake —
    /// the single-target path the timer's ticks take.
    #[tokio::test]
    async fn session_route_admits_to_the_configured_fixed_session() {
        let inbox = FanoutInbox::new();
        let router = sid(1);
        let watchdog = sid(42);
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "timer".into(),
            matcher: RuleMatcher::Topic("watchdog".into()),
            route: Route::Session(watchdog),
        }]);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules,
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let outcome = service
            .handle_event(event("timer", "tick-1", Some("watchdog")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: watchdog,
                deduplicated: false
            }
        );
        // Durable row keyed by the tick id, then the wake — invariant 1.
        assert_eq!(
            inbox.admits.lock().unwrap().clone(),
            vec![(watchdog, Some("tick-1".into()))]
        );
        assert_eq!(*inbox.wakes.lock().unwrap(), vec![watchdog]);

        // Re-handling the same tick id (a restart's re-emission) leaves
        // exactly one row: the dedupe the timer's per-process counter
        // relies on.
        let again = service
            .handle_event(event("timer", "tick-1", Some("watchdog")))
            .await
            .unwrap();
        assert_eq!(
            again,
            RoutingOutcome::Routed {
                session: watchdog,
                deduplicated: true
            }
        );
        assert_eq!(
            inbox.admits.lock().unwrap().len(),
            2,
            "the admit was attempted (and deduped), never skipped"
        );
        assert_eq!(
            inbox.committed.lock().unwrap().len(),
            1,
            "exactly one durable row exists"
        );
    }

    // -- port (a): the admission boundary (PLUGIN-SPEC §4.1) ---------------

    use futures::FutureExt;
    use kian_plugin::hooks::{
        AdmitVerdict, HandlerId, HookBus, HookId, HookPayload, HookReply, Registry,
    };

    /// A bus over a private registry with one `admit/pre` subscriber
    /// answering `reply` and recording the drafts it evaluated.
    fn admit_bus(
        plugin: &'static str,
        reply: HookReply,
    ) -> (
        HookBus,
        Arc<Mutex<Vec<kian_plugin::hooks::AdmitDraft>>>,
        kian_plugin::Disposer,
    ) {
        let registry = Arc::new(Registry::new());
        let drafts = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&drafts);
        let keep = registry.register(
            HandlerId {
                plugin,
                hook: HookId::AdmitPre,
                grants: kian_plugin::hooks::GrantSet::from_names(&["observe:admit", "veto:admit"])
                    .unwrap(),
                order: 0,
            },
            Arc::new(move |payload: &HookPayload| {
                let seen = Arc::clone(&seen);
                let reply = reply.clone();
                async move {
                    if let HookPayload::AdmitDraft(draft) = payload {
                        seen.lock().unwrap().push(draft.clone());
                    }
                    reply
                }
                .boxed()
            }),
        );
        (HookBus::with_registry(registry), drafts, keep)
    }

    #[tokio::test]
    async fn admit_pre_stop_refuses_the_event_for_every_target() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let worker = sid(2);
        let (bus, drafts, _keep_gate) = admit_bus(
            "gate",
            HookReply::FlowStop {
                reason: "off-hours".to_string(),
            },
        );
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        .with_hook_bus(bus);
        service.register_topic("pr-9", worker);

        let outcome = service
            .handle_event(event("ci", "evt-9", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::RefusedByHook {
                plugin: "gate".to_string(),
                reason: "off-hours".to_string(),
            }
        );
        // No inbox row for ANY target, and no wake (§4.1: the write
        // loop never starts after a stop).
        assert!(inbox.admits.lock().unwrap().is_empty());
        assert!(inbox.wakes.lock().unwrap().is_empty());

        // One evaluation, the complete target set in the view.
        let drafts = drafts.lock().unwrap().clone();
        assert_eq!(drafts.len(), 1, "one evaluation per inbound event");
        assert_eq!(drafts[0].targets.len(), 1);
        assert_eq!(drafts[0].targets[0].session, worker);
        assert!(!drafts[0].targets[0].router);
        assert_eq!(drafts[0].origin.as_str(), "channel");
        assert_eq!(
            drafts[0].channel.as_ref().unwrap().event_id,
            "evt-9",
            "channel provenance rides the view"
        );
    }

    #[tokio::test]
    async fn admit_pre_next_admits_and_admit_post_fires_per_target() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let receipts = Arc::new(Mutex::new(
            Vec::<kian_plugin::hooks::AdmitReceiptView>::new(),
        ));
        let registry = Arc::new(Registry::new());
        let seen = Arc::clone(&receipts);
        let _keep = registry.register(
            HandlerId {
                plugin: "auditor",
                hook: HookId::AdmitPost,
                grants: kian_plugin::hooks::GrantSet::from_names(&["observe:admit"]).unwrap(),
                order: 0,
            },
            Arc::new(move |payload: &HookPayload| {
                let seen = Arc::clone(&seen);
                async move {
                    if let HookPayload::AdmitReceipt(r) = payload {
                        seen.lock().unwrap().push(r.clone());
                    }
                    HookReply::Ack { note: None }
                }
                .boxed()
            }),
        );
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        .with_hook_bus(HookBus::with_registry(registry));

        let outcome = service
            .handle_event(event("mock-im", "evt-1", None))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
        // The row landed (unchanged fan-out behavior on `next`) and the
        // receipt fired once, for that target, after the wake.
        assert_eq!(inbox.admits.lock().unwrap().len(), 1);
        assert_eq!(inbox.wakes.lock().unwrap().len(), 1);
        let receipts = receipts.lock().unwrap().clone();
        assert_eq!(receipts.len(), 1, "admit/post fires per admitted target");
        assert_eq!(receipts[0].session, router);
        assert_eq!(receipts[0].event_id, "evt-1");
        assert_eq!(receipts[0].origin.as_str(), "channel");
    }

    #[tokio::test]
    async fn no_bus_mounted_is_byte_identical_legacy_behavior() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let service = SubscriptionService::new(
            crate::binding::ResidentBinding::new(router),
            rules(),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        );
        let outcome = service
            .handle_event(event("mock-im", "evt-3", None))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
        assert_eq!(inbox.admits.lock().unwrap().len(), 1);
    }

    /// Compile-time shape check: the verdict enum is what the boundary
    /// consumes (keeps `AdmitVerdict` in this module's surface).
    #[test]
    fn admit_verdict_shape_is_next_or_stop() {
        assert_eq!(AdmitVerdict::Next, AdmitVerdict::Next);
        assert_eq!(
            AdmitVerdict::Stop {
                plugin: "g".into(),
                reason: "r".into()
            },
            AdmitVerdict::Stop {
                plugin: "g".into(),
                reason: "r".into()
            }
        );
    }
}
