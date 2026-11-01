//! The six semantic invariants (DESIGN §2.3) plus NoHalfTerminated, as
//! checkable predicates over event streams/projections, and the resident
//! assertion-macro family for `debug_assert!`s and test assertions.
//!
//! Each predicate checks the invariant's **event-stream-visible**
//! consequence; the store- and orchestrator-owned halves (CAS fencing,
//! inbox rebuild, lease TTL) are verified in their own crates (C5/C6/C9).
//! Predicates are linear scans — safe to keep resident as debug_asserts.

use crate::events::{EndReason, Event, EventKind};
use crate::model::Epoch;
use crate::state::{ProjectionError, SessionState, project};

/// A violated invariant, named per predicate. Resident predicates return
/// these so `debug_assert!`s and test failures name the exact invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvariantViolation {
    /// The stream does not project at all; no invariant can be stated
    /// over it.
    #[error(transparent)]
    NotProjectable(#[from] ProjectionError),
    /// Invariant 1: a reply arrived without its persisted request, or a
    /// tool result without its logged call, at event index `at`.
    #[error("invariant 1 (durable before execute) violated at event {at}: {what}")]
    DurableBeforeExecute { at: usize, what: &'static str },
    /// Invariant 2: a later snapshot is not an append-only extension of
    /// the earlier one — work was lost.
    #[error("invariant 2 (no lost work) violated: streams diverge at index {at}")]
    EventsNotAppendOnly { at: usize },
    /// Invariant 3: a turn boundary landed inside an in-flight provider
    /// turn (between `step/request` and its `assistant/message`).
    #[error("invariant 3 (promote only at safe boundaries) violated at event {at}")]
    PromoteInsideProviderTurn { at: usize },
    /// Invariant 4: rewriting the fencing epochs changed the projected
    /// state — state must depend on event kinds, never on drain identity.
    #[error(
        "invariant 4 (drain has no durable identity) violated: {with_epochs:?} vs {without_epochs:?}"
    )]
    DrainIdentityRelevant {
        with_epochs: SessionState,
        without_epochs: SessionState,
    },
    /// Invariant 5: an interrupted/failed end write arrived while no
    /// drain owned the session.
    #[error("invariant 5 (interrupt targets the active chain) violated at event {at}")]
    InterruptFromInactiveChain { at: usize },
    /// Invariant 6: drain activity after an aborted end without a new
    /// claim taking ownership.
    #[error("invariant 6 (settle on abort) violated at event {at}: work without a new claim")]
    WorkWithoutReclaim { at: usize },
    /// NoHalfTerminated: a terminal projection must be produced by
    /// exactly one terminal `session/ended` event, with nothing after it.
    #[error(
        "NoHalfTerminated violated: terminal projection produced by {terminal_end_writes} terminal end writes"
    )]
    TerminalNotProducedByExactlyOneEnd { terminal_end_writes: usize },
}

/// **NoHalfTerminated** (constraint 4): a terminal projection is produced
/// by exactly one `session/ended` event — the last event of the stream —
/// and no event follows it. By construction (the atomic end write), the
/// projection is terminal *exactly when* the last event is a terminal
/// end write; this predicate keeps that checkable.
pub fn no_half_terminated(events: &[Event]) -> Result<(), InvariantViolation> {
    let state = project(events)?;
    let terminal_ends = events
        .iter()
        .filter(|e| {
            matches!(
                e.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Completed | EndReason::Cancelled
                }
            )
        })
        .count();
    let ok = match state {
        SessionState::Completed | SessionState::Cancelled => {
            terminal_ends == 1
                && events.last().is_some_and(|e| {
                    matches!(
                        e.kind,
                        EventKind::SessionEnded {
                            reason: EndReason::Completed | EndReason::Cancelled
                        }
                    )
                })
        }
        _ => terminal_ends == 0,
    };
    if ok {
        Ok(())
    } else {
        Err(InvariantViolation::TerminalNotProducedByExactlyOneEnd {
            terminal_end_writes: terminal_ends,
        })
    }
}

/// Invariant 1 (**durable before execute**): every `assistant/message`
/// replies to a persisted `step/request` (persist-what-you-send, A2), and
/// every `tool/result` answers a logged `tool/call`. Positional matching
/// over opaque payloads; resolution (a call with no result yet) is not a
/// violation — the stream may be mid-flight or crashed.
pub fn durable_before_execute(events: &[Event]) -> Result<(), InvariantViolation> {
    let mut open_request = false;
    let mut pending_calls: u32 = 0;
    for (at, ev) in events.iter().enumerate() {
        match ev.kind {
            EventKind::StepRequest => open_request = true,
            EventKind::AssistantMessage => {
                if !open_request {
                    return Err(InvariantViolation::DurableBeforeExecute {
                        at,
                        what: "assistant/message without a preceding step/request",
                    });
                }
                open_request = false;
            }
            EventKind::ToolCall => pending_calls += 1,
            EventKind::ToolResult => {
                if pending_calls == 0 {
                    return Err(InvariantViolation::DurableBeforeExecute {
                        at,
                        what: "tool/result without a preceding tool/call",
                    });
                }
                pending_calls -= 1;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Invariant 2 (**wake is advisory / NoLostWork**), event-stream side:
/// history is append-only, so a restart snapshot (`after`) must extend
/// the pre-crash snapshot (`before`) as a prefix. The inbox-rebuild half
/// is owned by the store/orchestrator (C6/C9).
pub fn no_lost_work(before: &[Event], after: &[Event]) -> Result<(), InvariantViolation> {
    for (at, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        if b != a {
            return Err(InvariantViolation::EventsNotAppendOnly { at });
        }
    }
    if before.len() > after.len() {
        return Err(InvariantViolation::EventsNotAppendOnly { at: after.len() });
    }
    Ok(())
}

/// Invariant 3 (**promote only at safe boundaries**): no `turn/start` or
/// `turn/end` may land inside an in-flight provider turn — the window
/// opened by `step/request` and closed by its `assistant/message`. An
/// ownership change (`drain/claimed`, takeover after a crash) or a
/// session end legitimately closes an abandoned window; slot fallbacks,
/// compaction and tool traffic are mid-turn activity by design.
pub fn promote_at_safe_boundaries(events: &[Event]) -> Result<(), InvariantViolation> {
    let mut in_provider_turn = false;
    for (at, ev) in events.iter().enumerate() {
        match ev.kind {
            EventKind::StepRequest => in_provider_turn = true,
            EventKind::AssistantMessage
            | EventKind::DrainClaimed
            | EventKind::SessionEnded { .. } => in_provider_turn = false,
            EventKind::TurnStart | EventKind::TurnEnd if in_provider_turn => {
                return Err(InvariantViolation::PromoteInsideProviderTurn { at });
            }
            _ => {}
        }
    }
    Ok(())
}

/// Invariant 4 (**drain has no durable identity**): the projection must
/// be a function of event kinds alone — rewriting every fencing epoch to
/// arbitrary values must not change the projected state.
pub fn drain_has_no_durable_identity(events: &[Event]) -> Result<(), InvariantViolation> {
    let base = project(events)?;
    let rewritten: Vec<Event> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| {
            let mut e = ev.clone();
            // arbitrary, deliberately non-monotone values
            e.epoch = Epoch((i as u64 * 2_654_435_761) % 97 + 1);
            e
        })
        .collect();
    let other = project(&rewritten)?;
    if base == other {
        Ok(())
    } else {
        Err(InvariantViolation::DrainIdentityRelevant {
            with_epochs: base,
            without_epochs: other,
        })
    }
}

/// Invariant 5 (**interrupt targets only the active ownership chain**):
/// an `interrupted`/`failed` end write may only arrive while a drain
/// holds the session (since the last claim). Honesty (`interrupted ≠
/// failed`) is carried structurally by [`EndReason`].
pub fn interrupt_targets_active_chain(events: &[Event]) -> Result<(), InvariantViolation> {
    let mut in_claim = false;
    for (at, ev) in events.iter().enumerate() {
        match ev.kind {
            EventKind::DrainClaimed => in_claim = true,
            EventKind::SessionEnded { reason } => {
                if matches!(reason, EndReason::Interrupted | EndReason::Failed) && !in_claim {
                    return Err(InvariantViolation::InterruptFromInactiveChain { at });
                }
                in_claim = false;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Invariant 6 (**settle on abort**): after an `interrupted`/`failed`
/// end, no drain activity may appear until a new claim has taken
/// ownership. `input/admitted` is a control-plane inbox write, not work,
/// and stays legal in an idle (aborted) session.
pub fn settle_on_abort(events: &[Event]) -> Result<(), InvariantViolation> {
    use crate::state::TransitionTrigger;
    let mut aborted = false;
    for (at, ev) in events.iter().enumerate() {
        match TransitionTrigger::from_event_kind(&ev.kind) {
            TransitionTrigger::Claimed => aborted = false,
            TransitionTrigger::Ended(reason) => {
                aborted = matches!(reason, EndReason::Interrupted | EndReason::Failed);
            }
            TransitionTrigger::DrainActivity if aborted => {
                return Err(InvariantViolation::WorkWithoutReclaim { at });
            }
            _ => {}
        }
    }
    Ok(())
}

/// Runs the full resident assertion family over one event stream:
/// projection legality, NoHalfTerminated and every single-stream
/// invariant predicate. Panics with the violated invariant's name — for
/// tests and for hard assertions in debug tooling.
#[macro_export]
macro_rules! assert_session_invariants {
    ($events:expr $(,)?) => {{
        let events: &[$crate::events::Event] = $events;
        $crate::invariants::no_half_terminated(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
        $crate::invariants::durable_before_execute(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
        $crate::invariants::promote_at_safe_boundaries(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
        $crate::invariants::drain_has_no_durable_identity(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
        $crate::invariants::interrupt_targets_active_chain(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
        $crate::invariants::settle_on_abort(events)
            .unwrap_or_else(|v| panic!("session invariants: {v}"));
    }};
}

/// [`assert_session_invariants!`] gated to debug builds — the resident
/// `debug_assert!` form intended for drain/store code paths.
#[macro_export]
macro_rules! debug_assert_session_invariants {
    ($events:expr $(,)?) => {
        if cfg!(debug_assertions) {
            $crate::assert_session_invariants!($events);
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::events::{EndReason, EventKind};
    use crate::state::test_support::{StreamBuilder, legal_stream};

    use super::{
        InvariantViolation, drain_has_no_durable_identity, durable_before_execute,
        interrupt_targets_active_chain, no_half_terminated, no_lost_work,
        promote_at_safe_boundaries, settle_on_abort,
    };

    #[test]
    fn all_predicates_hold_on_generated_legal_streams_and_prefixes() {
        for seed in 1..=40u64 {
            let events = legal_stream(seed, 60);
            for n in 1..=events.len() {
                let prefix = &events[..n];
                no_half_terminated(prefix)
                    .unwrap_or_else(|v| panic!("seed {seed} prefix {n} no_half_terminated: {v}"));
                durable_before_execute(prefix)
                    .unwrap_or_else(|v| panic!("seed {seed} prefix {n}: {v}"));
                promote_at_safe_boundaries(prefix)
                    .unwrap_or_else(|v| panic!("seed {seed} prefix {n}: {v}"));
                drain_has_no_durable_identity(prefix)
                    .unwrap_or_else(|v| panic!("seed {seed} prefix {n}: {v}"));
                interrupt_targets_active_chain(prefix)
                    .unwrap_or_else(|v| panic!("seed {seed} prefix {n}: {v}"));
                settle_on_abort(prefix).unwrap_or_else(|v| panic!("seed {seed} prefix {n}: {v}"));
            }
        }
    }

    #[test]
    fn no_half_terminated_accepts_terminal_stream_with_single_end() {
        let mut b = StreamBuilder::new(101);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(no_half_terminated(&b.build()), Ok(()));
    }

    /// A stream with a second terminal end write cannot project cleanly;
    /// the predicate must surface a violation either way — that is the
    /// point of keeping it resident.
    #[test]
    fn no_half_terminated_rejects_second_terminal_end() {
        let mut b = StreamBuilder::new(102);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            })
            .push(EventKind::SessionEnded {
                reason: EndReason::Cancelled,
            });
        assert!(no_half_terminated(&b.build()).is_err());
    }

    #[test]
    fn durable_before_execute_flags_reply_without_persisted_request() {
        let mut b = StreamBuilder::new(103);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::AssistantMessage); // reply with no step/request
        assert_eq!(
            durable_before_execute(&b.build()),
            Err(InvariantViolation::DurableBeforeExecute {
                at: 2,
                what: "assistant/message without a preceding step/request",
            })
        );
    }

    #[test]
    fn durable_before_execute_flags_orphan_tool_result() {
        let mut b = StreamBuilder::new(104);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::ToolResult); // result with no logged call
        assert_eq!(
            durable_before_execute(&b.build()),
            Err(InvariantViolation::DurableBeforeExecute {
                at: 2,
                what: "tool/result without a preceding tool/call",
            })
        );
    }

    #[test]
    fn no_lost_work_accepts_prefix_growth_and_flags_dropped_event() {
        let events = legal_stream(105, 40);
        let before = events[..events.len() / 2].to_vec();
        assert_eq!(no_lost_work(&before, &events), Ok(()));

        // a restart that "loses" a middle event is a corruption
        let mut holed = events.clone();
        holed.remove(2);
        assert_eq!(
            no_lost_work(&events, &holed),
            Err(InvariantViolation::EventsNotAppendOnly { at: 2 })
        );
    }

    /// The stream is projection-legal here (turn/start is state-preserving
    /// in running) — only the invariant predicate catches the promote
    /// violation. This is why the predicates exist beyond the table.
    #[test]
    fn promote_at_safe_boundaries_flags_turn_start_inside_provider_turn() {
        let mut b = StreamBuilder::new(106);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::TurnStart) // inside the provider-turn window
            .push(EventKind::AssistantMessage);
        assert_eq!(
            promote_at_safe_boundaries(&b.build()),
            Err(InvariantViolation::PromoteInsideProviderTurn { at: 3 })
        );
    }

    /// After a crash mid-provider-turn, a takeover claim legitimately
    /// appears inside the abandoned window: ownership change closes the
    /// window; only turn boundaries inside a live window are violations.
    #[test]
    fn promote_at_safe_boundaries_accepts_takeover_closing_the_window() {
        let mut b = StreamBuilder::new(107);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .claim() // crash + takeover
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage);
        assert_eq!(promote_at_safe_boundaries(&b.build()), Ok(()));
    }

    #[test]
    fn drain_identity_is_irrelevant_under_epoch_rewrites() {
        let events = legal_stream(108, 50);
        assert_eq!(drain_has_no_durable_identity(&events), Ok(()));
    }

    #[test]
    fn interrupt_targets_active_chain_flags_end_without_claim() {
        let mut b = StreamBuilder::new(109);
        b.push(EventKind::SessionCreated)
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            });
        assert_eq!(
            interrupt_targets_active_chain(&b.build()),
            Err(InvariantViolation::InterruptFromInactiveChain { at: 1 })
        );
    }

    #[test]
    fn settle_on_abort_flags_work_before_reclaim() {
        let mut b = StreamBuilder::new(110);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            })
            .push(EventKind::TurnStart); // work without a new claim
        assert_eq!(
            settle_on_abort(&b.build()),
            Err(InvariantViolation::WorkWithoutReclaim { at: 3 })
        );
    }

    #[test]
    fn assert_session_invariants_macro_passes_on_legal_stream() {
        let events = legal_stream(111, 40);
        crate::assert_session_invariants!(&events);
    }

    #[test]
    #[should_panic(expected = "durable before execute")]
    fn assert_session_invariants_macro_panics_on_violation() {
        let mut b = StreamBuilder::new(112);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::ToolResult);
        crate::assert_session_invariants!(&b.build());
    }

    #[test]
    fn debug_assert_macro_compiles_and_is_silent() {
        // Runs the full family under debug builds; nothing to observe on
        // a legal stream. (Under release the call compiles away.)
        let events = legal_stream(113, 30);
        crate::debug_assert_session_invariants!(&events);
        crate::assert_session_invariants!(&events);
    }
}
