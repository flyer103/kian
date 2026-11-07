//! Session state machine (DESIGN §2.2 "Session state machine
//! (projection-derived)").
//!
//! Session state is *only ever* the projection of the session's event
//! stream (constraint 1): [`project`] folds a stream through the explicit
//! transition table [`transition`], and any stream the table rejects is a
//! corruption, not a state.
//!
//! Trigger mapping and end-write semantics (refinement, recorded in
//! DESIGN §2.2): `session/created` → create; `drain/claimed` → claim
//! (also takeover: a second claim with no end write in between is the
//! crash-recovery shape, and safety against true double-claims is the
//! store's epoch fence, not the projection); `session/ended{reason}` →
//! end write where `completed`/`cancelled` are terminal and freeze the
//! stream while `interrupted`/`failed` release ownership cleanly and
//! return the session to `idle` (resumable via a new claim; the final end
//! write records the final outcome); `input/admitted` and `thread/started`
//! are state-preserving in `idle`/`running`/`recoverable`; every other
//! kind is drain activity, legal only while `running`. `recoverable` (lease
//! expiry) and `archived` are transitions of the machine with no v0 event
//! trigger.

use crate::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use crate::model::SessionId;

/// The projection-derived session states (DESIGN §2.2). All of them —
/// including `Recoverable` and `Archived`, which no v0 event kind
/// produces — participate in the transition table; the pure event
/// projection [`project`] only ever yields the event-reachable subset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionState {
    /// Created, holding no lease, safely waiting for the next wake.
    Idle,
    /// A drain owns the session (claim issued, lease held).
    Running,
    /// A running drain lost liveness (lease expiry); waiting for a
    /// takeover claim. Entered only from `running`.
    Recoverable,
    /// Terminal: ended with `end_reason=completed`. Frozen.
    Completed,
    /// Terminal: ended with `end_reason=cancelled`. Frozen.
    Cancelled,
    /// Retired by an ops action. Frozen.
    Archived,
}

impl SessionState {
    /// Terminal states freeze the stream: no event may follow them.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Archived)
    }
}

/// One input to the transition table. Most variants are driven by event
/// kinds; `LeaseExpired` (liveness observation, overlaid by the
/// orchestrator from the store's expired-lease query) and `Archived`
/// (ops action) have no v0 event representation — later event kinds may
/// drive them (additions-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransitionTrigger {
    /// `session/created`
    Created,
    /// `drain/claimed` — initial claim, resume, or takeover after crash.
    Claimed,
    /// `session/ended` — the atomic end write.
    Ended(EndReason),
    /// Lease TTL elapsed on a running drain (liveness, not an event).
    LeaseExpired,
    /// Ops retirement (not an event).
    Archived,
    /// `input/admitted` — state-preserving.
    InputAdmitted,
    /// `thread/started` (bundle) — state-preserving in
    /// `idle`/`running`/`recoverable`, mirroring `InputAdmitted`. The
    /// worker's stream records its delegation lineage after
    /// `session/created` but before the first `drain/claimed`; the
    /// Router's stream records it mid-drain.
    ThreadStarted,
    /// Any drain-activity kind (`turn/*`, `step/*`, `assistant/*`,
    /// `tool/*`, `compact`, `slot/*`) — state-preserving, legal only
    /// while running.
    DrainActivity,
}

impl TransitionTrigger {
    /// Maps an event kind to its trigger. Future event kinds (additions
    /// only) default to drain activity.
    #[must_use]
    pub fn from_event_kind(kind: &EventKind) -> Self {
        match kind {
            EventKind::SessionCreated => Self::Created,
            EventKind::DrainClaimed => Self::Claimed,
            EventKind::SessionEnded { reason } => Self::Ended(*reason),
            EventKind::InputAdmitted => Self::InputAdmitted,
            EventKind::ThreadStarted => Self::ThreadStarted,
            // the durable interrupt intent is drain activity — it
            // preserves `running` exactly like every other audit kind
            // the drain appends mid-episode.
            EventKind::ControlInterrupt => Self::DrainActivity,
            _ => Self::DrainActivity,
        }
    }
}

/// A rejected transition — the illegal-transition table's named error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("illegal session-state transition {from:?} --{on:?}--> rejected")]
pub struct IllegalTransition {
    /// `None` = the origin, before `session/created`.
    pub from: Option<SessionState>,
    pub on: TransitionTrigger,
}

/// Named projection errors. Every illegal stream shape is one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ProjectionError {
    /// An empty stream is not a session.
    #[error("projecting an empty event stream is not a session")]
    EmptyStream,
    /// The event violates the transition table.
    #[error(transparent)]
    Illegal(#[from] IllegalTransition),
    /// EndReason first-write-wins: a second `session/ended` in the same
    /// drain episode tried to change the recorded reason.
    #[error(
        "end-reason conflict: first write wins (recorded {recorded:?}, attempted {attempted:?})"
    )]
    EndReasonConflict {
        recorded: EndReason,
        attempted: EndReason,
    },
    /// A second `session/ended` in the same drain episode repeated the
    /// recorded reason.
    #[error("duplicate session end with reason {reason:?} in one drain episode")]
    DuplicateSessionEnd { reason: EndReason },
    /// Sequence numbers must strictly increase.
    #[error("event seq went backwards or repeated (prev {prev}, got {got})")]
    NonMonotoneSeq { prev: u64, got: u64 },
    /// A stream must belong to exactly one session.
    #[error("event belongs to another session (expected {expected:?}, got {got:?})")]
    SessionIdMismatch { expected: SessionId, got: SessionId },
    /// Fail closed on future schema versions (DESIGN §3.3).
    #[error("event schema version {got} is newer than supported {supported}")]
    UnsupportedVersion { got: u32, supported: u32 },
}

/// The explicit transition table of the DESIGN §2.2 state machine.
///
/// `None` is the origin (before `session/created`). Legal edges:
/// origin→idle (create); idle→running (claim); recoverable→running
/// (takeover claim); running→running (takeover claim after a crash —
/// the store's epoch fence guarantees single ownership, the projection
/// tolerates the recovered stream); running→completed/cancelled (terminal
/// end write); running→idle (interrupted/failed end write: ownership
/// released cleanly, session resumable); running→recoverable (lease
/// expiry, non-event); idle/running→archived (ops action, non-event);
/// `input/admitted` and `thread/started` preserve
/// idle/running/recoverable; drain activity preserves running only. Every
/// other pair is [`IllegalTransition`].
pub fn transition(
    from: Option<SessionState>,
    on: TransitionTrigger,
) -> Result<SessionState, IllegalTransition> {
    use EndReason as R;
    use SessionState as S;
    use TransitionTrigger as T;

    match (from, on) {
        (None, T::Created) => Ok(S::Idle),
        (Some(S::Idle), T::Claimed) => Ok(S::Running),
        (Some(S::Idle), T::InputAdmitted | T::ThreadStarted) => Ok(S::Idle),
        (Some(S::Idle), T::Archived) => Ok(S::Archived),
        (Some(S::Recoverable), T::Claimed) => Ok(S::Running),
        (Some(S::Recoverable), T::InputAdmitted | T::ThreadStarted) => Ok(S::Recoverable),
        (Some(S::Running), T::Claimed) => Ok(S::Running),
        (Some(S::Running), T::InputAdmitted | T::ThreadStarted | T::DrainActivity) => {
            Ok(S::Running)
        }
        (Some(S::Running), T::Ended(R::Completed)) => Ok(S::Completed),
        (Some(S::Running), T::Ended(R::Cancelled)) => Ok(S::Cancelled),
        (Some(S::Running), T::Ended(R::Interrupted) | T::Ended(R::Failed)) => Ok(S::Idle),
        (Some(S::Running), T::LeaseExpired) => Ok(S::Recoverable),
        (Some(S::Running), T::Archived) => Ok(S::Archived),
        (from, on) => Err(IllegalTransition { from, on }),
    }
}

/// The pure projection: folds a session's event stream through the
/// transition table. Session state is read *only* here — never from a
/// column or a cached field (constraint 1).
///
/// Envelope checks: one session per stream, strictly increasing `seq`,
/// schema version fail-closed. End-write semantics: within one drain
/// episode (between claims) the first `session/ended` wins; a second is
/// [`ProjectionError::EndReasonConflict`] or
/// [`ProjectionError::DuplicateSessionEnd`]. A new claim clears the
/// episode, so an interrupted session may legally end again with its
/// final outcome after a resume.
pub fn project(events: &[Event]) -> Result<SessionState, ProjectionError> {
    let Some(first) = events.first() else {
        return Err(ProjectionError::EmptyStream);
    };
    let session = first.session;
    let mut state: Option<SessionState> = None;
    let mut prev_seq: Option<u64> = None;
    let mut episode_end: Option<EndReason> = None;

    for ev in events {
        if ev.version > SCHEMA_VERSION {
            return Err(ProjectionError::UnsupportedVersion {
                got: ev.version,
                supported: SCHEMA_VERSION,
            });
        }
        if ev.session != session {
            return Err(ProjectionError::SessionIdMismatch {
                expected: session,
                got: ev.session,
            });
        }
        if let Some(prev) = prev_seq
            && ev.seq <= prev
        {
            return Err(ProjectionError::NonMonotoneSeq { prev, got: ev.seq });
        }

        if let EventKind::SessionEnded { reason } = ev.kind {
            if let Some(recorded) = episode_end {
                return Err(if recorded == reason {
                    ProjectionError::DuplicateSessionEnd { reason }
                } else {
                    ProjectionError::EndReasonConflict {
                        recorded,
                        attempted: reason,
                    }
                });
            }
            episode_end = Some(reason);
        }
        if matches!(
            TransitionTrigger::from_event_kind(&ev.kind),
            TransitionTrigger::Claimed
        ) {
            episode_end = None;
        }

        state = Some(transition(
            state,
            TransitionTrigger::from_event_kind(&ev.kind),
        )?);
        prev_seq = Some(ev.seq);
    }

    // The loop ran at least once, so state is Some.
    state.ok_or(ProjectionError::EmptyStream)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Deterministic fixtures for the state-machine and invariant tests.
    //! A tiny seeded PRNG keeps property tests reproducible without a
    //! dependency.

    use uuid::Uuid;

    use crate::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use crate::model::{Epoch, SessionId};
    use crate::state::SessionState;

    /// xorshift64* — seeded, deterministic, dependency-free.
    pub struct Rng(pub u64);

    impl Rng {
        pub fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        pub fn below(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// Builds a syntactically clean single-session stream: monotone seq,
    /// one session id, epoch bumped on each claim.
    pub struct StreamBuilder {
        session: SessionId,
        seq: u64,
        ts: u64,
        epoch: Epoch,
        pub events: Vec<Event>,
    }

    impl StreamBuilder {
        pub fn new(seed: u64) -> Self {
            Self {
                session: SessionId(Uuid::from_u64_pair(seed, seed ^ 0x9E37_79B9_7F4A_7C15)),
                seq: 0,
                ts: 1_000,
                epoch: Epoch::GENESIS,
                events: Vec::new(),
            }
        }

        pub fn push(&mut self, kind: EventKind) -> &mut Self {
            self.events.push(Event {
                seq: self.seq,
                session: self.session,
                epoch: self.epoch,
                ts: self.ts,
                kind,
                bytes: serde_json::json!({}),
                version: SCHEMA_VERSION,
            });
            self.seq += 1;
            self.ts += 7;
            self
        }

        /// A claim issues the next fencing epoch (ADR-005).
        pub fn claim(&mut self) -> &mut Self {
            self.epoch = self.epoch.next();
            self.push(EventKind::DrainClaimed)
        }

        pub fn build(&self) -> Vec<Event> {
            self.events.clone()
        }
    }

    /// Drain-activity templates that respect the invariant predicates in
    /// `crate::invariants` (request persisted before reply, tool call
    /// logged before result, no turn boundary inside a provider turn).
    const ACTIVITY: [&[EventKind]; 8] = [
        &[
            EventKind::TurnStart,
            EventKind::StepRequest,
            EventKind::AssistantMessage,
            EventKind::TurnEnd,
        ],
        &[
            EventKind::TurnStart,
            EventKind::StepRequest,
            EventKind::ToolCall,
            EventKind::ToolResult,
            EventKind::StepRequest,
            EventKind::AssistantMessage,
            EventKind::TurnEnd,
        ],
        &[EventKind::StepRequest, EventKind::AssistantMessage],
        &[EventKind::Compact],
        &[EventKind::SlotBinding],
        &[EventKind::SlotFallback],
        &[EventKind::SlotMigration],
        &[EventKind::ToolCall, EventKind::ToolResult],
    ];

    const END_REASONS: [EndReason; 4] = [
        EndReason::Completed,
        EndReason::Cancelled,
        EndReason::Interrupted,
        EndReason::Failed,
    ];

    /// Generates a random *legal* stream by rejection-sampling triggers
    /// against the transition table. `LeaseExpired` and `Archived` have no
    /// v0 event representation and are never emitted. Terminal ends stop
    /// the stream (terminal states are frozen).
    pub fn legal_stream(seed: u64, max_events: usize) -> Vec<Event> {
        use crate::state::{TransitionTrigger, transition};

        let mut rng = Rng(seed | 1);
        let mut b = StreamBuilder::new(seed);
        b.push(EventKind::SessionCreated);
        let mut state = SessionState::Idle;
        let mut steps = 0;
        while b.events.len() < max_events && steps < max_events * 4 {
            steps += 1;
            match state {
                SessionState::Idle => match rng.below(4) {
                    0 | 1 => {
                        b.push(EventKind::InputAdmitted);
                    }
                    _ => {
                        b.claim();
                        state = SessionState::Running;
                    }
                },
                SessionState::Running => {
                    match rng.below(4) {
                        0 => {
                            b.push(EventKind::InputAdmitted);
                        }
                        1 => {
                            // takeover claim: previous drain crashed; the
                            // store's fence guarantees single ownership
                            b.claim();
                        }
                        2 => {
                            let t = ACTIVITY[rng.below(ACTIVITY.len())];
                            for kind in t {
                                b.push(*kind);
                            }
                        }
                        _ => {
                            let reason = END_REASONS[rng.below(END_REASONS.len())];
                            b.push(EventKind::SessionEnded { reason });
                            state = transition(Some(state), TransitionTrigger::Ended(reason))
                                .expect("generator only emits legal triggers");
                            if state.is_terminal() {
                                return b.build();
                            }
                        }
                    }
                }
                // Unreachable from events in v0 (lease expiry is a
                // liveness overlay); kept for exhaustiveness.
                SessionState::Recoverable
                | SessionState::Completed
                | SessionState::Cancelled
                | SessionState::Archived => break,
            }
        }
        b.build()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{StreamBuilder, legal_stream};
    use super::{
        EndReason, EventKind, ProjectionError, SessionState, TransitionTrigger, project, transition,
    };
    use crate::context::derive_model_context;
    use crate::events::SCHEMA_VERSION;
    use crate::model::SessionId;

    use SessionState::{Archived, Cancelled, Completed, Idle, Recoverable, Running};
    use TransitionTrigger::{
        Archived as Archive, Claimed, Created, DrainActivity, Ended, InputAdmitted, LeaseExpired,
    };

    /// The additive `recovery/budget-reset` kind is neutral: it maps
    /// to no state transition and contributes no model-context message,
    /// so a stream containing it projects exactly like one without it
    /// (the event is the audit record, never a context mutation —
    /// constraints 1 and 3).
    #[test]
    fn recovery_budget_reset_is_projection_neutral() {
        let mut with = StreamBuilder::new(11);
        with.push(EventKind::SessionCreated);
        with.push(EventKind::InputAdmitted);
        with.claim();
        with.push(EventKind::RecoveryBudgetReset);
        with.push(EventKind::TurnStart);
        with.push(EventKind::SessionEnded {
            reason: EndReason::Completed,
        });
        let mut with = with.build();

        let mut without = StreamBuilder::new(11);
        without.push(EventKind::SessionCreated);
        without.push(EventKind::InputAdmitted);
        without.claim();
        without.push(EventKind::TurnStart);
        without.push(EventKind::SessionEnded {
            reason: EndReason::Completed,
        });
        let mut without = without.build();

        // `derive_model_context` requires shaped input payloads; the
        // builder's blank `{}` works for the state machine only.
        for stream in [&mut with, &mut without] {
            let input = stream
                .iter_mut()
                .find(|ev| matches!(ev.kind, EventKind::InputAdmitted))
                .expect("input event");
            input.bytes = serde_json::json!({"role": "user", "content": "hello"});
        }

        assert_eq!(project(&with), project(&without));
        let ctx_with = derive_model_context(&with).expect("context with reset event");
        let ctx_without = derive_model_context(&without).expect("context without reset event");
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(ctx_with.len(), 1, "exactly the one input message");
    }

    /// The additive `control/interrupt` kind is explicit drain
    /// activity: it maps to the state-preserving `DrainActivity`
    /// trigger (never a transition of its own — the *end write* is what
    /// settles an interrupt, constraint 4), and a stream containing it
    /// mid-episode projects `running` exactly like one without it.
    #[test]
    fn control_interrupt_maps_to_drain_activity_and_preserves_running() {
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::ControlInterrupt),
            TransitionTrigger::DrainActivity
        );

        let mut with = StreamBuilder::new(12);
        with.push(EventKind::SessionCreated);
        with.push(EventKind::InputAdmitted);
        with.claim();
        with.push(EventKind::ControlInterrupt);
        assert_eq!(project(&with.build()).unwrap(), SessionState::Running);

        // Mid-episode the intent is recorded; only the end write
        // settles it (back to Idle via `interrupted`).
        let mut settled = with;
        settled.push(EventKind::SessionEnded {
            reason: EndReason::Interrupted,
        });
        assert_eq!(project(&settled.build()).unwrap(), SessionState::Idle);
    }

    /// The complete legal transition set of the DESIGN §2.2 machine,
    /// including the state-preserving triggers. Every pair not listed
    /// here must be rejected. `None` is the origin (before
    /// `session/created`).
    fn legal_table() -> Vec<(Option<SessionState>, TransitionTrigger, SessionState)> {
        vec![
            (None, Created, Idle),
            (Some(Idle), Claimed, Running),
            (Some(Idle), InputAdmitted, Idle),
            (Some(Idle), Archive, Archived),
            (Some(Recoverable), Claimed, Running),
            (Some(Recoverable), InputAdmitted, Recoverable),
            (Some(Running), Claimed, Running), // takeover after crash
            (Some(Running), InputAdmitted, Running),
            (Some(Running), DrainActivity, Running),
            (Some(Running), Ended(EndReason::Completed), Completed),
            (Some(Running), Ended(EndReason::Cancelled), Cancelled),
            (Some(Running), Ended(EndReason::Interrupted), Idle),
            (Some(Running), Ended(EndReason::Failed), Idle),
            (Some(Running), LeaseExpired, Recoverable),
            (Some(Running), Archive, Archived),
        ]
    }

    /// Every state × every trigger, including the origin.
    fn all_combos() -> Vec<(Option<SessionState>, TransitionTrigger)> {
        let states = [
            None,
            Some(Idle),
            Some(Running),
            Some(Recoverable),
            Some(Completed),
            Some(Cancelled),
            Some(Archived),
        ];
        let triggers = [
            Created,
            Claimed,
            Ended(EndReason::Completed),
            Ended(EndReason::Cancelled),
            Ended(EndReason::Interrupted),
            Ended(EndReason::Failed),
            LeaseExpired,
            Archive,
            InputAdmitted,
            DrainActivity,
        ];
        let mut combos = Vec::new();
        for s in states {
            for t in triggers {
                combos.push((s, t));
            }
        }
        combos
    }

    #[test]
    fn transition_table_legal_set_is_exact() {
        for (from, on, want) in legal_table() {
            assert_eq!(
                transition(from, on),
                Ok(want),
                "legal transition {from:?} --{on:?}--> must project"
            );
        }
    }

    /// Exhaustive illegal-transition table: every pair outside the legal
    /// set is rejected with the named `IllegalTransition` error carrying
    /// the offending from/on. Includes the examples: `idle→
    /// completed`, `recoverable` entered from `idle`, `archived`/terminal
    /// states frozen, work events without a claim, events before create.
    #[test]
    fn transition_table_rejects_every_illegal_pair() {
        // Card examples, asserted explicitly first for clear failures.
        assert_eq!(
            transition(Some(Idle), Ended(EndReason::Completed)),
            Err(super::IllegalTransition {
                from: Some(Idle),
                on: Ended(EndReason::Completed),
            })
        );
        assert_eq!(
            transition(Some(Idle), LeaseExpired),
            Err(super::IllegalTransition {
                from: Some(Idle),
                on: LeaseExpired,
            })
        );
        assert_eq!(
            transition(None, Ended(EndReason::Completed)),
            Err(super::IllegalTransition {
                from: None,
                on: Ended(EndReason::Completed),
            })
        );

        let legal = legal_table();
        for (from, on) in all_combos() {
            let expected = legal
                .iter()
                .find(|(f, o, _)| *f == from && *o == on)
                .map(|(_, _, to)| *to);
            match transition(from, on) {
                Ok(got) => assert_eq!(
                    Some(got),
                    expected,
                    "{from:?} --{on:?}--> must not be legal"
                ),
                Err(err) => {
                    assert!(
                        expected.is_none(),
                        "{from:?} --{on:?}--> must be legal, got {err:?}"
                    );
                    assert_eq!(err.from, from);
                    assert_eq!(err.on, on);
                }
            }
        }
    }

    #[test]
    fn trigger_from_event_kind_covers_all_kinds() {
        use super::TransitionTrigger as T;
        let cases = [
            (EventKind::SessionCreated, T::Created),
            (EventKind::InputAdmitted, T::InputAdmitted),
            (EventKind::DrainClaimed, T::Claimed),
            (EventKind::TurnStart, T::DrainActivity),
            (EventKind::TurnEnd, T::DrainActivity),
            (EventKind::StepRequest, T::DrainActivity),
            (EventKind::AssistantMessage, T::DrainActivity),
            (EventKind::ToolCall, T::DrainActivity),
            (EventKind::ToolResult, T::DrainActivity),
            (EventKind::Compact, T::DrainActivity),
            (EventKind::SlotBinding, T::DrainActivity),
            (EventKind::SlotFallback, T::DrainActivity),
            (EventKind::SlotMigration, T::DrainActivity),
            (
                EventKind::SessionEnded {
                    reason: EndReason::Failed,
                },
                T::Ended(EndReason::Failed),
            ),
        ];
        assert_eq!(cases.len(), 14, "the durable set is exactly 14 kinds");
        for (kind, want) in cases {
            assert_eq!(TransitionTrigger::from_event_kind(&kind), want);
        }
    }

    // ------------------------------------------------------------------
    // bundle kinds: trigger mapping and projection behavior
    // ------------------------------------------------------------------

    /// `thread/started` maps to its own state-preserving trigger; the
    /// other three bundle kinds map to drain activity via the
    /// additions-only default.
    #[test]
    fn trigger_mapping_for_bundle_kinds() {
        use super::TransitionTrigger as T;
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::ThreadStarted),
            T::ThreadStarted
        );
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::ThreadCompleted),
            T::DrainActivity
        );
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::EvidenceVerdict),
            T::DrainActivity
        );
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::ChannelDelivered),
            T::DrainActivity
        );
    }

    /// `thread/started` mirrors `input/admitted` in the transition table
    /// exactly: state-preserving in idle/running/recoverable, rejected at
    /// the origin and from every terminal/frozen state.
    #[test]
    fn thread_started_mirrors_input_admitted_across_the_whole_table() {
        let states = [
            None,
            Some(Idle),
            Some(Running),
            Some(Recoverable),
            Some(Completed),
            Some(Cancelled),
            Some(Archived),
        ];
        for from in states {
            // Compare outcomes, not raw errors: an `IllegalTransition`
            // honestly names the offending trigger, so the error values
            // differ by design while the legality must not.
            let ts = transition(from, TransitionTrigger::ThreadStarted);
            let ia = transition(from, InputAdmitted);
            assert_eq!(
                ts.is_ok(),
                ia.is_ok(),
                "thread/started must mirror input/admitted legality from {from:?}"
            );
            assert_eq!(
                ts.ok(),
                ia.ok(),
                "thread/started must mirror input/admitted from {from:?}"
            );
        }
        // The mirroring rows are the legal ones, spelled out.
        assert_eq!(
            transition(Some(Idle), TransitionTrigger::ThreadStarted),
            Ok(Idle)
        );
        assert_eq!(
            transition(Some(Running), TransitionTrigger::ThreadStarted),
            Ok(Running)
        );
        assert_eq!(
            transition(Some(Recoverable), TransitionTrigger::ThreadStarted),
            Ok(Recoverable)
        );
        // Rejected at the origin (before session/created) and from the
        // terminal states (frozen streams).
        assert!(transition(None, TransitionTrigger::ThreadStarted).is_err());
        assert!(transition(Some(Completed), TransitionTrigger::ThreadStarted).is_err());
        assert!(transition(Some(Cancelled), TransitionTrigger::ThreadStarted).is_err());
        assert!(transition(Some(Archived), TransitionTrigger::ThreadStarted).is_err());
    }

    /// The worker-bootstrap shape: `thread/started` lands after
    /// `session/created` while still `idle` (before the first
    /// `drain/claimed`) and the stream projects.
    #[test]
    fn project_thread_started_legal_in_idle_after_create() {
        let mut b = StreamBuilder::new(21);
        b.push(EventKind::SessionCreated)
            .push(EventKind::InputAdmitted)
            .push(EventKind::ThreadStarted);
        assert_eq!(project(&b.build()), Ok(Idle));

        // It survives an interrupted first episode and the resume.
        let mut b = StreamBuilder::new(22);
        b.push(EventKind::SessionCreated)
            .push(EventKind::ThreadStarted)
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            })
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// The Router shape: `thread/started` appended mid-drain while
    /// `running` projects `running`.
    #[test]
    fn project_thread_started_legal_mid_drain_in_running() {
        let mut b = StreamBuilder::new(23);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::ThreadStarted)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// Terminal states are frozen: `thread/started` after a terminal end
    /// write is an illegal transition.
    #[test]
    fn project_thread_started_rejected_from_terminal() {
        let mut b = StreamBuilder::new(24);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            })
            .push(EventKind::ThreadStarted);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Completed),
                on: TransitionTrigger::ThreadStarted,
            }))
        );
    }

    /// The other three bundle kinds are drain activity by the
    /// additions-only default: no ownership, no drain activity — they are
    /// rejected while `idle` (only legal mid-drain under a claim).
    #[test]
    fn project_bundle_drain_activity_kinds_rejected_while_idle() {
        for kind in [
            EventKind::ThreadCompleted,
            EventKind::EvidenceVerdict,
            EventKind::ChannelDelivered,
        ] {
            let mut b = StreamBuilder::new(25);
            b.push(EventKind::SessionCreated)
                .push(EventKind::InputAdmitted)
                .push(kind);
            assert_eq!(
                project(&b.build()),
                Err(ProjectionError::Illegal(super::IllegalTransition {
                    from: Some(Idle),
                    on: DrainActivity,
                })),
                "kind {:?} must default to drain activity",
                kind
            );
        }
    }

    /// The same three kinds are legal mid-drain (under a claim) and the
    /// worker-end shape completes: `thread/completed` is appended by the
    /// worker's own drain before its end write.
    #[test]
    fn project_bundle_drain_activity_kinds_legal_mid_drain() {
        let mut b = StreamBuilder::new(26);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::EvidenceVerdict)
            .push(EventKind::ThreadCompleted)
            .push(EventKind::ChannelDelivered)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// `input/spilled` maps to drain activity via the additions-only
    /// default — it owns no trigger-table row (no state transition of its
    /// own; it rides the drain's consumption write).
    #[test]
    fn trigger_mapping_for_input_spilled_is_the_additions_only_default() {
        use super::TransitionTrigger as T;
        assert_eq!(
            TransitionTrigger::from_event_kind(&EventKind::InputSpilled),
            T::DrainActivity
        );
    }

    /// The spill shape: `input/spilled` is appended mid-drain by the
    /// consumption write right after its sibling `input/admitted` and
    /// the stream projects through to a terminal end.
    #[test]
    fn project_input_spilled_legal_mid_drain_after_its_sibling_admitted() {
        let mut b = StreamBuilder::new(27);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::InputAdmitted)
            .push(EventKind::InputSpilled)
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// `input/spilled` defaults to drain activity, so it is rejected
    /// while `idle` — the spill is a drain-time write, never a
    /// control-plane inbox write.
    #[test]
    fn project_input_spilled_rejected_while_idle() {
        let mut b = StreamBuilder::new(28);
        b.push(EventKind::SessionCreated)
            .push(EventKind::InputAdmitted)
            .push(EventKind::InputSpilled);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Idle),
                on: DrainActivity,
            }))
        );
    }

    /// Terminal states are frozen: `input/spilled` after a terminal end
    /// write is an illegal transition.
    #[test]
    fn project_input_spilled_rejected_from_terminal() {
        let mut b = StreamBuilder::new(29);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            })
            .push(EventKind::InputSpilled);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Completed),
                on: DrainActivity,
            }))
        );
    }

    #[test]
    fn project_empty_stream_is_an_error() {
        assert_eq!(project(&[]), Err(ProjectionError::EmptyStream));
    }

    #[test]
    fn project_full_lifecycle_to_completed() {
        let mut b = StreamBuilder::new(1);
        b.push(EventKind::SessionCreated)
            .push(EventKind::InputAdmitted)
            .claim()
            .push(EventKind::SlotBinding)
            .push(EventKind::TurnStart)
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::TurnEnd)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// semantics encoded early: an interrupted end is honest and
    /// non-terminal; the session resumes via a new claim and the final
    /// end write records the final outcome.
    #[test]
    fn project_interrupted_end_returns_to_idle_and_resumes_to_completed() {
        let mut b = StreamBuilder::new(2);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::TurnStart)
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            });
        assert_eq!(project(&b.build()), Ok(Idle));

        b.claim()
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    /// A takeover claim with no end write in between is the
    /// crash-recovery shape: the previous drain died mid-flight and a new
    /// drain re-claims. The store's epoch fence guarantees single
    /// ownership; the projection must not reject the recovered stream.
    #[test]
    fn project_accepts_takeover_claim_after_crash() {
        let mut b = StreamBuilder::new(3);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::StepRequest)
            .claim() // crash + takeover, no end write
            .push(EventKind::StepRequest)
            .push(EventKind::AssistantMessage)
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            });
        assert_eq!(project(&b.build()), Ok(Completed));
    }

    #[test]
    fn project_requires_create_first() {
        let mut b = StreamBuilder::new(4);
        b.push(EventKind::TurnStart);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: None,
                on: DrainActivity,
            }))
        );
    }

    #[test]
    fn project_rejects_duplicate_create() {
        let mut b = StreamBuilder::new(5);
        b.push(EventKind::SessionCreated)
            .push(EventKind::SessionCreated);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Idle),
                on: Created,
            }))
        );
    }

    /// EndReason first-write-wins within one drain episode: a second
    /// `session/ended` changing the reason is a named error.
    #[test]
    fn project_rejects_second_end_changing_reason() {
        let mut b = StreamBuilder::new(6);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Completed,
            })
            .push(EventKind::SessionEnded {
                reason: EndReason::Cancelled,
            });
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::EndReasonConflict {
                recorded: EndReason::Completed,
                attempted: EndReason::Cancelled,
            })
        );
    }

    #[test]
    fn project_rejects_duplicate_end_same_reason() {
        let mut b = StreamBuilder::new(7);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            })
            .push(EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            });
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::DuplicateSessionEnd {
                reason: EndReason::Interrupted,
            })
        );
    }

    /// Work events without a claim: no ownership, no drain activity.
    #[test]
    fn project_rejects_work_without_claim() {
        let mut b = StreamBuilder::new(8);
        b.push(EventKind::SessionCreated)
            .push(EventKind::InputAdmitted)
            .push(EventKind::TurnStart);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Idle),
                on: DrainActivity,
            }))
        );
    }

    /// Terminal states are frozen: nothing may follow a terminal end
    /// write, not even an admit.
    #[test]
    fn project_rejects_events_after_terminal_state() {
        let mut b = StreamBuilder::new(9);
        b.push(EventKind::SessionCreated)
            .claim()
            .push(EventKind::SessionEnded {
                reason: EndReason::Cancelled,
            })
            .push(EventKind::InputAdmitted);
        assert_eq!(
            project(&b.build()),
            Err(ProjectionError::Illegal(super::IllegalTransition {
                from: Some(Cancelled),
                on: InputAdmitted,
            }))
        );
    }

    #[test]
    fn project_rejects_non_monotone_seq() {
        let mut b = StreamBuilder::new(10);
        b.push(EventKind::SessionCreated).claim();
        let mut events = b.build();
        let mut replay = events.pop().unwrap(); // seq 1
        replay.seq = 0; // clash with seq 0
        events.push(replay);
        assert_eq!(
            project(&events),
            Err(ProjectionError::NonMonotoneSeq { prev: 0, got: 0 })
        );
    }

    #[test]
    fn project_rejects_foreign_session_event() {
        let mut b = StreamBuilder::new(11);
        b.push(EventKind::SessionCreated);
        let mut events = b.build();
        let mut stray = events[0].clone();
        stray.seq = 1;
        stray.kind = EventKind::InputAdmitted;
        stray.session = SessionId::new();
        let expected = stray.session;
        events.push(stray);
        assert_eq!(
            project(&events),
            Err(ProjectionError::SessionIdMismatch {
                expected: events[0].session,
                got: expected,
            })
        );
    }

    #[test]
    fn project_rejects_future_schema_version() {
        let mut b = StreamBuilder::new(13);
        b.push(EventKind::SessionCreated);
        let mut events = b.build();
        let mut stray = events[0].clone();
        stray.seq = 1;
        stray.kind = EventKind::InputAdmitted;
        stray.version = SCHEMA_VERSION + 1;
        events.push(stray);
        assert_eq!(
            project(&events),
            Err(ProjectionError::UnsupportedVersion {
                got: SCHEMA_VERSION + 1,
                supported: SCHEMA_VERSION,
            })
        );
    }

    /// Property test: for any prefix of a legal stream the projected
    /// state is itself legal, and no half-terminated state is observable
    /// — the projection is terminal *exactly when* the last event is a
    /// terminal end write, by construction.
    #[test]
    fn every_prefix_of_a_legal_stream_projects_to_a_legal_state() {
        for seed in 1..=40u64 {
            let events = legal_stream(seed, 60);
            assert!(events.len() >= 2, "seed {seed} produced a trivial stream");
            for n in 1..=events.len() {
                let prefix = &events[..n];
                let state =
                    project(prefix).unwrap_or_else(|e| panic!("seed {seed} prefix {n}: {e:?}"));
                let last = &prefix.last().expect("n >= 1").kind;
                let terminal_end = matches!(
                    last,
                    EventKind::SessionEnded {
                        reason: EndReason::Completed | EndReason::Cancelled
                    }
                );
                assert_eq!(
                    state.is_terminal(),
                    terminal_end,
                    "seed {seed} prefix {n}: state {state:?} vs last {last:?}"
                );
            }
        }
    }

    /// Sanity for the fixture itself: generated streams always project
    /// and never invent sessions out of nothing.
    #[test]
    fn generated_streams_always_project() {
        for seed in 1..=40u64 {
            let events = legal_stream(seed, 60);
            assert!(project(&events).is_ok(), "seed {seed}");
            assert_eq!(events[0].kind, EventKind::SessionCreated);
            let session: std::collections::HashSet<_> = events.iter().map(|e| e.session).collect();
            assert_eq!(session.len(), 1);
            let sid = SessionId(events[0].session.0);
            assert_eq!(events[0].session, sid);
        }
    }
}
