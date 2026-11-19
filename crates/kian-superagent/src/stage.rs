//! Ticket stage projection: "what is it waiting for".
//!
//! A kian **Ticket v1 = one delegated worker session** (spawned by
//! `spawn_worker`, named by that thread's `thread/started`). Its stage
//! answers the operator's first question about any open ticket — *what
//! is it waiting for?* — and is a **pure derived projection** (the
//! prototype's 003 first-match table mapped onto kian facts):
//!
//! | # | Stage | Condition (first match wins) |
//! |---|------------------|----------------------------------------------------------------------------------------------|
//! | 1 | `delivery-retry` | the outbox view reports a pending/parked delivery for the thread (wires the real view) |
//! | 1b | `awaiting-confirmation` | the worker stream has an unanswered `ask` (a durable `tool/call` with `permission.evaluated:"ask"` and no result) |
//! | 2 | `awaiting-answer`| latest structured report state `waiting`, the thread still outstanding, no input since |
//! | 3 | `awaiting-review`| latest structured report state `ready` (result delivered, human review pending) |
//! | 4 | `blocked-failed` | `thread/completed{failed}` or `session/ended{failed}`, and no input admitted since |
//! | 5 | `queued` | input admitted (pending inbox row, or no `drain/claimed` after the last input) |
//! | 6 | `idle` | thread completed successfully / session terminal-completed |
//! | 7 | `working` | everything else |
//!
//! Structured facts only (001's rule: "waiting on a person" is never
//! inferred from free text — the Executor asks through a structured
//! field). The structured report state rides the additive `"state"` key
//! on completion-report content ([`crate::worker::ReportState`]);
//! legacy reports carry no key and never satisfy arms 2–3.
//!
//! A stage is a *disposable projection* (constraint 1):
//! [`project_stage`] reads only the worker's own stream plus two
//! read-side inputs (pending inbox rows, an outbox view). Persisting a
//! stage would make it authoritative — rejected (§2 of the
//! plan). Drop the projection and replay the stream and the same stage
//! comes back (replay-stable by construction).
//!
//! v1 deviation, recorded honestly (DESIGN §4.7): an `idle` ticket is
//! resumable only via fresh Eval (a new `thread/started`), not
//! same-Mandate resume — the prototype's person-resume rule is future
//! work ('s release path).
//!
//! "No input admitted since" (arms 2 and 4) is the release rule: the
//! delegation wrapper is always the stream's *first* input, so every
//! `input/admitted` that appears after a waiting report or a failure is
//! a person's (or fan-out's) message — it releases the ticket toward
//! `queued`/`working`, exactly the 003 semantics.

use kian_core::events::{Event, EventKind};
use uuid::Uuid;

use crate::worker::{ReportState, parse_delegation, parse_thread_report};

/// The closed stage vocabulary. Serde names are stable (wire/JSON
/// contract for `kian tickets --json`); the display text is the same
/// hyphenated name so audit lines, the CLI table, and JSON never
/// disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TicketStage {
    /// A channel delivery for this ticket is pending/parked behind the
    /// outbox retry driver (person/delivery). Unreachable until
    /// wires a real [`OutboxView`]; the stub is empty.
    #[serde(rename = "delivery-retry")]
    DeliveryRetry,
    /// The worker asked for a person's approval (`ask`) and the call is
    /// still unanswered (person/confirmation). The answer is a durable
    /// control row on the same session; the ticket does not end.
    #[serde(rename = "awaiting-confirmation")]
    AwaitingConfirmation,
    /// The worker asked a question (report state `waiting`) and the
    /// thread is still outstanding (person/answer).
    #[serde(rename = "awaiting-answer")]
    AwaitingAnswer,
    /// The worker delivered a result for human review (report state
    /// `ready`) (person/review).
    #[serde(rename = "awaiting-review")]
    AwaitingReview,
    /// The thread failed (`thread/completed{failed}` or
    /// `session/ended{failed}`) and no message has released it
    /// (person/blocked·failed).
    #[serde(rename = "blocked-failed")]
    BlockedFailed,
    /// Input is admitted but no drain has claimed it since
    /// (working/queued).
    #[serde(rename = "queued")]
    Queued,
    /// The thread completed successfully / the session is
    /// terminal-completed — v1 resumable only via fresh Eval.
    #[serde(rename = "idle")]
    Idle,
    /// Everything else: the worker is making progress.
    #[serde(rename = "working")]
    Working,
}

impl TicketStage {
    /// The stable hyphenated name (equal to the serde name).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeliveryRetry => "delivery-retry",
            Self::AwaitingConfirmation => "awaiting-confirmation",
            Self::AwaitingAnswer => "awaiting-answer",
            Self::AwaitingReview => "awaiting-review",
            Self::BlockedFailed => "blocked-failed",
            Self::Queued => "queued",
            Self::Idle => "idle",
            Self::Working => "working",
        }
    }

    /// The inverse of [`as_str`](Self::as_str): the stage for its stable
    /// name, `None` for anything else (a stored name that is not a
    /// stage is a doubt, never a guess).
    #[must_use]
    pub fn from_str_name(name: &str) -> Option<Self> {
        match name {
            "delivery-retry" => Some(Self::DeliveryRetry),
            "awaiting-confirmation" => Some(Self::AwaitingConfirmation),
            "awaiting-answer" => Some(Self::AwaitingAnswer),
            "awaiting-review" => Some(Self::AwaitingReview),
            "blocked-failed" => Some(Self::BlockedFailed),
            "queued" => Some(Self::Queued),
            "idle" => Some(Self::Idle),
            "working" => Some(Self::Working),
            _ => None,
        }
    }
}

impl std::fmt::Display for TicketStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outbox read-side input (arm 1): whether the delivery retry
/// domain reports a pending/parked delivery for this ticket.
///
/// The default is the **stub**: no pending delivery. Until wires a
/// real implementation over the `tool/call` ↔ `channel/delivered`
/// projection, the `delivery-retry` arm is honestly unreachable — the
/// enum is complete from day one, only the input is empty.
pub trait OutboxView {
    /// Whether the outbox holds a pending or parked delivery for
    /// `thread_id`'s ticket.
    fn has_pending_delivery(&self, thread_id: Uuid) -> bool {
        let _ = thread_id;
        false
    }
}

/// The canonical empty [`OutboxView`] — the stub every caller passes
/// until lands.
pub struct NoOutbox;

impl OutboxView for NoOutbox {}

/// Projects one worker session's stage from its own stream plus the two
/// read-side inputs. Pure: no I/O, no ports, no clock — `pending_inbox`
/// and the outbox view are computed by the caller (assembly reads the
/// inbox table and passes the count; `kian tickets` does exactly this).
///
/// `events` is the worker session's committed stream in seq order (the
/// store's `scan`/`rebuild` order). A non-worker stream (no
/// `thread/started`, no delegation wrapper) projects `working` — the
/// projection never guesses a ticket where none was spawned.
#[must_use]
pub fn project_stage(
    events: &[Event],
    pending_inbox: usize,
    outbox: &dyn OutboxView,
) -> TicketStage {
    let facts = TicketFacts::derive(events);

    // Arm 1 — delivery-retry: the outbox view is the sole authority.
    if let Some(thread_id) = facts.thread_id
        && outbox.has_pending_delivery(thread_id)
    {
        return TicketStage::DeliveryRetry;
    }

    // Arm 1b — awaiting-confirmation: a durable `ask` is unanswered.
    // The pending set is a pure fold of this stream (CONFIRMATION-MODEL
    // §4); the answer is a control row on the same session, so the
    // ticket is waiting on a person without ever ending.
    if !kian_core::confirmation::pending_calls(events).is_empty() {
        return TicketStage::AwaitingConfirmation;
    }

    // Arm 2 — awaiting-answer: waiting question, thread still
    // outstanding, nothing has answered it since.
    if let Some((report_seq, ReportState::Waiting)) = facts.latest_stated_report
        && facts.thread_outstanding()
        && !facts.input_admitted_since(report_seq)
    {
        return TicketStage::AwaitingAnswer;
    }

    // Arm 3 — awaiting-review: the result is delivered, review is a
    // human's move.
    if matches!(facts.latest_stated_report, Some((_, ReportState::Ready))) {
        return TicketStage::AwaitingReview;
    }

    // Arm 4 — blocked-failed: a failure nobody has released yet (a
    // message after the failure moves the ticket ).
    if let Some(failed_seq) = facts.failed_terminal_seq
        && !facts.input_admitted_since(failed_seq)
    {
        return TicketStage::BlockedFailed;
    }

    // Arm 5 — queued: work is admitted but not claimed.
    if pending_inbox > 0 || facts.unclaimed_input() {
        return TicketStage::Queued;
    }

    // Arm 6 — idle: done (successfully); v1 resumes only via fresh Eval.
    if facts.completed_successfully() || facts.terminal_completed() {
        return TicketStage::Idle;
    }

    // Arm 7 — working.
    TicketStage::Working
}

/// The structured facts one worker stream yields, in the shape the
/// stage table reads. Derived once per projection; every field is a
/// pure fold over the events.
struct TicketFacts {
    thread_id: Option<Uuid>,
    /// The latest stated report (event seq + state) — the additive
    /// `state` key only; legacy reports never appear here.
    latest_stated_report: Option<(u64, ReportState)>,
    /// Latest `thread/completed` for this thread: (seq, status).
    thread_completed: Option<(u64, String)>,
    /// Latest `session/ended{completed}` seq (the terminal freeze).
    session_ended_completed: Option<u64>,
    /// The highest seq of any failure terminal (`thread/completed`
    /// with status `failed`, or `session/ended{failed}`).
    failed_terminal_seq: Option<u64>,
    last_input_seq: Option<u64>,
    last_claimed_seq: Option<u64>,
}

impl TicketFacts {
    fn derive(events: &[Event]) -> Self {
        let mut thread_id = None;
        let mut latest_stated_report = None;
        let mut thread_completed = None;
        let mut session_ended_completed = None;
        let mut failed_terminal_seq = None;
        let mut last_input_seq = None;
        let mut last_claimed_seq = None;

        for ev in events {
            match ev.kind {
                EventKind::ThreadStarted => {
                    // The worker-side lineage names this session and its
                    // thread; first one wins (one worker = one thread).
                    if thread_id.is_none()
                        && let Some(id) = event_thread_id(ev)
                    {
                        thread_id = Some(id);
                    }
                }
                EventKind::ThreadCompleted => {
                    let matches_thread = match (thread_id, event_thread_id(ev)) {
                        (Some(ours), Some(named)) => ours == named,
                        // Without a derived thread id, any completion on
                        // this stream is ours (one session, one thread).
                        _ => true,
                    };
                    if matches_thread
                        && let Some(status) = ev.bytes.get("status").and_then(|s| s.as_str())
                    {
                        if status == "failed" {
                            failed_terminal_seq = Some(ev.seq);
                        }
                        thread_completed = Some((ev.seq, status.to_string()));
                    }
                }
                EventKind::SessionEnded { reason } => {
                    use kian_core::events::EndReason;
                    match reason {
                        EndReason::Failed => failed_terminal_seq = Some(ev.seq),
                        EndReason::Completed => {
                            session_ended_completed = Some(ev.seq);
                        }
                        _ => {}
                    }
                }
                EventKind::InputAdmitted => {
                    // The delegation wrapper of the first input is a
                    // second thread-id source (worker streams always
                    // carry one).
                    if thread_id.is_none()
                        && let Some(delegation) = ev.bytes.get("content").and_then(parse_delegation)
                    {
                        thread_id = Some(delegation.thread_id);
                    }
                    // A report-shaped input content (e.g. a report
                    // relayed into this session) counts like any other
                    // stated report: latest wins.
                    if let Some(report) = ev.bytes.get("content").and_then(parse_thread_report)
                        && let Some(state) = report.state
                    {
                        latest_stated_report = Some((ev.seq, state));
                    }
                    last_input_seq = Some(ev.seq);
                }
                EventKind::AssistantMessage => {
                    // The worker's structured report is its assistant
                    // message content (the completion-report form).
                    if let Some(report) = ev.bytes.get("content").and_then(parse_thread_report)
                        && let Some(state) = report.state
                    {
                        latest_stated_report = Some((ev.seq, state));
                    }
                }
                EventKind::DrainClaimed => last_claimed_seq = Some(ev.seq),
                _ => {}
            }
        }

        Self {
            thread_id,
            latest_stated_report,
            thread_completed,
            session_ended_completed,
            failed_terminal_seq,
            last_input_seq,
            last_claimed_seq,
        }
    }

    /// No `thread/completed` for this thread (any status).
    fn thread_outstanding(&self) -> bool {
        self.thread_completed.is_none()
    }

    /// Any input admitted strictly after `seq` — the release rule. The
    /// delegation wrapper is always the first input, so it can never
    /// trip this.
    fn input_admitted_since(&self, seq: u64) -> bool {
        self.last_input_seq.is_some_and(|last| last > seq)
    }

    /// Input admitted with no subsequent `drain/claimed` (arm 5's
    /// stream half; the pending-inbox count is the other half).
    fn unclaimed_input(&self) -> bool {
        self.last_input_seq
            .is_some_and(|input| self.last_claimed_seq.is_none_or(|claim| input > claim))
    }

    /// `thread/completed{completed}` (not `failed`/`interrupted`).
    fn completed_successfully(&self) -> bool {
        self.thread_completed
            .as_ref()
            .is_some_and(|(_, status)| status == "completed")
    }

    /// `session/ended{completed}` — the terminal freeze.
    fn terminal_completed(&self) -> bool {
        self.session_ended_completed.is_some()
    }
}

/// `bytes.thread_id` as a uuid, if present and well-formed.
fn event_thread_id(ev: &Event) -> Option<Uuid> {
    let raw = ev.bytes.get("thread_id").and_then(|v| v.as_str())?;
    Uuid::parse_str(raw).ok()
}

#[cfg(test)]
mod tests {
    use super::TicketStage;

    /// `from_str_name` is the exact inverse of `as_str` for every stage,
    /// and refuses anything else (a doubt, never a guess — the P2a
    /// ticket view's stored stage strings decode through this).
    #[test]
    fn from_str_name_round_trips_every_stage() {
        for stage in [
            TicketStage::DeliveryRetry,
            TicketStage::AwaitingAnswer,
            TicketStage::AwaitingReview,
            TicketStage::BlockedFailed,
            TicketStage::Queued,
            TicketStage::Idle,
            TicketStage::Working,
        ] {
            assert_eq!(TicketStage::from_str_name(stage.as_str()), Some(stage));
        }
        assert_eq!(TicketStage::from_str_name("not-a-stage"), None);
        assert_eq!(TicketStage::from_str_name(""), None);
    }
}
