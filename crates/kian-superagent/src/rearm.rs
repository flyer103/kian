//! the resident re-arm path.
//!
//! A resident session (the Router, the watchdog) can end *terminally* —
//! `session/ended{completed}` (e.g. The loop's lifetime iteration budget
//! exhausting → `Flow::Stopped` → `EndReason::Completed`) freezes the
//! stream: the engine's drain projects `Terminal`, every later input
//! stays pending forever, and the (already consumed) idempotent
//! bootstrap row cannot re-wake it. A soft failure, by contrast, ends
//! `failed`, which projects back to `idle` — the *next* admitted input
//! re-opens the same session, no re-arm needed.
//!
//! Re-arm semantics (DESIGN "Resident re-arm"): on boot, the
//! pinned session id from config is walked down a **deterministic
//! successor chain**. A successor id is `uuid5(parent,
//! "kian:resident:re-arm")` — derivable from persisted state alone, so
//! every process, every audit tool, and every reboot agrees on the
//! successor of a given ended session. The first chain link whose
//! stream is empty (never created) or non-terminal is the **live**
//! resident session; every terminal link before it is recorded with its
//! end reason. Assembly then binds the live session everywhere the
//! pinned id used to go, and bootstraps it:
//!
//! - generation 0 (no ended predecessors): the legacy bootstrap key and
//!   prompt, byte-identical to the legacy behavior;
//! - generation ≥ 1: the key `"<prefix>:re-arm:<successor uuid>"`
//!   (stable per generation, so reboots dedupe onto the same row) and a
//!   judge prompt prefixed with an observable, derivable cause naming
//!   each ended predecessor and its reason.
//!
//! Zero new event kinds: the re-arm is visible as the successor's
//! `session/created` + `input/admitted` (the cause-carrying bootstrap
//! prompt) exactly like a first boot. No fake completions, no catch-up
//! ticks — inputs that arrived while the predecessor was terminal were
//! never consumed and stay honestly pending on the frozen stream.

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_core::state::project;

/// The chain-walk bound: a store whose chain exceeds this many terminal
/// resident sessions refuses to boot (fail-closed) instead of looping —
/// at that point the chain is growing faster than any honest watch and
/// an operator must look.
pub const RE_ARM_CHAIN_BOUND: usize = 64;

/// The frozen v5 name input of the successor derivation. This string is
/// part of the derivable-chain contract: changing it changes every
/// successor id and orphans every existing chain (additions-only spirit,
/// constraint 3 — it never changes after).
pub const RE_ARM_V5_NAME: &[u8] = b"kian:resident:re-arm";

/// The deterministic successor of an ended resident session:
/// `uuid5(parent, "kian:resident:re-arm")`.
#[must_use]
pub fn successor_session(session: SessionId) -> SessionId {
    SessionId(uuid::Uuid::new_v5(&session.0, RE_ARM_V5_NAME))
}

/// One ended predecessor in a re-arm chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedResident {
    pub session: SessionId,
    pub reason: EndReason,
}

impl EndedResident {
    /// The reason as the stable word used in re-arm prompts (serde's
    /// lowercase spelling of [`EndReason`]).
    #[must_use]
    pub fn reason_name(&self) -> &'static str {
        match self.reason {
            EndReason::Completed => "completed",
            EndReason::Cancelled => "cancelled",
            EndReason::Interrupted => "interrupted",
            EndReason::Failed => "failed",
        }
    }
}

/// A boot-time re-arm resolution: the live resident session and the
/// (possibly empty) chain of ended predecessors leading to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReArm {
    /// The session assembly binds everywhere (bootstrap target, inbox
    /// residency counter, spawner reports, subscription fallback).
    pub live: SessionId,
    /// Ended predecessors, oldest first. Empty = the configured pinned
    /// session is itself live (or never created) — generation 0.
    pub ended: Vec<EndedResident>,
}

impl ReArm {
    /// The generation-0 resolution of `pinned`.
    #[must_use]
    pub fn pinned(session: SessionId) -> Self {
        Self {
            live: session,
            ended: Vec::new(),
        }
    }

    /// Whether this resolution is a succession (generation ≥ 1).
    #[must_use]
    pub fn is_succession(&self) -> bool {
        !self.ended.is_empty()
    }

    /// The streams a rebuild must scan for the resident universe:
    /// ended predecessors (oldest first), then the live session.
    #[must_use]
    pub fn scan_chain(&self) -> Vec<SessionId> {
        let mut chain: Vec<SessionId> = self.ended.iter().map(|e| e.session).collect();
        chain.push(self.live);
        chain
    }
}

/// Why a chain walk failed.
#[derive(Debug, thiserror::Error)]
pub enum ReArmError<E> {
    #[error("resident stream read failed")]
    Stream(#[source] E),
    #[error("resident stream projection failed: {0}")]
    Project(String),
    #[error(
        "re-arm chain exceeded {bound} ended resident sessions from pinned {} — every link is terminal; operator intervention required",
        .pinned.0
    )]
    ChainTooLong { pinned: SessionId, bound: usize },
}

/// One walk step over a candidate session's committed stream.
///
/// `Ok(None)` — the candidate is live (or its stream is empty: never
/// created); the walk stops here. `Ok(Some(ended))` — the candidate is
/// terminal; the walk advances to its successor.
///
/// Terminal-from-events today means the tail event is
/// `session/ended{completed|cancelled}` (the only event-reachable
/// terminal states, DESIGN §2.2). A stream that projects terminal
/// without that tail shape (a future additions-only event kind) refuses
/// the walk fail-closed rather than guessing a reason.
pub fn terminal_resident(
    events: &[Event],
) -> Result<Option<EndedResident>, ReArmError<std::convert::Infallible>> {
    if events.is_empty() {
        return Ok(None);
    }
    let session = events[0].session;
    let state = project(events).map_err(|e| ReArmError::Project(e.to_string()))?;
    if !state.is_terminal() {
        return Ok(None);
    }
    match events.last().map(|e| e.kind) {
        Some(EventKind::SessionEnded { reason }) => Ok(Some(EndedResident { session, reason })),
        _ => Err(ReArmError::Project(format!(
            "session {} projects terminal ({state:?}) without a session/ended tail — unsupported stream shape",
            session.0
        ))),
    }
}

/// The chain walk: `stream_of` yields a candidate session's committed
/// stream (empty = never created). See the module docs for the
/// semantics; the walk is deterministic because [`successor_session`]
/// is.
///
/// `stream_of` is a port-shaped callback (the bundle crate reaches no
/// store — the §7 iron rule): each call returns a future producing the
/// candidate's stream, and assembly passes a closure reading the real
/// event store.
pub fn resolve_re_arm<S, E>(
    pinned: SessionId,
    mut stream_of: S,
) -> futures::future::BoxFuture<'static, Result<ReArm, ReArmError<E>>>
where
    S: FnMut(SessionId) -> futures::future::BoxFuture<'static, Result<Vec<Event>, E>>
        + Send
        + 'static,
    E: Send + 'static,
{
    Box::pin(async move {
        let mut current = pinned;
        let mut ended: Vec<EndedResident> = Vec::new();
        loop {
            if ended.len() >= RE_ARM_CHAIN_BOUND {
                return Err(ReArmError::ChainTooLong {
                    pinned,
                    bound: RE_ARM_CHAIN_BOUND,
                });
            }
            let events = stream_of(current).await.map_err(ReArmError::Stream)?;
            let Some(link) = terminal_resident(&events).narrow()? else {
                return Ok(ReArm {
                    live: current,
                    ended,
                });
            };
            ended.push(link);
            current = successor_session(current);
        }
    })
}

/// Infallible-error narrowing: [`terminal_resident`] never fails with a
/// stream error, so its error type widens losslessly to any
/// [`ReArmError<E>`].
trait Narrow<E> {
    fn narrow(self) -> Result<Option<EndedResident>, ReArmError<E>>;
}

impl<E> Narrow<E> for Result<Option<EndedResident>, ReArmError<std::convert::Infallible>> {
    fn narrow(self) -> Result<Option<EndedResident>, ReArmError<E>> {
        match self {
            Ok(v) => Ok(v),
            Err(ReArmError::Project(msg)) => Err(ReArmError::Project(msg)),
            Err(ReArmError::Stream(x)) => match x {},
            Err(ReArmError::ChainTooLong { pinned, bound }) => {
                Err(ReArmError::ChainTooLong { pinned, bound })
            }
        }
    }
}

/// The per-generation bootstrap idempotency key for a successor
/// session: `"<prefix>:re-arm:<uuid>"` (e.g.
/// `"router:bootstrap:re-arm:…"`). Distinct from every legacy
/// generation-0 key, and stable per generation because successor ids
/// are deterministic — every reboot dedupes onto the same row.
#[must_use]
pub fn re_arm_bootstrap_key(prefix: &str, session: SessionId) -> String {
    format!("{prefix}:re-arm:{}", session.0)
}

/// The cause line shared by re-arm bootstrap prompts: names each ended
/// predecessor and its reason, oldest first. Pure string rendering over
/// the chain — deterministic, replay-stable.
#[must_use]
pub fn re_arm_cause(ended: &[EndedResident]) -> String {
    let links = ended
        .iter()
        .map(|e| format!("{} ({})", e.session.0, e.reason_name()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("predecessor session(s) ended and are frozen: {links}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use serde_json::json;

    // A readable deterministic id builder: uuid v4 shape is irrelevant
    // to the walker; only distinctness matters.
    fn session(n: u8) -> SessionId {
        let mut bytes = [0u8; 16];
        bytes[0] = n;
        SessionId(uuid::Uuid::from_bytes(bytes))
    }

    fn ev(session: SessionId, seq: u64, kind: EventKind) -> Event {
        Event {
            seq,
            session,
            epoch: kian_core::model::Epoch(1),
            ts: 1_000 + seq,
            kind,
            bytes: json!({}),
            version: SCHEMA_VERSION,
        }
    }

    fn created(s: SessionId) -> Vec<Event> {
        vec![ev(s, 1, EventKind::SessionCreated)]
    }

    fn running(s: SessionId) -> Vec<Event> {
        vec![
            ev(s, 1, EventKind::SessionCreated),
            ev(s, 2, EventKind::DrainClaimed),
        ]
    }

    fn ended(s: SessionId, reason: EndReason) -> Vec<Event> {
        let mut events = running(s);
        events.push(ev(s, 3, EventKind::SessionEnded { reason }));
        events
    }

    fn ok(
        events: Vec<Event>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<Event>, std::convert::Infallible>> {
        Box::pin(async { Ok(events) })
    }

    #[test]
    fn successor_session_is_deterministic_distinct_and_chainable() {
        let parent = session(1);
        let a = successor_session(parent);
        let b = successor_session(parent);
        assert_eq!(a, b, "successor derivation is deterministic");
        assert_ne!(a, parent, "successor differs from its parent");
        assert_ne!(
            successor_session(a),
            a,
            "the chain extends without fixed points"
        );
        assert_ne!(successor_session(a), parent);
    }

    #[test]
    fn terminal_resident_passes_empty_live_and_ended_streams() {
        assert_eq!(terminal_resident(&[]).unwrap(), None);
        assert_eq!(terminal_resident(&created(session(1))).unwrap(), None);
        assert_eq!(terminal_resident(&running(session(1))).unwrap(), None);
        assert_eq!(
            terminal_resident(&ended(session(1), EndReason::Failed)).unwrap(),
            None,
            "soft failure projects idle — re-openable, not a chain link"
        );
        assert_eq!(
            terminal_resident(&ended(session(1), EndReason::Interrupted)).unwrap(),
            None,
            "interrupted projects idle — re-openable, not a chain link"
        );
    }

    #[test]
    fn terminal_resident_reports_completed_and_cancelled_links() {
        let s = session(2);
        assert_eq!(
            terminal_resident(&ended(s, EndReason::Completed)).unwrap(),
            Some(EndedResident {
                session: s,
                reason: EndReason::Completed
            })
        );
        assert_eq!(
            terminal_resident(&ended(s, EndReason::Cancelled)).unwrap(),
            Some(EndedResident {
                session: s,
                reason: EndReason::Cancelled
            })
        );
    }

    #[tokio::test]
    async fn resolve_fresh_store_arms_the_pinned_session() {
        let pinned = session(1);
        let rearm = resolve_re_arm(pinned, |_| ok(vec![])).await.unwrap();
        assert_eq!(rearm, ReArm::pinned(pinned));
        assert!(!rearm.is_succession());
        assert_eq!(rearm.scan_chain(), vec![pinned]);
    }

    #[tokio::test]
    async fn resolve_live_pinned_session_stays_generation_zero() {
        let pinned = session(1);
        let rearm = resolve_re_arm(pinned, move |s| {
            ok(if s == pinned { running(pinned) } else { vec![] })
        })
        .await
        .unwrap();
        assert_eq!(rearm, ReArm::pinned(pinned));
    }

    #[tokio::test]
    async fn resolve_soft_failed_pinned_session_stays_generation_zero() {
        // The design's original framing: a soft-failed resident projects
        // idle and re-opens on its next admitted input — same session,
        // no successor.
        let pinned = session(1);
        let rearm = resolve_re_arm(pinned, move |s| {
            ok(if s == pinned {
                ended(pinned, EndReason::Failed)
            } else {
                vec![]
            })
        })
        .await
        .unwrap();
        assert_eq!(rearm, ReArm::pinned(pinned));
    }

    #[tokio::test]
    async fn resolve_terminal_pinned_session_arms_its_successor() {
        let pinned = session(1);
        let successor = successor_session(pinned);
        let rearm = resolve_re_arm(pinned, move |s| {
            ok(if s == pinned {
                ended(pinned, EndReason::Completed)
            } else {
                vec![]
            })
        })
        .await
        .unwrap();
        assert_eq!(rearm.live, successor);
        assert_eq!(
            rearm.ended,
            vec![EndedResident {
                session: pinned,
                reason: EndReason::Completed
            }]
        );
        assert!(rearm.is_succession());
        assert_eq!(rearm.scan_chain(), vec![pinned, successor]);
    }

    #[tokio::test]
    async fn resolve_walks_multiple_terminal_generations() {
        let pinned = session(1);
        let s1 = successor_session(pinned);
        let s2 = successor_session(s1);
        let rearm = resolve_re_arm(pinned, move |s| {
            ok(if s == pinned {
                ended(pinned, EndReason::Completed)
            } else if s == s1 {
                ended(s1, EndReason::Cancelled)
            } else {
                running(s2)
            })
        })
        .await
        .unwrap();
        assert_eq!(rearm.live, s2);
        assert_eq!(rearm.ended.len(), 2);
        assert_eq!(rearm.ended[0].session, pinned);
        assert_eq!(rearm.ended[0].reason, EndReason::Completed);
        assert_eq!(rearm.ended[1].session, s1);
        assert_eq!(rearm.ended[1].reason, EndReason::Cancelled);
        assert_eq!(rearm.scan_chain(), vec![pinned, s1, s2]);
    }

    #[tokio::test]
    async fn resolve_refuses_an_unbounded_terminal_chain() {
        let pinned = session(1);
        // Every candidate (including every derived successor) is a
        // terminal stream — the walk must refuse, not loop.
        let err = resolve_re_arm(pinned, |s| ok(ended(s, EndReason::Completed)))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ReArmError::ChainTooLong {
                bound: RE_ARM_CHAIN_BOUND,
                ..
            }
        ));
    }

    #[test]
    fn re_arm_bootstrap_key_is_prefixed_stable_and_generation_scoped() {
        let a = session(1);
        let key = re_arm_bootstrap_key("router:bootstrap", a);
        assert_eq!(key, format!("router:bootstrap:re-arm:{}", a.0));
        assert_eq!(key, re_arm_bootstrap_key("router:bootstrap", a));
        assert_ne!(
            key,
            re_arm_bootstrap_key("router:bootstrap", successor_session(a))
        );
        assert_ne!(key, "router:bootstrap");
    }

    #[test]
    fn re_arm_cause_names_every_link_in_order() {
        let pinned = session(1);
        let s1 = successor_session(pinned);
        let cause = re_arm_cause(&[
            EndedResident {
                session: pinned,
                reason: EndReason::Completed,
            },
            EndedResident {
                session: s1,
                reason: EndReason::Cancelled,
            },
        ]);
        assert!(cause.contains(&pinned.0.to_string()));
        assert!(cause.contains(&s1.0.to_string()));
        assert!(cause.contains("(completed)"));
        assert!(cause.contains("(cancelled)"));
        assert!(cause.starts_with("predecessor session(s) ended and are frozen: "));
        // Order: pinned before its successor.
        assert!(cause.find(&pinned.0.to_string()) < cause.find(&s1.0.to_string()));
    }
}
