//! Versioned reads over the event stream (READ-MODEL §3–§4.1, P0).
//!
//! The read-side forms the wire serves `projection` versioning and the
//! SSE tail from. Everything here is a **pure function of the store**:
//! no cache, no projection state held between calls — a response's
//! version always identifies a committed prefix of the stream (R2) and
//! is derived from exactly the events `1..V` (never a mix across a
//! concurrent append).
//!
//! - [`versioned_projection`] — `version = head.next_seq - 1` with the
//!   unchanged fast path (R3: one head query, no scan) and the
//!   `since > version` refusal.
//! - [`events_after`] — the tail read SSE polls use: one head query +
//!   one bounded `scan` covering `(after, version]` plus the single row
//!   at `after` (the terminal probe), so per-poll work is O(new
//!   events), never O(history).

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::state::{SessionState, project};
use kian_store::EventStore;

use crate::engine::EngineError;

/// The result of a versioned projection read (READ-MODEL §3).
///
/// `unchanged` responses carry no state (R3); `state` is `Some` exactly
/// when `unchanged` is false.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VersionedProjection {
    /// The committed prefix the payload was derived from
    /// (`head.next_seq - 1` at read time).
    pub version: u64,
    /// `since == version`: nothing changed, no state is carried.
    pub unchanged: bool,
    /// `Some(state)` iff `unchanged` is false.
    pub state: Option<SessionState>,
}

/// The tail of a session's stream after `after` (READ-MODEL §4.1):
/// the delta events plus the head version and the terminal probe, all
/// from one head query and one bounded scan.
#[derive(Debug, Clone, PartialEq)]
pub struct EventTail {
    /// The head version the read observed (`head.next_seq - 1`).
    pub version: u64,
    /// Exactly the events with `after < seq <= version`, in seq order
    /// (R4: no gaps — contiguity comes from the CAS'd stream).
    pub events: Vec<Event>,
    /// Whether the stream's last committed event is `session/ended`
    /// (the SSE `done` condition). Probed from the same scan.
    pub terminal: bool,
}

/// `version = head.next_seq - 1`; `0` means the session has no events.
/// One seq-axis query — the R3 fast path's only store touch.
async fn version_of(store: &EventStore, session: SessionId) -> Result<u64, EngineError> {
    Ok(store.version(session).await?)
}

/// Versioned projection read (READ-MODEL §3).
///
/// - `since == None` → `{"version": V, "state": …}` (full read);
/// - `since == Some(V)` where `V` is current → `unchanged`, **no scan**
///   beyond the head query (R3);
/// - `since > version` → [`EngineError::SinceAhead`] (the client is
///   ahead or pointed at another store — an explicit error, never a
///   silent resync);
/// - an event-less session → [`ProjectionError::EmptyStream`] (the
///   `-32001` mapping the control plane already applies).
///
/// Consistency (R2): the changed path derives from a single committed
/// prefix — the scan's end bound is the head version read *before* it,
/// so an append racing between the two queries is wholly excluded from
/// the payload (the answer may go stale, never mixed).
pub async fn versioned_projection(
    store: &EventStore,
    session: SessionId,
    since: Option<u64>,
) -> Result<VersionedProjection, EngineError> {
    let version = version_of(store, session).await?;
    if version == 0 {
        return Err(kian_core::state::ProjectionError::EmptyStream.into());
    }
    if let Some(since) = since {
        if since == version {
            return Ok(VersionedProjection {
                version,
                unchanged: true,
                state: None,
            });
        }
        if since > version {
            return Err(EngineError::SinceAhead {
                session,
                since,
                version,
            });
        }
    }
    // The end bound pins the prefix: exactly events 1..=version,
    // whatever commits meanwhile.
    let events = store.scan(session, 0..version + 1).await?;
    if events.is_empty() {
        return Err(kian_core::state::ProjectionError::EmptyStream.into());
    }
    Ok(VersionedProjection {
        version,
        unchanged: false,
        state: Some(project(&events)?),
    })
}

/// The SSE tail read: events `(after, version]` plus the terminal
/// probe, in one head query + one bounded scan (READ-MODEL §4.1).
///
/// `after > version` is [`EngineError::SinceAhead`] (a resume id ahead
/// of the durable head — `event: error` + close on the wire). The scan
/// reads from `after` itself (not `after + 1`): the row at `after` —
/// already delivered on the resumed connection — is fetched only as
/// the terminal probe when nothing newer exists, keeping idle polls at
/// one indexed page without a second query.
pub async fn events_after(
    store: &EventStore,
    session: SessionId,
    after: u64,
) -> Result<EventTail, EngineError> {
    let version = version_of(store, session).await?;
    if version == 0 {
        // An event-less stream is not a session (the -32001 mapping).
        return Err(kian_core::state::ProjectionError::EmptyStream.into());
    }
    if after > version {
        return Err(EngineError::SinceAhead {
            session,
            since: after,
            version,
        });
    }
    // [after, version]: the probe row at `after` plus the delta.
    let rows = store.scan(session, after..version + 1).await?;
    let terminal = rows
        .last()
        .is_some_and(|e| matches!(e.kind, EventKind::SessionEnded { .. }));
    // R4 soundness: by seq-CAS contiguity the scan returns exactly
    // seqs after..=version. Verify rather than trust.
    let mut events = Vec::with_capacity(rows.len());
    let mut expected = after + 1;
    for ev in rows {
        if ev.seq < expected {
            continue; // the probe row at `after`
        }
        debug_assert_eq!(ev.seq, expected, "stream must be contiguous");
        if ev.seq != expected {
            return Err(kian_core::state::ProjectionError::EmptyStream.into());
        }
        expected += 1;
        events.push(ev);
    }
    Ok(EventTail {
        version,
        events,
        terminal,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::state::SessionState;
    use serde_json::json;
    use uuid::Uuid;

    use super::{EventTail, VersionedProjection, events_after, versioned_projection};
    use crate::engine::EngineError;
    use kian_store::{AppendGuard, EventStore};

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-read-test-{}.sqlite", Uuid::new_v4()))
    }

    fn ev(session: SessionId, seq: u64, epoch: Epoch, kind: EventKind) -> Event {
        Event {
            seq,
            session,
            epoch,
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes: json!({ "n": seq }),
            version: SCHEMA_VERSION,
        }
    }

    /// Seeds a projection-legal stream of `upto` events: `session/created`
    /// → `drain/claimed` → balanced `turn/start`/`turn/end` pairs; event
    /// `upto` is the end write when `terminal` (so terminal probing and
    /// full-state projection are both exercised).
    async fn seed(store: &EventStore, session: SessionId, upto: u64, terminal: bool) {
        for seq in 1..=upto {
            let kind = if seq == 1 {
                EventKind::SessionCreated
            } else if seq == 2 {
                EventKind::DrainClaimed
            } else if terminal && seq == upto {
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                }
            } else if seq % 2 == 1 {
                EventKind::TurnStart
            } else {
                EventKind::TurnEnd
            };
            store
                .append(
                    session,
                    ev(session, seq, Epoch::GENESIS, kind),
                    AppendGuard {
                        epoch: Epoch::GENESIS,
                        expected_seq: seq,
                    },
                )
                .await
                .expect("seed append");
        }
    }

    /// The kind `seed` places at `seq` (event-shape parity for tail
    /// assertions).
    fn seed_kind(seq: u64, upto: u64, terminal: bool) -> EventKind {
        if seq == 1 {
            EventKind::SessionCreated
        } else if seq == 2 {
            EventKind::DrainClaimed
        } else if terminal && seq == upto {
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            }
        } else if seq % 2 == 1 {
            EventKind::TurnStart
        } else {
            EventKind::TurnEnd
        }
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// Version is `head.next_seq - 1`; a no-since read carries it with
    /// the full state; `since == version` is unchanged with no state.
    #[tokio::test]
    async fn version_unchanged_and_full_read_shapes() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed(&store, sid, 5, false).await;

        let full = versioned_projection(&store, sid, None).await.expect("full");
        assert_eq!(
            full,
            VersionedProjection {
                version: 5,
                unchanged: false,
                // created → claimed (running) → turn events preserve running.
                state: Some(SessionState::Running),
            }
        );

        let same = versioned_projection(&store, sid, Some(5))
            .await
            .expect("unchanged");
        assert_eq!(
            same,
            VersionedProjection {
                version: 5,
                unchanged: true,
                state: None,
            }
        );

        // A stale since (behind head) is a normal full read.
        let stale = versioned_projection(&store, sid, Some(4))
            .await
            .expect("stale");
        assert!(!stale.unchanged);
        assert_eq!(stale.version, 5);
        assert!(stale.state.is_some());

        store.close().await;
        cleanup(&path);
    }

    /// `since > version` is the explicit SinceAhead error; an event-less
    /// session is EmptyStream either way.
    #[tokio::test]
    async fn ahead_and_empty_refusals() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");
        let sid = SessionId(Uuid::new_v4());
        seed(&store, sid, 3, false).await;
        let empty = SessionId(Uuid::new_v4());

        match versioned_projection(&store, sid, Some(4)).await {
            Err(EngineError::SinceAhead { since, version, .. }) => {
                assert_eq!((since, version), (4, 3));
            }
            other => panic!("expected SinceAhead, got {other:?}"),
        }
        assert!(matches!(
            versioned_projection(&store, empty, None).await,
            Err(EngineError::Projection(
                kian_core::state::ProjectionError::EmptyStream
            ))
        ));
        assert!(matches!(
            versioned_projection(&store, empty, Some(0)).await,
            Err(EngineError::Projection(
                kian_core::state::ProjectionError::EmptyStream
            ))
        ));

        store.close().await;
        cleanup(&path);
    }

    /// The tail read returns exactly `(after, version]`, flags terminal
    /// from the same scan (including the resumed-at-head case where the
    /// delta is empty), and refuses `after > version`.
    #[tokio::test]
    async fn tail_delta_terminal_and_ahead() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open");

        // Non-terminal stream.
        let sid = SessionId(Uuid::new_v4());
        seed(&store, sid, 6, false).await;
        let tail = events_after(&store, sid, 3).await.expect("tail");
        assert_eq!(
            tail,
            EventTail {
                version: 6,
                events: (4..=6)
                    .map(|seq| ev(sid, seq, Epoch::GENESIS, seed_kind(seq, 6, false)))
                    .collect(),
                terminal: false,
            }
        );
        // Resume at head: empty delta, terminal probed from row 6.
        let at_head = events_after(&store, sid, 6).await.expect("at head");
        assert!(at_head.events.is_empty());
        assert!(!at_head.terminal);
        // From the beginning: the full replay.
        let full = events_after(&store, sid, 0).await.expect("full");
        assert_eq!(full.events.len(), 6);

        // Terminal stream: the probe and the delta both see the end.
        let done = SessionId(Uuid::new_v4());
        seed(&store, done, 4, true).await;
        let mid = events_after(&store, done, 2).await.expect("mid");
        assert_eq!(mid.events.len(), 2);
        assert!(mid.terminal);
        let resumed = events_after(&store, done, 4)
            .await
            .expect("resumed at head");
        assert!(resumed.events.is_empty());
        assert!(
            resumed.terminal,
            "an idle poll on a terminal stream must report done"
        );

        // Ahead of head.
        assert!(matches!(
            events_after(&store, sid, 7).await,
            Err(EngineError::SinceAhead {
                since: 7,
                version: 6,
                ..
            })
        ));
        // Event-less session.
        let empty = SessionId(Uuid::new_v4());
        assert!(matches!(
            events_after(&store, empty, 0).await,
            Err(EngineError::Projection(
                kian_core::state::ProjectionError::EmptyStream
            ))
        ));

        store.close().await;
        cleanup(&path);
    }
}
