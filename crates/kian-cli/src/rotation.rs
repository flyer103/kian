//! Boot-time resident rotation (ADR-040 phase 0).
//!
//! DESIGN §4.6's runtime re-arm is a follow-up that needs a
//! runtime-rebindable Router binding; this module lands the safe first
//! phase: at `kian serve` assembly time — before any drain or service
//! exists — a Router whose stream grew past
//! `superagent.router.rotate_after_events` is retired with an honest
//! terminal `session/ended{rotated}` and the deterministic successor
//! (uuid5 chain) is bound everywhere the old id was. The successor then
//! receives, in this order: the ADR-038 memory index, the predecessor's
//! pending *message* rows, a bounded handoff derived from the
//! predecessor's stream, and the judge prompt.
//!
//! Why boot-time: every surface (driver routing, subscriptions, tools,
//! spawner, completion reports, watchdog) is constructed after the
//! re-arm walk, so binding the successor here needs no runtime rebind.
//! Rotation between restarts is ADR-040 phase 1/2; this phase bounds
//! context across restarts and refreshes the memory index on rotation.
//!
//! Gates: the live session must project `idle` (a crashed in-flight turn
//! is a takeover, never a rotation), the claim must be free (a live
//! replica's lease wins), and the threshold must be met. Pending
//! *control* rows are not migrated (they target the old session's
//! drains); pending message rows are copied to the successor and left on
//! the frozen predecessor as the audit record.
//!
//! Deployment assumption: phase 0 is a **single-serving-process** action
//! (or all replicas restart together). A second replica that keeps
//! serving would hold the retired session id in memory and write to a
//! frozen stream; making rotation safe across live replicas is ADR-040
//! phase 2 (the shared runtime binding).

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_core::secrets::SecretRedactor;
use kian_core::state::{SessionState, project};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};

/// What one boot-time rotation captured for the successor's bootstrap.
#[derive(Debug)]
pub struct BootRotation {
    /// The retired session (frozen `rotated`).
    pub predecessor: SessionId,
    /// The new live session (the deterministic successor).
    pub successor: SessionId,
    /// Pending message rows copied from the predecessor, in inbox order
    /// (payload verbatim, plus the re-admit idempotency key).
    pub migrated: Vec<(Value, Option<String>)>,
    /// The handoff message payload (a synthetic user turn).
    pub handoff: Value,
}

/// The rotation input row envelope: the reserved control shape never
/// migrates (it targets the old session's drains, not the successor).
fn is_control(payload: &Value) -> bool {
    payload
        .get("kian")
        .and_then(Value::as_object)
        .and_then(|inner| inner.get("type"))
        .and_then(Value::as_str)
        == Some("control")
}

/// Epoch milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Retires `live` and captures the successor's bootstrap material when
/// the threshold is met and every gate passes. `Ok(None)` is the common
/// case (feature off, under threshold, busy, or a fenced claim) and
/// changes nothing.
pub async fn rotate_at_boot(
    store: &EventStore,
    threshold: Option<u64>,
    live: SessionId,
    holder: &str,
    lease_ttl_ms: u64,
    redactor: &SecretRedactor,
) -> Result<Option<BootRotation>, String> {
    let Some(threshold) = threshold else {
        return Ok(None);
    };
    let events = store.rebuild(live).await.map_err(|e| e.to_string())?;
    if (events.len() as u64) < threshold {
        return Ok(None);
    }
    let state = project(&events).map_err(|e| e.to_string())?;
    if state != SessionState::Idle {
        tracing::warn!(
            session = %live.0,
            ?state,
            "resident rotation skipped: the session is not idle (takeover first)"
        );
        return Ok(None);
    }
    let successor = kian_superagent::rearm::successor_session(live);
    let now = now_ms();
    let rows = store
        .eligible_inputs(live, now)
        .await
        .map_err(|e| e.to_string())?;
    let migrated: Vec<(Value, Option<String>)> = rows
        .iter()
        .filter(|row| !is_control(&row.payload))
        .map(|row| {
            (
                row.payload.clone(),
                Some(format!("rotate:{}:{}", live.0, row.id)),
            )
        })
        .collect();
    let lease = match store.claim(live, holder, lease_ttl_ms, now).await {
        Ok(lease) => lease,
        Err(kian_store::InboxError::ClaimRejected { .. }) => {
            tracing::warn!(
                session = %live.0,
                "resident rotation skipped: a live lease owns the session"
            );
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    let head = store.head(live).await.map_err(|e| e.to_string())?;
    let event_count = events.len();
    let claim = Event {
        seq: head.next_seq,
        session: live,
        epoch: lease.epoch,
        ts: now,
        kind: EventKind::DrainClaimed,
        bytes: json!({"holder": holder, "epoch": lease.epoch.0}),
        version: SCHEMA_VERSION,
    };
    let end = Event {
        seq: head.next_seq + 1,
        session: live,
        epoch: lease.epoch,
        ts: now,
        kind: EventKind::SessionEnded {
            reason: EndReason::Rotated,
        },
        bytes: json!({
            "rotated_to": successor.0.to_string(),
            "events": event_count,
            "migrated": migrated.len(),
        }),
        version: SCHEMA_VERSION,
    };
    // One atomic batch: a crash before it leaves the session idle and
    // the next boot retries; a crash after it leaves a frozen
    // predecessor whose successor is derived deterministically.
    store
        .append_batch(
            live,
            &[claim, end],
            AppendGuard {
                epoch: lease.epoch,
                expected_seq: head.next_seq,
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        predecessor = %live.0,
        successor = %successor.0,
        events = event_count,
        migrated = migrated.len(),
        "resident router rotated at boot (ADR-040 phase 0)"
    );
    let handoff = handoff_payload(&events, live, event_count, redactor);
    Ok(Some(BootRotation {
        predecessor: live,
        successor,
        migrated,
        handoff,
    }))
}

/// The bounded handoff: counts plus the last assistant note, redacted and
/// truncated. It is a synthetic user turn (strippable on replay), never
/// model-authored free text.
fn handoff_payload(
    events: &[Event],
    predecessor: SessionId,
    event_count: usize,
    redactor: &SecretRedactor,
) -> Value {
    let turns = events
        .iter()
        .filter(|ev| ev.kind == EventKind::TurnStart)
        .count();
    let last_note = events.iter().rev().find_map(|ev| match ev.kind {
        EventKind::AssistantMessage => ev
            .bytes
            .pointer("/content/text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned),
        _ => None,
    });
    let mut text = format!(
        "[rotation] predecessor {} was retired after {} events ({} turns); its stream is the \
         frozen audit record. Continue its work; do not repeat completed actions.",
        predecessor.0, event_count, turns
    );
    if let Some(note) = last_note {
        let note = redactor.redact(&note);
        let mut end = note.len().min(600);
        while end > 0 && !note.is_char_boundary(end) {
            end -= 1;
        }
        if end > 0 {
            text.push_str("\n\nLast assistant note:\n");
            text.push_str(&note[..end]);
        }
    }
    json!({"synthetic": true, "content": text})
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Feature off, under threshold, or a running session: every gate
    /// leaves the stream byte-identical.
    #[tokio::test]
    async fn the_gates_leave_the_stream_untouched() {
        let dir = std::env::temp_dir().join(format!("kian-rotation-unit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let store = EventStore::open(&dir.join("kian.sqlite"))
            .await
            .expect("store");
        let redactor = SecretRedactor::default();
        let idle = SessionId(uuid::Uuid::new_v4());
        store
            .append_batch(
                idle,
                &[
                    ev(idle, 1, EventKind::SessionCreated),
                    ev(idle, 2, EventKind::InputAdmitted),
                ],
                AppendGuard {
                    epoch: kian_core::model::Epoch(1),
                    expected_seq: 1,
                },
            )
            .await
            .expect("seed idle");
        assert!(
            rotate_at_boot(&store, None, idle, "holder", 30_000, &redactor)
                .await
                .expect("off")
                .is_none()
        );
        assert!(
            rotate_at_boot(&store, Some(3), idle, "holder", 30_000, &redactor)
                .await
                .expect("under threshold")
                .is_none()
        );
        assert_eq!(store.rebuild(idle).await.expect("rebuild").len(), 2);

        let running = SessionId(uuid::Uuid::new_v4());
        store
            .append_batch(
                running,
                &[
                    ev(running, 1, EventKind::SessionCreated),
                    ev(running, 2, EventKind::InputAdmitted),
                    ev(running, 3, EventKind::DrainClaimed),
                ],
                AppendGuard {
                    epoch: kian_core::model::Epoch(1),
                    expected_seq: 1,
                },
            )
            .await
            .expect("seed running");
        assert!(
            rotate_at_boot(&store, Some(1), running, "holder", 30_000, &redactor)
                .await
                .expect("running gate")
                .is_none(),
            "a running session is a takeover, never a rotation"
        );
    }

    #[test]
    fn a_control_envelope_never_migrates() {
        assert!(is_control(&json!({
            "kian": {"type": "control", "action": "confirm", "call_id": "c",
                     "decision": "approve"}
        })));
        assert!(!is_control(&json!({"synthetic": true, "content": "hi"})));
        assert!(!is_control(&json!({"kian": {"type": "content"}})));
    }
}
