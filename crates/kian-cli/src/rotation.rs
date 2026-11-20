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

use std::sync::Arc;

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_core::seams::LoopDriver;
use kian_core::secrets::SecretRedactor;
use kian_core::state::{SessionState, project};
use kian_orchestrator::{DrainCtx, Engine, RotateOutcome};
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

// ---------------------------------------------------------------------------
// Runtime rotation (ADR-040 phase 2)
// ---------------------------------------------------------------------------

/// The runtime rotation planner: one tick of the ticker retires the live
/// Router (when idle, over the threshold and free of eligible work) and
/// bootstraps the successor. Holds the boot-time profile (cloned per
/// rotation with the successor's id and re-arm cause), the ADR-038
/// session-bootstrap seam (the memory index first), the mounted
/// residency inbox (the judge prompt's admit counter) and the shared
/// live-resident binding (swapped at the end).
pub struct RuntimeRotation {
    /// The shared live-resident session every consumer reads.
    pub binding: Arc<kian_superagent::ResidentBinding>,
    /// `superagent.router.rotate_after_events` (>= 1).
    pub threshold: u64,
    /// The tick period (`drain.heartbeat_interval_ms`, clamped).
    pub period_ms: u64,
    /// The boot-time Router profile (session = the boot resolution).
    pub profile: kian_superagent::RouterProfile,
    /// ADR-038's memory index injection (the successor is fresh).
    pub bootstrap: Option<Arc<dyn kian_superagent::SessionBootstrap>>,
    /// The mounted residency inbox (the judge prompt lands here).
    pub router_inbox: Arc<kian_superagent::RouterInbox>,
    /// The handoff's note redactor (the same secret policy as memory).
    pub redactor: SecretRedactor,
}

impl RuntimeRotation {
    /// One tick. Returns the successor when a rotation happened.
    ///
    /// Order (the successor's context is fixed by ADR-040): the caller
    /// retires the predecessor atomically (engine-owned, phase 2's
    /// close), injects the memory index into the **fresh** successor
    /// (first context-bearing row), swaps the binding (every consumer
    /// re-points), migrates any row that raced the close (the
    /// predecessor is frozen; a copy lands on the successor), admits the
    /// bounded handoff, then the judge prompt (which wakes the
    /// successor). All gates fail closed to `Ok(None)`: a busy or
    /// running session is never rotated.
    pub async fn tick<D>(
        &self,
        engine: &Arc<Engine<D>>,
        store: &EventStore,
    ) -> Result<Option<SessionId>, String>
    where
        D: LoopDriver<Ctx = DrainCtx> + 'static,
    {
        let live = self.binding.load();
        let events = store.rebuild(live).await.map_err(|e| e.to_string())?;
        if events.is_empty() || (events.len() as u64) < self.threshold {
            return Ok(None);
        }
        if project(&events).map_err(|e| e.to_string())? != SessionState::Idle {
            return Ok(None);
        }
        let successor = kian_superagent::rearm::successor_session(live);
        let payload = json!({
            "rotated_to": successor.0.to_string(),
            "events": events.len(),
            "trigger": "runtime",
        });
        match engine
            .rotate_idle(live, payload)
            .await
            .map_err(|e| e.to_string())?
        {
            RotateOutcome::Rotated { .. } => {}
            // Raced: a drain claimed it, a replica owns the lease, an
            // input became eligible, or it ended. Nothing was written.
            _ => return Ok(None),
        }
        // 1. The memory index first (an explicit, fresh-only admit: the
        //    successor has no rows yet).
        if let Some(bootstrap) = self.bootstrap.as_ref()
            && let Err(error) = bootstrap.bootstrap(successor).await
        {
            tracing::warn!(successor = %successor.0, %error, "rotation: memory index skipped");
        }
        // 2. Re-point every consumer.
        self.binding.store(successor);
        // 3. Copy anything that raced the close (the predecessor is
        //    frozen now; the successor gets the work, at-least-once).
        let now = now_ms();
        if let Ok(rows) = store.eligible_inputs(live, now).await {
            for row in rows.iter().filter(|row| !is_control(&row.payload)) {
                let key = Some(format!("rotate:{}:{}", live.0, row.id));
                if let Err(error) = engine
                    .admit_quiet(successor, row.payload.clone(), key)
                    .await
                {
                    tracing::warn!(successor = %successor.0, %error, "rotation: input migration skipped");
                }
            }
        }
        // 4. The bounded handoff.
        let handoff = handoff_payload(&events, live, events.len(), &self.redactor);
        if let Err(error) = engine
            .admit_quiet(
                successor,
                handoff,
                Some(format!("rotate-handoff:{}", live.0)),
            )
            .await
        {
            tracing::warn!(successor = %successor.0, %error, "rotation: handoff skipped");
        }
        // 5. The judge prompt (a successor profile: its id and the
        //    rotated predecessor in the re-arm cause), then the wake
        //    that starts the successor's first drain.
        let mut profile = self.profile.clone();
        profile.session = successor;
        profile.re_arm.push(kian_superagent::rearm::EndedResident {
            session: live,
            reason: EndReason::Rotated,
        });
        profile
            .bootstrap(self.router_inbox.as_ref())
            .await
            .map_err(|e| format!("successor bootstrap: {e}"))?;
        // The bootstrap's admit does not wake by itself (the inbox
        // contract: admit then wake); this wake starts the successor's
        // first drain, whose pre-drive take reads the rows above in
        // order (index, migrated, handoff, prompt).
        engine.wake(successor);
        tracing::info!(
            predecessor = %live.0,
            successor = %successor.0,
            "resident router rotated at runtime (ADR-040 phase 2)"
        );
        Ok(Some(successor))
    }
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
