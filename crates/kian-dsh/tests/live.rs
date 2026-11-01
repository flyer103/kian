//! The env-gated live smoke (C54): `KIAN_DSH_BIN` absent → SKIP with
//! a log line, never fail (rule 9 — default CI needs no dsh). Present
//! → drive one real `dsh --profile sdk` child through the bridge; any
//! outcome is asserted honest (a contract mismatch surfaces as the
//! honest error, never a fabricated completion).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_dsh::mapping::MapCtx;
use kian_dsh::{DshCtx, DshCtxError, DshDriver, DshDriverConfig};
use serde_json::{Value, json};
use uuid::Uuid;

/// The in-memory drain context (the same shape the driver suite uses).
struct SmokeCtx {
    session: SessionId,
    events: std::sync::Mutex<Vec<Event>>,
    rows: std::sync::Mutex<Vec<InputRow>>,
    seq: std::sync::atomic::AtomicU64,
}

impl DshCtx for SmokeCtx {
    fn session(&self) -> SessionId {
        self.session
    }
    fn epoch(&self) -> Epoch {
        Epoch(1)
    }
    fn in_turn(&self) -> bool {
        false
    }
    fn events(&self) -> futures::future::BoxFuture<'_, Result<Vec<Event>, DshCtxError>> {
        let snapshot = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(snapshot) })
    }
    fn append(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> futures::future::BoxFuture<'_, Result<Event, DshCtxError>> {
        use std::sync::atomic::Ordering;
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let ev = Event {
            seq,
            session: self.session,
            epoch: Epoch(1),
            ts: seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        self.events.lock().unwrap().push(ev.clone());
        Box::pin(async move { Ok(ev) })
    }
    fn begin_turn(
        &self,
        bytes: Value,
    ) -> futures::future::BoxFuture<'_, Result<Event, DshCtxError>> {
        self.append(EventKind::TurnStart, bytes)
    }
    fn end_turn(&self, bytes: Value) -> futures::future::BoxFuture<'_, Result<Event, DshCtxError>> {
        self.append(EventKind::TurnEnd, bytes)
    }
    fn take_eligible_inputs(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<Vec<InputRow>, DshCtxError>> {
        let taken: Vec<InputRow> = std::mem::take(&mut self.rows.lock().unwrap());
        Box::pin(async move { Ok(taken) })
    }
    fn has_eligible_inputs(&self) -> futures::future::BoxFuture<'_, Result<bool, DshCtxError>> {
        let has = !self.rows.lock().unwrap().is_empty();
        Box::pin(async move { Ok(has) })
    }
}

#[tokio::test]
async fn live_dsh_smoke_env_gated() {
    let Ok(bin) = std::env::var("KIAN_DSH_BIN") else {
        eprintln!("SKIP: KIAN_DSH_BIN not set — the live S6 run is env-gated (rule 9)");
        return;
    };
    if bin.trim().is_empty() {
        eprintln!("SKIP: KIAN_DSH_BIN empty — the live S6 run is env-gated (rule 9)");
        return;
    }

    let ctx = Arc::new(SmokeCtx {
        session: SessionId(Uuid::nil()),
        events: std::sync::Mutex::new(vec![
            Event {
                seq: 0,
                session: SessionId(Uuid::nil()),
                epoch: Epoch(1),
                ts: 0,
                kind: EventKind::SessionCreated,
                bytes: json!({}),
                version: SCHEMA_VERSION,
            },
            Event {
                seq: 1,
                session: SessionId(Uuid::nil()),
                epoch: Epoch(1),
                ts: 1,
                kind: EventKind::DrainClaimed,
                bytes: json!({"holder": "live-smoke", "epoch": 1}),
                version: SCHEMA_VERSION,
            },
            Event {
                seq: 2,
                session: SessionId(Uuid::nil()),
                epoch: Epoch(1),
                ts: 2,
                kind: EventKind::InputAdmitted,
                bytes: json!({"role": "user", "content": {"text": "hello from kian"}}),
                version: SCHEMA_VERSION,
            },
        ]),
        rows: std::sync::Mutex::new(Vec::new()),
        seq: std::sync::atomic::AtomicU64::new(3),
    });

    let driver = DshDriver::new(DshDriverConfig {
        command: bin,
        args: vec!["--profile".to_owned(), "sdk".to_owned()],
        map: MapCtx {
            slot: "main".to_owned(),
            provider: "openai-compat".to_owned(),
            model: "dsh".to_owned(),
        },
        max_steps: 8,
        startup_timeout: Duration::from_secs(30),
    });

    let stream = driver.drive(ctx.clone() as Arc<dyn DshCtx>);
    futures::pin_mut!(stream);
    let mut last = None;
    while let Some(outcome) = stream.next().await {
        eprintln!("live dsh outcome: {outcome:?}");
        last = Some(outcome);
    }
    // Whatever the real dsh said, the bridge must be honest about it:
    // a settle is a settle, a failure carries the reason, and nothing
    // is ever a fabricated completion (constraint 7).
    match last {
        Some(LoopOutcome::Settled) | Some(LoopOutcome::Stopped { .. }) => {}
        Some(LoopOutcome::SoftFailed { reason }) => {
            eprintln!("live dsh run surfaced an honest failure: {reason}");
        }
        other => panic!("live dsh run ended without an honest outcome: {other:?}"),
    }
}
