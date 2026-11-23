//! The env-gated live smoke: `KIAN_DSH_BIN` absent → SKIP with
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

/// Serializes the live tests: each spawns a real dsh child against the
/// same derived dsh session (same cwd), and two concurrent owners of one
/// session is exactly the ownership conflict the SDK rejects.
static LIVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

/// The seed stream: one promoted input the driver must deliver.
fn smoke_ctx() -> Arc<SmokeCtx> {
    Arc::new(SmokeCtx {
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
                bytes: json!({"role": "user", "content": "Reply with exactly the word: pong"}),
                version: SCHEMA_VERSION,
            },
        ]),
        rows: std::sync::Mutex::new(Vec::new()),
        seq: std::sync::atomic::AtomicU64::new(3),
    })
}

// The guard is held across awaits on purpose: each live test owns its own
// single-threaded tokio runtime on its own thread, so the std mutex only
// ever parks the *other* test's thread — exactly the serialization we want.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn live_dsh_smoke_env_gated() {
    let Ok(bin) = std::env::var("KIAN_DSH_BIN") else {
        eprintln!("SKIP: KIAN_DSH_BIN not set — the live dsh run is env-gated (rule 9)");
        return;
    };
    if bin.trim().is_empty() {
        eprintln!("SKIP: KIAN_DSH_BIN empty — the live dsh run is env-gated (rule 9)");
        return;
    }
    let _live = LIVE_LOCK.lock().unwrap();

    let ctx = smoke_ctx();

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
        step_timeout_ms: 1_800_000,
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

/// The live positive: `$KIAN_DSH_PEER` points at the contract-v0
/// peer (`contrib/dsh-v0-peer/dsh-v0-peer.mjs`), the driver spawns it with
/// the default `--profile sdk` args, and one real input rides kian → peer →
/// dsh → the real DeepSeek model. The assertions are evidence-based, not
/// all-or-nothing: the peer's ready handshake and event forwarding must be
/// proven by the frames that DID arrive, and the full mapped chain
/// (input/admitted, turn/start, step/request, assistant/message, turn/end)
/// is required only when the run actually settled with a model response —
/// a genuine model failure must surface as the honest error outcome, never
/// as a fabricated completion (constraint 7).
// See live_dsh_smoke_env_gated: the serialization guard is held across
// awaits on purpose (independent single-threaded runtimes, one per test).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn live_dsh_peer_positive_env_gated() {
    let Ok(peer_flag) = std::env::var("KIAN_DSH_PEER") else {
        eprintln!("SKIP: KIAN_DSH_PEER not set — the peer live positive is env-gated (rule 9)");
        return;
    };
    if peer_flag.trim().is_empty() {
        eprintln!("SKIP: KIAN_DSH_PEER empty — the peer live positive is env-gated (rule 9)");
        return;
    }
    // `KIAN_DSH_PEER` is either the peer script's path or `1` (the
    // runbook form), in which case `KIAN_DSH_BIN` carries the path.
    let peer = if peer_flag != "1" {
        peer_flag
    } else {
        match std::env::var("KIAN_DSH_BIN") {
            Ok(bin) if !bin.trim().is_empty() => bin,
            _ => {
                eprintln!("SKIP: KIAN_DSH_PEER=1 but KIAN_DSH_BIN does not name the peer script");
                return;
            }
        }
    };
    let _live = LIVE_LOCK.lock().unwrap();

    let ctx = smoke_ctx();
    let driver = DshDriver::new(DshDriverConfig {
        command: peer,
        args: vec!["--profile".to_owned(), "sdk".to_owned()],
        map: MapCtx {
            slot: "main".to_owned(),
            provider: "deepseek-official".to_owned(),
            model: "deepseek-chat".to_owned(),
        },
        max_steps: 8,
        startup_timeout: Duration::from_secs(30),
        step_timeout_ms: 1_800_000,
    });

    let stream = driver.drive(ctx.clone() as Arc<dyn DshCtx>);
    futures::pin_mut!(stream);
    let mut last = None;
    while let Some(outcome) = stream.next().await {
        eprintln!("live peer outcome: {outcome:?}");
        last = Some(outcome);
    }

    // The appended mapped stream is the evidence: kinds that arrived
    // through the peer, beyond the three seeded events.
    let events = ctx.events().await.unwrap_or_default();
    let appended: Vec<&Event> = events.iter().filter(|ev| ev.seq >= 3).collect();
    let kinds: Vec<EventKind> = appended.iter().map(|ev| ev.kind).collect();
    eprintln!(
        "live peer appended {} mapped events: {:?}",
        appended.len(),
        kinds
    );
    let has = |kind: EventKind| appended.iter().any(|ev| ev.kind == kind);

    let last = last.expect("the outcome stream produced at least one outcome");
    match last {
        LoopOutcome::Settled => {
            // Forwarding and the turn boundary are the floor: without
            // them the settle is vacuous (e.g. a peer that booted, did
            // nothing, and exited).
            assert!(
                has(EventKind::InputAdmitted),
                "Settled without a mapped input echo — the peer forwarded nothing"
            );
            assert!(
                has(EventKind::TurnStart),
                "Settled without a mapped turn/start — no provider turn crossed the bridge"
            );
            let last_turn_end = appended
                .iter()
                .rev()
                .find(|ev| ev.kind == EventKind::TurnEnd)
                .map(|ev| ev.bytes.clone());
            if has(EventKind::AssistantMessage) {
                // The full live positive.
                assert!(
                    has(EventKind::StepRequest),
                    "assistant/message arrived without the pre-flight step/request"
                );
                assert!(
                    has(EventKind::TurnEnd),
                    "assistant/message arrived without a mapped turn/end"
                );
                eprintln!("live peer POSITIVE: full mapped chain appended through the real model");
            } else if last_turn_end
                .as_ref()
                .and_then(|b| b.get("outcome"))
                .and_then(Value::as_str)
                == Some("error")
            {
                // The round trip reached the model and failed there; the
                // honest error outcome is mapped, not a fabricated
                // completion — exactly the acceptable negative.
                eprintln!(
                    "live peer honest model failure: turn/end outcome=error, no assistant message"
                );
            } else {
                panic!(
                    "Settled with no mapped assistant/message and no honest error outcome \
                     (last turn/end payload: {last_turn_end:?}) — the settle is unearned"
                );
            }
        }
        LoopOutcome::Stopped { reason } => {
            eprintln!("live peer stopped honestly: {reason}");
        }
        LoopOutcome::TurnDone => {
            eprintln!("live peer stream ended at a turn boundary (TurnDone was the last outcome)");
        }
        LoopOutcome::AwaitingBudget { budget } => {
            eprintln!("live peer paused on the token budget gate: {budget}");
        }
        LoopOutcome::SoftFailed { reason } => {
            // The peer's ready handshake is the floor for any failure
            // attribution: without it nothing about the peer half is
            // proven and the negative result would be vacuous.
            assert!(
                !reason.contains("never sent ready")
                    && !reason.contains("exited before ready")
                    && !reason.starts_with("spawn "),
                "the peer never ran or became ready — its half of the contract is unproven: {reason}"
            );
            eprintln!("live peer surfaced an honest failure after ready: {reason}");
            // Partial progress must stay visible: log which mapped kinds
            // DID arrive before the failure.
        }
    }
}
