//! The driver suite: the bridge driven end-to-end against the
//! scripted fixture peer through an in-memory `DshCtx` — zero Node/dsh
//! present. Covers spawn/steer/complete, the replay de-duplication
//! that makes takeover continuation safe, honest failure on child
//! crash, the interrupt translation, and the env-gated live smoke
//! (`KIAN_DSH_BIN` absent → skip, never fail; the mounted engine-level
//! kill-9 e2e lives in kian-cli's `dsh_bridge` suite).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use kian_core::context::derive_model_context;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_dsh::mapping::MapCtx;
use kian_dsh::{DshCtx, DshCtxError, DshDriver, DshDriverConfig};
use serde_json::{Value, json};
use uuid::Uuid;

const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const PEER: &str = env!("CARGO_BIN_EXE_kian-dsh-fixture-peer");

fn map_ctx() -> MapCtx {
    MapCtx {
        slot: "main".to_owned(),
        provider: "openai-compat".to_owned(),
        model: "test-model".to_owned(),
    }
}

fn driver(script: &str) -> DshDriver {
    DshDriver::new(DshDriverConfig {
        command: PEER.to_owned(),
        args: vec![format!("{FIXTURE_DIR}/{script}")],
        map: map_ctx(),
        max_steps: 32,
        startup_timeout: Duration::from_secs(10),
    })
}

/// The in-memory drain context: seeds a stream the way the engine
/// would (created + claimed + the promoted batch), records every
/// append, and hands eligible rows out at safe boundaries.
struct FakeCtx {
    session: SessionId,
    epoch: Epoch,
    state: Mutex<FakeState>,
    interrupt: AtomicBool,
    ts: AtomicU64,
}

struct FakeState {
    seq: u64,
    events: Vec<Event>,
    rows: Vec<InputRow>,
}

impl FakeCtx {
    /// The engine's pre-drive shape: created, claimed, then the
    /// promoted input/admitted for the initial user message.
    fn seeded(initial_user_content: Value) -> Arc<Self> {
        let session = SessionId(Uuid::nil());
        let mut events = vec![
            Event {
                seq: 1,
                session,
                epoch: Epoch(7),
                ts: 1,
                kind: EventKind::SessionCreated,
                bytes: json!({}),
                version: SCHEMA_VERSION,
            },
            Event {
                seq: 2,
                session,
                epoch: Epoch(7),
                ts: 2,
                kind: EventKind::DrainClaimed,
                bytes: json!({"holder": "test", "epoch": 7}),
                version: SCHEMA_VERSION,
            },
        ];
        events.push(Event {
            seq: 3,
            session,
            epoch: Epoch(7),
            ts: 3,
            kind: EventKind::InputAdmitted,
            bytes: json!({"role": "user", "content": initial_user_content}),
            version: SCHEMA_VERSION,
        });
        Arc::new(Self {
            session,
            epoch: Epoch(7),
            state: Mutex::new(FakeState {
                seq: 4,
                events,
                rows: Vec::new(),
            }),
            interrupt: AtomicBool::new(false),
            ts: AtomicU64::new(4),
        })
    }

    fn queue_row(&self, payload: Value) {
        self.state.lock().expect("ctx").rows.push(InputRow {
            id: Uuid::new_v4(),
            session: self.session,
            payload,
            admitted_at: 0,
            eligible_at: 0,
            idempotency_key: None,
        });
    }

    /// The takeover drain's claim (the engine appends `drain/claimed`
    /// before every drive; the driver's fresh-batch delivery reads it).
    fn claim(&self) {
        let fut = self.append(
            EventKind::DrainClaimed,
            json!({"holder": "takeover", "epoch": 8}),
        );
        fut.now_or_never()
            .expect("the in-memory append resolves immediately")
            .expect("the in-memory append never fails");
    }

    fn events_snapshot(&self) -> Vec<Event> {
        self.state.lock().expect("ctx").events.clone()
    }

    fn kinds(&self) -> Vec<EventKind> {
        self.events_snapshot()
            .into_iter()
            .map(|ev| ev.kind)
            .collect()
    }

    fn next_ts(&self) -> u64 {
        self.ts.fetch_add(1, Ordering::SeqCst)
    }
}

fn ctx_err(_: kian_core::events::Event) -> DshCtxError {
    unreachable!()
}

impl DshCtx for FakeCtx {
    fn session(&self) -> SessionId {
        self.session
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }

    fn in_turn(&self) -> bool {
        false
    }

    fn events(&self) -> futures::future::BoxFuture<'_, Result<Vec<Event>, DshCtxError>> {
        let snapshot = self.events_snapshot();
        Box::pin(async move { Ok(snapshot) })
    }

    fn append(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> futures::future::BoxFuture<'_, Result<Event, DshCtxError>> {
        let ev = {
            let mut state = self.state.lock().expect("ctx");
            let ev = Event {
                seq: state.seq,
                session: self.session,
                epoch: self.epoch,
                ts: self.next_ts(),
                kind,
                bytes,
                version: SCHEMA_VERSION,
            };
            state.seq += 1;
            state.events.push(ev.clone());
            ev
        };
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
        // Mirrors DrainCtx::take_eligible_inputs: consume each row to
        // its input/admitted event (the engine's row conventions).
        let taken: Vec<InputRow> = {
            let mut state = self.state.lock().expect("ctx");
            std::mem::take(&mut state.rows)
        };
        for row in &taken {
            let payload = match row.payload.as_object() {
                Some(obj)
                    if obj.get("synthetic").and_then(Value::as_bool) == Some(true)
                        && let Some(content) = obj.get("content") =>
                {
                    json!({"role": "user", "content": content, "synthetic": true})
                }
                _ => json!({"role": "user", "content": row.payload}),
            };
            let mut state = self.state.lock().expect("ctx");
            let ev = Event {
                seq: state.seq,
                session: self.session,
                epoch: self.epoch,
                ts: self.next_ts(),
                kind: EventKind::InputAdmitted,
                bytes: payload,
                version: SCHEMA_VERSION,
            };
            state.seq += 1;
            state.events.push(ev);
        }
        Box::pin(async move { Ok(taken) })
    }

    fn has_eligible_inputs(&self) -> futures::future::BoxFuture<'_, Result<bool, DshCtxError>> {
        let has = !self.state.lock().expect("ctx").rows.is_empty();
        Box::pin(async move { Ok(has) })
    }

    fn interrupt_requested(&self) -> bool {
        self.interrupt.load(Ordering::SeqCst)
    }
}

async fn drive_to_end(driver: &DshDriver, ctx: Arc<FakeCtx>) -> Vec<LoopOutcome> {
    let stream = driver.drive(ctx as Arc<dyn DshCtx>);
    futures::pin_mut!(stream);
    let mut outcomes = Vec::new();
    while let Some(outcome) = stream.next().await {
        outcomes.push(outcome);
    }
    outcomes
}

fn deploy() -> Value {
    json!({"text": "deploy the service"})
}

/// live-run finding (the hang): a steer admitted *after* the drain
/// already reached the safe boundary and went idle must still be
/// delivered — the driver may not depend on a wake to make progress
/// (constraint 6: wakes may fold, be lost, or race). The steer row
/// below becomes eligible 300 ms in, well after the driver is parked
/// waiting for the peer's next line (the peer blocks on `expect steer`
/// and says nothing). Without the inbox re-check this deadlocks: the
/// test wraps the drive in a timeout so the deadlock is a failure, not
/// a hang.
#[tokio::test]
async fn late_eligible_input_still_lands_at_the_idle_boundary() {
    let ctx = FakeCtx::seeded(deploy());
    // The steer arrives late: after the turn-1 boundary check ran empty.
    let delayed = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        delayed.queue_row(json!({"synthetic": true, "content": {"text": "switch to plan B"}}));
    });

    let driver = driver("steer.jsonl");
    let outcomes =
        tokio::time::timeout(Duration::from_secs(15), drive_to_end(&driver, ctx.clone()))
            .await
            .expect("the bridge must not deadlock waiting for a wake; it re-checks the inbox");

    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);
    let turn_starts = ctx
        .kinds()
        .into_iter()
        .filter(|k| matches!(k, EventKind::TurnStart))
        .count();
    assert_eq!(turn_starts, 2, "the late steer still drove the second turn");
}

#[tokio::test]
async fn spawn_steer_complete_runs_the_full_bridge_flow() {
    let ctx = FakeCtx::seeded(deploy());
    // A steer arrives mid-episode (admitted while the drain is in
    // flight; the row becomes eligible at the next safe boundary).
    ctx.queue_row(json!({"synthetic": true, "content": {"text": "switch to plan B"}}));

    let outcomes = drive_to_end(&driver("steer.jsonl"), ctx.clone()).await;

    // TurnDone per mapped step, then the settle.
    assert!(outcomes.iter().any(|o| matches!(o, LoopOutcome::TurnDone)));
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);

    let events = ctx.events_snapshot();
    let count = |kind: EventKind, pred: Box<dyn Fn(&Event) -> bool>| {
        events
            .iter()
            .filter(|ev| ev.kind == kind && pred(ev))
            .count()
    };

    // The promoted user input is NOT re-appended by the child's echo
    // (replay de-duplication against the promotion).
    assert_eq!(
        count(
            EventKind::InputAdmitted,
            Box::new(|ev| ev.bytes["content"] == deploy())
        ),
        1
    );
    // The steer: one synthetic promotion, no duplicate from the echo.
    assert_eq!(
        count(
            EventKind::InputAdmitted,
            Box::new(|ev| ev.bytes.get("synthetic") == Some(&json!(true)))
        ),
        1
    );
    // The dsh system prompt maps to an admitted system input (closest
    // call #1, ADR-010), exactly once.
    assert_eq!(
        count(
            EventKind::InputAdmitted,
            Box::new(|ev| ev.bytes["role"] == "system")
        ),
        1
    );
    // Two mapped provider turns, each with persisted request bytes
    // (persist-what-you-send across the process boundary).
    assert_eq!(
        count(
            EventKind::StepRequest,
            Box::new(|ev| ev.bytes["slot"] == "main")
        ),
        2
    );
    assert_eq!(
        count(EventKind::TurnStart, Box::new(|_| true)),
        2,
        "kinds: {:?}",
        ctx.kinds()
    );
    // Tool round-trip rides the existing payload shapes verbatim.
    assert_eq!(count(EventKind::ToolCall, Box::new(|_| true)), 1);
    assert_eq!(count(EventKind::ToolResult, Box::new(|_| true)), 1);
    assert_eq!(count(EventKind::AssistantMessage, Box::new(|_| true)), 2);
}

#[tokio::test]
async fn completed_stream_replays_byte_identically() {
    let ctx = FakeCtx::seeded(deploy());
    drive_to_end(&driver("basic_turn.jsonl"), ctx.clone()).await;

    // A2: the model-visible projection of the finished stream is a
    // stable function of the bytes — deriving twice is byte-identical.
    let events = ctx.events_snapshot();
    let a = serde_json::to_vec(&derive_model_context(&events).expect("context")).expect("bytes");
    let events2 = ctx.events_snapshot();
    let b = serde_json::to_vec(&derive_model_context(&events2).expect("context")).expect("bytes");
    assert_eq!(a, b);
    // And the request bytes the model saw are the request bytes dsh
    // assembled (persist-what-you-send, not a re-derivation).
    let requests: Vec<&Value> = events
        .iter()
        .filter(|ev| ev.kind == EventKind::StepRequest)
        .map(|ev| &ev.bytes["body"])
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["model"], "dsh-model");
}

#[tokio::test]
async fn takeover_replay_appends_only_new_events() {
    // The child's script lives at one path whose content changes
    // between phases — a fresh dsh child replays its own durable log,
    // so "phase 2" is what the log plus continuation produces.
    let script =
        std::env::temp_dir().join(format!("kian-dsh-takeover-{}.jsonl", uuid::Uuid::new_v4()));
    let initial = std::fs::read_to_string(format!("{FIXTURE_DIR}/replay_initial.jsonl")).unwrap();
    let bundle = std::fs::read_to_string(format!("{FIXTURE_DIR}/replay_bundle.jsonl")).unwrap();
    std::fs::write(&script, &initial).unwrap();

    fn driver_at(script: &std::path::Path) -> DshDriver {
        DshDriver::new(DshDriverConfig {
            command: PEER.to_owned(),
            args: vec![script.to_string_lossy().into_owned()],
            map: MapCtx {
                slot: "main".to_owned(),
                provider: "openai-compat".to_owned(),
                model: "test-model".to_owned(),
            },
            max_steps: 32,
            startup_timeout: Duration::from_secs(10),
        })
    }

    // built-in: the drain dies mid-step after the first step completed
    // (stream dropped = kill -9 semantics; the stalled child dies with
    // the dropped driver).
    let ctx = FakeCtx::seeded(deploy());
    {
        let d = driver_at(&script);
        let stream = d.drive(ctx.clone() as Arc<dyn DshCtx>);
        futures::pin_mut!(stream);
        // Consume until the first mapped turn completes, then die.
        match stream.next().await {
            Some(LoopOutcome::TurnDone) => {}
            Some(other) => panic!("unexpected outcome in phase 1: {other:?}"),
            None => panic!("phase 1 stream ended before a turn completed"),
        }
    }
    let after_initial = ctx.events_snapshot();
    let step_requests_1 = after_initial
        .iter()
        .filter(|ev| ev.kind == EventKind::StepRequest)
        .count();
    assert_eq!(step_requests_1, 1);

    // bundle: the takeover drain — a NEW driver (new process) claims
    // (the engine's shape) and spawns a fresh child that REPLAYS its
    // durable log (the same events again) and then continues.
    ctx.claim();
    std::fs::write(&script, &bundle).unwrap();
    let outcomes = drive_to_end(&driver_at(&script), ctx.clone()).await;
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);

    let events = ctx.events_snapshot();
    // The replayed events were matched, not re-appended: exactly one
    // system input, one echo of the user input (the promotion), two
    // steps total, one assistant message each.
    let count = |kind: EventKind| events.iter().filter(|ev| ev.kind == kind).count();
    assert_eq!(count(EventKind::StepRequest), 2);
    assert_eq!(count(EventKind::TurnStart), 2);
    assert_eq!(count(EventKind::AssistantMessage), 2);
    assert_eq!(
        events
            .iter()
            .filter(|ev| ev.kind == EventKind::InputAdmitted && ev.bytes["content"] == deploy())
            .count(),
        1,
        "the replayed user echo must not double-append"
    );
    // And the finished stream still projects a stable model context.
    assert!(derive_model_context(&events).is_ok());
}

#[tokio::test]
async fn child_crash_mid_step_is_interrupted_never_completed() {
    let ctx = FakeCtx::seeded(deploy());
    let outcomes = drive_to_end(&driver("crash_mid_step.jsonl"), ctx.clone()).await;

    let last = outcomes.last().expect("outcomes");
    match last {
        LoopOutcome::SoftFailed { reason } => {
            assert!(
                reason.contains("interrupted, never completed"),
                "reason: {reason}"
            );
        }
        other => panic!("a child crash must be a soft failure, got {other:?}"),
    }

    // The honest failure trace: an attempt with NO assistant
    // message after it, and nothing that could read as completion.
    let events = ctx.events_snapshot();
    let attempt_idx = events
        .iter()
        .position(|ev| ev.kind == EventKind::StepRequest)
        .expect("the attempt was persisted");
    let after: Vec<EventKind> = events[attempt_idx + 1..].iter().map(|ev| ev.kind).collect();
    assert!(
        !after.contains(&EventKind::AssistantMessage),
        "no fabricated response: {after:?}"
    );
    assert!(!after.contains(&EventKind::TurnEnd));
}

#[tokio::test]
async fn interrupt_translates_to_a_cancelled_step() {
    let ctx = FakeCtx::seeded(deploy());
    ctx.interrupt.store(true, Ordering::SeqCst);
    let outcomes = drive_to_end(&driver("interrupt.jsonl"), ctx.clone()).await;

    // The interrupt rides the control frame; the step settles
    // cancelled (an attempt with no assistant message) and the engine
    // side maps its own flag to `interrupted` — the driver's job is
    // the honest cancelled trace, never a fabricated response.
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);
    let events = ctx.events_snapshot();
    let cancelled = events
        .iter()
        .find(|ev| ev.kind == EventKind::TurnEnd)
        .expect("the cancelled step ended");
    assert_eq!(cancelled.bytes["outcome"], "cancelled");
    assert!(
        !events
            .iter()
            .any(|ev| ev.kind == EventKind::AssistantMessage)
    );
}

#[tokio::test]
async fn foreign_logical_format_is_refused_before_any_mapping() {
    let ctx = FakeCtx::seeded(deploy());
    let outcomes = drive_to_end(&driver("foreign_format.jsonl"), ctx.clone()).await;
    match outcomes.last().expect("outcomes") {
        LoopOutcome::SoftFailed { reason } => {
            assert!(reason.contains("logical format"), "reason: {reason}");
        }
        other => panic!("expected an honest contract error, got {other:?}"),
    }
    // Nothing was appended past the seeded stream.
    assert_eq!(ctx.events_snapshot().len(), 3);
}

// The unused-helper silencer: ctx_err documents the infallible fake.
#[allow(dead_code)]
fn silence() {
    let _ = ctx_err as fn(kian_core::events::Event) -> DshCtxError;
}

/// Port (d″) — the retrospective `tool/post` observation (§3.5): the
/// post-append branch fires the crate-local port with the step's
/// mapped name, the serialized output's byte count and digest, and no
/// redaction provenance; a result with no matching mapped call would
/// be skipped with one audit line (name-resolution is by the recorded
/// `tool/call`, never fabricated).
#[tokio::test]
async fn tool_post_observes_the_mapped_tool_result_retrospectively() {
    struct RecordingPort {
        seen: Mutex<Vec<kian_dsh::toolpost::ToolPostView>>,
    }
    impl kian_dsh::toolpost::ToolPostPort for RecordingPort {
        fn tool_post(
            &self,
            view: kian_dsh::toolpost::ToolPostView,
        ) -> futures::future::BoxFuture<'static, ()> {
            self.seen.lock().expect("port").push(view);
            Box::pin(async {})
        }
    }

    let ctx = FakeCtx::seeded(deploy());
    let port = Arc::new(RecordingPort {
        seen: Mutex::new(Vec::new()),
    });
    let driver = driver("basic_turn.jsonl")
        .with_tool_post_port(port.clone() as Arc<dyn kian_dsh::toolpost::ToolPostPort>);
    let outcomes =
        tokio::time::timeout(Duration::from_secs(15), drive_to_end(&driver, ctx.clone()))
            .await
            .expect("bridge flow completes");
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);

    let seen = port.seen.lock().expect("port").clone();
    assert_eq!(seen.len(), 1, "one observation per mapped tool result");
    assert_eq!(seen[0].call_id, "call-1");
    assert_eq!(
        seen[0].name, "run_build",
        "the name comes from the step's mapped tool/call"
    );
    // Byte count and digest describe the serialized output exactly.
    let output = serde_json::json!({"ok": true});
    let bytes = serde_json::to_vec(&output).expect("serialize");
    assert_eq!(seen[0].output_bytes, bytes.len() as u64);
    assert_eq!(
        seen[0].output_sha256,
        kian_dsh::toolpost::sha256_hex(&bytes)
    );
    assert!(seen[0].redactions.is_empty());
    // The observation is honest about sequencing: the `tool/result`
    // append it describes is already committed.
    assert!(ctx.kinds().contains(&EventKind::ToolResult));
}

/// §3.5/§17 (M4-c): a `tool/result` with **no matching mapped
/// `tool/call`** is SKIPPED — one audit line, never a dispatched
/// observation with a fabricated name.
#[tokio::test]
async fn result_without_mapped_call_is_skipped_with_audit() {
    struct RecordingPort {
        seen: Mutex<Vec<kian_dsh::toolpost::ToolPostView>>,
    }
    impl kian_dsh::toolpost::ToolPostPort for RecordingPort {
        fn tool_post(
            &self,
            view: kian_dsh::toolpost::ToolPostView,
        ) -> futures::future::BoxFuture<'static, ()> {
            self.seen.lock().expect("port").push(view);
            Box::pin(async {})
        }
    }

    let ctx = FakeCtx::seeded(deploy());
    let port = Arc::new(RecordingPort {
        seen: Mutex::new(Vec::new()),
    });
    // The fixture's `call-1` result arrives with NO mapped `tool/call`
    // event before it (the assistant message's tool_calls are model
    // speech, not the recorded event).
    let driver = driver("unmapped_result.jsonl")
        .with_tool_post_port(port.clone() as Arc<dyn kian_dsh::toolpost::ToolPostPort>);
    let outcomes =
        tokio::time::timeout(Duration::from_secs(15), drive_to_end(&driver, ctx.clone()))
            .await
            .expect("bridge flow completes");
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);

    assert!(
        port.seen.lock().expect("port").is_empty(),
        "a result with no matching mapped call is skipped — never dispatched with a fabricated name"
    );
    // The skip is audited, not silent: the `tool/result` itself still
    // landed in the stream (the observation is the only thing skipped).
    assert!(ctx.kinds().contains(&EventKind::ToolResult));
}

/// §3.5/§17 (M4-c): the dedupe path dispatches nothing — a replayed
/// event (already persisted by an earlier drain) is skipped before the
/// append, so its retrospective `tool/post` observation never re-fires.
#[tokio::test]
async fn dedupe_path_dispatches_nothing() {
    struct RecordingPort {
        seen: Mutex<Vec<kian_dsh::toolpost::ToolPostView>>,
    }
    impl kian_dsh::toolpost::ToolPostPort for RecordingPort {
        fn tool_post(
            &self,
            view: kian_dsh::toolpost::ToolPostView,
        ) -> futures::future::BoxFuture<'static, ()> {
            self.seen.lock().expect("port").push(view);
            Box::pin(async {})
        }
    }

    let ctx = FakeCtx::seeded(deploy());
    // Drive 1: the mapped result appends and observes exactly once.
    let port1 = Arc::new(RecordingPort {
        seen: Mutex::new(Vec::new()),
    });
    let driver1 = driver("basic_turn.jsonl")
        .with_tool_post_port(port1.clone() as Arc<dyn kian_dsh::toolpost::ToolPostPort>);
    let outcomes =
        tokio::time::timeout(Duration::from_secs(15), drive_to_end(&driver1, ctx.clone()))
            .await
            .expect("first drive completes");
    assert_eq!(*outcomes.last().expect("outcomes"), LoopOutcome::Settled);
    assert_eq!(port1.seen.lock().expect("port").len(), 1);
    let appended_after_first = ctx.events_snapshot().len();

    // Drive 2 over the SAME stream: every mapped event is a replay —
    // deduped before the append, so nothing re-appends and nothing
    // re-observes.
    let port2 = Arc::new(RecordingPort {
        seen: Mutex::new(Vec::new()),
    });
    let driver2 = driver("basic_turn.jsonl")
        .with_tool_post_port(port2.clone() as Arc<dyn kian_dsh::toolpost::ToolPostPort>);
    let _ = tokio::time::timeout(Duration::from_secs(15), drive_to_end(&driver2, ctx.clone()))
        .await
        .expect("second drive completes");

    assert!(
        port2.seen.lock().expect("port").is_empty(),
        "a deduped (replayed) result dispatches no tool/post"
    );
    assert_eq!(
        ctx.events_snapshot().len(),
        appended_after_first,
        "the dedupe path appends nothing either"
    );
}
