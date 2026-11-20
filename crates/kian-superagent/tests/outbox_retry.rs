//! Outbox bounded retry (DESIGN §4.2 + §4.7):
//! stored work for the write side — the four acceptance gates,
//! mirrored from the design:
//!
//! 1. **flaky → success**: a send failing twice then succeeding
//!    delivers exactly once with `attempt: 3` on `channel/delivered`,
//!    and the backoff schedule (2s, 4s, … cap 60s) is asserted
//!    against the injected clock;
//! 2. **per-conversation ordering**: a later message of the same
//!    `(ticket, conversation)` waits for the earlier one; a different
//!    ticket's message (and a different conversation's) never waits;
//! 3. **max-attempts ⇒ parked honestly**: no further sends, the stage
//!    projects `delivery-retry` through the real `OutboxView` wiring,
//!    audit + counter recorded, the model is not re-asked (one tool
//!    invocation, the driver did the attempts);
//! 4. **kill-the-driver-mid-retry ⇒ rebuild re-drives**: an in-flight
//!    intent (a `tool/call` with no `tool/result`) rebuilt from the
//!    stream re-drives through the adapter — at-least-once, the
//!    receipt lands once.
//!
//! Plus the compatibility gate: absent config ⇒ the legacy path is
//! byte-identical (single attempt, plain honest error, no attempt
//! keys, legacy `channel/delivered` payloads parse unchanged).
//!
//! No credentials, no network egress. Log capture follows the
//! `tests/speak_rules.rs` process-global discipline.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{
    ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt, ToolCall, ToolError,
    ToolProvider, ToolResult,
};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, ImPolicy, InboxWriteError, InboxWriter, ManualRetryClock, NoOutbox, OutboxConfig,
    OutboxDriver, OutboxState, OutboxStatus, RetryClock, RouterHold, RouterProfile, RouterTools,
    SpeakTracker, StreamPort, StreamPortError, StructuredPolicy, ThreadOutboxView, TicketStage,
    WorkerSpawner, im_delivered_payload, outbox_pending, project_stage, router_template,
    worker_template,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Log capture (audit lines) — one shared buffer per test binary
// ---------------------------------------------------------------------------

struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MakeShared(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        SharedWriter(Arc::clone(&self.0))
    }
}

fn logs() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    BUF.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(Arc::clone(&buf)))
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        buf
    })
    .clone()
}

fn log_text() -> String {
    String::from_utf8_lossy(&logs().lock().unwrap()).into_owned()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(uuid::Uuid::from_u128(n))
}

/// A stream port whose `read_stream` returns everything ever emitted —
/// the Router's committed stream as the tools see it.
struct ScriptedPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
}

impl ScriptedPort {
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(Vec::new()),
        });
        let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
        (port, dyn_port)
    }

    fn seed(&self, kind: EventKind, bytes: Value) {
        let mut events = self.events.lock().unwrap();
        let seq = events.len() as u64 + 1;
        events.push(Event {
            seq,
            session: self.session,
            epoch: Epoch(2),
            ts: 1_000 + seq,
            kind,
            bytes,
            version: 0,
        });
    }

    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn deliveries(&self) -> Vec<Event> {
        self.events()
            .into_iter()
            .filter(|e| e.kind == EventKind::ChannelDelivered)
            .collect()
    }
}

impl StreamPort for ScriptedPort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        self.seed(kind, bytes);
        Box::pin(async { Ok(()) })
    }
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events();
        Box::pin(async move { Ok(events) })
    }
}

/// The scripted flaky adapter: each `send` pops the next scripted
/// result (an empty script answers `Ok`); every attempt is counted
/// and every delivery captured.
struct FlakyChannel {
    calls: std::sync::atomic::AtomicU64,
    script: Mutex<VecDeque<Result<SendReceipt, ChannelError>>>,
    deliveries: Mutex<Vec<ChannelDelivery>>,
}

impl FlakyChannel {
    fn fail_n_times(&self, n: u64) {
        let mut script = self.script.lock().unwrap();
        for _ in 0..n {
            script.push_back(Err(ChannelError::DeliveryFailed(
                "scripted flaky failure".into(),
            )));
        }
    }

    fn calls(&self) -> u64 {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn deliveries(&self) -> Vec<ChannelDelivery> {
        self.deliveries.lock().unwrap().clone()
    }
}

impl ChannelAdapter for FlakyChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        futures::stream::empty().boxed()
    }
    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.deliveries.lock().unwrap().push(out);
        let result = self.script.lock().unwrap().pop_front().unwrap_or_else(|| {
            Ok(SendReceipt {
                message_id: format!("flk-{}", self.calls()),
                ts_ms: 42,
            })
        });
        Box::pin(async move { result })
    }
}

/// An inbox fake (admits recorded, never used by these gates).
struct NullInbox;

impl InboxWriter for NullInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

/// Router tools over the flaky channel. `Some(config, clock)` enables
/// the outbox (a driver sharing the clock — the test's assertion
/// surface); `None` is the default-off legacy path.
fn make_tools(
    outbox: Option<(OutboxConfig, Arc<ManualRetryClock>)>,
) -> (
    Arc<RouterTools>,
    Arc<RouterHold>,
    Arc<FlakyChannel>,
    Arc<ScriptedPort>,
) {
    // Install the capturing subscriber BEFORE any audited event runs
    // (a lazy first call at assert time would miss everything).
    logs();
    let router = sid(10);
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::new(NullInbox) as Arc<dyn InboxWriter>,
    ));
    let flaky = Arc::new(FlakyChannel {
        calls: std::sync::atomic::AtomicU64::new(0),
        script: Mutex::new(VecDeque::new()),
        deliveries: Mutex::new(Vec::new()),
    });
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&flaky) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    let mut tools = RouterTools::new(
        spawner,
        channels.clone(),
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    );
    if let Some((config, clock)) = outbox {
        let driver = Arc::new(OutboxDriver::new(config, clock, channels));
        tools = tools.with_outbox_driver(driver);
    }
    let (port, dyn_port) = ScriptedPort::shared(router);
    hold.install(dyn_port);
    (Arc::new(tools), hold, flaky, port)
}

fn reply_call(target: Value, correlation: Option<&str>) -> ToolCall {
    let mut args = json!({"channel": "mock-im", "text": "working on it", "target": target});
    if let Some(c) = correlation {
        args["correlation"] = json!(c);
    }
    ToolCall {
        id: "call-1".into(),
        name: "reply".into(),
        arguments: args,
    }
}

async fn invoke(tools: &RouterTools, call: ToolCall) -> Result<ToolResult, ToolError> {
    tools.invoke(call).await
}

/// Lets spawned tasks settle deterministically (no real-time flakiness:
/// a couple of scheduler ticks, generously).
async fn settle() {
    for _ in 0..8 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Gate 1 — flaky adapter: fail twice, succeed on attempt 3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_twice_then_success_delivers_once_with_attempt_3_and_backoff_schedule() {
    let clock = Arc::new(ManualRetryClock::new(10_000));
    let config = OutboxConfig::default(); // 6 attempts, 2s doubling, 60s cap
    let (tools, _hold, flaky, port) = make_tools(Some((config, clock.clone())));

    flaky.fail_n_times(2); // attempts 1 and 2 fail; attempt 3 succeeds

    let target = json!({"chat_id": 1});
    let thread = uuid::Uuid::new_v4();
    let result = invoke(
        &tools,
        reply_call(target.clone(), Some(&thread.to_string())),
    )
    .await
    .expect("third attempt delivers");

    // Exactly one receipt event, stamped with the additive keys.
    let deliveries = port.deliveries();
    assert_eq!(deliveries.len(), 1, "delivered exactly once");
    let payload = &deliveries[0].bytes;
    assert_eq!(payload["channel"], "mock-im");
    assert_eq!(payload["target"], target);
    assert_eq!(payload["attempt"], 3, "{payload}");
    assert_eq!(payload["first_attempt_ms"], 10_000);
    // The legacy keys are unchanged (shape, additive keys only).
    for key in ["channel", "target", "policy", "words", "truncated"] {
        assert!(
            payload.get(key).is_some(),
            "legacy key `{key}` present: {payload}"
        );
    }

    // Three adapter attempts, one captured delivery.
    assert_eq!(flaky.calls(), 3);
    assert_eq!(flaky.deliveries().len(), 3, "one send per attempt");
    assert_eq!(
        flaky.deliveries()[0].content,
        flaky.deliveries()[2].content,
        "every attempt sends the same policy-shaped bytes"
    );

    // The backoff schedule, asserted against the injected clock:
    // 2s after failure 1, 4s after failure 2 (the ManualTurnClock
    // discipline — recorded, never slept).
    assert_eq!(clock.sleeps(), vec![2_000, 4_000]);
    assert_eq!(clock.now_ms(), 10_000 + 2_000 + 4_000);

    // The tool result reports the receipt honestly.
    assert_eq!(result.output["message_id"], "flk-3");
    assert!(!result.is_error);

    // The receipt still parses for the projection (legacy
    // `channel/delivered` consumers are unaffected by the additive
    // keys).
    let tracker = SpeakTracker::from_events(&port.events());
    let key = kian_superagent::speak_key(&target);
    assert!(tracker.has_spoken("mock-im", &key));
}

#[tokio::test]
async fn structured_delivery_stamps_the_same_attempt_keys() {
    let clock = Arc::new(ManualRetryClock::new(1_000));
    let (tools, _hold, flaky, port) = make_tools(Some((
        OutboxConfig {
            max_attempts: 4,
            ..OutboxConfig::default()
        },
        Arc::clone(&clock),
    )));

    flaky.fail_n_times(1);

    let call = ToolCall {
        id: "call-2".into(),
        name: "deliver_structured".into(),
        arguments: json!({
            "channel": "mock-im",
            "target": {"issue": 7},
            "payload": {"background": "b", "changes": "c", "verification": "v"}
        }),
    };
    invoke(&tools, call).await.expect("second attempt delivers");
    let deliveries = port.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].bytes["attempt"], 2);
    assert_eq!(deliveries[0].bytes["first_attempt_ms"], 1_000);
    assert_eq!(clock.sleeps(), vec![2_000]);
    assert_eq!(flaky.calls(), 2);
}

// ---------------------------------------------------------------------------
// Gate 2 — per-(ticket, conversation) ordering
// ---------------------------------------------------------------------------

/// A gated send: counts on first poll, then waits on the shared gate
/// before answering (the "in-flight" stand-).
async fn gated_send(
    gate: Arc<tokio::sync::Mutex<()>>,
    counter: Arc<std::sync::atomic::AtomicU64>,
) -> Result<SendReceipt, ChannelError> {
    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let _held = gate.lock().await;
    Ok(SendReceipt {
        message_id: "gated".into(),
        ts_ms: 1,
    })
}

#[tokio::test]
async fn same_ticket_conversation_waits_other_tickets_never_wait() {
    let clock = Arc::new(ManualRetryClock::new(0));
    let driver = Arc::new(OutboxDriver::new(
        OutboxConfig::default(),
        clock,
        kian_superagent::ChannelRegistry::new(),
    ));

    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let permit = gate.clone().lock_owned().await; // the test holds the gate

    let count_first = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count_later = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count_other_ticket = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count_other_conv = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // The in-flight message: ticket t-1, conversation mock-im:c1.
    let d = Arc::clone(&driver);
    let g = Arc::clone(&gate);
    let c = Arc::clone(&count_first);
    let first = tokio::spawn(async move {
        d.deliver_with_retry("t-1", "mock-im:c1", move || {
            gated_send(Arc::clone(&g), Arc::clone(&c))
        })
        .await
    });
    settle().await;
    assert_eq!(
        count_first.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the first message is in flight"
    );

    // A later message of the SAME (ticket, conversation) waits.
    let d = Arc::clone(&driver);
    let g = Arc::clone(&gate);
    let c = Arc::clone(&count_later);
    let later = tokio::spawn(async move {
        d.deliver_with_retry("t-1", "mock-im:c1", move || {
            gated_send(Arc::clone(&g), Arc::clone(&c))
        })
        .await
    });

    // A DIFFERENT ticket's message to the SAME conversation never waits.
    let d = Arc::clone(&driver);
    let g = Arc::clone(&gate);
    let c = Arc::clone(&count_other_ticket);
    let other_ticket = tokio::spawn(async move {
        d.deliver_with_retry("t-2", "mock-im:c1", move || {
            gated_send(Arc::clone(&g), Arc::clone(&c))
        })
        .await
    });

    // (And a different conversation of the same ticket never waits.)
    let d = Arc::clone(&driver);
    let g = Arc::clone(&gate);
    let c = Arc::clone(&count_other_conv);
    let other_conv = tokio::spawn(async move {
        d.deliver_with_retry("t-1", "mock-im:c2", move || {
            gated_send(Arc::clone(&g), Arc::clone(&c))
        })
        .await
    });

    settle().await;
    assert_eq!(
        count_later.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the later same-key message waits for the earlier one"
    );
    assert_eq!(
        count_other_ticket.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a different ticket's message never waits"
    );
    assert_eq!(
        count_other_conv.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a different conversation's message never waits"
    );

    // Release the gate: the in-flight message completes, the waiting
    // one then goes out — in order, both delivered.
    drop(permit);
    let (r1, r2) = (first.await, later.await);
    assert!(r1.expect("first task ok").expect("first delivered").attempt >= 1);
    assert!(r2.expect("later task ok").expect("later delivered").attempt >= 1);
    assert!(other_ticket.await.expect("ok").expect("delivered").attempt >= 1);
    assert!(other_conv.await.expect("ok").expect("delivered").attempt >= 1);
    assert_eq!(
        count_later.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the later message went out exactly once, after the earlier"
    );
}

// ---------------------------------------------------------------------------
// Gate 3 — max attempts ⇒ parked honestly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn max_attempts_parks_with_stage_audit_counter_and_no_re_ask() {
    let clock = Arc::new(ManualRetryClock::new(5_000));
    let config = OutboxConfig {
        max_attempts: 3,
        ..OutboxConfig::default()
    };
    let (tools, _hold, flaky, port) = make_tools(Some((config, clock.clone())));

    flaky.fail_n_times(100); // always fails — the driver must stop

    let target = json!({"chat_id": 9});
    let thread = uuid::Uuid::new_v4();
    let err = invoke(
        &tools,
        reply_call(target.clone(), Some(&thread.to_string())),
    )
    .await
    .expect_err("parked deliveries are honest tool errors");
    let ToolError::Execution(message) = &err else {
        panic!("expected an execution error, got {err:?}");
    };
    assert!(
        message.starts_with("outbox park:"),
        "the park error carries the rebuild contract prefix: {message}"
    );

    // Bounded: exactly max_attempts sends, then nothing more (the
    // model is not re-asked — one tool invocation, the driver did the
    // attempts).
    assert_eq!(flaky.calls(), 3);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(flaky.calls(), 3, "no further sends after the park");

    // No receipt: nothing was delivered.
    assert!(port.deliveries().is_empty());

    // The backoff between the three attempts: 2s, 4s.
    assert_eq!(clock.sleeps(), vec![2_000, 4_000]);

    // Audit + counter.
    let logs = log_text();
    assert!(
        logs.contains("outbox audit: delivery parked after bounded retries"),
        "expected a park audit line; captured logs:\n{logs}"
    );
    let metrics = tools.outbox_driver().expect("driver installed").metrics();
    assert_eq!(metrics.snapshot()["parks"], 1);

    // The stage projects `delivery-retry` through the real OutboxView
    // wiring: a worker stream for the correlated thread, over a
    // rebuilt outbox status that classifies this intent as parked.
    let router_events = scripted_router_stream_with_park(&thread, &target, 3);
    let status = OutboxStatus::from_events(&router_events);
    assert_eq!(status.entries().len(), 1);
    assert_eq!(
        status.entries()[0].state,
        OutboxState::Parked { attempts: 3 }
    );
    assert_eq!(
        status.parked_lines(),
        vec![format!(
            "mock-im:{} (parked after 3 attempts)",
            serde_json::to_string(&target).unwrap()
        )]
    );
    let mut aliases = std::collections::BTreeMap::new();
    aliases.insert(thread, vec![thread.to_string()]);
    let view = ThreadOutboxView::new(status, aliases);
    let worker_stream = vec![worker_thread_started(&thread)];
    assert_eq!(
        project_stage(&worker_stream, 0, &view),
        TicketStage::DeliveryRetry
    );
    // Without the outbox input the same stream is `working` — the arm
    // is the view's alone (the shape).
    assert_eq!(
        project_stage(&worker_stream, 0, &NoOutbox),
        TicketStage::Working
    );
}

/// A scripted Router stream: the intent (`tool/call` reply, correlated
/// to `thread`) and its parked `tool/result` — the durable trace the
/// rebuild classifies.
fn scripted_router_stream_with_park(
    thread: &uuid::Uuid,
    target: &Value,
    attempts: u32,
) -> Vec<Event> {
    let session = sid(10);
    let mk = |seq, kind, bytes| Event {
        seq,
        session,
        epoch: Epoch(2),
        ts: 1_000 + seq,
        kind,
        bytes,
        version: 0,
    };
    vec![
        mk(
            1,
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "call-1", "name": "reply",
                "arguments": {"channel": "mock-im", "text": "working on it", "target": target,
                              "correlation": thread.to_string()}}}),
        ),
        mk(
            2,
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "call-1", "is_error": true,
                // the live shape — react.rs renders the park
                // as `ToolError::Execution(msg)`, whose Display wraps
                // "tool execution failed: " around the park message.
                // The run2 stream carried exactly this; the bare
                // prefix is the never-live shape.
                "output": {"error": ToolError::Execution(format!(
                    "outbox park: delivery to mock-im:{target} parked after {attempts} attempts: \
                     scripted flaky failure — no further sends; the ticket shows delivery-retry \
                     until released")).to_string()}}}),
        ),
    ]
}

fn worker_thread_started(thread: &uuid::Uuid) -> Event {
    Event {
        seq: 1,
        session: sid(99),
        epoch: Epoch(1),
        ts: 1,
        kind: EventKind::ThreadStarted,
        bytes: json!({"thread_id": thread.to_string()}),
        version: 0,
    }
}

// ---------------------------------------------------------------------------
// Gate 4 — kill the driver mid-retry ⇒ rebuild from the stream re-drives
// ---------------------------------------------------------------------------

#[tokio::test]
async fn killed_mid_retry_intent_re_drives_from_the_stream_rebuild() {
    // The crashed incarnation's Router stream: the intent is logged
    // (log-before-execute) but the process died before any result —
    // exactly a kill -9 mid-retry.
    let thread = uuid::Uuid::new_v4();
    let target = json!({"chat_id": 5});
    let router_session = sid(10);
    let intent = Event {
        seq: 7,
        session: router_session,
        epoch: Epoch(3),
        ts: 7_007,
        kind: EventKind::ToolCall,
        bytes: json!({"role": "assistant", "content": {"id": "call-7", "name": "reply",
            "arguments": {"channel": "mock-im", "text": "here is the result", "target": target,
                          "correlation": thread.to_string()}}}),
        version: 0,
    };

    // The rebooted incarnation's stream port: it already carries the
    // crashed incarnation's intent (rebuilt from the store), so the
    // re-drive's receipt lands at a later seq — the coherent-stream
    // shape the rebuild's receipt pairing reads.
    let (port, dyn_port) = ScriptedPort::shared(router_session);
    port.seed(intent.kind, intent.bytes.clone());
    let pending = outbox_pending(&port.events());
    assert_eq!(pending.len(), 1, "the in-flight intent rebuilds as pending");
    assert_eq!(pending[0].state, OutboxState::Pending);
    assert_eq!(pending[0].ticket, thread.to_string());
    assert_eq!(pending[0].call_id, "call-7");

    // A fresh driver (fresh clock), the adapter behind a registry.
    let clock = Arc::new(ManualRetryClock::new(20_000));
    let flaky = Arc::new(FlakyChannel {
        calls: std::sync::atomic::AtomicU64::new(0),
        script: Mutex::new(VecDeque::new()),
        deliveries: Mutex::new(Vec::new()),
    });
    flaky.fail_n_times(1); // the first re-drive attempt fails, the second lands
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&flaky) as Arc<dyn ChannelAdapter>);
    let driver = OutboxDriver::new(OutboxConfig::default(), clock, channels);

    let outcomes = driver
        .redrive(
            &pending,
            &ImPolicy::default(),
            &StructuredPolicy::default(),
            &port.events(),
            &dyn_port,
        )
        .await;

    // At-least-once: the intent was re-sent; the receipt landed once,
    // stamped with the re-drive attempt.
    assert_eq!(outcomes.len(), 1);
    let (call_id, outcome) = &outcomes[0];
    assert_eq!(call_id, "call-7");
    let delivered = outcome.as_ref().expect("the re-drive eventually delivers");
    assert_eq!(delivered.attempt, 2);
    assert_eq!(flaky.calls(), 2);
    assert_eq!(driver.metrics().snapshot()["redrives"], 1);

    let receipts = port.deliveries();
    assert_eq!(receipts.len(), 1, "the receipt lands exactly once");
    assert_eq!(receipts[0].bytes["attempt"], 2);
    assert_eq!(receipts[0].bytes["first_attempt_ms"], 20_000);
    // The re-driven bytes are the intent's bytes: the same pure
    // ImPolicy shaping the tool path applied.
    assert_eq!(
        receipts[0].bytes["words"],
        im_delivered_payload(
            "mock-im",
            &target,
            &ImPolicy::default().enforce("here is the result")
        )["words"],
        "the re-drive receipt matches the tool path's accounting"
    );
    assert_eq!(
        flaky.deliveries()[0].content,
        json!({"text": ImPolicy::default().enforce("here is the result").text})
    );

    // With the receipt committed, a rebuild of the post-re-drive
    // stream no longer reports the intent — the receipt satisfies the
    // result-less intent (the re-drive path's pairing), so the next
    // boot re-drives nothing. The projection is disposable, not
    // durable.
    assert!(
        outbox_pending(&port.events()).is_empty(),
        "the delivered intent leaves no pending work"
    );
}

// ---------------------------------------------------------------------------
// Compatibility gate — default-off is byte-identical, legacy receipts
// parse unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn absent_config_is_the_legacy_single_attempt_path_byte_identical() {
    let (tools, _hold, flaky, port) = make_tools(None);

    // Success: the receipt payload is exactly the legacy bytes (no
    // attempt keys).
    let target = json!({"chat_id": 3});
    invoke(&tools, reply_call(target.clone(), None))
        .await
        .expect("legacy path delivers");
    let enforced = ImPolicy::default().enforce("working on it");
    assert_eq!(
        port.deliveries()[0].bytes,
        im_delivered_payload("mock-im", &target, &enforced),
        "legacy receipts carry no additive keys"
    );

    // Failure: one attempt, the plain honest adapter error, no retry
    // loop, no park vocabulary, no receipt.
    flaky.fail_n_times(1);
    let err = invoke(&tools, reply_call(json!({"chat_id": 4}), None))
        .await
        .expect_err("the legacy path reports the honest adapter error");
    let ToolError::Execution(message) = &err else {
        panic!("expected an execution error, got {err:?}");
    };
    assert_eq!(message, "delivery failed: scripted flaky failure");
    assert!(!message.starts_with("outbox park:"));
    assert_eq!(flaky.calls(), 2, "one send for each invoke — no retry loop");
    assert_eq!(port.deliveries().len(), 1, "no new receipt for the failure");
}
