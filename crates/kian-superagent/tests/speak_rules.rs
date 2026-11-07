//! Output-side speak-rules (DESIGN §4.2 + §4.7):
//! the *whether-to-send* gate on the Router's `reply` and
//! `deliver_structured` tool paths — attention economics (reference
//! 000: "speak only when a person must act or can use a result;
//! confirm once, stay quiet while working").
//!
//! Four gates, mirrored from the acceptance:
//! 1. each `SpeakCondition` admits/suppresses correctly (pure policy);
//! 2. a suppressed send is an **honest tool error** naming the rule and
//!    condition — zero adapter sends, zero `channel/delivered`, an
//!    audit line and a counter recorded (the deliver-or-discard
//!    discipline: no send ⇒ no receipt event);
//! 3. `ack-first-only` suppresses the second ack to the same topic and
//!    admits one to a new topic, and the ack-once memory survives a
//!    restart because `SpeakTracker` is a disposable projection over
//!    the Router's own `channel/delivered` events;
//! 4. absent config ⇒ every send passes (today's behavior,
//!    byte-identical — the pre-existing router suites stay green
//!    unmodified, verified by the four-gate run).
//!
//! No credentials, no network egress. Log capture follows the
//! `tests/admission_classes.rs` process-global discipline.

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ChannelAdapter, ToolCall, ToolError, ToolProvider, ToolResult};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    InboxWriter, MockChannel, RouterHold, RouterProfile, RouterTools, SpeakCondition, SpeakKind,
    SpeakPolicy, SpeakRule, SpeakTracker, StreamPort, StreamPortError, WorkerSpawner,
    router_template, worker_template,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, OnceLock};

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
/// the Router's committed stream as the tools see it, so the
/// ack-once tracker rebuild (the constraint-1 projection) is
/// exercised against real tool-written events.
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

    /// Pre-seeds the stream (a scripted history — e.g. a previous
    /// incarnation's acks).
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

    fn count_kind(&self, kind: EventKind) -> usize {
        self.events().iter().filter(|e| e.kind == kind).count()
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

/// An inbox fake (admits recorded, never used by the speak-gate tests).
struct NullInbox;

impl InboxWriter for NullInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<kian_superagent::AdmitReceipt, kian_superagent::InboxWriteError>>
    {
        Box::pin(async {
            Ok(kian_superagent::AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

fn make_tools(
    router: SessionId,
    rules: Vec<SpeakRule>,
) -> (
    Arc<RouterTools>,
    Arc<RouterHold>,
    Arc<MockChannel>,
    Arc<ScriptedPort>,
) {
    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        Arc::new(NullInbox) as Arc<dyn InboxWriter>,
    ));
    let mock = Arc::new(MockChannel::new("mock-im"));
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.speak = SpeakPolicy::from_rules(rules);
    let tools = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));
    let (port, dyn_port) = ScriptedPort::shared(router);
    hold.install(dyn_port);
    (tools, hold, mock, port)
}

fn call(name: &str, args: Value) -> ToolCall {
    ToolCall {
        id: "call-1".into(),
        name: name.into(),
        arguments: args,
    }
}

fn reply_call(target: Value) -> ToolCall {
    call(
        "reply",
        json!({"channel": "mock-im", "text": "working on it", "target": target}),
    )
}

fn structured_call(target: Value) -> ToolCall {
    call(
        "deliver_structured",
        json!({
            "channel": "mock-im",
            "target": target,
            "payload": {"background": "b", "changes": "c", "verification": "v"}
        }),
    )
}

fn topic_a() -> Value {
    json!({"chat_id": 1})
}

fn topic_b() -> Value {
    json!({"chat_id": 2})
}

async fn invoke(tools: &RouterTools, call: ToolCall) -> Result<ToolResult, ToolError> {
    tools.invoke(call).await
}

// ---------------------------------------------------------------------------
// Gate 1 — the pure policy: each condition admits/suppresses correctly
// ---------------------------------------------------------------------------

fn verdict(
    rule: &SpeakRule,
    kind: SpeakKind,
    topic: &Value,
    tracker: &SpeakTracker,
) -> kian_superagent::SpeakVerdict {
    let key = kian_superagent::speak_key(topic);
    let context = kian_superagent::SpeakContext {
        channel: &rule.channel,
        topic: Some(key.as_str()),
        kind,
        tracker,
    };
    SpeakPolicy::check(rule, &context)
}

use kian_superagent::SpeakVerdict;

fn admits(v: &SpeakVerdict) -> bool {
    matches!(v, SpeakVerdict::Admit)
}

fn suppressed_condition(v: &SpeakVerdict) -> SpeakCondition {
    match v {
        SpeakVerdict::Admit => panic!("expected a suppression, got Admit"),
        SpeakVerdict::Suppress { condition } => *condition,
    }
}

#[test]
fn always_and_never_admit_or_suppress_both_kinds() {
    let tracker = SpeakTracker::new();
    let always = SpeakRule {
        channel: "mock-im".into(),
        when: SpeakCondition::Always,
        per_topic_ack: false,
    };
    assert!(admits(&verdict(
        &always,
        SpeakKind::Decision,
        &topic_a(),
        &tracker
    )));
    assert!(admits(&verdict(
        &always,
        SpeakKind::Result,
        &topic_a(),
        &tracker
    )));

    let never = SpeakRule {
        channel: "mock-im".into(),
        when: SpeakCondition::Never,
        per_topic_ack: false,
    };
    let v = verdict(&never, SpeakKind::Decision, &topic_a(), &tracker);
    assert_eq!(suppressed_condition(&v), SpeakCondition::Never);
    let v = verdict(&never, SpeakKind::Result, &topic_a(), &tracker);
    assert_eq!(suppressed_condition(&v), SpeakCondition::Never);
}

#[test]
fn decision_needed_admits_only_decisions_result_ready_only_results() {
    let tracker = SpeakTracker::new();
    let rule = |when: SpeakCondition| SpeakRule {
        channel: "mock-im".into(),
        when,
        per_topic_ack: false,
    };

    let decision = rule(SpeakCondition::DecisionNeeded);
    assert!(admits(&verdict(
        &decision,
        SpeakKind::Decision,
        &topic_a(),
        &tracker
    )));
    assert_eq!(
        suppressed_condition(&verdict(&decision, SpeakKind::Result, &topic_a(), &tracker)),
        SpeakCondition::DecisionNeeded
    );

    let result = rule(SpeakCondition::ResultReady);
    assert!(admits(&verdict(
        &result,
        SpeakKind::Result,
        &topic_a(),
        &tracker
    )));
    assert_eq!(
        suppressed_condition(&verdict(&result, SpeakKind::Decision, &topic_a(), &tracker)),
        SpeakCondition::ResultReady
    );
}

#[test]
fn ack_first_only_is_topic_scoped_or_channel_scoped_by_config() {
    let mut tracker = SpeakTracker::new();

    // per_topic_ack: the first send per topic passes, the second to the
    // same topic is suppressed, a new topic passes.
    let per_topic = SpeakRule {
        channel: "mock-im".into(),
        when: SpeakCondition::AckFirstOnly,
        per_topic_ack: true,
    };
    assert!(admits(&verdict(
        &per_topic,
        SpeakKind::Decision,
        &topic_a(),
        &tracker
    )));
    tracker.record("mock-im", &kian_superagent::speak_key(&topic_a()));
    assert_eq!(
        suppressed_condition(&verdict(
            &per_topic,
            SpeakKind::Decision,
            &topic_a(),
            &tracker
        )),
        SpeakCondition::AckFirstOnly
    );
    // A different topic is a different conversation: its first ack passes.
    assert!(admits(&verdict(
        &per_topic,
        SpeakKind::Decision,
        &topic_b(),
        &tracker
    )));

    // Channel-level (per_topic_ack = false): one ack per channel, any target.
    let per_channel = SpeakRule {
        channel: "mock-im".into(),
        when: SpeakCondition::AckFirstOnly,
        per_topic_ack: false,
    };
    let mut fresh = SpeakTracker::new();
    assert!(admits(&verdict(
        &per_channel,
        SpeakKind::Decision,
        &topic_a(),
        &fresh
    )));
    fresh.record("mock-im", &kian_superagent::speak_key(&topic_a()));
    assert_eq!(
        suppressed_condition(&verdict(
            &per_channel,
            SpeakKind::Decision,
            &topic_b(),
            &fresh
        )),
        SpeakCondition::AckFirstOnly
    );
}

#[test]
fn absent_rules_admit_everything_and_conditions_have_stable_codes() {
    let policy = SpeakPolicy::default();
    assert!(policy.rule_for("mock-im").is_none());
    // The closed vocabulary is exactly the five conditions, with
    // stable serde/audit codes.
    let codes: Vec<&str> = SpeakCondition::ALL.iter().map(|c| c.code()).collect();
    assert_eq!(
        codes,
        vec![
            "always",
            "ack-first-only",
            "decision-needed",
            "result-ready",
            "never"
        ]
    );
    for code in SpeakCondition::ALL {
        assert_eq!(SpeakCondition::from_code(code.code()), Some(code));
        // The serde name is the code (one stable spelling everywhere).
        let wire = serde_json::to_string(&code).unwrap();
        assert_eq!(wire.trim_matches('"'), code.code());
        let parsed: SpeakCondition = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed, code);
    }
    assert_eq!(SpeakCondition::from_code("sometimes"), None);
}

#[test]
fn speak_tracker_rebuilt_from_a_scripted_stream_equals_the_runtime_tracker() {
    // A scripted history: two acks (two distinct topics) on mock-im,
    // one delivery on another channel, plus events the tracker must
    // ignore.
    let scripted = vec![
        Event {
            seq: 1,
            session: sid(10),
            epoch: Epoch(2),
            ts: 1_001,
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: 0,
        },
        Event {
            seq: 2,
            session: sid(10),
            epoch: Epoch(2),
            ts: 1_002,
            kind: EventKind::ChannelDelivered,
            bytes: json!({
                "channel": "mock-im",
                "target": {"chat_id": 1},
                "policy": "im",
                "words": 3,
                "truncated": false
            }),
            version: 0,
        },
        Event {
            seq: 3,
            session: sid(10),
            epoch: Epoch(2),
            ts: 1_003,
            kind: EventKind::ToolCall,
            bytes: json!({"tool": "reply"}),
            version: 0,
        },
        Event {
            seq: 4,
            session: sid(10),
            epoch: Epoch(2),
            ts: 1_004,
            kind: EventKind::ChannelDelivered,
            bytes: json!({
                "channel": "mock-im",
                "target": {"chat_id": 2},
                "policy": "structured",
                "truncated": false
            }),
            version: 0,
        },
        Event {
            seq: 5,
            session: sid(10),
            epoch: Epoch(2),
            ts: 1_005,
            kind: EventKind::ChannelDelivered,
            bytes: json!({
                "channel": "github",
                "target": {"issue": 42},
                "policy": "structured",
                "truncated": false
            }),
            version: 0,
        },
    ];

    // The runtime tracker: the same sends, recorded as they happened.
    let mut runtime = SpeakTracker::new();
    runtime.record("mock-im", &kian_superagent::speak_key(&topic_a()));
    runtime.record("mock-im", &kian_superagent::speak_key(&topic_b()));
    runtime.record("github", &kian_superagent::speak_key(&json!({"issue": 42})));

    let rebuilt = SpeakTracker::from_events(&scripted);
    assert_eq!(rebuilt, runtime);
    // The keys are honest: per (channel, topic) pairs.
    assert!(rebuilt.has_spoken("mock-im", &kian_superagent::speak_key(&topic_a())));
    assert!(!rebuilt.has_spoken(
        "mock-im",
        &kian_superagent::speak_key(&json!({"chat_id": 9}))
    ));
    assert!(rebuilt.has_spoken_on_channel("github"));
    assert!(!rebuilt.has_spoken_on_channel("lark"));
}

// ---------------------------------------------------------------------------
// Gate 2 — a suppressed send is an honest tool error, nothing is sent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn suppressed_send_is_an_honest_error_zero_sends_zero_delivered_counter_and_audit() {
    let (tools, _hold, mock, port) = make_tools(
        sid(10),
        vec![SpeakRule {
            channel: "mock-im".into(),
            when: SpeakCondition::Never,
            per_topic_ack: false,
        }],
    );
    // Install the log capture before the send (the global subscriber
    // must exist when the audit line is emitted).
    let _ = logs();

    let err = invoke(&tools, reply_call(topic_a())).await.unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("speak rule"),
        "error names the rule, got: {text}"
    );
    assert!(
        text.contains("never"),
        "error names the condition, got: {text}"
    );
    assert!(
        text.contains("mock-im"),
        "error names the channel, got: {text}"
    );

    // Zero adapter sends, zero channel/delivered (deliver-or-discard).
    assert!(mock.deliveries().is_empty(), "no adapter send happened");
    assert_eq!(port.count_kind(EventKind::ChannelDelivered), 0);

    // Counter + audit recorded.
    let metrics = tools.speak_metrics();
    assert_eq!(metrics.suppressed(), 1);
    assert_eq!(metrics.snapshot()["by_condition"]["never"], json!(1));

    let logs = log_text();
    assert!(
        logs.contains("speak audit: send suppressed"),
        "expected the suppression audit line, got:\n{logs}"
    );
}

#[tokio::test]
async fn suppressed_structured_delivery_never_reaches_the_adapter() {
    let (tools, _hold, mock, port) = make_tools(
        sid(11),
        vec![SpeakRule {
            channel: "mock-im".into(),
            when: SpeakCondition::DecisionNeeded,
            per_topic_ack: false,
        }],
    );

    // The payload is valid — the only thing refusing it is the gate.
    let err = invoke(&tools, structured_call(topic_a()))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("decision-needed"));
    assert!(mock.deliveries().is_empty());
    assert_eq!(port.count_kind(EventKind::ChannelDelivered), 0);

    // The complementary kind passes the same channel.
    invoke(&tools, reply_call(topic_a()))
        .await
        .expect("reply passes");
    assert_eq!(mock.deliveries().len(), 1);
    assert_eq!(port.count_kind(EventKind::ChannelDelivered), 1);
}

// ---------------------------------------------------------------------------
// Gate 3 — ack-first-only: confirm once, per topic, across restarts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ack_first_only_suppresses_the_second_ack_and_admits_a_new_topic() {
    let (tools, _hold, mock, port) = make_tools(
        sid(12),
        vec![SpeakRule {
            channel: "mock-im".into(),
            when: SpeakCondition::AckFirstOnly,
            per_topic_ack: true,
        }],
    );

    // First ack to topic A: admitted.
    invoke(&tools, reply_call(topic_a()))
        .await
        .expect("first ack passes");

    // Second ack to the same topic: suppressed, honestly.
    let err = invoke(&tools, reply_call(topic_a())).await.unwrap_err();
    assert!(err.to_string().contains("ack-first-only"));

    // A new topic B: its first ack is admitted.
    invoke(&tools, reply_call(topic_b()))
        .await
        .expect("new topic passes");

    assert_eq!(mock.deliveries().len(), 2);
    assert_eq!(port.count_kind(EventKind::ChannelDelivered), 2);
    assert_eq!(tools.speak_metrics().suppressed(), 1);

    // The runtime tracker agrees with a rebuild from the stream (the
    // acceptance's equality, through the real tool path).
    assert_eq!(
        tools.speak_tracker_snapshot(),
        SpeakTracker::from_events(&port.events())
    );
}

#[tokio::test]
async fn ack_first_only_survives_a_restart_via_the_stream_rebuild() {
    let router = sid(13);
    // The tracker is a disposable projection: a fresh RouterTools over
    // the same stream (a reboot) still knows topic A was acknowledged.
    let (tools, hold, _mock, port) = make_tools(
        router,
        vec![SpeakRule {
            channel: "mock-im".into(),
            when: SpeakCondition::AckFirstOnly,
            per_topic_ack: true,
        }],
    );
    invoke(&tools, reply_call(topic_a()))
        .await
        .expect("ack passes");
    assert_eq!(port.count_kind(EventKind::ChannelDelivered), 1);

    // "Reboot": a brand-new tools instance (fresh tracker slot) over
    // the same hold and the same committed stream.
    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        Arc::new(NullInbox) as Arc<dyn InboxWriter>,
    ));
    let mock2 = Arc::new(MockChannel::new("mock-im"));
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock2) as Arc<dyn ChannelAdapter>);
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.speak = SpeakPolicy::from_rules(vec![SpeakRule {
        channel: "mock-im".into(),
        when: SpeakCondition::AckFirstOnly,
        per_topic_ack: true,
    }]);
    let rebooted = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));

    let err = invoke(&rebooted, reply_call(topic_a())).await.unwrap_err();
    assert!(
        err.to_string().contains("ack-first-only"),
        "the rebuilt tracker suppressed the repeat ack, got: {err}"
    );
    assert!(mock2.deliveries().is_empty());
    // A new topic is still admitted after the rebuild.
    invoke(&rebooted, reply_call(topic_b()))
        .await
        .expect("new topic after restart");
}

// ---------------------------------------------------------------------------
// Gate 4 — absent config: today's behavior, byte-identical
// ---------------------------------------------------------------------------

#[tokio::test]
async fn absent_rules_every_send_passes_with_today_payloads() {
    let (tools, _hold, mock, port) = make_tools(sid(14), Vec::new());

    let reply = invoke(&tools, reply_call(topic_a()))
        .await
        .expect("reply passes");
    assert!(!reply.is_error);
    assert_eq!(reply.output["words"], json!(3));
    assert_eq!(reply.output["truncated"], json!(false));

    let structured = invoke(&tools, structured_call(topic_a()))
        .await
        .expect("structured passes");
    assert_eq!(structured.output["sections_delivered"], json!(true));

    assert_eq!(mock.deliveries().len(), 2);
    // The delivered payloads are exactly the §2 schema (no speak
    // keys, no new fields) — absent config is byte-identical.
    let events = port.events();
    let delivered: Vec<&Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::ChannelDelivered)
        .collect();
    assert_eq!(delivered.len(), 2);
    assert_eq!(
        delivered[0].bytes,
        json!({
            "channel": "mock-im",
            "target": {"chat_id": 1},
            "policy": "im",
            "words": 3,
            "truncated": false,
        })
    );
    assert_eq!(
        delivered[1].bytes,
        json!({
            "channel": "mock-im",
            "target": {"chat_id": 1},
            "policy": "structured",
            "truncated": false,
        })
    );
    assert_eq!(tools.speak_metrics().suppressed(), 0);
}
