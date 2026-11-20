//! Delegation mandate (ADR-012, DESIGN §4.7): the
//! authority object on the frozen delegation wrapper. Four gates:
//!
//! 1. **Wrapper form** — the mandated wrapper is exactly
//!    `delegation_content` plus one additive `"mandate"` key (byte
//!    golden); legacy wrappers parse with `None` and the frozen
//!    `Delegation` shape is untouched.
//! 2. **Issuance** — `spawn_worker` (the single issuance point) stamps
//!    `strength` from the triggering input's provenance signal:
//!    `explicit` for the adapter-declared `explicit: true` payload
//!    field, `inferred` otherwise (the discipline — the raw's text
//!    is never parsed for consent).
//! 3. **Durable + model-visible** — the mandate appears verbatim in
//!    the worker's first message (the admitted wrapper) and inside the
//!    `thread/started` task payload, on both sides of the delegation
//!    pair (the `delegation_pairs` audit helper).
//! 4. **Revalidation** (with 's live routing) — a follow-up on a
//!    live worker's topic reaches the worker when the mandate is
//!    `explicit` (even under a changed policy version) or `inferred`
//!    under the current version; `inferred` under a *changed* version
//!    releases to the Router as fresh evaluation, audited
//!    `released_to_eval`. Speculative delegations carry mandates too
//!    (the markers compose).
//!
//! No credentials, no network egress. Log capture follows the
//! `tests/followup_ownership.rs` process-global discipline (own test
//! binary, own global subscriber).

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ChannelAdapter, ChannelEvent, ToolCall, ToolProvider};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, CorrelateBy, InboxWriteError, InboxWriter, MandatePolicy,
    MockChannel, PassthroughRegistry, Route, RouterHold, RouterProfile, RouterTools,
    RoutingOutcome, RuleMatcher, StreamPort, StreamPortError, SubscriptionRule,
    SubscriptionRuleSet, SubscriptionService, WorkerSpawner, delegation_content,
    delegation_is_speculative, delegation_mandate, delegation_pairs, mandate_delegation_content,
    parse_delegation, router_template, worker_template,
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
        tracing::subscriber::set_global_default(subscriber)
            .expect("mandate owns its process's global subscriber");
        buf
    })
    .clone()
}

fn log_text() -> String {
    String::from_utf8_lossy(&logs().lock().unwrap()).into_owned()
}

/// Lines of the captured log that contain every needle.
fn audit_lines<'a>(text: &'a str, needles: &[&'a str]) -> Vec<&'a str> {
    text.lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .collect()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(uuid::Uuid::from_u128(n))
}

fn tid(n: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(n)
}

fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(2),
        ts: 1_000 + seq,
        kind,
        bytes,
        version: 0,
    }
}

/// An inbox fake recording full admit payloads (the wrapper bytes —
/// the worker's first message) per session.
struct RecordingInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl RecordingInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

/// A stream port recording everything emitted (the Router's stream as
/// the tools see it).
struct RecordingPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
}

impl RecordingPort {
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(Vec::new()),
        });
        let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
        (port, dyn_port)
    }

    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

impl StreamPort for RecordingPort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
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
        Box::pin(async { Ok(()) })
    }
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

/// A channel-event wrapper task the Router passes to `spawn_worker`
/// when dispatching on a channel event — with or without the
/// adapter-declared explicit signal.
fn channel_task(event_id: &str, topic: &str, explicit: bool) -> Value {
    let mut payload = json!({
        "action": "opened",
        "sender": {"login": "reporter", "type": "User"},
    });
    if explicit {
        // The adapter-declared signal (github/lark stamp exactly this
        // top-level field on @-mentions).
        payload["explicit"] = json!(true);
    }
    kian_superagent::provenance::wrap_channel_event(&ChannelEvent {
        channel: "github".into(),
        event_id: event_id.into(),
        topic: Some(topic.into()),
        sender: None,
        group: None,
        payload,
        ts_ms: 1,
    })
}

/// The mandate issuance policy fixture: the stock worker template is
/// bounded; nothing else is.
fn mandate_policy() -> MandatePolicy {
    MandatePolicy {
        scopes: std::collections::BTreeMap::from([(
            "kian-worker".to_string(),
            "draft a triage comment for the issue; never post".to_string(),
        )]),
        default_scope: None,
    }
}

/// The `spawn_worker` tool surface with mandate issuance on and the
/// shared registry installed.
fn mandate_tools(
    policy: Option<MandatePolicy>,
    registry: Arc<std::sync::RwLock<PassthroughRegistry>>,
) -> (Arc<RouterTools>, Arc<RecordingInbox>, Arc<RecordingPort>) {
    let router = sid(1);
    let inbox = RecordingInbox::new();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    ));
    let mut channels = ChannelRegistry::new();
    channels.register(
        "mock-im",
        Arc::new(MockChannel::new("mock-im")) as Arc<dyn ChannelAdapter>,
    );
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.mandate = policy;
    let tools = Arc::new(
        RouterTools::new(
            spawner,
            channels,
            &profile,
            hold.clone(),
            kian_superagent::ResidentBinding::new(profile.session),
        )
        .with_followup_registry(registry),
    );
    let (port, dyn_port) = RecordingPort::shared(router);
    hold.install(dyn_port);
    (tools, inbox, port)
}

fn spawn_call(task: Value) -> ToolCall {
    ToolCall {
        id: "call-1".into(),
        name: "spawn_worker".into(),
        arguments: json!({"task": task}),
    }
}

/// The worker-side stream of a spawned worker: the admitted wrapper as
/// its first input, then the worker-side `thread/started` carrying the
/// same task message (the `lineage_plan::Emit` bytes).
fn worker_stream(worker: SessionId, router_events: &[Event]) -> Vec<Event> {
    let started = router_events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted)
        .expect("the router stream carries thread/started");
    let task = started.bytes.get("task").expect("task payload").clone();
    let wrapper = task.get("content").expect("wrapper content").clone();
    vec![
        event(
            worker,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": wrapper}),
        ),
        event(worker, 2, EventKind::ThreadStarted, started.bytes.clone()),
    ]
}

// ---------------------------------------------------------------------------
// Gate 1 — the wrapper form: exactly delegation_content + "mandate"
// ---------------------------------------------------------------------------

/// Byte golden: the mandated wrapper is the legacy bytes plus one
/// additive key, serializing to a stable form; the legacy builder
/// emits no mandate key at all and still parses as a delegation.
#[test]
fn mandate_wrapper_form_is_exactly_delegation_content_plus_mandate() {
    let mandate = json!({
        "strength": "explicit",
        "scope": "reply on the ci channel about build failures",
        "granted_by": {"login": "reporter", "type": "User"},
        "source": {"channel": "github", "topic": "acme/repo#7"},
        "policy_version": "v1",
    });
    let content = mandate_delegation_content(tid(0xaa), Some(json!("evt-9")), json!("t"), &mandate);
    assert_eq!(content.as_object().map(serde_json::Map::len), Some(4));
    // The byte golden: serde_json's BTree-map order is deterministic.
    let expected = format!(
        "{{\"correlation\":\"evt-9\",\"mandate\":{{\"granted_by\":{{\"login\":\"reporter\",\
         \"type\":\"User\"}},\"policy_version\":\"v1\",\"scope\":\"reply on the ci channel about \
         build failures\",\"source\":{{\"channel\":\"github\",\"topic\":\"acme/repo#7\"}},\
         \"strength\":\"explicit\"}},\"task\":\"t\",\"thread_id\":\"{}\"}}",
        tid(0xaa)
    );
    assert_eq!(content.to_string(), expected);

    // The frozen shape round-trips: parse_delegation is unchanged and
    // the companion parser reads the object back verbatim.
    let parsed = parse_delegation(&content).expect("mandate wrapper is a delegation");
    assert_eq!(parsed.thread_id, tid(0xaa));
    assert_eq!(parsed.correlation, Some(json!("evt-9")));
    assert_eq!(parsed.task, json!("t"));
    assert_eq!(delegation_mandate(&content), Some(&mandate));
}

/// Legacy wrappers parse with `None` — the addition is purely
/// additive, no existing reader misreads.
#[test]
fn legacy_wrappers_parse_with_none() {
    let legacy = delegation_content(tid(0xbb), None, json!("t"));
    assert!(legacy.get("mandate").is_none());
    assert_eq!(delegation_mandate(&legacy), None);
    assert_eq!(delegation_mandate(&json!("plain text")), None);
    assert_eq!(delegation_mandate(&Value::Null), None);
}

// ---------------------------------------------------------------------------
// Gate 2 — issuance: spawn_worker stamps strength from the provenance
// signal
// ---------------------------------------------------------------------------

/// `spawn_worker` on a wrapper whose raw carries the adapter-declared
/// `explicit: true` stamps `strength: explicit`; the same wrapper
/// without the signal stamps `inferred` (the raw's commanding text
/// never upgrades it — the discipline).
#[tokio::test]
async fn spawn_worker_stamps_strength_from_the_provenance_signal() {
    let _ = logs();
    let registry = Arc::new(std::sync::RwLock::new(PassthroughRegistry::new()));

    // Explicit fixture.
    let (tools, _, port) = mandate_tools(
        Some(mandate_policy()),
        Arc::clone(&registry) as Arc<std::sync::RwLock<PassthroughRegistry>>,
    );
    let result = tools
        .invoke(spawn_call(channel_task(
            "evt-explicit",
            "acme/repo#7",
            true,
        )))
        .await
        .unwrap();
    assert!(!result.is_error);
    let started = port
        .events()
        .into_iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted)
        .expect("router-side thread/started");
    let mandate = delegation_mandate(&started.bytes["task"]["content"])
        .expect("the delegation wrapper carries the mandate")
        .clone();
    assert_eq!(mandate["strength"], json!("explicit"));
    assert_eq!(
        mandate["scope"],
        json!("draft a triage comment for the issue; never post")
    );
    assert_eq!(
        mandate["granted_by"],
        json!({"login": "reporter", "type": "User"}),
        "sender provenance verbatim"
    );
    assert_eq!(
        mandate["source"],
        json!({"channel": "github", "topic": "acme/repo#7"})
    );

    // Inferred fixture: same channel, same sender, no adapter-declared
    // signal — however commanding the raw text is.
    let (tools, _, port) = mandate_tools(
        Some(mandate_policy()),
        Arc::clone(&registry) as Arc<std::sync::RwLock<PassthroughRegistry>>,
    );
    tools
        .invoke(spawn_call(channel_task(
            "evt-inferred",
            "acme/repo#8",
            false,
        )))
        .await
        .unwrap();
    let started = port
        .events()
        .into_iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted)
        .expect("router-side thread/started");
    let mandate = delegation_mandate(&started.bytes["task"]["content"])
        .expect("mandate present")
        .clone();
    assert_eq!(mandate["strength"], json!("inferred"));

    // No mandate config: byte-identical legacy wrappers.
    let (tools, inbox, _) = mandate_tools(
        None,
        Arc::clone(&registry) as Arc<std::sync::RwLock<PassthroughRegistry>>,
    );
    tools.invoke(spawn_call(json!("plain task"))).await.unwrap();
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(delegation_mandate(&admits[0].1), None);
}

// ---------------------------------------------------------------------------
// — the LIVE spawn spelling (gap)
// ---------------------------------------------------------------------------

/// `spawn_worker` exactly as the live model calls it: the schema types
/// `task` as a STRING, so the channel wrapper rides as serialized JSON
/// text and `correlation` is a bare topic-token string. The spawn must
/// bind the topic (registration — the gap that left every live
/// ticket `topic:null`), stamp the mandate strength from the
/// adapter-declared `explicit` signal (not `inferred`), attribute the
/// mandate source to the triggering channel/topic (not `router`/null),
/// and the registry rebuild over the emitted streams must equal the
/// runtime registration (one derivation, two readers).
#[tokio::test]
async fn live_string_task_and_correlation_bind_the_topic_and_attribute_the_mandate() {
    let _ = logs();
    let registry = Arc::new(std::sync::RwLock::new(PassthroughRegistry::new()));
    let (tools, _inbox, port) = mandate_tools(
        Some(mandate_policy()),
        Arc::clone(&registry) as Arc<std::sync::RwLock<PassthroughRegistry>>,
    );
    let wrapper = channel_task("evt-live", "github:acme/repo#9", true);
    let call = ToolCall {
        id: "call-live".into(),
        name: "spawn_worker".into(),
        arguments: json!({
            "task": wrapper.to_string(),
            "correlation": "issue-github:acme/repo#9",
        }),
    };
    let result = tools.invoke(call).await.unwrap();
    assert!(!result.is_error);
    let worker = SessionId(
        uuid::Uuid::parse_str(result.output["worker"].as_str().expect("worker id")).expect("uuid"),
    );

    // The topic binds: the spawned worker is the live Executor of the
    // wrapper's topic (the marker's topic wins over the token).
    assert_eq!(
        registry.read().unwrap().live_owner("github:acme/repo#9"),
        Some(worker),
        "the live string spelling must register the worker on the topic"
    );

    // The mandate stamps strength from the adapter-declared signal and
    // attributes the triggering channel/topic.
    let started = port
        .events()
        .into_iter()
        .find(|ev| ev.kind == EventKind::ThreadStarted)
        .expect("router-side thread/started");
    let mandate = delegation_mandate(&started.bytes["task"]["content"])
        .expect("the delegation wrapper carries the mandate")
        .clone();
    assert_eq!(mandate["strength"], json!("explicit"));
    assert_eq!(
        mandate["source"],
        json!({"channel": "github", "topic": "github:acme/repo#9"})
    );
    assert_eq!(
        mandate["granted_by"],
        json!({"login": "reporter", "type": "User"}),
        "sender provenance verbatim off the string form"
    );

    // One derivation, two readers: the rebuild over the Router's
    // emitted stream plus the worker's own stream equals the runtime
    // registration — the projection survives a restart.
    let mut events = port.events();
    let worker_events = worker_stream(worker, &events);
    events.extend(worker_events);
    let rebuilt = PassthroughRegistry::rebuild(&events);
    assert_eq!(
        rebuilt.resolve_live("github:acme/repo#9"),
        std::collections::BTreeSet::from([worker]),
        "the rebuild re-derives the topic binding from the streams"
    );
    assert_eq!(
        rebuilt.mandate_of(&worker),
        Some(&mandate),
        "the rebuild re-derives the mandate verbatim"
    );
}

// ---------------------------------------------------------------------------
// Gate 3 — durable + model-visible: the delegation-pair audit
// ---------------------------------------------------------------------------

/// The mandate appears verbatim in the worker's first message (the
/// admitted wrapper) and in the `thread/started` task payload on both
/// sides of the delegation pair (the `delegation_pairs` helper), and
/// the issuance audit line names the strength.
#[tokio::test]
async fn mandate_is_verbatim_in_first_message_and_thread_started() {
    let _ = logs();
    let registry = Arc::new(std::sync::RwLock::new(PassthroughRegistry::new()));
    let (tools, inbox, port) = mandate_tools(
        Some(mandate_policy()),
        registry as Arc<std::sync::RwLock<PassthroughRegistry>>,
    );
    tools
        .invoke(spawn_call(channel_task("evt-9", "acme/repo#7", true)))
        .await
        .unwrap();

    // The worker's first message (the durable admitted row) is the
    // wrapper carrying the mandate verbatim — model-visible by design.
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    let (worker, wrapper, key) = &admits[0];
    assert_eq!(
        key.as_deref(),
        Some(format!("thread:{}", parse_delegation(wrapper).unwrap().thread_id).as_str())
    );
    let mandate = delegation_mandate(wrapper)
        .expect("mandate in first message")
        .clone();

    // The delegation pair across both streams carries the same task
    // payload — the mandate is audit-visible forever on both sides.
    let router_events = port.events();
    let worker_events = worker_stream(*worker, &router_events);
    let pairs = delegation_pairs(&router_events, &worker_events);
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].worker, *worker);
    assert_eq!(
        delegation_mandate(&pairs[0].task["content"]),
        Some(&mandate),
        "the thread/started task payload carries the mandate verbatim"
    );

    // The issuance audit line names the strength (honest console trace).
    assert_eq!(
        audit_lines(
            &log_text(),
            &[
                "mandate audit: issued at spawn",
                "strength=explicit",
                "policy_version=none",
                &worker.0.to_string(),
            ]
        )
        .len(),
        1,
        "expected one issuance audit line for this worker"
    );
}

// ---------------------------------------------------------------------------
// Gate 4 — revalidation on follow-up routing (with live routing)
// ---------------------------------------------------------------------------

/// The passthrough rules of the routing tests: `github` events
/// correlate by topic.
fn rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![SubscriptionRule {
        channel: "github".into(),
        matcher: RuleMatcher::Topic("acme/repo#7".into()),
        route: Route::Passthrough {
            correlate_by: CorrelateBy::Topic,
        },
    }])
}

fn follow_up_event(event_id: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "github".into(),
        event_id: event_id.into(),
        topic: Some("acme/repo#7".into()),
        sender: None,
        group: None,
        payload: json!({"action": "commented"}),
        ts_ms: 2,
    }
}

/// A registry with one live worker owning the topic under `mandate`.
fn live_registry(worker: SessionId, mandate: Value) -> Arc<std::sync::RwLock<PassthroughRegistry>> {
    let mut registry = PassthroughRegistry::new();
    registry.register("acme/repo#7", worker);
    registry.register_mandate(worker, mandate);
    Arc::new(std::sync::RwLock::new(registry))
}

/// An explicit mandate passes revalidation even under a changed policy
/// version: the follow-up reaches the live worker, zero Router rows.
#[tokio::test]
async fn explicit_follow_up_reaches_the_live_worker_under_a_changed_version() {
    let _ = logs();
    let router = sid(1);
    let worker = sid(2);
    let registry = live_registry(
        worker,
        json!({
            "strength": "explicit",
            "scope": "reply on the ci channel",
            "granted_by": {"login": "reporter", "type": "User"},
            "source": {"channel": "github", "topic": "acme/repo#7"},
            "policy_version": "v1",
        }),
    );
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_shared_registry(registry)
    // The pointer moved past the mandate's stamped v1 — explicit
    // authority is not version-bound (ADR-012).
    .with_policy_pointer(Some("v2".into()));

    let outcome = service
        .handle_event(follow_up_event("evt-follow"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: worker,
            deduplicated: false
        }
    );
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(admits[0].0, worker, "the live worker owns the follow-up");
    // No release audit for the explicit mandate.
    assert!(audit_lines(&log_text(), &["released_to_eval", "evt-follow"]).is_empty());
}

/// An inferred mandate under a changed policy version releases the
/// follow-up to the Router — fresh evaluation, honestly audited with
/// the closed reason code and the stale version named.
#[tokio::test]
async fn inferred_under_changed_version_releases_to_the_router() {
    let _ = logs();
    let router = sid(1);
    let worker = sid(2);
    let registry = live_registry(
        worker,
        json!({
            "strength": "inferred",
            "scope": "draft a triage comment; never post",
            "granted_by": {"login": "reporter", "type": "User"},
            "source": {"channel": "github", "topic": "acme/repo#7"},
            "policy_version": "v1",
        }),
    );
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_shared_registry(registry)
    .with_policy_pointer(Some("v2".into()));

    let outcome = service
        .handle_event(follow_up_event("evt-stale"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        }
    );
    // Exactly one row, for the Router; the stale-mandate worker is not
    // admitted.
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(
        admits,
        vec![(
            router,
            json!({"kian": {"type": "channel_event", "channel": "github", "topic": "acme/repo#7", "event_id": "evt-stale", "raw": {"action": "commented"}}}),
            Some("evt-stale".into())
        )]
    );
    // Honest audit: the release line names the worker, the stale
    // version, and the closed reason code.
    let text = log_text();
    assert_eq!(
        audit_lines(
            &text,
            &[
                "mandate revalidation released live subscriber",
                &worker.0.to_string(),
                "from_policy=v1",
                "reason=released_to_eval",
                "evt-stale",
            ]
        )
        .len(),
        1,
        "expected the per-worker release audit line"
    );
    assert_eq!(
        audit_lines(
            &text,
            &[
                "topic released to fresh evaluation",
                "mandate revalidation released every live subscriber",
                "evt-stale",
            ]
        )
        .len(),
        1,
        "expected the release-to-router audit line"
    );
}

/// An inferred mandate under its *current* version passes; a legacy
/// worker (no mandate) always passes — legacy authority is never
/// retroactively narrowed.
#[tokio::test]
async fn inferred_under_current_version_and_legacy_workers_pass() {
    let _ = logs();
    let router = sid(1);
    let worker = sid(2);
    let registry = live_registry(
        worker,
        json!({
            "strength": "inferred",
            "scope": "draft a triage comment; never post",
            "granted_by": null,
            "source": {"channel": "github", "topic": "acme/repo#7"},
            "policy_version": "v1",
        }),
    );
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_shared_registry(registry)
    .with_policy_pointer(Some("v1".into()));
    let outcome = service
        .handle_event(follow_up_event("evt-current"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: worker,
            deduplicated: false
        }
    );

    // A legacy worker (no mandate recorded) passes under any pointer.
    let mut legacy_registry = PassthroughRegistry::new();
    legacy_registry.register("acme/repo#7", worker);
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_shared_registry(Arc::new(std::sync::RwLock::new(legacy_registry)))
    .with_policy_pointer(Some("v9".into()));
    let outcome = service
        .handle_event(follow_up_event("evt-legacy"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: worker,
            deduplicated: false
        }
    );
}

// ---------------------------------------------------------------------------
// Gate 4b — speculative delegations carry mandates too (compose)
// ---------------------------------------------------------------------------

/// A speculative spawn under mandate config composes both additive
/// markers: the worker's first message carries `speculative: true`
/// *and* the mandate, and the `thread/started` pair audits both.
#[tokio::test]
async fn speculative_delegations_carry_mandates_too() {
    let _ = logs();
    let router = sid(1);
    let inbox = RecordingInbox::new();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    ));
    let mandate = json!({
        "strength": "inferred",
        "scope": "investigate the flaky signal; deliver nothing unless confirmed",
        "granted_by": {"login": "reporter", "type": "User"},
        "source": {"channel": "github", "topic": "acme/repo#7"},
        "policy_version": "v1",
    });
    let spawned = spawner
        .spawn_speculative_with_mandate(
            json!("check the anomaly"),
            Some(json!("evt-weak")),
            mandate.clone(),
        )
        .await
        .expect("speculative mandated spawn");

    // The wrapper (the worker's first message) carries both markers.
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    let wrapper = &admits[0].1;
    assert!(delegation_is_speculative(wrapper));
    assert_eq!(delegation_mandate(wrapper), Some(&mandate));
    assert_eq!(admits[0].0, spawned.worker_session);

    // The delegation pair audits both: construct the Router-side
    // thread/started exactly as the tool writes it and confirm the
    // composed bytes ride the task payload on both sides.
    let task_message = spawned.task_message.clone();
    let router_events = vec![event(
        router,
        1,
        EventKind::ThreadStarted,
        kian_superagent::thread_started_payload(
            &spawned.thread,
            spawned.worker_session,
            &task_message,
        ),
    )];
    let worker_events = worker_stream(spawned.worker_session, &router_events);
    let pairs = delegation_pairs(&router_events, &worker_events);
    assert_eq!(pairs.len(), 1);
    let content = &pairs[0].task["content"];
    assert!(delegation_is_speculative(content));
    assert_eq!(delegation_mandate(content), Some(&mandate));

    // And the runtime registration shape: a rebuild from those router
    // events re-derives the mandate (restart survival).
    let rebuilt = PassthroughRegistry::rebuild(&router_events);
    assert_eq!(rebuilt.mandate_of(&spawned.worker_session), Some(&mandate));
}
