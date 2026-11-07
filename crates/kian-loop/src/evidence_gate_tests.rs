//! Evidence-gate tests (acceptance).
//!
//! Two layers:
//! - pure unit tests over hand-built event streams (table-driven per
//!   check, verdict payload golden form, attempt counting, serde);
//! - engine integration tests driving the *real* engine with the
//!   gate-enabled built-in loop (honesty, bounded retries, default-off,
//!   provider-call-count invariance).

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::state::{SessionState, project};
use kian_llm::ProviderRegistry;
use kian_orchestrator::{Engine, EngineConfig};
use kian_store::EventStore;

use crate::evidence::{
    BUILTIN_GATE_NAME, CheckSpec, EvidenceGateConfig, evaluate_evidence_gate, verdict_payload,
};
use crate::react::{ReActConfig, ReActLoop};
use crate::test_support::{HookTools, ScriptedProvider, agent_def, text_chunk, tool_call_chunk};

// ===========================================================================
// Pure-unit layer: stream builders + table-driven predicates
// ===========================================================================

fn ev(kind: EventKind, bytes: Value) -> Event {
    Event {
        seq: 0,
        session: SessionId(Uuid::nil()),
        epoch: Epoch(1),
        ts: 1_000,
        kind,
        bytes,
        version: Event::current_version(),
    }
}

fn input(content: &str) -> Event {
    ev(EventKind::InputAdmitted, json!({"content": content}))
}

fn assistant_final(text: &str) -> Event {
    ev(
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {"text": text}}),
    )
}

fn assistant_tool_call() -> Event {
    ev(
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {
            "text": "checking",
            "tool_calls": [{"id": "c1", "name": "t", "arguments": {}}]
        }}),
    )
}

fn tool_result(is_error: bool) -> Event {
    ev(
        EventKind::ToolResult,
        json!({"role": "tool", "content": {
            "call_id": "c1", "output": {"ok": !is_error}, "is_error": is_error
        }}),
    )
}

fn prior_verdict(gate: &str, passed: bool, attempt: u64) -> Event {
    ev(
        EventKind::EvidenceVerdict,
        json!({"gate": gate, "passed": passed, "checks": [], "attempt": attempt}),
    )
}

/// One table row: a check against a named stream, expecting pass/fail.
struct Case {
    label: &'static str,
    check: CheckSpec,
    stream: Vec<Event>,
    passes: bool,
}

fn check_predicate_table() -> Vec<Case> {
    vec![
        // final_message_present -------------------------------------------
        Case {
            label: "final: final answer after input passes",
            check: CheckSpec::FinalMessagePresent,
            stream: vec![input("do it"), assistant_final("done")],
            passes: true,
        },
        Case {
            label: "final: tool-call turn only (no final) fails",
            check: CheckSpec::FinalMessagePresent,
            stream: vec![input("do it"), assistant_tool_call(), tool_result(false)],
            passes: false,
        },
        Case {
            label: "final: no assistant message at all fails",
            check: CheckSpec::FinalMessagePresent,
            stream: vec![input("do it")],
            passes: false,
        },
        Case {
            label: "final: final before the last input does not count",
            check: CheckSpec::FinalMessagePresent,
            stream: vec![assistant_final("earlier answer"), input("and now?")],
            passes: false,
        },
        Case {
            label: "final: tool turn then final passes",
            check: CheckSpec::FinalMessagePresent,
            stream: vec![
                input("do it"),
                assistant_tool_call(),
                tool_result(false),
                assistant_final("done"),
            ],
            passes: true,
        },
        // tool_result_since_last_user_input --------------------------------
        Case {
            label: "tool-since-input: result after input passes",
            check: CheckSpec::ToolResultSinceLastUserInput,
            stream: vec![input("do it"), assistant_tool_call(), tool_result(false)],
            passes: true,
        },
        Case {
            label: "tool-since-input: result only before the input fails",
            check: CheckSpec::ToolResultSinceLastUserInput,
            stream: vec![tool_result(false), input("do it")],
            passes: false,
        },
        Case {
            label: "tool-since-input: no tool results fails",
            check: CheckSpec::ToolResultSinceLastUserInput,
            stream: vec![input("do it"), assistant_final("no tools needed")],
            passes: false,
        },
        Case {
            label: "tool-since-input: empty stream fails",
            check: CheckSpec::ToolResultSinceLastUserInput,
            stream: vec![],
            passes: false,
        },
        // no_error_tool_result_tail(k) -------------------------------------
        Case {
            label: "tail(2): clean tail passes",
            check: CheckSpec::NoErrorToolResultTail { k: 2 },
            stream: vec![input("d"), tool_result(false), tool_result(false)],
            passes: true,
        },
        Case {
            label: "tail(2): error inside the tail fails",
            check: CheckSpec::NoErrorToolResultTail { k: 2 },
            stream: vec![input("d"), tool_result(false), tool_result(true)],
            passes: false,
        },
        Case {
            label: "tail(2): error outside the tail window passes",
            check: CheckSpec::NoErrorToolResultTail { k: 2 },
            stream: vec![
                input("d"),
                tool_result(true),
                tool_result(false),
                tool_result(false),
            ],
            passes: true,
        },
        Case {
            label: "tail(1): single erroring result fails",
            check: CheckSpec::NoErrorToolResultTail { k: 1 },
            stream: vec![input("d"), tool_result(true)],
            passes: false,
        },
        Case {
            label: "tail(0): vacuous pass on any stream",
            check: CheckSpec::NoErrorToolResultTail { k: 0 },
            stream: vec![input("d"), tool_result(true)],
            passes: true,
        },
        // min_tool_results(n) ----------------------------------------------
        Case {
            label: "min(2): two results pass",
            check: CheckSpec::MinToolResults { n: 2 },
            stream: vec![input("d"), tool_result(false), tool_result(false)],
            passes: true,
        },
        Case {
            label: "min(2): one result fails",
            check: CheckSpec::MinToolResults { n: 2 },
            stream: vec![input("d"), tool_result(false)],
            passes: false,
        },
        Case {
            label: "min(0): vacuous pass on empty stream",
            check: CheckSpec::MinToolResults { n: 0 },
            stream: vec![],
            passes: true,
        },
    ]
}

#[test]
fn check_predicates_table_pass_and_fail_streams() {
    for case in check_predicate_table() {
        let verdict = evaluate_evidence_gate(BUILTIN_GATE_NAME, &[case.check], &case.stream);
        assert_eq!(
            verdict.passed, case.passes,
            "case `{}` expected passed={}",
            case.label, case.passes
        );
        assert_eq!(verdict.checks.len(), 1, "case `{}`", case.label);
        assert_eq!(
            verdict.checks[0].passed, case.passes,
            "case `{}`",
            case.label
        );
        if !case.passes {
            assert!(
                verdict.checks[0].detail.is_some(),
                "case `{}`: a failed check must carry a detail",
                case.label
            );
        }
    }
}

#[test]
fn gate_passes_only_when_all_checks_pass_and_reports_each_check() {
    let stream = vec![input("d"), assistant_tool_call(), tool_result(false)];
    let checks = vec![
        CheckSpec::ToolResultSinceLastUserInput, // passes
        CheckSpec::MinToolResults { n: 5 },      // fails
    ];
    let verdict = evaluate_evidence_gate(BUILTIN_GATE_NAME, &checks, &stream);
    assert!(!verdict.passed);
    assert_eq!(verdict.checks.len(), 2);
    assert!(verdict.checks[0].passed);
    assert!(!verdict.checks[1].passed);
    // An empty check list is a vacuous pass (all-of-nothing).
    assert!(evaluate_evidence_gate(BUILTIN_GATE_NAME, &[], &stream).passed);
}

#[test]
fn attempt_counter_counts_prior_failed_verdicts_of_the_same_gate_only() {
    let stream = vec![
        input("d"),
        prior_verdict("other-gate", false, 0), // different gate: ignored
        prior_verdict(BUILTIN_GATE_NAME, true, 0), // a pass: ignored
        prior_verdict(BUILTIN_GATE_NAME, false, 0), // failed retry 1
        prior_verdict(BUILTIN_GATE_NAME, false, 1), // failed retry 2
    ];
    let verdict = evaluate_evidence_gate(BUILTIN_GATE_NAME, &[], &stream);
    assert_eq!(verdict.attempt, 2);
    // Fresh stream: attempt zero.
    assert_eq!(
        evaluate_evidence_gate(BUILTIN_GATE_NAME, &[], &[input("d")]).attempt,
        0
    );
}

#[test]
fn verdict_payload_golden_form() {
    let stream = vec![input("d"), tool_result(false)];
    let verdict = evaluate_evidence_gate(
        BUILTIN_GATE_NAME,
        &[CheckSpec::MinToolResults { n: 2 }],
        &stream,
    );
    let payload = verdict_payload(BUILTIN_GATE_NAME, &verdict);
    // The exact payload schema, field for field.
    assert_eq!(
        payload,
        json!({
            "gate": "builtin",
            "passed": false,
            "checks": [
                {
                    "name": "min_tool_results",
                    "passed": false,
                    "detail": "expected >= 2 tool results, found 1"
                }
            ],
            "attempt": 0
        })
    );
    // Golden serialized form (compact, lexicographic keys — the frozen
    // serialization discipline, constraint 3).
    assert_eq!(
        serde_json::to_string(&payload).unwrap(),
        r#"{"attempt":0,"checks":[{"detail":"expected >= 2 tool results, found 1","name":"min_tool_results","passed":false}],"gate":"builtin","passed":false}"#
    );
    // A passing check omits `detail` (payload stays minimal).
    let ok = evaluate_evidence_gate(
        BUILTIN_GATE_NAME,
        &[CheckSpec::MinToolResults { n: 1 }],
        &stream,
    );
    assert_eq!(
        verdict_payload(BUILTIN_GATE_NAME, &ok),
        json!({
            "gate": "builtin",
            "passed": true,
            "checks": [{"name": "min_tool_results", "passed": true}],
            "attempt": 0
        })
    );
}

#[test]
fn default_config_is_disabled_with_zero_retries() {
    let cfg = EvidenceGateConfig::default();
    assert!(!cfg.enabled, "default behavior unchanged");
    assert!(cfg.checks.is_empty());
    assert_eq!(cfg.max_retries, 0);
}

#[test]
fn config_and_check_specs_serde_round_trip_and_fail_closed() {
    // Check specs round-trip through their tagged form.
    for spec in [
        CheckSpec::FinalMessagePresent,
        CheckSpec::ToolResultSinceLastUserInput,
        CheckSpec::NoErrorToolResultTail { k: 2 },
        CheckSpec::MinToolResults { n: 3 },
    ] {
        let s = serde_json::to_string(&spec).unwrap();
        assert_eq!(serde_json::from_str::<CheckSpec>(&s).unwrap(), spec);
    }
    assert_eq!(
        serde_json::to_string(&CheckSpec::NoErrorToolResultTail { k: 2 }).unwrap(),
        r#"{"type":"no_error_tool_result_tail","k":2}"#
    );
    // Full config round-trips; `{}` parses to the disabled default.
    let cfg = EvidenceGateConfig {
        enabled: true,
        checks: vec![CheckSpec::MinToolResults { n: 1 }],
        max_retries: 2,
        ..EvidenceGateConfig::default()
    };
    let s = serde_json::to_string(&cfg).unwrap();
    assert_eq!(serde_json::from_str::<EvidenceGateConfig>(&s).unwrap(), cfg);
    assert!(
        !serde_json::from_str::<EvidenceGateConfig>("{}")
            .unwrap()
            .enabled
    );
    // Fail-closed: unknown fields and unknown check types are rejected.
    assert!(
        serde_json::from_str::<EvidenceGateConfig>(r#"{"enabled": true, "surprise": 1}"#).is_err()
    );
    assert!(serde_json::from_str::<CheckSpec>(r#"{"type":"ask_the_model"}"#).is_err());
}

#[test]
fn cloned_config_shares_verdict_counters() {
    let cfg = EvidenceGateConfig::default();
    let clone = cfg.clone();
    cfg.counters.record_passed();
    cfg.counters.record_failed();
    assert_eq!(clone.counters.snapshot().passed, 1);
    assert_eq!(clone.counters.snapshot().failed, 1);
}

// ===========================================================================
// Engine integration layer: the gate inside the built-in loop's stop path
// ===========================================================================

fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("kian-gate-test-{}.sqlite", Uuid::new_v4()))
}

fn cleanup_db(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

async fn wait_for<F, Fut>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct GateHarness {
    engine: Arc<Engine<ReActLoop>>,
    store: EventStore,
    db_path: PathBuf,
    gate: EvidenceGateConfig,
    provider: Arc<ScriptedProvider>,
}

/// Assembles the real engine (sqlite store, admit/wake/drain) with the
/// built-in loop and an evidence gate mounted on its stop path.
async fn gate_harness(
    turns: Vec<Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>>,
    tools: Arc<dyn kian_core::seams::ToolProvider>,
    gate: EvidenceGateConfig,
    config: ReActConfig,
) -> GateHarness {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::with_evidence_gate(
        agent_def(),
        Arc::new(registry),
        tools,
        Arc::new(crate::test_support::NoopPolicy),
        config,
        gate.clone(),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    GateHarness {
        engine,
        store,
        db_path,
        gate,
        provider,
    }
}

async fn events_of(store: &EventStore, sid: SessionId) -> Vec<Event> {
    store.rebuild(sid).await.expect("rebuild")
}

async fn ended_reason(store: &EventStore, sid: SessionId) -> Option<EndReason> {
    events_of(store, sid)
        .await
        .last()
        .and_then(|ev| match ev.kind {
            EventKind::SessionEnded { reason } => Some(reason),
            _ => None,
        })
}

fn kinds(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|e| e.kind.as_str()).collect()
}

fn verdicts(events: &[Event]) -> Vec<&Event> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::EvidenceVerdict))
        .collect()
}

async fn submit_and_wait(h: &GateHarness, sid: SessionId, payload: Value) -> EndReason {
    h.engine.admit(sid, payload, None).await.expect("admit");
    let _ = h.engine.wake(sid);
    assert!(
        wait_for(10_000, || async {
            matches!(
                ended_reason(&h.store, sid).await,
                Some(EndReason::Completed | EndReason::Failed | EndReason::Interrupted)
            )
        })
        .await,
        "session never reached a terminal end write"
    );
    ended_reason(&h.store, sid).await.expect("end write")
}

/// The honesty test: gate-fail → loop continues (a real extra provider
/// turn) → gate-fail again past `max_retries` → `SoftFailed`; the session
/// settles `failed` — never `completed` — with every failed verdict
/// in-stream before the end write.
#[tokio::test]
async fn gate_fail_continue_fail_again_settles_failed_with_verdicts_in_stream() {
    // The model answers immediately (no tool use), so `min_tool_results(1)`
    // can never pass — but it takes a provider turn each time the gate
    // sends the loop back for another attempt.
    let turns = vec![
        vec![text_chunk("done, honest")],
        vec![text_chunk("still done")],
    ];
    let gate = EvidenceGateConfig {
        enabled: true,
        checks: vec![CheckSpec::MinToolResults { n: 1 }],
        max_retries: 1,
        ..EvidenceGateConfig::default()
    };
    let h = gate_harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        gate,
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");

    let reason = submit_and_wait(&h, sid, json!("prove it")).await;
    assert_eq!(
        reason,
        EndReason::Failed,
        "a failed gate must never complete"
    );

    let events = events_of(&h.store, sid).await;
    let verdicts = verdicts(&events);
    assert_eq!(verdicts.len(), 2, "exactly max_retries+1 gate evaluations");
    for (i, v) in verdicts.iter().enumerate() {
        assert_eq!(v.bytes.get("passed"), Some(&json!(false)));
        assert_eq!(v.bytes.get("attempt"), Some(&json!(i as u64)));
        assert_eq!(v.bytes.get("gate"), Some(&json!("builtin")));
    }
    // Every verdict is in-stream strictly before the end write, and the
    // loop really continued: two provider turns ran.
    assert_eq!(kinds(&events).last(), Some(&"session/ended"));
    assert!(verdicts[0].seq < verdicts[1].seq);
    assert!(verdicts[1].seq < events.last().unwrap().seq);
    let step_requests = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .count();
    assert_eq!(step_requests, 2, "one retry turn after the first failure");
    assert_eq!(
        h.provider.requests().len(),
        2,
        "the gate itself made no provider call"
    );
    // `end_reason=failed` on the end write (asserted above) is the honest
    // settle; the projection maps a failed end back to idle (retryable).
    assert_eq!(project(&events).unwrap(), SessionState::Idle);
    assert_eq!(h.gate.counters.snapshot().passed, 0);
    assert_eq!(h.gate.counters.snapshot().failed, 2);
    cleanup_db(&h.db_path);
}

/// Gate-pass on the loop's completion path: the verdict is appended and
/// the stop proceeds — `evidence/verdict` is the last event immediately
/// before the end write.
#[tokio::test]
async fn gate_pass_appends_verdict_immediately_before_end_write() {
    let (tools, _seen) = HookTools::ok(json!({"ok": true}));
    let turns = vec![
        vec![tool_call_chunk("call-1", "hook", json!({}))],
        vec![text_chunk("verified: the tool said ok")],
    ];
    let gate = EvidenceGateConfig {
        enabled: true,
        checks: vec![
            CheckSpec::FinalMessagePresent,
            CheckSpec::ToolResultSinceLastUserInput,
            CheckSpec::NoErrorToolResultTail { k: 1 },
        ],
        max_retries: 1,
        ..EvidenceGateConfig::default()
    };
    let h = gate_harness(turns, Arc::new(tools), gate, ReActConfig::default()).await;
    let sid = h.engine.create_session().await.expect("create");

    let reason = submit_and_wait(&h, sid, json!("do it and prove it")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    let k = kinds(&events);
    let n = k.len();
    assert_eq!(&k[n - 2..], &["evidence/verdict", "session/ended"]);
    let verdict = verdicts(&events)[0];
    assert_eq!(verdict.bytes.get("passed"), Some(&json!(true)));
    assert_eq!(verdict.bytes.get("attempt"), Some(&json!(0)));
    assert_eq!(
        verdict
            .bytes
            .get("checks")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(h.gate.counters.snapshot().passed, 1);
    assert_eq!(h.gate.counters.snapshot().failed, 0);
    cleanup_db(&h.db_path);
}

/// Bounded retries: an adversarial never-satisfiable gate terminates after
/// exactly `max_retries` extra provider turns and settles `failed`.
#[tokio::test]
async fn never_satisfiable_gate_is_bounded_by_max_retries_and_terminates() {
    let endless: Vec<_> = (0..10)
        .map(|_| vec![text_chunk("trust me, it worked")])
        .collect();
    let gate = EvidenceGateConfig {
        enabled: true,
        checks: vec![CheckSpec::MinToolResults { n: 999 }],
        max_retries: 3,
        ..EvidenceGateConfig::default()
    };
    let h = gate_harness(
        endless,
        Arc::new(crate::test_support::NoopTools),
        gate,
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");

    let reason = submit_and_wait(&h, sid, json!("impossible task")).await;
    assert_eq!(reason, EndReason::Failed);

    let events = events_of(&h.store, sid).await;
    // 1 initial attempt + max_retries extra iterations, then SoftFailed.
    let step_requests = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .count();
    assert_eq!(step_requests, 4, "exactly 1 + max_retries provider turns");
    let verdicts = verdicts(&events);
    assert_eq!(verdicts.len(), 4);
    for (i, v) in verdicts.iter().enumerate() {
        assert_eq!(v.bytes.get("attempt"), Some(&json!(i as u64)));
    }
    cleanup_db(&h.db_path);
}

/// The gate never calls the provider: with identical scripted turns, the
/// provider sees exactly the same number of requests with the gate
/// enabled-and-passing as with it disabled.
#[tokio::test]
async fn gate_execution_leaves_provider_request_count_unchanged() {
    let turns_disabled = vec![vec![text_chunk("done")]];
    let turns_enabled = vec![vec![text_chunk("done")]];

    let h_off = gate_harness(
        turns_disabled,
        Arc::new(crate::test_support::NoopTools),
        EvidenceGateConfig::default(),
        ReActConfig::default(),
    )
    .await;
    let p_off = h_off.provider.clone();
    let sid_off = h_off.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h_off, sid_off, json!("task")).await;
    assert_eq!(reason, EndReason::Completed);

    let gate = EvidenceGateConfig {
        enabled: true,
        checks: vec![CheckSpec::FinalMessagePresent],
        max_retries: 2,
        ..EvidenceGateConfig::default()
    };
    let h_on = gate_harness(
        turns_enabled,
        Arc::new(crate::test_support::NoopTools),
        gate,
        ReActConfig::default(),
    )
    .await;
    let p_on = h_on.provider.clone();
    let sid_on = h_on.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h_on, sid_on, json!("task")).await;
    assert_eq!(reason, EndReason::Completed);

    assert_eq!(p_off.requests().len(), p_on.requests().len());
    assert_eq!(h_on.gate.counters.snapshot().passed, 1);
    // And the only stream difference is the verdict itself.
    let on_events = events_of(&h_on.store, sid_on).await;
    assert_eq!(verdicts(&on_events).len(), 1);
    cleanup_db(&h_off.db_path);
    cleanup_db(&h_on.db_path);
}

/// Default-off regression: the built-in loop (`ReActLoop::new` semantics —
/// gate disabled) never appends a verdict event.
#[tokio::test]
async fn gate_disabled_by_default_appends_no_verdict_events() {
    let turns = vec![vec![text_chunk("done")]];
    let h = gate_harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        EvidenceGateConfig::default(),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("task")).await;
    assert_eq!(reason, EndReason::Completed);
    let events = events_of(&h.store, sid).await;
    assert!(
        verdicts(&events).is_empty(),
        "disabled gate must leave the stream untouched"
    );
    assert_eq!(
        h.gate.counters.snapshot().passed + h.gate.counters.snapshot().failed,
        0
    );
    cleanup_db(&h.db_path);
}

/// The gate also guards the intentional-stop path (iteration budget): a
/// passing verdict lets the budget stop proceed to `completed`, with the
/// verdict immediately before the end write.
#[tokio::test]
async fn budget_stop_with_passing_gate_completes_with_verdict_before_end_write() {
    let endless: Vec<_> = (0..10)
        .map(|i| vec![tool_call_chunk(&format!("call-{i}"), "hook", json!({}))])
        .collect();
    let (tools, seen) = HookTools::ok(json!({"ok": true}));
    let gate = EvidenceGateConfig {
        enabled: true,
        checks: vec![CheckSpec::MinToolResults { n: 1 }],
        max_retries: 0,
        ..EvidenceGateConfig::default()
    };
    let h = gate_harness(
        endless,
        Arc::new(tools),
        gate,
        ReActConfig {
            slot: kian_core::model::SlotName("main".into()),
            max_iterations: 2,
            permissions: std::collections::BTreeMap::new(),
        },
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");

    let reason = submit_and_wait(&h, sid, json!("loop forever")).await;
    assert_eq!(
        reason,
        EndReason::Completed,
        "budget stop stays intentional"
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "exactly max_iterations turns"
    );

    let events = events_of(&h.store, sid).await;
    let k = kinds(&events);
    let n = k.len();
    assert_eq!(&k[n - 2..], &["evidence/verdict", "session/ended"]);
    assert_eq!(verdicts(&events)[0].bytes.get("passed"), Some(&json!(true)));
    cleanup_db(&h.db_path);
}
