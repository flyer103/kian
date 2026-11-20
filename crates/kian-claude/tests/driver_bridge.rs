//! The Claude Code adapter through the real shared driver, against a scripted stream-json child
//! (no `claude`, no credentials, default CI).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_claude::ClaudeAdapter;
use kian_claude::mapping::ClaudeConfig;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::ApprovalPolicy;
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig, Decision, Rule};
use serde_json::json;

fn peer() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kian-claude-fixture-peer"))
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kian-claude-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn write_script(dir: &Path, name: &str, lines: &[&str]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, lines.join("\n")).expect("script");
    path
}

fn adapter(script: &Path) -> ClaudeAdapter {
    ClaudeAdapter::new(ClaudeConfig {
        command: peer().display().to_string(),
        args: vec![script.display().to_string()],
        model: None,
        env: Vec::new(),
        capture_base: None,
    })
}

fn driver(adapter: ClaudeAdapter, config: BridgeDriverConfig) -> BridgeDriver {
    BridgeDriver::new(Arc::new(adapter), config)
}

#[tokio::test]
async fn a_scripted_turn_maps_to_the_frozen_kinds_and_settles() {
    let dir = scratch("turn");
    let script = write_script(
        &dir,
        "turn.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect","line":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"pong"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;

    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    assert_eq!(
        ctx.kinds(),
        vec![
            EventKind::InputAdmitted,
            EventKind::TurnStart,
            EventKind::AssistantMessage,
            EventKind::TurnEnd,
        ]
    );
    let events = ctx.committed();
    assert_eq!(events[1].bytes["iteration"], 0);
    assert_eq!(events[3].bytes["outcome"], "final_answer");
    assert_eq!(events[3].bytes["iteration"], 0);
    assert!(
        events.iter().all(|e| e.kind != EventKind::StepRequest),
        "projection-only: no step/request"
    );
}

#[tokio::test]
async fn a_child_that_dies_mid_turn_is_a_soft_failure_never_completed() {
    let dir = scratch("crash");
    let script = write_script(
        &dir,
        "crash.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect","line":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"partial"}]}}}"#,
            r#"{"op":"exit","code":9}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;

    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { reason }] => {
            assert!(reason.contains("mid-turn"), "reason: {reason}");
        }
        other => panic!("expected one soft failure, got {other:?}"),
    }
    // The honest failure trace: the open turn never got its turn/end.
    assert_eq!(
        ctx.kinds(),
        vec![
            EventKind::InputAdmitted,
            EventKind::TurnStart,
            EventKind::AssistantMessage,
        ]
    );
}

#[tokio::test]
async fn a_denied_approval_is_recorded_and_never_executed() {
    let dir = scratch("approve");
    let script = write_script(
        &dir,
        "deny.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect","line":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"rm please"}]}}}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"rm -rf /"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm -rf /"}}}}"#,
            r#"{"op":"expect","line":{"type":"control_response","response":{"subtype":"success","request_id":"r1","response":{"behavior":"deny","message":"denied by kian approval policy"}}}}"#,
            r#"{"op":"send","line":{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"denied by policy","is_error":true}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "rm please"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let events = ctx.committed();
    let call = events
        .iter()
        .find(|e| e.kind == EventKind::ToolCall)
        .expect("tool/call recorded");
    assert_eq!(
        call.bytes.pointer("/content/permission/evaluated"),
        Some(&json!("deny"))
    );
    assert_eq!(
        call.bytes.pointer("/content/permission/policy"),
        Some(&json!("configured"))
    );
    let result = events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .expect("tool/result recorded");
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&json!(true))
    );
}

#[tokio::test]
async fn an_allowed_approval_carries_the_matching_rule() {
    let dir = scratch("allow");
    let script = write_script(
        &dir,
        "allow.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect","line":{"type":"user","message":{"role":"user","content":[{"type":"text","text":"read"}]}}}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"path":"x"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Read","input":{"path":"x"}}}}"#,
            r#"{"op":"expect","line":{"type":"control_response","response":{"subtype":"success","request_id":"r1","response":{"behavior":"allow","message":""}}}}"#,
            r#"{"op":"send","line":{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "read"}));
    let policy = ApprovalPolicy {
        default: Decision::Deny,
        rules: vec![Rule::parse("Read", Decision::Allow).expect("rule")],
    };
    let outcomes: Vec<LoopOutcome> = driver(
        adapter(&script),
        BridgeDriverConfig {
            policy,
            max_steps: 32,
            labels: Default::default(),
            step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
        },
    )
    .drive(ctx.clone())
    .collect()
    .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let call = ctx
        .committed()
        .into_iter()
        .find(|e| e.kind == EventKind::ToolCall)
        .expect("tool/call");
    assert_eq!(
        call.bytes.pointer("/content/permission/matched"),
        Some(&json!("Read"))
    );
}

/// A5 (boundary half): a synthetic steer row delivered at a safe boundary keeps its strippable
/// marker in the stream and reaches the harness as an ordinary control input.
#[tokio::test]
async fn a_steer_at_a_safe_boundary_keeps_its_synthetic_marker() {
    let dir = scratch("steer");
    let script = write_script(
        &dir,
        "steer.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    ctx.push_input(json!({"synthetic": true, "content": "plan B"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let events = ctx.committed();
    let admitted: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .map(|e| &e.bytes)
        .collect();
    assert_eq!(admitted.len(), 2, "both inputs admitted: {admitted:?}");
    assert!(
        admitted
            .iter()
            .any(|b| b.get("synthetic").and_then(|s| s.as_bool()) == Some(true)),
        "the steer keeps its marker: {admitted:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::TurnStart)
            .count(),
        1,
        "a steer does not open a second turn"
    );
}

#[tokio::test]
async fn the_step_budget_stops_before_a_turn_opens() {
    let dir = scratch("budget");
    let script = write_script(
        &dir,
        "budget.jsonl",
        &[r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(
        adapter(&script),
        BridgeDriverConfig {
            policy: ApprovalPolicy::deny_all(),
            max_steps: 0,
            labels: Default::default(),
            step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
        },
    )
    .drive(ctx.clone())
    .collect()
    .await;
    match outcomes.as_slice() {
        [LoopOutcome::Stopped { reason }] => assert!(reason.contains("budget"), "{reason}"),
        other => panic!("expected Stopped, got {other:?}"),
    }
    assert!(
        ctx.committed().is_empty(),
        "no turn may open under a 0 budget"
    );
}

#[tokio::test]
async fn an_early_child_exit_is_a_soft_failure() {
    let dir = scratch("early-exit");
    let script = write_script(&dir, "early-exit.jsonl", &[r#"{"op":"exit","code":9}"#]);
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;
    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { .. }] => {}
        other => panic!("expected SoftFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unknown_message_is_a_named_soft_failure() {
    let dir = scratch("drift");
    let script = write_script(
        &dir,
        "drift.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"mystery"}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;
    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { reason }] => {
            assert!(reason.contains("unknown claude message type"), "{reason}")
        }
        other => panic!("expected SoftFailed, got {other:?}"),
    }
}

struct DenyAll;
impl kian_harness::ToolPrePort for DenyAll {
    fn tool_pre(
        &self,
        _call: kian_harness::ToolPreCall,
    ) -> futures::future::BoxFuture<'static, kian_harness::ToolPreVerdict> {
        Box::pin(async {
            kian_harness::ToolPreVerdict::Deny {
                plugin: "gate".into(),
                reason: "blocked by policy".into(),
            }
        })
    }
}

struct AllowAll;
impl kian_harness::ToolPrePort for AllowAll {
    fn tool_pre(
        &self,
        _call: kian_harness::ToolPreCall,
    ) -> futures::future::BoxFuture<'static, kian_harness::ToolPreVerdict> {
        Box::pin(async { kian_harness::ToolPreVerdict::Allow })
    }
}

fn driver_with_port(
    adapter: ClaudeAdapter,
    port: std::sync::Arc<dyn kian_harness::ToolPrePort>,
) -> BridgeDriver {
    BridgeDriver::new(Arc::new(adapter), BridgeDriverConfig::default()).with_tool_pre_port(port)
}

#[tokio::test]
async fn a_tool_pre_deny_flips_an_allowed_call_to_policy_hook() {
    let dir = scratch("hook-deny");
    let script = write_script(
        &dir,
        "hook-deny.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"path":"x"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Read","input":{"path":"x"}}}}"#,
            r#"{"op":"expect","line":{"type":"control_response","response":{"subtype":"success","request_id":"r1","response":{"behavior":"deny","message":"denied by kian approval policy"}}}}"#,
            r#"{"op":"send","line":{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"denied","is_error":true}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "read"}));
    let policy = ApprovalPolicy {
        default: Decision::Deny,
        rules: vec![Rule::parse("Read", Decision::Allow).expect("rule")],
    };
    let outcomes: Vec<LoopOutcome> = BridgeDriver::new(
        Arc::new(adapter(&script)),
        BridgeDriverConfig {
            policy,
            max_steps: 32,
            labels: Default::default(),
            step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
        },
    )
    .with_tool_pre_port(Arc::new(DenyAll) as Arc<dyn kian_harness::ToolPrePort>)
    .drive(ctx.clone())
    .collect()
    .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let call = ctx
        .committed()
        .into_iter()
        .find(|e| e.kind == EventKind::ToolCall)
        .expect("tool/call");
    assert_eq!(
        call.bytes.pointer("/content/permission/evaluated"),
        Some(&json!("deny"))
    );
    assert_eq!(
        call.bytes.pointer("/content/permission/policy"),
        Some(&json!("hook"))
    );
    assert_eq!(
        call.bytes.pointer("/content/permission/matched"),
        Some(&json!("gate:tool/pre"))
    );
    assert_eq!(
        call.bytes.pointer("/content/permission/reason"),
        Some(&json!("blocked by policy"))
    );
}

#[tokio::test]
async fn a_tool_pre_allow_cannot_widen_a_config_deny() {
    let dir = scratch("hook-allow");
    let script = write_script(
        &dir,
        "hook-allow.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"rm -rf /"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm -rf /"}}}}"#,
            r#"{"op":"expect","line":{"type":"control_response","response":{"subtype":"success","request_id":"r1","response":{"behavior":"deny","message":"denied by kian approval policy"}}}}"#,
            r#"{"op":"send","line":{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"denied","is_error":true}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "rm"}));
    let outcomes: Vec<LoopOutcome> = driver_with_port(
        adapter(&script),
        Arc::new(AllowAll) as Arc<dyn kian_harness::ToolPrePort>,
    )
    .drive(ctx.clone())
    .collect()
    .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let call = ctx
        .committed()
        .into_iter()
        .find(|e| e.kind == EventKind::ToolCall)
        .expect("tool/call");
    assert_eq!(
        call.bytes.pointer("/content/permission/policy"),
        Some(&json!("configured")),
        "an allow verdict never widens the declarative deny"
    );
    assert_eq!(
        call.bytes.pointer("/content/permission/matched"),
        Some(&json!("default"))
    );
}

struct PreloadedCapture {
    bodies: Vec<Vec<u8>>,
}

struct PreloadedStream {
    pending: Vec<Vec<u8>>,
}

impl kian_harness::CaptureStream for PreloadedStream {
    fn drain(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.pending)
    }
}

impl kian_harness::CaptureSource for PreloadedCapture {
    fn open(&self, _session: SessionId) -> Option<Box<dyn kian_harness::CaptureStream>> {
        Some(Box::new(PreloadedStream {
            pending: self.bodies.clone(),
        }))
    }
}

#[tokio::test]
async fn captured_requests_are_appended_before_the_assistant_event() {
    let dir = scratch("capture");
    let script = write_script(
        &dir,
        "capture.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let driver = BridgeDriver::new(Arc::new(adapter(&script)), BridgeDriverConfig::default())
        .with_capture_source(Arc::new(PreloadedCapture {
            bodies: vec![br#"{"attempt":1}"#.to_vec(), br#"{"attempt":2}"#.to_vec()],
        }) as Arc<dyn kian_harness::CaptureSource>);
    let outcomes: Vec<LoopOutcome> = driver.drive(ctx.clone()).collect().await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let events = ctx.committed();
    let kinds: Vec<EventKind> = events.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            EventKind::InputAdmitted,
            EventKind::TurnStart,
            EventKind::StepRequest,
            EventKind::StepRequest,
            EventKind::AssistantMessage,
            EventKind::TurnEnd,
        ],
        "each captured attempt is a step/request before the answer"
    );
    assert_eq!(
        events[2].bytes.pointer("/body/attempt"),
        Some(&json!(1)),
        "the exact body is persisted"
    );
    assert_eq!(events[2].bytes["iteration"], 0);
    assert_eq!(events[3].bytes.pointer("/body/attempt"), Some(&json!(2)));
}

#[tokio::test]
async fn a_non_json_capture_is_an_honest_soft_failure() {
    let dir = scratch("capture-bad");
    let script = write_script(
        &dir,
        "capture-bad.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let driver = BridgeDriver::new(Arc::new(adapter(&script)), BridgeDriverConfig::default())
        .with_capture_source(Arc::new(PreloadedCapture {
            bodies: vec![b"not json".to_vec()],
        }) as Arc<dyn kian_harness::CaptureSource>);
    let outcomes: Vec<LoopOutcome> = driver.drive(ctx.clone()).collect().await;
    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { reason }] => {
            assert!(reason.contains("not JSON"), "{reason}")
        }
        other => panic!("expected SoftFailed, got {other:?}"),
    }
}

/// ADR-044: an open step that produces no progress costs one honest soft failure at the bound,
/// never a parked drain.
#[tokio::test]
async fn an_open_step_that_goes_silent_ends_in_a_soft_failure_at_the_bound() {
    let dir = scratch("step-stall");
    let script = write_script(
        &dir,
        "stall.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working"}]}}}"#,
            r#"{"op":"sleep","ms":60000}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let config = BridgeDriverConfig {
        policy: ApprovalPolicy::deny_all(),
        max_steps: 32,
        step_timeout_ms: 300,
        labels: Default::default(),
    };
    let started = std::time::Instant::now();
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), config)
        .drive(ctx.clone())
        .collect()
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the bound must fire promptly"
    );
    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { reason }] => {
            assert!(reason.contains("step bound"), "{reason}");
        }
        other => panic!("expected one SoftFailed, got {other:?}"),
    }
    // The child's pre-stall message is kept; nothing fabricates a completion or an end write.
    assert!(
        ctx.committed()
            .iter()
            .any(|e| e.kind == EventKind::AssistantMessage)
    );
    assert!(
        !ctx.committed()
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    );
}

/// ADR-044: the bound is per gap, not a total step cap — a step that keeps talking is not cut.
#[tokio::test]
async fn a_step_that_keeps_making_progress_is_not_cut() {
    let dir = scratch("step-progress");
    let script = write_script(
        &dir,
        "progress.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"one"}]}}}"#,
            r#"{"op":"sleep","ms":100}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"two"}]}}}"#,
            r#"{"op":"sleep","ms":100}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let config = BridgeDriverConfig {
        policy: ApprovalPolicy::deny_all(),
        max_steps: 32,
        step_timeout_ms: 400,
        labels: Default::default(),
    };
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), config)
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
}

/// ADR-044: `0` is the documented opt-out — the pre-ADR unbounded wait.
#[tokio::test]
async fn zero_step_timeout_disables_the_bound() {
    let dir = scratch("step-off");
    let script = write_script(
        &dir,
        "off.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"slow"}]}}}"#,
            r#"{"op":"sleep","ms":600}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let config = BridgeDriverConfig {
        policy: ApprovalPolicy::deny_all(),
        max_steps: 32,
        step_timeout_ms: 0,
        labels: Default::default(),
    };
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), config)
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
}

#[tokio::test]
async fn the_cli_session_id_is_persisted_as_the_resume_handle() {
    let dir = scratch("resume");
    let script = write_script(
        &dir,
        "resume.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init","session_id":"sess-1"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), BridgeDriverConfig::default())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let end = ctx
        .committed()
        .into_iter()
        .find(|e| e.kind == EventKind::TurnEnd)
        .expect("turn/end");
    assert_eq!(
        end.bytes.pointer("/harness/resume"),
        Some(&json!("sess-1")),
        "the CLI session id is the durable handle"
    );
}

fn ask_config() -> BridgeDriverConfig {
    BridgeDriverConfig {
        policy: kian_harness::approval::ApprovalPolicy {
            default: kian_harness::approval::Decision::Ask,
            rules: Vec::new(),
        },
        ..BridgeDriverConfig::default()
    }
}

#[tokio::test]
async fn a_confirmation_resumes_and_answers_an_identical_reissue_once() {
    let dir = scratch("ask-confirm");
    let first_script = write_script(
        &dir,
        "ask1.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let first: Vec<LoopOutcome> = driver(adapter(&first_script), ask_config())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(first, vec![LoopOutcome::Settled], "deferred, nothing ran");
    assert!(
        ctx.committed()
            .iter()
            .all(|event| event.kind != EventKind::ToolResult),
        "no effect before the human decision"
    );

    let intent = kian_core::control::ConfirmIntent {
        call_id: "t1".to_owned(),
        decision: kian_core::control::ConfirmDecision::Approve,
        reason: None,
        requested_by: Some("test-operator".to_owned()),
        answered_at_ms: 7,
        expired: false,
    };
    kian_harness::ctx::BridgeCtx::append(&*ctx, EventKind::ControlConfirm, intent.event_payload())
        .await
        .expect("seed the durable decision");

    let second_script = write_script(
        &dir,
        "ask2.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r2","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    ctx.start_new_drain();
    let second: Vec<LoopOutcome> = driver(adapter(&second_script), ask_config())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(second, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);

    let events = ctx.committed();
    let calls: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCall)
        .collect();
    assert_eq!(
        calls.len(),
        1,
        "the confirmed call keeps one record; calls: {calls:?}"
    );
    assert_eq!(
        calls[0].bytes.pointer("/permission/evaluated"),
        Some(&json!("ask")),
        "the original ask provenance is untouched"
    );
    let results: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolResult)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].bytes.pointer("/content/call_id"),
        Some(&json!("t1")),
        "the result maps onto the pending call"
    );
    assert_eq!(
        results[0].bytes.pointer("/confirmation/decision"),
        Some(&json!("approve"))
    );
    assert_eq!(
        results[0].bytes.pointer("/confirmation/requested_by"),
        Some(&json!("test-operator"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::InputAdmitted
                && event.bytes.pointer("/synthetic") == Some(&json!(true))),
        "the decision nudge is persisted as synthetic input"
    );
}

async fn answered_ask(dir: &std::path::Path) -> Arc<MemoryCtx> {
    let script = write_script(
        dir,
        "ask-defer-again.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), ask_config())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let intent = kian_core::control::ConfirmIntent {
        call_id: "t1".to_owned(),
        decision: kian_core::control::ConfirmDecision::Approve,
        reason: None,
        requested_by: Some("test-operator".to_owned()),
        answered_at_ms: 7,
        expired: false,
    };
    kian_harness::ctx::BridgeCtx::append(&*ctx, EventKind::ControlConfirm, intent.event_payload())
        .await
        .expect("seed the durable decision");
    ctx.start_new_drain();
    ctx
}

#[tokio::test]
async fn a_different_reissue_is_not_auto_approved_and_the_confirmed_call_closes() {
    let dir = scratch("ask-mismatch");
    let ctx = answered_ask(&dir).await;
    let script = write_script(
        &dir,
        "ask-mismatch.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t2","name":"Bash","input":{"command":"pwd"}}]}}}"#,
            r#"{"op":"send","line":{"type":"control_request","request_id":"r2","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"pwd"}}}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), ask_config())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled], "it asks again");
    let events = ctx.committed();
    let calls: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCall)
        .collect();
    assert_eq!(calls.len(), 2, "the confirmed t1 and the new t2");
    let results: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolResult)
        .collect();
    assert_eq!(results.len(), 1, "t1 closed unexercised");
    assert_eq!(
        results[0].bytes.pointer("/content/call_id"),
        Some(&json!("t1"))
    );
    assert_eq!(
        results[0].bytes.pointer("/content/output/reason"),
        Some(&json!("confirmed call was not re-issued"))
    );
}

#[tokio::test]
async fn a_turn_that_never_reissues_closes_the_confirmed_call_honestly() {
    let dir = scratch("ask-no-reissue");
    let ctx = answered_ask(&dir).await;
    let script = write_script(
        &dir,
        "ask-no-reissue.jsonl",
        &[
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            r#"{"op":"expect_any"}"#,
            r#"{"op":"send","line":{"type":"result","subtype":"success","is_error":false}}"#,
            r#"{"op":"eof"}"#,
        ],
    );
    let outcomes: Vec<LoopOutcome> = driver(adapter(&script), ask_config())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let results: Vec<_> = ctx
        .committed()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolResult)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].bytes.pointer("/content/output/reason"),
        Some(&json!("confirmed call was not re-issued"))
    );
    assert_eq!(
        results[0].bytes.pointer("/confirmation/decision"),
        Some(&json!("approve"))
    );
}
