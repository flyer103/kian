//! The Codex adapter through the real shared driver, against a scripted `app-server` child.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_codex::CodexAdapter;
use kian_codex::codec::CodexConfig;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::ApprovalPolicy;
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig};
use serde_json::json;

fn adapter(mode: &str) -> CodexAdapter {
    CodexAdapter::new(CodexConfig {
        command: PathBuf::from(env!("CARGO_BIN_EXE_kian-codex-fixture-peer"))
            .display()
            .to_string(),
        args: vec![mode.to_owned()],
        model: None,
        cwd: None,
        approval_policy: "on-request".to_owned(),
        env: Vec::new(),
        startup_timeout: Duration::from_secs(5),
        capture_base: None,
    })
}

async fn drive(
    mode: &str,
    config: BridgeDriverConfig,
    input: &str,
) -> (Vec<LoopOutcome>, Arc<MemoryCtx>) {
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": input}));
    let outcomes: Vec<LoopOutcome> = BridgeDriver::new(Arc::new(adapter(mode)), config)
        .drive(ctx.clone())
        .collect()
        .await;
    (outcomes, ctx)
}

#[tokio::test]
async fn a_simple_turn_maps_to_the_frozen_kinds_and_settles() {
    let (outcomes, ctx) = drive("simple", BridgeDriverConfig::default(), "hi").await;
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
    assert_eq!(
        events[2].bytes.pointer("/content/text"),
        Some(&json!("pong"))
    );
    assert_eq!(events[3].bytes["outcome"], "final_answer");
    assert!(events.iter().all(|e| e.kind != EventKind::StepRequest));
}

#[tokio::test]
async fn a_denied_command_approval_is_recorded_and_declined() {
    let (outcomes, ctx) = drive("approval", BridgeDriverConfig::default(), "rm please").await;
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let events = ctx.committed();
    let call = events
        .iter()
        .find(|e| e.kind == EventKind::ToolCall)
        .expect("tool/call");
    assert_eq!(
        call.bytes.pointer("/content/permission/evaluated"),
        Some(&json!("deny"))
    );
    assert_eq!(
        call.bytes.pointer("/content/name"),
        Some(&json!("commandExecution"))
    );
    let result = events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .expect("tool/result");
    assert_eq!(
        result.bytes.pointer("/content/is_error"),
        Some(&json!(true))
    );
    let _ = ApprovalPolicy::deny_all();
}

#[tokio::test]
async fn a_child_that_dies_mid_turn_is_a_soft_failure() {
    let (outcomes, ctx) = drive("crash", BridgeDriverConfig::default(), "hi").await;
    match outcomes.as_slice() {
        [LoopOutcome::SoftFailed { reason }] => assert!(reason.contains("mid-turn"), "{reason}"),
        other => panic!("expected one soft failure, got {other:?}"),
    }
    assert_eq!(
        ctx.kinds(),
        vec![EventKind::InputAdmitted, EventKind::TurnStart]
    );
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

/// A `tool/pre` deny flips an allowed Codex command approval to a denial with hook provenance.
#[tokio::test]
async fn a_tool_pre_deny_flips_an_allowed_command_to_policy_hook() {
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "rm please"}));
    let policy = ApprovalPolicy {
        default: kian_harness::Decision::Allow,
        rules: Vec::new(),
    };
    let outcomes: Vec<LoopOutcome> = BridgeDriver::new(
        Arc::new(adapter("approval")),
        BridgeDriverConfig {
            policy,
            max_steps: 32,
            labels: Default::default(),
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
