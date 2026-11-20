//! The env-gated live smoke for the Claude Code adapter: `KIAN_CLAUDE_BIN` absent → SKIP with a
//! log line, never fail (default CI needs no Claude install). Present → one real turn through the
//! bridge; the outcome must be honest either way (a protocol or auth failure is reported as a
//! named `SoftFailed`, never a fabricated completion).

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
use kian_harness::{BridgeDriver, BridgeDriverConfig};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_claude_turn_is_mapped_and_honest() {
    let Ok(bin) = std::env::var("KIAN_CLAUDE_BIN") else {
        eprintln!("SKIP: KIAN_CLAUDE_BIN not set (live Claude smoke)");
        return;
    };
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "Reply with exactly the word: pong"}));
    let driver = BridgeDriver::new(
        Arc::new(ClaudeAdapter::new(ClaudeConfig {
            command: bin,
            args: Vec::new(),
            model: None,
            env: Vec::new(),
        })),
        BridgeDriverConfig {
            policy: ApprovalPolicy::deny_all(),
            max_steps: 8,
        },
    );
    let outcomes = tokio::time::timeout(
        Duration::from_secs(180),
        driver.drive(ctx.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("live Claude run timed out");

    let events = ctx.committed();
    eprintln!(
        "live claude chain: {:?}",
        events
            .iter()
            .map(|e| (e.kind, e.bytes.clone()))
            .collect::<Vec<_>>()
    );
    eprintln!("live claude outcomes: {outcomes:?}");

    match outcomes.as_slice() {
        [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled] => {
            let has_answer = events.iter().any(|e| e.kind == EventKind::AssistantMessage);
            let outcome = events
                .iter()
                .find(|e| e.kind == EventKind::TurnEnd)
                .and_then(|e| e.bytes.get("outcome"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            assert!(
                has_answer || outcome == "error" || outcome == "cancelled",
                "settled without an answer or an honest error turn end: {events:?}"
            );
        }
        [LoopOutcome::SoftFailed { reason }] => {
            eprintln!("live claude honest failure: {reason}");
        }
        other => panic!("unexpected outcomes: {other:?}"),
    }
    assert!(
        events.iter().all(|e| e.kind != EventKind::StepRequest),
        "projection-only: no step/request"
    );
}
