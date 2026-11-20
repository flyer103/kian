//! The env-gated live smoke for the Codex adapter: `KIAN_CODEX_BIN` absent → SKIP with a log line,
//! never fail. Present → one real `codex app-server` turn; any protocol/auth failure must surface
//! as a named `SoftFailed`, never a fabricated completion.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_codex_turn_is_mapped_and_honest() {
    let Ok(bin) = std::env::var("KIAN_CODEX_BIN") else {
        eprintln!("SKIP: KIAN_CODEX_BIN not set (live Codex smoke)");
        return;
    };
    let cwd = std::env::temp_dir().join(format!("kian-codex-live-{}", std::process::id()));
    std::fs::create_dir_all(&cwd).expect("scratch cwd");

    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "Reply with exactly the word: pong"}));
    let driver = BridgeDriver::new(
        Arc::new(CodexAdapter::new(CodexConfig {
            command: bin,
            args: Vec::new(),
            model: None,
            cwd: Some(cwd.display().to_string()),
            approval_policy: "on-request".to_owned(),
            env: Vec::new(),
            startup_timeout: Duration::from_secs(90),
            capture_base: None,
        })),
        BridgeDriverConfig {
            policy: ApprovalPolicy::deny_all(),
            max_steps: 8,
            labels: Default::default(),
        },
    );
    let outcomes = tokio::time::timeout(
        Duration::from_secs(240),
        driver.drive(ctx.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("live Codex run timed out");

    let events = ctx.committed();
    eprintln!(
        "live codex chain: {:?}",
        events
            .iter()
            .map(|e| (e.kind, e.bytes.clone()))
            .collect::<Vec<_>>()
    );
    eprintln!("live codex outcomes: {outcomes:?}");

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
            eprintln!("live codex honest failure: {reason}");
        }
        other => panic!("unexpected outcomes: {other:?}"),
    }
    assert!(
        events.iter().all(|e| e.kind != EventKind::StepRequest),
        "projection-only: no step/request"
    );
    let _ = PathBuf::from(&cwd);
}
