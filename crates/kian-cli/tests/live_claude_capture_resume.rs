//! Env-gated live smoke: Claude Code capture (the loopback proxy records the exact request as
//! `step/request`) and conversation resume across drains (`--resume`, ADR-046/ADR-047).
//!
//! `KIAN_CLAUDE_BIN` absent → SKIP. `KIAN_CLAUDE_LIVE_CAPTURE=1` gates the real model call; the
//! provider is the shell's `ANTHROPIC_BASE_URL`/`ANTHROPIC_AUTH_TOKEN`/`ANTHROPIC_API_KEY`
//! (never printed, never persisted).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_claude::ClaudeAdapter;
use kian_claude::mapping::ClaudeConfig;
use kian_cli::capture::CaptureHub;
use kian_cli::config::CaptureSettings;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::ApprovalPolicy;
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig};
use serde_json::json;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn driver_config() -> BridgeDriverConfig {
    BridgeDriverConfig {
        policy: ApprovalPolicy::deny_all(),
        max_steps: 8,
        labels: Default::default(),
        step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
    }
}

fn claude_config(bin: &str, base: &str, env_vars: Vec<(String, String)>) -> ClaudeConfig {
    ClaudeConfig {
        command: bin.to_owned(),
        args: Vec::new(),
        model: env("ANTHROPIC_MODEL"),
        env: env_vars,
        capture_base: Some(base.to_owned()),
    }
}

async fn run_driver(
    bin: &str,
    base: &str,
    env_vars: &[(String, String)],
    hub: &Arc<CaptureHub>,
    ctx: &Arc<MemoryCtx>,
) -> Vec<LoopOutcome> {
    let driver = BridgeDriver::new(
        Arc::new(ClaudeAdapter::new(claude_config(
            bin,
            base,
            env_vars.to_vec(),
        ))),
        driver_config(),
    )
    .with_capture_source(Arc::clone(hub) as Arc<dyn kian_harness::CaptureSource>);
    tokio::time::timeout(
        Duration::from_secs(240),
        driver.drive(ctx.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("live Claude run timed out")
}

fn answers(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|event| event.kind == EventKind::AssistantMessage)
        .filter_map(|event| {
            event
                .bytes
                .pointer("/content/text")
                .and_then(|text| text.as_str())
                .map(str::to_owned)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_claude_capture_and_resume() {
    let Ok(bin) = std::env::var("KIAN_CLAUDE_BIN") else {
        eprintln!("SKIP: KIAN_CLAUDE_BIN not set (live Claude capture/resume smoke)");
        return;
    };
    if std::env::var("KIAN_CLAUDE_LIVE_CAPTURE").as_deref() != Ok("1") {
        eprintln!("SKIP: KIAN_CLAUDE_LIVE_CAPTURE not set (real model call)");
        return;
    }
    let Some(upstream) = env("ANTHROPIC_BASE_URL") else {
        panic!("ANTHROPIC_BASE_URL is not set");
    };
    let upstream = upstream.trim_end_matches('/').to_owned();
    let mut env_vars = Vec::new();
    if let Some(token) = env("ANTHROPIC_AUTH_TOKEN") {
        env_vars.push(("ANTHROPIC_AUTH_TOKEN".to_owned(), token));
    }
    if let Some(key) = env("ANTHROPIC_API_KEY") {
        env_vars.push(("ANTHROPIC_API_KEY".to_owned(), key));
    }
    assert!(
        !env_vars.is_empty(),
        "ANTHROPIC_AUTH_TOKEN / ANTHROPIC_API_KEY must be set"
    );
    let hub = CaptureHub::start(CaptureSettings {
        max_body_bytes: 8 * 1024 * 1024,
        anthropic_upstream: upstream,
        openai_upstream: "https://api.openai.com".to_owned(),
    })
    .await
    .expect("capture hub");
    let base = hub.base_url();

    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "Reply with exactly the word: pong"}));
    let outcomes = run_driver(&bin, &base, &env_vars, &hub, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "turn one: {outcomes:?}"
    );
    let events = ctx.committed();
    let captured: Vec<&Event> = events
        .iter()
        .filter(|event| event.kind == EventKind::StepRequest)
        .collect();
    assert!(
        !captured.is_empty(),
        "capture must persist the exact request as step/request"
    );
    let first_body = captured[0].bytes.get("body").expect("step/request body");
    assert!(
        first_body.to_string().contains("pong"),
        "the captured body is the model-visible request: {first_body}"
    );
    let first_answer = answers(&events).pop().unwrap_or_default();
    assert!(first_answer.contains("pong"), "no pong in {first_answer:?}");
    let handle = events
        .iter()
        .find(|event| event.kind == EventKind::TurnEnd)
        .and_then(|event| event.bytes.pointer("/harness/resume"))
        .and_then(|value| value.as_str())
        .expect("the CLI session id is the persisted resume handle");
    eprintln!(
        "live capture turn 1: {} request(s), body {} bytes, handle {handle}",
        captured.len(),
        first_body.to_string().len()
    );

    ctx.start_new_drain();
    ctx.push_input(json!({
        "text": "What single word did you just reply with? Reply with only that word."
    }));
    let outcomes = run_driver(&bin, &base, &env_vars, &hub, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "turn two: {outcomes:?}"
    );
    let events = ctx.committed();
    let captured: Vec<&Event> = events
        .iter()
        .filter(|event| event.kind == EventKind::StepRequest)
        .collect();
    assert!(
        captured.len() >= 2,
        "the resumed turn is captured too: {}",
        captured.len()
    );
    let resumed_answer = answers(&events).pop().unwrap_or_default();
    assert!(
        resumed_answer.contains("pong"),
        "the resumed Claude conversation lost its context: {resumed_answer:?}"
    );
    eprintln!(
        "live capture+resume OK: {} captured request(s); resumed answer {resumed_answer:?}",
        captured.len()
    );
}
