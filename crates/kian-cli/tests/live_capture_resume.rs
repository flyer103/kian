//! Env-gated live smoke: Codex capture (the loopback proxy records the exact request as
//! `step/request`) and conversation resume across drains (ADR-046/ADR-047).
//!
//! `KIAN_CODEX_BIN` absent → SKIP. `KIAN_CODEX_LIVE_CAPTURE=1` gates the real model call; the API
//! key is read from `~/.codex/auth.json` into the child env only (never logged).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_cli::capture::CaptureHub;
use kian_cli::config::CaptureSettings;
use kian_codex::CodexAdapter;
use kian_codex::codec::CodexConfig;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::ApprovalPolicy;
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig};
use serde_json::json;

/// The default Codex provider (`~/.codex/config.toml`): base URL, bearer token, model. The live
/// smoke reuses the working provider account through the proxy instead of the API-key route.
fn codex_provider() -> Option<(String, String, String)> {
    let home = std::env::var("HOME").ok()?;
    let raw = std::fs::read_to_string(format!("{home}/.codex/config.toml")).ok()?;
    let mut section = String::new();
    let mut model = None;
    let mut base_url = None;
    let mut token = None;
    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            section = line.to_owned();
        } else if let Some(value) = line.strip_prefix("model = ") {
            model = Some(value.trim_matches('"').to_owned());
        } else if section == "[model_providers.deepseek]" {
            if let Some(value) = line.strip_prefix("base_url = ") {
                base_url = Some(value.trim_matches('"').trim_end_matches('/').to_owned());
            } else if let Some(value) = line.strip_prefix("experimental_bearer_token = ") {
                token = Some(value.trim_matches('"').to_owned());
            }
        }
    }
    Some((
        std::env::var("KIAN_CODEX_LIVE_UPSTREAM")
            .ok()
            .or(base_url)?,
        std::env::var("KIAN_CODEX_LIVE_TOKEN").ok().or(token)?,
        std::env::var("KIAN_CODEX_LIVE_MODEL").ok().or(model)?,
    ))
}

fn driver_config() -> BridgeDriverConfig {
    BridgeDriverConfig {
        policy: ApprovalPolicy::deny_all(),
        max_steps: 8,
        labels: Default::default(),
        step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
    }
}

fn codex_config(bin: &str, cwd: &str, key: &str, model: &str, base: &str) -> CodexConfig {
    CodexConfig {
        command: bin.to_owned(),
        args: Vec::new(),
        model: Some(model.to_owned()),
        cwd: Some(cwd.to_owned()),
        approval_policy: "on-request".to_owned(),
        env: vec![("OPENAI_API_KEY".to_owned(), key.to_owned())],
        startup_timeout: Duration::from_secs(90),
        capture_base: Some(base.to_owned()),
    }
}

async fn run_driver(
    bin: &str,
    cwd: &str,
    key: &str,
    model: &str,
    base: &str,
    hub: &Arc<CaptureHub>,
    ctx: &Arc<MemoryCtx>,
) -> Vec<LoopOutcome> {
    let driver = BridgeDriver::new(
        Arc::new(CodexAdapter::new(codex_config(bin, cwd, key, model, base))),
        driver_config(),
    )
    .with_capture_source(Arc::clone(hub) as Arc<dyn kian_harness::CaptureSource>);
    tokio::time::timeout(
        Duration::from_secs(240),
        driver.drive(ctx.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("live Codex run timed out")
}

fn answers(events: &[kian_core::events::Event]) -> Vec<String> {
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
async fn live_codex_capture_and_resume() {
    let Ok(bin) = std::env::var("KIAN_CODEX_BIN") else {
        eprintln!("SKIP: KIAN_CODEX_BIN not set (live Codex capture/resume smoke)");
        return;
    };
    if std::env::var("KIAN_CODEX_LIVE_CAPTURE").as_deref() != Ok("1") {
        eprintln!("SKIP: KIAN_CODEX_LIVE_CAPTURE not set (real model call)");
        return;
    }
    let Some((upstream, key, model)) = codex_provider() else {
        panic!("~/.codex/config.toml has no working deepseek provider");
    };
    let hub = CaptureHub::start(CaptureSettings {
        max_body_bytes: 8 * 1024 * 1024,
        anthropic_upstream: "https://api.anthropic.com".to_owned(),
        openai_upstream: upstream,
    })
    .await
    .expect("capture hub");
    let base = hub.base_url();
    let cwd = std::env::temp_dir().join(format!("kian-live-capture-{}", std::process::id()));
    std::fs::create_dir_all(&cwd).expect("scratch cwd");
    let cwd = cwd.display().to_string();

    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "Reply with exactly the word: pong"}));
    let outcomes = run_driver(&bin, &cwd, &key, &model, &base, &hub, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "turn one: {outcomes:?}"
    );
    let events = ctx.committed();
    eprintln!("turn1 outcomes: {outcomes:?}");
    for event in &events {
        let mut text = event.bytes.to_string();
        text.truncate(220);
        eprintln!("  {:?} {text}", event.kind);
    }
    let captured: Vec<_> = events
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
    eprintln!(
        "live capture turn 1: {} request(s), body {} bytes",
        captured.len(),
        first_body.to_string().len()
    );
    let first_answers = answers(&events);
    assert!(
        first_answers.iter().any(|answer| answer.contains("pong")),
        "no pong in {first_answers:?}"
    );

    ctx.start_new_drain();
    ctx.push_input(json!({
        "text": "What single word did you just reply with? Reply with only that word."
    }));
    let outcomes = run_driver(&bin, &cwd, &key, &model, &base, &hub, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "turn two: {outcomes:?}"
    );
    let events = ctx.committed();
    let captured: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::StepRequest)
        .collect();
    assert!(
        captured.len() >= 2,
        "the resumed turn is captured too: {}",
        captured.len()
    );
    let last_answer = answers(&events).pop().unwrap_or_default();
    assert!(
        last_answer.contains("pong"),
        "the resumed Codex conversation lost its context: {last_answer:?}"
    );
    eprintln!(
        "live capture+resume OK: {} captured request(s); resumed answer {last_answer:?}",
        captured.len()
    );
}
