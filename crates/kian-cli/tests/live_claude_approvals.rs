//! Env-gated live smoke: Claude Code's real tool-approval gate (allow/deny through `tool/pre`) and
//! the durable-`ask` re-issue experiment (ADR-045/ADR-048).
//!
//! `KIAN_CLAUDE_BIN` absent → SKIP. `KIAN_CLAUDE_LIVE_APPROVALS=1` gates the real model calls; the
//! provider is the shell's `ANTHROPIC_BASE_URL`/`ANTHROPIC_AUTH_TOKEN`/`ANTHROPIC_API_KEY`
//! (never printed, never persisted).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kian_claude::ClaudeAdapter;
use kian_claude::mapping::ClaudeConfig;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::{ApprovalPolicy, Decision, Rule};
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig};
use serde_json::json;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn driver_config(policy: ApprovalPolicy) -> BridgeDriverConfig {
    BridgeDriverConfig {
        policy,
        max_steps: 8,
        labels: Default::default(),
        step_timeout_ms: kian_harness::driver::DEFAULT_STEP_TIMEOUT_MS,
    }
}

fn claude_config(bin: &str) -> ClaudeConfig {
    let mut env_vars = Vec::new();
    if let Some(token) = env("ANTHROPIC_AUTH_TOKEN") {
        env_vars.push(("ANTHROPIC_AUTH_TOKEN".to_owned(), token));
    }
    if let Some(key) = env("ANTHROPIC_API_KEY") {
        env_vars.push(("ANTHROPIC_API_KEY".to_owned(), key));
    }
    ClaudeConfig {
        command: bin.to_owned(),
        args: Vec::new(),
        model: env("ANTHROPIC_MODEL"),
        env: env_vars,
        capture_base: None,
    }
}

async fn run(bin: &str, policy: ApprovalPolicy, ctx: &Arc<MemoryCtx>) -> Vec<LoopOutcome> {
    let driver = BridgeDriver::new(
        Arc::new(ClaudeAdapter::new(claude_config(bin))),
        driver_config(policy),
    );
    tokio::time::timeout(
        Duration::from_secs(240),
        driver.drive(ctx.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("live Claude run timed out")
}

fn events_of(events: &[Event], kind: EventKind) -> Vec<&Event> {
    events.iter().filter(|event| event.kind == kind).collect()
}

fn assistant_texts(events: &[Event]) -> Vec<String> {
    events_of(events, EventKind::AssistantMessage)
        .into_iter()
        .filter_map(|event| {
            event
                .bytes
                .pointer("/content/text")
                .and_then(|text| text.as_str())
                .map(str::to_owned)
        })
        .collect()
}

fn setup() -> Option<(String, Vec<(String, String)>)> {
    let Ok(bin) = std::env::var("KIAN_CLAUDE_BIN") else {
        eprintln!("SKIP: KIAN_CLAUDE_BIN not set (live Claude approvals smoke)");
        return None;
    };
    if std::env::var("KIAN_CLAUDE_LIVE_APPROVALS").as_deref() != Ok("1") {
        eprintln!("SKIP: KIAN_CLAUDE_LIVE_APPROVALS not set (real model calls)");
        return None;
    }
    let upstream = env("ANTHROPIC_BASE_URL").expect("ANTHROPIC_BASE_URL");
    let mut env_vars = vec![("ANTHROPIC_BASE_URL".to_owned(), upstream)];
    if let Some(token) = env("ANTHROPIC_AUTH_TOKEN") {
        env_vars.push(("ANTHROPIC_AUTH_TOKEN".to_owned(), token));
    }
    if let Some(key) = env("ANTHROPIC_API_KEY") {
        env_vars.push(("ANTHROPIC_API_KEY".to_owned(), key));
    }
    Some((bin, env_vars))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_claude_tool_allow_round_trip() {
    let Some((bin, _env_vars)) = setup() else {
        return;
    };
    let policy = ApprovalPolicy {
        default: Decision::Deny,
        rules: vec![Rule::parse("Bash(*)", Decision::Allow).expect("rule")],
    };
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({
        "text": "Run the shell command `rm -f /tmp/kian-live-approval-probe.txt` with the Bash tool, then reply with the exact command output."
    }));
    let outcomes = run(&bin, policy, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "allow turn: {outcomes:?}"
    );
    let events = ctx.committed();
    let calls = events_of(&events, EventKind::ToolCall);
    assert!(!calls.is_empty(), "the model used a tool: {events:?}");
    let allowed = calls
        .iter()
        .find(|event| event.bytes.pointer("/content/permission/evaluated") == Some(&json!("allow")))
        .unwrap_or_else(|| panic!("no allow provenance: {events:?}"));
    assert_eq!(allowed.bytes.pointer("/content/name"), Some(&json!("Bash")));
    let results = events_of(&events, EventKind::ToolResult);
    assert!(!results.is_empty(), "the tool result maps: {events:?}");
    assert!(
        results
            .iter()
            .any(|event| !event.bytes["content"]["is_error"]
                .as_bool()
                .unwrap_or(false)
                && event.bytes["content"]["output"]
                    .to_string()
                    .contains("Bash completed")),
        "the allowed tool really ran: {:?}",
        results.iter().map(|e| &e.bytes).collect::<Vec<_>>()
    );
    let texts = assistant_texts(&events);
    assert!(
        !texts.is_empty(),
        "the final answer reports the turn: {texts:?}"
    );
    eprintln!(
        "live allow OK: calls {} results {}",
        calls.len(),
        results.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_claude_tool_deny_round_trip() {
    let Some((bin, _env_vars)) = setup() else {
        return;
    };
    let policy = ApprovalPolicy::deny_all();
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({
        "text": "Run the shell command `rm -f /tmp/kian-live-approval-probe.txt` with the Bash tool, then reply with the exact command output."
    }));
    let outcomes = run(&bin, policy, &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "deny turn: {outcomes:?}"
    );
    let events = ctx.committed();
    let calls = events_of(&events, EventKind::ToolCall);
    assert!(
        calls
            .iter()
            .any(|event| event.bytes.pointer("/content/permission/evaluated")
                == Some(&json!("deny"))),
        "deny provenance: {events:?}"
    );
    let results = events_of(&events, EventKind::ToolResult);
    assert!(
        results
            .iter()
            .any(|event| event.bytes.pointer("/content/is_error") == Some(&json!(true))),
        "the denied call has an honest error result: {events:?}"
    );
    assert!(
        results.iter().any(|event| event.bytes["content"]["output"]
            .to_string()
            .contains("denied by kian approval policy")),
        "the denial reason is the honest tool result"
    );
    assert!(
        results.iter().all(|event| !event.bytes["content"]["output"]
            .to_string()
            .contains("Bash completed")),
        "the denied tool must not run"
    );
    eprintln!(
        "live deny OK: calls {} results {}",
        calls.len(),
        results.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_claude_durable_ask_reissue_experiment() {
    let Some((bin, _env_vars)) = setup() else {
        return;
    };
    let policy = || ApprovalPolicy {
        default: Decision::Ask,
        rules: Vec::new(),
    };
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({
        "text": "Run the shell command `rm -f /tmp/kian-live-approval-probe.txt` with the Bash tool."
    }));
    let outcomes = run(&bin, policy(), &ctx).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled], "the ask defers");
    let events = ctx.committed();
    let calls = events_of(&events, EventKind::ToolCall);
    assert_eq!(calls.len(), 1, "one pending call: {events:?}");
    let pending = calls[0]
        .bytes
        .pointer("/content/id")
        .and_then(|id| id.as_str())
        .expect("pending call id")
        .to_owned();
    assert_eq!(
        calls[0].bytes.pointer("/permission/evaluated"),
        Some(&json!("ask")),
        "pending provenance: {events:?}"
    );
    assert!(events_of(&events, EventKind::ToolResult).is_empty());

    let intent = kian_core::control::ConfirmIntent {
        call_id: pending.clone(),
        decision: kian_core::control::ConfirmDecision::Approve,
        reason: None,
        requested_by: Some("live-operator".to_owned()),
        answered_at_ms: 1,
        expired: false,
    };
    kian_harness::ctx::BridgeCtx::append(&*ctx, EventKind::ControlConfirm, intent.event_payload())
        .await
        .expect("seed the durable decision");
    ctx.start_new_drain();
    let outcomes = run(&bin, policy(), &ctx).await;
    assert!(
        matches!(
            outcomes.as_slice(),
            [LoopOutcome::Settled] | [LoopOutcome::TurnDone, LoopOutcome::Settled]
        ),
        "resumed ask turn: {outcomes:?}"
    );
    let events = ctx.committed();
    let calls = events_of(&events, EventKind::ToolCall);
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::InputAdmitted
                && event.bytes.pointer("/synthetic") == Some(&json!(true))),
        "the decision nudge is persisted as synthetic input"
    );
    let results = events_of(&events, EventKind::ToolResult);
    // A consumed confirmation closed the call by running it; a close-out result carries the same
    // provenance but `is_error: true` and the "not re-issued" reason.
    let executed = results.iter().find(|event| {
        event.bytes.pointer("/confirmation/decision").is_some()
            && event.bytes.pointer("/content/is_error") != Some(&json!(true))
    });
    if let Some(result) = executed {
        assert_eq!(
            result.bytes.pointer("/confirmation/decision"),
            Some(&json!("approve"))
        );
        assert_eq!(
            result.bytes.pointer("/content/call_id"),
            Some(&json!(pending))
        );
        assert_eq!(
            calls.len(),
            1,
            "the identical re-issue keeps one call record; events: {events:?}"
        );
        eprintln!("live durable ask: the harness RE-ISSUED the approved call and it ran");
    } else {
        assert!(
            results.iter().any(|event| {
                event.bytes.pointer("/content/output/reason")
                    == Some(&json!("confirmed call was not re-issued"))
            }),
            "no identical re-issue: the pending call must close honestly; events: {events:?}"
        );
        assert_eq!(
            results.len(),
            1,
            "the pending call is closed exactly once; events: {events:?}"
        );
        eprintln!(
            "live durable ask: the harness did NOT re-issue identically ({} ask call(s)); the pending call closed honestly",
            calls.len()
        );
    }
}
