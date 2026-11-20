//! ADR-048: a durable ask records the pending call and answers nothing; the drain settles.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::approval::{ApprovalPolicy, Decision};
use kian_harness::event::{
    ApprovalAnswer, ApprovalRequest, Capabilities, ContractError, Control, HarnessSession,
    SessionEvent, SteerMode,
};
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig, SessionFactory};
use serde_json::json;

const CAPS: Capabilities = Capabilities {
    name: "fake-ask",
    request_bytes: false,
    steer: SteerMode::None,
    interrupt: false,
    approval_gate: true,
    resume: false,
};

struct FakeSession {
    answered: Arc<Mutex<usize>>,
}

impl HarnessSession for FakeSession {
    fn capabilities(&self) -> Capabilities {
        CAPS
    }

    fn next(&mut self) -> BoxFuture<'_, Result<SessionEvent, ContractError>> {
        Box::pin(async {
            Ok(SessionEvent::Approval(ApprovalRequest {
                id: "req-1".to_owned(),
                call_id: Some("call-1".to_owned()),
                tool: "Bash".to_owned(),
                input: json!({"command": "ls"}),
            }))
        })
    }

    fn send(&mut self, _control: Control) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async { Ok(()) })
    }

    fn answer(&mut self, _answer: ApprovalAnswer) -> BoxFuture<'_, Result<(), ContractError>> {
        let answered = Arc::clone(&self.answered);
        Box::pin(async move {
            *answered.lock().expect("answers") += 1;
            Ok(())
        })
    }
}

struct Factory {
    answered: Arc<Mutex<usize>>,
}

impl SessionFactory for Factory {
    fn capabilities(&self) -> Capabilities {
        CAPS
    }

    fn spawn(
        &self,
        _session: SessionId,
        _resume: Option<String>,
    ) -> BoxFuture<'static, Result<Box<dyn HarnessSession>, String>> {
        let answered = Arc::clone(&self.answered);
        Box::pin(async move { Ok(Box::new(FakeSession { answered }) as Box<dyn HarnessSession>) })
    }
}

#[tokio::test]
async fn a_durable_ask_records_the_pending_call_and_answers_nothing() {
    let answered = Arc::new(Mutex::new(0));
    let factory = Arc::new(Factory {
        answered: Arc::clone(&answered),
    });
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let outcomes: Vec<LoopOutcome> = BridgeDriver::new(
        factory,
        BridgeDriverConfig {
            policy: ApprovalPolicy {
                default: Decision::Ask,
                rules: Vec::new(),
            },
            ..BridgeDriverConfig::default()
        },
    )
    .drive(ctx.clone())
    .collect()
    .await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let events = ctx.committed();
    let calls: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCall)
        .collect();
    assert_eq!(calls.len(), 1, "one pending call");
    assert_eq!(
        calls[0].bytes.pointer("/permission/evaluated"),
        Some(&json!("ask"))
    );
    assert_eq!(
        calls[0].bytes.pointer("/content/id"),
        Some(&json!("call-1"))
    );
    assert_eq!(
        calls[0].bytes.pointer("/content/arguments"),
        Some(&json!({"command": "ls"}))
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != EventKind::ToolResult),
        "nothing ran"
    );
    assert_eq!(
        *answered.lock().expect("answers"),
        0,
        "the harness was never answered"
    );
}
