//! ADR-047: the harness handle is persisted on the turn's `turn/end` and used by the next episode.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_harness::event::{
    ApprovalAnswer, Capabilities, ContractError, Control, HarnessSession, Mapped, SessionEvent,
    SteerMode,
};
use kian_harness::testing::MemoryCtx;
use kian_harness::{BridgeDriver, BridgeDriverConfig, SessionFactory};
use serde_json::json;

const CAPS: Capabilities = Capabilities {
    name: "fake",
    request_bytes: false,
    steer: SteerMode::None,
    interrupt: false,
    approval_gate: false,
    resume: true,
};

struct FakeSession {
    finished: bool,
}

impl HarnessSession for FakeSession {
    fn capabilities(&self) -> Capabilities {
        CAPS
    }

    fn next(&mut self) -> BoxFuture<'_, Result<SessionEvent, ContractError>> {
        Box::pin(async move {
            if !self.finished {
                self.finished = true;
                Ok(SessionEvent::Mapped(Mapped::Event {
                    kind: EventKind::TurnEnd,
                    payload: json!({"outcome": "final_answer"}),
                }))
            } else {
                Ok(SessionEvent::Exit { code: Some(0) })
            }
        })
    }

    fn send(&mut self, _control: Control) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async { Ok(()) })
    }

    fn answer(&mut self, _answer: ApprovalAnswer) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async { Ok(()) })
    }

    fn resume_handle(&self) -> Option<String> {
        Some("h-1".to_owned())
    }
}

#[derive(Default)]
struct Factory {
    spawns: Mutex<Vec<Option<String>>>,
}

impl SessionFactory for Factory {
    fn capabilities(&self) -> Capabilities {
        CAPS
    }

    fn spawn(
        &self,
        _session: SessionId,
        resume: Option<String>,
    ) -> BoxFuture<'static, Result<Box<dyn HarnessSession>, String>> {
        self.spawns.lock().expect("spawns").push(resume);
        Box::pin(async { Ok(Box::new(FakeSession { finished: false }) as Box<dyn HarnessSession>) })
    }
}

#[tokio::test]
async fn the_handle_is_persisted_and_used_on_the_next_episode() {
    let factory = Arc::new(Factory::default());
    let ctx = Arc::new(MemoryCtx::new(SessionId::new()));
    ctx.push_input(json!({"text": "hi"}));
    let config = BridgeDriverConfig::default();

    let first: Vec<LoopOutcome> = BridgeDriver::new(factory.clone(), config.clone())
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(first, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let events = ctx.committed();
    let end = events
        .iter()
        .find(|event| event.kind == EventKind::TurnEnd)
        .expect("turn/end");
    assert_eq!(
        end.bytes.pointer("/harness/resume"),
        Some(&json!("h-1")),
        "the handle is durable on the turn"
    );

    // A different driver (a new process's drain) reads the handle from the same stream.
    ctx.push_input(json!({"text": "again"}));
    let second: Vec<LoopOutcome> = BridgeDriver::new(factory.clone(), config)
        .drive(ctx.clone())
        .collect()
        .await;
    assert_eq!(second, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let spawns = factory.spawns.lock().expect("spawns").clone();
    assert_eq!(spawns, vec![None, Some("h-1".to_owned())]);
}
