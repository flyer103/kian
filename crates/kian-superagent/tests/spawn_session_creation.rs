//! (HARNESS-BRIDGE §4.7) A spawn creates the worker session **before** admitting its input, so
//! `session/created` can carry the bridge marker the first drain cannot derive.

use std::sync::Arc;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, SpawnSessionWriter, WorkerSpawner, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

struct RecordingInbox {
    calls: Mutex<Vec<String>>,
    fail_create: AtomicBool,
}

impl RecordingInbox {
    fn shared(fail_create: bool) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fail_create: AtomicBool::new(fail_create),
        })
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl SpawnSessionWriter for RecordingInbox {
    fn create_session(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<(), InboxWriteError>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("create:{}", session.0));
        let fail = self.fail_create.load(Ordering::Acquire);
        Box::pin(async move {
            if fail {
                Err(InboxWriteError::Write("boom".into()))
            } else {
                Ok(())
            }
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.calls.lock().unwrap().push("admit".into());
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        self.calls.lock().unwrap().push("wake".into());
        WakeOutcome::Folded
    }
}

#[tokio::test]
async fn a_spawn_creates_the_session_before_admitting_and_waking() {
    let inbox = RecordingInbox::shared(false);
    let template = worker_template("worker", "worker", 7);
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(sid(1)),
        &template,
        inbox.clone(),
    )
    .with_session_creation(inbox.clone() as Arc<dyn SpawnSessionWriter>);
    let spawned = spawner.spawn(json!("deploy"), None).await.expect("spawn");

    assert_eq!(
        inbox.calls(),
        vec![
            format!("create:{}", spawned.worker_session.0),
            "admit".to_owned(),
            "wake".to_owned(),
        ]
    );
}

#[tokio::test]
async fn a_failed_creation_refuses_the_spawn_without_admitting() {
    let inbox = RecordingInbox::shared(true);
    let template = worker_template("worker", "worker", 7);
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(sid(1)),
        &template,
        inbox.clone(),
    )
    .with_session_creation(inbox.clone() as Arc<dyn SpawnSessionWriter>);
    let err = spawner.spawn(json!("deploy"), None).await.unwrap_err();
    assert!(err.to_string().contains("boom"), "{err}");
    assert_eq!(
        inbox.calls().len(),
        1,
        "no admit/wake after a failed create"
    );
}
