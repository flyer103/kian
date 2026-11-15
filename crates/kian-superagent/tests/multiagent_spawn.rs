//! (R15-C2) Acceptance probes for the spawn side of the multiagent
//! delegation core (plan §4 C2): the derived identity on
//! [`SpawnRequest`], the wrapper's pin-P6 class-B task-identity keys,
//! and the pure report builder.
//!
//! Every fixture (ids, template, inbox fake) is authored here. The
//! legacy path is the byte-identity control: without an identity the
//! two random mints and every landed wrapper byte are unchanged, and
//! the four task-identity keys are absent (never `null`). No network,
//! no daemon.

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, EXEC_WORKER_KEY, InboxWriteError, InboxWriter, SpawnIdentity, SpawnRequest,
    WorkerSpawner, child_session_id, child_thread_id, delegation_content,
    mandate_delegation_content, thread_report_content,
    worker::{SPECULATIVE_MARKER, spawn_identity_of, thread_report_content_ident},
    worker_template,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

/// An inbox fake that records every admit (with payload + key) and wake.
struct CaptureInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl CaptureInbox {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for CaptureInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

/// A spawner over a fixed router session and template (`mock-worker`
/// v7) whose inbox records every admit.
fn fixture(router: SessionId) -> (WorkerSpawner, Arc<CaptureInbox>) {
    let inbox = CaptureInbox::shared();
    let spawner = WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        inbox.clone(),
    );
    (spawner, inbox)
}

/// A request with everything except the identity set, so each probe
/// only names the leg under test.
fn base_request(identity: Option<SpawnIdentity>) -> SpawnRequest {
    SpawnRequest {
        task: json!("do the thing"),
        correlation: Some(json!("evt-9")),
        speculative: false,
        mandate: None,
        exec_worker: None,
        identity,
    }
}

const IDENTITY_KEYS: [&str; 4] = ["spawn_uid", "task_id", "agent_def", "agent_def_version"];

// ---------------------------------------------------------------------------
// 1. Legacy byte identity (the control for every additive change)
// ---------------------------------------------------------------------------

/// Without an identity the wrapper is EXACTLY the frozen builder's
/// bytes over the minted thread id — across every composition (plain,
/// speculative, mandate, exec_worker, and all of them together), the
/// legacy path is untouched.
#[test]
fn legacy_spawn_wrapper_bytes_are_unchanged_without_an_identity() {
    let (spawner, _) = fixture(sid(0x21));
    let task = json!("do the thing");
    let correlation = Some(json!("evt-9"));
    let mandate = json!({"strength": "explicit", "scope": "qa"});

    // Plain legacy: exact JSON equality against the golden form.
    let plain = spawner.prepare(&base_request(None));
    assert_eq!(
        plain.task_message["content"],
        delegation_content(plain.thread.id, correlation.clone(), task.clone())
    );
    // Serialized bytes too (the golden-string discipline).
    assert_eq!(
        serde_json::to_string(&plain.task_message["content"]).unwrap(),
        serde_json::to_string(&delegation_content(
            plain.thread.id,
            correlation.clone(),
            task.clone()
        ))
        .unwrap()
    );

    // Speculative: exactly delegation_content + the marker.
    let mut req = base_request(None);
    req.speculative = true;
    let spec = spawner.prepare(&req);
    let mut expected = delegation_content(spec.thread.id, correlation.clone(), task.clone());
    expected[SPECULATIVE_MARKER] = Value::Bool(true);
    assert_eq!(spec.task_message["content"], expected);

    // Mandate: exactly mandate_delegation_content.
    let mut req = base_request(None);
    req.mandate = Some(mandate.clone());
    let mand = spawner.prepare(&req);
    assert_eq!(
        mand.task_message["content"],
        mandate_delegation_content(mand.thread.id, correlation.clone(), task.clone(), &mandate)
    );

    // Binding: exactly delegation_content + the exec_worker key.
    let mut req = base_request(None);
    req.exec_worker = Some("mac-mini".to_string());
    let bound = spawner.prepare(&req);
    let mut expected = delegation_content(bound.thread.id, correlation.clone(), task.clone());
    expected[EXEC_WORKER_KEY] = json!("mac-mini");
    assert_eq!(bound.task_message["content"], expected);

    // Fully composed legacy form: everything at once, still exactly
    // the composed builders' bytes.
    let composed = spawner.prepare(&SpawnRequest {
        task: task.clone(),
        correlation: correlation.clone(),
        speculative: true,
        mandate: Some(mandate.clone()),
        exec_worker: Some("mac-mini".to_string()),
        identity: None,
    });
    let mut expected = mandate_delegation_content(composed.thread.id, correlation, task, &mandate);
    expected[SPECULATIVE_MARKER] = Value::Bool(true);
    expected[EXEC_WORKER_KEY] = json!("mac-mini");
    assert_eq!(composed.task_message["content"], expected);
}

// ---------------------------------------------------------------------------
// 2. Derived identity (pin P1)
// ---------------------------------------------------------------------------

/// Preparing the same identity twice derives the same thread id, the
/// same worker session, and the same wrapper — a replay never mints a
/// second child (M4). The ids are exactly the pin-P1 derivations.
#[test]
fn derived_spawn_mints_the_thread_and_session_from_the_spawn_uid() {
    let router = sid(0x22);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 12, "mock-worker", 7);

    let first = spawner.prepare(&base_request(Some(identity.clone())));
    let second = spawner.prepare(&base_request(Some(identity.clone())));

    // A replay recomputes the same child — identical everything.
    assert_eq!(first.thread.id, second.thread.id, "thread id is derived");
    assert_eq!(
        first.worker_session, second.worker_session,
        "worker session is derived"
    );
    assert_eq!(
        first.task_message, second.task_message,
        "wrapper is derived"
    );

    // Exactly the pin-P1 derivations, and task_id == thread id (P7).
    assert_eq!(first.thread.id, child_thread_id(router, identity.spawn_uid));
    assert_eq!(first.worker_session, child_session_id(first.thread.id));
    assert_eq!(first.thread.id, identity.task_id);
    assert_eq!(first.thread.parent_session, router);
    assert_eq!(first.thread.pinned_version, 7);

    // A different committed seq is a different spawn: different child.
    let other = SpawnIdentity::derive(router, 13, "mock-worker", 7);
    let third = spawner.prepare(&base_request(Some(other)));
    assert_ne!(third.thread.id, first.thread.id);
    assert_ne!(third.worker_session, first.worker_session);
}

// ---------------------------------------------------------------------------
// 3./4. The pin-P6 wrapper keys — present on new wrappers, absent on legacy
// ---------------------------------------------------------------------------

/// The multiagent wrapper carries exactly the four pin-P6 task
/// identity keys with the identity's values, and `assignee` is absent
/// (P2 — the roster does not exist before P2).
#[test]
fn multiagent_wrapper_carries_the_four_task_identity_keys() {
    let router = sid(0x23);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 4, "mock-worker", 7);
    let spawned = spawner.prepare(&base_request(Some(identity.clone())));
    let wrapper = &spawned.task_message["content"];

    assert_eq!(wrapper["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(wrapper["task_id"], json!(identity.task_id_str()));
    assert_eq!(wrapper["agent_def"], json!("mock-worker"));
    assert_eq!(wrapper["agent_def_version"], json!(7));
    // The wrapper's own thread_id names the same derived thread (P7).
    assert_eq!(wrapper["thread_id"], json!(identity.task_id.to_string()));
    // P2's `assignee` is meaningless before the roster exists.
    assert!(wrapper.get("assignee").is_none(), "assignee is P2, absent");
    // Exactly four keys over the legacy form for the same thread id.
    let legacy = delegation_content(
        spawned.thread.id,
        Some(json!("evt-9")),
        json!("do the thing"),
    );
    assert_eq!(
        wrapper.as_object().map(serde_json::Map::len),
        Some(legacy.as_object().map(serde_json::Map::len).unwrap() + 4),
        "exactly the four identity keys are added"
    );
}

/// On the legacy path the four keys are ABSENT — never written, never
/// `null` — across every composition.
#[test]
fn task_identity_keys_are_absent_not_null_on_the_legacy_path() {
    let (spawner, _) = fixture(sid(0x24));
    let compositions = [
        base_request(None),
        SpawnRequest {
            speculative: true,
            ..base_request(None)
        },
        SpawnRequest {
            mandate: Some(json!({"strength": "explicit", "scope": "qa"})),
            ..base_request(None)
        },
        SpawnRequest {
            exec_worker: Some("mac-mini".to_string()),
            ..base_request(None)
        },
    ];
    for req in &compositions {
        let spawned = spawner.prepare(req);
        let wrapper = &spawned.task_message["content"];
        for key in IDENTITY_KEYS {
            assert!(
                wrapper.get(key).is_none(),
                "legacy wrapper must not carry `{key}` (absent, never null)"
            );
        }
        let serialized = wrapper.to_string();
        for key in IDENTITY_KEYS {
            assert!(
                !serialized.contains(&format!("\"{key}\"")),
                "legacy wrapper bytes must not mention `{key}`"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 5. The admit key (pin P3)
// ---------------------------------------------------------------------------

/// The task-input key is `thread:<thread_id>` on BOTH paths, so a
/// retried admit de-dupes either way: every retry carries the
/// identical key and the identical admitted wrapper bytes.
#[tokio::test]
async fn task_input_key_is_thread_scoped_on_both_paths() {
    let router = sid(0x25);
    let (spawner, inbox) = fixture(router);

    // Legacy path: admit twice, both carries fold onto one row key.
    let legacy = spawner.prepare(&base_request(None));
    spawner.admit(&legacy).await.expect("legacy admit");
    spawner.admit(&legacy).await.expect("legacy re-admit");
    {
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 2, "both retries reached the inbox fake");
        for (session, payload, key) in &admits {
            assert_eq!(*session, legacy.worker_session);
            assert_eq!(payload, &legacy.task_message["content"]);
            assert_eq!(
                key.as_deref(),
                Some(format!("thread:{}", legacy.thread.id).as_str())
            );
        }
    }

    // Derived path: same thread-scoped key over the derived thread id.
    let identity = SpawnIdentity::derive(router, 9, "mock-worker", 7);
    let derived = spawner.prepare(&base_request(Some(identity.clone())));
    spawner.admit(&derived).await.expect("derived admit");
    spawner.admit(&derived).await.expect("derived re-admit");
    let admits = inbox.admits.lock().unwrap().clone();
    let derived_admits: Vec<_> = admits.iter().skip(2).collect();
    assert_eq!(derived_admits.len(), 2);
    for (session, payload, key) in derived_admits {
        assert_eq!(*session, derived.worker_session);
        assert_eq!(payload, &derived.task_message["content"]);
        // Pin P3: unchanged `thread:<thread_id>` key — the derived
        // thread id is the identity's task id (P7), so the row folds.
        assert_eq!(
            key.as_deref(),
            Some(format!("thread:{}", identity.task_id).as_str())
        );
        assert_eq!(
            key.as_deref(),
            Some(format!("thread:{}", derived.thread.id).as_str())
        );
        // The admitted wrapper carries the identity keys (the durable
        // row the child side later reads them back from).
        assert_eq!(payload["spawn_uid"], json!(identity.spawn_uid.to_string()));
    }
}

// ---------------------------------------------------------------------------
// 6./7. The report builder (pin P6 class-C)
// ---------------------------------------------------------------------------

/// The report content is a pure function of the identity and the
/// committed verdict: same inputs re-derive the same bytes, and the
/// bytes are exactly the legacy triple plus the additive keys.
#[test]
fn report_content_is_a_pure_function_of_the_identity_and_the_committed_verdict() {
    let identity = SpawnIdentity::derive(sid(0x26), 5, "mock-worker", 7);
    let contract = json!({"passed": false, "errors": ["has_artifact"]});
    let summary = json!({"text": "done", "tokens": 42});

    let a = thread_report_content_ident(
        &identity,
        "completed",
        summary.clone(),
        Some(contract.clone()),
    );
    let b = thread_report_content_ident(
        &identity,
        "completed",
        summary.clone(),
        Some(contract.clone()),
    );
    assert_eq!(a, b, "same events re-derive the same bytes");

    // Exactly the legacy triple plus the additive keys, in that
    // composition (the triple itself is untouched).
    let mut expected = thread_report_content(identity.task_id, "completed", summary.clone());
    expected["spawn_uid"] = json!(identity.spawn_uid.to_string());
    expected["task_id"] = json!(identity.task_id_str());
    expected["contract"] = contract;
    assert_eq!(a, expected);

    // The legacy triple is byte-identical inside the derived report.
    assert_eq!(a["thread_id"], json!(identity.task_id.to_string()));
    assert_eq!(a["status"], json!("completed"));
    assert_eq!(a["summary"], summary);

    // A failed verdict with no failed checks is the same pure shape.
    let passed = json!({"passed": true, "errors": []});
    assert_eq!(
        thread_report_content_ident(&identity, "completed", summary, Some(passed.clone()))["contract"],
        passed
    );
}

/// Without a committed gate verdict the `contract` key is absent —
/// never `null` — and the legacy builder itself is unchanged.
#[test]
fn report_content_omits_contract_when_no_gate_verdict_was_committed() {
    let identity = SpawnIdentity::derive(sid(0x27), 6, "mock-worker", 7);
    let summary = json!({"text": "no gate ran"});
    let report = thread_report_content_ident(&identity, "failed", summary.clone(), None);
    assert!(report.get("contract").is_none(), "absent, never null");
    assert!(!report.to_string().contains("\"contract\""));
    // The identity keys still ride.
    assert_eq!(report["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(report["task_id"], json!(identity.task_id_str()));
    // The legacy builder is not modified: its bytes stay the frozen
    // golden triple.
    assert_eq!(
        thread_report_content(identity.task_id, "failed", summary),
        json!({
            "thread_id": identity.task_id.to_string(),
            "status": "failed",
            "summary": json!({"text": "no gate ran"}),
        })
    );
}

// ---------------------------------------------------------------------------
// 8. The wrapper reader
// ---------------------------------------------------------------------------

/// `spawn_identity_of` round-trips a multiagent wrapper back to the
/// identity and returns `None` for legacy wrappers (and partial ones).
#[test]
fn spawn_identity_of_round_trips_and_returns_none_for_legacy_wrappers() {
    let router = sid(0x28);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 8, "mock-worker", 7);

    let spawned = spawner.prepare(&base_request(Some(identity.clone())));
    assert_eq!(
        spawn_identity_of(&spawned.task_message["content"]),
        Some(identity),
        "round trips off the wrapper bytes alone"
    );

    // Legacy wrapper: the keys are absent, so no identity.
    let legacy = spawner.prepare(&base_request(None));
    assert_eq!(spawn_identity_of(&legacy.task_message["content"]), None);

    // Partial wrapper (identity keys stripped one at a time): never
    // guessed, never reconstructed.
    let wrapper = spawned.task_message["content"].clone();
    for key in IDENTITY_KEYS {
        let mut partial = wrapper.clone();
        partial.as_object_mut().unwrap().remove(key);
        assert_eq!(spawn_identity_of(&partial), None, "partial without `{key}`");
    }
}
