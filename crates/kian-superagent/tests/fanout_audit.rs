//! C38 fan-out audit-log tests. This file is deliberately its own test
//! binary: each `tests/*.rs` runs in its own process, so the global
//! tracing subscriber installed here owns the process's single global
//! slot — no race with the Phase-2 `install_log_capture` fixture in
//! `tests/subscriptions.rs`, and no dependence on test order.
//!
//! Why a process-global install instead of thread-local
//! `set_default`: the `tracing` macros skip an event at the callsite
//! when the process-wide callsite Interest cache says "never", and
//! that cache is only (re)computed against the *global* default —
//! `set_global_default` rebuilds it, thread-locals never participate.
//! A `set_default` capture therefore only observes library events if
//! some other test happened to install a global subscriber first
//! (order-parasitic). Installing the global first thing in a fresh
//! process makes the capture deterministic under any filter, thread
//! count, or future fixture changes.

use std::sync::{Arc, Mutex, Once};

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::ChannelEvent;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, CorrelateBy, InboxWriteError, InboxWriter, Route, RoutingOutcome, RuleMatcher,
    SubscriptionRule, SubscriptionRuleSet, SubscriptionService,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Log capture (process-global, this binary only)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MakeShared(SharedWriter);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.0.clone()
    }
}

/// Installs this binary's one global subscriber (first call wins —
/// every test here goes through it) and returns the capture buffer.
fn install_audit_capture() -> Arc<Mutex<Vec<u8>>> {
    static INSTALL: Once = Once::new();
    let buf = Arc::new(Mutex::new(Vec::new()));
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(SharedWriter(Arc::clone(&buf))))
            .finish();
        // Fresh process: this cannot lose the global slot, and the
        // install rebuilds the callsite Interest cache so the library
        // audit events are dispatched from the very first event on.
        tracing::subscriber::set_global_default(subscriber)
            .expect("fanout_audit owns its process's global subscriber");
    });
    buf
}

/// Lines of the captured log that contain every needle.
fn audit_lines<'a>(text: &'a str, needles: &[&str]) -> Vec<&'a str> {
    text.lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .collect()
}

// ---------------------------------------------------------------------------
// Engine-free inbox fixture (the audit lines are emitted by the
// subscription service itself; no store or engine is needed to observe
// them — durable-row semantics are covered in tests/subscriptions.rs)
// ---------------------------------------------------------------------------

struct RecordingInbox {
    fail_for: Mutex<Option<SessionId>>,
    admitted: Mutex<Vec<(SessionId, String)>>,
    woken: Mutex<Vec<SessionId>>,
}

impl RecordingInbox {
    fn new(fail_for: Option<SessionId>) -> Arc<Self> {
        Arc::new(Self {
            fail_for: Mutex::new(fail_for),
            admitted: Mutex::new(Vec::new()),
            woken: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        _payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        if *self.fail_for.lock().unwrap() == Some(session) {
            return Box::pin(async { Err(InboxWriteError::Write("scripted flake".into())) });
        }
        self.admitted
            .lock()
            .unwrap()
            .push((session, idempotency_key.unwrap_or_default()));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.woken.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

fn rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic("pr-9".into()),
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        },
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic("pr-unbound".into()),
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        },
    ])
}

fn ci_event(event_id: &str, topic: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "ci".into(),
        event_id: event_id.into(),
        topic: Some(topic.into()),
        sender: None,
        group: None,
        payload: json!({"build": "ok"}),
        ts_ms: 1,
    }
}

/// C38 audit acceptance, filter-independent by construction: the audit
/// log names **every** fan-out admission (one line per subscriber,
/// naming the session and the event id), names the dropped subscriber
/// on a mid-set write failure while the other's admission is still
/// named, and names the empty-set fallback to the Router.
#[tokio::test]
async fn audit_log_names_every_fanout_admission_and_drop() {
    let logs = install_audit_capture();

    let router = SessionId::new();
    let worker_a = SessionId::new();
    let worker_b = SessionId::new();

    // --- Two subscribers: one line per admission, each naming its
    // session and the shared event id. ---
    let inbox = RecordingInbox::new(None);
    let service =
        SubscriptionService::new(router, rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
    service.register_topic("pr-9", worker_a);
    service.register_topic("pr-9", worker_b);

    let outcome = service
        .handle_event(ci_event("evt-audit", "pr-9"))
        .await
        .unwrap();
    assert!(matches!(outcome, RoutingOutcome::FannedOut { .. }));

    let text = {
        let bytes = logs.lock().unwrap().clone();
        String::from_utf8(bytes).expect("utf8 log")
    };
    for worker in [worker_a, worker_b] {
        assert_eq!(
            audit_lines(
                &text,
                &[
                    "subscription audit: event admitted",
                    &worker.0.to_string(),
                    "evt-audit"
                ]
            )
            .len(),
            1,
            "expected exactly one admission audit line naming {worker:?} for evt-audit, got:\n{text}"
        );
    }

    // --- Mid-set failure: the drop names the failed subscriber; the
    // other's admission is still named. ---
    let flaky = RecordingInbox::new(Some(worker_a));
    let service =
        SubscriptionService::new(router, rules(), Arc::clone(&flaky) as Arc<dyn InboxWriter>);
    service.register_topic("pr-9", worker_a);
    service.register_topic("pr-9", worker_b);

    let err = service
        .handle_event(ci_event("evt-flake", "pr-9"))
        .await
        .unwrap_err();
    assert!(matches!(err, InboxWriteError::Write(_)));

    let text = {
        let bytes = logs.lock().unwrap().clone();
        String::from_utf8(bytes).expect("utf8 log")
    };
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: admit failed",
                &worker_a.0.to_string(),
                "evt-flake"
            ]
        )
        .len(),
        1,
        "expected the drop audit line naming the failed subscriber, got:\n{text}"
    );
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: event admitted",
                &worker_b.0.to_string(),
                "evt-flake"
            ]
        )
        .len(),
        1,
        "expected worker b's admission named despite a's failure, got:\n{text}"
    );

    // --- Empty set: the fallback to the Router is named. ---
    let inbox = RecordingInbox::new(None);
    let service =
        SubscriptionService::new(router, rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
    let outcome = service
        .handle_event(ci_event("evt-fallback", "pr-unbound"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        }
    );

    let text = {
        let bytes = logs.lock().unwrap().clone();
        String::from_utf8(bytes).expect("utf8 log")
    };
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: passthrough topic unbound, falling back to router",
                "evt-fallback"
            ]
        )
        .len(),
        1,
        "expected the empty-set fallback audit line, got:\n{text}"
    );
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: event admitted",
                &router.0.to_string(),
                "evt-fallback"
            ]
        )
        .len(),
        1,
        "expected the Router's admission named for the fallback, got:\n{text}"
    );
}
