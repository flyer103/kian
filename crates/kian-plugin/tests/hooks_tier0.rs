//! P1 integration: the tier-0 path through the public API (PLUGIN-SPEC
//! §3, §4.1, §4.6; K16). These tests exercise `HookBus` as the bundle
//! will — register handlers, build views, call the port-shaped methods —
//! and assert outcomes, counters and marker shapes, never log text (K9:
//! `kian-plugin` links no `tracing-subscriber`).

use std::sync::{Arc, Mutex};

use futures::FutureExt;
use kian_core::model::SessionId;
use kian_plugin::hooks::{
    AdmissionInfo, AdmitDraft, AdmitInput, AdmitTarget, FrameInfo, HandlerId, HookId, HookPayload,
    HookPorts, HookReply, Origin, PermissionView, Registry, SettleCtl, SettleView, ToolCallView,
    ToolPostView, ToolVerdict, is_veto_marker, veto_marker,
};

/// A bus over a fresh, isolated registry — tests never share the
/// process-global one (§5.3).
fn fresh_bus() -> kian_plugin::hooks::HookBus {
    kian_plugin::hooks::HookBus::with_registry(Arc::new(Registry::new()))
}

fn sid(s: &str) -> SessionId {
    serde_json::from_str(&format!("\"{s}\"")).expect("valid uuid literal")
}

const SESSION_A: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";
const SESSION_B: &str = "1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

fn tool_call(frame: Option<FrameInfo>) -> ToolCallView {
    ToolCallView {
        call_id: "call_1".to_string(),
        name: "exec".to_string(),
        iteration: 1,
        arguments: serde_json::json!({"cmd": ["ls"]}),
        arguments_bytes: 12,
        arguments_sha256: "ab".to_string(),
        arguments_truncated: false,
        permission: PermissionView {
            evaluated: "allow".to_string(),
            policy: "default".to_string(),
            matched: "exec".to_string(),
            reason: None,
        },
        surface: "react".to_string(),
        frame,
    }
}

fn tool_post(frame: Option<FrameInfo>) -> ToolPostView {
    ToolPostView {
        call_id: "call_1".to_string(),
        name: "exec".to_string(),
        is_error: false,
        output_bytes: 812,
        output_sha256: "cd".to_string(),
        redactions: Vec::new(),
        frame,
    }
}

/// A recording settle control: counts marker rows per plugin and records
/// every admitted synthetic input.
#[derive(Default)]
struct RecordingSettleCtl {
    admitted: Mutex<Vec<String>>,
    counts: Mutex<Vec<String>>, // rows naming plugins, by content prefix
}

impl SettleCtl for RecordingSettleCtl {
    fn admit_synthetic(
        &self,
        content: String,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        self.counts.lock().unwrap().push(content.clone());
        self.admitted.lock().unwrap().push(content);
        Box::pin(async { Ok(()) })
    }

    fn veto_count(&self, plugin: &str) -> u64 {
        // The stream-derived predicate §4.6 fixes: synthetic rows whose
        // content starts with the plugin's marker prefix.
        self.counts
            .lock()
            .unwrap()
            .iter()
            .filter(|row| is_veto_marker(plugin, true, row))
            .count() as u64
    }
}

#[tokio::test]
async fn tool_pre_deny_reaches_caller_and_observer_still_sees_dispatch() {
    let bus = fresh_bus();

    let seen: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    // An observe-only subscriber ordered first: it must still be
    // consulted even though a later gate denies (§8.3.3 — only a verdict
    // stops the walk, and an observer has none).
    let observer_seen = Arc::clone(&seen);
    let _keep_observer = bus.registry().register(
        HandlerId {
            plugin: "auditor",
            hook: HookId::ToolPre,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:tool"]).unwrap(),
            order: 0,
        },
        Arc::new(move |_payload: &HookPayload| {
            let seen = Arc::clone(&observer_seen);
            async move {
                seen.lock().unwrap().push("auditor");
                HookReply::Ack { note: None }
            }
            .boxed()
        }),
    );

    let gate_seen = Arc::clone(&seen);
    let _keep_gate = bus.registry().register(
        HandlerId {
            plugin: "approval-gate",
            hook: HookId::ToolPre,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:tool", "veto:tool"])
                .unwrap(),
            order: 1,
        },
        Arc::new(move |_payload: &HookPayload| {
            let seen = Arc::clone(&gate_seen);
            async move {
                seen.lock().unwrap().push("approval-gate");
                HookReply::BailDeny {
                    reason: "exec not on the allowlist".to_string(),
                }
            }
            .boxed()
        }),
    );

    // The caller's view of the denial: plugin + reason are exactly what
    // the honest-error template needs (§4.4 step 4, built by the caller
    // in P2).
    let verdict = bus
        .tool_pre(&tool_call(Some(FrameInfo::new(sid(SESSION_A)))))
        .await;
    assert_eq!(
        verdict,
        ToolVerdict::Deny {
            plugin: "approval-gate".to_string(),
            reason: "exec not on the allowlist".to_string(),
        },
        "the deny reaches the caller with its provenance"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["auditor", "approval-gate"],
        "the observer was dispatched before the gate's verdict stopped the walk"
    );

    // And through the port trait, as kian-loop will call it (K3).
    let ports: Arc<dyn HookPorts> = Arc::new(bus.clone());
    let verdict = ports
        .tool_pre(&tool_call(Some(FrameInfo::new(sid(SESSION_A)))))
        .await;
    assert!(matches!(verdict, ToolVerdict::Deny { .. }));

    // tool/post observes regardless of the pre outcome (§4.4 step 5).
    let post_seen = Arc::new(Mutex::new(0u32));
    let counter = Arc::clone(&post_seen);
    let _keep_post = bus.registry().register(
        HandlerId {
            plugin: "auditor",
            hook: HookId::ToolPost,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:tool"]).unwrap(),
            order: 0,
        },
        Arc::new(move |_payload: &HookPayload| {
            let counter = Arc::clone(&counter);
            async move {
                *counter.lock().unwrap() += 1;
                HookReply::Ack { note: None }
            }
            .boxed()
        }),
    );
    bus.tool_post(&tool_post(Some(FrameInfo::new(sid(SESSION_A)))))
        .await;
    assert_eq!(*post_seen.lock().unwrap(), 1);
}

#[tokio::test]
async fn settle_deny_returns_deny() {
    let bus = fresh_bus();

    let ctl = Arc::new(RecordingSettleCtl::default());
    let _keep = bus.registry().register(
        HandlerId {
            plugin: "quality-gate",
            hook: HookId::SettlePre,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:settle", "veto:settle"])
                .unwrap(),
            order: 0,
        },
        Arc::new(move |_payload: &HookPayload| {
            Box::pin(async {
                HookReply::BailDeny {
                    reason: "evidence summary missing".to_string(),
                }
            })
        }),
    );

    let view = SettleView {
        reason: "completed".to_string(),
        turns: 4,
        tool_calls: 7,
        evidence: "none".to_string(),
        vetoes_used: 0,
        vetoes_remaining: 3,
        episodes: 2,
        frame: Some(FrameInfo::new(sid(SESSION_A))),
    };

    // Budget remaining (0 of 3, K8): the marker row is admitted through
    // the ctl and the caller must not settle (§4.6 step 3).
    let verdict = bus.settle(&view, ctl.as_ref()).await;
    assert_eq!(
        verdict,
        kian_plugin::hooks::SettleVerdict::Deny {
            plugin: "quality-gate".to_string(),
            reason: "evidence summary missing".to_string(),
        }
    );
    let admitted = ctl.admitted.lock().unwrap().clone();
    assert_eq!(admitted.len(), 1, "one synthetic input, exactly");
    assert_eq!(
        admitted[0],
        veto_marker("quality-gate", "settle/pre", "evidence summary missing"),
        "the marker is the §4.6 step 3 template, byte-exact (K7)"
    );

    // Budget exhaustion: two more markers (three in the stream, counting
    // the one admitted above) → override, nothing admitted (§4.6 step 4).
    for _ in 0..2 {
        ctl.counts
            .lock()
            .unwrap()
            .push(veto_marker("quality-gate", "settle/pre", "again"));
    }
    ctl.admitted.lock().unwrap().clear();
    let verdict = bus.settle(&view, ctl.as_ref()).await;
    assert!(matches!(
        verdict,
        kian_plugin::hooks::SettleVerdict::Override { ref plugin, vetoes_used: 3, .. }
            if plugin == "quality-gate"
    ));
    assert!(
        ctl.admitted.lock().unwrap().is_empty(),
        "an override admits nothing"
    );

    // The counter predicate never reads role and requires synthetic
    // (§4.6's trust boundary): a non-synthetic lookalike does not count.
    assert!(!is_veto_marker("quality-gate", false, &admitted[0]));
    // The prefix includes the trailing space: `plugin=foo` never matches
    // `plugin=foobar` (§3.2.8).
    let lookalike = veto_marker("quality-gate-extra", "settle/pre", "x");
    assert!(!is_veto_marker("quality-gate", true, &lookalike));
}

#[tokio::test]
async fn settle_admit_failure_does_not_settle_and_consumes_budget() {
    // §4.6's three-step append-failure path: the gate does not settle
    // (AdmitFailed — the caller propagates the error through the existing
    // append-failure route), and the budget is consumed anyway through
    // the in-process counter, so a persistently failing inbox cannot
    // re-spend the same veto forever.
    let bus = fresh_bus();

    /// A settle control whose inbox append always fails.
    struct FailingSettleCtl;
    impl SettleCtl for FailingSettleCtl {
        fn admit_synthetic(
            &self,
            _content: String,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            Box::pin(async { Err("inbox append failed: disk full".to_string()) })
        }
        fn veto_count(&self, _plugin: &str) -> u64 {
            0 // nothing ever landed in the stream
        }
    }

    let _keep = bus.registry().register(
        HandlerId {
            plugin: "quality-gate",
            hook: HookId::SettlePre,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:settle", "veto:settle"])
                .unwrap(),
            order: 0,
        },
        Arc::new(move |_payload: &HookPayload| {
            Box::pin(async {
                HookReply::BailDeny {
                    reason: "evidence summary missing".to_string(),
                }
            })
        }),
    );

    let view = SettleView {
        reason: "completed".to_string(),
        turns: 4,
        tool_calls: 7,
        evidence: "none".to_string(),
        vetoes_used: 0,
        vetoes_remaining: 3,
        episodes: 2,
        frame: Some(FrameInfo::new(sid(SESSION_A))),
    };
    let ctl = FailingSettleCtl;

    let verdict = bus.settle(&view, &ctl).await;
    assert!(matches!(
        &verdict,
        kian_plugin::hooks::SettleVerdict::AdmitFailed {
            plugin,
            reason,
            error,
            vetoes_used: 1,
        } if plugin == "quality-gate"
            && reason == "evidence summary missing"
            && error == "inbox append failed: disk full"
    ));

    // The in-process counter counts the failed attempt even though the
    // stream never saw a marker: three failures exhaust the budget and
    // the next deny overrides instead of retrying the append (§4.6 F9
    // step 2 — one veto is one veto, and the bound stays bounded).
    for expected in [2u64, 3u64] {
        let verdict = bus.settle(&view, &ctl).await;
        assert!(matches!(
            verdict,
            kian_plugin::hooks::SettleVerdict::AdmitFailed { vetoes_used, .. } if vetoes_used == expected
        ));
    }
    let verdict = bus.settle(&view, &ctl).await;
    assert!(matches!(
        verdict,
        kian_plugin::hooks::SettleVerdict::Override { vetoes_used: 3, .. }
    ));
}

#[tokio::test]
async fn admit_grid_one_evaluation_n_frames() {
    let bus = fresh_bus();

    // One subscriber, two targets: it is dispatched once per target frame
    // — each frame carrying that target's identity (§4.1, K16) — and one
    // stop anywhere refuses the event for every target.
    let frames_seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let stop_at: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let seen = Arc::clone(&frames_seen);
    let stop_flag = Arc::clone(&stop_at);
    let _keep = bus.registry().register(
        HandlerId {
            plugin: "ingress-gate",
            hook: HookId::AdmitPre,
            grants: kian_plugin::hooks::GrantSet::from_names(&["observe:admit", "veto:admit"])
                .unwrap(),
            order: 0,
        },
        Arc::new(move |payload: &HookPayload| {
            let seen = Arc::clone(&seen);
            let stop_flag = Arc::clone(&stop_flag);
            let payload = payload.clone();
            async move {
                let frame = payload
                    .frame()
                    .expect("the grid dispatches with the target's frame");
                seen.lock().unwrap().push(frame.session.0.to_string());
                if let Some(stopper) = stop_flag.lock().unwrap().as_ref()
                    && *stopper == frame.session.0.to_string()
                {
                    return HookReply::FlowStop {
                        reason: "off-hours".to_string(),
                    };
                }
                HookReply::FlowNext
            }
            .boxed()
        }),
    );

    let draft = |origin: Origin| AdmitDraft {
        origin,
        targets: vec![
            AdmitTarget {
                session: sid(SESSION_A),
                router: false,
            },
            AdmitTarget {
                session: sid(SESSION_B),
                router: true,
            },
        ],
        input: AdmitInput {
            role: "user".to_string(),
            synthetic: false,
            content_preview: "hello".to_string(),
            content_bytes: 5,
            content_sha256: "ab".to_string(),
            content_truncated: false,
            inline_view: false,
        },
        channel: None,
        admission: AdmissionInfo {
            policy: "inline_cap_bytes=8192".to_string(),
            spilled: false,
        },
        frames: vec![
            FrameInfo::new(sid(SESSION_A)),
            FrameInfo::new(sid(SESSION_B)),
        ],
    };

    // No stop: one evaluation, two frames — the complete fan-out set rode
    // the single view, the dispatch was per target.
    let verdict = bus.admit(&draft(Origin::Channel)).await;
    assert_eq!(verdict, kian_plugin::hooks::AdmitVerdict::Next);
    let mut seen = frames_seen.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        vec![SESSION_A.to_string(), SESSION_B.to_string()],
        "N frames for N targets, each with its own identity"
    );

    // A stop at the first-sorted target: the second target's frame is
    // never sent (§4.1 — the event is one decision).
    frames_seen.lock().unwrap().clear();
    *stop_at.lock().unwrap() = Some(SESSION_A.to_string());
    let verdict = bus.admit(&draft(Origin::Channel)).await;
    assert_eq!(
        verdict,
        kian_plugin::hooks::AdmitVerdict::Stop {
            plugin: "ingress-gate".to_string(),
            reason: "off-hours".to_string(),
        },
        "a stop on any frame refuses the event for every target"
    );
    assert_eq!(
        *frames_seen.lock().unwrap(),
        vec![SESSION_A.to_string()],
        "frames not yet sent are not sent"
    );

    // The same stop at an API origin is discarded: the port is
    // observe-only there and never refuses (§3.4.3).
    frames_seen.lock().unwrap().clear();
    let verdict = bus.admit(&draft(Origin::Api)).await;
    assert_eq!(
        verdict,
        kian_plugin::hooks::AdmitVerdict::Next,
        "an API-borne stop is a discarded defection, not a refusal"
    );
    assert_eq!(
        frames_seen.lock().unwrap().len(),
        2,
        "both frames still dispatched"
    );
}
