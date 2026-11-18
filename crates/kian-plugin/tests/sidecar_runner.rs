//! Live sidecar-runner tests (PLUGIN-SPEC §6.3–§6.5, §7.3, §7.5,
//! §10.3, §11.1) against the real child fixture
//! (`kian_sidecar_child`, reached through `CARGO_BIN_EXE_…`).
//!
//! These exercise the true process path — hardened spawn with
//! env_clear + env_allow, digest verification, the hello→ready
//! handshake identity check, deadlines via group-kill watchdog,
//! respawn, the failure window, quarantine (one-strike and window) and
//! the §11.1 class defaults through the existing `HookBus`
//! classifier.
//!
//! Env-var roles make the child process-global state, so every test
//! holds `ENV_LOCK` while a sidecar it configured is alive.

use std::sync::{Arc, OnceLock};

use kian_plugin::hooks::bus::HookBus;
use kian_plugin::hooks::registry::Registry;
use kian_plugin::hooks::sidecar::manifest::{Manifest, ValidationCtx, parse_manifest};
use kian_plugin::hooks::sidecar::runner::{QuarantineReason, ReplyOutcome, SidecarRunner};
use kian_plugin::hooks::views::{FrameInfo, PermissionView, ToolCallView, ToolVerdict};
use kian_plugin::hooks::wire::sha256_hex;

/// Env-var roles are child-process-global, and the tests hold this
/// lock for a sidecar's whole lifetime — across awaits. An async
/// mutex is the honest shape for that (and keeps clippy's
/// await-holding-lock check satisfied for the right reason).
async fn env_lock() -> tokio::sync::OwnedMutexGuard<()> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
        .lock_owned()
        .await
}

fn child_bin() -> String {
    env!("CARGO_BIN_EXE_kian_sidecar_child").to_string()
}

/// Writes a manifest with the child fixture as argv[0] and the given
/// limits, returning the parsed manifest plus its on-disk path (the
/// handshake compares the canonical path, so it must be a real file).
fn fixture_manifest(limits: &str) -> (Manifest, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "kian-sidecar-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let toml = format!(
        concat!(
            "[plugin]\n",
            "name = \"approval-gate\"\n",
            "version = \"0.1.0\"\n",
            "abi = 1\n",
            "\n",
            "[process]\n",
            "argv = [\"{bin}\"]\n",
            "sha256 = \"{digest}\"\n",
            "env_allow = [\"KIAN_SIDECAR_ROLE\", \"KIAN_SIDECAR_REPLY\"]\n",
            "startup_timeout_ms = {startup}\n",
            "\n",
            "[grants]\n",
            "hooks = [\"tool/pre\", \"tool/post\"]\n",
            "capabilities = [\"observe:tool\", \"veto:tool\"]\n",
            "\n",
            "[limits]\n",
            "deadline_ms = 50\n",
            "restarts_per_window = 2\n",
            "restart_window_ms = 60000\n",
            "{limits}\n"
        ),
        bin = child_bin(),
        digest = sha256_hex(&std::fs::read(child_bin()).unwrap()),
        startup = if cfg!(debug_assertions) { 2000 } else { 5000 },
        limits = limits,
    );
    let path = dir.join("approval-gate.toml");
    std::fs::write(&path, &toml).unwrap();
    let manifest = parse_manifest(
        &toml,
        &ValidationCtx {
            instance_key: "approval-gate",
            reserved_names: &[],
            secret_env: &[],
        },
    )
    .expect("fixture manifest validates");
    (manifest, path)
}

/// Sets the child role env vars (under the env lock).
fn set_role(role: &str, reply: Option<&str>) {
    // Env mutation is process-global: guarded by ENV_LOCK, held by the
    // caller for the sidecar's lifetime.
    // SAFETY: serialized by ENV_LOCK; no other thread reads these names.
    unsafe {
        std::env::set_var("KIAN_SIDECAR_ROLE", role);
        if let Some(reply) = reply {
            std::env::set_var("KIAN_SIDECAR_REPLY", reply);
        } else {
            std::env::remove_var("KIAN_SIDECAR_REPLY");
        }
    }
}

fn sid() -> kian_core::model::SessionId {
    serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap()
}

fn tool_pre_view() -> ToolCallView {
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
        frame: Some(FrameInfo {
            session: sid(),
            worker: "node-zulu".to_string(),
            seq: 41,
            epoch: Some(7),
        }),
    }
}

/// A bus with the runner's handlers registered (the P3b assembly's
/// job in production; here, the direct composition). The disposers
/// stay alive for the bus's life — dropping one unregisters (§5.3).
fn bus_with(runner: &Arc<SidecarRunner>) -> (HookBus, Vec<kian_plugin::ctx::Disposer>) {
    let bus = HookBus::with_registry(Arc::new(Registry::new()));
    let mut disposers = Vec::new();
    for (handler, f) in runner.hook_fns() {
        disposers.push(bus.registry().register(handler, f));
    }
    (bus, disposers)
}

const DENY_REPLY: &str = r#"{"id":"PLACEHOLDER","bail":{"decision":"deny","reason":"blocked"}}"#;

#[tokio::test]
async fn handshake_and_valid_deny_through_the_bus() {
    let _env = env_lock().await;
    set_role("ok", Some(DENY_REPLY));
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);

    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert_eq!(
        verdict,
        ToolVerdict::Deny {
            plugin: "approval-gate".to_string(),
            reason: "blocked".to_string()
        },
        "a valid in-grant deny is a verdict (§3.3)"
    );
    runner.stop();
}

#[tokio::test]
async fn rule_13_never_ready_is_spawn_failed() {
    let _env = env_lock().await;
    set_role("never-ready", None);
    let (mut manifest, path) = fixture_manifest("");
    manifest.process.startup_timeout_ms = 100;
    let refusal = SidecarRunner::start(manifest, &path).expect_err("no ready");
    assert_eq!(
        refusal.code,
        kian_plugin::hooks::sidecar::manifest::RefusalCode::SpawnFailed
    );
    assert!(refusal.message.contains("startup_timeout_ms"));
}

#[tokio::test]
async fn rule_12_ready_mismatch_and_abi_mismatch_are_typed_refusals() {
    let _env = env_lock().await;
    set_role("bad-ready", None);
    let (manifest, path) = fixture_manifest("");
    let refusal = SidecarRunner::start(manifest, &path).expect_err("wrong version");
    assert_eq!(
        refusal.code,
        kian_plugin::hooks::sidecar::manifest::RefusalCode::ReadyMismatch
    );
    assert!(refusal.message.contains("9.9.9"), "{}", refusal.message);

    set_role("abi-ready", None);
    let (manifest, path) = fixture_manifest("");
    let refusal = SidecarRunner::start(manifest, &path).expect_err("wrong abi");
    assert_eq!(
        refusal.code,
        kian_plugin::hooks::sidecar::manifest::RefusalCode::AbiMismatch
    );
}

#[tokio::test]
async fn rule_6_digest_mismatch_refuses_at_spawn() {
    let _env = env_lock().await;
    set_role("ok", Some(DENY_REPLY));
    let (mut manifest, path) = fixture_manifest("");
    // A syntactically valid digest that is not the binary's.
    manifest.process.sha256 = "ff".repeat(32);
    let refusal = SidecarRunner::start(manifest, &path).expect_err("digest");
    assert_eq!(
        refusal.code,
        kian_plugin::hooks::sidecar::manifest::RefusalCode::DigestMismatch
    );
}

#[tokio::test]
async fn deadline_takes_the_class_default_per_11_1() {
    // The acceptance path: a veto:tool holder that cannot answer fails
    // CLOSED — deny with the honest-error reason (§11.1 row 1).
    let _env = env_lock().await;
    set_role("slow", None);
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);

    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert_eq!(
        verdict,
        ToolVerdict::Deny {
            plugin: "approval-gate".to_string(),
            reason: "hook unavailable (deadline)".to_string()
        },
        "deadline → class default: fail-closed for a veto:tool holder"
    );
    runner.stop();
}

#[tokio::test]
async fn crash_window_trips_restarts_exhausted_quarantine() {
    let _env = env_lock().await;
    set_role("crash", None);
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);

    // restarts_per_window = 2: round 1 crashes and counts; round 2
    // crashes, trips the window and quarantines; round 3 is the
    // quarantined instance's class default.
    for round in 1..=3 {
        let verdict = bus.tool_pre(&tool_pre_view()).await;
        assert!(
            matches!(verdict, ToolVerdict::Deny { .. }),
            "every round fails closed (round {round}): {verdict:?}"
        );
        match round {
            1 => assert!(
                runner.quarantined().is_none(),
                "one failure does not trip a 2-window"
            ),
            2 => assert!(matches!(
                runner.quarantined(),
                Some(QuarantineReason::RestartsExhausted { failures: 2, .. })
            )),
            _ => {}
        }
    }
    assert_eq!(runner.counters().restarts(), 1, "one actual respawn");
    // Quarantine is one-way: a later dispatch is still the default.
    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert!(matches!(verdict, ToolVerdict::Deny { .. }));
}

#[tokio::test]
async fn frame_overflow_is_one_strike_not_window_counted() {
    let _env = env_lock().await;
    set_role("oversize", None);
    let (mut manifest, path) = fixture_manifest("max_payload_kib = 16");
    manifest.limits.max_payload_kib = 16;
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));

    let frame = kian_plugin::hooks::sidecar::frame::HookFrame::new(
        kian_plugin::hooks::ids::HookId::ToolPre,
        &FrameInfo {
            session: sid(),
            worker: String::new(),
            seq: 0,
            epoch: Some(1),
        },
        "sc-t".to_string(),
        50,
        serde_json::json!({}),
    )
    .unwrap();
    let outcome = runner.dispatch(frame).await;
    assert!(matches!(
        outcome,
        ReplyOutcome::Unreachable { ref detail, .. } if detail.contains("frame_overflow")
    ));
    // One strike: quarantined immediately, the window untouched.
    assert!(matches!(
        runner.quarantined(),
        Some(QuarantineReason::FrameOverflow {
            direction: "inbound"
        })
    ));
    assert_eq!(runner.counters().window_len(), 0);
    runner.stop();
}

#[tokio::test]
async fn wrong_frame_id_is_protocol_violation_and_as_if_absent() {
    let _env = env_lock().await;
    set_role("wrong-id", Some(DENY_REPLY));
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);

    // §7.4: the stray-id deny is discarded, the dispatch is
    // as-if-absent — the declarative gate's allow stands even though
    // the plugin holds veto:tool.
    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert_eq!(verdict, ToolVerdict::Allow);
    assert!(matches!(
        runner.quarantined(),
        Some(QuarantineReason::ProtocolViolation { .. })
    ));
}

#[tokio::test]
async fn garbage_reply_is_discarded_and_window_counted() {
    let _env = env_lock().await;
    set_role("garbage", None);
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);

    // An unparseable reply is a speech defect: discarded, as-if-absent.
    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert_eq!(verdict, ToolVerdict::Allow);
    // …and window-counted as err:malformed-class (§6.4): one entry, no
    // quarantine yet (restarts_per_window = 2).
    assert_eq!(runner.counters().window_len(), 1);
    assert!(runner.quarantined().is_none());
    runner.stop();
}

#[tokio::test]
async fn busy_fails_fast_and_never_queues() {
    let _env = env_lock().await;
    set_role("slow", None);
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));

    let frame = || {
        kian_plugin::hooks::sidecar::frame::HookFrame::new(
            kian_plugin::hooks::ids::HookId::ToolPre,
            &FrameInfo {
                session: sid(),
                worker: String::new(),
                seq: 0,
                epoch: Some(1),
            },
            "sc-busy".to_string(),
            50,
            serde_json::json!({}),
        )
        .unwrap()
    };
    let first = runner.dispatch(frame());
    let second = runner.dispatch(frame());
    let (first_out, second_out) = futures::future::join(first, second).await;
    // One resolves to the deadline default, the other fails fast busy.
    let outcomes = vec![first_out, second_out];
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, ReplyOutcome::Unreachable { .. })),
        "the in-flight dispatch resolves as unreachability: {outcomes:?}"
    );
    assert!(
        outcomes.iter().any(|o| matches!(o, ReplyOutcome::Busy)),
        "the second dispatch never queues: {outcomes:?}"
    );
    runner.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stderr_only_window_quarantines_as_stderr_overflow() {
    let _env = env_lock().await;
    set_role("stderr-spam", None);
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));

    // restarts_per_window = 2, one burst per second: the flood trips
    // the window within a few seconds.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while runner.quarantined().is_none() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        matches!(
            runner.quarantined(),
            Some(QuarantineReason::StderrRate { failures: 2, .. })
        ),
        "actual: {:?} (window {})",
        runner.quarantined(),
        runner.counters().window_len()
    );
    // The quarantined instance is skipped: unreachability at dispatch.
    let frame = kian_plugin::hooks::sidecar::frame::HookFrame::new(
        kian_plugin::hooks::ids::HookId::ToolPre,
        &FrameInfo {
            session: sid(),
            worker: String::new(),
            seq: 0,
            epoch: Some(1),
        },
        "sc-stderr".to_string(),
        50,
        serde_json::json!({}),
    )
    .unwrap();
    assert!(matches!(
        runner.dispatch(frame).await,
        ReplyOutcome::Unreachable { .. }
    ));
    runner.stop();
}

#[tokio::test]
async fn env_clear_leaks_nothing_the_manifest_does_not_allow() {
    // §10.3 step 2: env_clear first, then exactly env_allow — a
    // process-wide canary the manifest does not name must not reach
    // the child.
    let _env = env_lock().await;
    set_role("env-audit", None);
    // SAFETY: serialized by ENV_LOCK.
    unsafe {
        std::env::set_var("KIAN_ENV_CANARY", "present");
    }
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);
    let verdict = bus.tool_pre(&tool_pre_view()).await;
    // SAFETY: serialized by ENV_LOCK.
    unsafe {
        std::env::remove_var("KIAN_ENV_CANARY");
    }
    assert_eq!(
        verdict,
        ToolVerdict::Allow,
        "bail:null — the canary did not leak: {verdict:?}"
    );
    runner.stop();
}

#[tokio::test]
async fn graceful_stop_bye_half_close_bounded() {
    let _env = env_lock().await;
    set_role("ok", Some(r#"{"id":"PLACEHOLDER","ack":true}"#));
    let (manifest, path) = fixture_manifest("");
    let runner = Arc::new(SidecarRunner::start(manifest, &path).expect("handshake ok"));
    let (bus, _disposers) = bus_with(&runner);
    let verdict = bus.tool_pre(&tool_pre_view()).await;
    assert_eq!(verdict, ToolVerdict::Allow, "ack is as-if-absent");

    // bye + clean child exit: bounded, no quarantine, no failures.
    let started = std::time::Instant::now();
    runner.stop();
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(runner.quarantined().is_none());
    assert_eq!(runner.counters().window_len(), 0);
}
