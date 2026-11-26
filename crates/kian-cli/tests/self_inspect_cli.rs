//! E2E tests for the 'kian self inspect' verb (SPEC 3.1).
//!
//! The verb is a pure projection: a temp sqlite seeded with a few
//! hand-built rows, then the REAL binary is run against it. Nothing
//! here starts a server or a drain — the claim under test is that the
//! report can be produced offline from the durable stream alone.

mod common;

use common::{force_no_proxy, temp_dir, write_config};
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

async fn append(
    store: &EventStore,
    session: SessionId,
    seq: u64,
    ts: u64,
    kind: EventKind,
    bytes: Value,
) {
    store
        .append(
            session,
            Event {
                seq,
                session,
                epoch: Epoch(1),
                ts,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: seq,
            },
        )
        .await
        .expect("append");
}

fn run_inspect(config: &std::path::Path, extra: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .arg("self")
        .arg("inspect")
        .arg("--config")
        .arg(config);
    for arg in extra {
        command.arg(arg);
    }
    command.output().expect("run kian self inspect")
}

/// One window of a seeded stream: every section counts, findings cite
/// the row that evidences them, and no payload text leaks out.
#[tokio::test]
async fn self_inspect_reports_one_window_of_the_stream() {
    force_no_proxy();
    let dir = temp_dir("self-inspect");
    let sqlite = dir.join("self-inspect.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let session = SessionId::new();

    let store = EventStore::open(&sqlite).await.expect("store");
    let ts0 = now_ms().saturating_sub(60_000);
    append(
        &store,
        session,
        1,
        ts0,
        EventKind::SessionCreated,
        json!({"agent": "e2e-agent"}),
    )
    .await;
    append(
        &store,
        session,
        2,
        ts0 + 1,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": {"kian": {"raw": {
            "media": [{"kind": "image", "media_type": "image/jpeg",
                       "artifact": "a1", "size": 10,
                       "transcript": "SECRET TRANSCRIPT TEXT"}]
        }}}}),
    )
    .await;
    append(
        &store,
        session,
        3,
        ts0 + 2,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": {"kian": {"raw": {
            "media": [{"kind": "image", "media_type": "image/heic",
                       "error": "image_ocr failed with HTTP 502"}]
        }}}}),
    )
    .await;
    for seq in 4..=6 {
        append(
            &store,
            session,
            seq,
            ts0 + seq,
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer",
                   "error": {"class": "provider_down", "action": "retry"}}),
        )
        .await;
    }
    for (call_seq, call_id) in [(7u64, "c1"), (9, "c2"), (11, "c3")] {
        append(
            &store,
            session,
            call_seq,
            ts0 + call_seq,
            EventKind::ToolCall,
            json!({"content": {"id": call_id, "name": "shell", "arguments": {}}}),
        )
        .await;
        append(
            &store,
            session,
            call_seq + 1,
            ts0 + call_seq + 1,
            EventKind::ToolResult,
            json!({"content": {"call_id": call_id, "is_error": true, "output": "boom"}}),
        )
        .await;
    }
    append(
        &store,
        session,
        13,
        ts0 + 13,
        EventKind::EvidenceVerdict,
        json!({"gate": "no_half_terminated", "passed": false, "checks": []}),
    )
    .await;
    append(
        &store,
        session,
        14,
        ts0 + 14,
        EventKind::ChannelDelivered,
        json!({"channel": "imessage", "target": "+1555", "policy": "preview",
               "truncated": false}),
    )
    .await;
    append(
        &store,
        session,
        15,
        ts0 + 15,
        EventKind::SessionEnded {
            reason: EndReason::Failed,
        },
        json!({}),
    )
    .await;
    store.close().await;

    let out = run_inspect(&config, &["--json"]);
    assert!(out.status.success(), "self inspect failed: {out:?}");
    let text = String::from_utf8_lossy(&out.stdout);
    let report: Value = serde_json::from_str(&text).expect("report json");

    assert_eq!(
        report["events"]["by_kind"]["input/admitted"],
        json!(2),
        "{report}"
    );
    assert_eq!(
        report["events"]["by_kind"]["turn/end"],
        json!(3),
        "{report}"
    );
    assert_eq!(
        report["sessions"]["ended_by_reason"]["failed"],
        json!(1),
        "{report}"
    );
    assert_eq!(
        report["turns"]["error_classes"]["provider_down"],
        json!(3),
        "{report}"
    );
    assert_eq!(
        report["turns"]["outcomes"]["final_answer"],
        json!(3),
        "{report}"
    );
    assert_eq!(report["tools"]["calls"], json!(3), "{report}");
    assert_eq!(report["tools"]["errors"], json!(3), "{report}");
    assert_eq!(
        report["tools"]["top_failing"][0]["tool"], "shell",
        "{report}"
    );
    assert_eq!(
        report["tools"]["top_failing"][0]["errors"],
        json!(3),
        "{report}"
    );
    assert_eq!(report["media"]["items"], json!(2), "{report}");
    assert_eq!(report["media"]["transcribed"], json!(1), "{report}");
    assert_eq!(report["media"]["failed"], json!(1), "{report}");
    assert_eq!(report["evidence"]["failed"], json!(1), "{report}");
    assert_eq!(report["channels"]["delivered"], json!(1), "{report}");

    let observed = &report["window"]["observed"];
    assert_eq!(observed["first_ts_ms"], json!(ts0));
    assert_eq!(observed["last_ts_ms"], json!(ts0 + 15));
    assert_eq!(
        report["identity"]["config_sha256"].as_str().map(str::len),
        Some(64)
    );
    assert!(report["lifetime"]["usage"].is_object(), "{report}");
    assert!(report["lifetime"]["cache"].is_object(), "{report}");

    let findings = report["findings"].as_array().expect("findings");
    let codes: Vec<&str> = findings.iter().filter_map(|f| f["code"].as_str()).collect();
    for expected in [
        "media_error",
        "session_failed",
        "turn_error",
        "tool_error",
        "evidence_failed",
    ] {
        assert!(codes.contains(&expected), "missing {expected}: {report}");
    }
    let media_error = findings
        .iter()
        .find(|f| f["code"] == "media_error")
        .expect("media finding");
    assert_eq!(media_error["severity"], "error");
    assert_eq!(media_error["session"], session.0.to_string());
    assert_eq!(media_error["seq"], json!(3));

    // The privacy claim: counts, never content.
    assert!(
        !text.contains("SECRET TRANSCRIPT TEXT"),
        "transcript text leaked: {text}"
    );

    // The human form names the same evidence.
    let human = run_inspect(&config, &[]);
    assert!(human.status.success(), "text form failed: {human:?}");
    let human_text = String::from_utf8_lossy(&human.stdout);
    assert!(human_text.contains("media_error"), "{human_text}");
    assert!(human_text.contains(&session.0.to_string()), "{human_text}");
    assert!(!human_text.contains("SECRET TRANSCRIPT TEXT"));

    std::fs::remove_dir_all(&dir).ok();
}

/// The window bounds what counts, and --session narrows to one stream —
/// including an honest refusal for a uuid this store never saw.
#[tokio::test]
async fn self_inspect_narrows_by_window_and_session() {
    force_no_proxy();
    let dir = temp_dir("self-inspect-window");
    let sqlite = dir.join("self-inspect-window.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let store = EventStore::open(&sqlite).await.expect("store");

    let now = now_ms();
    let fresh_ts = now.saturating_sub(5_000);
    let old = now.saturating_sub(3 * 60 * 60 * 1000);
    let stale = SessionId::new();
    let fresh = SessionId::new();
    append(
        &store,
        stale,
        1,
        old,
        EventKind::SessionCreated,
        json!({"agent": "e2e-agent"}),
    )
    .await;
    append(
        &store,
        fresh,
        1,
        fresh_ts,
        EventKind::SessionCreated,
        json!({"agent": "e2e-agent"}),
    )
    .await;
    store.close().await;

    // Default window (1h) excludes the three-hour-old row.
    let out = run_inspect(&config, &["--json"]);
    assert!(out.status.success(), "window run failed: {out:?}");
    let report: Value = serde_json::from_slice(&out.stdout).expect("report json");
    assert_eq!(
        report["events"]["by_kind"]["session/created"],
        json!(1),
        "{report}"
    );
    assert_eq!(report["sessions"]["started"], json!(1), "{report}");
    assert_eq!(
        report["window"]["observed"]["first_ts_ms"],
        json!(fresh_ts),
        "{report}"
    );
    // The lifetime section is deliberately not windowed: both sessions exist,
    // while the window above holds one event.
    assert_eq!(
        report["lifetime"]["usage"]["sessions"],
        json!(2),
        "{report}"
    );

    // An explicit narrower window still succeeds with an honest zero row.
    let out = run_inspect(&config, &["--json", "--window", "1s"]);
    assert!(out.status.success(), "empty window failed: {out:?}");
    let report: Value = serde_json::from_slice(&out.stdout).expect("report json");
    assert_eq!(report["events"]["total"], json!(0), "{report}");
    assert!(report["window"].get("observed").is_none(), "{report}");
    assert_eq!(report["findings"], json!([]));

    // --session narrows to that stream, keeping the window filter.
    let out = run_inspect(&config, &["--json", "--session", &fresh.0.to_string()]);
    assert!(out.status.success(), "session run failed: {out:?}");
    let report: Value = serde_json::from_slice(&out.stdout).expect("report json");
    assert_eq!(report["events"]["total"], json!(1), "{report}");
    assert_eq!(report["sessions"]["started"], json!(1), "{report}");

    // The stale session is outside the window: a legal, empty slice.
    let out = run_inspect(&config, &["--json", "--session", &stale.0.to_string()]);
    assert!(out.status.success(), "stale session run failed: {out:?}");
    let report: Value = serde_json::from_slice(&out.stdout).expect("report json");
    assert_eq!(report["events"]["total"], json!(0), "{report}");
    assert!(report["window"].get("observed").is_none());

    // A uuid this store never saw is refused, not reported as zeros.
    let unknown = SessionId::new();
    let out = run_inspect(&config, &["--session", &unknown.0.to_string()]);
    assert!(
        !out.status.success(),
        "unknown session should refuse: {out:?}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unknown session"), "{stderr}");

    // A bare --window number is refused rather than guessed.
    let out = run_inspect(&config, &["--window", "3600"]);
    assert!(!out.status.success(), "bare window should refuse: {out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unit"), "{stderr}");

    std::fs::remove_dir_all(&dir).ok();
}
