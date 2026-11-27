//! E2E tests for the 'kian self evidence' verb (SPEC 3.1).
//!
//! The verb turns one anchor row of a seeded sqlite into a handoff
//! package, run through the REAL binary. Nothing here starts a server:
//! the claim under test is that a reviewer can be handed a complete,
//! self-verifying directory built offline from the durable stream, and
//! that a refusal leaves nothing behind.

mod common;

use common::{force_no_proxy, temp_dir, write_config};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
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

fn run_self(config: &std::path::Path, sub: &str, args: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kian"));
    command.arg("self").arg(sub).arg("--config").arg(config);
    for arg in args {
        command.arg(arg);
    }
    command.output().expect("run kian self")
}

/// Six rows, one payload carrying text that must never reach stdout.
async fn seed(sqlite: &std::path::Path) -> (SessionId, u64) {
    let session = SessionId::new();
    let store = EventStore::open(sqlite).await.expect("store");
    let ts0 = now_ms().saturating_sub(60_000);
    append(
        &store,
        session,
        1,
        ts0 + 1,
        EventKind::SessionCreated,
        json!({"agent": "e2e-agent"}),
    )
    .await;
    append(
        &store,
        session,
        2,
        ts0 + 2,
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
        ts0 + 3,
        EventKind::TurnStart,
        json!({"iteration": 0}),
    )
    .await;
    append(
        &store,
        session,
        4,
        ts0 + 4,
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "continue",
               "error": {"class": "provider_down", "action": "retry"}}),
    )
    .await;
    append(
        &store,
        session,
        5,
        ts0 + 5,
        EventKind::ToolCall,
        json!({"content": {"id": "c1", "name": "shell", "arguments": {}}}),
    )
    .await;
    append(
        &store,
        session,
        6,
        ts0 + 6,
        EventKind::ToolResult,
        json!({"content": {"call_id": "c1", "is_error": true, "output": "boom"}}),
    )
    .await;
    store.close().await;
    (session, ts0)
}

fn read_json(path: &std::path::Path) -> Value {
    let raw = std::fs::read_to_string(path).expect("read json");
    serde_json::from_str(&raw).expect("parse json")
}

/// One anchor row becomes a five-file package: the slice is exact, the
/// payload travels only inside the package, and the manifest's digest is
/// reproducible.
#[tokio::test]
async fn self_evidence_writes_a_verifiable_pack() {
    force_no_proxy();
    let dir = temp_dir("self-evidence");
    let sqlite = dir.join("self-evidence.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let (session, ts0) = seed(&sqlite).await;
    let session_arg = session.0.to_string();
    let out = dir.join("pack");

    let run = run_self(
        &config,
        "evidence",
        &[
            "--session",
            &session_arg,
            "--seq",
            "3",
            "--context",
            "2",
            "--out",
            out.to_str().expect("utf8"),
            "--json",
        ],
    );
    assert!(run.status.success(), "self evidence failed: {run:?}");
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        !stdout.contains("SECRET TRANSCRIPT TEXT"),
        "payload text reached stdout: {stdout}"
    );
    let manifest: Value = serde_json::from_str(&stdout).expect("manifest json");
    assert_eq!(manifest["kind"], "kian-evidence", "{manifest}");
    assert_eq!(manifest["anchor"]["seq"], json!(3), "{manifest}");
    assert_eq!(manifest["anchor"]["kind"], "turn/start", "{manifest}");
    assert_eq!(manifest["context"], json!(2), "{manifest}");
    assert_eq!(manifest["range"]["first_seq"], json!(1), "{manifest}");
    assert_eq!(manifest["range"]["last_seq"], json!(5), "{manifest}");
    assert_eq!(manifest["range"]["event_count"], json!(5), "{manifest}");
    assert_eq!(manifest["session_event_count"], json!(6), "{manifest}");
    assert_eq!(
        manifest["files_sha256"].as_str().map(str::len),
        Some(64),
        "{manifest}"
    );
    let files = manifest["files"].as_array().expect("files").clone();
    let names: Vec<&str> = files.iter().filter_map(|f| f["name"].as_str()).collect();
    assert_eq!(
        names,
        vec!["events.ndjson", "session.json", "repo.json", "inspect.json"],
        "{manifest}"
    );
    for name in &names {
        assert!(out.join(name).is_file(), "{name} missing");
    }
    assert_eq!(read_json(&out.join("manifest.json")), manifest);

    let events = std::fs::read_to_string(out.join("events.ndjson")).expect("events");
    let lines: Vec<&str> = events.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 5, "{events}");
    let first: Value = serde_json::from_str(lines[0]).expect("line json");
    assert_eq!(first["seq"], json!(1), "{events}");
    assert!(events.contains("SECRET TRANSCRIPT TEXT"), "{events}");

    let session_doc = read_json(&out.join("session.json"));
    assert_eq!(session_doc["session"], json!(session_arg));
    assert_eq!(session_doc["event_count"], json!(6), "{session_doc}");
    assert_eq!(session_doc["first_seq"], json!(1), "{session_doc}");
    assert_eq!(session_doc["last_seq"], json!(6), "{session_doc}");
    assert_eq!(session_doc["end_reason"], Value::Null, "{session_doc}");

    let repo = read_json(&out.join("repo.json"));
    assert!(repo["root"].is_string(), "{repo}");
    assert_eq!(repo["commit"].as_str().map(str::len), Some(40), "{repo}");
    assert!(repo["error"].is_null(), "{repo}");
    let crates = repo["crates"].as_array().expect("crates");
    assert!(crates.iter().any(|c| c["name"] == "kian-cli"), "{repo}");

    let inspect = read_json(&out.join("inspect.json"));
    assert_eq!(
        inspect["window"]["observed"]["first_ts_ms"],
        json!(ts0 + 1),
        "{inspect}"
    );
    assert_eq!(
        inspect["window"]["observed"]["last_ts_ms"],
        json!(ts0 + 6),
        "{inspect}"
    );

    let whole = run_self(&config, "inspect", &["--session", &session_arg, "--json"]);
    assert!(whole.status.success(), "self inspect failed: {whole:?}");
    let whole: Value =
        serde_json::from_str(&String::from_utf8_lossy(&whole.stdout)).expect("inspect json");
    assert_eq!(
        inspect["events"]["by_kind"], whole["events"]["by_kind"],
        "the pack projection must match the aggregate surface"
    );

    let again = dir.join("pack-again");
    let rerun = run_self(
        &config,
        "evidence",
        &[
            "--session",
            &session_arg,
            "--seq",
            "3",
            "--context",
            "2",
            "--out",
            again.to_str().expect("utf8"),
        ],
    );
    assert!(rerun.status.success(), "second run failed: {rerun:?}");
    let second = read_json(&again.join("manifest.json"));
    assert_eq!(
        second["files_sha256"], manifest["files_sha256"],
        "the same stream must produce the same digest"
    );
    assert!(
        String::from_utf8_lossy(&rerun.stdout).starts_with("package "),
        "the summary is a report, not JSON: {:?}",
        String::from_utf8_lossy(&rerun.stdout)
    );
}

/// A seq the stream does not carry is refused, and no directory appears.
#[tokio::test]
async fn self_evidence_refuses_an_unknown_seq() {
    force_no_proxy();
    let dir = temp_dir("self-evidence-missing");
    let sqlite = dir.join("missing.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let (session, _) = seed(&sqlite).await;
    let out = dir.join("pack");
    let run = run_self(
        &config,
        "evidence",
        &[
            "--session",
            &session.0.to_string(),
            "--seq",
            "99",
            "--out",
            out.to_str().expect("utf8"),
        ],
    );
    assert!(!run.status.success(), "an unknown seq must fail: {run:?}");
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("no event at seq 99"),
        "unexpected stderr: {stderr}"
    );
    assert!(!out.exists(), "a refusal must leave no package behind");
}

/// An occupied --out is someone else's data: refused untouched.
#[tokio::test]
async fn self_evidence_refuses_an_occupied_out() {
    force_no_proxy();
    let dir = temp_dir("self-evidence-occupied");
    let sqlite = dir.join("occupied.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let (session, _) = seed(&sqlite).await;
    let out = dir.join("pack");
    std::fs::create_dir_all(&out).expect("mkdir");
    std::fs::write(out.join("keep.txt"), b"mine").expect("write");
    let run = run_self(
        &config,
        "evidence",
        &[
            "--session",
            &session.0.to_string(),
            "--seq",
            "3",
            "--out",
            out.to_str().expect("utf8"),
        ],
    );
    assert!(!run.status.success(), "an occupied out must fail: {run:?}");
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("non-empty directory"),
        "{:?}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(out.join("keep.txt")).expect("untouched"),
        "mine"
    );
    assert!(!out.join("manifest.json").exists());
}

/// The slice bound is a refusal, not a silent truncation.
#[tokio::test]
async fn self_evidence_refuses_a_context_beyond_the_bound() {
    force_no_proxy();
    let dir = temp_dir("self-evidence-context");
    let sqlite = dir.join("context.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1", "127.0.0.1:0");
    let (session, _) = seed(&sqlite).await;
    let out = dir.join("pack");
    let run = run_self(
        &config,
        "evidence",
        &[
            "--session",
            &session.0.to_string(),
            "--seq",
            "3",
            "--context",
            "201",
            "--out",
            out.to_str().expect("utf8"),
        ],
    );
    assert!(
        !run.status.success(),
        "an oversized context must fail: {run:?}"
    );
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("wider than the 200-row bound"),
        "{:?}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(!out.exists());
}
