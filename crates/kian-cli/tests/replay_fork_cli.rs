//! The scripted fork replay against a real binary and a real sqlite store:
//! kian session replay <uuid> --provider fake --script <file>.
//!
//! Phase 1 seeds a hand-written episode (the recorded fixture), runs the
//! replay, and leaves the fork on disk; phase 2 copies the fork's own events
//! into a fresh store and replays *that* stream — the second verdict must be
//! reproduced, because both sides are engine-written.

mod common;

use std::path::Path;

use common::{force_no_proxy, temp_dir, write_config};
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};

fn run_replay(config: &Path, session: SessionId, extra: &[&str]) -> std::process::Output {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_kian"));
    cmd.arg("session")
        .arg("replay")
        .arg(session.0.to_string())
        .arg("--config")
        .arg(config);
    cmd.args(extra);
    cmd.output().expect("run kian session replay")
}

async fn append(
    sqlite: &Path,
    session: SessionId,
    seq: u64,
    ts: u64,
    kind: EventKind,
    bytes: Value,
) {
    let store = EventStore::open(sqlite).await.expect("open store");
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
        .expect("append event");
    store.close().await;
}

/// A plausible recorded episode: created, claimed, one admitted input, one
/// answered turn, ended.
async fn seed_episode(sqlite: &Path) -> SessionId {
    let session = SessionId::new();
    let ts = 1_700_000_000_000;
    let events = vec![
        (
            1,
            EventKind::SessionCreated,
            json!({"agent": {"id": "e2e-agent"}}),
        ),
        (
            2,
            EventKind::DrainClaimed,
            json!({"holder": "e2e", "epoch": 1}),
        ),
        (
            3,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "hi"}),
        ),
        (4, EventKind::TurnStart, json!({"iteration": 1})),
        (
            5,
            EventKind::StepRequest,
            json!({
                "slot": "main",
                "provider": "mock",
                "model": "mock-model",
                "iteration": 1,
                "body": {
                    "model": "mock-model",
                    "messages": [{"role": "user", "content": "hi"}],
                    "tools": [{
                        "type": "function",
                        "function": {"name": "fs.read", "description": "read", "parameters": {"type": "object"}},
                    }],
                },
                "body_version": 1,
                "dialect": "openai",
                "body_sha256": "00",
                "estimate": {"tokens": 3},
            }),
        ),
        (
            6,
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": "hello"}),
        ),
        (
            7,
            EventKind::TurnEnd,
            json!({"iteration": 1, "outcome": "final_answer"}),
        ),
        (
            8,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            },
            json!({}),
        ),
    ];
    for (seq, kind, bytes) in events {
        append(sqlite, session, seq, ts + seq, kind, bytes).await;
    }
    session
}

fn write_script(dir: &Path, lines: &str) -> std::path::PathBuf {
    let path = dir.join("script.ndjson");
    std::fs::write(&path, lines).expect("write script");
    path
}

fn report_of(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("report json")
}

#[tokio::test]
async fn a_scripted_replay_reproduces_an_engine_written_episode() {
    force_no_proxy();
    let dir = temp_dir("replay-fork");
    let sqlite = dir.join("kian.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let session = seed_episode(&sqlite).await;
    let script = write_script(&dir, "{\"text\":\"hello\"}\n");
    let script_arg = script.to_str().unwrap();

    // Phase 1: the recorded fixture is hand-written, so the verdict is a
    // divergence — but the replay itself must settle through the real engine.
    let first = run_replay(
        &config,
        session,
        &[
            "--provider",
            "fake",
            "--script",
            script_arg,
            "--keep-fork",
            "--json",
        ],
    );
    let first_report = report_of(&first);
    assert_eq!(first_report["provider"], "fake");
    assert_eq!(first_report["script"]["turns"], 1);
    assert_eq!(first_report["anchor"]["seq"], 2);
    assert_eq!(first_report["anchor"]["kind"], "drain/claimed");
    assert_eq!(first_report["anchor"]["prefix_events"], 1);
    assert_eq!(first_report["anchor"]["prefix_state"], "idle");
    assert_eq!(first_report["inputs"][0]["seq"], 3);
    assert_eq!(
        first_report["inputs"][0]["keys"],
        json!(["content", "role"])
    );
    assert_eq!(first_report["replay"]["settled"], true);
    assert_eq!(first_report["replay"]["end_reason"], "completed");
    assert!(first_report["replay"]["events"].as_u64().unwrap() >= 4);
    assert_eq!(
        first_report["comparison"]["ignored_kinds"][0],
        "drain/claimed"
    );
    assert_eq!(first_report["comparison"]["ignored"], 1);
    assert_eq!(first_report["fork"]["kept"], true);
    let fork = first_report["fork"]["path"].as_str().unwrap().to_string();
    assert!(
        Path::new(&fork).join("kian.db").exists(),
        "the kept fork holds its store"
    );

    // The kept fork's stream is engine-written truth: replay that stream.
    let fork_store = EventStore::open_read_only(Path::new(&fork).join("kian.db"))
        .await
        .expect("open the fork store");
    let replayed = fork_store
        .rebuild(session)
        .await
        .expect("read the fork stream");
    fork_store.close().await;
    assert!(replayed.len() >= 5, "fork stream: {replayed:?}");
    assert!(matches!(
        replayed.last().map(|e| &e.kind),
        Some(EventKind::SessionEnded { .. })
    ));

    let sqlite_b = dir.join("phase2.db");
    let config_b = write_config(&dir, &sqlite_b, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let store_b = EventStore::open(&sqlite_b)
        .await
        .expect("open phase2 store");
    let jsonl = kian_cli::archive::export_bytes(&replayed).expect("serialize the fork stream");
    kian_cli::commands::import_jsonl(&store_b, &jsonl)
        .await
        .expect("import the fork stream");
    store_b.close().await;

    let second = run_replay(
        &config_b,
        session,
        &["--provider", "fake", "--script", script_arg, "--json"],
    );
    let second_report = report_of(&second);
    assert_eq!(
        second_report["comparison"]["verdict"],
        "reproduced",
        "second replay: {}",
        serde_json::to_string_pretty(&second_report).unwrap()
    );
    assert_eq!(second_report["fork"]["kept"], false);
    assert!(
        !Path::new(second_report["fork"]["path"].as_str().unwrap()).exists(),
        "a fork is removed unless --keep-fork"
    );
    assert_eq!(second_report["comparison"]["divergence"], Value::Null);

    // The human-readable form names the verdict on its own line.
    let third = run_replay(
        &config_b,
        session,
        &["--provider", "fake", "--script", script_arg],
    );
    let text = String::from_utf8_lossy(&third.stdout);
    assert!(
        third.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&third.stderr)
    );
    assert!(text.contains("replay verdict       REPRODUCED"), "{text}");
    assert!(text.contains("compare"), "{text}");

    std::fs::remove_dir_all(&fork).ok();
}

#[tokio::test]
async fn a_missing_anchor_is_refused() {
    force_no_proxy();
    let dir = temp_dir("replay-fork-noclaim");
    let sqlite = dir.join("kian.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let session = SessionId::new();
    append(
        &sqlite,
        session,
        1,
        1_700_000_000_001,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    append(
        &sqlite,
        session,
        2,
        1_700_000_000_002,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "hi"}),
    )
    .await;
    let script = write_script(&dir, "{\"text\":\"hello\"}\n");
    let out = run_replay(
        &config,
        session,
        &["--provider", "fake", "--script", script.to_str().unwrap()],
    );
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("no drain episode to replay"), "{stderr}");
    assert!(out.stdout.is_empty());
}

#[tokio::test]
async fn a_non_boundary_from_seq_is_refused_with_the_nearest_boundary() {
    force_no_proxy();
    let dir = temp_dir("replay-fork-anchor");
    let sqlite = dir.join("kian.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let session = seed_episode(&sqlite).await;
    let script = write_script(&dir, "{\"text\":\"hello\"}\n");
    let out = run_replay(
        &config,
        session,
        &[
            "--provider",
            "fake",
            "--script",
            script.to_str().unwrap(),
            "--from-seq",
            "3",
        ],
    );
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not an episode boundary"), "{stderr}");
    assert!(
        stderr.contains("nearest at or before it is seq 2"),
        "{stderr}"
    );
}

#[tokio::test]
async fn the_flag_combinations_are_checked_before_any_work() {
    force_no_proxy();
    let dir = temp_dir("replay-fork-flags");
    let sqlite = dir.join("kian.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let session = seed_episode(&sqlite).await;
    let script = write_script(&dir, "{\"text\":\"hello\"}\n");
    let script_arg = script.to_str().unwrap();

    let unknown = run_replay(&config, session, &["--provider", "live"]);
    assert_eq!(unknown.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("is not supported"),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );

    let no_script = run_replay(&config, session, &["--provider", "fake"]);
    assert_eq!(no_script.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&no_script.stderr).contains("requires --script"),
        "{}",
        String::from_utf8_lossy(&no_script.stderr)
    );

    let with_url = run_replay(
        &config,
        session,
        &[
            "--provider",
            "fake",
            "--script",
            script_arg,
            "--url",
            "http://127.0.0.1:1",
        ],
    );
    assert_eq!(with_url.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&with_url.stderr).contains("cannot be combined"),
        "{}",
        String::from_utf8_lossy(&with_url.stderr)
    );

    let scripted_only = run_replay(&config, session, &["--script", script_arg]);
    assert_eq!(scripted_only.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&scripted_only.stderr).contains("belong to the scripted replay"),
        "{}",
        String::from_utf8_lossy(&scripted_only.stderr)
    );

    let bad_seq = run_replay(
        &config,
        session,
        &[
            "--provider",
            "fake",
            "--script",
            script_arg,
            "--from-seq",
            "x",
        ],
    );
    assert_eq!(bad_seq.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&bad_seq.stderr).contains("--from-seq requires an event seq"),
        "{}",
        String::from_utf8_lossy(&bad_seq.stderr)
    );
}

#[tokio::test]
async fn a_provider_error_turn_settles_as_failed() {
    force_no_proxy();
    let dir = temp_dir("replay-fork-error");
    let sqlite = dir.join("kian.db");
    let config = write_config(&dir, &sqlite, "http://127.0.0.1:1/v1", "127.0.0.1:0");
    let session = seed_episode(&sqlite).await;
    let script = write_script(&dir, "{\"error\":\"provider_down\"}\n");
    let out = run_replay(
        &config,
        session,
        &[
            "--provider",
            "fake",
            "--script",
            script.to_str().unwrap(),
            "--json",
        ],
    );
    let report = report_of(&out);
    assert_eq!(report["replay"]["settled"], true);
    assert_eq!(report["replay"]["end_reason"], "failed");
}
