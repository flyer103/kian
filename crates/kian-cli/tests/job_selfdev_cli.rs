//! E2E tests for the S1 self-dev admission surface (SELF-EVOLUTION-MODEL 4, 10).
//!
//! These drive the REAL binary against a REAL sqlite: a self-dev job run
//! carries a frozen evidence pack summary in its first input row, a pack that
//! does not verify leaves no row behind, and the consecutive-failures breaker
//! refuses before anything is written.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::{force_no_proxy, temp_dir};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::job::{plan, status};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const JOB: &str = "self-dev";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

fn write_config(dir: &Path) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite}"
agent:
  id: selfdev-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: router-mock
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
  router-mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
superagent:
  router:
    session_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    agent: router
    slot: router
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
  jobs:
    jobs:
      - id: self-dev
        limits:
          deadline_ms: 3600000
          max_parallel: 1
          consecutive_failures: 3
        plan:
          - id: patch
            action: agent
            requires: [repo.kian]
            prompt: "emit one unified diff"
            produces: {{name: patch, media: text, max_bytes: 262144}}
            timeout_ms: 900000
            retries: 1
            effect: read
            verify:
              - {{check: artifact_within_cap, name: patch}}
              - {{check: patch_paths_allowed, artifact: patch,
                 allow: ["crates/**"], deny: ["AGENTS.md", "scripts/**"]}}
          - id: deliver
            action: deliver
            depends_on: [patch]
            optional: true
            channel: mock-im
            target: {{handle: owner}}
            content: {{text: "a proposal is ready - NOT MERGED"}}
    triggers: []
    alerts: []
plugins: ["openai-compat", "local-tools", "superagent"]
tools:
  root: "{root}"
  exec_timeout_ms: 5000
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        root = dir.display()
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
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

/// Two rows of one session: enough for a pack anchored at the second.
async fn seed_pack_session(sqlite: &Path) -> (SessionId, u64) {
    let session = SessionId::new();
    let store = EventStore::open(sqlite).await.expect("store");
    let ts0 = now_ms().saturating_sub(60_000);
    append(
        &store,
        session,
        1,
        ts0 + 1,
        EventKind::SessionCreated,
        json!({"agent": "selfdev-agent"}),
    )
    .await;
    append(
        &store,
        session,
        2,
        ts0 + 2,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "the failing turn"}),
    )
    .await;
    store.close().await;
    (session, 2)
}

fn run_self_evidence(
    config: &Path,
    session: SessionId,
    seq: u64,
    out: &Path,
) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .arg("self")
        .arg("evidence")
        .arg("--config")
        .arg(config)
        .arg("--session")
        .arg(session.0.to_string())
        .arg("--seq")
        .arg(seq.to_string())
        .arg("--context")
        .arg("1")
        .arg("--out")
        .arg(out)
        .output()
        .expect("run kian self evidence")
}

fn run_job(config: &Path, args: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kian"));
    command
        .arg("job")
        .arg("run")
        .arg(JOB)
        .arg("--config")
        .arg(config);
    for arg in args {
        command.arg(arg);
    }
    command.output().expect("run kian job run")
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_json(path: &Path) -> Value {
    let bytes = std::fs::read(path).expect("read json");
    serde_json::from_slice(&bytes).expect("parse json")
}

fn stdout_json(out: &std::process::Output) -> Value {
    serde_json::from_slice(&out.stdout).expect("stdout json")
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

async fn rows_of(sqlite: &Path, session: SessionId) -> Vec<InputRow> {
    let store = EventStore::open(sqlite).await.expect("store");
    let rows = store.input_rows(session).await.expect("input rows");
    store.close().await;
    rows
}

/// A pack that verifies, built by the real evidence writer over a real store.
async fn verified_pack(dir: &Path, config: &Path) -> (PathBuf, PathBuf) {
    let sqlite = dir.join("kian.sqlite");
    let (session, anchor) = seed_pack_session(&sqlite).await;
    let pack = dir.join("pack");
    let out = run_self_evidence(config, session, anchor, &pack);
    assert!(
        out.status.success(),
        "self evidence failed: {}",
        stderr_of(&out)
    );
    (sqlite, pack)
}

#[tokio::test]
async fn a_verified_pack_admits_a_row_that_carries_it() {
    force_no_proxy();
    let dir = temp_dir("selfdev-verified");
    let config = write_config(&dir);
    let (sqlite, pack) = verified_pack(&dir, &config).await;

    let out = run_job(&config, &["--pack", pack.to_str().expect("utf8"), "--json"]);
    assert!(out.status.success(), "job run failed: {}", stderr_of(&out));
    let printed = stdout_json(&out);
    assert_eq!(printed["state"], json!("admitted"));
    assert_eq!(printed["job_id"], json!(JOB));
    let run_session =
        kian_cli::commands::parse_session(printed["session"].as_str().expect("session string"))
            .expect("session id");

    let rows = rows_of(&sqlite, run_session).await;
    assert_eq!(rows.len(), 1);
    let marker = &rows[0].payload["job"];
    assert_eq!(marker["job_id"], json!(JOB));
    assert_eq!(marker["trigger"], json!("manual"));
    let selfdev = &marker["selfdev"];
    let manifest = read_json(&pack.join("manifest.json"));
    let dir_text = pack.to_str().expect("utf8");
    assert_eq!(selfdev["pack"]["dir"], json!(dir_text));
    assert_eq!(
        selfdev["pack"]["files_sha256"],
        json!(format!(
            "sha256:{}",
            manifest["files_sha256"].as_str().expect("files digest")
        ))
    );
    let manifest_digest = selfdev["pack"]["manifest_sha256"]
        .as_str()
        .expect("manifest digest");
    assert!(manifest_digest.starts_with("sha256:"), "{manifest_digest}");
    assert_eq!(manifest_digest.len(), "sha256:".len() + 64);
    assert_eq!(selfdev["base_commit"], manifest["repo"]["commit"]);
    assert_eq!(selfdev["kian_version"], json!(env!("CARGO_PKG_VERSION")));
    let manifest_bytes = std::fs::read(pack.join("manifest.json")).expect("manifest bytes");
    assert_eq!(
        manifest_digest,
        format!("sha256:{}", sha256_hex(&manifest_bytes))
    );
}

#[tokio::test]
async fn a_tampered_pack_is_refused_and_writes_no_row() {
    force_no_proxy();
    let dir = temp_dir("selfdev-tampered");
    let config = write_config(&dir);
    let (sqlite, pack) = verified_pack(&dir, &config).await;

    let events = pack.join("events.ndjson");
    let mut bytes = std::fs::read(&events).expect("events slice");
    let at = bytes.iter().position(|byte| *byte == b'{').expect("brace");
    bytes[at] = b'[';
    std::fs::write(&events, &bytes).expect("tamper");

    let out = run_job(
        &config,
        &[
            "--pack",
            pack.to_str().expect("utf8"),
            "--slot",
            "tampered",
            "--json",
        ],
    );
    assert_eq!(out.status.code(), Some(2), "stderr: {}", stderr_of(&out));
    assert!(
        stderr_of(&out).contains("evidence pack does not verify"),
        "stderr: {}",
        stderr_of(&out)
    );
    let run_session = plan::run_session(JOB, "tampered");
    assert!(rows_of(&sqlite, run_session).await.is_empty());
}

#[tokio::test]
async fn a_selfdev_job_without_a_pack_is_refused_before_the_store_exists() {
    let dir = temp_dir("selfdev-no-pack");
    let config = write_config(&dir);
    let sqlite = dir.join("kian.sqlite");

    let out = run_job(&config, &["--json"]);
    assert_eq!(out.status.code(), Some(2), "stderr: {}", stderr_of(&out));
    assert!(
        stderr_of(&out).contains("selfdev job requires an evidence pack"),
        "stderr: {}",
        stderr_of(&out)
    );
    assert!(!sqlite.exists(), "a refusal must not touch the store");
}

/// Three settled failed runs of this job, each a real stream with the
/// frozen marker and a run-level verdict the operator surface reads.
async fn seed_failed_runs(sqlite: &Path, config: &Path, count: usize) {
    let loaded = kian_cli::config::ServeConfig::load(config).expect("config");
    let superagent = loaded.superagent.as_ref().expect("superagent section");
    let resolved = superagent
        .jobs
        .resolve(&superagent.channels, &BTreeSet::new(), chrono::Utc::now())
        .expect("jobs resolve");
    let job = resolved.catalog.job(JOB).expect("declared job");
    let digest = job.plan.digest();
    let store = EventStore::open(sqlite).await.expect("store");
    let ts0 = now_ms().saturating_sub(30_000);
    for index in 0..count {
        let slot = format!("failed-{index}");
        let run_session = plan::run_session(JOB, &slot);
        let marker = status::marker_payload(JOB, &slot, "manual", &job.plan);
        let base = ts0 + (index as u64) * 1_000;
        append(
            &store,
            run_session,
            1,
            base + 1,
            EventKind::SessionCreated,
            json!({"agent": "selfdev-agent"}),
        )
        .await;
        append(
            &store,
            run_session,
            2,
            base + 2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "Run job self-dev.", "job": marker}),
        )
        .await;
        append(
            &store,
            run_session,
            3,
            base + 3,
            EventKind::EvidenceVerdict,
            json!({"gate": "run", "attempt": 0, "passed": false, "checks": [],
                   "job": {"outcome": "failed", "plan_digest": digest}}),
        )
        .await;
    }
    store.close().await;
}

#[tokio::test]
async fn the_breaker_refuses_a_fourth_consecutive_failure_until_forced() {
    force_no_proxy();
    let dir = temp_dir("selfdev-breaker");
    let config = write_config(&dir);
    let (sqlite, pack) = verified_pack(&dir, &config).await;
    seed_failed_runs(&sqlite, &config, 3).await;

    let pack_arg = pack.to_str().expect("utf8");
    let refused = run_job(&config, &["--pack", pack_arg, "--slot", "gated", "--json"]);
    assert_eq!(
        refused.status.code(),
        Some(2),
        "stderr: {}",
        stderr_of(&refused)
    );
    assert!(
        stderr_of(&refused).contains("selfdev_circuit_open"),
        "stderr: {}",
        stderr_of(&refused)
    );
    let run_session = plan::run_session(JOB, "gated");
    assert!(
        rows_of(&sqlite, run_session).await.is_empty(),
        "an open circuit writes nothing"
    );

    let forced = run_job(
        &config,
        &["--pack", pack_arg, "--slot", "gated", "--force", "--json"],
    );
    assert!(
        forced.status.success(),
        "forced run: {}",
        stderr_of(&forced)
    );
    let rows = rows_of(&sqlite, run_session).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].payload["job"]["selfdev"]["circuit_overridden"],
        json!(true)
    );
}
