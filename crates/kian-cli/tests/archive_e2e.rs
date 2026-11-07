//! E2E acceptance for export-only retention (READ-MODEL §7/§11, P2b):
//! the archive → verify → delete → inspect flow, export byte parity
//! pre/post archive (the audit-invariance golden), corrupt-package
//! refusals on verify AND import, the crash windows (package written
//! without a commit leaves the online rows intact; a failed verify
//! never deletes), terminal-only eligibility, idempotent retries, and
//! import + replay of a package into a fresh store.

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::state::project;
use kian_store::{AppendGuard, EventStore};
use serde_json::json;

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "kian-arch-e2e-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ))
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kian-arch-e2e-{tag}-{}", uuid::Uuid::new_v4()))
}

fn cleanup_db(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// A valid config with the retention section appended (`extra` lands
/// after the base yaml). No server is ever contacted — the provider
/// points at a dead localhost port.
fn write_config(
    dir: &std::path::Path,
    sqlite: &std::path::Path,
    extra: &str,
) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{}"
agent:
  id: arch-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
providers:
  mock:
    base_url: "http://127.0.0.1:9"
    api_key_env: KIAN_E2E_UNUSED
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
{extra}
"#,
        sqlite.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

async fn push(store: &EventStore, session: SessionId, seq: u64, epoch: Epoch, kind: EventKind) {
    store
        .append(
            session,
            Event {
                seq,
                session,
                epoch,
                ts: 1_762_000_000_000 + seq,
                kind,
                bytes: json!({ "n": seq }),
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch,
                expected_seq: seq,
            },
        )
        .await
        .expect("seed append");
}

/// A terminal session with a consumed input row: the archive carries
/// both the events and the full inbox row (audit columns included).
async fn seed_terminal(store: &EventStore, with_input: bool) -> SessionId {
    let sid = SessionId::new();
    if with_input {
        let row = InputRow {
            id: uuid::Uuid::new_v4(),
            session: sid,
            payload: json!({"hello": "archive"}),
            admitted_at: 100,
            eligible_at: 100,
            idempotency_key: Some(format!("k-{}", sid.0)),
        };
        store.admit(row).await.expect("admit");
    }
    push(store, sid, 1, Epoch::GENESIS, EventKind::SessionCreated).await;
    push(store, sid, 2, Epoch::GENESIS, EventKind::InputAdmitted).await;
    push(store, sid, 3, Epoch(1), EventKind::DrainClaimed).await;
    push(store, sid, 4, Epoch(1), EventKind::TurnStart).await;
    store
        .append_end(
            sid,
            Event {
                seq: 5,
                session: sid,
                epoch: Epoch(1),
                ts: 1_762_000_000_005,
                kind: EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                bytes: json!({}),
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 5,
            },
        )
        .await
        .expect("end write");
    if with_input {
        // consume the row through the committed seq-2 event
        let id = store
            .archived_inputs_all(sid)
            .await
            .unwrap()
            .first()
            .unwrap()
            .id;
        store.mark_consumed(sid, id, 2).await.expect("consume");
    }
    sid
}

/// Retention enabled with the given thresholds and archive dir.
fn retention_yaml(archive_dir: &std::path::Path, min_age_ms: u64) -> String {
    format!(
        "retention:\n  enabled: true\n  archive_dir: \"{}\"\n  min_age_ms: {min_age_ms}\n",
        archive_dir.display()
    )
}

async fn online_events(db: &std::path::Path, sid: SessionId) -> Vec<Event> {
    let reader = EventStore::open_read_only(db).await.expect("reader");
    let events = reader.rebuild(sid).await.expect("rebuild");
    reader.close().await;
    events
}

/// The full L2 flow: archive → verify → delete → inspect, with the
/// export byte-parity golden checked pre and post archive.
#[tokio::test]
async fn archive_verify_delete_inspect_with_export_byte_parity() {
    let db = temp_db("flow");
    let archive_root = temp_dir("flow-root");
    let cfg_dir = temp_dir("flow-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, true).await;
    let pre_state = {
        let events = store.rebuild(sid).await.unwrap();
        project(&events).expect("project")
    };
    store.close().await;

    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));

    // The pre-archive audit export (the golden bytes).
    let pre_export = kian_cli::commands::run_export(&config, None, sid, None, None, None, false)
        .await
        .expect("pre-archive export");
    let pre_export_ids = kian_cli::commands::run_export(&config, None, sid, None, None, None, true)
        .await
        .expect("pre-archive export --with-event-ids");
    let pre_range =
        kian_cli::commands::run_export(&config, None, sid, Some(2), Some(4), None, false)
            .await
            .expect("pre-archive range export");

    // Run the retention pass.
    let out = kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("archive run");
    assert!(out.contains("1 archived"), "{out}");

    // Online rows are gone; the marker is present.
    assert!(
        online_events(&db, sid).await.is_empty(),
        "online events must be deleted after the verified commit"
    );

    // inspect reports archived + the package path (READ-MODEL §7).
    let inspect = kian_cli::commands::run_inspect(&config, None, sid, false)
        .await
        .expect("inspect archived");
    assert!(inspect.contains("archived"), "{inspect}");
    assert!(
        inspect.contains(&archive_root.display().to_string()),
        "{inspect}"
    );
    let inspect_json: serde_json::Value = serde_json::from_str(
        &kian_cli::commands::run_inspect(&config, None, sid, true)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(inspect_json["archived"], json!(true));
    assert_eq!(inspect_json["state"], json!("archived"));
    assert_eq!(inspect_json["event_count"], json!(5));

    // replay refuses online replay with import guidance.
    let replay_err = kian_cli::commands::run_replay(&config, None, None, sid, false)
        .await
        .expect_err("replay must refuse an archived session");
    assert!(replay_err.contains("session import"), "{replay_err}");

    // The audit export is byte-identical from the package.
    let post_export = kian_cli::commands::run_export(&config, None, sid, None, None, None, false)
        .await
        .expect("post-archive export");
    assert_eq!(
        pre_export, post_export,
        "R7 audit invariance: default export"
    );
    let post_ids = kian_cli::commands::run_export(&config, None, sid, None, None, None, true)
        .await
        .expect("post-archive export --with-event-ids");
    assert_eq!(pre_export_ids, post_ids, "--with-event-ids parity");
    let post_range =
        kian_cli::commands::run_export(&config, None, sid, Some(2), Some(4), None, false)
            .await
            .expect("post-archive range export");
    assert_eq!(pre_range, post_range, "range export parity");

    // archive verify on the archived session passes.
    let verify = kian_cli::archive::run_archive_verify(&config, None, sid, true)
        .await
        .expect("verify");
    let v: serde_json::Value = serde_json::from_str(&verify).unwrap();
    assert_eq!(v["verdict"], json!("verified"));
    assert_eq!(v["archived"], json!(true));
    assert_eq!(v["event_count"], json!(5));
    assert_eq!(v["input_count"], json!(1));

    // Import into a fresh store: the stream projects and re-exports
    // identically, and the input row round-trips with its audit column.
    let fresh = temp_db("flow-import");
    let import = kian_cli::archive::run_import(
        &archive_root.join(sid.0.to_string()),
        Some(&fresh),
        &config,
        false,
    )
    .await
    .expect("import");
    assert!(import.contains("5 events and 1 input"), "{import}");
    let restored = online_events(&fresh, sid).await;
    let re_export = kian_cli::archive::export_bytes(&restored).unwrap();
    assert_eq!(re_export, pre_export, "import re-export parity");
    assert_eq!(
        project(&restored).unwrap(),
        pre_state,
        "imported stream projects the identical state"
    );
    let imported_store = EventStore::open(&fresh).await.unwrap();
    let inputs = imported_store.archived_inputs_all(sid).await.unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].consumed_by_seq, Some(2), "audit column survives");
    imported_store.close().await;
    cleanup_db(&fresh);

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// A corrupt package is refused by BOTH verify and import — and a
/// failed verify never deleted anything.
#[tokio::test]
async fn corrupt_package_is_refused_and_never_deletes() {
    let db = temp_db("corrupt");
    let archive_root = temp_dir("corrupt-root");
    let cfg_dir = temp_dir("corrupt-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, true).await;
    store.close().await;
    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));

    // Phase 1 only — the crash window between write and commit.
    let writer = EventStore::open(&db).await.unwrap();
    let (dir, _manifest) =
        kian_cli::archive::write_package(&writer, &archive_root, sid, "completed")
            .await
            .expect("write package");
    writer.close().await;

    // The crash window leaves the online rows intact and no marker.
    assert_eq!(online_events(&db, sid).await.len(), 5);
    let probe = EventStore::open_read_only(&db).await.unwrap();
    assert!(probe.archived_session(sid).await.unwrap().is_none());
    probe.close().await;

    // Corrupt the events file: verify refuses, import refuses.
    let events_path = dir.join("events.jsonl");
    let good = std::fs::read_to_string(&events_path).unwrap();
    std::fs::write(&events_path, good.replace("\"n\":3", "\"n\":999")).unwrap();
    let err = kian_cli::archive::run_archive_verify(&config, None, sid, false)
        .await
        .expect_err("verify must refuse the tampered package");
    assert!(err.contains("hash mismatch"), "{err}");
    let fresh = temp_db("corrupt-import");
    let err = kian_cli::archive::run_import(&dir, Some(&fresh), &config, false)
        .await
        .expect_err("import must refuse the tampered package");
    assert!(err.contains("hash mismatch"), "{err}");
    cleanup_db(&fresh);

    // A corrupt manifest count is refused too (no silent truncation).
    let manifest_path = dir.join("manifest.json");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    std::fs::write(
        &manifest_path,
        manifest.replace("\"event_count\": 5", "\"event_count\": 4"),
    )
    .unwrap();
    std::fs::write(&events_path, &good).unwrap(); // events restored
    let err = kian_cli::archive::verify_package(&dir, Some(sid))
        .await
        .expect_err("count tamper must refuse");
    assert!(
        err.contains("manifest says 4") || err.contains("hash"),
        "{err}"
    );

    // Nothing was deleted by any refused step.
    assert_eq!(
        online_events(&db, sid).await.len(),
        5,
        "a failed verify never deletes"
    );

    // Restore a good package and finish: the retry is idempotent and
    // completes (write_package rewrites the torn attempt).
    std::fs::write(&events_path, &good).unwrap();
    std::fs::write(&manifest_path, &manifest).unwrap();
    let out = kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("retry completes after the crash window");
    assert!(out.contains("1 archived"), "{out}");
    assert!(online_events(&db, sid).await.is_empty());

    // A second full run is the idempotent no-op.
    let out = kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("second run");
    assert!(out.contains("0 archived"), "{out}");

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// Terminal-only eligibility and the online window: a live session is
/// never archived; a too-young terminal session stays online; a
/// retention-disabled config refuses the run outright.
#[tokio::test]
async fn eligibility_is_terminal_only_and_window_respected() {
    let db = temp_db("elig");
    let archive_root = temp_dir("elig-root");
    let cfg_dir = temp_dir("elig-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let terminal = seed_terminal(&store, false).await;
    let live = SessionId::new();
    push(&store, live, 1, Epoch::GENESIS, EventKind::SessionCreated).await;
    store.close().await;

    // Retention disabled: honest refusal, nothing archived.
    let off_cfg = write_config(&cfg_dir, &db, "");
    let err = kian_cli::archive::run_archive_run(&off_cfg, None, false)
        .await
        .expect_err("disabled retention must refuse");
    assert!(err.contains("retention is not enabled"), "{err}");

    // A huge age window keeps the terminal session online.
    let young_cfg = write_config(
        &cfg_dir,
        &db,
        &retention_yaml(&archive_root, 315_360_000_000),
    );
    let out = kian_cli::archive::run_archive_run(&young_cfg, None, false)
        .await
        .expect("run");
    assert!(out.contains("0 archived"), "{out}");
    assert_eq!(online_events(&db, terminal).await.len(), 5);

    // Age 0: the terminal session archives, the live one never does.
    let now_cfg = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));
    let out = kian_cli::archive::run_archive_run(&now_cfg, None, false)
        .await
        .expect("run");
    assert!(out.contains("1 archived"), "{out}");
    assert!(online_events(&db, terminal).await.is_empty());
    assert_eq!(
        online_events(&db, live).await.len(),
        1,
        "a non-terminal session is never archived"
    );

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// F1 (R9-08): a package whose rows name a different session than its
/// manifest is refused by BOTH verify and import. The manifest is not
/// self-hashed, so hand-editing `manifest.session` alone passes every
/// hash check — identity is a first-class refusal, not a hash side
/// effect. The forged package keeps every hash consistent (only the
/// manifest's `session` field and the directory name change), so this
/// exercises the identity check alone.
#[tokio::test]
async fn import_refuses_an_identity_inconsistent_package() {
    let db = temp_db("identity");
    let archive_root = temp_dir("identity-root");
    let cfg_dir = temp_dir("identity-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, true).await;
    store.close().await;
    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));

    // Phase 1 only: an honest package on disk for `sid` (no commit).
    let writer = EventStore::open(&db).await.unwrap();
    let (dir, _) = kian_cli::archive::write_package(&writer, &archive_root, sid, "completed")
        .await
        .expect("write package");
    writer.close().await;

    // Forge: identical events/inputs bytes (both file hashes and the
    // aggregate still match), manifest renamed to another session, and
    // the package directory renamed to match the forged id — every
    // pre-R9-08 check passes on this package.
    let forged = SessionId::new();
    let forged_dir = archive_root.join(forged.0.to_string());
    std::fs::create_dir_all(&forged_dir).unwrap();
    std::fs::copy(dir.join("events.jsonl"), forged_dir.join("events.jsonl")).unwrap();
    std::fs::copy(dir.join("inputs.jsonl"), forged_dir.join("inputs.jsonl")).unwrap();
    let manifest = std::fs::read_to_string(dir.join("manifest.json")).unwrap();
    let forged_manifest = manifest.replace(
        &format!("\"session\": \"{}\"", sid.0),
        &format!("\"session\": \"{}\"", forged.0),
    );
    assert_ne!(
        manifest, forged_manifest,
        "the manifest session field must be rewritten"
    );
    std::fs::write(forged_dir.join("manifest.json"), &forged_manifest).unwrap();

    // `archive verify` on the manifest's OWN id refuses.
    let err = kian_cli::archive::run_archive_verify(&config, None, forged, false)
        .await
        .expect_err("verify must refuse the identity-inconsistent package");
    assert!(err.contains("does not match manifest session"), "{err}");

    // `session import` refuses, and the target store stays empty —
    // nothing restored under either the rows' session or the manifest's.
    let fresh = temp_db("identity-import");
    let err = kian_cli::archive::run_import(&forged_dir, Some(&fresh), &config, false)
        .await
        .expect_err("import must refuse the identity-inconsistent package");
    assert!(err.contains("does not match manifest session"), "{err}");
    // The refusal fires before the target store is even touched: the
    // file was never created (or, if a stale file existed, holds
    // nothing under either session id).
    if fresh.exists() {
        assert!(
            online_events(&fresh, sid).await.is_empty(),
            "nothing may be restored under the rows' session"
        );
        let probe = EventStore::open_read_only(&fresh).await.unwrap();
        assert_eq!(
            probe.version(forged).await.unwrap(),
            0,
            "nothing under the manifest's session either"
        );
        probe.close().await;
    }
    cleanup_db(&fresh);

    // The honest package is untouched by the forgery: it still verifies.
    let out = kian_cli::archive::run_archive_verify(&config, None, sid, false)
        .await
        .expect("the honest package still verifies");
    assert!(out.contains("verified"), "{out}");

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// F3 (R9-08): `usage-stats --session` must not silently zero-report an
/// archived session — R7's explicit `archived` signal with import
/// guidance, the same class of refusal as `session replay`.
#[tokio::test]
async fn usage_stats_refuses_an_archived_session() {
    let db = temp_db("arch-usage");
    let archive_root = temp_dir("arch-usage-root");
    let cfg_dir = temp_dir("arch-usage-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, false).await;
    store.close().await;
    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));
    kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("archive");

    let err = kian_cli::usage_metrics::run_usage_stats(&config, Some(sid), false)
        .await
        .expect_err("usage-stats must report archived, not zeros");
    assert!(err.contains("is archived"), "{err}");
    assert!(err.contains("session import"), "import guidance: {err}");

    // The whole-store report (no per-session range) is unaffected.
    let ok = kian_cli::usage_metrics::run_usage_stats(&config, None, false)
        .await
        .expect("whole-store report still works");
    assert!(ok.contains("sessions"), "{ok}");

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// F3 (R9-08): `cache-stats --session` — same archived refusal as
/// usage-stats (both per-session aggregate reads answer R7's signal).
#[tokio::test]
async fn cache_stats_refuses_an_archived_session() {
    let db = temp_db("arch-cache");
    let archive_root = temp_dir("arch-cache-root");
    let cfg_dir = temp_dir("arch-cache-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, false).await;
    store.close().await;
    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));
    kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("archive");

    let err = kian_cli::cache_metrics::run_cache_stats(&config, Some(sid), false)
        .await
        .expect_err("cache-stats must report archived, not zeros");
    assert!(err.contains("is archived"), "{err}");
    assert!(err.contains("session import"), "import guidance: {err}");

    // The whole-store report (no per-session range) is unaffected.
    let ok = kian_cli::cache_metrics::run_cache_stats(&config, None, false)
        .await
        .expect("whole-store report still works");
    assert!(ok.contains("sessions"), "{ok}");

    cleanup_db(&db);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}

/// `session import` refuses an existing session id — never merges,
/// never overwrites.
#[tokio::test]
async fn import_refuses_an_existing_session() {
    let db = temp_db("import-dup");
    let archive_root = temp_dir("import-dup-root");
    let cfg_dir = temp_dir("import-dup-cfg");
    let store = EventStore::open(&db).await.expect("open");
    let sid = seed_terminal(&store, false).await;
    store.close().await;
    let config = write_config(&cfg_dir, &db, &retention_yaml(&archive_root, 0));
    kian_cli::archive::run_archive_run(&config, None, false)
        .await
        .expect("archive");

    // Import into the SAME store (the session still has its marker).
    let err = kian_cli::archive::run_import(
        &archive_root.join(sid.0.to_string()),
        Some(&db),
        &config,
        false,
    )
    .await
    .expect_err("import over an existing session must refuse");
    assert!(err.contains("already exists"), "{err}");

    // And into a store that holds the session's events.
    let dup = temp_db("import-dup-2");
    {
        let s = EventStore::open(&dup).await.unwrap();
        kian_cli::archive::run_import(
            &archive_root.join(sid.0.to_string()),
            Some(&dup),
            &config,
            false,
        )
        .await
        .expect("first import ok");
        s.close().await;
    }
    let err = kian_cli::archive::run_import(
        &archive_root.join(sid.0.to_string()),
        Some(&dup),
        &config,
        false,
    )
    .await
    .expect_err("second import must refuse");
    assert!(err.contains("already exists"), "{err}");

    cleanup_db(&db);
    cleanup_db(&dup);
    let _ = std::fs::remove_dir_all(&archive_root);
    let _ = std::fs::remove_dir_all(&cfg_dir);
}
