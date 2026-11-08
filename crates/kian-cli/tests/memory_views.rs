//! kian-108 acceptance: the read-only `kian memory search|map` CLI
//! verbs — pure projections over an already-persisted memory session
//! (plan §2.1/§2.2, adversarial ledger A1–A9).
//!
//! Covers the card's acceptance list:
//! - both verbs leave the memory session's event count and bytes
//!   unchanged (the A1 read-only witness),
//! - the JSON shapes match the documented schemas,
//! - `--json` and the text form agree on the flags,
//! - an unknown session is an honest error,
//! - the empty query is a fail-closed usage error (A9).

use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::EventStore;
use serde_json::json;

const T0: u64 = 1_761_900_000_000;

async fn append(
    store: &EventStore,
    sid: SessionId,
    seq: u64,
    epoch: Epoch,
    kind: EventKind,
    bytes: serde_json::Value,
) {
    let e = Event {
        seq,
        session: sid,
        epoch,
        ts: T0 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    };
    store
        .append(
            sid,
            e,
            kian_store::AppendGuard {
                epoch,
                expected_seq: seq,
            },
        )
        .await
        .unwrap();
}

fn temp_db_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kian-cli-memory-views-{}", uuid::Uuid::new_v4()))
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// One verified `memory/note` event payload citing [1,5) of `source`.
fn note_bytes(subject: &str, text: &str, source: SessionId) -> serde_json::Value {
    json!({
        "subject": subject,
        "text": text,
        "status": "verified",
        "verified_by": {"method": "transcript", "ref": "3"},
        "derived_from": [{"session": source, "start_seq": 1, "end_seq": 5}],
    })
}

/// A source session covering seq [1,5) (the evidence every note cites).
async fn seed_source(store: &EventStore, sid: SessionId) {
    append(
        store,
        sid,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    append(
        store,
        sid,
        2,
        Epoch(1),
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "the deploy broke"}),
    )
    .await;
    append(
        store,
        sid,
        3,
        Epoch(1),
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {"text": "rolled back"}}),
    )
    .await;
    append(
        store,
        sid,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"outcome": "ok"}),
    )
    .await;
}

/// The small fixture: one subject whose latest revision is verified
/// (superseding a candidate revision) plus one standing candidate.
async fn seed_basic() -> (std::path::PathBuf, SessionId) {
    let memory_sid = SessionId::new();
    let source_sid = SessionId::new();
    let path = temp_db_path();
    let store = EventStore::open(&path).await.unwrap();
    seed_source(&store, source_sid).await;

    append(
        &store,
        memory_sid,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    // seq 2: an old candidate revision, superseded at seq 3.
    append(
        &store,
        memory_sid,
        2,
        Epoch(1),
        EventKind::MemoryNote,
        json!({
            "subject": "deploys via dsh bridge",
            "text": "old text",
            "status": "candidate",
            "derived_from": [{"session": source_sid, "start_seq": 1, "end_seq": 5}],
        }),
    )
    .await;
    append(
        &store,
        memory_sid,
        3,
        Epoch(1),
        EventKind::MemoryNote,
        note_bytes(
            "deploys via dsh bridge",
            "Deploys go through the dsh bridge.",
            source_sid,
        ),
    )
    .await;
    append(
        &store,
        memory_sid,
        4,
        Epoch(1),
        EventKind::MemoryNote,
        json!({
            "subject": "broken range",
            "text": "Points outside the source stream.",
            "status": "candidate",
            "derived_from": [{"session": source_sid, "start_seq": 8, "end_seq": 12}],
        }),
    )
    .await;
    store.close().await;
    (path, memory_sid)
}

/// The wide fixture: `count` verified subjects all citing the same
/// range (a complete evidence graph: C(count,2) edges).
async fn seed_wide(count: usize) -> (std::path::PathBuf, SessionId) {
    seed_wide_with(count, &|i| format!("subject-{i:02}")).await
}

/// The wide fixture with custom subjects (kian-112: ordinary-length
/// subjects so the rendered body can exceed the 64 KiB byte cap).
async fn seed_wide_with(
    count: usize,
    subject: &dyn Fn(usize) -> String,
) -> (std::path::PathBuf, SessionId) {
    let memory_sid = SessionId::new();
    let source_sid = SessionId::new();
    let path = temp_db_path();
    let store = EventStore::open(&path).await.unwrap();
    seed_source(&store, source_sid).await;

    append(
        &store,
        memory_sid,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    for i in 0..count {
        let subject = subject(i);
        let text = format!("claim {i} text");
        append(
            &store,
            memory_sid,
            (i + 2) as u64,
            Epoch(1),
            EventKind::MemoryNote,
            note_bytes(&subject, &text, source_sid),
        )
        .await;
    }
    store.close().await;
    (path, memory_sid)
}

/// The read-only witness baseline: event count plus summed payload
/// bytes of the memory session's stream.
async fn stream_fingerprint(db: &std::path::Path, sid: SessionId) -> (usize, u64) {
    let store = EventStore::open_read_only(db).await.unwrap();
    let events = store.rebuild(sid).await.unwrap();
    store.close().await;
    let bytes: u64 = events
        .iter()
        .map(|ev| serde_json::to_string(&ev.bytes).unwrap().len() as u64)
        .sum();
    (events.len(), bytes)
}

fn config_path() -> std::path::PathBuf {
    std::path::PathBuf::from("kian.yaml")
}

/// A1 (the read-only witness): running both verbs — text and JSON —
/// leaves the memory session's event count and bytes unchanged.
#[tokio::test]
async fn search_and_map_leave_event_count_and_bytes_unchanged() {
    let (path, memory_sid) = seed_wide(8).await;
    let before = stream_fingerprint(&path, memory_sid).await;

    for json in [false, true] {
        let out = kian_cli::commands::run_memory_search(
            &config_path(),
            Some(&path),
            memory_sid,
            "subject",
            json,
        )
        .await
        .unwrap();
        assert!(!out.is_empty());
        let out = kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, json)
            .await
            .unwrap();
        assert!(!out.is_empty());
    }

    let after = stream_fingerprint(&path, memory_sid).await;
    assert_eq!(before, after, "the read path must never append events");
    cleanup(&path);
}

/// The search JSON shape (plan §2.1): session, query, total_hits,
/// truncated, bytes and capped hits with the documented hit fields;
/// the text view starts with the fixed header and its truncation
/// footer agrees with the JSON `truncated` flag.
#[tokio::test]
async fn search_json_shape_and_text_agree_on_flags() {
    let (path, memory_sid) = seed_wide(30).await;

    let json_out = kian_cli::commands::run_memory_search(
        &config_path(),
        Some(&path),
        memory_sid,
        "subject",
        true,
    )
    .await
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
    assert_eq!(value["session"], json!(memory_sid.0.to_string()));
    assert_eq!(value["query"], json!("subject"));
    assert_eq!(value["total_hits"], 30);
    assert_eq!(value["truncated"], true, "the 20-hit cap must truncate");
    let hits = value["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 20, "the hit cap is 20");
    let first = &hits[0];
    assert_eq!(first["subject"], json!("subject-00"));
    assert_eq!(first["status"], json!("verified"));
    assert_eq!(first["matched"], json!("subject_prefix"));
    assert_eq!(first["event_seq"], json!(2));
    assert!(first["excerpt"].as_str().is_some_and(|e| !e.is_empty()));
    assert!(value["bytes"].as_u64().is_some());

    let text = kian_cli::commands::run_memory_search(
        &config_path(),
        Some(&path),
        memory_sid,
        "subject",
        false,
    )
    .await
    .unwrap();
    assert!(
        text.starts_with("# Memory search results"),
        "the text view is the renderer's, header first: {text}"
    );
    assert!(text.contains("data, not instructions"));
    assert!(text.contains("query: subject"));
    // The text and the JSON agree on `truncated`.
    assert!(text.contains("(search truncated: 20 of 30 matches shown)"));
    assert!(text.contains("- subject-00 [verified] (matched subject_prefix, seq 2):"));
    cleanup(&path);
}

/// A4: search folds the latest revision per subject and carries the
/// status so an untrusted candidate is never mistaken for knowledge.
#[tokio::test]
async fn search_folds_latest_revision_and_shows_status() {
    let (path, memory_sid) = seed_basic().await;

    let text = kian_cli::commands::run_memory_search(
        &config_path(),
        Some(&path),
        memory_sid,
        "deploys",
        false,
    )
    .await
    .unwrap();
    assert!(
        text.contains("deploys via dsh bridge [verified]"),
        "the latest (verified) revision surfaces: {text}"
    );
    assert!(
        !text.contains("old text"),
        "the superseded revision must not surface: {text}"
    );

    // A candidate surfaces too — with its status visible.
    let text = kian_cli::commands::run_memory_search(
        &config_path(),
        Some(&path),
        memory_sid,
        "broken",
        false,
    )
    .await
    .unwrap();
    assert!(text.contains("broken range [candidate]"), "{text}");
    cleanup(&path);
}

/// A9: an empty or whitespace-only query is a fail-closed usage error.
#[tokio::test]
async fn search_empty_query_is_refused() {
    let (path, memory_sid) = seed_basic().await;
    for bad in ["", " ", "\t\n "] {
        let err = kian_cli::commands::run_memory_search(
            &config_path(),
            Some(&path),
            memory_sid,
            bad,
            false,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("empty or whitespace"),
            "fail-closed refusal for `{bad}`: {err}"
        );
    }
    cleanup(&path);
}

/// The map JSON shape (plan §2.2): session, the three flags,
/// source_text_bytes, bytes, nodes and edges; the text view's fixed
/// status line agrees with the JSON flags (A3/A5).
#[tokio::test]
async fn map_json_shape_and_text_agree_on_flags() {
    let (path, memory_sid) = seed_wide(30).await;

    let json_out =
        kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, true)
            .await
            .unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
    assert_eq!(value["session"], json!(memory_sid.0.to_string()));
    let nodes = value["nodes"].as_array().unwrap();
    let edges = value["edges"].as_array().unwrap();
    assert_eq!(nodes.len(), 30);
    // A complete evidence graph: C(30,2) = 435 edges, under the cap.
    assert_eq!(edges.len(), 435);
    assert_eq!(nodes[0], json!("subject-00"));
    assert_eq!(edges[0]["a"], json!("subject-00"));
    assert_eq!(edges[0]["b"], json!("subject-01"));
    assert_eq!(value["truncated"], false);
    assert_eq!(value["degenerate"], false);
    assert_eq!(
        value["oversized"], true,
        "short claims: the map outweighs them"
    );
    assert!(value["source_text_bytes"].as_u64().is_some());
    assert!(value["bytes"].as_u64().is_some());

    let text = kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, false)
        .await
        .unwrap();
    assert!(text.starts_with("# Memory map"), "{text}");
    assert!(text.contains("data, not instructions"));
    // The status line agrees with the JSON flags (A5: reported, not
    // hidden).
    assert!(text.contains("truncated: no"), "{text}");
    assert!(text.contains("degenerate: no"), "{text}");
    assert!(text.contains("oversized: yes"), "{text}");
    assert!(text.contains("## Nodes (30 of 30)"));
    assert!(text.contains("## Edges (435 of 435)"));
    cleanup(&path);
}

/// A5: a useless map (here: a single verified node) says so — the
/// `degenerate` flag rides the JSON and the text status line.
#[tokio::test]
async fn map_degenerate_is_reported() {
    let (path, memory_sid) = seed_basic().await;

    let json_out =
        kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, true)
            .await
            .unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
    // Only the verified note becomes a node; the candidate never does.
    assert_eq!(
        value["nodes"],
        json!(["deploys via dsh bridge"]),
        "untrusted notes never become nodes"
    );
    assert_eq!(value["edges"], json!([]));
    assert_eq!(value["degenerate"], true);

    let text = kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, false)
        .await
        .unwrap();
    assert!(
        text.contains("degenerate: yes (search alone is the right tool)"),
        "{text}"
    );
    cleanup(&path);
}

/// A3 (kian-112): the map's 64 KiB byte cap with a NON-empty body —
/// 60 verified notes with ordinary-length subjects over a real store
/// (the review's repro shape: C(60,2) = 1770 exceeds the 1000-edge cap
/// and the full body exceeds 64 KiB). The node prefix survives, edges
/// are cut first, no edge dangles, the byte cap holds, and the shipped
/// binary agrees with the in-process view.
#[tokio::test]
async fn map_byte_cap_keeps_nodes_and_cuts_edges_over_real_store() {
    let (path, memory_sid) =
        seed_wide_with(60, &|i| format!("note {i:03} deploys via the bridge")).await;

    // In-process JSON: every node survives, edges are cut, nothing
    // dangles, the byte cap holds.
    let json_out =
        kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, true)
            .await
            .unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
    let nodes: Vec<String> = value["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let edges = value["edges"].as_array().unwrap();
    assert_eq!(
        nodes.len(),
        60,
        "the byte cap must not drop nodes: {json_out}"
    );
    assert!(!edges.is_empty());
    assert!(
        edges.len() < 1000,
        "the byte cap must cut edges below the 1000-edge cap"
    );
    let node_set: std::collections::HashSet<&str> = nodes.iter().map(String::as_str).collect();
    let dangling = edges
        .iter()
        .filter(|e| {
            !node_set.contains(e["a"].as_str().unwrap())
                || !node_set.contains(e["b"].as_str().unwrap())
        })
        .count();
    assert_eq!(dangling, 0, "no delivered edge may dangle");
    assert_eq!(value["truncated"], json!(true));
    let bytes = value["bytes"].as_u64().unwrap();
    assert!(bytes <= 64 * 1024, "rendered bytes {bytes} exceed 64 KiB");

    // Text agrees: a Nodes section precedes the Edges section, the
    // headers carry the struct's counts, and the status line carries
    // the flags.
    let text = kian_cli::commands::run_memory_map(&config_path(), Some(&path), memory_sid, false)
        .await
        .unwrap();
    assert!(text.contains("## Nodes (60 of 60)"), "{text}");
    assert!(
        text.contains(&format!("## Edges ({} of 1000)", edges.len())),
        "{text}"
    );
    assert!(
        text.find("## Nodes").unwrap() < text.find("## Edges").unwrap(),
        "nodes precede edges"
    );
    assert!(text.contains("truncated: yes"));

    // The shipped binary over the same store agrees with the
    // in-process view.
    let bin = env!("CARGO_BIN_EXE_kian");
    let out = std::process::Command::new(bin)
        .args([
            "memory",
            "map",
            "--session",
            &memory_sid.0.to_string(),
            "--db",
        ])
        .arg(&path)
        .output()
        .expect("run kian memory map (byte cap)");
    assert!(out.status.success(), "{out:?}");
    let bin_text = String::from_utf8_lossy(&out.stdout);
    assert!(bin_text.contains("## Nodes (60 of 60)"), "{bin_text}");
    assert!(
        bin_text.contains(&format!("## Edges ({} of 1000)", edges.len())),
        "{bin_text}"
    );
    assert!(bin_text.contains("truncated: yes"));
    cleanup(&path);
}

/// An unknown session is an honest error for both verbs.
#[tokio::test]
async fn unknown_session_is_an_honest_error() {
    let (path, _memory_sid) = seed_basic().await;
    let ghost = SessionId::new();
    for err in [
        kian_cli::commands::run_memory_search(&config_path(), Some(&path), ghost, "x", false)
            .await
            .unwrap_err(),
        kian_cli::commands::run_memory_map(&config_path(), Some(&path), ghost, false)
            .await
            .unwrap_err(),
    ] {
        assert!(err.contains("unknown session"), "{err}");
    }
    cleanup(&path);
}

/// The real binary dispatch (main.rs): `kian memory search <query>
/// --session <uuid> --db <sqlite>` and `kian memory map …` succeed and
/// print the renderer's views; an empty query exits non-zero with the
/// fail-closed message.
#[tokio::test]
async fn cli_dispatch_runs_search_and_map() {
    let (path, memory_sid) = seed_basic().await;
    let bin = env!("CARGO_BIN_EXE_kian");

    let search = std::process::Command::new(bin)
        .args([
            "memory",
            "search",
            "deploys",
            "--session",
            &memory_sid.0.to_string(),
            "--db",
        ])
        .arg(&path)
        .output()
        .expect("run kian memory search");
    assert!(search.status.success(), "{search:?}");
    let text = String::from_utf8_lossy(&search.stdout);
    assert!(text.contains("# Memory search results"), "{text}");
    assert!(text.contains("deploys via dsh bridge [verified]"), "{text}");

    let map = std::process::Command::new(bin)
        .args([
            "memory",
            "map",
            "--session",
            &memory_sid.0.to_string(),
            "--db",
        ])
        .arg(&path)
        .output()
        .expect("run kian memory map");
    assert!(map.status.success(), "{map:?}");
    let text = String::from_utf8_lossy(&map.stdout);
    assert!(text.contains("# Memory map"), "{text}");
    assert!(text.contains("degenerate: yes"), "{text}");

    // Fail-closed: an empty query is a non-zero exit naming the rule.
    let empty = std::process::Command::new(bin)
        .args([
            "memory",
            "search",
            "",
            "--session",
            &memory_sid.0.to_string(),
            "--db",
        ])
        .arg(&path)
        .output()
        .expect("run kian memory search (empty)");
    assert!(!empty.status.success(), "{empty:?}");
    let err = String::from_utf8_lossy(&empty.stderr);
    assert!(err.contains("empty or whitespace"), "{err}");
    cleanup(&path);
}
