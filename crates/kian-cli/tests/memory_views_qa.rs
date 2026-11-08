//! kian-109 QA acceptance: independent probes for the read-only P3
//! views (`kian memory list|show|search|map`), authored by QA with its
//! own fixtures (real sqlite stores; one fixture lands its notes via a
//! real memory-dream drain against the scripted mock provider — no
//! network beyond localhost, `NO_PROXY` forced).
//!
//! Behaviour -> test map (the card's acceptance list):
//! 1. Read-only ............ qa_views_never_write_and_missing_db_fails_closed
//! 2. Determinism .......... qa_views_are_byte_identical_across_runs_and_interleavings
//! 3. Caps ................. qa_over_limit_search_and_map_truncate_text_json_agree
//! 4. Status handling ...... qa_map_is_verified_only_and_search_surfaces_every_status
//! 5. Heuristic honesty .... qa_map_reports_degenerate_and_oversized_honestly
//! 6. Data framing ......... qa_both_views_lead_with_the_data_not_instructions_frame
//! 7. No fork of truth ..... qa_real_drain_notes_and_a_deleting_event_change_the_view

mod common;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::response::IntoResponse;
use kian_cli::api::ControlPlane;
use kian_cli::ops::OpsPlane;
use kian_cli::server::Server;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::json;

// ---------------------------------------------------------------------
// Fixtures (QA-authored, independent of the implementer's tests).
// ---------------------------------------------------------------------

/// Fixed session ids so two stores of the same logical fixture render
/// byte-identical JSON (the `session` field must not differ).
const MEMORY_SID: &str = "11111111-2222-3333-4444-555555555555";
const SOURCE_SID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn memory_id() -> SessionId {
    SessionId(uuid::Uuid::parse_str(MEMORY_SID).unwrap())
}

fn source_id() -> SessionId {
    SessionId(uuid::Uuid::parse_str(SOURCE_SID).unwrap())
}

const T0: u64 = 1_762_000_000_000;

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
            AppendGuard {
                epoch,
                expected_seq: seq,
            },
        )
        .await
        .unwrap();
}

fn config_path() -> PathBuf {
    PathBuf::from("kian.yaml")
}

/// A `memory/note` payload. `status` picks the shape; a verified note
/// carries a valid `verified_by`, everything else does not.
fn note_payload(
    subject: &str,
    text: &str,
    status: &str,
    start: u64,
    end: u64,
) -> serde_json::Value {
    let mut payload = json!({
        "subject": subject,
        "text": text,
        "status": status,
        "derived_from": [{"session": SOURCE_SID, "start_seq": start, "end_seq": end}],
    });
    if status == "verified" {
        payload["verified_by"] = json!({"method": "transcript", "ref": "3"});
    }
    payload
}

async fn open(db: &Path) -> EventStore {
    EventStore::open(db).await.unwrap()
}

/// The source stream every note cites: SessionCreated + 3 events
/// (seq 1..=4, the cited window is [1,5)).
async fn seed_source(store: &EventStore) {
    let sid = source_id();
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
        json!({"role": "user", "content": "router prefers blue-green today"}),
    )
    .await;
    append(
        store,
        sid,
        3,
        Epoch(1),
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {"text": "noted"}}),
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

async fn seed_memory_created(store: &EventStore) {
    append(
        store,
        memory_id(),
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
}

/// The QA fixture: four subjects exercising the status vocabulary and
/// the revision fold — alpha verified, beta candidate, gamma stale,
/// delta verified then superseded by a candidate revision.
async fn seed_status_fixture(db: &Path) {
    let store = open(db).await;
    seed_source(&store).await;
    seed_memory_created(&store).await;
    let notes = [
        note_payload("alpha claim", "alpha verified text", "verified", 1, 5),
        note_payload("beta claim", "beta untrusted text", "candidate", 1, 5),
        note_payload("gamma claim", "gamma retired text", "stale", 1, 5),
        note_payload("delta claim", "delta old verified text", "verified", 1, 5),
        note_payload(
            "delta claim",
            "delta newer candidate text",
            "candidate",
            1,
            5,
        ),
    ];
    for (i, bytes) in notes.iter().enumerate() {
        append(
            &store,
            memory_id(),
            (i + 2) as u64,
            Epoch(1),
            EventKind::MemoryNote,
            bytes.clone(),
        )
        .await;
    }
    store.close().await;
}

/// `count` verified subjects all citing [1,5): a complete evidence
/// graph C(count,2) — the over-cap fixture.
async fn seed_wide(db: &Path, count: usize, text_len: usize) {
    let store = open(db).await;
    seed_source(&store).await;
    seed_memory_created(&store).await;
    let text = "x".repeat(text_len.max(1));
    for i in 0..count {
        let subject = format!("node-{i:03}");
        append(
            &store,
            memory_id(),
            (i + 2) as u64,
            Epoch(1),
            EventKind::MemoryNote,
            note_payload(&subject, &text, "verified", 1, 5),
        )
        .await;
    }
    store.close().await;
}

/// The same note set as `seed_wide`, but the appends are interleaved
/// with source-session writes and followed by trailing non-note events
/// on the memory session — same folded notes, different stream shape.
async fn seed_wide_interleaved(db: &Path, count: usize) {
    let store = open(db).await;
    seed_source(&store).await;
    seed_memory_created(&store).await;
    let text = "x".repeat(32);
    for i in 0..count {
        let subject = format!("node-{i:03}");
        append(
            &store,
            memory_id(),
            (i + 2) as u64,
            Epoch(1),
            EventKind::MemoryNote,
            note_payload(&subject, &text, "verified", 1, 5),
        )
        .await;
        // Interleave: a source-session write between memory appends.
        append(
            &store,
            source_id(),
            5 + i as u64,
            Epoch(1),
            EventKind::TurnEnd,
            json!({"outcome": "ok", "round": i}),
        )
        .await;
    }
    // Trailing non-note events on the memory session (stream shape
    // differs, folded note set identical).
    append(
        &store,
        memory_id(),
        (count + 2) as u64,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"outcome": "ok", "trailing": true}),
    )
    .await;
    store.close().await;
}

/// The read-only witness: event count plus summed envelope bytes of
/// the memory session's stream (envelope includes kind + payload, so
/// any append or byte change moves it).
async fn stream_fingerprint(db: &Path) -> (usize, u64) {
    let store = EventStore::open_read_only(db).await.unwrap();
    let events = store.rebuild(memory_id()).await.unwrap();
    store.close().await;
    let bytes: u64 = events
        .iter()
        .map(|ev| serde_json::to_string(ev).unwrap().len() as u64)
        .sum();
    (events.len(), bytes)
}

fn temp_db(tag: &str) -> PathBuf {
    let dir = common::temp_dir(tag);
    dir.join("kian.sqlite")
}

async fn search(db: &Path, query: &str, json: bool) -> String {
    kian_cli::commands::run_memory_search(&config_path(), Some(db), memory_id(), query, json)
        .await
        .unwrap()
}

async fn map(db: &Path, json: bool) -> String {
    kian_cli::commands::run_memory_map(&config_path(), Some(db), memory_id(), json)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------
// 1. Read-only
// ---------------------------------------------------------------------

/// A1 + acceptance 1: all four verbs — text and JSON — leave the
/// memory session's stream byte-identical, and the read path is the
/// fail-closed read-only open: a missing db is an error, never a
/// silently created empty store (a read-write open would create and
/// migrate the file).
#[tokio::test]
async fn qa_views_never_write_and_missing_db_fails_closed() {
    let db = temp_db("memqa-readonly");
    seed_wide(&db, 6, 32).await;
    let before = stream_fingerprint(&db).await;

    let listed = kian_cli::commands::run_memory_list(&config_path(), Some(&db), memory_id(), false)
        .await
        .unwrap();
    assert!(listed.contains("node-000"), "{listed}");
    let listed_json =
        kian_cli::commands::run_memory_list(&config_path(), Some(&db), memory_id(), true)
            .await
            .unwrap();
    assert!(listed_json.contains("\"notes\""));
    let shown = kian_cli::commands::run_memory_show(
        &config_path(),
        Some(&db),
        memory_id(),
        "node-000",
        false,
    )
    .await
    .unwrap();
    assert!(shown.contains("node-000"), "{shown}");
    for json in [false, true] {
        assert!(!search(&db, "node", json).await.is_empty());
        assert!(!map(&db, json).await.is_empty());
    }

    let after = stream_fingerprint(&db).await;
    assert_eq!(before, after, "the read path must never append events");

    // Fail-closed read-only open: a missing db is an error and no file
    // is created.
    let missing = db.parent().unwrap().join("no-such-store.sqlite");
    let err = kian_cli::commands::run_memory_search(
        &config_path(),
        Some(&missing),
        memory_id(),
        "x",
        false,
    )
    .await
    .unwrap_err();
    assert!(!err.is_empty(), "{err}");
    let err =
        kian_cli::commands::run_memory_map(&config_path(), Some(&missing), memory_id(), false)
            .await
            .unwrap_err();
    assert!(!err.is_empty(), "{err}");
    assert!(!missing.exists(), "a read-only open never creates the db");
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

// ---------------------------------------------------------------------
// 2. Determinism
// ---------------------------------------------------------------------

/// A2 + acceptance 2: the same note set renders byte-identical views
/// across repeated runs, across two separately-built stores, and
/// regardless of event insertion interleaving (source-session writes
/// interleaved between memory appends, trailing non-note events).
#[tokio::test]
async fn qa_views_are_byte_identical_across_runs_and_interleavings() {
    let plain = temp_db("memqa-det-plain");
    let interleaved = temp_db("memqa-det-interleaved");
    seed_wide(&plain, 12, 32).await;
    seed_wide_interleaved(&interleaved, 12).await;

    for json in [false, true] {
        let s1 = search(&plain, "node", json).await;
        let s2 = search(&plain, "node", json).await;
        assert_eq!(s1, s2, "same store, back-to-back runs must be identical");
        let s3 = search(&interleaved, "node", json).await;
        assert_eq!(
            s1, s3,
            "insertion interleaving must not change the search view"
        );

        let m1 = map(&plain, json).await;
        let m2 = map(&plain, json).await;
        assert_eq!(m1, m2, "same store, back-to-back runs must be identical");
        let m3 = map(&interleaved, json).await;
        assert_eq!(m1, m3, "insertion interleaving must not change the map");
    }

    // The folded content really is the same: 12 hits, 12 nodes, 66
    // edges (C(12,2)) in both stores.
    let sj: serde_json::Value = serde_json::from_str(&search(&plain, "node", true).await).unwrap();
    assert_eq!(sj["total_hits"], 12);
    let mj: serde_json::Value = serde_json::from_str(&map(&plain, true).await).unwrap();
    assert_eq!(mj["nodes"].as_array().unwrap().len(), 12);
    assert_eq!(mj["edges"].as_array().unwrap().len(), 66);
    let _ = std::fs::remove_dir_all(plain.parent().unwrap());
    let _ = std::fs::remove_dir_all(interleaved.parent().unwrap());
}

// ---------------------------------------------------------------------
// 3. Caps
// ---------------------------------------------------------------------

/// A3 + acceptance 3: over-limit searches and maps truncate with
/// `truncated: true`, and the text and JSON forms agree on it.
#[tokio::test]
async fn qa_over_limit_search_and_map_truncate_text_json_agree() {
    // Search: 30 matches, hit cap 20.
    let db = temp_db("memqa-caps-search");
    seed_wide(&db, 30, 32).await;
    let sj: serde_json::Value = serde_json::from_str(&search(&db, "node", true).await).unwrap();
    assert_eq!(sj["total_hits"], 30);
    assert_eq!(sj["hits"].as_array().unwrap().len(), 20);
    assert_eq!(sj["truncated"], true, "the hit cap must set truncated");
    let st = search(&db, "node", false).await;
    assert!(
        st.contains("(search truncated: 20 of 30 matches shown)"),
        "text and JSON agree on truncation: {st}"
    );
    let _ = std::fs::remove_dir_all(db.parent().unwrap());

    // Map: 210 verified notes sharing evidence — node cap 200, edge
    // cap 1000 (C(210,2) = 21845 before caps).
    let db = temp_db("memqa-caps-map");
    seed_wide(&db, 210, 32).await;
    let mj: serde_json::Value = serde_json::from_str(&map(&db, true).await).unwrap();
    let nodes = mj["nodes"].as_array().unwrap();
    let edges = mj["edges"].as_array().unwrap();
    assert_eq!(nodes.len(), 200, "the node cap is 200");
    assert_eq!(edges.len(), 1000, "the edge cap is 1000");
    assert_eq!(mj["truncated"], true, "over-cap must set truncated");
    // The kept nodes are the deterministic byte-order prefix.
    assert_eq!(nodes[0], json!("node-000"));
    assert_eq!(nodes[199], json!("node-199"));
    // Every edge endpoint is a kept node (no dangling references).
    for e in edges {
        let a = e["a"].as_str().unwrap();
        let b = e["b"].as_str().unwrap();
        assert!(a < b, "edge endpoints are byte-ordered: {a} !< {b}");
        assert!(a.starts_with("node-") && b.starts_with("node-"));
    }
    let mt = map(&db, false).await;
    assert!(mt.contains("truncated: yes"), "text agrees: {mt}");
    assert_eq!(mj["truncated"], true);
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

// ---------------------------------------------------------------------
// 4. Status handling
// ---------------------------------------------------------------------

/// A4 + acceptance 4: the map is verified-only (candidate and stale
/// never appear as nodes; a subject superseded by a candidate falls
/// back to its last verified revision), while search surfaces a status
/// for every hit — verified, candidate and stale all visible.
#[tokio::test]
async fn qa_map_is_verified_only_and_search_surfaces_every_status() {
    let db = temp_db("memqa-status");
    seed_status_fixture(&db).await;

    // Map: only subjects whose LATEST revision is verified become
    // nodes (the read path folds `project_notes` first — the same
    // composition the P2 injection index uses, server.rs injection
    // path — so delta, superseded by a candidate revision at seq 6,
    // is out entirely; the `eligible_notes` "last verified fallback"
    // only exists for unfolded input).
    let mj: serde_json::Value = serde_json::from_str(&map(&db, true).await).unwrap();
    let nodes: Vec<&str> = mj["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_str().unwrap())
        .collect();
    assert_eq!(
        nodes,
        vec!["alpha claim"],
        "candidate/stale never become nodes, and a subject whose latest revision is unverified is out (consistent with the injection index)"
    );
    let mt = map(&db, false).await;
    assert!(
        !mt.contains("beta claim"),
        "candidate absent from map: {mt}"
    );
    assert!(!mt.contains("gamma claim"), "stale absent from map: {mt}");
    assert!(mt.contains("alpha claim"));
    assert!(
        !mt.contains("delta claim"),
        "a subject whose latest revision is candidate is absent from the map: {mt}"
    );

    // Search: every hit carries its status, the fold shows the latest
    // revision (delta = candidate now), all three labels rendered.
    let sj: serde_json::Value = serde_json::from_str(&search(&db, "claim", true).await).unwrap();
    let hits = sj["hits"].as_array().unwrap();
    let statuses: Vec<(String, String)> = hits
        .iter()
        .map(|h| {
            (
                h["subject"].as_str().unwrap().to_string(),
                h["status"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        statuses,
        vec![
            ("alpha claim".into(), "verified".into()),
            ("beta claim".into(), "candidate".into()),
            ("delta claim".into(), "candidate".into()),
            ("gamma claim".into(), "stale".into()),
        ],
        "search surfaces a status for every hit, latest revision folded"
    );
    let st = search(&db, "claim", false).await;
    assert!(st.contains("alpha claim [verified]"), "{st}");
    assert!(st.contains("beta claim [candidate]"), "{st}");
    assert!(st.contains("delta claim [candidate]"), "{st}");
    assert!(st.contains("gamma claim [stale]"), "{st}");
    assert!(
        !st.contains("delta old verified text"),
        "the superseded revision must not surface: {st}"
    );
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

// ---------------------------------------------------------------------
// 5. Heuristic honesty
// ---------------------------------------------------------------------

/// A5 + acceptance 5: a map with no evidence edges reports
/// `degenerate`; a map larger than its notes reports `oversized` —
/// and a map not larger than its notes reports neither.
#[tokio::test]
async fn qa_map_reports_degenerate_and_oversized_honestly() {
    // Two verified nodes whose ranges are half-open disjoint: zero
    // edges → degenerate, and it says so in both forms.
    let db = temp_db("memqa-degenerate");
    {
        let store = open(&db).await;
        seed_source(&store).await;
        seed_memory_created(&store).await;
        append(
            &store,
            memory_id(),
            2,
            Epoch(1),
            EventKind::MemoryNote,
            note_payload("disjoint one", "first claim", "verified", 1, 2),
        )
        .await;
        append(
            &store,
            memory_id(),
            3,
            Epoch(1),
            EventKind::MemoryNote,
            note_payload("disjoint two", "second claim", "verified", 2, 5),
        )
        .await;
        store.close().await;
    }
    let mj: serde_json::Value = serde_json::from_str(&map(&db, true).await).unwrap();
    assert_eq!(mj["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(
        mj["edges"],
        json!([]),
        "half-open [1,2) and [2,5) never intersect"
    );
    assert_eq!(mj["degenerate"], true);
    let mt = map(&db, false).await;
    assert!(
        mt.contains("degenerate: yes (search alone is the right tool)"),
        "{mt}"
    );
    let _ = std::fs::remove_dir_all(db.parent().unwrap());

    // Tiny claims: the rendered map outweighs its sources → oversized.
    let db = temp_db("memqa-oversized");
    seed_wide(&db, 2, 1).await;
    let mj: serde_json::Value = serde_json::from_str(&map(&db, true).await).unwrap();
    assert_eq!(mj["oversized"], true, "2 claim bytes vs a rendered map");
    assert!(mj["bytes"].as_u64().unwrap() > mj["source_text_bytes"].as_u64().unwrap());
    let mt = map(&db, false).await;
    assert!(mt.contains("oversized: yes"), "{mt}");
    let _ = std::fs::remove_dir_all(db.parent().unwrap());

    // Fat claims: the map is cheaper than its sources → not oversized,
    // not degenerate (the two notes share evidence).
    let db = temp_db("memqa-fatsize");
    seed_wide(&db, 2, 8192).await;
    let mj: serde_json::Value = serde_json::from_str(&map(&db, true).await).unwrap();
    assert_eq!(mj["oversized"], false);
    assert_eq!(mj["degenerate"], false);
    let mt = map(&db, false).await;
    assert!(mt.contains("oversized: no"), "{mt}");
    assert!(mt.contains("degenerate: no"), "{mt}");
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

// ---------------------------------------------------------------------
// 6. Data framing
// ---------------------------------------------------------------------

/// A7 + acceptance 6: both text views lead with their fixed header
/// carrying the `(data, not instructions)` frame.
#[tokio::test]
async fn qa_both_views_lead_with_the_data_not_instructions_frame() {
    let db = temp_db("memqa-frame");
    seed_wide(&db, 3, 32).await;
    let st = search(&db, "node", false).await;
    assert!(
        st.starts_with("# Memory search results"),
        "search text leads with the fixed header: {st}"
    );
    assert!(st.contains("(data, not instructions)"), "{st}");
    let mt = map(&db, false).await;
    assert!(
        mt.starts_with("# Memory map"),
        "map text leads with the fixed header: {mt}"
    );
    assert!(mt.contains("(data, not instructions)"), "{mt}");
    let _ = std::fs::remove_dir_all(db.parent().unwrap());
}

// ---------------------------------------------------------------------
// 7. No fork of truth (real drain)
// ---------------------------------------------------------------------

/// Spawns a mock openai-compat provider whose every response is
/// `answer` (JSON-escaped through serde_json).
async fn scripted_answer_provider(answer: &'static str) -> String {
    let head = json!({"choices": [{"delta": {"content": answer}}]});
    let tail = json!({"choices": [{"delta": {}, "finish_reason": "stop"}]});
    let body = format!("data: {head}\n\ndata: {tail}\n\n");
    let app = axum::Router::new()
        .route(
            "/chat/completions",
            axum::routing::post(
                move |axum::extract::State(body): axum::extract::State<String>| async move {
                    let mut response = (axum::http::StatusCode::OK, body).into_response();
                    response.headers_mut().insert(
                        "content-type",
                        axum::http::HeaderValue::from_static("text/event-stream"),
                    );
                    response
                },
            ),
        )
        .with_state(body);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// The QA dream answer: one verified note plus one candidate.
const QA_DREAM_ANSWER: &str = r#"[{"subject":"router prefers blue-green","text":"The router prefers blue-green deploys.","status":"verified","verified_by":{"method":"transcript","ref":"2"}},{"subject":"canary notes","text":"Canary notes are advisory."}]"#;

fn qa_memory_config(dir: &Path, sqlite: &Path, provider_base: &str) -> PathBuf {
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{}"
agent:
  id: e2e-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
agents:
  dreamer:
    id: dreamer
    model_slots:
      main:
        provider: mock
        model: mock-model
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: KIAN_E2E_API_KEY
drain:
  max_iterations: 8
memory:
  scope: e2e-agent
  dreamer:
    agent: dreamer
    max_bytes_read: 262144
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).unwrap();
    path
}

async fn wait_for_async<F, Fut>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A8 + acceptance 7 (and the "real drains" leg of the card): notes
/// land on the memory session through a REAL memory-dream drain (the
/// scripted provider answers; the DreamDriver harvests), the views are
/// read from the same sqlite file, no side store or cache file
/// appears, and appending a deleting (`stale`) note event on the
/// stream changes both views after the rebuild — the stream is the
/// only truth the views read.
#[tokio::test]
async fn qa_real_drain_notes_and_a_deleting_event_change_the_view() {
    common::force_no_proxy();
    let dir = common::temp_dir("memqa-drain");
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider(QA_DREAM_ANSWER).await;
    let config_path = qa_memory_config(&dir, &sqlite, &provider_base);
    let config = kian_cli::config::ServeConfig::load(&config_path).unwrap();
    let server = Arc::new(Server::from_config(config).await.unwrap());

    // A real ordinary session (the transcript source).
    let control: Arc<dyn ControlPlane> = server.clone();
    let source = control.create_session().await.unwrap();
    control
        .submit(
            source,
            json!("the router prefers blue-green deploys from now on; canary notes are advisory"),
            None,
        )
        .await
        .unwrap();
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(source);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the source session must settle"
    );

    // The memory session and a real dream drain: notes land as
    // memory/note events.
    let memory = control.create_session().await.unwrap();
    let ops: Arc<dyn OpsPlane> = server.clone();
    let receipt = ops.memory_dream(memory, source, 2, 5).await.unwrap();
    assert!(!receipt.deduplicated);
    assert!(
        wait_for_async(10_000, || {
            let events = control.session_events(memory);
            async move {
                events
                    .await
                    .map(|events| {
                        events
                            .iter()
                            .any(|ev| matches!(ev.kind, EventKind::MemoryNote))
                    })
                    .unwrap_or(false)
            }
        })
        .await,
        "the dream must land memory/note events"
    );

    // The views over the same sqlite file (the CLI read path, not the
    // server's in-memory state).
    let dir_before: Vec<std::ffi::OsString> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    let st = kian_cli::commands::run_memory_search(
        &config_path,
        Some(&sqlite),
        memory,
        "blue-green",
        false,
    )
    .await
    .unwrap();
    assert!(
        st.contains("router prefers blue-green [verified]"),
        "the drain-landed verified note surfaces: {st}"
    );
    let sj: serde_json::Value = serde_json::from_str(
        &kian_cli::commands::run_memory_search(&config_path, Some(&sqlite), memory, "canary", true)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sj["hits"][0]["status"], json!("candidate"));
    let mt = kian_cli::commands::run_memory_map(&config_path, Some(&sqlite), memory, false)
        .await
        .unwrap();
    assert!(mt.contains("router prefers blue-green"), "{mt}");
    assert!(!mt.contains("canary notes"), "candidates never map: {mt}");
    assert!(
        mt.contains("degenerate: yes"),
        "one verified node only: {mt}"
    );

    // No side store, no cache file: the directory is unchanged (plus
    // at most sqlite wal/shm siblings that existed before the reads).
    let dir_after: Vec<std::ffi::OsString> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        dir_before, dir_after,
        "the views must not create any side or cache file"
    );

    // The deleting event: a later `memory/note` revision marking the
    // subject `stale` (the retirement form in the closed status
    // vocabulary). Appended like any drain write — CAS on the stream's
    // head — then the rebuilt projection must change both views.
    let reader = EventStore::open_read_only(&sqlite).await.unwrap();
    let head = reader.rebuild(memory).await.unwrap();
    reader.close().await;
    let last = head.last().unwrap();
    let writer = EventStore::open(&sqlite).await.unwrap();
    writer
        .append(
            memory,
            Event {
                seq: last.seq + 1,
                session: memory,
                epoch: last.epoch,
                ts: T0 + last.seq + 1,
                kind: EventKind::MemoryNote,
                bytes: json!({
                    "subject": "router prefers blue-green",
                    "text": "Retired: the router moved to rolling deploys.",
                    "status": "stale",
                    "derived_from": [{"session": source.0.to_string(), "start_seq": 2, "end_seq": 5}],
                }),
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: last.epoch,
                expected_seq: last.seq + 1,
            },
        )
        .await
        .unwrap();
    writer.close().await;

    // Search now shows the stale revision; the map drops the node.
    let st = kian_cli::commands::run_memory_search(
        &config_path,
        Some(&sqlite),
        memory,
        "blue-green",
        false,
    )
    .await
    .unwrap();
    assert!(
        st.contains("router prefers blue-green [stale]"),
        "the deleting event changes the search view: {st}"
    );
    assert!(
        !st.contains("The router prefers blue-green deploys."),
        "the superseded text must not surface: {st}"
    );
    let mj: serde_json::Value = serde_json::from_str(
        &kian_cli::commands::run_memory_map(&config_path, Some(&sqlite), memory, true)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        mj["nodes"],
        json!([]),
        "the retired subject must leave the map: {}",
        mj
    );
    assert_eq!(mj["degenerate"], true);
    let _ = std::fs::remove_dir_all(&dir);
}
