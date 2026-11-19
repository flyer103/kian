//! kian-101 acceptance: the creation-time memory injection (P2 plan
//! §2.2 / MEMORY-MODEL §6), in-process against a real sqlite store and
//! the scripted mock provider (no network beyond localhost; `NO_PROXY`
//! forced).
//!
//! Covers the card's acceptance list:
//! - creation admits exactly ONE quiet row before any submit, and no
//!   wake fires on creation (idle stays idle),
//! - the first submit drains `[system memory, user task]` in that
//!   order (the injected index precedes the task),
//! - injection-off configs (no `memory.session`) admit no row — the
//!   stream is byte-identical to a memory-off server,
//! - verified-only notes are injected (a candidate never enters),
//! - the caps are honoured (over-cap renders the truncated index),
//! - every NEW session gets its own fresh row while an existing
//!   session never re-injects.

mod common;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;

use kian_cli::api::ControlPlane;
use kian_cli::ops::OpsPlane;
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_core::model::SessionId;

/// Spawns a mock openai-compat provider whose every response is
/// `answer` (JSON-escaped properly — the dream answers are JSON
/// arrays full of quotes).
async fn scripted_answer_provider(answer: &'static str) -> String {
    let frame = serde_json::json!({
        "choices": [{"delta": {"content": answer}}]
    });
    let tail = serde_json::json!({
        "choices": [{"delta": {}, "finish_reason": "stop"}]
    });
    let body = format!("data: {frame}\n\ndata: {tail}\n\n");
    let app = Router::new()
        .route(
            "/chat/completions",
            post(move |State(body): State<String>| async move {
                let mut response = (StatusCode::OK, body).into_response();
                response.headers_mut().insert(
                    "content-type",
                    axum::http::HeaderValue::from_static("text/event-stream"),
                );
                response
            }),
        )
        .with_state(body);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serve");
    });
    format!("http://{addr}")
}

/// Polls `cond` every 25 ms until it holds or `timeout_ms` elapses.
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

/// Writes a kian.yaml with the memory section; `session`/`inject` are
/// spliced in as given (`""` = absent — injection off).
fn inject_config(
    dir: &std::path::Path,
    sqlite: &std::path::Path,
    provider_base: &str,
    session: &str,
    inject: &str,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite}"
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
  scope: e2e-agent{session}
  dreamer:
    agent: dreamer
    max_bytes_read: 262144{inject}
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

/// The dream answer with ONE verified note (in-range transcript ref)
/// and one candidate — only the verified note may ever be injected.
const ONE_VERIFIED_ANSWER: &str = r#"[{"subject":"deploys via dsh bridge","text":"Deploys go through the dsh bridge.","status":"verified","verified_by":{"method":"transcript","ref":"2"}},{"subject":"incident process","text":"Incidents are filed as INC-NN."}]"#;

/// The dream answer with TWO verified notes (for the cap test).
const TWO_VERIFIED_ANSWER: &str = r#"[{"subject":"deploys via dsh bridge","text":"Deploys go through the dsh bridge.","status":"verified","verified_by":{"method":"transcript","ref":"2"}},{"subject":"incident process","text":"Incidents are filed as INC-NN.","status":"verified","verified_by":{"method":"transcript","ref":"3"}}]"#;

/// Drives one ordinary session to a completed settle and returns its id.
async fn settled_session(control: &Arc<dyn ControlPlane>, content: &str) -> SessionId {
    let session = control.create_session().await.expect("session");
    control
        .submit(session, serde_json::json!(content), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    session
}

/// The phase-A fixture: a source transcript session and a memory
/// session whose stream carries the dream's notes (one dream over the
/// source's first turn, `answer` scripting the dreamer). Returns the
/// memory session id — the injection source for phase B.
async fn dream_fixture(tag: &str, answer: &'static str) -> (std::path::PathBuf, SessionId) {
    let dir = common::temp_dir(tag);
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider(answer).await;
    // Phase A: injection off (no `memory.session` yet).
    let config_path = inject_config(&dir, &sqlite, &provider_base, "", "");
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config");
    let server = Arc::new(Server::from_config(config).await.expect("server"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let ops: Arc<dyn OpsPlane> = server.clone();

    let source = settled_session(
        &control,
        "the deploy pipeline broke on staging; deploys go through the dsh bridge from now on",
    )
    .await;
    let memory = control.create_session().await.expect("memory session");
    ops.memory_dream(memory, source, 2, 5).await.expect("dream");
    assert!(
        wait_for_async(10_000, || {
            let events = control.session_events(memory);
            async {
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
    (dir, memory)
}

/// Mounts the server's public router and returns the base url.
async fn mount(server: &Arc<Server>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind rpc");
    let addr = listener.local_addr().expect("rpc addr");
    let app = server.router();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("rpc serve");
    });
    format!("http://{addr}")
}

/// `GET /metrics` off the server's own router.
async fn metrics(base: &str) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("get /metrics")
        .json()
        .await
        .expect("metrics json")
}

/// Creation injects exactly one quiet row before any submit; the first
/// submit drains `[system memory, user task]`; a candidate note never
/// enters; every new session gets its own row while an existing
/// session never re-injects.
#[tokio::test]
async fn creation_injects_one_quiet_row_then_first_submit_drains_memory_then_task() {
    common::force_no_proxy();
    let (dir, memory) = dream_fixture("inject-quiet", ONE_VERIFIED_ANSWER).await;
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider("plain answer").await;
    // Phase B: the same store, injection configured (default caps).
    let config_path = inject_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("\n  session: {}", memory.0),
        "",
    );
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config B");
    let server = Arc::new(Server::from_config(config).await.expect("server B"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let base = mount(&server).await;

    // (a) creation admits exactly ONE quiet row and wakes nothing: the
    // session is still Idle and its stream carries no admitted event
    // yet (the row lives in the inbox until a submit drains it).
    let session = control.create_session().await.expect("session");
    let report = metrics(&base).await;
    assert_eq!(report["memory"]["injected_sessions"], 1, "{report}");
    assert_eq!(report["memory"]["injected_skips"], 0, "{report}");
    assert!(
        matches!(
            control.projection(session).await,
            Ok(kian_core::state::SessionState::Idle)
        ),
        "no wake on creation — idle stays idle"
    );
    let events = control.session_events(session).await.expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted))
            .count(),
        0,
        "the quiet row is durable inbox state, not an event"
    );

    // (b) the first submit drains `[system memory, user task]` in that
    // order (admission order), and only the VERIFIED note entered.
    control
        .submit(session, serde_json::json!("the task"), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    let events = control.session_events(session).await.expect("events");
    let admitted: Vec<&kian_core::events::Event> = events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted))
        .collect();
    assert_eq!(admitted.len(), 2, "exactly [memory, task]: {events:?}");

    let injected = admitted[0];
    assert_eq!(injected.bytes["role"], "system", "{}", injected.bytes);
    assert_eq!(injected.bytes["synthetic"], true);
    let content = injected.bytes["content"].as_str().expect("content");
    assert!(
        content.starts_with(kian_memory::INDEX_HEADER.lines().next().unwrap_or_default()),
        "the frozen header leads the injected text: {content}"
    );
    assert!(content.contains("Deploys go through the dsh bridge."));
    assert!(
        !content.contains("INC-NN"),
        "a candidate note never enters a session: {content}"
    );
    // The marker (I1): the derived event id of the verified note, the
    // renderer's own provenance, and the value-free redaction report.
    let marker = &injected.bytes["memory"];
    let note_seq = {
        let mem_events = control.session_events(memory).await.expect("memory events");
        mem_events
            .iter()
            .find(|ev| {
                matches!(ev.kind, EventKind::MemoryNote)
                    && ev.bytes["subject"] == "deploys via dsh bridge"
            })
            .expect("the verified note")
            .seq
    };
    assert_eq!(marker["scope"], "e2e-agent");
    assert_eq!(
        marker["notes"],
        serde_json::json!([kian_core::events::event_id(&memory, note_seq)]),
        "exactly the verified note's derived event id"
    );
    assert_eq!(marker["items"], 1);
    assert_eq!(marker["truncated"], false);
    assert_eq!(marker["source_seq"], note_seq);
    assert!(marker["bytes"].as_u64().unwrap_or(0) > 0);
    assert!(marker["redactions"].as_array().is_some());

    assert_eq!(
        admitted[1].bytes,
        serde_json::json!({"role": "user", "content": "the task"}),
        "the user task follows the memory index"
    );

    // (c) a NEW session gets its own fresh row; the existing session
    // never re-injects (still exactly one memory-marked admitted).
    let second = control.create_session().await.expect("second session");
    let report = metrics(&base).await;
    assert_eq!(report["memory"]["injected_sessions"], 2, "{report}");
    let events = control.session_events(session).await.expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted)
                && ev.bytes.get("memory").is_some())
            .count(),
        1,
        "an existing session never re-injects"
    );
    let second_events = control.session_events(second).await.expect("second events");
    assert_eq!(second_events.len(), 1, "the fresh session is untouched");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Injection off (`memory.session` absent, memory section present):
/// no row, no `memory` marker anywhere, byte-identical stream — and no
/// `injected_*` metrics keys at all.
#[tokio::test]
async fn injection_off_admits_no_row_and_is_byte_identical() {
    common::force_no_proxy();
    let dir = common::temp_dir("inject-off");
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider("plain answer").await;
    // Memory section present, `session` absent: injection off.
    let config_path = inject_config(&dir, &sqlite, &provider_base, "", "");
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config");
    let server = Arc::new(Server::from_config(config).await.expect("server"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let base = mount(&server).await;

    let session = control.create_session().await.expect("session");
    let report = metrics(&base).await;
    assert!(
        report["memory"].get("injected_sessions").is_none(),
        "no injection counters when injection is off: {report}"
    );
    control
        .submit(session, serde_json::json!("the task"), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    let events = control.session_events(session).await.expect("events");
    let admitted: Vec<&kian_core::events::Event> = events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted))
        .collect();
    assert_eq!(admitted.len(), 1, "one ordinary input, nothing else");
    assert_eq!(
        admitted[0].bytes,
        serde_json::json!({"role": "user", "content": "the task"}),
        "byte-identical to a memory-off session"
    );
    assert!(
        !events.iter().any(|ev| ev.bytes.get("memory").is_some()),
        "no memory marker anywhere in the stream"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The caps are honoured: `max_items: 1` over two verified notes
/// renders the truncated index form (excerpts only) and counts the
/// truncation honestly.
#[tokio::test]
async fn caps_render_the_truncated_index_and_count_it() {
    common::force_no_proxy();
    let (dir, memory) = dream_fixture("inject-caps", TWO_VERIFIED_ANSWER).await;
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider("plain answer").await;
    let config_path = inject_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("\n  session: {}", memory.0),
        "\n  inject:\n    max_items: 1",
    );
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config B");
    let server = Arc::new(Server::from_config(config).await.expect("server B"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let base = mount(&server).await;

    let session = control.create_session().await.expect("session");
    let report = metrics(&base).await;
    assert_eq!(report["memory"]["injected_sessions"], 1, "{report}");
    assert_eq!(report["memory"]["injected_truncations"], 1, "{report}");

    control
        .submit(session, serde_json::json!("the task"), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    let events = control.session_events(session).await.expect("events");
    let injected = events
        .iter()
        .find(|ev| matches!(ev.kind, EventKind::InputAdmitted) && ev.bytes.get("memory").is_some())
        .expect("the injected row");
    let content = injected.bytes["content"].as_str().expect("content");
    assert!(
        content.contains("(index truncated: 1 of 2 entries, excerpts only)"),
        "the index-form footer: {content}"
    );
    assert!(
        !content.contains("## deploys via dsh bridge"),
        "over cap the full-form entry never renders, only the index line"
    );
    assert!(
        content.contains("- deploys via dsh bridge: Deploys go through the dsh bridge."),
        "the index-form entry is subject + one-line excerpt: {content}"
    );
    assert_eq!(injected.bytes["memory"]["items"], 1);
    assert_eq!(injected.bytes["memory"]["truncated"], true);
    // Both eligible notes stay pinned in the snapshot (source_seq
    // includes cap-dropped entries; the notes list names them).
    assert_eq!(
        injected.bytes["memory"]["notes"].as_array().map(Vec::len),
        Some(2),
        "the marker names the full eligible snapshot"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// I8 (honest degradation): a configured source that does not exist
/// (or is unreadable) injects nothing, counts a skip, and creation
/// still succeeds with a byte-identical session.
#[tokio::test]
async fn unknown_source_skips_honestly_and_never_fails_creation() {
    common::force_no_proxy();
    let dir = common::temp_dir("inject-ghost");
    let sqlite = dir.join("kian.sqlite");
    let provider_base = scripted_answer_provider("plain answer").await;
    let ghost = SessionId::new();
    let config_path = inject_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("\n  session: {}", ghost.0),
        "",
    );
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config");
    let server = Arc::new(Server::from_config(config).await.expect("server"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let base = mount(&server).await;

    // Creation succeeds — never failed by the injection path.
    let session = control
        .create_session()
        .await
        .expect("creation must succeed despite the ghost source");
    let report = metrics(&base).await;
    assert_eq!(report["memory"]["injected_sessions"], 0, "{report}");
    assert_eq!(report["memory"]["injected_skips"], 1, "{report}");

    // The session is ordinary and byte-identical: it settles, one
    // admitted row, no memory marker.
    control
        .submit(session, serde_json::json!("the task"), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    let events = control.session_events(session).await.expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::InputAdmitted))
            .count(),
        1
    );
    assert!(!events.iter().any(|ev| ev.bytes.get("memory").is_some()));

    let _ = std::fs::remove_dir_all(&dir);
}

// -- memory P3b-1 (kian-117): the injection read is narrow --------------

use kian_core::events::{Event, SCHEMA_VERSION};
use kian_core::model::Epoch;
use kian_core::secrets::SecretRedactor;
use kian_store::EventStore;
use serde_json::json;

async fn direct_append(
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
        ts: 1_761_900_000_000 + seq,
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

/// P3b-1 (kian-117): the creation-time injection reads the source
/// memory session through the narrow `scan_kind` path. On a source
/// stream interleaving notes with non-note events, the admitted
/// `{content, memory}` payload must be byte-identical to the one the
/// PRE-CHANGE read (full rebuild → fold → render → envelope) produces
/// — including the `source_seq` pin — and the injection read must
/// leave the source stream untouched (the A5 read-only witness).
#[tokio::test]
async fn injection_payload_byte_identical_on_a_mixed_source_stream() {
    common::force_no_proxy();
    let dir = common::temp_dir("inject-narrow");
    let sqlite = dir.join("kian.sqlite");

    // Seed the memory session directly (injection only reads the
    // stream — no dream needed): two verified notes with shared
    // evidence, one candidate, interleaved with three non-note events.
    let memory = SessionId::new();
    let source = SessionId::new();
    let note = |subject: &str, status: &str| {
        json!({
            "subject": subject,
            "text": format!("{subject} claim text."),
            "status": status,
            "verified_by": {"method": "transcript", "ref": "3"},
            "derived_from": [{"session": source.0, "start_seq": 1, "end_seq": 5}],
        })
    };
    let store = EventStore::open(&sqlite).await.unwrap();
    direct_append(
        &store,
        memory,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    direct_append(
        &store,
        memory,
        2,
        Epoch(1),
        EventKind::MemoryNote,
        note("alpha subject", "verified"),
    )
    .await;
    direct_append(
        &store,
        memory,
        3,
        Epoch(1),
        EventKind::DrainClaimed,
        json!({}),
    )
    .await;
    direct_append(
        &store,
        memory,
        4,
        Epoch(1),
        EventKind::MemoryNote,
        note("beta subject", "verified"),
    )
    .await;
    direct_append(
        &store,
        memory,
        5,
        Epoch(1),
        EventKind::TurnStart,
        json!({"iteration": 0}),
    )
    .await;
    direct_append(
        &store,
        memory,
        6,
        Epoch(1),
        EventKind::MemoryNote,
        note("gamma candidate", "candidate"),
    )
    .await;
    direct_append(
        &store,
        memory,
        7,
        Epoch(1),
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {"text": "tail"}}),
    )
    .await;
    let events_before = store.rebuild(memory).await.unwrap().len();
    store.close().await;

    // The expected payload through the PRE-CHANGE read path: full
    // rebuild, fold, render with the server's default caps and
    // pattern-scanning redactor, then the exact-two-key envelope.
    let store = EventStore::open(&sqlite).await.unwrap();
    let full = store.rebuild(memory).await.unwrap();
    let notes = kian_core::memory::project_notes(&full);
    let eligible = kian_memory::eligible_notes(&notes);
    let rendered = kian_memory::render_memory_index_with(
        &notes,
        &kian_memory::MemoryIndexConfig::default(),
        &SecretRedactor::from_env_names(&[]),
    )
    .expect("the fixture has verified notes — it renders");
    let expected =
        kian_cli::memory::injection_admit_payload("e2e-agent", memory, &rendered, &eligible);
    store.close().await;
    // The I7 pin is unchanged: the greatest seq among the eligible
    // notes (4 — beta), never the stream head (8 after the settle).
    assert_eq!(rendered.source_seq, 4);

    // The server over the same store, injection on (default caps).
    let provider_base = scripted_answer_provider("plain answer").await;
    let config_path = inject_config(
        &dir,
        &sqlite,
        &provider_base,
        &format!("\n  session: {}", memory.0),
        "",
    );
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config");
    let server = Arc::new(Server::from_config(config).await.expect("server"));
    let control: Arc<dyn ControlPlane> = server.clone();
    let base = mount(&server).await;

    let session = control.create_session().await.expect("session");
    let report = metrics(&base).await;
    assert_eq!(report["memory"]["injected_sessions"], 1, "{report}");
    assert_eq!(report["memory"]["injected_skips"], 0, "{report}");

    // A5: the injection read appended nothing to the source stream.
    let store = EventStore::open(&sqlite).await.unwrap();
    assert_eq!(
        store.rebuild(memory).await.unwrap().len(),
        events_before,
        "the narrow read leaves the source stream untouched"
    );
    store.close().await;

    // Drain and compare the admitted payload byte for byte against the
    // pre-change expectation.
    control
        .submit(session, serde_json::json!("the task"), None)
        .await
        .expect("submit");
    assert!(
        wait_for_async(10_000, || {
            let result = control.projection(session);
            async move { matches!(result.await, Ok(kian_core::state::SessionState::Completed)) }
        })
        .await,
        "the session must settle"
    );
    let events = control.session_events(session).await.expect("events");
    let injected = events
        .iter()
        .find(|ev| matches!(ev.kind, EventKind::InputAdmitted) && ev.bytes.get("memory").is_some())
        .expect("the injected row");
    assert_eq!(
        injected.bytes["content"], expected["content"],
        "the injected text is byte-identical to the pre-change fold"
    );
    assert_eq!(
        injected.bytes["memory"], expected["memory"],
        "the memory marker (scope, notes, items, bytes, truncated, source_seq, redactions) is byte-identical"
    );
    assert_eq!(injected.bytes["memory"]["source_seq"], json!(4));

    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-038: the resident Router is created by the first *admit*, not by
/// the control-plane `create_session`, so before this change it never
/// received the memory index. The session bootstrap now runs in
/// `start_services` before the judge prompt is admitted: the index is
/// the Router's first `input/admitted`, the bootstrap prompt the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_resident_router_receives_the_index_before_its_bootstrap_prompt() {
    common::force_no_proxy();
    let dir = common::temp_dir("boot-inject");
    let sqlite = dir.join("kian.sqlite");
    let memory_source = SessionId::new();

    let store = EventStore::open(&sqlite).await.expect("store opens");
    direct_append(
        &store,
        memory_source,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        serde_json::json!({}),
    )
    .await;
    direct_append(
        &store,
        memory_source,
        2,
        Epoch(1),
        EventKind::MemoryNote,
        serde_json::json!({
            "subject": "deploy path",
            "text": "Deploys go through the dsh bridge.",
            "status": "verified",
            "verified_by": {"method": "transcript", "ref": "2"},
            "derived_from": [{"session": memory_source.0, "start_seq": 1, "end_seq": 2}],
        }),
    )
    .await;
    store.close().await;

    let provider_base = scripted_answer_provider("ok").await;
    let router = "00000000-0000-4000-8000-00000000c0aa";
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
plugins: ["openai-compat", "local-tools", "superagent"]
storage:
  sqlite: "{sqlite}"
agent:
  id: e2e-agent
  model_slots:
    main: {{ provider: mock, model: mock-model }}
  tools: ["fs_read"]
agents:
  dreamer:
    id: dreamer
    model_slots:
      main: {{ provider: mock, model: mock-model }}
  router:
    id: router
    version: 1
    model_slots:
      router: {{ provider: mock, model: mock-model }}
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker-agent:
    id: worker-agent
    version: 1
    model_slots:
      main: {{ provider: mock, model: mock-model }}
    tools: ["exec"]
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
  session: "{memory_source}"
superagent:
  router:
    session_id: "{router}"
    agent: router
    slot: router
  worker:
    agent: worker-agent
  channels:
    mock:
      type: mock-im
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        provider_base = provider_base,
        memory_source = memory_source.0,
        router = router,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    let config = kian_cli::config::ServeConfig::load(&path).expect("config loads");
    let _server = Server::from_config(config).await.expect("server boots");

    let store = EventStore::open_read_only(&sqlite).await.expect("reader");
    let router_sid = SessionId(router.parse().expect("router uuid"));
    let reader = store.clone();
    assert!(
        wait_for_async(15_000, || {
            let reader = reader.clone();
            async move {
                reader
                    .rebuild(router_sid)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|ev| ev.kind == EventKind::InputAdmitted)
                    .count()
                    >= 2
            }
        })
        .await,
        "the resident Router never bootstrapped (index + judge prompt)"
    );
    let events = store.rebuild(router_sid).await.expect("router stream");
    let inputs: Vec<_> = events
        .iter()
        .filter(|ev| ev.kind == EventKind::InputAdmitted)
        .collect();
    assert!(inputs.len() >= 2, "{:?}", inputs.len());
    assert!(
        inputs[0].bytes.get("memory").is_some(),
        "the Router's first input must be the memory index: {}",
        inputs[0].bytes
    );
    assert!(
        inputs[1].bytes.get("memory").is_none(),
        "the second input is the bootstrap prompt: {}",
        inputs[1].bytes
    );
    let text = inputs[0]
        .bytes
        .get("content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        text.starts_with(kian_memory::INDEX_HEADER.lines().next().unwrap_or_default()),
        "the first input is the frozen index header: {text:?}"
    );
}
