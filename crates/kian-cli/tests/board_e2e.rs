//! E2E acceptance for the task board's honest derivation (R17 C4,
//! MULTIAGENT-MODEL §4.2/§4.3/§4.4, pins P3-3/P3-4/P3-5): one row per
//! delegation task, derived from the committed wrapper identity + the
//! child's own stream + the parent's durable report row — never from
//! prose, never a fabricated evidence link, `empty_output` never a
//! success.

use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::SpawnIdentity;
use serde_json::{Value, json};

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "kian-board-e2e-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ))
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

async fn push(
    store: &EventStore,
    session: SessionId,
    seq: u64,
    epoch: Epoch,
    kind: EventKind,
    bytes: Value,
) {
    store
        .append(
            session,
            Event {
                seq,
                session,
                epoch,
                ts: 1_763_000_000_000 + seq,
                kind,
                bytes,
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

/// One delegation lineage: the derived identity (pin P1/P7 — task_id
/// IS the derived thread id), the derived child session, the wrapper
/// (the class-B bytes `WorkerSpawner::prepare` records: the frozen
/// `thread_id`/`task` pair plus the four identity keys) and the
/// `thread/started` payload both sides carry byte-identically.
struct Lineage {
    identity: SpawnIdentity,
    child: SessionId,
    started: Value,
}

fn lineage(parent: SessionId, spawn_seq: u64, task_text: &str) -> Lineage {
    let identity = SpawnIdentity::derive(parent, spawn_seq, "worker", 7);
    let child = kian_superagent::child_session_id(identity.task_id);
    let wrapper = json!({
        "thread_id": identity.task_id_str(),
        "task": task_text,
        "spawn_uid": identity.spawn_uid.to_string(),
        "task_id": identity.task_id_str(),
        "agent_def": "worker",
        "agent_def_version": 7,
    });
    let started = json!({
        "thread_id": identity.task_id_str(),
        "parent": parent.0.to_string(),
        "worker": child.0.to_string(),
        "pinned_version": 7,
        "task": {"role": "user", "content": wrapper},
    });
    Lineage {
        identity,
        child,
        started,
    }
}

/// Seeds the parent's dispatch side (created once, then one
/// `thread/started` per spawn carrying that spawn's task bytes).
async fn seed_parent(store: &EventStore, parent: SessionId, lin: &Lineage) {
    let head = store.head(parent).await.unwrap().next_seq;
    if head == 0 {
        push(
            store,
            parent,
            1,
            Epoch::GENESIS,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
    }
    let seq = store.head(parent).await.unwrap().next_seq;
    push(
        store,
        parent,
        seq,
        Epoch(1),
        EventKind::ThreadStarted,
        lin.started.clone(),
    )
    .await;
}

/// Seeds the child's own side (created + self-naming thread/started +
/// the wrapper admission).
async fn seed_child_start(store: &EventStore, lin: &Lineage) {
    push(
        store,
        lin.child,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        store,
        lin.child,
        2,
        Epoch(1),
        EventKind::ThreadStarted,
        lin.started.clone(),
    )
    .await;
    push(
        store,
        lin.child,
        3,
        Epoch(1),
        EventKind::InputAdmitted,
        json!({"role": "user", "content": lin.started["task"]["content"].clone()}),
    )
    .await;
}

async fn next_seq(store: &EventStore, session: SessionId) -> u64 {
    store.head(session).await.unwrap().next_seq
}

fn row_for<'a>(
    rows: &'a [kian_cli::commands::TaskBoardRow],
    task_id: &str,
) -> &'a kian_cli::commands::TaskBoardRow {
    rows.iter()
        .find(|r| r.task_id == task_id)
        .unwrap_or_else(|| panic!("no row for task {task_id}: {rows:?}"))
}

/// 🔴 §4.3:497-505 (pin P3-4): a task whose child committed a PASSED
/// worker-gate verdict shows `gate:"pass"` + the derived evidence
/// pointer `{gate, passed, seq}` + `evidence_provenance:"derived"` —
/// and the row NEVER carries a fabricated `evidence_ref` key (the
/// durable per-task link is P4's, §7.2).
#[tokio::test]
async fn the_board_labels_completion_verification_as_derived_not_recorded() {
    let path = temp_db("derived");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    let lin = lineage(parent, 10, "verified task");
    seed_parent(&store, parent, &lin).await;
    seed_child_start(&store, &lin).await;
    push(
        &store,
        lin.child,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer"}),
    )
    .await;
    push(
        &store,
        lin.child,
        5,
        Epoch(1),
        EventKind::EvidenceVerdict,
        json!({"gate": "worker", "passed": true, "checks": [{"name": "final_message_present", "passed": true}]}),
    )
    .await;
    push(
        &store,
        lin.child,
        6,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": lin.identity.task_id_str(), "status": "completed", "summary": {"role": "assistant", "content": "done"}}),
    )
    .await;

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    let row = row_for(&rows, &lin.identity.task_id_str());
    assert_eq!(row.outcome, "completed_verified");
    assert_eq!(row.thread_status, "completed");
    assert_eq!(row.gate, "pass");
    assert_eq!(
        row.evidence.as_ref().unwrap(),
        &kian_cli::commands::TaskEvidence {
            gate: "worker".to_string(),
            passed: true,
            seq: 5,
        }
    );
    assert_eq!(row.evidence_provenance, "derived");
    // The fabricated durable link must not exist as a key at all.
    let serialized = serde_json::to_value(row).unwrap();
    assert!(
        serialized.get("evidence_ref").is_none(),
        "P3 must never fabricate evidence_ref: {serialized}"
    );
    // The full P3-3 key set, exactly (serde_json's Value map sorts
    // keys; the set is the assertion, C5's golden fixes the order).
    let mut keys: Vec<&str> = serialized
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "assignee",
            "attempt",
            "child",
            "depends_on",
            "evidence",
            "evidence_provenance",
            "gate",
            "group",
            "outcome",
            "parent",
            "task_id",
            "thread",
            "thread_status",
        ],
        "pin P3-3's row shape, key for key"
    );
    assert_eq!(serialized["group"], Value::Null);
    assert_eq!(serialized["depends_on"], json!([]));
    assert_eq!(
        serialized["assignee"],
        Value::Null,
        "no P2 roster ⇒ no assignee"
    );
    assert_eq!(serialized["attempt"], json!(1));

    store.close().await;
    cleanup(&path);
}

/// 🔴 §4.3:504-505 + §6.3:705-708: a child whose prose claims success
/// but has NO `thread/completed` reads `outstanding` — the outcome is
/// derived from committed carriers only, never from text. The failing
/// verdict case labels `gate:"fail"` without changing the honesty.
#[tokio::test]
async fn the_board_never_infers_an_outcome_from_prose() {
    let path = temp_db("prose");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    // A child that SAYS it succeeded (final answer prose) but never
    // committed a completion.
    let lin = lineage(parent, 20, "prose task");
    seed_parent(&store, parent, &lin).await;
    seed_child_start(&store, &lin).await;
    push(
        &store,
        lin.child,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer",
               "message": {"role": "assistant", "content": "Task complete, everything succeeded!"}}),
    )
    .await;
    // A second child with a FAILED verdict: gate "fail", outcome honest.
    let lin2 = lineage(parent, 21, "failed-verdict task");
    seed_parent(&store, parent, &lin2).await;
    seed_child_start(&store, &lin2).await;
    push(
        &store,
        lin2.child,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer"}),
    )
    .await;
    push(
        &store,
        lin2.child,
        5,
        Epoch(1),
        EventKind::EvidenceVerdict,
        json!({"gate": "worker", "passed": false, "checks": [{"name": "final_message_present", "passed": false}]}),
    )
    .await;
    push(
        &store,
        lin2.child,
        6,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": lin2.identity.task_id_str(), "status": "completed", "summary": {"role": "assistant", "content": "trust me it worked"}}),
    )
    .await;

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    let prose = row_for(&rows, &lin.identity.task_id_str());
    assert_eq!(
        prose.outcome, "outstanding",
        "no completion ⇒ outstanding, prose never counts"
    );
    assert_eq!(prose.thread_status, "outstanding");
    assert_eq!(prose.gate, "absent");
    assert!(prose.evidence.is_none());

    let failed = row_for(&rows, &lin2.identity.task_id_str());
    assert_eq!(
        failed.outcome, "completed_unverified",
        "a failed verdict is not verification"
    );
    assert_eq!(failed.gate, "fail");
    assert!(!failed.evidence.as_ref().unwrap().passed);
    assert_eq!(failed.evidence.as_ref().unwrap().seq, 5);

    store.close().await;
    cleanup(&path);
}

/// §4.4:513-516 (M6): an empty final answer is `empty_output` — a
/// soft failure that never reads as success on the board, at any
/// policy setting.
#[tokio::test]
async fn empty_output_never_reads_as_success_on_the_board() {
    let path = temp_db("empty");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    let lin = lineage(parent, 30, "empty task");
    seed_parent(&store, parent, &lin).await;
    seed_child_start(&store, &lin).await;
    push(
        &store,
        lin.child,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "error",
               "error": {"class": "empty_response"}}),
    )
    .await;
    push(
        &store,
        lin.child,
        5,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": lin.identity.task_id_str(), "status": "failed", "summary": {"role": "assistant", "content": ""}}),
    )
    .await;

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    let row = row_for(&rows, &lin.identity.task_id_str());
    assert_eq!(row.outcome, "empty_output");
    assert_eq!(row.thread_status, "failed");
    assert!(
        !row.outcome.starts_with("completed"),
        "empty_output is never a success spelling: {row:?}"
    );

    store.close().await;
    cleanup(&path);
}

/// Pin P3-5 (§6.2:667, §6.2.1:670-695): `input_required` is derived
/// from the parent's DURABLE report row (idempotency key
/// `spawn:<spawn_uid>`, structured `state:"waiting"`) — never from
/// text — and the row is still readable after `mark_consumed`
/// (the consumed-row case: the durable fact survives consumption).
#[tokio::test]
async fn input_required_is_derived_from_the_durable_report_row_not_from_text() {
    let path = temp_db("input-required");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();

    // A live child whose prose ASKS for input but has no report row:
    // outstanding, never input_required from text.
    let prose = lineage(parent, 40, "prose waiting");
    seed_parent(&store, parent, &prose).await;
    seed_child_start(&store, &prose).await;
    push(
        &store,
        prose.child,
        4,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer",
               "message": {"role": "assistant", "content": "I need your decision before I can continue."}}),
    )
    .await;

    // A live child WITH the durable waiting report row on the parent.
    let waiting = lineage(parent, 41, "waiting task");
    seed_parent(&store, parent, &waiting).await;
    seed_child_start(&store, &waiting).await;
    let report = json!({
        "thread_id": waiting.identity.task_id_str(),
        "status": "completed",
        "summary": {"role": "assistant", "content": "need a decision"},
        "state": "waiting",
    });
    let row_id = uuid::Uuid::new_v4();
    store
        .admit(InputRow {
            id: row_id,
            session: parent,
            payload: report,
            admitted_at: 1,
            eligible_at: 1,
            idempotency_key: Some(format!("spawn:{}", waiting.identity.spawn_uid)),
        })
        .await
        .unwrap();

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    assert_eq!(
        row_for(&rows, &prose.identity.task_id_str()).outcome,
        "outstanding",
        "prose asking for input is never input_required"
    );
    assert_eq!(
        row_for(&rows, &waiting.identity.task_id_str()).outcome,
        "input_required"
    );

    // The consumed-row case: the parent's drain consumed the report
    // row through its committed event — the durable row (and the
    // derivation) must survive.
    store
        .mark_consumed(parent, row_id, 2)
        .await
        .expect("consume the report row via the parent's seq 2");
    assert!(
        store
            .eligible_inputs(parent, 1_763_000_500_000)
            .await
            .unwrap()
            .is_empty(),
        "the row is no longer eligible"
    );
    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    assert_eq!(
        row_for(&rows, &waiting.identity.task_id_str()).outcome,
        "input_required",
        "a consumed report row still carries its waiting fact (pin P3-5)"
    );

    store.close().await;
    cleanup(&path);
}

/// The derived identity is the discovery key (§2.1.1): a parent's
/// `thread/started` and the child's own carry identical task bytes,
/// and the row carries the recorded `agent_def` facts. A legacy
/// delegation (no identity keys on the wrapper) produces NO row in
/// v0 — the board never guesses a task id.
#[tokio::test]
async fn discovery_reads_the_wrapper_identity_and_skips_legacy_delegations() {
    let path = temp_db("discovery");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    let lin = lineage(parent, 50, "task");
    seed_parent(&store, parent, &lin).await;
    seed_child_start(&store, &lin).await;

    // A legacy worker session (thread/started with a task message but
    // no identity keys): the ticket board's shape, not a task row.
    let legacy = SessionId::new();
    push(
        &store,
        legacy,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        &store,
        legacy,
        2,
        Epoch(1),
        EventKind::ThreadStarted,
        json!({
            "thread_id": uuid::Uuid::new_v4().to_string(),
            "parent": parent.0.to_string(),
            "worker": legacy.0.to_string(),
            "pinned_version": 7,
            "task": {"role": "user", "content": {"thread_id": "legacy", "task": "old-style"}},
        }),
    )
    .await;

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly the identity-carrying task: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row.task_id, lin.identity.task_id_str());
    assert_eq!(row.parent, parent.0.to_string());
    assert_eq!(row.child, lin.child.0.to_string());
    assert_eq!(row.thread, lin.identity.task_id_str());
    assert_eq!(row.attempt, 1);

    store.close().await;
    cleanup(&path);
}

/// The retry ordinal (§1:177-179): two spawns sharing a `task_id`
/// under different thread ids are ONE task whose row shows the
/// CURRENT (latest parent-side) attempt with `attempt: 2`. In this
/// tree `task_id` is per-spawn (pin P7), so the shared-id shape is
/// seeded directly through the bytes — the derivation reads what the
/// bytes carry, never a counter of its own.
#[tokio::test]
async fn attempt_counts_prior_thread_started_with_the_same_task_id() {
    let path = temp_db("attempt");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    let first = lineage(parent, 60, "the task");
    seed_parent(&store, parent, &first).await;
    seed_child_start(&store, &first).await;

    // The retry: the SAME task_id recorded under a DIFFERENT thread
    // (the P2 `task_key` shape, carried by bytes).
    let retry_uid = kian_superagent::spawn_uid(parent, 61, 0);
    let retry_thread = kian_superagent::child_thread_id(parent, retry_uid);
    let retry_child = kian_superagent::child_session_id(retry_thread);
    let retry_started = json!({
        "thread_id": retry_thread.to_string(),
        "parent": parent.0.to_string(),
        "worker": retry_child.0.to_string(),
        "pinned_version": 7,
        "task": {"role": "user", "content": {
            "thread_id": retry_thread.to_string(),
            "task": "the task, retried",
            "spawn_uid": retry_uid.to_string(),
            "task_id": first.identity.task_id_str(),
            "agent_def": "worker",
            "agent_def_version": 7,
        }},
    });
    let seq = next_seq(&store, parent).await;
    push(
        &store,
        parent,
        seq,
        Epoch(1),
        EventKind::ThreadStarted,
        retry_started.clone(),
    )
    .await;
    push(
        &store,
        retry_child,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        &store,
        retry_child,
        2,
        Epoch(1),
        EventKind::ThreadStarted,
        retry_started,
    )
    .await;

    let rows = kian_cli::commands::task_board_rows(&store).await.unwrap();
    // ONE task (one task_id) — the current attempt is the retry.
    assert_eq!(rows.len(), 1, "one row per task: {rows:?}");
    let row = &rows[0];
    assert_eq!(row.task_id, first.identity.task_id_str());
    assert_eq!(
        row.thread,
        retry_thread.to_string(),
        "the current attempt's thread"
    );
    assert_eq!(row.child, retry_child.0.to_string());
    assert_eq!(
        row.attempt, 2,
        "1 + the parent's prior thread/started with the same task_id"
    );

    store.close().await;
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// R17 C5 — the read-only operator surface (`kian board` /
// `kian tasks --all`), pins P3-1/P3-2/P3-3
// ---------------------------------------------------------------------------

/// Writes a kian.yaml enabling/disabling the two board switches (the
/// minimal valid super-agent document — the `superagent:` section
/// requires its router/worker/channels, so the board block rides the
/// full shape); returns its path (caller cleans up).
fn board_config(tag: &str, board: bool, views: bool, db: &std::path::Path) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "kian-board-cfg-{tag}-{}.yaml",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(
        &path,
        format!(
            concat!(
                "plugins: [\"openai-compat\", \"superagent\"]\n",
                "listen: \"127.0.0.1:7400\"\n",
                "storage:\n  sqlite: \"{db}\"\n",
                "agent:\n  id: main\n  model_slots:\n    main: {{ provider: local, model: m }}\n",
                "providers:\n  local: {{ base_url: \"http://127.0.0.1:9\", api_key_env: KBD_KEY }}\n",
                "agents:\n  router:\n    id: router\n    model_slots:\n      router: {{ provider: local, model: m }}\n",
                "superagent:\n",
                "  router: {{ session_id: \"00000000-0000-0000-0000-000000000001\", agent: router }}\n",
                "  worker: {{ agent: router }}\n",
                "  channels:\n    mock-im: {{ type: mock-im }}\n",
                "  board:\n    enabled: {board}\n",
                "read_model:\n  views:\n    enabled: {views}\n",
                "recovery:\n",
                "  budget: {{ tokens: 64, wall_clock_ms: 300000, retry_cost_tokens: 1 }}\n",
                "  rules:\n    - class: unknown\n      action: fail\n",
            ),
            db = db.display(),
            board = board,
            views = views,
        ),
    )
    .expect("write board config");
    path
}

/// Runs the built CLI with `args` (the ops_commands precedent).
fn run_kian(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(args)
        .output()
        .expect("run kian binary")
}

/// Seeds the parent's dispatch side in the state-machine-valid shape
/// (`session/created` → `drain/claimed` → `thread/started` — the
/// directory projection rejects `thread/started` straight after
/// creation, so the view rebuild needs the claim in between).
async fn seed_parent_full(store: &EventStore, parent: SessionId, lin: &Lineage) {
    push(
        store,
        parent,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        store,
        parent,
        2,
        Epoch(1),
        EventKind::DrainClaimed,
        json!({}),
    )
    .await;
    push(
        store,
        parent,
        3,
        Epoch(1),
        EventKind::ThreadStarted,
        lin.started.clone(),
    )
    .await;
}

/// Seeds the child's own side in the same valid shape.
async fn seed_child_full(store: &EventStore, lin: &Lineage) {
    push(
        store,
        lin.child,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        store,
        lin.child,
        2,
        Epoch(1),
        EventKind::DrainClaimed,
        json!({}),
    )
    .await;
    push(
        store,
        lin.child,
        3,
        Epoch(1),
        EventKind::ThreadStarted,
        lin.started.clone(),
    )
    .await;
    push(
        store,
        lin.child,
        4,
        Epoch(1),
        EventKind::InputAdmitted,
        json!({"role": "user", "content": lin.started["task"]["content"].clone()}),
    )
    .await;
}

/// Seeds a completed lineage (verdict pass + completion) on `parent`
/// at `spawn_seq` — the shape of C4's derived-evidence fixture, with
/// the state-machine-valid prefix (the verdict lands at child seq 6).
async fn seed_completed(
    store: &EventStore,
    parent: SessionId,
    spawn_seq: u64,
    task: &str,
) -> Lineage {
    let lin = lineage(parent, spawn_seq, task);
    seed_parent_full(store, parent, &lin).await;
    seed_child_full(store, &lin).await;
    push(
        store,
        lin.child,
        5,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer"}),
    )
    .await;
    push(
        store,
        lin.child,
        6,
        Epoch(1),
        EventKind::EvidenceVerdict,
        json!({"gate": "worker", "passed": true, "checks": [{"name": "final_message_present", "passed": true}]}),
    )
    .await;
    push(
        store,
        lin.child,
        7,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": lin.identity.task_id_str(), "status": "completed", "summary": {"role": "assistant", "content": "done"}}),
    )
    .await;
    lin
}

/// 🔴 Pin P3-1/P3-2: board off ⇒ both verbs refuse with exit 2 naming
/// `superagent.board.enabled`, print NO rows on stdout, and never
/// touch the store — proven by passing a `--db` that does not exist:
/// the refusal (not the missing-file error) answers, and the path is
/// never created.
#[test]
fn kian_board_and_kian_tasks_all_refuse_when_board_is_disabled() {
    let missing = std::env::temp_dir().join(format!(
        "kian-board-missing-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let cfg = board_config("off", false, true, &missing);
    let cfg_str = cfg.to_str().unwrap();
    let db_str = missing.to_str().unwrap();
    for args in [
        vec!["board", "--config", cfg_str, "--db", db_str],
        vec!["tasks", "--all", "--config", cfg_str, "--db", db_str],
    ] {
        let out = run_kian(&args);
        assert_eq!(
            out.status.code(),
            Some(2),
            "{args:?} must exit 2: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("superagent.board.enabled is false"),
            "the refusal must name the key: {stderr}"
        );
        // The gate runs BEFORE resolve_db — the missing-db error is
        // proof it did not.
        assert!(
            !stderr.contains("does not exist"),
            "the store path was checked before the switch: {stderr}"
        );
        assert!(
            out.stdout.is_empty(),
            "no rows on stdout while disabled: {}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
    assert!(
        !missing.exists(),
        "a refused verb must not create the store"
    );
    let _ = std::fs::remove_file(&cfg);
}

/// Pin P3-2: the bare `kian tasks` form refuses with exit 2 and a
/// usage line naming `--all` as required in v1.
#[test]
fn kian_tasks_without_all_refuses_with_usage_naming_all() {
    let cfg = board_config("bare", true, false, std::path::Path::new("/dev/null"));
    let out = run_kian(&["tasks", "--config", cfg.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--all"), "usage must name --all: {stderr}");
    assert!(
        stderr.contains("usage:"),
        "the usage block prints: {stderr}"
    );
    assert!(out.stdout.is_empty());
    let _ = std::fs::remove_file(&cfg);
}

/// 🔴 READ-MODEL §6/§8 (the honest-fallback equivalence): with the
/// view built and fresh, `read_model.views.enabled: true` (view path)
/// and `false` (stream path) produce byte-identical operator output —
/// the operator never learns which path answered.
#[tokio::test]
async fn served_from_the_view_and_served_from_the_stream_produce_identical_output() {
    let path = temp_db("equiv");
    let store = EventStore::open(&path).await.unwrap();
    let parent_a = SessionId::new();
    seed_completed(&store, parent_a, 10, "verified task").await;
    let parent_b = SessionId::new();
    let lin_b = lineage(parent_b, 20, "outstanding task");
    seed_parent_full(&store, parent_b, &lin_b).await;
    seed_child_full(&store, &lin_b).await;
    // Build the view (board on) so the two configs below really take
    // different paths.
    kian_cli::views::rebuild_views_with(&store, "c5", 1_000, true)
        .await
        .expect("rebuild");
    store.close().await;

    // Non-vacuous: the view exists and is fresh — the view path is
    // live, not a second stream read.
    let probe = EventStore::open_read_only(&path).await.unwrap();
    assert!(
        kian_cli::views::fresh_task_board(&probe).await.is_some(),
        "the equivalence is only meaningful with a live view"
    );
    probe.close().await;

    let cfg_view = board_config("view", true, true, &path);
    let cfg_stream = board_config("stream", true, false, &path);
    for json in [false, true] {
        let from_view = kian_cli::commands::run_board(&cfg_view, Some(&path), json)
            .await
            .expect("view-path board");
        let from_stream = kian_cli::commands::run_board(&cfg_stream, Some(&path), json)
            .await
            .expect("stream-path board");
        let flat_view = kian_cli::commands::run_tasks(&cfg_view, Some(&path), json)
            .await
            .expect("view-path tasks");
        let flat_stream = kian_cli::commands::run_tasks(&cfg_stream, Some(&path), json)
            .await
            .expect("stream-path tasks");
        if json {
            // now_ms ticks between runs; the rows and the schema
            // version must not differ.
            let a: Value = serde_json::from_str(&from_view).unwrap();
            let b: Value = serde_json::from_str(&from_stream).unwrap();
            assert_eq!(a["tasks"], b["tasks"], "board json rows differ by path");
            assert_eq!(a["version"], b["version"]);
            let fa: Value = serde_json::from_str(&flat_view).unwrap();
            let fb: Value = serde_json::from_str(&flat_stream).unwrap();
            assert_eq!(fa["tasks"], fb["tasks"], "tasks json rows differ by path");
        } else {
            assert_eq!(from_view, from_stream, "board text differs by path");
            assert_eq!(flat_view, flat_stream, "tasks text differs by path");
        }
    }
    // Two tasks served, both verbs.
    assert_eq!(from_rows_count(&cfg_stream, &path).await, 2);
    let _ = std::fs::remove_file(&cfg_view);
    let _ = std::fs::remove_file(&cfg_stream);
    cleanup(&path);
}

/// Counts served rows through the stream-path verb (test helper).
async fn from_rows_count(cfg: &std::path::Path, db: &std::path::Path) -> usize {
    let text = kian_cli::commands::run_tasks(cfg, Some(db), false)
        .await
        .unwrap();
    text.lines().count()
}

/// The frozen `--json` document (pin P3-3): `{now_ms, version, tasks}`
/// with every row carrying exactly the thirteen documented keys — a
/// verdict-backed row nests `{gate, passed, seq}`, an outstanding row
/// carries `evidence: null`, and no key outside the pin set (never a
/// fabricated `evidence_ref`) ever appears.
#[tokio::test]
async fn the_json_shape_matches_the_documented_row() {
    let path = temp_db("golden");
    let store = EventStore::open(&path).await.unwrap();
    let parent_a = SessionId::new();
    let lin_a = seed_completed(&store, parent_a, 10, "verified task").await;
    let parent_b = SessionId::new();
    let lin_b = lineage(parent_b, 20, "outstanding task");
    seed_parent_full(&store, parent_b, &lin_b).await;
    seed_child_full(&store, &lin_b).await;
    store.close().await;

    let cfg = board_config("golden", true, false, &path);
    let doc: Value = serde_json::from_str(
        &kian_cli::commands::run_board(&cfg, Some(&path), true)
            .await
            .expect("board json"),
    )
    .expect("parses");
    let mut top: Vec<&str> = doc
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    top.sort_unstable();
    assert_eq!(top, vec!["now_ms", "tasks", "version"], "top-level shape");
    assert!(doc["now_ms"].as_u64().is_some(), "now_ms is epoch millis");
    assert_eq!(
        doc["version"],
        json!(SCHEMA_VERSION),
        "version is the event schema version the rows derive under"
    );
    let tasks = doc["tasks"].as_array().expect("tasks array");
    assert_eq!(tasks.len(), 2);
    let ids: Vec<&str> = tasks
        .iter()
        .map(|t| t["task_id"].as_str().unwrap())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "task_id order (BTreeMap, pin P3-3)");
    for task in tasks {
        let mut keys: Vec<&str> = task
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "assignee",
                "attempt",
                "child",
                "depends_on",
                "evidence",
                "evidence_provenance",
                "gate",
                "group",
                "outcome",
                "parent",
                "task_id",
                "thread",
                "thread_status",
            ],
            "pin P3-3's row key set, key for key — nothing extra, ever"
        );
        assert_eq!(task["group"], Value::Null, "group is P4's key: null in P3");
        assert_eq!(task["depends_on"], json!([]), "no dependencies in P3");
        assert_eq!(task["assignee"], Value::Null, "no P2 roster yet");
        assert_eq!(task["evidence_provenance"], json!("derived"), "pin P3-4");
    }
    let verified = tasks
        .iter()
        .find(|t| t["task_id"] == json!(lin_a.identity.task_id_str()))
        .expect("verified row");
    assert_eq!(verified["gate"], json!("pass"));
    let evidence = verified["evidence"].as_object().expect("evidence pointer");
    let mut ek: Vec<&str> = evidence.keys().map(String::as_str).collect();
    ek.sort_unstable();
    assert_eq!(ek, vec!["gate", "passed", "seq"]);
    assert_eq!(evidence["gate"], json!("worker"));
    assert_eq!(evidence["passed"], json!(true));
    assert_eq!(
        evidence["seq"],
        json!(6),
        "the verdict's own seq on the child stream"
    );
    let outstanding = tasks
        .iter()
        .find(|t| t["task_id"] == json!(lin_b.identity.task_id_str()))
        .expect("outstanding row");
    assert_eq!(outstanding["evidence"], Value::Null, "no verdict ⇒ null");
    assert_eq!(outstanding["gate"], json!("absent"));
    assert_eq!(outstanding["outcome"], json!("outstanding"));
    let _ = std::fs::remove_file(&cfg);
    cleanup(&path);
}

/// The empty state (the `kian tickets` precedent): a store with no
/// thread/started lineage prints the honest one-liner on both verbs —
/// including when views are on and no view exists (the fallback).
#[tokio::test]
async fn an_empty_store_prints_the_empty_state() {
    let path = temp_db("empty");
    let store = EventStore::open(&path).await.unwrap();
    store.close().await;
    let cfg = board_config("empty", true, true, &path);
    for out in [
        kian_cli::commands::run_board(&cfg, Some(&path), false).await,
        kian_cli::commands::run_tasks(&cfg, Some(&path), false).await,
    ] {
        assert_eq!(
            out.unwrap(),
            "no tasks (no thread/started lineage in this store)\n"
        );
    }
    let _ = std::fs::remove_file(&cfg);
    cleanup(&path);
}

/// R8's observability boundary: the verbs write nothing — event count
/// (the store key), inbox rows and `view_rows` are unchanged across a
/// full run of both verbs, text and json.
#[tokio::test]
async fn the_verbs_write_nothing() {
    let path = temp_db("readonly");
    let store = EventStore::open(&path).await.unwrap();
    let parent = SessionId::new();
    let lin = seed_completed(&store, parent, 10, "verified task").await;
    kian_cli::views::rebuild_views_with(&store, "c5", 1_000, true)
        .await
        .expect("rebuild");
    let sessions = vec![parent, lin.child];
    store.close().await;

    let snapshot = |p: &std::path::Path, s: &[SessionId]| {
        let p = p.to_path_buf();
        let s = s.to_vec();
        async move {
            let store = EventStore::open_read_only(&p).await.unwrap();
            let key = store.view_store_key().await.unwrap();
            let mut counts = Vec::new();
            let mut inbox = Vec::new();
            for session in &s {
                counts.push(store.rebuild(*session).await.unwrap().len());
                inbox.push(format!("{:?}", store.input_rows(*session).await.unwrap()));
            }
            let view = format!(
                "{:?}",
                store
                    .view_rows(kian_cli::views::VIEW_TASK_BOARD)
                    .await
                    .unwrap()
            );
            store.close().await;
            (key.events_total, key.max_seq, counts, inbox, view)
        }
    };

    let before = snapshot(&path, &sessions).await;
    let cfg = board_config("wr", true, true, &path);
    for json in [false, true] {
        kian_cli::commands::run_board(&cfg, Some(&path), json)
            .await
            .expect("board run");
        kian_cli::commands::run_tasks(&cfg, Some(&path), json)
            .await
            .expect("tasks run");
    }
    let after = snapshot(&path, &sessions).await;
    assert_eq!(before, after, "a read-only verb changed the store");
    let _ = std::fs::remove_file(&cfg);
    cleanup(&path);
}
