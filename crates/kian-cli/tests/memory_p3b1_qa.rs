//! kian-118 QA acceptance: independent probes for the P3b-1
//! index-accelerated memory reads (`EventStore::scan_kind` + the
//! switched CLI/server read paths, kian-116/117), authored by QA with
//! its own fixtures and generators (real sqlite stores, no network).
//!
//! Behaviour -> test map (the card's acceptance list):
//!
//! 1. Parity ............... qa_scan_kind_matches_scan_then_filter_on_adversarial_streams
//!    and .................. qa_narrow_read_rides_the_index_and_feeds_byte_identical_verb_folds
//! 2. Narrowness ........... qa_narrow_read_rides_the_index_and_feeds_byte_identical_verb_folds
//! 3. Read-only ............ qa_memory_verbs_append_nothing_on_a_read_only_connection
//! 4. Migration ............ qa_pre_v6_store_falls_back_migrates_forward_and_rolls_back_untouched
//! 5. Cost evidence ........ qa_cost_probe_5000_notes_over_50000_events (#[ignore], recorded)
//!
//! L3 parity of every verb against the literal pre-change binary on the
//! same store, plus a real-binary v5→v6 forward-migration walk, is run by
//! the recorded QA procedure seeded by qa_l3_fixture_seed (#[ignore]).

// QA-local helpers only: this file does no network, so it does not
// mount the shared `common` module (whose no-proxy helper would be
// dead code here and trip the -D warnings gate).

use std::path::{Path, PathBuf};

use kian_cli::commands::{
    run_memory_list, run_memory_map, run_memory_search, run_memory_show, run_memory_verify,
};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION, event_id};
use kian_core::memory::project_notes;
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::json;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------
// Fixtures (QA-authored, independent of the implementer's seeds).
// ---------------------------------------------------------------------

const T0: u64 = 1_762_100_000_000;

fn fixed_id(s: &str) -> SessionId {
    SessionId(uuid::Uuid::parse_str(s).unwrap())
}

/// The memory session id of the canonical QA fixture (fixed so the
/// L3 pre/post-binary diff of verb outputs needs no substitution).
const QA_MEMORY_SID: &str = "12121212-3434-5656-7878-909090909090";
const QA_SOURCE_SID: &str = "abababab-cdcd-efef-0101-232323232323";

async fn append(
    store: &EventStore,
    sid: SessionId,
    seq: u64,
    epoch: u64,
    kind: EventKind,
    bytes: serde_json::Value,
) {
    let e = Event {
        seq,
        session: sid,
        epoch: Epoch(epoch),
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
                epoch: Epoch(epoch),
                expected_seq: seq,
            },
        )
        .await
        .unwrap();
}

/// A note payload deriving from the shared source range [1,5).
fn note_bytes(subject: &str, text: &str, status: &str, source: SessionId) -> serde_json::Value {
    let mut v = json!({
        "subject": subject,
        "text": text,
        "status": status,
        "derived_from": [{"session": source.0, "start_seq": 1, "end_seq": 5}],
    });
    if status == "verified" {
        v["verified_by"] = json!({"method": "transcript", "ref": "3"});
    }
    v
}

/// A ~1 KiB non-note payload (the heavy tail the narrow read must skip).
fn heavy_tail(i: u64) -> serde_json::Value {
    json!({
        "role": "assistant",
        "content": {"text": format!("tail event {i}: {}", "x".repeat(1024))},
    })
}

/// Serializes events for byte-level comparison.
fn ser(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect()
}

/// sha256 over the ordered truth rows — the "did any event byte move"
/// witness for the migration probe.
async fn events_digest(store: &EventStore) -> (i64, i64, String) {
    let rows: Vec<(String, i64, i64, String)> = sqlx::query_as(
        "SELECT session, seq, version, envelope FROM events ORDER BY session ASC, seq ASC",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    let mut hasher = Sha256::new();
    let mut total_bytes = 0i64;
    for (session, seq, version, envelope) in &rows {
        hasher.update(session.as_bytes());
        hasher.update(seq.to_string().as_bytes());
        hasher.update(version.to_string().as_bytes());
        hasher.update(envelope.as_bytes());
        total_bytes += envelope.len() as i64;
    }
    (
        rows.len() as i64,
        total_bytes,
        format!("{:x}", hasher.finalize()),
    )
}

/// Rewinds a v6 store to the exact rows a store last migrated by the
/// v5 chain would hold (the v6 generation is one `CREATE INDEX IF NOT
/// EXISTS` + one bookkeeping row — its SQL is pinned frozen by the
/// store suite, so dropping those two artifacts is a faithful v5 state).
async fn rewind_to_v5(store: &EventStore) {
    sqlx::query("DROP INDEX IF EXISTS idx_events_session_kind_seq")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM _kian_migrations WHERE version = 6")
        .execute(store.pool())
        .await
        .unwrap();
}

/// Seeds the canonical QA mixed fixture: a source session over [1,5)
/// plus a memory session interleaving `note_count` notes (mixed
/// statuses, one duplicate-subject revision pair, shared evidence for
/// a map edge) with `note_count * 4` heavy (~1 KiB) non-note events.
/// Returns (db_path, memory_sid, source_sid).
async fn seed_mixed(db: &Path, memory_sid: SessionId, source_sid: SessionId, note_count: u64) {
    let store = EventStore::open(db).await.unwrap();

    for seq in 1..=4u64 {
        let (epoch, kind, bytes) = match seq {
            1 => (0, EventKind::SessionCreated, json!({})),
            2 => (
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "source input"}),
            ),
            3 => (
                1,
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": {"text": "source answer"}}),
            ),
            _ => (1, EventKind::TurnEnd, json!({"outcome": "ok"})),
        };
        append(&store, source_sid, seq, epoch, kind, bytes).await;
    }

    append(
        &store,
        memory_sid,
        1,
        0,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    let mut seq = 1u64;
    for i in 0..note_count {
        // Four heavy non-note events, then one note.
        for _ in 0..4 {
            seq += 1;
            append(
                &store,
                memory_sid,
                seq,
                1,
                EventKind::AssistantMessage,
                heavy_tail(seq),
            )
            .await;
        }
        seq += 1;
        let (subject, status) = match i % 5 {
            0 => (format!("alpha subject {i}"), "verified"),
            1 => (format!("beta subject {i}"), "verified"),
            2 => (format!("gamma candidate {i}"), "candidate"),
            3 => (format!("delta stale {i}"), "stale"),
            // One duplicate subject per five notes: the SAME subject as
            // the alpha from i-4, revised — the fold must keep the
            // latest revision only.
            _ => (format!("alpha subject {}", i - 4), "verified"),
        };
        let text = format!("{subject} claim text {i}.");
        append(
            &store,
            memory_sid,
            seq,
            1,
            EventKind::MemoryNote,
            note_bytes(&subject, &text, status, source_sid),
        )
        .await;
    }
    store.close().await;
}

fn scratch_db(tag: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("kian-p3b1-qa-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("kian.sqlite");
    (dir, db)
}

// ---------------------------------------------------------------------
// 1. Parity + 2. Narrowness (store level, QA's own generator)
// ---------------------------------------------------------------------

/// QA's own mixed-stream generator — a different engine (xorshift*),
/// different seeds and different shapes from the implementer's LCG
/// fixture: long non-note runs with 2 KiB payloads, note-only stretches,
/// a note-first and a note-last stream, and a multi-session store.
#[tokio::test]
async fn qa_scan_kind_matches_scan_then_filter_on_adversarial_streams() {
    let (dir, db) = scratch_db("p3b1-parity");
    let store = EventStore::open(&db).await.unwrap();

    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let pool_kinds = [
        EventKind::MemoryNote,
        EventKind::TurnStart,
        EventKind::MemoryNote,
        EventKind::StepRequest,
        EventKind::Compact,
        EventKind::MemoryNote,
        EventKind::ToolResult,
        EventKind::SlotBinding,
    ];

    // Session 0 — the pseudo-random interleave (xorshift*-driven, so the
    // kind pattern is genuinely irregular).
    let rand_sid = fixed_id("0000000a-0000-0000-0000-000000000000");
    for seq in 1..=500u64 {
        let kind = if seq == 1 {
            EventKind::SessionCreated
        } else {
            pool_kinds[(next() % pool_kinds.len() as u64) as usize]
        };
        let bytes = if matches!(kind, EventKind::MemoryNote) {
            note_bytes(
                &format!("subject {seq}"),
                &format!("text {seq}"),
                "verified",
                rand_sid,
            )
        } else {
            heavy_tail(seq)
        };
        let epoch = if seq == 1 { 0 } else { 1 };
        append(&store, rand_sid, seq, epoch, kind, bytes).await;
    }

    // Sessions 1..3 — fixed shapes sharing the store.
    let shapes: &[fn(u64) -> EventKind] = &[
        // notes only after the created event
        |seq| {
            if seq == 1 {
                EventKind::SessionCreated
            } else {
                EventKind::MemoryNote
            }
        },
        // one long non-note run, notes only at both ends
        |seq| {
            if seq == 1 || seq == 250 || seq == 500 {
                EventKind::MemoryNote
            } else {
                EventKind::TurnStart
            }
        },
        // no notes at all
        |seq| {
            if seq == 1 {
                EventKind::SessionCreated
            } else {
                EventKind::InputAdmitted
            }
        },
    ];

    for (si, shape) in shapes.iter().enumerate() {
        let sid = fixed_id(&format!("0000000a-0000-0000-0000-00000000000{}", si + 1));
        for seq in 1..=500u64 {
            let kind = shape(seq);
            let bytes = if matches!(kind, EventKind::MemoryNote) {
                note_bytes(
                    &format!("subject {seq}"),
                    &format!("text {seq}"),
                    "verified",
                    sid,
                )
            } else {
                heavy_tail(seq)
            };
            let epoch = if seq == 1 { 0 } else { 1 };
            append(&store, sid, seq, epoch, kind, bytes).await;
        }

        let full = store.scan(sid, 0..u64::MAX).await.unwrap();
        assert_eq!(full.len(), 500, "session {} stream shape", si + 1);
        let expected: Vec<String> = ser(&full
            .iter()
            .filter(|e| matches!(e.kind, EventKind::MemoryNote))
            .cloned()
            .collect::<Vec<_>>());
        let got = store.scan_kind(sid, EventKind::MemoryNote).await.unwrap();
        assert_eq!(
            ser(&got),
            expected,
            "session {}: scan_kind must be byte-identical to scan-then-filter",
            si + 1
        );
        if si != 2 {
            assert!(
                !got.is_empty(),
                "session {} fixture must contain notes",
                si + 1
            );
        } else {
            assert!(
                got.is_empty(),
                "the note-less session must answer empty, not error"
            );
        }
        for pair in got.windows(2) {
            assert!(
                pair[0].seq < pair[1].seq,
                "session {}: ascending seqs",
                si + 1
            );
        }
        assert!(got.iter().all(|e| e.session == sid));
    }

    // Session 0 parity (checked separately: it is the irregular one).
    {
        let full = store.scan(rand_sid, 0..u64::MAX).await.unwrap();
        assert_eq!(full.len(), 500);
        let expected: Vec<String> = ser(&full
            .iter()
            .filter(|e| matches!(e.kind, EventKind::MemoryNote))
            .cloned()
            .collect::<Vec<_>>());
        let got = store
            .scan_kind(rand_sid, EventKind::MemoryNote)
            .await
            .unwrap();
        assert_eq!(ser(&got), expected, "session 0 parity");
        let notes = got.len();
        assert!(notes > 50 && notes < 450, "generator sanity: {notes} notes");
    }

    // An unknown session and an absent kind both answer empty, never error.
    assert!(
        store
            .scan_kind(SessionId::new(), EventKind::MemoryNote)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .scan_kind(rand_sid, EventKind::DrainClaimed)
            .await
            .unwrap()
            .is_empty(),
        "an absent kind answers empty"
    );

    store.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// 1+2. The switched CLI read paths: narrow, index-served, byte-identical
// ---------------------------------------------------------------------

/// On a memory session with a 4:1 heavy non-note tail, the switched
/// read is narrow (exactly the note events), rides the v6 index (no
/// table scan), folds byte-identically to the full-rebuild read, and
/// the verbs' outputs equal the full-rebuild-derived outputs.
#[tokio::test]
async fn qa_narrow_read_rides_the_index_and_feeds_byte_identical_verb_folds() {
    let (dir, db) = scratch_db("p3b1-narrow");
    let memory = fixed_id(QA_MEMORY_SID);
    let source = fixed_id(QA_SOURCE_SID);
    seed_mixed(&db, memory, source, 80).await; // 80 notes + 320 heavy non-notes

    let store = EventStore::open_read_only(&db).await.unwrap();

    // Narrow: exactly the note events, nothing else loaded.
    let notes_read = store
        .scan_kind(memory, EventKind::MemoryNote)
        .await
        .unwrap();
    assert_eq!(notes_read.len(), 80, "only the note events");
    assert!(
        notes_read
            .iter()
            .all(|e| matches!(e.kind, EventKind::MemoryNote))
    );
    assert_eq!(
        notes_read.iter().map(|e| e.seq).collect::<Vec<_>>(),
        notes_read
            .iter()
            .map(|e| e.seq)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "ascending, deduplicated"
    );

    // Parity of the fold every verb consumes (list/show/verify/search/map
    // are pure functions over this fold).
    let full = store.rebuild(memory).await.unwrap();
    assert_eq!(full.len(), 401, "created + 80 notes + 320 tail");
    assert_eq!(
        serde_json::to_string(&project_notes(&notes_read)).unwrap(),
        serde_json::to_string(&project_notes(&full)).unwrap(),
        "the fold from the narrow read is byte-identical to the full-stream fold"
    );
    // The duplicate-subject revision pair actually exercised the fold.
    let folded = project_notes(&notes_read);
    assert!(
        folded.len() < 80,
        "the duplicate-subject revisions must have collapsed (got {})",
        folded.len()
    );

    // Index-served: the exact SCAN_KIND_SQL plan names the v6 index and
    // never degrades to a table scan.
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN SELECT version, envelope FROM events \
         WHERE session = ?1 AND kind = ?2 ORDER BY seq ASC",
    )
    .bind(memory.0.to_string())
    .bind("memory/note")
    .fetch_all(store.pool())
    .await
    .unwrap();
    let detail = plan
        .iter()
        .map(|(_, _, _, d)| d.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        detail.contains("idx_events_session_kind_seq"),
        "plan must use the per-session kind index: {detail}"
    );
    assert!(
        !detail.to_uppercase().contains("SCAN EVENTS"),
        "plan must not degrade to a table scan: {detail}"
    );
    store.close().await;

    // The list verb's JSON is exactly {session, notes: <full-rebuild fold>}
    // — the pre-change rendering, reconstructed from the full stream.
    let cfg = Path::new("kian.yaml");
    let list_json = run_memory_list(cfg, Some(&db), memory, true).await.unwrap();
    let expected_json = serde_json::to_string_pretty(&json!({
        "session": memory.0,
        "notes": project_notes(&full),
    }))
    .unwrap();
    assert_eq!(list_json, expected_json, "list --json byte-identical");

    // Every switched verb runs clean on the fixture (text + json).
    for out in [
        run_memory_list(cfg, Some(&db), memory, false)
            .await
            .unwrap(),
        run_memory_list(cfg, Some(&db), memory, true).await.unwrap(),
        run_memory_show(cfg, Some(&db), memory, "alpha subject 0", false)
            .await
            .unwrap(),
        run_memory_show(cfg, Some(&db), memory, &event_id(&memory, 6), true)
            .await
            .unwrap(),
        run_memory_search(cfg, Some(&db), memory, "claim", false)
            .await
            .unwrap(),
        run_memory_search(cfg, Some(&db), memory, "subject", true)
            .await
            .unwrap(),
        run_memory_map(cfg, Some(&db), memory, false).await.unwrap(),
        run_memory_map(cfg, Some(&db), memory, true).await.unwrap(),
        run_memory_verify(cfg, Some(&db), memory, false)
            .await
            .unwrap(),
        run_memory_verify(cfg, Some(&db), memory, true)
            .await
            .unwrap(),
    ] {
        assert!(!out.is_empty());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// 3. Read-only: append nothing, on a read-only connection
// ---------------------------------------------------------------------

/// Every switched memory verb appends nothing (event count/bytes and
/// inbox rows unchanged), and the read path the verbs use is a genuine
/// read-only connection: an INSERT through it fails with sqlite's
/// readonly-database error.
#[tokio::test]
async fn qa_memory_verbs_append_nothing_on_a_read_only_connection() {
    let (dir, db) = scratch_db("p3b1-readonly");
    let memory = fixed_id(QA_MEMORY_SID);
    let source = fixed_id(QA_SOURCE_SID);
    seed_mixed(&db, memory, source, 40).await;

    let cfg = Path::new("kian.yaml");
    let before = {
        let store = EventStore::open(&db).await.unwrap();
        let inputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inputs")
            .fetch_one(store.pool())
            .await
            .unwrap();
        let digest = events_digest(&store).await;
        store.close().await;
        (inputs, digest)
    };

    // Run every switched verb, both renderings.
    for res in [
        run_memory_list(cfg, Some(&db), memory, false).await,
        run_memory_list(cfg, Some(&db), memory, true).await,
        run_memory_show(cfg, Some(&db), memory, "alpha subject 0", false).await,
        run_memory_show(cfg, Some(&db), memory, "alpha subject 0", true).await,
        run_memory_search(cfg, Some(&db), memory, "claim", false).await,
        run_memory_search(cfg, Some(&db), memory, "claim", true).await,
        run_memory_map(cfg, Some(&db), memory, false).await,
        run_memory_map(cfg, Some(&db), memory, true).await,
        run_memory_verify(cfg, Some(&db), memory, false).await,
        run_memory_verify(cfg, Some(&db), memory, true).await,
    ] {
        res.unwrap();
    }

    let after = {
        let store = EventStore::open(&db).await.unwrap();
        let inputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inputs")
            .fetch_one(store.pool())
            .await
            .unwrap();
        let digest = events_digest(&store).await;
        store.close().await;
        (inputs, digest)
    };
    assert_eq!(before.0, after.0, "no inbox rows may appear");
    assert_eq!(
        before.1, after.1,
        "event count, envelope bytes and row digest must be unchanged (count, bytes, sha256)"
    );

    // The connection flavor the verbs open is read-only: a write through
    // an identically-opened pool is refused by sqlite itself.
    let reader = EventStore::open_read_only(&db).await.unwrap();
    let write = sqlx::query(
        "INSERT INTO events (session, seq, epoch, ts, kind, version, envelope) \
                             VALUES ('x', 1, 0, 0, 'session/created', 0, '{}')",
    )
    .execute(reader.pool())
    .await;
    assert!(
        write.is_err(),
        "an INSERT through the read-only pool must fail"
    );
    let msg = write.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("readonly") || msg.contains("read-only"),
        "expected sqlite's readonly-database refusal, got: {msg}"
    );
    reader.close().await;

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// 4. Migration: adjacent forward walk, clean reads, untouched stream
// ---------------------------------------------------------------------

/// A pre-v6 store (the exact v5-chain rows) reads correctly through the
/// planner fallback, forward-migrates on the next read-write open with
/// the index physically recreated, keeps every event byte identical,
/// walks/rebuilds cleanly, and rolls back to v5 rows with the stream
/// still untouched and the verb outputs still byte-identical.
#[tokio::test]
async fn qa_pre_v6_store_falls_back_migrates_forward_and_rolls_back_untouched() {
    let (dir, db) = scratch_db("p3b1-migration");
    let memory = fixed_id(QA_MEMORY_SID);
    let source = fixed_id(QA_SOURCE_SID);
    seed_mixed(&db, memory, source, 30).await;
    let cfg = Path::new("kian.yaml");

    // Baseline outputs with v6 present (the post-change store).
    let baseline: Vec<String> = vec![
        run_memory_list(cfg, Some(&db), memory, false)
            .await
            .unwrap(),
        run_memory_list(cfg, Some(&db), memory, true).await.unwrap(),
        run_memory_search(cfg, Some(&db), memory, "claim", false)
            .await
            .unwrap(),
        run_memory_map(cfg, Some(&db), memory, false).await.unwrap(),
        run_memory_verify(cfg, Some(&db), memory, true)
            .await
            .unwrap(),
    ];

    // Rewind to the v5 chain state.
    let v6_digest = {
        let store = EventStore::open(&db).await.unwrap();
        let d = events_digest(&store).await;
        rewind_to_v5(&store).await;
        store.close().await;
        d
    };

    // (a) A read-only open of the pre-v6 store does NOT migrate and
    //     still answers every verb byte-identically via the fallback.
    {
        let reader = EventStore::open_read_only(&db).await.unwrap();
        let versions = kian_store::migrations::applied_versions(reader.pool())
            .await
            .unwrap();
        assert_eq!(
            versions,
            (0..=5u32).collect::<Vec<_>>(),
            "no v6, no migration"
        );
        let idx: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' \
             AND name='idx_events_session_kind_seq'",
        )
        .fetch_one(reader.pool())
        .await
        .unwrap();
        assert_eq!(idx, 0, "the index must not exist yet");
        assert_eq!(events_digest(&reader).await, v6_digest);

        let fallback: Vec<String> = vec![
            run_memory_list(cfg, Some(&db), memory, false)
                .await
                .unwrap(),
            run_memory_list(cfg, Some(&db), memory, true).await.unwrap(),
            run_memory_search(cfg, Some(&db), memory, "claim", false)
                .await
                .unwrap(),
            run_memory_map(cfg, Some(&db), memory, false).await.unwrap(),
            run_memory_verify(cfg, Some(&db), memory, true)
                .await
                .unwrap(),
        ];
        assert_eq!(fallback, baseline, "fallback reads are byte-identical");
        reader.close().await;
    }

    // (b) The next read-write open forward-migrates: adjacent v6 lands,
    //     the index physically exists, no event byte moved, the stream
    //     walks/rebuilds cleanly and the verbs stay byte-identical.
    {
        let store = EventStore::open(&db).await.unwrap();
        let versions = kian_store::migrations::applied_versions(store.pool())
            .await
            .unwrap();
        assert_eq!(
            versions,
            (0..=6u32).collect::<Vec<_>>(),
            "adjacent walk to head"
        );
        let idx: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' \
             AND name='idx_events_session_kind_seq'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(idx, 1, "v6 must physically create the index");
        assert_eq!(events_digest(&store).await, v6_digest, "stream untouched");

        let rebuilt = store.rebuild(memory).await.unwrap();
        assert_eq!(rebuilt.len(), 151, "clean walk of the migrated store");
        let narrow = store
            .scan_kind(memory, EventKind::MemoryNote)
            .await
            .unwrap();
        assert_eq!(
            ser(&narrow),
            ser(&rebuilt
                .iter()
                .filter(|e| matches!(e.kind, EventKind::MemoryNote))
                .cloned()
                .collect::<Vec<_>>())
        );
        store.close().await;

        let migrated: Vec<String> = vec![
            run_memory_list(cfg, Some(&db), memory, false)
                .await
                .unwrap(),
            run_memory_list(cfg, Some(&db), memory, true).await.unwrap(),
            run_memory_search(cfg, Some(&db), memory, "claim", false)
                .await
                .unwrap(),
            run_memory_map(cfg, Some(&db), memory, false).await.unwrap(),
            run_memory_verify(cfg, Some(&db), memory, true)
                .await
                .unwrap(),
        ];
        assert_eq!(
            migrated, baseline,
            "post-migration reads are byte-identical"
        );
    }

    // (c) Rollback to the v5 rows: the stream is untouched and the
    //     verbs still answer byte-identically.
    {
        let store = EventStore::open(&db).await.unwrap();
        rewind_to_v5(&store).await;
        assert_eq!(
            events_digest(&store).await,
            v6_digest,
            "rollback moves no byte"
        );
        store.close().await;

        let rolled: Vec<String> = vec![
            run_memory_list(cfg, Some(&db), memory, false)
                .await
                .unwrap(),
            run_memory_search(cfg, Some(&db), memory, "claim", false)
                .await
                .unwrap(),
            run_memory_map(cfg, Some(&db), memory, false).await.unwrap(),
        ];
        assert_eq!(
            rolled,
            baseline
                .iter()
                .take(1)
                .chain(baseline.iter().skip(2).take(2))
                .cloned()
                .collect::<Vec<_>>(),
            "post-rollback reads are byte-identical"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// 5. Cost evidence (recorded, not perf-gated) + the L3 fixture seeder
// ---------------------------------------------------------------------

/// The card's cost probe: one memory session with 5 000 notes over
/// 50 000 events (~10% notes). Measures and PRINTS (never asserts):
/// the pre-change read shape (full rebuild → fold → render) vs the
/// switched shape (scan_kind → fold → render), plus the end-to-end
/// `memory list` verb. Run with:
/// `cargo test --release -p kian-cli --test memory_p3b1_qa -- --ignored --nocapture`
#[tokio::test]
#[ignore = "QA cost probe: prints wall times, asserts nothing perf-shaped"]
async fn qa_cost_probe_5000_notes_over_50000_events() {
    let dir = std::env::temp_dir().join("kian-p3b1-cost");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("kian.sqlite");
    let memory = fixed_id(QA_MEMORY_SID);
    let source = fixed_id(QA_SOURCE_SID);

    let t_seed = std::time::Instant::now();
    {
        let store = EventStore::open(&db).await.unwrap();
        for seq in 1..=4u64 {
            let (epoch, kind, bytes) = match seq {
                1 => (0, EventKind::SessionCreated, json!({})),
                2 => (
                    1,
                    EventKind::InputAdmitted,
                    json!({"role": "user", "content": "src"}),
                ),
                3 => (
                    1,
                    EventKind::AssistantMessage,
                    json!({"role": "assistant", "content": {"text": "ans"}}),
                ),
                _ => (1, EventKind::TurnEnd, json!({"outcome": "ok"})),
            };
            append(&store, source, seq, epoch, kind, bytes).await;
        }
        append(&store, memory, 1, 0, EventKind::SessionCreated, json!({})).await;
        let mut seq = 1u64;
        // 45 000 non-note tail events + 5 000 notes = 50 001 rows.
        for i in 0..5_000u64 {
            for _ in 0..9 {
                seq += 1;
                append(
                    &store,
                    memory,
                    seq,
                    1,
                    EventKind::AssistantMessage,
                    heavy_tail(seq),
                )
                .await;
            }
            seq += 1;
            let subject = format!("subject {i:05}");
            append(
                &store,
                memory,
                seq,
                1,
                EventKind::MemoryNote,
                note_bytes(
                    &subject,
                    &format!("{subject} claim text about the deploy rules."),
                    "verified",
                    source,
                ),
            )
            .await;
        }
        store.close().await;
    }
    println!(
        "QA-COST seed: {:?} (50 001 events, 5 000 notes)",
        t_seed.elapsed()
    );

    let render = |notes: &Vec<kian_core::memory::Note>| {
        kian_memory::render_memory_index_with(
            notes,
            &kian_memory::MemoryIndexConfig::default(),
            &kian_core::secrets::SecretRedactor::from_env_names(&[]),
        )
        .map(|r| r.source_seq)
    };

    let best_of = |samples: &mut Vec<f64>| {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        samples[0]
    };

    // The pre-change read shape: full rebuild → fold → render.
    let mut before: Vec<f64> = Vec::new();
    let mut after: Vec<f64> = Vec::new();
    for _ in 0..20 {
        let store = EventStore::open_read_only(&db).await.unwrap();
        let t = std::time::Instant::now();
        let full = store.rebuild(memory).await.unwrap();
        let notes = project_notes(&full);
        let _ = render(&notes);
        before.push(t.elapsed().as_secs_f64() * 1e3);

        let t = std::time::Instant::now();
        let narrow = store
            .scan_kind(memory, EventKind::MemoryNote)
            .await
            .unwrap();
        let notes = project_notes(&narrow);
        let _ = render(&notes);
        after.push(t.elapsed().as_secs_f64() * 1e3);
        store.close().await;
    }
    let b = best_of(&mut before);
    let a = best_of(&mut after);
    println!("QA-COST read before (rebuild+fold+render): {b:.2} ms (best of 20)");
    println!("QA-COST read after  (scan_kind+fold+render): {a:.2} ms (best of 20)");
    println!("QA-COST speedup: {:.2}x", b / a);

    // End-to-end verb wall time (includes the read-only open).
    let mut e2e: Vec<f64> = Vec::new();
    let cfg = Path::new("kian.yaml");
    for _ in 0..10 {
        let t = std::time::Instant::now();
        run_memory_list(cfg, Some(&db), memory, false)
            .await
            .unwrap();
        e2e.push(t.elapsed().as_secs_f64() * 1e3);
    }
    println!(
        "QA-COST memory list end-to-end (post-change): {:.2} ms (best of 10)",
        best_of(&mut e2e)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Seeds the canonical fixture for the recorded L3 procedure (real
/// pre-change vs post-change binaries on the SAME store, plus a
/// real-binary v5→v6 migration walk). Sets nothing in CI: run with
/// `KIAN_QA_P3B1_DIR=<dir> cargo test -p kian-cli --test memory_p3b1_qa \
///  -- --ignored --nocapture qa_l3_fixture_seed`
#[tokio::test]
#[ignore = "QA L3 fixture seeder: run only with KIAN_QA_P3B1_DIR set"]
async fn qa_l3_fixture_seed() {
    let Some(dir) = std::env::var_os("KIAN_QA_P3B1_DIR") else {
        println!("qa_l3_fixture_seed: KIAN_QA_P3B1_DIR unset — nothing seeded");
        return;
    };
    let dir = PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("fixture.sqlite");
    let _ = std::fs::remove_file(&db);
    let memory = fixed_id(QA_MEMORY_SID);
    let source = fixed_id(QA_SOURCE_SID);
    seed_mixed(&db, memory, source, 25).await;
    println!("QA-L3 fixture: {}", db.display());
    println!("QA-L3 memory session: {}", memory.0);
    println!("QA-L3 source session: {}", source.0);
    println!("QA-L3 show-by-event-id: {}", event_id(&memory, 6));
}
