//! E2E acceptance for the materialized views (READ-MODEL §6/§11, P2a):
//! rebuild equivalence vs full derivation on a randomized multi-session
//! store (workers with `thread/started` lineage, usage-bearing turns,
//! compacts), the stale and corrupt fallbacks, the disabled gate, the
//! rebuild lease, and the pre-v4 read-only reader — every operator
//! surface (tickets board, usage-stats, cache-stats) answers from a
//! fresh view with values identical to the honest full derivation, and
//! falls back to exactly that derivation on any doubt (R1).

use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore, ViewRow};
use serde_json::{Value, json};

/// A tiny deterministic PRNG (the orchestrator snapshot-suite pattern).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "kian-views-e2e-{tag}-{}.sqlite",
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
                ts: 1_762_000_000_000 + seq,
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

/// Seeds a deterministic multi-session store: `plain` ordinary sessions
/// with usage-bearing turns (and a compact in one of them) plus `worker`
/// worker sessions carrying `thread/started` lineage naming themselves
/// (the worker-side discovery arm) and a pending inbox row on the first
/// worker. The exact contents do not matter — what matters is that the
/// view path and the honest path fold the SAME store.
async fn seed_store(store: &EventStore, plain: usize, workers: usize, seed: u64) -> Vec<SessionId> {
    let mut rng = Rng(seed);
    let mut all = Vec::new();
    for i in 0..plain {
        let sid = SessionId::new();
        push(
            store,
            sid,
            1,
            Epoch::GENESIS,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        push(
            store,
            sid,
            2,
            Epoch(1),
            EventKind::InputAdmitted,
            json!({"role": "user", "content": format!("p{i}")}),
        )
        .await;
        push(store, sid, 3, Epoch(1), EventKind::DrainClaimed, json!({})).await;
        let turns = 1 + rng.below(3);
        for t in 0..turns {
            let seq = 4 + t;
            let usage = json!({
                "input_tokens": 10 + t,
                "output_tokens": 2 + t,
                "cache_read_input_tokens": rng.below(5),
            });
            push(
                store,
                sid,
                seq,
                Epoch(1),
                EventKind::TurnEnd,
                json!({
                    "iteration": 0, "outcome": "final_answer", "usage": usage
                }),
            )
            .await;
        }
        // One session in three carries a compact (the R5 invalidation
        // point — the cache view must count it).
        if i % 3 == 0 {
            let head = store.head(sid).await.unwrap().next_seq;
            push(
                store,
                sid,
                head,
                Epoch(1),
                EventKind::Compact,
                json!({
                    "from_seq": 2, "to_seq": head - 1,
                    "summary": {"role": "assistant", "content": "sum"},
                }),
            )
            .await;
        }
        all.push(sid);
    }
    for w in 0..workers {
        let sid = SessionId::new();
        let thread = uuid::Uuid::new_v4();
        push(
            store,
            sid,
            1,
            Epoch::GENESIS,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        push(store, sid, 2, Epoch(1), EventKind::DrainClaimed, json!({})).await;
        // Worker-side lineage: the stream's own thread/started names the
        // worker (worker discovery's self-naming arm).
        push(
            store,
            sid,
            3,
            Epoch(1),
            EventKind::ThreadStarted,
            json!({
                "worker": sid.0.to_string(), "thread_id": thread.to_string(),
            }),
        )
        .await;
        let turns = 1 + rng.below(2);
        for t in 0..turns {
            let seq = 4 + t;
            push(
                store,
                sid,
                seq,
                Epoch(1),
                EventKind::TurnEnd,
                json!({
                    "iteration": 0, "outcome": "final_answer",
                    "usage": {"prompt_tokens": 7 + w as u64 + t, "completion_tokens": 1},
                }),
            )
            .await;
        }
        // An input after the last claim: the `queued` stage arm (arm 5).
        let head = store.head(sid).await.unwrap().next_seq;
        push(
            store,
            sid,
            head,
            Epoch(1),
            EventKind::InputAdmitted,
            json!({
                "role": "user", "content": format!("w{w}"),
            }),
        )
        .await;
        // A pending inbox row on the first worker (the board's
        // pending_inbox arm, read live by both paths at their own now).
        if w == 0 {
            store
                .admit(kian_core::model::InputRow {
                    id: uuid::Uuid::new_v4(),
                    session: sid,
                    payload: json!({"role": "user", "content": "queued work"}),
                    admitted_at: 1,
                    eligible_at: 1,
                    idempotency_key: None,
                })
                .await
                .expect("admit");
        }
        all.push(sid);
    }
    all
}

/// L1 (READ-MODEL §11 P2a): after a rebuild, every view equals the
/// honest full derivation — directory facts, usage totals, cache
/// counters and the ticket board (same `now` on both paths).
#[tokio::test]
async fn rebuild_equals_full_derivation_on_a_multi_session_store() {
    for &seed in &[7u64, 99] {
        let path = temp_db("equiv");
        let store = EventStore::open(&path).await.unwrap();
        seed_store(&store, 5, 3, seed).await;

        let out = kian_cli::views::rebuild_views(&store, "builder", 1_000)
            .await
            .unwrap();
        assert!(!out.lease_held);
        assert_eq!(out.sessions, 8);
        assert_eq!(out.views, 4);

        // usage: the view fold == the honest full scan.
        let via_view = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
            .await
            .unwrap();
        let honest = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
            .await
            .unwrap();
        assert_eq!(via_view, honest, "usage view must equal full derivation");
        assert_eq!(via_view.sessions, 8);
        assert!(via_view.requests_with_usage > 0);

        // cache: same equivalence (compacts counted on both paths).
        let via_view = kian_cli::cache_metrics::cache_stats_report(&store, None, true)
            .await
            .unwrap();
        let honest = kian_cli::cache_metrics::cache_stats_report(&store, None, false)
            .await
            .unwrap();
        assert_eq!(via_view, honest, "cache view must equal full derivation");
        assert!(via_view.compacts > 0);

        // directory: entries equal a per-session re-derivation.
        let entries = kian_cli::views::fresh_directory(&store)
            .await
            .expect("fresh");
        assert_eq!(entries.len(), 8);
        for entry in &entries {
            let sid: uuid::Uuid = entry.session.parse().unwrap();
            let events = store.rebuild(SessionId(sid)).await.unwrap();
            assert_eq!(entry.events, events.len() as u64);
            assert_eq!(entry.version, events.last().unwrap().seq);
            assert_eq!(entry.first_ts, events.first().unwrap().ts);
            assert_eq!(entry.last_ts, events.last().unwrap().ts);
        }

        // tickets: the view rows decode into exactly the honest board
        // (same now ⇒ same ages, same pending, same stages).
        let now = 1_762_000_060_000;
        let entries = kian_cli::views::fresh_tickets(&store).await.expect("fresh");
        let view_rows: Vec<_> = entries.iter().map(|e| e.to_row(now).unwrap()).collect();
        let honest_rows = kian_cli::commands::ticket_board(&store, now).await.unwrap();
        assert_eq!(view_rows, honest_rows, "ticket view must equal the board");
        assert_eq!(view_rows.len(), 3, "the three workers");

        // Single-session usage restriction is identical on both paths.
        let one = SessionId(honest_rows.first().unwrap().session.0);
        let via_view = kian_cli::usage_metrics::usage_stats_report(&store, Some(one), true)
            .await
            .unwrap();
        let honest = kian_cli::usage_metrics::usage_stats_report(&store, Some(one), false)
            .await
            .unwrap();
        assert_eq!(via_view, honest);

        store.close().await;
        cleanup(&path);
    }
}

/// Stale fallback (§6 rule): an append after the rebuild moves the
/// store key — every view reads stale (`None`) and the surfaces answer
/// from the honest full derivation.
#[tokio::test]
async fn stale_view_falls_back_to_full_derivation() {
    let path = temp_db("stale");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 2, 1, 5).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000)
        .await
        .unwrap();
    assert!(kian_cli::views::fresh_usage(&store).await.is_some());

    // ANY append — to a session shorter than the global max, so the
    // count axis alone catches it — makes every view stale.
    let head = store.head(sessions[0]).await.unwrap().next_seq;
    push(
        &store,
        sessions[0],
        head,
        Epoch(2),
        EventKind::TurnEnd,
        json!({"n": 1}),
    )
    .await;
    assert!(kian_cli::views::fresh_usage(&store).await.is_none());
    assert!(kian_cli::views::fresh_cache(&store).await.is_none());
    assert!(kian_cli::views::fresh_tickets(&store).await.is_none());
    assert!(kian_cli::views::fresh_directory(&store).await.is_none());

    // And the surfaces still answer — from the stream (identical with
    // the views gate on or off).
    let via_view = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
        .await
        .unwrap();
    let honest = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
        .await
        .unwrap();
    assert_eq!(via_view, honest);
    assert!(via_view.requests_with_usage > 0);

    store.close().await;
    cleanup(&path);
}

/// Corrupt view discarded, never guessed (§6 rule): a tampered payload
/// breaks the digest — the view is discarded and the honest derivation
/// answers. The digest-consistent garbage variant proves the enabled
/// gate is what hides views when disabled.
#[tokio::test]
async fn corrupt_view_is_discarded_and_the_gate_hides_views() {
    let path = temp_db("corrupt");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 2, 1, 11).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000)
        .await
        .unwrap();
    let honest = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
        .await
        .unwrap();

    // 1. Tamper a payload row: the recomputed digest disagrees — the
    //    view is discarded and the report falls back.
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("UPDATE view_rows SET payload = ? WHERE view = 'session_usage'")
            .bind(r#"{"session":"x","input_tokens":999999}"#)
            .execute(pool)
            .await
            .unwrap();
    }
    assert!(kian_cli::views::fresh_usage(&store).await.is_none());
    let report = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
        .await
        .unwrap();
    assert_eq!(report, honest, "the honest path answers after corruption");

    // 2. Digest-consistent garbage planted through the public API
    //    (the builder-is-trusted model): the gate decides. Enabled →
    //    the view IS consumed (proves the view path is real); disabled
    //    → hidden, the stream answers.
    let key = store.view_store_key().await.unwrap();
    let session_row = ViewRow {
        session: sessions[0].0.to_string(),
        row_version: 1,
        payload: serde_json::to_string(&kian_cli::views::UsageEntry {
            session: sessions[0].0.to_string(),
            requests_with_usage: 1,
            unknown_usage_requests: 0,
            input_tokens: 123_456,
            output_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        })
        .unwrap(),
    };
    store
        .view_publish("session_usage", vec![session_row], key, 2_000)
        .await
        .unwrap();
    let served = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
        .await
        .unwrap();
    assert_eq!(
        served.input_tokens, 123_456,
        "enabled gate consumes the view"
    );
    let gated = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
        .await
        .unwrap();
    assert_eq!(gated, honest, "disabled gate hides every view");

    store.close().await;
    cleanup(&path);
}

/// The rebuild lease (READ-MODEL §9): a live `views:all` holder
/// excludes the rebuilder — nothing is published; after release the
/// rebuild goes through (idempotent under retry).
#[tokio::test]
async fn rebuild_lease_excludes_concurrent_rebuilders() {
    let path = temp_db("lease");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 1, 1, 3).await;
    assert!(
        store
            .maintenance_lease_try_acquire("views:all", "other", 60_000, 1_000)
            .await
            .unwrap()
    );
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_100)
        .await
        .unwrap();
    assert!(out.lease_held);
    assert!(
        store.view_state("session_usage").await.unwrap().is_none(),
        "a lease-held rebuild publishes nothing"
    );

    store
        .maintenance_lease_release("views:all", "other")
        .await
        .unwrap();
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_200)
        .await
        .unwrap();
    assert!(!out.lease_held);
    assert!(store.view_state("session_usage").await.unwrap().is_some());

    store.close().await;
    cleanup(&path);
}

/// A read-only reader of a store last migrated before v4 (no view
/// tables) sees "no view" — never an error — and the honest derivation
/// answers.
#[tokio::test]
async fn pre_v4_read_only_reader_falls_back() {
    let path = temp_db("prev4");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 1, 1, 13).await;
    // Simulate the pre-v4 shape behind the API's back, then read the
    // same file read-only (the `kian tickets`/stats reader mode).
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("DROP TABLE view_rows")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE view_state")
            .execute(pool)
            .await
            .unwrap();
    }
    store.close().await;

    let reader = EventStore::open_read_only(&path).await.unwrap();
    assert!(kian_cli::views::fresh_usage(&reader).await.is_none());
    assert!(kian_cli::views::fresh_tickets(&reader).await.is_none());
    let via_view = kian_cli::usage_metrics::usage_stats_report(&reader, None, true)
        .await
        .unwrap();
    let honest = kian_cli::usage_metrics::usage_stats_report(&reader, None, false)
        .await
        .unwrap();
    assert_eq!(via_view, honest);
    assert_eq!(via_view.sessions, 2);
    reader.close().await;
    cleanup(&path);
}
