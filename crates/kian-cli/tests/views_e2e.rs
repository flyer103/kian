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

        let out = kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
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
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
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
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
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
            cache_miss_input_tokens: 0,
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
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_100, false)
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
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_200, false)
        .await
        .unwrap();
    assert!(!out.lease_held);
    assert!(store.view_state("session_usage").await.unwrap().is_some());

    store.close().await;
    cleanup(&path);
}

/// R9-10: an incremental tick on an UNCHANGED store is a no-op — zero
/// sessions derived, nothing published, every view stays fresh (the
/// default-on 60 s ticker must not re-fold O(total history) per beat).
#[tokio::test]
async fn incremental_tick_after_no_changes_is_a_noop() {
    let path = temp_db("idle");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 5, 2, 21).await;
    let first = kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();
    assert_eq!(first.sessions, 7);
    assert_eq!(first.views, 4);
    assert_eq!(first.derived, 7, "the first build derives every session");

    // Nothing appended between the ticks: the second tick derives zero
    // sessions and publishes nothing, and the views remain fresh.
    let second = kian_cli::views::rebuild_views(&store, "builder", 1_100, false)
        .await
        .unwrap();
    assert!(!second.lease_held);
    assert_eq!(second.derived, 0, "an idle tick folds nothing");
    assert_eq!(second.views, 0, "an idle tick publishes nothing");
    assert!(kian_cli::views::fresh_usage(&store).await.is_some());
    assert!(kian_cli::views::fresh_tickets(&store).await.is_some());

    store.close().await;
    cleanup(&path);
}

/// R9-10: the incremental rebuild after PARTIAL changes (appends to a
/// few sessions, one new session, one deleted session) derives only the
/// changed sessions and lands exactly the views a full rebuild would —
/// reused rows for untouched sessions, re-derived rows for the rest,
/// the deleted session's rows gone (READ-MODEL §6 equivalence holds).
#[tokio::test]
async fn incremental_rebuild_equals_full_after_partial_changes() {
    let path = temp_db("delta");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 6, 3, 33).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();

    // Two plain sessions grow (a turn each), one brand-new session
    // appears, and one plain session leaves the online store (the
    // retention shape — deleted behind the API's back, exactly what an
    // archived session looks like to the next tick).
    for &sid in &sessions[0..2] {
        let head = store.head(sid).await.unwrap().next_seq;
        push(
            &store,
            sid,
            head,
            Epoch(1),
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer",
                   "usage": {"input_tokens": 5, "output_tokens": 1}}),
        )
        .await;
    }
    let fresh = SessionId::new();
    push(
        &store,
        fresh,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        &store,
        fresh,
        2,
        Epoch(1),
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "new"}),
    )
    .await;
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("DELETE FROM events WHERE session = ?")
            .bind(sessions[2].0.to_string())
            .execute(pool)
            .await
            .unwrap();
    }

    let out = kian_cli::views::rebuild_views(&store, "builder", 1_200, false)
        .await
        .unwrap();
    assert!(!out.lease_held);
    assert_eq!(out.derived, 3, "two grown sessions + the new session");
    assert_eq!(out.sessions, 9, "6 plain + 3 workers - 1 deleted + 1 new");

    // Every view now equals the honest full derivation on the same
    // store (the equivalence a full rebuild guarantees, held by the
    // incremental path: reused rows are byte-identical folds).
    let via_view = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
        .await
        .unwrap();
    let honest = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
        .await
        .unwrap();
    assert_eq!(via_view, honest, "usage view equals full derivation");

    let via_view = kian_cli::cache_metrics::cache_stats_report(&store, None, true)
        .await
        .unwrap();
    let honest = kian_cli::cache_metrics::cache_stats_report(&store, None, false)
        .await
        .unwrap();
    assert_eq!(via_view, honest, "cache view equals full derivation");

    let entries = kian_cli::views::fresh_directory(&store)
        .await
        .expect("fresh after the incremental tick");
    let live: std::collections::BTreeSet<String> =
        entries.iter().map(|e| e.session.clone()).collect();
    assert_eq!(entries.len(), 9);
    assert!(
        !live.contains(&sessions[2].0.to_string()),
        "deleted session gone"
    );
    assert!(live.contains(&fresh.0.to_string()), "new session present");
    for entry in &entries {
        let sid: uuid::Uuid = entry.session.parse().unwrap();
        let events = store.rebuild(SessionId(sid)).await.unwrap();
        assert_eq!(entry.events, events.len() as u64);
        assert_eq!(entry.version, events.last().unwrap().seq);
    }

    let now = 1_762_000_060_000;
    let entries = kian_cli::views::fresh_tickets(&store)
        .await
        .expect("fresh tickets");
    let view_rows: Vec<_> = entries.iter().map(|e| e.to_row(now).unwrap()).collect();
    let honest_rows = kian_cli::commands::ticket_board(&store, now).await.unwrap();
    assert_eq!(view_rows, honest_rows, "ticket view equals the board");

    store.close().await;
    cleanup(&path);
}

/// R9-10: a tampered row breaks the stored digest — the incremental
/// path must NOT reuse the tampered view; the next tick discards and
/// fully rebuilds it (the self-heal a per-tick full rebuild used to
/// give, preserved).
#[tokio::test]
async fn tampered_view_self_heals_on_the_next_incremental_tick() {
    let path = temp_db("heal");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 4, 2, 44).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("UPDATE view_rows SET payload = ? WHERE view = 'session_directory'")
            .bind(r#"{"session":"x","state":"running","events":999}"#)
            .execute(pool)
            .await
            .unwrap();
    }

    // The stored digest no longer covers the stored bytes; the next
    // tick must NOT reuse the tampered rows — it discards and fully
    // rebuilds the view (self-heal preserved).
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_100, false)
        .await
        .unwrap();
    assert!(out.derived > 0, "digest mismatch ⇒ full re-derivation");
    let entries = kian_cli::views::fresh_directory(&store)
        .await
        .expect("fresh and intact after self-heal");
    assert_eq!(entries.len(), 6);
    for entry in &entries {
        assert_ne!(entry.events, 999, "no tampered payload survived");
    }

    store.close().await;
    cleanup(&path);
}

/// READ-MODEL §6: the reuse base must prove the stored rows DECODE,
/// not merely that their digest matches. A digest-clean payload whose
/// shape the served type no longer accepts — the production state
/// where every usage row predated a new counter — would otherwise be
/// reused forever, because the reuse key is the live head, not the
/// payload.
#[tokio::test]
async fn digest_clean_but_undecodable_view_self_heals_on_the_next_tick() {
    let path = temp_db("undecodable");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 4, 2, 47).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();

    // Stamp one payload the served type cannot decode and re-seal the
    // stored digest over it: the stored version is now cryptographically
    // consistent and structurally unusable.
    let mut rows = store
        .view_rows("session_usage")
        .await
        .unwrap()
        .expect("usage published");
    assert!(!rows.is_empty());
    rows[0].payload = json!({
        "session": rows[0].session.clone(),
        "requests_with_usage": "many",
    })
    .to_string();
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("UPDATE view_rows SET payload = ? WHERE view = ? AND session = ?")
            .bind(&rows[0].payload)
            .bind("session_usage")
            .bind(&rows[0].session)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("UPDATE view_state SET digest = ? WHERE view = ?")
            .bind(kian_store::view_rows_digest("session_usage", &rows))
            .bind("session_usage")
            .execute(pool)
            .await
            .unwrap();
    }
    assert!(
        serde_json::from_str::<kian_cli::views::UsageEntry>(&rows[0].payload).is_err(),
        "the stamped payload must not decode as the served shape"
    );

    let out = kian_cli::views::rebuild_views(&store, "builder", 1_100, false)
        .await
        .unwrap();
    assert!(out.derived > 0, "undecodable payload ⇒ full re-derivation");
    let entries = kian_cli::views::fresh_usage(&store)
        .await
        .expect("fresh usage after the self-heal");
    assert_eq!(entries.len(), rows.len(), "every session is served again");
    let via_view = kian_cli::usage_metrics::usage_stats_report(&store, None, true)
        .await
        .unwrap();
    let honest = kian_cli::usage_metrics::usage_stats_report(&store, None, false)
        .await
        .unwrap();
    assert_eq!(via_view, honest, "the healed view equals the derivation");

    store.close().await;
    cleanup(&path);
}

/// R9-10: `pending_inbox` can move with no append at all (an inbox row
/// crossing its `eligible_at`). The incremental tick must detect the
/// drift against the stored ticket rows and refresh the board (the
/// §6 declared staleness bound stays ≤ one rebuild interval).
#[tokio::test]
async fn pending_inbox_drift_without_appends_refreshes_tickets() {
    let path = temp_db("drift");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 2, 1, 55).await;
    let worker = sessions[2]; // seed_store: plain sessions first, workers after
    // An inbox row that is NOT yet eligible at t=1_000 but is at t=6_000.
    store
        .admit(kian_core::model::InputRow {
            id: uuid::Uuid::new_v4(),
            session: worker,
            payload: json!({"role": "user", "content": "later"}),
            admitted_at: 900,
            eligible_at: 5_000,
            idempotency_key: None,
        })
        .await
        .unwrap();

    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();
    let entries = kian_cli::views::fresh_tickets(&store)
        .await
        .expect("fresh at t=1_000");
    assert_eq!(entries.len(), 1);
    // seed_store's own queued row is eligible; the t=5_000 row is not.
    assert_eq!(entries[0].pending_inbox, 1, "only the seed row is eligible");

    // Time passes; no appends. The tick compares live pending counts
    // with the stored rows, sees the drift, refreshes the board.
    let out = kian_cli::views::rebuild_views(&store, "builder", 6_000, false)
        .await
        .unwrap();
    assert!(out.board_refreshed, "pending drift forces a board refresh");
    let entries = kian_cli::views::fresh_tickets(&store)
        .await
        .expect("fresh at t=6_000");
    assert_eq!(entries[0].pending_inbox, 2, "the later row became eligible");
    let honest = kian_cli::commands::ticket_board(&store, 6_000)
        .await
        .unwrap();
    assert_eq!(entries[0].to_row(6_000).unwrap(), honest[0]);

    store.close().await;
    cleanup(&path);
}

/// R9-10: the board trigger is the DELTA, not the stream — a plain
/// session (its history carries `input/admitted`, a board kind)
/// growing by a non-board event must NOT re-fold the board; the ticket
/// view stays exact via the state-only key flip.
#[tokio::test]
async fn plain_append_does_not_refresh_the_board() {
    let path = temp_db("plainappend");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 4, 2, 66).await;
    kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();

    // A turn on a plain session: head moves, no board event in the delta.
    let head = store.head(sessions[0]).await.unwrap().next_seq;
    push(
        &store,
        sessions[0],
        head,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer",
               "usage": {"input_tokens": 4, "output_tokens": 1}}),
    )
    .await;

    let out = kian_cli::views::rebuild_views(&store, "builder", 1_100, false)
        .await
        .unwrap();
    assert_eq!(out.derived, 1, "only the grown session");
    assert!(
        !out.board_refreshed,
        "a plain non-board delta must not re-fold the board"
    );
    assert_eq!(out.views, 4);

    // And the ticket view is still served and still exact.
    let now = 1_762_000_060_000;
    let entries = kian_cli::views::fresh_tickets(&store)
        .await
        .expect("fresh after the key flip");
    let view_rows: Vec<_> = entries.iter().map(|e| e.to_row(now).unwrap()).collect();
    let honest_rows = kian_cli::commands::ticket_board(&store, now).await.unwrap();
    assert_eq!(view_rows, honest_rows);

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

// ---------------------------------------------------------------------------
// the task_board view (R17 C4, pins P3-1/P3-3/P3-5)
// ---------------------------------------------------------------------------

/// Seeds one delegation lineage (parent dispatch side + child stream +
/// the parent's waiting report row) and returns the task id. The
/// wrapper carries the derived identity exactly as
/// `WorkerSpawner::prepare` records it.
async fn seed_task_lineage(store: &EventStore, tag: &str) -> String {
    let parent = SessionId::new();
    let identity = kian_superagent::SpawnIdentity::derive(parent, 100, "worker", 7);
    let child = kian_superagent::child_session_id(identity.task_id);
    let wrapper = json!({
        "thread_id": identity.task_id_str(),
        "task": format!("task {tag}"),
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
    // The parent's dispatch side.
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
        started.clone(),
    )
    .await;
    // The child's own side, completed with a passed verdict.
    push(
        store,
        child,
        1,
        Epoch::GENESIS,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    push(
        store,
        child,
        2,
        Epoch(1),
        EventKind::DrainClaimed,
        json!({}),
    )
    .await;
    push(store, child, 3, Epoch(1), EventKind::ThreadStarted, started).await;
    push(
        store,
        child,
        4,
        Epoch(1),
        EventKind::InputAdmitted,
        json!({"role": "user", "content": wrapper}),
    )
    .await;
    push(
        store,
        child,
        5,
        Epoch(1),
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer"}),
    )
    .await;
    push(
        store,
        child,
        6,
        Epoch(1),
        EventKind::EvidenceVerdict,
        json!({"gate": "worker", "passed": true, "checks": []}),
    )
    .await;
    push(
        store,
        child,
        7,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": identity.task_id_str(), "status": "completed",
               "summary": {"role": "assistant", "content": "done"}}),
    )
    .await;
    // The parent's durable report row (pin P3-5's read).
    store
        .admit(kian_core::model::InputRow {
            id: uuid::Uuid::new_v4(),
            session: parent,
            payload: json!({
                "thread_id": identity.task_id_str(),
                "status": "completed",
                "summary": {"role": "assistant", "content": "done"},
            }),
            admitted_at: 1,
            eligible_at: 1,
            idempotency_key: Some(format!("spawn:{}", identity.spawn_uid)),
        })
        .await
        .expect("admit report");
    identity.task_id_str()
}

/// 🔴 R17 C4: view ≡ honest derivation, byte-for-byte at the same
/// store key — the `rebuild_equals_full_derivation` pattern applied to
/// the fifth view, on a multi-session store (a plain session, a ticket
/// worker and a delegation lineage).
#[tokio::test]
async fn task_board_rows_equal_the_honest_derivation_at_the_same_store_key() {
    let path = temp_db("task-equiv");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 3, 2, 9).await;
    let task = seed_task_lineage(&store, "equiv").await;

    let out = kian_cli::views::rebuild_views_with(&store, "builder", 1_000, true)
        .await
        .unwrap();
    assert!(!out.lease_held);
    assert_eq!(out.views, 5, "the four views + the task board");

    let entries = kian_cli::views::fresh_task_board(&store)
        .await
        .expect("fresh task board");
    let via_view: Vec<_> = entries.iter().map(|e| e.to_row().unwrap()).collect();
    let honest = kian_cli::commands::task_board_rows(&store).await.unwrap();
    assert_eq!(
        via_view, honest,
        "task view must equal the honest derivation"
    );
    assert_eq!(via_view.len(), 1);
    assert_eq!(via_view[0].task_id, task);
    assert_eq!(via_view[0].outcome, "completed_verified");
    // And the full rebuild agrees with the incremental one at the
    // same key (the same store, nothing appended between).
    kian_cli::views::rebuild_views_full_with(&store, "builder", 1_050, true)
        .await
        .unwrap();
    let entries = kian_cli::views::fresh_task_board(&store)
        .await
        .expect("fresh after the full rebuild");
    let via_full: Vec<_> = entries.iter().map(|e| e.to_row().unwrap()).collect();
    assert_eq!(via_full, honest, "full rebuild == incremental == honest");

    store.close().await;
    cleanup(&path);
}

/// 🔴 R17 C4 (`anchors.md` rule 4, pin P3-1): board off ⇒ no view, no
/// rows, no state — and the other four views are unaffected. Rows
/// published by an earlier ENABLED era are deleted by the next
/// off-tick (a disposable view: a later re-enable cannot serve a
/// stale era).
#[tokio::test]
async fn board_off_publishes_no_task_board_rows_and_no_view_state() {
    let path = temp_db("task-off");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 2, 1, 17).await;
    seed_task_lineage(&store, "off").await;

    // The default rebuild (board off): four views, no task board.
    let out = kian_cli::views::rebuild_views(&store, "builder", 1_000, false)
        .await
        .unwrap();
    assert_eq!(out.views, 4);
    assert_eq!(
        store.view_rows("task_board").await.unwrap(),
        Some(vec![]),
        "no task_board rows"
    );
    assert!(
        store.view_state("task_board").await.unwrap().is_none(),
        "no task_board state"
    );
    // The other four views are unaffected (rule 4's isolation).
    assert!(kian_cli::views::fresh_directory(&store).await.is_some());
    assert!(kian_cli::views::fresh_usage(&store).await.is_some());
    assert!(kian_cli::views::fresh_cache(&store).await.is_some());
    assert!(kian_cli::views::fresh_tickets(&store).await.is_some());

    // An enabled era publishes; the next OFF tick deletes those rows
    // and the state in the same publication.
    kian_cli::views::rebuild_views_with(&store, "builder", 1_100, true)
        .await
        .unwrap();
    assert!(kian_cli::views::fresh_task_board(&store).await.is_some());
    kian_cli::views::rebuild_views(&store, "builder", 1_200, false)
        .await
        .unwrap();
    assert_eq!(
        store.view_rows("task_board").await.unwrap(),
        Some(vec![]),
        "the stale era's rows are deleted"
    );
    assert!(
        store.view_state("task_board").await.unwrap().is_none(),
        "the stale era's state is deleted"
    );
    // And the four views survive the disposal untouched.
    assert!(kian_cli::views::fresh_tickets(&store).await.is_some());

    store.close().await;
    cleanup(&path);
}

/// 🔴 R17 C4 (READ-MODEL §6 freshness gate): an append after a rebuild
/// moves the store key — `fresh_task_board` reads `None` and the
/// caller falls back to the honest derivation.
#[tokio::test]
async fn a_stale_task_board_view_is_never_served() {
    let path = temp_db("task-stale");
    let store = EventStore::open(&path).await.unwrap();
    let sessions = seed_store(&store, 1, 1, 23).await;
    seed_task_lineage(&store, "stale").await;
    kian_cli::views::rebuild_views_with(&store, "builder", 1_000, true)
        .await
        .unwrap();
    assert!(kian_cli::views::fresh_task_board(&store).await.is_some());

    // ANY append makes the whole version stale.
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
    assert!(
        kian_cli::views::fresh_task_board(&store).await.is_none(),
        "a stale task board is never served"
    );
    // The honest derivation still answers.
    assert_eq!(
        kian_cli::commands::task_board_rows(&store)
            .await
            .unwrap()
            .len(),
        1
    );

    store.close().await;
    cleanup(&path);
}

/// R17 C4 (§6 rules:270-274): a tampered payload breaks the digest —
/// the view is discarded, never repaired by guessing.
#[tokio::test]
async fn a_corrupt_task_board_view_is_discarded_not_repaired() {
    let path = temp_db("task-corrupt");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 1, 1, 29).await;
    seed_task_lineage(&store, "corrupt").await;
    kian_cli::views::rebuild_views_with(&store, "builder", 1_000, true)
        .await
        .unwrap();

    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("UPDATE view_rows SET payload = ? WHERE view = 'task_board'")
            .bind(r#"{"task_id":"x","outcome":"completed_verified"}"#)
            .execute(pool)
            .await
            .unwrap();
    }
    assert!(
        kian_cli::views::fresh_task_board(&store).await.is_none(),
        "a corrupt task board is discarded"
    );

    store.close().await;
    cleanup(&path);
}

/// R17 C4 (the R9-10 reuse path): the incremental rebuild after a
/// task-board delta (a verdict landing on the child) produces the
/// same rows as the honest full derivation.
#[tokio::test]
async fn incremental_rebuild_equals_full_for_task_board() {
    let path = temp_db("task-delta");
    let store = EventStore::open(&path).await.unwrap();
    seed_store(&store, 2, 1, 31).await;
    let task = seed_task_lineage(&store, "delta").await;
    // First build WITHOUT the completion: seed_task_lineage already
    // completed the child, so instead delete the completion and
    // re-append it as the delta. (Behind the API's back — the
    // retention shape the incremental path must survive. The store's
    // only thread/completed events are this lineage's.)
    if let EventStore::Sqlite(pool) = &store {
        sqlx::query("DELETE FROM events WHERE kind = 'thread/completed'")
            .execute(pool)
            .await
            .unwrap();
    }
    kian_cli::views::rebuild_views_with(&store, "builder", 1_000, true)
        .await
        .unwrap();
    let entries = kian_cli::views::fresh_task_board(&store)
        .await
        .expect("fresh");
    let before: Vec<_> = entries.iter().map(|e| e.to_row().unwrap()).collect();
    assert_eq!(
        before[0].outcome, "outstanding",
        "the completion is gone from the stream"
    );

    // The delta: the child completes (a task-board kind event).
    let identity = kian_superagent::SpawnIdentity::derive(
        before[0]
            .parent
            .parse()
            .map(kian_core::model::SessionId)
            .unwrap(),
        100,
        "worker",
        7,
    );
    let child: kian_core::model::SessionId = before[0]
        .child
        .parse()
        .map(kian_core::model::SessionId)
        .unwrap();
    let head = store.head(child).await.unwrap().next_seq;
    push(
        &store,
        child,
        head,
        Epoch(1),
        EventKind::ThreadCompleted,
        json!({"thread_id": identity.task_id_str(), "status": "completed",
               "summary": {"role": "assistant", "content": "done"}}),
    )
    .await;

    let out = kian_cli::views::rebuild_views_with(&store, "builder", 1_200, true)
        .await
        .unwrap();
    assert!(!out.lease_held);
    let entries = kian_cli::views::fresh_task_board(&store)
        .await
        .expect("fresh after the delta");
    let via_view: Vec<_> = entries.iter().map(|e| e.to_row().unwrap()).collect();
    let honest = kian_cli::commands::task_board_rows(&store).await.unwrap();
    assert_eq!(via_view, honest, "incremental == honest after the delta");
    assert_eq!(via_view[0].task_id, task);
    assert_eq!(via_view[0].outcome, "completed_verified");

    store.close().await;
    cleanup(&path);
}
