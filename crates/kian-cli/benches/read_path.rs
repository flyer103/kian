//! Read-path benchmark (READ-MODEL P0, plan §4.1): the unchanged fast
//! path vs the changed read, and the SSE tail poll vs the pre-P0
//! full-rebuild poll, on 1k/10k-event sessions.
//!
//! ```bash
//! cargo bench -p kian-cli --bench read_path
//! ```
//!
//! Expectations (READ-MODEL §11 P0): the unchanged read costs one
//! head query (flat across session sizes); the SSE poll is O(new
//! events) — flat when idle and proportional to the delta — while the
//! legacy full rebuild grows linearly with history.

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_orchestrator::read::{events_after, versioned_projection};
use kian_orchestrator::snapshots::{
    ReadModelConfig, SnapshotBuildConfig, build_snapshot, load_projection,
};
use kian_store::{AppendGuard, EventStore};
use serde_json::json;
use uuid::Uuid;

/// Seeds one session with a projection-legal stream of `upto` events
/// (created → claimed → balanced turn pairs).
async fn seed(store: &EventStore, session: SessionId, upto: u64) {
    seed_from(store, session, 1, upto).await;
}

/// The tail half of [`seed`]: appends `from..=upto` as balanced turn
/// pairs (the continuation after a mid-history snapshot).
async fn seed_from(store: &EventStore, session: SessionId, from: u64, upto: u64) {
    for seq in from..=upto {
        let kind = if seq == 1 {
            EventKind::SessionCreated
        } else if seq == 2 {
            EventKind::DrainClaimed
        } else if seq % 2 == 1 {
            EventKind::TurnStart
        } else {
            EventKind::TurnEnd
        };
        let event = Event {
            seq,
            session,
            epoch: Epoch::GENESIS,
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes: json!({"n": seq}),
            version: SCHEMA_VERSION,
        };
        store
            .append(
                session,
                event,
                AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: seq,
                },
            )
            .await
            .expect("seed append");
    }
}

fn bench_read_path(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");

    let dir = std::env::temp_dir().join(format!("kian-bench-read-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("bench dir");

    let mut group = c.benchmark_group("read_path");
    group
        .sample_size(50)
        .measurement_time(Duration::from_secs(6));

    for upto in [1_000u64, 10_000] {
        // One db per size, WAL-checkpointed after seeding: the benches
        // measure steady-state reads, not seeding debris.
        let size_db = dir.join(format!("bench-{upto}.sqlite"));
        let (store, session, version) = runtime.block_on({
            let size_db = size_db.clone();
            async move {
                let store = EventStore::open(&size_db).await.expect("open store");
                let session = SessionId(Uuid::new_v4());
                seed(&store, session, upto).await;
                if let EventStore::Sqlite(pool) = &store {
                    let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                        .execute(pool)
                        .await;
                }
                let version = store.head(session).await.expect("head").next_seq - 1;
                (store, session, version)
            }
        });
        let label = format!("{}events", upto / 1_000);

        // R3: the unchanged read is one head query — no scan, no
        // projection, flat across history sizes.
        group.bench_function(format!("projection_unchanged_{label}"), |b| {
            b.iter(|| {
                runtime.block_on(versioned_projection(
                    &store,
                    session,
                    Some(criterion::black_box(version)),
                ))
            })
        });

        // The changed read: one head query + one bounded scan + the
        // full projection (grows with history — the cost the fast
        // path avoids).
        group.bench_function(format!("projection_changed_{label}"), |b| {
            b.iter(|| {
                runtime.block_on(versioned_projection(
                    &store,
                    session,
                    Some(criterion::black_box(version - 1)),
                ))
            })
        });

        // The SSE idle poll: head + the one-page tail scan (probe row
        // only) — O(1) regardless of history.
        group.bench_function(format!("sse_tail_poll_idle_{label}"), |b| {
            b.iter(|| runtime.block_on(events_after(&store, session, version)))
        });

        // The SSE poll with a small delta (5 new events): O(delta).
        group.bench_function(format!("sse_tail_poll_5_new_{label}"), |b| {
            b.iter(|| runtime.block_on(events_after(&store, session, version - 5)))
        });

        // The pre-P0 poll cost: a full-stream rebuild every cycle —
        // O(history).
        group.bench_function(format!("sse_legacy_full_rebuild_{label}"), |b| {
            b.iter(|| runtime.block_on(store.rebuild(session)))
        });

        // -- READ-MODEL P1: snapshot resume -------------------------------
        // A second session per size: snapshotted 100 events behind head,
        // so "resume" = one snapshot query + tail replay of (V, head].
        // The full-rebuild counterpart (the honest fallback path the
        // loader would take with snapshots disabled) shows what the
        // snapshot saves.
        {
            let snap_session = SessionId(Uuid::new_v4());
            {
                let store = store.clone();
                runtime.block_on(async {
                    seed(&store, snap_session, upto - 100).await;
                    build_snapshot(
                        &store,
                        snap_session,
                        &SnapshotBuildConfig {
                            min_events: 1,
                            ..SnapshotBuildConfig::default()
                        },
                        "bench",
                        0,
                    )
                    .await
                    .expect("bench snapshot build");
                    seed_from(&store, snap_session, upto - 99, upto).await;
                });
            }
            let version = runtime
                .block_on(store.head(snap_session))
                .expect("head")
                .next_seq
                - 1;
            let read_on = ReadModelConfig {
                snapshots_enabled: true,
                tail_cap: 8_192,
            };

            // Resume = snapshot + tail replay: state AND context at head
            // for O(tail) work (READ-MODEL §11 P1).
            group.bench_function(format!("resume_snapshot_tail_{label}"), |b| {
                b.iter(|| {
                    runtime.block_on(load_projection(
                        &store,
                        snap_session,
                        criterion::black_box(version),
                        &read_on,
                    ))
                })
            });

            // The same resume without snapshots: the full derivation
            // (scan + project + derive over the whole history).
            group.bench_function(format!("resume_full_derivation_{label}"), |b| {
                b.iter(|| {
                    runtime.block_on(load_projection(
                        &store,
                        snap_session,
                        criterion::black_box(version),
                        &ReadModelConfig::default(),
                    ))
                })
            });

            // The maintenance build cost (two derivations + digest +
            // insert): what one snapshot publication costs off the
            // serving path. The GC clears the row first so every
            // iteration measures a real build (not the UpToDate skip).
            group.bench_function(format!("snapshot_build_maintenance_{label}"), |b| {
                b.iter(|| {
                    runtime.block_on(async {
                        let _ = store.snapshot_gc(snap_session, 0).await;
                        build_snapshot(
                            &store,
                            snap_session,
                            &SnapshotBuildConfig {
                                min_events: 1,
                                ..SnapshotBuildConfig::default()
                            },
                            "bench",
                            0,
                        )
                        .await
                    })
                })
            });
        }
    }
    group.finish();

    let _ = std::fs::remove_dir_all(&dir);
}

/// READ-MODEL P2a (plan §4.3): the 1k-session operator-surface
/// benchmark — the ticket board and the usage aggregate served from
/// fresh materialized views vs the honest full per-stream derivation,
/// plus the one-off maintenance rebuild cost (off the serving path).
///
/// ```bash
/// cargo bench -p kian-cli --bench read_path -- views
/// ```
///
/// Store shape: 800 plain sessions (usage-bearing turns) + 200 worker
/// sessions (`thread/started` lineage) ≈ 5.2k events.
fn bench_views(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");

    let dir = std::env::temp_dir().join(format!("kian-bench-views-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("bench dir");
    let db = dir.join("bench-views.sqlite");

    let store = runtime.block_on(async {
        let store = EventStore::open(&db).await.expect("open store");
        for p in 0..800u64 {
            let sid = SessionId(Uuid::new_v4());
            seed_plain(&store, sid, p).await;
        }
        for w in 0..200u64 {
            let sid = SessionId(Uuid::new_v4());
            seed_worker(&store, sid, w).await;
        }
        if let EventStore::Sqlite(pool) = &store {
            let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(pool)
                .await;
        }
        // Build the views once; no further appends, so every read below
        // is fresh (the exact-version gate holds through the benches).
        kian_cli::views::rebuild_views(&store, "bench", 0)
            .await
            .expect("bench view rebuild");
        store
    });

    let mut group = c.benchmark_group("views");
    group
        .sample_size(30)
        .measurement_time(Duration::from_secs(6));

    let now = 1_762_000_060_000;

    // The board from the fresh view: two small queries + digest + decode.
    group.bench_function("board_from_view_1k_sessions", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let entries = kian_cli::views::fresh_tickets(&store)
                    .await
                    .expect("fresh view in a static store");
                criterion::black_box(
                    entries
                        .iter()
                        .map(|e| e.to_row(criterion::black_box(now)).unwrap())
                        .collect::<Vec<_>>(),
                )
            })
        })
    });

    // The honest fallback: every stream rebuilt, worker discovery, one
    // pending-inbox query per worker.
    group.bench_function("board_full_derivation_1k_sessions", |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::commands::ticket_board(
                &store,
                criterion::black_box(now),
            ))
        })
    });

    // The usage aggregate from the fresh view vs the full scan.
    group.bench_function("usage_from_view_1k_sessions", |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::usage_metrics::usage_stats_report(
                &store,
                None,
                criterion::black_box(true),
            ))
        })
    });
    group.bench_function("usage_full_derivation_1k_sessions", |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::usage_metrics::usage_stats_report(
                &store,
                None,
                criterion::black_box(false),
            ))
        })
    });

    // One maintenance rebuild of all four views (the off-serving-path
    // cost the ticker pays per beat at this store size).
    group.bench_function("rebuild_maintenance_1k_sessions", |b| {
        b.iter(|| runtime.block_on(kian_cli::views::rebuild_views(&store, "bench", 0)))
    });

    group.finish();
    runtime.block_on(store.close());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A plain session: created → input → claimed → two usage-bearing turns.
async fn seed_plain(store: &EventStore, sid: SessionId, p: u64) {
    for (seq, (kind, bytes)) in [
        (EventKind::SessionCreated, json!({})),
        (
            EventKind::InputAdmitted,
            json!({"role": "user", "content": format!("p{p}")}),
        ),
        (EventKind::DrainClaimed, json!({})),
        (
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer", "usage": {"prompt_tokens": 11, "completion_tokens": 2}}),
        ),
        (
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer", "usage": {"prompt_tokens": 7, "completion_tokens": 1}}),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let seq = seq as u64 + 1;
        store
            .append(
                sid,
                Event {
                    seq,
                    session: sid,
                    epoch: Epoch(1),
                    ts: 1_762_000_000_000 + seq,
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
            .expect("seed append");
    }
}

/// A worker session: created → claimed → thread/started (self-named
/// worker lineage) → two turns → one input (the queued arm).
async fn seed_worker(store: &EventStore, sid: SessionId, w: u64) {
    let specs = vec![
        (EventKind::SessionCreated, json!({})),
        (EventKind::DrainClaimed, json!({})),
        (
            EventKind::ThreadStarted,
            json!({"worker": sid.0.to_string(), "thread_id": uuid::Uuid::new_v4().to_string()}),
        ),
        (
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer", "usage": {"prompt_tokens": 9 + w % 7, "completion_tokens": 1}}),
        ),
        (
            EventKind::TurnEnd,
            json!({"iteration": 0, "outcome": "final_answer", "usage": {"prompt_tokens": 5, "completion_tokens": 1}}),
        ),
        (
            EventKind::InputAdmitted,
            json!({"role": "user", "content": format!("w{w}")}),
        ),
    ];
    for (i, (kind, bytes)) in specs.into_iter().enumerate() {
        let seq = i as u64 + 1;
        store
            .append(
                sid,
                Event {
                    seq,
                    session: sid,
                    epoch: Epoch(1),
                    ts: 1_762_000_000_000 + seq,
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
            .expect("seed append");
    }
}

criterion_group!(benches, bench_read_path, bench_views);
criterion_main!(benches);
