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
    bench_views_at(c, "1k_sessions", 800, 200, 30, Duration::from_secs(6));
}

/// R9-10: the same read benches at 10k sessions (8 000 plain + 2 000
/// workers ≈ 52k events) — the incremental-tick benches here are the
/// acceptance numbers (per-tick work proportional to *changed*
/// sessions, at both store sizes).
fn bench_views_10k(c: &mut Criterion) {
    bench_views_at(c, "10k_sessions", 8_000, 2_000, 20, Duration::from_secs(5));
}

fn bench_views_at(
    c: &mut Criterion,
    tag: &str,
    plain: u64,
    workers: u64,
    sample_size: usize,
    measurement_time: Duration,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");

    let dir = std::env::temp_dir().join(format!("kian-bench-views-{tag}-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("bench dir");
    let db = dir.join("bench-views.sqlite");

    let now = 1_762_000_060_000;

    let store = runtime.block_on(async {
        let store = EventStore::open(&db).await.expect("open store");
        let mut plain_sessions = Vec::new();
        for p in 0..plain {
            let sid = SessionId(Uuid::new_v4());
            seed_plain(&store, sid, p).await;
            plain_sessions.push(sid);
        }
        let mut worker_sessions = Vec::new();
        for w in 0..workers {
            let sid = SessionId(Uuid::new_v4());
            seed_worker(&store, sid, w).await;
            worker_sessions.push(sid);
        }
        if let EventStore::Sqlite(pool) = &store {
            let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(pool)
                .await;
        }
        // Build the views once (the first build is a full rebuild); no
        // further appends outside the changed-tick benches, so the
        // read benches below stay fresh (the exact-version gate holds).
        kian_cli::views::rebuild_views(&store, "bench", 0, false)
            .await
            .expect("bench view rebuild");
        (store, plain_sessions, worker_sessions)
    });
    let (store, plain_sessions, worker_sessions) = store;

    let mut group = c.benchmark_group("views");
    group
        .sample_size(sample_size)
        .measurement_time(measurement_time);

    // The board from the fresh view: two small queries + digest + decode.
    group.bench_function(format!("board_from_view_{tag}"), |b| {
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
    group.bench_function(format!("board_full_derivation_{tag}"), |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::commands::ticket_board(
                &store,
                criterion::black_box(now),
            ))
        })
    });

    // The usage aggregate from the fresh view vs the full scan.
    group.bench_function(format!("usage_from_view_{tag}"), |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::usage_metrics::usage_stats_report(
                &store,
                None,
                criterion::black_box(true),
            ))
        })
    });
    group.bench_function(format!("usage_full_derivation_{tag}"), |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::usage_metrics::usage_stats_report(
                &store,
                None,
                criterion::black_box(false),
            ))
        })
    });

    // The pre-R9-10 maintenance beat: one FULL rebuild of all four
    // views (every stream re-read and re-folded) — the baseline the
    // incremental ticks are measured against.
    group.bench_function(format!("rebuild_full_{tag}"), |b| {
        b.iter(|| {
            runtime.block_on(kian_cli::views::rebuild_views_full(
                &store, "bench", 0, false,
            ))
        })
    });

    // R9-10: the incremental tick on an UNCHANGED store — one heads
    // scan, one pending scan, four digest verifications, zero folds.
    group.bench_function(format!("rebuild_incremental_idle_{tag}"), |b| {
        b.iter(|| runtime.block_on(kian_cli::views::rebuild_views(&store, "bench", 0, false)))
    });

    // R9-10: the incremental tick after 10 plain sessions grew by one
    // event each (a non-board change: the ticket rows are republished
    // verbatim, the board is not re-folded). The appends happen
    // outside the timed window (iter_custom).
    {
        let changed: Vec<SessionId> = plain_sessions.iter().take(10).copied().collect();
        group.bench_function(format!("rebuild_incremental_10_changed_{tag}"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    runtime.block_on(async {
                        for sid in &changed {
                            append_turn_end(&store, *sid).await;
                        }
                    });
                    let start = std::time::Instant::now();
                    let _ =
                        runtime.block_on(kian_cli::views::rebuild_views(&store, "bench", 0, false));
                    total += start.elapsed();
                }
                total
            })
        });
    }

    // R9-10: the incremental tick after ONE worker session grew (a
    // board-relevant change: the whole board re-derives from the
    // kind-filtered read + worker streams, not from every stream).
    {
        let worker = worker_sessions[0];
        group.bench_function(format!("rebuild_incremental_1_board_changed_{tag}"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    runtime.block_on(async {
                        append_turn_end(&store, worker).await;
                    });
                    let start = std::time::Instant::now();
                    let _ =
                        runtime.block_on(kian_cli::views::rebuild_views(&store, "bench", 0, false));
                    total += start.elapsed();
                }
                total
            })
        });
    }

    group.finish();
    runtime.block_on(store.close());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Appends one usage-bearing `turn/end` to a session (the bench's
/// per-tick change; sessions grow across samples, exactly what a live
/// store looks like to successive ticks).
async fn append_turn_end(store: &EventStore, sid: SessionId) {
    let head = store.head(sid).await.expect("head").next_seq;
    store
        .append(
            sid,
            Event {
                seq: head,
                session: sid,
                epoch: Epoch(1),
                ts: 1_762_000_000_000 + head,
                kind: EventKind::TurnEnd,
                bytes: json!({"iteration": 0, "outcome": "final_answer",
                              "usage": {"prompt_tokens": 3, "completion_tokens": 1}}),
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: head,
            },
        )
        .await
        .expect("bench append");
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

criterion_group!(benches, bench_read_path, bench_views, bench_views_10k);
criterion_main!(benches);
