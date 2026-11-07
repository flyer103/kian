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

criterion_group!(benches, bench_read_path);
criterion_main!(benches);
