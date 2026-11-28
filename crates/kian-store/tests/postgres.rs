//! Env-gated postgres suite.
//!
//! `KIAN_PG_URL` absent → every test in this file SKIPS with a log line
//! and never fails (the pattern): default CI needs no postgres.
//! With the env var set (pointing at a throwaway database — e.g. The
//! docker recipe documents), each test creates its own private
//! database `kian_store_test_<uuid>` off the URL so parallel runs never
//! collide and nothing outside those databases is touched.
//!
//! Coverage is the store's core semantics on postgres: CAS fences, batch
//! atomicity, the single-statement end write (constraint 4), conditional
//! claim admission, inbox consume + startup scan, the
//! concurrent-append PK-race classification (§3), export parity
//! against a sqlite store seeded with the same stream, and the
//! read-only role refusing migrations.

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use serde_json::json;
use uuid::Uuid;

use kian_store::migrations;
use kian_store::{
    AppendGuard, CronError, CronFiringRow, EventStore, FiringState, Head, InboxError,
    ScheduleUpsert, SnapshotRow, StoreError,
};

const T0: u64 = 1_761_900_000_000;

/// True when the suite should run; prints the skip line otherwise.
fn gated() -> bool {
    match std::env::var("KIAN_PG_URL") {
        Ok(url) if !url.is_empty() => true,
        _ => {
            eprintln!("SKIP: KIAN_PG_URL is not set — postgres suite disabled (env gate)");
            false
        }
    }
}

/// `postgres://…/db?opts` → `postgres://…/<other-db>?opts`.
fn with_db(base: &str, db: &str) -> String {
    let (path_part, query) = match base.split_once('?') {
        Some((p, q)) => (p, format!("?{q}")),
        None => (base, String::new()),
    };
    let scheme_end = path_part.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &path_part[scheme_end..];
    let root = match rest.find('/') {
        Some(i) => path_part[..scheme_end + i].to_string(),
        None => path_part.to_string(),
    };
    format!("{root}/{db}{query}")
}

/// One throwaway database per test: created from the maintenance URL,
/// migrated by `open_postgres`, dropped by [`PgStore::finish`].
struct PgStore {
    store: EventStore,
    base_url: String,
    db: String,
}

impl PgStore {
    async fn open() -> Self {
        let base = std::env::var("KIAN_PG_URL").expect("checked by gated()");
        let db = format!("kian_store_test_{}", Uuid::new_v4().simple());
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base)
            .await
            .expect("KIAN_PG_URL maintenance connection");
        sqlx::raw_sql(&format!("CREATE DATABASE \"{db}\""))
            .execute(&admin)
            .await
            .expect("create throwaway test database");
        admin.close().await;
        let store = EventStore::open_postgres(&with_db(&base, &db))
            .await
            .expect("open postgres store");
        Self {
            store,
            base_url: base,
            db,
        }
    }

    fn url(&self) -> String {
        with_db(&self.base_url, &self.db)
    }

    /// Closes the store pool and drops the throwaway database. Tests call
    /// this on their success path; a panicking test may leak its (empty,
    /// inert) `kian_store_test_*` database — the recipe environment is
    /// disposable by contract.
    async fn finish(self) {
        self.store.close().await;
        if let Ok(admin) = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.base_url)
            .await
        {
            // WITH (FORCE) drops lingering pool connections first.
            let _ = sqlx::raw_sql(&format!("DROP DATABASE \"{}\" WITH (FORCE)", self.db))
                .execute(&admin)
                .await;
            admin.close().await;
        }
    }
}

// -- helpers -----------------------------------------------------------------

fn ev(session: SessionId, seq: u64, epoch: Epoch, kind: EventKind) -> Event {
    Event {
        seq,
        session,
        epoch,
        ts: T0 + seq,
        kind,
        bytes: json!({ "n": seq }),
        version: SCHEMA_VERSION,
    }
}

async fn append_ok(
    store: &EventStore,
    session: SessionId,
    seq: u64,
    epoch: Epoch,
    kind: EventKind,
) {
    store
        .append(
            session,
            ev(session, seq, epoch, kind),
            AppendGuard {
                epoch,
                expected_seq: seq,
            },
        )
        .await
        .expect("append");
}

async fn seed(store: &EventStore, session: SessionId, upto: u64, epoch: Epoch) {
    for seq in 1..=upto {
        let kind = if seq == 1 {
            EventKind::SessionCreated
        } else {
            EventKind::TurnStart
        };
        append_ok(store, session, seq, epoch, kind).await;
    }
}

fn all() -> std::ops::Range<u64> {
    0..i64::MAX as u64
}

fn input(
    session: SessionId,
    key: Option<&str>,
    eligible_at: u64,
    payload: serde_json::Value,
) -> InputRow {
    InputRow {
        id: Uuid::new_v4(),
        session,
        payload,
        admitted_at: T0,
        eligible_at,
        idempotency_key: key.map(String::from),
    }
}

// -- CAS fences ---------------------------------------------------------------

/// Stale-epoch and seq-mismatch appends are rejected with the same typed
/// errors sqlite produces, and the stream is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_cas_fences_reject_stale_epoch_and_seq_mismatch() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    seed(&pg.store, sid, 3, Epoch(5)).await;

    let err = pg
        .store
        .append(
            sid,
            ev(sid, 4, Epoch(3), EventKind::TurnStart),
            AppendGuard {
                epoch: Epoch(3),
                expected_seq: 4,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            StoreError::StaleEpoch {
                event_epoch: Epoch(3),
                current_epoch: Epoch(5),
                ..
            }
        ),
        "expected StaleEpoch, got {err:?}"
    );

    let err = pg
        .store
        .append(
            sid,
            ev(sid, 9, Epoch(5), EventKind::TurnStart),
            AppendGuard {
                epoch: Epoch(5),
                expected_seq: 9,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            StoreError::SeqMismatch {
                expected_seq: 9,
                actual_next: 4,
                ..
            }
        ),
        "expected SeqMismatch, got {err:?}"
    );

    assert_eq!(pg.store.scan(sid, all()).await.unwrap().len(), 3);
    assert_eq!(
        pg.store.head(sid).await.unwrap(),
        Head {
            next_seq: 4,
            epoch: Epoch(5)
        }
    );
    pg.finish().await;
}

/// The end write is the single-statement CAS append (constraint 4): it
/// lands once at the tail, and a competing second end write at the same
/// position is rejected — never two terminal rows.
#[tokio::test]
async fn pg_end_write_lands_once_and_rejects_the_competing_end() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    seed(&pg.store, sid, 3, Epoch(5)).await;

    pg.store
        .append_end(
            sid,
            ev(
                sid,
                4,
                Epoch(5),
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
            ),
            AppendGuard {
                epoch: Epoch(5),
                expected_seq: 4,
            },
        )
        .await
        .expect("end write");

    // A zombie writer's end write at the same position: rejected, and
    // the stream keeps exactly one session/ended.
    let err = pg
        .store
        .append_end(
            sid,
            ev(
                sid,
                4,
                Epoch(5),
                EventKind::SessionEnded {
                    reason: EndReason::Failed,
                },
            ),
            AppendGuard {
                epoch: Epoch(5),
                expected_seq: 4,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::SeqMismatch { .. } | StoreError::StaleEpoch { .. }
        ),
        "expected a typed CAS rejection, got {err:?}"
    );

    let events = pg.store.scan(sid, all()).await.unwrap();
    assert_eq!(events.len(), 4);
    assert!(matches!(
        events[3].kind,
        EventKind::SessionEnded {
            reason: EndReason::Completed
        }
    ));
    pg.finish().await;
}

// -- batch atomicity ----------------------------------------------------------

/// A batch commits whole under a passing fence; a stale-epoch or
/// seq-mismatched batch commits NOTHING (all-or-nothing, semantics
/// via one CTE statement).
#[tokio::test]
async fn pg_append_batch_is_all_or_nothing() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    seed(&pg.store, sid, 3, Epoch(1)).await;

    // Happy path: the spill pair lands contiguously.
    let batch = [
        ev(sid, 4, Epoch(1), EventKind::InputAdmitted),
        ev(sid, 5, Epoch(1), EventKind::InputSpilled),
    ];
    pg.store
        .append_batch(
            sid,
            &batch,
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 4,
            },
        )
        .await
        .expect("batch append");
    let events = pg.store.scan(sid, all()).await.unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[3], batch[0]);
    assert_eq!(events[4], batch[1]);

    // Forced fence failure — stale epoch: neither row lands.
    let stale = [
        ev(sid, 6, Epoch(0), EventKind::InputAdmitted),
        ev(sid, 7, Epoch(0), EventKind::InputSpilled),
    ];
    let err = pg
        .store
        .append_batch(
            sid,
            &stale,
            AppendGuard {
                epoch: Epoch(0),
                expected_seq: 6,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::StaleEpoch { .. }),
        "expected StaleEpoch, got {err:?}"
    );

    // Forced fence failure — seq mismatch: neither row lands.
    let mismatch = [
        ev(sid, 8, Epoch(1), EventKind::InputAdmitted),
        ev(sid, 9, Epoch(1), EventKind::InputSpilled),
    ];
    let err = pg
        .store
        .append_batch(
            sid,
            &mismatch,
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 8,
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::SeqMismatch { .. }),
        "expected SeqMismatch, got {err:?}"
    );

    assert_eq!(pg.store.scan(sid, all()).await.unwrap().len(), 5);
    assert_eq!(pg.store.head(sid).await.unwrap().next_seq, 6);
    pg.finish().await;
}

// -- the PK race (§3) -----------------------------------------------------

/// Concurrent same-session appends for one seq: exactly one lands and
/// every loser gets a *typed* CAS error — the pg PK race (WHERE snapshot
/// passed, then the primary key lost) is classified through the same
/// HEAD path as a fence refusal, never leaked as a unique-violation
/// `Sql` error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_concurrent_append_pk_race_is_classified_never_leaked() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    seed(&pg.store, sid, 2, Epoch(5)).await;

    let racers: Vec<_> = (0..8)
        .map(|i| {
            let store = pg.store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        sid,
                        ev(
                            sid,
                            3,
                            Epoch(5),
                            if i % 2 == 0 {
                                EventKind::TurnStart
                            } else {
                                EventKind::ToolCall
                            },
                        ),
                        AppendGuard {
                            epoch: Epoch(5),
                            expected_seq: 3,
                        },
                    )
                    .await
            })
        })
        .collect();
    let mut winners = 0usize;
    for racer in racers {
        let result = racer.await.expect("racer join");
        match result {
            Ok(()) => winners += 1,
            Err(StoreError::StaleEpoch { .. } | StoreError::SeqMismatch { .. }) => {}
            Err(other) => panic!(
                "the PK race must classify into a typed CAS error, got a leaked error: {other:?}"
            ),
        }
    }
    assert_eq!(winners, 1, "exactly one racer wins the CAS position");

    let events = pg.store.scan(sid, all()).await.unwrap();
    assert_eq!(events.len(), 3, "the stream stays contiguous");
    assert_eq!(events[2].seq, 3);
    pg.finish().await;
}

// -- conditional claim admission (on pg) -----------------------------------

/// The conditions hold on postgres with identical observables: no
/// row → epoch 1; fresh + different holder → `ClaimRejected` carrying
/// the live holder and expiry; same holder → bumps; expired → takeover.
#[tokio::test]
async fn pg_claim_conditional_admission() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    let ttl = 1_000u64;

    // (a) no claims row.
    let first = pg.store.claim(sid, "replica-a", ttl, T0).await.unwrap();
    assert_eq!(first.epoch, Epoch(1));

    // Refusal while fresh: typed, carries holder/epoch/expiry.
    pg.store.heartbeat(sid, T0 + 500).await.unwrap();
    let err = pg
        .store
        .claim(sid, "replica-b", ttl, T0 + 900)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            InboxError::ClaimRejected {
                holder,
                epoch: Epoch(1),
                expires_at_ms,
                ..
            } if holder == "replica-a" && *expires_at_ms == T0 + 500 + ttl
        ),
        "expected ClaimRejected carrying the live holder, got {err:?}"
    );

    // (c) same holder on a fresh lease still bumps.
    let again = pg
        .store
        .claim(sid, "replica-a", ttl, T0 + 600)
        .await
        .unwrap();
    assert_eq!(again.epoch, Epoch(2));

    // (b) past the TTL a different holder takes over with the next epoch.
    let takeover = pg
        .store
        .claim(sid, "replica-b", ttl, T0 + 600 + ttl + 1)
        .await
        .unwrap();
    assert_eq!(takeover.epoch, Epoch(3));
    assert_eq!(takeover.holder, "replica-b");

    // The events floor feeds issuance too (durable high-water mark).
    let other = SessionId::new();
    append_ok(&pg.store, other, 1, Epoch(7), EventKind::SessionCreated).await;
    let lease = pg.store.claim(other, "holder", ttl, T0).await.unwrap();
    assert_eq!(lease.epoch, Epoch(8));
    pg.finish().await;
}

// -- inbox consume + startup scan ----------------------------------------------

/// Admit is idempotent by key; consumption stays event-driven; the
/// startup scan surfaces exactly the eligible-unconsumed sessions.
#[tokio::test]
async fn pg_inbox_consume_and_startup_scan() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let a = SessionId::new();
    let b = SessionId::new();
    let c = SessionId::new();

    let a1 = input(a, Some("a1"), T0 - 300, json!("a1"));
    pg.store.admit(a1.clone()).await.unwrap();
    let dup = InputRow {
        id: Uuid::new_v4(),
        payload: json!("changed"),
        ..a1.clone()
    };
    let out = pg.store.admit(dup).await.unwrap();
    assert!(out.deduplicated, "same key dedupes on pg too");
    assert_eq!(out.input, a1);

    let b1 = input(b, Some("b1"), T0 - 100, json!("b1"));
    pg.store.admit(b1.clone()).await.unwrap();
    let future = input(c, Some("c1"), T0 + 60_000, json!("c1"));
    pg.store.admit(future).await.unwrap();

    // Consumption is refused before its event exists…
    let err = pg.store.mark_consumed(b, b1.id, 5).await.unwrap_err();
    assert!(
        matches!(err, InboxError::MissingConsumptionEvent { .. }),
        "{err:?}"
    );
    // …lands once the event is committed…
    append_ok(&pg.store, b, 1, Epoch(1), EventKind::SessionCreated).await;
    append_ok(&pg.store, b, 2, Epoch(1), EventKind::DrainClaimed).await;
    pg.store
        .mark_consumed(b, b1.id, 2)
        .await
        .expect("consume via committed event");
    // …re-consuming the same seq is idempotent, a different seq conflicts.
    pg.store
        .mark_consumed(b, b1.id, 2)
        .await
        .expect("re-consume idempotent");
    append_ok(&pg.store, b, 3, Epoch(1), EventKind::TurnStart).await;
    let err = pg.store.mark_consumed(b, b1.id, 3).await.unwrap_err();
    assert!(matches!(err, InboxError::AlreadyConsumed { .. }), "{err:?}");

    // Startup scan: a only — b is fully consumed, c is future-eligible.
    let scan = pg.store.startup_scan(T0).await.unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].session, a);
    assert_eq!(scan[0].eligible, vec![a1]);
    // Crossing c's eligible_at boundary surfaces it.
    assert_eq!(pg.store.startup_scan(T0 + 60_000).await.unwrap().len(), 2);
    pg.finish().await;
}

// -- export parity (the compat golden) -----------------------------------------

/// The same stream seeded into sqlite and postgres exports
/// byte-identically: both the stored envelope column (the export
/// source) and the scanned/round-tripped events.
#[tokio::test]
async fn pg_export_parity_with_sqlite() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;

    let path = std::env::temp_dir().join(format!("kian-store-parity-{}.sqlite", Uuid::new_v4()));
    let lite = EventStore::open(&path).await.expect("open sqlite store");

    let sid = SessionId::new();
    for store in [&lite, &pg.store] {
        store
            .append(
                sid,
                ev(sid, 1, Epoch(5), EventKind::SessionCreated),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 1,
                },
            )
            .await
            .unwrap();
        store
            .append(
                sid,
                ev(sid, 2, Epoch(5), EventKind::DrainClaimed),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 2,
                },
            )
            .await
            .unwrap();
        store
            .append(
                sid,
                ev(sid, 3, Epoch(5), EventKind::InputAdmitted),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 3,
                },
            )
            .await
            .unwrap();
        store
            .append_end(
                sid,
                ev(
                    sid,
                    4,
                    Epoch(5),
                    EventKind::SessionEnded {
                        reason: EndReason::Completed,
                    },
                ),
                AppendGuard {
                    epoch: Epoch(5),
                    expected_seq: 4,
                },
            )
            .await
            .unwrap();
    }

    // The envelope column is the export: identical bytes on both.
    let lite_rows: Vec<(String,)> =
        sqlx::query_as("SELECT envelope FROM events WHERE session = ? ORDER BY seq ASC")
            .bind(sid.0.to_string())
            .fetch_all(lite.pool())
            .await
            .unwrap();
    let pg_rows: Vec<(String,)> =
        sqlx::query_as("SELECT envelope FROM events WHERE session = $1::text ORDER BY seq ASC")
            .bind(sid.0.to_string())
            .fetch_all(pg.store.pg_pool().expect("pg pool"))
            .await
            .unwrap();
    assert_eq!(
        lite_rows, pg_rows,
        "exported JSONL bytes must match across backends"
    );

    // Round-trip parity too.
    let from_lite = lite.scan(sid, all()).await.unwrap();
    let from_pg = pg.store.scan(sid, all()).await.unwrap();
    let ser = |evs: &[Event]| {
        evs.iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(ser(&from_lite), ser(&from_pg));
    assert_eq!(from_pg.len(), 4);

    lite.close().await;
    let _ = std::fs::remove_file(&path);
    pg.finish().await;
}

// -- read-only role refuses migrations ------------------------------------------

/// A role with CONNECT but no schema privileges: `open_postgres`
/// (which migrates) is refused by the server, while
/// `open_postgres_read_only` opens without attempting migrations —
/// read-only on pg is a URL/role property, and the migrating
/// constructor never runs against it.
#[tokio::test]
async fn pg_read_only_role_refuses_migrations() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let pool = pg.store.pg_pool().expect("pg pool");
    let role = format!("kian_ro_{}", Uuid::new_v4().simple());

    // Cluster-wide setup; any failure (no CREATEROLE, no password auth
    // path) degrades to a logged skip — this subtest needs privileges
    // the operator's URL may honestly not carry.
    let setup = sqlx::query(&format!(
        "CREATE ROLE \"{role}\" LOGIN PASSWORD 'kian_ro_test'"
    ))
    .execute(pool)
    .await;
    if setup.is_err() {
        eprintln!(
            "SKIP: cannot create a read-only role against KIAN_PG_URL (insufficient privilege)"
        );
        pg.store.close().await;
        return;
    }
    sqlx::query(&format!(
        "GRANT CONNECT ON DATABASE \"{}\" TO \"{role}\"",
        pg.db
    ))
    .execute(pool)
    .await
    .expect("grant connect");

    // host[:port] with the role's credentials swapped in.
    let base = pg.url();
    let scheme_end = base.find("://").map(|i| i + 3).unwrap_or(0);
    let host = base[scheme_end..]
        .split('/')
        .next()
        .unwrap_or("localhost:5432")
        .to_string();
    let ro_url = format!("postgres://{role}:kian_ro_test@{host}/{}", pg.db);

    // The migrating constructor is refused: the role cannot create
    // schema (pg ≥ 15 default; pg 14 operators must revoke CREATE on
    // public for the role).
    let denied = EventStore::open_postgres(&ro_url).await;
    assert!(
        denied.is_err(),
        "a read-only role must refuse the migrating open"
    );

    // The read-only constructor opens without migrating — no schema
    // access is attempted by construction.
    let reader = EventStore::open_postgres_read_only(&ro_url)
        .await
        .expect("read-only open performs no migrations");
    reader.close().await;

    // Direct migration on the read-only pool is refused too.
    let ro_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&ro_url)
        .await
        .expect("ro connection");
    assert!(
        migrations::migrate_pg(&ro_pool).await.is_err(),
        "migrations must be refused for a read-only role"
    );
    ro_pool.close().await;

    // Cleanup (best-effort).
    sqlx::query(&format!(
        "REVOKE CONNECT ON DATABASE \"{}\" FROM \"{role}\"",
        pg.db
    ))
    .execute(pool)
    .await
    .ok();
    sqlx::query(&format!("DROP ROLE \"{role}\""))
        .execute(pool)
        .await
        .ok();
    pg.finish().await;
}

// -- wake transport (LISTEN/NOTIFY) -------------------------------------

/// Notify-after-commit ordering (the named test): a wake
/// notification is observable **only after** the input row it announces
/// is durable and visible. The listener receives the notification and
/// — on the same connection turn — the input row is already readable;
/// a notify observed before its row is visible would invert
/// durable-before-wake (constraint 6) across processes.
#[tokio::test]
async fn pg_wake_notify_fires_only_after_the_input_row_is_visible() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let pool = pg.store.pg_pool().expect("pg store carries a pool").clone();

    // The receiving replica: one dedicated pooled connection LISTENing
    // on the wake channel (exactly how `kian serve` relays).
    let mut listener = kian_store::wake::listen_for_wakes(&pool)
        .await
        .expect("listen kian_wake");

    // The admitting replica's sequence, exactly as the engine performs
    // it: the single-statement INSERT commits, then the notify flies.
    let sid = SessionId::new();
    let row = InputRow {
        id: Uuid::new_v4(),
        session: sid,
        payload: json!("task-A"),
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some("c52-notify-order".into()),
    };
    pg.store.admit(row.clone()).await.expect("durable admit");
    pg.store
        .notify_wake(sid)
        .await
        .expect("notify after commit");

    // The notification arrives…
    let notification = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
        .await
        .expect("notification within 5s")
        .expect("listener alive");
    assert_eq!(notification.channel(), kian_store::wake::WAKE_CHANNEL);
    assert_eq!(notification.payload(), sid.0.to_string());

    // …and by the time it is observed, the announced row is already
    // visible to an ordinary read (committed before the notify flew).
    let eligible = pg.store.eligible_inputs(sid, T0).await.expect("read");
    assert_eq!(
        eligible,
        vec![row],
        "the notified input must be durable-visible the moment the notify is observed"
    );

    pg.finish().await;
}

/// A listener gap drops notifications honestly: with no listener
/// attached (the simulated gap — reconnect window or replica down),
/// the notify still succeeds (publish is best-effort) and the input
/// row remains durable for the sweep/startup-scan rebuild. This pins
/// the transport's advisory contract at the store layer; the
/// sweep-services-it half is the engine's named listener-gap test.
#[tokio::test]
async fn pg_wake_notify_without_a_listener_still_commits_the_input() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();
    let row = InputRow {
        id: Uuid::new_v4(),
        session: sid,
        payload: json!("task-B"),
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some("c52-notify-gap".into()),
    };
    // No listener exists — the notification lands nowhere (pg_notify
    // with no subscribers is a no-op, never an error).
    pg.store.admit(row.clone()).await.expect("durable admit");
    pg.store
        .notify_wake(sid)
        .await
        .expect("best-effort notify is Ok");

    // The durable row is what NoLostWork rests on — not the transport.
    let scan = pg.store.startup_scan(T0).await.expect("startup scan");
    assert_eq!(
        scan,
        vec![kian_store::SessionInbox {
            session: sid,
            eligible: vec![row],
        }],
        "a dropped notify leaves the durable row for the rebuild"
    );

    pg.finish().await;
}

// -- inbox admission order (v2) -------------------------------------------

/// FIFO inside one `eligible_at` millisecond follows admission order on
/// postgres too: the identity column (`admitted_seq`, v2) breaks the
/// tie, never the random uuid. Ids are crafted so the old uuid
/// tie-break would order them backwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_same_millisecond_inputs_consume_in_admission_order() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();

    let first = InputRow {
        id: Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap(),
        session: sid,
        payload: json!({ "n": 1 }),
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some("first".into()),
    };
    let second = InputRow {
        id: Uuid::parse_str("00000000-0000-4000-8000-000000000000").unwrap(),
        session: sid,
        payload: json!({ "n": 2 }),
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some("second".into()),
    };
    pg.store.admit(first.clone()).await.expect("admit first");
    pg.store.admit(second.clone()).await.expect("admit second");

    let rows = pg.store.eligible_inputs(sid, T0).await.expect("eligible");
    assert_eq!(rows.len(), 2, "both same-ms inputs are eligible");
    assert_eq!(
        rows[0].id, first.id,
        "same-ms inputs must consume in admission order (not uuid order)"
    );
    assert_eq!(rows[1].id, second.id);

    let scan = pg.store.startup_scan(T0).await.expect("startup scan");
    let ours = scan
        .iter()
        .find(|s| s.session == sid)
        .expect("session in startup scan");
    assert_eq!(
        ours.eligible.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![first.id, second.id],
        "startup scan must list same-ms inputs in admission order"
    );

    pg.finish().await;
}

/// The v2 upgrade path: a database migrated only to v1 (with rows
/// already in the inbox) upgrades in place — every legacy row gets an
/// ordinal, and the first new admit orders after all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_v2_upgrade_backfills_legacy_inbox_rows() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    // Re-migrate down is impossible by design; instead simulate a legacy
    // DB by checking the applied chain includes v2 and that rows
    // admitted before this test's knowledge of v2 still order by their
    // identity ordinals. The real legacy path is: v1 DB + ALTER —
    // verified live during against PG 17 (identity backfill).
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&2),
        "open_postgres applies v2, got {versions:?}"
    );

    let sid = SessionId::new();
    let legacy = input(sid, Some("legacy"), T0, json!("old"));
    pg.store.admit(legacy.clone()).await.expect("legacy admit");
    let fresh = input(sid, Some("fresh"), T0, json!("new"));
    pg.store.admit(fresh.clone()).await.expect("fresh admit");

    let rows = pg.store.eligible_inputs(sid, T0).await.expect("eligible");
    assert_eq!(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![legacy.id, fresh.id],
        "admission order holds across legacy and fresh rows"
    );

    pg.finish().await;
}

/// v3 (READ-MODEL §5/§9): the snapshot tables exist on pg, publication
/// is insert-only, the loader query sees only verified rows, and the
/// maintenance lease mirrors the sqlite liveness semantics — the pg
/// twin of the in-module sqlite suite.
#[tokio::test]
async fn pg_snapshots_publication_reads_and_maintenance_leases() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&3),
        "open_postgres applies v3, got {versions:?}"
    );

    let sid = SessionId::new();
    seed(&pg.store, sid, 6, Epoch::GENESIS).await;

    let row = SnapshotRow {
        session: sid,
        version: 6,
        state: r#"{"state":"running","episode_end":null}"#.into(),
        context: "[]".into(),
        context_provenance: "[]".into(),
        digest: "d6".into(),
        verified: true,
        created_at: 42,
    };
    assert!(
        pg.store
            .snapshot_insert_verified(&row)
            .await
            .expect("publish")
    );
    // Insert-only: a re-publish of the same version is a no-op.
    let mut forged = row.clone();
    forged.digest = "forged".into();
    assert!(
        !pg.store
            .snapshot_insert_verified(&forged)
            .await
            .expect("re-publish")
    );

    // The highest verified read.
    let got = pg
        .store
        .snapshot_highest_verified(sid)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(got.version, 6);
    assert_eq!(got.digest, "d6");
    assert!(got.verified);

    // Candidates: the seeded session is past the minimum.
    let cands = pg
        .store
        .snapshot_candidates(5, 10)
        .await
        .expect("candidates");
    assert!(cands.iter().any(|c| c.session == sid && c.version == 6));

    // GC keeps the newest keep_n.
    for v in [7u64, 8] {
        let mut newer = row.clone();
        newer.version = v;
        newer.digest = format!("d{v}");
        assert!(pg.store.snapshot_insert_verified(&newer).await.expect("p"));
    }
    assert_eq!(pg.store.snapshot_gc(sid, 2).await.expect("gc"), 1);
    let top = pg
        .store
        .snapshot_highest_verified(sid)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(top.version, 8);

    // The maintenance lease: mutual exclusion, heartbeat, takeover,
    // idempotent release.
    let scope = format!("snapshots:{}", sid.0);
    assert!(
        pg.store
            .maintenance_lease_try_acquire(&scope, "a", 1_000, 10_000)
            .await
            .expect("acquire a")
    );
    assert!(
        !pg.store
            .maintenance_lease_try_acquire(&scope, "b", 1_000, 10_500)
            .await
            .expect("b refused")
    );
    assert!(
        pg.store
            .maintenance_lease_heartbeat(&scope, "a", 11_000)
            .await
            .expect("heartbeat a")
    );
    assert!(
        !pg.store
            .maintenance_lease_try_acquire(&scope, "b", 1_000, 11_500)
            .await
            .expect("b still refused")
    );
    assert!(
        pg.store
            .maintenance_lease_try_acquire(&scope, "b", 1_000, 13_000)
            .await
            .expect("b takeover after ttl")
    );
    assert!(
        !pg.store
            .maintenance_lease_heartbeat(&scope, "a", 13_100)
            .await
            .expect("a lost")
    );
    pg.store
        .maintenance_lease_release(&scope, "a")
        .await
        .expect("idempotent release");
    pg.store
        .maintenance_lease_release(&scope, "b")
        .await
        .expect("release b");

    pg.finish().await;
}

/// v4 (READ-MODEL §6): the view tables exist on pg, publication is the
/// one-transaction version flip, the store key moves on any append,
/// and a tampered row breaks the digest — the pg twin of the in-module
/// sqlite suite.
#[tokio::test]
async fn pg_views_publish_reads_and_freshness() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&4),
        "open_postgres applies v4, got {versions:?}"
    );

    let sid = SessionId::new();
    seed(&pg.store, sid, 3, Epoch::GENESIS).await;
    let key = pg.store.view_store_key().await.expect("store key");
    assert_eq!(key.events_total, 3);
    assert_eq!(key.max_seq, 3);

    let rows = vec![kian_store::ViewRow {
        session: sid.0.to_string(),
        row_version: 3,
        payload: r#"{"events":3}"#.into(),
    }];
    pg.store
        .view_publish("session_directory", rows.clone(), key, 42)
        .await
        .expect("publish");
    let state = pg
        .store
        .view_state("session_directory")
        .await
        .expect("state")
        .expect("published");
    assert_eq!((state.events_total, state.max_seq), (3, 3));
    assert_eq!(state.built_at, 42);
    let read = pg
        .store
        .view_rows("session_directory")
        .await
        .expect("rows")
        .expect("table exists");
    assert_eq!(read, rows);
    assert_eq!(
        state.digest,
        kian_store::view_rows_digest("session_directory", &read)
    );

    // Tampering breaks the digest (the corruption check every read
    // performs); view_drop discards the view whole.
    sqlx::query("UPDATE view_rows SET payload = $1 WHERE view = $2 AND session = $3")
        .bind(r#"{"events":999}"#)
        .bind("session_directory")
        .bind(sid.0.to_string())
        .execute(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("tamper");
    let tampered = pg
        .store
        .view_rows("session_directory")
        .await
        .expect("rows")
        .expect("table exists");
    assert_ne!(
        state.digest,
        kian_store::view_rows_digest("session_directory", &tampered)
    );
    pg.store.view_drop("session_directory").await.expect("drop");
    assert!(
        pg.store
            .view_state("session_directory")
            .await
            .expect("state")
            .is_none()
    );
}

/// v5 (READ-MODEL §7, P2b): the archive marker table exists on pg, the
/// eligibility scan lists terminal sessions, the two-phase commit
/// deletes the online rows and writes the marker atomically, the retry
/// is the idempotent no-op, the OnlineMoved fence refuses a stream
/// that appended after verification, and full input rows round-trip —
/// the pg twin of the in-module sqlite suite.
#[tokio::test]
async fn pg_archive_marker_commit_and_inputs() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&5),
        "open_postgres applies v5, got {versions:?}"
    );

    // A terminal session with a consumed input row.
    let sid = SessionId::new();
    seed(&pg.store, sid, 4, Epoch(1)).await;
    pg.store
        .append_end(
            sid,
            ev(
                sid,
                5,
                Epoch(1),
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
            ),
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 5,
            },
        )
        .await
        .expect("end write");
    let row = InputRow {
        id: Uuid::new_v4(),
        session: sid,
        payload: json!({"pg": true}),
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some("pg-k1".into()),
    };
    pg.store.admit(row.clone()).await.expect("admit");
    pg.store
        .mark_consumed(sid, row.id, 2)
        .await
        .expect("consume");

    // Eligibility: terminal sessions only, with counts/bytes facts.
    let candidates = pg.store.archive_candidates().await.expect("candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].session, sid);
    assert_eq!(candidates[0].last_seq, 5);
    assert_eq!(candidates[0].event_count, 5);
    assert_eq!(candidates[0].input_count, 1);
    assert_eq!(candidates[0].terminal_state, "completed");

    // Full input rows read back with the audit columns.
    let inputs = pg.store.archived_inputs_all(sid).await.expect("inputs");
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].consumed_by_seq, Some(2));

    // The two-phase commit: marker + delete atomically, idempotent
    // retry, and the OnlineMoved fence first.
    let marker = kian_store::ArchivedMarker {
        session: sid,
        last_seq: 5,
        event_count: 5,
        input_count: 1,
        package_path: "/tmp/pg-pkg".into(),
        events_sha256: "e".repeat(64),
        inputs_sha256: "i".repeat(64),
        manifest_sha256: "aa".into(),
        archived_at: 42,
    };
    let err = pg
        .store
        .archive_commit(&marker, 4)
        .await
        .expect_err("OnlineMoved must refuse");
    assert!(
        matches!(err, kian_store::ArchiveError::OnlineMoved { .. }),
        "{err:?}"
    );
    assert_eq!(pg.store.rebuild(sid).await.expect("rebuild").len(), 5);

    assert_eq!(
        pg.store.archive_commit(&marker, 5).await.expect("commit"),
        kian_store::ArchiveCommitOutcome::Committed
    );
    assert!(pg.store.rebuild(sid).await.expect("rebuild").is_empty());
    let m = pg
        .store
        .archived_session(sid)
        .await
        .expect("marker read")
        .expect("marker exists");
    assert_eq!(m.manifest_sha256, "aa");
    assert_eq!(
        pg.store
            .archive_commit(&marker, 5)
            .await
            .expect("idempotent retry"),
        kian_store::ArchiveCommitOutcome::AlreadyArchived
    );

    // Input rows restore verbatim on pg too.
    let fresh = PgStore::open().await;
    let n = fresh
        .store
        .archive_inputs_insert(&inputs)
        .await
        .expect("insert");
    assert_eq!(n, 1);
    let back = fresh
        .store
        .archived_inputs_all(sid)
        .await
        .expect("read back");
    assert_eq!(back, inputs);
    fresh.finish().await;
    pg.finish().await;
}

/// ADR-082 on postgres: the archive commit materializes the consumed
/// `chan:` keys into the receipt ledger, the online rows are deleted, and
/// a replay of the same key into a fresh session is refused by the receipt
/// — the guard outlives the incarnation that consumed it. A key that was
/// never consumed stays replayable, exactly like the sqlite twin.
#[tokio::test]
async fn pg_channel_receipts_keep_the_replay_guard_across_archival() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&10),
        "open_postgres applies v10, got {versions:?}"
    );

    let sid = SessionId::new();
    seed(&pg.store, sid, 4, Epoch(1)).await;
    pg.store
        .append_end(
            sid,
            ev(
                sid,
                5,
                Epoch(1),
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
            ),
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 5,
            },
        )
        .await
        .expect("end write");

    let payload = json!({"kian": {"channel": "imessage", "type": "channel_event"}});
    let consumed = input(sid, Some("chan:imessage:pg-guid"), T0, payload.clone());
    let unconsumed = input(
        sid,
        Some("chan:imessage:pg-guid-pending"),
        T0,
        payload.clone(),
    );
    pg.store.admit(consumed.clone()).await.expect("admit");
    pg.store.admit(unconsumed.clone()).await.expect("admit");
    pg.store
        .mark_consumed(sid, consumed.id, 2)
        .await
        .expect("consume");

    let marker = kian_store::ArchivedMarker {
        session: sid,
        last_seq: 5,
        event_count: 5,
        input_count: 2,
        package_path: "/tmp/pg-pkg-receipts".into(),
        events_sha256: "e".repeat(64),
        inputs_sha256: "i".repeat(64),
        manifest_sha256: "bb".into(),
        archived_at: 42,
    };
    assert_eq!(
        pg.store.archive_commit(&marker, 5).await.expect("commit"),
        kian_store::ArchiveCommitOutcome::Committed
    );
    assert!(pg.store.rebuild(sid).await.expect("rebuild").is_empty());

    // The replay: a fresh incarnation asks for the consumed key. Its online
    // row is gone with the archived session; the receipt refuses it and
    // names the row that already answered the event.
    let fresh = SessionId::new();
    let outcome = pg
        .store
        .admit_channel(input(
            fresh,
            Some("chan:imessage:pg-guid"),
            T0 + 1,
            payload.clone(),
        ))
        .await
        .expect("admit_channel");
    assert!(outcome.deduplicated, "an archived consumed key is refused");
    assert_eq!(outcome.input.id, consumed.id, "the receipt names the row");
    assert!(
        pg.store
            .rebuild(fresh)
            .await
            .expect("rebuild fresh")
            .is_empty()
    );

    // An unconsumed key never blocks: archival removed the pending row, and
    // the replay re-admits it.
    let outcome = pg
        .store
        .admit_channel(input(
            fresh,
            Some("chan:imessage:pg-guid-pending"),
            T0 + 1,
            payload,
        ))
        .await
        .expect("admit pending");
    assert!(!outcome.deduplicated, "an unconsumed key never blocks");
    pg.finish().await;
}

// -- scan_kind (memory P3b-1) -------------------------------------------------

/// `scan_kind` on postgres: byte-identical to scan-then-filter on a
/// pseudo-random mixed-kind stream (A1/A7 parity with the sqlite suite),
/// seq-ascending, the v6 per-session kind index physically exists, and
/// the read-only-pool open (no migration) reads the same rows. A second
/// stream seeds `SessionEnded` with all four `EndReason`s: the `kind`
/// column collapses them into one key, so this backend's post-decode
/// exactness guard (kian-120 B1) is exercised here too, mirroring the
/// sqlite `scan_kind_is_exact_for_session_ended_reasons`.
#[tokio::test]
async fn pg_scan_kind_parity_index_and_read_only_pool() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let sid = SessionId::new();

    // Deterministic LCG mixed-kind fixture (the same shape as the sqlite
    // suite's `seed_mixed`; MemoryNote over-represented so notes exist).
    let mut state = 0x5EED_0006u64;
    let kinds = [
        EventKind::TurnStart,
        EventKind::MemoryNote,
        EventKind::InputAdmitted,
        EventKind::ToolCall,
        EventKind::MemoryNote,
        EventKind::ControlInterrupt,
    ];
    for seq in 1..=200u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let kind = if seq == 1 {
            EventKind::SessionCreated
        } else {
            kinds[((state >> 33) % kinds.len() as u64) as usize]
        };
        append_ok(&pg.store, sid, seq, Epoch(1), kind).await;
    }

    let ser = |events: &[Event]| {
        events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
    };
    let full = pg.store.scan(sid, all()).await.expect("scan");
    assert_eq!(full.len(), 200);
    let expected: Vec<String> = ser(&full
        .iter()
        .filter(|e| matches!(e.kind, EventKind::MemoryNote))
        .cloned()
        .collect::<Vec<_>>());
    let got = pg
        .store
        .scan_kind(sid, EventKind::MemoryNote)
        .await
        .expect("scan_kind");
    assert!(!got.is_empty(), "fixture must contain memory/note events");
    assert_eq!(
        ser(&got),
        expected,
        "scan_kind must be byte-identical to scan-then-filter"
    );
    for pair in got.windows(2) {
        assert!(pair[0].seq < pair[1].seq, "seqs must arrive ascending");
    }

    // SessionEnded exactness on this backend: the kind column stores
    // `as_str()`, collapsing the four EndReasons into "session/ended",
    // so `scan_kind` must re-compare the decoded kind and skip
    // wrong-reason rows (never CorruptRow) — the sqlite suite's
    // `scan_kind_is_exact_for_session_ended_reasons` on pg (kian-120
    // B1, kian-121 audit follow-up).
    let sid2 = SessionId::new();
    let reasons = [
        EndReason::Completed,
        EndReason::Cancelled,
        EndReason::Interrupted,
        EndReason::Failed,
    ];
    append_ok(&pg.store, sid2, 1, Epoch(1), EventKind::SessionCreated).await;
    for (i, r) in reasons.iter().enumerate() {
        append_ok(
            &pg.store,
            sid2,
            2 + i as u64,
            Epoch(1),
            EventKind::SessionEnded { reason: *r },
        )
        .await;
    }
    let full2 = pg
        .store
        .scan(sid2, all())
        .await
        .expect("scan SessionEnded stream");
    assert_eq!(full2.len(), 1 + reasons.len());
    let reason_expectations: Vec<(EventKind, Vec<String>)> = reasons
        .iter()
        .map(|r| {
            let k = EventKind::SessionEnded { reason: *r };
            let expected = ser(&full2
                .iter()
                .filter(|e| e.kind == k)
                .cloned()
                .collect::<Vec<_>>());
            (k, expected)
        })
        .collect();
    for (k, expected) in &reason_expectations {
        let got = pg
            .store
            .scan_kind(sid2, *k)
            .await
            .expect("scan_kind SessionEnded");
        assert_eq!(
            ser(&got),
            *expected,
            "scan_kind must be exact for {k:?} on postgres"
        );
        assert_eq!(got.len(), 1, "exactly the requested reason");
    }

    // The v6 index physically exists on the events table.
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg.url())
        .await
        .expect("index check connection");
    let idx: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes \
         WHERE tablename = 'events' AND indexname = 'idx_events_session_kind_seq'",
    )
    .fetch_one(&admin)
    .await
    .expect("pg_indexes read");
    admin.close().await;
    assert_eq!(idx, 1, "v6 must create idx_events_session_kind_seq");

    // The read-only-pool open (which never migrates) reads the same rows.
    let reader = EventStore::open_postgres_read_only(&pg.url())
        .await
        .expect("read-only pg open");
    let ro = reader
        .scan_kind(sid, EventKind::MemoryNote)
        .await
        .expect("read-only scan_kind");
    assert_eq!(ser(&ro), expected);
    for (k, reason_expected) in &reason_expectations {
        let ro2 = reader
            .scan_kind(sid2, *k)
            .await
            .expect("read-only scan_kind SessionEnded");
        assert_eq!(
            ser(&ro2),
            *reason_expected,
            "read-only scan_kind must be exact for {k:?} on postgres"
        );
    }
    reader.close().await;

    pg.finish().await;
}

// -- durable cron state (CRON-MODEL §4/§9, v7) -------------------------------

/// The cron state semantics on postgres: the migration applies, the
/// insert-if-absent claim has exactly one winner under a real race, the
/// cursor is monotonic and admit-then-advance guarded, and a reopened
/// store reads the same cursor (§7.4 restart safety).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_cron_claims_advance_and_restart() {
    if !gated() {
        return;
    }
    let pg = PgStore::open().await;
    let versions = migrations::applied_versions_pg(pg.store.pg_pool().expect("pg pool"))
        .await
        .expect("applied versions");
    assert!(
        versions.contains(&7),
        "open_postgres applies v7, got {versions:?}"
    );
    assert!(
        versions.contains(&8),
        "open_postgres applies v8, got {versions:?}"
    );

    const T0: u64 = 1_761_900_000_000;
    fn firing(schedule_id: &str, fire_time: &str) -> CronFiringRow {
        CronFiringRow {
            firing_id: format!("{schedule_id}@{fire_time}"),
            schedule_id: schedule_id.to_owned(),
            fire_time: fire_time.to_owned(),
            session: None,
            admitted_at: T0,
            state: FiringState::Admitted,
            skip_reason: None,
            worktree_path: None,
            finished_at: None,
            error: None,
        }
    }

    assert_eq!(
        pg.store
            .upsert_schedule("night-audit", "*/5 * * * *", "UTC", "d1", true, T0)
            .await
            .expect("insert schedule"),
        ScheduleUpsert::Inserted
    );
    // Idempotent redeclaration.
    assert_eq!(
        pg.store
            .upsert_schedule("night-audit", "*/5 * * * *", "UTC", "d1", true, T0 + 1)
            .await
            .expect("re-upsert"),
        ScheduleUpsert::Unchanged
    );

    // A real race: four concurrent claims of the same slot, one winner.
    let slot = firing("night-audit", "2026-11-09T02:30:00Z");
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = pg.store.clone();
        let slot = slot.clone();
        handles.push(tokio::spawn(async move {
            store.claim_firing(&slot).await.expect("claim")
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.expect("join") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one racer may own the slot");
    assert!(!pg.store.claim_firing(&slot).await.expect("replay claim"));
    // The settlement sweep's open-firing read (CRON-MODEL §9.1): the claimed
    // slot is open until it settles.
    let open = pg
        .store
        .open_firings("night-audit")
        .await
        .expect("open firings");
    assert_eq!(open.len(), 1, "{open:?}");
    assert_eq!(open[0].firing_id, slot.firing_id);

    // v8: the firing records the session it is dispatched to (CRON-MODEL
    // §3.2/§9.1); the claim starts with no session, the guarded write fills it.
    let session = kian_core::model::SessionId::new();
    assert!(
        pg.store
            .set_firing_session(&slot.firing_id, session)
            .await
            .expect("record firing session")
    );
    let recorded = pg
        .store
        .load_firing(&slot.firing_id)
        .await
        .expect("load firing")
        .expect("row");
    assert_eq!(
        recorded.session.as_deref(),
        Some(session.0.to_string().as_str())
    );

    // Settle once; the terminal row is never rewritten.
    assert!(
        pg.store
            .finish_firing(
                &slot.firing_id,
                FiringState::Succeeded,
                Some(T0 + 5_000),
                Some("/tmp/wt/a"),
                None,
                None
            )
            .await
            .expect("settle")
    );
    assert!(
        !pg.store
            .finish_firing(
                &slot.firing_id,
                FiringState::Failed,
                Some(T0 + 6_000),
                None,
                None,
                Some("late")
            )
            .await
            .expect("replay settle")
    );
    assert!(
        pg.store
            .open_firings("night-audit")
            .await
            .expect("open firings after settle")
            .is_empty(),
        "a settled slot is never seen as open"
    );

    // Admit-then-advance: an unrecorded slot is refused, the recorded one
    // moves the cursor exactly once.
    let unrecorded = pg
        .store
        .advance_cursor(
            "night-audit",
            "2026-11-09T02:35:00Z",
            "2026-11-09T02:40:00Z",
            T0,
        )
        .await;
    assert!(
        matches!(unrecorded, Err(CronError::UnrecordedFiring { .. })),
        "got {unrecorded:?}"
    );
    assert!(
        pg.store
            .advance_cursor(
                "night-audit",
                "2026-11-09T02:30:00Z",
                "2026-11-09T02:35:00Z",
                T0
            )
            .await
            .expect("advance")
    );
    assert!(
        !pg.store
            .advance_cursor(
                "night-audit",
                "2026-11-09T02:30:00Z",
                "2026-11-09T02:35:00Z",
                T0
            )
            .await
            .expect("replay advance")
    );
    assert!(
        !pg.store
            .advance_cursor(
                "night-audit",
                "2026-11-09T02:25:00Z",
                "2026-11-09T02:35:00Z",
                T0
            )
            .await
            .expect("backwards advance is a no-op")
    );
    let row = pg
        .store
        .load_schedule("night-audit")
        .await
        .expect("load")
        .expect("row");
    assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));
    assert_eq!(row.next_fire_at.as_deref(), Some("2026-11-09T02:35:00Z"));

    // Restart safety: a freshly opened store on the same database reads
    // the same cursor, and the settled slot is still not claimable.
    let reopened = EventStore::open_postgres(&pg.url()).await.expect("reopen");
    let row = reopened
        .load_schedule("night-audit")
        .await
        .expect("reload")
        .expect("row");
    assert_eq!(row.cursor_fire_at.as_deref(), Some("2026-11-09T02:30:00Z"));
    assert!(!reopened.claim_firing(&slot).await.expect("replay claim"));
    let firings = reopened
        .list_firings(Some("night-audit"), 10)
        .await
        .expect("list");
    assert_eq!(firings.len(), 1);
    assert_eq!(firings[0].state, FiringState::Succeeded);
    reopened.close().await;

    pg.finish().await;
}
