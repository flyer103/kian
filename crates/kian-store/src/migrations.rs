//! Schema versioning: adjacent migration chain (DESIGN §3.3).
//!
//! vN→vN+1 is one [`Migration`] entry; committed generations are never
//! renamed, replaced or deleted — a new generation is published
//! exclusively by appending to the chain. v0 is current in built-in and
//! the chain is extensible, so an old DB with a v0 stream reopens
//! cleanly (each unapplied generation runs in order, exactly once).

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::postgres::PgPool;
use sqlx::sqlite::SqlitePool;

use crate::event_store::StoreError;

/// One generation in the adjacent chain (DESIGN §3.3). `up` is DDL-only
/// for v0; data-carrying migrations append new generations the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub up: &'static str,
}

/// v0 — the append-only events table. Frozen golden: committed
/// generations are never rewritten (see `v0_generation_sql_is_frozen`).
///
/// The `envelope` column stores the exact serialized v0 event envelope so
/// replay is byte-identical; the projection columns (`session`, `seq`,
/// `epoch`, `kind`, `version`) exist for the CAS fences and queries.
const V0_EVENTS: Migration = Migration {
    version: 0,
    name: "v0_events",
    up: "CREATE TABLE IF NOT EXISTS events (\n    session TEXT NOT NULL,\n    seq INTEGER NOT NULL,\n    epoch INTEGER NOT NULL,\n    ts INTEGER NOT NULL,\n    kind TEXT NOT NULL,\n    version INTEGER NOT NULL,\n    envelope TEXT NOT NULL,\n    PRIMARY KEY (session, seq)\n)",
};

/// v1 — the inbox and claim/lease tables. Frozen golden like
/// v0: committed generations are never rewritten.
///
/// `inputs` rows are the durable inbox (constraint 6: inputs are durable
/// rows before any wake fires). Eligibility is *event-driven*: a row stops
/// being eligible only when its `consumed_by_seq` references a committed
/// event of the same session — the row itself is never removed before
/// that consumption event exists (and never removed after, either: the
/// inbox stays audit-visible — online, or inside a verified archive
/// package once the terminal session leaves the store through the
/// opt-in retention path, READ-MODEL §7 / migration v5).
///
/// `claims` holds the lease per session. `epoch` doubles as the durable
/// epoch high-water mark: issuance always takes
/// max(claims.epoch, events epoch floor) + 1, so epochs strictly increase
/// per session across process restarts and across claims whose
/// `drain/claimed` event has not been appended yet (the window
/// flagged). Leases are liveness only (ADR-005) — expiry never grants
/// permission to accept stale writes.
const V1_INBOX_CLAIMS: Migration = Migration {
    version: 1,
    name: "v1_inbox_claims",
    up: "CREATE TABLE IF NOT EXISTS inputs (\n    id TEXT PRIMARY KEY,\n    session TEXT NOT NULL,\n    payload TEXT NOT NULL,\n    admitted_at INTEGER NOT NULL,\n    eligible_at INTEGER NOT NULL,\n    idempotency_key TEXT,\n    consumed_by_seq INTEGER,\n    UNIQUE (session, idempotency_key)\n);\nCREATE INDEX IF NOT EXISTS idx_inputs_pending ON inputs (session, eligible_at) WHERE consumed_by_seq IS NULL;\nCREATE TABLE IF NOT EXISTS claims (\n    session TEXT PRIMARY KEY,\n    epoch INTEGER NOT NULL,\n    holder TEXT NOT NULL,\n    ttl_ms INTEGER NOT NULL,\n    last_heartbeat INTEGER NOT NULL\n)",
};

/// v4 — the materialized-view tables (READ-MODEL §6, P2a). Like
/// `projection_snapshots` these are **infrastructure rows, never
/// truth** (R1): every view can be dropped and rebuilt from the event
/// stream at any time without changing a single event byte.
///
/// One physical row table serves every declared logical view: the
/// `view` column carries the declared view name
/// (`session_directory`, `session_usage`, `ticket_stage`,
/// `session_cache`) and `payload` the canonical JSON of that view's
/// row type. A per-view `view_state` row records the store key
/// (`events_total`, `max_seq`) the rows were derived at plus a sha256
/// digest over the row set — the freshness and corruption checks every
/// read performs before a view is served (READ-MODEL §6 rules).
///
/// Publication is one transaction per view (delete the view's rows,
/// insert the new set, upsert `view_state`): atomic on both backends,
/// so a crash leaves the old complete version or the new complete
/// version — never half a view (a view is visible only when complete
/// for its version). Unlike snapshots (R6, insert-only) a view
/// *replaces* its predecessor — that is safe precisely because the
/// whole version flips in one transaction and no reader may treat a
/// view as truth anyway.
///
/// The declared `audit_kind_index` view is implemented as the SQL
/// index below rather than a view table: `kind`/`ts` are already
/// projection columns of the truth table, so a plain index accelerates
/// kind/time queries with zero duplication of derived data, is
/// maintained by the database itself at append time (always fresh —
/// no staleness class, no rebuild), and adds nothing that could ever
/// disagree with the stream. A view table would duplicate one row per
/// event only to go stale.
const V4_VIEWS: Migration = Migration {
    version: 4,
    name: "v4_materialized_views",
    up: "CREATE TABLE IF NOT EXISTS view_rows (\n    view TEXT NOT NULL,\n    session TEXT NOT NULL,\n    row_version INTEGER NOT NULL,\n    payload TEXT NOT NULL,\n    PRIMARY KEY (view, session)\n);\nCREATE TABLE IF NOT EXISTS view_state (\n    view TEXT PRIMARY KEY,\n    events_total INTEGER NOT NULL,\n    max_seq INTEGER NOT NULL,\n    digest TEXT NOT NULL,\n    built_at INTEGER NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events (kind, ts)",
};

/// v6 — the per-session kind index (READ-MODEL §6, memory P3b-1). The
/// per-session companion of v4's global `idx_events_kind_ts`
/// (`audit_kind_index`): `session, kind, seq` serves
/// `WHERE session = ? AND kind = ? ORDER BY seq` — the read shape behind
/// `EventStore::scan_kind` — as an index-only lookup instead of a
/// full-session stream read.
///
/// Like every SQL index this is **infrastructure, never truth** (R1):
/// `session`/`kind`/`seq` are already projection columns of the truth
/// table, the database maintains the index at append time (always
/// fresh — no staleness class, no rebuild), and it can never disagree
/// with the stream. A store that predates this generation still answers
/// the same query correctly via the planner's fallback (slower, never
/// wrong).
const V6_SESSION_KIND_INDEX: Migration = Migration {
    version: 6,
    name: "v6_session_kind_index",
    up: "CREATE INDEX IF NOT EXISTS idx_events_session_kind_seq ON events (session, kind, seq)",
};

/// v5 — the archive marker table (READ-MODEL §7, P2b). One row per
/// session whose online rows left the store through the export-only
/// retention path: the verified package's identity (path + hashes) and
/// range. The marker is the read surfaces' signal to answer `archived`
/// (`-32007`) instead of `unknown_session`.
///
/// The marker is **infrastructure, not truth**: it records that a
/// verified package exists and where; the package itself carries the
/// events and inputs. Insertion is CAS (first marker wins) and shares
/// one transaction with the online-row delete in
/// [`crate::archive::archive_commit`] — a crash leaves the online rows
/// and no marker, or the marker and no online rows, never anything
/// in between.
const V5_ARCHIVED_SESSIONS: Migration = Migration {
    version: 5,
    name: "v5_archived_sessions",
    up: "CREATE TABLE IF NOT EXISTS archived_sessions (\n    session TEXT PRIMARY KEY,\n    last_seq INTEGER NOT NULL,\n    event_count INTEGER NOT NULL,\n    input_count INTEGER NOT NULL,\n    package_path TEXT NOT NULL,\n    events_sha256 TEXT NOT NULL,\n    inputs_sha256 TEXT NOT NULL,\n    manifest_sha256 TEXT NOT NULL,\n    archived_at INTEGER NOT NULL\n)",
};

/// v7 — durable cron state (CRON-MODEL §4/§9). Two tables, both the
/// scheduler's own bookkeeping — never event truth (a firing is an
/// ordinary admitted turn whose record is an ordinary event row).
///
/// `cron_schedules` — one row per declared schedule: the normalized
/// declaration (`expr`, `tz`, `declaration_digest`), the durable cursor
/// (`cursor_fire_at` = last *admitted* fire time, `next_fire_at`;
/// `NULL` before the first computation and after a redeclaration, §12.3)
/// and `enabled`. Fire times are TEXT in the canonical UTC RFC 3339
/// `Z`/whole-seconds form, so lexicographic order is chronological order
/// and the monotonic cursor guard is a plain string comparison.
///
/// `cron_firings` — one row per slot `(schedule_id, fire_time)` with
/// `firing_id = "{schedule_id}@{fire_time}"` as the PK: the primary key
/// *is* the idempotency mechanism (§4.3), so a raced claim is one
/// `INSERT … ON CONFLICT DO NOTHING` and exactly one caller wins. The
/// extra `UNIQUE (schedule_id, fire_time)` states the slot invariant
/// independently of the id format. Indexes: by slot time (per-schedule
/// `cron logs`), by `(state, finished_at)` (the overlap lookup of §5.1
/// and the terminal-row GC of §6.4).
const V7_CRON_STATE: Migration = Migration {
    version: 7,
    name: "v7_cron_state",
    up: "CREATE TABLE IF NOT EXISTS cron_schedules (\n    id TEXT PRIMARY KEY,\n    expr TEXT NOT NULL,\n    tz TEXT NOT NULL,\n    declaration_digest TEXT NOT NULL,\n    cursor_fire_at TEXT,\n    next_fire_at TEXT,\n    enabled INTEGER NOT NULL,\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_cron_schedules_next_fire ON cron_schedules (enabled, next_fire_at);\nCREATE TABLE IF NOT EXISTS cron_firings (\n    firing_id TEXT PRIMARY KEY,\n    schedule_id TEXT NOT NULL,\n    fire_time TEXT NOT NULL,\n    admitted_at INTEGER NOT NULL,\n    state TEXT NOT NULL,\n    skip_reason TEXT,\n    worktree_path TEXT,\n    finished_at INTEGER,\n    error TEXT,\n    UNIQUE (schedule_id, fire_time)\n);\nCREATE INDEX IF NOT EXISTS idx_cron_firings_schedule_time ON cron_firings (schedule_id, fire_time);\nCREATE INDEX IF NOT EXISTS idx_cron_firings_state ON cron_firings (state, finished_at)",
};

/// v8 — the firing's session (CRON-MODEL §3.2/§9.1). The resident arm may run
/// a slot in a *derived* session (the configured target was frozen at
/// dispatch), so the durable firing record names where the firing actually
/// ran. Additive and nullable: rows recorded before v8 keep `NULL` and the
/// settlement sweep falls back to the configured target for them.
const V8_CRON_FIRING_SESSION: Migration = Migration {
    version: 8,
    name: "v8_cron_firing_session",
    up: "ALTER TABLE cron_firings ADD COLUMN session TEXT",
};

/// The registered adjacent chain, ordered v0 → head. Extensible: append
/// the next [`Migration`]; never edit a committed entry.
pub fn chain() -> &'static [Migration] {
    &[
        V0_EVENTS,
        V1_INBOX_CLAIMS,
        V2_INBOX_ADMIT_ORDER,
        V3_PROJECTION_SNAPSHOTS,
        V4_VIEWS,
        V5_ARCHIVED_SESSIONS,
        V6_SESSION_KIND_INDEX,
        V7_CRON_STATE,
        V8_CRON_FIRING_SESSION,
    ]
}

/// v2 — the inbox admission ordinal. The take queries
/// ordered eligible rows by `(eligible_at, id)`, and `id` is a random
/// uuid: two inputs admitted in the same millisecond consumed in a
/// random order, which misrouted worker sessions whose classification
/// reads the *first* input and made cross-replica replay
/// nondeterministic. v2 adds a database-assigned monotone admission
/// ordinal (`admitted_seq`) as the tie-break: sqlite computes
/// `max(admitted_seq)+1` inside the single INSERT statement (sqlite
/// serializes whole write statements, so the max is race-free); pg uses
/// an identity column (existing rows are backfilled by the ALTER in
/// scan order — a one-time best-effort for legacy rows; every new admit
/// is strictly ordered). Both backends then order eligible rows by
/// `(eligible_at, admitted_seq)`.
const V2_INBOX_ADMIT_ORDER: Migration = Migration {
    version: 2,
    name: "v2_inbox_admit_order",
    up: "ALTER TABLE inputs ADD COLUMN admitted_seq INTEGER NOT NULL DEFAULT 0;\nUPDATE inputs SET admitted_seq = rowid;\nCREATE INDEX IF NOT EXISTS idx_inputs_admit_order ON inputs (session, eligible_at, admitted_seq) WHERE consumed_by_seq IS NULL;",
};

/// v3 — the read-model snapshot tables (READ-MODEL §5, P1). Infrastructure
/// rows only: `projection_snapshots` is a disposable, verified projection
/// cache (state + model context + provenance at a committed version) —
/// never truth (R1; the event stream stays the sole source). The table
/// may be dropped and rebuilt at any time without changing the stream.
///
/// Publication is insert-only: a builder writes a row only after a second
/// independent derivation verified `(state, context, provenance)` against
/// the first, so a crash window can leave nothing or one complete
/// verified row — never a partially applied snapshot (R6). Readers only
/// ever see `verified = 1` rows; superseded/orphan rows are GC-able.
///
/// `maintenance_leases` holds the snapshot builder's liveness lease
/// (READ-MODEL §9): per-scope, TTL + heartbeat only — safety comes from
/// the insert-only publication shape, never from the lease (the claims
/// doctrine, ADR-005, applied to maintenance).
const V3_PROJECTION_SNAPSHOTS: Migration = Migration {
    version: 3,
    name: "v3_projection_snapshots",
    up: "CREATE TABLE IF NOT EXISTS projection_snapshots (\n    session TEXT NOT NULL,\n    version INTEGER NOT NULL,\n    state TEXT NOT NULL,\n    context TEXT NOT NULL,\n    context_provenance TEXT NOT NULL,\n    digest TEXT NOT NULL,\n    verified INTEGER NOT NULL,\n    created_at INTEGER NOT NULL,\n    PRIMARY KEY (session, version)\n);\nCREATE INDEX IF NOT EXISTS idx_projection_snapshots_verified ON projection_snapshots (session, version) WHERE verified = 1;\nCREATE TABLE IF NOT EXISTS maintenance_leases (\n    scope TEXT PRIMARY KEY,\n    holder TEXT NOT NULL,\n    ttl_ms INTEGER NOT NULL,\n    last_heartbeat INTEGER NOT NULL\n)",
};

/// v0 (postgres chain) — same version and name as the sqlite
/// generation, pg DDL (TEXT/BIGINT; DESIGN §3.3). Envelope stays verbatim
/// TEXT so the same event stream exported from either backend is
/// byte-identical (the export-parity golden).
const PG_V0_EVENTS: Migration = Migration {
    version: 0,
    name: "v0_events",
    up: "CREATE TABLE IF NOT EXISTS events (\n    session TEXT NOT NULL,\n    seq BIGINT NOT NULL,\n    epoch BIGINT NOT NULL,\n    ts BIGINT NOT NULL,\n    kind TEXT NOT NULL,\n    version BIGINT NOT NULL,\n    envelope TEXT NOT NULL,\n    PRIMARY KEY (session, seq)\n)",
};

/// v1 (postgres chain) — the inbox and claim/lease tables, pg DDL.
/// The same partial index and the same UNIQUE (session, idempotency_key)
/// semantics: pg unique indexes treat NULLs as distinct, exactly like
/// sqlite, so keyless admits never deduplicate on either backend.
const PG_V1_INBOX_CLAIMS: Migration = Migration {
    version: 1,
    name: "v1_inbox_claims",
    up: "CREATE TABLE IF NOT EXISTS inputs (\n    id TEXT PRIMARY KEY,\n    session TEXT NOT NULL,\n    payload TEXT NOT NULL,\n    admitted_at BIGINT NOT NULL,\n    eligible_at BIGINT NOT NULL,\n    idempotency_key TEXT,\n    consumed_by_seq BIGINT,\n    UNIQUE (session, idempotency_key)\n);\nCREATE INDEX IF NOT EXISTS idx_inputs_pending ON inputs (session, eligible_at) WHERE consumed_by_seq IS NULL;\nCREATE TABLE IF NOT EXISTS claims (\n    session TEXT PRIMARY KEY,\n    epoch BIGINT NOT NULL,\n    holder TEXT NOT NULL,\n    ttl_ms BIGINT NOT NULL,\n    last_heartbeat BIGINT NOT NULL\n)",
};

/// v2 (postgres chain) — the inbox admission ordinal, pg DDL: an
/// identity column (the sequence is the race-free global ordinal; the
/// ALTER backfills existing rows in scan order — a one-time best-effort
/// for legacy rows, every new admit is strictly ordered). Same version
/// and name as the sqlite generation.
const PG_V2_INBOX_ADMIT_ORDER: Migration = Migration {
    version: 2,
    name: "v2_inbox_admit_order",
    up: "ALTER TABLE inputs ADD COLUMN admitted_seq BIGINT GENERATED BY DEFAULT AS IDENTITY;\nCREATE INDEX IF NOT EXISTS idx_inputs_admit_order ON inputs (session, eligible_at, admitted_seq) WHERE consumed_by_seq IS NULL;",
};

/// v3 (postgres chain) — the read-model snapshot tables, pg DDL (same
/// version and name as the sqlite generation, READ-MODEL §5/§9).
const PG_V3_PROJECTION_SNAPSHOTS: Migration = Migration {
    version: 3,
    name: "v3_projection_snapshots",
    up: "CREATE TABLE IF NOT EXISTS projection_snapshots (\n    session TEXT NOT NULL,\n    version BIGINT NOT NULL,\n    state TEXT NOT NULL,\n    context TEXT NOT NULL,\n    context_provenance TEXT NOT NULL,\n    digest TEXT NOT NULL,\n    verified BIGINT NOT NULL,\n    created_at BIGINT NOT NULL,\n    PRIMARY KEY (session, version)\n);\nCREATE INDEX IF NOT EXISTS idx_projection_snapshots_verified ON projection_snapshots (session, version) WHERE verified = 1;\nCREATE TABLE IF NOT EXISTS maintenance_leases (\n    scope TEXT PRIMARY KEY,\n    holder TEXT NOT NULL,\n    ttl_ms BIGINT NOT NULL,\n    last_heartbeat BIGINT NOT NULL\n)",
};

/// v4 (postgres chain) — the materialized-view tables, pg DDL (same
/// version, name and shape as the sqlite generation, READ-MODEL §6).
/// The `audit_kind_index` is the SQL index, exactly like sqlite.
const PG_V4_VIEWS: Migration = Migration {
    version: 4,
    name: "v4_materialized_views",
    up: "CREATE TABLE IF NOT EXISTS view_rows (\n    view TEXT NOT NULL,\n    session TEXT NOT NULL,\n    row_version BIGINT NOT NULL,\n    payload TEXT NOT NULL,\n    PRIMARY KEY (view, session)\n);\nCREATE TABLE IF NOT EXISTS view_state (\n    view TEXT PRIMARY KEY,\n    events_total BIGINT NOT NULL,\n    max_seq BIGINT NOT NULL,\n    digest TEXT NOT NULL,\n    built_at BIGINT NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events (kind, ts)",
};

/// v5 (postgres chain) — the archive marker table, pg DDL (same
/// version, name and shape as the sqlite generation, READ-MODEL §7).
const PG_V5_ARCHIVED_SESSIONS: Migration = Migration {
    version: 5,
    name: "v5_archived_sessions",
    up: "CREATE TABLE IF NOT EXISTS archived_sessions (\n    session TEXT PRIMARY KEY,\n    last_seq BIGINT NOT NULL,\n    event_count BIGINT NOT NULL,\n    input_count BIGINT NOT NULL,\n    package_path TEXT NOT NULL,\n    events_sha256 TEXT NOT NULL,\n    inputs_sha256 TEXT NOT NULL,\n    manifest_sha256 TEXT NOT NULL,\n    archived_at BIGINT NOT NULL\n)",
};

/// v6 (postgres chain) — the per-session kind index, pg DDL (same
/// version, name and shape as the sqlite generation; `session`, `kind`,
/// `seq` are all TEXT/BIGINT columns, so the DDL is identical). See the
/// sqlite twin's docs — infrastructure, never truth.
const PG_V6_SESSION_KIND_INDEX: Migration = Migration {
    version: 6,
    name: "v6_session_kind_index",
    up: "CREATE INDEX IF NOT EXISTS idx_events_session_kind_seq ON events (session, kind, seq)",
};

/// v7 (postgres chain) — the durable cron tables, pg DDL (same version,
/// name and shape as the sqlite generation, CRON-MODEL §4/§9): integers
/// widen to BIGINT, fire times stay TEXT in the same canonical UTC
/// RFC 3339 form so a cursor written by either backend orders — and
/// compares — identically.
const PG_V7_CRON_STATE: Migration = Migration {
    version: 7,
    name: "v7_cron_state",
    up: "CREATE TABLE IF NOT EXISTS cron_schedules (\n    id TEXT PRIMARY KEY,\n    expr TEXT NOT NULL,\n    tz TEXT NOT NULL,\n    declaration_digest TEXT NOT NULL,\n    cursor_fire_at TEXT,\n    next_fire_at TEXT,\n    enabled BIGINT NOT NULL,\n    created_at BIGINT NOT NULL,\n    updated_at BIGINT NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_cron_schedules_next_fire ON cron_schedules (enabled, next_fire_at);\nCREATE TABLE IF NOT EXISTS cron_firings (\n    firing_id TEXT PRIMARY KEY,\n    schedule_id TEXT NOT NULL,\n    fire_time TEXT NOT NULL,\n    admitted_at BIGINT NOT NULL,\n    state TEXT NOT NULL,\n    skip_reason TEXT,\n    worktree_path TEXT,\n    finished_at BIGINT,\n    error TEXT,\n    UNIQUE (schedule_id, fire_time)\n);\nCREATE INDEX IF NOT EXISTS idx_cron_firings_schedule_time ON cron_firings (schedule_id, fire_time);\nCREATE INDEX IF NOT EXISTS idx_cron_firings_state ON cron_firings (state, finished_at)",
};

/// v8 (postgres chain) — the firing's session, pg DDL (same version, name and
/// shape as the sqlite generation; TEXT is the same column type on both).
const PG_V8_CRON_FIRING_SESSION: Migration = Migration {
    version: 8,
    name: "v8_cron_firing_session",
    up: "ALTER TABLE cron_firings ADD COLUMN session TEXT",
};

/// The postgres chain: same versions, same names, same ordering as
/// [`chain`] — the two backends' schemas track each other generation for
/// generation (DESIGN §3.3), enforced by
/// `pg_chain_mirrors_sqlite_chain_versions_and_names`.
pub fn pg_chain() -> &'static [Migration] {
    &[
        PG_V0_EVENTS,
        PG_V1_INBOX_CLAIMS,
        PG_V2_INBOX_ADMIT_ORDER,
        PG_V3_PROJECTION_SNAPSHOTS,
        PG_V4_VIEWS,
        PG_V5_ARCHIVED_SESSIONS,
        PG_V6_SESSION_KIND_INDEX,
        PG_V7_CRON_STATE,
        PG_V8_CRON_FIRING_SESSION,
    ]
}

/// Applies every unapplied generation in order. Each generation's DDL and
/// its bookkeeping row land in one transaction, so a crash mid-migration
/// leaves that generation fully unapplied and retryable.
pub async fn migrate(pool: &SqlitePool) -> Result<(), StoreError> {
    // Meta table owned by the runner (not part of any generation's DDL).
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _kian_migrations (\n    version INTEGER PRIMARY KEY,\n    name TEXT NOT NULL,\n    applied_at INTEGER NOT NULL\n)",
    )
    .execute(pool)
    .await?;
    for m in chain() {
        let applied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _kian_migrations WHERE version = ?")
                .bind(i64::from(m.version))
                .fetch_one(pool)
                .await?;
        if applied != 0 {
            continue;
        }
        let applied_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(m.up).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO _kian_migrations (version, name, applied_at) VALUES (?, ?, ?)")
            .bind(i64::from(m.version))
            .bind(m.name)
            .bind(applied_at)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// Versions applied to this database, ascending.
pub async fn applied_versions(pool: &SqlitePool) -> Result<Vec<u32>, StoreError> {
    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT version FROM _kian_migrations ORDER BY version ASC")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(v,)| v as u32).collect())
}

/// Applies every unapplied pg generation in order — the same runner
/// logic as [`migrate`], dispatched per backend. Read-only roles
/// are refused here by the server itself: a migration is a write, and
/// `open_postgres_read_only` never calls this.
pub async fn migrate_pg(pool: &PgPool) -> Result<(), StoreError> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _kian_migrations (\n    version BIGINT PRIMARY KEY,\n    name TEXT NOT NULL,\n    applied_at BIGINT NOT NULL\n)",
    )
    .execute(pool)
    .await?;
    for m in pg_chain() {
        let applied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _kian_migrations WHERE version = $1")
                .bind(i64::from(m.version))
                .fetch_one(pool)
                .await?;
        if applied != 0 {
            continue;
        }
        let applied_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(m.up).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO _kian_migrations (version, name, applied_at) VALUES ($1, $2, $3)")
            .bind(i64::from(m.version))
            .bind(m.name)
            .bind(applied_at)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// Versions applied to this postgres database, ascending.
pub async fn applied_versions_pg(pool: &PgPool) -> Result<Vec<u32>, StoreError> {
    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT version FROM _kian_migrations ORDER BY version ASC")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(v,)| v as u32).collect())
}

#[cfg(test)]
mod tests {
    use super::{Migration, chain, pg_chain};

    /// The chain must be adjacent and gapless starting at v0 (DESIGN
    /// §3.3): a database at any generation can always walk to head.
    #[test]
    fn migration_chain_is_adjacent_and_gapless_from_zero() {
        let chain = chain();
        assert!(!chain.is_empty(), "chain must contain v0");
        assert_eq!(chain[0].version, 0, "chain starts at v0");
        for w in chain.windows(2) {
            assert_eq!(
                w[1].version,
                w[0].version + 1,
                "gap in migration chain at v{}",
                w[0].version
            );
        }
        let versions: Vec<u32> = chain.iter().map(|m| m.version).collect();
        assert_eq!(versions, (0..chain.len() as u32).collect::<Vec<_>>());
    }

    /// Every entry carries a name and DDL — a malformed registration is
    /// caught here, not at open time on a user's database. (Relaxed in
    /// the chain is designed to grow beyond the events generation;
    /// a generation is well-formed when it names a store table it
    /// creates or extends with an additive column.)
    #[test]
    fn every_migration_is_well_formed() {
        for m in chain() {
            assert!(!m.name.is_empty(), "unnamed migration v{}", m.version);
            assert!(
                m.up.contains("CREATE TABLE")
                    || m.up.contains("CREATE INDEX")
                    || m.up.contains("ALTER TABLE cron_firings")
                    || m.name.contains("meta"),
                "migration v{} ({}) neither creates nor extends a store table — is it \
                 registered to the right chain?",
                m.version,
                m.name
            );
        }
    }

    /// v8 is a committed generation: its SQL is pinned byte-for-byte so a
    /// rewrite is caught instead of silently diverging from every already
    /// migrated database (the same doctrine as v0/v1).
    #[test]
    fn v8_generation_sql_is_frozen() {
        let v8: &Migration = &chain()[8];
        assert_eq!(v8.version, 8);
        assert_eq!(v8.name, "v8_cron_firing_session");
        assert_eq!(v8.up, "ALTER TABLE cron_firings ADD COLUMN session TEXT");
        let pg_v8: &Migration = &pg_chain()[8];
        assert_eq!(pg_v8.version, 8);
        assert_eq!(pg_v8.name, "v8_cron_firing_session");
        assert_eq!(pg_v8.up, "ALTER TABLE cron_firings ADD COLUMN session TEXT");
    }

    /// v0 is a committed generation: its SQL is pinned byte-for-byte so
    /// any edit — a rewrite of history — trips this test instead of
    /// silently diverging from every already-created database.
    #[test]
    fn v0_generation_sql_is_frozen() {
        let v0: &Migration = &chain()[0];
        assert_eq!(v0.version, 0);
        assert_eq!(v0.name, "v0_events");
        let golden = concat!(
            "CREATE TABLE IF NOT EXISTS events (\n",
            "    session TEXT NOT NULL,\n",
            "    seq INTEGER NOT NULL,\n",
            "    epoch INTEGER NOT NULL,\n",
            "    ts INTEGER NOT NULL,\n",
            "    kind TEXT NOT NULL,\n",
            "    version INTEGER NOT NULL,\n",
            "    envelope TEXT NOT NULL,\n",
            "    PRIMARY KEY (session, seq)\n",
            ")",
        );
        assert_eq!(v0.up, golden);
    }

    /// v1 is a committed generation: its SQL is pinned byte-for-byte so
    /// any edit — a rewrite of history — trips this test instead of
    /// silently diverging from every already-created database.
    #[test]
    fn v1_generation_sql_is_frozen() {
        let v1: &Migration = &chain()[1];
        assert_eq!(v1.version, 1);
        assert_eq!(v1.name, "v1_inbox_claims");
        let golden = concat!(
            "CREATE TABLE IF NOT EXISTS inputs (\n",
            "    id TEXT PRIMARY KEY,\n",
            "    session TEXT NOT NULL,\n",
            "    payload TEXT NOT NULL,\n",
            "    admitted_at INTEGER NOT NULL,\n",
            "    eligible_at INTEGER NOT NULL,\n",
            "    idempotency_key TEXT,\n",
            "    consumed_by_seq INTEGER,\n",
            "    UNIQUE (session, idempotency_key)\n",
            ");\n",
            "CREATE INDEX IF NOT EXISTS idx_inputs_pending ON inputs (session, eligible_at) WHERE consumed_by_seq IS NULL;\n",
            "CREATE TABLE IF NOT EXISTS claims (\n",
            "    session TEXT PRIMARY KEY,\n",
            "    epoch INTEGER NOT NULL,\n",
            "    holder TEXT NOT NULL,\n",
            "    ttl_ms INTEGER NOT NULL,\n",
            "    last_heartbeat INTEGER NOT NULL\n",
            ")",
        );
        assert_eq!(v1.up, golden);
    }

    /// v2 is a committed generation the moment it ships: pinned
    /// byte-for-byte like v0/v1 (append-only chain).
    #[test]
    fn v2_generation_sql_is_frozen() {
        let v2: &Migration = &chain()[2];
        assert_eq!(v2.version, 2);
        assert_eq!(v2.name, "v2_inbox_admit_order");
        assert_eq!(
            v2.up,
            concat!(
                "ALTER TABLE inputs ADD COLUMN admitted_seq INTEGER NOT NULL DEFAULT 0;\n",
                "UPDATE inputs SET admitted_seq = rowid;\n",
                "CREATE INDEX IF NOT EXISTS idx_inputs_admit_order ",
                "ON inputs (session, eligible_at, admitted_seq) WHERE consumed_by_seq IS NULL;",
            )
        );
    }

    /// The pg chain tracks the sqlite chain generation for generation:
    /// same versions, same names, same order (DESIGN §3.3). A
    /// divergence means one backend's schema drifted from the other's —
    /// caught here, not on a user's database.
    #[test]
    fn pg_chain_mirrors_sqlite_chain_versions_and_names() {
        let sqlite = chain();
        let pg = pg_chain();
        assert_eq!(
            sqlite.len(),
            pg.len(),
            "the two chains must stay the same length"
        );
        for (s, p) in sqlite.iter().zip(pg.iter()) {
            assert_eq!(s.version, p.version, "version mismatch at v{}", s.version);
            assert_eq!(s.name, p.name, "name mismatch at v{}", s.version);
        }
    }

    /// The pg chain is adjacent and gapless from v0, like the sqlite
    /// chain.
    #[test]
    fn pg_chain_is_adjacent_and_gapless_from_zero() {
        let pg = pg_chain();
        assert!(!pg.is_empty(), "pg chain must contain v0");
        assert_eq!(pg[0].version, 0, "pg chain starts at v0");
        let versions: Vec<u32> = pg.iter().map(|m| m.version).collect();
        assert_eq!(versions, (0..pg.len() as u32).collect::<Vec<_>>());
    }

    /// The pg generations carry pg types (TEXT/BIGINT), never sqlite's
    /// untyped INTEGER column spelling.
    #[test]
    fn pg_generations_use_pg_types() {
        for m in pg_chain() {
            assert!(
                !m.up.contains("INTEGER"),
                "pg migration v{} ({}) uses sqlite's INTEGER spelling",
                m.version,
                m.name
            );
        }
    }

    /// v0/v1 of the pg chain are committed generations the moment the
    /// chain ships: pinned byte-for-byte like their sqlite twins.
    #[test]
    fn pg_v0_and_v1_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[0].version, 0);
        assert_eq!(pg[0].name, "v0_events");
        assert_eq!(
            pg[0].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS events (\n",
                "    session TEXT NOT NULL,\n",
                "    seq BIGINT NOT NULL,\n",
                "    epoch BIGINT NOT NULL,\n",
                "    ts BIGINT NOT NULL,\n",
                "    kind TEXT NOT NULL,\n",
                "    version BIGINT NOT NULL,\n",
                "    envelope TEXT NOT NULL,\n",
                "    PRIMARY KEY (session, seq)\n",
                ")",
            )
        );
        assert_eq!(pg[1].version, 1);
        assert_eq!(pg[1].name, "v1_inbox_claims");
        assert_eq!(
            pg[1].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS inputs (\n",
                "    id TEXT PRIMARY KEY,\n",
                "    session TEXT NOT NULL,\n",
                "    payload TEXT NOT NULL,\n",
                "    admitted_at BIGINT NOT NULL,\n",
                "    eligible_at BIGINT NOT NULL,\n",
                "    idempotency_key TEXT,\n",
                "    consumed_by_seq BIGINT,\n",
                "    UNIQUE (session, idempotency_key)\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_inputs_pending ON inputs (session, eligible_at) WHERE consumed_by_seq IS NULL;\n",
                "CREATE TABLE IF NOT EXISTS claims (\n",
                "    session TEXT PRIMARY KEY,\n",
                "    epoch BIGINT NOT NULL,\n",
                "    holder TEXT NOT NULL,\n",
                "    ttl_ms BIGINT NOT NULL,\n",
                "    last_heartbeat BIGINT NOT NULL\n",
                ")",
            )
        );
    }

    /// The pg v2 generation is pinned like its sqlite twin.
    #[test]
    fn pg_v2_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[2].version, 2);
        assert_eq!(pg[2].name, "v2_inbox_admit_order");
        assert_eq!(
            pg[2].up,
            concat!(
                "ALTER TABLE inputs ADD COLUMN admitted_seq BIGINT GENERATED BY DEFAULT AS IDENTITY;\n",
                "CREATE INDEX IF NOT EXISTS idx_inputs_admit_order ",
                "ON inputs (session, eligible_at, admitted_seq) WHERE consumed_by_seq IS NULL;",
            )
        );
    }

    /// v3 is a committed generation the moment it ships: pinned
    /// byte-for-byte like v0–v2 (append-only chain).
    #[test]
    fn v3_generation_sql_is_frozen() {
        let v3: &Migration = &chain()[3];
        assert_eq!(v3.version, 3);
        assert_eq!(v3.name, "v3_projection_snapshots");
        assert_eq!(
            v3.up,
            concat!(
                "CREATE TABLE IF NOT EXISTS projection_snapshots (\n",
                "    session TEXT NOT NULL,\n",
                "    version INTEGER NOT NULL,\n",
                "    state TEXT NOT NULL,\n",
                "    context TEXT NOT NULL,\n",
                "    context_provenance TEXT NOT NULL,\n",
                "    digest TEXT NOT NULL,\n",
                "    verified INTEGER NOT NULL,\n",
                "    created_at INTEGER NOT NULL,\n",
                "    PRIMARY KEY (session, version)\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_projection_snapshots_verified ",
                "ON projection_snapshots (session, version) WHERE verified = 1;\n",
                "CREATE TABLE IF NOT EXISTS maintenance_leases (\n",
                "    scope TEXT PRIMARY KEY,\n",
                "    holder TEXT NOT NULL,\n",
                "    ttl_ms INTEGER NOT NULL,\n",
                "    last_heartbeat INTEGER NOT NULL\n",
                ")",
            )
        );
    }

    /// The pg v3 generation is pinned like its sqlite twin.
    #[test]
    fn pg_v3_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[3].version, 3);
        assert_eq!(pg[3].name, "v3_projection_snapshots");
        assert_eq!(
            pg[3].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS projection_snapshots (\n",
                "    session TEXT NOT NULL,\n",
                "    version BIGINT NOT NULL,\n",
                "    state TEXT NOT NULL,\n",
                "    context TEXT NOT NULL,\n",
                "    context_provenance TEXT NOT NULL,\n",
                "    digest TEXT NOT NULL,\n",
                "    verified BIGINT NOT NULL,\n",
                "    created_at BIGINT NOT NULL,\n",
                "    PRIMARY KEY (session, version)\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_projection_snapshots_verified ",
                "ON projection_snapshots (session, version) WHERE verified = 1;\n",
                "CREATE TABLE IF NOT EXISTS maintenance_leases (\n",
                "    scope TEXT PRIMARY KEY,\n",
                "    holder TEXT NOT NULL,\n",
                "    ttl_ms BIGINT NOT NULL,\n",
                "    last_heartbeat BIGINT NOT NULL\n",
                ")",
            )
        );
    }

    /// v4 is a committed generation the moment it ships: pinned
    /// byte-for-byte like v0–v3 (append-only chain).
    #[test]
    fn v4_generation_sql_is_frozen() {
        let v4: &Migration = &chain()[4];
        assert_eq!(v4.version, 4);
        assert_eq!(v4.name, "v4_materialized_views");
        assert_eq!(
            v4.up,
            concat!(
                "CREATE TABLE IF NOT EXISTS view_rows (\n",
                "    view TEXT NOT NULL,\n",
                "    session TEXT NOT NULL,\n",
                "    row_version INTEGER NOT NULL,\n",
                "    payload TEXT NOT NULL,\n",
                "    PRIMARY KEY (view, session)\n",
                ");\n",
                "CREATE TABLE IF NOT EXISTS view_state (\n",
                "    view TEXT PRIMARY KEY,\n",
                "    events_total INTEGER NOT NULL,\n",
                "    max_seq INTEGER NOT NULL,\n",
                "    digest TEXT NOT NULL,\n",
                "    built_at INTEGER NOT NULL\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events (kind, ts)",
            )
        );
    }

    /// The pg v4 generation is pinned like its sqlite twin.
    #[test]
    fn pg_v4_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[4].version, 4);
        assert_eq!(pg[4].name, "v4_materialized_views");
        assert_eq!(
            pg[4].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS view_rows (\n",
                "    view TEXT NOT NULL,\n",
                "    session TEXT NOT NULL,\n",
                "    row_version BIGINT NOT NULL,\n",
                "    payload TEXT NOT NULL,\n",
                "    PRIMARY KEY (view, session)\n",
                ");\n",
                "CREATE TABLE IF NOT EXISTS view_state (\n",
                "    view TEXT PRIMARY KEY,\n",
                "    events_total BIGINT NOT NULL,\n",
                "    max_seq BIGINT NOT NULL,\n",
                "    digest TEXT NOT NULL,\n",
                "    built_at BIGINT NOT NULL\n",
                ");\n",
                "CREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events (kind, ts)",
            )
        );
    }

    /// v5 is a committed generation the moment it ships: pinned
    /// byte-for-byte like v0–v4 (append-only chain).
    #[test]
    fn v5_generation_sql_is_frozen() {
        let v5: &Migration = &chain()[5];
        assert_eq!(v5.version, 5);
        assert_eq!(v5.name, "v5_archived_sessions");
        assert_eq!(
            v5.up,
            concat!(
                "CREATE TABLE IF NOT EXISTS archived_sessions (\n",
                "    session TEXT PRIMARY KEY,\n",
                "    last_seq INTEGER NOT NULL,\n",
                "    event_count INTEGER NOT NULL,\n",
                "    input_count INTEGER NOT NULL,\n",
                "    package_path TEXT NOT NULL,\n",
                "    events_sha256 TEXT NOT NULL,\n",
                "    inputs_sha256 TEXT NOT NULL,\n",
                "    manifest_sha256 TEXT NOT NULL,\n",
                "    archived_at INTEGER NOT NULL\n",
                ")",
            )
        );
    }

    /// The pg v5 generation is pinned like its sqlite twin.
    #[test]
    fn pg_v5_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[5].version, 5);
        assert_eq!(pg[5].name, "v5_archived_sessions");
        assert_eq!(
            pg[5].up,
            concat!(
                "CREATE TABLE IF NOT EXISTS archived_sessions (\n",
                "    session TEXT PRIMARY KEY,\n",
                "    last_seq BIGINT NOT NULL,\n",
                "    event_count BIGINT NOT NULL,\n",
                "    input_count BIGINT NOT NULL,\n",
                "    package_path TEXT NOT NULL,\n",
                "    events_sha256 TEXT NOT NULL,\n",
                "    inputs_sha256 TEXT NOT NULL,\n",
                "    manifest_sha256 TEXT NOT NULL,\n",
                "    archived_at BIGINT NOT NULL\n",
                ")",
            )
        );
    }

    /// v6 is a committed generation the moment it ships: pinned
    /// byte-for-byte like v0–v5 (append-only chain).
    #[test]
    fn v6_generation_sql_is_frozen() {
        let v6: &Migration = &chain()[6];
        assert_eq!(v6.version, 6);
        assert_eq!(v6.name, "v6_session_kind_index");
        assert_eq!(
            v6.up,
            concat!(
                "CREATE INDEX IF NOT EXISTS idx_events_session_kind_seq ",
                "ON events (session, kind, seq)",
            )
        );
    }

    /// The pg v6 generation is pinned like its sqlite twin.
    #[test]
    fn pg_v6_generation_sql_is_frozen() {
        let pg = pg_chain();
        assert_eq!(pg[6].version, 6);
        assert_eq!(pg[6].name, "v6_session_kind_index");
        assert_eq!(
            pg[6].up,
            concat!(
                "CREATE INDEX IF NOT EXISTS idx_events_session_kind_seq ",
                "ON events (session, kind, seq)",
            )
        );
    }
    /// v7 -> v8 real upgrade path (CRON-MODEL §12.1): a database created with
    /// the pre-v8 chain and an existing `cron_firings` row opens under this
    /// build, applies v8 additively, and the old row decodes with
    /// `session: NULL` (the settlement fallback).
    #[tokio::test]
    async fn v7_database_with_a_firing_row_upgrades_additively() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

        let path =
            std::env::temp_dir().join(format!("kian-migrate-v7-{}.sqlite", uuid::Uuid::new_v4()));
        {
            let connect = SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(connect)
                .await
                .expect("raw pool opens");
            sqlx::raw_sql(
                "CREATE TABLE _kian_migrations (\n    version INTEGER PRIMARY KEY,\n    name TEXT NOT NULL,\n    applied_at INTEGER NOT NULL\n)",
            )
            .execute(&pool)
            .await
            .expect("meta table");
            for migration in &super::chain()[..8] {
                sqlx::raw_sql(migration.up)
                    .execute(&pool)
                    .await
                    .expect("pre-v8 migration applies");
                sqlx::query(
                    "INSERT INTO _kian_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
                )
                .bind(i64::from(migration.version))
                .bind(migration.name)
                .bind(0i64)
                .execute(&pool)
                .await
                .expect("bookkeeping row");
            }
            sqlx::raw_sql(
                "INSERT INTO cron_firings (firing_id, schedule_id, fire_time, admitted_at, state)\nVALUES ('night-audit@2026-11-09T02:30:00Z', 'night-audit', '2026-11-09T02:30:00Z', 1761900000000, 'succeeded')",
            )
            .execute(&pool)
            .await
            .expect("legacy firing row");
            pool.close().await;
        }

        // The new build opens the file: migrate() applies v8 only.
        let store = crate::EventStore::open(&path).await.expect("open store");
        let versions = super::applied_versions(store.pool())
            .await
            .expect("versions");
        assert!(versions.contains(&8), "v8 applied: {versions:?}");
        let row = store
            .load_firing("night-audit@2026-11-09T02:30:00Z")
            .await
            .expect("load legacy firing")
            .expect("row");
        assert_eq!(row.session, None, "pre-v8 row decodes with no session");
        store.close().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
