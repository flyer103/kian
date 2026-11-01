//! Schema versioning: adjacent migration chain (DESIGN §3.3).
//!
//! vN→vN+1 is one [`Migration`] entry; committed generations are never
//! renamed, replaced or deleted — a new generation is published
//! exclusively by appending to the chain. v0 is current in Phase 1 and
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

/// v0 — the append-only events table (card C5). Frozen golden: committed
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

/// v1 — the inbox and claim/lease tables (card C6). Frozen golden like
/// v0: committed generations are never rewritten.
///
/// `inputs` rows are the durable inbox (constraint 6: inputs are durable
/// rows before any wake fires). Eligibility is *event-driven*: a row stops
/// being eligible only when its `consumed_by_seq` references a committed
/// event of the same session — the row itself is never removed before
/// that consumption event exists (and never removed after, either: the
/// inbox is audit-visible).
///
/// `claims` holds the lease per session. `epoch` doubles as the durable
/// epoch high-water mark: issuance always takes
/// max(claims.epoch, events epoch floor) + 1, so epochs strictly increase
/// per session across process restarts and across claims whose
/// `drain/claimed` event has not been appended yet (the window C5
/// flagged). Leases are liveness only (ADR-005) — expiry never grants
/// permission to accept stale writes.
const V1_INBOX_CLAIMS: Migration = Migration {
    version: 1,
    name: "v1_inbox_claims",
    up: "CREATE TABLE IF NOT EXISTS inputs (\n    id TEXT PRIMARY KEY,\n    session TEXT NOT NULL,\n    payload TEXT NOT NULL,\n    admitted_at INTEGER NOT NULL,\n    eligible_at INTEGER NOT NULL,\n    idempotency_key TEXT,\n    consumed_by_seq INTEGER,\n    UNIQUE (session, idempotency_key)\n);\nCREATE INDEX IF NOT EXISTS idx_inputs_pending ON inputs (session, eligible_at) WHERE consumed_by_seq IS NULL;\nCREATE TABLE IF NOT EXISTS claims (\n    session TEXT PRIMARY KEY,\n    epoch INTEGER NOT NULL,\n    holder TEXT NOT NULL,\n    ttl_ms INTEGER NOT NULL,\n    last_heartbeat INTEGER NOT NULL\n)",
};

/// The registered adjacent chain, ordered v0 → head. Extensible: append
/// the next [`Migration`]; never edit a committed entry.
pub fn chain() -> &'static [Migration] {
    &[V0_EVENTS, V1_INBOX_CLAIMS, V2_INBOX_ADMIT_ORDER]
}

/// v2 — the inbox admission ordinal (card C57). The take queries
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

/// v0 (postgres chain, C50) — same version and name as the sqlite
/// generation, pg DDL (TEXT/BIGINT; DESIGN §3.3). Envelope stays verbatim
/// TEXT so the same event stream exported from either backend is
/// byte-identical (the C50 export-parity golden).
const PG_V0_EVENTS: Migration = Migration {
    version: 0,
    name: "v0_events",
    up: "CREATE TABLE IF NOT EXISTS events (\n    session TEXT NOT NULL,\n    seq BIGINT NOT NULL,\n    epoch BIGINT NOT NULL,\n    ts BIGINT NOT NULL,\n    kind TEXT NOT NULL,\n    version BIGINT NOT NULL,\n    envelope TEXT NOT NULL,\n    PRIMARY KEY (session, seq)\n)",
};

/// v1 (postgres chain, C50) — the inbox and claim/lease tables, pg DDL.
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

/// The postgres chain: same versions, same names, same ordering as
/// [`chain`] — the two backends' schemas track each other generation for
/// generation (DESIGN §3.3), enforced by
/// `pg_chain_mirrors_sqlite_chain_versions_and_names`.
pub fn pg_chain() -> &'static [Migration] {
    &[PG_V0_EVENTS, PG_V1_INBOX_CLAIMS, PG_V2_INBOX_ADMIT_ORDER]
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
/// logic as [`migrate`], dispatched per backend (C50). Read-only roles
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
    /// C6: the chain is designed to grow beyond the events generation;
    /// a generation is well-formed when it names a store table it
    /// creates.)
    #[test]
    fn every_migration_is_well_formed() {
        for m in chain() {
            assert!(!m.name.is_empty(), "unnamed migration v{}", m.version);
            assert!(
                m.up.contains("CREATE TABLE")
                    || m.up.contains("CREATE INDEX")
                    || m.name.contains("meta"),
                "migration v{} ({}) creates no table — is it registered to the right chain?",
                m.version,
                m.name
            );
        }
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
    /// same versions, same names, same order (C50; DESIGN §3.3). A
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
}
