//! Schema versioning: adjacent migration chain (DESIGN §3.3).
//!
//! vN→vN+1 is one [`Migration`] entry; committed generations are never
//! renamed, replaced or deleted — a new generation is published
//! exclusively by appending to the chain. v0 is current in Phase 1 and
//! the chain is extensible, so an old DB with a v0 stream reopens
//! cleanly (each unapplied generation runs in order, exactly once).

use std::time::{SystemTime, UNIX_EPOCH};

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
    &[V0_EVENTS, V1_INBOX_CLAIMS]
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

#[cfg(test)]
mod tests {
    use super::{Migration, chain};

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
}
