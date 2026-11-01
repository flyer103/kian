//! Wake transport — the postgres LISTEN/NOTIFY accelerator (C52, ADR-009).
//!
//! Wakes stay **advisory** (AGENTS.md constraint 6, invariant 2): the
//! in-process [`crate::inbox`]-adjacent `WakeRegistry` (owned by
//! kian-orchestrator) keeps its fold/lose/rebuild semantics untouched;
//! this module only adds a *cross-process* accelerator so a replica that
//! admits an input can hint every other replica immediately.
//!
//! Ordering rule (the card's named guarantee): the notify fires
//! **after the input insert commits** — never inside the transaction. A
//! notify observed before its row is visible would invert
//! durable-before-wake. Because `EventStore::admit` is a single INSERT
//! statement (its own implicit transaction), the caller issues the
//! notify only after `admit` returned — by then the row is committed
//! and visible to every later read.
//!
//! Loss bounds (ADR-009 decision 4): a missed notify — listener
//! reconnect, replica down, notify error — delays nothing that
//! correctness depends on. The existing recovery sweep ticker plus each
//! replica's startup inbox scan bound the window, exactly as they
//! already bound losable in-process wakes.
//!
//! sqlite behavior is unchanged: there is one process, the in-process
//! registry is the wake transport, and [`EventStore::notify_wake`] is a
//! no-op.

use kian_core::model::SessionId;
use sqlx::postgres::{PgListener, PgPool};

use crate::event_store::EventStore;
use crate::inbox::InboxError;

/// The postgres channel wakes are published on. The payload is the
/// session uuid in its canonical string form.
pub const WAKE_CHANNEL: &str = "kian_wake";

impl EventStore {
    /// Publishes one advisory wake for `session` on the wake transport.
    ///
    /// - **postgres**: `pg_notify('kian_wake', '<session-uuid>')` as its
    ///   own statement — the caller invokes this strictly *after* the
    ///   input insert committed, so any listener that observes the
    ///   notification can already see the durable row.
    /// - **sqlite**: a no-op — single-process mode; the in-process wake
    ///   registry (kian-orchestrator) is the transport and its
    ///   semantics are unchanged.
    ///
    /// Best-effort by contract: callers log failures and move on (a
    /// wake is losable by invariant; the sweep and the startup scan
    /// bound the miss). Never returns an error on sqlite.
    pub async fn notify_wake(&self, session: SessionId) -> Result<(), InboxError> {
        match self {
            EventStore::Sqlite(_) => Ok(()),
            EventStore::Postgres(pool) => {
                sqlx::query("SELECT pg_notify($1::text, $2::text)")
                    .bind(WAKE_CHANNEL)
                    .bind(session.0.to_string())
                    .execute(pool)
                    .await?;
                Ok(())
            }
        }
    }
}

/// Starts listening for wake notifications on a **dedicated pooled
/// connection** (sqlx's `PgListener::connect_with` checks out one
/// connection from the pool and holds it for the listener's lifetime —
/// AGENTS.md: PgListener gets a dedicated connection).
///
/// The returned listener is a pull surface: the replica's relay task
/// (`kian serve`'s wake listener) calls `recv()` and fires its local
/// engine `wake` per notification. Reconnect handling is the caller's:
/// on a listener error, drop it and call this again — the gap is a
/// losable-wake window bounded by the sweep ticker and the startup
/// scan, never a correctness hole.
pub async fn listen_for_wakes(pool: &PgPool) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(WAKE_CHANNEL).await?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::WAKE_CHANNEL;
    use crate::event_store::EventStore;

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-store-wake-{}.sqlite", Uuid::new_v4()))
    }

    /// sqlite is single-process: `notify_wake` is a successful no-op —
    /// the in-process registry is the transport and nothing here ever
    /// touches the database (C52: "sqlite behavior is unchanged").
    #[tokio::test]
    async fn sqlite_notify_wake_is_a_successful_noop() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        let sid = kian_core::model::SessionId::new();
        store
            .notify_wake(sid)
            .await
            .expect("sqlite notify must be Ok(())");
        store.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The channel name is frozen wire surface: replicas on different
    /// builds must agree on it.
    #[test]
    fn wake_channel_name_is_frozen() {
        assert_eq!(WAKE_CHANNEL, "kian_wake");
    }
}
