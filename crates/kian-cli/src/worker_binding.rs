//! The store-wide binding authority for worker spawn admission.
//!
//! JOB-MODEL section 9.5 / WORKER-MODEL section 3.2: one worker has at
//! most one live bound worker session, and the serialized spawn admission
//! is the only authority. The job conductor and the Router both allocate
//! children through the same `WorkerSpawner`, so admission needs a view over
//! every committed `thread/started` fact, not just one parent's stream.
//!
//! The view is derived, never stored: scan the event stream, project the
//! bound threads, and ask the worker streams port for each candidate's
//! liveness. The new dispatch record written by the spawning conductor is
//! the caller's own reservation and is excluded explicitly.

use std::sync::Arc;

use kian_core::model::SessionId;
use kian_store::EventStore;
use kian_superagent::worker::{BusyOwner, WorkerStreamsError, busy_owner_excluding};

use crate::superagent::StoreWorkerStreams;

/// Read-only projection over every committed binding fact.
pub struct StoreBindingAdmission {
    store: EventStore,
    streams: Arc<StoreWorkerStreams>,
}

impl StoreBindingAdmission {
    /// A fresh view over the store's committed events.
    #[must_use]
    pub fn new(store: EventStore) -> Arc<Self> {
        let streams = StoreWorkerStreams::new(store.clone());
        Arc::new(Self { store, streams })
    }
}

impl kian_superagent::worker::BindingAdmissionPort for StoreBindingAdmission {
    fn check_binding(
        &self,
        worker_id: &str,
        candidate_thread: uuid::Uuid,
        candidate_session: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    ) -> futures::future::BoxFuture<'static, Result<Option<BusyOwner>, WorkerStreamsError>> {
        let worker_id = worker_id.to_string();
        let store = self.store.clone();
        let streams = Arc::clone(&self.streams);
        Box::pin(async move {
            let events = crate::cache_metrics::thread_started_events(&store)
                .await
                .map_err(|e| WorkerStreamsError(format!("binding view read failed: {e}")))?;
            busy_owner_excluding(
                &events,
                streams.as_ref(),
                &worker_id,
                now_ms,
                ttl_ms,
                Some((candidate_thread, candidate_session)),
            )
            .await
        })
    }
}
