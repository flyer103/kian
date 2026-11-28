//! Rebuildable admitted-run index for `kian job list` and
//! `kian job status <job>` (JOB-MODEL §15.2).
//!
//! The index is a read cache over A1 rows. It is never consulted by
//! execution, placement or settlement, and losing it loses no fact: a cold
//! read discovers every run from the bounded global admitted-input read,
//! then folds each run's own stream when it exists.

use std::collections::BTreeMap;

use kian_core::model::SessionId;
use kian_store::EventStore;
use kian_superagent::job::plan;
use kian_superagent::job::status::{self, JobMarker, RunStatus};

/// The largest admitted-event slice one cold index rebuild reads. The store
/// read is ordered and bounded; this limit only bounds the convenience
/// surface, never execution or truth.
pub const RUN_INDEX_SCAN_LIMIT: u64 = 100_000;

/// One run discovered from its A1 marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunIndexEntry {
    pub run_id: String,
    pub job_id: String,
    pub slot: String,
    pub session: SessionId,
    pub started_ms: u64,
    pub status: RunStatus,
}

/// Runs keyed by id plus the newest-first ordering needed by job lookups.
#[derive(Debug, Clone, Default)]
pub struct RunIndex {
    runs: BTreeMap<String, RunIndexEntry>,
    by_job: BTreeMap<String, Vec<String>>,
    pub truncated: bool,
}

impl RunIndex {
    /// The most recent run admitted for a job, independent of trigger.
    #[must_use]
    pub fn last_run(&self, job_id: &str) -> Option<&RunIndexEntry> {
        self.by_job
            .get(job_id)
            .and_then(|runs| runs.first())
            .and_then(|run_id| self.runs.get(run_id))
    }
}

/// Rebuild the index from admitted run rows. The run-key row is the A1
/// authority; a consumed session is folded from its stream, while an
/// unconsumed row yields the same pending view the direct CLI reports.
pub async fn rebuild(store: &EventStore) -> Result<RunIndex, String> {
    let rows = store
        .input_rows_across_sessions(RUN_INDEX_SCAN_LIMIT)
        .await
        .map_err(|error| format!("admitted run index: {error}"))?;
    let truncated = rows.len() as u64 == RUN_INDEX_SCAN_LIMIT;
    let mut admitted: BTreeMap<SessionId, (u64, JobMarker)> = BTreeMap::new();
    for row in rows {
        let Some(marker_value) = row.payload.get(status::JOB_KEY) else {
            continue;
        };
        let Ok(marker) = status::parse_marker(marker_value) else {
            continue;
        };
        let expected_key = plan::run_input_key(&marker.run_id);
        if row.idempotency_key.as_deref() != Some(expected_key.as_str()) {
            continue;
        }
        admitted
            .entry(row.session)
            .or_insert((row.admitted_at, marker));
    }

    let mut index = RunIndex {
        truncated,
        ..RunIndex::default()
    };
    for (session, (admitted_at, marker)) in admitted {
        let events = store
            .rebuild(session)
            .await
            .map_err(|error| format!("run index session {}: {error}", session.0))?;
        let view = if events.is_empty() {
            status::pending_view(&marker, None, None, admitted_at)
        } else {
            let Some(view) = status::view(&events) else {
                continue;
            };
            view
        };
        let entry = RunIndexEntry {
            run_id: view.run_id.clone(),
            job_id: view.job_id.clone(),
            slot: view.slot.clone(),
            session,
            started_ms: view.started_ms,
            status: view.status,
        };
        index
            .by_job
            .entry(entry.job_id.clone())
            .or_default()
            .push(entry.run_id.clone());
        index.runs.insert(entry.run_id.clone(), entry);
    }
    for run_ids in index.by_job.values_mut() {
        run_ids.sort_by(|left, right| {
            let left = &index.runs[left];
            let right = &index.runs[right];
            right
                .started_ms
                .cmp(&left.started_ms)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
    }
    Ok(index)
}
