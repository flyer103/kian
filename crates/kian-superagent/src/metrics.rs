//! Router batching metrics (card C41): the article's §7.2 observability
//! shape over the Router drain — per judge turn, **inputs consumed**
//! (batch size), **turn wall duration** (per-request) and
//! **duration/inputs** (per-decision amortized latency).
//!
//! Bundle-owned counters in the `SubscriptionMetrics` pattern (plain
//! counters a `/metrics` snapshot can read cheaply), plus a **bounded**
//! batch-size histogram: buckets 1…128 with one overflow bucket, so the
//! series shape is fixed no matter how large a sweep gets. Recording and
//! snapshotting share one lock, so a snapshot is a consistent
//! point-in-time copy (derived means always agree with the counters
//! shown next to them) and every field is monotone.
//!
//! Timing comes from [`TurnClock`] — a bundle-owned clock trait on
//! purpose: the crate's iron rule permits no `kian-orchestrator` symbol
//! beyond `WakeOutcome`, and observability must not widen the port
//! surface. Assembly injects [`SystemTurnClock`]; tests drive
//! [`ManualTurnClock`] so duration assertions are exact.
//!
//! Pure observability: nothing here feeds a decision, and recording
//! never emits an event. [`crate::router::RouterDriver`] records around
//! each judge turn; a read failure on the stream merely skips the
//! record (metrics must never change behavior).

use std::sync::Arc;
use std::sync::Mutex;

use serde_json::{Value, json};

/// Batch-size histogram bucket upper bounds (bounded set). A batch
/// above the largest bound lands in the overflow bucket (`"128+"`).
pub const ROUTER_BATCH_BUCKETS: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 128];

/// Wall-clock source for judge-turn timing. Bundle-owned (see the
/// module doc): assembly injects the system clock, tests a manual one.
pub trait TurnClock: Send + Sync {
    /// Current time, unix epoch milliseconds.
    fn now_ms(&self) -> u64;
}

/// The real clock (unix epoch milliseconds).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemTurnClock;

impl TurnClock for SystemTurnClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A manually advanced clock for deterministic duration tests.
#[derive(Debug, Default)]
pub struct ManualTurnClock(std::sync::atomic::AtomicU64);

impl ManualTurnClock {
    #[must_use]
    pub fn new(start_ms: u64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(start_ms))
    }

    /// Advances the clock, returning the new time.
    pub fn advance(&self, by_ms: u64) -> u64 {
        use std::sync::atomic::Ordering;
        self.0.fetch_add(by_ms, Ordering::Relaxed) + by_ms
    }
}

impl TurnClock for ManualTurnClock {
    fn now_ms(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One recorded judge turn: the batch it consumed and its wall
/// duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnRecord {
    /// Inputs consumed by the judge turn (the batch size).
    pub batch: u64,
    /// Wall duration of the judge turn, milliseconds.
    pub duration_ms: u64,
}

#[derive(Debug, Default)]
struct RouterMetricsState {
    judge_turns: u64,
    inputs_consumed: u64,
    turn_duration_ms_total: u64,
    /// `ROUTER_BATCH_BUCKETS.len() + 1` counters — the last is overflow.
    buckets: [u64; ROUTER_BATCH_BUCKETS.len() + 1],
    last_turn: Option<TurnRecord>,
}

/// Counters + a bounded batch-size histogram over the Router's judge
/// turns (article §7.2 shape). All fields monotone; [`RouterMetrics::snapshot`]
/// is taken under the same lock a record holds, so it is
/// snapshot-consistent.
#[derive(Debug, Default)]
pub struct RouterMetrics {
    state: Mutex<RouterMetricsState>,
}

impl RouterMetrics {
    /// A fresh shared handle (the shape assembly mounts).
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one completed judge turn. A turn that consumed nothing
    /// is not a decision and is ignored — the amortized series divides
    /// by `inputs_consumed`, so a zero batch must never land here.
    pub fn record_turn(&self, batch: u64, duration_ms: u64) {
        if batch == 0 {
            return;
        }
        let mut state = self.state.lock().expect("router metrics lock");
        state.judge_turns += 1;
        state.inputs_consumed += batch;
        state.turn_duration_ms_total = state.turn_duration_ms_total.saturating_add(duration_ms);
        state.buckets[bucket_index(batch)] += 1;
        state.last_turn = Some(TurnRecord { batch, duration_ms });
    }

    /// Judge turns recorded so far.
    #[must_use]
    pub fn judge_turns(&self) -> u64 {
        self.state.lock().expect("router metrics lock").judge_turns
    }

    /// The §7.2-shaped snapshot: `judge_turns`, `inputs_consumed`
    /// (batch size), `turn_duration_ms_total` / `mean_turn_duration_ms`
    /// (per-request), `amortized_ms_per_input` (duration/inputs,
    /// per-decision), the bounded `batch_histogram` and `last_turn`.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let state = self.state.lock().expect("router metrics lock");

        // The bounded bucket set is fully materialized — the series
        // shape is stable across snapshots.
        let mut histogram = serde_json::Map::with_capacity(ROUTER_BATCH_BUCKETS.len() + 1);
        for (idx, upper) in ROUTER_BATCH_BUCKETS.iter().enumerate() {
            histogram.insert(upper.to_string(), json!(state.buckets[idx]));
        }
        let overflow_key = format!("{}+", ROUTER_BATCH_BUCKETS[ROUTER_BATCH_BUCKETS.len() - 1]);
        histogram.insert(
            overflow_key,
            json!(state.buckets[ROUTER_BATCH_BUCKETS.len()]),
        );

        // Derived means come from the same locked copy — internally
        // consistent with the counters shown beside them.
        let mean_turn = if state.judge_turns == 0 {
            0.0
        } else {
            state.turn_duration_ms_total as f64 / state.judge_turns as f64
        };
        let amortized = if state.inputs_consumed == 0 {
            0.0
        } else {
            state.turn_duration_ms_total as f64 / state.inputs_consumed as f64
        };
        let last_turn = state.last_turn.map(|t| {
            json!({
                "batch": t.batch,
                "duration_ms": t.duration_ms,
                "amortized_ms": t.duration_ms as f64 / t.batch as f64,
            })
        });

        json!({
            "judge_turns": state.judge_turns,
            "inputs_consumed": state.inputs_consumed,
            "turn_duration_ms_total": state.turn_duration_ms_total,
            "mean_turn_duration_ms": mean_turn,
            "amortized_ms_per_input": amortized,
            "batch_histogram": Value::Object(histogram),
            "last_turn": last_turn,
        })
    }
}

/// Histogram slot for `batch`: the first upper bound that covers it,
/// else the overflow slot.
fn bucket_index(batch: u64) -> usize {
    ROUTER_BATCH_BUCKETS
        .iter()
        .position(|&upper| batch <= upper)
        .unwrap_or(ROUTER_BATCH_BUCKETS.len())
}

/// The sink [`crate::router::RouterDriver`] records through: the
/// counters plus the clock that times each judge turn.
#[derive(Clone)]
pub struct RouterTurnRecorder {
    metrics: Arc<RouterMetrics>,
    clock: Arc<dyn TurnClock>,
}

impl RouterTurnRecorder {
    #[must_use]
    pub fn new(metrics: Arc<RouterMetrics>, clock: Arc<dyn TurnClock>) -> Self {
        Self { metrics, clock }
    }

    #[must_use]
    pub fn metrics(&self) -> &RouterMetrics {
        &self.metrics
    }

    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::{ManualTurnClock, ROUTER_BATCH_BUCKETS, RouterMetrics, TurnClock};

    #[test]
    fn buckets_partition_batches_into_a_bounded_set() {
        let metrics = RouterMetrics::default();
        // batch sizes chosen to hit the 1-bucket, an interior bucket,
        // the top bucket and the overflow.
        metrics.record_turn(1, 0);
        metrics.record_turn(3, 0); // -> 4
        metrics.record_turn(5, 0); // -> 8
        metrics.record_turn(128, 0); // -> 128
        metrics.record_turn(10_000, 0); // -> overflow
        let snap = metrics.snapshot();
        let hist = &snap["batch_histogram"];
        // Every bucket of the bounded set is materialized, in order.
        let mut keys: Vec<String> = hist
            .as_object()
            .expect("histogram object")
            .keys()
            .cloned()
            .collect();
        assert_eq!(keys.len(), ROUTER_BATCH_BUCKETS.len() + 1);
        keys.sort_by_key(|k| k.trim_end_matches('+').parse::<u64>().unwrap());
        let mut expected: Vec<String> =
            ROUTER_BATCH_BUCKETS.iter().map(|u| u.to_string()).collect();
        expected.push(format!(
            "{}+",
            ROUTER_BATCH_BUCKETS[ROUTER_BATCH_BUCKETS.len() - 1]
        ));
        assert_eq!(keys, expected);
        assert_eq!(hist["1"], serde_json::json!(1));
        assert_eq!(hist["4"], serde_json::json!(1));
        assert_eq!(hist["8"], serde_json::json!(1));
        assert_eq!(hist["16"], serde_json::json!(0));
        assert_eq!(hist["128"], serde_json::json!(1));
        assert_eq!(hist["128+"], serde_json::json!(1));
    }

    #[test]
    fn counters_are_monotone_and_snapshot_consistent() {
        let metrics = RouterMetrics::default();
        metrics.record_turn(2, 100);
        metrics.record_turn(1, 50);
        let first = metrics.snapshot();
        assert_eq!(first["judge_turns"], serde_json::json!(2));
        assert_eq!(first["inputs_consumed"], serde_json::json!(3));
        assert_eq!(first["turn_duration_ms_total"], serde_json::json!(150));
        // Derived means agree with the counters in the same snapshot.
        assert_eq!(first["mean_turn_duration_ms"], serde_json::json!(75.0));
        assert_eq!(first["amortized_ms_per_input"], serde_json::json!(50.0));
        assert_eq!(first["last_turn"]["batch"], serde_json::json!(1));
        assert_eq!(first["last_turn"]["duration_ms"], serde_json::json!(50));
        assert_eq!(first["last_turn"]["amortized_ms"], serde_json::json!(50.0));

        metrics.record_turn(4, 250);
        let second = metrics.snapshot();
        for field in ["judge_turns", "inputs_consumed", "turn_duration_ms_total"] {
            assert!(
                second[field].as_u64().unwrap() >= first[field].as_u64().unwrap(),
                "{field} must be monotone"
            );
        }
        assert_eq!(second["judge_turns"], serde_json::json!(3));
        assert_eq!(second["inputs_consumed"], serde_json::json!(7));
        assert_eq!(second["turn_duration_ms_total"], serde_json::json!(400));
        assert_eq!(
            second["mean_turn_duration_ms"],
            serde_json::json!(400.0 / 3.0)
        );
        assert_eq!(
            second["amortized_ms_per_input"],
            serde_json::json!(400.0 / 7.0)
        );
    }

    #[test]
    fn a_turn_that_consumed_nothing_is_not_a_decision() {
        let metrics = RouterMetrics::default();
        metrics.record_turn(0, 999);
        let snap = metrics.snapshot();
        assert_eq!(snap["judge_turns"], serde_json::json!(0));
        assert_eq!(snap["inputs_consumed"], serde_json::json!(0));
        assert!(snap["last_turn"].is_null());
        assert_eq!(snap["mean_turn_duration_ms"], serde_json::json!(0.0));
        assert_eq!(snap["amortized_ms_per_input"], serde_json::json!(0.0));
    }

    #[test]
    fn manual_turn_clock_advances_only_when_told() {
        let clock = ManualTurnClock::new(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        assert_eq!(clock.advance(3_000), 4_000);
        assert_eq!(clock.now_ms(), 4_000);
    }
}
