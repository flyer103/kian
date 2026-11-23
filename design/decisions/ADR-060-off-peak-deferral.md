# ADR-060: Off-peak deferral is an inbox eligibility gate, not a second scheduler

Status: accepted (owner-approved roadmap semantics, 2026-11-22; P4 slice)
Supersedes: none
Related: ADR-053 (price book), ADR-055 (session budget gate), CRON-MODEL,
DESIGN section 2.3 (admit-wake-drain), EVENT-MODEL (input/admitted)

## Context

Provider peak pricing (ADR-053) makes some wall-clock windows cost up to
the configured peak multiplier. Batch-shaped work - cron firings, jobs,
explicit API submissions - is indifferent to *when* the result arrives and
would rather run off peak. Interactive work is not: a user waiting on
turns must never be delayed by a pricing calendar.

The runtime already has the exact primitive this needs: an inbox row
carries eligible_at, the store refuses to hand out a row before that
instant ("SELECT ... WHERE consumed_by_seq IS NULL AND eligible_at <=
now"), and every consumer - drain take, startup scan, has_open_work -
already gates on it. What does not exist is (a) any producer that sets a
future eligible_at, and (b) any wake-up path that fires when a future row
becomes due: the wake registry deliberately holds no timers, and the only
sweep (startup rebuild / lease recovery) runs at process start or on a
lease-expiry schedule.

## Decision

**1. Deferral is a pure planning decision plus the existing eligibility
gate.** Off-peak planning lives in kian-cli: PriceBook::next_off_peak_ms
reads the same peak windows and holidays the cost projection uses, and
DeferCalendar turns a policy into an eligible_at instant. The only effect
on the runtime is InputRow.eligible_at. No new event kind, no new wake
transport, no second scheduler, no new crate, and no change to the shape
of an admitted input.

**2. Interactive inputs never defer.** CLI runs, channel messages and
steer are always immediate. Only explicit opt-ins defer: API submit/queue
with defer: off_peak, and a cron schedule that declares defer: off_peak
(cron defaults to immediate - a wall-clock timer that silently runs late
is a bug, not a feature). steer refuses defer. A request that asks for
deferral when no defer: section is configured is refused (-32602) rather
than silently admitted immediately.

**3. The plan is the next off-peak instant, capped.** Already off peak ->
immediate. Next off-peak beyond defer.max_wait_ms -> immediate, with the
reason recorded (why-didn't-wait). Because the cap is a bound, a
configuration in which no off-peak instant exists (windows covering the
whole week) refuses boot instead of deferring forever.

**4. cron records the deferral decision in the durable row.** The firing
payload gains an additive, non-context defer object beside cron:
{policy, deferred, eligible_at_ms, reason}. The input/admitted event is
therefore the durable audit of why the firing did not run yet. The firing
itself is claimed normally; the row simply stays ineligible until due.
Overlap skip is unaffected: has_open_work counts only currently eligible
work, so a deferred row never makes its session look busy. Worktree
schedules refuse defer: they run as isolated external turns, not as
session rows, so there is nothing to gate.

**5. Due wake is a sweep, not a timer per row.** Engine::due_wake_sweep
reuses the startup rebuild's scan - rows with eligible_at <= now, the
same frozen / pending-calls filters - and wakes those sessions. serve
arms the ticker only when the defer: section is configured (period
defer.sweep_interval_ms, default 5000 ms). Correctness never depends on
the sweep: once due, any ordinary wake, a restart, or the next submit
also finds the row (wake is advisory, constraint 6). The sweep only
bounds the latency of a deferred row that nothing else touches.

**6. The time model is the price book's, unchanged.** Windows carry their
own IANA timezone; the holiday table has an explicit timezone; an empty
holiday table can only over-estimate peak (conservative, never
under-estimates). Minute granularity is enough: eligible_at is a gate,
not a promise of execution time, and the sweep period is coarser anyway.

**7. The API delta is additive.** submit/queue accept an optional defer:
off_peak; with it the result gains eligible_at_ms and deferred, without
it the response is byte-identical to the pre-P4 shape. The ControlPlane
trait gains deferred submit/queue methods whose default implementation
answers an honest unsupported error, so planes that cannot defer (tests,
embedded hosts) keep compiling and keep their exact prior behavior.

**8. Recorded limit.** Job deferral (defer_budget_ms with a why-not-wait
record) has no landing in this base: the job runtime is not in this tree.
When it lands, it calls the same DeferCalendar and Engine::admit_at; no
new core primitive is needed.

## Alternatives considered

- A timer wheel holding one timer per delayed row. Rejected: a timer is
  not durable, the row already is; a sweep over the existing eligibility
  index is restart-safe, replication-safe (the predicate lives in the
  store), and adds no invariant.
- Delaying the cron claim instead of the row. Rejected: the claim is
  execution-shaped (control plane reads, execution writes) and a delayed
  claim would push the decision into the dispatching layer, losing the
  durable audit that the admitted row already gives us.
- Auto-deferring interactive work when a peak multiplier applies.
  Rejected: owner decision - surprise latency on a user-facing turn is
  worse than paying for it.
- A new deferred-input event kind. Rejected: the row and its input/admitted
  event already persist what was sent (constraint 2); the defer decision
  is an admission attribute, not a model-visible fact.

## Consequences

Positive: batch work shifts off peak using machinery the runtime already
upholds; crash safety is inherited (the row is durable before any wake);
there is no second schedule to reconcile with cron's cursor or with the
wake registry; the feature is inert when the section is absent.

Negative: a deferred row can start up to one sweep period (default 5 s)
late; deferral only covers resident sessions and wall-clock cron; the
decision is local to one replica (safe, because it is stored, not
in-memory).

## Acceptance

- Pure planner: next off-peak for already-off-peak, window edges,
  cross-midnight, cross-week, DST gaps, holidays, and the
  never-off-peak case; calendar cap reason.
- Engine: a future eligible_at row is neither consumed nor woken early;
  due_wake_sweep wakes it once due and the session completes; the
  startup rebuild and all pre-P4 admit paths are byte-identical.
- cron: default immediate is byte-identical; defer off_peak yields a
  future eligible_at and the defer audit key in the durable payload.
- Config: boot refuses defer without peak windows, windows covering the
  whole week, a zero sweep interval, worktree + defer, and cron defer
  without the defer section.
- API: defer parameter and additive result fields; refusal without the
  section; steer rejects defer; every non-defer response unchanged.
