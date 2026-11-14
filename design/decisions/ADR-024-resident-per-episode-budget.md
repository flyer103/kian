# ADR-024: Resident iterations are budgeted per episode

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: `design/DESIGN.md` §4.6 (Resident re-arm — the tracked follow-up this ADR closes),
  `crates/kian-loop` (`ReActConfig`/budget), `crates/kian-superagent` (`RouterDriver` park),
  ADR-021 (runtime membership — unrelated), `docs/SPEC.md` R13.
- **Context**: the ReAct loop's iteration budget is **lifetime-scoped** — the counter is the number
  of `turn/start` events ever committed on the session ([DESIGN §4.6](../DESIGN.md)). That is correct
  for a worker or plain session, which is short-lived, but wrong for a **resident** session (the
  Router, the watchdog): a long-lived resident necessarily exhausts any finite budget, the engine
  maps the loop's `Stopped` to `session/ended{completed}`, the stream freezes, and every later
  admitted input stays pending until a process restart runs the boot-time re-arm. The owner
  deployment hit this exactly: with the default `max_iterations = 32` the Router replied for 32
  turns and then froze; WeChat messages were admitted (durable rows) but never consumed and got no
  reply. The boot re-arm is the designed recovery, but requiring a `kian serve` restart for a
  routine, predictable budget exhaustion is not an acceptable operating model for a resident
  service. DESIGN §4.6 already records the intended fix as a tracked follow-up ("moving the budget
  to per-episode for resident loops"), not as a contract change; this ADR decides and lands it.
- **Decision**: resident loops get a **per-episode** budget; non-resident loops keep the lifetime
  budget.

  1. **Scope is a loop configuration.** `ReActConfig` gains `BudgetScope::{Lifetime, PerEpisode}`
     with `Lifetime` as the default, so every existing worker/plain construction is byte-identical
     and behavior-identical. Only the two resident sides built by the super-agent assembly (the
     Router and the watchdog) select `PerEpisode`.
  2. **Per-episode counter.** The `PerEpisode` count is scoped to one inner-loop drive — one
     Router batch / one resident episode — held in the drive stream's state and reset when the
     Router parks and starts the next episode. The `turn/start` payload keeps the stream-derived
     lifetime iteration number (audit/replay unchanged); only the *budget decision* uses the
     episode counter.
  3. **Exhaustion parks, never freezes.** The loop emits a dedicated stop reason
     (`EPISODE_BUDGET_STOP_REASON_PREFIX`); the `RouterDriver` recognizes it and parks the resident
     (the same path as an idle final answer) instead of propagating `Stopped` to the engine. The
     next admitted input starts a fresh episode with a fresh budget. No `session/ended` is written
     for routine budget exhaustion.
  4. **Observability.** The park is a `warn` audit line (session, limit, episode turns) plus an
     additive `router_batching.episode_budget_parks` counter on `/metrics`. No new event kind, no
     wire change, no store change.
  5. **Explicit terminal ends are unchanged.** A cancelled or fatally failed resident still ends
     terminally; the boot-time re-arm (successor chain, observable cause) remains the recovery for
     those, exactly as today. Runtime re-arm for explicit terminal ends is **deferred** (see
     Alternatives): it needs a runtime-rebindable Router binding shared by subscriptions, the
     driver, the worker completion reports and the timer route, which is a much larger change and
     not needed for the failure class this ADR fixes.

  Adversarial consequences checked: a single episode that needs more than the cap parks **without**
  replying to that batch (the next input starts a fresh episode) — a bounded honesty cost: the cap is a safety bound, not a quality target. A crash mid-episode
  starts the next drive with a fresh episode budget (the counter is in-memory, disposable; recovery
  budgets still bound tokens/wall-clock). The boot re-arm still works for explicit ends, and the
  successor's cause prompt is unchanged. No request bytes, events or tool schemas change; a
  non-resident loop sees exactly the legacy code path.
- **Alternatives considered**:
  - **Infinite budget for resident loops**: no rotation at all; context grows until the provider
    window/compaction, and the resident loses its only automatic stop safety bound. Rejected.
  - **Runtime re-arm on every terminal state** (shared binding + successor side factory +
    admission-time walk): handles cancel/fatal too, but touches `SubscriptionService`, the driver
    routing, the Router tools/spawner, worker completion reports and the timer `Route::Session`
    target, plus in-flight worker reports across a rotation. Deferred with a design sketch.
  - **Raise the deployment config only** (e.g. 10_000): pushes the freeze out, leaves the product
    trap for every operator and every resident; rejected as the product answer (the deployment did
    remove its accidental 32 override, but the default is still finite).
  - **Per-drain budget for all loops**: changes worker/plain semantics (a recovered session would
    get a fresh budget per drain), a wider blast radius for no product need; rejected.
- **Consequences**: one config field, one enum, a small loop counter, a RouterDriver park arm and
  one additive metric. Resident sessions now run until an explicit terminal event; their context
  growth is bounded by compaction/model limits rather than by a silent freeze. The DESIGN §4.6
  known interaction is resolved; explicit-terminal runtime re-arm remains a recorded follow-up.
