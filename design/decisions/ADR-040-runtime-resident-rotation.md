# ADR-040: Runtime resident rotation — a shared binding, an idle-boundary rotation, and a handoff

- **Status**: accepted (owner directive 2026-11-20 — phase 0 landed; phases 1/2 remain; append-only — ADR-001…039 immutable)
- **Date**: 2026-11-19
- **Related**: [design/DESIGN.md](../DESIGN.md) §2.2/§4.6/§4.7, [design/MEMORY-MODEL.md](../MEMORY-MODEL.md)
  §6, [design/WORKER-MODEL.md](../WORKER-MODEL.md) §3, [ADR-024](ADR-024-resident-per-episode-budget.md),
  [ADR-027](ADR-027-resident-empty-batch.md), [ADR-035](ADR-035-worker-release-and-idle-reclaim.md),
  [ADR-038](ADR-038-memory-injection-for-internal-sessions.md).

## Context

The resident Router (and the optional watchdog) are **one session for the life of a deployment**.
Two consequences accumulate:

1. **Context grows without a bound.** The Router's judge turn re-sends its whole history every
   episode. Compression bounds the *window*, but the stream (and the compaction history) keeps
   growing, and every episode pays the prefix that compaction leaves.
2. **Memory is frozen at creation.** Creation-time injection is exactly-once per session
   (MEMORY-MODEL I2/I7); a resident session created before a note existed never sees it. ADR-038
   closed the *reachability* gap (a fresh resident now gets the index) but not the *freshness* gap
   for a long-lived one.

The article's model avoids both by starting a fresh session per run and carrying continuity in
memory. kian chose the opposite (one durable session, cached prefix); the missing piece is a
**rotation**: retire the live resident session at a safe point, start its deterministic successor,
carry a bounded handoff, and re-point every surface.

DESIGN §4.6 already recorded the blocker: "Runtime re-arm for explicit ends remains a recorded
follow-up — it needs a runtime-rebindable Router binding shared by subscriptions, driver routing,
tools and worker completion reports, which is a separate semantic change." This ADR is that
change's design, and it stops at the owner gate because two decisions are genuinely open (the
rotation trigger and the end reason).

## Implementation status

- **Phase 0 — landed (2026-11-20).** Boot-time rotation: `superagent.router.rotate_after_events`
  (absent = off; explicit `0` refuses boot); the sixth end reason `rotated` and the terminal
  `rotated` session state; the atomic `drain/claimed` + `session/ended{rotated}` close in assembly
  (before any drain or service exists); the chain re-resolution onto the uuid5 successor; migration
  of the predecessor's pending *message* rows; the bounded synthetic handoff; and the successor's
  fixed context order (ADR-038 memory index → migrated rows → handoff → judge prompt). Gates: only
  an `idle` live session rotates, and a live lease wins. This bounds context across restarts and
  refreshes the memory index without any runtime rebind. Deployment assumption: a single serving
  process (or all replicas restart together) — a second live replica keeps the retired id in
  memory; safe rotation across live replicas is phase 2's shared binding.
- **Phase 1 — not started.** The shared `ResidentBinding` refactor (every surface resolves the live
  session through one binding), behavior-identical.
- **Phase 2 — not started.** Runtime rotation at a proven-idle boundary, serialized by a reserved
  control row inside the drain, with the input-migration rule and the successor re-point of every
  surface.

## Decision (the phase-2 design)

1. **A shared `ResidentBinding`.** Every surface that resolves the live resident session reads one
   `Arc<ResidentBinding>` (`ArcSwap<SessionId>` + generation) instead of capturing a `SessionId` at
   construction: `SuperAgentDriver` routing, `RouterTools`/`WorkerSpawner` lineage, the
   subscription `Route::Router` target, the watchdog route, the outbox reply target, the worker
   completion-report target and the timer rule's fallback. (This is the "phase 1" refactor; no
   behavior change, every reader still returns the same id.)
2. **Rotation happens only at a proven-idle boundary.** The trigger (a maintenance tick, the same
   shape as the worker-release ticker) consults `Engine::has_open_work`; rotation refuses while the
   session is running, has eligible input, or has a pending decision. The trigger then performs the
   rotation **inside the drain's ownership**, not from the outside: a reserved control row
   (`control/rotate`, the ADR-014 precedent) is consumed at a safe boundary, the drain appends the
   rotation end, and only then swaps the binding. That ordering is what keeps an input admitted
   during the rotation from landing on a frozen stream (NoLostWork) — the control row is the
   serialization point.
3. **A successor with a bounded handoff.** The successor is the existing deterministic chain
   (`uuid5(session, "kian:resident:re-arm")`, the name string frozen). Its bootstrap reuses
   ADR-038's seam: the memory index first (fresh session → freshness restored), then a **handoff
   input** derived purely from the predecessor's stream (recent task/assistant turns, bounded bytes,
   redacted, additive `handoff` provenance key — never model-authored free text), then the judge
   prompt. Inputs that arrived after the rotation end stay on the frozen predecessor and are
   re-offered to the successor by the successor's startup/wake path — the exact migration rule is
   phase 2's acceptance gate.
4. **A first-class end reason.** Rotation needs an honest terminal reason: reusing `completed`
   would claim the Router finished its job. Proposal: widen the closed `session/ended.reason` set
   with `rotated` (terminal, like `completed`/`cancelled`), record the successor id on the end
   payload, and keep `recoverable`/`idle` untouched. This is a constraint-3-adjacent contract
   change (a new closed value on a frozen envelope), which is why it is a gate item.
5. **The watchdog rotates with its own binding.** It is a separate pinned session; the same
   machinery applies, keyed by its own chain. Cron `{session}` targets that name a rotated
   resident are refused at load (a static target cannot follow a moving binding) — the cron
   `{channel}` namespace is unaffected.

## Alternatives considered

1. **Boot-time-only rotation.** On `kian serve` restart, if the live Router's stream exceeds a
   threshold, close it and walk to the successor (reusing the existing re-arm walk). Safe, no
   runtime rebind — but it does not bound context or refresh memory *between* restarts, so it only
   helps deployments that restart regularly. Kept as a possible phase 1 fallback.
2. **No rotation; rely on compaction.** Compaction bounds the window but not the stream nor the
   frozen memory index; the resident's prefix cost keeps growing and notes never refresh. Rejected
   as the terminal answer.
3. **Fresh session per event (the article's model).** Loses the cached prefix benefit that is the
   whole reason for a resident session, and would multiply judge turns. Rejected.
4. **Outside-the-drain rotation with a generation check at admission.** Admission would have to
   reject stale-generation writes and redirect, which means a second serialization point beside
   the event stream. The control-row-inside-the-drain shape is the kian-native one.

## Consequences

- The Router's stream becomes bounded by the rotation period, and memory freshness returns.
- Every surface that captured a session id must be re-pointed; a missed reader silently keeps
  writing to the old session (the tests must assert the *whole* set, not a happy path).
- A new terminal end reason (a closed-set widening) and a reserved control row are
  additions-only event-model changes; EVENT-MODEL/API-CONTRACT must land with the code.
- Cron `{session}` targets on a rotating resident become a config error; deployments using them
  must move to the channel namespace or a non-rotating session.
- Rotation cost: one extra successor bootstrap (index + handoff + prompt) per period; amortized
  against the context it sheds.

## Rollback

Config-gated, default off (`superagent.resident.rotate_after_turns: 0` = never). With the gate off
the binding refactor is behavior-identical (the readers resolve the same id) and no control row is
ever admitted; removing the ticker restores the previous release. The new end reason stays in the
catalog (additions-only) and is never emitted when the feature is off.

## Open questions (the gate)

1. **Trigger.** Turns per episode? Events since creation? A wall-clock period? A context-byte
   estimate? Recommendation: `events_since_creation` (cheap, monotone, replay-derived) with a
   default high enough to be a no-op for most deployments.
2. **End reason.** `rotated` (proposed) vs `completed` with additive `rotated_to`. The former is
   honest but widens a frozen closed set; the latter keeps four values and abuses one.
3. **Handoff content.** Which slice of the predecessor stream, bounded at what bytes, and with what
   redaction? Recommendation: the last N task/assistant turns, the same redactor as memory, and the
   `handoff` key outside `content`.
4. **Input migration rule.** Exactly which predecessor inputs the successor re-offers, and how the
   successor proves it saw each once (a re-offer key derived from the predecessor's row ids).
5. **Cron targets.** Refuse `{session}` rotation targets at load (proposed) vs resolve them through
   the binding (a moving target in a static declaration).
