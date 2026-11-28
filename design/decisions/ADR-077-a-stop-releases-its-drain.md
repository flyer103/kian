# ADR-077 — A stop releases the drain it interrupts

- Status: accepted (2026-11-28, process lifecycle)
- Related: ADR-005 (safety by fencing, liveness by lease), ADR-009 (conditional claim admission),
  ADR-041 (a claim chain without an end is closed as failed), ADR-076 (liveness is a probe)

## Context

A claimed drain always writes an end: the episode loop exits through
`ctx.end_session(end_reason)` (`crates/kian-orchestrator/src/engine.rs:2555`), with `interrupted`,
`completed`, `failed`, `awaiting_input`, `awaiting_budget` or `rotated`. An un-ended
`drain/claimed` therefore exists in exactly one situation: the process died between the claim and
that write.

ADR-041 turns such a claim into a signal — the takeover counts the `drain/claimed` events since the
last `session/ended` and, at the third un-ended claim
(`MAX_UNENDED_CLAIMS` = 3, `crates/kian-orchestrator/src/engine.rs:358`), closes the session
`failed` with additive `crashed` bytes. That is right for a crash loop. It is wrong for an ordinary
restart: `systemd` stops the unit with **SIGTERM**, and `kian serve` installed no signal handler, so
the default disposition killed the process mid-drain. Three restarts — a deploy, a
configuration-driven restart, and a supervisor self-heal — were enough to close the resident router
session as `failed` three times in one day, each time with an un-ended claim the operator never
caused and no crash ever evidenced.

The two events are not the same fact. A crash is an owner that vanished while holding a claim; a
stop is an owner that is still alive and can release. The stream already has the honest end for the
second case — the loop writes `interrupted` for an operator interrupt today — so nothing about the
claim chain needs a new shape, only a stop path that reaches it.

## Decision

1. **The runtime owns its stop.** `kian serve` installs a signal handler
   (`crates/kian-cli/src/main.rs`, `shutdown_signal`): the first SIGTERM (the supervisor, a deploy)
   or SIGINT (Ctrl-C) resolves a stop future that is passed to
   `Server::serve_with_shutdown`; `Server::serve` keeps its old signature and delegates with a
   never-resolving future.
2. **The engine stops before the listener does.** The stop future calls
   `Engine::begin_shutdown` — a one-way latch (`ShutdownSignal`) — so from that instant no new drain
   starts: `wake` folds instead of spawning (the admitted row stays durable and the next start's
   inbox rebuild re-derives the work, constraint 6), and the episode loop refuses a claim.
3. **A stop cuts.** `Engine::supervise` races the latch against the driver stream
   (`tokio::select!`), so an in-flight drain settles immediately instead of waiting for the next
   driver outcome — a provider call that never returns cannot hold the process. The cut is the same
   torn-turn shape a crash leaves, which the stream already tolerates; what makes it honest is the
   end write that follows.
4. **The claim settles in the end it already writes.** A claimed episode cut by the latch breaks to
   `ctx.end_session(EndReason::Interrupted)` — one `session/ended{interrupted}`, projected back to
   `idle` (`crates/kian-core/src/state.rs:202`). No new event kind, no change to the ADR-041
   counter: the third *crashed* claim still closes a crash loop, because a released claim is no
   longer un-ended.
5. **The exit is bounded.** The listener stops accepting and drains its connections;
   `Engine::wait_drained` polls the in-flight set and returns once it is empty, bounded at 5 s
   (`Server::SHUTDOWN_GRACE`). The process then exits 0. A drain that does not reach a yield inside
   the bound simply expires its lease, exactly as before — the bound is a floor under the stop, not
   a promise about a stuck task.

The wire and the event catalog are unchanged (`design/API-CONTRACT.md`, `design/EVENT-MODEL.md`);
the operator-visible contract is the CLI's: a stop is a release, and it exits 0.

## Alternatives considered

- **A new `drain/released` event and counter subtraction.** Rejected: the stream already knows how
  to close an episode honestly (`session/ended`), and a second closing write would make the
  crash-loop bound depend on a path a crash can never take — the counter would have to trust a
  release it cannot verify.
- **Wait for the provider turn to finish inside a long grace.** Rejected: a provider call has no
  bound the operator controls, and the stop request says the opposite; the supervisor's own
  `TimeoutStopSec` would kill the process anyway, leaving the claim.
- **Signal handling inside `Server::serve`.** Rejected: the in-process L2/e2e tests boot the
  assembled server inside the test binary, and a process-wide handler there would trap the test
  harness's own signals. Signal policy belongs to the binary, not the assembly.
- **An episode watchdog to reap stuck drains (ADR-044 style).** Rejected as the fix for restarts: it
  needs a new timer and would cut legitimate long turns, while the stop path is exact — the
  operator asked for it.

## Consequences

- A restart no longer adds an un-ended claim, so the ADR-041 bound counts crashes again. The
  resident router session survives deploys and supervisor restarts.
- A stop interrupts the turn in flight: a reply being generated is truncated, and the session ends
  `interrupted` rather than resuming the same turn. That is the same shape as an operator interrupt
  today, and it is honest — the stream says interrupted, not completed.
- The in-process server embeddings (tests, `kian serve` in-process uses) keep the old behavior
  unless they pass a stop future; the shipped binary always does.
- The 5 s bound is a grace for the end-write append, not for the provider: a drain cut mid-call
  spends its time in `end_session`, not in the network.

## Limits

- **SIGKILL and a crash leave the claim un-ended** — by design: that is the case the ADR-041 bound
  exists for.
- No `drain/released` audit row: the evidence of a graceful stop is the `session/ended{interrupted}`
  itself, and a reader distinguishes it from a crash by that write's presence.
- A drain that blocks the runtime thread (a blocking call on a tokio worker) cannot be cut before
  it yields; the process waits out the 5 s bound and exits anyway, leaving that claim to the lease.
- Multi-replica: the latch is process-local; another replica's drain is untouched, which is correct
  — a stop releases only the claims this process holds.

## Acceptance

- L1: `crates/kian-orchestrator/src/engine.rs` — `quiesce_cuts_a_parked_drain_and_ends_interrupted`
  (a drain parked on a gate that never opens: `quiesce` returns 0, exactly one claim, the last event
  is `session/ended{interrupted}`, the projection is `idle`) and
  `a_wake_after_shutdown_is_folded_and_claims_nothing` (a wake after the latch folds, writes
  nothing, starts no drain).
- L2: `crates/kian-cli/tests/graceful_shutdown_l2.rs` — the real binary against a gated mock that
  holds the provider request forever; SIGTERM while parked mid-turn; the process exits 0 through the
  shutdown path, and the stream ends `session/ended{interrupted}` after exactly one claim, with no
  `failed`/`completed` end.
- L3: the deployed unit is restarted by `systemctl` and the resident session keeps its identity
  (no `failed` end, no `crashed` payload) across the restart.
