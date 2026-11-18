# ADR-035: A binding releases by closing its thread, and an idle-ended thread is reclaimed

- **Status**: accepted (owner gate, 2026-11-18)
- **Date**: 2026-11-18
- **Supersedes**: [ADR-021](ADR-021-runtime-worker-membership.md) §Decision 13's and
  §Consequences' "no new ops method" clause — for `release_worker` only. ADR-020's rejection of a
  lease on the live binding stands.
- **Context**: a two-worker deployment hit the shape the contract currently has no answer for. A
  worker session's delegated task was interrupted; the session projects `idle` with
  `session/ended{interrupted}`; `thread_liveness` therefore reads `Live` (`failed`/`interrupted`
  are "resumable and still hold it",
  [worker.rs](../../crates/kian-superagent/src/worker.rs) `thread_liveness`, and
  [WORKER-MODEL](../WORKER-MODEL.md) §3). Nothing in the product can move that thread out of
  `Live`:
  - there is no release/cancel verb (`kian workers` is a read-only board;
    [commands.rs](../../crates/kian-cli/src/commands.rs) `run_workers`);
  - `interrupt` on an idle session is an honest no-op (`InterruptOutcome::NotDraining`,
    [engine.rs](../../crates/kian-orchestrator/src/engine.rs));
  - the state machine has no `idle → terminal` transition (only `running → completed/cancelled`,
    [state.rs](../../crates/kian-core/src/state.rs) `legal_table`), so no append can close it;
  - recovery never revisits it: with no eligible inbox row and no claim, no wake exists.

  The 1:1 check keeps refusing the worker (`busy_owner` names the owner), so the machine is
  unusable for the rest of the process lifetime — the report's "permanently busy, only the other
  machine left". [ADR-030](ADR-030-exec-wait-bounds-and-budget-stop.md) already recorded this
  follow-up ("the Router has no designed escape (resume/cancel a held worker) … needs its own ADR
  because it changes worker liveness"). ADR-020 rejected a lease on the *live* binding
  ("leases detect liveness, they never grant permission … a lease would make a durable policy fact
  expire on a clock"); that reasoning binds this ADR too: the fix must be a durable terminal fact,
  not a clock-driven reinterpretation of a live binding.

- **Decision**:

  1. **Release = close the thread, not reinterpret the binding.** A release appends, to the bound
     worker session's own stream, exactly one atomic batch
     `[drain/claimed{holder, epoch}, session/ended{reason:"cancelled", bytes:{"released":{…}}}]`
     under the freshly issued claim epoch (`EventStore::append_batch`, one transaction on sqlite /
     one statement on postgres). `cancelled` is terminal, so `thread_liveness` releases the worker
     by its existing rule and the thread can never be resumed — the 1:1 invariant survives by
     construction. The binding bytes (`thread/started`, the wrapper) are untouched: nothing is
     rebound, rewritten, rerouted or migrated.
  2. **The close needs a claim, because the machine has no `idle → terminal` edge.** The claim is
     the fence (monotonic epoch, ADR-005) and the batch is atomic, so there is no "claimed but not
     closed" crash window in the stream: a crash before the batch leaves the projection `idle` and
     the next release retries; a crash after it leaves a terminal session. The state machine is
     unchanged, and a pre-ADR binary can still project every released stream (both kinds existed).
  3. **Eligibility is fail-closed** — every condition is checked before the claim, and the claim
     itself rejects a live drain holding the lease:
     - the worker's bound thread is `Live` and its projection is `idle`;
     - its last event is `session/ended{interrupted|failed}` (a thread that never ended, or ended
       completed/cancelled, is not releasable);
     - the inbox has no eligible row (`has_eligible_inputs` false — a resumed/queued thread is
       never released);
     - the worker's durable command queue has no `pending`/`inflight` row
       (`ExecStore::drivable` empty — the machine is not still executing or owing work from this
       thread; rows are terminal-only at release time, and a terminal session creates none);
     - this process holds no drain for the session.
  4. **Two triggers, one operation.**
     - **Explicit**: a new ops-plane method `release_worker {worker, reason?}` (CLI
       `kian workers release <id>`). The server resolves the bound thread, applies §3 and calls
       one engine primitive (`Engine::release_idle`). Idempotent: a thread already terminal answers
       `already_released`; a running thread answers `-32002 session_busy` ("interrupt it first");
       an unknown id or no live binding answers a new `-32008 worker_not_bound`.
     - **Bounded automatic**: `workers.release_after_ms` (default `300_000`, `0` disables) reclaims
       a bound thread that has met §3 and whose last event is at least that old. A maintenance
       tick in `kian serve` (assembly, the `multiagent_tick` precedent) walks the bindings, applies
       the same primitive, and swallows/logs per-thread errors. Boot's first beat releases threads
       that went stale while the process was down.
  5. **Audit is additive.** The end event's `bytes` gains one optional `released` object
     (`worker`, `thread`, `by`, `reason`); EVENT-MODEL documents the key set. One tracing line per
     release names the worker, session, thread and trigger. No new event kind, no new table, no
     status, no prompt or tool-schema byte (the Router's `spawn_worker` schema and prompt are
     untouched).
  6. **Release is not rebind, failover, takeover, or resume.** It ends the old thread; a new spawn
     mints a new thread. Queued commands are never cancelled by a release (the §3 gate guarantees
     none are live). A `running` thread is never force-closed: interrupt is the stop surface, and
     release is only for an already-ended one.

- **Alternatives considered**:
  1. **Derive `Released` for `interrupted`/`failed` without writing anything** (change
     `thread_liveness`). Rejected: the interrupted thread stays resumable (`input/admitted` is legal
     on an idle session), so after a new spawn bound the same worker, a resume of the old thread
     would execute a second live bound session on that worker — the 1:1 rule this contract exists
     for would be silently broken. A terminal close is the only release that cannot race a resume.
  2. **Release on the interrupted end immediately** (no grace, no verb). Rejected: it makes an
     interrupt destructive without an explicit decision, removes the window to inspect/steer the
     thread through the API, and leaves no place to record *who* chose to free the worker.
  3. **A lease/TTL on the live binding** (the ADR-020 rejected alternative). Rejected again: it
     would expire a durable 1:1 fact on a clock and could free a worker that is still legitimately
     running a long task. `release_after_ms` is measured from a **terminal** event and only ever
     closes an already-ended thread — it is a bounded cleanup, not a liveness lease.
  4. **A new event kind (`worker/released`, `thread/released`)**. Rejected: `cancelled` already
     means "deliberately closed", the projection already releases on it, and a second durable fact
     would need its own semantics on replay, snapshots and the join. The audit lives in the end
     payload.
  5. **`idle → cancelled` as a legal state-machine edge** (one append, no claim). Rejected for
     rollback: a pre-ADR binary would fail to project every released stream
     (`IllegalTransition{from:Idle,on:Ended(Cancelled)}`), making those sessions unreadable, while
     the claim+batch uses only edges that every shipped binary understands.
  6. **A Router-side `release_worker` tool / spawn-time auto-release**. Deferred: it would add a
     model-callable destructive tool and a new execution-write seam to the bundle (prompt-prefix
     cost, narrow-waist pressure). The bounded tick plus the operator verb fix the reported state
     without new prompt bytes; a spawn-time fast path can be layered on later through a port.
  7. **Force-close a running thread** (`release --force`). Rejected: an in-flight provider turn has
     no safe boundary; `interrupt` is the honest stop, and a release after it is the two-step
     operator flow.

- **Adversarial walk** (state → failure → recovery):
  | Failure point | Invariant | Outcome |
  |---|---|---|
  | Crash before the claim | no write | projection unchanged; thread still held; operator/tick retries |
  | Crash after the claim, before the batch | no stream write | lease lapses; projection still `idle`; retry allowed; epoch high-water mark only advances |
  | Crash after the batch | `NoHalfTerminated` | one terminal end, last event; terminal sessions freeze; replay identical |
  | Release vs. a concurrent drain claim | fencing | `is_draining` and the fresh-lease claim reject one side; a release that loses refuses `-32002`, never appends into a live episode |
  | Release vs. a concurrent `submit` | no lost work | the eligibility check refuses while a row is eligible; a row admitted inside the check→claim window makes the claim's holder the release and can strand that row — documented, bounded to one instant, and impossible for the Router surface (it never submits to a finished worker thread) |
  | Release twice | idempotent | second call reads terminal → `already_released`; the tick skips terminal threads |
  | Two replicas release the same worker | fencing | one claim wins; the other gets `ClaimRejected` → busy/skip; the batch CAS commits nothing on the loser |
  | Old thread's command still running | 1:1 | release refuses while `drivable` is non-empty; the row's own bound (ADR-033) converges it, then the release proceeds |
  | Resume after release | 1:1 | `cancelled` is terminal: a claim would project `Terminal`; no resume drain runs |
  | Rollback to a pre-ADR binary | readability | claim + cancelled end are existing kinds/edges; the released streams project normally |

- **Invariants preserved**:
  - **Event stream is sole truth** (constraint 1): the release *is* an event; the binding stays the
    delegation bytes.
  - **End writes are single atomic appends** (constraint 4): the terminal end and its transition are
    one row inside the atomic batch; the claim precedes it as every drain's does.
  - **Safety by fencing** (constraint 5): the close carries a fresh claim epoch; no clock ever grants
    write permission.
  - **Layered iron rule** (constraint 9): the operator plane resolves and audits; the *engine* (an
    execution authority) performs the claim and the append; the bundle is not involved.
  - **Honesty** (constraint 7): the earlier `interrupted`/`failed` end is preserved; the join still
    derives `interrupted`/`stalled`, never `completed`.
  - **Bounded recovery** (constraint 8): at most one claim+batch per release, no retry loop; the
    tick is one pass per interval.

- **Consequences**:
  - Positive: a worker whose thread ended non-completed frees itself within
    `workers.release_after_ms` (or immediately on the verb); the audit shows the earlier honest end
    plus the deliberate close; no new kind/table/prompt byte; the Router's busy refusal keeps
    naming the owner.
  - Negative: a released thread can no longer be resumed — that is the deliberate trade (the
    alternative is the permanent slot leak). An `interrupted` thread that a human intended to resume
    must be resumed before the reclaim bound; `0` disables the automatic path.
  - Negative: the `kian workers` board frees the slot only after the queue is idle, so a hung
    `exec` delays the reclaim by at most its own kill/wait bound — honest, because the machine is
    still working.
  - Rollback: revert the ops method and the tick; released sessions remain readable. The config key
    is additive and validated.

- **Non-goals (phase 1)**:
  - Cancelling or migrating the old thread's queued commands (there are none at release time; a
    queue-side cancel rule remains part of the deferred rebind work, ADR-020 §Deferred).
  - Force-closing a `running`/`recoverable` thread; resuming a released one.
  - A Router tool, a spawn-time fast path, or any model-visible surface.
  - Multi-Router coordination beyond the existing claim fencing.

- **Unverified**:
  - No L3/real-deployment run: the reclaim tick and the CLI verb are exercised at L1/L2 over a real
    store/engine; a two-machine deployment with a real interrupt is not in CI.
  - The check→append window against a concurrent public `submit` is reasoned, not race-tested; the
    test suite pins the stable refusals (busy, eligible input, live queue row), not that instant.
