# ADR-030: Exec waits are bounded per phase; abandonment is never silent; a budget stop is `interrupted`

- **Status**: accepted (owner gate, 2026-11-15)
- **Date**: 2026-11-15
- **Context**: a delegated worker ran `hermes-agent` (a 23-minute external agent) through the
  synchronous `exec` tool with `timeout_ms: 3000000`. The call returned
  `{"error":"execution timed out"}` after exactly 60 s — `queue_timeout` — while the command kept
  running on the worker; the model could not tell "killed" from "still running", so it invented a
  `sleep 55` polling loop (60 `exec` calls, 865 s of pure sleep, 32 iterations = the whole lifetime
  budget, ~890k prompt tokens) and the thread was then recorded `thread/completed{completed}` with
  an empty summary. The result of the real command was discarded
  (`late result for a settled command discarded`) and the user saw 14 minutes of silence.
  Three distinct defects produced that: (a) one knob `queue_timeout` bound both "no worker has
  taken this yet" and "a worker is executing this"; (b) the tool schema advertised `timeout_ms`
  that the caller-side wait ignored, and the timeout outcome was uninformative; (c) budget
  exhaustion was recorded as success. `superagent.async_tools` (ADR-008) already exists but shares
  (a) — its observer waits through the same `queue_timeout`, so `start` settles `abandoned` after
  60 s instead of delivering the job's real result.
- **Decision**:
  1. **Two wait phases, not one.** A queue row is waited on in two distinct phases.
     *Dispatch wait*: the row is `pending` — no route has taken it. The bound is `queue_timeout`
     (unchanged; this is the honest bound on an **absent** worker) and expiry marks the row
     `abandoned` (nothing ran, nothing is discarded). *Execution wait*: the row left `pending`
     and is `inflight` — a worker is executing it, and the command's own `timeout_ms` (worker-side
     kill bound) is what bounds it. Expiry of the execution wait **never mutates the row**: the
     queue row keeps the truth and stays re-driveable; only the caller stops waiting.
     The execution bound is `2 * exec_bound + queue_timeout`, where
     `exec_bound = cmd.timeout_ms.unwrap_or(queue_timeout)` — one dispatch plus one re-dispatch
     (ADR-007's at-least-once), plus one queue-wait of slack. No new config knob.
  2. **The caller selects a phase policy.** `WaitPolicy::Caller` (the synchronous `exec` tool) is
     today's behavior — one `queue_timeout` wait, then `abandoned`. `WaitPolicy::Job` (the async
     observer) runs both phases. `RemoteExecBackend::exec_tracked` keeps its signature and stays
     `Caller`; `exec_tracked_watched` exposes the phase-policy result. `ExecError` (core seam) is
     unchanged — the phase information travels in a `kian-exec`-local
     `ExecWaitError::Timeout { row_state: Pending | Inflight | Abandoned }`.
  3. **The synchronous `exec` fails closed when its schema would lie.** `timeout_ms` above the
     caller's `queue_timeout` is refused at call time — before enqueueing, with no row and no
     dispatch — naming both bounds and pointing at the async pair. Previously such a call burned
     the full wait and then returned a timeout that read like a kill.
  4. **A timeout result is semantic.** The remote `exec` result on a wait expiry carries,
     additively: `row_state` (`pending` | `inflight` | `abandoned`), `still_running`
     (`row_state == "inflight"`), `row_abandoned` (whether the result was discarded),
     `wait_bound_ms`, and a `hint`. `is_error` and the `error` key are unchanged for existing
     consumers. The *success* shape stays byte-identical to the local built-in's (the "mirrors
     exactly" promise is about the interface, and it holds where both halves can act), and the
     extra keys appear only on a state the local executor cannot produce: the local `exec` kills
     its own child at `timeout_ms`, so a local timeout is already unambiguous (`timed_out: null
     exit_code`) — and it deliberately does **not** claim `still_running: false`, because a
     surviving grandchild is exactly the case this ADR is about.
  5. **A job observer reports what it saw, and stops watching honestly.** Phase-A expiry settles
     the `async_jobs` row `abandoned` (`{"error":"queue timeout"}` — never dispatched);
     phase-B expiry settles it `abandoned` with
     `{"error":"worker silent past the command's own bound"}` **without** touching the live queue
     row, so a reconnecting worker still re-drives and settles it. `stop` is unaffected and remains
     the escape hatch for a wedged job.
  6. **A budget stop is `interrupted`, not `completed`.** The loop's only `LoopOutcome::Stopped`
     producer is iteration-budget exhaustion; the worker records
     `thread/completed{status:"interrupted"}` with the stop reason as the summary
     (`{"text": reason}`) instead of the last assistant message (which, on a budget stop, is by
     construction a pending tool call and an empty text). The report the Router receives therefore
     says `interrupted` and why. `session/ended{completed}` is deliberately **unchanged** in this
     ADR (see Consequences) so the worker is still released — `thread/completed{interrupted}`
     followed by `session/ended{completed}` projects `Released` in `thread_liveness`, which is the
     status-aware contract.
  7. **The tool descriptions carry the steering.** `exec` states its bounded wait and points at
     `start` for longer work plus "never poll with `sleep`"; `start` states that the result arrives
     as an input message and must not be polled; `spawn_worker` asks the Router for a one-line
     acknowledgement to the requesting channel after dispatch. Tool schemas are prompt-prefix
     content (constraint 3): this is a one-time prefix invalidation per upgrade, accepted
     deliberately, and the parameter objects (which the schema-parity tests pin) do not change.
- **Alternatives considered**:
  1. **One bound = `timeout_ms` for everybody.** Simple, but a synchronous `exec` would then pin a
     drain for the command's whole lifetime — the exact pathology ADR-008 exists to remove (steer
     is unavailable and a crashed command holds the drain until its own timeout). Rejected: the
     synchronous call keeps a bounded wait; long work goes async.
  2. **Raise `queue_timeout`.** Only moves the cliff; the schema would still lie for anything above
     it, and an absent worker would be waited on for minutes. Rejected.
  3. **Clamp `timeout_ms` silently to `queue_timeout`.** Deterministic but still burns the full wait
     and still returns a result that reads like a kill. Fail-closed at call time is honest and
     cheaper. Rejected.
  4. **Mark the queue row `abandoned` on execution-wait expiry too.** That destroys a result the
     worker will still produce and forbids the re-drive. Rejected: only the *watcher* stops.
  5. **Map a budget stop to `failed`.** `failed` means the task or environment failed; the budget is
     a policy bound, and the design vocabulary already separates `interrupted` from `failed`
     (constraint 7). Rejected.
  6. **Also write `session/ended{interrupted}` for a budget stop.** The honest end reason, but
     `thread_liveness` then reads `Live` (resumable, worker held) and the Router has no designed
     escape (resume/cancel a held worker) — it would trade a lying event for a stuck worker.
     Deferred; see Consequences.
- **Consequences**:
  - A synchronous `exec` whose `timeout_ms` exceeds the deployment's `queue_timeout` now refuses
    instead of running to a misleading timeout. A replayed historical `tool/call` of that shape
    refuses too — previously it also ended in an error (60 s later), so replay reaches the same
    class of outcome, faster and with a cause.
  - A job observer can hold an in-process observer slot for up to `2 * exec_bound + queue_timeout`.
    It is bounded, the row lease is liveness-only, and `stop` releases it; a restart re-attaches
    through `redrive_pending`.
  - Phase-B expiry can leave `async_jobs{abandoned}` next to a live `exec_queue{inflight}` row. That
    asymmetry is the point: the job view says "we stopped watching", the queue row keeps the truth
    and re-drives. A duplicate `start` on the same command id answers from the job row.
  - `session/ended{completed}` still reads optimistic for a budget stop in this slice. Recorded as a
    known limitation: the follow-up is a supervised outcome that distinguishes "budget" from
    "intentional stop" and a Router-side resume/cancel for a held worker (`session/ended{interrupted}`),
    which needs its own ADR because it changes worker liveness.
  - Model-visible tool descriptions change once (prefix invalidation on upgrade); the parameter
    objects do not, so `remote_tool_schema_matches_builtin_exec_shape` and the fs schema-parity
    tests stay green.
  - One more `exec` result key set (`row_state`/`still_running`/`row_abandoned`/`wait_bound_ms`/
    `hint`) appears in `tool/result.output`, which is opaque JSON (EVENT-MODEL §4.1) — no event
    schema change, no wire change.
- **Rollback Trigger**: if real workloads need a synchronous wait longer than `queue_timeout`, add
  an explicit per-deployment bound alongside a drain-steer story rather than reusing one knob; if
  phase-B expiry turns out to mask a dead worker, tighten the execution bound (or add a wire kill
  frame, ADR-008's rollback trigger) instead of abandoning the live row.
- **Cost Impact**: Negative-cost. It removes a class of wasted provider turns (60 polling turns /
  ~890k prompt tokens in the observed incident), avoids discarding results the worker produced, and
  adds no table, no event kind, and no config knob.
