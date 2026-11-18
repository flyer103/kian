# ADR-033: Queue re-drive is bounded, and a spent bound converges the row

- **Status**: accepted (owner gate, 2026-11-18)
- **Date**: 2026-11-18
- **Context**: on 2026-11-18 the owner instance was upgraded to KiandProto v3 while both `kiand`
  workers stayed on v2, which kept the workers offline for ~13 h (typed `version_mismatch`
  refusals, no shim by design). During recovery, the first worker that re-registered was handed a
  queue row that the upgrade itself had left `inflight`: `ssh vultr install … /tmp/kian-new …
  && systemctl restart kian`. Each re-drive restarted the server the queue lives in, so the
  command's result could never be acknowledged, so the next reconnect re-drove it again — the
  observed loop restarted `kian` every ~11 s with nothing bounding it. The row was 13 h old, its
  owning worker session had already ended `failed`, and no watcher would ever consume the result.

  The upgrade exposed a liveness gap that predates it. § "Dispatch and queue"
  ([WORKER-MODEL](../WORKER-MODEL.md) §4) says a route attach re-drives an id's `pending` **and**
  `inflight` rows (at-least-once, ADR-007), and the per-endpoint driver does exactly that
  (`drive_route` loads every drivable row on connect). Nothing bounds the number of attempts:
  - `queue_timeout` is applied by the **caller**, so a row no caller is waiting on can never expire;
  - a command whose own side effect is the ack path (restart the server, kill the worker) is
    unacknowledgeable **by construction** — retrying it cannot succeed, only repeat the damage.

  That contradicts hard constraint 8 ("no recovery path may loop unboundedly") and the honesty
  rule: a row must not replay forever with no terminal state. The observed row is the general
  shape, not a one-off: `pkill kiand`, `systemctl restart kiand-builder`, or a command that wedges
  the transport reach the same state.

- **Decision**:
  1. **Count dispatches durably.** `exec_queue` grows `dispatch_attempts` (exec migration v6,
     additive `ALTER TABLE`, `DEFAULT 0`, both sqlite and postgres chains), incremented inside the
     existing claim statement — the claim is the only dispatch gate, so the count cannot drift from
     the wire.
  2. **Bound the re-drive.** A `pending` row's first dispatch is always admitted. An `inflight`
     row is re-drivable only while `dispatch_attempts < 1 + max_redrives`, where `max_redrives`
     comes from the new `workers.queue_max_redrives` config knob (default `1`; `0` = first dispatch
     only; validated ceiling `16` so a typo cannot quietly restore unbounded replay). The value
     reaches `RemoteExecConfig::max_redrives`; static pins keep the default. The knob is **global by
     construction**, like `queue_timeout` (ADR-031 D3): one bound keeps the trade-off legible.
  3. **Converge the spent bound honestly.** When the claim is refused, the driver asks the store to
     converge the row: `inflight → abandoned` iff the count is spent **and** no live lease is held
     (`claimed_by IS NULL OR claimed_until_ms < now`), in one guarded `UPDATE`. The lease guard is
     what prevents abandoning a row another driver is dispatching right now; two replicas converging
     the same row is idempotent (one wins, the other's guard matches nothing). The convergence is
     terminal, deletes nothing, and emits one `WARN` audit line naming the command id, endpoint,
     attempt count, bound and `tool_call_ref`.
  4. **No new caller-visible state.** A caller that re-attaches after convergence reads the row
     `abandoned` and gets the existing honest timeout; a caller whose retries just converged ends on
     its own bound (the abandonment does not wake it — recorded under Consequences). No new status,
     no new event kind, no HTTP route, no wire frame, no prompt byte.
  5. **`async_jobs` is out of scope.** It is a separate table with its own observer and settle path;
     a `start` job's result arrives as an admitted input rather than a correlated tool result. If the
     same unbounded shape is observed there, it gets its own bounded-re-drive decision.

- **Alternatives considered**:
  1. **Boot-time sweep of rows claimed by a dead process (epoch rule).** Rejected: a legitimate
     in-flight long command must survive a server restart (that is what at-least-once is for), and
     the same loop exists *within* one process when the command kills its worker (`pkill kiand`), so
     an epoch-only rule would not bound the general case.
  2. **Age-based abandonment (`inflight` older than `queue_timeout`).** Rejected: ADR-030 phase B
     legitimately keeps a row `inflight` for `2 * exec_bound + queue_timeout`, so age is not
     evidence that a result is lost.
  3. **Count only re-drives across process boots.** Rejected: same within-process worker-kill loop
     as (1); and it silently accepts unbounded replay inside one process.
  4. **Detect "restart commands" and refuse them.** Rejected: the queue cannot inspect command
     intent, any command can kill its worker, and the bound is needed regardless.
  5. **Keep unbounded re-drive (status quo).** Rejected: violates constraint 8 and was observed
     live; a server-restart command is a permanent storm.
  6. **Per-endpoint `queue_max_redrives`.** Rejected for phase 1: the dispatch bound is global by
     construction (ADR-031 D3's reasoning), and a per-id bound would let one flaky device stretch
     every unacknowledgeable command's replay.

- **Consequences**:
  - Worst case for an unacknowledgeable row is `1 + N` dispatches (`N = 1` → two), then a terminal
    row plus one audit line. Had this bound existed on 2026-11-18, the loop would have restarted
    `kian` once more and stopped.
  - A command that would have succeeded on attempt `N + 2` is now abandoned honestly (`Timeout`,
    `row_abandoned: true`); the audit line and the abandoned row are the evidence, and the caller is
    never handed a fabricated result.
  - Convergence can lag by up to the row-lease TTL after the last dispatch (the guard waits for the
    lease to lapse); that lag is passive — no dispatch happens, no storm forms.
  - Convergence needs one more connect *after* the bound is spent (the claim is what observes the
    refusal). A spend-bound row whose worker never reconnects simply stays `inflight` forever, which
    is the pre-existing parked-row state and produces no replay.
  - Rollback: a pre-v6 binary ignores `dispatch_attempts`; the column stays harmless and the old
    unbounded behavior returns. The migration itself is additive and forward-compatible.
  - A live caller attached when the bound is spent still ends on its own wait bound rather than at
    convergence; the public outcome is the same `Timeout`, only later.
  - The knob's scope is the whole deployment: raising it for a flaky worker also raises the
    worst-case replay of every unacknowledgeable command.

- **Invariants preserved**:
  - **Event stream is the sole truth** (constraint 1): queue rows are pre-event durable state
    (ADR-007); convergence writes no event.
  - **No deletes, terminal rows frozen**: the only new transition is the guarded
    `inflight → abandoned`; `done` rows are untouched by it.
  - **Honesty** (constraint 7): abandonment is recorded (row + audit line), never fabricated as
    success; `abandoned` answers as a timeout.
  - **No wire or prompt bytes**: `PROTOCOL_VERSION`, frame shapes, tool schemas and prompt prefixes
    are untouched.
  - **Bounded recovery** (constraint 8): the bound is the point of this ADR.

- **Non-goals (phase 1)**:
  - Bounding `async_jobs` re-drive (separate observer; follow-up if observed).
  - Per-command delivery receipts ("the worker received it, the result was lost") — that is a wire
    change, and KiandProto v3 has no per-command ack.
  - Any operator command to re-arm a specific abandoned row (a manual replay is a new surface).
