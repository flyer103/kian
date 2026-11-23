# ADR-052 — The job model: declarative plans executed as runs and step sessions

- **Status**: accepted (owner gate passed 2026-11-22). P0 and P1 landed; the surface that ships is
  tabulated in [JOB-MODEL](../JOB-MODEL.md) §0 ("Implementation status"), and §21.2 lists what is
  designed but not v1 (the worker binding view, enforced cancellation, binary artifacts, the HTTP
  status route).
- **Date**: 2026-11-22
- **Context**: [JOB-MODEL](../JOB-MODEL.md) §0–§21, [CRON-MODEL](../CRON-MODEL.md) §3–§4, §9.1,
  [WORKER-MODEL](../WORKER-MODEL.md) §3, §5, §9, [MULTIAGENT-MODEL](../MULTIAGENT-MODEL.md) §4.1,
  §4.4, §5.3, [DESIGN](../DESIGN.md) §0.2, §2–§4, [EVENT-MODEL](../EVENT-MODEL.md) (23 kinds),
  [READ-MODEL](../READ-MODEL.md), [ADR-034](ADR-034-cron-scheduler.md),
  [ADR-035](ADR-035-worker-release-and-idle-reclaim.md),
  [ADR-039](ADR-039-cron-worktree-firing-is-an-external-job.md),
  [ADR-042](ADR-042-imessage-channel.md).

## Context

An operator wants a daily digest: at 09:00 fetch from a machine that has unrestricted network, then
summarise, then send it over iMessage, then tell the truth about whether it was sent, and alert if it
was not. Today that is a `launchd` plist plus a shell script that ssh'es to a VPS and then polls
`chat.db` for `is_sent = 1`. Every piece of that script exists because the runtime offers no way to
say *what* the work is, *where* it should run, or *whether it really happened*.

The runtime does own four of the five pieces already: cron admits a durable firing
([ADR-034](ADR-034-cron-scheduler.md)); a worker binding supplies hands on another machine
([WORKER-MODEL](../WORKER-MODEL.md)); the channel adapter already resolves exactly the receipt the
script polls for ([ADR-042](ADR-042-imessage-channel.md)); and the evidence gate already defines a
verdict event ([EVENT-MODEL](../EVENT-MODEL.md)). What is missing is the *plan* — the composition —
and the discipline that keeps a plan's three other axes from fusing into it.

The tempting shortcut is a scheduler-side v2 of the cron spec: `CronSchedule.steps[]` with a
`depends_on` graph, a `requires` list and a delivery poll, executed by the ticker. That shortcut is
refused by the repository's own rules: the control plane never writes execution (constraint 9), only
a parent's own drain spawns its children ([MULTIAGENT-MODEL](../MULTIAGENT-MODEL.md) §4.1 M10), a
board/tasks table as authority is forbidden (§4.1), and a deterministic workflow engine is an explicit
non-goal ([DESIGN](../DESIGN.md) §0.2). This ADR records the shape that survives those rules.

## Decision

### D1 — Four axes, four vocabularies, one composition rule

A *trigger* says when (a slot), a *plan* says what (a DAG of steps), *placement* says where (a
capability match), *evidence* says whether it happened (a verdict). Each is a separate vocabulary
owned by a separate section of [JOB-MODEL](../JOB-MODEL.md); the only legal cross-axis functions are
listed in its §2.5.

The composition rule is the acceptance test for future work: **every new requirement must be a new
*value* in one of those four vocabularies, never a new mechanism.** A requirement that cannot be
expressed that way is evidence the abstraction is wrong, not a licence to add a second scheduler.

### D2 — A run is an ordinary session; the conductor is a loop decorator that parks

The carrier is decided by the iron rule, not by preference. A run is one ordinary session whose first
admitted input carries a `job` marker; a `JobLoopDriver` decorator (the same shape as
`MemoryLoopDriver`'s trusted-marker dispatch) interprets the frozen plan and spawns steps.

The conductor waits by **parking**: it holds the drain, yields `TurnDone` on a coarse tick, writes
nothing, and is woken by a durable admit. It must never yield `Settled` while the run is unfinished,
because the engine treats a settle with an empty inbox as `completed` — and a terminal end freezes the
stream ([DESIGN](../DESIGN.md) §2.2). This is the one genuinely new control-flow obligation the model
adds, and it is not new code: it is `RouterDriver`'s park discipline, with a coarser tick.

### D3 — The plan is frozen at admit, and the freeze is data

The trigger canonicalises the plan (sorted keys, no insignificant whitespace), hashes it
(`sha256:…`), and puts both the plan and the digest in the run's first input row. The conductor
executes those bytes. A configuration edit never changes an in-flight run, a re-drive never consults
today's config, and `kian job status` can say which plan a run used without reading configuration at
all.

This is what makes the model safe to operate: the object being executed is committed data, not a
lookup performed at dispatch time. It is also why the digest is carried rather than recomputed — a
recomputation is a different object the moment anyone edits a prompt.

### D4 — Derivation over storage: identity, dispatch and settlement are pure functions

Every identifier is derived by `uuid5` from the logical triple: `(run, step, attempt)` for the spawn,
the thread and the child session; `(job, slot)` for the run; `(job)` for the namespace. Dispatch is
therefore idempotent by construction: the record is committed **before** the child exists, the child's
ids are recomputable, `create_session` is idempotent for an existing stream, and the child's input
carries a deterministic idempotency key so a repeated admit dedupes.

The namespace is pinned, not invented: `KIAN_NS = 99be60b5-c4d7-52ec-8563-6de7b8697856`, derived
once as `uuid5(Uuid::NAMESPACE_URL, "kian.dev/ns/job")`. Changing that constant re-derives every
run session id and orphans every in-flight run, so it is contract in the same sense as the resident
re-arm name (ADR-040) and the firing-session name (CRON-MODEL §3.2).

Settlement is equally pure: a step's verdict is a fold over the child's committed events, and the
run's outcome is a fold over step verdicts. Nothing is decided from the live tail or process memory,
so a takeover after `kill -9` makes the *same* decisions — same worker, same bytes, same verdict.

### D5 — Placement: capability is eligibility, chosen once, refused instantly

`requires` (≤ 16 validated capability names) intersects the worker catalog. The candidate set is
narrowed by two derived exclusions — offline workers (unless `allow_offline: true`) and workers that
already failed this step in this run — ordered by state, then a declared rank, then id, and the
choice is committed in the dispatch record.

Three refusals are deliberate. **"Most recent success" is not in the order**: it is hidden global
state that makes identical streams score differently after a restart. **A worker's capability claim is
not authority** ([WORKER-MODEL](../WORKER-MODEL.md) §9): a lying worker produces a failing verdict,
never a privilege. **An empty candidate set is a refusal, not a wait**: the step fails immediately
with a verdict naming which exclusion emptied it, which is what makes "no available worker in 5 s, not
90 s" true by construction.

Retries are new attempts with new identities — never a rebind of a live child — which is how the
retry policy coexists with [WORKER-MODEL](../WORKER-MODEL.md)'s no-failover rule.

### D6 — Evidence reuses the existing verdict, and adds exactly two checks

A step's checks are the closed `CheckSpec` vocabulary the evidence gate already evaluates
(`final_message_present`, `tool_result_since_last_user_input`, `no_error_tool_result_tail`,
`min_tool_results`) plus two job-owned checks: `delivery_receipt{channel, target}` and
`artifact_within_cap{name, max_bytes}`. No new check language: a check must stay a pure predicate a
reviewer can read.

`unverified` is a first-class outcome, not a synonym for success. A run whose steps all "completed"
but whose checks did not pass ends `unverified`, which the cron sweep must read from the additive
`job.outcome` key because both `succeeded` and `unverified` end `completed`. Reporting a receipt-less
run as successful is exactly the dishonesty (constraint 7) the model exists to prevent.

### D7 — Artifacts are references; a cap breach is a failure, never a truncation

A text artifact is `{producer session, seq, sha256, bytes}` — a pointer into immutable committed
bytes, so there is no storage to add, nothing to expire, and nothing a later edit can corrupt.
Binary artifacts need a blob store and are designed-not-v1; when they land, TTL is a completeness
bound and a missing blob is `artifact_unavailable`.

Exceeding `max_bytes` (default cap 256 KiB) fails the producing attempt. Silently truncating an
artifact would make every downstream verdict a lie about the input.

### D8 — Delivery is a conductor action; an alert is a run

A `deliver` step is not placed and has no worker. It runs in the conductor against the server-side
channel adapter, whose allowlist, echo guard and honest dispositions already exist
([ADR-042](ADR-042-imessage-channel.md)). It commits the send intent as a `tool/call` before calling
the adapter and the receipt as `channel/delivered` after — the outbox's discipline, without the
outbox's Router-only driver.

Two consequences follow directly. The spec's "a VPS worker must never be able to send iMessage" needs
no policy: no worker is ever a candidate for a send. And a crash between the intent and the receipt is
`needs_operator`, never an automatic re-send, because the message may have landed.

An alert is a run of a deliver-only synthetic job, admitted by the failing conductor with slot
`alert:<failed run id>` — one alert per failed run, no chains (an alert job may not declare
`on_failure_of`), and validation requires its channel to differ from every delivery channel of the
job it watches.

### D9 — Footprint: config and bundle; one read surface; no new truth

Config gains a `jobs:` section whose triggers lower into the existing cron runtime vector (one
scheduler, one cursor, one misfire policy, one claim table), adding one runtime-only target variant.
The bundle gains the conductor, the plan types, the two checks, the deliver action and the status
projection. The only cross-cutting additions are three additive event keys, one additive
delegation-wrapper key (so a child knows its real parent and its completion report routes to the run,
not to the resident), one field on the spawn request, one CLI verb group, and one read-only projection
(`WorkerBindingView`) so a second spawner can see binding contention that today only a single caller's
stream reveals.

Explicitly refused: a `job_runs` table, a board/tasks column, a supervisor daemon, a global
dispatcher, a second scheduler, an expression language for plans. Each would be pre-event state
(ADR-014) or a second authority over execution (constraint 9).

### D10 — The honest gaps, stated rather than papered over

- **Step timeouts are observed, not enforced.** An interrupt is only observed at a driver outcome, so
  a child parked inside a tool call to an offline worker never sees it, and the exec queue has no
  pickup bound. The conductor therefore stops *waiting*, records `timed_out`, and interrupts the child
  for the future; the child keeps its worker binding until it ends. An enforced cancellation seam is
  P3.
- **A send followed by a crash is unresolvable.** Without a channel-side dedupe token the outcome is
  unknown, so it becomes `needs_operator`. The iMessage adapter's per-part `client_id` is the natural
  P2 hook.
- **A conductor that dies before writing its end raises no alert in v1.** The firing is still marked
  failed by the settlement sweep, so it is visible but not pushed; sweep-side alerting is P2.

## Consequences

- The digest becomes configuration plus a plan, with no shell script, no `launchd`, no poll loop, and
  a receipt in the same stream as the work it describes.
- A run is inspectable with the tools the runtime already has: `kian job status` is a read of a
  session's stream plus its children, and deleting the projection loses nothing.
- Every step is a real session, so per-step retries, timeouts, interrupts, model slots and cost
  accounting apply unchanged — the job model inherits the runtime's semantics instead of
  reimplementing them.
- The cost is a park: a run holds a drain for its whole duration. The tick is coarse (100 ms–5 s), it
  writes nothing, and the lease is unaffected because `MAX_UNENDED_CLAIMS` counts claims, not ticks —
  but a run is a long-lived claim by design, and the model's liveness depends on the wake path
  (constraint 6) being healthy.
- Placement is only as good as the catalog, and the catalog is a declaration. A mislabelled worker
  produces a failing step and a clear verdict; it cannot produce a silent privilege.
- Nothing lands without the amendments in [JOB-MODEL](../JOB-MODEL.md) §18.2 and the tests in §18.4.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| A `job_runs` table with a status column, written by the ticker | A second authority over execution and a mutable status column — constraints 1 and 9, and [MULTIAGENT-MODEL](../MULTIAGENT-MODEL.md) §4.1's "no board entity, ever". |
| A Temporal-style workflow engine | [DESIGN](../DESIGN.md) §0.2 refuses deterministic re-execution of control flow: LLM steps are non-deterministic, and what the runtime replays is bytes and events, not a program. |
| A scheduler-side `steps[]` executed by the cron ticker | The control plane would write execution (constraint 9); there would be no parent drain, hence no legal spawner (M10). |
| Hostnames / a pinned machine in the plan | The spec's own R2. Placement by *capability* is the only form that survives a fleet change, and the pinned escape hatch exists as `pin` without becoming the default. |
| One big prompt that does fetch-and-summarise-and-send | Works once, then cannot answer which step failed, cannot retry the fetch alone, cannot prove delivery, and re-sends the whole message on any failure. |
| Success = process exit code | The spec's central correction: only a receipt is delivery. An exit code is a claim about a process, not about the world. |
| Polling `chat.db` for `is_sent = 1` | The adapter already resolves that condition and emits a receipt; a second poller duplicates the truth and races the adapter. |
| Letting a worker run `imsg send` | Bypasses the allowlist, the echo guard and the honest dispositions, and would put the send path on an arbitrary machine. |
| Retrying a failed attempt by re-binding the same child | Violates [WORKER-MODEL](../WORKER-MODEL.md)'s no-failover rule; a retry is a new attempt with a new identity. |
| Making the run status a new serving surface in v1 | The CLI proves the projection first; a wire contract for an unproven projection is rework waiting to happen. |

## Non-goals

- No deterministic workflow semantics: no expression language, no dynamic DAGs, no computed plans.
- No second scheduler: triggers lower into cron, and cron keeps its cursor, misfire and claim rules.
- No cross-run dependencies, no `max_parallel > 1`, no binary artifacts, no enforced cancellation in
  v1 ([JOB-MODEL](../JOB-MODEL.md) §21.2).
- No operator facility that edits a run's plan or rebinds a live child.
- No new event kind, no new table, no new HTTP route, no new metrics key, no new crate in the narrow
  waist.

## References

- [JOB-MODEL](../JOB-MODEL.md) — the normative contract: §2 axes, §4 invariants J1–J16, §5 carrier and
  park, §6 derivations, §7 config and validation, §8 dispatch and retries, §9 placement, §10 effects,
  §11 verdicts, §12 artifacts, §13 delivery and alerts, §14 the additive surface, §15 status,
  §16–§17 failures and crashes, §18 footprint and amendments, §19 migration and phases.
- [CRON-MODEL](../CRON-MODEL.md) — §2 tz/DST, §4 the durable cursor and claim, §9.1 settlement,
  §12.3 what resets a cursor.
- [WORKER-MODEL](../WORKER-MODEL.md) — §3 capabilities as declarations, §5.1 the 1:1 binding, §9 what
  capabilities do not grant.
- [MULTIAGENT-MODEL](../MULTIAGENT-MODEL.md) — §4.1 no board, §4.4 readiness as a pure function,
  §5.3 child limits.
- [DESIGN](../DESIGN.md) — §0.2 the workflow non-goal, §2.1 the drain, §2.2 terminal ends, §4.1 the
  loop stays server-side, §6.3 the footprint ladder.
- [EVENT-MODEL](../EVENT-MODEL.md) — the 23 kinds, additive keys, `evidence/verdict` and
  `channel/delivered`.
- [ADR-034](ADR-034-cron-scheduler.md) — the scheduler this model borrows rather than duplicates.
- [ADR-035](ADR-035-worker-release-and-idle-reclaim.md) — binding release; why an offline worker stays
  a valid binding.
- [ADR-039](ADR-039-cron-worktree-firing-is-an-external-job.md) — the precedent for saying plainly what
  a scheduler does *not* claim.
- [ADR-042](ADR-042-imessage-channel.md) — the receipt a deliver step verifies.

