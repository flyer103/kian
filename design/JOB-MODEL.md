# Kian — Job Model (normative)

> **Status: normative.** Date: 2026-11-28.
>
> This document is the contract for declarative, multi-step work. It defines what a **job** is,
> how a **run** is carried, how a step's **placement** is chosen, how an **artifact** is handed
> between steps, and how a step or a run is **proven** done.
>
> It does not define the event envelope, the trigger clock, worker membership or its wire protocol,
> channel adapters, or serving transports. It relies on those contracts only through the facts it
> names: an append-only session stream, an admitted input row, a child session, a channel receipt,
> and a read-only worker catalog. Where this document names such a fact it means the fact, not a
> particular implementation.
>
> This contract adds **no** new event kind, **no** new authoritative table, **no** new HTTP route
> and **no** new crate. It adds three additive event keys, two additive receipt fields, one
> superagent.jobs config section, one rebuildable read projection, two metric names, and one CLI
> verb group.

## 0. Scope and reading

The failure this contract prevents is a *category* failure, not a bug. An operator asks for
"at 09:00 fetch, then summarize, then send, and tell me the truth about whether it was sent", and
the runtime answers with four unrelated mechanisms: a scheduler, a spawn path, a channel adapter
and an evidence gate. Each owns a different third of the answer. Gluing them into one prompt works
once and then rots: the prompt encodes placement, an exit code stands in for delivery, and nothing
can answer *which step failed* or *did the message land*.

| Question | Answered here |
|---|---|
| What work is it, as a composition of steps? | §7 |
| When does a run start, and what does a missed slot mean? | §2.1, §7.6 (the trigger clock itself is external) |
| Where does a step run, and which worker provides its hands? | §9 |
| How is a step's attempt carried and fenced? | §5, §6, §8 |
| What proves the step happened? | §11 |
| What does a run's status mean? | §11.4, §15 (a projection, never a row) |
| What happens when the conductor dies? | §16 |

A request reaches the runtime through one of three entries, and only one of them involves a plan:

| Entry | Needs a frozen job plan? | What it creates |
|---|---|---|
| ordinary session (chat, agent, harness) | no | a session driven by its own loop |
| job run | yes | a session whose first input carries a job marker and whose driver is the conductor |
| control or observation | no | an input row, a CLI read, or a Router client call against an existing run |

"Router is optional" therefore does not mean "every request is a job": it means the job path is one
entry among several, and a deployment may run jobs with no Router at all. The plan is an internal run
fact produced at admission; a caller supplies a job id, a slot and parameters, not necessarily plan
bytes.

The four axes are trigger, plan, placement and evidence. They may influence each other only through
the five named functions in §2.5. Everything else in this document is either a consequence of that
isolation or a rule that keeps the event stream the only truth.

## 1. The pattern

### 1.1 The concrete need

The driving case is one operator's daily digest: at 09:00 Asia/Shanghai fetch model rankings,
Hacker News and Hugging Face; summarize in Chinese; send one iMessage; report whether it *really*
left the building; alert if it did not. Today it is a launchd plist running a shell script on a
Mac, which SSHes to a VPS, which calls a model over HTTP, which shells back out to a local binary
and then polls a chat database twelve times hoping for a sent flag. Each hop is a separate failure
domain, none of them reports, and the success signal is a side effect of the delivery tool rather
than a receipt.

Read past the deployment accident and the requirement is not "cron" and not "a workflow engine".
It is one sentence:

> A named composition of steps, each of which declares the capabilities it needs and the evidence
> that proves it happened, started by a trigger, carried to completion by the runtime, and never
> silently declared successful.

That sentence is general. It recurs in every shape of "do these things in order on the machines that
can, and tell me the truth": nightly reports, ingestion pipelines, release checks, multi-machine
sweeps, agent-authored plans. The alternative — a big prompt plus shell chaining — is not a smaller
design, it is the absence of one: placement, retry and verification end up encoded in prose, and
the runtime cannot report on work it cannot name.

### 1.2 What one config object conflates

A single config object folds four independent decisions into one shape. A schedule that carries
steps, prompts, placement hints and a delivery poll in one struct fuses them, which is what forces
questions such as "does max_concurrent mean per task or per step?" Separating the axes answers most
of them by construction.

| Ask | Axis | Gap this contract fills |
|---|---|---|
| Fire at 09:00 Asia/Shanghai, catch up the latest missed slot | trigger | a schedule can name a session or a channel topic, but not a composition of steps |
| Never hardcode a hostname in the work | placement | capabilities exist as a declaration; nothing selects on them |
| "local first, VPS as fallback, never the wrong machine" | placement | no rank, no candidate set, no refusal path |
| fetch → summarize → send, with data dependencies | plan | a firing is one turn; there is no step |
| 256 KiB artifact cap, by reference, no truncation | plan | no artifact notion at all |
| success = a fresh sent row with no error | evidence | the adapter proves it; nothing declares it as the step's proof |
| alert within minutes when both paths fail | evidence + trigger | no failure hook independent of the step that failed |
| per-step status, attempts, latency | evidence | events exist, unfiltered; no run index, no CLI |
| retries and timeouts per step | plan | no per-step budget, no run budget |

### 1.3 The composition rule

The abstraction is worth having only if future needs extend it by adding a **value**, never a new
**mechanism**:

> Every new requirement must be expressible as a new value in exactly one of four vocabularies — a
> trigger kind (§2.1), a step action (§7.3), a capability (§9.2), or a check (§11.2) — and must not
> require a second scheduler, a second spawn path, a second delivery path, or a durable status
> column.

That rule is the acceptance test for this document. It is why the event surface adds keys rather
than kinds: every fact the model needs is already a fact the event catalog records about a session.

## 2. The four axes

### 2.1 Trigger — *when*

A trigger is an occurrence with a durable **slot** token, unique within a job. The slot makes a run
idempotent: (job, slot) names at most one run, forever.

| Kind | Slot token | Notes |
|---|---|---|
| cron | the firing id schedule_id@fire_time | one slot per scheduled instant; catch-up and overlap are trigger policy |
| manual | manual:<26-character ULID> | one slot per operator request; an explicit slot may be supplied for idempotence |
| alert | alert:<failed run id> | at most one alert run per failed run with an enabled alert declaration; a run without a committed failed verdict does not alert |
| event | designed, not v1 | an event subscription may later resolve a slot |

A trigger resolves a slot into a run **decision**: which job, and which plan bytes. Resolution is a
pure function of the admission request and the loaded configuration, and happens in the trigger's
own actor. In v1 the only source is a declared job, so plan = resolve(job_id, loaded_config); a
template is a declared plan with no trigger and resolves through the same config path. An inline or
Router-authored plan is admitted only after the phase in §17.4 lands, and is validated and frozen by
the same rule. The trigger never executes; it freezes the plan and admits one input row with a slot
token.

The cron trigger has one clock, one cursor and one firing record. It supports catch_up = skip or
latest and overlap = skip, and nothing else in v1. A missed slot with catch_up = latest produces at
most one run, for the latest missed instant; catch_up = skip produces none. overlap = skip means a
firing is skipped while a run of the same job is still live. There is no catch_up = all in v1.
The cursor is claimed insert-first-wins on (schedule_id, fire_time), so a double tick, a restart and
a catch-up pass converge on one run.

### 2.2 Plan — *what*

A plan is a named, ordered-by-dependency set of steps. It is **declaration only**: no worker id, no
hostname, no absolute path, no time of day, no secret. Its whole authority is §7.

### 2.3 Placement — *where*

Placement is a pure function of the step's declared capabilities, the read-only worker catalog, the
declared rank, and the catalog's live binding hint. It is computed **once per attempt, before the
attempt exists**, and committed in the attempt's dispatch record. It is never recomputed: J3.

A deliver step is not placed. It runs in the conductor's own drain and has no worker binding.

### 2.4 Evidence — *did it happen*

A step declares checks; the checks are predicates over the committed events of the attempt. A step
whose checks do not pass is not successful, no matter what its process exit code or its model
output said (J7). The run's outcome is a fold over step outcomes (§11.4).

### 2.5 Isolation rule

The axes may influence each other **only** through these named functions:

| From → To | Function | Section |
|---|---|---|
| trigger → plan | resolve(job_id) + canonicalization + digest | §7.4 |
| plan → placement | candidates(step, catalog) | §9.2 |
| plan → evidence | checks(step.verify) | §11.2 |
| attempt → evidence | observed(attempt events) | §11.3 |
| evidence → trigger | alert(run, reason) = a new run of an alert job | §13.6 |

No other cross-axis channel exists. A plan may not contain a placement decision beyond the checked
pin of §9.2, a trigger may not contain plan content beyond the job id, a placement may not weaken a
check, and a verdict may not start a step.

## 3. Terminology

Every term below is either an existing runtime noun or a noun this document owns. No term is a
table, a daemon, or a service.

| Term | Kind | Definition |
|---|---|---|
| **job** | config declaration | A named, reusable plan plus its limits. It carries no trigger and no state. |
| **plan** | config, frozen per run | The ordered DAG of steps plus limits. Canonicalized to bytes and digested. Once a run is admitted, that copy is the plan. |
| **step** | plan node | id, action, depends_on, requires, inputs, produces, timeout_ms, retries, effect, pin. A description, never an execution. |
| **trigger** | existing runtime fact | What makes a run exist: a cron slot, a manual command, or an alert. Its only job is to admit one input row with a slot token. |
| **slot** | derived string | The discriminator that makes (job, slot) unique: a fire time, manual:<ULID>, or alert:<failed run id>. |
| **run** | session | One execution of one job at one slot. A run is an ordinary session whose first input carries a job marker; its drain is driven by the conductor. There is no run table. |
| **run id** | derived string | job_id@slot. |
| **conductor** | loop driver decorator | The driver that runs a run session's drain. Stateless: every tick re-derives run state from committed events. |
| **attempt** | derived pair | (step, attempt_no), attempt_no starting at 1. A retry is a new attempt, never a re-run of the same one. |
| **dispatch record** | parent-side commit | The run stream's thread/started for one agent attempt, carrying the frozen placement and exec_worker. Written before the child is admitted, so a crash after it is a re-drive, not a lost spawn. |
| **placement** | derived once per attempt | The resolved execution site for an agent step: a worker id. Deliver steps have no placement. Computed from requires × the catalog × declared priority, then committed. |
| **artifact** | reference | One declared output of an attempt: name, media, bytes, sha256, ref, producer. The ref points at committed event bytes. Never a copy into a new store of record. |
| **effect** | plan attribute | read (retry freely) or write (a side effect that may have happened). Drives the retry gate (§10). |
| **verdict** | event | An evidence/verdict on the run stream: gate, passed, checks, attempt, plus the additive job key. |
| **step outcome** | derived enum | succeeded, unverified, invalid_output, timed_out, interrupted, needs_operator, failed. A skipped step has no attempt outcome; skipped is a step state. |
| **run outcome** | derived enum | succeeded, unverified, failed, cancelled. These are the only settled outcomes. |
| **park** | execution state | The conductor holding its drain while a run is unfinished: it yields TurnDone on a coarse tick and writes nothing. There is no "waiting session state". |
| **job marker** | additive input key | The object on the first admitted input row that carries run_id, job_id, slot, trigger, plan_source, plan_author, plan_digest and plan. It is authority; the human-readable content is not. |
| **run verdict** | event | The single evidence/verdict with gate run that records the settled run outcome. It is the only outcome record; session/ended never carries a job key. |
| **driver** | loop driver | The single loop that advances a session, selected when the session is assembled and fixed for its life. For a job run it is the conductor; for any other session it is the ordinary loop, the Router driver, a worker loop, or a harness. |
| **Router** | optional bundle session | An ordinary session that may act as front door, client, plan author and observer. It is never the driver of a job run and never a layer inside one. |
| **plan_source** | additive input field | Audit provenance of the frozen plan: config, inline, or router. It never overrides the plan bytes or the digest. |
| **plan_author** | additive input field | Audit provenance of the author: operator, a Router session id, or an external planner id. |

Two naming rules, stated so the vocabulary does not drift:

1. **No new noun for "task".** A step is not a task. A step's child may itself use the ordinary
   delegation surface, and that has nothing to do with plans.
2. **No new noun for "workflow".** A plan is a *data* description of who runs what next, executed by
   a drain that may be re-driven from bytes. It is not a program and it is not re-executed as a
   deterministic workflow.

Publishing is not a job action. A step that publishes is an agent step whose prompt asks it to use
the publish tool; the tool's own contract owns the sequence and its receipt, and the step declares
publish_ok as its evidence (V33). The job model owns only agent and deliver steps. The tool is
implemented ([ADR-085](decisions/ADR-085-the-publish-tool.md)); the retired conductor publish
action is kept only to replay plans frozen before it was retired ([ADR-084](decisions/ADR-084-publishing-is-a-tool-not-a-job-action.md)).

## 4. Invariants

These are the whole contract. A later change that violates one is a new design, not an edit.

**J1 — The stream is the sole truth for a run.** Every run fact is an event on the run session, or
on a child session reachable through thread/started. There is no run row, no board, no job status
column, no scheduler database. Status is a projection. Consequence: deleting the status projection
loses nothing.

**J2 — The plan is frozen at admit.** The run's first job-marked input carries the canonicalized
plan and its digest. The conductor executes that plan, byte for byte. A config edit after admit
never affects a live run; a re-drive re-reads the committed copy. The frozen digest is on the
stream, so "explain this run" does not trust the config file.

**J3 — Placement is computed once per attempt and committed before the child exists.** The dispatch
record carries exec_worker and the candidate list with the reason the winner won. A re-drive reuses
the committed choice and never re-runs selection. A retry is a new attempt with a new selection.
Deliver steps are not placed.

**J4 — Dispatch is idempotent by derivation.** (run, step, attempt) derives the child's thread id,
session id and task id by uuid5 (§6). The dispatch record is written by the parent before the child
input is admitted; the child input's idempotency key is derived too. A crash anywhere in the
sequence heals by re-deriving the same ids and re-admitting under the same key — never a second
child, never a second inbox row.

**J5 — The conductor is the only spawner of its run's steps.** No scheduler, no CLI, no HTTP
handler, no operator command ever spawns a step. The control plane only admits the run input row.
An operator cancel admits a control row; it does not write execution.

**J6 — A conductor never settles while the run is unfinished.** It parks (§5.4). It must never yield
Settled with an unfinished plan, because Settled plus an empty inbox ends the session completed and
a terminal end freezes the stream — the run would be permanently un-drivable. It ends the run
exactly once, through one session/ended.

**J7 — No evidence, no success.** An attempt is succeeded only with a passing evidence/verdict whose
checks the plan declared. A child's completed end, a zero exit status, and a report that merely
claims success are never success by themselves. An attempt that ends without evaluable evidence is
unverified, a first-class non-success. Unverified is permitted only where the plan explicitly
accepts it (§8.1).

**J8 — A retry is a new attempt, never a re-drive of the old one.** New identity, new child, new
placement. A step never rebinds a worker, never waits for a worker to come back, and never re-sends
a request whose effect is unknown (§10).

**J9 — The effect gate is decided before the retry, not after it fails.** effect: read may be
retried freely. effect: write may be retried only when the plan declares idempotent: true or the
previous attempt provably did not reach the effect. Otherwise the attempt is needs_operator and the
run halts honestly instead of guessing.

**J10 — Artifacts are references, not copies.** A text artifact is producer session, seq, bytes,
sha256 pointing into committed, immutable event bytes: no TTL, no storage, replayable forever. A
binary artifact is a blob path plus digest and TTL, and a missing blob is artifact_unavailable (the
check fails), never an empty string. An artifact over its declared cap is a failed attempt, never a
truncation.

**J11 — One clock per stage.** timeout_ms bounds exactly one attempt and is observed by the
conductor. deadline_ms bounds exactly one run and is observed by the conductor. Neither is used to
bound admission, dispatch, or queue pickup: "no candidate" is an instant refusal, not a slow
timeout. A wait for a busy worker is bounded by defer_budget_ms and is a distinct fact.

**J12 — Truth is monotone.** The first committed step verdict for an attempt, and the first
committed run verdict for a run, are final. Facts that arrive later from an abandoned child are
recorded on that child's stream and are never folded back into the run's outcome. Why: an abandoned
child may still finish; the run must not oscillate. A later verdict with the same (gate, attempt)
key is ignored and does not rewrite the first; a verdict for a later attempt is a retry's own fact.

**J13 — Job facts are non-context-bearing.** No placement candidate, verdict, check result, artifact
ref, digest, or slot token is rendered into a model prompt or into the cacheable prefix of a later
turn. The only model-visible job content is the child's task message, built deterministically from
the frozen step: identity (run session id, step, attempt), the prompt verbatim, and inlined text
inputs. The identity is the uuid, never the slot-bearing run id. Plan bytes, step digests, candidate
lists and verdicts stay on the run stream.

**J14 — A delivery is a receipt, not a return value.** A deliver step succeeds only when a
channel/delivered receipt for the frozen target and this attempt's call id exists and no failure
fact for the same call id follows it. A transport ok without a receipt, and a receipt for a
different target or a different call id, are both failures. Multi-part sends produce one logical
receipt; partial delivery is not success unless the plan explicitly accepts it.

**J15 — Capability is eligibility, never authority.** requires narrows which workers may run a step.
It grants nothing and enforces nothing: execution authority is the worker's own tool grants and
sandbox. A plan that needs a privileged effect must select a worker that already has it; the
selector cannot confer it, and a worker that lacks the tool fails the step honestly.

**J16 — No silent degradation.** Every refusal is an in-stream fact with a reason code: no
candidate, cap exceeded, digest mismatch, gate failed, allowlist miss, budget exhausted, identity
conflict. Nothing runs anyway, nothing is dropped without a record, and an unreadable child stream
is unknown — never "still running", never "dead".

**J17 — Cancellation is a terminal close.** A cancelled run writes exactly one run verdict with
outcome cancelled and ends session/ended with reason cancelled. It never resumes, never rebinds,
never dispatches another step. Outstanding children are abandoned, not interrupted: the conductor
has no write path into another session's stream. Their attempts settle interrupted with a cancelled
check, without writing to the child; their later facts are ignored by the run. Cancel is idempotent
by input key; a cancel that arrives after the run verdict is a no-op.

**J18 — The run verdict is the sole outcome record.** session/ended carries no job key. Status,
settlement and alerting read the run verdict first. A terminal end without a run verdict is failed
with reason ended_without_outcome; it is never inferred as success from the end reason.

**J19 — One run, one driver.** Exactly one driver advances a session. For a job run that driver is
the conductor, selected by the job marker on the stream; for any other session it is the loop the
deployment's profile and the session's committed facts select. The choice never changes during the
session, no two drivers share a stream, and a run is never handed from one driver to another.

**J20 — The Router is a client, never a co-driver.** A Router session may route intent, submit and
observe a run, cancel it, and author a candidate plan before admission. It may not drive a run,
write or settle a verdict, mutate a frozen plan, bind a worker, or bypass spawn admission. The
conductor never calls the Router to decide a step; a step that needs model judgment is an agent step.

## 5. The run: carrier, conductor, park, end

### 5.1 A run is a session

The three candidate carriers and why only the third survives:

| Carrier | Verdict |
|---|---|
| A durable job_runs table with a status column | **Refused.** A second store of authority; status must be a projection. |
| A supervisor daemon that spawns children on a timer | **Refused.** The control plane would be writing execution and a global dispatcher would exist. |
| **An ordinary session whose driver is a conductor** | **Chosen.** The only legal orchestrator is a drain. A trigger already admits an input row into an ordinary session. |

A run session is not special in any persistent way: no new state machine, no new event kind, no new
table. What is special is only which driver claims its drain and what that driver does.

### 5.2 The first input row carries the plan

The trigger admits one row:

~~~json
{
  "role": "user",
  "content": "Run job \"digest\" for slot 2026-11-23T09:00:00+08:00 (cron).",
  "job": {
    "run_id": "digest@2026-11-23T09:00:00+08:00",
    "job_id": "digest",
    "slot": "2026-11-23T09:00:00+08:00",
    "trigger": "cron",
    "plan_source": "config",
    "plan_author": "operator",
    "plan_digest": "sha256:9f2c...",
    "plan": { "steps": [ ... ], "limits": { ... } }
  }
}
~~~

Rules, in order of authority:

1. job is an additive key. The conductor reads it from the event bytes; it never parses the human
   content to recover the plan, the slot or the job id.
2. The content line is for humans, operators and the read model. It is never authority.
3. The row's idempotency key is run_input_key. A duplicate fire for the same (job, slot) is the
   same row, so a restart, a double tick and a catch-up converge on one run. An alert is the one
   exception: it uses alert_input_key, because its identity is derived from the failed run, not
   from a new trigger fire.
4. A row whose job key is present but unparsable (unknown schema version, missing plan, digest
   mismatch against the canonicalized plan) makes the conductor end the run failed with reason
   invalid_job_input. It does not fall back to "run as a plain agent": an unverified plan must not
   execute with the wrong authority.
5. A session whose stream has no job marker is never claimed by the conductor, even if the session
   id matches a derived run id. Routing is derived from the stream, not from a registry.
6. The first job-marked input is the run's plan. A second job-marked input on the same run is
   invalid_job_input; a plan is never mutated by a later steer.
7. The derived run session id must not already belong to a session without the same job marker. If
   it does, the trigger refuses to admit with run_identity_conflict; it never overwrites or reuses
   another session's stream.
8. plan_source is config, inline or router; plan_author is operator, a Router session id, or an
   external planner id. Both are audit provenance, never authority: the plan bytes and the digest
   remain the only execution input. In v1 only source config with author operator is admitted; any
   other source or author refuses with invalid_job_input until its phase lands.
9. A session without a job marker is not a run and carries no plan. A request that does not choose the
   job path does not need one; a job run always does, but the plan may be resolved from a declaration
   and need not be written by the caller.

The frozen plan is stored in the event, not in the content, for three reasons: events are the
immutable record; the digest can be recomputed by any reader; and the plan cannot be confused with
prompt text by a model.

### 5.3 The conductor

~~~text
serve driver
  ├─ Router session                  → RouterDriver
  ├─ thread lineage                  → WorkerLoopDriver
  ├─ job marker on first input       → JobLoopDriver(inner = the ordinary agent loop)
  └─ else                            → default agent loop
~~~

The branches are disjoint, and the job marker is authoritative: a stream whose first input carries a
job marker is a run and is driven only by the conductor. A Router session, a thread child and a
plain session are never claimed by the conductor, and a child's driver is its own loop, not its
parent's. The conductor is a decorator over a real loop driver. It is not a new engine and not a new crate of
execution logic. Its own turn is not a model call. Its state machine per tick:

~~~text
tick(now):
  promote_inputs()                            # drain newly admitted rows first (cancel, notes, answers)
  facts = derive(run_stream)                  # pure: plan, dispatch records, verdicts, outcomes
  if facts.run_verdict: ensure_end(facts); return finished
  if canceled(facts): write_run_verdict(cancelled); end(Cancelled); return finished
  if invalid_input(facts): write_run_verdict(failed, invalid_job_input); end(Failed); return finished
  for attempt in facts.outstanding:
      observe(child_stream(attempt))          # read-only
      if closed(attempt): settle_attempt(attempt)
      elif attempt_timed_out(attempt, now): settle_attempt(attempt, timed_out)
  facts = derive(run_stream)                  # settlements above are now visible
  if all_steps_terminal(facts) and no_outstanding(facts):
      write_run_verdict(fold(facts))
      end(completed_or_failed_or_cancelled)
      return finished
  if deadline_passed(facts): settle_outstanding(deadline); write_run_verdict(failed, deadline_exceeded); end(Failed); return finished
  for step in ready_steps(facts):
      if input_failure(step, facts) != none: settle_step(step, input_failure(step, facts)); continue
      if step.action == deliver: deliver(step); continue
      if no_capable_candidate(step): settle_step(step, no_candidate); continue
      if all_capable_busy(step): defer(step); continue
      dispatch(step)                          # §8.3, idempotent
  for step in retry_due(facts, now):
      if input_failure(step, facts) != none: settle_attempt(step, attempts_used(step) + 1, input_failure(step, facts)); continue
      if step.action == deliver: deliver(step, attempts_used(step) + 1); continue
      if all_capable_busy(step): defer(step); continue
      dispatch(step, attempts_used(step) + 1) # §8.7
  return TurnDone                             # park tick: no event written
~~~

Four properties are load-bearing:

* **Nothing is held in memory.** Every tick re-derives from the stream, so a takeover, a restart or
  a second server after a crash behaves identically. Memory may cache; it may never decide.
* **Ticks write nothing.** The park tick yields TurnDone with zero appends, so a run that waits for
  hours adds no bytes to its stream and invalidates no cache.
* **The conductor uses only legal ports.** Its own stream for writes, a read-only child-stream port
  for observation, an inbox writer for child admits, a spawner for child creation, and a session
  writer for creation. It never touches another session's store rows and never mutates a projection.
* **Cancellation is processed before observation and dispatch.** Once a cancel is consumed, no
  further step is dispatched and the run verdict is cancelled. A step verdict already committed
  remains a fact; the run outcome is still cancelled.

### 5.4 Park, and why park is the only honest wait

A terminal end freezes the stream. Therefore:

* a conductor **cannot** "settle and wait" — that would mark the run completed while steps are still
  running, freeze the stream, and lose the run forever;
* a conductor **cannot** park by ending the session and relying on a later wake — Idle after
  interrupted or failed is resumable, but a run that wants to wait for an hour is not a failure;
* a conductor **must** park inside supervision by yielding TurnDone on a coarse tick. The engine keeps
  consuming the same stream, and an interrupt is observed at each outcome, so a parked run is still
  cancellable within one tick.

Park mechanics:

| Property | Value | Rationale |
|---|---|---|
| Tick floor | 100 ms (JOB_TICK_MIN_MS) | responsiveness for cancellation and manual control |
| Tick ceiling | 5 s (JOB_TICK_MAX_MS) | a run may wait 24 h; 5 s is 17k wakeups/day, all no-ops |
| Adaptive sleep | clamp(next_due - now, floor, ceiling), where next_due is the minimum of attempt timeouts, retry backoff, defer budget and run deadline | a run with no outstanding work sleeps the ceiling; a run with a 2 s retry wakes exactly then |
| Events per tick | 0 | park must not grow the stream |
| Interrupt latency | at most one tick | supervision checks the flag at every outcome |
| Lease | untouched: one claim, ordinary heartbeat | a long park is not a crash; the claim count counts claims, not ticks |

The conductor promotes its own inputs. Otherwise a control row admitted while the run parks would
sit unconsumed until the run ended. The v1 meaning of a promoted row on a run stream:

| Row kind | v1 meaning |
|---|---|
| control/interrupt, or a steer row whose content is exactly "cancel" | cancel the run: no new dispatch, outstanding attempts settle interrupted with no child write, run verdict cancelled, end cancelled |
| any other admitted message | recorded as an operator note on the run stream; no plan mutation. The exception is a second job-marked input, which is invalid_job_input (§5.2) and is never downgraded to a note. |

Plan mutation by steer is deliberately out of scope. A plan is frozen at admit; a change is a new
run with a new slot.

### 5.5 Ending a run

The engine owns the end write: the drain is the sole writer of session/ended, and the engine maps a
driver's final outcome onto the reason. A driver cannot supply the end payload, and a second end
append would break the one-atomic-end rule.

A run's outcome does **not** ride session/ended. The conductor writes **one** evidence/verdict with
gate run (§11.1) before it yields its final outcome. The end reason follows from that outcome:

| Run outcome | Run verdict | Final loop outcome | session/ended.reason |
|---|---|---|---|
| succeeded | passed: true, job.outcome: succeeded | Stopped | completed |
| unverified | passed: false, job.outcome: unverified | Stopped | completed |
| failed | passed: false, job.outcome: failed, job.failed_step?, job.reason? | SoftFailed | failed |
| cancelled | passed: false, job.outcome: cancelled, job.reason: cancelled | Stopped with the terminal-cancel flag | cancelled |
| invalid input | passed: false, job.outcome: failed, job.reason: invalid_job_input | SoftFailed | failed |

unverified ends completed on purpose: the session finished its work, and the reason vocabulary has
no better word. The job-level truth is the verdict, which is where it belongs.

Two consequences:

* the settlement sweep settles a job firing from the run verdict, never from the bare end reason,
  which cannot tell success from an unverified run. A job stream that ends with no run verdict
  settles failed with reason ended_without_outcome, because the conductor's silence is not evidence;
* session/ended is not given a job key by anything. The run verdict is the only outcome record.

The conductor must make the verdict-then-end sequence idempotent. If it crashes after the verdict
and before the end, the next tick sees the verdict, writes no second verdict, and ends the session
with the verdict's outcome. If it crashes after the end, the sweep reads the verdict. A second run
verdict at gate run is ignored (J12).

### 5.6 Cancellation

Cancellation is a terminal close, not a pause and not a failure.

Sequence:

1. The operator admits a control row with idempotency key kian:job:cancel:<run_id>. A duplicate
   cancel is the same row and has no second effect.
2. The conductor promotes the row before observation and dispatch. If a run verdict already exists,
   the cancel is a no-op; the run is already settled.
3. For every outstanding attempt, the conductor writes a step verdict with outcome interrupted and
   check cancelled, without writing to the child's stream. The child is abandoned.
4. The conductor writes the run verdict with outcome cancelled, reason cancelled, and the list of
   step states (settled attempts keep their earlier outcome; outstanding attempts are interrupted).
5. The conductor yields the terminal-cancel outcome; the engine ends the session cancelled.

Properties:

* A cancelled run never resumes, never rebinds, never dispatches another step. It is terminal in the
  job sense and in the session sense.
* A cancel that races a step completion: cancel is processed first in the tick, so the run is
  cancelled even if the child had already completed but the conductor had not yet settled it. A step
  verdict committed before the cancel remains; the run outcome is still cancelled.
* A cancel that races the run verdict: first committed verdict wins. If the run verdict is first, the
  cancel is a no-op; if the cancel row is consumed first, the run is cancelled.
* A cancel does not kill a live child and does not interrupt a tool future. The runtime has no write
  path into another session's stream. The child ends whenever its own loop ends; its later facts are
  on its own stream and are never folded into the run (J12). This is honest abandonment, not a
  promise of preemption.
* A cancelled run is never alerted. Only failed runs alert.

### 5.7 Interruption is not failure

An attempt whose child ends interrupted is outcome interrupted. It is never rewritten as failed.
The retry gate decides whether a new attempt is legal:

* effect: read, or a write attempt with no tool/call fact: retry if budget remains;
* effect: write with any tool/call fact: needs_operator, because the effect may have landed;
* no retry budget: the step is settled interrupted and the run fails with reason step_interrupted,
  preserving the attempt's interrupted outcome in the step verdict. The run-level failure names the
  interrupted step; it does not relabel the attempt.

A run session that ends interrupted because its drain was interrupted (crash, shutdown) has no run
verdict and is resumable. It is not settled and not failed by the sweep. On takeover the conductor
re-derives and continues. Only a run with a committed run verdict is settled.

### 5.8 Control drivers and the Router boundary

A session is advanced by exactly one driver. The driver is selected when the session is assembled,
from its committed facts plus the deployment's declared profile:

| Selection fact | Driver |
|---|---|
| job-marked first input | the conductor |
| Router residency in the bundle profile | the Router driver |
| thread lineage from a parent | the worker loop |
| anything else | the ordinary agent loop or the selected harness |

The branches are disjoint. A job run is never a Router session and a Router session never becomes a
job run. A child session has its own driver and is not driven by its parent's conductor. Once
selected, the driver is fixed for the life of the session; a crash, restart or takeover re-selects
the same driver from the same facts. This is J19.

The Router is optional. It is an ordinary session whose driver is the Router driver, and it is the
intent plane, not an execution layer. A cron, a CLI call or an alert can admit a run with no Router
process; later phases add webhook and event triggers. When present, the Router relates to a run in
four ways, all outside the run:

1. **Front door.** Choose the entry: ordinary session, harness session, or job path; choose the job
   or template; decide the approval path.
2. **Client.** Submit a run, read status, and cancel it through the ordinary public surfaces. It
   receives a run id, not a control handle over the conductor.
3. **Plan author.** When the inline plan-admission phase lands, produce a candidate plan (or select
   a declared job); the candidate is validated by the same rules as any other plan and is frozen at
   admission. After admission the Router has no authority over it.
4. **Observer.** Read the same run, step and attempt projection as every other client; summarize,
   escalate, and submit a follow-up run.

The Router may not be a step, drive or settle a run, write a step or run verdict, mutate the frozen
plan, bind a worker, or bypass spawn admission. The conductor never calls the Router to decide a
step; if a step needs model judgment, it is an agent step with its own child session. This is J20:
the Router is a client of a run, not a co-driver.

Scheduling is two-level, and the levels never merge:

| Level | Owner | Decides |
|---|---|---|
| intent | Router, when present | entry, job or template, priority, budget envelope, approval path |
| execution | conductor and placement | step order, placement, retry, deadline, verdict, worker binding |

The Router expresses requirements; it does not place, dispatch, retry or settle. A requirement such
as "exact evidence" or "a worker with capability X" is a plan or limits input, not an instruction to
the conductor.

Entry modes follow from the same rule:

| Request | Plan? | Driver |
|---|---|---|
| chat, agent, harness session | no | that session's own loop |
| job run | yes, frozen at admit | conductor |
| observation or cancel of an existing run | no | the run's existing driver |

Only the job path needs a plan, and the plan need not be written by the caller: a declared job or a
template resolves from config, and later phases admit an inline plan authored by an operator or a
Router. A template is a declared plan with no trigger, so it adds no second planning mechanism.
Provenance is recorded on the first input as plan_source and plan_author; it is audit data, never
authority.

## 6. Identity derivations

All ids are uuid5 over committed inputs, so any process can recompute them from the stream.

~~~text
KIAN_NS                    = 99be60b5-c4d7-52ec-8563-6de7b8697856
job_ns(job_id)             = uuid5(KIAN_NS, "kian:job:" + job_id)
run_session(job, slot)     = uuid5(job_ns(job), "run:" + slot)
run_id                     = job + "@" + slot
run_input_key              = "kian:job:run:" + run_id
spawn_uid(run, step, n)    = uuid5(run_session, "kian:job:step:" + step + ":" + n)
child_thread_id            = uuid5(run_session, "kian:thread:" + spawn_uid)
child_session              = uuid5(child_thread_id, "kian:session")
task_id(run, step, n)      = uuid5(run_session, "kian:task:" + step + ":" + n)
child_inbox_key            = "thread:" + child_thread_id
plan_digest                = "sha256:" + hex(sha256(canonical_json(resolved_plan)))
step_digest                = "sha256:" + hex(sha256(canonical_json(frozen_step)))
alert_job(alert_id)        = "alert." + alert_id
alert_run_id               = alert_job + "@alert:" + failed_run_id
alert_input_key            = "kian:alert:" + failed_run_id
cancel_input_key           = "kian:job:cancel:" + run_id
~~~

Notes:

* KIAN_NS is a fixed namespace constant. Changing it re-derives every run session id and orphans
  every in-flight run. It is not a configuration knob.
* canonical_json sorts object keys recursively and emits no insignificant whitespace; the digest is
  over those bytes. Two configs that differ only in key order are the same plan.
* resolved_plan includes the job id, the steps with every default filled in, and the limits. The
  digest therefore covers identity and policy, not just prompt text.
* spawn_uid is seeded by the logical triple, not by an event sequence number. That is what makes a
  dispatch replayable after a crash: the plan already fixed the step and the attempt.
* Reusing the child thread and session shapes keeps the ordinary delegation read surface working on
  job children.
* The alert job id is synthetic and derived from the alert declaration; the alert run id is derived
  from the failed run id. Two different failed runs never share an alert run. The alert. prefix is
  reserved: a declared job id may not start with it, so a synthetic alert job can never collide with a
  configured job.

## 7. Configuration

### 7.1 One new section, one lowered trigger

The section is mounted at superagent.jobs; the excerpt below shows its body. It has three lists:
plans, triggers and alerts.

~~~yaml
jobs:
  jobs:
    - id: digest
      limits:
        deadline_ms: 900000
        max_parallel: 1
        prefer: []
        accept_unverified: []
        defer_budget_ms: 300000
        inline_max_bytes: 65536
        max_plan_bytes: 1048576
        max_prompt_bytes: 65536
      plan:
        - id: gather
          action: agent
          requires: [net.unrestricted]
          prompt: |
            Collect today's sources. Return the digest inputs as one
            markdown block, at most 40 lines.
          produces: {name: sources, media: text, max_bytes: 24576}
          timeout_ms: 120000
          retries: 1
          effect: read
          verify:
            - {check: final_message_present}
            - {check: min_tool_results, n: 1}

        - id: compose
          action: agent
          depends_on: [gather]
          inputs: [{step: gather, name: sources}]
          prompt: |
            You are the editor. Use the supplied sources; do not fetch.
          produces: {name: digest, media: text, max_bytes: 24576}
          timeout_ms: 120000
          effect: read
          verify:
            - {check: final_message_present}
            - {check: artifact_within_cap, name: digest}

        - id: deliver
          action: deliver
          depends_on: [compose]
          channel: imessage
          target: {handle: "+8613800000000"}
          content: {from_artifact: {step: compose, name: digest}}
          timeout_ms: 60000
          retries: 1
          verify:
            - {check: delivery_receipt, channel: imessage, target: {handle: "+8613800000000"}}

  triggers:
    - id: digest-morning
      job: digest
      expr: "0 9 * * *"
      tz: Asia/Shanghai
      catch_up: latest
      overlap: skip

  alerts:
    - id: digest-failed
      on_failure_of: digest
      channel: wechat
      target: {user_id: "oUSER@im.wechat"}
~~~

One alert per failed run needs no repeat knob: the alert input row's idempotency key is
kian:alert:<failed run id>, so a re-driven conductor cannot alert twice and a second failure is a
second run id.

limits is the whole job's budget and defaults; it is not a plan node:

| limits field | Default | Meaning | Section |
|---|---|---|---|
| deadline_ms | 1800000 | Wall-clock budget for the entire run, measured from the run input's ts. | §8.8 |
| max_parallel | 1 | How many steps of one run may be in flight at once. v1 = 1. | §8.2 |
| prefer | [] | Job-wide default for every step's prefer list. A step's own list wins outright. | §9.3 |
| accept_unverified | [] | Producer steps whose unverified outcome still satisfies a dependent. The run outcome becomes unverified, never succeeded. | §8.1 |
| defer_budget_ms | 300000 | How long a step may wait for a capable but busy worker before failing no_candidate. | §9.4 |
| inline_max_bytes | 65536 | Largest artifact a step may inline into an agent consumer's task message. Larger artifacts may feed a deliver step by reference, but not an agent input. | §12.4 |
| max_plan_bytes | 1048576 | Maximum canonical plan size at load. | §7.5 |
| max_prompt_bytes | 65536 | Maximum bytes of one agent step's prompt at load. | §7.5 |

Two structural decisions:

**(a) A job carries no trigger; a trigger names a job.** The trigger list is lowered at load into
the runtime's existing schedule vector, with a target that names the job. Everything the scheduler
already owns then applies with no second scheduler: the durable cursor, claim-firing on
(schedule_id, fire_time), misfire and catch-up, overlap skip, coalescing, one-shot-at-a-time, the
settlement sweep, and the schedule CLI. A job trigger is a schedule whose turn is a job run.

**(b) The job's resolved runtime form is a frozen value, not a live reference.** At trigger resolve
time the tick loop canonicalizes the plan and embeds it in the input row (§5.2). Editing jobs.jobs
afterwards changes only future runs. Editing a trigger's expr or tz resets that trigger's cursor as
the scheduler contract specifies; editing a plan does not touch any cursor because no cursor belongs
to a plan.

### 7.2 Step schema

| Field | Type | Default | Meaning |
|---|---|---|---|
| id | string | — | Step identity inside the plan; also the artifact namespace and the gate suffix. |
| action | agent or deliver | agent | agent: a child session bound to a worker. deliver: an in-conductor channel action. |
| prompt | string | — | Required for agent. The task message's verbatim body, plus the frozen inputs. |
| requires | string[] | [] | Capability names the worker must declare: at most 16, at most 64 bytes each, first character [a-z0-9], then [a-z0-9._-]. [] = any worker. |
| pin | string | absent | Exact worker id. When present, requires must be a subset of that worker's declared capabilities at dispatch (V6). Named pin to match the existing exec_worker verb. |
| allow_offline | bool | false | Admit an Offline worker as the last-ranked candidate. Default false: a step cannot progress without tools, so dispatching to an offline worker is a guaranteed timeout. |
| prefer | string[] | the job's limits.prefer, else [] | Worker ids this step would rather run on, in order; a soft tie-break inside the deterministic sort key. Absent or unrecognized ids rank nothing and never fail a load. Distinct from pin, a hard constraint. |
| depends_on | string[] | [] | Step ids that must reach a success outcome first (§8.1). |
| inputs | ref[] | [] | {step, name} artifact references; the producer's produces.name must exist (V5) and the producer must be a dependency (V29). |
| produces | object | absent | {name, media: text or binary, max_bytes}. At most one per step in v1; absent = no artifact. |
| output_schema | object | absent | JSON object passed to the child as its structured-output tool schema. At most 16 KiB. |
| timeout_ms | u64 | 300000 | One attempt (§8.6). Observed, not enforced, for agent steps; see §8.6. |
| retries | u8 | 0 | Extra attempts: total = retries + 1, at most 5 (V8). |
| effect | read or write | read | The retry gate (§10). A write with retries > 0 and no idempotent: true is a config error (V7). Deliver is always write. |
| idempotent | bool | false | Declares that repeating the effect is harmless. Only meaningful for effect: write. |
| verify | check[] | see §11.6 | Declarative predicates over the attempt's own facts. |
| on_failure | halt or continue | halt | halt: the run fails at this step once the step is terminal. A retryable failure first consumes its retry budget; halting before that would make retries meaningless. A terminal step failure halts the DAG; independent steps do not start. continue: independent steps still run; the run ends failed (not succeeded) unless the step is optional. |
| optional | bool | false | With on_failure: continue, a failed optional step does not fail the run. Requires on_failure: continue (V28). |

deliver steps use instead: channel (required), target (required, the channel's target shape), content
({text} or {from_artifact: {step, name}}, exactly one), timeout_ms, retries and verify. They have no
requires, no pin, no prefer, no allow_offline, no produces and no output_schema. effect is write and
idempotent is false; a successful delivery is proven by its receipt, not by a repeat-safe flag.

Publishing is not an action. A publishing step is an agent step with a prompt that uses the publish
tool; the tool's own contract owns the publish sequence and its receipt, and the step declares
publish_ok as its evidence (V33). The tool takes no arguments: it derives the run, the step, the
attempt, the base commit and the patch bytes from the committed streams. A new plan that names
action: publish is refused (V32); the retired action survives only for frozen-plan replay
([ADR-084](decisions/ADR-084-publishing-is-a-tool-not-a-job-action.md)).

### 7.3 Canonicalization and the digest

canonical_json(value) = recursively sorted object keys, no insignificant whitespace, UTF-8, arrays
in declared order. resolved_plan has every default filled in. plan_digest is
"sha256:" + hex(sha256(canonical_json(resolved_plan))). Consequences:

* defaults are part of the digest: a run admitted before a default changed is not the same plan;
* the frozen plan can be re-digested by an operator and compared to the stream, which is how
  "explain this run" is answered without trusting the config file;
* the step digest is over the frozen step, so a re-drive produces byte-identical prompts and
  dispatch records.

### 7.4 Validation

All checks run at config load and refuse the whole config with a path-qualified message. Nothing is
validated lazily except where stated. Unknown fields are refused on every struct.

| # | Rule | Refusal |
|---|---|---|
| V1 | Job id shape: at most 64 bytes, first character [a-z0-9], then [a-z0-9._-]. Step ids have the same shape. | invalid job id / invalid step id |
| V2 | Job ids unique. Trigger ids unique among all triggers and schedules known to the runtime. | duplicate job id / duplicate schedule id |
| V3 | Plan non-empty and at most 32 steps. Step ids unique. Every agent step declares prompt. | invalid plan |
| V4 | depends_on is a subset of the plan, has no self-edge and no duplicate entry, and is acyclic. | cycle in depends_on / duplicate dependency |
| V5 | Every inputs[i] names a producer step that declares produces with that name. Artifact names are unique per producer. | unknown artifact / duplicate artifact |
| V6 | requires is a valid capability list (at most 16, each at most 64 bytes). With pin, the pin is a declaration, so it is never a load error: the conductor checks requires is a subset of the pin's declared capabilities at selection and refuses pin_unknown or pin_ineligible at dispatch. Config therefore loads offline. | invalid capability (load) / pin_unknown, pin_ineligible (dispatch) |
| V7 | effect: write with retries > 0 requires idempotent: true. Deliver is exempt only because its retry gate is receipt-based. | write effect must be idempotent to retry |
| V8 | retries at most 5; timeout_ms at least 1000; timeout_ms * (retries + 1) <= deadline_ms. | budget cannot be spent |
| V9 | deadline_ms at least 60000 and at least the largest step timeout. | invalid deadline |
| V10 | max_parallel == 1 in v1. | max_parallel > 1 is not supported |
| V11 | produces.max_bytes in 1..=262144; media: binary is refused in v1. | invalid artifact cap |
| V12 | Every alerts[].channel differs from every deliver step's channel in the alerted job. | alert channel must be independent |
| V13 | Every trigger's job exists; catch_up is skip or latest; overlap is skip. | unknown job / unsupported trigger policy |
| V14 | Every step is reachable from a root; no orphan subgraph. | unreachable step |
| V15 | A job has at most one trigger in v1; that trigger uses overlap: skip and max_concurrent 1. | duplicate trigger / unsupported overlap policy |
| V16 | deliver steps: channel, target and content present; content exactly one of {text} or {from_artifact}; no requires/pin/prefer/allow_offline/produces/output_schema; channel known to the channel registry at load; target shape valid for that channel; effect is write; idempotent false; retries at most 5. | unknown channel / invalid target / invalid deliver step |
| V17 | output_schema is a JSON object of at most 16 KiB serialized. | invalid output_schema |
| V18 | Prompts and schema strings contain no literal matching the secret redactor's patterns. | prompt contains a secret-shaped literal |
| V19 | A job with no trigger is valid (manual-only); recorded, not an error. | — |
| V20 | Every alerts[].on_failure_of names a declared job, and every trigger's job exists. A declared job id may not start with alert. (V31), so an alert declaration cannot name a synthetic alert job; a synthetic alert job has no alert declaration and no trigger, so an alert cannot chain. | unknown job / alert chain refused |
| V21 | Every step resolves to at least one check after defaults. The only exception is an agent step listed in limits.accept_unverified with verify: [] exactly; that step settles unverified if its child completes and failed if it does not. A deliver step can never have verify: []. Every accept_unverified entry must name an agent step in this plan; an unknown entry or a deliver step refuses load. | plan declares no evidence |
| V22 | prefer entries are non-empty, at most 128 bytes, unique within one list. | invalid prefer list |
| V23 | For every artifact edge into an agent step, the producer's max_bytes is at most limits.inline_max_bytes. A deliver step's from_artifact edge is exempt because it reads by reference. | agent input exceeds inline cap |
| V24 | canonical_json(resolved_plan) is at most limits.max_plan_bytes. | plan exceeds size cap |
| V25 | Every agent prompt is at most limits.max_prompt_bytes. | prompt exceeds size cap |
| V26 | The derived child task message is at most 1 MiB; a plan that would exceed it is refused at load, not truncated at dispatch. | task message exceeds cap |
| V27 | Every check name is in the closed set of §11.2 and its parameters validate; artifact_within_cap names a produces on the same step or an input; delivery_receipt names a deliver step's channel and target. no_candidate, binding_conflict, artifact_unavailable, artifact_digest_mismatch, effect_gate and cancelled are conductor-added and are never declarable in verify. | invalid check |
| V28 | optional: true requires on_failure: continue. | optional step must continue on failure |
| V29 | Every inputs[i].step is a direct or transitive dependency of the consuming step. | input is not a dependency |
| V30 | A declaration requesting a later-phase policy is refused at load: output_schema or report_matches_schema in v1, event or parent triggers. Binary produces and max_parallel > 1 already fail V11 and V10. | phase_unavailable |
| V31 | Alert ids match the job-id shape, are at most 58 bytes so alert. plus the id fits V1, and are unique; at most one alert declaration names the same job; the channel is known at load and the target validates for it; a declared job id may not start with alert., which is reserved for synthetic alert jobs. | invalid alert / duplicate alert / reserved job id |
| V32 | No step declares the retired action publish. A publish is an agent step that calls the publish tool (§7.2), and a plan that names action: publish is refused before any publish-specific capability could be read. The publish field is refused on any step that is not the frozen publish action. | invalid action / invalid publish step |
| V33 | publish_ok is allowed only on an agent step that declares exactly one step input (the patch it publishes) and retries: 0, and a plan declares at most one such step. A publish is never retried: a second publish could not tell a landed write from a missing one. | invalid publish step |

V21 deserves its own sentence: a plan that cannot produce evidence is not a plan, it is a prompt
with extra steps. Evidence is the reason. The single exception is an explicit, named acceptance of
an unverified producer; it is never a global switch.

### 7.5 The trigger and the frozen config

A trigger is lowered at load into the runtime's schedule vector. The lowering preserves the durable
cursor and the settlement sweep; it changes only the target of the turn. The target names a job id.
A trigger is disabled by enabled: false; a disabled trigger is validated and listed but never fires.
A job with no trigger is manual-only.

The job's resolved runtime form is frozen at resolve time: the plan object in the input row is the
canonical plan with defaults filled in and the digest computed. The config file is never read again
by a live run. Deleting a job stops future triggers; in-flight runs continue from their frozen
plans. Editing a plan changes only new slots.

## 8. Readiness, dispatch, observation, retries, deadlines

### 8.1 Readiness is a pure function

~~~text
ready(step, facts) :=
      attempts_used(step) == 0
   and step has no dispatch record or step verdict for attempt 1
   and every d in step.depends_on: outcome(d) in SUCCESS
   and run not halted and run not cancelled and deadline not passed

retry_due(step, facts, now) :=
      attempts_used(step) > 0
   and last_outcome(step) in {failed, timed_out, interrupted, invalid_output}
   and attempts_used(step) <= step.retries
   and now >= last_verdict_ts(step) + min(2000 * 2^(attempts_used(step) - 1), 60000)
   and step has no dispatch record or step verdict for attempt attempts_used(step) + 1
   and every d in step.depends_on: outcome(d) in SUCCESS
   and run not halted and run not cancelled and deadline not passed
   and the effect gate permits another attempt
   and not artifact_failed(step)

artifact_failed(step) := the last settled attempt's verdict carries artifact_unavailable or
                         artifact_digest_mismatch

SUCCESS := { succeeded } union { unverified | producer is in limits.accept_unverified }
~~~

unverified is deliberately not in SUCCESS by default: a downstream step that consumes an
unverified artifact is building on nothing. But a plan may declare limits.accept_unverified for the
editorial case ("a digest with no receipts is still worth sending, but the run must not claim
success"). Even then the run's outcome is unverified, never succeeded.

A step that cannot be readied because a dependency failed is skipped, not attempted:

* if the step is optional and on_failure: continue, the skip does not fail the run;
* otherwise a skipped required step fails the run with reason dependency_failed;
* a halted run stops dispatching but never stops observing. Outstanding attempts still settle and
  are recorded, so the stream explains what was in flight when it halted.

Inputs are checked immediately before dispatch by `input_failure(step, facts)`, which returns none,
artifact_unavailable (missing, unreadable or over cap) or artifact_digest_mismatch (readable bytes
whose sha256 differs from the ref). A ready step with a non-none result is settled failed with that
check; it is not placed, it creates no child, and it is never silently replaced by an empty string or
a truncated prompt. The same pre-check runs before a retry dispatch, so a vanished input cannot
consume the retry budget. An artifact failure is terminal for the step: the stream cannot heal a
missing ref or altered bytes, so the step settles failed and is not retried.

### 8.2 Ordering

Ready steps dispatch in plan declaration order, never in DAG-topological order. Two runs of the same
plan therefore dispatch in the same order, which makes the streams diffable. max_parallel is 1 in
v1, so order is observable as sequence; a later parallel mode keeps declaration order as the
tie-break.

### 8.3 Dispatch: the exact sequence for an agent step

For one ready step, attempt n:

1. **Select placement** (section 9). This yields one worker id or a refusal (no candidate, all busy,
   pin unknown, pin ineligible). A refusal never creates a child.
2. **Derive** spawn_uid, child_thread_id, child_session and task_id (section 6). The spawner derives
   the child inbox key from child_thread_id when it admits.
3. **Check for an existing dispatch record** at that identity on the run stream. If present, this is
   a re-drive only when its step digest, attempt and task message match the frozen step; otherwise it
   is a child_identity_conflict and the run fails without adopting the record. A matching record
   skips to step 6 with the committed placement. This check is what makes the sequence idempotent; it
   costs one scan of the run's own stream.
4. **Check identity conflicts.** If child_session already exists with a different task, or the
   derived thread id is already bound to another parent, refuse child_identity_conflict and fail
   the run. Never adopt or overwrite another session.
5. **Commit the dispatch record**: one thread/started on the run stream carrying the child thread id,
   the parent run session, the worker's session id, the child task message, exec_worker, and the
   additive job key {run, step, attempt, step_digest, effect, candidates, rationale}. The worker's
   session id is the derived child_session. The record is a dispatch reservation, not the live
   binding: spawn admission is the sole binding authority, so a refused spawn leaves a reservation
   with no binding and no child, and there is no separate binding map. Parent-side write before any
   child write is what a takeover re-reads.
6. **Create the child**: create the session if the assignee differs, bootstrap it if a bootstrap is
   mounted, admit the task row under the thread-scoped key thread:<child_thread_id>, then wake the
   child. Each of the four is independently idempotent: create-if-absent, bootstrap-keyed,
   admit-keyed, wake-advisory.
7. **Record the attempt as in flight** - nothing to write: the dispatch record is the fact. The
   conductor's derived state sees "dispatched, no terminal fact yet" and observes it next tick.

A thread/started written here is a dispatch reservation, not proof that a live child exists. If the
spawn is refused after the record, the record remains as the attempt's fact, the attempt settles
failed, and no child is reported by status or by any projection. A re-drive may re-attempt creation
for the same attempt; it never adopts an unrelated child.

Two ordering rules that are easy to get wrong:

* **Never admit the child before the dispatch record.** A child that exists without a parent-side
  record is unreachable from the run after a crash: the run cannot derive its identity because it
  never committed the triple. The record-first order makes every crash window recoverable.
* **Never select a placement at dispatch time and again at retry time.** A retry is a new attempt:
  new selection, new record, new child. Reusing a selection across attempts would be a rebind.

If the spawner refuses before a child exists (binding_conflict, pin_ineligible, identity_conflict),
the attempt is settled failed with a check naming the refusal. The dispatch record remains as the
attempt's fact. A retry, if budget remains, is a new attempt and selects again; the refused worker
is ranked after untried workers (section 9.3). A stale busy view can therefore cost one attempt; a
plan that expects contention declares retries: 1.

### 8.4 Dispatch: the deliver action

A deliver step has no placement and no child. It does not write thread/started. The conductor invokes it directly from the tick; a deliver step is never passed to placement or to the no_candidate check. Its sequence is:

1. Derive call_id = job:deliver:<step>:<attempt>.
2. If the run stream already has a receipt or a result for this call_id, this is a re-drive: reuse
   them and settle from the receipt when one exists, otherwise from the result's disposition
   (section 13.5). A result without a receipt is not evidence of delivery.
3. Apply the effect gate (section 10): a deliver is a write, but its receipt is its proof. If an
   earlier attempt for this step has an intent, no receipt, and no result that reports not_started,
   the current attempt must not be dispatched; the earlier attempt is needs_operator and the run
   halts.
4. Resolve content: literal text, or read the artifact by reference, verify its sha256 and its cap.
   A missing, over-cap or mismatched artifact settles the attempt failed with artifact_unavailable
   before any intent is written; no send starts.
5. Shape the content through the channel's policy: allowlist, echo guard, splitting, truncation
   policy. Shaping is deterministic for a frozen target and policy.
6. Append tool/call with call_id, tool job.deliver, and the shaped content digest. The intent is
   durable before the adapter is called.
7. Call the channel adapter's send.
8. On a delivered disposition: append channel/delivered with call_id, attempt, channel, target,
   policy, words, parts_sent and parts_total, then append tool/result ok with the same call_id.
9. On an error disposition: append tool/result with ok: false, the disposition, channel, target and
   call_id; settle per section 13.3.

The receipt is not a return value that can be lost with the process. It is an event. The result after
it is a convenience for readers; the receipt is the commit point.

### 8.5 The child task message

The message is a pure function of the frozen step and the frozen text inputs:

~~~text
[kian-job] run=<run session id> step=compose attempt=1
[kian-job] You are one step of a plan. Do only this step.

<the step's prompt, verbatim>

Inputs:
- sources (text, 8123 B):
<the artifact text, verbatim>

Report when done: one short paragraph plus any tool evidence you relied on.
~~~

Properties:

* the header lines are content, not structure: the child needs no job parser and cannot be confused
  about its role. They are also the strippable marker for a synthetic message.
* no digest, no artifact ref, no plan, no candidate list, no timestamp and no slot token is
  present. The run identity is the session uuid, not the slot-bearing run id. The child sees
  identity, prompt and inlined text only. J13.
* the artifact text is inlined only when it is media: text and under limits.inline_max_bytes.
  Otherwise the plan is invalid at load (V23); the conductor never truncates an input.
* no live config value appears: the step is frozen, so a re-drive is byte-identical.
* the message is stored in thread/started.task, so a mismatch after a config edit is visible rather
  than silently different.
* the message is at most 1 MiB (V26); a plan that would exceed it is refused at load.

### 8.6 Observation, settlement, and the honesty boundary

Each tick, for every dispatched agent attempt without a terminal fact, the conductor reads the
child's stream through a read-only child-stream port and derives:

| Fact | Source |
|---|---|
| started / alive | the child's liveness: released for completed, cancelled or rotated; live for failed, interrupted or awaiting input; never started when there is no created session and no admitted input |
| last activity | the maximum event timestamp on the child stream, 0 if empty |
| report | the child's completion summary plus its final message |
| artifacts | for each produces, the last assistant message with non-empty text and no tool calls, or the structured output payload (P2) |
| delivery receipt | channel/delivered on the child's stream for an agent-driven reply, or on the run's stream for a deliver step |

Settlement writes exactly two facts for an agent attempt, in this order:

1. The artifact pair, if the step produces an artifact (section 12.3). It must be committed before
   the verdict so the check can read it.
2. evidence/verdict on the run stream: gate step:<id>, passed, checks, attempt, and the additive job
   key {run, step, attempt, outcome}. Failed checks name a detail.

The attempt outcome is derived from the verdict plus the child's end reason and the effect gate:

* passed and child completed: succeeded;
* verify is empty and the step is in accept_unverified, child completed: unverified;
* child completed but a check failed: failed;
* child ended failed or released other than completed: failed;
* child ended interrupted: interrupted, unless a write attempt has a tool/call fact, which is
  needs_operator;
* no terminal fact by timeout: timed_out, unless a write attempt has a tool/call fact, which is
  needs_operator;
* schema validation fails (P2): invalid_output.

> **Honesty boundary: step timeouts are observed, not enforced.** An interrupt is consumed at a
> driver's outcome boundaries, not inside an in-flight tool future. A child blocked inside a tool
> call bound to an offline worker therefore never sees a control interrupt, and there is no
> queue-pickup bound to fall back on. Consequences, stated rather than hidden:
>
> * timeout_ms means "the conductor stops waiting and records timed_out", not "the child is killed".
>   v1 does not admit a control interrupt to the abandoned child: a drain has no write path into
>   another session's stream, so the child keeps its worker binding and ends whenever its own loop
>   ends. The attempt's outcome is final; a later fact from the abandoned child is never folded back
>   into the run.
> * an abandoned write attempt is needs_operator, because it may still reach the effect;
> * a retry that selects the same worker finds it busy (1:1 binding) and defers until the abandoned
>   child ends and releases the binding. The repeat preference ranks a freed worker after untried
>   workers but never bypasses 1:1; on a single-worker fleet a wedged child blocks every retry until
>   the child ends. The step still consumes its retry and defer budgets and the run fails once the
>   step is terminal; the abandoned child may keep the worker bound past that point.
> * the designed fix is a cancellation seam that lets an interrupt cancel the in-flight tool future.
>   That is a core change and is out of v1. Until it lands, plans prefer short timeouts and read
>   effects over long blocking writes.

### 8.7 Retries

An attempt is retried iff all hold:

1. its outcome is in {failed, timed_out, interrupted, invalid_output}; never succeeded, unverified,
   needs_operator or skipped;
2. attempts_used(step) <= step.retries;
3. the effect gate passes: read always; write only if idempotent: true or the previous attempt
   provably did not reach the effect (section 10.2);
4. the run deadline has not passed and the run is not cancelled.

Backoff is derived, not stored: next_attempt_due = last_verdict_ts + min(2000 * 2^(n-1), 60000).
Here n is the number of the attempt that just settled, so the first retry waits 2 s and the fifth
waits 32 s. Because it is a pure function of the verdict's timestamp, a takeover computes the same
due time and does not need a timer table.

Caps: retries at most 5 (V8), the deadline, and the retry predicate itself. A step whose
attempts_used equals retries + 1 is terminal with its last outcome. There is deliberately no second
run-wide dispatch cap at sum(retries + 1): that is already the maximum number of attempts the plan
can request, so a cap at the same value could never bind. The deadline is the only additional
run-wide bound, and it is enforced at section 8.8. No retry path loops.

A retry does not re-run a settled attempt, does not rewrite its verdict, and does not reuse its
child. It is a new attempt with a new child, a new placement and a new verdict. A retry that selects
a worker whose earlier child is still live defers; it does not fail immediately and does not create
a second binding.

### 8.8 The run deadline

deadline_ms is measured from the run input's timestamp, not from the first dispatch, so time spent
waiting for a worker counts against the run. That is what an operator means by "the digest must be
out by 09:15".

On expiry:

* no new dispatch;
* every outstanding attempt is settled with outcome timed_out, except an outstanding write attempt
  with a tool/call fact, which settles needs_operator because it may still land;
* the run verdict is failed with reason deadline_exceeded;
* the run ends failed; live children are abandoned, not killed.

The deadline is a run bound and is never applied to an individual step; timeout_ms owns that. The
two clocks can never double-bound one wait.

## 9. Placement

### 9.1 What placement is allowed to decide

Placement answers exactly one question: which execution site performs this agent attempt? It may not
change the prompt, the checks, the timeout, the artifact caps, or the order. Everything placement
sees is the frozen step, the read-only worker catalog, and the catalog's live binding hint. If a plan
wants a different prompt on a different machine, it declares two steps with different requires.

### 9.2 The candidate set

~~~text
candidates(step, catalog) :=
    if step.pin:      { w in catalog | w.id == step.pin and step.requires subset w.capabilities }
    else:             { w in catalog | step.requires subset w.capabilities }

admissible(step, catalog, attempt) :=
    candidates(step, catalog)
      minus { w | w.state == Offline }                 unless step.allow_offline
      minus { w | catalog.live_binding(w) is non-empty }
~~~

The catalog is read-only, disposable, and returns no events, no rows and no wakes. Capabilities are
sanitized declarations. Two properties are inherited and stated because the requirements depend on
them:

* **Capability is a declaration, not authority** (J15). A worker that declares net.unrestricted but
  lacks the network is simply a step that fails. Nothing in the selector can grant or verify a
  privilege. Selection is eligibility; enforcement stays with the worker's tool grants and sandbox.
* **An offline worker is still a valid binding target** for a session, but a job step cannot
  progress without tools. An Offline worker is admitted only when the step declares
  allow_offline: true. That default is what makes "no available worker" true at refusal time
  instead of by a long timeout.

The live-binding exclusion is a hint, not authority. The catalog reports the session or thread
currently bound to a worker; the hint may be stale by one heartbeat. The spawn admission is the only
1:1 refusal authority. A stale view can select a busy worker, the spawn refuses binding_conflict,
and the attempt settles failed with that check; placement never creates a second binding.

The repeat rule is a **preference**, not an exclusion:

~~~text
tried(w, step) := w is the worker of an earlier attempt of this step in this run
                  whose outcome was not succeeded
~~~

A tried worker is admissible again once it is otherwise free. The sort prefers untried workers, but
it never forbids the only free worker. While the earlier child is still live, 1:1 makes that worker
busy and the retry defers. Once the child ends and the binding is released, a retry can use it. This
is what makes a retry possible on a single-worker fleet after the first child releases, and why a
wedged child blocks that retry until the deadline instead of allowing a second binding.

If admissible is empty, distinguish four cases:

| Case | Test | Result |
|---|---|---|
| no capable worker | candidates is empty | instant no_candidate; no child; attempt failed |
| all capable workers are offline | candidates nonempty, every candidate is Offline, allow_offline false | instant no_candidate; no child; attempt failed |
| all remaining candidates are busy | candidates nonempty, and every candidate that passes the offline filter has a live binding | defer, no verdict, until defer_budget_ms |
| pin unknown or ineligible | pin not in catalog, or requires not subset of its capabilities | instant no_candidate; no child; attempt failed |

A deferral writes nothing: no verdict, no dispatch record, no child. It is a next_due_ms in the
status projection only. After defer_budget_ms, the attempt settles failed with check no_candidate and
detail all_busy. If retries remain, the next attempt selects again; the run fails only when the step
is terminal with no retry left. This is the only wait in placement, and it is distinct from a step
timeout and from a run deadline.

### 9.3 The order

~~~text
sort key := ( state_rank, tried_rank, declared_rank, id )
state_rank    := Online=0, Draining=1, Offline=2, Unknown=3
tried_rank    := never tried=0, tried=1
declared_rank := index in the step's prefer, else the job limits.prefer, else usize::MAX
id            := byte order, only to make the order total
~~~

"Most recent success" is deliberately not in the key. It would make placement depend on hidden
global history, make two runs of the same config non-diffable, and starve a worker that had one bad
day. Stickiness is expressed explicitly with prefer; a plan that wants a machine names it.

### 9.4 Busy, refusal, and the defer budget

* **A capable worker with a live binding is not a candidate for this attempt.** It is busy by the
  1:1 rule. If every capable worker is busy, the step defers; it does not fail on the first tick.
* **A deferral is not an attempt.** It has no attempt number, no dispatch record and no verdict. It
  is recorded only as status next_due_ms. When a worker frees, the next tick dispatches.
* **The defer budget is anchored to durable facts, not to process memory.** defer_started(step) is the
  later of the run input's timestamp and the latest dependency verdict's timestamp; for a retry it is
  the later of that and the retry's backoff due time. next_due_ms = defer_started + defer_budget_ms,
  computed from the stream. A takeover derives the same anchor, so a crash cannot reset the budget,
  and no event is written merely because a step is waiting.
* **defer_budget_ms bounds the wait.** At expiry, the attempt settles failed with no_candidate and
  detail all_busy; a retry, if budget remains, selects again. The run fails only when the step is
  terminal. A plan that wants a longer wait raises the budget; a plan that wants an instant refusal
  sets it to 0.
* **A spawn refusal is authoritative.** If the spawner refuses binding_conflict, the attempt is
  settled failed with that check. A retry selects again, preferring untried workers. The refused
  worker is not blacklisted forever; it may be free on the next attempt.
* **A pin is a hard constraint.** pin_unknown and pin_ineligible are instant no_candidate, never a
  defer. A pin that names an offline worker with allow_offline false is also instant no_candidate.
* **A plan cannot select on health it cannot see.** The catalog's state is advisory; a worker that
  dies after selection is discovered by the child's timeout, not by re-selection. Placement is
  immutable once the dispatch record is committed (J3).

### 9.5 One worker per live binding

One worker has at most one live bound worker session. That exclusivity is session-level: a second
spawn for the same worker is refused at admission, regardless of which conductor, CLI or trigger
asked. Placement's live-binding exclusion is an optimisation and an observability surface, never
the authority. Two conductors that both read an idle worker can both commit a dispatch record; the
first spawn wins, the second is refused binding_conflict, and the second attempt settles failed.
There is never a second binding.

## 10. Effects and the retry gate

### 10.1 Declaration

Every step declares effect: read or write. read means repeating the work cannot change the world;
write means it can. deliver is always write. idempotent: true means repeating the write is harmless
and the retry gate may treat it like a read.

The declaration is a statement by the plan author, not a runtime proof. The conductor enforces the
consequences: it refuses to retry a non-idempotent write whose effect may have landed.

### 10.2 The gate

The gate is evaluated before a retry and before settling needs_operator. It reads only the attempt's
committed facts:

| Effect | Facts before the end | Retry? | Otherwise |
|---|---|---|---|
| read | any | yes, if budget and deadline | failed/interrupted/timed_out stays as observed |
| write, idempotent: true | any | yes, if budget and deadline | - |
| write, non-idempotent | no tool/call in the attempt | yes, if budget and deadline | - |
| write, non-idempotent | a tool/call, no result/receipt | no | needs_operator |
| write, non-idempotent | a result that reports success | no; the attempt may be succeeded if checks pass | - |
| write, non-idempotent | a result that reports an error | no | needs_operator (partial effect cannot be ruled out) |
| deliver | a receipt for this attempt's call_id | no; succeeded only if the section 13.4 receipt check passes | - |
| deliver | an intent, no receipt, and no result that reports not_started | no | needs_operator |
| deliver | not_started | yes, if budget | failed |

"No result yet" counts as may-have-landed because a durable command row can outlive the session that
issued it. The conductor never assumes that a missing result means no effect. That is the whole point
of the gate.

### 10.3 needs_operator

needs_operator is an outcome, not a retry. It means the runtime cannot decide whether the effect
landed. The step settles needs_operator, the run halts, and the run verdict is failed with reason
effect_unknown and failed_step naming the step. The ledger is the step verdict's effect_gate check,
which names the evidence (tool/call, no result) and the decision.

v1 operator actions are:

* re-run the job at a new slot, after inspecting the effect; or
* cancel the run, which is terminal.

A resume with --assume-no-effect is not v1: it would let an operator assert a fact the runtime could
not prove. If it ships later, it is an explicit, audited operator action, not an automatic retry.

### 10.4 The ledger

The effect gate is not a separate store. It is a check on the step verdict:

~~~text
{ check: "effect_gate", passed: <retry was legal or the attempt was not retried>,
  detail: "read" | "idempotent" | "no tool/call" | "tool/call without result" | "receipt" | "not_started" }
~~~

The verdict is the audit record. An operator can read the step's tool/call and tool/result facts and
reproduce the decision without trusting the conductor's memory.

## 11. Evidence and verdicts

### 11.1 The verdict facts

A verdict is an evidence/verdict event on the run stream. Its base shape is gate, passed, checks and
attempt. It carries an additive job key.

**Step verdict**, one per settled attempt:

~~~json
{
  "gate": "step:compose",
  "passed": true,
  "attempt": 1,
  "checks": [
    {"check": "final_message_present", "passed": true},
    {"check": "artifact_within_cap", "name": "digest", "passed": true}
  ],
  "job": {
    "run": "digest@2026-11-23T09:00:00+08:00",
    "step": "compose",
    "attempt": 1,
    "outcome": "succeeded"
  }
}
~~~

**Run verdict**, exactly one per settled run:

~~~json
{
  "gate": "run",
  "passed": true,
  "attempt": 0,
  "checks": [],
  "job": {
    "run": "digest@2026-11-23T09:00:00+08:00",
    "outcome": "succeeded",
    "plan_digest": "sha256:9f2c...",
    "steps": [
      {"step": "gather", "outcome": "succeeded"},
      {"step": "compose", "outcome": "succeeded"},
      {"step": "deliver", "outcome": "succeeded"}
    ]
  }
}
~~~

Rules:

* passed is true only for succeeded. unverified, failed and cancelled are passed: false.
* A step gate's attempt is the attempt number; a run gate's attempt is 0.
* A step verdict's job key names the outcome, so a reader does not have to re-derive it from child
  events. The outcome must agree with the checks and the child's end; a mismatch is a corrupt
  record, not a second interpretation.
* The run verdict's steps array lists every step's final state, including skipped steps. A step
  that was never attempted because the run halted, reached its deadline, was cancelled, or failed
  before dispatch is recorded as skipped with no attempt. That skip is a bookkeeping final state,
  not a second cause, and it does not supply the dependency_failed reason; dependency_failed means a
  required step was skipped because a dependency did not succeed.
* The run verdict is committed before the end. It is never carried on session/ended.
* First committed verdict at a (gate, attempt) key wins. A later verdict with the same key is
  ignored; a verdict for a later attempt is the retry's own fact and does not rewrite the earlier
  one (J12).

### 11.2 Checks

Plan checks are pure predicates over the attempt's own committed events. The set is closed; a plan
cannot invent a predicate. Conductor-added checks record a decision the conductor made from committed
facts, including the absence of a child; a plan cannot declare them. P2 adds report_matches_schema.

| Check | Parameters | Meaning |
|---|---|---|
| final_message_present | - | The child has a final assistant message with non-empty text and no tool calls, or the structured output payload when output_schema is set. |
| tool_result_since_last_user_input | - | At least one tool/result event after the child's last user input. |
| no_error_tool_result_tail | k, 1..10 | The last k tool/result events contain no error. |
| min_tool_results | n, 1..100 | At least n tool/result events in the attempt. |
| artifact_within_cap | name | The named artifact exists, its bytes are at most produces.max_bytes, and its sha256 matches the ref. |
| delivery_receipt | channel, target | A channel/delivered receipt for this attempt's call_id, this channel and this target exists, with no later error tool/result for the same call_id. |
| publish_ok | - | The child of this attempt called the publish tool exactly once, with no arguments, and has a tool/result receipt that is ok: true with a 40-hex commit, the derived branch selfdev/<run_session>, and, when the gate is configured, gate.exit_code 0. An intent without a receipt is a failed check that settles needs_operator, never a pass. |
| report_matches_schema | schema | P2. The structured output validates against the step's output_schema. |
| no_candidate | - | Conductor-added, not declarable. Placement produced no dispatchable candidate; detail is no_capable, all_offline, pin_unknown, pin_ineligible, or all_busy after the defer budget. |
| binding_conflict | - | Conductor-added, not declarable. Spawn admission refused because the worker already has a live binding; no child exists. |
| artifact_unavailable | - | Conductor-added, not declarable. A required input artifact is missing, over cap or unreadable; the step does not consume a substitute. |
| artifact_digest_mismatch | - | Conductor-added, not declarable. A readable artifact's bytes do not match the ref's sha256; the stream is corrupt and the step fails rather than consuming altered bytes. |
| effect_gate | - | Conductor-added, not declarable. Records the retry decision: passed when no retry was attempted or a retry was legal, failed when the attempt is needs_operator. Detail names the evidence. |
| cancelled | - | Conductor-added, not declarable. The attempt was settled interrupted because a cancel was consumed; the child was not written. |

A check that cannot be evaluated (for example, artifact_within_cap when no artifact was produced)
is a failure, not an abstention. The one exception is an explicit verify: [] accepted by
accept_unverified, which produces no checks at all and therefore cannot pass.

### 11.3 Step outcomes

A step is either skipped (no attempt) or settled with exactly one outcome. The attempt outcomes are
total and disjoint; skipped is a step state, not an attempt outcome:

| Outcome | Condition |
|---|---|
| succeeded | The child completed (agent) or a receipt for this attempt is committed (deliver), and every check, including the section 13.4 receipt check, passed. |
| unverified | The step is in accept_unverified with verify: [], and the child completed. There is no evidence; this is a first-class non-success. |
| invalid_output | P2: the structured output does not validate against output_schema. |
| timed_out | No terminal child fact by timeout_ms. If a write attempt has a tool/call, the outcome is needs_operator instead. |
| interrupted | The child ended interrupted. If a write attempt has a tool/call, the outcome is needs_operator instead. |
| needs_operator | The effect gate cannot prove whether the effect landed. |
| failed | The child ended failed or released other than completed; a check failed; an artifact exceeded its cap; an input was unavailable; a dispatch was refused; or no evidence exists and the step is not in accept_unverified. |
| skipped (step state) | A dependency did not succeed and the step was not attempted. There is no attempt verdict. |

Precedence when more than one condition applies: needs_operator (effect unknown) > invalid_output >
timed_out > interrupted > failed > unverified > succeeded. A child that ends interrupted never has
its attempt outcome rewritten as failed; a write with a tool/call is needs_operator, not failed.

A step with verify: [] and accept_unverified that ends failed is failed, not unverified: unverified
requires the child completed.

### 11.4 Run outcomes

A run is settled with exactly one outcome. The outcomes are total over settled runs:

| Outcome | Condition |
|---|---|
| succeeded | Every required step succeeded; no step is unverified or needs_operator. An optional step may be failed, invalid_output, timed_out, interrupted or skipped; that does not change the run outcome. |
| unverified | Every required step is succeeded or unverified; at least one step is unverified; every unverified step is in accept_unverified; no step is needs_operator; no required step is failed, invalid_output, timed_out, interrupted or skipped. Optional steps may have any other outcome except needs_operator. |
| failed | A required step is failed, invalid_output, timed_out, interrupted or skipped; any step is needs_operator; or the run itself failed with child_identity_conflict, deadline_exceeded, invalid_job_input, dependency_failed, effect_unknown or ended_without_outcome. |
| cancelled | A cancel was consumed before the run verdict. Cancel is terminal. |

Rules:

* A required step's interrupted attempt is not relabeled failed. The run fails with reason
  step_interrupted and failed_step naming the step; the attempt's own outcome stays interrupted in
  its step verdict.
* An optional step's non-success does not fail the run, except needs_operator, which is always
  fatal to a successful run. A required step's skip fails the run.
* A run with any unverified step is unverified, never succeeded.
* A run with a needs_operator step is failed with reason effect_unknown, never unverified.
* The run verdict's passed flag is true only for succeeded.

The run verdict's reason, when present, is one of: child_identity_conflict, step_failed,
step_interrupted, step_timed_out, step_invalid_output, dependency_failed,
deadline_exceeded, invalid_job_input, effect_unknown, ended_without_outcome, cancelled.
A step's no_candidate, all_busy, effect_unknown or artifact failure is named by failed_step and the
step verdict's checks; it is not a separate run reason. When more than one applies, effect_unknown
wins, then child_identity_conflict, then deadline_exceeded, then invalid_job_input, then the first
step failure in declaration order.
* The lattice is total: every settled run has exactly one of these four outcomes. A run whose
  session ends interrupted without a run verdict is not settled; it is resumable and its status is
  interrupted.

### 11.5 First final

The first step verdict for an attempt and the first run verdict for a run are final. A re-driven
conductor that sees them does not append a second. A second verdict with the same (gate, attempt)
key is a no-op and is not counted; a verdict for a later attempt is a new attempt's fact. This is what makes a crash after the verdict and before the end safe: the verdict is
the truth, and the end is a lifecycle detail.

### 11.6 Defaults

If a step does not declare verify, the default is:

* agent: final_message_present, plus artifact_within_cap for every produces;
* deliver: delivery_receipt for the step's channel and target.

A publish step is an agent step that declares publish_ok explicitly (V33); it has no implicit
default, because a publish without a receipt is not a publish.

A step may set verify: [] only when it is listed in limits.accept_unverified, and only when it is
an agent step. A deliver step can never have verify: []: a delivery without a receipt is not a
delivery. A step with verify: [] is unverified when its child completes and failed when it does not.

## 12. Artifacts

### 12.1 The reference

A text artifact is:

~~~json
{
  "name": "digest",
  "media": "text",
  "bytes": 8123,
  "sha256": "sha256:77de...",
  "ref": {"session": "<producer session id>", "seq": 42},
  "producer": {"run": "digest@...", "step": "compose", "attempt": 1}
}
~~~

The ref points at committed event bytes. The artifact is not copied into a new table and has no TTL.
A reader that has the event stream can replay the exact bytes forever.

### 12.2 The producer

An agent step's artifact is its final answer: the last assistant/message with non-empty text and no
tool calls. When the step declares output_schema (P2), the artifact is the structured output payload
instead. There is no child tool named job.artifact; the child is not asked to announce anything. The
conductor derives the artifact from the child's own stream, so a model cannot forget to declare it.

A deliver step produces nothing. A step that declares produces but never produces a final message
fails artifact_within_cap; it does not invent an empty artifact.

### 12.3 The artifact pair and its repair

The conductor commits the artifact on the run stream as an ordinary tool/call plus tool/result pair:

~~~text
tool/call   call_id = job:artifact:<step>:<attempt>
            tool = job.artifact
            args = {ref: <artifact ref>}
tool/result call_id = job:artifact:<step>:<attempt>
            ok = true
            artifact = <artifact ref>
~~~

Rules:

* The pair is committed before the step verdict, so artifact_within_cap can read it.
* The pair is idempotent by call_id. If the call exists and the result exists, the conductor
  recomputes the sha256 and compares; a mismatch fails the step with artifact_digest_mismatch.
* If the call exists and the result is missing (crash between appends), the conductor appends the
  missing result. It does not append a second call.
* If both are missing, the conductor commits both. It never appends a duplicate pair for the same
  attempt.
* The pair is written by the conductor, not by the child. A child's own tool calls are ordinary
  work and are never treated as artifact declarations.
* The artifact ref is never rendered into a model prompt. A consumer sees inlined text, not a ref
  (J13).

### 12.4 Caps and inlining

* produces.max_bytes is in 1..=262144 (V11). An artifact over the cap fails the attempt with
  artifact_within_cap false. It is never truncated, and a truncated value is never written.
* limits.inline_max_bytes bounds the inline handoff to an **agent** consumer. V23 refuses at load a
  plan whose agent input edge has a producer max_bytes above the cap. The child task message inlines
  the text, so the consumer never needs a ref and J13 holds.
* A **deliver** from_artifact edge is exempt from the inline cap: the conductor reads the bytes by
  ref, verifies the sha256, and sends them. A 256 KiB artifact can be delivered without being
  inlined into a model prompt.
* Binary artifacts are designed, not v1. They need an upload seam and a blob root, and V11 refuses
  media: binary. When they land, a missing blob is artifact_unavailable; it is never an empty
  string.
* Text artifacts have no TTL. Deleting the producer session would break the ref, so retention obeys
  the same rule as the event stream: the bytes are kept as long as the run is readable.

### 12.5 Availability

An input artifact that is missing, unreadable or over cap is caught by the pre-dispatch input check
and fails the attempt with artifact_unavailable. If the producer's ref is readable but the bytes no
longer match its sha256, the attempt fails with artifact_digest_mismatch: that is a corrupt stream,
not an artifact. Either way the attempt is failed, the run fails when the step is terminal, and the
conductor does not retry the producer silently or substitute an empty string.

## 13. Delivery and alerts

### 13.1 Delivery is a conductor action

A deliver step runs in the conductor's own drain. It is not placed, has no worker candidate, no
capability requirement and no child session. The channel adapter is server-side; the conductor
calls it with the frozen target and the shaped content. The adapter's allowlist, echo guard,
splitting and truncation policy are part of the send, not part of the plan.

### 13.2 The sequence

See section 8.4 for the dispatch sequence. The delivery-specific rules are:

* The content is either literal text or one artifact, read by reference and digest-verified.
* Shaping happens before the intent is appended. The same frozen target and policy produce the same
  shaped bytes on a re-drive.
* The intent is a tool/call with call_id job:deliver:<step>:<attempt>, tool job.deliver. It is
  committed before the adapter is called. A crash after the intent and before the receipt is an
  unknown effect, not a safe retry.
* A successful send produces one channel/delivered receipt with call_id, attempt, channel, target,
  policy, words, parts_sent and parts_total. The receipt is committed before the ok result.
* A failed send produces a tool/result with ok: false and a disposition. It does not produce a
  receipt.
* The ok result is a convenience. The receipt is the commit point; a reader that finds a receipt and
  no result treats the delivery as successful and repairs the result.

### 13.3 Dispositions

| Adapter disposition | Durable facts | Attempt outcome | Retry |
|---|---|---|---|
| delivered | receipt, then ok result | succeeded (if the receipt check passes) | no |
| not_started | error result | failed | yes, if budget; the send did not start |
| unavailable | error result | failed | yes, if budget |
| delivery_failed | error result | failed; needs_operator if the adapter says a part may have landed | yes, only if budget and no part may have landed |
| may_have_completed | error result | needs_operator | never automatic |
| in_flight | error result | needs_operator | never automatic |
| receipt append failed | intent, no receipt, no ok result | needs_operator | never automatic; the send may have landed |
| result append failed after receipt | receipt, no result | succeeded after repair | no; the receipt is the commit |

A receipt append failure is not a delivery failure. The conductor must not append an ok result and
must not claim success; it records needs_operator, because the adapter may have sent the message and
the runtime failed to record it.

### 13.4 The receipt check

delivery_receipt passes iff all hold:

1. a channel/delivered receipt exists on the run stream;
2. its call_id equals the step's attempt call_id;
3. its channel and target equal the frozen step's channel and target;
4. its attempt equals the step attempt;
5. no tool/result for the same call_id after the receipt has ok: false;
6. for a multi-part send, parts_sent equals parts_total; a partial receipt does not pass.

A receipt for a different target, a different call_id or a different attempt is not evidence for
this step. A transport ok without a receipt is not evidence. A receipt followed by an error result
is not evidence of success.

### 13.5 Crash repair

* Crash after the intent, before the adapter returns: intent with no receipt. The attempt is
  needs_operator; no automatic retry. The child (none) and the target are frozen.
* Crash after the adapter returns, before the receipt: same as above. The runtime cannot know
  whether the message landed.
* Crash after the receipt, before the ok result: the next tick sees the receipt for the call_id. If
  no later error result exists, it appends an ok result and settles succeeded. The receipt is the
  commit; the result is repaired.
* Crash after the ok result, before the step verdict: the next tick sees the receipt and result and
  settles succeeded. No second send.
* Crash after the step verdict: first verdict is final; the run continues.

### 13.6 Alerts

An alert is a synthetic job with one deliver step. It is not declared in jobs.jobs; the runtime
constructs it from an alert declaration:

~~~yaml
alerts:
  - id: digest-failed
    on_failure_of: digest
    channel: wechat
    target: {user_id: "oUSER@im.wechat"}
~~~

One failed run produces exactly one alert run:

~~~text
alert_job   = "alert." + alert.id
alert_slot  = "alert:" + failed_run_id
alert_run   = alert_job + "@" + alert_slot
alert_key   = "kian:alert:" + failed_run_id
~~~

The alert run's first input carries a job marker with a one-step deliver plan. Its trigger is alert;
alert is a synthetic trigger, never a configured one. A marker is accepted as an alert only when all
hold: job_id starts alert., slot starts alert:, the suffix of slot is a run id whose run verdict is
committed failed, and the frozen plan is the alert plan for that declaration. Otherwise admission is
refused invalid_job_input. The marker is validated, not cryptographically authenticated; admission
itself is a trusted local control path. Its channel must differ from every deliver channel of the
failed job (V12). Its content is a short human-readable failure report: job, run, failed step,
reason and a pointer to the run. It is not a retry and it does not carry the failed run's artifacts.

Admission:

* after the failing conductor commits the run verdict, it admits the alert input under alert_key;
* if the conductor dies before admitting it, the settlement sweep admits the same input under the
  same key from the run verdict;
* the key makes the two paths idempotent: the first admitted row wins, the second is a duplicate and
  is ignored.

Alert declarations are resolved from the current superagent.jobs configuration when the alert is admitted,
not from the frozen run plan. Disabling or deleting an alert before admission stops it; an alert
run already admitted is unaffected. This is the one deliberate exception to freezing the job's
declarations: the kill switch must be able to stop new alerts without changing the source run's
frozen plan.

Guarantee: every run that commits a failed run verdict and has an enabled alert declaration when
the alert is admitted gets exactly one alert run, admitted by the conductor or by the sweep. A run that ends without a run verdict has no
evidence of failure and gets no alert; the sweep may settle it failed ended_without_outcome for its
own bookkeeping, but it does not invent an alert. A cancelled or unverified run does not alert. An
alert run that fails does not produce another alert; an alert synthetic job has no alert declaration
and no trigger, and V20 refuses an alert whose on_failure_of names an alert job. An alert whose
channel is down fails as an ordinary deliver run; the source run's outcome is unchanged.

### 13.7 Kill switch

A trigger or alert with enabled: false is validated and listed but never fires. Disabling a job's
only trigger stops new runs; in-flight runs continue from their frozen plans and may still alert.
Disabling an alert stops new alerts; already-admitted alert runs finish.

## 14. Event and metric surface

### 14.1 Additive event keys

The run and its children use existing event kinds. The new facts are additive keys, omitted when
unset:

| # | Fact | Where | Shape |
|---|---|---|---|
| A1 | input/admitted.job | the run's first input | {run_id, job_id, slot, trigger, plan_source, plan_author, plan_digest, plan:{steps,limits}} |
| A2 | thread/started.job | the run stream, for an agent attempt | {run, step, attempt, step_digest, effect, candidates, rationale}; the top-level thread/started worker is the execution site |
| A3 | evidence/verdict.job | the run stream, at a step or run gate | step: {run, step, attempt, outcome}; run: {run, outcome, plan_digest, steps, failed_step?, reason?} |
| A4 | channel/delivered.call_id | the run stream, on a delivery receipt | call_id and attempt, added to the existing receipt fields |

A1 is authority for the frozen plan. plan_source and plan_author are optional audit provenance,
omitted in older rows and read as config/operator when absent; they never override plan or plan_digest.
A2 is the dispatch record; it is written only for agent steps. A3 is the only outcome record. A4
correlates a receipt with the attempt that produced it.

No session/ended job key is written. No job status column is written. No run table is written.

### 14.2 No new kind, table, route or crate

This contract introduces:

* no new event kind: A1-A4 are additive keys/fields on existing kinds;
* no new authoritative table: the run index (section 15.2) is a rebuildable read projection, not
  truth;
* no new HTTP route: the CLI reads the store;
* no new JSON-RPC method, SSE frame or environment variable;
* no new crate: the conductor is a driver decorator in the existing bundle.

It does add two metric names and one CLI verb group.

### 14.3 Metrics

| Metric | Labels | Increment |
|---|---|---|
| job_runs_total | outcome (succeeded, unverified, failed, cancelled) | once when a run verdict is committed |
| job_step_attempts_total | action (agent, deliver), outcome | once when a step verdict is committed |

Metrics are observability, not truth. They carry no job id or run id label, so cardinality is
bounded. A projection rebuild recomputes them from verdicts; it does not replay increments. A
dropped metric loses no fact.

## 15. Status and the CLI

### 15.1 The projection

Status is a pure fold over the run stream, plus an optional read-only runtime input for liveness
and `next_due_ms`. It is never a row. A stream-only fold cannot distinguish a live drain from a dead
unended claim, and it cannot derive an all-busy `next_due_ms` without the runtime catalog and
binding observation. The status surface therefore accepts that read-only input; runtime input is
observability, never execution authority.

RunView:

| Field | Meaning |
|---|---|
| run_id, job_id, slot | identity |
| status | pending, running, interrupted, succeeded, unverified, failed, cancelled |
| outcome | the run verdict's outcome, absent while unsettled |
| reason | the run verdict's reason, if any; the transient cancellation_pending note while a consumed cancel has no verdict |
| started_ms, ended_ms | first input timestamp, end timestamp if ended |
| plan_digest | the frozen plan digest |
| plan_source, plan_author | audit provenance from A1; absent when the row predates the field |
| status_basis | `runtime` when the read-only runtime input was supplied, else `stream_only` |
| steps | StepView[] |

StepView:

| Field | Meaning |
|---|---|
| id, action | identity |
| state | pending, ready, dispatched, settled, skipped |
| attempts | AttemptView[] |
| next_due_ms | retry backoff or the section 9.4 defer budget, if waiting |
| gate, checks | the latest settled attempt's verdict, if any |

AttemptView:

| Field | Meaning |
|---|---|
| attempt | attempt number |
| worker | exec_worker, absent for deliver |
| started_ms, ended_ms | first dispatch event for agent, delivery intent tool/call for deliver; absent when the attempt settled before writing either; ended_ms is the step verdict event |
| outcome | the step outcome |
| gate, checks | the verdict's gate and check results |

Derivation rules:

* a run verdict, if present, is the outcome. Status follows it: succeeded -> succeeded,
  unverified -> unverified, failed -> failed, cancelled -> cancelled.
* without a run verdict: a terminal end -> failed with ended_without_outcome; a live drain observed
  through runtime input -> running; a parked run with no in-flight attempt -> running; a resumable
  interrupted end -> interrupted; no drain and no end -> pending. A consumed cancel row without a
  verdict is running with a cancellation_pending reason; it settles only through the run verdict.
* without runtime input, an unended `drain/claimed` may be live or dead. It maps to `running` as a
  stream-only approximation with `status_basis:"stream_only"`; status must not present that as
  evidence of a live drain. Event age or timestamps are never used to guess liveness or death.
* `next_due_ms` derived from retry backoff remains available stream-only. The all-busy deferral
  bound requires runtime catalog/binding observation; without runtime input that value is omitted
  rather than guessed from event timestamps.
* a step verdict is the step's settled outcome. Without one, a dispatch record means dispatched; a
  deferral means ready with next_due_ms; a dependency skip, or an unattempted step of a run whose
  verdict is already committed, means skipped.
* status never infers success from session/ended.completed. It reads the verdict first (J18).

### 15.2 The run index

kian job list and kian job status <job> need to find runs by job. The serving layer keeps a
**rebuildable read index** keyed by job_id and run_id, derived from the A1 keys on admitted inputs.
The index stores run_id, job_id, slot, started_ms and the current status; it is a cache, not truth.

* losing the index loses only list/status convenience; a cold read scans the store's admitted rows
  and rebuilds it;
* the index is never consulted for execution, placement or settlement;
* the index is not a table of record and has no schema migration;
* a run whose A1 key is present but whose index row is stale is corrected by the run verdict on the
  next read.

A manual run is enumerable because its A1 key carries the manual slot. An alert run is enumerable
because its A1 key carries the alert trigger and slot. There is no hidden run registry.

### 15.3 CLI

~~~text
kian job list [--config PATH] [--json]
kian job status <run-id | job-id> [--json]
kian job run <job-id> [--slot TOKEN] [--config PATH]
kian job cancel <run-id> [--config PATH]
~~~

* list prints declared jobs and triggers, plus the last run per job from the index. A job with no
  run prints no last run, not a fabricated success.
* status <run-id> derives directly from the run stream. status <job-id> resolves the job's most
  recent run from the index; no run prints no_runs.
* run admits a manual run with slot manual:<26-character ULID>, or the explicit --slot token. The
  slot is the idempotency key: running the same slot twice returns the existing run, not a second.
* cancel admits a control row under kian:job:cancel:<run_id>. A duplicate is a no-op. A cancel for
  an already terminal run reports already_terminal and exits 0. A cancel for an unknown run is an
  error.
* --json emits the RunView/StepView/AttemptView shape above, with stable field names.

### 15.4 No HTTP route in v1

There is no new HTTP or JSON-RPC route in v1. The CLI opens the store directly, the way the
schedule CLI does. A read-only GET /jobs/<run> and an SSE stream are P2; they would serve the same
RunView, not a second status model. Adding them does not change any semantic in this document.

## 16. Failure and crash semantics

### 16.1 Failure table

| Condition | Durable facts | Outcome |
|---|---|---|
| no capable worker | step verdict failed, check no_candidate, no child | attempt failed; retry if budget, and each retry selects again; run fails only when the step is terminal |
| all capable workers busy | no verdict; status next_due_ms | defer; after defer_budget_ms the attempt fails no_candidate/all_busy; retry if budget; run fails when the step is terminal |
| pin unknown or ineligible | step verdict failed, check no_candidate | attempt failed; retry if budget; no child |
| spawn refuses binding_conflict | dispatch record, step verdict failed, check binding_conflict | attempt failed; retry if budget, preferring untried workers |
| input artifact missing, unreadable or over cap | step verdict failed, check artifact_unavailable, no child | attempt failed; step terminal, no retry |
| input artifact digest mismatch | step verdict failed, check artifact_digest_mismatch, no child | attempt failed; corrupt stream; step terminal, no retry |
| run identity conflict | none; the trigger refuses the admission | no run; never overwrite another session |
| child identity conflict | run verdict failed, reason invalid_job_input or child_identity_conflict, no child adopted | run failed; no overwrite |
| dispatch record committed, create fails | dispatch record | re-drive next tick; same ids; no duplicate |
| child ends failed | child end, step verdict failed | retry per effect gate and budget |
| child ends completed, checks pass | step verdict passed | succeeded |
| child ends completed, checks fail | step verdict failed | failed; retry only if the effect gate permits (a non-idempotent write with a result may have landed) |
| deadline | step verdicts timed_out or needs_operator, run verdict failed deadline_exceeded | run failed; children abandoned |
| child ends completed, verify [] accepted | step verdict unverified | unverified |
| child ends interrupted | child end, step verdict interrupted | retry for read/no effect; needs_operator for write with tool/call; run failed step_interrupted if budget exhausted |
| no terminal fact by timeout | step verdict timed_out | child abandoned; needs_operator for write with tool/call; retry for read |
| child stream unreadable | no verdict | status unknown; keep observing; timeout closes; no invented result |
| deliver not_started | error result | failed; retry if budget |
| deliver may_have_completed or in_flight | error result | needs_operator; never automatic |
| deliver receipt | receipt, ok result | succeeded if the receipt check passes; failed otherwise |
| receipt append fails | intent, no receipt | needs_operator; no success claim |
| conductor crash before verdict | committed facts | takeover re-derives; same ids |
| conductor crash after verdict before end | run verdict | next tick ends once; no second verdict |
| end without run verdict | terminal end | settlement failed ended_without_outcome |
| cancel consumed | run verdict cancelled, end cancelled | terminal; no further dispatch; outstanding attempts interrupted; children abandoned, not written |
| cancel after run verdict | none | no-op |
| alert channel down | alert run failed | source run unchanged; no chain |
| plan edited | frozen plan | no effect on the live run |
| job removed | frozen plan | trigger stops; in-flight run finishes |
| clock jumps back | cursor and event timestamps | slots already consumed stay consumed; the deadline is measured from the run input's event timestamp |
| second job-marked input | run verdict failed invalid_job_input | no plan mutation |

### 16.2 Crash windows

| Window | What is durable | Recovery |
|---|---|---|
| W1 after run input admitted, before drain | input row with job key | ordinary wake claims the run |
| W2 after claim, before dispatch | claim + input | takeover derives the same plan and dispatches |
| W3 after dispatch record, before create | thread/started with derived ids | re-drive creates the same child; admit is keyed |
| W4 after create, before admit | child session | admit the task under thread:<child_thread_id> |
| W5 after admit, before wake | task row | wake is advisory; the inbox rebuild re-derives the wake |
| W6 mid-child | child events | observe; settle when terminal or timed out |
| W7 after deliver intent, before send | tool/call | intent with no receipt -> needs_operator |
| W8 after send, before receipt | adapter side effect only | no receipt -> needs_operator; never auto-retry |
| W9 after receipt, before ok result | receipt | repair the ok result; settle succeeded |
| W10 after run verdict, before end | run verdict | next tick ends once with the verdict's outcome |
| W11 after end, before sweep | end + run verdict | sweep reads the verdict; no inference from end |
| W12 after cancel control, before tick | control row | next tick consumes it; cancel wins |
| W13 after cancel verdict, before end | run verdict cancelled | next tick ends cancelled; no resume |

### 16.3 Properties

* Every write is derived from committed facts, keyed, or fenced. No write depends on process memory
  or on a live tail.
* A child fact that arrives after its attempt settled is never folded back into the run (J12).
* An unknown effect is never retried automatically (J9, section 10).
* A terminal end without a verdict is failed, never succeeded (J18).
* A cancelled run is terminal in both the job and session senses (J17).

## 17. Footprint, tests, phases, rollback

### 17.1 The ladder

The capability ladder is config, then plugin/bundle, then seam, then core primitive. A job is almost
entirely config and bundle:

| Rung | What a job adds |
|---|---|
| config | the superagent.jobs section, triggers lowered into the existing schedule vector, validation |
| bundle | the conductor decorator, plan/step types, readiness, placement, verdicts, artifacts, delivery, status, CLI |
| seam | a cancellation seam, channel idempotency and artifact upload are the three deferred seams; v1 does not take them |
| core primitive | nothing. A job table, a Run entity or a dispatcher daemon would be pre-event state and would contradict J1 |

No core crate gains code. The run is a session; the step child is a session; the verdict is an
evidence event; the receipt is a channel event. The only new object is the declarative plan.

### 17.2 Deferred seams and designed-not-v1

Designed, not v1:

* binary artifacts (P3): require an upload seam and a blob root;
* enforced step timeouts and child cancellation (P3): require a cancellation seam;
* per-step cost and token accounting (P3);
* event and parent triggers (P4);
* max_parallel > 1 and cross-run dependencies (P4);
* HTTP GET /jobs/<run>, JSON-RPC and SSE status (P2);
* structured-output validation and report_matches_schema (P2);
* inline and Router-authored plan admission (P2): the frozen plan remains A1 authority; a template
  is a declared plan with no trigger and needs no separate admission surface;
* resume of a needs_operator run with an explicit --assume-no-effect (P2);
* channel idempotency keys for a retry after an unknown send (P2).

### 17.3 Test matrix

Tests are organized by what they can prove, not by file. Every acceptance row is observable as a
state -> behavior pair.

| Level | Test | Acceptance |
|---|---|---|
| L1 | plan canonicalization and digest | two key orders produce one digest; defaults are in the digest; a changed default changes it |
| L1 | validation V1-V33 | each rule refuses the whole config with a path-qualified reason; V23 refuses an agent input over the inline cap; V24-V26 refuse oversized plans; V30 refuses a P2 policy in v1; V31 refuses a colliding or malformed alert; V32 refuses the retired publish action for a new plan; V33 refuses a publish step that is not one agent step with one input and retries 0 |
| L1 | step and run lattices | every settled combination maps to exactly one outcome; interrupted never becomes succeeded; unverified is never succeeded |
| L1 | status fold | a run with no verdict and a live drain is running; an end without a verdict is failed ended_without_outcome; a verdict wins over the end reason |
| L1 | identity | all ids are uuid5-stable; a second derivation produces the same child, task and key |
| L1 | driver boundary | a job-marked stream selects only the conductor; a Router session and a thread child are not claimed; a Router submit creates a run whose only driver is the conductor |
| L2 | dispatch and settlement | a fake child completes; one thread/started before create; one step verdict; no duplicate on re-drive |
| L2 | placement | busy defers with no verdict; after budget it fails no_candidate; a single worker can be retried only after the abandoned child releases, and while it is live the retry defers rather than creating a second binding; a stale busy view yields binding_conflict and a retry |
| L2 | artifacts | final message becomes a ref with sha256; over-cap fails; a crash between pair appends repairs without a duplicate; a digest mismatch fails |
| L2 | delivery | receipt then result; not_started retries; may_have_completed is needs_operator; receipt append failure is not success; crash after receipt repairs |
| L2 | cancel | cancel writes one run verdict cancelled, one end cancelled; no further dispatch; a late child fact does not change the run; duplicate cancel is a no-op |
| L2 | alerts | a failed run admits one alert under the key; the sweep admits it if the conductor dies; a second admission is a duplicate; no chain |
| L3 | end-to-end digest | a real server, worker and channel: gather -> compose -> deliver; status shows the verdict and the receipt; no duplicate delivery |
| L3 | no available worker | a plan requiring an absent capability fails with no child within the refusal, not a 300 s timeout |
| L3 | deadline | two sequential steps whose first consumes the deadline leave the second undispatched; outstanding attempts settle timed_out (or needs_operator for a write), the run fails deadline_exceeded, and no success is invented |
| L3 | rollback | deleting jobs stops triggers; an in-flight run finishes from its frozen plan; older readers ignore the additive keys |

A test that cannot run at L2/L3 is reported as unverified in the delivery report; it is not replaced
by an L1 assertion of the same name.

### 17.4 Phases

| Phase | Scope |
|---|---|
| P0 | the run carrier, one agent step, one deliver step, verdicts, status list/status, one alert |
| P1 | the DAG, artifacts, retries, deadlines, cancel, accept_unverified, the run index |
| P2 | structured-output validation, resume with an explicit assumption, HTTP/SSE status, channel idempotency, inline/Router-authored plan admission |
| P3 | binary artifacts, enforced cancellation and timeouts, cost accounting |
| P4 | event and parent triggers, max_parallel > 1, cross-run dependencies |

v1 is P0 plus P1. A declaration that asks for a later phase's policy refuses load rather than
silently degrading.

### 17.5 Rollback

The change is additive:

* delete the superagent.jobs section; triggers stop, in-flight runs continue from their frozen plans;
* no schema migration is needed; the event stream is unchanged in kind;
* older readers ignore the additive keys; a reader that does not know job sees an ordinary session;
* an operator can list in-flight runs and cancel them; a cancel is terminal;
* a run that was settled keeps its run verdict after the config is removed, because the verdict and
  the frozen plan are in the stream.

The rollback does not delete events. Deleting the run index is safe: it is a cache, and a cold read
rebuilds it.

## 18. Decisions and change protocol

### 18.1 Open questions, answered

| Question | Decision | Why |
|---|---|---|
| Is a capability an enum or an open string? | An open, validated, namespaced string. | An enum needs a recompile per capability and makes a new machine a release event. The grammar is lowercase start, then [a-z0-9._-], at most 64 bytes, at most 16 per worker. Governance is three rules: capabilities are properties of a machine, never of a person or a task; a dotted prefix names the domain; a plan may ship before the worker that satisfies it, with a load-time notice and a dispatch-time no_candidate. |
| Can the runtime verify that a worker honours a capability? | No. | Capability is eligibility, never authority. The runtime verifies effects after the fact, not privileges before. A lying worker fails its checks. |
| What is the artifact retention? | Text artifacts live as long as the event stream; no TTL. Binary artifacts are P3 with a digest and a TTL. | A text ref is a pointer into immutable bytes. A TTL on a ref would make a replay non-deterministic. |
| Timezone and DST? | The trigger clock owns them. The job inherits them by lowering into the schedule vector. A run's deadline is measured from the run input's event timestamp, so a wall-clock jump stretches a deadline instead of expiring a run. | The job contract should not have a second clock. |
| max_concurrent versus max_parallel? | max_concurrent is runs per trigger (schedule policy); max_parallel is steps in one run (plan policy). v1 has one run at a time per trigger and one step at a time per run. | They bound different resources; conflating them is the original config bug. |
| Can a capability forbid privilege escalation? | No. Selection can only choose a worker that already has the privilege. Deliver is not placed, so it cannot be used to escalate. | The selector is not a security boundary. |
| Are secrets allowed in a plan? | No. V18 refuses secret-shaped literals. A step that needs a secret uses the runtime's tool credential, not prompt text. | Plans are persisted and model-visible; a secret in a plan is a leak. |
| What does retries mean? | Extra attempts: total = retries + 1, at most 5, and bounded by the run deadline. | This makes retries: 0 mean "do not retry", which is what an operator expects. |
| Same-day idempotence? | The slot is the idempotency key. A manual run is a different slot. | (job, slot) names at most one run; a double tick, a restart and a catch-up converge. A person asking twice is two requests. |
| What happens to an in-flight run when its job is deleted? | The trigger stops; the run finishes from its frozen plan. | The plan is in the stream; deleting config cannot unmake a committed run. |
| Is the tick configurable? | No. 100 ms floor, 5 s ceiling, adaptive between. | A configurable tick invites a 1 ms busy loop and a 1 h cancellation latency. |
| Does a job need a second scheduler? | No. Triggers are lowered into the existing schedule vector. | A second cursor and a second sweep would be a second source of truth. |
| Must every request become a job plan? | No. Only a job run carries a plan; a chat, agent or harness session and a control/observation call do not. A Router may submit a run, but it does not drive it. | A plan is an execution fact for the job path, not the runtime's universal input form. |
| Can a Router drive a job run? | No. It may submit, observe and cancel; the conductor is the only driver. | One run, one driver (J19, J20); a second driver would be a second writer and a second authority. |
| Does a job need a Router? | No. A cron, a CLI call or an alert can admit a run with no Router process; later phases add webhook and event triggers. | The Router is an optional intent plane, not a dependency of the job runtime. |

### 18.2 Change protocol

This document owns job and step-placement semantics. A change follows these rules:

1. **A new requirement is a new value, not a new mechanism.** Add a trigger kind, a step action, a
   capability or a check. Do not add a second scheduler, a second spawn path, a second delivery path
   or a status column.
2. **A new mechanism needs a separate design.** If a requirement cannot be expressed as a value, it
   is a new contract, not an edit. The review must show the failure it prevents, the isolation rule
   it preserves, and the phase it lands in.
3. **The event surface is additive.** Add keys, never rewrite a committed event. A change that makes
   a replay produce different bytes is invalid.
4. **The run verdict stays the only outcome record.** Do not move the outcome onto session/ended, a
   status table or a metrics label.
5. **Cancel stays terminal.** Do not add a resume path to a cancelled run. A resumable run is
   interrupted, not cancelled.
6. **The honesty boundary stays explicit.** A timeout is observed, not enforced; an abandoned child
   is not killed; an unknown effect is needs_operator. A change that hides one of these is a
   regression, even if it makes the happy path smoother.
7. **The four axes stay isolated.** A new cross-axis channel must be a named function in section
   2.5, not an implicit read of live state.
8. **Every new config field is enforced or refused.** A knob the runtime cannot enforce does not
   load. A later-phase policy is refused, not ignored.
9. **The driver boundary stays single.** Do not give a run a second driver, make the Router a step or
   a co-driver, or let the conductor call the Router. A new control plane is a client of the run,
   not a layer inside it.

The contract is complete when a reader can answer, from this document alone:

* what a job is, and what a run is;
* how a run is started, frozen and driven;
* how a step is placed, dispatched and observed;
* what proves a step and a run;
* how artifacts and deliveries are handed off;
* what happens when a conductor, a child, a worker, a channel or a clock fails;
* why no fact is silently lost or silently declared successful.

That is the bar.

**Status: normative.** This document is a contract, not a progress report. It describes what the
runtime must do, not which phase has shipped. A phase table is in section 17.4; it is not a claim
about a particular build.
