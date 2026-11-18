# Kian — Cron Model (normative)

> **This document is the normative contract for wall-clock-scheduled kian turns.** It defines
> cron expressions, timezones, durable schedule cursors, misfire/catch-up semantics, overlap
> policy, execution through the dsh harness with per-firing git worktrees, the CLI surface, and
> the observability that answers *why didn't it run*. It never defines truth: a firing is an
> ordinary admitted turn on an ordinary session, and its record is an ordinary event row
> (`EVENT-MODEL.md`). Execution mechanics are in `DESIGN.md`; session/worker lifecycle is in
> `WORKER-MODEL.md`.
>
> Status: **proposed** ([ADR-033](decisions/ADR-033-cron-scheduler.md), draft, 2026-11-08;
> round 10). This document **supersedes** the `READ-MODEL.md` §7 "Cadence" scope statement that
> keeps cron cadence out of scope: retention cadence, schedule cadence and every other
> wall-clock cadence are owned here. **P0** — config shape, next-fire math, durable cursor,
> `kian cron add|list|rm`, skip/queue overlap, restart-safe single-fire. **P1** — worktree-backed
> dsh firings, catch-up coalescing, `cron logs`, `cron run-now`. **P2** — store-scoped schedules,
> per-schedule slot overrides, GC eviction under disk pressure.

## 0. Scope and relationship to the other contracts

This contract owns: the expression grammar, aliases and timezone semantics (§2); next-fire
computation including DST gap/overlap (§2.4); the declaration shape and its validation (§3);
the durable cursor, single-writer and idempotency rules (§4); overlap, misfire/catch-up,
concurrency and timeout (§5); the worktree lifecycle and disk cap (§6); how the scheduler sits
beside the timer channel, the watchdog and the resident server loop (§7); authorization, sandbox
and audit (§8); firing records and the *why didn't it run* path (§9); the `kian cron` CLI (§10);
the Footprint Ladder rung (§11) and the migration path (§12).

It does not own: event envelope, kinds, payloads, change protocol — `EVENT-MODEL.md`; method
names, error codes, SSE frames, metrics keys — `API-CONTRACT.md`; drain/recovery/execution —
`DESIGN.md`; session admission, worker templates, delegation and the board — `WORKER-MODEL.md`,
`MULTIAGENT-MODEL.md`; hook points — `PLUGIN-SPEC.md`; read versions, snapshots and
retention/archive mechanics — `READ-MODEL.md` (this contract only decides *when* an operator
pass is triggered).

The decision record is [ADR-033](decisions/ADR-033-cron-scheduler.md). The dsh bridge it
executes through is [ADR-010](decisions/ADR-010-dsh-harness-bridge.md); the resident-server and
wait-bound rules it must respect are [ADR-027](decisions/ADR-027-resident-empty-batch.md) and
[ADR-030](decisions/ADR-030-exec-wait-bounds-and-budget-stop.md).

## 1. Terms

- **Schedule**: one declared recurrence — expression, timezone, target, payload, policies.
  Identified by `id`.
- **Firing**: one instantiation of a schedule at a computed fire time. A firing that produced
  an admitted turn has a `firing_id`.
- **Fire time**: the logical instant `T` a firing is *due*, computed from the expression and
  timezone — never the wall-clock instant the runtime noticed it.
- **Slot**: the pair `(schedule_id, fire_time)`, the unique logical key of a firing.
- **Cursor**: the durable `(last_fired_at, next_fire_at)` pair for a schedule (§4.1).
- **Overlap**: two firings of one schedule attempting to run concurrently.
- **Misfire**: a fire time whose due-ness was discovered late — after downtime, laptop sleep or
  a stalled tick. Governed by the catch-up policy (§5.3).
- **Coalesce window**: the bounded lateness (`coalesce_ms`) inside which a missed fire time is
  still honored, as a single *fire-latest-only* firing.
- **Worktree**: the per-firing git worktree a dsh turn executes in (§6).
- **Scheduler**: the tick loop owned by the resident server (§7.3) — not a separate process.

## 2. Schedule model

### 2.1 Expression grammar

Five fields, whitespace-separated, classic order:

```
┌──────── minute        0-59
│ ┌────── hour          0-23
│ │ ┌──── day of month  1-31
│ │ │ ┌── month         1-12
│ │ │ │ ┌ day of week   0-6 (0 = Sunday; 7 also = Sunday)
* * * * *
```

Each field accepts `*`, a literal, a comma list (`1,15,30`), a range (`1-5`) and the forms
`*/n`, `a-b/n`, `a/n`. Month and day-of-week also accept three-letter English names
(`JAN`-`DEC`, `SUN`-`SAT`), case-insensitive. **Not** in v1: a seconds field, `L`/`W`/`#`,
`@reboot`, sub-minute intervals. A malformed expression is a config error at load — fail-closed,
never a silent skip (§3.3).

**Day-of-month / day-of-week union rule.** When both fields are restricted (neither is `*`), the
day matches if *either* matches — the classic vixie-cron rule. When only one is restricted, only
that one constrains. This is the single most common cron surprise and is therefore normative
here rather than inherited.

### 2.2 Aliases

| Alias | Equivalent | Note |
|---|---|---|
| `@hourly` | `0 * * * *` | minute 0 |
| `@daily` | `0 0 * * *` | midnight, calendar day boundary (DST: §2.4) |
| `@weekly` | `0 0 * * 0` | midnight Sunday |
| `@monthly` | `0 0 1 * *` | one fire per month |
| `@yearly` | `0 0 1 1 *` | one fire per year |

Aliases are sugar: the loader rewrites them to the five-field form *before* hashing, so `@daily`
and `0 0 * * *` are the same schedule and cannot both be declared under one `id`.

### 2.3 Timezone

Every schedule carries an explicit **IANA timezone** (`tz:`), e.g. `Europe/Berlin`,
`America/Los_Angeles`, `UTC`. When `tz:` is omitted the default is the **host timezone**, resolved
once at load (`/etc/localtime` symlink or `TZ`) and then frozen into the schedule's effective
config and echoed by `kian cron list` — a later host timezone change does **not** silently move
existing schedules. A `tz` that does not resolve in the bundled tz database is a config error,
not a fallback to UTC. Fire times are computed in the schedule's tz and persisted as UTC RFC 3339
with an explicit offset (`2026-11-08T09:00:00Z`).

### 2.4 DST gap and overlap semantics

A local fire time can be nonexistent (spring-forward gap) or duplicated (fall-back overlap).
The rules are fixed per schedule and not configurable in v1:

| Case | Semantics |
|---|---|
| **Gap** (`02:30` does not exist, e.g. `America/New_York` spring-forward) | **Skip.** No firing for that local time; the next fire time is the next expression match in the shifted local calendar. Never shift forward to `03:00` — that runs at an hour the operator did not ask for. |
| **Overlap** (`01:30` occurs twice on fall-back) | **Run once**, at the *first* (pre-transition) occurrence. The second is suppressed by the slot key: both occurrences resolve to the same local wall time under the expression, and that slot has already fired. |
| Wildcard hour/minute | Each local time is evaluated independently; only matched instants are deduped, so an hourly schedule loses exactly the missing hours in a gap and does not double-fire in an overlap. |

Evaluation is: convert `next_after(cursor)` to local; instantiate candidate local times; map each
back to UTC; drop candidates that do not round-trip (gap); deduplicate UTC instants appearing
twice (overlap); take the earliest UTC instant strictly after the cursor.

### 2.5 Next-fire computation

`next_fire(expr, tz, after)` is a pure function — no I/O, no clock read, no store access — that
returns the earliest UTC instant strictly after `after` matching the expression in `tz`, subject
to §2.4. Required properties, each a test (§13): **monotonicity** (`next_fire(a2) >= next_fire(a1)`
for `a2 >= a1`); **boundary idempotence** (`next_fire(x - 1ns) == x` when `x` matches);
**termination** (bounded forward scan of at most 366 days, then a `no_fire_within_horizon` config
error — reachable only for valid-but-never-matching shapes like `0 0 30 2 *`). The scheduler
calls it at most once per tick per due schedule and never re-derives a fire time from the wall
clock during a firing (§4.3).

## 3. Declaration

### 3.1 Shape

Schedules live in a new `superagent.cron:` section, beside `channels:`, `watchdog:` and
`harness:`, with `#[serde(deny_unknown_fields)]` as everywhere else in `config.rs`.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `id` | `[a-z0-9][a-z0-9_-]{0,63}` | yes | — | Stable identity; cursor key and log namespace. |
| `expr` | string | yes | — | Five-field expression or alias (§2.1, §2.2). |
| `tz` | string | no | host tz | IANA zone (§2.3). |
| `enabled` | bool | no | `true` | Declared-but-off schedules are validated and listed, never fired. |
| `target` | table | yes | — | Where the firing's turn lands (§3.2). |
| `prompt` | string | yes | — | The turn's user message (§3.4 templates). |
| `overlap` | `skip`\|`queue`\|`kill-previous` | no | `skip` | §5.1. |
| `catch_up` | `skip`\|`latest` | no | `latest` | §5.3. |
| `coalesce_ms` | u64 | no | `900000` (15 min) | Lateness honored by `latest` (§5.3). |
| `max_concurrent` | u32 | no | `1` | Ceiling for this schedule (§5.4). |
| `timeout_ms` | u64 | no | `1800000` (30 min) | Per-firing ceiling (§5.5). |
| `sandbox` | table | no | inherit | Per-firing exec surface (§8.2). |
| `worktree` | table | no | inherit | Worktree policy (§6). |

### 3.2 Target

`target` is exactly one of `{session: <uuid>}` — fire into a fixed session (the watchdog-shaped
pattern) — or `{channel: <name>, topic: <topic>}` — fire as a channel event so existing
subscription rules decide the session (`route: router` spawns a worker per firing,
`route: {session: …}` lands in the resident one). A target naming an undeclared channel, or a
channel whose `type` is not `cron`, is a fail-closed config error — the same cross-reference
discipline `superagent.subscriptions[*].channel` already enforces.

### 3.3 Validation (all fail-closed at load)

1. `expr` parses and `next_fire(expr, tz, now)` exists within the §2.5 horizon.
2. `id` is unique across `superagent.cron` and does not collide with a channel name.
3. `target` resolves (§3.2); `worktree.repo` (if given) is a git repository.
4. `coalesce_ms > 0`, `max_concurrent >= 1`, `timeout_ms > 0`.
5. `overlap: kill-previous` with `max_concurrent > 1` is rejected as contradictory.
6. `sandbox.allow` / `exec_allowlist` entries outside the configured surface are errors, never
   silently dropped.
7. A schedule targeting the watchdog session (§7.2) is rejected: `cron_targets_watchdog`.

### 3.4 Example

```yaml
superagent:
  cron:
    night-audit:
      id: night-audit
      expr: "30 3 * * 1-5"          # 03:30 Mon-Fri
      tz: Europe/Berlin
      enabled: true
      target: { channel: cron-jobs, topic: audit }
      prompt: "Audit .state/ for orphaned worktrees and stale locks; open a PR if you change anything."
      overlap: skip
      catch_up: latest
      coalesce_ms: 900000
      max_concurrent: 1
      timeout_ms: 1800000
      sandbox:
        allow: ["fs_read", "exec"]
        exec_allowlist: ["git", "rg"]
        cpu_ms: 120000
        mem_mb: 2048
      worktree:
        repo: "~/code/kian"
        keep_on_failure: true
        disk_cap_mb: 5120
  channels:
    cron-jobs:
      type: cron
      cron:
        topic: audit       # the channel only carries firings; no cadence of its own
```

The channel declaration is required because every subscription must target a declared channel; a
`type: cron` channel is inert without a schedule, and a schedule with a `channel` target is inert
without a channel. Both directions are validated, exactly as `type: timer` is today.

## 4. Durable state

### 4.1 The cursor

One row per schedule in the same store as the rest of kian state (`storage.sqlite` —
`./.state/kian.db` in every example config):

| Column | Type | Notes |
|---|---|---|
| `schedule_id` | TEXT PK | `${id}` |
| `expr_hash` | TEXT | sha256 of normalized `expr` + `tz` + `target`; a change resets the cursor visibly (§12.3) |
| `last_fired_at` | TEXT NULL | UTC RFC 3339 of the last **committed** firing |
| `next_fire_at` | TEXT NULL | UTC RFC 3339; `NULL` before first computation |
| `last_firing_id` | TEXT NULL | idempotency key of the last firing (§4.3) |
| `last_outcome` | TEXT NULL | `ok`\|`failed`\|`timeout`\|`skipped_overlap`\|`skipped_misfire`\|`worktree_unavailable` |
| `updated_at` | TEXT | cursor write timestamp |

The cursor is derived state in the `READ-MODEL.md` sense — rebuildable in principle from firing
records (§9.1) — but persisted so a restart never re-derives a fire time from the wall clock.

### 4.2 Single-writer rule

Only the resident server holding the **drain epoch** may advance a cursor (`EVENT-MODEL.md` epoch
fencing — the same mutual exclusion that gates appends). A second `kian serve` on the same store
fails to acquire the epoch and therefore never ticks: it cannot double-fire. `kian cron run-now`
is the operator override: it takes the same epoch-scoped write lock for the one firing it admits,
and is refused with exit `3` when another process holds it.

### 4.3 Idempotency

The idempotency key of a firing is `firing_id = "{schedule_id}@{fire_time_utc}"`
(`night-audit@2026-11-09T02:30:00Z`). Inserting a firing record whose `firing_id` already exists
is a no-op that still counts as *already fired*: the scheduler advances the cursor and continues.
Order is therefore **admit, then advance** — the cursor is only ever behind, never ahead, so a
crash can re-attempt a slot but can neither skip nor duplicate one.

## 5. Firing semantics

### 5.1 Overlap policy

| Policy | Behaviour when the previous firing of this schedule is still running |
|---|---|
| `skip` (**default**) | No new firing. One record with `outcome: skipped_overlap` (§9.1) so the gap is observable. The cursor still advances — the slot is *consumed*, not deferred. |
| `queue` | The firing is parked in a bounded per-schedule queue (`max_concurrent` + 1); it starts when a slot frees. Queued firings older than `coalesce_ms` are dropped as `skipped_misfire`/`queue_starved`. |
| `kill-previous` | The running firing takes the ordinary interrupt path (A6/A9: interrupt ≠ failed), settles honestly, and the new firing starts. Legal only with `max_concurrent: 1`. |

`skip` is the default because a scheduled turn is usually an *inspection* that is either doing
useful work or stuck; overlapping two inspections of one repo produces duplicate PRs, the failure
mode operators punish most.

### 5.2 Due-ness and tick coalescing

Each tick (§7.3) reads `now` once, loads all `enabled` schedules and fires those with
`next_fire_at <= now`. Lateness `now - next_fire_at` decides plain versus misfire: `<=
coalesce_ms` is a normal firing whose `fire_time` is the computed `next_fire_at` (not `now`, so
the record stays reproducible); `> coalesce_ms` is a misfire (§5.3).

### 5.3 Misfire and catch-up

Downtime, laptop sleep and a blocked runtime all look alike: several fire times passed.

| `catch_up` | Behaviour |
|---|---|
| `latest` (**default**) | If `now - last_fire_time <= coalesce_ms`, fire **once** for the most recent missed slot (`fire_time` = that slot) and record the rest as `skipped_misfire`/`superseded`. If the missed window exceeds `coalesce_ms`, still fire **once at the next tick** for the latest slot and record the gap as `skipped_misfire`/`outside_coalesce` with the missed count. |
| `skip` | Never fire for a past slot. Advance the cursor to the first fire time after `now` and record `skipped_misfire`/`policy_skip` with `missed: n`. |

Either way, firings after a resume are **at most one per schedule** regardless of how many slots
were missed; the coalesce window is the only knob, and there is no unbounded replay in v1
(`kian cron run-now` is how an operator backfills one slot deliberately).

### 5.4 Concurrency

Per schedule, `max_concurrent` (default 1). Globally, `superagent.cron.max_concurrent` (default
4), keeping the resident server's memory and model-slot budget bounded. A firing that cannot
start because the global ceiling is reached behaves as an overlap for that schedule — `skip` by
default — so a storm of schedules at `0 0 * * *` degrades into fewer runs, never a thundering
herd. Firings share the drain loop and are ordinary turns, so ADR-030 wait bounds apply unchanged.

### 5.5 Timeout

`timeout_ms` bounds one firing end to end (create + turn + capture + cleanup). On expiry the
firing takes the ordinary interrupt path, settles as `timeout`, and — with
`worktree.keep_on_failure` — leaves its worktree for inspection (§6.4). A timeout is never
retried automatically within the same slot.

## 6. Execution via the dsh harness

### 6.1 Path

A firing is a normal turn; there is no cron-specific execution engine.

1. The scheduler admits the prompt as an input row (durable-before-wake, A7) to the target
   session, tagged `origin: {cron: {schedule_id, firing_id}}`.
2. The session's harness is whatever `superagent.harness.dsh` configures
   ([ADR-010](decisions/ADR-010-dsh-harness-bridge.md)); with no `dsh` section the firing runs on
   the ReAct loop and §6.2-§6.5 degrade to a no-worktree turn — recorded as such, never silently.
3. The dsh child is spawned with `cwd` = the per-firing worktree, inheriting the configured
   `bin`, `args`, `slot`, `provider`, `model`, `max_steps`.
4. On settlement the scheduler captures evidence (§6.2) and cleans up (§6.4).

### 6.2 Worktree lifecycle: create → run → capture → cleanup

| Phase | Action | Failure handling |
|---|---|---|
| **create** | `git worktree add --detach <root>/<slug> <base>`, `base` defaulting to `HEAD` of `worktree.repo`. | Any failure — not a repo, permission denied, ref lock, disk cap, `git` missing — is `worktree_unavailable`. The firing does not run and does not fall back to the primary checkout (§6.5). |
| **run** | The dsh turn executes with `cwd` inside the worktree. | A turn failure is an ordinary failed firing; the worktree is preserved per policy. |
| **capture** | `git status --porcelain`, `git diff --stat`, the full `git diff` and the tail of the dsh child's stderr are written to the firing record, diff capped at `worktree.max_diff_kb` (default 512, truncated with an explicit marker). | Capture errors are recorded, never fatal to a turn that already ran. |
| **cleanup** | delete the worktree `target/` directory (where `CARGO_TARGET_DIR` points, §6.4) and record the freed bytes on the firing, then `git worktree remove --force <path>` and `git worktree prune`. Kept when `keep_on_failure: true` and outcome ≠ `ok`, or on `--keep`. | Cleanup is unconditional (success and failure alike) and failure-tolerant: a cleanup error is a note on the firing, the directory is enqueued for GC (§6.4), and the tick never fails. |

### 6.3 Naming

Path: `<root>/<schedule_id>/<YYYYMMDDTHHMMSSZ>-<hash8>`, where `root` is `worktree.root`
(default `<repo>/.git/kian-worktrees`, so worktrees live in the git dir and are invisible to
`git status`) and `hash8` is the first 8 hex chars of sha256(`firing_id`). The worktree is
**detached**: cron never creates a named branch, never pushes, and never commits unless the
prompt's own tools do. Slugs are filesystem-safe because `id` is `[a-z0-9][a-z0-9_-]*` (§3.1).

### 6.4 Disk guards and GC

Cron must never fill the host disk. Five knobs in the cron config section, `superagent.cron.disk`,
all fail-closed at load (an explicit `0` refuses boot rather than quietly disabling a guard) and
all measured *before* a new worktree is written:

| knob | default | meaning |
| --- | --- | --- |
| `min_free_gib` | 80 | free-space preflight floor, GiB |
| `min_free_percent` | 25 | free-space preflight floor, percent of the filesystem |
| `max_worktree_mib` | 2048 | cap on a single worktree, MiB |
| `total_cap_mib` | 20480 | cap across all cron worktrees, MiB |
| `max_output_kib` | 256 | bound on each stored stdout/stderr/diff of a firing, KiB |

The floor the preflight enforces is `max(min_free_gib, min_free_percent% of the filesystem)`.
The defaults are deliberately high because cron turns build Rust: a `target/` tree is GiB-sized, so
max(80 GiB, 25% of the volume) is the floor that keeps one firing's build artefacts from filling it.
Before a create the scheduler probes free space on `worktree.repo`'s filesystem; below the floor it
does not create, records the firing as skipped with reason `disk_pressure` (outcome token
`skipped_disk_pressure`), emits the same structured skip event as `skipped_overlap`, and does not
retry in that slot. A probe that cannot answer is treated as `disk_pressure` — fail-closed, never a
silent bypass.

The total cap is enforced by GC before the create: the scheduler sums the worktrees under `root`,
evicts the oldest *non-running* worktree first until the incoming create fits, and if the total is
still over `total_cap_mib` after GC the firing is skipped with `disk_pressure` — nothing is created
and the slot is not retried. Preserved (`keep_on_failure`) worktrees are GC'd once older than
`worktree.retain_hours` (default 72), oldest first, never one whose firing is running. GC runs on
the same tick loop at the 15s floor (§7.3) — deliberately *not* a cron schedule, since bootstrapping
a scheduler with itself is a foot-gun.

The per-worktree cap (`max_worktree_mib`, default 2048) bounds one run: the harness runner polls
the worktree's size while the turn runs, and a run that exceeds the cap has its whole process group
killed; the firing is recorded `failed` with `worktree_too_large`, and the worktree is removed even
under `keep_on_failure` (a too-large tree is exactly what must not be preserved).

Captured output is bounded as well: each firing stores at most `max_output_kib` (default 256) of
stdout, of stderr and of `git diff`, and every clip appends an explicit marker to the firing's
truncation markers, naming the field and both sizes. An operator sees "truncated", never a log that
was silently cut.
### 6.5 When the worktree cannot be created

The firing is recorded as `worktree_unavailable` with the git stderr, the cursor advances (the
slot is consumed), and the firing is not retried in that slot: a broken repo is an operator
problem, and retrying every minute turns a visible config error into a log flood. `kian cron
run-now <id>` is the deliberate retry, and `kian cron list` shows the last failure verbatim.
There is no fallback to the operator's live checkout — a scheduled turn mutating the tree an
operator is editing is unacceptable, and isolation is the entire point of the worktree.

## 7. Integration

### 7.1 Relationship to the timer channel

|  | `type: timer` (unchanged) | cron |
|---|---|---|
| Trigger | monotonic interval from boot (`interval_ms`) | wall-clock fire time in a tz |
| Phase | drifts from boot; no wall-clock meaning | fixed phase; survives restart |
| Persistence | none — a restart restarts the cadence | cursor persisted (§4) |
| Use | liveness/progress pings, the watchdog | calendar-meaningful work (nightly audit, weekday sweeps) |

The timer channel stays byte-for-byte as documented in `config.rs` (`interval_ms`,
`emit_on_start`, `topic`, `event_id: "tick-<n>"`). Cron adds a `type: cron` channel plus
`superagent.cron`; both may be declared in one config and both route through the same
subscription rules. Cron and timer are complementary, not versions of each other.

### 7.2 Relationship to the watchdog

The watchdog (`superagent.watchdog:`) is timer-driven and answers *is this session progressing*.
Cron must not become a second watchdog, so a schedule targeting the watchdog session is rejected
at load (§3.3 rule 7): a wall-clock judge and an interval judge on one session would fight over
steering. The `WatchdogSource` seam (`kian-core/src/seams.rs`) is untouched — cron produces
sessions and turns, not `HealthEvent`s, and adds no seam.

### 7.3 Where the tick loop lives

- **Startup**: constructed by `kian serve` after the store is open and the epoch acquired, before
  the first channel adapter starts. It runs one resume pass (§7.4), then enters the loop.
- **Resolution**: 1s when any enabled schedule has `now - next_fire_at` within 120s, otherwise
  15s — minute-granularity accuracy without a permanent 1s wakeup.
- **Tick coalescing**: ticks are single-flight; a tick finding the previous still running is
  dropped, and each tick reads `now` once, so a stall yields one late decision rather than a
  burst of catch-up firings (§5.3).
- **Ownership**: the loop lives in the resident server beside the drain loop and the channel
  adapters — not a plugin, not a second binary. It exits with the server and never writes a turn
  outside the drain path.

### 7.4 Avoiding double-fire across restarts

Three independent, deliberately redundant guards: (1) **cursor ordering** — admit then advance
(§4.3), so the cursor is only ever behind and a crash cannot skip a slot; (2) **epoch** — only the
epoch holder ticks (§4.2), so a stale supervisor or second `serve` cannot race the primary;
(3) **firing-record uniqueness** — `firing_id` is a primary key, and a duplicate insert is
detected and treated as already-fired.

## 8. Security and permissions

**8.1 Who may declare.** Schedules are **operator config only** in v1: they live in the `kian
serve` config file, the same trust class as `agents:`, `providers:` and `tools:`. Agents and
plugins cannot create schedules at runtime — no tool, no method. `kian cron add` writes a
`superagent.cron.<id>` block (or, with `--store`, a store-scoped row) and requires the same
privileges as editing that file; it is still operator-authored. There is no authenticated
"schedule as a user" path in v1.

**8.2 Sandbox and command allowlist.** A scheduled payload is untrusted text executing
unattended, so it gets *less* surface than an interactive turn, never more: the firing's tools are
`agent.tools` ∩ `sandbox.allow` (intersection, not union — a schedule cannot widen its agent);
`sandbox.exec_allowlist` (argv[0] basenames, default `["git","rg"]`) filters `exec` for firings
only, which stays argv-direct with no shell under `tools.exec_timeout_ms`; the worktree is the
`fs_*` root and paths resolving outside it are refused; no new network permission is implied by
being scheduled.

**8.3 Resource caps.** `sandbox.cpu_ms` (default 120000/firing) and `sandbox.mem_mb` (default
2048) are dsh-child rlimits; `timeout_ms` is the wall-clock ceiling via the interrupt path (§5.5);
`worktree.disk_cap_mb` bounds disk (§6.4); §5.4 bounds concurrency.

**8.4 Audit.** Every firing writes `cron/firing_requested` *before* the turn is admitted and
`cron/firing_settled` after settlement — ordinary event rows on the target session — plus one
`INFO` `tracing` line carrying `schedule_id`, `firing_id`, `fire_time`, `outcome`,
`duration_ms`, `worktree`. The `origin: {cron: …}` tag on the admitted input distinguishes
scheduled from human input forever after and is never rewritten.

## 9. Observability

### 9.1 Firing records (`cron_firings`, one row per slot)

| Column | Notes |
|---|---|
| `firing_id` | PK, `{schedule_id}@{fire_time}` |
| `schedule_id`, `fire_time` | logical identity |
| `started_at`, `ended_at` | wall clock |
| `outcome` | `ok`\|`failed`\|`timeout`\|`interrupted`\|`skipped_overlap`\|`skipped_misfire`\|`worktree_unavailable`\|`session_unavailable` |
| `reason` | machine token: `outside_coalesce`, `policy_skip`, `queue_starved`, `disk_cap`, `max_concurrent_global`, … |
| `session_id`, `turn_id` | when a turn was admitted |
| `worktree_path`, `diff_stat`, `diff` | bounded (§6.2) |
| `missed` | slots superseded by this firing |
| `detail` | bounded stderr, credential-redacted |

### 9.2 Events and metrics

Event kinds: `cron/firing_requested`, `cron/firing_settled`, `cron/firing_skipped` (one per
skipped slot, carrying `reason`), plus `cron/cursor_reset` (§12.3). Metrics in the
`API-CONTRACT.md` namespace: `cron_firings_total{outcome}`, `cron_lateness_ms` (histogram),
`cron_fire_lag_ms` (due → admitted), `cron_worktrees{state="live|kept|gc"}`,
`cron_worktree_disk_bytes`. Every skip is counted: a scheduler that silently does nothing is
indistinguishable from a broken one, so no decision is invisible.

### 9.3 The "why didn't it run" path

`kian cron logs <id> --why` evaluates this fixed order and prints the first decisive answer —
the same data as `--json`:

1. **Not declared / disabled** — `enabled: false`, or the id missing from the effective config.
2. **Config rejected** — the load-time error (§3.3) with the offending field.
3. **Not the epoch holder** — this process is not the primary (§4.2).
4. **Misfire-suppressed** — the last due slot has a `skipped_misfire` record; shows `reason`
   (`outside_coalesce`, `policy_skip`, `queue_starved`) and `missed`.
5. **Overlap-skipped** — a still-running firing; shows its `firing_id`, `started_at` and elapsed
   time, the fact operators most often need.
6. **Blocked by the global ceiling** — lists concurrently running firings.
7. **Worktree unavailable** — the captured git stderr verbatim (§6.5).
8. **Session unavailable** — target session missing or terminal; shows its `end_reason`.
9. **Ran and failed** — the settlement record, `outcome`, and last turn error.

## 10. CLI surface

| Command | Flags | Behaviour |
|---|---|---|
| `kian cron add` | `--id --expr --tz --target-session`\|`--target-channel --topic --prompt`\|`--prompt-file --overlap --catch-up --coalesce --timeout --disabled --config` | Validates by computing a next fire, writes the YAML block, prints the diff applied. Refuses a duplicate id. |
| `kian cron list` | `--json --all` | Table below; `NEXT` rendered in the schedule's tz. |
| `kian cron rm` | `--id --keep-state` | Removes the declaration; history is always kept, the cursor row too unless `--keep-state` is given. |
| `kian cron run-now` | `--id --force --keep` | Admits one firing immediately with `fire_time = now` floored to the schedule granularity; `--force` ignores `enabled: false` and the overlap policy; `--keep` preserves the worktree. |
| `kian cron logs` | `--id --limit --json --why` | Firing records newest-first; `--why` is §9.3. |

```
$ kian cron list
ID            EXPR          TZ                NEXT (local)       LAST        OUTCOME           WORKTREE
night-audit   30 3 * * 1-5  Europe/Berlin     2026-11-09 03:30   11-08 03:30 ok                gc
weekly-report @weekly       UTC               2026-11-15 00:00   11-08 00:00 ok                gc
stale-locks   */15 * * * *  America/New_York  2026-11-08 09:45   11-08 09:30 skipped_overlap   gc
```

Exit codes: `0` success; `2` validation refusal (nothing written); `3` another process holds the
epoch (`run-now` only).

## 11. Footprint Ladder placement

The capability grows at the smallest rung that expresses it (`DESIGN.md` §6.3): **rung 1,
configuration** for the declaration and its policies (zero core footprint); **rung 2, plugin** for
the firing's transport into a session (`type: cron` is an ordinary ChannelAdapter; no new seam);
and **explicitly not rung 3** — no new trait, no widening of the waist. The scheduler is a loop
owned by the resident server, reading and writing only through the store and drain path that
already exist. The one addition outside those rungs is a derived store table (`cron_cursor`,
`cron_firings`) — the same class of addition as the read-model views: beside truth, not a change
to truth.

## 12. Compatibility and migration

**12.1 Timer keeps working.** `type: timer` is unchanged, covered by the existing `issue-watch`
examples, and not deprecated on any timeline in this document; nothing in `superagent.cron` alters
timer parsing, `emit_on_start` or `tick-<n>` ids.

**12.2 Absent section is a no-op.** Without `superagent.cron`, kian behaves exactly as before: no
table created lazily, no tick loop started, no worktree root created. Upgrade is a no-op for every
existing deployment.

**12.3 Changing a schedule.** Editing `expr`, `tz` or `target` changes `expr_hash`; on load with a
changed hash the scheduler keeps history but recomputes the cursor from `now` — never back-filling
the new expression over the old window — and writes one `cron/cursor_reset` event carrying both
hashes, so an edit is never indistinguishable from normal operation. Editing only `prompt`,
`sandbox`, `timeout_ms` or `overlap` does not reset the cursor.

**12.4 Deprecation path (not scheduled).** If timer and cron ever converge: cron becomes the only
scheduler, timer ships as a built-in `@every <ms>`-equivalent alias rewritten by
`kian config migrate`, one release of dual support, then removal at a major version. This
document does not trigger that path.

## 13. Test matrix

| Level | Case | Assertion |
|---|---|---|
| Unit (next-fire) | `*/15 * * * *` across an hour boundary | exact UTC instants; strictly monotonic |
| Unit (next-fire) | both DOM and DOW restricted (`0 0 1 * 1`) | union rule: the 1st *or* any Monday |
| Unit (next-fire) | spring gap, `30 2 * * *`, `America/New_York` | gap day skipped; next fire the following day |
| Unit (next-fire) | fall-back overlap, `30 1 * * *`, `America/New_York` | fires once at the pre-transition instant; second 01:30 suppressed |
| Unit (next-fire) | `0 0 30 2 *` | `no_fire_within_horizon`; bounded scan terminates |
| Unit (tz) | typo'd `tz`, zone absent from the database | hard config error; no UTC fallback |
| Unit (misfire) | clock jumps 3h, `catch_up: latest`, 15m window | exactly one firing (latest slot), `missed: 11`, `outside_coalesce` |
| Unit (misfire) | same with `catch_up: skip` | zero firings, one `skipped_misfire`/`policy_skip`, cursor past the gap |
| Integration (catch-up) | kill the server across two fire times, restart | resume pass fires at most once per schedule; lateness recorded |
| Integration (restart idempotency) | SIGKILL between admit and cursor advance | one turn, not two; duplicate insert detected; cursor repaired |
| Integration (single-writer) | two `kian serve` on one store | second never ticks; `run-now` refused, exit `3` |
| Integration (overlap) | slow firing × `skip`/`queue`/`kill-previous` | `skipped_overlap`; FIFO then start; interrupt then start |
| Integration (worktree) | success / failure / timeout / `keep_on_failure` / `--keep` | full lifecycle verified; kept trees survive, GC'd after `retain_hours` |
| Integration (worktree) | not a git dir; ref lock; `git` absent | `worktree_unavailable`, no run, no fallback, stderr captured |
| Integration (disk cap) | fill `disk_cap_mb` with kept worktrees | oldest non-running evicted first; cap never exceeded; refusal when nothing evictable |
| Integration (concurrency) | 8 schedules at one minute, global ceiling 4 | ≤4 concurrent; the rest recorded as overlap skips, not errors |
| Integration (sandbox) | prompt calling a non-allowlisted `exec` | refused honestly; interactive sessions unaffected |
| Integration (coexist) | cron + timer + watchdog in one config | all three fire; a cron target naming the watchdog session rejected at load |
| Integration (CLI) | `add` → `list` → `run-now` → `logs --why` → `rm` | round-trip; `--why` names the decisive reason |
| Regression | existing `examples/issue-watch` timer config | unchanged behaviour, no cron table created |

## 14. Open questions

1. **Store-scoped schedules.** v1 keeps schedules in operator config; `--store` rows are listed
   in §10 but underspecified — do they belong in v1, or only after an authority model for "who
   may schedule" exists (§8.1)?
2. **Per-schedule model slots.** A nightly audit wants a strong slot, a 15-minute poll a cheap
   one. `agents:` templates cover this today; a schedule-level `slot:` override may surprise less
   than a dedicated agent.
3. **Diff-driven escalation.** Should a firing that produces a non-empty diff notify the IM
   channel automatically, or is that always the prompt's job via `reply`?
4. **`@every` for sub-minute.** It would blur the timer/cron boundary; deliberately absent in v1 —
   confirm it stays absent.
5. **Worktree base.** `worktree.base` defaults to `HEAD`; should a schedule be able to declare
   `base: origin/main` plus a fetch-before-create policy, given unattended network calls?
6. **Clock authority.** v1 trusts the host clock (and its tz database) and treats a large
   backward jump as a no-op (already-passed fire times stay consumed). Should a backward jump
   instead rewind the cursor, or be surfaced as a misfire?

## 15. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Duplicate turns after a crash (idempotency bug) | duplicate PRs/comments; trust loss | `firing_id` PK plus admit-then-advance ordering (§4.3, §7.4); restart-idempotency test is a gate |
| Laptop sleep producing a burst on wake | thundering herd, cost spike | `catch_up: latest` + coalesce window + global ceiling (§5.3, §5.4) |
| Worktree disk growth on a busy repo | fills the operator's disk silently | `disk_cap_mb` with pre-create measurement and eviction; GC on the tick loop (§6.4) |
| DST bugs | firings at the wrong hour, twice or never | pure `next_fire` with dedicated DST tests; skip-in-gap / run-once-on-overlap fixed in v1 (§2.4) |
| Silent scheduler death (loop panic) | nothing runs, nothing complains | every skip is an event and a metric; `cron list` exposes stale `LAST`/`NEXT` (§9) |
| Unattended agent mutating the real checkout | operator data loss | mandatory worktree isolation, no fallback path (§6.5); `fs_*` confined to the worktree (§8.2) |
| Config sprawl (many schedules in one file) | unreadable config, mis-scoped schedules | fail-closed validation with precise messages; `cron list` is the single source of "what is armed" |
| Two schedulers racing after a failed failover | double fire | epoch-scoped writer (§4.2) plus unique `firing_id` |
