# ADR-034 — Cron scheduler: declarative schedules executed as dsh turns

- **Status**: accepted (owner gate 2026-11-18; P0/P1 landed — see "Built shape" below); supersedes
  the READ-MODEL §7 "cadence out of scope" statement.
- **Date**: 2026-11-08
- **Context**: [CRON-MODEL](../CRON-MODEL.md) §1–§12, [READ-MODEL](../READ-MODEL.md) §7,
  [ADR-004](ADR-004-cordis-inspired-plugin.md), [ADR-010](ADR-010-dsh-session-mapping.md),
  [ADR-027](ADR-027-resident-empty-batch.md), [ADR-030](ADR-030-exec-wait-bounds-and-budget-stop.md).

## Context

READ-MODEL §7 kept cadence out of scope: retention has no automatic loop, `kian archive run` is an
explicit operator pass. Scheduled work therefore falls to host crontab/launchd, which has no
durable notion of what fired, cannot see the resident server's memory and model-slot budget, and
runs turns outside the harness — no worktree, no diff, no firing record. CRON-MODEL now specifies
the alternative. This ADR records the shape actually built on this branch and the seams that remain.

## Decision

### D1 — `kian-cron`: a pure scheduling crate (no I/O, no clock)

- Five-field grammar `m h dom mon dow` with the §2.2 aliases; parsing and local matching live in
  `CronExpr::parse` / `matches_local`.
- `tz` is an IANA name per schedule, resolved at load (`parse_timezone`). Every fire time is
  computed in the schedule's own zone (`next_fire_after`, horizon-bounded), so one store serves
  schedules in different zones.
- DST (§2.4): a gap time does not fire (skip); an overlap fires **once**, on the pre-transition
  offset. No double fire, no missed fire.
- Misfire (§5.3): `catch_up = latest` (default) fires **at most once per resume**, for the latest
  missed slot; `skip` advances past the slot without firing. Lateness within `coalesce_ms` is a
  normal firing whose `fire_time` is the computed slot (not `now`); a longer gap is reported as
  skipped (`superseded` / `outside_coalesce` / `policy_skip`) with the missed count, bounded by
  `MAX_SKIPPED_SLOTS`. There is no unbounded replay.
- Every slot has a stable key: `firing_id = "{schedule_id}@{fire_time}"` (RFC 3339, UTC).

### D2 — Durable state: `cron_schedules` / `cron_firings`

- Two tables in both sqlite and postgres. `cron_schedules` holds expr, tz, the declaration digest,
  `cursor_fire_at`, `next_fire_at`, `enabled`. `cron_firings` is keyed by `firing_id`
  (`UNIQUE (schedule_id, fire_time)`) and carries state, `skip_reason`, `worktree_path`,
  `finished_at`, `error`.
- **Admit-then-advance.** The firing row is claimed first (`INSERT … ON CONFLICT DO NOTHING`); the
  cursor is advanced afterwards, guarded by "cursor not already at/past this slot" **and**
  `EXISTS (SELECT 1 FROM cron_firings WHERE firing_id = …)`. A crash between claim and advance
  leaves a recorded firing with an unadvanced cursor; the replay re-claims nothing.
- **Single-writer claim.** The primary-key conflict is the only arbiter — no lock, no lease. Two
  servers racing the same slot produce one firing row and one advance.

### D3 — Server tick loop

- Spawned by serve only when `superagent.cron` resolves non-empty. Empty section = nothing armed,
  no task, no cost.
- Fixed interval with `MissedTickBehavior::Delay`. The loop owns cadence, the panic guard and event
  emission; due-ness, catch-up and coalescing belong to `tick_once`.
- Each schedule is ticked under its own `catch_unwind` panic guard, so one malformed declaration
  cannot kill the ticker for the others; a caught panic emits `cron/failed`.

### D4 — One firing = one turn through the dsh harness

- Execution goes through the dsh harness (ADR-010) inside a fresh **detached** git worktree under
  `<repo>/.git/kian-worktrees` (§6.3) — inside the git dir, so the checkout's own index, HEAD and
  working tree are never touched and the trees are invisible to `git status`.
- Lifecycle create → run → capture → cleanup (§6.2): the diff is captured (capped) before cleanup;
  a failed firing may keep its worktree (`keep_on_failure`) until `retain_hours`.
- The turn sits behind a `TurnRunner` trait — `DshTurnRunner` in production, a scripted runner in
  tests (the ADR-004 static-trait-plugin stance: compile-time injection, no registry).

### D5 — Disk guards

- Preflight floor = `max(min_free_gib = 80 GiB, min_free_percent = 25% of the filesystem)`;
  deliberately high because cron turns build Rust and a `target/` tree is GiB-sized.
- Caps: `max_worktree_mib` per worktree, `total_cap_mb` across all cron worktrees, `max_output_kib`
  per stored stream/diff (§6.4). The total cap is enforced by GC **before** a create: oldest
  non-running worktree first, never one whose firing is running; GC rides the same tick, never a
  cron schedule of its own.
- Below the floor, over the cap after GC, or an unanswerable probe → the firing is skipped with
  outcome token `skipped_disk_pressure`, emits the structured skip event, and is not retried in that
  slot. Fail-closed on probe error, never a silent bypass.

### D6 — CLI surface

- `kian cron list`, `run-now <id>` (deliberate one-slot backfill), `logs --schedule <id> [--limit]`,
  `rm <id>`. The subcommands open the store themselves and auto-migrate a fresh one, so cron is
  inspectable without a running server.

## Consequences

- **Supersedes READ-MODEL §7 "cadence out of scope".** Cadence now exists in-process; the archive
  retention pass remains explicit, because GC here is a size guard, not a retention policy.
- `catch_up = latest` means a laptop that slept a day runs **one** firing, not N: bounded model
  spend, and the firing row records the missed count so the gap is visible rather than silent.
- Skipped rows are written, not omitted, so "why didn't it run" (§9.3) is answerable from
  `cron_firings` alone — including disk pressure and overlap skips.
- Because a firing is an ordinary turn in the drain loop, ADR-030 wait bounds and budget stops apply
  unchanged; a firing is never retried inside its slot.
- The worktree cost is real: one checkout per firing, and a 80 GiB floor can refuse to run on a
  small disk by design.
- **Built shape (v1).** `kian serve` resolves `superagent.cron` and arms one 1s ticker on a
  dedicated OS thread; the ticker claims `cron_firings` rows, advances the cursor, and dispatches
  through `ScheduleDispatcher` — a resident arm that admits one durable inbox row (content
  `{content, cron}`, idempotency key `cron:<firing_id>`) and a worktree arm that cuts a detached
  git tree and runs the configured harness with `timeout_ms`. Disk guards (`min_free_gib` /
  `min_free_percent` / `max_worktree_mib` / `total_cap_mib` / `max_output_kib`) and `keep_on_failure`
  are enforced by `WorktreeDispatcher`. The CLI is `list` / `run-now` / `logs --schedule` / `rm`.
- **Designed, not v1 (seams, stated plainly).** No config-writing `kian cron add`; no
  per-firing `sandbox.*` knobs (tools are the target agent's, unchanged); no store-scoped
  schedules; `queue` / `kill-previous` overlap policies are parsed and validated but behave as
  `skip`; no `cron/*` event kinds and no cron metrics keys (the durable record is the
  `cron_firings` row plus one JSON line per decision on the ticker's stream); no `--why`
  diagnosis verb; the firing row does not yet persist `session_id` / `diff` / `missed` / `detail`;
  the resident arm does not track a running-set (the session's single-flight drain serializes
  overlapping slots).

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Host crontab / launchd invoking `kian` | No durable firing record, no shared slot budget, unauthenticated external entry, and the host's own scheduler has no catch-up or disk guard. |
| Fire from the timer channel (ADR-027) directly | The timer channel is a per-endpoint liveness fixture, not a calendar; it has no expression, tz or catch-up semantics. |
| In-memory schedule state only | A restart silently re-fires or silently drops; D2 exists precisely so a crash between admit and advance is decidable. |
| Replay every missed slot (`catch_up = all`) | Unbounded spend after downtime, storms of identical turns; `run-now` covers the deliberate backfill case. |
| A daemon thread per schedule | N wakeups, N panics, no single writer; one tick loop with a per-schedule panic guard keeps the writer and the failure domain single. |
| Run in the live checkout | A scheduled agent editing the operator's tree is unacceptable; a detached worktree makes the diff the artefact and the cleanup mechanical. |

## Non-goals

- No host-scheduler integration and no `crontab` generation.
- No per-slot retry, no unbounded backfill, no second-granularity expressions.
- Cron is not a liveness mechanism (§7.2); the watchdog stays authoritative.
- No cross-machine schedule ownership: one store, one writer.

## References

- [CRON-MODEL](../CRON-MODEL.md) — §2 (grammar, aliases, tz, DST), §4 (cursor, single writer,
  idempotency), §5 (overlap, coalescing, catch-up), §6 (dsh path, worktree lifecycle, disk guards),
  §7.3–§7.4 (tick loop placement, no double-fire across restarts), §9.3, §10 (CLI), §12 (migration).
- [ADR-004](ADR-004-cordis-inspired-plugin.md) — static trait plugins; the `TurnRunner` and
  `CronTickStore` seams.
- [ADR-010](ADR-010-dsh-session-mapping.md) — the dsh mapping contract a firing's turn travels.
- [ADR-027](ADR-027-resident-empty-batch.md) — a tick that finds nothing due does not call the
  provider.
- [ADR-030](ADR-030-exec-wait-bounds-and-budget-stop.md) — per-phase wait bounds; a firing is an
  ordinary turn and inherits them.
