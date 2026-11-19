# ADR-039: A cron worktree firing is an external job, not a session turn

- **Status**: accepted (owner directive 2026-11-19; append-only — ADR-001…038 immutable)
- **Date**: 2026-11-19
- **Related**: [design/CRON-MODEL.md](../CRON-MODEL.md) §6.1, §9.1, §15; [ADR-034](ADR-034-cron-scheduler.md)
  (the scheduler itself); [ADR-010](ADR-010-dsh-session-mapping.md) (the dsh bridge a real turn
  would use).

## Context

CRON-MODEL §6.1 opened with "a firing is a normal turn; there is no cron-specific execution
engine", then described two arms that behave very differently:

- the **resident** arm admits the prompt as an ordinary inbox row — a real turn, with events,
  usage, recall and replay;
- the **worktree** arm creates a git worktree and spawns the harness as a child process, then
  captures its output and diff onto the firing record. It creates **no session and no events**.

The first sentence was therefore false for one of the two arms. The implementation (P1) made that
explicit: a worktree firing has no `session/created`, no `tool/call`, no `turn/end`, no usage
event, and nothing to replay — its whole truth is the `cron_firings` row. Review questions
surfaced the mismatch repeatedly ("is the firing auditable?", "why does `cron logs` not show
usage?"). The choice is a contract decision, not a bug fix.

Two directions were available:

1. make the worktree firing a **real session turn** (a session whose turn runs with `cwd` inside
   the worktree), which requires a per-session working directory through the dsh bridge or the
   worker protocol, plus the isolation guarantees the worktree exists to provide;
2. name it what it is — an **external job** — and state its guarantees separately.

## Decision

1. **The worktree arm is an external job.** Its durable record is the `cron_firings` row
   (`state`, `worktree_path`, `finished_at`, bounded `error`); it creates no session, no events and
   no usage. It is executed by the harness child the scheduler spawns, under the per-firing
   worktree, and it is **not replayable** from the event stream. The contract states this instead
   of claiming a turn.
2. **The resident arm remains an ordinary turn** — the prompt is an admitted input with the
   additive, non-context `cron` proof key and the `cron:<firing_id>` idempotency key, and the
   firing settles from the target session's end (CRON-MODEL §9.1).
3. **The scheduler never claims job semantics it does not have.** No model-context claim, no
   cache-stability claim, no recall/usage claim for the worktree arm; choosing `worktree:` is
   choosing a job, not a session. The audit answers "did it run, where, with what exit/error" from
   the row; the captured stdout/diff is bounded in-process detail and is not durable in v1.
4. **The naming is unified** in the contract and the code comments (`external job` for the
   worktree arm, `resident turn` for the other), so a later change to make it a turn is an explicit
   ADR superseding this one, never a silent reinterpretation.

## Alternatives considered

1. **Make it a real turn (option 1 above).** The principled end state — every firing replayable and
   visible in usage/recall — but it needs a per-session execution surface (`cwd`/worktree) that the
   dsh bridge and the worker protocol do not have, plus a cache/context story for a job whose
   prompt is usually one line. Deferred, not rejected: a future ADR can supersede this one.
2. **Drop the worktree arm.** Loses the isolation that is its entire point (a scheduled job must not
   mutate the operator's checkout, CRON-MODEL §6.5).
3. **Keep the ambiguous wording.** Rejected: a contract that says "normal turn" while the code
   writes no events is exactly the declared-vs-executed drift the audit is meant to remove.

## Consequences

- The contract is honest: the two arms' guarantees differ, and both are stated. `kian cron logs`
  is the job's read surface; the resident arm's session stream stays the turn's.
- The captured diff/stdout is currently in-process only (bounded, redacted before any future
  persistence). Durable detail is a follow-up that must carry a redaction decision (harness output
  may contain secrets) — a store migration, not a scheduler change.
- A future "worktree firing = turn" change must supply the per-session cwd/execution surface and
  supersede this ADR; it cannot be done by rewording.
