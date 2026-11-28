# ADR-083 — Terminal admission refuses a post-close straggler; rotation migration covers only rows admitted before the close

- Status: accepted (2026-11-29, terminal admission refusal landed)
- Related: ADR-040 (runtime resident rotation), ADR-048 (multi-replica resident rotation),
  ADR-076 (liveness is a probe)

## Context

ADR-048 decision 5 accepted a residual window: a replica whose binding still points at a retired
resident could admit a row to the frozen predecessor for up to one tick period. Those rows were
durable, were never executed on the frozen stream, and were migrated to the successor when that
replica caught up. The window was bounded and the rows were never lost, so the trade-off was
acceptable at the time.

The terminal-admission contract now says the opposite. Admission is linearized with the terminal
close under one per-session write transaction, and a NEW row for a session whose last event is
session/ended with reason completed, cancelled or rotated is refused session_terminal
(WORKER-MODEL W5, API-CONTRACT -32003). The reason is W5 itself: every accepted input row must
belong to a session that can still consume it, or admission must be refused. A row accepted onto a
frozen predecessor cannot be consumed there; migration is a compensation, not a consumption
guarantee. A replica that dies between the stale admit and the migration scan leaves a row that only
a later catch-up can recover, which is exactly the accepted-but-unconsumable state W5 forbids.

The two contracts cannot both hold. The terminal-admission contract wins because it removes the
lost-work class instead of compensating for it.

## Decision

1. **A post-close admission is refused, including on a rotated predecessor.** The store's terminal
   predicate includes EndReason::Rotated; admit returns SessionTerminal for a would-be new row. The
   already-committed idempotency-key path still deduplicates first, so a retry of a row admitted
   before the close returns the existing row.
2. **Rotation migration remains for rows admitted before the close.** A row that was accepted while
   the predecessor could still consume it is moved to the successor with its
   rotate:<predecessor>:<row> key; the original row stays on the frozen stream as the audit record.
   This is the same at-least-once, never-lost property ADR-048 decision 4 promised, now scoped to
   rows that were admissible when they were accepted.
3. **A stale replica's post-close write fails visibly.** The submit/steer/queue caller receives
   session_terminal (control error -32003) and can retry against the re-derived successor. It is not
   silently accepted into a stream that nothing will execute.
4. **The catch-up still happens.** A replica whose local binding points at a terminal session
   re-derives the live resident from the durable chain and re-points. The catch-up is not a
   migration authorization; it only repairs the routing binding.

## Consequences

- The ADR-048 residual window is closed. A stale replica no longer creates post-close rows, so the
  one-tick lost-work class no longer exists.
- The migration path is still required for rows accepted before the close, including a row admitted
  in the same tick as the close; it keeps the rotate key and the frozen original.
- A caller with a stale resident id observes a typed refusal instead of a successful submit with no
  execution. This is the intended honesty of the terminal-admission gate.
- EndReason::Rotated remains terminal for reads and projections, but is no longer a distinct
  admission case: the refusal is the generic terminal refusal.

## Alternatives rejected

- **Keep ADR-048's window open and exempt rotated from the terminal predicate.** Rejected: W5 would
  then have a third outcome (accepted, never consumable) in exactly the path this change exists to
  remove, and a client could not distinguish a durable row from a lost one.
- **Refuse post-close rows only at the public control plane and keep the store-level path open for
  internal replica writes.** Rejected: the stale replica's write is not distinguishable from any
  other admission at the store seam, and a caller-specific exception would put policy in the store.
- **Migrate synchronously on admission.** Rejected: the store cannot resolve the live successor
  (that is a bundle projection), and admission must not depend on a rotation walk.
