# ADR-048: Multi-replica resident rotation — derive, repair, migrate

- **Status**: accepted (owner directive 2026-11-20; append-only — ADR-001…047 immutable)
- **Date**: 2026-11-20
- **Related**: [ADR-040](ADR-040-runtime-resident-rotation.md) (the rotation itself),
  [ADR-038](ADR-038-memory-injection-for-internal-sessions.md) (the idempotent session bootstrap),
  [ADR-005](ADR-005-fencing-via-expectedseq.md) (leases/fencing), [design/DESIGN.md](../DESIGN.md) §4.6.

## Context

ADR-040 phases 0–2 landed rotation with an explicit deployment assumption: a **single serving
process** (or all replicas restart together). Its phase-2 status says so, because the live Router id
lives in two places at once:

1. **In the event stream** — the pinned uuid5 chain, where "live" is the first non-terminal link.
   This is the durable truth (constraint 1).
2. **In each replica's memory** — the `ResidentBinding` (ADR-040 phase 1), read by driver routing,
   the residency inbox, the spawner parent, worker completion reports, the subscription route, the
   tools, recall and the child-timeout root.

With N replicas serving one store, four things can go wrong:

- **Stale bindings**: after replica A closes the Router and binds the successor, replicas B..N keep
  the old id. Their routing sends the frozen session to the Router side (a terminal stream → the
  drain releases, no execution), and their admissions land on a frozen stream — durable rows that
  nothing will consume.
- **Concurrent closes**: two replicas can decide to rotate at the same tick. The close is already
  claim-fenced (the lease + epoch), so only one wins and the other gets `Busy` — that part is safe.
- **A crash between the close and the successor's bootstrap**: A wins the close, then dies before
  admitting the successor's memory index / handoff / judge prompt. The Router is frozen, the
  successor exists but has no prompt, and no replica knows to finish the job.
- **The repair's own race**: any replica finishing the job must not duplicate context (two
  bootstraps must collapse to one).

The fixes must not add durable state beside the stream (constraint 1) and must not require a
replica-to-replica channel.

## Decision

1. **Derive, don't broadcast.** The live resident id is a pure function of the pinned id and the
   event stream (`resolve_re_arm`: walk the deterministic uuid5 chain to the first non-terminal
   link). A replica whose local binding points at a **terminal** session re-derives from the store,
   stores the result in its binding, and logs the catch-up. The check is cheap: the ticker already
   rebuilds the local live stream to evaluate the threshold, so the *walk* runs only when that
   stream projects terminal (another replica rotated); otherwise one rebuild per tick — the cost the
   phase-2 planner already paid.
2. **The close stays claim-fenced.** `Engine::rotate_idle` is unchanged: lease + epoch decide the
   single winner; a loser observes the rotated end on its next tick and catches up. No leader
   election is introduced for an action the claim already serializes.
3. **The successor bootstrap is idempotent and re-runnable by any replica.** Every admit it makes
   carries a durable key: `memory-inject:<successor>` (the ADR-038 seam), `rotate:<predecessor>:<row>`
   (migrated rows), `rotate-handoff:<predecessor>` (the bounded handoff) and the re-arm judge-prompt
   key (`router:bootstrap:re-arm:<successor>`). A replica that finds the live session to be a
   successor with **no events** (its bootstrap never landed, or has not been consumed yet) re-runs
   the bootstrap; the keys collapse every duplicate, so a repeated run is a no-op.
4. **Straggler migration.** A replica that catches up migrates the rows on the retired predecessor
   (`rotate:<predecessor>:<row>` keys, control rows excluded) to the live successor. The originals
   stay on the frozen predecessor as the audit record — at-least-once, never lost. The same
   migration runs on the winner right after its close, so the two paths cover both the winner's own
   in-flight rows and a stale replica's later writes.
5. **Residual window, stated honestly.** A stale replica can admit to the frozen predecessor for up
   to one tick period (until its next tick catches up). Those rows are durable, never executed on
   the frozen stream, and are migrated when that replica catches up; after the catch-up its binding
   is the successor, so no further stale writes happen. A row admitted *after* the catching-up
   replica's migration scan but before its binding re-point is impossible (the scan runs after the
   store-and-swap in the same tick).

## Alternatives considered

1. **Broadcast/notify between replicas** (a channel, a shared cache): still races a restarted or
   partitioned replica, and duplicates the stream-derived truth. Rejected.
2. **Leader election for rotation**: a lease per deployment. It reduces wasted `Busy` attempts but
   the close's claim is already the serialization point; an elected leader that crashes still needs
   the repair path. Rejected as unnecessary state; the advisory claim is the gate.
3. **A durable rotation row** (the ADR-040 sketch's `control/rotate`): the chain already encodes the
   rotation durably (the predecessor's `session/ended{rotated}` + the uuid5 successor). A second
   record would be a projection beside the truth. Rejected.
4. **Refuse multi-replica deployments** (the status quo's assumption): the failure modes above are
   silent lost work, not a clean refusal — the honest choice is to fix them.

## Consequences

- Multi-replica rotation is safe within one tick period of the winner's close: other replicas
  re-point, the successor's bootstrap is completed exactly-once by the durable keys, and the stale
  writes are migrated. No new event kind, state edge, wire byte or config key.
- The per-tick derivation cost is bounded: one local rebuild; the chain walk only on a terminal
  catch-up (and the walk itself is the boot path's existing `resolve_re_arm`).
- The winner's post-close migration and the catchers' migration are the same durable operation, so
  they can be retried in any order.
- Boot-time rotation (phase 0) inherits the same safety: two replicas booting together race the
  close, the claim picks the winner, and the loser's boot walk (or its first tick) resolves the
  successor. ADR-040's single-serving-process assumption is **superseded** by this ADR; the
  remaining caveat is only the bounded window above.
- Not addressed here: a replica that is *partitioned from the store* cannot be helped (it is
  already outside the durable truth). Cron `{session}` targets naming a rotating Router are refused
  at load instead (ADR-040 open question 5; CRON-MODEL §3.3 rule 9).
