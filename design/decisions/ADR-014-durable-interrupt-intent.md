# ADR-014: Durable interrupt intent — the `control/interrupt` event and the reserved control envelope

- **Status**: draft (C68 / kian-80; append-only — ADR-001…013 immutable)
- **Date**: 2026-11-07
- **Context**: Round-7 plan §2.2/§3 card C68. Until now an `interrupt` RPC flipped an in-memory
  flag on the draining replica: the intent died with the process (a crash between request and the
  next safe boundary silently dropped it) and never crossed replicas (an interrupt aimed at a
  session owned by another replica was a no-op `not_draining`). Kian's own vocabulary already has
  the answer — durable rows before wakes (constraint 6), the inbox as the only control-plane
  write surface (constraint 9), and events as the sole audit truth (constraint 1). Borrowed
  shape: CMA's `user.interrupt` as a *persisted* event, filtered through Kian's constraints.

## Decision

1. **The 21st event kind `control/interrupt` is the audit record of an interrupt intent.**
   Drain-activity, non-context-bearing: it never maps to a state transition of its own (the end
   write settles an interrupt — constraint 4) and never enters a model request or the
   context projection (constraint 3; golden-tested byte-identical context with and without it).
   Payload: `{"action":"interrupt","reason":string|null,"requested_by":string|null,
   "requested_at_ms":u64,"target_epoch":u64|null,"stale"?:true}` — `stale` is present only on
   the audit-only arm (additive key; live intents never carry it).
2. **The control plane admits intent, it does not append events (constraint 9 unchanged).**
   `Engine::interrupt` writes a reserved control-envelope inbox row —
   `{"kian":{"type":"control","action":"interrupt","reason":…,"requested_at_ms":…,
   "target_epoch":…}}`, an exact single-key row-level convention like the steer wrapper and the
   C32 spill envelope before it — *in addition to* the in-memory flag when a local drain handle
   exists. The row is idempotent per `interrupt:<session>:<target_epoch>`, so repeated RPCs in
   one episode dedupe to exactly one row. Idle/terminal sessions keep the exact
   `NotDraining`/`UnknownSession` behavior and admit nothing (invariant 5).
3. **Epoch targeting.** `target_epoch` is the epoch of the stream's last event at admit time —
   the episode the intent aims to stop (best-effort; `null` when the stream is momentarily
   unreadable, in which case the row always interrupts — the intent stands even when its target
   could not be named). Because a claim issues `max(claim, event epochs) + 1`, a row admitted
   while a drain is appending targets that drain's own epoch; a row admitted after a takeover's
   first append targets the takeover.
4. **Consumption honors intent at the next safe boundary.** The drain's input take recognizes
   the reserved envelope (`Consumption::Control`): it appends `control/interrupt`, binds the row
   via `mark_consumed` (admit-before-consume ordering preserved), and — for a current-epoch row —
   sets the *same* interrupt flag the RPC path uses, so `supervise` settles `interrupted`
   exactly as before. Control rows are never wrapped as model messages and never returned by the
   take (there is nothing for a loop to see). Before the end write the drain does one final
   control-only take — a current-epoch row that raced the settle flips the end write to
   `interrupted` (the honest **stop-before-done**; the completed-interrupt race is recorded in
   DESIGN §2.4). Regular pending inputs are deliberately left eligible for the resumer.
5. **Stale rule: the targeted episode must not already have settled.** A consumed row is
   *stale* when the stream carries a `session/ended` at its `target_epoch` — the episode the
   intent aimed at is over, so the row is consumed for audit with `"stale": true` and ends
   nothing. Every other row interrupts, and two cases make that the right call:
   - *same-epoch*: the consuming drain is the drain the intent targeted;
   - *takeover*: the targeted episode never settled — its owner died mid-episode and the
     taker is resuming that same unfinished work, which is precisely what the interrupt
     intended to stop.
   This reconciles the two acceptance behaviors the round-7 plan demands (crash → takeover
   settles `interrupted`; a late row against a settled epoch does not end the new episode) under
   one decidable predicate: *did the target episode settle?* — derived purely from the event
   stream, never from scattered state (constraint 1).
6. **Cross-replica is best-effort at the next boundary, and needs no wake.** A remote replica's
   row is just data in the shared inbox: the live owner consumes it at its next boundary take
   (every episode re-evaluates the inbox), a dead owner's successor reaches it via the takeover
   path (recovery sweep / startup inbox scan — both already exist). Firing a wake for an
   interrupt would add a transport to *stop* work; none is needed because every drain already
   polls the inbox at safe boundaries, and a session with no drain in flight has nothing to
   interrupt until the next admit wakes it anyway.
7. **Interrupt of a running-but-unowned session is now `requested`** (was `not_draining`): the
   projection is `running`/`recoverable`, no local drain holds it — the owner is another
   replica or dead — and the durable row genuinely will be honored. The wire response vocabulary
   is unchanged (`requested`/`not_draining`/`unknown_session`).

## Rules it keeps

- Constraint 1: intent, targeting and staleness are all derived from the event stream and the
  inbox — no new authoritative state; the kind's projection mapping is explicit `DrainActivity`.
- Constraint 3: the kind is non-context-bearing; request bytes, `derive_model_context` bytes and
  the 20 prior kinds' frozen serializations are golden-tested unchanged.
- Constraint 4: the end write is still the single atomic settle; `control/interrupt` never ends
  a session by itself.
- Constraint 6: the row is durable before the RPC returns; no wake depends on it.
- Constraint 9: the control plane writes only the inbox; only a drain appends the event.
- `mark_consumed` invariants, the envelope format of the other row-level conventions, lease-as-
  liveness, and the `EndReason` shape are untouched.

## Consequences

- An interrupt can no longer be lost to a crash between request and boundary: the row survives,
  and the takeover settles `interrupted` with the audit event in the stream.
- Operators see pending intents: `kian session inspect` renders a `pending control` line (and
  `counters.pending_control_inputs`) derived read-only from the inbox.
- The in-memory flag and the durable row coexist by design (the flag is the low-latency path,
  the row the durable one); consumption is idempotent per epoch, so the pair produces exactly
  one `control/interrupt` event per episode.
- A `null`-target row always interrupts — deliberate: an unnamed target must not silently
  downgrade the operator's intent.

## Rejected

- **The control plane appending `control/interrupt` directly** — violates constraint 9
  (execution appends; the control plane admits) and would let a non-owner write without an
  epoch, breaking the fencing story.
- **Comparing `target_epoch` against the consuming drain's claiming epoch** — every drain claim
  is `max+1`, so *every* row predating the current claim would compare "older" and the
  crash-takeover case (the acceptance's whole point) would silently downgrade to audit-only.
  The settled-episode predicate is the decidable form of "the intent arrived too late".
- **A wake on interrupt** — see decision 6; stopping work needs no transport.
- **A new table for intents** — the inbox already is the durable intent store; a second store
  would fork the truth (constraint 1).
- **Carrying the interrupt on `session/ended` only** — the intent must exist *before* the end
  write to influence it, and must survive the case where the end write never happens (crash).
