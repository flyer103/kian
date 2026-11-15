# ADR-033: The durable confirmation plane — `ask` as a deferred tool call, a fifth end reason, and the answer as an admitted control row

- **Status**: proposed (owner gate pending; append-only — ADR-001…032 immutable)
- **Date**: 2026-11-15
- **Related**: [design/CONFIRMATION-MODEL.md](../CONFIRMATION-MODEL.md) (normative semantics),
  [ADR-015](ADR-015-tool-permission-provenance.md) (§7: `ask` out of scope, "deferred, not
  forgotten"), [ADR-014](ADR-014-durable-interrupt-intent.md) (the reserved control-envelope
  precedent), [ADR-028](ADR-028-harness-hook-plugins.md) (D13: the hook verdict vocabulary stays
  closed at `deny`), `crates/kian-superagent/src/stage.rs` (the `awaiting-answer` stage is read-side
  only; an ended ticket re-enters fresh Eval), `docs/SPEC.md` §5, `the design notes` §8.

## Context

ADR-015 §7 recorded `ask` as out of scope and named exactly three missing pieces: *a durable
pending-confirmation state, a human/control-plane response path into the inbox, and hold semantics
for a drain paused mid-turn*. ADR-028 D13 kept the hook verdict vocabulary closed at `deny` for the
same reason ("a drain has no attached human"). The superagent bundle's `awaiting-answer` stage is a
read-side projection over a worker that already ended, and its v1 deviation is explicit: an idle
ticket resumes only via fresh Eval, never same-session resume.

The external input (Anthropic, "Building always-on agents", 2026-11-13) supplies the missing
mechanism, not a new primitive: "when a call is deferred, the run **ends** and the session is saved
**mid-call**; the answer comes back as an inbox event; a new run resumes the saved session." That is
kian's own grammar — drain ends, session is the durable entity, answer is an admitted row — with one
hole: kian has no honest *resumable* end that means "waiting for a person". The four existing end
reasons are completed/cancelled (terminal), interrupted (an owner stopped us), failed (a failure).

Two shapes were available and only one is kian-native.

## Decision

1. **An `ask` is a deferred tool call, not a hold and not a denial.** `tool/call` gains
   `permission.evaluated:"ask"` (a widened closed value, the ADR-028 widening precedent). The ask
   arm never invokes the tool and appends no `tool/result`; the drain ends.
2. **The pending set is a projection, never a store.** `kian-core::confirmation::pending_calls`
   folds the stream: ask calls without an answering result, in order. It reads events (the
   provenance key is non-context), is gated by the cheap necessary condition "the last end reason is
   `awaiting_input`", and survives restarts by construction. No pending table, no inbox status
   column, no in-memory wait. A `compact` range may not cut an unpaired ask.
3. **A fifth end reason `awaiting_input`** (widened `session/ended.reason`) projects
   `running → idle`: the session is resumable, holds no lease, and is never reported completed while
   a call is pending. It is a widening of a closed value set, not an envelope shape change — no
   schema version bump; goldens for the new value plus frozen goldens for the four existing ones.
   `interrupted` and `failed` are deliberately not reused (honesty rule: interrupted ≠ failed ≠
   waiting).
4. **The answer is an admitted control row, never a direct event write.** A new control-plane method
   `confirm` writes the reserved control envelope (`{"kian":{"type":"control","action":"confirm",…}}`,
   the ADR-014 pattern) and wakes; the owning drain consumes it into the **23rd kind
   `control/confirm`** (non-context, `DrainActivity`) and then applies it: approve → invoke the
   frozen call and append the real `tool/result`; reject → the ADR-015 honest-error shape with
   `policy:"ask"`. `control/confirm` exists because the decision must be durable *before* the
   effect — the same reason `control/interrupt` exists — and no existing kind carries an answer.
5. **No provider request with an unanswered call.** The resume rule: consume answers, and only when
   the pending set is empty continue the turn. The answer appends at the tail, so the prefix is
   byte-identical — the wait costs no cache invalidation, which is the property the input's
   design implicitly needs and never states.
6. **Expiry is lazy and durable.** `confirmation.ttl_ms` (absent/0 = no expiry): a late answer is
   admitted as a reject-with-`expired`, the RPC says `expired`, the session resumes and the model
   sees the honest error. No timer in phase 1; an abandoned wait is a visible idle session, never a
   held lease.
7. **Phase-1 boundary is fail-closed.** `ask` refuses at boot for agents driven by the superagent
   worker loop or a resident loop (no waiting path yet, and ending a worker `awaiting_input` would
   strand its ticket). Hook `ask`, channel answering, automatic expiry, and the worker waiting path
   are phase 2/3/4 (CONFIRMATION-MODEL §14).
8. **The loop seam is untouched.** The engine derives the end reason from the stream; `LoopDriver`
   and `LoopOutcome` are unchanged in phase 1.

## Rejected alternatives

- **Hold the lease while waiting** (ADR-015's "hold semantics"). It wedges the session on the
  resource the feature exists to free, needs a multi-hour heartbeat, turns a dead confirmation
  channel into a permanent `running` session, and its answer would have to become a *new* safe
  boundary kind. Rejected.
- **Reuse `interrupted`** (running→idle for free, no schema question). It launders waiting into the
  interrupt metric and contradicts the honesty rule; the end reason is the only durable statement of
  why a drain stopped.
- **A durable pending-state table or a `deferred` flag on the inbox row.** A second truth beside the
  stream; exactly the input's four-store failure mode. Rejected by constraint 1.
- **A new kind `tool/deferred` beside `tool/call`.** The call already carries the decision; two
  events for one fact. `ask` is provenance on the call, not a second action.
- **A schema version bump for the end reason.** It would force every deployment to upgrade before
  any session may ask, for no byte-shape change; the additions-only discipline already accepts the
  same forward-compatibility posture for new kinds. Recorded as the explicit cost: an old binary
  must not read a stream that contains the new value.
- **Make the answer an ordinary message and let the model re-issue the call.** The provider would
  see an unanswered tool call (invalid), and approval of a *specific frozen* action becomes an
  ambiguous re-request.

## Consequences

- One new kind (22 → 23), two widened closed value sets, two control-plane methods, one CLI pair,
  one config key, two additive payload keys. No new session state, no new crate, no seam change.
- The superagent `awaiting-answer` stage stays a read-side projection in phase 1; phase 2 makes it
  resumable for real, which is the first time kian can answer "what is it waiting for" *and* act on
  it.
- The honesty surface grows: `kian tickets`/`pending` can now show a session that is blocked on a
  person without inventing a state.
- Cost: an out-of-date binary reading an `awaiting_input` stream fails to deserialize (recorded
  above); a tool approved after an at-least-once crash may run twice (the existing built-in-tool
  class, not a new guarantee).
- Open questions stay in CONFIRMATION-MODEL §15 (snapshot carriage, `pending` shape, batch UX,
  worker waiting path).
