# ADR-055: A session-lifetime token budget that gates before the request, not after the bill

- **Status**: accepted (owner direction 2026-11-22; phase 1b slice).
- **Context**: `AgentDef.budgets` has existed since the first config surface — `{tokens,
  wall_clock_ms}` — but nothing consumed it. Cost was measurable (ADR-053 price book, `kian cost`,
  usage projections) and compression was possible, yet a session could still run a 20-step tool loop
  through its ceiling and report the overspend afterwards. Three properties were missing.
  (1) **Binding**: a ceiling that only appears in a report is not a ceiling; the decision must be
  made before the bytes fly, at a point where refusing costs nothing and is observable.
  (2) **Resumability**: an agent that hits its budget has not failed and has not completed — it is
  out of money. kian's honesty rule (interrupted ≠ failed) demands a distinct, resumable end, not a
  terminal `failed` that loses the work in flight. (3) **Raisability**: the operator who sees the
  park must be able to top up a *live* session through the same durable path as every other
  control-plane action (ADR-014/ADR-036), without touching execution and without a second
  scheduler. A fourth constraint shaped the design: the gate must not itself be a cost. A strict
  pre-send price estimate would need a second tokenizer and an exact price promise kian cannot keep
  (ADR-053 rejected a priced event for the same reason), so the gate uses the label-honest
  `chars_v1` estimate it already has.
- **Decision**:
  1. **The budget is session-lifetime and session-pinned.** `agents.<name>.budgets.tokens` is the
     base ceiling. When at least one limit is configured, the server pins
     `session/created.budgets = {tokens, wall_clock_ms}` (an additive BEGIN-only key) so replay and
     projections never consult today's config; an unconfigured agent emits `{}` exactly as before
     (byte-identical legacy). An explicitly pinned `null` is authoritative and means "no limit for
     this dimension", so a later config edit cannot retroactively change a live session.
  2. **The projection is `base + Σbudget/raised − Σspent`.** `kian-core::budget` folds
     `session/created.budgets` (or the agent declaration in the loop when the stream has none),
     every `budget/raised` and every `turn/end.usage`. Usage parsing recognizes the
     `prompt_tokens`/`input_tokens` and `completion_tokens`/`output_tokens` spellings; a turn
     whose usage object carries neither spends nothing (unknown is never assumed to be free or
     expensive — ADR-053's honesty rule, applied to the gate). The projection is derived on read:
     no budget counter is stored outside the event log.
  3. **The gate runs after the plan, before `step/request`.** Each provider turn builds its
     `RequestPlan` (ADR-054), projects the budget from the committed stream, and admits the
     attempt only when `spent + estimate.high + max_output_tokens` fits the remaining ceiling. A
     refusal writes no `step/request` and dials nothing: persist-what-you-send only covers bytes
     that were actually sent. The coarse high bound is deliberately conservative — a false park is
     resumable, an overspend is not.
  4. **An exhausted budget ends `awaiting_budget`, a resumable end.** The loop returns
     `LoopOutcome::AwaitingBudget`; the orchestrator closes the drain with the seventh end reason
     `awaiting_budget` (`running → idle`, never terminal) and records the projected numbers plus
     the refused estimate on `turn/end` (`outcome: "awaiting_budget"`). The in-flight work is not
     lost: the next raise and wake resumes the same session through the ordinary drain path.
  5. **A raise is a durable control row consumed into `budget/raised`.** `raise_budget` (RPC, CLI,
     SDK) validates "at least one positive amount", then admits ONE reserved
     `{"kian":{"type":"control","action":"budget",...}}` row under an idempotency key (caller key
     wins, else `budget:<session>:<tokens>:<wall_clock_ms>:<requested_at_ms>`) and wakes. The
     owning drain consumes it into a `budget/raised` event and binds the row; the control plane
     writes nothing else and never touches execution (constraint 9). A terminal session answers
     `terminal` and admits nothing. The event carries the operator's reason/identity, so a top-up
     is auditable.
  6. **Wall-clock is recorded, not enforced.** `wall_clock_ms` raises are projected and audited,
     but no gate consumes the dimension while a drain is idle: charging an `awaiting_input` park
     against a wall-clock budget would penalize waiting for a human, which is the opposite of the
     durable-pause model. Enforcing it would need a definition of "active wall clock" (turn time?
     drain time?) that this slice does not guess; the dimension stays a declared, observable fact.
  7. **No budget change for unconfigured sessions.** The projection has no ceiling, the gate returns
     "do not refuse", and the stream, wire and end reasons are unchanged. Legacy behavior is not
     "budget unlimited by guess" — it is no projection at all.
- **Alternatives considered**:
  1. **A terminal `failed` end on exhaustion** — rejected: out of money is not failure, and a
     terminal end would strand resumable work and lie in the audit.
  2. **Hold the drain lease while parked** — rejected: the confirmation plane already established
     that a durable wait is an end + a resumable row, not a held lease; the same argument applies to
     budgets.
  3. **Priced gate (exact micro-USD before send)** — rejected: pricing is a projection and a price
     promise before a byte is sent is not one kian can keep (ADR-053); the token estimate is
     label-honest and sufficient for a conservative gate.
  4. **A direct `budget/raised` event written by the control plane** — rejected by the layered iron
     rule: the control plane writes inbox rows, execution appends events.
  5. **Dynamic mid-session slot downgrade when the budget is tight** — rejected for this slice:
     runtime model swap is cache-hostile and forbidden on the hot path (constraint 11); a tighter
     model is a `slot/migration` decision, not a gate side effect.
  6. **Compaction as the budget response** — rejected: compaction is a context-limit lever with its
     own cost (it breaks the prefix cache once and re-pays the miss); spending money to save money is
     not the gate's call.
- **Consequences and limits**:
  - The projection is O(events) per provider turn in this slice (a fold over the session stream);
    a cached projection can be added when a session's stream is large, without changing semantics.
  - A harness-owned (projection-only) loop keeps driving its own turns: its session can declare a
    budget and be audited, but kian cannot refuse a request it does not send. The pin and the
    projection are still honest; the gate is a loop-side capability.
  - Spawned sessions other than the server's `create_session` path (cron firings, residents, worker
    spawns) fall back to the loop's agent declaration; pinning every spawn path is a follow-up, not a
    different semantic.
  - The `budget/raised` event is drain activity and never context: it cannot change
    `derive_model_context` or the cache prefix.
  - `kian usage-stats`/`kian cost` remain the spend report; the budget is the policy, not a
    duplicate ledger.
- **Acceptance (phase 1b)**: a pinned budget below the worst case ends the session `awaiting_budget`
  with zero provider requests and no `step/request`; a raise resumes it to `completed`; an exact
  raise replay dedupes to one row/one event; both amounts absent is refused; a terminal session
  answers `terminal`; wall-clock raises do not gate; a session with no budget keeps its old stream
  shape. Semantics land in [EVENT-MODEL](../EVENT-MODEL.md) §3/§4.4, the wire in
  [API-CONTRACT](../API-CONTRACT.md) §3, the placement in [DESIGN](../DESIGN.md) §15.
