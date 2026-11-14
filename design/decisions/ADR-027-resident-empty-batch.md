# ADR-027: Resident empty batches do not call the provider

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: ADR-024 (resident per-episode budget), ADR-022 (reasoning replay), DESIGN §4.6
  (resident re-arm), `crates/kian-loop` (`run_iteration`), `crates/kian-superagent` (RouterDriver
  park).
- **Context**: ADR-024 removed the budget freeze, but the owner deployment then showed a residual
  400 after some replies. The stream evidence is exact: a resident Router turn whose
  `assistant/message` carried **no** `reasoning_content` (the provider can omit it on trivial
  turns — `reasoning_tokens: 0`) is followed, with **no `input/admitted` in between**, by another
  `drain/claimed` and a `step/request` whose message list ends on that assistant message. DeepSeek's
  thinking mode rejects a request whose assistant history lacks the reasoning it expects (HTTP 400
  `format_error`); kian classifies it as a soft failure, the session goes `failed → idle`, and the
  next input recovers — replies still arrive, but every such cycle burns a turn and ends the
  session once. The trigger is an **empty judge turn**: the resident loop was driven with no
  eligible input (a fresh process's first drive, a stale/spurious wake, or a replay) and still
  spent a provider call on "Batch is empty". Replaying reasoning (ADR-022) cannot fix the case
  where the model returned none, and synthesizing it is forbidden (constraint 2/3).
- **Decision**: a resident (`BudgetScope::PerEpisode`) ReAct iteration with **no eligible input**
  and a derived context whose **last message is `assistant`** settles without a provider call.

  1. **Where.** In `ReActLoop::run_iteration`, after `take_eligible_inputs` and the context
     derivation: `taken.is_empty() && budget_scope == PerEpisode && messages.last().role ==
     "assistant"` ⇒ `Flow::Settled`. The RouterDriver's existing Settled handling parks the
     resident; no `turn/start`, `step/request`, `assistant/message` or `session/ended` is written.
  2. **Why the last-role test.** A tool-result continuation (last role `tool`, e.g. after `reply`)
     must still call the model; a new input (taken non-empty) and the bootstrap/takeover (pending
     row) are taken and proceed. Only the ended-on-an-assistant, no-work case is skipped.
  3. **Scope.** `Lifetime` loops (worker/plain) are unchanged: their empty-batch behavior and
     request bytes stay exactly as before. No new event kind, no wire change, no config field, no
     table. One `debug` audit line records the skip.
  4. **Liveness.** Inputs are durable rows; skipping a no-work drive loses nothing. A later
     admitted input wakes the resident and starts a normal judge turn. An outstanding worker report
     is itself an inbox row (taken non-empty) when it arrives.

  Adversarial consequences checked: the skip is idempotent (a second empty drive skips again, no
  model call); a control/interrupt row is not a judge input, so parking is safe (the engine handles
  it); a resident whose history ends on a **user** input but whose row was already consumed still
  proceeds (the last-role guard) — this preserves takeover/steer semantics; worker/plain request
  bytes are untouched. The cost is that the Router no longer spends a call to say "nothing pending",
  which is the desired behavior.
- **Alternatives considered**:
  - **Disable thinking** (provider request knob): removes the 400 class but permanently lowers
    quality and is a broader provider-contract change; not used here.
  - **Fabricate/derive reasoning** for the assistant message: forbidden (model-visible bytes are
    never invented).
  - **Drop the empty turn in the RouterDriver only**: the driver cannot see eligibility without
    consuming inputs; the loop's `take_eligible_inputs` is the authoritative point.
  - **Rely on ADR-022 replay**: cannot help a turn where the provider returned no reasoning.
- **Consequences**: resident sessions stop issuing no-work provider calls and no longer end failed
  after a reply whose final assistant had no reasoning. Non-resident loops and all wire/event
  contracts are unchanged. The remaining explicit-terminal runtime re-arm follow-up (ADR-024) is
  unaffected.
