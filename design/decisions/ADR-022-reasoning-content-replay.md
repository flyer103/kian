# ADR-022: Replay assistant reasoning — persist and resend `reasoning_content`

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: `design/EVENT-MODEL.md` §4.1 (assistant message payload), `design/DESIGN.md`
  §3.2 (model-context projection, frozen serialization), ADR-006 (model slots), ADR-007
  (delivery semantics — unaffected), `docs/SPEC.md` §5.
- **Context**: DeepSeek's V4 family runs in thinking mode by default and returns
  `reasoning_content` on assistant messages. In thinking mode the API **requires** every assistant
  message in the history to carry that field back — otherwise the next request is a deterministic
  HTTP 400 (`reasoning_content ... must be passed back to the API`). Kian preserves the exact
  request bytes it sends (`step/request`) and replays history from the event stream, but the
  assistant payload only carried `text` and `tool_calls`; the reasoning was dropped in the stream
  assembler and the replay builder. A live owner deployment surfaced the failure at the resident
  Router's empty-batch judge turn: the request history ended on an assistant message without
  reasoning, upstream answered 400 (`format_error`), kian classified it as a soft failure and the
  next admitted input recovered — functional, but a recurring failure on every empty-batch turn
  and a correctness hole for any session whose history contains tool-call assistant turns.
  The alternative is to disable thinking (`thinking: {type: "disabled"}`) — cheaper and simpler,
  but it removes the reasoning quality the model was chosen for and does not support thinking at
  all. This ADR records the replay decision; the provider-side switch is deliberately not added
  (see Alternatives).
- **Decision**: persist the assistant's reasoning verbatim as part of the assistant message and
  replay it on the provider request when present.

  1. **Payload (additive, context-affecting).** `assistant/message` `content` gains an optional
     `"reasoning_content": <str>` key. It is part of `content` because it is *context* (it enters
     the next request), unlike the additive provenance keys of EVENT-MODEL §4.2. `content`
     continues to pass through verbatim and is never reformatted.
  2. **Capture.** The built-in ReAct stream assembler concatenates
     `choices[0].delta.reasoning_content` fragments in arrival order, exactly as it already
     concatenates `content` and tool-call fragments. A turn with no reasoning fragments emits the
     exact legacy payload (no key).
  3. **Replay.** The message → chat conversion adds the top-level wire field
     `reasoning_content` on assistant messages that carry it. Assistant messages without the key
     serialize exactly as before; `FROZEN_CHAT_BODY_SHA256` is **unchanged** (the field is
     `skip_serializing_if = None`). Sessions created before this ADR replay byte-identically.
  4. **Scope.** The field is provider-neutral: any OpenAI-compatible endpoint that returns
     `reasoning_content` gets it replayed; endpoints that never return it are unaffected.
     `kian-dsh`/harness drivers that do not use the built-in assembler are out of scope (their
     events keep today's payload).
  5. **Cost.** Replayed reasoning is input tokens (and part of the cached prefix when stable). The
     contract accepts the cost: it is the price of thinking-mode correctness, and it is observable
     through the existing usage events. No pricing or trimming logic is added.

  Adversarial consequences checked: a crash between the provider response and the assistant append
  loses the turn exactly as before (the stream is truth); compaction skips the compressed range and
  the summary carries no reasoning (unchanged); export/import round-trips the additive key;
  a legacy event without the key produces byte-identical requests; replaying the same bytes twice
  is idempotent (the field is data, not an action).

- **Alternatives considered**:
  - **Disable thinking per provider** (`thinking: {type: "disabled"}` in the request body): small
    config-surface change, no event change, cheaper. Rejected as the *only* answer because it
    permanently removes thinking quality and leaves the underlying "reasoning is not preserved"
    defect; it remains a possible future cost-control knob (a separate additive decision).
  - **Strip/downgrade on replay**: dropping reasoning is exactly the current 400; synthesizing it
    is forbidden (constraint 2/3 — never invent model-visible bytes).
  - **A provider-side transform**: vadis (or any gateway) cannot synthesize the missing field; the
    producer (kian) must keep it.
  - **Ignore the failure**: the soft failure is recoverable but recurs and hides a real provider
    contract violation.
- **Consequences**: one context-bearing payload key; request bytes grow by the replayed reasoning
  for thinking sessions (only); no new event kind, no new table, no wire/API change, no core state.
  The provider contract (OpenAI-compatible chat) is unchanged — the field is additive.
