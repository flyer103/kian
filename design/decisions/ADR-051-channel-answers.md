# ADR-051: Channel answers — a closed vocabulary, never a free-text decision

- **Status**: accepted (owner gate, 2026-11-22).
- **Context**: the confirmation plane (ADR-036) is complete for a control-plane client: a pending
  `tool/call` ends the session `awaiting_input`, `confirm` admits a durable control row, and the
  frozen call executes on approval. It is unusable from a phone, which is where a decision often
  has to be made. A channel is an inbound text path with no request/response shape: the person
  replies in prose, and prose cannot be a decision — mapping "同意，但把 X 也改一下" onto
  `{decision: approve}` requires judgment the runtime is not allowed to fake
  (CONFIRMATION-MODEL §14, non-goals). The alternative — letting the model read the reply and then
  call `confirm` — makes a model-visible string into an authorization, which no durable audit can
  separate from a hallucinated one.
- **Decision** (phase 3a, opt-in):
  1. **Opt-in, closed set.** `superagent.confirm_answers.channels` names the channels that may answer;
     absent or empty is off, and boot refuses a channel that is not declared, whose `type` is not an
     IM channel, or that is admitted as `class: ignore` (the intercept runs before the class gate, so
     the two must not disagree).
  2. **A closed grammar is the decision.** `approve|ok|yes|同意|批准` + a six-character code, and the
     same verbs for `reject` with an optional single-line reason. Normalization is a restricted fold
     (full-width block, ideographic space, whitespace collapse, ASCII lowercase) — not full NFKC,
     and no new dependency. A verb followed by anything that is not code-shaped is **prose** and
     stays ordinary input: dropping a person's words to be safe would lose real work, and a
     prose "approval" cannot be mistaken for one because the Router is forbidden to assert that a
     confirmation happened.
  3. **The sender is the authenticator.** The channel allowlist already dropped every other sender
     before the event existed (CHANNEL-IMESSAGE §4). The code is a correlation token derived from
     `(session, call_seq, call_id)`, so it is restart- and replay-stable, needs no storage, and may
     leak harmlessly. No secret store, no rotation, no rate limit — they would guard the wrong
     boundary.
  4. **Resolution is deterministic and bounded.** Candidates come from the passthrough registry's
     live subscribers for the event topic; each candidate's pending set is the existing
     `pending_calls` fold; every candidate is visited (no early exit, so timing reveals nothing);
     exactly one match answers, zero is `no_match`, two or more is `ambiguous`.
  5. **The write is the existing seam.** The adapter calls one port that the CLI implements over the
     same `confirm` entry point the RPC uses, with `principal = "{channel}:{sender}:{event_id}"`, so
     TTL expiry, `not_pending`, archived-session refusal and the `confirmation` stamp on
     `tool/result` are §5's verbatim.
  6. **The text is not persisted.** The durable record is the decision, its principal, timestamp and
     reason. Persisting the command as an input would make it model-visible and re-open exactly the
     question this design closes.
  7. **No new kind, no new wire method, no new metric key.** The intercept adds audit lines carrying
     the existing `IntakeReason` codes and one read-only Router tool (`pending_decisions`), mounted
     only when the key is set.
- **Alternatives considered**:
  1. **Let the Router decide from prose and call `confirm`** — rejected: a model-visible string
     becomes an authorization, and no audit can distinguish it from a fabricated one.
  2. **A single-use secret token per ask** — rejected: defends the wrong boundary. The sender handle
     is already authoritative; a secret only adds a store, a rotation and a leak path.
  3. **Free-text clarification on the channel** — rejected (§14 non-goal): a decision is not a
     conversation, and a contradictory instruction would be unanswerable by a closed parser.
  4. **A bare `approve` with exactly one pending call** — deferred: it makes every "同意" a decision.
     If wanted it needs the extra condition that the last outbound delivery named that call.
  5. **A system-rendered decision notification** — deferred to 3b: it needs a new outbound trigger,
     a bigger authority change than this slice.
- **Consequences**:
  - a decision now travels on a channel, but only a six-token command; everything else stays model
    input, and the prompt line tells the Router never to claim a decision happened;
  - the code reaches the human through the model's reply (model-mediated), so a mangled code
    produces `no_match`, an honest no-op — never a guessed approval;
  - `topic → live worker` binding is the whole candidate set, so a delegation that does not carry
    the conversation's `correlation` cannot be answered from the phone (fail-closed, documented in
    the mobile example's charter);
  - enabling the key changes the Router's prompt prefix and tool block once, at boot — the only
    place this codebase allows a prefix-invalidating decision to come from (constraint 3);
  - L2 evidence is the `mock-im` adapter on a real server; the live-phone check remains manual
    (CONFIRMATION-MODEL §14d X9 row 8) because iMessage has no emulator.
- **Live findings (2026-11-22, macOS, `kian serve` + `mock-im`, real sqlite store)**: a worker parked
  on `ask` (`tool/call{permission.evaluated:"ask"}` → `session/ended{awaiting_input}`); `approve
  <code>` produced `control/confirm` then `tool/result` carrying `confirmation`, the worker resumed
  and replayed 2 of 2 requests byte-identically, and the answer event produced **no** routing or
  admission audit line — the inbound text never became model input. A malformed code and a stale
  code each produced `reason=answer_no_match` with no row and no wake. With the key absent, the
  Router's first `step/request` body was byte-identical to the pre-change binary's.
- **Rollback Trigger**: a channel whose sender identity is not trustworthy (then it must not be in
  the list), or a grammar that keeps mis-parsing real messages (then a narrower verb table, still a
  closed set).
- **Cost Impact**: one bundle module (`answers.rs`), four `RoutingOutcome` arms, four reason codes,
  one config key with its fail-closed validation, one read-only tool, one frozen prompt line, and
  the `kian-cli` port that maps the existing confirm outcomes; no crate, no event kind, no wire
  method.
