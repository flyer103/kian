# ADR-062: Vision inputs as dialect-level content parts

- Status: accepted (2026-11-23, P5c slice)
- Related: ADR-053 (request plan, model profiles), ADR-055 (tool result
  artifacts), ADR-060 (client-side media tools), ADR-061 (channel-edge
  media), DESIGN §3.2, SPEC §P5c

## Context

`ModelProfile` has carried a `vision` flag since ADR-053, and the
`model_profiles:` section can declare it, but nothing consumes it: every
user message becomes a plain `{role, content: <string>}` chat message, so
a model that accepts images has never seen one. The P5 phase list ends
with vision for a reason: it is the only capability that changes the
request dialect rather than adding a surface beside it.

The pieces are already in place: the content-addressed artifact store
(ADR-055) can hand bytes to a drain, the request plan (ADR-053) is the
single place that turns messages into the bytes both persisted and sent,
and the loop is the single place that derives the model context from the
event stream. What is missing is the wire shape and the gate.

## Decision

1. **Vision rides the dialect, not a new tool.** A user input payload may
   carry, as its `content` object, `{"text": <string>, "images":
   [{"artifact": <content id>, "media_type": <iana type>}]}`. The loop
   resolves each referenced blob through the artifact store and attaches
   the bytes to the `ChatMessage` it derives. The V2 plan builder then
   rewrites exactly those messages into OpenAI content parts:
   `[{"type": "text", "text": ...}, {"type": "image_url",
   "image_url": {"url": "data:<media_type>;base64,<bytes>"}}]`.
2. **V1 is untouched.** `ChatMessage::images` is `#[serde(skip)]`, so the
   frozen V1 body (no profile) and every image-free V2 body keep their
   exact bytes. Only a body that actually carries an image can change,
   and only under a profile that declares the capability.
3. **Gated by `profile.vision`.** Only a target whose `model_profiles:`
   entry sets `vision: true` attaches bytes; without it the content
   passes through as today's compact JSON text, byte-identical to the
   current behavior. A profile does not have to be globally new — vision
   is per (provider, model) target, exactly like thinking and effort.
4. **Bytes only through the artifact store.** The loop reads with a new
   `DrainCtx::artifact_bytes` accessor (same store the spill path and
   `artifact_read` use). A missing blob, an oversized image, or a
   malformed entry drops that image with a warning; the text envelope
   still reaches the model, so a reclaimed artifact degrades the answer
   instead of failing the turn.
5. **Persist-what-you-send still holds, with one honest limit.** The
   `step/request` body carries the data URLs — the exact bytes sent — so
   the audit trail is complete. A replay cannot re-derive base64 from the
   event stream alone, so `check_model_context` counts a request whose
   recorded `messages` already carry content parts as a
   `vision_request` instead of reporting a byte-identity mismatch. The
   persisted body stays the truth; only the offline re-derivation is
   declared unable to reproduce it.
6. **Bounded inputs.** At most 4 images per message and 8 MiB per image,
   enforced by the loop before any base64 encoding. The estimate band and
   the budget gate see the final body, so a vision request is budgeted
   like any other request.
7. **No new event kind, crate or provider code.** The shape lives in
   `kian-llm` (plan builder) and `kian-loop` (assembly); `kian-core` gains
   one read accessor. The channel-edge media processor (ADR-061) keeps
   transcribing to text; vision is an explicit API/file path
   (`kian artifacts put` + `submit` with the content envelope).

## Alternatives considered

- **A vision tool instead of content parts.** Rejected: an OCR-style tool
  returns text only; asking a vision model to describe an image needs the
  image in the prompt, and the tool round trip would spend a turn and
  lose the native multimodal capability the provider sells.
- **Always attach images across dialects.** Rejected: the V1 body is
  frozen stable content (constraint 3); it may never grow fields. Vision
  is a V2-only capability, exactly like `thinking` and `max_tokens`.
- **Re-derive images during replay from the artifact store.** Rejected
  for this slice: replay reads a single event stream, not a store
  configuration, and the CLI would have to grow an `--artifacts`
  argument. The persisted body already records the exact bytes; the
  offline check is a convenience detector, not the audit.
- **Automatic vision from the channel media edge.** Rejected: text is
  cheaper, works with every model, and survives artifact GC. A channel
  image is transcribed (P5b); attaching it as vision is a later opt-in.

## Consequences

- A vision request's body is large (base64 is ~4/3 of the image bytes).
  The cache-stability rules still apply to the prefix they create, and
  the per-image caps bound the damage.
- `kian cost` sees no special case: the request is priced from its
  usage like any other, and the cache checker compares it like any other
  adjacent pair.
- A session that starts on a vision target keeps it for its lifetime
  (slots are session-immutable), so a capability toggle is a migration,
  not a mid-session switch.

## Limits recorded with this phase

- No real vision provider is wired in the tree: the L2 evidence is a
  scripted provider asserting the exact content-parts body, plus the
  store round trip.
- Images are not priced per image; only the tokens the provider reports
  are.
- Only `role: user` messages carry images; assistant/tool messages do not.
- Replay's byte-identity check cannot re-derive a vision body offline (see
  decision 5).

## Acceptance

- `kian-llm` unit tests: content parts for an image-bearing user message
  under a vision profile; byte-identical V2 body when `vision` is false or
  no images are attached; V1 body unchanged with images present; base64
  encoder vectors.
- `kian-loop` L2: a scripted provider receives the content-parts body for
  a user input that references an ingested artifact; a missing artifact
  yields the text envelope only.
- `kian-orchestrator`: `DrainCtx::artifact_bytes` round trips a stored
  blob and returns `None` when spill is off.
- Docs: SPEC P5c section and phase row, DESIGN §3.2, advanced §Vision
  inputs (with a runnable snippet), CHANGELOG, overview status row.