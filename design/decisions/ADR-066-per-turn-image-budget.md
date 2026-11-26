# ADR-066 — A per-turn image budget

- Status: accepted (2026-11-26, vision cost-control slice)
- Amends: ADR-065 (channel images as vision references)

## Context

ADR-065 lets channel images reach a vision target as pixels: the artifact's own bytes, base64-inlined,
re-sent with the whole history on every turn. A drain consumes a burst in one sweep, so a backlog of
seventeen photos became one 18,235,916-byte request (2,357,776 bytes before the burst). The carrier
rejected it at its inbound bound, kian saw a transport error instead of an answer, and the turn
soft-failed. Two facts make the fix small:

- The model context is a projection of the event stream, not a stored message list: whatever the
  budget decides must be a pure function of the committed stream at the step's seq (hard constraint
  13), and it must never rewrite a request that already flew (constraint 3, cache-byte stability).
- The transcript is already in the wrapper text (ADR-062/065), so a drop costs detail, not meaning.

## Decision

1. The budget applies only to the **unsent tail**: the messages after the last assistant message in
   the model context. Everything at or before the last assistant message has already flown; the
   budget never touches it.
2. Selection is newest-first and contiguous: walk the tail backwards and admit references while both
   limits hold; the first reference that would exceed either limit ends the selection, and every
   older one stays text-only. The outcome is a pure function of the projected message order, so a
   retry after a crash or a soft failure composes byte-identical images.
3. Two limits: `superagent.shaping.max_images_per_turn` (default `4`, range 1–32) and
   `superagent.shaping.max_image_bytes_per_turn` (default `4194304`, range 64 KiB–64 MiB). Bytes are
   the artifact's raw length, measured before base64 inflation, so a request grows by ≈1.33× the
   budget plus wrappers.
4. A reference that does not fit is not rendered and nothing else changes: the wrapper keeps its
   exact bytes, the transcript stays in the text, the message still renders. One warning per turn
   names both limits and how many references were admitted.
5. The existing per-message ceiling (4 images, 8 MiB each, ADR-063) still applies; the turn budget is
   applied on top of it.
6. Both keys refuse boot unless `superagent.shaping.vision: true`: a budget without pixels is a
   configuration mistake, and the config layer is fail-closed (ADR-065 precedent).
7. No new event, crate or seam. The persisted `step/request` is the audit record of what the budget
   admitted; the `input/admitted` content keeps the complete reference list, because what arrived is
   a fact and what was shown is the request's business.

## Alternatives

- **Evict old pixels from history.** Rejected: it rewrites prior context, destroying the provider
  prefix cache and the byte identity of what was already sent (constraint 3). The only sanctioned
  prefix invalidation is compaction; eliding pixels at a compaction point is a separate change.
- **Drop the whole message instead of its pixels.** Rejected: the transcript is the point of the
  channel edge; losing the text loses the fact that a picture arrived.
- **Enforce the limit in the admission shaper.** Rejected: the shaper sees one event at a time, while
  the burst is assembled by the drain; the loop is the only place that knows the turn's tail, and a
  decision pinned at admission would leave the tail unbounded after a compaction or a re-arm.
- **Downscale each image.** Rejected for this slice: it changes the pixels the user asked to be
  shown and needs its own quality contract.

## Consequences

- A burst of N images renders at most `max_images_per_turn` references and at most the byte budget
  for that turn; the rest are text-only, and nothing disappears from the stream.
- A single turn's request is bounded by the budget plus history; history growth stays unbounded until
  the follow-up below.
- Existing deployments need no new config; a budget key without `vision: true` refuses boot.

## Limits recorded with this phase

- No retroactive eviction: images already in history are re-sent every turn, so cost and body size
  grow with the number of image-bearing turns. The hard stop is the provider's `max_request_bytes`
  (ADR-067), which fails honestly instead of letting the carrier drop the connection.
- No downscale, no dedupe, no per-image cache, no per-turn token or pricing accounting.
- The follow-up that bounds history is eliding pixels at the ADR-058 compaction point — the only
  sanctioned prefix invalidation — keeping the transcript in the folded summary.

## Acceptance

- L1: six tail references under the default limits render the newest four and log one warning while
  the other two messages still render text; a retry of the same stream composes identical images; a
  turn that already flew keeps its pixels on the next turn even with new tail references; the byte
  limit admits a prefix and stops at the first reference that would exceed it; a budget key without
  `vision: true` refuses parse.
- L2: a live `kian serve` fed a batch of image inputs persists a `step/request` whose image parts are
  within both limits.
