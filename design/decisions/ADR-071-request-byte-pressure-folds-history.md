# ADR-071 — Request byte pressure folds history before the turn exists

- Status: accepted (2026-11-27)
- Amends: ADR-058 (production compaction)
- Relates: ADR-066 (per-turn image budget), ADR-068 (oversized-request classification)

## Context

ADR-065 makes a channel image reach a vision target as the artifact's own bytes, and the loop
re-sends the whole history on every turn. ADR-066 bounds only the *unsent tail* (the newest
references, 4 images / 4 MiB by default); every image that once entered the context stays in the
request body for the life of the session. On the production deployment four consecutive Router
sessions died the same way: a photo burst pushed the rendered body to 35–37 MB, the carrier's
inbound bound sat at 32 MiB, and ADR-066's tail budget could not remove a single byte of it.

ADR-068 then did exactly what it promises — the pre-flight compared the body with
`providers.<name>.max_request_bytes` and failed `request_too_large` with *nothing sent*. But the
stream is the truth and the history is immutable, so the next turn rendered the same body and failed
again: soft failure after soft failure, the drain crash-loop guard closed the session `failed`, and
the operator had to rotate it by hand to escape. The refusal is honest; the session is still wedged.

The missing piece is a way to *shrink* the body before it is built into a turn. Compaction (ADR-058)
is exactly that mechanism, and it is the one sanctioned prefix-invalidation point (constraint 3).
It is triggered today by two readings, neither of which fires here:

- a declared `profile.context_window` that the next request would fill to `trigger_percent`;
  an image is billed by area, not by its base64 length, so a 30 MB body can be far from a 1M-token
  window;
- the provider's `ContextOverflow` answer, which a byte bound refuses long before it is reached.

The bound that is actually exceeded is a byte bound, and the loop already knows the exact number:
the plan's body is the same serialized value the ADR-068 precheck compares.

## Decision

1. **A third trigger, byte pressure.** `CompactionTrigger::Bytes { request_bytes, limit_bytes }`
   joins `Pressure` and `Overflow`; it authorizes a fold exactly when the rendered request would
   exceed the bound. Every other planner rule is unchanged: keep `keep_recent_turns` verbatim,
   start after the last `compact`, return a strictly-past disjoint range, one fold per call.
2. **The reading is the request itself, not an estimator.** The request is prepared exactly as a
   send would prepare it — chat messages with artifact bytes inlined for a vision target, the
   deterministically sorted tool schemas, the built plan — and measured with
   `kian_llm::provider::serialize_request_body`, the single function that produces both the
   persisted and the sent bytes (ADR-054). No second byte model, no size heuristic, no drift
   between the gate, the audit copy and the precheck.
3. **The gate sits before the turn exists.** Preparation moves ahead of `begin_turn`: the loop
   builds the request, measures it, and only then starts a turn. An oversized body is therefore never
   persisted (persist-what-you-send covers requests that fly — this one is never created) and never
   sent; the fold that follows is an ordinary maintenance turn, exactly as ADR-058 defines it.
   The byte gate runs before the token-budget gate, so an over-bound request folds rather than parks
   `awaiting_budget` — both outcomes are non-destructive, and the fold leaves a sendable request that
   the budget gate then judges on its real size. ADR-068 is not replaced: it remains the last line of
   defense for the cases a fold cannot fix.
4. **The bound is the one already declared.** `providers.<name>.max_request_bytes` (ADR-068) is
   resolved per slot target through `binding.target.provider` and delivered into the loop as a
   provider → bound map built from the same `providers:` block. There is no new config key; a
   deployment that declares no bound has no byte gate and no behaviour change. The gate reads the
   primary target's body; a fallback attempt is built per target and keeps that provider's own
   precheck.
5. **A byte-triggered fold skips the minimum-reclaim rule, nothing else.** `min_reclaim_tokens`
   counts the projected *text* bytes, so it is blind to the pixel payload the fold is there to
   remove; requiring it would refuse the only fold that can help. The fold still respects the
   fold boundary and the monotonic frontier, still runs at most
   `MAX_COMPACTIONS_PER_EPISODE` (2) times per episode, and the byte reading is written into the
   `compact` event's existing `reason` field so the invalidation is auditable. After a fold the
   loop re-measures the newly rendered request; one that is still over the bound once the
   per-episode fold budget is spent is sent, and ADR-068 refuses it.
6. **The ceiling is arithmetic, not hope.** After the fold, the body is bounded by
   `keep_recent_turns × max_image_bytes_per_turn` plus the summary, the tool schemas and the text of
   those turns. The defaults (4 turns × 4 MiB, ADR-066) leave roughly 16 MiB of pixels under a
   32 MiB bound, so one fold is enough to return to a sendable size. When an operator configures a
   product that already exceeds the bound (for example `keep_recent_turns: 8` with 4 MiB turns
   against a 16 MiB bound), no fold can relieve the request: the loop logs that arithmetic once per
   episode and the refusal is ADR-068's, honestly named.
7. **No new event kind, no new state, no new seam.** `compact` keeps its frozen keys and gains
   nothing but the reason text it already carries; the trigger is a pre-send reading and is never
   re-derived on replay (the fold it caused is an event like any other). Nothing about the decision
   is stored outside the stream.

## Alternatives

- **Drop the oldest rendered images outside compaction.** A window that slides as messages arrive
  rewrites the prefix on every turn — the exact cost blowup constraint 3 forbids, and invisible in
  the stream.
- **Estimate the body from projected text.** The planner's `message_bytes` sees artifact
  references, not pixels; an estimate built on it would understate a 30 MB body by an order of
  magnitude.
- **Split the request / raise the bound automatically.** Both move the failure to the carrier and
  hide the deliberate invalidation.
- **Cap images per request by count.** Same sliding-window problem as the first alternative, with a
  worse unit: 30 MB of JPEGs is four images for one row and sixty for another.
- **Rotate the session on byte pressure.** Loses the conversation to avoid a fold that keeps its
  summary.
- **Reactive-only (fold after a `request_too_large` refusal).** Correct but wasteful: every episode
  would persist and store a 30 MB `step/request` that provably cannot be sent, and the store write
  alone cost seconds in production.

## Consequences

Positive: a vision session stops being a one-way ratchet toward the carrier's bound; the fold that
removes old pixels is the documented invalidation point, with its own event, summary and cost
accounting; byte and token pressure now share one planner and one mechanism.

Negative: pressure can now fold history earlier than a 1M-token window would — an operator who
declares a byte bound trades context retention for deliverability, and should read the bound as a
retention knob. A byte-triggered fold invalidates the provider prefix cache, which is exactly the
cost ADR-058 already accepts for a fold, now paid at a byte boundary instead of a token one. A
declared bound also costs one extra serialization of the body per request (the measurement); the
peak memory is unchanged because the provider's own serialization is transient in the same way.

Limits recorded with this phase: no downscale or re-encode (an image is folded or kept whole), no
per-request image count cap, no retroactive eviction outside a fold, no cross-session sharing of the
fold, no automatic raising of the bound, no change to ADR-066's tail budget or ADR-068's precheck. A request that is over the bound but has no foldable
range left (fewer than `keep_recent_turns` turns, or every foldable turn already folded) is sent —
the loop warns once with the size, the bound and the retained turns, and ADR-068 refuses it. The
summarizer's own request is not byte-gated; it is small by construction (a bounded transcript
summary) and ADR-068 would refuse it honestly.

## Acceptance

- L1 (`kian-orchestrator` planner): a byte trigger authorizes only over the bound; it does not
  consult `min_reclaim_tokens`; the range rules (keep-recent boundary, frontier after the last
  `compact`, strictly past) still hold, and its reason names both byte numbers.
- L1 (`kian-loop`): a request over the bound with nothing left to fold is not a spin — the
  iteration warns with the size, the bound and the retained turns, sends the body, and the turn
  settles; no `compact` event is invented for an empty window.
- L2 (`kian-cli`): a real serve with a mock provider, a small `max_request_bytes` and a growing
  history of channel pictures folds when the rendered body crosses the bound, the retried request
  carries fewer image parts and still carries pixels, and the turn settles. Verified against a
  measured 289,450-byte body over a 250,000-byte bound (six image parts before the fold, four
  after).
- L3 (production): with the bound at the carrier's inbound limit, a photo burst folds instead of
  wedging; the session keeps answering. Not run with this phase.