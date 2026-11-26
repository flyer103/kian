# ADR-067 — An oversized request is a classified, non-retryable failure

- Status: accepted (2026-11-26, provider-failure classification slice)

## Context

A vision turn sent 18,235,916 bytes to the carrier. The carrier read the declared length, answered
`413 request_too_large` and closed the connection before reading the body; the client was still
writing, so the failure surfaced as `ProviderError::Transport` — classified `provider_down`, which
the deployment's policy table did not cover, so the turn soft-failed and the drain eventually tripped
the crash-loop guard. Three separate faults:

1. The loop had no pre-flight notion of "this body is too large to send".
2. A 413 *answer* would have been `InvalidResponse` → `validation_failed` — honest but misnamed:
   nothing was invalid about the request, it was simply too large.
3. A connection closed mid-upload is indistinguishable from a dead provider, so the honest class is
   not reachable from that path at all.

## Decision

1. A new closed class, `request_too_large`, joins the policy vocabulary. It is non-retryable by
   default: the same bytes cannot succeed, so the default table maps it to `fail` and a deployment
   that wants compaction to shrink the history opts into `compress_then_retry` explicitly.
2. The provider seam gains `ProviderError::RequestTooLarge { size, limit, detail }`; the
   orchestrator classifies it as `request_too_large`. Error text names the size, the bound (when
   known) and whether any byte left the process.
3. `providers.<name>.max_request_bytes` (optional, absent = no pre-flight check, floor 1024) is
   compared against the serialized body **before** the POST. Over the bound the call fails
   `request_too_large` with "no request byte left this process". This is the fix for fault 3: with
   the bound declared, the carrier's close-on-413 path is never entered.
4. An HTTP 413 answer is classified `request_too_large` too, carrying the status, the size of the
   body that was sent and a bounded snippet of the answer; 400/422 keep their current
   context-overflow-or-invalid split.
5. The persisted `step/request` stays the audit record — it is written before the send (ADR-054), so
   a refused request still shows exactly the bytes that were not sent.
6. No transport heuristics: without a declared bound, a mid-upload connection close stays
   `provider_down`, because guessing "the peer refused our size" from an I/O error would misclassify
   genuine outages.

## Alternatives

- **Map 413 onto `validation_failed` and stop there.** Rejected: the class name points operators at
  the request shape, while the actionable facts are the two sizes and the carrier's bound.
- **Infer the carrier's bound from a 413 body and retry with a smaller context.** Rejected: the
  carrier usually closes before the body is read (no body to parse), and silently rewriting history
  would violate cache-byte stability (constraint 3).
- **Auto-shrink the context on every oversized request.** Rejected for this slice: compaction is the
  sanctioned invalidation point (ADR-058) and must stay an explicit policy action, not a hidden
  retry path.
- **Retry the same request.** Rejected by construction: the bytes are identical.

## Consequences

- An oversized turn ends `failed` with an error that names both numbers, instead of a soft failure
  that looks like an outage.
- A deployment pairs `providers.<name>.max_request_bytes` with the carrier's own inbound bound (for
  vadis, `server.max_body_bytes`); the two numbers are configuration, not discovery.
- The policy table gains one name; an explicit `recovery:` table that omits it falls back to the
  documented "no rule covers → fail honestly" behavior, which is now visible in the audit reason.

## Limits recorded with this phase

- The pre-flight check only exists when configured; an unset bound keeps today's behavior exactly.
- No body splitting, no streaming upload, no automatic history shrinking, no bound discovery.
- History growth (images included) stays unbounded; this class makes the failure honest, it does not
  make the request fit.

## Acceptance

- L1: a 413 answer classifies `request_too_large` and the error text carries the size and status; a
  body one byte over `max_request_bytes` fails without opening a connection (the test server sees
  zero requests); a body at the bound is sent; `ProviderError::RequestTooLarge` maps to the policy
  class and the default table resolves `fail`; 400 with a context-overflow body still classifies
  `context_overflow`; a missing key parses as absent.
- L2: a live `kian serve` with a deliberately small `max_request_bytes` ends the turn `failed` with
  the `request_too_large` reason and the carrier log shows no request.
