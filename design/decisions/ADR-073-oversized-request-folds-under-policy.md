# ADR-073 — An oversized request folds when the policy resolves `compress_then_retry`

- Status: accepted (2026-11-27, compaction slice)
- Related: ADR-058 (production compaction and maintenance reserve), ADR-066 (per-turn image
  budget), ADR-068 (oversized request classification)

## Context

A vision session on the production deployment sent `35,350,711` bytes to a carrier whose
`providers.vadis.max_request_bytes` is `33,554,432`. The carrier call failed
`request_too_large` (ADR-068) before a byte left the process, the recovery policy resolved the
class to `fail`, and the turn ended with no answer. The request had grown monotonically:
`17,798,936` bytes at seq 151, `28.1` MB at seq 188, `34.7` MB at seq 205, `35,310,425` at
seq 223 — inline base64 image parts carried in history, roughly 2-4 MB per vision turn.

Compaction existed and was configured, but never ran: the session's counters read
`compactions: 0` and the store-wide projection read `compacts: 0`. Two independent reasons:

1. The preventive arm needs a declared `profile.context_window` and the production profile declares
   none, so it could not arm (ADR-058's own guard).
2. The reactive arm recognized exactly one class — `context_overflow`. A byte refusal is a
   different class, so the loop fell through to the honest soft failure.

ADR-068 deliberately left this gap open: it declared `request_too_large` non-retryable by default
because the same bytes cannot succeed, and named the escape hatch in prose — "a deployment that
wants compaction to shrink the history opts into `compress_then_retry` explicitly". The
documentation in `docs/advanced.md` promises the same. No code path honored the opt-in, so the
promise was drift: the policy table could name an action the loop would never take.

## Decision

1. **The reactive fold is keyed on the resolved recovery action, not on the class.** After a soft
   failure the loop reads `action` from the recovery audit it already persists, and folds only when
   that action is `compress_then_retry`. The audit and the fold therefore cannot disagree: the
   decision the operator configured is the decision the loop executes, and an operator who maps the
   class to `fail` still gets a failed turn. The same gate covers `context_overflow`: the table is
   authoritative for every reactive fold, and the default table already names
   `compress_then_retry` for that class.
2. **The class selects the trigger, not the disposition.** `context_overflow` folds with the
   existing `CompactionTrigger::Overflow`; `request_too_large` folds with a new
   `CompactionTrigger::Oversized`. Both carry no payload — the refusal is already in the audit.
   The trigger's reason names the byte verdict ("provider refused the request as too large
   (ADR-068)") so a fold's `compact` event never claims a window overflow that did not happen.
3. **Defaults are unchanged.** `request_too_large` still resolves to `fail` in the default table,
   so no deployment changes behavior without an explicit rule. ADR-068 decision 1 stands; this ADR
   only makes the documented opt-in real.
4. **The fold stays bounded and observable.** It runs through the same `compact_now` path as every
   other fold: one persisted maintenance turn (`purpose: "compact"`), a `compact` event appended
   only after the summary exists, at most two folds per drain episode, and a planner that refuses
   without real reclaim. When no fold is possible the turn settles soft exactly as today.
5. **Nothing folds pre-emptively.** The loop still learns about the size from the provider's
   verdict, not from a probe: `ModelProvider` has no size seam, the byte math already lives in the
   provider adapter, and ADR-068 keeps the refused `step/request` on purpose as the record of what
   was not sent. A refused request costs one persisted envelope, never a provider call.

## Alternatives

- **Fold on the class regardless of the table.** Rejected: it would silently override an operator's
  `fail`, and ADR-068 makes history shrinking an explicit policy action. Constraint 8 puts the
  table in the caller's hands for exactly this kind of choice.
- **Loop-side pre-flight against `max_request_bytes`.** Rejected: it duplicates the seam's
  serialization math in a second place, needs a new `ModelProvider` method to stay honest across
  dialects, and would fold before the provider has said anything — turning a configuration bound
  into loop policy.
- **Elide old image parts at a sliding watermark.** Rejected: any per-turn change to already-sent
  history rewrites the request prefix and invalidates the provider cache every turn (constraint 3).
  Only a `compact` event may invalidate a prefix, which is why the fix is a fold.
- **Declare `context_window` and rely on the preventive arm.** Rejected as the only fix: the
  failure was byte-bound, and pixels make bytes grow faster than tokens, so a token-window estimate
  can stay under its threshold while the serialized body is already over the carrier's bound.
- **Retry after trimming just enough messages.** Rejected: it is a silent history rewrite outside
  the event model, and the summary that makes the fold cheap would be missing.

## Consequences

- A deployment with a vision-heavy session and a `max_request_bytes` bound can now keep the session
  alive: the turn that would have died folds once, retries a smaller request and finishes.
- The opt-in costs one rule, but `PolicyConfig::from_yaml` **replaces** the table rather than
  merging with the defaults, so an explicit `recovery:` block must carry every rule the deployment
  relies on. The documented shape is the default table plus the one override.
- With four recent turns retained verbatim, a vision-heavy session still grows to the byte cap
  before it folds, and the fold drops the pixels outside that window. Requests oscillate near the
  cap instead of staying small; a byte-aware preventive arm and pixel elision at the fold are later
  work (this ADR does not claim them).
- If the retried request is still too large, a second fold may run (bounded), then the turn ends
  `failed` with `request_too_large` — the honest terminal state, now reached after a real attempt
  to shrink rather than on the first refusal.
- The refused request was never sent, so the fold invalidates no cached prefix beyond its own
  `compact` event: the cost of the recovery is one summarizer call.
- The action gate is a behavior delta for `context_overflow`: a deployment that supplied an
  explicit `recovery.rules` list without the overflow rule (or mapped it to `fail`) no longer
  folds on a window overflow, where the old class-only check folded regardless of the table. A
  deployment with no explicit table, or with the defaults, is unaffected.

## Limits recorded with this phase

- No pre-flight size estimation, no partial-body or streaming upload, no bound discovery.
- No summarizer hint about *why* it is folding (byte refusal vs window pressure); the compaction
  prompt is unchanged.
- No pixel elision: folded turns keep their text summary, not their images, and the retained turns
  keep theirs in full.

## Acceptance

- L1 (covered by `crates/kian-loop/src/integration_tests.rs` and
  `crates/kian-orchestrator/src/compaction.rs`): a `request_too_large` soft failure under a
  policy that resolves `compress_then_retry` folds once, retries the smaller request and
  completes; the same failure under the default (`fail`) table folds nothing and the session
  ends failed; the same is true of a table that maps `context_overflow` to `fail`; the
  `compact` event's reason names the byte refusal; the `Oversized` trigger authorizes a fold
  without a window reading.
- L2 (covered by `crates/kian-cli/tests/oversized_fold_l2.rs`): a live assembled server with a
  `max_request_bytes` bound and the opt-in rule answers a resident Router batch that would
  otherwise fail — one `compact` event, the refused bytes still in the stream, and nothing over
  the bound ever reaching the provider.
- L3: the production vision session that failed at 35,350,711 bytes completes its next turn after
  the deployment enables the rule and the shape is re-enabled; the refused envelope stays in the
  stream beside the fold that answered it.
