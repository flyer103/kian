# ADR-073 — An oversized request folds when the policy resolves `compress_then_retry`

- Status: accepted (2026-11-27, compaction slice)
- Related: ADR-058 (production compaction and maintenance reserve), ADR-066 (per-turn image
  budget), ADR-068 (oversized request classification), ADR-072 (request byte pressure folds
  before the turn exists)

## Context

A vision session on the production deployment sent `35,350,711` bytes to a carrier whose
`providers.vadis.max_request_bytes` is `33,554,432`. The carrier call failed
`request_too_large` (ADR-068) before a byte left the process, the recovery policy resolved the
class to `fail`, and the turn ended with no answer. The request had grown monotonically:
`17,798,936` bytes at seq 151, `28.1` MB at seq 188, `34.7` MB at seq 205, `35,310,425` at
seq 223 — inline base64 image parts carried in history, roughly 2-4 MB per vision turn.

ADR-072 answers that shape at its source: with a declared bound, the loop measures the body a send
would use and folds before the turn exists, so the refusal above no longer happens. This ADR
answers the refusal that still arrives, because the gate compares a *declared* bound and a
provider can refuse bytes for a reason the declaration does not describe:

1. The carrier's real limit can sit below `providers.<name>.max_request_bytes`; the local
   precheck passes and the carrier answers HTTP 413, which the adapter maps to the same
   `request_too_large` class (`crates/kian-llm/src/provider.rs:601-615`, past the local precheck
   at `crates/kian-llm/src/provider.rs:287`).
2. A deployment can declare no bound at all — ADR-072 deliberately has no gate then — and still
   meet a carrier that refuses.
3. The gate runs on the primary target's body; a fallback target builds its own body and keeps
   its own precheck, so a refusal can come from a body the gate never measured.

In each case the class reaches the loop as a soft failure. Before this ADR, the reactive arm
recognized exactly one class — `context_overflow` — so a byte refusal fell through to the honest
failure: the same bytes were rebuilt on the next turn, and a resident session could only be escaped
by rotation. ADR-068 named the escape hatch in prose — "a deployment that wants compaction to
shrink the history opts into `compress_then_retry` explicitly" — and `docs/advanced.md`
promises the same. No code path honored the opt-in, so the policy table could name an action the
loop would never take.

## Decision

1. **The reactive fold is keyed on the resolved recovery action, not on the class.** After a soft
   failure the loop reads `action` from the recovery audit it already persists, and folds only when
   that action is `compress_then_retry`. The audit and the fold therefore cannot disagree: the
   decision the operator configured is the decision the loop executes, and an operator who maps the
   class to `fail` still gets a failed turn. The same gate covers `context_overflow`: the table is
   authoritative for every reactive fold, and the default table already names
   `compress_then_retry` for that class.
2. **The class selects the trigger, not the disposition.** `context_overflow` folds with the
   existing `CompactionTrigger::Overflow`; `request_too_large` folds with
   `CompactionTrigger::Oversized`. Both carry no payload — the refusal is already in the audit,
   and the gate of ADR-072 carries the byte readings. The trigger's reason names the byte verdict
   ("provider refused the request as too large (ADR-068)") so a fold's `compact` event never
   claims a window overflow that did not happen.
3. **Defaults are unchanged.** `request_too_large` still resolves to `fail` in the default table,
   so no deployment changes behavior without an explicit rule. ADR-068 decision 1 stands; this ADR
   only makes the documented opt-in real.
4. **The fold stays bounded and observable.** It runs through the same `compact_now` path as every
   other fold: one persisted maintenance turn (`purpose: "compact"`), a `compact` event appended
   only after the summary exists, at most two folds per drain episode, and a planner that refuses
   without real reclaim. When no fold is possible the turn settles soft exactly as today.
5. **An `Oversized` fold skips the minimum-reclaim rule, like a byte fold.** `min_reclaim_tokens`
   counts projected text; the payload a byte refusal is about is the inlined artifact bytes the
   projection cannot see, so requiring the text minimum would refuse the only fold that can make
   the retried request sendable. Every range rule still holds: the keep-recent boundary, the
   frontier after the last fold, and a strictly-past disjoint range.
6. **Nothing folds pre-emptively here.** The loop still learns about this refusal from the
   provider's verdict, not from a probe: the pre-flight of ADR-072 is the preventive arm, and this
   is the arm that answers a verdict it could not predict. ADR-068 keeps the refused
   `step/request` on purpose as the record of what was not sent; a refused request costs one
   persisted envelope, never a provider call.

## Alternatives

- **Fold on the class regardless of the table.** Rejected: it would silently override an operator's
  `fail`, and ADR-068 makes history shrinking an explicit policy action. Constraint 8 puts the
  table in the caller's hands for exactly this kind of choice.
- **Drop this slice because ADR-072 prevents the refusal.** Rejected: the preventive gate compares
  a declared bound, and the cases above — a carrier limit below the declaration, no declaration, a
  fallback body — still refuse. Without a fold the session rebuilds the same body every turn and
  wedges exactly as it did in production.
- **Elide old image parts at a sliding watermark.** Rejected: any per-turn change to already-sent
  history rewrites the request prefix and invalidates the provider cache every turn (constraint 3).
  Only a `compact` event may invalidate a prefix, which is why the fix is a fold.
- **Declare `context_window` and rely on the preventive arm.** Rejected as the only fix: the
  failure was byte-bound, and pixels make bytes grow faster than tokens, so a token-window estimate
  can stay under its threshold while the serialized body is already over the carrier's bound.
- **Retry after trimming just enough messages.** Rejected: it is a silent history rewrite outside
  the event model, and the summary that makes the fold cheap would be missing.

## Consequences

- A session that meets a refusal the gate could not foresee now folds once, retries a smaller
  request and finishes, instead of failing every turn on the same bytes.
- The two byte paths are complementary and must stay consistent: ADR-072 decides *before* the turn
  from the declared bound, this ADR decides *after* a verdict. A deployment that declares a bound
  sees both; a deployment that declares none sees only this one.
- The opt-in costs one rule, but `PolicyConfig::from_yaml` **replaces** the table rather than
  merging with the defaults, so an explicit `recovery:` block must carry every rule the deployment
  relies on. The documented shape is the default table plus the one override.
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

- No body re-measurement after the fold: the retry either fits or fails, as ADR-068 says.
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
  without a window reading and is not refused by the minimum-reclaim rule.
- L2 (covered by `crates/kian-cli/tests/oversized_fold_l2.rs`): a live assembled server whose
  carrier refuses a body the declared bound did not predict (an upstream 413 under a permissive
  declared bound) folds old turns on the first refusal and answers the input with a smaller
  request; the refused `step/request` stays in the stream.
- L3: the production vision session that failed at 35,350,711 bytes completes its next turn after
  the deployment enables the rule and the shape is re-enabled; the refused envelope stays in the
  stream beside the fold that answered it.
