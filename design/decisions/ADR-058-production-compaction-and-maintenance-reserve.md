# ADR-058: Production context compaction is a pure planner plus a maintenance turn, with a reserved budget share

- **Status**: accepted (owner direction 2026-11-22, approved semantics 7; P3 slice).
- **Context**: the event model has carried a compact kind and a frozen context-projection rule
  since phase 1 (EVENT-MODEL section 4.3): a summary event emits its message at from_seq and the
  folded range is skipped. But no production code ever appended one. The projection, the
  validation rules (disjoint ranges, strict past, a < b) and the recovery vocabulary
  (compress_then_retry) existed, while the only callers of the recovery runtime were tests: a
  provider context overflow was an honest soft failure and the session ended failed. Two approved
  requirements forced the gap to close. First, a real runtime must survive a heavy session instead
  of dying at the window. Second, compaction is the one deliberate prefix-invalidation point
  (AGENTS.md constraint 3), so whatever appends a compact must be a narrow, observable, bounded
  decision - never an ad-hoc reaction scattered through the loop. The same slice also has to keep
  the economics honest: compaction costs a summarization request, its benefit is a smaller prefix,
  and under cache-hit pricing an early compaction can lose money (a 200K-token context folded to
  50K needs roughly ten to seventeen turn-equivalents to pay back). A third requirement is the
  deadlock risk the owner named: if the budget can be spent to the last token on ordinary turns,
  the session may be unable to pay for the very compaction that would stop it from overflowing -
  "no money to compress -> context overflow -> even less money".
- **Decision**:
  1. **Planning is a pure function in the orchestrator.** CRATE kian-orchestrator, module
     compaction, owns CompactionSettings (keep_recent_turns, min_reclaim_tokens, trigger_percent),
     CompactionPressure, CompactionTrigger (Pressure or Overflow), CompactionPlan and
     plan_compaction(events, settings, trigger) -> Result<Option<CompactionPlan>>. It reads only the
     event stream and the settings: no clock, no store, no price book, no model. It folds the
     oldest turns, keeps the most recent K turn/start boundaries verbatim, starts after the last
     compact event, refuses a fold whose estimate is below min_reclaim_tokens, and returns an
     explicit reason. A plan is a value: the caller cannot accidentally fold twice or fold a range
     that is not strictly in the past.
  2. **Two triggers, both evidence-driven; never cost-driven.** The preventive arm fires only when
     the bound profile declares a context_window and the worst case of the next request - projected
     context estimate plus the declared output cap - reaches trigger_percent of that window. A
     profile with no window never compacts preventively. The reactive arm fires when the provider
     itself answered with context overflow (the authoritative reading) and needs no window at all.
     The runtime never compacts because it is merely expensive: compaction is the only
     cache-byte-invalidating action, so paying for a fold before the context is actually under
     pressure trades a certain summary cost for a speculative cache win. At 90 percent of a
     declared window and with a real reclaim threshold, both arms are answering a measured
     pressure, not a guess.
  3. **The summary is produced as an ordinary persisted maintenance turn.** The loop appends
     turn/start with purpose compact, then a step/request with purpose compact whose body is the
     exact summarizer request, then - only after the summary text exists - the compact event. It
     uses the session's bound slot and provider, one request, no fallback traversal, and does not
     fire the turn/request or turn/response hook ports: a maintenance turn is not a model turn and
     must not be routed through hooks that assume one. The summarizer request is frozen (system
     frame plus a bounded transcript), so production is deterministic and replayable; the
     transcript is character-bounded so a single huge tool result cannot make the maintenance
     request itself overflow.
  4. **The compact event is written only after success, and failure stays accountable.** A failed
     or empty summary leaves the projection untouched and is reported through the existing soft
     failure path (audit class plus recovery disposition); the failed request is still a
     step/request in the stream and is still priced as a request. The compact payload keeps its
     frozen keys (from_seq, to_seq, summary) and adds audit-only keys: produced_by.step_seq names
     the request that produced the summary, reason names the trigger, folded_messages and
     reclaimed_tokens record the fold's size. The summary content is {"text": ...}, the same shape
     a model assistant message uses, so context derivation needs no second grammar.
  5. **Compaction is bounded per episode.** At most two folds run in one drain episode, and a fold
     resets the iteration loop over the now-smaller context instead of issuing the original request.
     Together with the planner's min_reclaim_tokens and keep_recent_turns, no path can loop folding
     forever, and the recovery policy never sees an unbounded retry. If a third fold would be
     needed, the session's normal provider error surfaces honestly.
  6. **Ordinary turns reserve a maintenance share; maintenance draws it.** The budget projection
     gains an additive method set over the same event inputs: allowance (limit plus raises),
     maintenance_reserve_tokens (ten percent of the allowance) and
     would_exceed_tokens_reserving(estimate, reserve). The ordinary request gate subtracts the
     reserve, while the maintenance request uses the full allowance: exactly enough room is kept
     for the compaction that prevents the overflow deadlock, and nothing more. Unbudgeted sessions
     never reserve. Wall-clock budgets are not gated here (they remain audit and operator
     information; idle waiting must not burn them).
  7. **Observability follows the purpose, not the event count.** The prefix-cache checker compares
     only ordinary requests (purpose absent or turn) and lets the chain span maintenance requests
     untouched, so a maintenance body can never be reported as an unexpected prefix invalidation.
     The cost projection reports maintenance requests and their priced subtotal apart from turns
     while still including them in the total. The maintenance request and its compact event are
     visible as their own events; no side table exists.
- **Alternatives considered**:
  - A separate compaction service or scheduler: rejected. It would be a second execution path with
    its own liveness, and compaction must interleave with the drain's event writes to stay
    crash-safe.
  - Compacting proactively whenever the estimated cost of the next request is high: rejected by
    the cache economics above; it also cannot be expressed without a price book, which the core
    must not read.
  - A provider-specific summarization call outside the persisted request path: rejected by
    persist-what-you-send. The summarizer request is model-visible input and must be an event
    before it flies.
  - Storing the summary outside the compact event or referring to it by step seq only: rejected.
    The event stream is the sole truth; the compact event is the context-bearing artifact, the
    produced_by pointer is provenance.
  - A hard trigger on the raw token count of the last request: not universally available and
    defeated by a large output cap; the planner uses the same chars estimator family as the plan
    (ADR-054) and the declared cap so every dialect behaves the same.
- **Consequences**:
  - A long session now survives the declared window: the raw history stays intact in the stream
    (history is immutable), while the projection folds old turns behind one truthful summary.
  - Cost line items gain maintenance: more requests in the stream, but each one is a persisted,
    priced, replayable request instead of an invisible side effect.
  - Operator surfaces tell the purpose apart: cache metrics do not flag maintenance bodies as
    cache regressions, and cost separates maintenance from turn spend.
  - Failure semantics do not change: a failed summary is a soft failure with the usual recovery
    disposition, and no compact event is fabricated.
- **Limits (recorded, not hidden)**:
  - The planner does not evaluate a price snapshot or a break-even point; it compacts on context
    pressure only. A future price-aware planner must decide against pinned prices, not the current
    book.
  - The summarizer always uses the session's main slot; there is no compact_slot override yet.
  - The maintenance request has no fallback traversal: a provider outage during a fold is a soft
    failure, not a retried maintenance turn.
  - The estimator stays the chars_v1 family; it is a bound, not a tokenizer.
  - Wall-clock budget raises and reserves remain inert for this gate.
- **Acceptance**: state and projection tests for the new trigger/plan shapes; unit tests that the
  planner is pure and repeatable and refuses malformed history, absent pressure, short history and
  small reclaims; loop tests that the preventive arm folds before a request and persists purpose
  compact, that the reactive arm recovers a provider overflow and retries the smaller request, that
  the reserve parks an ordinary turn and a budget raise resumes it, and that a session with no
  declared window never compacts; CLI tests that maintenance requests do not join the prefix chain
  and are priced apart. All gates green.
