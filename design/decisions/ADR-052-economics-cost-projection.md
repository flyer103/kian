# ADR-052: Economics as a projection — a config price book, not a priced event

- **Status**: accepted (owner direction 2026-11-22; phase 0 slice).
- **Context**: the provider bill is dominated by two facts the event stream already observes but the
  projections did not surface. (1) A cache-hit input token is 50× cheaper than a miss on
  `deepseek-flash` (30–50× on other current providers), and the provider reports
  `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`; `kian usage-stats` read neither, so a
  session with a 90% hit rate and one with none rendered the same cache-read total (zero). (2) Peak
  and off-peak prices differ by 2× on a weekday wall-clock schedule, and no projection could answer
  "what did this session cost". At the same time `kian cache-stats` is a byte-prefix *proxy*, not a
  provider hit count: it proves stability, not the billed number. The economics of a durable agent
  are therefore invisible exactly where kian claims an advantage — long sessions that keep a stable
  prefix. Separately, a provider 422 (a malformed request, e.g. a missing `tool_call_id`) fell
  through to `Unavailable` and triggered slot failover: a deterministic client error burned the
  fallback budget and could fail a second provider for the same bad bytes.
- **Decision** (phase 0, read-only):
  1. **The price book is configuration, not core state.** A top-level `pricing:` section holds
     decimal prices per million tokens (off-peak base), currency, a peak multiplier, peak windows
     (weekday + minute range + IANA timezone) and holiday dates. Boot parses and validates it
     fail-closed; the runtime normalizes every price to integer micro-USD per million tokens once,
     so the projection does exact integer arithmetic thereafter. Prices are not embedded in the
     binary: they are a deployment fact that changes independently of kian.
  2. **Cost is a pure projection over committed events.** `kian cost` folds `turn/end.usage` and
     attributes each usage object to the attempt that produced it (the latest `step/request`, or the
     `slot/fallback` target when one fired), evaluating the peak window at that attempt's `ts`.
     Nothing is written: no event, no column, no live counter. The truth stays the event stream and
     the config; a price edit re-renders history at the new price, which is honest (the report
     names its currency and whether a book was present) and avoids a second authoritative copy.
  3. **Usage learns provider cache spellings.** The projection reads DeepSeek's
     `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`, OpenAI's
     `prompt_tokens_details.cached_tokens`, and Anthropic's `cache_read_input_tokens`; a new
     `cache_miss_input_tokens` total is derived by a documented rule (explicit miss wins; Anthropic
     input is the miss; OpenAI/DeepSeek miss = input − hit), and the cache hit rate is computed at
     render time, never stored. The `session_usage` view gains the same field, so an old view row
     no longer decodes and the honest full-scan fallback runs (READ-MODEL discipline).
  4. **Capability profiles are declared now, enforced next.** `model_profiles:` maps
     `provider/model` to dialect, context window, max output, reasoning, vision, strict and cache
     flags. Phase 0 parses and validates them; the request planner (phase 1) is their first
     consumer. Keeping the declaration in phase 0 means the phase-1 change is behavior, not a new
     config grammar.
  5. **422 is an invalid response, not provider down.** A 422 with context-overflow markers classifies
     `ContextOverflow`; any other 422 classifies `InvalidResponse`. Neither is failover-eligible, so
     a bad request no longer spends a fallback attempt. In-band error objects follow the same rule.
  6. **No budget gate in this phase.** A budget gate needs a pre-send estimate and a durable
     `awaiting_budget` end; that is phase 1 and is specified separately. Phase 0 is deliberately
     read-only so the numbers can be validated before anything is allowed to refuse work.
- **Alternatives considered**:
  1. **Persist `cost_micros` on `turn/end`** — rejected: it makes a derived number authoritative,
     duplicates the price table into the stream, and silently freezes a deployment's old prices into
     a session that may outlive them.
  2. **A DeepSeek-specific adapter that prices in the provider** — rejected: the narrow waist keeps
     provider names out of core, and every provider has cache/peak semantics; the projection is
     provider-neutral and the config names the prices.
  3. **A price-table digest per request** — deferred: it would pin history exactly, but phase 0 does
     not need it and the digest adds an event field before the budget gate decides its shape. The
     phase-0 report names the price book by its currency and presence, not by hash.
  4. **Estimate before send in phase 0** — deferred to phase 1 with the budget gate: an estimate is
     only useful when something acts on it, and an estimate displayed without a gate invites false
     precision.
- **Consequences**:
  - `kian usage-stats` now distinguishes hit from miss and renders a hit rate; `kian cost` answers
    the per-session and per-store bill, with unpriced models counted separately rather than priced
    at zero.
  - the event schema is unchanged (no new kind, no new field on a frozen envelope); the only wire
    surface touched is the additive nested `cache_miss_input_tokens` in the metrics `usage`
    aggregate, which API-CONTRACT §6 already permits (additive nested counters).
  - a config with `pricing:` absent behaves exactly as before; a config with a malformed price book
    refuses boot instead of silently costing zero.
  - the report's cost is only as honest as the price book: an unpriced model is `unpriced_requests`,
    not $0, and a turn with no `step/request` is `unknown`. No estimate is shown in phase 0.
  - phase 1 will add the request plan, the pre-send estimate and the budget gate; the approved
    semantics (session-lifetime budget, `awaiting_budget`, `control/budget`, `budget/raised`) will
    be recorded in SPEC/DESIGN and land with their own acceptance tests.
- **Rollback Trigger**: a price book that cannot be validated without provider-specific branching, or
  a cost projection that disagrees with the provider's invoice on a known-input session by more than
  the documented rounding (one micro-USD per component).
- **Cost Impact**: one new CLI module pair (`pricing.rs`, `cost.rs`), two additive config sections,
  one additive usage field and one classification arm; no crate, no event kind, no request-byte
  change.
