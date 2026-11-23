# ADR-054: Request plans are values — declared model profiles, one planner, persisted fallback bytes

- **Status**: accepted (owner direction 2026-11-22; phase 1a slice).
- **Context**: kian already declared slot capabilities as config (`model_profiles:`), but the loop still
  built every request body inline from a hard-coded openai-compat shape. Three consequences were
  visible. (1) A deployment could not say "this model thinks at high effort" or "this model has no
  reasoning mode" without provider-specific code; the loop silently inherited provider defaults, so
  the same kian session behaved differently on different endpoints. (2) On failover the provider turn
  rebuilt the body for the fallback target and persisted only `slot/fallback {slot, from, to}` — the
  exact bytes a retry sent were not reconstructible from the stream, which is the one place
  persist-what-you-send was still aspirational. (3) Reasoning replay was unconditional: a fallback
  target without a reasoning channel received `reasoning_content` it never produced, and a
  reasoning-disabled model could not be declared at all. The phase-0 price book made the cost of a
  cache prefix visible; this phase makes the bytes that create that prefix auditable.
- **Decision**:
  1. **A model profile is declared data, resolved once.** `model_profiles:` maps
     `provider/model` to dialect, context window, max output, reasoning/thinking/effort,
     strict, cache and vision flags. Agent assembly resolves a slot's primary and every fallback
     target to `ModelProfile`; `SlotSpec` and `SlotTarget` carry it, so the session's
     `slot/binding` event and every `slot/fallback` target record the profile that was in force.
     The binding stays fixed for the session lifetime (ADR-006, constraint 11); changing a profile
     is a `slot/migration`, never a live mutation.
  2. **One pure planner produces one request plan per attempt.** `build_request_plan` owns the body
     bytes, the body version (`v1`/`v2`), the dialect, the profile digest, the body SHA-256 and a
     token estimate. The loop persists the plan's metadata, the provider sends the plan's body, the
     cache checker compares the plan's prefix, and the audit reads the same value. No other code path
     may build model bytes.
  3. **V1 stays frozen; V2 is explicit.** A slot with no profile uses the existing
     `build_chat_body` and `FROZEN_CHAT_BODY_SHA256` byte-for-byte. A slot with a profile uses the
     V2 builder: same base shape plus `thinking` (only when declared), `reasoning_effort` (only
     when declared) and `max_tokens` (only when `max_output` is declared). V2 is deterministic
     compact JSON with lexicographic keys, like V1. No `strict` field is emitted in this slice —
     the tool-schema seam does not carry it yet; the profile flag is recorded for the phase that
     adds it.
  4. **Reasoning replay follows the declared profile.** With no profile, the existing replay is
     unchanged (V1 legacy). With a profile, `reasoning_content` is preserved verbatim only when the
     model declares reasoning and thinking is not explicitly disabled; otherwise it is stripped from
     every message before the body is built. Stripping is applied consistently to all messages, so
     the prefix is stable across turns.
  5. **A fallback's bytes are persisted before the fallback is called.** `complete_turn` builds the
     plan first, adds `body`, `body_version`, `dialect`, `profile_digest`, `body_sha256` and
     `estimate` to the `slot/fallback` payload, persists it, and only then dials. A crash between
     persist and dial is reconstructible; a retry sends the recorded bytes, not a fresh guess.
     `step/request` carries the same plan metadata additively.
  6. **The estimate is labeled, never a promise.** `chars_v1` derives a low/high band from the
     serialized body length (plus an overhead floor) and reports `max_output_tokens` separately.
     It is audit metadata; the phase-1b budget gate uses the high bound as its worst case. No
     consumer may present it as an exact token count or a price.
- **Alternatives considered**:
  1. **A DeepSeek-specific adapter that injects thinking/effort** — rejected: the narrow waist keeps
     provider names out of core, and every reasoning-capable provider uses the same three wire fields.
  2. **Change V1 to always send `thinking`/`max_tokens`** — rejected: it rewrites the frozen prefix
     for every existing session and invalidates provider cache for no declared capability.
  3. **Persist only the fallback body, not the plan metadata** — rejected: the body alone cannot say
     which dialect/version produced it, and replay would have to infer the profile from live config,
     which may have changed.
  4. **Carry a live `ModelProfile` into the provider seam** — rejected: the provider seam stays
     `{slot, body}`; the body is already the exact contract and the profile is recorded at binding.
  5. **Emit `strict` now** — deferred: `ToolSchema` has no strict bit and changing its many struct
     literals is a separate seam change; the profile flag is parsed and recorded so the emitter can
     land without a grammar change.
- **Consequences**:
  - `slot/binding` and `slot/fallback` payloads gain additive `profile` / plan keys; legacy
    payloads (and V1 request bodies) stay byte-identical because `profile` is skipped when absent.
  - a session that declares no profile behaves exactly as before; a session that declares one gets a
    distinct, stable prefix, and a profile change is an explicit migration event.
  - the loop no longer rebuilds a body for the primary; the same plan feeds persistence, the
    provider call and the audit. The fallback path now has the same guarantee.
  - the estimate can be wrong; it is marked `chars_v1`, and the budget gate must use its high bound
    plus `max_output` rather than treating it as exact.
  - no new event kind, no API wire change, no provider-specific code in core.
- **Rollback Trigger**: a V1 request byte changing for a profile-less slot; a fallback attempt whose
  persisted body differs from the bytes sent; or a profile-bearing session whose prefix changes
  without a `slot/migration` or `compact` event.
- **Cost Impact**: one new `kian-llm` planner module, two additive core fields on slot spec/target,
  additive payload keys on three event kinds, one config field group; no new crate, no new event
  kind, no change to the frozen V1 hash.
