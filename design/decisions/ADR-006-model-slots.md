# ADR-006: Declarative model slots (slot → provider/model/fallback), immutable for a session's lifetime

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: P6 requires "model-selection determinism in Kian, access resilience in the
  gateway". A3 requires stable prefix bytes: any change to historical context invalidates the
  provider prefix cache. With free runtime model switching, cache hits become unpredictable.
- **Decision**: An Agent definition declares model slots (e.g. `main`/`router`/`cheap`); each
  slot = provider + model + an ordered fallback chain. The slot binding is immutable for a
  session's lifetime (switching models = an explicit new session or an explicit migration event).
  Fallback switches emit durable events (observable).
- **Alternatives considered**:
  1. Runtime dynamic model selection — cache-byte stability cannot be guaranteed; cost governance
     (A3) fails.
  2. Per-request model choice (router-style smart routing) — that is the gateway's job (access
     resilience / quality routing); Kian owns only deterministic binding; the two layers' duties
     are separate (P6 verbatim).
- **Consequences**: Model-switch inflexibility is deliberate (predictable cost > flexibility); an
  explicit migration event keeps an escape hatch. The super-agent's Router slot (cheap fast
  model) and Worker slot (strong model) are naturally expressed by this mechanism.
- **Rollback Trigger**: If real workloads prove the need for multi-model alternation within one
  session (e.g. hybrid reasoning), extend to "slot groups" rather than opening free switching.
- **Cost Impact**: Positive — a predictable cache hit rate means predictable cost (DS cache hits
  are ~30× cheaper).
