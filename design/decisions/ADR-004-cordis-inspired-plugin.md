# ADR-004: Plugin mechanism borrowed from Cordis; Rust uses static trait plugins (compile-time registration)

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: dsh's everything-is-a-plugin is built on Cordis (service keys / inject / typed
  events / reversible registration). Kian needs "capabilities are replaceable" (the narrow-waist
  implementation path), but Rust has neither TS declaration merging nor a runtime dynamic-plugin
  ecosystem.
- **Decision**: Borrow four concepts, statically: ① `trait Plugin { fn apply(self, ctx) ->
  Disposer }` (reversible registration); ② `Ctx`, a typed service registry (service key = trait
  object type); ③ dependencies resolved topologically at construction time (equivalent to
  inject); ④ the five event-dispatch modes (emit/waterfall/parallel/serial/bail) as a `dispatch`
  API. The plugin set is known at compile time; the `kian_plugins![...]` macro registers plugins
  into the binary. Dynamic loading (wasm/dylib) is left to.
- **Alternatives considered**:
  1. dylib dynamic loading — the Rust ABI is unstable (abi3 does not yet cover general cases);
     high risk of safety and version hell.
  2. WASM plugins (wasmtime) — good isolation; but async/streaming traits (ModelProvider's
     Stream) are costly across the wasm boundary; not worth it for v0.
  3. Pure configuration, no plugins — recovery policy tables / subscription rules go through
     configuration (Footprint Ladder level 1), but ChannelAdapter and friends require code.
- **Consequences**: Adding/removing plugins requires recompilation (acceptable for self-hosted
  deployments, as many Rust gateways do); in exchange: full compile-time type checking and a
  single binary.
- **Rollback Trigger**: Strong community demand appears — third parties installing plugins
  without touching Kian source → start a wasm plugin track (seam-level, ChannelAdapter /
  ToolProvider first).
- **Cost Impact**: Zero.
