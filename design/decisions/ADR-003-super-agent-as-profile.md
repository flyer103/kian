# ADR-003: Super-agent is a profile/bundle composition pattern, not a core capability

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: The DeepSeek article gives the four super-agent traits (continuous / efficient /
  proactive / persistent) and five modules (two-layer architecture / event-driven / human-facing
  interaction / compute-state-environment separation / watchdog), but explicitly says "there will
  be different variants in concrete deployment". Kian axiom A10: a narrow-waist core, new
  capabilities grow at the edge.
- **Decision**: The super-agent reference architecture lives entirely in the `kian-superagent`
  bundle: Router session template (cheap slot) + Worker sessions (derived via Thread delegation) +
  subscription rules (including passthrough) + channel output policies (IM ≤100 words etc.) +
  kiand endpoint + watchdog event source. The core provides only the primitives it reuses:
  Thread, the event-subscription Admit entry, the ExecBackend seam, the health event source.
- **Alternatives considered**:
  1. Router/Worker layer built into the core — freezes one organizational pattern as an invariant
     structure; every other super-agent variant (multi-Router sharding, different channel mixes)
     would need core changes.
  2. A separate project layered on the Kian API — viable, but loses template reuse and bundle
     distribution convenience; the compromise is a bundle crate, not a separate repo.
- **Consequences**: The core carries no super-agent semantics; super-agent evolution (e.g. the
  article's §8.1 scaling questions) never impacts the core; the cost is that the bundle maintains
  its own template versions for the composition logic.
- **Rollback Trigger**: If most real deployments need core changes to express super-agent
  variants, the primitives are insufficient — promote the *missing primitives* (not Router/Worker
  themselves) into the core with a new ADR.
- **Cost Impact**: Zero.
