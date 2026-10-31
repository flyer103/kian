# ADR-001: Rust + tokio + sqlite-first foundation

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: Kian is a long-horizon reliable execution substrate. Its core load is event
  appends, projection replay, CAS/fencing and streaming model calls; it must deploy as a
  self-hostable single binary (Temporal's operational surface, but lighter).
- **Decision**: Rust + tokio async runtime; storage starts with sqlite (single-machine,
  zero-dependency), Postgres as the v1 multi-replica path; API via axum (HTTP/JSON-RPC + SSE).
- **Alternatives considered**:
  1. Go (one of the owner's default languages) — mature ecosystem, fast development; but GC tail
     latency and long-running drains with resident memory (projection caches, stream buffers) are
     unfavorable; the type system is weak for encoding state machines like the six invariants.
  2. TypeScript (aligning with the dsh/Cordis ecosystem) — same stack as the integrated party is
     smoothest; but runtime performance and the deployment surface (node_modules) work against a
     "single-binary endpoint daemon".
  3. Rust but async-std/smol — the ecosystem has converged on tokio; no reason to deviate.
- **Consequences**: Development speed is lower than Go/TS; in exchange: no-GC tail latency,
  strongly-typed invariant encoding, and kiand cross-compiled as a single binary. dsh integration
  goes through a protocol, not in-process.
- **Rollback Trigger**: If Rust development efficiency becomes a milestone bottleneck (Phase 1
  exceeding 2× the estimate) while core semantics are already stable, evaluate keeping the core
  in Rust with the super-agent bundle integrating via API in another language.
- **Cost Impact**: Zero license cost; sqlite has zero infrastructure cost.
