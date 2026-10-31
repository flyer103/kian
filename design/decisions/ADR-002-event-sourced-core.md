# ADR-002: Event-sourced core (append-only events + rebuildable projections)

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: A1 (the event stream is the sole truth), A2 (sent bytes persisted; byte-identical
  replay) and A9 (intentional end vs crash distinguishable) all require "state = a projection of
  events"; audit = replay is the moat claim in the BP.
- **Decision**: The EventStore is append-only and never modified; every state read goes through a
  projection (`project(events) -> State`); session state reads from scattered columns are
  forbidden (the NoHalfTerminated corollary); projections are disposable and rebuildable.
- **Alternatives considered**:
  1. Direct state tables (CRUD + checkpoints) — intuitive to implement; but auditing needs
     after-the-fact instrumentation, crash-recovery semantics are weak, and a "write succeeded but
     transition failed" half-termination cannot be represented.
  2. State tables + dual event writes — dual-write consistency costs more than pure projections,
     and creates two sources of truth.
- **Consequences**: The read path gains a projection layer (mitigated by incremental projection
     caches); in return, auditability/reproducibility/migratability are the product claim itself.
- **Rollback Trigger**: None — this is the direct implementation of A1; rolling it back equals
  abandoning the product positioning.
- **Cost Impact**: Storage footprint = event-stream size (including sent bytes); affordable on
  sqlite/postgres; compression/archival policies are handled at the store layer.
