# ADR-016: Memory is event-sourced — notes are events, dreaming is a drain, injection is a creation-time synthetic input

- **Status**: accepted (owner gate, 2026-11-07)
- **Date**: 2026-11-07
- **Gate**: approved 2026-11-07; four confirmed decisions: (1) a new core kind rather than the
  `memory_write`-tool fallback, (2) memory scope = Agent identity, (3) injection as `role:system` +
  `synthetic:true`, (4) Phase 1 is explicit `kian dream` only (no automatic cadence).
- **Context**: Kian persists every model-visible byte (constraint 2) and treats the event stream as
  the sole truth (constraint 1), but it has no cross-session memory: sessions end and their
  transcripts are only reachable through `RecallPort` by an agent that already knows what to ask
  for. The Anthropic "components of agent memory" article (2026-11) shows the missing layers —
  memory notes distilled from transcripts, offline consolidation ("dreaming"), linked
  representations, and a small index injected at session start — and measures both the gains and
  the failure modes. Two independent inputs converge on long-range memory as the scaling bottleneck
  before sharding. The design question is not *whether* to add memory, but *where memory lives*:
  the article's file-folder memory store is an artifact of a substrate without a durable log; Kian
  has one.
- **Decision**: memory is event-sourced, in three commitments:
  1. **A note is a durable, non-context event** (`memory/note`, a new core kind) in the stream of a
     per-memory-scope **memory session**. Notes are immutable; the effective note is the
     latest-per-subject fold. A note carries its derivation (`derived_from`) and verification state
     (`status` + `verified_by`) as first-class fields, so provenance is structure, not prompt.
  2. **Dreaming is an ordinary drain of the memory session**, triggered by a durable reserved
     inbox row naming a bounded source range. It reads the source read-only, never appends to it,
     and inherits fencing, crash recovery, budget and audit from the existing substrate.
  3. **Injection is one `input/admitted` event with `synthetic:true` at session creation**, carrying
     only `verified` notes, hard-capped (index-only over cap), persisted before the request flies
     (constraint 2) and strippable (A6). Never on resume, never mid-session, never a rewrite of live
     context (constraint 3).
  The memory **scope is the Agent identity**, not a session or a thread. Core owns the kind plus a
  pure `project_notes` fold; the dreamer prompt, cadence and caps live in a bundle crate.
  Design contract: [design/MEMORY-MODEL.md](../MEMORY-MODEL.md).
- **Alternatives considered**:
  1. **A file-folder memory store (the source article's model).** Rejected: authoritative state
     outside the event log (constraint 1); no replay, no audit, no cache reasoning; and the
     article's own concurrent-write result — 41 of 57 contract facts surviving — is a data-loss bug
     that a shared mutable store invites. A file projection may still exist as a *disposable view*,
     never as truth.
  2. **Notes as a `memory_write` tool call in a memory session (zero new kinds).** Considered
     seriously under the Footprint Ladder, and it would keep the core kind set at 21. Rejected in
     Phase 1 for two structural reasons: `tool/call` is context-bearing, so every note ever written
     re-enters the dreamer's own context and grows without bound (the memory session's cost is the
     opposite of what memory is for); and the note schema would live in tool arguments rather than
     the event contract, escaping the additions-only and golden enforcement that make a durable
     fact auditable. Precedent for a non-context data kind already exists (`input/spilled`). This
     remains the documented fallback if the core kind is refused at the gate.
  3. **Memory as a projection only (no note events).** Rejected: a projection needs an authoritative
     input; without events it becomes a second store, which is the same violation as (1) in
     disguise.
  4. **Mid-session memory updates.** Rejected: appending memory to a live session rewrites future
     requests and invalidates provider prefix cache (constraint 3). A session pins the memory it
     started with; new sessions see new memory.
  5. **Rewrite-in-place note status (UPDATE semantics).** Rejected: forbidden by append-only
     (constraint 1); status changes are new revisions with the same subject.
- **Consequences**:
  - The durable kind set grows 21 → 22 (`memory/note`), so EVENT-MODEL, the additions-only golden
    and the docs-map drift test move together in the landing change; this ADR is the justification
    required by EVENT-MODEL §6.2.
  - Memory is replayable, exportable and auditable like any other session data; `session export`
    already carries it, and `RecallPort` reaches notes with no new search index.
  - Injection introduces a new prefix byte at session creation. Because it is the first event of a
    session, no existing session's prefix changes; the renderer becomes frozen content.
  - The architecture answers three of the article's reported failure modes structurally rather than
    with prompting: concurrent shared-page writes (one writer per stream), stale/incorrect notes
    (append-only corrections with recorded evidence), and unbounded memory (caps + disposable
    representations).
  - Cost: dreaming consumes model compute, budgeted like any drain; injection costs a bounded prefix
    (default ≤ 4 KiB / 20 items) once per session.
- **Rollback Trigger**: Phase 1 — stop dreaming; notes are inert events with no reader, and no
  context byte has changed. Phase 2 — a config flag turns injection off, restoring byte-identical
  sessions. Phase 3 — representations are projections and can be dropped. No rollback path requires
  deleting or rewriting events.
- **Cost Impact**: Zero on the steady-state execution path when no agent configures memory (no
  event, no byte). With memory: one bounded injected prefix per new session (index-only, default
  4 KiB) plus offline dreaming compute, both observable through existing usage metrics.
