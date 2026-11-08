# ADR-019: Governed memory — scoped reads, explicit grants, and budgeted consolidation

- **Status**: accepted (owner gate, 2026-11-08)
- **Date**: 2026-11-08
- **Related**: `design/MEMORY-MODEL.md` (§2 terms, §6 injection, §7 retrieval, §11 open questions,
  §6 I10), `design/decisions/ADR-016-memory-as-events.md`, `docs/SPEC.md` P8/P9,
  `design/READ-MODEL.md` (truth stays the stream).
- **Context**: P1 (notes + dreaming), P2 (creation-time injection) and P3a (read-time search/map)
  are landed and live-verified. A real-provider A/B showed memory recovering factual recall
  (0/8 without memory vs 8/8 with the injected index) at roughly 10x prompt tokens on small
  sessions — memory is valuable but its cost and its **scope** are now the product questions. The
  current posture is permissive by construction: `memory.session` is server configuration, so any
  new session created while injection is on receives that index, and the only cross-scope guard
  (the identity check) is defense-in-depth that cannot fire at the creation seam
  (MEMORY-MODEL §6 I10). Automatic consolidation ("cadence") and agent-side memory reads are the
  next capabilities; both are cost-bearing and cross scope boundaries. Without a governance model
  they turn an auditable, bounded feature into an opaque one — the opposite of kian's positioning
  (audit, cost control, governance as default).
- **Decision**: memory is governed by four rules, all config-first, all fail-closed, no new event
  kind and no new durable table.

  1. **Scope binding.** A memory scope is an Agent identity. A session reads only its own agent's
     memory by default. `memory.scope` names the owning agent and `memory.session` names that
     scope's memory session; injection is permitted only when the new session's agent **is** the
     scope (the P2 identity check becomes an enforcement rule, not merely defense-in-depth).
  2. **Cross-scope reads require an explicit grant.** Grants are declarative config, resolved at
     boot and failing closed on an unknown agent or scope:
     `memory.grants: [{reader_agent: <agent-id>, scope: <agent-id>}]`. A grant-bound read is
     allowed for (a) injection into a session of `reader_agent`, (b) the read-only CLI verbs when
     the operator names a scope (`--scope`), and (c) the future agent-side tool. Every granted read
     records its provenance (the `memory` additive key already carries scope/source ids; the
     metrics carry a `granted_reads` counter). No grant = own scope only.
  3. **Consolidation cadence is a declarative, budgeted policy table, off by default.**
     `memory.cadence: {idle_after_ms, min_interval_ms, max_runs_per_hour, daily_budget_tokens,
     max_source_bytes}`. Each run is admitted as the existing durable dream row (one request per
     `(source, range)`, idempotent); the driver is one-shot guarded, consumes the declarative
     budget with refund semantics (constraint 8), and an over-budget run is **skipped honestly**
     (counter + log), never silently truncated or retried unboundedly. Cadence off = today's
     explicit-only behavior.
  4. **Retention and export.** Notes are never deleted; `stale`/superseded revisions fold out of
     injection and the map but remain in the stream and in `session export`. Memory sessions are
     exportable/importable like any session. No memory content is copied between stores: the
     stream is the sole truth (constraint 1), and the snapshot/view trust boundary of
     `READ-MODEL.md` §5 applies unchanged.

  Enforcement lives at the existing points — server injection (`create_session`), the CLI read
  verbs, and (when it lands) the agent tool — with honest refusals and counters. No new kind: a
  grant is configuration plus audit on the reads it authorizes; if grants must change at runtime
  without a restart, that is a later decision (see open questions).
- **Alternatives considered**:
  1. **Open memory (any agent reads any scope).** Rejected: breaks agent isolation; a poisoned or
     confidential note would leak across scopes, and channel-derived transcript content could seed
     another agent's memory. It also makes the P2 "reachable consequence" (any fresh session
     receives the configured index) the intended behavior rather than a documented edge.
  2. **Per-session memory only.** Rejected: crossing the session boundary is the point of memory;
     per-session state already exists as the event stream.
  3. **An external memory store or vector database.** Rejected: a second source of truth
     (constraint 1), with its own consistency, audit and deletion semantics that the event stream
     would then have to reconcile with.
  4. **Automatic cadence on by default.** Rejected: an unbounded background cost surface; kian's
     cost axiom (constraint 3) and recovery boundedness (constraint 8) require an explicit,
     budgeted, observable policy and a default of off.
  5. **Grants as a new event kind.** Rejected for now: config + read provenance/counters cover the
     audit need without an event-model change; a durable grant-change event is only justified when
     grants must be mutated at runtime and replayed as authority (see open questions).
- **Consequences**:
  - `memory.session` injection becomes agent-scoped: a session whose agent is not the scope gets
    nothing unless a grant names it. P2's documented reachable consequence (a fresh session
    receiving the index) is closed for non-scope agents and made explicit for granted readers.
  - Cadence adds a background cost surface; the daily token budget is the bound, and every run is
    a durable dream row, so cost and effect stay auditable and replayable.
  - Agent-side memory reads must carry the grant and stay capped/framed (`data, not instructions`)
    and audited as ordinary `tool/result`s.
  - No new kind, no new table, no event-byte change; default behavior with no `memory:` section is
    byte-identical. The work lands as config schema + enforcement + tests, then the cadence driver.
- **Rollback Trigger**: grants absent = own-scope only (today's posture); cadence absent = explicit
  dreams only (today); the config block can be dropped with no durable-state consequence. No
  migration to reverse.
- **Cost Impact**: cadence is the only new steady-state cost, bounded by `daily_budget_tokens` and
  `max_runs_per_hour`; grants add a config lookup and one counter. No cost when the feature is off.
- **Open questions** (recorded, not silently dropped):
  1. Runtime grant changes without a restart: config reload vs a durable grant event (would need
     the EVENT-MODEL change protocol).
  2. Grant granularity for the agent tool: per-call permission provenance (ADR-015 style) vs a
     config grant at assembly.
  3. Retention horizon: stale-note folding is not deletion; whether a scope may ever reclaim
     storage (export-only retention, READ-MODEL §7) is a separate decision.
  4. Multi-tenant scopes: a scope is an agent id today; a tenant-scoped model needs its own ADR.
