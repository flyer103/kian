# ADR-029: Multiagent — child-session delegation, and the board as a protocol rather than a table

- **Status**: proposed (design record, Round 14; docs-only — nothing in this ADR is implemented,
  no test or live evidence exists yet). It is written ahead of the landing change on the
  [ADR-012](ADR-012-delegation-mandate.md) precedent, and it records *why*, not *what*: the
  normative rules live in [design/MULTIAGENT-MODEL.md](../MULTIAGENT-MODEL.md).
- **Date**: 2026-11-15
- **Deciders**: owner scope ask (session-internal multiagent; cross-session composition; an explicit
  ruling on whether a kanban-like durable board is needed), architecture (`R14` decision log D1–D8).
- **Related**: [ADR-012](ADR-012-delegation-mandate.md) (Mandate — unchanged, reused),
  [ADR-020](ADR-020-durable-worker-binding.md) (durable binding + the two-copy payload discipline),
  [ADR-021](ADR-021-runtime-worker-membership.md) (the catalog-in-the-schema supersession that this
  ADR obeys for the roster), [ADR-017](ADR-017-read-model-and-snapshots.md) (read model / views),
  [EVENT-MODEL](../EVENT-MODEL.md) §3/§4.2/§6, [READ-MODEL](../READ-MODEL.md) §2/§6,
  [WORKER-MODEL](../WORKER-MODEL.md) §5, [DESIGN](../DESIGN.md) §4.1/§4.7/§6.3,
  `docs/SPEC.md` §5.

## Context

Kian has a durable, context-isolated delegation primitive (`Thread`) used by the Router/Worker pair,
and it has no multiagent *contract*: nothing states what a child may do, what a parent may rely on
across a restart, whether a "named agent" identity exists outside a session, or whether cross-session
work needs the durable board that the reference implementation (profiles + a kanban board) uses.

Three facts force the shape of the answer:

1. **Slot immutability decides the mechanism.** Model slots are fixed per session (AGENTS.md
   constraint 11), so a child with a different model is necessarily a different session. A
   same-session child would also place child messages in the parent's stream, and every
   message-bearing kind is context-bearing (EVENT-MODEL §3, §4.3) — isolation by filter would be a
   context change, refused by EVENT-MODEL §4.2 and constraint 3.
2. **A spawn is an effect with an unrecorded window.** `tool/call` is appended log-before-execute and
   the child thread id is currently minted randomly, so a crash in between is indistinguishable from a
   spawn that never happened — the classic "effect committed, record absent" problem that durable
   execution engines solve with stable identity plus reconciliation.
3. **A board and a truth store are different objects.** The existing board-like surface is explicitly
   observability-only (`ticket_stage` is a disposable projection; no control read may consume a
   view). A mutable task row would be a second source of truth, forbidden by constraint 1 and
   READ-MODEL R1 — so the only real question is *which facts a board cannot infer*, not whether to
   store rows.

The reference implementation's delegation is process- and session-tied, and its board is a SQLite row
per task. Kian cannot copy the first (it would give up its thesis) and must not copy the second (it
would fork truth).

### Grounding (primary sources, by identifier)

The three clusters the rulings rest on — durable execution and replay, the effect-committed/record-absent
window, and delegation isolation plus board-versus-projection — each have primary literature. These nine
identifiers were resolved and their titles matched on 2026-11-15; they are the citation subset this ADR
keeps so that the *why* survives in the repository. `the design notes` remains the home for the
fuller synthesis and is deliberately untouched in this round.

**Cluster 1 — durable execution is replay plus effect control (D1, M4).**

| Id | Title | What it establishes, and where it lands here |
|---|---|---|
| arXiv:2608.03836 | *Resume Means Resume: A Machine-Checked Conformance Contract for Checkpoint, Interrupt, and Resume Semantics in Workflow Persistence Layers* | Consume-once holds sequentially and fails under concurrent delivery: k processes resuming one parked interrupt fire the gated effect k times. → the fence must be the claim/epoch, not the checkpoint (M4, §2.1); a derived id alone is not exclusivity. |
| arXiv:2611.06924 | *Replay Is a Family of Assertions: Fork-Safety in Event-Sourced Agent Runtimes* | A fixed-log fold can be deterministic while reactive settlement stays schedule-sensitive, and a prefix copy cannot undo an action in a discarded suffix. → "replay is byte-identical" is a claim about the fold, not about side effects; derivation is applied per branch (constraint 13, §2.1). |
| arXiv:2609.13672 | *Recoverability as a System Primitive for Long-Horizon AI Agents* | Recovery deserves to be a first-class contract, separate from replay. → this ADR's durable tier is stated separately for lineage, execution, completion and spawning (§2.2), rather than as one "durable" claim. |

**Cluster 2 — effect committed, record absent (M5, §2.1, §2.2.1).**

| Id | Title | What it establishes, and where it lands here |
|---|---|---|
| arXiv:2610.31916 | *Permission to Retry: A Study of Authorization and Recovery in AI Agent Workflows* | Checkpoint state, permission to dispatch, evidence of an external effect and permission to reconcile are four distinct obligations. → why the *spawn* has its own outcome vocabulary (`dispatched`/`unknown`, §6.1.2) separate from the child's, and why reconciliation is a rule rather than an assumption. |

**Cluster 3 — delegation isolation and the board as a projection (D1, D2, D3).**

| Id | Title | What it establishes, and where it lands here |
|---|---|---|
| arXiv:2308.08155 | *AutoGen: Enabling Next-Gen LLM Applications via Multi-Agent Conversation* | Conversation programming: a uniform message/reply abstraction expresses supervisor, peer and dynamic topologies without hard-coding one. → supports "one mechanism, policy-parameterised" (D1) and warns that implicit conversation state is not durability (M5). |
| arXiv:2411.04468 | *Magentic-One: A Generalist Multi-Agent System for Solving Complex Tasks* | An orchestrator with a long-lived task ledger (facts, guesses, plan) plus a per-step progress ledger, explicit stall detection and replan. → the ledger is a projection of events here, never a row (D3); stall detection is a bound, not an outcome (`stalled`, §6.2). |
| arXiv:2310.02170 | *A Dynamic LLM-Powered Agent Network for Task-Oriented Agent Collaboration* | Separates one-time team selection from per-query execution, with contribution scoring, pruning and early stopping. → the roster is a selection list, not a scheduler (D2), and a re-selection is a new session, not a mutated slot (constraint 11). |
| arXiv:2503.13657 | *Why Do Multi-Agent LLM Systems Fail?* | 14 failure modes in 3 categories across 1,642 traces, including premature termination and no/incomplete verification. → the completion gate and the per-task verification reference (§4.3 item 3) exist to make those two modes structurally impossible, not merely discouraged. |
| arXiv:2610.14515 | *SEPM: Self-Evolving Procedural Memory Grounded in Task Graphs for Multi-Agent Collaboration* | A lightweight shared blackboard records structured evidence alongside task dependencies, and verified graph fragments become reusable procedures. → the nearest task-graph precedent: dependencies and evidence are first-class facts (hence §7.2), and the board over them is a view. |

Two honest limits of this evidence base: none of these sources evaluates an event-sourced lineage plus
a materialized board (the exact combination here), and none defines a depth or fan-out contract — the
`max_children: 4` / `max_depth: 1` defaults are policy, not findings (see the contract's §10).

## Decision

### D1 — Delegation is one mechanism: a child session via `Thread`, and it is durable

`spawn_worker` creates a child **session** through the existing `Thread` primitive. The spawn call
happens inside the parent's drain; the child has its own stream, inbox, claim/epoch, slot bindings and
Mandate. There is no second, lighter, non-durable tier: "light" is policy (roster entry, budgets,
caps). Superseding nothing, this makes the delegation contract explicit: isolation, authority,
durability and fencing are the child session's own properties, not conventions.

Two consequences are normative:

1. **Derived spawn identity.** The spawn carries `spawn_uid = uuid5(parent_session,
   "kian:spawn:<seq>:<index>")`, computed as a pure function of the parent's committed stream at the
   requesting `tool/call`'s own `seq` (constraint 13), and the child thread/session ids are minted
   from it. A replay of that committed call therefore re-finds the same child instead of minting a
   second one. The value rides in two different byte classes, not two copies of one decision: the
   wrapper `content` copy is model-visible message content on new delegations (P1, the `speculative` /
   `mandate` / `exec_worker` precedent), while the top-level `thread/started` copy is a non-context
   audit key deferred to P4 (additions are one-way). Pairing needs neither — the parent derives the
   value and holds the child id it minted.
2. **Honest tier statement.** Lineage, child execution and completion are durable. The *spawn* is
   not exactly-once: an unrecorded window remains, and it is reported as the **spawn outcome**
   `unknown` — a fact about the dispatch, never a child outcome — rather than papered over. This is
   precisely where kian beats the reference implementation (whose running child dies with its owner)
   and precisely where it must not overclaim.

### D2 — Cross-session composition is the existing primitives plus a config roster

Composition = `Thread` (lineage + version pin) + Mandate (authority) + the subscription registry
(fan-out) + `AgentDef`/`agent.id` (identity and persistent memory scope) + a **config roster** (the
named-agent selection list) + a **join contract** (one durable parent inbox row per completion) + a
**board view**. Nothing else is added; in particular:

- **No profile-home analog.** A separate state root per agent would fork truth (constraint 1) and
  duplicate the memory-scope semantics that already give durable per-agent state.
- **No agent registry table.** Membership/selection is config plus runtime reads, the same posture the
  worker catalog took (membership is not a table).
- **No second scheduler or workflow store.**

The roster is deliberately a *config list over `AgentDef`* rather than a prompt block or a schema
enum: ADR-021 already withdrew the catalog-from-the-schema design because it made membership a
prompt-prefix fact. Roster data reaches the model only through a read-only `list_agents` result, and
the roster is narrowed into the child's Mandate and tool surface — never widened.

### D3 — A board is a protocol, not a table: observability view + control path recomputed from the primary

**A mutable board entity is refused permanently** — a `tasks` row that can disagree with the event log
violates constraint 1 and READ-MODEL R1, and no phase makes that acceptable.

"Board" is adopted as **two surfaces split by read class**:

- **Board view (`task_board`)** — a disposable materialized view over `thread/started`,
  `thread/completed`, `evidence/verdict`, pending-inbox state and the directory, declared in
  READ-MODEL §6, observability-only, stale within the declared bound, and **never** usable for a
  control read (R8). The operator surface is a read-only CLI; the web UI stays a non-goal.
- **Control path** — every decision (may this be claimed? promoted? is it done?) computed at decision
  time from the primary: events, durable inbox rows and config.

A board over today's bytes renders **threads, not a task graph**. Two load-bearing facts are not
inferable from lineage + inbox + subscriptions today, plus one that becomes load-bearing the moment a
shared work pool exists — that conditional status is part of the honest list:

1. **multi-parent dependency edges** (`thread/started` carries exactly one `parent`, so fan-in has no
   durable edge). Deferred with fan-in itself: v1 tasks are **parent-local**
   (`uuid5(parent_session, "kian:task:<key>")`), because a cross-parent namespace without an atomic
   fenced claim on one authoritative stream lets two parents each observe "no live claim" and each
   spawn — and the extra authoritative stream that would fix it is exactly what this ADR's
   board-entity refusal forbids. Recorded as drafting correction 7 in the contract's §12.
2. **claim identity with an epoch — conditional on the deferred shared pool.** A Mandate records
   authority *at spawn*, not "who owns this unit now, until when, under which fence". Under M10 and
   the single-spawner rule no agent ever selects work from a shared pool, and the owner of a unit is
   already answered by `thread/started` plus the child's projected liveness — so this fact is *not*
   missing for v1; it would become load-bearing only if unassigned work ever existed.
3. **completion verification reference** (`thread/completed` carries status + summary; whether a gate
   passed is a separate fact, and linking it is what prevents unverified-completion failures).

The minimal mechanism, in dependency order, adds **no new object type**:

- **intent queue = the existing durable inbox row** (work created but not started is a pre-event row;
  the control plane still writes only the inbox);
- **task facts = two additive non-context key groups on events that already exist** (`spawn_uid` +
  a namespaced `multiagent` block on `thread/started` and `thread/completed`), deferred to phase P4
  because emitting additions is one-way;
- **readiness = a pure function of the primary**; **claim = the spawn itself** (the child's
  `thread/started` is its audit record, the child session's epoch is its fence); the only spawner of a
  parent's children is that parent's drain — **no global dispatcher**;
- the **board view** recomputes those facts and never authorizes anything.

**The rung**: configuration (roster, limits, join policy) → bundle code (`kian-superagent` + read-only
CLI) → one additive READ-MODEL view → (P4, owner gate) two additive EVENT-MODEL key groups. No new
seam, no core primitive, no new event kind, no second store.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Same-session child (in-process sub-drain)** | Breaks context isolation (all message kinds are context-bearing; isolation-by-filter is a context change refused by EVENT-MODEL §4.2), fencing (two writers on one stream = `NoSplitBrainWrite`), and slot immutability. Would need a new event kind and a second fencing model. |
| **Two delegation tiers** (a light, hermes-parity tier plus the durable one) | Two semantics for one object, and the one a caller picks by accident. The requirement was to *beat* the reference gap or document why not; the child thread beats it, so no second tier is justified. A lighter mode of the *same* child thread remains addable later without touching the event contract. |
| **A mutable `tasks` table (reference parity)** | Constraint 1 / R1: a second source of truth, which is exactly the failure kian exists to avoid. Permanent refusal, not a phase decision. |
| **Infer dependency edges and claims from lineage** | Impossible for multi-parent joins and claim epochs; would render a confidently wrong board. Discovery is not a claim. |
| **Let the board view drive promotion** ("fresh enough") | R8 forbids control reads on a view; staleness on pending-inbox state is already declared, so "fresh enough" is a guess the runtime must not make. |
| **Adopt A2A / ACP / MCP-Tasks as the internal task model** | Those models describe a remote request lifecycle, not a shared dependency scheduler. They stay outbound adapters only. |
| **A profile home per agent (separate config/memory/state root)** | Forks truth; and `AgentDef` + the `agent.id` memory scope already provide durable per-agent identity and memory. |
| **A new `task/*` event-kind family** | Fails EVENT-MODEL §6.2: lineage, completion, verification and authority all already have carriers, so no fact requires a new kind. |
| **Roster data in the spawn tool schema or judge prompt** | The ADR-021 supersession already rejected catalog-in-schema (prefix invalidation, cost); the roster follows the same rule and flows through `list_agents` results only. |
| **A shared unassigned work pool with volunteer claims** | The one reference capability kian cannot express without a scheduler service; it would need a drained board session, i.e. a `LoopDriver` reuse or a new seam plus an ADR. Deferred and named, not designed. |

## Consequences

- Multiagent becomes a contract with an owner (`MULTIAGENT-MODEL.md`) rather than a Router/Worker
  convention, and the answer to "do we need a kanban board?" is recorded, with its gap list.
- A child cannot out-live its parent's *record*: lineage, execution and completion are durable, and a
  parent that crashes re-enters from a durable inbox row. The one honesty hole (spawn exactly-once) is
  named in the contract's UNVERIFIED section instead of hidden.
- The board ships in a phase that adds **no** durable fact (P3), so the visible surface carries no
  contract risk; the only one-way step (P4) is an owner gate.
- Cap defaults (`max_children: 4`, `max_depth: 1`) are policy, not science: the literature defines no
  general depth or fan-out contract, so both are config and may change without a contract change.
- A roster change affects new spawns only; persisted wrappers and live prompts are never rewritten
  (constraint 3), and drift is reported rather than applied.
- No `kian-core` crate, seam, route, event kind, table or metrics key is touched: the whole capability
  is config plus bundle code, except the P4 payload additions.

## Reversibility

| Choice | Reversible? | How |
|---|---|---|
| One-tier child-session delegation | yes | nothing forecloses an in-process child drain later; lineage + completion stay |
| Derived `spawn_uid` (and derived thread/session ids) | yes in principle | it is bundle-side derivation; changing it changes new spawns only, never history |
| Roster as config over `AgentDef` | yes | drop the block; legacy spawn bytes return |
| Board view | yes | views are disposable by contract; drop and rebuild |
| Control path (readiness/claim) | yes | bundle code; the board can be removed without touching streams |
| Parent-local task namespace (D3, correction 7) | yes | it is bundle-side derivation over the parent's own stream; a cross-parent namespace arrives with its own ADR and an atomic claim, and nothing here forecloses it |
| P4 additive event keys | **no, in practice** | additions-only forbids removal once emitted — which is exactly why it is an owner gate rather than a phase |
| The refusal of a board entity | **no** | it is a constraint consequence (constraint 1); reversing it is a supersession ADR, not a config change |

## What this ADR does not decide

- **Whether a shared unassigned work pool exists** (fleet-style volunteering). Deferred with a
  default of *no*: it needs a scheduler seam and is a separate ADR.
- **Per-roster-entry credentials or provider routes.** Not adopted in v1: credentials stay
  deployment-level, and a roster entry differs by slots, tools and limits only.
- **Any web UI, cross-kian federation, or hosted multi-tenant concern.**
- **The exact numeric cap defaults**, which are policy values in config.
- **The mechanics of output-schema validation** (where the schema is declared, which drain runs the
  check, its retry budget) and the exact fan-in default beyond `all` — drafting freedoms recorded in
  the contract (§3.2.1, §5.3), not architectural choices. What *is* decided here: an expectation must
  exist and be **persisted with the spawn**, because a gate whose expectation cannot be reconstructed
  after a restart validates nothing.
- **R13's hook/plugin surface** (`design/PLUGIN-SPEC.md` is in flight in a separate round): named as a
  forward pointer only, with no dependency, no link and no citation into it.
