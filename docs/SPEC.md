# Kian — Spec v0.1

> Spec = WHAT. Design lives in [design/DESIGN.md](../design/DESIGN.md).

## 1. Positioning

**Kian = the Temporal of agents**: an open-source, model-neutral, self-hostable durable execution
runtime that makes agent tasks **killable, resumable, migratable and auditable**.

Kian is **not**:

| Not | But |
|-----|-----|
| A general agent orchestration framework (no fight with LangGraph/AutoGen over the expression layer) | A long-horizon reliable execution substrate (durable execution) |
| A hosted service (no fight with Anthropic/AWS over the managed business) | An open-source, self-hostable neutral runtime |
| The super-agent itself | The execution substrate of the super-agent reference architecture — the four super-agent traits are composition patterns over Kian primitives |
| Yet another abstraction layer | Session as the first-class durable entity + event stream as sole truth + drain as execution hold interval; the agent loop is just a control flow inside a drain |
| A harness (no fight with dsh/Claude Code over the interaction layer) | The integrated party — harnesses own interaction and capabilities, Kian owns reliable execution |

**Problems P1–P8 to solve** (inherited from the Feishu design doc):

| # | Problem | One-liner |
|---|---------|-----------|
| P1 | Long-horizon reliability | Execution must be killable, resumable, migratable |
| P2 | Cost governance | Cache-byte stability must be an invariant and observable |
| P3 | Unified recovery abstraction | In-loop / session-layer / durable-state recovery has no first-class subsystem |
| P4 | Honesty | "claimed done" ≠ "verified done"; stopping must pass an evidence gate |
| P5 | Runtime control plane | steer/interrupt/queue/confirmation semantics are un-protocoled |
| P6 | Multi-model | Slot semantics in Kian, access resilience in the gateway |
| P7 | Task-centric orchestration | An agent is one form of tool |
| P8 | Governance as default | permission/approval/audit/quota must fail closed |

## 2. User Interface

### 2.1 User roles

| Role | Description | Cares about |
|------|-------------|------------|
| **App developer** | Writes agent applications on the Kian SDK | Ergonomic API, pluggable models, debugging/replay experience |
| **Harness integrator** | Connects a harness (e.g. dsh) onto Kian | Clear protocol, embedding cost, capability boundary |
| **Super-agent operator** | Deploys Router/Worker, event subscriptions, BYOM endpoints | Deployment topology, watchdog, recovery behavior |
| **Platform SRE** | Runs the hosted control plane / multi-replica cluster | lease/fencing, quotas, audit export |

### 2.2 Usage flows (three main lines)

**A. Single-agent application (SDK line)**
```
1. Define an Agent (versioned config: identity, model slots, toolset, budgets)
2. kian run / SDK kian.create_session(agent)
3. Submit inputs via the Inbox (messages/events/tool results) → Kian Admit→Wake→Drain→Promote
4. Read progress and results via the event stream / projection; steer/interrupt via the control-plane API
5. After a crash the session becomes recoverable → a new drain replays events and continues
```

**B. Super-agent reference deployment (composition mode)**
```
1. Install the super-agent profile (bundle): Router session + Worker session templates + subscription rules
2. Attach channel adapters (IM / GitHub / …) → events land in the Router session inbox
3. Inside the Router drain: judge events → dispatch in batches → spawn Worker sessions (Thread delegation)
4. Workers report back on completion; the Router replies on the channel (IM ≤100 words / structured PR delivery)
5. The endpoint daemon (BYOM) executes commands, events flow back; watchdog monitors endpoint health
```

**C. Harness integration (being-integrated line)**
```
1. The harness plugs its loop driving authority into Kian (replacing its self-managed loop)
2. The harness's models/tools/sandboxes mount as seam providers
3. Kian provides durable state, recovery, control plane, audit
   This line composes with B: a harness loop can serve as a Kian Worker wholesale.
```

### 2.3 Configuration interface (v0.1 draft)

```yaml
# kian.yaml — single-file configuration (narrow waist)
storage:
  event_store: sqlite        # v0: sqlite | postgres
  projections: sqlite

models:                      # model slots (P6: determinism in Kian)
  - slot: main
    provider: openai         # adapter name (plugin-provided)
    model: gpt-5.2
    fallback: [kimi-k2, deepseek-v4]
  - slot: router             # cheap fast slot for the super-agent Router
    provider: deepseek
    model: deepseek-chat-v4

sessions:
  default_agent: default     # points at an agent definition
  inbox_admission: durable   # A7
  drain:
    max_iterations_per_drain: 200
    recovery_budget: { tokens: 500k, wall_clock: 30m }

recovery:                    # A4/A5: pure-function classification + declarative policy table
  policies:
    - match: { error_class: rate_limited }
      action: backoff_retry { max: 5, base: 2s }
    - match: { error_class: context_overflow }
      policy: compress_then_retry
    - match: { error_class: provider_down }
      action: failover { slots: [main], to: fallback }

control_plane:               # P5 protocoled control plane
  steer: enabled
  interrupt: { grace: 5s }   # settle-on-abort (invariant 6)
  confirmation: { mode: policy }

plugins:                     # Cordis-inspired plugin loading
  - name: lark-adapter
    config: { ... }
  - name: super-agent-router
    config: { im_reply_word_limit: 100 }
```

### 2.4 API surface (v0.1 draft; details in DESIGN)

```rust
// SDK shape (placeholder; DESIGN is authoritative)
let mut session = kian.create_session(CreateSession { agent: "default", ..Default::default() }).await?;
session.submit(Submit { input: Input::Message("investigate the vadis R75 CI failure"), .. }).await?;  // Admit (durable first)
                                                                                                       // Wake is advisory
let projection = session.projection().await;   // state is read only from the projection
session.steer(Steer { text: "switch to plan B", .. }).await?;
session.interrupt(Interrupt { reason: "user stop", .. }).await?;
let events = session.events(..).await;         // audit = an event-stream interval
```

### 2.5 Demo scenarios (acceptance)

| Scenario | Demonstrates | Input | Output |
|----------|-------------|-------|--------|
| S1 crash recovery | kill -9 mid-drain | A half-finished task | session → recoverable → new drain continues, zero lost work |
| S2 steer/interrupt | Redirect/stop while running | steer "plan B", interrupt | Redirection lands at a safe boundary; interrupt ≠ failed |
| S3 cache stability | Consecutive requests in one session | Requests with unchanged history | Cache hits (stable prefix bytes) observable above expectation |
| S4 super-agent demo | IM request → PR delivery | Group-chat message + CI events | Router dispatches Worker; structured PR description; IM reply ≤100 words |
| S5 BYOM | Endpoint daemon execution | Local repository commands | Cloud loop + endpoint execution; watchdog detects endpoint offline |
| S6 harness integration | dsh loop as a worker | Harness config | dsh interaction unchanged, made durable |
| S7 audit export | Event-stream interval export | Time/causal interval | A replayable audit package (byte-identical) |

## 3. Operations Interface

### 3.1 Observability
- Metrics: session state-machine transition counts, drain depth/duration, pendingWake depth,
  cache hit rate / invalidation causes (A3), recovery action counts and budget consumption (A5),
  evidence-gate verdicts (A6).
- Trace: every event carries drain_id + epoch (fencing); cross-session delegation (Thread lineage)
  chains into a causal trace.
- Audit: event-stream interval export + replay (A1/A2).

### 3.2 Operational actions
- `kian` CLI: serve (single machine), session inspect/replay/export, wake-now, lease inspect,
  recovery budget reset.
- Endpoint: `kiand` daemon install/pair/heartbeat/reconnect.

### 3.3 Operational flows
- Version upgrades: event schema versioning + adjacent migrations (borrowed from dsh session
  format migrations).
- Half-terminated session repair: event projections auto-detect NoHalfTerminated violations and
  alert.

## 4. Data structures (concept level; field level in DESIGN)

Core concept model (inherited in full from the Feishu design doc §4.2): Agent (versioned config
asset) / Environment (unversioned config) / Session (the one first-class durable entity) / Input
(durable table rows) / Run-Drain (no durable identity; in-memory hold + lease) / Turn (logical
derivation, an event slice) / Iteration (aggregate counters + events) / Event (append-only, never
modified) / Thread (independent event stream + version pin; task-centric delegation).

## 5. Scope

### Phase 1 (MVP, ~6 weeks)
| Module | Contents |
|--------|----------|
| kian-core | Concept model, session state machine, A1–A10 invariant assertions |
| EventStore (sqlite) | Append-only writes, expectedSeq CAS, epoch fencing |
| Inbox + Admit→Wake→Drain→Promote | The six semantic invariants |
| Recovery subsystem | A4/A5: pure-function error classification + policy table + budget refunds |
| kian-llm | Model slots (single provider adapter + fallback) |
| kian-loop | LoopDriver seam + built-in ReAct loop |
| kian-endpoint | Endpoint SDK library (KiandProto endpoint side) |
| kian CLI + SDK crate | create/submit/steer/interrupt/events/projection |
| Acceptance | S1, S2, S3 smoke |

### Phase 2 (super-agent reference architecture, ~6 weeks)
| Module | Contents |
|--------|----------|
| Router/Worker two-layer composition | Thread delegation |
| Event subscriptions + channel adapter framework | IM/GitHub first |
| kiand endpoint daemon (BYOM) | + watchdog |
| Evidence gate v1 | A6 |
| Acceptance | S4, S5 |

### Phase 3 (article-sync round, landed 2026-11-01; ecosystem, ongoing)

What the round landed, recorded against the contract (cards C31–C44; closeout C45). Scenario
definitions S1–S7 in §2.5 are unchanged — only status is recorded here.

| Module | Contents | Status |
|--------|----------|--------|
| Inline/Spill admission shaping (the `input/spilled` kind) | Oversized channel payloads admit as a bounded Inline view in model context; the verbatim raw is appended in-stream, out of context, in the same atomic consumption write (C31–C33) | landed |
| Recall toolpack (Chain of Recall) | Read-only `recall_event` / `recall_neighbors` / `recall_thread` over `RecallPort`; outputs are themselves Inline/Spill-shaped so multi-hop recall composes; Router global observability rides the event stream via recall, not Router context (C34–C35) | landed |
| Two-level admission filtering | Fast-path `classes:` table (`candidate | explicit-only | ignore`, keyed by channel/topic/sender, most-specific-wins) evaluated before the rule set and before any model call; the judge prompt is the natural-language policy extension point (C36) | landed |
| Async `start()`/`stop()` tool pattern | `start` acks immediately with a job id; the completion returns as an admitted input (`async_result` envelope) promoted at the next safe boundary; `stop` cancels honestly; job rows are pre-event durable state ([ADR-008](../design/decisions/ADR-008-async-tool-completion-admitted-input.md)) | landed |
| Many-to-many passthrough fan-out | A topic maps to the set of workers that served it; one event admits one durable row per subscriber, idempotent per `(session, event_id)`; empty set falls back to the Router (C38) | landed |
| Speculative-execution profile | Additive `speculative: true` delegation marker; stream-derived `max_outstanding` cap enforced with honest tool errors; discarded speculation audits everything and delivers nothing (C39) | landed |
| Watchdog Session v2 | Model-in-the-loop progress judge: a `type: timer` channel routes ticks to a fixed session via `route: {session: <uuid>}`; `intervene` admits a steer row durable-before-wake, `escalate` replies under the IM policy; v1 endpoint heartbeat untouched (C40) | landed |
| Batching metrics (§7.2 shape) | `superagent.router_batching` series on `/metrics`: batch size, turn wall duration, amortized ms/input; metrics-off is behavior-identical (C41) | landed |
| Ablation benchmark (§7.1 shape) | Deterministic with/without-passthrough comparison in kian's native units (Router judge turns + request bytes); directionality asserted, both configurations must complete (C42) | landed |
| Untrusted-input marking | `"untrusted": true` in the provenance wrapper for configured channels (real channels default untrusted); judge-prompt and recall-hop hygiene (C43) | landed |
| Real lark channel adapter | Feishu IM receive/send over the open APIs; env-only credentials (`KIAN_LARK_*`), fail-closed startup; default-untrusted (C44) | landed |
| Plugin SDK + Footprint Ladder | Static plugin system + Footprint Ladder landed across Phases 1–3; SDK extraction → Phase 4 (below) | partial |
| Audit export/replay tooling (S7) | `session export`/re-import (byte-identical, C14) + delegation-audit replay assertions (C28) — demonstrated by prior rounds, recorded here at closeout (C45); no new work | **demonstrated** |

### Phase 4 (deferred from Phase 3)

| Item | Reason (round-3 plan §3, summarized) |
|------|--------------------------------------|
| Postgres EventStore + multi-replica lease/fencing | No Phase-3 acceptance scenario requires it (S1–S5 are sqlite-proven; everything this round ran single-machine); effort XL, and mid-round store churn would destabilize exactly the goldens the recall/ablation cards depend on. DESIGN §12 already assigns multi-replica to the hosted layer; constraint 4 anticipates the one-statement end write |
| S6 dsh harness integration | Ecosystem-gated: depends on dsh's own session-log semantics staying stable and touches an external repo; nothing in the article-sync backlog depends on it. Opens Phase 4 after `kian-sdk` so the embedding contract freezes once |
| kian-sdk | The thin client extraction is small, but publishing an SDK freezes the control-plane API — do it after the Phase-3 additions (async completion inputs, recall surfaces, admission classes) settle so the freeze happens once |
| GitHub adapter | Needs an inbound webhook HTTP surface + an auth/verification model — a new server-surface class, not just an adapter; no acceptance scenario beyond S4's shape that mock + lark don't already cover |
| A2A boundaries | Remains exploration; the context-sharing boundary is the Session/Thread boundary |

### Deferred (unchanged)
| Item | Reason |
|------|--------|
| Web UI | Polish core semantics via CLI/API first |
| Multi-Router horizontal sharding | Article experiments: single Router has no throughput bottleneck at team scale |
| Credential-less credential use | Standalone security topic, Phase 3+ |
| End-to-end encrypted endpoint channel | Same |

## 6. Quality requirements

| Dimension | Standard |
|-----------|----------|
| Reliability invariants | NoLostWork / NoHalfTerminated / NoSplitBrainWrite resident assertions + chaos tests |
| Reproducibility | Byte-identical replay of the same event stream (A2) |
| Cost | Cache hit rate / invalidation causes as first-class metrics (A3) |
| Honesty | interrupt ≠ failed; empty response is a soft failure; evidence gate (A6) |
| Governance | permission/approval fail-closed; audit = event-stream export (P8/A1) |
| Compatibility | Event schema versioning + adjacent migrations |
