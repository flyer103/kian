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
  event_store: sqlite        # v0: sqlite | postgres (postgres implemented C51:
                             #   requires storage.postgres.url_env — env-only URLs,
                             #   never a config value — and an explicit unique
                             #   drain.holder; both refuse to start otherwise)
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
| S8 multi-replica takeover | S1 lifted to multi-process scale (defined round 4) | Two `kian serve` replicas over one postgres DB; the drain owner is `kill -9`'d mid-drain | The survivor's recovery sweep takes over the expired lease and completes: zero lost work, exactly one end write, a stale-epoch probe write from the dead replica rejected (NoSplitBrainWrite between processes) |

S8 status: **demonstrated** via C52's env-gated evidence (`KIAN_PG_URL`-gated e2e, default CI
excluded) — the definition lives here, once; the proof is recorded in §5's Phase-4 table.

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

### Phase 4 (hosted foundation + SDK freeze + ecosystem surfaces, landed 2026-11-01)

Round 4 (cards C46–C54, closeout C55) landed the items Phase 3 deferred. Scenario definitions
S1–S8 in §2.5 are unchanged — only status is recorded here. Zero new event kinds this round;
the durable set stays 20.

| Item | Contents (card) | Status |
|------|-----------------|--------|
| Postgres EventStore + inbox (dual-backend) | `kian-store` backend dispatch (`Sqlite \| Postgres` enum, identical public surface); postgres dialect SQL (single-statement end write, CTE batch, PK-race classification through the HEAD path); export parity vs sqlite is the compat golden (C50) | landed |
| Postgres exec + assembly | `kian-exec` dual-backend; `storage.event_store: postgres` with env-only URL (`storage.postgres.url_env`) and an explicit unique `drain.holder`, both fail-closed; one server, one database (C51) | landed |
| Multi-replica lease/fencing | Conditional claim admission + unique holders (C47), postgres LISTEN/NOTIFY advisory wake transport (notify-after-commit; sweep + startup scan bound loss) and exec row leases (C52) — [ADR-009](../design/decisions/ADR-009-multi-replica-claim-admission.md). **S8 demonstrated** by the env-gated e2e (`KIAN_PG_URL`, default CI excluded) | landed |
| kian-sdk | The wire-only embedded SDK (frozen C13/C14 surface, typed methods, SSE helper, matchable errors, golden wire fixtures); **frozen additive-only** after C46 — a breaking change requires a new plan round | landed |
| GitHub adapter | Channel mapping + structured send (C48, untrusted-by-default, env-only token) and the verified webhook receiver — a fourth server surface, verify-then-parse, admit-only (C49, [ADR-011](../design/decisions/ADR-011-inbound-webhook-plane.md)) | landed |
| S6 dsh harness integration | The dsh SessionEvent → existing-kind mapping contract + conformance fixture (C53, [ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md)) and the `kian-dsh` bridge LoopDriver (C54, `superagent.harness.dsh`, default off) | **demonstrated — fixture-proven; live leg executed (honest negative), S6-live open** (fixture-proven in CI; the live leg is env-gated on `KIAN_DSH_BIN`, ran against real dsh 0.2.1-rc.5 on 2026-11-01 and stopped at an honest `SoftFailed { reason: "peer never sent ready within 30s" }` — the stock dsh SDK app does not meet the [ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md) contract-v0 bar — so S6-live stays open; not claimed as CI-proven, the fixture suites remain the CI proof, outcome recorded in the README run log) |
| A2A boundaries | The exploration note below — no cards this round | parked (note) |

#### A2A boundary note (exploration; round-4 verdict, no cards)

The context-sharing boundary in kian is the **Session/Thread boundary**: a Session is the
maximal context-sharing unit, and Threads cross it only with explicit, auditable lineage. Any
cross-instance A2A experiment (agents on different kian deployments, or a kian agent talking to
a foreign agent) crosses that boundary with unshared context and distinct permission
boundaries; it is therefore a *new surface class*, not an adapter, and has no acceptance
scenario behind it yet. **Preconditions any future experiment must state before a card
exists**: (1) the authenticated channel — how the remote party authenticates and how event
provenance survives the hop (anything arriving over it is untrusted-marked by default, the C43
rule); (2) the trust model — whose permission boundary evaluates whose actions, and how a
foreign action is labeled in the audit stream; (3) the failure/honesty model — interrupt and
recovery semantics across the boundary (interrupted ≠ failed applies unchanged). An experiment
that skips stating these freezes speculation; none was attempted this round.

### Phase 5 (live positives + dogfood + field fixes, landed 2026-11-02)

Round 5 (cards kian-59/kian-60 + worker-initiated kian-61/kian-62). Scenario definitions S1–S8
unchanged; status recorded here.

| Item | Contents | Status |
|------|---------|--------|
| S6 live leg — contract-v0 peer | `contrib/dsh-v0-peer/dsh-v0-peer.mjs` (zero-dep Node) adapting stock dsh JSON-RPC onto the frozen [ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md) v0 contract; live run green (real `deepseek-chat`, full mapped chain, honest `Settled`) — supersedes the 2026-11-01 honest negative | landed |
| Dogfood workload | `examples/issue-watch/`: TimerChannel → `gh issue list` → Router triage → worker draft (dry-run) on the real `flyer103/kian` repo; `demo-restart.sh` proves kill -9 reboot resumes the same session; RUNLOG records real data | landed |
| Wire dialect v2 (breaking) | Strict OpenAI-compatible serialization: dotless built-in tool names (`fs_list`/`fs_read`/`fs_write`, dotted forms replayable), wire-level `tool_calls`/`tool_call_id`, `AgentDef.tools` fail-closed. Found live via DeepSeek HTTP 400/422; constraint-3 event in DESIGN (golden hash same commit) | landed |
| Router re-arm | Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident Router with an observable `[re-arm]` cause input; soft-failed sessions re-open the same pinned session. Zero new kinds; DESIGN §4.6 | landed |

### Phase 6 (bundle-domain policy round, proposed 2026-11-02 — input: dsh-superagent-prototype)

Adoption of the official article implementation's domain-policy layer onto kian's substrate.
Reference: `a vendored archive/dsh-superagent-*.md` (archived rev 6337fc8), digest §5. **Everything
below is bundle-layer (`kian-superagent`, at most core projection support); nothing touches
kian-core/store/orchestrator seams except through the ADR path.**

| Candidate | From prototype | kian's today-gap | Layer/cost | Priority |
|---|---|---|---|---|
| Ticket stage projection | 003 first-match stage table ("what is it waiting for") | session state is execution state; no "awaiting person" semantics | pure projection, zero new kinds | P6a — highest |
| Closed reason vocabularies | Decision reasons as closed enum + display text | free-text rationale only | enum + display text, zero new kinds | P6a |
| Output-side speak-rules | attention economics (000/005): speak only when someone must act or can use a result | input-side admission only | config-first rule table, zero new kinds | P6a |
| Follow-up ownership routing | a person's follow-up belongs to the live Ticket's Executor; Router may only stop | all follow-ups re-enter Router | routing policy reusing existing kinds | P6a/P6b |
| Mandate (delegation authority scope) | Ticket carries an explicit authority object; actions bounded by it | delegation carries task text, no authority object | **ADR-012 candidate** — new event shape (charter-like) | P6b |
| Thread/Correlation intake projection | Source read-position + Thread key + one-Trigger-many-Tickets fan-out (grants no authority) | channel admit idempotency only; one source feeds one session | **ADR-013 candidate** — projection + derivation rule | P6b |
| Outbox+receipts bounded retry | delivery failures retried 2s→1min exponential, then parked honestly | structured delivery exists; no unified retry domain | check delivery-kind reuse first | P6b |
| Policy/prompt versioning + drift marking | numbered snapshots + current pointer; overrides marked "default updated" on upgrade | prompts unversioned in config | config-first + replayable version events | P6c |

Wave split (mirrors P4's structure): **P6a** = zero-new-kind bundle work (stage projection, reason
vocabularies, speak-rules, follow-up routing); **P6b** = ADR-backed event-shape additions (Mandate
ADR-012, Thread intake ADR-013, outbox retry); **P6c** = dogfood validation — issue-watch upgraded to
multi-Ticket subscription fan-out, run live against the prototype's scenarios.

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
