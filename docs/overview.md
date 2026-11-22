# Kian — Overview

> An external reader's map of Kian: the problem it solves, the execution model, the shape of the
> code, and what has actually been demonstrated. The contract is [SPEC.md](SPEC.md); the blueprint
> is [DESIGN.md](../design/DESIGN.md); the decisions are [design/decisions/](../design/decisions/);
> the changelog is [../CHANGELOG.md](../CHANGELOG.md).

## 1. The problem: a single-session agent does not survive

An agent implemented as one process running one conversation has a single point of failure that is
structural, not incidental:

- **It dies with its process.** A crash, a deploy or an OOM loses the work in flight, and there is
  no record of what was sent to the model, what the model answered, or which tools ran.
- **It cannot be redirected safely.** Steering or interrupting mid-turn has no defined boundary, so
  a new instruction either gets ignored or corrupts the turn it lands in.
- **It cannot be audited.** "The agent said it was done" is not evidence. Without a durable record
  there is nothing to replay, export or diff.
- **It leaks cost silently.** Every rewrite of earlier context invalidates the provider's prefix
  cache; a runtime that reformats history on each turn pays for the same tokens again and again,
  invisibly.
- **It does not scale past one process.** Restart it somewhere else and the state does not travel;
  run two copies and they fight over the same work.

Kian's answer is the durable-execution answer, applied to agent semantics: make the **Session** the
durable entity, make the **event stream** the only source of truth, and make each execution interval
(a **drain**) disposable and restartable.

**One-line claim:** Kian is an open-source, model-neutral, self-hostable durable execution runtime
for agents, built on three promises: agent work survives the process (killable,
resumable, migratable, auditable), compute stays on machines you own (BYOM), and the loop stays
yours (harness integration).

## 2. The model

Five sentences define the whole system:

1. A **Session** is the one first-class durable entity; everything else (turns, threads, endpoints)
   is derived from or hangs off it.
2. The **event stream** is the sole truth: append-only, never rewritten, and session state is a
   *projection* of it — disposable and rebuildable.
3. An **input** becomes durable (an inbox row) *before* any wake fires; a wake is advisory and may
   fold or be lost, because the inbox is the real queue.
4. A **drain** is the execution hold interval: claim the session (epoch + lease), consume everything
   eligible, promote inputs only at provider-turn safe boundaries, then exit. A drain has no durable
   identity — after a crash a new drain replays the stream and continues.
5. The **agent loop is just a control flow inside a drain** — the core knows how to hold and persist
   execution; the loop (built-in ReAct, or a harness such as dsh behind the `LoopDriver` seam) decides
   what to do with it.

```
Admit  ->  Wake  ->  Drain  ->  Promote
durable    advisory  claim     input rows
row first  signal    lease     become context
```

### Session state machine

```mermaid
stateDiagram-v2
    [*] --> idle: session/created
    idle --> running: drain/claimed (epoch+lease)
    running --> idle: drain settled (inbox empty)
    running --> recoverable: lease expiry (liveness only)
    recoverable --> running: takeover claim (epoch+1)
    running --> completed: session/ended (atomic append)
    running --> cancelled: session/ended (cancelled)
```

Two rules make the machine safe under crashes (both are hard constraints, [DESIGN §2](../design/DESIGN.md)):

- **The end write is one atomic append.** `session/ended` and the state transition commit together,
  so a half-terminated session cannot exist even across a `kill -9`.
- **Safety is fencing, not leases.** A claim issues a monotonic epoch; every drain write carries it;
  the store rejects stale-epoch writes (`expectedSeq` CAS). Leases only *detect* recoverable sessions
  — they never authorize a write.

### Honesty rules that show up in the output

| Rule | What you observe |
|------|------------------|
| interrupted ≠ failed | an interrupted session ends `cancelled`/`interrupted` with the reason in the stream — never `completed` |
| an empty model response is a soft failure | `turn/end` records the soft failure instead of a silent success |
| a stop must pass an evidence gate | the worker bundle's completion claim is checked against the stream before delivery |
| persist-what-you-send | the exact request bytes are a `step/request` event *before* the HTTP call flies; replay is byte-identical |
| compression is the only deliberate prefix invalidation | `compact` is the only counted invalidation cause; anything else is reported as `unexpected_prefix_change` and is a bug |

## 3. Architecture

```mermaid
graph TB
    subgraph Users["Users & channels"]
        U([Developer / SRE])
        CLI([kian CLI])
        SDK([kian-sdk client])
        IM([IM / Lark])
        GH([GitHub webhook])
    end
    subgraph Server["kian serve (control plane + execution)"]
        API[API: JSON-RPC + ops + SSE<br/>axum]
        ORCH[orchestrator<br/>Admit → Wake → Drain → Promote<br/>+ recovery]
        LOOP[LoopDriver<br/>built-in ReAct / dsh bridge]
        SA[superagent bundle<br/>Router / Worker / subscriptions]
        ES[(EventStore<br/>sqlite · postgres)]
        INBOX[(inbox + claims)]
        MET[/metrics/]
    end
    subgraph Ext["External"]
        LLM([OpenAI-compatible provider])
        KD([kiand endpoint daemon<br/>BYOM])
    end
    U --> CLI
    CLI --> API
    SDK --> API
    IM & GH --> SA
    API --> ORCH
    SA --> ORCH
    ORCH --> ES
    ORCH --> INBOX
    ORCH --> LOOP
    LOOP <-->|model turns| LLM
    ORCH <-->|durable command queue| KD
    MET -.->|projection| ES
```

The **layered iron rule** (constraint 9) is what keeps this clean: the control plane only writes the
inbox and reads projections — it never touches execution; execution only appends events — it never
mutates projections directly. After a crash, replay rebuilds every in-memory structure.

## 4. Crate map

Nineteen crates, one responsibility each. Dependency direction is the crate form of the iron rule:
`kian-orchestrator` depends only on core + store + seam traits, and no provider, channel or harness
implementation ever enters its dependency tree.

| Crate | One line |
|-------|----------|
| `kian-core` | Concept model, the 23 event kinds, session state machine, projection engine, model-context derivation, seam traits |
| `kian-store` | EventStore with `expectedSeq` CAS + epoch fencing, inbox, claims/leases, dual backend (`sqlite` default, `postgres` optional) |
| `kian-orchestrator` | The Admit → Wake → Drain → Promote engine and the three-layer recovery subsystem (pure classification + declarative policy table + budget) |
| `kian-llm` | OpenAI-compatible `ModelProvider`, model slots, and the persist-before-send hook |
| `kian-loop` | `LoopDriver` seam plus the built-in ReAct loop |
| `kian-memory` | The memory bundle: notes and dreaming, creation-time injection, and the read-only search/map representations |
| `kian-cron` | Wall-clock scheduling core: five-field/`@`-alias grammar, IANA zones, DST-aware next-fire, misfire/coalescing — pure, no I/O |
| `kian-plugin` | Static plugin system: `Ctx` registry, `Plugin` trait, five-mode dispatch, `Disposer` |
| `kian-endpoint` | KiandProto **endpoint-side** SDK library (embeddable in any process) |
| `kian-exec` | KiandProto **server half**: connection manager, durable command queue with row leases, heartbeat watchdog |
| `kian-superagent` | The super-agent *bundle*: Router/Worker templates, event subscriptions, channels, output policies, recall, watchdog v2 |
| `kian-sdk` | Wire-only embedded client (frozen JSON-RPC + SSE surface, typed errors); depends on no kian crate |
| `kian-dsh` | The dsh bridge: a `LoopDriver` that drives a `dsh` child over stdio and maps its events onto existing kinds; depends on `kian-core` only |
| `kian-harness` | Shared external-harness skeleton: driver state machine, approval policy, fixtures and the capability types the three bridges share |
| `kian-claude` | Claude Code adapter: `stream-json` child, `can_use_tool` approvals, interrupt |
| `kian-codex` | Codex adapter: `app-server` JSON-RPC child, server-initiated approvals, interrupt |
| `kian-cli` | The `kian` binary: `serve` assembly, operator CLI (`inspect`/`replay`/`export`/`wake-now`), the GitHub webhook receiver |
| `kiand` | The endpoint daemon binary (`kian-endpoint` + a thin main) |
| `kian-imessage-bridge` | The macOS-side iMessage bridge: loopback HTTP over `imsg rpc` for the `type: imessage` channel |

## 5. What has been demonstrated

Status is recorded here and in the [CHANGELOG](../CHANGELOG.md). "Default-green" means it runs in
`cargo test --workspace` on any machine with no credentials and no services.

| Capability | Status | One-line proof |
|------------|--------|----------------|
| Crash recovery: `kill -9` mid-drain, restart, no lost work | **done** — default-green | `crates/kian-cli/tests/chaos_recovery.rs`: six kill points (after admit, after claim, mid-provider-turn, mid-tool, before/after the end write) over repeated runs on the same sqlite — no lost work, no half-terminated session, monotone epochs; stale-epoch writes are rejected |
| Steer / interrupt semantics and honesty | **done** — default-green | `crates/kian-cli/tests/steer_interrupt.rs`: steer lands at the next safe boundary (no input inside an in-flight provider turn); interrupt settles on abort and resumes honestly (interrupt ≠ failed); idle/terminal interrupts are no-ops |
| Cache-byte stability | **done** — default-green | `crates/kian-cli/tests/cache_stability.rs`: consecutive requests share the byte prefix; only `compact` breaks it and counts under the `compact` cause; `unexpected_prefix_change` stays zero — exposed on `GET /metrics` and `kian cache-stats` |
| Super-agent demo: IM request → Router judges → Worker completes → ≤300-unit reply + structured delivery | **done** — default-green | `crates/kian-cli/tests/superagent_demo.rs` (10 tests) plus the negative path where a gate-failed worker never reaches delivered |
| BYOM: cloud loop + `kiand` execution + watchdog detects an offline endpoint | **done** — default-green | `crates/kian-cli/tests/byom_e2e.rs`: exec round-trip through the durable queue, kill -9 while queued → row parks honestly, watchdog health input within the documented bound, reconnect drains, timeout is an honest error |
| Harness integration: dsh interaction unchanged, made durable | **demonstrated — fixture-proven; live leg green via the contract-v0 peer** | `crates/kian-dsh/tests/driver_bridge.rs` + `crates/kian-superagent/tests/dsh_conformance.rs` (every mapping row, replay byte parity) + `crates/kian-cli/tests/dsh_bridge.rs` (mounted e2e, kill-9 takeover, child crash → `interrupted`); the live leg (env-gated on `KIAN_DSH_BIN`) ran green on 2026-11-02 with `contrib/dsh-v0-peer/dsh-v0-peer.mjs` and real `deepseek-chat` — full mapped chain, honest `Settled`; details in [runbook-dsh.md](runbook-dsh.md) |
| Multi-harness bridges: Claude Code (`stream-json`) and Codex (`app-server`) as projection-only worker loops | **fixture-proven; Codex live positive (0.166.0), Claude protocol-verified (model turn blocked by local login)** | `crates/kian-harness` (driver state machine, approval policy) + `crates/kian-claude/tests/driver_bridge.rs` and `crates/kian-codex/tests/driver_bridge.rs` (scripted native children: turn mapping, deny/allow approval provenance, mid-turn child death → soft failure, no `step/request`) + `crates/kian-cli/src/config.rs` tests (mount resolution, one-harness rule, worker-surface exclusion, `ask`/bypass refusals) + `crates/kian-cli/tests/harness_bridge.rs` (mounted `kian serve` worker template over the scripted Claude peer) + the eager worker-session creation that writes `session/created.bridge` (`kian-orchestrator` `create_session_with`, `kian-superagent/tests/spawn_session_creation.rs`). Live runs need a real `claude` / `codex` binary and are not yet executed — recorded as unverified in [runbook-harness.md](runbook-harness.md) |
| Human-in-the-loop confirmation: `ask` defers a tool call, the answer resumes the same turn | **done** (phase 1) — default-green | `crates/kian-cli/tests/confirmation_e2e.rs`: ask → `awaiting_input` (idle, no lease) → `pending` → approve/reject/duplicate/TTL expiry → exactly one result with `confirmation` provenance → session completes. Crash-point takeover at the ask window is designed but not exercised by a kill test |
| Audit export / replay: a byte-identical replayable package | **demonstrated** — default-green | `kian session export` / re-import round-trips byte-identically (`crates/kian-cli/tests/ops_commands.rs`), and `superagent_delegation_audit_export_replays` replays both streams of a delegation |
| Multi-replica takeover: two replicas over one postgres, `kill -9` the drain owner mid-drain | **demonstrated** — env-gated (`KIAN_PG_URL`) | `crates/kian-cli/tests/postgres_multi_replica.rs::multi_replica_two_replicas_kill9_takeover_no_split_brain`: real `kian serve` children, real `kill -9`, the survivor completes, exactly one end write, and a stale-epoch probe write from the dead replica is rejected |

**Default-green vs env-gated.** Everything except the live harness and multi-replica legs runs with
zero credentials and zero external services. `KIAN_PG_URL` (postgres — exercised in CI on every push
since 2026-11-02), `KIAN_DSH_BIN` (a real dsh install — exercised live 2026-11-02 via the
contract-v0 peer) and the channel smokes (`KIAN_API_KEY`, `KIAN_LARK_*`, `KIAN_GITHUB_*`) gate the
rest: absent, those suites **skip with a log line and never fail**; present, they run and must pass.
The README's environment-contract table lists every variable.

## 6. Where the design inputs went

Kian was designed against three public references plus its own durable-execution design work. Each
input contributed something specific and identifiable in the code:

| Input | What it contributed | Where it lives in Kian |
|-------|--------------------|------------------------|
| **DeepSeek "Super-agent Design Exploration" article** | The four super-agent traits as *composition patterns*: two-layer Router/Worker with Thread delegation, event-driven triggers with two-level admission filtering, subscription passthrough (many-to-many fan-out), Inline/Spill context shaping + Chain of Recall, speculative execution, async `start()`/`stop()` tools, channel-matched output, the watchdog session, and the §7.1 ablation methodology | `kian-superagent` (bundle, not core): Router/Worker profiles, `channels`, `subscriptions`, `classes`, `shaping`, `recall`, `speculation`, `watchdog`, `async_tools`; the ablation benchmark is `crates/kian-superagent/tests/ablation_passthrough.rs` |
| **deepseek-harness (dsh)** | The harness/kernel split: a harness owns interaction and capability; the substrate owns durability. dsh's append-only `SessionEvent` log, its turn/step vocabulary and its "model-visible means logged" rule shaped Kian's event schema and context derivation | `kian-core` event schema + `derive_model_context`; the dsh bridge `kian-dsh` (ADR-010) maps dsh session events onto **existing** kian kinds — zero new kinds |
| **Cordis** | Plugin mechanics: context as a service container (`ctx.<key>`), `inject`-declared dependencies, typed events, and registrations as reversible effects with disposers | `kian-plugin`: `Ctx` registry, `Plugin` trait, five-mode dispatch, `Disposer`; new capability grows config-first → plugin → seam (ADR-004) |
| **The durable-execution design work behind this project** | The core itself: Session as the sole first-class entity, event stream as sole truth, drain as the execution hold interval, the Admit → Wake → Drain → Promote invariants, the epoch-fencing/lease split, and the three-layer recovery model | `kian-core` (state machine, invariants), `kian-store` (CAS + fencing), `kian-orchestrator` (engine + recovery), and the ADR series ([ADR-001](../design/decisions/ADR-001-rust-foundation.md) … [ADR-011](../design/decisions/ADR-011-inbound-webhook-plane.md)) |

The narrow-waist rule (constraint 10) is what keeps the bundles out of the core: the article's
Router/Worker architecture, the channels, the dsh harness and the endpoint daemons are all
*compositions* over core primitives, and the core crates name none of them.

## 7. Read next

| If you want to… | Read |
|-----------------|------|
| see it run in a minute | [README Quickstart](../README.md#quickstart) |
| know what the system promises | [SPEC.md](SPEC.md) (WHAT) |
| know how it is built | [DESIGN.md](../design/DESIGN.md) (HOW) |
| know why a choice was made | [design/decisions/](../design/decisions/) (WHY) |
| see what landed when | [../CHANGELOG.md](../CHANGELOG.md) |
| see what a run looks like | [../docs/advanced.md](advanced.md) |
