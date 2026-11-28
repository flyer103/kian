# Kian — Spec v0.1

> Spec = WHAT. Design lives in [design/DESIGN.md](../design/DESIGN.md).

## 1. Positioning

**Kian is an open-source, model-neutral, self-hostable durable execution runtime for agents**,
built on three promises: agent work is **killable, resumable, migratable and auditable**
(durable execution), compute runs on **machines you own** (BYOM), and the **loop stays yours**
(harness integration).

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
| P9 | Long-range memory | Knowledge must survive the session boundary: notes, offline consolidation (dreaming), a bounded injected index — without forking the event stream |

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
4. Workers report back on completion; the Router replies on the channel (IM ≤300-unit replies — a details request is delivered whole — / structured PR delivery)
5. The endpoint daemon (BYOM) executes commands, events flow back; watchdog monitors endpoint health
```

**C. Harness integration (being-integrated line)**
```
1. The harness plugs its loop driving authority into Kian (replacing its self-managed loop)
2. The harness's models/tools/sandboxes mount as seam providers
3. Kian provides durable state, recovery, control plane, audit
   This line composes with B: a harness loop can serve as a Kian Worker wholesale.
4. Bridges under this line: dsh (ADR-010), plus Claude Code and Codex (phase 1,
   design/HARNESS-BRIDGE.md, ADR-043). A harness-owned loop is a projection-only session
   unless the harness exposes the assembled model request; the class bounds what the session
   may claim (audit, cache accounting) and is durable on `session/created` for spawned workers.
```

### 2.3 Configuration interface (v0.1 draft)

```yaml
# kian.yaml — single-file configuration (narrow waist)
storage:
  event_store: sqlite # v0: sqlite | postgres (postgres implemented:
                             # requires storage.postgres.url_env — env-only URLs,
                             # never a config value — and an explicit unique
                             # drain.holder; both refuse to start otherwise)
  projections: sqlite

models: # model slots (P6: determinism in Kian)
  - slot: main
    provider: openai # adapter name (plugin-provided)
    model: gpt-5.2
    fallback: [kimi-k2, deepseek-v4]
  - slot: router # cheap fast slot for the super-agent Router
    provider: deepseek
    model: deepseek-chat-v4

model_profiles: # capability facts, config-first (enforced by the request planner from phase 1)
  deepseek/deepseek-flash:
    dialect: openai_chat
    context_window: 1000000
    max_output: 384000
    reasoning: true
    thinking: true # explicit toggle; absent leaves the provider default
    effort: high # low | high | max; requires reasoning
    vision: true
    strict: true
    cache: true

pricing: # read-only cost projection (kian cost, ADR-053); absent = cost unavailable
  currency: USD
  peak_multiplier: 2.0 # applied inside a peak window
  peak_windows:
    - weekdays: [mon, tue, wed, thu, fri]
      start: "01:00"
      end: "04:00"
      timezone: UTC
    - weekdays: [mon, tue, wed, thu, fri]
      start: "06:00"
      end: "10:00"
      timezone: UTC
  holidays: ["2026-10-01"] # off-peak regardless of weekday
  holiday_timezone: Asia/Shanghai
  models: # off-peak base prices, per million tokens
    deepseek/deepseek-flash: { cache_hit: 0.003, cache_miss: 0.15, output: 0.6 }

sessions:
  default_agent: default # points at an agent definition
  inbox_admission: durable # A7
  drain:
    max_iterations_per_drain: 200
    recovery_budget: { tokens: 500k, wall_clock: 30m }

recovery: # A4/A5: pure-function classification + declarative policy table
  rules: # an explicit table replaces the defaults, so list every class this deployment governs
    - class: rate_limited
      action: { backoff_retry: { delay_ms: 500 } }
    - class: context_overflow
      action: compress_then_retry
    - class: provider_down
      action: failover

control_plane: # P5 protocoled control plane
  steer: enabled
  interrupt: { grace: 5s } # settle-on-abort (invariant 6)
  confirmation: { mode: policy }

plugins: # Cordis-inspired plugin loading
  - name: lark-adapter
    config: {... }
  - name: super-agent-router
    config: { im: { reply_units: 300 } }
```

### 2.4 API surface (v0.1 draft; details in DESIGN)

```rust
// SDK shape (placeholder; DESIGN is authoritative)
let mut session = kian.create_session(CreateSession { agent: "default",..Default::default }).await?;
session.submit(Submit { input: Input::Message("investigate the vadis R75 CI failure"),.. }).await?; // Admit (durable first)
                                                                                                       // Wake is advisory
let projection = session.projection.await; // state is read only from the projection
session.steer(Steer { text: "switch to plan B",.. }).await?;
session.interrupt(Interrupt { reason: "user stop",.. }).await?;
let events = session.events(..).await; // audit = an event-stream interval
```

The normative wire contract is [design/API-CONTRACT.md](../design/API-CONTRACT.md): endpoints,
JSON-RPC methods/params/results, error codes, SSE frames, metrics keys, the webhook status matrix and
KiandProto. This section keeps the interface sketch.

### 2.5 Acceptance capabilities

Each capability below is proven by named tests; the executed evidence is recorded in
[overview.md](overview.md) §5.

| Capability | Demonstrates | Input | Output |
|------------|--------------|-------|--------|
| Crash recovery | kill -9 mid-drain | A half-finished task | session → recoverable → new drain continues, zero lost work |
| Steer / interrupt | Redirect or stop while running | steer "plan B", interrupt | Redirection lands at a safe boundary; interrupt ≠ failed |
| Cache stability | Consecutive requests in one session | Requests with unchanged history | Cache hits (stable prefix bytes) observable above expectation |
| Super-agent demo | IM request → PR delivery | Group-chat message + CI events | Router dispatches Worker; structured PR description; IM reply ≤300 units (a details request is delivered whole) |
| BYOM | Endpoint daemon execution | Local repository commands | Cloud loop + endpoint execution; watchdog detects endpoint offline |
| Harness integration | dsh loop as a worker | Harness config | dsh interaction unchanged, made durable |
| Audit export | Event-stream interval export | Time/causal interval | A replayable audit package (byte-identical) |
| Multi-replica takeover | Crash recovery at multi-process scale | Two `kian serve` replicas over one postgres DB; the drain owner is `kill -9`'d mid-drain | The survivor's recovery sweep takes over the expired lease and completes: zero lost work, exactly one end write, a stale-epoch probe write from the dead replica rejected (NoSplitBrainWrite between processes) |

The multi-replica takeover is demonstrated by the env-gated postgres e2e (`KIAN_PG_URL`); the proof
is recorded in [overview.md](overview.md) §5.

## 3. Operations Interface

### 3.1 Observability
- Metrics: session state-machine transition counts, drain depth/duration, pendingWake depth,
  cache hit rate / invalidation causes (A3), recovery action counts and budget consumption (A5),
  evidence-gate verdicts (A6).
- Trace: every event carries drain_id + epoch (fencing); cross-session delegation (Thread lineage)
  chains into a causal trace.
- Audit: event-stream interval export + replay (A1/A2).
- Self-inspection: `kian self inspect` reads one window of the durable stream and answers what the
  runtime did and where it is failing — event volume by kind, session ends by reason, turn outcomes
  and provider error classes, tool failures by tool, media items with or without a transcript,
  evidence-gate verdicts, and the store/artifact footprint; the usage and cache projections appear
  under `lifetime`, since they are lifetime totals (session-filtered with `--session`), never
  windowed. It is a pure projection over the log with a stable top-level key set (consumers ignore
  unknown keys); it runs offline against `--db` without a server, and it never writes, starts or
  stops execution. Content-bearing payloads are counted, never printed; every finding names the
  session and seq that evidence it.
- Evidence pack: `kian self evidence --session <uuid> --seq <n> --out <dir>` names one anchor row
  and writes a handoff package a reviewer — human or agent — can read without the store: the raw
  envelope slice (±`--context` rows, default 10, refused above 200), `session.json` (projected
  state and the end writes), `repo.json` (checkout commit/branch/dirty and the crate map),
  `inspect.json` (the same self-inspection projection over the session's whole stream) and a
  `manifest.json` carrying the store/config/repo identity plus the sha256 of every file. Payload
  text travels only inside the package and never to stdout — the terminal gets a summary and
  `--json` the manifest. A pack is complete or absent: it is built in a temp sibling, re-read and
  re-verified, then renamed into a path that did not already hold data.
- Scripted replay: `kian session replay <uuid> --provider fake --script <file>` re-drives one
  recorded drain episode on a scripted model instead of only re-projecting it, so a prompt, tool
  schema or loop change can be compared against what the recorded bytes actually did. The anchor is
  a `drain/claimed` seq — the last episode by default, `--from-seq` selects an earlier boundary;
  the prefix is forked into a temp store, the episode's admitted rows are re-entered as inbox rows,
  and the built-in loop runs against a tool surface that replays the recorded results. The diff is
  `reproduced`, `diverged` (the first row that differs in kind or payload, with a bounded excerpt)
  or `incomplete` (a recording gap the run improvised around, always with its notes); every verdict
  exits 0, because a divergence is a finding, not a failed command. It refuses a live lease, a
  mid-episode steer, a spilled input pair and an input shape it cannot invert; the fork is a temp
  store, `--keep-fork` leaves it for inspection, and the source store is only ever read.

### 3.2 Operational actions
- `kian` CLI: serve (single machine), session inspect/replay/export (`--with-event-ids`),
  wake-now, lease inspect, self inspect, self evidence, scripted replay (`--provider fake`),
  recovery budget reset, cache-stats, usage-stats, cost, raise-budget, tickets, channel credentials
  (`kian wechat login` — the QR flow writes the WeChat channel's credential file; no other channel
  needs an interactive login).
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

The event envelope, the complete kind catalog, payload contracts and the event-change protocol
are normative in [design/EVENT-MODEL.md](../design/EVENT-MODEL.md). The memory model — notes,
dreaming, injection, representations — is normative in
[design/MEMORY-MODEL.md](../design/MEMORY-MODEL.md).

## 5. Scope

### MVP: durable sessions
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
| Acceptance | crash recovery, steer/interrupt, cache stability |

### Super-agent bundle and BYOM endpoints
| Module | Contents |
|--------|----------|
| Router/Worker two-layer composition | Thread delegation |
| Event subscriptions + channel adapter framework | IM/GitHub first |
| kiand endpoint daemon (BYOM) | + watchdog |
| Evidence gate v1 | A6 |
| Acceptance | super-agent demo, BYOM |

### Recall, admission economics, channels

What landed, recorded against the contract. The acceptance capabilities in §2.5 are unchanged —
only status is recorded here.

| Module | Contents | Status |
|--------|----------|--------|
| Inline/Spill admission shaping (the `input/spilled` kind) | Oversized channel payloads admit as a bounded Inline view in model context; the verbatim raw is appended in-stream, out of context, in the same atomic consumption write | landed |
| Recall toolpack (Chain of Recall) | Read-only `recall_event` / `recall_neighbors` / `recall_thread` over `RecallPort`; outputs are themselves Inline/Spill-shaped so multi-hop recall composes; Router global observability rides the event stream via recall, not Router context | landed |
| Two-level admission filtering | Fast-path `classes:` table (`candidate | explicit-only | ignore`, keyed by channel/topic/sender, most-specific-wins) evaluated before the rule set and before any model call; the judge prompt is the natural-language policy extension point | landed |
| Async `start`/`stop` tool pattern | `start` acks immediately with a job id; the completion returns as an admitted input (`async_result` envelope) promoted at the next safe boundary; `stop` cancels honestly; job rows are pre-event durable state ([ADR-008](../design/decisions/ADR-008-async-tool-completion-admitted-input.md)) | landed |
| Many-to-many passthrough fan-out | A topic maps to the set of workers that served it; one event admits one durable row per subscriber, idempotent per `(session, event_id)`; empty set falls back to the Router | landed |
| Speculative-execution profile | Additive `speculative: true` delegation marker; stream-derived `max_outstanding` cap enforced with honest tool errors; discarded speculation audits everything and delivers nothing | landed |
| Watchdog Session v2 | Model-in-the-loop progress judge: a `type: timer` channel routes ticks to a fixed session via `route: {session: <uuid>}`; `intervene` admits a steer row durable-before-wake, `escalate` replies under the IM policy; v1 endpoint heartbeat untouched | landed |
| Batching metrics (§7.2 shape) | `superagent.router_batching` series on `/metrics`: batch size, turn wall duration, amortized ms/input; metrics-off is behavior-identical | landed |
| Ablation benchmark (§7.1 shape) | Deterministic with/without-passthrough comparison in kian's native units (Router judge turns + request bytes); directionality asserted, both configurations must complete | landed |
| Untrusted-input marking | `"untrusted": true` in the provenance wrapper for configured channels (real channels default untrusted); judge-prompt and recall-hop hygiene | landed |
| Real lark channel adapter | Feishu IM receive/send over the open APIs; env-only credentials (`KIAN_LARK_*`), fail-closed startup; default-untrusted | landed |
| WeChat iLink channel | `type: wechat`, **best-effort by contract** ([CHANNEL-WECHAT.md](../design/CHANNEL-WECHAT.md), [ADR-018](../design/decisions/ADR-018-wechat-ilink-channel.md)): one long-poll loop per channel over Tencent's reverse-documented ClawBot iLink API (`getupdates` ~35 s hold), `message_id` → `event_id`, the config `topics` table as the receive allowlist, reply-only sends echoing the latest inbound `context_token`, QR-issued bot token in a `0600` credential file written by `kian wechat login` (no env var), `errcode -14` → pause + resume on file change, empty cursor with no position table, text-only/single-account phase 1, forced untrusted; reply tokens additionally persist **best-effort** in a TTL-bounded `0600` state file beside the credential file ([ADR-026](../design/decisions/ADR-026-wechat-context-token-state.md)) so a restart can still reply; no new surface, no new event kind | landed |
| iMessage channel (macOS bridge) | `type: imessage`, **best-effort by construction** ([CHANNEL-IMESSAGE.md](../design/CHANNEL-IMESSAGE.md), [ADR-042](../design/decisions/ADR-042-imessage-channel.md)): iMessage has no API, so the channel long-polls a Mac-side `kian-imessage-bridge` (`imsg rpc` under Full Disk Access + Automation) over a loopback HTTP ABI; `guid` → `event_id`, `topics` is both allowlists, the bridge's `participants` is a privacy filter, text-bearing direct chats (attachment metadata is mapped into the payload; with `attachments: bytes` plus a `media:` section, image and audio bytes become OCR/ASR transcripts — [ADR-064](../design/decisions/ADR-064-imessage-attachment-bytes.md); `voice: true` adds a spoken reply path — [ADR-067](../design/decisions/ADR-067-imessage-voice-io.md)), one channel per config, forced untrusted; sends are **not** reply-only (any allowlisted handle) with a deterministic `client_id` that makes the outbox retry effectively once; revoked Full Disk Access pauses receive but sends continue, `-1743` denies sends but receive continues; a multi-attachment row settles before admission, bounded by a five-second wait, and a row over the eight-item media cap loses the excess with an audit line ([ADR-070](../design/decisions/ADR-070-imessage-multi-attachment-settling.md)); no new surface, no new event kind, no kian-side position table. L1/L2 landed; a first live L3 round trip (real Mac + iPhone) is recorded 2026-11-20; the attachment-bytes path is L1/L2 ([ADR-064](../design/decisions/ADR-064-imessage-attachment-bytes.md)) | landed (L1/L2/L3) |
| Plugin SDK + Footprint Ladder | Static plugin system + Footprint Ladder landed across the capability sets; SDK extraction → the hosted set (below) | partial |
| Audit export/replay tooling | `session export`/re-import (byte-identical) + delegation-audit replay assertions — demonstrated by earlier work, recorded here; no new work | **Demonstrated** |

### Hosted foundation, SDK freeze, ecosystem surfaces

This capability set landed the items the previous set deferred. The acceptance capabilities in
§2.5 are unchanged — only status is recorded here. Zero new event kinds;
the durable set stays 20.

| Item | Contents | Status |
|------|-----------------|--------|
| Postgres EventStore + inbox (dual-backend) | `kian-store` backend dispatch (`Sqlite \| Postgres` enum, identical public surface); postgres dialect SQL (single-statement end write, CTE batch, PK-race classification through the HEAD path); export parity vs sqlite is the compat golden | landed |
| Postgres exec + assembly | `kian-exec` dual-backend; `storage.event_store: postgres` with env-only URL (`storage.postgres.url_env`) and an explicit unique `drain.holder`, both fail-closed; one server, one database | landed |
| Multi-replica lease/fencing | Conditional claim admission + unique holders, postgres LISTEN/NOTIFY advisory wake transport (notify-after-commit; sweep + startup scan bound loss) and exec row leases — [ADR-009](../design/decisions/ADR-009-multi-replica-claim-admission.md). **Demonstrated** by the env-gated e2e (`KIAN_PG_URL`, default CI excluded) | landed |
| kian-sdk | The wire-only embedded SDK (frozen surface, typed methods, SSE helper, matchable errors, golden wire fixtures); **frozen additive-only** after — a breaking change requires a new design decision | landed |
| GitHub adapter | Channel mapping + structured send (untrusted-by-default, env-only token) and the verified webhook receiver — a fourth server surface, verify-then-parse, admit-only ([ADR-011](../design/decisions/ADR-011-inbound-webhook-plane.md)) | landed |
| dsh harness integration | The dsh SessionEvent → existing-kind mapping contract + conformance fixture ([ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md)) and the `kian-dsh` bridge LoopDriver (`superagent.harness.dsh`, default off) | **demonstrated — fixture-proven; live leg executed (honest negative), the live leg stays open** (fixture-proven in CI; the live leg is env-gated on `KIAN_DSH_BIN`, ran against real dsh 0.2.1-rc.5 on 2026-11-01 and stopped at an honest `SoftFailed { reason: "peer never sent ready within 30s" }` — the stock dsh SDK app does not meet the [ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md) contract-v0 bar — so the live leg stays open; not claimed as CI-proven, the fixture suites remain the CI proof, outcome recorded in the README run log) |
| Per-agent harness selection | One serve declares several harnesses (`superagent.harness.{dsh,claude,codex}`) and selects the worker inner loop per agent (`superagent.worker.harness` / `superagent.roster[].harness`: `kian` \| `dsh` \| `claude` \| `codex`); bridged agents own their tools (no `tools:`/`tools_allow`, no `exec_worker`), the `session/created.bridge` marker is per spawn, and a config with no selection keeps the legacy single-harness rules — [ADR-049](../design/decisions/ADR-049-per-agent-harness-selection.md), contract [HARNESS-BRIDGE](../design/HARNESS-BRIDGE.md) §6.1/§6.2 | **demonstrated** — L1 config matrix + tool-ownership refusals, L2 one serve / one Router entry spawning a kian-loop child and a Codex child with their distinct markers and classes, and a live local run (real Codex CLI 0.166 + a real model on the kian-side worker) recorded in the PR |
| A2A boundaries | The exploration note below — no implementation | parked (note) |

#### A2A boundary note (exploration; no implementation)

The context-sharing boundary in kian is the **Session/Thread boundary**: a Session is the
maximal context-sharing unit, and Threads cross it only with explicit, auditable lineage. Any
cross-instance A2A experiment (agents on different kian deployments, or a kian agent talking to
a foreign agent) crosses that boundary with unshared context and distinct permission
boundaries; it is therefore a *new surface class*, not an adapter, and has no acceptance
scenario behind it yet. **Preconditions any future experiment must state before an implementation
exists**: (1) the authenticated channel — how the remote party authenticates and how event
provenance survives the hop (anything arriving over it is untrusted-marked by default, the untrusted-marking
rule); (2) the trust model — whose permission boundary evaluates whose actions, and how a
foreign action is labeled in the audit stream; (3) the failure/honesty model — interrupt and
recovery semantics across the boundary (interrupted ≠ failed applies unchanged). An experiment
that skips stating these freezes speculation; none was attempted.

### Live positives, dogfood, field fixes

The previous input's honest negatives became live positives, and the run found two real bugs —
status recorded here.

| Item | Contents | Status |
|------|---------|--------|
| live leg — contract-v0 peer | `contrib/dsh-v0-peer/dsh-v0-peer.mjs` (zero-dep Node) adapting stock dsh JSON-RPC onto the frozen [ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md) v0 contract; live run green (real `deepseek-chat`, full mapped chain, honest `Settled`) — supersedes the 2026-11-01 honest negative | landed |
| Dogfood workload | an out-of-tree issue-watch harness: TimerChannel → `gh issue list` → Router triage → worker draft (dry-run) on the real `flyer103/kian-runtime` repo; its restart demo proves a kill -9 reboot resumes the same session; the run log records real data | landed |
| Wire dialect v2 (breaking) | Strict OpenAI-compatible serialization: dotless built-in tool names (`fs_list`/`fs_read`/`fs_write`, dotted forms replayable), wire-level `tool_calls`/`tool_call_id`, `AgentDef.tools` fail-closed. Found live via DeepSeek HTTP 400/422; constraint-3 event in DESIGN (golden hash same commit) | landed |
| Router re-arm | Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident Router with an observable `[re-arm]` cause input; soft-failed sessions re-open the same pinned session. Zero new kinds; DESIGN §4.6 | landed |

### Bundle-domain policy layer

Adoption of the reference implementation's domain-policy layer onto kian's substrate.
**Everything
landed at bundle-layer (`kian-superagent`, plus read/assembly slices of `kian-cli` and the
the issue-watch dogfood); zero new event kinds — the durable set stays 22; every addition
is an additive payload key on a frozen wrapper, a disposable projection, or a config/template
artifact.** Semantics: DESIGN §4.7; operator surface: [advanced.md](advanced.md) bundle-policy and operator-surface sections.

| Item | What landed | Status |
|---|---|---|
| Ticket stage projection | "What is it waiting for" as a pure derived projection over the worker stream + inbox + outbox view (first-match table); additive `state` key on completion-report content; read surface `kian tickets [--json]` | landed |
| Closed reason vocabularies | Closed `IntakeReason` codes as `reason=<code>` on every drop/admit audit line + per-code metrics counters; judge-side `decision:<accept\|stop\|ignore\|unavailable>` tag convention in the default template | landed |
| Output-side speak-rules | The whether-to-send gate before output shaping: `superagent.speak_rules` over `always \| ack-first-only \| decision-needed \| result-ready \| outcome-only \| never`; suppression = honest tool error + audit + counter, no send; `SpeakTracker` ack-once memory rebuilt from the stream; `outcome-only` drops the dispatch acknowledgement from a per-hold slot (never persisted) | landed |
| Follow-up ownership routing | Registry liveness projection (`live \| ended`); fan-out admits to live subscribers only; an all-ended topic releases to the Router (`reason=released_to_eval`, fresh Eval); `spawn_worker` refuses a live worker's topic naming the owner | landed |
| Mandate (delegation authority scope) | Additive `mandate` object on the frozen delegation wrapper (immutable, verbatim in `thread/started`, model-visible as the worker's first message); single issuance at `spawn_worker`; revalidation before follow-up routing — [ADR-012](../design/decisions/ADR-012-delegation-mandate.md) | landed (ADR-012 landed) |
| Thread/Correlation intake projection | Config-owned canonical thread keys `<channel>:<group>#<key>` used as routing topics; stream-derived read positions (lark/GitHub cursors) or honest idempotent re-poll — no position table; correlation adds context, never routes; fan-out grants no authority — [ADR-013](../design/decisions/ADR-013-thread-intake-projection.md) | landed (ADR-013 stays a draft record) |
| Outbox+receipts bounded retry | Reuse audit verdict: the outbox is a disposable projection over `tool/call` intent + `channel/delivered` receipt plus a bounded driver — no new durable state; 2s→60s backoff, `max_attempts` 6 then parked honestly (`delivery-retry` stage, named on `kian tickets`); per-(ticket, conversation) ordering; additive `attempt`/`first_attempt_ms` on new receipts; park survives reboot ( +, plan) | landed |
| Policy/prompt versioning + drift marking | `[policy <label>]` header added at render (template text untouched, persisted prompts replay byte-identically); boot-time drift marking (audit + counter, live sessions never rewritten); config-gated one-shot policy-changed marker input | landed |
| Issue-watch multi-Ticket dogfood | GitHub poll-receive mode beside the webhook (`since` cursors, bounded backfill); the issue-watch v2 harness composes the whole stack — one Ticket per live issue, thread-keyed topics, mandate-bounded triagers, speak-rule deliveries through the outbox; v1 kept as the timer config | landed |
| live validation | The eight-step validation protocol executed live against `flyer103/kian-runtime` (real DeepSeek + GitHub REST, `DRY_RUN=1`): steps 1, 4, 5, 6, 7, 8 PASS with captured evidence; steps 2–3 honest negatives (structural `spawn_topic` gap — no live topic binding), every claim cited by session id + seq; the gap report filed the gaps (the issue-watch run log §7) | executed (with honest negatives) |
| gap fixes | The four gaps, all landed: the `User-Agent` stamp on every github/lark HTTP client + `flaky-proxy.py` delivery-leg fault mode; `spawn_topic` accepts the live spawn spellings — live topic binding, mandate strength and outbox ticket aliasing work live; outbox park rebuilt at boot and surfaced per-ticket on the stage board; exec secret hygiene — shared core redactor + env policy, `env` dumps never persist secrets (the one cross-bundle-fence commit, filed separately) | landed |
| Multiagent delegation (session-internal + roster) | The multiagent contract: delegation = a **child session** via `Thread` (one mechanism, no lighter tier) with a derived spawn identity (`spawn_uid`) so a replay cannot mint a duplicate child, and a completion delivered as a **pair of writes** with a reconciliation rule; cross-session composition = existing Thread/Mandate/subscription/`agent.id` + a config agent roster over `AgentDef` (`superagent.roster`, selected only through a read-only `list_agents`); the board ruling — a disposable `task_board` view plus a control path recomputed from the primary, **never** a board entity and never a control read on the view; a closed nine-value task-outcome vocabulary (an empty answer is never a success) plus a separate two-value spawn outcome; config-only caps (`max_children` 4, `max_depth` 1). **Zero new event kinds, no new table/route/ops method/metrics key, no core change**; **no event-payload key set changes before P4** — P1 adds message content inside new delegation wrappers and new reports, P2 adds three `spawn_worker` arguments (one constraint-3 change), and the two additive `thread/*` audit key groups are a P4 owner gate — [MULTIAGENT-MODEL](../design/MULTIAGENT-MODEL.md), [ADR-029](../design/decisions/ADR-029-multiagent-delegation-and-board.md) | implemented P1–P4 (tests only — no live-run evidence) |

Known deviations, kept visible (the honest v1 record): a kian **Ticket v1 = one worker
session** (one `thread/started`), not the prototype's full resumable lifecycle; **an ended
ticket resumes via fresh Eval** (release-to-Router, new accept, new mandate), not same-Mandate
resume; **the panel is the CLI** (`kian tickets` — the stage board), not a web UI; and the
**WeChat channel's credential is a QR-issued session token in a local `0600` credential file**
(written by `kian wechat login`), not an env-var static secret — the one deliberate break from the
secrets discipline, bounded and recorded in
[ADR-018](../design/decisions/ADR-018-wechat-ilink-channel.md).

### Event-model enhancements

Event-model ideas from Anthropic's Claude Managed Agents catalog — provider usage facts, a
persisted interrupt intent, derived event ids, redaction/permission provenance — were borrowed as
**design inspiration only** (no wire-protocol dependency). **Exactly one new event kind
(`control/interrupt`): the durable set grows 20 → 21.** Everything else is an additive payload key on a frozen wrapper, a pure
projection, or a config-first capability; no model-request or context-projection byte changed
anywhere (cache-byte stability, constraint 3). Semantics: DESIGN §3.1/§3.2/§3.3/§6.2; operator
surfaces: the README's operator-surface section.

| Item | Contents | Status |
|------|-----------------|--------|
| Provider usage + structured recovery on `turn/end` | `ModelChunk.usage` additive seam; the openai-compat provider parses SSE `usage` objects verbatim (last frame wins; a usage-only frame never flips the empty-response liveness flag); `turn/end` carries `"usage"` verbatim plus `"error": {class, action}` on soft-failure/error outcomes ( classifier + declarative policy, `"none"` when no rule); pure projection `kian-cli::usage_metrics` powers `kian usage-stats`, the `GET /metrics` `"usage"` aggregate and `session inspect` usage lines — no pricing, no normalization in core. Tests: `usage_frame_at_stream_end_is_parsed_verbatim_after_content`, `mid_stream_and_final_usage_frames_both_surface`, `usage_only_stream_is_still_an_empty_response`, `turn_end_carries_usage_for_tool_calls_and_final_answer` (incl. the A2 context golden), `turn_end_soft_failure_carries_partial_usage_and_classified_error`, `turn_end_without_usage_is_byte_identical_to_the_legacy_shape`, `recovery_audit_carries_class_and_policy_action`, `usage_surfaces_inspect_stats_and_metrics_over_a_seeded_store` | landed |
| Durable interrupt intent | The 21st kind `control/interrupt` (drain-activity, non-context; [ADR-014](../design/decisions/ADR-014-durable-interrupt-intent.md)): `interrupt` admits a reserved control-envelope inbox row (idempotent per `(session, target_epoch)`) alongside the in-memory flag; the owning drain consumes it at a safe boundary, appends the audit event and settles `interrupted` — so the intent survives a drain crash and crosses replicas best-effort. Stale rule: a row whose target episode already settled appends `"stale": true` and ends nothing; idle/terminal sessions keep `not_draining`/`unknown_session` and admit nothing. Tests: `durable_interrupt_intent_survives_crash_and_takeover_settles_interrupted`, `repeated_interrupts_in_one_epoch_dedupe_to_one_row_and_one_event`, `stale_epoch_control_row_is_audit_only_and_does_not_end_the_session`, `control_row_racing_the_settle_flips_the_end_write_to_interrupted`, `interrupt_of_idle_or_terminal_session_admits_no_control_row`, `control_interrupt_leaves_model_context_unchanged`, `additions_only_regression_all_kind_bytes_are_frozen` (+ golden count 21) | landed |
| Derived event id + redaction provenance | `kian_core::events::event_id` = `"evt_" + uuid5(session, "kian:event:<seq>")` (pure, never stored, never an envelope field), mirrored dependency-free in the SDK; `kian session export --with-event-ids` (opt-in, default export bytes unchanged, re-import round-trips). `SecretRedactor::redact_with_report` emits `{kind, name, count}` provenance; the report rides the additive `redactions` key on `tool/result` payloads outside `content` — what was redacted, never the values. Tests: `event_id_golden_vectors_match_rfc4122_v5`, `event_id_is_deterministic_and_distinct_per_seq_and_session`, `event_id_matches_the_core_golden_vectors`, `default_export_bytes_are_unchanged_without_the_flag`, `with_event_ids_export_adds_stable_ids_and_round_trips`, `report_counts_all_three_kinds_and_text_matches_plain_redact`, `remote_exec_result_and_row_carry_the_redaction_report`, `tool_result_payload_carries_redactions_outside_content`, `redactions_payload_key_never_enters_the_model_context` | landed |
| Per-call tool permission provenance | `agent.permissions` (tool name → `allow\|deny`, closed vocabulary, fail-closed parse: unknown value / key ∉ `tools` / table on a judge agent refuse boot; [ADR-015](../design/decisions/ADR-015-tool-permission-provenance.md)): evaluated once per call **before** the `tool/call` append (log-before-execute holds on both arms), recorded as the additive top-level `permission: {evaluated, policy, matched}` key — allows are audited exactly like denials; a deny never invokes and appends an honest-error `tool/result` the model reacts to; absent table = payload byte-identical. Tests: `permission_is_a_closed_vocabulary_fail_closed`, `permissions_unknown_value_refuses_boot`, `permissions_key_absent_from_agent_tools_refuse_boot`, `permissions_on_judge_agents_refuse_boot`, `allow_path_invokes_tool_and_records_provenance_default_vs_configured`, `deny_path_does_not_invoke_and_emits_honest_error_result`, `absent_permissions_keep_tool_call_payload_byte_identical_to_today`, `permission_payload_key_never_enters_the_model_context` | landed |
| Final record | This record + DESIGN/README alignment + docs-map link check | landed |

Out of scope (recorded, not silently dropped): the `ask` permission/confirmation plane
(ADR-015 records the shape; blocking confirmation is a separate design decision), CMA wire
compatibility, and cross-replica interrupt latency beyond "next safe boundary" (ADR-014).

Cost conversion is no longer out of scope at the projection layer: usage still persists verbatim
and no event carries a price, but `kian cost` prices a stream from the deployment's config
`pricing:` book and reports exact integer micro-currency totals ([ADR-053](../design/decisions/ADR-053-economics-cost-projection.md)).
The core never normalizes or stores cost, and the book is a deployment fact rather than event
state, so editing a price re-renders history instead of corrupting it.

### Agent memory (P1–P2, P3a and P3b-1 landed; P3b designed)

Scope: knowledge that survives a session boundary (P9). The normative contract is
[design/MEMORY-MODEL.md](../design/MEMORY-MODEL.md); the rationale is
[ADR-016](../design/decisions/ADR-016-memory-as-events.md). Phase 1 (notes + dreaming), Phase 2
(creation-time injection), Phase 3a (read-time retrieval + representations) and Phase 3b-1
(index-accelerated memory reads) have landed; the P3b remainder records the agreed later phase,
not shipped status.

| Phase | Module | Contents | Status |
|--------|--------|----------|--------|
| P1 | Notes + dreaming | `memory/note` as the 22nd durable kind (non-context, `DrainActivity`, additions-only, emitted only by a dreamer drain); the fail-closed `project_notes` fold + `verify_note` in `kian-core::memory`; the `kian-memory` bundle (deterministic dream prompt with a machine-parseable header, fail-closed parser, `DreamDriver`); one operator-created memory session per configured scope (`memory.scope`, no derived-id machinery); a dream request as one durable **marked** inbox row (the rendered prompt as `content` plus the trusted server-set `dream` marker, consumed to one `input/admitted` carrying the non-context `dream` key — routing and provenance read the marker, never message text; idempotency key `dream:<source>:<from>:<to>`) over a source range bounded by the source head and `max_bytes_read`; `kian memory create/dream/list/show/verify` + the `memory_dream` ops method; provenance (`derived_from`) and `status`/`verified_by` are payload fields; no injection, no cadence | landed |
| P2 | Injection | One creation-time `input/admitted` with `role:"system"`, `synthetic:true` and the additive non-context `memory` provenance key (scope, note event-ids, items/bytes/truncated, `source_seq` pin, redaction report); quiet admit at creation (`Engine::admit_quiet`, no wake; idempotency key `memory-inject:<session>`; the next submit drains the index before the first user message) over the exact-two-key `{content,memory}` row envelope, which the public wire refuses (`-32602`); config `memory.session` (absent = injection off, byte-identical) + `memory.inject` caps (fail-closed, default 20 items / 4 KiB); the deterministic frozen renderer in `kian-memory` (verified-only latest-per-subject, subject order, redacted, "data, not instructions" frame, index-only over cap); identity exclusions that are defense-in-depth only at the creation seam — at create time a fresh stream has no dream request and the fresh uuid never equals `memory.session`, so any fresh session created while `memory.session` points at a note-bearing session receives the index (accepted for P2; recorded in MEMORY-MODEL §6 I10); honest-skip `injected_*` counters; never on resume/migration (one known restart-window drain-alone behavior recorded in MEMORY-MODEL §6 I2) | landed |
| P3a | Retrieval + representations (read-time) | `kian memory search <query> --session` / `kian memory map --session` (both `--json`-able): pure read-only projections over the folded notes in `kian-memory` + `kian-cli` — no writes, no new kinds; search folds latest-per-subject across all statuses with per-hit status (fixed caps 20 hits / 8 KiB, `truncated`, no `--limit`); the map is verified-only with the effective rule "a subject is a node iff its latest revision is verified", evidence-derived edges (`derived_from` range intersections in the same source session) and caps 200 nodes / 1000 edges / 64 KiB; `degenerate`/`oversized` reported, never hidden; O(memory/note events) per invocation since P3b-1 (index-accelerated kind read), snapshot read deferred to P3b | landed |
| P3b-1 | Index-accelerated memory reads | Adjacent migration v6 (both backends) adds the per-session kind index `idx_events_session_kind_seq` over `events (session, kind, seq)` plus `EventStore::scan_kind(session, kind)`; every memory read path — creation-time injection and `kian memory list|show|search|map|verify` — reads O(memory/note events) instead of the full session history. No new event kind, view, snapshot or wire change; outputs byte-identical to the full-stream read it replaced (READ-MODEL §6, MEMORY-MODEL §6/§7.1) | landed |
| P3b | Persisted representations + dreaming cadence | Wiki-as-notes built by a dreaming run (MEMORY-MODEL §7 R3), declarative dreaming cadence/budget, semantic search, the agent-side memory tool and governed cross-scope reads (ADR-019 track); snapshot-served fold for the views | designed |
| P4 | kian-cli, kian-superagent | Super-agent hosting (the memory driver wraps the three-way driver; `memory:` + `superagent:` compose) and the [ADR-038](../design/decisions/ADR-038-memory-injection-for-internal-sessions.md) session bootstrap: the resident Router/watchdog, delegated workers and cron targets receive the creation-time index before their first input (fresh-only, idempotent, no-op without `memory:`) | landed |

Live-model verification (2026-11-08, env-gated): dream + injected index + read views exercised
against a real provider (`deepseek-chat`); the dream wrote 8 verified notes, 8/8 new sessions
carried the index, and a single-model 8-question A/B scored 0/8 (off) vs 8/8 (on) at ~10x prompt
tokens. Details, caveats and the README-policy note: MEMORY-MODEL §10.1.

Non-goals recorded with the phases: no in-session memory tool (P1), no mid-session memory updates
(P2), no representation as truth or unbounded build (P3a/P3b), no cross-agent shared memory (open).

### Read model and serving form (P0–P3 landed)

Scope: versioned unchanged/delta reads, resumable per-session streams, verified snapshots,
materialized views, opt-in export-only retention, and bounded-staleness reads. The normative
contract is [design/READ-MODEL.md](../design/READ-MODEL.md); the decision record is
[ADR-017](../design/decisions/ADR-017-read-model-and-snapshots.md). All phases landed
(commits `958fa76`..`1f4d669`); the incremental view rebuild landed as `c71080b`.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P0 | kian-store, kian-orchestrator, kian-cli | version + unchanged/delta reads, resumable SSE, drain context cache | landed |
| P1 | kian-store, kian-core, kian-orchestrator | verified snapshots (state + model context), tail replay | landed |
| P2a | kian-store, kian-cli | session directory, usage/stage views, kind index | landed |
| P2b | kian-store, kian-cli | opt-in export-only archive, import, retention | landed |
| P3 | kian-cli, kian-sdk | explicit-allowlist fan-in stream | landed |

Non-goals: transparent cold-tier query, wildcard streams in OSS, multi-region active-active.

### Durable per-session execution-worker binding

Scope: the owner's worker shape — a worker is a `kiand` endpoint (`endpoints[].id`), the agent loop
stays on the `kian serve` side, and every delegated worker session is bound **1:1** to one worker
for its lifetime. The decision record is
[ADR-020](../design/decisions/ADR-020-durable-worker-binding.md); semantics: DESIGN §4.1/§4.3/§4.7;
the durable shape: EVENT-MODEL §4.2 (additive `exec_worker` key on the delegation wrapper and on
both sides' `thread/started`). **No new event kind (the durable set stays 22), no new table, no
wire surface, no core change.**

| Item | Contents | Status |
|------|----------|--------|
| Worker catalog | `endpoints[]` gains an optional validated `description` (≤200 chars, no control characters — it renders into the Router prompt); the catalog (`id` + `description`, config order) feeds the `spawn_worker` schema enum and the prompt block; zero endpoints = feature off, byte-identical | landed |
| Router-chosen binding | `spawn_worker` gains the `exec_worker` argument (required whenever membership is on — a plain non-empty string; refused with zero workers); the choice is recorded as the additive `exec_worker` key on the delegation wrapper and both sides' `thread/started`, echoed in the spawn result — ADR-020 | landed |
| 1:1 exclusivity | A worker has at most one live bound worker session; the check is a stream-derived pure function (`thread/completed` + `session/ended` on the worker stream: `completed`/`cancelled` release, `failed`/`interrupted` hold until the ADR-035 release) called before any side effect through a bundle-owned read-only port; a busy refusal names the owning session and thread | landed |
| Per-drive routing | One prebuilt worker side per endpoint (its own `exec` `RemoteExecTool`, its own async pair); the driver selects the side from the session's own stream binding before the first provider turn — bound id unconfigured ⇒ honest refusing side (no re-route, no local run); legacy unbound sessions keep the first-**configured**-endpoint route with one audit line; no resident routing state (restart rebuild = replay) | landed |
| Lifecycle | Worker crash/restart/migration (same id) re-drives the durable queue (ADR-007) with the binding unchanged; lineage-before-admit spawn ordering bounds the crash window (a never-started reservation is released after `queue_timeout_ms` with an audit line); `harness.dsh` × `endpoints:` refuses boot (the harness never mounts the kian `exec`) | landed |
| Live rebind of a session to a different worker id | Designed only (ADR-020 §Deferred: durable rebind intent, old-worker queue fencing, seq-range projection — needs an EVENT-MODEL change, owner gate); the landed reattachment path is the same worker id restarting/migrating | designed |
| Deployment (Vultr + WeChat workers) | Explicitly the next discussion; no deployment tooling, no L3 claim | deferred |

Non-goals: no new transport/protocol or daemon binary, no worker pooling, no bind-time liveness
gate, no automatic failover (an offline worker parks honestly on the durable queue), no per-call
endpoint choice.

### Runtime worker membership

Scope: workers join and leave a running `kian serve` at runtime — `kiand --register-to` dials kian,
authenticates with an env-only token (the shared token, or its own token when enrolled by id —
ADR-031) on a **private** registration listener, and the accepted connection *is* the command route
for that worker id. Joining or leaving never edits `kian.yaml` and
never restarts the server; a kian restart/upgrade requires no worker-side change (workers reconnect
and re-register). The decision record is
[ADR-021](../design/decisions/ADR-021-runtime-worker-membership.md) (supersedes ADR-020 D5/D6/D7
only); the worker-domain contract is [WORKER-MODEL](../design/WORKER-MODEL.md); the wire is
KiandProto v3 ([API-CONTRACT](../design/API-CONTRACT.md) §8). **No new event kind, no new HTTP
surface, no core change; wire v3 adds only the optional `register.capabilities` field, with dual
goldens.** **Amendment (ADR-035)**: binding release and bounded idle reclaim — one ops method
(`release_worker`), one `workers:` key (`release_after_ms`), and one additive engine primitive
(`Engine::release_idle`, composed from the existing claim + batch-append primitives) — still no new
event kind, table or route.

| Item | Contents | Status |
|------|----------|--------|
| Registration (KiandProto v1) | Worker-dial `register`/`welcome`/`rejected` handshake over the same line framing; closed five-reason refusal set (`version_mismatch`/`bad_token`/`duplicate_static`/`id_conflict`/`disabled`) with typed terminal-vs-retryable reactions; worker-side `bye`; legacy static `hello`→`welcome` kept at v1; upgrade order kian-first, no v0 shim — API-CONTRACT §8 | landed |
| Membership directory + liveness | Runtime-only directory (id → description, version, generation, last-seen, peer); state is **computed** (online = live connection ∧ fresh heartbeat); durable projection is the existing `endpoint_registry` row plus the ADR-025 last-declared cache columns (migration v4 — declarations, not liveness); duplicate dynamic id replaces (generation+1, audited); static-vs-dynamic id collision refused; stale ids never pruned (offline-known stays honest) | landed |
| Queue-first dispatch | `exec` enqueues durably by the bound worker id with no live-route requirement; rows park `pending` until the worker registers or the honest `queue_timeout` abandons them; no cross-worker dispatch, no failover; runtime routes attach/detach with membership | landed |
| Bounded queue re-drive | At-least-once re-drive terminates: `exec_queue.dispatch_attempts` (exec migration v6) counts dispatches, an `inflight` row re-drives only below `1 + workers.queue_max_redrives` (default 1, 0 = first dispatch only, ceiling 16), and a spent bound converges the row `inflight → abandoned` once no live lease is held — one audit line, no fabricated result, no new status/event/wire — [ADR-033](../design/decisions/ADR-033-bounded-queue-redrive.md) | landed |
| Router surface | `spawn_worker.exec_worker` is a plain non-empty string (no enum, no catalog block — supersedes ADR-020 D5–D7); one frozen prompt instruction line; new read-only `list_workers` tool (id, description?, state, bound session/thread; a registry-only id's description comes from the ADR-025 last-declared cache with `declaration_stale: true`); spawn validates against the runtime catalog — unknown refused naming known ids, offline-known allowed with a queue note, busy refused naming the owner; no membership byte ever enters a prompt or tool schema | landed |
| Operator verb | `kian workers [--json]`: read-only embedded view over the durable projection (id, state, last-seen, bound session/thread, and — since ADR-025 — the last-declared description/protocol version when declared; absent otherwise) | landed |
| Binding release + idle reclaim | A thread ending `interrupted`/`failed` holds its worker until released; `release_worker` / `kian workers release <id>` closes it with one atomic `drain/claimed` + `session/ended{cancelled}` batch (fresh claim epoch; additive `released` end payload), and a `kian serve` maintenance tick reclaims stale idle-ended threads after `workers.release_after_ms` (default 300 000, `0` disables). Gates: idle projection, non-terminal last end, no eligible inbox row, no live drain, no live `pending`/`inflight` queue row — [ADR-035](../design/decisions/ADR-035-worker-release-and-idle-reclaim.md) | landed |
| Security boundary | Private-bind-only listener (loopback/RFC1918/CGNAT/ULA; wildcard counts as public) with `allow_public` override; env-only tokens (shared or per-worker enrollment, ADR-031), constant-time compare, redacting type, bounded failed attempts; token never in config/argv/logs/events/metrics | landed |
| Single kian instance | A worker registers to exactly one `kian serve`; no federation, relay, cross-instance dispatch or multi-replica membership | landed (boundary) |
| Shared-token limitation | Narrowed by per-worker enrollment (ADR-031): a listed id accepts only its own token; in hybrid mode an unlisted id still shares the fallback. mTLS remains the planned fix | landed (narrowed; mTLS deferred) |
| mTLS / per-worker identity, token rotation without restart | Out of scope here — [WORKER-MODEL](../design/WORKER-MODEL.md) §9 | deferred |
| Live rebind / worker retirement / takeover | Unchanged (ADR-020 §Deferred, ADR-021 §Deferred) — the same id returning reattaches; a permanently lost worker ends in the honest timeout | deferred |
| Last-declared worker cache | `endpoint_registry` migration v4 adds nullable `description`/`protocol_version`; the registration upsert writes them, heartbeats never touch them, a description-less registration clears the text; `list_workers` serves the cache for registry-only ids with `declaration_stale: true`; live wins; no membership decision reads it — [ADR-025](../design/decisions/ADR-025-worker-last-declared-cache.md) | landed |
| Worker-declared capabilities | `register.capabilities` (v3, optional): the worker declares its own list, the server sanitizes (lowercase `[a-z0-9._-]`, ≤16, unique) with one audit line and still registers; migration v5 caches the accepted JSON list on `endpoint_registry`; `list_workers`/`kian workers` serve the live or cached list; static pins omit it — [ADR-032](../design/decisions/ADR-032-worker-declared-capabilities.md) | landed |

### Worker filesystem tools (KiandProto v2)

Scope: a bound worker session's `fs_list`/`fs_read`/`fs_write` execute on the **worker's** machine,
under its `--workspace-root`; the local built-ins stay for the Router/plain sessions. Wire:
[ADR-023](../design/decisions/ADR-023-worker-fs-tools.md), API-CONTRACT §8.3/§8.6; semantics:
[WORKER-MODEL](../design/WORKER-MODEL.md) §4/§6/§9. **No new event kind, no new HTTP route, no new
ops method; one wire bump (v1 → v2) and one additive queue migration (v3 `kind`).**

| Item | Contents | Status |
|------|----------|--------|
| Wire v2 fs pair | `fs_command`/`fs_result` frames; `PROTOCOL_VERSION = 2`; exec frames keep v1 bytes; dual goldens; upgrade order kian-first, no shim (a v1 worker reads offline with rows parked) | landed |
| Queue v3 | `exec_queue.kind TEXT NOT NULL DEFAULT 'exec'`; pre-v3 rows decode as exec and re-drive unchanged; fs rows share the queue, command-id dedupe and `queue_timeout`; no cross-worker dispatch | landed |
| Per-drive routing + schemas | A bound worker side mounts `RemoteFsTool` (filtered by the agent's `tools:`) and the refusing side mounts the same three names with an honest refusal; schemas/result shapes are byte-identical to the local built-ins (golden parity) | landed |
| Root + limits | `kiand --workspace-root` (default cwd, must exist); lexical `..` guard on both sides; 1 MiB read/write bounds; non-UTF-8 read is a typed `io` error surfaced as the local `{"error": …}` result; root is a guard, not a sandbox | landed |
| Live deployment | Owner deployment (Vultr + local worker + WeChat) redeployed at v2; the L3 fs round-trip through the WeChat Router path is an owner-run result | landed (owner L3) |
| Binary/streaming/fs_edit/glob, symlink-proof sandbox | Out of scope (WORKER-MODEL §9) | deferred |

### Thinking-mode reasoning replay (ADR-022)

Providers that require the assistant's reasoning to be replayed (DeepSeek V4 thinking mode) get
`reasoning_content` persisted in the assistant event and re-sent verbatim; a legacy assistant
message without the key produces byte-identical request bytes (the frozen chat-body hash is
unchanged). No new event kind, no wire change. L1/L2 tests; the live empty-batch Router turn no
longer returns 400. **Landing note: the v1 → v2 worker protocol bump is independent of this row.**
| Item | Contents | Status |
|------|----------|--------|
| Capture + replay | ReAct stream assembler concatenates `reasoning_content`; `assistant/message` content carries it; the message → chat conversion re-emits the top-level field | landed |
| Cost | Replayed reasoning is input tokens on thinking sessions only; observable through usage events; no trimming/synthesis | recorded |

### Resident iteration budget — per episode (ADR-024)

Scope: a resident loop's iteration budget is **per episode**, not per session lifetime. This closes
DESIGN §4.6's tracked follow-up and the owner incident where the Router froze at its 32nd turn and
later channel inputs were admitted but never consumed.

| Item | Contents | Status |
|------|----------|--------|
| Budget scope | `ReActConfig.budget_scope: Lifetime | PerEpisode`; only the super-agent assembly's Router and watchdog sides select `PerEpisode`; worker/plain loops keep the legacy lifetime scope (byte- and behavior-identical) | landed |
| Park, not freeze | On exhaustion the loop emits `EPISODE_BUDGET_STOP_REASON_PREFIX`; the `RouterDriver` parks (the idle-final-answer path) instead of propagating `Stopped`, so no `session/ended` is written and the next input starts a fresh episode. `turn/start` keeps the lifetime iteration number for audit/replay | landed |
| Observability | One warn audit line per park plus the additive `superagent.router_batching.episode_budget_parks` metric; no new event kind, wire or table | landed |
| Empty episode | A resident iteration with no eligible input and a context ending on an assistant message settles without a provider call (ADR-027); tool-result continuations and newly taken inputs proceed; non-resident loops unchanged. This removes the empty-batch `reasoning_content` 400 that could follow a no-reasoning assistant turn | landed |
| Explicit terminal ends | cancel/fatal still end terminally; the boot-time re-arm remains the recovery. Runtime re-arm for explicit ends is a recorded deferred follow-up (needs a runtime-rebindable Router binding across subscriptions, driver, tools and worker reports) | landed (boundary) |

### Durable confirmation plane (phase 1 landed)

Scope: a tool call the agent may *request* but a person must decide (`permissions.<tool>: ask`).
The wait is a resumable end, not a held lease; the request is the event stream, not an outbox row;
the answer is an admitted control row, never a direct event write. This closes the gap ADR-015 §7
recorded ("a durable pending-confirmation state, a human/control-plane response path into the
inbox, and hold semantics") and the superagent `awaiting-answer` stage's read-side-only deviation.

The normative contract is [design/CONFIRMATION-MODEL.md](../design/CONFIRMATION-MODEL.md); the
decision record is [ADR-036](../design/decisions/ADR-036-durable-confirmation-plane.md). The catalog
deltas (the 23rd kind `control/confirm`; the widened `tool/call.permission.evaluated` and
`session/ended.reason`) landed with the code in EVENT-MODEL and API-CONTRACT, and the drift tests
pin them together.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P1 | kian-core, kian-loop, kian-orchestrator, kian-cli, kian-sdk | `ask` permission arm (a widened closed value, never invokes); `pending_calls` projection in `kian-core::confirmation`; the fifth end reason `awaiting_input` (`running → idle`); the `control/confirm` kind + reserved confirm row envelope; `confirm`/`pending` control-plane methods + SDK + CLI pair; lazy TTL expiry; the no-provider-request-while-pending resume rule; the **promotion gate** (no message row is promoted inside an open call, with the wake consequence) and the **open-call crash repair** (`kian-core::repair`, additive `recovered` marker, no fabricated permission); fail-closed config (unknown values, and `ask` refused on worker/resident agents) | landed |
| P2 | kian-superagent | Worker integration: a deferred turn ends `awaiting_input` without `thread/completed` or the parent report; the worker stays live (follow-ups keep routing to the same session); the ticket stage gains `awaiting-confirmation`; `ask` is accepted on worker agents (resident agents stay refused) | landed |
| P3a | kian-superagent, kian-cli | Channel-side answers: a closed-vocabulary IM reply (`approve <code>`) resolved by the adapter against the pending set and written through the existing `confirm` seam — never model-interpreted, never a free-text decision ([CONFIRMATION-MODEL §14d](../design/CONFIRMATION-MODEL.md#14d-phase-3a--channel-answers-closed-vocabulary-landed)) | landed (L3 live-phone check manual) |
| P3b | kian-plugin, bundle | Hook `ask` verdict (`tool/pre` defers) and its ordering with the declarative gate (a PLUGIN-SPEC widening) | not started |
| P4 | orchestrator, read model | Automatic expiry without a control-plane touch; a read-model snapshot carrying the pending set | not started |

### Session token budget (ADR-055, P1b landed)

Scope: a **session-lifetime token ceiling** an operator can declare per agent and raise while a
session is parked. It closes the gap the original `Budgets` type recorded but never enforced (A5:
"budget consumption" was observable, not binding) and makes cost a pre-send decision rather than a
post-hoc report.

Semantics: `agents.<name>.budgets.tokens` is the base ceiling; a configured budget is pinned on
`session/created.budgets` (a BEGIN-only additive key) so replay never depends on today's config, and
an unconfigured one leaves the event byte-identical to legacy. The gate runs inside the loop at the
provider-turn safe boundary, **after** the request plan is built and **before** `step/request` is
appended: the projection `base + Σbudget/raised − Σ turn/end.usage` must admit
`estimate.high + max_output`, or the turn is refused with no bytes persisted and no request sent,
and the drain ends `awaiting_budget` (`running → idle`, resumable, honest). A raise is a durable
reserved control row (`kian raise-budget` / `raise_budget` RPC, idempotent) that the owning drain
consumes into `budget/raised` and wakes; the control plane never touches execution. Wall-clock
raises are projected but not enforced while a drain is idle; a usage-less turn spends nothing
(unknown, never assumed).

The normative contract is [design/EVENT-MODEL.md](../design/EVENT-MODEL.md) §2.3/§3/§4.4; the
wire is [design/API-CONTRACT.md](../design/API-CONTRACT.md) §3; the decision record is
[ADR-055](../design/decisions/ADR-055-session-budget-gate.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P1b | kian-core, kian-loop, kian-orchestrator | `kian-core::budget` projection (`base + Σraised − Σspent`, DeepSeek/OpenAI usage spellings); the seventh end reason `awaiting_budget` (`running → idle`); the `budget/raised` kind + reserved budget row envelope; the pre-send gate (`estimate.high + max_output`, no `step/request` on refusal); `session/created.budgets` pin | landed |
| P1b-wire | kian-cli, kian-sdk | `raise_budget` RPC + `ControlPlane` method + CLI (`kian raise-budget`); no new error code (reuses `-32602`/`-32001`/`-32007`; outcomes `accepted`/`deduplicated`/`terminal`) | landed |

Non-goals recorded with the phase: no per-step monetary gate (pricing stays a projection, ADR-053),
no enforcement of wall-clock budgets while idle, no budget on harness-owned (projection-only) loops,
no automatic compaction or model downgrade as a budget response.

### Tool result artifacts (ADR-056, P2 landed)

Scope: keep a large tool result **out of the log and out of the model's context** while keeping both
replayable and auditable. A result is the payload class that otherwise grows the event stream, the
export, the read-model page and every later request body at once.

Semantics: with an `artifacts:` block configured, the engine's single append path rewrites a
`tool/result` whose successful `content.output` exceeds `inline_max_bytes` (default 16 KiB) into
`{"artifact":{"id","size","media_type"},"preview":<str>,"truncated":true}` before the event is
written. The persisted bytes are therefore exactly what the model sees next turn (constraint 2),
and replay — which derives context from the stream — yields the same preview with or without the
blob: the content-addressed blob store (`sha256` ids, atomic temp+rename, retention GC) is an
operator cache, never a second truth. `artifact_read` (a bundle-edge tool mounted beside the local
tools) serves the full payload in bounded windows and reports an aged-out blob as an honest error.
Error results never spill; an absent block keeps every legacy payload byte-identical.

The normative event contract is [design/EVENT-MODEL.md](../design/EVENT-MODEL.md) §3/§4.1; the
placement is [design/DESIGN.md](../design/DESIGN.md) §2.2; the decision record is
[ADR-056](../design/decisions/ADR-056-artifacts-and-tool-result-spill.md).

P3b ([ADR-059](../design/decisions/ADR-059-maintenance-artifact-gc-and-export-pinning.md)) adds two
operational surfaces without changing this contract: `serve` runs a retention GC ticker when the
`artifacts:` block exists (interval from `artifacts.gc.interval_ms`, default one hour, outside
drains, non-fatal), and `kian session export --with-artifacts` copies referenced blobs beside the
output file on request. The stream still carries only references and previews, and a blob that was
already reclaimed is an honest, documented outcome.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P2 | kian-store, kian-orchestrator, kian-cli | `kian-store::ArtifactStore` (content-addressed put/get, atomic write, id validation, retention+cap GC); append-path spill of successful oversized `tool/result` payloads (preview + ref, `is_error` never spills); `artifact_read` tool + mounted-list wiring; `artifacts:` config (dir, `inline_max_bytes`, GC bounds) and `kian artifacts gc` | landed |
| P3b | kian-cli | serve-side retention GC ticker armed from `artifacts.gc.interval_ms` (default one hour, outside drains, non-fatal); `kian session export --with-artifacts` pins referenced blobs to `<out>.artifacts/<id>` and reports reclaimed blobs as missing without failing the export | landed |

Non-goals recorded with the phase: no per-session quota, no reference counting (kian has no
session enumeration API and compaction rewrites ranges), no server-side fetch in core. Export blob
pinning and the serve GC ticker were deferred here and landed in P3b under ADR-059.

### Client-side web tools (ADR-057, P2b landed)

Scope: give the loop web search and URL reading as ordinary audited tool calls on the operator's own
provider key — without adding a provider capability to core and without a second truth for page
content.

Semantics: a `web:` block (protocol-only: `base_url`, `api_key_env`, `timeout_ms`,
`search.max_results`, `search.need_content`) mounts two bundle tools, `web_search` and
`web_fetch`, assembled from the `kian-web` crate. Every call is a normal `tool/call` +
`tool/result`: the documented Search/Fetch request JSON is persisted before the request flies, the
provider's response object is the result content, and a large page spills by reference through the
ADR-056 path instead of being truncated. Failures are honest `is_error` results carrying the HTTP
status and a bounded body (408/429/5xx marked retryable, nothing retried automatically);
out-of-contract arguments are refused without a request. An absent `web:` block leaves the tool set
and every request byte-identical; present, the key is read once at serve assembly and a missing
value refuses boot. Returned pages are untrusted data and no provider-encrypted signature is
retained.

The normative decision record is [ADR-057](../design/decisions/ADR-057-client-side-web-tools.md);
placement is [design/DESIGN.md](../design/DESIGN.md) §2.2/§7. Search/Fetch bill per successful
request; the phase-0 cost projection prices model tokens only (request-count pricing is a later
slice).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P2b | kian-web, kian-cli | `kian-web` bundle `ToolProvider` (`web_search`/`web_fetch` over the documented Search/Fetch HTTP APIs; honest classified failures; out-of-contract arguments refused without a request); `web:` config with boot validation and fail-closed key read; presence-locked mounting beside the local tools | landed |

Non-goals recorded with the phase: no server-side web tools in core, no citation/signature
retention, no automatic retry loop, no request-count cost in the phase-0 projection.

### Context compaction (ADR-058, P3 landed)

Scope: keep a long session inside its declared context window without rewriting history, and keep
compaction economically honest and observable. Folding is the only deliberate prefix-invalidation
point (constraint 3), so it is a bounded, evidence-driven decision - never a background chore and
never a cost optimization.

Semantics: `kian-orchestrator::compaction` exposes a pure planner
(`plan_compaction(events, settings, trigger)`) that keeps the most recent turns verbatim, starts
after the previous compact, requires a minimum reclaim and returns a strictly-past disjoint range
with an explicit reason. The loop folds under four triggers: a declared
`profile.context_window` whose worst-case next request reaches `trigger_percent` (default 90), the
provider's authoritative `ContextOverflow` answer, a declared request-byte bound that the rendered
request itself would exceed (ADR-072), or the provider's `request_too_large` refusal against a
bound the gate did not compare (ADR-068, ADR-074). The preventive byte trigger is measured on the
exact body the send would use - artifact bytes inlined, tool schemas sorted, plan body serialized -
and it is checked before `turn/start`: an oversized body is never persisted and never sent. It
skips the minimum-reclaim rule, which counts projected text and cannot see the pixels the fold
removes, and records both byte readings in the fold's reason. The two reactive triggers fold only
when the deployment's recovery policy table resolves the class to `compress_then_retry` (ADR-074),
so the default table - which maps `request_too_large` to `fail` - keeps a byte refusal terminal,
and the class selects the trigger whose name the `compact` reason reports; an `Oversized` fold
skips the same minimum-reclaim rule, and its refused request stays in the stream as the record of
what was not sent. The summary is produced by an ordinary persisted maintenance turn -
`turn/start`, `step/request` with `purpose: "compact"`, the summary, then `compact` - using the
session's bound slot/provider with one request and no fallback; the summarizer is not routed through
`turn/request` / `turn/response` hook ports. The `compact` event is appended only after the
summary exists; a failed summarizer leaves the projection untouched and its request is still
persisted and priced. Ordinary turns reserve ten percent of the budget allowance for maintenance
requests, so the session can always pay for the compaction that prevents the overflow deadlock. At
most two folds run per drain episode.
Observability follows the purpose: cache metrics pair only ordinary requests, and `kian cost`
reports maintenance requests apart from turns.

The normative event contract is [design/EVENT-MODEL.md](../design/EVENT-MODEL.md)
§3/§4.2/§4.3; the placement is [design/DESIGN.md](../design/DESIGN.md) §3.2; the decision record is
[ADR-058](../design/decisions/ADR-058-production-compaction-and-maintenance-reserve.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P3 | kian-orchestrator, kian-loop, kian-core, kian-cli | the pure compaction planner (settings/pressure/trigger/plan, keep-recent-turns fold, minimum reclaim, strictly-past disjoint ranges); preventive and reactive folds in the ReAct loop as persisted maintenance turns (`purpose: "compact"`, summary request, `compact` on success only); per-episode fold bound; the maintenance budget reserve; purpose-aware cache metrics and cost reporting | landed |
| P3b | kian-orchestrator, kian-loop, kian-cli | byte pressure as a fold trigger: `CompactionTrigger::Bytes` measured on the rendered request against `providers.<name>.max_request_bytes`, the pre-turn gate that folds instead of persisting an oversized body, the per-slot bound delivered into the loop ([ADR-072](../design/decisions/ADR-072-request-byte-pressure-folds-history.md)) | landed |
| P3c | kian-orchestrator, kian-loop, kian-cli | the refusal-side fold: `CompactionTrigger::Oversized` for a `request_too_large` the byte gate did not predict, taken only when the recovery table resolves the class to `compress_then_retry`, skipping the text-based minimum reclaim and naming the refusal in the `compact` reason ([ADR-074](../design/decisions/ADR-074-oversized-request-folds-under-policy.md)) | landed |

Non-goals recorded with the phase: no price-aware break-even planner, no separate compaction
scheduler, no `compact_slot` override, no fallback traversal for the summarizer, no wall-clock
enforcement of the reserve. Byte pressure adds no downscale, no per-request image cap, no
retroactive eviction outside a fold, and no automatic raising of the provider bound.

### Off-peak deferral (ADR-060, P4 landed)

Scope: let a deliberately submitted batch admission or a scheduled firing wait for an off-peak
pricing window, without introducing a second scheduler and without ever delaying interactive work.

Semantics: the planner is pure and lives in `kian-cli` beside the price book. Given the current
instant and the configured peak/holiday calendar it returns the next off-peak instant, bounded by
`defer.max_wait_ms`; when the bound is exceeded, or the admit instant is already off peak, the
admission runs immediately and the recorded reason says why it did not wait (a `deferred: false`
decision is an honest record, never a silent skip). Deferral is opt-in only: the JSON-RPC
`submit`/`queue` methods accept `defer: "off_peak"`, and a cron resident schedule declares
`defer: off_peak` (absent keeps today's immediate bytes). The admission path persists the ordinary
durable inbox row with the planned `eligible_at`; the store's existing eligibility predicate is
the hard gate, so no wake can consume the row early and no second timer system exists. A serve-side
due-wake sweep, armed only by a `defer:` section, re-runs the startup eligibility scan to wake
sessions whose rows became due; a late sweep adds latency only. Interactive inputs — CLI runs,
chat, channel messages, steering — never defer, and a cron worktree firing cannot defer (an
isolated external job has no inbox row). The cron decision rides the admitted payload as a
non-context additive `defer` key; no new event kind is involved.

The normative wire is [design/API-CONTRACT.md](../design/API-CONTRACT.md) §3; the scheduling
semantics are [design/CRON-MODEL.md](../design/CRON-MODEL.md) §3.1/§3.3/§6.1; the event contract is
[design/EVENT-MODEL.md](../design/EVENT-MODEL.md) §4.2; the placement is
[design/DESIGN.md](../design/DESIGN.md) §2.3; the decision record is
[ADR-060](../design/decisions/ADR-060-off-peak-deferral.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P4 | kian-cli, kian-orchestrator | the pure off-peak planner over `pricing.peak_windows`/holidays (bounded by `defer.max_wait_ms`, immediate when already off peak or the bound is exceeded); `Engine::admit_at` over the existing `eligible_at` hard gate; the serve due-wake sweep; the optional `defer:` config section with boot validation; `defer: off_peak` on cron resident schedules; additive `defer` on `submit`/`queue` and the non-context payload decision | landed |

Non-goals recorded with the phase: no second scheduler or per-row timer, no deferral of
interactive inputs or steering, no job deferral (the job runtime is not in this tree), no
`overlap`/queue policy change, no cost guarantee from deferring.

### Client-side media tools (ADR-061, P5a landed)

Scope: OCR, speech-to-text and text-to-speech as client-side bundle tools over documented HTTP
shapes, with every input and output byte flowing through the content-addressed artifact store. This
is the ingest half of the media capability: an image or a recording becomes model-addressable
without bytes ever entering the prompt, and an operator can turn a local file into an artifact —
before a channel adapter that can deliver media exists (that is P5b).

Semantics: `media:` mounts `image_ocr`, `audio_transcribe` and `speech_synthesize` beside the local tools
and requires `artifacts:` (boot refuses without it); an absent section keeps the mounted tool set
byte-identical. `image_ocr` takes `{"artifact","media_type"?,"prompt"?}` and returns `{"text"}`;
`audio_transcribe` takes `{"artifact","media_type"?,"language"?}` and returns `{"text"}`;
`speech_synthesize` takes `{"text","format"?,"voice"?}` and writes the audio blob into the artifact
store, returning `{"artifact":{"id","size","media_type"}}`. A missing artifact (aged out) or an input
above `media.max_input_bytes` is refused before any request. Provider failures are honest error results
with a bounded body; 408/429/5xx are retryable, a 4xx is not. `kian artifacts put <path>` writes a local
file into the same store and prints the reference. Tool descriptions and result text mark OCR/ASR
output as untrusted data, never instructions.

The provider protocol is the client-side HTTP adapter described in
[ADR-061](../design/decisions/ADR-061-client-side-media-tools.md); the blob contract is ADR-056; the
placement is [design/DESIGN.md](../design/DESIGN.md) §2.2.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P5a | kian-media, kian-cli | the `kian-media` bundle tools (OCR, ASR, TTS) over the artifact store, the `media:` config section with boot validation, and `kian artifacts put` ingest | landed |

Non-goals recorded with the phase: no channel media delivery (P5b), no vision content parts
(P5c), no media pricing in the cost projection, no media persisted as an event, no server-side
media tool in the core.

### Channel-edge media (ADR-062, P5b landed)

Scope: inbound images and voice messages become durable text context at the channel edge, before the
turn exists, while their bytes live in the artifact store. This is the second half of the P5 media
capability: P5a made media addressable; P5b makes what a person *sent* readable by the model.

Semantics: the assembly's channel pump (`kian-cli`) runs one optional edge before
`SubscriptionService::handle_event`. An adapter event whose payload carries the additive `media`
array — items `{"kind":"image"|"audio","media_type","base64"[,"language"]}`, absent from every
text event so existing payload bytes are unchanged — is processed item by item: the bytes are written
to the artifact store (ADR-056), `image_ocr` / `audio_transcribe` (ADR-061) produce the transcript, and
the item is rewritten to `{"kind","media_type","artifact","size","transcript"}` or
`{"kind","media_type","error"}`. Transcripts are appended to the payload's `text` under
`[image transcript]` / `[voice message transcript]` markers, in item order. Base64 never re-enters the
payload, no message is dropped, and every failure (decode, provider, oversize, missing blob) is an
honest per-item `error` that does not block admission. A payload without `media` is passed through
byte-identically; an unconfigured `media:` section mounts no edge. Outbound audio is the iMessage
channel's voice path ([ADR-067](../design/decisions/ADR-067-imessage-voice-io.md)): `imessage.voice: true`
mounts a `reply_voice` tool, the voice decorator on the channel edge synthesizes the utterance and
rewrites the delivery into bytes, and every other channel still answers in text. An enabled voice
path does not change the default: the tool is used only when the user asks for a spoken reply, and
every other turn is text.

Admission is per row, not per attachment: on the iMessage channel a page that renders more attachment
placeholders than the attachments it delivered is a row mid-commit, and the adapter parks its
in-process read position immediately before that row (`rowid - 1`) instead of admitting a prefix, so
the input it eventually admits carries every attachment `chat.db` committed meanwhile. The hold is
bounded by `ROW_SETTLE_WINDOW` (five seconds); a row that never settles is admitted with what arrived
plus one audit line naming the placeholder and attachment counts, the per-row media cap is eight, and
a row over the cap loses the excess items with an audit line ([ADR-070](../design/decisions/ADR-070-imessage-multi-attachment-settling.md)).
Nothing durable is added: the cursor is process state and a restart re-reads through the bridge's
bounded replay.

The edge contract is [ADR-062](../design/decisions/ADR-062-channel-edge-media.md); the tool protocol is
ADR-061; the blobs are ADR-056; the placement is [design/DESIGN.md](../design/DESIGN.md) §4.2.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P5b | kian-cli | the channel-pump media edge: the typed inbound media envelope, artifact-backed OCR/ASR transcripts appended to the admitted text, honest per-item failures | landed |
| P5b | kian-superagent | whole-row attachment settling on the iMessage adapter: a mid-commit row parks the read position for a bounded window instead of admitting an attachment prefix, the per-row media cap is eight, and both the expired wait and the truncation are audited ([ADR-070](../design/decisions/ADR-070-imessage-multi-attachment-settling.md)) | landed (L1/L2) |
| P5b | kian-cli, kian-imessage-bridge | the outbound voice path: `imessage.voice` mounts `reply_voice`, the channel-edge voice decorator synthesizes and rewrites the delivery, and the bridge sends it as a native voice message or an audio attachment ([ADR-067](../design/decisions/ADR-067-imessage-voice-io.md)) | landed (L1/L2) |

Non-goals recorded with the phase: no real-adapter media ingestion in phase 5b itself (WeChat CDN
download remains that channel's phase 2; the iMessage attachment-bytes path landed separately as
[ADR-064](../design/decisions/ADR-064-imessage-attachment-bytes.md)), no outbound media other than the
iMessage voice path, no voice answer the model did not ask for (a reply is spoken only when it calls
`reply_voice`), no media pricing, no new event kind and no queue or batching at the edge.

### Vision inputs (ADR-063, P5c landed)

Scope: let a vision-capable model see an image without leaving the event log. A user input payload
may carry its content as `{"text": <string>, "images": [{"artifact": <content id>,
"media_type": <IANA type>}]}`; downstream this becomes a dialect-level content-parts message.

Semantics: the loop resolves each referenced blob through the artifact store
(`DrainCtx::artifact_bytes`) and attaches the bytes to the chat message it derives. Only a target
whose `model_profiles:` entry declares `vision: true` takes this path: the V2 request plan rewrites
exactly those messages to OpenAI content parts (`[{"type":"text","text":...},{"type":"image_url",
"image_url":{"url":"data:<media_type>;base64,<bytes>"}}]`); the frozen V1 body (no profile) and
every image-free V2 body keep their exact bytes, because the image field is `#[serde(skip)]`. At
most 4 images per message and 8 MiB per image; a missing, oversized or malformed reference drops
that image with a warning and the text envelope still reaches the model (a reclaimed blob degrades
the answer, never fails the turn). The persisted `step/request` proves the exact bytes without
copying the pixels again: `body_sha256` covers the wire body and each image part is recorded as a
content-addressed reference (ADR-073). An offline replay still cannot re-derive the base64 from the
stream alone, so `kian session replay` counts such requests as vision requests instead of
byte-identity mismatches.

The contract is [ADR-063](../design/decisions/ADR-063-vision-content-parts.md); the artifacts are
ADR-056; the profile knob is ADR-054; the placement is [design/DESIGN.md](../design/DESIGN.md) §3.2.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P5c | kian-llm, kian-loop, kian-orchestrator | dialect-level vision: artifact-backed image bytes on vision-profile messages as OpenAI content parts, bounded and honestly degradable | landed |

Non-goals recorded with the phase: no real vision provider wired in the tree (scripted-provider L2
evidence), no image pricing, no images outside `role: user`, no replay re-derivation from the
artifact store. Channel media reaches the model as pixels only through the explicit opt-in below;
without it the edge still transcribes every item and no channel attaches an image.

### Channel images on a vision target (ADR-065, opt-in)

Scope: let an image that arrived over a channel reach a vision-capable model as pixels, without
changing what every other reader of the admitted content sees.

Semantics: with `superagent.shaping.vision: true` (default `false`), the admission shaper keeps the
`{"kian": …}` channel wrapper byte-identical and adds a sibling `"images": [{"artifact": <content
id>, "media_type": <IANA type>}]` built from the items the media edge already stored (`kind:
"image"` with a non-empty artifact and media type). Inputs above `inline_cap_bytes` keep the Inline
view and carry no images. The loop recognizes the sibling and derives the same model-visible text as
before; the ADR-063 capability gate is unchanged, so only a `vision: true` target attaches bytes and
a blind target drops the references with one warning and sends the text. Config refuses
`shaping.vision: true` without a `media:` section or without a Router slot that resolves to a vision
profile, and `kian serve` logs one warning when the resident session's projected binding predates
the change (slot bindings are session-immutable; `superagent.router.rotate_after_events` retires the
session, ADR-040). A media item whose transcription failed keeps its artifact reference, so a picture
still reaches a vision target when the text tool could not describe it.

The contract is [ADR-065](../design/decisions/ADR-065-channel-images-as-vision-references.md); the
transcript path is [ADR-062](../design/decisions/ADR-062-channel-edge-media.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| Channel | kian-superagent, kian-loop, kian-cli | opt-in sibling image references on channel admission, capability-gated bytes, boot validation and the stale-binding diagnostic | landed (default off) |

Non-goals recorded with this slice: no vision on the spill path, no outbound media, no per-image
pricing or dedupe, no automatic slot migration, no automatic session rotation.

### Per-turn image budget (ADR-066)

Scope: bound what a burst may cost in one request without rewriting a turn that already flew.

Semantics: the loop budgets only the unsent tail of the model context — the messages after the last
assistant message. References are admitted newest-first and the admitted set is a contiguous prefix
of that order: `superagent.shaping.max_images_per_turn` (default `4`, range 1–32) counts references
and `superagent.shaping.max_image_bytes_per_turn` (default `4194304`, range 64 KiB–64 MiB) sums the
artifact bytes before base64. A reference that does not fit is not rendered — its wrapper and
transcript stay byte-identical — and the turn logs one warning naming both limits. Both keys refuse
boot unless `shaping.vision: true`. Once a request flies its images sit at or before the last
assistant message and are never re-budgeted, so a retry sends identical bytes and a later turn keeps
the pixels it already sent; the persisted `step/request` remains the exact bytes sent. The
per-message ceiling (4 images, 8 MiB each, ADR-063) still applies inside a message.

The contract is
[ADR-066](../design/decisions/ADR-066-per-turn-image-budget.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| Channel | kian-loop, kian-cli | unsent-tail image budget, per-message ceiling, two config keys with boot refusal | landed |

Non-goals recorded with this slice: no retroactive eviction of history pixels, no downscale, no
dedupe, no per-image cache. Growth across turns is stopped by the provider's `max_request_bytes`
(ADR-068), the byte-pressure fold it authorizes (ADR-072) and the refusal-side fold (ADR-074), not
here.

### Request audit bodies by reference (ADR-073)

Scope: bound what the audit record of a request costs while keeping the exact bytes identifiable
and verifiable.

Semantics: on a vision target the persisted `step/request` body — and the `slot/fallback` attempt
copy — renders each image part as `{"type":"image_url","image_url":{"url":"artifact:<sha256-hex>",
"media_type":<IANA type>,"size":<u64>}}` and carries `"body_form":"artifact_refs"` plus
`"body_bytes":<u64>`, the length of the wire body. The wire body is unchanged: the provider still
receives `data:` URL parts and `body_sha256` stays the digest of those exact bytes; a body with no
image carries no `body_form` and keeps its bytes. The reference is the artifact store's own address
(the lowercase sha256 hex of the image bytes), derived from bytes the loop already holds, so
rendering it reads no store and stays deterministic across replay. `kian session request <uuid>
--seq <n> [--expand]` prints the stored payload; `--expand` re-reads each blob, requires
`sha256(blob) == id`, rebuilds the exact `data:` URL and verifies the result against
`body_sha256`, failing by id when a blob is gone. The live blob store stays disposable (ADR-056):
a durable copy is what `kian session export --with-artifacts` produces, pinning every blob a request
body references beside the JSONL. A reclaimed blob leaves the identity, media type, size and digest
in place, never fabricated bytes. Replay keeps counting content-part requests as vision requests; no
consumer re-derives from the store on that path.

The contract is
[ADR-073](../design/decisions/ADR-073-request-audit-bodies-by-reference.md); the rendering is
ADR-063, the store ADR-056.

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| Audit | kian-llm, kian-loop, kian-cli | reference-rendered request bodies (`body_form`, `body_bytes`) for both append sites and the fallback copy, the verified `session request --expand` reader, export pinning of request references | landed (L1/L2) |

Non-goals recorded with this slice: no rewrite of existing inline events (append-only), no per-image
dedupe beyond content addressing, no reference-aware GC (retention stays policy), no re-derivation in
`kian session replay`, no change to the wire bytes or to any gate's reading.

### Resident rotation (ADR-040)

Scope: bound the resident Router's context and refresh its memory index (MEMORY-MODEL I2 freezes it
at creation) without a runtime rebind of every surface. `superagent.router.rotate_after_events: <n>`
(absent = off; an explicit `0` refuses boot) makes `kian serve` assembly retire a live Router whose
stream holds at least `n` events: the predecessor ends `session/ended{rotated}` (terminal, one atomic
`drain/claimed` + end batch, the payload naming the successor), the deterministic uuid5 successor is
bound everywhere the pinned id was, and the successor's context order is the ADR-038 memory index, the
predecessor's pending *message* rows, a bounded synthetic handoff, then the judge prompt. Only an
`idle` live session rotates (a crashed in-flight turn is a takeover, never a rotation), and a live
lease wins. The normative contract is [design/DESIGN.md](../design/DESIGN.md) §2.2/§4.6; the decision
record is [ADR-040](../design/decisions/ADR-040-runtime-resident-rotation.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P0 | kian-cli | The sixth end reason `rotated` + the terminal `rotated` state; `superagent.router.rotate_after_events`; the boot rotation (atomic close, chain re-resolution, pending-message migration, bounded handoff, successor bootstrap ordering) | landed (multi-replica: ADR-048 derive/repair/migrate) |
| P1 | kian-superagent, kian-cli | The shared `ResidentBinding` read at use time by driver routing, the mounted residency inbox, the spawner's thread parent, the worker completion-report target, the subscription `router` route, the Router tools (mandate/spawn identity/task ids), the recall reader, the runtime metrics and the child-timeout root — behavior-identical, the prerequisite for rotation between restarts | landed |
| P2 | kian-orchestrator, kian-superagent, kian-cli | Runtime rotation between restarts: the hold owner (Router driver) ends its own parked session `rotated` at a park boundary (threshold + no outstanding thread), the serve ticker derives the successor and bootstraps it (memory index → migrated rows → handoff → judge prompt), the engine's atomic `rotate_idle` covers a genuinely idle window, and the shared-binding swap (phase 1) re-points every consumer without a restart | landed |
| P3 | kian-cli | Multi-replica coordination: each replica's tick derives the live session from the durable chain when its binding turns terminal (catch-up + re-point), any replica completes an interrupted successor bootstrap (idempotent admit keys), and the catching-up replica migrates the rows it wrote to the frozen predecessor | landed |

Acceptance criteria are named in CONFIRMATION-MODEL §13 (L1 contract tests; L2 on a real server
covering the ask→idle→answer→resume round trip, duplicate answers, TTL expiry and a terminal end
while waiting). No L3 claim in phase 1 (no UI card; the `pending` read is its seam).

Non-goals recorded with phase 1: no hook `ask`, no channel answering, no automatic expiry, no
`ask` on worker/resident agents, no new session state, no `LoopDriver`/`LoopOutcome` change, no
outbox row for the request.
### Oversized requests (ADR-068)

Scope: make a request the upstream cannot accept a classified, non-retryable failure instead of a
transport error that reads as an outage.

Semantics: `providers.<name>.max_request_bytes` (optional, absent = no pre-flight check, floor
1024) is compared against the serialized body before the POST; over the bound the call fails
`request_too_large` naming the body size and the bound and stating that no request byte left the
process. An HTTP 413 answer is classified `request_too_large` as well, carrying the status, the
size that was sent and a bounded snippet. `request_too_large` is a closed policy-table class: the
default table maps it to `fail` because re-sending identical bytes cannot succeed, and a deployment
that wants compaction to shrink the history opts into `compress_then_retry` explicitly — the loop
then folds the old turns under the byte trigger and retries the smaller request (ADR-074), bounded
by the per-episode fold cap; when no fold can reclaim anything the turn settles failed as before.
Without a declared bound a mid-upload connection close stays `provider_down` — no transport
heuristics. The persisted `step/request` remains the audit record of the bytes that were not sent.

The contract is
[ADR-068](../design/decisions/ADR-068-oversized-request-classification.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| Provider | kian-llm, kian-orchestrator, kian-cli | the `request_too_large` class, 413 classification, the pre-flight `max_request_bytes` bound, default `fail` | landed |

Non-goals recorded with this slice: no automatic history shrinking, no body splitting, no discovery
of the carrier's own inbound bound.

### Scheduled turns (cron, P0–P1 landed)

Scope: wall-clock scheduled work executes as ordinary durable turns — no schedule-execution
engine, no new event kinds. The normative contract is
[design/CRON-MODEL.md](../design/CRON-MODEL.md); the decision record is
[ADR-034](../design/decisions/ADR-034-cron-scheduler.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P0 | kian-cron, kian-store, kian-cli | Five-field/`@`-alias grammar with IANA zones and DST gap/overlap rules; durable `cron_schedules`/`cron_firings` (sqlite + postgres, migration v7); claim-before-dispatch cursor; `kian cron list\|run-now\|logs\|rm`; config-only declarations, fail-closed at load | landed |
| P1 | kian-cli | Worktree-backed firings: detached per-firing git tree, harness invocation, diff/stderr capture, `keep_on_failure`, free-space floor and total-cap GC; catch-up coalescing; one JSON tick report per decision; per-schedule isolation repos; **firing settlement** (the worktree arm's known outcome, the resident arm's recorded-session sweep) so `cron list`/`cron logs` answer "did it run"; **frozen-target firing-session succession** (a recurring resident schedule keeps running after its plain target ends, one deterministic session per slot, recorded on the firing row); the resident arm's `skip` overlap judgement (`Engine::has_open_work`); **non-blocking worktree dispatch** (a spawned turn, a global ceiling `superagent.cron.max_concurrent` default 4, outcomes drained on a later tick) | landed |
| P2 | kian-cli, kian-store | Config-writing `kian cron add`, store-scoped schedules, per-firing `sandbox.*` knobs, `queue`/`kill-previous` overlap enforcement (refused at load in v1), durable worktree detail (diff/stdout beyond `state`/`path`/`error`) | designed |

No new event kinds: a **resident** firing is an ordinary admitted turn. The admitted input carries
the additive, non-context `cron` key plus a `cron:<firing_id>` idempotency key, so the model
request bytes are the prompt alone. A **worktree** firing is an isolated child run by design (no
session, no events): its record is the firing row, so it is not replayable and carries no
usage/recall (CRON-MODEL §6.1). The tick loop and the durable adapter are exercised in-process
(L1: real store + the real decision table); the grammar, DST, misfire, store, settlement and
worktree paths carry unit coverage, and the confirmation plane's crash repair is exercised by the
`kill -9` chaos suite (L2).

### Declarative jobs (P0–P1 landed)

Scope: a unit of work that is a *plan* of steps — fetch on one machine, summarise, then deliver —
carried as ordinary durable machinery, with honest per-step evidence. No workflow engine, no job
table, no second scheduler, no new event kinds. The normative contract is
[design/JOB-MODEL.md](../design/JOB-MODEL.md); the decision record is
[ADR-052](../design/decisions/ADR-052-job-model.md).

| Phase | Module | Contents | Status |
|-------|--------|----------|--------|
| P0 | kian-superagent, kian-cli | The four axes (trigger / plan / placement / evidence); a run is an ordinary session whose stateless conductor parks (so a run may wait hours without ending); the plan frozen into the admitted row with its digest; capability-only step declarations with placement computed once per attempt; a step attempt is an ordinary child thread; delivery as a conductor action over the channel adapter; one run verdict as the outcome record; alerts as deliver-only runs; `kian job list\|status\|run\|cancel` | landed |
| P1 | kian-superagent | The kernel: dependency-ordered readiness, per-attempt retries with a bounded backoff, the effect gate (`needs_operator` for an unknown write), text artifacts by reference with a cap breach as a failure, the run deadline, the outcome lattice | landed |
| P2 | kian-cli | The worker binding view, HTTP/JSON-RPC/SSE status, sweep-side alerting, `resume --assume-no-effect`, schema checks on step output, channel-side dedupe | designed |
| P3–P4 | — | Binary artifacts (needs a worker upload seam), enforced cancellation (needs a core interrupt that cancels an in-flight tool future), per-step cost, event triggers, `max_parallel > 1` | designed |

A run's trigger is a cron schedule whose runtime target is the job (§7.1 of the contract), so the
durable cursor, misfire policy and settlement sweep are the ones already tested — a job firing adds
no scheduler and no table. A step's success is never an exit code: it is the declared check set
(§11.2) evaluated over the child's own committed facts, and a delivery is the channel's receipt.
The run's outcome rides one additive `evidence/verdict` with gate `run`, because the engine — not a
driver — owns the session end.


### Self-evolution (S1 landed, S2a landed, S2b landed, S3 landed)

Scope: a committed failure becomes a reviewable proposal. A self-dev run is an ordinary job run
whose subject is a verified evidence pack, and no run merges, deploys or holds a credential while
proposing. No new session state, event kind, table or daemon: the marking travels as one additive
key on the admitted row. The normative contract is
[design/SELF-EVOLUTION-MODEL.md](../design/SELF-EVOLUTION-MODEL.md); the decision records are
[ADR-069](../design/decisions/ADR-069-self-evolution-is-a-proposal-pipeline.md),
[ADR-071](../design/decisions/ADR-071-publish-is-a-conductor-action.md) and
[ADR-075](../design/decisions/ADR-075-landing-is-the-owners-merge-verified.md).

| Phase | Contents | Status |
|-------|----------|--------|
| S1 propose | `kian job run --pack <dir>` verifies the pack before admitting, the `job.selfdev` key, the `patch_paths_allowed` surface check, the `consecutive_failures` breaker, the proposal delivered as artifacts on the run | landed |
| S2a publish-local | the conductor's `publish` step: one derived worktree and branch at the frozen base, the patch artifact applied and committed with the audit trailers, the configured scoped gates re-run inside the worktree — no credential | landed |
| S2b publish-remote | one non-forced push of that branch and one pull request whose body carries the audit chain, with the credential read from the environment at publish time | landed |
| S3 land | the owner merges the reviewed pull request; `kian self verify <run>` recomputes the audit chain (read-only) and `--merged` reports the merge | landed |

A symptom is packaged by `kian self evidence` (§3.1). A proposal's `succeeded` means "a reviewable
patch exists inside the declared surface", never "the defect is fixed": the patch is evidence, and
the merge is the owner's decision on the pull request.

An agent step produces its artifact from the child's own stream: the final answer — the last
`assistant/message` carrying text and no `tool_calls` — is the artifact, so `artifact_within_cap` and
`patch_paths_allowed` read the bytes the producer committed
([design/JOB-MODEL.md](../design/JOB-MODEL.md) §12.1). A child that never reaches a final answer
produces no artifact, and the step fails its artifact check instead of passing it.

Publishing is a conductor action, not a general command: the `publish` step's sequence is fixed
(worktree, apply, commit, gates, one non-forced push and one pull request), every step
leaves a receipt on the run stream, and a plan can ask for a publish but never for an arbitrary
command ([design/JOB-MODEL.md](../design/JOB-MODEL.md) §13.5,
[design/SELF-EVOLUTION-MODEL.md](../design/SELF-EVOLUTION-MODEL.md) §9.4).

S2a lands that sequence with no credential and no network write: the receipt carries the branch,
the commit and the gate's exit code, `kian job status <run>` renders it, an operator whose
configuration has no `selfdev.publish` section sees the step fail honestly with
`publish is not configured` before anything is written, and the gate is the operator's
configuration — a plan cannot choose the evidence that judges its own proposal. A worktree already
on disk, or an intent whose receipt was never written, settles the attempt `needs_operator`: the
worktree stays as the evidence and the attempt is never retried.

S2b carries that receipt to the code host: one non-forced push of `selfdev/<run_session>` and one
pull request whose body carries the audit chain, with the credential read from
`selfdev.publish.token_env` at publish time and exported only into those two child processes. A
push that landed while its pull request failed settles `needs_operator` with the branch on the
remote, and `kian job status <run>` renders `pushed` and the pull request URL alongside the
commit.

S3 keeps the merge with the owner: no `land` verb, no auto-merge, no credential. `kian self verify
<run>` reads the run streams and the local repository and recomputes the chain — the frozen plan,
the artifact bytes, the commit's parent, diff and `Kian-*` trailers, the branch — and `--merged`
reports whether the commit is in the base branch's history. It writes nothing and never fetches; a
chain that breaks exits 2 naming the link. The landing shape is a merge commit, because a squash or
a rebase drops the artifact commit from history
([design/SELF-EVOLUTION-MODEL.md](../design/SELF-EVOLUTION-MODEL.md) §18).

### Deferred (unchanged)
| Item | Reason |
|------|--------|
| Web UI | Polish core semantics via CLI/API first |
| Multi-Router horizontal sharding | Article experiments: single Router has no throughput bottleneck at team scale |
| Credential-less credential use | Standalone security topic, + |
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
| Read cost | Unchanged projection reads cost one version check; stream polling is O(new events); benchmarked |
| Resume | A recoverable session resumes from the latest verified snapshot + tail (P1) |
| Retention | Opt-in, terminal-only; the archive package reproduces the audit export byte-identically |
