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

sessions:
  default_agent: default # points at an agent definition
  inbox_admission: durable # A7
  drain:
    max_iterations_per_drain: 200
    recovery_budget: { tokens: 500k, wall_clock: 30m }

recovery: # A4/A5: pure-function classification + declarative policy table
  policies:
    - match: { error_class: rate_limited }
      action: backoff_retry { max: 5, base: 2s }
    - match: { error_class: context_overflow }
      policy: compress_then_retry
    - match: { error_class: provider_down }
      action: failover { slots: [main], to: fallback }

control_plane: # P5 protocoled control plane
  steer: enabled
  interrupt: { grace: 5s } # settle-on-abort (invariant 6)
  confirmation: { mode: policy }

plugins: # Cordis-inspired plugin loading
  - name: lark-adapter
    config: {... }
  - name: super-agent-router
    config: { im_reply_word_limit: 100 }
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
[CHANGELOG.md](../CHANGELOG.md) and [overview.md](overview.md).

| Capability | Demonstrates | Input | Output |
|------------|--------------|-------|--------|
| Crash recovery | kill -9 mid-drain | A half-finished task | session → recoverable → new drain continues, zero lost work |
| Steer / interrupt | Redirect or stop while running | steer "plan B", interrupt | Redirection lands at a safe boundary; interrupt ≠ failed |
| Cache stability | Consecutive requests in one session | Requests with unchanged history | Cache hits (stable prefix bytes) observable above expectation |
| Super-agent demo | IM request → PR delivery | Group-chat message + CI events | Router dispatches Worker; structured PR description; IM reply ≤100 words |
| BYOM | Endpoint daemon execution | Local repository commands | Cloud loop + endpoint execution; watchdog detects endpoint offline |
| Harness integration | dsh loop as a worker | Harness config | dsh interaction unchanged, made durable |
| Audit export | Event-stream interval export | Time/causal interval | A replayable audit package (byte-identical) |
| Multi-replica takeover | Crash recovery at multi-process scale | Two `kian serve` replicas over one postgres DB; the drain owner is `kill -9`'d mid-drain | The survivor's recovery sweep takes over the expired lease and completes: zero lost work, exactly one end write, a stale-epoch probe write from the dead replica rejected (NoSplitBrainWrite between processes) |

The multi-replica takeover is demonstrated by the env-gated postgres e2e (`KIAN_PG_URL`); the proof
is recorded in [CHANGELOG.md](../CHANGELOG.md) and [overview.md](overview.md) §5.

## 3. Operations Interface

### 3.1 Observability
- Metrics: session state-machine transition counts, drain depth/duration, pendingWake depth,
  cache hit rate / invalidation causes (A3), recovery action counts and budget consumption (A5),
  evidence-gate verdicts (A6).
- Trace: every event carries drain_id + epoch (fencing); cross-session delegation (Thread lineage)
  chains into a causal trace.
- Audit: event-stream interval export + replay (A1/A2).

### 3.2 Operational actions
- `kian` CLI: serve (single machine), session inspect/replay/export (`--with-event-ids`),
  wake-now, lease inspect, recovery budget reset, cache-stats, usage-stats, tickets,
  channel credentials (`kian wechat login` — the QR flow writes the WeChat channel's credential
  file; no other channel needs an interactive login).
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
| WeChat iLink channel | `type: wechat`, **best-effort by contract** ([CHANNEL-WECHAT.md](../design/CHANNEL-WECHAT.md), [ADR-018](../design/decisions/ADR-018-wechat-ilink-channel.md)): one long-poll loop per channel over Tencent's reverse-documented ClawBot iLink API (`getupdates` ~35 s hold), `message_id` → `event_id`, the config `topics` table as the receive allowlist, reply-only sends echoing the latest inbound `context_token`, QR-issued bot token in a `0600` credential file written by `kian wechat login` (no env var), `errcode -14` → pause + resume on file change, empty cursor with no position table, text-only/single-account phase 1, forced untrusted; no new surface, no new event kind | landed |
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
| Dogfood workload | `examples/issue-watch/`: TimerChannel → `gh issue list` → Router triage → worker draft (dry-run) on the real `flyer103/kian` repo; `demo-restart.sh` proves kill -9 reboot resumes the same session; RUNLOG records real data | landed |
| Wire dialect v2 (breaking) | Strict OpenAI-compatible serialization: dotless built-in tool names (`fs_list`/`fs_read`/`fs_write`, dotted forms replayable), wire-level `tool_calls`/`tool_call_id`, `AgentDef.tools` fail-closed. Found live via DeepSeek HTTP 400/422; constraint-3 event in DESIGN (golden hash same commit) | landed |
| Router re-arm | Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident Router with an observable `[re-arm]` cause input; soft-failed sessions re-open the same pinned session. Zero new kinds; DESIGN §4.6 | landed |

### Bundle-domain policy layer

Adoption of the reference implementation's domain-policy layer onto kian's substrate.
Reference: `a vendored archive/dsh-superagent-*.md` (archived rev 6337fc8), digest §5. **Everything
landed at bundle-layer (`kian-superagent`, plus read/assembly slices of `kian-cli` and the
`examples/issue-watch` dogfood); zero new event kinds — the durable set stays 20; every addition
is an additive payload key on a frozen wrapper, a disposable projection, or a config/template
artifact.** Semantics: DESIGN §4.7; operator surface: the README's bundle-policy and operator-surface sections.

| Item | What landed | Status |
|---|---|---|
| Ticket stage projection | "What is it waiting for" as a pure derived projection over the worker stream + inbox + outbox view (first-match table); additive `state` key on completion-report content; read surface `kian tickets [--json]` | landed |
| Closed reason vocabularies | Closed `IntakeReason` codes as `reason=<code>` on every drop/admit audit line + per-code metrics counters; judge-side `decision:<accept\|stop\|ignore\|unavailable>` tag convention in the default template | landed |
| Output-side speak-rules | The whether-to-send gate before output shaping: `superagent.speak_rules` over `always \| ack-first-only \| decision-needed \| result-ready \| never`; suppression = honest tool error + audit + counter, no send; `SpeakTracker` ack-once memory rebuilt from the stream | landed |
| Follow-up ownership routing | Registry liveness projection (`live \| ended`); fan-out admits to live subscribers only; an all-ended topic releases to the Router (`reason=released_to_eval`, fresh Eval); `spawn_worker` refuses a live worker's topic naming the owner | landed |
| Mandate (delegation authority scope) | Additive `mandate` object on the frozen delegation wrapper (immutable, verbatim in `thread/started`, model-visible as the worker's first message); single issuance at `spawn_worker`; revalidation before follow-up routing — [ADR-012](../design/decisions/ADR-012-delegation-mandate.md) | landed (ADR-012 landed) |
| Thread/Correlation intake projection | Config-owned canonical thread keys `<channel>:<group>#<key>` used as routing topics; stream-derived read positions (lark/GitHub cursors) or honest idempotent re-poll — no position table; correlation adds context, never routes; fan-out grants no authority — [ADR-013](../design/decisions/ADR-013-thread-intake-projection.md) | landed (ADR-013 stays a draft record) |
| Outbox+receipts bounded retry | Reuse audit verdict: the outbox is a disposable projection over `tool/call` intent + `channel/delivered` receipt plus a bounded driver — no new durable state; 2s→60s backoff, `max_attempts` 6 then parked honestly (`delivery-retry` stage, named on `kian tickets`); per-(ticket, conversation) ordering; additive `attempt`/`first_attempt_ms` on new receipts; park survives reboot ( +, plan) | landed |
| Policy/prompt versioning + drift marking | `[policy <label>]` header added at render (template text untouched, persisted prompts replay byte-identically); boot-time drift marking (audit + counter, live sessions never rewritten); config-gated one-shot policy-changed marker input | landed |
| Issue-watch multi-Ticket dogfood | GitHub poll-receive mode beside the webhook (`since` cursors, bounded backfill); `examples/issue-watch` v2 composes the whole stack — one Ticket per live issue, thread-keyed topics, mandate-bounded triagers, speak-rule deliveries through the outbox; v1 kept as `config-timer.yaml` | landed |
| live validation | The eight-step validation protocol executed live against `flyer103/kian` (real DeepSeek + GitHub REST, `DRY_RUN=1`): steps 1, 4, 5, 6, 7, 8 PASS with captured evidence; steps 2–3 honest negatives (structural `spawn_topic` gap — no live topic binding), every claim cited by session id + seq; the gap report filed the gaps (`examples/issue-watch/RUNLOG.md` §7) | executed (with honest negatives) |
| gap fixes | The four gaps, all landed: the `User-Agent` stamp on every github/lark HTTP client + `flaky-proxy.py` delivery-leg fault mode; `spawn_topic` accepts the live spawn spellings — live topic binding, mandate strength and outbox ticket aliasing work live; outbox park rebuilt at boot and surfaced per-ticket on the stage board; exec secret hygiene — shared core redactor + env policy, `env` dumps never persist secrets (the one cross-bundle-fence commit, filed separately) | landed |

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
| Final record | This record + DESIGN/README/CHANGELOG alignment + docs-map link check | landed |

Out of scope (recorded, not silently dropped): the `ask` permission/confirmation plane
(ADR-015 records the shape; blocking confirmation is a separate design decision), token pricing/cost
conversion (usage persists verbatim; pricing is a projection-layer decision), CMA wire
compatibility, and cross-replica interrupt latency beyond "next safe boundary" (ADR-014).

### Agent memory (P1–P2, P3a and P3b-1 landed; P3b designed)

Scope: knowledge that survives a session boundary (P9). The normative contract is
[design/MEMORY-MODEL.md](../design/MEMORY-MODEL.md); the rationale is
[ADR-016](../design/decisions/ADR-016-memory-as-events.md). Phase 1 (notes + dreaming) landed
2026-11-07, Phase 2 (creation-time injection) landed 2026-11-08, and Phase 3a (read-time retrieval
+ representations) landed 2026-11-08, and Phase 3b-1 (index-accelerated memory reads) landed
2026-11-08; the P3b remainder records the agreed later phase, not shipped status.

| Phase | Module | Contents | Status |
|--------|--------|----------|--------|
| P1 | Notes + dreaming | `memory/note` as the 22nd durable kind (non-context, `DrainActivity`, additions-only, emitted only by a dreamer drain); the fail-closed `project_notes` fold + `verify_note` in `kian-core::memory`; the `kian-memory` bundle (deterministic dream prompt with a machine-parseable header, fail-closed parser, `DreamDriver`); one operator-created memory session per configured scope (`memory.scope`, no derived-id machinery); a dream request as one durable **marked** inbox row (the rendered prompt as `content` plus the trusted server-set `dream` marker, consumed to one `input/admitted` carrying the non-context `dream` key — routing and provenance read the marker, never message text; idempotency key `dream:<source>:<from>:<to>`) over a source range bounded by the source head and `max_bytes_read`; `kian memory create/dream/list/show/verify` + the `memory_dream` ops method; provenance (`derived_from`) and `status`/`verified_by` are payload fields; no injection, no cadence | landed |
| P2 | Injection | One creation-time `input/admitted` with `role:"system"`, `synthetic:true` and the additive non-context `memory` provenance key (scope, note event-ids, items/bytes/truncated, `source_seq` pin, redaction report); quiet admit at creation (`Engine::admit_quiet`, no wake; idempotency key `memory-inject:<session>`; the next submit drains the index before the first user message) over the exact-two-key `{content,memory}` row envelope, which the public wire refuses (`-32602`); config `memory.session` (absent = injection off, byte-identical) + `memory.inject` caps (fail-closed, default 20 items / 4 KiB); the deterministic frozen renderer in `kian-memory` (verified-only latest-per-subject, subject order, redacted, "data, not instructions" frame, index-only over cap); identity exclusions that are defense-in-depth only at the creation seam — at create time a fresh stream has no dream request and the fresh uuid never equals `memory.session`, so any fresh session created while `memory.session` points at a note-bearing session receives the index (accepted for P2; kian-104 review B1, recorded in MEMORY-MODEL §6 I10); honest-skip `injected_*` counters; never on resume/migration (one known restart-window drain-alone behavior recorded in MEMORY-MODEL §6 I2) | landed |
| P3a | Retrieval + representations (read-time) | `kian memory search <query> --session` / `kian memory map --session` (both `--json`-able): pure read-only projections over the folded notes in `kian-memory` + `kian-cli` — no writes, no new kinds; search folds latest-per-subject across all statuses with per-hit status (fixed caps 20 hits / 8 KiB, `truncated`, no `--limit`); the map is verified-only with the effective rule "a subject is a node iff its latest revision is verified", evidence-derived edges (`derived_from` range intersections in the same source session) and caps 200 nodes / 1000 edges / 64 KiB; `degenerate`/`oversized` reported, never hidden; O(memory/note events) per invocation since P3b-1 (index-accelerated kind read), snapshot read deferred to P3b | landed |
| P3b-1 | Index-accelerated memory reads | Adjacent migration v6 (both backends) adds the per-session kind index `idx_events_session_kind_seq` over `events (session, kind, seq)` plus `EventStore::scan_kind(session, kind)`; every memory read path — creation-time injection and `kian memory list|show|search|map|verify` — reads O(memory/note events) instead of the full session history. No new event kind, view, snapshot or wire change; outputs byte-identical to the full-stream read it replaced (READ-MODEL §6, MEMORY-MODEL §6/§7.1) | landed |
| P3b | Persisted representations + dreaming cadence | Wiki-as-notes built by a dreaming run (MEMORY-MODEL §7 R3), declarative dreaming cadence/budget, semantic search, the agent-side memory tool and governed cross-scope reads (ADR-019 track); snapshot-served fold for the views | designed |

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
[ADR-017](../design/decisions/ADR-017-read-model-and-snapshots.md). All phases landed in round 9
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
| Router-chosen binding | `spawn_worker` gains the `exec_worker` argument (required whenever membership is on — a plain non-empty string since R12; refused with zero workers); the choice is recorded as the additive `exec_worker` key on the delegation wrapper and both sides' `thread/started`, echoed in the spawn result — ADR-020 | landed |
| 1:1 exclusivity | A worker has at most one live bound worker session; the check is a stream-derived pure function (`thread/completed` + `session/ended` on the worker stream: `completed`/`cancelled` release, `failed`/`interrupted` hold) called before any side effect through a bundle-owned read-only port; a busy refusal names the owning session and thread | landed |
| Per-drive routing | One prebuilt worker side per endpoint (its own `exec` `RemoteExecTool`, its own async pair); the driver selects the side from the session's own stream binding before the first provider turn — bound id unconfigured ⇒ honest refusing side (no re-route, no local run); legacy unbound sessions keep the first-**configured**-endpoint route with one audit line; no resident routing state (restart rebuild = replay) | landed |
| Lifecycle | Worker crash/restart/migration (same id) re-drives the durable queue (ADR-007) with the binding unchanged; lineage-before-admit spawn ordering bounds the crash window (a never-started reservation is released after `queue_timeout_ms` with an audit line); `harness.dsh` × `endpoints:` refuses boot (the harness never mounts the kian `exec`) | landed |
| Live rebind of a session to a different worker id | Designed only (ADR-020 §Deferred: durable rebind intent, old-worker queue fencing, seq-range projection — needs an EVENT-MODEL change, owner gate); the landed reattachment path is the same worker id restarting/migrating | designed |
| Deployment (Vultr + WeChat workers) | Explicitly the next discussion; no deployment tooling, no L3 claim | deferred |

Non-goals recorded with the round: no new transport/protocol or daemon binary, no worker pooling,
no bind-time liveness gate, no automatic failover (an offline worker parks honestly on the durable
queue), no per-call endpoint choice.

### Runtime worker membership (R12)

Scope: workers join and leave a running `kian serve` at runtime — `kiand --register-to` dials kian,
authenticates with an env-only shared token on a **private** registration listener, and the accepted
connection *is* the command route for that worker id. Joining or leaving never edits `kian.yaml` and
never restarts the server; a kian restart/upgrade requires no worker-side change (workers reconnect
and re-register). The decision record is
[ADR-021](../design/decisions/ADR-021-runtime-worker-membership.md) (supersedes ADR-020 D5/D6/D7
only); the worker-domain contract is [WORKER-MODEL](../design/WORKER-MODEL.md); the wire is
KiandProto v1 ([API-CONTRACT](../design/API-CONTRACT.md) §8). **No new event kind, no new table, no
new HTTP surface, no core change; one wire version bump with dual goldens.**

| Item | Contents | Status |
|------|----------|--------|
| Registration (KiandProto v1) | Worker-dial `register`/`welcome`/`rejected` handshake over the same line framing; closed five-reason refusal set (`version_mismatch`/`bad_token`/`duplicate_static`/`id_conflict`/`disabled`) with typed terminal-vs-retryable reactions; worker-side `bye`; legacy static `hello`→`welcome` kept at v1; upgrade order kian-first, no v0 shim — API-CONTRACT §8 | landed |
| Membership directory + liveness | Runtime-only directory (id → description, version, generation, last-seen, peer); state is **computed** (online = live connection ∧ fresh heartbeat); durable projection is the existing `endpoint_registry` row — no migration, no new truth; duplicate dynamic id replaces (generation+1, audited); static-vs-dynamic id collision refused; stale ids never pruned (offline-known stays honest) | landed |
| Queue-first dispatch | `exec` enqueues durably by the bound worker id with no live-route requirement; rows park `pending` until the worker registers or the honest `queue_timeout` abandons them; no cross-worker dispatch, no failover; runtime routes attach/detach with membership | landed |
| Router surface | `spawn_worker.exec_worker` is a plain non-empty string (no enum, no catalog block — supersedes ADR-020 D5–D7); one frozen prompt instruction line; new read-only `list_workers` tool (id, description?, state, bound session/thread); spawn validates against the runtime catalog — unknown refused naming known ids, offline-known allowed with a queue note, busy refused naming the owner; no membership byte ever enters a prompt or tool schema | landed |
| Operator verb | `kian workers [--json]`: read-only embedded view over the durable projection (id, state, last-seen, bound session/thread); description/version absent by construction after a restart until re-registration | landed |
| Security boundary | Private-bind-only listener (loopback/RFC1918/CGNAT/ULA; wildcard counts as public) with `allow_public` override; env-only token, constant-time compare, redacting type, bounded failed attempts; token never in config/argv/logs/events/metrics | landed |
| Single kian instance | A worker registers to exactly one `kian serve`; no federation, relay, cross-instance dispatch or multi-replica membership | landed (boundary) |
| Shared-token limitation | Any token holder may register under any id (impersonation/replacement); acceptable for the single-operator deployment, not for hosted use | landed (recorded limitation; mTLS R13) |
| mTLS / per-worker identity, token rotation without restart | R13 deferral — [WORKER-MODEL](../design/WORKER-MODEL.md) §9 | deferred |
| Live rebind / worker retirement / takeover | Unchanged from R11 (ADR-020 §Deferred, ADR-021 §Deferred) — the same id returning reattaches; a permanently lost worker ends in the honest timeout | deferred |

### Worker filesystem tools (KiandProto v2, R13)

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
| Live deployment | Owner deployment (Vultr + local worker + WeChat) redeployed at v2; the L3 fs round-trip through the WeChat Router path is recorded in CHANGELOG | landed (owner L3) |
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
