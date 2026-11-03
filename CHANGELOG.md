# Changelog

Kian is built in rounds ("phases"), each ending in an acceptance gate. This file records what
landed, when, and how it is proven. Scenario definitions (S1–S8) live in [docs/SPEC.md](docs/SPEC.md)
§2.5; the phase scope tables are in SPEC §5. Entries are ordered oldest first.

Two labels are used throughout:

- **default-green** — runs in `cargo test --workspace` on any machine, no credentials, no services.
- **env-gated** — runs only when its environment variable is present; absent, the suite skips with a
  log line and never fails. Gated surfaces are listed in
  [README → Environment contract](README.md#environment-contract).

---

## Phase 0 — 2026-10-31 — design baseline (documentation only)

- `docs/SPEC.md` (WHAT), `design/DESIGN.md` (HOW), and the first six ADRs (Rust foundation,
  event-sourced core, super-agent as a profile, Cordis-inspired plugins, fencing via `expectedSeq`,
  model slots).
- `AGENTS.md` with the hard constraints that bind every later change; `the design notes`
  synthesizing the four design inputs; frozen snapshots of those inputs in `a vendored archive/`.

## Phase 1 — 2026-11-01 — MVP: durable sessions (S1–S3) — default-green

- **Workspace**: eight crates (`kian-core`, `kian-store`, `kian-orchestrator`, `kian-llm`,
  `kian-loop`, `kian-plugin`, `kian-endpoint`, `kian-cli`).
- **Event schema v0**: 15 event kinds; session state machine; projection engine; model-context
  derivation. State is read only from the projection of the append-only stream.
- **EventStore on sqlite**: single-transaction appends, `expectedSeq` CAS and epoch fencing — stale
  writes from a dead drain are rejected, not repaired.
- **Admit → Wake → Drain → Promote**: durable inbox rows before any wake, advisory (foldable, losable)
  wakes, promotion only at provider-turn safe boundaries, per-session serial drains, inbox rebuild at
  startup so wakes lost to a crash are re-derived.
- **Three-layer recovery**: a pure error-classification function, a declarative policy table owned by
  the caller (`recovery:` in `kian.yaml`, required — no silent defaults), a one-shot guard per
  recovery kind and a token/wall-clock budget with refund semantics.
- **`kian-llm`**: openai-compatible provider, model slots fixed per session, persist-before-send
  (request bytes are an event before the call flies).
- **`kian-cli`**: `kian serve` (JSON-RPC `/rpc`, SSE `/sessions/{id}/events`, `GET /metrics`) plus
  `session inspect` / `session replay` / `session export` / `wake-now` / `lease inspect` /
  `recovery budget reset` / `cache-stats`.
- **Acceptance**: S1 crash recovery (`chaos_s1.rs`, six kill points), S2 steer/interrupt honesty
  (`s2_steer_interrupt.rs`), S3 cache-byte stability (`s3_cache_stability.rs`) — all default-green.

## Phase 2 — 2026-11-01 — super-agent bundle + BYOM endpoints (S4–S5) — default-green

- **Four additive event kinds** (`thread/started`, `thread/completed`, `evidence/verdict`,
  `channel/delivered`); the durable set grows 15 → 19.
- **`kian-superagent`** (the bundle, never the core): resident Router profile, Worker session
  templates, Thread delegation, event subscriptions, channel adapters with output policies
  (IM ≤100 words; structured async delivery), and a mock channel for demos.
- **Evidence gate v1**: bounded, declarative stream checks; a failed gate yields an honesty-preserving
  soft failure — a worker that cannot prove completion never reaches "delivered".
- **BYOM**: `kian-exec` (KiandProto server half: connection manager, endpoint registry, durable
  command queue, heartbeats) + `kian-endpoint` (endpoint-side SDK) + `kiand` (the daemon binary).
  At-least-once delivery with command-id dedupe ([ADR-007](design/decisions/ADR-007-kiand-delivery-semantics.md));
  a queue timeout is an honest `Timeout` and the row is `abandoned`, never a fake success.
- **Watchdog v1**: endpoint heartbeats; an offline endpoint admits a durable health input to the
  affected worker session.
- **Acceptance**: S4 (`s4_superagent_demo.rs`, 8 tests including the gate-failure negative path) and
  S5 (`s5_byom_e2e.rs`, real `kiand` child process) — default-green.

## Phase 3 — 2026-11-01 — article-sync: recall, admission economics, channels (S4-extended, S7) — default-green

- **One additive event kind** (`input/spilled`); the durable set grows 19 → 20, and stays there.
- **Inline/Spill admission shaping**: an oversized channel payload admits as a bounded Inline view
  (excerpt + association ids + continuation handle); the verbatim raw is appended in the same atomic
  consumption write as a non-context-bearing sibling. Shaping happens once, at admission — committed
  context is never rewritten (cache-byte stability).
- **Recall toolpack (Chain of Recall)**: read-only `recall_event` / `recall_neighbors` /
  `recall_thread` over a `RecallPort`; every output is itself Inline/Spill-shaped so multi-hop recall
  composes; unknown ids are honest tool errors.
- **Two-level admission filtering**: a `classes:` table (`candidate` / `explicit-only` / `ignore`,
  most-specific-wins) evaluated before the rule set and before any model call; drops are audit lines
  and counters, not rows.
- **Async `start()`/`stop()` tools** ([ADR-008](design/decisions/ADR-008-async-tool-completion-admitted-input.md)):
  immediate ack with a job id; the completion returns as an admitted input, durable before its wake
  and promoted at the next safe boundary; `stop` labels the result `cancelled`. Default off.
- **Many-to-many passthrough fan-out**: a topic maps to the set of workers that served it; one event
  admits one row per subscriber, idempotent per `(session, event_id)`.
- **Speculative execution**: an additive, audit-visible delegation marker with a stream-derived
  outstanding cap and honest tool errors; a discarded speculation audits every token and delivers
  nothing.
- **Watchdog Session v2**: a model-in-the-loop progress judge driven by `type: timer` channel ticks;
  `intervene` admits a steer row durable-before-wake, `escalate` replies under the IM policy, and
  every tick (including a healthy no-op) is audited in the watchdog's own stream.
- **Batching metrics + ablation benchmark**: `superagent.router_batching` on `/metrics`
  (turn count, batch size, amortized ms/input) and a deterministic passthrough-vs-Router ablation
  that asserts directionality, never the article's numbers.
- **Untrusted-input marking**: `"untrusted": true` in the provenance wrapper for configured channels
  (real channels default untrusted); the marker survives every recall hop.
- **Real lark channel adapter**: Feishu IM receive (chat poll) and send over the open APIs, env-only
  credentials, fail-closed startup.
- **Acceptance**: S4-extended legs (recall, async, watchdog) and S7 (audit export/replay,
  **demonstrated**) — default-green.

## Phase 4 — 2026-11-01 — hosted foundation: postgres, multi-replica, SDK freeze, ecosystem surfaces (S6, S8)

- **`kian-sdk`** (twelfth crate): the wire-only embedded client extracted from `kian-cli` — typed
  methods, the SSE helper, matchable error codes, golden wire fixtures, and **no dependency on any
  kian crate**. Frozen additive-only after this round; a breaking change needs a new plan round.
- **Postgres dual backend** in `kian-store` and `kian-exec`: an enum-dispatch `EventStore` with
  identical semantics (single-statement end write, batch CTE append, PK-race classification), one
  server / one database / one pool. URL is env-only (`KIAN_PG_URL`) and postgres mode requires an
  explicit unique `drain.holder` — both fail closed. Export parity (byte-identical JSONL) is the
  compat golden. — **env-gated** (`KIAN_PG_URL`)
- **Multi-replica semantics** ([ADR-009](design/decisions/ADR-009-multi-replica-claim-admission.md)):
  conditional claim admission (a claim lands only when no claims row exists, the lease expired, or the
  holder is the same process), postgres LISTEN/NOTIFY as an advisory *accelerator* only (notify after
  commit; sweep + startup scan bound any miss), and single-statement row leases on `exec_queue` /
  `async_jobs` that are liveness-only — safety stays with epoch fencing.
- **GitHub channel + verified webhook receiver** ([ADR-011](design/decisions/ADR-011-inbound-webhook-plane.md)):
  a fourth server surface at `POST /webhooks/github` that verifies **before** it parses — body-size cap
  (413) and content-type gate (415) precede signature work, HMAC-SHA256 constant-time compare (401,
  zero parses), then a per-source token bucket (429 with an audit line), then admit-only delivery with
  `(session, event_id)` idempotency. Token and secret are env-only.
- **`kian-dsh`** (thirteenth crate, S6, [ADR-010](design/decisions/ADR-010-dsh-session-mapping.md)):
  a `LoopDriver` that drives a `dsh --profile sdk` child over stdio JSON-lines and maps its session
  events onto existing kian kinds. Config-gated and default off (`superagent.harness.dsh`); depends on
  `kian-core` only (narrow waist).
- **Zero new event kinds** this round; the durable set stays 20.
- **Acceptance**: S8 multi-replica takeover **demonstrated** (`postgres_s8.rs`, two real `kian serve`
  replicas, real `kill -9`, stale-epoch probe rejected) — **env-gated**; S6 **demonstrated by
  fixtures** in default CI (`driver_bridge.rs`, `dsh_conformance.rs`, `dsh_bridge.rs`), with the live
  leg **executed** against real dsh 0.2.1-rc.5 (`dsh --profile sdk`, `KIAN_DSH_BIN` set) and coming
  back an honest negative: the stock dsh SDK app is client-initiated JSON-RPC and never sends `ready`
  within 30 s, so the bridge stopped at `SoftFailed { reason: "peer never sent ready within 30s" }`
  exactly as ADR-010 §3 requires, instead of manufacturing a completion. **S6-live stays open** until
  a contract-v0 dsh-side peer exists; the fixture suites remain the CI proof.

## Phase 5 — 2026-11-02 — live positives, dogfood, and field fixes

- **S6 live positive landed** (`contrib/dsh-v0-peer/dsh-v0-peer.mjs`, kian-59): a zero-dependency Node
  peer adapting stock `dsh --profile sdk` (JSON-RPC 2.0) onto the frozen ADR-010 contract v0 — `ready`
  emitted immediately, `session.event` notifications forwarded as v0 session-event frames, `input`
  frames translated to `session/prompt`. Driving one real input through peer → dsh → real
  `deepseek-chat` appended the full mapped chain (turn/start, input/admitted, step/request,
  assistant/message, turn/end) with an honest `Settled` outcome — the S6 live leg is now **green**,
  upgrading the 2026-11-01 honest negative. Re-run: see `docs/runbook-dsh.md`.
- **Dogfood example** (`examples/issue-watch/`, kian-60): a real long-running workload on
  `flyer103/kian` — TimerChannel ticks → `gh issue list` → Router delegates triage → worker drafts a
  reply (dry-run by default) → everything in the durable audit stream. Proven live: real issue triaged
  (severity, decision, draft in-stream), and `demo-restart.sh` shows `kill -9` + reboot resuming the
  same session from the event stream. `RUNLOG.md` records the real data.
- **Wire dialect v2** (kian-61, breaking): strict OpenAI-compatible serialization — built-in tools
  renamed to dotless spellings (`fs_list`/`fs_read`/`fs_write`; dotted forms still invocable for
  replay), wire-level `tool_calls`/`tool_call_id` emitted on tool round trips, `AgentDef.tools`
  enforced fail-closed. Found by the dogfood run replaying persisted `step/request` bodies against the
  real DeepSeek API (HTTP 400/422); constraint-3 event recorded in DESIGN (golden hash updated in the
  same commit). After: direct-to-DeepSeek green — 13 sessions, 59 requests, 0 failures.
- **Router re-arm** (kian-62): a resident Router whose turn soft-failed no longer stayed terminally
  frozen across reboots. Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident
  with an observable `[re-arm]` cause input on the successor stream; soft-failed sessions re-open the
  same pinned session. Zero new event kinds; DESIGN §4.6 records the semantics.

## Gated surfaces — current live-run status

| Surface | Gate | Status |
|---------|------|--------|
| Multi-replica / postgres (S8) | `KIAN_PG_URL` | Suite in place; **executed green** in CI on every push since 2026-11-02 (postgres service container; the job also asserts the gated suites ran, not skipped) |
| dsh bridge (S6 live leg) | `KIAN_DSH_BIN` | Suite in place; **executed green** 2026-11-02 via the contract-v0 peer (`contrib/dsh-v0-peer/`) against dsh 0.2.1-rc.5 + real `deepseek-chat` — full mapped chain, honest `Settled`. (The 2026-11-01 stock-dsh honest negative is superseded; runbook records both.) |
| Live provider smoke | `KIAN_API_KEY` | Suite in place; **not executed** — no provider key in the build environment |
| Live lark channel | `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` | Suite in place; **not executed** — no Feishu app credentials in the build environment |
| GitHub send / webhook | `KIAN_GITHUB_TOKEN` / `KIAN_GITHUB_WEBHOOK_SECRET` | Covered by scripted fixtures, default-green; no live GitHub delivery executed |

These are deliberately honest gaps, not silent claims: the fixture-driven suites prove the semantics,
and the README run logs record each gated surface's real state — the dsh leg with its executed
outcome (an honest negative, kept as the true record), the rest as explicitly not yet executed.

## Unreleased

- **Second dogfood example** (`examples/kanban-watch/`, 2026-11-03): kian watching a local
  Hermes Kanban board — TimerChannel ticks → Router delegates a poll worker
  (`hermes kanban --board <board> list --json`) → stuck tasks (closed stage vocabulary:
  stalled-running / blocked-needs-input / review-idle / failure-loop) → one stage worker per
  (task, stage) that re-verifies via `show --json` and, in live mode (`DRY_RUN=0`) only, pushes
  the task forward (`reclaim` / `unblock` / `comment`, all carrying the `kanban-watch:` marker;
  dry-run by default). Model provider: the local vadis gateway (OpenAI-compatible,
  `127.0.0.1:8290`) — no external API key. Zero new event kinds. Proven live against a
  throwaway test board: precise stuck classification (a healthy running task not flagged, a
  blocked task flagged `blocked-needs-input`), stage worker judging needs-human (non-mechanical
  block) without writing, (id, stage) dedupe across ticks 3–4, and kill + reboot resuming the
  same Router session (epoch 2, seq continued, charter re-submit deduped, tick 5 poll normal).
  Not re-run against the real `kian` board yet — first such run should be recorded here.
  Owner-gated next steps remain: making the repo public, executing the remaining gated surfaces
  with real credentials (live provider smoke, live lark channel, GitHub delivery), and any
  Phase-6 scoping. Tracked on the kanban board, not here.
