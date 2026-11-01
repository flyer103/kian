# Kian — Super-agent Platform / Managed Agent Substrate

> **One-liner: the Temporal of agents — an open-source durable execution runtime that makes agent
> tasks killable, resumable, migratable and auditable, with the super-agent reference architecture
> as a first-class workload.**

- Implementation language: **Rust**
- Design inputs: frozen local snapshots in [a vendored archive/](a vendored archive/) (archived
  2026-10-31: the WeChat super-agent article, dsh/cordis docs, two internal design notes); structured
  synthesis in [the design notes](the design notes)

## Docs map

| Document | Type | Contents |
|----------|------|----------|
| [docs/SPEC.md](docs/SPEC.md) | Spec (WHAT) | Positioning, user interfaces, scope, milestones |
| [design/DESIGN.md](design/DESIGN.md) | Design (HOW) | Combined architecture, execution model, super-agent reference architecture, plugin system, crate map |
| [design/decisions/](design/decisions/) | Decisions (WHY) | ADRs: key decisions with alternatives |
| [the design notes](the design notes) | Research | Structured digest of the four design inputs |
| [a vendored archive/](a vendored archive/) | Archive | Frozen local snapshots of the design inputs (read-only) |

Docs-map consistency (every relative link in the authored docs resolves) is enforced by the
`docs_map` test in `crates/kian-cli/tests/docs_map.rs`.

## Core claim

**Agent Orchestrator = a durable execution runtime where the Session is the one first-class
durable entity, the event stream is the sole truth, and the drain is the execution hold interval.
The agent loop is just a control flow inside a drain.**

The four super-agent characteristics (continuous / efficient / proactive / persistent) are not
hard-coded core capabilities but **composition patterns** over Kian primitives (Router/Worker
two-layer, event subscriptions, async promote, BYOM endpoints) — the core stays a narrow waist
(A10).

## Status

Phase 1 (MVP) implemented: an eight-crate cargo workspace under `crates/` —
`kian-core` (event schema v0, projections, model-context derivation), `kian-store` (sqlite
EventStore with CAS/epoch fencing, inbox, leases), `kian-orchestrator` (Admit → Wake → Drain →
Promote engine, three-layer recovery), `kian-llm` (openai-compat provider, slots,
persist-before-send), `kian-loop` (built-in ReAct driver), `kian-plugin` (static plugin system),
`kian-endpoint` (KiandProto endpoint-side SDK), `kian-cli` (`kian serve` control plane + CLI).

Phase 2 (super-agent bundle + BYOM endpoints) implemented on top: three more members —
`kian-exec` (KiandProto server half: connection manager, durable command queue, watchdog),
`kian-superagent` (Router/Worker bundle: subscriptions, channels, output policies), and `kiand`
(the endpoint daemon binary) — assembled into `kian serve` by the additive `superagent:` /
`endpoints:` config surface (see [Phase 2](#phase-2-super-agent-bundle--byom-endpoints) below).

Phase 3 (the article-sync round) landed on the same 11-crate workspace — no new crates, exactly
one additive event kind (`input/spilled`; the durable set is now 20): Inline/Spill admission
shaping + the recall toolpack, two-level admission filtering, async `start()`/`stop()` tools
(ADR-008), many-to-many passthrough fan-out, speculation, Watchdog Session v2, batching metrics,
the ablation benchmark, untrusted-input marking, and the real lark channel adapter (see
[Phase 3](#phase-3-article-sync-recall-admission-economics-real-channels) below).

## Quickstart

Prerequisites: a recent Rust toolchain (stable, 1.85+ for let-chains); nothing else — Phase 1 is
single-machine sqlite with no external services.

### Build

```bash
cargo build --workspace
```

### Configure and serve

`kian serve` reads a `kian.yaml` (fail-closed: a malformed config refuses to start). Providers
take their API keys from the environment — never from the file:

```bash
export MY_PROVIDER_KEY=sk-...   # must match `api_key_env` below
cat > kian.yaml <<'EOF'
listen: "127.0.0.1:7433"
storage:
  sqlite: "./kian.db"
agent:
  id: my-agent
  model_slots:
    main:
      provider: my-provider
      model: gpt-4o-mini
providers:
  my-provider:
    base_url: "https://api.openai.com/v1"
    api_key_env: MY_PROVIDER_KEY
drain:
  max_iterations: 32
EOF
cargo run -p kian-cli --bin kian -- serve
```

### Run a session

The control plane is JSON-RPC over `POST /rpc` plus SSE at `GET /sessions/{id}/events`:

```bash
SID=$(curl -s localhost:7433/rpc -d '{"jsonrpc":"2.0","id":1,"method":"create_session"}' | jq -r .result.session)
curl -s localhost:7433/rpc -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"submit\",\"params\":{\"session\":\"$SID\",\"content\":\"hello kian\"}}"
curl -sN localhost:7433/sessions/$SID/events     # live event stream; ends after session/ended
curl -s localhost:7433/rpc -d "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"projection\",\"params\":{\"session\":\"$SID\"}}"
```

Methods: `create_session`, `submit`, `steer`, `queue`, `interrupt`, `projection`; `events` is the
SSE endpoint. Observability: `GET /metrics` returns engine counters and the cache-byte stability
report.

### Cache-byte stability report (S3)

```bash
cargo run -p kian-cli --bin kian -- cache-stats --config kian.yaml --json
```

Reports, per store: consecutive request pairs compared, how many shared a byte-identical message
prefix (the provider-independent prefix-cache hit-rate proxy), and invalidation causes. A
`compact` event counts under `compact`; **any other prefix change counts under
`unexpected_prefix_change` and is a bug** (constraint 3: compression is the only deliberate
invalidation point).

### Test

```bash
cargo test --workspace
```

Everything — unit, wire-level e2e against a scripted mock provider, and the kill-and-restart
(chaos-style) suite — runs in the default workspace test invocation. Local e2e sets
`NO_PROXY=127.0.0.1,localhost` itself (the macOS system proxy intercepts localhost).

### Chaos

The S1 crash-recovery chaos suite — kill -9 a live `kian serve` at every drain stage (after admit,
after claim, mid-provider-turn, mid-tool, before/after the end write), restart on the same sqlite,
and assert no lost work, no half-terminated sessions and monotone epochs — lives in
`crates/kian-cli/tests/chaos_s1.rs` and runs as part of `cargo test --workspace`.

## Ablation benchmark: passthrough vs. Router judgment (§7.1, C42)

A deterministic, CI-safe replication of the article's §7.1 ablation on kian's own bundle
(`crates/kian-superagent/tests/ablation_passthrough.rs`): one scripted workload — a mock-IM work
request plus a train of same-topic CI-style events on `ci/pr-42` — runs twice under identical
scripted providers, with **passthrough on** (rules route the topic to the dispatched worker; the
train never reaches the Router) and **passthrough off** (every event reaches the Router). It
measures kian's native units — Router judge turns (`turn/start` on the Router stream) and the
read-token proxy (summed `step/request` body bytes on the Router stream) — and asserts
**directionality only**, never the article's numbers, with `ablation_both_configurations_complete`
as the honesty anchor (both runs end `thread/completed{completed}` + `channel/delivered`). Fully
in-process: `ManualClock`, scripted providers, fixed event order, zero credentials, zero network
egress.

```bash
# the CI-sized variant (train of 6) — part of cargo test --workspace
cargo test -p kian-superagent --test ablation_passthrough

# the heavier local variant (trains of 40, three seeds)
cargo test -p kian-superagent --test ablation_passthrough -- --ignored --nocapture
```

- **2026-11-01 (C42)**: directionality holds, strictly, in every observed run. Heavy variant
  (train of 40, seeds 11–13): passthrough on — 3 Router judge turns, ~7.0 KB summed Router
  request bytes, exactly 2 Router inputs (the work request + the completion report); passthrough
  off — 4–7 turns (train batching varies by scheduling; each consumed batch is a judge turn),
  ~34.9–54.4 KB request bytes (the train wrappers ride every post-consumption request under the
  append-only context), 42 Router inputs. Both configurations completed the identical workload
  (`thread/completed{completed}` + `channel/delivered`). One deliberate asymmetry, by
  construction: with passthrough off the CI train can never reach the worker, so after the Router
  has consumed the whole train the test admits an explicit finish row to the worker (with
  passthrough on, the worker finishes by observing the train itself — the behavior under test);
  the ablation never weakens the completion anchor.

## Live-provider run log (manual, env-gated)

The live-provider e2e is excluded from CI and runs only when a real key is present:

```bash
KIAN_API_KEY=sk-... cargo test -p kian-cli --test live_provider -- --ignored --nocapture
```

- **2026-11-01 (C17)**: *not executed* — no `KIAN_API_KEY` was available in the working
  environment. The test is in place (`crates/kian-cli/tests/live_provider.rs`); the first
  executed run should be recorded here with its session state and cache report.

## Live lark-channel run log (manual, env-gated)

The C44 lark live smoke is excluded from CI and runs only when real Feishu app credentials are
present (`KIAN_LARK_APP_ID` absent → the test skips, it never fails). Receive additionally needs
`KIAN_LARK_POLL_CHAT=oc_xxx:topic`; send needs `KIAN_LARK_CHAT_ID`:

```bash
KIAN_LARK_APP_ID=cli_xxx KIAN_LARK_APP_SECRET=xxx \
KIAN_LARK_CHAT_ID=oc_xxx [KIAN_LARK_POLL_CHAT=oc_xxx:topic] \
cargo test -p kian-superagent --test lark_channel -- --ignored --nocapture
```

All unit/integration coverage runs against a scripted in-process HTTP mock with zero credentials
(`crates/kian-superagent/tests/lark_channel.rs`).

- **2026-11-01 (C44)**: *not executed* — no `KIAN_LARK_APP_ID` was available in the working
  environment. The test is in place; the first executed run should be recorded here with the
  sent `message_id` and the first received event id.

## Phase-1 acceptance checklist (S1–S3)

| Scenario | Card | Status | Proof |
|----------|------|--------|-------|
| S1 crash recovery (kill -9 at every drain stage, restart, no lost work) | C15 | **done** | `chaos_s1.rs` (six kill points × repeated runs; zombie-epoch writes rejected); API-level preview in `restart_serve.rs` |
| S2 steer/interrupt semantics + honesty | C16 | in flight | card C16 e2e; steer boundary + synthetic marker + `interrupted ≠ failed` |
| S3 cache-byte stability | C17 | **done** | `s3_cache_stability.rs`: consecutive requests share the byte prefix; only a `compact` breaks it and counts under the `compact` cause; `unexpected_prefix_change` stays zero; exposed via `GET /metrics` and `kian cache-stats` |

Unit-level underpinnings: A2 golden-bytes and A3 compact-prefix tests in `kian-core`
(`context.rs`), the frozen request-serialization golden hash in `kian-llm` (`FROZEN_CHAT_BODY_SHA256`).

## Phase 2 (super-agent bundle + BYOM endpoints)

Phase 2 turns the Phase-1 runtime into the super-agent reference architecture: event
subscriptions + channels, a resident Router judging work and dispatching Workers via Thread
delegation (evidence-gated completion), and BYOM execution endpoints — a `kiand` daemon the
server dials over KiandProto (TCP + JSON lines), with a server-side durable command queue
(at-least-once, command-id dedupe — [ADR-007](design/decisions/ADR-007-kiand-delivery-semantics.md))
and a heartbeat watchdog that admits offline health inputs to affected worker sessions.

### Build (S4 + S5)

```bash
cargo build --workspace   # 11 members: Phase-1 eight + kian-exec, kian-superagent, kiand
```

### kiand — the execution endpoint daemon (S5)

```bash
# terminal 1: the endpoint. Prints exactly one registration line on stdout:
#   kiand: endpoint kiand listening on 127.0.0.1:7401
cargo run -p kiand -- --listen 127.0.0.1:7401 --heartbeat-ms 1000
```

Flags > `KIAND_*` env > optional `./kiand.yaml` > defaults (`--help` lists them; unknown keys,
malformed values and zero/negative numbers refuse to start). `SIGTERM`/`SIGINT` drain in-flight
commands and exit 0.

### Serve with the super-agent assembly (S4 + S5)

```bash
# terminal 2: the control plane. Additive config; a Phase-1 kian.yaml still boots identically.
cat > kian.yaml <<'EOF'
listen: "127.0.0.1:7433"
storage:
  sqlite: "./kian.db"
agents:
  router:
    id: router
    version: 1
    model_slots:
      router:
        provider: my-provider
        model: gpt-4o-mini
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 1
    model_slots:
      main:
        provider: my-provider
        model: gpt-4o-mini
    tools: ["fs.read", "fs.write", "exec"]
superagent:
  router:
    session_id: "00000000-0000-0000-0000-000000000001"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: mock-im
      match: always
      route: router
  channels:
    mock-im:
      type: mock-im
endpoints:
  - id: dev-box
    addr: "127.0.0.1:7401"
    queue_timeout_ms: 30000
    offline_after_ms: 5000
providers:
  my-provider:
    base_url: "https://api.openai.com/v1"
    api_key_env: MY_PROVIDER_KEY
plugins: ["openai-compat", "local-tools", "superagent", "remote-exec"]
EOF
cargo run -p kian-cli --bin kian -- serve
```

Endpoint timing has a documented detection-bound relationship: the watchdog declares an endpoint
offline when `now - last_heartbeat > offline_after_ms`, polling the registry every
`clamp(offline_after_ms / 4, 50, 1000)` ms, and the bridge sweeps offline endpoints every 250 ms —
so offline detection lands within `offline_after_ms + poll + tick`. `offline_after_ms` must exceed
the endpoint's heartbeat cadence (`--heartbeat-ms`), and `queue_timeout_ms` is the caller's honest
wait bound: on expiry the tool result is an honest `Timeout` and the queue row is `abandoned`
(never a fake success).

### Demo

```bash
# S4: inject a mock-IM work request; the Router judges it, dispatches a Worker via Thread
# delegation, the Worker executes (evidence-gated) and the Router delivers the reply +
# structured report back onto the channel.
cargo run -p kian-cli --bin kian -- mock-channel-send mock-im --text "deploy please"

# S5: the Worker's `exec` tool runs on the kiand endpoint. Kill -9 the daemon while a command
# is queued: the watchdog admits a health input to the worker's session, the queue row parks
# durably, and a restarted kiand (same port) reconnects and drains it — or the queue timeout
# resolves honestly. Watch it live:
curl -s localhost:7433/metrics | jq .superagent   # evidence/subscription/channel/watchdog counters
```

The scripted, no-credentials versions of both scenarios run in CI: S4 in
`crates/kian-cli/tests/superagent_serve.rs` (C27/C28 assemblies over mock providers + the mock
channel) and S5 in `crates/kian-cli/tests/s5_byom_e2e.rs` (the real `kiand` binary as a child
process, paired with the assembled server).

## Phase-2 acceptance checklist (S4–S5)

| Scenario | Card | Status | Proof |
|----------|------|--------|-------|
| S4 super-agent demo (mock-IM → Router judges → Worker completes → ≤100-word reply + structured delivery) | C28 | **done** | `s4_superagent_demo.rs` (7 tests over the assembled serve, mock channel + scripted providers): `s4_router_judges_and_dispatches`, `s4_worker_completes_with_evidence`, `s4_reply_within_word_limit`, `s4_structured_delivery`, `s4_delegation_audit_export_replays`, `s4_readme_demo_cli_mock_channel_send_against_serve`, and the negative path `s4_gate_failed_worker_never_reaches_delivered_done_reply`; mechanics covered by `superagent_serve.rs` (C27 assembly: serve smoke, watchdog→health-input, startup order, Phase-1 compat) |
| S5 BYOM (cloud loop + kiand execution + watchdog detects endpoint offline) | C29 | **done** | `s5_byom_e2e.rs`: `s5_exec_on_kiand_round_trip` (exec → durable row → dial → real `tool/result` → completion), `s5_offline_queue_parks_durable` (kill -9 while queued → row parks, no fake result), `s5_watchdog_detects_offline_within_bound` (durable health input within `offline_after + poll + tick` + slack), `s5_reconnect_drains_queue` (restart same port → reconnect drains → result flows → completion strictly after the real result), `s5_honesty_across_offline_window` (queue timeout first → honest `Timeout`, row `abandoned`, completion only after the honest error) |

Phase-2 final four-gate run (card C29 closeout, 2026-11-01): `cargo build --workspace`,
`cargo test --workspace`, `cargo clippy --workspace -- -D warnings`, `cargo fmt --all -- --check`
— all green; recorded per-card in the kanban handoffs.

## Phase 3 (article-sync: recall, admission economics, real channels)

Phase 3 completes the article-sync backlog on the same 11-crate workspace — no new crates, no
new dependencies in the core, and exactly one additive event kind this round (`input/spilled`;
the durable set is now 20). DESIGN §4.1/§4.5 carry the full semantics; this section is the
operator surface. Everything is config-first (Footprint Ladder level 1) under the additive
`superagent:` section, and every omitted block preserves the previous round's behavior
byte-identically.

What landed, in dependency order:

- **Inline/Spill admission shaping** (C31–C33): an oversized channel payload admits as a bounded
  Inline view (excerpt + association ids + a continuation handle) in model context, while the
  verbatim raw is appended to the same stream as a non-context-bearing `input/spilled` sibling
  in one atomic consumption write. Shaping happens exactly once, at admission, before the input
  row exists — committed context is never rewritten (constraint 3).
- **The recall toolpack — Chain of Recall** (C34–C35): read-only `recall_event` /
  `recall_neighbors` / `recall_thread` tools over the bundle's `RecallPort`. Every output is
  itself Inline/Spill-shaped, so multi-hop recall composes (each hop returns bounded views plus
  next-hop ids). Router global observability rides the event stream via recall, never Router
  context.
- **Two-level admission filtering** (C36): a `classes:` table (`candidate | explicit-only |
  ignore`, keyed by `(channel, topic?, sender?)`, most-specific-wins) evaluated before the rule
  set and before any model call — drops are audit lines + counters, not rows. The Slow Path is
  the Router's judge prompt: the natural-language policy extension point, editable without code.
- **Async `start()`/`stop()` tools** (C37,
  [ADR-008](design/decisions/ADR-008-async-tool-completion-admitted-input.md)): `start` acks
  immediately with a job id; the completion returns to the originating session as an admitted
  input (`{"kian":{"type":"async_result",…}}`) durable before its wake and promoted at the next
  safe boundary; `stop` cancels honestly — the cancelled completion still lands, labeled
  `cancelled`. Default off (the article's own "not default-on").
- **Many-to-many passthrough fan-out** (C38): a topic maps to the *set* of workers that served
  it; one channel event admits one durable row per subscriber (idempotent per
  `(session, event_id)`); only an empty set falls back to the Router.
- **Speculation** (C39): `spawn_worker` with `speculative: true` — an additive, model- and
  audit-visible marker in the delegation wrapper. The Router profile's
  `speculation: {max_outstanding, label}` enforces a stream-derived cap with honest tool errors
  (the count survives Router restarts because it is a projection, not memory); a discarded
  speculation audits every token spent and delivers nothing. The silent-repro policy ships as an
  editable prompt template.
- **Watchdog Session v2** (C40): a model-in-the-loop progress judge. Ticks come from a
  `type: timer` channel routed to the fixed watchdog session via `route: {session: <uuid>}`;
  the judge's tool surface is `list_threads` / `read_progress` (over `RecallPort`),
  `intervene` (admits a steer row durable-before-wake) and `escalate` (an IM ≤100-word policy
  reply). Every tick — including a healthy no-op — is audited in the watchdog session's own
  stream. The v1 endpoint heartbeat watchdog (S5) is unchanged.
- **Batching metrics** (C41): the `superagent.router_batching` series on `/metrics` (below).
- **Untrusted-input marking** (C43): `"untrusted": true` inside the provenance wrapper for
  configured channels; real channels default untrusted. Honest labeling only — the marker
  survives every recall hop; decisions stay with the classes (C36) and the judge.
- **The real lark adapter** (C44): Feishu IM receive (chat-history poll) and send (IM API) over
  the open APIs, env-only credentials, default-untrusted.
- **The §7.1 ablation benchmark** (C42):
  [Ablation benchmark: passthrough vs. Router judgment](#ablation-benchmark-passthrough-vs-router-judgment-71-c42),
  above.

### Recall + shaping config

```yaml
superagent:
  shaping:                      # admission-time Inline/Spill (C33); absent = legacy wrapper
    inline_cap_bytes: 2048      # serialized-raw byte cap; absent = infinity
    first_chars: 50             # Inline excerpt length (default 50)
    neighbor_fields: ["pr", "branch"]   # payload fields inlined as association ids
  recall:                       # the toolpack (C35); defaults shown
    router: true                # mounted on the Router profile (default)
    worker: false               # opt-in per worker template
    output_cap_bytes: 8192      # verbatim bound for one recall output; above = Inline view
    neighbor_window: 3          # seq window either side of a recall_neighbors anchor
```

The model calls `recall_event(event_id)` to surface a spilled raw, `recall_neighbors(event_id)`
for the same-topic locality view, and `recall_thread(thread_id)` for a thread's task, status and
final summary. Unknown ids are honest tool errors, never silence.

### Async tools usage

```yaml
superagent:
  async_tools: true   # mounts `start`/`stop` on the Router profile + worker template
endpoints:            # required: the pair routes through the C20 durable exec queue
  - id: dev-box
    addr: "127.0.0.1:7401"
```

`start` returns `{"status":"started","job_id":…}` immediately — the session's drain is never
blocked by the job (a queued steer interleaves in the same session, the S2 path). When the job
settles, the bridge admits its result to the originating session's inbox as a provenance-wrapped
`async_result` input (durable before the wake); if that session already projects terminal, the
completion is dropped with an audit line — an honest no-op, never a zombie row. A server restart
re-drives `pending`/`inflight` jobs; job ids dedupe across recovery.

### Watchdog v2 timer config

```yaml
superagent:
  watchdog:
    session_id: "00000000-0000-0000-0000-00000000d00d"   # fixed judge session; ≠ Router session
    agent: watchdog            # a worker-template agent (own `main` slot); tools mounted by assembly
  channels:
    wd-timer:
      type: timer
      timer:
        interval_ms: 300000    # tick cadence (must be > 0)
        topic: watchdog        # the topic every tick carries (the convention rules match on)
        emit_on_start: true    # one immediate first check after (re)start
  subscriptions:
    - channel: wd-timer
      match: {topic: watchdog}
      route: {session: "00000000-0000-0000-0000-00000000d00d"}
```

Ticks missed while the server is down simply never happened — the stream honestly shows no check
ran. Ticks dedupe by `event_id` (`tick-<n>`) across restarts, so a re-delivered tick is a no-op.

### Lark channel setup (env-only tokens)

```yaml
superagent:
  channels:
    lark-im:
      type: lark
      lark:
        topics:                # chat id → routing topic; the keys are exactly the polled chats
          "oc_xxxxxxxxxxxxxxxx": "team-im"
        poll_interval_ms: 2000 # optional (default 2000); api_base overridable for private deploys
```

Credentials never live in `kian.yaml`: `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` come from the
environment and absent/blank values refuse to start (fail-closed). Lark channels are untrusted by
default (C43/C44). All unit/integration coverage runs against a scripted in-process Feishu mock
with zero credentials; the live smoke and its run log:
[Live lark-channel run log](#live-lark-channel-run-log-manual-env-gated).

### Batching metrics (C41)

`GET /metrics` additionally carries the `superagent.router_batching` snapshot, recorded by the
Router driver around each judge turn: `judge_turns`, `inputs_consumed` (batch size),
`turn_duration_ms_total` / `mean_turn_duration_ms` (per-request wall duration),
`amortized_ms_per_input` (duration/inputs — the per-decision amortized latency), the bounded
`batch_histogram` (buckets 1…128 plus overflow), and `last_turn`. Pure observability: with the
recorder absent, the driver performs zero stream reads and zero clock touches — identical
provider requests and identical event streams (test-asserted).

### Ablation run commands (C42)

```bash
cargo test -p kian-superagent --test ablation_passthrough                       # CI-sized
cargo test -p kian-superagent --test ablation_passthrough -- --ignored --nocapture   # heavy, local
```

Design and the dated results note:
[Ablation benchmark: passthrough vs. Router judgment](#ablation-benchmark-passthrough-vs-router-judgment-71-c42).

## Phase-3 acceptance checklist (S4-extended legs + S7)

The S4 scenario's extended legs and the S7 record. Scenario wording lives in
[SPEC §2.5](docs/SPEC.md); proof is by the named tests, all zero-credentials
(`NO_PROXY=127.0.0.1,localhost` set by the suites themselves).

| Leg | Card | Status | Proof |
|-----|------|--------|-------|
| S4-extended: recall (over-cap event → Inline view + `input/spilled` pair; `recall_event` round-trip; two-hop chain trigger → `recall_thread` → worker summary; capped outputs; honest unknown-id errors; stable-ordered tool schemas) | C31–C35 | **done** | `crates/kian-superagent/tests/recall_toolpack.rs` (`over_cap_event_recall_event_round_trip_returns_verbatim_raw_as_a_tail_tool_result`, `two_hop_chain_trigger_to_recall_thread_reaches_the_worker_summary`, `recall_of_an_unknown_id_is_an_honest_error_result_never_silence`, `recall_outputs_are_capped_no_unbounded_raw_reinjection`, `recall_neighbors_returns_the_same_topic_locality_view`, `recall_tool_schemas_are_stable_ordered_golden`); `crates/kian-cli/tests/recall_port.rs`, `crates/kian-cli/tests/spill_replay.rs` |
| S4-extended: async (`start` acks fast while a steer interleaves in the same session; completion lands durable-before-wake and is consumed at the next drain; `stop` mid-run → honest `cancelled` end-to-end; restart re-drives jobs) | C37 | **done** | `crates/kian-cli/tests/async_tools_e2e.rs` (`start_returns_fast_steer_interleaves_and_completion_lands`, `stop_mid_run_labels_cancelled_end_to_end`); `crates/kian-exec/tests/async_tools.rs`; `crates/kian-cli/tests/async_bridge.rs` (bridge honesty: terminal-session drop-with-audit, no zombie rows, labelled cancelled) |
| S4-extended: watchdog (stuck worker → tick → judge → `intervene` steer row → worker consumes it at the safe boundary and completes; healthy worker → auditable no-op; bootstrapped watchdog replays) | C40 | **done** | `crates/kian-superagent/tests/watchdog_v2.rs` (`stuck_worker_tick_judges_intervenes_and_the_worker_consumes_the_steer`, `healthy_worker_tick_is_an_auditable_no_op`, `bootstrapped_watchdog_still_judges_ticks`) |
| S7 audit export/replay (byte-identical replay package) | C14 + C28 (recorded at closeout C45) | **demonstrated** | `kian session export`/re-import byte-identical (C14); `s4_delegation_audit_export_replays` in `crates/kian-cli/tests/s4_superagent_demo.rs` (C28); both-streams replay assertions — no new work this round |

Phase-3 final four-gate run (card C45 closeout, 2026-11-01): `cargo build --workspace`,
`cargo test --workspace` (with `NO_PROXY=127.0.0.1,localhost`), `cargo clippy --workspace --
-D warnings`, `cargo fmt --all -- --check` — all green; recorded per-card in the kanban handoffs.
