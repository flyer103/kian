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

## Live-provider run log (manual, env-gated)

The live-provider e2e is excluded from CI and runs only when a real key is present:

```bash
KIAN_API_KEY=sk-... cargo test -p kian-cli --test live_provider -- --ignored --nocapture
```

- **2026-11-01 (C17)**: *not executed* — no `KIAN_API_KEY` was available in the working
  environment. The test is in place (`crates/kian-cli/tests/live_provider.rs`); the first
  executed run should be recorded here with its session state and cache report.

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
