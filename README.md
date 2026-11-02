# Kian — the durable execution runtime for agents

> **One-liner: the Temporal of agents — an open-source, model-neutral, self-hostable durable
> execution runtime that makes agent tasks killable, resumable, migratable and auditable, with the
> super-agent reference architecture as a first-class workload.**

A single-session agent dies with its process, cannot be redirected at a safe boundary, cannot prove
what it did, and re-pays for its own context on every turn. Kian makes the **Session** the durable
entity, the **event stream** the only truth, and each execution interval (a **drain**) disposable —
so a crash, a deploy or a machine move costs a restart, not the work. Start with
[docs/overview.md](docs/overview.md): the problem, the model, the crate map and what has actually
been demonstrated.

- Implementation language: **Rust** (13 crates, edition 2024)
- Storage: **sqlite** by default (single machine); **postgres** for multi-replica deployments
  (env-gated)
- Design inputs: frozen local snapshots in [a vendored archive/](a vendored archive/) (archived
  2026-10-31 and 2026-11-02: the WeChat super-agent article, dsh/cordis docs, two internal design notes, the
  dsh-superagent-prototype); structured
  synthesis in [the design notes](the design notes)

## Docs map

| Document | Type | Contents |
|----------|------|----------|
| [docs/overview.md](docs/overview.md) | **Start here** | The problem, the execution model, architecture, crate map, S1–S8 proofs, design-input map |
| [docs/SPEC.md](docs/SPEC.md) | Spec (WHAT) | Positioning, user interfaces, scope, milestones |
| [design/DESIGN.md](design/DESIGN.md) | Design (HOW) | Execution model, session state machine, recovery, event schema, plugin system, crate map, super-agent bundle |
| [design/decisions/](design/decisions/) | Decisions (WHY) | ADR-001 … ADR-011: each decision with its alternatives and trade-offs |
| [the design notes](the design notes) | Research | Structured digest of the five design inputs |
| [docs/runbook-dsh.md](docs/runbook-dsh.md) | Runbook | Operating the S6 dsh bridge: config, the live positive via the contract-v0 peer (`contrib/dsh-v0-peer/`), and the C57 stock-dsh contract survey |
| [docs/runbook-postgres.md](docs/runbook-postgres.md) | Runbook | Operating postgres mode: `KIAN_PG_URL`, `drain.holder`, migrations, the S8 live replay |
| [a vendored archive/](a vendored archive/) | Archive | Frozen local snapshots of the design inputs (read-only) |
| [CHANGELOG.md](CHANGELOG.md) | History | Phase-dated record of what landed, and what is env-gated |

Docs-map consistency is enforced by the `docs_map` test in `crates/kian-cli/tests/docs_map.rs`: it
checks that every relative link in `README.md`, `AGENTS.md`, `docs/SPEC.md`, `design/DESIGN.md`,
`the design notes` and the ADRs resolves. The two runbooks, `docs/overview.md` and
`CHANGELOG.md` are not yet in that enforced set — their links are checked by hand for now (extending
`AUTHORED` is tracked as a follow-up, not done here).

## Status

| Property | Value |
|----------|-------|
| Crates | 13 (`cargo build --workspace`) |
| Tests | 751 at the Phase-4 closeout (more as later cards land regression tests), all default-green with zero credentials (`cargo test --workspace`) |
| Event kinds | 20, append-only (additions only, never renamed) |
| ADRs | 11 |
| Acceptance | S1–S5 and S7 default-green; S6 fixture-proven in CI, its live leg executed against dsh 0.2.1-rc.5 with an honest negative (S6-live still open); S8 env-gated (`KIAN_PG_URL`) |

Phase history, one line each (details in the [phase sections](#phase-1-acceptance-checklist-s1s3)
below):

- **Phase 1 — MVP, durable sessions**: event store on sqlite with `expectedSeq` CAS + epoch fencing,
  Admit → Wake → Drain → Promote, three-layer recovery, control-plane API and CLI. Scenarios S1–S3.
- **Phase 2 — super-agent bundle**: Router/Worker composition over the core (never in it), event
  subscriptions + channels, the evidence gate, BYOM endpoint daemons. Scenarios S4–S5.
- **Phase 3 — article-sync**: Inline/Spill shaping + Chain of Recall, two-level admission filtering,
  async tools, passthrough fan-out, speculation, Watchdog v2, batching metrics, untrusted marking,
  the lark channel adapter. S4-extended legs + S7.
- **Phase 4 — hosted foundation**: postgres dual backend and multi-replica lease/fencing, the frozen
  wire-only `kian-sdk`, the GitHub channel with a verified webhook receiver, the dsh bridge.
  Scenarios S6, S8.

## Quickstart

Prerequisites: Rust stable 1.85+ (`edition 2024`), plus `curl` and `jq`. Nothing else — sqlite is
embedded and a single `kian serve` process is the whole system.

Every command and every output block below was executed against this tree on 2026-11-01 (macOS,
rustc 1.98.1). The provider was a local OpenAI-compatible SSE stub that answers
`hello from a scripted provider`, so the run needed no credentials and no network; point
`base_url`/`model` at any real OpenAI-compatible endpoint and the same session runs against a model.

### 1. Build

```bash
cargo build --workspace          # 13 crates
```

### 2. Write `kian.yaml`

```yaml
listen: "127.0.0.1:7433"
storage:
  sqlite: "./kian.db"
agent:
  id: my-agent
  model_slots:
    main:
      provider: local
      model: gpt-4o-mini
providers:
  local:
    base_url: "http://127.0.0.1:7411/v1"   # any OpenAI-compatible endpoint
    api_key_env: KIAN_LOCAL_KEY            # names the env var; the key never lives in the file
drain:
  max_iterations: 32
  lease_ttl_ms: 1500             # default 30000; shortened so a takeover is visible in seconds
  heartbeat_interval_ms: 300     # default 10000; must stay well under the lease TTL
recovery:                        # required: the policy table never defaults silently
  rules:
    - class: rate_limited
      action: {backoff_retry: {delay_ms: 500}}
    - class: unknown
      action: fail
```

Two rules are fail-closed and worth knowing before you edit anything: a missing `recovery:` section
refuses to start (a policy table is never implied), and a provider whose `api_key_env` variable is
unset refuses to start (secrets are never read from the file). A malformed config never degrades
into a running server.

### 3. Serve

```bash
export KIAN_LOCAL_KEY=sk-...        # any non-empty value; the stub ignores it
cargo run -p kian-cli --bin kian -- serve --config kian.yaml
```

The process stays in the foreground on `127.0.0.1:7433`. Prefix local commands with
`NO_PROXY=127.0.0.1,localhost` on macOS — the system proxy otherwise intercepts localhost.

### 4. Run a session

```bash
SID=$(curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"create_session"}' | jq -r .result.session)
echo "$SID"      # 36e8cedd-693f-414e-bcf3-21f9f737f8d2

curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"submit\",\"params\":{\"session\":\"$SID\",\"content\":\"hello kian\"}}"
# {"id":2,"jsonrpc":"2.0","result":{"deduplicated":false}}

curl -sN localhost:7433/sessions/$SID/events      # live SSE stream; ends after session/ended
```

The stream *is* the audit record. Abridged below — session ids shortened to `36e8cedd-…` and the
full `step/request` body elided; every other value is verbatim:

```
event: kian
data: {"seq":1,"session":"36e8cedd-…","epoch":0,"ts":1793541036163,"kind":"session/created","bytes":{},"version":0}
data: {"seq":2,"session":"36e8cedd-…","epoch":1,"ts":1793541036175,"kind":"drain/claimed","bytes":{"epoch":1,"holder":"kian-serve"},"version":0}
data: {"seq":3,"session":"36e8cedd-…","epoch":1,"ts":1793541036175,"kind":"input/admitted","bytes":{"content":"hello kian","role":"user"},"version":0}
data: {"seq":4,…,"kind":"slot/binding","bytes":{"slot":"main","spec":{"fallback":[],"model":"gpt-4o-mini","provider":"local"}}}
data: {"seq":5,…,"kind":"turn/start","bytes":{"iteration":0}}
data: {"seq":6,…,"kind":"step/request","bytes":{"body":{…,"messages":[{"content":"hello kian","role":"user"}],…},"iteration":0,"model":"gpt-4o-mini","provider":"local","slot":"main"}}
data: {"seq":7,"session":"36e8cedd-…","epoch":1,"ts":1793541040187,"kind":"assistant/message","bytes":{"content":{"text":"hello from a scripted provider"},"role":"assistant"},"version":0}
data: {"seq":8,"session":"36e8cedd-…","epoch":1,"ts":1793541040188,"kind":"turn/end","bytes":{"iteration":0,"outcome":"final_answer"},"version":0}
data: {"seq":9,"session":"36e8cedd-…","epoch":1,"ts":1793541040189,"kind":"session/ended","reason":"completed","bytes":{},"version":0}
event: done
data: ended
```

Note `step/request` at seq 6: the exact request bytes are persisted **before** the call flies, which
is what makes replay byte-identical. Read state back from the projection, never from a column:

```bash
curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"projection\",\"params\":{\"session\":\"$SID\"}}"
# {"id":3,"jsonrpc":"2.0","result":{"state":"completed"}}

target/debug/kian session inspect "$SID" --config kian.yaml
```

```
session             36e8cedd-693f-414e-bcf3-21f9f737f8d2
state               completed
events              9 (next seq 10, epoch 1)
last event ts       1793541040189
inputs admitted     1
drain claims        1
turns               1
requests            1
assistant messages  1
tool calls          0
tool results        0
compactions         0
budget resets       0
pending inputs      0
```

### 5. Wake a session (operator action)

```bash
IDLE=$(curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"create_session"}' | jq -r .result.session)
target/debug/kian wake-now "$IDLE" --config kian.yaml
```

```
session             b8c6cdcf-166f-4e78-b515-89c9a7b2a730
outcome             started (a drain was spawned)
```

A wake is advisory: it spawns a drain that finds an empty inbox and exits, and the session stays
`idle` until an input is admitted. Waking a terminal session is refused honestly rather than silently
ignored:

```
$ target/debug/kian wake-now "$SID" --config kian.yaml
kian: wake-now failed — wake-now: rpc error -32003: session 36e8cedd-… is terminal
```

### 6. Audit it: replay, export, cache report

```bash
target/debug/kian session replay "$SID" --config kian.yaml
target/debug/kian session export "$SID" --config kian.yaml --out audit.jsonl
target/debug/kian cache-stats --config kian.yaml --json
```

```
session             36e8cedd-693f-414e-bcf3-21f9f737f8d2
events              9
derived state       "completed"
live state          (not fetched; pass --url to diff)
invariants          ok
model context       1 of 1 requests byte-identical   <- A2: replay derives the same bytes
```
```
exported 9 events (seq 1..=9) to audit.jsonl
```
```json
{
  "sessions": 2,
  "requests": 1,
  "compared_pairs": 0,
  "prefix_stable_pairs": 0,
  "compacts": 0,
  "invalidation_causes": {},
  "parse_errors": 0
}
```

The cache report is the S3 observable: per store, consecutive request pairs compared, how many
shared a byte-identical message prefix (the provider-independent prefix-cache proxy), and why a
prefix broke. `compact` is the **only** counted invalidation cause — any other prefix change is
reported as `unexpected_prefix_change` and is a bug. One request in this session, so there are no
pairs to compare yet.

`GET /metrics` carries the engine counters and the same cache report:

```bash
curl -s localhost:7433/metrics | jq '{engine: .engine, pending_wake_depth, cache: .cache}'
```

```json
{
  "engine": {
    "drain_depth": 0,
    "drain_duration_ms_total": 4016,
    "drains_started": 2,
    "end_writes": 1,
    "state_transitions": 4,
    "wakes_folded": 0,
    "wakes_registered": 0
  },
  "pending_wake_depth": 0,
  "cache": {
    "compacts": 0,
    "compared_pairs": 0,
    "invalidation_causes": {},
    "parse_errors": 0,
    "prefix_stable_pairs": 0,
    "requests": 1,
    "sessions": 2
  }
}
```

(`drains_started` is 2 because the idempotent wake in step 5 spawned a second, empty drain; the engine
counters are in-memory and restart with the process, while the cache report is a pure projection over
the durable stream.)

### 7. Prove durability: `kill -9` mid-turn, restart, no lost work

This is the whole pitch in one run. Submit an input, `kill -9` the server while the provider call is
in flight, then restart on the same sqlite file:

```bash
S2=$(curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":5,"method":"create_session"}' | jq -r .result.session)
curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"submit\",\"params\":{\"session\":\"$S2\",\"content\":\"run the long job\"}}"

sleep 1
kill -9 <pid-of-kian-serve>
target/debug/kian session inspect "$S2" --config kian.yaml
```

```
session             62795e1c-708d-4b2b-802d-ebeba1151da2
state               running
events              6 (next seq 7, epoch 1)
last event ts       1793541042379
inputs admitted     1
drain claims        1
turns               1
requests            1               <- the request bytes are durable; the turn is not finished
assistant messages  0
```

The session is still `running`: no end write happened, and the dead drain's lease is still unexpired
(leases are liveness — their job is only to make this session *detectable* as recoverable):

```
$ target/debug/kian lease inspect --config kian.yaml
SESSION                                  EPOCH     HOLDER   TTL(ms) LAST-HEARTBEAT  EXPIRED
36e8cedd-693f-414e-bcf3-21f9f737f8d2      1 kian-serve      1500  1793541040078      yes
b8c6cdcf-166f-4e78-b515-89c9a7b2a730      1 kian-serve      1500  1793541042313      yes
62795e1c-708d-4b2b-802d-ebeba1151da2      1 kian-serve      1500  1793541043280       no
```

Restart `kian serve` on the same `kian.db`; the recovery sweep takes over the expired lease and a new
drain continues from the stream:

```bash
target/debug/kian session inspect "$S2" --config kian.yaml    # a few seconds later
```

```
session             62795e1c-708d-4b2b-802d-ebeba1151da2
state               completed
events              12 (next seq 13, epoch 2)
inputs admitted     1
drain claims        2
turns               2
requests            2
assistant messages  1
```

And the stream shows the takeover without rewriting a single earlier event:

```
$ curl -sN localhost:7433/sessions/$S2/events | sed -n 's/^data: //p' | grep -v '^ended$' | jq -c '{seq, epoch, kind}'
{"seq":1,"epoch":0,"kind":"session/created"}
{"seq":2,"epoch":1,"kind":"drain/claimed"}      <- the drain that died
{"seq":3,"epoch":1,"kind":"input/admitted"}
{"seq":4,"epoch":1,"kind":"slot/binding"}
{"seq":5,"epoch":1,"kind":"turn/start"}
{"seq":6,"epoch":1,"kind":"step/request"}
{"seq":7,"epoch":2,"kind":"drain/claimed"}      <- takeover claim, epoch 2
{"seq":8,"epoch":2,"kind":"turn/start"}
{"seq":9,"epoch":2,"kind":"step/request"}
{"seq":10,"epoch":2,"kind":"assistant/message"}
{"seq":11,"epoch":2,"kind":"turn/end"}
{"seq":12,"epoch":2,"kind":"session/ended"}
```

The retried request derived a byte-identical message prefix from the stream — nothing in the earlier
history was rewritten — which the cache report (now one comparable pair) confirms:

```bash
target/debug/kian cache-stats --config kian.yaml --json
```

```json
{
  "sessions": 3,
  "requests": 3,
  "compared_pairs": 1,
  "prefix_stable_pairs": 1,
  "compacts": 0,
  "invalidation_causes": {},
  "parse_errors": 0
}
```

The process died; the work did not. Safety came from the **epoch** (every drain write carries it and
the store rejects stale-epoch writes), not from the lease — a partitioned-but-alive old holder would
be fenced out the same way. The automated version of this run, with six kill points and repeated
iterations, is `crates/kian-cli/tests/chaos_s1.rs` (scenario S1).

### Test

```bash
cargo test --workspace
```

Everything — unit, wire-level e2e against a scripted mock provider, and the kill-and-restart
(chaos-style) suite — runs in the default workspace test invocation. Local e2e sets
`NO_PROXY=127.0.0.1,localhost` itself (the macOS system proxy intercepts localhost). Suites whose
environment is absent skip with a log line and never fail; see the
[environment contract](#environment-contract).

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

## Live dsh-bridge run log (manual, env-gated)

The C54 S6 live smoke is excluded from CI and runs only when a real dsh install is present
(`KIAN_DSH_BIN` absent → the test skips, it never fails):

```bash
KIAN_DSH_BIN=/path/to/dsh cargo test -p kian-dsh --test live -- --nocapture
```

The mounted bridge is config-gated and default-off (`superagent.harness.dsh` in `kian.yaml`;
the command may come from config or `KIAN_DSH_BIN` — neither refuses to start, fail-closed).
All default-CI coverage runs against the C53 scripted fixture peer with zero Node/dsh present:
the driver suite (`crates/kian-dsh/tests/driver_bridge.rs`) and the mounted e2e — spawn/steer/
complete, kill-9 takeover continuation, and honest child-crash failure
(`crates/kian-cli/tests/dsh_bridge.rs`). Operational detail: [docs/runbook-dsh.md](docs/runbook-dsh.md).

- **2026-11-01 (C54)**: *not executed* — no dsh install was available in the working
  environment. The bridge, fixture-driven CI proof and the smoke are in place. (Superseded by the
  C57 entry below, which did execute the smoke against a real dsh install.)
- **2026-11-01 (C57)**: *executed* against real dsh **0.2.1-rc.5** (`dsh --profile sdk`).
  Outcome: honest `SoftFailed { reason: "peer never sent ready within 30s" }` — the stock
  dsh SDK app is a JSON-RPC server (client-initiated `initialize`, `session.event`
  notifications, no steer/cancel methods, attempt events only for unsettled attempts) and
  does not satisfy the C53/ADR-010 v0 contract, which ADR-010 scopes as the bar for an
  ecosystem-owned dsh-side plugin (outside this repo). The kind vocabulary matches real
  `dsh-session` verbatim; the gap is transport + pre-flight bytes + steer/interrupt, and the
  bridge stopped at the documented negative result exactly as ADR-010 §3 requires — never a
  fabricated completion. The parallel run also surfaced and fixed two real bugs with
  deterministic regressions: same-millisecond inbox ordering (store migration
  `v2_inbox_admit_order`, both backends, live-verified on PG 17) and an idle-boundary
  wake-loss deadlock in the `kian-dsh` driver (bounded `IDLE_INPUT_RECHECK`). All fixture
  suites green with the gate present; the mounted e2e then ran 300 consecutive clean runs.
  Full survey, evidence and re-run commands: [docs/runbook-dsh.md](docs/runbook-dsh.md).
  S6-live stays open until a contract-v0 dsh-side peer (or an ecosystem plugin) exists and
  the smoke settles green.
- **2026-11-02 (C59)**: *executed, positive* — `contrib/dsh-v0-peer/dsh-v0-peer.mjs`
  (Node ESM, zero npm deps) adapts stock `dsh --profile sdk` JSON-RPC onto bridge
  contract v0: immediate `ready`, `session.event` → v0 session-event frames, `input` →
  `session/prompt`, interrupt → honest child kill (the SDK has no cancel), pre-flight
  request bytes synthesized from the in-step `request/header` + model-visible surface.
  Live run with `KIAN_DSH_BIN=$PWD/contrib/dsh-v0-peer/dsh-v0-peer.mjs KIAN_DSH_PEER=1`
  and a real `DEEPSEEK_API_KEY`: both live tests green — the smoke `TurnDone → Settled`,
  the peer positive appending `[TurnStart, InputAdmitted, StepRequest,
  AssistantMessage, TurnEnd]` with model reply "pong" (verified twice, ~4 s). Observed
  SDK vocabulary, divergences from ADR-010's assumed payloads, and the exact commands:
  [contrib/dsh-v0-peer/README.md](contrib/dsh-v0-peer/README.md) and
  [docs/runbook-dsh.md](docs/runbook-dsh.md). **S6-live positive closed.**

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

## Phase-1 acceptance checklist (S1–S3)

| Scenario | Card | Status | Proof |
|----------|------|--------|-------|
| S1 crash recovery (kill -9 at every drain stage, restart, no lost work) | C15 | **done** | `chaos_s1.rs` (six kill points × repeated runs; zombie-epoch writes rejected); API-level preview in `restart_serve.rs` |
| S2 steer/interrupt semantics + honesty | C16 | **done** | `s2_steer_interrupt.rs`: `steer_lands_at_next_safe_boundary_with_strippable_synthetic_marker` (no input lands inside an in-flight provider turn; the synthetic marker is strippable from durable context), `interrupt_mid_tool_settles_on_abort_and_resumes_honestly` (interrupt ≠ failed), `interrupt_of_idle_and_completed_sessions_is_a_noop` |
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
cargo build --workspace   # 13 members: Phase 1's eight (kian-core, kian-store, kian-orchestrator,
                          # kian-llm, kian-loop, kian-plugin, kian-endpoint, kian-cli) plus
                          # kian-exec, kian-superagent, kiand, then kian-sdk, kian-dsh
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
agent:                       # the single-agent definition Phase 1 used (always required)
  id: default
  model_slots:
    main:
      provider: my-provider
      model: gpt-4o-mini
  tools: ["fs_read"]
agents:                      # the bundle's named agent templates (additive)
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
    tools: ["fs_read", "fs_write", "exec"]
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
drain:
  max_iterations: 32
recovery:                    # required, as in Phase 1 (no silent policy defaults)
  rules:
    - class: rate_limited
      action: {backoff_retry: {delay_ms: 500}}
    - class: unknown
      action: fail
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
| S4 super-agent demo (mock-IM → Router judges → Worker completes → ≤100-word reply + structured delivery) | C28 | **done** | `s4_superagent_demo.rs` (8 tests over the assembled serve, mock channel + scripted providers): `s4_router_judges_and_dispatches`, `s4_worker_completes_with_evidence`, `s4_reply_within_word_limit`, `s4_structured_delivery`, `s4_delegation_audit_export_replays`, `s4_readme_demo_cli_mock_channel_send_against_serve`, `s4_shaping_config_spills_over_cap_channel_event_on_assembled_server`, and the negative path `s4_gate_failed_worker_never_reaches_delivered_done_reply`; mechanics covered by `superagent_serve.rs` (C27 assembly: serve smoke, watchdog→health-input, startup order, Phase-1 compat) |
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

## Phase 4 (hosted foundation: postgres, multi-replica, SDK freeze, ecosystem surfaces)

Phase 4 (round 4, cards C46–C54, closeout C55) lands the hosted substrate on the now
**13-crate** workspace: the postgres dual-backend store with multi-replica lease/fencing (S8,
[ADR-009](design/decisions/ADR-009-multi-replica-claim-admission.md)), the frozen wire-only
`kian-sdk` (C46), the GitHub channel + verified webhook receiver
([ADR-011](design/decisions/ADR-011-inbound-webhook-plane.md)), and the S6 dsh bridge
(`kian-dsh`, [ADR-010](design/decisions/ADR-010-dsh-session-mapping.md)). **Zero new event
kinds this round** — the durable set stays 20; the dsh mapping selects from existing kinds
only. sqlite stays the default backend; every omitted config section is byte-identical
previous-round behavior.

### Environment contract

Default CI needs none of these — every gated suite **skips with a log line, never fails**:

| Env var | Used by | Absent behavior |
|---------|---------|-----------------|
| `KIAN_PG_URL` | `kian-store`/`kian-exec` pg suites, `kian-cli` `postgres_serve`/`postgres_s8` | skip with a log line |
| `KIAN_DSH_BIN` | the dsh live smoke; also the default command source for a configured `superagent.harness.dsh` | smoke skips; a mounted harness without `command` or the env var refuses to start (fail-closed) |
| `KIAN_GITHUB_TOKEN` | github channel **send** | `send: true` configured → refuses to start (receive-only needs none) |
| `KIAN_GITHUB_WEBHOOK_SECRET` | webhook receiver HMAC verification | a `type: github` channel mounts the route → refuses to start |
| `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` / `KIAN_LARK_CHAT_ID` / `KIAN_LARK_POLL_CHAT` | lark live smoke | skip |
| `KIAN_API_KEY` | live-provider smoke | skip |

Secrets and URLs are **env-only, never a config value** (the C13 secrets rule); provider keys
additionally follow each provider's `api_key_env` name from `kian.yaml`.

### Postgres setup (dual-backend store)

`storage.event_store: postgres` switches the event store, inbox and exec tables
(`endpoint_registry`, `exec_queue`, `async_jobs`) to one postgres database — one server, one
database, one shared pool. The URL is env-only (`url_env` *names* the variable; the value may
carry a password but never lives in the file), and postgres mode **requires an explicit unique
`drain.holder`** — the shared default refuses to start, because two replicas sharing a holder
would defeat same-holder claim admission (C47/ADR-009). Both rules fail closed at startup.

A local postgres via docker (documented recipe; not a CI gate):

```bash
docker run -d --name kian-pg -e POSTGRES_PASSWORD=kian -p 5432:5432 postgres:16
docker exec kian-pg createdb -U postgres kian
export KIAN_PG_URL="postgres://postgres:***@127.0.0.1:5432/kian"
```

Full local runbook — Homebrew/dedicated `createdb` setup, the docker alternative on postgres 17,
per-suite commands, and exactly what the S8 kill -9 e2e proves:
[docs/runbook-postgres.md](docs/runbook-postgres.md).

```yaml
# kian.yaml (postgres mode)
listen: "127.0.0.1:7433"
storage:
  event_store: postgres
  postgres:
    url_env: KIAN_PG_URL      # names the env var holding the URL
drain:
  holder: replica-a           # explicit + unique per replica (required under postgres)
```

The env-gated postgres suites then run for real (migrations, CAS fences, batch atomicity,
claim admission, export parity, exec park/re-drive, the assembled serve, and S8 below):

```bash
KIAN_PG_URL=postgres://postgres:kian@127.0.0.1:5432/kian \
NO_PROXY=127.0.0.1,localhost cargo test --workspace
```

Export parity is the compat golden: the same event stream exported from sqlite and from
postgres produces byte-identical JSONL (S7's shape holds across backends).

### Multi-replica and S8

The semantics (C47 + C52, [ADR-009](design/decisions/ADR-009-multi-replica-claim-admission.md)):

- **Conditional claim admission** — a claim lands only when no claims row exists, the lease has
  expired, or the holder is the same process. A rejected claim is an honest no-op drain exit;
  epochs stay monotonic.
- **LISTEN/NOTIFY wake transport** (postgres only) — an advisory accelerator, never a
  correctness mechanism: the notify fires *after* the input INSERT commits, and the sweep
  ticker + startup scan bound any missed-notify window (wakes stay losable by invariant).
- **Exec row leases** — `exec_queue`/`async_jobs` rows are claimed by a single-statement lease,
  heartbeated by the owner, re-drivable by any replica after expiry. **Liveness only**: write
  acceptance never consults lease state; safety stays with epoch fencing (ADR-005 intact).

S8 — the acceptance e2e (`crates/kian-cli/tests/postgres_s8.rs`):
`s8_two_replicas_kill9_takeover_no_split_brain` runs two `kian serve` replicas over one
postgres database, `kill -9`s the drain owner mid-turn, and asserts the survivor's recovery
sweep takes over, exactly one end write lands, and a stale-epoch probe write issued with the
dead replica's frozen epoch is rejected — NoSplitBrainWrite between processes. Env-gated on
`KIAN_PG_URL`; default CI never needs postgres.

### GitHub channel and webhook receiver

Configuring a `type: github` channel (C48 adapter) is what mounts the `POST /webhooks/github`
receiver route (C49); the topic-mapping table doubles as the receive allowlist — a delivery
from an unmapped repository is an honest drop:

```yaml
superagent:
  channels:
    github:
      type: github
      github:
        topics:                 # owner/repo (or owner/repo#N) → topic; also the allowlist
          "flyer103/kian": "kian-repo"
        events: []              # empty = every supported type (issues, issue_comment,
                                #   pull_request, push, check_run, status)
        send: true              # reply half (issue/PR comments); requires KIAN_GITHUB_TOKEN
        webhook:                # receiver limits (defaults shown)
          max_body_bytes: 1048576
          rate_capacity: 60
          rate_refill_per_second: 2
```

Credentials never live in `kian.yaml`: the send token comes from `KIAN_GITHUB_TOKEN` and the
webhook secret from `KIAN_GITHUB_WEBHOOK_SECRET` (fail-closed at startup when the
corresponding capability is configured). The receiver verifies **before it parses**
(ADR-011): body-size cap (413) and content-type gate (415) precede any signature work;
HMAC-SHA256 over the raw body with a constant-time compare (401 on mismatch — zero parses);
then the per-source token bucket (429 with an audit line); only then is the verified delivery
handed to the subscription pipeline, whose `(session, event_id)` idempotency dedupes
delivery-id replays. GitHub payloads are attacker-influenced text — the channel is
untrusted-by-default (C43). All coverage runs against scripted HTTP/body fixtures with zero
credentials and zero network (`crates/kian-superagent/tests/github_channel.rs`,
`crates/kian-cli/tests/webhook_receiver.rs`).

### SDK quickstart (`kian-sdk`)

The wire-only embedded SDK (C46): speaks the frozen C13/C14 contract (JSON-RPC over
`POST /rpc` + `POST /ops/rpc`, the `events` SSE stream) and depends on **no kian crate**
(reqwest/serde/serde_json/futures/thiserror only) — embeddable anywhere a Rust HTTP client
runs. The surface is **frozen additive-only**; a breaking change requires a new plan round.

```toml
[dependencies]
kian-sdk = { path = "crates/kian-sdk" }   # versioned 0.1.0, decoupled from the workspace
```

```rust
use kian_sdk::KianClient;

let client = KianClient::new("http://127.0.0.1:7433");
let session = client.create_session().await?.session;
client.submit(&session, json!("investigate the CI failure"), None).await?;
client.steer(&session, json!("switch to plan B"), None).await?;   // lands at a safe boundary
client.interrupt(&session).await?;
let projection = client.projection(&session).await?;

// live event stream: parsed SseFrames until the server closes after done/error
use futures::StreamExt;
let mut stream = client.events(&session).await?;
while let Some(frame) = stream.next().await {
    let frame = frame?; // typed errors: match on ClientError, never string-parse
}

// ops plane: wake_now / recovery_budget_reset / mock_channel_send
client.wake_now(&session).await?;
```

Golden wire fixtures (`crates/kian-sdk/tests/fixtures/`) pin the request/response bytes of
every method.

### dsh bridge config (S6)

The bridge (C54) is mounted on the worker template, config-gated and **default off**
(`superagent.harness.dsh`); the command comes from config or `KIAN_DSH_BIN`, and neither being
present refuses to start (fail-closed). When mounted, the worker's loop is driven by a
`dsh --profile sdk` child over stdio JSON-lines — dsh keeps its interaction model, kian owns
durability (every mapped event rides the ordinary fenced append path; steers promote as inbox
rows at safe boundaries; the ADR-010 mapping is the only translation):

```yaml
superagent:
  harness:
    dsh:                        # absent = the built-in loop, unchanged
      command: /usr/local/bin/dsh   # or export KIAN_DSH_BIN=/usr/local/bin/dsh
      args: ["--profile", "sdk"]    # default
      slot: main                   # audit facts stamped into mapped step/request envelopes
      provider: openai-compat
      model: dsh
      max_steps: 32                # mapped-step budget across the session
```

CI proves the bridge against the C53 scripted fixture peer with zero Node/dsh present:
`crates/kian-dsh/tests/driver_bridge.rs` (driver), `crates/kian-superagent/tests/dsh_conformance.rs`
(the C53 mapping conformance suite), and the mounted e2e `crates/kian-cli/tests/dsh_bridge.rs`
— spawn/steer/complete, kill-9 takeover continuation, honest `interrupted` on child crash. The
live leg is env-gated; its executed outcome (an honest negative against dsh 0.2.1-rc.5) is recorded
in the [run log](#live-dsh-bridge-run-log-manual-env-gated) above.

## Phase-4 acceptance checklist (S8 + S6)

Scenario wording lives in [SPEC §2.5](docs/SPEC.md); proof is by the named tests.

| Scenario | Card | Status | Proof |
|----------|------|--------|-------|
| S8 multi-replica takeover (two replicas, kill -9 mid-drain, no split-brain) | C47 + C52 | **demonstrated** (env-gated `KIAN_PG_URL`; default CI excluded) | `crates/kian-cli/tests/postgres_s8.rs::s8_two_replicas_kill9_takeover_no_split_brain` (real `kian serve` children, real kill -9, stale-epoch probe rejected); store/exec pg semantics suites in `kian-store`/`kian-exec` `tests/postgres.rs` |
| S6 harness integration (dsh interaction unchanged, made durable) | C53 + C54, live leg C57 | **demonstrated — fixture-proven; live leg executed (honest negative), S6-live open** | fixture-proven in CI: `kian-dsh/tests/driver_bridge.rs` + `kian-superagent/tests/dsh_conformance.rs` (every mapping row, replay byte-parity, pre-flight ordering, honest unknown-kind errors) and `kian-cli/tests/dsh_bridge.rs` (mounted e2e: spawn/steer/complete, kill-9 takeover, child crash → `interrupted`). The live leg (`KIAN_DSH_BIN`) **ran** against real dsh 0.2.1-rc.5 and returned an honest `SoftFailed { reason: "peer never sent ready within 30s" }` — the stock dsh SDK app does not meet the ADR-010 contract-v0 bar — so S6-live **stays open**; it is not claimed as CI-proven, and the fixture suites remain the CI proof (see the [run log](#live-dsh-bridge-run-log-manual-env-gated)) |
| SDK freeze (wire-only client, golden bytes) | C46 | done | `crates/kian-sdk/tests/` — every typed method round-trips against a scripted server with golden request bytes; matchable typed errors |
| GitHub channel + webhook receiver (verify-then-parse, admit-only) | C48 + C49 | done | `crates/kian-superagent/tests/github_channel.rs` (mapping, send honesty, fail-closed token) + `crates/kian-cli/tests/webhook_receiver.rs` (signature matrix, 413/415-before-signature, 429 + audit, idempotent replay) + the iron-rule grep in `tests/iron_rule.rs` |
| Postgres export parity (S7 shape across backends) | C50 | done (env-gated) | `kian-store/tests/postgres.rs` — byte-identical JSONL export vs a sqlite store seeded with the same stream |

Phase-4 final four-gate run (card C55 closeout, 2026-11-01): `cargo build --workspace`,
`cargo test --workspace`, `cargo clippy --workspace -- -D warnings`, `cargo fmt --all --
--check` — all green with the gated env vars absent (postgres/dsh/github suites skip with log
lines); recorded per-card in the kanban handoffs.
