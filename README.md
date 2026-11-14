# Kian — the durable execution runtime for agents

> **One-liner: the Temporal of agents — an open-source, model-neutral, self-hostable durable
> execution runtime that makes agent tasks killable, resumable, migratable and auditable, with the
> super-agent reference architecture as a first-class workload.**

A single-session agent dies with its process, cannot be redirected at a safe boundary, cannot prove
what it did, and re-pays for its own context on every turn. Kian makes the **Session** the durable
entity, the **event stream** the only truth, and each execution interval (a **drain**) disposable —
so a crash, a deploy or a machine move costs a restart, not the work.

## Features

Three properties separate a *runtime* from an agent framework: work that survives the process,
machines you choose, and the loop you already run. Kian ships each as a product surface.

- **Durable execution** — a crash, deploy or machine move costs a restart, not the work: a new
  drain recovers the session, steers land at provider-turn safe boundaries, and interrupts are
  durable (never mapped onto `completed`). `kill -9` mid-turn, restart on the same sqlite file, and
  the turn finishes with a byte-identical context.
- **BYOM execution** — bring your own machine: a `kiand` daemon runs commands on the box you
  choose, over a durable at-least-once queue with a heartbeat watchdog, bound 1:1 to its worker
  session for that session's whole life; offline work parks and resumes honestly.
- **Harness integration** — bring your own loop: the built-in ReAct loop, or an existing harness
  such as dsh, behind the same `LoopDriver` seam and event kinds — durability does not force a
  rewrite of the loop that already works.

The same runtime also ships:

- **Audit by construction** — the append-only event stream is the only truth; replay is
  byte-identical and `session export` produces a replayable audit package.
- **Cost as an invariant** — model request bytes are persisted before send, history is never
  rewritten, and cache-prefix stability is observable (`compact` is the only deliberate break).
- **Bounded recovery** — pure error classification plus a caller-owned policy table, with one-shot
  guards and token/wall-clock budgets.
- **Multi-replica safety** — epoch fencing + `expectedSeq` CAS reject stale writes; sqlite by
  default, postgres for replicas (`KIAN_PG_URL`).
- **Super-agent bundle** — Router/Worker over Thread delegation, event subscriptions,
  evidence-gated completion, and channel output policies — all outside the core.
- **Real channels and intake economics** — Lark IM, GitHub (webhook + poll) and WeChat iLink
  (best-effort long-poll, QR login), two-level admission filtering, Inline/Spill shaping, Chain
  of Recall, and untrusted-input marking.
- **Bundle policy layer** — speak-rules, delegation Mandate, bounded outbox retry, policy
  versioning/drift marking, and the `kian tickets` stage board.
- **Durable worker binding** — each delegated worker session is bound 1:1 to one `kiand`
  worker (Router-chosen from the configured catalog, recorded as additive event bytes,
  re-derived from the stream): worker restarts, migrations and server reboots keep the
  binding; a busy or unknown worker refuses honestly.
- **Runtime worker membership** — workers join and leave a running server without config edits
  or restarts: `kiand --register-to` authenticates on a private, token-gated listener and the
  connection becomes the command route; commands queue durably per worker id while one is
  offline, `list_workers` exposes the live catalog, and join/leave never changes a prompt byte.
- **Worker filesystem tools** — `fs_list`/`fs_read`/`fs_write` execute on the **bound worker's**
  machine (its `--workspace-root`, a lexical path guard, 1 MiB bounds) with the same schemas and
  result shapes as the local built-ins; the durable queue, 1:1 binding and no-failover rules are
  unchanged (KiandProto v2, [ADR-023](design/decisions/ADR-023-worker-fs-tools.md)).
- **Agent memory (notes + dreaming + creation-time injection)** — the memory session's drain reads
  a bounded transcript range and appends `memory/note` events (claims with provenance and
  verification state); `kian memory create/dream/list/show/verify` plus the read-only
  `kian memory search|map` views over the notes. New sessions can open with a
  bounded, verified-only memory index injected once at creation (`memory.session`, config-gated,
  byte-identical when off).
- **Read-model serving form** — versioned unchanged/delta projection reads, resumable SSE
  (`Last-Event-ID`), verified snapshots + tail replay, materialized operator views, opt-in
  export-only archive, and an explicit-allowlist fan-in stream — all disposable projections;
  the event stream stays the only truth.
- **Operator surfaces and SDK** — inspect/replay/export/tickets/usage-stats, `/metrics`, and a
  frozen wire-only `kian-sdk`.
- **Model-neutral and self-hostable** — any OpenAI-compatible provider or gateway; one Rust
  process plus embedded sqlite is the whole system, with no hosted service required.

## How it works

- **Session** — the one first-class durable entity; turns, threads and endpoints derive from it.
- **Event stream** — append-only and never rewritten; session state is a projection of it, so the
  stream is also the audit record. The exact model request bytes are persisted before the call
  flies (`step/request`), which makes replay byte-identical.
- **Drain** — the execution hold interval: claim the session (epoch + lease), consume everything
  eligible, promote inputs only at provider-turn safe boundaries, then exit. A drain has no durable
  identity; after a crash a new drain replays the stream and continues.
- **Admit before wake** — inputs are durable rows before any wake fires; wakes are advisory (they
  may fold or be lost), and the inbox rebuild at startup re-derives them.
- **Safety by fencing** — the end write is one atomic append; every drain write carries a monotonic
  epoch and the store rejects stale writes via `expectedSeq` CAS. Leases only detect recoverable
  sessions, never authorize writes.
- **Honesty** — `interrupted ≠ failed`; an empty model response is a soft failure; a completion must
  pass the evidence gate. Compression is the only deliberate prefix-invalidation point.
- **The loop is control flow** — the core holds and persists execution; the agent loop (built-in
  ReAct, or a harness behind the `LoopDriver` seam) decides what to do inside a drain.

## Docs map

| Document | Contents |
|----------|----------|
| [docs/overview.md](docs/overview.md) | Problem, model, crate map, demonstrated capabilities — start here |
| [docs/SPEC.md](docs/SPEC.md) | The contract (WHAT): positioning, interfaces, scope |
| [design/DESIGN.md](design/DESIGN.md) | The blueprint (HOW): execution model, recovery, plugin system, bundle |
| [design/EVENT-MODEL.md](design/EVENT-MODEL.md) | The normative event contract: envelope, kind catalog, change protocol |
| [design/API-CONTRACT.md](design/API-CONTRACT.md) | The normative API contract: JSON-RPC surfaces, error codes, SSE/metrics/webhook, KiandProto |
| [design/MEMORY-MODEL.md](design/MEMORY-MODEL.md) | The normative memory contract: notes, dreaming, injection, representations (P1–P2 and P3a retrieval landed; persisted representations designed) |
| [design/READ-MODEL.md](design/READ-MODEL.md) | The normative serving-form contract: versions, deltas, snapshots, views, retention |
| [design/CHANNEL-WECHAT.md](design/CHANNEL-WECHAT.md) | The normative WeChat iLink channel contract: protocol mapping, credential file, config/CLI, dedupe, degradation |
| [design/WORKER-MODEL.md](design/WORKER-MODEL.md) | The normative worker-domain contract: membership lifecycle, binding/dispatch, catalog, security boundary |
| [design/decisions/](design/decisions/) | ADRs (append-only): each decision with alternatives and trade-offs |
| [the design notes](the design notes) | Synthesis of the design inputs |
| [docs/runbook-dsh.md](docs/runbook-dsh.md) | Running the dsh bridge |
| [docs/runbook-postgres.md](docs/runbook-postgres.md) | Running postgres mode and the multi-replica replay |
| [a vendored archive/](a vendored archive/) | Frozen snapshots of the design inputs (read-only) |
| [CHANGELOG.md](CHANGELOG.md) | What landed when, and what is env-gated |

## Quickstart

Prerequisites: Rust stable 1.85+ (`edition 2024`), plus `curl` and `jq`. Nothing else — sqlite is
embedded and a single `kian serve` process is the whole system.

The commands below were run against this tree; the output blocks are abridged from those runs. The
provider was a local OpenAI-compatible SSE stub, so no credentials or network were needed. Point
`base_url`/`model` at any real OpenAI-compatible endpoint and the same session runs against a model;
a gateway that fronts several provider accounts is just another such endpoint, because the provider
contract is `base_url` + OpenAI chat-completions + SSE.

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

Two rules are fail-closed: a missing `recovery:` section refuses to start (a policy table is never
implied), and a provider whose `api_key_env` variable is unset refuses to start (secrets never live
in the file). A malformed config never degrades into a running server.

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

curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"submit\",\"params\":{\"session\":\"$SID\",\"content\":\"hello kian\"}}"
# {"id":2,"jsonrpc":"2.0","result":{"deduplicated":false}}

curl -sN localhost:7433/sessions/$SID/events      # live SSE stream; ends after session/ended
```

The stream *is* the audit record. Abridged — ids shortened and the full `step/request` body elided:

```
event: kian
data: {"seq":1,…,"kind":"session/created","bytes":{},"version":0}
data: {"seq":2,…,"epoch":1,"kind":"drain/claimed","bytes":{"epoch":1,"holder":"kian-serve"}}
data: {"seq":3,…,"kind":"input/admitted","bytes":{"content":"hello kian","role":"user"}}
data: {"seq":6,…,"kind":"step/request","bytes":{"body":{…,"messages":[{"content":"hello kian","role":"user"}]},"iteration":0,"model":"gpt-4o-mini"}}
data: {"seq":7,…,"kind":"assistant/message","bytes":{"content":{"text":"hello from a scripted provider"},"role":"assistant"}}
data: {"seq":8,…,"kind":"turn/end","bytes":{"iteration":0,"outcome":"final_answer"}}
data: {"seq":9,…,"kind":"session/ended","reason":"completed","bytes":{}}
event: done
data: ended
```

Note `step/request`: the exact request bytes are persisted **before** the call flies, which is what
makes replay byte-identical. Read state back from the projection, never from a column:

```bash
curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"projection\",\"params\":{\"session\":\"$SID\"}}"
# {"id":3,"jsonrpc":"2.0","result":{"state":"completed"}}

target/debug/kian session inspect "$SID" --config kian.yaml
```

```
session             36e8cedd-…
state               completed
events              9 (next seq 10, epoch 1)
inputs admitted     1
drain claims        1
turns               1
requests            1
assistant messages  1
```

### 5. Wake, replay, audit

A wake is advisory: it spawns a drain that finds an empty inbox and exits; the session stays `idle`
until an input is admitted. Waking a terminal session is refused honestly rather than ignored.

```bash
IDLE=$(curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"create_session"}' | jq -r .result.session)
target/debug/kian wake-now "$IDLE" --config kian.yaml

target/debug/kian session replay "$SID" --config kian.yaml
target/debug/kian session export "$SID" --config kian.yaml --out audit.jsonl
target/debug/kian cache-stats --config kian.yaml --json
```

```
derived state       "completed"
invariants          ok
model context       1 of 1 requests byte-identical   <- replay derives the same bytes
```

The cache report is the cache-byte-stability observable: consecutive request pairs compared, how
many shared a byte-identical message prefix, and why a prefix broke. `compact` is the **only**
counted invalidation cause; any other prefix change is reported as `unexpected_prefix_change` and
is a bug.

### 6. Prove durability: `kill -9` mid-turn, restart, no lost work

Submit an input, `kill -9` the server while the provider call is in flight, then restart on the same
sqlite file:

```bash
SID2=$(curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":5,"method":"create_session"}' | jq -r .result.session)
curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"submit\",\"params\":{\"session\":\"$SID2\",\"content\":\"run the long job\"}}"

sleep 1
kill -9 <pid-of-kian-serve>
target/debug/kian session inspect "$SID2" --config kian.yaml
```

```
session             62795e1c-…
state               running
events              6 (next seq 7, epoch 1)
requests            1               <- the request bytes are durable; the turn is not finished
assistant messages  0
```

The session is still `running`: no end write happened, and the dead drain's lease has not expired —
leases are liveness only, their job is to make the session *detectable* as recoverable. Restart
`kian serve` on the same `kian.db`; the recovery sweep takes over and a new drain continues from the
stream:

```
session             62795e1c-…
state               completed
events              12 (next seq 13, epoch 2)
drain claims        2
requests            2
```

The takeover rewrites no earlier event (the stream shows `drain/claimed` epoch 2), and the retried
request derived a byte-identical message prefix — the cache report confirms one stable pair. The
process died; the work did not. Safety came from the **epoch**, not from the lease: a
partitioned-but-alive old holder would be fenced out the same way. The automated version of this
run, with six kill points and repeated iterations, is `crates/kian-cli/tests/chaos_recovery.rs`.

### Test

```bash
cargo test --workspace
```

Everything — unit, wire-level e2e against a scripted mock provider, and the kill-and-restart
(chaos-style) suite — runs in the default workspace test invocation. Local e2e sets
`NO_PROXY=127.0.0.1,localhost` itself. Suites whose environment is absent skip with a log line and
never fail; see the [environment contract](#environment-contract).

## Advanced usage

### Super-agent bundle (Router / Worker)

The super-agent reference architecture is a *bundle* over the core, never a core capability: a
resident Router session judges incoming events, dispatches work to Worker sessions over Thread
delegation, and the worker's completion must pass the evidence gate before the Router delivers.
On top of the quickstart config (same provider), enable the bundle and define its templates:

```yaml
plugins: ["openai-compat", "local-tools", "superagent", "remote-exec"]
agents:
  router:
    id: router
    model_slots:
      router: { provider: local, model: gpt-4o-mini }
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    model_slots:
      main: { provider: local, model: gpt-4o-mini }
    tools: ["fs_list", "fs_read", "fs_write", "exec"]   # fs_* run on the bound worker (v2)
superagent:
  router: { session_id: "00000000-0000-0000-0000-000000000001", agent: router, slot: router }
  worker: { agent: worker }
  channels:
    mock-im: { type: mock-im }
  subscriptions:
    - { channel: mock-im, match: always, route: router }
endpoints:
  - { id: dev-box, addr: "127.0.0.1:7401", queue_timeout_ms: 30000, offline_after_ms: 5000 }
```

With the server running:

```bash
cargo run -p kian-cli --bin kian -- mock-channel-send mock-im --text "deploy please"
curl -s localhost:7433/metrics | jq .superagent   # evidence / subscription / channel counters
```

### Channels (GitHub, Lark, WeChat)

Channels admit external events into subscriptions. `type: github` mounts the `POST /webhooks/github`
receiver; the topic-mapping table doubles as the receive allowlist. The same channel can poll with
`since` cursors instead of receiving webhooks.

```yaml
superagent:
  channels:
    github:
      type: github
      github:
        topics: { "flyer103/kian": "kian-repo" }   # owner/repo → topic; also the allowlist
        send: true                                  # reply half; requires KIAN_GITHUB_TOKEN
        webhook: { max_body_bytes: 1048576 }        # mounts POST /webhooks/github
    lark-im:
      type: lark
      lark:
        topics: { "oc_xxxxxxxxxxxxxxxx": "team-im" }
        poll_interval_ms: 2000
    wechat:
      type: wechat
      wechat:
        topics: { "oXXXX@im.wechat": "wechat" }     # sender → topic; also the allowlist
        credentials_file: ./.state/wechat-credentials.json   # written by `kian wechat login`
```

`type: wechat` is **best-effort by contract**: one long-poll loop per channel over Tencent's
reverse-documented iLink Bot API (~35 s server hold), text-only receive, and reply-only sends that
echo the latest inbound `context_token` — so it cannot message a user who has not messaged the bot
first. A stale token (`errcode -14`) pauses that channel and resumes when the credential file
changes ([design/CHANNEL-WECHAT.md](design/CHANNEL-WECHAT.md)).

Credentials are **env-only** (`KIAN_GITHUB_TOKEN`, `KIAN_GITHUB_WEBHOOK_SECRET`,
`KIAN_LARK_APP_ID`/`*_SECRET`) and absent values refuse to start when the capability is configured
— with one deliberate exception: the WeChat token is *issued* by a QR scan, so it lives in the
local `0600` credential file named by `wechat.credentials_file`, never in an env var or
`kian.yaml` ([ADR-018](design/decisions/ADR-018-wechat-ilink-channel.md)). The webhook receiver
verifies before it parses (size cap → content-type → HMAC constant-time
compare → rate limit), and real channels are untrusted-by-default. A complete live dogfood lives in
[examples/issue-watch](examples/issue-watch/README.md).

### Bundle policies (speak-rules, Mandate, outbox, policy version)

A few config-first policies sit between the Router and the outside world:

```yaml
superagent:
  speak_rules:
    - { channel: issue-github, when: result-ready }   # whether-to-send gate
  mandate:
    scopes:
      worker: "draft a triage comment; never post unless DRY_RUN=0"
  outbox: { max_attempts: 6, backoff_cap_ms: 60000 }  # bounded delivery retry
  policy: { version: "2026-11-04.1", notify_live: false }
```

- **Speak-rules** suppress a send with an honest tool error, an audit line and a counter — no send
  means no `channel/delivered`.
- **Mandate** is the immutable delegation authority object, carried verbatim into `thread/started`
  and revalidated before follow-ups ([ADR-012](design/decisions/ADR-012-delegation-mandate.md)).
- **Outbox** is a disposable projection over send intent + delivery receipt; retries are bounded
  and a parked delivery names its conversation on the stage board.
- **Policy version** renders as a `[policy <label>]` header; boot-time drift is marked without
  rewriting live sessions.

### BYOM execution endpoints

The cloud loop can execute commands on a remote endpoint daemon (`kiand`) over a durable queue:

```bash
cargo run -p kiand -- --listen 127.0.0.1:7401 --heartbeat-ms 1000 --workspace-root .
```

A worker session's `exec` runs commands on its bound worker; since KiandProto v2 its
`fs_list`/`fs_read`/`fs_write` do too, resolved under that worker's `--workspace-root` (default: the
daemon's working directory; the server's local fs tools are not used for a bound session). The
worker surface is text-only with a 1 MiB read/write bound; a larger body or a `..` path is an
honest tool error.

Rows are at-least-once with command-id dedupe; an offline endpoint parks work durably, a restarted
daemon reconnects and drains it, and the queue timeout is an honest `Timeout` — never a fake
success. The heartbeat watchdog admits a durable health input to affected worker sessions.

#### Durable worker binding (per session)

`endpoints:` is also the worker catalog: each entry gains an optional human `description` the
Router sees when picking a machine, and each delegated worker session is bound **1:1** to exactly
one worker for its whole lifetime
([ADR-020](design/decisions/ADR-020-durable-worker-binding.md)).

```yaml
endpoints:
  - id: mac-mini
    addr: "127.0.0.1:7401"
    description: "local macOS dev machine — repo checkouts, cargo, git"
  - id: vultr-builder
    addr: "10.0.0.7:7401"
    description: "linux builder — docker, long builds"
```

Whenever worker membership is on (any `endpoints:` entry or a `workers:` section), the Router must
name one on every spawn —
`spawn_worker {"task": "run the release build", "exec_worker": "vultr-builder"}` — and the choice
is recorded as additive `exec_worker` bytes on the delegation (both sides' `thread/started`), then
re-derived from the stream on every restart: the worker's `exec` routes only to its bound daemon
across worker restarts, migrations (same id, new address) and server reboots, with no rebind and no
failover to another machine. A busy or unknown worker refuses the spawn honestly before any side
effect — the refusal names the owning session. There is no single-worker default: the argument is
required as soon as membership is on (a schema that depended on how many workers happen to be known
would change bytes when one joins), and with no workers configured the feature is off and the
argument is refused ([ADR-020](design/decisions/ADR-020-durable-worker-binding.md),
[WORKER-MODEL.md](design/WORKER-MODEL.md)).

#### Runtime worker membership (workers join/leave without a restart)

Workers can also register themselves at runtime — no `kian.yaml` edit, no `kian serve` restart
([ADR-021](design/decisions/ADR-021-runtime-worker-membership.md), the contract is
[WORKER-MODEL.md](design/WORKER-MODEL.md)):

```yaml
workers:                       # absent = dynamic membership off (byte-identical)
  listen: "127.0.0.1:7400"     # private bind only (loopback/RFC1918/CGNAT/ULA)
  token_env: KIAN_WORKER_TOKEN # env-only shared token; the value never lives in the file
```

```bash
export KIAN_WORKER_TOKEN=...   # unset/blank refuses server startup (fail-closed)
# on the worker machine (reach the listener over an SSH tunnel or Tailscale):
cargo run -p kiand -- --register-to 127.0.0.1:7400 --endpoint-id mac-mini \
  --description "local macOS dev machine" --workspace-root .   # registers over
                                            # KiandProto v2; the connection becomes the command route
kian workers                   # read-only operator view: id, state, bound session
```

Joining makes the worker spawnable immediately (`list_workers` shows it `online`); leaving —
graceful `bye` or `kill -9` — marks it offline within `offline_after_ms`. Commands for a
known-but-offline worker park durably and run when it returns; an unknown id is refused naming the
known ids. Static `endpoints:` pins and dynamic workers share one catalog (a colliding id is
refused), and membership changes never touch a prompt or tool-schema byte.

Reaching a server on another machine: the listener accepts only a private bind, so a laptop reaches
a remote `kian serve` through an SSH tunnel (`ssh -N -L 7400:127.0.0.1:7400 <server>`, then
`--register-to 127.0.0.1:7400`) or over Tailscale — its `100.64.0.0/10` addresses are in the
private set. `workers.allow_public: true` exists as an explicit escape hatch, but it exposes a
command-execution surface under one shared token; a tunnel is the supported shape. A worker on the
server itself is an ordinary static `endpoints:` pin (kian dials it, so it needs no token), and the
local worker's `--description` is what the Router sees in `list_workers` when it picks a machine.

### Postgres and multi-replica

sqlite is the default; postgres is the hosted backend. The URL is env-only (the config names the
variable) and postgres mode requires an explicit unique `drain.holder` — both fail closed.

```yaml
storage:
  event_store: postgres
  postgres:
    url_env: KIAN_PG_URL
drain:
  holder: replica-a
```

Multi-replica safety is fencing-based: a claim lands only under conditional admission, LISTEN/NOTIFY
is an advisory wake accelerator, and stale-epoch writes are rejected by the store. The multi-replica e2e runs
two replicas, `kill -9`s the owner and asserts the survivor completes with exactly one end write.
Setup and the replay: [docs/runbook-postgres.md](docs/runbook-postgres.md).

### Embedded SDK

`kian-sdk` is a wire-only client over the JSON-RPC + SSE contract; it depends on no kian crate and
the surface is frozen additive-only.

```toml
[dependencies]
kian-sdk = { path = "crates/kian-sdk" }
```

```rust
use kian_sdk::KianClient;

let client = KianClient::new("http://127.0.0.1:7433");
let session = client.create_session().await?.session;
client.submit(&session, json!("investigate the CI failure"), None).await?;
client.steer(&session, json!("switch to plan B"), None).await?;   // lands at a safe boundary
client.interrupt(&session).await?;
let state = client.projection(&session).await?;
```

### Harness bridge (dsh)

A harness can keep its interaction model while kian owns durability: `kian-dsh` drives a
`dsh --profile sdk` child over stdio and maps its events onto existing kian kinds. Config-gated and
default off.

```yaml
superagent:
  harness:
    dsh:
      command: /usr/local/bin/dsh   # or export KIAN_DSH_BIN
      args: ["--profile", "sdk"]
      max_steps: 32
```

#### Live dsh-bridge run log (manual, env-gated)

The live smoke runs only when `KIAN_DSH_BIN` is present; default CI runs against the scripted
contract-v0 fixture peer. The live leg ran green on 2026-11-02 via
[`contrib/dsh-v0-peer/`](contrib/dsh-v0-peer/) against real dsh 0.2.1-rc.5 and real
`deepseek-chat` — full mapped chain, honest `Settled`. Re-run commands and the survey:
[docs/runbook-dsh.md](docs/runbook-dsh.md).

### Tool permissions (per-call governance)

```yaml
agent:
  tools: ["fs_read", "fs_write"]
  permissions:
    fs_read: allow
    fs_write: deny
```

The decision is evaluated **before** the call is logged and recorded as provenance on `tool/call`
([ADR-015](design/decisions/ADR-015-tool-permission-provenance.md)); a deny never invokes the tool —
the model receives an honest error result and reacts. Unknown values and keys outside `tools:`
refuse to start. Absent table = empty = allow-all, byte-identical to a server without the feature.

### Agent memory (notes + dreaming)

Memory is a bundle over the same event stream: a memory session whose drain ("dreaming") reads a
bounded range of a source session's transcript and appends `memory/note` events — immutable claims
keyed by subject, carrying provenance (`derived_from`) and a fail-closed
`candidate|verified|stale` status. Notes are non-context events: no session's model request bytes
change. Absent the `memory:` section, behavior is byte-identical (memory off).

```yaml
memory:
  scope: my-agent            # the agent identity whose memory this is
  session: 0f9c2a11-…        # the source memory session: new sessions open with its
                             # verified notes as an injected index (absent = injection off)
  inject:
    max_items: 20            # default; over-cap renders subject + one-line excerpt only
    max_bytes: 4096          # default; the rendered index never exceeds this
  dreamer:
    agent: my-agent          # a named agents: entry or the default agent: id
    max_bytes_read: 262144   # default 256 KiB; an over-budget dream is refused, not truncated
    max_notes: 20            # default; an over-cap answer is refused, not truncated
```

With the server running (`memory dream` goes through the `memory_dream` ops method):

```bash
MEM=$(target/debug/kian memory create --config kian.yaml --json | jq -r .session)
target/debug/kian memory dream --session "$MEM" --source "$SID" --from 1 --to 9 --config kian.yaml
target/debug/kian memory list   --session "$MEM" --config kian.yaml   # the projected notes
target/debug/kian memory show   --session "$MEM" deploys.rollback --config kian.yaml
target/debug/kian memory verify --session "$MEM" --config kian.yaml   # re-check ranges offline
```

A dream is idempotent per `(source, range)` — repeating the same range is a no-op, and a drain
crash resumes without duplicate notes (dedupe on `(subject, derived_from)`).

With `memory.session` set, every newly created session quietly admits one bounded index of the
**verified** notes (frozen "data, not instructions" framing, secret-redacted, hard-capped) as its
first system message — persisted as an event before any request flies, injected exactly once
(never on resume or migration), strippable from replay, and byte-identical to a memory-off server
when the key is absent. The `injected_*` keys in `curl -s localhost:7433/metrics | jq .memory`
count sessions, items, bytes, truncations and honest skips.
([design/MEMORY-MODEL.md](design/MEMORY-MODEL.md) §6.)

Two read-only views navigate the growing memory without any write (a missing store is an error,
never a created file; caps and `truncated` flags bound the output):

```bash
target/debug/kian memory search rollback --session "$MEM" --config kian.yaml   # per-hit status
target/debug/kian memory map      --session "$MEM" --config kian.yaml --json   # verified-only
```

Search folds latest-per-subject across all statuses and shows each hit's
`verified|candidate|stale`; the map keeps only subjects whose latest revision is `verified` and
joins notes by intersecting `derived_from` evidence — under its caps every delivered edge keeps
both endpoints in the delivered nodes (the byte cap keeps the node prefix and cuts edges first) —
reporting `degenerate` (use search instead) and `oversized` honestly instead of hiding a useless
view
([design/MEMORY-MODEL.md](design/MEMORY-MODEL.md) §7).

### Read model (versions, snapshots, views, retention)

Every serving read is versioned and disposable — the event stream stays the only truth.
`projection` answers unchanged reads with one version check; SSE frames carry `id: <seq>` and
reconnect with `Last-Event-ID`; recovery loads the latest verified snapshot and replays only the
tail. Snapshots and views default on (`read_model:`) and can be disabled for byte-identical
full-derivation behavior.

Retention is opt-in and export-only (default off): terminal sessions leave the online store only
behind a verified package that reproduces the audit export byte-identically; reads of an archived
session answer the explicit `archived` error with import guidance.

```bash
# unchanged poll ($V = the version you last saw): one version check, no state in the answer
NO_PROXY=127.0.0.1,localhost curl -s localhost:7433/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"projection","params":{"session":"'"$SID"'","since":'"$V"'}}'
# → {"jsonrpc":"2.0","id":1,"result":{"version":9,"unchanged":true}}

# opt-in retention (fail-closed: archive_dir + one threshold required when enabled)
# retention:
#   enabled: true
#   archive_dir: /var/lib/kian/archive
#   min_age_ms: 2592000000        # terminal ≥ 30 days
target/debug/kian archive run    --config kian.yaml          # verify → marker → delete, idempotent
target/debug/kian archive verify <uuid> --config kian.yaml   # re-check hashes + replay offline
```

### Operator surfaces

| Surface | What it does |
|---------|--------------|
| `kian tickets [--json]` | Read-only Ticket stage board: `queued`, `working`, `awaiting-answer`, `awaiting-review`, `delivery-retry`, `blocked-failed`, `idle` |
| `kian usage-stats [--json]` | Provider token usage projected from `turn/end` (no pricing — the provider's own numbers) |
| `kian session inspect <id>` | Projected state, counters, usage lines, pending control intents |
| `kian session replay <id>` | Replay the stream and verify invariants and byte-identical context |
| `kian session export <id> --out audit.jsonl` | Audit package; `--with-event-ids` adds deterministic derived event ids |
| `kian cache-stats [--json]` | Prefix-cache stability report from the durable stream |
| `kian wake-now`, `kian lease inspect`, `kian recovery budget reset` | Operational actions |
| `GET /metrics` | Engine, cache, usage and super-agent counters |

Interrupt is durable: it admits a control-envelope inbox row (idempotent per epoch), so the intent
survives a drain crash and the takeover settles `interrupted`
([ADR-014](design/decisions/ADR-014-durable-interrupt-intent.md)).

## Environment contract

Default CI needs none of these — every gated suite **skips with a log line, never fails**:

| Env var | Used by | Absent behavior |
|---------|---------|-----------------|
| `KIAN_PG_URL` | postgres store/exec suites and the multi-replica e2e | skip with a log line |
| `KIAN_DSH_BIN` | the dsh live smoke; default command source for a configured dsh harness | smoke skips; a mounted harness without a command refuses to start |
| `KIAN_GITHUB_TOKEN` | github channel send and poll reads | `send: true` refuses to start; public polls run tokenless |
| `KIAN_GITHUB_WEBHOOK_SECRET` | webhook HMAC verification | a github webhook channel refuses to start |
| `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` / `KIAN_LARK_CHAT_ID` / `KIAN_LARK_POLL_CHAT` | lark channel and live smoke | skip |
| `KIAN_API_KEY` | live-provider smoke | skip |

Secrets and URLs are env-only, never config values — the WeChat bot token is the one documented
exception: it is issued by a QR scan and kept in a `0600` file
([ADR-018](design/decisions/ADR-018-wechat-ilink-channel.md)). Executed live records live in
[CHANGELOG.md](CHANGELOG.md).

## Crate map

| Crate | One line |
|-------|----------|
| `kian-core` | Concept model, the 21 event kinds, session state machine, projection engine, model-context derivation, seam traits |
| `kian-store` | EventStore with `expectedSeq` CAS + epoch fencing, inbox, claims/leases, dual backend (sqlite default, postgres optional) |
| `kian-orchestrator` | Admit → Wake → Drain → Promote engine and the bounded three-layer recovery subsystem |
| `kian-llm` | OpenAI-compatible provider, model slots, persist-before-send hook |
| `kian-loop` | `LoopDriver` seam plus the built-in ReAct loop |
| `kian-memory` | The memory bundle: the dreaming drain — dream prompt, fail-closed note parser, `DreamDriver` — the frozen creation-time index renderer, and the read-only search/map views |
| `kian-plugin` | Static plugin system: `Ctx` registry, `Plugin` trait, dispatch, `Disposer` |
| `kian-endpoint` | KiandProto endpoint-side SDK library (embeddable) |
| `kian-exec` | KiandProto server half: connections, durable command queue, heartbeat watchdog |
| `kian-superagent` | The bundle: Router/Worker templates, subscriptions, channels, output policies, recall, watchdog |
| `kian-sdk` | Wire-only embedded client (frozen JSON-RPC + SSE surface, typed errors) |
| `kian-dsh` | The dsh bridge: a `LoopDriver` over a stdio child, mapping onto existing kinds |
| `kian-cli` | The `kian` binary: serve assembly, operator CLI, inbound webhook receiver |
| `kiand` | The endpoint daemon binary |

## Examples

- [examples/issue-watch](examples/issue-watch/README.md) — one Ticket per live GitHub issue:
  poll channel → Router triage → Mandate-bounded worker → speak-rule delivery through the outbox.
- [examples/kanban-watch](examples/kanban-watch/README.md) — a timer-driven Router watching a local
  Kanban board, classifying stuck tasks and (dry-run by default) pushing them forward.
