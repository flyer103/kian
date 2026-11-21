# Kian — capability and usage guide

> The full capability list and the usage recipes: the super-agent bundle, channels, bundle policies,
> the task board, BYOM endpoints, postgres, the embedded SDK, the harness bridges, tool permissions,
> hook plugins, agent memory, the read model, scheduled turns and the operator surfaces.
>
> Semantics live in the contract ([SPEC.md](SPEC.md)) and the blueprint
> ([DESIGN.md](../design/DESIGN.md)); the normative sub-contracts are under [design/](../design/).
> The five-minute path is the [README quickstart](../README.md#quickstart).

## What ships

- **Audit by construction** — the append-only event stream is the only truth; replay is
  byte-identical and `session export` produces a replayable audit package.
- **Cost as an invariant** — model request bytes are persisted before send, history is never
  rewritten, and cache-prefix stability is observable (`compact` is the only deliberate break).
- **Bounded recovery** — pure error classification plus a caller-owned policy table, with one-shot
  guards and token/wall-clock budgets.
- **Human-in-the-loop by design** — a tool call can be deferred for approval (`permissions: ask`):
  the drain ends `awaiting_input` without holding a lease, the pending call is derived from the
  stream, and the answer is a durable control row that resumes the same turn — same prefix bytes,
  no lost work if the answer arrives after a restart.
- **Multi-replica safety** — epoch fencing + `expectedSeq` CAS reject stale writes; sqlite by
  default, postgres for replicas (`KIAN_PG_URL`).
- **Super-agent bundle** — Router/Worker over Thread delegation, event subscriptions,
  evidence-gated completion, and channel output policies — all outside the core.
- **Real channels and intake economics** — Lark IM, GitHub (webhook + poll), WeChat iLink
  (best-effort long-poll, QR login) and iMessage through a Mac-side bridge, two-level admission
  filtering, Inline/Spill shaping, Chain of Recall, and untrusted-input marking.
- **Bundle policy layer** — speak-rules, delegation Mandate, bounded outbox retry, policy
  versioning/drift marking, and the `kian tickets` stage board.
- **Scheduled turns** — declarative `superagent.cron` schedules execute as ordinary durable turns
  (resident, or a detached per-firing git worktree), with a durable slot cursor, DST-correct
  next-fire math, misfire coalescing, disk guards and a `kian cron list|run-now|logs|rm` surface.
- **Durable worker binding** — each delegated worker session is bound 1:1 to one `kiand`
  worker (Router-chosen from the worker catalog, recorded as additive event bytes,
  re-derived from the stream): worker restarts, migrations and server reboots keep the
  binding; a busy or unknown worker refuses honestly, and `kian workers release <id>` closes a
  thread that ended `interrupted`/`failed` (or the bounded reclaim does) so a dead thread can
  never leak the slot ([ADR-035](../design/decisions/ADR-035-worker-release-and-idle-reclaim.md)).
- **Runtime worker membership** — workers join and leave a running server without config edits
  or restarts: `kiand --register-to` authenticates on a private listener (shared token or
  per-worker enrollment) and the connection becomes the command route; commands queue durably per
  worker id while one is offline, `list_workers` exposes the live catalog including an
  worker-declared capability list, and join/leave never changes a prompt byte.
- **Worker filesystem tools** — `fs_list`/`fs_read`/`fs_write` execute on the **bound worker's**
  machine (its `--workspace-root`, a lexical path guard, 1 MiB bounds) with the same schemas and
  result shapes as the local built-ins; the durable queue, 1:1 binding and no-failover rules are
  unchanged (KiandProto v2, [ADR-023](../design/decisions/ADR-023-worker-fs-tools.md)).
- **Hook plugins** — operator-trusted sidecars extend the harness at the admission, tool and
  settlement boundaries ([PLUGIN-SPEC](../design/PLUGIN-SPEC.md), [ADR-028](../design/decisions/ADR-028-harness-hook-plugins.md)):
  observe every port, deny a side effect before it happens (`tool/pre`), gate `completed`, with
  per-dispatch deadlines, digest-pinned spawns, quarantine and fail-closed defaults — in any
  language that speaks newline-delimited JSON on stdio.
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
- **Operator surfaces and SDK** — inspect/replay/export/tickets/board/tasks/usage-stats/workers,
  `/metrics`, and a frozen wire-only `kian-sdk`.
- **Model-neutral and self-hostable** — any OpenAI-compatible provider or gateway; one Rust
  process plus embedded sqlite is the whole system, with no hosted service required.


## Super-agent bundle (Router / Worker)

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
      router: { provider: deepseek, model: deepseek-flash }
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    model_slots:
      main: { provider: deepseek, model: deepseek-flash }
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

To delegate to named agents, add a roster: each entry points at an `agents:` definition, and
`tools_allow` narrows the child's tools to exactly the intersection with that definition's
`tools:` (see DESIGN §4.8 and MULTIAGENT-MODEL §7.3). The Router's `spawn_worker` then accepts
`agent`, `task_key` and `output_schema`:

```yaml
superagent:
  roster:
    - id: researcher
      description: Finds and cites sources
      agent: researcher            # an entry in agents: with its own model slots
      mandate_template: { scope: read-only research, strength: inferred }
      tools_allow: ["fs_read"]     # the child sees only these tools
```

```bash
cargo run -p kian-cli --bin kian -- agents   # list the delegation roster
```

## Channels (GitHub, Lark, WeChat, iMessage)

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
    imessage:
      type: imessage
      imessage:
        bridge_url: "http://127.0.0.1:7420"          # the Mac bridge, over an SSH -R / mesh VPN
        topics: { "+14155551212": "imessage" }       # handle → topic; both allowlists
```

`type: wechat` is **best-effort by contract**: one long-poll loop per channel over Tencent's
reverse-documented iLink Bot API (~35 s server hold), text-only receive, and reply-only sends that
echo the latest inbound `context_token` — so it cannot message a user who has not messaged the bot
first. A stale token (`errcode -14`) pauses that channel and resumes when the credential file
changes ([design/CHANNEL-WECHAT.md](../design/CHANNEL-WECHAT.md)).

`type: imessage` needs a Mac signed into a dedicated Apple ID running
`kian-imessage-bridge` (Full Disk Access + Automation → Messages, `imsg` ≥ 0.14), because iMessage
has no API — the message must land on that Mac's `chat.db`. The adapter long-polls the bridge's loopback ABI;
the bridge's `participants` list keeps non-allowlisted conversations from leaving the Mac.
`kian imessage check` is the read-only preflight
([design/CHANNEL-IMESSAGE.md](../design/CHANNEL-IMESSAGE.md),
[docs/runbook-imessage.md](../docs/runbook-imessage.md)).

Credentials are **env-only** (`KIAN_GITHUB_TOKEN`, `KIAN_GITHUB_WEBHOOK_SECRET`,
`KIAN_LARK_APP_ID`/`*_SECRET`, `KIAN_IMESSAGE_BRIDGE_TOKEN`) and absent values refuse to start
when the capability is configured
— with one deliberate exception: the WeChat token is *issued* by a QR scan, so it lives in the
local `0600` credential file named by `wechat.credentials_file`, never in an env var or
`kian.yaml` ([ADR-018](../design/decisions/ADR-018-wechat-ilink-channel.md)). The webhook receiver
verifies before it parses (size cap → content-type → HMAC constant-time
compare → rate limit), and real channels are untrusted-by-default. 

## Bundle policies (speak-rules, Mandate, outbox, policy version)

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
  and revalidated before follow-ups ([ADR-012](../design/decisions/ADR-012-delegation-mandate.md)).
- **Outbox** is a disposable projection over send intent + delivery receipt; retries are bounded
  and a parked delivery names its conversation on the stage board.
- **Policy version** renders as a `[policy <label>]` header; boot-time drift is marked without
  rewriting live sessions.

## Delegation task board (kian board / kian tasks --all)

Every `spawn_worker` delegation is a task with a derived identity. The board is the read-only
operator surface over those tasks — one row per task, derived from the committed wrapper plus the
child's own stream (never from prose), served from the materialized `task_board` view when fresh
and from the honest full-stream derivation otherwise (identical output either way):

```yaml
superagent:
  board:
    enabled: true      # default false: no view published, verbs refuse with exit 2
    gating: true       # P4: the depends_on/group spawn arguments, readiness gating and the
                       # multiagent audit key groups (default false: P3 bytes exactly)
```

```console
$ kian board --db kian.sqlite
TASK_ID                               ASSIGNEE   ATTEMPT                OUTCOME              STATUS       GATE   EVIDENCE                  READY
8d810f2b-a34f-5188-81fb-3611c2489591  -                1     completed_verified         completed     pass   pass@6 (derived)          yes
4e69aaa3-4650-5b49-b37e-5b9a01ac212f  -                1           outstanding       outstanding   absent   -                          no

$ kian tasks --all --db kian.sqlite --json   # the same rows, flat / as {now_ms, version, tasks}
```

`kian tasks` without `--all` refuses with usage (there is no filtered task surface in v1); both
verbs write nothing — no events, no inbox rows, no view updates.

## BYOM execution endpoints

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
([ADR-020](../design/decisions/ADR-020-durable-worker-binding.md)).

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
argument is refused ([ADR-020](../design/decisions/ADR-020-durable-worker-binding.md),
[WORKER-MODEL.md](../design/WORKER-MODEL.md)).

A thread that ended `interrupted`/`failed` is resumable and keeps holding the worker — so it is
closed explicitly or after a bounded grace, never leaked forever. `kian workers release <id>` does
it now: one atomic claim + `session/ended{cancelled}` batch preserves the earlier honest end,
frees the slot, and makes the thread terminal (it can never resume into the slot a newer thread
holds). The same close runs automatically when the thread has been idle-ended for
`workers.release_after_ms` (default five minutes, `0` disables). A running thread, a thread with
eligible input, or a worker still owing a queued command refuses the release honestly
([ADR-035](../design/decisions/ADR-035-worker-release-and-idle-reclaim.md)).

```bash
kian workers release mac-mini --reason "interrupted task, freeing the slot"
kian workers release mac-mini --json     # {"worker": ..., "outcome": "released"|"already_released", ...}
```

#### Runtime worker membership (workers join/leave without a restart)

Workers can also register themselves at runtime — no `kian.yaml` edit, no `kian serve` restart
([ADR-021](../design/decisions/ADR-021-runtime-worker-membership.md), the contract is
[WORKER-MODEL.md](../design/WORKER-MODEL.md)):

```yaml
workers:                       # absent = dynamic membership off (byte-identical)
  listen: "127.0.0.1:7400"     # private bind only (loopback/RFC1918/CGNAT/ULA)
  token_env: KIAN_WORKER_TOKEN # env-only shared token; the value never lives in the file
```

For per-device credentials, enroll workers by id (ADR-031); the worker declares its capabilities on `register` (ADR-032):

```yaml
workers:
  listen: "127.0.0.1:7400"
  enrolled_only: true          # only listed ids; forbids the shared token
  tokens:                      # id -> env var name (values stay env-only)
    iphone: KIAN_IPHONE_TOKEN
    mac-mini: KIAN_MAC_MINI_TOKEN
  queue_timeout_ms: 600000     # backend-level bound (global; see WORKER-MODEL §4)
  queue_max_redrives: 1        # re-drive retries before an unacknowledgeable row is abandoned
  queue_redrive_grace_ms: 5000 # postpone a re-drive that would race a running execution (ADR-037)
  release_after_ms: 300000     # idle-ended thread freed after 5 min; 0 = explicit release only
```

```bash
export KIAN_WORKER_TOKEN=...   # unset/blank refuses server startup (fail-closed)
# on the worker machine (reach the listener over an SSH tunnel or Tailscale):
cargo run -p kiand -- --register-to 127.0.0.1:7400 --endpoint-id mac-mini \
  --description "local macOS dev machine" --workspace-root . \
  --capability exec --capability fs          # registers over KiandProto v3; capabilities
                                            # are cached for list_workers / kian workers
kian workers                   # read-only operator view: id, state, capabilities, bound session
```

Joining makes the worker spawnable immediately (`list_workers` shows it `online`); leaving —
graceful `bye` or `kill -9` — marks it offline within `offline_after_ms`. Commands for a
known-but-offline worker park durably and run when it returns; an unknown id is refused naming the
known ids. Static `endpoints:` pins and dynamic workers share one catalog (a colliding id is
refused), and membership changes never touch a prompt or tool-schema byte.

Reaching a server on another machine: the listener accepts only a private bind, so a laptop reaches
a remote `kian serve` through an SSH tunnel (`ssh -N -L 7400:127.0.0.1:7400 <server>`, then
`--register-to 127.0.0.1:7400`) or over Tailscale — its `100.64.0.0/10` addresses are in the
private set. The enrolled example above uses the per-worker variables instead of the shared
`KIAN_WORKER_TOKEN`. `workers.allow_public: true` exists as an explicit escape hatch, but it exposes
a command-execution surface under the shared token (or an enrolled worker's own token); a tunnel is
the supported shape. A worker on the server itself is an ordinary static `endpoints:` pin (kian
dials it, so it needs no token), and the local worker's `--description` is what the Router sees in
`list_workers` when it picks a machine.

## Postgres and multi-replica

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
Setup and the replay: [docs/runbook-postgres.md](../docs/runbook-postgres.md).

## Embedded SDK

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

## Harness bridge (dsh)

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
[`contrib/dsh-v0-peer/`](../contrib/dsh-v0-peer/) against real dsh 0.2.1-rc.5 and real
`deepseek-chat` — full mapped chain, honest `Settled`. Re-run commands and the survey:
[docs/runbook-dsh.md](../docs/runbook-dsh.md).

## Harness bridges (Claude Code / Codex)

The same mount point runs Claude Code (`stream-json`) or Codex (`app-server`) as the worker
template's inner loop; the harness keeps its tools, sandbox and model, kian keeps the session,
control and audit. Default off; a deployment may declare several harnesses and select one per
worker agent (`superagent.worker.harness` / `superagent.roster[].harness`) — a bridged agent owns
its tools (no `tools:`, no `exec_worker`), and a config with no selection keeps the legacy
single-harness rules ([ADR-049](../design/decisions/ADR-049-per-agent-harness-selection.md)).

```yaml
superagent:
  harness:
    claude:                         # or codex:
      command: /usr/local/bin/claude # or export KIAN_CLAUDE_BIN
      model: claude-sonnet-4-5      # passed to the harness; the harness owns the call
      approvals:                    # the harness's own tool gate, answered by kian
        default: deny
        allow: ["Read", "Bash(*git status*)"]
```

Per-agent harness selection — one Router entry, different worker loops:

```yaml
superagent:
  harness:
    codex:
      command: /usr/local/bin/codex   # or export KIAN_CODEX_BIN
  worker:
    agent: worker
    harness: kian                    # the default worker template's loop
  roster:
    - id: codex-agent
      agent: codex-w                 # an AgentDef with tools: []
      harness: codex                 # this entry's loop
      tools_allow: []
      mandate_template: { scope: codex work, strength: inferred }
      limits: { max_children: 0 }
```

```yaml
superagent:
  harness:
    codex:
      command: /usr/local/bin/codex # or export KIAN_CODEX_BIN
      approval_policy: on-request # `never` refuses boot (it removes the gate)
      # approvals: { default: ask, durable_ask: true }   # ADR-050: durable human gate
      approvals:
        default: deny
        allow: ["commandExecution(*git *)"]
```

The stream records the session's class: a spawned worker's `session/created` carries
`bridge:{name,request_bytes}` ([EVENT-MODEL](../design/EVENT-MODEL.md)), so an audit export states
whether the model request is byte-replayable. Phase-1 limits are the contract's, not a bug: no
`step/request` (no cache/cost audit), no cross-process harness resume (a later input starts a
fresh harness conversation), and `ask` approvals refuse boot until the phase-2 confirmation
design. Start the harness with a gated
approval mode — a bypass flag refuses boot. Contracts, mappings and the capability matrix:
[HARNESS-BRIDGE](../design/HARNESS-BRIDGE.md), [ADR-043](../design/decisions/ADR-043-multi-harness-bridge.md);
live smokes: [docs/runbook-harness.md](../docs/runbook-harness.md).

## Tool permissions (per-call governance)

```yaml
agent:
  tools: ["fs_read", "fs_write"]
  permissions:
    fs_read: allow
    fs_write: deny
```

The decision is evaluated **before** the call is logged and recorded as provenance on `tool/call`
([ADR-015](../design/decisions/ADR-015-tool-permission-provenance.md)); a deny never invokes the tool —
the model receives an honest error result and reacts. Unknown values and keys outside `tools:`
refuse to start. Absent table = empty = allow-all, byte-identical to a server without the feature.

`ask` defers instead of deciding:

```yaml
agent:
  tools: ["send_email", "read_issue"]
  permissions:
    send_email: ask
confirmation:
  ttl_ms: 86400000        # absent/0 = answer whenever
```

The session goes `idle` with end reason `awaiting_input`; `kian pending <uuid>` lists the call and
`kian confirm <uuid> <call-id> --approve|--reject` answers it (or the `confirm`/`pending` RPCs).
Approving invokes the frozen call and appends its result; rejecting appends the honest error.
Contract: [CONFIRMATION-MODEL](../design/CONFIRMATION-MODEL.md)
([ADR-036](../design/decisions/ADR-036-durable-confirmation-plane.md)). Phase 1 supports plain
sessions — `ask` on a superagent worker agent refuses to start until phase 2.
## Hook plugins (`sidecars:`)

Operator-trusted sidecar processes subscribe to harness hooks over a stdio JSON-lines ABI
([PLUGIN-SPEC](../design/PLUGIN-SPEC.md)): `tool/pre` can deny a side effect before it happens — a ReAct tool or a harness-gated Claude
Code/Codex tool alike (the honest error says which plugin and why) — and every port is
observable. Deadlines, digest-pinned
spawns, quarantine and fail-closed defaults are the contract's own. Any language works — the
reference approval gate is a small Python stdlib script:

```yaml
sidecars:
  plugins:
    approval-gate:
      manifest: "/etc/kian/plugins/approval-gate.toml"
  scopes:
    server: ["approval-gate"]   # every session
```

Absent the `sidecars:` section, behavior is byte-identical — no bus, no spawns.

## Agent memory (notes + dreaming)

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
([design/MEMORY-MODEL.md](../design/MEMORY-MODEL.md) §6.)

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
([design/MEMORY-MODEL.md](../design/MEMORY-MODEL.md) §7).

## Read model (versions, snapshots, views, retention)

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

## Scheduled turns (`superagent.cron`)

```yaml
superagent:
  cron:
    schedules:
      - id: night-audit
        expr: "30 3 * * 1-5"          # or an @alias: @daily, @weekly, ...
        tz: Europe/Berlin
        target: { channel: lark, topic: ops }
        prompt: "audit the repo and summarise drift"
        worktree: true                # detached per-firing git tree
    harness:                          # optional; default is `dsh --profile headless "<prompt>"`
      raw: true                       # any harness: argv = args + "<prompt>" (no --profile)
      command: /usr/local/bin/claude
      args: ["--print", "--output-format", "text"]
```

Every due slot is claimed once in `cron_firings` and runs as an ordinary turn — on the resident
engine, or in a detached worktree when `worktree:` is declared (disk guards and
`keep_on_failure` apply). Inspect and drive it from the CLI:

```bash
target/debug/kian cron list   --config kian.yaml            # declarations, next fire, last outcome
target/debug/kian cron logs   --schedule night-audit --limit 20
target/debug/kian cron run-now night-audit --config kian.yaml   # one explicit slot
target/debug/kian cron rm     night-audit --config kian.yaml    # drop the store row
```

Semantics: [design/CRON-MODEL.md](../design/CRON-MODEL.md); decision:
[ADR-034](../design/decisions/ADR-034-cron-scheduler.md).

## Operator surfaces

| Surface | What it does |
|---------|--------------|
| `kian tickets [--json]` | Read-only Ticket stage board: `queued`, `working`, `awaiting-answer`, `awaiting-review`, `delivery-retry`, `blocked-failed`, `idle` |
| `kian board [--json]` / `kian tasks --all [--json]` | Read-only delegation task board: one row per task (task_id, assignee, attempt, outcome, gate); refuses with exit 2 while `superagent.board.enabled` is false |
| `kian workers [--json]` | Read-only worker catalog view: id, state, bound session |
| `kian usage-stats [--json]` | Provider token usage projected from `turn/end` (no pricing — the provider's own numbers) |
| `kian session inspect <id>` | Projected state, counters, usage lines, pending control intents |
| `kian session replay <id>` | Replay the stream and verify invariants and byte-identical context |
| `kian session export <id> --out audit.jsonl` | Audit package; `--with-event-ids` adds deterministic derived event ids |
| `kian cache-stats [--json]` | Prefix-cache stability report from the durable stream |
| `kian wake-now`, `kian lease inspect`, `kian recovery budget reset` | Operational actions |
| `GET /metrics` | Engine, cache, usage and super-agent counters |

Interrupt is durable: it admits a control-envelope inbox row (idempotent per epoch), so the intent
survives a drain crash and the takeover settles `interrupted`
([ADR-014](../design/decisions/ADR-014-durable-interrupt-intent.md)).

