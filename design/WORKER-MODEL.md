# Worker model

- **Status**: normative
- **Date**: 2026-11-08
- **Owns**: worker identity, membership, binding, release, queueing, dispatch, catalog,
  security, protocol compatibility, failure behavior, degradation, and the change rules for those
  semantics.
- **Does not own**: wire serialization details, event-envelope mechanics, storage implementation,
  process assembly, CLI presentation, or prompt wording. Those artifacts must realize the
  semantics in this specification without adding authority or weakening an invariant.

## 0. Contract invariants

| Id | Invariant | Testable consequence |
|---|---|---|
| W1 | Membership is live runtime state derived from an authenticated connection and its heartbeat. | After process restart, with no worker connection, every worker is reported `offline`; a durable cached state cannot make one online, routable, or bindable. |
| W2 | A worker id identifies a machine identity, while a worker session identifies one delegated execution. They are never interchangeable. | A catalog, queue row, and binding refer to the worker id; a thread, liveness decision, and 1:1 check refer to the worker session. |
| W3 | One worker has at most one live bound worker session. | Spawn admission refuses a candidate that already owns a live binding and names the owning session and thread. |
| W4 | Binding is immutable for a worker session. | No membership transition, reconnect, restart, release, or failed dispatch rewrites the binding to another worker id. |
| W5 | Every accepted input row belongs to a session that can still consume it, or admission was refused. | A submit racing a terminal close either lands before the close and makes release refuse `session_busy`, or is refused `session_terminal` after the close. No third outcome exists. |
| W6 | Delivery is at least once, settlement is once, and re-drive is bounded. | A command id can execute more than once after a transport failure, but only one result settles it and the dispatch-attempt count reaches a finite cap. |
| W7 | Worker-domain data never enters a prompt or tool schema. | Adding, removing, or changing a member changes no prompt or schema bytes; callers obtain membership only from `list_workers` results. |

The worker domain introduces no new event kind, durable membership table, inbound HTTP route, or metrics key. It defines one worker-release operation, `release_worker`, and the worker configuration surface below.

## 1. Terms and identity

- **Worker** is an execution endpoint on another machine. The `kiand` daemon and an embedded `kian-endpoint` process are carriers of one worker identity.
- **Worker id**, carried as `endpoint_id` and `endpoints[].id`, is an opaque non-empty string of 1–200 bytes with no control characters. It is identity, not address; restart, upgrade, or address change preserves it.
- **Worker session** is one delegated execution bound to a worker. Its catalog key is the worker id; its lifecycle key is the worker-session UUID in `thread/started`.
- **Member** is a live-directory entry containing connection generation, last heartbeat, protocol version, observed peer address, and accepted declaration. It disappears when the connection ends.
- **Durable registry row** is the persisted catalog record for an id. It stores last-observed state only as display cache, plus the last-declared description, protocol version, and capabilities. It is neither a member nor membership authority.
- **Binding** is the durable, session-immutable assignment of one worker session to one worker id. It is reconstructed from the committed thread-start fact on the parent stream, never from a mutable routing map.
- **Route** is the command channel for one authenticated registration generation. Exactly one transport generation owns a route, and only a route dispatches queue rows onto the wire.
- **Static worker** is a configured `endpoints:` entry whose address and description are configuration-owned; a **dynamic worker** registers itself over the listener.
- **Enrolled worker** is a dynamic id in `workers.tokens`. It authenticates only with its own token; under `workers.enrolled_only: true`, the enrollment map is the complete admission set.
- **Capability declaration** is the optional sanitized `capabilities` list in `register`; it may be cached for display. It proves neither installation nor execution authority.
- **Drain holder** is the single process permitted to drive a session. It is not a worker, route, member, or binding; one live drain never creates a second bound worker session.

## 2. Membership lifecycle

Membership truth is the live directory derived from the connection plus heartbeat. The durable registry row's `state` column is a last-observed cache used only for cold-start display. It is never consulted for admission, routing, busy checks, release eligibility, or catalog liveness. There is no separate membership table.

After a server restart, the live directory is empty. Registry-recovered rows are therefore displayed as `offline`; a cached `online` value may appear only as historical diagnostic metadata. Cached state cannot restore a route or authorize work.

The live member state is computed:

| State | Derivation | Permitted effect |
|---|---|---|
| `online` | An authenticated, protocol-compatible transport has a live connection and a heartbeat no older than `workers.offline_after_ms`. | The catalog shows online and spawn admission may bind the id. |
| `draining` | The worker has sent `bye` and its transport is closing. | No new dispatch starts; existing bookkeeping may settle. |
| `offline` | No live connection exists, the heartbeat is stale, or registration failed. | The catalog shows offline; a known id may receive a durable offline binding, whose commands park. |

A route is tied to a live authenticated registration generation, not to computed catalog state. A stale heartbeat with an open transport computes the member `offline`, but the route may remain attached until transport failure or replacement. Queue and per-command bounds make that ambiguity finite; the route grants nothing.

| Transition | Trigger | Required result |
|---|---|---|
| Join | Authenticated `register` on the listener | Serialize on the per-id slot, create a new member and route generation, replace any prior route in the specified order, and re-drive that id's `pending` and `inflight` rows. |
| Heartbeat | `heartbeat` on the current connection generation | Advance server-clock last-seen and refresh only liveness cache; never mutate binding, declaration, queue, or prompt data. |
| Heartbeat absence | No heartbeat for `workers.offline_after_ms` | Compute the member offline and admit the ordinary `endpoint_health` input to affected live worker sessions. Do not fabricate a result or reroute a command. |
| Graceful leave | `bye`, then transport close | Detach the route, show `draining` through teardown, then compute offline; retain durable queued rows and registry display data. |
| Crash | Transport loss without `bye` | Detach the route, compute offline, retain queued rows and the immutable binding, and write an audit line distinguishing the loss from graceful leave. |
| Replace | New authenticated registration for an online id | Under the per-id slot, close the old connection, detach its route, attach the new generation, increment the generation, and audit `replaced`. Outstanding rows re-drive through the ordinary queue rule. |
| Server restart | Process start | Start with an empty live directory, show all workers offline, rebuild bindings from worker streams, preserve registry display data, and park queued rows until routes return. |
| Worker restart or upgrade | Same id registers again | Reconnect with backoff, preserve the binding, and dispatch subsequent commands to the same id. |
| Version mismatch | Handshake observes an incompatible protocol revision | Refuse with `version_mismatch`, audit once, close the connection, and leave membership, bindings, and queue rows unchanged. |
| Static id collision | Dynamic registration uses an id pinned by configuration | Refuse and audit; the static pin keeps the id and its route. |
| Malformed id | Id violates the identity shape | Refuse with `id_conflict`, audit, and create no member. A malformed optional description is dropped and audited without rejecting an otherwise valid id. |

Replacement is serialized per id; two registrations cannot create two live routes for one id. Membership transitions write no membership event history. Static pins and previously observed dynamic ids remain catalog rows. Known ids are never pruned: keeping them visible makes an offline-known binding honest, while pruning would turn a park into an apparent authority transfer.

## 3. Binding and release

### 3.1 Binding is durable, derived, and immutable

- A binding is committed before the child's first command as one thread-start fact on the parent session's stream. That fact names the worker id and the worker session id, and replay reconstructs the binding from it; there is no separate routing map.
- A thread-start fact written by a dispatcher before spawn admission is a reservation, not a live binding. It becomes the live binding only when admission accepts and a child exists; until then it is a reservation bounded by its lease.
- Join, leave, replacement, restart, upgrade, queue failure, and release never rewrite, reorder, or re-emit the binding.
- Routing selects the bound worker id afresh for each drive from the committed stream, so restart correctness follows from replay correctness.
- The store-wide binding view is derived from committed thread-start facts whose worker session is non-terminal and has a live child. It is an observability and selection optimisation surface, never authority.

### 3.2 One worker, one live binding

One worker has at most one live bound worker session. Busyness is based on the live worker session,
not merely on the connection:

- `completed` and `cancelled` release the worker.
- `interrupted` and `failed` hold the worker while the thread remains resumable.
- A never-started reservation holds the worker only until its reservation lease expires.
- An offline worker with a live bound worker session is still busy.

Placement and selection are advisory. A dispatcher that cannot observe a live binding may
optimistically select a busy worker. Spawn admission is the only refusal authority: it rechecks the
derived live binding and refuses the candidate, after which the dispatcher may try the next
candidate. The optimistic selection creates no binding and is not a second worker session.

No component may treat the derived store-wide view, a catalog row, a cached state, or capability
declaration as permission to spawn. Only the serialized spawn admission point may create the
binding.

### 3.3 Release is a terminal close

Release closes an idle, resumable worker session; it does not kill a process, interrupt a live drain,
cancel a command, or transfer work. A successful release appends `drain/claimed` and
`session/ended{cancelled}` atomically under a fresh claim epoch and the expected-sequence compare-and-
swap. `cancelled` is terminal, so the released session can never resume, rebind, reroute, or execute
again.

Release proceeds only when every gate holds. The list below is the gate set; each gate is
re-evaluated inside the per-session admission/release critical section immediately before the terminal
append, and only that inside-lock evaluation is authoritative:

- The session projection is `idle`.
- The session's last event is a resumable end (`interrupted` or `failed`); a session that has
  never ended is not releasable.
- The inbox has no unconsumed input row, whether or not it is eligible yet. A pending row whose
  eligibility time is in the future still blocks release; closing the session would strand it.
- No drain is live.
- The bound worker's durable queue has no live `pending` or `inflight` row.

Release has two triggers: explicit `release_worker`, exposed as `kian workers release <id>`, and automatic reclaim after `workers.release_after_ms` from the resumable end timestamp. The reclaim default is `300_000` ms and `0` disables it.

Each successful release writes one audit line and records a `released` object on the terminal end.
Refusals are typed and side-effect free:

| Refusal | Meaning |
|---|---|
| `session_busy` | The thread is running, has an unconsumed input row (eligible or not), or still owns a live queue row; nothing was closed. |
| `already_released` | The thread is already terminal; the operation is an idempotent no-op. |
| `worker_not_bound` | The id is unknown, has no live binding, or names only a never-started reservation. |

### 3.4 Admission and release linearize per session

The store serializes input admission and the terminal close for one session under one per-session write lock or transaction. Admission checks the projected terminal state inside that critical section; a terminal session is refused `session_terminal`.

Every release gate is re-evaluated inside that same critical section immediately before the append; checks performed before entering it are advisory and cannot authorize a close. The authoritative release act is the atomic append of `drain/claimed` plus `session/ended{cancelled}` under the claim epoch and expected-sequence compare-and-swap, performed in the same per-session critical section as admission. A concurrent submit therefore either lands before the in-lock check, making an unconsumed inbox row (eligible or future-ineligible) observable and causing release to refuse `session_busy`, or arrives after the terminal append, causing admission to refuse `session_terminal`.

The no-live-drain gate is fenced by the same expected-sequence append: a drain claim is itself an
append at that sequence, so a claim and a release cannot both commit. The loser re-reads the stream
and either refuses or re-evaluates its gates from fresh state.

No accepted-but-unconsumable inbox row can exist. This is invariant W5: every accepted input row belongs to a session that can still consume it, or its admission was refused.

Acceptance is a race test that starts submit and release concurrently against one session and repeats both orderings. Every run must observe exactly one outcome: accepted input with no terminal append and release refusing `session_busy`, or a terminal append with `session_terminal` and no accepted input. The test rejects any interleaving that accepts input and then closes without release observing `session_busy`. A second test admits one unconsumed row with a future eligibility time, then requests release: the release refuses `session_busy`, and after the row becomes eligible the session can consume it or release again.

### 3.5 Release does not provide liveness

Release cannot preempt a running or recoverable drain. A worker remains bound until its worker thread ends. A wedged command is bounded by its worker-side execution timeout and the queue's dispatch, re-drive, and caller-wait bounds; a wedged model call is bounded by the provider's finite connect timeout and per-gap idle timeout. A drain that repeatedly loses its holder is converged by the bounded takeover chain rather than retried forever.

After `MAX_UNENDED_CLAIMS` distinct un-ended claims, takeover writes one honest `session/ended{failed}` instead of taking over again. Release may then close the resumable failure. Worker liveness is therefore provided by bounded per-command queue and execution timeouts, bounded provider connect and idle-gap timeouts, and the bounded takeover chain. It is not provided by release. A provider idle bound configured as `0` removes the per-gap bound and is outside a deployment profile that claims bounded worker liveness.

### 3.6 Offline binding and no failover

A spawn may bind a known but offline worker. The binding and command are durable; the command parks until a route appears or the honest queue timeout ends it. The spawn result and one audit line say `offline — commands will queue`.

A worker going offline never releases a binding, reroutes a command, executes it locally, or hands it to another worker id. A permanently lost worker produces the honest queue timeout and a parked session. There is no binding failover.

### 3.7 Legacy unbound sessions

When workers are configured, a worker session created without a binding keeps the legacy first-endpoint route and emits one `legacy unbound` audit line. It is never retroactively bound, counted as a live worker binding, or treated as a candidate for cross-worker dispatch. With no workers configured, the worker feature is off.

## 4. Command queue and dispatch

### 4.1 Queue before dispatch

Every `exec`, `fs_list`, `fs_read`, and `fs_write` command creates a durable queue row keyed by the session's bound worker id before any transport write. The row carries the command family, command id, command payload or durable reference, attempt count, and current state.

One queue serves both families. A missing route is not an admission error: the row remains `pending` until a route appears or it becomes `abandoned` at `queue_timeout`. Settlement is durable; a duplicate command id reads the prior settled outcome, including the same timeout.

Attaching a route re-drives the id's `pending` and `inflight` rows; detaching leaves them durable. Every driver filters by its own worker id; there is no fallback route, any-online candidate, local substitution, or cross-worker dispatch.

### 4.2 At-least-once delivery, once-only settlement

A command may be dispatched again after a crash or reconnect. Its command id is the dedupe key, and exactly one durable result settles the row.

The worker also dedupes by command id. Before sending a result, it records the result in a bounded local cache with a 24-hour TTL. A re-dispatched cache hit is answered without re-execution; a duplicate arriving during the first execution joins it. A cache miss, oversized result, or disabled cache re-executes the command and never fabricates a result.

Filesystem command replay preserves its payload: `fs_list` and `fs_read` are idempotent, and repeated `fs_write` uses the same path and bytes. Modification time may move; path and payload may not change.

### 4.3 Bounded re-drive and NotWritten refund

Every effective dispatch increments `dispatch_attempts`. A `pending` row always dispatches. An `inflight` row re-drives only while attempts are below `1 + workers.queue_max_redrives`; the default is `1`, the ceiling is `16`, and `0` means no re-drive after the first dispatch.

Within `workers.queue_redrive_grace_ms` of the last dispatch, the driver waits for the remaining grace before re-sending. The default is `5_000` ms, the ceiling is `60_000` ms, and `0` disables the delay. Grace postpones dispatch only; it never settles a row or changes a timeout.

After the re-drive bound is spent and no live lease holds the row, the driver converges `inflight` to `abandoned` and writes one audit line naming the command id, worker id, attempt count, and tool-call reference. The command may have run; the result is unknown and no result is invented.

An attempt counts only after its frame leaves the process. If the writer is already closed, transport reports `NotWritten`, marks the route generation unusable, and the queue refunds the charged attempt. Without the refund, a worker reconnecting after a dead write could find a command abandoned even though it provably never received it.

### 4.4 Honest, phase-specific waits

A wait names what it actually observes:

| Phase | Row state | Bound and result |
|---|---|---|
| Dispatch wait | `pending` | `queue_timeout`. Expiry moves `pending` to `abandoned`; no worker received the command. |
| Execution wait | `inflight` | The command's own `timeout_ms` bounds worker execution. The caller's upper bound is `2 * exec_bound + queue_timeout`, covering the first dispatch, one at-least-once re-dispatch, and slack. |
| Model call | live drain | Provider `connect_timeout_ms` bounds connection establishment; default `10_000` ms. Provider `idle_timeout_ms` bounds response headers and each body-chunk gap; default `300_000` ms. The idle bound is not a total-generation bound. |

The backend's dispatch bound is the maximum of static
`endpoints[].queue_timeout_ms` and `workers.queue_timeout_ms`. A row's execution wait timeout does not
mutate the row: the caller stops waiting, while the queue row remains truthful and re-driveable.

The synchronous `exec` wait uses only the dispatch bound because it parks a drain. The asynchronous
`start` observer uses both dispatch and execution bounds because nothing is parked. `exec` refuses a
`timeout_ms` larger than its own wait bound before creating a row or dispatching, so a configured
command cannot be represented as a kill it did not cause.

A remote wait expiry reports what it observed:

- `row_state: pending` means the command was never dispatched and is now abandoned.
- `row_state: inflight` means it started and may still be running; this caller discarded the result.
- `row_state: abandoned` means a prior abandonment is being answered from the durable row.
- `still_running` says whether execution may continue.
- `row_abandoned` says whether the durable row is abandoned.
- `bound_ms` identifies the wait bound in force.

An asynchronous job observer settles its job row rather than a tool result. A dispatch-phase expiry
settles the job row `abandoned`. An execution-phase expiry also settles it `abandoned` while leaving
the live queue row untouched. `stop` remains the command's explicit escape hatch.

### 4.5 Budget stop and caller attachment

Iteration-budget exhaustion is an intentional stop. The worker records
`thread/completed{status: "interrupted"}` with the stop reason as the summary. It never records
`completed`, because the last assistant turn may be only a pending tool call with empty text. The
following `session/ended{completed}` decides binding liveness and may release the worker.

A resumable interrupted end that does not release the worker holds it until explicit release or
automatic reclaim. It is never silently released while it can resume.

A caller waits on the durable command id, not on a route, connection, or command frame. A detach,
attach, replacement, restart, or reconnect does not sever the wait.

## 5. Catalog, discovery, and spawn authority

The catalog is runtime data: the union of configured static ids and every id in the durable registry.
It is not configuration-only and never prompt content.

`list_workers` is Router-only, read-only, has no arguments, and writes no event, row, wake, or
dispatch. It returns rows ordered by id with:

- worker id;
- optional description;
- computed live state `online`, `offline`, or `draining`;
- protocol version;
- optional non-empty worker-declared capabilities;
- the live bound worker session and thread, when one exists.

For a registry-only id, description, protocol version, and capabilities may come from the
last-declared cache, and the row includes `declaration_stale: true`. That flag describes declaration
data only. It cannot change the computed state, make the worker bindable, or make a claim about the
worker's live execution authority. A live registration omits the flag. An empty declaration omits
its optional field.

Spawn validation has this order:

1. argument shape;
2. feature-off check when no workers are configured;
3. unknown-id refusal, naming known ids;
4. 1:1 busy refusal, naming the owning session and thread;
5. offline-known handling, with the queue note;
6. follow-up ownership;
7. speculation cap;
8. mandate;
9. mint, lineage, and admission.

Every refusal occurs before any side effect. Only step 9 may create the durable binding. Catalog
selection before step 4 is advisory and may race with another admission.

`spawn_worker.exec_worker` is a required plain non-empty string whenever membership is enabled. It is
never an enum or a catalog object. The prompt contains one frozen instruction to call `list_workers`
and never invent an id. Joins, leaves, state changes, and declaration changes therefore alter no
prompt or schema byte. Capabilities appear only in result data.

## 6. Security boundary

The worker listener binds only loopback, RFC1918, CGNAT `100.64.0.0/10`, or IPv6 ULA addresses by
default. A wildcard or public bind refuses startup unless `workers.allow_public: true` is explicitly
configured. Remote access is expected through an SSH tunnel or private overlay network.

Tokens are environment-only:

- `workers.token_env` names the shared token's environment variable.
- `workers.tokens` maps an enrolled worker id to its own token variable name.
- Startup refuses when a named variable is absent or blank.
- Token values are compared in constant time and are never written to configuration, argv, logs,
  events, errors, or metrics.
- Token values use a redacting representation so formatting cannot print them.
- Failed authentication attempts are bounded per connection and per peer; every refusal is audited
  without secret material.

An enrolled id authenticates only with its own token; the shared token never replaces it. With
`workers.enrolled_only: true`, only listed ids are admitted. Strict mode requires a non-empty
`workers.tokens` map and forbids `workers.token_env`. In hybrid mode, an unlisted id may still use the
shared token, so enrollment narrows the shared-token limitation without eliminating it.

The listener is a KiandProto socket, not HTTP. The worker domain adds no inbound HTTP surface, and a
worker receives no control-plane authority. The filesystem root is a lexical guard, not a sandbox.
The v2 filesystem commands resolve under `--workspace-root` and refuse `..`; `exec` already runs
arbitrary programs as the same user, so filesystem commands add no authority. A symlink may still
point outside the root.

Per-worker bearer tokens remain identity claims, not cryptographic machine attestation. Without mTLS,
a deployment must treat a leaked token as full impersonation of that worker id.

## 7. Protocol version and upgrade policy

The worker wire protocol is KiandProto v3. Both halves declare `PROTOCOL_VERSION = 3`
independently and pin golden frames. The worker-domain frame set consists of registration
(`register`, `welcome`, `heartbeat`, `bye`), command frames for `exec` and the v2 filesystem family,
their correlated result frames, and the closed typed rejection set. Registration carries the peer's
protocol revision and optional capabilities; `welcome` carries the server's revision and heartbeat
cadence.

Kian upgrades before workers. During the mixed-version window:

- A v0 static endpoint is refused with a typed terminal version error and an audit line; its route
  stays offline and no fake connection is reported.
- A v0 worker that cannot use registration remains offline.
- A v1 worker that lacks the filesystem frames is refused `version_mismatch` until upgraded.
- Parked commands remain parked; none is lost or sent to a wrong machine.

A peer whose declaration does not match the accepted revision is refused
`version_mismatch`; it is never accepted into a half-compatible state. No compatibility shim
translates between protocol revisions. The v0 and v1 steady states differ by more than a no-op
integer rewrite, and silent translation would make a partially upgraded deployment appear healthy.

## 8. Failure and degradation

| Condition | Detection | Required behavior | Blast radius |
|---|---|---|---|
| Worker absent or offline | No route, or live state offline | Allow spawn only for a known id; park commands durably until route return or `queue_timeout`. | That worker's sessions and commands |
| Transport loss during command | Connection closes | Detach route, compute offline, leave the row truthful (`inflight` or re-driveable), and resume by command id on reconnect. | That worker |
| Re-dispatch after execution | Command id found in worker result cache | Return cached result without re-execution; on cache miss or eviction, execute again. | That command |
| Heartbeat silence with open transport | Last heartbeat exceeds `workers.offline_after_ms` | Compute offline, admit `endpoint_health` to affected live worker sessions, and retain the route until transport loss or replacement. | That worker |
| Newer registration for a live id | Authenticated `register` | Serialize replacement: close old transport, swap route, increment generation, audit. | That id |
| Dynamic id collides with static pin | Registration validation | Refuse and audit; keep the static pin and route. | That id |
| Malformed registration | Identity or payload validation | Refuse malformed id with `id_conflict`; drop and audit malformed optional description; create no state. | That connection |
| Incompatible protocol | Handshake | Refuse `version_mismatch`, audit, close, and preserve parked work. | That worker |
| Bad or absent token; unlisted id in strict mode | Authentication | Refuse `bad_token`, audit, allow one attempt per connection, and rate-limit closes after the peer window; strict mode does not reveal enrollment state. | That peer |
| Named token variable missing or blank | Startup validation | Refuse to start `kian serve`. | Whole configured process |
| Public or wildcard bind without override | Startup validation | Refuse to start `kian serve`. | Whole configured process |
| Server crash or restart | Process start | Empty live directory, all workers offline, bindings rebuilt from streams, queue rows preserved, and routes recreated by registration. | All workers transiently |
| Listener shutdown | Shutdown | Send `bye` where possible, detach routes, preserve queued rows, and refuse new registrations. | All workers |
| Filesystem path escapes root | Server lexical check, then worker re-check | Return a typed `io` result or `InvalidCall` before any write; never write outside the intended path. | That call |
| Filesystem body over 1 MiB or non-UTF-8 read | Worker bound or decode | Return a typed `io` tool error; never truncate or partially write. | That call |
| Queue timeout | Timer | Move `pending` to `abandoned`; duplicate command ids receive the same timeout. | That command |
| Drain dies or model call stalls | Lease expiry, provider timeout, or panic cleanup | Unwind bookkeeping, let the bounded takeover chain take over, and settle one honest end; never continue hiding a lost holder. | That thread; no command moves |
| Bound thread ends `interrupted` or `failed` | Idle resumable projection with no live inbox or queue row | Hold the worker until explicit release or `workers.release_after_ms` reclaim. | That thread; no command moves |
| Release sees running work or any unconsumed input row (eligible or future-ineligible) | Gate failure | Refuse `session_busy`, append nothing, and let the caller wait or interrupt before retrying. | That thread |
| Release sees an already terminal thread | Terminal projection | Return idempotent `already_released` and append nothing. | That thread |
| Release targets an unknown or unbound id | Binding lookup | Return `worker_not_bound` and append nothing. | That id |
| Child ignores graceful termination | Worker execution timeout | Enforce the command's configured kill bound and return a timeout result; release does not force-kill the child and cannot free the binding before the thread ends. | That command and worker session |

No row in this table converts an abort, crash, missing result, timeout, or interrupt into
`completed`. No degradation path invents a result, silently drops a command, or fails a command over
to another worker.

## 9. Boundaries and non-goals

- **One kian instance.** A worker registers to exactly one `kian serve`. There is no federation,
  relay, multi-register mode, or cross-instance dispatch.
- **No live rebind, retirement, or worker takeover.** Reconnect is valid only for the same worker id.
  A lost worker produces timeout and a parked session. Release terminates the old worker session and
  mints no replacement binding.
- **Release is not force-kill.** A running or recoverable drain cannot be released. `interrupt` is
  the stop surface and must settle before release. A successful release cannot be resumed, and the
  queue has no live row at the close, so release has no command to cancel.
- **No mTLS or per-worker certificates.** Per-worker bearer tokens narrow the shared-token risk but
  are not machine attestation. Token rotation requires changing the environment value and restarting
  the relevant process.
- **No worker pools.** The one-live-binding rule remains the unit of concurrency.
- **Capabilities are declarations, not authority.** The server sanitizes
  `[a-z0-9._-]`, at most 16 unique entries, and caches the accepted list for display. A wrong or
  stale declaration may mislead a selector; it grants nothing. A selector may use a capability as an
  eligibility hint, but the worker's own tool grants and sandbox enforce execution.
- **No per-worker queue timeout or failover.** `workers.queue_timeout_ms` is a backend-level bound.
  The effective bound is the maximum of static endpoint bounds and this value.
- **No capability-based spawn authorization.** Spawn eligibility is identity, one-live-binding,
  ownership, speculation, and mandate. A declaration does not bypass any of them.
- **The worker filesystem surface is text-only.** `fs_list`, `fs_read`, and `fs_write` have 1 MiB
  bounds and use `--workspace-root` with a lexical `..` guard. Binary bodies, streaming or large
  files, `fs_edit`, glob, symlink-proof sandboxing, and filesystem tools on unbound Router sessions
  are outside this domain.
- **No membership data in prompts or schemas.** `list_workers` results are the only membership
  channel. The last-declared cache never participates in identity, liveness, bindability, routing,
  spawn legality, or release eligibility.
- **No public listener by default.** Hosted multi-tenant identity is not claimed by the bearer-token
  boundary.
- **No new event kind, membership table, HTTP route, or metrics key.** The durable model is the
  existing worker-session stream, command queue, and one registry row per known id. There is no
  separate membership table.
- **No new operational surface beyond release.** The worker domain defines `release_worker` and the
  configuration keys `workers.listen`, `workers.token_env`, `workers.tokens`,
  `workers.enrolled_only`, `workers.allow_public`, `workers.offline_after_ms`,
  `workers.queue_timeout_ms`, `workers.queue_max_redrives`,
  `workers.queue_redrive_grace_ms`, and `workers.release_after_ms`. The numeric defaults are
  `offline_after_ms: 30_000`, `queue_timeout_ms: 60_000`, `queue_max_redrives: 1`,
  `queue_redrive_grace_ms: 5_000`, and `release_after_ms: 300_000`; the remaining keys are
  flags or environment-variable names with no numeric default.

## 10. Change protocol

| Change | Treatment |
|---|---|
| Worker identity, membership state, pruning, or binding policy | Change this specification first; then update implementation, durable schema, and tests. A change to the reason or rejected alternatives requires a separate decision record. |
| A worker frame, field, typed error, rejection reason, or protocol version | Define the worker-domain meaning here and update both protocol implementations, golden frames, public client fixtures, and this section in one logical change. |
| A `workers:` configuration field or default | Update this specification, configuration validation, catalog or CLI surfaces, examples, and fail-closed tests in one change. |
| Binding, release, admission linearization, or reclaim semantics | Update this specification, the per-session critical section, claim/append implementation, operation surface, payload evidence, and concurrency tests in one change. |
| A worker command family or dispatch semantic | Update this specification, queue row schema, command and result handling, dedupe behavior, timeout bounds, and transport integration tests. |
| A durable registry column or declaration-cache change | Prove it remains non-authoritative, update this specification, the registry schema and migration, storage tests, and catalog output together. |
| A new event kind or authoritative table | Requires an explicit architectural decision before implementation; this worker specification may define the semantics but may not silently expand durable truth. |
| Assembly, crate placement, CLI formatting, or presentation | Implement without changing worker authority; a presentation change must still expose the refusals, defaults, and state meanings defined here. |

Three rules bind every worker-domain change:

1. Membership remains live runtime state; no durable cache becomes admission or routing authority.
2. Binding remains immutable and reconstructible from its committed thread-start fact; no new
   mutable routing table appears.
3. No accepted input row survives a terminal close without being consumable, and no deployment
   path converts loss into fabricated completion.
