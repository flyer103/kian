# ADR-020: Durable per-session execution-worker binding

- **Status**: Accepted (owner decisions, 2026-11-08; spec stage — implementation lands behind this
  contract)
- **Date**: 2026-11-08
- **Scope**: bundle (`kian-superagent`), CLI/assembly (`kian-cli`), docs — additive payload keys on
  two existing events, one additive config field, prompt/tool-schema text. No core, no store, no
  orchestrator, no wire surface, no new event kind.
- **Supersedes**: none
- **Related**: AGENTS.md constraints 1/2/3/5/7/10; `design/DESIGN.md` §4.1/§4.3/§4.7;
  `design/EVENT-MODEL.md` §3 (`thread/started`), §4.2 (additive provenance keys), §6.3;
  ADR-007 (kiand delivery semantics — the durable queue the binding rides), ADR-012 (delegation
  mandate — the additive-wrapper-key precedent), ADR-006 (model slots are session-immutable — the
  same immutability rule applied to the execution worker), ADR-009 (claim admission — the
  single-drain exclusivity the 1:1 check leans on)

## Context

A delegated worker session runs its agent loop on the `kian serve` side and executes `exec`
commands through `ExecBackend`. With `endpoints:` configured, every configured endpoint (a `kiand`
daemon) gets a `RemoteExecTool`, every one of those tools is named `exec`, and `CombinedTools` is
first-wins — so today only the **first** endpoint is model-reachable no matter how many are
configured (`crates/kian-cli/src/plugins.rs`, `crates/kian-cli/src/superagent.rs` §"CombinedTools").
There is no record of *which* machine a worker session was meant to run on; the choice is implicit,
un-auditable and unenforceable.

The owner's direction (2026-11-08) is explicit and narrows the design to one shape:

1. The local machine can be a worker, Vultr can be a worker; **each worker binds to one session,
   1:1**; a worker may die, restart or migrate and still reconnect to the session it was bound to.
2. Land the feature first; deployment is discussed after.

Pinned by the round brief: the worker shape is the **kiand execution endpoint** (owner choice C —
the loop stays on the `kian serve` side, `endpoints[].id` is the worker identity, no new protocol, no
new binary), the binding is **durable 1:1** (owner choice A — derived from the session's own stream,
immutable for the session, only that worker executes its commands), the assignment is **Router
chosen per task** (owner choice A — model-visible, audited in the delegation record, catalog
advertised in config), and **backward compatibility is mandatory** (a config without `endpoints:`
stays byte-identical).

The design constraints that decide the *how*:

- Constraint 1: session state is a projection of the event stream. A binding held only in a resident
  map would be a second source of truth and would not survive a restart.
- Constraint 2/3: the delegation wrapper the worker consumes is a persisted message; anything
  model-visible about the assignment must already be a recorded event, and history is immutable.
- Constraint 10 (narrow waist): the routing decision belongs to the bundle/assembly, not to a new
  `kian-core` seam.
- ADR-012's precedent: the delegation wrapper already carries additive, immutable objects
  (`mandate`, and the `speculative` marker) that ride verbatim into both sides' `thread/started`.

## Decision

1. **The binding is additive bytes on the two existing delegation records.** The delegation wrapper
   (the worker's first message `content`) gains the additive key `"exec_worker": "<endpoint id>"`
   and the `thread/started` payload gains the same additive top-level key. Both sides (Router and
   worker) carry identical task-message bytes, as they already do. `worker` in that payload keeps
   its meaning (the worker **session** uuid); `exec_worker` is the execution **worker id** and the
   two are never conflated. Nothing else about either payload changes: absent key = legacy unbound,
   absent key = byte-identical legacy bytes.
2. **One name on every surface.** `exec_worker` is the config identity reference, the wrapper key,
   the `thread/started` key, the `spawn_worker` argument and the spawn tool result echo. Value is
   the configured `endpoints[].id`; it is treated as an opaque non-empty string everywhere.
3. **The binding is derived, never resident.** Routing selects a worker side per drive from the
   session's own stream (`thread/started` naming that session, or the first input wrapper); there is
   no routing map, no binding table, no resident choice. Restart rebuild therefore equals replay by
   construction, and the durable record is the two `thread/started` events.
4. **Routing is per-drive selection of a prebuilt worker side.** Assembly builds one
   `WorkerLoopDriver` per configured worker (each mounting that worker's `RemoteExecTool` as the
   single `exec`, plus that worker's async pair, recall and local tools) and the existing
   `SuperAgentDriver::drive` selects the side from the stream's binding before the first provider
   turn. `kian-core` and the `LoopDriver` seam are untouched; the worker's tool surface stays one
   `exec` tool with unchanged schema.
5. **1:1 exclusivity is a stream-derived check at spawn time.** A worker (by id, never by address)
   has at most one live bound worker session. `spawn_worker` refuses a spawn naming a busy worker
   **before any side effect**, naming the owning worker session and thread. Liveness is the existing
   worker-liveness vocabulary — `thread/completed` and `session/ended` on the *worker's own* stream:
   `completed`/`cancelled` release the worker; `failed`/`interrupted` are resumable and still live.
   To make that checkable without a resident map — and to close the crash window the brief names —
   `spawn_worker` writes the Router-side `thread/started` **before** admitting the task row, so a
   spawn that dies in between leaves a reserved-but-never-started thread rather than an unrecorded
   live session, and a never-started reservation is honestly released after the configured
   `queue_timeout_ms` with an audit line.
6. **Matching stays per worker, not global.** The worker catalog (`id` + optional human
   `description`) is configuration; the Router picks per task; the schema advertises the catalog and
   the bootstrap prompt explains the choice. Both are prompt-prefix content: **new** sessions pin a
   new `superagent.policy.version`; persisted prompts and persisted requests are never rewritten.
7. **Legacy is a documented mode, not a silent path.** Zero `endpoints:` = feature off, byte
   identical (no wrapper key, no payload key, no schema property, no prompt block, local exec). A
   pre-change worker session with no binding on a server that now has workers keeps the legacy
   first-endpoint route plus a "legacy unbound" audit line: never rewritten, never silently rebound,
   never counted as bound (I6 is not claimed for it).
8. **No new event kind, no new durable table, no new route, no `API-CONTRACT.md` change.** The
   durable footprint is two additive keys on events that already existed.

## Alternatives considered

| Alternative | Rejected because |
|---|---|
| A `worker/bound` event kind (or a `session_bindings` table) | A second durable record of a fact the delegation bytes already carry — constraint 1 (single truth) and constraint 2 (the wrapper *is* the message). It would also need the EVENT-MODEL change protocol for no new information |
| Resident `Arc<Mutex<HashMap<worker, session>>>` owned by assembly | Not replay-derived: a restart, or a second replica, would lose or disagree on the binding (constraint 1/6). The audit would then point at memory instead of at the stream |
| Bind by address (`addr`/tunnel host) instead of by `id` | The owner's reattachment semantic is *the same worker coming back*, and a migrated machine keeps its id while its address changes (stable tunnel, or an edited `addr` plus a restart). Address binding would silently split one worker into two identities, or bind a session to whoever answers an address |
| Let the model choose the endpoint per `exec` call (a per-call argument) | Violates the owner's 1:1 rule and makes the choice un-auditable per call; it would also rename/extend the worker's `exec` schema — a prompt-prefix change on the worker surface for no benefit. The session-level binding is the narrowest surface that expresses the choice |
| A new `kian-core` seam (e.g. an endpoint-aware `ExecBackend` routed by session) | The binding is a bundle/assembly concern; core would then have to know about endpoints and workers, violating constraint 10. Per-drive side selection needs no core change |
| Enforce exclusivity with a lease held by the worker session (TTL + heartbeat) | Leases *detect* liveness, they never grant permission (constraint 5). A lease would also make a durable policy fact (1:1) expire on a clock |
| Refuse `superagent.async_tools` whenever workers are configured | Over-broad: `async_jobs` already carries `endpoint_id`, so each prebuilt worker side can mount its own async pair bound to that worker. Refusing would break every existing `async_tools` + `endpoints:` deployment to prevent a violation that a per-side pair removes |
| Document the dsh harness as "uncovered" and keep booting | The dsh harness runs the loop inside the dsh child and never mounts the kian `exec` tool, so a recorded binding could not route anything: the audit would claim a binding the runtime cannot honour. Fail closed at config validation instead |
| Rebind a live session to a different worker this round | Owner scope (§5): the landed reattachment path is the same worker id restarting/migrating. A live rebind needs a new durable intent, a fencing rule for the old worker's queued rows and a projection rule for which worker owns which seq range — an EVENT-MODEL change, therefore an owner gate, not a spec decision (see Deferred) |

## Consequences

**Positive**

- The binding is auditable forever on both streams and costs two additive keys: no new kind, no new
  table, no wire change, and zero-endpoints deployments stay byte-identical.
- Today's multi-endpoint limitation is fixed by construction: a bound session has exactly one
  reachable `exec`, so `CombinedTools` first-wins stops being a silent cap on the catalog.
- Restart, replica failover and re-arm all rebuild the same binding from the same bytes; there is no
  routing state to lose, so "rebuild == replay" is not a test of a cache, it is the design.
- A worker dying, restarting or migrating (same id) is a non-event for the session: the durable exec
  queue re-drives on reconnect (ADR-007) and the binding never moves.
- Refusals are honest and specific (busy worker names its owner; unknown worker names the catalog),
  and each refusal happens before any inbox row, thread or provider call.

**Negative / risks**

- **Prompt-prefix cost at upgrade.** The `spawn_worker` schema and the Router bootstrap prompt
  change, so a resident Router pays one prefix invalidation on its first request after the upgrade,
  and operators must bump `superagent.policy.version`. Persisted prompts and requests are never
  rewritten, and the drift machinery reports the mismatch instead of hiding it.
- **A reserved-but-never-started thread** (crash between the lineage append and the task admit) can
  hold a worker until the `queue_timeout_ms` release, with an audit line. The stall is visible and
  bounded; the alternative (admit first) leaves a live session the Router-side scan cannot see,
  which is a silent exclusivity violation.
- **Legacy sessions stay legacy.** A pre-upgrade worker session is never rebound and keeps the first
  endpoint with an audit line; the mode is documented, not supported.
- **`superagent.harness.dsh` + `endpoints:` now refuses to boot.** A deployment combining the dsh
  harness with execution endpoints must drop one of them; this is the price of never claiming a
  binding the harness cannot route through.
- **One Router per deployment (v1).** The exclusivity check reads the Router's own stream; a second
  Router spawning the same worker is out of scope (multi-Router is not a supported configuration).

## Deferred: rebinding a live session to a different worker

Design only — **not** this round, and any move here is an owner gate (it needs an EVENT-MODEL
change). What it would take:

- **A durable intent, not a mutation.** A live session's binding is already-persisted bytes; rewriting
  them is forbidden (constraint 3). A rebind would be a *new* durable fact naming
  `(thread, from_worker, to_worker, reason, operator, epoch)` — a new event kind (or a
  `control/interrupt`-shaped reserved row consumed into one), appended by the control plane on
  `kian-superagent`'s behalf, never by a tool the model calls for convenience.
- **Fencing for the old worker's queued rows.** The durable queue is keyed by endpoint id and command
  id (ADR-007). After a rebind the outstanding rows on the old worker must be abandoned honestly —
  not silently re-dispatched and not left to execute late against a session that has moved. That
  needs a queue-side rule (cancel-and-audit by `(command ids ≤ seq)`) that does not exist today.
- **A projection rule.** "Which worker owns which seq range of this session" must be derivable, so
  the audit answers "this command ran on B, that one on A". A single-valued binding (this ADR) is the
  degenerate case where the range is the whole stream; the general case needs the range recorded.
- **Where it would live.** `design/EVENT-MODEL.md` (the intent kind), `design/DESIGN.md` §4.1/§4.3
  (the range projection), `kian-superagent` (the spawn/rebind tool surface) and `kian-cli` (the
  assembly + ops verb). Not in `kian-core`.
- **The surface it would need.** An operator verb (`kian thread rebind`) plus a control-plane row,
  because a model-callable rebind would let the Router silently move work off a healthy worker — the
  exact failure mode I3/I4 exist to prevent.
- Until then: a worker that never returns produces the honest queue timeout (ADR-007), the session
  parks, and the durable record still says which worker was assigned.

## Non-goals (this round)

- No new transport/protocol, no session-execution protocol, no new binary; `kiand` only.
- No live rebind to a different worker id (above).
- No worker pooling: one live bound worker session per worker (owner 1:1).
- No bind-time liveness gate and no automatic failover: a temporarily offline worker is a valid
  binding (durable-queue semantics); the session parks honestly.
- No fs or tool surface on the worker beyond `exec`; no `kiand` protocol change.
- No deployment work (Vultr + WeChat): that discussion is next.
- No README/SPEC/CHANGELOG capability rows here — the round's closeout card owns them.

## Verification

The contract is normative in `design/EVENT-MODEL.md` (additive keys) and `design/DESIGN.md`
§4.1/§4.3/§4.7; the runtime shape, the decision table and the L1/L2 test classification live in the
round-11 spec (`shared/round11/spec.md`). L1 (default-green, no network): byte-identical legacy
wrapper/`thread/started`/schema/prompt when no endpoints are configured; exactly one additive key
when bound; parse tolerance (absent = unbound, malformed = refused); the spawn matrix (unknown,
busy, missing-when-required, defaulted-when-single, refused-when-feature-off) with no side effects on
every refusal; routing selection as a pure function of (stream, catalog); the liveness release table.
L2 (real `kian serve` + two real `kiand` processes): each bound session's `exec` lands on its own
daemon (asserted by endpoint-side effects), `kill -9` mid-command re-drives on the same worker with
no duplicate execution, a server restart re-derives the same binding, a legacy unbound session keeps
the first-endpoint route with its audit line, and zero-endpoints configurations keep every existing
suite green. No L3 claim is made; deployment is out of scope. Independent review must confirm: no
core/store/orchestrator change, no new event kind or table, additive-keys-only EVENT-MODEL diff, the
iron rule intact (no store/engine/claim symbol in `kian-superagent`), cache-byte stability (persisted
prompts/requests untouched), and that every refusal precedes every side effect.

## References

- `design/EVENT-MODEL.md` §3 `thread/started`, §4.2 additive keys, §6.3 payload evolution.
- `design/DESIGN.md` §4.1 (Router/Worker), §4.3 (BYOM endpoint), §4.7 (bundle policy/prompt-prefix).
- ADR-007 — kiand delivery semantics (durable queue, at-least-once, command-id dedupe, re-drive).
- ADR-012 — the additive delegation-wrapper object precedent (`mandate`) and its immutability rule.
- ADR-006 — session-immutable model slots: the same "fixed for the session's lifetime" rule applied
  to the execution worker.
- ADR-009 — claim admission (one drain per session at a time), the ordering this ADR leans on for
  the 1:1 check.
