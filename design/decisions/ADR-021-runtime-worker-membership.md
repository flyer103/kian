# ADR-021: Runtime worker membership — self-registration, dynamic join/leave

- **Status**: accepted (owner decisions, 2026-11-08; landed with Round 12 — implemented, QA-verified
  and reviewed per the CHANGELOG R12 entry)
- **Date**: 2026-11-08
- **Scope**: `kian-endpoint`/`kiand` (register mode), `kian-exec` (accept listener, runtime routes,
  membership directory), `kian-cli` (config, assembly, `kian workers`), `kian-superagent`
  (`list_workers`, plain-string `exec_worker`, directory port), docs and tests. **No `kian-core`, no
  `kian-store`, no `kian-orchestrator`, no new event kind, no new table, no new HTTP surface.** One
  wire version bump (KiandProto v1) with its dual goldens.
- **Supersedes**: **ADR-020 D5/D6/D7 only** (the static catalog surfaces: config-only membership, the
  `spawn_worker` enum, the judge-prompt catalog block). **ADR-020 D1–D4 and D8–D16 stand unchanged**
  — the durable stream-derived binding, the 1:1 live-session exclusivity, the per-drive routing
  principle, the legacy byte-identity rules, the async-pair carry and the refusing side are all still
  the contract.
- **Related**: `design/WORKER-MODEL.md` (the worker-domain contract, WHAT), `design/API-CONTRACT.md`
  §8/§9 (KiandProto frames, `PROTOCOL_VERSION`), `design/EVENT-MODEL.md` §3 (`thread/started`,
  untouched), `design/DESIGN.md` §4.1/§4.3/§4.7, ADR-007 (durable at-least-once exec queue),
  ADR-009 (single-drain exclusivity), AGENTS.md constraints 1/2/3/5/7/10.

## Context

ADR-020 made a worker session's execution target durable and auditable, but membership stayed
**configuration**: the worker catalog is the `endpoints:` list read at boot, the endpoint registry is
seeded from it, and joining or leaving a worker means editing `kian.yaml` and restarting
`kian serve`. ADR-020 also put the catalog into **prompt-prefix content** (a `spawn_worker` enum plus a
judge-prompt catalog block), so every membership change would rewrite a prompt prefix and pay the
provider cache invalidation recorded as constraint 3.

The owner requirement (2026-11-08, design thread) is about lifecycle independence, and it is the
whole reason for this round:

> kian and a worker may have independent lifecycles — kian itself can crash, restart or upgrade, and
> a worker can crash, restart or upgrade — so workers must be able to join and leave kian
> dynamically, and kian must support that dynamic membership.

Owner-pinned consequences (round brief §0):

1. Membership is runtime, not configuration. Joining/leaving must never require a `kian.yaml` edit or
   a `kian serve` restart; a kian restart/upgrade must require no worker-side config change.
2. Workers self-register (owner choice): the worker dials kian, and kian reuses that connection as the
   command channel. The server-dial direction survives only for legacy static `endpoints:` entries.
3. One `kian serve` instance. Multi-replica/federation is out of scope and recorded as a boundary.
4. Auth boundary: a private listener bind (loopback/RFC1918/Tailscale CGNAT) plus a required
   env-only shared token, compared constant-time, never logged, with bounded failures; public binds
   refuse to start unless explicitly overridden. mTLS / per-worker identity is R13.
5. A spawn may bind an **offline but known** worker (commands park durably, the honest queue timeout
   is the bound, the result/audit says so); an **unknown** id is refused naming the known ids.
6. 1:1 is unchanged: one worker ↔ at most one live bound worker session; an offline worker does not
   release its binding.
7. Rebind / retirement is deferred to R13 (designed below, built nowhere).
8. ADR-020's binding layer is unchanged; this round supersedes its static **catalog** surfaces
   (D5/D6/D7) only.

The design constraints that decide the *how*:

- Constraint 1/2: membership is liveness, not truth. If membership facts entered the event stream, a
  restart would have to reconcile two records of who is alive. The durable `endpoint_registry` row is
  exactly the disposable projection the heartbeat watchdog already consumes (ADR-007's pre-event
  durable pattern), so reusing it keeps the truth model intact and adds no migration.
- Constraint 3: prompt/tool-schema bytes are cost. Membership data must therefore live in **tool
  results**, never in the prefix — which is also why the ADR-020 enum must go.
- Constraint 10 (narrow waist): all of this is exec-layer and bundle/assembly work. Nothing here
  belongs in `kian-core`.
- ADR-020 D3: the binding is derived per drive from the session's own stream. Dynamic membership must
  therefore not introduce a resident binding map; it introduces a resident *liveness* map, which is a
  different, disposable thing.

## Decision

1. **Membership is a runtime directory, not configuration.** `kian-exec` owns a `WorkerDirectory`
   (`id → {description, version, generation, max_concurrent, peer_addr, last_seen, connection,
   teardown}`). Its state is **computed** by a pure function of (live connection, `last_seen`, now,
   teardown flag): `online` requires a live connection *and* a heartbeat inside
   `offline_after_ms`; `draining` is the display name of the teardown window; everything else is
   `offline`. The directory is disposable and is never a truth. No membership fact enters the event
   stream.
2. **The durable projection is the existing `endpoint_registry` row** (`endpoint_id`, `addr`,
   `last_heartbeat_ms`, `state`). No migration, no new table, no new state value. For a registered
   worker, `addr` is the *observed peer address* (observability only — identity is the id, ADR-020
   D1). Because dynamic members write ordinary registry rows, the existing heartbeat watchdog and the
   existing `endpoint_health` health-input bridge cover them with **zero new code paths and no new
   event kind**.
3. **Worker-dial registration on KiandProto v1.** `PROTOCOL_VERSION` becomes 1 in both halves, with
   dual goldens in the same change. New frames: `register {endpoint_id, description, token, version,
   max_concurrent}` (worker → server), `rejected {reason, detail?}` (server → worker) with the closed
   reason set `version_mismatch | bad_token | duplicate_static | id_conflict | disabled`, and a
   worker-side `bye` for graceful leave. After the handshake the connection carries the unchanged
   `command`/`result`/`ping`/`heartbeat` frames. The token travels in a **redacting newtype** so no
   frame, log line or error string can print it. The server-dial `hello → welcome` shape stays valid
   at v1 for static pins.
4. **The upgrade policy is explicit: kian first, workers second, and no compatibility shim.** A v0
   peer is refused `version_mismatch` with one audit line and never gets a half-compatible connection.
   A shim would exist only to hide a version gap: the v0 and v1 steady states are byte-identical apart
   from the version integer, so "compatibility" would buy nothing except an invisible half-upgraded
   deployment.
5. **Runtime routes + queue-first dispatch.** A route is created on registration and torn down on
   leave; the per-route driver is the only component that turns queue rows into wire dispatches.
   `exec_tracked` enqueues the durable row **without requiring a live route** and awaits the
   correlated outcome: no route ⇒ the row parks `pending` until a route appears or `queue_timeout`
   abandons it honestly. This removes the pre-R12 `no endpoint route registered` error for dynamic
   workers and is what makes an offline-known binding honest. Registration re-drives `pending` and
   `inflight` rows under ADR-007's at-least-once + command-id dedupe. No cross-worker dispatch, no
   failover, no local-exec fallback — ever.
6. **1:1 unchanged, busy is live-session based.** One worker ↔ at most one live bound worker session;
   a worker going offline releases nothing, reroutes nothing, and never runs commands locally. The
   busy refusal names the owning worker session and thread and precedes every side effect.
7. **The Router catalog becomes runtime data, never prompt content.** `spawn_worker.exec_worker` is a
   plain non-empty string property (no `enum`) and is required whenever membership is on; the judge
   prompt carries one frozen instruction line ("call `list_workers`; never invent an id"); a new
   read-only, Router-only `list_workers` tool returns `{id, description?, state, bound_session?,
   bound_thread?}` from the directory plus the stream-derived live-session projection. Membership
   changes therefore never alter a prompt prefix or a tool schema. New sessions pin a new
   `superagent.policy.version`; live sessions keep their persisted prompts.
8. **Offline-known is allowed; unknown is refused.** A spawn may bind a worker that has registered
   before (registry rows are never pruned) or a static pin, even while it is offline: the result and
   the audit line say `offline — commands will queue`, and the honest queue timeout is the bound. An
   unknown id is refused naming the known ids. Refusing offline workers would couple the Router to
   worker liveness and fail exactly during the restart/upgrade windows this round is about.
9. **Auth boundary (R12).** The registration listener binds a private address by default (loopback,
   RFC1918, CGNAT `100.64.0.0/10`, IPv6 ULA); a wildcard or public bind refuses startup unless
   `workers.allow_public: true` is set explicitly, and the docs never set it. Application auth is a
   **required env-only shared token** (`workers.token_env` names the variable): constant-time
   compare, never logged or echoed, one attempt per connection, a per-peer attempt bound (5 in 60 s),
   and startup refusal when the listener is configured but the variable is absent or blank. The value
   never appears in `kian.yaml`, argv, logs, events or error text.
10. **Static `endpoints:` stays, and the static entry wins a collision.** Pinned workers keep the
    server-dial path, the config-owned description and their boot-created routes. A dynamic
    registration whose id equals a static pin is refused `duplicate_static` and audited; the reverse
    (dynamic wins over static) is not offered in R12 — a config knob for it would be a new surface for
    no present need.
11. **Per-worker `exec` sides become runtime-provided.** ADR-020 prebuilt one `WorkerLoopDriver` side
    per configured worker at boot; with runtime membership the driver keeps a **lazy per-worker-id
    side cache** built from a factory, selected per drive from the session's own stream exactly as
    today, and **never evicted** (an offline worker's side still enqueues correctly by id). The
    known-id snapshot is config ids ∪ the directory's known ids, read through a bundle-owned
    read-only port (`WorkerDirectoryPort`, the `RecallPort`/`WorkerStreamsPort` precedent). No
    `kian-core` seam: the driver and the assembly live in `kian-cli`. `async_tools` follows ADR-020
    D11 — each worker id's side mounts its own async pair bound to that id.
12. **One kian instance.** A worker registers to exactly one kian; no federation, no relay, no
    multi-replica membership. Recorded as a boundary in `DESIGN` §4.3 and `WORKER-MODEL` §9.
13. **No new event kind, no new table, no new HTTP route, no new ops method.** The only wire change is
    KiandProto v1. The operator surface is a read-only embedded CLI verb (`kian workers`), which reads
    the durable projection and writes nothing.

## Alternatives considered

| Alternative | Rejected because |
|---|---|
| Keep static config and reload `endpoints:` on SIGHUP | Still operator-config-driven (not worker self-registration), and a live reload of prompt-prefix content re-opens the cache-invalidation problem. It also reloads a config *section*, not membership: no join/leave semantics, no per-worker identity |
| Runtime registration on the kian side (CLI/ops add/remove, server keeps dialing) | Meets "no restart" but not "the worker self-registers", and still requires the worker to be reachable at a known address. Kept only as the fallback if the owner reverses the self-registration choice |
| A discovery service or a shared DB as rendezvous | A second source of truth for membership plus new infrastructure; kian's own registry is already the liveness record |
| Persist membership as events (`worker/joined`, `worker/left`) | Membership is liveness, not truth: an event stream of joins cannot answer "is it alive now" without a clock, and replay would resurrect dead members. It would also need the EVENT-MODEL change protocol for facts the watchdog already owns |
| Auto-prune stale ids (TTL) | Pruning makes a previously-known worker look *unknown*, which would turn an offline-known bind into an honest refusal — the exact coupling to liveness the requirement forbids. R12 keeps stale ids visible/offline (small scale) and defers the TTL question |
| Keep the ADR-020 enum + judge-prompt catalog and re-admit sessions on change | Every membership change would rewrite prompt-prefix content and pay cache invalidation; dynamic membership makes that constant churn. It is also unimplementable as a *stable* enum: the value set would change under a live Router |
| Make `exec_worker` optional (defaulted) for the single known worker | "There is exactly one worker" is not a stable runtime fact; the schema would change bytes when a second worker joins, and an omitted argument would silently bind a worker the model never chose |
| Publish the listener publicly and rely on an obscure id | Anyone could register a fake worker and receive command execution |
| mTLS now | Certificate issuance/rotation and a public-capable listener are R13 work; SSH/Tailscale already provides transport authentication, and the shared token closes the remaining same-host hole |
| A v0 compatibility shim | v0 and v1 differ only by the version integer on the steady-state frames; accepting v0 would hide a half-upgraded deployment with no functional gain, and the round pins an explicit upgrade order instead |
| Reuse the `endpoint_registry` table but add `description`/`protocol_version` columns | A second durable copy of display-only data, needing a migration and a reconciliation rule after a restart. Absence until re-registration is honest; the worker is the identity source |
| Give `kian workers` a runtime view via a new ops method | A new API surface (API-CONTRACT change) for a display convenience. The CLI reports the durable view and documents the absence |

## Consequences

**Positive**

- Workers join and leave at runtime; kian restart/upgrade and worker restart/upgrade are independent
  and require no config edit on the other side — the owner requirement, structurally.
- The binding is untouched (ADR-020 D1–D4): nothing in this round rewrites a session, a wrapper or a
  `thread/started`; membership changes are invisible to the event stream.
- Prompt-prefix stability becomes a property, not a discipline: the schema and prompt no longer
  contain any membership value, so a join/leave cannot change a byte of either.
- Queue-first dispatch turns "the worker is upgrading" from a failed dispatch into a parked row, and
  the honest timeout remains the bound on a permanently lost worker.
- No migration, no new table, no new event kind: the durable footprint is the registry row that
  already existed, and the watchdog already consumes it.
- The `CombinedTools`-era multi-endpoint limitation stays fixed for dynamic workers too: a bound
  session has exactly one reachable `exec`, by construction.

**Negative / risks**

- **A new inbound listener** is a new surface. It is private-bind-only by default and token-gated, but
  it must stay private in every deployment; the docs say SSH tunnel or Tailscale, never a public port.
- **The shared token does not prove per-worker identity.** Recorded limitation: any token holder may
  register under any id, including an id another worker uses (which replaces that worker's connection
  and re-drives its commands on the attacker's machine). Acceptable for the single-user R12
  deployment; **mTLS / per-worker identity is the R13 fix** and is a prerequisite for any hosted or
  multi-tenant use. Until then the deployment boundary (private bind + SSH/Tailscale + one trusted
  operator) is load-bearing.
- **A wire version bump requires a coordinated upgrade.** kian first: during the window, static
  endpoints are refused with a typed terminal error plus an audit line, and a v0 worker simply never
  registers (it has no register mode at all). Deployments must plan the worker upgrade.
- **Right after a kian restart every dynamic worker is offline** until it re-registers, and
  `description`/`version` are absent until then. Honest, visible in `list_workers`/`kian workers`,
  and bounded by the worker's backoff.
- **An unknown-then-typo'd id is refused, but a *previously registered* typo becomes a permanent
  member**: registry rows are never pruned (M5), so a one-off bad registration stays in the known set
  as an offline entry. Acceptable at R12 scale; the TTL question is recorded as deferred.
- **Per-spawn directory + registry read**: the validation path gains one snapshot read (the same cost
  class as the pre-existing exclusivity scan). Unmeasured; small scale assumed.

## Deferred (R13 and beyond) — design sketch only, build nothing

1. **Live rebind of a bound session to a different worker id.** ADR-020 §Deferred stands unchanged as
   the design: it needs a *new durable intent* naming `(thread, from_worker, to_worker, reason,
   operator, epoch)`, fencing for the old worker's queued rows (cancel-and-audit by command ids ≤ seq),
   and a projection rule for which worker owns which seq range. That is an EVENT-MODEL change, hence
   an owner gate. Note the interaction this round creates: with runtime membership a worker can vanish
   permanently *while* holding a live binding, so the honest-timeout park (R12's answer) will become
   an operational problem sooner — retirement is the natural first R13 item.
2. **Worker retirement / draining protocol beyond graceful `bye`.** A "retire worker" operator verb
   would let a bound session be told to end cleanly; it needs a control-plane surface and a policy for
   in-flight commands, and therefore a durable intent (same gate as rebind).
3. **mTLS and per-worker identity, token rotation without restart.** Needs certificate issuance and
   rotation, a public-capable listener, and a config surface for per-worker credentials; it also
   replaces `workers.token_env` with a per-worker credential file (the CHANNEL-WECHAT 0600 precedent).
   This is the gate for hosted/multi-tenant use.
4. **Multi-instance federation.** A worker registered to all replicas, or a relay/leader, plus a rule
   for which instance owns an exec row. Out of scope by owner decision (§0.3).
5. **Stale-id TTL and id retirement.** Requires deciding how a pruned id interacts with the
   offline-known bind policy and with audit history (registry rows are the only residue).
6. **Persisting `description`/`version`** so post-restart `list_workers` and `kian workers` can show
   them: a migration under the ADR-007 pre-event pattern, display-only, deferred.

## Non-goals (this round)

- No multi-replica / multiple kian instances / relays / worker-to-all-replicas.
- No mTLS, no per-worker identity, no token rotation without restart.
- No live rebind, no worker retirement protocol, no automatic takeover.
- No worker pools: 1:1 stays.
- No membership data inside the prompt or tool schemas; no dynamic worker descriptions anywhere except
  `list_workers` results.
- No non-`exec` worker tools (fs/recall placement unchanged from ADR-020).
- No public registration listener; no hosted multi-tenant concerns.
- No new event kind, no new table, no migration, no new HTTP route, no new ops method.
- No deployment work (Vultr/WeChat): still the owner's later discussion.
- No README/SPEC/CHANGELOG/API-CONTRACT rows here — the round closeout card owns them.

## Verification

The normative runtime shape, the exact frame bytes, the config surface, the state machine, the
invariant-to-test matrix and the per-card work orders live in the round-12 spec
(`shared/round12/spec.md`); the worker-domain semantics live in `design/WORKER-MODEL.md`; the wire is
`design/API-CONTRACT.md` §8 (v1, written by the closeout card) and the payload keys stay in
`design/EVENT-MODEL.md` (unchanged).

L1 (default-green, no network): dual frame goldens at v1 in both halves (register/welcome/rejected/
bye/command/result/heartbeat, including the absent-`description` and minimal-`rejected` forms); the
token-redaction assertion; the typed-refusal matrix (version/bad-token/duplicate-static/id-conflict/
disabled) with bounded attempts and zero state change; the bind-policy and token-env startup refusals;
the membership state machine as a pure function over an injected clock; runtime routes (enqueue with
no route parks, attach drains `pending`+`inflight`, timeout is honest, no cross-worker dispatch);
`online ⇔ route attached`; the spawn matrix (feature-off, unknown, busy-offline, offline-known with
the note) with zero side effects on every refusal; `list_workers` determinism and read-only-ness; the
four-render byte equality of the `spawn_worker` schema and the judge prompt across
off/static/join/leave; zero-dynamic byte identity of every pre-R12 suite; the iron rule with no new
exemptions.

L2 (real `kian serve` + real `kiand` over loopback, fake token values): join without a restart or
config edit and execute on the registered worker; `bye` and `kill -9` offlining within
`offline_after_ms` with the health input reaching the bound session; queued commands parking and
re-driving with no duplicate execution; kian `kill -9`/restart with re-registration and a rebuilt
binding (exactly one end write, no stream rewrite); same-id worker upgrade with the binding unchanged;
wrong token / version mismatch / id collision refused honestly with no partial connection; static and
dynamic coexistence. The real v0→v1 upgrade window and any process-death window the harness cannot
inject stay marked **unverified** in the round report; the L1 fixtures are then the evidence.

Independent review must confirm: no core/store/orchestrator change, no new event kind, no new table or
migration, additive-only API-CONTRACT §8 v1 (frames, not surfaces), iron rule intact, prompt/schema
byte stability, no token in any artifact, and that every refusal precedes every side effect.

## References

- `design/WORKER-MODEL.md` — the worker-domain contract (identity, lifecycle, binding/dispatch,
  catalog/discovery, security boundary, version policy, degradation).
- `design/API-CONTRACT.md` §8 (KiandProto v0 today; v1 written by the R12 closeout), §9.1/§9.2
  (frozen surface and change classes), §2.4 (trust boundary).
- `design/DESIGN.md` §4.3 (BYOM endpoint → membership directory and runtime catalog), §4.7
  (prompt-prefix stability across membership changes), §4.1 (binding).
- `design/EVENT-MODEL.md` §3 `thread/started` (unchanged by this ADR).
- ADR-020 — durable per-session worker binding (D1–D4, D8–D16 stand; D5/D6/D7 superseded here).
- ADR-007 — kiand delivery semantics: the durable queue, at-least-once, command-id dedupe, re-drive.
- ADR-009 — claim admission: the single-drain exclusivity the 1:1 check leans on.
- `shared/round12/spec.md` — the locked round-12 contract; `shared/round12/brief.md` — the owner
  decisions and invariants this ADR records.
