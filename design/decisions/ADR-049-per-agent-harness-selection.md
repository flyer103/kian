# ADR-049: Per-agent harness selection — one Router, many worker loops

- **Status**: accepted (owner gate, 2026-11-20)
- **Date**: 2026-11-20
- **Context**: HARNESS-BRIDGE phase 1 wires exactly **one** inner loop per deployment, and the
  owner needs one entry point that can dispatch to a kian-loop worker, a Codex worker, a Claude
  Code worker or a dsh worker. Today three places hard-code the single-harness assumption:

  1. config validation refuses more than one `superagent.harness.{dsh|claude|codex}` and refuses a
     harness together with a worker surface ([config.rs](../../crates/kian-cli/src/config.rs)
     `ServeConfig::validate`);
  2. assembly resolves `worker_inner = Bridge | Dsh | React` **once** for every worker side
     ([plugins.rs](../../crates/kian-cli/src/plugins.rs) `SuperAgentPlugin::apply`);
  3. the bridge marker is one process-global `session/created` payload
     ([superagent.rs](../../crates/kian-cli/src/superagent.rs) `EngineInbox::set_bridge_marker`).

  The audit also exposes an honesty bug this ADR fixes: the per-assignee side factory always
  builds a ReAct loop ([plugins.rs](../../crates/kian-cli/src/plugins.rs)), so in a bridged
  deployment a roster child runs the kian loop while the global marker claims it is bridged.

  The owner's gate answers: (1) selection is **per agent**; (2) a bridged agent that declares
  non-empty `tools:` is **refused at boot**; (3) a mixed deployment may keep `endpoints:`, but a
  bridged agent may never be dispatched with `exec_worker`; (4) the P1 acceptance pair is
  **kian-loop + Codex** (claude/dsh go through the same resolver under their fixture suites).

- **Decision**:

  1. **The selection surface is the worker template, not the machine.** Two additive config keys:
     `superagent.worker.harness` (the default worker template) and `superagent.roster[].harness`
     (one roster entry), values `kian | dsh | claude | codex`; absent = `kian`. `superagent.harness`
     may then declare **multiple** bridge/dsh sections (at most one per kind — two Codex workers
     with different argv stay a phase-2 item). `kian` is always available (the built-in loop).
     Harness selection therefore rides the bundle config, never `agents:` (the core `AgentDef`
     gains no harness concept — narrow waist, constraint 10).
  2. **Selection is per spawned worker and stream-derived.** The spawn wrapper already records the
     roster `assignee`; the driver selects the side by the same committed-stream read it uses
     today (`stream_assignee_of` → per-assignee factory; default/no assignee → the worker
     template). The per-harness agents (bridge driver/dsh driver) are built once at assembly and
     shared; the side caches, the decorator (lineage, evidence gate, completion report) and the
     takeover path are unchanged. Config is static per process; a later config change is a deploy
     event (the bridge marker on each session's `session/created` keeps the audited class honest).
  3. **The marker becomes per spawn.** `EngineInbox` resolves the `session/created` payload from
     the spawn's assignee (roster entry → its harness) or the default template; a kian-loop spawn
     writes the historical `{}`. This fixes the roster-child marker lie and makes a mixed
     deployment auditable per session.
  4. **Tool ownership is enforced per bridged agent** (fail-closed):
     - an explicit bridged selection requires the referenced agent's `tools:` to be empty
       (refuse boot) and the roster entry's `tools_allow` to be empty (refuse boot);
     - `spawn_worker(agent: <bridged>)` carrying `exec_worker` is **refused** at the tool (the
       child owns the tools; a kian exec binding on that session would be audit fiction);
     - a bridged **default** template cannot coexist with a worker surface (`endpoints:` /
       `workers:`) — every non-roster bound spawn would have nowhere to route;
     - a kian-loop agent keeps its tools and its `exec_worker` binding; endpoints may therefore
       stay configured beside declared harnesses as long as the default template is kian-loop.
  5. **Legacy mode is byte-compatible.** With **no** explicit `harness:` selection anywhere, the
     old rules apply verbatim: at most one harness section; that harness replaces the worker
     template; a harness and a worker surface are mutually exclusive; the single global marker is
     written for every spawn. The only intentional audit delta is the roster-child marker in that
     mixed legacy shape (it now writes `{}` when the child actually runs the kian loop), recorded
     as a fix, not a byte-identical promise.

- **Alternatives considered**:
  1. **A gateway in front of one kian instance per harness.** Meets "one API entry" but not the
     actual requirement: the Router/board/audit cannot dispatch across harnesses, sessions are
     split across databases. Rejected as the main line.
  2. **Per-call `spawn_worker(harness: …)`.** More flexible but loses config-time fail-closed
     safety and capability validation; deferred to phase 2 (a model could silently pick a harness
     whose agent contract does not fit the task).
  3. **Per-worker-machine (`endpoints`) harness.** Wrong axis: a harness is a property of the
     worker *agent/template*, not of the exec machine; and bridges own their tools, so they never
     bind an exec machine at all.
  4. **Keep one-harness-per-deployment and accept N instances.** The status quo; rejected by the
     owner.
  5. **Put `harness` on the core `AgentDef`.** Rejected: harness is a bundle concept; the core
     model stays harness-free (constraint 10).

- **Adversarial walk** (state → failure → recovery):
  | Failure point | Invariant | Outcome |
  |---|---|---|
  | Config names a harness that is not declared | fail-closed | boot refuses, naming the key and the declared set |
  | Bridged agent with non-empty `tools:` / roster `tools_allow` | honesty (no dead tool surface) | boot refuses |
  | `spawn_worker(agent=bridged, exec_worker=…)` | no audit fiction | tool refusal before any side effect (no thread id, no row, no lineage) |
  | Bridged default template + `endpoints:` | no unroutable binding | boot refuses; move the default to `kian` or drop the surface |
  | Spawn's `session/created` marker vs. the side actually selected | audit honesty | marker is resolved from the **same** spawn identity the side is; a legacy bridged deployment's roster children now write `{}` (fix) |
  | Roster entry vanishes between spawn and the first drive | no widening | the existing refusing side runs; harness selection is never guessed from another entry |
  | Config edited mid-session / takeover | constraint 1/13 | the side caches are built at boot; the committed wrapper/assignee is the selection input, never live config during a drive |
  | Shared bridge driver across sessions | per-session child isolation | the bridge driver keys live children by `SessionId` (today's single-harness design); sharing the Arc adds no cross-session state |
  | Capture proxy + multiple bridges | class honesty | the capture hub is serve-level and applies to every declared claude/codex adapter; `request_bytes: exact` only when configured, per spawn marker |
  | Legacy config (one harness, no selections) | byte compatibility | exactly today's behavior |
  | Rollback to a pre-ADR binary | readability | unknown keys refuse that binary's boot (`deny_unknown_fields`); streams are existing kinds only |

- **Invariants preserved**:
  - **Event stream sole truth** (constraint 1): the harness is selected from the committed
    wrapper/assignee; no resident routing state.
  - **Replayable bundle shaping** (constraint 13): the selection input is the spawn's own recorded
    bytes; the marker is written once at creation.
  - **Honesty** (constraint 7): a bridged spawn is marked bridged; a kian-loop spawn is not; a
    refused mixed spawn has zero side effects.
  - **Narrow waist** (constraint 10): harness selection lives in `superagent` config/assembly and
    the bundle crate; `kian-core`/`kian-orchestrator` learn nothing.
  - **No new event kind, no wire change**: `session/created.bridge` already exists
    ([EVENT-MODEL](../EVENT-MODEL.md) §4); only its timing of choice changes.

- **Consequences**:
  - Positive: one Router entry can delegate to a kian-loop agent, a Codex worker, a Claude Code
    worker or a dsh worker in one instance — same board, same event stream, same audit; the
    roster marker becomes truthful.
  - Negative: config validation grows a matrix (legacy vs per-agent mode); a bridged agent's
    `tools:`/`tools_allow` must be empty (a deliberate restriction); the default worker template
    must be kian-loop to keep a worker surface.
  - Rollback: remove the selection keys (back to the single-harness rules) or revert the ADR.

- **Non-goals (phase 1)**:
  - Call-time harness override (`spawn_worker(harness=…)`).
  - Multiple sections of the same kind (two Codex workers with different argv/models).
  - Per-harness concurrency/queue policy, per-harness capturometrics, cron reuse of the resolver
    (`superagent.cron.harness` stays its own raw-mode runner, ADR-039).
  - Cross-process harness resume; per-session `ask` for bridged agents (still allow/deny).
  - Per-session capability re-negotiation or hot harness swap.

- **Verification map**:
  | Given | When | Then | Level |
  |---|---|---|---|
  | multi-harness config with explicit worker/roster selections | boot | mounts every declared harness; `kian` agents keep tools; legacy configs unchanged | L1 (config matrix) |
  | bridged agent with tools / `tools_allow` / undeclared harness / bridged default + endpoints | boot | refuses, naming the key | L1 |
  | roster entry marked bridged | `spawn_worker(agent=it, exec_worker=…)` | tool refusal, no side effect | L1 (router) |
  | one serve, two roster agents (kian-loop + Codex fixture peer) | two spawns/submits through the same entry | the kian child carries `step/request` and no bridge marker; the Codex child carries the per-spawn marker and no `step/request`; both end honestly | L2 |
  | kian-loop + real Codex in one instance | one delegation each | both settle with their own evidence (recorded in the PR) | L3 (manual, env-gated) |
  | `claude`/`dsh` per-agent selection | fixture adapters | the same resolver selects their driver; their existing suites stay green | L1/L2 (existing fixtures) |
