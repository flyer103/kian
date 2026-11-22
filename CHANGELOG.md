# Changelog

This file records **what changed, when, and in which capability**. The contract lives in
[docs/SPEC.md](docs/SPEC.md); the design and its decisions live in [design/DESIGN.md](design/DESIGN.md)
and [design/decisions/](design/decisions/). Entries are ordered **newest first**, and an entry
carries the functional change only: verification results, gates and test counts belong to the
pull request and the run logs, not here (see [AGENTS.md](AGENTS.md) § CHANGELOG policy).

## Unreleased

- **Declarative jobs: a run is a plan of steps, not one prompt (JOB-MODEL, ADR-052)**. A new
  `superagent.jobs` section declares jobs (a plan of capability-addressed steps with `depends_on`,
  `inputs`/`produces`, per-step `verify`, `timeout_ms`, `retries`, the effect gate and an optional
  step), `triggers` that lower into the existing cron runtime vector as a job-targeted schedule
  (one scheduler, one cursor, one claim table) and `alerts` that fire as deliver-only runs. A run is
  an ordinary durable session whose new `JobLoopDriver` conductor parks between ticks, dispatches one
  child session per step attempt through the existing spawner with derived ids (`kian:job:step:…`),
  freezes the plan and its digest into the admitted row, records placement before the child exists,
  and writes exactly one run verdict (`evidence/verdict` gate `run`) before its final outcome — so a
  step's success is the declared check set over the child's own events and a delivery is the
  channel's receipt, never an exit code. New surface: `kian job list|status|run|cancel`, the additive
  `job` key on `input/admitted`, `thread/started` and `evidence/verdict`, the runtime-only
  `CronTarget::Job`, `SpawnRequest.parent` with the `kian.parent` wrapper key for a non-resident
  parent, and job metrics. No new event kind, table, route or config-file scheduler; the settlement
  sweep settles a job firing from its run verdict (unverified settles failed, honestly named).

- **A dispatch that never left the process no longer spends the re-drive bound (WORKER-MODEL,
  ADR-033)**. `RegisteredTransport::dispatch_frame` and the connection manager now fail a frame
  whose writer is already closed as `ConnectionError::NotWritten` and flip the transport offline
  instead of reporting a plain transport loss. The queue layer refunds the attempt the claim had
  just charged (`refund_dispatch_attempt`, holder-scoped, no row below zero), so a worker that
  registers a moment later still gets its re-drive instead of finding its row `abandoned` for a
  dispatch the endpoint never received. No status, event or wire change; the refund names the
  command in a debug/warn line and a failed refund leaves the bound one attempt short, honestly
  logged.

- **Speak-rules gain `outcome-only` (DESIGN §4.2)**. The closed condition vocabulary
  `always | ack-first-only | decision-needed | result-ready | never` grows a sixth value for the
  one-conversation channel shape: results and decision prompts are sent, the dispatch
  acknowledgement after `spawn_worker` is suppressed, so a delegated task reaches the person once,
  when the outcome is ready. The acknowledgement slot is per execution hold (one per dispatch, the
  first decision-shaped send spends it, an admitted result closes it) and never persisted — a
  replay re-derives it from the same tool calls. Suppression stays honest: an error naming the
  condition, an audit line, the `speak_suppressed` counter, and no `channel/delivered`. No tool
  schema changes, so a deployment that does not set the condition keeps byte-identical prompts.
  `examples/mobile-dispatch` now uses it.
- **A pending `ask` can be answered from a channel for good (CONFIRMATION-MODEL §14d, ADR-051)**.
  `superagent.confirm_answers.channels` opts a channel into the confirmation plane: an inbound
  `approve <code>` / `reject <code> <reason>` is intercepted in the bundle's admission path — before
  the class gate and before any inbox row — and written through the same `confirm` seam the RPC
  uses (`control/confirm`, then the frozen call's `tool/result` carrying `confirmation`). The
  sender handle is the authenticator (the channel allowlist ran first); the six-character code is a
  pure function of `(session, call_seq, call_id)`, so it needs no storage and is replay-stable.
  Anything that is not the closed grammar stays ordinary input — free text is never a decision — and
  a code-shaped line that matches no pending call is dropped with an audit line
  (`answer_no_match`), never forwarded. With the key set, the Router gains a read-only
  `pending_decisions` tool (open calls with their codes) and a frozen prompt line forbidding it to
  assert that a confirmation happened. Absent is off, byte-identical to today; boot refuses an
  undeclared channel, a non-IM channel, or one admitted as `class: ignore`. No new event kind, wire
  method or metric key; the inbound text itself is not persisted.

- **The IM reply budget is configurable and defaults to 300 units (DESIGN §4.2)**.
  `superagent.im.reply_units` sets `ImPolicy::max_words`; the Router judge prompt renders the same
  number at bootstrap (`{budget}`), so the model is told what the policy enforces, and the
  watchdog's `escalate` shares the budget. The default rises from 100 to 300: a normal channel
  reply now carries a conclusion plus a short findings list instead of routinely truncating and
  forcing the `回复「详情」看完整结果` round trip. The policy-version fingerprint covers the
  rendered budget, so a budget change under a pinned label is auditable. Truncation at the budget,
  the honest `truncated` receipt and the details-request full-delivery mode are unchanged. New
  sessions render the new prompt bytes; live sessions keep their persisted prompt (cache-byte
  stability).

- **The fan-in stream takes its tail cursors when the request is accepted (READ-MODEL §4.2)**.
  `GET /events` used to read each listed session's head lazily, on its first poll cycle; an event
  appended between the client opening the stream and that cycle was already behind the head, so a
  tail-only consumer silently missed it. The head queries now run before the response is handed
  back, which is what "tail from open" was always documented to mean.

- **Frozen cron targets succeed through a per-slot firing session (CRON-MODEL §3.2/§5.1/§9.1)**.
  A plain target session ends `completed` after its turn, so the resident arm used to run exactly
  one successful firing per schedule and then fail every later slot (`target_terminal`). Dispatch
  now resolves the target first: a live/empty target receives the firing as before; a **terminal**
  target is replaced by the deterministic firing session
  `uuid5(target, "kian:cron:firing:<firing_id>")` (one fresh session per slot, frozen name
  contract) and the payload is admitted there, so a recurring schedule keeps running with no
  operator action. The configured target stays a delivery anchor with continuity while it lives;
  the new `cron_firings.session` column (additive migration **v8**, sqlite + postgres, nullable
  for pre-v8 rows) records where each firing actually ran, the settlement sweep reads that record
  instead of guessing from the target, and a stranded pre-v8 / crash-interrupted row settles
  `failed`/`target_terminal` only after a 60 s grace so a concurrently claiming replica's
  in-flight dispatch is never mis-settled (a pending row on a live recorded session is left
  alone; a pre-v8 row on a live target is not inferred, because a re-pointed schedule may name a
  different delivery session). The overlap judgement is made on the resolved session and re-checked
  at the admit boundary, so a session that became busy after resolution still records
  `skipped_overlap` rather than queueing a duplicate; a `set_firing_session` that loses a race to
  settlement refuses the dispatch. `target_terminal` remains for the race where a resolved session
  freezes before the admit, a crash between the session record and the admit, pre-succession
  residue, and a corrupt `session` column. The migration gate is bumped to v8 (a real
  v7-database-with-rows upgrade path is pinned), and CRON-MODEL §12.1a records the rolling-upgrade
  caveat: an *older-running* replica has no grace, so the upgrade should be stop-the-world.
  Firing-session footprint and guidance are recorded in CRON-MODEL §9.4 (a held-drain resident
  target projects `running` and is skipped by the overlap judgement, so the practical recurring
  mode is the per-slot path or the worktree arm); chain-shaped succession remains an alternative
  not taken. Contract:
  [design/CRON-MODEL.md](design/CRON-MODEL.md) §3.2/§5.1/§9.1/§9.3/§9.4,
  [design/DESIGN.md](design/DESIGN.md) §2.3/§4.6.

- **Claude bridge: tool permission prompts reach kian**. The Claude child no longer runs in
  print mode (which auto-denied every permission prompt) and passes
  `--permission-mode manual --permission-prompt-tool stdio`, so the harness's `can_use_tool`
  control requests reach kian's approval policy. Live-verified 2026-11-21: an allowed `Bash` call
  ran with `permission.evaluated:"allow"`, a denied one produced an honest error result and did
  not execute. `--resume`/capture keep working under the new argv (recorded in the runbook).

- **A frozen cron target no longer wedges its schedule (CRON-MODEL §5.1/§9.3)**. A session whose
  stream projects terminal can never consume another input, so its leftover inbox rows are audit
  facts, not work: `has_open_work` now answers `false` for a terminal projection. The resident arm
  used to read those rows as "still working" and record `skipped_overlap` for every later slot — a
  permanently stuck busy that no wake could clear (`wake_now` refuses terminal sessions), while a
  target with no leftover rows at firing time was silently admitted onto the frozen stream and its
  firing stayed `admitted` forever. The resident sink now refuses delivery into a frozen target
  *before* the ADR-038 bootstrap and admits nothing; the firing settles `failed` with a
  `target_terminal:` error (a dead target is visible, never a silent skip or a silent admit). The
  settlement sweep closes a firing stranded by an earlier frozen admit, finds an open firing
  anywhere in the history (the wedge left one behind an unbounded run of later `skipped_overlap`
  rows, so the newest-page read never saw it), and reads the first **settling** end after the
  admission (skipping the resumable `awaiting_input` end, and never attributing a later episode's
  end to the firing), so a turn that asked a question and later completed settles instead of
  staying `admitted`. The startup inbox rebuild no longer wakes frozen sessions — their rows stay
  honestly pending as the audit record — and its new frozen-skip read tolerates an unprojectable
  stream (warn and wake, never abort the pass). Contract:
  [design/CRON-MODEL.md](design/CRON-MODEL.md) §5.1/§9.1/§9.3, [design/DESIGN.md](design/DESIGN.md)
  §2.3/§4.6.

- **Claude bridge: absorb thinking-only assistant messages**. Extended thinking can arrive as its
  own `assistant` message whose content blocks are `thinking`/`redacted_thinking` only (observed
  live with a Claude-compatible provider); it carries no model context and is now absorbed as
  `claude/thinking` instead of failing the turn as contract drift.

- **Codex bridge: absorb `warning` notifications**. codex-cli 0.166.0 emits a `warning`
  notification at runtime; it is advisory (never the turn's content) and is now absorbed with the
  `codex/advisory` counter instead of failing the turn as contract drift.

- **Harness durable `ask` (HARNESS-BRIDGE §4.4.1, CONFIRMATION-MODEL §14c, ADR-050;
  experimental, opt-in)**. `approvals.durable_ask: true` lets a bridge defer a gated tool to a
  human: the pending call is recorded with `permission.evaluated:"ask"` and the drain ends
  `awaiting_input` (no lease, no child). A `confirm` resumes the harness conversation (ADR-047),
  delivers a persisted synthetic decision nudge, and auto-answers only an identical re-issue
  (same tool + arguments digest, one-shot) — the single pending call record stays, and the result
  carries `confirmation` provenance. A different call follows the policy again; a resumed turn
  that never re-issues closes the pending call with an honest error result, never a fabricated
  execution or a stranded ask. The harness re-issue behaviour is unverified live (the matrix marks
  it experimental). Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md) §4.4.1.

- **Harness bridges: per-agent harness selection (ADR-049)**. `superagent.worker.harness` and
  `superagent.roster[].harness` choose the worker inner loop per agent — `kian` (built-in, default) |
  `dsh` | `claude` | `codex` — so one serve/Router can declare several harnesses and dispatch each
  worker agent to its own loop. `superagent.harness.{dsh,claude,codex}` may then be declared
  together; every explicit selection must name a declared section, a bridged agent must declare no
  `tools:` (and a bridged roster entry no `tools_allow`), a bridged default template cannot coexist
  with `endpoints:`/`workers:`, and `spawn_worker(agent=<bridged>, exec_worker=…)` refuses at the
  tool. The `session/created.bridge` class marker is now resolved per spawn (a kian-loop child
  writes `{}` even beside bridged siblings). With no selection anywhere the legacy single-harness
  rules and bytes are unchanged. Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md)
  §6.1/§6.2/§9 A13, [design/MULTIAGENT-MODEL.md](design/MULTIAGENT-MODEL.md) §3.1.

- **Resident rotation fires for a parked Router (ADR-040 phase 2)**. A parked resident holds its
  drain, so the rotation trigger is now the **hold owner**: the Router driver checks its policy at a
  park boundary (stream at `rotate_after_events`, no delegated thread outstanding) and ends its own
  session `rotated`; the serve ticker then derives the uuid5 successor, bootstraps it (memory index
  → migrated rows → handoff → judge prompt) and re-points the shared binding. The planner's
  engine-owned `rotate_idle` close remains for a genuinely idle window. Verified by a live
  `kian serve` + scripted-provider test (the driver rotates, the ticker completes the successor).

- **A fixed cron `session:` target on a rotating Router refuses boot (CRON-MODEL §3.3 rule 9)**.
  With `superagent.router.rotate_after_events` set, the live Router moves along the uuid5 chain, so
  a schedule naming the pinned Router session would fire into a frozen session:
  `cron_targets_rotating_router`. The `channel:` namespace is unaffected; with rotation off the
  pinned id is stable and the historical behavior is unchanged.
- **Multi-replica resident rotation (ADR-048)**. A replica whose `ResidentBinding` points at a
  terminal Router now derives the live successor from the durable uuid5 chain, re-points the
  binding and migrates the rows it had written to the frozen predecessor (`rotate:<old>:<row>`
  keys; the originals stay as the audit record). Any replica can complete a rotation whose owner
  died between the close and the successor's bootstrap: the successor bootstrap's admits are
  idempotent by their durable keys (`memory-inject:<successor>`, `rotate:<old>:<row>`,
  `rotate-handoff:<old>`, the re-arm judge-prompt key), so a repeated run is a no-op. The close
  stays claim-fenced (one winner), and a stale replica's window is one tick with no lost work. No
  new event kind, state edge, wire byte or config key. Contract:
  [design/DESIGN.md](design/DESIGN.md) §4.6,
  [ADR-048](design/decisions/ADR-048-multi-replica-resident-rotation.md).
- **Harness conversation resume across drains (HARNESS-BRIDGE §4.6, ADR-047)**. A bridged
  session now remembers its harness conversation: the adapter reports the harness's continuation
  handle (Claude `session_id` from `system/init`, Codex thread id from `thread/start`), the driver
  stamps it onto the turn's `turn/end` (`harness.resume`, an additive payload key — no new kind,
  no side table), and the next episode passes the latest handle to the factory, which resumes with
  `claude --resume <id>` or Codex `thread/resume`. A takeover still ends an interrupted turn
  honestly `failed` (it is not re-driven), but the following input continues the same
  conversation; a rejected resume is a named `SoftFailed` — never a silent fresh conversation.
  No config or class change; dsh is unchanged. Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md)
  §4.6; [design/EVENT-MODEL.md](design/EVENT-MODEL.md) §3.- **Harness request capture: bridged sessions can be `exact` (HARNESS-BRIDGE §5.4, ADR-046)**.
  With `superagent.harness.capture:` on, kian starts a loopback recording proxy and points the
  mounted Claude Code (`ANTHROPIC_BASE_URL`) or Codex (`model_providers.kian-capture`) at it. Each
  request body is recorded **before** it is forwarded (fail-closed: an oversized/unknown route or
  an unavailable queue answers 413/502 and nothing is sent), the bridge driver appends it as
  `step/request` before the assistant event, and the session's `session/created.bridge` class
  becomes `"exact"` — enabling cache/cost audit and byte-request replay for that session. Retries
  are recorded per attempt; auth headers are forwarded but never persisted or logged; the proxy
  binds loopback only and is pinned to the configured Anthropic/OpenAI upstreams. Live probes
  confirmed the two routes (claude 2.1.318 honors `ANTHROPIC_BASE_URL`, codex-cli 0.166.0 honors a
  generated provider config); both were API-key routes, so OAuth-only routes remain unverified.
  Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md) §5.4; config in README.

- **Harness bridge: an open step is bounded (ADR-044)**. `superagent.harness.<name>` gains
  `step_timeout_ms` (default `1_800_000` = 30 min; `0` disables; below `10_000` refuses boot),
  bounding how long an open harness step may pass with neither a protocol event from the child nor
  a completed control write. Expiry is `SoftFailed` naming the bound — the engine settles `failed`,
  the episode-end kill path kills the child, and the ADR-035 release/reclaim frees the worker —
  instead of a live-but-silent child parking the drain forever. Applies to both bridge drivers
  (`kian-harness`: Claude Code/Codex; `kian-dsh`); no new event kind, state edge or wire byte.
  Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md) §4.5, §6.1, §6.2, §9.

- **Harness approvals dispatch at `tool/pre` (HARNESS-BRIDGE §4.4, PLUGIN-SPEC §3.2.5,
  ADR-045)**. A Claude Code/Codex tool that the harness gates is now consulted on kian's plugin
  bus before the answer: a `veto:tool` sidecar can deny it, deny wins over a declarative allow,
  and an allow never widens a declarative deny. The call record carries
  `permission:{policy:"hook", matched:"<plugin>:tool/pre", reason}` for a veto (otherwise
  `policy:"configured"` as before); the view is `surface:"harness"` with the harness's own call
  id (Claude `tool_use` id, Codex `itemId`), bounded arguments and the declarative decision.
  No new config, event kind or wire change; without a mounted bus the behavior is byte-identical.
  Auto-approved harness tools, subagents and background jobs stay outside the contract (the
  deferred `PreToolUse` shim is phase 3). Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md)
  §4.4; [design/PLUGIN-SPEC.md](design/PLUGIN-SPEC.md) §3.2.5, §3.4.1.

- **Resident rotation between restarts (ADR-040 phase 2)**. With
  `superagent.router.rotate_after_events` set, a serve ticker (the `drain.heartbeat_interval_ms`
  beat) retires an idle, over-threshold Router at runtime: the engine's new atomic `rotate_idle`
  close writes `drain/claimed` + `session/ended{rotated}` in one claim-fenced batch (a pending row
  does not block the close — it stays durable on the frozen predecessor and is migrated), the
  deterministic successor receives the memory index, the predecessor's pending rows, a bounded
  handoff and the judge prompt in that order, and the shared `ResidentBinding` re-points every
  router-facing surface (driver routing, residency inbox, spawner parent, completion reports,
  subscription route, tools, recall, child-timeout root) without a restart. Contract:
  [design/DESIGN.md](design/DESIGN.md) §2.2/§4.6, [docs/SPEC.md](docs/SPEC.md),
  [ADR-040](design/decisions/ADR-040-runtime-resident-rotation.md).


- **Cron: the worktree arm can run any harness (CRON-MODEL §6.2)**. `superagent.cron.harness`
  gains `args` and `raw: true`: the default stays the dsh shape (`args --profile <name> <payload>`,
  profile `headless`), while raw mode appends the scheduled payload to the fixed args
  (`claude --print … "<task>"`, `codex exec "<task>"`). Raw mode requires an explicit `command`,
  refuses a `profile`, and rejects empty args — all fail-closed at load. The firing record, worktree
  isolation, ceilings and capture are unchanged. Contract: [design/CRON-MODEL.md](design/CRON-MODEL.md)
  §6.2.
- **Harness bridges: live verification and the first protocol fixes (HARNESS-BRIDGE phase 1,
  ADR-043)**. Live suites now ship for both adapters (skip without `KIAN_CLAUDE_BIN` /
  `KIAN_CODEX_BIN`). Running them against `codex-cli 0.166.0` fixed the real handshake: the pinned
  `unlessTrusted` approval policy is rejected by app-server (the variants are `untrusted`,
  `on-request`, `granular`, `never`; the bridge pins `on-request`, `never` still refuses boot),
  and advisory notification namespaces (MCP lifecycle, account, skills, config, thread/usage,
  diffs/plans, item deltas) are absorbed instead of failing the turn. Running against
  `claude 2.1.318` fixed readiness and error honesty: the CLI has no ready frame and may stay
  silent until the first input, emits `system/hook_*` before `system/init`, and reports
  provider/auth failures as a synthetic `is_api_error_message` assistant message (absorbed —
  the `result`'s error turn end is the honest record). Codex ran a real live turn (`pong`;
  `Settled`); Claude is protocol-verified but its model turn was not exercised (the local CLI is
  not logged in). Contract: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md) §5.1, §5.2, §11;
  run log: [docs/runbook-harness.md](docs/runbook-harness.md).

- **iMessage: the bridge requires `imsg` ≥ 0.14.0 (ADR-042)**. The RPC surface the bridge calls —
  protocol-v1 `status`/`initialize`, `messages.after` ROWID pagination and typed delivery
  dispositions — landed in `imsg` 0.14.0; `0.11.x` answers `-32601 Method not found` and the bridge
  refuses startup. The contract pins the floor and the runbook upgrades the dependency. Contract:
  [design/CHANNEL-IMESSAGE.md](design/CHANNEL-IMESSAGE.md) §2.5; the first live L3 evidence is §12.
- **Harness bridges: Claude Code and Codex (HARNESS-BRIDGE phase 1, ADR-043)**. The worker
  template's inner loop can now be a shared `kian-harness` `BridgeDriver` over the Claude Code
  `stream-json` adapter (`kian-claude`) or the Codex `app-server` JSON-RPC adapter
  (`kian-codex`): `session/prompt`-style input delivery, steer/interrupt, the harness's own
  pre-execution approval gate answered by a declarative allow/deny policy and recorded as
  `tool/call.permission`, and honest outcomes (a mid-turn child death is a soft failure, never
  `completed`). These are *projection-only* sessions — no `step/request`, no cache or byte-request
  replay — and phase 1 has no cross-process harness resume; the capability matrix states both.
  Contract, mappings and limits: [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md),
  [ADR-043](design/decisions/ADR-043-multi-harness-bridge.md). Spawned worker sessions record the
  class durably: `session/created` carries the additive `bridge:{name,request_bytes}` key through
  the eager creation seam (HARNESS-BRIDGE §4.7).

- **iMessage channel through a macOS bridge (`type: imessage`, ADR-042)**. iMessage has no API, so
  the channel speaks a narrow loopback HTTP ABI to `kian-imessage-bridge`, a new macOS-side daemon
  (`imsg rpc` under Full Disk Access + Automation, run as a LaunchAgent in the signed-in Messages
  user's session) reached over an SSH remote-forward or a mesh VPN. Receive is one bounded long
  poll over `/v1/updates`: `guid` → `event_id` (durable idempotency), the config `topics` table is
  both the receive and recipient allowlist, the bridge's `watch.participants` keeps non-allowlisted
  conversations from leaving the Mac, and a replaced `chat.db` rotates the generation so both
  cursors reset. Sends go to any allowlisted handle (not reply-only) with a deterministic
  `client_id` that turns the outbox's bounded retry into effectively-once; `not_started` and
  `may_have_completed` are reported honestly, never as a receipt. The bridge token is env-only
  (`KIAN_IMESSAGE_BRIDGE_TOKEN`); `kian imessage check` is the read-only preflight. Text-only
  direct chats, one channel per config, forced untrusted. Contract:
  [design/CHANNEL-IMESSAGE.md](design/CHANNEL-IMESSAGE.md); setup:
  [docs/runbook-imessage.md](docs/runbook-imessage.md).

- **Resident rotation phase 0: the Router can be retired at boot (ADR-040)**. A new sixth end
  reason `rotated` (terminal, frozen) and a terminal `rotated` session state; the
  `superagent.router.rotate_after_events` switch (absent = off, explicit `0` refuses boot) makes
  `kian serve` assembly retire a live Router over the threshold with one atomic `drain/claimed` +
  `session/ended{rotated}` batch, bind the deterministic uuid5 successor everywhere the pinned id
  was, and give the successor its context in a fixed order: the ADR-038 memory index, the
  predecessor's pending message rows, a bounded synthetic handoff, then the judge prompt. Only an
  `idle` live session rotates (a crashed turn is a takeover), and a live lease wins. Runtime
  rotation between restarts is ADR-040 phase 1/2. Contract:
  [design/DESIGN.md](design/DESIGN.md) §2.2/§4.6, [docs/SPEC.md](docs/SPEC.md).

- **Bounded model calls and drain-crash containment: an abnormal interruption can delay freeing a
  worker, never prevent it (ADR-041)**. A model call is now bounded like every other wait: two
  additive `providers.<name>` keys — `connect_timeout_ms` (default 10 000) and `idle_timeout_ms`
  (default 300 000, `0` disables) — where the idle bound covers the wait for response headers and
  each gap between streamed body chunks (never the total request, so a long generation is not
  killed). A stalled provider therefore settles the turn `failed` (`provider_down` class) instead of
  parking the drain forever, and the ADR-035 release/reclaim path frees the worker. A drain that
  dies (panic or task cancellation) now unwinds its in-process handle and heartbeat so the lease
  expires and the existing takeover path re-drives it; a takeover chain is bounded at three
  un-ended claims (`MAX_UNENDED_CLAIMS`) before writing one honest `session/ended{failed}` with an
  additive `crashed` payload instead of re-driving forever. No new event kind, state edge, table or
  wire byte; contract: [design/WORKER-MODEL.md](design/WORKER-MODEL.md) §3, §6, §8,
  [design/EVENT-MODEL.md](design/EVENT-MODEL.md) §4.


- **Confirmation phase 2: workers can wait on a person (CONFIRMATION-MODEL §8 C2, §14b)**. `ask`
  is accepted on a superagent worker agent. A worker that stops on a pending `ask` now writes
  **no** `thread/completed` and no parent report (it is waiting, not done); the engine still settles
  `awaiting_input`, the worker stays **live** in the passthrough registry (follow-ups keep routing
  to the same session), and the ticket stage gains `awaiting-confirmation` (ranked above the
  structured `waiting` report). Resident Router/watchdog agents still refuse `ask` (a judge waiting
  on itself cannot answer itself). Contract: [design/CONFIRMATION-MODEL.md](design/CONFIRMATION-MODEL.md)
  §8, §14b; [design/MULTIAGENT-MODEL.md](design/MULTIAGENT-MODEL.md).

- **Memory: the bundle hosts on the super-agent assembly and reaches internally created sessions
  (ADR-038)**. `memory:` and `superagent:` now compose: `MemoryLoopDriver` wraps
  `Arc<dyn LoopDriver<Ctx = DrainCtx>>` (the built-in plain loop or the three-way
  `SuperAgentDriver`), dream sessions still route to the dream side, and the config refusal ("the
  Phase-1 bundle assembles on the built-in assembly") is removed. A bundle-level
  `SessionBootstrap` seam runs before each internal creator admits its triggering input — the
  resident Router/watchdog at boot, delegated workers at spawn, cron targets at dispatch — so the
  creation-time index is the session's first context-bearing event there too. The seam is
  fresh-only (an existing session is never rewritten) and idempotent by
  `memory-inject:<session>`; with no `memory:` section every creator is byte-identical. Contract:
  [design/MEMORY-MODEL.md](design/MEMORY-MODEL.md) §6 I10;
  [design/decisions/ADR-038-memory-injection-for-internal-sessions.md](design/decisions/ADR-038-memory-injection-for-internal-sessions.md).


- **Worker result retention: a re-dispatched command id is answered from the endpoint's record, not
  executed again (ADR-037)**. `kiand` records each result frame by command id before sending it and
  answers a duplicate dispatch from that record; a duplicate that arrives while the first execution
  is still running joins it. A crash-replayed `systemctl restart kian` can therefore no longer
  restart the server twice. The record is bounded (24 h TTL, 512 entries, 256 KiB per entry,
  64 MiB file), lives in the worker's state directory (`--dedupe-dir` / `KIAND_DEDUPE_DIR`, on by
  default, `--no-dedupe` / `KIAND_DEDUPE=0` opts out), and a miss still re-executes — at-least-once
  is unchanged and no result is fabricated. Re-drive of a row dispatched within
  `workers.queue_redrive_grace_ms` (default 5 s, `0` disables) is postponed instead of racing a
  running execution. No wire frame, protocol version or store schema changes. Contract:
  [design/WORKER-MODEL.md](design/WORKER-MODEL.md) §4, §8.

- **Cron: the declaration sync is read-first (CRON-MODEL §4.1)**. The ticker loads the schedule
  row and upserts only when it is absent, the declaration digest changed, or `enabled` flipped; the
  steady state performs no write. The per-tick upsert raced the drain/assembly writers on sqlite
  (a real boot tick failed with `database is locked`) and amplified write-lock contention for no
  data change.

- **Cron: worktree turns stop blocking the ticker (CRON-MODEL §5.4, §7.3)**. The production
  dispatcher now spawns each worktree turn on its own OS thread, bounded by a process-wide
  `superagent.cron.max_concurrent` ceiling (default 4; an explicit `0` refuses boot) and by the
  existing per-schedule `max_concurrent`. A turn over the ceiling is a recorded `skipped_overlap`
  (`global concurrency ceiling N reached`), and a long turn no longer suspends every other
  schedule's tick. The turn's outcome (state, worktree path, error, real finish time) is drained
  by a later tick and settled on the firing row; the in-process ledger is cleared as it drains, so
  the ticker's memory stays bounded. `kian cron run-now` and the unit suites keep the synchronous
  arm. Contract: [design/CRON-MODEL.md](design/CRON-MODEL.md) §5.4, §6.2, §7.3, §9.1, §9.2;
  [docs/SPEC.md](docs/SPEC.md) cron P1.
- **Confirmation plane: the promote gate and the open-call crash repair (CONFIRMATION-MODEL §7
  R1b, §15.6)**. While an `ask` waits, an ordinary message row is no longer promoted into context:
  promoting it interleaved a user message between the assistant `tool_calls` and its answered
  `tool/result`, and the resumed request was rejected by strict endpoints (`kian-orchestrator
  consume_inputs` skips message plans while `pending_calls` is non-empty; control/confirm rows still
  consume). The wake follows the gate — a decision-blocked settle does not re-register the wake and
  the startup inbox rebuild skips such sessions unless it also sees a control row, so the wait
  cannot spin. A drain that died inside a tool batch is repaired at claim time, before any
  promotion: `kian-core::repair` names every unpaired call that is not a logged `ask`, the engine
  reconstructs a missing `tool/call` (additive non-context `recovered` marker; no fabricated
  `permission`) and appends one honest-error `tool/result` (`not_dispatched` / `in_flight`). Logged
  approvals stay on the at-least-once re-invoke path. Contract:
  [design/CONFIRMATION-MODEL.md](design/CONFIRMATION-MODEL.md) §7, §15.6;
  [design/EVENT-MODEL.md](design/EVENT-MODEL.md) §4.2.

- **Cron: firing settlement and the real `skip` judgement (CRON-MODEL §5.1, §9.1)**. A resident
  firing now settles from its target session's end (a throttled sweep reading the stream:
  `completed` → `succeeded`, `failed`/`interrupted`/`cancelled` → `failed`; `awaiting_input` keeps
  it open), and the worktree arm persists its known outcome (state, worktree path, error) through
  `finish_firing` — so `cron list` / `cron logs` answer "did it run" instead of showing `admitted`
  forever. The default `overlap: skip` is now enforced on the resident arm too: the dispatcher asks
  `Engine::has_open_work` (running drain / eligible input / pending decision) and records
  `skipped_overlap` instead of admitting a duplicate run behind the single-flight drain. Each
  schedule's `worktree.repo` is honoured per schedule (one dispatcher per isolated schedule; the
  first-schedule-wins collapse is gone), and `kian cron run-now` cuts from that schedule's repo.
  Contract: [design/CRON-MODEL.md](design/CRON-MODEL.md) §5.1, §6, §9.1, §10.

- **Cron: the v1 scope is fail-closed at load (CRON-MODEL §3.3 rules 6–8, §5.1, §5.5)**. A
  declaration now refuses to boot when it asks for a policy v1 cannot enforce: `overlap: queue` /
  `kill-previous` (no running-set queue and no cross-replica kill path), an explicit `timeout_ms` on
  a resident-arm schedule (the scheduler cannot interrupt a turn it does not run), and a schedule
  targeting the watchdog session (`cron_targets_watchdog`, rule 7 — previously documented but not
  enforced). The undocumented `sandbox` block, the phantom `last_outcome` cursor column and the
  `type: cron` channel are gone from the contract; a `{channel}` target is documented as the
  deterministic plain-session namespace it is. `kian cron run-now` refuses a resident-arm schedule
  instead of running its payload in a worktree the schedule never asked for. Contract:
  [design/CRON-MODEL.md](design/CRON-MODEL.md) §3.2, §3.3, §5.1, §5.5, §10.
- **Cron: the declaration reaches the store (CRON-MODEL §4.1/§12.3 fix)**. The resident ticker now
  syncs each declared schedule into `cron_schedules` before it decides anything: a fresh deployment
  writes its row instead of reporting `no row` on every tick, a store row deleted behind a running
  server (`kian cron rm`) is re-created from the config on the next tick, and the upsert carries the
  §4.1 declaration identity (sha256 over the normalized expression, the effective zone and the
  target) so an edited declaration resets the cursor in the same atomic statement (both digests
  logged) while policy-only edits (`prompt`, `overlap`, `catch_up`, timeouts) leave it untouched.
  Contract: [design/CRON-MODEL.md](design/CRON-MODEL.md) §4.1, §12.3.

- **Durable confirmation plane — `ask`, `awaiting_input`, `control/confirm` (CONFIRMATION-MODEL
  phase 1, ADR-036)**. A tool call can be deferred to a person: `permissions.<tool>: ask` logs the
  call (a widened `permission.evaluated` value), never invokes it, and the drain ends with a fifth
  end reason `awaiting_input` (`running → idle`, resumable) instead of `completed`. The pending set
  is a pure projection (`kian-core::confirmation::pending_calls`, no table, no inbox status column,
  no held lease). The answer arrives as a reserved control-envelope row (refused by the public
  `submit`/`steer`/`queue` wire — the guard now covers the whole `{"kian":{"type":"control",…}}`
  family, closing an existing hole) and is consumed into the 23rd kind `control/confirm`
  (non-context, `DrainActivity`); the loop then applies it in call order — approve invokes the
  frozen call and appends the real `tool/result` with additive `confirmation` provenance, reject
  appends the honest-error result with `policy:"ask"`, and an answer older than
  `confirmation.ttl_ms` is admitted as a durable reject-expired. No provider request is issued while
  a call is unanswered, and the answer appends at the tail, so the resume keeps the prefix bytes
  (cache stability). New control-plane methods `confirm`/`pending` (+ SDK + `kian confirm` /
  `kian pending`), no new error code. Fail-closed boundaries: unknown permission values refuse boot,
  and `ask` on a superagent worker agent refuses boot until phase 2 (no worker waiting path).
- **Worker binding release + idle reclaim (R18)**. A worker thread that ended `interrupted`/`failed`
  no longer holds its 1:1 slot forever: `release_worker` / `kian workers release <id>` closes it with
  one atomic `drain/claimed` + `session/ended{cancelled}` batch (fresh claim epoch; additive
  `released` end payload), and a `kian serve` maintenance tick reclaims stale idle-ended threads
  after `workers.release_after_ms` (default 300 000, `0` disables). Release is refused while the
  thread is running, has eligible input, or has a live `pending`/`inflight` command for its worker;
  the close is terminal, so it can never be resumed or rebound. Contract:
  [design/WORKER-MODEL.md](design/WORKER-MODEL.md) §3, [design/API-CONTRACT.md](design/API-CONTRACT.md)
  §4, [ADR-035](design/decisions/ADR-035-worker-release-and-idle-reclaim.md).
- **Cron scheduler: declarative wall-clock turns (P0/P1)**. `superagent.cron.schedules` declares
  five-field or `@`-alias expressions in an IANA zone; each due slot is claimed once in
  `cron_firings` (migration v7, sqlite + postgres) and runs as an ordinary turn — on the resident
  engine, or in a detached per-firing worktree when `worktree:` is declared, with disk guards and
  `keep_on_failure`. Includes catch-up coalescing, DST-correct next-fire math, the
  `kian cron list|run-now|logs|rm` CLI, and a non-context `cron` proof on the admitted input
  (prompt bytes unchanged). Contract: [design/CRON-MODEL.md](design/CRON-MODEL.md),
  [ADR-034](design/decisions/ADR-034-cron-scheduler.md), [docs/SPEC.md](docs/SPEC.md) §5.
- **Multiagent P4: `depends_on` + `group`, readiness gating (round 17, C7)**. With
  `superagent.board.gating: true`, `spawn_worker` grows `depends_on` (task keys) and `group`: the
  spawn is refused — before any side effect, from the primary only — until every named task has a
  successful completion (`empty_output` never releases; `completed_unverified` only under a policy
  that accepts it), and a self-reference or cycle over the recorded task graph is refused naming
  the cycle. The parent's `thread/started` audit group carries the declared facts, and `kian
  board` gains the `depends_on`/`group`/`ready` columns, flipping `evidence_provenance` to
  `"recorded"` only where the committed `evidence_ref` key exists. `superagent.multiagent.join`
  validates `all` | `any` | `quorum:<n>` and refuses to boot on `any`/`quorum:<n>` (no evaluation
  site in v1 — deferred with fan-in). Gating off is byte-identical P3. Contract:
  [design/MULTIAGENT-MODEL.md](design/MULTIAGENT-MODEL.md) §4.4/§7.3, [docs/SPEC.md](docs/SPEC.md) §5.
- **Multiagent roster: the per-assignee child surface and the output-schema gate (round 17, C1–C3)**.
  `superagent.roster` names delegation entries over `agents:` definitions; `spawn_worker` grows
  `agent`/`task_key`/`output_schema` and `kian agents` lists the roster. A roster-spawned child runs
  its entry's own slots/budgets with the structural `agent.tools ∩ tools_allow` surface (denied tools
  never reach the request schema), resolved from the committed wrapper — a vanished assignee runs a
  refusing side, never a wider one. The child's `output_schema` is enforced by a gate with one
  correction attempt; a second miss commits `evidence/verdict{gate:"output_schema"}` and reads as
  `invalid_output`, preserving the raw answer. Roster off is byte-identical to the pre-roster bundle.
  Contract: [design/DESIGN.md](design/DESIGN.md) §4.8 and
  [design/MULTIAGENT-MODEL.md](design/MULTIAGENT-MODEL.md) §7.3.
- **Delegation task board — the read-only operator surface (kian board / kian tasks, round 17, C5)**. Two new
  `kian` verbs over the fifth view's rows (R17 C4): `kian board [--db] [--config] [--json]`
  renders one row per delegated task (`task_id`, `assignee`, `attempt`, `outcome`,
  `thread_status`, `gate`, the derived evidence pointer) in deterministic `task_id` order, and
  `kian tasks --all` prints the same rows flat (`--all` is required in v1; the bare form refuses
  with usage). Both are strictly read-only (`open_read_only` — no events, no inbox writes, no
  view updates, no ops RPC), served from the fresh `task_board` view when
  `superagent.board.enabled` and `read_model.views.enabled` are both on (the declared staleness
  bound is the rebuild interval) and from the honest full-stream derivation otherwise — identical
  operator output on both paths. `--json` emits the frozen document `{now_ms, version, tasks}`
  (pin P3-3's row key set exactly; absent facts are never fabricated — `evidence_provenance`
  stays `"derived"` until P4 records the durable link). Board off (the default): both verbs
  refuse with exit 2 naming `superagent.board.enabled` before the store is even opened.
- **Bounded queue re-drive (ADR-033)** — `exec_queue` rows carry `dispatch_attempts` (exec migration
  v6, additive, sqlite + postgres); the claim statement counts a dispatch and refuses an `inflight`
  re-drive at `1 + workers.queue_max_redrives` (new config knob in `kian.yaml` under `workers:`,
  default `1`, `0` = first dispatch only, validated ceiling `16`), and a spent bound converges the
  row `inflight → abandoned` once no live lease is held — one audit line naming command id, endpoint,
  attempts and `tool_call_ref`, no fabricated result. This fixes an unbounded replay observed live:
  a command whose own side effect destroys its ack path (`systemctl restart kian`, `pkill kiand`)
  was re-dispatched on every reconnect forever, because the caller-applied `queue_timeout` bounds
  only rows someone is waiting on. No new status, event kind, wire frame or prompt byte; a caller
  re-attaching after convergence reads `abandoned` and gets the existing honest timeout. Semantics:
  [WORKER-MODEL §4](design/WORKER-MODEL.md), placement [DESIGN](design/DESIGN.md),
  decision [ADR-033](design/decisions/ADR-033-bounded-queue-redrive.md).

- **Hook plugins — the two-tier harness-extension contract (round 16, [PLUGIN-SPEC](../design/PLUGIN-SPEC.md) v2.2,
  [ADR-028](../design/decisions/ADR-028-harness-hook-plugins.md))**. A closed hook vocabulary over seven
  interception ports (admission, turn, tool, recovery, settlement) with a tier-0 compile-time registry
  (`kian-plugin::hooks`) and a tier-1 sidecar tier: operator-trusted child processes over a stdio
  JSON-lines ABI with digest-pinned spawns, `env_allow`-only environments, per-dispatch deadlines, a
  failure window with one-strike quarantine, and grant-keyed fail-closed/fail-open class defaults. New
  `sidecars:` config section (instance manifests + server/worker scopes, set digest + session-consistency
  quarantine); `tool/pre` denials surface as honest-error `tool/result`s with the additive
  `tool/call.permission.policy:"hook"` value naming `<plugin>:<hook>` (the round's one EVENT-MODEL
  amendment). Ships with an in-tree reference sidecar — a Python stdlib `tool/pre` approval gate under
  `examples/hook-approval-gate/` — and no `sidecars:` section leaves behavior byte-identical.

- **Agent memory Phase 1 — notes as events + the dreaming drain (round 8, kian-84..87)**.
  One additive event kind: `memory/note`, the 22nd durable kind (non-context,
  `DrainActivity`, emitted only by a dreamer drain; the durable set grows 21 → 22). `kian-core`:
  the fail-closed `memory` module (`parse_note`, `project_notes`, `verify_note` — unknown status →
  `candidate`, verified without valid `verified_by` → `candidate`, empty/inverted provenance
  dropped). New bundle crate `kian-memory`: the deterministic dream prompt (machine-parseable
  header carrying the source range), the fail-closed note parser (provenance always from the
  header, never the model; `verified` survives only with a `transcript` method and an in-range
  ref; over-`max_notes` refused, not truncated), and `DreamDriver` — wraps the built-in ReAct
  loop, appends one `memory/note` per valid draft after `Settled`, dedupes on
  `(subject, derived_from)` so a crash-resume writes no duplicates; a parse failure appends
  nothing and counts (honest zero). `kian-cli`: the `memory:` config section (absent = memory
  off, byte-identical; present parses fail-closed), the `memory_dream` ops method (admits ONE
  durable marked inbox row — the ops-plane dream envelope `{content, dream}` (the rendered prompt
  plus the trusted server-set `dream` marker) under idempotency key
  `dream:<source>:<from>:<to>`; out-of-bounds, over-cap and unconfigured refused honestly,
  −32602/−32005/−32006), and
  the `kian memory create/dream/list/show/verify` verbs. Ordinary sessions keep the exact same
  loop handle — request bytes are byte-identical with memory off. No
  injection, no cadence, no representations (P2/P3 stay designed in SPEC §5). Closeout: SPEC §5 P1
  `designed` → `landed`, MEMORY-MODEL status + §3.1 canonical-payload pointer + §5 D1
  message-row refinement and memory-session policy, DESIGN §3.1/§7, README, this entry.
- **Memory round-8 repair (kian-91, from the kian-90 review)**. Four defects fixed.
  B-1: `memory_dream` refuses
  any range outside `1 <= from_seq < to_seq <=` the source head (typed `SourceRangeOutOfBounds`,
  `-32602`) — no `verified` note can carry provenance past the end of the source stream. B-2:
  dreamer routing and provenance read the trusted, server-set `dream` marker on the admitted row
  (the exact two-key `{content, dream}` envelope, consumed to one marked `input/admitted`);
  a forged `[kian-dream: …]` header in a plain `submit` is ordinary content and routes nothing.
  M-1: note `text` passes the `SecretRedactor` before append; the value-free report rides the
  additive `redactions` key (`[{kind,name,count}]`, the `tool/result` shape). M-2: harvest folds
  within one answer by subject keeping the LAST revision, then stream-dedupes on
  `(subject, derived_from)`. Docs reconciliation
  (kian-92): MEMORY-MODEL §3.1/§3.3/§5/§9/§11 aligned to the landed bytes (seq-string `ref`,
  marker-based D1, byte-cap-only D3 with the head bound, no `scope_tags` in the payload, the
  landed CLI verbs and `/metrics` fragment), SPEC §5 P1 row and this entry.
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
  Owner-gated next steps remain: making the repo public and executing the remaining gated
  surfaces with real credentials (live provider smoke, live lark channel). Tracked on the
  kanban board, not here.
- **Read model P0 — versions, unchanged/delta reads, resumable SSE, drain context cache
  (round 9, R9-02)**. The serving-form contract's first phase
  (`design/READ-MODEL.md` §3/§4.1; API-CONTRACT §3.1/§3.2/§5 promoted from planned §5.3):
  `projection` gains `version` (u64 = `head.next_seq - 1`) and the optional `since` —
  `since == version` answers `{"version":V,"unchanged":true}` with no state and costs exactly
  one seq-axis query (`EventStore::version`, a PK-index min/max lookup; `head`'s unindexed
  `MAX(epoch)` aggregate would make it O(history)); `since > version` is `-32602`, never a
  silent resync; the payload is always derived from one committed prefix (the scan's end bound
  pins the version). SSE `kian` frames carry `id: <seq>`; a reconnect with `Last-Event-ID`
  resumes after that seq without re-emitting the pre-resume prefix; ahead/non-numeric resume
  ids are one `event: error` + close; a terminal stream resumed at head emits `done`
  immediately; polling is tail-only (`kian_orchestrator::read::events_after`: one version
  query + one bounded scan — never `rebuild()` per poll). `DrainCtx` keeps one committed-event
  Vec per drain episode, tail-extended per iteration, with `compact` as the single
  full-re-derivation point (R5); the returned Vec is byte-identical to `rebuild` (request bytes
  untouched). SDK additive:
  `projection_since`, `ProjectionVersioned`, optional `version` on `ProjectionResult`,
  `SseFrame::KianWithId`, `events_from`; old fixtures still parse. Zero new event kinds;
  migration chains untouched. READ-MODEL §11 P0 `planned` → `landed`; DESIGN §3.2 pointer updated.
- **Read model P1 — verified snapshots (state + model context) and tail replay (round 9,
  R9-03)**. The serving-form contract's second phase
  (`design/READ-MODEL.md` §5/§9/§11): migration **v3** appended to BOTH chains (v0–v2 frozen) adds
  `projection_snapshots` —
  `(session, version, state, context, context_provenance, digest, verified, created_at)` — and
  `maintenance_leases`; the two backends' chains stay generation-for-generation identical
  (drift test). Snapshots are infrastructure state, never truth (R1): rows are disposable and
  the table can be dropped without touching the stream. Publication is **verify-then-insert**
  (R6): the builder — outside every drain, under a per-session maintenance lease (liveness
  only) — pins version `V`, reads `1..V` twice independently, derives
  `(state fold, context, provenance)` from both, and publishes one atomic INSERT of an
  already-verified row (`ON CONFLICT DO NOTHING`: insert-only, idempotent across replicas);
  a crash window leaves nothing or one complete verified row, never a partial snapshot;
  superseded rows GC to keep-N. The `state` column carries the full fold seed (state + the
  episode-end first-write-wins bit) so tail replay mirrors `project` exactly; `context` is
  the exact `context_to_bytes` serialization plus per-message provenance
  (`kian_core::context::context_provenance`, derived by the same fold as
  `derive_model_context` — aligned by construction; `derive_model_context` bytes unchanged).
  The loader picks the highest verified `V <= head`, re-checks the digest over the stored
  columns, and replays `(V, head]` (state via the public transition-table fold; context by
  appending the derived compact-free tail slice); a `compact` in the tail is the R5 single
  invalidation — one full re-derivation, exactly the P0 DrainCtx behavior; missing/corrupt/
  unverified rows, a rejected replay, or a tail beyond the configured cap → honest full
  rebuild. Projection and drain takeover state reads use the loader (`Engine::
  new_with_read_model`); the wire never changes. Config gate `read_model.snapshots`
  (`enabled` default **true** with a conservative `min_events` 2000, `tail_cap` 8192,
  `keep_n` 3, `interval_ms` 30 000; fail-closed validation; disabling hides every snapshot);
  `kian serve` spawns the maintenance ticker. Zero new event kinds; no history rewrite; no
  request-byte change (snapshot path bytes == `context_to_bytes` of the full derivation). READ-MODEL
  §11 P1 `planned` → `landed` + header updated.
- **Read model P2a — materialized views (directory, usage, ticket stage, kind index) (round 9,
  R9-04)**. The serving-form contract's third phase
  (`design/READ-MODEL.md` §6/§9/§11): migration **v4** appended to BOTH chains (v0–v3 frozen) adds
  one physical `view_rows`
  table (logical views
  `session_directory` / `session_usage` / `ticket_stage` / `session_cache` keyed by name,
  canonical-JSON payloads) plus a per-view `view_state` row (store key `(events_total, max_seq)`
  + sha256 digest over the row set), and implements the declared `audit_kind_index` as a plain
  SQL index `idx_events_kind_ts` — chosen over a view table because `kind`/`ts` are already
  projection columns of the truth table: an index is append-time fresh, duplicates nothing and
  can never disagree with the stream (justification in the migration comment and READ-MODEL
  §6). `session_cache` is declared additively in §6 by this change (the plan requires
  `cache-stats` served from views; §10's additive protocol). Views are disposable
  observability accelerators, never truth (R1/R8): no control read consumes one (the orchestrator
  sources never reference the view surface). The freshness key is `(COUNT(events), MAX(seq))` — the
  count axis is load-bearing
  because `seq` is per-session and an append to a shorter session moves only the count
  (`store_key_changes_on_any_append`). A view is served only when the live key equals the
  derived-at key exactly (no TTL), the digest over the read rows matches (corruption check —
  tampered rows break it, `tampered_rows_break_the_digest`), and every payload decodes; stale,
  corrupt, undecodable, absent (a read-only reader of a pre-v4 store — `absent_tables_read_as_no_view`,
  `pre_v4_read_only_reader_falls_back`) or disabled → the honest full stream derivation, which
  stays the truth path (`stale_view_falls_back_to_full_derivation`,
  `corrupt_view_is_discarded_and_the_gate_hides_views`). Rebuild runs outside every drain under
  the `views:all` maintenance lease (liveness only; `rebuild_lease_excludes_concurrent_rebuilders`),
  pins the key BEFORE reading any stream, and publishes each view as ONE transaction (rows +
  state + digest together): a crash leaves the old complete version or the new one, never half
  a view (`publication_is_atomic_versioned_and_digest_covered`). Operator surfaces consult
  fresh views first with identical numbers by construction — the builder calls the SAME
  derivations the honest paths run (`session_usage_report`, `session_cache_report`, and the
  `kian tickets` board extraction `ticket_board`/`ticket_rows`); `kian tickets`, `usage-stats`,
  `cache-stats` and the `/metrics` aggregates fall back per-read. Declared staleness
  (READ-MODEL §6): event-derived fields are exact-version fresh; `ticket_stage`'s
  `pending_inbox` (and the stage's dependence on it) is as of the last rebuild — an inbox row
  can become eligible with no append. Config gate `read_model.views` (`enabled` default **true**,
  `interval_ms` 60 000, fail-closed validation; disabling hides every view);
  `kian serve` spawns the rebuild ticker. Equivalence executed:
  `rebuild_equals_full_derivation_on_a_multi_session_store` (directory facts, usage totals,
  cache counters incl. compacts, and the full ticket board — view rows decode into exactly
  `ticket_board`'s rows at the same `now`); `pg_views_publish_reads_and_freshness` (env-gated,
  live against PG 17.11); `from_str_name_round_trips_every_stage` (the stored stage names);
  `read_model_views_default_enabled`, `read_model_views_section_parses_and_gates` (config);
  `v4_generation_sql_is_frozen`, `pg_v4_generation_sql_is_frozen` (migrations). Benchmark
  (`cargo bench -p kian-cli --bench read_path -- views`, sqlite, criterion 0.5, M-series
  laptop, 1 000 sessions ≈ 5.2k events: 800 plain + 200 workers): ticket board from the fresh
  view **486 µs** vs **30.5 ms** full derivation (≈63×); usage aggregate **1.63 ms** vs
  **28.4 ms** (≈17×); one maintenance rebuild of all four views **66.5 ms**, off the serving
  path. READ-MODEL §11 P2a `planned` → `landed` + header updated.
- **Read model P2b — opt-in export-only retention, archive verify/import (R9-05)**. Retention is strictly opt-in (`retention:` in `kian.yaml`, default **off** —
  absent/vague parses fail-closed: an enabled retention without `archive_dir` or without at
  least one online-window threshold (`min_age_ms`/`min_bytes`) refuses to start;
  `retention_defaults_to_off`, `retention_enabled_parses_fail_closed`). Migration v5
  `archived_sessions` on both chains (v0–v4 frozen, goldens `v5_generation_sql_is_frozen` /
  `pg_v5_generation_sql_is_frozen`): one infrastructure marker row per archived session —
  range, counts, package path, per-file + manifest sha256. The package
  (`<archive_dir>/<session>/`): `events.jsonl` (the exact `session export` bytes, shared
  serializer), `inputs.jsonl` (full inbox rows incl. `consumed_by_seq`/`admitted_seq`) and
  `manifest.json` (format_version 1, seq bounds, counts, per-file + aggregate sha256,
  created_at, backend, terminal state), written via temp dir + one atomic rename. The
  two-phase commit (`kian-store` `archive_commit`, the ONLY events/inputs delete site — the
  crate's append-only review assertion now carves out exactly `archive.rs` and a second
  assertion pins the carve-out to that one file): marker insert under CAS + online-row delete
  in ONE transaction, fenced on the online event count the verified package covers — a session
  that appended after verification aborts `OnlineMoved` and deletes nothing
  (`commit_refuses_an_online_stream_that_moved`); first marker wins
  (`commit_refuses_a_conflicting_marker`); retry with the same package is the idempotent
  no-op (`commit_deletes_online_rows_and_writes_the_marker_atomically`). Eligibility is
  terminal-only — the last committed event must be `session/ended`
  (`candidates_lists_terminal_sessions_only`,
  `candidates_ignores_a_mid_stream_end_event`). Reads answer the typed `archived` error:
  `-32007` (API-CONTRACT §2.2; the plan's planned `-32005` had been taken by the round-8
  memory registry, so `archived` is the next free code) on `projection` and the SSE stream
  (`archived_error_maps_to_the_archived_code`); `session inspect` reports archived + the
  package path; `session export` serves the package byte-identically (hashes re-verified on
  the way out); `session replay` refuses online replay pointing at import. `kian archive
  run|verify` and `kian session import <package>` (manifest verified, corrupt package or
  existing session id refused, events restored under their own epoch/seq, input rows
  restored verbatim). The "inbox audit-visible forever" wording in `migrations.rs`/`inbox.rs`
  is amended to "online store or verified archive package" (READ-MODEL §7). READ-MODEL §11 P2b
  `planned` → `landed` + §7 landed shape +
  header; API-CONTRACT §2.2/§3.2/§3.3/§5.3 updated. No new event kind; no transparent cold
  tier; no automatic retention cadence.
- **Read model P3 — explicit-allowlist fan-in event stream (round 9, R9-06)**.
  `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]` (API-CONTRACT §5.4, normative; change
  class additive): an explicit allowlist of at most 64 sessions per connection (counted before
  dedup), **no wildcard** (`*` refused by name — the owner gate), HTTP 400 with a plain-text
  body on missing/repeated `sessions`, a non-UUID id, an empty entry, over-cap, or a `kinds`
  entry outside the EVENT-MODEL catalog (the validation table is pinned to `EventKind::as_str`
  by `known_event_kinds_table_matches_the_core_catalog`). Tail-only: the connection establishes
  each cursor with ONE head query (new `ControlPlane::session_version`) — pre-connect history is
  never replayed (`fan_in_interleaves_two_sessions_tail_only`); each poll cycle reads the same
  bounded `events_after` tails §4.1 uses, so per-poll work stays O(new events). Frames carry
  `id: <session>:<seq>`; no cross-session ordering is promised; per session the seq is strictly
  increasing. An unknown or archived listed session emits exactly one `event: error` frame
  naming it and the stream continues (`fan_in_unknown_session_errors_once_and_continues`); the
  `kinds` filter applies server-side and a terminal session is delivered through its
  `session/ended` event then dropped, the stream closing when every listed session has ended or
  errored — no `done` frame (`fan_in_kind_filter_and_terminal_close`). Backpressure is bounded
  twice over: ≤ 512 frames per yielded cycle (the cursor advances only to the last emitted seq,
  so nothing is dropped or re-emitted), and a stalled consumer suspends the poll loop outright —
  the server never buffers unbounded data on its behalf. Config `api.event_stream.enabled`
  (default **true**, fail-closed parse; disabled mounts no route — 404:
  `fan_in_disabled_by_config_forty_fours`). `/metrics` gains the `event_stream` object
  (`subscribers`, `sessions_watched`, `events_sent`; a connection-lifetime guard settles the
  gauges on disconnect: `fan_in_metrics_track_the_open_connection`,
  `fan_in_metrics_settle_to_zero_after_drop`). The per-session resume surface is untouched
  (`per_session_stream_resume_unaffected_by_fan_in`; the full resume suite stays in
  `read_path_e2e.rs`). `kian-sdk`: the SSE parser splits compound ids additively
  (`SseFrame::KianFanIn`; old id-less and numeric-id fixtures still parse) and
  `KianClient::fan_in` sends the exact wire request (byte-level request-line pinning in
  `golden_wire.rs`, incl. the 400 mapping). No authz change: the deployment remains the trust
  boundary and the allowlist is explicit ids only. READ-MODEL §4.2 landed shape + §11 P3
  `planned` → `landed` + header; API-CONTRACT §1/§5.3/§5.4/§6/§10.2. No new event kind; the
  planned `read` metrics object remains planned (P0-owned surface).
- **Read model review R9-08 fixes — package identity, archived aggregate reads, snapshot trust
  (R9-08a)**. Fixes the three findings of the round-9 independent review
  (REQUEST-CHANGES; fix card opened from it). **F1 package identity:**
  `session import`/`archive verify` now refuse a package whose rows name a different session
  than its manifest — `verify_package` cross-checks every `events.jsonl` event's and every
  `inputs.jsonl` row's `session` against `manifest.session`, refusing with the line number and
  both session ids (the manifest is not self-hashed, so hand-editing `session` alone passed
  every hash check; a hash-consistent identity-inconsistent package previously imported,
  restoring events under one id while reporting another). `verify_package` now returns the
  parsed rows (`VerifiedPackage`) and `run_import` appends exactly those rows — the files are
  never re-read after verification (the verify/use gap is closed). **F3 archived aggregate
  reads:** `usage-stats --session` and `cache-stats --session` check the archived marker first
  and refuse with the package path and import guidance — never a silent zero report (R7's
  "any read whose range touches archived data MUST report `archived` explicitly";
  READ-MODEL §7 read behavior updated). Whole-store aggregates are unchanged (online window
  only). **F2 snapshot trust documented:** READ-MODEL §5 gains the explicit trust boundary —
  load-time verification is builder attestation + digest↔columns integrity; a
  poisoned-but-self-consistent row (columns rewritten together with a recomputed
  `kian-snapshot-v1` digest, `verified=1`) is not detectable at load and is served; the table
  sits inside the same trust boundary as `events`, must never be copied between stores, and
  drop/rebuild from the stream is always safe. This corrects the P1 entry's wording above:
  `corrupt_unverified_and_tampered_snapshots_are_ignored` covers digest-stale tampering (and
  unverified rows) only — it is not a general tamper guarantee, and the new
  `self_consistent_poisoned_snapshot_is_served_builder_attestation_only` pins the documented
  (served) behaviour so the claim and the test agree. No Merkle/proof mechanism (a deliberate
  non-goal). **F5 note (SDK compat):** `SseFrame` (`kian-sdk/src/sse.rs`) is not `#[non_exhaustive]`;
  marking it is itself a source-compat change for downstream exhaustive matches, so the
  decision is deliberately deferred to the SDK freeze — downstream matches should carry a `_`
  arm meanwhile. Docs drift (F4) and default-on maintenance cost (F6) are owned by follow-up
  cards, not this change. No new event kind; no wire change; no migration.

## 2026-11-15 — worker enrollment + worker-declared capabilities (ADR-031, ADR-032)

A device worker (the iPhone design in [docs/runbook-ios-worker.md](docs/runbook-ios-worker.md))
exposed three gaps in the R12 worker domain; this change closes them with per-worker enrollment, a
worker-declared capability list on the wire, and a configurable dynamic queue bound. ADR-031's
config-owned capability catalog was superseded by ADR-032 before release; the final shape is
wire-declared.

- **Per-worker tokens (ADR-031 D1)** — `workers.token_env` is now optional; `workers.tokens` maps an
  id to its own env-var name. A listed id authenticates only against its own token (constant-time,
  env-only, redacting); `workers.enrolled_only: true` admits only listed ids and forbids the shared
  fallback. The R12 shared-token impersonation limitation **narrows** (a shared-token holder can no
  longer replace an enrolled id); it does not disappear in hybrid mode, and mTLS stays the R13 fix.
  Refusals reuse `bad_token` — strict mode reveals nothing about whether an id is enrolled.
- **Worker-declared capabilities (ADR-032, KiandProto v3)** — `register.capabilities` is an optional
  list the worker sends about itself. The worker validates its own list at startup (lowercase
  `[a-z0-9._-]`, 1..=64 bytes, ≤16, unique) and refuses to boot on a bad entry; the server
  re-sanitizes (trim, drop invalid/duplicate/over-limit) with one audit line and the registration
  still succeeds. The accepted list is the live directory fact for connected members and a new
  nullable registry cache column (migration v5) for registry-only ids; `list_workers` and
  `kian workers` serve the live or cached list, and the cached row's `declaration_stale` now covers
  capabilities too. Result-only: never a prompt, a tool schema or an event. Static pins have no
  declaration in v3 (`hello` unchanged) and omit the field.
- **Dynamic queue bound (ADR-031 D3)** — `workers.queue_timeout_ms` joins the backend's single queue
  bound (max over static pins and this value). The bound is global by construction: tuning it for an
  intermittent phone also lengthens every queued command's honest timeout. Recorded, not hidden;
  per-id timeouts/rebind remain out of scope.
- **Config validation** — at least one token source; strict mode requires enrollment and forbids
  `token_env`; token env names and worker ids validate fail-closed. The `workers.capabilities`
  config field from the first phase is removed (one source: the worker's declaration).
- **Wire + migration** — `PROTOCOL_VERSION` moves 2 → 3 in both halves; no compatibility shim (a v2
  worker reads offline with rows parked). Registry migration v5 adds nullable `capabilities` TEXT on
  sqlite and postgres; pre-v5 rows read as an empty list, and a registration that omits the key
  clears the cache. No new event kind, table, HTTP route or ops method.

## 2026-11-15 — per-phase exec waits + honest timeout result + budget stop is `interrupted` (ADR-030)

A delegated worker ran a 23-minute external agent through the synchronous `exec` tool with
`timeout_ms: 3000000`. The call returned `{"error":"execution timed out"}` after exactly 60 s
(`queue_timeout`) while the command kept running; the model could not tell "killed" from "still
running", invented a `sleep 55` polling loop (60 `exec` calls, 865 s of sleep, 32 iterations = the
whole budget, ~890k prompt tokens, then `thread/completed{completed}` over an empty summary), and
the user saw 14 minutes of silence. The real result was discarded
(`late result for a settled command discarded`). Three defects, one decision record.

- **Per-phase waits (1a)** — `RemoteExecBackend` now distinguishes the *dispatch* wait (row
  `pending`, bound `queue_timeout`, expiry abandons — honest, nothing ran) from the *execution*
  wait (row `inflight`, bound `2 * exec_bound + queue_timeout` where
  `exec_bound = timeout_ms.unwrap_or(queue_timeout)`, expiry **never mutates the row**: the queue
  keeps the truth and stays re-driveable, only the watcher stops). `exec_tracked` keeps its
  signature and stays `WaitPolicy::Caller`; the new `exec_tracked_watched` takes the policy, and
  the async `start` observer uses `WaitPolicy::Job` — so a job that outlives the dispatch bound
  finally delivers its **real** result instead of `{"error":"queue timeout"}` at 60 s. Phase-A
  expiry still abandons; phase-B expiry settles the job `abandoned` with the observed state and
  leaves the live queue row untouched.
- **Fail-closed `timeout_ms` (1b)** — the sync `exec` refuses a `timeout_ms` above its own wait
  bound *before enqueueing* (no row, no dispatch), naming both bounds and pointing at `start`.
  Previously such a call burned the full wait and then returned a timeout that read like a kill.
- **Semantic timeout result (1c)** — a remote wait expiry now reports what it observed:
  `row_state` (`pending`/`inflight`/`abandoned`), `still_running`, `row_abandoned`,
  `wait_bound_ms`, `hint`. The four legacy keys and `is_error` are unchanged; the extra keys
  appear only on a state the local executor cannot produce (it kills its own child, so a local
  timeout stays unambiguous and claims nothing about a surviving grandchild).
- **Steering (2/4a)** — the model-visible `exec` description states its bounded wait, the
  discarded result and "never poll with `sleep`"; `start` states the completion arrives on its
  own; `spawn_worker` asks the Router for a one-line acknowledgement right after dispatch.
  Descriptions only — the parameter objects are untouched, so the exec/fs schema-parity tests
  still pass byte-identically.
- **Budget stop is `interrupted` (3)** — the loop's only intentional-stop producer is
  iteration-budget exhaustion, where the last assistant turn is by construction a pending tool
  call with empty text. `reported_outcome` (new, pure, unit-tested) records
  `thread/completed{status:"interrupted"}` with the stop reason as the summary, so the Router's
  report says why instead of handing it an empty success. `session/ended{completed}` is unchanged
  in this change: `thread_liveness` then still reads `Released` (worker reusable), and making the
  end reason `interrupted` would hold the worker until a designed resume/cancel exists — deferred
  to its own ADR.
- **Docs** — new ADR-030; WORKER-MODEL §4 rewritten (the old bullet claimed `queue_timeout` was a
  "bound on an absent worker" while the code also bounded running commands); DESIGN crate map
  updated. The record was minted as ADR-028 and renumbered to ADR-030 on merge: the
  harness-hook plugin record that landed in parallel (R13) already held 028, so the series had
  two claimants. `docs_map` now also asserts ADR numbers are **unique**
  (`adr_series_is_contiguous_from_001_and_unique`) — contiguity alone never caught it.
  No EVENT-MODEL/API-CONTRACT change: `tool/result.output` is opaque JSON and no wire or
  event kind moved.

## 2026-11-14 — last-declared worker cache + durable WeChat reply tokens (P1)

Two cold-start/reply gaps found in the owner deployment are closed with additive, best-effort
state; both are isolated from the resident-budget change on a separate branch.

- **Last-declared worker cache (ADR-025)** — `endpoint_registry` migration v4 adds nullable
  `description`/`protocol_version`; the registration upsert writes the declaration, heartbeat and
  detach upserts never touch it, and a registration that omits the description clears the cached
  text (the latest declaration is the fact). The directory read serves the cache only for
  registry-only ids and labels it `declaration_stale: true`; the live directory always wins.
  `list_workers` gains the additive flag on cached rows (live rows byte-identical);
  `kian workers --json` exposes the two fields when declared. Store tests cover reopen,
  heartbeat-preserves and description-clear; the CLI and Router surfaces are covered too.
- **Durable WeChat reply tokens (ADR-026)** — a best-effort, TTL-bounded `0600` state file beside
  the credential file (`<credentials_file>.context-state.json` by default, `context_state_file` /
  `context_ttl_ms` configurable, default 1 h) persists the latest admitted sender's
  `context_token`; a channel rebuilt after a restart can reply without a new inbound. Admission
  never waits on disk (the in-memory map updates first, the write is best-effort with one warn on
  failure); a missing/malformed/expired state file is empty, never a startup failure; a stale
  token still follows the existing `-14` degradation. The token stays out of events, exports,
  logs and metrics. L1 covers write/load/TTL/malformed/0600 and a rebuilt channel sending with
  the persisted token; the live restart story is L1-verified against the mock iLink server.
- **Status** — rebased onto the per-episode budget change (ADR-024); this branch's contracts are
  ADR-025 (worker cache) and ADR-026 (WeChat reply tokens). Not deployed; the live `context_token`
  TTL across a real WeChat restart and a live post-restart cold catalog are unverified until this
  branch is deployed.

## 2026-11-14 — WeChat details reply is delivered whole (no truncation)

- **Defect** — the documented IM flow offers `回复「详情」看完整结果`, but every reply went through the
  same ≤100-unit shaping: when a person took the offer, the complete content the model sent was cut
  at the limit, so the one interaction that promises "the complete result" delivered a prefix. The
  truncation lived in the bundle policy, not the channel — the WeChat adapter already splits long
  text at ~2000 characters losslessly. There was no delivery mode for a details request, only a
  prompt sentence.
- **Change (bundle only)** — `ImPolicy` gains the full-delivery mode (`enforce_full`: complete text,
  `truncated: false`, the unit count still honest). The mode is derived from the Router's own
  committed stream — the latest admitted channel input on the reply's channel, bounded by the
  reply's own `tool/call` seq (log-before-execute) — and matched **exactly** against the closed
  `DETAIL_REQUEST_TEXTS` vocabulary (`详情`, `详细`, `完整结果`, `完整内容`, `details`, …) after
  trimming surrounding punctuation. It is never model-declared and never a tool argument, so a
  pinned old Router prompt still delivers the full result. The receipt carries the additive
  `detail: true` bit (summary receipts stay byte-identical); a long full delivery rides the
  channel's existing ordered split. **No new event kind, no wire, table, config or tool-schema
  change**; the judge prompt gains one details-request sentence (a new-session default — persisted
  prompts replay byte-identically, constraint 3).
- **Re-drive byte-identity** — the mode is a pure function over events, so the outbox boot re-drive
  derives it at the intent's own `tool/call` seq and re-sends the intent's exact bytes; a details
  reply killed mid-send is not re-truncated. `redrive` now takes the stream it already rebuilds
  from.
- **Bounds** — channel-scoped and exact: a passing mention ("详情页打不开") never lifts the budget;
  a newer inbound message on the same channel before the reply leaves the summary budget in force
  (the derivation is bounded by the call's seq, so mid-turn admits cannot flip it); full delivery
  has no unit cap — the transport split is the only boundary.
- **Docs** — DESIGN §4.2 (IM row + the derivation paragraph), CHANNEL-WECHAT §4 (output shape +
  details reply), EVENT-MODEL §3 (`channel/delivered` additive `detail`), SPEC §1/§4 wording.
  No ADR: bundle output-policy behavior, contract recorded in DESIGN/CHANNEL-WECHAT.

## 2026-11-14 — resident iteration budget is per episode (ADR-024)

- **Incident root cause** — the ReAct iteration budget was session-lifetime scoped, so the resident
  Router ended `completed` at its 32nd turn; later WeChat messages were admitted as durable rows on
  the frozen stream and never consumed (no reply) until a `kian serve` restart ran the boot-time
  re-arm. The deployment's own `drain.max_iterations: 32` override made it immediate, but the
  default is also finite — the trap was the product's, not just the config's.
- **Change (ADR-024)** — `ReActConfig` gains `BudgetScope::{Lifetime, PerEpisode}` (default
  `Lifetime`). The super-agent assembly gives the Router and watchdog `PerEpisode`: the counter is
  scoped to one inner-loop drive (one batch/episode), while `turn/start` keeps the lifetime number
  for audit. On exhaustion the loop emits a dedicated stop reason and the `RouterDriver` **parks**
  (the same path as an idle final answer) instead of ending the session; the next input starts a
  fresh episode. One warn audit line plus the additive
  `superagent.router_batching.episode_budget_parks` metric. No event, wire, table or tool-schema
  change; worker/plain loops are byte-identical.
- **Explicit terminals unchanged** — cancel/fatal still end terminally and use the boot-time re-arm;
  runtime re-arm for those is a recorded deferred follow-up (a runtime-rebindable Router binding
  across subscriptions, driver routing, Router tools and worker completion reports).
- **Deployment / L3** — the accidental `drain.max_iterations: 32` override was removed from the
  owner config; kian was rebuilt/redeployed with the fix. With `drain.max_iterations: 1` and three
  mock inputs the Router parked three times and stayed `running` (delivered 2 of 3 before the next
  finding); the stranded messages from the incident were lost (admitted on the frozen stream) and
  were resent.
- **Residual fixed (ADR-027)** — after a reply whose assistant turn carried no
  `reasoning_content` (the model can omit it on trivial turns), the Router's empty-batch turn sent
  a request whose history ended on that assistant message and DeepSeek rejected it with the same
  400; the session went `failed → idle` and the next input recovered, so replies arrived but the
  session churned. A resident ReAct iteration now settles **without a provider call** when
  `take_eligible_inputs` returned nothing and the context ends on an assistant message (tool-result
  continuations and new inputs proceed; worker/plain loops unchanged). No event, wire or config
  change; one debug audit line. L1 predicate test; live re-check: two messages after the redeploy
  delivered both replies with zero `soft failure` lines over the following windows, and a `kian`
  restart with the live Router added no `step/request` and no 400 (previously the empty-batch turn
  fired there); workers re-registered, session still `running`.

## 2026-11-14 — reasoning replay + worker filesystem tools (KiandProto v2)

- **Reasoning replay (ADR-022, landed)** — DeepSeek V4 thinking mode requires the assistant's
  `reasoning_content` to be replayed; kian dropped it, so a resident-Router turn ending on an
  assistant message (the empty-batch judge turn) was rejected 400. `assistant/message` content now
  carries an optional `reasoning_content` (EVENT-MODEL §4.1); the ReAct stream assembler captures
  `delta.reasoning_content` in arrival order and the message → chat conversion re-emits the
  top-level wire field. A legacy message without the key produces byte-identical request bytes:
  `FROZEN_CHAT_BODY_SHA256` is unchanged and the golden test passes. L1: delta assembly + payload
  + replay test; L3: a live Router request replayed 1,479 bytes of assistant reasoning and the
  empty-batch 400 disappeared (the live run no longer logs it). Cost note: replayed reasoning is
  input tokens on thinking sessions only; no trimming or synthesis.
- **Worker filesystem tools (ADR-023, landed)** — a bound worker session's
  `fs_list`/`fs_read`/`fs_write` now execute on the worker's machine. KiandProto v2 adds the
  `fs_command`/`fs_result` frame pair (`PROTOCOL_VERSION = 2`; exec frames keep their v1 bytes);
  the durable queue migration v3 adds `kind TEXT NOT NULL DEFAULT 'exec'` so every pre-v3 row
  decodes as exec and re-drives unchanged; `RemoteFsTool` mounts the three local-identical
  schemas on a bound worker side (filtered by the agent's `tools:`), the refusing side mounts the
  same names with an honest refusal, and `kiand --workspace-root` (default cwd, must exist) plus a
  shared lexical `..` guard and 1 MiB read/write bounds contain the surface. At-least-once applies
  to fs writes (same path/content on a re-drive). L1: fs execution, path/limits, queue
  round-trip + legacy-row decode, schema parity; L2: a real `kiand` over TCP serving
  write→read→list, an honest missing-file error and a no-row refusal.
- **Deployment** — the owner instance was upgraded kian-first to v2 (builder worker and local
  worker re-registered; the WeChat Router path stayed alive). Cross-built with cargo-zigbuild; no
  deployment artifacts in the repo. Honest gap: the v1→v2 window was not exercised with a real old
  worker binary (the typed refusal and the queue's legacy-row decode are tested), and the worker fs
  round-trip over WeChat is verified after the redeploy (recorded below once observed).
- **L3 owner verification (after the v2 redeploy)** — a Router-dispatched task on
  `vultr-builder` used `fs_write` (`{"written":true}`) then `fs_read` (`{"content":"v2-ok"}`) and the
  file appeared at `/srv/work/kian-fs-check.txt`; a second task on the local `mac-mini` worker used
  `fs_write`/`fs_read` and created the file under the laptop workspace
  (`kian-fs-local.txt` = `mac-ok`). Reasoning replay is observable in the live requests: a worker
  session replayed 239/17/621-byte assistant reasoning across its later turns, and the Router
  replayed 1,479 bytes of reasoning on a later turn; no `400`/`reasoning_content` soft failure
  appeared after the redeploy. The WeChat long-poll connection was live after the restart.
- **Incident (honest)** — mid-run the Vultr host stopped answering SSH (TCP accepted, no sshd
  banner) for several minutes; the owner recovered access and the deployment was re-verified. The
  host was healthy after recovery (load 0.06, 7 GB free) and no kian re-drive storm was visible in
  the journal; the cause was not established, so it is recorded as an unresolved operational
  incident, not attributed to the v2 change.

## 2026-11-14 — README corrections + owner-run remote deployment (documentation only)

- **README (corrected)** — `exec_worker` is required whenever worker membership is on: the R11
  sentence "with exactly one worker the argument may be omitted" was superseded by R12 (the schema
  is a plain string and the single-pin default is refused) but survived in the README. The worker
  section now states how a laptop reaches a private listener (SSH tunnel, or a Tailscale
  `100.64.0.0/10` address — in the private set; `workers.allow_public` is the recorded escape hatch,
  not the deployment shape), and the provider note says an OpenAI-compatible gateway is just another
  `base_url` because the provider contract is chat-completions + SSE. No
  code, config-schema, event, wire, or crate change.
- **Owner-run L3 (infrastructure only — no repo artifact, no deployment tooling)** — a
  `kian serve` on a Vultr Ubuntu 22.04 x86_64 host with the registration listener, HTTP API and
  vadis all loopback-bound (UFW: 22 only); vadis as the OpenAI-compatible gateway (the `deepseek`
  provider, model `deepseek/deepseek-flash`); a `kiand` builder dynamically registered on the same
  host; a laptop `kiand` registered through an SSH tunnel; the WeChat iLink channel. Observed live:
  a WeChat message → Router judge → `spawn_worker` → real `exec` on the selected machine → WeChat
  reply; both joins in `/metrics`; a laptop `kill -9` offlined `mac-mini` and a restart re-registered
  it; a `kian` restart re-registered both workers within seconds with the bindings rebuilt from the
  streams. Binaries cross-built on the workstation (cargo-zigbuild, `x86_64-unknown-linux-gnu`) —
  deployment pieces stayed out of the repository by request.
- **Known issue (provider)** — DeepSeek V4 thinking mode requires the assistant's
  `reasoning_content` to be replayed; kian does not carry it. A resident-Router turn whose history
  ends on an assistant message (the empty-batch judge turn) is therefore rejected upstream with a
  400 `format_error`, surfacing as one soft failure that the next admitted input recovers. One
  occurrence in the run; not changed here (a fix is a provider-body/replay decision, not a
  deployment knob). Evidence: the vadis trace records `upstream_status: 400` and DeepSeek's
  `reasoning_content... must be passed back`.
- **Observation (read-model projection)** — `kian tickets` reports completed worker sessions as
  `queued`. The spawn path claims the drain (seq 2) before admitting the delegation input (seq 3),
  so stage arm 5 ("no `drain/claimed` after the last input") fires before the terminal-completed
  arm; reproduced on all three sessions of the run, while `session inspect` shows them `completed`.
  Not changed here (it is a bundle projection question, not an event or wire fact).

## 2026-11-09 — runtime worker membership (self-registration, dynamic join/leave)

Round 12 (cards kian-128..135): workers join and leave a running `kian serve` at runtime.
[ADR-021](design/decisions/ADR-021-runtime-worker-membership.md) is the decision record
(supersedes ADR-020 D5/D6/D7 only — the static catalog surfaces); the worker-domain contract is
[WORKER-MODEL.md](design/WORKER-MODEL.md) (new normative doc); the wire is
KiandProto **v1** ([API-CONTRACT](design/API-CONTRACT.md) §8). **No new event kind (the durable set
stays 22), no new table, no migration, no new HTTP surface, no core change.**

- **Protocol v1 (kian-129)** — `PROTOCOL_VERSION = 1` in both halves with dual goldens; new frames
  `register {endpoint_id, description?, token, version, max_concurrent}` (worker-dial), `bye`
  (endpoint side), `rejected {reason, detail?}`; a closed five-reason refusal set
  (`version_mismatch` / `bad_token` / `duplicate_static` / `id_conflict` / `disabled`, no serde
  fallback); the token travels in a redacting newtype so no `{:?}` can print it;
  `welcome.heartbeat_ms = max(1, offline_after_ms/3)` server-directed. Legacy static endpoints keep
  `hello`→`welcome` at v1. Upgrade order kian-first, no v0 shim: a v0 peer gets a typed refusal +
  audit line, never a half-compatible connection.
- **Registration listener (kian-129)** — private-bind-only (`workers.listen`; loopback/RFC1918/
  CGNAT/ULA — wildcard counts as public; `allow_public: true` overrides), env-only shared token
  (`workers.token_env`, constant-time compare, one attempt per connection, 5-refusals/60 s per-peer
  window, clock-injected in tests), handshake discipline (register must be the first frame, 10 s
  deadline, no half-registered state), duplicate-id replace (serialized per id, old connection
  closed, generation+1, audited) and `duplicate_static` pin protection.
- **Membership directory (kian-130)** — runtime-only `WorkerDirectory`; state is **computed**
  (`online` = live connection ∧ fresh heartbeat; `draining` = teardown window; else `offline`);
  durable projection is the existing `endpoint_registry` row — no migration; the existing heartbeat
  watchdog covers dynamic members unchanged (absence admits the ordinary `endpoint_health` input).
  Stale ids are never pruned: offline-known binding stays honest. `kian workers [--json]` is the
  read-only operator verb (description/version absent after a restart until re-registration).
- **Queue-first dispatch (kian-130)** — `exec` enqueues durably by the bound worker id with no
  live-route requirement; rows park `pending` until the worker registers or the honest
  `queue_timeout` abandons them; runtime routes attach/detach with membership and re-drive
  `pending`+`inflight`; no cross-worker dispatch, no failover, callers survive route swaps.
- **Router surface (kian-131)** — `spawn_worker.exec_worker` is a plain non-empty string (no enum,
  no catalog block — the ADR-020 D5–D7 supersede); one frozen prompt instruction line appended only
  when membership is on; new read-only Router-only `list_workers` tool (sorted by id, membership
  data only in results); spawn validation against the runtime catalog (unknown refused naming known
  ids, offline-known allowed with the `offline — commands will queue` note, busy refused naming the
  owner); lazy per-worker side cache, never evicted; per-side async pairs carried (S3).
- **Two reviewer-found defects fixed** — B1 the constant-time token fold truncated the length
  mismatch to a byte (token + 256k NULs compared equal) — it now folds as
  `u8::from(a.len() != b.len())`; B2 `kiand` lacked worker-side identity/description validation
  (malformed id ⇒ `id_conflict` ⇒ unbounded retry) — register mode now validates both at startup,
  exit 2, no dial.
- **Closeout (kian-135, this entry)** — API-CONTRACT §8 v1 (handshake, frames, reason set, upgrade
  order) + §9.1/§9.2 goldens; WORKER-MODEL.md (proposed → normative) wired into the docs map
  (`docs_map` AUTHORED 10 → 11); ADR-021 status → landed; SPEC §5 R12 section (membership,
  single-instance boundary, shared-token limitation, R13 deferrals); README feature bullet + runtime
  membership advanced-usage example + docs-map row; this entry.
- **Deferred with the round** — mTLS/per-worker identity and token rotation without restart (R13);
  live rebind/retirement/takeover (unchanged, ADR-020/021 §Deferred); multi-instance membership
  federation (out of scope, single kian instance by design); persisting description/version across
  a restart and a stale-id TTL (open questions recorded in the round spec §11); deployment
  (Vultr/WeChat) stays the owner's discussion.
- **Owner follow-up (applied)** — the closeout run could not update AGENTS.md's docs-map list
  (protected file; the required consent timed out), so it landed in the owner follow-up docs
  commit: `design/WORKER-MODEL.md` — normative worker-domain contract (membership lifecycle,
  binding/dispatch, catalog, security boundary); any worker-membership semantic change starts here.
  `docs_map` does not gate AGENTS.md content, so no test was affected.

## 2026-11-08 — durable per-session execution-worker binding (`exec_worker`)

Round 11 (cards kian-122..127): every delegated super-agent worker session is durably bound 1:1 to
one `kiand` execution worker. [ADR-020](design/decisions/ADR-020-durable-worker-binding.md) is the
decision record; DESIGN §4.1/§4.3/§4.7 and EVENT-MODEL §4.2/§6.3 carry the semantics. **No new
event kind (the durable set stays 22), no new table, no wire/API change, no core change** — the
durable footprint is two additive `exec_worker` keys on events that already existed.

- **Config / catalog (kian-123)** — `endpoints[]` gains an optional `description` (fail-closed:
  non-empty, ≤200 chars, no control characters — it is prompt-prefix content); duplicate ids still
  refuse; `superagent.harness.dsh` × non-empty `endpoints:` now refuses boot (the dsh harness never
  mounts the kian `exec`, so a binding there would be audit fiction). The worker catalog (ids +
  descriptions, config order) feeds the `spawn_worker` schema enum and the Router prompt block.
  Zero endpoints = feature off, byte-identical everywhere (wrapper, `thread/started`, schema,
  prompt, routing).
- **Binding bytes (kian-123)** — the delegation wrapper and both sides' `thread/started` gain the
  additive top-level key `exec_worker: <endpoints[].id>` (`worker` keeps meaning the worker
  *session* uuid); readers classify absent=Unbound / non-empty string=Bound / anything else
  (incl. empty, wrong type, disagreeing copies)=Malformed → honest refusal, never a guess.
  `SpawnRequest` carries the choice through one struct; the Router side emits `thread/started`
  **before** admitting the task row (lineage-before-admit), bounding the crash window: a
  never-started reservation is released after `queue_timeout_ms` with an audit line.
- **Router surface (kian-123)** — `spawn_worker` gains the `exec_worker` argument: required with
  ≥2 workers (refusal lists the catalog), omittable with exactly 1 (bound to that worker and
  recorded — never silently unbound), refused with 0. The spawn result echoes the choice. The
  prompt gains the catalog block (ids + descriptions + the choice rule) appended only when the
  catalog is non-empty, so zero-endpoint renders are byte-identical.
- **1:1 exclusivity (kian-123)** — a worker has at most one live bound worker session. The check
  is a pure stream-derived function (`bound_workers` + status-aware `thread_liveness` +
  `busy_owner`: `thread/completed{completed}`/`session/ended{completed,cancelled}` release,
  `{failed,interrupted}` hold; NeverStarted held only inside the `queue_timeout_ms` window; read
  errors fail closed) called **before any side effect** through the bundle-owned read-only
  `WorkerStreamsPort` (implemented in `kian-cli`; the `RecallPort` precedent — the iron rule
  holds, no store symbol in `kian-superagent`). A busy refusal names the owning worker session
  uuid and thread id and leaves zero inbox rows / threads / `thread/started` / provider calls.
- **Per-worker routing (kian-123 + B1 fix)** — assembly prebuilds one worker side per endpoint
  (its own `exec` `RemoteExecTool`, its own async `start`/`stop` pair bound to that endpoint —
  the async row already carries `endpoint_id`); `SuperAgentDriver` selects the side per drive
  from the session's own stream binding: `Bound` → that worker; unconfigured bound id → a
  **refusing** `exec` side (honest tool error, no queue row, no re-route, no local run);
  `Malformed` → refusing side; legacy `Unbound` → the first **configured** endpoint (config
  order, not map-key order — review B1 fixed with an L1 regression test and an L2 leg using
  non-alphabetical ids) with exactly one per-session audit line. No resident routing state;
  a `kian serve` restart re-derives the binding from the stream.
- **Reviewer-found defect fixed** — the legacy-unbound route used `BTreeMap` key order instead of
  config order; `worker_order` is now carried through `with_worker_sides`, so an unbound session
  resolves to the first configured endpoint (a non-alphabetical configuration is covered).
- **Deferred with the round** — live rebind of a session to a different worker id (ADR-020
  §Deferred: durable intent + queue fencing + seq-range projection, owner gate), deployment
  tooling.
- **Closeout (kian-127)** — SPEC §5 worker-binding section, README feature bullet + BYOM
  advanced-usage example, this entry.

## 2026-11-08 — agent-memory P3b-1: index-accelerated memory reads (`scan_kind`)

- **Scope** — the memory P3b-1 plan: replace the memory paths' full-stream read with an
  index-accelerated, kind-filtered read of `memory/note` events. **No new event kind (the
  durable set stays 22), no new view/snapshot/table — one adjacent migration that adds a SQL
  index only — no wire or behavior change.** The stream stays the sole truth; every switched
  read path is byte-identical. Persisted representations, dreaming cadence, the agent-side
  memory tool and governed cross-scope reads stay designed (P3b remainder + ADR-019 track —
  moved, not dropped).
- **Store (kian-116)** — adjacent migration v6 on both chains adds `idx_events_session_kind_seq`
  over `events (session, kind, seq)`; `EventStore::scan_kind(session, kind)` returns the
  session's events of one kind in seq order (`WHERE session = ? AND kind = ? ORDER BY seq`),
  served by the index (EXPLAIN QUERY PLAN: index-named plan, no `SCAN events` table scan). A
  pre-v6 store reads correctly and byte-identically via the planner fallback, and a read-only
  open never migrates. Tests (new only): `scan_kind_parity_with_scan_then_filter_on_a_mixed_stream`,
  `scan_kind_works_on_a_read_only_store` (kian-store), and the env-gated pg twin
  `pg_scan_kind_parity_index_and_read_only_pool`.
- **CLI reads (kian-117)** — every memory read path in `kian-cli` goes through
  `scan_kind(MemoryNote)`: the shared `memory_notes` helper (list/search/map/verify),
  `run_memory_show`, and the creation-time injection's source-session read. Test (new only):
  `injection_payload_byte_identical_on_a_mixed_source_stream` — the admitted `{content, memory}`
  payload is byte-identical to the pre-change full-rebuild read (including the `source_seq`
  pin) on a source stream interleaving notes with non-note events, and the injection read
  leaves the source stream untouched.
- **Failure path (kian-120 M1, accepted)** — the switched memory reads surface `StoreError` text
  directly instead of the old `InboxError::Store` `store error: …` prefix; every successful-output
  path is byte-identical (old-vs-new binary comparison).
- **Closeout (kian-119, this entry)** — MEMORY-MODEL §6's cost note and §7.1's cost bound now
  record **O(memory/note events)** via the index-accelerated kind scan (a recorded bound, not
  a perf claim); READ-MODEL §6 notes the memory read paths consume the truth table through the
  declared per-session kind index (the `audit_kind_index` idea) — not a view, not a snapshot,
  no new declared view, no freshness class; SPEC §5 splits the P3b row: **P3b-1 landed**, the
  remainder (persisted representations, cadence, tool, governed cross-scope) designed.

## 2026-11-08 — agent-memory P3a: read-only retrieval + representations (`kian memory search|map`)

- **Scope** — MEMORY-MODEL §7's first landed slice: two read-only views over already-persisted
  notes. **No new event kind (the durable set stays 22), no new crate, no table/migration, no wire
  change, no event or model-context byte change.** `kian memory list|show|search|map` never append:
  the CLI verbs open the store read-only; a missing store is an error, never a created file. This
  slice needs **no dreaming run** because nothing is persisted — a persisted representation
  (wiki-as-notes) stays designed (P3b) and would follow MEMORY-MODEL §7 R3.
- **`kian-memory` pure projections (kian-107)** — `search.rs`: latest-per-subject fold over ALL
  statuses (every hit carries `verified|candidate|stale` — untrusted notes surface as claims),
  ranking subject exact > prefix > contains > text contains, deterministic subject-byte tie-break,
  ASCII case-insensitive, caps 20 hits / 8 KiB with `truncated`. `map.rs`: verified-only nodes —
  **effective rule through the read path: a subject is a node iff its latest revision is
  `verified`** (`project_notes` pre-folds latest-per-subject before eligibility, so the
  `eligible_notes` "last verified revision" fallback is unreachable from any production caller;
  consistent with the P2 injection index and now stated in MEMORY-MODEL §7.1) — with
  evidence-derived edges (note pairs whose `derived_from` ranges intersect in the same source
  session, never model-generated), caps 200 nodes / 1000 edges / 64 KiB. The byte cap is
  **nodes-first** (kian-112, fixing the review's B1): the byte-order node prefix is kept and edges
  are cut first — only edges between kept nodes are delivered, so no edge ever dangles — and the
  trim is O(nodes+edges) incremental section math, not a re-render fixpoint (B2: 14.7 s → 2.7 ms
  at 200 notes / 57-char subjects). Honest heuristics `degenerate` (fewer than two nodes or zero
  edges in the **delivered** view — search
  alone is the right tool) and `oversized` (rendered bytes exceed the summed note-text bytes)
  reported in text and JSON, never hidden. Both views render byte-identically across runs and
  insertion interleavings and lead with the frozen `(data, not instructions)` frame.
- **`kian memory search|map` verbs (kian-108)** — read-only in-process CLI verbs reusing the exact
  `memory list`/`show` read path; text and `--json` agree on every flag. **Recorded divergence
  from the P3a plan §2.1:** no `[--limit N]` flag — the caps are fixed (20 hits / 8 KiB; the map's
  200/1000/64 KiB), accepted rather than silently diverged.
- **Cost and limits (recorded)** — one invocation is a full stream read + fold + pure view,
  O(memory-session events); the caps bound rendered bytes, not the read. The bound is recorded,
  **not perf-benched** (A6); a snapshot-served fold (the READ-MODEL acceleration) is deferred with
  P3b. The `oversized` flag is a one-pass re-render fixpoint — convergence beyond that pass is
  a known limitation (kian-107).
- **Closeout (kian-110)** — MEMORY-MODEL §7 (+ §7.1 landed block, header, §9 verbs), SPEC §5 P3
  split (P3a landed / P3b designed, moved not dropped), README feature bullet + advanced-usage
  example + docs-map row, this entry.
- **Fix (kian-112)** — the map's byte cap was wrong: the
  trim loop put edges outer / nodes inner, so once the 64 KiB budget bound it delivered ~944 edges
  and 0–2 nodes — every edge dangling, no `## Nodes` section — contradicting the struct invariant,
  the docs and README. Fixed by restructuring the trim nodes-first (largest fitting byte-order
  node prefix, then only edges between kept nodes while each next edge line fits — closed by
  construction) and replacing the O(nodes×edges) re-render fixpoint with O(nodes+edges) incremental
  section lengths. MEMORY-MODEL §7.1, README (feature wording + crate-map line) reconciled in the same
  change.

## Round-10 WeChat L3 live-run fixes (2026-11-08)

The first real phone/QR run of the WeChat iLink channel surfaced five defects that the mocked
suites could not; all are fixed.

- **Official iLink client identity is required** (`kian-superagent`, `wechat/protocol.rs`) —
  `iLink-App-Id: bot`, `iLink-App-ClientVersion: 0x0002040A` and `base_info.channel_version:
  2.4.10`. The kian-branded values made WeChat's ClawBot login page fail with `Network error`
  before the scan reached the server; the QR scan then confirmed and wrote the 0600 credential.
- **Sender provenance reaches the model** (`wechat.rs`) — the event `raw` now carries
  `from_user_id`; the Router prompt derives the reply target from `raw.from_user_id` instead of
  guessing (it previously used the message id).
- **Target spellings tolerated** (`wechat.rs`) — the model may pass `target` as a JSON object or
  as a JSON-encoded string; both are parsed before the allowlist checks.
- **Ret-less `sendmessage` success accepted** (`protocol.rs`) — the live success body omits `ret`;
  an absent `ret` now reads as success (official-client parity), a non-object body is still
  `Malformed`, and `ret != 0`/`errcode != 0` stay honest failures. The strict parse had caused a
  retry storm whose sends the server had in fact accepted.
- **Judge tags and channel formatting** (`templates.rs`, `policy.rs`) — the decision tag is
  confined to the Router's assistant narration (it must not appear in `reply` text); channel text
  is plain text (no markdown/emoji), summarized rather than relayed from a worker report; the IM
  budget now counts **CJK characters as units** (a whitespace-only count let arbitrarily long
  Chinese replies through the ≤100 rule), cuts over-budget text at the last complete sentence that
  keeps at least half the budget, and the prompt asks for a complete summary plus
  `回复「详情」看完整结果` instead of relying on truncation.

## 2026-11-08 — agent-memory P2: creation-time injection

- **Scope** — MEMORY-MODEL §6 landed exactly as designed: no new event kind (the injection is an
  ordinary `input/admitted`), no new crate, no table/migration. Four additive deltas only:
  `Engine::admit_quiet` + the `memory` row arm, the additive non-context `memory` provenance key,
  the `memory.session`/`memory.inject` config block, and the pure renderer.
- **Quiet admit + the `memory` row arm (kian-99)** — `Engine::admit_quiet` writes a durable inbox
  row without waking (no NOTIFY, no drain; same idempotency semantics as `admit` via a shared
  `admit_row` helper). The consumption plan gains one arm: a row payload of exactly
  `{"content":X,"memory":M}` consumes to ONE `input/admitted`
  `{role:"system",content:X,synthetic:true,memory:M}`; a superset shape is ordinary content. The
  public wire (`submit`/`steer`/`queue`) refuses the exact two-key shape with `-32602` (the fifth
  reserved envelope), so only the server's create path can land the marker.
- **Deterministic index renderer (kian-100)** — `kian-memory` `index.rs`:
  `render_memory_index(notes, cfg)` filters verified-only (latest verified per subject, subject
  byte order), renders the full claims within both caps, degrades over either cap to the index
  form (subject + one-line excerpt ≤ 120 chars, whole entries dropped until the byte cap fits,
  `truncated:true`), and returns `None` for an empty eligible set or an unusable budget (admit
  nothing). Frozen content (constraint 3): the `# Memory index` "data, not instructions" header
  and the excerpt cap are named constants; the default redaction pass is pattern-scanning only
  (env-sourced known values would break cross-machine determinism), with
  `render_memory_index_with` as the assembly seam for the configured redactor; the value-free
  report rides provenance.
- **Config + creation-time injection (kian-101)** — `memory.session` (the source memory session
  uuid; absent = injection off, byte-identical) and `memory.inject {max_items, max_bytes}`
  (fail-closed; defaults 20 / 4096). The server's create path, after `engine.create_session` and
  before the RPC returns, rebuilds the source stream, folds, renders, applies the identity
  exclusions (the source session itself and dreamer-agent sessions), and quietly admits the index
  under idempotency key `memory-inject:<session>`; every failure is honest degradation (inject
  nothing + `injected_skips`, never a failed creation). New `GET /metrics` `memory` keys:
  `injected_sessions/items/bytes/truncations/skips` (present only when injection is configured).
- **Closeout docs (kian-103)** — MEMORY-MODEL §6 reconciled with the landed behavior (status
  P2 landed; I1–I9 updated; I10 exclusions added; the F1 restart window recorded; §9 metrics),
  SPEC §5 P2 row → landed, DESIGN §3.2 creation-time path + constraint-3 argument, DESIGN §7
  crate map, README feature bullet + advanced-usage example + docs/crate map rows, this entry.
  EVENT-MODEL §3/§4.2/§4.5 were reconciled with the payload changes in the kian-99/101 changes.

## 2026-11-08 — round-10 WeChat iLink channel: adapter, config/CLI, login

- **`design/CHANNEL-WECHAT.md` (normative)** — the `type: wechat` channel contract: the frozen
  iLink protocol mapping (endpoints, headers, `WeixinMessage`, the `sendmessage` payload), the
  `ChannelEvent`/`ChannelDelivery` mapping, the `context_token` rule, the `0600` credential-file
  schema and lifecycle, the config/CLI contract (`kian wechat login`), the bidirectional receive
  allowlist (§3 drop + §9 send gate), dedupe (`message_id` ring + `(session, event_id)`
  idempotency), the empty-cursor bound (no position table), the failure/degradation table,
  phase-2 non-goals, L1/L2/L3 acceptance and the full UNVERIFIED list.
- **`design/decisions/ADR-018-wechat-ilink-channel.md`** — a native adapter in the bundle rather
  than the OpenClaw bridge, 公众号, 微信客服 or personal-account hooks; the QR-issued token as a
  deliberate deviation from the env-only static-secret rule; best-effort consequences (operator
  re-login on `-14`, reply-only so no proactive push, phase-2 media).
- **`kian-superagent` adapter (commit `c066e7d`)** — `WechatChannel` over the `ChannelAdapter`
  seam: one long-poll loop per channel, `message_id` → `event_id` (lossless decimal string),
  ring + idempotency dedupe, topics allowlist, `context_token` send cache populated only for
  admitted senders and a send gate refusing non-allowlisted targets before any wire traffic,
  ordered >2000-char split, `-14` → 1 h pause + credential-mtime resume; `wechat/login.rs` QR
  flow (a status long-poll timeout is `wait`, never fatal); the judge prompt gains the
  channel-target sentence (new-session `policy_version` change only — old sessions replay
  byte-identically). 22 adapter tests against a scripted axum mock iLink server.
- **`kian-cli` config/assembly/login (commit `8153938`)** — `WechatChannelSection` fail-closed
  validation (credentials required, non-empty topics, single wechat channel, forced untrusted),
  `kian wechat login` (QR print, atomic `0600` write, ids never the token), assembly mounts the
  channel (registry, consumers, poll-driver order), token redaction. 9 tests including the
  real-`Server` + sqlite end-to-end: one inbound admitted exactly once across replay **and**
  process restart; channel failure leaves session state untouched.
- **R10-06a review fixes (folded into both commits)** — the four findings of the independent
  review: the send-path allowlist hole (blocker: token cache moved admit-only + the §9 send
  gate), the vacuous restart leg (baseline-before-respawn), login abort on a normal long-poll
  timeout, and missing-`ret` minting a delivered receipt; plus minors (api_base doc/code match,
  verify-code wording, dead stub removed, `0700` credential parent, `-14` observability note,
  baseurl-vs-api_base UNVERIFIED item)
- **No new surface, no wire, no event kind** — `API-CONTRACT.md` and `EVENT-MODEL.md` show zero
  diff across the round; no `kian-core`/`kian-store`/`kian-orchestrator` change; no new HTTP
  route

## 2026-11-08 — round-9 closeout: read model docs landed — documentation only

- **Scope**: documentation reconciliation for the read-model round (P0–P3 + the R9-08 fixes +
  R9-10 incremental rebuild). No code, no wire, no migration, no event kind.
- **README**: Features gains the read-model line (versioned unchanged/delta reads, resumable
  SSE, verified snapshots + tail replay, operator views, opt-in export-only archive,
  allowlist fan-in — all disposable projections; the stream stays the only truth); Docs map row
  re-worded from "proposed" to "the normative serving-form contract"; new Advanced usage section
  (unchanged `projection` poll, opt-in `retention:` config, `kian archive run|verify`).
- **SPEC §5**: "Read model and serving form" flips to **(P0–P3 landed)** with the round-9
  commit range; all five phase rows `designed` → `landed`.
- **READ-MODEL**: title `(proposed)` → `(normative)`; header rewritten to "accepted and fully
  landed" (ADR-017, owner-gated 2026-11-07) with the phase summary; R1 gains the §5
  load-time-trust-boundary pointer (re-review residual); §7 read-behavior "any read" narrowed
  to the enumerated surfaces with an honest scope note (memory surfaces answer `unknown
  session` — they do not consult the marker); the §4.2 512-frame batch now points at
  API-CONTRACT §5.4 as its normative home (F4 duplicate).
- **DESIGN §3.2**: the serving-form pointer tag "P0 landed" → "landed" (F4).
- **Residuals (owner-pending, not blocking)**: `AGENTS.md` docs-map bullet for
  `design/READ-MODEL.md` (plan §1.6) and the `21-kind` → `22-kind` catalog count (F4) were
  **not** applied — AGENTS.md is a protected file and the write required owner approval that
  did not arrive in this run. `SseFrame #[non_exhaustive]` stays deferred to the SDK freeze
  (F5, recorded with its reason in the R9-08a entry).

## 2026-11-08 — read model R9-10: incremental view rebuild

- **Problem (R9-08 F6, follow-up card t_988f5de3).** The default-on view maintenance ticker
  (`read_model.views`, enabled, `interval_ms` 60 000) did recurring O(total history) work on the
  serving runtime: every tick re-read and re-folded every session's whole stream (CHANGELOG P2a
  bench: 66.5 ms at 1 000 sessions / 5.2k events, growing linearly).
- **Remedy chosen (1 + 3 of the card's candidates; the default stays ON — an idle tick is now
  near-free, so the 60 s cadence costs ~0.01–0.1 % of one core even at 10k sessions).** The
  rebuild is **incremental**: one index-only `GROUP BY session` heads scan diffs live heads
  against the stored rows' `row_version`s; only moved/new sessions are re-read and re-folded,
  with a `yield_now()` between sessions (the folds never monopolize the runtime worker).
  Publication becomes a **delta transaction** per view (upsert changed rows, delete gone
  sessions, flip `view_state` to the merged set's digest at the pinned key) — same one-tx
  version-flip atomicity, writes proportional to the change. Soundness: streams are append-only
  under CAS guards, so equal head ⇒ identical stream ⇒ the reused row is byte-identical to a
  re-derivation. `ticket_stage` is all-or-nothing (its global folds read board-kind events
  store-wide and `pending_inbox` moves with `eligible_at` alone): a board-kind delta, a change
  to a session already on the board, a deletion, or a pending drift re-derives the whole board
  from a **kind-filtered events read** (`thread/started`, `thread/completed`, `session/ended`,
  `input/admitted`, `tool/call`, `tool/result`, `channel/delivered`) plus the worker streams —
  never from every session's full stream; otherwise only the state flips to the new key. Any
  doubt about the stored views (absent state, digest mismatch, undecodable ticket payload)
  falls back to the full rebuild (`rebuild_views_full`, kept as first-build path, self-heal and
  bench baseline) — a tampered view still heals on the next tick. Read semantics unchanged
  (R1/R8): the merged row set is byte-identical to a full rebuild's at the same pinned store
  key; the freshness gate, digest check and honest fallbacks are untouched. The snapshot
  builder's 30 s candidate scan stays an index-only `GROUP BY` bounded by its `LIMIT`
  (documented in READ-MODEL §9).
- **Store surface (both backends, dialect SQL in `kian-store` `sqlite.rs`/`postgres.rs`):**
  `session_heads`, `pending_input_counts`, `events_where_kinds`, `view_publish_delta`. The board
  fold core moved from `ticket_rows` to `ticket_rows_from(all_events, worker_streams)` with the
  kind-filtered input equivalent by construction (`board_kinds_stay_in_sync` pins the seven
  kinds to the enum). `RebuildOutcome` gains `derived` and `board_refreshed` counters.
- **Equivalence executed** (`views_e2e`): `incremental_rebuild_equals_full_after_partial_changes`
  (2 grown + 1 new + 1 deleted session ⇒ `derived == 3`, all four views equal the honest full
  derivations, deleted rows gone), `incremental_tick_after_no_changes_is_a_noop` (`derived == 0`,
  `views == 0`, views stay fresh), `plain_append_does_not_refresh_the_board` (delta-scoped board
  trigger; ticket view exact via the key flip), `tampered_view_self_heals_on_the_next_incremental_tick`,
  `pending_inbox_drift_without_appends_refreshes_tickets` (the §6 declared bound holds without
  appends), plus the pre-existing P2a suite (equivalence, stale/corrupt fallback, lease,
  pre-v4 reader) unchanged and green. Config unchanged (`read_model.views` default
  enabled / 60 000 ms; no migration, no wire change, no new event kind).
- **Benchmark** (`cargo bench -p kian-cli --bench read_path -- views`, sqlite, criterion 0.5,
  M-series laptop; 1k = 800 plain + 200 workers ≈ 5.2k events, 10k = 8 000 + 2 000 ≈ 52k
  events): full rebuild **64.8 ms @1k / 696.8 ms @10k**; incremental idle tick
  **6.0 ms / 60.1 ms** (heads + pending scans + four digest verifications); incremental tick
  after 10 plain sessions grew **26.8 ms / 78.7 ms** — marginal cost per changed session
  **≈2.1 ms @1k and ≈1.9 ms @10k** (flat across a 10× store: proportional to the change, the
  ~20 ms constant is the four publication commits); board-refreshing tick (one worker grew)
  **24.2 ms / 226.2 ms** vs the honest full board derivation 30.1 ms / 340.9 ms. READ-MODEL §6 (incremental rebuild bullet) and §9 (per-tick
  cost rule) updated in the same change.

## 2026-11-07 — event-model enhancements borrowed from CMA

Event-model ideas from Anthropic's Claude Managed Agents catalog were borrowed (usage facts, a persisted interrupt intent, derived event ids,
redaction/permission provenance) as design inspiration only — no wire-protocol dependency.
**Exactly one new event kind (`control/interrupt`): the durable set grows 20 → 21.** Everything
else is an additive payload key on a frozen wrapper, a pure projection, or a config-first
capability; request bytes, tool schemas, `FROZEN_CHAT_BODY_SHA256` and `derive_model_context`
bytes are untouched (each test asserts this — constraint 3).

- **Provider usage + structured recovery on `turn/end`**: the openai-compat
  provider parses SSE `usage` objects verbatim (last frame wins; a usage-only frame never flips
  the empty-response liveness flag); `turn/end` carries `"usage"` verbatim plus
  `"error": {"class", "action"}` on soft-failure/error outcomes (the pure classifier +
  declarative policy, `"none"` when no rule); legacy payloads are byte-identical when unobserved.
  New pure projection `kian-cli::usage_metrics` powers `kian usage-stats`, the `GET /metrics`
  `"usage"` aggregate and `session inspect` usage lines — no pricing, no normalization in core.
 Stale rows (target episode already settled) append `"stale": true` and end nothing;
  idle/terminal sessions keep `not_draining`/`unknown_session` and admit nothing. `session
  inspect` gains the `pending control` line. Absent table = `tool/call` payload byte-identical to legacy.

Deferred: the `ask`/confirmation permission plane and token pricing (out-of-scope records in SPEC §5).

## 2026-11-04 — bundle-domain policy: ticket semantics, mandate, attention economics

The dsh-superagent-prototype's domain-policy layer was adopted onto the bundle. **Zero new event kinds — the durable set stays 20**; every addition is an additive
payload key on a frozen wrapper, a disposable projection, or a config/template artifact.

- **Ticket stage projection + `kian tickets`**: "what is it waiting for" as a pure
  first-match derived projection over the worker stream + inbox + outbox view
  (`delivery-retry / awaiting-answer / awaiting-review / blocked-failed / queued / idle /
  working`), with the additive `state` key on completion-report content; the read-only CLI
  stage board. Honest v1 shape: a Ticket = one worker session; ended tickets resume via fresh
  Eval; the panel is the CLI.
- **Closed decision-reason vocabularies**: `reason=<code>` on every drop/admit audit
  line, per-code metrics counters, and the judge-side `decision:<accept|stop|ignore|unavailable>`
  tag convention in the default template.
- **Output-side speak-rules**: the whether-to-send gate before output shaping
  (`superagent.speak_rules`; suppression = honest tool error + audit + counter, no send);
  `ack-first-only` confirm-once memory rebuilt from the stream.
- **Thread/Correlation intake projection** ([ADR-013](design/decisions/ADR-013-thread-intake-projection.md),): config-owned canonical thread keys used as routing topics; stream-derived read
  positions (lark/GitHub cursors) or honest idempotent re-poll — no position table; correlation
  adds context, never routes; fan-out grants no authority.
- **Follow-up ownership routing**: registry liveness projection; fan-out admits to
  live subscribers only; an all-ended topic releases to the Router (`released_to_eval` fresh
  Eval); `spawn_worker` refuses a live worker's topic naming the owner.
- **Policy/prompt versioning + drift marking**: `[policy <label>]` render header
  (persisted prompts replay byte-identically), boot-time drift marking that never rewrites live
  sessions, config-gated one-shot policy-changed marker input.
- **Delegation Mandate** ([ADR-012](design/decisions/ADR-012-delegation-mandate.md)): the immutable authority object `{strength, scope, granted_by, source,
  policy_version}` additive on the frozen delegation wrapper — verbatim in `thread/started`,
  model-visible as the worker's first message; issued only at `spawn_worker`; revalidated
  before follow-up routing (stale `inferred` releases to fresh Eval).
- **Outbox bounded retry**: a disposable projection over `tool/call`
  intent + `channel/delivered` receipt plus a bounded driver (2 s doubling to 60 s, 6 attempts,
  per-(ticket, conversation) ordering); park is honest — `delivery-retry` stage, named on
  `kian tickets`, survives reboot; additive `attempt`/`first_attempt_ms` on new receipts.
- **issue-watch v2 multi-Ticket dogfood + github poll-receive mode**: `since`-cursored
  REST polls beside the untouched webhook surface; one Ticket per live issue on the real repo,
  composing the whole bundle stack; v1 kept as `config-timer.yaml`.
- **Gap fixes** (from the RUNLOG §7.2 gap report): `User-Agent` on every
  github/lark HTTP client + `flaky-proxy.py` delivery-leg fault mode; the lenient
  live spawn spellings — live topic binding, mandate strength and outbox ticket aliasing
outbox park rebuilt at boot and surfaced on the stage board; exec
  secret hygiene — shared core `SecretRedactor` + `ExecEnvPolicy`, secrets redacted from
  `tool/result` before persist (HIGH; the one cross-bundle-fence commit, filed separately off the live gap report).

## 2026-11-02 — live positives, dogfood, and field fixes

- **live positive landed** (`contrib/dsh-v0-peer/dsh-v0-peer.mjs`): a zero-dependency Node
  peer adapting stock `dsh --profile sdk` (JSON-RPC 2.0) onto the frozen ADR-010 contract v0 — `ready`
  emitted immediately, `session.event` notifications forwarded as v0 session-event frames, `input`
  frames translated to `session/prompt`. Driving one real input through peer → dsh → real
  `deepseek-chat` appended the full mapped chain (turn/start, input/admitted, step/request,
  assistant/message, turn/end) with an honest `Settled` outcome — see `docs/runbook-dsh.md` for the run.
- **Dogfood example** (`examples/issue-watch/`): a real long-running workload on
  `flyer103/kian` — TimerChannel ticks → `gh issue list` → Router delegates triage → worker drafts a
  reply (dry-run by default) → everything in the durable audit stream. `demo-restart.sh` resumes the
  same session from the event
  stream after `kill -9` + reboot; `RUNLOG.md` records the run.
- **Wire dialect v2** (breaking): strict OpenAI-compatible serialization — built-in tools
  renamed to dotless spellings (`fs_list`/`fs_read`/`fs_write`; dotted forms still invocable for
  replay), wire-level `tool_calls`/`tool_call_id` emitted on tool round trips, `AgentDef.tools`
  enforced fail-closed. Found by the dogfood run replaying persisted `step/request` bodies against the
  real DeepSeek API (HTTP 400/422); constraint-3 event recorded in DESIGN (golden hash updated in the
  same commit)
- **Router re-arm**: a resident Router whose turn soft-failed no longer stayed terminally
  frozen across reboots. Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident
  with an observable `[re-arm]` cause input on the successor stream; soft-failed sessions re-open the
  same pinned session. Zero new event kinds; DESIGN §4.6 records the semantics.

## 2026-11-01 — hosted foundation: postgres, multi-replica, SDK freeze, ecosystem surfaces

- **`kian-sdk`** (twelfth crate): the wire-only embedded client extracted from `kian-cli` — typed
  methods, the SSE helper, matchable error codes, golden wire fixtures, and **no dependency on any
  kian crate**. Frozen additive-only; a breaking change requires a new design decision.
- **Postgres dual backend** in `kian-store` and `kian-exec`: an enum-dispatch `EventStore` with
  identical semantics (single-statement end write, batch CTE append, PK-race classification), one
  server / one database / one pool. URL is env-only (`KIAN_PG_URL`) and postgres mode requires an
  explicit unique `drain.holder` — both fail closed. Export parity (byte-identical JSONL) is the
  compat golden.
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
- **`kian-dsh`** (thirteenth crate, [ADR-010](design/decisions/ADR-010-dsh-session-mapping.md)):
  a `LoopDriver` that drives a `dsh --profile sdk` child over stdio JSON-lines and maps its session
  events onto existing kian kinds. Config-gated and default off (`superagent.harness.dsh`); depends on
  `kian-core` only (narrow waist).
- **Zero new event kinds**; the durable set stays 20.

## 2026-11-01 — article-sync: recall, admission economics, channels

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
- **Async `start`/`stop` tools** ([ADR-008](design/decisions/ADR-008-async-tool-completion-admitted-input.md)):
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

## 2026-11-01 — super-agent bundle + BYOM endpoints

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

## 2026-11-01 — MVP: durable sessions

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

## 2026-10-31 — design baseline (documentation only)

- `docs/SPEC.md` (WHAT), `design/DESIGN.md` (HOW), and the first six ADRs (Rust foundation,
  event-sourced core, super-agent as a profile, Cordis-inspired plugins, fencing via `expectedSeq`,
  model slots).
- `AGENTS.md` with the hard constraints that bind every later change.
