# Harness bridges — the external-harness integration contract

- **Status**: phase 1, draft for the confirmation gate (2026-11-19). Normative for every
  session whose loop is owned by an external harness. The dsh wire contract stays
  [ADR-010](decisions/ADR-010-dsh-session-mapping.md); this document generalizes the
  *kian-side* contract and adds Claude Code and Codex bridges
  ([ADR-043](decisions/ADR-043-multi-harness-bridge.md)).
- **Owners**: `kian-harness` (shared skeleton), `kian-claude` (Claude Code adapter),
  `kian-codex` (Codex adapter). `kian-dsh` keeps its v0 contract; convergence is phase 3.
- **Read this with**: [SPEC](../docs/SPEC.md) §2.2 C, [DESIGN](DESIGN.md) §6.2/§7/§16,
  [EVENT-MODEL](EVENT-MODEL.md) §3, [PLUGIN-SPEC](PLUGIN-SPEC.md) §3.4.1/§3.5,
  [CONFIRMATION-MODEL](CONFIRMATION-MODEL.md) (phase 2).

## 0. Scope and non-goals

A **bridge** runs an external harness as a kian `LoopDriver`: the harness keeps its prompts,
tools, sandbox, model choice and interaction model; kian keeps the durable session, the event
stream, control (steer/interrupt), recovery, audit and the channel/thread plumbing. The
substrate never fights the harness over the interaction layer ([SPEC](../docs/SPEC.md) §1).

In scope (phase 1): the shared bridge skeleton, the Claude Code and Codex adapters, a
declarative approval policy, the durable bridge marker, capability matrix, config grammar,
fixtures and conformance tests, and the live-smoke gates.

Not in scope (phase 1), each with a named phase in §10: byte-level request capture
(`step/request` for harness-owned calls), durable interactive `ask` approvals,
harness-hook → kian-plugin bridging, cron per-schedule harness selection, ACP, and moving
`kian-dsh` onto the shared skeleton.

## 1. Terms and the split of truth

| Fact | Truth owner | Where it lives |
|---|---|---|
| Prompt assembly, tool choice, sandbox, model call | **harness** | inside the harness process |
| User-visible inputs/outputs, tool calls/results, turn boundaries | harness, mirrored by kian | the kian event stream (existing kinds) |
| Session lifecycle, drain, steady state, recovery budget, end write | **kian** | `session/*`, `drain/*`, `turn/*`, `session/ended` |
| Control (admit, steer, interrupt, confirm) | **kian**, delivered to the harness | inbox rows → bridge control frames |
| Tool approval decision | **kian policy** answers a harness-initiated request | `tool/call.permission` + the harness's own log |

Hard rules carried over unchanged: the event stream is the sole truth
([AGENTS](../AGENTS.md) constraint 1); the drain is the only writer (constraint 9); the end
write is the engine's alone (constraint 4); a crash is never `completed` (constraint 7);
payloads ride the frozen shapes the built-in loop emits, and **no new event kinds** are added.

## 2. Bridge classes (the honesty axis)

A bridge cannot claim what its harness does not expose. Two classes exist, and the class is a
durable fact of the session:

| Class | Meaning | `request_bytes` | Sessions today |
|---|---|---|---|
| **exact** | the assembled model request is observable before the call settles; kian persists it as `step/request` and byte-replays it | `"exact"` | dsh via contract v0 (ADR-010) |
| **projection-only** | the harness assembles and sends the request itself; kian observes inputs/outputs but **not** the assembled bytes | `"unavailable"` | Claude Code, Codex |

**Why the second class exists.** The A2 rule (persist-what-you-send) is kian's property over
*its own* provider calls. In a projection-only bridge the provider call is the harness's: no
public surface of Claude Code or Codex exposes the assembled request, so kian cannot persist
it, and fabricating one from the response would be a post-hoc logger —
[ADR-010](decisions/ADR-010-dsh-session-mapping.md) already rejected exactly that. The honest
answer is a distinct class with an explicit guarantee set, not a silent weakening of A2:

- a projection-only session appends **no `step/request`** and claims **no** cache accounting,
  prefix-stability or byte-identical request replay (and no `slot/binding`: kian never binds a
  model — the harness's own model config is the audit fact, and dashboards must keep these
  sessions out of cache/cost aggregation);
- it does claim event-level durability, replay of the projection, steer/interrupt,
  session recovery, tool-call audit with approval provenance, and the honest end reason;
- the class is a **durable event fact**: a spawned worker session is created eagerly and
  `session/created` carries the additive key
  `"bridge":{"name":<str>,"request_bytes":"exact"|"unavailable"}` (§4.7). An auditor reads the
  class from the export alone; sessions created without the seam (the API's plain
  `create_session`) keep the historical `{}` bytes.

Phase 3 designs the request-capture proxy (point the harness's `base_url` at a local recording
proxy) as the upgrade path that turns projection-only sessions into exact ones. Until then the
class is the contract.

## 3. Capability matrix (normative)

The matrix is the bridge's public contract: a capability the harness does not have is never
configured as required, and the code (`kian-harness::Capabilities`) is drift-tested against
this table (acceptance A8). `✔` phase 1, `◐` documented degradation, `—` absent, `P2/P3` later
phase.

| Capability | dsh (v0) | Claude Code | Codex |
|---|---|---|---|
| model request bytes (`step/request`) | ✔ (peer contract) | — (P3 proxy) | — (P3 proxy) |
| cache accounting / prefix audit | ✔ | — | — |
| steer (strippable, injected at the next safe boundary) | ✔ | ◐ queued as the next user message | ◐ `turn/steer` when the pinned protocol supports it |
| interrupt (stop the in-flight turn) | ◐ frame exists; mounted driver does not forward mid-episode | ✔ `control_request{interrupt}` | ✔ `turn/interrupt` |
| pre-execution tool gate answered by kian | — | ✔ `can_use_tool` control request | ✔ server-initiated approval request |
| approval provenance on `tool/call` | — | ✔ (`policy:"configured"`) | ✔ |
| `tool/call` appended before execution | ✔ (log-before-execute contract) | ◐ requires the pre-execution `tool_use` block (verified per pin, §11) | ◐ same question for `item/started` |
| `tool/post` observation (hook port) | ✔ (port; test-mounted) | ◐ port exists, bus wiring P2 | ◐ same |
| kian `tool/pre` veto (plugin bus) | — | P2 (PreToolUse → bus) | — (approvals only) |
| harness hooks bridged to the kian bus | — | P2 | — |
| session resume after process death | ✔ child log replay + dedup | — (P2: takeover ends honestly interrupted) | — (P2: same) |
| model selection | harness-owned (labels only) | kian passes `--model` | kian passes the model field |
| multi-turn within one drain | ✔ | ✔ stream-json input | ✔ app-server |
| tool execution sandbox | harness | harness | harness |
| subagents / background jobs | harness | harness | harness |

Two consequences worth naming: (1) for Claude Code and Codex the **tool gate lives in the
harness's own permission system** — kian answers it (allow/deny from config), so the decision is
recorded and enforceable, but it is not kian's `tool/pre` hook; (2) subagents and background
jobs a harness spawns are opaque to kian — they are the harness's effects, outside this
contract (same exclusion as [PLUGIN-SPEC](PLUGIN-SPEC.md) §3.5 for dsh-child tools).

## 4. The shared skeleton (`kian-harness`)

Narrow waist: the crate depends on `kian-core` only (seam types + the existing event
vocabulary), exactly like [`kian-dsh`](../crates/kian-dsh/src/lib.rs) (enforced by a
`narrow_waist` tree test). Assembly implements the ctx port over the real `DrainCtx`, as it
already does for dsh ([`ctx.rs`](../crates/kian-dsh/src/ctx.rs) lines 36–68, the port shape to
mirror).

### 4.1 Session trait

Per-harness code is a protocol adapter plus a spawn recipe; the state machine is shared.

```
trait HarnessSession: Send {
    fn capabilities(&self) -> Capabilities;
    fn next(&mut self) -> BoxFuture<'_, Result<SessionEvent, ContractError>>;
    fn send(&mut self, control: Control) -> BoxFuture<'_, Result<(), ContractError>>;
    fn answer(&mut self, approval: ApprovalAnswer) -> BoxFuture<'_, Result<(), ContractError>>;
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

enum SessionEvent {
    Mapped(Mapped),                 // one kian event, or an explicit Absorbed marker
    Approval(ApprovalRequest),      // the harness asks; the driver answers
    Exit { code: Option<i32> },
}
```

A `SessionFactory` spawns the child with the resolved argv/env, performs the protocol handshake
(`ready` / `initialize`) inside the startup deadline, and returns the session with its declared
capabilities; a handshake failure is a `SoftFailed` naming the deadline or the refusal (the dsh
shape: [`driver.rs`](../crates/kian-dsh/src/driver.rs) lines 523–553). The driver owns the
process through a pump task and kills it when the episode ends (the dsh shape:
[`driver.rs`](../crates/kian-dsh/src/driver.rs) lines 138–211, 427–808).

### 4.2 Event vocabulary

Adapters map **native events onto existing kian kinds only**; `Mapped::Absorbed` is reported,
never silently dropped. The driver appends through the ctx port (`append`, `begin_turn`,
`end_turn`) so the drain stays the sole writer. The vocabulary and payload shapes are exactly
[EVENT-MODEL](EVENT-MODEL.md) §3: `input/admitted`, `assistant/message`, `tool/call`,
`tool/result`, `turn/start`, `turn/end`.

### 4.3 Control frames

`Control::Input{content}` (an admitted row), `Control::Steer{content}` (a synthetic row; the
mapped echo carries `synthetic:true`), `Control::Interrupt`. Delivery follows the dsh
conventions: promoted rows at safe boundaries, synthetic rows as steers, and no input while a
harness turn is open unless the capability matrix says the harness queues it.

The engine's consumption write is the durable record of every admitted input; the bridge never
depends on the harness echoing it. Echoes are deduplicated when present, and an absent echo
changes nothing — the steer marker survives on the kian side regardless.

### 4.4 Approvals (phase 1: allow/deny)

A harness-initiated approval is a kian decision with a durable record:

1. The adapter surfaces `ApprovalRequest{id, tool, input, matched_hint}`.
2. The driver evaluates the declarative policy (§6.3): `allow` | `deny`; `ask` is refused at
   boot in phase 1 (no silent downgrade, no deadlocked child).
3. The driver answers the harness (`ApprovalAnswer{id, allow}`) and, on `allow`, appends the
   pending `tool/call` with `permission:{evaluated,policy:"configured",matched}`; on `deny`, the
   harness's own error result is mapped to `tool/result{is_error:true}` and the call record says
   `deny`. A request that matches no rule is **denied** (fail-closed) with `matched:"default"`.
4. If the child dies before the answer is written, the episode ends `SoftFailed`; no fabricated
   call record is appended.

No approval is ever logged with its input payload — only tool name, rule name and decision.

The policy is a pure function of config and the request, so a decision lost in a crash window is
re-derived identically on the takeover (`allow` → the tool may run again under the harness's own
idempotency; `deny` → the same denial). Phase 1 accepts that re-derivation instead of persisting
an in-flight approval: the harness owns the effect, and kian cannot resume a process. This is
recorded because it is the phase-2 `ask` problem in miniature (§10).

### 4.5 Honesty rules

- child exit mid-turn or mid-approval → `SoftFailed` (engine settles `failed`/`interrupted`,
  never `completed`);
- contract/protocol drift (an unknown frame/notification the adapter cannot classify) →
  `SoftFailed` naming the frame; unknown *kinds* inside a known envelope are absorbed with a
  counter only when they carry no model-visible content, otherwise an error;
- step budget (`max_steps`, default 32) → `Stopped`, exactly like the built-in loop's
  iteration budget;
- a harness with an explicit handshake (Codex `initialize`/`thread/start`) has a startup
  deadline → `SoftFailed` naming it; Claude Code has no ready frame — readiness is the spawn
  plus the first input, and a silent child is bounded only by the drain's interrupt/lease, like
  an in-flight provider call;
- an idle safe boundary re-checks inputs on a bounded interval (the dsh fix for folded wakes,
  [`driver.rs`](../crates/kian-dsh/src/driver.rs) lines 606–626) so a late admit cannot
  deadlock the episode.

### 4.6 Crash, takeover, resume

Each episode of a Claude Code or Codex bridge spawns a **fresh** child. Phase 1 has no
cross-process harness resume: the harness's conversation handle is not durable in kian (there is
no event to carry it, and inventing a side table would violate constraint 1), so a takeover
cannot re-enter the dead child's conversation. A takeover drain therefore does **not** re-deliver
consumed inputs (that would re-run side effects); with no fresh input it ends the session
honestly as a soft failure (`failed`, the engine's mapping for a lost turn) with a named reason —
never `completed`. A later user input starts a new drain and a new
harness child — continuation of the *session* (durable kian state, control, audit) without
continuation of the *harness conversation*.

Within one live episode the driver still de-duplicates echoes against the persisted stream
(multiset, the dsh rule: [`driver.rs`](../crates/kian-dsh/src/driver.rs) lines 213–284) and
never re-delivers an already-echoed input. dsh is unchanged in phase 1 (its own log replay +
dedup). Resume for the new bridges is phase 2 and needs a durable harness-handle record. A
dropped drain never leaks a child (the pump kills it).

### 4.7 The durable marker (landed)

A worker session is created eagerly by the spawn path, before its input is admitted: the
spawner's `SpawnSessionWriter` port (deliberately **not** `InboxWriter`, which stays exactly
admit+wake) → `Engine::create_session_with(session, payload)` writes
`session/created` with the additive
`"bridge":{"name":<str>,"request_bytes":"exact"|"unavailable"}` payload. The default
Without the port the historical lazy creation (`{}`) is unchanged, so fakes and non-super-agent
deployments are byte-identical; the production writer is seeded from the mounted harness
(`dsh` → `exact`, `claude-code`/`codex` → `unavailable`). Creation is idempotent: a replayed
spawn leaves an existing stream untouched. This is the only additive payload key the bridges
introduce (no new kinds), and it participates in no plugin resolution or model-slot semantics.

## 5. Per-harness mapping

### 5.1 Claude Code — `claude --print --input-format stream-json --output-format stream-json --verbose`

Long-lived child; one JSON object per line each way. The CLI has **no ready frame**: readiness is
the spawn plus the first input (the stream may stay silent until then), and it emits advisory
`system/hook_*` lines before `system/init`. The adapter pins the exact CLI version it was
verified against (§11).

| Claude event | kian mapping | notes |
|---|---|---|
| `system`/`init` | absorbed (counter) | records session id, model, tool list; not model-visible context |
| `assistant` message, `text` block | `assistant/message` | assembled content verbatim; no reasoning block invention |
| `assistant` message, `tool_use` block | pending `tool/call` | appended when the approval outcome is known, with `permission` provenance |
| `user` message, `tool_result` block | `tool/result` | `is_error` from the block |
| `control_request` `can_use_tool` | approval (§4.4) | kian answers `control_response` |
| `control_request` `interrupt` (ours) | — | we send it; its response is absorbed |
| `result` (`success` / `error_*` / interrupted) | `turn/end` outcome `final_answer` / `error` / `cancelled` | interrupts are `cancelled`, never `final` |
| `stream_event` (partial deltas) | absorbed (counter) | never mapped; the final message is the truth |
| unknown top-level `type` | `ContractError` → `SoftFailed` | forward-compat is a contract amendment |
| `rate_limit_event` / `system/hook_*` / other advisory system messages | absorbed (counter) | audit-only |
| `assistant` with `is_api_error_message: true` or `error` (a synthetic provider/auth failure) | absorbed (counter) | not model output; the `result`'s error turn end is the honest record |

Turn boundaries: the driver opens a `turn/start` when it delivers an input at a safe boundary
and closes it on `turn/end`; a steer queued mid-turn does **not** open a second turn. A child
that exits between `turn/start` and `turn/end` leaves the honest failure trace (open turn, no
assistant message).

### 5.2 Codex — `codex app-server` (JSON-RPC 2.0 over stdio)

Client role. Handshake: `initialize` (capabilities negotiated; a version mismatch is a
`ContractError`). Thread: `thread/start` for a new session, `thread/resume` on a continuation.
Turn: `turn/start`; new user input during a turn is `turn/steer` when supported, otherwise a
queued next turn; `turn/interrupt` for interrupt. `thread/start` pins `approvalPolicy`; app-server
**0.166.0** accepts `untrusted | on-request | granular | never` (the older `unlessTrusted` spelling
is rejected). The bridge pins `on-request`; `never` refuses boot because it removes the gate.

| Codex event | kian mapping |
|---|---|
| `turn/started` | absorbed (we opened the turn) |
| `item/started` + `item/completed`, agent message | `assistant/message` (completed only; started is audit) |
| `item/*` command execution / file change / MCP tool call / web search | `tool/call` + `tool/result` (completed carries the result) |
| server→client approval request (exec/patch/…), answered by kian | approval (§4.4) |
| `turn/completed` (status completed/interrupted/failed) | `turn/end` `final_answer` / `cancelled` / `error` |
| `turn/diff`, plan/usage notifications | absorbed (counter), audit-only |
| advisory namespaces (`mcpServer/`, `account/`, `skills/`, `app/`, `config/`, `thread/`, `turn/diff|plan/`, `item/*` deltas) | absorbed (counter), audit-only |
| unknown method/notification outside those namespaces | `ContractError` → `SoftFailed` |

Exact method and notification names, item variants and approval shapes are pinned per Codex
version in §11; an unverified name is a boot-time preflight refusal, never a runtime guess.

### 5.3 dsh — unchanged

The dsh bridge keeps contract v0 (ADR-010) in phase 1: same wire, same mapping, same fixtures.
Phase 3 converges it onto `kian-harness` without changing the wire contract; until then two
skeletons exist and that duplication is a recorded, bounded debt (§10).

## 6. Config grammar

### 6.1 Harness selection

```yaml
superagent:
  harness:
    claude:            # or codex / dsh — exactly one
      command: claude                    # else KIAN_CLAUDE_BIN, else refuse (fail-closed)
      args: ["--permission-mode", "default"]   # fixed at spawn; defaults documented
      model: claude-sonnet-4-5           # passed to the harness; audit label for the stream
      max_steps: 32
      approvals:                          # §6.3
        default: deny
        allow: ["Read", "Bash(git status*)"]
      env: ["ANTHROPIC_API_KEY"]          # names passed through; never logged
```

`harness.{dsh,claude,codex}` are mutually exclusive (one inner loop per worker template);
`command` resolves from config, then the harness env var (`KIAN_CLAUDE_BIN`, `KIAN_CODEX_BIN`,
`KIAN_DSH_BIN`), then refusal. Unknown keys refuse (`deny_unknown_fields`, the house rule).

### 6.2 Invariants at boot

1. a bridge and a worker surface (`endpoints:` or `workers:`) are mutually exclusive — the child
   owns the tools and kian never mounts `exec`, so a binding could not route anything (the
   [ADR-020](decisions/ADR-020-durable-worker-binding.md) rationale, today enforced for dsh at
   [`config.rs`](../crates/kian-cli/src/config.rs) lines 2900–2918);
2. a bridge requires the `superagent` plugin and section;
3. `approvals.default: ask` refuses boot in phase 1; a rule whose tool pattern cannot be parsed
   refuses boot;
4. `model`/argv/model-selection is fixed for the session's lifetime — no hot swap (the analogue
   of the model-slot immutability rule, AGENTS constraint 11);
5. the adapter builds argv from the structured fields and appends `args`; an `args` entry matching
   the harness's known bypass flag (`--dangerously-skip-permissions` / `bypassPermissions`, Codex
   `--dangerously-bypass-approvals-and-sandbox`) **refuses boot** — a bypassed harness never asks,
   so kian's approval policy would be a claim with no decision behind it;
6. `superagent.cron.harness` stays dsh-shaped (`--profile <name> <payload>`) in phase 1 and is not
   a bridge config; pointing it at Claude Code/Codex is a misconfiguration that fails honestly at
   the firing, and the generic argv extension is phase 2 (§10).

### 6.3 Approval policy

`default` plus an ordered `allow` list; a request matches on the harness's tool name and, where
the harness exposes arguments, a `tool(args-glob)` form. First match wins; no match → deny.
`ask` is reserved for phase 2 (§10) because a durable human answer cannot be delivered to a
harness process that died with the drain: the answer would have to be re-derived after a
re-spawn, and the harness's side effects before the ask cannot be replayed. That restart
problem is the reason phase 2 needs its own design, not a flag.

## 7. Assembly, crates and wiring

| Crate | Responsibility |
|---|---|
| `kian-harness` | shared skeleton: session trait, driver state machine, dedup, budget, approval policy, fixture script runner, capabilities |
| `kian-claude` | Claude Code adapter: spawn, stream-json codec, control requests, approval translation |
| `kian-codex` | Codex adapter: spawn, JSON-RPC client, thread/turn lifecycle, approval translation |
| `kian-cli` | config + assembly: `WorkerInnerDriver::{React,Dsh,Claude,Codex}`, ctx ports over `DrainCtx`, `tool/post` ports, the `session/created` marker |

Assembly follows the existing dsh mount ([`plugins.rs`](../crates/kian-cli/src/plugins.rs) lines
1112–1127, [`superagent.rs`](../crates/kian-cli/src/superagent.rs) lines 906–925): only the
worker template's inner loop changes; router, plain and watchdog sessions stay on ReAct.

## 8. Security boundary

- credentials are env-only (no config values, no argv, never logged or persisted);
- argv-direct spawn, no shell; a configured command is a path, not a shell string;
- child stdout is untrusted protocol input: bounded line size (8 MiB, fail-closed on
  overflow), no eval, unknown frames are errors;
- `stderr` is inherited for operator debugging but is never parsed and never persisted; a firing
  or event never carries it;
- approval inputs are never logged; only tool name, rule and decision;
- a bridge never widens kian's own tool surface: no kian `exec`, no kian tool provider is
  mounted on a bridged session.

## 9. Acceptance criteria

Observable, with the level that proves it (L1 unit/contract, L2 integration with a scripted
harness child, L3 env-gated live smoke). Every row is a test or a documented skip.

| # | State → behaviour | Level |
|---|---|---|
| A1 | config: `harness.claude` alone boots; missing command, two harnesses, `ask` approval, or `endpoints:` present → **refuses boot** with the offending key named | L1 |
| A2 | worker session with `harness.claude` against a scripted child: assistant text + tool call/result map to the existing kinds (no new kind), no `step/request`, stream replays byte-identically at export; a spawned worker's `session/created` carries the bridge marker (L1: engine + spawner-seam tests) | L1/L2 |
| A3 | child killed mid-turn → SoftFailed → session end `failed`/`interrupted`, never `completed`; the open `turn/start` has no `assistant/message` | L2 (kill) |
| A4 | `kian serve` killed -9 mid-drain → takeover ends the session honestly (`failed`, the mapping for a lost turn; never `completed`), no re-run of consumed inputs, no duplicate mapped events, exactly one end write; a new input afterwards starts a fresh harness child | L2 |
| A5 | steer delivered at the next safe boundary → mapped `input/admitted{synthetic:true}`, strippable, no extra turn opened | L2 |
| A6 | approval deny: harness receives deny, `tool/call.permission` records `evaluated:"deny"`, no execution effect, result is an honest error | L2 (script asserts both sides) |
| A7 | step budget exceeded → `Stopped`; startup deadline → SoftFailed naming the deadline | L1 |
| A8 | `Capabilities` consts equal this table (drift test over a fixture JSON) | L1 |
| A9 | unknown protocol frame → SoftFailed naming it; an unknown advisory system frame is absorbed with a counter (never a silent drop of model-visible content) | L1 |
| A10 | export/replay round-trips at event level; `session/created.bridge` survives and states the class (§4.7) | L2 |
| A11 | without `KIAN_CLAUDE_BIN`/`KIAN_CODEX_BIN` the live suites skip green; with them and credentials they run and are recorded in the runbook — Codex 0.166.0 ran one live turn (`pong`, `Settled`) on 2026-11-20; Claude 2.1.318 is protocol-verified but its model turn is unverified (the local CLI is not logged in) | L3 (env-gated, CI-excluded) |

## 10. Phases

**Phase 1 (this change).** Shared skeleton, Claude Code and Codex bridges, allow/deny approval
policy, durable marker, config, capability matrix, fixtures, conformance tests, docs. dsh
untouched. Cron worktree firings keep the dsh-shaped `superagent.cron.harness` (a `command` +
`profile`); running Claude Code/Codex under cron needs the phase-2 argv extension.

**Phase 2.** Harness hooks → kian plugin bus (Claude Code `PreToolUse`/`PostToolUse` via a
kian-provided hook shim; Codex approvals as `tool/pre` verdicts), durable interactive `ask`
(with the restart semantics designed), interrupt forwarded to a live child, `harness/<name>/*`
hook registry entries, cron per-schedule harness + generic argv.

**Phase 3.** Request-capture proxy (turn projection-only sessions into `exact` ones), an ACP
client driver if ACP coverage holds for both, and `kian-dsh` convergence onto `kian-harness`
(deleting the duplicated skeleton).

## 11. Verification required before coding each adapter

Protocol facts are external and versioned; each row must be pinned with a version and proven
with a live smoke before an adapter claims the capability:

| Fact to pin | Why it matters | Where it bites |
|---|---|---|
| Claude Code stream-json message set, `can_use_tool` shape, interrupt control request, `--resume` semantics | the adapter's whole mapping | `kian-claude` codec, §5.1 — **2.1.318 verified**: `system/hook_*` precede `init`, no ready frame, synthetic API-error assistant messages; `can_use_tool`/`--resume` still unverified live |
| whether Claude Code re-emits history on `--resume` | dedup correctness on takeover | A4 |
| Codex app-server method/notification names, item variants, approval request/response shapes, `thread/resume`, `turn/steer` | the adapter's whole mapping | `kian-codex` codec, §5.2 — **0.166.0 verified**: handshake, `thread/start`, `turn/start`, `item/*` agent message, `turn/completed`; approval shapes and `turn/steer` still unverified live |
| whether either CLI exposes the assembled request anywhere (debug/telemetry) | whether phase 3's proxy is the only route to exact | §2 |
| exit codes and mid-turn kill behaviour of both CLIs | honesty mapping (A3) | §4.5 |

Unverified facts are recorded as such in the runbook; no capability is claimed before its
smoke exists.

## 12. Open questions (decisions the gate should confirm)

1. Phase 1 boundary: is "allow/deny approvals, no durable `ask`, no request bytes" the right
   first slice, or must the request-capture proxy ship in phase 1?
2. Claude Code transport: direct stream-json (chosen) versus driving the community ACP adapter
   (`@agentclientprotocol/claude-agent-acp`, one more process + a third-party version to pin).
3. Codex transport: app-server (chosen) versus `codex exec --json` (simpler, no approvals and
   no mid-turn steering — a materially weaker bridge).
4. The durable marker on `session/created` landed via eager creation (§4.7); the remaining
   open question is whether resume should add a second BEGIN-only key (a harness handle) or a
   separate event in phase 2.
5. Should the two new crates ship behind a `harness-bridges` feature flag, or as plain workspace
   members like `kian-dsh`?
