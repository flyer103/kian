# Runbook — harness bridges (Claude Code / Codex)

What phase 1 proves in default CI, what is **not yet verified**, and the exact commands to run
the live legs. The contract lives in [HARNESS-BRIDGE](../design/HARNESS-BRIDGE.md) and
[ADR-043](../design/decisions/ADR-043-multi-harness-bridge.md); this file is operational only.

## Status

| Leg | Status | Evidence |
|---|---|---|
| Claude Code mapping + driver | **fixture-proven, default CI** | `crates/kian-claude/tests/driver_bridge.rs` (scripted `stream-json` child: turn → kinds, deny/allow provenance, mid-turn death → soft failure) + `crates/kian-claude/src/mapping.rs` unit tests |
| Codex mapping + driver | **fixture-proven, default CI** | `crates/kian-codex/tests/driver_bridge.rs` (scripted `app-server` child: initialize/thread/turn, approval request → decline, crash) + `crates/kian-codex/src/codec.rs` unit tests |
| Config / mount | **default CI** | `crates/kian-cli/src/config.rs` tests: mount resolution, defaults, one-harness rule, worker-surface exclusion, `ask` and bypass-flag refusals |
| Durable bridge marker | **default CI (L1)** | `crates/kian-orchestrator` `create_session_with` (payload written once, idempotent) + `crates/kian-superagent/tests/spawn_session_creation.rs` (create → admit → wake ordering, failed create admits nothing) + config `bridge_marker()` derivation |
| Request capture | **default CI (unit/integration)** | `crates/kian-cli/src/capture.rs` (record-before-forward, response passthrough, oversize/unknown-session fail-closed, never forwarded) + `kian-claude/tests/driver_bridge.rs` (captured attempts append `step/request` before the assistant event, non-JSON is an honest soft failure) |
| Mounted `kian serve` e2e | **default CI** | `crates/kian-cli/tests/harness_bridge.rs`: real `kian serve`, worker template over the scripted Claude peer — mapped turn, no `step/request`, honest end. (Includes a kill -9 mid-turn takeover: the restarted server ends the session `failed`, never `completed`, with no re-run.) |
| Claude Code live | **protocol-verified; model turn UNVERIFIED** — `claude 2.1.318`, not logged in on this host (`Not logged in · Please run /login`) | `KIAN_CLAUDE_BIN=… cargo test -p kian-claude --test live -- --nocapture`: observed `InputAdmitted → TurnStart → TurnEnd(error)` and `Settled` — the error is reported honestly, no fabricated answer |
| Codex live | **positive, 2026-11-20** — `codex-cli 0.166.0`, one real turn, reply `pong`, `Settled` | `KIAN_CODEX_BIN=… cargo test -p kian-codex --test live -- --nocapture`: observed `InputAdmitted → TurnStart → AssistantMessage("pong") → TurnEnd(final_answer)` |

The fixture children speak the *native* protocol shapes pinned below. Codex's live leg is recorded;
Claude's protocol is live-verified but its model turn is blocked by the local login, and neither
live run exercised tool approvals — the capability matrix's `◐` rows for approvals and
`tool/call`-before-execution stay `◐` until a tool-using live run is recorded.

## Protocol pins

Claude Code (`claude --print --input-format stream-json --output-format stream-json --verbose
--permission-mode default`):

- input: `{"type":"user","message":{"role":"user","content":[{"type":"text","text":…}]}}`
- interrupt: `{"type":"control_request","request_id":…,"request":{"subtype":"interrupt"}}`
- approval answer: `{"type":"control_response","response":{"subtype":"success","request_id":…,
  "response":{"behavior":"allow"|"deny","message":…}}}`
- output: `system/init`, `assistant`, `user` (tool results), `result`, `control_request`
  (`can_use_tool`), `control_response`, `stream_event`

Codex (`codex app-server`, JSON-RPC 2.0 JSONL without the `jsonrpc` header; pinned from the
upstream `codex-rs/app-server/README.md`):

- handshake: `initialize` (`clientInfo`) → `initialized` notification
- thread: `thread/start` (returns `result.thread.id`, emits `thread/started`)
- turn: `turn/start` (input items `{"type":"text","text":…}`), `turn/steer` (`expectedTurnId`),
  `turn/interrupt` (`threadId`, `turnId`)
- notifications: `turn/started`, `item/started`, `item/completed`, `turn/completed`
- approvals: `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`; answer
  `{"decision":"accept"|"decline"}`
- `thread/start.approvalPolicy`: `untrusted | on-request | granular | never` (0.166.0 rejects the
  old `unlessTrusted`); the bridge pins `on-request`

## Running the live legs

Both live suites ship and skip cleanly when the env var is absent:

```bash
KIAN_CLAUDE_BIN="$(command -v claude)" cargo test -p kian-claude --test live -- --nocapture
KIAN_CODEX_BIN="$(command -v codex)"   cargo test -p kian-codex   --test live -- --nocapture
```

The Claude CLI must be logged in (`claude` then `/login`, or `ANTHROPIC_API_KEY`) for a model round
trip; without it the run records the honest `TurnEnd(error)`. The same adapters are mounted in
production with `superagent.harness.{claude,codex}` (see README).

A live run is **recorded here** with: the CLI version, the model, the mapped chain observed, the
outcome (`Settled`/`SoftFailed` and why), and any protocol drift. A green fixture run is not a
substitute.

## Durable ask (ADR-049, experimental)

`approvals: { default: ask, durable_ask: true }` records the harness's pending call as a `tool/call`
with `permission.evaluated:"ask"` and ends the drain `awaiting_input` (no lease, no child). A
`control/confirm` decision resumes the harness conversation (ADR-047), delivers a persisted
synthetic nudge, and answers only a re-issued request whose tool + argument digest match the pending
call (one-shot); the result maps onto the pending call with `confirmation` provenance. A different
call follows the policy; a turn that never re-issues closes the pending call with an honest error
result. Fixture-tested; **live re-issue behaviour is unverified** (Claude needs a login, Codex
approval is fixture-only).

## Request capture (ADR-046)

Enable with `superagent.harness.capture:`; kian starts a loopback proxy and points the harness at
it. Probes (local stub upstream, no model call): `claude 2.1.318` honors `ANTHROPIC_BASE_URL`
(`POST /v1/messages?beta=true`, 91 KB body, 3 retries on 5xx); `codex-cli 0.166.0` ignores
`OPENAI_BASE_URL` but honors a generated provider config (`CODEX_HOME`/`-c`, `POST
/v1/responses`, 52 KB body, 5 reconnects). Both used API-key auth; **OAuth-only routes are
unverified**. A full live captured turn (proxy → real model) is not recorded yet.

## Known limits (phase 1, by contract)

- projection-only: no `step/request`, no cache accounting, no byte-request replay;
- the conversation resumes across drains from the persisted handle on `turn/end`
  (`harness.resume`, ADR-047); an interrupted turn is not re-driven — a takeover with no
  undelivered input ends `failed` (the engine's mapping for a lost turn) with a named reason,
  never `completed`, and the next input continues the same conversation;
- `ask` approvals are opt-in and experimental (`approvals.durable_ask: true`, ADR-049): the
  pending call ends `awaiting_input`, a confirm resumes the conversation with a synthetic nudge,
  and only an identical re-issue (tool + arguments digest) is auto-answered; a turn that never
  re-issues closes the call with an honest error result. The live re-issue behaviour is unverified;
- one harness per worker template; a bridge and `endpoints:`/`workers:` are mutually exclusive;
- cron's worktree arm is still dsh-shaped (`--profile <name> <payload>`); running Claude Code or
  Codex under cron needs the phase-2 argv extension.
