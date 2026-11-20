# Runbook — harness bridges (Claude Code / Codex)

What phase 1 proves in default CI, what is **not yet verified**, and the exact commands to run
the live legs. The contract lives in [HARNESS-BRIDGE](../design/HARNESS-BRIDGE.md) and
[ADR-041](../design/decisions/ADR-041-multi-harness-bridge.md); this file is operational only.

## Status

| Leg | Status | Evidence |
|---|---|---|
| Claude Code mapping + driver | **fixture-proven, default CI** | `crates/kian-claude/tests/driver_bridge.rs` (scripted `stream-json` child: turn → kinds, deny/allow provenance, mid-turn death → soft failure) + `crates/kian-claude/src/mapping.rs` unit tests |
| Codex mapping + driver | **fixture-proven, default CI** | `crates/kian-codex/tests/driver_bridge.rs` (scripted `app-server` child: initialize/thread/turn, approval request → decline, crash) + `crates/kian-codex/src/codec.rs` unit tests |
| Config / mount | **default CI** | `crates/kian-cli/src/config.rs` tests: mount resolution, defaults, one-harness rule, worker-surface exclusion, `ask` and bypass-flag refusals |
| Durable bridge marker | **default CI (L1)** | `crates/kian-orchestrator` `create_session_with` (payload written once, idempotent) + `crates/kian-superagent/tests/spawn_session_creation.rs` (create → admit → wake ordering, failed create admits nothing) + config `bridge_marker()` derivation |
| Mounted `kian serve` e2e | **default CI** | `crates/kian-cli/tests/harness_bridge.rs`: real `kian serve`, worker template over the scripted Claude peer — mapped turn, no `step/request`, honest end. (Includes a kill -9 mid-turn takeover: the restarted server ends the session `failed`, never `completed`, with no re-run.) |
| Claude Code live | **UNVERIFIED** — no real `claude` binary in this environment, so no live run is recorded | run the command below |
| Codex live | **UNVERIFIED** — no real `codex` binary in this environment | run the command below |

The fixture children speak the *native* protocol shapes pinned below; an unverified live leg means
the shapes may drift with the CLI version. Until a live run is recorded, the capability matrix's
`◐` rows stay `◐`.

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

## Running the live legs

Claude Code (needs a real `claude` and a working credential):

```bash
export KIAN_CLAUDE_BIN="$(command -v claude)"
# No live suite ships in phase 1; drive it through a worker session with
# `superagent.harness.claude.command` set to the same path, or point the fixture tests'
# adapter at the real binary in a scratch harness config.
```

Codex:

```bash
export KIAN_CODEX_BIN="$(command -v codex)"
# Same shape: mount `superagent.harness.codex` with `command: $KIAN_CODEX_BIN` and
# `approval_policy: unlessTrusted`, then run one worker turn.
```

A live run is **recorded here** with: the CLI version, the model, the mapped chain observed, the
outcome (`Settled`/`SoftFailed` and why), and any protocol drift. A green fixture run is not a
substitute.

## Known limits (phase 1, by contract)

- projection-only: no `step/request`, no cache accounting, no byte-request replay;
- no cross-process harness resume: a later input starts a fresh harness conversation; a takeover
  with no undelivered input ends `failed` (the engine's mapping for a lost turn) with a named
  reason, never `completed`;
- `ask` approvals refuse boot (phase 2): an allow/deny policy answers the harness's own gate;
- one harness per worker template; a bridge and `endpoints:`/`workers:` are mutually exclusive;
- cron's worktree arm is still dsh-shaped (`--profile <name> <payload>`); running Claude Code or
  Codex under cron needs the phase-2 argv extension.
