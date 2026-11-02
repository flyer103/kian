# dsh-v0-peer — the contract-v0 peer for `dsh --profile sdk`

The ecosystem half of the kian S6 bridge (card C59). `dsh-v0-peer.mjs` is a
Node ESM script (node: builtins only, no npm dependencies, executable) that
adapts the stock `dsh --profile sdk` JSON-RPC 2.0 server onto kian's frozen
bridge contract v0 (ADR-010). The kian side (`crates/kian-dsh`) is the frozen
contract; this peer adapts **to** it.

```
kian DshDriver <--v0 JSON-lines--> dsh-v0-peer <--JSON-RPC 2.0--> dsh --profile sdk
```

## Running

The kian driver spawns the script with the default `["--profile", "sdk"]`
argv (accepted and ignored — they belong to the inner dsh spawn):

```bash
source <(grep '^DEEPSEEK_API_KEY=' ~/.hermes/.env | sed 's/^/export /')   # export-prefix matters; never echo the value
NO_PROXY=127.0.0.1,localhost \
KIAN_DSH_BIN="$PWD/contrib/dsh-v0-peer/dsh-v0-peer.mjs" KIAN_DSH_PEER=1 \
DSH_PEER_SESSION="kian-live-$(date +%s)" \
cargo test -p kian-dsh --test live -- --nocapture
```

Requirements: `node` ≥ 18 (ESM + `node:` specifiers), `dsh` 0.2.1-rc.5 on
`PATH` (or `$DSH_BIN`), `DEEPSEEK_API_KEY` in the environment for a model
round trip. Without the credential the run still exercises the ready
handshake and exits honestly — see "Honesty" below.

Env knobs (all optional): `DSH_BIN`, `DSH_PEER_CWD`, `DSH_PEER_PROVIDER`
(default `deepseek-official` — **`deepseek` is not a registered adapter and
`initialize` rejects it**), `DSH_PEER_MODEL` (default `deepseek-chat`),
`DSH_PEER_SESSION` (default `kian-` + sha256(cwd)[0:16], stable per working
directory so a restarted peer continues the same dsh session; dsh's own
durable session log is then the replay — turn numbering continues
monotonically and resumed sessions do **not** re-emit historical events).

## Observed SDK event vocabulary (empirical, dsh 0.2.1-rc.5, deepseek-chat)

One real prompt (`Reply with exactly the word: pong`) drove this stream.
Table: observed dsh SDK `session.event` envelope → v0 frame emitted.

| Observed SDK event (envelope `{type, seq, time, data}`) | v0 frame emitted | Notes |
|---|---|---|
| `turn/start` `{turn}` | `session-event` kind `turn/start` | absorbed by the kian mapper (episode envelope) |
| `step/start` `{turn, step}` | kind `step/start`, payload `{}` | maps to kian `turn/start` |
| `system/message` `{turn, step, message}` | kind `system/message`, payload `{content: message.content}` | maps to `input/admitted` (role system) |
| `user/message` (`data` IS the UserMessage) | kind `user/message`, payload `{content}` (+ `synthetic:true` when `source.kind !== "user"`) | the direct prompt and 3 harness-injected contexts (AGENTS.md reminder, runtime-context snapshot, skills catalog) all arrive as `user/message`; the producer-kind test marks the injected ones synthetic |
| `request/header` `{header, reason}` | **synthesized** kind `assistant/attempt`, payload `{status: "pre-flight/<reason>", request: {header, messages: <model-visible surface so far>}}` | the pre-flight moment: the SDK appends `request/header` inside the step **before dispatch**. This is the adaptation point — see below |
| `assistant/message` `{turn, step, message, stream, usage?}` | kind `assistant/message`, payload `{content: message.content}` | the real model response (observed: `[{type:"reasoning"},{"type":"text","text":"pong"}]`) |
| `assistant/attempt` `{turn, step, stream}` (failed attempts only; not observed live) | kind `assistant/attempt`, payload `{status: "settled-without-message", request: <same durable reconstruction>}` | the SDK embeds the *response* stream, never request bytes |
| `tool/call` `{turn, step, callId, name, arguments}` | kind `tool/call`, payload `{id: callId, name, arguments}` | arguments kept as the raw JSON string; not observed live (no tools requested) |
| `tool/result` `{turn, step, message, error?, meta?}` | kind `tool/result`, payload `{call_id: message.toolCallId, output: message.content, is_error}` | not observed live |
| `step/end` `{turn, step}` — **carries no outcome** | kind `step/end`, payload `{outcome, tool_calls?}` — **buffered** until the outcome is derivable | flushed at the next `step/start` (→ `tool_calls`, count from the step's `tool/call`s) or at `turn/end` (reason → outcome, below) |
| `turn/end` `{turn, reason}` | kind `turn/end` (absorbed) + triggers the buffered `step/end` flush | reason mapping: `completed`→`final` (or `tool_calls` if the closing step had tool calls), `error`/`blocked`→`error`, `aborted`/`interrupted`/`forked`→`cancelled`, `max-tokens`→`final` (closest call, logged) |
| `developer/message`, `request/context`, `session/end-seed` | — dropped, stderr-logged | real durable kinds with no contract-v0 row (ADR-010 table); `developer/message` still feeds the request reconstruction (it is model-visible) |
| `permission/preset`, `sandbox/mode`, `approval/policy`, `agent/inbox/spliced`, `session/title`, `session-log-deepseek/delivery-accepted` | — dropped, stderr-logged | plugin-registered records outside `SessionEventMap`, no v0 counterpart |
| `session.status`, `subagent.started/finished` notifications | — (not session events) | status transitions logged; subagent notifications logged |

Notifications from foreign session ids (subagent children) are dropped with
a log line.

## Divergences from ADR-010's assumed vocabulary (reported, not patched)

The ADR is frozen; these are the exact observed deltas, recorded here and in
card C59's completion:

1. **The kind vocabulary itself matches** — every `session.event` type seen
   (`turn/start`, `step/start`, `system/message`, `user/message`,
   `request/header`, `assistant/message`, `step/end`, `turn/end`) exists
   verbatim in the ADR-010 table or its "deliberately outside v0" list. The
   divergence is in **payloads and timing**, not kinds.
2. **`step/end` carries no outcome.** The v0 contract requires
   `outcome: tool_calls|final|error|cancelled`; the SDK puts the reason in
   the following `turn/end`. The peer buffers `step/end` and derives the
   outcome (table above).
3. **Successful steps emit no `assistant/attempt`.** The SDK appends
   `request/header` (config + tools, no messages) inside the step *before
   dispatch* and folds settled request records there; `assistant/attempt`
   exists only for failed/retried/cancelled attempts and embeds the response
   stream. The pre-flight rule (no response frame before the step's request
   bytes) would therefore reject every successful step. The peer satisfies
   it the only honest way available: it synthesizes the v0
   `assistant/attempt` at the `request/header` moment (genuinely pre-flight)
   with a **durable reconstruction** — the header verbatim plus the
   model-visible surface assembled from the same event stream. The literal
   provider wire bytes are not observable on the SDK surface; the
   reconstruction is labeled by `status` and recorded, never invented.
4. **Synthetic marking rides the producer kind.** dsh marks injected user
   context via `UserMessage.source.kind !== "user"`; the peer translates
   that to the v0 `synthetic: true` payload marker (A6 strip survives).
   Note: `<system-reminder>` workspace-instruction injections arrived with
   source.kind `user`-classified producers and are therefore **not** marked
   synthetic — the peer is faithful to the source metadata, not to content
   sniffing.
5. **Extra event kinds beyond the dsh-session core map** (permission/preset,
   sandbox/mode, approval/policy, agent/inbox/spliced, session/title,
   session-log-deepseek/delivery-accepted) arrive on real sessions — outside
   ADR-010's assumed vocabulary; dropped with stderr logs (an UnknownKind
   error would be the alternative if they ever need durable counterparts —
   that is a contract amendment).
6. **Provider name**: the registered adapter is `deepseek-official`, not
   `deepseek` (initialize rejects the latter).

## Honesty

- **No cancel method exists in the SDK protocol.** An `interrupt` arriving
  while a dsh step is open kills the dsh child, so the turn settles as an
  attempt with no `assistant/message` — `interrupted`, never `completed`.
  An `interrupt` at an idle boundary cancels nothing and only logs to
  stderr. Nothing is ever faked as cancelled.
- **No `agent.inject` on the SDK wire.** A v0 `steer` frame can only ride as
  an ordinary `session/prompt`; the harness-injected `synthetic` marker
  cannot survive that path (the prompt arrives as a plain user message).
  Known limitation, logged when it happens.
- **Shutdown is the SDK-blessed lifecycle end.** After the turn ends with
  no prompt in flight, the peer sends the `shutdown` request and exits with
  dsh's exit code. A peer whose dsh child dies mid-stream exits non-zero
  after logging; the kian driver reports that honestly.
- Failure to spawn dsh or to `initialize` is fatal (stderr + exit 1) — the
  kian side surfaces a spawn/exit failure rather than a vacuous success.

## Verified live (2026-11-02)

`KIAN_DSH_BIN=$PWD/contrib/dsh-v0-peer/dsh-v0-peer.mjs KIAN_DSH_PEER=1
cargo test -p kian-dsh --test live` — both live tests green against the
real DeepSeek model: outcome `TurnDone → Settled`, appended chain
`[TurnStart, InputAdmitted, StepRequest, AssistantMessage, TurnEnd]`, model
reply "pong". See docs/runbook-dsh.md for the current status.
