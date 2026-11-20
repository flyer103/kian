# ADR-047: Resume the harness conversation across drains

- **Status**: accepted (owner gate, 2026-11-20).
- **Context**: a bridged session spawns a fresh child per drain (HARNESS-BRIDGE §4.6). That is
  honest but loses the harness's conversation at every drain boundary and on takeover: a later
  input starts a new conversation, and an interrupted turn is not resumable at all. The harnesses
  do expose continuation, but only through an id they mint: Claude Code resumes with
  `--resume <session-id>` (its `session_id` arrives in `system/init`; no public
  `--session-id`-style seeding is documented), Codex with `thread/resume <threadId>` (from
  `thread/start`). kian must therefore remember that id durably, and the event stream is the only
  durable store (constraint 1).
- **Decision**:
  1. **The adapter owns the handle.** `HarnessSession::resume_handle()` returns the harness's
     continuation id when known (Claude: the `session_id` from `system/init`; Codex: the thread id
     from `thread/start`).
  2. **The driver persists it on the turn that used it.** When a turn ends, the driver merges
     `"harness":{"resume":<id>}` into the `turn/end` payload (additive; [EVENT-MODEL](../EVENT-MODEL.md)
     §3). No new event kind, no side table: a replay of the stream carries the handle.
  3. **The driver resumes at episode start.** It scans the committed stream for the latest
     `turn/end` carrying `harness.resume` and passes it to
     `SessionFactory::spawn(session, resume)`; the adapters map it to `--resume <id>` (Claude) or
     `thread/resume` (Codex) instead of a fresh session/thread.
  4. **A lost in-flight turn stays lost.** A takeover with no undelivered input still ends
     `SoftFailed` (the interrupted turn is not re-driven — the harness cannot resume a turn, only
     a conversation). The value is continuity: the *next* input continues the same conversation,
     and the ordinary drain boundary no longer discards it.
  5. **Failures are honest.** A resume the harness rejects (missing log, id mismatch, protocol
     error) is a named `SoftFailed`; the driver never silently falls back to a fresh conversation.
     A turn that crashed before its first `turn/end` carries no handle, so the next input starts
     fresh — recorded, not guessed.
  6. **No config, no class change.** Resume is always on when the adapter reports a handle; the
     `capture` class and every existing byte are unaffected.
- **Alternatives considered**:
  1. **Seed a deterministic id** (pass the kian session uuid as the harness's session id) — only
     possible where the harness exposes a seeding flag; Claude Code does not document one, and a
     derived id would still need Codex's thread id. The uniform handle mechanism is smaller.
  2. **A durable side table** (harness-handle store outside the event stream) — rejected by
     constraint 1 (state outside the log cannot be rebuilt).
  3. **A new `harness/*` event kind** — rejected: the frozen 23-kind catalog; the handle is
     turn-scoped audit metadata, exactly what an additive key is for.
  4. **Re-running the interrupted turn on takeover** — rejected: unobservable side effects would
     be replayed; the phase-1 honesty rule stands.
- **Consequences**:
  - multi-turn continuity across drains for Claude Code/Codex; a takeover no longer destroys the
    conversation for subsequent inputs;
  - one additive `turn/end` key; the harness's own session log remains the conversation's truth,
    kian only remembers the pointer;
  - resume risk surfaces honestly on the first harness error after a restart;
  - dsh is unchanged (it already replays its own log).
- **Rollback Trigger**: a harness whose resume path is unreliable or whose handle cannot be
  obtained before the first `turn/end` (then that harness keeps fresh-per-drain and the matrix
  says so).
- **Cost Impact**: one trait method, one factory argument, one additive payload key, adapter
  argv/config plumbing, tests; no new crates, no wire change.
