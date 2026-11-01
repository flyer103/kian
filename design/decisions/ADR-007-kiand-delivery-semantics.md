# ADR-007: kiand delivery semantics — server-side durable queue, at-least-once, command-id correlation

- **Status**: proposed
- **Date**: 2026-11-01
- **Context**: S5 requires a cloud loop to keep working while its execution endpoint
  (`kiand`) is offline, and to recover cleanly across a server crash: a command issued while
  the endpoint is down must not vanish, and a crash mid-flight must not produce a phantom
  result or a lost one. The event stream is the sole truth for *sessions*, but a queued
  command is not session state — it is pre-event durable state exactly like an inbox row:
  it exists before execution starts, and its result reaches the session only through the
  normal `tool/call`/`tool/result` loop path.
- **Decision**: command delivery to `kiand` runs through a **server-side durable sqlite
  queue** (`exec_queue` in the shared `kian.sqlite`, under `kian-exec`'s own migrations):
  1. **Enqueue-first** (admit-before-execute): the row (`pending`) is committed before any
     dispatch flies. Offline endpoint → the row parks; the caller's future parks with it.
  2. **At-least-once**: on reconnect — and after a server restart — `pending` *and*
     `inflight` rows are re-driven (`inflight` is presumed lost). A command re-delivered
     across a crash window may therefore execute twice on the endpoint. This is documented
     and caller-tolerated in v1; endpoint-side dedupe is an explicit Phase-3 hook.
  3. **Command-id correlation**: the durable `command_id` is derived deterministically from
     the calling tool-call id (UUIDv5), so session recovery and re-execution collapse onto
     the same row — one row, one dispatch, ever; a settled row answers duplicates from its
     recorded result. The wire command id equals the row id, making correlation observable
     end to end.
  4. **Honest timeout**: `queue_timeout` expires to `ExecError::Timeout` (never a fabricated
     result) with the row settled `abandoned` — terminal, like `done`. Queue-row statuses
     are monotone (`pending → inflight → done|abandoned`); no row is ever deleted and no
     committed `done` row is ever mutated (guarded transitions only).
- **Alternatives considered**:
  1. Exactly-once via endpoint-side idempotency keys — requires durable state on the
     endpoint, which the endpoint deliberately does not keep (brain–hand separation);
     deferred to Phase 3.
  2. Fail-fast while offline (no queue) — fails S5's core semantic (offline enqueue parks
     durable, resolves when the endpoint returns).
  3. Queue as session events — violates the event contract: a queued command is not a
     session fact until its result enters the loop's `tool/call`/`tool/result` path.
- **Consequences**: duplicate execution on crash-replay is real and must stay documented
  (callers run side-effecting commands); queue rows are a second durable table family next
  to the event stream and the inbox, owned by `kian-exec`; the endpoint stays stateless.
- **Rollback Trigger**: if real workloads cannot tolerate duplicate execution, add
  endpoint-side dedupe (persisted last-executed command id) rather than weakening the
  durable queue.
- **Cost Impact**: Positive — commands survive endpoint downtime and server crashes with
  one small table family and no distributed coordination.
