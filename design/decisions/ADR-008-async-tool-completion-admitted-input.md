# ADR-008: Async tool completion is an admitted input; job rows are pre-event durable state; `stop` is honest cancellation

- **Status**: proposed
- **Date**: 2026-11-01
- **Context**: the article's §4.1 per-session full-duplex — a session keeps processing inputs
  while a long-running command executes, and the command's completion returns to that same
  session. In kian's native shape the input half *is* invariant 3 (inputs are durable rows
  promoted at provider-turn safe boundaries — already true), and the completion half *is* an
  admit→wake→promote round-trip. The only gap was the tool wrapper: the sync `exec` tool
   parks the calling drain until the command settles. A blocked drain would break the
   steer path for the duration of the job; a detached fire-and-forget command without a
  durable completion path would break NoLostWork.
- **Decision**:
  1. **Async completion is an admitted input, never a direct event write.** When a job
     settles, the completion bridge (kian-cli assembly) admits a provenance-wrapped
     `{"kian":{"type":"async_result","job_id":…,"status":…,"result":…}}` content envelope
     to the originating session's inbox via `InboxWriter`, then wakes — strictly after the
     row is durable (invariant 1). The completion is promoted at the next safe boundary as
     a normal message. The built-in loop, the promote-boundary semantics and the events
     table are untouched; no new event kind exists.
  2. **Job rows are pre-event durable state, per ADR-007's pattern.** `async_jobs`
     (`job_id PK, session, endpoint_id, cmd JSON, status
     pending|inflight|done|cancelled|abandoned, tool_call_ref, result, created_at,
     updated_at`) lives in the shared `kian.sqlite` under `kian-exec`'s own append-only
     migration chain. The row is committed **before dispatch** (admit-before-execute). The
     job id *is* the command id, derived from the calling tool-call id (UUIDv5), so
     recovery and replay dedupe onto the same row. Dispatch itself is entirely the 
     durable queue's business — `async_jobs` adds only "who to notify", never a second
     dispatch system. Job statuses are monotone and terminal rows freeze; cancellation
     wins a settle race by guarded transition, exactly like the queue.
  3. **`stop` is honest cancellation, not erasure.** Cancelling marks the job `cancelled`
     (terminal) and kills the in-flight command best-effort: the v0 wire has no kill frame,
     so a not-yet-dispatched queue row is abandoned (it never flies) and an already-running
     command is unkillable — its late result is discarded by the queue's terminal guard.
     The cancelled job's completion input **still lands**, labeled `cancelled`, and the
     `stop` tool's own result reports what actually happened (including "already settled").
  4. **Honest no-op on terminal sessions.** If the target session projects terminal (or is
     gone), the bridge drops the completion with an audit line and a counter — never a
     silent swallow, never a zombie row claim, nothing left in any wake registry.
  5. **Restart semantics.** On startup (after the inbox rebuild) the bridge re-drives
     `pending`/`inflight` jobs (observers re-attach through the queue, which dedupes at the
     row) and re-offers settled rows to the sink; the admit idempotency key
     `async-result:{job_id}` dedupes redelivery at the inbox row.
  6. **Default off.** The pair mounts on the Router profile and worker template only when
     `superagent.async_tools` is on (the article's own "not default-on"); the sync `exec`
     tool's resolution semantics are unchanged either way.
- **Alternatives considered**:
  1. Completion as a new event kind appended by the executor — violates the layered iron
     rule (execution appends events; an external completion is *input*, and inputs are
     durable inbox rows admitted then promoted, not events written out of band).
  2. A separate async dispatch queue — duplicates 's proven at-least-once machinery and
     forks the correlation scheme; rejected in favor of one queue plus a notify row.
  3. Blocking the drain but streaming progress — still blocks steers for the job's lifetime;
     the path must interleave, so the tool resolves fast instead.
  4. Kill-frame wire extension for `stop` — a wire-protocol change is out of scope;
     best-effort cancellation with an honest label is the v1 semantics.
- **Consequences**: a completion may arrive while the session is mid-drain; it is promoted
  only at a provider-turn boundary (cache-byte stability is untouched). Duplicate
  *execution* across a crash window remains ADR-007's documented at-least-once; duplicate
  *completion delivery* is deduped at the inbox row. `kian-exec` never names an inbox, an
  engine or an event — its whole session-facing surface is the `AsyncResultSink` callback.
- **Rollback Trigger**: if real workloads need hard kills or exactly-once completion
  effects, extend the wire protocol (kill frame) and/or add endpoint-side dedupe rather
  than weakening the admitted-input path.
- **Cost Impact**: Positive — long jobs no longer pin a drain (fewer held leases and no
  idle provider turns); one small table, no new event kinds, no loop changes.
