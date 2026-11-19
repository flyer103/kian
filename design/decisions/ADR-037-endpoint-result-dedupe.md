# ADR-037: The endpoint answers a re-dispatched command from a bounded result cache

- **Status**: accepted (owner gate, 2026-11-19)
- **Date**: 2026-11-19
- **Context**: on 2026-11-19 the owner instance was down for ~9 h (04:52–13:57 UTC) because a
  worker-dispatched command rewrote `/srv/kian/kian.yaml` as root (`open(tmp)` + `os.replace` + a
  `copystat` mode copy), leaving the file `root:root 0640`; `kian serve` (systemd `User=kian`)
  refused every start with `Permission denied`, and systemd restart-looped it 9,982 times. The
  outage itself is an operations failure (config-write discipline, crash-loop alerting), not a
  delivery-semantics defect. What this ADR fixes is the **amplification** on recovery:

  1. 04:52:37 — row `dfbdabd5` (`ssh vultr "systemctl restart kian; sleep 6; systemctl is-active
     kian; …"`, a mac-mini dispatch) was sent; the restart killed the server before the result
     could be acknowledged, so the row stayed `inflight` with one dispatch attempt.
  2. 13:52:56 — the server finally started once during the outage; the driver attached the route,
     loaded the `inflight` row and re-drove it (ADR-007 at-least-once, ADR-033 attempt 2).
  3. The mac-mini worker **executed the command a second time**, restarting `kian` again, which
     destroyed the ack path again. The row then converged `abandoned` (ADR-033) at 13:53:54; the
     re-armed session's caller got `thread/completed: failed` at 13:52:30 with no tool result.

  ADR-007 documents duplicate execution on crash-replay as an accepted v1 cost, and names the fix
  as its own rollback trigger: *"if real workloads cannot tolerate duplicate execution, add
  endpoint-side dedupe (persisted last-executed command id) rather than weakening the durable
  queue"*. This incident is that trigger: for a command whose side effect **is** the ack path, the
  at-least-once replay is not a harmless retry — it repeats the damage, once per re-drive, and
  ADR-033's bound only stops it after the damage has been done `1 + max_redrives` times.

- **Decision**:
  1. **The endpoint keeps a bounded result cache keyed by command id.** `kiand` records the
     `CommandResult`/`FsCommandResult` frame **before** it is sent. A re-dispatched command id
     (same UUID — the row id *is* the wire id, ADR-007 decision 3) is answered from that record
     instead of being executed again. The wire is unchanged: the duplicate is answered with an
     ordinary result frame, so no new frame, no protocol version bump and no coordinated upgrade
     of server and workers are required.
  2. **Concurrent duplicates coalesce to one execution.** A duplicate that arrives while the same
     id is still executing subscribes to the running execution and receives the same frame; it
     never starts a second process. If the owner dies without producing a frame (e.g. a panic),
     the follower drops the duplicate instead of hanging — the row stays re-drivable.
  3. **The cache is durable, bounded and advisory.** It is an append-only JSONL file under the
     worker's state directory, loaded and compacted at startup: TTL 24 h, at most 512 entries,
     at most 256 KiB per entry (larger results are not cached and re-execute), 64 MiB file
     budget. A miss, an oversized result, an unreadable or corrupt entry all mean *execute again*
     — at-least-once is unchanged and no result is ever fabricated. A cache that cannot be opened
     degrades to memory-only with one warning; it never refuses startup.
  4. **It is on by default and can be turned off.** `kiand --dedupe-dir <dir>` (env
     `KIAND_DEDUPE_DIR`) sets the location; `--no-dedupe` (env `KIAND_DEDUPE=0`) disables it; the
     default location is `<state dir>/results.jsonl` (`$KIAND_STATE_DIR`, else
     `$HOME/.kian/kiand`). With dedupe off the endpoint behaves exactly as before this ADR.
  5. **Re-drive gets a small grace.** `RemoteExecConfig::redrive_grace` (new
     `workers.queue_redrive_grace_ms`, default 5 000, ceiling 60 000) delays re-dispatch of an
     `inflight` row whose last dispatch is younger than the grace, so a reconnect cannot race an
     execution that may still be running on the worker. The grace only delays a dispatch: it
     never settles a row, never changes the caller's bound, and a row older than the grace
     re-drives exactly as before.
  6. **Audit, not silence.** A cache hit logs one `info` line (`command_id`, age, source of the
     frame) and a follower logs one `info` line; the server-side row settles exactly as it would
     for a fresh execution, because it is the same logical command.

- **Alternatives considered**:
  1. **Server asks the endpoint for the result before re-driving (`result?` frame).** Rejected for
     now: it is a wire change (new frame + protocol version bump + coordinated upgrade of every
     worker — the exact window that produced ADR-033's 13 h outage). The endpoint cache answers
     the same question with the frames that already exist. If results are still lost because a
     worker restarted before persisting (write-ahead is per-result, not per-dispatch), this frame
     is the recorded follow-up.
  2. **Persist only the last executed command id (ADR-007's minimal hook).** Rejected: refusing to
     re-execute without being able to answer leaves the row with nothing to settle with; the
     cache must carry the result bytes.
  3. **Delay re-drive by `queue_timeout` instead of a small grace.** Rejected: it converts every
     genuinely lost result into an abandonment (the caller's timeout wins the race) and turns a
     recoverable command into a permanent failure. The race this fixes is narrow; the grace is
     correspondingly small.
  4. **Server-side only: epoch-guard re-drive to the replica that dispatched it.** Rejected: the
     duplicate execution happens *on the endpoint*, not on another server; no server-side rule can
     know whether the worker already ran the command.

- **Consequences**:
  - The endpoint is no longer fully stateless. ADR-007's "the endpoint stays stateless" is
    superseded **for this one artifact**: the cache is keyed by a command id with no session,
    thread, worker or model identity, and it can only answer a command the endpoint already ran —
    it cannot influence which command runs next, so brain–hand separation holds.
  - The endpoint now persists unredacted command output (as the in-flight result already held it).
    The file is created `0600` in the worker's own state directory — the same trust domain as the
    worker's log — and redaction still happens exactly once, server-side, at settle
    (`kian-exec` `complete`). An operator who cannot accept that uses `--no-dedupe`.
  - A cache hit returns the bytes of the earlier execution, including its original timestamps and
    exit code. That is the point (it is the same logical call), but it means a command with
    time-dependent output is *not* re-run to produce fresh output.
  - A worker that restarts keeps its cache; a crash between execution and the write-ahead record
    re-executes (at-least-once, unchanged).
  - The re-drive grace adds up to `queue_redrive_grace_ms` latency to the recovery of a genuinely
    lost result, and to nothing else.
  - A command id is deterministic (UUIDv5 of the tool-call id), so a session restored from an
    archive and re-executed within the TTL is answered from the record instead of running again.
    That is the same dedupe the server already applies to a settled row; an operator who intends a
    genuine re-run issues a new call (new id) or starts the worker with `--no-dedupe`.
  - The default file is shared by every `kiand` on the host. Ids are globally unique, so a shared
    record cannot answer a different call; a record lost to two processes compacting at once is a
    miss (re-execute), never a wrong answer.

- **Rollback Trigger**: set `workers.queue_redrive_grace_ms: 0` and start workers with
  `--no-dedupe` / `KIAND_DEDUPE=0` — both are config-only rollbacks, with no migration, no wire
  change and no store change. A cache that is suspected of serving a stale result for a
  legitimately re-issued command id is a correctness incident: command ids are UUIDv5 of the
  tool-call id, so a collision means the id derivation changed and must be investigated before
  the cache is re-enabled.

- **Cost Impact**: positive — it removes the repeat execution (and the second self-restart) from
  every recovery replay, with one local file and no new coordination. Disk use is bounded by the
  cache budget (64 MiB).
