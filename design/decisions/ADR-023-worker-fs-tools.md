# ADR-023: Worker filesystem tools — KiandProto v2 `fs_list` / `fs_read` / `fs_write`

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: `design/WORKER-MODEL.md` §4/§6/§7/§9 (worker-domain semantics),
  `design/API-CONTRACT.md` §8.3/§8.6 (wire v2), `design/DESIGN.md` §4.3/§4.7 (per-drive side,
  dispatch), ADR-007 (at-least-once queue), ADR-015 (per-call permission provenance), ADR-021
  (runtime membership), ADR-022 (reasoning replay — independent).
- **Context**: the landed worker surface is exactly one tool, `exec` (argv, no shell), routed 1:1 to
  the session's bound worker id. The built-in `fs_list`/`fs_read`/`fs_write` tools exist only in the
  local `kian-loop` toolset and operate on the **kian server's** filesystem; a worker session that
  lists them therefore reads/writes the wrong machine. That is not a cosmetic gap: the whole point
  of a worker is to act on *its* machine (repo checkouts, build outputs, laptop files). The owner
  deployment surfaced it immediately: the worker agent had only `exec`, and the workaround
  (shelling `cat`/`tee`) loses the local tool contracts (typed errors, path guard, result shapes).
  Two paths were possible: synthesize fs over `exec` on the server side, or add a real worker
  command family. The first is a quoting/encoding/size minefield and cannot match the local result
  shapes; the second needs a wire change but keeps every existing invariant (durable queue,
  command-id dedupe, no failover, 1:1 binding).
- **Decision**: add a filesystem command family to the worker protocol and route the built-in fs
  tool names to the bound worker.

  1. **Wire v2, additive frames.** `PROTOCOL_VERSION = 2`. The exec frames (`command`/`result`) keep
     their v1 bytes; the new frames are `fs_command {id, cmd}` (server → worker) and
     `fs_result {id, result|error}` (worker → server). `FsCommand`/`FsResult` are tagged with their
     tool name (`fs_list`/`fs_read`/`fs_write`) and live in `kian-core` so both halves share one
     type. No new HTTP route, no new event kind, no new ops method.
  2. **Durable queue, same discipline.** Fs commands ride the existing per-worker queue: enqueue
     before dispatch, park `pending` while the worker is offline, at-least-once with command-id
     dedupe at settlement, honest `queue_timeout` — no cross-worker dispatch and no local fallback.
     The queue migration v3 adds `kind TEXT NOT NULL DEFAULT 'exec'`; pre-v3 rows decode as exec and
     re-drive unchanged (no row rewrite, no lost work).
  3. **Per-drive routing, config-owned surface.** A bound worker session mounts the remote fs tools
     (filtered by its agent's `tools:` list) instead of the local fs subset; the Router/plain
     sessions keep the local tools; the refusing side (unconfigured/malformed binding) mounts the
     same three names with an honest refusal and no backend handle. Tool names, descriptions and
     parameter schemas are byte-identical to the local built-ins (golden parity test): the
     model-visible surface does not change with *which* machine runs the call.
  4. **Workspace root + limits.** The worker resolves paths under `--workspace-root` (default: its
     working directory; validated to exist and be a directory at startup) and applies the same
     lexical `..` rule as the local tools. Read and write bodies are capped at 1 MiB each; a larger
     body or a non-UTF-8 read is a typed `io` error surfaced as the local `{"error": …}` tool
     result. The root is a **guard, not a sandbox** — `exec` already runs arbitrary programs as the
     same user, so fs adds no authority; this limitation is recorded rather than implied.
  5. **At-least-once writes.** A crash/reconnect can re-execute an fs command; `fs_list`/`fs_read`
     are idempotent, and a re-executed `fs_write` writes the same path and content (mtime may
     change). This is the existing queue contract, made explicit for side-effecting fs writes.
  6. **Permissions unchanged.** `permissions:` (ADR-015) is evaluated per call before the tool
     event and before invocation; a denied `fs_write` never enqueues anything. The remote tool
     carries no permission logic of its own.

  Adversarial consequences checked: a crash after enqueue re-drives the row (at-least-once); a crash
  after the write but before the result re-drives and rewrites the same bytes; a worker going
  offline mid-fs parks the row to the honest queue timeout; path traversal is refused both
  server-side (before a row exists) and worker-side; a too-large body is an honest error; a v1
  worker is refused at the handshake and its parked rows stay parked (no dispatch to a wrong
  machine); the queue's old rows keep their exec meaning; concurrent writes to one path are
  last-writer-wins (no locking is promised).

- **Alternatives considered**:
  - **Synthesize fs over `exec`** (`cat`, `tee`, base64 in argv/env): no wire change, but shell
    quoting, environment-size limits, no typed results, and a different result shape from the local
    tools — rejected.
  - **Keep fs on the server and call it "remote"**: wrong machine; the defect being fixed.
  - **A second connection/backend for fs**: breaks the one-route-per-worker rule (a second dial with
    the same id replaces the first) and duplicates the queue; rejected.
  - **A new HTTP surface for worker fs**: contradicts the narrow inbound surface (WORKER-MODEL §6).
  - **Unbounded fs (no caps)**: queue/trace growth and prompt blow-ups; a 1 MiB honest error is the
    product choice.
- **Consequences**: one wire bump (v1 → v2) with kian-first upgrade; one additive queue column
  (v3); the worker tool surface grows from one tool to four, so a worker agent's prompt prefix
  changes once (new sessions; live sessions keep their persisted prompts). Existing exec behavior,
  bindings, membership and queue semantics are unchanged. The filesystem family is text-only and
  non-streaming; binary bodies, glob/edit operations and a symlink-proof sandbox are explicitly
  out of scope (recorded in WORKER-MODEL §9).
