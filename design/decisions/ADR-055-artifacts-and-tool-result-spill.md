# ADR-055: Tool results ride the event stream by reference, and the blob store is disposable

- **Status**: accepted (owner direction 2026-11-22; phase 2 slice).
- **Context**: the event stream is the sole truth and every model-visible byte is persisted before it
  flies (constraints 1-2), but a tool result is exactly the payload class that breaks the economics
  of that rule: a `web_fetch`, an OCR page, a files-API read, or one `exec` dump can be megabytes,
  and inlining it into the log (a) makes every later `step/request` body, export and read-model page
  carry it, (b) forces the model to page through it in context at the hit-token price, and (c)
  turns retention into a log-size problem instead of a policy problem. Three properties are
  required. (1) **No silent giant payload**: the model must see a bounded, honest rendering of the
  result, with the full bytes reachable on demand through a tool. (2) **Replay correctness**: a
  crash, a restart, or a resumed session must derive the same model-visible context without the blob
  — the blob store cannot be a second truth. (3) **No reference counting**: kian has no session
  enumeration API and compaction rewrites ranges, so 'which sessions still reference this blob' is
  not derivable at GC time, and a refcount would become authoritative state outside the event log
  (constraint 1). The design therefore has to make blob loss a *normal*, documented outcome rather
  than a corruption.
- **Decision**:
  1. **The event carries a reference plus a bounded preview, never the full bytes.** A spilled
     `tool/result` keeps the frozen envelope and replaces only `content.output` with
     `{"artifact":{"id","size","media_type"},"preview":<str>,"truncated":true}`. The artifact id is
     the sha256 hex of the bytes, so it is content-addressed and independent of any session or
     clock. The preview is the first `inline_max_bytes / 4` bytes decoded lossily as UTF-8; it is
     marked `truncated: true` and is what the model reads in the turn's context. The event stream
     stays the sole truth: replaying a session yields the same preview bytes with or without the
     blob.
  2. **The spill happens inside the append, so persisted bytes equal model-visible bytes.**
     `DrainCtxCore::append_event` (the single write path every drain shares) rewrites a `tool/result`
     payload before the append, not in the loop or the tool adapter. There is no window in which the
     full output would have been persisted and then shrunk, and no second code path that could
     persist a different shape. The rewrite is idempotent: a payload whose output already carries
     an `artifact` key is left alone, so a re-append, a replay repair or a bundle that composes an
     artifact reference cannot double-spill.
  3. **Only successful results spill, and only above the configured bound.** `is_error: true`
     results stay inline whatever their size: an error is the one payload a human and a model must
     read without a second tool call, and error strings are bounded by construction. Spill applies
     only when the `artifacts:` block is configured (`inline_max_bytes`, default 16 KiB) and the
     compact JSON of `content.output` exceeds it. With no `artifacts:` block nothing changes: no
     rewrite, no new bytes, byte-identical legacy sessions.
  4. **Blob writes are atomic and failures degrade honestly.** `ArtifactStore::put` writes a temp
     file next to the final path and renames it into place, so a crash leaves either no blob or a
     complete one; an already-present blob is reused (content-addressed dedupe). Ids are validated
     as exactly 64 lowercase hex characters before any path join, so a crafted id can never traverse
     out of the root. If the blob write fails (full disk, permissions), the append degrades to the
     inline output rather than losing the result or failing a completed tool call: the log remains
     truthful, the degradation is visible as the absence of an `artifact` key, and the configured
     size bound is a target, not a lie.
  5. **`artifact_read` is the only reader, and it is windowed and honest.** The tool takes
     `{id, offset?, limit?}` (default 8 KiB, hard cap 64 KiB) and returns
     `{id, size, offset, content, next_offset?}`; out-of-range offsets clamp to the end and return
     empty content, and a missing blob returns `{"error":"artifact expired or unavailable",
     "artifact_id":...}` as an honest error result rather than an empty success. It is mounted at
     the assembly edge (`kian-cli`) as an ordinary `ToolProvider`, composed with the local tools
     when `artifacts:` is configured, and its name is added to the session's mounted tool list so
     the fail-closed schema filter keeps it. Core never names it and no provider-specific code
     enters the core (constraint 10).
  6. **GC is retention-based, never reference-counted.** `kian artifacts gc` (and an operator
     ticker later) walks the blob tree oldest-mtime-first and deletes until both bounds hold:
     total bytes <= `gc.max_bytes` (default 10 GiB) and mtime >= now - `gc.retention_days` (default
     30 days). A blob deleted while a session still references it is the expected aged-out case;
     replay still produces the preview, and `artifact_read` says it is gone. Export pinning (an
     exported session's blobs copied into the export and exempt from local GC) is deferred to the
     export slice and recorded as a limit, not silently promised.
- **Alternatives considered**:
  - *Inline everything and compress the log*: rejected — compression does not bound the model's
    context cost, and every later request would still carry the full bytes until a compaction.
  - *Store the full output in a sibling event or a separate `artifact` event kind*: rejected — a
    second event that no consumer defines would still be log bytes, would need ordering rules
    against its `tool/result`, and would become another kind in the catalog. The reference shape is
    additive and needs no new kind.
  - *Reference counting / a `session_artifacts` table*: rejected — authoritative state outside the
    event log (constraint 1) and not derivable after compaction. Aged-out blobs are cheaper than a
    second truth.
  - *Pin `artifact_read` in kian-core as a built-in tool*: rejected — core has no tool surface; the
    tool is a bundle-edge provider like every other capability (constraints 9-10).
  - *Let the loop spill instead of the append*: rejected — the loop is replaceable by a harness
    bridge, and a harness that never spills would persist megabytes while a native session does
    not. Spilling at the engine's append makes the property hold for every driver.
- **Consequences**: replay, export and read-model pages stay bounded; context carries a preview at
  hit-token price; the full payload is one audited tool call away. The blob tree is disposable: a
  wiped artifacts directory loses payload detail but never session truth or model-visible bytes.
  The cost is a second storage directory to operate and a new failure mode (aged-out blobs) that
  the tool must report honestly. `tool/result` consumers are unaffected: `evidence`, the outbox and
  the Claude bridge read `call_id`, `is_error` and small fields, never the full output.
- **Limits**: no export pinning yet (export copies the events; blobs stay local and may age out);
  no per-session artifact quota; no server-side fetch (P2b web tools are a bundle crate that spills
  through this same path); no artifact garbage collection during `serve` (CLI only in this slice);
  artifacts never enter `derive_model_context` as bytes — only the preview does.
- **Acceptance**: `ArtifactStore` unit tests (content-addressed put/get, id traversal rejection,
  retention+cap GC, crash-safe rename); append-path tests (small output inline, large output
  spilled with preview == first bound/4 bytes, `is_error` never spilled, absent config byte-identical,
  blob loss keeps context identical); `artifact_read` tests (window, clamp, missing blob);
  config tests (absent block keeps legacy bytes); `docs_map` stays green.
