# ADR-058: Artifact GC runs as a serve-side ticker, and export pins blobs beside the package

- **Status**: accepted (owner direction 2026-11-22, approved semantics 3; P3b slice).
- **Context**: phase 2 landed the content-addressed artifact store, the append-path spill and the
  manual `kian artifacts gc` command (ADR-055). Two operator gaps remained and were written down
  as non-goals at the time: serve never armed a garbage collector, so a long-running server grew
  the blob tree until an operator remembered the CLI; and `session export` copied the events (which
  carry only the reference and the preview), so an exported audit package silently lost fidelity
  as soon as the local blobs aged out. Both gaps are operational, not semantic: the event stream stays
  the sole truth, a lost blob is a normal aged-out outcome, and neither feature may become a second
  authority (no reference counts, no per-session quota, no pinned-blob manifest inside the event
  log). The approved direction requires the GC to ride the maintenance ticker and the export to be
  able to pin what it references, without inventing new invariants.
- **Decision**:
  1. **The serve GC ticker arms only from configuration.** When the `artifacts:` block exists, the
     server spawns one GC loop beside the snapshot and view tickers. The loop reads
     `artifacts.gc.interval_ms` (default one hour), `max_bytes` and `retention_days`, and calls the
     same `ArtifactStore::gc` the CLI calls. No block, no ticker: an operator who never configured
     spill has no background scanner and no hidden directory policy. This mirrors the mount rule of
     `artifact_read`: absent configuration leaves behavior byte-identical to legacy.
  2. **GC stays retention-only, outside drains, and non-fatal.** The policy is unchanged: strict
     mtime retention first, then oldest-first cap enforcement, with zero meaning "dimension off"
     and both dimensions off rejected at boot. The ticker runs in its own task outside every drain,
     logs a single info line when it removes anything and a warning on failure, and never fails a
     request. A serving process that loses a GC round is degraded in disk hygiene only.
  3. **One-shot GC remains the operator escape hatch.** `kian artifacts gc` stays the manual
     command with the same bounds and `--json` output. The ticker does not replace it: operators
     can run a reclaim ahead of the schedule, and the ticker has no effect on a stopped server.
  4. **Export pins on request, never by default.** `kian session export --with-artifacts` requires
     `--out` (pinning needs a destination file to sit beside) and copies every blob referenced by
     the selected `tool/result` events into a sibling directory named `<out>.artifacts/<id>`. The
     copy is content-addressed, read-only in spirit (the store still owns the canonical bytes), and
     leaves the event bytes untouched: the export remains a replay of the stream, not a new format.
  5. **Missing blobs are reported honestly and do not fail the export.** An already-reclaimed blob is
     the documented aged-out case. The export succeeds, the CLI summary reports pinned and missing
     counts, and the missing ids stay named. Failing the export would make an audit package's
     success depend on a disposable cache; silently omitting the count would hide the loss.
  6. **Archived packages get the same flag for parity.** Export from an archived package resolves
     artifact ids from the package rows and pins beside the new output, so the two export paths keep
     one contract. Pinning from a package whose blobs are already gone reports them missing like any
     other case.
  7. **Pinned directories are operator artifacts, not a cache tier.** The sibling directory is never
     automatically garbage-collected, imported, or read by the runtime. Copying it with the export is
     the operator's audit decision; deleting it reclaims disk and loses nothing the event log needs.
- **Alternatives considered**:
  - A GC ticker inside `drain` (the episode loop): rejected. GC is not a session activity and must
    not consume a session's drain, budget or lease; a slow filesystem scan would stretch turns.
  - Reference counting, or a pin set in the event log: rejected for the reasons ADR-055 already
    records (no session enumeration authority, compaction rewrites ranges, constraint 1).
  - Exporting blobs inline into a single archive by default: rejected. It changes the export format
    and grows every package by payload size; opt-in pinning keeps the default package events-only.
  - Failing the export when a blob is missing: rejected, it couples audit success to cache retention.
- **Consequences**:
  - A long-running server reclaims its own blob tree without operator cron; a disk guard exists by
    default wherever spill is enabled.
  - Audit exports can be made self-sufficient: pinned directories travel with the JSONL and preserve
    full payload fidelity for results that still exist locally.
  - The event contract does not change: no new kind, no payload key, no projection rule. Replay with
    or without pinned blobs stays identical.
  - Two operator surfaces now share one GC policy and one pin summary, which keeps the manual and
    automatic paths from drifting.
- **Limits (recorded, not hidden)**:
  - The GC interval is a fixed duration, not a cron expression; it is not part of the scheduling model.
  - Pinning is a best-effort copy at export time; it cannot recover a blob the ticker already removed.
  - A pinned directory is not tracked, deduplicated against the store, or validated on later reads;
    the runtime never reads it.
  - There is still no per-session quota and no reference counting.
- **Acceptance**: config tests that the GC interval defaults to one hour, validates positive, and is
  rejected at boot when zero; store tests that the same `gc` call the ticker makes still enforces
  retention and cap; CLI tests that referenced ids are collected only from spill-shaped results,
  that pinning copies present blobs and reports missing ones without failing, and that
  `--with-artifacts` without `--out` is refused before any store access; existing export byte
  parity tests unchanged. All gates green.
