# Kian — Read Model (proposed)

> **This document is the normative contract for the serving form of a session's event stream.**
> It defines versions, deltas, resumable streams, snapshots, materialized views, retention and
> bounded-staleness reads. It **never defines truth**: the append-only event stream remains the
> sole truth and the only source of session state (`EVENT-MODEL.md`; AGENTS.md constraint 1).
> Wire shapes are normative in `API-CONTRACT.md`; execution mechanics are in `DESIGN.md`.
>
> Status: **proposed**. Nothing in this document is landed yet. The phase table in §11 records
> the agreed target; each phase flips to `landed` in the change that implements it and updates
> this header.

## 0. Scope and relationship to the other contracts

This contract owns:

- what a read version means and how unchanged/delta reads behave (§3);
- resumable per-session streams and multi-session fan-in streams (§4);
- the snapshot lifecycle for state and model context (§5);
- materialized-view/read-accelerator rules (§6);
- retention and archival of terminal sessions (§7);
- bounded staleness and degradation (§8);
- the maintenance-mutation contract (§9).

It does not own:

- event envelope, kinds, payloads, or the event-change protocol — `EVENT-MODEL.md`;
- method names, request/response fields, error codes, SSE frame names, metrics keys —
  `API-CONTRACT.md`;
- the drain/recovery/execution model — `DESIGN.md`;
- memory note semantics and representations — `MEMORY-MODEL.md` (this contract only governs
  how a representation is served as a view);
- product positioning and scope — `../docs/SPEC.md`.

The decision record for this contract is
[ADR-017](decisions/ADR-017-read-model-and-snapshots.md).

## 1. Terms

- **Truth**: the append-only per-session event stream; the only source of session state.
- **Version `V(s)`**: the `seq` of the last committed event of session `s` (`head.next_seq - 1`;
  `0` for a session with no events). Versions are monotonic per session; a terminal stream's
  version is frozen.
- **Epoch**: drain fencing epoch. Orthogonal to version; a takeover does not change version
  semantics.
- **Projection**: a pure function of the event stream (`SessionState`, `derive_model_context`).
- **Snapshot**: a persisted projection (state and, optionally, model context with provenance)
  at a version, verified against the stream. A disposable cache, never truth.
- **View / read accelerator**: any persisted derived data that is not truth (indexes,
  aggregates, directories, stage boards).
- **Read class**: a **control read** participates in a decision (append guards, claim,
  promotion, steer eligibility, state transitions); an **observability read** does not
  (inspection, streams, boards).
- **Online window**: the event and input rows served from the primary online store.
- **Archive package**: the verified export artifact of a terminal session (events + inputs +
  manifest) after it leaves the online store.
- **Resync**: the explicit outcome when a requested range is unavailable (archived, unknown,
  ahead of head). Never a silent truncation.

## 2. Invariants (normative)

- **R1 Truth separation.** No read path may treat a snapshot, view or cache as authority. Every
  response MUST be derivable from the event stream; if derivation or verification fails, the
  response MUST fail explicitly — never serve unverified derived data as if it were truth.
- **R2 Version.** A response's version MUST identify a committed prefix of the stream. A version
  is never reused or allowed to decrease per session. Versions MUST NOT be mixed: the payload
  returned for version `V` is derived from exactly the events `1..V`.
- **R3 Unchanged fast path.** A read that presents a version MUST be answerable without payload
  when that version is current: the response is `unchanged`, carries no state/events, and costs
  no stream scan beyond the version check.
- **R4 Delta completeness.** A delta for `(from, to]` MUST contain exactly the events with
  `from < seq <= to`, ordered by `seq`, with no gaps. If `from` is not satisfiable (ahead of
  head, archived, unknown), the response MUST be an explicit resync/archived error, never a
  truncated or silently restarted stream.
- **R5 Single invalidation point.** Derived context caches/snapshots are invalidated only by a
  `compact` event; any other difference in derived bytes is a bug (constraint 3). The
  `unexpected_prefix_change` metric keeps its meaning.
- **R6 Snapshot publication.** A snapshot MUST be verified equivalent to the event range it
  replaces before it becomes visible. Publication MUST be insert-only (no overwrite of a
  previously visible snapshot version) and MUST NOT delete or rewrite events. Crash windows may
  leave orphan snapshots; they may never leave a visible inconsistent snapshot.
- **R7 Retention and audit invariance.** Archival is not deletion of truth: data leaves the
  online store only after a verified archive package is durable. The archive package MUST
  reproduce the audit export byte-identically. Any read whose range touches archived data MUST
  report `archived` explicitly. Importing an archive package MUST be possible for replay and
  forensics.
- **R8 Bounded staleness.** Reads used for control decisions MUST read the primary. Observability
  reads MAY be served from a replica/cache within a declared staleness bound `B`; the surface
  MUST document that it may be stale. No staleness bound may be applied to append guards, claim,
  promotion, or steer eligibility.
- **R9 Maintenance idempotency.** Maintenance mutations MUST be idempotent under retry. Leases
  are liveness only; safety comes from the store CAS/fencing. Snapshot builders, view rebuilders
  and the archive job MUST be safe to run concurrently across replicas without violating
  R1/R6/R7.

## 3. Versions and deltas (P0)

- **Version source.** `version = head.next_seq - 1` (the last committed `seq`). `projection`
  responses gain `version`; an optional request field `since` carries the caller's last version.
- **Unchanged.** When `since == version`, the result is `{"version":V,"unchanged":true}` and
  contains no `state`. When `since` is absent or differs, the result is
  `{"version":V,"state":<projection state>}`. State deltas are out of scope: the state value is
  small and returned whole.
- **Consistency.** The server MUST derive the payload from a single committed prefix: read the
  head, derive events `1..V`, return `V`. A concurrent append may make the answer stale, never
  mixed. `since > version` is `invalid_params` (client ahead or pointed at another store).
- **Event deltas.** Readers that follow events (SSE, CLI tails, fan-in) use
  `(from, head]` scans; per-poll work MUST be O(new events), never O(history).
- **Compaction.** A `compact` event in a delta is applied normally; context consumers recompute
  the affected range (R5).

## 4. Resumable streams (P0, P3)

### 4.1 Per-session stream (P0)

- Each `event: kian` SSE frame carries `id: <seq>`. A reconnect with `Last-Event-ID: <seq>`
  resumes at `seq + 1`; a connection without the header starts at the beginning.
- A resume id that is ahead of the durable head, not a number, or inside an archived range is an
  explicit `event: error` followed by close. A terminal stream replays its remaining events and
  closes with `done`.
- Per-connection frames carry strictly increasing `seq`. Re-delivery of an already-emitted `seq`
  on the same connection is a contract violation; a resumed connection MUST NOT re-emit the
  events before its resume point.

### 4.2 Fan-in stream (P3, OSS)

- `sessions` is an explicit allowlist (at most 64 UUIDs per connection). There is **no wildcard**
  in OSS; a principal-scoped wildcard is a hosted-layer addition under API-CONTRACT's additive
  rule.
- P3 v0 is **tail-only** (no resume header); per-session resume remains §4.1. Each frame carries
  `id: <session>:<seq>`; no ordering across sessions is implied or promised.
- `kinds` is an optional allowlist. A malformed or over-cap request is HTTP 400. An unknown
  listed session is one `event: error` frame naming it; the stream continues with the others.
- Backpressure is bounded; slow consumers are disconnected rather than causing unbounded buffering.

## 5. Snapshots (P1)

- **Storage.** Snapshots are infrastructure rows (like `claims`/`inputs`/`exec_queue`), not
  events. No new event kind is added. The table may be dropped and rebuilt at any time without
  changing the stream.
- **Content.** `(session, version, state, context, context_provenance, digest, verified)`.
  `context` is `derive_model_context`'s output plus the per-message provenance needed to apply a
  later `compact`; the serialized model request bytes remain owned by `kian-core` and MUST be
  unchanged (constraint 3; witness test).
- **Build.** Snapshots are built outside a drain, under a maintenance lease, from a consistent
  read of `1..V`. Publication requires verification: a second independent derivation compares
  the digest of `(state, context, provenance)`. Only verified rows are visible to readers.
- **Load.** A reader picks the highest verified `V <= head`, replays `(V, head]`, and uses the
  result. If verification metadata is missing, replay fails, or the tail exceeds the configured
  cap, the reader MUST rebuild from the stream (honest fallback) — never serve a partially
  applied snapshot.
- **Crash and rollback.** Build-verify-publish is insert-only; orphans are GC-able. Disabling the
  builder (config) makes all snapshots invisible; the stream is unaffected.

## 6. Materialized views and read accelerators (P2)

A view is declared here before it is built. Every entry records: source, version key, build
mechanism, disposal, staleness class, and whether a control read may consume it (initial set:
none may).

| View | Source | Version key | Build | Disposal | Staleness |
|---|---|---|---|---|---|
| `session_directory` | events | session `head` | maintenance rebuild | drop/rebuild | observability, ≤ B |
| `session_usage` | `turn/end` usage | store head | maintenance rebuild | drop/rebuild | observability, ≤ B |
| `ticket_stage` | `thread/started`, outbox, directory | store head | maintenance rebuild | drop/rebuild | observability, ≤ B |
| `audit_kind_index` | event kind/ts | store head | append-time or rebuild | drop/rebuild | observability |

Rules:

- Views are disposable; a corrupt or stale view MUST be discarded/rebuilt, never repaired by
  guessing.
- A view read MUST report (or check) its source version; a request that needs a newer version
  MUST trigger rebuild or fall back to stream derivation.
- No control read may consume a view (R1/R8). Operator surfaces may serve a view within its
  declared bound.
- Maintenance rebuilds run under a lease; a view is visible only when complete for its version.

## 7. Retention and archival (P2)

- **Opt-in, default off.** With retention disabled, behavior is byte-identical to today.
- **Eligibility: terminal sessions only** (`completed`, `cancelled`, `archived`). Non-terminal
  sessions are never archived, so resume semantics are untouched.
- **Online window** is configured by age and/or bytes; the maintenance job decides eligibility
  and always leaves a verified archive package behind.
- **Package.** Export-format `events.jsonl` + `inputs.jsonl` + `manifest.json`
  (`format_version`, session, event-schema version, first/last seq, event count, input count,
  per-file and aggregate sha256, created_at, store backend).
- **Two-phase, idempotent.** Write package → verify (hashes, replay, byte-identical export) →
  insert the archived marker under CAS → delete online rows in one transaction. A failure at any
  point leaves the online rows intact; retries are idempotent. Delete is never attempted before a
  verified package exists.
- **Read behavior.** Any read of an archived session returns the explicit `archived` error
  (`API-CONTRACT` code). `session inspect` reports archived plus the package path;
  `session export` reads the package and MUST be byte-identical; `session replay` refuses online
  replay with import guidance.
- **Import.** `kian session import <package>` verifies the manifest, then restores the session
  into a store for replay/forensics; a corrupt package or an existing session id is refused.
- **Invariant amendment.** The inbox is audit-visible via the online store **or a verified
  archive package** — superseding the current "audit-visible forever in the store" wording
  (`crates/kian-store/src/inbox.rs`, `migrations.rs` comments) in the same change.
- **Audit invariance.** Exporting from `(online store + archive packages)` equals the
  pre-archive export byte-for-byte.

## 8. Bounded staleness and degradation (P2/P3)

- **Read classes.** Control reads read the primary. Observability reads may use a read replica
  and declare a bound `B` (hosted layer; OSS single-store reads are current).
- **Degradation matrix.** Store unavailable → error, never stale-as-truth. Replica lag > B →
  report stale/refuse. Snapshot corrupt/unverified → rebuild from stream. View corrupt → discard.
  Archive package missing/unverifiable → the session is not archived online and the read errors. Maintenance backlog → reads fall back to stream derivation (bounded by a configured cap, then error).
- **No silent fallback that changes semantics.** Falling back from a view/snapshot to the stream
  is always allowed (it is the truth path); falling back from the stream to a cache is not.

## 9. Maintenance-mutation contract (P1/P2)

- Maintenance (snapshot builder, view rebuilder, archive job) runs outside drains, in the same
  binary under a configurable role, or as a separate process; it is never part of a drain's
  critical path.
- It takes a per-session or per-shard lease (liveness only) and guards every write with the
  store's CAS/fencing (safety). Every mutation is idempotent on retry.
- It may write only derived structures (snapshots, views) and, under §7, delete online rows
  after a verified package exists. It never rewrites events or changes event bytes.
- It reports counters on `/metrics` (planned additive namespace `read` / `event_stream`;
  API-CONTRACT owns the exact keys).

## 10. Change protocol

- **Additive:** a new view, a new optional response field, a new bounded query parameter, a new
  metrics field. Ships with this document and tests.
- **Behavior-changing:** version semantics, unchanged soundness, snapshot format/verification,
  staleness bound, archive format/online window. Requires an ADR update, equivalence tests and a
  benchmark, and a rollback plan.
- **Incompatible:** removing a field/SSE frame, changing archive format without a reader,
  changing the trust boundary. Requires a version bump and a dual-read window.
- No change may move authoritative state out of the event stream (AGENTS.md constraint 1).

## 11. Phase status and acceptance

| Phase | Scope | Status | Acceptance (L-level) |
|---|---|---|---|
| P0 | version + unchanged/delta reads, resumable SSE, drain context cache, bench | planned | L1 unit; L2 e2e (resume, unchanged); L3 chaos unchanged; bench: unchanged read = 1 head query, SSE poll = O(Δ) |
| P1 | snapshots (state + context with provenance), verified publish, tail replay | planned | L1 equivalence incl. compact; L2 resume/crash; bench: 10k-event resume = O(tail); request bytes unchanged |
| P2a | session directory, usage/stage views, kind index | planned | L1 rebuild equivalence; L2 operator surfaces on 1k sessions; corrupt/stale fallback |
| P2b | opt-in export-only archive, import, retention | planned | L2 archive→verify→delete→inspect; export byte parity; corrupt package refused; crash windows |
| P3 | explicit-allowlist fan-in stream, caps, filters, metrics | planned | L2 two-session fan-in; filters/caps/unknown-id; reconnect per-session unaffected; no wildcard |

## 12. Non-goals

- No consensus layer / Ref-CAS-like service; per-session DB CAS + fencing remain the ordering
  mechanism.
- No object-store split of the event stream.
- No multi-region active-active; bounded-staleness read replicas only (hosted).
- No transparent cold-tier query: archive is export/import, not a second online tier.
- No global ordering across sessions; fan-in is tail-only in P3.
- No wildcard cross-session stream in OSS.
- No new event kinds for serving structures; no mutable history.
