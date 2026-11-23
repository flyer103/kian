# Kian — Read Model (normative)

> **This document is the normative contract for the serving form of a session's event stream.**
> It defines versions, deltas, resumable streams, snapshots, materialized views, retention and
> bounded-staleness reads. It **never defines truth**: the append-only event stream remains the
> sole truth and the only source of session state (`EVENT-MODEL.md`; AGENTS.md constraint 1).
> Wire shapes are normative in `API-CONTRACT.md`; execution mechanics are in `DESIGN.md`.
>
> Status: **accepted and fully landed**
> ([ADR-017](decisions/ADR-017-read-model-and-snapshots.md)). **P0** — version + unchanged/delta
> reads, resumable per-session streams, the drain context cache. **P1** — verified snapshots of state
> + model context with provenance, tail replay, the maintenance builder. **P2a** — materialized views:
> session directory, usage, ticket stage, cache, the kind index (incremental rebuild). **P2b** —
> opt-in export-only retention: the archive package, the two-phase verified commit,
> `session import`, the `-32007 archived` read error. **P3** — the explicit-allowlist fan-in
> event stream: `GET /events`, caps, kind filter, the `event_stream` metrics. §11 records the
> phase statuses and acceptance evidence.

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
  (Snapshots carry an explicit load-time trust boundary — §5.)
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

Landed shape (P3):

- **Tail-only start.** The connection establishes each listed session's cursor with ONE head
  query (`version`); the pre-connection history is never replayed and never scanned — the
  "no full-history polling" rule holds at connect time, not only per poll.
- **Poll loop.** Each cycle reads every watched session's bounded tail (`(last, head]` — the
  same `events_after` read §4.1 uses); per-poll work stays O(new events). A terminal session
  is delivered through its `session/ended` event (subject to the kind filter) and then dropped
  from the watch; the stream closes when every listed session has ended or errored. There is no
  `done` frame — that is a per-session concept.
- **Backpressure, concretely.** One cycle emits at most 512 frames before yielding (the fixed
  batch bound is normative in `API-CONTRACT` §5.4), and the
  poll loop is driven by the consumer's reads: a stalled consumer suspends polling outright, so
  the server never buffers unbounded data on its behalf (a slow consumer is flow-controlled,
  not a memory risk). An explicit idle-disconnect is not implemented in v0 — a stalled
  connection holds at most its ≤ 64 watched sessions and one bounded batch.
- **Config.** `api.event_stream.enabled` in `kian.yaml`, default **true**; disabled mounts no
  route (the path 404s). Explicit ids only — `*` is refused by name (no wildcard in OSS).
- **Metrics.** The `/metrics` `event_stream` object: `subscribers` / `sessions_watched` (live
  gauges, settled symmetrically by a connection-lifetime guard) and `events_sent` (counter).

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
- **Trust boundary.** Load-time verification is **builder attestation + digest↔columns
  integrity**, nothing more: the `verified` flag and the digest are written by the builder the
  table already trusts, so a row whose stored columns were rewritten *together with a recomputed
  digest* (a poisoned-but-self-consistent row) is **not detectable at load and is served** — the
  loader cannot distinguish it from an honest build. This is the documented behaviour
  (`self_consistent_poisoned_snapshot_is_served_builder_attestation_only`); the
  "corrupt/unverified/tampered snapshots are ignored" guarantee covers exactly the rows whose
  digest is stale or whose `verified` flag is clear. Consequences: `projection_snapshots` sits
  inside the same trust boundary as the `events` table itself (whoever can poison one can poison
  the other — there is nothing extra to gain); the table MUST never be copied between stores or
  restored from an untrusted dump; dropping and rebuilding it from the stream is always safe. No
  Merkle/proof mechanism is planned (§12 non-goal).
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
| `audit_kind_index` | event kind/ts | n/a | append-time (database-maintained) | drop/rebuild | none (never stale) |
| `idx_events_session_kind_seq` | event session/kind/seq columns | n/a | append-time (database-maintained) | recreated by the v6 migration; an out-of-band drop is **not** repaired (reads stay correct, slower) | none (never stale) |
| `session_cache` | `step/request` prefixes, `compact` | store head | maintenance rebuild | drop/rebuild | observability, ≤ B |
| `task_board` | events + inbox rows | session `head` + store key | maintenance rebuild | drop/rebuild | observability, ≤ B |

Landed shape (P2a):

- **Physical form.** `session_directory` / `session_usage` / `ticket_stage` / `session_cache`
  are logical views over one physical `view_rows` table (migration v4; the `view` column carries
  the declared name, `payload` the canonical JSON row). A per-view `view_state` row records the
  store key `(events_total, max_seq)` the rows were derived at plus a sha256 digest over the row
  set. The count axis is load-bearing: `seq` is per-session, so `MAX(seq)` alone would miss an
  append to a shorter session.
- **`audit_kind_index` is a SQL index**, not a view table: `kind`/`ts` are already projection
  columns of the truth table (`events`), so a plain index accelerates kind/time queries with
  zero duplication of derived data, is maintained by the database at append time (always fresh —
  no staleness class, no rebuild), and can never disagree with the stream. A view table would
  duplicate one row per event only to go stale.
- **P3b-1 (2026-11-08): the memory read paths ride the kind index.** A per-session
  `(session, kind, seq)` SQL index (`idx_events_session_kind_seq`, migration v6 — the
  `audit_kind_index` idea scoped per session) plus `EventStore::scan_kind(session, kind)`
  serves every memory read (creation-time injection and `kian memory
  list|show|search|map|verify`): a kind-filtered, index-ordered read of the truth table — not
  a view, not a snapshot. No new declared view, no freshness class; outputs byte-identical to
  the full-stream read it replaced.
- **Postgres migration cost (P3b-1, recorded):** the v6 `idx_events_session_kind_seq` is a
  blocking `CREATE INDEX` inside the migration transaction — on a large existing `events` table
  the first boot after upgrade holds a SHARE lock and pauses writes for the build (the same
  precedent as v4's `idx_events_kind_ts`; `CONCURRENTLY` is not available inside the migration
  transaction). Schedule upgrades accordingly.
- **`session_cache` was declared additively by the P2a change** (per §10's additive protocol):
  the read-model plan requires `kian cache-stats` to be served from views when fresh, and §6 had
  declared no view over the prefix-stability counters. Its source is the `step/request` audit
  copies (the A2 bytes) and `compact` events; it reports the same per-session counters
  `kian cache-stats` derives.
- **`task_board` was declared additively by the multiagent change** (MULTIAGENT-MODEL §4.2/§4.3):
  a logical view over the same v4 `view_rows`/`view_state` tables — one row per delegation **task**
  (keyed by the wrapper's `task_id`, `BTreeMap`-ordered), derived exactly like the honest full-stream
  derivation and published through the same one-transaction version flip under the `views:all` lease,
  with its own change trigger (`thread/started`, `thread/completed`, `evidence/verdict`,
  `session/ended`, `input/admitted` — separate from `ticket_stage`'s). Publication requires **both**
  `superagent.board.enabled` and `read_model.views.enabled`; a disabled board deletes any stored rows
  and state in the next rebuild (a disposable view — a later re-enable cannot serve a stale era).
- **`task_board` declared bound.** Event-derived columns are exact-version fresh; `input_required`
  (derived from the parent's durable inbox report row — the one non-event input, read through the
  store's read-only `input_rows`) is as of the last rebuild: `B = read_model.views.interval_ms`.
- **Freshness gate.** A view is served only at exactly the store key it was derived at (any
  append makes it stale — not a TTL), only when the digest over the read rows matches the stored
  digest, and only when every payload decodes. Stale, corrupt, undecodable, absent (a read-only
  reader of a pre-v4 store) or disabled (`read_model.views.enabled: false`) → the honest full
  stream derivation.
- **`ticket_stage` declared bound.** Event-derived fields are exact-version fresh;
  `pending_inbox` (an inbox row can become eligible with no append, via `eligible_at`) and the
  stage's dependence on it are as of the last rebuild — the declared staleness bound `B` (the
  rebuild cadence). The rendered board is therefore ≤ one rebuild interval old on those fields.
- **Publication** is one transaction per view (rows + freshness key + digest together): a crash
  leaves the old complete version or the new one, never half a view. A view *replaces* its
  predecessor — unlike snapshots (R6 insert-only) this is safe because the whole version flips
  atomically and no reader may treat a view as truth. Rebuilds run under the `views:all`
  maintenance lease (liveness only), the store key pinned before any stream is read.
- **Incremental rebuild.** The maintenance tick is incremental: a session whose head is
  unchanged since the last publication is *reused*, not re-derived — a stored row whose
  `row_version` equals the live head was derived from an identical stream (streams are
  append-only under CAS guards, so the head determines the stream). An unchanged store is a
  no-op tick (one index-only heads scan, one pending-inbox scan, four digest verifications,
  nothing published); per-tick fold work is proportional to the *changed* sessions. Publication
  is the same one-transaction version flip, expressed as a delta (upserts for changed sessions,
  deletes for gone sessions, state flip to the merged set's digest). The `ticket_stage` view is
  all-or-nothing: its global folds read board-kind events across every session and
  `pending_inbox` moves with `eligible_at` alone, so a board-kind change, a session deletion or
  a pending drift re-derives the whole board — from a kind-filtered events read plus the worker
  streams, never from every session's full stream. Any doubt about the stored views (absent
  state, digest mismatch, undecodable payload) falls back to the full rebuild — the self-heal a
  per-tick full rebuild used to give. Read semantics are unchanged: the merged row set is
  byte-identical to a full rebuild's, pinned to the same store key.

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
- **Read behavior.** Reads whose range touches the online rows of an archived session return the
  explicit `archived` error (`API-CONTRACT` code). `session inspect` reports archived plus the
  package path; `session export` reads the package and MUST be byte-identical; `session replay`
  refuses online replay with import guidance. The per-session aggregate reads
  (`usage-stats --session`, `cache-stats --session`) refuse with the archived marker and import
  guidance — never a silent zero report (R7). Whole-store aggregates report the online
  window only. (Scope note: the memory surfaces — `kian memory list/show/verify --session` — do
  not consult the archive marker; an archived id answers the same explicit `unknown session` as
  any absent one. The `archived` marker surfaces exactly on the read paths enumerated above,
  where the package path changes the guidance.)
- **Import.** `kian session import <package>` verifies the manifest, then restores the session
  into a store for replay/forensics; a corrupt package, a package whose rows name a different
  session than its manifest (package identity), or an existing session id is refused.
- **Invariant amendment.** The inbox is audit-visible via the online store **or a verified
  archive package** — superseding the current "audit-visible forever in the store" wording
  (`crates/kian-store/src/inbox.rs`, `migrations.rs` comments) in the same change.
- **Audit invariance.** Exporting from `(online store + archive packages)` equals the
  pre-archive export byte-for-byte.

Landed shape (P2b):

- **Config.** `retention:` in `kian.yaml` — `enabled` (default **false**, strictly opt-in),
  `archive_dir` (required when enabled), `min_age_ms` and/or `min_bytes` (at least one required
  when enabled; both configured ⇒ both must be met — the conservative window). Fail-closed
  parse: an enabled-but-vague retention refuses to start.
- **Marker.** Migration v5 `archived_sessions` (one row per archived session: range, counts,
  package path, per-file + manifest sha256, archived_at). Infrastructure, never truth; a
  read-only reader of a pre-v5 store reads it as "not archived".
- **Package.** `<archive_dir>/<session>/events.jsonl + inputs.jsonl + manifest.json`
  (`format_version: 1`). `events.jsonl` is byte-identical to `session export`'s default output
  by construction (shared serializer); `inputs.jsonl` carries the full inbox rows including
  `consumed_by_seq`/`admitted_seq`; the manifest carries the §7 identity fields plus the
  aggregate sha256 over the digest pair. Written via a temp directory + one atomic rename.
- **Two-phase commit.** `kian archive run` scans terminal candidates (`session/ended` is the
  last committed event), then per session: write (idempotent — an existing verified package is
  kept) → verify (hashes recomputed, every line re-parsed, counts/seq bounds cross-checked,
  byte-parity against a fresh stream derivation) → `archive_commit` (marker CAS + online-row
  delete in ONE transaction, fenced on the online event count the package was verified
  against — a resumed session aborts `OnlineMoved` and nothing is deleted). The events/inputs
  delete exists in exactly one guarded site (`kian-store`'s archive module; the crate's
  append-only review assertion carves out that file alone).
- **Reads.** `projection` and the SSE stream answer `-32007 archived` (API-CONTRACT §2.2;
  the planned `-32005` had been taken by the memory registry) with package guidance.
  `session inspect` reports archived + the package path; `session export` serves the package
  byte-identically (hashes re-verified on the way out); `session replay` refuses online replay
  with import guidance; the per-session aggregate reads (`usage-stats --session`,
  `cache-stats --session`) refuse the same way (never a silent zero report).
- **Import.** `kian session import <package>` verifies the manifest, refuses a corrupt package,
  a package whose rows name a different session than its manifest (every event and input row's
  session is cross-checked against `manifest.session`; the manifest is not self-hashed, so
  identity is a first-class check), or an existing session id, and restores the events (under
  their own epoch/seq) and the full input rows — exactly the rows verification replayed, never a
  re-read — into the target store; the restored stream projects and re-exports identically.
- **Cadence.** No automatic retention loop: `kian archive run` is an explicit operator pass.
  Wall-clock scheduling — retention cadence, dream cadence and every other cadence — is owned by
  [CRON-MODEL.md](CRON-MODEL.md) (ADR-033), which supersedes the earlier "cadence out of scope"
  statement; this document only defines the serving form.

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
- **Per-tick cost is proportional to the change, not to total history.** The view
  rebuilder's whole-store reads are index-only aggregates (session heads, pending-inbox counts);
  streams are re-read and re-folded only for sessions whose head moved, and the folds yield to
  the runtime between sessions. The snapshot builder's candidate scan stays an index-only
  `GROUP BY` bounded by its `LIMIT`.

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
| P0 | version + unchanged/delta reads, resumable SSE, drain context cache, bench | landed | L1 unit; L2 e2e (resume, unchanged); L3 chaos unchanged; bench: unchanged read = 1 head query, SSE poll = O(Δ) |
| P1 | snapshots (state + context with provenance), verified publish, tail replay | landed | L1 equivalence incl. compact; L2 resume/crash; bench: 10k-event resume = O(tail); request bytes unchanged |
| P2a | session directory, usage/stage views, kind index | landed | L1 rebuild equivalence; L2 operator surfaces on 1k sessions; corrupt/stale fallback |
| P2b | opt-in export-only archive, import, retention | landed | L2 archive→verify→delete→inspect; export byte parity; corrupt package refused; crash windows |
| P3 | explicit-allowlist fan-in stream, caps, filters, metrics | landed | L2 two-session fan-in; filters/caps/unknown-id; reconnect per-session unaffected; no wildcard |

## 12. Non-goals

- No consensus layer / Ref-CAS-like service; per-session DB CAS + fencing remain the ordering
  mechanism.
- No object-store split of the event stream.
- No multi-region active-active; bounded-staleness read replicas only (hosted).
- No transparent cold-tier query: archive is export/import, not a second online tier.
- No global ordering across sessions; fan-in is tail-only in P3.
- No wildcard cross-session stream in OSS.
- No new event kinds for serving structures; no mutable history.
