# ADR-017: The read model — versions, snapshots, and export-only retention

- **Status**: Accepted (owner gate, 2026-11-07)
- **Date**: 2026-11-07
- **Scope**: core, storage, CLI/API serving surfaces, docs
- **Supersedes**: none
- **Related**: AGENTS.md constraints 1/3/5; `design/EVENT-MODEL.md`; `design/API-CONTRACT.md`;
  `design/DESIGN.md`; `design/MEMORY-MODEL.md`; `design/READ-MODEL.md` (the contract this
  decision establishes)

## Context

Kian's sole truth is the append-only event stream; every read derives from it. That is the right
truth model, but it currently makes the **serving form** pay for history everywhere:

- `projection` rebuilds and folds the whole stream per call (`engine.projection` →
  `EventStore::rebuild` → `scan(0..MAX)`).
- The SSE endpoint re-reads and filters the whole stream on every poll interval, per subscriber
  (100 ms default), and the contract explicitly declares it non-resumable.
- The built-in loop re-reads the whole stream and re-derives the model context on every
  iteration; with `step/request` persisting the full request body each turn, history bytes are
  superlinear and every resume pays for all of them.

The owner has committed to all four read-model phases (P0 versions/deltas/resumable streams,
P1 snapshots, P2 materialized views + retention, P3 fan-in streams). Two decisions were required
before a normative contract could be written:

1. **P3 authorization**: v0 has no authentication/authorization (API-CONTRACT §2.4). A
   cross-session stream could become a session-enumeration surface.
2. **Retention**: archived sessions must remain auditable (constraint 1), but the online store
   cannot grow without bound.

## Decision

1. **Establish a separate normative contract**, `design/READ-MODEL.md`, for the serving form.
   It never defines truth; wire shapes stay in API-CONTRACT, mechanics in DESIGN, rationale in
   ADRs. A fourth `-MODEL` contract is justified because versions, deltas, resumable streams,
   snapshots, views, retention and staleness otherwise get restated in EVENT-MODEL,
   API-CONTRACT, DESIGN and MEMORY-MODEL with no single owner.
2. **Version = `head.next_seq - 1`** (last committed seq per session), orthogonal to the drain
   epoch. `projection` gains `version`; an optional `since` yields an `unchanged` result with no
   payload when current. Event readers use `(from, head]` deltas; per-poll work is O(new events).
3. **Resumable per-session SSE** via `id: <seq>` + `Last-Event-ID`; a resume id that is ahead of
   head or archived is an explicit error, never a silent restart/truncation.
4. **Snapshots are verified, disposable caches**, stored as infrastructure rows (not events):
   insert-only publication after a second derivation verifies `(state, context, provenance)`
   against `1..V`; readers load the highest verified `V <= head`, replay the tail, and fall back
   to the stream on any doubt. Model-context snapshots carry per-message provenance so a later
   `compact` can be applied; request bytes remain owned by `kian-core` (constraint 3, witness
   test).
5. **Views are declared, disposable and never control-read**: `session_directory`,
   `session_usage`, `ticket_stage`, `audit_kind_index`; each is versioned by its source head,
   rebuildable, and may be served only to observability reads within a declared staleness bound.
6. **Retention is opt-in and export-only, terminal sessions only**: write a verified archive
   package (`events.jsonl` + `inputs.jsonl` + manifest with hashes), mark archived under CAS,
   then delete online rows in one transaction; never delete before verification. Reads of
   archived data return an explicit `archived` error; `session export` reads the package
   byte-identically; `session import` restores for replay/forensics. The "inbox audit-visible
   forever in the store" wording is amended to "online store or verified archive package".
7. **P3 fan-in stream uses an explicit session allowlist** (≤64, no wildcard in OSS), tail-only;
   per-session resume remains P0. A principal-scoped wildcard is a hosted-layer additive
   extension.
8. **No new event kinds, no consensus layer, no object-store split, no multi-region
   active-active, no transparent cold-tier query.** Ordering remains per-session DB CAS +
   fencing; the event stream stays the only truth.

## Alternatives considered

| Alternative | Rejected because |
|---|---|
| Fold the read model into API-CONTRACT/DESIGN | Wire vs semantics vs mechanics; three docs would own one semantic and drift (AGENTS.md "single truth"). |
| Persist snapshots as events | Pollutes the truth stream and the audit export; grows the event-kind catalog for a disposable cache; infrastructure state already has a home (`claims`/`inputs`/`exec_queue`). |
| Make snapshots authoritative session state | Violates constraint 1; rebuildability is the source of crash safety. |
| Poll the full stream and cache in the client | Per-subscriber O(history) CPU/DB on the server; no resumability; scales with subscriber count. |
| Transparent cold-tier query for archived data | New backend, latency SLO, consistency and ops surface; contradicts the single-binary self-host posture. |
| Wildcard cross-session stream in OSS | No authz in v0; becomes session enumeration. |
| Agent-scope tokens in OSS | Pulls authentication/token lifecycle/multi-tenancy into the open-source core; hosted concern. |
| No retention | Unbounded store growth and replay/restore cost; SRE/compliance gap. |
| Export/import as the only archive path (chosen) | Keeps audit intact with no new online tier; archived sessions are recoverable for replay. |

## Consequences

**Positive**

- Read/watch cost becomes independent of history length for unchanged sessions and O(delta) for
  resumed streams; long-horizon sessions stay cheap to inspect and follow.
- Resume can start from a verified snapshot + tail instead of full replay, while the full stream
  remains the audit record.
- Operator surfaces and (hosted) replicas can serve views/stale reads without touching control
  decisions.
- Retention becomes an explicit, verifiable operation with byte-identical export guarantees.
- P3 gets a useful integration surface without weakening the v0 trust boundary.

**Negative / risks**

- Two representations (stream + snapshot/view) can drift. Mitigation: R1/R6 verification,
  insert-only publication with a second derivation, fallback to the stream on any doubt, and
  tests that compare snapshot+tail against full replay.
- Snapshot context with provenance touches the model-context derivation internals. Mitigation:
  provenance is metadata never serialized into request bytes; a witness test pins request bytes
  and the cache-stability report.
- Retention changes an existing "audit-visible forever" wording; the amendment must land in the
  same change as the archive implementation.
- `docs_map` and API-contract planned sections are documentation debt until each phase lands;
  each phase flips its own status and moves the wire surface from planned to normative.

## Verification

Each phase carries its own acceptance in `design/READ-MODEL.md` §11 (L1/L2/L3 + benchmark). The
four repository gates plus `cargo test -p kian-cli --test docs_map` are required on every phase.
The review card must independently verify R1–R9 and constraint 3 before closeout.

## References

- `design/READ-MODEL.md` — the contract this decision establishes.
- `design/EVENT-MODEL.md` — truth model and `compact` semantics.
- `design/API-CONTRACT.md` — wire shapes (planned section until each phase lands).
- GitHub, "Inside GitHub's new Git storage architecture" (2026-11-05) — the external input;
  adopted principles: immutable truth vs minimal
  ordered state, version-check fast path, verified compaction, maintenance off the serving path;
  rejected: consensus layer, object-store split, transparent cold tier.
