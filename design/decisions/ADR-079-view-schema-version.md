# ADR-079 — A view's stored rows must prove their schema, not only their digest

- Status: accepted (2026-11-28, read-model self-heal)
- Related: ADR-076 (liveness is a probe — the incident this closes), ADR-041 (a claim chain
  without an end is closed as failed)

## Context

Materialized views are disposable accelerators: `session_directory`, `session_usage`,
`session_cache`, `ticket_stage` and `task_board` serve operator surfaces while the event stream
stays the sole truth (`design/READ-MODEL.md` §6). The maintenance tick is incremental. A stored
row is reused, not re-derived, when its `row_version` equals the session's live head — and the
head is the only reuse key, because streams are append-only under CAS guards
(`crates/kian-cli/src/views.rs`, `incremental_under_lease`). The tick's reuse base was verified
by one test only: the digest recomputed over the stored rows had to equal the stored digest.

That test is blind to the payload's *shape*. `session_usage` rows are `UsageEntry` values
(`crates/kian-cli/src/views.rs:126`). The type gained `cache_miss_input_tokens`; rows written
before it do not decode. On 2026-11-28 the production view held 343 rows, 205 of them written
under the older shape. Every read discarded the whole view (`view payload failed to decode —
discarding the view view="session_usage"`, 569 times since 2026-11-27 13:07) and fell back to
the honest full derivation, a scan over all 343 sessions (`crates/kian-cli/src/usage_metrics.rs`,
full-scan fallback). `/metrics` renders that report, so each render took 6–7 s; the supervisor's
5 s probe then marked a healthy process dead and restarted it (ADR-076's incident).

The writer never repaired it, and could not: the bytes were internally consistent, so the digest
matched, so the rows were reused forever. The reader's correctness rule ("only when every payload
decodes") and the writer's reuse rule ("digest matches") disagreed, and the disagreement is
structural — it survives any digest. Nothing in the stored version identified the row-payload
schema either, so a shape change was invisible to the reuse base even when someone did remember
it. The documented cure was manual: drop `view_state` and pay a full rebuild (~21–25 min of store
reads on the production database).

## Decision

1. `kian_store::VIEW_SCHEMA_VERSION` (a `u32`, currently `1`) is part of the digest domain.
   `view_rows_digest` hashes the domain label, the version, the view name and the rows; a bump
   therefore invalidates every stored view at once (digest mismatch → full rebuild). Views are
   disposable, so discarding them wholesale is the only safe answer to a shape change.
2. The maintenance tick's reuse base proves decodability, not merely consistency:
   `stored_view_decodes::<T>` reads a view's rows, verifies the digest over exactly those bytes,
   and decodes every payload as the served entry type. Any doubt — absent state, a digest
   mismatch (a schema-version bump included), an undecodable payload, a store error — drops the
   tick to the full rebuild. A row the reader would discard can never anchor a reuse base.
3. Each doubt is logged once per view per tick: `stored view digest mismatch — full rebuild` or
   `stored view payload failed to decode — full rebuild`. Self-healing that leaves no trace is
   indistinguishable from a tick that silently keeps serving stale rows.
4. Read-side semantics are unchanged (`fresh_entries` already required decodability, READ-MODEL
   §6). The change makes the writer obey the rule the reader was already checking, so the two
   halves cannot drift apart again.

## Alternatives considered

- **Schema version only, no decode check.** A deliberate bump invalidates the rows, but the
  incident needed no bump: an old writer and a new reader coexisted, and nobody had a version to
  bump. Manual levers fail exactly when a deployment rolls back — the rolled-back binary's rows
  would still decode under the newer type and be reused as if nothing had changed.
- **Decode check only, no schema version.** Covers the observed case but not a shape change that
  still decodes: a renamed field of the same type, a changed unit, a new field with a different
  meaning. Those need an explicit, reviewable invalidation point, which is what the version is.
- **Rebuild every tick (drop the incremental path).** Correct and simple, and it is what the
  incremental path replaced: a full derivation per tick re-reads the store and holds every
  session's stream in memory, cost that grows with the store.
- **Version the view name (`session_usage@2`).** Puts the schema into the store key and the read
  path, leaves the old rows behind as garbage and multiplies every future drift check.
- **Repair the rows on read.** Violates the rule that a view is never repaired by guessing, and a
  reader cannot know how a payload was meant to be shaped.

## Consequences

- One full derivation per schema-version bump, and one per undecodable payload found. That is the
  cost of serving correct rows; it is bounded by the tick's existing lease and publication
  transaction.
- The digest domain label changed (`kian-view-v1` → `kian-view` + version), so every existing
  store's first tick after this lands does a full rebuild once. On the production database that
  is the ~20 min of store reads the manual cure already needed — paid once, automatically.
- Rollback is symmetric: the rolled-back binary's domain differs, so it rebuilds too. Neither
  direction serves rows it cannot prove.
- A payload that still decodes but whose *meaning* changed stays reused until the version is
  bumped. The rule for a semantic change to a view's payload is therefore "bump the version in
  the same change", and it is now reviewable in one constant instead of a manual drop.
- The tick decodes every stored row — roughly 1,400 small JSON payloads in production (343
  sessions across four views, plus the board). A no-op tick stays a no-op: no publication, no
  delta.

## Limits

- The full derivation still holds every session's stream in memory at once; bounding that is a
  separate concern and is not resolved here.
- There is no cross-version migration. A bump discards; it never rewrites rows in place.
- The version is global to all views. A shape change in one view invalidates the other four as
  well — acceptable because a rebuild is bound by the lease and correctness is worth more than
  one tick's reuse.

## Acceptance

- **L1** `crates/kian-store/src/views.rs::tests::the_digest_domain_carries_the_view_and_the_schema_version`
  — the public digest is the current version's; the view name and the version are separate
  domains.
- **L2** `crates/kian-cli/tests/views_e2e.rs::digest_clean_but_undecodable_view_self_heals_on_the_next_tick`
  — a payload re-stamped undecodable and re-sealed digest-clean drives a full re-derivation on
  the next tick and `fresh_usage` serves again (before the change the tick reused and derived
  nothing). The existing `tampered_view_self_heals_on_the_next_incremental_tick` still covers the
  digest-mismatch arm.
- **L3** production: the first tick after the deploy rebuilds every view once, then `/metrics`
  renders without `discarding the view` warnings. Not run at landing.
