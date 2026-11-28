# ADR-080 — The full view rebuild streams one session at a time

- Status: accepted (2026-11-28, bounded view rebuild)
- Related: ADR-079 (a view's stored rows prove their schema), ADR-076 (liveness is a probe)

## Context

Materialized views are disposable accelerators over the event stream
(`design/READ-MODEL.md` §6). The incremental maintenance tick is bounded: it reuses a stored row
whose `row_version` equals the live head and re-derives only what changed. The *full* rebuild is
the fallback that self-heals a view with no usable stored state — and it was not bounded: it read
every session's complete stream into one in-memory map before deriving anything.

On 2026-11-28 01:08–01:25 UTC that cost became visible. A forced full rebuild on the production
store (`/srv/kian/kian.db`, 3.1 GB; 31,238 events; `sum(length(envelope))` = 3,229,792,439 B
across 343 sessions) pushed `kian.service` to ~4.9–5.35 GB RSS on a 7,931 MB box, i.e. the
process's memory tracked the *store*, not the work in flight. Three of the five views
(`session_directory`, `session_usage`, `session_cache`) are per-session folds and never needed
another session's events; the other two (`ticket_stage`, `task_board`) are global folds that only
need board-kind events plus the streams the board names.

The tempting fix — reuse the incremental tick's shape by keeping a map of worker streams for the
ticket fold — buys nothing on this store: 337 sessions carry `thread/started` and their byte total
is 3,229,468,968 B, i.e. the "worker streams" *are* the whole store.

## Decision

1. **The full rebuild streams one session at a time.** `rebuild_under_lease` lists the sessions,
   then loops: read one session's stream, derive its directory/usage/cache rows, drop the stream
   (with a `yield_now` so the runtime keeps serving) and move on. Nothing holds two sessions'
   events at once.
2. **The ticket fold gets a streaming form, not a map of streams.** `ticket_rows_streaming` reads
   the board kinds once for the global inputs (worker discovery, registry aliases, outbox status),
   then re-reads one worker at a time and yields its row. The per-row attribution lives in
   `ticket_row_of`, a pure function the whole-store fold (`ticket_rows_from`, the reference
   derivation used by tests) calls too — so the streaming path cannot drift from the honest one:
   both compute the same row from the same inputs.
3. **The task board reads bounded inputs on both paths.** The kind-filtered parent streams, plus
   each delegated child's stream one at a time; the incremental tick and the full rebuild share
   the same read.
4. **Read semantics are unchanged.** The published row set, the store key it is pinned at, the
   freshness gate, the digest and the one-transaction version flip are exactly what they were.
   The change is memory, not meaning.

## Alternatives considered

- **Cap the rebuild and fail when the store is too large.** Honest but useless: the fallback is
  what recovers a corrupt view, and it would refuse precisely on the stores that need it.
- **Keep the whole-store map and rely on the OS.** That is the status quo that reached 4.9 GB; on
  a smaller box the OOM killer ends the process and the views stay absent.
- **Derive tickets from a map of only the "worker" streams.** Measured above: those streams are
  the store. No reduction.
- **Reduce the retained event set (drop bodies, keep envelopes) inside one session.** Tempting
  next step, but it changes which bytes the derivations see; the derivations need the payloads
  (`turn/end` usage, `step/request` prefixes). Deferred, not adopted.
- **Skip the full rebuild and trust the incremental one forever.** Loses the self-heal ADR-079
  relies on.

## Consequences

- **Peak memory is bounded by the largest single session**, not by the store: on this store
  ~709,367,049 B of raw envelopes (session `2935de59-7356-59b7-89ff-aa2595bd4a9c`, 313 events),
  ~1–1.5 GB RSS with the parsed form — versus ~4.9 GB observed for the whole-store map.
- **Wall time is comparable.** The per-session loop does the same reads; the ticket fold adds one
  kind-filtered read and one pass per worker, both index-backed, replacing a full-store pass.
- **One row function, two callers.** Equivalence is structural (`ticket_row_of`), not asserted by
  keeping two copies in sync. The existing full-vs-incremental e2e tests still guard the merged
  row sets end to end.
- **The server stays responsive during a rebuild.** Each iteration yields; `/healthz` answers from
  process state alone (ADR-076) and is unaffected either way, but the ticker loop and requests no
  longer wait behind one long synchronous fold.
- **Rebuild cost still scales with the store** — that is inherent to a full derivation. What
  changed is that the process no longer has to hold it.

## Limits

- A single pathological session still bounds the peak; per-session streaming is as fine-grained as
  the derivations allow. A session whose own stream does not fit remains a hard floor.
- The ticket fold re-reads the board kinds in full on every full rebuild (it already did on the
  incremental path when the board changed). No new cache was introduced.
- The task board's parent streams are read again on top of the children; on a store with many
  parents this is a second bounded pass, not a second copy of the store.

## Acceptance

- **L1/L2 (landed).** `cargo test -p kian-cli --test views_e2e` — the full-vs-incremental
  equivalence tests for all five views, including
  `rebuild_equals_full_derivation_on_a_multi_session_store`,
  `incremental_rebuild_equals_full_after_partial_changes`,
  `task_board_rows_equal_the_honest_derivation_at_the_same_store_key` and
  `incremental_rebuild_equals_full_for_task_board`; plus
  `commands::tests::ticket_row_attributes_parked_deliveries_by_alias_or_conversation` pinning the
  shared row function's alias/parked attribution.
- **L3 (deployed run, not run at landing).** Force a full rebuild on the production store and
  observe `memory.peak` for `kian.service` stay ≈ ≤ 1.5 GB (the 2026-11-28 run peaked ~4.9 GB),
  with every view published, `stale_usage_rows` = 0 and `/healthz` 200 throughout.
