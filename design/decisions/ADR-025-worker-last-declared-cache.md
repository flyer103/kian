# ADR-025: Last-declared worker cache on the registry projection

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: `design/WORKER-MODEL.md` §1/§2/§5/§9, `design/API-CONTRACT.md` §8 (the
  `register` frame already carries both fields — unchanged), `kian-exec` registry migration v4,
  `docs/SPEC.md` §5 R12/R13.
- **Context**: membership is runtime data. The live directory (description, protocol version,
  connection generation, peer address, last-seen) is a projection of connections; the durable
  `endpoint_registry` row kept only id/addr/last-heartbeat/state. After a `kian serve` restart the
  directory is empty, so `list_workers` and `kian workers` show every known worker **id-only** until
  it re-registers (bounded backoff usually restores it in seconds, but a worker that is offline at
  restart stays id-only indefinitely, and the Router has to choose a machine without its
  description). The declaration is already on the wire (`register.description`,
  `register.version`); only the durable copy was missing.
- **Decision**: cache the worker's **last declaration** on the existing registry projection.

  1. **Additive columns, one migration.** `endpoint_registry` gains `description TEXT` and
     `protocol_version` (INTEGER/BIGINT), migration v4, both backends; pre-v4 rows read as "no
     declaration". No new table, no event, no wire change.
  2. **The registration writes it; heartbeats never touch it.** The registration handoff upserts the
     row with the validated description and declared revision in the same statement that marks the
     id online (`upsert_endpoint_with_declaration`). Heartbeat/detach/static upserts keep the old
     SQL and leave the columns alone, so a worker that stays connected and heartbeats for days keeps
     its declaration. A later registration that omits the description (or declares an invalid one,
     which the listener already drops) **clears** the cached text: the latest declaration is the
     fact, never a merge with an older one.
  3. **Live wins; the cache is labeled stale.** The directory read uses the runtime member when the
     worker is connected this process lifetime (`declaration_stale = false`) and falls back to the
     cached declaration only for registry-only ids (`declaration_stale = true`). `list_workers`
     emits `"declaration_stale": true` on rows whose description/version come from the cache;
     live rows keep today's bytes exactly. `kian workers --json` exposes `description` /
     `protocol_version` when declared (absent otherwise); the text board keeps its columns.
  4. **Still not truth.** The cache never decides identity, state, liveness, bindability or spawn
     legality: those remain the live directory + registry liveness computation. It is a declaration
     cache for the cold-start catalog only, and the stale flag is part of the contract. A worker
     that changes its description updates it on its next registration.

  Adversarial consequences checked: a crash between the registry upsert and the route attach leaves
  the id online-or-not per the heartbeat and the declaration cached (the next heartbeat/registration
  reconciles; no wrong routing); a worker that never returns keeps a stale-but-labeled description,
  which is honest and matches "stale ids are never pruned"; a registration without a description
  clears the old text (no ghost description); a v1 static pin has no declaration columns and reads
  exactly as before; the Router prompt and tool **schemas** are untouched (the data is a normal
  `list_workers` result), so no prefix invalidation is introduced.
- **Alternatives considered**:
  - **A separate declaration table**: more schema for two nullable columns; rejected.
  - **Keep it runtime-only (status quo)**: the cold-start catalog is id-only; the owner hit it in
    the live deployment and asked for the cache.
  - **Cache in the prompt or a tool schema**: forbidden by the membership/prompt-stability rule
    (WORKER-MODEL §5/§9) — data flows through `list_workers` results only.
  - **Never clear on a description-less registration**: merges two declarations; rejected.
- **Consequences**: one additive migration (v4); the Router/operator cold-start catalog now has the
  last text with an explicit stale marker; no change to membership lifecycle, routing, queueing or
  wire frames. The durable truth remains the event stream; this is a cache on the existing
  disposable registry projection.
