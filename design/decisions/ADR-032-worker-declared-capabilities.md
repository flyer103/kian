# ADR-032 — Worker-declared capabilities (KiandProto v3)

- **Status**: accepted; supersedes [ADR-031](ADR-031-worker-enrollment.md) **D2 only** (the
  config-owned capability catalog). D1 (per-worker tokens) and D3 (queue bound) stand.
- **Date**: 2026-11-15
- **Context**: [WORKER-MODEL](../WORKER-MODEL.md) §1/§5/§7,
  [API-CONTRACT](../API-CONTRACT.md) §8, [ADR-025](ADR-025-worker-last-declared-cache.md),
  [ADR-031](ADR-031-worker-enrollment.md).

## Context

ADR-031 D2 put capabilities in `kian.yaml` (`workers.capabilities: id → [names]`). That was the
smallest phase-1 step, but it has two structural problems:

1. **The worker knows its build; the operator's file does not.** A device command added or removed
   in the app silently drifts from the config catalog. There is no moment at which the two are
   reconciled, and the catalog is served as if it were a fact about the running process.
2. **Two sources for one fact.** ADR-025 already caches the worker's *last declaration*
   (description/version) at registration. Capabilities are the same kind of fact. Keeping them in
   config creates a parallel truth that the live directory and the cache must both paper over.

The wire already carries the declaration path: `register` is the one frame a worker sends about
itself. Adding an optional list there removes the second source and lets the cache do what it was
built for.

## Decision

### D1 — `register.capabilities` (v3)

- `EndpointFrame::Register` gains an optional `capabilities: Vec<String>`; the key is omitted when
  empty, so every v0–v2 register byte is unchanged.
- The worker validates its own list at startup (lowercase `[a-z0-9._-]`, 1–64 bytes, ≤16, unique)
  and refuses to boot on an invalid entry.
- The server re-sanitizes (trim, drop invalid/duplicate/over-limit) with one audit line; the
  registration still succeeds. A malformed declaration is never a new refusal reason and never
  fails the handshake.
- `PROTOCOL_VERSION` moves 2 → 3 in both halves. No compatibility shim: a v2 worker is refused
  `version_mismatch`; a v3 worker pointed at a v2 kian is refused and keeps its bounded backoff.

### D2 — Cache the accepted declaration (migration v5)

- `endpoint_registry` gains a nullable `capabilities` TEXT column (sqlite and postgres, v5). The
  declaration upsert writes the sanitized JSON list; heartbeat/detach upserts never touch it; a
  registration that omits the key clears it (the latest declaration is the fact).
- The live directory carries the declaration for connected members; the registry cache carries it
  for registry-only ids. `list_workers` and `kian workers` serve the live view when connected and
  the cached view otherwise; the cached row keeps `declaration_stale: true`, which now covers
  capabilities as well as description/version.

### D3 — Remove `workers.capabilities`

- The config catalog is removed. `kian.yaml` no longer has a capability field; capabilities are
  worker-supplied. The config remains the place for enrollment (`tokens`), the liveness bound
  (`offline_after_ms`/`queue_timeout_ms`) and bind policy.

### D4 — Static pins have no declaration (non-goal)

- `hello` is unchanged in v3, so a static `endpoints:` pin cannot declare capabilities. Its catalog
  row omits the field. This keeps the version bump to one frame and one cache column; a v4
  `hello.capabilities` is a separate decision if static capability selection becomes necessary.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Keep the ADR-031 config catalog | Two sources, operator drift; the catalog claims to describe the running worker without any evidence. |
| Add `capabilities` to both `register` and `hello` now | Doubles the wire surface and the static-endpoint plumbing for no user in this phase; the iPhone worker is dynamic. Deferred, not dropped. |
| A separate capability RPC after registration | A second round trip and a second source; the declaration already has a natural home in `register`. |
| Append capabilities to `description` | The 200-char human string is not a selection surface; losing structure loses the unique/size guarantees. |
| Enforce capabilities at spawn | The `spawn_worker` schema must stay a plain string (ADR-020 D5/D7); selection is a Router decision over `list_workers` results. |

## Consequences

- A v2 worker must upgrade before it can register to a v3 kian; the queue keeps its rows parked
  during the window (no lost work, no cross-worker dispatch).
- Migration v5 is additive and nullable: pre-v5 rows read as an empty declaration; no backfill.
- The declaration is **not** an execution proof. A worker can declare `apple-calendar` and then
  fail the command; the catalog misleads selection, it grants nothing.
- `kian workers` now reads capabilities from the registry cache, not from config; a config edit
  can no longer change the board.
- The wire is frozen at v3; any future capability change (static `hello`, enforcement, a
  capability-scoped spawn argument) is a new decision.

## Invariants preserved

- No membership fact becomes durable truth: the cache is a projection of the worker's declaration,
  the live directory wins, and no binding/state decision reads capabilities.
- No prompt or tool-schema bytes: capabilities flow only through `list_workers` results and the
  CLI.
- Token handling, enrollment, queue-first dispatch, 1:1 binding and no-failover are untouched.
- Fail-closed at the worker: an invalid declaration refuses startup; the server's sanitizer is a
  defence, not a silent repair of the worker's own config.

## Non-goals

- Static-pin capabilities (`hello.capabilities`), capability-scoped spawn validation/enforcement.
- mTLS/per-worker certificates (R13; ADR-031 D1 remains bearer-token enrollment).
- Per-id queue timeouts, rebind, failover.
- Detecting declaration drift against the worker's actual build.
