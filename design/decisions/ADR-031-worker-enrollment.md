# ADR-031 — Worker enrollment: per-worker tokens, a config-owned capability catalog, and the dynamic queue bound

- **Status**: accepted (phase 1; D2 superseded by [ADR-032](ADR-032-worker-declared-capabilities.md), the rest stands)
- **Date**: 2026-11-15
- **Context**: [ADR-021](ADR-021-runtime-worker-membership.md) (runtime membership),
  [ADR-025](ADR-025-worker-last-declared-cache.md) (last-declared cache),
  [WORKER-MODEL](../WORKER-MODEL.md) §6, `docs/SPEC.md` §5.

## Context

R12's worker domain admits every dynamic worker with **one shared token** and exposes only
`id`/`description`/`version`/`max_concurrent` in the catalog. The contract records the consequence
explicitly: any token holder may register under any id, including a live one, and replace that
worker's connection — acceptable only for the single-operator deployment where the transport
(SSH/Tailscale) authenticates the user. mTLS and per-worker identity were deferred to R13.

A device worker (the iPhone worker, [docs/runbook-ios-worker.md](../../docs/runbook-ios-worker.md))
makes the gap concrete:

1. **Identity**: the phone shares the token with every build box. A compromised build box could
   replace the phone's route and receive its commands. The phone also holds device APIs
   (calendar, health, photos), so a replacement is a privacy event, not just a routing event.
2. **Capabilities**: the Router cannot tell that `iphone` can read a calendar but cannot run a
   Rust build. `description` is a 200-character human string; it is not a selection surface.
3. **Liveness**: a phone is intermittent. The default backend queue bound is 60 s, so an
   offline-at-the-wrong-moment phone turns an honest park into a timeout before the device
   comes back.

The three fixes are coupled only by deployment, not by mechanism. Splitting them lets each land
on the narrowest existing seam.

## Decision

**Phase 1 is config-first and does not change KiandProto.** Worker-declared capabilities,
per-worker certificates and per-command capability enforcement are later phases.

### D1 — Per-worker tokens (`workers.tokens`, `workers.enrolled_only`)

- `workers.tokens` is an `id -> environment-variable name` map. Each listed id authenticates
  against **its own** token; the shared `workers.token_env` is not accepted for a listed id.
- `workers.token_env` becomes optional. At least one of the shared token and a non-empty
  enrollment map is required.
- `workers.enrolled_only: true` admits **only** listed ids. It requires a non-empty map and
  forbids `token_env`, so a strict deployment cannot silently keep a shared fallback.
- Comparison stays constant-time; tokens stay env-only, redacting, never logged or echoed. A
  refusal is the existing `bad_token` (no new reason, no id-enumeration oracle: strict-mode
  unknown ids and wrong tokens are indistinguishable).
- The shared-token limitation is **narrowed, not eliminated**: in hybrid mode an unlisted id
  still uses the fallback. A fully closed deployment must enroll every id and set
  `enrolled_only: true`.

### D2 — Config-owned capability catalog (`workers.capabilities`)

- `workers.capabilities` is an `id -> [name]` map. Names are lowercase `[a-z0-9._-]`, 1..=64
  bytes, at most 16 per worker, unique.
- The catalog is **configuration**, not membership truth: it survives the worker being offline
  and is not part of ADR-025's last-declared cache. `declaration_stale` keeps its existing
  meaning (description/version only).
- It reaches the Router **only** through `list_workers` results (an additive `capabilities`
  array, omitted when empty) and `kian workers` (JSON field + text column). `spawn_worker`'s
  schema and the judge prompt are untouched — no catalog byte enters a prompt or a schema.
- The catalog is declarative, not enforced: a wrong entry misleads selection; it cannot grant a
  tool the worker does not have.

### D3 — A dynamic queue bound (`workers.queue_timeout_ms`)

- `workers.queue_timeout_ms` (default 60 s, positive) joins the backend's existing single queue
  bound: the backend takes the max over static `endpoints[].queue_timeout_ms` and this value.
- The bound is **global** (one `RemoteExecConfig`), so raising it for an intermittent worker also
  lengthens every queued command's honest timeout. This is recorded, not hidden: the alternative
  (per-row timeouts) would change the queue's lease/abandonment machinery and is deferred.
- No new timeout knob per id; no rebind/failover. A worker that never returns still ends in the
  honest timeout.

## Alternatives considered

| Alternative | Why rejected for phase 1 |
|---|---|
| **mTLS / per-worker certificates (R13)** | The correct end state for identity, but it needs PKI, issuance, rotation and an upgrade story for every worker. It does not help capabilities or the queue bound, and it would block a device worker behind a certificate-distribution project. Deferred, not dropped. |
| **KiandProto v3: `capabilities` in `register`** | The honest long-term declaration (the worker knows what it can do). But it forces a dual-crate version bump, a registry-cache migration and every worker to upgrade before the first iPhone worker can be selected. Config-first follows the repo's "config, then plugin, then seam (with an ADR), core primitive last" rule. Deferred. |
| **Per-id `endpoints:` pins for phones** | A static pin cannot join/leave at runtime, and the pin owns the id (a dynamic registration is refused `duplicate_static`). It solves capabilities but breaks the runtime-membership model. |
| **A `best_effort`/`intermittent` scheduling flag** | New scheduling semantics (rebind, failover, per-row timeouts) need ADR-020's deferred machinery. A timeout knob is the smallest honest step. |
| **Per-command capability enforcement in `spawn_worker`** | The schema must stay a plain string (ADR-020 D5/D7, prefix stability). Selection stays a Router decision over `list_workers` results. |

## Consequences

- `kian.yaml` becomes the enrollment registry for a strict deployment: adding a worker is a config
  edit **and** a restart. That is deliberate — per-worker credentials are a security boundary, and
  the existing contract already accepts config edits for `endpoints:` pins. Non-strict deployments
  keep the runtime join/leave behavior with the shared token.
- The capability catalog can drift from the worker's actual build. A stale entry is a selection
  mistake, never an authority: `exec` still runs (or fails) on the worker.
- Per-worker tokens are still bearer tokens. A token leak is still a compromise of that id; only
  rotation (restart with a new env value) revokes it. mTLS remains the R13 fix.
- The queue bound is global; a phone-tuned value can make static workers wait longer before the
  honest timeout. Deployments that need different bounds must split into separate kian instances.
- The catalog lives on the `workers:` section, so a **static-only** deployment (no `workers:`) has
  no capability catalog in phase 1. Adding a catalog entry for a static pin means enabling a
  `workers:` section, which also opens a listener — a coupling removed only by moving the catalog
  to a top-level config section (or the wire) later.
- `kian workers` and `list_workers` gain an additive field/column; the wire is unchanged, so
  KiandProto v2 workers keep registering without an upgrade.

## Invariants preserved

- **No membership truth**: tokens and capabilities are config, not durable membership facts; the
  runtime directory stays disposable; the registry remains a projection.
- **No prompt/schema bytes**: capabilities flow only through tool **results** and the CLI.
- **No wire change**: frame shapes, `PROTOCOL_VERSION` and both halves' goldens are untouched.
- **Fail-closed**: missing/blank token env refuses `serve` startup; conflicting modes refuse
  config load; a strict deployment cannot keep a shared fallback.
- **Constant-time, redacted**: token comparison and `Debug`/`Display` behavior are unchanged.

## Non-goals (phase 1)

- mTLS, per-worker certificates, issuance/rotation.
- Worker-declared capabilities on the wire (KiandProto v3).
- Per-id queue timeouts, rebind, failover or worker pools.
- Capability-based spawn validation or enforcement.
- A hosted/multi-tenant security story.
- L3 evidence on a real iPhone deployment.
