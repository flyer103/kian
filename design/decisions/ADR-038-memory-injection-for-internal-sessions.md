# ADR-038: Memory injection reaches internally created sessions through a bootstrap seam

- **Status**: proposed (owner gate; append-only — ADR-001…037 immutable)
- **Date**: 2026-11-19
- **Related**: [design/MEMORY-MODEL.md](../MEMORY-MODEL.md) §6 (the injection contract), §11
  (open questions); [design/DESIGN.md](../DESIGN.md) §4.1/§4.6/§4.7 (resident sessions,
  delegation, cron); [design/CRON-MODEL.md](../CRON-MODEL.md) §9.1; [ADR-016](ADR-016-memory-as-events.md)
  (notes as events), [ADR-034](ADR-034-cron-scheduler.md) (cron), [ADR-036](ADR-036-durable-confirmation-plane.md)
  (the latest gate precedent). Evidence: `crates/kian-cli/src/server.rs:1554,1668`,
  `crates/kian-superagent/src/worker.rs:781-810`, `crates/kian-cli/src/cron_engine.rs:4060,429`.

## Context

MEMORY-MODEL §6 landed creation-time injection as a **quiet admit before the session's first
input** (`{"content": index, "memory": marker}`, idempotency key `memory-inject:<session>`). The
contract's language is "any fresh session", and I10's exclusions name only the source memory
session and dreamer sessions. The implementation has exactly **one call site**:
`Server::create_session` (`server.rs:1668`). `kian-core` is the projection, `kian-memory` renders,
but the admission decision lives in the control plane's create handler.

Every other way a session comes into existence bypasses that handler:

| Session family | Creation path | Why the create handler is not used |
|---|---|---|
| Resident Router / watchdog | bootstrapped by the first admit at boot (`config.rs` note; `superagent::start_services`) | the id is **config-pinned**, not minted per request |
| Delegated worker | `WorkerSpawner::spawn_bound` → admit (`worker.rs:781`) | the child id is **derived** (`child_session_id(thread_id)`, `worker.rs:810`) so a replay cannot mint a duplicate child |
| Cron target | `EngineTurn::deliver` → admit | `{channel, topic}` targets are a **derived** id (`cron_engine.rs:429`); `{session}` targets are pinned |

The consequence is a silent composition gap, not a documented exclusion: the most "always-on"
components — the resident Router, the watchdog, every worker, every scheduled turn — never receive
the memory index. Dreaming writes notes that only client-created sessions can see. This is the
"parallel features compose wrongly" case the review discipline asks for, and it is invisible in the
per-feature tests (each family passes its own suite).

Two constraints make the obvious fixes wrong:

1. **The index must be the session's first context-bearing event.** It is a prompt prefix
   (I5/I7); a later injection would land mid-history, change the prefix, and break constraint 3.
   Therefore the bootstrap must run *before* the internal creator admits its triggering input —
   not in the drain (the triggering row is already durable and earlier in inbox order), and not at
   the first wake.
2. **Derived ids are non-negotiable.** A "create a session first, then admit" workaround would
   have to mint a random id, breaking worker replay determinism (`child_session_id`) and cron's
   restart-stable target. The bootstrap must accept a caller-chosen id.

## Decision

1. **A `SessionBootstrap` seam, owned by the bundle, implemented by the assembly.** The trait
   lives in `kian-superagent` (it is Router/Worker/cron policy, not core — constraint 10), with
   one method: `bootstrap(&self, session: SessionId) -> Result<(), String>`. The `kian-cli`
   assembly implements it over the memory assembly (`inject_memory_index`) and passes it to the
   three internal creators: `superagent::start_services` (resident boot), the `WorkerSpawner`
   (spawn), and the cron `EngineTurn` (firing dispatch).
2. **Ordering is the contract.** Each creator calls `bootstrap(session)` **before** it admits the
   triggering row (the router/watchdog bootstrap prompt, the delegation wrapper, the cron firing).
   The index therefore becomes the first `input/admitted`, and the triggering input follows it in
   inbox order — exactly the order the create handler produces today.
3. **Fresh-only, idempotent, no-op by default.** The seam checks `version(session) == 0` before
   injecting; an existing session is never touched (a restart must not rewrite a live prefix —
   I2's spirit). The quiet admit keeps `memory-inject:<session>` as its key, so a retry or a race
   between two creators dedupes at the store. With no `memory:` section the seam is `None` and
   every call is a no-op (`Ok(())`), leaving streams byte-identical.
4. **The exclusion list is unchanged.** I10's identity exclusions still run inside the injection
   (`inject_memory_index` already checks them); the bootstrap adds no new authority. The
   `injected_skips` counter and the fail-open-on-error rule (I8) apply unchanged — a failed
   bootstrap never fails the creator.
5. **The seam is observable, not invisible.** Each bootstrap appends nothing when it skips; when it
   injects, the existing `memory` provenance on the first `input/admitted` is the audit record.
   No new event kind, no new payload key.

## Alternatives considered

1. **Status quo (client-created sessions only).** Cheapest, and honest once documented — but it
   makes dreaming a feature of the one session family the product does not run unattended, and the
   gap is silent in every acceptance table. Rejected as the terminal answer; acceptable only if the
   owner decides memory is intentionally scoped to client sessions.
2. **Drain-side event write at the first episode.** The drain appends the index event before
   consuming input. Rejected: it is execution writing an input-shaped event (layered iron rule),
   it bypasses the inbox row and its idempotency key, and it would have to synthesize the
   provenance the quiet-admit path already produces.
3. **Prepend / reorder the inbox.** No honest ordering exists: the triggering row's admission time
   precedes the wake, and rewriting inbox order violates the append-only admission contract.
4. **Mint a session via `create_session`, then admit the derived id.** Breaks derived-id replay
   (`child_session_id`, cron's hash) and would fork the id space. Rejected.
5. **Inject on the first wake instead of before the triggering admit.** The triggering input is
   already durable and earlier in inbox order; the index would be promoted *after* it. Rejected by
   the ordering constraint.
6. **A core seam instead of a bundle seam.** Core would have to know a memory concept; the
   narrow waist forbids it. The memory assembly is a CLI-level artifact; the seam belongs where the
   policies live.

## Consequences

- Resident Router/watchdog sessions, delegated workers and cron firings receive the index at
  creation. For a deployment with `memory:` configured, the first provider request of each of
  these sessions gains the index (a **one-time, first-position** prefix extension: no existing
  bytes are rewritten; constraint 3 is satisfied by construction, as at the create seam).
- **Constraint-3 event**: an index that changes after deploy (dreaming continues) makes new
  internally-created sessions differ from old ones — expected and observable via `source_seq`
  pinning; live sessions are never touched.
- One extra `version(session)` read per internal admit (cheap, the same read a claim performs).
- The worker spawn path gains a dependency on the bootstrap port; the port is injected at assembly,
  so `kian-superagent` stays memory-agnostic and its tests use a no-op.
- Cron firings now consume the index in their context; whether that is desirable for a nightly
  job with a narrow task prompt is a product question (open question 1).

## Rollback

Remove the seam wiring (the default `None` restores byte-identical behavior) or keep a config
switch `memory.bootstrap: false`; no data migration is involved, because injection writes ordinary
rows with the existing idempotency key. A session that already received the index keeps it; a
rollback never rewrites a stream.

## Open questions

1. **Scope for cron firings.** A nightly audit's prompt is narrow; the index may be noise. Options:
   all sessions, all *except* cron, or per-agent opt-in. Recommendation: all, then observe; the
   `source_seq` marker makes the cost visible.
2. **Cross-scope reads.** I10 keeps worker memory separate from the Router's scope; the existing
   open question (MEMORY-MODEL §11.6) is unchanged — this ADR only delivers the *same-scope* index.
3. **Bootstrap failure policy.** I8 says injection failure never fails creation; the seam returns
   `Err` for the log/counter but the creator proceeds. Confirm that a failed bootstrap on a worker
   spawn is also non-fatal (the worker runs without memory).
4. **Metrics.** Add `memory.bootstrap.injected_skips` per family, or reuse the existing counter with
   a `family` label? The latter risks a metrics-key change (API-CONTRACT); the former is
   process-local.
