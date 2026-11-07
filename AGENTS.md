# AGENTS.md — kian

Durable execution runtime for agents ("the Temporal of agents"): sessions die, resume, migrate and
audit cleanly. A `Session` is the one first-class durable entity, the append-only event stream is
the sole truth, a `drain` is the execution hold interval, and **the agent loop is just a control
flow inside a drain**. Super-agent (Router/Worker, event subscriptions, BYOM endpoints) is a
*bundle* on top of the core, never core behavior.

`crates/` (when it exists), `docs/`, `design/`, and this file are the product. `docs/SPEC.md` is
the WHAT, `design/DESIGN.md` the HOW, `design/decisions/ADR-NNN-*.md` the WHY (append-only),
`the design notes` + `a vendored archive/` the frozen research inputs.

## Hard constraints (bind every change)

1. **Event stream is the sole truth.** All state is a projection of events; projections are
   disposable and rebuildable. Session state is *read only from the event projection* — never from
   scattered columns. A change that stores authoritative state outside the event log is wrong even
   if it passes tests.
2. **Persist-what-you-send.** The exact bytes sent to a model are persisted as events before the
   request flies; replay is byte-identical. If a model-visible input exists, a session event for it
   exists. Synthetic (steer-injected) messages carry a strippable marker.
3. **Cache-byte stability is a cost axiom.** Historical context is immutable. Compression is the
   *only* deliberate prefix-invalidation point and must emit an observable event. Never introduce a
   code path that rewrites, reorders or reformats prior context — that silently destroys provider
   prefix cache and raises cost.
4. **End writes are single atomic appends.** Session end + state transition in one atomic append;
   session state is projected from it. Splitting them violates NoHalfTerminated even with per-write
   CAS. On sqlite this means one transaction, on postgres one statement.
5. **Safety by fencing, liveness by lease.** Claim issues a monotonic epoch; every drain write
   carries it; the store rejects stale-epoch writes via expectedSeq CAS (NoSplitBrainWrite). Lease
   TTL + heartbeat only *detect* recoverable sessions — never treat lease expiry as permission to
   accept stale writes.
6. **Admit before wake; wake is advisory.** Inputs are durable rows before any wake fires. Wakes
   may fold, be lost, or race; the inbox rebuild at startup re-derives the wake registry, so
   NoLostWork holds across restarts. Promote only at provider-turn safe boundaries.
7. **Honesty over completion rate.** interrupted ≠ failed; an empty model response is a soft
   failure; a stop must pass the evidence gate. Never map an abort/crash onto `completed`.
8. **Recovery is bounded and declarative.** Error classification is a pure function; recovery
   policy is a declarative table owned by the caller. Every recovery action has a one-shot guard
   and consumes budget with refund semantics. No recovery path may loop unboundedly.
9. **The layered iron rule.** The control plane only writes the inbox and reads projections — it
   never touches execution. Execution only appends events — it never mutates projections directly.
   After a crash, replay rebuilds all in-memory state.
10. **Narrow waist.** The core crates (`kian-core`, `kian-store`, `kian-orchestrator` and the seam
    traits) contain no provider, channel, daemon or super-agent specific code. New capability grows
    config-first, then plugin, then seam (with an ADR), core primitive last. Router/Worker/channel
    policies live in `kian-superagent` (bundle), not the core.
11. **Model slots are declarative and session-immutable.** Slot → provider/model/fallback is fixed
    for a session's lifetime; switching emits an explicit migration event. Runtime model swap is
    cache-hostile and forbidden on the hot path.
12. **English only.** All repository text is English — docs, code comments, commit messages, file
    names. Frozen reference snapshots in `a vendored archive/` are captured source material, not
    authored text, and keep their original language.

## Environment gotchas

- **`docs/` carries no research snapshots.** The design inputs are not vendored here: the design
  is the authority, and an external source is cited by its live URL in prose when a decision needs
  it. Do not reintroduce an archive directory for third-party pages or internal docs.









- **macOS system proxy intercepts localhost** (inherited vadis lesson): any local end-to-end test
  involving `kian serve` + a client needs `NO_PROXY=127.0.0.1,localhost`.
- **Tool schemas are part of the prompt prefix.** Changing tool-schema ordering/format in
  serialization invalidates provider prefix cache across a whole session — treat serialization
  format as stable content, gated like constraint 3.
- **Rust async: no blocking in drain tasks.** Drain tasks run on tokio; sqlite access goes through
  a dedicated blocking pool (`spawn_blocking`) or a sqlx pool — a blocking call on a runtime worker
  stalls every concurrent drain.

## Commands

```bash
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

(Workspace does not exist yet — these become the CI gates the moment `crates/` lands. Until then
the gates are: every doc change keeps the docs map consistent and every ADR link resolves.)

## Repo layout

- `docs/SPEC.md` — WHAT: positioning, user/ops interfaces, acceptance scenarios S1–S7, phases.
- `design/DESIGN.md` — HOW: execution model (Admit→Wake→Drain→Promote), session state machine,
  three-layer recovery, event schema versioning, plugin system, crate map, super-agent bundle.
- `design/decisions/` — ADRs, append-only. ADR-001 Rust foundation … ADR-006 model slots.
- `the design notes` — structured synthesis of the four design inputs.
- `a vendored archive/` — frozen snapshots (WeChat super-agent article; dsh README/architecture;
  cordis README/primer; Feishu kian design rev 1425; Feishu an internal design review rev 47).

## Version control

Plain git. One logical change per commit, containing only its own files. Do not rewrite published
history. Commits land as work completes (owner convention); publishing to a remote happens only
when the owner asks. Negative results commit documentation only — never broken code.

## Docs map

- `README.md` — what kian is, the one-line claim, the docs map.
- `docs/SPEC.md` — the contract (WHAT). Changing scope means changing this file in the same change.
- `design/DESIGN.md` — the blueprint (HOW). Any crate/seam/invariant change updates it in the same
  change.
- `design/EVENT-MODEL.md` — the normative event contract (source of truth): envelope, the complete
  21-kind catalog, payload schemas and the event-change protocol. **Any Event change starts here**
  and updates this file first; code/tests must be reconciled in the same change.
- `design/decisions/ADR-NNN-*.md` — the why, append-only; superseding writes a new ADR that links
  the old one.
- `the design notes` — research synthesis; update when a new design input is added.
- `a vendored archive/` — immutable archive; new inputs append new files with Archived headers.
