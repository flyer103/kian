# AGENTS.md — kian

Durable execution runtime for agents: sessions die, resume, migrate and
audit cleanly. A `Session` is the one first-class durable entity, the append-only event stream is
the sole truth, a `drain` is the execution hold interval, and **the agent loop is just a control
flow inside a drain**. Super-agent (Router/Worker, event subscriptions, BYOM endpoints) is a
*bundle* on top of the core, never core behavior.

`crates/`, `docs/`, `design/`, and this file are the product. `docs/SPEC.md` is
the WHAT, `design/DESIGN.md` the HOW, `design/decisions/ADR-NNN-*.md` the WHY (append-only).

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
    names.
13. **Bundle shaping is replayable at its own `tool/call` seq.** When a bundle output branch
    depends on runtime context (which shaping mode, which ack, which target), derive it as a pure
    function of the committed stream evaluated at the write's own `tool/call` seq — never from the
    live tail or process state. The outbox re-drive reconstructs a pending intent from that same
    seq and re-applies the shaping; a branch that cannot be re-derived there silently changes the
    bytes a retry sends.

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

These are the CI gates on every push ([`.github/workflows/gates.yml`](.github/workflows/gates.yml)).
A docs-only change that skips the full workspace run must still keep links and crate mappings
consistent: `cargo test -p kian-cli --test docs_map`.

## Repo layout

- `docs/SPEC.md` — WHAT: positioning, user/ops interfaces, scope.
- `design/DESIGN.md` — HOW: execution model (Admit→Wake→Drain→Promote), session state machine,
  three-layer recovery, event schema versioning, plugin system, crate map, super-agent bundle.
- `design/decisions/` — ADRs, append-only. ADR-001 Rust foundation … ADR-019 governed memory.
- `LICENSE` — Apache-2.0 (the whole workspace; `license.workspace = true` in every crate).
- `docs/advanced.md` — the capability and usage guide: the full capability list and one runnable
  snippet per capability, out of the README.

## Version control

Plain git. One logical change per commit, containing only its own files. Do not rewrite published
history. Commits land as work completes (owner convention); publishing to a remote happens only
when the owner asks. Negative results commit documentation only — never broken code.

## Docs map

- `README.md` — the landing page (see the README policy below): one-line claim, three features, the
  model, quickstart, environment contract, license. Long form is never here — the crate map is
  overview §4, not the landing page.
- `docs/advanced.md` — every capability with one runnable snippet, plus the super-agent bundle,
  channels, scheduling, memory, plugins and the operator surfaces.
- `docs/SPEC.md` — the contract (WHAT). Changing scope means changing this file in the same change.
- `design/DESIGN.md` — the blueprint (HOW). Any crate/seam/invariant change updates it in the same
  change.
- `design/EVENT-MODEL.md` — the normative event contract (source of truth): envelope, the complete
  23-kind catalog, payload schemas and the event-change protocol. **Any Event change starts here**
  and updates this file first; code/tests must be reconciled in the same change.
- `design/API-CONTRACT.md` — the normative external API contract (source of truth): endpoints,
  JSON-RPC methods/params/results, error codes, SSE frames, metrics keys, the webhook status matrix
  and KiandProto frames. **Any wire change starts here** and updates this file first; `kian-sdk`,
  code and tests are reconciled in the same change.
- `design/MEMORY-MODEL.md` — the normative memory contract (source of truth): notes as
  non-context events, the dreaming drain, creation-time injection, representations as projections.
  **Any memory semantic change starts here**; the kind bytes stay in EVENT-MODEL, the wire in
  API-CONTRACT, the placement in DESIGN.
- `design/CONFIRMATION-MODEL.md` — the normative confirmation-plane contract (source of truth,
  phase 1 landed): what an `ask` is, the pending-call projection, the `awaiting_input` resumable
  end, the answer as an admitted control row and the resume rule.
  **Any `ask`/confirmation semantic change starts here**; the kind bytes and wire tables land in
  EVENT-MODEL / API-CONTRACT with the implementation (both are drift-tested against code).
- `design/READ-MODEL.md` — the normative serving-form contract (source of truth): versions,
  unchanged/delta reads, resumable streams, verified snapshots, materialized views, opt-in
  export-only retention and bounded-staleness reads. Truth stays the event stream; wire shapes
  stay in API-CONTRACT. **Any serving/read-surface change starts here**.
- `design/CRON-MODEL.md` — the normative wall-clock-scheduling contract (source of truth, P0/P1
  landed): cron expressions, timezones and DST rules, the durable schedule cursor, misfire and
  overlap policy, worktree-backed firings and disk guards, the `kian cron` CLI and the
  "why didn't it run" records. **Any scheduling-semantic change starts here**; a firing is an
  ordinary turn, so the kind bytes stay in EVENT-MODEL and the wire in API-CONTRACT.
- `design/CHANNEL-WECHAT.md` — the normative WeChat iLink channel contract (source of truth,
  best-effort by contract): protocol mapping, credential file, config/CLI, receive allowlist,
  dedupe and the cursor bound, degradation. **Any WeChat channel change starts here**; a protocol
  break degrades this channel only (ADR-018).
- `design/CHANNEL-IMESSAGE.md` — the normative iMessage channel contract (source of truth,
  best-effort by construction): the macOS bridge ABI (health/updates/send), the two allowlists,
  the privacy filter, cursor/generation discipline, degradation, acceptance. **Any iMessage
  channel change starts here**; the `kian-imessage-bridge` crate and `kian-cli` config/CLI carry
  no semantics of their own (ADR-042).
- `design/WORKER-MODEL.md` — the normative worker-domain contract (source of truth): membership
  lifecycle, binding/dispatch, catalog/discovery, security boundary, version policy and
  degradation. **Any worker-membership semantic change starts here**; wire shapes stay in
  API-CONTRACT, event kinds in EVENT-MODEL, placement in DESIGN.
- `design/PLUGIN-SPEC.md` — the normative hook-plugin contract (source of truth): the two-tier
  model (compile-time handlers + operator-trusted stdio sidecars), hook vocabulary and payload
  catalog, interception ports, capability grants, manifest and stdio ABI, per-worker resolution,
  session consistency, failure policy and trust model. **Any harness-hook plugin change starts
  here**; event kinds stay in EVENT-MODEL, wire shapes in API-CONTRACT, placement in DESIGN.
- `design/HARNESS-BRIDGE.md` — the normative external-harness bridge contract (source of truth,
  phase 1): bridge classes (exact vs projection-only), the capability matrix, per-harness mapping
  (Claude Code stream-json, Codex app-server, dsh v0), the approval policy, the config grammar and
  the phase plan. **Any bridge semantic change starts here**; the one event change
  (`session/created.bridge`) lands in EVENT-MODEL, crates in DESIGN's crate map.
- `design/decisions/ADR-NNN-*.md` — the why, append-only; superseding writes a new ADR that links
  the old one.

## README policy

`README.md` is the landing page, not a changelog and not a manual: a stranger should understand it
in three minutes and run it in five. Keep this shape, top to bottom: title + status badges →
one-liner + problem → one recorded demo (GIF, real run) → `## Who this is for` (who it is for and
the three things it explicitly does not promise) → `## Features` (exactly three bullets, one per
core promise — never an implementation/storage/deployment metadata list) → `## How it works` (the
model, closing with one "read more" line) → `## Install` (the fastest path that actually
works today, plus the container/archive paths once a release has published them) → `## Quickstart` (copy-paste commands that run) → `## Examples` (one or two
runnable shapes, each its own README) → `## Environment contract` → `## License`.

Recordings live in `docs/assets/` as both `.gif` (for the README) and `.cast` (replayable with
`asciinema play`). A GIF must come from a real run against a real binary — never a mocked render of
what the output would look like.

The long form lives outside the README: `docs/advanced.md` carries the full capability list and one
runnable snippet per capability; the document map is this file's §Docs map and
[docs/overview.md](docs/overview.md).

**Never add to README** — these live in their owning docs:

- status tables, test counts, event-kind/ADR counts, acceptance checklists, four-gate records —
  [docs/SPEC.md](docs/SPEC.md), [docs/overview.md](docs/overview.md), the pull request;
- release history and internal work-plan narratives — [CHANGELOG.md](CHANGELOG.md), SPEC §5;
- verbose live-run logs and benchmark methodology — runbooks, SPEC, the pull request;
- semantic detail already owned by SPEC / DESIGN / EVENT-MODEL — link to it, never restate it;
- positioning-by-analogy ("the X of Y") or comparisons with other products — describe what the
  runtime is and what it guarantees, not what it resembles.

**When changing README**, in the same change:

1. Fix cross-references: this file's docs map and any SPEC/DESIGN/runbook sentence pointing at a
   README section (no stale pointer to a removed README section).
2. Preserve the anchors other docs link to (`#quickstart`, `#environment-contract`).
3. Keep every relative link resolvable; the crate map stays in overview §4 + DESIGN §7, and
   `cargo test -p kian-cli --test docs_map` is the gate for both.
4. Keep Quickstart commands executable against this tree, YAML snippets parseable, and abridged
   output marked as abridged.
5. Adding a capability = one short section with a runnable snippet in `docs/advanced.md` (plus one
   line in `## Features` only if it is one of the three core promises); its semantics stay in
   SPEC / DESIGN / EVENT-MODEL.
6. `site/index.html` mirrors this README's claims, install steps and quickstart (its own README says
   so). A change to the one-liner, `## Features`, `## Install` or `## Quickstart` updates the site in
   the same change, or the two drift and the landing page starts promising what the runtime no
   longer does.

## CHANGELOG policy

`CHANGELOG.md` records **what changed, when, and in which capability** — nothing else.

- **Newest first.** The newest entry is at the top; the file reads as a reverse-chronological log.
- **One bullet per functional change** — capability, contract, behavior — naming the owning
  doc/ADR. A change with no functional delta (a verification-only round, a log edit) gets no entry.
- **No acceptance results**: no gates, test counts, "verified / unverified / live-run / L2-L3
  evidence" bullets, QA verdicts, or deployment status. Those belong to the pull request, the run
  logs/runbooks, and the SPEC / overview status tables.

