# ADR-042: Multi-harness bridges — Claude Code and Codex behind one kian-side contract

- **Status**: proposed for the confirmation gate (2026-11-19). Implements phase 1 of
  [HARNESS-BRIDGE](../HARNESS-BRIDGE.md); leaves [ADR-010](ADR-010-dsh-session-mapping.md)
  untouched.
- **Context**: the product is the integrated party — harnesses own interaction and capability,
  kian owns reliable execution ([SPEC](../../docs/SPEC.md) §1). Only dsh is bridged today
  (`kian-dsh`, ADR-010), and the dsh case proved both the shape and the cost: a frozen wire
  contract, a mapping table onto existing kinds, a conformance fixture in default CI, and an
  env-gated live leg (whose only green run needs the ecosystem peer `contrib/dsh-v0-peer`).
  The two harnesses with the largest real-user surface — Claude Code and Codex — expose
  long-lived programmatic surfaces (stream-json; app-server JSON-RPC) that make a bridge
  possible with no ecosystem-side component. Neither exposes the assembled model request.
  Supporting them is therefore a distribution decision, not a research question: it meets
  developers inside the tool they already use. The risk it creates is the opposite of the one
  the dsh bridge created — breadth chipping at kian's honesty claims.
- **Decision**: bridge both harnesses, but as an explicitly *weaker* class of session than
  kian-owned loops, with the weakness made durable and machine-checkable.

  1. **Two bridge classes.** `exact` sessions persist the assembled request bytes as
     `step/request` and byte-replay them (today: dsh via the contract-v0 peer).
     `projection-only` sessions — Claude Code and Codex — persist every input/output event the
     harness exposes but **no** `step/request`, and claim no cache accounting, prefix
     stability or byte-identical request replay. This is a narrowing of A2's scope, not a
     silent weakening: A2 governs *kian's own* provider calls; a harness-owned call is not
     kian's to persist, and fabricating request bytes from the response is the post-hoc
     logger ADR-010 already rejected. The class is a durable fact:
     `session/created.bridge:{name,request_bytes}` — a spawned worker session is created eagerly
     (the spawner's `SpawnSessionWriter` port → `Engine::create_session_with`) so the marker
     is written before the first drain, which has no bridge knowledge (HARNESS-BRIDGE §4.7).
  2. **A shared kian-side skeleton, not two more copies.** `kian-harness` owns the state
     machine (episode spawn/park, safe boundaries, dedup, step budget, honesty, approval
     policy, the `tool/post` port); each harness contributes a protocol adapter and a spawn
     recipe (`kian-claude`, `kian-codex`). Narrow waist holds: `kian-core` only, a crate-local
     ctx port implemented by assembly — the dsh shape.
  3. **Native protocols, no ecosystem peer required.** Claude Code over `--print
     --input-format stream-json --output-format stream-json`; Codex over `codex app-server`.
     ACP is not the phase-1 transport (alternatives 2 and 3).
  4. **The harness's own permission gate becomes the tool gate.** Both harnesses ask the
     client before executing a gated tool. Kian answers from a declarative allow/deny policy
     and records the decision as `tool/call.permission{policy:"configured"}`. A request that
     matches no rule is denied. `ask` is refused at boot in phase 1: a human answer cannot be
     delivered to a harness process that died with the drain, and the harness's pre-ask side
     effects cannot be replayed — that restart problem needs its own design (phase 2).
  5. **One harness per worker template; no worker surface alongside.** The child owns its
     tools, so kian's `exec` and the remote-exec queue cannot be routed through it; the
     existing dsh mutual exclusion generalizes to every bridge (boot refusal, not a runtime
     surprise).
  6. **dsh does not move.** Its wire contract, mapping and tests are frozen; convergence onto
     the shared skeleton is phase 3 and must keep contract v0 byte-identical. Two skeletons
     for one phase is a recorded debt with a named removal condition.

- **Alternatives considered**:
  1. **One generic "external harness" driver with per-harness shell scripts** — rejected: it
     moves protocol translation into unversioned operator scripts, so the mapping table stops
     being reviewable and drift-testable, and honest contract errors become shell failures.
  2. **Drive ACP only (`@agentclientprotocol/claude-agent-acp` and equivalents)** — rejected for
     phase 1, retained for phase 3: ACP is the right *long-term* multi-harness surface (dsh
     already ships an `acp` profile), but the Claude Code adapter is a third-party process at
     an independently moving version, Codex ACP coverage is unverified, and ACP's
     session/update vocabulary still does not expose request bytes. Adding it first would put
     a community dependency on the critical path of the first impression.
  3. **Claude Code `--output-format json` one-shot / Codex `exec --json`** — rejected as the
     transport: no long-lived session, no mid-turn approval channel, no interrupt; the bridge
     would degrade to "run a task", not "hold a durable session".
  4. **Ecosystem peers for Claude/Codex as for dsh** — rejected: the dsh live leg's only green
     run depends on a `contrib/` peer, which is the single largest adoption friction in the
     dsh story. Shipping the adapter in-tree keeps a two-process story (kian ↔ harness) and a
     testable boundary.
  5. **Ship the request-capture proxy first, then bridges** — rejected for phase 1: a
     recording MITM proxy is a security-sensitive component (header/credential handling,
     streaming, provider-format drift) and would delay the user-visible bridge by a whole
     slice. It is phase 3's upgrade path to `exact`; the projection-only class is honest until
     then.
  6. **Derive the bridge class from config instead of recording it** — rejected: config is not
     in the event stream, so an auditor reading an export could not distinguish a
     projection-only session from an exact one; absence of `step/request` is also the shape of
     a crashed first turn.

- **Consequences**:
  - new crates `kian-harness`, `kian-claude`, `kian-codex`; `kian-cli` gains the assembly, the
    config grammar and the eager worker-session creation; `session/created` gains one additive
    payload key (`bridge`), no new event kinds;
  - cross-process harness resume is phase 2: a takeover ends a bridged session honestly
    (`failed` for a lost turn), and a later input starts a fresh harness child (the session, not
    the harness conversation, is durable) — a claim the capability matrix states explicitly;
  - the capability matrix ([HARNESS-BRIDGE](../HARNESS-BRIDGE.md) §3) becomes the public claim
    surface — a capability is either proven by a fixture/live smoke or absent from the matrix;
  - projection-only sessions are for interaction durability and control, not for cache/cost
    audit; dashboards and docs must not aggregate them into cache or cost claims;
  - fixture-based CI stays credential-free and binary-free: scripted children speak each
    native protocol, and the live suites stay env-gated (`KIAN_CLAUDE_BIN`,
    `KIAN_CODEX_BIN`), the dsh pattern;
  - phase-2 work (durable `ask`, hook bridging, cron argv) is now explicitly *blocked by* this
    ADR's approval-policy and marker choices, not merely deferred.
- **Rollback Trigger**: a pinned harness version changes its protocol in a way the adapter
  cannot map with existing kinds/frames (then a new design decision renegotiates the mapping),
  or a live smoke proves a claimed capability false (then the matrix row is withdrawn in the
  same change that records the negative). A bridge whose class claim cannot be honoured is
  removed from the config grammar rather than shipped degraded silently.
- **Cost Impact**: three crates + one cli assembly + fixtures; zero new event kinds; one
  additive payload key; one new normative doc and this ADR. The duplicated skeleton (dsh vs
  `kian-harness`) is bounded by the phase-3 convergence and must not grow a third copy.
