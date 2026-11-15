# ADR-028: Harness hook plugins — a stdio sidecar tier on top of ADR-004

- **Status**: accepted (owner decisions, 2026-11-15; Round 13 — docs-only: the contract is
  normative, the implementation is a later round per `design/PLUGIN-SPEC.md` §14)
- **Date**: 2026-11-15
- **Revised**: 2026-11-15 (R13-04d, before publication) — the ten MUST-FIX items of the codex FINAL
  cross-review are closed in place: F1 fallback dispatch, F2 dsh request/response ports, F3
  out-of-grant speech, F4 multi-target admission preflight, F5 `required` + frame overflow, F6
  `binding.harness`/`profile`, F7 digest coverage, F8 handshake identity + admission epoch, F9
  settle-gate synthetic-input failure, F10 dependency claim + SHA-256. **F2, F5 and F6 narrowed the
  design** (two dsh rows removed, quarantine made single-mechanism, harness/profile scopes removed);
  none widened it. The row-by-row closure is *Appendix B*. Append-only discipline is intact: ADR-004
  is untouched, and this file was still pre-publication when it was corrected.
- **Further revised**: 2026-11-15 (R13-04e, still before publication) — the codex **CONFIRMATION**
  review accepted all ten FINAL rows above and opened two blockers, both closed here as **corrections**
  that narrow nothing: **F11** (the §8.1 worked example contradicted the narrowing rule it illustrates —
  the example now lists both instances in `scopes.server` and narrows through `worker.<id>.use:`, and D26
  pins `use:` as a *filter* over `scopes.server`) and **F12** (one multi-target `admit/pre` *evaluation*
  fans out to per-target *frames* carrying each target's own identity — D24). The row-by-row closure is
  *Appendix C*; Appendix B is left exactly as accepted.
- **Scope**: `kian-plugin` (new `hooks` module: bus, ports, registry, sidecar runner; one new
  **workspace** dependency edge, `sha2`), `kian-loop` (request/response/tool/recovery ports; **new
  dependency edge** on `kian-plugin`, with the bus instance injected at construction), `kian-dsh`
  (`tool_post` through a **crate-local** port trait injected at driver construction; **no new edge** —
  its narrow-waist test forbids one), `kian-superagent` (admission and settle
  ports; edge already exists), `kian-cli` (config `sidecars:`, assembly, `KNOWN_PLUGINS` collision
  check), `design/PLUGIN-SPEC.md` (WHAT), `design/DESIGN.md` §6.4 (pointer). **No `kian-core`, no
  `kian-store`, no `kian-orchestrator`, no new event kind, no new table, no new HTTP route, no new
  ops method, no new metrics key.** One EVENT-MODEL payload **value** is required and deferred to the
  implementation round (D23).
- **Supersedes**: **nothing.** This ADR **extends** [ADR-004](ADR-004-cordis-inspired-plugin.md) and
  leaves every one of its decisions standing: static/compile-time registration, the `Plugin`/`Ctx`/
  `Disposer` runtime, the five dispatch modes, the rejection of dylib/HMR, and the deferral of wasm.
  ADR-004 is edited by nothing in this round; the file is unchanged on disk.
- **Related**: `design/PLUGIN-SPEC.md` (the hook-plugin contract, WHAT — this ADR is its WHY),
  `design/DESIGN.md` §6.1–§6.3 (plugin system; §6.4 is the new pointer) and §3.2 (the additive
  payload-key discipline), `design/EVENT-MODEL.md` (22 kinds; unchanged except the deferred value),
  `design/WORKER-MODEL.md` §3/§6/§7 (per-id sides, security register, version policy),
  ADR-010 (dsh bridge), ADR-015 (tool permission provenance), ADR-020 (durable worker binding),
  ADR-024 (resident per-episode budget), AGENTS.md constraints 1/2/3/5/6/7/8/9/10/11.

## Context

ADR-004 froze plugins at compile time and named a rollback trigger for revisiting that: third
parties must be able to extend behavior **without touching kian source**. It also deferred wasm. Two
facts have since accumulated:

1. **A recurring in-tree need.** Tapping the execution lifecycle from outside the loop — admit,
   turn, tool, settle — has been wanted twice, once for the built-in ReAct harness
   (`crates/kian-loop`) and once for the dsh bridge (`crates/kian-dsh`, ADR-010). Written as
   compile-time plugins it is the same code twice, inside the binary, unremovable at runtime.
2. **The lifecycle is already modelled.** Admit→Wake→Drain→Promote gives the points a name and a
   home; the engine, the loop and the wrapper already sit at the boundaries a hook would need.

A v1 draft of `design/PLUGIN-SPEC.md` proposed three tiers (compile-time / stdio sidecar /
wasm-gated) with per-worker plugin sets and a hook vocabulary. External review (codex, 2026-11-15)
returned **FAIL** with 21 findings (6 blocker, 12 major, 3 minor), and a repository-backed
self-review added 11 more. The failures were not stylistic. In summary:

- the draft promised a **durable per-session plugin set** with no event to hold it (blocker);
- it promised **hook context mutation** at tier 0 and claimed purity would preserve replay and the
  provider prefix cache — a category error: purity of *today's* body says nothing about historical
  prefix bytes, and mutation is the one thing constraint 3 forbids (blocker);
- it promised a **settlement veto** that could hold a session open forever and could obstruct honest
  non-completion (blocker);
- it placed `admit/pre` "before an inbox row" but sourced the bus from "drain control flow", so the
  bus could not be called where the hook had to fire; and it claimed `tool/pre` and `settle/pre` were
  implementable through existing surfaces which, in the tree, **do not exist**: the dsh bridge has
  no pre-effect tool port, and the engine calls `end_session` on a concrete `DrainCtx` (blockers);
- the ABI had no closed per-hook payload schemas, no ordering identity, no outcome matrix;
- a veto's honest error result **is** model context, so the draft's "never changes model-visible
  bytes" claim was false as written;
- `restarts_per_window` had no window, quarantine was described as "until an operator reloads" with
  no reload surface permitted, `optional` existed in prose but not in the manifest, and `static_only.rs`
  was described as proving things it does not prove.

The owner then locked rulings D2-amended, D8 and R1–R16 (Kanban `t_41bfc314`, 2026-11-15) which
bindingly resolve the architectural questions and are adopted here as decisions D2, D8 and the
mechanism choices in D14–D26.

A second, FINAL cross-review (codex, 2026-11-15) closed baseline findings 1 and 3–21, left **#2**
unfixed and raised seven new ones, producing **ten MUST-FIX items** (F1–F10) with owner rulings on each.
They were closed in place before publication; *Appendix B* is the row-by-row table, and three of them
(F2, F5, F6) narrowed the design rather than correcting prose.

### Why a sidecar before wasm

This is the decision most likely to be revisited, so the reasoning is on the record:

- **ADR-004's trigger is a policy-extension demand, not an isolation demand.** "Third parties may
  extend behavior without touching kian source" is satisfied by any out-of-process mechanism. Wasm is
  needed only when the third party is **untrusted** and needs confinement — a different requirement,
  with a different cost (host-function interface design, resource limits, runtime pinning, cold-start
  measurement). Conflating the two is what made the first draft promise a tier it could not describe.
- **A child process costs nothing ADR-004 rejected.** dylib/HMR was rejected because the Rust
  dynamic-loading ABI is unstable in-process. A child process has no in-process ABI at all, so the
  objection does not apply, and stdio JSON-lines is a carrier kian already operates (the dsh bridge,
  ADR-010) and already tests.
- **Wasm's own evidence argues for waiting.** Wasmtime's documentation is explicit that sandbox
  escapes are possible and should be treated as high-severity bugs; a WasmEdge CNCF assessment names
  sandbox-escape risk; Lucet — the historical in-process option — is end-of-life (2025-11-06). A
  security tier adopted before an isolation requirement exists buys risk, not safety, and its cost
  (WIT world design, host shims, AOT/pooling, cold-start measurement) would be paid against an
  unmeasured need.
- **The tier boundary is a trust statement, and v2 states it honestly**: tier 1 is an
  **operator-trusted extension** with a crash/protocol boundary — the same trust class as an
  `endpoints:` address. Real confinement is out of scope (D18). D8 records wasm as non-normative
  future work with its own trigger.

## Decision

Decisions D1–D8 are the owner's locked decisions (cited, not re-litigated). D9–D27 are the
mechanism decisions this ADR makes inside those rulings; each carries its rationale, and each is
implemented as a normative rule in `design/PLUGIN-SPEC.md` (section cited).

**D1 — Static-first is non-negotiable (owner D1).** Tier-0 compile-time plugins remain the only tier
that may occupy a `Ctx` service key. No dynamically loaded library, ever. → SPEC §2, §5.3.

**D2 — Tier 1 is hook-subscription-only, and v1 has no context mutation at any tier (owner D2, as
amended).** A sidecar may observe and, where a hook allows, veto. It may never provide a seam, and no
tier may mutate a hook view or a request body in v1: `turn/request` is observe-only for tiers 0 and
1, and `mutate:context` is removed from the capability set (the name stays reserved and is refused).
Rationale: mutation is the single largest cost hazard in the system — it rewrites history's bytes,
breaks replay identity and invalidates provider prefix caches (constraint 3) — and no v1 use case
requires it. A future prefix-safe transform grammar needs its own ADR. → SPEC §3.3, §5.1, §12.

**D3 — No new event kind, table, HTTP route, ops method or metrics key (owner D3).** Every
model-visible effect rides an existing honest path (`tool/result`, a synthetic `input/admitted`, an
absent inbox row) and every other effect is a `tracing` audit line. One EVENT-MODEL payload **value**
is required and is named for the implementation round (D23); it is not added here. → SPEC §0, §4.4,
§10.6.

**D4 — The hook bus lives in `kian-plugin::hooks` (owner D4).** Nothing lands in `kian-core`,
`kian-store` or `kian-orchestrator` — no trait, no trait object, no field. Verified current edges:
only `kian-superagent` and `kian-cli` depend on `kian-plugin`. → SPEC §4, §5.3.

**D5 — ADR-028 extends ADR-004 and never edits it; PLUGIN-SPEC owns WHAT, this ADR owns WHY (owner
D5).** Append-only ADR discipline, same as every supersede in this repo. → this file, header.

**D6 — Round 13 is docs-only (owner D6).** `design/PLUGIN-SPEC.md` + this ADR + a pointer in
`design/DESIGN.md` §6.4 + one docs-map line. No `crates/` change, and no prose may imply a crates
edit lands "in the same change" (the draft did; finding S01). Implementation work is listed in SPEC
§14 as future, owned by crate. → SPEC §14.

**D7 — The config surface is a new `sidecars:` section, fail-closed, collision-checked; `plugins:`
semantics are untouched (owner D7).** `plugins:` stays the five-name `KNOWN_PLUGINS` list
(`config.rs:2231`, fail-closed at `:2432`); instance names must not collide across `plugins:`,
`sidecars.plugins` keys and tier-0 plugin names. → SPEC §6.2, §8.1, §13.1.

**D8 — Tier 2 (wasm) is removed from the normative contract (owner D8).** PLUGIN-SPEC v2 specifies
two tiers. Wasm is recorded here as a **non-normative future direction** whose trigger is distinct
from ADR-004's: **untrusted third-party demand for isolation**, not the policy-extension demand tier 1
already satisfies. This resolves the draft's internal contradiction (it claimed sidecars answer
ADR-004's rollback trigger while also claiming wasm arrives only when "its" trigger fires). If that
trigger opens, the research defaults are recorded in the round's security grounding memo §6.2 and the
track starts with a new ADR — not by editing this one's normative sections. → SPEC §2, §16.6.

**D9 — The second tier is a process boundary, not a sandbox, and it comes before wasm.** Rationale in
*Context*. Operationally: a sidecar runs as the same OS user with full reach; the stdio pipe is a
crash and protocol boundary. → SPEC §10.1.

**D10 — No dylib, ever (reaffirms ADR-004 with fresh evidence).** The Rust dynamic-loading ABI remains
unstable; `static_only.rs` keeps `libloading`/`dylib*`/`wasm*` out of `kian-plugin`'s normal tree.
Cost of the alternative (see *Alternatives A*) is a permanent in-process ABI compatibility burden for
a capability that a child process already delivers. → SPEC §2, §13.2.

**D11 — The ports are injected callables at named, existing call sites — not decorators around
concrete types (resolves findings C05/S08; corrected by F2).** The specification names seven port
methods over plain-data views, each with its owning crate and its *today's* call site: admission
(`subscriptions.rs:477`, API side `server.rs:1183`), request (`react.rs:537`), response
(`react.rs:520`), tool-pre (`react.rs:676`, after the declarative permission gate and before the
`tool/call` append / `invoke` at `:710`), tool-post (`react.rs:710`; under dsh `driver.rs:650`),
settle (`worker.rs:1276`), recovery (`react.rs:787`). Dependency shape, stated as it will land:
`kian-loop` gains an edge on `kian-plugin` **and** receives the bus by injection at construction — the
edge is unavoidable because the loop must construct the views, so "injection only" would be a fiction;
`kian-dsh` gains **no** edge and reaches `tool_post` through a crate-local trait over a locally declared
struct, adapted in assembly, because `tests/narrow_waist.rs` asserts `kian-core` as its only kian
dependency. The two **dsh `turn/*` rows are removed** by F2: `mapping.rs:411` is a mapping function
that runs before the driver's append (`driver.rs:650`), and no bridge site can fill
`message_count`/`tool_names`/`tool_schema_sha256` from an opaque child body or `usage`/`error` for the
response view, so those rows were phantom ports rather than narrow ones (SPEC §3.4.1, §3.5, §16.17).
Rationale: an abstraction that does not name a real call site is a promise, not a design — and three of
the draft's promises had no call site at all.

**D12 — The settle gate lives at the loop-outcome boundary and acts by admitting one synthetic input
(resolves S08, the draft's hardest finding).** The engine derives `end_reason = Completed` from
`Supervised::Settled` and then performs one atomic append via a method on the concrete `DrainCtx`
inside `kian-orchestrator` (`engine.rs:1463`, `:728`). Therefore:

- a decorator wrapped around `DrainCtx` **could not** intercept that call, and adding a trait object
  to the orchestrator would violate D4 — so R4's phrasing ("a `DrainCtx`-boundary decorator") is not
  implementable as written and **is deliberately not followed literally**; the intent it protects
  (zero `kian-orchestrator` dependency on `kian-plugin`) is preserved exactly;
- the gate sits in `WorkerLoopDriver` (`kian-superagent`), the `LoopDriver` decorator that already
  owns terminal-outcome decoration and already holds an `InboxWriter` — and therefore **only on the
  sessions that wrapper is mounted on**. It is mounted on worker sides only (`plugins.rs:1083`, `:1165`,
  `:1214`); the Router (resident) session runs under `RouterDriver` (`plugins.rs:1061`, `:1073`) and the
  built-in/memory assembly under `ServeLoopDriver` (`server.rs:647`, `:654`, `:701`), so neither has a
  completion gate in v1 (SPEC §3.4.4, §15, §16.16). That is a coverage limit of the chosen mount site,
  not a second mechanism — and it is stated so no plugin claims a gate it does not have;
- a denial is expressed by **admitting one synthetic `input/admitted`** (the existing steer path,
  `synthetic:true`, strippable, persisted before the model sees it). The engine's own "settle on an
  empty inbox" logic then re-arms the episode — no new mechanism, no event kind, no engine change;
- **if that admission itself fails, the veto is not swallowed (F9)**: the gate propagates the append
  error instead of `Settled`, so the drain takes the ordinary append-failure route and the session ends
  recoverably (`failed`/`interrupted`) rather than `completed`; the veto counts as consumed in the
  gate's in-process counter (the stream cannot see a row that does not exist), and one audit line says
  *applied and unpersistable*. "Veto silently dropped, settle anyway" is precisely the dishonest
  outcome this rule forbids (SPEC §4.6);
- the atomic end write is never split (constraint 4): the gate decides *whether* an end happens this
  episode, not *how* it is written.

**D13 — The verdict vocabulary is `deny` or nothing.** No `ask` (a drain has no attached human; an
"ask" verdict has no addressee without a new wire surface, D3). No `allow`-as-opinion (an extension
must never manufacture permission; dropping it also removes a precedence trap — with several
subscribers, "most restrictive wins" is the only safe rule, and deny-only is that rule with fewer
words). No mutation. Every reply variant names the capability it requires, enforced at dispatch.
Rationale: a verdict vocabulary is a security surface; each extra verb is a new way to be wrong, and
`ask` in particular would quietly reintroduce the interactive-approval assumption kian's drain model
does not have. → SPEC §3.3, §5.

**D14 — Failure policy is asymmetric by hook class, the asymmetry is the contract (owner R9), and it
applies to *unreachability only* (F3).** Action hooks **fail closed** on deadline/crash/quarantine — an
approval gate that admits when it is unavailable is not a gate. "Fail closed" is the default of a
**veto-capable** subscription, and in v1 `admit/pre` is veto-capable only for channel-borne input
(D24, R3): at an observe-only origin (API `submit`/`queue`/`steer`) — and for an observe-only
subscriber at *any* hook — an unavailable handler costs the observation and the input/action proceeds
(SPEC §11.1, §3.4.3). The settlement gate **fails open** — a wedged quality gate must not hold a
session hostage, and terminal outcomes are protected (D15, constraint 8). Observers lapse.

**Speech is a different class and never resolves into a default.** A reply that is out of grant
(`err:unauthorized`), illegal for the hook's mode, or otherwise malformed is **discarded**: the outcome
is exactly the as-if-absent outcome, never the fail-closed class default (F3, resolving the final
review's NEW-1). The v2 draft resolved these to the class default, which meant a manifest holding only
`observe:tool` could have a tool call denied by sending a `deny` it had no grant for, or by hanging —
privilege escalation through a failure mode, contradicting D2/§10.2's non-escalation claim. The two
classes are now disjoint by rule: unreachability → class default (per grant); speech defect → discarded
opinion + audit, with health accounting (§6.4's failure window) kept deliberately separate from outcome
accounting. A handler that *says* it is unreachable (`err{deadline|crashed}`) lands in the first class
and gains nothing, since it could always force the default by silence.

Rationale for the asymmetry itself: Envoy reaches the same conclusion per filter (`failure_mode_allow`
is a per-filter choice, never a global default); a global rule in either direction would be wrong for
one of the two classes. → SPEC §5.2, §8.3, §11.1.

**D15 — The completion veto is bounded, re-armable, and re-derivable from the stream (owner R2).** A
veto applies to `completed` only — concretely, to `LoopOutcome::Settled`, the loop's own completion
decision; a harness-declared stop (`Stopped`) is not a settle, and `interrupted`, `failed` and user
`cancelled` never reach the gate at all, so no plugin can obstruct an honest stop (constraint 7). The
veto budget (`max_settle_vetoes`, default 3) is counted from the `[kian:hook-veto …]` synthetic inputs
already in the session's stream, so it survives crash and resume without a side table; exhaustion
settles with one audit line recording the override. Rationale: this is the evidence gate's bounded-stop
discipline extended to a second stop condition — bounded, one-shot-guarded, and honest when it gives
up. → SPEC §4.6, §3.4.2.

**D16 — One sidecar process per plugin per server; one in-flight dispatch; busy means fail-fast, never
queue (owner R11).** Every dispatch carries session id, worker id, `seq`, `epoch` and a dispatch id —
no implicit identity, so a global process cannot bleed one worker's decision into another. Sidecar
state is cross-session disposable cache, and veto verdicts must be pure functions of the payload with
an auditable reason. Rationale: a dispatch queue per sidecar would convert one slow plugin into
head-of-line blocking across every worker on the server; failing fast to the class default keeps the
degradation local and honest. → SPEC §7.5, §8.6.

**D17 — Set identity is deterministic re-derivation plus a digest guard whose coverage is one process
lifetime (owner R1), not durable pinning; and the derivation inputs are narrowed to what is durable
(F6), with the digest covering the whole behaviour-bearing set (F7).** The effective set is a pure
function of exactly two things — the config snapshot and the durable worker binding (`exec_worker`,
already durable per ADR-020) — and **nothing else**: the four-scope grammar's middle layers
(`scopes.harness`, `scopes.profile`) are removed in v1 because `session/created` carries `{}` and no
kind records a harness or profile (EVENT-MODEL §3), so a set that depended on them could not be
recomputed at claim from durable state. Per-worker sets survive through the worker scope's `use:` list
and per-instance `limits:` narrowing; "only under dsh / only for the router profile" is expressible
only via worker-id scoping or `required = false`, and closing that gap needs a durable harness/profile
record (SPEC §16.18), not a new resolution rule. A canonical digest is recorded in one audit line at
session open and recomputed at every claim that *same* process serves; a mismatch quarantines that
session's sidecar hooks for the process's lifetime, with the diff audited. The digest's field list is
**closed and complete** — identity (name/version/abi), the resolved scope, `order`, `required`, the
binary digest, a hash over `argv`+`env_allow`+`cwd`+startup budget, the grant/hook sets and every
limit key — so a plugin with different arguments, environment or restart behaviour can never share an
identity with another (SPEC §8.5). Durable pinning would need an additive payload on a session-start
event, which D3 forbids this round. The honest limit, stated because the v2 draft drew the wrong
conclusion here: a `tracing` line is not a store, so after a cross-process restart nothing is compared
— the new process re-resolves from the current config and **v1 cannot detect that the config
changed**, which is exactly R1's case. The guard's real value is as a fail-closed invariant check
*inside* one process lifetime (an operator's config edit changes nothing until `kian serve` restarts,
since there is no live reload). Closing the restart case is durable pinning, owned and deferred as
SPEC §16.2 / §9.4 — it is not something the guard may silently imply. → SPEC §8.1, §8.2, §8.5, §9.1,
§9.2.

**D18 — Tier 1 is an operator-trusted extension; its artifacts are digest-pinned.** The trust class is
that of an `endpoints:` address: same OS user, full reach, no sandbox claim (D9). Proportionate
controls for a self-hosted single-operator deployment: absolute `argv` (no shell), `env_clear` +
exact allowlist, no kian secret may be allowlisted, dedicated cwd, closed inherited descriptors,
killable process group, bounded frames/channels/stderr, and a **required** SHA-256 digest verified at
spawn and every respawn. Rationale: a digest closes artifact-integrity (the "the bytes are the ones I
approved" question) with no operational surface; publisher identity (cosign) only matters when
artifacts cross a publisher boundary and is deferred. Stating the trust class plainly is itself a
control: it stops reviewers and operators from treating the sidecar as confinement. → SPEC §10.

**D19 — No executor in `kian-plugin`: one dedicated std thread per sidecar, `futures` oneshot for the
async side (resolves S10), and exactly one added dependency edge for digests (F10).** `kian-plugin`
has no normal `tokio` dependency (tokio is dev-only) and drain tasks must never block. So the runner is
a plain `std::thread` per plugin doing blocking `std::process` + `std::io`, enforcing deadlines itself
with `Instant`; the awaiting bus side holds a `futures::channel::oneshot` and needs **no timer at all**,
because the runner guarantees the oneshot resolves (reply, deadline, crash or stop). No tokio worker
thread ever blocks on a pipe.

SHA-256 is **not** implementable from `std` here, and this ADR does not pretend otherwise (F10): the
manifest digest, the payload digests and the §8.5 set digest use the workspace's already-pinned
`sha2 = "0.10"` (`Cargo.toml`, `[workspace.dependencies]`) — the same crate `kian-store`,
`kian-orchestrator`, `kian-llm`, `kian-superagent` and `kian-cli` already link, so the lockfile gains
no package and the implementation adds exactly one line (`sha2.workspace = true`) to
`crates/kian-plugin/Cargo.toml`. `tests/static_only.rs` refuses `libloading`/`dylib*`/`wasm*` only, so
`sha2` is outside its refusal list **by design** (it is a pure-Rust digest with no loading or
runtime-code surface) and that test stays untouched. The corrected claim is therefore: **zero new
third-party crates, one new workspace dependency edge** — the earlier draft's "zero dependency-tree
entries" was false as written. → SPEC §2, §7.6, §8.5, §13.2.

**D20 — Tier-0 hook registration is hook-scoped, in `kian-plugin::hooks` (owner R13).** `HandlerId`
carries plugin name, hook id, grants and an order key; registration returns a `Disposer`; process-start
registration is server scope and session-scoped registration participates only in its sessions. Raw
`Ctx::on*` stays for non-hook typed events (ADR-004 unchanged) — the existing dispatch keys handlers by
kind and `TypeId` and carries no plugin/hook/session metadata, so reusing it would have forced harness
metadata into the core registry. → SPEC §5.3.

**D21 — Cross-harness reuse means the shared vocabulary subset, and the matrix says so (owner R14,
narrowed by F2).** A plugin subscribing only points that fire under both harnesses is one binary with
one payload shape — in v1 that subset is `admit/pre`, `admit/post` and `tool/post`. `tool/pre` is
ReAct-only (`surface:"react"`) and `recover/decided` has no dsh site in v1; `turn/request` and
`turn/response` are removed from the dsh column because the bridge cannot populate their normative
payloads (F2, SPEC §16.17); dsh-child tool calls are outside the contract entirely. Rationale: a
bridge cannot lend a power its harness does not have — "loads everywhere" is not "behaves the same
everywhere", and the honest matrix lets a manifest choose `required = false` instead of discovering
the difference in production. → SPEC §3.4.1, §3.5, §16.17.

**D22 — The model-visible byte guarantee is "identical prefixes up to the first policy divergence"
(owner R8).** Tool schemas, tool order and request bodies are identical across worker ids and plugin
sets (no hook adds bytes to a request in v1). A denial legitimately diverges *after* the hook, and deny
bytes are stable for identical inputs. Rationale: the draft's stronger claim was false — a denial is
model context — and a false cache guarantee is worse than a precise weaker one, because it hides the
one place histories can legitimately differ. → SPEC §12.

**D23 — A hook denial rides the existing `permission` provenance; one EVENT-MODEL value is required
and deferred (owner R6).** Ordering is normative: declarative permission gate first, hook second; the
attempted `tool/call` is appended in both arms (log-before-execute survives hook denials); a denied
call never invokes; the model sees an honest-error `tool/result`. `tool/call.permission` already has
`evaluated`/`policy`/`matched`/`reason`, but `policy` is the closed set `default|configured`, so a hook
denial needs the value `"hook"` (with `matched = "<plugin>:<hook>"`). That is an EVENT-MODEL change and
therefore belongs to the implementation round, in the same change as the code — this round only names
it. → SPEC §4.4, §16.1.

**D24 — Admission policy attaches at the existing pre-admission boundary; the veto is channel-only and
audit-only in v1 (owner R3, resolves C04/C14).** The port sits where the bundle already shapes channel
input into the payload that will become the row (`subscriptions.rs:477`), so the control plane stays
dumb (constraint 9) and `admit-before-wake` is never at risk: a veto means no row was ever written, so
there is no wake and no model turn. Veto availability: channel-borne inputs only. **No sender is
notified, and no NACK surface exists to notify one** — the channel seam has inbound `subscribe()` and
outbound `send()` only (`seams.rs:344-351`), the fan-out's failure path is a `warn!` that leaves the drop
to the adapter's idempotent retry (`subscriptions.rs:520-534`), and `SubscriptionService::spawn` turns a
handling error into one more `warn!` (`:568-575`); a redelivery is re-vetoed, not admitted. The refusal
is therefore an audit line plus a row that never existed. API-borne `submit`/`queue`/`steer` are
observe-only (SPEC §16.3), so an unavailable handler there costs the observation and nothing else —
the input is admitted unchanged, audited — because the class default belongs to a port that can act
(SPEC §11.1 states both rows). A sender-visible refusal — the channel NACK or an API error code — is a
wire, which D3 forbids this round, so it is deferred with its owner (SPEC §16.15). Internal producers
(watchdog timers, async completion, memory injection, subscriptions) do not pass the port at all and are
recorded as a limitation, not glossed over. → SPEC §3.4.3, §4.1, §11.1.

**The multi-target case is per-target frames on one evaluation (F12).** `admit/pre` is the one hook
whose evaluation spans several sessions, while effective sets, narrowed limits and quarantine are all
per **session** — so the event's single evaluation fans out to **one `hook` frame per (subscriber,
target session)**, each carrying that target's `session`, `worker`, `seq` and post-narrowing
`deadline_ms`, with its own frame id for reply correlation, sent one at a time by that sidecar's
single-in-flight rule so one event's frames are a sequence and never a busy-path collision (§7.5). The
verdict stays global: the first `stop` on any frame refuses the admission for **every** target, no inbox
row is written anywhere, and the audit line names the denying plugin, the denying target and the size of
the refused set; a target whose resolution has no subscriber for the hook simply gets no frame, which is
a resolution result and not an error. The alternative — one frame carrying a representative identity
plus a target list the plugin must untangle — was rejected because it makes every plugin re-derive which
target a verdict belongs to, i.e. infer identity from state, which R11 forbids; and routing the whole
fan-out through one target's identity would have made one worker's deadline and one session's set
silently govern another session's admission. → SPEC §1 (Dispatch), §3.1, §3.2.1, §4.1, §7.2, §7.3,
§8.3, §10.6.

**D25 — The manifest is a closed schema; validation and refusal are per instance; quarantine is for the
process lifetime and has one window plus two immediate triggers (owner R12/R16, corrected by F5).**
Unknown keys refuse; every limit has a clamp; a `ready` mismatch is a typed refusal, and the `ready`
frame must restate the full handshake identity (canonical manifest path + name + version + `abi`, F8);
quarantine lasts until the server restarts — there is no live reload and no un-quarantine ops method,
because D3 forbids new ops surface. Two corrections the final review forced:

- **`required` is per plugin instance, never per section.** Each instance's own manifest decides
  whether *its* refusal is fatal to `kian serve`; there is no whole-section refusal, so one broken
  plugin never unmounts the others, and a manifest too malformed to declare `required = false` is
  fatal (it cannot opt out of a rule it cannot parse).
- **Frame overflow is immediate, not windowed.** `frame_overflow` (either direction) and
  `protocol_violation` quarantine on the first occurrence; the failure *window* counts only
  rate-shaped failures (deadline, crash, malformed reply, unauthorised reply, stderr-rate). The draft
  had `frame_overflow` counted both ways, which is not a rule. Stderr and frame bounds are symmetric
  and normative. Rationale: the draft had three inconsistent descriptions of one lifecycle (a restart
  counter, a "consecutive crashes" rule and "until an operator reloads" with no reload surface); one
  state machine, stated once, removes the class. → SPEC §6.1–§6.5, §7.2, §7.3.

**D26 — Resolution is innermost-wins-by-name and narrowing only; v1 carries two scopes, because the two
middle layers have no durable source (owner R15, narrowed by F6).** `server` < `worker` id in v1; a name
resolves to exactly one instance (no shadow copies, no double subscription); worker scope may only
narrow (subset + smaller limits) and may never widen beyond manifest grants — concretely, the worker
layer is a **filter** over `scopes.server`: `worker.<id>.use:` can remove a name, and a name it
restates is declared at the worker layer (that is the scope the set digest records), so a per-worker
set is expressed by *omitting* names and never by adding them (F11); ordering is by
`[plugin].order` then name, never config-file order; worker-id scopes resolve lazily but their shape is
validated at boot. **`harness` and `profile` scopes are removed**: `session/created` carries `{}` and no
event kind records either value, so a set that depended on them could not be recomputed at claim from
durable state — which is exactly what D17's guard requires. Per-worker sets remain fully expressible
(worker `use:` narrowing, per-instance `limits:`), "only under dsh / only for the router profile" needs
worker-id scoping or `required = false`, and the narrow is recorded with its reopening trigger as SPEC
§16.18. Rationale: every industry precedent found (Codex first-match, Envoy most-specific,
profiles/injection per instance) agrees on named scopes and deterministic resolution and disagrees on
the algorithm — so kian states its own rule rather than claiming a standard; and a scope whose key is
not durable is a resolution input that cannot be audited after a restart. → SPEC §8.

**D27 — The `harness/<name>/*` namespace ships empty in v1.** The namespace, its validation and its
refusal rule exist; no harness declares a point, so a `harness/…` subscription is a boot refusal, not
a silently-never-firing hook. Rationale: an unconstrained wildcard would let a manifest claim a
capability nobody implemented, and inventing a point for a harness the design did not need would be a
promise without an owner. → SPEC §3.6, §16.11.

## Alternatives considered

**A — Dynamic loading (dylib/cdylib `Plugin`).** Would give true third-party runtime extension without
a process. Rejected: the Rust dynamic-loading ABI is unstable, so every plugin build would pin against
a kian-internal symbol layout and every kian refactor would be an ABI break for someone else's binary
— the exact instability ADR-004 rejected, now with a wider blast radius (a dylib panic or symbol clash
is in-process). A child process delivers the same capability with a carrier kian already operates, at
the cost of serialization and a round-trip.

**B — Pure configuration: no hooks, extend the declarative tables instead.** Recovery policy is
already declarative; tool permission is already a table. Rejected as the *general* answer: a policy
table cannot express "call this external system and wait for a verdict", and the recurring need is
precisely the one that does not fit a table (an approval gate). It remains the right answer for
policy-shaped behavior — which is why D24/D14 keep policy declarative and let a hook only deny.

**C — Wasm first (implement the sandbox tier now).** Rejected for cost and timing (see *Context*): it
implements confinement for a threat model kian does not yet have, and the evidence base
(Wasmtime escape caveats, WasmEdge assessment, Lucet EOL) argues for starting that track only when an
untrusted-third-party requirement exists, with its own ADR and its own measurements.
Recorded as D8's non-normative future direction.

**D — In-process only: extend the existing `Ctx` typed-event dispatch to the lifecycle, no sidecar
tier.** The cheapest option, and it would satisfy harness-lifecycle hooks. Rejected because it fails
the actual requirement: a compile-time-only mechanism cannot be removed or configured at runtime, and
"without touching kian source" is the demand ADR-004 itself named. It also would have put
plugin/hook/session metadata into the core registry (`dispatch.rs` keys by kind and `TypeId`), which is
the wrong crate for it. Kept as tier 0 — with a hook-scoped registry beside it (D20), not inside it.

**E — A `DrainCtx` decorator for the settle gate (the ruling's literal wording).** Rejected as not
implementable: the engine calls `end_session` on the concrete `DrainCtx` it owns (`engine.rs:1463`),
so wrapping the value cannot intercept the call, and adding an injection point inside
`kian-orchestrator` would make the orchestrator depend on the bus (D4). Chosen alternative: gate at
`WorkerLoopDriver`'s outcome boundary and express denial as one synthetic admission (D12) — same
guarantees, zero orchestrator surface, and one fewer place where an end write could be split.

**F — gRPC or HTTP transport for the sidecar instead of stdio JSON-lines.** Gives typed schemas,
streaming and standard deadlines. Rejected for v1: it adds a network listener surface, TLS/auth
questions and a dependency footprint to a component whose whole value is "no new surface and, in
dependency terms, no new package"; and the dsh bridge already proves the stdio JSON-lines carrier in
this codebase. Revisitable if a plugin ever needs multi-tenant remote deployment — which is also when
D8's wasm/remote track opens.

**G — One sidecar process per session (or per worker).** Considered for isolation and state clarity.
Rejected: process spawn cost and memory multiply by session count, and per-session state would invite
plugins to hold session truth outside the event stream — the opposite of the disposable-cache rule
(D16). Recorded as a deferred measurement (§16.14).

**H — No settlement gate: express the need as a config rule ("require evidence gate pass, else
fail").** Rejected: completion conditions that depend on external judgement (a reviewer service, a
test suite exit code) cannot be expressed as a class→action table entry, which is why R2 asked for a
bounded veto instead. The bound (D15) is what makes it safe; the gate is the smallest mechanism that
answers "not done yet, and here is why".

## Consequences

**Gained**

- Lifecycle extension points that exist **once**, at named call sites, usable by both harnesses with
  one vocabulary and one payload shape (SPEC §3.4.1) — the "one plugin, many harnesses" goal made
  checkable rather than aspirational.
- Per-session plugin sets with deterministic resolution and a fail-closed identity guard, without a new
  event, table or config semantics change for `plugins:`.
- A trust and failure model that is stated once and honestly: operator-trusted sidecars, fail-closed
  action hooks **for veto-capable subscribers only**, a discarded (never escalating) class for
  out-of-grant speech, fail-open settlement, bounded vetoes, process-lifetime quarantine with two
  immediate triggers, no fake sandbox.
- No dynamic-loading crates and `static_only.rs` untouched; **zero new third-party packages**, because
  the one added edge (`sha2`) is already a workspace dependency of five other crates.
- Model-visible effects all ride existing honest paths: a denial is an honest error result, a blocked
  settlement is a synthetic steer input, a vetoed admission is a row that never existed.

**Given up**

- **Post-effect gaps that the contract now states instead of hiding**: no dsh-child tool veto, no dsh
  `turn/request`/`turn/response` observation (F2 removed both rows rather than approximating their
  payloads), no dsh recovery observation, no API-borne admission veto, no per-chunk interception, no
  transform. A user who needs any of these must wait for the named deferral (SPEC §16) — the contract
  does not pretend otherwise.
- **No harness- or profile-scoped plugin sets** (F6): the effective set is a function of the config
  snapshot and the durable `exec_worker` id only, so "only under dsh" is expressed by worker-id scoping
  or `required = false` (SPEC §8.2, §16.18).
- **No sender-visible refusal on the admission path in v1**: a channel-borne veto is an audit line and a
  row that never existed; the sender is never told, because there is no inbound refusal wire (SPEC §4.1,
  §16.15).
- **No completion gate on the Router (resident) session or on the built-in/memory assembly in v1**: the
  settle phase lives in `WorkerLoopDriver`, which those sessions do not run under (SPEC §3.4.4, §16.16).
- **No durable set identity across a restart**: the §9 guard compares only within one process lifetime, so
  a config change made while the process was down is undetected (SPEC §9.2 step 5, §16.2).
- **A `kian-loop → kian-plugin` dependency edge, plus one workspace edge for digests.** The loop is no
  longer plugin-free, and `kian-plugin` gains `sha2.workspace = true` (already in the lockfile).
  Mitigation: the port trait is plain data, `kian-plugin` is already a leaf-side crate with no execution
  surface, and the loop still receives the bus by injection — so a test can run it with a null bus.
- **A required `sha256` per manifest.** More friction for plugin authors, deliberately: it is the one
  artifact-integrity control that costs an operator nothing at runtime.
- **Hot reload / live un-quarantine** — operators restart the server. Acceptable at
  single-operator scale; wrong for a hosted multi-tenant future, which is also when the wasm track and
  an ops surface would arrive together.
- **A latency budget on the model-critical path** (`deadline_ms`, default 500) that is a policy value,
  not a measurement (SPEC §17). This is a real open risk: if a synchronous `tool/pre` round-trip proves
  expensive at realistic payload sizes, the default must be tuned from measurement, and the first
  candidate mitigation is making observe-only hooks asynchronous while veto hooks stay awaited.

**Neutral**

- `design/DESIGN.md` gains a §6.4 pointer only; §6.1–§6.3 are unchanged. The Footprint Ladder still
  governs: a hook can never justify a new seam, and rung 3 keeps requiring its own ADR.
- The hook vocabulary is deliberately isomorphic to what the industry already teaches (pre/post action,
  session start/end, permission, notification; initialization-then-traffic like LSP/MCP; awaited
  verdicts vs detached observation) so that plugin authors do not have to learn a private dialect —
  with kian's deviations (no `ask`, no `allow`, no transform) stated and justified rather than
  implied.

## Rollback trigger

Reconsider the whole tier if **any** of the following becomes true (each is a measured or observed
condition, not a preference):

1. **Latency**: a warm sidecar's p99 `tool/pre` round-trip exceeds ~5 % of the observed p50 provider
   turn time on a realistic payload, and no server-side mitigation (observe-only hooks detached,
   smaller views, pre-warmed handshake) brings it under. Then: reconsider transport (Alternative F) or
   demote synchronous veto hooks to the evidence gate's config surface.
2. **No adoption**: after the reference sidecar ships (SPEC §14 P5), no operator-authored plugin exists
   outside the repo within two rounds. Then: retier to tier-0-only and delete §6–§9 — the contract
   should not outlive its users.
3. **Security incident**: a sidecar is implicated in a compromise that a documented control would have
   prevented (digest pinning bypass, environment leakage, unbounded frame). Then: fix the control and,
   if the class of risk is confinement rather than coding error, promote D8's wasm trigger.
4. **Invariant breach in practice**: any implementation finds itself needing a new event kind, a table,
   an ops method, or a mutation of a view to make a hook work. Then D3/D2 were wrong, and this ADR is
   superseded (never edited) by one that reopens the question with the evidence.
5. **Config-shape regret**: `sidecars:` proves unmanageable in real deployments (e.g. operators need
   per-session sets, not per-worker), triggering a new ADR for the resolution model (D26).

## Cost impact

**Runtime** — sidecars are opt-in; a deployment with no `sidecars:` section pays nothing but a bus that
is never called (no thread, no process, no timer). With sidecars:

- one dedicated OS thread and one child process (plus its RSS) per plugin instance, independent of
  session count;
- one serialized stdio round-trip per dispatched hook; synchronous on the model-critical path for
  `tool/pre` and `settle/pre`, bounded by `deadline_ms`;
- one canonical-JSON digest per claim (a few KiB of hashing at most — negligible next to the drain's
  own state load), plus one audit line at session open.

**Operational** — a new failure mode operators must learn (quarantine means "restart the server"), a
new artifact class to install and digest-pin, and a new config section. All three are documented in one
place (SPEC §6, §10, §13).

**Maintenance** — one ABI (`abi: 1`) with an additive-only accepted set, one closed manifest schema, one
closed capability set, one closed hook vocabulary with a six-item change checklist (SPEC §18), and one
state machine. The burden the contract carries is the burden of its closures: adding a hook, a
capability or a grant requires updating the document first.

**Code** — not in this round (D6). Estimated shape for the implementation: a `hooks` module in
`kian-plugin` (bus, registry, views, runner), port wiring at nine call sites, a `sidecars:` section plus
validation in `kian-cli`, and one EVENT-MODEL payload value. Dependency edges: `kian-loop → kian-plugin`
(with the bus injected) and `kian-plugin → sha2` (a workspace edge, no new package); **no new external
dependency anywhere**, and `kian-dsh` keeps its `kian-core`-only waist through a crate-local port.

## Appendix A — findings ledger (every finding: fixed or deferred)

External findings keep the reviewer's numbering (C01–C21); repository-backed self-findings are S01–S11
as published in `shared/round13/r13-gap-analysis.md`. "Fixed" cites the rule that resolves it;
"Deferred" cites the SPEC §16 entry **and** the ruling that permits the deferral. No finding is
dropped and none is silently re-scoped.

| ID | Sev | Resolution | Where |
|---|---|---|---|
| C01 | blocker | **Fixed in part; the remainder is owned by §16.2** — durability replaced by deterministic re-derivation from (config, durable binding) + a canonical digest recorded in one audit line + recompute at claim **within the same process** + session-lifetime hook quarantine on mismatch; the cross-process-restart case R1 names is **not** closed in v1 and is now stated as such instead of claimed | SPEC §9.1–§9.4, §8.5; §16.2; D17 |
| C02 | blocker | **Fixed** — no context mutation at any tier; `turn/request` is observe-only; `mutate:context` removed (reserved, refused); the purity-as-cache proof deleted; the request body is not even shipped | SPEC §3.3, §3.2.3, §5.1, §12; D2, D13 |
| C03 | blocker | **Fixed** — per-reason matrix (`completed` only bailable; `interrupted`/`failed`/`cancelled` never reach the gate); veto is bounded by `max_settle_vetoes`, counted from the stream, exhaustion settles with an override audit | SPEC §3.4.2, §4.6; D15 |
| C04 | blocker | **Fixed** — admission port at the existing pre-admission boundary (after shaping, before the first inbox write), control plane dumb, plus the coverage matrix including the internal-producer exclusions and the audit-only nature of a channel veto (no row written, sender not notified — §16.15) | SPEC §3.4.3, §4.1; D24 |
| C05 | blocker | **Fixed** — seven port methods at nine call sites, each with owning crate, exact today's call site, and required dependency edge; the dsh bridge keeps `kian-core`-only through a crate-local port (F2 removed its two unpopulatable `turn/*` rows) | SPEC §4, §4.1–§4.6; D11, D12; §16.17 |
| C06 | blocker | **Fixed** — closed per-hook payload catalog with field-level types, per-variant capability requirements, dispatch/session/worker/seq/epoch identity, reply legality, ordering/idempotency, and the per-hook outcome matrix | SPEC §3.2, §7 |
| C07 | major | **Fixed** — declarative permission first, then hook; `tool/call` appended in both arms with provenance; deny never invokes; honest-error `tool/result`; the one required EVENT-MODEL value named as a same-change implementation-round amendment | SPEC §4.4; D23; §16.1 |
| C08 | major | **Fixed** — every reply variant names its capability; `flow.stop` at admit = veto requiring `veto:admit`; observe-only replies cannot carry a decision; no `mutate:admit` exists | SPEC §3.3, §5.1, §5.2; D13 |
| C09 | major | **Fixed** — guarantee restated as identical prefixes up to the first policy divergence, with deny bytes stable for identical inputs; worker scope may mount observe/veto hooks only | SPEC §12; D22 |
| C10 | major | **Fixed** — normative asymmetry stated per veto-capable port: action hooks fail closed (`tool/pre` → a named honest error; channel-borne `admit/pre` → no row, audit-only), an observe-only origin lapses the observation and admits the input unchanged (R3), the settlement gate fails open, observers abstain; quarantined plugins take the same defaults | SPEC §11.1; D14, D24; §3.4.3 |
| C11 | major | **Fixed** — tier 1 reclassified as an operator-trusted extension (same class as `endpoints:`); redaction is defense-in-depth only; same-OS-user reach stated plainly; real confinement out of v1 scope | SPEC §10.1, §10.4; D9, D18 |
| C12 | major | **Fixed** — one process per plugin per server locked; session + worker on every frame; one in-flight dispatch with busy fail-fast to the class default (never queue); state is cross-session disposable cache; verdicts must be payload-pure | SPEC §6.3, §7.3, §7.5, §8.6; D16 |
| C13 | major | **Fixed** — `restart_window_ms` (default 60000) defined; `required` defined; readiness/refusal behavior; in-flight crash outcome; process-lifetime quarantine; boot after restart clean; no live reload introduced | SPEC §6, §6.2, §6.4, §6.5; D25 |
| C14 | major | **Fixed** — admission veto limited to the channel boundary, and the refusal is audit-only (no inbox row is written; the sender is not notified in v1, because no inbound refusal wire exists); the API-level veto and any sender-visible refusal are separately deferred with owners; no silent *successful* admission is implied | SPEC §3.4.3, §4.1; D24; §16.3, §16.15 |
| C15 | major | **Fixed** — hook-scoped registry (`HandlerId{plugin, hook, grants, order}` + `Disposer`), session-scope filtering, process-global registration = server scope; raw `Ctx::on*` unchanged | SPEC §5.3; D20 |
| C16 | major | **Fixed** — normative hook × harness matrix; reuse defined as the shared vocabulary subset; dsh-child tools explicitly out of contract; request/response/recover dsh behavior stated per point | SPEC §3.4.1, §3.5; D21 |
| C17 | major | **Fixed** — closed YAML grammar (two scopes in v1: `server` + durable `worker`, the harness/profile layers removed by F6), pure resolution pseudocode, ordering/replacement rules, narrowing-only enforcement, worked limits example, boot validation | SPEC §8, §16.18; D26 |
| C18 | major | **Fixed** — tier 2 removed from the normative contract; tiers 0 and 1 only; wasm recorded as non-normative with its own trigger and no v1 ABI/host-function promises | SPEC §2, §16.6; D8 |
| C19 | minor | **Fixed** — bounded stderr (line size, retained rate, drop-and-audit, concurrent drain) and symmetric frame caps with quarantine on violation; failures folded into the lifecycle/outcome matrices | SPEC §7.6, §10.5, §6.4; D25 |
| C20 | minor | **Fixed** — `WorkerLoopDriver` ownership corrected (lineage, evidence gate/verdict, `thread/completed`, Router completion report); mandate issuance named as spawn-path and not attributed to the wrapper | SPEC §4.6 step 1; D12 |
| C21 | minor | **Fixed** — the two triggers are split explicitly: tier 1 satisfies ADR-004's source-independent policy-extension demand; wasm's future trigger is distinct untrusted-third-party isolation demand. ADR-004 not edited | this file (D8), SPEC §16.6 |
| S01 | major | **Fixed** — config/assembly language recast as normative *future implementation* work; the round's change set is this spec, this ADR, the DESIGN §6.4 pointer and one docs-map line; no "same-change crates edit" claim survives | SPEC §14, §13.1; D6 |
| S02 | major | **Fixed** — the `optional` flag is gone; optionality is expressed only as `required = true|false` per instance, and unknown/absent harness points are boot refusals (the `harness/*` registry ships empty) | SPEC §6.1, §6.2, §3.6; D25, D27 |
| S03 | major | **Fixed** — `abi` is the single version identity (`abi` ≡ wire `v`), `ready` set-equality rules defined, plugin `version` reported, audited and included in the set digest; refusal codes closed | SPEC §7.3, §8.5 |
| S04 | major | **Fixed** — one lifecycle state machine, one failure window with two reported numbers (`failures`, `restarts`), explicit consecutive-reset rule, in-flight crash outcome, process-lifetime quarantine | SPEC §6.3, §6.4; D25 |
| S05 | major | **Fixed** — the forever-cached per-id tool side (`WorkerSideFactory`, `superagent.rs:615`) is explicitly *not* the hook mechanism; hook resolution is per session over (session, binding, config snapshot) with the §9 digest guard | SPEC §8.4, §9; D17 |
| S06 | major | **Fixed, then narrowed** — observation timing was made explicit via `timing:"pre_send" \| "post_attempt"` with no claim that a dsh hook precedes or alters the child's provider request; F2 then removed the dsh `turn/*` rows entirely, so `timing` dispatches `"pre_send"` only and `"post_attempt"` is reserved for the deferred site (SPEC §16.17) | SPEC §3.2.3, §3.4.1, §4.2; D21 |
| S07 | blocker | **Fixed (by exclusion)** — dsh-child tool veto is unsupported in v1 and stated as such; `tool/pre` fires only where kian mediates the call (`surface:"react"`), and the matrix says `no` under dsh rather than converting a post-hoc mapped `tool/call` into a claimed pre-execution veto; the extension is a named deferral owned by `kian-dsh` | SPEC §3.4.1, §3.5, §4.4; §16.4; D21 |
| S08 | blocker | **Fixed (by mechanism change)** — the settle gate is not a `DrainCtx` decorator (impossible: the engine calls `end_session` on the concrete ctx) but a `WorkerLoopDriver` phase at the outcome boundary whose denial is one synthetic admission; atomic-end preserved, orchestrator untouched; the deviation from R4's literal wording is recorded | SPEC §4.6; D12; *Alternatives E* |
| S09 | minor | **Fixed** — `static_only.rs` untouched and its claim narrowed to "no dynamic-loading crate in kian-plugin's normal tree"; no process-isolation or behavioral-compatibility claim | SPEC §13.2; D6, D10 |
| S10 | major | **Fixed** — runtime-neutral design: one dedicated `std` thread per sidecar, blocking `std::process`/`std::io`, runner-owned deadline via `Instant`, `futures::channel::oneshot` on the async side, no tokio in `kian-plugin`; no drain task ever blocks | SPEC §2, §7.6; D19 |
| S11 | major | **Fixed** — rollback split: fresh/never-enabled deployments get byte-identical behavior; an existing session's rollback is an edit plus a `kian serve` restart, after which that session runs under the re-resolved (here: empty) set with **no detection and no session quarantine** — the guard's coverage ends at the restart (SPEC §9.2 step 5), so the last `hook-set` audit line from the old process is the operator's only record, and no document calls the post-restart run byte-identical to an unextended one; inside one process, mismatch quarantine remains an invariant guard only (no v1 path produces that mismatch by editing the config) | SPEC §13.3, §9.2 step 5, §9.3; D17; §16.2 |

## Appendix B — the FINAL review closure (F1–F10)

The codex FINAL cross-review (`shared/round13/codex-final-review.md`) left ten MUST-FIX items: baseline
finding **#2** (unfixed) plus nine new ones (its NEW-1…NEW-7 plus the two baseline items #5 and #13/#17
it still counted). Each row is an owner ruling, implemented where the fourth column says. Three rows
**narrow** the contract; the rest correct it.

| # | Finding | Owner ruling | Where it landed | Decision |
|---|---|---|---|---|
| F1 | baseline #2: `turn/request` vs. fallback-rebuilt bodies is unspecified | fire **exactly once**, on the persisted primary `step/request` bytes, after the append and before the first provider call; fallback bodies are provider-layer internals and are **not** re-dispatched | SPEC §3.1 row, §3.2.3 (normative paragraph), §4.2 | D11 |
| F2 | baseline #5 / NEW: the dsh request·response "ports" are not real post-persist / settled-response sites | verify; if a row cannot be populated, **remove it** (`mapping.rs:411` precedes the driver append; `mapping.rs:447` is `map_tool_call`) | SPEC §3.4.1 (both rows `no`), §3.5, §4 rows, §4.2, §4.3, **§16.17**; DESIGN §6.4 | D11, D21 — **narrowing** |
| F3 | NEW-1: an out-of-grant reply resolved to the fail-closed default let an observe-only plugin induce a denial | out-of-grant / wrong-mode speech is **discarded as an opinion** (+ audit); outcome = as-if-absent; fail-closed belongs **only** to unreachability, and only to a `veto:*` holder | SPEC §1 (Class default, Veto-capable subscriber), §3.3, §3.4.3, §5.1, §5.2, §8.3, §10.2, §11.1 (two disjoint classes), §10.6 (`hook-discard`) | D14 — **corrects an escalation hole** |
| F4 | NEW-2: admission is one payload but the fan-out writes per target | normative order: shaping → **one** `admit/pre` at the channel boundary → per-target writes → `admit/post`; hooks never write; partial-write semantics unchanged (`subscriptions.rs:499/520-541`) | SPEC §3.1 row, §3.2.1 (`targets`), §3.2.2, §4.1 (ordering block) | D24 |
| F5 | baseline #13: whole-section vs per-instance `required`; `frame_overflow` counted and quarantining at once | `required` is **per instance**; refusals are per instance, no section refusal; **frame overflow = immediate quarantine**, not a window entry | SPEC §6.1, §6.2 (per-instance + two boundary cases), §6.4, §6.5, §7.6, §11.2 | D25 — **narrowing** |
| F6 | baseline #17: `binding.harness`/`profile` have no durable source | EVENT-MODEL `session/created` carries `{}` → **remove both** from the inputs; set = (config snapshot, durable `exec_worker`); record the narrowing | SPEC §1 (Effective set), §8.1 (two scopes), §8.2 (pseudocode + limitation), §9.2 step 2, §15, **§16.18** | D17, D26 — **narrowing** |
| F7 | NEW-3: the digest covered three limits and ignored `argv`/`env_allow`/restart/stderr/`order`/`required` | enumerate the **closed, complete** input list (identity, scope, order, required, binary digest, `process_sha256` over argv+env_allow+cwd+startup, grants, all seven limits); state the list is closed | SPEC §8.5 | D17 |
| F8 | NEW-4 + NEW-7: the `ready` frame was not required to match the manifest identity, and the admission frame's `epoch` was undefined | identity = canonical manifest path + name + version + `abi`, restated exactly by `ready` (typed refusal otherwise); **admission frames carry no `epoch`** — present on drain-phase hooks only | SPEC §6.2 rule 12, §7.2 (frames + epoch rule), §7.3 (identity) | D25 |
| F9 | NEW-5: no outcome when the settle gate's synthetic admission fails | three-step path: gate **does not settle** (ordinary append-failure route, session recoverable); veto **counted** in-process; one audit line (`hook-veto-admit-failed`) | SPEC §4.6 | D12 |
| F10 | NEW-6: "zero dependency-tree entries" is false — SHA-256 is needed and absent | name the implementation: `sha2 = "0.10"`, already a workspace dependency → **zero new packages, one new edge**; `kian-loop` grows the `kian-plugin` edge *and* gets injection; `kian-dsh` neither (crate-local port); reconcile `static_only.rs` | SPEC §2, §4 (edge column), §8.5, §13.2; DESIGN §6.4 | D11, D19 |

## Appendix C — the CONFIRMATION review closure (F11–F12)

The codex CONFIRMATION review (`shared/round13/codex-confirm-review.md`) accepted all ten FINAL rows
in Appendix B and opened two blockers. Both close as **corrections, not narrowings**: the first fixes
an example so it obeys the rule it illustrates, the second states the frame identity the F4 ordering
always implied.

| # | Finding | Owner ruling | Where it landed | Decision |
|---|---|---|---|---|
| F11 | the §8.1 worked YAML contradicted its own narrowing rule: `scopes.server` omitted `approval-gate` while `worker.node-zulu` added it through `use:` + `limits:`, which the subset rule refuses at boot | keep the rule, **fix the example**: `scopes.server` lists **both** instances, `worker.node-zulu` narrows with `use:` + `limits:`, a second worker keeps both with one tightened bound; `resolve` is restated as a **filter** (it is what the subset rule always meant) and the example is walked through it with both effective sets stated; a `limits:` key for a name the worker cannot receive now refuses boot instead of becoming a silent no-op | SPEC §8.1 (YAML + subset/`limits:` bullets), §8.2 (pseudocode, invariants 1–2, the walk-through, the F6 limitation cross-ref) | D26 — **corrects the example and pins the filter** |
| F12 | the remainder of F4: one multi-session `admit/pre` evaluation cannot populate the closed frame's singular `session`/`worker`/`seq`, and nothing said which target's set selects subscribers or which worker's deadline applies | one **evaluation**, one `hook` frame **per (subscriber, target session)** carrying that target's identity and post-narrowing `deadline_ms`; replies correlate by frame id; the first deny refuses the event for **all** targets (no row anywhere; audit names the denying plugin, the denying target and the set size); a target with no subscriber gets no frame, and that is not an error | SPEC §1 (Dispatch), §3.1 row, §3.2, §3.2.1, §4.1 (ordering block, per-target-frames paragraph, veto audit fields), §7.2, §7.3, §8.3, §10.6 | D24 — **states the identity F4's ordering implied** |
