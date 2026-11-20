# ADR-041: A model call is bounded, and a crashed drain cannot hold a session forever

- **Status**: accepted (owner gate, 2026-11-19)
- **Date**: 2026-11-19
- **Context**: two "abnormal interruption" shapes still held a session — and with it the 1:1
  worker slot — forever, even though [ADR-035](ADR-035-worker-release-and-idle-reclaim.md) bounds
  the freeing of a thread that *reached* a terminal end:

  1. **An unbounded model call.** The openai-compat provider builds its `reqwest::Client` with no
     connect bound and reads the SSE body with no idle bound
     ([provider.rs](../../crates/kian-llm/src/provider.rs) `OpenAiCompatProvider::new`,
     `post`, `response_stream`); the bundle's other adapter clients set one
     (`lark.rs`, `wechat/protocol.rs`, `github.rs`). A provider that accepts the request and then
     stops sending — a half-dead local proxy is the common case — parks the drain inside
     `supervise`'s `stream.next()` forever. The heartbeat keeps the lease fresh
     ([engine.rs](../../crates/kian-orchestrator/src/engine.rs) `heartbeat_loop`), so the lease never
     expires and `Engine::recovery_sweep` never takes over; `interrupt` is only honored at a stream
     outcome boundary; `release_worker` refuses a `running` thread; the ADR-035 reclaim only closes
     `idle` + already-ended threads. The worker is permanently busy with no operator escape short of
     a process restart. This violates [ADR-030](ADR-030-exec-wait-bounds-and-budget-stop.md)'s
     rule — "a wait is bounded by what is actually being waited on" — for the one wait that is
     actually unbounded.
  2. **A drain that dies without unwinding its bookkeeping.** `Engine::drain_task` removes its
     in-process `drains` entry and `drain_once` stops the heartbeat only on the normal return path
     ([engine.rs](../../crates/kian-orchestrator/src/engine.rs) `drain_task`, `drain_once`). A panic
     inside the drain (a bundle driver, a tool, a provider parse) unwinds past both: the entry
     stays in the map (so every later `wake` folds and every release answers `Busy`) and the
     detached heartbeat task keeps refreshing the lease (so no replica can ever take over). The
     session is bricked until the process restarts. Even once a dead drain is *detected*, the
     takeover chain had no bound: a deterministically-crashing episode would be re-driven forever
     (constraint 8).
  3. The live deployment hit both faces of this on 2026-11-19: the 9 h config-permission crash loop
     was an outage (no healthy process could run recovery — ops fixes in
     [runbook-kian-vultr.md](../../docs/runbook-kian-vultr.md)), but the *stuck* worker session it
     left behind, and the model-call stall class in general, are product gaps this ADR closes. The
     ADR-035 escape itself is correct and stays untouched: release is still a terminal close of an
     already-ended thread, and a `running` thread is still never force-closed.

- **Decision**:

  1. **The model call is bounded like every other wait.** Two additive `providers.<name>` keys:
     `connect_timeout_ms` (default `10_000`; `0` = no explicit connect bound) and
     `idle_timeout_ms` (default `300_000`; `0` = disabled). `idle_timeout_ms` bounds **two** gaps,
     never the total request: (a) waiting for the response headers, and (b) each gap between body
     chunks. It is a per-gap idle bound on purpose — a legitimate long generation or a long
     reasoning silence must not be cut; a *stalled* stream must.
     An expiry is one typed `ProviderError::Transport("… idle timeout …")`, i.e. the existing
     `provider_down` class: the loop reports its existing honest soft failure, the end write is
     `failed` (never `completed`), and the ADR-035 release/reclaim path then frees the worker. The
     provider layer owns the bound (it is the thing being waited on, ADR-030); no new seam, no
     policy in the provider, no retry inside it (retry policy stays caller-owned, constraint 8).
  2. **A drain's bookkeeping is unwound on every exit, not just the happy one.** A drop guard owns
     the in-process `drains` entry (removed on return, panic or task cancellation) and a drop guard
     owns the heartbeat stop (the lease is no longer refreshed once the episode is gone, so the
     existing lease-expiry sweep re-wakes it). The drain task catches its own unwind at the task
     boundary: one structured `tracing::error!` line, drain metrics settled, then exit. Recovery is
     still the takeover path — this ADR only makes a dead drain *look* dead.
  3. **The takeover chain is bounded by construction.** A takeover claim counts the `drain/claimed`
     events since the last `session/ended` in the committed stream (the count is derived, constraint
     1 — no new event, column or counter). At `MAX_UNENDED_CLAIMS = 3` (the original attempt plus at
     most one retry) the taker does **not** drive again: it appends one honest
     `session/ended{failed}` carrying an additive `crashed` object
     (`bytes: {"crashed": {"takeovers": <u64>, "reason": <str>}}`), and the stream freezes. A
     deterministic crash can therefore never become an unbounded re-drive loop, and the session
     reaches the idle-ended shape ADR-035 reclaims. A normal single-takeover recovery (one claim
     without an end) is unaffected.
  4. **Compatibility.** No new event kind, no state-machine edge, no new table, no change to
     `thread_liveness`, no wire frame, no prompt or tool-schema byte. Two additive config keys and
     one additive `session/ended` bytes key ([EVENT-MODEL](../EVENT-MODEL.md) §4). A pre-ADR binary
     projects every stream this ADR can produce.

- **Alternatives considered**:
  1. **A total request timeout instead of a per-gap idle bound.** Rejected: it cuts legitimate long
     generations and long reasoning silences, and it re-introduces the "one knob for two semantics"
     failure ADR-030 forbids (a stream that is progressing is not a stalled one).
  2. **A drain-level / episode-level watchdog.** Rejected: it cannot distinguish a hung provider from
     a legitimate long task (one `exec` may run 30 minutes; a reasoning turn may be slow), so any
     bound it picks is either useless or dishonest. Bound the *thing being waited on* instead.
  3. **Force-release / force-close the worker slot on a hung drain.** Rejected (with ADR-035's
     reasoning): a `release` is a terminal close of an already-ended thread; force-closing a live
     drain would let two writers race into one stream. The provider bound makes the thread reach a
     real end first — then the documented release path applies.
  4. **Retry the model call inside the provider on timeout.** Rejected: retry is recovery policy,
     which is caller-owned and budgeted (constraint 8); the provider classifies and reports, it
     never decides.
  5. **A durable crash counter (new event kind / column).** Rejected: the count is a projection of
     existing `drain/claimed`/`session/ended` events; a second durable fact would need its own
     replay, snapshot and join semantics for a disposable liveness bound.
  6. **Catch the panic and re-drive in-process.** Rejected: unbounded, and it duplicates side
     effects; the lease-expiry takeover is the designed retry, and decision 3 bounds it.
  7. **A `workers.*` / product-wide CLI knob for the model bound.** Deferred: the bound belongs to
     the provider being waited on (`providers.<name>`), the same place `base_url` lives; a global
     default would silently override a per-provider proxy.
  8. **Making the reclaim shorter on failure.** Rejected for this ADR: `workers.release_after_ms`
     is the operator's window to resume an interrupted thread; the stall fix is upstream of it.

- **Adversarial walk** (state → failure → recovery):
  | Failure point | Invariant | Outcome |
  |---|---|---|
  | Provider accepts, never sends headers | drain makes progress or ends | header wait expires at `idle_timeout_ms` → `Transport` → honest `failed` end → reclaim frees the slot |
  | Provider sends one chunk, then stalls | same | the per-gap idle bound fires on the next chunk wait; the partial turn is never accepted as complete |
  | Provider is slow but progressing (long generation) | legitimate work never killed | the idle bound resets on every chunk; only a real gap fires |
  | `idle_timeout_ms: 0` configured | explicit operator choice | no idle bound (pre-ADR behavior); the ADR-035 release and the crash containment still apply |
  | Crash (panic) inside `drain_episode` | no leaked in-process state, lease can expire | both drop guards run on unwind: `drains` entry removed, heartbeat stopped; one error log; the lease expires and the sweep takes over |
  | Task cancellation / runtime shutdown mid-drain | same | the same drop guards run when the future is dropped |
  | Takeover of a crashed stream | bounded recovery (constraint 8) | claims-since-last-end is counted from the stream; the 3rd claim appends `session/ended{failed}` with `crashed` bytes instead of driving |
  | Crash loop plus concurrent wake | exactly one driver | the claim's epoch fences the losers; a wake folds on the live handle; the count is read from committed events, never process state |
  | Crash between claim and the crash-loop end write | atomic end | claim lands, end does not; the next takeover re-counts and writes the end once; projection stays `running`/`idle`-safe across the window |
  | Release races the crash-loop end | fencing | whichever claim wins, the loser's write is rejected by expectedSeq/epoch CAS; no second terminal end |
  | Rollback to a pre-ADR binary | readability | `session/ended{failed}` is an existing kind/edge; provider timeouts are additive config a pre-ADR binary ignores (its default is the old unbounded call) |

- **Invariants preserved**:
  - **Event stream is sole truth** (constraint 1): the crash bound is derived from `drain/claimed` /
    `session/ended`; no counter is stored.
  - **Persist-what-you-send** (constraint 2): the timeout is applied after the request body is
    persisted and after the request is issued; the sent bytes are unchanged.
  - **Cache-byte stability** (constraint 3): no context, prompt, tool-schema or serialization byte
    changes; a timeout only ends a turn.
  - **End writes are single atomic appends** (constraint 4): the crash-loop end is one
    `session/ended` append; no state splits.
  - **Safety by fencing, liveness by lease** (constraint 5): the timeout is liveness only; no clock
    ever grants write permission, and every appended end still carries the drain's epoch.
  - **Honesty over completion rate** (constraint 7): a timeout or a crash loop maps to `failed`,
    never to `completed`; the earlier honest end of a resumed thread is preserved.
  - **Bounded recovery** (constraint 8): ≤ 2 driving attempts before the honest failed end; one end
    write; no retry loop in the provider.
  - **Layered iron rule** (constraint 9): the provider bounds its own wait; the engine owns the
    drain lifecycle and the end write; the bundle is untouched.
  - **Narrow waist** (constraint 10): `kian-llm` gains no core seam and no store symbol;
    `kian-orchestrator` gains no provider/channel code.

- **Consequences**:
  - Positive: a stalled model call now becomes an honest `failed` end within
    `idle_timeout_ms` (plus the ordinary end/reclaim bound), so a bound worker frees itself without
    an operator restart; a panicking drain no longer bricks a session in-process or on other
    replicas; a deterministic crash loop terminates in ≤ 3 claims instead of forever.
  - Positive: the unbounded-wait class is now closed where the tree's other HTTP clients already
    were, and the two knobs default on — the safe behavior is the default, the escape (`0`) is
    explicit and auditable in the config.
  - Negative: a provider that legitimately stops sending for longer than `idle_timeout_ms` (a
    pathological queue) is now a failure rather than a hang; operators with such a backend raise the
    knob instead of waiting forever.
  - Negative: a session that crashes twice (e.g. two process kills in a row) now ends `failed` on
    the next takeover instead of being retried a third time — deliberate (bounded recovery) and
    recoverable by a new session.
  - Rollback: revert the two config keys (pre-ADR binary ignores them) and the engine guards; the
    streams produced are ordinary.

- **Non-goals (phase 1)**:
  - Bounding *every* external wait: plugin sidecars, tool providers and a custom `LoopDriver` remain
    outside this ADR (each owns its own bound; a hung custom driver still parks its drain). The
    model call is the one the product itself makes on every turn.
  - A total-request cap, a streaming inactivity metric, per-model tuning, or a provider-level retry.
  - Force-release / force-close of a live drain, or a `release_worker --force`.
  - Changing `workers.release_after_ms` or the reclaim tick.
  - Releasing a worker that ended `awaiting_input` (CONFIRMATION-MODEL phase 2): such a thread is
    deliberately live — the answer (or an interrupt) is its release path, and `release_worker`
    keeps refusing it by design. That wait is an *intentional* pause, not an abnormal interruption.
  - Making the crash bound configurable (a fixed, tiny bound is the point; a knob would let an
    operator reintroduce the loop).

- **Unverified**:
  - No L3/real-deployment run: the stall is exercised at L1 over a raw stalling socket and at L2
    over the real `kian serve` assembly with a stalling mock provider; a real provider proxy stall
    on the vultr box is not part of CI.
  - `connect_timeout_ms` has no deterministic unit test (a blackholed peer needs network shaping);
    the header-wait path is tested through the same idle bound.
  - Postgres mode is not exercised for this change (the guards are engine-local; the store-level
    claim/heartbeat paths are unchanged).
  - The crash bound is reasoned for a *deterministic* crash loop; the test drives it by seeding a
    stream with un-ended claims, not by injecting a real panic three times.

- **Verification map**:
  | Given | When | Then | Level |
  |---|---|---|---|
  | a provider that never sends headers | `complete()` is polled | one `Transport` error within `idle_timeout_ms`; no fabricated chunk | L1 |
  | a provider that sends one chunk then stalls | `complete()` is polled | the chunk is delivered, then one `Transport` error at the gap bound | L1 |
  | `idle_timeout_ms: 0` | a normal complete response | unchanged success (opt-out works) | L1 |
  | a `providers:` entry without the new keys | config parse | defaults 10 000 / 300 000; explicit values and `0` parse | L1 |
  | a driver that panics inside its stream | the drain runs | `drains` entry gone, heartbeat stopped, lease expires, `wake` starts again, the sweep takes over | L1 |
  | a `running` stream with 3 un-ended claims | a takeover claims | one `session/ended{failed}` with `crashed.takeovers`, no driver run | L1 |
  | a `running` stream with 1 un-ended claim | a takeover drives | the existing re-drive settles honestly (regression) | L1 |
  | real `kian serve` + a stalling mock provider | one turn is submitted | the session ends `failed` within the bound, no manual restart | L2 |
  | a committed crash-looping stream seeded pre-boot | `kian serve` boots | the sweep finishes it `failed`; the stream freezes; the process stays healthy | L2 |
  | a bound worker thread whose committed stream crashed | `kian serve` boots with a short `workers.release_after_ms` | the crash chain ends `failed`, then the ADR-035 reclaim closes the binding (`released.by = "reclaim"`) and `thread_liveness` reads `Released` — the worker is freed with no operator call | L2 (composition) |
