# ADR-044: A harness step is bounded — no unbounded open-step wait on the bridge drivers

- **Status**: accepted (owner gate, 2026-11-20)
- **Date**: 2026-11-20
- **Context**: after [ADR-041](ADR-041-bounded-model-calls-and-crash-containment.md) closed the
  unbounded *model call*, an audit of every external wait on the drain path found exactly one
  remaining unbounded class: the open-step wait of the external-harness bridges
  ([HARNESS-BRIDGE](../HARNESS-BRIDGE.md), [ADR-043](ADR-043-multi-harness-bridge.md)).

  - While a harness turn is open the shared skeleton awaits the child's next event with no
    deadline ([driver.rs](../../crates/kian-harness/src/driver.rs) `live.session.next().await`,
    line 421); the adapters' `next()` blocks on the child's stdout
    ([kian-claude/session.rs](../../crates/kian-claude/src/session.rs) line 90,
    [kian-codex/session.rs](../../crates/kian-codex/src/session.rs) line 161).
  - The dsh driver has the same shape, and its comment records the assumption this ADR retires:
    "While a step is open the peer will speak (the provider call is in flight — **unbounded, like
    the built-in loop's model call**)" ([kian-dsh/driver.rs](../../crates/kian-dsh/src/driver.rs)
    lines 606-616).
  - Control writes to the child are unbounded too: `send`/`answer` await the child's stdin
    ([driver.rs](../../crates/kian-harness/src/driver.rs) lines 357, 521, 568).
  - [HARNESS-BRIDGE](../HARNESS-BRIDGE.md) §4.5 bounds the *startup* handshake (default 10 s), the
    *step count* (`max_steps`, default 32) and the *idle-boundary* re-check
    (`IDLE_INPUT_RECHECK`, 50 ms); it does not bound an open step, and §10's phase list does not
    record it. The audit found no other unbounded drain-path wait: the model call
    (ADR-041), exec/queue (ADR-030/ADR-033), plugin sidecars (`limits.deadline_ms`), channel
    HTTP clients (`HTTP_TIMEOUT`/`send_timeout`), the startup handshake and the cron worktree
    turns (ADR-039) are all bounded.

  Consequence: a *live but silent* harness child — a deadlocked CLI, a hung internal tool call, a
  wedged network request inside the harness — parks the drain inside the driver's stream forever.
  The heartbeat keeps the lease fresh, so the recovery sweep never takes over; `interrupt` is only
  honored at a stream outcome (never, while blocked); `release_worker` refuses a `running` thread;
  the ADR-035 reclaim only closes idle-ended threads. The session — and the worker slot it binds —
  is permanently busy, exactly the symptom ADR-041 removed for model calls. The assumption in the
  dsh comment ("unbounded, like the built-in loop's model call") is no longer true on one side and
  must not remain true on the other.

- **Decision**:

  1. **One additive config key, one semantic: child liveness.** `superagent.harness.<name>`
     gains `step_timeout_ms` (default `1_800_000` = 30 min; minimum `10_000`; `0` disables the
     bound; a value between `1` and `9_999` refuses boot rather than reintroducing a smaller
     livelock). The knob is **per harness** — the same layer as `max_steps` /
     `startup_timeout_ms`, resolved at mount time; a deployment that needs different bounds for
     one long-build bridge and one interactive bridge configures two harness sections (a
     per-worker-template knob would duplicate the config grammar for no semantic gain). It bounds the time an *open* harness step may pass with neither an event from the
     child nor a completed control write to it. The knob deliberately covers both the event read
     and the control write: they are the same question ("is the child still responding?"), not two
     semantics, so ADR-030's one-knob-for-one-semantic rule holds.
  2. **The default is generous on purpose.** The bound's job is to end a *dead* bridge, not to
     police a slow step: a harness may legitimately run a long silent tool (a 20-minute build
     emits one `tool_use` and one `tool_result` and nothing in between). 30 minutes tolerates that
     while still guaranteeing the session frees itself; operators with longer builds raise the
     knob, and `0` is the explicit pre-ADR opt-out.
  3. **Enforced in both bridge drivers** (`kian-harness` for Claude Code/Codex phase 1, and
     `kian-dsh`'s second skeleton) so the debt cannot survive behind the phase-3 convergence.
     Expiry is `LoopOutcome::SoftFailed` naming the bound (never `completed`); the engine settles
     `failed`, ADR-035's release/reclaim then frees the worker, and the existing episode-end kill
     path kills the child (no orphan, no second end).
  4. **No new state, event or wire.** Nothing is persisted about the step; the bound is a pure
     driver-side wait limit. A pre-ADR binary ignores the additive config key (and keeps the old
     unbounded wait), and every stream this change can produce is existing kinds only.

- **Alternatives considered**:
  1. **Rely on interrupt forwarding (HARNESS-BRIDGE phase 2) instead of a bound.** Rejected as
     *the* fix: forwarding only helps if the child honors the interrupt, and the whole failure
     mode is a child that cannot make progress. Forwarding stays a phase-2 improvement; the bound
     is what guarantees the slot frees.
  2. **Bound each harness tool call.** Rejected: kian cannot see the harness's tool calls (that is
     the honesty axis of the bridge contract — the harness owns its tools), and a
     protocol-per-harness deadline would leak vendor specifics into the skeleton.
  3. **A total step-duration cap instead of an inactivity bound.** Rejected for the same reason
     ADR-041 rejected a total request cap: it cuts legitimate long-running steps while telling us
     nothing about progress. Inactivity is the only progress signal the bridge has for a
     subprocess whose internals kian does not own.
  4. **Leave it unbounded and rely on operator restart (the status quo).** Rejected: this is the
     reported incident class; a deploy/restart may not be available, and every restart costs
     in-flight sessions on other workers.
  5. **Put the bound in the engine (an episode watchdog).** Rejected (ADR-041 alternative 2): the
     engine cannot distinguish a hung bridge from a legitimately long one; the bridge is the thing
     that knows the shape of its own progress.
  6. **Default `0` (opt-in) to avoid any behavior change.** Rejected: safe-by-default is the
     established direction (ADR-041 defaults on); an operator who wants the pre-ADR wait can say
     so explicitly, with the knob visible in config.

- **Adversarial walk** (state → failure → recovery):
  | Failure point | Invariant | Outcome |
  |---|---|---|
  | Child hangs while a turn is open | one end write, no orphan | the open-step wait expires → `SoftFailed` naming the bound → engine `failed` → the episode-end kill path kills the child → ADR-035 reclaim frees the worker |
  | Child hangs while receiving a control frame | same | the write is bounded by the same deadline → `SoftFailed`; no half-delivered input is claimed (the engine's durable inbox row is the record) |
  | Child exits exactly at the deadline | one outcome, no double end | either the mapped `Exit` event wins (existing mapping) or the deadline wins (`SoftFailed`); one end write either way |
  | Timeout races a turn boundary | turn events stay balanced | the driver returns `SoftFailed` without emitting a synthetic `turn/end`; the engine's settle path closes the turn records as it does for every soft failure |
  | `step_timeout_ms: 0` | explicit opt-out | pre-ADR behavior (unbounded); the ADR-035 release and the ADR-041 model bound still apply |
  | Interrupt during a hung step | honest stop | the interrupt is delivered/attempted; if the child never answers, the bound ends the episode (the interrupt row stays durable for the resumer) |
  | Takeover during a hung step | fencing | the heartbeat is fresh, so no takeover happens until the bound fires — that is exactly why the bound is needed; after it, the normal recovery path runs |
  | Rollback to a pre-ADR binary | readability | the config key is ignored; streams are existing kinds only |

- **Invariants preserved**:
  - **Honesty over completion rate** (constraint 7): a timeout is `failed`, never `completed`; the
    child's own outcome (if it exits) is mapped as before.
  - **Bounded recovery** (constraint 8): one deadline per step, no retry loop; the recovery is the
    existing end → reclaim path.
  - **Event stream is sole truth** (constraint 1): no step state is persisted; the bound is a wait
    limit, and the only writes are the existing turn/tool/end events.
  - **Layered iron rule** (constraint 9): the driver bounds its own wait; the engine still owns
    the end write; control still only writes the inbox.
  - **Narrow waist / bundle boundary** (constraint 10): the knob and the bound live in the bundle
    driver + config; nothing enters `kian-core`/`kian-orchestrator`.

- **Consequences**:
  - Positive: the last unbounded drain-path wait is closed. A wedged harness child ends the
    session within `step_timeout_ms` (+ the ordinary reclaim bound) instead of holding the worker
    until a restart; interrupt remains the fast path when the child is alive.
  - Negative: a harness step that is legitimately silent for longer than the knob is now killed
    (with `failed` named after the bound); the default (30 min) is chosen so ordinary long tools
    fit, and the escape is explicit config.
  - Negative: two skeletons must carry the same bound until phase 3 converges them; the ADR binds
    both so the debt cannot reintroduce the gap.
  - Rollback: remove the config key and the deadline wrappers; streams stay readable.

- **Non-goals (phase 1)**:
  - Per-tool deadlines inside a harness (kian does not own the harness's tools).
  - Forwarding interrupt into the child (phase 2, HARNESS-BRIDGE §10) — it stays the fast path,
    not the guarantee.
  - Cross-process harness resume / re-entering a dead child's conversation (phase 2).
  - Converging `kian-dsh` onto `kian-harness` (phase 3); this ADR only requires both to bound.
  - Changing the built-in loop, the engine, the event model, or the ADR-041 provider bounds.

- **Unverified**:
  - No L3 observation of a real Claude Code/Codex child wedging mid-turn; the L1/L2 shapes use a
    controlled fixture peer that stops emitting.
  - The 30-minute default is reasoned from harness event cadence (one event at tool start/end), not
    measured across real long-running tools; it is configurable per deployment.
  - Whether a child that ignores SIGKILL-on-drop can leave any OS-level residue is out of scope
    here (the existing pump's kill path is unchanged).
  - The blocked-*pipe*-write shape (a control payload larger than the OS pipe buffer against a
    child that never reads) is covered at the wrapper level (the bound unit test); its
    end-to-end integration is covered by the open-step read bound, not by a dedicated
    pipe-saturation test.

- **Verification map**:
  | Given | When | Then | Level |
  |---|---|---|---|
  | a fixture session whose `next()` never yields | the driver drives an open turn | `SoftFailed` naming `step_timeout_ms`; no synthetic completion | L1 |
  | a session that yields one event per short interval | the same bound (short test value) | the step is not cut | L1 |
  | a child whose stdin write blocks | a control frame is sent | `SoftFailed` at the bound; the input row stays durable | L1 |
  | `step_timeout_ms: 0` | a stalled session | pre-ADR unbounded behavior (opt-out works) | L1 |
  | a fixture peer that stalls after its `ready` handshake | real `kian serve` + mounted bridge session | the session ends `failed` within the bound, no fabricated `turn/end`, and the process stays healthy; the ADR-041 reclaim then frees any bound worker slot (the reclaim composition itself is proven in ADR-041's L2) | L2 |
  | timeout vs mapped `Exit` racing | the child exits at the deadline | exactly one `session/ended` | L1 |
