# Kian — Design v0.1

> Design = HOW. Spec lives in [docs/SPEC.md](../docs/SPEC.md); decisions live in [decisions/](decisions/).
> Language: **Rust**, async runtime tokio. This design follows axioms A1–A10 (Feishu design doc).

## 0. Goals and boundaries

### 0.1 Goals

- G1: Full Admit→Wake→Drain→Promote semantics on single-machine (sqlite) mode; S1 (kill -9
  recovery) with zero lost work.
- G2: NoSplitBrainWrite — when two processes contend for one session, stale-epoch writes are
  always rejected by the EventStore (expectedSeq CAS + epoch comparison).
- G3: The exact bytes of every model request are persisted (A2); replaying the same event stream
  is byte-identical.
- G4: Cache hit rate and invalidation causes become first-class observability metrics (A3);
  compression is the only deliberate invalidation point.
- G5: The super-agent reference architecture (Router/Worker/event subscriptions/BYOM) runs as a
  bundle on top of the core without modifying core crates.
- G6: Minimal core surface (A10): core crates import no provider-, channel- or daemon-specific
  code.

### 0.2 Non-goals

- No Web UI (CLI/API first).
- No multi-region / multi-Router horizontal sharding (article experiments show no throughput
  bottleneck at team scale).
- No credential-less credential mechanism (standalone Phase 3+ topic).
- No built-in LLM gateway (access resilience lives in an external gateway; Kian owns only slot
  determinism).
- No Temporal-style deterministic workflow constraints (LLMs are inherently non-deterministic;
  we replay bytes and events, not control-flow re-execution).

### 0.3 Decision quick reference

| ADR | Decision | Alternatives | Rejected because | Scope |
|-----|----------|--------------|------------------|-------|
| [001](decisions/ADR-001-rust-foundation.md) | Rust + tokio + sqlite-first | Go, TS | No-GC tail latency, type safety, single-binary deployment | Everything |
| [002](decisions/ADR-002-event-sourced-core.md) | Event-sourced core: append-only events + rebuildable projections | Direct state tables | A1 auditability; crash recovery = replay | core, storage |
| [003](decisions/ADR-003-super-agent-as-profile.md) | Super-agent is a profile/bundle composition pattern, not core capability | Router/Worker built into core | A10 narrow waist; the article: many variants, not an invariant structure | Everything |
| [004](decisions/ADR-004-cordis-inspired-plugin.md) | Plugin mechanism borrowed from Cordis (service keys/injection/reversible registration); Rust uses traits + registries | dylib dynamic loading | Stability first; compile-time plugin set | plugins |
| [005](decisions/ADR-005-fencing-via-expectedseq.md) | Safety = fencing (epoch + expectedSeq CAS); lease is only liveness | Lease TTL only | NoSplitBrainWrite must not depend on detection timeliness | core, storage |
| [006](decisions/ADR-006-model-slots.md) | Declarative model slots (slot→provider/model/fallback), immutable at runtime | Free runtime model switching | A3 cache-byte stability; P6 determinism | llm seam |
| [007](decisions/ADR-007-kiand-delivery-semantics.md) | kiand delivery: server-side durable queue, at-least-once, command-id correlation | Exactly-once via endpoint state; fail-fast offline; queue-as-events | Endpoint stays stateless (brain–hand separation); offline park is S5's core semantic; queue rows are pre-event state, not session facts | exec seam |
| [008](decisions/ADR-008-async-tool-completion-admitted-input.md) | Async `start()`/`stop()`: completion is an admitted input; job rows are pre-event durable state; `stop()` is honest cancellation | Completion-as-event; second dispatch queue; blocking drain | Full-duplex without loop changes; invariant-1 ordering; default-off tool pair | exec seam |
| [009](decisions/ADR-009-multi-replica-claim-admission.md) | Conditional claim admission (no row / expired / same holder); unique holders under postgres; wake transport advisory | Unconditional claim + fencing only; distributed lock | No multi-replica epoch ping-pong livelock; single-replica behavior byte-identical; lease never grants write permission (ADR-005 intact) | core, storage |

## 1. Architecture overview

### 1.1 System context (C4 L1)

```mermaid
graph TB
    subgraph Users["Users & channels"]
        U([Developer/SRE])
        IM([IM group chat])
        GH([GitHub])
        MAIL([Email/docs])
    end
    subgraph Kian["Kian (this system)"]
        CORE[kian-core<br/>EventStore/Session/Inbox/Drain/recovery]
        PLUGINS[Plugins: channel adapters / model adapters / tools]
        SA[super-agent bundle<br/>Router/Worker composition]
    end
    subgraph Ext["External"]
        LLM([LLM provider])
        GW([LLM gateway])
        KIAND([kiand endpoint daemons ×N<br/>BYOM])
    end
    U -->|CLI/SDK| CORE
    IM & GH & MAIL -->|event subscription| PLUGINS
    PLUGINS -->|Admit| CORE
    SA -->|runs on the core| CORE
    CORE <-->|slot calls| GW --> LLM
    CORE <-->|command exec / result flow-back| KIAND
```

### 1.2 Containers (C4 L2)

```mermaid
graph LR
    subgraph kian-server["kian serve (single-machine form)"]
        API[API/JSON-RPC<br/>axum]
        ORCH[orchestrator<br/>Admit→Wake→Drain→Promote]
        REC[recovery subsystem]
        PROJ[projection engine]
        ES[(EventStore<br/>sqlite→postgres)]
        INBOX[(Inbox table)]
        WAKE[wake registry<br/>pendingWake]
    end
    CLI[kian CLI] --> API
    SDK[kian-sdk crate] --> API
    subgraph workers["drain executors"]
        FIBER[tokio task/fiber]
        LOOP[agent loop<br/>pluggable: built-in/dsh]
    end
    API --> ORCH --> FIBER --> LOOP
    ORCH --> ES
    ORCH --> INBOX
    WAKE --> ORCH
    subgraph endpoints["Endpoint side"]
        KD[kiand<br/>stateless lightweight daemon]
        WD[watchdog]
    end
    ORCH <--> KD
    WD -.->|health events| ORCH
```

**The layered iron rule** (inherited from Feishu §4.1): the control plane only writes the inbox
and reads projections — it never touches execution; execution only appends events — it never
mutates projections directly; after a crash, replaying events rebuilds all in-memory state.

### 1.3 Relationship to the three references (synthesis map)

| Reference | What we borrow | Where it lands in Kian |
|-----------|---------------|------------------------|
| Super-agent article | Router/Worker two-layer, event-driven subscriptions, async ReAct / safe-boundary promote, channel bandwidth matching (IM ≤100 words), BYOM brain-hand separation, watchdog | **super-agent bundle** (not core): Router session template + Worker session template + subscription rules + channel adapters + kiand + watchdog |
| deepseek-harness | everything-is-a-plugin, session log as model-context truth, deriveMessages projection, capability seams (Service Definition/Provider/Consumer three roles), schema versioned migrations | Plugin system design (§6), event schema versioning (§3), model-context projection |
| Cordis | stable service keys, inject dependency declaration, typed event dispatch five modes, reversible registration side effects, narrow waist | Plugin system (§6): `Ctx` service registry, event dispatch modes, Footprint Ladder |
| Kian Feishu doc | All core semantics (P1–P8, A1–A10, concept model, state machine, six invariants, fencing) | **all of kian-core** |

## 2. Core execution model

### 2.1 Concept model and Rust type skeletons

| Concept | Persistence | Rust skeleton (illustrative) |
|---------|-------------|------------------------------|
| Agent | Versioned config (library table) | `struct AgentDef { id, version, model_slots: Map<SlotName, SlotSpec>, tools, budgets }` |
| Environment | Config (unversioned) | `struct EnvSpec { sandbox, network_allowlist, deps }` |
| Session | The one first-class durable entity | `struct SessionId(Uuid)`; state = projection |
| Input | Durable table rows | `struct InputRow { id, session, payload, admitted_at, eligible_at }` |
| Drain | No durable identity | `struct Drain { session, epoch: u64, claim: LeaseRecord }` |
| Turn | Event slice | Interval derivation over `enum TurnEvent` |
| Event | Append-only, never modified | `struct Event { seq, session, epoch, ts, kind: EventKind, bytes }` |
| Thread | Independent event stream + version pin | `struct Thread { id, parent_session, pinned_version }` |

### 2.2 Session state machine (projection-derived)

```mermaid
stateDiagram-v2
    [*] --> idle: create
    idle --> running: claim (lease+epoch)
    running --> idle: drain settled<br/>(inbox empty)
    running --> recoverable: lease expiry<br/>(only from running)
    recoverable --> running: new claim takeover
    running --> completed: end_reason=intentional<br/>atomic append
    running --> cancelled: end_reason=cancelled
    idle --> archived
    running --> archived
```

Rules:
1. **End write + state transition = one atomic append** (NoHalfTerminated). On sqlite = one
   transaction; on postgres = one INSERT..RETURNING. Corollary: session state is read only from
   the event projection.
2. **Safety by fencing**: claim issues a monotonically increasing epoch; every drain write carries
   it; the EventStore rejects stale epochs (expectedSeq CAS is the fence) → NoSplitBrainWrite.
   Lease (claim TTL + heartbeat) is only liveness.
3. recoverable is entered only from running; idle holds no lease and safely waits for the next
   wake.

C3 refinement (event→trigger mapping; the projection's contract): the projection folds events
through an explicit transition table. `session/created`→create; `drain/claimed`→claim — initial,
resume, or takeover: a second claim with no end write in between is the crash-recovery stream
shape and projects `running→running` (safety against true double-claims is the store's epoch
fence, not the projection); `session/ended{reason}`→end write, where `completed`/`cancelled` are
terminal and freeze the stream, while `interrupted`/`failed` release ownership cleanly and return
the session to `idle` (honest labeling lives in the events; resume = new claim; the final end
write records the final outcome); `input/admitted` and `thread/started` (C18, Phase 2)
preserve `idle`/`running`/`recoverable` — `thread/started` is appended to the worker's
stream after `session/created` but before the first `drain/claimed`, and to the Router's
stream mid-drain; every other kind is drain activity, legal only while `running`. EndReason
first-write-wins holds
within one drain episode: a second `session/ended` before the next claim is a named projection
error (the projection-side mirror of the store's end-write CAS race). `recoverable` (lease
expiry — a liveness observation overlaid from the expired-lease query) and `archived` (ops
action) are table edges with no v0 event trigger; later event kinds may drive them
(additions-only).

C56 note (read-side only, no state-machine change): the **Ticket stage** (`delivery-retry →
awaiting-answer → awaiting-review → blocked-failed → queued → idle → working`, first match
wins) is a *disposable projection over worker streams* (§4.7) — like `recoverable`, it is an
overlay computed at read time, never a session state and never persisted; the one durable fact
it reads beyond existing events is the additive `"state"` key on completion-report content
(§4.7, additions-only).

### 2.3 Admit → Wake → Drain → Promote

```mermaid
sequenceDiagram
    participant C as control plane/adapter
    participant S as Store(durable)
    participant W as wake registry
    participant D as drain(executor)
    C->>S: 1. Admit: INSERT input (durable first, A7)
    C->>W: 2. Wake (advisory: foldable, losable)
    W->>D: 3. After folding/scheduling, trigger claim
    D->>S: claim(session, TTL, epoch++) [lease = liveness]
    loop until no eligible input in inbox
        D->>S: 4. Take eligible input (safe boundary)
        D->>D: iteration: loop/tools/model calls<br/>(sent bytes persisted first, A2)
        D->>S: append events (carrying epoch, expectedSeq CAS)
        Note over D: promote only at provider-turn boundaries
        D->>S: 5. Settle: re-check inbox<br/>(settle-on-abort, invariant 6)
    end
    D->>S: end write (atomic append) → idle/completed/cancelled
```

The six semantic invariants (inherited in full; resident as debug_assert + test assertions):
1. **Durable before execute**: an input is never woken before it is durable; side-effecting tool
   calls are logged first.
2. **Wake is advisory**: pendingWake is foldable, losable, concurrency-safe; at startup
   kiand/kian fully rebuilds the wake registry from the inbox → NoLostWork.
3. **Promote only at safe boundaries**: no event insertion into an in-flight provider-turn; steer
   resets the provider-turn allowance once per steer batch.
4. **Drain has no durable identity**: after a crash, no in-process continuation is assumed;
   recovery = a new drain replaying events + continuing to consume the inbox.
5. **Interrupt targets only the active ownership chain**: idle/missing → no-op; interrupted work is
   honestly labeled interrupted ≠ failed.
6. **Settle on abort**: before releasing ownership, an interrupting drain re-evaluates the inbox;
   if pending work exists it keeps pendingWake registered.

C9 refinement (the drain's consumption and promote-boundary mechanics, as implemented by
`kian-orchestrator`): the drain is the *sole* writer of `input/admitted` **events** — the
control plane (`admit`/`steer`) only writes inbox rows. When the drain takes a row at a safe
boundary it appends one `input/admitted` event carrying the §3.2 message payload
(`{"role","content","synthetic"?}`) and binds the row to that event's seq via the C6
event-driven consumption guard. The input-payload convention: a plain row's payload *is* the
message content; a steer row wraps it as `{"synthetic": true, "content": X}` so the strippable
marker survives the inbox round-trip. The C32 spill envelope extends this row-level convention: a
row payload of exactly `{"content": X, "spill": Y}` consumes as **two events in one fenced write
batch** — `input/admitted` carrying `{"role": "user", "content": X}` exactly as the bare content
would have produced, followed immediately by `input/spilled` carrying the opaque sidecar verbatim —
while rows without the envelope consume byte-identically to before. Promote-boundary enforcement lives in the drain-context
surface handed to the `LoopDriver` seam (`LoopDriver::Ctx`): input-taking is refused while a
provider turn is in flight, and every drain write carries the claim's epoch under expectedSeq
CAS (seq-mismatch from a fenced-out concurrent writer is repaired by re-reading the head; a
stale-epoch rejection abandons the drain — it never fights the fence). The "steer resets the
allowance once per steer batch" invariant reads: a newly eligible batch observed at a settle
point grants exactly one more driver episode. An end write with reason
`interrupted`/`failed` returns the session to `idle` with pendingWake deliberately kept
registered for the resumer (invariants 5/6); a drain that finds the projection terminal
releases without appending anything (terminal streams freeze).

### 2.4 Three-layer recovery (A4/A5)

```mermaid
graph TB
    subgraph L1["In-loop recovery (within an iteration)"]
        R1[Pure error-classification function<br/>classify: Error → ErrorClass]
        R2[Policy-table lookup<br/>policy: ErrorClass × Ctx → Action]
        R3[backoff_retry / compress / failover / fail]
    end
    subgraph L2["Session-layer recovery"]
        R4[drain crash → recoverable<br/>lease-expiry detection]
        R5[new drain: replay events to rebuild in-memory state]
        R6[recovery budget: tokens/wall_clock<br/>refund semantics]
    end
    subgraph L3["Durable-state recovery"]
        R7[EventStore corruption / half-written tail repair]
        R8[projection rebuild: full replay]
        R9[half-terminated session detection & alerting]
    end
    L1 -->|iterations/budget exhausted| L2
    L1 -->|store failure| L3
```

- Error classification is a **pure function** (unit-testable, table-drivable); recovery policy is a
  **declarative table** (config-driven, disposition owned by the caller, A4).
- Every recovery kind has a one-shot guard (the recovery loop itself cannot run away); recovery
  consumption counts against a budget with refund semantics = unused remainder returned on task
  completion (A5).

C10 refinement (the implementation contract, `kian-orchestrator::recovery`):
- The class vocabulary is closed and string-keyed for the policy table: `provider_down`,
  `rate_limited`, `context_overflow`, `tool_failed`, `store_failure`, `budget_exhausted`,
  `validation_failed`, `empty_response` (the constraint-7 soft failure), `unknown`. Wrapping the
  typed seam/store errors (`ProviderError`, `ToolError`, `StoreError`, `InboxError`) means
  classification never parses error strings.
- Policy resolution is **first-match-wins** in declaration order (deterministic); a class no rule
  covers fails honestly — the runner never invents a default action. `kian.yaml` parsing is
  fail-closed (unknown class or action names refuse to load). The config-default table is the only
  hardcoded policy set.
- Disposition is caller-owned (A4): the bounded runner (`run_with_recovery`) only classifies,
  resolves, guards, budgets and sequences; side-effecting actions run through caller-supplied
  `RecoveryReactor` hooks (`compact`/`failover`/`ask`). `compress_then_retry` requires the reactor
  to append the observable `compact` event **before** invalidating the prefix (constraint 3).
- Ordering inside the runner: one-shot guard *before* budget — a repeated kind escalates
  regardless of remaining tokens. Retry-class actions consume `retry_cost_tokens` first; denial
  escalates with a distinguishable token/wall-clock cause.
- Escalation (L1 → L2) is an outcome, not an internal state change: the caller responds to
  `EscalatedToSession` by releasing the claim *without* an end write, so the session lands in
  `recoverable` via lease-expiry detection (ADR-005 liveness) and a fresh drain replays and
  continues. The loop is bounded by construction: ≤ one fire per class (≤ `ALL.len() + 1`
  operation invocations) and ≤ `tokens / retry_cost` retry actions.
- Budget counters (`consumed`/`refunded`/`returned`) are observable snapshots; `settle()` (task
  completion, A5) returns the unused remainder exactly once — the data source for the C14
  `recovery budget reset` command.

C15 refinement (session-layer recovery as implemented by `kian-orchestrator` + `kian serve`,
proven by the S1 chaos suite): the engine exposes `Engine::recovery_sweep()` — the §2.4-R4
liveness loop over the C6 expired-lease query — which wakes every session whose lease is past its
TTL *and* whose stream projects `running`; `kian serve` runs it on a ticker whose period equals
`drain.heartbeat_interval_ms`, so detection is bounded by `last_heartbeat + lease_ttl_ms +
heartbeat_interval_ms` (the §13 target plus at most one heartbeat of kill-to-last-heartbeat skew;
safety never depends on the timing — the takeover claim's epoch fences any lingering old owner,
ADR-005). The drain episode recognizes a *takeover*: a stream that already projects `running`
under a fresh claim belongs to a drain that died mid-episode, so the taker re-drives the loop to
an honest settle **even when the inbox is empty** (the killed episode consumed its inputs; the
startup inbox scan cannot see those sessions — without the takeover re-drive they would sit in
`running` forever while the sweep churned). Honesty is preserved end to end: the killed session
never projects `completed`/`failed` across the kill window (only `running`, overlaid
`recoverable` by the lease query), and the final end write lands exactly once, after the re-drive
actually settles.

C47 refinement (multi-replica claim admission, ADR-009): claiming is *conditional* — the claim lands only when no
claims row exists, the row's lease is expired, or the claiming holder already owns the row; a fresh lease held by a
different holder is refused with the typed `InboxError::ClaimRejected` (nothing written, epoch floor unchanged) and the
engine treats it as an honest no-op drain exit — the live holder's sweep or the advisory wake transport (C52) services
the input, never a retry loop. Same-holder claims still bump the epoch, so single-replica behavior (back-to-back
drains) is byte-identical; write acceptance never consults lease state (ADR-005 unchanged — this conditions claiming
only). Postgres mode requires an explicit unique `drain.holder` (fail-closed at startup, C51): two replicas sharing a
holder string would defeat the same-holder condition.

C68 refinement (durable interrupt intent, ADR-014): an `interrupt` RPC is durable *in addition to* the in-memory
flag — whenever work may still be in flight (a local drain handle, or a `running`/`recoverable` projection whose
owner is another replica or dead), the control plane admits a reserved control-envelope inbox row targeting the
last event's epoch; the owning drain consumes it at a safe boundary into a `control/interrupt` event and settles
`interrupted`. Idle/terminal sessions keep the exact `NotDraining`/`UnknownSession` behavior and admit nothing.
Two races are settled explicitly:

- **Completed-interrupt race.** A current-epoch intent that becomes eligible while the loop is finishing its work
  is honored, not completed past: the boundary take after the settle (and, as the last line of defense, one final
  control-only take immediately before the end write) consumes the row and flips the end write from `completed`
  (or `failed`) to `interrupted` — the honest stop-before-done. The final take consumes *only* control rows;
  regular pending inputs stay eligible for the resumer's registered wake.
- **Stale-epoch rule.** A row whose target episode already settled (the stream carries a `session/ended` at the
  row's `target_epoch`) is consumed for audit with `"stale": true` and ends nothing — a later episode the intent
  never aimed at must survive. A row targeting an *un-settled* episode always interrupts, including one whose
  owner died and a takeover claim resumed: the resumed work is the work the interrupt intended to stop, so the
  takeover settles `interrupted` (the crash-safe path — the durable row outlives the dead owner and the startup
  inbox scan hands it to the takeover).

## 3. Event model and schema evolution

### 3.1 Event classification (borrowing dsh's durable/live split)

| Class | Examples | Persisted | Purpose |
|-------|----------|-----------|---------|
| Durable events | turn/start, step/request, assistant/message (sent bytes), tool/call, tool/result, input/admitted, drain/claimed, session/ended | ✅ append-only | Truth and replay |
| Live events | streaming chunks, heartbeats, progress notifications | ❌ not persisted | UI/observability (derived from durable events or via a side channel) |

The durable kind set is exactly twenty-one (C18 + C31 + C68): the fifteen kinds frozen through Phase 1
(fourteen by C2 + the additive C14 `recovery/budget-reset`) plus the four Phase-2 additions —
`thread/started`, `thread/completed`, `evidence/verdict`, `channel/delivered` — and the Phase-3
addition `input/spilled`, all additions-only (no renames, no reformats of existing kinds) and all
non-context-bearing: thread lineage, evidence verdicts and delivery receipts are audit data that
never enter the model context. `thread/started` is state-preserving in `idle`/`running`/`recoverable`
(§2.2); the other Phase-2 kinds and `input/spilled` are drain activity. The spilled raw is the
verbatim channel payload kept in-stream but out of context — the Inline form of the same input
(the excerpt carried by its sibling `input/admitted`) is what the model sees, while the full raw
stays in the stream as the audit/recall copy (constraint 1: the event log remains the sole truth;
any spill index is a disposable projection over it). The pair is produced by the C32 consumption
write: the `{"content", "spill"}` envelope is a **row-level convention** riding in the existing
inbox row `payload` — not an event-payload change; the C4 frozen event payload contracts are
untouched, and the envelope never appears in any committed event.

C68 addition (`control/interrupt`, ADR-014): the twenty-first kind is the **durable interrupt
intent** — drain-activity, non-context-bearing (never a message, never in a model request,
constraint 3), carrying `{"action":"interrupt","reason","requested_by","requested_at_ms",
"target_epoch","stale"?}`. Like the spill envelope, the control envelope
`{"kian":{"type":"control","action":"interrupt",…}}` is a row-level convention in the inbox row
payload; the control plane admits the row (idempotent per `(session, target_epoch)`) and only the
owning drain appends the event — the layered iron rule (constraint 9) is untouched: the control
plane still writes nothing but the inbox.

### 3.2 Model-context projection (the implementation core of A2/A3)

`derive_model_context(session) -> Vec<Message>`: projects the next model request's messages from
the event stream. Rules:
- **History is immutable**: the bytes field of an assistant/message event *is* what was sent;
  replay is byte-identical.
- **Compression is the only deliberate invalidation point**: a `compact` event carries the
  pre-compression range [a,b) and the post-compression summary; the projection function skips the
  compressed range when it encounters the event. Compression must emit an event (observable, A3).
- **Synthetic messages are strippable** (A6): steer-injected synthetic user messages carry a
  `synthetic=true` marker; replay/export can strip them to restore the "real" context.
- **Request serialization is frozen (C8)**: model request bodies are serialized with `serde_json`
  compact output — object keys in lexicographic order (the workspace does **not** enable
  `preserve_order`; enabling it would silently change every request's bytes), while arrays keep
  caller-supplied order (the built-in loop passes tools in a deterministic order). The golden body
  hash is pinned by `kian-llm::provider::FROZEN_CHAT_BODY_SHA256`; changing the serialization
  contract is a constraint-3 event (prefix-cache invalidation for every session) and must update
  the hash and this section in the same change.
- **Wire dialect v2 (kian-61, constraint-3 event)**: strict OpenAI-compatible endpoints (real
  DeepSeek verified live; OpenAI documents the same shapes) rejected the v1 dialect after any tool
  round trip — HTTP 400 for tool names outside `^[a-zA-Z0-9_-]+$` (the built-ins were
  `fs.list`/`fs.read`/`fs.write`) and HTTP 422 for `role:"tool"` results with no wire-level
  `tool_call_id`. Dialect v2, in one deliberate invalidation: (1) built-in tool names are dotless
  (`fs_list`/`fs_read`/`fs_write`; the dotted spellings remain *invocable* — never advertised — so
  pre-rename history replays); (2) `to_chat_message` emits the strict wire shape: assistant
  tool-call turns carry `tool_calls` (string `function.arguments`, `content: null` when textless)
  and tool results carry `tool_call_id` with the serialized output as content; (3) the per-call
  `tool/call` transcript duplicates are dropped from request bodies (the enclosing assistant
  turn's `tool_calls` already carries the call id). Plain `{content, role}` messages are
  byte-identical to v1, and the event-stream payload contracts (C4/C11) are untouched — only the
  request dialect changed. Consequences, accepted: every session crossing the upgrade counts one
  `unexpected_prefix_change` invalidation (the observable cause record; pre-v2 sessions with tool
  round trips no longer replay byte-identical), and the golden hash moved to
  `b743584d…7708` (from `4a1dad31…d7ce`). Alongside, `AgentDef.tools` became the enforced mounted
  surface (`FilteredTools`, fail-closed at assembly: a listed-but-unmounted name refuses boot).

**C4 payload contracts (frozen after merge, additions-only)**: the context-bearing kinds —
`input/admitted`, `assistant/message`, `tool/call`, `tool/result` — carry a message payload in
`bytes`: `{"role": <non-empty string>, "content": <any JSON>, "synthetic"?: true}`; `content`
passes through verbatim and is never reformatted. A `compact` event carries
`{"from_seq": a, "to_seq": b, "summary": <message payload>}` with `a < b <= seq(compact)` and
pairwise-disjoint ranges; the projection emits the summary at the position of `from_seq` and skips
context-bearing events with seq in `[a,b)` exactly (skipped payloads are never parsed). The
canonical projection byte form (`context_to_bytes`: serde_json compact output, message fields in
declaration order `role`, `content`, `synthetic` only when true, object keys lexicographic —
`preserve_order` must stay disabled) is frozen stable content, as is the tool-schema serialization
owned by the provider (C8 `build_chat_body` above): both are prompt-prefix bytes, and changing
either is a constraint-3 event (observable invalidation + golden-hash update).

**C11 loop conventions (built-in ReAct loop, frozen after merge, additions-only)**: the built-in
loop (kian-loop) is a passenger inside the drain — it interacts with the engine only through the
`DrainCtx` surface (append, begin/end turn with payload, take eligible inputs at safe boundaries,
read the committed stream). Its event payloads extend the C4 contracts: `turn/start`/`turn/end`
carry `{"iteration": n}` where the counter is *derived from the stream* (count of prior
`turn/start` events — no resident loop state); `step/request` carries
`{"slot", "provider", "model", "iteration", "body"}` as the A2 audit copy appended before the
provider is called (the C8 provider-level `PersistSink` additionally covers fallback-request bytes
at the HTTP edge); `tool/call` content is `{"id", "name", "arguments"}` (role `assistant`, logged
before execution) and `tool/result` content is `{"call_id", "output", "is_error"}` (role `tool`).
An empty model response (or a `ProviderError::EmptyResponse`) is a soft failure: the loop consults
the declarative recovery table for class `empty_response` and reports `SoftFailed` — the engine
settles `failed`, never `completed`; the loop itself never retries (disposition is one-shot-guarded
and caller-owned, §2.4). Local tool execution (fs + exec) runs entirely inside `spawn_blocking`,
so a sleeping tool cannot stall concurrent drains; the built-in exec tool kills the child on
timeout and reports `timed_out` as an honest error result. Wiring: kian-loop depends on
kian-orchestrator (for `LoopDriver::Ctx = DrainCtx`) and on kian-llm (slot resolution +
`complete_turn`), matching the §7 crate map edge `KLD → KO`.

**C67 audit enrichment (additive, additions-only)**: `turn/end` payloads may carry two further
keys — both drain-activity, never context-bearing, both omitted when not applicable so legacy
payloads replay byte-identically: `"usage": <provider usage JSON verbatim>` when the provider
reported usage for the turn (the openai-compat provider parses the SSE `usage` object verbatim,
last frame wins, and a usage-only frame never flips the constraint-7 empty-response liveness
flag), and `"error": {"class": <pure classifier class>, "action": <policy disposition or
"none">}` on `soft_failure` / `error` outcomes (class from the C10 pure classifier; action from
the declarative table's disposition names; context-projection and slot errors classify as
`unknown`, engine/store errors as `store_failure`). The `ModelChunk` seam gained the additive
`usage: Option<Value>` for transport. The `kian usage-stats` command, the `GET /metrics`
`"usage"` aggregate and the `session inspect` usage lines are pure projections over these
payloads (`kian-cli::usage_metrics`, pattern of C17): per-session and per-store totals for
`input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens` plus
`requests_with_usage` / `unknown_usage_requests` counts. Usage is never normalized or priced in
core (§15).

Round-7 payload additions on the frozen C4/C11 contracts (C69/C70, same discipline as C67 —
additive top-level keys **outside `content`**, omitted when unset, never in
`derive_model_context` or request bytes): `tool/result` payloads may carry the `redactions`
provenance report (§6.2, C69) and `tool/call` payloads may carry the `permission` provenance
object (§6.2, [ADR-015](decisions/ADR-015-tool-permission-provenance.md), C70). The C4 message
shape inside `bytes.content` and the C11 key set (`iteration`, `outcome`, `tool_calls`, …) are
unchanged; both keys are drain-activity audit surface only.

### 3.3 Schema versioning

- Event envelope carries a version field; JSON encoding (v0).
- Adjacent migration chain (vN→vN+1 = one migration crate module), borrowing dsh's generation
  mechanism: committed generations are never renamed, replaced or deleted; a new generation is
  published exclusively.
- C50 (dual-backend store): one chain per backend with the *same versions and names* — sqlite
  (`?N` dialect, frozen v0/v1) and postgres (`$N` dialect, TEXT/BIGINT, same partial index, same
  `_kian_migrations` runner dispatched per backend). Envelope columns stay verbatim TEXT on both,
  so the same stream exports byte-identically from either backend (the export-parity golden).
  pg dialect notes: the claim floor max is `GREATEST(...)` (pg `MAX` is aggregate-only); the
  batch append is one CTE statement (all rows land or none); a concurrent same-session append
  that passes the fence snapshot and loses the primary-key race is turned into zero rows
  (`ON CONFLICT DO NOTHING`) or a caught 23505 and classified through the same HEAD re-read as
  every other CAS refusal (`StaleEpoch` vs `SeqMismatch`).
- C57 (`v2_inbox_admit_order`, both backends): the inbox take order is `(eligible_at,
  admitted_seq)` — a database-assigned admission ordinal (sqlite: `max+1` inside the single
  INSERT; pg: identity column), never the random uuid. Same-millisecond admits consume in
  admission order: the worker-template classification reads a stream's *first* input, and
  cross-replica replay must be deterministic.
- C69 (derived event id): every `(session, seq)` pair has a deterministic external id — `"evt_" +
  RFC 4122 v5(namespace = session uuid, name = "kian:event:<seq>")` — defined by the pure function
  `kian_core::events::event_id` and mirrored dependency-free in the SDK (`KianClient::event_id`,
  golden-pinned to the same literals, generated by an independent implementation). It is never an
  envelope field and never stored: envelope bytes stay frozen v0, and `(session, seq)` remains the
  only identity — the id is a naming convention over that pair, never a second source of truth.
  Surfaces: `kian session export --with-event-ids` adds the id to each JSONL line as a presentation
  key (opt-in; default export bytes unchanged; `import_jsonl` ignores the extra key, so the
  augmented lines round-trip — verified by test, not export-only).

## 4. Super-agent reference architecture (bundle, not core)

**Key stance (ADR-003)**: the four super-agent traits (continuous/efficient/proactive/persistent)
are **composition patterns** over Kian primitives. The article itself says "similar design
constraints derive similar architectural choices", but deployment variants abound — so it goes
into a bundle, not the core. The core only guarantees: Thread (cross-session delegation), the
event-subscription Admit entry, the BYOM execution seam, and the watchdog event source.

### 4.1 Router/Worker two-layer (expressed via Thread)

```mermaid
graph TB
    subgraph channels["Channel adapters (plugins)"]
        IM[IM]
        GH[GitHub]
        CI[CI events]
    end
    IM & GH & CI -->|subscription rule match → Admit| RS[Router Session<br/>resident · global context<br/>cheap fast slot]
    RS -->|in drain: judge / batch-dispatch / context recall| W1[Worker Session A<br/>strong model · full tools]
    RS --> W2[Worker Session B]
    RS -->|worker completion report → Router replies on channel| IM
    W1 -.->|subscription passthrough: CI events of its own PR| CI
    W1 & W2 -->|Thread delegation lineage| RS
```

- **Router session**: a resident session (idle long-lived); event judging uses the `router` slot
  (cheap fast model); it emits dispatch-decision events. Residency is mechanical (C26): the
  Router's loop driver parks between provider turns — while delegated threads are outstanding
  and while the inbox is empty — instead of settling, so the session never freezes after an idle
  final answer; completion reports and new channel events re-drive it in place.
- **Worker session**: derived via `Thread { parent_session: router, pinned_version }` delegation;
  context-isolated, full tools, `main` slot strong model. Workers never reply on the channel
  directly — completion events report back to the Router, which aggregates and replies (avoiding
  the chaos of multiple sessions replying independently).
- **Subscription passthrough**: subscription rules may carry a passthrough flag (e.g. CI events of
  a PR created by a Worker flow straight into that Worker's inbox without Router judgment) — the
  load-reduction device validated by the article. The registry is many-to-many (C38): a topic maps
  to the *set* of worker sessions that served it, and one event admits one durable row per
  subscriber (idempotent per `(session, event_id)`; a mid-set write failure is retried per
  subscriber), so workers sharing a topic observe each other's events and self-resolve conflicts
  without peer links — the Router stays the cross-task rendezvous, and only an empty set falls
  back to it.
- **Follow-up ownership** (C59, kian-68, reference 002): a person's follow-up belongs to the
  **live** Ticket's Executor. The registry (C38's `topic → subscriber set`) gains a **liveness
  projection**: every worker is `live` or `ended`, derived from `thread/completed` /
  `session/ended` on the *worker's own* stream (a Router-side end marks nothing) — a disposable
  projection exactly like the topic sets, so a rebuild from streams equals the runtime
  `register`/`mark_ended` map. Fan-out admits to **live subscribers only** (the historical set is
  kept — a worker that served a topic stays bound to it in the record); an **all-ended** topic is
  the *release to fresh evaluation* (002's Ticket-end rule): the event falls back to the Router
  with the closed `released_to_eval` reason code, the release audit line naming the topic. The
  tool-surface half of the rule: while a live Executor owns a topic the Router may only **stop**
  it (the control-plane interrupt — untouched), never requeue — `spawn_worker` whose task is the
  channel-event wrapper of an owned topic (the same derivation the registry rebuild uses), or
  whose correlation carries `{"topic": …}`, is refused with an honest tool error naming the owner
  session ("follow-up owned by live executor"); a successful channel spawn registers the new
  worker as the topic's live owner (durable lineage first — `thread/started` — then the
  projection). Config: `superagent.followup_ownership` (default **on**); `false` restores the
  pre-C59 behavior on both surfaces. Runtime ended-transitions are honest-v1 bounded: the boot
  rebuild re-derives them from streams, and `mark_worker_ended` is the runtime seam.
- **Two-level filtering** (article §3.1, C36): the *Fast Path* is an admission taxonomy over
  provenance — a config-level `classes:` table keys `(channel, topic?, sender?)` to
  `candidate | explicit-only | ignore` with most-specific-wins resolution, evaluated in code
  **before** the rule set and before any model call (`ignore` and `explicit-only`-without-signal
  drops are audit lines + counters, no row, no wake; absent classes = today's behavior). The
  *Slow Path* is the Router's judge prompt (C26): it is the natural-language policy extension
  point — admission semantics that would need semantic judgment are edited as prompt text, not
  code.
- **Admission Inline/Spill shaping** (C33): the additive `superagent.shaping:` config carries
  `inline_cap_bytes` (absent = infinity, hence byte-identical legacy behavior), `first_chars`
  (default 50), and `neighbor_fields`. The subscription service applies it exactly once, after
  routing and immediately before the inbox row exists. At or below the serialized-raw byte cap,
  the existing `channel_event` wrapper is unchanged. Over the cap, `content` instead carries an
  Inline view (a strictly shorter excerpt, canonical `topic`/`event_id`, and any configured
  top-level scalar association ids), `"spill": true`, and `continuation: {event_id}`; the C32 row
  envelope carries the verbatim raw as its sidecar, producing the atomic admitted/spilled pair at
  the ordinary consumption write. Idempotent `(session, event_id)` admission still dedupes the
  whole pair. There is no retroactive re-shaper: committed context remains immutable and
  `derive_model_context` stays verbatim. Neighbor inlining v1 reads only fields on this payload;
  it never fetches cross-event IM history (that is a C35 recall hop).
- **Batching**: the Router drain batches naturally (a drain has "drain the queue" semantics —
  eligible inputs are consumed in one sweep) — matching the article §7.2 batch-merge experimental
  conclusion. The §7.2 observability shape is bundle-owned counters (C41), recorded by the
  resident Router's driver around each judge turn and snapshotted on the merged `/metrics` route
  as `superagent.router_batching`: `judge_turns`, `inputs_consumed` (batch size),
  `turn_duration_ms_total` / `mean_turn_duration_ms` (per-request wall duration),
  `amortized_ms_per_input` (duration/inputs — per-decision amortized latency), the bounded
  `batch_histogram` (buckets 1…128 plus overflow) and `last_turn`.
- **Speculation** (article §3.4, as a policy pattern over existing primitives): the Router may
  dispatch a worker on a *weak* signal by calling `spawn_worker` with `speculative: true`. The
  delegation wrapper gains one additive, model-visible field — `"speculative": true` inside the
  task `content` — so the worker's first message honestly says it was spawned on a weak signal,
  and the marker is audit-visible forever (the wrapper bytes live in both streams; legacy wrappers
  parse and replay byte-identically). The Router profile's `speculation: {max_outstanding: n,
  label: …}` config bounds concurrent speculative threads: the spawner enforces the cap with an
  honest tool error the Router model sees and reacts to (never a silent refusal), and the
  outstanding count is *derived from the Router's event stream* (`thread/started` without a
  consumed completion report — a pure projection, so the budget survives Router restarts).
  The article's silent-repro delivery policy ships as a **prompt template** on the Router profile
  (deliver only after the bug is confirmed *and* a simple fix found; otherwise stay quiet) — a
  config artifact, not code. Deliver-or-discard is the existing channel-policy gate: a discarded
  speculation simply never calls `reply` (no `channel/delivered`), while the speculative worker's
  own stream still audits every token spent — honest by construction. Per-session budgets (A5)
  are untouched; the cap is the Router-profile speculation budget.
- **Thread/Correlation intake projection** (C61, kian-67, ADR-013 draft): the additive
  `superagent.intake.thread_keys: [{channel, key: [group?, <payload-field>…]}]` config derives the
  canonical thread key `<channel>:<group>#<key>` at adapter mapping time — deterministic and
  collision-free by construction (reserved characters percent-encoded per component), and
  **config-owned**: a config change is a new key, old streams keep their topics (cache-stable, no
  rewrite). After derivation the key *is* a routing topic — the subscription rule matcher gains no
  correlation concept and is unchanged. The **source read position** (a restart must not replay)
  is derived from the last admitted event's payload cursor fields per source kind (lark
  `message_id` offsets; GitHub REST `since` timestamps, `updated_at`); where a source exposes no
  derivable cursor the honest v1 bound is idempotent re-poll under the existing `event_id`
  idempotency — `deduplicated` admissions, zero duplicate rows, **no position table** (rejected as
  pre-event durable state outside the event log). **Correlation adds context, never routes**: the
  derived key surfaces in the C33 Inline `neighbors` block (`thread_key`, an association id for
  recall hops through the C35 toolpack). Normative, with ADR-012: **fan-out grants no authority**
  — a fanned-out message reaches live subscribed tickets as context bounded by their Mandates, and
  no admission check runs on it (001's rejected-concepts rows, kept); one trigger fanning out to
  many tickets is the C38 fan-out mechanics restated under the authority rule.
- **Closed decision-reason vocabulary** (C57, reference 005): every intake decision — the three
  drop shapes, the empty-set fallback, every admitted row — stamps a machine-stable
  `reason=<code>` into its audit line, and the assembly's subscription-metrics snapshot counts
  per code (`by_reason`, every code of the closed set materialized). The closed
  `IntakeReason` set is `no_rule | class_ignored | not_explicit | admitted_router |
  admitted_passthrough | deduplicated | released_to_eval` (the last is produced only by the
  follow-up-ownership release rule, C59 — see the "Follow-up ownership" bullet); a
  deduplicated admission reports `deduplicated` (the
  row already existed — no new decision was spent), a `Route::Session` fixed target reports
  `admitted_passthrough` (the shared "no judge turn spent" code — the closed set has no
  per-route variant). The codes ride **audit lines + counters only**: `RoutingOutcome`'s shape
  is frozen (no ripple), consumers match the enum (never a string parse upstream), and zero new
  event kinds exist — the durable trace of a Router decision stays the existing
  `tool/call`/`thread/started`/`channel/delivered` triple. The *judge-side* convention is a
  template sentence (the soft half): the Router is instructed to tag each batch decision in its
  reply text as exactly one of `decision:accept`, `decision:stop`, `decision:ignore`,
  `decision:unavailable` (the closed `DecisionKind` set) — model-visible convention, not a
  parsed contract; template discipline per the C43 note pattern (new sessions render the new
  default, persisted prompts replay byte-identically; the template constant lives in
  `templates.rs` beside the other template artifacts, re-exported under its historical path).

### 4.2 Channel bandwidth matching (output policy)

Policies shape the reply **before the send** (`ChannelDelivery` carries already
policy-shaped content); the adapter delivers what it is given. Policy code lives in the
bundle (`kian-superagent`), never in a channel adapter and never in the core. Every
successful send is followed by a `channel/delivered` event; a policy rejection (e.g. a
structured payload missing a section) is an honest error to the loop — never a silent
send. Composing **before** the shaping policies is the output-side speak-gate (C58,
`superagent.speak_rules`): a *whether-to-send* decision over the closed condition
vocabulary `always | ack-first-only | decision-needed | result-ready | never` — a
suppressed send is an honest tool error naming the rule and condition (the model sees it
and reacts), an audit line + counter, and no send ⇒ no `channel/delivered` (the C39
deliver-or-discard discipline). `ack-first-only` keys its confirm-once memory in
`SpeakTracker`, a **disposable projection** over the Router's own `channel/delivered`
events rebuilt from the stream at boot, so "first ack only" survives restarts; absent
table = every send passes, byte-identical.

Composed after the gate (and after the shaping policies), the **outbox bounded retry**
(C62, `superagent.outbox` — absent = one attempt, the honest adapter error,
byte-identical) wraps the send itself: `OutboxDriver::deliver_with_retry` re-executes the
same policy-shaped `ChannelDelivery` bytes with backoff `2s·2^(n-1)` cap 60s, at most
`max_attempts` (default 6) total, serialized per `(ticket, conversation)` — a later message
of the same key waits for the earlier one (its ack precedes its result); other tickets and
other conversations never wait (003's channel-failure rules). The ticket is the optional
`correlation` tool argument (the `spawn_worker` vocabulary — thread id or topic; absent ⇒
the Router session itself), the conversation is `(channel, speak_key(target))` — the C58
canonicalization reused. An exhausted delivery **parks honestly**: an `outbox park:`
tool error (a stable-text prefix, the C57 vocabulary discipline — the rebuild's
disambiguator between parked and speak-suppressed), an audit line, an `OutboxMetrics`
counter, the C56 `delivery-retry` stage arm (the outbox view is real), and the parked
conversation named on `kian tickets` — never a silent swallow, never an unbounded loop.
New receipts carry additive `attempt`/`first_attempt_ms` keys (legacy receipts parse
unchanged). The outbox itself is a **disposable projection** over `tool/call` (intent,
log-before-execute) + `tool/result` (call_id pairing) + `channel/delivered` (receipt):
rebuild classifies each intent — no result ⇒ pending (a kill -9 mid-retry; the boot
re-drive re-sends it, at-least-once), park-prefixed error ⇒ parked (visible, terminal for
the driver — a person releases), other error ⇒ suppressed/invalid/legacy (never re-driven),
receipt/ok ⇒ delivered. No durable outbox state exists anywhere.

| Channel | Output policy (bundle built-in templates) |
|---------|-------------------------------------------|
| Any channel (C58) | speak-rules: the *whether-to-send* gate (`superagent.speak_rules: [{channel, when, per_topic_ack}]`, first match per channel) evaluated before the shaping policies below; `decision-needed` admits the two-way `reply` surface, `result-ready` admits `deliver_structured`, `ack-first-only` confirms once per topic (or channel), `never` mutes — suppression is honest (tool error + audit + counter), never a silent swallow |
| IM | ≤100-word hard rule; only direct questions / short conclusions; notify only when a decision or acceptance is needed |
| GitHub PR/Issue | Structured delivery: problem background / change notes / verification results |
| GitHub (adapter, C48) | the second *real* adapter beside lark, the S4 "group-chat message + CI events" family (`issues`/`issue_comment`/`pull_request`/`push`/`check_run`/`status`): receive is webhook-shaped deliveries verified by the C49 receiver surface (HMAC precedes parsing) and handed to the adapter, which maps each delivery to one provenance-carrying `ChannelEvent` (delivery id → `event_id` idempotency, repo → `group`, `owner/repo` or an exact `owner/repo#N` key → `topic` from the config table — the table is the receive allowlist —, verbatim sender, adapter-declared `explicit` on @-mentions in human text); send is PR review comment / issue comment / PR description via the REST API, resolving only on 2xx + id acceptance, structured content rendered as markdown sections; token env-only (`KIAN_GITHUB_TOKEN`, fail-closed when send is configured; receive needs none); the channel is untrusted by default (C43) |
| Email/docs | Structured summary drafting |
| Mock (in-memory) | built-in first-class bundle adapter (production code, not a test stub): the S4 IM stand-in, same ImPolicy/StructuredPolicy enforcement through the Router's dispatch tools |
| Lark/Feishu | the first *real* adapter (C44): IM row — same ImPolicy (≤100 words) through the Router's dispatch tools; receive is an adapter-side chat-history poll mapped to provenance-carrying `ChannelEvent`s (chat id → group, sender → sender), send resolves only on API acceptance; credentials env-only (`KIAN_LARK_APP_ID`/`KIAN_LARK_APP_SECRET`, fail-closed at startup); the channel is untrusted by default (C43) |

### 4.3 BYOM endpoint (kiand daemon / endpoint SDK)

```mermaid
graph LR
    subgraph cloud["Kian (cloud/server)"]
        ORCH[orchestrator<br/>runs LoopDriver inside drains]
        SEAM[exec seam<br/>ExecBackend]
        WD[watchdog v1<br/>server-side heartbeat monitor]
    end
    subgraph endpoint["Endpoint machines (two forms)"]
        KD[kiand<br/>daemon binary]
        EP[kian-endpoint SDK<br/>embedded in any process]
    end
    ORCH --> SEAM
    SEAM <-->|KiandProto bidirectional stream:<br/>cmd/result + heartbeat| KD & EP
    WD -.->|heartbeat freshness over the endpoint registry| KD & EP
    WD -.->|health events → Admit| ORCH
```

**Endpoint access forms (same protocol, either or both)**:

| Form | Carrier | Fits |
|------|---------|------|
| daemon | `kiand` (= kian-endpoint library + thin main) | Resident on personal dev machines/servers (primary BYOM form) |
| SDK | `kian-endpoint` crate embedded in any process | Harnesses (the dsh process itself is an execution endpoint), user applications |

- The endpoint **keeps no session state** (brain-hand separation): it only executes commands and
  returns results; session logs live in Kian's EventStore.
- **The v1 watchdog is the server-side heartbeat monitor** (kian-exec, C21): a dead daemon cannot
  report its own death, so the authoritative health view is the server's heartbeat-absence
  observation over the endpoint registry — offline when `now - last_heartbeat > offline_after_ms`,
  online again on a fresh heartbeat; edge-triggered (one event per state change, no flapping), and
  freshness-based even when the transport is still up (a silent-but-connected endpoint is offline).
  Endpoint-side self-checks / anomaly self-healing remain **Phase 3**.
- The watchdog emits health events (endpoint offline) → they flow through
  Admit into the related session inbox (event-driven self-healing); the watchdog itself never
  writes session streams.
- Intermittent endpoint presence is the norm: commands queue in durable state and are consumed
  after the endpoint reconnects (BYOM's offline-tolerant semantics).
- **The two axes of separation**: where execution runs (ExecBackend provider: local /
  remote-endpoint / sandbox / cloud machine) and who the loop is (LoopDriver seam, see §6.2) —
  independently pluggable.
- **Async `start()`/`stop()` tool pattern (C37, ADR-008)**: the article's §4.1 per-session
  full-duplex in kian-native shape. `start` enqueues through the same durable queue and returns
  immediately with `{"status":"started","job_id":…}` (the drain is never blocked; the loop is
  untouched); an `async_jobs` row — pre-event durable state per ADR-007's pattern, committed
  before dispatch, job id = command id derived from the tool-call id — records *who to notify*.
  On settle, the kian-cli completion bridge admits a provenance-wrapped
  `{"kian":{"type":"async_result",…}}` envelope to the originating session's inbox and wakes it
  (invariant 1); the completion is promoted at the next safe boundary as a normal message.
  `stop` marks the job `cancelled` and kills best-effort (the v0 wire has no kill frame); the
  cancelled completion still lands, labeled `cancelled`. Completions to terminal sessions drop
  with an audit line. The pair mounts on the Router/Worker profiles config-gated
  (`superagent.async_tools`, default off).
- **Watchdog Session v2 (C40)**: the article's §6 progress judge — a *model-in-the-loop* resident
  session, not a health poll. It is a worker-template agent (own strong `main` slot) whose tools
  are exactly the read/intervene surface: `list_threads` / `read_progress` over `RecallPort`
  (thread status, last assistant message, age of outstanding threads), `intervene(thread_id,
  text)` — which admits a steer-marked `{"synthetic":true,"content":…}` row to the stuck worker
  through `InboxWriter` (control-plane-legal, no engine API; durable before the wake, consumed
  at the worker's next provider-turn safe boundary) — and `escalate(channel, text)`, a normal
  reply under the IM policy (≤100 words, `channel/delivered`). Each input is a timer tick; the
  drain is one judge turn, and every outcome — no-op (the default), nudge, escalate — is honestly
  in the watchdog session's own stream. **The timer input source is a channel** (`TimerChannel`,
  a production `ChannelAdapter` beside `MockChannel`, emitting
  `ChannelEvent{channel:"timer", topic:"watchdog", event_id:"tick-<n>", payload:{"tick":n}}` on a
  configurable, clock-injected cadence) routed to the fixed session by an ordinary subscription
  rule through the additive `Route::Session(SessionId)` variant (config-addressed, generic —
  nothing in the vocabulary names a watchdog). Rejected alternatives: an in-process interval
  admitting rows directly (ticks would leave no durable audit, bypass the pipeline's `event_id`
  idempotency, and fork the admission path) and a `TimerSource` core seam (constraint 10 —
  nothing generic justifies a seam). Ticks are idempotent by `event_id`, so a restart that
  re-emits `tick-3` dedupes against the committed row; ticks missed while the server was down
  simply never happened (the stream honestly shows no check ran); `emit_on_start: true` fires one
  immediate first check after restart. The v1 heartbeat watchdog (C21, above) is untouched —
  endpoint health and worker progress are orthogonal loops.

### 4.4 Security boundary (Phase 3+, stake planted)

Risks from article §8.4 are accepted as-is: a compromised cloud-side agent loop controls every
endpoint; endpoint credentials entering the model context leak. Kian's response roadmap: the exec
seam's data-egress tiering (A8) + credential-less use (future) + endpoint execution approval
policy (policy plugin).

### 4.5 The recall toolpack (Chain of Recall, C35)

The super-agent's working set is a **reference graph**: Trigger (a channel event), Thread (a
delegation) and Worker (a session) are objects that carry ids — the event id on the admitted
wrapper's Inline view, the thread id on `thread/started`, the worker session on both. Nothing
copies another object's content into a session's own context: recall *walks the graph on demand*
through three read-only `ToolProvider` tools mounted on the Router profile (config-gated,
default on) and per worker template (opt-in, `superagent.recall`):

- `recall_event(event_id)` — surfaces a spilled channel raw: the Inline/Spill-shaped view plus
  next-hop ids (the locality tool, the topic);
- `recall_neighbors(event_id)` — the locality view: channel events on the same topic within a seq
  window around the anchor, each entry Inline-shaped with its event id as the next hop;
- `recall_thread(thread_id)` — the thread's task, its status (from the worker's
  `thread/completed`, else `outstanding`) and the final summary, with the worker session as the
  next hop.

Every result is itself Inline/Spill-shaped — summaries + ids, reusing the C33 shaping vocabulary
(`first_chars` excerpts, `neighbor_fields` association ids) — so **multi-hop recall composes**:
each hop returns bounded views and the ids of the next hops, never a full context transfer. The
verbatim-in-one-output bound is the toolpack's own `output_cap_bytes` (default 8 KiB): above it a
surfaced value becomes the Inline view (excerpt + continuation handle) — no unbounded raw
re-injection, and the cap applies to tool outputs exactly as it applies to admission. Untrusted
hygiene (C43) survives every hop: a surfaced raw re-wraps through the `raw_view` marker keyed off
its sibling admitted content.

Results land as ordinary `tool/result` events appended at the tail by the unmodified built-in
loop — persisted before use, byte-stable thereafter, never reordering prior context (constraint
3; the log-before-execute `tool/call` discipline already holds). Reads go exclusively through the
`RecallPort` (C34): the spill index is the fast path, and a raw spilled after the startup rebuild
is found by scanning the Router stream and the workers it names — the stream stays the sole truth
and the index stays a disposable cache (constraint 1).

**Router global observability rides the event stream via recall, not Router context.** The
Router's own context stays the cheap Inline views + ids of what arrived; anything more (a spilled
raw, a sibling event, a worker's summary) is one recall hop away, audited as a `tool/result` on
the Router's stream. No capability may push global state into Router context — that is the
cache-byte-stability and cost discipline of §3.2/§4.1 applied to the super-agent layer.

### 4.6 Resident re-arm (kian-62)

A resident session (the Router, the watchdog) can end **terminally** — `session/ended{completed}`
(freeze) — most mundanely because the loop's iteration budget is *lifetime*-scoped (the counter
counts every `turn/start` ever committed), so a long-lived resident exhausts it and the engine's
`Stopped → Completed` path freezes the stream. Once terminal, the engine's drain projects
`Terminal` forever: later inputs stay pending, and the (already consumed) idempotent bootstrap
row cannot re-wake the session. A **soft failure** (`session/ended{failed}`) is the other story:
it projects back to `idle` (§2.2) and the *next admitted input* re-opens the same session — no
re-arm needed and none happens.

Re-arm semantics (zero new event kinds; the chain is derivable from persisted state):

- **Successor chain.** `successor(s) = uuid5(s, "kian:resident:re-arm")` — the name string is
  frozen contract (constraint-3 spirit: changing it orphans every existing chain). At boot,
  assembly walks from the config-pinned session down the chain: every link whose stream projects
  terminal is recorded (session + end reason) and skipped; the first empty-or-live link is the
  **live** session and is bound everywhere the pinned id used to go (bootstrap target, inbox
  residency counter, worker completion reports, subscription fallback, driver routing, and the
  timer rule's `Route::Session` target for the watchdog). The walk refuses to boot (fail-closed)
  past 64 ended links — at that point the chain is growing faster than any honest watch.
- **Observable cause.** Generation 0 bootstraps byte-identically to the pre-kian-62 behavior
  (legacy key `router:bootstrap` / `watchdog:bootstrap`, plain judge prompt). A successor
  generation bootstraps under the per-generation key `"<prefix>:re-arm:<successor uuid>"`
  (deterministic successor ids ⇒ every reboot dedupes onto the same row) with the judge prompt
  prefixed by `[re-arm] … predecessor session(s) ended and are frozen: <uuid> (<reason>), …` —
  the cause is on the stream as the successor's first `input/admitted`, derivable, replay-stable.
- **Rebuilds span the chain.** The passthrough-registry and spill-index rebuilds scan every
  frozen predecessor stream plus the live session (predecessors still carry `thread/started`
  lineage and spill envelopes).
- **Honesty unchanged.** No fake completions, no catch-up ticks: inputs that arrived while the
  predecessor was terminal were never consumed and stay honestly pending on the frozen stream.
  The successor starts with a fresh in-context view (one possible re-draft across the succession
  boundary — observable, not hidden). Known interaction: with the lifetime iteration budget
  unchanged, a resident Router will chain successors at every budget exhaustion; moving the
  budget to per-episode for resident loops is a follow-up (tracked), not a contract change here.

### 4.7 Bundle domain-policy layer (Phase-6 seed)

The Phase-6 round (plan round-6, SPEC §5 P6a/P6b/P6c) lands the *domain-policy* layer of the
dsh-superagent-prototype onto the bundle substrate: ticket semantics, mandate, attention
economics and dogfood validation — additions-only, zero new event kinds, everything a
disposable projection, a persisted-bytes marker, or a template/config artifact. Each card noted
its slice here as it landed; this is the reconciled whole-section record (closeout C66 / kian-78
— including the Wave-C gap fixes kian-74…77, which post-dated the per-card notes below and are
recorded in the closing bullets).

- **Closed decision-reason vocabularies (C57, kian-65)** — the panel's "Decision reasons from a
  closed vocabulary" (reference 005) in kian's shape: the closed `IntakeReason` code set
  stamped as `reason=<code>` on every drop/admit audit line of the subscription service and
  counted per code in the assembly metrics snapshot (`by_reason`, stable series shape), plus
  the judge-side `DecisionKind` decision-tag convention (`decision:<accept|stop|ignore|
  unavailable>`) rendered into the default judge-prompt template. Codes ride audit + metrics,
  never the `RoutingOutcome` enum; the durable trace of a Router decision stays the existing
  `tool/call`/`thread/started`/`channel/delivered` triple; `released_to_eval` is defined now
  and produced only by the follow-up-ownership release rule (C59). Full placement note: §4.1
  bullet "Closed decision-reason vocabulary".
- **Ticket stage projection (C56, kian-64)** — "what is it waiting for" (reference 003's
  first-match stage table) as a **pure derived projection** (`kian-superagent` `stage.rs`):
  a kian Ticket v1 = one delegated worker session (one `thread/started`), and its stage is
  `project_stage(worker_stream, pending_inbox, outbox_view)` — first match wins over
  `delivery-retry` (outbox view pending/parked; the stub was empty only until C62/kian-71
  wired the real `ThreadOutboxView`, and kian-76 made a parked intent survive reboot and
  surface per-ticket), `awaiting-answer`
  (latest structured report `waiting`, thread outstanding, no input since), `awaiting-review`
  (latest `ready`), `blocked-failed` (failed thread/session end, no input since — a message
  releases it), `queued` (input admitted, no claim since), `idle` (success/terminal-completed),
  `working` (else). Structured facts only (001: "waiting on a person" is a structured field,
  never prose inference): the report state rides the **additive** `"state"` key on
  completion-report content (`thread_report_content_stated`; legacy reports parse with
  `state: None`, byte-identical). Persisting a stage would make a disposable projection
  authoritative — rejected. Read surface: `kian tickets [--db] [--json]` (read-only embedded,
  uuid-ascending board). Known v1 deviation, recorded honestly: **Ticket v1 = one worker
  session; an ended ticket resumes only via fresh Eval, not same-Mandate resume** (C59's
  release path is the follow-up).
- **Output-side speak-rules (C58, kian-66)** — attention economics on the send path
  (reference 000: "speak only when a person must act or can use a result; confirm once,
  stay quiet while working"): the output-policy table (§4.2) gains the
  **whether-to-send gate**, `superagent.speak_rules: [{channel, when, per_topic_ack}]`
  over the closed condition vocabulary `always | ack-first-only | decision-needed |
  result-ready | never` (`policy.rs`: `SpeakRule`/`SpeakCondition`/`SpeakPolicy::check`,
  pure). The gate composes **before** `ImPolicy`/`StructuredPolicy` shaping on the
  Router's `reply`/`deliver_structured` tool paths; the send kind is derived from the
  tool (reply = decision, deliver_structured = result), never model-declared. A
  suppressed send is an **honest tool error** naming the rule and condition (the model
  sees it and reacts), an audit line + per-condition counter (`SpeakMetrics`), and no
  send ⇒ no `channel/delivered` (the C39 deliver-or-discard discipline — there is no
  event kind for "did not happen"). The `ack-first-only` confirm-once memory is
  `SpeakTracker`: a **disposable projection** over the Router's own `channel/delivered`
  events keyed `(channel, target)`, rebuilt from the stream at boot, so first-ack-only
  survives restarts (constraint 1). Absent table = every send passes, byte-identical
  (zero sanctioned test edits). Full placement note: §4.2 speak-gate paragraph + table
  row.
- **Follow-up ownership routing (C59, kian-68)** — reference 002's rule ("a person's
  follow-up belongs to the live Ticket's Executor; the Router may only stop") landed as
  policy over the existing C38 fan-out, zero new event kinds: (1) `registry.rs` gains the
  worker-status projection (`WorkerStatus::Live | Ended`, from `thread/completed` +
  `session/ended` on worker streams — never a Router-side end) with
  `resolve_live(topic)` / `live_owner(topic)` beside the historical `resolve`; a rebuild
  from a mixed live/ended stream equals the runtime `register`/`mark_ended` map (the
  projection is disposable, constraint 1); (2) `subscriptions.rs` admits fan-out rows to
  **live subscribers only** — an all-ended topic falls back to the Router as the
  **release to fresh evaluation** (002's Ticket-end rule), audited with
  `reason=released_to_eval` (the C57 reserved code's producing path — audit lines name
  the released topic; the `by_reason` counter of the release stays a known v1 bound, the
  assembly counts the resulting Router admit through the shared `routed_reason`
  mapping); (3) `router.rs` enforces the rule at the tool surface: `spawn_worker` with a
  topic correlation (task-as-channel-wrapper, the registry rebuild's own derivation, or
  an explicit `{"topic": …}` correlation) that a **live** worker already owns is refused
  before any side effect with an honest error naming the owner session — no thread id,
  no row, no `thread/started`; a successful channel spawn then registers the new worker
  as the topic's live owner (lineage first, projection second); stopping stays available
  (control-plane interrupt — untouched, that *is* the Router-may-only-stop rule in
  kian's vocabulary). Config `superagent.followup_ownership` default **on**; `false`
  restores the pre-C59 behavior on both surfaces (every existing suite stays green
  unmodified — the sanctioned `tests/fanout_audit.rs` edit set resolved to zero: all its
  fixtures register live subscribers). Full placement note: §4.1 bullet
  "Follow-up ownership".
- **Thread/Correlation intake projection (C61, kian-67)** — reference 001's Source/Thread/
  Correlation rows as **derived projections** (`kian-superagent` `intake.rs`, bundle-only —
  pure functions over `ChannelEvent`s and admitted stream events, no store type named): the
  config-owned canonical thread key `<channel>:<group>#<key>` (deterministic, collision-free by
  construction via per-component percent-encoding of the reserved characters; a config change is
  a new key, old streams keep their topics — cache-stable), the stream-derived source read
  position (lark `message_id` offsets; GitHub REST `since` timestamps via the pure adapter
  cursor helpers `github::payload_since_cursor` / `lark::payload_message_cursor`), and the
  honest v1 bound where no cursor derives: idempotent re-poll under `event_id` dedupe —
  `deduplicated` admissions, zero duplicate rows, **no position table and no
  `intake/positioned` kind** (both rejected, ADR-013). Correlation is context only: the key
  surfaces in the C33 Inline `neighbors` block for recall hops and never routes (the
  subscription rule matcher is unchanged). Normative companion recorded in the same ADR and
  §4.1: **fan-out grants no authority; bounded by Mandate (ADR-012)** — no admission check runs
  on a fanned-out message. Full placement note: §4.1 bullet "Thread/Correlation intake
  projection".
- **Policy/prompt versioning + drift marking (C63, kian-69)** — the prototype's versioned
  Policy (rules + accept mode + source parts, references 001/007) in kian's shape, zero new
  event kinds (`kian-superagent` `policy_version.rs`, pure): the configured revision
  (`superagent.policy.version: <label>`, absent = today's bytes, byte-identical) rides the
  **rendered** bootstrap prompt of the resident Router and watchdog as a `[policy <label>]`
  header — added at render, so the template text (the `JUDGE_PROMPT_TEMPLATE` goldens) is
  untouched and legacy persisted prompts replay byte-identically (constraint 3); the
  `PolicyVersion` also carries sha256 display fingerprints of the prompt templates the label
  was pinned against, so a label always names template bytes. Boot-time drift marking
  (assembly `start_services`) compares the **live** resident sessions' persisted bootstrap
  headers against the configured version: an audit line + the `superagent.policy_drift`
  counter — and **live sessions are never rewritten** (cache-byte stability: a drifted
  session keeps its persisted prompt; only the next bootstrap renders the new header; the
  idempotent bootstrap key guarantees it). Config-gated `superagent.policy.notify_live`
  (default **false**): when true, a drift delivers **one ordinary admitted input** to each
  affected live session — the marker text `[policy <old>→<new>] defaults updated; …`,
  synthetic-marked and strippable (the watchdog steer-row vocabulary, constraint 2),
  idempotent by the stable event id `policy:drift:<old>-><new>` (a `(session, key)` admit;
  each upgrade notifies exactly once), admitted through the resident's **mounted inbox**
  (the residency counter a parked resident polls — a raw-engine admit would strand behind
  the park). The version is replayable: it lives in persisted bootstrap prompts and marker
  inputs — derivable from the stream, no `policy/version` kind (rejected: the persisted
  prompt is the record).
- **Delegation mandate (C60, kian-70, ADR-012)** — the authority object separating *what to
  do* (task) from *with what authority* (Mandate; references 001/002) lands as an
  **additive `"mandate"` object on the C18-frozen delegation wrapper** (the C39
  `speculative`-marker mechanism applied to an object — zero new event kinds, `Delegation`
  stays its frozen shape, a companion parser `delegation_mandate` reads the key, legacy
  wrappers parse with `None`): `{strength: explicit|inferred, scope, granted_by, source
  {channel, topic}, policy_version, matched_rules?}`. **Immutable** — carried verbatim
  inside `thread/started`'s task payload (durable + audit-visible forever on both sides of
  the delegation pair) and model-visible as the worker's first message (the wrapper *is*
  that message); a wider scope is a new Ticket, never a mutation. **Issuance**:
  `spawn_worker` is the single issuance point (`router.rs` `build_mandate` over
  `superagent.mandate` config — per-worker-template scope texts `scopes`/`default_scope`,
  fail-closed non-blank), built from the triggering input's provenance wrapper under the
  C43 untrusted discipline: `strength` is `explicit` **only** for the adapter-declared
  `explicit: true` payload field (never text-derived consent), `granted_by` is the sender
  provenance verbatim (honestly `null` for sender-less machine events; the Router session
  for non-channel spawns), `source` is the wrapper's channel/topic, and `policy_version`
  stamps the current C63 pointer. Config absent = no mandate, byte-identical wrappers.
  Speculative delegations carry mandates too (the C39/C60 markers compose — weak signal,
  bounded authority). **Revalidation** (composed with the C59 live routing, additive to
  its frozen semantics): before a follow-up admits to a live subscriber, the registry's
  mandate projection (`register_mandate` at spawn, strictly after the lineage write;
  re-derived by `rebuild` from `thread/started` task payloads — disposable, restart-safe)
  is revalidated (`worker.rs` `revalidate_mandate`, pure): `explicit` passes;
  `inferred` passes while its `policy_version` equals the current pointer (the same
  configured label the bootstrap renders — assembly wires one pointer to both surfaces);
  a stale `inferred` mandate drops the worker from the admission set with an honest
  per-worker audit line, and a set emptied by releases falls back to the Router — the
  same `released_to_eval` release-to-fresh-eval as the all-ended path. Legacy workers (no
  mandate object) always pass — pre-C60 authority is never retroactively narrowed. The
  worker template doc restates the mandate as prompt text (`templates.rs`
  `WORKER_PROMPT_TEMPLATE`/`worker_mandate_line`) — restatement is documentation, never
  widening. Normative record: **ADR-012**. kian-75 follow-up (RUNLOG §7.2 G2): live, the
  model serializes the wrapper as a JSON *string* in `task` plus a bare-string `correlation`,
  and the C60 issuance path only read the strict object forms — live spawns degraded to
  `inferred` with `source: {channel: "router"}` despite the adapter-declared `explicit: true`.
  `provenance.rs` now feeds `spawn_topic` and the mandate builder the same lenient reading
  (`parse_channel_marker_lenient` + `correlation_topic`, the one derivation in
  `registry.rs`), so the explicit signal and the wrapper's channel/topic survive the live
  spellings.
- **Outbox bounded retry (C62, kian-71)** — stored work for the **write side** (reference
  003: "outgoing messages are stored and sent as stored work, retried with backoff …
  a message waits while an earlier one of the same Ticket to the same conversation has not
  gone out, until that one … shows as a problem") landed after the card's Step-1
  **delivery-kind reuse audit** — none of the three existing retry surfaces is reusable
  (lark's adapter-internal auth refresh-and-retry is transport-bound; the loop's model
  backoff retries provider turns, not sends; `subscriptions.rs` leaves a failed admit to
  the *adapter's* idempotent re-delivery — the read side has a re-sender, the write side
  had none), and the stream already carries intent + receipt, so the outbox is a
  **disposable projection plus a bounded driver** — no new durable state, no new kinds,
  no ADR (`kian-superagent` `outbox.rs`). Identity: ticket = the optional `correlation`
  argument on `reply`/`deliver_structured` (the only ticket identity that survives a
  restart rebuild without new durable state; absent ⇒ the Router session), conversation =
  `(channel, speak_key(target))` (the C58 key reused). Ordering per `(ticket,
  conversation)` via a fair per-key mutex held across one `deliver_with_retry` — a later
  message of the same key waits, everything else never waits. Backoff `2s·2^(n-1)` cap
  60s, `max_attempts` 6 (clock-injected; `ManualRetryClock` records the sleep schedule —
  the `ManualTurnClock` discipline). **Parked honestly**: `outbox park:` tool error (a
  stable-text prefix — the rebuild's disambiguator between parked and speak-suppressed,
  which leave identical `tool/call` + error-`tool/result` shapes), audit line,
  `OutboxMetrics` counter, the C56 `delivery-retry` stage arm (C62 wires the real
  `ThreadOutboxView` — kian-64's stub exemption), the parked conversation named on
  `kian tickets`. Restart: `outbox_pending(router_stream)` classifies each intent —
  result-less ⇒ pending (kill -9 mid-retry; the boot re-drive re-sends, at-least-once,
  and the receipt it writes satisfies the intent on the *next* rebuild — the receipt pass
  exists exactly so a re-driven intent never re-drives forever), park-prefixed ⇒ parked
  (never re-driven — a person releases, 003), other error ⇒ suppressed/invalid/legacy
  (never re-driven — re-driving would duplicate honestly-not-made sends). New receipts
  carry additive `attempt`/`first_attempt_ms` (the C39 marker precedent; C4/C18 frozen,
  legacy receipts parse unchanged). Config `superagent.outbox` (absent = off =
  byte-identical single attempt); at-least-once duplicates under concurrent tickets are
  the documented bound (the plan's risk-4 note). kian-76 follow-up (RUNLOG §7.2 G3): the
  boot rebuild now parses the live `ToolError`-wrapped park shape (react.rs renders parks
  as tool errors — the original rebuild matched only the bare prefix), and a live intent's
  ticket key resolves through both the thread's admitted wrappers and its registry topics,
  so a parked delivery survives reboot (visible in the rebuild's `pending`/`parked` counts)
  and surfaces per-ticket on `kian tickets`; a parked delivery still waits for a person —
  the boot states it, never re-drives it. Full placement note: §4.2 outbox
  paragraph.
- **Issue-watch multi-Ticket fan-out (C64, kian-72)** — the P6c dogfood shape: `github.rs`
  gains an optional **poll-receive mode beside the (untouched) C49 webhook surface** —
  config `github.poll {repo, interval_ms, kinds[issues, issue_comments], since_field,
  backfill_ms}` drives the REST list endpoints with `since` cursors (the C61 read
  position: the cursor derives from the last admitted event's payload; a fresh deployment
  admits at most the bounded `backfill_ms` window, never repo history), token env-only
  (`KIAN_GITHUB_TOKEN`, falling back to the invoking user's `gh auth token`). Polled
  items map through the existing C48 mapping unchanged: delivery id = `event_id`
  (idempotent re-poll), topic = the C61 thread key `issue-github:<owner>/<repo>#<N>`,
  admission classes per C36 (bot/CI ignored, human comments candidate, adapter-declared
  `explicit` on issue authorship → ADR-012 mandate strength). The example
  (`examples/issue-watch` v2; the timer v1 kept verbatim as `config-timer.yaml` for the
  regression demo) composes the whole Wave A/B stack on one source: the Router judges
  new issues (accept with Mandate → `spawn_worker` per issue topic), follow-ups admit to
  the live triager with zero Router judge turns (C59), ended workers release to fresh
  Eval, drafts deliver once under speak-rules (C58) retrying through the outbox (C62),
  `kian tickets` shows the live stage board (C56) — fixture-proven in CI
  (`tests/github_channel.rs` poll suites, `tests/issue_watch_fanout.rs` end-to-end),
  no default-CI network; live-repo evidence: RUNLOG §7 (C65, kian-73). kian-74 follow-up
  (RUNLOG §7.2 G1): every github/lark reqwest client now stamps a
  `User-Agent: kian/<version>` — api.github.com answers UA-less requests with 403 + HTML
  that hides the status in the error chain, which the scripted fixture pages can never
  show — and `flaky-proxy.py` gained the path-scoped delivery-leg fault mode
  (`FAIL_N`/`FAIL_PATH`) so the retry/park arms reproduce with shipped tooling.
- **P6c live run (C65, kian-73)** — the plan's eight-scenario validation protocol executed
  live against the real `flyer103/kian` repo (2026-11-04, four scratch stores, real
  DeepSeek round trips, real GitHub REST traffic, `DRY_RUN=1` throughout; every claim cited
  by session id + seq or a captured log line — `examples/issue-watch/RUNLOG.md` §7):
  scenarios 1 (Eval accept → Ticket + Mandate), 4 (speak-rules: deliver-once, one
  sub-item not-reached and honestly marked), 5 (stage board honesty, including the
  arm-5 `queued`-on-completed projection gap confirmed live exactly as documented),
  6 (bounded retry with `attempt:3` observed, and the full 6-attempt park schedule),
  7 (kill -9 reboot: poll cursor derived from the admitted tail, replayed events
  `deduplicated`), and 8 (`reason=` codes + policy drift with exactly one marker input,
  live prompt byte-identical across reboots) **PASS live**; scenarios 2–3 (follow-up to a
  live/ended Executor) were **honest negatives** — structurally unreachable live (the
  `spawn_topic` spelling gap, G2), exercised only by the C64 fixtures at that moment, with
  the incidental positives the run did show (the Router honestly re-judged echo comments
  and dispatched nothing) recorded beside them. The gap report (§7.2, the kian-60
  precedent) filed G1–G4 as cards kian-74…77 — all landed, below.
- **Wave-C gap fixes (kian-74…77, from the C65 gap report)** — the round's live-validation
  loop closing on itself: G1 the `User-Agent` stamp + flaky-proxy delivery-leg mode
  (kian-74); G2 the lenient live spawn spellings — live topic binding, mandate
  strength/source, and outbox ticket aliasing all work live (kian-75, noted in the C59/C60/
  C62 bullets above); G3 the boot-time outbox-park rebuild + stage-board surfacing
  (kian-76, noted in the C62 bullet); G4 **exec secret hygiene** (kian-77, HIGH): a worker
  that ran `bash -c env` wrote the live GitHub token and DeepSeek key verbatim into
  `tool/result` and every later `step/request` — kian-77 lands the shared `kian-core`
  secrets module (`SecretRedactor` — exact configured-secret values, K/V patterns, known
  token shapes; `ExecEnvPolicy` — inherit default, allowlist mode, fail-closed), built once
  at assembly from every secret name `kian.yaml` itself names, redacting before persist on
  both the exec backend and the local tool surface. **This is the round's one commit that
  crosses the plan's bundle fence** (`kian-core`/`kian-exec`/`kian-loop`/`kian-cli`): it
  landed as its own filed card off the live gap report — the fence rule's stop-and-report
  path, never a silent widening — and is recorded here exactly because §4.7 otherwise
  describes only bundle-layer work. Zero new event kinds throughout; the durable set
  stays 20 for the whole round.

Round-7 additions keep the same discipline (each an additive payload key or a config-first
capability; full ADR record in [ADR-014](decisions/ADR-014-durable-interrupt-intent.md) and
[ADR-015](decisions/ADR-015-tool-permission-provenance.md)):

- **C67 (kian-79)** — provider usage + structured recovery on `turn/end`: the openai-compat
  provider persists the provider's `usage` object verbatim; `turn/end` carries it plus
  `{"class","action"}` on soft-failure/error outcomes. §15 cost note.
- **C68 (kian-80)** — durable interrupt intent: the 21st kind `control/interrupt` (the round's
  only new kind) + the reserved control envelope through the inbox; §2.4/§3.1 notes.
- **C69 (kian-81)** — derived event id + redaction provenance: §6.2 note.
- **C70 (kian-82) — per-call tool permission provenance** (allow/deny, fail-closed; `ask`
  deferred): `agent.permissions` / `agents.<id>.permissions` maps tool name → closed
  `Permission {Allow, Deny}` (unknown values refuse to parse; keys must be in the agent's
  `tools:`; both fail-closed). `ReActConfig.permissions` evaluates once per call **before** the
  `tool/call` append (log-before-execute holds on both arms) and records the decision as the
  additive top-level `permission` key `{evaluated, policy: default|configured, matched}` —
  provenance-on-the-action, so an *allow* is audited exactly like a deny. The deny arm never
  invokes and appends a synthetic honest-error `tool/result`
  (`{"error":"permission denied by policy: <tool>","policy":"deny"}`) — a denied call is a tool
  failure the model reacts to, never a loop abort. With no table configured the key is omitted:
  the payload is byte-identical to pre-C70 (constraint 3). Evaluated on the built-in, the
  plain-session (default agent) and worker-template loops; the Router/Watchdog judge loops do
  not evaluate a table — one on their agents refuses boot (never silently ignored). The
  `AgentDef.tools` schema filter (what the model *sees*) is untouched; permissions decide what
  *runs*.

## 5. Control-plane protocol (P5 protocolized)

Unified semantics (portable across harnesses/channels):

| Primitive | Semantics |
|-----------|-----------|
| `steer(text, scope)` | Inject a direction change; advisory; takes effect only at provider-turn safe boundaries; resets the allowance |
| `interrupt(reason)` | Abort the active drain; settle on abort (re-evaluate inbox); partial completion labeled interrupted |
| `queue(msg)` | Queue an input without interrupting the current drain |
| `confirm(token, decision)` | Confirmation gate: approve/reject/escalate; fail-closed (P8) |

Implementation: JSON-RPC over HTTP (axum) + SSE event stream. All control-plane operations **only
write the inbox / read projections** (the layered iron rule), never touching execution directly.

C13 refinement (the Phase-1 surface, as implemented by `kian-cli`): the wire is `POST /rpc` carrying
`{jsonrpc, id, method, params}` with the method set `create_session`, `submit`, `steer`, `queue`,
`interrupt`, `projection` (standard JSON-RPC error codes; unknown session = -32001), plus
`GET /sessions/{id}/events` — SSE frames `event: kian` (one per store event, interval-poll of the
event stream, strictly increasing seq), `event: done` after `session/ended`, `event: error` for an
unknown session. `confirm` is Phase 2+ and deliberately absent. The iron rule is enforced
structurally: the HTTP module sees only a `ControlPlane` trait (`create_session` / `submit` /
`steer` / `queue` / `interrupt` / `projection` / `session_events` — inbox writes and reads only);
`kian-cli::server` implements it over the engine, and a resident test greps the handler module for
execution symbols. `queue` and `submit` share the admit path (identical durable row; a drain in
flight folds the wake — the C16 same-sweep batch semantics); `steer` wraps the payload with the
strippable `synthetic` marker. Assembly: `kian.yaml` (listen, storage.sqlite, agent model
slots/tools/budgets, providers with `api_key_env` — secrets only ever read from the environment at
startup, fail-closed; drain lease/heartbeat/iteration/slot tuning; tools root; SSE poll; the
`recovery:` section parsed by C10's `PolicyConfig`) loads via the `kian-cli` config module with
`deny_unknown_fields` everywhere; the three built-in plugins (`openai-compat`, `local-tools`,
`react-loop` — the loop declares the other two as dependencies) activate through
`kian_plugins![...]` into the C7 `Ctx`, the loop resolves out of the activated `Ctx`, and the
engine runs the inbox rebuild **before** the listener accepts traffic (NoLostWork across restarts).

C17 refinement (S3 observability, as implemented by `kian-cli`): `GET /metrics` is a read-only
observability route merged into the serve router **outside** the `ControlPlane` handler module
(it names neither execution nor store-write APIs; it snapshots the C9 engine counters and derives
the S3 cache report). The S3 report is a pure projection over `step/request` audit copies: for
each consecutive request pair within a session, the message prefix must be byte-identical (the
provider-independent prefix-cache hit-rate proxy); the only counted invalidation cause is
`compact` — any other prefix change surfaces under `unexpected_prefix_change` and is a
constraint-3 violation. The same report is exposed by `kian cache-stats [--config <path>]
[--session <uuid>] [--json]`; the live-provider run is env-gated (`KIAN_API_KEY`), excluded from
CI, and recorded in the README run log.

C14 refinement (operational commands, as implemented by `kian-cli`): the SPEC §3.2 set lands as six
commands with two access modes. Read-only commands (`kian session inspect <id>`, `kian session
replay <id>`, `kian session export <id> [--since/--until]`, `kian lease inspect`) open the sqlite
file **read-only in-process** (`EventStore::open_read_only` — no migrations, no write path ever
held by the CLI) and derive everything from the event stream (constraint 1); resource resolution
is fail-closed (`--db`/`--url` else `kian.yaml`). Mutating commands talk to a running `serve`
through a dedicated **operational plane**, `POST /ops/rpc` (`wake_now`,
`recovery_budget_reset`, and — added in Phase 2, C27 — the demo/S4 injection
`mock_channel_send {channel, topic?, payload}`, which places one event onto a configured mock
channel and lets the subscription engine route it per `kian.yaml`),
mounted on the serve router outside the `ControlPlane` handler module behind its own
trait-isolated `OpsPlane` — the C13 wire surface and semantics stay untouched, and the resident
iron-rule test greps the ops handler module with the same forbidden-token table. `recovery budget
reset` appends an explicit additive event kind `recovery/budget-reset` (payload
`{"reason": string}`) through the normal epoch/expectedSeq-fenced store append — never a silent
counter wipe; the kind is projection-neutral in effect (state-preserving while running, contributes
no model-context message) and C3's additions-only default maps it to drain activity, so the ops
plane guards it: unknown → -32001, draining → -32002, terminal → -32003, idle → -32004 (an idle
session never consumed a recovery budget). `session replay` re-derives state, runs the C3
invariant predicates, optionally diffs the derived state against the live server projection
(`--url`), and verifies A2 end-to-end: for every `step/request` event it re-derives
`derive_model_context` over the pre-`turn/start` prefix (exactly what the built-in loop derived)
and asserts the canonical serialization is byte-identical to the recorded request body's messages
(reusing the frozen `to_chat_message` conversion, now public for exactly this comparison). `session
`session export` emits the stored envelopes one-per-line (JSONL; inclusive seq bounds); re-importing a
package into a fresh store appends under each event's own fence and projects the identical state
(S7 audit shape). All commands support `--json`; the schemas are frozen and documented in
`kian_cli::commands` module docs.

C49 refinement (inbound webhook receiver, as implemented by `kian-cli`): `POST /webhooks/github` is
the **fourth server surface** (beside the C13 control plane, the C14 ops plane and the C17 metrics
route; ADR-011), mounted on the serve router only when the route is configured (a `type: github`
channel under `superagent.channels`; otherwise the path 404s) — outside the `ControlPlane` handler
module, behind its own trait-isolated `WebhookPlane` (`ingest_verified_github` — verify-then-hand-off
is its only capability; webhook → admit is an inbox write, the same control-plane legality as the
subscription service). Verification order: size cap (router-level body limit — 413 *before any
signature work*) and content-type gate (415, `application/json` only); HMAC-SHA256 over the raw body
against `X-Hub-Signature-256`, constant-time compare, secret env-only
(`KIAN_GITHUB_WEBHOOK_SECRET`, fail-closed at startup when the route is configured — never a
signature-less receiver, never a secret in `kian.yaml`); per-source in-memory token bucket (429;
v1 per-process bound, bucket keyed on the connection peer, limits `kian.yaml`-tunable with zero
refused); then — and only then — parse and hand the verified `(event type, delivery id, body)`
triple to the github adapter's `ingest_delivery`. Verification precedes any parsing
(untrusted-by-default, the C43/C44 discipline applied to the transport); delivery-id dedupe is
deliberately not the receiver's — the delivery id is the event id, so the subscription pipeline's
existing `(session, event_id)` idempotency is the durable guard. Rejections are honest 4xx
responses with `kian::webhook` audit lines — no event, no row, no parse — and the resident
iron-rule test greps the webhook module with the same forbidden-token table as the other handler
modules (the module names no execution or store-write symbol).

## 6. Plugin system (Cordis-inspired, Rust-ized)

### 6.1 Design mapping

| Cordis concept | Kian Rust implementation |
|----------------|---------------------------|
| Plugin = Service | `trait Plugin { fn apply(self, ctx: &mut Ctx) -> Result<Disposer> }` (returning a Disposer = reversible registration) |
| ctx container / service keys | `Ctx` = a typed service registry: `ctx.get::<dyn ModelProvider>()`, `ctx.get::<dyn ExecBackend>()` |
| inject dependency declaration | Construction-time dependency resolution (topologically-ordered activation, equivalent to inject semantics; Rust has no runtime declaration merging) |
| Typed event dispatch | `dispatch` enum: `emit` (observe) / `waterfall` (middleware, may short-circuit) / `parallel` / `serial` / `bail` (stops at the first bail value) |
| Reversible side effects | `Disposer` (teardown in reverse order); registration and disposal are paired |

**Static (compile-time) plugins first** (ADR-004): a plugin = a crate implementing the trait,
registered into the server binary via the `kian_plugins![...]` macro; HMR/dynamic loading
(dylib/wasm) is left to Phase 3. Rationale: the Rust dynamic-loading ecosystem is unstable, and
compile-time plugins already satisfy the narrow-waist goal of "capabilities are replaceable";
dsh-style total plugin-ification is its product choice — Kian's narrow waist is smaller.

### 6.2 Seam three roles (borrowing dsh capability seams)

Every seam names three roles: **Service Definition** (the trait) / **Provider** (the impl) /
**Consumer** (usually a model-visible tool).

| Seam | trait (illustrative) | Built-in Provider | Third-party swappable |
|------|----------------------|-------------------|-----------------------|
| `ModelProvider` | `complete(req: ModelRequest) -> Stream<ModelChunk>` | openai-compat (covers OR/kimi/ds) | any |
| `ExecBackend` | `exec(cmd, env) -> ExecResult` | local / remote-endpoint (kiand, endpoint SDK) | sandbox/cloud machines |
| `LoopDriver` | `drive(drain_ctx) -> Stream<LoopOutcome>` | built-in ReAct loop | **dsh bridge (`kian-dsh`, C54: a `dsh --profile sdk` child over stdio JSON-lines, ADR-010 mapping)**, custom loops |
| `ChannelAdapter` | `subscribe() -> Stream<ChannelEvent>`; `send(out) -> Future<SendReceipt>` | — | lark/im/github |
| `ToolProvider` | `schemas() -> Vec<ToolSchema>`; `invoke(call)` | built-in fs/exec/kiand | any |
| `RecoveryPolicySource` | `table() -> Vec<PolicyRule>` | config file | any |
| `WatchdogSource` | `health() -> Stream<HealthEvent>` | kiand heartbeat | any |

The async exec pattern (C37, ADR-008) adds no new seam: `AsyncResultSink` is a narrow
assembly callback (kian-exec → kian-cli completion bridge), not a provider/plugin
extension point — the `ToolProvider` role already covers the model-visible `start`/`stop`
pair, and the completion path is the ordinary Admit→Wake→Promote cycle, not a channel.

C69 (redaction provenance): the kian-77 redactor emits a structured report alongside its
scrubbing — `SecretRedactor::redact_with_report` returns `(text, Vec<Redaction{kind, name,
count}>)` with the closed kind set `known_value` / `token_pattern` / `env_line` (aggregate
counts sorted by `(kind, name)`, never secret values); `redact` delegates to it, so the
redacted text is byte-identical to kian-77's. The report rides the additive
`ExecResult.redactions` seam field — including the durable remote-exec queue row, so an
ADR-007 deduped read-back still answers with the stored report — is copied onto
`ToolResult.redactions` by the local (`LocalTools` exec) and remote (`RemoteExecTool`)
paths, and lands on the `tool/result` event payload as an additive top-level `redactions`
key **outside `content`**, omitted when empty. The context projection reads `role`/
`content` only, so the key never enters `derive_model_context` or request bytes
(constraint 3). The async `start`/`stop` acks and the async completion envelope carry no
report: their payload content is model-visible, and the completion keeps the four legacy
exec fields only.

C70 (tool permission provenance, ADR-015): the `ToolProvider` seam is unchanged — the
per-call allow/deny decision is a *loop* concern, evaluated on the built-in ReAct loop before
the `tool/call` append from the declarative `ReActConfig.permissions` table (the footprint
ladder's configuration rung: no new trait, no new provider). Provenance rides the `tool/call`
payload as the additive top-level `permission` key outside `content` (the same projection
discipline as C69's `redactions`); a denied call never reaches `invoke` — the seam sees only
permitted calls, and the model sees an honest error `tool/result` instead.

### 6.3 Footprint Ladder (capability growth ladder)

A new capability is attempted in order of core footprint, smallest first:
1. **Configuration** (zero footprint): recovery policy tables, subscription rules.
2. **Plugin** (edge): ChannelAdapter / ToolProvider / a new ModelProvider.
3. **Seam** (wider waist, requires an ADR): a new trait + three roles + built-in default.
4. **Core primitive** (last resort): entering kian-core requires proof it cannot be expressed at
   the edge.

## 7. Module layout (crate structure)

```mermaid
graph TB
    subgraph crates
        KC[kian-core<br/>concept model / state machine / invariants]
        KS[kian-store<br/>EventStore / Inbox / projection engine]
        KO[kian-orchestrator<br/>Admit→Wake→Drain→Promote<br/>+ recovery subsystem]
        KL[kian-llm<br/>ModelProvider seam + openai-compat]
        KE[kian-exec<br/>KiandProto server half:<br/>ConnectionManager<br/>+ durable queue]
        KP[kian-plugin<br/>Ctx/Plugin/Disposer/dispatch]
        KLD[kian-loop<br/>LoopDriver seam + built-in ReAct]
        KEP[kian-endpoint<br/>endpoint SDK library (KiandProto endpoint side)]
        KSA[kian-superagent<br/>Router/Worker bundle + channel templates]
        KCL[kian-cli<br/>serve/run/inspect/replay/export]
        KSDK[kian-sdk<br/>wire-only embedded SDK]
        KDD[kiand<br/>endpoint daemon = kian-endpoint + main]
    end
    KC --> KS
    KO --> KC & KS
    KL & KP & KLD --> KO
    KE --> KC & KS
    KSA --> KP & KO & KLD
    KCL --> KO
    KDD --> KEP
    KE -.->|dials over TCP| KEP
    KSDK -.->|C13/C14 JSON-RPC + SSE over HTTP| KCL
```

| crate | Responsibility | Key interfaces |
|-------|----------------|----------------|
| kian-core | Concept model, session state machine, six-invariant assertions | `SessionId`, `EventKind`, `Projection`, `seams::*` |
| kian-store | EventStore (sqlite v0 → postgres), Inbox, expectedSeq CAS, epoch fencing, projection rebuild. C50: dual-backend — the public `EventStore` is a dispatching enum (`Sqlite(..) \| Postgres(..)`) with identical method names/signatures on both; per-backend SQL lives in private `sqlite`/`postgres` modules (dialect notes in §3.3); `open(path)` stays sqlite construction, `open_postgres(url)` is the pg entry | `append(carrying epoch, expected_seq)`, `scan`, `project` |
| kian-orchestrator | Execution model, recovery subsystem (classification + policy + budget), control-plane implementation | `admit`, `wake`, `claim`, `drain_loop` |
| kian-llm | ModelProvider seam, slot resolution, sent-bytes persistence hooks | `ModelProvider`, `SlotResolver` |
| kian-exec | KiandProto server half: `ConnectionManager` dials the endpoint, validates the hello/welcome handshake (version mismatch is terminal), tracks heartbeat freshness (`last_seen`, `inflight`), pipelines commands and correlates results by command id, answers with ping, says bye on shutdown, and reconnects with bounded backoff. C20 adds the durable half (ADR-007): `endpoint_registry` + `exec_queue` tables in the server's `kian.sqlite` under this crate's own migrations, `RemoteExecBackend` (enqueue-first `ExecBackend`: park while offline, re-drive on connect/restart, honest `Timeout`→`abandoned` at `queue_timeout`), and `RemoteExecTool` (the same `exec` tool schema as the local built-in, routed through a configured endpoint). Depends on core + store only — never on the orchestrator | `ConnectionManager`, `KiandProto`, `RemoteExecBackend`, `RemoteExecTool`, `ExecStore` |
| kian-loop | LoopDriver seam, built-in ReAct loop | `LoopDriver` |
| kian-endpoint | Endpoint SDK library: KiandProto's endpoint-side implementation, embeddable in any process | `Endpoint::serve`, `Endpoint::embed` |
| kian-plugin | Ctx registry, Plugin trait, five-mode dispatch, Disposer | `Ctx`, `Plugin`, `Disposer` |
| kian-superagent | super-agent bundle: Router/Worker templates, subscription rules, channel output policies | profile assembly |
| kian-cli | CLI and serve assembly: serve/run/inspect/replay/export; re-exports the SDK client so `commands` speak the same wire | `commands`, `client` (re-export of `KianClient`) |
| kian-sdk | Wire-only embedded SDK (C46): the frozen C13/C14 client surface over HTTP — JSON-RPC transport, typed methods (`create_session`, `submit`, `steer`, `queue`, `interrupt`, `projection`), the `events` SSE helper, ops wrappers (`wake_now`, `recovery_budget_reset`, `mock_channel_send`) and matchable typed errors. Depends on no kian crate (reqwest/serde/serde_json/futures/thiserror only) — embeddable anywhere | `KianClient`, `ClientError`, `SseFrame` |
| kian-dsh | The S6 dsh bridge (C54, ADR-010 / §16): `DshDriver` implements the `LoopDriver` seam by driving a `dsh --profile sdk` child process over stdio JSON-lines — dsh keeps its interaction model, kian owns durability (every mapped event rides the ordinary fenced append path; the C53 mapping is the only translation; steers arrive as promoted inbox rows). Narrow waist: depends on `kian-core` only (seam types + event vocabulary); the drain surface is the crate-local `DshCtx` port, implemented over the real `DrainCtx` by assembly (`kian-cli`, config-gated `superagent.harness.dsh`, default off) | `DshDriver`, `DshCtx`, `DshDriverConfig` |
| kiand | Endpoint stateless daemon (kian-endpoint + thin main) + watchdog | command execution + health events |

**SDK freeze and versioning (C46).** The `kian-sdk` public surface *is* the frozen C13/C14 wire
contract: after card C46 it is **additive-only** — removing or reshaping a method, param, response
field, or error code is a breaking change and requires a new plan round (the event-kinds rule
applied to the API). The crate carries its own version (`0.1.0`), decoupled from the workspace
bump cadence; publishing to crates.io happens only when the owner asks. Golden wire fixtures in
`crates/kian-sdk/tests/fixtures/` pin the request/response bytes of every method (the C8
serialization-freeze discipline applied to the client).

Dependency direction (the crate form of the layered iron rule): `kian-orchestrator` depends only
on core + store + seam traits; every provider/channel implementation is assembled through
`kian-plugin` and never enters the orchestrator dependency tree.

One bundle edge crosses the loop crate: `kian-superagent` depends on `kian-loop` for the reusable
evidence-gate evaluation (`evaluate_evidence_gate`, `CheckSpec`, the verdict payload — card C24's
public, pure functions consumed by C25's `WorkerLoopDriver` instead of duplicating gate semantics).
Types and pure functions only — no execution surface flows along that edge, and the bundle still
reaches the server exclusively through its ports (`InboxWriter`, `StreamPort`, and — as of C34,
Phase 3 — `RecallPort`, the read-only cross-session port: `read_events` over a seq window,
`list_threads` derived from `thread/started`, and the spill-index lookup. Assembly implements it
over the store's public scan; the spill index itself is a disposable `event_id → (session, seq)`
location cache rebuilt at startup from `input/spilled` events — a pure projection, the raws live
only in the streams), enforced by the crate-level iron-rule test.

Seam trait **definitions** (`ModelProvider`, `LoopDriver`, `ToolProvider`, `ExecBackend`,
`RecoveryPolicySource`, and — as of C18, Phase 2 — `ChannelAdapter` and `WatchdogSource`)
live in `kian-core::seams`; the implementation crates listed above
(kian-llm, kian-loop, kian-exec, …) implement and re-export them. Phase-1 decision (round-1 plan,
risk 1): hosting the traits in the core keeps the orchestrator dependency tree free of provider
crates while the built-in impls stay swappable. The Phase-2 seams follow the same rule:
`ChannelAdapter` and `WatchdogSource` are definition-only in core (no impls, no new
dependencies); their impls land in the bundle (`kian-superagent`) and `kian-exec`
respectively (ADR-003: super-agent semantics stay out of the core).

The KiandProto wire contract is deliberately declared **twice, once per half**:
`kian-endpoint` and `kian-exec` each carry their own golden-pinned frame types and
share no protocol code — two independent implementations of one contract, with the
golden tests asserting byte-identical frames and the pairing tests asserting both
`PROTOCOL_VERSION`s match. Any wire change is a same-commit dual update with a
version bump on both sides. `kian-exec` therefore has **no production dependency**
on `kian-endpoint` (it dials the endpoint over TCP; `kian-endpoint` appears only as
a dev-dependency for the real-TCP pairing tests).

## 8. Data flow (example: IM message → PR delivery)

```mermaid
sequenceDiagram
    participant IM as IM group
    participant CA as lark ChannelAdapter
    participant RS as Router Session
    participant WS as Worker Session (Thread)
    participant KD as kiand@laptop
    participant GH as GitHub
    IM->>CA: @kian fix the vadis R75 CI failure
    CA->>RS: Admit(msg) → inbox row persisted
    Note over RS: drain: judge = work request → batch dispatch
    RS->>WS: Thread delegation (context pinned)
    WS->>WS: loop: read code / locate / fix
    WS->>KD: exec (run tests)
    KD-->>WS: results flow back
    WS->>GH: create PR (structured description)
    GH-->>CA: PR CI events (subscription passthrough to WS)
    WS->>RS: completion report (with PR link)
    RS->>IM: ≤100-word conclusion + PR link
```

## 9. Concurrency model

- **Per-session serial drain, cross-session concurrency** (invariant 2): a
  `DashMap<SessionId, DrainHandle>`; strictly serial within one session.
- The drain executor is a tokio task; model streaming responses use `Stream` + bounded-channel
  backpressure.
- Epoch/lease safety comes from store CAS; no cross-session global lock in-process.
- **Wake transport (C52, postgres)**: wakes stay advisory — LISTEN/NOTIFY
  (`kian_wake` channel, payload = session uuid) only *accelerates* the
  admit→wake hint across replicas. The notify flies as its own statement after
  the input INSERT committed (a received notification implies a visible row);
  each replica relays through a `PgListener` on one dedicated pooled connection
  firing its local engine `wake`. A listener gap (reconnect, replica down) is a
  losable-wake window bounded by the recovery sweep ticker and each replica's
  startup inbox scan — correctness never depends on the transport (invariant 6).
  sqlite runs no transport: the in-process wake registry is the whole story.
- **Exec queue row leases (C52, ADR-009)**: `exec_queue`/`async_jobs` rows are
  owned while driven — a single-statement lease (`claimed_by`/`claimed_until_ms`,
  claimed before any row mutation, heartbeated by the owner's drive loop,
  re-drivable by any replica after expiry). Liveness only: the lease bounds
  duplicate dispatch across replicas and never gates accepting a result — the
  command-id correlation (ADR-007) is the safety net. Endpoints themselves stay
  dialable by any replica (v1: duplicate dialing is harmless under at-least-once
  + command-id dedupe; the registry's `last_heartbeat` is written by whichever
  replica holds the connection).
- A kiand connection = one TCP connection carrying JSON-lines KiandProto frames with pipelined
  commands; the endpoint-side execution concurrency cap is configurable. **v1 topology (C22): the
  endpoint listens, the server dials.** quinn/quic and websocket-over-TLS remain the Phase-3
  transport upgrade path.

## 10. Error-handling strategy

- Pure classification function `classify(error, ctx) -> ErrorClass`: provider_down / rate_limited /
  context_overflow / tool_failed / store_failure / budget_exhausted / validation_failed …
- Policy table (declarative): `match → action`, action ∈ {backoff_retry, compress_then_retry,
  failover, interrupt_ask, fail}; disposition owned by the caller (A4).
- Every recovery has a one-shot guard; budget refund semantics (A5).
- end_reason state machine: completed / cancelled / interrupted / failed / recoverable
  (distinguishing intentional ends from crashes, A9).

## 11. Testing strategy

| Layer | Strategy | Tooling |
|-------|----------|---------|
| Invariants | Six invariants + NoHalfTerminated + NoSplitBrainWrite resident debug_asserts; table-driven exhaustive checks | built-in assertion macros |
| Classification function | Pure-function table-driven tests | cargo test |
| Chaos | kill -9 injection (at every drain stage), half-written store, lease-expiry races, two-process contention | scripted fault injection |
| Replay | A2 byte-consistency: same event stream → derive_model_context byte-identical | golden-bytes tests |
| e2e | S1–S7 acceptance scenarios | driven by kian-cli |
| Fuzz | EventStore append/scan, projection rebuild | cargo-fuzz |

## 12. Deployment architecture

- v0 single machine: `kian serve` (one binary + sqlite). Docker optional.
- v1 multi-replica: postgres EventStore + lease/fencing (claim row + epoch CAS); executor scale-out
  = a stateless worker pool claiming sessions.
- Multi-replica proof (C52, ADR-009 completed): the S8 acceptance e2e — two
  `kian serve` replicas over one postgres database, the drain owner `kill -9`'d
  mid-provider-turn, the survivor's recovery sweep takes over the expired lease
  (conditional claim, C47) and completes with zero lost work, exactly one end
  write, and a stale-epoch probe write from the dead replica's frozen epoch
  rejected by the store fence (NoSplitBrainWrite at multi-process scale; S1's
  guarantee lifted between processes). Env-gated on `KIAN_PG_URL`; default CI
  needs no postgres.
- Postgres assembly (C51): `kian.yaml` `storage.event_store: sqlite | postgres`. One server, one
  database — under postgres the event store and the exec tables (`endpoint_registry`,
  `exec_queue`, `async_jobs`) share one pg pool. The URL is env-only (`storage.postgres.url_env`
  names the variable; the value may carry a password but never lives in the config — the C13
  secrets rule) and a missing variable refuses to start. Postgres mode also **requires** an
  explicit unique `drain.holder` (the shared default refuses to start — same-holder claim
  admission, C47/ADR-009 condition (c), must not be defeat-able by two replicas sharing a name).
  sqlite mode keeps today's defaults byte-identically.
- Endpoint side: kiand single binary (systemd/launchd managed) + watchdog.
- Hosted control plane (commercial layer, not in the open-source core): multi-replica hosting,
  audit console, evidence-gate reports — see the BP.

## 13. SLA and availability

| Dependency | Assumed SLA | Failure impact | Degradation |
|------------|-------------|----------------|-------------|
| LLM provider | 99% (no SLA guarantee) | iteration failure → L1 recovery (failover slot) | fallback model chain |
| sqlite | local file | single-machine store failure → L3 repair | postgres migration path |
| kiand endpoint | intermittently online (the norm) | exec queueing | durable queue + reconnect consumption |
| LLM gateway | externally governed | same as provider failure | slot fallback |

Kian's own availability target (single machine): after a drain crash, recoverable detection ≤
lease TTL + heartbeat interval; NoLostWork is guaranteed by inbox rebuild.

Multi-replica (C47/ADR-009, the hosted substrate Phase 4 builds): claim admission is conditional
— a fresh lease held by another holder refuses the claim (no epoch ping-pong livelock), so a live
drain on any replica is never displaced; takeover after a holder's death keeps the single-machine
bound (recoverable detection ≤ lease TTL + heartbeat interval, then the expired-lease claim). The
wake transport (C52) is an advisory accelerator — availability never depends on it.

## 14. Security and privacy

- Control-plane/execution-plane separation + data-egress tiering (A8): orchestration authority
  never flows out.
- Endpoint credentials never leave the endpoint (kiand executes locally; returned results carry no
  credentials).
- Confirmation gates fail-closed (P8); audit = event-stream export.
- Credential-less credential use = Phase 3+ topic (same-source risk as article §8.4).

## 15. Cost model

- Runtime side: single-machine sqlite ≈ zero infrastructure cost; tokens dominate.
- The commercial translation of A3: higher cache hit rate → lower token cost (DS cache hits are
  ~30× cheaper) — cache hit rate is Kian's first-class metric.
- Compression is the only deliberate invalidation point: compression policy = a cost/context-
  quality tradeoff, configurable.
- Token usage is a durable fact (C67): providers' `usage` objects persist verbatim on `turn/end`
  (drain-activity, never context-bearing) and `kian usage-stats` / `GET /metrics` project token
  totals from them. Pricing/cost conversion is deliberately a projection-layer decision outside
  core — the event stream carries the provider's own numbers, unnormalized.

## 16. Migration and compatibility

- v0 event schema JSON + envelope version; adjacent migration chain.
- dsh being-integrated: the dsh loop as a Worker (Phase 3→4): a mapping protocol between harness
  session-log semantics and the Kian EventStore (harness events → a Kian durable-event subset).
  The contract is fixed by [ADR-010](decisions/ADR-010-dsh-session-mapping.md) and lives
  bundle-level (`kian-superagent::dsh`): dsh durable `user/message`/`system/message` →
  `input/admitted` (role rides verbatim), `assistant/message` → `assistant/message`,
  `assistant/attempt` → `step/request` (the attempt's assembled request bytes ride `body`
  verbatim; an attempt with no following `assistant/message` *is* the honest failure trace),
  `tool/call`/`tool/result` → the same kinds in the loop's frozen payload shapes,
  `step/start`/`step/end` → `turn/start`/`turn/end` (a dsh step is one provider request cycle),
  and the dsh `turn/*` episode envelope is absorbed into the drain episode the orchestrator
  already owns (explicitly reported, never silently dropped). Zero new kinds. dsh session
  generations are decoded to current logical events *before* mapping — Kian never stores foreign
  generations (a foreign logical-format handshake is refused). Persist-what-you-send holds across
  the process boundary: the peer must deliver the step's assembled request bytes before the
  response is offered, or the bridge stops at a documented negative result — never a best-effort
  post-hoc logger. The C53 conformance fixture (`dsh-fixture-peer` over scripted JSONL, no
  Node/dsh dependency) proves both mapping directions, the steer/interrupt translation and the
  pre-flight ordering rule in default CI; C54's `kian-dsh` driver implements the same contract,
  which is also the bar any dsh-side plugin must clear to be kian-compatible.
- Sync with the Feishu design doc: this document is its Rust implementation design; the upstream
  doc's "why the agent loop is not enough" TODO section is argued by this document's §1/§2
  (write-back to upstream is the owner's call).
