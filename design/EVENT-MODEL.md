# Kian Event Model — normative contract (v0)

> **Status: normative.** This file is the single source of truth for the durable event model:
> envelope, classification, the complete kind catalog, payload contracts, cross-cutting
> conventions, invariants and the **change protocol**.
>
> Authority chain: **this file wins** for what an event is and how it changes; `design/DESIGN.md`
> §3 carries the design rationale and must link here rather than restate the catalog; ADRs explain
> *why* a decision was made; `crates/kian-core/src/events.rs` plus its golden tests are the
> executable enforcement. If code and this document disagree, the document and the code must be
> reconciled in the same change — a silent drift is a bug.
>
> Scope: durable events appended to a session stream. Inbox rows, exec/async job rows and
> projections are **not** events (see §7); live/stream-only signals are **not** durable events
> (see §2).

## 1. Envelope

Every durable event is one JSON envelope, serialized canonically and frozen (golden-tested; audit
export/replay depends on byte identity):

```json
{
  "seq": 7,
  "session": "36e8cedd-...",
  "epoch": 1,
  "ts": 1793541040187,
  "kind": "turn/end",
  "bytes": { "...": "kind-specific payload" },
  "version": 0
}
```

| Field | Contract |
|---|---|
| `seq` | Monotone per session, starts at 1. The CAS fence input (`expectedSeq`); never reused. |
| `session` | Session UUID; one stream per session. |
| `epoch` | Fencing epoch of the drain that appended the event; stale-epoch writes are rejected by the store. |
| `ts` | Unix epoch milliseconds at append. Recorded fact, not an ordering key (ordering is `seq`). |
| `kind` | The durable kind name. Flattened: the JSON carries `"kind": "<name>"`; structured kinds may add top-level fields (only `session/ended` carries `reason`). |
| `bytes` | Opaque JSON payload. **No provider/channel/harness schema may leak into the envelope**; payload schemas are owned by the emitting subsystem and listed in §3. |
| `version` | Envelope schema version; absent means `0`. Changes follow §6.3 (adjacent migrations only). |

Frozen rules:

- Appends are immutable: no update, no delete, no rewrite, no reordering.
- The end write (`session/ended`) and the state transition are **one atomic append**
  (`NoHalfTerminated`); this is one transaction on sqlite and one statement on postgres.
- Every drain append carries its epoch and is accepted only at the expected `seq`; the store
  rejects stale-epoch writes (`NoSplitBrainWrite`). Leases are liveness only — they never grant
  write permission.
- Event envelopes are the audit/export unit. The derived event id (`§5.4`) is not stored.

## 2. Classification

### 2.1 Durable vs live

| Class | Examples | Persisted | Purpose |
|---|---|---|---|
| Durable events | everything in the §3 catalog | yes, append-only | truth and replay |
| Live events | token/streaming chunks, heartbeats, progress notifications, event deltas | **no** | UI/observability only; derived from durable events or a side channel |

There is **no durable live-event kind**, and no persisted token-level delta. Streaming deltas are
assembled by the loop and only the final message is persisted (constraint 2: persist-what-you-send;
constraint 3: cache-byte stability).

### 2.2 Context-bearing vs non-context

- **Context-bearing** (projected into model messages by `derive_model_context`): `input/admitted`,
  `assistant/message`, `tool/call`, `tool/result`. Their payload IS a message payload (§4.1).
- **Context-affecting**: `compact` — not a message, but the projection emits its summary at the
  compressed range's start and skips the range. Compression is the **only** deliberate
  prefix-invalidation point.
- **Non-context**: every other kind. Adding one must leave `derive_model_context` output and model
  request bytes byte-identical (golden-tested per kind).
- Additive provenance keys live **outside** `content` and are non-context by construction
  (`usage`, `error`, `redactions`, `permission`; §4.2). They must never enter the request body.

### 2.3 State effect (projection triggers)

`EventKind → TransitionTrigger` is fixed:

| Trigger | Kinds | Effect |
|---|---|---|
| Created | `session/created` | `None → idle` |
| Claimed | `drain/claimed` | `idle/recoverable → running` (running stays running) |
| Ended | `session/ended{reason}` | `running → completed/cancelled/rotated` (terminal) for `completed`/`cancelled`/`rotated`; `running → idle` (resumable) for `interrupted`/`failed`/`awaiting_input`/`awaiting_budget` |
| State-preserving | `input/admitted` (Input trigger), `thread/started` (Thread trigger) | preserves `idle`/`running`/`recoverable` |
| DrainActivity | all other kinds | legal only while `running` (or `archived` where the table allows) |

State is **read only from the projection** (`project(events)`), never from a column or cached
field. `recoverable` and `archived` are liveness/ops overlays, not event-sourced transitions.

## 3. Kind catalog (24, additions-only)

Payload schemas below are the **frozen** shape at schema v0 unless marked additive. "Additive"
keys are optional, omitted when unset, and leave legacy event bytes byte-identical. Emitters name
the subsystem that owns the payload schema; the core kind set stays provider/channel-agnostic.

| Kind | Ctx | State | Payload (payload = `bytes`) | Emitter | Notes |
|---|---|---|---|---|---|
| `session/created` | — | Created | `{}`; additive (BEGIN-only, [HARNESS-BRIDGE](HARNESS-BRIDGE.md) §4.7): `"bridge":{"name":<str>,"request_bytes":"exact"\|"unavailable"}` | orchestrator (spawn seam) | First event of a stream. `bridge` marks a spawned worker session whose loop is an external harness and states whether the assembled model request is observable (`exact`, persisted as `step/request`) or not (`unavailable`, projection-only). Absent on kian-owned loops and on the API's plain sessions, so those streams stay byte-identical. Audit metadata only. |
| `input/admitted` | C | Input | `{"role":"<non-empty>","content":<any>,"synthetic"?:true}`; additive: `"dream":{"source_session":<uuid-str>,"start_seq":<u64>,"end_seq":<u64>,"scope":<str>}`, `"memory":{"scope":<str>,"notes":[<event-id>...],"bytes":<u64>,"items":<u64>,"truncated":<bool>,"source_seq":<u64>,"redactions":[{"kind","name","count"}]}` (the creation-time injection marker, §4.2), `"job":{"run_id":<str>,"job_id":<str>,"slot":<str>,"trigger":"cron"\|"manual"\|"alert","plan_digest":"sha256:<hex>","plan":<frozen job plan>}` (the frozen plan of a run, [JOB-MODEL](JOB-MODEL.md) §14 A1; absent on every non-run row) | orchestrator (drain consumption) | Durable inbox row consumed into context. `synthetic` marks steer-injected content (strippable, A6). `dream` (§4.2) is the ops-set dream marker — trusted routing/provenance, never read from content. |
| `drain/claimed` | — | Claimed | `{"holder":<str>,"epoch":<u64>}` | orchestrator | Claim audit. Epoch is the fencing token; claim admission is conditional (ADR-009). |
| `turn/start` | — | DrainActivity | `{"iteration":<u64>}`; additive: `"purpose":"compact"` marks a maintenance turn ([ADR-058](decisions/ADR-058-production-compaction-and-maintenance-reserve.md)) | loop | Brackets one provider request cycle. Iteration derived from the stream. |
| `turn/end` | — | DrainActivity | `{"iteration":<u64>,"outcome"?:"final_answer"\|"tool_calls"\|"soft_failure"\|"error","tool_calls"?:<u64>}`; additive: `"usage":<provider usage verbatim>`, `"error":{"class":<class>,"action":<policy disposition\|"none">}`, `"harness":{"resume":<str>}` (an external harness's continuation handle; [HARNESS-BRIDGE](HARNESS-BRIDGE.md) §4.6, [ADR-047](decisions/ADR-047-harness-conversation-resume.md)), `"purpose":"compact"` (maintenance turn; [ADR-058](decisions/ADR-058-production-compaction-and-maintenance-reserve.md)) | loop | Usage/error are audit-only; `harness.resume` is the durable pointer a later episode resumes from (never model context). |
| `step/request` | — | DrainActivity | `{"slot":<str>,"provider":<str>,"model":<str>,"iteration":<u64>,"body":<exact request bytes>}`; additive: `"body_version":"v1"\|"v2"`, `"dialect":"openai_chat"`, `"profile_digest":<sha256>`, `"body_sha256":<sha256>`, `"estimate":{"low":<u64>,"high":<u64>,"max_output_tokens"?:<u64>,"source":"chars_v1","estimator_version":1}`, `"purpose":"compact"` (the summarizer request of a maintenance turn; absent means an ordinary turn, [ADR-058](decisions/ADR-058-production-compaction-and-maintenance-reserve.md)) | loop / provider persist hook | **A2**: persisted before the request flies; replay is byte-identical. The additive keys describe the plan that produced `body`; a session with no declared profile keeps the V1 body byte-identical (the additive metadata is still recorded). |
| `assistant/message` | C | DrainActivity | `{"role":"assistant","content":{"text":<str>,"tool_calls"?:[...],"reasoning_content"?:<str>}}` | loop | The sent bytes are the truth; never rewritten. `reasoning_content` is the provider's thinking-mode reasoning, part of context (replayed on the wire); absent for non-thinking providers and legacy events ([ADR-022](decisions/ADR-022-reasoning-content-replay.md)). |
| `tool/call` | C | DrainActivity | `{"role":"assistant","content":{"id":<str>,"name":<str>,"arguments":<json>}}`; additive: `"permission":{"evaluated":"allow"\|"deny"\|"ask","policy":"default"\|"configured"\|"hook","matched":<str>,"reason"?}` | loop | **Log-before-execute**: appended before `invoke`. Deny never invokes (ADR-015). `policy:"hook"` is a `tool/pre` veto denial; `matched` then carries `"<plugin>:<hook>"` ([PLUGIN-SPEC](PLUGIN-SPEC.md) §4.4, [ADR-028](decisions/ADR-028-harness-hook-plugins.md)). `ask` never invokes either: the call stays pending until a `control/confirm` answers it and the drain ends `awaiting_input` (ADR-036, [CONFIRMATION-MODEL](CONFIRMATION-MODEL.md)). Additive `"recovered":"not_dispatched"\|"in_flight"` marks a call the drain reconstructed after a crash mid-batch (no `permission` key — no decision was recorded); see §4.2. |
| `tool/result` | C | DrainActivity | `{"role":"tool","content":{"call_id":<str>,"output":<json>,"is_error":<bool>}}`; with `artifacts:` configured, a successful `output` above the inline bound is `{"artifact":{"id","size","media_type"},"preview":<str>,"truncated":true}` (ADR-056, §4.1); additive: `"redactions":[{"kind","name","count"}]`, `"confirmation":{"call_id","decision":"approve"\|"reject","requested_by":<str\|null>,"answered_at_ms":<u64>,"expired"?:true}` | loop / exec | Redaction provenance never carries secret values. `confirmation` records which human decision produced the result (§4.2); omitted on ordinary results. Additive `"recovered":"not_dispatched"\|"in_flight"` marks the synthetic honest-error result that closes a crashed tool batch (§4.2). |
| `compact` | c | DrainActivity | `{"from_seq":<u64>,"to_seq":<u64>,"summary":{"role":<str>,"content":<json>}}`; additive: `"produced_by":{"step_seq":<u64>}`, `"reason":<str>`, `"folded_messages":<u64>`, `"reclaimed_tokens":<u64>` ([ADR-058](decisions/ADR-058-production-compaction-and-maintenance-reserve.md)) | loop (production planner) / recovery reactor | The only deliberate prefix-invalidation point; ranges pairwise-disjoint, `a < b <= seq`. The additive keys are audit-only and never model-visible: `produced_by.step_seq` is the summarizer `step/request` that produced the summary, `reason` names the trigger, and the two sizes describe the fold (ADR-058). |
| `slot/binding` | — | DrainActivity | `{"slot":<str>,"spec":{"provider","model","fallback":[...]}}`; additive: a `"profile"` object on `spec` and on each fallback target (`dialect`, `context_window`?, `max_output`?, `reasoning`, `thinking`?, `effort`?, `strict`, `cache`, `vision`) | loop at first use | Slot → provider/model fixed for the session's lifetime (ADR-006); a declared profile is part of that binding and is session-immutable (ADR-054). |
| `slot/fallback` | — | DrainActivity | `{"slot":<str>,"from":{"provider","model"},"to":{"provider","model"}}`; additive: `"profile"` on each target; additive attempt copy: `"body":<exact request bytes>`, `"body_version"`, `"dialect"`, `"profile_digest"`, `"body_sha256"`, `"estimate"` | provider turn | One fallback-chain switch, persisted before the fallback call. The attempt copy is the exact body the fallback sends, so replay can reconstruct the retry without a second `step/request` (ADR-054). |
| `slot/migration` | — | DrainActivity | `{"slot":<str>,"from_spec":<spec>,"to_spec":<spec>,"reason":<str>}` | ops/control path | The only escape hatch from an immutable binding. |
| `recovery/budget-reset` | — | DrainActivity | `{"reason":<str>}` | operator | Running-preserving only (the ops path refuses idle/terminal sessions); audit only — budget state re-derives from config, context skips it. |
| `thread/started` | — | Thread | `{"thread_id":<uuid-str>,"parent":<session-uuid>,"worker":<session-uuid>,"pinned_version":<u64>,"task":<message payload>}`; additive: `"exec_worker":<str>`, `"job":{"run":<str>,"step":<str>,"attempt":<u64>,"step_digest":"sha256:<hex>","effect":"read"\|"write","candidates":[<str>],"rationale":<str>}` (the dispatch record written by the conductor, [JOB-MODEL](JOB-MODEL.md) §14 A2); additive (P4, gated by `superagent.board.gating`, MULTIAGENT-MODEL §7.2): `"spawn_uid":<uuid-str>`, `"multiagent":{"task_id":<uuid-str>,"assignee"?:<str>,"attempt"?:<u64>,"depends_on":[<str>],"group"?<str>}` — the parent side carries the full group, the child side the derivable subset `{task_id, assignee}` only | superagent / worker | Thread lineage + delegation authority wrapper are durable here (ADR-012). `exec_worker` is the execution-worker id the session is bound to (ADR-020, §4.2). The P4 keys are class-A audit keys outside `task` (§4.2); the wrapper stays byte-identical on both streams. |
| `thread/completed` | — | DrainActivity | `{"thread_id":<uuid-str>,"status":"completed"\|"failed"\|"interrupted","summary":<message payload>}`; additive (P4, gated by `superagent.board.gating`): `"spawn_uid":<uuid-str>`, `"multiagent":{"task_id":<uuid-str>,"outcome"?:<one of the six completion-borne §6.1.1 values>,"evidence_ref"?:{"session":<uuid-str>,"seq":<u64>},"contract"?:{"passed":<bool>,"errors":[<str>]}}` | worker | Worker outcome before the end write; never maps a failed gate onto `completed`. `outcome` persists only the six completion-borne values (the three no-carrier values stay derived on demand); `evidence_ref` is a pointer at the committed verdict, never a copy. |
| `evidence/verdict` | — | DrainActivity | `{"gate":<str>,"passed":<bool>,"checks":[{"name":<str>,"passed":<bool>,"detail"?<str>}],"attempt":<u64>}`; additive: `"job":{"step":<str>,"attempt":<u64>,"outcome":<str>}` on a step gate and `"job":{"outcome":<str>,"plan_digest":"sha256:<hex>","steps":[{"step":<str>,"outcome":<str>}],"failed_step"?<str>,"reason"?<str>}` on the run gate ([JOB-MODEL](JOB-MODEL.md) §14 A3) | loop (evidence gate) | Bounded, declarative stop gate (A6); no model call. |
| `channel/delivered` | — | DrainActivity | `{"channel":<str>,"target":<json>,"policy":<str>,"words"?<u64>,"truncated":<bool>}`; additive: `"attempt"`,`"first_attempt_ms"`,`"detail"` (present and `true` only on a details-request full delivery), `"voice"` (present and `true` only on a spoken delivery, ADR-066) | superagent/channel | Honest delivery receipt; no receipt means no delivery. |
| `input/spilled` | — | DrainActivity | `{"event_id":<str>,"channel":<str>,"topic":<str\|null>,"raw":<verbatim channel payload>}` | orchestrator (consumption) | Non-context sibling of the Inline `input/admitted`; recall reads the raw from here. |
| `control/interrupt` | — | DrainActivity | `{"action":"interrupt","reason":<str\|null>,"requested_by":<str\|null>,"requested_at_ms":<u64>,"target_epoch":<u64\|null>,"stale"?:true}` | orchestrator (consumption) | Durable interrupt intent audit (ADR-014). Never a message, never context. |
| `control/confirm` | — | DrainActivity | `{"action":"confirm","call_id":<str>,"decision":"approve"\|"reject","reason":<str\|null>,"requested_by":<str\|null>,"answered_at_ms":<u64>,"expired"?:true}` | orchestrator (consumption) | Durable confirmation intent audit (ADR-036): the human's answer to a pending `ask`, recorded before any effect. The loop applies it in call order and appends the answered `tool/result`. Never a message, never context. |
| `budget/raised` | — | DrainActivity | `{"action":"budget","requested_at_ms":<u64>,"tokens":<u64>?,"wall_clock_ms":<u64>?,"reason":<str\|null>,"requested_by":<str\|null>}` | orchestrator (consumption) | Durable budget-raise audit (ADR-055): a reserved control row consumed at a drain boundary adds to the session-lifetime projection (`base + Σraised − Σspent`). Both amounts absent is refused before admission, so at least one is present. Never a message, never context. |
| `memory/note` | — | DrainActivity | `{"subject":<non-empty str>,"text":<str>,"status":"candidate"\|"verified"\|"stale","derived_from":[{"session":<uuid-str>,"start_seq":<u64>,"end_seq":<u64>}],"verified_by"?:{"method":"transcript"\|"evidence_gate"\|"human","ref":<str>}}`; additive: `"redactions":[{"kind","name","count"}]` | memory dreamer drain | A note is a claim (MEMORY-MODEL): non-context, append-only, latest-per-subject; the fold lives in `kian-core::memory`. `text` passes the secret redactor before append (§4.2). |
| `session/ended` | — | Ended | envelope-level `"reason":"completed"\|"cancelled"\|"interrupted"\|"failed"\|"awaiting_input"\|"awaiting_budget"\|"rotated"`; `bytes: {}`; additive: `"rotated_to":<uuid-str>` plus `"events"`/`"migrated"` on a boot rotation's `rotated` end (ADR-040); additive: `"released":{"worker":<str>,"thread":<uuid-str>,"by":<str>,"reason":<str>}` present only on a release's `cancelled` end (ADR-035); additive: `"crashed":{"takeovers":<u64>,"reason":<str>}` present only on the `failed` end a takeover chain writes after `MAX_UNENDED_CLAIMS` claims without an end (ADR-041) | orchestrator | **One atomic append** with the transition (`NoHalfTerminated`). `interrupted`/`failed`/`awaiting_input`/`awaiting_budget` return the session to `idle` (resumable); `completed`/`cancelled`/`rotated` are terminal. A release closes an idle-ended worker thread with a `drain/claimed` + this `cancelled` end in one atomic batch (the claim is the fence; the state machine is unchanged). A crash-looping takeover chain ends with one `failed` end carrying `crashed` instead of re-driving forever (ADR-041). |

## 4. Cross-cutting payload contracts

### 4.1 Message payload 

`input/admitted`, `assistant/message`, `tool/call`, `tool/result` carry a message payload:
`{"role": <non-empty string>, "content": <any JSON>, "synthetic"?: true}`. `content` passes through
verbatim and is never reformatted (wire dialect v2 and the frozen request serialization are part of
this contract). `synthetic: true` marks steer-injected context and is strippable for replay/export.

Assistant messages may carry `"reasoning_content": <str>` **inside `content`** — the provider's
thinking-mode reasoning, persisted verbatim and replayed on the next request when present
([ADR-022](decisions/ADR-022-reasoning-content-replay.md)). It is context, not an additive
provenance key: a legacy assistant message without the key produces byte-identical request bytes,
and a non-thinking provider never emits it.

When `artifacts:` is configured ([ADR-056](decisions/ADR-056-artifacts-and-tool-result-spill.md)),
a `tool/result` whose successful `content.output` exceeds `inline_max_bytes` is rewritten **at the
append** to `{"artifact":{"id","size","media_type"},"preview":<str>,"truncated":true}`. `id` is the
sha256 hex of the full bytes (content-addressed, so identical payloads dedupe across sessions);
`preview` is the leading `inline_max_bytes / 4` bytes decoded lossily as UTF-8 and is exactly what
the model reads in context, now and on every replay; the blob is an operator-managed cache of the
full payload, never a second truth. Error results (`is_error: true`) never spill. A session that
did not configure `artifacts:` persists no such shape: the legacy payload is byte-identical.

### 4.2 Additive provenance keys (non-context)

These keys ride the payload **outside `content`**, are audit-only, and are omitted when unset:

- `turn/end`: `"usage"` (provider `usage` object verbatim; no normalization/pricing in core),
  `"error": {"class","action"}`.
- `turn/start` + `turn/end`: `"purpose": "compact"` brackets a maintenance turn; absent on an
  ordinary model turn (ADR-058).
- `step/request`: `"purpose": "compact"` marks the summarizer request; absent means an ordinary
  turn (ADR-058).
- `compact`: `"produced_by": {"step_seq"}` (the summarizer `step/request` that produced the
  summary), `"reason"`, `"folded_messages"`, `"reclaimed_tokens"` — audit-only fold metadata;
  the summary itself stays the message payload (ADR-058).
- `tool/call`: `"permission": {"evaluated","policy","matched","reason"?}` (ADR-015). `policy` is the closed
  set `"default" | "configured" | "hook"`; `"hook"` rides a `tool/pre` veto denial and pairs with
  `matched = "<plugin>:<hook>"` ([PLUGIN-SPEC](PLUGIN-SPEC.md) §4.4, [ADR-028](decisions/ADR-028-harness-hook-plugins.md))
  — an additive payload **value**, not a new key.
- `tool/result`: `"redactions": [{"kind","name","count"}]`; `"confirmation":
  {"call_id","decision","requested_by","answered_at_ms","expired"?}` — the applied human
  decision, present only on a result produced by answering an `ask` (ADR-036).
- `tool/call` + `tool/result`: `"recovered": "not_dispatched" | "in_flight"` — the open-call
  recovery marker (CONFIRMATION-MODEL §15.6, DESIGN §2.3). A drain that died inside a tool batch
  leaves an assistant `tool_calls` entry without its resolution; the next claim reconstructs the
  missing `tool/call` (when it was never logged) and appends one honest-error `tool/result`
  before any provider request. `not_dispatched` means the call never left the process;
  `in_flight` means its effect is unknown. A reconstructed call carries **no** `permission` key
  (no decision was recorded, so none is fabricated).
- `input/admitted`: `"dream": {"source_session","start_seq","end_seq","scope"}` — the trusted
  dream marker, set only by the ops plane's `memory_dream` admit (the public wire refuses the
  reserved envelope key sets with `-32602`, so client content cannot land one; kian-94, review
  B-2′). Dreamer routing and note
  provenance read this marker, never message content (a user-typed `[kian-dream: …]` header is
  ordinary content); omitted on every other row.
- `input/admitted`: `"memory": {"scope","notes","bytes","items","truncated","source_seq","redactions"}` — the
  creation-time injection provenance marker (MEMORY-MODEL §6 I1): the rendered memory index is the
  message `content` (a `role:"system"`, `synthetic:true` admission); this key records which
  verified notes (by derived event id), how many bytes/items, whether caps truncated the render,
  and the memory snapshot's `source_seq` (I7 pinning). `redactions` is the value-free
  secret-redaction report over the rendered index — aggregate `{kind,name,count}` entries only,
  the same discipline as the `memory/note` and `tool/result` redaction reports below. Set only by
  the server's own creation path
  (`Engine::admit_quiet` consuming the reserved `{content,memory}` row envelope); the public wire
  refuses that exact shape with `-32602` (kian-99). Non-context by construction:
  `derive_model_context` reads `role`/`content`/`synthetic` only, so the key never enters a model
  request.
- `input/admitted`: `defer`: {`policy`,`deferred`,`eligible_at_ms`,`reason`} — the off-peak
  decision (ADR-060, CRON-MODEL §6.1), present exactly when the cron schedule declared
  `defer: off_peak` (an immediate schedule's bytes are unchanged: no key). It rides beside the
  `cron` proof and outside `content`, so the delivered turn is still the prompt alone;
  `deferred: false` records a decision that ran immediately because the next off-peak was beyond
  `defer.max_wait_ms` or the admit instant was already off peak, and `reason` says which. The
  row is an ordinary durable inbox row: `eligible_at` is the gate that keeps it invisible until
  due; no new event kind is involved.
- `memory/note`: `"redactions": [{"kind","name","count"}]` — note `text` passes the secret
  redactor before append; the report is aggregate counts only (the `tool/result` precedent).
- `thread/started` and the delegation wrapper (ADR-020): `"exec_worker"` — the **execution-worker
  id** (a configured `endpoints[].id`) the delegated worker session is bound to. The same key and
  the same value appear twice, in one payload each: as a top-level `thread/started` payload key
  (audit echo, readable without parsing the task) and inside the task's delegation-wrapper
  `content` (the worker's first message — the wrapper *is* the message, so the binding is recorded
  in the same bytes the model consumed). `worker` in that payload keeps its own meaning (the worker
  **session** uuid); `worker` and `exec_worker` are never conflated and `worker` is never
  repurposed. Absent = legacy unbound (both places, byte-identical legacy bytes); a present but
  malformed value (non-string / empty) is never guessed, never defaulted and never fallback-routed —
  it is an honest refusal (ADR-020 I7). The two copies are derived from the same wrapper, so a
  stream where they disagree is malformed history and is refused rather than reconciled.
  Context effect, stated precisely: the top-level payload key is a pure additive audit key (it rides
  outside `content`, so `derive_model_context` — which reads `role`/`content`/`synthetic` only —
  never puts it in a request), and it is the key this section's rule governs. The wrapper copy is a
  field of a message payload's `content`, which passes through verbatim (§4.1) — the same standing
  as `speculative` and `mandate`: model-visible by design, because the worker is entitled to know
  which machine it runs on, and legitimate because only **new** message bytes carry it. Legacy
  wrapper bytes and every persisted `step/request.body` stay untouched, so history is not rewritten
  and no live session's prefix changes as a result of the record itself.
- `thread/started` / `thread/completed` (MULTIAGENT-MODEL §7.2, P4): `"spawn_uid"` and the
  namespaced `"multiagent"` group — the task facts the audit needs (`task_id`, `assignee`,
  `attempt`, `depends_on[]`, `group?` on the parent's `thread/started`; `task_id`, `assignee` on the
  child's; `task_id`, `outcome`, `evidence_ref?`, `contract?` on the child's `thread/completed`).
  Gated by `superagent.board.gating` (default `false` ⇒ **no key is emitted at all** — P3 bytes
  exactly); omitted when unset, never `null`; legacy event bytes parse/replay byte-identical (§6.3).
  The two sides' `thread/started` payloads differ by design — the parent-only bookkeeping
  (`attempt`, `depends_on`, `group`) never rides the child's event — while the embedded delegation
  wrapper (`task`) stays byte-identical between them. `outcome` is written at the completion by
  `outcome_for_completion` (the §6.2 table applied to the completion being appended) and persists
  only the six completion-borne values; the three no-carrier values (`stalled`, `interrupted`,
  `input_required`) stay derived on demand. `evidence_ref` is `{"session","seq"}` — a durable
  pointer at the committed `evidence/verdict` that decided the outcome, never a copy of it.
  Non-context, stated precisely: the keys ride the payload **outside `content`**, and
  `derive_model_context` — which reads `role`/`content`/`synthetic` only — never puts them in a
  model request, so they must not change `derive_model_context` output or model request bytes; the
  model-visible half of every task fact is the wrapper/report **content** copy (class B/C), which
  P1/P2 already carry.

Rule: an additive key must leave legacy event bytes byte-identical and must not change
`derive_model_context` output or model request bytes. If a proposed field is model-visible, it is
**not** an additive audit key — it is a context change and needs the full §6 process.

### 4.3 Compact and context derivation

`derive_model_context(events)` rules (frozen):
history is immutable; a `compact` summary is emitted at `from_seq` and events in `[a,b)` are
skipped; synthetic messages are strippable; request bytes are the persisted `step/request.body`;
only `compact` may invalidate the prefix (`unexpected_prefix_change` must stay zero).

Production folding is planned by a pure function and executed by the loop as an ordinary
persisted turn ([ADR-058](decisions/ADR-058-production-compaction-and-maintenance-reserve.md)):
the planner keeps the most recent turns verbatim, starts after the previous compact, requires a
minimum reclaim and returns a strictly-past disjoint range; the loop folds only under measured
context pressure (a declared window) or an authoritative provider context overflow, never for
cost, persists the summarizer request as `step/request` with `purpose: "compact"`, and appends
the `compact` event only after the summary text exists. A failed summarizer leaves the projection
untouched (its request is still a persisted, priced request), and at most two folds run per drain
episode.

### 4.4 Ordering, idempotency, derived id

- Ordering is `seq` per session; drains append under `expectedSeq` CAS + epoch fence.
- Channel admission is idempotent per `(session, event_id)`; inbox admission is idempotent per
  `(session, idempotency_key)`; `control/interrupt` is idempotent per
  `interrupt:<session>:<target_epoch>`; `budget/raised` per
  `budget:<session>:<tokens>:<wall_clock_ms>:<requested_at_ms>` (a caller key wins, ADR-055).
- **Derived event id** (never stored): `evt_` + `uuid5(session.0, "kian:event:<seq>")`.
  Mirrored in `kian-sdk`; exposed by `session export --with-event-ids`; not an envelope field.

### 4.5 Reserved inbox-row envelopes (pre-event, NOT event kinds)

The control plane writes only inbox rows; the drain consumes them into events. Seven reserved
row-payload shapes must not be confused with event payloads:

| Row payload | Consumed into |
|---|---|
| `{"synthetic":true,"content":X}` | `input/admitted` `{role:"user",content:X,synthetic:true}` |
| `{"content":X,"spill":Y}` | the `input/admitted` + `input/spilled` pair in one fenced batch |
| `{"content":X,"dream":M}` | ONE `input/admitted` `{role:"user",content:X,dream:M}` (M is the trusted dream marker, §4.2) |
| `{"content":X,"memory":M}` | ONE `input/admitted` `{role:"system",content:X,synthetic:true,memory:M}` (M is the creation-time injection marker, §4.2) |
| `{"kian":{"type":"control","action":"interrupt",...}}` | `control/interrupt` (never a message) |
| `{"kian":{"type":"control","action":"confirm",...}}` | `control/confirm` (never a message; the loop applies the decision) |
| `{"kian":{"type":"control","action":"budget",...}}` | `budget/raised` (never a message; adds to the session budget projection) |

Every envelope is an **exact key-set match**: a row payload with extra keys is ordinary content —
the `synthetic` arm consumes only at exactly `{synthetic,content}` (kian-96, review N-1), so a public
superset key set passes the wire guard and lands as plain nested content, never as a client-minted
strippable marker.

## 5. Invariants (must hold for every change)

1. **Sole truth**: all session state is a projection of the event stream; projections are
   disposable/rebuildable.
2. **Append-only + additions-only**: no update/delete/rename/reformat; new kinds append; their
   serialization is golden-tested.
3. **Persist-what-you-send**: the exact model request bytes are a `step/request` before the call;
   replay is byte-identical.
4. **Cache-byte stability**: history is immutable; only `compact` invalidates the prefix; serialization
   changes are constraint-3 events.
5. **Fencing**: epoch + expectedSeq CAS; lease is liveness only.
6. **Atomic end**: one append for `session/ended` + transition.
7. **Honesty**: `interrupted` ≠ `failed` ≠ `awaiting_input`; an empty model response is a soft
   failure; a stop passes the evidence gate; a denied tool call is an honest error result.
8. **Narrow waist**: core kinds carry no provider/channel/harness-specific schema.

## 6. Change protocol (required for every event change)

### 6.1 Order of work

1. **Update this document first** (this file is the gate: a change that is not described here is
   not approved).
2. Update the design rationale in `design/DESIGN.md` §3 (link, do not duplicate the catalog).
3. If the change is semantic (new kind, envelope shape, state effect, context effect, version),
   write an ADR (append-only; supersede by linking, never edit).
4. Implement in `kian-core` (+ owner crate) and update the golden tests.
5. Update the docs map and every count/claim (`README` status table, `CHANGELOG`, SPEC capability record);
   `docs_map` must stay green.
6. Run the full four gates.

### 6.2 New kind checklist

A new kind is the **last** resort (config → plugin → seam → core primitive). To add one:

- [ ] ADR states why no existing kind/additive payload expresses the fact.
- [ ] Classification decided: durable (not live), context-bearing/affecting or non-context, state
      trigger (usually `DrainActivity`).
- [ ] `EventKind` variant + serde tag + `as_str`; golden JSON; count/regression updates
      (e.g. the "exactly N kinds" assertion).
- [ ] `derive_model_context` behavior proven unchanged for non-context kinds (byte-identical
      context test) and for the state machine (illegal-transition table).
- [ ] This catalog table updated; a doc↔code drift test must pass.
- [ ] No model request/tool-schema/context bytes change; `FROZEN_CHAT_BODY_SHA256` unchanged.

### 6.3 Payload and envelope evolution

- **Additive payload key**: allowed; omit when unset; legacy event bytes parse/replay byte-identical;
  no context change; document in §3/§4 and test the legacy shape.
- **Widened envelope value**: a closed value set of an envelope-level enum (`session/ended.reason`)
  may widen exactly like an additive payload value — the *shape* is unchanged, legacy streams
  replay unchanged, and the new value carries a golden plus a frozen regression for every existing
  value (`awaiting_input` is the precedent, ADR-036). Recorded cost: a binary older than the
  widening cannot read a stream containing the new value — the same forward-compatibility posture
  the additions-only kind discipline already accepts.
- **Additive payload value**: a closed value set of an existing key may widen — no new key, no kind
  change, legacy streams replay unchanged (they never carry the new value). The widening change
  updates the §3 row and carries byte goldens for the new value plus a frozen regression for every
  pre-existing value (the `permission.policy:"hook"` widening is the precedent,
  [ADR-028](decisions/ADR-028-harness-hook-plugins.md)).
- **Envelope change**: a new schema `version` plus an adjacent migration; committed generations are
  never rewritten; old streams must reopen cleanly.
- **Rename/removal/reformat**: forbidden. Express the new semantics with a new additive kind/payload
  and keep the old one; a breaking serialization change is a constraint-3 event (observable
  invalidation + golden-hash update).

### 6.4 Required enforcement tests

- Envelope/kind golden JSON round-trips (`kian-core`).
- Additions-only regression: every prior kind's bytes frozen.
- A2 byte-identical replay and context golden.
- Non-context kinds leave `derive_model_context`/request bytes unchanged.
- Export/import byte-identical (and postgres/sqlite parity when storage changes).
- This catalog ↔ `EventKind` drift test (`crates/kian-cli/tests/docs_map.rs`).

## 7. What is NOT an event

- **Live signals**: streaming token chunks, heartbeats, progress; event deltas (if ever added) are
  stream-only and never persisted.
- **Pre-event durable state**: inbox input rows, control rows, exec queue rows, async job rows —
  durable, but not an event until consumed/appended.
- **Derived projections**: session state, ticket stage, outbox view, usage/cache reports — pure
  projections over events; never authoritative state.
- **Bundle-level provenance**: channel wrappers, untrusted markers, mandates, the speculative
  marker and the execution-worker binding `exec_worker` (they ride message payloads /
  `thread/started` task payloads as additive keys, not new event kinds).
- **Envelope/audit metadata**: the derived event id is computed, never stored; export wrappers are
  not events.

## 8. Where it lives

| Surface | Role |
|---|---|
| `crates/kian-core/src/events.rs` | executable schema: envelope, `EventKind`, `EndReason`, golden tests |
| `crates/kian-core/src/context.rs` | `derive_model_context` payload parsing and A2/A3 rules |
| `crates/kian-core/src/state.rs` | transition table and projection |
| `crates/kian-core/src/control.rs` | reserved control-envelope row convention (pre-event) |
| `crates/kian-core/src/secrets.rs` | redaction report used by `tool/result` provenance |
| `crates/kian-store/src/artifact.rs` | content-addressed blob store behind `tool/result` artifact references (disposable; ADR-056) |
| `design/DESIGN.md` §3 | design rationale (links here for the catalog) |
| `design/decisions/ADR-NNN-*.md` | why a decision was made (append-only) |
| `crates/kian-cli/tests/docs_map.rs` | doc↔code drift + docs-map link enforcement |
