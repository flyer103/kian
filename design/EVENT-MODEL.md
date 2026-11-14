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
| Ended | `session/ended{reason}` | `running → completed/cancelled` (terminal) for `completed`/`cancelled`; `running → idle` (resumable) for `interrupted`/`failed` |
| State-preserving | `input/admitted` (Input trigger), `thread/started` (Thread trigger) | preserves `idle`/`running`/`recoverable` |
| DrainActivity | all other kinds | legal only while `running` (or `archived` where the table allows) |

State is **read only from the projection** (`project(events)`), never from a column or cached
field. `recoverable` and `archived` are liveness/ops overlays, not event-sourced transitions.

## 3. Kind catalog (22, additions-only)

Payload schemas below are the **frozen** shape at schema v0 unless marked additive. "Additive"
keys are optional, omitted when unset, and leave legacy event bytes byte-identical. Emitters name
the subsystem that owns the payload schema; the core kind set stays provider/channel-agnostic.

| Kind | Ctx | State | Payload (payload = `bytes`) | Emitter | Notes |
|---|---|---|---|---|---|
| `session/created` | — | Created | `{}` | orchestrator | First event of a stream. |
| `input/admitted` | C | Input | `{"role":"<non-empty>","content":<any>,"synthetic"?:true}`; additive: `"dream":{"source_session":<uuid-str>,"start_seq":<u64>,"end_seq":<u64>,"scope":<str>}`, `"memory":{"scope":<str>,"notes":[<event-id>...],"bytes":<u64>,"items":<u64>,"truncated":<bool>,"source_seq":<u64>,"redactions":[{"kind","name","count"}]}` (the creation-time injection marker, §4.2) | orchestrator (drain consumption) | Durable inbox row consumed into context. `synthetic` marks steer-injected content (strippable, A6). `dream` (§4.2) is the ops-set dream marker — trusted routing/provenance, never read from content. |
| `drain/claimed` | — | Claimed | `{"holder":<str>,"epoch":<u64>}` | orchestrator | Claim audit. Epoch is the fencing token; claim admission is conditional (ADR-009). |
| `turn/start` | — | DrainActivity | `{"iteration":<u64>}` | loop | Brackets one provider request cycle. Iteration derived from the stream. |
| `turn/end` | — | DrainActivity | `{"iteration":<u64>,"outcome"?:"final_answer"\|"tool_calls"\|"soft_failure"\|"error","tool_calls"?:<u64>}`; additive: `"usage":<provider usage verbatim>`, `"error":{"class":<class>,"action":<policy disposition\|"none">}` | loop | Usage/error are audit-only. |
| `step/request` | — | DrainActivity | `{"slot":<str>,"provider":<str>,"model":<str>,"iteration":<u64>,"body":<exact request bytes>}` | loop / provider persist hook | **A2**: persisted before the request flies; replay is byte-identical. |
| `assistant/message` | C | DrainActivity | `{"role":"assistant","content":{"text":<str>,"tool_calls"?:[...],"reasoning_content"?:<str>}}` | loop | The sent bytes are the truth; never rewritten. `reasoning_content` is the provider's thinking-mode reasoning, part of context (replayed on the wire); absent for non-thinking providers and legacy events ([ADR-022](decisions/ADR-022-reasoning-content-replay.md)). |
| `tool/call` | C | DrainActivity | `{"role":"assistant","content":{"id":<str>,"name":<str>,"arguments":<json>}}`; additive: `"permission":{"evaluated":"allow"\|"deny","policy":"default"\|"configured","matched":<str>,"reason"?}` | loop | **Log-before-execute**: appended before `invoke`. Deny never invokes (ADR-015). |
| `tool/result` | C | DrainActivity | `{"role":"tool","content":{"call_id":<str>,"output":<json>,"is_error":<bool>}}`; additive: `"redactions":[{"kind","name","count"}]` | loop / exec | Redaction provenance never carries secret values. |
| `compact` | c | DrainActivity | `{"from_seq":<u64>,"to_seq":<u64>,"summary":{"role":<str>,"content":<json>}}` | recovery reactor | The only deliberate prefix-invalidation point; ranges pairwise-disjoint, `a < b <= seq`. |
| `slot/binding` | — | DrainActivity | `{"slot":<str>,"spec":{"provider","model","fallback":[...]}}` | loop at first use | Slot → provider/model fixed for the session's lifetime (ADR-006). |
| `slot/fallback` | — | DrainActivity | `{"slot":<str>,"from":{"provider","model"},"to":{"provider","model"}}` | provider turn | One fallback-chain switch, persisted before the fallback call. |
| `slot/migration` | — | DrainActivity | `{"slot":<str>,"from_spec":<spec>,"to_spec":<spec>,"reason":<str>}` | ops/control path | The only escape hatch from an immutable binding. |
| `recovery/budget-reset` | — | DrainActivity | `{"reason":<str>}` | operator | Running-preserving only (the ops path refuses idle/terminal sessions); audit only — budget state re-derives from config, context skips it. |
| `thread/started` | — | Thread | `{"thread_id":<uuid-str>,"parent":<session-uuid>,"worker":<session-uuid>,"pinned_version":<u64>,"task":<message payload>}`; additive: `"exec_worker":<str>` | superagent / worker | Thread lineage + delegation authority wrapper are durable here (ADR-012). `exec_worker` is the execution-worker id the session is bound to (ADR-020, §4.2). |
| `thread/completed` | — | DrainActivity | `{"thread_id":<uuid-str>,"status":"completed"\|"failed"\|"interrupted","summary":<message payload>}` | worker | Worker outcome before the end write; never maps a failed gate onto `completed`. |
| `evidence/verdict` | — | DrainActivity | `{"gate":<str>,"passed":<bool>,"checks":[{"name":<str>,"passed":<bool>,"detail"?<str>}],"attempt":<u64>}` | loop (evidence gate) | Bounded, declarative stop gate (A6); no model call. |
| `channel/delivered` | — | DrainActivity | `{"channel":<str>,"target":<json>,"policy":<str>,"words"?<u64>,"truncated":<bool>}`; additive: `"attempt"`,`"first_attempt_ms"`,`"detail"` (present and `true` only on a details-request full delivery) | superagent/channel | Honest delivery receipt; no receipt means no delivery. |
| `input/spilled` | — | DrainActivity | `{"event_id":<str>,"channel":<str>,"topic":<str\|null>,"raw":<verbatim channel payload>}` | orchestrator (consumption) | Non-context sibling of the Inline `input/admitted`; recall reads the raw from here. |
| `control/interrupt` | — | DrainActivity | `{"action":"interrupt","reason":<str\|null>,"requested_by":<str\|null>,"requested_at_ms":<u64>,"target_epoch":<u64\|null>,"stale"?:true}` | orchestrator (consumption) | Durable interrupt intent audit (ADR-014). Never a message, never context. |
| `memory/note` | — | DrainActivity | `{"subject":<non-empty str>,"text":<str>,"status":"candidate"\|"verified"\|"stale","derived_from":[{"session":<uuid-str>,"start_seq":<u64>,"end_seq":<u64>}],"verified_by"?:{"method":"transcript"\|"evidence_gate"\|"human","ref":<str>}}`; additive: `"redactions":[{"kind","name","count"}]` | memory dreamer drain | A note is a claim (MEMORY-MODEL): non-context, append-only, latest-per-subject; the fold lives in `kian-core::memory`. `text` passes the secret redactor before append (§4.2). |
| `session/ended` | — | Ended | envelope-level `"reason":"completed"\|"cancelled"\|"interrupted"\|"failed"`; `bytes: {}` | orchestrator | **One atomic append** with the transition (`NoHalfTerminated`). `interrupted`/`failed` return the session to `idle` (resumable); only `completed`/`cancelled` are terminal. |

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

### 4.2 Additive provenance keys (non-context)

These keys ride the payload **outside `content`**, are audit-only, and are omitted when unset:

- `turn/end`: `"usage"` (provider `usage` object verbatim; no normalization/pricing in core),
  `"error": {"class","action"}`.
- `tool/call`: `"permission": {"evaluated","policy","matched","reason"?}` (ADR-015).
- `tool/result`: `"redactions": [{"kind","name","count"}]`.
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

Rule: an additive key must leave legacy event bytes byte-identical and must not change
`derive_model_context` output or model request bytes. If a proposed field is model-visible, it is
**not** an additive audit key — it is a context change and needs the full §6 process.

### 4.3 Compact and context derivation

`derive_model_context(events)` rules (frozen):
history is immutable; a `compact` summary is emitted at `from_seq` and events in `[a,b)` are
skipped; synthetic messages are strippable; request bytes are the persisted `step/request.body`;
only `compact` may invalidate the prefix (`unexpected_prefix_change` must stay zero).

### 4.4 Ordering, idempotency, derived id

- Ordering is `seq` per session; drains append under `expectedSeq` CAS + epoch fence.
- Channel admission is idempotent per `(session, event_id)`; inbox admission is idempotent per
  `(session, idempotency_key)`; `control/interrupt` is idempotent per
  `interrupt:<session>:<target_epoch>`.
- **Derived event id** (never stored): `evt_` + `uuid5(session.0, "kian:event:<seq>")`.
  Mirrored in `kian-sdk`; exposed by `session export --with-event-ids`; not an envelope field.

### 4.5 Reserved inbox-row envelopes (pre-event, NOT event kinds)

The control plane writes only inbox rows; the drain consumes them into events. Five reserved
row-payload shapes must not be confused with event payloads:

| Row payload | Consumed into |
|---|---|
| `{"synthetic":true,"content":X}` | `input/admitted` `{role:"user",content:X,synthetic:true}` |
| `{"content":X,"spill":Y}` | the `input/admitted` + `input/spilled` pair in one fenced batch |
| `{"content":X,"dream":M}` | ONE `input/admitted` `{role:"user",content:X,dream:M}` (M is the trusted dream marker, §4.2) |
| `{"content":X,"memory":M}` | ONE `input/admitted` `{role:"system",content:X,synthetic:true,memory:M}` (M is the creation-time injection marker, §4.2) |
| `{"kian":{"type":"control","action":"interrupt",...}}` | `control/interrupt` (never a message) |

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
7. **Honesty**: `interrupted` ≠ `failed`; an empty model response is a soft failure; a stop passes
   the evidence gate; a denied tool call is an honest error result.
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
| `design/DESIGN.md` §3 | design rationale (links here for the catalog) |
| `design/decisions/ADR-NNN-*.md` | why a decision was made (append-only) |
| `crates/kian-cli/tests/docs_map.rs` | doc↔code drift + docs-map link enforcement |
