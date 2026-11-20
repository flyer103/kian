# Kian API Contract — normative (v0)

> **Status: normative.** This file is the single source of truth for Kian's inbound wire API: the
> HTTP surfaces served by `kian serve`, and the KiandProto frames spoken by `kiand`.
>
> **Authority chain.** This file wins for endpoints, JSON-RPC methods, params/results, error codes,
> SSE frame names, metrics top-level keys, the webhook status matrix and KiandProto frame shapes.
> [design/EVENT-MODEL.md](EVENT-MODEL.md) wins for event envelopes and payloads.
> [design/DESIGN.md](DESIGN.md) §5 keeps the control-plane semantics and rationale and links here
> instead of restating the wire. [docs/SPEC.md](../docs/SPEC.md) §2.4/§3 and the
> [README](../README.md) docs map link here. If code and this document disagree, reconcile them in
> the same change — a silent drift is a bug.
>
> **Scope: inbound surfaces.** What a client, operator, webhook sender or endpoint daemon can call.
> Outbound clients Kian speaks as a client — OpenAI-compatible model providers, GitHub REST, the
> Lark OpenAPI, a `dsh` child over stdio — are **not** part of this contract.
>
> **Enforcement.** `crates/kian-sdk/tests/fixtures/` pins request/response bytes per method;
> `crates/kian-cli/tests/{e2e_serve,ops_commands,webhook_receiver,iron_rule}.rs` pin behavior; and
> `crates/kian-cli/tests/docs_map.rs` pins this file's links plus the method catalog ↔ dispatch
> drift.

## 1. Surfaces

| Surface | Wire | Mount condition | Auth |
|---|---|---|---|
| Control plane | `POST /rpc` (JSON-RPC 2.0) | always | none (v0; see §2.4) |
| Event stream | `GET /sessions/{id}/events` (SSE) | always | none |
| Fan-in event stream | `GET /events` (SSE, §5.4) | `api.event_stream.enabled` (default true); otherwise the path 404s | none |
| Operational plane | `POST /ops/rpc` (JSON-RPC 2.0) | always | none |
| Metrics | `GET /metrics` (JSON) | always | none |
| GitHub webhook | `POST /webhooks/github` | only when a `type: github` channel is configured; otherwise the path 404s | HMAC-SHA256 |
| KiandProto | TCP, one JSON frame per line | when `endpoints:` is configured and an endpoint dials | none (v0) |

All HTTP surfaces share the `listen` address from `kian.yaml`.

## 2. Common conventions

### 2.1 Envelope

Request body (POST surfaces):

```json
{"jsonrpc":"2.0","id":<any JSON value>,"method":"<name>","params":<object|null>}
```

- `jsonrpc` must be exactly `"2.0"`.
- `id` is echoed verbatim; `null` is allowed.
- `params` defaults to `{}` when omitted. Unknown params are ignored (forward compatibility).
- A request without a `method` string, or with the wrong `jsonrpc`, is `-32600`.

Success (HTTP 200):

```json
{"jsonrpc":"2.0","id":<echoed>,"result":<object>}
```

Error (HTTP 200 — the JSON-RPC envelope is the error carrier, not the HTTP status):

```json
{"jsonrpc":"2.0","id":<echoed>,"error":{"code":<integer>,"message":"<string>"}}
```

Transport-level failures (non-JSON body, wrong content type) are rejected by the HTTP layer with a
non-200 status; they are not JSON-RPC errors. Method names are flat strings; there is no version
suffix.

### 2.2 Error-code registry

| Code | Name | Meaning | Surfaces |
|---|---|---|---|
| -32600 | invalid_request | Malformed envelope: missing `method`, or `jsonrpc` != `"2.0"` | both RPC |
| -32601 | method_not_found | Unknown method; the message lists the surface's current set | both RPC |
| -32602 | invalid_params | Missing/ill-typed required param; non-UUID `session`; non-empty params for `create_session` | both RPC |
| -32000 | internal | Server-side failure. For ops, the message may carry diagnostics (e.g. the configured mock channels) | both RPC |
| -32001 | unknown_session | No event stream / inbox row for the UUID | both RPC |
| -32002 | session_busy | An ops action requires an idle session; a drain holds it. `release_worker` also answers it for a thread still holding eligible input or a live command row (the message says which) | ops |
| -32003 | session_terminal | The session is terminal (completed/cancelled/archived) | ops |
| -32004 | session_idle | `recovery_budget_reset` on a session whose recovery budget was never consumed | ops |
| -32005 | memory_not_configured | `memory_dream` on a server without a `memory:` section — a closed refusal, never a silent no-op | ops |
| -32006 | dream_range_too_large | `memory_dream` over a source range whose transcript exceeds `memory.dreamer.max_bytes_read` — refused, not truncated | ops |
| -32007 | archived | The session's online range has been archived (READ-MODEL §7, P2b); use the archive package / `session import` | both RPC + SSE |
| -32008 | worker_not_bound | `release_worker` for an unknown worker id, no live bound session, or a never-started reservation — nothing was closed | ops |

Codes are append-only: existing codes never change meaning or get reused; a new failure class gets a
new documented code. The confirmation plane introduces **no** new code: its non-normal answers are
`outcome` values (`not_pending`/`expired`), and its refusals reuse `-32001`/`-32007`/`-32602`.

### 2.3 Values

| Field | Contract |
|---|---|
| `session` | UUID string (hyphenated, 36 chars) |
| `content` | Any JSON value except a reserved row-envelope shape — an object whose keys are exactly `{content,dream}`, `{content,spill}`, `{synthetic,content}` or `{content,memory}`, or the reserved control envelope `{"kian":{"type":"control",...}}` (any action), is `-32602`: those shapes are admitted only by the server's internal planes (§3.2/§4.2), never by public input. Typical use is a string or a structured message. Input semantics (synthetic markers, Inline/Spill shaping, provenance wrappers) are owned by DESIGN/EVENT-MODEL, not by this wire |
| `idempotency_key` | Optional string. Replaying it for the same session does not create a second durable row; the result reports `deduplicated: true` |
| `projection.state` | `idle \| running \| completed \| cancelled` — the event-derived states. `recoverable` is a liveness overlay (DESIGN §2.2) and `archived` has no v0 event, so neither can appear in a pure projection |
| `topic` | Optional string; JSON `null` when absent |

### 2.4 Trust boundary (v0)

- `/rpc`, `/ops/rpc` and `/metrics` have **no authentication or authorization** in v0. The
  deployment is the boundary: bind a private address, or put an authenticating reverse proxy in
  front.
- The GitHub webhook is the only surface intended for internet traffic; it is HMAC-authenticated and
  rate-limited (§7).
- There is no multi-tenancy: one server, one database, one trusted principal (or a proxy that
  enforces more).

## 3. Control plane — `POST /rpc`

The control plane only writes the inbox and reads projections/events (the layered iron rule,
AGENTS.md constraint 9). It never starts, steers or stops execution directly.

### 3.1 Method catalog

| Method | Params | Result |
|---|---|---|
| `create_session` | `{}` (or `null`) | `{"session":"<uuid>"}` |
| `submit` | `{"session":"<uuid>","content":<any>,"idempotency_key":"<string>"?}` | `{"deduplicated":<bool>}` |
| `steer` | same as `submit` | `{"deduplicated":<bool>}` |
| `queue` | same as `submit` | `{"deduplicated":<bool>}` |
| `interrupt` | `{"session":"<uuid>"}` | `{"outcome":"requested" \| "not_draining"}` |
| `confirm` | `{"session":"<uuid>","call_id":"<str>","decision":"approve" \| "reject","reason":"<str>"?,"requested_by":"<str>"?}` | `{"outcome":"accepted" \| "not_pending" \| "expired"}` |
| `pending` | `{"session":"<uuid>"}` | `{"pending":[{"call_id","tool","arguments","requested_at_ms","expired"}]}` |
| `projection` | `{"session":"<uuid>","since":<u64>?}` | `{"version":<u64>,"state":"<see §2.3>"}` or `{"version":<u64>,"unchanged":true}` |

### 3.2 Semantics

- `create_session` takes no params; the default agent, model slots and tools come from `kian.yaml`.
  The new session is `idle`.
- `submit` admits one durable input row before any wake fires (admit-before-wake). The result only
  reports idempotency; progress is observed on the event stream. A `content` that is a reserved
  row-envelope key set (§2.3) is refused with `-32602` before anything is admitted.
- `steer` uses the same admit path as `submit` but carries the strippable synthetic marker; it lands
  at the next provider-turn safe boundary and never inside an in-flight provider turn.
- `queue` uses the same admit path; the input is promoted at the next safe boundary and never
  interrupts the current turn.
- `interrupt` requests an abort. The intent is durable (ADR-014): whenever work may still be in
  flight the server also admits a control-envelope inbox row that the owning drain consumes into a
  `control/interrupt` event, so the intent survives a drain crash and crosses replicas.
  `requested` = an abort may be settled; `not_draining` = nothing was in flight (an honest no-op).
  An interrupted result is never reported as `completed`.
- `confirm` answers one pending ask (CONFIRMATION-MODEL §5): it validates the call against the
  durable pending set, applies the configured TTL, admits ONE reserved confirmation row under the
  derived key `confirm:<session>:<call_id>` and wakes. It writes nothing else and never touches
  execution. Outcomes: `accepted` = the row is durable and the decision takes effect on the stream;
  `not_pending` = no unanswered ask with that `call_id` (already answered — including a decision
  already recorded but not yet applied — unknown, not an ask, or the session is terminal);
  `expired` = the ask lapsed under `confirmation.ttl_ms` and a durable reject-with-`expired` row was
  admitted so the session resumes (the caller's decision was **not** applied). An archived session
  is `-32007`. A malformed `call_id`/`decision` is `-32602`.
- `pending` is a pure read of one session's stream: the unanswered asks in call order with the TTL
  overlay computed at read time (`expired` is a liveness overlay, never persisted). Unknown session
  `-32001`, archived `-32007`.
- `projection` reads the stream and returns the pure projection state plus `version` — the last
  committed seq (`head.next_seq - 1`, READ-MODEL §3). The optional `since` (u64) carries the
  caller's last version: `since == version` returns `{"version":V,"unchanged":true}` with **no**
  state (the read costs one version check, no stream scan); any older value returns the full
  `{"version":V,"state":…}`. `since > version` is `-32602` (the client is ahead or pointed at
  another store — an explicit refusal, never a silent resync); a non-u64 `since` (including
  explicit `null`) is likewise `-32602`. A session with no events is `-32001` either way. An
  archived session (online range removed, READ-MODEL §7) is `-32007` — an explicit refusal
  naming the archive package, never an empty projection. The
  payload is always derived from one committed prefix (READ-MODEL R2: a concurrent append may
  make an answer stale, never mixed).

### 3.3 Errors

`-32600`, `-32601`, `-32602`, `-32000`, `-32001`, `-32007` (§2.2).

## 4. Operational plane — `POST /ops/rpc`

The ops plane carries explicit operator actions whose effects are durable and auditable. It is
mounted outside the control-plane handler module and obeys the same iron rule.

### 4.1 Method catalog

| Method | Params | Result |
|---|---|---|
| `wake_now` | `{"session":"<uuid>"}` | `{"outcome":"started" \| "folded"}` |
| `recovery_budget_reset` | `{"session":"<uuid>","reason":"<string>"?}` | `{"seq":<u64>,"epoch":<u64>,"ts":<u64>}` |
| `mock_channel_send` | `{"channel":"<string>","topic":"<string>"?,"payload":<any>}` | `{"event_id":"<string>","channel":"<string>","topic":"<string>" \| null}` |
| `memory_dream` | `{"session":"<uuid>","source":"<uuid>","from_seq":<u64>,"to_seq":<u64>}` | `{"session":<uuid>,"source":<uuid>,"from_seq":<u64>,"to_seq":<u64>,"deduplicated":<bool>,"bytes_read":<u64>,"prompt_bytes":<u64>}` |
| `release_worker` | `{"worker":"<string>","reason":"<string>"?}` | `{"worker":"<string>","outcome":"released" \| "already_released","session":"<uuid>","thread_id":"<uuid>","seq":<u64>}` |

### 4.2 Semantics

- `wake_now` is advisory. `started` = no drain was in flight and one was spawned; `folded` = a
  drain was already running and the wake folded onto the in-process wake registry. A wake over an empty
  inbox is a no-op drain that exits; the session state does not change.
- `recovery_budget_reset` appends a `recovery/budget-reset` event (an explicit append — never a
  silent counter wipe) and returns its stream position. `reason` is optional and is recorded in the
  event payload.
- `mock_channel_send` requires the super-agent assembly and a configured mock channel; an unknown
  channel is `-32000` with the configured channel list in the message. The injected `event_id` is
  the subscription admit's idempotency key, so re-injecting the same id never double-admits.
- `memory_dream` (round 8, MEMORY-MODEL Phase 1) requires a `memory:` section (`-32005` otherwise).
  `session` is the operator-created memory session (`kian memory create`); `source` names the
  session whose half-open range `[from_seq, to_seq)` is consolidated. The range must satisfy
  `1 <= from_seq < to_seq <=` the source head (the source's last event seq + 1, read from the
  rebuilt stream; anything else is `-32602` — refused at admission, never a silent truncation).
  The server reads the range, refuses honestly when the rendered transcript exceeds
  `memory.dreamer.max_bytes_read` (`-32006` — never truncates), renders the deterministic dream
  prompt through `kian-memory` and admits ONE durable inbox row (the ops-plane dream envelope:
  exactly `{"content": <rendered prompt>, "dream": <trusted marker>}` — the marker carries
  source + range + scope and is set by the server, never derived from message text), under the
  idempotency key `dream:<source>:<from>:<to>`, which wakes the dreamer drain.
  The public control plane (`submit`/`steer`/`queue`) refuses this reserved envelope shape with
  `-32602` (§2.3), so only the ops plane can land a marked row — no client content can forge the
  marker (kian-94, review B-2′).
  Repeating the same range is a no-op: `deduplicated: true` — while the row is still pending via
  the inbox idempotency key, and after its `input/admitted` landed via the stream itself (the
  trusted `dream` marker carries source + range), including on a session that has since settled.
  Dreamer routing and note provenance read the marker only: a plain `submit` whose text mimics
  the `[kian-dream: …]` prompt header is ordinary content and never routes onto the dreamer. A
  terminal memory session refuses a NEW range (`-32003`); a dream settles its memory session
  (notes land, then the end write), so a new range goes on a fresh memory session. An unknown
  `session` is `-32001`; an unknown or empty `source` range is `-32001` / `-32602`.
- `release_worker` (ADR-035) closes the live thread bound to one worker id and thereby frees the
  1:1 slot. The server resolves the binding from the committed streams (the caller never supplies a
  session id) and requires all of: the owner's projection is `idle`, its last event is
  `session/ended{interrupted|failed}`, its inbox has no eligible row, no drain holds it, and the
  worker's durable queue has no `pending`/`inflight` row. It then appends one atomic
  `drain/claimed` + `session/ended{cancelled}` batch (claim epoch, one transaction) — the end
  payload carries the additive `released` object (`worker`, `thread`, `by`, `reason`) —
  and answers `released` with the end's `seq`. A thread that is already terminal answers
  `already_released` (idempotent; `seq` is the existing terminal end's). A running/recoverable
  owner, an owner still holding eligible inbox input, and a worker still owing a `pending`/
  `inflight` command row are all refused with `-32002` (the message says which; a claim lost to a
  live lease is the same retryable code); an unknown id, no live binding, or a never-started
  reservation is `-32008`. It never rebinds, reroutes or moves a command, and never touches a
  running drain.

### 4.3 Errors

`-32600`, `-32601`, `-32602`, `-32000`, `-32001`, `-32002`, `-32003`, `-32004`, `-32005`,
`-32006`, `-32008` (§2.2).

## 5. Event stream — `GET /sessions/{id}/events`

Server-sent events over `text/event-stream`. This is a bounded audit/observability view of one
session's durable stream, resumable per connection via `Last-Event-ID` (READ-MODEL §4.1).

### 5.1 Frames

| Frame | `data` | Meaning |
|---|---|---|
| `event: kian` | One event JSON envelope (EVENT-MODEL §1) | One store event, emitted in strictly increasing `seq` order |
| `event: done` | `ended` | The stream is closing; sent after the first `session/ended` event observed |
| `event: error` | The error message (plain text) | The stream is closing due to a read failure or unknown session |

- Every `event: kian` frame carries `id: <seq>` (READ-MODEL §4.1) — the value to send back as
  `Last-Event-ID` on reconnect.
- The stream is interval-polled server-side; the cadence is `sse.poll_interval_ms` (default 100 ms).
  Polling cadence is not part of the contract — clients must not derive ordering or timing from it.
  Each poll is a tail-only read of the new events (O(new events), never a full-history rebuild).
- Implementations may emit SSE comment keep-alives; clients must ignore comment lines.
- The path `id` must be a UUID; a malformed id is an HTTP 400 with a plain-text body (not a JSON-RPC
  envelope).

### 5.2 Termination and resumption

The stream closes after the first `session/ended` event, whatever its reason. Terminal ends
(`completed`, `cancelled`, `rotated`) freeze the projection. An `interrupted`/`failed`/`awaiting_input` end is
state-preserving (`running → idle`), so a resumed session appends later events; those are observed
on a **new connection**.

Resumption (READ-MODEL §4.1): a reconnect may send `Last-Event-ID: <seq>`; the server resumes at
`seq + 1` and never re-emits frames at or before the resume point on that connection. A resume id
that is ahead of the durable head, not a number, or inside an archived range is one explicit
`event: error` frame followed by close — never a silent restart or truncation. A terminal stream
replays its remaining events and closes with `done`; a connection resumed at the terminal head
emits `done` immediately.

### 5.3 Planned: read-model surface (READ-MODEL P3)

> Planned additive surface. Each item lands with the READ-MODEL phase that implements it; until
> then v0 behavior above is normative and nothing here is callable. Semantics are owned by
> [READ-MODEL.md](READ-MODEL.md); this file only fixes wire shapes. The P0 items (projection
> `version`/`since`/`unchanged`, SSE `id:` + `Last-Event-ID` resume) have landed and are normative
> in §3.1/§3.2 and §5.1/§5.2 above. The P2b item (`-32007 archived`) has landed too and is
> normative in §2.2. The P3 fan-in stream has landed and is normative in §5.4.

- ~~Planned fan-in (P3, OSS)~~ — **landed**: `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]`
  is normative in §5.4 (tail-only, explicit allowlist ≤ 64, `id: <uuid>:<seq>`, no wildcard,
  HTTP 400 on malformed/over-cap, one `event: error` per unknown listed session then continue).
- ~~Planned error code (P2b, append-only registry): `-32005 archived`~~ — **landed as
  `-32007 archived`** (§2.2): the round-8 memory surface had already taken `-32005`/`-32006`,
  and the registry is append-only, so `archived` is the next free code. Every read of an
  archived session (`projection`, the SSE stream) answers `-32007` / one `event: error` frame
  with package guidance — never `unknown_session`, never a silent truncation.
- Planned metrics (additive): the `event_stream` object (`subscribers`, `sessions_watched`,
  `events_sent`) has landed (§6); the `read` object (`unchanged`, `delta_reads`, `resync`,
  `full_scans`) remains planned.

### 5.4 Fan-in event stream — `GET /events` (READ-MODEL P3, normative)

> Change class: **additive** (§9.2) — a new path, a new metrics key, a new SDK frame variant;
> no existing surface, frame or code changed.

Server-sent events over `text/event-stream`: the explicit-allowlist multi-session tail stream
(READ-MODEL §4.2). Mounted iff `api.event_stream.enabled` (default `true`); a disabled server
mounts no route and the path 404s. Same v0 trust boundary as §2.4 — no authn/authz, the
deployment is the boundary, and the allowlist is **explicit session ids only**.

**Request.** `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]`

| Parameter | Contract |
|---|---|
| `sessions` | Required, exactly once. Comma-separated explicit session uuids — the allowlist. 1–64 entries (counted before dedup; duplicates collapse). **No wildcard** in OSS: `*` is refused by name, and a principal-scoped wildcard would be a hosted-layer additive extension |
| `kinds` | Optional, exactly once. Comma-separated event kinds (the EVENT-MODEL §3 catalog); unknown kind or empty token is malformed. Absent = every kind |
| other keys | Ignored (forward compatibility, the §2.1 posture) |
| `Last-Event-ID` | Ignored — P3 v0 is tail-only; per-session resume stays on §5.1/§5.2 |

Malformed (missing/repeated `sessions`, a non-UUID id, an empty entry, over-cap, malformed
`kinds`) is **HTTP 400** with a plain-text body (not a JSON-RPC envelope — the same posture as a
malformed path id in §5.1).

**Frames.**

| Frame | `id` | `data` | Meaning |
|---|---|---|---|
| `event: kian` | `<session-uuid>:<seq>` | One event JSON envelope (EVENT-MODEL §1) | One event of one watched session |
| `event: error` | — | The error message, naming the session | One listed session failed (unknown, archived, unreadable); it is dropped and the stream continues with the others |

- Tail-only: the connection starts at each listed session's current head (one version query per
  session) — pre-connect events are never replayed and never scanned.
- No ordering across sessions is implied or promised. Per session, `seq` is strictly increasing
  on the connection (the compound id carries the position).
- Polling uses the §5.1 cadence (`sse.poll_interval_ms`); each poll reads only the bounded
  tails — per-poll work is O(new events), never O(history).
- A watched session that ends is delivered through its `session/ended` event (subject to the
  kind filter) and then dropped — its stream is frozen. The stream closes when every listed
  session has ended or errored. There is no `done` frame; that is a per-session concept.
- Backpressure is bounded: one poll cycle emits at most a fixed batch of frames (512) before
  yielding, and a consumer that stops reading suspends the poll loop outright — the server never
  buffers unbounded data on a slow consumer's behalf.

## 6. Metrics — `GET /metrics`

Read-only JSON snapshot. HTTP 200, `application/json`.

| Key | Type | Contents |
|---|---|---|
| `engine` | object | In-memory engine counters (drain depth, drains started, state transitions, wakes…) — reset with the process |
| `pending_wake_depth` | integer | In-process pending wake registry depth |
| `cache` | object | Prefix-cache stability report derived from the durable stream; a store error surfaces as `{"error": "<message>"}` |
| `usage` | object | Provider token-usage aggregate projected from `turn/end`; the provider's own numbers, no pricing or normalization |
| `superagent` | object or `null` | Bundle counters (evidence, subscription, channel, watchdog, batching); `null` when no `superagent:` section assembled |
| `memory` | object or `null` | Counters of the memory assembly: dream counters (dreams, read/parse/append failures, notes appended/deduped) and, when `memory.session` is configured (P2), the injection counters `injected_sessions`/`injected_items`/`injected_bytes`/`injected_truncations`/`injected_skips`; `null` when no `memory:` section assembled |
| `event_stream` | object | Fan-in `GET /events` gauges (READ-MODEL §4.2, §5.4): `subscribers` and `sessions_watched` (live gauges) plus `events_sent` (monotonic counter) — process-local, reset with the process |

The seven top-level keys are stable; nested counters are additive. Consumers must ignore unknown
keys and unknown nested fields. `engine` and `pending_wake_depth` are process-local, while `cache`
and `usage` are pure projections over durable events — a restart resets the former and not the
latter. `event_stream` is process-local like `engine`.

## 7. GitHub webhook — `POST /webhooks/github`

Mounted only when at least one `type: github` channel is configured; otherwise the path 404s. This
is the only inbound surface meant for the public internet, so nothing is parsed before its bytes are
authenticated.

### 7.1 Required headers

| Header | Meaning |
|---|---|
| `X-GitHub-Event` | The event type (e.g. `issues`, `issue_comment`, `push`) |
| `X-GitHub-Delivery` | The delivery id; it becomes the channel event id (the durable dedupe key) |
| `X-Hub-Signature-256` | `sha256=<hex>` HMAC-SHA256 over the raw body |
| `Content-Type` | Must be `application/json` |

The secret comes from `KIAN_GITHUB_WEBHOOK_SECRET` (environment only) and is fail-closed at
assembly.

### 7.2 Verification order (normative)

1. Content-type gate → `415`.
2. Body-size cap (`webhook.max_body_bytes`, default 1 MiB) → `413`. Enforced before any signature
   work.
3. HMAC-SHA256 over the raw body, constant-time compare → `401` on missing/invalid signature.
4. Per-source token bucket (`webhook.rate_capacity`, default 60; `rate_refill_per_second`, default
   2) → `429`.
5. Required headers and JSON parse (now authenticated) → `400`.
6. Hand-off to the subscription pipeline → `200` or `500`.

A rejection at any step produces no event and no row, plus an audit log line. The rate limiter is
per-process and per-source (v0 honest bound: a multi-replica deployment has N buckets, one per
replica).

### 7.3 Status matrix

| Status | Meaning | Body |
|---|---|---|
| 200 | Verified delivery accepted | `{"surfaced":<count>}` |
| 400 | Missing `X-GitHub-Event` / `X-GitHub-Delivery`, or the authenticated body is not valid JSON | `{"error":{"status":400,"message":"<reason>"}}` |
| 401 | Missing or invalid `X-Hub-Signature-256` | `{"error":{"status":401,"message":"<reason>"}}` |
| 413 | Body exceeds the configured cap | HTTP-layer rejection (the JSON error envelope is not guaranteed) |
| 415 | Content type is not `application/json` | `{"error":{"status":415,"message":"<reason>"}}` |
| 429 | Rate limit exceeded | `{"error":{"status":429,"message":"<reason>"}}` |
| 500 | Verified delivery could not be handed to the subscription pipeline | `{"error":{"status":500,"message":"<reason>"}}` |

### 7.4 Success and dedupe

`{"surfaced": n}` is the number of channel events surfaced downstream. `0` is a successful, honest
drop (duplicate delivery id, unsupported/filtered event type, or a repository outside the mapping
table). Delivery-id dedupe is downstream, per `(session, event_id)` — the webhook route itself does
not keep a delivery table.

## 8. KiandProto v3 — BYOM endpoint protocol

Transport: TCP, one JSON object per line (UTF-8, no framing header). One bidirectional stream carries
the whole conversation. Since v1 there are **two dial directions** over the same framing (v3 keeps
both; v2 added the filesystem command pair, [ADR-023](decisions/ADR-023-worker-fs-tools.md), and v3
adds the optional worker capability declaration, [ADR-032](decisions/ADR-032-worker-declared-capabilities.md)):

| Path | Who dials | Handshake | Use |
|---|---|---|---|
| **worker-dial** (v3) | the worker (`kiand --register-to`) | worker → `register`; server → `welcome` or `rejected` | dynamic membership ([WORKER-MODEL](WORKER-MODEL.md) §2) |
| **server-dial** (legacy) | the server (`kian-exec`), per the `endpoints:` config | endpoint → `hello`; server → `welcome` | static pinned workers |

After a successful handshake both paths carry the same steady-state frames
(`command`/`result`, `fs_command`/`fs_result`, `ping`/`heartbeat`/`bye`) on that one connection; a
worker-dial connection *is* the command route for that worker id.

`PROTOCOL_VERSION = 3`, declared independently by both halves (`kian-exec` and `kian-endpoint`) and
pinned by their own golden tests. A wire change updates both crates' goldens and bumps
`PROTOCOL_VERSION` in both, in the same commit.

### 8.1 Handshake — worker-dial (register mode, v3)

1. The server opens a **private** registration listener (`workers.listen` in `kian.yaml`;
   loopback/RFC1918/CGNAT/IPv6-ULA; a public or wildcard bind refuses startup unless
   `workers.allow_public: true`). The listener refuses to start when a configured env-only token
   variable (the shared `workers.token_env` or any `workers.tokens` entry) is absent or blank —
   fail-closed ([WORKER-MODEL](WORKER-MODEL.md) §6; enrollment is ADR-031 and config-only).
2. Worker → server: `register` — **exactly one, and the first decodable frame** on the connection.
   No `register` within the 10 s deadline, a second `register`, or any other first frame closes the
   connection immediately with one `protocol` audit line; there is no half-registered state.
3. Server → worker: `welcome` (carries `v` and the server-directed heartbeat cadence
   `heartbeat_ms = max(1, offline_after_ms / 3)`; the worker honours the server value over its own
   `--heartbeat-ms`), **or** `rejected` and the connection closes.
4. Steady state: the server may send `ping` (answered with `heartbeat`); the worker sends
   `heartbeat` on cadence; `bye` from either side is a graceful close; the server treats transport
   loss as liveness loss, not as a command outcome.

**Auth.** The `register` token is compared **constant-time**; the token value never appears in
config, argv, logs, events, error text or `/metrics` (it travels in a redacting type). Failed auth
is bounded twice: one attempt per connection (the server closes after a refusal, never re-prompts)
and a per-peer-address window — after 5 refusals in 60 s, further connections from that address are
closed without reading a frame. An id listed in `workers.tokens` is checked only against its own
token; with `workers.enrolled_only` an unlisted id is refused. Otherwise a **shared** token admits
any id ([WORKER-MODEL](WORKER-MODEL.md) §6 records the narrowed limitation; mTLS is the planned
fix).

**Duplicate id.** A newer authenticated `register` for a live dynamic id **replaces** the incumbent:
serialized per id, the old connection is closed (server `bye` first), the route is swapped, the
connection generation increments, one `replaced` audit line; the old connection's outstanding
commands re-drive on the new route by the ordinary queue rule. A registration whose id matches a
static `endpoints[].id` is refused `duplicate_static` — the static entry is the pin.

**Identity and description shape.** `endpoint_id`: 1–200 bytes after trim, no control characters
(the worker validates the same rule at startup and refuses to boot, so a malformed id never reaches
the wire; the server re-checks — a violation answers `id_conflict`). `description` (optional):
non-empty after trim, ≤200 chars, no control characters — a violation is **dropped with one audit
line and the registration still succeeds** (the id is what matters; it never fails the handshake
and never reaches a model). `capabilities` (v3, optional): each name is lowercase
`[a-z0-9._-]`, 1–64 bytes; at most 16, unique. The worker validates its own list at startup and
refuses to boot on an invalid entry; the server re-sanitizes (trim, drop invalid/duplicate/
over-limit) **with one audit line and the registration still succeeds** — a bad declaration never
fails the handshake and never grants authority. `max_concurrent == 0` clamps to 1 with a `warn`.

### 8.2 Handshake — server-dial (legacy static endpoints, `hello` at v3)

Unchanged shape: endpoint → `hello {v, endpoint_id, max_concurrent}`; server → `welcome`. The
`hello`/`welcome` frames name the protocol revision `v`; the `register` frame names it `version` —
a locked asymmetry, frozen by the v3 goldens (a rename is a wire break).

### 8.3 Frames

Server → worker/endpoint:

| `type` | Fields |
|---|---|
| `welcome` | `v` (protocol version), `heartbeat_ms` (optional; serializes `null` when absent — no `skip_serializing_if`) |
| `command` | `id` (command UUID), `cmd` (`ExecCommand`) |
| `fs_command` (v2) | `id` (command UUID), `cmd` (`FsCommand`) |
| `ping` | — |
| `bye` | — |
| `rejected` (v2) | `reason`, `detail` (optional; omitted — not `null` — when absent) |

Endpoint/worker → server:

| `type` | Fields |
|---|---|
| `hello` | `v`, `endpoint_id`, `max_concurrent` |
| `register` (v3) | `endpoint_id`, `description` (optional; omitted when absent), `token`, `version`, `max_concurrent`, `capabilities` (optional; omitted when empty; ≤16 unique names) |
| `heartbeat` | `endpoint_id`, `ts_ms`, `inflight` |
| `result` | `id` (command UUID), plus exactly one of `result` (`ExecResult`) or `error` (`CommandError`) |
| `fs_result` (v2) | `id` (command UUID), plus exactly one of `result` (`FsResult`) or `error` (`CommandError`) |
| `bye` (v2) | — |

`ExecCommand`: `{"argv":[<string>...],"env":{<string>:<string>},"cwd":<string>|null,"timeout_ms":<u64>|null}`.

`ExecResult`: `{"exit_code":<i32>|null,"stdout":"<string>","stderr":"<string>","timed_out":<bool>,"redactions":[...]}`;
`redactions` is additive and omitted when empty.

`CommandError`: `{"code":"spawn"|"timeout"|"io","message":"<string>"}`.

`FsCommand` (v2, ADR-023): one tagged object —
`{"kind":"fs_list","path":"<string>"}`, `{"kind":"fs_read","path":"<string>"}`, or
`{"kind":"fs_write","path":"<string>","content":"<string>"}`. Paths are non-empty and never contain
`..`; the worker resolves them under its `--workspace-root` (default: its working directory) and
re-checks the rule. Read and write bodies are bounded at 1 MiB each; a larger body is a typed
`io` error, never a truncation.

`FsResult`: `{"kind":"fs_list","entries":[...]}`, `{"kind":"fs_read","content":"<string>"}` or
`{"kind":"fs_write","written":true}` — the exact local built-in tool result shapes. The server maps
a failure onto the same honest `{"error": …}` tool result the local tools produce.

**v2 reason set (closed — no fallback, no `#[serde(other)]`)**, with the worker-side reaction each
refusal demands:

| `reason` | Trigger | Worker reaction |
|---|---|---|
| `version_mismatch` | announced revision ≠ `PROTOCOL_VERSION` | **terminal**: log once with actionable text, exit non-zero; never a retry loop |
| `bad_token` | token missing/blank/mismatched (constant-time) | **terminal**: log once naming the `token_env` variable, exit non-zero |
| `duplicate_static` | id matches a static `endpoints[].id` | **terminal**: log naming the static pin |
| `id_conflict` | per-id registration slot held past the wait bound, or a malformed `endpoint_id` | retryable with bounded backoff |
| `disabled` | server not admitting members (shutdown/drain) | retryable with bounded backoff |

`rejected.detail` is generated from fixed templates that interpolate **only** the `endpoint_id` and
protocol version numbers — never a token, never a worker-supplied description.

Worker reconnect policy (register mode): bounded backoff, 500 ms ×2 up to a 30 s cap, unbounded
attempts while the process lives; transport refusal/EOF is retryable. `SIGTERM`/`SIGINT` ⇒ finish
in-flight commands, send `bye`, exit 0. Audit lines carry the id and peer address only.

### 8.4 Delivery semantics

The queue is server-side and durable; delivery is at-least-once with command-id correlation/dedupe
([ADR-007](decisions/ADR-007-kiand-delivery-semantics.md)). Since v1 (unchanged at v3) dispatch is **queue-first by
worker id**: a command enqueues durably even with no live route (a registered worker's route appears
and disappears at runtime) and parks `pending` until a route appears or the `queue_timeout` abandons
it honestly; there is no cross-worker dispatch. At-least-once re-drive is **bounded** (ADR-033):
an endpoint may see the same command id at most `1 + workers.queue_max_redrives` times, after which
the row converges to the terminal `abandoned` without a further frame — a command whose own side
effect destroys the ack path (restarting the server or the worker) can therefore be received but
never replayed forever. A re-dispatch is additionally postponed by
`workers.queue_redrive_grace_ms` when the row was dispatched more recently than the grace, and the
endpoint answers a repeated command id from its recorded result instead of executing it again
([ADR-037](decisions/ADR-037-endpoint-result-dedupe.md)) — the frames are unchanged, so a cached
answer is byte-identical to a fresh one. That semantics lives in [WORKER-MODEL](WORKER-MODEL.md) §4
and DESIGN §4.3; this section fixes only the frames.

### 8.5 Upgrade v0 → v1

**Upgrade order: kian first, workers second.** There is **no v0 compatibility shim** — a peer on the
wrong revision is refused with `version_mismatch` plus one audit line, never a half-compatible
connection ([ADR-021](decisions/ADR-021-runtime-worker-membership.md)). Window consequences, all
honest and visible:

- A v0 **static endpoint** dialed by v1 kian answers `hello{v:0}` ⇒ typed terminal
  `VersionMismatch`, one audit line, the route stays offline (never a fake connect).
- A v0 **worker** (a `kiand` with no `--register-to`) never registers; it reads offline until
  upgraded.
- A v1 worker pointed at a v0 kian gets a refused dial and keeps its bounded backoff.

### 8.6 Upgrade v1 → v2 (filesystem commands)

**Upgrade order: kian first, workers second.** v2 is additive on the wire — the `command`/`result`
frames keep their v1 bytes exactly; the only new frames are `fs_command`/`fs_result`, and the
handshake revision moves 1 → 2. There is **no compatibility shim**: a v1 worker (which cannot decode
v2) is refused `version_mismatch` and stays offline until it is upgraded; a v2 worker pointed at a
v1 kian is refused and keeps its bounded backoff. Window consequences:

- A v1 worker reads offline (typed terminal refusal at the listener) while the v2 kian runs; the
  queue keeps parking its rows — no lost work, no cross-worker dispatch.
- A v2 kian's queue migration v3 adds the `kind` column with `DEFAULT 'exec'`: every pre-v3 row
  decodes as the exec family and re-drives byte-identically after the upgrade. Fs rows exist only
  after the upgrade.
- The worker's `--workspace-root` defaults to its working directory; an explicit root must exist
  and be a directory (fail-closed at startup).

### 8.7 Upgrade v2 → v3 (worker-declared capabilities)

**Upgrade order: kian first, workers second.** v3 is additive on the wire: every v2 frame keeps its
bytes; the only change is the optional `capabilities` key on `register`, and the handshake revision
moves 2 → 3. There is **no compatibility shim**: a v2 worker is refused `version_mismatch` and stays
offline until upgraded; a v3 worker pointed at a v2 kian is refused and keeps its bounded backoff.
Window consequences:

- A v2 worker reads offline (typed terminal refusal at the listener) while the v3 kian runs; its
  parked rows stay parked — no lost work, no cross-worker dispatch.
- The registry cache migration v5 adds a nullable `capabilities` TEXT column. Pre-v5 rows read as
  an empty list; a registration that omits the key clears the cached list (the latest declaration
  is the fact). `list_workers`/`kian workers` keep serving registry-only ids; their
  `declaration_stale` flag now also covers the cached capabilities.
- Static endpoints have no capability declaration in v3 (`hello` is unchanged); a static pin's
  catalog row omits the field. This is recorded as a non-goal, not an accident.

## 9. Versioning and freeze

### 9.1 Frozen surface

- Endpoint paths and methods in §1.
- JSON-RPC method names, required param names, result field names, and the error-code registry.
- SSE frame names (`kian`, `done`, `error`) and their `data` shape.
- `/metrics` top-level keys and their meaning (nested fields additive).
- Webhook required headers and the status matrix.
- KiandProto frame `type` values and payload field names within a `PROTOCOL_VERSION`; the v3
  register (including `capabilities`)/rejected/fs frames and the closed five-reason set, pinned by
  both halves' goldens.

### 9.2 Change classes

- **Additive** (allowed in v0): a new method or path, a new optional param, a new optional result
  field, a new documented error code, a new metrics nested field, a new webhook event type.
  Additive changes ship with this file and tests updated in the same change.
- **Breaking** (requires a new design decision): removing/renaming a method, param, result field, error
  code, SSE frame, metrics key or KiandProto frame; changing a status code's meaning; changing the
  trust model; changing a KiandProto handshake or the rejection reason set. The wire is frozen
  additive-only after the `kian-sdk` freeze — the SDK's public
  surface *is* this contract. The KiandProto v0 → v1 bump (register mode, worker-dial) is the
  recorded instance: [ADR-021](decisions/ADR-021-runtime-worker-membership.md), §8.5; the v1 → v2
  bump (worker filesystem commands) is [ADR-023](decisions/ADR-023-worker-fs-tools.md), §8.6; the
  v2 → v3 bump (worker-declared capabilities) is
  [ADR-032](decisions/ADR-032-worker-declared-capabilities.md), §8.7.

### 9.3 Version numbers

This file is **v0** (the version marker in the title). `kian-sdk` is `0.1.0`; its golden fixtures
pin the current bytes. KiandProto carries its own `PROTOCOL_VERSION`. There is no JSON-RPC
`version` field and no URL versioning — additions are called out here.

## 10. Change protocol (required for every wire change)

### 10.1 Order of work

1. **Update this document first** — it is the gate; a wire change not described here is not
   approved.
2. If the change is semantic (new primitive, trust boundary, behavior), update
   `design/DESIGN.md` §5 (link, do not duplicate this table) and write an ADR when the decision is
   architectural.
3. Implement in `kian-cli` (handlers/router) and, for any client-visible method, in `kian-sdk`
   (typed surface + golden fixtures).
4. Update the method catalog here and the drift test in the same change.
5. Update `docs/SPEC.md` §2.4/§3 and the README only where they enumerate the surface.
6. Run the four gates plus `cargo test -p kian-cli --test docs_map`.

### 10.2 Required enforcement

| Contract | Enforcement |
|---|---|
| Method catalog ↔ dispatch arms | `docs_map.rs` catalog-drift test |
| Request/response bytes per client method | `kian-sdk/tests/golden_wire.rs` + `tests/fixtures/` |
| SSE frames and termination | `kian-sdk` SSE parser tests, `kian-cli/tests/e2e_serve.rs` |
| Fan-in frames, caps, filters, tail-only start | `kian-cli/tests/fan_in_e2e.rs`, `kian-sdk` SSE parser tests (`KianFanIn`) |
| Webhook verification order and status matrix | `kian-cli/tests/webhook_receiver.rs` |
| Ops error mapping | `kian-cli/tests/ops_commands.rs` |
| Iron rule (no execution symbols in handler modules) | `kian-cli/tests/iron_rule.rs` |
| KiandProto frames | `kian-exec` and `kian-endpoint` golden tests (dual, same commit) |

### 10.3 New method checklist

- [ ] Add the row to §3.1 or §4.1 and describe its semantics (or link the owner).
- [ ] Decide the error mapping; add new codes to §2.2 rather than inventing ad-hoc ones.
- [ ] If it is a client-visible method: `kian-sdk` typed wrapper + golden fixture (request and
      response bytes).
- [ ] Add/extend a test that pins behavior; update the dispatch-drift expectations.
- [ ] If the method changes who may call it, update §2.4 and the ADR trail.
- [ ] No arbitrary new HTTP endpoints: a new surface is a design decision (webhook was one,
      ADR-011).

## 11. Non-goals and the OpenAPI posture

- **No hand-written OpenAPI document.** The wire is JSON-RPC + SSE over one server, not a REST
  resource tree; describing many methods on one POST body in OpenAPI needs `oneOf`/`anyOf` and drifts
  from the semantics. This Markdown file plus the golden fixtures is the contract.
- If a machine-readable spec is ever required, **derive it** (OpenAPI 3.1 or JSON Schema) from this
  file and the fixtures as a build artifact; never let the derived artifact become the source of
  truth. The webhook and `/metrics` surfaces are the natural first candidates because they are
  single-resource HTTP surfaces.
- Out of scope in v0: authentication/authorization, multi-tenancy, quotas, a REST mapping, an
  OpenAI-compatible server surface, a `/health` endpoint, Prometheus text exposition, rate limiting
  outside the webhook, and resumable SSE subscriptions.
- The `kian` CLI is an operator surface (read-only commands open the store directly), not part of
  this API contract.
