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
| -32002 | session_busy | An ops action requires an idle session; a drain holds it | ops |
| -32003 | session_terminal | The session is terminal (completed/cancelled/archived) | ops |
| -32004 | session_idle | `recovery_budget_reset` on a session whose recovery budget was never consumed | ops |
| -32005 | memory_not_configured | `memory_dream` on a server without a `memory:` section — a closed refusal, never a silent no-op | ops |
| -32006 | dream_range_too_large | `memory_dream` over a source range whose transcript exceeds `memory.dreamer.max_bytes_read` — refused, not truncated | ops |

Codes are append-only: existing codes never change meaning or get reused; a new failure class gets a
new documented code.

### 2.3 Values

| Field | Contract |
|---|---|
| `session` | UUID string (hyphenated, 36 chars) |
| `content` | Any JSON value except a reserved row-envelope key set — an object whose keys are exactly `{content,dream}`, `{content,spill}` or `{synthetic,content}` is `-32602`: those shapes are admitted only by the server's internal planes (§4.2), never by public input. Typical use is a string or a structured message. Input semantics (synthetic markers, Inline/Spill shaping, provenance wrappers) are owned by DESIGN/EVENT-MODEL, not by this wire |
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
- `projection` reads the stream and returns the pure projection state plus `version` — the last
  committed seq (`head.next_seq - 1`, READ-MODEL §3). The optional `since` (u64) carries the
  caller's last version: `since == version` returns `{"version":V,"unchanged":true}` with **no**
  state (the read costs one version check, no stream scan); any older value returns the full
  `{"version":V,"state":…}`. `since > version` is `-32602` (the client is ahead or pointed at
  another store — an explicit refusal, never a silent resync); a non-u64 `since` (including
  explicit `null`) is likewise `-32602`. A session with no events is `-32001` either way. The
  payload is always derived from one committed prefix (READ-MODEL R2: a concurrent append may
  make an answer stale, never mixed).

### 3.3 Errors

`-32600`, `-32601`, `-32602`, `-32000`, `-32001` (§2.2).

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

### 4.3 Errors

`-32600`, `-32601`, `-32602`, `-32000`, `-32001`, `-32002`, `-32003`, `-32004`, `-32005`,
`-32006` (§2.2).

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
(`completed`, `cancelled`) freeze the projection. An `interrupted`/`failed` end is state-preserving
(`running → idle`), so a resumed session appends later events; those are observed on a **new
connection**.

Resumption (READ-MODEL §4.1): a reconnect may send `Last-Event-ID: <seq>`; the server resumes at
`seq + 1` and never re-emits frames at or before the resume point on that connection. A resume id
that is ahead of the durable head, not a number, or inside an archived range is one explicit
`event: error` frame followed by close — never a silent restart or truncation. A terminal stream
replays its remaining events and closes with `done`; a connection resumed at the terminal head
emits `done` immediately.

### 5.3 Planned: read-model surface (READ-MODEL P2b/P3)

> Planned additive surface. Each item lands with the READ-MODEL phase that implements it; until
> then v0 behavior above is normative and nothing here is callable. Semantics are owned by
> [READ-MODEL.md](READ-MODEL.md); this file only fixes wire shapes. The P0 items (projection
> `version`/`since`/`unchanged`, SSE `id:` + `Last-Event-ID` resume) have landed and are normative
> in §3.1/§3.2 and §5.1/§5.2 above.

- Planned fan-in (P3, OSS): `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]`, tail-only (no
  resume), explicit allowlist of at most 64 sessions, `id: <uuid>:<seq>`, no wildcard, HTTP 400
  on malformed/over-cap; an unknown listed session emits one `event: error` naming it and the
  stream continues.
- Planned error code (P2b, append-only registry): `-32005 archived` — the session's online range
  has been archived; use the archive package / `session import`.
- Planned metrics (additive): `read` object (`unchanged`, `delta_reads`, `resync`, `full_scans`)
  and `event_stream` object (`subscribers`, `sessions_watched`, `events_sent`).

## 6. Metrics — `GET /metrics`

Read-only JSON snapshot. HTTP 200, `application/json`.

| Key | Type | Contents |
|---|---|---|
| `engine` | object | In-memory engine counters (drain depth, drains started, state transitions, wakes…) — reset with the process |
| `pending_wake_depth` | integer | In-process pending wake registry depth |
| `cache` | object | Prefix-cache stability report derived from the durable stream; a store error surfaces as `{"error": "<message>"}` |
| `usage` | object | Provider token-usage aggregate projected from `turn/end`; the provider's own numbers, no pricing or normalization |
| `superagent` | object or `null` | Bundle counters (evidence, subscription, channel, watchdog, batching); `null` when no `superagent:` section assembled |
| `memory` | object or `null` | Dream counters of the memory assembly (dreams, read/parse/append failures, notes appended/deduped); `null` when no `memory:` section assembled |

The six top-level keys are stable; nested counters are additive. Consumers must ignore unknown
keys and unknown nested fields. `engine` and `pending_wake_depth` are process-local, while `cache`
and `usage` are pure projections over durable events — a restart resets the former and not the
latter.

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

## 8. KiandProto v0 — BYOM endpoint protocol

Transport: TCP, one JSON object per line (UTF-8, no framing header). The endpoint (`kiand`) listens;
the server (`kian-exec`) dials it per the `endpoints:` configuration. One bidirectional stream
carries the whole conversation.

`PROTOCOL_VERSION = 0`, declared independently by both halves (`kian-exec` and `kian-endpoint`) and
pinned by their own golden tests. A wire change updates both crates' goldens and bumps
`PROTOCOL_VERSION` in both, in the same commit.

### 8.1 Handshake

1. Endpoint → server: `hello`.
2. Server → endpoint: `welcome` (carries the negotiated heartbeat cadence when present).
3. The server may send `ping` at any time; the endpoint answers with a `heartbeat`.
4. `bye` is a graceful close; the server treats transport loss as liveness loss, not as a command
   outcome.

### 8.2 Frames

Server → endpoint:

| `type` | Fields |
|---|---|
| `welcome` | `v` (protocol version), `heartbeat_ms` (optional) |
| `command` | `id` (command UUID), `cmd` (`ExecCommand`) |
| `ping` | — |
| `bye` | — |

Endpoint → server:

| `type` | Fields |
|---|---|
| `hello` | `v`, `endpoint_id`, `max_concurrent` |
| `heartbeat` | `endpoint_id`, `ts_ms`, `inflight` |
| `result` | `id` (command UUID), plus exactly one of `result` (`ExecResult`) or `error` (`CommandError`) |

`ExecCommand`: `{"argv":[<string>...],"env":{<string>:<string>},"cwd":<string>|null,"timeout_ms":<u64>|null}`.

`ExecResult`: `{"exit_code":<i32>|null,"stdout":"<string>","stderr":"<string>","timed_out":<bool>,"redactions":[...]}`;
`redactions` is additive and omitted when empty.

`CommandError`: `{"code":"spawn"|"timeout"|"io","message":"<string>"}`.

### 8.3 Delivery semantics

The queue is server-side and durable; delivery is at-least-once with command-id correlation/dedupe
([ADR-007](decisions/ADR-007-kiand-delivery-semantics.md)). That semantics lives in DESIGN §4.3;
this section fixes only the frames.

## 9. Versioning and freeze

### 9.1 Frozen surface

- Endpoint paths and methods in §1.
- JSON-RPC method names, required param names, result field names, and the error-code registry.
- SSE frame names (`kian`, `done`, `error`) and their `data` shape.
- `/metrics` top-level keys and their meaning (nested fields additive).
- Webhook required headers and the status matrix.
- KiandProto frame `type` values and payload field names within a `PROTOCOL_VERSION`.

### 9.2 Change classes

- **Additive** (allowed in v0): a new method or path, a new optional param, a new optional result
  field, a new documented error code, a new metrics nested field, a new webhook event type.
  Additive changes ship with this file and tests updated in the same change.
- **Breaking** (requires a new design decision): removing/renaming a method, param, result field, error
  code, SSE frame, metrics key or KiandProto frame; changing a status code's meaning; changing the
  trust model. The wire is frozen additive-only after the `kian-sdk` freeze — the SDK's public
  surface *is* this contract.

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
