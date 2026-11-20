# ADR-046: Capture the harness's exact model request at a loopback proxy

- **Status**: accepted (owner gate, 2026-11-20).
- **Context**: Claude Code and Codex bridges are projection-only: kian observes the harness's
  inputs/outputs but not the assembled request, so those sessions claim no `step/request`, no
  cache accounting and no byte-request replay ([HARNESS-BRIDGE](../HARNESS-BRIDGE.md) §2). The
  live probes show the gap is closable without a harness plugin: **Claude Code honors
  `ANTHROPIC_BASE_URL`** (with an API key) and **Codex honors a generated provider config**
  (`CODEX_HOME`/`-c model_providers.…`), both speaking plain HTTP/JSON. A loopback proxy can
  therefore read the exact request body before forwarding it, and kian can persist it as
  `step/request` — turning a projection-only session into an `exact` one.
- **Decision**:
  1. **One loopback capture listener per server**, started when any bridge config enables
     `capture:`. It binds `127.0.0.1:0`, serves one opaque per-session token path, and forwards
     only to the configured upstream (no open proxy). The token exists only in the child's
     environment/config and is never logged.
  2. **The adapter points the harness at the proxy**: Claude via `ANTHROPIC_BASE_URL`, Codex via
     `-c model_provider=kian-capture -c model_providers.kian-capture.base_url=… -c
     model_providers.kian-capture.wire_api=responses` (preserving the user's `CODEX_HOME`/auth).
     Only API-key/base-url provider routes are in scope; OAuth-only routes are out of scope until
     verified.
  3. **Record before forward, fail closed.** The proxy buffers the exact body (bounded by
     `max_body_bytes`, default 8 MiB), enqueues it to the session's capture queue, then forwards
     and streams the upstream response back verbatim (status, headers, chunks). If the body
     exceeds the cap or the queue is gone, it answers a fail-closed `413`/`502` **without forwarding** (A2: an
     unrecorded model-visible input must not be sent). Auth headers are forwarded but never
     persisted or logged.
  4. **The bridge driver appends `step/request`.** At the top of every loop it drains the
     session's capture queue, parses each body as JSON (the APIs are JSON; a non-JSON body is a
     contract error), and appends `step/request{slot,provider,model,iteration,body}` through the
     fenced ctx. The queue is fed before forwarding, and the harness cannot emit its assistant
     event before the response returns, so the request event always precedes the response event;
     a drain-before-event rule makes the ordering explicit rather than timing-dependent.
  5. **Retries are attempts, not dropped sends.** Both harnesses retry on 5xx (observed: Claude
     3 attempts, Codex 5 reconnects). Every forwarded request is captured and appended; a turn may
     therefore carry several `step/request` events. That is the honest record of what was sent;
     no attempt is deduplicated away.
  6. **Class upgrade.** With `capture:` on, `session/created.bridge.request_bytes` is `"exact"`;
     cache accounting and byte-request replay become available for that session. Without it the
     class and every byte stay as today.
- **Alternatives considered**:
  1. **A harness-side plugin/peer that emits request bytes** — the dsh shape; no such surface
     exists for Claude/Codex, and the ecosystem peer was the dsh live leg's adoption friction.
  2. **TLS interception** — rejected: no need when the harness accepts an HTTP base URL; a MITM
     CA would be a far larger trust and deployment burden.
  3. **Record at the provider SDK inside the harness** — not available; the CLI owns the client.
  4. **Post-hoc request logging** — rejected by A2 and ADR-010 alike.
- **Consequences**:
  - the class claim becomes configurable and honest: exact with capture, projection-only without;
  - the proxy is a new security-sensitive component: loopback only, per-session token, pinned
    upstream, bounded bodies, fail-closed recording, auth never persisted;
  - the `step/request` stream for a bridged session now mirrors real attempts, including retries
    and any subagent/background calls that share the provider route; attributing those to the
    enclosing turn is a documented approximation (the harness does not label them);
  - event ordering and replay: `step/request` is the exact body, so the audit package carries
    what the model saw; the bridged session still does not *derive* its next request from the
    stream (the harness owns assembly);
  - a capture failure is a hard failure of that turn (502), never a silent unrecorded send.
- **Rollback Trigger**: a harness route that cannot be pointed at a loopback origin without
  breaking auth (OAuth-only), or a request body that cannot be bound/persisted faithfully — then
  that harness stays projection-only and the capability matrix says so.
- **Cost Impact**: one loopback listener (axum, already a dependency), one capture port on the
  bridge driver, config grammar, tests; no new event kinds, no wire change for existing surfaces.
