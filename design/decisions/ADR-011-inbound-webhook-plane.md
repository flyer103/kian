# ADR-011: The inbound webhook plane — a fourth server surface, verify-then-parse

- **Status**: accepted ( lands `POST /webhooks/github`; 's `GitHubChannel` is its consumer)
- **Date**: 2026-11-01
- **Context**: parked the GitHub adapter for exactly one reason: it requires a *new
  server-surface class* — an inbound HTTP surface. The existing three surfaces ( control plane,
   ops plane, metrics) all speak for an authenticated client or an operator; a webhook
  speaks for the internet. Every byte of a delivery is attacker-influenced until cryptography says
  otherwise, so the surface needs its own discipline, not a bolt-on to `ControlPlane` (whose wire
  surface is frozen and whose trait is inbox-write + projection-read shaped — not
  verify-shaped). plan,.
- **Decision**:
  1. **A fourth iron-rule-grepped surface.** `POST /webhooks/github`, mounted on the serve router
     beside the ops plane, **only when the route is configured** (a `type: github` channel exists;
     otherwise the path 404s), outside the `ControlPlane` module, behind its own trait-isolated
     `WebhookPlane` (`ingest_verified_github`) whose sole capability is "verify, then hand a
     verified delivery to the subscription pipeline" — an inbox write, the same control-plane
     legality as the subscription service. The resident iron-rule test greps the module with the
     same forbidden-token table: no execution or store-write symbol is nameable in it.
  2. **Verify-then-parse, in a fixed order.** (1) size cap (router-level `DefaultBodyLimit` — an
     oversized body is a 413 *before any signature work*) and content-type gate (415);
     (2) HMAC-SHA256 over the raw body against `X-Hub-Signature-256`, constant-time compare
     (`verify_slice`/`CtEq`); (3) per-source token-bucket rate limit (429); (4) only then parse
     and hand off. Nothing is parsed before its bytes are cryptographically authenticated — the
      discipline applied to the transport itself, not just the payload.
  3. **Untrusted-by-default, end to end.** The channel is untrusted by config construction (no
     opt-out, the lark discipline), and the receiver treats the request itself as hostile until
     the signature holds.
  4. **Secrets are env-only and fail-closed.** `KIAN_GITHUB_WEBHOOK_SECRET`; configuring the
     route without it (absent or blank) refuses to start — never a signature-less receiver, never
     a secret in `kian.yaml`. The send token (`KIAN_GITHUB_TOKEN`, the rule) is separate:
     receive-only channels need no token.
  5. **Dedupe is the pipeline's.** The delivery id is the channel event id; the durable guard is
     the subscription service's existing `(session, event_id)` idempotency (the adapter's bounded
     in-memory ring is only a re-surface backstop). The receiver deliberately keeps **no** dedupe
     table of its own — a second dedupe store would duplicate the pipeline's idempotency and add
     authoritative state outside the event log (constraint 1). A webhook is a channel event; its
     durable trace is the admitted pair, exactly like lark (`webhook/received` was considered and
     rejected in the §2 contract).
  6. **Honest rejections.** Every rejection is a 4xx with a body and a `kian::webhook` audit
     line; no event, no row, no parse. Internal hand-off failures are 5xx with an audit line —
     never mapped onto a success.
  7. **v1 honest bounds, documented not hidden.** The rate limiter is per-process in-memory
     (a multi-replica deployment gets N independent buckets — an availability bound, not a
     correctness claim; safety is the HMAC + the pipeline idempotency). Buckets key on the
     connection's peer IP when the serve layer supplies connect info; the map is bounded with
     idle eviction. The body cap defaults to 1 MiB, the bucket to 60 tokens refilled 2/s —
     all `kian.yaml`-tunable, zero values refused.
- **Alternatives considered**:
  1. Mount inside `ControlPlane` — freezes the wrong surface, breaks trait isolation, and the 
     wire is frozen (the SDK freeze compounds this).
  2. Verify after parse — parses attacker bytes before authentication; the exact inversion of the
      discipline (parse only what cryptographically authenticated).
  3. Shared secret in `kian.yaml` — violates the secrets discipline.
  4. A persistent receiver-side dedupe table — duplicates the pipeline's `(session, event_id)`
     idempotency and stores authoritative state outside the event log.
  5. Rejecting per-IP at the edge only (reverse proxy) — the honest bound belongs to the process;
     the proxy may not exist in self-hosted deployments.
- **Consequences**: one more handler module under the resident iron-rule grep; a new workspace
  dependency `hmac` (RustCrypto, beside the existing `sha2` — pinned 0.12 so the digest traits
  line up); `serve` supplies peer connect info for per-source buckets (additive; every other
  surface ignores it); rejections cost one log line and no I/O.
- **Rollback Trigger**: None — removing the surface removes the only inbound integration path the
   "group-chat message + CI events" family has.
- **Cost Impact**: O(body) HMAC per delivery (SHA-256, ~1 µs/KiB); one mutex-guarded bucket map
  bounded at 8192 sources; no new infrastructure.
