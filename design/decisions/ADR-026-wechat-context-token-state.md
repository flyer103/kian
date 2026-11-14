# ADR-026: Best-effort durable WeChat reply tokens

- **Status**: accepted (owner gate, 2026-11-14)
- **Date**: 2026-11-14
- **Related**: `design/CHANNEL-WECHAT.md` §5/§6/§7/§11/§13 (the channel contract),
  ADR-018 (the iLink channel and its credential-file deviation), `docs/SPEC.md` §5.
- **Context**: the WeChat iLink reply path echoes the latest inbound `context_token` for a user;
  the adapter cached it in memory only. After a `kian serve` restart, a reply attempt (a worker
  completing a task that started before the restart, an outbox retry, or a Router decision) fails
  `Unavailable` until the user sends a new message. The channel is best-effort by contract, but a
  restart is a routine event and a task that completed is exactly the moment a reply matters.
  Options were: keep memory-only; persist into the event stream; persist into the credential file;
  or a small dedicated state file.
- **Decision**: persist the reply tokens in a **best-effort, TTL-bounded state file** beside the
  credential file.

  1. **A separate state file.** Default `<credentials_file>.context-state.json`; overridable via
     `wechat.context_state_file`, TTL via `wechat.context_ttl_ms` (default 1 h). Schema
     `{version:1, tokens:{user_id:{token, updated_at_unix_ms}}}`. `kian wechat login` never
     touches it.
  2. **Not the event stream.** A `context_token` is a transport capability; the event stream is
     replayable, exportable and recallable audit data. Putting the token there would leak it into
     every export and violate CHANNEL-WECHAT §5.4 (tokens never in events). Not the credential
     file either: the login flow owns that schema and rewrites it wholesale.
  3. **Best-effort, never blocking.** Admission updates the in-memory map first (the reply path
     never waits on disk); the file is written atomically (temp + rename, `0600`, parent `0700`)
     on a blocking task, and a write failure is one warn line — the in-memory token still serves
     the reply. On startup a missing/malformed/old-version/unreadable file is an **empty** state
     (one warn), never a startup failure; entries older than the TTL are dropped on load.
  4. **TTL and honesty.** A persisted token is usable only within `context_ttl` of its inbound
     message. A stale token that the server rejects still surfaces as the existing `-14` pause/
     degradation path (§11); the persistence never invents a send capability. The TTL is
     deliberately conservative (1 h): long enough to bridge a restart or a short outage, short
     enough not to keep a reply capability indefinitely. The value is configurable.
  5. **Scope.** Only admitted senders (the `topics` allowlist) are cached, as before; no new
     recipient can be reached by a persisted state file. The token is never logged, never in an
     error, never in an event, never in `/metrics`.

  Adversarial consequences checked: a crash mid-write leaves the old state file intact (rename);
  a corrupted file starts empty and the next inbound repairs it; a token for a user removed from
  the allowlist is still only a transport capability, and a send to that user is refused by the
  existing allowlist/target validation before any wire call; a stale token produces the same
  honest `Unavailable`/`-14` path; two concurrent captures for one user are last-writer-wins in
  the map and on disk; the file is not part of `session export` or recall.
- **Alternatives considered**:
  - **Memory-only (status quo)**: the restart gap; the owner asked to close it.
  - **Event stream**: leaks a transport capability into audit/export; rejected.
  - **Credential file**: mixes a whole-file login artifact with per-user runtime state; rejected.
  - **A database table**: the bundle has no store dependency for channel transport state, and a
    restart bridge does not justify a schema/migration in the event store; rejected.
- **Consequences**: one new optional file per WeChat channel, documented and `0600`; a new
  `context_ttl` knob; no event/wire/API change; the channel remains best-effort and the honest
  `Unavailable` path is unchanged when no token or an expired one exists.
