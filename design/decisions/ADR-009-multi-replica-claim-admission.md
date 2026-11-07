# ADR-009: Multi-replica claim admission — conditional claim, unique holders

- **Status**: accepted ( landed the conditional claim + unique-holder rule on
  sqlite; completed it — wake transport, exec queue row leases, and the 
  two-replica kill-9 e2e — all merged)
- **Date**: 2026-11-01
- **Context**: ADR-005 makes claims safe (epoch fencing) but the legacy `CLAIM_SQL`
  claims *unconditionally*: any process can bump the epoch of a fresh lease. Single-machine
  that is invisible — one holder, and re-claims are the same process. With two replicas every
  remote wake would steal the epoch from a live drain: a ping-pong livelock. Fencing keeps it
  *safe* (the displaced drain's writes are rejected), but safety-with-livelock is not the
  semantics multi-replica needs. plan,.
- **Decision**:
  1. **Conditional claim admission.** A claim lands only when (a) no claims row exists,
     (b) the row's lease is expired (`now - ttl > last_heartbeat`, the existing EXPIRED
     predicate — the takeover path already exercises), or (c) `holder == self` (the same
     process re-claiming for its next drain episode). SQLite shape:
     `INSERT … ON CONFLICT(session) DO UPDATE SET … WHERE <expired OR holder =?> RETURNING
     epoch`; zero rows returned → the store reads the row and returns the typed
     `InboxError::ClaimRejected { session, holder, epoch, expires_at_ms }`. Nothing was
     written, so the epoch floor is unchanged. Same-holder claims still bump the epoch —
     epoch monotonicity and the `max(claims.epoch, events floor) + 1` issuance arithmetic are
     untouched, so every single-replica behavior (including back-to-back drains) is
     byte-identical.
  2. **Unique holders under postgres.** Condition (c) is only as strong as holder identity:
     two replicas sharing a holder string would defeat it. Postgres mode therefore
     *requires* an explicit unique `drain.holder` and refuses to start otherwise
     (fail-closed). Sqlite keeps the defaulted holder — single-machine default unchanged, no
     event-byte change.
  3. **`ClaimRejected` is an honest no-op drain exit.** The engine appends nothing and exits
     the drain without re-taking the folded wake — never a retry loop (constraint 8). The
     live holder's sweep or the wake transport services the input.
  4. **Wake transport = advisory accelerator.** LISTEN/NOTIFY accelerates wakes only;
     wakes stay losable by invariant — the startup rebuild and the sweep ticker bound any
     missed-notify window. Correctness never depends on the transport.
  5. **Exec queue row leases** ( extends this ADR): `exec_queue`/`async_jobs` rows are
     claimed by a single-statement lease (`claimed_by`/`claimed_until_ms`), heartbeated by
     the owner, re-drivable by any replica after expiry — the same lease-is-liveness
     discipline; the command-id correlation (ADR-007) remains the safety net.
  - **Safety boundary restated** ( rule 10): this conditions *claiming*, never write
    acceptance. Lease state never grants permission to accept a write; that stays with epoch
    fencing (ADR-005).
- **Alternatives considered**:
  1. Keep unconditional claims and rely on fencing alone — safe but livelocking between
     replicas; livelock is an availability defect the hosted layer cannot ship.
  2. A distributed lock service for drain ownership — new infrastructure dependency, and it
     still needs fencing tokens (Kleppmann: locks don't fence); the claims row already is the
     lease record.
  3. Leader election per session — heavier than needed; conditional claim on the existing
     claims table is one statement and backend-portable.
- **Consequences**: the claim statement gains a WHERE clause (negligible cost); a rejected
  claim is a typed error to the caller, not a session event (no `lease/rejected` kind —
  claims/leases are pre-event durable state, DESIGN §2 additions-only contract); takeover
  latency after a crash is bounded by lease TTL + sweep exactly as before ( semantics
  unchanged on the expired path).
- **Rollback Trigger**: None — reverting reintroduces the multi-replica epoch ping-pong.
- **Cost Impact**: Zero new infrastructure; one conditional clause on sqlite (and its
  postgres mirror in).

## Completion 

All three planned completions landed, on both backends:

1. **Wake transport (decision 4, realized).** `EventStore::notify_wake` publishes
   `pg_notify('kian_wake', '<session-uuid>')` as its own statement, called by the
   engine's admit path strictly *after* the single-statement input INSERT
   committed — the named ordering test proves a received notification implies a
   visible row. Each `kian serve` replica relays through a `PgListener` on one
   dedicated pooled connection (`listen_for_wakes`), firing its local engine
   `wake` per notification; listener gaps log, back off and re-listen. sqlite is
   a no-op (in-process registry only, semantics untouched). The named
   listener-gap test proves a dropped notify is serviced within one sweep period
   (manual clock, no sleep-races).
2. **Exec queue row leases (decision 5, realized).** `exec_queue`/`async_jobs`
   rows carry `claimed_by`/`claimed_until_ms` (the v2 generation) claimed by
   a single-statement lease (`UPDATE … WHERE … status IN ('pending','inflight')
   AND (unclaimed OR expired OR same-holder) RETURNING`), heartbeated every
   ttl/3 by the owner's drive loop (the exec driver and the async observer),
   re-drivable by any replica after expiry. The claim precedes any row mutation,
   so a foreign-leased row is left untouched (named drive-loop test). Terminal
   rows freeze. The `IN ('pending','inflight')` widening over the earlier
   `status='pending'` sketch is deliberate: `inflight` rows are the at-least-once
   re-drive set (ADR-007) — a fresh foreign lease on one must block exactly the
   same way. Same-holder claims always land (the condition-(c) discipline),
   so single-process behavior is unchanged. The lease is liveness only; the
   command-id correlation remains the safety net.
3. ** demonstrated.** The env-gated two-replica e2e (`KIAN_PG_URL`): replica
   A drains a submitted task, is `kill -9`'d mid-provider-turn, B's sweep takes
   over the expired lease via the conditional claim, replays and completes —
   exactly one end write, the input consumed once, and a stale-epoch probe write
   with A's frozen epoch (issued directly against the store) is rejected
   (`StaleEpoch`): NoSplitBrainWrite at multi-process scale.
