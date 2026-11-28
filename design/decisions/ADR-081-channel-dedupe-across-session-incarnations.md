# ADR-081 — Channel admission is deduplicated by a channel-scoped key across session incarnations

- Status: accepted (2026-11-28, channel replay after restart)
- Related: ADR-011 (inbound webhook plane), ADR-040 (runtime resident rotation), ADR-042 (the
  iMessage channel), ADR-048 (multi-replica resident rotation)

## Context

A kian serve restart replayed the iMessage bridge's bounded tail (`since_rowid: 0` →
`watch.catchup_rows`, 200 rows) and the Router answered seventy already-answered messages again.
The rows were not new: every one already had a consumed inbox row in an earlier session.

Two properties of the durable guard made that possible. First, `inputs` was unique on
`(session, idempotency_key)`, so the guard's scope was the *session*, not the channel event. Second,
the Router's resident id is not stable: rotation ends the session and re-arms a derived successor
(ADR-040/ADR-048), and the operator can re-pin `superagent.router.session_id` in `kian.yaml` at any
time. Each incarnation therefore had an empty dedupe scope, and a replay after a restart or a
rotation admitted the same guid once per incarnation — seven rows for the oldest guids in the
incident, each one answered in its own session.

§9 of `design/CHANNEL-IMESSAGE.md` had claimed "exactly one admission per `guid` (idempotent replay),
never two rows". That claim held only while the resident session id held still, which it does not.

A persistent receiver-side dedupe table was already rejected in ADR-011 as a second pipeline guard.
This ADR does not add one: the guard stays the inbox row, which is the row the drain consumes.

## Decision

1. **The idempotency key is channel-scoped.** A channel event's key is
   `chan:<channel>:<event_id>` (`crate::intake::channel_idempotency_key`), not the bare event id.
   The channel prefix keeps two channels that reuse an id from colliding, and it makes the key's
   provenance readable in the store.
2. **"Already processed" means consumed, not admitted.** The cross-session guard refuses an insert
   only when a row with that key has `consumed_by_seq IS NOT NULL` in *any* session. A row that was
   admitted but never consumed (a pending row on a frozen predecessor, an interrupted drain) never
   blocks, so a replay still rescues it into the live session. Blocking on admission would strand
   work permanently.
3. **The cross-session guard applies to the Router route only.** `Route::Router` (and the
   passthrough fallbacks that release to the Router) admit under the channel guard. Passthrough
   subscribers and pinned `Route::Session` targets keep the per-session guard: a fan-out admits one
   row per subscriber, and a subscriber that is consumed before its sibling's insert would otherwise
   swallow the sibling's row.
4. **The write stays one statement.** The guarded insert is
   `INSERT ... SELECT ... WHERE NOT EXISTS (SELECT 1 FROM inputs p WHERE p.idempotency_key = ? AND
   p.consumed_by_seq IS NOT NULL) ON CONFLICT (session, idempotency_key) DO NOTHING`, with a new
   `inputs(idempotency_key)` index behind the subquery. No check-then-insert window, no new table.
5. **Migration v9 rewrites the stored keys and adds the index.** Rows whose payload carries
   `kian.type = "channel_event"` (either the under-cap wrapper or the over-cap `content.kian`
   shape) get the `chan:<channel>:` prefix; a row whose new key already exists in its session is left
   alone rather than failing the migration. Without the rewrite, the first restart after the upgrade
   would replay the whole tail once more.
6. **The receipt still says `deduplicated`.** A suppressed channel insert returns the row that
   suppressed it (its session may be another incarnation). The audit code and the assembly counters
   are unchanged; the zero-row path reads this session's row first, then the consumed row elsewhere.

## Consequences

- A bridge tail replay after any restart, rotation or pinned-id change admits nothing already
  consumed. The Router answers each channel event once, in the incarnation that first consumed it.
- A channel event whose row was admitted but never consumed is still re-admitted on the next replay
  — the rescue path for a frozen predecessor survives.
- The key format is an operator-visible durable value. Renaming a channel, or changing an adapter's
  event-id format, re-delivers once (the old keys no longer match); this is bounded by the bridge's
  catchup window and recorded as a degradation, not a break.
- Rollback is a dedupe regression, not a corruption: an older binary inserts old-format keys beside
  the new ones and replays once, exactly as before this ADR.
- The confirmation-answer path (`answer_attempt`) still bypasses admission; a replayed answer is
  matched against a pending call and dropped when none is live. That is unchanged and out of scope.

## Alternatives rejected

- **Session lineage as the dedupe scope** (walk `uuid5` successors from the configured root).
  Rejected because the configured id is operator-editable: re-pinning `router.session_id` starts a
  chain the guard does not know, and the guard would silently stop working at the exact moment the
  operator was trying to recover. Channel scope has no such coupling.
- **A persistent channel-dedupe table.** Rejected in ADR-011 and still rejected: the inbox row is
  already the durable record of the event, and a second table would need its own retention,
  fencing and rebuild rules.
- **A unique index on `(key)` alone (no consumed predicate).** Rejected: it would make an
  unconsumed row block forever, turning a transient freeze into permanent lost work.
- **Only bounding the bridge's catchup window by age.** Rejected as sufficient: the window still
  replays the last rows after every restart, and the user's requirement is that processed events do
  not come back at all.
