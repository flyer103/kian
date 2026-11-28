# ADR-082 — Retention preserves the replay evidence: a channel receipt ledger, and an archived resident is never re-created

- Status: accepted (2026-11-28, archived resident revived; iMessage replay after retention)
- Related: ADR-011 (inbound webhook plane), ADR-040 (runtime resident rotation),
  ADR-042 (the iMessage channel), ADR-081 (channel dedupe across session incarnations)

## Context

Export-only retention (READ-MODEL §7) deletes a terminal session's online events and inbox rows
after a verified package is durable. That is the point of the feature: the package is the audit
record and the store keeps only the working window.

Two durable facts lived exclusively in the rows it deletes, and both are read by startup paths:

1. **The channel guard's evidence.** ADR-081 made channel admission refuse a key already consumed
   in any session by reading `inputs.consumed_by_seq`. Archiving the session that consumed a guid
   deleted that row, so the guard's scope became empty again — the same failure ADR-081 removed,
   re-entering through retention.
2. **The resident's own history.** A resident re-arms by walking `uuid5` successors from the
   config-pinned id and treating a stream that projects terminal as a frozen predecessor (ADR-040).
   An archived predecessor has *no* online stream, and an empty stream is also how "never created"
   looks. After the archive, boot treated the archived pinned Router as new, bootstrapped it in
   place under the same config id, and its first drain consumed the bridge's tail replay — thirty
   already-answered messages, delivered again.

The incident hit both in one restart: the archived Router was revived, and because its own consumed
rows had been deleted, the replay was admitted. Removing the marker row by hand stopped the
bleeding for one id; the defect is that retention and the two startup paths disagreed about what
an archived session means.

## Decision

1. **The archive commit copies the consumed channel facts into a receipt ledger.** In the same
   transaction that inserts the marker and deletes the online rows,
   `INSERT INTO channel_receipts (...) SELECT ... FROM inputs WHERE session = ? AND
   consumed_by_seq IS NOT NULL AND idempotency_key LIKE 'chan:%' ON CONFLICT DO NOTHING`.
   Only consumed keys are materialized: an unconsumed row never blocks an admission (ADR-081 #2),
   so it has nothing to preserve. The row keeps the original input id, payload and audit columns so
   a suppressed admit still returns a real row, not a synthetic one.
2. **The ledger is a projection of the packages, not a second pipeline guard.** ADR-011 rejected a
   receiver-side dedupe table; this still is not one. The fact it stores already exists in the
   package (`inputs.jsonl` carries the consumed rows), and `kian archive receipts` rebuilds the
   ledger from every marker's verified package, idempotently. Deleting the ledger loses no truth —
   it costs a rebuild, exactly like a dropped projection. The guard's semantic stays "a channel
   event a drain already consumed"; the ledger only keeps that fact readable after the row retires.
3. **The Router route's guard reads both stores in one statement.** `ADMIT_CHANNEL_SQL` keeps the
   consumed-input subquery and adds `NOT EXISTS (SELECT 1 FROM channel_receipts r WHERE
   r.idempotency_key = ?)`. Passthrough subscribers and pinned `Route::Session` targets still use
   the plain per-`(session, key)` admit, unchanged (ADR-081 #3).
4. **Re-arm treats an archived predecessor as terminal.** The walk's stream port reports
   `ResidentStream::Archived` when the store holds a marker for the id, and the link is recorded
   with cause `archived`. The walk advances to the deterministic successor and the archived id is
   never bootstrapped again. The marker — not an empty stream — decides, because retention deletes
   the stream by design. The cause prompt's wording is unchanged ("predecessor session(s) ended and
   are frozen: <uuid> (archived)"): an archived session is terminal by construction (eligibility is
   terminal-only), so it did end.
5. **No new event kind.** The ledger and the marker are infrastructure (READ-MODEL §7); the event
   stream and its replay semantics are untouched.

## Consequences

- A restart after retention admits nothing already consumed, and a pinned resident whose marker says
  archived is skipped by re-arm: the first boot after archiving binds the successor instead of
  reviving the retired id.
- A store rebuilt from packages (import, disaster recovery) recovers the wire-level replay guard by
  running `kian archive receipts`; the command verifies each package first, so a corrupt package is
  reported and skipped rather than silently dropping its keys.
- The ledger grows with archived channel traffic and has no retention of its own: it is the smallest
  durable evidence for the fact the guard needs, and deleting it would require the package to be
  deleted too. Operators see it in the store size budget, not in a separate tier.
- Rollback is a dedupe regression, not corruption: a pre-v10 binary ignores the table and behaves as
  before (the replay comes back once per incarnation); the table and its rows remain for the next
  upgrade.
- The passthrough fan-out keeps admitting one row per subscriber: the ledger is consulted only on the
  Router route, and materializing a passthrough row's key changes nothing there.
- `archived_session_list` reads the marker table as empty when it does not exist, so a read-only
  reader of a pre-v5 store still works.

## Alternatives rejected

- **Keep the consumed rows online while archiving the events.** Rejected: it defeats the retention
  promise (the inbox is the second-largest table) and leaves a half-retired session whose reads
  answer `archived` while its rows still sit in the store.
- **Consult the packages per admission.** Rejected: package verification on the admit hot path, and
  a filesystem dependency inside a store transaction — the ledger exists to make the fact cheap.
- **A global unique index on `inputs.idempotency_key`.** Rejected: it would break the passthrough
  fan-out (ADR-081 #3), where several sessions legitimately hold the same key.
- **Treat every empty stream as archived in re-arm.** Rejected: a genuinely new session whose first
  event has not landed yet is indistinguishable, and refusing it would strand a legitimate bootstrap.
- **Remember retired resident ids in `kian.yaml`.** Rejected: operator-editable state loses exactly
  when an operator re-pins the id, which is the recovery move this fix must survive.
