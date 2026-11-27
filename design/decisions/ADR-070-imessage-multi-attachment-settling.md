# ADR-070 — A multi-attachment row settles before it is admitted

- Status: accepted (2026-11-27, channel surface slice)
- Amends: ADR-064 (the attachment-bytes path), CHANNEL-IMESSAGE §3.2/§4/§9

## Context

One iMessage row can carry several attachments. The Mac writes the attachment join rows into
`chat.db` one at a time, after the message row itself, and the bridge maps whatever it can join at
the moment it scans. So a page read while the phone is still copying a burst carries a *prefix* of
the row's attachments:

- The row's text already renders one marker per attachment placeholder (`[image]` for an
  `image/*` item, `[attachment]` otherwise), so the mismatch is visible to the caller: three
  markers, one delivered attachment.
- The adapter used to advance its read position past the row as soon as the page was processed and
  admit the row immediately. When the row was scanned again later (a replay, a restart, a dropped
  cursor) the admission was already durable: `event_id` is the Apple GUID, so the later, complete
  read was absorbed by guid dedupe as a duplicate. **The remaining attachments were lost forever.**

A production row demonstrated it: seven HEIC photos arrived as one admitted input with one photo,
and the model itself remarked on the six `[attachment]` markers it could not resolve.

A second, independent bound compounded the loss: the adapter fetched at most four media items per
row and truncated the rest **silently** — no audit line, no counter, nothing an operator could grep
for.

## Decision

1. **Whole-row admission.** A row whose text renders more attachment placeholders than the
   attachments it arrived with is *unsettled* and is not admitted. The adapter parks its in-memory
   read position immediately before that row (`rowid - 1`) and re-reads the page instead of
   advancing to the page's `next_rowid`, so the next page carries the row together with whatever
   `chat.db` has committed meanwhile. Re-reading is free: nothing about the row was admitted, and
   an admission that did happen is idempotent by `guid` (CHANNEL-IMESSAGE §9).
2. **The hold is bounded.** A row that stays unsettled for `ROW_SETTLE_WINDOW` (five seconds of
   polls, `crates/kian-superagent/src/imessage.rs`) is admitted with what arrived and one audit line
   naming the placeholder count, the attachment count and the window. A deleted attachment row must
   not stall the whole channel behind one message.
3. **One audit line per wait, one on expiry.** Entering the wait writes
   `imessage audit: inbound row is still settling…`; expiry writes
   `imessage audit: the settle window elapsed…`. Both carry `channel`, `rowid`, `guid`,
   `placeholders`, `attachments` and `window_ms`. No counter and no metric key is added: the
   health surface is a contract (API-CONTRACT §metrics) and this is an adapter-local wait.
4. **The per-row media cap is eight, and truncation is audited.** Eight covers a normal burst
   (`MAX_MEDIA_ITEMS_PER_ROW`, ADR-064's four was a guess). A row carrying more media items than
   the cap is still admitted, and one audit line names `media_items`, `limit`, `rowid` and
   `guid`: the items the adapter did not fetch stay on the Mac, and the operator can see it.
5. **The settle rule does not depend on `attachments: metadata | bytes`.** A metadata-only payload
   loses the same join rows (its `attachments` array is short), and the marker count is identical in
   both modes, so the wait applies in both. Only the `bytes` mode additionally fetches.
6. **Nothing durable is added.** The hold is process state — the adapter's cursor is already
   in-memory only, and kian persists no position table (constraint 1). A restart during a hold
   re-reads the row through the bridge's bounded tail replay, exactly like any other restart. No new
   event kind, no new crate, no seam change.
7. **The completeness signal is the marker count.** The bridge renders every attachment placeholder
   as a marker, so `markers > attachments` is the only evidence of a mid-commit row that exists
   without a second database read. The heuristic can be fooled by a user typing `[image]` literally;
   the cost is that bounded five-second hold, never a lost or duplicated row.

## Alternatives considered

- **Wait in the bridge.** The bridge is the component that talks to `chat.db`, but it is a separate
  binary installed on the Messages Mac, and replacing it revokes its Full Disk Access grant. It also
  cannot distinguish "this row is complete" from "this row has no more joins coming" without a
  timeout of its own, so the wait has to live somewhere; the adapter already owns the read position.
- **Admit, then patch the input when the rest arrives.** A second admission for the same `guid` is
  absorbed by idempotency by design, and a mutation of an admitted input would break the
  persist-what-you-send invariant (the model may already have read the first version).
- **Hold the read position without a bound.** One deleted or never-synced attachment would stop the
  channel forever; a best-effort channel must fail per row, not globally.
- **Rely on the bridge's edit/update notifications.** The bridge drops a repeated `guid` (it is the
  same message edited, not a new one), so a later complete read never becomes a second row.
- **Raise the cap without a settle rule.** It only makes a *complete* row bigger; it does not fix a
  row read mid-commit.

## Consequences

- A burst that arrives as one row now reaches the model as one input with every attachment (up to
  eight), instead of an unpredictable prefix.
- The channel's read position can lag by up to five seconds behind the bridge: head-of-line for that
  channel, never for a turn, because nothing is admitted while the row is held.
- A row whose attachment never lands is admitted partial after five seconds, with the same partial
  evidence the model would have had before — plus an audit line that says so.
- A multi-item row over the cap loses the excess items; the audit line replaces the silence.

## Limits recorded with this phase

- No per-row byte budget is added: ADR-064's per-attachment bound (16 MiB) and the media edge's own
  bounds still apply, and the vision budget (ADR-066) bounds what a turn renders.
- The settle window is a constant, not a config key: it is a heuristic latency bound, not a
  deployment policy.
- The cap is per row, not per turn; a burst spread over several messages is bounded by ADR-066.

## Acceptance

- L1: marker surplus detection (complete row, partially delivered row, no text, no markers) and the
  bounded hold (holds, keeps holding inside the window, admits at the window, restarts for a new row).
- L2: a real `kian serve` against a scripted bridge that delivers three placeholders with one
  attachment, then the complete row: exactly one admission, carrying all three attachments, and the
  second `/v1/updates` request parked at `rowid - 1`.
- L3: a real Mac burst (seven photos in one message) admits one input with every attachment the DB
  committed, with the truncation audit line when the row exceeds eight.
