# ADR-064: iMessage attachment bytes via an authenticated bridge route

- Status: accepted (2026-11-24, P5b extension slice)
- Related: ADR-042 (iMessage channel), ADR-056 (artifacts), ADR-061
  (client-side media tools), ADR-062 (channel-edge media), ADR-063 (vision
  content parts), CHANNEL-IMESSAGE §2.5/§3.2/§3.3/§3.4/§4/§7/§11,
  SPEC §P5b

## Context

The channel admits what arrived: a row's `attachments` become metadata in
the durable payload, so the model can say "a 2.3 MB HEIC named
`IMG_0001.heic`". It cannot know what the picture shows, because the
bytes never leave the Mac: the bridge exports no bytes route, and the
metadata whitelist deliberately never exports the file path, so there is
nothing for the adapter to fetch.

The piece that would consume bytes already exists. The ADR-062 media
edge runs in the channel pump before admission: it writes each
`media` item into the artifact store, transcribes it with the ADR-061
tools, rewrites the item to an artifact reference plus transcript, and
appends the transcript to the admitted text under a fixed marker. Nothing
emits that envelope for iMessage because the bridge has no route to hand
out bytes — the phase-1 non-goal, recorded as "media support needs an
ADR, not a ticket" (CHANNEL-IMESSAGE §11, §15).

This is that ADR. It does not reopen ADR-063: the model reads a
transcript, not pixels; automatic vision from a channel edge stays
rejected and a vision profile is the only path by which a model sees
image bytes.

## Decision

1. **One new route, resolved by the bridge.** `GET
   /v1/attachment?attachment_id=<rowid>:<index>&generation=<g>`,
   bearer-authenticated like every other route and loopback by
   construction (§3.3). The bridge re-reads the row through `imsg` and
   takes `attachments[index].filename`; a client never supplies a path,
   so the "no paths on the wire" property of the metadata whitelist is
   unaffected. The bytes are returned as the response body with
   `content-type` from the attachment's `mime_type` (fallback
   `application/octet-stream`).
2. **The id carries a position, so the fetch carries a generation.**
   `attachment_id` embeds a ROWID, and ROWIDs rebase when `chat.db` is
   replaced or restored (§9). A fetch whose `generation` differs from
   the bridge's current generation answers `409 stale_generation`
   instead of serving whatever attachment now occupies that position:
   bytes for a stale id are plausibly the wrong file, which is worse
   than no bytes.
3. **Bounded and filtered.** `watch.max_attachment_bytes` (default
   16 MiB) answers `413 too_large`; the `watch.participants` privacy
   filter applies exactly as it does to `/v1/updates`, so a row the
   filter hides answers `404 not_found` and never leaks its existence.
   A missing row, index or unreadable file is the same `404`.
4. **Opt-in on the kian side, default unchanged.**
   `superagent.channels.<name>.imessage.attachments` is `metadata`
   (default) or `bytes`. In `bytes` mode the adapter fetches only
   `image/*` and `audio/*` items — at most four per row, and never a
   sticker (decorative: OCR on one pays a provider call for no context)
   — and the mapped payload gains the ADR-062 envelope
   `{"media": [{"kind", "media_type", "base64"}]}` beside the metadata
   projection. The adapter also skips an item whose metadata already
   exceeds its own 16 MiB fetch bound, so the kian side never buffers
   more than the edge could accept. In `metadata` mode the adapter never
   calls the route and the payload is byte-identical to the field's
   current behavior.
5. **Fail-closed coupling to the edge.** `attachments: bytes` without a
   `media:` section refuses startup. The media edge is the only consumer
   of base64; without it the bytes would be persisted and prompted as
   text. Defense in depth: the channel pump strips a `media` key it
   cannot process rather than admitting it.
6. **Failure is per item, never a dropped row.** A `404`/`409`/`413`,
   an unreachable bridge or a timeout leaves that item out of the
   envelope; the message is still admitted with its metadata, so the
   model sees the marker and the size and never a fabricated
   transcript.
7. **The edge, not the adapter, decides what a transcript is.** The
   adapter never inspects bytes, runs no tool and appends no marker: it
   only carries the envelope the edge rewrites (ADR-062).

## Alternatives considered

- **OCR in the bridge.** Would put a provider key and a second media
  implementation on the Mac, duplicating ADR-061's tools and splitting
  the cost story. Rejected.
- **Export the file path and let the adapter read `file://`.** Leaks
  the Mac's directory layout and hands a path to a client, which is the
  one thing the metadata whitelist exists to prevent. Rejected.
- **Inline base64 in `/v1/updates`.** Multiplies every poll page,
  persists bytes before any edge can strip them, and leaves no place
  for a generation guard. Rejected.
- **Vision parts from the channel.** ADR-063 considered and rejected
  automatic vision from the media edge; doing it here would double the
  cost (transcript plus pixels) and change the request dialect for a
  channel whose payload is opaque. Rejected; a vision profile remains
  the supported way to see images.
- **A path-free token in the bridge's state.** The bridge is stateless
  about rows and the `imsg` cursor is not a content address; an id that
  survives a `chat.db` swap cannot be derived without a durable map on
  the Mac. Rejected in favor of the generation guard, which is exact
  and cheap.

## Consequences

- With `attachments: bytes` and a `media:` section, an inbound photo
  arrives as an `[image transcript]` line and a voice note as
  `[voice message transcript]`; the base64 is consumed before admission
  and never persists (ADR-062).
- **Egress delta.** The attachment bytes leave the Mac to the media
  provider when the operator sets `attachments: bytes`. That is a real
  privacy change, it is explicit, and it is off by default; the runbook
  states it beside `watch.participants`.
- A replayed page re-fetches and re-OCRs, because the edge runs before
  dedupe; the artifact bytes stay content-addressed, so the store does
  not grow, but provider calls are paid again. Accepted for phase 2;
  dedupe-before-edge is a separate change.
- The bridge grows a fourth route and one config key; `kian imessage
  check` keeps its read-only preflight role and does not fetch bytes.

## Limits recorded with this phase

- No vision: the model reads the transcript, not the pixels. ADR-063's
  decision stands.
- Video, PDF and sticker items stay metadata-only: the edge has no tool
  for them and the adapter never fetches them.
- No outbound media.
- The route is best-effort like the rest of the bridge: an unreadable
  database answers `503` (`database_unreadable`), and a fetch race with
  a deletion answers `404`.

## Acceptance

- **L1 bridge:** the route requires the bearer token; a known id returns
  the bytes with the attachment's media type; a stale generation answers
  `409`; an oversize file answers `413`; an unknown id and a row outside
  the participant filter answer `404`.
- **L1 adapter:** `bytes` mode emits the envelope with the fetched
  bytes; `metadata` mode never calls the route; a failed fetch keeps the
  metadata and still admits the row.
- **L1 config:** `attachments: bytes` without `media:` refuses startup;
  with `media:` it is accepted.
- **L2:** the media edge rewrites the adapter's envelope shape into an
  artifact reference plus transcript (existing edge tests, same shape).
- **L3 (owner):** a real photo on a real Mac yields an
  `[image transcript]` line in the admitted text.
