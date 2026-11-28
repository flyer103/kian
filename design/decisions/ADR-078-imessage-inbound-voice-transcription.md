# ADR-078 — An inbound iMessage voice note is fetched and transcribed

- Status: accepted (2026-11-27, iMessage inbound voice)
- Related: ADR-062 (channel edge media), ADR-064 (iMessage attachment bytes), ADR-067 (iMessage
  voice I/O)

## Context

A voice note is an ordinary attachment row in `chat.db`, and the Mac records it with an **empty**
`mime_type`: the production row was `Audio Message.caf`, `mime_type: ""`, 8,535 bytes. Both floors
of the `attachments: bytes` path keyed on that type, so a real voice note fell out twice:

1. `BridgeAttachment::media_kind` (`crates/kian-superagent/src/imessage/bridge.rs:285`) admits only
   `image/*` and `audio/*`; an empty type is `None`, so `attach_media` filters the row's only media
   item before any fetch (`crates/kian-superagent/src/imessage.rs:1261`).
2. `AttachmentSource::read` (`crates/kian-imessage-bridge/src/attachment.rs:174-180`) refuses
   anything that is not `image/*` with `415 unsupported_type`, so even a correctly typed audio item
   could not be read.

The admitted payload therefore carried the attachment's metadata and no `media` array, and the model
answered from a placeholder. The backend was not the gap: the same `.caf` posted to
`POST /v1/tools/transcribe` returned its transcript, and `audio/x-caf`, `audio/wav` and `audio/mp4`
were all accepted.

§3.2 of `design/CHANNEL-IMESSAGE.md` already promised that the adapter fetches the row's
`image/*` **and** `audio/*` items through `/v1/attachment`. ADR-064's slice implemented the image
half; this ADR closes the drift for the audio half, which is what an inbound voice note is.

## Decision

1. **The bridge infers a media type from the filename when the row records none.**
   `attachment::media_type_of(filename, recorded)` returns the recorded type when it is non-empty and
   otherwise maps the extension — `.caf` → `audio/x-caf`, `.m4a` → `audio/mp4`, `.mp3`, `.wav`,
   `.aac`, `.ogg`/`.oga`/`.opus`, `.amr`, `.aif`/`.aiff` and the image extensions. The projection
   (`project_attachments`) uses it, so a voice note's `mime_type` stops being `""`, and `read()` uses
   it, so the bytes route agrees with the page that published the id.
2. **`/v1/attachment` serves `audio/*` as the original bytes.** The image-only gate becomes "image or
   audio"; `?as=jpeg` keeps its meaning for an `image/heic` and is a no-op (`x-kian-variant: original`)
   for everything else. Every other guard is unchanged: the participant filter, the generation guard,
   the settle retry, `watch.max_attachment_bytes`, `410` for a vanished file and the sticker refusal.
   `415 unsupported_type` remains for an item the media edge has no tool for — video, PDF, contact
   cards.
3. **The adapter classifies by MIME prefix first, then by filename extension.** The fallback keeps a
   bridge built before this ADR usable (it still projects `""`), so the adapter and the bridge can be
   upgraded in either order.
4. **No new wire, event, config or seam.** One value inside an existing projection field changes, and
   one route serves more types. `attachments: bytes` plus a `media:` section stays the opt-in, and
   `metadata` mode still never calls the route.

## Alternatives considered

- **Fill the type only in the adapter.** The bridge would still answer `415` for the bytes and the
  model would still read `mime_type: ""`; both defects would survive.
- **Transcode `.caf` to another format in the bridge.** Adds an `afconvert` dependency and a new
  failure mode for bytes the ASR provider already accepts, and the media edge would still need the
  type to label the call.
- **Serve every media type.** The edge has an OCR tool and a transcription tool and nothing for
  video, PDF or contact cards, so widening the route would only move the refusal later, after the
  bytes crossed the tunnel.
- **Change the row marker to `[voice message]`.** The marker is durable payload text the settle logic
  and every transcript key on; the appended `[voice message transcript]` line already carries the
  meaning, so a non-image item keeps `[attachment]`.

## Consequences

- A voice note reaches `audio_transcribe` as `audio/x-caf` and the admitted text carries
  `[voice message transcript] …`; the metadata keeps the filename, size and now a type.
- A row whose extension the table does not know, and that records no type, stays metadata-only — the
  same honest degradation as today, with the same audit line.
- `mime_type` becomes a non-empty string for a voice note. That is a payload value inside an admitted
  event, never a request prefix, so provider cache is unaffected.

## Limits

- The table is deliberately small: no `.mp4` (video and audio are ambiguous), no `.pdf`, no `.vcf`.
- No language hint: the adapter never sets `language`, so the ASR provider auto-detects.
- A voice note over `watch.max_attachment_bytes` (default 16 MiB) or the adapter's
  `MAX_MEDIA_ATTACHMENT_BYTES` still fails per item; both bounds stay.
- The route still serves one item per call: a row's audio and its image are separate fetches.

## Acceptance

- L1: `media_type_of` (a recorded type wins, an empty type plus `.caf` infers, an unknown extension
  yields none); `read` serves an audio item with no recorded type, refuses a PDF, and never runs
  `sips` for audio; the projection emits `audio/x-caf`; `media_kind` falls back to the filename.
- L2: the bridge route answers `200` with `content-type: audio/x-caf` and
  `x-kian-variant: original` for a seeded `.caf` row; the adapter emits the `media` envelope for a
  page whose `mime_type` is empty (a bridge built before this ADR).
- L3: a real voice note on the Mac, its transcript in the admitted payload and the visible reply.
  Not run at landing; the first owner voice note is the acceptance.
