# ADR-062: Channel-edge media ingestion (phase 5b)

- Status: accepted (2026-11-23, P5b slice)
- Supersedes: none
- Related: ADR-056 (tool result artifacts), ADR-057 (client-side web tools), ADR-061 (client-side media tools), ADR-018 (WeChat, best-effort), ADR-042 (iMessage, best-effort)

## Context

ADR-061 gave the runtime OCR, speech-to-text and text-to-speech as ordinary tools, plus an
operator ingest path (`kian artifacts put`). What is still missing is the inbound channel half:
a voice message or a photo arriving on a channel cannot become context, because both real
adapters map text items only and drop media (CHANNEL-WECHAT §7, CHANNEL-IMESSAGE §8).

The approved P5 order puts ASR/TTS *at the channel edge*, before the turn exists, and the reason
is structural: a person sends a three-second voice note, and the transcript must be part of the
admitted input bytes. A model cannot transcribe it by calling a tool afterwards, because the bytes
only ever existed in the adapter's hands and are not in the event stream.

The constraint stack decides the placement. Channel adapters live in `kian-superagent` (the
bundle) and may not grow provider knowledge; no core crate may name a provider, an endpoint or a
response shape (constraint 10, narrow waist). Admission is one funnel: an event becomes a durable
inbox row and every admitted payload is what the model reads (constraint 2, persist-what-you-send).
And model context must stay small and cache-stable: a base64 photo inside an admitted payload would
be megabytes in the prompt wrapper, in every replay and in every prefix (constraints 1 and 3).

## Decisions

1. **The media edge sits in the channel pump.** The assembly-owned consumer loop
   (`spawn_subscription_consumers` in `kian-cli`) is the one place every adapter event passes
   through before `SubscriptionService::handle_event`. The edge is a `kian-cli` component holding
   the ADR-061 media tools and the artifact store; `kian-superagent` and every adapter stay
   provider-blind and their code is unchanged. It is not a new seam, not a new core primitive and
   not a second scheduler.

2. **The inbound contract is an additive typed envelope on `ChannelEvent.payload`.** A channel
   event whose payload object carries a `media` array is a media event: each item is
   `{"kind":"image"|"audio","media_type":"<mime>","base64":"<bytes>"}` with an optional
   `language` for audio. The ordinary `text` key stays the caption or command text. A payload
   without `media` is passed through byte-identically, so every existing event and every existing
   adapter test keeps its exact bytes.

3. **Store first, then transcribe.** For each item the edge decodes the bytes, writes them into the
   content-addressed artifact store (`ArtifactStore::put`) and calls the matching tool
   (`image_ocr` for images, `audio_transcribe` for audio) with the artifact id and the item's
   media type. The rewritten item carries `{"kind","media_type","artifact","size"}` plus a
   `transcript` (or an `error`); the base64 is never re-emitted, so an admitted media message holds
   a reference and a transcript, never raw bytes. The blob is then retained, collected and
   export-pinned exactly like any other artifact (ADR-056, ADR-059).

4. **The transcript joins the admitted text.** A successful transcript is appended to the payload's
   `text` under a fixed English marker (`[image transcript]` / `[voice message transcript]`) in item
   order. The durable row — and therefore the model context, the audit and every replay — contains
   what the person actually said. The envelope stays on the payload as the non-context carrier
   (channel provenance shaping already serializes the whole payload into the wrapper's `raw`, so the
   artifact reference is readable there); the model reads the text.

5. **The edge never drops a message.** A decode failure, a missing tool, a provider 4xx/5xx, a
   transport failure, an oversize input or a missing artifact each become an `error` string on that
   item, and the event proceeds to admission with whatever text and transcripts exist. A failed item
   says so in the payload; no media item silently disappears, no admission is blocked and no pump
   failure is invented. The edge does not retry.

6. **Only a configured `media:` section mounts the edge.** `media:` (ADR-061) already requires
   `artifacts:`. Without the section there are no media tools and no edge, and the channel pumps
   behave exactly as before. Inbound media therefore requires both sections; a media envelope from
   an adapter without them is a configuration error, not a supported degradation.

7. **Tool output stays untrusted data.** OCR/ASR text enters the payload exactly as the ADR-061
   tool result and passes through the same channel-provenance shaping as any other channel text.
   Untrusted channels stay untrusted; the transcript is never treated as an instruction in runtime
   code.

8. **Outbound audio is not a channel capability yet.** Shipped adapters are text-only by contract,
   so a voice reply is `speech_synthesize` (which stores the audio and returns an artifact
   reference) plus a text reply naming it. The edge never rewrites a delivery, no
   `channel/delivered` payload changes, and no TTS call happens behind the model's back. When a
   channel grows an audio-send path, the delivery already carries an artifact reference it can
   resolve — that is a channel phase, not this slice.

## Limits recorded with this phase

- No shipped adapter emits media envelopes yet. WeChat media lives on a separate CDN
  (CHANNEL-WECHAT §7) and the iMessage bridge hard-codes `attachments:false`
  (CHANNEL-IMESSAGE §7). The edge, the envelope and the tools are landed and tested against mock
  channel events; real ingestion is each channel's phase 2 and reuses this contract unchanged.
- The edge is sequential per event: one OCR/ASR call per media item, no queueing and no batching. A
  slow provider delays that channel's consumer, never the admission of other channels' events.
- Media requests are not priced (ADR-061 limit), and the transcription request is not a
  `step/request` event — it is a client-side tool call made by the edge, before the turn exists.
- No per-item policy beyond `media.max_input_bytes`; an adapter is responsible for not handing the
  edge an unbounded base64 string.
- The envelope is not projected into model context as structure; the model reads transcript text
  only.

## Alternatives considered

- **A `ChannelMediaProcessor` seam on `kian-superagent`'s `SubscriptionService`.** Rejected: the
  bundle would need provider knowledge or a new core seam, for a preprocessing step that the
  assembly pump already owns. Narrow waist wins.
- **Let the model call `audio_transcribe` itself.** Rejected: the adapter's bytes never reach the
  event stream, so the model would have no artifact to name; and the transcript would not be in the
  admitted bytes, which breaks replay and audit.
- **Admit base64 and let the ADR-056 spill path handle it.** Rejected: the payload rides the
  provenance wrapper into model context at full size, breaking cache stability and inflating every
  later request (and possibly the event-size bound).

## Consequences

- A voice message or a photo becomes a durable, replayable text turn with a referenced blob; the
  cost is one media request per item at admission time, paid before the model turn.
- Channel adapters gain one typed, additive payload contract to implement when their media phase
  lands; nothing else in the bundle changes.
- The edge is observable only through the durable payload and the tools' own audit (a tool/call is
  not appended here: the edge is not the loop). Tests and operators read the admitted payload.

## Acceptance (landed)

- `crates/kian-cli/src/media_edge.rs` tests: an image item becomes an artifact reference plus a
  transcript and no base64 remains in the rewrite; an audio item carries its language into the
  provider body and gets the voice marker; a provider 5xx becomes an honest `error` item and the
  event still proceeds; an undecodable item is reported, not dropped; a payload without `media` is
  returned byte-identically and issues no request.
- `crates/kian-cli/src/superagent.rs` tests: the pump still routes every event exactly once when an
  edge is absent, and the injected edge sees each event before `handle_event`.
- Design contract: CHANNEL-WECHAT and CHANNEL-IMESSAGE record the envelope as their phase-2 media
  contract; DESIGN §4.2 records the placement; SPEC P5b records the capability.
