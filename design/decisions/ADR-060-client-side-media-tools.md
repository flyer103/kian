# ADR-060: Client-side media tools and artifact ingest (phase 5a)

- Status: accepted (2026-11-23, P5a slice)
- Supersedes: none
- Related: ADR-055 (tool result artifacts), ADR-056 (client-side web tools), ADR-059 (off-peak deferral), ADR-061 (channel-edge media, phase 5b)

## Context

The runtime needs the media capabilities a Chinese-market operator expects: OCR of
images, speech-to-text for voice messages, and text-to-speech for voice replies. The
DeepSeek media HTTP APIs provide all three. The core question is where the bytes live:
the model-facing prompt must never carry megabytes of base64 (ADR-055 already decided
that large outputs spill by reference), and no core crate may name a provider, an
endpoint or a response shape (constraint 10, narrow waist).

Two consumers exist. The agent loop needs tools it can call like any other tool; it
already has a tool-provider seam (`ToolProvider`) and an artifact store. An operator needs a
way to turn a local file into model-addressable media before any channel adapter can
deliver media (CHANNEL-WECHAT and CHANNEL-IMESSAGE are text-only by contract today).

## Decisions

1. **A new bundle crate, `kian-media`, shaped exactly like `kian-web` (ADR-056).** It is a
   `ToolProvider` only: no new core seam, no new event kind, no new hook port. It exposes
   `image_ocr`, `audio_transcribe` and `speech_synthesize`. The `media:` config section mounts
   them; an absent section leaves the mounted tool set byte-identical.

2. **Protocol-only provider adapter.** The crate speaks documented request/response
   shapes over HTTP: `POST {base_url}/v1/tools/ocr` with `{"image":{"media_type","base64"},"prompt"?}`
   and response `{"text"}`; `POST {base_url}/v1/tools/transcribe` with
   `{"audio":{"media_type","base64"},"language"?}` and response `{"text"}`; `POST
   {base_url}/v1/tools/synthesize` with `{"text","format"[,"voice"]}` and response
   `{"audio":{"media_type","base64"}}`. No provider SDK, no retry loop inside the tool:
   429 and 5xx are reported retryable, a 4xx is an honest non-retryable error.

3. **Bytes cross the seam as artifact references only.** A reading tool takes an
   artifact id, loads the blob through `ArtifactStore::get`, and refuses before any network
   call when the blob is missing (aged out) or when it exceeds `max_input_bytes`. Reading
   tools return `{"text": ...}`; the speech tool returns
   `{"artifact":{"id","size","media_type"}}`. The model never sees base64, and the
   prompt prefix stays small and cache-stable.

4. **Synthesized audio is stored through the same content-addressed store
   (ADR-055).** It is retained, collected and export-pinned like any other tool
   result. Because the tools read and write blobs, `media:` requires an `artifacts:`
   section and the config refuses boot without one.

5. **`kian artifacts put <path> [--media-type M] [--json]` is the operator ingest
   path.** It writes one file into the same store and prints the artifact reference.
   The media type is inferred from a small closed extension table or supplied
   explicitly. This is how a local image or recording becomes model-addressable
   before a channel adapter can deliver media.

6. **Failures are honest tool errors.** A missing artifact, an oversize input, a 403,
   a transport failure and a malformed provider response each produce a structured
   `is_error` tool result with the reason. There is no partial result, no empty-text
   success and no silent fallback to a text-only answer.

7. **Provider output is untrusted data.** The tool descriptions say so, the failure
   text says so, and the returned text is an ordinary tool result: when it is large
   the existing tool-result spill path (ADR-055) decides whether it goes inline or
   by reference. Nothing in OCR/ASR output is treated as an instruction.

## Limits recorded with this phase

- The real WeChat and iMessage adapters still deliver text-only payloads; channel
  media is ADR-061 (phase 5b), and it reuses these tools through the artifact store.
- Image content parts for vision models are phase 5c (last, per the approved order),
  because they change the dialect's message serialization.
- Media requests are not priced: the price book has no media rates, so the cost
  projection covers prompt/completion tokens only. Recorded rather than guessed.
- No media is persisted as an event. The event stream carries artifact references
  (inside `tool/call` and `tool/result`); the blob is disposable by design
  (ADR-055), and losing it only degrades fidelity.

## Alternatives considered

- **Inline base64 in the prompt** — rejected: multi-megabyte prefixes destroy cache
  stability (constraint 3) and violate ADR-055.
- **Server-side media tools in the core** — rejected by narrow waist (constraint 10);
  the provider must stay in a bundle crate.
- **A new event kind for media** — rejected: artifact references already ride
  `tool/call` and `tool/result`, and a media event would need its own projection
  rule for no semantic gain.
- **OCR every artifact read automatically** — rejected: it would spend provider calls
  without an explicit model decision, making cost unpredictable.

## Consequences

- One crate map row, one config section, one CLI subcommand.
- No event-kind change, no core seam change, no hook-port change; docs_map and the
  event catalog stay as they are.
- Sessions that never configure `media:` stay byte-identical.

## Acceptance

- Unit tests: schema order, argument validation, missing/oversize artifact before
  any request, 403/429/transport/empty-response honesty, synthesize stores a blob and
  returns its reference, base64 codec round-trip.
- Wiremock HTTP contract tests for the three endpoints (body and bearer header).
- Config boot validation: bad URL, bad format, blank key env, media without
  artifacts.
- `kian artifacts put` round-trips a file into the store and reports its reference.
- `cargo test -p kian-cli --test docs_map` green (crate map + ADR series).
