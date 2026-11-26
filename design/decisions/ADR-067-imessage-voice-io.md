# ADR-067: Outbound voice on the iMessage channel edge

- Status: accepted (2026-11-26, P5b extension slice)
- Related: ADR-042 (iMessage channel), ADR-056 (artifacts), ADR-061
  (client-side media tools), ADR-062 (channel-edge media), ADR-064
  (attachment bytes), CHANNEL-IMESSAGE §2.5/§3.2/§5/§7/§11/§14/§15,
  API-CONTRACT (bridge wire), EVENT-MODEL (`channel/delivered`),
  SPEC §P5b

## Context

Inbound voice works today: with `attachments: bytes` and a `media:`
section, the ADR-062 edge writes a voice note into the artifact store,
transcribes it and appends `[voice message transcript]` to the admitted
text (ADR-064). Outbound voice does not exist in any form. `POST
/v1/send` carries `{to|chat_guid, text, service}` and nothing else; the
adapter splits that text and posts one part per request. The media
tools already synthesize speech (`speech_synthesize` returns an
artifact reference) but no channel consumes an artifact, so a channel
can describe a voice note and cannot answer with one.

The transport can carry it. `imsg` exposes `send.attachment` (an
IMCore-bridge op that also converts audio into a native voice message)
and `send` accepts a `file` that the AppleScript path stages into
`~/Library/Messages/Attachments/`. Native voice requires the IMCore
bridge, which requires SIP to be disabled on the host; `imsg status`
on the reference Mac reports `sip: enabled`, so the native path is not
reachable there today and a design that depends on it would ship
nothing that runs.

Three constraints shape the decision. The tool schema is part of the
prompt prefix, so changing a published tool's arguments invalidates the
provider cache for every live session (AGENTS.md constraint 3). The
bundle must stay provider- and store-blind, so synthesis cannot live in
`kian-superagent` (the iron rule its tests enforce). `ChannelAdapter`
is a core seam: a new method there needs its own ADR and would put a
voice field on every channel.

## Decision

1. **A new tool, never a new argument.** `reply_voice` takes exactly the
   arguments `reply` takes (`channel`, `text`, optional `target`,
   optional `correlation`) and is mounted only when the assembled
   session has at least one voice-capable channel. `reply`'s schema
   stays byte-identical, so a session that never enables voice sees no
   prompt-prefix change at all and one that does sees an addition rather
   than a rewrite.
2. **Voice is a summary channel.** `reply_voice` always applies the
   in-message policy's summary shaping (`im.enforce`): a spoken
   utterance has no detail mode and no full-mode event. The speak gate
   and speak record apply exactly as they do to `reply`.
3. **The router emits an intent, not bytes.** The delivery content is
   `{"voice": {"text": ...}}`. The bytes are produced on the channel
   edge, in `kian-cli`: the edge synthesizes the utterance, reads the
   artifact from the store, and rewrites the content to
   `{"voice": {"text", "base64", "media_type", "audio": true}}` before
   the adapter sees it. `kian-superagent` therefore gains no provider
   and no store dependency, and the adapter still receives a credential-free
   `ChannelDelivery`.
4. **The bridge ABI grows one additive field.** `POST /v1/send` accepts
   optional `voice: {base64, media_type, audio}`; `text` may be empty
   if and only if `voice` is present (a voice note has no caption), so
   the two are mutually exclusive — both set is a `400`, neither is a
   `400`; decoded bytes are bounded at 1 MiB (`413 too_large`). The success
   body gains `voice_mode: "native" | "attachment"` and `voice_bytes`.
   Protocol stays 1: an absent field is an absent field, and the text
   path is byte-identical.
5. **The bridge owns the transport choice and reports it.** When the
   request asks for audio, the bridge has a chat guid, and the IMCore
   bridge is ready, it calls `send.attachment` with `audio` set (a
   native voice message). Otherwise it calls `send` with the staged
   file (an ordinary audio attachment). A `-32003` from the native
   attempt degrades to the attachment path instead of failing the send,
   and `voice_mode` names the path that actually ran, so the operator
   never has to guess which one they got.
6. **Idempotency keys on the spoken text, not on the audio bytes.**
   Speech synthesis is not guaranteed byte-stable across calls, so a
   client id derived from the synthesized bytes would make a re-drive
   look like a new message. The adapter derives the id from the channel,
   the handle, the spoken text and the voice marker, exactly as it
   derives a text id from the text; the bridge's observed-result cache
   then collapses a replay even though the bytes differ.
7. **A cached id never touches the disk.** The bridge checks its
   observed-result cache before staging anything, so a duplicate answers
   from cache with no temp file. A staged file is created with mode
   0600 under the system temp directory under a unique name and removed
   on every path, including the failure and fallback paths.
8. **Fail-closed configuration.** `superagent.channels.<name>.imessage.voice:
   true` without a `media:` section and an `artifacts:` root refuses
   startup, the same coupling `attachments: bytes` already has: a
   channel that promises voice without the tools to synthesize it would
   only fail at delivery time.
9. **The event stays additive.** `channel/delivered` gains
   `"voice": true` on a voice delivery; every existing key keeps its
   meaning and a text delivery is unchanged.

## Alternatives considered

- **An optional `voice` argument on `reply`.** It is the smallest
  wire change and the wrong one: the schema is prompt prefix, so adding
  an argument rewrites the cached prefix of every session that has the
  reply tool, and the model would have to choose a transport it cannot
  observe. Rejected.
- **The adapter synthesizes from text.** Puts a media provider and the
  artifact store inside `kian-superagent`, which the iron rule forbids,
  and re-runs synthesis inside the execution plane on every replay.
  Rejected.
- **The bridge synthesizes from text.** Would ship a provider key and a
  second synthesis implementation to the Mac, splitting the cost story
  from ADR-061. Rejected.
- **A new `ChannelAdapter` method (or a `voice` field on
  `ChannelDelivery`).** A core seam change, needing its own ADR, for a
  capability one channel has; the content object already carries
  channel-private JSON. Rejected.
- **Send the artifact id and let the bridge resolve it.** The bridge
  would need to read the kian artifact store, whose ids do not cross the
  bridge's trust boundary (§8). Rejected.
- **Native voice only, requiring the operator to disable SIP.** Makes a
  host hardening property a hard dependency of the feature; an ordinary
  audio attachment already works on a stock Mac. Rejected in favour of
  degrade-and-report.
- **Pre-transcode to the native codec in kian.** The codec requirement
  belongs to the transport that imposes it, and `imsg` already converts
  before dispatch; a second converter would drift. Rejected.

## Consequences

- A voice delivery arrives either as a native voice bubble (IMCore
  bridge ready, chat guid known) or as an audio attachment, and the
  bridge response says which. On the reference Mac — SIP enabled — the
  attachment path is what runs.
- A re-drive re-synthesizes (the bytes differ) but converges on the same
  client id, so the bridge's cache answers `deduplicated: true` inside
  its 10-minute window and the recipient never sees a duplicate.
- Cost: one speech-synthesis call per voice delivery. The utterance is
  bounded at 4096 characters by the media tool and at 1 MiB of audio by
  the bridge, so a delivery that cannot fit fails before it flies.
- The adapter's split logic does not apply: a voice delivery is exactly
  one part, because a split audio stream is not a message.
- The 1 MiB bound keeps the request an order of magnitude under the
  bridge's 2 MiB body limit; base64 inflation is accounted for in the
  decode-and-measure, not in the request size.

## Limits recorded with this phase

- No mixed text plus voice delivery: one delivery carries one or the
  other.
- No automatic voice replies. A voice note is answered in voice only
  when the model decides to call `reply_voice`; the channel does not
  mirror the inbound modality.
- Native voice needs two things the operator controls: an IMCore bridge
  (`imsg launch`, SIP disabled) and a known chat guid. Without both the
  delivery is an audio attachment.
- No voice for structured or worker deliveries; `reply_voice` is a
  router-facing tool only.
- Dedupe lives inside the bridge's 10-minute observed-result window; a
  retry after that window is a new message.
- Inbound is unchanged: voice-in already ships through ADR-062 and
  ADR-064.

## Acceptance

- **L1 bridge:** `voice` without `text` is accepted; `text` plus
  `voice` is refused (`400`, mutually exclusive); neither is refused; invalid base64 answers
  `400`; decoded bytes over 1 MiB answer `413`; a cached client id
  answers `deduplicated` without staging a file; the native path is
  chosen when the bridge is ready and a chat guid is known; a
  `bridge_unavailable` from the native attempt falls back and the
  response reports `attachment`; `voice_mode` is present on success.
- **L1 adapter:** `reply_voice` emits `{"voice": {"text": ...}}`; a
  channel outside the voice set is refused; the client id is identical
  for two deliveries of the same text; a voice send maps a bridge
  success to a receipt.
- **L1 edge:** a synthesis failure and a missing artifact both surface
  as a delivery failure rather than an empty send; an artifact over
  1 MiB is refused; a success rewrites the content to the base64 form
  and delegates once.
- **L1 config:** `voice: true` without `media:` and `artifacts:`
  refuses startup; with both it is accepted.
- **L2:** a fake bridge plus a fake media provider carry a
  `reply_voice` call end to end through the CLI assembly.
- **L3 (owner):** on a real Mac, a voice reply arrives as an audio
  attachment; with SIP disabled and `imsg launch` running, it arrives
  as a native voice message and the response reports `native`.
