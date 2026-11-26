# ADR-065: Channel images as vision references (opt-in)

- Status: accepted (2026-11-26, channel surface slice)
- Related: ADR-062 (channel-edge media), ADR-063 (vision content parts),
  ADR-064 (iMessage attachment bytes), ADR-006 (model slots are immutable
  for a session's lifetime), ADR-040 (runtime resident rotation),
  DESIGN §3.2, SPEC §P5c

## Context

ADR-062 made the channel edge durable for media: every inbound item is
stored as a content-addressed artifact and replaced in the payload by a
reference plus a transcript, so text reaches the model even when the
media tool fails. ADR-063 then added vision content parts — but only for
message content admitted as `{"text", "images"}`, and it recorded the
channel edge as an explicit non-goal: "the channel-edge media processor
keeps transcribing to text; vision is an explicit API/file path". The
later ADR-063 alternatives list called automatic channel vision "a later
opt-in".

The live iMessage channel shows why that deferral costs answers. A photo
whose only legible text is a logo reaches the model as the transcript
`bilibili`; the loop holds the pixels in the artifact store, the channel
payload holds their reference (`media: [{kind, media_type, artifact,
size, transcript}]` after the edge), and no code path joins the two. The
model can only recite the transcript. Two earlier slices removed the
transport blockers (ADR-064 serves the bytes, the JPEG variant makes HEIC
readable upstream), so what remains is a shape problem, not a fetching
problem.

Three constraints decide where the shape may live:

1. **The channel wrapper is load-bearing.** Every routing, labeling and
   read decision parses `content["kian"]`: `parse_channel_marker` and
   its lenient twin (`provenance.rs:227`, `:250`), `raw_view` (`:212`),
   `channel_event_untrusted` (`:189`), `channel_input_text` (`:288`),
   `latest_channel_input_text_before` (`:338`), consumed by the outbox,
   router, toolpack and registry. Replacing the wrapper with ADR-063's
   flat envelope would blind reply routing and channel reads in one move.
2. **Vision is per target and the binding is session-immutable**
   (ADR-063 decision 3, ADR-006). A deployment can turn pixels on without
   every session that already exists being able to read them.
3. **Cache-byte stability** (constraint 3). Turning the feature on must
   not rewrite the text the model reads for past turns.

## Decision

1. **One opt-in flag.** `superagent.shaping.vision: true`, default
   `false`. It is a shaping knob because the decision is "what shape does
   the admitted content take", and shaping is the one place the content is
   built. A channel-level flag would duplicate the same decision per
   channel section.
2. **The wrapper stays; the references ride beside it.** At admission,
   inline content becomes `{"kian": <the wrapper as today>, "images":
   [{"artifact": <content id>, "media_type": <iana type>}]}`. One entry
   per payload item that the edge already materialized: `kind == "image"`
   and a non-empty `artifact` and `media_type`. Audio never qualifies —
   its transcript is the whole value of the slice. The `kian` object is
   produced by exactly the same code and serializes to the same bytes as
   today, so every marker parser above keeps working untouched.
3. **Over-cap inputs keep today's shape.** The Inline/Spill path still
   admits the bounded excerpt with no `images`: the pixel path is bounded
   by the same cap as the text path, and a spilled raw is out of context
   by design. Recorded as a limit, not a hidden fallback.
4. **The loop recognizes the sibling, and the text stays byte-identical.**
   `vision_envelope` accepts any user content object carrying a usable
   `images` array. Its text is the object's sole `text` string when that
   is the only other key (ADR-063's original shape), otherwise the
   compact serialization of the object *without* `images` — which for a
   channel wrapper is exactly the string the model reads today. No
   channel knowledge enters `kian-loop`; the narrow waist holds.
5. **The capability gate does not move.** Bytes load only when the bound
   profile sets `vision: true`. A vision-shaped message on a blind target
   drops the references with one warning naming the count and sends the
   text — never base64 to a provider that cannot read it, never a failed
   turn.
6. **Config refuses the contradictory pair at boot.** `shaping.vision:
   true` requires a `media:` section (nothing else stores the artifacts)
   and requires the Router agent's configured slot to resolve to a
   `model_profiles:` entry with `vision: true`. A flag whose only purpose
   is pixels, pointed at a target that cannot see them, is a
   misconfiguration that must fail at startup rather than at 03:00.
7. **Boot diagnostic for the already-bound session.** A resident Router
   session replays its binding from its own stream (ADR-006), so a live
   session whose projected binding lacks a vision profile will keep
   reading text even after the flag is on. `kian serve` logs that once,
   naming `superagent.router.rotate_after_events` as the way to retire
   the session (ADR-040). The flag never rotates a session by itself:
   choosing which live session ends is not a rendering decision.
8. **A failed transcription keeps its reference.** The edge's error item
   becomes `{"kind", "artifact", "media_type", "size", "error"}` when the
   blob was already stored; a store failure still carries no reference
   because there is none. An image the model can see is worth admitting
   even when the text tool could not describe it (ADR-062 amendment).
9. **No new event kind, no new crate, no core seam.** `ShapingConfig`
   gains a `bool`, `provenance` adds one sibling key, the loop widens one
   recognizer, `config` adds two checks and one diagnostic.

## Alternatives considered

- **Replace `content` with ADR-063's `{text, images}`.** Blind every
  marker parser (reply routing, recall labeling, channel reads). A
  rendering feature may not take a security- and correctness-relevant
  blast radius.
- **Teach `kian-loop` to read `content.kian.raw.media`.** Pulls channel
  provenance into the core crate (constraint 10) and duplicates reference
  extraction in a second place.
- **Decide at send time from the bound profile, no flag.** The admitted
  content would then depend on which session received it: a blind target
  would get a JSON object with artifact hashes and no wrapper semantics,
  or admission would have to project session slots on every inbound
  event. Both are worse than one explicit flag.
- **A per-channel flag (`imessage.vision: true`).** Duplicates the same
  decision on every channel section while the shape itself is shaping's.
- **Rotate the session automatically when the binding is stale.** A
  rendering flag must not retire a live agent session as a side effect.

## Consequences

- A photo now reaches a vision target as pixels, while the transcript
  stays in the text — so replay, compaction, cost accounting and blind
  targets keep today's behavior.
- Flipping the flag costs one prefix-cache invalidation for the affected
  sessions and nothing else; text bytes are unchanged by construction.
- The images ride every later turn of the session (prior context is
  immutable, constraint 3): each vision turn re-sends the image bytes to
  the provider, and the persisted `step/request` carries them (ADR-063
  decision 5 — the audit body grows by roughly 4/3 of the image bytes).
  Compaction removes them from later context when the message is folded.
- A blind target under a stale binding sees one content hash per image in
  its text until the boot warning's rotation is applied — visible, not
  silent.
- Spilled inputs still cannot show pixels.
- Pixels are an unfiltered input path: the untrusted bit still labels the
  wrapper, but the image bytes themselves bypass text hygiene. Vision is
  off by default and only an operator can turn it on.

## Limits recorded with this phase

- No per-image pricing or dedupe: the target's profile prices the request
  like any other, and two messages re-sending one artifact are two
  uploads.
- No vision on the spill path (bounded by the admission cap).
- No outbound direction: the channel still sends text only.
- The media tool still runs for every image (ADR-062), so the transcript
  cost is unchanged — the transcript is what keeps blind targets and
  compaction honest.
- No automatic slot migration: an already-bound session needs the
  configured rotation (ADR-040), and this slice adds no CLI verb for it.

## Acceptance

- L1 `provenance`: flag on/off; only `kind: "image"` items with both
  fields; the `kian` object serializes identically to the flag-off
  content; the untrusted bit survives; the spill path emits no `images`.
- L1 `react`: a wrapper-plus-siblings message renders today's exact text
  under a blind profile and attaches bytes under a vision profile; the
  ADR-063 `{text, images}` shape is unchanged; a missing artifact degrades
  to text with a warning.
- L1 `config`: `shaping.vision` without `media:` is refused; without a
  vision Router profile is refused; the boot warning names the rotation
  knob for an already-bound session.
- L1 `media_edge`: a failed transcription with a stored blob keeps
  `artifact`/`media_type`/`size`; a store failure still carries none.
- L2 live host: with the flag on and a rotated Router, an inbound iMessage
  photo produces a `step/request` carrying image content parts and a
  reply that describes the picture; before rotation the warning appears
  once and the same photo answers from the transcript.
