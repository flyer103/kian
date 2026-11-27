# ADR-073 — Request audit bodies carry artifact references, not inlined image bytes

- Status: accepted (2026-11-27, audit surface slice)
- Amends: ADR-063 (vision content parts)
- Relates: ADR-056 (artifact references and disposable blobs), ADR-054 (one plan produces the
  persisted and sent bytes), ADR-066 (per-turn image budget), ADR-068 (oversized-request
  classification), ADR-072 (byte-pressure folds)

## Context

ADR-063 renders an image-bearing message as OpenAI content parts and inlines the image as a
`data:<media_type>;base64,<bytes>` URL inside `step/request`. ADR-066 keeps every image at or before
the last assistant message pinned, and ADR-072 can only remove pixels by folding whole history
ranges. Each decision is correct for what it promises; together they make the log a copy machine —
every turn re-persists every image it re-sends, at 4/3 of the bytes.

Production numbers from one deployment: a resident Router session reached 227 events in fifty
minutes while each vision turn persisted 5.5–19 MB; the database held 30,776 events and
3,210,220,129 bytes of envelope across 339 sessions, with four vision sessions at 300–700 MB each.
Replaying one such session at startup took the process to 6.9 GB RSS on a 7.9 GB host, the sqlite
reads stretched to seconds, the claim heartbeat was missed, and the takeover chain closed the
session `failed`. The bytes are the same photograph a hundred times over.

The photograph already has a canonical home — the content-addressed artifact store, whose `id` is
the lowercase sha256 hex of the bytes (ADR-056) — and the stream already names it:
`input/admitted` carries `content.images: [{"artifact", "media_type"}]` (ADR-065), which is what the
loop loads. The inline base64 inside the request body is a *rendering* of a reference the stream
already holds, and that rendering is exactly what `body_sha256` (ADR-054) and the byte gates
(ADR-068, ADR-072) measure.

## Decision

1. **A persisted request body renders each image part as a content-addressed reference.**
   `{"type":"image_url","image_url":{"url":"artifact:<sha256-hex>","media_type":<IANA
   type>,"size":<u64>}}`, and the payload gains `"body_form":"artifact_refs"` plus
   `"body_bytes":<u64>` (the length of the wire body). A payload without `body_form` is inline:
   every legacy row and every image-free body keeps its exact bytes.
2. **The wire body does not change.** The provider still receives the `data:` URL content parts, and
   `body_sha256` stays the digest of those exact bytes, so the plan digest, the ADR-068 pre-flight
   bound, the ADR-072 byte measurement and the `turn/request` hook metadata keep their meaning.
3. **The reference is a pure function of the plan.** The id is the store's own address function
   (lowercase sha256 hex of the image bytes), computed from bytes the loop already holds, so the
   audit rendering reads no store and adds no clock or process input; the same committed inputs and
   blobs yield the same reference bytes on replay.
4. **One plan, two renderings.** `RequestPlan` gains the audit body and its form. Both
   `step/request` append sites — the ordinary turn and the `purpose: "compact"` maintenance turn —
   persist the audit form, and so does the `slot/fallback` attempt copy, so a fallback retry stops
   duplicating a megabyte body as well. The `turn/request` hook keeps receiving the wire body,
   because its `request_bytes` and `request_sha256` describe what flew.
5. **Reconstruction is one named surface, and it verifies.**
   `kian session request <uuid> --seq <n> [--expand] [--json]` prints the stored payload;
   `--expand` reads each referenced blob, requires `sha256(blob) == id`, rebuilds the exact `data:`
   URL, and checks the rebuilt body against `body_sha256`. A missing blob is reported by id and the
   command fails — a reconstruction is never silently partial.
6. **Durability moves to the export.** The live blob store stays disposable (ADR-056 decision 6: no
   reference counts, retention GC reaps). `kian session export --with-artifacts` already pins the
   blobs a stream references (`referenced_artifact_ids`); it now also collects the ids a request
   body references, so an exported package reconstructs the exact bytes without the live store. The
   export is the self-sufficient audit artifact; the live log is the reference-bearing projection.
7. **What survives a reclaimed blob.** Identity (the id is the digest of the bytes), media type,
   size, the wire length and `body_sha256` remain; only the pixels are gone, and no reader
   substitutes fabricated bytes for them. That is the degradation ADR-056 already accepts for a
   spilled tool result, and the one vision replay already has (`load_images` drops a missing blob
   with a warning).
8. **Everything else is unchanged.** `kian session replay` still counts a content-part request as a
   vision request rather than a byte-identity mismatch (its non-goal stands: no re-derivation on
   that path), the context-fidelity audit still skips content-part requests, and the estimate band,
   the token and byte gates, compaction and the per-turn image budget read the wire body exactly as
   before.

## Alternatives

- **Keep inlining and bound retention.** Rejected: growth is O(images × turns) inside one session's
  life, so any bound that keeps the session usable is a time bomb — which is the incident above.
- **Spill the whole request body to a blob and keep a preview** (the ADR-056 shape applied to the
  body). Rejected: it writes the same tens of megabytes every turn — the bytes move, the growth does
  not — and makes the request unreadable without the blob, strictly worse than a reference that
  leaves the text intact.
- **Persist only `body_sha256`.** Rejected: the body records the plan's decisions (which references
  the per-turn budget admitted, the tool-schema order, the fallback targets) and a digest
  reconstructs nothing, not even from a complete store.
- **Reference counting or pin files.** Rejected by ADR-056 decision 6: a count is authoritative
  state outside the event log and is not derivable when compaction rewrites ranges; a pin file is the
  same state with a different name.
- **A separate immutable audit tree with its own retention.** Rejected: it duplicates every image the
  store already holds once by content address, adds a second retention story to operate, and is
  policy-bound anyway. Export pinning gives self-sufficiency on demand instead.
- **Expand inside every consumer** (replay, fidelity audit, cache stats). Rejected for this phase:
  replay explicitly refuses to re-derive from the store (ADR-063 non-goal) and reports its behaviour
  honestly today; one named, verified read surface is the smaller testable delta.

## Consequences

Positive: per-turn log growth drops from O(image bytes) to O(text) — the incident's 18–35 MB bodies
become tens of kilobytes — so replay memory, restart time and database size follow; a byte-pressure
fold stops being the only way to keep a vision session alive; the audit keeps an exact digest chain
and gains a verified reconstruction command.

Negative: the log alone is no longer sufficient for the pixels, and an export without
`--with-artifacts` carries references. This narrows persist-what-you-send from "the bytes are in the
event" to "the event identifies the exact bytes and proves them" — a real weakening, and the reason
this decision needs a record. Every reader of a request body must check `body_form` before treating
a part as inline, and a reconstruction costs one blob read per image plus a digest check.

Rollback: a pre-ADR-073 binary reads the reference body as an ordinary JSON body — `tools` and
`messages` still project, the fidelity audit still recognizes the content parts — it simply cannot
expand them, so a rollback degrades reconstruction, never execution.

Limits recorded with this phase: no rewrite of existing events (the inline bodies already on disk
stay; the change is append-only), no per-image dedupe beyond content addressing, no reference-aware
GC (retention stays by policy), no re-derivation in `kian session replay`, no change to what the
provider receives or to any gate's reading. A reference whose blob is gone fails expansion by id.

## Acceptance

- L1 (`kian-llm`): a vision plan yields a wire body byte-identical to today's and an audit body whose
  image parts are references; expanding the audit body through a blob map reproduces the wire body
  and its `body_sha256`; an image-free body carries no `body_form` and stays byte-identical; a
  missing blob fails expansion naming the id.
- L1 (`kian-loop`): a vision turn's persisted `step/request` carries `body_form` and `body_bytes`
  with reference parts while the `turn/request` hook still reports the wire size, and the
  `slot/fallback` attempt copy carries the reference form.
- L2 (`kian-cli`): a real serve with a mock provider admits one image, persists a body orders of
  magnitude smaller than the wire body, `kian session request --expand` reproduces it byte-for-byte
  with a verified `body_sha256`, and `session export --with-artifacts` pins the referenced blob
  beside the JSONL.
- L3 (production): after deploy a real vision turn's `step/request` row is kilobytes, `--expand`
  reproduces the tens-of-megabytes wire body with a verified digest, and a restart that previously
  reached 6.9 GB RSS stays flat.
