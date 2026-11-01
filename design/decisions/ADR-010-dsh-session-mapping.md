# ADR-010: The S6 dsh mapping contract — harness events onto the existing Kian kind set

- **Status**: accepted for the contract (C53 lands the mapping module + conformance
  fixture; C54 implements the `kian-dsh` bridge driver against it)
- **Date**: 2026-11-01
- **Context**: SPEC S6 and DESIGN §16 pin the S6 shape as a *mapping protocol*
  between dsh session-log semantics and the Kian EventStore — "harness events → a
  Kian durable-event subset". The round-4 additions-only contract adds **zero**
  event kinds, so the mapping must select from the existing twenty; the two
  closest calls were pre-recorded in the round-4 plan §2. The bridge itself (C54)
  drives a `dsh --profile sdk` child over stdio; any dsh-side plugin (the
  `dsh-superagent-prototype` lineage, ecosystem-owned, outside this repo) must
  satisfy this contract to be kian-compatible.
- **Decision**: the contract lives bundle-level (`kian-superagent::dsh` — the
  `kian-core` kind set and `derive_model_context` are untouched) and consists of
  four parts:

  1. **The mapping table** (executable form:
     `crates/kian-superagent/tests/fixtures/dsh/mapping_table.json`):

     | dsh durable kind | kian kind | payload rule |
     |---|---|---|
     | `user/message` | `input/admitted` | `{"role":"user","content":<verbatim>}` (+ `synthetic` when the peer marks an injected message) |
     | `system/message` | `input/admitted` | `{"role":"system","content":<verbatim>}` — **closest call #1**: legal under the frozen C4 message contract (any non-empty role) and honest: it *is* an admitted input |
     | `assistant/message` | `assistant/message` | `{"role":"assistant","content":<assembled content verbatim>}` |
     | `assistant/attempt` | `step/request` | the attempt's assembled request bytes ride `body` verbatim; the envelope's `slot`/`provider`/`model`/`iteration` come from the bridge session's binding. An attempt with no following `assistant/message` **is** the honest failure trace — **closest call #2**, no `attempt/*` kind exists |
     | `tool/call` | `tool/call` | `{"role":"assistant","content":{"id","name","arguments"}}` (the loop's log-before-execute shape) |
     | `tool/result` | `tool/result` | `{"role":"tool","content":{"call_id","output","is_error"}}` |
     | `step/start` | `turn/start` | `{"iteration":<n>}` — a dsh *step* (one model request plus its tools) is exactly a Kian provider turn; `iteration` is derived from emitted turn boundaries, as the built-in loop derives it |
     | `step/end` | `turn/end` | the loop's payload shapes; `cancelled` is the interrupt-settled outcome (interrupted ≠ completed, constraint 7; the session-level end reason stays the orchestrator's end write) |
     | `turn/start`, `turn/end` | — (absorbed) | the dsh episode envelope's Kian counterpart is the drain episode, whose events the orchestrator already owns; the mapper returns an explicit `Absorbed` marker — auditable, never a silent drop |
     | anything else | error | `UnknownKind` — an honest contract error; adding a row is a contract amendment. (dsh `request/header`/`request/context` are real dsh durable kinds deliberately outside v0.) |

  2. **Generation handling.** dsh session files are versioned generations that
     decode to *current* logical events before anything else (the dsh session-
     format contract). The bridge accepts only logical events: the peer's first
     frame is `ready` declaring its logical format, and any value other than the
     supported one is `ForeignLogicalFormat`, refused before any mapping. Kian
     never stores foreign generations — mapped events are re-encoded as v0 Kian
     envelopes.

  3. **Persist-what-you-send across the process boundary (constraint 2).** The
     peer must deliver each step's assembled request bytes (`assistant/attempt`)
     **before** the provider call settles. Observed at the boundary: no
     `assistant/message` or `tool/*` frame is accepted for a step whose request
     bytes have not arrived first (`ResponseBeforeRequestBytes`). If the dsh SDK
     surface cannot emit the bytes pre-flight, constraint 2 cannot hold and the
     bridge **stops at a documented negative result** — never a best-effort
     post-hoc logger.

  4. **Control translation (Kian → dsh).** Promoted inputs ride `input` frames
     (the plain message content, exactly what the inbox row carried); steers ride
     `steer` frames (the peer injects the content into its next admitted request
     and marks the resulting `user/message` `synthetic: true`, so the A6 strip
     survives the round trip); interrupts ride `interrupt` frames (the peer stops
     its turn; a cancelled step settles as an attempt with no
     `assistant/message`).

  Wire form: contract `v0`, stdio JSON-lines (one JSON object per line); frames
  are additive-only like every frozen surface in this repo — breaking changes
  are contract amendments.

- **Alternatives considered**:
  1. New `attempt/*`, `harness/*` or `generation/*` event kinds — rejected: the
     round-4 additions-only contract is empty, and every fact in the table is
     expressible with existing kinds; a mapped session must replay through the
     *unmodified* `derive_model_context`, which the conformance fixture asserts.
  2. Mapping dsh `turn/*` onto Kian `turn/*` and folding steps away — rejected:
     a Kian turn is one provider request cycle (the loop brackets every
     iteration); that is a dsh *step*. The dsh episode maps onto the Kian drain
     episode, which the orchestrator already owns — absorbing it explicitly
     keeps both sides' semantics honest.
  3. Storing dsh generations as opaque envelope payloads and decoding at read —
     rejected: it puts foreign authoritative state inside the Kian stream
     (constraint 1) and breaks byte-stable replay (constraint 3).
  4. A post-hoc request logger when the peer cannot emit pre-flight bytes —
     rejected by the card and by constraint 2: an honest documented stop beats a
     best-effort audit.
- **Consequences**: the contract is provable without Node/dsh — the C53
  conformance fixture (`dsh-fixture-peer` + scripted JSONL) runs in default CI
  and pins both mapping directions, the steer/interrupt translation, the
  ordering rule and the honest-error discipline; the live `KIAN_DSH_BIN` smoke
  (C54) is env-gated and CI-excluded. Mapped payloads must keep riding the
  frozen payload shapes the built-in loop emits (verified byte-for-byte by the
  fixture's A2-style context assert).
- **Rollback Trigger**: a dsh-side contract amendment (new kinds, new frames)
  that cannot be expressed additively — the mapping table is then renegotiated
  in a new plan round, not patched in place.
- **Cost Impact**: zero new infrastructure, zero core-crate changes; one
  pure module + fixture scripts in the bundle.
