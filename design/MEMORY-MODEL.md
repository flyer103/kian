# Kian — Memory Model (normative)

> **Status: accepted (owner gate, 2026-11-07); Phase 1 (notes + dreaming) landed 2026-11-07;
> Phase 2 (creation-time injection, §6) landed 2026-11-08.** Representations (§7) remain designed;
> SPEC §5 records P1/P2 as `landed` and P3 as `designed`.
>
> **Change protocol:** any memory semantic change starts here. The event payload bytes are
> normative in [design/EVENT-MODEL.md](EVENT-MODEL.md); the external wire (RPC, CLI, SSE, metrics)
> is normative in [design/API-CONTRACT.md](API-CONTRACT.md); crate/seam placement is
> [design/DESIGN.md](DESIGN.md). A change touching memory semantics updates this file first and the
> three neighbours in the same change.

## 0. Authority split (single truth, no restatement)

| Question | Owner |
|---|---|
| What is a note, a memory scope, a dream, an injection? What are their rules? | this file |
| What bytes does a `memory/note` event carry? Which kinds are context-bearing? | EVENT-MODEL §3/§4 |
| What is the `memory.*` wire surface, error codes, metrics keys? | API-CONTRACT |
| Which crate emits what, and how does memory obey A1–A10? | DESIGN §3/§7 |
| Why memory is event-sourced instead of a file store? | [ADR-016](decisions/ADR-016-memory-as-events.md) |
| Is memory in product scope, and what is its status? | SPEC §1/§5 |

This file does **not** restate payload shapes beyond the minimum needed to fix semantics; the
canonical shape lives in EVENT-MODEL and this file links to it.

## 1. Scope

**In scope:** knowledge that survives a session boundary.

- **Notes** — model-written claims about the work, addressed by a stable subject, carried with
  their derivation and verification state.
- **Dreaming** — an offline consolidation run: a durable input that instructs a memory session to
  read a bounded transcript range and append notes.
- **Injection** — a bounded memory index loaded into a session at creation time.
- **Representations** — disposable projections over notes (e.g. a linked index/map) that make a
  large memory navigable.

**Out of scope (recorded, not silently omitted):**

| Not this | Why / where it lives |
|---|---|
| Weight updates, in-context learning | Not context the runtime can persist; ADR-016 |
| `compact` / compaction | Already owned by EVENT-MODEL §4.3; compression is not memory |
| Harness-side memory (dsh auto-memory, `MEMORY.md`, Claude Code files) | Harness owns interaction and its own files; the kian-dsh mapping is ADR-010 |
| A vector database or file folder as the author of memory | Forbidden: the event stream is the sole truth (constraint 1) |
| In-session memory writing (a `memory_write` tool mid-turn) | Deferred to a later phase; see §12 |
| Cross-agent shared memory | Deferred; see §11 |
| Memory editing UI | Not a semantics question; CLI first |

## 2. Terms

| Term | Definition |
|---|---|
| **Memory scope** | The identity memory belongs to. P1: the Agent identity (`agent.id`), not a session and not a thread. Sessions are the isolation unit; memory exists to cross them. |
| **Memory session** | An ordinary session whose stream holds the notes of one memory scope. It is drained only by dreaming runs; it never receives channel inputs. |
| **Source session** | A session whose transcript is being consolidated. Read-only to dreaming. |
| **Note** | An immutable claim appended as one `memory/note` event, keyed by `subject`. |
| **Revision** | A later note with the same `subject`. The latest revision in `seq` order is the effective one. |
| **Candidate** | A note written from one source range, not yet validated. |
| **Verified** | A note validated against recorded evidence; only verified notes are eligible for injection. |
| **Stale** | A note known to be superseded, wrong, or expired. Projected out of injection; never deleted. |
| **Dreaming run** | One drain episode of the memory session that consumes a dream request and appends notes. |
| **Injection** | The one bounded memory index appended at session creation. |
| **Representation** | A projection over notes (index, map, wiki-like pages). Disposable, rebuildable, never authoritative. |

## 3. Notes as events

### 3.1 The kind (`memory/note`, landed)

One durable kind, non-context-bearing, `DrainActivity`, emitted only by a memory-session drain.
The **canonical byte shape** is the `memory/note` row in [EVENT-MODEL.md](EVENT-MODEL.md) §3 —
the table below is semantic commentary on those fields, and on any disagreement EVENT-MODEL wins
and both files are corrected in the same change. The semantic fields:

| Field | Rule |
|---|---|
| `subject` | Non-empty stable key within the memory scope. The fold key. |
| `text` | The claim. Markdown allowed. Redacted through the `SecretRedactor` before append (kian-91); the value-free report (`[{kind,name,count}]`, the `tool/result` shape) rides the additive, non-context `redactions` key. |
| `status` | Closed vocabulary `candidate \| verified \| stale`; unknown value is fail-closed to `candidate` (never injectable). |
| `derived_from` | Non-empty list of `{session, start_seq, end_seq}` ranges (`[start_seq, end_seq)`, the EVENT-MODEL field names). The evidence the claim is about. |
| `verified_by` | Required iff `status=verified`. `ref` is the **source seq the claim was re-checked at, carried as a string**: the dreamer parser requires it to parse as a seq inside the note's `derived_from` range — it is never an event id. The method vocabulary is closed exactly as implemented: the fold parser (`kian-core::memory`) accepts `transcript \| evidence_gate \| human`, while the only P1 producer (the dreamer parser) accepts and emits `transcript` alone (`evidence_gate`/`human` are reserved vocabulary with no producer yet). Absent otherwise. |

### 3.2 Fold rule (append-only, no mutation)

There is **no note update and no note delete**. The effective note set is a deterministic fold over
the memory session's events:

```
effective(subject) = the note with the greatest seq for that subject
```

Status changes and corrections are new notes with the same subject. History stays in the stream and
is replayable. The fold is a pure function (`project_notes(events)`) and therefore rebuildable —
never stored in a column (constraint 1).

### 3.3 Invariants

- **N1** A note is never rewritten after append; corrections are new revisions.
- **N2** `derived_from` is non-empty; each range satisfies `start_seq < end_seq`.
- **N3** `status=verified` requires `verified_by`; `candidate`/`stale` must not carry it.
- **N4** A note never carries secrets: `text` passes the same `SecretRedactor` as model-visible
  exec output before the append is durable (kian-91), and the value-free report rides outside the
  claim on the additive `redactions` key (`[{kind,name,count}]`, the `tool/result` precedent).
- **N5** Notes are scoped to exactly one memory scope; cross-scope references are forbidden in P1.
- **N6** The note fold is byte-deterministic for a given stream (subject lexicographic ordering when
  rendered); the renderer is frozen content (constraint 3) because its output is injected.

## 4. Note lifecycle

```
             (dreaming run writes)
                      |
                 candidate ---- verify against transcript/evidence ---> verified
                      |                                                   |
                      |                                          drift / superseded / wrong
                      v                                                   v
                    stale <------------------------------------------------+
```

Rules:

- **L1** Every note starts as `candidate` unless the same run re-read the recorded evidence and
  records `verified_by` (the article's "closed loop": rule + how it was checked).
- **L2** `verified` requires evidence that exists at the recorded range; a run that cannot check
  must not claim it.
- **L3** `stale` is terminal for that revision but not for the subject: a later claim is a new note.
- **L4** Injection reads only `verified`; `candidate` and `stale` are never injected.
- **L5** Verification is a claim, not a proof: the event records who checked and against what, and
  `kian memory verify` re-checks ranges offline.

## 5. The dreaming contract

A dreaming run is **a normal drain of the memory session** — not a new execution mode. It inherits
crash recovery, epoch fencing, budget, one-shot guards and audit from the existing substrate.

**Memory-session policy (as landed):** the memory session is operator-created (`kian memory
create`); its scope is the configured agent id (`memory.scope` in `kian.yaml`). There is no
derived-session-id machinery — no hashing of scope ids into session ids, no implicit session
creation on first dream. The session is plain until a dream request lands on it, and it is drained
only by dreaming runs (§2: it never receives channel inputs).

- **D1** The run is triggered by a **marked** durable inbox row on the memory session — admitted
  only by the ops plane (`memory_dream`): exactly `{content, dream}`, the rendered dream prompt as
  `content` plus the trusted, server-set `dream` marker (`{source_session, start_seq, end_seq,
  scope}`), under idempotency key `dream:<source_session>:<from_seq>:<to_seq>`. The drain's
  consumption plan consumes it through the normal path into ONE `input/admitted` payload carrying
  `role`/`content` plus the non-context `dream` key. Dreamer routing and note provenance read
  **only the marker** (fail-closed parse); the prompt's `[kian-dream: …]` first line is
  model-readability, never a trust decision — a plain `submit` whose text mimics it is ordinary
  content and never routes onto the dreamer (kian-91, review B-2). The two-key row shape is a
  reserved envelope in the spill/steer sense — the public wire (`submit`/`steer`/`queue`) refuses
  all three reserved envelope key sets (`{content,dream}`, `{content,spill}`, `{synthetic,content}`)
  with `-32602`, so only the ops plane's admit can land a marked row; no client content can forge
  the marker (kian-94, review B-2′). The layered iron rule and the existing admission
  semantics hold unchanged.
- **D2** Dreaming reads the source session **read-only** (recall / scan). It never appends to the
  source session's stream — that would pollute a live drain's context and invalidate its prefix
  (constraint 3).
- **D3** The source range is bounded by policy: it must lie within the source head
  (`1 <= from_seq < to_seq <=` the source's `head.next_seq`), and the rendered transcript must fit
  `memory.dreamer.max_bytes_read` — the byte cap; no event-count cap exists. An out-of-bounds or
  over-budget request is refused at admission honestly, never silently truncated.
- **D4** Every note carries non-empty `derived_from` pointing into the read range (N2). A note
  without provenance is not written.
- **D5** `verified` may only be claimed when the run (or a later run) re-read the evidence in the
  same memory-scope stream; the check is recorded in `verified_by`.
- **D6** Idempotency: one dream request per `(source_session, [from_seq, to_seq))`. A repeated
  request is a no-op (the existing inbox idempotency key applies).
- **D7** Budget and boundedness: the existing declarative recovery policy governs fallbacks and is
  one-shot guarded per action; the per-run caps that exist in P1 are `max_notes` and
  `max_bytes_read` (`memory.dreamer.*` in `kian.yaml`, config-first, no core code). No scope-level
  rate cap (`runs/hour`) is implemented — dreaming is explicit-only (§11.2).
- **D8** One writer: only a drain of the memory session appends notes — there is no shared mutable
  page (this is why the article's concurrent-write data loss cannot occur here).
- **D9** Dreaming never injects; injection is a separate read at session creation (§6).

## 6. The injection contract (P2 landed 2026-11-08)

Injection is the only path by which memory becomes model-visible. **As landed**, the server's
session-create path (after `engine.create_session`, before the RPC returns) renders the index and
admits it **quietly**: `Engine::admit_quiet` writes a durable inbox row without waking a drain
(wake is advisory; the next client submit wakes a drain that consumes rows in admission order, so
the index always precedes the first user message). The row payload is the reserved envelope
exactly `{"content": <rendered index>, "memory": <marker>}` — the public wire
(`submit`/`steer`/`queue`) refuses that exact two-key shape with `-32602` (the reserved-envelope
discipline), so only the server's own create path can land it. The drain's consumption plan
consumes the row into ONE `input/admitted` carrying the message plus the additive, non-context
`memory` key. The byte-level contracts live in EVENT-MODEL §4.2/§4.5.

- **I1** Shape: one `input/admitted` event with `role:"system"`, `synthetic:true`, appended **before
  the first `turn/start`** of a session. Provenance rides an additive, non-context key:
  `{"memory":{"scope":"<agent-id>","notes":[<event-id>...],"bytes":<u64>,"items":<u64>,
  "truncated":<bool>,"source_seq":<u64>,"redactions":[{kind,name,count}...]}}` — `notes` are the
  derived event ids of the eligible (verified) notes the render started from, and `redactions` is
  the value-free secret-redaction report (the `tool/result` precedent). The `memory` key never
  enters `derive_model_context` or a model request byte; request bytes equal the persisted event
  bytes (verified end-to-end by QA acceptance, kian-102).
- **I2** Exactly once per session lifetime: never on resume, re-arm, migration or takeover. A resumed
  session replays the same first event; re-injecting would change the prefix. Landed mechanics: the
  quiet admit carries the idempotency key `memory-inject:<session-id>` (a repeated admit dedupes at
  the store), and no code path re-injects — the row is an ordinary inbox row consumed once.
  **Known restart-window behavior (QA finding F1, accepted):** a session that crosses a server
  restart between `create` and its first submit has its quiet row woken by the NoLostWork startup
  rebuild, so the index drains **alone** as the session's first provider turn. Exactly-once, prefix
  stability and `unexpected_prefix_change == 0` still hold; the deviations are one extra provider
  round-trip and that the (non-synthetic) assistant reply to that index-only turn survives a
  `strip_synthetic` replay. Removing the window needs a "quiet" discriminator on inbox rows (a
  schema change, out of P2 scope).
- **I3** Only `verified` notes are eligible. `candidate`/`stale` never enter a session.
  Eligibility is *latest verified per subject*: a subject whose newest revision is
  `candidate`/`stale` surfaces its last verified revision.
- **I4** Caps are hard: `max_items` (default 20) and `max_bytes` (default 4096), configured by
  `memory.inject` (fail-closed: zero/negative/non-integer values refuse to start; absent block =
  defaults). Over either cap renders an **index** — subject plus a one-line excerpt (≤ 120 chars,
  frozen), whole entries dropped from the end until the byte cap fits — never the full text.
  Zero eligible notes — or a budget too small for the header plus one entry — admits nothing at
  all (byte-identical to a session without memory).
- **I5** Deterministic rendering: stable subject byte order, frozen renderer — the pure
  `render_memory_index` in `kian-memory` (verified-only fold, fixed `# Memory index` header framing
  the payload as **data, not instructions**, fixed excerpt cap). The default redaction pass is
  pattern-scanning only (env-sourced known values would break cross-machine determinism); assembly
  injects the configured redactor through the `render_memory_index_with` seam and the value-free
  report rides the marker (I1). The rendered bytes are prompt-prefix content; changing the header,
  the entry shapes or the excerpt cap is a constraint-3 event (observable invalidation + golden
  update).
- **I6** Strippable: `synthetic:true` means export/replay can strip the injection
  (`strip_synthetic`) to recover the session's "real" context (A6); a stripped replay is
  byte-identical to a pre-memory session — except across the I2 restart window, where the
  index-only turn's ordinary assistant reply survives the strip.
- **I7** Pinning: a session pins the memory snapshot it started with — the marker's `source_seq` is
  the greatest seq among all eligible notes (including cap-dropped ones), so even a truncated index
  names the full snapshot it was rendered from. Later note changes affect only new sessions; a live
  session's context is never rewritten (constraint 3).
- **I8** Failure isolation: an unreadable or corrupt memory projection, an unknown source session,
  an empty eligible set, an over-tight budget, an identity exclusion (I10) or an admit failure all
  inject nothing and increment the honest `injected_skips` counter (the reason rides the log,
  never a silent drop). It never fails session creation.
- **I9** Config-absent = byte-identical: an agent without a `memory:` section is memory-off, and a
  `memory:` section without `memory.session` is injection-off — both produce byte-identical event
  streams and provider request bytes to a server without the feature (verified end-to-end by QA
  acceptance).
- **I10** Exclusions (identity-based, adversarial A5): the configured **source memory session**
  never receives its own index, and a session whose stream carries a **dream request** (the
  dreamer-agent sessions) is excluded — the dreamer gets its work from dream prompts, never from
  memory injection. The check is pure over the new session's own events (constraint 1).
  **Known creation-seam reachability (kian-104 review B1, accepted for P2):** at the creation
  call site this check is **defense-in-depth only and cannot fire** — a freshly created session's
  stream holds only `session/created` (no dream request exists yet), and the fresh uuid is never
  equal to the configured `memory.session`. The reachable consequence: **any** fresh session
  created while `memory.session` points at a note-bearing session receives that index too —
  including a future memory session created before its first dream request (review probe p7). The
  exposure is bounded: verified-only notes, I4 caps, the "data, not instructions" frame, and a
  `strip_synthetic` replay is unaffected. Accepted for P2; closing the consequence needs an
  explicit "create as memory session" discriminator on the create call (a wire change,
  API-CONTRACT scope) and is deferred beyond P2.

**Cost note (kian-104 review I1):** with injection configured, every `create_session` performs
two full stream rebuilds on the hottest control-plane path — the fresh session's (a single
`session/created` event) and the source memory session's **entire history**, which is then
folded. Render cost is O(memory-session history) per created session; the `memory.inject` caps
bound the rendered **bytes**, not the read. Serving the fold from a snapshot/projection of the
memory session (the READ-MODEL acceleration) is P3 work.

## 7. Retrieval and representations

- **R1** Notes are events, so the existing `RecallPort` can address them by event id with no new
  search infrastructure. `kian memory list/show` are thin projections over the fold.
- **R2** Representations (indexes, maps, wiki-like pages) are **disposable projections** over notes,
  rebuilt on demand. They are never authoritative and never a second truth (constraint 1).
- **R3** Building a representation is itself a dreaming run (same contract, separate subject
  namespace), so it is crash-safe, budgeted and auditable for free.
- **R4** Representation admission heuristic (from the source article's negative results): build one
  only when queries need cross-note synthesis; a representation whose payload exceeds its source
  notes is a failure metric, not progress. Text-search-equivalent questions are answered by recall.

## 8. Consistency with the hard constraints

| Constraint | How memory obeys it |
|---|---|
| 1. Event stream is sole truth | Notes are events; representations and the memory index are disposable projections; nothing lives in a column or a file |
| 2. Persist-what-you-send | Injection is a persisted `input/admitted`; dream prompts and model requests are persisted as usual; a model-visible memory byte always has an event |
| 3. Cache-byte stability | Injection only at creation; live sessions never receive memory updates; the renderer is frozen; memory work happens in a separate session stream |
| 4. Atomic end writes | Untouched — no memory write shares an end append |
| 5. Fencing / CAS | Notes append under the memory session's drain epoch like any other event |
| 7. Honesty | A note is a claim with evidence and status; `candidate`/`stale` are representable and never injected; "verified" records who checked |
| 8. Bounded recovery | Dreaming is a drain with the declarative policy table, per-run caps and one-shot guards |
| 10. Narrow waist | Only the kind + pure fold are core-side; the dreamer prompt/policy live in a bundle crate; no channel/provider specifics |
| 11. Model slots | The memory session binds its own slot at first use; injection happens before the first turn, so no slot rule is crossed |

## 9. Operations and observability

- CLI (wire owned by API-CONTRACT): `kian memory create|dream|list|show|verify` (no `stats` verb
  in P1).
- Metrics: the `memory` fragment of `GET /metrics` — `scope`, `dreams`, `read_failures`,
  `parse_failures`, `notes_appended`, `notes_deduped` (stream crash-resume dedupe),
  `append_failures`; with injection configured (P2, landed), also `injected_sessions`,
  `injected_items`, `injected_bytes`, `injected_truncations` and `injected_skips` (the honest
  inject-nothing counter, I8). Notes-by-status and verification-rate metrics are designed with
  their phase (P3), not landed.
- Audit: the memory session's stream is the audit trail; `session export --with-event-ids` already
  captures it.

## 10. Acceptance criteria

Levels follow the workspace convention: L1 unit/contract, L2 integration (real store/drain), L3
end-to-end (real process/CLI).

| # | Criterion | Level |
|---|---|---|
| M1 | `memory/note` payload golden JSON; unknown `status`/`verified_by.method` fail-closed to candidate | L1 |
| M2 | Fold is latest-per-subject, deterministic and order-independent of insertion interleaving | L1 |
| M3 | A non-context kind leaves `derive_model_context` and request bytes byte-identical | L1 |
| M4 | Dream request crash → recoverable → new drain completes without duplicate notes (idempotency key) | L2 |
| M5 | Two dreaming requests for disjoint ranges interleave without lost notes; a repeated range is a no-op | L2 |
| M6 | `RecallPort` reaches a note by its derived event id across sessions | L2 |
| M7 | Injection caps hold; over-cap renders an index; zero eligible notes emits nothing | L2 |
| M8 | Resumed session never re-injects; `unexpected_prefix_change` stays zero over a kill/resume cycle | L3 |
| M9 | Strip injection → replay is byte-identical to a pre-memory session | L3 |
| M10 | Memory on/off comparison on the `issue-watch` dogfood records task outcome and per-question token cost | L3 (manual, recorded) |

## 11. Open questions

1. **Scope granularity**: agent identity is the P1 choice. Tenant-wide or per-thread memory needs a
   separate decision (sharing, permission provenance, deletion/retention).
2. **Cadence**: when does dreaming run? Idle threshold, access-frequency weighting, or explicit
   CLI only. P1 is explicit-only.
3. **Staleness detection**: environment drift is not observable from events alone (the article's
   open problem). Candidates: TTL on `verified`, re-verification on injection misses, access-count
   driven re-checks.
4. **Representation format**: index + map first; wiki-like pages only after R4's admission heuristic
   is measured.
5. **Dedupe across subjects**: a fold keyed by free-string `subject` permits near-duplicates. A
   merge rule (two notes, one canonical subject) is not designed yet.
6. **Cross-scope reads**: the Router could benefit from worker memory. Deferred until the permission
   model for memory reads is designed.
7. **Subject tagging**: `scope_tags` (optional, non-authoritative labels on notes) has no producer
   and no consumer in P1 and is not part of the payload contract (EVENT-MODEL §3); it stays a
   deferred idea until a consumer is designed.

## 12. Phase non-goals (explicit)

- **Phase 1** (notes + dreaming, no injection): no automatic cadence, no representation, no
  injection, no in-session memory tool, no cross-scope sharing. Rollback = stop dreaming; notes are
  inert events.
- **Phase 2** (injection): no mid-session memory updates, no mutable memory, no memory editor.
  Rollback = config flag off → byte-identical sessions.
- **Phase 3** (representations): no representation as truth, no unbounded build (budget + admission
  heuristic).

## 13. Enforcement tests (when landing)

- Event-model additions-only regression for the new kind and every prior kind (frozen bytes).
- `derive_model_context` byte-identity for the non-context kind.
- Fold determinism and latest-per-subject.
- Injection determinism: the same note set renders the same bytes across runs and machines.
- Crash/resume: no duplicate notes, no prefix change.
- The docs-map test keeps this file's links resolvable.
