# ADR-013: Thread/Correlation intake — derived read positions and thread keys, context-only correlation

- **Status**: draft (C61 / kian-67; append-only — ADR-001…011 immutable, ADR-010 contract v0 stays frozen)
- **Date**: 2026-11-03
- **Context**: Round-6 Wave B, plan §3 card C61. Kian's intake has channel-admit idempotency
  (`(session, event_id)` — one durable row per event, never two) and adapter-chosen topics. The
  prototype's domain layer (reference 001, the Source/Thread/Correlation/Trigger rows; digest §5
  "what kian lacks" gap row 2) adds three things kian does not name: a **Source read position**
  ("a restart must not replay events" — 001's Source rule), a **Thread key** (one canonical
  correlation unit per source shape), and the normative rule that **fan-out grants no authority**
  (001's Subscription row; 002's "this delivery bypasses the gate and grants no new authority. Each
  Ticket remains limited by its Mandate"). This ADR projects all three onto the existing substrate
  with zero new event kinds, zero tables, zero core seams.

## Decision

1. **Thread key — a config-owned canonical derivation.** The additive config
   `superagent.intake.thread_keys: [{channel, key: [group?, <payload-field>…]}]` derives, at adapter
   mapping time, the canonical topic form `<channel>:<group>#<key>`:
   - `key` entries are, in config order: `"group"` — the event's adapter-declared group provenance
     (chat id, `owner/repo`), filling the `<group>` slot (omitted when not configured; a named-but-
     absent group is honestly underivable); any other name — a payload field path (dotted segments
     walk nested objects, e.g. `issue.number`) whose scalar value joins the `<key>` part.
   - **Deterministic**: same config + same event ⇒ same key, always (pure function, no clock, no I/O).
   - **Collision-free by construction**: every reserved character (`%`, `:`, `#`, `.`) inside a
     component is percent-encoded, so distinct component tuples always derive distinct topics — a
     `:` inside a group can never impersonate the channel separator, a `.` inside a value never
     impersonates the key-field join.
   - **Keys are config-owned**: a config change is a *new* key; old streams keep their topics —
     cache-stable, no rewrite of already-admitted context (constraint 3). This is an **operator
     contract**: thread identity belongs to the deployment's config, recorded here, not to the
     event store.
   - After derivation the key **is** a topic: routing stays the existing topic-keyed subscription
     rule matcher, unchanged (C61 never touches it — the matcher gains no correlation concept).
   - Underivable is an honest `None` (missing field, non-scalar field, missing group, channel
     without a rule): the adapter's own topic stands; never a fallback guess.
2. **Source read position — derived from the stream, or honestly re-polled.** The resume cursor is
   derived from the **last admitted event's payload cursor fields** per source kind
   (`read_position(source, admitted_tail)` in `kian-superagent/src/intake.rs`):
   - lark chat history: the `message_id` of the last admitted message (offset semantics — the next
     poll resumes after it; the history API pages by create time and the id is the durable marker
     of what admission already saw);
   - GitHub REST list endpoints: the `since` timestamp — the `updated_at` of the issue/PR the last
     admitted payload belongs to, exactly the field the list endpoints filter on.
   - Where a source provides **no derivable cursor**, the honest v1 bound is **idempotent re-poll
     under `event_id` dedupe**: a restart re-polls at most once per underivable source, and the
     pipeline's existing `(session, event_id)` idempotency absorbs the replay — `deduplicated`
     admissions, zero duplicate rows. **No durable position table** (that would be pre-event
     durable state outside the event log, needing its own ADR — explicitly rejected this round).
3. **Correlation adds context, never routes.** The derived key surfaces in the C33 Inline
   `neighbors` block (`thread_key` beside the canonical `topic`/`event_id` association ids) so the
   Router and workers reach sibling threads through the existing C35 recall toolpack — a recall
   hop, never a routing input. 001's rejected-concepts row, kept verbatim: *Subscriptions route;
   Correlation only adds context.*
4. **Fan-out grants no authority; bounded by Mandate (ADR-012).** A fanned-out message reaches live
   subscribed tickets as **context** bounded by their Mandates; **no admission check runs on it**
   (001's rejected-concepts row "Admission checks on Subscriptions", kept). One trigger fanning out
   to many tickets is the existing C38 fan-out mechanics (one durable row per subscriber,
   idempotent per `(session, event_id)`) restated under the authority rule — nothing in this ADR
   re-derives or re-gates it. This is the normative statement of DESIGN §4.1.

## Rules it keeps

- The event stream stays the sole truth: read positions and thread keys are **derived**, never
  stored outside events (a position or key that only lived in a table would be authoritative state
  the stream cannot rebuild — constraint 1).
- `ChannelEvent`'s shape is untouched (the derivation *reads* it); admission idempotency is
  untouched (the re-poll bound *is* it); the subscription rule matcher is untouched (keys are
  topics after derivation — no correlation concept enters routing).
- Zero new event kinds (§2 of the round-6 plan): the durable trace of an intake stays the existing
  admitted/spilled pair; the key and the cursor ride payload bytes that already exist.

## Consequences

- A restart costs at most one re-poll per underivable source — bounded waste, never duplicate rows.
- Thread identity is deployment-config-owned (the operator contract above): two configs over the
  same event derive two keys, and both stay stable under their own config forever.
- `intake.rs` is bundle-only: pure derivation over `ChannelEvent`s and admitted stream events
  against `kian-core` seams; no store, no inbox, no wake — assembly and adapters call it
  (enforced by the resident iron-rule grep and the C61 acceptance test).
- Correlation hops compose with the C33/C35 Inline/Spill vocabulary: the key is one more
  association id in a bounded view, so multi-hop recall stays bounded.
- The adapter cursor helpers (`github::payload_since_cursor`, `lark::payload_message_cursor`) are
  pure functions over payloads — fixture-testable, no network, no client.

## Rejected

- An `intake/positioned` event kind — read position is *source* state, not session state, and v1
  derives it or re-polls; a second record would fork the truth the admitted events already carry.
- A bundle-owned durable position table — pre-event durable state outside the event log; would need
  its own ADR and a plan amendment; the honest v1 bound (idempotent re-poll) makes it unnecessary.
- Cross-source identity mapping — 001: a person is Trigger metadata; an identity mapping across
  sources is prompt text, never a derived structure. The thread key correlates *threads*, not
  persons; it never merges identities.
- Correlation that routes (again, and forever) — subscription routing stays topic-keyed; the
  matcher gains no correlation concept.
- Admission checks on fan-out — the Mandate (ADR-012) separates authorization from context; a gate
  on fan-out would re-couple them and contradict 001's Subscription row.
