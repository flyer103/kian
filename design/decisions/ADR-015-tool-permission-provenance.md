# ADR-015: Per-call tool permission provenance — allow/deny, fail-closed

- **Status**: draft (; append-only — ADR-001…014 immutable)
- **Date**: 2026-11-07
- **Context**: plan §2.4/§3. Until now the only tool-surface control was the
  agent's `tools:` list: an unlisted tool never rides the request schema — a coarse,
  *compile-time* filter of what the model can *see*. There was no per-call decision recorded when
  a listed tool was *invoked*, and no way to deny one without hiding it (hiding changes tool-schema
  bytes and therefore the request prefix). Borrowed shape: CMA's `evaluated_permission` +
  `evaluation` on tool use, filtered through Kian's constraints — config-first (the footprint
  ladder's smallest rung), zero new event kinds, zero model-request byte changes.

## Decision

1. **Config-first: `agent.permissions` (and per-agent `agents.<id>.permissions`)** —
   `BTreeMap<String, Permission>` with the **closed** vocabulary `Permission { Allow, Deny }`
   (`kian_core::model`; lowercase wire form). Unknown values (`ask`, `maybe`, casing variants)
   refuse to *parse* — fail-closed at the serde boundary, before validation even runs. Every key
   must appear in the agent's `tools:` list (validated fail-closed in `ServeConfig::validate`):
   a policy for a tool the agent cannot call is a config error, never a silent no-op. Absent
   section = empty table = allow-all.
2. **Provenance-on-the-action.** A non-empty table decides *every* call, not only denials: an
   explicit entry is `policy:"configured"`; a tool the table does not name falls through to the
   `policy:"default"` allow. The decision is recorded on the `tool/call` payload itself as an
   additive top-level `permission` key:
   `{evaluated:"allow"|"deny", policy:"default"|"configured", matched:<tool>}` plus
   `reason:"tool_denied_by_policy"` on the deny arm. The audit truth of *why a tool ran (or did
   not)* therefore lives in the event stream (constraint 1) — not in logs, not in a projection.
3. **Evaluation point: before the `tool/call` append, once per call.** Log-before-execute holds
   for **both** arms: the allow arm logs then invokes as today; the deny arm logs the attempted
   call (with deny provenance) and **never invokes** — no tool side effect. The deny arm then
   appends a synthetic `tool/result` with `is_error:true` and
   `output:{"error":"permission denied by policy: <tool>","policy":"deny"}`; the model sees an
   honest error next turn and reacts. A denied call is a *tool failure*, never a loop abort —
   the loop continues and the session can still complete honestly (constraint 7's spirit: the
   truth of what happened is recorded, not laundered into a crash).
4. **No new event kind.** The `tool/call` event (log-before-execute) already carries the
   call as an action record; permission is *provenance on that action*, the same pattern set
   for redaction reports and for usage — an additive payload key on a frozen wrapper, never
   a new kind (additions-only event discipline, plan rule 4). The synthetic denied `tool/result`
   reuses the existing kind because it *is* a tool result the model must see — inventing a
   `tool/denied` kind would make the model-visible round trip a special case for no gain.
5. **Cache-byte stability (constraint 3).** The `permission` key rides the payload **outside
   `content`**; `derive_model_context` reads `role`/`content` only, so the projected message —
   and therefore the request bytes and the provider prefix cache — is identical with and without
   the key (golden-tested). Furthermore, with **no table configured the key is omitted
   entirely**: the `tool/call` payload is byte-identical to the legacy literal, so an unchanged
   config produces an unchanged stream. (This reconciles the design's "absent config =
   byte-identical today" with "provenance on allow": provenance rides only when a table exists;
   `"policy":"default"` is the *fall-through* arm of a configured table, not the no-table case.)
6. **Where it evaluates.** The built-in loop (`react-loop` plugin / assembly), the
   superagent plain-session (default agent) loop, and the superagent worker-template loop map
   their agent's table into `ReActConfig.permissions`. The Router and Watchdog judge loops do
   **not** evaluate permissions — rather than silently ignore a table on their agents,
   config validation **refuses boot** for one. The dsh harness replaces the worker's inner
   loop and does not evaluate the table (recorded boundary; the config still validates).
7. **`ask` is out of scope, by design.** The vocabulary is closed at `allow|deny` so a later
   confirmation plane can add `ask` additively — both to the config enum and to the payload's
   `evaluated` field. `ask` requires machinery that is deliberately not built: a durable
   pending-confirmation state, a human/PLane response path into the inbox, and a hold semantics
   for a drain paused mid-turn — each its own design question (an `ask` that crashes the
   confirmation channel must not wedge sessions). Deferred, not forgotten.

## Consequences

- Deny is the only behavior-changing arm, and only when explicitly configured: no invocation, an
  honest error result, provenance in the stream. Everything else is audit surface.
- The deny arm's synthetic `tool/result` *does* enter the model context (that is its purpose);
  this changes request bytes for sessions where a deny actually fires — inherent to the feature
  and gated on config, never on the default path.
- `AgentDef.tools` enforcement semantics are untouched: the schema filter (what the model sees)
  stays exactly as built it; permissions decide what *runs*. Two layers, one honest
  boundary: schema-hidden tools are never called; schema-visible tools may be denied per call.
- Tool-schema order/serialization, `derive_model_context`, request bytes and the evidence gate
  are untouched (asserted by tests).
