# ADR-012: Delegation Mandate — the authority object on the frozen delegation wrapper

- Status: draft (round 6, plan C60)
- Date: 2026-11-03
- Deciders: round-6 architect plan (`shared/round6-plan.md` §5), orchestrator-landed draft ahead of implementation card C60
- Relates to: ADR-010 (frozen contract v0), the C18 delegation wrapper freeze, C39 speculative-marker precedent, ADR-013 (fan-out authority rule)

## Context

A kian delegation today carries task text and no authority object; a worker's bounds are implicit
in the prompt. The dsh-superagent domain model (digest §5 gap row 1; reference 001 Mandate row,
002 authorization paths) separates *what to do* (task) from *with what authority* (Mandate):
immutable, recorded at accept, bounds the Executor, outlives the Ticket, revalidated before resume.
Follow-up fan-out needs an explicit "grants no new authority" anchor (ADR-013 states the routing
half; this ADR owns the authority object).

## Decision

The Mandate is an **additive `"mandate"` object on the C18-frozen delegation wrapper** (the C39
`speculative`-marker mechanism, applied to an object):

```json
{
  "strength": "explicit" | "inferred",
  "scope": "<natural-language bounds>",
  "granted_by": "<sender provenance verbatim>",
  "source": { "channel": "…", "topic": "…" },
  "policy_version": "<string>",
  "matched_rules": ["<names>", "…"]
}
```

- **Immutable**: carried verbatim inside `thread/started`'s task payload (durable, audit-visible
  forever) and as the worker's first message (model-visible). A wider scope is a new Ticket (a new
  `thread/started`), never a mutation of an existing mandate.
- **Issuance**: the Router's `spawn_worker` is the single issuance point; the mandate is built from
  the triggering input's provenance wrapper (the C43 untrusted discipline: the mandate records *who
  triggered*, it does not trust their text) plus `superagent.mandate` config (per-worker-template
  scope templates; strength `explicit` for adapter-declared explicit signals, else `inferred`).
- **Revalidation** (with the follow-up-ownership routing card): on follow-up routing to a live
  worker, `strength: explicit` passes; `inferred` + `policy_version` ≠ the current pointer (the
  policy-versioning card) releases to fresh Eval. The revoke path releases to fresh Eval, honestly
  audited.
- `Delegation` stays frozen; a companion parser (`delegation_mandate`) reads the key; legacy
  wrappers parse unchanged. **Zero new event kinds.**

## Rules it keeps

- Fan-out delivers context bounded by the Mandate (with ADR-013): a subscribed message grants no
  authority beyond the receiver's mandate.
- Only a person's stop (Router judge accept of a stop-strength input, or control-plane interrupt)
  withdraws work; nothing else widens or narrows a live mandate.
- Prompt text may restate but never widen a Mandate (the worker template renders the mandate line;
  restatement is documentation, not authority).

## Consequences

- Authority becomes auditable per thread forever (the mandate rides the persisted
  `thread/started` payload — constraints 2–3 satisfied by construction).
- The Router's spawn tool becomes the single issuance point; config changes to scope templates
  affect new Tickets only (persisted mandates are immutable — cache-byte stability holds).
- Speculative delegations carry mandates too (the C39 markers compose; both are additive wrapper
  keys).

## Rejected

- A `mandate/granted` event kind — the wrapper + `thread/started` already persist it; a second
  record would fork the truth (constraint 1).
- Mutable mandates — widening mid-Ticket is a new Ticket, not a mutation; mutation would rewrite
  persisted context (constraint 3) and break the audit trail.
- Core-side enforcement — the worker harness is outside the domain; kian's fence is the tool
  surface + audit, not the core loop.
