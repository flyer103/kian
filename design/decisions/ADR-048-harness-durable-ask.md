# ADR-048: Durable `ask` for harness tools (experimental, opt-in)

- **Status**: accepted (owner gate, 2026-11-20).
- **Context**: phase 1 refuses `ask` on a bridge because the harness process dies with the drain
  (HARNESS-BRIDGE §4.4/§6.3). The confirmation plane itself is harness-agnostic: a `tool/call`
  recorded with `permission.evaluated:"ask"` and no result makes a settle end `awaiting_input`
  (CONFIRMATION-MODEL §6), and `confirm` admits a durable control row keyed by
  `(session, call_id)`. What is missing is the bridge's resume path: the harness's in-flight turn
  cannot be re-entered, so the human decision must be delivered into a **resumed conversation**,
  and the harness may or may not re-issue the same call. That uncertainty cannot be removed by
  kian; it must be made honest.
- **Decision** (phase 1, opt-in):
  1. **Opt-in.** `approvals.durable_ask: true` allows `ask` in a bridge's approval policy;
     without it, `ask` keeps refusing boot exactly as today (fail-closed, no silent downgrade).
  2. **Defer.** On an `ask` decision the driver appends the pending `tool/call` with
     `permission:{evaluated:"ask",policy:"configured",matched}`, does **not** answer the harness,
     drops the child, and returns `Settled`; the engine derives `awaiting_input` from the pending
     call. Nothing runs; no lease is held.
  3. **Resume with the decision.** A `confirm` row is read at the next episode start. The driver
     admits a **synthetic decision nudge** (`input/admitted{synthetic:true}` — model-visible and
     persisted before it is sent) that tells the harness the tool/arguments digest and the
     decision, then resumes the conversation (ADR-047).
  4. **Answer only an identical re-issue, once.** A subsequent approval request is auto-answered
     from the confirm only when its tool name and canonical `arguments` digest match the pending
     call; the confirm is consumed one-shot. The confirmed call keeps **one** record: the adapter
     suppresses a second `tool/call` and maps the harness's result onto the pending call id, with
     `confirmation:{call_id, decision, requested_by, answered_at}` on the `tool/result`
     (CONFIRMATION-MODEL §5, EVENT-MODEL §3). A second identical request after consumption is a
     new call and follows the policy again.
  5. **Anything else is honest.** A different call follows the policy (an `ask` policy asks again).
     If the resumed turn ends without a matching re-issue, the driver closes the pending call with
     one `tool/result{is_error:true, output:{"reason":"confirmed call was not re-issued"}}` carrying
     the confirmation provenance — never a fabricated execution, and never a stranded pending call
     (which would keep the session `awaiting_input` forever). An expired confirm answers deny.
  6. **No new kind/wire.** The decision rides existing rows (`control/confirm`, `tool/call`,
     `tool/result.confirmation`, synthetic `input/admitted`).
- **Alternatives considered**:
  1. **Keep the child alive while a human decides** — rejected: the drain would hold a lease and a
     worker slot for an unbounded wall-clock wait, contradicting CONFIRMATION-MODEL's
     "awaiting_input holds no lease".
  2. **Deny-only (status quo)** — rejected: it leaves harnesses without the confirmation plane the
     ReAct loop has.
  3. **Re-run the interrupted turn after the decision** — rejected: unobservable side effects
     would replay; the decision can only gate a *re-issued* call.
  4. **Trust the harness to re-issue silently** — rejected: the outcome must be durable and
     auditable (the close-out result), not inferred.
- **Consequences**:
  - harness tools gain the durable approval plane, but with a weaker guarantee than ReAct:
    approval covers an *identical re-issue*, not the original in-flight call; a re-planning model
    can choose a different action, and the turn then follows the policy normally;
  - the nudge is model-visible and persisted (A2), marked synthetic (strippable);
  - the harness's re-issue behaviour (Claude after `--resume`, Codex after `thread/resume`) is
    **unverified live** — the capability matrix marks durable ask `◐ experimental`, and a
    deployment opts in explicitly;
  - a harness that never re-issues still leaves an honest audit trail (the close-out result).
- **Rollback Trigger**: a harness whose resume never re-issues (then durable ask stays opt-out for
  it and the matrix says so), or a re-issue that cannot be correlated by tool+arguments digest
  (then the confirmed call is closed unexercised rather than guessed).
- **Cost Impact**: an `Ask` policy variant, a pending-call close-out path, an adapter suppression
  of the second call + confirmation stamping, config opt-in, fixtures; no new crates or kinds.
