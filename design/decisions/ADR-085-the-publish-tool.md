# ADR-085 — The publish tool: a session-scoped agent tool whose receipt is the publish evidence

- Status: accepted (2026-11-29, publish tool landed)
- Related: ADR-084 (publishing is a tool, not a job action), ADR-069 (self-evolution is a
  proposal pipeline), ADR-071 (the retired conductor publish action), ADR-075 (landing is the
  owner's merge, verified), ADR-052 (job model)

## Context

ADR-084 retired the conductor publish action and named the migration target: an agent-callable
publish tool with its own authority boundary, receipt shape and idempotency/crash rules. That
tool is the only way a new self-dev plan can publish; without it S2a/S2b remain frozen-plan
replay only.

Three constraints shape the design. First, the job plan may not name a write action (JOB-MODEL
§3), so the publish step has to be an ordinary `agent` step whose prompt asks for a tool call.
Second, the agent loop in `kian-loop` holds one `ToolProvider` for its whole life and the seam
carries no session identity (`ToolProvider::schemas` / `invoke` take only the call), so a tool
that must know which run it is publishing for needs a per-drive resolution point rather than a
global provider. Third, the event stream is the sole authority: what a tool did must be readable
from committed events, and a tool call that never got a result must never be silently retried
over a possibly-landed write.

## Decision

1. **Publishing is a tool call inside an ordinary agent step.** A publish step declares
   `verify: publish_ok`; validation (JOB-MODEL V33) accepts that check only on an `agent` step
   that declares exactly one step input (the patch it publishes) and pins `retries: 0`, and allows
   at most one such step per plan.
   The tool takes no arguments: the goal is "publish the artifact this step was given", and any
   argument would be a second, agent-authored source of authority.

2. **The tool is mounted per drive through a session-scoped tool scope.** `kian-loop` gains
   `ToolScope::tools_for(session)`; a drive resolves its provider once for the driving session
   and uses it for the whole turn. The publish tool refuses any session it cannot place in a
   committed job run, so the same loop can serve ordinary workers without exposing publish.

3. **Authority is derived from committed events, never from the agent.** The tool reads the
   child session stream, requires a `session/created` parent that names the run, the
   authoritative first-input job marker with `self_dev` provenance, a dispatch record from that
   run to this child and step, a plan step that declares `publish_ok`, and the step artifact as
   an input. It re-verifies the artifact digest before writing. The only operator authority is
   config: `selfdev.publish.gates`, `push`, `pull_request` and `token_env`; a push without a
   token env is a config error, and a pull request without a push is a config error.

4. **The receipt is a child-stream `tool/call` / `tool/result` pair.** The loop owns the call id
   and appends the intent before the external work and the result after it; the result carries
   `{"ok": true, "step", "attempt", "result": <publish receipt>}`. The conductor judges
   `publish_ok` from that pair: exactly one call, empty arguments, a matching result, `ok: true`,
   a 40-hex commit, the derived branch and an exit-zero gate when configured. An intent without a
   result is `needs_operator`, never a pass and never a retry.

5. **One publish per run, replay-safe, never retried.** A prior call with a result is replayed
   idempotently (the same receipt is returned); a prior call without a result refuses with "a
   prior publish attempt is unresolved ... an operator must inspect". A failed write step that
   called the tool settles `needs_operator` with the effect ledger, because the worktree or
   branch may exist. The step pins `retries: 0` so the runtime never re-drives the write on its
   own.

6. **Verification reads the same receipt.** `kian self verify` keeps its frozen-plan path
   (run-stream `job.publish` pair) for old plans and gains a tool path that locates the
   `publish_ok` step, reads the child stream through the dispatch record and validates the
   tool receipt. Both paths prove the same link: the artifact digest, the commit and the gate.

7. **No new event kind, table or route.** The call and its result are the existing `tool/call`
   and `tool/result` kinds on the child stream; the conductor check is an extra check on an
   existing step verdict; the tool mounts through an in-process scope, not a wire surface.

## Consequences

- A new self-dev plan can publish again, with the same fixed sequence but a tool-owned
  authority and a receipt the conductor and the verifier can both judge.
- The frozen conductor publish path stays in code for old streams and stays unreachable from new
  plans; it is a format reader, not a second supported surface.
- A publish attempt that fails between the intent and the result leaves explicit evidence and
  demands operator inspection instead of a blind retry. That is the intended cost of a write.
- Deployments without `selfdev.publish` do not mount the scope at all, so the default worker
  tool set and its prompt prefix are unchanged.

## Alternatives rejected

- **Restore `action: publish` as a job action.** Rejected by ADR-084: it reinstates the
  conductor as a repository writer and gives new plans a second write authority.
- **Make `publish_ok` an implicit default on write steps.** Rejected: a plan must declare where
  its evidence comes from; implicit checks would let a step publish without saying so.
- **Take arguments (branch, commit, artifact id) from the agent.** Rejected: the agent would
  select its own authority surface; every field is derivable from the frozen plan and the run
  identity.
- **Expose publish as an admin or gateway tool outside the job child.** Rejected: it would make
  the receipt unlinked to a step, and the conductor could not judge `publish_ok` from the child
  stream.
- **Mark the step retryable and let a retry repair a missing result.** Rejected: the external
  write may have landed already; a retry is a second write, not a repair.

## Limits recorded with this decision

- The tool path is covered by unit and conductor tests plus verifier tests; a real serve
  end-to-end publish against a live repository was not executed in this landing, and Postgres
  was not exercised (no database available).
- `push` and `pull_request` extend the same receipt but were not exercised beyond config
  validation; a Partial result keeps them operator-visible.
- The frozen conductor path remains intentionally unmigrated so old receipts stay verifiable.
