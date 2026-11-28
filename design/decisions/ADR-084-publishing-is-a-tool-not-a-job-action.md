# ADR-084 — Publishing is a tool, not a job action; the conductor publish path is frozen replay

- Status: accepted (2026-11-29, publish-action retirement landed)
- Amended by [ADR-085](ADR-085-the-publish-tool.md): the agent publish tool landed; this record's
  "tool pending" limits are the state at retirement, not the current state.
- Related: ADR-069 (self-evolution is a proposal pipeline), ADR-071 (publishing is a conductor
  action), ADR-075 (landing is the owner's merge, verified), ADR-052 (job model)

## Context

ADR-071 decided that publishing is a conductor action: a self-dev plan declares a `publish` step,
the conductor runs one fixed sequence (worktree, apply, commit, gate, push, pull request), and the
receipt is a `tool/call` / `tool/result` pair on the run stream. S2a and S2b were implemented on
that decision (`PublishPort`, `GitPublishPort`, `publish_ok`, the frozen `publish` field and
its digest rules).

The hardened job contract says the opposite. JOB-MODEL §3: "Publishing is not a job action. A step
that publishes is an agent step whose prompt asks it to use the publish tool; the tool's own
contract owns the sequence and its receipt. The job model owns only agent and deliver steps."
JOB-MODEL §7.2 repeats it in the step table. The reason is the narrow waist: a job plan is data, and
every action it can name is a runtime-owned semantic. A publish action put a repository write into
the plan vocabulary and made the conductor the only component that can write a repository. The
tool route keeps the same fixed sequence, but the authority is a tool grant and the receipt is a
tool receipt, not a second job action.

The two contracts cannot both describe new plans. Keeping the action accepted would leave
JOB-MODEL §3 false and would preserve a second write authority the hardened contract deliberately
removed. Removing the action without a migration record would strand the plans already frozen on
older streams and make their replay/verification look like a regression.

## Decision

1. **A publish is a tool, not a job action. New plans declare only `agent` and `deliver` steps.**
   Validation refuses a new plan whose step names `action: publish` (JOB-MODEL V32), and the plan type keeps
   the action only so a frozen plan from an older stream still deserializes. The publish sequence
   and its receipt belong to the publish tool's own contract.

2. **The conductor publish path is frozen replay, not a supported plan surface.**
   `PublishPort`, `GitPublishPort`, `execute_publish`, `observe_publish`, `publish_ok` and
   the `publish` plan field remain in the code solely to replay and verify plans admitted before
   this decision. A new plan cannot reach them. They are not a second supported way to publish, and
   no new test may treat a newly admitted `publish` step as accepted.

3. **The frozen digest stays byte-identical.** The `publish` field keeps its serialization and
   digest contribution for old plans; retiring the action does not rewrite a recorded plan,
   re-canonicalize a digest or invalidate an old receipt. `kian self verify` keeps reading the old
   receipt.

4. **S2a/S2b are not re-declared usable.** Until the agent publish tool lands, a self-dev run ends
   at its proposal artifact and the publish half of the authority ladder is reachable only by
   replaying a frozen plan. The self-evolution contract records this as a migration state, not as a
   newly landed capability.

5. **The migration target is an agent-callable publish tool.** Its design must name its authority
   boundary (which sessions may call it and with what credential), its receipt shape and its
   idempotency/crash rules. Landing it is a separate change that updates SELF-EVOLUTION-MODEL and
   docs/advanced again; this ADR does not pre-approve that surface.

## Consequences

- JOB-MODEL is the authority for new plans: only `agent` and `deliver` are actionable, and a
  publishing step is an agent step whose tool contract owns the write.
- Existing streams keep replaying and verifying: the conductor's publish code is retained as a
  frozen-format reader/executor, not removed, so an old run can still settle or be audited.
- The self-evolution contract temporarily advertises less than ADR-071 did: S2a/S2b receipts are
  legacy evidence, not a capability a new plan can use. The docs say so instead of presenting a
  refused plan shape as runnable.
- A repository write now requires a tool grant, which is the same confinement point the worker's
  other tools use. The credential question moves with the tool and must be answered when it lands.

## Alternatives rejected

- **Restore V27 acceptance and keep `publish` as a job action.** Rejected: it would reverse the
  hardened job contract, keep the conductor as a repository writer, and require a second migration
  when the tool route lands.
- **Delete the conductor publish code and the frozen plan fields outright.** Rejected: it would
  make already-admitted runs unreplayable and unverifiable, breaking the stream-is-truth and
  digest-compatibility rules for data the runtime already committed to.
- **Accept both the action and a tool indefinitely.** Rejected: two ways to publish means two
  authority models, two receipt shapes and two validation surfaces; the hardened contract exists to
  remove exactly that ambiguity.
- **Pre-approve a tool that simply wraps the conductor port.** Rejected: a tool with no authority
  boundary would let any worker session publish anything the conductor can, which is the leak
  ADR-071's "capability is eligibility, never authority" argument already warned about.

## Limits recorded with this decision

- The agent publish tool does not exist yet; the only publish path in code is the frozen replay
  path. No new run can publish until that tool lands.
- A replay of a frozen `publish` plan still uses the old conductor sequence and credential
  handling; that code is intentionally unmigrated so the recorded receipts stay verifiable.
- The plan type keeps `StepAction::Publish` for deserialization compatibility; that is a format
  concession, not an accepted action. New-plan validation is the gate that keeps it out.
