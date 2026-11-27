# ADR-071 — Publishing is a conductor action, not a general command surface

- Status: accepted (2026-11-27, self-evolution contract S2 publish; specification only)

## Context

The self-evolution contract (ADR-069, design/SELF-EVOLUTION-MODEL.md) splits the authority ladder
into S1 propose, S2 publish and S3 land. S1 is landed: a self-dev run is an ordinary job run over a
verified evidence pack, its product is a patch artifact checked against a denial surface frozen in
the plan, and the run ends at its proposal. S2 is the first step that writes: it must turn the
proposal into a branch and a commit, and then into a push and a pull request.

ADR-069 left the enabling amendment open: "S2 publish: one derived worktree and branch, one push,
one pull request, enabled by the `exec` step action JOB-MODEL already anticipates." That sentence
carries a decision that was never argued. `exec` means the plan can name any command, which turns
the job surface into an unbounded command surface and forces three new mechanisms into the runtime:
an allowlist of programs, a credential distribution story for every program, and an idempotency
story for every side effect a program might have. None of those are capabilities the runtime
needs in order to publish a patch; all three are capabilities the runtime would then have to
defend.

Publishing a patch is not an arbitrary command. It is a fixed sequence with known inputs (a
patch artifact and a base commit), known outputs (a branch, a worktree path, a commit, a gate
verdict) and known failure points, and it already has a precedent in this contract: delivery is a
conductor action (JOB-MODEL §13.1), not a tool a worker may call. Delivery is not a model command
because the run must not be able to decide where its own words go; publishing is not a model
command because the run must not be able to decide what its own write runs.

## Decision

1. **Publishing is a conductor action.** A `publish` step is executed by the conductor, not
   placed on a worker: no capability, no binding, no dispatch, no model turn. JOB-MODEL §13.5 states
   the symmetry with delivery and the reasons `exec` is rejected.
2. **The step is declarative and narrow.** `publish: {base?, gates?, push?, pull_request?}`, exactly
   one input pointing at a produced text artifact, `effect: write`, `retries: 0`; prompt, requires,
   pin, prefer, allow_offline, produces, channel, target and content are refused by validation
   (SELF-EVOLUTION-MODEL.md §14 V27–V32). There is no field that names an arbitrary program.
3. **The sequence is fixed and audited.** Read the producer's committed bytes, re-verify the
   artifact digest at the moment of the write, create the derived worktree and branch, apply the
   patch, commit with audit trailers, run the configured gate, and write one receipt
   (SELF-EVOLUTION-MODEL.md §9.4). A step cannot choose a different order or a different tool.
4. **Every name is derived.** The branch is `selfdev/<run_session>` and the worktree is
   `<repo>/.agents/worktrees/selfdev-<run_session>`; the call id is
   `job:publish:<step>:<attempt>`. Derivation is what makes two concurrent proposals safe without a
   lock (SE9) and what makes a retry of the same attempt distinguishable from a second attempt.
5. **Intent before the write, receipt after it.** The `tool/call` pair is appended before the
   worktree exists and the `tool/result` pair only after the commit and the gate, so an
   interruption leaves an intent with no receipt, which the outcome lattice maps to
   `needs_operator` with the worktree left in place. A step that finds an occupied worktree or
   branch refuses instead of adopting it.
6. **The gate is configuration, not plan content.** `selfdev.publish.gate` names the command
   (default `./scripts/selfcheck.sh`) and a plan cannot override it, so a proposal cannot choose
   the evidence that judges it. The gate's output is bounded before it reaches the receipt.
7. **S2 is split where the credential appears.** S2a (publish-local) stops at a commit in a local
   worktree and holds no credential at all. S2b (publish-remote) adds exactly one non-forced push
   and exactly one pull request, and the token is read from the environment named by
   `selfdev.publish.token_env` at the moment of the push — never stored in the plan, the event
   stream or the receipt.

## Alternatives

- **A general `exec` step action (the amendment ADR-069 anticipated).** Rejected: it would add a
  command allowlist, a credential distribution path and a per-command idempotency contract to the
  runtime in order to express one fixed workflow. The blast radius of a wrong allowlist is the whole
  host, while the blast radius of the publish sequence is one derived worktree.
- **A CLI verb, `kian job publish <run>`.** Rejected: publishing is a step inside a run, so a verb
  would advance a run from outside its own stream and break the conductor's sole-driver rule (J5)
  and the park/resume recovery story.
- **A receipt file beside the worktree (`<worktree>.publish.json`).** Rejected: a second copy of a
  fact that also lives in the stream drifts the moment either side is edited, and the contract's
  carrier rule forbids a new one. The receipt is the `tool/result` pair; the branch and the PR body
  are references the receipt names.
- **Shipping S2 in one phase.** Rejected: the local sequence can be reviewed and tested without any
  credential, and the credential surface deserves its own review. S2a and S2b land as separate
  slices of the same contract.
- **Giving the publish step a capability or placing it on a worker.** Rejected: a capability is
  eligibility, never authority (J15), and a worker that can publish can publish anything the
  conductor can — the authority would leak to the harness.
- **Force-pushing a derived branch on retry.** Rejected: a retry after a push must not rewrite what a
  reviewer may already be reading; an existing branch is a refusal, not a reset.

## Consequences

- JOB-MODEL gains one step action, one job-owned check (`publish_ok`) and one section (§13.5); its
  invariants J1–J16 are unchanged and the additive list A15–A19 stays additive (no new event kind,
  no new route, no new table).
- The conductor becomes the only component that writes to a repository, which keeps the layered
  iron rule intact: execution appends events, and the publish pair is an event about a write,
  committed before and after it.
- S2b is the first capability in the runtime that holds a repository credential. Its scope is one
  push and one pull request; the credential is never written down.
- Review throughput remains the limiting resource, and a failed publish leaves a worktree and a
  branch behind for the operator to delete; the contract records that instead of hiding it.
- The gate makes a publish step cost one build. A proposal that cannot afford the gate is not a
  proposal the owner can review, so the cost is part of the contract rather than a knob.

## Limits recorded with this phase

- Specification only: no `publish` action, no receipt, no gate configuration and no credential
  handling exists in code; nothing in this ADR changes runtime behavior.
- S2a produces a commit that is not pushed, so the "proposal" is only visible on the host that ran
  it until S2b lands.
- The gate is a local command; a compromised or stale checkout can make it pass. The receipt
  records the commit and the gate name, not a proof of correctness.
- S2b's push and pull-request plumbing is specified at the level of the operations it performs, not
  at the level of every failure mode the code host can return; an already-existing branch is a
  refusal.
- The publish sequence assumes a git repository whose worktree root is the repository root; a
  submodule or a bare checkout is out of scope.

## Acceptance

This slice (documentation):

- design/SELF-EVOLUTION-MODEL.md and design/JOB-MODEL.md both link this record, and every relative
  link in the authored docs resolves (`cargo test -p kian-cli --test docs_map`).
- Every surface the contract adds is marked as not implemented, and the documents add no event kind,
  wire route or table.

S2a, when it is implemented (the levels are the ones SELF-EVOLUTION-MODEL.md §16 fixes):

- L1: a publish step is refused when it carries a forbidden field, names no artifact, or names an
  artifact another step does not produce (V27–V32); the receipt is a `job:publish:<step>:<attempt>`
  call/result pair and a step with no receipt settles `needs_operator`; an occupied worktree or
  branch is refused; the artifact digest is re-verified before the write and a mismatch refuses.
- L2: a real store and a real repository — a self-dev run reaches its publish step, the derived
  worktree holds the applied patch and the audited commit, the gate verdict is in the receipt, and
  the run verdict is `succeeded` only when the branch, the commit and the gate are all present.
- L3: one real proposal over a real failure, published to a local worktree and reviewed by the
  owner.

S2b is not accepted by this ADR; it is deferred to its own slice and its own acceptance
(SELF-EVOLUTION-MODEL.md §16).
