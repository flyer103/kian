# ADR-069 — Self-evolution is a proposal pipeline, not an autonomous engineer

- Status: accepted (2026-11-27, self-evolution contract; specification only)

## Context

Three capabilities already make a failure reproducible outside production: `kian self inspect`
aggregates a window over the stream, `kian self evidence` packages one `(session, seq)` anchor
with per-file and aggregate digests, and `kian session replay --provider fake --script` re-drives a
recorded episode against a scripted provider over a forked store (docs/SPEC.md §3.1). A failure can be
pointed at, packaged and replayed without exposing the owner to production.

The next question is what may act on that record. The tempting shape — an agent that opens a pull
request, merges when the gates are green, and deploys — fuses three decisions with different blast
radii: proposing a change, publishing it, and landing it. The runtime already answers "may I claim
this worked?" with its honesty rule (AGENTS.md constraint 7): a green gate is a claim about the change,
not proof that the defect is gone. An agent that merges on green would make that claim on the owner's
behalf, and every later gate would become advisory.

The job contract already carries the pieces a proposal needs: a plan frozen at admit with a digest, a
run as an ordinary session whose driver is a stateless conductor, evidence as declarative checks over
committed bytes, artifacts by reference, delivery as a conductor action, verdicts with an outcome
lattice, and an additive-key rule that keeps the event vocabulary closed. What is missing is a
contract for *who may propose what*, against which denial surface, with what authority.

## Decision

1. **Self-evolution is a proposal pipeline.** A *self-dev run* is an ordinary job run whose subject is
   a committed symptom; its product is a reviewable proposal, never a landing. The normative contract
   is design/SELF-EVOLUTION-MODEL.md (invariants SE1–SE9, the canonical S1 plan, the denial surface,
   the authority ladder, the breaker, the additive surface A11–A14).
2. **The subject is a job run.** No new session state, no new event kind, no table, no daemon, no
   "evolve" verb. The marking travels as one additive key, `job.selfdev`, beside the existing job
   marker; the conductor reads it, the status projection renders it.
3. **A symptom is an evidence pack, verified before admission.** The trigger runs the pack's own
   verifier before any inbox row exists, refuses on mismatch with exit 2 and writes nothing, and
   records the verified digests plus the base commit in the admitted row. The digests are what the run
   was pointed at.
4. **The denial surface is a plan parameter.** `patch_paths_allowed{artifact, allow, deny}` is a
   job-owned check evaluated over the harvested diff bytes, so it is covered by `plan_digest` and
   cannot be edited by a run: a proposal cannot widen its own surface. Deny wins over allow; paths are
   normalised to repository-relative form first; a test-only diff is refused (SE8).
5. **Authority is phased.** S1 propose: no repository write, no credential, artifacts in a worker's
   scratch space. S2 publish: one derived worktree and branch, one push, one pull request, enabled by
   the `exec` step action JOB-MODEL already anticipates. S3 land: not designed; landing is the
   owner's action and requires its own decision record.
6. **A run never waits and never lands.** The run ends at its proposal (SE5); the review gate is the
   owner's pull request, and `succeeded` means "a reviewable proposal exists", not "the patch is
   right" (SE1, §8.1).
7. **Proposing is parallel; landing is serial.** Write isolation comes from derivation — each run's
   branch, worktree and pull request head are derived from its identity — not from a lock, so two
   proposals cannot collide and no lock serialises the thinking (SE9).
8. **Repetition is a fact, not a retry loop.** The trigger is manual, and a `consecutive_failures`
   limit refuses the next run while the last N run verdicts are all failures; clearing it is a passing
   proposal or an explicit `--force` that records the override.

## Alternatives

- **An agent that merges on green and deploys.** Rejected: it makes every later gate advisory and
  transfers the owner's decision to a model, which is precisely what SE1 and constraint 7 forbid.
- **A new daemon, queue, session state or `selfdev/*` event kinds.** Rejected: the job contract
  already provides the run, the plan, the verdicts and the artifacts; a parallel carrier would fork
  the truth and need its own recovery story (J1).
- **A global repository write lock instead of derived isolation.** Rejected: it serialises proposing
  as well as landing, so two symptoms wait on each other for no safety gain.
- **Running the proposal in the owner's checkout.** Rejected: the diff would depend on uncommitted
  state outside the stream, and the run could disturb an in-flight change.
- **A live model as the reviewer of its own patch.** Rejected for this contract: the judgement would
  be unrecorded, cache-hostile, and would put a model in the merge decision.
- **Re-verifying the pack on every read or at each step.** Rejected for S1: checks are predicates over
  committed events, and no kian component reads the pack after admission. The inability is recorded as
  a limit instead of being papered over with a claim the runtime cannot keep.

## Consequences

- One document owns proposal authority; the job contract is amended only by the additive surface
  A11–A14 and keeps its invariants J1–J16 unchanged.
- S2 becomes the first self-modifying capability that holds repository credentials; until JOB-MODEL
  carries `exec` with its own contract, S2 is a declaration rather than a phase.
- Review throughput becomes the limiting resource. The trigger stays manual until S2 has produced
  proposal-volume and review-time numbers, so proposal volume cannot outrun the review lane.
- The denial surface is configuration, reviewed like code: changing it is an owner commit, and no run
  has a path to propose a change to it.
- A refused proposal costs a run, not a rollback: a propose run has no repository side effect to undo.

## Limits recorded with this phase

- Specification only: none of the surface in SELF-EVOLUTION-MODEL.md §13 (the `job.selfdev` key, the
  `--pack` flag, the `patch_paths_allowed` check, the `consecutive_failures` limit) exists yet,
  and nothing in this ADR changes runtime behavior.
- The pack is verified at admission only; an edit of its `dir` afterwards is invisible to the
  runtime.
- "No network write to the code host" is enforced by the worker's tool grants, not by the plan: a
  deployment that grants such a tool has weakened SE4 in configuration.
- The breaker is a streak over the last N run verdicts, not a window, so an alternating
  failure/success pattern does not trip it.
- Proposal quality is a human judgement; nothing here measures whether a patch fixes the defect.
- S2's `exec` action, its credentials and its pull-request plumbing are not designed here, and
  cross-repository proposals are out of scope.

## Acceptance

This slice (documentation):

- design/SELF-EVOLUTION-MODEL.md is listed in the AGENTS.md docs map, and every relative link in the
  authored docs resolves (`cargo test -p kian-cli --test docs_map`).
- Every surface the document adds is marked as not implemented, and the document adds no event kind,
  wire route or table (JOB-MODEL §14's additive list and §13's "not added" paragraph stay closed).

S1, when it is implemented (the levels are the ones SELF-EVOLUTION-MODEL.md §16 fixes):

- L1: `patch_paths_allowed` passes a diff inside the surface and refuses a denied path, a path
  outside `allow`, an absolute path, a `..` path, an unparsable diff and a test-only diff, each with
  its reason in the verdict detail; `--pack` refuses a tampered pack before admitting and writes no
  row; the breaker refuses the fourth consecutive failure and `--force` records the override;
  V23–V26 refuse as written.
- L2: a real store seeded with a self-dev run — `kian job run --pack <verified pack>` admits, the
  conductor reaches `deliver`, and the run verdict is `succeeded`; a tampered pack is refused by the
  trigger with exit 2 and leaves no run row.
- L3: one real proposal over a real failure from a local `kian serve`, reviewed by the owner. The
  review is the acceptance, not a green gate; until S1 code exists this level is unverified.

S2 is not accepted by this ADR; it is deferred to the `exec` slice and its own acceptance.
