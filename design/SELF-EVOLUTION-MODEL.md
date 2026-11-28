# Kian — Self-Evolution Model (normative)

Self-evolution here is a **proposal pipeline**, not an autonomous engineer: a self-dev run reads a
committed failure, produces a patch confined to a declared surface, and hands the owner evidence for
review. This document owns the subject, the authority ladder, the denial surface, the concurrency
argument, the budgets, and what a run's outcome means to a reviewer. It adds to
[JOB-MODEL.md](JOB-MODEL.md) and never restates it: a self-dev run **is** an ordinary job run, and
where this document and the job contract disagree, the job contract wins.

## 0. Scope and authority split

| Question | Owner |
|---|---|
| what a run is, how it is driven, what a verdict is | JOB-MODEL.md |
| the event envelope, the kind catalog, the payload schemas | EVENT-MODEL.md |
| the evidence pack format and its verification | docs/SPEC.md §3.1, crates/kian-cli/src/self_evidence.rs |
| the wire, metrics keys, operator routes | API-CONTRACT.md |
| **who may propose what, with what authority, against which denial surface** | this document |

Two rules bind every paragraph below:

1. **No new carrier.** A self-dev run has no table, no daemon, no new event kind, and no new session
   state. Its facts are its own stream plus the pack it was admitted with.
2. **Nothing here weakens a job invariant.** J1–J16 hold unchanged. Where this contract needs a new
   sentence from them it says so explicitly — `job.selfdev`, `patch_paths_allowed`, the `publish`
   action — and records it in §13.

## 1. What self-evolution is, and what it is not

A **symptom** is a real failure of this runtime that a reader can point at: one session and one event
sequence number, usually already packaged by `kian self evidence` (docs/SPEC.md §3.1). A
**self-dev run** turns a symptom into a **proposal**: a minimal patch, confined to a declared
surface, that a human can review and apply. The run's product is evidence.

It is not:

* **an autonomous merger.** No run writes `main`, and no run decides that its change is correct.
* **a production actor.** No run deploys, migrates, rotates credentials, or touches a live store.
* **a scheduler with a taste for refactors.** A self-dev run is opened for a symptom, and its plan
  must name the symptom it answers.

The boundary is this repository's honesty rule (AGENTS.md constraint 7) applied to code: a green gate
on a patch is a *claim about the patch*, not proof that the defect is gone. Phase S1 therefore makes
no claim the stream cannot check (§8).

## 2. The subject: a self-dev run is a job run

```text
symptom (pack) —► trigger (manual) —► job run (frozen plan, conductor)
                                          ├─ diagnose   agent step → diagnosis artifact
                                          ├─ patch      agent step → patch artifact
                                          └─ deliver    deliver step → owner receipt
                                 run verdict (gate run) + step verdicts
```

* The run is a session whose first input carries the plan and its digest (JOB-MODEL §5.2); the
  conductor (crates/kian-superagent/src/job/conduct.rs) drives it; the plan is frozen at admit (J2);
  the run verdict is an `evidence/verdict` at gate `run` written before the final outcome
  (JOB-MODEL §5.5).
* **The trigger is manual in S1 and S2.** A cron trigger for a self-dev job is a later decision,
  taken only after S2 has produced review-throughput numbers; an unattended proposal stream with no
  merge lane produces review debt, not evolution.
* **The job is an ordinary declaration** in `jobs.jobs` (crates/kian-cli/src/job_config.rs:51).
  The denial surface lives inside the plan as a check parameter (§5.4), so it is covered by
  `plan_digest` and cannot be edited by a run.
* **No new verb for "evolve".** `kian job run` (crates/kian-cli/src/job_cli.rs:158) gains flags
  (§15); `kian job status` already renders the run.

### 2.1 Terms

| Term | Definition |
|---|---|
| **symptom** | A committed failure identified by `(session, seq)` and packaged as an evidence pack. |
| **evidence pack** | The five-file package `kian self evidence` writes: `events.ndjson`, `session.json`, `repo.json`, `inspect.json`, `manifest.json`, with the manifest's digests. |
| **proposal** | The run's output: a diagnosis artifact, a patch artifact, and the run's verdict. |
| **patch artifact** | A unified diff in the producing attempt's committed final message, capped by `produces.max_bytes`. |
| **declared surface** | The `allow`/`deny` path lists of a `patch_paths_allowed` check, frozen in the plan. |
| **denial surface** | The `deny` half: paths a self-dev patch may never change (§5.4). |
| **review gate** | The owner's pull-request review. The only merge channel. |
| **landing** | Merging a proposal into a release branch. Never a run action. |
| **iteration** | One self-dev run. Iterations do not accumulate authority: run *n+1* has exactly the authority of run 1. |

## 3. Invariants

**SE1 — A run proposes, never lands.** No self-dev run writes a protected branch, merges a pull
request, deploys, or edits its own configuration. *Why:* the merge is the owner's decision point, and
a run that could merge would make every later gate advisory.

**SE2 — The symptom is admitted by digest.** A run is admitted only with a pack that verified
(§4.1), and the admitted row carries the pack's digests. The runtime verifies once, at admission, and
the digests are the record of what the run was pointed at — the run never claims a later read
matched them, because no kian component reads the pack after admission (§17). *Why:* constraint 2
applied to inputs — a candidate that is admitted without a verified handoff cannot be reviewed, and
an unverified re-read would be a different symptom under the same run.

**SE3 — The patch is inside the declared surface, checked over committed bytes.** The
`patch_paths_allowed` check evaluates the harvested patch artifact, not the child's claim about it
(§5.3). A refusal is a failed attempt with the offending paths in the verdict detail.

**SE4 — No credentials in a propose run.** S1 places no step on a worker holding repository write
credentials or deployment secrets, and no step performs a network write to the code host. S2 scopes
credentials to one push of one derived branch (§7).

**SE5 — The run's outcome never depends on a human decision.** A run does not wait for a review, a
merge, or an approval: it ends when its plan ends. Waiting for a human would break J6 (park is for
unfinished plan work) and J11 (a deadline bounds a run, not a person).

**SE6 — Audit is a chain of digests.** pack `files_sha256` → `plan_digest` → patch `sha256` →
(S2) branch, commit, pull request → the owner's merge commit. Every link is recomputable from
committed bytes; no link depends on a chat transcript or on a file that can be edited later.

**SE7 — A refused proposal is a fact.** Every refusal — pack mismatch at admission, cap exceeded,
path denied, test-only patch, no worker, exhausted breaker — is a trigger refusal (exit 2, no row) or
a verdict or a run-level failure with a reason code. Nothing is dropped silently and nothing "runs
anyway" (J16).

**SE8 — A test-only patch is not a proposal.** A patch that changes only test files does not answer a
symptom; the check refuses it. Tests may accompany a code change, at any size.

**SE9 — Authority does not accumulate across iterations.** Iteration *n+1* is admitted with the same
job declaration, the same surface, and the same budgets as iteration 1. "It worked last time" is not
an input.


## 4. Admission: the symptom arrives as a pack

### 4.1 The trigger verifies before it admits

`kian job run <job> --pack <dir>` calls `verify_pack` (crates/kian-cli/src/self_evidence.rs:568)
before any inbox row exists. A pack that fails verification — wrong `format_version`, a missing
file, a byte-length or sha256 mismatch, an aggregate digest mismatch, an `events.ndjson` line that
is not an `Event` — is **refused with exit 2 and no admission**. There is no "admit anyway": a run
over bytes the runtime already knows are not the bytes the manifest names is not evidence.

The verified facts are re-derived in the trigger, not trusted from the command line:

```text
manifest_sha256 = sha256: + sha256(manifest.json bytes)
files_sha256    = sha256: + manifest.files_sha256  (the per-file digest fold)
base_commit     = manifest repo commit, or the --base override
```

A pack whose `repo.json` has no commit (it was taken outside a git checkout) and no `--base`
override is refused: a proposal with no base cannot be applied, and inventing one would break SE6.

### 4.2 What the admitted row carries

One additive key beside the job marker (JOB-MODEL §14 A1):

```json
{
  "job": {
    "run_id": "self-dev@manual:01J8…",
    "job_id": "self-dev",
    "slot": "manual:01J8…",
    "trigger": "manual",
    "plan_digest": "sha256:…",
    "plan": { "steps": [], "limits": {} },
    "selfdev": {
      "pack": {
        "dir": "/Users/owner/evidence/self-dev@manual:01J8…",
        "manifest_sha256": "sha256:…",
        "files_sha256": "sha256:…"
      },
      "base_commit": "9f3c…",
      "kian_version": "0.3.0"
    }
  }
}
```

Rules:

1. `job.selfdev` is **not optional** for a job whose plan declares a `patch_paths_allowed` check
   (V24): such a job admitted without a symptom is `invalid_job_input` and the conductor fails the
   run (§8.3).
2. The key is bounded: `dir` is at most 1024 bytes, digests are hex, and the whole `selfdev` value
   is at most 4 KiB. An over-long value is an admission refusal, not a truncation.
3. `dir` is a **reference**, not a copy: the pack stays where the owner put it, and the run does not
   copy it into the stream. The digests describe the bytes that were verified at admission; an edit
   of `dir` afterwards is invisible to the runtime and is recorded as a limit (§17), not papered
   over with a re-read.
4. The key is immutable for the run's lifetime. There is no "update the pack" verb; a new symptom is a
   new iteration.

### 4.3 The child sees the symptom, deterministically

A job child's task message already opens with a deterministic header —
`[kian-job] run=… job=… step=… attempt=… plan=… step_digest=…`
(`crates/kian-superagent/src/job/conduct.rs:1438`). A self-dev run appends one more block to that
header: the frozen `job.selfdev` value, at most 4 KiB, before the step prompt. The result is
byte-identical on every re-drive (JOB-MODEL §8.4, J2), which extends J13's "built from the frozen
step spec" to "built from the frozen step spec and the frozen run input": both are committed before
the child exists, and neither can change under a retry. The pack's **content** is not inlined — the
child reads the files from `dir`, which is why the run's placement requires a worker that can see
that path (§6.1).

## 5. The proposal plan

### 5.1 The canonical S1 plan

```yaml
jobs:
  jobs:
    - id: self-dev
      limits:
        deadline_ms: 3600000        # one hour for a proposal, hard stop
        max_parallel: 1
        consecutive_failures: 3     # §10
      plan:
        - id: diagnose
          action: agent
          requires: [repo.kian]
          prompt: |
            You are diagnosing one committed failure of this runtime. The evidence pack
            named in your task's selfdev block is the whole record: read manifest.json,
            session.json, inspect.json and the events slice. State the defect, the smallest
            change that would fix it, and the files that change. Answer in markdown, at
            most 60 lines. Do not propose a change to any path the plan denies.
          produces: {name: diagnosis, media: text, max_bytes: 8192}
          timeout_ms: 300000
          retries: 1
          effect: read
          verify:
            - {check: final_message_present}
            - {check: artifact_within_cap, name: diagnosis}

        - id: patch
          action: agent
          depends_on: [diagnose]
          requires: [repo.kian]
          inputs: [{step: diagnose, name: diagnosis}]
          prompt: |
            Apply the smallest change that answers the diagnosis. Work in a scratch worktree
            of the repo at the base commit named in your task, never in the checkout itself.
            Emit exactly one unified diff (git diff format) as your final message: it is the
            artifact, and it is the only thing reviewed. No prose after the diff.
          produces: {name: patch, media: text, max_bytes: 262144}
          timeout_ms: 900000
          retries: 1
          effect: read
          verify:
            - {check: artifact_within_cap, name: patch}
            - {check: patch_paths_allowed, artifact: patch,
               allow: ["crates/**", "docs/**", "design/**", "README.md",
                       "examples/**", "site/**"],
               deny:  ["design/decisions/**", "kian.yaml", "scripts/**", ".github/**",
                       "AGENTS.md", "design/SELF-EVOLUTION-MODEL.md",
                       "**/*.pem", "**/*.key", "**/.env", "**/credentials*"]}

        - id: deliver
          action: deliver
          depends_on: [patch]
          optional: true
          channel: imessage
          target: {handle: "+8613800000000"}
          content:
            text: |
              A self-dev proposal is ready in the kian stream — NOT MERGED.
              Read it with `kian job status <the run id the trigger printed>`;
              the patch is an artifact on the run, not a message.
          verify:
            - {check: delivery_receipt, channel: imessage, target: {handle: "+8613800000000"}}
```

What the declaration makes explicit:

* every step is `effect: read`; a proposal run writes nothing outside a worker's scratch space;
* the patch step's evidence is the diff's **paths**, not the child's summary;
* delivery is `optional: true` so a channel failure cannot erase a proposal that exists (§9.3);
* the delivery is a nudge, not the record: `DeliverContent.text` is literal today
  (`crates/kian-superagent/src/job/plan.rs:355`), so the message cannot name the run — the trigger
  prints the run id and `kian job status` is the record. A channel is not a review surface, and
  `from_artifact` delivery does not exist yet (`crates/kian-superagent/src/job/conduct.rs:1345` fails
  it), so no diff is ever sent through a channel.

### 5.2 The patch artifact

A unified diff, `media: text`, cap 256 KiB, harvested from the producing attempt's final
`assistant/message` (JOB-MODEL §12.1). The harvest rule is the existing one: the artifact **is** the
committed bytes, with `sha256` over them and a `ref` into the child's stream. Over-cap is a failed
attempt, never a truncation (J10).

The diff must be self-contained: a partial diff that assumes uncommitted state in the worker's
worktree is unreviewable, and the check refuses a patch whose headers do not name repository-relative
paths.

### 5.3 `patch_paths_allowed`: the surface check

A job-owned check evaluated by the conductor over the harvested artifact bytes (the job-owned list in
JOB-MODEL §11.2; kian-loop is not modified). It passes iff:

1. every changed path in the diff is **repository-relative** after normalisation: a leading `a/`,
   `b/` or `./` is stripped, and an absolute path, a `..` segment, or a path that escapes the
   root fails the check;
2. every changed path matches at least one `allow` entry and no `deny` entry, with `**` matched
   against the whole path and `deny` winning over `allow` when both match;
3. at least one changed path is **not** a test path (SE8), where a test path is any path under a
   `tests/` directory and any file whose name ends in `_test.rs`, `.test.ts`, `.test.js`, or
   `.snap`;
4. the diff parses: every hunk belongs to a `---`/`+++` pair or to a `rename to`/`copy to`
   line. Unparsable bytes fail the check — a surface that cannot be read has not passed.

The failure detail names the refused paths (bounded to 20) and the rule that refused them
(`outside allow`, `denied`, `test-only`, `unparsable`). The patch artifact stays on the
stream: the owner sees exactly what was refused.

Creation and deletion are ordinary paths (`/dev/null` on one side): a new file must be inside the
surface, and so must a deleted one. A rename is checked on both the old and the new path.

### 5.4 The denial surface

The default deny list is the one in §5.1. Its members and reasons:

| Denied | Why |
|---|---|
| `design/decisions/**` | ADRs are append-only here; a proposal may not rewrite a decision record. |
| `design/SELF-EVOLUTION-MODEL.md`, `AGENTS.md` | The rules of the game belong to the owner; a run that can edit them can authorise itself. |
| `kian.yaml` | Configuration carries authority (channels, policy tables, jobs). A proposal that edits it can widen its own future surface. |
| `scripts/**`, `.github/**` | The gates and their wiring. A patch that can relax its own gate is not evidence. |
| `**/*.pem`, `**/*.key`, `**/.env`, `**/credentials*` | Secrets never enter a diff, a stream, or a channel. |
| `tests/` (as the *only* change) | SE8: a patch that only edits tests answers nothing. |

The list is **declared in the plan**, so it is frozen at admit and reviewed like any other
configuration change. Changing it is an owner commit, not a run action: a run has no path by which it
can propose a change to its own denial surface.

## 6. Placement and the worker's world

### 6.1 The capability the plan names

The plan requires `repo.kian`: a worker that holds a checkout of this repository with `git` and
read access to the pack's `dir`. A worker that cannot see the pack is not a candidate, and a plan
whose candidate set is empty fails `no_candidate` (J16) rather than fabricating a patch.

S1 uses no other capability and no credential. Publishing is a **conductor** action (§9.4), so it is
never placed: there is no worker candidate, no capability and no binding behind it, and repository
write authority is the conductor's own scoped credential (S2b, §7), never a capability a worker could
acquire.

### 6.2 What a worker may do while proposing

The proposal steps are ordinary agent turns on an ordinary worker: they may run commands locally in
their own scratch space (that is how a diff is produced) and may write nothing outside it. The
worker's tool grants are the enforcement point for "no network write to the code host" — a deployment
that grants such a tool to a proposing worker has weakened SE4 in configuration, and this document
says so rather than pretending the plan enforces it. The publish action's own network write is not a
worker act at all: it is the conductor's, bounded by §9.4 and one push-scoped credential (S2b).

## 7. The authority ladder

| Phase | A run may | A run may not |
|---|---|---|
| **S1 propose** (this contract) | read committed streams; read the pack; produce artifacts in a worker's scratch space; notify the owner | write a repository; push; open a pull request; hold a credential; deploy |
| **S2a publish-local** (landed) | create one derived worktree and branch; apply the patch artifact; commit with the audit trailers; re-run the configured scoped gates as a step | push; open a pull request; hold any credential; write `main`; merge; edit another run's branch |
| **S2b publish-remote** (landed) | push that branch once (never forced); open one pull request whose body carries the audit chain | force-push; write `main`; merge; edit another run's branch; read the credential from anything but the configured environment variable |
| **S3 land** (landed) | nothing beyond S2b; the owner merges the reviewed pull request and the runtime verifies it | auto-merge; force-push; tag; deploy; land without review; write the merge on the run's behalf |

**S2's enabling amendment** is a conductor-owned `publish` step action (JOB-MODEL §7.2 and §13.5),
not a general-purpose `exec`: the sequence — worktree, apply, commit, gates, push, pull request —
is fixed, and every step leaves a receipt on the run stream (§9.4). S2a needs no credential at all;
S2b adds the one-push credential, read from the environment at publish time and never carried in an
argument or an event. ADR-071 records why the `exec` route was rejected. The two phases are one
fixed sequence behind one port: S2a is the five local steps and S2b adds the push and the pull
request, with the credential the only thing that distinguishes them.

**S3 is the owner's merge, verified rather than executed.** The review gate is the pull request and
the merge is the owner's act; S3 adds no `land` verb, no auto-merge and no new write authority
(ADR-075). What it adds is evidence: `kian self verify <run>` recomputes every link of the audit
chain from the run streams and the local repository, and `--merged` reports whether the merge
happened and whether it preserved the proposal's commit. Landing stays a human decision on the pull
request; the runtime only makes the record checkable. The merge shape is normative (§18.2): a merge
commit, never a squash or a rebase, because only the merge commit keeps the artifact commit — and
its trailers — in the base branch's history.

## 8. Evidence, outcomes, and the review gate

### 8.1 What `succeeded` means

A self-dev run's outcome is the ordinary job outcome (JOB-MODEL §11.3), and it means exactly this:

```text
succeeded  a reviewable proposal exists, inside the declared surface, with a passing step verdict
unverified at least one step produced no evaluable check (never folded into succeeded)
failed     a check refused, a step failed, or the run could not be driven to a proposal
cancelled  a cancel row was consumed before the run finished
```

`succeeded` does **not** mean "the change is right", "the defect is fixed", or "the patch builds".
The stream cannot prove those, and this document does not let a summary imply them. The delivery text
(§5.1) says "NOT MERGED" for the same reason.

### 8.2 The run never waits

The review is a pull-request review (S2) or an owner reading the artifacts (S1). Neither is a fact a
run can observe, and a run that parked on one would hold a drain open until its deadline and then
report `timed_out` for a decision it was never going to see. So the run ends at its proposal; the
review happens after the stream is closed, and the merge is not a run fact. This is a deliberate
trade — a run cannot report "my proposal was accepted" — and it is why SE6 puts the audit chain in
the pull request body rather than in a verdict written later (§11).

### 8.3 Outcome table

| Situation | Where recorded | Outcome |
|---|---|---|
| pack digest mismatch at admission | the trigger's stderr | exit 2; no inbox row, so there is no outcome |
| patch over 256 KiB | step verdict, gate `step:patch` | attempt `failed` (never truncated) |
| patch touches a denied path | step verdict, gate `step:patch` | attempt `failed`, detail names the paths |
| patch changes only tests | step verdict, gate `step:patch` | attempt `failed`, `test-only` |
| patch does not apply to the frozen base commit | step verdict, gate `step:publish` | attempt `failed`, git's stderr in the detail |
| the derived worktree path already exists | step verdict, gate `step:publish` | attempt `needs_operator`, the path in the detail, nothing deleted |
| the scoped gates fail | step verdict, gate `step:publish` | attempt `failed`, the gate's exit code and bounded output tail |
| publish is not configured | step verdict, gate `step:publish` | attempt `failed`, reason `publish is not configured` |
| no worker declares `repo.kian` | run-level verdict | run `failed`, reason `no_candidate` |
| breaker open at admission | nothing written | exit 2 from `kian job run` (§10) |
| admitted, conductor never ran | no run verdict | the sweep settles `failed` (existing rule) |

## 9. Concurrency and write isolation

### 9.1 S1 has no shared writer

A propose run writes no repository ref and no shared file. Two propose runs over two symptoms cannot
interfere: their artifacts live on their own streams, and their workers' scratch space is their own.
No lock is required, and adding one would invent a second authority for no gain.

### 9.2 S2 isolates writes by derivation

Every write a publish run performs is named by a value derived from the run id (JOB-MODEL §6):

```text
branch        selfdev/<run_session>            (never a long-lived branch)
worktree      <repo_root>/.agents/worktrees/selfdev-<run_session>
pull request  head = that branch, base = main
```

Two runs therefore never name the same ref or the same path: the branch is unique per run, and
`git worktree add` fails atomically when the path exists. That failure is the fact — a stale
worktree is `needs_operator` with the path in the detail, and the run never deletes a directory it
did not create in the current attempt.

**The refinement this contract makes to its own premise:** "one writer per repository at a time" is
required at **landing** (the owner's merge of `main`), not at proposing. Proposals are naturally
parallel and disposable; serialising them buys nothing and costs throughput. The merge stays
single-writer because it is the owner's action (SE1).

### 9.3 The deliver step

Delivery is the conductor's own action (JOB-MODEL §13.1) with the existing receipt rule (J14). A run
whose delivery cannot land still ends with its verdict: the proposal exists on the stream, and the
owner can read it with `kian job status`. A failed delivery is recorded on an `optional` step so
it cannot erase the proposal's outcome (JOB-MODEL §7.2).

### 9.4 The publish step, step by step

Publishing is the conductor's own action, the same shape as delivery (JOB-MODEL §13.1): a fixed
sequence whose every step needs a receipt, run in the run's own drain, never placed on a worker. The
plan names the artifact it consumes; the runtime derives every other name, and the base commit was
frozen at admission.

```text
 1. from     := artifact(step.inputs[0])          # {step, name, sha256, bytes, ref}; already checked
 2. base     := job.selfdev.base_commit           # frozen at admission (A11): --base, else the
                                                  # pack's own repo.json commit
 3. branch   := "selfdev/" + run_session
    worktree := <repo_root>/.agents/worktrees/selfdev-<run_session>
 4. call_id  := "job:publish:<step>:<attempt>"
    append tool/call {call_id, tool: "job.publish",
                      args: {step, attempt, from, base_commit, branch, worktree}}
 5. read the artifact's bytes from the producer's stream (read-only) and re-check "from.sha256"
 6. git worktree add -b <branch> <worktree> <base>; git apply --index <patch>
 7. git commit -m <subject> -m <audit trailers>
 8. run the configured gate command inside <worktree> (§9.5)
 9. append tool/result {call_id, ok, result: {branch, worktree, commit, gate}}
```

The receipt is step 9, and the `publish_ok` check (JOB-MODEL §11.2) reads it:

* `ok: false` settles the attempt `failed` with the reason in the detail;
* `ok: true` requires a 40-hex `commit`, the derived `branch`, and — when `gates: true` — a gate
  with `exit_code: 0`;
* an intent with no receipt settles `needs_operator`, the same rule as delivery: the conductor
  started a write it cannot see the end of, and deleting the worktree would destroy the only evidence
  of how far it got.

Three properties fall out of the sequence rather than out of a lock:

* **The digest is re-checked at the write.** Between admission and the publish the artifact's bytes
  are read from the stream again and compared to the frozen `sha256`; a mismatch settles the attempt
  `failed` and publishes nothing, so a commit can only ever contain bytes the plan's checks saw.
* **The worktree is the isolation.** Every step runs with its current directory inside the derived
  worktree, so a publish never touches the operator's checkout, and `git worktree add` failing on
  an occupied path is the fact that makes two runs unable to share one (§9.2).
* **A publish is never retried.** `retries: 0` (V27), because a failed attempt leaves its worktree
  behind: a retry would find the path occupied and could never succeed. An operator who wants another
  attempt removes the path — an explicit act, not a retry.

Steps 4–9 extend with `push` and `pull_request`: `git push <remote> HEAD:refs/heads/<branch>`
(no `--force`, so an existing branch refuses), then `gh pr create --base <base> --head <branch>`,
whose body carries the audit chain (§11). The credential is read from `selfdev.publish.token_env`
at publish time, exported only into those two child processes — every other child runs with the
variable removed — and never written to an argument or an event; its absence fails the step with
`credential <name> is not set` before the worktree exists. A push that landed while its pull
request failed is `Partial`: the attempt settles `needs_operator`, the branch stays on the remote,
and no receipt is written.

### 9.5 The gate command

The gate is configuration, not plan content: `selfdev.publish.gate` names one command (default
`./scripts/selfcheck.sh`) that the conductor runs inside the derived worktree with a bounded timeout
and a bounded output tail. It exits 0 or it does not; there is no parsing of its output and no
"warning means pass". A plan cannot choose a different gate — the denial-surface argument (§5.4)
applies to the evidence as well: a run that could pick its own gate could pick a trivial one.

The gate is the dominant cost of a publish (a cold worktree builds the workspace), which is why
`selfdev.publish.target_dir` can point `CARGO_TARGET_DIR` at an existing build cache, and why the
step has `retries: 0`: paying the build twice is the failure mode to avoid, not the retry. Point that
directory at a cache no other working tree builds into: cargo replaces binaries in place, so two
trees sharing one target dir each end up with artifacts built from the other's sources.

## 10. Budgets and the circuit breaker

### 10.1 The job's own limits

`deadline_ms` bounds the whole run (J11); `retries` bound each attempt; `effect: read` lets a
proposal retry freely. S1 needs nothing more: the cost of a proposal is two agent turns. S2a adds
one gate run per publish — the expensive half of the phase — so the limit that matters there is the
step's `timeout_ms` and the gate's own bound, not a retry count (§9.5).

### 10.2 The consecutive-failure breaker

A job limit, `consecutive_failures` (default 3, 0 disables it, 10 is the maximum — V25):

```text
admit(job, manual):
  if job.limits.consecutive_failures > 0
     and the last N settled runs of this job (through the projection `kian job list` uses)
         are all in {failed, needs_operator}:
       refuse: selfdev_circuit_open — nothing is written, exit 2
  unless --force: admit once and record circuit_overridden: true in job.selfdev
```

The breaker exists because a self-dev job's failure mode is **expensive repetition**: the same bad
patch proposed nightly, each run paying for a model turn. It is a **trigger** rule, not a conductor
rule — the conductor never refuses a run it was given (J5: the conductor is the only spawner, and
admission belongs to the control plane).

Clearing the breaker is an owner act: either a passing proposal settles it naturally (the last N are
no longer all failures), or `--force` admits one run and records that a human overrode a fact. The
breaker has no timer and no reset verb; a wall-clock reset would hide the fact that nothing improved.

## 11. Audit chain

```text
pack manifest_sha256 ──┐
plan_digest ───────────┼──► run stream (input row, verdicts, artifact refs, publish receipt)
patch sha256 ──────────┘              │
        │                             └── (S2a) commit ──► branch selfdev/<run_session>
        └── (S2b) pull request body ──► the owner's merge commit
```

* Every link is a digest over committed bytes. The stream holds the first three; the commit message
  carries `Kian-Run`, `Kian-Plan-Digest`, `Kian-Patch-Sha256` and `Kian-Pack` trailers, so the chain
  survives a copy of the branch; the pull request body carries the same values (S2b), so the chain
  survives outside the database.
* The merge commit is the one link this repository does not write, which is the point of SE1: the
  owner's merge writes it, and S3 verifies it instead of performing it (§18).
* Reading the chain is a read: `kian job status <run>` renders the run and its publish receipt, and
  the pack is re-verifiable with the same verifier `kian self evidence` uses. `kian self verify
  <run>` recomputes the chain link by link — the frozen plan, the artifact bytes, the commit's
  parent, diff and trailers, and with `--merged` the merge — without writing an event, a row or a
  ref. No new reader state is introduced.

## 12. Crash windows, walked through

| Crash point | After recovery | Why it is safe |
|---|---|---|
| after pack verification, before admit | no row exists; the owner runs again with the same `--pack` | admission is the only writer, and it is one append |
| after admit, before the conductor ticks | the run exists, unclaimed; the next tick drives it | admission is durable before any wake (constraint 6) |
| mid diagnose attempt | a retry is a new attempt with a new child (J8), same prompt bytes | the step digest covers the frozen step (§7.3) |
| after the patch artifact is harvested, before the step verdict | the next tick re-evaluates checks over the same committed bytes | the artifact is a reference; the check is pure |
| after the publish `tool/call`, before its `tool/result` | the next tick sees an intent with no receipt and settles `needs_operator`; nothing is deleted | a started publish is never silently repeated (§9.4) |
| after the commit, before the gates | same: `needs_operator` with the commit in the detail; the worktree and branch stay as evidence | the receipt is written once, after the gates |
| S2b: after the push, before the pull request | `needs_operator`; the branch exists on the remote and the owner can open the pull request by hand | the push is per-branch and never forced |
| S3: after the owner merges, before the verifier runs | nothing to recover; the merge exists in git and `kian self verify --merged` reports it | the verifier is read-only and writes no state |
| after the step verdict, before the run verdict | the run verdict is written once before the final outcome; a re-drive sees it and does not rewrite | the last `run` verdict wins (§11.1), so a settled outcome is stable |
| after the run verdict, before `session/ended` | the engine's end write is atomic (constraint 4); the sweep settles from the verdict | settlement never reads the bare end reason |
| delivery fails after the proposal exists | the deliver step is `optional`; the run still ends with its verdict | the proposal is not lost (§9.3) |

## 13. The additive surface

| # | Surface | Shape | Writer | Reader |
|---|---|---|---|---|
| A11 | `input/admitted.job.selfdev` | `{pack {dir, manifest_sha256, files_sha256}, base_commit, kian_version, circuit_overridden?}` | the manual trigger | the conductor (validation), the child task builder, the status projection |
| A12 | `kian job run --pack <dir> [--base <commit>] [--force]` | flags on the existing verb | operator | — |
| A13 | check `patch_paths_allowed{artifact, allow, deny}` | a job-owned check in `verify` | the plan | the conductor's evaluator |
| A14 | limit `consecutive_failures` | `u32`, default 3, at most 10 | the plan's `limits` | the manual trigger |
| A15 | step action `publish` and its field `publish {base?, gates?, push?, pull_request?}` | a frozen plan shape; the field serializes only when present, so an existing plan's digest does not move | the plan | the conductor (dispatch), validation |
| A16 | run-stream pair `tool/call` / `tool/result` with `tool: "job.publish"` | args `{step, attempt, from, base_commit, branch, worktree}`; result `{branch, worktree, commit, gate {command, exit_code, digest, output_tail}, push {remote, ref}, pull_request {base, head, url?}}` — the last two only when the plan asked for them | the conductor | the `publish_ok` check, the status projection |
| A17 | check `publish_ok` | a job-owned check in `verify` | the plan | the conductor's evaluator |
| A18 | config `selfdev.publish` | `{repo, base, remote, gate, target_dir, author {name, email}, token_env}`; absent means the step fails honestly | the operator | the publish port |
| A19 | the remote half of the receipt | `push {remote, ref}`, `pull_request {base, head, url?}` and the credential's environment variable name; no new route and no new verb | the plan, the config | the publish port |
| A20 | verb `kian self verify <run> [--merged] [--base <branch>] [--config <path>] [--json]` | a read-only chain check: exit 0 when every link holds, exit 2 naming the broken link; no event, no row, no ref write | — | the operator |
| A21 | the landing record | the owner's merge commit: the artifact commit is a parent, so the trailers and the pull request reference stay in history; the runtime adds no trailer and no event | the owner's merge | the verifier (`--merged`) |

Not added, explicitly: no event kind, no table, no HTTP route, no JSON-RPC method, no SSE frame, no
new crate in the narrow waist, and no change to a job verdict's shared shape. The verifier (A20)
adds no route either: it is a local read of the streams and the repository. The publish pair uses
the existing `tool/call` / `tool/result` kinds, `evidence/verdict` stays `{gate, passed, checks[],
attempt}` plus the job-owned `job` key, `patch_paths_allowed` and `publish_ok` are entries in
`checks[]`, and no credential ever appears in an event.

## 14. Validation rules

| # | Rule | Refusal |
|---|---|---|
| V23 | `patch_paths_allowed` names a produced artifact, declares a non-empty `allow` and a non-empty `deny`, and its entries are repository-relative globs without `..` or a leading `/`; at most one such check per step | invalid surface |
| V24 | a job whose plan declares any `patch_paths_allowed` check must be admitted with `job.selfdev` (`--pack` is required for such a job) | selfdev job requires an evidence pack |
| V25 | `consecutive_failures <= 10` | invalid breaker |
| V26 | `kian job run --pack` verifies before admitting; a mismatch, a missing file, an unreadable `repo.json` commit with no `--base`, or an over-long `dir` refuses with exit 2 and writes nothing | evidence pack does not verify: <reason> |
| V27 | a `publish` step names exactly one input that resolves to a produced artifact, sets `retries: 0`, `effect: write`, `idempotent: false`, declares none of `prompt`, `requires`, `pin`, `prefer`, `allow_offline`, `produces`, `channel`, `target`, `content`, and names its producer in `depends_on` | invalid publish step |
| V28 | at most one `publish` step per plan | invalid publish step |
| V29 | a plan with a `publish` step declares at least one `patch_paths_allowed` check over the same artifact | publish requires a declared surface |
| V30 | `--base`, when given, is a full 40-hex commit; the admitted `job.selfdev.base_commit` is that value, else the pack's `repo.json` commit | invalid base commit |
| V31 | `publish.pull_request: true` requires `publish.push: true`; `publish.base` is a non-empty branch name without whitespace (default `main`) | invalid publish target |
| V32 | `selfdev.publish.repo` resolves to a git work tree and `selfdev.publish.gate` is a non-empty command; a publish step with no `selfdev.publish` section is admitted but its attempt fails `publish is not configured` | invalid publish config |

V24 is the rule that keeps the model honest: a self-dev plan without a symptom would be a general
"improve the code" prompt with a conductor attached.

S3 adds no validation rule: landing is not an admission decision, so the verifier reads a run that
already exists (§18).

## 15. The verbs

| Verb | Behaviour |
|---|---|
| `kian job run <job> --pack <dir> [--base <commit>] [--slot <token>] [--force] [--json]` | verifies the pack, checks the breaker, admits one run, prints the run id |
| `kian job status <run> [--json]` | unchanged; a self-dev run renders like any other run, and a settled publish step renders its branch, commit and gate |
| `kian self evidence --session <uuid> --seq <n> --out <dir>` | unchanged; the producer of the pack |
| `kian self verify <run> [--merged] [--base <branch>] [--config <path>] [--json]` | recomputes the audit chain of one self-dev run from its streams and the local repository; read-only, exit 0/2 (§18.3) |

Parsing follows the hand-rolled shape in crates/kian-cli/src/job_cli.rs:158 (the same `next_value`
discipline); `kian self verify` lives beside `kian self inspect` and `kian self evidence` in
crates/kian-cli/src/main.rs, opens the store read-only and touches the repository only through
`cron_worktree::git_command()`. The verb appears in `USAGE` beside the other `kian self` lines,
and `kian job run`'s `--json` `run_id` field is unchanged.

## 16. Phases and acceptance

### S1 — propose (this contract)

Acceptance, by level:

* **L1** — `patch_paths_allowed` passes a diff inside the surface and refuses a denied path, a path
  outside `allow`, an absolute path, a `..` path, an unparsable diff, and a test-only diff, each
  with its reason in the detail; `--pack` refuses a tampered pack before admitting and writes no row
  (asserted by reopening the store); the breaker refuses the fourth consecutive failure and records
  `circuit_overridden` under `--force`; V23–V26 refuse as written.
* **L2** — a real store seeded with a self-dev run: `kian job run --pack <verified pack>` admits,
  the conductor reaches `deliver`, and the run verdict is `succeeded`; a tampered pack is refused by
  the trigger with exit 2 and leaves no run row, service side unchanged.
* **L3** — one real proposal over a real failure from a local `kian serve`, reviewed by a human.
  The pull-request half is the S2 line.

S1 is explicitly not accepted by "the model produced a plausible diff": a plausible diff outside the
surface is a failed attempt (§5.3).

### S2a — publish-local

Acceptance, by level:

* **L1** — V27–V32 refuse as written; a fake publish port proves the conductor's three outcomes: a
  passing receipt settles the step `succeeded`, a failing one `failed` with the gate in the detail,
  and an intent with no receipt `needs_operator`; an artifact whose bytes no longer match its frozen
  `sha256` is refused before anything is written; `publish_ok` rejects a receipt with a short
  `commit`, a foreign `branch` or a non-zero gate.
* **L2** — against a temporary git repository: the worktree lands at the derived path, the branch at
  the frozen base, the commit contains exactly the artifact's bytes, the gate runs inside the
  worktree (exit 0 and exit 1 cases), an occupied path is `needs_operator`, and a patch that does
  not apply fails with git's stderr in the detail.
* **L3** — one real publish over this repository: a commit on `selfdev/<run_session>` whose diff is
  the reviewed artifact and whose trailers carry the chain, with the scoped gates re-run as the step.

### S2b — publish-remote

Acceptance, by level:

* **L1** — `publish_ok` refuses a receipt that carries no push, names a foreign ref or a foreign
  head; a missing credential fails the step before the worktree exists.
* **L2** — against a temporary repository with a local bare remote and a stub `gh`: one non-forced
  push creates the branch, a second attempt on the same branch refuses, the pull request body
  carries the audit chain, and only the push and the pull request child processes see the
  credential.
* **L3** — one real publish against the operator's remote and an authenticated `gh`; the local
  acceptance uses a bare remote and a stub.

### S3 — land (landed)

The phase has one slice: a read-only verifier. The merge stays the owner's, and no `land` verb
exists (ADR-075). The S2 numbers still gate any write automation; they do not gate the verifier,
which adds no authority.

* **L1** — chain fixtures: a matching run verifies; a tampered artifact byte, plan digest, trailer,
  short commit, foreign branch or non-zero gate each fails naming the link; a run with no
  `job.selfdev` or no publish receipt fails closed; a missing `selfdev.publish.repo` fails
  `landing is not configured`.
* **L2** — a temporary repository and a real run stream: the chain verifies; `--merged` passes
  after a merge commit and fails `not merged into <base>` before it; a squash merge fails
  because the artifact commit is unreachable; a deleted local branch still verifies when the
  commit is reachable from the base.
* **L3** — one real proposal published by S2b, reviewed and merged by the owner, then
  `kian self verify --merged` over this repository.

## 17. Non-goals and open questions

Non-goals for S1–S3: automatic merge; deployment; cross-repository changes; using a live provider
to "judge" a patch; rewriting test baselines; editing this document, the gates, the ADRs or
`kian.yaml`; changing the event vocabulary; training across iterations.

Seven limits are recorded rather than deferred, because a reader must not mistake a declaration for
an enforcement point:

* the pack is verified at admission only, so a later edit of `dir` is not detected (§4.2);
* "no network write to the code host" is enforced by the worker's tool grants, not by the plan
  (§6.2) — a deployment that grants such a tool has weakened SE4 in configuration;
* a failed publish leaves its worktree and branch behind, because the run never deletes a directory
  it did not create in the current attempt (§9.4): the operator's cleanup is the price of the rule;
* a push that landed while its pull request failed leaves the branch on the remote: the attempt
  settles `needs_operator` (§12), because the run never force-pushes or reuses a branch, and the
  operator either opens the pull request by hand or deletes the branch;
* the verifier reads local git only: it never fetches, so a commit that exists only on the remote
  is reported missing rather than retrieved (§18.3);
* it does not prove that a review happened, that the gate covered the change, or that the patch
  is correct: it proves the bytes, and those are the owner's claims (§18.4);
* the merge-commit rule is enforced only by `--merged`: a squash merge leaves the chain in the
  pull request body, not in the base branch's history (§18.2).

Open questions, each with the fact that would answer it:

| Question | The fact that would answer it |
|---|---|
| Should a proposal be allowed to touch `docs/**`? | S1 review data: how often a code fix needs a doc change, and how often a docs-only proposal was noise. |
| Is the plan the right home for the test-only rule? | Whether S1 review sees code patches that "fix" tests; if so, the rule becomes structural rather than a check parameter. |
| Should iterations be allowed to accumulate into a series? | S2 data on proposals that needed a second run to be reviewable. |
| Does the breaker need a window rather than a streak? | Whether real failures alternate with successes often enough that a streak is the wrong shape. |
| Should the verifier run as part of `kian job status`? | Whether reviewers actually run it by hand before or after a merge, and how often a broken chain is found late. |
| Should `--merged` query the pull request state through `gh`? | Whether the merge-commit requirement is the only fact git cannot answer locally; the verifier currently keeps its no-network rule. |
| Is an assisted `land` verb worth its authority after all? | S2 numbers: proposal volume, review time, regression rate — the facts this phase deliberately does not have. |

## 18. Landing: the owner's merge, verified

### 18.1 Authority

The owner merges. No run, no conductor and no kian CLI verb writes the base branch, calls
`gh pr merge` or tags a release; S3 adds no approval mechanism and no second merge channel. The
review gate is the pull request review (§8.2), and the merge is its outcome. A proposal is landed
when the owner merges it; `kian self verify` reports that fact, it does not create it.

### 18.2 The merge shape

The owner merges with a **merge commit** (GitHub's "Create a merge commit"), so the published
commit stays a parent of the merge and its `Kian-*` trailers and the pull request reference stay in
the base branch's history. A squash or a rebase merge is a different proposal in history: the
published commit becomes unreachable, its trailers disappear from the base branch, and a rebased
copy no longer matches the artifact bytes. `--merged` therefore reports a squash or rebase as
`not merged into <base>` even though the change is on the branch; the owner re-lands it with a
merge commit or accepts that the record lives only in the pull request. No trailer is added to the
merge commit: the chain is already complete on its parent.

### 18.3 `kian self verify <run>`

The verb takes a run id, the usual `--config`, an optional `--base <branch>` (default
`selfdev.publish.base`) and `--merged`. It reads the run stream, the producer stream the artifact
ref names, and the local repository configured by `selfdev.publish.repo`; it writes nothing and
never fetches. With no `selfdev.publish.repo` it exits 2 with `landing is not configured`.

It checks, in order, naming the first link that fails:

1. the admitted input row carries `job.selfdev`, and each recorded digest is `sha256:` plus 64
   lowercase hex characters;
2. the frozen plan re-canonicalises to the marker's `plan_digest`;
3. the `job.artifact` receipt names a producer event whose committed text still hashes to the
   recorded `sha256` and length;
4. the publish receipt is `ok: true`, names `selfdev/<run_session>`, carries a 40-hex commit, and
   reports `exit_code: 0` when the plan ran a gate;
5. that commit exists in the local object database, its first parent is the frozen
   `base_commit`, `git diff <base_commit> <commit>` is byte-identical to the artifact, and the
   `Kian-Run` / `Kian-Plan-Digest` / `Kian-Patch-Sha256` / `Kian-Pack` trailers match the recorded
   values;
6. the branch named by the receipt resolves to that commit, locally or as a remote-tracking ref;
   a missing branch is allowed only with `--merged`, where the commit's reachability from the base
   is the stronger fact;
7. with `--merged`, the commit is an ancestor of the base branch.

Any failure exits 2 as `audit chain broken at <link>: <detail>`; a complete chain exits 0 and
prints one line per link. `--json` prints `{ok, run, links: [{link, ok, detail}], merged?}`. A
commit that exists only on the remote is reported missing, not fetched: the verifier is a read,
and a clone that has never fetched the branch cannot verify it.

### 18.4 What verification does not prove

It proves the bytes: that the reviewed artifact is the commit's diff, that the commit descends
from the frozen base, that the trailers and the stream agree, and — with `--merged` — that the
commit is in the base branch's history. It does not prove that a review happened, that the patch
is correct, that the gate covered the change, or that the owner merged the commit they reviewed.
Those are judgements about people and process; the contract records them as the owner's, not the
verifier's.

### 18.5 Crash and concurrency

The verifier has no state to recover: it reads committed bytes and a git repository, and an
interrupted run leaves nothing to clean up. A merge that lands during a verification only adds an
ancestor, so a pass cannot be invalidated by a concurrent merge; a force-push of the base branch
rewrites history and is outside this contract (§17). The merge record is durable because git is.

## Closing: what this contract asks the owner to decide

1. **A run proposes; the owner disposes.** No merge, no deploy, no self-authorisation — SE1.
2. **The symptom is admitted by digest.** Admission verifies before admitting and records the
   digests it verified; the runtime does not claim a later read matched them — SE2.
3. **The surface lives in the plan.** The denial list is configuration, frozen at admit, reviewed like
   code; a run has no path to propose a change to it — §5.4.
4. **Proposing is parallel; landing is serial.** Write isolation by derivation replaces a lock — §9.2.
5. **The review gate is a pull request**, and a run never waits for one — §8.2.
6. **Publishing is a conductor action, not a general `exec`.** The sequence is fixed and
   auditable, so a plan can ask for a publish and never for an arbitrary command — §9.4, ADR-071.
7. **Landing is the owner's merge, and the runtime verifies what it did not write** — §18,
   ADR-075.
