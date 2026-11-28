# ADR-075 — Landing is the owner's merge, verified rather than executed

- Status: accepted (2026-11-28, self-evolution S3 contract)
- Related: ADR-069 (self-evolution is a proposal pipeline), ADR-071 (publish is a conductor
  action)

## Context

S2b ends with one branch pushed (never forced) and one pull request whose body carries the audit
chain. The ladder's last rung, S3 land, was left undesigned: merge, tag, deploy and rollback were
named as the owner's actions, with no statement of how a merged proposal stays verifiable and no
answer to whether the runtime should help. Three routes exist:

1. the runtime merges the pull request once its gates pass;
2. an operator verb, `kian selfdev land <run>`, merges one pull request on request;
3. the owner keeps merging, and the runtime only verifies the result.

The constraints are already decided: SE1 says no run writes a protected branch or merges a pull
request; the merge commit is the one link this repository does not write (§11); S3 was conditioned
on its own ADR and on the S2 numbers — proposal volume, review time, regression rate — which do
not exist yet; and the audit chain must survive the merge so a reviewer can check a landed
proposal after the branch is deleted.

## Decision

Landing stays the owner's act, and the runtime verifies what it did not write.

* **No runtime merge.** No run, conductor or CLI verb writes the base branch, calls `gh pr merge`
  or holds a merge credential. The review gate is the pull request review, and making it
  programmatic would make it advisory — the failure SE1 exists to prevent.
* **No assisted `land` verb in this phase.** A verb that merges one pull request is a second merge
  channel with the same authority and the same credential surface as the code host's merge button,
  and it would be specified without the S2 numbers the ladder requires. It is deferred, not
  rejected forever: a later ADR may add it once the numbers exist.
* **A read-only verifier.** `kian self verify <run> [--merged] [--base <branch>]` recomputes the
  chain — the frozen plan digest, the artifact bytes through its producer ref, the publish receipt,
  the commit's parent, diff, trailers and branch, and with `--merged` the merge's place in the
  base — and exits 2 naming the first broken link. It writes no event, row or ref and never
  fetches.
* **The merge shape is a merge commit.** The owner merges with GitHub's "Create a merge commit", so
  the published commit stays a parent and its trailers and pull request reference stay in history.
  Squash and rebase merges orphan the artifact commit and are reported as unmerged.

## Consequences

A merged proposal is checkable from the streams and the local repository alone: the verifier needs
no network, no credential and no new state. The price is discipline: the owner must merge with a
merge commit, and the verifier refuses a squash instead of reconstructing what it would have been.
The verifier proves bytes, not judgement — it cannot prove a review happened or that the gate
covered the change — and that limitation is written down rather than hidden (§18.4). A chain that
was already broken before the merge fails the same way after it, so verification is useful before
the merge too, which is when a reviewer should run it.

## Alternatives rejected

* **Autonomous merge after gates.** The gates are evidence, not consent; merging on them would let
  the runtime decide that its own patch is correct, and would require a merge credential the model
  has so far kept out of the runtime.
* **An assisted merge verb now.** It would be a second merge channel specified without the data the
  ladder demands, and it buys little over the code host's button plus a verifier.
* **Squash or rebase merges.** They rewrite the published commit, so the artifact bytes and the
  trailers no longer describe the commit that landed; the chain would survive only in the pull
  request body.
* **Doing nothing.** The chain would be unverifiable after the branch is deleted, and the "one link
  the repository does not write" would become an unchecked claim.

## Limits

The verifier reads local git and never fetches, so a clone must fetch the branch or the base before
verifying. It cannot prove that the commit the owner merged is the commit they reviewed beyond the
trailers and the diff. The merge-commit rule is enforced only by `--merged`, and a base
force-push is outside the model. Tagging, releasing, deploying and rolling back remain outside S3.
