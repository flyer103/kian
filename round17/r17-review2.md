# R17 re-review (RV2) — after the fix card `t_92f085e9`

**Verdict: PASS (9/10).** The R17 round may be committed to `main`. All six MUST-FIX items from
`r17-review.md` are closed, each one re-derived in this tree by execution rather than read off the
fix handoff. Nothing in the fix set introduced a new defect; two pre-existing cosmetics are listed
as suggestions and do not gate the commit.

- **Tree under review:** worktree `a private worktree`,
  branch `wt/r16-p2-roster`, `HEAD = 3dc74f0` (`feat(multiagent): P4 — audit key groups,
  depends_on/group, readiness gating (R17 C6/C7)`), **uncommitted by design**:
  `git status --porcelain` = 9 modified files, 0 untracked and 0 staged at review start (this file's
  own `round17/` directory is the single untracked addition afterwards — see the artifact note).
- **Fingerprint:** `git diff | shasum -a 256` = `b3628e36b63003312439dbd6f3b284a31baaf35b77a7e98a3af82dad8d98e11b`,
  identical at start, after each of my three mutation restores, and at the end of the review.
- **Read-only review:** no commit, no push, no `git add`. Every mutation below was reverted and
  verified by `sha256` (superagent source) or by the whole-diff fingerprint (`kian-cli` source);
  `crates/kian-superagent/src/` shows **zero** tracked diff, as before I touched it.

## Gates (my own runs on this tree)

| Gate | Result |
|---|---|
| `cargo build --workspace` | exit 0 |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| `cargo test -p kian-cli --test docs_map` | 6/6 passed, exit 0 |
| `cargo test --workspace --no-fail-fast` (clean re-run) | **134 suites, 1685 passed, 0 failed, 7 ignored, `TEST_EXIT=0`** — identical to the baseline `r17-review.md` recorded |

**One flake, triaged, not an R17 regression:** the first full run (used for nothing else) failed
exactly one test, `kian-superagent --test policy_version::notify_live_false_marks_but_admits_none`
(`policy_version.rs:588`, `no new inputs: [AssistantMessage{content:"ok"}, TurnEnd]`). The file is
untouched by this round (`git diff --name-only | grep -c policy_version` = 0; last commit touching
it is `feacd84`), the test is green **3/3 isolated**, and the clean re-run is 1685/1685. The
assertion that trips is `events.len() <= before` after `wake` + a 150 ms window: a resident turn
landing inside the window adds `assistant_message`+`turn/end` although **no policy marker was
admitted** (the test's second, meaningful assertion was satisfied). Load-sensitive, pre-existing,
out of this card's scope — see suggestion S4.

## MUST-FIX closure (all six verified closed)

| # | Item | Evidence in this tree | Verdict |
|---|---|---|---|
| **B1** | README roster example did not parse (`mandate_template` lacked required `strength`) | `README.md:359` now `mandate_template: { scope: read-only research, strength: inferred }`. Ran the real binary on a config built from the README quickstart + the roster block **verbatim**: `target/debug/kian agents --config pos.yaml` → **exit 0**, roster row rendered (`researcher / Finds and cites sources / researcher / fs_read / 4`). Negative control (identical file without `strength`) → **exit 2**, `kian: agents failed — malformed kian.yaml: superagent.roster[0].mandate_template: missing field `strength` at line 54 column 25` — the review's exact error. | **Closed** |
| **B2** | DESIGN §4.8 said "neither view nor CLI exists yet … nothing there is implemented" and misstated `mandate_template` | `DESIGN.md:1356-1358` now: "The view and the three verbs exist, and Phases P1–P4 are landed in code + tests with no live-run evidence — MULTIAGENT-MODEL §10 records the status per phase; §8 still records the per-phase rollback." Both pointers resolve: §10 = "UNVERIFIED … **Implemented through P4, but with no live-run evidence**"; §8.1 still carries the per-phase rollback column. `DESIGN.md:1360-1363` now "(`scope`, `strength` — required, `explicit` \| `inferred`), optional `tools_allow` (the separate tool fence)". Cross-checked against the structs: `roster.rs:43-53` `MandateTemplate { scope: String, strength: MandateStrength }` `#[serde(deny_unknown_fields)]` (no `tools`, `strength` not defaulted) and `config.rs:1253-1261` `RosterMandateTemplateDef { scope, strength }`; `RosterEntryDef.limits` is `#[serde(default)]` (`config.rs:1238`), so "optional `limits`" is right too. | **Closed** |
| **M1** | MULTIAGENT-MODEL §7.2 / §0 said "proposed / NOT this round" against its own landed status | `MULTIAGENT-MODEL.md:755` heading = "§7.2 The P4 additive payload keys (**landed R17; owner gate; one-way**)"; `:42` = "three read-only CLI verbs". `grep -rn "proposed\|NOT this round\|not implemented\|nothing there is implemented\|exists yet"` over `DESIGN.md` + `MULTIAGENT-MODEL.md` → **0 hits**. "one-way" agrees with §8.1's P4 rollback cell ("emitted keys are not removable (additions-only) — hence an owner gate"). | **Closed** |
| **M2** | The R8 ledger row's "stale view" plant was dead code, and `no_control_read_consumes_a_view` scanned only `kian-orchestrator` | See the three mutations below. Plant is live (`ViewFedStreams` serves the stale `task_board` row through the child-stream port; `multiagent_readiness.rs:906-913` asserts the port was asked for exactly `[worker]`, i.e. the fabricated answer reached the walk); the scan now visits `kian-superagent/src` (`views.rs:1232`) with a coverage assertion that **is** load-bearing (mutation c). | **Closed** |
| **m3** | Log-only correction key undocumented | `DESIGN.md:1381-1384`: "The correction's uniqueness is count-derived, not key-derived: `attempt = schema_correction_count(events)` … the logged `schema-correction:<spawn_uid>:<attempt>` key is an audit label, never a persisted fact." Code agrees: `worker.rs:1178` counts committed synthetic `input/admitted` rows carrying `content/kian_schema_check`; `worker.rs:1263` sets `attempt = schema_correction_count(events)`; the key is read at exactly one place, a `tracing::info!` field (`worker.rs:1840`), and has no non-test consumer anywhere (`grep -rn schema_correction_key crates/`). | **Closed** |
| **m4** | EVENT-MODEL said `depends_on` optional vs always emitted | `EVENT-MODEL.md:116` now `"depends_on":[<str>]` (no `?`). Code agrees on the parent side: `audit.rs:94-98` always writes `depends_on` (empty array = "declared none"), fed by `router.rs:1847` from `declared_depends_on`. The child side's derivable subset (`task_id`, `assignee` only) is stated in the same table row and in `audit.rs:12-18`, so the union schema is not contradicted — see nit S3. | **Closed** |
| **m5** | CHANGELOG label `kian-R17 C5` | `CHANGELOG.md:34` = "**Delegation task board — the read-only operator surface (kian board / kian tasks, round 17, C5)**", matching its siblings "(round 17, C7)" (`:13`) and "(round 17, C1–C3)" (`:24`). | **Closed** |
| **m6** | Leading space on the DESIGN refinement paragraph | `DESIGN.md:1360` starts at column 0 with `Refinement (the roster …)`, like every neighbour. | **Closed** |
| **m8** | `rebuild_views*` defaulted `board_enabled=false` | `views.rs:431-437` and `:474-480` take `board_enabled: bool` with no defaulting wrapper left (the doc comment at `:426-430` states the reason). All 22 call sites pass an explicit value (17 in `views_e2e.rs`, 5 in `benches/read_path.rs`), and the production path is unaffected: nothing outside `views.rs` calls `rebuild_views`, and the serve-side ticker enters at `server.rs:1105 → views_maintenance_tick_with(…, board_enabled)` → `views.rs:1081 rebuild_views_with(…)` with the computed `superagent.board.enabled && read_model.views.enabled` conjunction. | **Closed** |

## Mutation evidence (M2 — re-applied by me, then reverted)

| Mutation | Where | Observed | Restore |
|---|---|---|---|
| **(a)** readiness trusts a claimed outcome — accept `outcome: "completed_verified"` off the child's bytes before `derive_outcome` | `multiagent.rs:727-733` (`ready_deps`) | `readiness_reads_the_primary_never_the_view` **FAILED** at `multiagent_readiness.rs:898` (`the stale view must not release the spawn`) with a real `ToolResult { status:"dispatched", spawn_outcome:"dispatched", … }` — the stale row authorised a spawn. Other 8 tests green (the mutation is specific). | file `sha256 -c` OK, 9/9 green again |
| **(b)** does the new scan leg actually see `kian-superagent/src`? — planted a `fresh_task_board` reference in a superagent source comment | `multiagent.rs:609` (comment) | `no_control_read_consumes_a_view` **FAILED**: "control-plane code must not consume views: ['…/kian-superagent/src/multiagent.rs references `fresh_task_board`']" | diff fingerprint back to `b3628e36…` |
| **(c)** is the coverage assertion tied to the superagent leg? — removed `visit(…, &superagent)` | `views.rs:1232` | Test **FAILED**: "the R8 scan must cover the readiness control surface (`ready_deps` not found under kian-superagent/src)". Also statically guaranteed: `kian-orchestrator/src` contains **0** hits for `ready_deps|readiness`, so only the superagent leg can satisfy it. | diff fingerprint back to `b3628e36…`, test green |

Why (a) is now a real detector, unlike the pre-fix version: the pre-fix test asserted a JSON literal
against itself; now the row is injected through the port readiness genuinely consults, the test
asserts that consultation happened (`streams.asked == [worker]`), and the one named mutation turns
the test red with the failure message the ledger row promises.

## Diff confinement (no new findings by construction)

`git diff --numstat` vs `3dc74f0` — 9 files, every one named by a finding:
`README.md` (B1), `design/DESIGN.md` (B2/m3/m6), `design/MULTIAGENT-MODEL.md` (M1),
`design/EVENT-MODEL.md` (m4), `CHANGELOG.md` (m5), `crates/kian-superagent/tests/multiagent_readiness.rs` (M2),
`crates/kian-cli/src/views.rs` (M2/m8), `crates/kian-cli/tests/views_e2e.rs` + `crates/kian-cli/benches/read_path.rs` (m8 ripple).

- No change to `kian-core`, `kian-store`, `kian-orchestrator`, `kian-loop`, `Cargo.toml/lock`,
  migrations, routes or metrics keys; `crates/kian-superagent/src/` is **byte-identical** to `3dc74f0`.
- The diff removes exactly **one** assertion — the dead `assert_eq!(stale_view["outcome"], …)` — and
  replaces it with the connected equivalent on `stale_row`. No `#[test]` was removed or renamed; no
  test count changes (134 suites / 1685 tests, same as the pre-fix baseline).
- `views_e2e.rs`'s 17 changed lines are argument-only (`…, 1_000)` → `…, 1_000, false)`), including the two
  board tests at `:954` / `:979` where board-off is the deliberate semantics under test.

## Suggestions (non-blocking; no re-review needed)

- **S1 (m8's tail):** `views.rs:1066-1068 views_maintenance_tick` is still a defaulting entry point
  with exactly the failure mode m8 removed one level down (`…, false`), and it has **no callers**
  anywhere in the repo (the serve path uses `views_maintenance_tick_with`). `rebuild_views`'s new
  comment reads "there is deliberately no defaulting entry point left", which is true of the two
  names it scopes but not of this sibling. Delete the wrapper (or require the argument) in a later
  polish commit — nothing gates on it today.
- **S2 (docs, pre-existing):** `worker.rs:1141-1145` still calls `schema-correction:<spawn_uid>:<attempt>`
  "the idempotency key … never double-appends", which the new DESIGN sentence ("an audit label, never
  a persisted fact") now contradicts. DESIGN is the correct side (nothing consumes the key); the code
  comment should be reworded when that file is next touched.
- **S3 (nit):** `EVENT-MODEL.md:116` puts `"depends_on":[<str>]` in a group that is the parent-side
  superset; the row's trailing sentence does scope the child side, but a reader of the bare schema
  could infer the child's group carries an empty array. A parent-side qualifier inside the cell would
  remove the ambiguity.
- **S4 (test hardening, outside R17):** `policy_version.rs:585-592` asserts `events.len() <= before`
  after a wake; the meaningful claim is the marker absence two lines below. Dropping the length
  assertion (or comparing only `input/admitted` rows) removes the load-sensitivity that produced
  this run's single flake.
- **Observation, not a finding:** DESIGN §4.8's heading still reads "(R14, designed)" while the body
  now says landed. Section provenance is legitimate; recorded only so nobody "fixes" it by accident.

## Carried note from the first review (still standing)

The P3-3 pin's literal `group: null` / `depends_on: []` wins over C5's "absent keys omitted" prose —
the implementation and its golden follow the pin, and `EVENT-MODEL.md:116`'s non-optional
`depends_on` is consistent with it. Do not "fix" this later without a new pin.

## What I did not verify (unchanged from the first review)

No live-provider run, no live multi-session child delegation, no postgres chain, benches executed only
as compile targets — the round's own §10 keeps those claims visible as UNVERIFIED, which is the honest
state.

## Verdict

**PASS — commit the R17 round.** The two blockers and two majors of `r17-review.md` are closed by
executed evidence (parse of the README example, the DESIGN/MULTIAGENT-MODEL consistency reads, and
two independent mutations that turn the new readiness test and the new scan leg red), the five
bundled minors are closed, and the fix diff stays inside the findings' files with no assertion
weakened.

*Artifact note: `round17/` is untracked scratch (the same status the first review's `r17-review.md`
had). It must stay out of the commit; the round's closeout should not treat this file as a repo change.*
