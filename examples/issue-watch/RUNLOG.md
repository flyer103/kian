# RUNLOG — issue-watch live run (card C60, 2026-11-02)

All data below is captured from the real run (`examples/issue-watch/.state/kian.db`,
adapter log, `gh`), not reconstructed. Times are CST (UTC+08:00). Model: DeepSeek
`deepseek-chat` (the API reports serving model `deepseek-flash` for that alias).
Watched repo: `flyer103/kian` (private, owner-operated).

## 0. The gap found first (and how the run still happened)

The first live attempt (00:14) ran WITHOUT any adapter, provider
`base_url: https://api.deepseek.com`. The Router leg worked — but both
later legs failed against the real API, honestly recorded by kian:

- The poll worker's first provider call: **HTTP 400** —
  `Invalid 'tools[1].function.name': string does not match pattern
  '^[a-zA-Z0-9_-]+$'` — kian's built-in `fs.list`/`fs.read`/`fs.write`
  tool names (always mounted on worker/plain sessions; the agent
  `tools:` list does not filter the provider surface) are rejected by
  strict OpenAI-compatible endpoints.
- The Router's first turn after any tool round trip: **HTTP 422** —
  `messages[7]: missing field 'tool_call_id'` — kian's frozen C8
  serialization sends tool results as role:"tool" messages with the call
  id inside the string content and no wire-level `tool_call_id` (and
  assistant tool calls as stringified content).

kian's own honesty machinery handled both exactly as designed: the
worker's `thread/completed{status:"failed"}` carried the provider error
text verbatim, the Router consumed the failed report, its retry turn
failed, and the session ended `session/ended{reason:"failed"}` — no fake
completion anywhere. Both errors were re-verified by replaying the exact
persisted request bodies (`step/request` envelopes) against the API.

Fixing this means changing the frozen C8 serialization
(`FROZEN_CHAT_BODY_SHA256`, constraint 3) or making the mounted tool
surface configurable — core work, out of this card's scope (never-touch
rule). **Reported as a follow-up card for the orchestrator.**

The run below therefore routes the provider slot through
`wire-adapter.py` (example-scoped, ~200 lines, stdlib only): it
translates kian's request dialect to the strict shape (drops
pattern-invalid tool names; lifts `tool_calls`/`tool_call_id` to wire
fields; drops kian's duplicate per-call assistant transcript message)
and forwards to `https://api.deepseek.com`. **Responses — including the
SSE stream — pass through byte-for-byte; the model, the key and the
answers are DeepSeek's own.** Every model call in this log went through
it (adapter log: 100% HTTP 200 on 20+ requests).

## 1. The main run (./run.sh, DRY_RUN=1, fresh state, 00:22–00:47)

Booted `kian serve` (sqlite `.state/kian.db`) + adapter; charter
submitted once (`deduplicated:false`, later resubmits would be `true`).

Timeline (Router stream, session `00000000-0000-4000-8000-00000000c060`):

| Router seq | What happened |
|---|---|
| 1–8 | session created, judge prompt consumed, first (idle) judge turn |
| 9–13 | charter consumed; Router restates policy, parks |
| 14–21 | tick-1 consumed → judge turn → `spawn_worker` (`tool/call`+`thread/started`+`tool/result`) |
| 22 | poll #1 completion report admitted and consumed |
| ... | tick-2 → poll #2 (baseline: `open: []`) |
| — | **throwaway issue #1 created via `gh`** (00:33, "docs: README quickstart shows the wrong build command") |
| ... | tick-3 → poll #3 reports `open: [{number:1,...}]` |
| — | Router diffs tick-3 vs tick-2 **in its own context** → dispatches ONE draft worker |
| ... | draft worker completes (57-event session) |
| ... | tick-4, tick-5 → polls #4, #5: issue #1 still open but **NOT re-dispatched** (charter rule B dedupe held) |

End state (kian session inspect): **94 events, 14 inputs, 14 turns,
14 provider requests, 7 tool calls / 7 results, 0 compactions, state
`running` (parked, healthy)**. 6 worker sessions total: 5 polls + 1
draft. Router batching metrics: 13 judge turns consumed 13 inputs
(batch size 1 each), mean turn ≈1.72 s.

Key artifacts (verbatim from `thread/completed` events):

Poll #1 final message (tick 1, baseline):
```json
{"tick": 1, "mode": "dry-run", "open": []}
```

Poll #3 final message (tick 3, after the issue was filed):
```json
{"tick": 3, "mode": "dry-run", "open": [{"number": 1, "title": "docs: README quickstart shows the wrong build command", "body": "The quickstart section suggests running cargo run -p kian-cli, but the workspace binary is kian (cargo run -p kian-cli --bin kian). ..."}]}
```

Draft worker final message (the triage decision, thread
`c52c41e1-e78e-4480-839b-b03c3b64287e`):
```json
{"issue": 1, "severity": "minor", "decision": "needs-human", "posted": false, "draft": "Thanks for the report. I can't verify it from the material available to this triage worker: the only README I can inspect is the issue-watch example README (examples/issue-watch), which describes the build step as `cargo build --workspace` producing `../../target/debug/kian`, so it neither uses `cargo run -p kian-cli` nor contains the quickstart text quoted in the issue. The repository's top-level README.md, and whether a `kian` target in the `kian-cli` crate actually exists, need a maintainer's check. Once the exact quickstart location is identified, the fix is a one-line docs correction."}
```

Dry-run discipline verified from the outside: `gh issue view 1
--json comments,state` → `{"comments":0,"state":"OPEN"}` at the end of
the run — the workers read (`gh issue list`, `printenv DRY_RUN`) but
never wrote. (The issue was closed by the operator afterwards, with a
closing comment referencing this runlog.)

Note the model's `printenv DRY_RUN` check reported `mode: "dry-run"` on
every poll — the env-gated posting path exists (charter DRAFT TASK step
4) but was deliberately not exercised live; posting requires
`DRY_RUN=0` and would appear as a `tool/call{exec gh issue comment}`
event pair on the draft worker's stream.

## 2. The restart demo (./demo-restart.sh, fresh state, 00:47–00:52)

- Fresh boot, charter submitted (`deduplicated:false`), tick-1 flowed,
  poll worker ran: Router stream = **26 events**.
- `kill -9` the serve process mid-watch (shell reported
  `Killed: 9`). No signal handling, no flush.
- Reboot on the same sqlite store; charter resubmitted:
  `{"deduplicated":true}` — the committed row won, exactly the S1
  idempotency story.
- Next tick arrived and was judged: Router stream = **31 events** —
  the SAME session id, seq strictly growing (26 → 31), `epoch` 1 → 2
  (new monotonic claim), `drain claims` 2, 5 turns total post-restart.
  The per-process tick counter restarted at 1 and admission deduped the
  already-committed `tick-1`; only genuinely new ticks drove turns.

One honesty footnote from this demo window: one later judge turn (after
the script's own proof point, while the demo server was still up)
ended `turn/end{outcome:"soft_failure"}` and the Router session
recorded `session/ended{reason:"failed"}`. The adapter logged HTTP 200
for that request and a byte-identical replay of the persisted
`step/request` body against the API returns a valid SSE stream — i.e. a
transient provider-stream failure, not a request-shape problem. It is
recorded here because it happened; it also surfaces a liveness
observation for the bundle: a resident Router that soft-fails a turn
ends its session, and the idempotent bootstrap row (already consumed)
will not re-wake it on the next boot — the watch would need operator
attention. Reported in the follow-up notes.

## 3. What this run proves

- **S1 durability on a real workload**: kill -9 → reboot → same session,
  growing seq, deduped ticks, charter idempotency — with real gh +
  real model calls in flight around the crash.
- **S4 delegation**: 5 poll threads + 1 draft thread, all
  `spawn_worker`-dispatched, evidence-summarized, and reported back as
  admitted inputs; the Router's in-context diff (its own `input/admitted`
  events are the derived last-seen state) detected exactly one new
  issue and drafted exactly once.
- **Channel honesty**: missed ticks during downtime never happened (no
  fake catch-up); DRY_RUN was checked per poll by the workers
  themselves; zero unauthorized writes to GitHub.
- **And, incidentally, provider-failure honesty**: the unadapted first
  attempt produced `thread/completed{failed}` reports carrying the
  provider's own error text — no fake completions (§0).

## 4. Files

`README.md`, `config.yaml`, `triage-prompt.md`, `run.sh`,
`demo-restart.sh`, `RUNLOG.md` (this file), `.gitignore`.
(`wire-adapter.py` existed only for the C60 run; kian-61 fixed the core
dialect and the adapter was deleted — §5.) Demo artifacts under `.state/` are gitignored.

## 5. kian-61: the core fix and the adapter-free green run (2026-11-02)

The follow-up card landed as **wire dialect v2** (option "both"):

1. **Dotless built-in tool names** — `fs_list`/`fs_read`/`fs_write`
   (`crates/kian-loop/src/tools.rs`). The dotted spellings stay
   *invocable* (never advertised) so sessions recorded before the
   rename replay unchanged.
2. **Strict wire shape** — `to_chat_message` (`crates/kian-loop/src/react.rs`)
   now emits assistant tool-call turns with the wire `tool_calls` field
   (string `function.arguments`, `content: null` when textless) and
   `role:"tool"` results with `tool_call_id`; kian's per-call
   `tool/call` transcript duplicates are dropped from request bodies.
3. **Config-filterable mounted surface** — the agent `tools:` list is
   now the enforced mounted surface (`FilteredTools`, fail-closed: a
   listed-but-unmounted name refuses boot), for plain sessions, RAct
   plugins, and super-agent workers alike.

This is a constraint-3 event, done by the book: DESIGN.md §"Wire
dialect v2" note added, golden hash moved
`4a1dad31…d7ce` → `b743584d…7708`, event-stream payload contracts
(C4/C11) untouched — only the request dialect changed.

**The adapter (`wire-adapter.py`) is deleted.**
`providers.deepseek.base_url` is `https://api.deepseek.com` directly.

### The green run (fresh state, 01:30–01:36 CST, DRY_RUN=1)

Same shape as §1 (resident Router `…c060` + poll/draft workers), no
adapter anywhere. All numbers below are programmatic queries against
`.state/kian.db`:

- **13 sessions**: Router (170 events, still `running`/parked at
  capture), **11 poll workers**, **1 draft worker** — every
  `thread/completed` is `status:"completed"`; zero `soft_failure`
  turns, zero failed sessions in the whole store.
- **59 provider requests, all accepted** by the strict endpoint; **45
  of them follow a tool round trip** (their bodies carry
  `tool_call_id` / `tool_calls`) — exactly the shape the v1 dialect
  422'd on. 49 `tool/call`+`tool/result` pairs total.
- **Every request's `tools[]` matches `^[a-zA-Z0-9_-]+$`** (checked
  across all 59 persisted `step/request` envelopes). The `fs.read`
  strings still present in 9 envelopes live inside tool-result
  *content* (the model echoing commands), never in `tools[]`.
- **The config filter is live**: every worker request mounts exactly
  `["exec"]`; the Router's mount exactly
  `["deliver_structured", "reply", "spawn_worker"]` — the unlisted fs
  tools never ride any request.
- **The loop closed on a real issue**: throwaway issue #2
  ("kian-61 wire-dialect live check: strict tool_calls round trip")
  was detected at tick 2, **exactly one draft worker dispatched**
  (charter dedupe held through tick 11), and the draft triaged it
  correctly — its final message: *"The issue body is not a bug report:
  it's a marker created by the acceptance run itself … this triage
  worker's own session ran a multi-iteration tool loop with
  `tool/call{id}` + `tool/result{call_id}` pairs accepted across 8
  turns with no provider errors — i.e. the strict `tool_calls`/
  `tool_call_id` round trip"* — the model verified the fix with the
  fix. Dry-run honored: `gh issue view 2` → 0 comments.

Workspace gates at this commit: `cargo fmt --check`, `cargo build
--workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` (exit 0) — all green.

## 6. kian-62: the re-arm run — a dead resident Router comes back on reboot (2026-11-02)

The §2 footnote gap, closed. Two ways a resident Router can die, two
honest recoveries, one mechanism (DESIGN §4.6 "Resident re-arm",
landed in commits `0924b6c` → `a76544c`):

- **Soft-failed** (`turn/end{outcome:"soft_failure"}` →
  `session/ended{reason:"failed"}`) projects *idle*: the next admitted
  input re-opens the **same pinned session**. No successor, no re-arm.
- **Terminally ended** (`session/ended{reason:"completed"}` — e.g. the
  lifetime iteration budget exhausting, which is exactly what froze
  the §5 router `…c060` at seq 211) is frozen forever: the consumed
  bootstrap row can never re-wake it. On boot, the server walks the
  deterministic successor chain `uuid5(s, "kian:resident:re-arm")`,
  binds the resolved live session everywhere the pinned id went, and
  the successor bootstraps under the per-generation idempotency key
  `<prefix>:re-arm:<uuid>` with a cause-carrying prompt. Zero new
  event kinds — the re-arm is observable as the successor's first
  `input/admitted`, and (new in this commit) as a boot log line now
  that `kian serve` actually installs a tracing subscriber
  (`RUST_LOG`, default `warn`).

### The acceptance run (`./demo-rearm.sh`, DRY_RUN=1, 02:33–02:37 CST)

All numbers are programmatic queries against the demo stores
(`.state-rearm-{a,b}/kian.db`); full transcript in
`.state-rearm-run.log`.

**Act A — forced soft failure, kill -9, reboot (pinned `…c062`).**
`flaky-proxy.py` (stdlib-only, forwards to DeepSeek) killed router
judge request #2 *mid-stream* — HTTP 200, SSE content-type, a few real
bytes, abrupt close (the first attempt's proxy died on
`import requests`, so its failure was connection-refused; this one is
the genuine C60/C61 flavor). The loop recorded exactly 1
`turn/end{soft_failure}` and `session/ended{failed}` at seq 13 —
honest. Then `kill -9` (true crash, no flush) and a reboot *direct*
against DeepSeek on the same store: the next tick re-opened the SAME
pinned session — judge turn at seq 16 (> 13), and a **completed
(`final_answer`) judge turn at seq 27**: the watch is judging again
with no operator action. Successor-of-pinned events: **0** — a soft
failure is not a succession.

**Act B — the real C61 frozen store (pinned `…c060`, 211 events,
tail `session/ended{completed}`).** Boot on a copy of `.state/kian.db`:

- Boot log: `superagent: resident Router re-armed onto a successor
  session pinned=…c060 live=e43eb465-bff4-5b5f-9273-c0d8aed9a30b
  ended=1` — the deterministic `uuid5(c060, "kian:resident:re-arm")`.
- The successor's first `input/admitted` (seq 3) is the observable
  cause: *"[re-arm] This session re-arms the resident Router;
  predecessor session(s) ended and are frozen: …c060 (completed) …"*.
- The timer restarts tick numbering per boot, so ticks 1–15 were
  re-delivered — and **honestly re-judged**: the successor ran green
  judge turns back-to-back (16 `final_answer` turns by capture;
  tick-16 admitted at seq 90, continuing past the frozen run's 15),
  with exactly **1 tool call total** (the re-arm issue poll) and
  **zero duplicate worker dispatches** — the frozen workers stayed at
  their C61 event counts (19/79), no new poll/draft sessions.
- The frozen predecessor is byte-identical: still 211 events.

Known limitation (documented in DESIGN §4.6): a resident still ends
`{completed}` each time its lifetime iteration budget exhausts — until
that budget is revisited, residents simply chain successors at each
exhaustion, one per reboot. The demo configs raise `max_iterations`
to 500 so the successor outlives the demo window.

Workspace gates at this commit: `cargo fmt --check`, `cargo build
--workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` (exit 0, pg-gated suites included) — all
green.

## 7. kian-73 (C65): the P6c validation protocol executed live (2026-11-04)

The §6 protocol (round6-plan.md) run against the real `flyer103/kian`
repo: four scratch stores (run1 main 05:19–05:29, run2 retry/park
05:29–05:35, run3 kill -9 demo 05:36–05:38, run4 policy drift
05:39–05:41 CST), real DeepSeek round trips, real GitHub REST traffic,
`DRY_RUN=1` throughout. Router session `…c060` everywhere; every claim
cites session id + seq or a log line captured during the run. The only
GitHub writes were the run's own throwaway issues #3–#7 (created and
closed by the run with a closing comment referencing this section),
kian's own `deliver_structured` triage drafts on those same issues, and
the closing comments; no worker-side `exec gh` posting ever ran (every
triager checked `printenv DRY_RUN` → `1` first).

### 7.0 The gap found first (and how the run still happened)

The first boot's polls all failed honestly:

```
github audit: poll failed, next interval re-polls … error=channel
unavailable: poll response unreadable: error decoding response body
```

Root cause (reproduced with a minimal reqwest probe, same feature set
as the workspace): the C64 adapter sends **no `User-Agent`**, and
api.github.com answers UA-less requests with **HTTP 403 + an HTML
body** — reqwest cannot JSON-decode it, and the error chain hides the
403. With a `User-Agent` header the same request returns 200 JSON. The
C64 fixture suites never saw this because they run scripted pages.
**Filed as kian-74 (board `t_9edc80c0`).**

The run therefore pointed the channel's config-reachable `api_base` at
a scratch UA-injecting shim (`gh-shim.py`, ~100 lines, patterned on
`flaky-proxy.py`, stdlib only, Authorization forwarded and never
logged) that relays byte-faithfully to `https://api.github.com` and
additionally can inject path-scoped 502s on comment POSTs — the
delivery-leg fault `flaky-proxy.py` cannot inject (its kill trigger is
provider-judge-specific). The shim and all stores live only in the C65
kanban scratch workspace, never in this repo; the variant config is
exactly `config.yaml` plus `channels.issue-github.github.api_base:
"http://127.0.0.1:7462"`. All github traffic below flowed through it
(its request log is cited where the adapter view matters).

### 7.1 The eight scenarios

**1 — new issue → Eval accept → Ticket + Mandate: PASS.** Issue #3
created 05:19:51; admitted 05:23:34 (`event admitted … event_id=
poll:issues:flyer103/kian#3:2026-11-03T21:19:50Z deduplicated=false
reason=admitted_router`); Router judge turn → `spawn_worker` `tool/call`
(Router seq 19) → worker `fe52a9c4-f8bb-472c-bf45-66aa0b76080a`
`thread/started` (worker seq 4) whose task payload carries the
delegation wrapper verbatim (the whole `{"kian":{…}}` poll event) plus
the `mandate` object (`{strength, scope, granted_by, source,
policy_version}`) — the worker's first message. Mandate audit:
`thread_id=ed581e28… worker=fe52a9c4…`. Caveats recorded under gap G2
below: live strength landed `inferred` although the wrapper carries the
adapter-declared `"explicit":true`, and `source` = `{channel:"router",
topic:null}` — the mandate builder reads the same wrapper form the
`spawn_topic` parser cannot see live.

**2 — follow-up on a live issue → Executor owns it: NEGATIVE (product
gap G2).** The worker ended 5 s after spawn (one `printenv DRY_RUN`
call; `thread/completed` worker seq 16 at 05:23:43), the follow-up
comment (posted 05:23:52) therefore reached an ended topic — and was
admitted with `reason=admitted_router` (the *unbound* fallback), not to
a live owner and not `released_to_eval`. Root cause is structural, not
timing: the `spawn_worker` schema types `task` as a **string** and the
model passes the wrapper serialized as a string plus `correlation` as a
bare string, while `spawn_topic` only recognizes the wrapper as a JSON
**object** or a `{"topic":…}` correlation object — so the worker is
never registered on the topic (`kian tickets` shows `topic:null` for
every ticket all run). The zero-Router-judge-turns property is
unreachable live; `tests/issue_watch_fanout.rs` scripts the object form
and escapes it. **Filed as kian-75 (board `t_e69b8f35`).** (Incidental
positive: the Router honestly re-judged the follow-up batch —
including kian's own delivered draft, which re-entered through the poll
as comment 6615773978 — and correctly dispatched nothing for the
echoes.)

**3 — follow-up after the worker ended → fresh Eval: NEGATIVE (same
root cause).** With no topic binding there is no all-ended set to
release: every ended-topic event took the unbound branch
(`admitted_router`), the `released_to_eval` counter stayed 0 in every
run, and the release path was exercised only by the C64 fixtures. The
Router's charter rule B still behaved (judge; no re-dispatch for
echo-only comments). Covered by kian-75.

**4 — speak-rules: PASS (partial), one sub-item not-reached.** Under
the shipped `result-ready` rule: each issue's triage draft delivered
exactly once as structured content — `channel/delivered` Router seq 28
(issue #3, `attempt:1`, `first_attempt_ms:1793741026254`, sections
background/changes/verification rendered as the comment body) and seq
63 (issue #4, `attempt:1`); zero intermediate `channel/delivered`
events (2 spawns + 2 deliveries on the Router stream, nothing else);
`channel_policy` metrics `delivered:2 structured:2 im:0`. The
suppressed-send audit line is **not-reached**: the Router never
attempted a `reply` (charter rule D; the model complied), and the
closed vocabulary binds one condition per channel, so the
`ack-first-only` spelling (confirm-once) is exercised only by the
fixtures — the config picks the trade-off explicitly.

**5 — stage board honesty: PASS.** `kian tickets --json` snapshots at
≥3 moments: 05:23:52 (one ticket, mid-window), 05:27:48 (two tickets,
independent fan-out: `fe52a9c4`/#3 and `9d5238f6`/#4, one source), and
the run3 board below — plus a same-store re-run whose projection is
byte-identical modulo `now_ms`/`age_ms` (`replay-stable: true`). The
run3 board shows real variety matching stream facts:

```
SESSION                                STAGE            TOPIC   THREAD …  EVENTS
11054753-f397-4acc-b887-910795dfac81   queued           —       88084e96…  39
7e4a64e9-5915-4bc1-95de-8ce4d63ab8d1  idle             —       be626aa8…  22
a3715f21-c609-49a0-a6e5-7b996ce8bf22  blocked-failed   —       6cfc572d…  23
e0dec151-f1d6-4dd3-b347-b8e09fca8e20  working          —       0e88cb79…  76
fbb4a1ba-b575-4011-bebc-207b46b22163  blocked-failed   —       723b9f65…  37
```

(both `blocked-failed` tickets carry `thread/completed{status:"failed"}`
+ `session/ended{reason:"failed"}` — honest failures, §7.2 G4). Known
gap confirmed live exactly as the README documents: a *completed*
thread whose consuming drain appended `input/admitted` after its
`drain/claimed` projects `queued`, not `idle` (arm-5 ordering) — run1's
board shows `queued` for both successfully-completed tickets.

**6 — delivery failure → bounded retry → parked honestly: PASS at the
stream level; two surfacing negatives (gap G3).** With the shim failing
comment POSTs: issue #3's draft failed twice —
`outbox audit: send failed, backing off … attempt=1 backoff_ms=2000`
(21:30:02) and `attempt=2 backoff_ms=4000` (21:30:04), shim log
`502 INJECTED (1)`, `502 INJECTED (2)`, then `201` (21:30:09) — and
`channel/delivered` Router seq 38 carries **`attempt:3`** with
`first_attempt_ms` from the true first try. Exactly one comment landed.
The park arm (fail-forever): issues #4 and #6 each ran the full bounded
schedule — attempts 1–6 at backoffs 2/4/8/16/32 s and the sixth try,
then `ERROR outbox audit: delivery parked after bounded retries …
attempts=6` (21:32:17 and 21:33:33) — no seventh attempt, no unbounded
loop, the model not re-asked. Negatives: (a) the stage board showed
`parked:[]` and no `delivery-retry` stage throughout — the parked
intent's ticket key (the freeform `correlation` string the model
passed) matches no thread alias; (b) after a reboot on the same store
the boot rebuild logs `pending=0 parked=0` — the parked state is
process-local and vanishes (neither re-driven nor visible).
**Filed as kian-76 (board `t_b231e176`).**

**7 — kill -9 reboot mid-watch: PASS.** `demo-restart.sh` v2 on a fresh
store: boot, charter (`deduplicated:false`), issue #7 filed, the first
poll admitted the 24 h backfill (5 issue events + 5 comment events,
`surfaced=10`), spawns in flight — `kill -9` (shell: `Killed: 9`) at
Router seq 48. Reboot on the same store: charter resubmit
`{"deduplicated":true}` (committed row won), the same session id with
seq 48 → 85 (epoch 2), the poll **resume cursor derived from the
admitted-event tail** (`since=2026-11-03T21:27:36Z`), and the re-polled
already-admitted events came back `deduplicated=true
reason=deduplicated` (×4 issues + comment echoes) while genuinely new
events (issue #4's closing comment) admitted fresh — the source did not
re-deliver admitted events. No duplicate `thread/started` per thread;
the board above is the post-reboot projection.

**8 — decision reasons + policy drift: PASS.** `reason=` codes live on
every audit line and in the counters: run1 `by_reason
admitted_router:7`, run4 `admitted_router:4, deduplicated:4`
(`released_to_eval:0` — the S3 negative is visible at counter level).
Drift (run4, variant config adding `superagent.policy.version:
c65-r1`, `notify_live: true`): the bootstrap persists the header —
Router seq 3 `input/admitted` content starts `[policy c65-r1] You are
the Router…` and seq 6 `step/request` carries it. Bump the label to
`c65-r2`, reboot on the same store:

```
superagent: policy drift [policy c65-r1] → [policy c65-r2]: live
session keeps its persisted prompt; the next bootstrap renders the new
header session=…c060 from="c65-r1" to="c65-r2"
```

`superagent.policy_drift` counter: 1; exactly **one** synthetic marker
input admitted (Router seq 11, `[policy c65-r1→c65-r2] defaults
updated; …`, `"synthetic":true`); the live prompt stayed byte-identical
— SHA-256 of the seq-3 envelope `886a997d…6592` before, after one
reboot, and after a second. A second same-version reboot re-marks the
drift (the frozen header still lags the config — the audit repeats and
the per-boot counter re-ticks) but the marker is **not** re-delivered:
exactly 1 marker and 1 charter admission exist in the whole store.

### 7.2 Gap report (follow-up cards, the kian-60 precedent)

| # | Gap | Live evidence | Card |
|---|-----|---------------|------|
| G1 | github adapter omits `User-Agent`; api.github.com 403s poll+send with HTML (live-blocker); `flaky-proxy.py` cannot inject delivery-leg faults | §7.0 verbatim errors + probe | kian-74 `t_9edc80c0` |
| G2 | `spawn_topic` misses the live spellings (string task / bare-string correlation) → no topic binding → S2/S3 unreachable, mandate `inferred`+`source:router/null` despite `explicit:true`, outbox ticket aliasing dead | §7.1 scenarios 2–3; `topic:null` on every ticket | kian-75 `t_e69b8f35` |
| G3 | outbox parked state not rebuilt at boot (`pending=0 parked=0`) and not surfaced on the stage board | §7.1 scenario 6 | kian-76 `t_b231e176` |
| G4 | **exec environment passthrough**: a worker that dumps its environment writes the live GitHub token and DeepSeek key verbatim into `tool/result` (and every later `step/request` transcript). A run3 worker did exactly that (verified by boolean equality only; values never printed; the store was scratch-only and is destroyed with this run). A sibling worker also ran `bash -c "ls ~; find ~ …"` — exec is argv-direct but bash is a shell, so the blast radius is the operator's home | run3 worker `a3715f21…` seq 12/15/20; both workers ended `session/ended{reason:"failed"}` honestly | kian-77 `t_c2ba1a38` (HIGH) |

Minor observations (no card): the README-documented arm-5 `queued`-on-
completed projection gap confirmed live (§7.1 scenario 5); one triager
returned a prose final message instead of the charter's exact-JSON
report (model behavior — no structured `state` ⇒ no `awaiting-review`
stage observed live); kian's own delivered comments re-enter via the
poll and are honestly re-judged (no self-echo filter on sender).

### 7.3 Repo state after the run

Issues #3–#7 created and closed by the run (closing comments reference
this section); drafts landed on #3 (run1), #4 (run1), #3+#5 (run2
retry-success); #4/#6 (run2 park arm) intentionally received no draft.
No credentials in this repo: the shim, stores, probe and exports stayed
in the C65 scratch workspace. This change is docs-only (this section).

Workspace gates at this change (docs-only; the fixture suites remain
the CI proof of the mechanics, this section the live-leg evidence):
`cargo fmt --all -- --check`, `cargo build --workspace`, `cargo clippy
--workspace --all-targets -- -D warnings`, `cargo test --workspace`
(exit 0) — all green.
