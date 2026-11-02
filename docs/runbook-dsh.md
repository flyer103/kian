# Runbook — S6 live dsh bridge (C57, updated C59)

What it takes to exercise the S6 dsh bridge against a **real** dsh install, what each
suite asserts, the current live status (2026-11-02, C59: **live positive landed**), and
the exact commands to re-run. The bridge contract itself is fixed by
[ADR-010](../design/decisions/ADR-010-dsh-session-mapping.md) and DESIGN §16; this
runbook is operational only.

## What S6-live needs

| Requirement | Value used / expected | How to check (values never printed) |
|---|---|---|
| dsh binary | verified against dsh **0.2.1-rc.5** (npm `-g`, `@deepseek-ai/dsh`) | `dsh --version` |
| binary path | e.g. `/Users/you/.hermes/node/bin/dsh` | `command -v dsh` |
| contract-v0 peer | `contrib/dsh-v0-peer/dsh-v0-peer.mjs` (C59) — the ecosystem half that adapts `dsh --profile sdk` JSON-RPC to bridge contract v0; needs `node` ≥ 18 and `dsh` on PATH (or `$DSH_BIN`) | `node --check contrib/dsh-v0-peer/dsh-v0-peer.mjs` |
| provider route | `deepseek-official` (the adapter the sdk profile registers; plain `deepseek` is rejected by `initialize`) | `DSH_PEER_PROVIDER` in the peer README |
| profile | `sdk` (bundles `@deepseek-ai/dsh-base` + `@deepseek-ai/dsh-sdk-app`); serves "DeepSeek Harness SDK clients over stdio JSON-RPC" | `dsh --profile sdk --help` |
| env gate | `KIAN_DSH_BIN=<path-to-dsh>` — absent/empty → every live test SKIPs, never fails | run the suites without the var; expect `SKIP:` log lines |
| proxy bypass | `NO_PROXY=127.0.0.1,localhost` for any e2e that talks to a local `kian serve` (macOS system proxy intercepts localhost) | — |
| model credential | a model-capable dsh route needs a provider credential: `DEEPSEEK_API_KEY` in the environment, or dsh's own credential store (`~/.dsh/.credentials.yaml`, keyed by the composed adapter). Without one the live run cannot reach a model call at all. | `env \| grep -c '^DEEPSEEK_API_KEY='`; `test -f ~/.dsh/.credentials.yaml && grep -icE 'api.?key\|token\|secret' ~/.dsh/.credentials.yaml` — counts only, never values |

## What each suite asserts

| Suite | Needs dsh? | Asserts |
|---|---|---|
| `cargo test -p kian-dsh --tests` — `tests/live.rs` | **yes** (one real `dsh --profile sdk` child) | Drives the real child through the `DshDriver`; whatever the peer does, the outcome must be honest — `Settled`/`Stopped`/`SoftFailed` carry the real reason; the test panics only on a missing/dishonest outcome (constraint 7). |
| `cargo test -p kian-dsh --tests` — `tests/live.rs` (`KIAN_DSH_PEER`, C59) | **yes** (peer + real model round trip) | The S6 live positive: spawns `contrib/dsh-v0-peer/dsh-v0-peer.mjs`, drives one real input through peer → dsh → DeepSeek. On `Settled` with a model response it requires the full mapped chain (`input/admitted`, `turn/start`, `step/request`, `assistant/message`, `turn/end`); a genuine model failure must surface as the honest `turn/end outcome=error`; on `SoftFailed` the peer's ready handshake must at least be proven (never a vacuous skip). |
| `cargo test -p kian-dsh --tests` — `tests/driver_bridge.rs`, unit tests | no (scripted fixture peer) | Spawn/steer/complete bridge flow, interrupt → cancelled step, child crash → `interrupted` never `completed`, replay byte-parity, takeover continuation, foreign-logical-format refusal, narrow waist. |
| `cargo test -p kian-cli --test dsh_bridge` | no (fixture peer behind `kian serve`) | The mounted S6 path end-to-end: worker template spawn/steer/complete, byte-identical replay of the finished stream, honest `interrupted` on child crash, kill -9 takeover continuing the same interaction. |
| `cargo test -p kian-superagent --test dsh_conformance` | no (C53 fixture) | Both mapping directions of the ADR-010 table, steer → synthetic-marked message, interrupt → cancelled attempt with no assistant message, the pre-flight request-bytes ordering rule, foreign-format refusal, unknown-kind honest error, byte-identical re-import replay. |

The two fixture-based suites are green in default CI with zero Node/dsh present; running
them with `KIAN_DSH_BIN` set additionally proves the env gate does not disturb them.

## Commands

The live positive through the contract-v0 peer (C59) — the primary live command:

```bash
source <(grep '^DEEPSEEK_API_KEY=' ~/.hermes/.env | sed 's/^/export /')   # export-prefix matters, see note
NO_PROXY=127.0.0.1,localhost \
KIAN_DSH_BIN="$PWD/contrib/dsh-v0-peer/dsh-v0-peer.mjs" KIAN_DSH_PEER=1 \
DSH_PEER_SESSION="kian-live-$(date +%s)" \
cargo test -p kian-dsh --test live -- --nocapture
```

(`$PWD` matters: the test binary's cwd is the crate dir, so a relative
`contrib/...` path fails to spawn — the failure surfaces as an honest
`SoftFailed("spawn ...")`, never a fake pass. `KIAN_DSH_PEER` is the gate:
either `1` — with `KIAN_DSH_BIN` naming the peer — or the peer's path.
`DSH_PEER_SESSION` fresh per run keeps the dsh session deterministic; unset
it to continue the cwd-derived session and exercise resume/continuation.)

The `sed 's/^/export /'` prefix is load-bearing: in agent-terminal sessions a
bare `DEEPSEEK_API_KEY=...` assignment read via `source <(grep ...)` can be
silently dropped by the host's secret scrubber, leaving a keyless run that
still exits honestly (MISSING_CREDENTIAL) — indistinguishable from a missing
key. Always gate the run with `[ -n "$DEEPSEEK_API_KEY" ]` (length check
only; never echo the value).

The raw-dsh smoke against stock `dsh` (still the honest C57 negative — stock
dsh speaks JSON-RPC, not contract v0):

```bash
KIAN_DSH_BIN=/Users/you/.hermes/node/bin/dsh NO_PROXY=127.0.0.1,localhost cargo test -p kian-dsh --test live -- --nocapture
```

The full S6 surface with the gate present:

```bash
KIAN_DSH_BIN=/Users/you/.hermes/node/bin/dsh NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-dsh --tests && \
KIAN_DSH_BIN=/Users/you/.hermes/node/bin/dsh NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-cli --test dsh_bridge && \
KIAN_DSH_BIN=/Users/you/.hermes/node/bin/dsh NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-superagent --test dsh_conformance
```

And with the gate absent (the CI default — everything must still be green, with the
live smoke skipping, never failing):

```bash
env -u KIAN_DSH_BIN cargo test -p kian-dsh --tests
```

## Current live status (2026-11-02, C59)

**The S6 live positive has landed.** `contrib/dsh-v0-peer/dsh-v0-peer.mjs`
(C59) is the contract-v0 peer the C57 run identified as the missing
ecosystem half. Verified live twice in a row against real dsh 0.2.1-rc.5 +
`deepseek-chat` with a real `DEEPSEEK_API_KEY`:

- `cargo test -p kian-dsh --test live` with
  `KIAN_DSH_BIN=$PWD/contrib/dsh-v0-peer/dsh-v0-peer.mjs KIAN_DSH_PEER=1`:
  **2 passed** —
  - `live_dsh_smoke_env_gated`: `TurnDone → Settled` through the peer (the
    smoke now also completes the real round trip when pointed at the peer);
  - `live_dsh_peer_positive_env_gated`: **the positive** — appended mapped
    chain `[TurnStart, InputAdmitted, StepRequest, AssistantMessage,
    TurnEnd]`, model reply "pong", `Settled`, ~4 s wall clock.
- The peer's adaptation facts (observed SDK vocabulary → v0 frames, the
  pre-flight `request/header` → `assistant/attempt` synthesis, the buffered
  `step/end` outcome derivation, the no-cancel interrupt limitation) are
  recorded in [contrib/dsh-v0-peer/README.md](../contrib/dsh-v0-peer/README.md).
- Zero-credential default suites stay green (both live tests SKIP).

Known operational notes: `initialize` rejects provider `deepseek` (the
registered adapter is `deepseek-official`); a known session id in a fresh
runtime needs `session/resume` before `session/prompt` (the peer retries
once on "already exists"); the peer exits via the SDK-blessed `shutdown`
after the turn settles.

## History: C57 run and findings (2026-11-01)

**Bugs the live parallel run surfaced (both fixed in this change, with
deterministic regression tests):**

1. **Same-millisecond inbox ordering (kian-store, both backends).** The
   take queries ordered eligible rows by `(eligible_at, id)` with a
   random uuid `id`, so a task/steer pair admitted in the same
   millisecond could consume the steer first — misrouting the worker
   session to the default agent (the worker classification reads the
   *first* input) and making cross-replica replay nondeterministic.
   Fix: migration `v2_inbox_admit_order` adds a database-assigned
   admission ordinal (`admitted_seq`) as the tie-break — sqlite computes
   `max+1` inside the single INSERT statement, postgres uses an identity
   column; both legacy-upgrade paths verified live (sqlite CLI, PG 17:
   v1 table with rows → v2 ALTER backfills → new admits order after
   legacy rows). Regressions: `same_millisecond_inputs_consume_in_admission_order`
   (sqlite) and the two `pg_*` tests in
   `crates/kian-store/tests/postgres.rs` (run live against PG 17 during
   C57: 12/12 green).
2. **Idle-boundary wake-loss deadlock (`kian-dsh` driver).** At a safe
   boundary with nothing eligible, the driver blocked indefinitely on
   the peer's next line; a peer blocked on `expect steer` says nothing,
   and a lost/folded wake (allowed by constraint 6) left the episode
   deadlocked — the drain kept heartbeating, the session never settled.
   Fix: the idle line-wait is bounded by `IDLE_INPUT_RECHECK` (50 ms)
   and the loop re-checks interrupt + eligible inputs; a step-open wait
   stays unbounded (the provider call is in flight, like the built-in
   loop's). Regression:
   `late_eligible_input_still_lands_at_the_idle_boundary`
   (`crates/kian-dsh/tests/driver_bridge.rs`), RED on the old code
   (15 s deadlock), GREEN after.

**Executed against real dsh 0.2.1-rc.5** (`KIAN_DSH_BIN` set):

- `kian-dsh` all tests: **8+7+1+1 passed** (the driver suite gained the
  late-input regression), including the live smoke —
  outcome `SoftFailed { reason: "peer never sent ready within 30s" }`, an **honest
  failure at the protocol boundary, not a fabricated completion**. This is the
  card's expected outcome (b).
- `kian-cli --test dsh_bridge`: **3 passed** (fixture-driven, gate present).
- `kian-superagent --test dsh_conformance`: **7 passed** (fixture-driven, gate present).
- Flake acceptance: the mounted e2e was hammered **300 consecutive runs, 0
  failures** after both fixes (pre-fix: ~1 failing run per 5–8 under the same
  loop). One earlier post-fix failure (~1 in 500, full-suite hammering under
  load) was not captured and remains unattributed — both captured shapes are
  root-caused above; if it recurs, capture the panic line and the newest
  `kian-c13-dsh-steer-*` temp dir's `kian.sqlite` before cleanup.

**Credential survey (names/presence only, values never read):**
`DEEPSEEK_API_KEY` absent from the environment; `~/.dsh/.env` absent;
`~/.dsh/settings.yaml` absent; `~/.dsh/.credentials.yaml` present with two
key-shaped fields (dsh's own credential store, used by the desktop profile). The
live run never reached a model call, so the credential was never exercised.

**Finding — the stock dsh SDK wire protocol does not satisfy the C53/ADR-010
contract.** Verified against the shipped packages (`dsh-sdk-jsonrpc-server`,
`dsh-sdk-protocol`, `dsh-session`):

1. **Handshake direction.** The bridge waits for a peer-initiated unsolicited
   `ready` frame (ADR-010 §2). The stock `--profile sdk` app is a JSON-RPC 2.0
   *server*: it sends nothing unsolicited on stdout and waits for a client
   `initialize` request — so the 30 s ready wait can never succeed. That is the
   observed `SoftFailed`.
2. **Framing.** The contract is raw JSON-lines frames; stock dsh speaks
   newline-delimited JSON-RPC 2.0 (requests carry `id`+`method`, session events
   arrive as `session.event` notifications, prompts go out as `session/prompt`).
3. **Control translation.** The contract's `steer` and `interrupt` frames have no
   stock counterparts: the SDK protocol documents "no cancel or session-close
   methods — a client abandons a turn by closing the runtime process".
4. **Pre-flight request bytes.** Real dsh commits `assistant/attempt` only for
   failed/retried/cancelled/stream-error attempts; a settled attempt folds into
   `assistant/message` with no preceding attempt event. The pre-flight record for
   successful steps lives in `request/header` — deliberately outside contract v0.
   ADR-010 §3 pre-decided exactly this case: when the dsh SDK surface cannot emit
   request bytes pre-flight, the bridge stops at a documented negative result,
   never a best-effort post-hoc logger.

The **event-kind vocabulary itself is aligned**: every kind in the ADR-010 table
(`user/message`, `system/message`, `developer`, `assistant/message`,
`assistant/attempt`, `tool/call`, `tool/result`, `step/start`, `step/end`,
`turn/start`, `turn/end`, `request/header`) exists verbatim in the shipped
`dsh-session` package — only the transport and the four points above differ.

**Ownership of the fix: not this repo.** ADR-010 scopes the wire contract as "the
bar any dsh-side plugin must clear to be kian-compatible" (the
`dsh-superagent-prototype` lineage, ecosystem-owned, outside this repo). Stock
`dsh --profile sdk` is not that plugin; until it exists, a live green run requires
mounting a contract-v0 peer as the `superagent.harness.dsh` command (the config
accepts any command — the fixture peer proves the path). The bridge, the mapping
and all fixture-driven suites are correct against the contract they were given;
no kian-side code change is warranted by this run.

**Re-run checklist once a credential + a contract-v0 dsh-side peer exist:**

1. Export the credential (e.g. `DEEPSEEK_API_KEY=...` — presence-check it, never
   echo it) or confirm `~/.dsh/.credentials.yaml` holds a usable key.
2. Point `superagent.harness.dsh.command` (or `KIAN_DSH_BIN`) at the
   contract-satisfying peer.
3. Run the one-liner above; record the outcome in the README's
   [live dsh-bridge run log](../README.md#live-dsh-bridge-run-log-manual-env-gated)
   — a `Settled` run with the session stream and replay report closes S6-live.
