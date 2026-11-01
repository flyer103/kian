# Runbook — postgres backend & S8 multi-replica live replay (C56)

What it takes to exercise the env-gated postgres surfaces against a **real** local postgres, the
exact commands to re-run them, and what S8 proves. The semantics themselves are fixed by
[ADR-009](../design/decisions/ADR-009-multi-replica-claim-admission.md) and DESIGN §13; this
runbook is operational only.

## Stand up a local postgres (macOS, Homebrew)

```bash
brew install postgresql@17
brew services start postgresql@17      # starts on boot thereafter
createdb kian_e2e                       # trust auth for the local user; no password needed
psql -d kian_e2e -c 'select version();' # smoke check
```

The URL used by every gated suite (trust auth, no password):

```bash
export KIAN_PG_URL="postgres://$(whoami)@localhost/kian_e2e"
```

Non-macOS (or any machine with docker) — one-liner alternative:

```bash
docker run -d --name kian-pg -e POSTGRES_PASSWORD=kian -p 5432:5432 postgres:17 \
  && docker exec kian-pg createdb -U postgres kian_e2e \
  && export KIAN_PG_URL="postgres://postgres:kian@127.0.0.1:5432/kian_e2e"
```

Credential hygiene (AGENTS.md / the C13 secrets rule): the URL is env-only and may carry a
password — never print its value; presence checks only, e.g. `env | grep -c '^KIAN_PG_URL='`.
The database is scratch: every suite creates and drops its own objects, and a clean replay is
always available via

```bash
dropdb --if-exists kian_e2e && createdb kian_e2e
```

## What each gated suite asserts

| Suite | Gate | Asserts |
|---|---|---|
| `cargo test -p kian-store --test postgres` | `KIAN_PG_URL` | 10 tests: migrations, append/CAS `expectedSeq` fencing, epoch-stale write rejection, batch atomicity, conditional claim admission (C47), inbox admit/wake ordering, lease TTL/heartbeat, byte-identical JSONL export parity vs sqlite (S7 shape across backends). |
| `cargo test -p kian-exec --test postgres` | `KIAN_PG_URL` | 9 tests: pg twins of the exec store — durable queue park/re-drive, terminal-guard late-result discard, exec row leases (claim/heartbeat/expiry re-drive), async job rows. |
| `cargo test -p kian-orchestrator` (pg wake tests) | `KIAN_PG_URL` | The LISTEN/NOTIFY wake transport: notify fires only after the input INSERT commits; a lost notify is recovered by the sweep ticker (wakes are advisory by invariant). |
| `cargo test -p kian-cli --test postgres_serve` | `KIAN_PG_URL` | The assembled `kian serve` on postgres: boots, requires an explicit unique `drain.holder` (shared default refuses to start), round-trips a session. |
| `cargo test -p kian-cli --test postgres_s8` | `KIAN_PG_URL` | **S8** (below): two replicas, kill -9, takeover, no split brain. |

Absent env → every one of them **skips with a `SKIP:` log line, never fails** (verify with
`env -u KIAN_PG_URL cargo test -p kian-cli --tests`: the suites report `0 passed; 0 failed` with
skip lines, and the rest of the tree stays green — that is the CI default).

## The S8 e2e and what it proves

`s8_two_replicas_kill9_takeover_no_split_brain`
(`crates/kian-cli/tests/postgres_s8.rs`) is S1 lifted to multi-process scale:

1. Two `kian serve` child processes (holders `replica-a` / `replica-b`) share ONE postgres
   database; a task is submitted and claimed by replica A.
2. A is `kill -9`'d mid-provider-turn (no shutdown path, no end write).
3. B's recovery sweep takes over the expired lease (conditional claim admission), replays the
   stream, and completes the task — zero lost work, exactly one end write
   (`NoHalfTerminated` + `NoLostWork` across processes).
4. A stale-epoch probe write issued with A's frozen epoch, directly against the store, is
   **rejected** — safety came from epoch fencing, not from the lease. That is
   `NoSplitBrainWrite` between real processes.

Takeover is bounded, never sleep-raced: `lease_ttl_ms + heartbeat_interval_ms` (1.5 s + 0.5 s in
the test config), every wait a deadline-poll.

## Commands

The full gated surface in one run:

```bash
KIAN_PG_URL=postgres://$(whoami)@localhost/kian_e2e NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-store --test postgres && \
KIAN_PG_URL=postgres://$(whoami)@localhost/kian_e2e NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-exec --test postgres && \
KIAN_PG_URL=postgres://$(whoami)@localhost/kian_e2e NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-orchestrator && \
KIAN_PG_URL=postgres://$(whoami)@localhost/kian_e2e NO_PROXY=127.0.0.1,localhost \
  cargo test -p kian-cli --tests
```

`NO_PROXY=127.0.0.1,localhost` is required on macOS (the system proxy intercepts localhost).
Time the S8 leg alone (expected: seconds, bounded by the lease TTL):

```bash
KIAN_PG_URL=postgres://$(whoami)@localhost/kian_e2e NO_PROXY=127.0.0.1,localhost \
  time cargo test -p kian-cli --test postgres_s8 -- --nocapture
```

And the zero-credential default (nothing above runs; the tree stays green):

```bash
env -u KIAN_PG_URL cargo test --workspace
```

## Live run record

- **2026-11-01 (C56)**: executed against Homebrew postgres 17.11 on macOS (database `kian_e2e`,
  trust auth). `kian-cli --tests` under the gate: all suites green including `postgres_serve`
  (ran, passed) and `postgres_s8` (ran, passed, ~16 s wall). During the same card a genuine bug
  was found and fixed in the async `stop` path (see CHANGELOG / `kian-exec` async tools
  regression test); the S8/pg suites themselves needed no code changes. Per-suite counts are in
  the card handoff.
