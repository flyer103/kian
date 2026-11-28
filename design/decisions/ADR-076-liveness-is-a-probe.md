# ADR-076 — Liveness is a probe, not the metrics surface

- Status: accepted (2026-11-28, supervisor liveness)
- Related: ADR-041 (a claim chain without an end is closed as failed), ADR-077 (a graceful
  shutdown releases its drain claim)

## Context

`kian serve` exposes one observability route, `GET /metrics`, and it is a **derived** surface: it
snapshots the engine counters and projects the cache, usage and super-agent reports over the whole
store (`crates/kian-cli/src/server.rs:3011`). Its cost therefore grows with the store: on a
production database holding 342 sessions and a ~1.4 GB stream, one render took 6–7 s.

The supervisor (`kian-watch.timer`) probed `/metrics` with a 5 s cap. On 2026-11-28 three
consecutive timeouts (00:08:06–00:08:11) marked a **healthy, serving** process as dead and
restarted it. The turn being served had already delivered its reply; the damage was the restart
itself — under ADR-041 every restart that interrupts an in-flight drain leaves an un-ended
`drain/claimed`, and the third one ends the session as `failed`.

The miss is a category error rather than a slow report. A supervisor needs an O(1) signal that the
listener serves; `/metrics` is a read model whose correctness is *projection* truth, and any
projection-backed probe eventually times out on a healthy process as the store grows. Caching the
report only moves the cliff: the cache still has to build it, and a stale cache reports a wedged
process as healthy.

## Decision

1. `kian serve` mounts `GET /healthz` (`design/API-CONTRACT.md` §1.1), always on, no token.
2. The handler reads **process state only** — no store read, no projection, no session lookup,
   no bundle probe. It answers `200` with `{"ok":true,"version":"<crate version>"}`. `ok` is the
   stable field; the body is additive-only (§9.2).
3. It is **liveness, not readiness**: `200` means "the listener accepted and answered this
   request". It makes no claim about the store, a provider, a channel or a session — Kian still
   has no readiness surface, and this ADR does not add one.
4. `/metrics` keeps its meaning and its cost, and is never a liveness signal. The operator probe
   points at `/healthz` with a 5 s cap.

The wire change lands contract-first: the §1 row and §1.1 ship with the handler and the test that
pins both.

## Alternatives considered

- **Raise the probe timeout.** Applied as an immediate operational mitigation, rejected as the
  design: it detects a real hang later and leaves the probe's cost growing with the store — the
  next larger store reproduces the same false positive.
- **Cache the metrics report.** Rejected: the cache must still build the report, and a stale entry
  would report a wedged process as healthy — the opposite of a liveness signal.
- **`GET /metrics?shallow=1`.** Rejected: it overloads a surface whose top-level keys §9.1
  freezes, and "shallow" is this probe wearing a more expensive name.
- **A TCP connect test in the probe script.** Rejected: no runtime change, but it tests the kernel
  backlog rather than the runtime — a process that accepts a connection and never answers (the
  observed 6 s render) passes it.
- **A `kian status` CLI.** Rejected: the supervisor already speaks HTTP over loopback; a CLI
  probe adds a process spawn and a config load to the hot path.

## Consequences

- One always-mounted, unauthenticated loopback path. It returns no store data, so it does not widen
  the trust surface beyond `/metrics`.
- The answer stays O(1) as the store grows: a supervisor no longer attributes health to
  projections, and a slow `/metrics` is visible as cost instead of as downtime.
- A process that accepts connections but does not progress still answers 200. That is the line
  between liveness and readiness, drawn deliberately: Kian's supervisor restarts a listener that
  is not serving, and does not police session progress through the process probe.

## Limits

- No readiness probe and no dependency checks (store, provider, channels, bundle).
- No negotiation: `version` is the serving binary's crate version, and a client must not branch on
  it.

## Acceptance

- L2: `crates/kian-cli/tests/e2e_serve.rs` boots the assembled server and asserts `200` plus
  `{"ok": true, "version": <crate version>}` from `GET /healthz` — with no token and no session.
- L3: the deployed supervisor probes `/healthz` inside a 5 s cap and stops self-healing a serving
  process while `/metrics` takes longer than that.
