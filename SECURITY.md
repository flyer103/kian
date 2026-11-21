# Security policy

Kian runs model-driven commands on machines you own, keeps a durable record of everything a session
did, and holds credentials for providers and channels. A bug in that surface can be worse than a
crash, so please report it privately first.

## Reporting a vulnerability

Use GitHub's private advisory flow: **Security → Report a vulnerability** on
<https://github.com/flyer103/kian>. That opens a draft advisory only the maintainers can see.

Please do **not** open a public issue for anything that could be exploited before a fix ships. If
the advisory flow is unavailable, contact the maintainer ([@flyer103](https://github.com/flyer103))
and say only that you have a security report — details go in the advisory.

A useful report contains:

- the version or commit (`kian --version`, or the tag),
- the configuration class involved (provider, channel, worker, plugin, harness),
- what an attacker gains, and the smallest reproduction you have,
- whether the bug needs local access, a network position, or only untrusted input.

## What is in scope

| Area | Examples of a real finding |
|---|---|
| Durability and fencing | a stale drain can write after takeover; a crash path can produce `completed`; replay returns different bytes than the original request |
| Event stream integrity | an append that rewrites history; a projection that trusts a column instead of the stream; a state transition the catalog forbids |
| BYOM execution | a command reaches a worker the session is not bound to; a path guard or the 1 MiB bound can be bypassed; a re-dispatched command executes twice with a different effect |
| Credentials | a provider key or channel token reaches a log, an event payload, an error message, or `session export` |
| Channels | an allowlist bypass (WeChat `topics`, iMessage `watch.participants`), or a message from an untrusted sender admitted as trusted input |
| Plugins and harness bridges | a sidecar that runs without its manifest digest check, or a `tool/pre` decision that can be bypassed by a harness-owned tool |
| HTTP surface | an unauthenticated JSON-RPC or webhook call that mutates a session, or an SSE stream that leaks another session's events |

## What is not a vulnerability

- A model producing wrong or unsafe *content*: Kian guarantees durable execution, not model behavior.
- Running deliberately destructive commands you configured a worker to run: that is the product.
- The documented boundaries in [docs/SPEC.md](docs/SPEC.md) and the capability matrix in
  [design/HARNESS-BRIDGE.md](design/HARNESS-BRIDGE.md) — for example, at-least-once command
  delivery, or the absence of byte-level replay for projection-only harnesses.
- Missing hardening for a deployment that deliberately disables it (for example
  `workers.allow_public: true`).

## Supported versions

The project is pre-1.0: only the latest release and `main` receive fixes. There is no backport
branch, and no guaranteed response time — this is a maintained-in-the-open project, not a product
with an SLA. Advisories are published once a fix is available.
