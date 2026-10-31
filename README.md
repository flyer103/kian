# Kian — Super-agent Platform / Managed Agent Substrate

> **One-liner: the Temporal of agents — an open-source durable execution runtime that makes agent
> tasks killable, resumable, migratable and auditable, with the super-agent reference architecture
> as a first-class workload.**

- Implementation language: **Rust**
- Design inputs: frozen local snapshots in [a vendored archive/](a vendored archive/) (archived
  2026-10-31: the WeChat super-agent article, dsh/cordis docs, two internal design notes); structured
  synthesis in [the design notes](the design notes)

## Docs map

| Document | Type | Contents |
|----------|------|----------|
| [docs/SPEC.md](docs/SPEC.md) | Spec (WHAT) | Positioning, user interfaces, scope, milestones |
| [design/DESIGN.md](design/DESIGN.md) | Design (HOW) | Combined architecture, execution model, super-agent reference architecture, plugin system, crate map |
| [design/decisions/](design/decisions/) | Decisions (WHY) | ADRs: key decisions with alternatives |
| [the design notes](the design notes) | Research | Structured digest of the four design inputs |
| [a vendored archive/](a vendored archive/) | Archive | Frozen local snapshots of the design inputs (read-only) |

## Core claim

**Agent Orchestrator = a durable execution runtime where the Session is the one first-class
durable entity, the event stream is the sole truth, and the drain is the execution hold interval.
The agent loop is just a control flow inside a drain.**

The four super-agent characteristics (continuous / efficient / proactive / persistent) are not
hard-coded core capabilities but **composition patterns** over Kian primitives (Router/Worker
two-layer, event subscriptions, async promote, BYOM endpoints) — the core stays a narrow waist
(A10).

## Status

Design phase — aligned with the owner; no code yet.
