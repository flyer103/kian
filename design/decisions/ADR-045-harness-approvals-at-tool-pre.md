# ADR-045: Harness tool approvals dispatch at `tool/pre`

- **Status**: accepted (owner gate, 2026-11-20). Implements the phase-2 slice of
  [HARNESS-BRIDGE](../HARNESS-BRIDGE.md) §4.4/§10.
- **Context**: phase 1 answers a harness's pre-execution approval request with the declarative
  allow/deny policy only ([HARNESS-BRIDGE](../HARNESS-BRIDGE.md) §4.4). The ReAct loop already
  dispatches the same class of decision to the operator plugin bus (`tool/pre`, PLUGIN-SPEC §4.4
  step 2), where a `veto:tool` sidecar can deny before the effect. Two governance planes therefore
  exist for the same operation: kian-loop tools are vetoable by sidecars, harness tools are not.
  The obvious alternative — a Claude `PreToolUse` hook shim that calls back into kian — covers
  *auto-approved* harness tools too, but needs a new local IPC surface, authentication and a
  failure policy of its own, and the harness's own hook configuration becomes part of the contract.
  Start in-process, on the approvals the harness already asks kian about.
- **Decision**:
  1. **Dispatch.** On every `ApprovalRequest` the driver evaluates the declarative policy first
     (the existing "declarative gate"), then dispatches `tool/pre` with a `ToolCallView` whose
     `surface` is `"harness"`, `call_id` is the harness call id (Claude `tool_use` id, Codex
     `itemId`), `name` is the harness's tool name verbatim, `iteration` is the mapped turn,
     `arguments` is bounded/redacted exactly like the ReAct site, and `permission` carries the
     declarative decision. The view is built by assembly (the bus stays outside the narrow waist).
  2. **Deny wins.** A valid `veto:tool` denial — or the bus's fail-closed class default — flips
     the answer to deny regardless of the declarative decision; otherwise the declarative decision
     stands. The denial is recorded on the pending `tool/call.permission` as
     `{"evaluated":"deny","policy":"hook","matched":"<plugin>:tool/pre","reason":…}`; an allowed
     call keeps `policy:"configured"`. This mirrors the ReAct combination rule (a hook can only
     narrow the declarative gate, never widen it).
  3. **Ordering.** The consultation happens after the adapter surfaced the request and before the
     harness answer is written, so the harness is blocked on the answer: the call record is still
     log-before-execute, and a deny means the tool never runs. The adapter appends the call on the
     answer, as today.
  4. **No new config, no new event kinds, no wire change.** The bus is whatever the deployment
     assembled; without a bus the behavior is byte-identical to phase 1 (`policy:"configured"`).
  5. **Scope.** Only tools the harness itself gates (Claude `can_use_tool`, Codex actions the
     `on-request` policy routes to kian). Auto-approved harness tools, harness-internal subagents
     and background jobs stay outside this contract; covering them is the deferred shim's job.
- **Alternatives considered**:
  1. **Claude `PreToolUse` shim now** — rejected for this slice: a new local IPC/auth surface plus a
     harness-side hook configuration, and the coverage gain (auto-approved tools) is a different
     security claim. It remains the phase-3+ path for full coverage.
  2. **Keep approvals config-only** — rejected: it leaves two governance planes and makes the
     "one plugin, many harnesses" goal false for the most important hook.
  3. **Per-harness hook namespaces (`harness/claude/…`)** — rejected: the vocabulary is the same
     decision; the `surface` field already tells a plugin where the call came from, and a second
     namespace would duplicate the payload contract.
- **Consequences**:
  - one `tool/pre` site serves ReAct and both bridges; a plugin subscribed to `tool/pre` sees
    harness-gated tools with `surface:"harness"` (an additive value in PLUGIN-SPEC §3.2.5/§3.5);
  - a `veto:tool` sidecar can now block a Claude Code/Codex tool before it runs, and the denial is
    auditable on the call record;
  - the driver gains one port (`kian-harness`) and assembly one adapter; no event or wire change;
  - `tool/post` for harness calls stays phase 2 (the port exists, the bus wiring does not);
  - a harness that cannot supply a stable call id would have to stay config-only; both adapters
    supply one.
- **Rollback Trigger**: a harness approval whose call id cannot be correlated reliably (then that
  harness's row reverts to config-only and the matrix says so), or a hook denial path that cannot
  be kept ahead of the harness answer (the ordering guarantee would be false).
- **Cost Impact**: one port + one assembly adapter + one view value; tests at the driver and the
  assembly seam; no new crates.
