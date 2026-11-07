# Changelog

Kian is built in rounds ("phases"), each ending in an acceptance gate. This file records what
landed, when, and how it is proven. Scenario definitions (S1–S8) live in [docs/SPEC.md](docs/SPEC.md)
§2.5; the phase scope tables are in SPEC §5. Entries are ordered oldest first.

Two labels are used throughout:

- **default-green** — runs in `cargo test --workspace` on any machine, no credentials, no services.
- **env-gated** — runs only when its environment variable is present; absent, the suite skips with a
  log line and never fails. Gated surfaces are listed in
  [README → Environment contract](README.md#environment-contract).

---

## Phase 0 — 2026-10-31 — design baseline (documentation only)

- `docs/SPEC.md` (WHAT), `design/DESIGN.md` (HOW), and the first six ADRs (Rust foundation,
  event-sourced core, super-agent as a profile, Cordis-inspired plugins, fencing via `expectedSeq`,
  model slots).
- `AGENTS.md` with the hard constraints that bind every later change; `the design notes`
  synthesizing the four design inputs; frozen snapshots of those inputs in `a vendored archive/`.

## Phase 1 — 2026-11-01 — MVP: durable sessions (S1–S3) — default-green

- **Workspace**: eight crates (`kian-core`, `kian-store`, `kian-orchestrator`, `kian-llm`,
  `kian-loop`, `kian-plugin`, `kian-endpoint`, `kian-cli`).
- **Event schema v0**: 15 event kinds; session state machine; projection engine; model-context
  derivation. State is read only from the projection of the append-only stream.
- **EventStore on sqlite**: single-transaction appends, `expectedSeq` CAS and epoch fencing — stale
  writes from a dead drain are rejected, not repaired.
- **Admit → Wake → Drain → Promote**: durable inbox rows before any wake, advisory (foldable, losable)
  wakes, promotion only at provider-turn safe boundaries, per-session serial drains, inbox rebuild at
  startup so wakes lost to a crash are re-derived.
- **Three-layer recovery**: a pure error-classification function, a declarative policy table owned by
  the caller (`recovery:` in `kian.yaml`, required — no silent defaults), a one-shot guard per
  recovery kind and a token/wall-clock budget with refund semantics.
- **`kian-llm`**: openai-compatible provider, model slots fixed per session, persist-before-send
  (request bytes are an event before the call flies).
- **`kian-cli`**: `kian serve` (JSON-RPC `/rpc`, SSE `/sessions/{id}/events`, `GET /metrics`) plus
  `session inspect` / `session replay` / `session export` / `wake-now` / `lease inspect` /
  `recovery budget reset` / `cache-stats`.
- **Acceptance**: S1 crash recovery (`chaos_s1.rs`, six kill points), S2 steer/interrupt honesty
  (`s2_steer_interrupt.rs`), S3 cache-byte stability (`s3_cache_stability.rs`) — all default-green.

## Phase 2 — 2026-11-01 — super-agent bundle + BYOM endpoints (S4–S5) — default-green

- **Four additive event kinds** (`thread/started`, `thread/completed`, `evidence/verdict`,
  `channel/delivered`); the durable set grows 15 → 19.
- **`kian-superagent`** (the bundle, never the core): resident Router profile, Worker session
  templates, Thread delegation, event subscriptions, channel adapters with output policies
  (IM ≤100 words; structured async delivery), and a mock channel for demos.
- **Evidence gate v1**: bounded, declarative stream checks; a failed gate yields an honesty-preserving
  soft failure — a worker that cannot prove completion never reaches "delivered".
- **BYOM**: `kian-exec` (KiandProto server half: connection manager, endpoint registry, durable
  command queue, heartbeats) + `kian-endpoint` (endpoint-side SDK) + `kiand` (the daemon binary).
  At-least-once delivery with command-id dedupe ([ADR-007](design/decisions/ADR-007-kiand-delivery-semantics.md));
  a queue timeout is an honest `Timeout` and the row is `abandoned`, never a fake success.
- **Watchdog v1**: endpoint heartbeats; an offline endpoint admits a durable health input to the
  affected worker session.
- **Acceptance**: S4 (`s4_superagent_demo.rs`, 8 tests including the gate-failure negative path) and
  S5 (`s5_byom_e2e.rs`, real `kiand` child process) — default-green.

## Phase 3 — 2026-11-01 — article-sync: recall, admission economics, channels (S4-extended, S7) — default-green

- **One additive event kind** (`input/spilled`); the durable set grows 19 → 20, and stays there.
- **Inline/Spill admission shaping**: an oversized channel payload admits as a bounded Inline view
  (excerpt + association ids + continuation handle); the verbatim raw is appended in the same atomic
  consumption write as a non-context-bearing sibling. Shaping happens once, at admission — committed
  context is never rewritten (cache-byte stability).
- **Recall toolpack (Chain of Recall)**: read-only `recall_event` / `recall_neighbors` /
  `recall_thread` over a `RecallPort`; every output is itself Inline/Spill-shaped so multi-hop recall
  composes; unknown ids are honest tool errors.
- **Two-level admission filtering**: a `classes:` table (`candidate` / `explicit-only` / `ignore`,
  most-specific-wins) evaluated before the rule set and before any model call; drops are audit lines
  and counters, not rows.
- **Async `start()`/`stop()` tools** ([ADR-008](design/decisions/ADR-008-async-tool-completion-admitted-input.md)):
  immediate ack with a job id; the completion returns as an admitted input, durable before its wake
  and promoted at the next safe boundary; `stop` labels the result `cancelled`. Default off.
- **Many-to-many passthrough fan-out**: a topic maps to the set of workers that served it; one event
  admits one row per subscriber, idempotent per `(session, event_id)`.
- **Speculative execution**: an additive, audit-visible delegation marker with a stream-derived
  outstanding cap and honest tool errors; a discarded speculation audits every token and delivers
  nothing.
- **Watchdog Session v2**: a model-in-the-loop progress judge driven by `type: timer` channel ticks;
  `intervene` admits a steer row durable-before-wake, `escalate` replies under the IM policy, and
  every tick (including a healthy no-op) is audited in the watchdog's own stream.
- **Batching metrics + ablation benchmark**: `superagent.router_batching` on `/metrics`
  (turn count, batch size, amortized ms/input) and a deterministic passthrough-vs-Router ablation
  that asserts directionality, never the article's numbers.
- **Untrusted-input marking**: `"untrusted": true` in the provenance wrapper for configured channels
  (real channels default untrusted); the marker survives every recall hop.
- **Real lark channel adapter**: Feishu IM receive (chat poll) and send over the open APIs, env-only
  credentials, fail-closed startup.
- **Acceptance**: S4-extended legs (recall, async, watchdog) and S7 (audit export/replay,
  **demonstrated**) — default-green.

## Phase 4 — 2026-11-01 — hosted foundation: postgres, multi-replica, SDK freeze, ecosystem surfaces (S6, S8)

- **`kian-sdk`** (twelfth crate): the wire-only embedded client extracted from `kian-cli` — typed
  methods, the SSE helper, matchable error codes, golden wire fixtures, and **no dependency on any
  kian crate**. Frozen additive-only after this round; a breaking change needs a new plan round.
- **Postgres dual backend** in `kian-store` and `kian-exec`: an enum-dispatch `EventStore` with
  identical semantics (single-statement end write, batch CTE append, PK-race classification), one
  server / one database / one pool. URL is env-only (`KIAN_PG_URL`) and postgres mode requires an
  explicit unique `drain.holder` — both fail closed. Export parity (byte-identical JSONL) is the
  compat golden. — **env-gated** (`KIAN_PG_URL`)
- **Multi-replica semantics** ([ADR-009](design/decisions/ADR-009-multi-replica-claim-admission.md)):
  conditional claim admission (a claim lands only when no claims row exists, the lease expired, or the
  holder is the same process), postgres LISTEN/NOTIFY as an advisory *accelerator* only (notify after
  commit; sweep + startup scan bound any miss), and single-statement row leases on `exec_queue` /
  `async_jobs` that are liveness-only — safety stays with epoch fencing.
- **GitHub channel + verified webhook receiver** ([ADR-011](design/decisions/ADR-011-inbound-webhook-plane.md)):
  a fourth server surface at `POST /webhooks/github` that verifies **before** it parses — body-size cap
  (413) and content-type gate (415) precede signature work, HMAC-SHA256 constant-time compare (401,
  zero parses), then a per-source token bucket (429 with an audit line), then admit-only delivery with
  `(session, event_id)` idempotency. Token and secret are env-only.
- **`kian-dsh`** (thirteenth crate, S6, [ADR-010](design/decisions/ADR-010-dsh-session-mapping.md)):
  a `LoopDriver` that drives a `dsh --profile sdk` child over stdio JSON-lines and maps its session
  events onto existing kian kinds. Config-gated and default off (`superagent.harness.dsh`); depends on
  `kian-core` only (narrow waist).
- **Zero new event kinds** this round; the durable set stays 20.
- **Acceptance**: S8 multi-replica takeover **demonstrated** (`postgres_s8.rs`, two real `kian serve`
  replicas, real `kill -9`, stale-epoch probe rejected) — **env-gated**; S6 **demonstrated by
  fixtures** in default CI (`driver_bridge.rs`, `dsh_conformance.rs`, `dsh_bridge.rs`), with the live
  leg **executed** against real dsh 0.2.1-rc.5 (`dsh --profile sdk`, `KIAN_DSH_BIN` set) and coming
  back an honest negative: the stock dsh SDK app is client-initiated JSON-RPC and never sends `ready`
  within 30 s, so the bridge stopped at `SoftFailed { reason: "peer never sent ready within 30s" }`
  exactly as ADR-010 §3 requires, instead of manufacturing a completion. **S6-live stays open** until
  a contract-v0 dsh-side peer exists; the fixture suites remain the CI proof.

## Phase 5 — 2026-11-02 — live positives, dogfood, and field fixes

- **S6 live positive landed** (`contrib/dsh-v0-peer/dsh-v0-peer.mjs`, kian-59): a zero-dependency Node
  peer adapting stock `dsh --profile sdk` (JSON-RPC 2.0) onto the frozen ADR-010 contract v0 — `ready`
  emitted immediately, `session.event` notifications forwarded as v0 session-event frames, `input`
  frames translated to `session/prompt`. Driving one real input through peer → dsh → real
  `deepseek-chat` appended the full mapped chain (turn/start, input/admitted, step/request,
  assistant/message, turn/end) with an honest `Settled` outcome — the S6 live leg is now **green**,
  upgrading the 2026-11-01 honest negative. Re-run: see `docs/runbook-dsh.md`.
- **Dogfood example** (`examples/issue-watch/`, kian-60): a real long-running workload on
  `flyer103/kian` — TimerChannel ticks → `gh issue list` → Router delegates triage → worker drafts a
  reply (dry-run by default) → everything in the durable audit stream. Proven live: real issue triaged
  (severity, decision, draft in-stream), and `demo-restart.sh` shows `kill -9` + reboot resuming the
  same session from the event stream. `RUNLOG.md` records the real data.
- **Wire dialect v2** (kian-61, breaking): strict OpenAI-compatible serialization — built-in tools
  renamed to dotless spellings (`fs_list`/`fs_read`/`fs_write`; dotted forms still invocable for
  replay), wire-level `tool_calls`/`tool_call_id` emitted on tool round trips, `AgentDef.tools`
  enforced fail-closed. Found by the dogfood run replaying persisted `step/request` bodies against the
  real DeepSeek API (HTTP 400/422); constraint-3 event recorded in DESIGN (golden hash updated in the
  same commit). After: direct-to-DeepSeek green — 13 sessions, 59 requests, 0 failures.
- **Router re-arm** (kian-62): a resident Router whose turn soft-failed no longer stayed terminally
  frozen across reboots. Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident
  with an observable `[re-arm]` cause input on the successor stream; soft-failed sessions re-open the
  same pinned session. Zero new event kinds; DESIGN §4.6 records the semantics.

## Phase 6 — 2026-11-04 — bundle-domain policy round: ticket semantics, mandate, attention economics — default-green

Round 6 (cards kian-63…kian-78) adopted the dsh-superagent-prototype's domain-policy layer onto
the bundle. **Zero new event kinds — the durable set stays 20**; every addition is an additive
payload key on a frozen wrapper, a disposable projection, or a config/template artifact.

- **Ticket stage projection + `kian tickets`** (kian-64): "what is it waiting for" as a pure
  first-match derived projection over the worker stream + inbox + outbox view
  (`delivery-retry / awaiting-answer / awaiting-review / blocked-failed / queued / idle /
  working`), with the additive `state` key on completion-report content; the read-only CLI
  stage board. Honest v1 shape: a Ticket = one worker session; ended tickets resume via fresh
  Eval; the panel is the CLI.
- **Closed decision-reason vocabularies** (kian-65): `reason=<code>` on every drop/admit audit
  line, per-code metrics counters, and the judge-side `decision:<accept|stop|ignore|unavailable>`
  tag convention in the default template.
- **Output-side speak-rules** (kian-66): the whether-to-send gate before output shaping
  (`superagent.speak_rules`; suppression = honest tool error + audit + counter, no send);
  `ack-first-only` confirm-once memory rebuilt from the stream.
- **Thread/Correlation intake projection** ([ADR-013](design/decisions/ADR-013-thread-intake-projection.md),
  kian-67): config-owned canonical thread keys used as routing topics; stream-derived read
  positions (lark/GitHub cursors) or honest idempotent re-poll — no position table; correlation
  adds context, never routes; fan-out grants no authority.
- **Follow-up ownership routing** (kian-68): registry liveness projection; fan-out admits to
  live subscribers only; an all-ended topic releases to the Router (`released_to_eval` fresh
  Eval); `spawn_worker` refuses a live worker's topic naming the owner.
- **Policy/prompt versioning + drift marking** (kian-69): `[policy <label>]` render header
  (persisted prompts replay byte-identically), boot-time drift marking that never rewrites live
  sessions, config-gated one-shot policy-changed marker input.
- **Delegation Mandate** ([ADR-012](design/decisions/ADR-012-delegation-mandate.md), kian-70,
  ADR landed): the immutable authority object `{strength, scope, granted_by, source,
  policy_version}` additive on the frozen delegation wrapper — verbatim in `thread/started`,
  model-visible as the worker's first message; issued only at `spawn_worker`; revalidated
  before follow-up routing (stale `inferred` releases to fresh Eval).
- **Outbox bounded retry** (kian-71 + kian-76): a disposable projection over `tool/call`
  intent + `channel/delivered` receipt plus a bounded driver (2 s doubling to 60 s, 6 attempts,
  per-(ticket, conversation) ordering); park is honest — `delivery-retry` stage, named on
  `kian tickets`, survives reboot; additive `attempt`/`first_attempt_ms` on new receipts.
- **issue-watch v2 multi-Ticket dogfood + github poll-receive mode** (kian-72): `since`-cursored
  REST polls beside the untouched webhook surface; one Ticket per live issue on the real repo,
  composing the whole Wave A/B stack; v1 kept as `config-timer.yaml`.
- **P6c live validation** (kian-73): the eight-scenario protocol executed live against
  `flyer103/kian` (real DeepSeek + GitHub REST, `DRY_RUN=1`; `examples/issue-watch/RUNLOG.md`
  §7): six scenarios PASS live with captured evidence; scenarios 2–3 honest negatives (the
  structural `spawn_topic` live-spelling gap), every claim cited by session id + seq.
- **Wave-C gap fixes** (kian-74…77, from the RUNLOG §7.2 gap report): `User-Agent` on every
  github/lark HTTP client + `flaky-proxy.py` delivery-leg fault mode (kian-74); the lenient
  live spawn spellings — live topic binding, mandate strength and outbox ticket aliasing
  (kian-75); outbox park rebuilt at boot and surfaced on the stage board (kian-76); exec
  secret hygiene — shared core `SecretRedactor` + `ExecEnvPolicy`, secrets redacted from
  `tool/result` before persist (kian-77, HIGH; the round's one cross-bundle-fence commit,
  filed as its own card off the live gap report).
- **Acceptance**: the P6c checklist is recorded in the README Phase-6 section (all eight
  scenarios done — live, or fixture-proven with the live negative root-caused and fixed);
  the four RUNLOG §7.2 negatives are closed by kian-74…77. S1–S8 unchanged.

## Phase 7 — 2026-11-07 — event-model enhancements borrowed from CMA — default-green

Round 7 (cards kian-79…kian-83, plan `round7-plan.md`) borrowed event-model ideas from Anthropic's
Claude Managed Agents catalog (usage facts, a persisted interrupt intent, derived event ids,
redaction/permission provenance) as design inspiration only — no wire-protocol dependency.
**Exactly one new event kind (`control/interrupt`): the durable set grows 20 → 21.** Everything
else is an additive payload key on a frozen wrapper, a pure projection, or a config-first
capability; request bytes, tool schemas, `FROZEN_CHAT_BODY_SHA256` and `derive_model_context`
bytes are untouched (each card asserts this — constraint 3).

- **Provider usage + structured recovery on `turn/end`** (kian-79, C67): the openai-compat
  provider parses SSE `usage` objects verbatim (last frame wins; a usage-only frame never flips
  the empty-response liveness flag); `turn/end` carries `"usage"` verbatim plus
  `"error": {"class", "action"}` on soft-failure/error outcomes (the C10 pure classifier +
  declarative policy, `"none"` when no rule); legacy payloads are byte-identical when unobserved.
  New pure projection `kian-cli::usage_metrics` powers `kian usage-stats`, the `GET /metrics`
  `"usage"` aggregate and `session inspect` usage lines — no pricing, no normalization in core.
  Tests: `usage_frame_at_stream_end_is_parsed_verbatim_after_content`,
  `mid_stream_and_final_usage_frames_both_surface`, `usage_only_stream_is_still_an_empty_response`,
  `turn_end_carries_usage_for_tool_calls_and_final_answer` (incl. the A2 context golden),
  `turn_end_soft_failure_carries_partial_usage_and_classified_error`,
  `turn_end_without_usage_is_byte_identical_to_the_legacy_shape`,
  `recovery_audit_carries_class_and_policy_action`,
  `usage_surfaces_inspect_stats_and_metrics_over_a_seeded_store`.
- **Durable interrupt intent** (kian-80, C68, [ADR-014](design/decisions/ADR-014-durable-interrupt-intent.md)):
  the 21st kind `control/interrupt` (drain-activity, non-context). `interrupt` admits a reserved
  control-envelope inbox row (idempotent per `(session, target_epoch)`) alongside the in-memory
  flag; the owning drain consumes it at a safe boundary, appends the audit event and settles
  `interrupted` — the intent survives a drain crash, and a takeover resumes-then-interrupts
  honestly. Stale rows (target episode already settled) append `"stale": true` and end nothing;
  idle/terminal sessions keep `not_draining`/`unknown_session` and admit nothing. `session
  inspect` gains the `pending control` line. Tests:
  `durable_interrupt_intent_survives_crash_and_takeover_settles_interrupted`,
  `repeated_interrupts_in_one_epoch_dedupe_to_one_row_and_one_event`,
  `stale_epoch_control_row_is_audit_only_and_does_not_end_the_session`,
  `control_row_racing_the_settle_flips_the_end_write_to_interrupted`,
  `interrupt_of_idle_or_terminal_session_admits_no_control_row`,
  `control_interrupt_leaves_model_context_unchanged`,
  `additions_only_regression_round6_all_twenty_kind_bytes_are_frozen` (golden count now 21).
- **Derived event id + redaction provenance** (kian-81, C69):
  `kian_core::events::event_id` = `"evt_" + uuid5(session, "kian:event:<seq>")` — pure, never
  stored, never an envelope field; mirrored dependency-free in the SDK
  (`KianClient::event_id`, golden-pinned via an independent implementation). `kian session export
  --with-event-ids` adds the id per JSONL line (opt-in; default export bytes unchanged; the
  augmented lines round-trip through import). `SecretRedactor::redact_with_report` emits
  `{kind, name, count}` provenance (never secret values); the report rides the additive
  `redactions` key on `tool/result` payloads outside `content`, omitted when empty. Tests:
  `event_id_golden_vectors_match_rfc4122_v5`,
  `event_id_is_deterministic_and_distinct_per_seq_and_session`,
  `event_id_matches_the_core_golden_vectors`,
  `default_export_bytes_are_unchanged_without_the_flag`,
  `with_event_ids_export_adds_stable_ids_and_round_trips`,
  `report_counts_all_three_kinds_and_text_matches_plain_redact`,
  `remote_exec_result_and_row_carry_the_redaction_report`,
  `tool_result_payload_carries_redactions_outside_content`,
  `redactions_payload_key_never_enters_the_model_context`.
- **Per-call tool permission provenance** (kian-82, C70, [ADR-015](design/decisions/ADR-015-tool-permission-provenance.md)):
  `agent.permissions` maps tool name → `allow`/`deny` (closed vocabulary, fail-closed parse —
  unknown value, key ∉ `tools`, or a table on a Router/Watchdog judge agent refuse boot).
  Evaluated once per call **before** the `tool/call` append (log-before-execute holds on both
  arms) and recorded as the additive `permission: {evaluated, policy, matched}` key — allows are
  audited exactly like denials; a deny never invokes and appends an honest-error `tool/result`
  the model reacts to. Absent table = `tool/call` payload byte-identical to pre-C70. Tests:
  `permission_is_a_closed_vocabulary_fail_closed`, `permissions_unknown_value_refuses_boot`,
  `permissions_key_absent_from_agent_tools_refuses_boot`,
  `permissions_on_judge_agents_refuse_boot`,
  `allow_path_invokes_tool_and_records_provenance_default_vs_configured`,
  `deny_path_does_not_invoke_and_emits_honest_error_result`,
  `absent_permissions_keep_tool_call_payload_byte_identical_to_today`,
  `permission_payload_key_never_enters_the_model_context`.
- **Closeout** (kian-83, C71): SPEC §5 Phase-7 record, DESIGN payload-contract alignment
  (§3.2/§3.3), README status table (21 kinds) + Phase-7 operator sections, this entry, docs-map
  link check green.

Verification notes (honest): all four gates green on the final tree (build, `cargo test
--workspace` 91 suites 0 failures, clippy `--all-targets -D warnings`, fmt `--check`), docs_map
green. Env-gated suites (`KIAN_PG_URL`, `KIAN_DSH_BIN`, live provider creds) skipped by design in
the default run — skip-never-fail held. Not exercised: a live-provider usage frame (fixture SSE
streams only), the postgres path of `usage-stats` (identical code path to `cache-stats`), a real
two-process cross-replica interrupt (the durable-intent path is exercised in-process over a
killed owner), and a full `kian serve` end-to-end deny (assembly is compile-checked and
config-tested). Deferred this round: the `ask`/confirmation permission plane and token pricing
(out-of-scope records in SPEC §5 Phase 7).

## Gated surfaces — current live-run status

| Surface | Gate | Status |
|---------|------|--------|
| Multi-replica / postgres (S8) | `KIAN_PG_URL` | Suite in place; **executed green** in CI on every push since 2026-11-02 (postgres service container; the job also asserts the gated suites ran, not skipped) |
| dsh bridge (S6 live leg) | `KIAN_DSH_BIN` | Suite in place; **executed green** 2026-11-02 via the contract-v0 peer (`contrib/dsh-v0-peer/`) against dsh 0.2.1-rc.5 + real `deepseek-chat` — full mapped chain, honest `Settled`. (The 2026-11-01 stock-dsh honest negative is superseded; runbook records both.) |
| Live provider smoke | `KIAN_API_KEY` | Suite in place; **not executed** — no provider key in the build environment |
| Live lark channel | `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` | Suite in place; **not executed** — no Feishu app credentials in the build environment |
| GitHub send / webhook | `KIAN_GITHUB_TOKEN` / `KIAN_GITHUB_WEBHOOK_SECRET` | Webhook + send covered by scripted fixtures, default-green; **executed live** 2026-11-04 (kian-73, the P6c dogfood — real REST polls and `deliver_structured` triage comments on throwaway issues of `flyer103/kian`, `DRY_RUN=1`); see `examples/issue-watch/RUNLOG.md` §7 |

These are deliberately honest gaps, not silent claims: the fixture-driven suites prove the semantics,
and the README run logs record each gated surface's real state — the dsh leg with its executed
outcome (an honest negative, kept as the true record), the rest as explicitly not yet executed.

## Unreleased

- **Second dogfood example** (`examples/kanban-watch/`, 2026-11-03): kian watching a local
  Hermes Kanban board — TimerChannel ticks → Router delegates a poll worker
  (`hermes kanban --board <board> list --json`) → stuck tasks (closed stage vocabulary:
  stalled-running / blocked-needs-input / review-idle / failure-loop) → one stage worker per
  (task, stage) that re-verifies via `show --json` and, in live mode (`DRY_RUN=0`) only, pushes
  the task forward (`reclaim` / `unblock` / `comment`, all carrying the `kanban-watch:` marker;
  dry-run by default). Model provider: the local vadis gateway (OpenAI-compatible,
  `127.0.0.1:8290`) — no external API key. Zero new event kinds. Proven live against a
  throwaway test board: precise stuck classification (a healthy running task not flagged, a
  blocked task flagged `blocked-needs-input`), stage worker judging needs-human (non-mechanical
  block) without writing, (id, stage) dedupe across ticks 3–4, and kill + reboot resuming the
  same Router session (epoch 2, seq continued, charter re-submit deduped, tick 5 poll normal).
  Not re-run against the real `kian` board yet — first such run should be recorded here.
  Owner-gated next steps remain: making the repo public and executing the remaining gated
  surfaces with real credentials (live provider smoke, live lark channel). Tracked on the
  kanban board, not here.
