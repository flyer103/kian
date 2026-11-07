# Changelog

This file records what landed, when, and how it is proven. The contract lives in
[docs/SPEC.md](docs/SPEC.md); the design and its decisions live in [design/DESIGN.md](design/DESIGN.md)
and [design/decisions/](design/decisions/). Entries are ordered oldest first.

Two labels are used throughout:

- **default-green** — runs in `cargo test --workspace` on any machine, no credentials, no services.
- **env-gated** — runs only when its environment variable is present; absent, the suite skips with a
  log line and never fails. Gated surfaces are listed in
  [README → Environment contract](README.md#environment-contract).

---

## 2026-10-31 — design baseline (documentation only)

- `docs/SPEC.md` (WHAT), `design/DESIGN.md` (HOW), and the first six ADRs (Rust foundation,
  event-sourced core, super-agent as a profile, Cordis-inspired plugins, fencing via `expectedSeq`,
  model slots).
- `AGENTS.md` with the hard constraints that bind every later change; `the design notes`
  synthesizing the four design inputs; frozen snapshots of those inputs in `a vendored archive/`.

## 2026-11-01 — MVP: durable sessions — default-green

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
- **Acceptance**: crash recovery (`chaos_recovery.rs`, six kill points), steer/interrupt honesty
  (`steer_interrupt.rs`), cache-byte stability (`cache_stability.rs`) — all default-green.

## 2026-11-01 — super-agent bundle + BYOM endpoints — default-green

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
- **Acceptance**: super-agent demo (`superagent_demo.rs`, 8 tests including the gate-failure negative path) and
   (`byom_e2e.rs`, real `kiand` child process) — default-green.

## 2026-11-01 — article-sync: recall, admission economics, channels — default-green

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
- **Async `start`/`stop` tools** ([ADR-008](design/decisions/ADR-008-async-tool-completion-admitted-input.md)):
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
- **Acceptance**: extended legs (recall, async, watchdog) and audit export/replay (**demonstrated**) — default-green.

## 2026-11-01 — hosted foundation: postgres, multi-replica, SDK freeze, ecosystem surfaces

- **`kian-sdk`** (twelfth crate): the wire-only embedded client extracted from `kian-cli` — typed
  methods, the SSE helper, matchable error codes, golden wire fixtures, and **no dependency on any
  kian crate**. Frozen additive-only; a breaking change requires a new design decision.
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
- **`kian-dsh`** (thirteenth crate, [ADR-010](design/decisions/ADR-010-dsh-session-mapping.md)):
  a `LoopDriver` that drives a `dsh --profile sdk` child over stdio JSON-lines and maps its session
  events onto existing kian kinds. Config-gated and default off (`superagent.harness.dsh`); depends on
  `kian-core` only (narrow waist).
- **Zero new event kinds**; the durable set stays 20.
- **Acceptance**: multi-replica takeover **demonstrated** (`postgres_multi_replica.rs`, two real `kian serve`
  replicas, real `kill -9`, stale-epoch probe rejected) — **env-gated**; **demonstrated by
  fixtures** in default CI (`driver_bridge.rs`, `dsh_conformance.rs`, `dsh_bridge.rs`), with the live
  leg **executed** against real dsh 0.2.1-rc.5 (`dsh --profile sdk`, `KIAN_DSH_BIN` set) and coming
  back an honest negative: the stock dsh SDK app is client-initiated JSON-RPC and never sends `ready`
  within 30 s, so the bridge stopped at `SoftFailed { reason: "peer never sent ready within 30s" }`
  exactly as ADR-010 §3 requires, instead of manufacturing a completion. **The live leg stays open** until the contract-v0 dsh-side peer exists; the fixture suites remain the CI proof.

## 2026-11-02 — live positives, dogfood, and field fixes

- **live positive landed** (`contrib/dsh-v0-peer/dsh-v0-peer.mjs`): a zero-dependency Node
  peer adapting stock `dsh --profile sdk` (JSON-RPC 2.0) onto the frozen ADR-010 contract v0 — `ready`
  emitted immediately, `session.event` notifications forwarded as v0 session-event frames, `input`
  frames translated to `session/prompt`. Driving one real input through peer → dsh → real
  `deepseek-chat` appended the full mapped chain (turn/start, input/admitted, step/request,
  assistant/message, turn/end) with an honest `Settled` outcome — the live leg is now **green**,
  upgrading the 2026-11-01 honest negative. Re-run: see `docs/runbook-dsh.md`.
- **Dogfood example** (`examples/issue-watch/`): a real long-running workload on
  `flyer103/kian` — TimerChannel ticks → `gh issue list` → Router delegates triage → worker drafts a
  reply (dry-run by default) → everything in the durable audit stream. Proven live: real issue triaged
  (severity, decision, draft in-stream), and `demo-restart.sh` shows `kill -9` + reboot resuming the
  same session from the event stream. `RUNLOG.md` records the real data.
- **Wire dialect v2** (breaking): strict OpenAI-compatible serialization — built-in tools
  renamed to dotless spellings (`fs_list`/`fs_read`/`fs_write`; dotted forms still invocable for
  replay), wire-level `tool_calls`/`tool_call_id` emitted on tool round trips, `AgentDef.tools`
  enforced fail-closed. Found by the dogfood run replaying persisted `step/request` bodies against the
  real DeepSeek API (HTTP 400/422); constraint-3 event recorded in DESIGN (golden hash updated in the
  same commit). After: direct-to-DeepSeek green — 13 sessions, 59 requests, 0 failures.
- **Router re-arm**: a resident Router whose turn soft-failed no longer stayed terminally
  frozen across reboots. Boot-time deterministic successor walk (uuid5 chain) revives a frozen resident
  with an observable `[re-arm]` cause input on the successor stream; soft-failed sessions re-open the
  same pinned session. Zero new event kinds; DESIGN §4.6 records the semantics.

## 2026-11-04 — bundle-domain policy: ticket semantics, mandate, attention economics — default-green

The dsh-superagent-prototype's domain-policy layer was adopted onto the bundle. **Zero new event kinds — the durable set stays 20**; every addition is an additive
payload key on a frozen wrapper, a disposable projection, or a config/template artifact.

- **Ticket stage projection + `kian tickets`**: "what is it waiting for" as a pure
  first-match derived projection over the worker stream + inbox + outbox view
  (`delivery-retry / awaiting-answer / awaiting-review / blocked-failed / queued / idle /
  working`), with the additive `state` key on completion-report content; the read-only CLI
  stage board. Honest v1 shape: a Ticket = one worker session; ended tickets resume via fresh
  Eval; the panel is the CLI.
- **Closed decision-reason vocabularies**: `reason=<code>` on every drop/admit audit
  line, per-code metrics counters, and the judge-side `decision:<accept|stop|ignore|unavailable>`
  tag convention in the default template.
- **Output-side speak-rules**: the whether-to-send gate before output shaping
  (`superagent.speak_rules`; suppression = honest tool error + audit + counter, no send);
  `ack-first-only` confirm-once memory rebuilt from the stream.
- **Thread/Correlation intake projection** ([ADR-013](design/decisions/ADR-013-thread-intake-projection.md),
): config-owned canonical thread keys used as routing topics; stream-derived read
  positions (lark/GitHub cursors) or honest idempotent re-poll — no position table; correlation
  adds context, never routes; fan-out grants no authority.
- **Follow-up ownership routing**: registry liveness projection; fan-out admits to
  live subscribers only; an all-ended topic releases to the Router (`released_to_eval` fresh
  Eval); `spawn_worker` refuses a live worker's topic naming the owner.
- **Policy/prompt versioning + drift marking**: `[policy <label>]` render header
  (persisted prompts replay byte-identically), boot-time drift marking that never rewrites live
  sessions, config-gated one-shot policy-changed marker input.
- **Delegation Mandate** ([ADR-012](design/decisions/ADR-012-delegation-mandate.md)): the immutable authority object `{strength, scope, granted_by, source,
  policy_version}` additive on the frozen delegation wrapper — verbatim in `thread/started`,
  model-visible as the worker's first message; issued only at `spawn_worker`; revalidated
  before follow-up routing (stale `inferred` releases to fresh Eval).
- **Outbox bounded retry**: a disposable projection over `tool/call`
  intent + `channel/delivered` receipt plus a bounded driver (2 s doubling to 60 s, 6 attempts,
  per-(ticket, conversation) ordering); park is honest — `delivery-retry` stage, named on
  `kian tickets`, survives reboot; additive `attempt`/`first_attempt_ms` on new receipts.
- **issue-watch v2 multi-Ticket dogfood + github poll-receive mode**: `since`-cursored
  REST polls beside the untouched webhook surface; one Ticket per live issue on the real repo,
  composing the whole bundle stack; v1 kept as `config-timer.yaml`.
- **live validation**: the eight-scenario protocol executed live against
  `flyer103/kian` (real DeepSeek + GitHub REST, `DRY_RUN=1`; `examples/issue-watch/RUNLOG.md`
  §7): six scenarios PASS live with captured evidence; scenarios 2–3 honest negatives (the
  structural `spawn_topic` live-spelling gap), every claim cited by session id + seq.
- **Gap fixes** (from the RUNLOG §7.2 gap report): `User-Agent` on every
  github/lark HTTP client + `flaky-proxy.py` delivery-leg fault mode; the lenient
  live spawn spellings — live topic binding, mandate strength and outbox ticket aliasing
outbox park rebuilt at boot and surfaced on the stage board; exec
  secret hygiene — shared core `SecretRedactor` + `ExecEnvPolicy`, secrets redacted from
  `tool/result` before persist (HIGH; the one cross-bundle-fence commit, filed separately off the live gap report).
- **Acceptance**: the checklist is recorded in the README section (all eight
  scenarios done — live, or fixture-proven with the live negative root-caused and fixed);
  the four RUNLOG §7.2 negatives are closed. The acceptance contract is unchanged.

## 2026-11-07 — event-model enhancements borrowed from CMA — default-green

Event-model ideas from Anthropic's Claude Managed Agents catalog were borrowed (usage facts, a persisted interrupt intent, derived event ids,
redaction/permission provenance) as design inspiration only — no wire-protocol dependency.
**Exactly one new event kind (`control/interrupt`): the durable set grows 20 → 21.** Everything
else is an additive payload key on a frozen wrapper, a pure projection, or a config-first
capability; request bytes, tool schemas, `FROZEN_CHAT_BODY_SHA256` and `derive_model_context`
bytes are untouched (each test asserts this — constraint 3).

- **Provider usage + structured recovery on `turn/end`**: the openai-compat
  provider parses SSE `usage` objects verbatim (last frame wins; a usage-only frame never flips
  the empty-response liveness flag); `turn/end` carries `"usage"` verbatim plus
  `"error": {"class", "action"}` on soft-failure/error outcomes (the pure classifier +
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
- **Durable interrupt intent** ([ADR-014](design/decisions/ADR-014-durable-interrupt-intent.md)):
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
  `additions_only_regression_all_kind_bytes_are_frozen` (golden count now 21).
- **Derived event id + redaction provenance**:
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
- **Per-call tool permission provenance** ([ADR-015](design/decisions/ADR-015-tool-permission-provenance.md)):
  `agent.permissions` maps tool name → `allow`/`deny` (closed vocabulary, fail-closed parse —
  unknown value, key ∉ `tools`, or a table on a Router/Watchdog judge agent refuse boot).
  Evaluated once per call **before** the `tool/call` append (log-before-execute holds on both
  arms) and recorded as the additive `permission: {evaluated, policy, matched}` key — allows are
  audited exactly like denials; a deny never invokes and appends an honest-error `tool/result`
  the model reacts to. Absent table = `tool/call` payload byte-identical to legacy. Tests:
  `permission_is_a_closed_vocabulary_fail_closed`, `permissions_unknown_value_refuses_boot`,
  `permissions_key_absent_from_agent_tools_refuses_boot`,
  `permissions_on_judge_agents_refuse_boot`,
  `allow_path_invokes_tool_and_records_provenance_default_vs_configured`,
  `deny_path_does_not_invoke_and_emits_honest_error_result`,
  `absent_permissions_keep_tool_call_payload_byte_identical_to_today`,
  `permission_payload_key_never_enters_the_model_context`.
- **Closeout**: SPEC §5 record, DESIGN payload-contract alignment
  (§3.2/§3.3), README status table (21 kinds) + operator sections, this entry, docs-map
  link check green.

Verification notes (honest): all four gates green on the final tree (build, `cargo test
--workspace` 91 suites 0 failures, clippy `--all-targets -D warnings`, fmt `--check`), docs_map
green. Env-gated suites (`KIAN_PG_URL`, `KIAN_DSH_BIN`, live provider creds) skipped by design in
the default run — skip-never-fail held. Not exercised: a live-provider usage frame (fixture SSE
streams only), the postgres path of `usage-stats` (identical code path to `cache-stats`), a real
two-process cross-replica interrupt (the durable-intent path is exercised in-process over a
killed owner), and a full `kian serve` end-to-end deny (assembly is compile-checked and
config-tested). Deferred: the `ask`/confirmation permission plane and token pricing
(out-of-scope records in SPEC §5).

## Gated surfaces — current live-run status

| Surface | Gate | Status |
|---------|------|--------|
| Multi-replica / postgres | `KIAN_PG_URL` | Suite in place; **executed green** in CI on every push since 2026-11-02 (postgres service container; the job also asserts the gated suites ran, not skipped) |
| dsh bridge | `KIAN_DSH_BIN` | Suite in place; **executed green** 2026-11-02 via the contract-v0 peer (`contrib/dsh-v0-peer/`) against dsh 0.2.1-rc.5 + real `deepseek-chat` — full mapped chain, honest `Settled`. (The 2026-11-01 stock-dsh honest negative is superseded; runbook records both.) |
| Live provider smoke | `KIAN_API_KEY` | Suite in place; **not executed** — no provider key in the build environment |
| Live lark channel | `KIAN_LARK_APP_ID` / `KIAN_LARK_APP_SECRET` | Suite in place; **not executed** — no Feishu app credentials in the build environment |
| GitHub send / webhook | `KIAN_GITHUB_TOKEN` / `KIAN_GITHUB_WEBHOOK_SECRET` | Webhook + send covered by scripted fixtures, default-green; **executed live** 2026-11-04 (the dogfood — real REST polls and `deliver_structured` triage comments on throwaway issues of `flyer103/kian`, `DRY_RUN=1`); see `examples/issue-watch/RUNLOG.md` §7 |

These are deliberately honest gaps, not silent claims: the fixture-driven suites prove the semantics,
and the README run logs record each gated surface's real state — the dsh leg with its executed
outcome (an honest negative, kept as the true record), the rest as explicitly not yet executed.

## Unreleased

- **Agent memory Phase 1 — notes as events + the dreaming drain (round 8, kian-84..87)** —
  default-green. One additive event kind: `memory/note`, the 22nd durable kind (non-context,
  `DrainActivity`, emitted only by a dreamer drain; the durable set grows 21 → 22). `kian-core`:
  the fail-closed `memory` module (`parse_note`, `project_notes`, `verify_note` — unknown status →
  `candidate`, verified without valid `verified_by` → `candidate`, empty/inverted provenance
  dropped). New bundle crate `kian-memory`: the deterministic dream prompt (machine-parseable
  header carrying the source range), the fail-closed note parser (provenance always from the
  header, never the model; `verified` survives only with a `transcript` method and an in-range
  ref; over-`max_notes` refused, not truncated), and `DreamDriver` — wraps the built-in ReAct
  loop, appends one `memory/note` per valid draft after `Settled`, dedupes on
  `(subject, derived_from)` so a crash-resume writes no duplicates; a parse failure appends
  nothing and counts (honest zero). `kian-cli`: the `memory:` config section (absent = memory
  off, byte-identical; present parses fail-closed), the `memory_dream` ops method (admits ONE
  durable marked inbox row — the ops-plane dream envelope `{content, dream}` (the rendered prompt
  plus the trusted server-set `dream` marker) under idempotency key
  `dream:<source>:<from>:<to>`; out-of-bounds, over-cap and unconfigured refused honestly,
  −32602/−32005/−32006), and
  the `kian memory create/dream/list/show/verify` verbs. Ordinary sessions keep the exact same
  loop handle — request bytes are byte-identical with memory off (constraint 3 witness). No
  injection, no cadence, no representations (P2/P3 stay designed in SPEC §5). Tests:
  `parse_note_fail_closed_matrix`, `fold_latest_wins_with_interleaved_subjects`,
  `fold_is_order_independent`, `fold_drops_invalid_notes_and_ignores_other_kinds`,
  `verify_note_checks_ranges_against_the_source_stream` (kian-core);
  `settled_dream_appends_notes_from_the_answer`, `parse_failure_appends_nothing_and_counts`,
  `crash_resume_dedupes_on_subject_and_range`, `over_cap_is_refused_not_truncated`,
  `kian_memory_depends_only_on_core_orchestrator_and_loop` (kian-memory);
  `memory_dream_is_idempotent_refuses_over_cap_and_lands_notes`,
  `memory_dream_refuses_over_cap_and_when_off`,
  `ordinary_sessions_keep_the_plain_loop_and_request_bytes` (kian-cli). Closeout: SPEC §5 P1
  `designed` → `landed`, MEMORY-MODEL status + §3.1 canonical-payload pointer + §5 D1
  message-row refinement and memory-session policy, DESIGN §3.1/§7, README, this entry, docs-map
  green. Independent QA acceptance (kian-88): 7/7 behaviours pass, four gates green, no defects.
- **Memory round-8 repair (kian-91, from the kian-90 review)** — default-green. Four verified
  defects fixed, each mutation-verified against its regression test. B-1: `memory_dream` refuses
  any range outside `1 <= from_seq < to_seq <=` the source head (typed `SourceRangeOutOfBounds`,
  `-32602`) — no `verified` note can carry provenance past the end of the source stream. B-2:
  dreamer routing and provenance read the trusted, server-set `dream` marker on the admitted row
  (the exact two-key `{content, dream}` envelope, consumed to one marked `input/admitted`);
  a forged `[kian-dream: …]` header in a plain `submit` is ordinary content and routes nothing.
  M-1: note `text` passes the `SecretRedactor` before append; the value-free report rides the
  additive `redactions` key (`[{kind,name,count}]`, the `tool/result` shape). M-2: harvest folds
  within one answer by subject keeping the LAST revision, then stream-dedupes on
  `(subject, derived_from)`. Tests: `marker_round_trips_through_the_admit_envelope`,
  `marker_parser_is_fail_closed`, `finds_the_latest_dream_marker_and_ignores_plain_streams`,
  `forged_dream_header_in_user_content_does_not_route`, `within_answer_last_revision_wins`,
  `note_text_is_redacted_with_a_value_free_report` (kian-memory);
  `dream_envelope_consumes_to_one_admitted_with_marker` (kian-orchestrator, with the
  derive_model_context byte-identity witness); `memory_dream_refuses_to_seq_past_source_head`,
  `memory_dream_refuses_inverted_or_zero_range`,
  `forged_dream_header_in_user_content_does_not_route` (kian-cli e2e). Docs reconciliation
  (kian-92): MEMORY-MODEL §3.1/§3.3/§5/§9/§11 aligned to the landed bytes (seq-string `ref`,
  marker-based D1, byte-cap-only D3 with the head bound, no `scope_tags` in the payload, the
  landed CLI verbs and `/metrics` fragment), SPEC §5 P1 row and this entry; docs-map green.
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
- **Read model P0 — versions, unchanged/delta reads, resumable SSE, drain context cache
  (round 9, R9-02)** — default-green. The serving-form contract's first phase
  (`design/READ-MODEL.md` §3/§4.1; API-CONTRACT §3.1/§3.2/§5 promoted from planned §5.3):
  `projection` gains `version` (u64 = `head.next_seq - 1`) and the optional `since` —
  `since == version` answers `{"version":V,"unchanged":true}` with no state and costs exactly
  one seq-axis query (`EventStore::version`, a PK-index min/max lookup; `head`'s unindexed
  `MAX(epoch)` aggregate would make it O(history)); `since > version` is `-32602`, never a
  silent resync; the payload is always derived from one committed prefix (the scan's end bound
  pins the version). SSE `kian` frames carry `id: <seq>`; a reconnect with `Last-Event-ID`
  resumes after that seq without re-emitting the pre-resume prefix; ahead/non-numeric resume
  ids are one `event: error` + close; a terminal stream resumed at head emits `done`
  immediately; polling is tail-only (`kian_orchestrator::read::events_after`: one version
  query + one bounded scan — never `rebuild()` per poll). `DrainCtx` keeps one committed-event
  Vec per drain episode, tail-extended per iteration, with `compact` as the single
  full-re-derivation point (R5); the returned Vec is byte-identical to `rebuild` (request
  bytes untouched — cache-stability + golden-wire witnesses green, unchanged). SDK additive:
  `projection_since`, `ProjectionVersioned`, optional `version` on `ProjectionResult`,
  `SseFrame::KianWithId`, `events_from`; old fixtures still parse. Zero new event kinds;
  migration chains untouched. Executed evidence: `version_unchanged_and_full_read_shapes`,
  `ahead_and_empty_refusals`, `tail_delta_terminal_and_ahead` (kian-orchestrator read);
  `drain_ctx_events_cache_equals_rebuild_across_self_and_foreign_appends`,
  `compact_in_the_tail_forces_one_full_rederivation` (engine);
  `version_reports_the_last_committed_seq` (kian-store);
  `projection_version_unchanged_delta_and_ahead_e2e`,
  `sse_ids_terminal_replay_and_resume_refusals_e2e`,
  `sse_resume_across_reconnect_with_mid_session_append_e2e` (kian-cli e2e);
  `projection_since_round_trips_golden_request_bytes`,
  `sse_helper_carries_kian_ids_and_resumes_with_last_event_id`,
  `parse_frame_carries_kian_ids_additively` (kian-sdk); chaos + cache-stability suites green.
  Benchmark (`cargo bench -p kian-cli --bench read_path`, sqlite, criterion 0.5, M-series
  laptop): unchanged read **17.2 µs flat** (1k events: 17.2 µs; 10k: 17.2 µs) vs changed read
  870 µs → 8.7 ms (grows with history); SSE tail poll 36 µs idle / 40 µs with 5 new events —
  flat across history — vs legacy full-rebuild poll 843 µs → 9.2 ms (≈250× the tail poll at
  10k events). READ-MODEL §11 P0 `planned` → `landed`; DESIGN §3.2 pointer updated; docs-map
  green.
- **Read model P1 — verified snapshots (state + model context) and tail replay (round 9,
  R9-03)** — default-green (pg chain env-gated). The serving-form contract's second phase
  (`design/READ-MODEL.md` §5/§9/§11): migration **v3** appended to BOTH chains (v0–v2 frozen,
  pinned byte-for-byte by new golden tests) adds `projection_snapshots` —
  `(session, version, state, context, context_provenance, digest, verified, created_at)` — and
  `maintenance_leases`; the two backends' chains stay generation-for-generation identical
  (drift test). Snapshots are infrastructure state, never truth (R1): rows are disposable and
  the table can be dropped without touching the stream. Publication is **verify-then-insert**
  (R6): the builder — outside every drain, under a per-session maintenance lease (liveness
  only) — pins version `V`, reads `1..V` twice independently, derives
  `(state fold, context, provenance)` from both, and publishes one atomic INSERT of an
  already-verified row (`ON CONFLICT DO NOTHING`: insert-only, idempotent across replicas);
  a crash window leaves nothing or one complete verified row, never a partial snapshot;
  superseded rows GC to keep-N. The `state` column carries the full fold seed (state + the
  episode-end first-write-wins bit) so tail replay mirrors `project` exactly; `context` is
  the exact `context_to_bytes` serialization plus per-message provenance
  (`kian_core::context::context_provenance`, derived by the same fold as
  `derive_model_context` — aligned by construction; `derive_model_context` bytes unchanged).
  The loader picks the highest verified `V <= head`, re-checks the digest over the stored
  columns, and replays `(V, head]` (state via the public transition-table fold; context by
  appending the derived compact-free tail slice); a `compact` in the tail is the R5 single
  invalidation — one full re-derivation, exactly the P0 DrainCtx behavior; missing/corrupt/
  unverified rows, a rejected replay, or a tail beyond the configured cap → honest full
  rebuild. Projection and drain takeover state reads use the loader (`Engine::
  new_with_read_model`); the wire never changes. Config gate `read_model.snapshots`
  (`enabled` default **true** with a conservative `min_events` 2000, `tail_cap` 8192,
  `keep_n` 3, `interval_ms` 30 000; fail-closed validation; disabling hides every snapshot);
  `kian serve` spawns the maintenance ticker. Zero new event kinds; no history rewrite; no
  request-byte change (witness: `snapshot_path_request_bytes_are_byte_identical` — snapshot
  path bytes == `context_to_bytes` of the full derivation; cache-stability suite green,
  unchanged). Executed evidence: migrations `v3_generation_sql_is_frozen`,
  `pg_v3_generation_sql_is_frozen`; `publication_is_insert_only_and_idempotent`,
  `highest_verified_picks_the_greatest_version` (unverified rows invisible),
  `gc_keeps_the_newest_rows_only`, `candidates_list_sessions_past_the_minimum`,
  `maintenance_lease_liveness_semantics` (kian-store);
  `provenance_is_position_aligned_and_carries_source_seqs`,
  `provenance_of_a_compacted_context_sources_summaries_from_the_compact` (kian-core);
  `snapshot_plus_tail_equals_full_replay_on_random_streams` (randomized equivalence, with
  and without compacts), `compact_in_the_tail_falls_back_to_full_and_stays_correct`,
  `corrupt_unverified_and_tampered_snapshots_are_ignored`,
  `tail_beyond_the_cap_falls_back_to_full`,
  `build_crash_windows_leave_nothing_or_one_complete_row`,
  `mismatched_derivations_never_publish`,
  `episode_end_bit_survives_the_boundary_and_reproduces_duplicate_end_errors`,
  `state_at_agrees_with_project_across_prefixes`, `builder_lease_excludes_concurrent_builders`,
  `rebuilds_gc_older_versions` (kian-orchestrator);
  `pg_snapshots_publication_reads_and_maintenance_leases` (env-gated pg chain, live against
  PG 17); `read_model_defaults_are_enabled_and_conservative`,
  `read_model_section_parses_and_gates` (config);
  `projection_wire_identical_with_snapshots_e2e`, `disabled_read_model_hides_snapshots_e2e`
  (kian-cli e2e — wire byte-identical with/without a snapshot);
  `kill9_in_snapshot_crash_windows_never_serves_a_partial_snapshot` (chaos: `kill -9` against
  the real `kian serve` with the publish write blocked behind a held sqlite `BEGIN IMMEDIATE`
  lock, restart, every visible row load-equal to the full derivation; plus a kill during live
  traffic). Benchmark (`cargo bench -p kian-cli --bench read_path -- resume`, sqlite,
  criterion 0.5, M-series laptop): 10k-event resume **117 µs** via snapshot + 100-event tail
  vs **8.96 ms** full derivation (≈77×; flat 115→117 µs from 1k→10k history while the full
  derivation grows 826 µs→8.96 ms — resume is O(tail)); one maintenance build (two
  derivations + digest + insert) 2.0 ms @1k → 18.8 ms @10k, off the serving path.
  READ-MODEL §11 P1 `planned` → `landed` + header updated; docs-map green.
- **Read model P2a — materialized views (directory, usage, ticket stage, kind index) (round 9,
  R9-04)** — default-green (pg chain env-gated). The serving-form contract's third phase
  (`design/READ-MODEL.md` §6/§9/§11): migration **v4** appended to BOTH chains (v0–v3 frozen,
  pinned byte-for-byte by new golden tests) adds one physical `view_rows` table (logical views
  `session_directory` / `session_usage` / `ticket_stage` / `session_cache` keyed by name,
  canonical-JSON payloads) plus a per-view `view_state` row (store key `(events_total, max_seq)`
  + sha256 digest over the row set), and implements the declared `audit_kind_index` as a plain
  SQL index `idx_events_kind_ts` — chosen over a view table because `kind`/`ts` are already
  projection columns of the truth table: an index is append-time fresh, duplicates nothing and
  can never disagree with the stream (justification in the migration comment and READ-MODEL
  §6). `session_cache` is declared additively in §6 by this change (the plan requires
  `cache-stats` served from views; §10's additive protocol). Views are disposable
  observability accelerators, never truth (R1/R8): no control read consumes one — pinned by
  `no_control_read_consumes_a_view` (the orchestrator sources never reference the view
  surface). The freshness key is `(COUNT(events), MAX(seq))` — the count axis is load-bearing
  because `seq` is per-session and an append to a shorter session moves only the count
  (`store_key_changes_on_any_append`). A view is served only when the live key equals the
  derived-at key exactly (no TTL), the digest over the read rows matches (corruption check —
  tampered rows break it, `tampered_rows_break_the_digest`), and every payload decodes; stale,
  corrupt, undecodable, absent (a read-only reader of a pre-v4 store — `absent_tables_read_as_no_view`,
  `pre_v4_read_only_reader_falls_back`) or disabled → the honest full stream derivation, which
  stays the truth path (`stale_view_falls_back_to_full_derivation`,
  `corrupt_view_is_discarded_and_the_gate_hides_views`). Rebuild runs outside every drain under
  the `views:all` maintenance lease (liveness only; `rebuild_lease_excludes_concurrent_rebuilders`),
  pins the key BEFORE reading any stream, and publishes each view as ONE transaction (rows +
  state + digest together): a crash leaves the old complete version or the new one, never half
  a view (`publication_is_atomic_versioned_and_digest_covered`). Operator surfaces consult
  fresh views first with identical numbers by construction — the builder calls the SAME
  derivations the honest paths run (`session_usage_report`, `session_cache_report`, and the
  `kian tickets` board extraction `ticket_board`/`ticket_rows`); `kian tickets`, `usage-stats`,
  `cache-stats` and the `/metrics` aggregates fall back per-read. Declared staleness
  (READ-MODEL §6): event-derived fields are exact-version fresh; `ticket_stage`'s
  `pending_inbox` (and the stage's dependence on it) is as of the last rebuild — an inbox row
  can become eligible with no append. Config gate `read_model.views` (`enabled` default **true**,
  `interval_ms` 60 000, fail-closed validation; disabling hides every view);
  `kian serve` spawns the rebuild ticker. Equivalence executed:
  `rebuild_equals_full_derivation_on_a_multi_session_store` (directory facts, usage totals,
  cache counters incl. compacts, and the full ticket board — view rows decode into exactly
  `ticket_board`'s rows at the same `now`); `pg_views_publish_reads_and_freshness` (env-gated,
  live against PG 17.11); `from_str_name_round_trips_every_stage` (the stored stage names);
  `read_model_views_default_enabled`, `read_model_views_section_parses_and_gates` (config);
  `v4_generation_sql_is_frozen`, `pg_v4_generation_sql_is_frozen` (migrations). Benchmark
  (`cargo bench -p kian-cli --bench read_path -- views`, sqlite, criterion 0.5, M-series
  laptop, 1 000 sessions ≈ 5.2k events: 800 plain + 200 workers): ticket board from the fresh
  view **486 µs** vs **30.5 ms** full derivation (≈63×); usage aggregate **1.63 ms** vs
  **28.4 ms** (≈17×); one maintenance rebuild of all four views **66.5 ms**, off the serving
  path. READ-MODEL §11 P2a `planned` → `landed` + header updated; docs-map green.
