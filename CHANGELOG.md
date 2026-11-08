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
- **Read model P2b — opt-in export-only retention, archive verify/import (R9-05)** —
  default-green. Retention is strictly opt-in (`retention:` in `kian.yaml`, default **off** —
  absent/vague parses fail-closed: an enabled retention without `archive_dir` or without at
  least one online-window threshold (`min_age_ms`/`min_bytes`) refuses to start;
  `retention_defaults_to_off`, `retention_enabled_parses_fail_closed`). Migration v5
  `archived_sessions` on both chains (v0–v4 frozen, goldens `v5_generation_sql_is_frozen` /
  `pg_v5_generation_sql_is_frozen`): one infrastructure marker row per archived session —
  range, counts, package path, per-file + manifest sha256. The package
  (`<archive_dir>/<session>/`): `events.jsonl` (the exact `session export` bytes, shared
  serializer), `inputs.jsonl` (full inbox rows incl. `consumed_by_seq`/`admitted_seq`) and
  `manifest.json` (format_version 1, seq bounds, counts, per-file + aggregate sha256,
  created_at, backend, terminal state), written via temp dir + one atomic rename. The
  two-phase commit (`kian-store` `archive_commit`, the ONLY events/inputs delete site — the
  crate's append-only review assertion now carves out exactly `archive.rs` and a second
  assertion pins the carve-out to that one file): marker insert under CAS + online-row delete
  in ONE transaction, fenced on the online event count the verified package covers — a session
  that appended after verification aborts `OnlineMoved` and deletes nothing
  (`commit_refuses_an_online_stream_that_moved`); first marker wins
  (`commit_refuses_a_conflicting_marker`); retry with the same package is the idempotent
  no-op (`commit_deletes_online_rows_and_writes_the_marker_atomically`). Eligibility is
  terminal-only — the last committed event must be `session/ended`
  (`candidates_lists_terminal_sessions_only`,
  `candidates_ignores_a_mid_stream_end_event`). Reads answer the typed `archived` error:
  `-32007` (API-CONTRACT §2.2; the plan's planned `-32005` had been taken by the round-8
  memory registry, so `archived` is the next free code) on `projection` and the SSE stream
  (`archived_error_maps_to_the_archived_code`); `session inspect` reports archived + the
  package path; `session export` serves the package byte-identically (hashes re-verified on
  the way out); `session replay` refuses online replay pointing at import. `kian archive
  run|verify` and `kian session import <package>` (manifest verified, corrupt package or
  existing session id refused, events restored under their own epoch/seq, input rows
  restored verbatim). The "inbox audit-visible forever" wording in `migrations.rs`/`inbox.rs`
  is amended to "online store or verified archive package" (READ-MODEL §7). Executed evidence
  (`tests/archive_e2e.rs`): `archive_verify_delete_inspect_with_export_byte_parity` (the full
  L2 flow — pre/post-archive default, `--with-event-ids` and range exports byte-equal; import
  re-export parity; identical projection state; the consumed_by_seq audit column survives),
  `corrupt_package_is_refused_and_never_deletes` (tampered events/manifest refused by verify
  AND import; the write-without-commit crash window leaves online rows + no marker; the retry
  completes and a second run is the idempotent no-op),
  `eligibility_is_terminal_only_and_window_respected` (disabled retention refuses; the age
  window holds; a live session is never archived),
  `import_refuses_an_existing_session` (never merges, never overwrites); pg twin
  `pg_archive_marker_commit_and_inputs` (env-gated, live PG: marker, candidates, commit,
  fence, idempotent retry, input round-trip); store unit suite incl.
  `input_rows_round_trip_with_audit_columns`, `absent_marker_table_reads_as_not_archived`
  (pre-v5 read-only reader). READ-MODEL §11 P2b `planned` → `landed` + §7 landed shape +
  header; API-CONTRACT §2.2/§3.2/§3.3/§5.3 updated. No new event kind; no transparent cold
  tier; no automatic retention cadence.
- **Read model P3 — explicit-allowlist fan-in event stream (round 9, R9-06)** — default-green.
  `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]` (API-CONTRACT §5.4, normative; change
  class additive): an explicit allowlist of at most 64 sessions per connection (counted before
  dedup), **no wildcard** (`*` refused by name — the owner gate), HTTP 400 with a plain-text
  body on missing/repeated `sessions`, a non-UUID id, an empty entry, over-cap, or a `kinds`
  entry outside the EVENT-MODEL catalog (the validation table is pinned to `EventKind::as_str`
  by `known_event_kinds_table_matches_the_core_catalog`). Tail-only: the connection establishes
  each cursor with ONE head query (new `ControlPlane::session_version`) — pre-connect history is
  never replayed (`fan_in_interleaves_two_sessions_tail_only`); each poll cycle reads the same
  bounded `events_after` tails §4.1 uses, so per-poll work stays O(new events). Frames carry
  `id: <session>:<seq>`; no cross-session ordering is promised; per session the seq is strictly
  increasing. An unknown or archived listed session emits exactly one `event: error` frame
  naming it and the stream continues (`fan_in_unknown_session_errors_once_and_continues`); the
  `kinds` filter applies server-side and a terminal session is delivered through its
  `session/ended` event then dropped, the stream closing when every listed session has ended or
  errored — no `done` frame (`fan_in_kind_filter_and_terminal_close`). Backpressure is bounded
  twice over: ≤ 512 frames per yielded cycle (the cursor advances only to the last emitted seq,
  so nothing is dropped or re-emitted), and a stalled consumer suspends the poll loop outright —
  the server never buffers unbounded data on its behalf. Config `api.event_stream.enabled`
  (default **true**, fail-closed parse; disabled mounts no route — 404:
  `fan_in_disabled_by_config_forty_fours`). `/metrics` gains the `event_stream` object
  (`subscribers`, `sessions_watched`, `events_sent`; a connection-lifetime guard settles the
  gauges on disconnect: `fan_in_metrics_track_the_open_connection`,
  `fan_in_metrics_settle_to_zero_after_drop`). The per-session resume surface is untouched
  (`per_session_stream_resume_unaffected_by_fan_in`; the full resume suite stays in
  `read_path_e2e.rs`). `kian-sdk`: the SSE parser splits compound ids additively
  (`SseFrame::KianFanIn`; old id-less and numeric-id fixtures still parse) and
  `KianClient::fan_in` sends the exact wire request (byte-level request-line pinning in
  `golden_wire.rs`, incl. the 400 mapping). No authz change: the deployment remains the trust
  boundary and the allowlist is explicit ids only. READ-MODEL §4.2 landed shape + §11 P3
  `planned` → `landed` + header; API-CONTRACT §1/§5.3/§5.4/§6/§10.2. No new event kind; the
  planned `read` metrics object remains planned (P0-owned surface).
- **Read model review R9-08 fixes — package identity, archived aggregate reads, snapshot trust
  (R9-08a)** — default-green. Fixes the three findings of the round-9 independent review
  (REQUEST-CHANGES; fix card opened from it). **F1 package identity:**
  `session import`/`archive verify` now refuse a package whose rows name a different session
  than its manifest — `verify_package` cross-checks every `events.jsonl` event's and every
  `inputs.jsonl` row's `session` against `manifest.session`, refusing with the line number and
  both session ids (the manifest is not self-hashed, so hand-editing `session` alone passed
  every hash check; a hash-consistent identity-inconsistent package previously imported,
  restoring events under one id while reporting another). `verify_package` now returns the
  parsed rows (`VerifiedPackage`) and `run_import` appends exactly those rows — the files are
  never re-read after verification (the verify/use gap is closed). **F3 archived aggregate
  reads:** `usage-stats --session` and `cache-stats --session` check the archived marker first
  and refuse with the package path and import guidance — never a silent zero report (R7's
  "any read whose range touches archived data MUST report `archived` explicitly";
  READ-MODEL §7 read behavior updated). Whole-store aggregates are unchanged (online window
  only). **F2 snapshot trust documented:** READ-MODEL §5 gains the explicit trust boundary —
  load-time verification is builder attestation + digest↔columns integrity; a
  poisoned-but-self-consistent row (columns rewritten together with a recomputed
  `kian-snapshot-v1` digest, `verified=1`) is not detectable at load and is served; the table
  sits inside the same trust boundary as `events`, must never be copied between stores, and
  drop/rebuild from the stream is always safe. This corrects the P1 entry's wording above:
  `corrupt_unverified_and_tampered_snapshots_are_ignored` covers digest-stale tampering (and
  unverified rows) only — it is not a general tamper guarantee, and the new
  `self_consistent_poisoned_snapshot_is_served_builder_attestation_only` pins the documented
  (served) behaviour so the claim and the test agree. No Merkle/proof mechanism (a deliberate
  non-goal). Executed evidence: `import_refuses_an_identity_inconsistent_package` (a
  hash-consistent forged package refused by `archive verify` on the manifest's own id AND by
  `session import`; the target store stays empty; the honest package still verifies),
  `usage_stats_refuses_an_archived_session`, `cache_stats_refuses_an_archived_session`
  (kian-cli e2e; both also pin that the whole-store report is unaffected — red before the fix:
  the same commands returned all-zero reports / accepted the forged package),
  `self_consistent_poisoned_snapshot_is_served_builder_attestation_only` (kian-orchestrator).
  **F5 note (SDK compat):** `SseFrame` (`kian-sdk/src/sse.rs`) is not `#[non_exhaustive]`;
  marking it is itself a source-compat change for downstream exhaustive matches, so the
  decision is deliberately deferred to the SDK freeze — downstream matches should carry a `_`
  arm meanwhile. Docs drift (F4) and default-on maintenance cost (F6) are owned by follow-up
  cards, not this change. No new event kind; no wire change; no migration.

## 2026-11-08 — read model R9-10: incremental view rebuild — default-green

- **Problem (R9-08 F6, follow-up card t_988f5de3).** The default-on view maintenance ticker
  (`read_model.views`, enabled, `interval_ms` 60 000) did recurring O(total history) work on the
  serving runtime: every tick re-read and re-folded every session's whole stream (CHANGELOG P2a
  bench: 66.5 ms at 1 000 sessions / 5.2k events, growing linearly).
- **Remedy chosen (1 + 3 of the card's candidates; the default stays ON — an idle tick is now
  near-free, so the 60 s cadence costs ~0.01–0.1 % of one core even at 10k sessions).** The
  rebuild is **incremental**: one index-only `GROUP BY session` heads scan diffs live heads
  against the stored rows' `row_version`s; only moved/new sessions are re-read and re-folded,
  with a `yield_now()` between sessions (the folds never monopolize the runtime worker).
  Publication becomes a **delta transaction** per view (upsert changed rows, delete gone
  sessions, flip `view_state` to the merged set's digest at the pinned key) — same one-tx
  version-flip atomicity, writes proportional to the change. Soundness: streams are append-only
  under CAS guards, so equal head ⇒ identical stream ⇒ the reused row is byte-identical to a
  re-derivation. `ticket_stage` is all-or-nothing (its global folds read board-kind events
  store-wide and `pending_inbox` moves with `eligible_at` alone): a board-kind delta, a change
  to a session already on the board, a deletion, or a pending drift re-derives the whole board
  from a **kind-filtered events read** (`thread/started`, `thread/completed`, `session/ended`,
  `input/admitted`, `tool/call`, `tool/result`, `channel/delivered`) plus the worker streams —
  never from every session's full stream; otherwise only the state flips to the new key. Any
  doubt about the stored views (absent state, digest mismatch, undecodable ticket payload)
  falls back to the full rebuild (`rebuild_views_full`, kept as first-build path, self-heal and
  bench baseline) — a tampered view still heals on the next tick. Read semantics unchanged
  (R1/R8): the merged row set is byte-identical to a full rebuild's at the same pinned store
  key; the freshness gate, digest check and honest fallbacks are untouched. The snapshot
  builder's 30 s candidate scan stays an index-only `GROUP BY` bounded by its `LIMIT`
  (documented in READ-MODEL §9).
- **Store surface (both backends, dialect SQL in `kian-store` `sqlite.rs`/`postgres.rs`):**
  `session_heads`, `pending_input_counts`, `events_where_kinds`, `view_publish_delta`. The board
  fold core moved from `ticket_rows` to `ticket_rows_from(all_events, worker_streams)` with the
  kind-filtered input equivalent by construction (`board_kinds_stay_in_sync` pins the seven
  kinds to the enum). `RebuildOutcome` gains `derived` and `board_refreshed` counters.
- **Equivalence executed** (`views_e2e`): `incremental_rebuild_equals_full_after_partial_changes`
  (2 grown + 1 new + 1 deleted session ⇒ `derived == 3`, all four views equal the honest full
  derivations, deleted rows gone), `incremental_tick_after_no_changes_is_a_noop` (`derived == 0`,
  `views == 0`, views stay fresh), `plain_append_does_not_refresh_the_board` (delta-scoped board
  trigger; ticket view exact via the key flip), `tampered_view_self_heals_on_the_next_incremental_tick`,
  `pending_inbox_drift_without_appends_refreshes_tickets` (the §6 declared bound holds without
  appends), plus the pre-existing P2a suite (equivalence, stale/corrupt fallback, lease,
  pre-v4 reader) unchanged and green. Config unchanged (`read_model.views` default
  enabled / 60 000 ms; no migration, no wire change, no new event kind).
- **Benchmark** (`cargo bench -p kian-cli --bench read_path -- views`, sqlite, criterion 0.5,
  M-series laptop; 1k = 800 plain + 200 workers ≈ 5.2k events, 10k = 8 000 + 2 000 ≈ 52k
  events): full rebuild **64.8 ms @1k / 696.8 ms @10k**; incremental idle tick
  **6.0 ms / 60.1 ms** (heads + pending scans + four digest verifications); incremental tick
  after 10 plain sessions grew **26.8 ms / 78.7 ms** — marginal cost per changed session
  **≈2.1 ms @1k and ≈1.9 ms @10k** (flat across a 10× store: proportional to the change, the
  ~20 ms constant is the four publication commits); board-refreshing tick (one worker grew)
  **24.2 ms / 226.2 ms** vs the honest full board derivation 30.1 ms / 340.9 ms. Verification
  notes (honest): all four gates green on the final tree (build, `cargo test --workspace`
  0 failures, clippy `--all-targets -D warnings`, fmt `--check`); docs_map green. Env-gated
  suites (`KIAN_PG_URL` etc.) skipped by design; the new store SQL is dispatched per backend
  but only the sqlite paths ran. READ-MODEL §6 (incremental rebuild bullet) and §9 (per-tick
  cost rule) updated in the same change.

## 2026-11-08 — round-9 closeout: read model docs landed — documentation only

- **Scope**: documentation reconciliation for the read-model round (P0–P3 + the R9-08 fixes +
  R9-10 incremental rebuild). No code, no wire, no migration, no event kind.
- **README**: Features gains the read-model line (versioned unchanged/delta reads, resumable
  SSE, verified snapshots + tail replay, operator views, opt-in export-only archive,
  allowlist fan-in — all disposable projections; the stream stays the only truth); Docs map row
  re-worded from "proposed" to "the normative serving-form contract"; new Advanced usage section
  (unchanged `projection` poll, opt-in `retention:` config, `kian archive run|verify`).
- **SPEC §5**: "Read model and serving form" flips to **(P0–P3 landed)** with the round-9
  commit range; all five phase rows `designed` → `landed`.
- **READ-MODEL**: title `(proposed)` → `(normative)`; header rewritten to "accepted and fully
  landed" (ADR-017, owner-gated 2026-11-07) with the phase summary; R1 gains the §5
  load-time-trust-boundary pointer (re-review residual); §7 read-behavior "any read" narrowed
  to the enumerated surfaces with an honest scope note (memory surfaces answer `unknown
  session` — they do not consult the marker); the §4.2 512-frame batch now points at
  API-CONTRACT §5.4 as its normative home (F4 duplicate).
- **DESIGN §3.2**: the serving-form pointer tag "P0 landed" → "landed" (F4).
- **Gates on the final tree**: build / `cargo test --workspace` / clippy `--all-targets
  -D warnings` / fmt `--check` green, `docs_map` 6/6. Independent review verdict on the round
  (R9-08 + re-review of fix `1f4d669`): PASS; QA acceptance 12/12 groups.
- **Residuals (owner-pending, not blocking)**: `AGENTS.md` docs-map bullet for
  `design/READ-MODEL.md` (plan §1.6) and the `21-kind` → `22-kind` catalog count (F4) were
  **not** applied — AGENTS.md is a protected file and the write required owner approval that
  did not arrive in this run. `SseFrame #[non_exhaustive]` stays deferred to the SDK freeze
  (F5, recorded with its reason in the R9-08a entry).

## 2026-11-08 — round-10 WeChat iLink channel: adapter, config/CLI, login — default-green (L1/L2)

- **`design/CHANNEL-WECHAT.md` (normative)** — the `type: wechat` channel contract: the frozen
  iLink protocol mapping (endpoints, headers, `WeixinMessage`, the `sendmessage` payload), the
  `ChannelEvent`/`ChannelDelivery` mapping, the `context_token` rule, the `0600` credential-file
  schema and lifecycle, the config/CLI contract (`kian wechat login`), the bidirectional receive
  allowlist (§3 drop + §9 send gate), dedupe (`message_id` ring + `(session, event_id)`
  idempotency), the empty-cursor bound (no position table), the failure/degradation table,
  phase-2 non-goals, L1/L2/L3 acceptance and the full UNVERIFIED list.
- **`design/decisions/ADR-018-wechat-ilink-channel.md`** — a native adapter in the bundle rather
  than the OpenClaw bridge, 公众号, 微信客服 or personal-account hooks; the QR-issued token as a
  deliberate deviation from the env-only static-secret rule; best-effort consequences (operator
  re-login on `-14`, reply-only so no proactive push, phase-2 media).
- **`kian-superagent` adapter (commit `c066e7d`)** — `WechatChannel` over the `ChannelAdapter`
  seam: one long-poll loop per channel, `message_id` → `event_id` (lossless decimal string),
  ring + idempotency dedupe, topics allowlist, `context_token` send cache populated only for
  admitted senders and a send gate refusing non-allowlisted targets before any wire traffic,
  ordered >2000-char split, `-14` → 1 h pause + credential-mtime resume; `wechat/login.rs` QR
  flow (a status long-poll timeout is `wait`, never fatal); the judge prompt gains the
  channel-target sentence (new-session `policy_version` change only — old sessions replay
  byte-identically). 22 adapter tests against a scripted axum mock iLink server.
- **`kian-cli` config/assembly/login (commit `8153938`)** — `WechatChannelSection` fail-closed
  validation (credentials required, non-empty topics, single wechat channel, forced untrusted),
  `kian wechat login` (QR print, atomic `0600` write, ids never the token), assembly mounts the
  channel (registry, consumers, poll-driver order), token redaction. 9 tests including the
  real-`Server` + sqlite end-to-end: one inbound admitted exactly once across replay **and**
  process restart; channel failure leaves session state untouched.
- **R10-06a review fixes (folded into both commits)** — the four findings of the independent
  review: the send-path allowlist hole (blocker: token cache moved admit-only + the §9 send
  gate), the vacuous restart leg (baseline-before-respawn), login abort on a normal long-poll
  timeout, and missing-`ret` minting a delivered receipt; plus minors (api_base doc/code match,
  verify-code wording, dead stub removed, `0700` credential parent, `-14` observability note,
  baseurl-vs-api_base UNVERIFIED item). Re-review verdict: **PASS**, each fix mutation-tested
  red→green by the reviewer.
- **No new surface, no wire, no event kind** — `API-CONTRACT.md` and `EVENT-MODEL.md` show zero
  diff across the round; no `kian-core`/`kian-store`/`kian-orchestrator` change; no new HTTP
  route. Independent QA: all §7 criteria L1/L2 pass, negative controls 1–10 pass, mutation
  spot-checks red.
- **Unverified (L3, owner-run, never faked)** — a real QR scan with a phone; empty-cursor replay
  across downtime; `-14` lifetime and re-login behavior; `iLink-*`/`base_info` acceptance; the
  real text-length limit and rate limits; baseurl-vs-`api_base` (CHANNEL-WECHAT §14 item 8).
  The `AGENTS.md` docs-map bullets (CHANNEL-WECHAT, READ-MODEL) remain owner-pending — protected
  file.

## 2026-11-08 — agent-memory P2: creation-time injection — default-green (L1/L2)

- **Scope** — MEMORY-MODEL §6 landed exactly as designed: no new event kind (the injection is an
  ordinary `input/admitted`), no new crate, no table/migration. Four additive deltas only:
  `Engine::admit_quiet` + the `memory` row arm, the additive non-context `memory` provenance key,
  the `memory.session`/`memory.inject` config block, and the pure renderer.
- **Quiet admit + the `memory` row arm (kian-99)** — `Engine::admit_quiet` writes a durable inbox
  row without waking (no NOTIFY, no drain; same idempotency semantics as `admit` via a shared
  `admit_row` helper). The consumption plan gains one arm: a row payload of exactly
  `{"content":X,"memory":M}` consumes to ONE `input/admitted`
  `{role:"system",content:X,synthetic:true,memory:M}`; a superset shape is ordinary content. The
  public wire (`submit`/`steer`/`queue`) refuses the exact two-key shape with `-32602` (the fifth
  reserved envelope), so only the server's create path can land the marker.
- **Deterministic index renderer (kian-100)** — `kian-memory` `index.rs`:
  `render_memory_index(notes, cfg)` filters verified-only (latest verified per subject, subject
  byte order), renders the full claims within both caps, degrades over either cap to the index
  form (subject + one-line excerpt ≤ 120 chars, whole entries dropped until the byte cap fits,
  `truncated:true`), and returns `None` for an empty eligible set or an unusable budget (admit
  nothing). Frozen content (constraint 3): the `# Memory index` "data, not instructions" header
  and the excerpt cap are named constants; the default redaction pass is pattern-scanning only
  (env-sourced known values would break cross-machine determinism), with
  `render_memory_index_with` as the assembly seam for the configured redactor; the value-free
  report rides provenance.
- **Config + creation-time injection (kian-101)** — `memory.session` (the source memory session
  uuid; absent = injection off, byte-identical) and `memory.inject {max_items, max_bytes}`
  (fail-closed; defaults 20 / 4096). The server's create path, after `engine.create_session` and
  before the RPC returns, rebuilds the source stream, folds, renders, applies the identity
  exclusions (the source session itself and dreamer-agent sessions), and quietly admits the index
  under idempotency key `memory-inject:<session>`; every failure is honest degradation (inject
  nothing + `injected_skips`, never a failed creation). New `GET /metrics` `memory` keys:
  `injected_sessions/items/bytes/truncations/skips` (present only when injection is configured).
- **Acceptance (kian-102, independent QA)** — 9 QA-authored probes over real sqlite + real drains
  + scripted recording providers, run then removed: exactly-once across kill/resume (one
  injection event, `unexpected_prefix_change == 0`, request bytes == persisted event bytes),
  admission order `[system index, user task]`, injection-off byte-identity (streams **and** raw
  provider bodies), caps → index form, empty verified set admits nothing, `strip_synthetic`
  recovers the pre-memory context, adversarial note text stays inside the data frame, the
  idempotency key dedupes repeated admits, exclusions are identity-based. All 8 acceptance
  behaviours PASS. **Finding F1 (accepted, documented)**: a session that crosses a restart
  between `create` and its first submit has its quiet row woken by the NoLostWork startup
  rebuild — the index drains alone as the first provider turn. Exactly-once and prefix stability
  hold; recorded as known behavior in MEMORY-MODEL §6 I2 (fix needs an inbox-row schema change,
  out of scope).
- **Closeout docs (kian-103)** — MEMORY-MODEL §6 reconciled with the landed behavior (status
  P2 landed; I1–I9 updated; I10 exclusions added; the F1 restart window recorded; §9 metrics),
  SPEC §5 P2 row → landed, DESIGN §3.2 creation-time path + constraint-3 argument, DESIGN §7
  crate map, README feature bullet + advanced-usage example + docs/crate map rows, this entry.
  EVENT-MODEL §3/§4.2/§4.5 were reconciled with the payload changes in the kian-99/101 changes.
- **Unverified (named)** — no process-level `kill -9` variant (in-process drop + rebuild over
  the same sqlite exercised the same `startup_rebuild` path); residual prompt-injection risk vs a
  real model (no provider key — the frame is asserted structurally, A8 known limitation);
  concurrent create/submit not stress-tested (guarded by construction: fresh uuid + per-session
  key); the dreamer-agent exclusion is covered at the pure level only (structurally unreachable
  at server create).
- **Gates on the final tree** — build / `cargo test --workspace` (1263 passed / 0 failed /
  5 ignored, 104 suites) / clippy `--workspace --all-targets -D warnings` / fmt `--all --check`
  green; `docs_map` green. New tests: kian-99 (`admit_quiet_does_not_wake`,
  `submit_after_admit_quiet_drains_both_rows_in_admission_order`,
  `admit_quiet_dedupes_on_a_repeated_key`,
  `memory_envelope_consumes_to_one_admitted_system_message` + wire-refusal/mutation guards in
  `kian-cli`), kian-100 (`kian-memory` renderer suite), kian-101
  (`crates/kian-cli/tests/memory_inject.rs` end-to-end suite).

## Round-10 WeChat L3 live-run fixes (2026-11-08)

The first real phone/QR run of the WeChat iLink channel surfaced five defects that the mocked
suites could not; all are fixed, tested, and re-verified live (messages delivered in both
directions).

- **Official iLink client identity is required** (`kian-superagent`, `wechat/protocol.rs`) —
  `iLink-App-Id: bot`, `iLink-App-ClientVersion: 0x0002040A` and `base_info.channel_version:
  2.4.10`. The kian-branded values made WeChat's ClawBot login page fail with `Network error`
  before the scan reached the server; the QR scan then confirmed and wrote the 0600 credential.
- **Sender provenance reaches the model** (`wechat.rs`) — the event `raw` now carries
  `from_user_id`; the Router prompt derives the reply target from `raw.from_user_id` instead of
  guessing (it previously used the message id).
- **Target spellings tolerated** (`wechat.rs`) — the model may pass `target` as a JSON object or
  as a JSON-encoded string; both are parsed before the allowlist checks.
- **Ret-less `sendmessage` success accepted** (`protocol.rs`) — the live success body omits `ret`;
  an absent `ret` now reads as success (official-client parity), a non-object body is still
  `Malformed`, and `ret != 0`/`errcode != 0` stay honest failures. The strict parse had caused a
  retry storm whose sends the server had in fact accepted.
- **Judge tags and channel formatting** (`templates.rs`, `policy.rs`) — the decision tag is
  confined to the Router's assistant narration (it must not appear in `reply` text); channel text
  is plain text (no markdown/emoji), summarized rather than relayed from a worker report; the IM
  budget now counts **CJK characters as units** (a whitespace-only count let arbitrarily long
  Chinese replies through the ≤100 rule), cuts over-budget text at the last complete sentence that
  keeps at least half the budget, and the prompt asks for a complete summary plus
  `回复「详情」看完整结果` instead of relying on truncation.
- **L3 evidence recorded** — `design/CHANNEL-WECHAT.md` §13: QR login, admission/delivery event
  ids, empty-cursor downtime replay (single observation), ret-less wire shape, target spelling,
  tag leak, CJK budget; §14 item 4 resolved, item 3 updated.
- **Unverified remains** — `-14` token lifetime / automatic re-login; real text-length and rate
  limits; `baseurl` vs `api_base`; media/typing/group/multi-account/proactive push (phase 2).

## 2026-11-08 — agent-memory P3a: read-only retrieval + representations (`kian memory search|map`) — default-green

- **Scope** — MEMORY-MODEL §7's first landed slice: two read-only views over already-persisted
  notes. **No new event kind (the durable set stays 22), no new crate, no table/migration, no wire
  change, no event or model-context byte change.** `kian memory list|show|search|map` never append:
  the CLI verbs open the store read-only; a missing store is an error, never a created file. This
  slice needs **no dreaming run** because nothing is persisted — a persisted representation
  (wiki-as-notes) stays designed (P3b) and would follow MEMORY-MODEL §7 R3.
- **`kian-memory` pure projections (kian-107)** — `search.rs`: latest-per-subject fold over ALL
  statuses (every hit carries `verified|candidate|stale` — untrusted notes surface as claims),
  ranking subject exact > prefix > contains > text contains, deterministic subject-byte tie-break,
  ASCII case-insensitive, caps 20 hits / 8 KiB with `truncated`. `map.rs`: verified-only nodes —
  **effective rule through the read path: a subject is a node iff its latest revision is
  `verified`** (`project_notes` pre-folds latest-per-subject before eligibility, so the
  `eligible_notes` "last verified revision" fallback is unreachable from any production caller;
  consistent with the P2 injection index and now stated in MEMORY-MODEL §7.1) — with
  evidence-derived edges (note pairs whose `derived_from` ranges intersect in the same source
  session, never model-generated), caps 200 nodes / 1000 edges / 64 KiB. The byte cap is
  **nodes-first** (kian-112, fixing the review's B1): the byte-order node prefix is kept and edges
  are cut first — only edges between kept nodes are delivered, so no edge ever dangles — and the
  trim is O(nodes+edges) incremental section math, not a re-render fixpoint (B2: 14.7 s → 2.7 ms
  at 200 notes / 57-char subjects). Honest heuristics `degenerate` (fewer than two nodes or zero
  edges in the **delivered** view — search
  alone is the right tool) and `oversized` (rendered bytes exceed the summed note-text bytes)
  reported in text and JSON, never hidden. Both views render byte-identically across runs and
  insertion interleavings and lead with the frozen `(data, not instructions)` frame.
- **`kian memory search|map` verbs (kian-108)** — read-only in-process CLI verbs reusing the exact
  `memory list`/`show` read path; text and `--json` agree on every flag. **Recorded divergence
  from the P3a plan §2.1:** no `[--limit N]` flag — the caps are fixed (20 hits / 8 KiB; the map's
  200/1000/64 KiB), accepted rather than silently diverged.
- **Acceptance (kian-109, independent QA)** — 8/8 behaviours PASS over real sqlite (one fixture
  lands its notes through a real memory-dream drain against the scripted mock provider):
  `qa_views_never_write_and_missing_db_fails_closed` (event count + summed bytes identical
  before/after all four verbs; no file created), `qa_views_are_byte_identical_across_runs_and_interleavings`,
  `qa_over_limit_search_and_map_truncate_text_json_agree`,
  `qa_map_is_verified_only_and_search_surfaces_every_status` (incl. a subject superseded by a
  candidate revision — dropped from the map, `[stale]` in search),
  `qa_map_reports_degenerate_and_oversized_honestly`,
  `qa_both_views_lead_with_the_data_not_instructions_frame`,
  `qa_real_drain_notes_and_a_deleting_event_change_the_view` (views derive only from the event
  stream; no side store/cache file). Plus the kian-107/108 suites
  (`fold_keeps_latest_revision_and_shows_status`, `map_is_verified_only`,
  `degenerate_flag_is_reported`, `oversized_flag_compares_rendered_bytes_to_claim_bytes`,
  `search_and_map_leave_event_count_and_bytes_unchanged`, `search_empty_query_is_refused`, …).
- **Cost and limits (recorded)** — one invocation is a full stream read + fold + pure view,
  O(memory-session events); the caps bound rendered bytes, not the read. The bound is recorded,
  **not perf-benched** (A6); a snapshot-served fold (the READ-MODEL acceleration) is deferred with
  P3b. The `oversized` flag is a one-pass re-render fixpoint — convergence beyond that pass is
  pinned by the landed tests, not proven by an argument (kian-107 unverified item).
- **Closeout (kian-110)** — MEMORY-MODEL §7 (+ §7.1 landed block, header, §9 verbs), SPEC §5 P3
  split (P3a landed / P3b designed, moved not dropped), README feature bullet + advanced-usage
  example + docs-map row, this entry.
- **Fix (kian-112)** — the kian-111 independent review was BLOCKING on the map's byte cap: the
  trim loop put edges outer / nodes inner, so once the 64 KiB budget bound it delivered ~944 edges
  and 0–2 nodes — every edge dangling, no `## Nodes` section — contradicting the struct invariant,
  the docs and README. Fixed by restructuring the trim nodes-first (largest fitting byte-order
  node prefix, then only edges between kept nodes while each next edge line fits — closed by
  construction) and replacing the O(nodes×edges) re-render fixpoint with O(nodes+edges) incremental
  section lengths. Tests: strengthened `byte_cap_drops_edges_first_and_stays_within_budget` (all
  nodes survive, edges cut, closed view, headers agree), added
  `byte_cap_keeps_node_prefix_when_nodes_alone_overflow`, `large_byte_capped_map_stays_closed_and_within_budget`
  (the 200-note/57-char review worst case) and the CLI-level
  `map_byte_cap_keeps_nodes_and_cuts_edges_over_real_store` (60 verified notes / ordinary subjects
  over a real sqlite store, shipped binary included). Measured: 60 notes 640 ms → 1.2 ms,
  200/31-char 2.21 s → 2.7 ms, 200/57-char 14.68 s → 2.7 ms; dangling 944 → 0; bytes 65 406–65 421
  ≤ 65 536. MEMORY-MODEL §7.1, README (feature wording + crate-map line) reconciled in the same
  change.
- **Gates** — build / `cargo test --workspace` (106 suites, 0 failed) / clippy `--workspace
  --all-targets -D warnings` / fmt `--all --check` green; `docs_map` green. This closeout entry is
  documentation-only on top of the already-gated tree.

## 2026-11-08 — agent memory live verification (P1/P2/P3a)

**env-gated (live provider), not default-green.** With a real `deepseek-chat` endpoint (key via
`providers.*.api_key_env`; no value captured) and a loopback `kian serve`:

- Dream: an 8-fact runbook transcript dreamed over `[1,10)` produced 8 `runbook/*` notes, all
  `verified` with transcript refs; a trivial transcript produced `[]` (honest empty,
  `parse_failures=0`).
- Injection: 8/8 new sessions received the index as their first `input/admitted`
  (`role:"system"`, `synthetic:true`, `memory` marker; 64 items / 7 424 bytes total); the model's
  first request carried it as `messages[0]`.
- A/B (one model, 8 questions, keyword rubric): 0/8 memory-off vs 8/8 memory-on; prompt tokens
  206 vs 2 110; completion 138 vs 97; turns 8 vs 8. Directional (synthetic corpus, single model,
  small N); the harness lives in a scratch directory and is not committed.
- Recorded in `design/MEMORY-MODEL.md` §10.1 and `docs/SPEC.md` §5, deliberately **not** in the
  README (README policy: no live-run logs or benchmark methodology). No code changed.
