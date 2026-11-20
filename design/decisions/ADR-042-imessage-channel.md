# ADR-042: An iMessage channel through a macOS bridge, not a server-side adapter

- **Status**: **Accepted** (owner gate 2026-11-19) — implemented as slice 1a; first live L3 run
  2026-11-20 (the contract's §12)
- **Date**: 2026-11-19
- **Scope**: a new crate (`kian-imessage-bridge`), the bundle (`kian-superagent`), CLI/assembly
  (`kian-cli`), docs — no core, no store, no orchestrator, no wire surface, no event kind
- **Supersedes**: none
- **Related**: AGENTS.md constraints 1/3/7/10; [DESIGN](../DESIGN.md) §4.2/§6.2/§7;
  [CHANNEL-IMESSAGE](../CHANNEL-IMESSAGE.md) (the contract this decision establishes);
  [ADR-018](ADR-018-wechat-ilink-channel.md) (the channel pattern this reuses, and where it stops
  applying); [ADR-011](ADR-011-inbound-webhook-plane.md) (why the GitHub receive half *is*
  a server surface and this one cannot be)

## Context

The owner already reaches kian from their phone through the WeChat channel
([CHANNEL-WECHAT](../CHANNEL-WECHAT.md), ADR-018). The request is the same product outcome on a
different app: **send an iMessage from the iPhone, get a kian reply in the same conversation.**

Unlike WeChat, iMessage has no server-side integration point at all, and that single fact decides
the architecture:

1. **No API, publicly or privately.** Apple publishes no consumer iMessage API. The official
   business surface (Apple Messages for Business) requires an approved Message Service Provider and
   a business identity — not a self-serve channel for a personal assistant. The
   reverse-engineered route (pypush, the engine behind Beeper Mini) has a documented history of
   Apple **blocking the service and banning the hardware identities** of people who used it
   (<https://messagingagents.com/guides/apple-terms-and-imessage-apis>); it is a non-starter for a
   tool the owner runs on their personal Apple ID.
2. **The message physically lands on a Mac.** iMessage delivery is per-device via APNs; a Mac
   signed into the receiving Apple ID stores messages in `~/Library/Messages/chat.db`. That file,
   read under Full Disk Access, is the only reliable receive surface. Sending is Messages.app's
   AppleScript surface under Automation permission. So the channel's endpoint is **a Mac**, and its
   uptime is that Mac's uptime.
3. **The macOS privacy gates are per process context, and SSH is the wrong context.** `imsg`'s own
   permissions guide and the OpenClaw iMessage deployment notes both record the trap: a send
   launched from an SSH session can attach the Automation grant to `/usr/libexec/sshd-keygen-wrapper`
   and then fail with `-1743` (no usable Messages toggle), while reads and probes look healthy
   (<https://github.com/openclaw/imsg/blob/main/docs/permissions.md#permissions>,
   <https://docs.openclaw.ai/channels/imessage/setup>). The documented fix is to run the reader and
   sender **in the logged-in Messages user's GUI session** — i.e. a LaunchAgent, not an SSH command.
4. **Kian's deployment puts `kian serve` off the Mac.** In the owner's topology the server runs on a
   Linux VPS (`/srv/kian/kian.db`, `listen 127.0.0.1:7433`) and the Mac is a worker endpoint
   (`kiand --register-to`) reached through an SSH tunnel. Running `kian serve` on the Mac to get an
   in-process adapter would move the product's control plane and lose the worker topology.

Kian's `ChannelAdapter` seam already expresses everything a channel needs (`subscribe`/`send` over
`ChannelEvent`/`ChannelDelivery`/`SendReceipt`/`ChannelError`), and the WeChat adapter proved the
bundle can host a poll-based external-protocol channel with no new seam, no new event kind and no
inbound HTTP surface. What iMessage adds is a **machine boundary**: the component that owns the Mac
integration must run on the Mac, in a GUI session, under a stable binary path.

## Decision

1. **Two components, one contract.** A new `type: imessage` channel adapter in `kian-superagent`
   (Footprint-Ladder level 2, behind the existing seam) talks over plain HTTP to a new
   **`kian-imessage-bridge`** daemon on the Mac. The bridge owns every macOS-specific thing
   (`imsg rpc` child supervision, `chat.db` cursor, TCC-facing behaviour, the local token); the
   adapter owns every kian-specific thing (mapping, allowlist, dedupe, degradation, policy). The
   bridge is a **separate crate and binary**, like `kiand` — not linked into `kian` and not a
   plugin sidecar (a sidecar would run on the Linux server, which is the wrong host).
2. **The bridge ABI is kian-owned and narrow.** Three routes: `GET /v1/health`,
   `GET /v1/updates` (bounded long-poll over a `rowid` cursor), `POST /v1/send`
   (with a caller-supplied `client_id` as the delivery idempotency key). No read-all route, no
   chat listing, no arbitrary target. The ABI is versioned (`protocol: 1`) and is the only thing
   kian knows about iMessage; a different bridge implementation (a hosted Mac, a VM) can replace it.
3. **The bridge runs as a LaunchAgent in the logged-in Messages user's session**, loopback-only,
   reached by the server over an SSH remote-forward (the tunnel pattern the owner already uses) or
   a mesh VPN. This is the decision forced by fact 3: it is the only context where Full Disk Access
   and Automation → Messages reliably attach to a grantable binary path.
4. **iMessage is not reply-only, and the contract says so.** Unlike iLink there is no
   `context_token`: a send needs a handle or chat GUID. The reply target is the inbound sender's
   handle; a per-handle cache of the latest inbound `chat_guid` keeps replies in the same thread and
   service. The recipient allowlist (`topics`, the same table as the receive allowlist) is the
   security boundary, because the reply target is model-controlled.
5. **The identity shape is a dedicated Apple ID.** The supported deployment is a dedicated Mac (or
   VM) + a dedicated Apple ID for the bot. The personal-account self-chat shape is **rejected**:
   a message from the same account is recorded with `is_from_me = 1`, which the adapter must drop to
   avoid an echo loop, so the bot could never see it. SMS needs same-account Text Message Forwarding
   and is therefore not available in the recommended shape; SMS is phase 2.
6. **No new surface, no new core, no new event kind, no kian-side cursor.** The adapter only makes
   outbound requests; `kian serve` mounts nothing. `event_id = message.guid`. The bridge's
   `rowid` position is bridge-local disposable transport state (kian never reads or writes it);
   admission idempotency on `guid` is the durable guard, exactly as `message_id` is for WeChat.
   `kian-core`, `kian-store`, `kian-orchestrator`, EVENT-MODEL and API-CONTRACT show zero diff.
7. **Untrusted by default, forced; privacy-filtered at the source.** The channel joins the assembly's
   forced untrusted set. Because `chat.db` on a personal Mac is a firehose of *all* conversations,
   the bridge's `watch.participants` filter (a privacy filter, not a policy decision) is the
   documented default and `kian imessage check` diffs it against `topics` so a silent-drop mismatch
   is visible to the operator.
8. **Best-effort by construction, with the blast radius named.** FDA revoked, Automation denied,
   `imsg` missing, Messages signed out, Mac asleep, DB replaced, bridge restarted: each has a row in
   the degradation table, each is an audit line plus an honest `ChannelError`, and none of them can
   touch another channel or a session's stream. This is the WeChat contract discipline applied to a
   weaker foundation.

## Alternatives considered

| # | Alternative | Why rejected |
|---|---|---|
| **D2** | **Adapter spawns `ssh <mac> imsg rpc` directly** (the OpenClaw remote-Mac pattern). No bridge process, no HTTP, smallest code. | Sends run in the SSH/TCC context and can fail with `-1743` ungrantably (fact 3); the kian contract would become a third-party stdio RPC pinned to `imsg`'s version, with no place to own the health/degradation map. It is the right *fallback* when the operator cannot run a LaunchAgent (recorded in the runbook), not the default. |
| **D3** | **Run `kian serve` on the Mac and implement the adapter in-process** (optionally spawning `imsg rpc` locally). | Moves the control plane and the session store onto a laptop, loses the worker/endpoint topology, and makes iMessage availability a precondition for server availability. A channel must not decide where the product runs. |
| **D4** | **BlueBubbles server as the bridge**, adapter speaks its REST/webhooks. Mature, handles media/reactions/typing. | Heavy Node/Electron service with its own data store and webhook/password surface; the ecosystem that built the best-documented agent gateway is actively migrating **away** from it to `imsg` (`https://docs.openclaw.ai/announcements/bluebubbles-imessage`), and its advanced features still need SIP-off private APIs. A dependency with a retirement date is not a foundation. |
| **D5** | **pypush / reverse-engineered iMessage from the Linux server** — no Mac at all. | Documented Apple blocks and **hardware bans** on users of this route; an encrypted, un-documented protocol with a hostile counterparty is the opposite of a durable channel (AGENTS.md honesty/constraint 7). Rejected outright, not deferred. |
| **D6** | **Apple Messages for Business** (official). | Requires a registered MSP and Apple account approval — no self-serve path for a personal assistant; it also answers as a business, not as the bot's Apple ID the owner texts. |
| **D7** | **iOS Shortcuts "message received" automation → HTTP → kian.** | Shortcuts cannot reply into the triggering conversation through a supported action, runs only when the phone is unlocked, and exposes no reliable sender routing; a custom iOS app is the separate `kian-ios-worker` route (runbook) about *workers*, not channels, and cannot promise 24/7 background execution. |
| **D8** | **Reuse the worker/endpoint transport** (`kiand` on the Mac) to carry channel commands to `imsg`. | Conflates agent execution authority with channel transport: endpoint exec is attributed to sessions and gated by agent permissions, while a channel needs an unattributed, always-on reader. It also couples the channel's liveness to worker membership. |
| **D9** | **An inbound webhook surface on `kian serve`** fed by the Mac bridge. | The Mac is behind NAT (the owner's deployment reaches it through a reverse SSH tunnel); an inbound route would need the same tunnel plus a new authenticated server surface, for no gain over the bridge's outbound-call direction. ADR-011's GitHub webhook is a server *receiver* because GitHub can reach the server; a Mac cannot. |

## Consequences

**Positive.**

- iMessage access reuses the proven channel pattern: pure mapping, allowlist both ways, forced
  untrusted, `guid` idempotency, bounded split, honest errors, one normative doc. No core concept,
  no event kind, no wire change. Rollback is removing a config key.
- The macOS fragility is confined to one small daemon with its own health surface, and the bridge
  can be replaced without touching kian (a hosted Mac or VM is a config change, not a code change).
- Unlike WeChat, the channel is **not reply-only**: proactive/escalation sends to allowlisted
  handles need no new mechanism.
- The `client_id` idempotency key turns the outbox's at-least-once retry into effectively-once for
  the common timeout case — a property the WeChat channel does not have.

**Negative / costs.**

- A second machine is in the critical path: the Mac must be awake and logged in, and its uptime is
  the channel's uptime. The Mac is a single point of failure for this channel (not for kian).
- Two TCC grants, one external binary (`imsg`, MIT, pinned `0.11.x`) and an operator-run LaunchAgent
  — more setup than any other channel. The runbook exists because of this.
- A dedicated Apple ID usually means a dedicated Mac or macOS user; on the owner's laptop that
  costs the personal Messages profile. Shape C (second Apple ID in the same user) is possible but
  unmeasured (§13.1 of the contract).
- Because `chat.db` holds every conversation on the Mac, the bridge's participant filter is a real
  privacy control; an operator who leaves it empty sends personal message traffic across the bridge
  (the adapter drops it, but it transits). Documented loudly rather than silently permitted.
- Delivery is text-only, DM-only, iMessage-only in phase 1; SMS, media, groups, reactions and
  read receipts are phase 2, and read receipts/typing need SIP disabled (private API).

**Neutral.**

- The outbox's bounded retry still exists, so an uncertain send followed by a bridge restart can
  duplicate a message. Recorded as an honest bound; a seam-level delivery idempotency key is a
  phase-1b candidate with its own ADR.
- The bridge is a new crate in the workspace; CI builds it, and its tests run against a fake `imsg`
  binary so the macOS dependency never enters CI.

## Implementation notes (landed, 2026-11-19)

The implementation follows the decision with two deliberate simplifications, both recorded in the
contract (§2.5, §13):

1. **The bridge polls `messages.after`; it does not hold a `watch.subscribe`.** The ABI is
   unchanged (`/v1/updates` still takes an exclusive `rowid`, a `wait_ms` and a `limit`), but the
   bridge is a plain request/response client of one `imsg rpc` child — no notification reader, no
   buffer/overflow state, no second state machine on the Mac. Latency is bounded by the 250 ms
   cadence; a future low-latency bridge can subscribe instead without touching kian.
2. **The first boot bootstraps the cursor at the tail** by paging `messages.after` from rowid 1
   once (bounded by `watch.page_limit`/a page cap). That keeps the honest "before this bridge
   existed" gap while never replaying history into a new install.
3. **The `imsg` floor is `0.14.0`.** `0.11.x` lacks protocol-v1 `status`/`initialize`,
   `messages.after` and the typed dispositions; the first L3 attempt failed with `-32601`. The
   bridge is unchanged — the contract pins the floor (§2.5) and the runbook upgrades the
   dependency (L3 used `0.21.1`).
4. **First L3 run (2026-11-20).** Shape C (a bot iMessage account with the Mac's iCloud left
   personal) works end to end on macOS 27.2: the iPhone message was admitted once, the reply was
   delivered in 6.4 s, and the child-inheritance TCC question was answered (the grant on the
   bridge binary covers its `imsg` child). The contract's §12 records the evidence; §13 keeps the
   remaining gaps.

Shipped pieces: `crates/kian-imessage-bridge` (config, `imsg rpc` supervisor, cursor store, ABI
router with the `client_id` cache, 22 unit tests); `crates/kian-superagent/src/imessage{,/bridge,/handle}.rs`
(adapter + ABI client + normalization, 16 integration tests over a scripted bridge mock);
`kian-cli` config/validation/assembly/`kian imessage check`; the router prompt's iMessage target
sentence (new sessions pin a new `policy_version`; live prompts replay byte-identically);
`crates/kian-cli/tests/imessage_cli.rs` (real `Server` + sqlite + mock bridge: admission exactly
once across replay and a real restart; fail-closed env token; the read-only preflight).

## Verification plan (what must be true before this is considered complete)

1. L3 on the owner's Mac for the recommended shape: create the bot Apple ID, grant FDA + Automation,
   install the LaunchAgent, open the tunnel, text from the iPhone, observe `input/admitted` and
   `channel/delivered`, and record latency, sleep/wake, a revoked-FDA run and a `-1743` recovery.
2. Answer the five deployment questions in the contract's §13 (same-user second Apple ID,
   fast-user-switching, sleeping-Mac delivery completeness, SMS availability, real limits) — or keep
   them explicitly unverified.
3. Confirm the `-1743` restriction does **not** apply to a LaunchAgent-started bridge on macOS 27
   (the whole architecture rests on it; if it does, fall back to D2 and record why).

## Open questions

- Should `kian serve` ever run a **second** iMessage channel against a different bridge (multi-line)?
  Phase 1 says one channel / one account; the config validation enforces it.
- Should the bridge expose its counters on a Prometheus endpoint for the server to scrape, or is the
  health snapshot consumed by `kian imessage check` enough? Deferred to phase 1b.
- Does the bundle want a per-channel "quiet hours" policy for iMessage specifically? No: output
  policy stays channel-agnostic (`speak_rules` already exists).
