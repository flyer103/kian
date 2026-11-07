# ADR-018: A native WeChat iLink channel adapter, and a QR-issued credential file

- **Status**: Accepted (owner gate, 2026-11-07)
- **Date**: 2026-11-07
- **Scope**: bundle (`kian-superagent`), CLI/assembly (`kian-cli`), docs — no core, no store,
  no orchestrator, no wire surface
- **Supersedes**: none
- **Related**: AGENTS.md constraints 1/3/7/10; `design/DESIGN.md` §4.2/§6.2/§7;
  `design/CHANNEL-WECHAT.md` (the contract this decision establishes); ADR-011 (why the GitHub
  channel's receive half *is* a server surface, and WeChat's is not)

## Context

WeChat is the channel where the owner's users actually are, and Kian has no first-party path to it.
Three properties of the platform decide the whole design:

1. **There is no public developer contract.** Tencent ships a WeChat ClawBot bot client
   (`openclaw-weixin`, MIT-licensed; the npm package ships its source) which talks to an
   HTTP/JSON bot API — the *iLink Bot API* — but that API has no developer documentation and no
   stability promise. What is known about it is reverse-documented from that client plus
   community corroboration, and is frozen as the round's protocol facts in
   `design/CHANNEL-WECHAT.md` §2.
2. **The API is poll-shaped, not push-shaped.** Receive is a long-poll (`POST /ilink/bot/getupdates`
   that the server holds for ~35 s); send is a one-shot `POST /ilink/bot/sendmessage` that must
   echo the `context_token` issued with the inbound message being answered. There is no public
   callback URL to register, and by construction there is no way to message a user who has not
   messaged the bot first.
3. **The credential is a session token minted by a QR scan**, not a static app secret. It is
   issued by the login flow (`bot_token`), it expires (the official client treats `errcode -14` as
   "stale token, pause an hour, operator re-login"), and it cannot be pre-provisioned at
   deployment time.

Kian's existing channel inventory covers a webhook/push receive (GitHub, which mounts
`POST /webhooks/github` — ADR-011) and a poll receive (lark). The seam
(`ChannelAdapter`: `subscribe`/`send`, `ChannelEvent`/`ChannelDelivery`/`SendReceipt`/
`ChannelError`) already expresses both shapes, so the only open questions were **where** the
adapter lives, **how** the token is stored, and **how much** the channel is allowed to promise.

## Decision

1. **A native Rust adapter for the iLink Bot API, in the bundle.** A `type: wechat` channel is
   implemented in `kian-superagent` (`wechat.rs` + `wechat/` submodules) behind the existing
   `ChannelAdapter` seam, assembled by `kian-cli` exactly like `type: lark`/`type: github`.
   Footprint-Ladder level 2 (plugin-hosted code over a core seam): no new trait, no new seam.
2. **No new surface, no new core, no new event kinds.** The receive path is one outbound
   long-poll loop per configured channel; nothing listens. `kian-core`, `kian-store` and
   `kian-orchestrator` are untouched; `design/API-CONTRACT.md` and `design/EVENT-MODEL.md` show
   zero diff. An inbound message becomes an ordinary admitted input with `event_id = message_id`.
3. **A new normative contract document**, `design/CHANNEL-WECHAT.md`. The protocol facts, the
   mapping, the credential/config/CLI surfaces, the allowlist, dedupe, the cursor bound, the
   degradation table and the unverified list need one owner; restating them in DESIGN §4.2 alone
   would bury a reverse-documented protocol inside a general chapter. DESIGN keeps a pointer.
4. **The credential is a local file, not an environment variable.** `kian wechat login` runs the
   QR flow and atomically writes `{version, base_url, token, account_id, user_id, saved_at_unix_ms}`
   with mode `0600` to an explicit config path (`credentials_file`). Serve reads it at startup,
   fails closed and names the command when it is missing or unusable, and adopts a replaced token
   by watching the file's mtime (no restart). This is a **deliberate, recorded deviation** from
   the static-secret rule (secrets are env-only, never config values): the token is issued at
   runtime and rotates by re-login, so an env var would be stale by construction and a restart
   would be the only way to pick up a new one.
5. **Best-effort is contractual.** The channel owns exactly two neighbours' worth of blast radius:
   it may pause, degrade or die, and the session/event stream and every other channel keep
   running. Protocol errors are audit + bounded backoff; `-14` is pause + resume-on-file-change;
   an unparseable response is never converted into an event.
6. **Reply-only, text-only, single-account for phase 1.** A send requires the latest inbound
   `context_token` for that user; with none, the adapter returns `Unavailable` with an explicit
   reason rather than pretending a proactive send could work. Media, typing, `notifystart`/`stop`,
   group chat and multi-account are recorded non-goals with a phase-2 home.
7. **Untrusted by default, forced.** WeChat text is attacker-controllable; the channel is added to
   the assembly's untrusted set and the config cannot turn the marker off. The `topics` table is
   simultaneously the receive allowlist — an unknown sender is an honest drop with an audit line.
8. **Transport state stays transport state.** The `context_token` cache and the dedupe ring are
   in-memory; `get_updates_buf` is never persisted. There is no position table and no new durable
   row: the durable guard is admission's `(session, event_id)` idempotency, exactly as for lark.

## Alternatives considered

| Alternative | Rejected because |
|---|---|
| Front-end the **OpenClaw bridge** process instead of implementing iLink | Adds a second runtime, a second release cadence and an un-durable hop in the receive path (the bridge would have to hold events while Kian is down); the iLink client source is MIT-licensed and small, so a native adapter is less code than owning a bridge integration plus its failure modes. It would also make the channel's trust boundary depend on a third party's process |
| WeChat **公众号** (Official Account) platform | Needs a verified account and a **public inbound callback URL** — i.e. a new server surface beside the control, ops and metrics planes (the thing ADR-011 had to justify for GitHub), with its own signature/verification scheme; and its product shape is broadcast/reply, not a 1:1 assistant session. Kian is self-hostable behind no public URL; the route would invert that |
| **微信客服** / WeCom customer-service API | Enterprise-tenant and secret-based (a different credential model), and it models a queued customer-service conversation rather than a bot session; also a new surface with its own protocol. Wrong shape for the product, not just more work |
| Personal-account protocol hooks (reverse-engineered personal WeChat clients) | Violates the platform's terms, has no support boundary, risks account bans for the operator, and requires injecting into a third-party client process. Not a surface a durable-execution runtime should hang a deployment on |
| No WeChat channel; keep lark/GitHub only | Leaves the users' actual channel unserved, and the seam already supports poll-shaped receive — the cost is one bundle module plus a CLI half, not a new concept |
| Store the bot token in `kian.yaml` | A credential in a config file is committable by accident, differs per deployment and defeats redaction; the static-secret rule exists for this reason. Rejected in favour of the 0600 file |
| Store the bot token in an env var (`KIAN_WECHAT_TOKEN`) | Consistent with the rule, but structurally stale: the token is issued by a QR scan at run time and rotates on re-login, so an operator would have to edit the service environment and restart on every rotation. The file + mtime watch is the honest fit; the deviation is recorded here rather than hidden |
| Persist `get_updates_buf` in a table to get exactly-once across restarts | Pre-event durable state outside the event log (constraint 1) for a cursor whose replay semantics are unverified; ADR-013 already rejected a position table for the same reason. Idempotent re-poll under `event_id` is the honest bound, and the downtime gap is documented instead of papered over |
| Emit a synthetic event for non-text messages ("unsupported content") | Would put channel noise into every subscription and the audit stream; a queue of unanswerable events is not information. Audit line only |
| Support proactive push by caching tokens per user across restarts | The protocol issues a token per inbound message for a reply; a long-lived cache would be an authoritative store outside the event log with unverified validity. Out of scope, and `Unavailable` says so plainly |

## Consequences

**Positive**

- WeChat reach with no new server surface, no new core concept and no new event kind; the channel
  is a bundle module plus a CLI half, so the narrow waist holds.
- A protocol break is a channel-local event: audit, backoff, pause — sessions and other channels
  are untouched, which is the property that makes a reverse-documented API acceptable at all.
- The credential deviation is bounded and visible: one documented file schema, one command, one
  fail-closed startup check, one documented rotation path.
- Reply-only is stated instead of faked: an operator learns immediately why a proactive message is
  impossible (no token), rather than debugging a mysterious delivery failure.
- No durable state is added anywhere: dedupe, tokens and the cursor are all disposable, so the
  store/event contracts do not grow for this channel.

**Negative / risks**

- **No contract means no guarantee.** Tencent can change or remove iLink; the unverified list in
  `design/CHANNEL-WECHAT.md` §14 is a standing risk, and L3 evidence is owner-run and may stay
  incomplete for a while.
- **Operator involvement is unavoidable.** `-14` needs a human to re-scan a QR code; the channel is
  not unattended-forever. A long unattended deployment will pause (honestly) until someone logs in.
- **The token lives in a file.** File permissions and the host's disk become part of the trust
  boundary; a file that is world-readable is reported but cannot be prevented by Kian. No secret
  scanning in the repo can catch a copy the operator makes.
- **At-most-once across downtime, not exactly-once.** If the server does not replay from an empty
  cursor, messages during downtime are lost. This is a documented bound, not a bug, and closing it
  would need a new decision (a persisted cursor, i.e. pre-event durable state).
- **Prompt-prefix change.** Documenting the WeChat target shape changes the template constant, so
  new sessions pin a new `policy_version` fingerprint; live sessions replay byte-identically
  (constraint 3). Operators see a policy-version bump without a behavioral change.
- **Split sends are at-least-once.** A failure mid-split leaves earlier parts delivered; the
  outbox's retry re-drives the whole shaped delivery, so a duplicate is possible on the failure
  path — an honest trade against dropping text.

## Verification

`design/CHANNEL-WECHAT.md` §13 owns the evidence: L1 (default-green unit/adapter tests against a
scripted mock iLink server), L2 (real `Server` + sqlite + mock iLink: one admission, replay still
one, channel failure leaves session state untouched, token never in logs/config), L3 (env-gated,
owner-run: real QR scan; the §14 unverified items measured). The four repository gates plus
`cargo test -p kian-cli --test docs_map` are required. Independent review must confirm: no core
change, no new surface, no new event kind, forced-untrusted, credential handling (0600, no secrets
in repo/logs, fail-closed), bounded loops/backoff, honest degradation, and that
`API-CONTRACT.md`/`EVENT-MODEL.md` show zero diff.

## References

- `design/CHANNEL-WECHAT.md` — the channel contract this decision establishes.
- `design/DESIGN.md` §4.2 (channel table), §6.2 (`ChannelAdapter` seam), §7 (bundle placement).
- ADR-011 — the inbound webhook plane (why GitHub's receive half is a surface and this one is not).
- ADR-013 — thread intake projection and the rejected position table.
- Tencent WeChat ClawBot client `openclaw-weixin` (MIT) and its shipped iLink Bot API usage; the
  community protocol notes corroborating it. Both are the frozen round-10 input; the protocol
  facts are recorded in `design/CHANNEL-WECHAT.md` §2 and are not re-researched per card.
