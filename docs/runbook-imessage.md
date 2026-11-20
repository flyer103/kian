# Runbook — iMessage channel (design + phase-1 contract)

- **Status**: **phase 1a landed (2026-11-19); first live L3 run 2026-11-20.** The adapter, the
  `kian-imessage-bridge` crate, the config key and `kian imessage check` exist and are covered by
  L1/L2 tests ([design/CHANNEL-IMESSAGE.md](../design/CHANNEL-IMESSAGE.md),
  [ADR-042](../design/decisions/ADR-042-imessage-channel.md)); the Mac/iPhone legs were executed on
  2026-11-20 (§7 records what was verified and what remains).
- **Scope**: how an iPhone iMessage reaches `kian serve`, the Mac-side bridge that makes it
  possible, the three macOS grants, the transport, and the failure modes operators actually hit.
- **Owner docs**: semantics and the bridge ABI live in
  [design/CHANNEL-IMESSAGE.md](../design/CHANNEL-IMESSAGE.md); the why in
  [ADR-042](../design/decisions/ADR-042-imessage-channel.md). This file never restates the
  contract — it is the step-by-step.

## 1. The shape, in one paragraph

An iMessage from the iPhone is delivered by Apple to **a Mac signed into the bot's Apple ID**, where
Messages.app stores it in `~/Library/Messages/chat.db`. A small daemon on that Mac
(`kian-imessage-bridge`) reads the database (Full Disk Access), sends through Messages.app
(Automation), and exposes three loopback-only HTTP routes. `kian serve` — on the Linux server —
holds a `type: imessage` channel that long-polls the bridge over an SSH remote-forward and posts
replies back. The iPhone sees an ordinary iMessage conversation; kian sees an ordinary channel.

```
iPhone Messages ──iMessage──▶ Apple ──▶ Mac Messages.app ──▶ chat.db
                                                             │ Full Disk Access
                                                     kian-imessage-bridge  (LaunchAgent, loopback:7420)
                                                             ▲            │
                                        ssh -R 7420 …        │            │ imsg send → AppleScript
                                                             │            ▼
                                     kian serve (VPS) ── imessage adapter ──▶ Messages.app ──▶ iPhone
```

**The Mac is the endpoint, not a convenience.** There is no iMessage API; no Mac means no channel
(see ADR-042 D5 for why pypush is rejected).

## 2. Prerequisites

| Requirement | Detail |
|---|---|
| A Mac | macOS 14+; it must be **awake and logged in** while the channel is expected to work. A MacBook with the lid closed and asleep is offline for this channel (messages are expected to catch up on wake — unverified, contract §13.3). |
| A **dedicated Apple ID** for the bot | Create it, enable two-factor with the owner's phone as the trusted number. Do **not** use the personal Apple ID: a message from the same account is recorded `is_from_me`, which the channel drops (contract §2.4). |
| `imsg` **≥ 0.14.0** (0.21.1 verified) | `brew install steipete/tap/imsg` / `brew upgrade imsg` (repo: <https://github.com/openclaw/imsg>; MIT; macOS 14+). On Homebrew 5 trust the third-party tap first: `brew trust steipete/tap`. `0.11.x` lacks the RPC surface the bridge calls (`status`, `messages.after`, typed dispositions) and fails the preflight with `-32601` (contract §2.5). |
| Full Disk Access | For the bridge binary (or the LaunchAgent's executable path). |
| Automation → Messages | For the same binary; the first send prompts, or pre-approve in System Settings. |
| A private transport to the server | The owner's installation already uses an SSH tunnel from the Mac (`ssh -L 7400 …`); add a **remote** forward for the bridge (`ssh -R 7420:127.0.0.1:7420 …`). Tailscale is an alternative (bind the bridge to loopback, forward the port, or bind the Tailscale address and set `listen` accordingly). Never expose the bridge publicly. |
| `KIAN_IMESSAGE_BRIDGE_TOKEN` on the server | The same secret as the bridge's `token_file`; env-only, never in `kian.yaml`. |

## 3. Set up the Mac

### 3.1 Sign Messages into the bot Apple ID

1. Create the bot Apple ID; turn on two-factor (trusted phone = the owner's).
2. On the Mac: **Messages → Settings → iMessage**. If the personal account is signed in, sign out
   (this is the real cost of shape A/C: the Mac stops being the personal Messages device), then sign
   in with the bot Apple ID.
3. In **Send & Receive**, confirm the bot's address is checked; note every reachable address — the
   operator will text one of them.
4. Send a test message from the owner's iPhone to that address. It must appear in Messages on the
   Mac. If it does not, fix the Apple side first (`launchctl kickstart -k system/com.apple.apsd`,
   `…/com.apple.CommCenter`, `…/com.apple.identityservicesd`, `…/com.apple.imagent`, then retry) —
   no kian setting can compensate for a message that never reached the Mac.

> **Do not** use a message addressed to the bot Apple ID **from the same Apple ID** as the Mac: it
> is recorded as outgoing (`is_from_me = 1`) and is dropped by contract. That is why the bot has its
> own Apple ID.

### 3.2 Grant the two TCC permissions

1. **Full Disk Access**: System Settings → Privacy & Security → Full Disk Access → add the bridge
   binary's absolute path (the same path the LaunchAgent runs; not "Terminal"). To debug the grant
   first, add the terminal and re-open it: `imsg chats --limit 3` must print chats and
   `imsg status --json` must report `database.ready: true`.
2. **Automation → Messages**: run one manual send from the *same launch context* the LaunchAgent
   will use (`imsg send --to <your own iPhone number> --text probe` from the logged-in GUI session).
   Approve the prompt. If the toggle is missing, open System Settings → Privacy & Security →
   Automation and enable Messages for the bridge/terminal.
   **Fallback if the grant does not reach the child:** TCC usually attributes to the LaunchAgent's
   binary and its children, but if `/v1/send` reports `not_started`/`-1743` while reads work, add
   **`/opt/homebrew/bin/imsg` itself** to both Full Disk Access and Automation → Messages, then
   `launchctl kickstart -k` the agent. That is the path-level grant, and it is the operator's escape
   hatch when the parent attribution misbehaves (contract §13.9).

`imsg status --json` proves reads; it does **not** prove Automation. Only a real send does.

### 3.3 Install and configure the bridge

```bash
# on the Mac, from a kian checkout
cargo build -p kian-imessage-bridge --release
mkdir -p ~/bin
install -m 0755 target/release/kian-imessage-bridge ~/bin/kian-imessage-bridge

# token: one line, 0600 — the server holds the same value in KIAN_IMESSAGE_BRIDGE_TOKEN
umask 077
openssl rand -hex 32 > ~/.config/kian/imessage-bridge.token
```

`~/.config/kian/imessage-bridge.json` (mode `0600`):

```json
{
  "version": 1,
  "listen": "127.0.0.1:7420",
  "token_file": "~/.config/kian/imessage-bridge.token",
  "imsg": "/opt/homebrew/bin/imsg",
  "db": "~/Library/Messages/chat.db",
  "state_dir": "~/Library/Application Support/kian-imessage-bridge",
  "watch": {
    "participants": ["+14155551212"],
    "poll_interval_ms": 250,
    "catchup_rows": 200,
    "page_limit": 200
  }
}
```

`watch.participants` is the **privacy filter**: only conversations with these handles are read from
`chat.db`. It must equal (or be a superset of) the kian `topics` keys; `kian imessage check` diffs
them. Leaving it empty means *every conversation on the Mac* transits the bridge — do not do that
on a personal account.

LaunchAgent `~/Library/LaunchAgents/com.kian.imessage-bridge.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.kian.imessage-bridge</string>
  <key>ProgramArguments</key>
  <array><string>/Users/you/bin/kian-imessage-bridge</string>
         <string>--config</string><string>/Users/you/.config/kian/imessage-bridge.json</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/you/Library/Logs/kian-imessage-bridge.log</string>
  <key>StandardErrorPath</key><string>/Users/you/Library/Logs/kian-imessage-bridge.log</string>
</dict>
</plist>
```

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kian.imessage-bridge.plist
launchctl kickstart -k gui/$(id -u)/com.kian.imessage-bridge   # after grant/update changes
curl -s -H "Authorization: Bearer $(cat ~/.config/kian/imessage-bridge.token)" \
     http://127.0.0.1:7420/v1/health | jq .
```

`/v1/health` must show `database.ready: true`, a non-empty `watch.participants`, and
`imsg.rpc: true`. `automation` stays `unknown` until the first send.

Two ordering facts from the L3 run (contract §2.5 rule 9, §12): **grant Full Disk Access before the
first bootstrap** — the bridge cannot start without it (`imsg rpc` fails `status` with `-32603`,
because the child opens the database before it serves any method), and TCC attributes the request to
the bridge binary's path while its `imsg` child inherits the grant. A stable path (`~/bin/...`) is
what the grant is bound to; rebuilding the binary *may* require re-adding it after a stale-grant
symptom (toggle the FDA entry off/on).

### 3.4 Open the transport

The forward must survive reboots and drops, so run it as its own LaunchAgent
(`~/Library/LaunchAgents/com.kian.imessage-tunnel.plist`) rather than a one-off shell:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.kian.imessage-tunnel</string>
  <key>ProgramArguments</key>
  <array><string>/usr/bin/ssh</string><string>-N</string>
         <string>-o</string><string>BatchMode=yes</string>
         <string>-o</string><string>ExitOnForwardFailure=yes</string>
         <string>-o</string><string>ServerAliveInterval=15</string>
         <string>-o</string><string>ServerAliveCountMax=3</string>
         <string>-R</string><string>7420:127.0.0.1:7420</string><string>vultr</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/you/Library/Logs/kian-imessage-tunnel.log</string>
  <key>StandardErrorPath</key><string>/Users/you/Library/Logs/kian-imessage-tunnel.log</string>
</dict>
</plist>
```

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kian.imessage-tunnel.plist
```

On the server: `curl -s -H "Authorization: Bearer $KIAN_IMESSAGE_BRIDGE_TOKEN" \
http://127.0.0.1:7420/v1/health` must answer the same JSON. A remote forward binds the **server's**
loopback only (no public exposure); keep it that way.

## 4. Set up `kian serve`

```yaml
superagent:
  channels:
    imessage:
      type: imessage
      imessage:
        bridge_url: "http://127.0.0.1:7420"
        topics:
          "+14155551212": imessage         # the owner's iPhone handle (allowlist both ways)
        service: imessage                   # dedicated Apple ID: SMS is unavailable; auto is safe too
    # ...
  subscriptions:
    - channel: imessage
      match: always
      route: router
```

```bash
export KIAN_IMESSAGE_BRIDGE_TOKEN=...          # the same secret as the Mac token_file
kian imessage check --config kian.yaml         # read-only preflight: health + gates + allowlist diff
kian serve --config kian.yaml
```

`kian imessage check` (contract §7/§8) should print, and fail closed on: the resolved channel, the
token presence, the bridge `protocol` (must be 1), `imsg.version`, `database.ready`,
`watch.participants` vs `topics` diff, and the automation state. It never prints the token.

## 5. Verify end to end

1. From the iPhone, text the bot's address: `hi`.
2. On the server, read the committed stream (the projection tool does not print events; query the
   store or export the session):

   ```bash
   sqlite3 -readonly /srv/kian/kian.db "
     select seq, kind, substr(envelope,1,160) from events
     where session='<router-session-uuid>' and envelope like '%imessage%'
     order by seq desc limit 10;"
   ```

   The `现在几点` round trip in the L3 run produced exactly one `input/admitted`
   (`event_id` = Apple `guid`, `untrusted: true`) followed by one `channel/delivered`
   (`target: {"handle": …}`) **6.4 s** later.
3. The iPhone receives the reply **in the same conversation** (the adapter passes the inbound
   `chat_guid`), as plain text (no markdown).
4. Failure probes worth running once: revoke Full Disk Access (receive stops, sends still work and
   report `verified: false`); deny Automation (sends `Unavailable` naming System Settings, receive
   keeps working); stop the bridge (adapter backs off, sends `Unavailable`, resumes when it
   returns); sleep the Mac and send a message (it should arrive on wake — record whether it does).

## 6. Troubleshooting

| Symptom | First check |
|---|---|
| Reads work, sends fail `-1743` | Automation → Messages for the **bridge's** path. If the bridge is running under SSH, move it to the LaunchAgent (contract §2.2) — an SSH-launched grant can attach to `sshd-keygen-wrapper` and never appear in the UI. |
| Sends fail `-1712` | Apple event timeout: a hidden permission prompt, or Messages.app wedged. Check the screen, restart Messages, retry. |
| Inbound silent, `chat.db` row exists | The bridge is not reading it: `database.ready`, `watch.participants` (a filter mismatch is a silent drop), or the bridge was started before the grant (restart it). |
| Inbound silent, **no new `chat.db` row** | Not a kian problem: Apple delivery never reached the Mac. `imsg chats --limit 5 --json`, then the `apsd`/`CommCenter`/`identityservicesd`/`imagent` kickstart above, then retry from the phone. |
| `kian imessage check`: `database.ready:false` | Full Disk Access is stale (it goes stale after Homebrew/OS updates): toggle the entry off and on, relaunch the LaunchAgent. |
| Bridge refuses startup: `imsg rpc preflight failed: imsg rpc error -32601: Method not found` | `imsg` is older than `0.14.0` — `brew upgrade imsg` (on Homebrew 5, `brew trust steipete/tap` first). The bridge's whole RPC surface (`status`/`messages.after`/typed dispositions) landed in 0.14.0; see contract §2.5. |
| Bridge refuses startup: `-32603 … authorization denied` | Full Disk Access is not granted to **this binary path**. TCC attributes to the responsible process; grant `~/bin/kian-imessage-bridge` (its `imsg` child inherits it), then `launchctl kickstart -k`. |
| 401 from the bridge | `KIAN_IMESSAGE_BRIDGE_TOKEN` ≠ the Mac `token_file` (rotated one side only). |
| Bridge unreachable from the server | The `ssh -R` is down; check the tunnel process, then `/v1/health` on the Mac. |
| Duplicate replies after a timeout | The outbox retried an uncertain send across a bridge restart (contract §13.6). The bridge's `client_id` cache prevents the common case; a duplicate means the cache was lost. |
| Strange `is_from_me` behaviour / the bot ignoring the owner | The Mac is signed into the **same** Apple ID the owner texts from (self-chat). Use a dedicated bot Apple ID. |
| Messages stop arriving after a macOS update | Re-verify both grants; `imsg --version` against the **0.14.0 floor**; TCC entries go stale per path after updates. |

## 7. L3 record (2026-11-20) and what remains

**Verified** (macOS 27.2, dedicated bot Apple ID as the iMessage account with the Mac's iCloud left
personal, `imsg 0.21.1`, bridge + tunnel as LaunchAgents, `kian serve` on the VPS):

- iPhone → kian: one `input/admitted` per message (`event_id` = the Apple `guid`,
  `untrusted: true`); the previous test messages were tail-replayed once on first start and then
  admission idempotency held.
- kian → iPhone: `channel/delivered` with `target: {"handle": "<owner>"}`; the `现在几点` round trip
  took **6.4 s** (single sample, model turn included).
- Full Disk Access on the **bridge binary path** covered its `imsg` child (tccd attributes to the
  responsible process); Automation was granted on the first send.
- `kian imessage check` reported the bridge and failed closed without the token.
- A malformed reply target produced an honest `DeliveryFailed`, and the Router retried with the
  correct `{"handle": …}` shape — the error loop works without extra model-side machinery.
- **Restart recovery**: restarting `kian serve` mid-turn closed the drain honestly and the next input
  re-claimed the same session (`drain/claimed` → reply in 3.2 s); no re-arm, no lost work.
- **Send idempotency**: posting the same `client_id` twice sent once and returned
  `deduplicated: true` on the second response.
- **Long text**: a 2616-character single send was accepted natively (`verified: true`); production
  parts stay ≤2000 by the adapter's split.
- **Burst**: six back-to-back sends all delivered, in order, with no rate-limit and zero uncertain
  dispositions (4.4 s cold, then 0.12–0.97 s).

**Remaining** (contract §13): sleeping/logged-out Mac delivery completeness (§13.3), SMS as a
channel (§13.4), the `详情` full-delivery split and sustained rate limits (§13.5 partially measured),
fast-user-switched sessions (§13.2), the `-1743` recovery path (§13.9 resolved for attribution
only), latency under load (§13.11), and the private-API features (typing/read receipts) which stay
out of scope.
