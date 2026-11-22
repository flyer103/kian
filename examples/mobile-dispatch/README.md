# mobile-dispatch — text your Mac, get bounded work done, get the result back

A resident Router listens to one iMessage conversation. A task texted from the phone is judged,
delegated to an executor under a Mandate, and the report comes back in the same conversation —
short enough to read on a lock screen, with 详情 for the full text. It is the smallest shape that
exercises the bundle's phone side end to end: admission allowlists, a speak-rule, the IM reply
budget, the outbox, and one worker template.

```
iPhone Messages ──► iMessage channel (allowlist) ──► Router (resident session)
                                                        │  spawn_worker (Mandate)
                                                        ▼
                                                     executor ──► report
                                                        │
                                              reply / deliver_structured ──► outbox ──► iPhone
```

## Before you start

The Mac is the endpoint, not a convenience: iMessage has no API, so a Mac signed into a dedicated
Apple ID must stay awake running `kian-imessage-bridge`
([design/CHANNEL-IMESSAGE.md](../../design/CHANNEL-IMESSAGE.md),
[docs/runbook-imessage.md](../../docs/runbook-imessage.md) §3 is the step-by-step).

| Need | Why |
|---|---|
| A Mac signed into the **bot** Apple ID | The message must land in that Mac's `chat.db`; signing the personal ID in means the Mac stops being your personal Messages device |
| Full Disk Access + Automation → Messages | Read the database, send through Messages.app |
| A private transport to this host | `ssh -R 7420:127.0.0.1:7420 …` or a mesh VPN — never expose the bridge |
| `KIAN_IMESSAGE_BRIDGE_TOKEN` here | env-only; the bridge token never lives in `kian.yaml` |

## Run it

```bash
export DEEPSEEK_API_KEY=...
export KIAN_IMESSAGE_BRIDGE_TOKEN=...      # same secret as the bridge's token_file

mkdir -p .state                            # the sqlite file's directory
kian imessage check --config kian.yaml     # read-only preflight: protocol, imsg, gates, allowlist diff
kian serve --config kian.yaml              # terminal 1
./run.sh                                   # terminal 2: submit the charter, follow the stream
```

Edit `topics` in `kian.yaml` to the owner's handle (and keep the bridge's `watch.participants` a
superset — `kian imessage check` diffs the two and fails closed on a mismatch).

Then, from the phone:

1. `跑一下 <repo> 的测试，结果给我` — the Router delegates; the reply is the result, inside 300 units.
2. Reply `详情` — the full text arrives, split into ordered messages.
3. Send a task with no acceptance criteria — the Router asks for the missing piece once instead of
   delegating on a guess.

A delegated task reaches you **once**: the `outcome-only` speak-rule drops the dispatch ack and
lets the result through, so the buzz you get is the report, not "working on it". Suppressed sends
still leave an honest tool error the Router sees plus an audit line and a counter, so nothing
disappears silently.

### No Mac? Run the shape with a mock channel

The admission path, the Router, the worker, the speak-rule, the budgeting and the outbox are the
same over a `mock-im` channel, so the shape is testable without the phone leg:

```yaml
# replace the whole `phone:` block in kian.yaml with this one:
    phone:
      type: mock-im
```

```bash
mkdir -p .state
kian serve --config kian.yaml &
kian mock-channel-send phone --text "运行 echo hello，把输出告诉我" --url http://127.0.0.1:7461
kian tickets --db ./.state/kian.db
```

The committed stream then reads `input/admitted` → `turn/*` → `tool/call` → `channel/delivered`, and
a delegated task adds `thread/started` → the worker's own `tool/call`/`tool/result` →
`thread/completed` before the result delivery. Nothing about the mock channel is a promise: it is
the test transport, not a product surface.

## What to look at

| Surface | What it shows |
|---|---|
| `kian tickets` | the stage board: which worker holds what, and what it is waiting for |
| `kian session inspect <router-session>` | one admitted `untrusted: true` input → one `channel/delivered` |
| `kian cache-stats --json` | prefix reuse across turns; `unexpected_prefix_change` must stay zero |
| `kian board` / `kian tasks --all` | the delegation rows (needs `superagent.board.enabled`) |

## Boundaries

- **A pending `ask` is answered only by the closed vocabulary.** With `superagent.confirm_answers`
  naming the channel, `approve <code>` / `reject <code> <reason>` are intercepted *before* the model
  and written to the confirmation plane; every other message — including a free-text "同意" — is an
  ordinary task input, so the Router never treats prose as a decision
  ([design/CONFIRMATION-MODEL.md](../../design/CONFIRMATION-MODEL.md) §14d). Without that key the
  pending call is answered only by the control plane (`kian pending`, `kian confirm`).
- **One speak condition per channel.** `result-ready` and `decision-needed` are mutually exclusive,
  which is why this example uses `outcome-only` (results and decisions, no dispatch ack). A channel
  that must also hear progress chatter is a different shape: `always`, with the volume handled on
  the Router prompt.
- **Text only, both directions.** An iMessage attachment or screenshot never reaches kian
  (CHANNEL-IMESSAGE §4); ask for evidence in text, or open the desktop.
- **One handle, one topic.** Follow-ups while a worker is live still route to the Router, which
  decides; the live-ticket ownership rule (`followup_ownership`) keeps two workers off one topic.
- **Not here:** the watchdog ground-truth session, the worker roster
  (`superagent.roster` + `superagent.multiagent` for named delegates), and scheduling (`kian cron`).
  Each is a config block on top of this shape — see [docs/advanced.md](../../docs/advanced.md).
