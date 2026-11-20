# Runbook: the vultr kian server

Scope: the owner instance on vultr — `kian.service`, its workers, and the operational guard rails.
Product semantics live in [WORKER-MODEL](../design/WORKER-MODEL.md); this file is the deployment.

## Layout

| Path | What |
|---|---|
| `/opt/kian/bin/kian` | server binary (timestamped backups `kian.bak-<ts>` beside it) |
| `/opt/kian/bin/kiand` | worker binary used by the vultr worker |
| `/srv/kian/kian.yaml` | config — **must be `kian:kian 0640`** |
| `/srv/kian/kian.db` | sqlite store (events + queue + registry) |
| `/etc/systemd/system/kian.service` | unit: `User=kian`, `Restart=always`, `OnFailure=kian-alert@%n` |
| `/etc/systemd/system/kian-watch.{service,timer}` | watchdog: probes every 30 s |
| `/usr/local/bin/kian-watch.sh` | the probe (unit state, restart churn, config readability, `kian workers`) |
| `/usr/local/bin/kian-config-guard` | config ownership guard, also `ExecStartPre=+` of the unit |
| `/usr/local/bin/kian-alert.sh` | `OnFailure` handler |
| `/var/log/kian-watch.log`, `/var/log/kian-alert.log` | alert records |

## Config writes — the failure that took the box down

`kian serve` runs as `User=kian`. **Never rewrite `/srv/kian/kian.yaml` as root with an atomic
replace** (`open(tmp)` + `rename`): the new inode is `root:root`, and the service can no longer read
its own config. Two safe forms:

```bash
sudo -u kian <editor or script> /srv/kian/kian.yaml   # edit as the service user
# or, after any root-side write:
/usr/local/bin/kian-config-guard                       # chown kian:kian, 0640, verifies readability
```

The unit additionally runs the guard as `ExecStartPre=+…` on **every** start, so a missed chown is
repaired at the next restart instead of bricking the service, and the watchdog reports an
unreadable config within one probe.

## Watchdog

`kian-watch.timer` runs `kian-watch.service` every 30 s. The probe fails when the unit is not
active, `NRestarts` rose since the previous probe, `/srv/kian/kian.yaml` is not readable by `kian`,
or `kian workers --config … --json` does not answer. Three consecutive failures (≈90 s) write an
`ALERT` to the journal and `/var/log/kian-watch.log`; a single restart is absorbed by the streak.

The watchdog exists because `Restart=always` keeps a crash-looping unit out of `failed`, so
`OnFailure` alone never fires: on 2026-11-19 the service restarted 9,982 times over ~9 h with no
alert.

## Health and recovery

```bash
systemctl status kian
journalctl -u kian -n 50 --no-pager
/opt/kian/bin/kian workers --config /srv/kian/kian.yaml
ls -l /srv/kian/kian.yaml                 # expect kian:kian 0640
/usr/local/bin/kian-config-guard          # repair ownership/mode
systemctl reset-failed kian && systemctl start kian   # leave a crash loop
```

| Symptom | Cause / action |
|---|---|
| `refusing to start — cannot read config file … Permission denied` | ownership; run the guard |
| `register refused reason=VersionMismatch` | a worker on another KiandProto revision; upgrade it, no shim exists |
| workers `offline` while the service is active | worker-side tunnel (`ssh -N -L 7400:127.0.0.1:7400 vultr`) or registration refusals in `journalctl -u kian` |
| `queue row abandoned: re-drive bound exhausted` | expected for a command whose side effect is its own ack path (ADR-033); the command may have run |
| a delegated task failed/interrupted and `kian workers` shows it holding the worker | not permanent since ADR-035: the slot is reclaimed after `workers.release_after_ms` (default 5 min) or immediately with `kian workers release <id>`; an abnormally interrupted drain (crash/stall) now reaches an honest end first (ADR-041 — a stalled model call is bounded by `providers.<name>.idle_timeout_ms`, default 5 min). Only a `running` drain that is still making progress, or one that ended `awaiting_input` awaiting a person (CONFIRMATION-MODEL phase 2 — answer it to release), legitimately holds the slot; `kian workers release` refuses both by design |

## Deploy

Server (build on a Linux host with rustup, or on the box itself):

```bash
cd <source> && cargo build --release -p kian-cli -p kiand
install -m 0755 target/release/kian /opt/kian/bin/kian.new
mv /opt/kian/bin/kian.new /opt/kian/bin/kian     # keep a kian.bak-<ts> beside it
systemctl restart kian && systemctl is-active kian
```

Workers: replace `~/bin/kiand` and restart the worker — mac-mini runs launchd
`local.kian.worker` (`launchctl kickstart -k gui/$(id -u)/local.kian.worker`), the vultr worker runs
its own unit. KiandProto is pinned: a worker on the wrong revision is refused with
`version_mismatch` and never half-works, so upgrade the server first, then the workers (or both
together when the protocol version is unchanged).

Deploying a build with [ADR-037](../design/decisions/ADR-037-endpoint-result-dedupe.md) turns on the
endpoint result record by default (`<home>/.kian/kiand/results.jsonl`, `--no-dedupe` opts out); the
startup log line `result record enabled` is the evidence it is active.

## Incident record: 2026-11-19

A worker-dispatched root command rewrote the config with `os.replace`, leaving it `root:root 0640`.
The next restart could not read it; systemd restarted the service every 3 s for ~9 h (9,982
attempts) and `OnFailure` never fired. On recovery the queue re-drove the `systemctl restart kian`
row that the outage had left `inflight` and executed it a second time. Fixes: this runbook's config
discipline, `kian-config-guard` and the watchdog (operations), and
[ADR-037](../design/decisions/ADR-037-endpoint-result-dedupe.md) (endpoint result retention plus the
re-drive grace).
