# Runbook — iPhone worker (design + phase-1 contract)

- **Status**: phase-1 design; kian-side enrollment/catalog landed (ADR-031). The iOS app itself is
  **not built**, and there is no L3 evidence. Treat every device-side claim below as a hypothesis
  with the named verification step, not a tested result.
- **Scope**: how a Swift worker on iPhone connects to `kian serve` as a KiandProto v3 worker, how
  the operator enrolls it, and what kian guarantees while it is offline.
- **Owners**: [`design/WORKER-MODEL.md`](../design/WORKER-MODEL.md) (semantics), ADR-031
  (enrollment/catalog/queue bound), [`design/API-CONTRACT.md`](../design/API-CONTRACT.md) §8
  (frames).

## Repository placement: a separate repo, not `crates/`

The worker is a **peer implementation of a frozen wire contract**, not a kian crate.

| Concern | kian repo | separate `kian-ios-worker` repo |
|---|---|---|
| Build/CI | Rust workspace + four cargo gates | Xcode/Swift, signing, TestFlight |
| License | no app code; core stays as-is | an app embedding iSH is GPLv3 (iSH is GPLv3); the app's license is its own |
| Dependency | `kian-endpoint` is Rust; iOS cannot link it | Swift protocol codec against API-CONTRACT §8 |
| Release | server releases | App Store review has its own cadence |

**Decision**: a separate repo (`kian-ios-worker`), with a thin `WorkerKit` Swift package (the
JSON-lines frame codec + register/heartbeat/command loop) and the app target. kian keeps the
contract, the enrollment config and the runbook. The iOS repo vendors/pins the protocol fixtures
from a kian commit; a wire change is already required to update both halves' goldens, so the
fixture pin is the cross-repo gate.

If the only deliverable is the codec (no app), it may live under `contrib/ios-worker/`; an app does
not belong in the product repo.

## Architecture

```
kian serve (Rust)  ──TCP, JSON lines (KiandProto v3)──  Swift worker (iOS)
  registration listener (private + token)                  register / heartbeat / result
  durable queue by worker id                               exec → iSH Alpine / native commands
  list_workers(capabilities)                               fs   → workspace root
```

- **Wire**: TCP, one JSON object per line, `PROTOCOL_VERSION = 3`; `register` → `welcome`, then
  `command`/`result`, `fs_command`/`fs_result`, `ping`/`heartbeat`/`bye`
  ([API-CONTRACT](../design/API-CONTRACT.md) §8).
- **exec backend**: reuse an iSH/Alpine sandbox (the OpenMinis approach) for fork/exec; do not try
  to reimplement a shell. A native Swift worker owns the socket and forwards `argv` into the
  sandbox.
- **Capabilities**: the app's device commands (calendar, health, photos, …) are native handlers
  invoked through argv (the OpenMinis offload pattern). The worker declares them in the `register`
  frame (`capabilities`); kian sanitizes and caches the accepted list, and the Router sees it in
  `list_workers` results.
- **Background**: iOS suspends a backgrounded app; long-lived sockets die unless a keep-alive is
  active (audio/location/continued-processing). Do not promise 24/7. kian's offline-known binding
  + durable queue is the honest degradation: commands park and run when the phone returns.
- **Reachability**: the listener is private; use Tailscale/WireGuard (or an SSH tunnel). Never set
  `workers.allow_public`.

## kian-side setup

```yaml
workers:
  listen: "127.0.0.1:7400"
  enrolled_only: true           # only listed ids; forbids the shared token (ADR-031 D1)
  tokens:
    iphone: KIAN_IPHONE_TOKEN
  offline_after_ms: 60000       # the phone may miss heartbeats while suspended
  queue_timeout_ms: 600000      # global backend bound: every queued command may wait this long
```

```bash
export KIAN_IPHONE_TOKEN=...    # on the kian host; absent/blank refuses serve startup
kian serve --config kian.yaml
kian workers --config kian.yaml # board: state, cached capabilities, bound session
```

The Worker calls `register` with `capabilities: ["exec", "apple-calendar", "apple-health"]`; kian
sanitizes the list (lowercase `[a-z0-9._-]`, ≤16, unique), caches it on the registry, and serves it
through `list_workers` (`capabilities` is an additive array, omitted when empty) and `kian workers`.
`spawn_worker`'s schema and the judge prompt are unchanged: capability data is a **result**, never a
prefix byte.

## Worker side

- **Spike (fastest, no app code)**: run a static `aarch64-unknown-linux-musl` `kiand` inside the
  OpenMinis Alpine sandbox (iSH), `kiand --register-to <tailscale-addr> --endpoint-id iphone
  --token-env KIAN_IPHONE_TOKEN --workspace-root /var/minis/workspace`. Verifies the protocol,
  heartbeats, exec/fs and the offload commands end to end. OpenMinis' shell timeout/SIGKILL and
  background model make this a test harness, not the product.
- **Product**: a native Swift worker implements KiandProto v3 and forwards exec into iSH; the
  native handlers are the device commands. The worker validates its id (1..=200 bytes, no control
  characters) and refuses to boot on an invalid id.

## Verification matrix

| Claim | Level | Status |
|---|---|---|
| Wire frames, register/heartbeat/exec + fs (v3) | L1/L2 (Rust both halves, goldens, real TCP) | verified in kian |
| Per-worker token accept/refuse, strict enrollment, shared fallback | L1 over real loopback TCP + config validation | verified in kian |
| `register.capabilities` sanitization + live directory | L1/L2 | verified in kian |
| Cached capabilities on `list_workers` / `kian workers` | L1/L2 | verified in kian |
| Global queue-bound timeouts | L1 | verified in kian (unit/config) |
| Swift codec parity with the Rust frames | L1 (cross-repo fixture pin) | **not built** |
| iSH syscall coverage for a Rust/tokio worker | L3 (device) | **unverified** |
| Background suspension / keep-alive longevity | L3 (device) | **unverified** |
| Battery/performance under a long exec | L3 (device) | **unverified** |
| App Store review of a remote-command worker | process | **unverified** |

## Failure modes (by contract, not by hope)

- Phone offline mid-command → route detaches, row stays `inflight`, re-drives on re-registration;
  at-least-once with command-id dedupe. No failover to another worker, ever
  ([WORKER-MODEL](../design/WORKER-MODEL.md) §4).
- Phone gone forever → the honest `queue_timeout`; the bound session stays parked. There is no
  rebind/takeover in this phase.
- Wrong/absent token, or an unlisted id under `enrolled_only` → `bad_token`, one attempt per
  connection, per-peer rate bound, no id-enumeration detail.
- Capability drift (catalog says X, app cannot do X) → the command fails honestly on the worker;
  the catalog never grants authority.

## Non-goals (phase 1)

- mTLS / per-worker certificates — per-worker bearer tokens narrow the shared-token hole,
  they do not close it.
- Static-pin capabilities (`hello.capabilities`); capability-based spawn validation/enforcement.
- Per-id queue timeouts, rebind, failover, worker pools.
- Capability-based spawn validation or enforcement.
- Hosted/multi-tenant security, App Store distribution guarantees.
