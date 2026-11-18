# The `approval-gate` reference sidecar (kian hook plugin, tier 1)

This directory is kian's in-tree **reference sidecar**: one complete, honest
implementation of the hook-plugin contract ([design/PLUGIN-SPEC.md](../../design/PLUGIN-SPEC.md) v2.2,
[ADR-028](../../design/decisions/ADR-028-harness-hook-plugins.md)) written in **Python 3, standard
library only** — which is the point. PLUGIN-SPEC §2 makes the ABI a *process*
boundary, not a language boundary ("a sidecar may be written in any language
that can read and write newline-delimited UTF-8 JSON on stdio"); a Python
script under `examples/` demonstrates that claim with zero build cost and zero
workspace members, exactly the shape an operator's own plugin would have:
`argv[0]` is the interpreter, the script is `argv[1]`.

## What it does

A `tool/pre` **approval gate** with the simplest honest rule:

> Deny the side-effecting `fs_write` tool unless the frame's target session
> appears in the `APPROVAL_GATE_SESSIONS` allowlist; abstain (`bail: null`) on
> everything else.

There is no `allow` verdict anywhere — absence of a denial is the strongest
statement a sidecar can make (PLUGIN-SPEC §3.3, "No allow-as-opinion"); an
abstain leaves the declarative `permissions:` table's decision standing. The
verdict function is **pure** (§8.6): it reads only the frame's own bytes and
the startup environment, so a replayed drain re-derives the identical reply.

It exercises the full v1 contract surface:

| Contract clause | Where |
|---|---|
| §7.3 handshake | answers `hello` with `ready`, restating the identity (manifest path, plugin name/version, abi) exactly and the hooks/capabilities sets by set-equality |
| §7.4 reply legality | `bail` verdicts for `tool/pre` (mode `bail`), `ack` for observe-only hooks, one reply per dispatch `id` |
| §7.5 graceful stop | exits cleanly on the `bye` frame (`{}`) |
| §8.6 purity | `verdict()` depends only on the frame and the startup env |
| §11.1 fail-closed | the `sleep` mode (below) lets you *watch* a wedged `veto:tool` holder deny honestly |

## Environment

The manifest's `env_allow` is the sidecar's entire environment (the server
clears everything else — deny by default):

| Variable | Meaning |
|---|---|
| `APPROVAL_GATE_SESSIONS` | comma-separated session uuids allowed to `fs_write` (default: none — every `fs_write` is denied) |
| `APPROVAL_GATE_FRAMES` | when set, every received frame (and each reply) is appended to this path — the operator's observable ABI trail |
| `APPROVAL_GATE_MODE` | `sleep` = wedge on every hook frame and miss every deadline (failure injection, for demonstrating §11.1's fail-closed class default); any other value is normal operation |

## Files

- `sidecar.py` — the sidecar (Python 3, stdlib only)
- `manifest.toml` — a sample manifest (see the digest note below)
- `kian.yaml` — a runnable `sidecars:` config snippet

## Running it

1. Compute the digest of the interpreter (the manifest's `sha256` pins
   `argv[0]`, PLUGIN-SPEC §6.2 rule 6):

   ```sh
   shasum -a 256 "$(command -v python3)"
   ```

2. Put your interpreter's absolute path and that digest into `manifest.toml`
   (the sample ships with placeholders — a manifest with a wrong digest is a
   `digest_mismatch` refusal at boot, by design).

3. Extend your `kian.yaml` with the `sidecars:` section from `kian.yaml` in
   this directory, restart `kian serve`, and watch the `kian::hook` audit
   sink: `sidecars mounted`, then `hook-set` on each session's first drain.

4. Ask the model to write a file. An unapproved session gets the honest
   error — `{"error":"denied by plugin approval-gate: fs_write requires
   approval: session <uuid> is not in APPROVAL_GATE_SESSIONS","policy":"hook"}`
   — as a `tool/result` the model can see and react to; the attempted
   `tool/call` is still logged with `permission.policy = "hook"` naming
   `approval-gate:tool/pre` (PLUGIN-SPEC §4.4).

The end-to-end proof lives in
[crates/kian-cli/tests/hook_sidecar_e2e.rs](../../crates/kian-cli/tests/hook_sidecar_e2e.rs),
which spawns this exact script through a real `Server::from_config_file`.
