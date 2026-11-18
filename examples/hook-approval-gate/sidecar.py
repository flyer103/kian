#!/usr/bin/env python3
"""The kian reference sidecar: a `tool/pre` approval gate.

An in-tree, executable reference for PLUGIN-SPEC §2's language-neutrality
paragraph ("a sidecar may be written in any language that can read and
write newline-delimited UTF-8 JSON on stdio") and the smallest honest
demonstration of the full v1 contract:

  * the §7.3 handshake — answer `hello` with `ready`, restating the
    identity (manifest path, plugin name/version, abi) exactly and the
    hooks/capabilities sets by set-equality;
  * the §3.3/§7.4 reply discipline — `bail` verdicts for `tool/pre`,
    `ack` for observe-only hooks, one reply per dispatch id;
  * a pure verdict function (§8.6): the decision depends only on the
    frame's own bytes and the process's startup configuration — never on
    mutable process state — so a replayed drain re-derives the same
    verdict;
  * deadlines honoured by design (the loop answers immediately; the
    server's watchdog owns the kill), plus a `sleep` failure-injection
    mode that lets an operator or test prove §11.1's fail-closed class
    default for a wedged `veto:tool` holder.

The approval rule is deliberately simple: deny the side-effecting
`fs_write` tool unless the frame's target session appears in the
`APPROVAL_GATE_SESSIONS` allowlist; abstain (`bail: null`) on everything
else, leaving the declarative permission gate's decision standing. There
is no `allow` verdict — absence of a denial is the strongest statement a
sidecar can make (§3.3 "No allow-as-opinion").

Environment (the manifest's `env_allow` is the only surface — the server
clears everything else):

  APPROVAL_GATE_SESSIONS  comma-separated session uuids allowed to
                          fs_write (default: none — every fs_write is
                          denied)
  APPROVAL_GATE_FRAMES    when set, every received frame line is
                          appended to this path (hello and hooks; the
                          operator's observable ABI trail)
  APPROVAL_GATE_MODE      `sleep` = wedge on every hook frame, missing
                          every deadline (failure injection for the
                          fail-closed path; any other value is normal
                          operation)

Python 3, standard library only.
"""

import json
import os
import sys
import time

#: The tool this gate is opinionated about (the side-effecting one).
GATED_TOOL = "fs_write"

#: Failure-injection knob value: park on every hook frame.
_MODE_SLEEP = "sleep"

#: How long the sleep mode parks per frame — far beyond any legal
#: deadline_ms (the server kills the group; this process never returns).
_SLEEP_SECONDS = 3600


def allowlist():
    """The approved session ids, as a set (pure function of the env)."""
    raw = os.environ.get("APPROVAL_GATE_SESSIONS", "")
    return {entry.strip() for entry in raw.split(",") if entry.strip()}


def verdict(frame, approved):
    """The §8.6-pure verdict for one `tool/pre` frame.

    Returns `(True, reason)` to deny, or `(False, None)` to abstain.
    Depends only on the frame's own bytes and the startup allowlist —
    identical `(hook, payload)` always yields the identical reply, which
    is what makes a re-driven drain deterministic.
    """
    payload = frame.get("payload") or {}
    if payload.get("name") != GATED_TOOL:
        # Not the gated tool: no opinion, the declarative gate stands.
        return (False, None)
    session = frame.get("session")
    if session in approved:
        # Approved session: still no `allow` — abstain and let the
        # declarative permission table decide (§3.3).
        return (False, None)
    reason = (
        "{tool} requires approval: session {session} is not in "
        "APPROVAL_GATE_SESSIONS"
    ).format(tool=GATED_TOOL, session=session)
    return (True, reason[:512])


def reply_for(frame, approved):
    """Builds the legal reply frame (§7.4) for one dispatch."""
    dispatch_id = frame.get("id", "")
    mode = frame.get("mode")
    if mode == "bail":
        deny, reason = verdict(frame, approved)
        if deny:
            return {
                "id": dispatch_id,
                "bail": {"decision": "deny", "reason": reason},
            }
        return {"id": dispatch_id, "bail": None}
    # Every other hook of this plugin's subscription set is observe-only
    # (`emit` mode): acknowledge.
    return {"id": dispatch_id, "ack": True}


def ready_from_hello(hello):
    """§7.3 step 2: restate the identity exactly, sets by equality.

    The reply carries the hello's own values back — the manifest path,
    plugin name/version and abi verbatim, and the hooks/capabilities
    sets (order-insensitive; sorted here for a deterministic line).
    """
    return {
        "v": 1,
        "manifest": hello["manifest"],
        "plugin": {
            "name": hello["plugin"]["name"],
            "version": hello["plugin"]["version"],
        },
        "abi": hello["abi"],
        "hooks": sorted(hello["hooks"]),
        "capabilities": sorted(hello["capabilities"]),
    }


class FramesLog:
    """Appends every received frame line to APPROVAL_GATE_FRAMES."""

    def __init__(self):
        self.path = os.environ.get("APPROVAL_GATE_FRAMES")

    def write(self, line):
        if not self.path:
            return
        try:
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(line.rstrip("\n") + "\n")
        except OSError:
            # Observability only: a broken log never changes verdicts
            # (§8.6 purity — the reply depends on the frame and env).
            pass


def emit(obj, log=None):
    """Writes one compact NDJSON line to stdout and flushes it."""
    line = json.dumps(obj, separators=(",", ":"))
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    if log is not None:
        log.write("reply: " + line)


def main():
    approved = allowlist()
    log = FramesLog()
    sleep_mode = os.environ.get("APPROVAL_GATE_MODE") == _MODE_SLEEP

    hello_line = sys.stdin.readline()
    if not hello_line:
        return 0
    hello = json.loads(hello_line)
    log.write(hello_line)
    # §7.3: the handshake answers before anything else; no dispatch is
    # ever answered before ready.
    emit(ready_from_hello(hello), log)

    while True:
        line = sys.stdin.readline()
        if not line:
            # Stdin closed: clean exit (the server half-closes after bye).
            return 0
        stripped = line.strip()
        if not stripped:
            continue
        if stripped == "{}":
            # §7.5 bye: exit cleanly within the grace period.
            return 0
        try:
            frame = json.loads(stripped)
            log.write(stripped)
            if sleep_mode:
                # Wedged on purpose: the deadline elapses, the watchdog
                # kills the group, and the bus applies §11.1's fail-closed
                # class default for this veto:tool holder.
                time.sleep(_SLEEP_SECONDS)
                continue
            reply = reply_for(frame, approved)
        except Exception as exc:  # noqa: BLE001 - one honest error report
            # An exception here would be a silent crash to the server
            # ("hook unavailable (crash)") — record it in the operator's
            # frames log so the defect is diagnosable, then re-raise.
            import traceback

            log.write("sidecar exception: " + traceback.format_exc())
            raise
        emit(reply, log)


if __name__ == "__main__":
    sys.exit(main())
