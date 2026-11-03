#!/usr/bin/env python3
"""flaky-proxy.py — a fault-injecting pass-through for the kian-62 demo.

Forwards openai-compat chat-completions traffic to a real upstream
(default https://api.deepseek.com) and, on the Nth *router judge*
request (a request whose first message starts with the Router judge
prompt), kills the response stream mid-flight: HTTP 200, SSE
content-type, a promised content-length larger than what will ever be
sent, a few bytes of real SSE — then the connection closes. The kian
provider surfaces an honest mid-turn stream error; the loop records
`turn/end{outcome:"soft_failure"}` and the session ends `failed`.
That is exactly the transient provider-stream failure the C60/C61
issue-watch runs observed in the wild.

A second, independent fault mode (kian-74) targets the *delivery
leg*: when FAIL_PATH is set, the first FAIL_N requests whose method
matches FAIL_METHOD and whose path contains FAIL_PATH are answered
with an injected HTTP 502 + a JSON `message` body (the shape the
github adapter surfaces in its honest send error) instead of being
forwarded — the C65 RUNLOG §7.1 scenario-6 evidence (bounded retry,
then park) becomes reproducible with the shipped tooling instead of a
scratch shim. Set FAIL_N high (e.g. 99) to fail the whole bounded
schedule (the park arm).

Stdlib only (urllib) so the demo has no third-party dependency.

Env:
  PORT          listen port (default 7461)
  UPSTREAM      upstream base url (default https://api.deepseek.com)
  KILL_ON       kill the Nth router-judge request (default 2: the first
                judge turn after the bootstrap batch)
  FAIL_PATH     delivery-leg fault mode: substring of the request path
                (unset = the mode is off), e.g. /comments
  FAIL_METHOD   the method the delivery-leg fault matches (default POST)
  FAIL_N        how many of the first matching requests to fail
                (default 2: attempts 1–2 fail, attempt 3 succeeds)

No request bodies or keys are logged (the Authorization header is
forwarded, never printed).
"""

import json
import os
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "7461"))
UPSTREAM = os.environ.get("UPSTREAM", "https://api.deepseek.com").rstrip("/")
KILL_ON = int(os.environ.get("KILL_ON", "2"))
FAIL_PATH = os.environ.get("FAIL_PATH", "")
FAIL_METHOD = os.environ.get("FAIL_METHOD", "POST").upper()
FAIL_N = int(os.environ.get("FAIL_N", "2"))

_lock = threading.Lock()
_router_judges = 0
_path_faults = 0


def is_router_judge(body: bytes) -> bool:
    try:
        parsed = json.loads(body)
        first = parsed["messages"][0]["content"]
        return isinstance(first, str) and first.startswith(
            "You are the Router of a super-agent"
        )
    except Exception:
        return False


def path_fault_armed() -> bool:
    return bool(FAIL_PATH)


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep the demo output clean
        pass

    # The delivery-leg fault (kian-74): True when this request is one
    # of the first FAIL_N method+path matches and must be answered
    # with an injected 502 instead of forwarded.
    def _claim_path_fault(self, method: str) -> bool:
        global _path_faults
        if not path_fault_armed() or method != FAIL_METHOD:
            return False
        if FAIL_PATH not in self.path:
            return False
        with _lock:
            _path_faults += 1
            nth = _path_faults
        doomed = nth <= FAIL_N
        print(
            f"[flaky-proxy] path fault match {method} {self.path} (#{nth}"
            + (f"/{FAIL_N}) — INJECTING 502" if doomed else ") — passing through"),
            flush=True,
        )
        return doomed

    def _inject_502(self) -> None:
        body = json.dumps(
            {"message": f"flaky-proxy injected 502 (delivery-leg fault, path={FAIL_PATH})"}
        ).encode()
        self.send_response(502)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # The delivery-leg fault is method-scoped; GETs pass through.
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        if self._claim_path_fault("GET"):
            self._inject_502()
            return
        self._forward("GET", body=b"")

    def do_POST(self):
        global _router_judges
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        if self._claim_path_fault("POST"):
            self._inject_502()
            return

        doomed = False
        if is_router_judge(body):
            with _lock:
                _router_judges += 1
                if _router_judges == KILL_ON:
                    doomed = True
            print(
                f"[flaky-proxy] router judge request #{_router_judges}"
                + (" — KILLING THE STREAM" if doomed else ""),
                flush=True,
            )

        self._forward("POST", body=body, judge_doomed=doomed)

    def _forward(self, method: str, body: bytes, judge_doomed: bool = False):
        req = urllib.request.Request(UPSTREAM + self.path, data=body or None, method=method)
        for key, value in self.headers.items():
            if key.lower() not in ("host", "content-length", "accept-encoding",
                                   "connection"):
                req.add_header(key, value)
        upstream = urllib.request.urlopen(req, timeout=120)

        if not judge_doomed:
            # Faithful pass-through (buffered: the demo needs no
            # streaming subtleties on the healthy path).
            payload = upstream.read()
            self.send_response(upstream.status)
            self.send_header(
                "content-type",
                upstream.headers.get("content-type", "text/event-stream"),
            )
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        # The doomed response: 200 + SSE content type + a content-length
        # we will not satisfy, a few real bytes, then an abrupt close.
        head = upstream.read(200)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("content-length", "1048576")
        self.end_headers()
        self.wfile.write(head)
        self.wfile.flush()
        self.close_connection = True
        try:
            self.connection.close()
        except Exception:
            pass
        print("[flaky-proxy] stream killed mid-flight", flush=True)


if __name__ == "__main__":
    modes = [f"kill router-judge #{KILL_ON}"]
    if path_fault_armed():
        modes.append(
            f"fail first {FAIL_N} {FAIL_METHOD} *{FAIL_PATH}* with 502"
        )
    print(
        f"[flaky-proxy] listening on 127.0.0.1:{PORT} -> {UPSTREAM} "
        f"({'; '.join(modes)})",
        flush=True,
    )
    ThreadingHTTPServer(("127.0.0.1", PORT), Proxy).serve_forever()
