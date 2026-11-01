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

Stdlib only (urllib) so the demo has no third-party dependency.

Env:
  PORT          listen port (default 7461)
  UPSTREAM      upstream base url (default https://api.deepseek.com)
  KILL_ON       kill the Nth router-judge request (default 2: the first
                judge turn after the bootstrap batch)

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

_lock = threading.Lock()
_router_judges = 0


def is_router_judge(body: bytes) -> bool:
    try:
        parsed = json.loads(body)
        first = parsed["messages"][0]["content"]
        return isinstance(first, str) and first.startswith(
            "You are the Router of a super-agent"
        )
    except Exception:
        return False


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep the demo output clean
        pass

    def do_POST(self):
        global _router_judges
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

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

        req = urllib.request.Request(UPSTREAM + self.path, data=body, method="POST")
        for key, value in self.headers.items():
            if key.lower() not in ("host", "content-length", "accept-encoding",
                                   "connection"):
                req.add_header(key, value)
        upstream = urllib.request.urlopen(req, timeout=120)

        if not doomed:
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
    print(
        f"[flaky-proxy] listening on 127.0.0.1:{PORT} -> {UPSTREAM} "
        f"(kill router-judge #{KILL_ON})",
        flush=True,
    )
    ThreadingHTTPServer(("127.0.0.1", PORT), Proxy).serve_forever()
