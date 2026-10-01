"""Local webhook receiver for the Compose demo."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock


events: dict[str, dict] = {}
lock = Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/events":
            self.send_error(404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        if size > 8192:
            self.send_error(413)
            return
        try:
            payload = json.loads(self.rfile.read(size))
        except (ValueError, UnicodeDecodeError):
            self.send_error(400)
            return
        key = self.headers.get("Idempotency-Key", payload.get("id", ""))
        with lock:
            events[key] = payload
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        if self.path != "/events":
            self.send_error(404)
            return
        with lock:
            body = json.dumps(list(events.values())).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8001), Handler).serve_forever()
