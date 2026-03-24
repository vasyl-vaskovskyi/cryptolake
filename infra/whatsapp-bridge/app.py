#!/usr/bin/env python3
"""Webhook bridge: AlertManager -> Callmebot WhatsApp."""
import json
import os
import time
import urllib.parse
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler

PHONE = os.environ["CALLMEBOT_PHONE"]
APIKEY = os.environ["CALLMEBOT_APIKEY"]
CALLMEBOT_URL = "https://api.callmebot.com/whatsapp.php"
# Callmebot rate-limits; AlertManager already batches via group_wait/group_interval
SEND_DELAY = 2  # seconds between messages in a batch


def send_whatsapp(text: str) -> None:
    params = urllib.parse.urlencode(
        {"phone": PHONE, "text": text, "apikey": APIKEY}
    )
    url = f"{CALLMEBOT_URL}?{params}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            print(f"callmebot status={resp.status}")
    except Exception as exc:
        print(f"callmebot error: {exc}")


def format_alert(alert: dict) -> str:
    status = alert.get("status", "unknown")
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    name = labels.get("alertname", "Unknown")
    severity = labels.get("severity", "")
    summary = annotations.get("summary", "")
    description = annotations.get("description", "")

    if status == "resolved":
        return f"RESOLVED: {name}\n{summary}"

    tag = "CRITICAL" if severity == "critical" else "WARNING"
    return f"{tag}: {name}\n{summary}\n{description}"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        alerts = body.get("alerts", [])

        for i, alert in enumerate(alerts):
            if i > 0:
                time.sleep(SEND_DELAY)
            send_whatsapp(format_alert(alert))

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_GET(self):  # noqa: N802 – health check
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        print(f"[whatsapp-bridge] {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9095), Handler)
    print(f"whatsapp-bridge listening on :9095 (phone={PHONE})")
    server.serve_forever()
