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


def _parse_iso(ts: str) -> str:
    """Convert ISO-8601 timestamp to compact human-readable form."""
    if not ts or ts.startswith("0001"):
        return ""
    # Strip sub-second and timezone for brevity: "2025-03-25T14:02:33.123Z" → "14:02:33 UTC"
    try:
        # Handle both "Z" and "+00:00" suffixes
        clean = ts.replace("Z", "+00:00")
        from datetime import datetime, timezone

        dt = datetime.fromisoformat(clean).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ts[:19]


def _duration_str(start: str, end: str) -> str:
    """Return human-readable duration between two ISO timestamps."""
    if not start or not end or end.startswith("0001"):
        return ""
    try:
        from datetime import datetime

        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        delta = e - s
        secs = int(delta.total_seconds())
        if secs < 0:
            return ""
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m {secs % 60}s"
    except Exception:
        return ""


def format_alert(alert: dict) -> str:
    status = alert.get("status", "unknown")
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    name = labels.get("alertname", "Unknown")
    severity = labels.get("severity", "")
    summary = annotations.get("summary", "")
    description = annotations.get("description", "")

    # Extract gap-specific labels if present
    symbol = labels.get("symbol", "")
    stream = labels.get("stream", "")
    reason = labels.get("reason", "")
    exchange = labels.get("exchange", "")

    # Timing and identity fields from AlertManager payload
    starts_at = alert.get("startsAt", "")
    ends_at = alert.get("endsAt", "")
    fingerprint = alert.get("fingerprint", "")
    instance = labels.get("instance", "")
    job = labels.get("job", "")

    lines = []

    if status == "resolved":
        lines.append(f"RESOLVED: {name}")
        if symbol:
            lines.append(f"Pair: {exchange}/{symbol}/{stream}")
        if reason:
            lines.append(f"Reason: {reason}")
        duration = _duration_str(starts_at, ends_at)
        if duration:
            lines.append(f"Duration: {duration}")
        fired = _parse_iso(starts_at)
        resolved = _parse_iso(ends_at)
        if fired and resolved:
            lines.append(f"Fired: {fired} -> Resolved: {resolved}")
        lines.append(summary)
    else:
        tag = "CRITICAL" if severity == "critical" else "WARNING"
        lines.append(f"{tag}: {name}")
        if symbol:
            lines.append(f"Pair: {exchange}/{symbol}/{stream}")
        if reason:
            lines.append(f"Reason: {reason}")
        fired = _parse_iso(starts_at)
        if fired:
            lines.append(f"Since: {fired}")
        if instance or job:
            source = "/".join(filter(None, [job, instance]))
            lines.append(f"Source: {source}")
        lines.append(summary)
        if description and description != summary:
            lines.append(description)

    if fingerprint:
        lines.append(f"[{fingerprint}]")

    return "\n".join(lines)


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
