"""Host lifecycle event ledger agent.

Runs on the host (outside Docker) and records lifecycle facts to an
append-only JSONL ledger at ``/data/.cryptolake/lifecycle/events.jsonl``.

The ledger is read by the writer on recovery to upgrade restart classification
from generic ``host|system`` to specific component causes.

**Deployment requirements:**

- Read access to ``/var/run/docker.sock`` (for Docker event subscription).
  Typically run as a systemd service with supplementary ``docker`` group.
- The ledger directory must be on a host-mounted volume (not inside a
  container's ephemeral filesystem) so it survives container recreation.

**Ledger format:** JSONL (one JSON object per line).  A crash mid-write
corrupts at most the last line, which the reader discards.  Each record
carries ``ts`` (ISO 8601) and ``event_type`` fields.

**Pruning:** Entries older than 7 days are removed on agent startup.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.jsonl import append_jsonl, read_jsonl
from src.common.system_identity import get_host_boot_id

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LEDGER_PATH = Path("/data/.cryptolake/lifecycle/events.jsonl")

TRACKED_CONTAINERS: frozenset[str] = frozenset({
    "collector",
    "writer",
    "redpanda",
    "postgres",
})

TRACKED_ACTIONS: frozenset[str] = frozenset({
    "start",
    "stop",
    "die",
})

PRUNE_MAX_AGE = timedelta(days=7)

# Exit code 137 = 128 + SIGKILL(9), typically indicates OOM kill
_OOM_EXIT_CODE = 137


# ---------------------------------------------------------------------------
# Ledger I/O — delegates to src.common.jsonl
# ---------------------------------------------------------------------------


def read_ledger_events(ledger_path: Path) -> list[dict]:
    """Read all valid JSONL events from the ledger, discarding bad lines."""
    return read_jsonl(ledger_path)


def append_event(ledger_path: Path, event: dict) -> None:
    """Append a single event record to the JSONL ledger."""
    append_jsonl(ledger_path, event)


# ---------------------------------------------------------------------------
# Pruning
# ---------------------------------------------------------------------------


def prune_ledger(ledger_path: Path, max_age: timedelta | None = None) -> None:
    """Remove events older than *max_age* (default 7 days) from the ledger.

    Events with unparseable timestamps are also removed.
    The file is rewritten atomically (write-tmp + rename).
    """
    if not ledger_path.exists():
        return

    if max_age is None:
        max_age = PRUNE_MAX_AGE

    cutoff = datetime.now(timezone.utc) - max_age
    events = read_ledger_events(ledger_path)

    kept: list[dict] = []
    for evt in events:
        ts_str = evt.get("ts", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                kept.append(evt)
        except (ValueError, TypeError):
            # Unparseable timestamp — treat as old, discard
            continue

    # Rewrite the file with only kept events
    tmp_path = ledger_path.with_suffix(".jsonl.tmp")
    with open(tmp_path, "w") as f:
        for evt in kept:
            f.write(json.dumps(evt, separators=(",", ":")) + "\n")
    tmp_path.rename(ledger_path)


# ---------------------------------------------------------------------------
# Event recording helpers
# ---------------------------------------------------------------------------


def record_boot_id(ledger_path: Path) -> None:
    """Write a ``boot_id`` event with the current host boot ID."""
    boot_id = get_host_boot_id()
    event = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "boot_id",
        "boot_id": boot_id,
    }
    append_event(ledger_path, event)


def record_maintenance_intent(
    ledger_path: Path,
    reason: str,
    source: str = "cli",
) -> None:
    """Write a ``maintenance_intent`` event to the ledger.

    Parameters
    ----------
    ledger_path:
        Path to the JSONL ledger file.
    reason:
        Human-readable reason for the maintenance window.
    source:
        Origin of the intent (``"cli"`` or ``"pg"``).
    """
    event = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "maintenance_intent",
        "reason": reason,
        "source": source,
    }
    append_event(ledger_path, event)


# ---------------------------------------------------------------------------
# Docker event parsing
# ---------------------------------------------------------------------------


def _match_tracked_container(name: str) -> str | None:
    """Return the canonical tracked container name if *name* matches.

    Docker Compose names containers like ``<project>-<service>-<replica>``,
    so we check whether the service segment matches exactly.  We also accept
    a bare service name (e.g. ``collector``) for non-Compose environments.

    Matching is strict: ``cryptolake-collector-1`` matches ``collector``,
    but ``my-redpanda-proxy`` does NOT match ``redpanda`` because
    ``redpanda`` is not a standalone hyphen-delimited segment followed
    by a replica number or end-of-string in the Compose naming convention.
    """
    # Exact match (bare name)
    if name in TRACKED_CONTAINERS:
        return name
    # Docker Compose: <project>-<service>-<replica>
    # The service name must appear as the second-to-last segment (before replica)
    # or as any segment when there are exactly 2 parts (project-service).
    parts = name.split("-")
    if len(parts) >= 2:
        # Try second-to-last segment (standard Compose: project-service-replica)
        if parts[-2] in TRACKED_CONTAINERS:
            return parts[-2]
        # Try last segment (project-service, no replica number)
        if parts[-1] in TRACKED_CONTAINERS:
            return parts[-1]
    return None


def parse_docker_event(raw: dict) -> dict | None:
    """Parse a raw Docker engine event into a ledger record.

    Returns ``None`` for events that should not be tracked (wrong type,
    action, or container name).
    """
    if raw.get("Type") != "container":
        return None

    action = raw.get("Action", "")
    if action not in TRACKED_ACTIONS:
        return None

    attrs = raw.get("Actor", {}).get("Attributes", {})
    container_name = attrs.get("name", "")

    tracked = _match_tracked_container(container_name)
    if tracked is None:
        return None

    ts_unix = raw.get("time", 0)
    ts = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()

    event: dict = {
        "ts": ts,
        "event_type": f"container_{action}",
        "container": tracked,
    }

    if action == "die":
        exit_code_str = attrs.get("exitCode", "")
        try:
            exit_code = int(exit_code_str)
        except (ValueError, TypeError):
            exit_code = -1
        event["exit_code"] = exit_code
        event["clean_exit"] = exit_code == 0
        event["oom_suspected"] = exit_code == _OOM_EXIT_CODE

    return event


# ---------------------------------------------------------------------------
# Agent startup
# ---------------------------------------------------------------------------


def agent_startup(ledger_path: Path) -> None:
    """Run the agent startup sequence: prune old events, record boot ID."""
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    prune_ledger(ledger_path)
    record_boot_id(ledger_path)


# ---------------------------------------------------------------------------
# Main loop (Docker event subscription)
# ---------------------------------------------------------------------------


def _run_event_loop(ledger_path: Path) -> None:  # pragma: no cover
    """Subscribe to Docker events via the Unix socket and persist them.

    This function blocks indefinitely, reading events from
    ``/var/run/docker.sock``.  It requires read access to the socket.

    **Privilege requirement:** The process must have read access to
    ``/var/run/docker.sock``.  Typically achieved by running the agent
    as a systemd service with supplementary ``docker`` group membership.
    """
    import http.client
    import socket

    class _DockerUnixConnection(http.client.HTTPConnection):
        """HTTP connection over a Unix domain socket."""

        def __init__(self, socket_path: str = "/var/run/docker.sock"):
            super().__init__("localhost")
            self._socket_path = socket_path

        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(self._socket_path)

    conn = _DockerUnixConnection()
    conn.request(
        "GET",
        "/events?filters="
        + json.dumps({"type": ["container"], "event": list(TRACKED_ACTIONS)}),
    )
    resp = conn.getresponse()

    # Docker streams events as newline-delimited JSON
    buf = b""
    while True:
        chunk = resp.read(4096)
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = parse_docker_event(raw)
            if event is not None:
                append_event(ledger_path, event)


def main() -> None:  # pragma: no cover
    """Entry point for the host lifecycle agent."""
    ledger_path = Path(os.environ.get("LIFECYCLE_LEDGER_PATH", str(DEFAULT_LEDGER_PATH)))

    print(f"Host lifecycle agent starting — ledger: {ledger_path}")
    agent_startup(ledger_path)
    print("Startup complete. Subscribing to Docker events...")

    try:
        _run_event_loop(ledger_path)
    except KeyboardInterrupt:
        print("Agent stopped.")
        sys.exit(0)
    except Exception as exc:
        print(f"Agent error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
