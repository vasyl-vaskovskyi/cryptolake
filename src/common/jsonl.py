"""Crash-resilient JSONL file I/O utilities.

Used by both the host lifecycle agent (``scripts/``) and the writer's
host lifecycle reader (``src/writer/``).  Placing these helpers in
``src/common/`` ensures the dependency direction is correct:
application code and scripts both import from ``src/common/``.
"""
from __future__ import annotations

import json
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    """Read all valid JSONL records from *path*, discarding bad lines.

    Any line that is not valid JSON is silently dropped.  This makes the
    reader resilient to partial writes caused by a crash mid-write.

    Returns an empty list if the file does not exist.
    """
    if not path.exists():
        return []

    events: list[dict] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except (json.JSONDecodeError, ValueError):
                continue
    return events


def append_jsonl(path: Path, record: dict) -> None:
    """Append a single JSON record to *path*.

    Uses a single ``write()`` syscall per record (``line + "\\n"``) to
    minimise the window for partial writes on crash.  The parent directory
    is created if it does not yet exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    with open(path, "a") as f:
        f.write(line)
