"""Fixture-capture tap.

Writes each inbound WebSocket frame verbatim to disk plus its resulting
envelope. Used by /port-init to build the parity fixture corpus.

No-op when output_dir is None.
"""
from __future__ import annotations

import itertools
import json
import time
from pathlib import Path
from typing import Mapping


class FrameTap:
    """Persists raw frames + envelopes for later Java parity checks."""

    def __init__(self, output_dir: Path | None, stream: str) -> None:
        self._output_dir = output_dir
        self._stream = stream
        self._counter = itertools.count()
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, raw: bytes, envelope: Mapping[str, object]) -> None:
        if self._output_dir is None:
            return
        seq = next(self._counter)
        stem = f"{time.time_ns():020d}-{seq:06d}-{self._stream}"
        raw_path = self._output_dir / f"{stem}.raw"
        env_path = self._output_dir / f"{stem}.json"
        raw_path.write_bytes(raw)
        env_path.write_text(json.dumps(dict(envelope)))
