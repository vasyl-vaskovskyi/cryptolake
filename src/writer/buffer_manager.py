from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path

import orjson
import structlog

from src.writer.file_rotator import FileTarget, build_file_path

logger = structlog.get_logger()


@dataclass
class FlushResult:
    target: FileTarget
    file_path: Path
    lines: list[bytes]
    high_water_offset: int
    partition: int
    count: int


class BufferManager:
    """Routes incoming envelopes to per-file buffers and triggers flushes."""

    def __init__(self, base_dir: str, flush_messages: int = 10_000,
                 flush_interval_seconds: int = 30):
        self.base_dir = base_dir
        self.flush_messages = flush_messages
        self.flush_interval_seconds = flush_interval_seconds
        self._buffers: dict[tuple, list[dict]] = {}
        self.total_buffered = 0

    def add(self, envelope: dict) -> list[FlushResult] | None:
        """Add an envelope to the appropriate buffer. Returns flush results if threshold hit."""
        target = self._route(envelope)
        key = target.key
        if key not in self._buffers:
            self._buffers[key] = []
        self._buffers[key].append(envelope)
        self.total_buffered += 1

        if len(self._buffers[key]) >= self.flush_messages:
            return self._flush_buffer(key, target)
        return None

    def flush_key(self, file_key: tuple[str, str, str]) -> list[FlushResult]:
        """Flush buffers matching a (exchange, symbol, stream) prefix."""
        exchange, symbol, stream = file_key
        results: list[FlushResult] = []
        for key in list(self._buffers.keys()):
            if key[0] == exchange and key[1] == symbol and key[2] == stream:
                if self._buffers[key]:
                    target = FileTarget(*key)
                    results.extend(self._flush_buffer(key, target))
        return results

    def flush_all(self) -> list[FlushResult]:
        """Flush all non-empty buffers."""
        results: list[FlushResult] = []
        for key in list(self._buffers.keys()):
            if self._buffers[key]:
                target = FileTarget(*key)
                results.extend(self._flush_buffer(key, target))
        return results

    def _flush_buffer(self, key: tuple, target: FileTarget) -> list[FlushResult]:
        messages = self._buffers.pop(key, [])
        if not messages:
            return []
        self.total_buffered -= len(messages)

        lines = [orjson.dumps(env) + b"\n" for env in messages]
        high_water = max(m["_offset"] for m in messages if m["_offset"] >= 0)
        partition = messages[0]["_partition"]  # all messages in same buffer share partition
        file_path = build_file_path(
            self.base_dir, target.exchange, target.symbol,
            target.stream, target.date, target.hour,
        )
        return [FlushResult(
            target=target,
            file_path=file_path,
            lines=lines,
            high_water_offset=high_water,
            partition=partition,
            count=len(messages),
        )]

    def _route(self, envelope: dict) -> FileTarget:
        received_at_ns = envelope["received_at"]
        dt = datetime.datetime.fromtimestamp(
            received_at_ns / 1_000_000_000, tz=datetime.timezone.utc
        )
        return FileTarget(
            exchange=envelope["exchange"],
            symbol=envelope["symbol"],
            stream=envelope["stream"],
            date=dt.strftime("%Y-%m-%d"),
            hour=dt.hour,
        )
