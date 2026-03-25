"""Unit tests for writer error handling — no silent data loss."""
from __future__ import annotations

import orjson
import pytest
from pathlib import Path

from src.common.envelope import serialize_envelope, deserialize_envelope


class TestCorruptMessageHandling:
    def test_deserialize_corrupt_json_raises(self):
        """Confirm orjson raises on corrupt input."""
        with pytest.raises(orjson.JSONDecodeError):
            deserialize_envelope(b"not valid json {{{")

    def test_deserialize_truncated_message_raises(self):
        """Truncated message from Redpanda should raise."""
        valid = serialize_envelope({"type": "data", "stream": "trades"})
        truncated = valid[:10]
        with pytest.raises(orjson.JSONDecodeError):
            deserialize_envelope(truncated)


class TestErrorGapUsesDataTimestamp:
    def test_make_error_gap_received_at_matches_data_range(self):
        """_make_error_gap must set received_at to last_ts from the batch,
        NOT wall-clock time.  Wall-clock received_at pollutes checkpoints
        and causes inverted timestamps on recovery (buffer_overflow regression)."""
        from src.writer.consumer import WriterConsumer
        from src.writer.buffer_manager import FlushResult
        from src.writer.file_rotator import FileTarget

        data_ts = 1_700_000_000_000_000_000  # fixed nanosecond timestamp
        line = orjson.dumps({"received_at": data_ts})
        result = FlushResult(
            target=FileTarget("binance", "btcusdt", "trades", "2026-03-18", 10),
            file_path=Path("/data/test.jsonl.zst"),
            lines=[line],
            high_water_offset=100,
            partition=0,
            count=1,
        )
        gap = WriterConsumer._make_error_gap(result, "test error")
        assert gap["received_at"] == data_ts, (
            "received_at must come from data timestamps, not wall-clock"
        )
