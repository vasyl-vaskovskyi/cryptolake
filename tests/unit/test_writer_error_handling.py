"""Unit tests for writer error handling — no silent data loss."""
from __future__ import annotations

import orjson
import pytest

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
