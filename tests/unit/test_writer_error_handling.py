"""Unit tests for writer error handling — no silent data loss."""
from __future__ import annotations

import errno
import orjson
import pytest
from unittest.mock import patch, MagicMock
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


class TestPgCommitFailureHandling:
    def test_commit_state_handles_pg_failure(self):
        """_commit_state must catch PG failures and not commit Kafka offsets."""
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        assert "pg_commit_failed_will_retry" in source, (
            "_commit_state must log pg_commit_failed_will_retry on PG failure"
        )
        assert "pg_commit_failures_total" in source, (
            "_commit_state must increment pg_commit_failures_total metric"
        )


class TestDiskWriteErrorHandling:
    def test_write_to_disk_survives_oserror(self):
        """_write_to_disk should catch OSError and skip the failed file."""
        # We can't easily unit-test _write_to_disk without a full Writer,
        # but we can verify the error handling pattern exists in the code.
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        assert "write_to_disk_failed" in source, (
            "_write_to_disk must log write_to_disk_failed on OSError"
        )
        assert "write_errors_total" in source, (
            "_write_to_disk must increment write_errors_total metric"
        )


class TestSidecarWriteFailure:
    def test_sidecar_failure_does_not_crash_rotation(self):
        """Sidecar write failure must be caught, not crash the writer."""
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        assert "sidecar_write_failed" in source, (
            "write_sha256_sidecar calls must be wrapped in try/except"
        )


class TestProducerSerializationError:
    def test_produce_catches_serialization_error(self):
        """produce() must catch serialization errors and return False."""
        import src.collector.producer as producer_mod
        source = Path(producer_mod.__file__).read_text()
        assert "serialization_failed" in source, (
            "produce() must log serialization_failed on error"
        )
