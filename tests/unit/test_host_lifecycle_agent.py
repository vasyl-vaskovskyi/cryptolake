"""Unit tests for the host lifecycle event ledger agent."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_events(ledger_path: Path) -> list[dict]:
    """Read all valid JSONL events from a ledger file."""
    from src.common.jsonl import read_jsonl

    return read_jsonl(ledger_path)


def _make_event(event_type: str, ts: str | None = None, **extra) -> dict:
    """Build a minimal event dict."""
    return {
        "ts": ts or datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        **extra,
    }


# ===========================================================================
# Test: Ledger File I/O — write events, read back, verify format
# ===========================================================================


class TestLedgerWriteAndRead:
    """Writing events to the ledger and reading them back."""

    def test_write_single_event_creates_file(self, tmp_path: Path):
        """Writing a single event should create the ledger file."""
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "lifecycle" / "events.jsonl"
        event = _make_event("boot_id", boot_id="test-boot-123")
        append_jsonl(ledger, event)

        assert ledger.exists()

    def test_written_event_is_valid_json_line(self, tmp_path: Path):
        """Each line in the ledger must be a valid JSON object."""
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "events.jsonl"
        event = _make_event("boot_id", boot_id="abc")
        append_jsonl(ledger, event)

        lines = ledger.read_text().strip().splitlines()
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["event_type"] == "boot_id"
        assert parsed["boot_id"] == "abc"

    def test_multiple_events_appended(self, tmp_path: Path):
        """Multiple writes should produce multiple lines."""
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "events.jsonl"
        for i in range(5):
            append_jsonl(ledger, _make_event("test", seq=i))

        events = _read_events(ledger)
        assert len(events) == 5
        assert [e["seq"] for e in events] == [0, 1, 2, 3, 4]

    def test_each_event_has_ts_and_event_type(self, tmp_path: Path):
        """Every record must include 'ts' (ISO 8601) and 'event_type'."""
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "events.jsonl"
        now = datetime.now(timezone.utc).isoformat()
        append_jsonl(ledger, _make_event("container_start", ts=now, container="writer"))

        events = _read_events(ledger)
        assert len(events) == 1
        evt = events[0]
        assert "ts" in evt
        assert "event_type" in evt
        # ts should be parseable as ISO 8601
        datetime.fromisoformat(evt["ts"])

    def test_line_ends_with_newline(self, tmp_path: Path):
        """Each record must end with a newline for crash resilience."""
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "events.jsonl"
        append_jsonl(ledger, _make_event("boot_id"))

        raw = ledger.read_text()
        assert raw.endswith("\n")

    def test_read_empty_file_returns_empty_list(self, tmp_path: Path):
        """Reading an empty ledger should return an empty list."""
        ledger = tmp_path / "events.jsonl"
        ledger.write_text("")

        events = _read_events(ledger)
        assert events == []

    def test_read_nonexistent_file_returns_empty_list(self, tmp_path: Path):
        """Reading a missing ledger should return an empty list."""
        ledger = tmp_path / "events.jsonl"
        events = _read_events(ledger)
        assert events == []


# ===========================================================================
# Test: Partial last line (crash mid-write) discarded gracefully
# ===========================================================================


class TestPartialLineResilience:
    """Simulate crash mid-write — partial last line must be discarded."""

    def test_partial_last_line_discarded(self, tmp_path: Path):
        """A truncated final line (no closing brace) should be silently dropped."""
        ledger = tmp_path / "events.jsonl"
        good_event = json.dumps({"ts": "2025-01-01T00:00:00+00:00", "event_type": "boot_id"})
        # Write a good line + an incomplete line (simulating crash mid-write)
        ledger.write_text(good_event + "\n" + '{"ts": "2025-01-01T01:00:00+00:00", "event_typ')

        events = _read_events(ledger)
        assert len(events) == 1
        assert events[0]["event_type"] == "boot_id"

    def test_multiple_partial_lines_only_valid_kept(self, tmp_path: Path):
        """Only fully valid JSON lines should be kept."""
        ledger = tmp_path / "events.jsonl"
        lines = [
            json.dumps({"ts": "2025-01-01T00:00:00+00:00", "event_type": "a"}),
            "NOT_JSON_AT_ALL",
            json.dumps({"ts": "2025-01-01T00:00:00+00:00", "event_type": "b"}),
            '{"incomplete": true',
        ]
        ledger.write_text("\n".join(lines) + "\n")

        events = _read_events(ledger)
        # Only lines 0 and 2 are valid JSON
        assert len(events) == 2
        assert events[0]["event_type"] == "a"
        assert events[1]["event_type"] == "b"

    def test_empty_lines_skipped(self, tmp_path: Path):
        """Blank lines in the ledger should be silently skipped."""
        ledger = tmp_path / "events.jsonl"
        good = json.dumps({"ts": "2025-01-01T00:00:00+00:00", "event_type": "x"})
        ledger.write_text(good + "\n\n\n" + good + "\n")

        events = _read_events(ledger)
        assert len(events) == 2


# ===========================================================================
# Test: Boot ID recording
# ===========================================================================


class TestBootIdRecording:
    """The agent should record a boot_id event on startup."""

    def test_record_boot_id_event(self, tmp_path: Path):
        """record_boot_id should write a boot_id event with the current boot ID."""
        from scripts.host_lifecycle_agent import record_boot_id

        ledger = tmp_path / "events.jsonl"
        with patch.dict(os.environ, {"CRYPTOLAKE_TEST_BOOT_ID": "test-boot-42"}):
            record_boot_id(ledger)

        events = _read_events(ledger)
        assert len(events) == 1
        evt = events[0]
        assert evt["event_type"] == "boot_id"
        assert evt["boot_id"] == "test-boot-42"
        assert "ts" in evt

    def test_boot_id_uses_system_identity(self, tmp_path: Path):
        """Should use get_host_boot_id() from system_identity."""
        from scripts.host_lifecycle_agent import record_boot_id

        ledger = tmp_path / "events.jsonl"
        with patch("scripts.host_lifecycle_agent.get_host_boot_id", return_value="mocked-boot-id"):
            record_boot_id(ledger)

        events = _read_events(ledger)
        assert events[0]["boot_id"] == "mocked-boot-id"


# ===========================================================================
# Test: Pruning logic — entries older than 7 days removed
# ===========================================================================


class TestPruning:
    """Ledger pruning removes events older than 7 days."""

    def test_prune_removes_old_events(self, tmp_path: Path):
        """Events older than 7 days should be removed."""
        from src.common.jsonl import append_jsonl
        from scripts.host_lifecycle_agent import prune_ledger

        ledger = tmp_path / "events.jsonl"

        old_ts = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        recent_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        append_jsonl(ledger, _make_event("old_event", ts=old_ts))
        append_jsonl(ledger, _make_event("recent_event", ts=recent_ts))

        prune_ledger(ledger)

        events = _read_events(ledger)
        assert len(events) == 1
        assert events[0]["event_type"] == "recent_event"

    def test_prune_keeps_all_recent_events(self, tmp_path: Path):
        """Events within the last 7 days should all be kept."""
        from src.common.jsonl import append_jsonl
        from scripts.host_lifecycle_agent import prune_ledger

        ledger = tmp_path / "events.jsonl"

        for i in range(5):
            ts = (datetime.now(timezone.utc) - timedelta(days=i)).isoformat()
            append_jsonl(ledger, _make_event("event", ts=ts, day=i))

        prune_ledger(ledger)

        events = _read_events(ledger)
        assert len(events) == 5

    def test_prune_removes_all_old_events(self, tmp_path: Path):
        """If all events are older than 7 days, ledger should be empty after pruning."""
        from src.common.jsonl import append_jsonl
        from scripts.host_lifecycle_agent import prune_ledger

        ledger = tmp_path / "events.jsonl"

        for i in range(3):
            ts = (datetime.now(timezone.utc) - timedelta(days=10 + i)).isoformat()
            append_jsonl(ledger, _make_event("old", ts=ts))

        prune_ledger(ledger)

        events = _read_events(ledger)
        assert len(events) == 0

    def test_prune_on_missing_file_is_noop(self, tmp_path: Path):
        """Pruning a non-existent ledger should not raise."""
        from scripts.host_lifecycle_agent import prune_ledger

        ledger = tmp_path / "events.jsonl"
        prune_ledger(ledger)  # Should not raise

    def test_prune_handles_events_with_bad_timestamps(self, tmp_path: Path):
        """Events with unparseable timestamps should be pruned (treated as old)."""
        from scripts.host_lifecycle_agent import prune_ledger

        ledger = tmp_path / "events.jsonl"
        good_ts = datetime.now(timezone.utc).isoformat()
        lines = [
            json.dumps({"ts": "NOT_A_DATE", "event_type": "bad"}),
            json.dumps({"ts": good_ts, "event_type": "good"}),
        ]
        ledger.write_text("\n".join(lines) + "\n")

        prune_ledger(ledger)

        events = _read_events(ledger)
        assert len(events) == 1
        assert events[0]["event_type"] == "good"


# ===========================================================================
# Test: Docker event parsing
# ===========================================================================


class TestDockerEventParsing:
    """Test parsing of Docker engine events into ledger records."""

    def test_parse_container_start_event(self):
        """A container start event should map to a container_start record."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "start",
            "Actor": {
                "Attributes": {
                    "name": "collector",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["event_type"] == "container_start"
        assert result["container"] == "collector"
        assert "ts" in result

    def test_parse_container_stop_event(self):
        """A container stop event should map to a container_stop record."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "stop",
            "Actor": {
                "Attributes": {
                    "name": "writer",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["event_type"] == "container_stop"
        assert result["container"] == "writer"

    def test_parse_container_die_event_with_exit_code(self):
        """A container die event should include the exit code."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "die",
            "Actor": {
                "Attributes": {
                    "name": "redpanda",
                    "exitCode": "137",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["event_type"] == "container_die"
        assert result["container"] == "redpanda"
        assert result["exit_code"] == 137

    def test_die_exit_code_zero_means_clean(self):
        """Exit code 0 means a clean shutdown."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "die",
            "Actor": {
                "Attributes": {
                    "name": "postgres",
                    "exitCode": "0",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["exit_code"] == 0
        assert result["clean_exit"] is True

    def test_die_exit_code_nonzero_means_unexpected(self):
        """Non-zero exit code means an unexpected exit."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "die",
            "Actor": {
                "Attributes": {
                    "name": "collector",
                    "exitCode": "1",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["exit_code"] == 1
        assert result["clean_exit"] is False

    def test_die_exit_code_137_is_oom_or_sigkill(self):
        """Exit code 137 (128+9 = SIGKILL) is typically OOM kill."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "die",
            "Actor": {
                "Attributes": {
                    "name": "postgres",
                    "exitCode": "137",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["exit_code"] == 137
        assert result["clean_exit"] is False
        assert result["oom_suspected"] is True

    def test_ignores_non_tracked_containers(self):
        """Events from containers not in the tracked set should be ignored."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "start",
            "Actor": {
                "Attributes": {
                    "name": "some-random-container",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is None

    def test_ignores_non_container_events(self):
        """Non-container events (image, network, etc.) should be ignored."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "image",
            "Action": "pull",
            "Actor": {
                "Attributes": {
                    "name": "some-image",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is None

    def test_ignores_irrelevant_actions(self):
        """Only start/stop/die actions should be tracked."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "create",
            "Actor": {
                "Attributes": {
                    "name": "collector",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is None

    def test_container_name_prefix_matching(self):
        """Container names like 'cryptolake-collector-1' should match 'collector'."""
        from scripts.host_lifecycle_agent import parse_docker_event

        raw = {
            "Type": "container",
            "Action": "start",
            "Actor": {
                "Attributes": {
                    "name": "cryptolake-collector-1",
                },
            },
            "time": 1700000000,
        }

        result = parse_docker_event(raw)
        assert result is not None
        assert result["container"] == "collector"


# ===========================================================================
# Test: Maintenance intent recording
# ===========================================================================


class TestMaintenanceIntent:
    """Test recording maintenance intents into the ledger."""

    def test_record_maintenance_intent(self, tmp_path: Path):
        """Should write a maintenance_intent event."""
        from scripts.host_lifecycle_agent import record_maintenance_intent

        ledger = tmp_path / "events.jsonl"
        record_maintenance_intent(ledger, reason="scheduled upgrade", source="cli")

        events = _read_events(ledger)
        assert len(events) == 1
        evt = events[0]
        assert evt["event_type"] == "maintenance_intent"
        assert evt["reason"] == "scheduled upgrade"
        assert evt["source"] == "cli"
        assert "ts" in evt


# ===========================================================================
# Test: Agent startup sequence
# ===========================================================================


class TestAgentStartup:
    """Test the agent startup sequence (boot ID + prune)."""

    def test_startup_records_boot_id_and_prunes(self, tmp_path: Path):
        """On startup, agent should record boot ID and prune old events."""
        from scripts.host_lifecycle_agent import agent_startup
        from src.common.jsonl import append_jsonl

        ledger = tmp_path / "events.jsonl"

        # Pre-populate with an old event
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        append_jsonl(ledger, _make_event("old_event", ts=old_ts))

        with patch("scripts.host_lifecycle_agent.get_host_boot_id", return_value="startup-boot-id"):
            agent_startup(ledger)

        events = _read_events(ledger)
        # Old event should be pruned, boot_id event should be present
        assert len(events) == 1
        assert events[0]["event_type"] == "boot_id"
        assert events[0]["boot_id"] == "startup-boot-id"

    def test_startup_creates_ledger_directory(self, tmp_path: Path):
        """Startup should create the ledger directory if it doesn't exist."""
        from scripts.host_lifecycle_agent import agent_startup

        ledger = tmp_path / "deep" / "nested" / "events.jsonl"

        with patch("scripts.host_lifecycle_agent.get_host_boot_id", return_value="boot-1"):
            agent_startup(ledger)

        assert ledger.exists()
        events = _read_events(ledger)
        assert len(events) == 1
        assert events[0]["event_type"] == "boot_id"
