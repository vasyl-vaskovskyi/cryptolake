"""Unit tests for the host lifecycle reader and Phase 2 classifier promotion."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.writer.state_manager import MaintenanceIntent


def _ts(minutes_ago: int = 0) -> str:
    """ISO timestamp some minutes in the past (negative = future)."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()


def _write_events(ledger: Path, events: list[dict]) -> None:
    """Write events to a JSONL ledger file."""
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger, "w") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")


def _make_intent(*, expires_in_minutes: int = 30) -> MaintenanceIntent:
    """Helper to create a valid MaintenanceIntent."""
    now = datetime.now(timezone.utc)
    return MaintenanceIntent(
        maintenance_id="maint-001",
        scope="full_stack",
        planned_by="operator",
        reason="scheduled update",
        created_at=(now - timedelta(minutes=5)).isoformat(),
        expires_at=(now + timedelta(minutes=expires_in_minutes)).isoformat(),
    )


# ===========================================================================
# Test: HostLifecycleEvidence loading and querying
# ===========================================================================


class TestHostLifecycleReader:
    """Test loading and filtering host lifecycle evidence."""

    def test_load_empty_ledger(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        ledger.write_text("")
        evidence = load_host_evidence(ledger)
        assert evidence.is_empty

    def test_load_missing_ledger(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "nonexistent" / "events.jsonl"
        evidence = load_host_evidence(ledger)
        assert evidence.is_empty

    def test_load_events_within_window(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(120), "event_type": "boot_id", "boot_id": "old"},
            {"ts": _ts(10), "event_type": "container_die", "container": "writer",
             "exit_code": 1, "clean_exit": False},
            {"ts": _ts(5), "event_type": "boot_id", "boot_id": "new"},
        ]
        _write_events(ledger, events)

        # Filter to last 30 minutes
        evidence = load_host_evidence(ledger, window_start_iso=_ts(30))
        assert not evidence.is_empty
        assert evidence.has_component_die("writer")

    def test_filter_excludes_old_events(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(120), "event_type": "container_die", "container": "redpanda",
             "exit_code": 1, "clean_exit": False},
            {"ts": _ts(5), "event_type": "boot_id", "boot_id": "new"},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger, window_start_iso=_ts(30))
        assert not evidence.has_component_die("redpanda")

    def test_component_clean_exit(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "postgres",
             "exit_code": 0, "clean_exit": True},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger)
        assert evidence.component_clean_exit("postgres") is True

    def test_component_unclean_exit(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "redpanda",
             "exit_code": 137, "clean_exit": False},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger)
        assert evidence.component_clean_exit("redpanda") is False

    def test_no_die_event_returns_none(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_start", "container": "writer"},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger)
        assert evidence.component_clean_exit("writer") is None

    def test_has_maintenance_intent(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "maintenance_intent",
             "reason": "upgrade", "source": "cli"},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger)
        assert evidence.has_maintenance_intent()

    def test_partial_last_line_discarded(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        good = json.dumps({"ts": _ts(10), "event_type": "container_die",
                           "container": "writer", "exit_code": 1, "clean_exit": False})
        ledger.parent.mkdir(parents=True, exist_ok=True)
        ledger.write_text(good + "\n" + '{"ts": "incomplete')

        evidence = load_host_evidence(ledger)
        assert evidence.has_component_die("writer")

    def test_has_component_stop(self, tmp_path: Path):
        from src.writer.host_lifecycle_reader import load_host_evidence

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_stop", "container": "postgres"},
        ]
        _write_events(ledger, events)

        evidence = load_host_evidence(ledger)
        assert evidence.has_component_stop("postgres")


# ===========================================================================
# Test: Classifier promotion with host lifecycle evidence (Phase 2)
# ===========================================================================


class TestClassifierWithHostEvidence:
    """Test classifier promotion with host lifecycle evidence."""

    def test_writer_unexpected_exit_unchanged_boot(self, tmp_path: Path):
        """Writer container died with unchanged boot ID -> component=writer."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "writer",
             "exit_code": 1, "clean_exit": False},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-same",
            current_session_id="session-same",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        assert result["component"] == "writer"
        assert result["cause"] == "unclean_exit"
        assert result["planned"] is False
        assert "host_evidence_writer_die" in result["evidence"]

    def test_postgres_planned_restart_with_intent(self, tmp_path: Path):
        """Postgres container stopped cleanly under maintenance intent."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "maintenance_intent",
             "reason": "pg upgrade", "source": "cli"},
            {"ts": _ts(8), "event_type": "container_die", "container": "postgres",
             "exit_code": 0, "clean_exit": True},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=_make_intent(),
            host_evidence=evidence,
        )

        assert result["component"] == "postgres"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True
        assert "host_evidence_postgres_die" in result["evidence"]
        assert "host_evidence_postgres_clean_exit" in result["evidence"]

    def test_redpanda_unexpected_restart_unchanged_boot(self, tmp_path: Path):
        """Redpanda died unexpectedly -> component=redpanda, cause=unclean_exit."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "redpanda",
             "exit_code": 137, "clean_exit": False},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        assert result["component"] == "redpanda"
        assert result["cause"] == "unclean_exit"
        assert result["planned"] is False
        assert "host_evidence_redpanda_die" in result["evidence"]

    def test_fallback_when_no_host_evidence(self):
        """No host evidence -> fall back to Phase 1 classification."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=None,
        )

        # Phase 1: collector unclean exit
        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"

    def test_fallback_when_empty_host_evidence(self, tmp_path: Path):
        """Empty host evidence -> fall back to Phase 1 classification."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        ledger.write_text("")
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"

    def test_collector_die_does_not_override_phase1(self, tmp_path: Path):
        """Collector die event should NOT change Phase 1 classification."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "collector",
             "exit_code": 1, "clean_exit": False},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        # Phase 1 handles collector — collector is not in PROMOTABLE_COMPONENTS
        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"

    def test_host_reboot_not_promoted_when_boot_id_changed(self, tmp_path: Path):
        """When boot ID changed, host evidence does not override host_reboot."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "redpanda",
             "exit_code": 137, "clean_exit": False},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        # Boot ID changed -> host_reboot takes precedence
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"

    def test_redpanda_prioritized_over_writer(self, tmp_path: Path):
        """When both redpanda and writer died, redpanda is root cause (infra priority)."""
        from src.writer.host_lifecycle_reader import load_host_evidence
        from src.writer.restart_gap_classifier import classify_restart_gap

        ledger = tmp_path / "events.jsonl"
        events = [
            {"ts": _ts(10), "event_type": "container_die", "container": "redpanda",
             "exit_code": 137, "clean_exit": False},
            {"ts": _ts(9), "event_type": "container_die", "container": "writer",
             "exit_code": 1, "clean_exit": False},
        ]
        _write_events(ledger, events)
        evidence = load_host_evidence(ledger)

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
            host_evidence=evidence,
        )

        # redpanda has higher priority than writer
        assert result["component"] == "redpanda"
        assert result["cause"] == "unclean_exit"
