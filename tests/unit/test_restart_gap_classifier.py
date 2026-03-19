"""Unit tests for the restart gap classifier (Phase 1 strict classification)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.writer.state_manager import MaintenanceIntent


def _make_intent(
    *,
    expires_in_minutes: int = 30,
    scope: str = "full_stack",
    consumed: bool = False,
) -> MaintenanceIntent:
    """Helper to create a MaintenanceIntent with sensible defaults."""
    now = datetime.now(timezone.utc)
    return MaintenanceIntent(
        maintenance_id="maint-001",
        scope=scope,
        planned_by="operator",
        reason="scheduled update",
        created_at=(now - timedelta(minutes=5)).isoformat(),
        expires_at=(now + timedelta(minutes=expires_in_minutes)).isoformat(),
        consumed_at=now.isoformat() if consumed else None,
    )


def _expired_intent(*, scope: str = "full_stack") -> MaintenanceIntent:
    """Helper to create an already-expired MaintenanceIntent."""
    now = datetime.now(timezone.utc)
    return MaintenanceIntent(
        maintenance_id="maint-expired",
        scope=scope,
        planned_by="operator",
        reason="scheduled update",
        created_at=(now - timedelta(hours=2)).isoformat(),
        expires_at=(now - timedelta(hours=1)).isoformat(),
        consumed_at=None,
    )


class TestPlannedSystemRestart:
    """Case 1: Valid maintenance intent + clean shutdown evidence for prior components."""

    def test_planned_system_restart_full_stack(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=True,
            maintenance_intent=_make_intent(),
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "system"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True
        assert result["classifier"] == "writer_recovery_v1"
        assert isinstance(result["evidence"], list)
        assert len(result["evidence"]) > 0

    def test_planned_system_restart_same_boot_id(self):
        """Planned system restart where boot ID happens to stay the same (e.g. container restart)."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=True,
            maintenance_intent=_make_intent(),
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "system"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True


class TestHostRebootUnplanned:
    """Case 2: Boot ID changed, no clean shutdown, no maintenance intent."""

    def test_unplanned_host_reboot(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is False
        assert result["classifier"] == "writer_recovery_v1"
        assert "host_boot_id_changed" in result["evidence"]

    def test_unplanned_host_reboot_with_expired_intent(self):
        """Expired maintenance intent does NOT make a reboot planned."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=_expired_intent(),
        )

        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is False
        assert "maintenance_intent_expired" in result["evidence"]


class TestPlannedCollectorRestart:
    """Case 3: Same boot ID, session changed, clean collector shutdown, valid maintenance."""

    def test_planned_collector_restart(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=False,
            maintenance_intent=_make_intent(scope="collector"),
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "collector"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True
        assert result["classifier"] == "writer_recovery_v1"
        assert "collector_session_changed" in result["evidence"]
        assert "host_boot_id_unchanged" in result["evidence"]
        assert "collector_clean_shutdown" in result["evidence"]

    def test_planned_collector_restart_full_stack_scope(self):
        """A full_stack maintenance intent also covers collector-only restarts."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=False,
            maintenance_intent=_make_intent(scope="full_stack"),
        )

        assert result["component"] == "collector"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True


class TestCollectorUncleanExit:
    """Case 4: Same boot ID, session changed, no clean shutdown evidence."""

    def test_collector_crash(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"
        assert result["planned"] is False
        assert result["classifier"] == "writer_recovery_v1"
        assert "collector_session_changed" in result["evidence"]
        assert "host_boot_id_unchanged" in result["evidence"]

    def test_collector_unclean_with_expired_intent(self):
        """Expired maintenance intent does NOT make an unclean exit planned."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=_expired_intent(),
        )

        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"
        assert result["planned"] is False

    def test_collector_clean_shutdown_but_no_maintenance(self):
        """Clean shutdown without maintenance intent: still unclean_exit (strict mode)."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        # Without maintenance intent, we cannot prove it was planned,
        # but the clean shutdown evidence still lets us know it was collector.
        # However, without intent, planned=False.
        assert result["component"] == "collector"
        assert result["cause"] == "unclean_exit"
        assert result["planned"] is False


class TestHostRebootWithMaintenanceIntent:
    """Case 5: Boot ID changed + valid maintenance intent."""

    def test_planned_host_reboot(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=_make_intent(),
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is True
        assert result["classifier"] == "writer_recovery_v1"
        assert "host_boot_id_changed" in result["evidence"]
        assert "maintenance_intent_valid" in result["evidence"]


class TestUnknownFallback:
    """Case 6: None of the above proved - unknown fallback."""

    def test_no_previous_boot_id(self):
        """No previous boot ID means we can't compare - fall back to unknown."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id=None,
            current_boot_id="boot-bbb",
            previous_session_id=None,
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["reason"] == "restart_gap"
        assert result["component"] == "system"
        assert result["cause"] == "unknown"
        assert result["planned"] is False
        assert result["classifier"] == "writer_recovery_v1"

    def test_no_previous_session_id_same_boot(self):
        """No previous session ID means we can't determine session change."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id=None,
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["component"] == "system"
        assert result["cause"] == "unknown"
        assert result["planned"] is False

    def test_same_session_same_boot(self):
        """Same session and same boot: no evidence of restart - unknown."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-same",
            current_session_id="session-same",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["component"] == "system"
        assert result["cause"] == "unknown"
        assert result["planned"] is False


class TestMaintenanceIntentExpiry:
    """Edge cases around maintenance intent TTL."""

    def test_expired_intent_treated_as_no_intent_for_system_restart(self):
        """Expired intent with clean shutdowns: falls through to appropriate unplanned."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=True,
            maintenance_intent=_expired_intent(),
        )

        # Boot ID changed + no valid intent = unplanned host reboot
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is False

    def test_valid_intent_includes_maintenance_id(self):
        """When maintenance intent is valid, maintenance_id should be in the result."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        intent = _make_intent()
        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=True,
            system_clean_shutdown=True,
            maintenance_intent=intent,
        )

        assert result["maintenance_id"] == "maint-001"

    def test_no_maintenance_id_when_no_intent(self):
        """When no maintenance intent, maintenance_id should not be in result."""
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert "maintenance_id" not in result or result.get("maintenance_id") is None


class TestOutputShape:
    """Verify the output dict has all required keys in the correct shape."""

    def test_all_required_keys_present(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert "reason" in result
        assert "component" in result
        assert "cause" in result
        assert "planned" in result
        assert "classifier" in result
        assert "evidence" in result

    def test_evidence_is_list_of_strings(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-aaa",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert isinstance(result["evidence"], list)
        for item in result["evidence"]:
            assert isinstance(item, str)

    def test_planned_is_bool(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id="boot-aaa",
            current_boot_id="boot-bbb",
            previous_session_id="session-old",
            current_session_id="session-new",
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert isinstance(result["planned"], bool)

    def test_classifier_version(self):
        from src.writer.restart_gap_classifier import classify_restart_gap

        result = classify_restart_gap(
            previous_boot_id=None,
            current_boot_id="boot-aaa",
            previous_session_id=None,
            current_session_id=None,
            collector_clean_shutdown=False,
            system_clean_shutdown=False,
            maintenance_intent=None,
        )

        assert result["classifier"] == "writer_recovery_v1"
