"""Strict Phase 1 restart gap classifier.

A pure function that classifies restart gaps based on durable evidence.
No side effects, no I/O -- takes evidence as parameters, returns a classification dict.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.writer.state_manager import MaintenanceIntent

_CLASSIFIER_VERSION = "writer_recovery_v1"


def _is_intent_valid(intent: MaintenanceIntent | None) -> bool:
    """Check if a maintenance intent exists and has not expired."""
    if intent is None:
        return False
    try:
        expires_at = datetime.fromisoformat(intent.expires_at)
        # Ensure timezone-aware comparison
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) <= expires_at
    except (ValueError, TypeError):
        return False


def classify_restart_gap(
    *,
    previous_boot_id: str | None,
    current_boot_id: str,
    previous_session_id: str | None,
    current_session_id: str | None,
    collector_clean_shutdown: bool,
    system_clean_shutdown: bool,
    maintenance_intent: MaintenanceIntent | None,
) -> dict[str, object]:
    """Classify a restart gap based on durable evidence.

    Classification matrix (evaluated in order):

    1. Planned system restart:
       valid maintenance_intent + prior components show clean_shutdown
       -> component=system, cause=operator_shutdown, planned=true

    2. Host reboot (unplanned):
       boot_id changed + no valid maintenance_intent
       -> component=host, cause=host_reboot, planned=false

    3. Host reboot with maintenance intent:
       boot_id changed + valid maintenance_intent
       -> component=host, cause=host_reboot, planned=true

    4. Planned collector restart:
       same boot_id + session changed + clean collector shutdown + valid maintenance_intent
       -> component=collector, cause=operator_shutdown, planned=true

    5. Collector unclean exit:
       same boot_id + session changed + no clean collector shutdown
       -> component=collector, cause=unclean_exit, planned=false

    6. Unknown fallback:
       none of the above proved
       -> component=system, cause=unknown, planned=false
    """
    evidence: list[str] = []
    intent_valid = _is_intent_valid(maintenance_intent)

    if maintenance_intent is not None:
        if intent_valid:
            evidence.append("maintenance_intent_valid")
        else:
            evidence.append("maintenance_intent_expired")

    # Determine boot ID comparison
    boot_id_changed: bool | None = None
    if previous_boot_id is not None:
        boot_id_changed = previous_boot_id != current_boot_id
        if boot_id_changed:
            evidence.append("host_boot_id_changed")
        else:
            evidence.append("host_boot_id_unchanged")

    # Determine session change
    session_changed: bool | None = None
    if previous_session_id is not None and current_session_id is not None:
        session_changed = previous_session_id != current_session_id
        if session_changed:
            evidence.append("collector_session_changed")
        else:
            evidence.append("collector_session_unchanged")

    # Record shutdown evidence
    if collector_clean_shutdown:
        evidence.append("collector_clean_shutdown")
    if system_clean_shutdown:
        evidence.append("system_clean_shutdown")

    # --- Classification logic (order matters) ---

    # Case 1: Planned system restart
    # Valid maintenance intent + clean shutdown evidence for prior components
    if intent_valid and system_clean_shutdown and collector_clean_shutdown:
        return _result(
            component="system",
            cause="operator_shutdown",
            planned=True,
            evidence=evidence,
            maintenance_id=maintenance_intent.maintenance_id if maintenance_intent else None,
        )

    # For remaining cases, we need to check boot ID
    if boot_id_changed is None:
        # No previous boot ID to compare -- cannot determine cause
        return _result(
            component="system",
            cause="unknown",
            planned=False,
            evidence=evidence,
        )

    if boot_id_changed:
        # Cases 2 and 5: Host reboot
        if intent_valid:
            # Case 5: Host reboot with maintenance intent
            return _result(
                component="host",
                cause="host_reboot",
                planned=True,
                evidence=evidence,
                maintenance_id=maintenance_intent.maintenance_id if maintenance_intent else None,
            )
        else:
            # Case 2: Host reboot (unplanned)
            return _result(
                component="host",
                cause="host_reboot",
                planned=False,
                evidence=evidence,
            )

    # Boot ID unchanged -- check for collector-level changes
    if session_changed is None:
        # Cannot determine session change without both session IDs
        return _result(
            component="system",
            cause="unknown",
            planned=False,
            evidence=evidence,
        )

    if session_changed:
        # Cases 3 and 4: Collector restart on same host
        if collector_clean_shutdown and intent_valid:
            # Case 3: Planned collector restart
            return _result(
                component="collector",
                cause="operator_shutdown",
                planned=True,
                evidence=evidence,
                maintenance_id=maintenance_intent.maintenance_id if maintenance_intent else None,
            )
        else:
            # Case 4: Collector unclean exit (or clean but no intent)
            return _result(
                component="collector",
                cause="unclean_exit",
                planned=False,
                evidence=evidence,
            )

    # Case 6: No evidence of restart -- unknown fallback
    return _result(
        component="system",
        cause="unknown",
        planned=False,
        evidence=evidence,
    )


def _result(
    *,
    component: str,
    cause: str,
    planned: bool,
    evidence: list[str],
    maintenance_id: str | None = None,
) -> dict[str, object]:
    """Build a classification result dict with the standard shape."""
    result: dict[str, object] = {
        "reason": "restart_gap",
        "component": component,
        "cause": cause,
        "planned": planned,
        "classifier": _CLASSIFIER_VERSION,
        "evidence": evidence,
    }
    if maintenance_id is not None:
        result["maintenance_id"] = maintenance_id
    return result
