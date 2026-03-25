from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.writer.state_manager import MaintenanceIntent


def make_intent(
    *,
    expires_in_minutes: int = 30,
    scope: str = "full_stack",
    consumed: bool = False,
) -> MaintenanceIntent:
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


def expired_intent(*, scope: str = "full_stack") -> MaintenanceIntent:
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
