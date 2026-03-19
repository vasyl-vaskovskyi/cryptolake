"""Host lifecycle ledger reader for writer recovery classification.

Reads the JSONL lifecycle ledger written by the host lifecycle agent
and provides strict evidence lookup for component-specific restart
classification.  The reader is used once at writer startup (recovery
time) and results are cached for the lifetime of the writer process.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from src.common.jsonl import read_jsonl

DEFAULT_LEDGER_PATH = Path("/data/.cryptolake/lifecycle/events.jsonl")

# Components that Phase 2 can promote to (not collector — Phase 1 handles it).
# Ordered by infrastructure priority: infra components cause cascading restarts.
PROMOTABLE_COMPONENTS = ("redpanda", "postgres", "writer")


class HostLifecycleEvidence:
    """Parsed host lifecycle evidence for a specific restart window.

    Immutable after construction.  Indexes container die/stop events
    by component name for fast lookup during classification.
    """

    def __init__(self, events: list[dict]):
        self._events = events
        self._container_dies: dict[str, list[dict]] = {}
        self._container_stops: dict[str, list[dict]] = {}
        self._maintenance_intents: list[dict] = []

        for evt in events:
            event_type = evt.get("event_type", "")
            if event_type == "container_die":
                component = evt.get("container", "")
                self._container_dies.setdefault(component, []).append(evt)
            elif event_type == "container_stop":
                component = evt.get("container", "")
                self._container_stops.setdefault(component, []).append(evt)
            elif event_type == "maintenance_intent":
                self._maintenance_intents.append(evt)

    def has_component_die(self, component: str) -> bool:
        """Check if a specific component had a die event in the window."""
        return component in self._container_dies

    def component_clean_exit(self, component: str) -> bool | None:
        """Check if a component's most recent die event was a clean exit.

        Returns None if no die event found for the component.
        """
        dies = self._container_dies.get(component, [])
        if not dies:
            return None
        return dies[-1].get("clean_exit", False)

    def has_component_stop(self, component: str) -> bool:
        """Check if a specific component had a stop event in the window."""
        return component in self._container_stops

    def has_maintenance_intent(self) -> bool:
        """Check if a maintenance intent was recorded in the window."""
        return len(self._maintenance_intents) > 0

    @property
    def is_empty(self) -> bool:
        """True if no events were found in the restart window."""
        return len(self._events) == 0


def load_host_evidence(
    ledger_path: Path,
    window_start_iso: str | None = None,
    window_end_iso: str | None = None,
) -> HostLifecycleEvidence:
    """Load host lifecycle evidence from the JSONL ledger.

    Filters events to the restart window [window_start, window_end].
    If no window boundaries are specified, returns all events.
    Returns an empty evidence object if the ledger is missing or empty.
    """
    all_events = read_jsonl(ledger_path)

    if not window_start_iso and not window_end_iso:
        return HostLifecycleEvidence(all_events)

    window_start = None
    window_end = None
    if window_start_iso:
        try:
            window_start = datetime.fromisoformat(window_start_iso)
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass
    if window_end_iso:
        try:
            window_end = datetime.fromisoformat(window_end_iso)
            if window_end.tzinfo is None:
                window_end = window_end.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass

    filtered: list[dict] = []
    for evt in all_events:
        ts_str = evt.get("ts", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if window_start and ts < window_start:
            continue
        if window_end and ts > window_end:
            continue
        filtered.append(evt)

    return HostLifecycleEvidence(filtered)
