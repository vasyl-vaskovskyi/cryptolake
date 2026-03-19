"""System identity helpers for restart-gap classification."""
from __future__ import annotations

import os

_BOOT_ID_PATH = "/proc/sys/kernel/random/boot_id"
_ENV_OVERRIDE = "CRYPTOLAKE_TEST_BOOT_ID"


def get_host_boot_id() -> str:
    """Return the current host boot ID.

    Resolution order:
    1. ``CRYPTOLAKE_TEST_BOOT_ID`` env var (deterministic override for tests).
    2. ``/proc/sys/kernel/random/boot_id`` (Linux).
    3. ``"unknown"`` sentinel when strict evidence is unavailable (e.g., macOS).
    """
    # 1. Env override for testing
    env_val = os.environ.get(_ENV_OVERRIDE)
    if env_val is not None:
        return env_val.strip()

    # 2. Linux /proc boot_id
    try:
        with open(_BOOT_ID_PATH) as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError, OSError):
        pass

    # 3. Fallback sentinel
    return "unknown"
