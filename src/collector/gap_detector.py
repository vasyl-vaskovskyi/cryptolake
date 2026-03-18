from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DiffValidationResult:
    valid: bool
    gap: bool = False
    stale: bool = False
    reason: str = ""


@dataclass
class SeqGap:
    expected: int
    actual: int


class DepthGapDetector:
    """Validates depth diff pu chain after synchronization with a snapshot."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._synced = False
        self._last_update_id: int | None = None
        self._last_u: int | None = None

    def set_sync_point(self, last_update_id: int) -> None:
        self._synced = True
        self._last_update_id = last_update_id
        self._last_u = None

    def validate_diff(self, U: int, u: int, pu: int) -> DiffValidationResult:
        if not self._synced:
            return DiffValidationResult(valid=False, reason="no_sync_point")

        # First diff after sync: find the sync point
        if self._last_u is None:
            lid = self._last_update_id
            if lid is None:
                return DiffValidationResult(valid=False, reason="no_sync_point")
            if u < lid:
                return DiffValidationResult(valid=False, stale=True, reason="stale")
            if U <= lid + 1 and u >= lid + 1:
                self._last_u = u
                return DiffValidationResult(valid=True)
            return DiffValidationResult(valid=False, reason="no_sync_diff_found")

        # Subsequent diffs: pu must equal previous u
        if pu != self._last_u:
            return DiffValidationResult(valid=False, gap=True, reason="pu_chain_break")

        self._last_u = u
        return DiffValidationResult(valid=True)

    def reset(self) -> None:
        self._synced = False
        self._last_update_id = None
        self._last_u = None


class SessionSeqTracker:
    """Tracks session_seq for in-session gap detection."""

    def __init__(self):
        self._last_seq: int | None = None

    def check(self, seq: int) -> SeqGap | None:
        if self._last_seq is None:
            self._last_seq = seq
            return None
        expected = self._last_seq + 1
        self._last_seq = seq
        if seq != expected:
            return SeqGap(expected=expected, actual=seq)
        return None
