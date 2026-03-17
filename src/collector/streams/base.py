from __future__ import annotations

import time
from abc import ABC, abstractmethod

import structlog

from src.collector.gap_detector import SessionSeqTracker
from src.collector import metrics as collector_metrics
from src.common.envelope import create_gap_envelope

logger = structlog.get_logger()


class StreamHandler(ABC):
    """Processes a routed message for a specific stream type.

    Subclasses that want session_seq gap detection should call
    self._check_seq(symbol, session_seq) at the start of handle().
    """

    def _init_seq_tracking(self, exchange: str, collector_session_id: str,
                           producer, stream_name: str) -> None:
        """Call from subclass __init__ to enable session_seq gap detection."""
        self._seq_exchange = exchange
        self._seq_collector_session_id = collector_session_id
        self._seq_producer = producer
        self._seq_stream = stream_name
        self._seq_trackers: dict[str, SessionSeqTracker] = {}

    def _check_seq(self, symbol: str, session_seq: int) -> None:
        """Check session_seq continuity; emit gap record on skip (spec 7.4)."""
        tracker = self._seq_trackers.get(symbol)
        if tracker is None:
            tracker = SessionSeqTracker()
            self._seq_trackers[symbol] = tracker
        gap = tracker.check(session_seq)
        if gap is not None:
            logger.warning("session_seq_skip", symbol=symbol,
                           stream=self._seq_stream,
                           expected=gap.expected, actual=gap.actual)
            collector_metrics.gaps_detected_total.labels(
                exchange=self._seq_exchange, symbol=symbol,
                stream=self._seq_stream,
            ).inc()
            now = time.time_ns()
            gap_env = create_gap_envelope(
                exchange=self._seq_exchange, symbol=symbol,
                stream=self._seq_stream,
                collector_session_id=self._seq_collector_session_id,
                session_seq=session_seq,
                gap_start_ts=now, gap_end_ts=now,
                reason="session_seq_skip",
                detail=f"session_seq skip: expected {gap.expected}, got {gap.actual}",
            )
            self._seq_producer.produce(gap_env)

    @abstractmethod
    async def handle(
        self,
        symbol: str,
        raw_text: str,
        exchange_ts: int | None,
        session_seq: int,
    ) -> None:
        """Process a single stream message: wrap in envelope, produce to Redpanda."""
