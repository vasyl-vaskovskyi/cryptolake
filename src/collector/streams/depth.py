from __future__ import annotations

from typing import Callable

import structlog

from src.collector.gap_detector import DepthGapDetector
from src.collector.producer import CryptoLakeProducer
from src.collector.streams.base import StreamHandler
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


_MAX_PENDING_DIFFS = 5000  # max buffered diffs per symbol during resync


class DepthHandler(StreamHandler):
    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        symbols: list[str],
        on_pu_chain_break: Callable[[str], None] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.detectors: dict[str, DepthGapDetector] = {
            s: DepthGapDetector(symbol=s) for s in symbols
        }
        self._pending_diffs: dict[str, list[tuple[str, int | None, int]]] = {
            s: [] for s in symbols
        }
        self._on_pu_chain_break = on_pu_chain_break

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        detector = self.detectors.get(symbol)
        if detector is None:
            return

        U, u, pu = self.adapter.parse_depth_update_ids(raw_text)
        result = detector.validate_diff(U=U, u=u, pu=pu)

        if result.stale:
            logger.debug("depth_stale_diff_dropped", symbol=symbol, u=u)
            return

        if result.gap:
            import time as _time
            logger.warning("depth_pu_chain_break", symbol=symbol,
                           expected_pu=detector._last_u, actual_pu=pu)
            collector_metrics.gaps_detected_total.labels(
                exchange=self.exchange, symbol=symbol, stream="depth",
            ).inc()
            now = _time.time_ns()
            gap = create_gap_envelope(
                exchange=self.exchange, symbol=symbol, stream="depth",
                collector_session_id=self.collector_session_id,
                session_seq=session_seq,
                gap_start_ts=now, gap_end_ts=now,
                reason="pu_chain_break",
                detail=f"pu chain break: expected pu={detector._last_u}, got pu={pu}",
            )
            self.producer.produce(gap)
            if self._on_pu_chain_break:
                self._on_pu_chain_break(symbol)
            return

        if not result.valid:
            # Not synced yet — buffer live diffs for replay after snapshot arrives
            pending = self._pending_diffs.get(symbol)
            if pending is not None and len(pending) < _MAX_PENDING_DIFFS:
                pending.append((raw_text, exchange_ts, session_seq))
            else:
                import time as _time
                logger.warning("depth_pending_buffer_full", symbol=symbol)
                collector_metrics.gaps_detected_total.labels(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                ).inc()
                now = _time.time_ns()
                gap = create_gap_envelope(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                    collector_session_id=self.collector_session_id,
                    session_seq=session_seq,
                    gap_start_ts=now, gap_end_ts=now,
                    reason="buffer_overflow",
                    detail=f"Depth pending-diff buffer full ({_MAX_PENDING_DIFFS}), diffs dropped while awaiting snapshot",
                )
                self.producer.produce(gap)
                detector.reset()
                if pending is not None:
                    pending.clear()
                if self._on_pu_chain_break:
                    self._on_pu_chain_break(symbol)
            return

        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="depth",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)

    def set_sync_point(self, symbol: str, last_update_id: int) -> None:
        """Set sync point from snapshot and replay buffered diffs."""
        detector = self.detectors.get(symbol)
        if detector is None:
            return
        detector.set_sync_point(last_update_id)

        # Replay buffered diffs
        pending = self._pending_diffs.get(symbol, [])
        self._pending_diffs[symbol] = []
        replayed = 0
        for raw_text, exchange_ts, session_seq in pending:
            U, u, pu = self.adapter.parse_depth_update_ids(raw_text)
            result = detector.validate_diff(U=U, u=u, pu=pu)
            if result.stale:
                continue
            if result.gap:
                import time as _time
                logger.warning("depth_replay_pu_chain_break", symbol=symbol,
                               expected_pu=detector._last_u, actual_pu=pu)
                collector_metrics.gaps_detected_total.labels(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                ).inc()
                now = _time.time_ns()
                gap = create_gap_envelope(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                    collector_session_id=self.collector_session_id,
                    session_seq=session_seq,
                    gap_start_ts=now, gap_end_ts=now,
                    reason="pu_chain_break",
                    detail=f"pu chain break during replay: expected pu={detector._last_u}, got pu={pu}",
                )
                self.producer.produce(gap)
                if self._on_pu_chain_break:
                    self._on_pu_chain_break(symbol)
                return
            if not result.valid:
                continue
            envelope = create_data_envelope(
                exchange=self.exchange,
                symbol=symbol,
                stream="depth",
                raw_text=raw_text,
                exchange_ts=exchange_ts or 0,
                collector_session_id=self.collector_session_id,
                session_seq=session_seq,
            )
            self.producer.produce(envelope)
            replayed += 1
        if replayed > 0:
            logger.info("depth_pending_replayed", symbol=symbol, replayed=replayed,
                        total_buffered=len(pending))

    def reset(self, symbol: str) -> None:
        detector = self.detectors.get(symbol)
        if detector:
            detector.reset()
        # Keep pending diffs — they will be replayed after next sync point
