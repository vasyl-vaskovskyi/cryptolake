from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable

import structlog
from confluent_kafka import Producer as KafkaProducer

from src.collector import metrics as collector_metrics
from src.collector.tap import FrameTap
from src.common.envelope import create_gap_envelope, serialize_envelope

logger = structlog.get_logger()

class CryptoLakeProducer:
    """Wraps confluent_kafka.Producer with envelope routing and overflow protection.

    Tracks overflow windows per (symbol, stream). When the first BufferError occurs,
    records the start timestamp. When produce succeeds after an overflow period,
    emits a buffer_overflow gap record covering the dropped interval.
    """

    def __init__(
        self,
        brokers: list[str],
        exchange: str,
        collector_session_id: str = "",
        max_buffer: int = 100_000,
        buffer_caps: dict[str, int] | None = None,
        default_stream_cap: int = 10_000,
        on_overflow: Callable[[str, str, str], None] | None = None,
        topic_prefix: str = "",
        tap_root: Path | None = None,
    ):
        self.exchange = exchange
        self.topic_prefix = topic_prefix
        self.collector_session_id = collector_session_id
        self.max_buffer = max_buffer
        self.buffer_caps = buffer_caps if buffer_caps is not None else {"depth": 80_000, "trades": 10_000}
        self.other_cap = default_stream_cap
        self._on_overflow = on_overflow
        self._buffer_counts: dict[str, int] = {}
        self._lock = threading.Lock()
        self._tap_root = tap_root
        self._taps: dict[str, FrameTap] = {}
        # Overflow window tracking: {(symbol, stream): {"start_ts": ns, "dropped": count}}
        self._overflow_windows: dict[tuple[str, str], dict] = {}
        self._overflow_seq: int = 0

        self._producer = KafkaProducer({
            "bootstrap.servers": ",".join(brokers),
            "acks": "all",
            "linger.ms": 5,
            "queue.buffering.max.messages": max_buffer,
            "queue.buffering.max.kbytes": 1048576,  # 1GB
        })

    def _get_stream_cap(self, stream: str) -> int:
        """Return the per-stream buffer cap for partitioned overflow protection."""
        return self.buffer_caps.get(stream, self.other_cap)

    def produce(self, envelope: dict) -> bool:
        """Produce an envelope to the appropriate Redpanda topic.

        Returns True if produced, False if dropped due to overflow.
        Enforces per-stream buffer caps to prevent high-volume streams (depth)
        from starving low-volume irreplaceable streams (liquidations, funding_rate).
        On recovery from overflow, emits a buffer_overflow gap record.
        """
        stream = envelope["stream"]
        symbol = envelope["symbol"]
        topic = f"{self.topic_prefix}{self.exchange}.{stream}"
        key = symbol.encode()

        # Fixture-capture tap (no-op when tap_root is None). Captures every
        # envelope with raw_text verbatim — the source of truth for Java
        # gate 3 (raw_text / raw_sha256 byte-identity). Gap records have no
        # raw_text and are skipped.
        raw_text = envelope.get("raw_text")
        if self._tap_root is not None and raw_text is not None:
            tap = self._taps.get(stream)
            if tap is None:
                tap = FrameTap(output_dir=self._tap_root / stream, stream=stream)
                self._taps[stream] = tap
            tap.write(raw_text.encode(), envelope)

        try:
            value = serialize_envelope(envelope)
        except Exception:
            logger.error(
                "serialization_failed",
                stream=stream,
                symbol=symbol,
            )
            collector_metrics.messages_dropped_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()
            return False

        # Check per-stream cap and optimistically increment under a single lock
        # to avoid TOCTOU between cap check and count update (spec 7.5)
        with self._lock:
            current = self._buffer_counts.get(stream, 0)
            cap = self._get_stream_cap(stream)
            if current >= cap:
                # Per-stream cap exceeded — drop to protect other streams
                collector_metrics.messages_dropped_total.labels(
                    exchange=self.exchange, symbol=symbol, stream=stream,
                ).inc()
                self._record_overflow(symbol, stream)
                return False
            # Optimistically increment before produce; delivery callback decrements
            self._buffer_counts[stream] = current + 1

        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self._make_delivery_cb(stream),
            )
            self._producer.poll(0)
            collector_metrics.messages_produced_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()

            # Check if we're recovering from overflow for this (symbol, stream)
            overflow_key = (symbol, stream)
            if overflow_key in self._overflow_windows:
                window = self._overflow_windows.pop(overflow_key)
                if window["dropped"] > 0:
                    self._emit_overflow_gap(symbol, stream, window)

            return True
        except BufferError:
            # Roll back optimistic increment since produce failed
            with self._lock:
                self._buffer_counts[stream] = max(0, self._buffer_counts.get(stream, 1) - 1)
            collector_metrics.messages_dropped_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()
            self._record_overflow(symbol, stream)
            return False

    def _record_overflow(self, symbol: str, stream: str) -> None:
        """Track overflow window with drop count."""
        overflow_key = (symbol, stream)
        if overflow_key not in self._overflow_windows:
            self._overflow_windows[overflow_key] = {"start_ts": time.time_ns(), "dropped": 0}
        self._overflow_windows[overflow_key]["dropped"] += 1
        if self._on_overflow:
            self._on_overflow(self.exchange, symbol, stream)

    def _make_delivery_cb(self, stream: str):
        """Create a delivery callback that decrements per-stream buffer count."""
        def _cb(err, msg):
            with self._lock:
                count = self._buffer_counts.get(stream, 0)
                if count > 0:
                    self._buffer_counts[stream] = count - 1
            if err is not None:
                logger.error("producer_delivery_failed", error=str(err),
                             topic=msg.topic(), key=msg.key())
        return _cb

    def _emit_overflow_gap(self, symbol: str, stream: str, window: dict) -> None:
        """Emit a buffer_overflow gap record when recovering from overflow."""
        dropped = window["dropped"]
        collector_metrics.gaps_detected_total.labels(
            exchange=self.exchange, symbol=symbol, stream=stream, reason="buffer_overflow",
        ).inc()
        gap = create_gap_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=self.collector_session_id,
            session_seq=self._overflow_seq,
            gap_start_ts=window["start_ts"],
            gap_end_ts=time.time_ns(),
            reason="buffer_overflow",
            detail=f"Producer buffer was full; {dropped} messages dropped for {stream}/{symbol}",
        )
        self._overflow_seq += 1
        # Produce the gap record itself (best-effort — buffer just recovered so should succeed)
        topic = f"{self.topic_prefix}{self.exchange}.{stream}"
        try:
            self._producer.produce(
                topic=topic,
                key=symbol.encode(),
                value=serialize_envelope(gap),
                on_delivery=self._make_delivery_cb(stream),
            )
            logger.info("buffer_overflow_gap_emitted", symbol=symbol, stream=stream)
        except BufferError:
            logger.error("buffer_overflow_gap_emit_failed", symbol=symbol, stream=stream)

    def emit_gap(
        self,
        *,
        symbol: str,
        stream: str,
        session_seq: int,
        reason: str,
        detail: str,
        gap_start_ts: int | None = None,
        gap_end_ts: int | None = None,
    ) -> None:
        """Emit a gap envelope and increment the gap metric. Convenience for callers."""
        now = time.time_ns()
        collector_metrics.gaps_detected_total.labels(
            exchange=self.exchange, symbol=symbol, stream=stream, reason=reason,
        ).inc()
        gap = create_gap_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
            gap_start_ts=gap_start_ts if gap_start_ts is not None else now,
            gap_end_ts=gap_end_ts if gap_end_ts is not None else now,
            reason=reason,
            detail=detail,
        )
        self.produce(gap)

    def is_connected(self) -> bool:
        """Check if the producer can reach the broker via metadata query."""
        try:
            # list_topics() actually contacts the broker — timeout keeps it fast
            self._producer.list_topics(timeout=5)
            return True
        except Exception:
            return False

    def is_healthy_for_resync(self) -> bool:
        """True if producer is connected and not actively dropping messages.
        Used as precondition for depth resync (spec 7.2)."""
        if self._overflow_windows:
            return False  # actively in overflow for at least one stream
        try:
            pending = len(self._producer)
            if pending > self.max_buffer * 0.8:
                return False  # buffer above 80% — too risky for resync
            self._producer.list_topics(timeout=5)
            return True
        except Exception:
            return False

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def poll(self, timeout: float = 0) -> int:
        return self._producer.poll(timeout)
