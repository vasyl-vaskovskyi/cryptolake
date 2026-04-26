"""Per-(symbol, stream) liveness heartbeat emitter.

Phase 2 of the silent-loss elimination work. The collector cannot reliably
infer what data was lost upstream — it only knows what it received. By
emitting heartbeats on a fixed cadence regardless of data flow, the COLLECTOR
publishes a continuous liveness signal and the CONSUMER (writer / verify CLI)
infers gaps from missing heartbeats. This is more robust than upstream gap
emission because:

  - Missing heartbeats = collector down (no other path produces this signal).
  - Heartbeats with stale ``last_data_at_ns`` = Binance silent on this stream
    (collector is alive; data path is the issue).
  - Cross-collector heartbeat comparison lets the consumer pick the freshest
    feed without coordination between collectors.

The heartbeat is published to the same Kafka topic as data envelopes so it
lands in the writer's archive alongside the streams it describes.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import structlog

from src.collector import metrics as collector_metrics
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_heartbeat_envelope, serialize_envelope

logger = structlog.get_logger()

# Default cadence: 5 s. Trade-off: too short adds Kafka load (n_streams *
# n_symbols / 5 messages/sec); too long lengthens the consumer's blind spot
# when the collector goes silent. 5 s gives 12 heartbeats/min/stream which
# is comfortable on Kafka and tight enough that two missed = 10 s suspicion.
DEFAULT_INTERVAL_SECONDS = 5.0


class HeartbeatEmitter:
    """Emits per-(symbol, stream) heartbeat envelopes on a fixed cadence."""

    def __init__(
        self,
        *,
        producer: CryptoLakeProducer,
        exchange: str,
        symbols: list[str],
        streams: list[str],
        collector_session_id: str,
        last_received_at: dict[tuple[str, str], int],
        seq_counters: dict[tuple[str, str], int],
        ws_connected: dict[str, bool],
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    ) -> None:
        self.producer = producer
        self.exchange = exchange
        self.symbols = symbols
        self.streams = streams
        self.collector_session_id = collector_session_id
        # Reference (not copy) into WebSocketManager state — read-only.
        self._last_received_at = last_received_at
        self._seq_counters = seq_counters
        self._ws_connected = ws_connected
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Run the heartbeat loop. Blocks until cancelled."""
        logger.info("heartbeat_emitter_starting",
                    interval_s=self.interval_seconds,
                    symbols=self.symbols, streams=self.streams)
        try:
            while True:
                self._emit_one_cycle()
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            logger.info("heartbeat_emitter_stopped")
            raise

    def _emit_one_cycle(self) -> None:
        """Emit one heartbeat per (symbol, stream) for the configured set."""
        emitted_at = time.time_ns()
        ws_up = any(self._ws_connected.values()) if self._ws_connected else False
        for symbol in self.symbols:
            for stream in self.streams:
                key = (symbol, stream)
                last_data = self._last_received_at.get(key)
                last_seq = self._seq_counters.get(key, 0)
                status = self._classify_status(emitted_at, last_data, ws_up)
                env = create_heartbeat_envelope(
                    exchange=self.exchange,
                    symbol=symbol,
                    stream=stream,
                    collector_session_id=self.collector_session_id,
                    emitted_at_ns=emitted_at,
                    last_data_at_ns=last_data,
                    last_session_seq=last_seq,
                    status=status,
                )
                try:
                    self._publish(symbol, stream, env)
                    collector_metrics.heartbeats_emitted_total.labels(
                        exchange=self.exchange, symbol=symbol,
                        stream=stream, status=status,
                    ).inc()
                except Exception as e:
                    logger.error("heartbeat_emit_failed",
                                 symbol=symbol, stream=stream, error=str(e))

    def _classify_status(
        self, now_ns: int, last_data_ns: int | None, ws_up: bool
    ) -> str:
        """Classify the stream's current state for the consumer.

        - disconnected: WebSocket is currently down (no socket, no data path).
        - subscribed_silent: WS is up, but no data within 2x heartbeat interval.
          Distinguishes Binance-side silence (e.g. silent subscription drop)
          from collector-side silence (heartbeats stop entirely).
        - alive: WS is up and data flowed within 2x heartbeat interval.
        """
        if not ws_up:
            return "disconnected"
        if last_data_ns is None:
            return "subscribed_silent"
        idle_ns = now_ns - last_data_ns
        if idle_ns > int(self.interval_seconds * 2 * 1e9):
            return "subscribed_silent"
        return "alive"

    def _publish(self, symbol: str, stream: str, envelope: dict[str, Any]) -> None:
        """Publish heartbeat to the same topic as data for this (exchange, stream).

        Reuses the producer's topic naming so heartbeats land in the writer's
        archive alongside the streams they describe.
        """
        topic = f"{self.producer.topic_prefix}{self.producer.exchange}.{stream}"
        # Bypass the data-cap path: heartbeats are small, fixed-cadence, and
        # must not be dropped by per-stream buffer caps (they ARE the
        # liveness signal).
        self.producer._producer.produce(
            topic=topic,
            key=symbol.encode(),
            value=serialize_envelope(envelope),
            # No delivery callback: heartbeat-on-heartbeat-failure recursion
            # would dwarf the actual data path on broker outage. The next
            # heartbeat will reflect the gap naturally.
        )
        try:
            asyncio.get_running_loop().run_in_executor(
                None, self.producer._producer.poll, 0)
        except RuntimeError:
            self.producer._producer.poll(0)
