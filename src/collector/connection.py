from __future__ import annotations

import asyncio
import time

import aiohttp
import structlog
import websockets

from src.collector import metrics as collector_metrics
from src.collector.producer import CryptoLakeProducer
from src.collector.streams.base import StreamHandler
from src.collector.streams.depth import DepthHandler
from src.common.async_utils import cancel_tasks
from src.common.envelope import create_data_envelope
from src.collector.backup_chain_reader import read_last_depth_update_id, other_depth_topic
from src.exchanges.binance import BinanceAdapter, _PUBLIC_STREAMS, _MARKET_STREAMS

logger = structlog.get_logger()

# Backoff: 1s, 2s, 4s, 8s, 16s, 32s, max 60s
_MAX_BACKOFF = 60
_RECONNECT_BEFORE_24H = 23 * 3600 + 50 * 60  # 23h50m
_REST_ONLY_STREAMS = frozenset({"depth_snapshot", "open_interest"})


class WebSocketManager:
    """Manages WebSocket connections to Binance with reconnect and demultiplexing."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        adapter: BinanceAdapter,
        producer: CryptoLakeProducer,
        handlers: dict[str, StreamHandler],
        symbols: list[str],
        enabled_streams: list[str],
        brokers: list[str] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.adapter = adapter
        self.producer = producer
        self.handlers = handlers
        self.symbols = symbols
        self.enabled_streams = enabled_streams
        self._brokers = brokers or []
        self._seq_counters: dict[tuple[str, str], int] = {}
        self._running = False
        self._ws_connected: dict[str, bool] = {}  # {socket_name: connected}
        self._last_received_at: dict[tuple[str, str], int] = {}
        self._consecutive_drops = 0
        self._backpressure_threshold = 10  # consecutive drops before pausing WS reads
        self._tasks: list[asyncio.Task] = []

    def _next_seq(self, symbol: str, stream: str) -> int:
        key = (symbol, stream)
        seq = self._seq_counters.get(key, 0)
        self._seq_counters[key] = seq + 1
        return seq

    def _get_ws_urls(self) -> dict[str, str]:
        """Build WebSocket URLs, excluding REST-only streams."""
        ws_streams = [s for s in self.enabled_streams if s not in _REST_ONLY_STREAMS]
        return self.adapter.get_ws_urls(self.symbols, ws_streams)

    async def start(self) -> None:
        self._running = True
        urls = self._get_ws_urls()

        self._tasks = []
        for socket_name, url in urls.items():
            self._tasks.append(asyncio.create_task(
                self._connection_loop(socket_name, url)
            ))
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def has_ws_streams(self) -> bool:
        """True if this manager is expected to open any WebSocket connections."""
        return len(self._get_ws_urls()) > 0

    def is_connected(self) -> bool:
        """True if all expected WebSocket sockets are currently connected.
        Returns True if no WS sockets are expected (REST-only config, spec 11.3)."""
        if not self.has_ws_streams():
            return True  # no WS needed — skip check
        if not self._ws_connected:
            return False
        return all(self._ws_connected.values())

    async def stop(self) -> None:
        self._running = False
        await cancel_tasks(self._tasks)

    async def _connection_loop(self, socket_name: str, url: str) -> None:
        backoff = 1
        while self._running:
            connect_time = time.monotonic()
            try:
                collector_metrics.ws_connections_active.labels(
                    exchange=self.exchange
                ).inc()
                async with websockets.connect(url, ping_interval=30, ping_timeout=10,
                                              close_timeout=5) as ws:
                    self._ws_connected[socket_name] = True
                    logger.info("ws_connected", socket=socket_name, url=url[:80])
                    backoff = 1  # reset on successful connect

                    # Trigger initial depth resync on first connect (not just reconnects)
                    if socket_name == "public" and "depth" in self.enabled_streams:
                        for symbol in self.symbols:
                            asyncio.get_running_loop().create_task(
                                self._depth_resync(symbol))

                    await self._receive_loop(ws, socket_name, connect_time)
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning("ws_disconnected", socket=socket_name, error=str(e))
            except Exception as e:
                logger.error("ws_unexpected_error", socket=socket_name, error=str(e))
            finally:
                self._ws_connected[socket_name] = False
                collector_metrics.ws_connections_active.labels(
                    exchange=self.exchange
                ).dec()
                collector_metrics.ws_reconnects_total.labels(
                    exchange=self.exchange
                ).inc()

            # Emit gap records for all streams on this socket
            # (must run before the _running check so graceful shutdown still records gaps)
            self._emit_disconnect_gaps(socket_name)

            if not self._running:
                break

            # Trigger depth resync if public socket disconnected (spec 7.2)
            if socket_name == "public" and "depth" in self.enabled_streams:
                for symbol in self.symbols:
                    await self._depth_resync(symbol)

            await asyncio.sleep(min(backoff, _MAX_BACKOFF))
            backoff = min(backoff * 2, _MAX_BACKOFF)

    async def _receive_loop(self, ws, socket_name: str, connect_time: float) -> None:
        async for raw_frame in ws:
            if not self._running:
                break
            # Proactive 24h reconnect
            if time.monotonic() - connect_time > _RECONNECT_BEFORE_24H:
                logger.info("ws_proactive_reconnect", socket=socket_name)
                await ws.close()
                return

            # Backpressure: if producer is overwhelmed, pause WS reads
            if self._consecutive_drops >= self._backpressure_threshold:
                logger.warning("ws_backpressure_active", socket=socket_name,
                               consecutive_drops=self._consecutive_drops)
                while self._consecutive_drops >= self._backpressure_threshold and self._running:
                    self.producer.poll(0.1)
                    self._consecutive_drops = 0  # reset and re-evaluate on next produce
                    await asyncio.sleep(0.1)
                logger.info("ws_backpressure_released", socket=socket_name)

            try:
                stream_type, symbol, raw_text = self.adapter.route_stream(raw_frame)
            except Exception as e:
                logger.error("ws_route_failed", socket=socket_name, error=str(e))
                continue

            handler = self.handlers.get(stream_type)
            if handler is None:
                continue

            exchange_ts = self.adapter.extract_exchange_ts(stream_type, raw_text)
            seq = self._next_seq(symbol, stream_type)

            # Track latency
            if exchange_ts is not None:
                received_ms = time.time_ns() / 1_000_000
                latency = received_ms - exchange_ts
                collector_metrics.exchange_latency_ms.labels(
                    exchange=self.exchange, symbol=symbol, stream=stream_type,
                ).observe(latency)

            self._last_received_at[(symbol, stream_type)] = time.time_ns()
            await handler.handle(symbol, raw_text, exchange_ts, seq)

    async def _depth_resync(self, symbol: str) -> None:
        """Depth resync flow per spec Section 7.2."""
        _handler = self.handlers.get("depth")
        if not isinstance(_handler, DepthHandler):
            return
        depth_handler: DepthHandler = _handler

        logger.info("depth_resync_starting", symbol=symbol)

        # Precondition: wait until producer is healthy (spec 7.2)
        _max_wait = 60  # seconds
        _waited = 0
        while not self.producer.is_healthy_for_resync() and _waited < _max_wait:
            logger.warning("depth_resync_waiting_for_producer", symbol=symbol,
                           waited_s=_waited)
            await asyncio.sleep(2)
            _waited += 2
        if not self.producer.is_healthy_for_resync():
            logger.error("depth_resync_aborted_producer_unhealthy", symbol=symbol)
            self.producer.emit_gap(
                symbol=symbol, stream="depth",
                session_seq=self._next_seq(symbol, "depth"),
                reason="pu_chain_break",
                detail=f"Depth resync aborted: producer unhealthy after {_max_wait}s wait",
            )
            return

        depth_handler.reset(symbol)

        # Check if the other collector's depth topic has recent diffs.
        # If so, seed the pu chain from there and skip the REST snapshot.
        if self._brokers:
            backup_topic = other_depth_topic(self.producer.topic_prefix, self.exchange)
            backup_u = read_last_depth_update_id(
                brokers=self._brokers,
                topic=backup_topic,
                symbol=symbol,
                max_age_seconds=30,
            )
            if backup_u is not None:
                depth_handler.set_sync_point(symbol, backup_u)
                logger.info("depth_resync_skipped_snapshot",
                            symbol=symbol, backup_last_u=backup_u)
                return

        retries = 3
        for attempt in range(retries):
            try:
                url = self.adapter.build_snapshot_url(symbol)
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        resp.raise_for_status()
                        raw_text = await resp.text()

                last_update_id = self.adapter.parse_snapshot_last_update_id(raw_text)
                depth_handler.set_sync_point(symbol, last_update_id)

                # Produce the resync snapshot to the archive
                received_at = time.time_ns()
                env = create_data_envelope(
                    exchange=self.exchange, symbol=symbol, stream="depth_snapshot",
                    raw_text=raw_text, exchange_ts=int(received_at / 1_000_000),
                    collector_session_id=self.collector_session_id,
                    session_seq=self._next_seq(symbol, "depth_snapshot"),
                    received_at=received_at,
                )
                self.producer.produce(env)
                logger.info("depth_resync_complete", symbol=symbol,
                            last_update_id=last_update_id)
                return
            except Exception as e:
                logger.warning("depth_resync_snapshot_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        # All retries exhausted
        logger.error("depth_resync_exhausted", symbol=symbol)
        self.producer.emit_gap(
            symbol=symbol, stream="depth",
            session_seq=self._next_seq(symbol, "depth"),
            reason="pu_chain_break",
            detail=f"Depth resync failed after {retries} retries, waiting for periodic snapshot",
        )

    def _emit_disconnect_gaps(self, socket_name: str) -> None:
        """Emit gap records for all symbol/stream combos on this socket.

        Always emits — session_seq continuity cannot prove Binance data
        continuity since session_seq is our internal counter. The actual
        record count is determined later by the integrity checker or backfill.
        """
        now = time.time_ns()
        if socket_name == "public":
            affected = _PUBLIC_STREAMS
        elif socket_name == "market":
            affected = _MARKET_STREAMS
        else:
            return

        for symbol in self.symbols:
            for stream in affected:
                if stream not in self.enabled_streams:
                    continue
                gap_start = self._last_received_at.get((symbol, stream), now)
                seq = self._next_seq(symbol, stream)
                self.producer.emit_gap(
                    symbol=symbol, stream=stream,
                    session_seq=seq,
                    reason="ws_disconnect",
                    detail=f"WebSocket {socket_name} disconnected",
                    gap_start_ts=gap_start, gap_end_ts=now,
                )
                # Advance the stream handler's seq tracker past the gap
                # envelope's seq so the next data message doesn't trigger
                # a spurious session_seq_skip.
                handler = self.handlers.get(stream)
                if handler is not None and hasattr(handler, "_seq_trackers"):
                    tracker = handler._seq_trackers.get(symbol)
                    if tracker is not None:
                        tracker._last_seq = seq
