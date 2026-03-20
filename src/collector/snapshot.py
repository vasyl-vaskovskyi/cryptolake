from __future__ import annotations

import asyncio
import time

import aiohttp
import structlog

from src.collector.producer import CryptoLakeProducer
from src.collector.streams.depth import DepthHandler
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


def parse_interval_seconds(interval: str) -> int:
    """Parse '5m', '1m', '30s' to seconds."""
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("s"):
        return int(interval[:-1])
    raise ValueError(f"Invalid interval: {interval}")


class SnapshotScheduler:
    """Periodically fetches depth snapshots via REST API."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        depth_handler: DepthHandler,
        symbols: list[str],
        default_interval: str = "5m",
        overrides: dict[str, str] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.depth_handler = depth_handler
        self.symbols = symbols
        self.default_interval_s = parse_interval_seconds(default_interval)
        self.overrides = {
            s: parse_interval_seconds(v) for s, v in (overrides or {}).items()
        }
        self._session: aiohttp.ClientSession | None = None
        self._seq_counters: dict[str, int] = {s: 0 for s in symbols}
        self._running = False
        self._stop_event: asyncio.Event | None = None
        self._tasks: list[asyncio.Task] = []

    def _get_interval(self, symbol: str) -> int:
        return self.overrides.get(symbol, self.default_interval_s)

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15, sock_connect=5),
        )
        self._running = True
        self._stop_event = asyncio.Event()
        self._tasks = []
        for i, symbol in enumerate(self.symbols):
            interval = self._get_interval(symbol)
            delay = (interval / len(self.symbols)) * i
            self._tasks.append(asyncio.create_task(self._poll_loop(symbol, interval, delay)))
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        self._running = False
        if self._stop_event:
            self._stop_event.set()
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session:
            await self._session.close()

    async def fetch_snapshot(self, symbol: str, retries: int = 3) -> str | None:
        """Fetch a single depth snapshot. Returns raw_text or None on total failure."""
        assert self._session is not None, "call start() first"
        url = self.adapter.build_snapshot_url(symbol)
        for attempt in range(retries):
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    raw_text = await resp.text()
                    collector_metrics.snapshots_taken_total.labels(
                        exchange=self.exchange, symbol=symbol,
                    ).inc()
                    return raw_text
            except Exception as e:
                logger.warning("snapshot_fetch_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                collector_metrics.snapshots_failed_total.labels(
                    exchange=self.exchange, symbol=symbol,
                ).inc()
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None

    async def _poll_loop(self, symbol: str, interval: int, initial_delay: float) -> None:
        assert self._stop_event is not None, "call start() first"
        if initial_delay > 0:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=initial_delay)
                return  # stop requested
            except asyncio.TimeoutError:
                pass
        while self._running:
            await self._take_snapshot(symbol)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                return  # stop requested
            except asyncio.TimeoutError:
                pass

    async def _take_snapshot(self, symbol: str) -> None:
        raw_text = await self.fetch_snapshot(symbol)
        if raw_text is None:
            # All retries exhausted
            collector_metrics.gaps_detected_total.labels(
                exchange=self.exchange, symbol=symbol, stream="depth_snapshot",
            ).inc()
            seq = self._seq_counters[symbol]
            self._seq_counters[symbol] += 1
            gap = create_gap_envelope(
                exchange=self.exchange,
                symbol=symbol,
                stream="depth_snapshot",
                collector_session_id=self.collector_session_id,
                session_seq=seq,
                gap_start_ts=time.time_ns(),
                gap_end_ts=time.time_ns(),
                reason="snapshot_poll_miss",
                detail="Depth snapshot poll failed after all retries",
            )
            self.producer.produce(gap)
            return

        received_at = time.time_ns()
        exchange_ts = int(received_at / 1_000_000)  # no E in REST snapshot
        seq = self._seq_counters[symbol]
        self._seq_counters[symbol] += 1

        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="depth_snapshot",
            raw_text=raw_text,
            exchange_ts=exchange_ts,
            collector_session_id=self.collector_session_id,
            session_seq=seq,
            received_at=received_at,
        )
        self.producer.produce(envelope)
        # NOTE: Periodic snapshots are archived checkpoints for later reconstruction.
        # They must NOT call depth_handler.set_sync_point() — that would reset the
        # live pu chain validation mid-stream. Live sync is only reset during the
        # depth resync flow (reconnect or pu_chain_break, see Section 7.2).
