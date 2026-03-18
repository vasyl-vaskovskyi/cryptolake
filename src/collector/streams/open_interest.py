from __future__ import annotations

import asyncio
import time

import aiohttp
import structlog

from src.collector.producer import CryptoLakeProducer
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


class OpenInterestPoller:
    """Periodically polls Binance REST API for open interest data."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        symbols: list[str],
        poll_interval_seconds: int = 300,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.symbols = symbols
        self.poll_interval = poll_interval_seconds
        self._session: aiohttp.ClientSession | None = None
        self._seq_counters: dict[str, int] = {s: 0 for s in symbols}
        self._running = False
        self._stop_event: asyncio.Event | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        self._running = True
        self._stop_event = asyncio.Event()
        self._tasks = []
        for i, symbol in enumerate(self.symbols):
            delay = (self.poll_interval / len(self.symbols)) * i
            self._tasks.append(asyncio.create_task(self._poll_loop(symbol, initial_delay=delay)))
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

    async def _poll_loop(self, symbol: str, initial_delay: float = 0) -> None:
        assert self._stop_event is not None, "call start() first"
        assert self._session is not None, "call start() first"
        if initial_delay > 0:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=initial_delay)
                return  # stop requested
            except asyncio.TimeoutError:
                pass
        while self._running:
            await self._poll_once(symbol)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
                return  # stop requested
            except asyncio.TimeoutError:
                pass

    async def _poll_once(self, symbol: str, retries: int = 3) -> None:
        assert self._session is not None, "call start() first"
        url = self.adapter.build_open_interest_url(symbol)
        for attempt in range(retries):
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning("open_interest_rate_limited", symbol=symbol,
                                       retry_after=retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    raw_text = await resp.text()
                    exchange_ts = self.adapter.extract_exchange_ts("open_interest", raw_text)
                    seq = self._seq_counters[symbol]
                    self._seq_counters[symbol] += 1
                    envelope = create_data_envelope(
                        exchange=self.exchange,
                        symbol=symbol,
                        stream="open_interest",
                        raw_text=raw_text,
                        exchange_ts=exchange_ts or int(time.time_ns() / 1_000_000),
                        collector_session_id=self.collector_session_id,
                        session_seq=seq,
                    )
                    self.producer.produce(envelope)
                    return
            except Exception as e:
                logger.warning("open_interest_poll_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        # All retries exhausted — emit gap
        logger.error("open_interest_poll_exhausted", symbol=symbol)
        collector_metrics.gaps_detected_total.labels(
            exchange=self.exchange, symbol=symbol, stream="open_interest",
        ).inc()
        seq = self._seq_counters[symbol]
        self._seq_counters[symbol] += 1
        gap = create_gap_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="open_interest",
            collector_session_id=self.collector_session_id,
            session_seq=seq,
            gap_start_ts=time.time_ns(),
            gap_end_ts=time.time_ns(),
            reason="snapshot_poll_miss",
            detail=f"Open interest poll failed after {retries} retries",
        )
        self.producer.produce(gap)
