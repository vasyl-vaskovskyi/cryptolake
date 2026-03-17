from __future__ import annotations

import asyncio
import os
import signal
import time
from pathlib import Path

import structlog
import uvloop
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.common.config import load_config
from src.common.logging import setup_logging
from src.collector.connection import WebSocketManager
from src.collector.producer import CryptoLakeProducer
from src.collector.snapshot import SnapshotScheduler, parse_interval_seconds
from src.collector.streams.trades import TradesHandler
from src.collector.streams.depth import DepthHandler
from src.collector.streams.bookticker import BookTickerHandler
from src.collector.streams.funding_rate import FundingRateHandler
from src.collector.streams.liquidations import LiquidationsHandler
from src.collector.streams.open_interest import OpenInterestPoller
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


class Collector:
    def __init__(self, config_path: str):
        self.config = load_config(Path(config_path))
        self.exchange_cfg = self.config.exchanges.binance
        self.session_id = f"{self.exchange_cfg.collector_id}_{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
        self.adapter = BinanceAdapter(
            ws_base=self.exchange_cfg.ws_base,
            rest_base=self.exchange_cfg.rest_base,
        )
        self.producer = CryptoLakeProducer(
            brokers=self.config.redpanda.brokers,
            exchange="binance",
            collector_session_id=self.session_id,
        )
        self.enabled_streams = self.exchange_cfg.get_enabled_streams()
        self.symbols = self.exchange_cfg.symbols

        # Build handlers
        self.handlers: dict[str, object] = {}
        if "trades" in self.enabled_streams:
            self.handlers["trades"] = TradesHandler("binance", self.session_id, self.producer)
        if "depth" in self.enabled_streams:
            self.handlers["depth"] = DepthHandler(
                "binance", self.session_id, self.producer, self.adapter, self.symbols,
                on_pu_chain_break=self._on_pu_chain_break)
        if "bookticker" in self.enabled_streams:
            self.handlers["bookticker"] = BookTickerHandler("binance", self.session_id, self.producer)
        if "funding_rate" in self.enabled_streams:
            self.handlers["funding_rate"] = FundingRateHandler("binance", self.session_id, self.producer)
        if "liquidations" in self.enabled_streams:
            self.handlers["liquidations"] = LiquidationsHandler("binance", self.session_id, self.producer)

        # Initialize optional components to None (created conditionally in start())
        self.snapshot_scheduler = None
        self.oi_poller = None

        self.ws_manager = WebSocketManager(
            exchange="binance",
            collector_session_id=self.session_id,
            adapter=self.adapter,
            producer=self.producer,
            handlers=self.handlers,
            symbols=self.symbols,
            enabled_streams=self.enabled_streams,
        )
        self._tasks: list[asyncio.Task] = []
        # Wire producer overflow to WS backpressure
        self.producer._on_overflow = lambda ex, sym, st: self._on_producer_overflow()

    def _on_producer_overflow(self) -> None:
        self.ws_manager._consecutive_drops += 1

    def _on_pu_chain_break(self, symbol: str) -> None:
        """Callback from DepthHandler when pu chain breaks -- triggers depth resync."""
        loop = asyncio.get_running_loop()
        loop.create_task(self.ws_manager._depth_resync(symbol))

    async def start(self) -> None:
        logger.info("collector_starting", session_id=self.session_id,
                     symbols=self.symbols, streams=self.enabled_streams)
        self._tasks = []

        # WebSocket streams
        ws_task = asyncio.create_task(self.ws_manager.start())
        self._tasks.append(ws_task)

        # Snapshot scheduler
        if "depth" in self.enabled_streams:
            depth_handler = self.handlers["depth"]
            self.snapshot_scheduler = SnapshotScheduler(
                exchange="binance",
                collector_session_id=self.session_id,
                producer=self.producer,
                adapter=self.adapter,
                depth_handler=depth_handler,
                symbols=self.symbols,
                default_interval=self.exchange_cfg.depth.snapshot_interval,
                overrides=self.exchange_cfg.depth.snapshot_overrides,
            )
            self._tasks.append(asyncio.create_task(self.snapshot_scheduler.start()))

        # Open interest poller
        if "open_interest" in self.enabled_streams:
            self.oi_poller = OpenInterestPoller(
                exchange="binance",
                collector_session_id=self.session_id,
                producer=self.producer,
                adapter=self.adapter,
                symbols=self.symbols,
                poll_interval_seconds=parse_interval_seconds(
                    self.exchange_cfg.open_interest.poll_interval),
            )
            self._tasks.append(asyncio.create_task(self.oi_poller.start()))

        # Health/metrics HTTP server
        self._tasks.append(asyncio.create_task(self._start_http()))

        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        logger.info("collector_shutting_down")
        await self.ws_manager.stop()
        if self.snapshot_scheduler:
            await self.snapshot_scheduler.stop()
        if self.oi_poller:
            await self.oi_poller.stop()
        self.producer.flush(timeout=10.0)
        # Cancel any remaining top-level tasks
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("collector_shutdown_complete")

    async def _start_http(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.config.monitoring.prometheus_port)
        await site.start()

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(self, request: web.Request) -> web.Response:
        loop = asyncio.get_running_loop()
        producer_ok = await loop.run_in_executor(None, self.producer.is_connected)
        checks = {
            "ws_connected": self.ws_manager.is_connected(),
            "producer_connected": producer_ok,
        }
        status = 200 if all(checks.values()) else 503
        return web.json_response(checks, status=status)

    async def _metrics(self, request: web.Request) -> web.Response:
        return web.Response(
            body=generate_latest(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )


def main():
    setup_logging()
    uvloop.install()

    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    collector = Collector(config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _signal_handler():
        loop.create_task(collector.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(collector.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
