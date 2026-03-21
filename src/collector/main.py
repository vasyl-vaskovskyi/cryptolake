from __future__ import annotations

import asyncio
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog
import uvloop
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.common.async_utils import cancel_tasks
from src.common.config import load_config
from src.common.logging import setup_logging
from src.common.system_identity import get_host_boot_id
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
from src.writer.state_manager import ComponentRuntimeState, StateManager

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
        producer_cfg = self.config.redpanda.producer
        self.producer = CryptoLakeProducer(
            brokers=self.config.redpanda.brokers,
            exchange="binance",
            collector_session_id=self.session_id,
            max_buffer=producer_cfg.max_buffer,
            buffer_caps=producer_cfg.buffer_caps,
            default_stream_cap=producer_cfg.default_stream_cap,
        )
        self.enabled_streams = self.exchange_cfg.get_enabled_streams()
        self.symbols = self.exchange_cfg.symbols

        # Build handlers
        from src.collector.streams.base import StreamHandler
        self.handlers: dict[str, StreamHandler] = {}
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

        # Lifecycle state (best-effort PG connection for restart-gap classification)
        self._db_url = self.config.database.url
        self._state_manager: StateManager | None = None
        self._lifecycle_state: ComponentRuntimeState | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_interval: float = 30.0  # seconds
        self._active_maintenance_id: str | None = None

    def _on_producer_overflow(self) -> None:
        self.ws_manager._consecutive_drops += 1

    def _on_pu_chain_break(self, symbol: str) -> None:
        """Callback from DepthHandler when pu chain breaks -- triggers depth resync."""
        loop = asyncio.get_running_loop()
        loop.create_task(self.ws_manager._depth_resync(symbol))

    # ── Lifecycle state (best-effort) ──────────────────────────────────

    async def _init_state_manager(self) -> None:
        """Create and connect the lifecycle StateManager.

        Best-effort: if PG is unreachable the collector continues normally.
        """
        try:
            self._state_manager = StateManager(self._db_url)
            await self._state_manager.connect()
        except Exception as exc:
            logger.warning("lifecycle_pg_connect_failed",
                           msg="Collector will continue without lifecycle tracking",
                           error=str(exc))
            self._state_manager = None

    async def _register_lifecycle_start(self) -> None:
        """Upsert a ComponentRuntimeState row for this collector session."""
        now = datetime.now(timezone.utc).isoformat()
        boot_id = get_host_boot_id()

        self._lifecycle_state = ComponentRuntimeState(
            component="collector",
            instance_id=self.session_id,
            host_boot_id=boot_id,
            started_at=now,
            last_heartbeat_at=now,
        )

        if self._state_manager is None:
            return

        try:
            await self._state_manager.upsert_component_runtime(self._lifecycle_state)
            logger.info("lifecycle_registered",
                        instance_id=self.session_id, boot_id=boot_id)
        except Exception:
            logger.warning("lifecycle_register_failed",
                           msg="Will retry via heartbeat")

    async def _send_heartbeat(self) -> None:
        """Update last_heartbeat_at in PG. Best-effort: log and skip on failure."""
        if self._lifecycle_state is None or self._state_manager is None:
            return

        now = datetime.now(timezone.utc).isoformat()
        self._lifecycle_state.last_heartbeat_at = now

        try:
            await self._state_manager.upsert_component_runtime(self._lifecycle_state)
        except Exception:
            logger.warning("lifecycle_heartbeat_failed",
                           msg="PG unreachable, skipping heartbeat")

    async def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats until cancelled."""
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._send_heartbeat()

    async def _mark_lifecycle_shutdown(self) -> None:
        """Mark this collector instance as cleanly shut down.

        Loads the active maintenance intent from PG at shutdown time (the intent
        is typically recorded by the operator *after* the collector starts).
        """
        if self._state_manager is None:
            return

        # Load the maintenance intent now — it may have been created after startup.
        maintenance_id = self._active_maintenance_id
        if maintenance_id is None:
            try:
                intent = await self._state_manager.load_active_maintenance_intent()
                if intent is not None:
                    maintenance_id = intent.maintenance_id
            except Exception:
                pass  # best-effort; fall through with maintenance_id=None

        planned = maintenance_id is not None
        try:
            await self._state_manager.mark_component_clean_shutdown(
                component="collector",
                instance_id=self.session_id,
                planned_shutdown=planned,
                maintenance_id=maintenance_id,
            )
            logger.info("lifecycle_clean_shutdown_marked",
                        instance_id=self.session_id, planned=planned)
        except Exception:
            logger.warning("lifecycle_shutdown_mark_failed",
                           msg="PG unreachable during shutdown")

    async def _close_state_manager(self) -> None:
        """Close the lifecycle PG connection."""
        if self._state_manager is not None:
            try:
                await self._state_manager.close()
            except Exception:
                pass

    # ── Core collector lifecycle ─────────────────────────────────────

    async def start(self) -> None:
        logger.info("collector_starting", session_id=self.session_id,
                     symbols=self.symbols, streams=self.enabled_streams)
        self._tasks = []

        # Register lifecycle state (best-effort)
        await self._init_state_manager()
        await self._register_lifecycle_start()

        # Start heartbeat loop
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._tasks.append(self._heartbeat_task)

        # WebSocket streams
        ws_task = asyncio.create_task(self.ws_manager.start())
        self._tasks.append(ws_task)

        # Snapshot scheduler
        if "depth" in self.enabled_streams:
            depth_handler = self.handlers["depth"]
            assert isinstance(depth_handler, DepthHandler)
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

        # Cancel heartbeat loop first
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self.ws_manager.stop()
        if self.snapshot_scheduler:
            await self.snapshot_scheduler.stop()
        if self.oi_poller:
            await self.oi_poller.stop()
        self.producer.flush(timeout=10.0)

        # Mark clean shutdown (best-effort)
        await self._mark_lifecycle_shutdown()
        await self._close_state_manager()

        # Cancel any remaining top-level tasks
        await cancel_tasks(self._tasks)
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

    shutdown_task = None

    def _signal_handler():
        nonlocal shutdown_task
        if shutdown_task is None:
            shutdown_task = loop.create_task(collector.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(collector.start())
    finally:
        # Ensure the shutdown task completes before closing the loop,
        # so that clean_shutdown_at is written to the database.
        if shutdown_task is not None and not shutdown_task.done():
            loop.run_until_complete(shutdown_task)
        loop.close()


if __name__ == "__main__":
    main()
