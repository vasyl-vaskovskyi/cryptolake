from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import structlog
import uvloop
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.common.config import load_config
from src.common.logging import setup_logging
from src.common.system_identity import get_host_boot_id
from src.writer.buffer_manager import BufferManager
from src.writer.compressor import ZstdFrameCompressor
from src.writer.consumer import WriterConsumer
from src.writer.host_lifecycle_reader import (
    DEFAULT_LEDGER_PATH,
    load_host_evidence,
)
from src.writer.state_manager import ComponentRuntimeState, StateManager

logger = structlog.get_logger()


class Writer:
    def __init__(self, config_path: str):
        self.config = load_config(Path(config_path))
        exchange_cfg = self.config.exchanges.binance
        self.enabled_streams = exchange_cfg.get_enabled_streams()
        if exchange_cfg.writer_streams_override:
            self.enabled_streams = exchange_cfg.writer_streams_override

        self.topics = [f"binance.{s}" for s in self.enabled_streams]
        self.state_manager = StateManager(self.config.database.url)
        self.compressor = ZstdFrameCompressor(level=self.config.writer.compression_level)
        self.buffer_manager = BufferManager(
            base_dir=self.config.writer.base_dir,
            flush_messages=self.config.writer.flush_messages,
            flush_interval_seconds=self.config.writer.flush_interval_seconds,
        )
        # Load host lifecycle evidence (Phase 2) — best-effort
        ledger_path = Path(
            os.environ.get("LIFECYCLE_LEDGER_PATH", str(DEFAULT_LEDGER_PATH))
        )
        host_evidence = None
        try:
            host_evidence = load_host_evidence(ledger_path)
            if not host_evidence.is_empty:
                logger.info("host_evidence_loaded", ledger_path=str(ledger_path))
        except Exception:
            logger.warning("host_evidence_load_failed", ledger_path=str(ledger_path),
                           exc_info=True)

        self.consumer = WriterConsumer(
            brokers=self.config.redpanda.brokers,
            topics=self.topics,
            group_id="cryptolake-writer",
            buffer_manager=self.buffer_manager,
            compressor=self.compressor,
            state_manager=self.state_manager,
            base_dir=self.config.writer.base_dir,
            host_evidence=host_evidence,
        )

        # Writer runtime metadata
        self._boot_id = get_host_boot_id()
        now = datetime.now(timezone.utc)
        self._instance_id = f"writer_{now.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        self._started_at = now.isoformat()

    async def start(self) -> None:
        logger.info("writer_starting", topics=self.topics, boot_id=self._boot_id)
        await self.state_manager.connect()

        # Record writer runtime state on start
        await self.state_manager.upsert_component_runtime(ComponentRuntimeState(
            component="writer",
            instance_id=self._instance_id,
            host_boot_id=self._boot_id,
            started_at=self._started_at,
            last_heartbeat_at=self._started_at,
        ))
        logger.info("writer_runtime_registered",
                     instance_id=self._instance_id, boot_id=self._boot_id)

        await self.consumer.start()
        await asyncio.gather(
            self.consumer.consume_loop(),
            self._start_http(),
        )

    async def shutdown(self) -> None:
        logger.info("writer_shutting_down")

        # Mark clean shutdown in durable state
        try:
            await self.state_manager.mark_component_clean_shutdown(
                component="writer",
                instance_id=self._instance_id,
            )
            logger.info("writer_clean_shutdown_marked", instance_id=self._instance_id)
        except Exception:
            logger.warning("writer_clean_shutdown_mark_failed", exc_info=True)

        await self.consumer.stop()
        await self.state_manager.close()
        logger.info("writer_shutdown_complete")

    async def _start_http(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        runner = web.AppRunner(app)
        await runner.setup()
        port = self.config.monitoring.prometheus_port + 1  # writer on 8001
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(self, request: web.Request) -> web.Response:
        checks = {"consumer_connected": self.consumer.is_connected(), "storage_writable": True}
        try:
            Path(self.config.writer.base_dir).mkdir(parents=True, exist_ok=True)
            test_file = Path(self.config.writer.base_dir) / ".write_test"
            test_file.write_text("ok")
            test_file.unlink()
        except Exception:
            checks["storage_writable"] = False
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
    writer = Writer(config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _signal_handler():
        loop.create_task(writer.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(writer.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
