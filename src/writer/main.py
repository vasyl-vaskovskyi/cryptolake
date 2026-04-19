from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

from src.common.config import load_config
from src.common.health_server import start_health_server
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
            # Filter to events from the last 24 hours — the restart window
            # cannot exceed the time since the previous writer lifecycle.
            # The ledger is pruned to 7 days on agent startup, but we only
            # need recent events for classification.
            window_start = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            host_evidence = load_host_evidence(
                ledger_path, window_start_iso=window_start,
            )
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
            gap_filter_grace_period_seconds=self.config.writer.gap_filter.grace_period_seconds,
            gap_filter_snapshot_miss_grace_seconds=self.config.writer.gap_filter.snapshot_miss_grace_seconds,
        )

        # Writer runtime metadata
        self._boot_id = get_host_boot_id()
        now = datetime.now(timezone.utc)
        self._instance_id = f"writer_{now.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        self._started_at = now.isoformat()

    async def start(self) -> None:
        logger.info("writer_starting", topics=self.topics, boot_id=self._boot_id)
        await self.state_manager.connect()

        # Start the consumer FIRST so it loads previous component state
        # (including the old boot ID) before we overwrite it with the current run.
        await self.consumer.start()

        # Now record the current writer runtime state
        await self.state_manager.upsert_component_runtime(ComponentRuntimeState(
            component="writer",
            instance_id=self._instance_id,
            host_boot_id=self._boot_id,
            started_at=self._started_at,
            last_heartbeat_at=self._started_at,
        ))
        logger.info("writer_runtime_registered",
                     instance_id=self._instance_id, boot_id=self._boot_id)
        await asyncio.gather(
            self.consumer.consume_loop(),
            start_health_server(self.config.monitoring.prometheus_port + 1, self._ready_checks),
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

    async def _ready_checks(self) -> dict[str, bool]:
        checks = {"consumer_connected": self.consumer.is_connected(), "storage_writable": True}
        try:
            Path(self.config.writer.base_dir).mkdir(parents=True, exist_ok=True)
            test_file = Path(self.config.writer.base_dir) / ".write_test"
            test_file.write_text("ok")
            test_file.unlink()
        except Exception:
            checks["storage_writable"] = False
        return checks


def main():
    from src.common.service_runner import run_service

    run_service(Writer)


if __name__ == "__main__":
    main()
