from __future__ import annotations

import asyncio
import os
import signal
from typing import Callable

import uvloop

from src.common.logging import setup_logging


def run_service(
    service_factory: Callable[[str], object],
    config_env: str = "CONFIG_PATH",
    default_config: str = "config/config.yaml",
) -> None:
    """Shared bootstrap for async services (collector, writer).

    The service_factory must accept a config_path string and return
    an object with async start() and shutdown() methods.
    """
    setup_logging()
    uvloop.install()

    config_path = os.environ.get(config_env, default_config)
    service = service_factory(config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    shutdown_task = None

    def _signal_handler():
        nonlocal shutdown_task
        if shutdown_task is None:
            shutdown_task = loop.create_task(service.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(service.start())
    finally:
        if shutdown_task is not None and not shutdown_task.done():
            loop.run_until_complete(shutdown_task)
        loop.close()
