from __future__ import annotations

from typing import Awaitable, Callable

from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


async def start_health_server(
    port: int,
    ready_check: Callable[[], Awaitable[dict[str, bool]]],
) -> None:
    """Start an HTTP server with /health, /ready, and /metrics endpoints."""

    async def _health(request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(request: web.Request) -> web.Response:
        checks = await ready_check()
        status = 200 if all(checks.values()) else 503
        return web.json_response(checks, status=status)

    async def _metrics(request: web.Request) -> web.Response:
        return web.Response(
            body=generate_latest(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    app = web.Application()
    app.router.add_get("/health", _health)
    app.router.add_get("/ready", _ready)
    app.router.add_get("/metrics", _metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
