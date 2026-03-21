from __future__ import annotations

import asyncio


async def cancel_tasks(tasks: list[asyncio.Task]) -> None:
    """Cancel all tasks, await their completion, and clear the list."""
    for t in tasks:
        if not t.done():
            t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    tasks.clear()
