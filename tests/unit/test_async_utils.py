import asyncio
import pytest
from src.common.async_utils import cancel_tasks


@pytest.mark.asyncio
async def test_cancel_tasks_cancels_and_clears():
    ran = False

    async def sleeper():
        nonlocal ran
        await asyncio.sleep(100)
        ran = True

    tasks = [asyncio.create_task(sleeper())]
    await cancel_tasks(tasks)
    assert tasks == []
    assert not ran


@pytest.mark.asyncio
async def test_cancel_tasks_handles_empty_list():
    tasks: list[asyncio.Task] = []
    await cancel_tasks(tasks)
    assert tasks == []
