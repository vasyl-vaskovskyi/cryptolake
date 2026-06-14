"""
Minimal Binance USD-M Futures WS mock.

Behavior:
- Accepts a single combined-streams WS connection on /ws.
- Reads a SUBSCRIBE message of the form {"method":"SUBSCRIBE","params":[...],"id":<int>}.
- Responds with {"result":null,"id":<id>}.
- Replays fixture.jsonl line-by-line at ~10 lines/second.
- Loops the fixture until the client disconnects.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import websockets


FIXTURE = Path(__file__).parent / "fixture.jsonl"
REPLAY_RATE_HZ = float(os.environ.get("REPLAY_RATE_HZ", "10"))


async def serve(ws):
    print(f"[mock-ws] client connected: {ws.remote_address}", flush=True)
    try:
        sub = await asyncio.wait_for(ws.recv(), timeout=10.0)
    except asyncio.TimeoutError:
        print("[mock-ws] no SUBSCRIBE within 10s; closing", flush=True)
        return
    try:
        sub_obj = json.loads(sub)
    except json.JSONDecodeError:
        print(f"[mock-ws] non-JSON SUBSCRIBE: {sub!r}", flush=True)
        return
    if sub_obj.get("method") != "SUBSCRIBE":
        print(f"[mock-ws] first message not SUBSCRIBE: {sub_obj}", flush=True)
        return
    await ws.send(json.dumps({"result": None, "id": sub_obj.get("id")}))
    print(f"[mock-ws] subscribed to {len(sub_obj.get('params', []))} streams", flush=True)

    lines = FIXTURE.read_text().splitlines()
    delay = 1.0 / REPLAY_RATE_HZ
    while True:
        for line in lines:
            await ws.send(line)
            await asyncio.sleep(delay)


async def main():
    host = os.environ.get("MOCK_WS_HOST", "0.0.0.0")
    port = int(os.environ.get("MOCK_WS_PORT", "9001"))
    print(f"[mock-ws] listening on {host}:{port}", flush=True)
    async with websockets.serve(serve, host, port):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[mock-ws] shutting down", flush=True)
        sys.exit(0)
