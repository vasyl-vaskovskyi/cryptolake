"""Minimal WebSocket echo-style server for the gate 3 spike.

Reads payloads.jsonl (one JSON object per line, verbatim bytes), accepts one
client connection, sends each line as a text frame in order, then closes.
Exits after the client disconnects.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import websockets


async def serve_once(payloads: list[str], port: int) -> None:
    done = asyncio.Event()

    async def handler(ws: websockets.ServerConnection) -> None:
        for line in payloads:
            await ws.send(line)
        await asyncio.sleep(0.25)  # let Java drain before close
        await ws.close()
        done.set()

    async with websockets.serve(handler, "127.0.0.1", port):
        print(f"server listening on 127.0.0.1:{port} with {len(payloads)} payloads", flush=True)
        await done.wait()
    print("server done", flush=True)


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    payloads_file = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "payloads.jsonl"
    payloads = [ln for ln in payloads_file.read_text().splitlines() if ln]
    asyncio.run(serve_once(payloads, port))
    return 0


if __name__ == "__main__":
    sys.exit(main())
