"""Side tap: captures market-only WebSocket frames + writes them in the same
format as the collector's FrameTap. Bypasses the WebSocketManager bug where
the second simultaneous WS connection silently delivers no frames.

Run inside the collector container, alongside the running collector:
  docker exec -d cryptolake-collector-1 /app/.venv/bin/python /tmp/side_tap.py
"""
import asyncio
import itertools
import json
import os
import sys
import time
from pathlib import Path

import websockets

sys.path.insert(0, "/app")
from src.common.envelope import create_data_envelope
from src.exchanges.binance import BinanceAdapter

URL = "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder"
TAP_ROOT = Path(os.environ.get("COLLECTOR__TAP_OUTPUT_DIR", "/tap"))
SESSION_ID = os.environ.get("SIDE_TAP_SESSION", "side_tap_session")
EXCHANGE = "binance"

class StreamTap:
    def __init__(self, root: Path, stream: str) -> None:
        self.dir = root / stream
        self.dir.mkdir(parents=True, exist_ok=True)
        self.stream = stream
        self._counter = itertools.count()

    def write(self, raw: bytes, envelope: dict) -> None:
        seq = next(self._counter)
        stem = f"{time.time_ns():020d}-{seq:06d}-{self.stream}"
        (self.dir / f"{stem}.raw").write_bytes(raw)
        (self.dir / f"{stem}.json").write_text(json.dumps(envelope))


async def main():
    adapter = BinanceAdapter("wss://fstream.binance.com", "https://fapi.binance.com")
    taps: dict[str, StreamTap] = {}
    seqs: dict[tuple[str, str], int] = {}
    while True:
        try:
            print(f"side_tap connecting to {URL}", flush=True)
            async with websockets.connect(URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                print("side_tap connected", flush=True)
                async for raw_frame in ws:
                    try:
                        stream_type, symbol, raw_text = adapter.route_stream(raw_frame)
                    except Exception as e:
                        print(f"route_failed: {e}", flush=True)
                        continue
                    ts = adapter.extract_exchange_ts(stream_type, raw_text)
                    key = (symbol, stream_type)
                    seqs[key] = seqs.get(key, -1) + 1
                    envelope = create_data_envelope(
                        exchange=EXCHANGE,
                        symbol=symbol,
                        stream=stream_type,
                        raw_text=raw_text,
                        exchange_ts=ts or 0,
                        collector_session_id=SESSION_ID,
                        session_seq=seqs[key],
                    )
                    tap = taps.get(stream_type)
                    if tap is None:
                        tap = StreamTap(TAP_ROOT, stream_type)
                        taps[stream_type] = tap
                    tap.write(raw_text.encode(), envelope)
        except Exception as e:
            print(f"side_tap error: {type(e).__name__}: {e} — reconnecting in 2s", flush=True)
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
