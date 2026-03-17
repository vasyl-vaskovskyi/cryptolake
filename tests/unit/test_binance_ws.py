from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from src.exchanges.binance import BinanceAdapter

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestBinanceWSFixtures:
    def test_route_all_fixture_types(self) -> None:
        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )

        cases = [
            ("binance_aggtrade.json", "btcusdt@aggTrade", "trades"),
            ("binance_depth_diff.json", "btcusdt@depth@100ms", "depth"),
            ("binance_bookticker.json", "btcusdt@bookTicker", "bookticker"),
            ("binance_markprice.json", "btcusdt@markPrice@1s", "funding_rate"),
            ("binance_forceorder.json", "btcusdt@forceOrder", "liquidations"),
        ]

        for fixture_name, stream_key, expected_type in cases:
            inner = (FIXTURES_DIR / fixture_name).read_text()
            frame = orjson.dumps({"stream": stream_key, "data": orjson.loads(inner)}).decode()
            stream_type, symbol, raw_data = adapter.route_stream(frame)

            assert stream_type == expected_type, f"failed for {fixture_name}"
            assert symbol == "btcusdt"
            assert orjson.loads(raw_data) == orjson.loads(inner)
