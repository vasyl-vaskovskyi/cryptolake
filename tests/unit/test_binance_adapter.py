class TestBinanceURLBuilding:
    def test_ws_urls_two_sockets(self, binance_adapter):
        urls = binance_adapter.get_ws_urls(
            symbols=["btcusdt", "ethusdt"],
            streams=["trades", "depth", "bookticker", "funding_rate", "liquidations"],
        )
        # Should produce two sockets: public and market
        assert "public" in urls
        assert "market" in urls

    def test_ws_public_socket_streams(self, binance_adapter):
        urls = binance_adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["depth", "bookticker"],
        )
        url = urls["public"]
        assert "btcusdt@depth@100ms" in url
        assert "btcusdt@bookTicker" in url
        assert url.startswith("wss://fstream.binance.com/stream?streams=")

    def test_ws_market_socket_streams(self, binance_adapter):
        urls = binance_adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["trades", "funding_rate", "liquidations"],
        )
        url = urls["market"]
        assert "btcusdt@aggTrade" in url
        assert "btcusdt@markPrice@1s" in url
        assert "btcusdt@forceOrder" in url

    def test_ws_no_public_streams_omits_socket(self, binance_adapter):
        urls = binance_adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["trades"],  # only market streams
        )
        assert "public" not in urls
        assert "market" in urls

    def test_snapshot_url(self, binance_adapter):
        url = binance_adapter.build_snapshot_url("btcusdt", limit=1000)
        assert url == "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"

    def test_open_interest_url(self, binance_adapter):
        url = binance_adapter.build_open_interest_url("btcusdt")
        assert url == "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"


class TestBinanceStreamRouting:
    def test_route_aggtrade(self, binance_adapter):
        import orjson

        frame = orjson.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade", "E": 100, "s": "BTCUSDT", "p": "1.0", "q": "1.0",
                     "a": 1, "f": 1, "l": 1, "T": 100, "m": True}
        }).decode()
        stream_type, symbol, raw_data = binance_adapter.route_stream(frame)
        assert stream_type == "trades"
        assert symbol == "btcusdt"
        # raw_data should be valid JSON
        parsed = orjson.loads(raw_data)
        assert parsed["e"] == "aggTrade"

    def test_route_depth(self, binance_adapter):
        import orjson

        frame = orjson.dumps({
            "stream": "btcusdt@depth@100ms",
            "data": {"e": "depthUpdate", "E": 100, "s": "BTCUSDT",
                     "U": 1, "u": 2, "pu": 0, "b": [], "a": []}
        }).decode()
        stream_type, symbol, raw_data = binance_adapter.route_stream(frame)
        assert stream_type == "depth"
        assert symbol == "btcusdt"

    def test_route_bookticker(self, binance_adapter):
        import orjson

        frame = orjson.dumps({
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "s": "BTCUSDT", "b": "1.0", "a": "2.0",
                     "B": "1.0", "A": "1.0", "u": 1, "E": 100, "T": 100}
        }).decode()
        stream_type, symbol, raw_data = binance_adapter.route_stream(frame)
        assert stream_type == "bookticker"
        assert symbol == "btcusdt"

    def test_route_markprice(self, binance_adapter):
        import orjson

        frame = orjson.dumps({
            "stream": "btcusdt@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": 100, "s": "BTCUSDT",
                     "p": "1.0", "i": "1.0", "P": "1.0", "r": "0.0001", "T": 100}
        }).decode()
        stream_type, symbol, raw_data = binance_adapter.route_stream(frame)
        assert stream_type == "funding_rate"
        assert symbol == "btcusdt"

    def test_route_forceorder(self, binance_adapter):
        import orjson

        frame = orjson.dumps({
            "stream": "btcusdt@forceOrder",
            "data": {"e": "forceOrder", "E": 100, "o": {"s": "BTCUSDT", "S": "SELL",
                     "o": "LIMIT", "f": "IOC", "q": "1", "p": "1", "ap": "1",
                     "X": "FILLED", "l": "1", "z": "1", "T": 100}}
        }).decode()
        stream_type, symbol, raw_data = binance_adapter.route_stream(frame)
        assert stream_type == "liquidations"
        assert symbol == "btcusdt"


class TestBinanceRawTextExtraction:
    def test_raw_text_preserves_original_bytes(self, binance_adapter):
        """raw_text must be extracted via string slicing, not re-serialized.
        Key ordering, whitespace, and number formatting must be preserved."""
        # Deliberately use non-canonical formatting to test fidelity
        inner = '{"e":"aggTrade","E":100,"p":"0.00100000","q":"1.000"}'
        frame = '{"stream":"btcusdt@aggTrade","data":' + inner + '}'
        _, _, raw_data = binance_adapter.route_stream(frame)
        assert raw_data == inner

    def test_raw_text_unicode_escapes_preserved(self, binance_adapter):
        inner = '{"e":"aggTrade","E":100,"note":"\\u0048ello"}'
        frame = '{"stream":"btcusdt@aggTrade","data":' + inner + '}'
        _, _, raw_data = binance_adapter.route_stream(frame)
        assert raw_data == inner


class TestBinanceTimestampExtraction:
    def test_extract_ts_aggtrade(self, binance_adapter):
        raw = '{"e":"aggTrade","E":1741689600120,"s":"BTCUSDT"}'
        ts = binance_adapter.extract_exchange_ts("trades", raw)
        assert ts == 1741689600120

    def test_extract_ts_depth(self, binance_adapter):
        raw = '{"e":"depthUpdate","E":1741689600200,"s":"BTCUSDT","U":1,"u":2,"pu":0}'
        ts = binance_adapter.extract_exchange_ts("depth", raw)
        assert ts == 1741689600200

    def test_extract_ts_bookticker_missing_E_fallback(self, binance_adapter):
        """If bookTicker has no E field, return None to signal fallback to received_at"""
        raw = '{"e":"bookTicker","s":"BTCUSDT","b":"1.0","a":"2.0"}'
        ts = binance_adapter.extract_exchange_ts("bookticker", raw)
        assert ts is None

    def test_extract_ts_depth_snapshot_returns_none(self, binance_adapter):
        """depth_snapshot has no exchange_ts; caller must derive from received_at per spec 3.2.3"""
        raw = '{"lastUpdateId":1005,"bids":[],"asks":[]}'
        ts = binance_adapter.extract_exchange_ts("depth_snapshot", raw)
        assert ts is None

    def test_extract_ts_open_interest(self, binance_adapter):
        raw = '{"openInterest":"12345","symbol":"BTCUSDT","time":1741689600700}'
        ts = binance_adapter.extract_exchange_ts("open_interest", raw)
        assert ts == 1741689600700


class TestBinanceDepthParsing:
    def test_parse_depth_update_ids(self, binance_adapter):
        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":1000,"u":1005,"pu":999,"b":[],"a":[]}'
        U, u, pu = binance_adapter.parse_depth_update_ids(raw)
        assert U == 1000
        assert u == 1005
        assert pu == 999

    def test_parse_snapshot_last_update_id(self, binance_adapter):
        raw = '{"lastUpdateId":1005,"bids":[],"asks":[]}'
        assert binance_adapter.parse_snapshot_last_update_id(raw) == 1005
