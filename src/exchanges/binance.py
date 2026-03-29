from __future__ import annotations

import orjson

# Stream name mapping from Binance subscription key to our stream type
_STREAM_KEY_MAP = {
    "aggTrade": "trades",
    "depth": "depth",
    "bookTicker": "bookticker",
    "markPrice": "funding_rate",
    "forceOrder": "liquidations",
}

# Which streams go on which socket
_PUBLIC_STREAMS = {"depth", "bookticker"}
_MARKET_STREAMS = {"trades", "funding_rate", "liquidations"}

# Binance subscription suffixes per our stream name
_SUBSCRIPTION_MAP = {
    "depth": "@depth@100ms",
    "bookticker": "@bookTicker",
    "trades": "@aggTrade",
    "funding_rate": "@markPrice@1s",
    "liquidations": "@forceOrder",
}


class BinanceAdapter:
    def __init__(self, ws_base: str, rest_base: str):
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")

    def get_ws_urls(self, symbols: list[str], streams: list[str]) -> dict[str, str]:
        public_subs: list[str] = []
        market_subs: list[str] = []

        for symbol in symbols:
            s = symbol.lower()
            for stream in streams:
                suffix = _SUBSCRIPTION_MAP.get(stream)
                if suffix is None:
                    continue
                sub = f"{s}{suffix}"
                if stream in _PUBLIC_STREAMS:
                    public_subs.append(sub)
                elif stream in _MARKET_STREAMS:
                    market_subs.append(sub)

        urls: dict[str, str] = {}
        if public_subs:
            urls["public"] = f"{self.ws_base}/stream?streams={'/'.join(public_subs)}"
        if market_subs:
            urls["market"] = f"{self.ws_base}/stream?streams={'/'.join(market_subs)}"
        return urls

    def route_stream(self, raw_frame: str) -> tuple[str, str, str]:
        """Parse combined stream frame and extract (stream_type, symbol, raw_data_text).

        Uses balanced-brace string slicing for raw_data_text to preserve exact bytes.
        This is the "zero-copy equivalent" mechanism required by spec Section 4.1:
        the original data value's bytes are extracted without re-serialization.
        """
        # Locate the "data": key and extract the value via balanced brace counting
        data_key = '"data":'
        data_idx = raw_frame.index(data_key)
        data_start = data_idx + len(data_key)
        raw_data_text = _extract_data_value(raw_frame, data_start)

        # Parse outer frame for routing only (stream key)
        parsed = orjson.loads(raw_frame)
        stream_key = parsed["stream"]  # e.g., "btcusdt@aggTrade"
        symbol, stream_type = _parse_stream_key(stream_key)

        return stream_type, symbol, raw_data_text

    def extract_exchange_ts(self, stream: str, raw_text: str) -> int | None:
        parsed = orjson.loads(raw_text)
        if stream == "open_interest":
            return parsed.get("time")
        if stream == "depth_snapshot":
            # REST snapshot has no event timestamp
            return None
        return parsed.get("E")

    def build_snapshot_url(self, symbol: str, limit: int = 1000) -> str:
        return f"{self.rest_base}/fapi/v1/depth?symbol={symbol.upper()}&limit={limit}"

    def build_open_interest_url(self, symbol: str) -> str:
        return f"{self.rest_base}/fapi/v1/openInterest?symbol={symbol.upper()}"

    def build_historical_trades_url(
        self, symbol: str, *,
        start_time: int | None = None,
        end_time: int | None = None,
        from_id: int | None = None,
        limit: int = 1000,
    ) -> str:
        base = f"{self.rest_base}/fapi/v1/aggTrades?symbol={symbol.upper()}"
        if from_id is not None:
            return f"{base}&fromId={from_id}&limit={limit}"
        return f"{base}&startTime={start_time}&endTime={end_time}&limit={limit}"

    def build_historical_funding_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/fundingRate"
            f"?symbol={symbol.upper()}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_historical_liquidations_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/allForceOrders"
            f"?symbol={symbol.upper()}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_historical_open_interest_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 500,
    ) -> str:
        return (
            f"{self.rest_base}/futures/data/openInterestHist"
            f"?symbol={symbol.upper()}&period=5m&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def parse_snapshot_last_update_id(self, raw_text: str) -> int:
        parsed = orjson.loads(raw_text)
        return parsed["lastUpdateId"]

    def parse_depth_update_ids(self, raw_text: str) -> tuple[int, int, int]:
        parsed = orjson.loads(raw_text)
        return parsed["U"], parsed["u"], parsed["pu"]


def _extract_data_value(frame: str, search_start: int) -> str:
    """Extract the JSON value after "data": using balanced brace counting.
    This preserves the exact original bytes of the data field."""
    # Skip whitespace
    i = search_start
    while i < len(frame) and frame[i] in " \t\n\r":
        i += 1

    if i >= len(frame):
        raise ValueError("Frame truncated: no value after 'data' key")
    if frame[i] != "{":
        raise ValueError(f"Expected '{{' at position {i}, got '{frame[i]}'")

    depth = 0
    in_string = False
    escape_next = False
    start = i

    while i < len(frame):
        ch = frame[i]
        if escape_next:
            escape_next = False
        elif ch == "\\":
            if in_string:
                escape_next = True
        elif ch == '"':
            in_string = not in_string
        elif not in_string:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return frame[start : i + 1]
        i += 1

    raise ValueError("Unbalanced braces in frame")


def _parse_stream_key(stream_key: str) -> tuple[str, str]:
    """Parse 'btcusdt@aggTrade' -> ('btcusdt', 'trades')"""
    symbol, _, remainder = stream_key.partition("@")
    # Strip any parameters (e.g., depth@100ms -> depth, markPrice@1s -> markPrice)
    base_stream = remainder.split("@")[0]

    stream_type = _STREAM_KEY_MAP.get(base_stream)
    if stream_type is None:
        raise ValueError(f"Unknown Binance stream key: {stream_key}")

    return symbol.lower(), stream_type
