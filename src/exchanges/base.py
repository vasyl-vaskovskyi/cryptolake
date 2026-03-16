from __future__ import annotations

from abc import ABC, abstractmethod


class ExchangeAdapter(ABC):
    """Base interface for exchange-specific logic.

    Each exchange adapter must implement URL building, stream routing,
    payload parsing, and raw_text extraction.
    """

    @abstractmethod
    def get_ws_urls(self, symbols: list[str], streams: list[str]) -> dict[str, str]:
        """Return dict of {socket_name: ws_url} for the given symbols and streams.
        May split across multiple sockets by traffic type."""

    @abstractmethod
    def route_stream(self, raw_frame: str) -> tuple[str, str, str]:
        """Parse the outer combined-stream frame and return (stream_type, symbol, raw_data_text).
        raw_data_text must be extracted via zero-copy string slicing."""

    @abstractmethod
    def extract_exchange_ts(self, stream: str, raw_text: str) -> int:
        """Extract exchange timestamp (milliseconds) from raw payload.
        Uses lightweight parsing via orjson."""

    @abstractmethod
    def build_snapshot_url(self, symbol: str, limit: int = 1000) -> str:
        """Build REST URL for depth snapshot."""

    @abstractmethod
    def build_open_interest_url(self, symbol: str) -> str:
        """Build REST URL for open interest."""

    @abstractmethod
    def parse_snapshot_last_update_id(self, raw_text: str) -> int:
        """Extract lastUpdateId from depth snapshot raw_text."""

    @abstractmethod
    def parse_depth_update_ids(self, raw_text: str) -> tuple[int, int, int]:
        """Extract (U, u, pu) from depth diff raw_text."""
