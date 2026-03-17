from __future__ import annotations

from src.collector.producer import CryptoLakeProducer
from src.collector.streams.simple import SimpleStreamHandler


class FundingRateHandler(SimpleStreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        super().__init__(exchange, collector_session_id, producer, "funding_rate")
