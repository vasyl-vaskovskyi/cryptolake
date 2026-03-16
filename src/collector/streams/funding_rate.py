from __future__ import annotations

from src.collector.streams.base import StreamHandler
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_data_envelope


class FundingRateHandler(StreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self._init_seq_tracking(exchange, collector_session_id, producer, "funding_rate")

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="funding_rate",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
