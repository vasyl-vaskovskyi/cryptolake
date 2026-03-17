from __future__ import annotations

from src.collector.producer import CryptoLakeProducer
from src.collector.streams.base import StreamHandler
from src.common.envelope import create_data_envelope


class SimpleStreamHandler(StreamHandler):
    """Handler for streams that need no special processing beyond envelope creation.

    Used for trades, bookticker, funding_rate, and liquidations -- all of which
    follow the same pattern: check seq, create data envelope, produce.
    """

    def __init__(self, exchange: str, collector_session_id: str,
                 producer: CryptoLakeProducer, stream_name: str):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.stream_name = stream_name
        self._init_seq_tracking(exchange, collector_session_id, producer, stream_name)

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream=self.stream_name,
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
