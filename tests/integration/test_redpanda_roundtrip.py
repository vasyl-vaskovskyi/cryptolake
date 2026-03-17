from __future__ import annotations

import socket

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from src.common.envelope import (
    create_data_envelope,
    deserialize_envelope,
    serialize_envelope,
)


def _reserve_host_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def redpanda() -> str:
    host_port = _reserve_host_port()
    command = [
        "redpanda",
        "start",
        "--overprovisioned",
        "--smp",
        "1",
        "--memory",
        "512M",
        "--reserve-memory",
        "0M",
        "--check=false",
        "--node-id",
        "0",
        "--kafka-addr",
        "internal://0.0.0.0:9092,external://0.0.0.0:19092",
        "--advertise-kafka-addr",
        f"internal://redpanda:9092,external://127.0.0.1:{host_port}",
        "--set",
        "redpanda.auto_create_topics_enabled=true",
        "--set",
        "redpanda.default_topic_partitions=1",
        "--set",
        "redpanda.log_retention_ms=172800000",
    ]

    with (
        DockerContainer("redpandadata/redpanda:v24.1.2")
        .waiting_for(LogMessageWaitStrategy("started").with_startup_timeout(30))
        .with_bind_ports(19092, host_port)
        .with_command(command) as _container
    ):
        yield f"127.0.0.1:{host_port}"


@pytest.mark.integration
class TestRedpandaRoundTrip:
    def test_produce_consume_envelope(self, redpanda: str) -> None:
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": redpanda})
        futures = admin.create_topics(
            [NewTopic("binance.trades", num_partitions=1, replication_factor=1)]
        )
        for future in futures.values():
            future.result(10)

        producer = Producer({"bootstrap.servers": redpanda, "acks": "all"})
        envelope = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text='{"e":"aggTrade","E":100}',
            exchange_ts=100,
            collector_session_id="test",
            session_seq=0,
        )
        producer.produce(
            "binance.trades",
            key=b"btcusdt",
            value=serialize_envelope(envelope),
        )
        producer.flush(10)

        consumer = Consumer(
            {
                "bootstrap.servers": redpanda,
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["binance.trades"])
        message = consumer.poll(timeout=10.0)
        assert message is not None
        assert message.error() is None

        received = deserialize_envelope(message.value())
        assert received["exchange"] == "binance"
        assert received["symbol"] == "btcusdt"
        assert received["raw_text"] == '{"e":"aggTrade","E":100}'

        consumer.close()
