from prometheus_client import Counter, Gauge, Histogram

messages_produced_total = Counter(
    "collector_messages_produced_total",
    "Messages sent to Redpanda",
    ["exchange", "symbol", "stream"],
)

ws_connections_active = Gauge(
    "collector_ws_connections_active",
    "Current open WebSocket connections",
    ["exchange"],
)

ws_reconnects_total = Counter(
    "collector_ws_reconnects_total",
    "Reconnection count",
    ["exchange"],
)

gaps_detected_total = Counter(
    "collector_gaps_detected_total",
    "Gaps detected (sequence breaks, disconnects, drops)",
    ["exchange", "symbol", "stream", "reason"],
)

exchange_latency_ms = Histogram(
    "collector_exchange_latency_ms",
    "received_at - exchange_ts distribution (ms)",
    ["exchange", "symbol", "stream"],
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000],
)

snapshots_taken_total = Counter(
    "collector_snapshots_taken_total",
    "Successful REST snapshots",
    ["exchange", "symbol"],
)

snapshots_failed_total = Counter(
    "collector_snapshots_failed_total",
    "Failed snapshot attempts",
    ["exchange", "symbol"],
)

messages_dropped_total = Counter(
    "collector_messages_dropped_total",
    "Messages dropped due to buffer overflow",
    ["exchange", "symbol", "stream"],
)

heartbeats_emitted_total = Counter(
    "collector_heartbeats_emitted_total",
    "Per-(symbol, stream) liveness heartbeats produced to Kafka",
    ["exchange", "symbol", "stream", "status"],
)
