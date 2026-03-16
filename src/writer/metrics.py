from prometheus_client import Counter, Gauge, Histogram

messages_consumed_total = Counter(
    "writer_messages_consumed_total",
    "Messages read from Redpanda",
    ["exchange", "symbol", "stream"],
)

consumer_lag = Gauge(
    "writer_consumer_lag",
    "Messages behind head (gap proxy)",
    ["exchange", "stream"],
)

files_rotated_total = Counter(
    "writer_files_rotated_total",
    "File rotations completed",
    ["exchange", "symbol", "stream"],
)

bytes_written_total = Counter(
    "writer_bytes_written_total",
    "Compressed bytes to disk",
    ["exchange", "symbol", "stream"],
)

compression_ratio = Gauge(
    "writer_compression_ratio",
    "Raw / compressed size ratio",
    ["exchange", "stream"],
)

disk_usage_bytes = Gauge(
    "writer_disk_usage_bytes",
    "Total storage consumed",
)

disk_usage_pct = Gauge(
    "writer_disk_usage_pct",
    "Percentage of volume used",
)

flush_duration_ms = Histogram(
    "writer_flush_duration_ms",
    "Time to flush buffer to disk (ms)",
    ["exchange", "stream"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
)
