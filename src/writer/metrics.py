from prometheus_client import Counter, Gauge, Histogram

messages_consumed_total = Counter(
    "writer_messages_consumed_total",
    "Messages read from Redpanda",
    ["exchange", "symbol", "stream"],
)

messages_skipped_total = Counter(
    "writer_messages_skipped_total",
    "Messages skipped during recovery (already in archive)",
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
    ["exchange", "symbol", "stream"],
)

disk_usage_bytes = Gauge(
    "writer_disk_usage_bytes",
    "Total storage consumed",
)

disk_usage_pct = Gauge(
    "writer_disk_usage_pct",
    "Percentage of volume used",
)

session_gaps_detected_total = Counter(
    "writer_session_gaps_detected_total",
    "Collector session changes detected (potential data loss)",
    ["exchange", "symbol", "stream"],
)

flush_duration_ms = Histogram(
    "writer_flush_duration_ms",
    "Time to flush buffer to disk (ms)",
    ["exchange", "symbol", "stream"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

write_errors_total = Counter(
    "writer_write_errors_total",
    "Disk write failures (ENOSPC, permission, etc.)",
    ["exchange", "symbol", "stream"],
)

pg_commit_failures_total = Counter(
    "writer_pg_commit_failures_total",
    "PostgreSQL state commit failures",
)

kafka_commit_failures_total = Counter(
    "writer_kafka_commit_failures_total",
    "Kafka async offset commit failures",
)

gap_records_written_total = Counter(
    "writer_gap_records_written_total",
    "Gap envelopes persisted to archive",
    ["exchange", "symbol", "stream", "reason"],
)

hours_sealed_today = Gauge(
    "writer_hours_sealed_today",
    "Hours sealed so far for the current UTC date",
    ["exchange", "symbol", "stream"],
)

hours_sealed_previous_day = Gauge(
    "writer_hours_sealed_previous_day",
    "Hours sealed for the previous UTC date",
    ["exchange", "symbol", "stream"],
)

# --- Failover metrics (replaced backup_recovery_* metrics) ---
failover_active = Gauge(
    "writer_failover_active",
    "1 if reading from backup topics, 0 if primary",
)

failover_total = Counter(
    "writer_failover_total",
    "Total failover activations",
)

failover_duration_seconds = Histogram(
    "writer_failover_duration_seconds",
    "Duration of each failover episode",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
)

failover_records_total = Counter(
    "writer_failover_records_total",
    "Records consumed from backup topics during failover",
)

switchback_total = Counter(
    "writer_switchback_total",
    "Successful switchbacks from backup to primary",
)

# --- Gap coverage filter metrics ---
gap_envelopes_suppressed_total = Counter(
    "writer_gap_envelopes_suppressed_total",
    "Gap envelopes dropped because the other collector covered the window",
    ["source", "reason"],
)

gap_coalesced_total = Counter(
    "writer_gap_coalesced_total",
    "Gap envelopes merged into an existing pending entry (stacked reconnect gaps)",
    ["source"],
)

gap_pending_size = Gauge(
    "writer_gap_pending_size",
    "Current size of the coverage filter's pending gap queue",
)
