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

backup_recovery_attempts = Counter(
    "writer_backup_recovery_attempts_total",
    "Total backup recovery attempts",
)
backup_recovery_success = Counter(
    "writer_backup_recovery_success_total",
    "Full recoveries from backup (no gap envelope needed)",
)
backup_recovery_partial = Counter(
    "writer_backup_recovery_partial_total",
    "Partial recoveries from backup (narrowed gap)",
)
backup_recovery_miss = Counter(
    "writer_backup_recovery_miss_total",
    "Backup had no data for the gap",
)
