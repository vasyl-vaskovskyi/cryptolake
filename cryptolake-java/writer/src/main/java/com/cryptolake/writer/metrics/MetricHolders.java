package com.cryptolake.writer.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable suppliers-of-value for Micrometer gauge meters.
 *
 * <p>Micrometer gauges are PULLED (not pushed), so each gauge must hold a strongly-referenced
 * object whose value the supplier reads. This class keeps those holders alive for the full service
 * lifetime so the gauge does not GC away and report {@code NaN} (Tier 5 H6 watch-out).
 *
 * <p>Package-private — only {@link WriterMetrics} reads/writes these holders.
 *
 * <p>Thread safety: the holders use {@code AtomicLong} / {@code AtomicReference} so that the
 * Micrometer scrape thread always reads a coherent value without locking. The consume-loop thread
 * writes; the Prometheus scrape thread reads (Tier 2 §9 — no {@code synchronized}).
 */
final class MetricHolders {

  // Gauge: writer_consumer_lag — keyed by "exchange|stream"
  final ConcurrentHashMap<String, AtomicLong> consumerLag = new ConcurrentHashMap<>();

  // Gauge: writer_compression_ratio — keyed by "exchange|symbol|stream"
  final ConcurrentHashMap<String, AtomicReference<Double>> compressionRatio =
      new ConcurrentHashMap<>();

  // Gauge: writer_disk_usage_bytes (no labels)
  final AtomicLong diskUsageBytes = new AtomicLong(0L);

  // Gauge: writer_disk_usage_pct (no labels)
  final AtomicReference<Double> diskUsagePct = new AtomicReference<>(0.0);

  // Gauge: writer_failover_active (no labels) — 1.0 = active, 0.0 = inactive
  final AtomicReference<Double> failoverActive = new AtomicReference<>(0.0);

  // Gauge: writer_hours_sealed_today — keyed by "exchange|symbol|stream"
  final ConcurrentHashMap<String, AtomicLong> hoursSealedToday = new ConcurrentHashMap<>();

  // Gauge: writer_hours_sealed_previous_day — keyed by "exchange|symbol|stream"
  final ConcurrentHashMap<String, AtomicLong> hoursSealedPreviousDay = new ConcurrentHashMap<>();

  // Gauge: writer_gap_pending_size (no labels)
  final AtomicLong gapPendingSize = new AtomicLong(0L);
}
