package com.cryptolake.writer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.naming.NamingConvention;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns all 24 Micrometer meters for the writer service (design §9).
 *
 * <p>Applies {@link NamingConvention#identity} to the registry so metric names are emitted
 * verbatim (Tier 5 H4 watch-out): counters named {@code foo_total} do NOT get the extra
 * {@code _total} suffix that Micrometer's default {@code PrometheusNamingConvention} would append,
 * which would result in {@code foo_total_total}.
 *
 * <p>Gauges use supplier-backed {@link MetricHolders} — strongly referenced by this class so they
 * are never GC'd and never return {@code NaN} (Tier 5 H6 watch-out).
 *
 * <p>Thread safety: Micrometer meters are thread-safe; holders use atomic types (no {@code
 * synchronized} — Tier 2 §9).
 */
public final class WriterMetrics {

  private final PrometheusMeterRegistry registry;
  private final MetricHolders holders;

  /** SLO buckets for flush duration histogram (Tier 5 H5). */
  private static final double[] FLUSH_DURATION_SLO =
      new double[] {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

  /** SLO buckets for failover duration histogram (Tier 5 H5). */
  private static final double[] FAILOVER_DURATION_SLO =
      new double[] {1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600};

  /**
   * Constructs and registers all 24 meters against the supplied registry.
   *
   * <p>Applies {@link NamingConvention#identity} immediately (Tier 5 H4).
   */
  public WriterMetrics(PrometheusMeterRegistry registry) {
    this.registry = registry;
    this.holders = new MetricHolders();
    // Tier 5 H4: identity convention prevents _total_total suffix on counters already ending in _total
    registry.config().namingConvention(NamingConvention.identity);

    // Register no-label gauges once (keyed holders already default to 0)
    Gauge.builder("writer_disk_usage_bytes", holders.diskUsageBytes, AtomicLong::get)
        .description("Disk usage in bytes")
        .register(registry);
    Gauge.builder("writer_disk_usage_pct", holders.diskUsagePct, AtomicReference::get)
        .description("Disk usage percent")
        .register(registry);
    Gauge.builder("writer_failover_active", holders.failoverActive, AtomicReference::get)
        .description("1 if failover is active, 0 otherwise")
        .register(registry);
    Gauge.builder("writer_gap_pending_size", holders.gapPendingSize, AtomicLong::get)
        .description("Number of gap envelopes parked in coverage filter")
        .register(registry);
  }

  /** Returns the underlying {@link PrometheusMeterRegistry} (e.g., for {@code /metrics} scrape). */
  public PrometheusMeterRegistry registry() {
    return registry;
  }

  // ── Counters — per stream ────────────────────────────────────────────────────────────────────

  /** Metric #1: total messages consumed from Kafka (design §9). */
  public Counter messagesConsumed(String exchange, String symbol, String stream) {
    return Counter.builder("writer_messages_consumed_total")
        .description("Messages read from Redpanda")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #2: messages skipped (below recovery high-water offset). */
  public Counter messagesSkipped(String exchange, String symbol, String stream) {
    return Counter.builder("writer_messages_skipped_total")
        .description("Messages skipped due to recovery high-water")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #4: files rotated (sealed). */
  public Counter filesRotated(String exchange, String symbol, String stream) {
    return Counter.builder("writer_files_rotated_total")
        .description("Number of archive files rotated")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #5: bytes written to disk. */
  public Counter bytesWritten(String exchange, String symbol, String stream) {
    return Counter.builder("writer_bytes_written_total")
        .description("Compressed bytes written to disk")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #9: session gaps detected. */
  public Counter sessionGapsDetected(String exchange, String symbol, String stream) {
    return Counter.builder("writer_session_gaps_detected_total")
        .description("Session change gaps detected")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #11: write errors (IOException during file write). */
  public Counter writeErrors(String exchange, String symbol, String stream) {
    return Counter.builder("writer_write_errors_total")
        .description("File write errors")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #14: gap records written to archive. */
  public Counter gapRecordsWritten(String exchange, String symbol, String stream, String reason) {
    return Counter.builder("writer_gap_records_written_total")
        .description("Gap envelope records written to archive")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream, "reason", reason)
        .register(registry);
  }

  // ── Counters — no labels ─────────────────────────────────────────────────────────────────────

  /** Metric #12: PG commit failures. */
  public Counter pgCommitFailures() {
    return Counter.builder("writer_pg_commit_failures_total")
        .description("PostgreSQL checkpoint failures")
        .register(registry);
  }

  /** Metric #13: Kafka commit failures. */
  public Counter kafkaCommitFailures() {
    return Counter.builder("writer_kafka_commit_failures_total")
        .description("Kafka offset commit failures")
        .register(registry);
  }

  /** Metric #18: total failover activations. */
  public Counter failoverTotal() {
    return Counter.builder("writer_failover_total")
        .description("Total backup-consumer activations")
        .register(registry);
  }

  /** Metric #20: records consumed from backup topic during failover. */
  public Counter failoverRecordsTotal() {
    return Counter.builder("writer_failover_records_total")
        .description("Records consumed from backup topic during failover")
        .register(registry);
  }

  /** Metric #21: total switchback events (backup → primary). */
  public Counter switchbackTotal() {
    return Counter.builder("writer_switchback_total")
        .description("Total switchback events (backup to primary)")
        .register(registry);
  }

  /** Metric #22: gap envelopes suppressed by coverage filter. */
  public Counter gapEnvelopesSuppressed(String source, String reason) {
    return Counter.builder("writer_gap_envelopes_suppressed_total")
        .description("Gap envelopes suppressed by coverage filter")
        .tags("source", source, "reason", reason)
        .register(registry);
  }

  /** Metric #23: gap envelopes coalesced. */
  public Counter gapCoalesced(String source) {
    return Counter.builder("writer_gap_coalesced_total")
        .description("Gap envelopes coalesced in coverage filter")
        .tags("source", source)
        .register(registry);
  }

  // ── Gauges — updated via holders ────────────────────────────────────────────────────────────

  /**
   * Metric #3: consumer lag per exchange+stream. Lazily registers and caches the holder.
   *
   * <p>Update cadence: on every {@code flushAndCommit} call (Q3 preferred, design §11).
   */
  public void setConsumerLag(String exchange, String stream, long lag) {
    String key = exchange + "|" + stream;
    AtomicLong holder =
        holders.consumerLag.computeIfAbsent(
            key,
            k -> {
              AtomicLong h = new AtomicLong(0L);
              Gauge.builder("writer_consumer_lag", h, AtomicLong::get)
                  .description("Consumer lag in number of messages")
                  .tags("exchange", exchange, "stream", stream)
                  .register(registry);
              return h;
            });
    holder.set(lag);
  }

  /**
   * Metric #6: compression ratio per stream. Guarded against divide-by-zero (design §9 E1 mapping
   * risk 12).
   */
  public void setCompressionRatio(String exchange, String symbol, String stream, double ratio) {
    String key = exchange + "|" + symbol + "|" + stream;
    AtomicReference<Double> holder =
        holders.compressionRatio.computeIfAbsent(
            key,
            k -> {
              AtomicReference<Double> h = new AtomicReference<>(0.0);
              Gauge.builder("writer_compression_ratio", h, AtomicReference::get)
                  .description("Compression ratio (uncompressed/compressed bytes)")
                  .tags("exchange", exchange, "symbol", symbol, "stream", stream)
                  .register(registry);
              return h;
            });
    holder.set(ratio);
  }

  /** Metric #7: disk usage in bytes. */
  public void setDiskUsageBytes(long bytes) {
    holders.diskUsageBytes.set(bytes);
  }

  /** Metric #8: disk usage percent. */
  public void setDiskUsagePct(double pct) {
    holders.diskUsagePct.set(pct);
  }

  /** Metric #17: failover active flag (1.0 = active, 0.0 = inactive). */
  public void setFailoverActive(boolean active) {
    holders.failoverActive.set(active ? 1.0 : 0.0);
  }

  /** Metric #15: hours sealed today (UTC midnight reset — Tier 5 M11). */
  public void setHoursSealedToday(String exchange, String symbol, String stream, int count) {
    String key = exchange + "|" + symbol + "|" + stream;
    AtomicLong holder =
        holders.hoursSealedToday.computeIfAbsent(
            key,
            k -> {
              AtomicLong h = new AtomicLong(0L);
              Gauge.builder("writer_hours_sealed_today", h, AtomicLong::get)
                  .description("Number of hours sealed today (UTC)")
                  .tags("exchange", exchange, "symbol", symbol, "stream", stream)
                  .register(registry);
              return h;
            });
    holder.set(count);
  }

  /** Metric #16: hours sealed on the previous UTC day. */
  public void setHoursSealedPreviousDay(String exchange, String symbol, String stream, int count) {
    String key = exchange + "|" + symbol + "|" + stream;
    AtomicLong holder =
        holders.hoursSealedPreviousDay.computeIfAbsent(
            key,
            k -> {
              AtomicLong h = new AtomicLong(0L);
              Gauge.builder("writer_hours_sealed_previous_day", h, AtomicLong::get)
                  .description("Number of hours sealed on the previous UTC day")
                  .tags("exchange", exchange, "symbol", symbol, "stream", stream)
                  .register(registry);
              return h;
            });
    holder.set(count);
  }

  /** Metric #24: number of gap envelopes parked in coverage filter. */
  public void setGapPendingSize(int size) {
    holders.gapPendingSize.set(size);
  }

  // ── Histograms ───────────────────────────────────────────────────────────────────────────────

  /**
   * Metric #10: flush duration histogram in milliseconds (Tier 5 H5). Uses explicit SLOs (not
   * {@code publishPercentileHistogram(true)}) to match Python's fixed bucket set.
   */
  public DistributionSummary flushDurationMs(String exchange, String symbol, String stream) {
    return DistributionSummary.builder("writer_flush_duration_ms")
        .description("Time to flush buffer to disk (ms)")
        .baseUnit("milliseconds")
        .serviceLevelObjectives(FLUSH_DURATION_SLO)
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /**
   * Metric #19: failover episode duration histogram in seconds (Tier 5 H5).
   */
  public DistributionSummary failoverDurationSeconds() {
    return DistributionSummary.builder("writer_failover_duration_seconds")
        .description("Duration of failover episode in seconds")
        .baseUnit("seconds")
        .serviceLevelObjectives(FAILOVER_DURATION_SLO)
        .register(registry);
  }
}
