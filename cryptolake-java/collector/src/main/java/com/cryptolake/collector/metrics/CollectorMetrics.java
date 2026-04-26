package com.cryptolake.collector.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns all 9 Prometheus meters for the collector service (design §9).
 *
 * <p><b>Counter naming — Tier 5 H4:</b> counters are registered WITHOUT the {@code _total} suffix;
 * Prometheus scrape appends it automatically. {@link NamingConvention#identity} prevents any other
 * Micrometer transformation.
 *
 * <p>Gauges use supplier-backed {@link MetricHolders} — strongly referenced to avoid GC (Tier 5 H6
 * watch-out).
 *
 * <p>Thread safety: Micrometer meters are thread-safe; holders use atomic types (Tier 2 §9).
 */
public final class CollectorMetrics {

  private final PrometheusMeterRegistry registry;
  private final MetricHolders holders;

  /** SLO buckets for exchange-latency histogram (Tier 5 H5; matches {@code metrics.py:27-32}). */
  private static final double[] LATENCY_SLO =
      new double[] {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000};

  /**
   * Constructs and registers all 9 meters against the supplied registry.
   *
   * <p>Applies {@link NamingConvention#identity} immediately (Tier 5 H4).
   */
  public CollectorMetrics(PrometheusMeterRegistry registry) {
    this.registry = registry;
    this.holders = new MetricHolders();
    registry.config().namingConvention(NamingConvention.identity);

    // Metric #2: gauge — registered once at construction with a holder supplier
    Gauge.builder("collector_ws_connections_active", holders.wsConnectionsActive, AtomicLong::get)
        .description("Current open WebSocket connections")
        .tags("exchange", "binance") // default label; updated via setWsConnectionsActive
        .register(registry);
  }

  /** Returns the underlying registry (for {@code /metrics} scrape and harness tests). */
  public PrometheusMeterRegistry registry() {
    return registry;
  }

  // ── Counters ─────────────────────────────────────────────────────────────

  /** Metric #1: messages sent to Redpanda. */
  public Counter messagesProduced(String exchange, String symbol, String stream) {
    return Counter.builder("collector_messages_produced")
        .description("Messages sent to Redpanda")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #3: WebSocket reconnections. */
  public Counter wsReconnects(String exchange) {
    return Counter.builder("collector_ws_reconnects")
        .description("Reconnection count")
        .tags("exchange", exchange)
        .register(registry);
  }

  /** Metric #4: gaps detected (sequence breaks, disconnects, drops). */
  public Counter gapsDetected(String exchange, String symbol, String stream, String reason) {
    return Counter.builder("collector_gaps_detected")
        .description("Gaps detected (sequence breaks, disconnects, drops)")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream, "reason", reason)
        .register(registry);
  }

  /** Metric #6: successful REST depth snapshots taken. */
  public Counter snapshotsTaken(String exchange, String symbol) {
    return Counter.builder("collector_snapshots_taken")
        .description("Successful REST snapshots")
        .tags("exchange", exchange, "symbol", symbol)
        .register(registry);
  }

  /** Metric #7: failed snapshot attempts. */
  public Counter snapshotsFailed(String exchange, String symbol) {
    return Counter.builder("collector_snapshots_failed")
        .description("Failed snapshot attempts")
        .tags("exchange", exchange, "symbol", symbol)
        .register(registry);
  }

  /** Metric #8: messages dropped due to buffer overflow. */
  public Counter messagesDropped(String exchange, String symbol, String stream) {
    return Counter.builder("collector_messages_dropped")
        .description("Messages dropped due to buffer overflow")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  /** Metric #9: per-(symbol,stream) liveness heartbeats produced. */
  public Counter heartbeatsEmitted(String exchange, String symbol, String stream, String status) {
    return Counter.builder("collector_heartbeats_emitted")
        .description("Per-(symbol, stream) liveness heartbeats produced to Kafka")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream, "status", status)
        .register(registry);
  }

  // ── Gauge ─────────────────────────────────────────────────────────────────

  /**
   * Metric #2: sets the current number of active WebSocket connections. Uses a holder so the gauge
   * is always valid (Tier 5 H6 — supplier references a strongly-held {@link AtomicLong}).
   *
   * <p>Note: the gauge is registered once at construction with label {@code exchange="binance"};
   * this method just updates the backing value.
   */
  public void setWsConnectionsActive(String exchange, int value) {
    holders.wsConnectionsActive.set(value);
    // Also register a labeled gauge if not yet present for this exchange
    Gauge.builder("collector_ws_connections_active", holders.wsConnectionsActive, AtomicLong::get)
        .description("Current open WebSocket connections")
        .tags("exchange", exchange)
        .register(registry); // idempotent — returns existing meter if already registered
  }

  // ── Histogram ─────────────────────────────────────────────────────────────

  /**
   * Metric #5: received_at − exchange_ts distribution in milliseconds (Tier 5 H5 — explicit SLO
   * buckets, not publishPercentileHistogram).
   */
  public DistributionSummary exchangeLatencyMs(String exchange, String symbol, String stream) {
    return DistributionSummary.builder("collector_exchange_latency_ms")
        .description("received_at - exchange_ts distribution (ms)")
        .serviceLevelObjectives(LATENCY_SLO)
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
  }

  // ── Inner: holder class ───────────────────────────────────────────────────

  /** Strongly-held backing fields for supplier-based gauges (Tier 5 H6). */
  static final class MetricHolders {
    final AtomicLong wsConnectionsActive = new AtomicLong(0);
    final AtomicReference<Double> ignored = new AtomicReference<>(0.0); // placeholder
  }
}
