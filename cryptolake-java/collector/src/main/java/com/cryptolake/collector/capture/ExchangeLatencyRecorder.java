package com.cryptolake.collector.capture;

import com.cryptolake.collector.metrics.CollectorMetrics;

/**
 * Records the latency between exchange-side event timestamps and local received-at timestamps into
 * a histogram.
 *
 * <p>Latency is computed as {@code receivedAtNs / 1_000_000 - exchangeTsMs} — integer division
 * from nanoseconds to milliseconds, matching Python's {@code int(received_at / 1_000_000)}
 * pattern (Tier 5 E4).
 *
 * <p>Thread safety: Micrometer meters are thread-safe.
 */
public final class ExchangeLatencyRecorder {

  private final String exchange;
  private final CollectorMetrics metrics;

  public ExchangeLatencyRecorder(String exchange, CollectorMetrics metrics) {
    this.exchange = exchange;
    this.metrics = metrics;
  }

  /**
   * Records one latency observation for the given {@code (symbol, stream)} pair.
   *
   * @param symbol lowercase symbol
   * @param stream stream type
   * @param exchangeTsMs exchange timestamp in milliseconds
   * @param receivedAtNs local received-at timestamp in nanoseconds (Tier 5 E2)
   */
  public void record(String symbol, String stream, long exchangeTsMs, long receivedAtNs) {
    long receivedAtMs = receivedAtNs / 1_000_000L; // integer division (Tier 5 E4)
    double latencyMs = (double) (receivedAtMs - exchangeTsMs);
    if (latencyMs < 0) latencyMs = 0; // clock skew guard
    metrics.exchangeLatencyMs(exchange, symbol, stream).record(latencyMs);
  }
}
