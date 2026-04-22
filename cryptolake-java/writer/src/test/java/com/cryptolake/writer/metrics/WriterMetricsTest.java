package com.cryptolake.writer.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WriterMetrics}.
 *
 * <p>Ports: Python's {@code test_writer_metrics.py} — verifies NamingConvention.identity prevents
 * _total_total suffix (Tier 5 H4), gauge holders are strongly referenced (Tier 5 H6).
 */
class WriterMetricsTest {

  private PrometheusMeterRegistry registry;
  private WriterMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
  }

  // ports: Tier 5 H4 — NamingConvention.identity → counter name NOT suffixed with _total_total
  @Test
  void messagesConsumed_scrapeOutput_noDoubleTotalSuffix() {
    metrics.messagesConsumed("binance", "btcusdt", "trades").increment();

    String scrape = registry.scrape();
    assertThat(scrape).doesNotContain("_total_total");
  }

  // ports: Tier 5 H4 — counter name is exactly as registered (ends with _total, not _total_total)
  @Test
  void messagesConsumed_counterName_exactlyWriter_messages_consumed_total() {
    metrics.messagesConsumed("binance", "btcusdt", "trades").increment(3.0);

    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_messages_consumed_total");
  }

  // ports: Tier 5 H4 — files rotated counter
  @Test
  void filesRotated_counterExists() {
    metrics.filesRotated("binance", "btcusdt", "trades").increment();
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_files_rotated_total");
  }

  // ports: Tier 5 H6 — gauge holders strongly referenced; calling setConsumerLag doesn't throw
  @Test
  void setConsumerLag_doesNotThrow() {
    metrics.setConsumerLag("binance", "trades", 42L);
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_consumer_lag");
  }

  // ports: Tier 5 H6 — gap pending size gauge
  @Test
  void setGapPendingSize_updatesGauge() {
    metrics.setGapPendingSize(5);
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_gap_pending_size");
  }

  // ports: Tier 5 H5 — failover_duration histogram has SLO buckets (no publishPercentileHistogram)
  @Test
  void failoverDuration_histogramBuckets_inScrapeOutput() {
    metrics.failoverDurationSeconds().record(1.5);
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_failover_duration_seconds");
  }

  // ports: design §2.8 — messages skipped counter
  @Test
  void messagesSkipped_counterExists() {
    metrics.messagesSkipped("binance", "btcusdt", "trades").increment();
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_messages_skipped_total");
  }

  // ports: design §2.8 — failover records total counter
  @Test
  void failoverRecordsTotal_counterExists() {
    metrics.failoverRecordsTotal().increment();
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_failover_records_total");
  }

  // ports: design §2.8 — gap coalesced counter
  @Test
  void gapCoalesced_counterExists() {
    metrics.gapCoalesced("primary").increment();
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_gap_coalesced_total");
  }

  // ports: design §2.8 — gap envelopes suppressed counter
  @Test
  void gapEnvelopesSuppressed_counterExists() {
    metrics.gapEnvelopesSuppressed("primary", "covered").increment();
    String scrape = registry.scrape();
    assertThat(scrape).contains("writer_gap_envelopes_suppressed_total");
  }
}
