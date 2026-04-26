package com.cryptolake.collector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.collector.metrics.CollectorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * Regression test for {@link MetricSkeletonDump}: verifies that the canonical output contains the
 * expected lines (gate 4 regression — design §8.1).
 */
class MetricSkeletonDumpTest {

  @Test
  // ports: (new) MetricSkeletonDumpTest::canonicalizedOutputMatchesFixture
  void canonicalizedOutputMatchesExpectedMetrics() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    CollectorMetrics metrics = new CollectorMetrics(registry);

    // Exercise all 9 meters
    metrics.messagesProduced("binance", "btcusdt", "depth").increment(1);
    metrics.setWsConnectionsActive("binance", 1);
    metrics.wsReconnects("binance").increment(1);
    metrics.gapsDetected("binance", "btcusdt", "depth", "pu_chain_break").increment(1);
    metrics.exchangeLatencyMs("binance", "btcusdt", "depth").record(5.0);
    metrics.snapshotsTaken("binance", "btcusdt").increment(1);
    metrics.snapshotsFailed("binance", "btcusdt").increment(1);
    metrics.messagesDropped("binance", "btcusdt", "depth").increment(1);
    metrics.heartbeatsEmitted("binance", "btcusdt", "depth", "alive").increment(1);

    String canonical = MetricSkeletonDump.canonicalize(registry.scrape());

    // Must contain all 9 metric families
    assertThat(canonical).contains("collector_messages_produced_total");
    assertThat(canonical).contains("collector_ws_connections_active");
    assertThat(canonical).contains("collector_ws_reconnects_total");
    assertThat(canonical).contains("collector_gaps_detected_total");
    assertThat(canonical).contains("collector_exchange_latency_ms");
    assertThat(canonical).contains("collector_snapshots_taken_total");
    assertThat(canonical).contains("collector_snapshots_failed_total");
    assertThat(canonical).contains("collector_messages_dropped_total");
    assertThat(canonical).contains("collector_heartbeats_emitted_total");

    // Must NOT contain _max lines
    assertThat(canonical).doesNotContain("_max");
  }
}
