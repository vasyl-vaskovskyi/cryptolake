package com.cryptolake.collector.gap;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.producer.OverflowWindow;
import com.cryptolake.collector.producer.TestProducerBridge;
import com.cryptolake.common.util.ClockSupplier;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GapEmitter}.
 *
 * <p>Ports {@code test_emit_gap.py} tests.
 */
class GapEmitterTest {

  private PrometheusMeterRegistry registry;
  private CollectorMetrics metrics;
  private TestProducerBridge testProducer;
  private GapEmitter gapEmitter;

  @BeforeEach
  void setUp() {
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new CollectorMetrics(registry);
    testProducer = new TestProducerBridge();
    CollectorSession session =
        new CollectorSession("test-collector", "test-collector_2026-01-01T00:00:00Z", Instant.now());
    ClockSupplier clock = () -> 1_000_000_000_000L;
    gapEmitter = new GapEmitter("binance", session, testProducer, metrics, clock);
  }

  @Test
  // ports: tests/unit/collector/test_emit_gap.py::test_emit_gap_produces_gap_envelope_and_increments_metric
  void emitGapProducesEnvelopeAndIncrementsMetric() {
    gapEmitter.emit("btcusdt", "depth", 5L, "ws_disconnect", "test detail");

    assertThat(testProducer.gapEnvelopes).hasSize(1);
    var gap = testProducer.gapEnvelopes.get(0);
    assertThat(gap.symbol()).isEqualTo("btcusdt");
    assertThat(gap.stream()).isEqualTo("depth");
    assertThat(gap.reason()).isEqualTo("ws_disconnect");
    assertThat(gap.detail()).isEqualTo("test detail");

    Counter counter = metrics.gapsDetected("binance", "btcusdt", "depth", "ws_disconnect");
    assertThat(counter.count()).isEqualTo(1.0);
  }

  @Test
  // ports: tests/unit/collector/test_emit_gap.py::test_emit_gap_uses_custom_timestamps
  void emitGapUsesCustomTimestamps() {
    long gapStart = 500_000_000_000L;
    long gapEnd = 600_000_000_000L;
    gapEmitter.emitWithTimestamps(
        "btcusdt", "depth", 5L, "ws_disconnect", "detail", gapStart, gapEnd);
    assertThat(testProducer.gapEnvelopes).hasSize(1);
    var gap = testProducer.gapEnvelopes.get(0);
    assertThat(gap.gapStartTs()).isEqualTo(gapStart);
    assertThat(gap.gapEndTs()).isEqualTo(gapEnd);
  }

  @Test
  // ports: tests/unit/collector/test_emit_gap.py::test_emit_overflow_gap_increments_gaps_detected_total
  void emitOverflowGapIncrementsGapsDetectedTotal() {
    OverflowWindow window = new OverflowWindow(900_000_000_000L, 5);
    gapEmitter.emitOverflowRecovery("btcusdt", "depth", window);
    assertThat(testProducer.gapEnvelopes).hasSize(1);
    Counter counter = metrics.gapsDetected("binance", "btcusdt", "depth", "buffer_overflow");
    assertThat(counter.count()).isEqualTo(1.0);
  }
}
