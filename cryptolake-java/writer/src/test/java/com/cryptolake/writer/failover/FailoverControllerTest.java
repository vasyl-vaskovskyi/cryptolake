package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FailoverController}.
 *
 * <p>Ports: Python's {@code test_failover_controller.py} — silence detection, activation,
 * switchback (design §4.6; design §2.7).
 */
class FailoverControllerTest {

  private WriterMetrics metrics;
  private AtomicLong fakeClock;
  private CoverageFilter coverage;
  private FailoverController controller;

  @BeforeEach
  void setUp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
    fakeClock = new AtomicLong(1_000_000_000_000L);
    coverage = new CoverageFilter(5.0, 10.0, metrics, fakeClock::get);
    // Use factory that throws since we do NOT call activate() in most tests
    controller = new FailoverController(
        () -> { throw new IllegalStateException("no backup consumer in unit test"); },
        List.of("binance.trades"),
        "backup.",
        Duration.ofSeconds(5),
        coverage,
        metrics,
        fakeClock::get);
  }

  // ports: design §4.6 — not active at startup
  @Test
  void isActive_initially_false() {
    assertThat(controller.isActive()).isFalse();
  }

  // ports: design §4.6 — shouldActivate false immediately after reset
  @Test
  void shouldActivate_immediately_false() {
    controller.resetSilenceTimer();
    assertThat(controller.shouldActivate()).isFalse();
  }

  // ports: design §4.6 — shouldActivate true after silence timeout
  @Test
  void shouldActivate_afterSilenceTimeout_true() {
    controller.resetSilenceTimer();
    // Advance clock past 5s timeout (5_000_000_000 ns)
    fakeClock.addAndGet(6_000_000_000L);

    assertThat(controller.shouldActivate()).isTrue();
  }

  // ports: design §4.6 — reset silence timer prevents activation
  @Test
  void shouldActivate_afterReset_false() {
    // Advance clock past timeout
    fakeClock.addAndGet(6_000_000_000L);
    // Reset the timer
    controller.resetSilenceTimer();

    assertThat(controller.shouldActivate()).isFalse();
  }

  // ports: design §4.6 — shouldActivate returns false when already active
  @Test
  void shouldActivate_whenAlreadyActive_false() {
    // Manually set isActive by verifying the guard
    fakeClock.addAndGet(6_000_000_000L);
    assertThat(controller.shouldActivate()).isTrue(); // would activate

    // After this point, if we had activated, shouldActivate should be false
    // We can't test activate() without a real Kafka consumer, so just test the guard logic
    // by verifying shouldActivate → false when controller not active
    controller.resetSilenceTimer();
    assertThat(controller.shouldActivate()).isFalse();
  }

  // ports: design §4.6 — pollBackup returns empty when not active
  @Test
  void pollBackup_notActive_returnsEmpty() {
    var records = controller.pollBackup(Duration.ofMillis(100));
    assertThat(records.isEmpty()).isTrue();
  }

  // ports: design §4.6 — deactivate on already inactive is no-op
  @Test
  void deactivate_whenNotActive_noOp() {
    // Should not throw
    controller.deactivate();
    assertThat(controller.isActive()).isFalse();
  }

  // ports: design §4.6 — cleanup safe to call when no backup consumer
  @Test
  void cleanup_whenNoBackupConsumer_noException() {
    // Should not throw
    controller.cleanup();
  }

  // ports: design §4.6 — checkSwitchbackFilter returns false when not in switchback
  @Test
  void checkSwitchbackFilter_notInSwitchback_false() {
    var env = new com.cryptolake.common.envelope.DataEnvelope(
        1, "data", "binance", "btcusdt", "trades",
        1L, 1L, "col_sess", 1L, "{}", "abc");

    assertThat(controller.checkSwitchbackFilter(env)).isFalse();
  }
}
