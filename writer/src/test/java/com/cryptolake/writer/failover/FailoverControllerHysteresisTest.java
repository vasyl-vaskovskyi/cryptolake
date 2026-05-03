package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Hysteresis state machine for FailoverController (bug B from chaos test 01).
 *
 * <p>The state machine has three logical states: STEADY — !isActive(); shouldDeactivate() == false
 * ACTIVE_FAILOVER — isActive(); markPrimaryDelivered not yet seen post-activate RECOVERY_OBSERVING
 * — isActive(); markPrimaryDelivered seen; observing for 10s
 *
 * <p>shouldDeactivate() == true ONLY when: - isActive(), AND - first post-activate primary record
 * was ≥ recoveryStabilityWindow ago, AND - last primary record is fresh (within silenceTimeout —
 * primary still delivering).
 */
class FailoverControllerHysteresisTest {

  private static final Duration SILENCE_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration RECOVERY_WINDOW = Duration.ofSeconds(10);

  private AtomicLong fakeClockNs;
  private ClockSupplier clock;
  private WriterMetrics metrics;
  private FailoverController controller;

  @BeforeEach
  void setUp() {
    fakeClockNs = new AtomicLong(1_000_000_000_000L); // t = 1000s
    clock = fakeClockNs::get;
    metrics = new WriterMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
    CoverageFilter coverage = new CoverageFilter(5.0, 10.0, metrics, clock);
    controller =
        new FailoverController(
            List.of("binance.btcusdt.depth"),
            "backup.",
            SILENCE_TIMEOUT,
            RECOVERY_WINDOW,
            coverage,
            metrics,
            clock);
  }

  private void advanceSeconds(long s) {
    fakeClockNs.addAndGet(s * 1_000_000_000L);
  }

  @Test
  void steadyState_shouldDeactivateIsFalse() {
    assertThat(controller.isActive()).isFalse();
    assertThat(controller.shouldDeactivate()).isFalse();
  }

  @Test
  void activeFailover_beforeAnyPrimaryRecord_shouldDeactivateIsFalse() {
    controller.activate();

    assertThat(controller.isActive()).isTrue();
    assertThat(controller.shouldDeactivate()).isFalse();
  }

  @Test
  void recoveryObserving_beforeStabilityWindow_shouldDeactivateIsFalse() {
    controller.activate();

    advanceSeconds(2); // t=1002s
    controller.markPrimaryDelivered(); // first record post-activate

    advanceSeconds(5); // t=1007s, only 5s of recovery — window is 10s
    controller.markPrimaryDelivered();
    assertThat(controller.shouldDeactivate()).isFalse();
  }

  @Test
  void recoveryObserving_afterStabilityWindow_shouldDeactivateIsTrue() {
    controller.activate();
    advanceSeconds(2);
    controller.markPrimaryDelivered(); // first record post-activate

    // Simulate continuous primary delivery at 1 Hz across the 10 s window.
    // Production calls markPrimaryDelivered for every primary Kafka record (much
    // faster than 1 Hz), so this is a conservative simulation.
    for (int i = 0; i < 11; i++) {
      advanceSeconds(1);
      controller.markPrimaryDelivered();
    }

    // ≥ recoveryStabilityWindow has elapsed since the first post-activate record
    // AND primary delivered within the last second — fresh.
    assertThat(controller.shouldDeactivate()).isTrue();
  }

  @Test
  void recoveryObserving_silenceLongerThanTimeout_resetsRecovery() {
    controller.activate();
    advanceSeconds(2);
    controller.markPrimaryDelivered(); // recovery starts at t=1002s

    // Primary delivers continuously for 8 s (still inside the 10 s window).
    for (int i = 0; i < 8; i++) {
      advanceSeconds(1);
      controller.markPrimaryDelivered();
    }
    // recoveredFor=9s, sinceLast=1s — inside both bounds → still false.
    assertThat(controller.shouldDeactivate()).isFalse();

    // Then primary goes silent for 15 s (> silenceTimeout = 5 s).
    advanceSeconds(15);

    // Primary resumes — silence > silenceTimeout means the recovery clock resets.
    controller.markPrimaryDelivered();
    assertThat(controller.shouldDeactivate()).isFalse();

    // After the reset, primary needs to deliver continuously for another full window.
    for (int i = 0; i < 11; i++) {
      advanceSeconds(1);
      controller.markPrimaryDelivered();
    }
    assertThat(controller.shouldDeactivate()).isTrue();
  }

  @Test
  void afterDeactivate_shouldDeactivateIsFalse() {
    controller.activate();
    advanceSeconds(2);
    controller.markPrimaryDelivered(); // first record post-activate

    // Continuous primary delivery at 1 Hz across the recovery window.
    for (int i = 0; i < 11; i++) {
      advanceSeconds(1);
      controller.markPrimaryDelivered();
    }
    assertThat(controller.shouldDeactivate()).isTrue();

    controller.deactivate();

    assertThat(controller.isActive()).isFalse();
    assertThat(controller.shouldDeactivate()).isFalse();
  }
}
