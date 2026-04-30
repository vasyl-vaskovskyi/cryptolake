package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
            () ->
                new KafkaConsumer<>(
                    stubConsumerProps(), new ByteArrayDeserializer(), new ByteArrayDeserializer()),
            List.of("binance.btcusdt.depth"),
            "backup.",
            SILENCE_TIMEOUT,
            RECOVERY_WINDOW,
            coverage,
            metrics,
            clock);
  }

  /** Minimal props — KafkaConsumer construction is offline (no broker contact). */
  private static Properties stubConsumerProps() {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-hysteresis");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return p;
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
    controller.markPrimaryDelivered(fakeClockNs.get()); // first record post-activate

    advanceSeconds(5); // t=1007s, only 5s of recovery — window is 10s
    controller.markPrimaryDelivered(fakeClockNs.get());
    assertThat(controller.shouldDeactivate()).isFalse();
  }

  @Test
  void recoveryObserving_afterStabilityWindow_shouldDeactivateIsTrue() {
    controller.activate();
    advanceSeconds(2);
    long firstRecoveryNs = fakeClockNs.get();
    controller.markPrimaryDelivered(firstRecoveryNs);

    advanceSeconds(11); // 11s after first recovery — window elapsed
    controller.markPrimaryDelivered(fakeClockNs.get());

    assertThat(controller.shouldDeactivate()).isTrue();
  }

  @Test
  void recoveryObserving_silenceLongerThanTimeout_resetsRecovery() {
    controller.activate();
    advanceSeconds(2);
    controller.markPrimaryDelivered(fakeClockNs.get()); // recovery starts at t=1002s

    advanceSeconds(8); // t=1010s — primary then goes silent
    // No markPrimaryDelivered between t=1010s and t=1020s (silence > 5s timeout)

    advanceSeconds(15); // t=1025s
    // Primary delivers again — recovery should restart
    controller.markPrimaryDelivered(fakeClockNs.get());

    // Even though >10s elapsed since the FIRST mark at t=1002s, the silence
    // gap reset the recovery clock. Only 0s of fresh recovery — must be false.
    assertThat(controller.shouldDeactivate()).isFalse();

    advanceSeconds(11); // t=1036s — now 11s of fresh continuous recovery
    controller.markPrimaryDelivered(fakeClockNs.get());

    assertThat(controller.shouldDeactivate()).isTrue();
  }

  @Test
  void afterDeactivate_shouldDeactivateIsFalse() {
    controller.activate();
    advanceSeconds(2);
    controller.markPrimaryDelivered(fakeClockNs.get());
    advanceSeconds(11);
    controller.markPrimaryDelivered(fakeClockNs.get());
    assertThat(controller.shouldDeactivate()).isTrue();

    controller.deactivate();

    assertThat(controller.isActive()).isFalse();
    assertThat(controller.shouldDeactivate()).isFalse();
  }
}
