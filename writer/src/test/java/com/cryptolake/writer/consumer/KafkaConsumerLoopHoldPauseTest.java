package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link KafkaConsumerLoop#applyHoldPauseState()} — the testability seam that gates
 * Kafka consumption on hold state. We exercise the seam directly rather than driving the full
 * consume loop because the loop has many dependencies and timing-sensitive iteration.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
class KafkaConsumerLoopHoldPauseTest {

  @SuppressWarnings("unchecked")
  private KafkaConsumer<byte[], byte[]> newMockConsumer(Set<TopicPartition> assignment) {
    KafkaConsumer<byte[], byte[]> c = mock(KafkaConsumer.class);
    when(c.assignment()).thenReturn(assignment);
    return c;
  }

  private KafkaConsumerLoop newLoop(
      KafkaConsumer<byte[], byte[]> consumer, OffsetCommitCoordinator committer) {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    return new KafkaConsumerLoop(
        consumer,
        null, // BackupTailConsumer — not exercised by this test
        List.of("binance.trades"),
        mock(RecordHandler.class),
        mock(FailoverController.class),
        committer,
        mock(RecoveryCoordinator.class),
        mock(HourRotationScheduler.class),
        mock(BufferManager.class),
        mock(CoverageFilter.class),
        mock(GapEmitter.class),
        metrics);
  }

  /** Test 1: no pause/resume calls when hold is never active. */
  @Test
  void applyHoldPauseState_doesNotPauseOrResume_whenHoldNeverActive() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(false);

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    // Drive the seam several times.
    loop.applyHoldPauseState();
    loop.applyHoldPauseState();
    loop.applyHoldPauseState();

    verify(consumer, never()).pause(any());
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }

  /** Test 2: pause fires on the first iteration where hold becomes active. */
  @Test
  void applyHoldPauseState_pausesPrimary_onActiveEdge() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(false, true); // first call: false, second: true

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // hold inactive → no-op
    loop.applyHoldPauseState(); // hold flipped active → pause

    verify(consumer, times(1)).pause(assignment);
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isTrue();
  }

  /**
   * Test 3: hold stays active across many iterations — pause is re-issued each iteration (so
   * rebalance-mid-hold is covered) but the LIFECYCLE log line is emitted exactly once via the
   * edge-detect. We verify edge-detect via {@link KafkaConsumerLoop#isLastKnownHeldForTest()}; the
   * underlying pause call may run multiple times (test asserts at-least-once).
   */
  @Test
  void applyHoldPauseState_doesNotResetEdge_whileHoldStaysActive() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(true);

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // edge: false→true; pause + log
    loop.applyHoldPauseState(); // no edge; pause again (rebalance defense), no log
    loop.applyHoldPauseState(); // no edge; pause again (rebalance defense), no log

    // Edge state is true after first call and stays true.
    assertThat(loop.isLastKnownHeldForTest()).isTrue();
    // pause is called every iteration while held (3 total here); resume never.
    verify(consumer, times(3)).pause(assignment);
    verify(consumer, never()).resume(any());
  }

  /** Test 4: resume fires on the first iteration where hold becomes inactive. */
  @Test
  void applyHoldPauseState_resumesPrimary_onInactiveEdge() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(true, false); // first: true, second: false

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // edge: false→true; pause
    loop.applyHoldPauseState(); // edge: true→false; resume

    verify(consumer, times(1)).resume(assignment);
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }
}
