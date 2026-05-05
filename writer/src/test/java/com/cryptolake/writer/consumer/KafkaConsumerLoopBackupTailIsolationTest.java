package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Isolation-boundary test for the continuous backup-topic tail (plan {@code
 * 2026-05-03-continuous-dual-source-tailing.md}, Task 3 — code-review Issue #1).
 *
 * <p>The tail is best-effort liveness; primary archiving MUST NOT be coupled to its health. A
 * {@link KafkaException}, broker disconnect, or auth failure on the tail must not stop the writer
 * from continuing to consume the primary topic.
 *
 * <p>This test injects a {@link Consumer} into {@link BackupTailConsumer} that throws {@code
 * KafkaException} on the first poll, then returns empty thereafter. Assertions:
 *
 * <ul>
 *   <li>Primary records continue to be processed by {@link RecordHandler}.
 *   <li>The {@code writer_backup_tail_errors_total} counter increments at least once.
 *   <li>The consume loop does NOT stop — it keeps running until {@link
 *       KafkaConsumerLoop#requestShutdown()} is called.
 * </ul>
 */
class KafkaConsumerLoopBackupTailIsolationTest {

  @Test
  @Timeout(10)
  void backupPollThrows_primaryStillProcessed_counterIncrements_loopKeepsRunning()
      throws Exception {
    // ── Primary: deliver one record on the first poll, then empty forever ────────────────────
    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> primaryKafka = mock(KafkaConsumer.class);
    ConsumerRecord<byte[], byte[]> primaryRec =
        new ConsumerRecord<>("binance.btcusdt.depth", 0, 0L, new byte[0], new byte[0]);
    when(primaryKafka.poll(any(Duration.class)))
        .thenReturn(
            new ConsumerRecords<>(
                Map.of(new TopicPartition("binance.btcusdt.depth", 0), List.of(primaryRec))))
        .thenAnswer(invocation -> ConsumerRecords.empty());

    // ── Backup tail: an injected Consumer that throws KafkaException on the first poll, then
    //    returns empty. BackupTailConsumer is final, so we can't subclass it — instead we feed
    //    it a Consumer impl whose poll() drives the failure path. start()/close() on the tail
    //    are unaffected; the throw originates inside the underlying consumer.
    AtomicInteger pollCount = new AtomicInteger(0);
    Consumer<byte[], byte[]> failingConsumer = new ThrowingFirstPollConsumer(pollCount);
    BackupTailConsumer tail =
        new BackupTailConsumer(failingConsumer, List.of("backup.binance.btcusdt.depth"));

    // ── Real WriterMetrics so we can assert the new backupTailErrors counter ─────────────────
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);

    FailoverController failover = mock(FailoverController.class);
    when(failover.isActive()).thenReturn(false);
    when(failover.shouldActivate()).thenReturn(false);
    when(failover.shouldDeactivate()).thenReturn(false);

    RecordHandler recordHandler = mock(RecordHandler.class);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    RecoveryCoordinator recovery = mock(RecoveryCoordinator.class);
    HourRotationScheduler rotator = mock(HourRotationScheduler.class);
    BufferManager buffers = mock(BufferManager.class);
    when(buffers.shouldFlushByInterval()).thenReturn(false);
    CoverageFilter coverage = mock(CoverageFilter.class);
    GapEmitter gaps = mock(GapEmitter.class);

    KafkaConsumerLoop loop =
        new KafkaConsumerLoop(
            primaryKafka,
            tail,
            List.of("binance.btcusdt.depth"),
            recordHandler,
            failover,
            committer,
            recovery,
            rotator,
            buffers,
            coverage,
            gaps,
            metrics);

    // ── Drive the loop on a worker thread ────────────────────────────────────────────────────
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread t =
        new Thread(
            () -> {
              try {
                loop.run();
              } catch (Throwable e) {
                error.set(e);
              }
            },
            "consume-loop-isolation-test");
    t.start();

    // Wait for at least 2 backup polls (so the throwing one and one subsequent empty one have
    // both run, proving the loop continues past the exception).
    long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (pollCount.get() < 2 && System.nanoTime() < deadlineNanos) {
      Thread.sleep(20);
    }

    loop.requestShutdown();
    t.join(2_000);

    if (error.get() != null) {
      throw new AssertionError("loop.run() threw unexpectedly", error.get());
    }

    // ── Assertions ───────────────────────────────────────────────────────────────────────────
    // 1. The loop kept running after the tail exception (multiple polls observed).
    assertThat(pollCount.get())
        .as("loop must continue after tail exception — pollCount should be >= 2")
        .isGreaterThanOrEqualTo(2);

    // 2. Primary record was still dispatched (isolation worked).
    verify(recordHandler, atLeastOnce()).handle(any(ConsumerRecord.class), anyBoolean());

    // 3. Counter incremented exactly once for the one thrown exception.
    assertThat(metrics.backupTailErrors().count())
        .as("writer_backup_tail_errors_total must increment for the caught tail exception")
        .isEqualTo(1.0);

    // 4. Loop terminated cleanly via requestShutdown — not via an uncaught throw.
    assertThat(t.isAlive()).as("consume loop must have terminated").isFalse();

    // 5. Sanity: the loop never tried to deactivate failover in this scenario.
    verify(failover, never()).deactivate();
  }

  /**
   * Minimal {@link Consumer} stub that throws {@link KafkaException} on the first poll and returns
   * empty thereafter. All other methods are no-ops or return reasonable defaults — the test only
   * exercises subscribe(List), poll(Duration), and close(Duration).
   */
  private static final class ThrowingFirstPollConsumer implements Consumer<byte[], byte[]> {
    private final AtomicInteger pollCount;

    ThrowingFirstPollConsumer(AtomicInteger pollCount) {
      this.pollCount = pollCount;
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
      int n = pollCount.incrementAndGet();
      if (n == 1) {
        throw new KafkaException("simulated tail failure (test)");
      }
      return ConsumerRecords.empty();
    }

    @Override
    public void subscribe(Collection<String> topics) {}

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {}

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {}

    @Override
    public void subscribe(Pattern pattern) {}

    @Override
    public void unsubscribe() {}

    @Override
    public void assign(Collection<TopicPartition> partitions) {}

    @Override
    public Set<TopicPartition> assignment() {
      return Set.of();
    }

    @Override
    public Set<String> subscription() {
      return Set.of();
    }

    @Override
    @Deprecated
    public ConsumerRecords<byte[], byte[]> poll(long timeoutMs) {
      return poll(Duration.ofMillis(timeoutMs));
    }

    @Override
    public void commitSync() {}

    @Override
    public void commitSync(Duration timeout) {}

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {}

    @Override
    public void commitAsync() {}

    @Override
    public void commitAsync(OffsetCommitCallback callback) {}

    @Override
    public void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {}

    @Override
    public void seek(TopicPartition partition, long offset) {}

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {}

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {}

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {}

    @Override
    public long position(TopicPartition partition) {
      return 0L;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
      return 0L;
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
      return null;
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
      return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(
        Set<TopicPartition> partitions, Duration timeout) {
      return Map.of();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
      return Map.of();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
      return List.of();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
      return List.of();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
      return Map.of();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
      return Map.of();
    }

    @Override
    public Set<TopicPartition> paused() {
      return Set.of();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {}

    @Override
    public void resume(Collection<TopicPartition> partitions) {}

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(
        Collection<TopicPartition> partitions, Duration timeout) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
      return Map.of();
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(
        Collection<TopicPartition> partitions, Duration timeout) {
      return Map.of();
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
      return OptionalLong.empty();
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
      return new ConsumerGroupMetadata("test-group");
    }

    @Override
    public void enforceRebalance() {}

    @Override
    public void enforceRebalance(String reason) {}

    @Override
    public void close() {}

    @Override
    public void close(Duration timeout) {}

    @Override
    public void wakeup() {}

    @Override
    public Uuid clientInstanceId(Duration timeout) {
      return Uuid.ZERO_UUID;
    }
  }
}
