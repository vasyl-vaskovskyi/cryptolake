package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * RED integration test for the continuous-dual-source-tailing fix (plan
 * {@code 2026-05-03-continuous-dual-source-tailing.md}, Task 2).
 *
 * <p>Proves the production bug: while {@link FailoverController#isActive()} is {@code false},
 * {@link KafkaConsumerLoop} never reaches the backup topic, so {@link CoverageFilter}'s per-source
 * data-freshness map never sees backup records. The TWO-COLLECTOR rule for gap suppression then
 * runs on stale {@code lastDataTsByStream[(stream, "backup")]} state and leaks false-positive
 * gaps when MAIN flaps briefly. The fix in Task 3 makes the loop tail backup unconditionally,
 * and this test will go GREEN.
 *
 * <p>Test design: a {@link BackupTailConsumer} wraps a {@link MockConsumer} pre-loaded with one
 * backup data envelope. {@link KafkaConsumerLoop} is driven for ~300 ms with {@code
 * failover.isActive()} stubbed to {@code false} throughout. Assertion: {@code
 * coverage.getLastDataTs("backup")} is positive (i.e. {@link CoverageFilter#handleData} was
 * called for a backup record). Pre-fix this assertion fails because the loop has no wiring to
 * the tail consumer; post-fix it passes.
 */
class KafkaConsumerLoopDualPollTest {

  @Test
  @Timeout(10)
  void coverageFilter_seesBackupRecord_withoutFailoverActive() throws Exception {
    // ── Mocks for I/O collaborators we don't drive ────────────────────────────────────────
    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> primaryKafka = mock(KafkaConsumer.class);
    when(primaryKafka.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    // ── Real backup tail consumer with one record loaded ──────────────────────────────────
    // MockConsumer requires either assign(...) OR subscribe(...), not both. BackupTailConsumer
    // calls subscribe(), so we initialize the partition state via rebalance() — the standard
    // MockConsumer idiom for driving a subscribe-mode consumer in tests.
    String backupTopic = "backup.binance.trades";
    TopicPartition backupTp = new TopicPartition(backupTopic, 0);
    MockConsumer<byte[], byte[]> backupKafka = new MockConsumer<>(OffsetResetStrategy.LATEST);
    Map<TopicPartition, Long> beginning = new HashMap<>();
    beginning.put(backupTp, 0L);
    backupKafka.updateBeginningOffsets(beginning);
    Map<TopicPartition, Long> end = new HashMap<>();
    end.put(backupTp, 1L);
    backupKafka.updateEndOffsets(end);

    BackupTailConsumer tail = new BackupTailConsumer(backupKafka, List.of(backupTopic));
    tail.start();
    backupKafka.rebalance(List.of(backupTp));

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    DataEnvelope backupEnv =
        DataEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "{\"e\":\"trade\",\"s\":\"BTCUSDT\",\"p\":\"50000\",\"q\":\"0.01\"}",
            1_777_326_113_000L,
            "binance-collector-backup_2026-04-27T21:41:23Z",
            42L,
            Clocks.fixed(1_777_326_113_915_016_714L));
    byte[] backupEnvBytes = codec.toJsonBytes(backupEnv);
    backupKafka.addRecord(
        new ConsumerRecord<>(backupTopic, 0, 0L, new byte[0], backupEnvBytes));

    // ── Real CoverageFilter — the assertion target ────────────────────────────────────────
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    ClockSupplier clock = Clocks.systemNanoClock();
    CoverageFilter coverage = new CoverageFilter(10.0, 30.0, metrics, clock);

    // ── FailoverController in INACTIVE state for the entire test ──────────────────────────
    FailoverController failover = mock(FailoverController.class);
    when(failover.isActive()).thenReturn(false);
    when(failover.shouldActivate()).thenReturn(false);
    when(failover.shouldDeactivate()).thenReturn(false);
    when(failover.pollBackup(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    // ── Real RecordHandler so a backup record routes through coverage.handleData ──────────
    SessionChangeDetector sessionDetector = mock(SessionChangeDetector.class);
    when(sessionDetector.observe(any(DataEnvelope.class), any(String.class)))
        .thenReturn(Optional.empty());
    DepthRecoveryGapFilter depthFilter = mock(DepthRecoveryGapFilter.class);
    RecoveryCoordinator recovery = mock(RecoveryCoordinator.class);
    when(recovery.checkOnFirstEnvelope(any(DataEnvelope.class))).thenReturn(null);
    BufferManager buffers = mock(BufferManager.class);
    when(buffers.add(any(DataEnvelope.class), any(), any(String.class)))
        .thenReturn(Optional.empty());
    when(buffers.shouldFlushByInterval()).thenReturn(false);
    GapEmitter gaps = mock(GapEmitter.class);

    RecordHandler recordHandler =
        new RecordHandler(
            codec,
            sessionDetector,
            depthFilter,
            coverage,
            failover,
            recovery,
            buffers,
            gaps,
            metrics,
            "backup.");

    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    HourRotationScheduler rotator = mock(HourRotationScheduler.class);

    // NOTE: the production KafkaConsumerLoop constructor does NOT yet take a BackupTailConsumer
    // — that's exactly the gap the dual-tailing fix will close. We construct the loop with the
    // CURRENT signature; the BackupTailConsumer is created above and held by this test only.
    // Pre-fix the loop has no path to consume from `tail`, so coverage stays at zero for the
    // backup source. Post-fix the loop will own a BackupTailConsumer and poll it every
    // iteration, and this test will be re-wired (Task 3) to pass `tail` to the constructor.
    KafkaConsumerLoop loop =
        new KafkaConsumerLoop(
            primaryKafka,
            List.of("binance.trades"),
            recordHandler,
            failover,
            committer,
            recovery,
            rotator,
            buffers,
            coverage,
            gaps,
            metrics);

    // ── Drive the loop briefly on a worker thread (mirrors KafkaConsumerLoopFailoverDeactivateTest)
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
            "consume-loop-test");
    t.start();
    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    loop.requestShutdown();
    primaryKafka.wakeup();
    t.join(2_000);

    if (error.get() != null) {
      throw new AssertionError("loop.run() threw", error.get());
    }

    // The bug: with failover inactive, the consume loop never tails backup, so the coverage
    // map's "backup" entry is still zero. After the dual-tailing fix the tail consumer is
    // polled every iteration regardless of failover state, the loaded record is dispatched
    // to RecordHandler, and CoverageFilter.handleData("backup", env) updates this timestamp.
    long backupLastTs = coverage.getLastDataTs("backup");
    assertThat(backupLastTs)
        .as(
            "CoverageFilter must see the backup record even when failover.isActive()==false; "
                + "pre-fix the consume loop never polls the BackupTailConsumer, so this stays at 0")
        .isPositive();
  }
}
