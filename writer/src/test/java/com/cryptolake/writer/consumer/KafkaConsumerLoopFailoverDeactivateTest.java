package com.cryptolake.writer.consumer;

import static org.mockito.ArgumentMatchers.any;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration test for KafkaConsumerLoop's bug-B wiring: markPrimaryDelivered is called for each
 * primary record and deactivate() fires when shouldDeactivate is true.
 */
class KafkaConsumerLoopFailoverDeactivateTest {

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<byte[], byte[]> primary = mock(KafkaConsumer.class);

  private FailoverController failover;
  private RecordHandler recordHandler;
  private OffsetCommitCoordinator committer;
  private RecoveryCoordinator recovery;
  private HourRotationScheduler rotator;
  private BufferManager buffers;
  private CoverageFilter coverage;
  private GapEmitter gaps;
  private WriterMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new WriterMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
    failover = mock(FailoverController.class);
    recordHandler = mock(RecordHandler.class);
    committer = mock(OffsetCommitCoordinator.class);
    recovery = mock(RecoveryCoordinator.class);
    rotator = mock(HourRotationScheduler.class);
    buffers = mock(BufferManager.class);
    coverage = mock(CoverageFilter.class);
    gaps = mock(GapEmitter.class);

    when(failover.isActive()).thenReturn(true);
    when(failover.shouldActivate()).thenReturn(false);
  }

  private ConsumerRecord<byte[], byte[]> stubRecord(String topic) {
    return new ConsumerRecord<>(topic, 0, 0L, new byte[0], new byte[0]);
  }

  @Test
  @Timeout(5)
  void primaryRecord_callsMarkPrimaryDelivered() {
    ConsumerRecord<byte[], byte[]> rec = stubRecord("binance.btcusdt.depth");
    when(primary.poll(any(Duration.class)))
        .thenReturn(
            new ConsumerRecords<>(
                Map.of(new TopicPartition("binance.btcusdt.depth", 0), List.of(rec))))
        .thenAnswer(invocation -> ConsumerRecords.empty());

    when(failover.shouldDeactivate()).thenReturn(false);
    when(failover.pollBackup(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    KafkaConsumerLoop loop =
        new KafkaConsumerLoop(
            primary,
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

    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread t =
        new Thread(
            () -> {
              try {
                loop.run();
              } catch (Throwable e) {
                error.set(e);
              }
            });
    t.start();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    loop.requestShutdown();
    try {
      t.join(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (error.get() != null) {
      throw new AssertionError("loop.run() threw", error.get());
    }

    verify(failover, atLeastOnce()).markPrimaryDelivered();
    verify(failover, never()).deactivate();
  }

  @Test
  @Timeout(5)
  void shouldDeactivateTrue_callsDeactivateOnce() {
    when(primary.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
    when(failover.shouldDeactivate()).thenReturn(true).thenReturn(false);
    when(failover.pollBackup(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    KafkaConsumerLoop loop =
        new KafkaConsumerLoop(
            primary,
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

    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread t =
        new Thread(
            () -> {
              try {
                loop.run();
              } catch (Throwable e) {
                error.set(e);
              }
            });
    t.start();
    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    loop.requestShutdown();
    try {
      t.join(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (error.get() != null) {
      throw new AssertionError("loop.run() threw", error.get());
    }

    verify(failover, atLeastOnce()).deactivate();
  }
}
