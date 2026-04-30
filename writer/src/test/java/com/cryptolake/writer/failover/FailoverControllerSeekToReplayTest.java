package com.cryptolake.writer.failover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Verifies that on failover.activate(), the backup consumer subscribes with a
 * ConsumerRebalanceListener that seeks each assigned partition to a timestamp
 * approximately silenceTimeout + 5s before now. This causes backup to replay
 * the bridging u-frames between primary's last record and backup's current head,
 * eliminating the spurious cross_source_pu_chain_break gap observed in chaos
 * test 01 (bug C).
 */
class FailoverControllerSeekToReplayTest {

  private static final Duration SILENCE_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration RECOVERY_WINDOW = Duration.ofSeconds(10);
  private static final long SEEK_BACK_BUFFER_MS = 5_000L;

  private AtomicLong fakeClockNs;
  private ClockSupplier clock;

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<byte[], byte[]> backupConsumer = mock(KafkaConsumer.class);

  private FailoverController controller;

  @BeforeEach
  void setUp() {
    fakeClockNs = new AtomicLong(1_000_000_000_000_000L); // some ns timestamp
    clock = fakeClockNs::get;
    WriterMetrics metrics =
        new WriterMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
    CoverageFilter coverage = new CoverageFilter(5.0, 10.0, metrics, clock);

    controller =
        new FailoverController(
            () -> backupConsumer,
            List.of("binance.btcusdt.depth", "binance.btcusdt.bookticker"),
            "backup.",
            SILENCE_TIMEOUT,
            RECOVERY_WINDOW,
            coverage,
            metrics,
            clock);
  }

  @Test
  void activate_subscribesWithRebalanceListenerAndSeeksToTimestamp() {
    controller.activate();

    @SuppressWarnings({"rawtypes", "unchecked"})
    ArgumentCaptor<ConsumerRebalanceListener> listenerCaptor =
        ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
    verify(backupConsumer).subscribe(anyCollection(), listenerCaptor.capture());
    ConsumerRebalanceListener listener = listenerCaptor.getValue();
    assertThat(listener).isNotNull();

    TopicPartition depthTp = new TopicPartition("backup.binance.btcusdt.depth", 0);
    TopicPartition bookTp = new TopicPartition("backup.binance.btcusdt.bookticker", 0);
    Set<TopicPartition> assigned = new HashSet<>();
    assigned.add(depthTp);
    assigned.add(bookTp);

    Map<TopicPartition, OffsetAndTimestamp> offsets = new HashMap<>();
    offsets.put(depthTp, new OffsetAndTimestamp(12345L, 0L));
    offsets.put(bookTp, new OffsetAndTimestamp(67890L, 0L));
    when(backupConsumer.offsetsForTimes(any())).thenReturn(offsets);

    listener.onPartitionsAssigned(assigned);

    long expectedTsMs =
        (clock.nowNs() / 1_000_000L) - SILENCE_TIMEOUT.toMillis() - SEEK_BACK_BUFFER_MS;

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<TopicPartition, Long>> tsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(backupConsumer).offsetsForTimes(tsCaptor.capture());
    Map<TopicPartition, Long> requestedTs = tsCaptor.getValue();
    assertThat(requestedTs).containsOnlyKeys(depthTp, bookTp);
    assertThat(requestedTs.get(depthTp)).isCloseTo(expectedTsMs, Offset.offset(50L));
    assertThat(requestedTs.get(bookTp)).isCloseTo(expectedTsMs, Offset.offset(50L));

    verify(backupConsumer).seek(depthTp, 12345L);
    verify(backupConsumer).seek(bookTp, 67890L);
  }

  @Test
  void onPartitionsAssigned_offsetsForTimesReturnsNull_doesNotSeek() {
    controller.activate();

    @SuppressWarnings({"rawtypes", "unchecked"})
    ArgumentCaptor<ConsumerRebalanceListener> listenerCaptor =
        ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
    verify(backupConsumer).subscribe(anyCollection(), listenerCaptor.capture());
    ConsumerRebalanceListener listener = listenerCaptor.getValue();

    TopicPartition tp = new TopicPartition("backup.binance.btcusdt.depth", 0);
    Map<TopicPartition, OffsetAndTimestamp> offsets = new HashMap<>();
    offsets.put(tp, null); // Kafka contract: null means no record at or before that ts
    when(backupConsumer.offsetsForTimes(any())).thenReturn(offsets);

    listener.onPartitionsAssigned(Set.of(tp));

    verify(backupConsumer, never()).seek(eq(tp), anyLong());
  }

  @Test
  void onPartitionsRevoked_isNoOp() {
    controller.activate();

    @SuppressWarnings({"rawtypes", "unchecked"})
    ArgumentCaptor<ConsumerRebalanceListener> listenerCaptor =
        ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
    verify(backupConsumer).subscribe(anyCollection(), listenerCaptor.capture());
    ConsumerRebalanceListener listener = listenerCaptor.getValue();

    listener.onPartitionsRevoked(Set.of(new TopicPartition("backup.binance.btcusdt.depth", 0)));

    verify(backupConsumer, never()).seek(any(), anyLong());
    verify(backupConsumer, never()).offsetsForTimes(any());
  }
}
