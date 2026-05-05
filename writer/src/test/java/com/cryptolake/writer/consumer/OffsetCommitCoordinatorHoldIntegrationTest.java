package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.durability.DiskFullHoldController;
import com.cryptolake.writer.durability.PgOutageHoldController;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.recovery.SealedFileIndex;
import com.cryptolake.writer.state.StateManager;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the DiskFull / PgOutage hold controller wiring inside {@link
 * OffsetCommitCoordinator#flushAndCommit}. Uses real controller instances (no mocks) so the
 * state-machine semantics are exercised end-to-end. Outer collaborators ({@link KafkaConsumer},
 * {@link DurableAppender}, {@link StateManager}) are Mockito-mocked because they are out of test
 * scope.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-disk-full-pg-outage-wiring-design.md.
 */
class OffsetCommitCoordinatorHoldIntegrationTest {

  private static final String EXCHANGE = "binance";
  private static final List<DiskFullHoldController.SymbolStream> DISK_SS =
      List.of(new DiskFullHoldController.SymbolStream("btcusdt", "trades"));
  private static final List<PgOutageHoldController.SymbolStream> PG_SS =
      List.of(new PgOutageHoldController.SymbolStream("btcusdt", "trades"));

  private static DiskFullHoldController newDiskHoldNeverFreeing() {
    AtomicBoolean diskFree = new AtomicBoolean(false); // probe always reports "still full"
    return new DiskFullHoldController(
        Clocks.systemNanoClock(), diskFree::get, (sym, str, seq, r, d, s, e) -> {}, DISK_SS);
  }

  private static PgOutageHoldController newPgHoldNeverRecovering() {
    AtomicBoolean pgUp = new AtomicBoolean(false); // probe always reports "still down"
    return new PgOutageHoldController(
        Clocks.systemNanoClock(), pgUp::get, (sym, str, seq, r, d, s, e) -> {}, PG_SS);
  }

  /** Test 3: hold pre-active → flushAndCommit early-returns 0 without touching any collaborator. */
  @Test
  void flushAndCommit_returnsZero_immediately_whenDiskHoldAlreadyActive() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task1", 100, 60, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    // Pre-activate disk hold so the gate short-circuits before any I/O.
    diskHold.onWriteError(new IOException("No space left on device"));
    assertThat(diskHold.isHoldActive()).isTrue();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    int n = coord.flushAndCommit(buffers);

    assertThat(n).isEqualTo(0);
    // No collaborators touched — early-return happened before flushAll().
    verify(consumer, never()).commitSync(any(Map.class));
    try {
      verify(appender, never()).appendAndFsync(any(), any());
    } catch (IOException unreachable) {
      throw new AssertionError(unreachable);
    }
  }
}
