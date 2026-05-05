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
import java.util.concurrent.atomic.AtomicLong;
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

  /**
   * Monotonic broker-offset counter for {@link #enqueueOneBufferedRecord(BufferManager)}. Each call
   * must use a unique {@code (topic, partition, offset)} triple so the BufferManager's
   * archive-layer dedup window does not silently drop the second/third add inside a single test
   * (the dedup window is per-BufferManager-instance, so unique-across-tests is overkill but
   * harmless; the important property is unique-within-a-test).
   */
  private static final AtomicLong NEXT_OFFSET = new AtomicLong(0L);

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

  /**
   * Test 1: ENOSPC IOException on appendAndFsync triggers diskHold.onWriteError, hold becomes
   * active, flushAndCommit returns 0 cleanly (no rethrow), no commitSync call.
   */
  @Test
  void flushAndCommit_returnsZero_whenDiskHoldEntered_byEnospcIoException() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    // BufferManager with one buffered record so flushAll() returns a non-empty result.
    BufferManager buffers = new BufferManager("/tmp/test-hold-task2-1", 100, 60, codec);
    enqueueOneBufferedRecord(buffers);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    // Compressor returns a small byte[] so the for-loop reaches appendAndFsync.
    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1, 2, 3});
    // Appender throws ENOSPC on the first (and only) call.
    org.mockito.Mockito.doThrow(new IOException("No space left on device"))
        .when(appender)
        .appendAndFsync(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

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
    assertThat(diskHold.isHoldActive()).isTrue();
    verify(appender).appendAndFsync(any(), any()); // called exactly once
    verify(consumer, never()).commitSync(any(Map.class));
    verify(stateManager, never()).saveStatesAndCheckpoints(any(), any());
  }

  /**
   * Test 2: non-ENOSPC IOException on appendAndFsync still throws UncheckedIOException (preserves
   * today's behavior). Disk hold is NOT entered.
   */
  @Test
  void flushAndCommit_rethrowsUncheckedIO_whenIoExceptionIsNotEnospc() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task2-2", 100, 60, codec);
    enqueueOneBufferedRecord(buffers);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1});
    org.mockito.Mockito.doThrow(new IOException("Permission denied"))
        .when(appender)
        .appendAndFsync(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

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

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(java.io.UncheckedIOException.class)
        .hasMessageContaining("File write failed");

    assertThat(diskHold.isHoldActive()).isFalse();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Test 5: three consecutive PG failures → recordPgFailure on each → threshold reached →
   * pgHold.isHoldActive flips true → third call returns 0 cleanly (no exception). Calls 1 and 2
   * still throw — today's "fail fast on first PG failure" behavior is preserved below the
   * threshold.
   */
  @Test
  void flushAndCommit_returnsZero_whenPgHoldEntered_afterThreeConsecutiveFailures()
      throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task3", 100, 60, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1});
    // Appender succeeds — PG is the failure point.
    // saveStatesAndCheckpoints throws on every call.
    org.mockito.Mockito.doThrow(new com.cryptolake.writer.state.CryptoLakeStateException("pg down"))
        .when(stateManager)
        .saveStatesAndCheckpoints(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

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

    // Buffer a fresh record before each call so flushAll() returns non-empty.
    enqueueOneBufferedRecord(buffers);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(com.cryptolake.writer.state.CryptoLakeStateException.class);

    enqueueOneBufferedRecord(buffers);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(com.cryptolake.writer.state.CryptoLakeStateException.class);

    enqueueOneBufferedRecord(buffers);
    int n = coord.flushAndCommit(buffers);

    assertThat(n).isEqualTo(0);
    assertThat(pgHold.isHoldActive()).isTrue();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a non-empty
   * FlushResult. Uses {@link BufferManager#add(com.cryptolake.common.envelope.DataEnvelope,
   * com.cryptolake.common.envelope.BrokerCoordinates, String)} — the writer's real ingest path.
   *
   * <p>Symbol/stream are {@code btcusdt}/{@code trades} (matches the {@code DISK_SS}/{@code PG_SS}
   * symbol-stream lists). Date/hour are derived inside {@code BufferManager.route} from {@code
   * env.receivedAt()} = now — the specific hour does not matter for these tests; only that one
   * record is buffered and ready to flush.
   */
  private static void enqueueOneBufferedRecord(BufferManager buffers) {
    com.cryptolake.common.envelope.DataEnvelope env =
        com.cryptolake.common.envelope.DataEnvelope.create(
            EXCHANGE,
            "btcusdt",
            "trades",
            "{}",
            1_000_000L,
            "binance-collector-01_2026-05-05T14:00:00Z",
            1L,
            Clocks.systemNanoClock());
    com.cryptolake.common.envelope.BrokerCoordinates coords =
        new com.cryptolake.common.envelope.BrokerCoordinates(
            "binance.trades", 0, NEXT_OFFSET.getAndIncrement());
    buffers.add(env, coords, "primary");
  }
}
