package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.state.StreamCheckpoint;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OffsetCommitCoordinator}.
 *
 * <p>Ports: Python's {@code test_offset_commit_coordinator.py}. Exercises the empty-buffer
 * short-circuit (no commit issued when nothing to flush — Tier 1 §4 ordering invariant is vacuously
 * upheld) and the seed/cache semantics for durable checkpoints. Full flush-order coverage needs
 * a MockConsumer-driven integration test under chaos suite §8 (§chaos_05, §chaos_07) which is
 * disabled pending the Testcontainers stack.
 */
class OffsetCommitCoordinatorTest {

  // Tier 1 §4 — no buffers ⇒ no commit path taken; returns 0. Passing null consumer is safe
  // because flushAndCommit short-circuits on empty results before any consumer access.
  @Test
  void flushAndCommit_emptyBuffers_returnsZero_noConsumerAccess() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test", 100, 60, codec);

    OffsetCommitCoordinator coord = new OffsetCommitCoordinator(
        null, // consumer — not touched on empty path
        null, null, null, null,
        metrics, Clocks.systemNanoClock());

    int n = coord.flushAndCommit(buffers);
    assertThat(n).isEqualTo(0);
  }

  // Design §2.1 — recovery seeds durable checkpoints; later lookups reflect the seeded state
  @Test
  void seedDurableCheckpoints_laterLookup_returnsSeeded() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);

    OffsetCommitCoordinator coord = new OffsetCommitCoordinator(
        null, null, null, null, null, metrics, Clocks.systemNanoClock());

    StreamKey key = new StreamKey("binance", "btcusdt", "trades");
    StreamCheckpoint checkpoint = new StreamCheckpoint(
        "binance", "btcusdt", "trades",
        "2024-01-15T14:00:00Z", "col_sess", null);

    coord.seedDurableCheckpoints(Map.of(key, checkpoint));

    Map<StreamKey, StreamCheckpoint> live = coord.durableCheckpoints();
    assertThat(live).containsEntry(key, checkpoint);
  }

  // Durable checkpoints view is an unmodifiable projection (callers can't mutate committed state)
  @Test
  void durableCheckpoints_returnsUnmodifiableView() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);

    OffsetCommitCoordinator coord = new OffsetCommitCoordinator(
        null, null, null, null, null, metrics, Clocks.systemNanoClock());

    Map<StreamKey, StreamCheckpoint> view = coord.durableCheckpoints();
    assertThat(view).isEmpty();
    // Mutation attempt should throw
    org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
        () -> view.put(new StreamKey("x", "y", "z"),
            new StreamCheckpoint("x", "y", "z", "2024-01-01T00:00:00Z", "s", null)));
  }
}
