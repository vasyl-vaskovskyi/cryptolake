package com.cryptolake.writer.gap;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.metrics.WriterMetrics;

/**
 * THE TRIAD: every gap MUST go through {@link #emit} which enforces the three-action requirement
 * (Tier 1 §5; design §2.8):
 *
 * <ol>
 *   <li>Increment the appropriate Micrometer counter ({@code writer_gap_records_written_total}).
 *   <li>Log a structured event ({@code gap_emitted} — Tier 5 H2).
 *   <li>Enqueue the gap envelope into {@link BufferManager} for archival (via coverage filter).
 * </ol>
 *
 * <p>Callers NEVER write a gap envelope directly to the buffer — only via this class. This is a
 * static-analysis-enforceable invariant: grep for {@code GapEnvelope.create} outside
 * {@link GapEmitter} → forbidden.
 *
 * <p>Ports Python's combination of {@code _emit_gap} + gap-triad logic from {@code consumer.py}
 * (design §2.8; mapping §5 Rule 5).
 *
 * <p>Thread safety: consume-loop thread only (T1).
 */
public final class GapEmitter {

  private static final StructuredLogger log = StructuredLogger.of(GapEmitter.class);

  private final BufferManager buffers;
  private final WriterMetrics metrics;
  private final CoverageFilter coverage;

  public GapEmitter(
      BufferManager buffers, WriterMetrics metrics, StructuredLogger ignoredLog, CoverageFilter coverage) {
    this.buffers = buffers;
    this.metrics = metrics;
    this.coverage = coverage;
  }

  /**
   * Emits a gap through the triad. Routes the gap through the coverage filter; if the filter
   * accepts it, the gap is buffered for archival.
   *
   * @param gap the gap envelope to emit
   * @param source "primary" or "backup"
   * @param topic Kafka topic the gap originated from
   * @param partition Kafka partition (-1 for synthetic)
   * @param offset Kafka offset (-1L for synthetic — Tier 5 M9)
   * @return {@code true} if the gap was written to the buffer; {@code false} if coverage-filtered
   */
  public boolean emit(GapEnvelope gap, String source, String topic, int partition, long offset) {
    // (1) Increment metric (Tier 1 §5 action 1)
    metrics.gapRecordsWritten(gap.exchange(), gap.symbol(), gap.stream(), gap.reason()).increment();

    // (2) Log structured event (Tier 1 §5 action 2; Tier 5 H2)
    log.info("gap_emitted",
        "exchange", gap.exchange(),
        "symbol", gap.symbol(),
        "stream", gap.stream(),
        "reason", gap.reason(),
        "gap_start_ts", gap.gapStartTs(),
        "gap_end_ts", gap.gapEndTs(),
        "source", source,
        "topic", topic,
        "partition", partition,
        "offset", offset);

    // (3) Route through coverage filter (Tier 1 §5 action 3)
    BrokerCoordinates coords = new BrokerCoordinates(topic, partition, offset);
    boolean accepted = coverage.handleGap(source, gap);
    if (accepted) {
      // Write to buffer for archival
      buffers.add(gap, coords, source);
      return true;
    }
    // Coverage filter suppressed it
    metrics.gapEnvelopesSuppressed(source, GapReasonsLocal.COVERED).increment();
    return false;
  }

  /**
   * Emits a gap directly to the buffer, bypassing the coverage filter. Used for shutdown flush of
   * pending gaps (design §3.4) and for gaps that are already known to be uncovered.
   *
   * @param gap the gap envelope to write directly
   * @param source "primary" or "backup"
   * @param topic Kafka topic
   * @param partition Kafka partition
   */
  public void emitUnfiltered(GapEnvelope gap, String source, String topic, int partition) {
    // (1) Increment metric
    metrics.gapRecordsWritten(gap.exchange(), gap.symbol(), gap.stream(), gap.reason()).increment();

    // (2) Log
    log.info("gap_emitted_unfiltered",
        "exchange", gap.exchange(),
        "symbol", gap.symbol(),
        "stream", gap.stream(),
        "reason", gap.reason(),
        "source", source);

    // (3) Buffer directly (bypass coverage filter)
    BrokerCoordinates coords = new BrokerCoordinates(topic, partition, -1L); // synthetic offset
    buffers.add(gap, coords, source);
  }
}
