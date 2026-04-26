package com.cryptolake.collector.gap;

import com.cryptolake.collector.metrics.CollectorMetrics;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.collector.producer.OverflowWindow;
import com.cryptolake.collector.CollectorSession;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;

/**
 * THE TRIAD: every gap emission in the collector module goes through this class (Tier 1 §5;
 * design §2.7).
 *
 * <p>Each {@link #emit} call does:
 * <ol>
 *   <li>Increments {@code collector_gaps_detected_total{exchange, symbol, stream, reason}}.
 *   <li>Logs a structured {@code gap_emitted} event (Tier 5 H2).
 *   <li>Builds a {@link GapEnvelope} and produces it to Kafka via {@link KafkaProducerBridge}.
 * </ol>
 *
 * <p>No caller in the module is allowed to produce a gap envelope directly — only via this class.
 *
 * <p>Thread safety: delegates to thread-safe collaborators. May be called from any virtual thread.
 */
public final class GapEmitter {

  private static final StructuredLogger log = StructuredLogger.of(GapEmitter.class);

  private final String exchange;
  private final CollectorSession session;
  private final KafkaProducerBridge producer;
  private final CollectorMetrics metrics;
  private final ClockSupplier clock;

  public GapEmitter(
      String exchange,
      CollectorSession session,
      KafkaProducerBridge producer,
      CollectorMetrics metrics,
      ClockSupplier clock) {
    this.exchange = exchange;
    this.session = session;
    this.producer = producer;
    this.metrics = metrics;
    this.clock = clock;
  }

  /**
   * Emits a gap with {@code now} as both gap_start_ts and gap_end_ts.
   */
  public void emit(
      String symbol, String stream, long sessionSeq, String reason, String detail) {
    long now = clock.nowNs();
    emitWithTimestamps(symbol, stream, sessionSeq, reason, detail, now, now);
  }

  /**
   * Emits a gap with explicit start and end timestamps.
   */
  public void emitWithTimestamps(
      String symbol,
      String stream,
      long sessionSeq,
      String reason,
      String detail,
      long gapStartTs,
      long gapEndTs) {

    // (1) Metric increment (Tier 1 §5 action 1)
    metrics.gapsDetected(exchange, symbol, stream, reason).increment();

    // (2) Structured log (Tier 1 §5 action 2; Tier 5 H2)
    log.info(
        "gap_emitted",
        "exchange", exchange,
        "symbol", symbol,
        "stream", stream,
        "reason", reason,
        "gap_start_ts", gapStartTs,
        "gap_end_ts", gapEndTs,
        "session_seq", sessionSeq,
        "detail", detail);

    // (3) Build and produce gap envelope (Tier 1 §5 action 3)
    GapEnvelope gap =
        GapEnvelope.create(
            exchange,
            symbol,
            stream,
            session.sessionId(),
            sessionSeq,
            gapStartTs,
            gapEndTs,
            reason,
            detail,
            clock);
    producer.produceGap(gap);
  }

  /**
   * Emits a {@code buffer_overflow} gap when recovering from an overflow window.
   */
  public void emitOverflowRecovery(String symbol, String stream, OverflowWindow window) {
    long now = clock.nowNs();
    String detail =
        "Producer buffer was full; "
            + window.dropped()
            + " messages dropped for "
            + stream
            + "/"
            + symbol;
    emitWithTimestamps(symbol, stream, -1L, "buffer_overflow", detail, window.startTsNs(), now);
  }
}
