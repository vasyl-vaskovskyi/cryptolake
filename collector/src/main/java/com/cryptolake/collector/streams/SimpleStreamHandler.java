package com.cryptolake.collector.streams;

import com.cryptolake.collector.CollectorSession;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.gap.SeqGap;
import com.cryptolake.collector.gap.SessionSeqTracker;
import com.cryptolake.collector.producer.KafkaProducerBridge;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Handles simple streams: {@code trades}, {@code bookticker}, {@code funding_rate}, {@code
 * liquidations}, {@code depth_snapshot}, and {@code open_interest}.
 *
 * <p>Ports {@code SimpleStreamHandler} from {@code src/collector/streams/simple.py}. For each
 * message: checks the session seq, emits a {@code session_seq_skip} gap on mismatch, builds a
 * {@link DataEnvelope} and produces it.
 *
 * <p>Thread safety: each instance is confined to a single virtual thread (one WebSocket
 * connection).
 */
public final class SimpleStreamHandler implements StreamHandler {

  private static final StructuredLogger log = StructuredLogger.of(SimpleStreamHandler.class);

  private final String exchange;
  private final CollectorSession session;
  private final KafkaProducerBridge producer;
  private final String streamName;
  private final GapEmitter gapEmitter;
  private final ClockSupplier clock;

  /** Per-symbol session seq trackers — allocated lazily. */
  private final Map<String, SessionSeqTracker> trackers = new HashMap<>();

  public SimpleStreamHandler(
      String exchange,
      CollectorSession session,
      KafkaProducerBridge producer,
      String streamName,
      GapEmitter gapEmitter,
      ClockSupplier clock) {
    this.exchange = exchange;
    this.session = session;
    this.producer = producer;
    this.streamName = streamName;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
  }

  @Override
  public void handle(String symbol, String rawText, Long exchangeTs, long sessionSeq) {
    // Check session seq for gaps
    SessionSeqTracker tracker = trackers.computeIfAbsent(symbol, k -> new SessionSeqTracker());
    Optional<SeqGap> gap = tracker.check(sessionSeq);
    if (gap.isPresent()) {
      SeqGap g = gap.get();
      log.info(
          "session_seq_gap_detected",
          "symbol",
          symbol,
          "stream",
          streamName,
          "expected",
          g.expected(),
          "actual",
          g.actual());
      gapEmitter.emit(
          symbol,
          streamName,
          sessionSeq,
          "session_seq_skip",
          "Expected seq " + g.expected() + " got " + g.actual());
    }

    // Build and produce data envelope
    DataEnvelope env =
        DataEnvelope.create(
            exchange,
            symbol,
            streamName,
            rawText,
            exchangeTs != null ? exchangeTs : 0L,
            session.sessionId(),
            sessionSeq,
            clock);
    producer.produce(env);
  }
}
