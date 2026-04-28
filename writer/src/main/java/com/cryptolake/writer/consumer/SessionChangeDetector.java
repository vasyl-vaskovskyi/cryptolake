package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects runtime collector session changes and emits a {@code "collector_restart"} gap.
 *
 * <p>Ports Python's {@code WriterConsumer._check_session_change} (design §2.2; design §4.2).
 * Maintains a {@code Map<StreamKey, SessionMark>} with the last-seen session ID per stream.
 *
 * <p>Thread safety: consume-loop thread only (T1). State dict owned by this class; no locking (Tier
 * 5 A5).
 */
public final class SessionChangeDetector {

  private static final Logger log = LoggerFactory.getLogger(SessionChangeDetector.class);

  private final GapEmitter gaps;
  private final CoverageFilter coverage;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /** Per-stream last-seen session mark. */
  private final Map<StreamKey, SessionMark> lastSession = new HashMap<>();

  public SessionChangeDetector(
      GapEmitter gaps, CoverageFilter coverage, WriterMetrics metrics, ClockSupplier clock) {
    this.gaps = gaps;
    this.coverage = coverage;
    this.metrics = metrics;
    this.clock = clock;
  }

  /**
   * Observes an envelope and emits a gap if the collector session ID changed since the last
   * envelope for this stream.
   *
   * <p>Ports {@code WriterConsumer._check_session_change}.
   *
   * @param env the incoming data envelope
   * @param source "primary" or "backup"
   * @return the gap envelope if a session change was detected, otherwise empty
   */
  public Optional<GapEnvelope> observe(DataEnvelope env, String source) {
    StreamKey key = new StreamKey(env.exchange(), env.symbol(), env.stream());
    SessionMark prev = lastSession.get(key);

    // Update the session mark for this stream
    lastSession.put(key, new SessionMark(env.collectorSessionId(), env.receivedAt()));

    if (prev == null) {
      return Optional.empty(); // First envelope for this stream — no change to detect
    }

    if (prev.sessionId().equals(env.collectorSessionId())) {
      return Optional.empty(); // Same session — no change
    }

    // Session change detected — emit gap
    log.info(
        "session_change_detected",
        "exchange",
        env.exchange(),
        "symbol",
        env.symbol(),
        "stream",
        env.stream(),
        "prev_session",
        prev.sessionId(),
        "new_session",
        env.collectorSessionId(),
        "source",
        source);

    metrics.sessionGapsDetected(env.exchange(), env.symbol(), env.stream()).increment();

    GapEnvelope gap =
        GapEnvelope.create(
            env.exchange(),
            env.symbol(),
            env.stream(),
            env.collectorSessionId(),
            -1L, // writer-injected sentinel (Tier 5 M10)
            prev.receivedAtNs(),
            env.receivedAt(),
            "collector_restart",
            "collector_session_id changed from "
                + prev.sessionId()
                + " to "
                + env.collectorSessionId(),
            clock);

    return Optional.of(gap);
  }
}
