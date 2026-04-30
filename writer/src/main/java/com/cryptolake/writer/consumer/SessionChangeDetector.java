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
 *
 * <h3>TWO-COLLECTOR rule — per-source session tracking</h3>
 *
 * <p>Session marks are kept <strong>per (stream, source)</strong>, not per stream. A
 * <em>cross-source switch</em> (e.g. MAIN→BACKUP at the moment MAIN dies) is the writer's failover
 * mechanism doing its job — both collectors have full coverage of the transition window, so no
 * gap is emitted. Only a <em>within-source</em> session change (the SAME source's session_id
 * differs from its previous envelope, i.e. that specific collector restarted) is a gap candidate.
 * That candidate is then routed through {@link CoverageFilter}, which suppresses it if the OTHER
 * source's data covered the down window — which is the steady state under the TWO-COLLECTOR rule.
 *
 * <p>Thread safety: consume-loop thread only (T1). State map owned by this class; no locking (Tier
 * 5 A5).
 */
public final class SessionChangeDetector {

  private static final Logger log = LoggerFactory.getLogger(SessionChangeDetector.class);

  private final GapEmitter gaps;
  private final CoverageFilter coverage;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /** Composite key (stream + source) for per-source session tracking. */
  private record SessionKey(StreamKey stream, String source) {}

  /** Per-(stream, source) last-seen session mark. */
  private final Map<SessionKey, SessionMark> lastSession = new HashMap<>();

  public SessionChangeDetector(
      GapEmitter gaps, CoverageFilter coverage, WriterMetrics metrics, ClockSupplier clock) {
    this.gaps = gaps;
    this.coverage = coverage;
    this.metrics = metrics;
    this.clock = clock;
  }

  /**
   * Observes an envelope and emits a gap if the collector session ID changed within the SAME
   * source since the last envelope for this stream from that source.
   *
   * <p>Cross-source switches (MAIN→BACKUP or BACKUP→MAIN) are the writer's failover mechanism
   * working as designed and never emit a gap here — under the TWO-COLLECTOR rule they are not
   * data events. A gap is only a candidate when a specific collector's session_id changes
   * within its own envelope stream.
   *
   * @param env the incoming data envelope
   * @param source "primary" or "backup" — the source the writer received this envelope from
   * @return the gap envelope if a within-source session change was detected, otherwise empty
   */
  public Optional<GapEnvelope> observe(DataEnvelope env, String source) {
    StreamKey streamKey = new StreamKey(env.exchange(), env.symbol(), env.stream());
    SessionKey key = new SessionKey(streamKey, source);
    SessionMark prev = lastSession.get(key);

    // Update the session mark for this (stream, source) pair
    lastSession.put(key, new SessionMark(env.collectorSessionId(), env.receivedAt()));

    if (prev == null) {
      // First envelope for this stream from this source — no within-source change to detect.
      // (This includes the very first BACKUP envelope after MAIN dies: it's a cross-source
      // switch handled silently by the failover controller, not a gap.)
      return Optional.empty();
    }

    if (prev.sessionId().equals(env.collectorSessionId())) {
      return Optional.empty(); // Same session within this source — no change
    }

    // Within-source session change detected — emit gap candidate (CoverageFilter decides
    // whether to archive based on the OTHER source's coverage of the down window).
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
    log.info(
        "LIFECYCLE WITHIN_SOURCE_SESSION_CHANGE source={} stream={} prev_session_id={}"
            + " new_session_id={} — that specific collector restarted; emitting gap candidate"
            + " (will be suppressed by CoverageFilter if OTHER source covered).",
        source,
        env.stream(),
        prev.sessionId(),
        env.collectorSessionId());

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
