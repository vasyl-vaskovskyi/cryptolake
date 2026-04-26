package com.cryptolake.collector.gap;

import java.util.Optional;

/**
 * Tracks session sequence numbers for in-session gap detection.
 *
 * <p>Ports {@code SessionSeqTracker} from {@code src/collector/gap_detector.py}.
 *
 * <p>Thread safety: caller-confined (one tracker per (symbol, stream) pair, owned by a single
 * virtual thread).
 */
public final class SessionSeqTracker {

  /** Sentinel: no message seen yet. */
  private static final long UNSET = -1L;

  private long lastSeq = UNSET;

  /**
   * Checks the given sequence number against the expected next value.
   *
   * @return {@link Optional#empty()} if no gap, or a {@link SeqGap} if a gap was detected
   */
  public Optional<SeqGap> check(long seq) {
    if (lastSeq == UNSET) {
      lastSeq = seq;
      return Optional.empty();
    }
    long expected = lastSeq + 1;
    lastSeq = seq;
    if (seq != expected) {
      return Optional.of(new SeqGap(expected, seq));
    }
    return Optional.empty();
  }

  /** Resets the tracker — use when a session restarts. */
  public void reset() {
    lastSeq = UNSET;
  }
}
