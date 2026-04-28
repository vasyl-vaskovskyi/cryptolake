package com.cryptolake.verify.integrity;

import java.util.List;

/**
 * Result of an integrity check for a single stream.
 *
 * <p>Ports the return value of {@code _check_trades}, {@code _check_depth}, {@code
 * _check_bookticker} from {@code integrity.py}.
 *
 * <p>Tier 2 §12 — records (immutable, no setters).
 */
public record IntegrityCheckResult(int recordCount, List<Break> breaks) {

  /**
   * Represents a single continuity break in the stream.
   *
   * <p>Fields match Python's {@code integrity.py} break representation.
   */
  public record Break(String field, long expected, long actual, long missing, long atReceived) {}
}
