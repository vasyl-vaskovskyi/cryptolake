package com.cryptolake.collector.gap;

/**
 * Result of validating a depth diff against the pu-chain.
 *
 * <p>Ports {@code DiffValidationResult} from {@code src/collector/gap_detector.py}.
 *
 * <p>Immutable record (Tier 2 §12). Field names use full words to avoid conflict with Java record
 * accessor methods (a record named {@code stale} would clash with a static factory named {@code
 * stale()}).
 */
public record DiffValidationResult(boolean valid, boolean isGap, boolean isStale, String reason) {

  /** Convenience factory: valid diff (no gap, not stale). */
  public static DiffValidationResult ok() {
    return new DiffValidationResult(true, false, false, "");
  }

  /** Convenience factory: invalid diff due to pu-chain break. */
  public static DiffValidationResult puBreak(String reason) {
    return new DiffValidationResult(false, true, false, reason);
  }

  /** Convenience factory: stale diff (before sync point). */
  public static DiffValidationResult staleDiff() {
    return new DiffValidationResult(false, false, true, "stale");
  }

  /** Convenience factory: invalid for a generic reason (not a gap, not stale). */
  public static DiffValidationResult invalid(String reason) {
    return new DiffValidationResult(false, false, false, reason);
  }
}
