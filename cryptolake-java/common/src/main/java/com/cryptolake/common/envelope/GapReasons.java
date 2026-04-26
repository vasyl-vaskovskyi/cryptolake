package com.cryptolake.common.envelope;

import java.util.Set;

/**
 * Vocabulary guard for gap {@code reason} strings.
 *
 * <p>Intentionally a {@code Set<String>}, NOT an enum. Downstream readers (Python {@code verify},
 * {@code consolidate}) must tolerate unknown reasons without crashing, and the set is additive-only
 * for archival compatibility. An enum with {@code @JsonValue} would fail on unknown values during
 * deserialization (Tier 5 M6).
 */
public final class GapReasons {

  /** Immutable vocabulary of valid gap reason strings (Tier 5 M6). */
  public static final Set<String> VALID =
      Set.of(
          "ws_disconnect",
          "pu_chain_break",
          "session_seq_skip",
          "buffer_overflow",
          "snapshot_poll_miss",
          "collector_restart", // kept for migration: existing archives contain it
          "restart_gap",
          "recovery_depth_anchor",
          "write_error",
          "deserialization_error",
          "checkpoint_lost",
          "missing_hour",
          // Added in collector silent-loss work (commits 30348b2, 3e068b7):
          "kafka_delivery_failed",
          "handler_error");

  private GapReasons() {}

  /**
   * Throws {@link IllegalArgumentException} if {@code reason} is not in {@link #VALID}.
   *
   * <p>Tier 2 §16: fail-fast on invariant violation.
   */
  public static void requireValid(String reason) {
    if (!VALID.contains(reason)) {
      throw new IllegalArgumentException("Invalid gap reason: '" + reason + "'");
    }
  }
}
