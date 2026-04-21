package com.cryptolake.common.util;

import java.time.Instant;

/**
 * Factory for {@link ClockSupplier} implementations.
 *
 * <p>Tier 5 E2: {@code systemNanoClock} uses {@code Math.addExact(Math.multiplyExact(...))} to
 * preserve nanosecond resolution without floating-point rounding.
 */
public final class Clocks {

  private Clocks() {}

  /**
   * Returns a {@link ClockSupplier} that reads {@link Instant#now()} and converts to nanoseconds.
   */
  public static ClockSupplier systemNanoClock() {
    return () -> {
      Instant n = Instant.now();
      return Math.addExact(Math.multiplyExact(n.getEpochSecond(), 1_000_000_000L), n.getNano());
    };
  }

  /** Returns a fixed-value clock for testing. */
  public static ClockSupplier fixed(long ns) {
    return () -> ns;
  }
}
