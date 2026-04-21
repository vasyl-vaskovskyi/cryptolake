package com.cryptolake.common.util;

/**
 * Testable nanosecond clock abstraction.
 *
 * <p>Default production impl: {@link Clocks#systemNanoClock()}. Tests inject {@link
 * Clocks#fixed(long)} for deterministic timestamps.
 *
 * <p>Tier 5 E2: implementation uses {@code Instant.getEpochSecond() * 1_000_000_000L +
 * getNano()} to preserve full nanosecond resolution.
 */
@FunctionalInterface
public interface ClockSupplier {
  /** Returns nanoseconds since the Unix epoch (Tier 5 E2). */
  long nowNs();
}
