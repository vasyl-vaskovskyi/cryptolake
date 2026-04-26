package com.cryptolake.collector.connection;

import java.time.Duration;
import java.time.Instant;

/**
 * Exponential backoff reconnection policy with a 24h proactive reconnect limit.
 *
 * <p>Ports the reconnect backoff logic from {@code src/collector/connection.py} (Tier 5 D7). Pure
 * data — no sleeps inside this class; callers sleep using the value returned by {@link
 * #nextBackoffMillis()}.
 *
 * <p>Thread safety: caller-confined per-client instance.
 */
public final class ReconnectPolicy {

  /** Initial backoff: 1 second. */
  private static final long INITIAL_BACKOFF_MS = 1_000L;

  /** Maximum backoff: 60 seconds (Tier 5 D7; Python _MAX_BACKOFF = 60). */
  public static final long MAX_BACKOFF_SECONDS = 60L;

  private static final long MAX_BACKOFF_MS = MAX_BACKOFF_SECONDS * 1_000L;

  /** Proactive reconnect window: 23h50m (Tier 5 D6; Python {@code _RECONNECT_BEFORE_24H}). */
  public static final Duration PROACTIVE_RECONNECT_DURATION = Duration.ofHours(23).plusMinutes(50);

  private long currentBackoffMs = INITIAL_BACKOFF_MS;

  /**
   * Returns the current backoff duration in milliseconds, then doubles it up to {@link
   * #MAX_BACKOFF_MS} for the next call.
   */
  public long nextBackoffMillis() {
    long result = currentBackoffMs;
    currentBackoffMs = Math.min(currentBackoffMs * 2, MAX_BACKOFF_MS);
    return result;
  }

  /** Resets the backoff to the initial value (call when a connection succeeds). */
  public void reset() {
    currentBackoffMs = INITIAL_BACKOFF_MS;
  }

  /**
   * Returns {@code true} when the connection has been up long enough that a proactive reconnect is
   * warranted (Tier 5 D6).
   *
   * @param connectTime when the connection was established
   * @param now current wall-clock instant
   */
  public boolean shouldProactivelyReconnect(Instant connectTime, Instant now) {
    return Duration.between(connectTime, now).compareTo(PROACTIVE_RECONNECT_DURATION) > 0;
  }
}
