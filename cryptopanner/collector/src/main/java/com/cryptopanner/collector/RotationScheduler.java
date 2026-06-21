package com.cryptopanner.collector;

import com.cryptopanner.common.config.ConfigParse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

/**
 * Decides when the Collector should rotate its WS connection ahead of Binance's 24h forced-close
 * (design doc §5.1). Pure decision logic — the actual shadow-open/equivalence/cutover lifecycle
 * (§5.2) is driven by the WsConnectionManager. A per-minute task evaluates {@link #decide}.
 *
 * <p>Normal path: rotate only when the connection is older than {@code connection_max_age}, the
 * current minute is in the {@code rotation_window}, and it is this node's deterministic per-node
 * minute (so a fleet doesn't reconnect in lockstep). Emergency path: once the connection is within
 * 15 min of the 24h cliff ({@code max_age + 45min}), bypass both the window and the per-node minute
 * — an out-of-window rotation beats a hard disconnect.
 */
public final class RotationScheduler {

  /** Minutes past {@code max_age} at which rotation forces through window/minute constraints. */
  private static final Duration EMERGENCY_AFTER = Duration.ofMinutes(45);

  private RotationScheduler() {}

  public enum Decision {
    /** Defer — conditions not met. */
    NONE,
    /** Rotate on the normal scheduled path. */
    SCHEDULED,
    /** Rotate now, bypassing window + per-node minute (approaching the 24h cliff). */
    EMERGENCY
  }

  /**
   * This node's deterministic rotation minute: {@code (uint32(sha256(node_id)[0..4]) % 40) + 10},
   * landing in {@code [10, 50)}. SHA-256 (not {@code String.hashCode}) keeps it
   * platform-independent and uniformly distributed so co-located nodes rarely collide (design doc
   * §5.1).
   */
  public static int perNodeMinute(String nodeId) {
    byte[] d = sha256(nodeId.getBytes(StandardCharsets.UTF_8));
    long u32 =
        ((d[0] & 0xffL) << 24) | ((d[1] & 0xffL) << 16) | ((d[2] & 0xffL) << 8) | (d[3] & 0xffL);
    return (int) (u32 % 40) + 10;
  }

  /**
   * Evaluates the rotation decision for the current minute given the live connection age.
   *
   * @param nodeId this node's id (drives the per-node minute)
   * @param connectionAge age of the current primary WS connection
   * @param minuteOfHour current UTC minute of the hour (0..59)
   * @param connectionMaxAge configured {@code connection_max_age} (default 23h)
   * @param rotationWindow configured {@code rotation_window} (default HH:10-HH:50)
   */
  public static Decision decide(
      String nodeId,
      Duration connectionAge,
      int minuteOfHour,
      Duration connectionMaxAge,
      ConfigParse.HourWindow rotationWindow) {
    if (connectionAge.compareTo(connectionMaxAge.plus(EMERGENCY_AFTER)) > 0) {
      return Decision.EMERGENCY;
    }
    boolean ageReady = connectionAge.compareTo(connectionMaxAge) > 0;
    boolean windowOk = rotationWindow.contains(minuteOfHour);
    boolean slotUnique = minuteOfHour == perNodeMinute(nodeId);
    return (ageReady && windowOk && slotUnique) ? Decision.SCHEDULED : Decision.NONE;
  }

  private static byte[] sha256(byte[] in) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(in);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
