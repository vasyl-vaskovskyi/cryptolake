package com.cryptolake.common.identity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Boot-ID resolution and session-ID formatting.
 *
 * <p>Ports Python's {@code get_host_boot_id} and the session-ID format from {@code
 * src/collector/main.py:32}.
 *
 * <p>Boot ID resolution order (design §2.6):
 *
 * <ol>
 *   <li>{@code CRYPTOLAKE_TEST_BOOT_ID} env var — deterministic override for tests.
 *   <li>{@code /proc/sys/kernel/random/boot_id} — Linux.
 *   <li>{@code "unknown"} sentinel on any exception.
 * </ol>
 *
 * <p>Session ID format: {@code collectorId + "_" + yyyy-MM-dd'T'HH:mm:ss'Z'} — no fractional
 * seconds (Tier 5 M7). Never uses {@link Instant#toString()} which includes fractional seconds.
 *
 * <p>Package-private seam: {@code bootIdPath} is overridable in tests (design §11 Q4).
 *
 * <p>Stateless; formatters are immutable. Thread-safe.
 */
public final class SystemIdentity {

  private static final String ENV_OVERRIDE = "CRYPTOLAKE_TEST_BOOT_ID";

  /** Package-private seam for tests — allows overriding the boot ID file path. */
  static Path bootIdPath = Path.of("/proc/sys/kernel/random/boot_id");

  private static final DateTimeFormatter SESSION_ID_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  private SystemIdentity() {}

  /**
   * Returns the current host boot ID.
   *
   * <p>Resolution order: env override → {@code /proc} → {@code "unknown"}.
   */
  public static String getHostBootId() {
    // 1. Env override for testing
    String envVal = System.getenv(ENV_OVERRIDE);
    if (envVal != null) {
      return envVal.strip();
    }

    // 2. Linux /proc boot_id
    try {
      return Files.readString(bootIdPath).strip();
    } catch (IOException e) {
      // NoSuchFileException, AccessDeniedException, or other I/O error — fall through
    }

    // 3. Fallback sentinel
    return "unknown";
  }

  /**
   * Builds a session ID in the format {@code collectorId_yyyy-MM-dd'T'HH:mm:ss'Z'} (Tier 5 M7).
   *
   * @param collectorId e.g. {@code "binance-collector-01"}
   * @param now the current time
   */
  public static String buildSessionId(String collectorId, Instant now) {
    return collectorId + "_" + SESSION_ID_FMT.format(now);
  }

  /** Builds a session ID using {@link Instant#now()}. */
  public static String buildSessionId(String collectorId) {
    return buildSessionId(collectorId, Instant.now());
  }
}
