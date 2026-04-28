package com.cryptolake.consolidation.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes hourly source files after successful daily consolidation.
 *
 * <p>Ports {@code cleanup_hourly_files} from {@code consolidate.py}. ONLY called after {@link
 * DailyFileVerifier#verify} passes — Tier 1 §4 indirect discipline (delete-after-verify).
 *
 * <p>Individual file deletion failures are swallowed (Tier 5 G1 — best-effort cleanup).
 *
 * <p>Thread safety: stateless utility.
 */
public final class HourlyCleanup {

  private static final Logger log = LoggerFactory.getLogger(HourlyCleanup.class);

  private HourlyCleanup() {}

  /**
   * Deletes each file and its sidecar (if present) from the filesystem.
   *
   * @param originalFiles list of hourly archive paths to delete
   * @param sidecars list of corresponding sidecar paths to delete
   */
  public static void delete(List<Path> originalFiles, List<Path> sidecars) {
    for (Path f : originalFiles) {
      try {
        Files.deleteIfExists(f);
      } catch (IOException ignored) {
        // best-effort shutdown; never block main shutdown path (Tier 5 G1)
        log.warn("cleanup_delete_failed", "path", f.toString());
      }
    }
    for (Path s : sidecars) {
      try {
        Files.deleteIfExists(s);
      } catch (IOException ignored) {
        // best-effort shutdown; never block main shutdown path (Tier 5 G1)
        log.warn("cleanup_sidecar_delete_failed", "path", s.toString());
      }
    }
  }
}
