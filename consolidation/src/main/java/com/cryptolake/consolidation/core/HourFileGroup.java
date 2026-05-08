package com.cryptolake.consolidation.core;

import java.nio.file.Path;
import java.util.List;

/**
 * Groups the archive files for a single hour: one base file, zero or more late-arrival files, and
 * zero or more backfill files.
 *
 * <p>Ports the return shape of {@code discover_hour_files} from {@code consolidate.py}.
 *
 * <p>Tier 2 §12 — record (immutable, no setters).
 */
public record HourFileGroup(
    Path base, // nullable — may be absent
    List<Path> late, // sorted by seq ascending (Tier 5 M15)
    List<Path> backfill // sorted by seq ascending
    ) {

  /** Returns true if no files are present for this hour (missing hour). */
  public boolean isEmpty() {
    return base == null && late.isEmpty() && backfill.isEmpty();
  }

  /**
   * Manifest hour-status classification (spec §Manifest):
   *
   * <ul>
   *   <li>{@code "missing"} — no files at all (a synthetic gap envelope is emitted).
   *   <li>{@code "backfilled"} — no base file, but backfill files exist.
   *   <li>{@code "present"} — base file exists (may include late/backfill supplements).
   * </ul>
   */
  public String status() {
    if (isEmpty()) return "missing";
    if (base == null && !backfill.isEmpty()) return "backfilled";
    return "present";
  }
}
