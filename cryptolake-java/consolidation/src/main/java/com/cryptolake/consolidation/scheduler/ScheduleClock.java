package com.cryptolake.consolidation.scheduler;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Deterministic next-run instant computation.
 *
 * <p>Ports {@code _compute_sleep_until_next_run} from {@code consolidation_scheduler.py}. Always
 * UTC (Tier 5 M11, F3).
 *
 * <p>Thread safety: stateless utility.
 */
public final class ScheduleClock {

  private ScheduleClock() {}

  /**
   * Returns the next {@code startHourUtc:30} UTC instant that is strictly after {@code now}.
   *
   * <p>If today's run time has not yet passed, returns it for today; otherwise rolls to tomorrow.
   *
   * @param now current instant
   * @param startHourUtc UTC hour (0–23) at which the scheduler fires
   * @return next run instant
   */
  public static Instant nextRunInstant(Instant now, int startHourUtc) {
    LocalDate today = now.atOffset(ZoneOffset.UTC).toLocalDate();

    // Try today
    Instant candidate =
        LocalDateTime.of(today, java.time.LocalTime.of(startHourUtc, 30)).toInstant(ZoneOffset.UTC);

    if (candidate.isAfter(now)) {
      return candidate;
    }

    // Roll to next day
    return LocalDateTime.of(today.plusDays(1), java.time.LocalTime.of(startHourUtc, 30))
        .toInstant(ZoneOffset.UTC);
  }
}
