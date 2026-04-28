package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.consolidation.scheduler.ScheduleClock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

/** ports: tests/unit/cli/test_consolidation_scheduler.py */
class ScheduleClockTest {

  @Test
  void nextRunInstantBeforeStartHour() {
    // ports: test_consolidation_scheduler.py::test_next_run_before_start_hour
    // It's 01:00 UTC, start hour = 2 → next run is same day at 02:30
    Instant now = ZonedDateTime.of(2026, 4, 23, 1, 0, 0, 0, ZoneOffset.UTC).toInstant();
    Instant next = ScheduleClock.nextRunInstant(now, 2);
    assertThat(next)
        .isEqualTo(ZonedDateTime.of(2026, 4, 23, 2, 30, 0, 0, ZoneOffset.UTC).toInstant());
  }

  @Test
  void nextRunInstantAtOrAfterStartHourRollsToNextDay() {
    // ports: test_consolidation_scheduler.py::test_next_run_rolls_to_next_day
    // It's 03:00 UTC, start hour = 2 → next run is next day at 02:30
    Instant now = ZonedDateTime.of(2026, 4, 23, 3, 0, 0, 0, ZoneOffset.UTC).toInstant();
    Instant next = ScheduleClock.nextRunInstant(now, 2);
    assertThat(next)
        .isEqualTo(ZonedDateTime.of(2026, 4, 24, 2, 30, 0, 0, ZoneOffset.UTC).toInstant());
  }

  @Test
  void dstTransitionsHandledViaUtc() {
    // new — Tier 5 M11: always UTC, no DST surprises
    // March 29, 2026 is a DST transition date in Europe, but we're UTC-only
    Instant now = ZonedDateTime.of(2026, 3, 29, 1, 0, 0, 0, ZoneOffset.UTC).toInstant();
    Instant next = ScheduleClock.nextRunInstant(now, 2);
    // Should still be 02:30 UTC exactly
    assertThat(next)
        .isEqualTo(ZonedDateTime.of(2026, 3, 29, 2, 30, 0, 0, ZoneOffset.UTC).toInstant());
  }
}
