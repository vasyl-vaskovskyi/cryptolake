package com.cryptolake.verify.audit;

import java.time.*;
import java.time.temporal.WeekFields;

public record PeriodSelector(long startMs, long endMs) {

  public static PeriodSelector parse(
      String periodKind, String periodValue, String since, String until) {
    if (since != null && until != null) {
      // endMs is inclusive; --since/--until callers pass the exact boundary millis.
      return new PeriodSelector(
          Instant.parse(since).toEpochMilli(), Instant.parse(until).toEpochMilli());
    }
    if ("--day".equals(periodKind)) {
      LocalDate d = LocalDate.parse(periodValue);
      long start = d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      return new PeriodSelector(start, start + 86_400_000L - 1);
    }
    if ("--week".equals(periodKind)) {
      String[] p = periodValue.split("-W");
      int year = Integer.parseInt(p[0]);
      int week = Integer.parseInt(p[1]);
      LocalDate monday =
          LocalDate.of(year, 1, 4)
              .with(WeekFields.ISO.weekOfYear(), week)
              .with(WeekFields.ISO.dayOfWeek(), 1);
      long start = monday.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      return new PeriodSelector(start, start + 7L * 86_400_000L - 1);
    }
    if ("--hour".equals(periodKind)) {
      LocalDateTime dt = LocalDateTime.parse(periodValue + ":00:00");
      long start = dt.toInstant(ZoneOffset.UTC).toEpochMilli();
      return new PeriodSelector(start, start + 3_600_000L - 1);
    }
    throw new IllegalArgumentException(
        "Specify exactly one of --day / --week / --hour, or --since with --until");
  }
}
