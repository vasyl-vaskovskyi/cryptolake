package com.cryptopanner.common.config;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsers for the human-friendly scalar formats used throughout the §15 config: duration strings
 * ({@code 10s}, {@code 5m}, {@code 23h}, {@code 7d}) and recurring per-hour windows ({@code
 * HH:10-HH:50}). Kept tiny and side-effect-free so config loading can fail fast (§15.a) with a
 * clear message about the offending value.
 */
public final class ConfigParse {

  private ConfigParse() {}

  private static final Pattern DURATION = Pattern.compile("\\s*(\\d+)\\s*([smhd])\\s*");
  private static final Pattern WINDOW =
      Pattern.compile("\\s*HH:(\\d{1,2})\\s*-\\s*HH:(\\d{1,2})\\s*");

  /** Parses a duration like {@code 10s} / {@code 5m} / {@code 23h} / {@code 7d}. */
  public static Duration duration(String s) {
    if (s == null) {
      throw new IllegalArgumentException("duration is null");
    }
    Matcher m = DURATION.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException(
          "malformed duration: '" + s + "' (expected e.g. 10s, 5m, 23h, 7d)");
    }
    long n = Long.parseLong(m.group(1));
    return switch (m.group(2)) {
      case "s" -> Duration.ofSeconds(n);
      case "m" -> Duration.ofMinutes(n);
      case "h" -> Duration.ofHours(n);
      case "d" -> Duration.ofDays(n);
      default -> throw new IllegalArgumentException("unreachable unit in: " + s);
    };
  }

  /** Parses a recurring per-hour window like {@code HH:10-HH:50}. */
  public static HourWindow hourWindow(String s) {
    if (s == null) {
      throw new IllegalArgumentException("window is null");
    }
    Matcher m = WINDOW.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException(
          "malformed window: '" + s + "' (expected e.g. HH:10-HH:50)");
    }
    int start = Integer.parseInt(m.group(1));
    int end = Integer.parseInt(m.group(2));
    if (start > 59 || end > 59) {
      throw new IllegalArgumentException("window minute out of range [0,59]: '" + s + "'");
    }
    return new HourWindow(start, end);
  }

  /**
   * A recurring window expressed as minutes-of-the-hour, start-inclusive and end-exclusive. When
   * {@code startMinute >= endMinute} the window wraps across the hour boundary (e.g. {@code
   * HH:50-HH:15} covers minutes 50..59 and 00..14).
   */
  public record HourWindow(int startMinute, int endMinute) {

    /** Whether the given minute-of-hour (0..59) falls inside this window. */
    public boolean contains(int minuteOfHour) {
      if (startMinute < endMinute) {
        return minuteOfHour >= startMinute && minuteOfHour < endMinute;
      }
      // Wrapping window: inside if at/after start OR before end.
      return minuteOfHour >= startMinute || minuteOfHour < endMinute;
    }

    /**
     * Whether this window shares any minute-of-hour with {@code other} (§15.a contradiction check).
     */
    public boolean overlaps(HourWindow other) {
      for (int min = 0; min < 60; min++) {
        if (contains(min) && other.contains(min)) {
          return true;
        }
      }
      return false;
    }
  }
}
