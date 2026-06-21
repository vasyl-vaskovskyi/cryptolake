package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ConfigParseTest {

  @Test
  void parsesDurationSuffixes() {
    assertEquals(Duration.ofSeconds(10), ConfigParse.duration("10s"));
    assertEquals(Duration.ofSeconds(120), ConfigParse.duration("120s"));
    assertEquals(Duration.ofMinutes(5), ConfigParse.duration("5m"));
    assertEquals(Duration.ofHours(23), ConfigParse.duration("23h"));
    assertEquals(Duration.ofDays(7), ConfigParse.duration("7d"));
  }

  @Test
  void durationToleratesSurroundingWhitespace() {
    assertEquals(Duration.ofSeconds(30), ConfigParse.duration("  30s "));
  }

  @Test
  void rejectsMalformedDuration() {
    assertThrows(IllegalArgumentException.class, () -> ConfigParse.duration("10x"));
    assertThrows(IllegalArgumentException.class, () -> ConfigParse.duration("abc"));
    assertThrows(IllegalArgumentException.class, () -> ConfigParse.duration(""));
  }

  @Test
  void parsesHourWindowWithinSameHour() {
    ConfigParse.HourWindow w = ConfigParse.hourWindow("HH:10-HH:50");
    assertEquals(10, w.startMinute());
    assertEquals(50, w.endMinute());
    assertFalse(w.contains(5));
    assertTrue(w.contains(10), "start is inclusive");
    assertTrue(w.contains(30));
    assertFalse(w.contains(50), "end is exclusive");
    assertFalse(w.contains(55));
  }

  @Test
  void parsesWrappingHourWindow() {
    // HH:50-HH:15 means minute-of-hour 50 through 15 of the next hour: it wraps
    // midnight-of-the-hour.
    ConfigParse.HourWindow w = ConfigParse.hourWindow("HH:50-HH:15");
    assertTrue(w.contains(55));
    assertTrue(w.contains(0));
    assertTrue(w.contains(10));
    assertFalse(w.contains(15), "end is exclusive");
    assertFalse(w.contains(30));
  }

  @Test
  void detectsOverlappingWindows() {
    // Abutting windows share no minute (end-exclusive meets start-inclusive at 30).
    ConfigParse.HourWindow early = ConfigParse.hourWindow("HH:10-HH:30");
    ConfigParse.HourWindow late = ConfigParse.hourWindow("HH:30-HH:50");
    assertFalse(early.overlaps(late), "abutting windows do not overlap");

    // §15.a contradiction check: a wrapping window overlapping another at the wrapped minutes.
    ConfigParse.HourWindow forbidden = ConfigParse.hourWindow("HH:50-HH:15"); // {50..59, 0..14}
    ConfigParse.HourWindow bad = ConfigParse.hourWindow("HH:05-HH:20"); // {5..19}
    assertTrue(
        forbidden.overlaps(bad), "HH:05-HH:20 overlaps the wrapping HH:50-HH:15 at minutes 5..14");
  }
}
