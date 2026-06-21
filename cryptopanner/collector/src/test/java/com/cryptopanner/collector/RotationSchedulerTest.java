package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.config.ConfigParse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RotationSchedulerTest {

  private static final Duration MAX_AGE = Duration.ofHours(23);
  private static final ConfigParse.HourWindow WINDOW = ConfigParse.hourWindow("HH:10-HH:50");

  @Test
  void perNodeMinuteIsDeterministicAndInsideWindow() {
    int m1 = RotationScheduler.perNodeMinute("vps-fra-1");
    int m2 = RotationScheduler.perNodeMinute("vps-fra-1");
    assertEquals(m1, m2, "deterministic");
    assertTrue(m1 >= 10 && m1 < 50, "within [10,50): " + m1);
  }

  @Test
  void perNodeMinuteMatchesSha256First4BytesMod40Plus10() throws Exception {
    byte[] d =
        MessageDigest.getInstance("SHA-256").digest("vps-tyo-1".getBytes(StandardCharsets.UTF_8));
    long u32 =
        ((d[0] & 0xffL) << 24) | ((d[1] & 0xffL) << 16) | ((d[2] & 0xffL) << 8) | (d[3] & 0xffL);
    int expected = (int) (u32 % 40) + 10;
    assertEquals(expected, RotationScheduler.perNodeMinute("vps-tyo-1"));
  }

  @Test
  void youngConnectionDoesNotRotate() {
    RotationScheduler.Decision d =
        RotationScheduler.decide("n", Duration.ofHours(1), 30, MAX_AGE, WINDOW);
    assertEquals(RotationScheduler.Decision.NONE, d);
  }

  @Test
  void agedConnectionAtNodeMinuteInWindowRotatesScheduled() {
    int minute = RotationScheduler.perNodeMinute("n");
    RotationScheduler.Decision d =
        RotationScheduler.decide("n", Duration.ofHours(23).plusMinutes(1), minute, MAX_AGE, WINDOW);
    assertEquals(RotationScheduler.Decision.SCHEDULED, d);
  }

  @Test
  void agedConnectionAtWrongMinuteDefers() {
    int minute = RotationScheduler.perNodeMinute("n");
    int otherMinute = minute == 10 ? 11 : 10; // a different in-window minute
    RotationScheduler.Decision d =
        RotationScheduler.decide(
            "n", Duration.ofHours(23).plusMinutes(1), otherMinute, MAX_AGE, WINDOW);
    assertEquals(RotationScheduler.Decision.NONE, d);
  }

  @Test
  void agedConnectionOutsideWindowDefers() {
    int minute = RotationScheduler.perNodeMinute("n");
    // force a clearly out-of-window minute regardless of the node's deterministic minute
    RotationScheduler.Decision d =
        RotationScheduler.decide("n", Duration.ofHours(23).plusMinutes(1), 5, MAX_AGE, WINDOW);
    assertEquals(RotationScheduler.Decision.NONE, d);
    assertTrue(minute >= 10); // sanity: node minute is in-window so 5 is genuinely outside
  }

  @Test
  void pastEmergencyThresholdBypassesWindowAndMinute() {
    // age > max_age + 45min → emergency: rotate even outside window and off the node minute.
    RotationScheduler.Decision d =
        RotationScheduler.decide("n", Duration.ofHours(24), 5, MAX_AGE, WINDOW);
    assertEquals(RotationScheduler.Decision.EMERGENCY, d);
  }
}
