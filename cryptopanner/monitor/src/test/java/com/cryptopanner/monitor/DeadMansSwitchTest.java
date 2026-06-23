package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.monitor.DispatchedMessage.Kind;
import com.cryptopanner.monitor.testutil.StubHttpServer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §13.c — Healthchecks.io dead-man push + daily self-test alert. */
class DeadMansSwitchTest {

  private StubHttpServer hc;

  // Captures self-test messages.
  private static final class RecordingChannel implements AlertChannel {
    final List<DispatchedMessage> sent = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
      return "rec";
    }

    @Override
    public boolean enabled() {
      return true;
    }

    @Override
    public boolean send(DispatchedMessage m) {
      sent.add(m);
      return true;
    }
  }

  private RecordingChannel channel;

  @BeforeEach
  void setUp() throws Exception {
    hc = new StubHttpServer().route("/ping/uuid", 200, "OK");
    channel = new RecordingChannel();
  }

  @AfterEach
  void tearDown() {
    hc.close();
  }

  private DeadMansSwitch dms(String url, LocalTime selfTest) {
    return new DeadMansSwitch(
        NodeScraper.newHttpClient(), url, Duration.ofSeconds(60), selfTest, List.of(channel));
  }

  @Test
  void pushesOnFirstTickThenEveryInterval() {
    Instant t0 = Instant.parse("2026-06-23T10:00:00Z");
    DeadMansSwitch d = dms(hc.baseUrl() + "/ping/uuid", LocalTime.of(2, 0));
    d.tick(t0);
    assertEquals(1, hc.received().size());
    d.tick(t0.plusSeconds(30)); // under 60s
    assertEquals(1, hc.received().size());
    d.tick(t0.plusSeconds(61)); // over 60s
    assertEquals(2, hc.received().size());
  }

  @Test
  void selfTestFiresOncePerDayAtConfiguredTime() {
    DeadMansSwitch d = dms(hc.baseUrl() + "/ping/uuid", LocalTime.of(2, 0));
    d.tick(Instant.parse("2026-06-23T01:59:00Z")); // before 02:00 → no self-test
    assertTrue(channel.sent.isEmpty());
    d.tick(Instant.parse("2026-06-23T02:00:05Z")); // at/after 02:00 → self-test
    assertEquals(1, channel.sent.size());
    assertEquals(Kind.SELF_TEST, channel.sent.get(0).kind());
    d.tick(Instant.parse("2026-06-23T02:05:00Z")); // same day → not again
    assertEquals(1, channel.sent.size());
    d.tick(Instant.parse("2026-06-24T02:00:05Z")); // next day → again
    assertEquals(2, channel.sent.size());
  }

  @Test
  void blankHealthchecksUrlDisablesPush() {
    DeadMansSwitch d = dms("", LocalTime.of(2, 0));
    d.tick(Instant.parse("2026-06-23T10:00:00Z"));
    assertTrue(hc.received().isEmpty());
  }

  @Test
  void unreachablePushDoesNotThrow() {
    DeadMansSwitch d = dms("http://127.0.0.1:1/ping", LocalTime.of(2, 0));
    d.tick(Instant.parse("2026-06-23T10:00:00Z")); // must not throw
  }
}
