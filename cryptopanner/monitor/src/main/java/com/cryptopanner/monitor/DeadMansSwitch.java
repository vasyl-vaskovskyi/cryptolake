package com.cryptopanner.monitor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * The §13.c dead-man's switch. On each {@link #tick}:
 *
 * <ul>
 *   <li>pushes a heartbeat (GET) to Healthchecks.io every {@code push_interval} — if the pushes
 *       stop for 5 min, Healthchecks alerts the operator out-of-band, covering "the Monitor itself
 *       is down".
 *   <li>fires a daily self-test "alert path healthy" message on every channel at the configured UTC
 *       time, so a silently-broken alert channel (revoked token, changed webhook) is noticed.
 * </ul>
 *
 * <p>Driven by the injected scrape {@code now} so cadence is deterministic in tests. A blank
 * Healthchecks URL disables the push; push failures never throw.
 */
public final class DeadMansSwitch {

  private final HttpClient http;
  private final String healthchecksUrl;
  private final Duration pushInterval;
  private final LocalTime selfTestTimeUtc;
  private final List<AlertChannel> channels;

  private Instant lastPush;
  private LocalDate lastSelfTestDate;

  public DeadMansSwitch(
      HttpClient http,
      String healthchecksUrl,
      Duration pushInterval,
      LocalTime selfTestTimeUtc,
      List<AlertChannel> channels) {
    this.http = http;
    this.healthchecksUrl = healthchecksUrl;
    this.pushInterval = pushInterval;
    this.selfTestTimeUtc = selfTestTimeUtc;
    this.channels = channels;
  }

  public void tick(Instant now) {
    maybePush(now);
    maybeSelfTest(now);
  }

  private void maybePush(Instant now) {
    if (healthchecksUrl == null || healthchecksUrl.isBlank()) {
      return;
    }
    if (lastPush != null && Duration.between(lastPush, now).compareTo(pushInterval) < 0) {
      return;
    }
    lastPush = now;
    try {
      HttpRequest req =
          HttpRequest.newBuilder(URI.create(healthchecksUrl))
              .timeout(Duration.ofSeconds(10))
              .GET()
              .build();
      http.send(req, HttpResponse.BodyHandlers.discarding());
    } catch (Exception e) {
      // A failed ping is itself the dead-man signal Healthchecks watches for; never propagate.
    }
  }

  private void maybeSelfTest(Instant now) {
    LocalDate today = now.atZone(ZoneOffset.UTC).toLocalDate();
    Instant fireAt = today.atTime(selfTestTimeUtc).toInstant(ZoneOffset.UTC);
    if (!now.isBefore(fireAt) && !today.equals(lastSelfTestDate)) {
      lastSelfTestDate = today;
      DispatchedMessage msg =
          DispatchedMessage.selfTest(
              "daily self-test at " + selfTestTimeUtc + " UTC — channels live");
      for (AlertChannel ch : channels) {
        if (ch.enabled()) {
          ch.send(msg);
        }
      }
    }
  }
}
