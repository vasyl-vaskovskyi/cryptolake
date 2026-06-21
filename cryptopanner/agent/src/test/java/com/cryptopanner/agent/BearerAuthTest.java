package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class BearerAuthTest {

  private static final String TOKEN = "s3cr3t-opaque-token";
  private final BearerAuth auth = new BearerAuth(TOKEN);
  private static final Instant NOW = Instant.parse("2026-06-21T12:00:00Z");

  @Test
  void validTokenAndFreshTimestampPasses() {
    assertEquals(
        BearerAuth.Outcome.OK,
        auth.check("Bearer " + TOKEN, "2026-06-21T11:59:50Z", NOW)); // 10s old
  }

  @Test
  void missingHeaderIsRejected() {
    assertEquals(BearerAuth.Outcome.MISSING_TOKEN, auth.check(null, "2026-06-21T12:00:00Z", NOW));
  }

  @Test
  void wrongTokenIsRejected() {
    assertEquals(
        BearerAuth.Outcome.BAD_TOKEN, auth.check("Bearer nope", "2026-06-21T12:00:00Z", NOW));
  }

  @Test
  void staleTimestampIsRejected() {
    // 40s old > 30s replay window.
    assertEquals(
        BearerAuth.Outcome.STALE_TIMESTAMP,
        auth.check("Bearer " + TOKEN, "2026-06-21T11:59:20Z", NOW));
  }

  @Test
  void farFutureTimestampIsRejected() {
    assertEquals(
        BearerAuth.Outcome.STALE_TIMESTAMP,
        auth.check("Bearer " + TOKEN, "2026-06-21T12:01:00Z", NOW));
  }

  @Test
  void unparseableTimestampIsRejected() {
    assertEquals(
        BearerAuth.Outcome.BAD_TIMESTAMP, auth.check("Bearer " + TOKEN, "not-a-time", NOW));
  }

  @Test
  void missingTimestampIsRejected() {
    assertEquals(BearerAuth.Outcome.BAD_TIMESTAMP, auth.check("Bearer " + TOKEN, null, NOW));
  }
}
