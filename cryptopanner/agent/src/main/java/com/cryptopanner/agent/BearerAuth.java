package com.cryptopanner.agent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Authorizes mutating Node Agent endpoints ({@code POST /restart/*}, {@code /rotation/trigger}) per
 * master spec §4.f: an opaque bearer token plus an ISO-8601 request timestamp that must be within a
 * ±30s window (replay protection). Read-only endpoints ({@code /status}, {@code /metrics}) are not
 * gated by this. The token is compared in constant time; it is not a JWT — no claims, no expiry.
 */
public final class BearerAuth {

  private static final Duration MAX_SKEW = Duration.ofSeconds(30);

  private final byte[] tokenBytes;

  public BearerAuth(String token) {
    this.tokenBytes = token.getBytes(StandardCharsets.UTF_8);
  }

  public enum Outcome {
    OK,
    MISSING_TOKEN,
    BAD_TOKEN,
    BAD_TIMESTAMP,
    STALE_TIMESTAMP
  }

  /**
   * @param authHeader value of the {@code Authorization} header (expected {@code "Bearer <token>"})
   * @param timestampHeader the request's ISO-8601 timestamp header
   * @param now current time
   */
  public Outcome check(String authHeader, String timestampHeader, Instant now) {
    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
      return Outcome.MISSING_TOKEN;
    }
    byte[] presented = authHeader.substring("Bearer ".length()).getBytes(StandardCharsets.UTF_8);
    if (!MessageDigest.isEqual(presented, tokenBytes)) {
      return Outcome.BAD_TOKEN;
    }
    if (timestampHeader == null) {
      return Outcome.BAD_TIMESTAMP;
    }
    Instant ts;
    try {
      ts = Instant.parse(timestampHeader);
    } catch (DateTimeParseException e) {
      return Outcome.BAD_TIMESTAMP;
    }
    if (Duration.between(ts, now).abs().compareTo(MAX_SKEW) > 0) {
      return Outcome.STALE_TIMESTAMP;
    }
    return Outcome.OK;
  }
}
