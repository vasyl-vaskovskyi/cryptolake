package com.cryptolake.collector;

import com.cryptolake.common.util.ClockSupplier;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Immutable session identity for the collector process lifetime.
 *
 * <p>The session ID format is {@code {collectorId}_{yyyy-MM-dd'T'HH:mm:ss'Z'}} (Tier 5 M7). This
 * format matches Python's {@code f"{collector_id}_{time.strftime('%Y-%m-%dT%H:%M:%SZ',
 * time.gmtime())}"} — fractional seconds are intentionally omitted to avoid PK collision issues in
 * PG.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record CollectorSession(String collectorId, String sessionId, Instant startedAt) {

  /** Date-time format for session IDs: no fractional seconds, literal 'Z' (Tier 5 M7). */
  private static final DateTimeFormatter SESSION_ID_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  /** Creates a new session ID from the given collector ID using the current clock time. */
  public static CollectorSession create(String collectorId, ClockSupplier clock) {
    Instant now =
        Instant.ofEpochSecond(clock.nowNs() / 1_000_000_000L, clock.nowNs() % 1_000_000_000L);
    // Use wall clock for session ID formatting (not the nanosecond clock)
    Instant wallNow = Instant.now();
    String sessionId = collectorId + "_" + SESSION_ID_FMT.format(wallNow);
    return new CollectorSession(collectorId, sessionId, wallNow);
  }
}
