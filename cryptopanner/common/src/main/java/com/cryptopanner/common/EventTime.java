package com.cryptopanner.common;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Optional;

/**
 * Server-event-time extraction for minute bucketing per master spec §8.c. A frame's minute file is
 * chosen by the exchange timestamp inside the frame — {@code T} (trade time) for {@code trade} and
 * {@code aggTrade}, {@code E} (event time) for every other stream (including the {@code forceOrder}
 * broadcast).
 *
 * <p>Returns {@link Optional#empty()} when the expected field is absent or non-numeric so the
 * caller can fall back to receive time (and count the frame as unparseable) rather than
 * mis-bucketing.
 */
public final class EventTime {

  private EventTime() {}

  /**
   * Returns the bucket instant for a frame's {@code data} node given its stream type, or empty if
   * the timestamp field is missing/non-numeric.
   */
  public static Optional<Instant> bucketInstant(String streamType, JsonNode data) {
    JsonNode ts = data.path(bucketField(streamType));
    if (!ts.isIntegralNumber()) {
      return Optional.empty();
    }
    return Optional.of(Instant.ofEpochMilli(ts.asLong()));
  }

  /**
   * {@code T} for trade/aggTrade, {@code E} otherwise. Parameter suffixes (e.g. {@code @100ms}) are
   * stripped.
   */
  static String bucketField(String streamType) {
    String base = streamType;
    int at = streamType.indexOf('@');
    if (at >= 0) {
      base = streamType.substring(0, at);
    }
    return switch (base) {
      case "trade", "aggTrade" -> "T";
      default -> "E";
    };
  }
}
