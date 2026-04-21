package com.cryptolake.writer.buffer;

/**
 * Routing key for archive files: {@code (exchange, symbol, stream, date, hour)}.
 *
 * <p>Ports Python's {@code FileTarget} dataclass (design §6.2; Tier 5 M14). Record
 * auto-implements {@code equals/hashCode} on components, matching Python tuple identity.
 *
 * <p>{@code date} stays a {@code String} — do NOT "improve" to {@code LocalDate} (Tier 5 M14
 * watch-out). {@code hour} is an {@code int} in range {@code [0, 23]}.
 *
 * <p>Not serialized to JSON — internal routing key only.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record FileTarget(String exchange, String symbol, String stream, String date, int hour) {

  /** Compact constructor validates non-null fields and hour range. */
  public FileTarget {
    if (exchange == null || symbol == null || stream == null || date == null) {
      throw new IllegalArgumentException("FileTarget fields must not be null");
    }
    if (hour < 0 || hour > 23) {
      throw new IllegalArgumentException("hour out of range: " + hour);
    }
  }
}
