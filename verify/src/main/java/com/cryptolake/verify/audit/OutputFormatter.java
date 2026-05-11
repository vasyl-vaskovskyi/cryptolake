package com.cryptolake.verify.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

/**
 * Formats {@link GapRecord} lists for CLI output.
 *
 * <p>Two representations are supported:
 *
 * <ul>
 *   <li>{@link #toJson} — JSON array, pretty-printed for readability.
 *   <li>{@link #toHuman} — aligned text table with fixed-width columns.
 * </ul>
 *
 * <p>Thread safety: stateless; all methods are static.
 */
public final class OutputFormatter {

  // Column widths for the human-readable table
  private static final int W_SOURCE = 18;
  private static final int W_EXCHANGE = 10;
  private static final int W_SYMBOL = 12;
  private static final int W_STREAM = 14;
  private static final int W_START = 15;
  private static final int W_END = 15;
  private static final int W_REASON = 22;

  // detail is the last column — no fixed width; just append

  private OutputFormatter() {}

  /**
   * Serializes the records as a pretty-printed JSON array.
   *
   * @param records list of gap records (may be empty)
   * @param mapper shared Jackson mapper (from {@link
   *     com.cryptolake.common.envelope.EnvelopeCodec#newMapper()})
   * @return JSON string
   * @throws com.fasterxml.jackson.core.JsonProcessingException if serialization fails (unexpected)
   */
  public static String toJson(List<GapRecord> records, ObjectMapper mapper) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(records);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize GapRecord list to JSON", e);
    }
  }

  /**
   * Formats the records as an aligned text table.
   *
   * <p>Columns: {@code source}, {@code exchange}, {@code symbol}, {@code stream}, {@code startMs},
   * {@code endMs}, {@code reason}, {@code detail}.
   *
   * @param records list of gap records (may be empty)
   * @return multi-line string (includes header; ends with a newline)
   */
  public static String toHuman(List<GapRecord> records) {
    StringBuilder sb = new StringBuilder();
    // Header
    sb.append(
        String.format(
            "%-"
                + W_SOURCE
                + "s  %-"
                + W_EXCHANGE
                + "s  %-"
                + W_SYMBOL
                + "s  %-"
                + W_STREAM
                + "s  %-"
                + W_START
                + "s  %-"
                + W_END
                + "s  %-"
                + W_REASON
                + "s  %s%n",
            "source",
            "exchange",
            "symbol",
            "stream",
            "startMs",
            "endMs",
            "reason",
            "detail"));
    // Separator
    int totalWidth =
        W_SOURCE
            + 2
            + W_EXCHANGE
            + 2
            + W_SYMBOL
            + 2
            + W_STREAM
            + 2
            + W_START
            + 2
            + W_END
            + 2
            + W_REASON
            + 2
            + 10;
    sb.append("-".repeat(totalWidth)).append('\n');
    // Rows
    for (GapRecord r : records) {
      sb.append(
          String.format(
              "%-"
                  + W_SOURCE
                  + "s  %-"
                  + W_EXCHANGE
                  + "s  %-"
                  + W_SYMBOL
                  + "s  %-"
                  + W_STREAM
                  + "s  %-"
                  + W_START
                  + "d  %-"
                  + W_END
                  + "d  %-"
                  + W_REASON
                  + "s  %s%n",
              truncate(r.source(), W_SOURCE),
              truncate(r.exchange(), W_EXCHANGE),
              truncate(r.symbol(), W_SYMBOL),
              truncate(r.stream(), W_STREAM),
              r.startMs(),
              r.endMs(),
              truncate(r.reason(), W_REASON),
              r.detail() != null ? r.detail() : ""));
    }
    return sb.toString();
  }

  /** Truncates a string to {@code maxLen} characters if it exceeds it. */
  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return "";
    }
    return s.length() <= maxLen ? s : s.substring(0, maxLen - 1) + "…";
  }
}
