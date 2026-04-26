package com.cryptolake.collector.adapter;

/**
 * Extracts the raw JSON value of the {@code "data"} key from a Binance combined-stream frame using
 * balanced-brace counting.
 *
 * <p>Tier 1 §1 and Tier 5 B4: the extraction happens <em>before</em> any {@code ObjectMapper.readTree}
 * call, preserving the exact byte sequence of the inner payload. Any parse-and-reserialize
 * round-trip via Jackson would break byte identity (gate 3 invariant).
 *
 * <p>Ports Python's {@code _extract_data_value} in {@code src/exchanges/binance.py:160-196}.
 *
 * <p>Stateless utility. Thread-safe.
 */
public final class RawDataExtractor {

  private RawDataExtractor() {}

  /**
   * Extracts the JSON object value that follows the {@code "data":} key in {@code frame}.
   *
   * <p>The returned string is a substring of the input — same char sequence, no copying beyond
   * what {@link String#substring} does. After {@code getBytes(UTF_8)}, the bytes are identical to
   * the original frame bytes for that slice.
   *
   * @param frame the full combined-stream WebSocket frame text (e.g. {@code
   *     {"stream":"btcusdt@aggTrade","data":{...}}})
   * @param searchStart offset within {@code frame} to start scanning for the data value (should be
   *     set to the index right after the {@code "data":} key + colon)
   * @return substring covering the full JSON object value, braces inclusive
   * @throws IllegalArgumentException if the frame is truncated or braces are unbalanced
   */
  public static String extractDataValue(String frame, int searchStart) {
    // Skip whitespace
    int i = searchStart;
    int len = frame.length();
    while (i < len && isWhitespace(frame.charAt(i))) {
      i++;
    }

    if (i >= len) {
      throw new IllegalArgumentException("Frame truncated: no value after 'data' key");
    }
    if (frame.charAt(i) != '{') {
      throw new IllegalArgumentException(
          "Expected '{' at position " + i + ", got '" + frame.charAt(i) + "'");
    }

    int depth = 0;
    boolean inString = false;
    boolean escapeNext = false;
    int start = i;

    while (i < len) {
      char ch = frame.charAt(i);
      if (escapeNext) {
        escapeNext = false;
      } else if (ch == '\\') {
        if (inString) {
          escapeNext = true;
        }
      } else if (ch == '"') {
        inString = !inString;
      } else if (!inString) {
        if (ch == '{') {
          depth++;
        } else if (ch == '}') {
          depth--;
          if (depth == 0) {
            return frame.substring(start, i + 1);
          }
        }
      }
      i++;
    }

    throw new IllegalArgumentException("Unbalanced braces in frame");
  }

  private static boolean isWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
  }
}
