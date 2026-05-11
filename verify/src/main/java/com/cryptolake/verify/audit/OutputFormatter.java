package com.cryptolake.verify.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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

  // -------------------------------------------------------------------------
  // Reconcile output
  // -------------------------------------------------------------------------

  /**
   * Serializes a {@link GapRecordReconciler.ReconcileResult} as a pretty-printed JSON object.
   *
   * <p>Shape:
   *
   * <pre>{@code
   * {
   *   "explained":   [ { "file": GapRecord, "state": GapRecord }, ... ],
   *   "unexplained": [ GapRecord, ... ],
   *   "orphanState": [ GapRecord, ... ]
   * }
   * }</pre>
   *
   * @param result the reconcile result
   * @param mapper shared Jackson mapper
   * @return pretty-printed JSON string
   */
  public static String toReconcileJson(
      GapRecordReconciler.ReconcileResult result, ObjectMapper mapper) {
    try {
      ObjectNode root = mapper.createObjectNode();

      ArrayNode explainedNode = root.putArray("explained");
      for (GapRecordReconciler.ExplainedRecord er : result.explained()) {
        ObjectNode pair = explainedNode.addObject();
        pair.set("file", mapper.valueToTree(er.file()));
        pair.set("state", mapper.valueToTree(er.state()));
      }

      ArrayNode unexplainedNode = root.putArray("unexplained");
      for (GapRecord r : result.unexplained()) {
        unexplainedNode.add(mapper.valueToTree(r));
      }

      ArrayNode orphanNode = root.putArray("orphanState");
      for (GapRecord r : result.orphanState()) {
        orphanNode.add(mapper.valueToTree(r));
      }

      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize ReconcileResult to JSON", e);
    }
  }

  /**
   * Formats a {@link GapRecordReconciler.ReconcileResult} as an aligned human-readable table.
   *
   * <p>Columns: {@code status}, {@code file_source}, {@code state_source}, {@code reason}, {@code
   * exchange}, {@code symbol}, {@code stream}, {@code window}.
   *
   * <p>Row status values:
   *
   * <ul>
   *   <li>{@code EXPLAINED} — file gap matched to a state cause
   *   <li>{@code UNEXPLAINED} — file gap with no match
   *   <li>{@code orphan} — state record with no file consequence
   * </ul>
   *
   * <p>Followed by a one-line summary: {@code Explained: X | UNEXPLAINED: Y | orphan: Z}.
   *
   * @param result the reconcile result
   * @return multi-line string (ends with a newline)
   */
  public static String toReconcileHuman(GapRecordReconciler.ReconcileResult result) {
    // Column widths
    final int W_STATUS = 11; // "UNEXPLAINED"
    final int W_FILE_SRC = 20;
    final int W_STATE_SRC = 22;
    final int W_REASON = 22;
    final int W_EXCHANGE = 10;
    final int W_SYMBOL = 12;
    final int W_STREAM = 14;
    // window is last — no fixed width

    StringBuilder sb = new StringBuilder();
    // Header
    sb.append(
        String.format(
            "%-"
                + W_STATUS
                + "s  %-"
                + W_FILE_SRC
                + "s  %-"
                + W_STATE_SRC
                + "s  %-"
                + W_REASON
                + "s  %-"
                + W_EXCHANGE
                + "s  %-"
                + W_SYMBOL
                + "s  %-"
                + W_STREAM
                + "s  %s%n",
            "status",
            "file_source",
            "state_source",
            "reason",
            "exchange",
            "symbol",
            "stream",
            "window"));
    int totalWidth =
        W_STATUS
            + 2
            + W_FILE_SRC
            + 2
            + W_STATE_SRC
            + 2
            + W_REASON
            + 2
            + W_EXCHANGE
            + 2
            + W_SYMBOL
            + 2
            + W_STREAM
            + 2
            + 30;
    sb.append("-".repeat(totalWidth)).append('\n');

    // EXPLAINED rows
    for (GapRecordReconciler.ExplainedRecord er : result.explained()) {
      GapRecord f = er.file();
      GapRecord s = er.state();
      sb.append(
          String.format(
              "%-"
                  + W_STATUS
                  + "s  %-"
                  + W_FILE_SRC
                  + "s  %-"
                  + W_STATE_SRC
                  + "s  %-"
                  + W_REASON
                  + "s  %-"
                  + W_EXCHANGE
                  + "s  %-"
                  + W_SYMBOL
                  + "s  %-"
                  + W_STREAM
                  + "s  %s%n",
              "EXPLAINED",
              truncate(f.source(), W_FILE_SRC),
              truncate(s.source(), W_STATE_SRC),
              truncate(f.reason(), W_REASON),
              truncate(f.exchange(), W_EXCHANGE),
              truncate(f.symbol(), W_SYMBOL),
              truncate(f.stream(), W_STREAM),
              isoWindow(f.startMs(), f.endMs())));
    }

    // UNEXPLAINED rows
    for (GapRecord f : result.unexplained()) {
      sb.append(
          String.format(
              "%-"
                  + W_STATUS
                  + "s  %-"
                  + W_FILE_SRC
                  + "s  %-"
                  + W_STATE_SRC
                  + "s  %-"
                  + W_REASON
                  + "s  %-"
                  + W_EXCHANGE
                  + "s  %-"
                  + W_SYMBOL
                  + "s  %-"
                  + W_STREAM
                  + "s  %s%n",
              "UNEXPLAINED",
              truncate(f.source(), W_FILE_SRC),
              "—",
              truncate(f.reason(), W_REASON),
              truncate(f.exchange(), W_EXCHANGE),
              truncate(f.symbol(), W_SYMBOL),
              truncate(f.stream(), W_STREAM),
              isoWindow(f.startMs(), f.endMs())));
    }

    // orphan rows
    for (GapRecord s : result.orphanState()) {
      sb.append(
          String.format(
              "%-"
                  + W_STATUS
                  + "s  %-"
                  + W_FILE_SRC
                  + "s  %-"
                  + W_STATE_SRC
                  + "s  %-"
                  + W_REASON
                  + "s  %-"
                  + W_EXCHANGE
                  + "s  %-"
                  + W_SYMBOL
                  + "s  %-"
                  + W_STREAM
                  + "s  %s%n",
              "orphan",
              "—",
              truncate(s.source(), W_STATE_SRC),
              truncate(s.reason(), W_REASON),
              truncate(s.exchange(), W_EXCHANGE),
              truncate(s.symbol(), W_SYMBOL),
              truncate(s.stream(), W_STREAM),
              isoWindow(s.startMs(), s.endMs())));
    }

    // Summary line
    sb.append(
        String.format(
            "Explained: %d | UNEXPLAINED: %d | orphan: %d%n",
            result.explained().size(), result.unexplained().size(), result.orphanState().size()));
    return sb.toString();
  }

  // ── internal helpers ──────────────────────────────────────────────────────

  private static final DateTimeFormatter ISO_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  /** Formats a time window as {@code startIso..endIso} using compact UTC timestamps. */
  private static String isoWindow(long startMs, long endMs) {
    return ISO_FMT.format(Instant.ofEpochMilli(startMs))
        + ".."
        + ISO_FMT.format(Instant.ofEpochMilli(endMs));
  }

  /** Truncates a string to {@code maxLen} characters if it exceeds it. */
  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return "";
    }
    return s.length() <= maxLen ? s : s.substring(0, maxLen - 1) + "…";
  }
}
