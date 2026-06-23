package com.cryptopanner.common;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * UTC-canonicalized path derivation per master spec §6.a and §10.c.
 *
 * <p>All paths are computed in UTC; the caller's local timezone is irrelevant.
 */
public final class Paths {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR =
      DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR_MINUTE =
      DateTimeFormatter.ofPattern("HH-mm").withZone(ZoneOffset.UTC);

  private Paths() {}

  /** segments/&lt;symbol&gt;/&lt;stream&gt;/&lt;date&gt;/minute-&lt;HH-MM&gt;.jsonl.zst */
  public static Path minuteSegment(Path base, String symbol, String stream, Instant t) {
    return minuteSegment(base, symbol, stream, t, false);
  }

  /**
   * Minute-segment path. When {@code shadow} is true the {@code .shadow} infix is inserted before
   * {@code .jsonl.zst} (overlap-minute segment written by a rotation/deploy shadow connection,
   * design doc §5.3) — e.g. {@code minute-14-23.shadow.jsonl.zst}.
   */
  public static Path minuteSegment(
      Path base, String symbol, String stream, Instant t, boolean shadow) {
    String infix = shadow ? ".shadow.jsonl.zst" : ".jsonl.zst";
    return base.resolve(symbol)
        .resolve(stream)
        .resolve(DATE.format(t))
        .resolve("minute-" + HOUR_MINUTE.format(t) + infix);
  }

  /** sealed/&lt;symbol&gt;/&lt;stream&gt;/&lt;date&gt;/hour-&lt;HH&gt;.jsonl.zst */
  public static Path hourSealed(Path base, String symbol, String stream, Instant t) {
    return base.resolve(symbol)
        .resolve(stream)
        .resolve(DATE.format(t))
        .resolve("hour-" + HOUR.format(t) + ".jsonl.zst");
  }

  /** &lt;node-id&gt;/&lt;symbol&gt;/&lt;stream&gt;/&lt;date&gt;/hour-&lt;HH&gt;.jsonl.zst */
  public static String s3Key(String nodeId, String symbol, String stream, Instant t) {
    return nodeId
        + "/"
        + symbol
        + "/"
        + stream
        + "/"
        + DATE.format(t)
        + "/hour-"
        + HOUR.format(t)
        + ".jsonl.zst";
  }
}
