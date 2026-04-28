package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Interpolates synthetic {@code markPriceUpdate} records across a funding_rate gap window.
 *
 * <p>Ports {@code _interpolate_funding_rate_gap} from {@code gaps.py:155-190}. Generates one
 * synthetic record per second across the gap, linearly interpolating price fields.
 *
 * <p>Tier 5 M4 — prices formatted as {@code String.format(Locale.ROOT, "%.8f", value)}. Tier 5 M16
 * — {@code computeNextFundingTime} uses 0/8/16 UTC boundaries.
 *
 * <p>Thread safety: stateless utility.
 */
public final class FundingRateInterpolator {

  private FundingRateInterpolator() {}

  /**
   * Generates interpolated mark-price records for a funding_rate gap.
   *
   * @param beforeRecord boundary record before the gap
   * @param afterRecord boundary record after the gap
   * @param gapStartMs gap start in milliseconds
   * @param gapEndMs gap end in milliseconds
   * @param symbol symbol name
   * @param mapper shared {@link ObjectMapper}
   * @return list of interpolated records (one per second in the gap window)
   */
  public static List<JsonNode> interpolate(
      JsonNode beforeRecord,
      JsonNode afterRecord,
      long gapStartMs,
      long gapEndMs,
      String symbol,
      ObjectMapper mapper) {

    List<JsonNode> results = new ArrayList<>();

    double startP = boundaryDouble(beforeRecord, "p");
    double endP = boundaryDouble(afterRecord, "p");
    double startI = boundaryDouble(beforeRecord, "i");
    double endI = boundaryDouble(afterRecord, "i");
    double startCapP = boundaryDouble(beforeRecord, "P");
    double endCapP = boundaryDouble(afterRecord, "P");
    String rate = beforeRecord.path("r").asText("0");
    long nextFunding = beforeRecord.path("T").asLong(computeNextFundingTime(gapStartMs));

    // Align to second boundaries
    long firstSec = ((gapStartMs / 1000L) + 1L) * 1000L;
    long lastSec = (gapEndMs / 1000L) * 1000L;
    long totalSpan = Math.max(1L, lastSec - firstSec);

    for (long t = firstSec; t <= lastSec; t += 1000L) {
      double frac = (totalSpan > 0) ? (double) (t - firstSec) / totalSpan : 0.0;
      double p = startP + (endP - startP) * frac;
      double i = startI + (endI - startI) * frac;
      double capP = startCapP + (endCapP - startCapP) * frac;

      // Build the interpolated record in Python insertion order (gaps.py:174-184)
      // Tier 5 B1: e, E, s, p, ap (alias for p), P, i, r, T
      ObjectNode node = mapper.createObjectNode();
      node.put("e", "markPriceUpdate");
      node.put("E", t);
      node.put("s", symbol.toUpperCase(Locale.ROOT)); // Tier 5 M1
      node.put("p", String.format(Locale.ROOT, "%.8f", p)); // Tier 5 M4
      node.put("ap", String.format(Locale.ROOT, "%.8f", p));
      node.put("P", String.format(Locale.ROOT, "%.8f", capP));
      node.put("i", String.format(Locale.ROOT, "%.8f", i));
      node.put("r", rate);
      node.put("T", nextFunding);
      results.add(node);
    }

    return results;
  }

  /**
   * Computes the next funding time given a reference millisecond timestamp.
   *
   * <p>Tier 5 M16 — Binance funding occurs at 0, 8, 16 UTC boundaries.
   */
  public static long computeNextFundingTime(long refMs) {
    // Funding times are at 0:00, 8:00, 16:00 UTC
    long msPerDay = 86_400_000L;
    long msPerPeriod = 8 * 3600_000L;
    long dayStart = (refMs / msPerDay) * msPerDay;
    for (int offset = 0; offset < 3; offset++) {
      long fundingTs = dayStart + (long) offset * msPerPeriod;
      if (fundingTs > refMs) {
        return fundingTs;
      }
    }
    // Roll to next day
    return dayStart + msPerDay;
  }

  /** Parses a double field from a boundary record (returns 0.0 on missing/error). */
  private static double boundaryDouble(JsonNode node, String field) {
    JsonNode n = node.path(field);
    if (n.isMissingNode() || n.isNull()) {
      return 0.0;
    }
    if (n.isTextual()) {
      try {
        return Double.parseDouble(n.asText());
      } catch (NumberFormatException e) {
        return 0.0;
      }
    }
    return n.asDouble(0.0);
  }
}
