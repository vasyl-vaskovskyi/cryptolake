package com.cryptolake.consolidation.core;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.Clocks;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Creates a synthetic gap envelope for a missing hour.
 *
 * <p>Ports {@code synthesize_missing_hour_gap} from {@code consolidate.py:154-178}.
 *
 * <p>Tier 1 §5 honored: every call produces (a) a {@code missing_hour} gap envelope, (b) a metric
 * increment in {@link com.cryptolake.consolidation.scheduler.ConsolidationMetrics}, and (c) a
 * structured log in {@link com.cryptolake.consolidation.scheduler.ConsolidationCycle}.
 *
 * <p>Tier 5 F3, M11 — always UTC. Tier 5 E5 — ns computation via {@code getEpochSecond() *
 * 1_000_000_000L + getNano()}. Tier 5 M10 — {@code session_seq = -1L}. Tier 5 M6 — {@code reason =
 * "missing_hour"} (in {@link com.cryptolake.common.envelope.GapReasons}).
 *
 * <p>Thread safety: stateless utility.
 */
public final class MissingHourGapFactory {

  private MissingHourGapFactory() {}

  /**
   * Creates a {@code missing_hour} gap envelope for the given hour, serialized as compact JSON.
   *
   * @param exchange exchange name
   * @param symbol symbol name
   * @param stream stream type
   * @param date date string (YYYY-MM-DD)
   * @param hour UTC hour (0–23)
   * @param sessionId session ID for this consolidation run
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return compact JSON bytes (no trailing newline — caller appends 0x0A if needed)
   * @throws IOException on serialization failure
   */
  public static byte[] create(
      String exchange,
      String symbol,
      String stream,
      String date,
      int hour,
      String sessionId,
      ObjectMapper mapper)
      throws IOException {

    LocalDate localDate = LocalDate.parse(date); // Tier 5 M11 — parse only
    Instant hourStart = localDate.atTime(hour, 0).toInstant(ZoneOffset.UTC); // Tier 5 F3
    Instant hourEndExclusive = hourStart.plus(Duration.ofHours(1));

    // Tier 5 E5: ns = epochSecond * 1_000_000_000L + nano
    long gapStartNs = hourStart.getEpochSecond() * 1_000_000_000L + hourStart.getNano();
    long gapEndNs = hourEndExclusive.getEpochSecond() * 1_000_000_000L - 1L; // matches Python

    // Tier 5 M10: session_seq = -1L (synthetic, not from a live session)
    GapEnvelope gap =
        GapEnvelope.create(
            exchange,
            symbol,
            stream,
            sessionId,
            -1L, // session_seq sentinel (Tier 5 M10)
            gapStartNs,
            gapEndNs,
            "missing_hour",
            "No data files found for hour " + hour + "; not recoverable via backfill",
            Clocks.systemNanoClock());

    return mapper.writeValueAsBytes(gap); // compact JSON (Tier 5 B2)
  }
}
