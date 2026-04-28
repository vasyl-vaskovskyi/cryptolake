package com.cryptolake.verify.integrity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Checks aggregate trade ID ({@code a}) continuity.
 *
 * <p>Ports {@code _check_trades} from {@code integrity.py}. Walks aggregate trade IDs and reports
 * gaps where {@code a != lastA + 1}.
 *
 * <p>Tier 5 E1 — all IDs ({@code a} field) are {@code long} (Tier 5 E1 watch-out: exceed 2^31).
 *
 * <p>Thread safety: stateless utility.
 */
public final class TradesContinuity {

  private TradesContinuity() {}

  /**
   * Checks aggregate trade ID continuity in the stream of records.
   *
   * @param records iterator over parsed data envelopes for the trades stream
   * @param mapper shared {@link ObjectMapper} for parsing {@code raw_text}
   * @return {@link IntegrityCheckResult} with break details
   */
  public static IntegrityCheckResult check(Iterator<JsonNode> records, ObjectMapper mapper) {
    List<IntegrityCheckResult.Break> breaks = new ArrayList<>();
    int count = 0;
    long lastA = -1L; // -1 sentinel = not yet seen

    while (records.hasNext()) {
      JsonNode env = records.next();
      if (!"data".equals(env.path("type").asText())) {
        continue; // skip gap envelopes
      }
      count++;

      // Parse raw_text to get the aggregate trade ID (Tier 5 E1 — asLong)
      long a = -1L;
      try {
        String rawText = env.path("raw_text").asText("");
        if (!rawText.isEmpty()) {
          JsonNode raw = mapper.readTree(rawText);
          a = raw.path("a").asLong(-1L);
        }
      } catch (IOException | RuntimeException ignored) {
        continue; // skip on parse error
      }

      if (a < 0L) {
        continue;
      }

      if (lastA >= 0L && a != lastA + 1L) {
        long missing = a - lastA - 1L;
        long atReceived = env.path("received_at").asLong(0L);
        breaks.add(new IntegrityCheckResult.Break("a", lastA + 1L, a, missing, atReceived));
      }
      lastA = a;
    }

    return new IntegrityCheckResult(count, breaks);
  }
}
