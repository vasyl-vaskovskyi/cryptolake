package com.cryptolake.verify.integrity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Checks depth update ID pu-chain continuity.
 *
 * <p>Ports {@code _check_depth} from {@code integrity.py}. Walks the pu chain and reports breaks
 * where {@code pu != lastU} (no snapshot anchoring here — purely sequential).
 *
 * <p>Tier 5 E1 — all IDs ({@code u}, {@code pu}) are {@code long}.
 *
 * <p>Thread safety: stateless utility.
 */
public final class DepthContinuity {

  private DepthContinuity() {}

  /**
   * Checks depth pu-chain continuity.
   *
   * @param records iterator over parsed depth data envelopes
   * @param mapper shared {@link ObjectMapper} for parsing {@code raw_text}
   * @return {@link IntegrityCheckResult} with break details
   */
  public static IntegrityCheckResult check(Iterator<JsonNode> records, ObjectMapper mapper) {
    List<IntegrityCheckResult.Break> breaks = new ArrayList<>();
    int count = 0;
    long lastU = -1L;

    while (records.hasNext()) {
      JsonNode env = records.next();
      if (!"data".equals(env.path("type").asText())) {
        continue;
      }
      count++;

      long u = -1L;
      long pu = -1L;
      try {
        String rawText = env.path("raw_text").asText("");
        if (!rawText.isEmpty()) {
          JsonNode raw = mapper.readTree(rawText);
          u = raw.path("u").asLong(-1L); // Tier 5 E1
          pu = raw.path("pu").asLong(-1L);
        }
      } catch (IOException | RuntimeException ignored) {
        continue;
      }

      if (u < 0L || pu < 0L) {
        continue;
      }

      if (lastU >= 0L && pu != lastU) {
        long atReceived = env.path("received_at").asLong(0L);
        breaks.add(new IntegrityCheckResult.Break("pu", lastU, pu, 0L, atReceived));
      }
      lastU = u;
    }

    return new IntegrityCheckResult(count, breaks);
  }
}
