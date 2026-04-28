package com.cryptolake.verify.integrity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Checks bookticker update ID backwards-jump detection.
 *
 * <p>Ports {@code _check_bookticker} from {@code integrity.py}. Forward jumps in {@code u} are
 * normal (other symbols share the bookticker stream); only backwards jumps are breaks.
 *
 * <p>Tier 5 E1 — {@code u} field is {@code long}.
 *
 * <p>Thread safety: stateless utility.
 */
public final class BooktickerContinuity {

  private BooktickerContinuity() {}

  /**
   * Checks bookticker update ID for backwards jumps.
   *
   * @param records iterator over parsed bookticker data envelopes
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
      try {
        String rawText = env.path("raw_text").asText("");
        if (!rawText.isEmpty()) {
          JsonNode raw = mapper.readTree(rawText);
          u = raw.path("u").asLong(-1L); // Tier 5 E1
        }
      } catch (IOException | RuntimeException ignored) {
        continue;
      }

      if (u < 0L) {
        continue;
      }

      // Backwards jump only (forward jumps are normal — multi-symbol stream)
      if (lastU >= 0L && u < lastU) {
        long atReceived = env.path("received_at").asLong(0L);
        breaks.add(new IntegrityCheckResult.Break("u", lastU, u, 0L, atReceived));
      }
      lastU = Math.max(lastU, u); // keep track of highest seen
    }

    return new IntegrityCheckResult(count, breaks);
  }
}
