package com.cryptolake.consolidation.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Computes the sort key for a data envelope within a stream.
 *
 * <p>Ports {@code _data_sort_key} from {@code consolidate.py}. The sort key is a {@code long} (Tier
 * 5 M5; Tier 5 E1 — never {@code int} to avoid overflow on large IDs).
 *
 * <p>Thread safety: stateless utility.
 */
public final class DataSortKey {

  private DataSortKey() {}

  /**
   * Returns the sort key for a data record within the given stream.
   *
   * <ul>
   *   <li>{@code trades}: aggregate trade ID ({@code raw_text.a})
   *   <li>{@code depth}: update ID ({@code raw_text.u})
   *   <li>else: {@code exchange_ts}
   * </ul>
   *
   * @param record parsed envelope {@link JsonNode}
   * @param stream stream type
   * @param mapper shared {@link ObjectMapper} for parsing raw_text (Tier 5 B6)
   * @return long sort key
   */
  public static long of(JsonNode record, String stream, ObjectMapper mapper) {
    long exchangeTs = record.path("exchange_ts").asLong(0L);

    switch (stream) {
      case "trades":
        {
          String rawText = record.path("raw_text").asText("");
          if (!rawText.isEmpty()) {
            try {
              JsonNode raw = mapper.readTree(rawText);
              return raw.path("a").asLong(exchangeTs); // Tier 5 E1 — asLong
            } catch (IOException ignored) {
              // fallback to exchange_ts
            }
          }
          return exchangeTs;
        }
      case "depth":
        {
          String rawText = record.path("raw_text").asText("");
          if (!rawText.isEmpty()) {
            try {
              JsonNode raw = mapper.readTree(rawText);
              return raw.path("u").asLong(exchangeTs); // Tier 5 E1 — asLong
            } catch (IOException ignored) {
              // fallback to exchange_ts
            }
          }
          return exchangeTs;
        }
      default:
        return exchangeTs;
    }
  }
}
