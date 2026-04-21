package com.cryptolake.writer.failover;

import com.cryptolake.common.envelope.DataEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts the natural sort key from a {@link DataEnvelope}'s raw text for failover deduplication.
 *
 * <p>Ports Python's {@code FailoverManager.extract_natural_key} (design §2.7; design §4.6; Tier 5
 * E1, M3).
 *
 * <p>Key extraction:
 * <ul>
 *   <li>Trades → {@code raw.get("a").asLong()} (aggregate trade ID — Tier 5 E1)
 *   <li>Depth → {@code raw.get("u").asLong()} (last update ID)
 *   <li>Bookticker → {@code raw.get("u").asLong()}
 *   <li>Else → {@code env.exchangeTs()}
 * </ul>
 *
 * <p>Returns primitive {@code long -1L} for "missing" — no {@code Optional} allocation (hot path;
 * Tier 5 M3 watch-out).
 *
 * <p>Thread safety: stateless utility; all methods are static.
 */
public final class NaturalKeyExtractor {

  private static final Logger log = LoggerFactory.getLogger(NaturalKeyExtractor.class);

  /** Maps stream types to their raw-text field names for natural key extraction (Tier 5 M3). */
  private static final Map<String, String> RAW_KEY_STREAMS =
      Map.of(
          "trades", "a",
          "depth", "u",
          "bookticker", "u");

  private NaturalKeyExtractor() {}

  /**
   * Extracts the natural key from the envelope's {@code rawText}.
   *
   * <p>Returns {@code -1L} for gap envelopes, missing raw data, or parse errors (Tier 5 M3
   * watch-out — sentinel, not null, to avoid allocation on hot path).
   *
   * @param env the data envelope
   * @param mapper the ObjectMapper for parsing raw_text (Tier 5 B3)
   * @return natural key as {@code long}, or {@code -1L} for missing/parse error
   */
  public static long extract(DataEnvelope env, ObjectMapper mapper) {
    String stream = env.stream();
    String fieldName = RAW_KEY_STREAMS.get(stream);
    if (fieldName == null) {
      return env.exchangeTs(); // No natural key for this stream type
    }

    String rawText = env.rawText();
    if (rawText == null || rawText.isEmpty()) {
      return -1L;
    }

    try {
      JsonNode node = mapper.readTree(rawText); // Tier 5 B3 — readTree not readValue
      // Use asLong(-1L) with default to avoid NPE on missing field (appendix watch-out)
      return node.path(fieldName).asLong(-1L); // Tier 5 E1 — asLong not asInt
    } catch (Exception e) {
      log.debug("natural_key_extract_failed", "stream", stream, "error", e.getMessage());
      return -1L;
    }
  }
}
