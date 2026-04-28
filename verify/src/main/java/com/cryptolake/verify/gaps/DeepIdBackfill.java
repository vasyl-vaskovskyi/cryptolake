package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * ID-anchored backfill for trades using the Binance {@code aggTrades} REST endpoint.
 *
 * <p>Ports {@code _fetch_by_id} from {@code gaps.py}. Paginates via {@code fromId} until {@code
 * toId} is reached or the page is empty.
 *
 * <p>Tier 5 E1 — all IDs ({@code fromId}, {@code toId}, record {@code a} field) are {@code long}.
 * Tier 5 D3 — uses the shared {@link BinanceRestClient}.
 *
 * <p>Thread safety: stateless; relies on thread-safe {@link BinanceRestClient}.
 */
public final class DeepIdBackfill {

  private static final int PAGE_LIMIT = 1000;
  private static final String BASE = GapStreams.BINANCE_REST_BASE;

  private final BinanceRestClient client;

  public DeepIdBackfill(BinanceRestClient client) {
    this.client = client;
  }

  /**
   * Fetches aggregate trade records from {@code fromId} to {@code toId} (inclusive).
   *
   * @param symbol symbol (uppercase, e.g. "BTCUSDT")
   * @param fromId starting aggregate trade ID (Tier 5 E1 — long)
   * @param toId ending aggregate trade ID (inclusive; Tier 5 E1 — long)
   * @return list of trade records
   * @throws IOException on HTTP failure
   * @throws InterruptedException if interrupted during retry sleep
   */
  public List<JsonNode> fetchById(String symbol, long fromId, long toId)
      throws IOException, InterruptedException {
    List<JsonNode> results = new ArrayList<>();
    long currentId = fromId;

    while (currentId <= toId) {
      String url =
          BASE
              + "/fapi/v1/aggTrades?symbol="
              + symbol.toUpperCase(Locale.ROOT) // Tier 5 M1
              + "&fromId="
              + currentId
              + "&limit="
              + PAGE_LIMIT;

      JsonNode page = client.fetchPage(url);
      if (!page.isArray() || page.size() == 0) {
        break; // empty page = no more data
      }

      for (JsonNode record : page) {
        long a = record.path("a").asLong(0L); // Tier 5 E1 — asLong, never asInt
        if (a > toId) {
          return results;
        }
        results.add(record);
        currentId = a + 1L;
      }

      // If page was smaller than limit, we've exhausted the data
      if (page.size() < PAGE_LIMIT) {
        break;
      }
    }

    return results;
  }
}
