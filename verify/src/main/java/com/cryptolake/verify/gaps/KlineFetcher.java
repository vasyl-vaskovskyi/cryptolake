package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;

/**
 * Fetches mark/index/premium klines for the funding-rate boundary build path.
 *
 * <p>Ports the kline-fetch helpers from {@code gaps.py}. All methods use the shared {@link
 * BinanceRestClient} (Tier 5 D3).
 *
 * <p>Thread safety: stateless; relies on thread-safe {@link BinanceRestClient}.
 */
public final class KlineFetcher {

  private static final String BASE = GapStreams.BINANCE_REST_BASE;

  private final BinanceRestClient client;

  public KlineFetcher(BinanceRestClient client) {
    this.client = client;
  }

  /**
   * Fetches mark price klines for a symbol in a time range.
   *
   * @param symbol symbol (uppercase, e.g. "BTCUSDT")
   * @param startMs start time in milliseconds
   * @param endMs end time in milliseconds
   * @return kline data as a {@link JsonNode} array
   * @throws IOException on HTTP failure
   * @throws InterruptedException if interrupted during retry sleep
   */
  public JsonNode fetchMarkKlines(String symbol, long startMs, long endMs)
      throws IOException, InterruptedException {
    String url =
        BASE
            + "/fapi/v1/markPriceKlines?symbol="
            + symbol.toUpperCase(Locale.ROOT) // Tier 5 M1
            + "&interval=1m&startTime="
            + startMs
            + "&endTime="
            + endMs
            + "&limit=1500";
    return client.fetchPage(url);
  }

  /** Fetches index price klines. */
  public JsonNode fetchIndexKlines(String symbol, long startMs, long endMs)
      throws IOException, InterruptedException {
    String url =
        BASE
            + "/fapi/v1/indexPriceKlines?pair="
            + symbol.toUpperCase(Locale.ROOT)
            + "&interval=1m&startTime="
            + startMs
            + "&endTime="
            + endMs
            + "&limit=1500";
    return client.fetchPage(url);
  }

  /** Fetches premium index klines. */
  public JsonNode fetchPremiumKlines(String symbol, long startMs, long endMs)
      throws IOException, InterruptedException {
    String url =
        BASE
            + "/fapi/v1/premiumIndexKlines?symbol="
            + symbol.toUpperCase(Locale.ROOT)
            + "&interval=1m&startTime="
            + startMs
            + "&endTime="
            + endMs
            + "&limit=1500";
    return client.fetchPage(url);
  }
}
