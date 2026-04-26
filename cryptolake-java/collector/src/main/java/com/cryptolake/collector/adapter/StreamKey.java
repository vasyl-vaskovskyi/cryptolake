package com.cryptolake.collector.adapter;

import java.util.Map;
import java.util.Set;

/**
 * Binance WebSocket stream-key mapping constants.
 *
 * <p>Ports {@code _STREAM_KEY_MAP}, {@code _WS_STREAMS}, and {@code _SUBSCRIPTION_MAP} from {@code
 * src/exchanges/binance.py} (Tier 5 M2). Uses static maps — NOT derived from an enum because the
 * mapping is asymmetric ({@code bookTicker} → {@code bookticker} → {@code @bookTicker}).
 *
 * <p>Single consolidated socket design (commit 9e40d25): the prior public/market split is gone;
 * {@link #WS_STREAMS} covers all five stream types on one connection.
 *
 * <p>Stateless utility. Thread-safe.
 */
public final class StreamKey {

  /** Maps Binance wire event names to internal stream type names. */
  public static final Map<String, String> STREAM_KEY_MAP =
      Map.of(
          "aggTrade", "trades",
          "depth", "depth",
          "bookTicker", "bookticker",
          "markPrice", "funding_rate",
          "forceOrder", "liquidations");

  /** All streams delivered over the single WebSocket connection (commit 9e40d25). */
  public static final Set<String> WS_STREAMS =
      Set.of("depth", "bookticker", "trades", "funding_rate", "liquidations");

  /** REST-only streams that must NOT receive WS subscriptions. */
  public static final Set<String> REST_ONLY_STREAMS = Set.of("depth_snapshot", "open_interest");

  /** Maps internal stream type name to Binance subscription suffix (e.g. {@code @aggTrade}). */
  public static final Map<String, String> SUBSCRIPTION_MAP =
      Map.of(
          "depth", "@depth@100ms",
          "bookticker", "@bookTicker",
          "trades", "@aggTrade",
          "funding_rate", "@markPrice@1s",
          "liquidations", "@forceOrder");

  private StreamKey() {}

  /**
   * Converts a Binance wire event-type key (e.g. {@code "aggTrade"}) to the internal stream name
   * (e.g. {@code "trades"}). Returns {@code null} if unmapped.
   */
  public static String toInternal(String binanceKey) {
    return STREAM_KEY_MAP.get(binanceKey);
  }

  /**
   * Returns the WS subscription suffix for an internal stream name (e.g. {@code "trades"} → {@code
   * "@aggTrade"}). Returns {@code null} if not a WS stream.
   */
  public static String subscriptionSuffix(String stream) {
    return SUBSCRIPTION_MAP.get(stream);
  }

  /**
   * Parses a Binance combined-stream key (e.g. {@code "btcusdt@aggTrade"}) into {@code (symbol,
   * streamType)}.
   *
   * <p>Strips parameters after a second {@code "@"} (e.g. {@code depth@100ms} → {@code "depth"}).
   * Returns {@code null} for unknown stream keys (caller should drop the frame).
   *
   * @return 2-element array {@code [symbol, streamType]}, or {@code null} if unknown
   */
  public static String[] parseStreamKey(String streamKey) {
    int atIdx = streamKey.indexOf('@');
    if (atIdx < 0) return null;
    String symbol = streamKey.substring(0, atIdx).toLowerCase(java.util.Locale.ROOT);
    String remainder = streamKey.substring(atIdx + 1);
    // Strip parameters: "depth@100ms" -> "depth", "markPrice@1s" -> "markPrice"
    String base = remainder.split("@")[0];
    String streamType = STREAM_KEY_MAP.get(base);
    if (streamType == null) return null;
    return new String[] {symbol, streamType};
  }
}
