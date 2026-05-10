package com.cryptolake.collector.adapter;

import java.util.Map;
import java.util.Set;

/**
 * Binance WebSocket stream-key mapping constants.
 *
 * <p>Subscription suffixes and the stream-name dictionary that turns {@code "btcusdt@aggTrade"}
 * back into the pair {@code ("btcusdt", "trades")}. Uses static maps — NOT derived from an enum
 * because the mapping is asymmetric ({@code bookTicker} → {@code bookticker} →
 * {@code @bookTicker}).
 *
 * <p>Dual-socket routing (post-Binance routed-endpoint change, May 2026): Binance now splits its
 * USD-M Futures WS streams into routed endpoints {@code /public/stream} and {@code /market/stream}.
 * An unrouted {@code /stream} silently delivers only {@code /public} streams, which is what caused
 * trades / funding_rate / liquidations to go dark on the prior single-socket build. Streams are
 * bucketed via {@link #PUBLIC_WS_STREAMS} and {@link #MARKET_WS_STREAMS}; {@link #WS_STREAMS}
 * remains as the union for "is this a WS stream at all" checks.
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

  /** Streams delivered via Binance's {@code /public/stream} routed endpoint. */
  public static final Set<String> PUBLIC_WS_STREAMS = Set.of("depth", "bookticker");

  /** Streams delivered via Binance's {@code /market/stream} routed endpoint. */
  public static final Set<String> MARKET_WS_STREAMS =
      Set.of("trades", "funding_rate", "liquidations");

  /** All streams delivered over any WebSocket connection (union of public + market). */
  public static final Set<String> WS_STREAMS =
      Set.of("depth", "bookticker", "trades", "funding_rate", "liquidations");

  /** REST-only streams that must NOT receive WS subscriptions. */
  public static final Set<String> REST_ONLY_STREAMS = Set.of("depth_snapshot", "open_interest");

  /** Socket key name for the {@code /public/stream} connection. */
  public static final String SOCKET_PUBLIC = "public";

  /** Socket key name for the {@code /market/stream} connection. */
  public static final String SOCKET_MARKET = "market";

  /**
   * Maps internal stream type name to Binance per-symbol subscription suffix (e.g. {@code
   * "@aggTrade"}).
   *
   * <p>Note: {@code liquidations} is NOT here — its wire subscription is the single all-market
   * broadcast {@code "!forceOrder@arr"}, not a per-symbol suffix (see {@link
   * #LIQUIDATIONS_BROADCAST_SUBSCRIPTION}).
   */
  public static final Map<String, String> SUBSCRIPTION_MAP =
      Map.of(
          "depth", "@depth@100ms",
          "bookticker", "@bookTicker",
          "trades", "@aggTrade",
          "funding_rate", "@markPrice@1s");

  /**
   * The single all-market liquidation broadcast subscription. Binance no longer reliably delivers
   * per-symbol {@code <symbol>@forceOrder}; using the broadcast and filtering downstream gives
   * dense-enough data for the liveness watchdog to detect silent-drop bugs.
   */
  public static final String LIQUIDATIONS_BROADCAST_SUBSCRIPTION = "!forceOrder@arr";

  /**
   * Binance wire stream-key for the all-market liquidation broadcast (matches the subscription).
   */
  public static final String LIQUIDATIONS_BROADCAST_STREAM_KEY = "!forceOrder@arr";

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
   * "@aggTrade"}). Returns {@code null} if the stream has no per-symbol suffix (notably {@code
   * liquidations}, which uses the broadcast subscription instead).
   */
  public static String subscriptionSuffix(String stream) {
    return SUBSCRIPTION_MAP.get(stream);
  }

  /**
   * Returns the socket key that carries this stream, or {@code null} if the stream is not WS-bound.
   */
  public static String socketFor(String stream) {
    if (PUBLIC_WS_STREAMS.contains(stream)) return SOCKET_PUBLIC;
    if (MARKET_WS_STREAMS.contains(stream)) return SOCKET_MARKET;
    return null;
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
