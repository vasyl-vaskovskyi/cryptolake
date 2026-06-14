package com.cryptopanner.common;

/** Stream → routed socket mapping per master spec §7.a / §8.a. */
public final class StreamRouting {

  public enum Socket {
    PUBLIC,
    MARKET
  }

  private StreamRouting() {}

  /**
   * Returns the routed Binance socket for a stream type (the part after {@code <symbol>@}, with any
   * parameter suffixes stripped — e.g. {@code "depth@100ms"} → {@code "depth"}).
   *
   * <p>Throws {@link IllegalArgumentException} for unknown streams so the operator catches config
   * mistakes at startup rather than runtime.
   */
  public static Socket forStreamType(String streamType) {
    // Strip param suffixes (depth@100ms, markPrice@1s; kline_1m stays as "kline_1m").
    String base = streamType;
    int at = streamType.indexOf('@');
    if (at >= 0) {
      base = streamType.substring(0, at);
    }
    // kline_* all on /market
    if (base.startsWith("kline_")) {
      return Socket.MARKET;
    }
    return switch (base) {
      case "trade", "depth", "bookTicker" -> Socket.PUBLIC;
      case "aggTrade", "ticker", "markPrice", "miniTicker" -> Socket.MARKET;
      default -> throw new IllegalArgumentException("Unknown stream type: " + streamType);
    };
  }

  /**
   * Returns the routed socket for an all-market broadcast subscription (e.g. {@code
   * "!forceOrder@arr"}).
   */
  public static Socket forBroadcast(String broadcast) {
    if (broadcast.equals("!forceOrder@arr")) return Socket.MARKET;
    throw new IllegalArgumentException("Unknown broadcast: " + broadcast);
  }
}
