package com.cryptopanner.common;

/**
 * Maps a stream name to the JSON field under {@code data} that carries its monotonic sequence ID.
 * Returns {@code null} for streams that are not ID-bearing — non-ID streams (kline, ticker,
 * bookTicker, markPrice, forceOrder) skip continuity validation entirely (master spec §7.a, §10.d).
 *
 * <p>{@code depth@100ms} is ID-bearing but uses the {@code U}/{@code u}/{@code pu} triple for
 * pu-chain continuity rather than a single field — that's a separate analyzer, not in scope here.
 */
public final class SequenceId {

  private SequenceId() {}

  /** Returns the {@code data.<field>} name carrying the sequence ID, or {@code null} if none. */
  public static String idField(String stream) {
    return switch (stream) {
      case "trade" -> "t";
      case "aggTrade" -> "a";
      default -> null;
    };
  }
}
