package com.cryptopanner.mockbinance;

import java.util.regex.Pattern;

/**
 * Restamps a replayed Binance frame's event time {@code "E"} and trade/transaction time {@code "T"}
 * to a supplied wall-clock millis value, leaving the rest of the frame byte-for-byte intact.
 *
 * <p>The dev fixture carries fixed historical timestamps, so without this every replayed frame
 * buckets into the same minute (server-event-time bucketing, §8.c) and minutes never seal during a
 * run. Rewriting only the uppercase {@code E}/{@code T} numeric values lets the collector fill
 * consecutive real-time minutes while preserving the fields that matter for verification — the
 * lowercase event-type {@code "e"}, ids ({@code "t"}), the depth pu-chain ({@code U}/{@code
 * u}/{@code pu}), and quoted prices (which are strings, never matched by the numeric pattern).
 */
public final class EventTimeRewriter {

  // Matches an uppercase "E" or "T" key mapped to an integer value, anywhere in the frame
  // (including nested objects). Quoted string values (prices, symbols) are not integers, so they
  // never match; lowercase e/t and other keys (U/u/pu) are excluded by the literal key.
  private static final Pattern TIMESTAMP = Pattern.compile("\"([ET])\"\\s*:\\s*-?\\d+");

  private EventTimeRewriter() {}

  public static String rewrite(String frame, long nowMillis) {
    return TIMESTAMP.matcher(frame).replaceAll(m -> "\"" + m.group(1) + "\":" + nowMillis);
  }
}
