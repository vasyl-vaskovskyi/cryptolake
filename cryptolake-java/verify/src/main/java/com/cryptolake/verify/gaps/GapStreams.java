package com.cryptolake.verify.gaps;

import java.util.Map;
import java.util.Set;

/**
 * Constant sets and maps from {@code gaps.py:23-38}.
 *
 * <p>Immutable collections; all types match Python's semantics (Tier 5 B1).
 */
public final class GapStreams {

  public static final String BINANCE_REST_BASE = "https://fapi.binance.com";

  /** Streams that can be recovered via REST backfill. */
  public static final Set<String> BACKFILLABLE = Set.of("trades", "funding_rate", "open_interest");

  /** Streams that cannot be backfilled (no REST endpoint with historic data). */
  public static final Set<String> NON_BACKFILLABLE =
      Set.of("depth", "depth_snapshot", "bookticker", "mark_price");

  /**
   * Maps stream name to the field in the raw payload used as the exchange timestamp key.
   *
   * <p>Matches {@code gaps.py:STREAM_TS_KEYS}.
   */
  public static final Map<String, String> STREAM_TS_KEYS =
      Map.of(
          "trades", "T",
          "funding_rate", "T",
          "open_interest", "T",
          "depth", "E",
          "depth_snapshot", "E",
          "bookticker", "T",
          "mark_price", "T");

  /**
   * Per-stream REST endpoint weight (Binance rate-limit units per request).
   *
   * <p>Matches {@code gaps.py:ENDPOINT_WEIGHTS}.
   */
  public static final Map<String, Integer> ENDPOINT_WEIGHTS =
      Map.of(
          "trades", 1,
          "funding_rate", 1,
          "open_interest", 1);

  private GapStreams() {}
}
