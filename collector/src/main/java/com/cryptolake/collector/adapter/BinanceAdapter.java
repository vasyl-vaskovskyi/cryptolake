package com.cryptolake.collector.adapter;

import com.cryptolake.collector.capture.FrameRoute;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Binance USD-M Futures exchange adapter.
 *
 * <p>Ports {@code BinanceAdapter} from {@code src/exchanges/binance.py}. Stateless per instance
 * except for the two base URLs. All methods are pure (thread-safe) given a thread-safe {@link
 * ObjectMapper}.
 *
 * <p>The {@link #routeStream(String)} method extracts {@code raw_text} using balanced-brace byte
 * slicing (via {@link RawDataExtractor}) <em>before</em> any Jackson parse — Tier 1 §1, Tier 5 B4.
 *
 * <p>Update IDs are returned as {@code long} throughout — Tier 5 E1.
 */
public final class BinanceAdapter {

  private static final String DATA_KEY = "\"data\":";

  private final String wsBase;
  private final String restBase;
  private final ObjectMapper mapper;

  public BinanceAdapter(String wsBase, String restBase, ObjectMapper mapper) {
    this.wsBase = wsBase.replaceAll("/+$", "");
    this.restBase = restBase.replaceAll("/+$", "");
    this.mapper = mapper;
  }

  // ── WebSocket URL building ────────────────────────────────────────────────

  /**
   * Returns the subscriptions list for the SUBSCRIBE JSON message: one entry per {@code (symbol,
   * stream)} pair that has a WS subscription suffix (Tier 5 M2).
   */
  public List<String> getSubscriptions(List<String> symbols, List<String> streams) {
    List<String> subs = new ArrayList<>();
    for (String symbol : symbols) {
      String s = symbol.toLowerCase(Locale.ROOT);
      for (String stream : streams) {
        String suffix = StreamKey.subscriptionSuffix(stream);
        if (suffix == null || !StreamKey.WS_STREAMS.contains(stream)) continue;
        subs.add(s + suffix);
      }
    }
    return subs;
  }

  /**
   * Returns a map with one entry {@code "ws"} → bare combined-stream URL if there are WS
   * subscriptions; empty map otherwise (no subscriptions = no connection needed).
   */
  public Map<String, String> getWsUrls(List<String> symbols, List<String> streams) {
    if (getSubscriptions(symbols, streams).isEmpty()) return Map.of();
    Map<String, String> result = new LinkedHashMap<>();
    result.put("ws", wsBase + "/stream");
    return result;
  }

  // ── Frame routing ─────────────────────────────────────────────────────────

  /**
   * Parses a combined-stream frame and returns a {@link FrameRoute} carrying the internal stream
   * type, symbol (lowercase), and the raw data value (byte-for-byte substring of the frame — Tier 1
   * §1, Tier 5 B4).
   *
   * <p>Returns {@code null} when the stream key is unknown (caller should drop the frame silently).
   */
  public FrameRoute routeStream(String rawFrame) {
    // 1. Locate "data": and extract the value via balanced brace counting BEFORE any parse.
    int dataIdx = rawFrame.indexOf(DATA_KEY);
    if (dataIdx < 0) return null; // not a data frame (e.g. subscription ack)
    int dataStart = dataIdx + DATA_KEY.length();
    String rawText;
    try {
      rawText = RawDataExtractor.extractDataValue(rawFrame, dataStart);
    } catch (IllegalArgumentException e) {
      return null; // malformed frame — drop
    }

    // 2. Parse outer frame for routing only (just the "stream" field).
    JsonNode parsed;
    try {
      parsed = mapper.readTree(rawFrame);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    JsonNode streamNode = parsed.get("stream");
    if (streamNode == null) return null;
    String streamKey = streamNode.asText();

    // 3. Parse stream key: "btcusdt@aggTrade" → (symbol, streamType)
    String[] parts = StreamKey.parseStreamKey(streamKey);
    if (parts == null) return null; // unknown stream type — drop silently

    return new FrameRoute(parts[1], parts[0], rawText);
  }

  // ── Exchange timestamp extraction ─────────────────────────────────────────

  /**
   * Extracts the exchange timestamp in milliseconds from a data payload. Returns {@code null} for
   * streams that have no event timestamp (depth_snapshot, or any missing "E" field).
   *
   * <p>The returned value is left as-is (ms) — callers convert to the correct unit for latency math
   * (Tier 5 E4).
   */
  public Long extractExchangeTs(String stream, String rawText) {
    JsonNode parsed;
    try {
      parsed = mapper.readTree(rawText);
    } catch (IOException e) {
      return null; // malformed payload; upstream caller handles
    }
    if ("open_interest".equals(stream)) {
      JsonNode t = parsed.path("time");
      return t.isMissingNode() ? null : t.asLong();
    }
    if ("depth_snapshot".equals(stream)) {
      return null; // REST snapshot has no event timestamp
    }
    JsonNode e = parsed.path("E");
    return e.isMissingNode() ? null : e.asLong();
  }

  // ── REST URL builders ─────────────────────────────────────────────────────

  /** Builds the depth snapshot URL for the given symbol (uppercase — Tier 5 M1). */
  public String buildSnapshotUrl(String symbol, int limit) {
    return restBase
        + "/fapi/v1/depth?symbol="
        + symbol.toUpperCase(Locale.ROOT)
        + "&limit="
        + limit;
  }

  /** Builds the open-interest URL for the given symbol. */
  public String buildOpenInterestUrl(String symbol) {
    return restBase + "/fapi/v1/openInterest?symbol=" + symbol.toUpperCase(Locale.ROOT);
  }

  // ── Depth ID parsing ──────────────────────────────────────────────────────

  /**
   * Parses the depth snapshot's {@code lastUpdateId} from raw JSON text (Tier 5 E1 — never int).
   *
   * @throws UncheckedIOException if {@code rawText} is not valid JSON
   */
  public long parseSnapshotLastUpdateId(String rawText) {
    try {
      JsonNode node = mapper.readTree(rawText);
      return node.get("lastUpdateId").asLong(); // never .asInt() — Tier 5 E1
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses {@code U}, {@code u}, {@code pu} from a depth-diff payload (Tier 5 E1 — all long).
   *
   * @throws UncheckedIOException if {@code rawText} is not valid JSON
   */
  public DepthUpdateIds parseDepthUpdateIds(String rawText) {
    try {
      JsonNode node = mapper.readTree(rawText);
      long U = node.get("U").asLong();
      long u = node.get("u").asLong();
      long pu = node.get("pu").asLong();
      return new DepthUpdateIds(U, u, pu);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
