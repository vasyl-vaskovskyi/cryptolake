package com.cryptolake.collector.adapter;

import com.cryptolake.collector.capture.FrameRoute;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Binance USD-M Futures exchange adapter.
 *
 * <p>Stateless per instance except for the two base URLs and the subscribed-symbols filter (used to
 * drop foreign-symbol frames from the {@code !forceOrder@arr} broadcast — see below). All methods
 * are pure (thread-safe) given a thread-safe {@link ObjectMapper}.
 *
 * <p>The {@link #routeStream(String)} method extracts {@code raw_text} using balanced-brace byte
 * slicing (via {@link RawDataExtractor}) <em>before</em> any Jackson parse.
 *
 * <p>Routing model (post-Binance routed-endpoint change, May 2026): two sockets, one per Binance
 * routed path. The adapter exposes both URLs and the per-socket subscription list; the supervisor
 * owns one connection per entry. Liquidations are subscribed via the single all-market broadcast
 * {@code "!forceOrder@arr"} on the {@code /market} socket; each frame's symbol is extracted from
 * {@code data.o.s} and frames for unsubscribed symbols are dropped here.
 */
public final class BinanceAdapter {

  private static final String DATA_KEY = "\"data\":";

  private final String wsBase;
  private final String restBase;
  private final ObjectMapper mapper;
  private final Set<String> subscribedSymbols;

  public BinanceAdapter(
      String wsBase, String restBase, ObjectMapper mapper, List<String> subscribedSymbols) {
    this.wsBase = wsBase.replaceAll("/+$", "");
    this.restBase = restBase.replaceAll("/+$", "");
    this.mapper = mapper;
    Set<String> lower = new HashSet<>(subscribedSymbols.size());
    for (String s : subscribedSymbols) lower.add(s.toLowerCase(Locale.ROOT));
    this.subscribedSymbols = Set.copyOf(lower);
  }

  // ── WebSocket URL building ────────────────────────────────────────────────

  /**
   * Returns the wire subscriptions for a specific socket. {@code socketName} must be one of {@link
   * StreamKey#SOCKET_PUBLIC} or {@link StreamKey#SOCKET_MARKET}.
   *
   * <p>Public socket: one per-symbol entry per public stream. Market socket: one per-symbol entry
   * per non-liquidation market stream, plus the single broadcast {@code "!forceOrder@arr"} if
   * {@code liquidations} is enabled.
   */
  public List<String> getSubscriptionsForSocket(
      String socketName, List<String> symbols, List<String> streams) {
    Set<String> socketStreams =
        StreamKey.SOCKET_PUBLIC.equals(socketName)
            ? StreamKey.PUBLIC_WS_STREAMS
            : StreamKey.MARKET_WS_STREAMS;
    List<String> subs = new ArrayList<>();
    boolean liquidationsEnabled = false;
    for (String stream : streams) {
      if (!socketStreams.contains(stream)) continue;
      if ("liquidations".equals(stream)) {
        liquidationsEnabled = true;
        continue;
      }
      String suffix = StreamKey.subscriptionSuffix(stream);
      if (suffix == null) continue;
      for (String symbol : symbols) {
        subs.add(symbol.toLowerCase(Locale.ROOT) + suffix);
      }
    }
    if (liquidationsEnabled) {
      subs.add(StreamKey.LIQUIDATIONS_BROADCAST_SUBSCRIPTION);
    }
    return subs;
  }

  /**
   * Returns a map of socket name → bare combined-stream URL, one entry per socket that actually has
   * subscriptions. Empty map means no WS connection is needed.
   *
   * <p>Public streams go to {@code wss://.../public/stream}; market streams go to {@code
   * wss://.../market/stream}. The legacy unrouted {@code /stream} path silently routes to {@code
   * /public} only, which is exactly what broke trades/funding_rate/liquidations on the
   * single-socket build.
   */
  public Map<String, String> getWsUrls(List<String> symbols, List<String> streams) {
    Map<String, String> result = new LinkedHashMap<>();
    if (!getSubscriptionsForSocket(StreamKey.SOCKET_PUBLIC, symbols, streams).isEmpty()) {
      result.put(StreamKey.SOCKET_PUBLIC, wsBase + "/public/stream");
    }
    if (!getSubscriptionsForSocket(StreamKey.SOCKET_MARKET, symbols, streams).isEmpty()) {
      result.put(StreamKey.SOCKET_MARKET, wsBase + "/market/stream");
    }
    return result;
  }

  // ── Frame routing ─────────────────────────────────────────────────────────

  /**
   * Parses a combined-stream frame and returns a {@link FrameRoute} carrying the internal stream
   * type, symbol (lowercase), and the raw data value (byte-for-byte substring of the frame).
   *
   * <p>Returns {@code null} when the stream key is unknown, or when the frame is a {@code
   * !forceOrder@arr} broadcast event for a symbol we did not subscribe to (caller drops silently).
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

    // 3a. Broadcast liquidations: stream key is "!forceOrder@arr"; extract symbol from data.o.s.
    if (StreamKey.LIQUIDATIONS_BROADCAST_STREAM_KEY.equals(streamKey)) {
      JsonNode dataNode = parsed.get("data");
      if (dataNode == null) return null;
      JsonNode oNode = dataNode.get("o");
      if (oNode == null) return null;
      JsonNode symNode = oNode.get("s");
      if (symNode == null) return null;
      String symbol = symNode.asText().toLowerCase(Locale.ROOT);
      if (!subscribedSymbols.contains(symbol)) {
        return null; // foreign symbol — drop silently (Tier 1 §3)
      }
      return new FrameRoute("liquidations", symbol, rawText);
    }

    // 3b. Standard per-symbol stream: "btcusdt@aggTrade" → (symbol, streamType)
    String[] parts = StreamKey.parseStreamKey(streamKey);
    if (parts == null) return null; // unknown stream type — drop silently
    return new FrameRoute(parts[1], parts[0], rawText);
  }

  // ── Exchange timestamp extraction ─────────────────────────────────────────

  /**
   * Extracts the exchange timestamp in milliseconds from a data payload. Returns {@code null} for
   * streams that have no event timestamp (depth_snapshot, or any missing "E" field).
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

  /** Builds the depth snapshot URL for the given symbol (uppercase). */
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
   * Parses the depth snapshot's {@code lastUpdateId} from raw JSON text.
   *
   * @throws UncheckedIOException if {@code rawText} is not valid JSON
   */
  public long parseSnapshotLastUpdateId(String rawText) {
    try {
      JsonNode node = mapper.readTree(rawText);
      return node.get("lastUpdateId").asLong();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses {@code U}, {@code u}, {@code pu} from a depth-diff payload (all long).
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
