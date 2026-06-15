package com.cryptopanner.mockbinance;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bare-bones HTTP server for the dev-stack REST mock. Handles:
 *
 * <ul>
 *   <li>{@code /fapi/v1/openInterest} — fresh-counter responses for the polling story.
 *   <li>{@code /fapi/v1/historicalTrades} — Binance-shaped trade objects from {@code fromId} up to
 *       {@code limit}; used by Sealer's gap backfill (master spec §7.d).
 *   <li>{@code /fapi/v1/aggTrades} — Binance-shaped agg-trade objects from {@code fromId} up to
 *       {@code limit}; same purpose for aggTrade gaps.
 * </ul>
 *
 * <p>The backfill endpoints synthesize responses on the fly — there's no authoritative ID space, so
 * the mock will always return the requested IDs unless overridden. For tests that need a
 * partial-fill scenario, supply an {@code unknownIds} set that the mock will skip when generating.
 */
public final class MockRestServer implements AutoCloseable {

  private final HttpServer server;
  private final AtomicLong oiCounter = new AtomicLong(123450);
  private final java.util.Set<Long> unknownIds = new java.util.HashSet<>();

  public MockRestServer(int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/fapi/v1/openInterest", this::handleOpenInterest);
    server.createContext("/fapi/v1/historicalTrades", this::handleHistoricalTrades);
    server.createContext("/fapi/v1/aggTrades", this::handleAggTrades);
    server.setExecutor(null);
    server.start();
    System.out.println("[mock-rest] listening on 0.0.0.0:" + port);
  }

  public int port() {
    return server.getAddress().getPort();
  }

  /** Mark IDs the mock should pretend not to know — useful for partial-fill chaos scenarios. */
  public void blockIds(java.util.Collection<Long> ids) {
    unknownIds.addAll(ids);
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void handleOpenInterest(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex);
    String symbol = q.getOrDefault("symbol", "UNKNOWN").toUpperCase();
    long oi = oiCounter.incrementAndGet();
    respondJson(
        ex,
        200,
        "{\"symbol\":\""
            + symbol
            + "\",\"openInterest\":\""
            + oi
            + ".000\",\"time\":"
            + System.currentTimeMillis()
            + "}");
  }

  private void handleHistoricalTrades(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex);
    String symbol = q.getOrDefault("symbol", "UNKNOWN").toUpperCase();
    long fromId = Long.parseLong(q.getOrDefault("fromId", "0"));
    int limit = Integer.parseInt(q.getOrDefault("limit", "500"));
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (long id = fromId; id < fromId + limit; id++) {
      if (unknownIds.contains(id)) continue;
      if (!first) sb.append(',');
      sb.append("{\"id\":")
          .append(id)
          .append(",\"price\":\"60001.00\",\"qty\":\"0.001\",\"quoteQty\":\"60.001\",\"time\":")
          .append(System.currentTimeMillis())
          .append(",\"isBuyerMaker\":false}");
      first = false;
    }
    sb.append("]");
    respondJson(ex, 200, sb.toString());
  }

  private void handleAggTrades(HttpExchange ex) throws IOException {
    Map<String, String> q = parseQuery(ex);
    String symbol = q.getOrDefault("symbol", "UNKNOWN").toUpperCase();
    long fromId = Long.parseLong(q.getOrDefault("fromId", "0"));
    int limit = Integer.parseInt(q.getOrDefault("limit", "500"));
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (long id = fromId; id < fromId + limit; id++) {
      if (unknownIds.contains(id)) continue;
      if (!first) sb.append(',');
      sb.append("{\"a\":")
          .append(id)
          .append(",\"p\":\"60001.50\",\"q\":\"0.010\",\"f\":")
          .append(id * 10)
          .append(",\"l\":")
          .append(id * 10 + 4)
          .append(",\"T\":")
          .append(System.currentTimeMillis())
          .append(",\"m\":false}");
      first = false;
    }
    sb.append("]");
    respondJson(ex, 200, sb.toString());
  }

  private static Map<String, String> parseQuery(HttpExchange ex) {
    Map<String, String> out = new HashMap<>();
    String query = ex.getRequestURI().getRawQuery();
    if (query == null) return out;
    for (String kv : query.split("&")) {
      int eq = kv.indexOf('=');
      if (eq > 0) out.put(kv.substring(0, eq), kv.substring(eq + 1));
    }
    return out;
  }

  private static void respondJson(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
