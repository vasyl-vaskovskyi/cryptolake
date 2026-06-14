package com.cryptopanner.mockbinance;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bare-bones HTTP server for the dev-stack REST mock. Supports a handful of Binance USD-M Futures
 * endpoints with canned bodies that change per request so we can see freshness on the consumer
 * side.
 */
public final class MockRestServer implements AutoCloseable {

  private final HttpServer server;
  private final AtomicLong oiCounter = new AtomicLong(123450);

  public MockRestServer(int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/fapi/v1/openInterest", this::handleOpenInterest);
    server.setExecutor(null); // default executor — one thread, fine for tests.
    server.start();
    System.out.println("[mock-rest] listening on 0.0.0.0:" + port);
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void handleOpenInterest(com.sun.net.httpserver.HttpExchange ex) throws IOException {
    String query = ex.getRequestURI().getRawQuery();
    String symbol = "UNKNOWN";
    if (query != null) {
      for (String kv : query.split("&")) {
        int eq = kv.indexOf('=');
        if (eq > 0 && "symbol".equals(kv.substring(0, eq))) {
          symbol = kv.substring(eq + 1).toUpperCase();
          break;
        }
      }
    }
    long oi = oiCounter.incrementAndGet();
    String body =
        "{\"symbol\":\""
            + symbol
            + "\",\"openInterest\":\""
            + oi
            + ".000\",\"time\":"
            + System.currentTimeMillis()
            + "}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
