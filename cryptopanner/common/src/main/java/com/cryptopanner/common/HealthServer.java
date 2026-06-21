package com.cryptopanner.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Minimal liveness + metrics HTTP server (master spec §13). Serves {@code GET /status} (always
 * {@code 200 "ok"}) and {@code GET /metrics} (OpenMetrics text from the supplied provider,
 * evaluated per request so counters are live). VPN-bound, read-only, no auth.
 */
public final class HealthServer implements AutoCloseable {

  private final HttpServer server;

  /** Binds to {@code port} (0 = ephemeral, useful for tests) and starts serving immediately. */
  public HealthServer(int port, Supplier<String> metrics) throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/status", ex -> respond(ex, 200, "ok\n"));
    server.createContext("/metrics", ex -> respond(ex, 200, metrics.get()));
    server.setExecutor(null); // default executor (serial); request volume is trivial
    server.start();
  }

  /** The actual bound port (resolves the ephemeral port when constructed with 0). */
  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private static void respond(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(bytes);
    }
  }
}
