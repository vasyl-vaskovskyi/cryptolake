package com.cryptopanner.agent;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Node Agent HTTP surface (master spec §11.c, §4.f). Read-only endpoints are unauthenticated; the
 * mutating endpoints require a valid bearer token + fresh timestamp via {@link BearerAuth}.
 *
 * <ul>
 *   <li>{@code GET /status} — aggregated component/VPS JSON snapshot (scraped by the Monitor)
 *   <li>{@code GET /metrics} — OpenMetrics text
 *   <li>{@code POST /restart/{component}} — e.g. {@code /restart/sealer}, {@code
 *       /restart/collector/a}
 *   <li>{@code POST /rotation/trigger} — operator-forced WS rotation (§5.4, §15.f)
 * </ul>
 *
 * <p>systemd/process side effects are injected (restart handler, rotation trigger) so the server is
 * testable over real HTTP without touching the host.
 */
public final class AgentServer implements AutoCloseable {

  private final HttpServer http;
  private final BearerAuth auth;

  public AgentServer(
      int port,
      BearerAuth auth,
      Supplier<String> statusJson,
      Supplier<String> metrics,
      Function<String, Boolean> restart,
      Supplier<Boolean> rotationTrigger)
      throws IOException {
    this.auth = auth;
    this.http = HttpServer.create(new InetSocketAddress(port), 0);

    http.createContext("/status", ex -> readOnly(ex, "application/json", statusJson));
    http.createContext("/metrics", ex -> readOnly(ex, "text/plain; version=0.0.4", metrics));
    http.createContext(
        "/restart/", ex -> mutating(ex, () -> restart.apply(component(ex, "/restart/"))));
    http.createContext("/rotation/trigger", ex -> mutating(ex, rotationTrigger));
    http.setExecutor(null);
    http.start();
  }

  public int port() {
    return http.getAddress().getPort();
  }

  @Override
  public void close() {
    http.stop(0);
  }

  private void readOnly(HttpExchange ex, String contentType, Supplier<String> body)
      throws IOException {
    if (!"GET".equals(ex.getRequestMethod())) {
      send(ex, 405, "method not allowed");
      return;
    }
    ex.getResponseHeaders().add("Content-Type", contentType);
    send(ex, 200, body.get());
  }

  /** Auth-gated mutating endpoint: authorize, then run {@code action} (returns success flag). */
  private void mutating(HttpExchange ex, Supplier<Boolean> action) throws IOException {
    if (!"POST".equals(ex.getRequestMethod())) {
      send(ex, 405, "method not allowed");
      return;
    }
    String authHeader = ex.getRequestHeaders().getFirst("Authorization");
    String ts = ex.getRequestHeaders().getFirst("X-Timestamp");
    BearerAuth.Outcome outcome = auth.check(authHeader, ts, Instant.now());
    switch (outcome) {
      case OK -> {
        boolean ok = action.get();
        send(ex, ok ? 200 : 500, ok ? "ok" : "failed");
      }
      case BAD_TIMESTAMP -> send(ex, 400, "bad timestamp");
      default -> send(ex, 401, outcome.name());
    }
  }

  /**
   * Extracts the component path segment after {@code prefix} (e.g. {@code sealer}, {@code
   * collector/a}).
   */
  private static String component(HttpExchange ex, String prefix) {
    String path = ex.getRequestURI().getPath();
    return path.length() > prefix.length() ? path.substring(prefix.length()) : "";
  }

  private static void send(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
