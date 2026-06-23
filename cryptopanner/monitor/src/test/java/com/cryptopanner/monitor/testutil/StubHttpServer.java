package com.cryptopanner.monitor.testutil;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A tiny configurable HTTP server for monitor tests: stands in for a Node Agent (`/status`,
 * `/metrics`, `/restart/...`) or a webhook endpoint (Telegram, Healthchecks). Each registered route
 * returns a canned ({@code status}, {@code body}); every received request is captured for
 * assertions. Ephemeral port, single serial executor.
 */
public final class StubHttpServer implements AutoCloseable {

  /** A captured inbound request. */
  public record Received(String method, String path, String body, Map<String, String> headers) {}

  private record Canned(int status, String body) {}

  private final HttpServer http;
  private final Map<String, Canned> routes = new ConcurrentHashMap<>();
  private final List<Received> received = new CopyOnWriteArrayList<>();

  public StubHttpServer() throws IOException {
    http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    http.createContext("/", this::dispatch);
    http.setExecutor(null);
    http.start();
  }

  /** Register a canned response for an exact path. */
  public StubHttpServer route(String path, int status, String body) {
    routes.put(path, new Canned(status, body));
    return this;
  }

  public int port() {
    return http.getAddress().getPort();
  }

  /** Host:port form used in MonitorConfig node endpoints. */
  public String endpoint() {
    return "127.0.0.1:" + port();
  }

  public String baseUrl() {
    return "http://" + endpoint();
  }

  public List<Received> received() {
    return received;
  }

  private void dispatch(HttpExchange ex) throws IOException {
    String path = ex.getRequestURI().getPath();
    byte[] reqBody = ex.getRequestBody().readAllBytes();
    Map<String, String> headers = new ConcurrentHashMap<>();
    ex.getRequestHeaders()
        .forEach((k, v) -> headers.put(k, v.isEmpty() ? "" : String.join(",", v)));
    received.add(
        new Received(
            ex.getRequestMethod(), path, new String(reqBody, StandardCharsets.UTF_8), headers));

    Canned canned = routes.getOrDefault(path, new Canned(404, "no route\n"));
    byte[] out = canned.body().getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(canned.status(), out.length);
    try (var os = ex.getResponseBody()) {
      os.write(out);
    }
  }

  @Override
  public void close() {
    http.stop(0);
  }
}
