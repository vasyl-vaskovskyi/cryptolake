package com.cryptolake.common.health;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tiny HTTP server exposing {@code /health}, {@code /ready}, and {@code /metrics} endpoints.
 *
 * <p>Uses JDK built-in {@link HttpServer} (framework-free, Tier 2 §11). Every request is served on
 * a fresh virtual thread via {@code Executors.newVirtualThreadPerTaskExecutor()} (design §3.2).
 *
 * <p>No {@code synchronized} blocks (Tier 2 §9). No {@code Thread.sleep} (Tier 2 §10). Handlers are
 * stateless; all mutable state lives in the injected {@link ReadyCheck}/{@link MetricsSource}
 * suppliers.
 */
public final class HealthServer {

  private static final Logger log = LoggerFactory.getLogger(HealthServer.class);

  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final String CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8";

  private final int port;
  private final ReadyCheck readyCheck;
  private final MetricsSource metricsSource;

  private volatile HttpServer server;

  /**
   * Constructs a health server.
   *
   * @param port TCP port to bind (0 = OS-assigned ephemeral port; call {@link #boundPort()} after
   *     {@link #start()})
   * @param readyCheck supplies readiness checks for {@code /ready}
   * @param metricsSource supplies Prometheus text bytes for {@code /metrics}
   */
  public HealthServer(int port, ReadyCheck readyCheck, MetricsSource metricsSource) {
    this.port = port;
    this.readyCheck = readyCheck;
    this.metricsSource = metricsSource;
  }

  /**
   * Binds the server and starts serving on a virtual-thread executor.
   *
   * @throws UncheckedIOException if binding fails
   */
  public void start() {
    try {
      HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
      s.createContext("/health", this::handleHealth);
      s.createContext("/ready", this::handleReady);
      s.createContext("/metrics", this::handleMetrics);
      s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
      s.start();
      this.server = s;
      log.info("health_server_started", (Object) null);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to start health server on port " + port, e);
    }
  }

  /**
   * Stops the server, giving in-flight requests up to 1 second to drain.
   *
   * <p>Best-effort: exceptions are caught and logged (Tier 5 G1; design §7.4).
   */
  public void stop() {
    HttpServer s = this.server;
    if (s != null) {
      try {
        s.stop(1); // best-effort shutdown; never block main shutdown path
      } catch (Exception ignored) {
        // best-effort shutdown; never block main shutdown path
        log.warn("health_server_stop_error", (Object) null);
      }
    }
  }

  /**
   * Returns the actual bound port (useful when the server was started on port 0).
   *
   * @throws IllegalStateException if the server has not been started yet
   */
  public int boundPort() {
    HttpServer s = this.server;
    if (s == null) throw new IllegalStateException("HealthServer not started");
    return s.getAddress().getPort();
  }

  // ── Handlers ──

  private void handleHealth(HttpExchange ex) throws IOException {
    byte[] body = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
    sendResponse(ex, 200, CONTENT_TYPE_JSON, body);
  }

  private void handleReady(HttpExchange ex) throws IOException {
    Map<String, Boolean> checks;
    try {
      checks = readyCheck.get();
    } catch (Exception e) {
      log.warn("ready_check_error", e);
      byte[] err = "{\"error\":\"check failed\"}".getBytes(StandardCharsets.UTF_8);
      sendResponse(ex, 503, CONTENT_TYPE_JSON, err);
      return;
    }

    boolean allReady = checks.values().stream().allMatch(Boolean::booleanValue);
    int status = allReady ? 200 : 503;

    byte[] body;
    try {
      body = DEFAULT_MAPPER.writeValueAsBytes(checks);
    } catch (JsonProcessingException e) {
      body = "{}".getBytes(StandardCharsets.UTF_8);
    }
    sendResponse(ex, status, CONTENT_TYPE_JSON, body);
  }

  private void handleMetrics(HttpExchange ex) throws IOException {
    byte[] body;
    try {
      body = metricsSource.get();
    } catch (Exception e) {
      log.warn("metrics_source_error", e);
      body = new byte[0];
    }
    if (body == null) body = new byte[0];
    sendResponse(ex, 200, CONTENT_TYPE_TEXT, body);
  }

  private static void sendResponse(HttpExchange ex, int status, String contentType, byte[] body)
      throws IOException {
    ex.getResponseHeaders().set("Content-Type", contentType);
    ex.sendResponseHeaders(status, body.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(body);
    }
  }
}
