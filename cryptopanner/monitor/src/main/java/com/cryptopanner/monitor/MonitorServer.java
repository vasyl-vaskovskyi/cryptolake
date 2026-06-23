package com.cryptopanner.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

/**
 * The Monitor's read-only HTTP surface (master spec §11.d): {@code /dashboard} (auto-refreshing
 * HTML with one card per node, deploy/rotation banners, and unreachable/breaker highlights at the
 * top), {@code /api/nodes} (the JSON backing the dashboard), and its own {@code /status}+{@code
 * /metrics} for liveness. Same {@code com.sun.net.httpserver} pattern as {@code AgentServer}/{@code
 * HealthServer}; the view is supplied fresh each request from the scrape loop's latest snapshot.
 */
public final class MonitorServer implements AutoCloseable {

  private final HttpServer http;
  private final ObjectMapper mapper;
  private final Supplier<List<NodeView>> view;
  private final int refreshIntervalS;

  public MonitorServer(
      int port, ObjectMapper mapper, Supplier<List<NodeView>> view, int refreshIntervalS)
      throws IOException {
    this.mapper = mapper;
    this.view = view;
    this.refreshIntervalS = refreshIntervalS;
    this.http = HttpServer.create(new InetSocketAddress(port), 0);
    http.createContext("/api/nodes", ex -> send(ex, 200, "application/json", apiNodes()));
    http.createContext("/dashboard", ex -> send(ex, 200, "text/html; charset=utf-8", dashboard()));
    http.createContext("/status", ex -> send(ex, 200, "text/plain", "ok\n"));
    http.createContext(
        "/metrics", ex -> send(ex, 200, "text/plain", "cryptopanner_monitor_up 1\n"));
    http.setExecutor(null);
    http.start();
  }

  public int port() {
    return http.getAddress().getPort();
  }

  private String apiNodes() {
    try {
      return mapper.writeValueAsString(view.get());
    } catch (Exception e) {
      return "[]";
    }
  }

  private String dashboard() {
    List<NodeView> nodes = view.get();
    StringBuilder b = new StringBuilder();
    b.append("<!doctype html><html><head><meta charset=\"utf-8\">")
        .append("<meta http-equiv=\"refresh\" content=\"")
        .append(refreshIntervalS)
        .append("\"><title>CryptoPanner Monitor</title></head><body>")
        .append("<h1>CryptoPanner Monitor</h1>");

    List<NodeView> attention = nodes.stream().filter(NodeView::needsAttention).toList();
    if (!attention.isEmpty()) {
      b.append("<section style=\"background:#fee\"><h2>Needs attention</h2><ul>");
      for (NodeView n : attention) {
        b.append("<li>")
            .append(esc(n.id()))
            .append(" — ")
            .append(!n.reachable() ? "UNREACHABLE (" + esc(n.error()) + ")" : "")
            .append(n.circuitBreakerTripped() ? " CIRCUIT-BREAKER-TRIPPED" : "")
            .append("</li>");
      }
      b.append("</ul></section>");
    }

    for (NodeView n : nodes) {
      b.append("<div class=\"card\" style=\"border:1px solid #ccc;margin:8px;padding:8px\">");
      b.append("<h3>").append(esc(n.id())).append("</h3>");
      if (!n.reachable()) {
        b.append("<p><b>UNREACHABLE</b>: ").append(esc(n.error())).append("</p>");
      } else {
        StatusSnapshot s = n.status();
        if (n.activityInProgress()) {
          b.append("<p class=\"banner\" style=\"background:#eef\">deploy=")
              .append(esc(s.deployState()))
              .append(" rotation=")
              .append(esc(s.rotationState()))
              .append("</p>");
        }
        b.append("<p>active_slot=").append(esc(s.activeSlot())).append("</p><ul>");
        for (var e : s.components().entrySet()) {
          b.append("<li>")
              .append(esc(e.getKey()))
              .append(": ")
              .append(esc(e.getValue().state()))
              .append("</li>");
        }
        b.append("</ul>");
        if (!n.activeAlerts().isEmpty()) {
          b.append("<p>alerts: ").append(esc(String.join(", ", n.activeAlerts()))).append("</p>");
        }
      }
      b.append("</div>");
    }
    b.append("</body></html>");
    return b.toString();
  }

  private static String esc(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  private static void send(HttpExchange ex, int status, String contentType, String body)
      throws IOException {
    byte[] out = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", contentType);
    ex.sendResponseHeaders(status, out.length);
    try (var os = ex.getResponseBody()) {
      os.write(out);
    }
  }

  @Override
  public void close() {
    http.stop(0);
  }
}
