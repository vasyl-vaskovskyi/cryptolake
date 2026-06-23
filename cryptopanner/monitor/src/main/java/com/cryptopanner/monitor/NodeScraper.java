package com.cryptopanner.monitor;

import com.cryptopanner.common.config.MonitorConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Polls one node's read-only Node Agent endpoints — {@code GET /status} and {@code GET /metrics}
 * (master spec §11.c) — over HTTP and returns a {@link ScrapeResult}. Read-only endpoints need no
 * bearer auth (§6.f), so the scraper carries no token. Any transport error, non-2xx response, or
 * parse failure becomes a {@code failure} result rather than an exception, so the scrape loop keeps
 * its cadence and the {@link NodeStateTracker} can count the miss.
 */
public final class NodeScraper {

  private final HttpClient http;
  private final ObjectMapper mapper;

  public NodeScraper(HttpClient http, ObjectMapper mapper) {
    this.http = http;
    this.mapper = mapper;
  }

  /** HTTP/1.1 client with the project's standard timeouts (mirrors {@code RestPoller}). */
  public static HttpClient newHttpClient() {
    return HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(5))
        .build();
  }

  public ScrapeResult poll(MonitorConfig.Node node) {
    String base = "http://" + node.endpoint();
    try {
      String statusBody = get(base + "/status");
      String metricsBody = get(base + "/metrics");
      StatusSnapshot status = StatusSnapshot.parse(statusBody, mapper);
      MetricsSnapshot metrics = MetricsSnapshot.parse(metricsBody);
      return ScrapeResult.success(status, metrics);
    } catch (Exception e) {
      return ScrapeResult.failure(e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  private String get(String url) throws Exception {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      throw new IllegalStateException("HTTP " + resp.statusCode() + " from " + url);
    }
    return resp.body();
  }
}
