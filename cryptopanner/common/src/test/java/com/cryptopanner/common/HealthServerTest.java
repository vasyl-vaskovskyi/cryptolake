package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class HealthServerTest {

  private static HttpResponse<String> get(int port, String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void statusReturns200Ok() throws Exception {
    try (HealthServer server = new HealthServer(0, () -> "")) {
      HttpResponse<String> resp = get(server.port(), "/status");
      assertEquals(200, resp.statusCode());
      assertEquals("ok", resp.body().strip());
    }
  }

  @Test
  void metricsReflectsLiveSupplierValue() throws Exception {
    AtomicLong frames = new AtomicLong(0);
    try (HealthServer server =
        new HealthServer(0, () -> "cryptopanner_frames_written_total " + frames.get())) {
      frames.set(42);
      HttpResponse<String> resp = get(server.port(), "/metrics");
      assertEquals(200, resp.statusCode());
      assertTrue(
          resp.body().contains("cryptopanner_frames_written_total 42"),
          "metrics should reflect the live supplier, got: " + resp.body());
    }
  }
}
