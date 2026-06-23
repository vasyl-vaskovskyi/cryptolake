package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** §11.d/§13 — composition-root smoke: Main wires the stack and serves the dashboard. */
class MainTest {

  private static final String YAML =
      """
      nodes: []
      scrape_interval_s: 1
      dashboard:
        listen_address:     127.0.0.1:0
        refresh_interval_s: 5
      restart:
        backoff: [5s, 15s, 60s, 300s]
        circuit_breaker:
          failure_count: 3
          window:        5m
      alert:
        dedup_ttl: 1h
        correlation:
          threshold_nodes: 3
          window:          1m
        warning:
          degraded_persisting:       2m
          upload_backlog_age:        30m
          deploy_stuck:              1h
          rest_failed_poll_rate_pct: 10
          rest_failed_poll_window:   10m
          disk_data_pct:             80
          clock_skew_s:              1
        critical:
          extended_ws_disconnect:            5m
          rest_rate_limit_persistence_hours: 2
          disk_data_pct:                     95
          clock_skew_s:                      5
      alerting:
        telegram:
          webhook_url: ""
        whatsapp:
          webhook_url: ""
        healthchecks_url: ""
      dead_man:
        healthchecks_push_interval: 60s
        self_test_time_utc:         "02:00"
      """;

  @Test
  void startsAndServesDashboard(@TempDir Path dir) throws Exception {
    Path cfg = dir.resolve("monitor.yaml");
    Files.writeString(cfg, YAML);

    try (Main.App app = Main.start(com.cryptopanner.common.config.MonitorConfig.load(cfg))) {
      HttpResponse<String> r =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(
                          URI.create("http://127.0.0.1:" + app.server().port() + "/dashboard"))
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.ofString());
      assertEquals(200, r.statusCode());
      assertTrue(r.body().contains("CryptoPanner Monitor"), r.body());
    }
  }
}
