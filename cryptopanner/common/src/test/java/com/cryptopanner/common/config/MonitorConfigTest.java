package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MonitorConfigTest {

  private static final String YAML =
      """
      nodes:
        - id:         vps-fra-1
          endpoint:   100.1.2.3:9100
          token_file: /etc/cryptopanner-monitor/tokens/vps-fra-1
        - id:         vps-tyo-1
          endpoint:   100.4.5.6:9100
          token_file: /etc/cryptopanner-monitor/tokens/vps-tyo-1

      scrape_interval_s: 5

      dashboard:
        listen_address:     100.7.8.9:9200
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
          degraded_persisting: 2m
          disk_data_pct:       80
          clock_skew_s:        1
        critical:
          extended_ws_disconnect: 5m
          disk_data_pct:          95
          clock_skew_s:           5

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

  private MonitorConfig load(@TempDir Path dir) throws Exception {
    Path f = dir.resolve("monitor.yaml");
    Files.writeString(f, YAML);
    return MonitorConfig.load(f);
  }

  @Test
  void loadsNodesAndScrapeInterval(@TempDir Path dir) throws Exception {
    MonitorConfig cfg = load(dir);
    assertEquals(2, cfg.nodes().size());
    assertEquals("vps-fra-1", cfg.nodes().get(0).id());
    assertEquals("100.4.5.6:9100", cfg.nodes().get(1).endpoint());
    assertEquals(5, cfg.scrapeIntervalS());
  }

  @Test
  void loadsDashboardAndRestartPolicy(@TempDir Path dir) throws Exception {
    MonitorConfig cfg = load(dir);
    assertEquals("100.7.8.9:9200", cfg.dashboard().listenAddress());
    assertEquals(List.of("5s", "15s", "60s", "300s"), cfg.restart().backoff());
    assertEquals(3, cfg.restart().circuitBreaker().failureCount());
    assertEquals(java.time.Duration.ofMinutes(5), cfg.restart().circuitBreaker().windowDuration());
  }

  @Test
  void loadsAlertThresholdsAndChannels(@TempDir Path dir) throws Exception {
    MonitorConfig cfg = load(dir);
    assertEquals(java.time.Duration.ofHours(1), cfg.alert().dedupTtlDuration());
    assertEquals(3, cfg.alert().correlation().thresholdNodes());
    assertEquals(80, cfg.alert().warning().diskDataPct());
    assertEquals(95, cfg.alert().critical().diskDataPct());
    assertEquals("02:00", cfg.deadMan().selfTestTimeUtc());
  }
}
