package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeConfigTest {

  /** A representative §15.b node config (two symbols) plus the dev overlay. */
  private static final String YAML =
      """
      node_id: vps-fra-1

      symbols:
        - btcusdt
        - ethusdt

      streams:
        per_symbol:
          - trade
          - depth@100ms
          - aggTrade
        all_symbol:
          - "!forceOrder@arr"

      paths:
        segments:      /data/cryptopanner/segments
        sealed:        /data/cryptopanner/sealed
        staging:       /data/cryptopanner/staging
        deploy:        /data/cryptopanner/deploy
        logs:          /data/cryptopanner/logs
        fs_heavy_lock: /data/cryptopanner/.fs-heavy.lock

      storage:
        endpoint:          http://localhost:9000
        bucket:            cryptopanner-test
        access_key:        cryptopanner
        secret_key:        changeme-dev
        region:            us-east-1
        path_style_access: true

      collector:
        ws_public_endpoint_url: wss://fstream.binance.com/public/stream
        ws_market_endpoint_url: wss://fstream.binance.com/market/stream
        rest_base_url:          https://fapi.binance.com
        rest_connect_timeout_s: 5
        rest_request_timeout_s: 30
        unplanned_reconnect_backoff_max_s: 60
        unplanned_reconnect_jitter_pct:    25
        subscribe_ack_timeout_s:           10
        seal_grace_window:  10s
        connection_max_age: 23h
        rotation_window:    "HH:10-HH:50"
        rest:
          depth:
            url_template:           "/fapi/v1/depth?symbol={symbol}&limit=1000"
            baseline_poll_interval: 5m
            on_demand_resync:       true
          open_interest:
            url_template:  "/fapi/v1/openInterest?symbol={symbol}"
            poll_interval: 60s
          exchange_info:
            url_template:  "/fapi/v1/exchangeInfo"
            poll_time_utc: "00:05"

      sealer:
        hour_grace_window: 120s
        backfill:
          attempts_per_gap: 3
          backoff:           [1s, 5s, 30s]
          cross_merge_retry: false

      uploader:
        retry_backoff_max_s: 300

      deploy:
        forbidden_window:          "HH:50-HH:15"
        recommended_window:        "HH:15-HH:45"
        superseded_retention_days: 7
        versions_kept:             2

      agent:
        listen_address:  100.1.2.3:9100
        token_file:      /etc/cryptopanner/agent.token
        test_mode:       false
        metrics_enabled: true
        disk_mounts:
          - /
          - /data
        heartbeat:
          degraded_threshold_s: 15
          stuck_threshold_s:    60

      dev:
        health_port:            8088
        collector_max_runtime_s: 120
      """;

  private NodeConfig load(@TempDir Path dir) throws Exception {
    Path f = dir.resolve("config.yaml");
    Files.writeString(f, YAML);
    return NodeConfig.load(f);
  }

  @Test
  void loadsIdentityAndSymbols(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    assertEquals("vps-fra-1", cfg.nodeId());
    assertEquals(List.of("btcusdt", "ethusdt"), cfg.symbols());
  }

  @Test
  void expandsSymbolsTimesPerSymbolStreamsIntoSubscriptions(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    // 2 symbols × 3 per-symbol streams = 6 explicit WS subscriptions.
    assertEquals(6, cfg.subscriptions().size());
    assertTrue(cfg.subscriptions().contains(new NodeConfig.Subscription("btcusdt", "trade")));
    assertTrue(cfg.subscriptions().contains(new NodeConfig.Subscription("ethusdt", "depth@100ms")));
    assertEquals(List.of("!forceOrder@arr"), cfg.broadcasts());
  }

  @Test
  void derivesRestPollsFromNamedEndpoints(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    // depthSnapshot: per-symbol, endpoint without query, limit kept as a static param, 5m cadence.
    NodeConfig.RestPoll depth =
        cfg.restPolls().stream()
            .filter(p -> p.stream().equals("depthSnapshot"))
            .findFirst()
            .orElseThrow();
    assertEquals("/fapi/v1/depth", depth.endpoint());
    assertTrue(depth.perSymbol());
    assertEquals("1000", depth.params().get("limit"));
    assertFalse(
        depth.params().containsKey("symbol"),
        "{symbol} placeholder must not become a static param");
    assertEquals(300, depth.cadenceSeconds());

    NodeConfig.RestPoll oi =
        cfg.restPolls().stream()
            .filter(p -> p.stream().equals("openInterest"))
            .findFirst()
            .orElseThrow();
    assertEquals("/fapi/v1/openInterest", oi.endpoint());
    assertTrue(oi.perSymbol());
    assertEquals(60, oi.cadenceSeconds());

    NodeConfig.RestPoll xi =
        cfg.restPolls().stream()
            .filter(p -> p.stream().equals("exchangeInfo"))
            .findFirst()
            .orElseThrow();
    assertEquals("/fapi/v1/exchangeInfo", xi.endpoint());
    assertFalse(xi.perSymbol(), "exchangeInfo is exchange-wide");
  }

  @Test
  void effectiveSubscriptionsFansOutBroadcastsAndRestPolls(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    List<NodeConfig.Subscription> eff = cfg.effectiveSubscriptions();
    // forceOrder fanned per symbol.
    assertTrue(eff.contains(new NodeConfig.Subscription("btcusdt", "forceOrder")));
    assertTrue(eff.contains(new NodeConfig.Subscription("ethusdt", "forceOrder")));
    // per-symbol REST polls fanned per symbol; global poll under the reserved global symbol.
    assertTrue(eff.contains(new NodeConfig.Subscription("btcusdt", "openInterest")));
    assertTrue(eff.contains(new NodeConfig.Subscription(NodeConfig.GLOBAL_SYMBOL, "exchangeInfo")));
  }

  @Test
  void exposesPathsStorageAndCollectorScalars(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    assertEquals(Path.of("/data/cryptopanner/segments"), cfg.paths().segments());
    assertEquals(Path.of("/data/cryptopanner/sealed"), cfg.paths().sealed());
    assertEquals("cryptopanner-test", cfg.storage().bucket());
    assertEquals("cryptopanner", cfg.storage().accessKey());
    assertTrue(cfg.storage().pathStyleAccess());
    assertEquals("wss://fstream.binance.com/public/stream", cfg.wsPublicEndpointUrl());
    assertEquals("wss://fstream.binance.com/market/stream", cfg.wsMarketEndpointUrl());
    assertEquals("https://fapi.binance.com", cfg.restBaseUrl());
    // seal_grace_window "10s" → 10 seconds.
    assertEquals(10, cfg.sealGraceSeconds());
    assertEquals(java.time.Duration.ofHours(23), cfg.collector().connectionMaxAgeDuration());
  }

  @Test
  void exposesDevOverlayKnobs(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir);
    assertEquals(8088, cfg.healthPort());
    assertEquals(120, cfg.collectorMaxRuntimeS());
  }
}
