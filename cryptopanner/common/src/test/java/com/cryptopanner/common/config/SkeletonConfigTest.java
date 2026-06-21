package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkeletonConfigTest {

  @Test
  void parsesAllFields(@TempDir Path tmp) throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: vps-fra-1
        subscriptions:
          - symbol: btcusdt
            stream: trade
          - symbol: ethusdt
            stream: aggTrade
        ws_public_endpoint_url: ws://mock-binance-ws:9001/public/stream
        ws_market_endpoint_url: ws://mock-binance-ws:9001/market/stream
        paths:
          segments: /data/cryptopanner/segments
          sealed:   /data/cryptopanner/sealed
        collector_max_runtime_s: 120
        storage:
          endpoint:          http://minio:9000
          bucket:            cryptopanner-prod
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    SkeletonConfig cfg = SkeletonConfig.load(yaml);

    assertEquals("vps-fra-1", cfg.nodeId());
    assertEquals(2, cfg.subscriptions().size());
    assertEquals("btcusdt", cfg.subscriptions().get(0).symbol());
    assertEquals("trade", cfg.subscriptions().get(0).stream());
    assertEquals("ethusdt", cfg.subscriptions().get(1).symbol());
    assertEquals("aggTrade", cfg.subscriptions().get(1).stream());
    assertEquals("ws://mock-binance-ws:9001/public/stream", cfg.wsPublicEndpointUrl());
    assertEquals("ws://mock-binance-ws:9001/market/stream", cfg.wsMarketEndpointUrl());
    assertEquals(Path.of("/data/cryptopanner/segments"), cfg.paths().segments());
    assertEquals(Path.of("/data/cryptopanner/sealed"), cfg.paths().sealed());
    assertEquals(120, cfg.collectorMaxRuntimeS());
    assertEquals(10, cfg.sealGraceSeconds(), "seal_grace_seconds defaults to 10 when absent");
    assertEquals("http://minio:9000", cfg.storage().endpoint());
    assertEquals("cryptopanner-prod", cfg.storage().bucket());
    assertEquals(true, cfg.storage().pathStyleAccess());
  }

  @Test
  void nonPerSymbolPollAppearsUnderGlobalSymbolWithParsedParams(@TempDir Path tmp)
      throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: vps-fra-1
        subscriptions:
          - symbol: btcusdt
            stream: trade
        rest_base_url: https://fapi.binance.com
        rest_polls:
          - stream: depth
            endpoint: /fapi/v1/depth
            cadence_seconds: 300
            per_symbol: true
            params:
              limit: "1000"
          - stream: exchangeInfo
            endpoint: /fapi/v1/exchangeInfo
            cadence_seconds: 86400
            per_symbol: false
        ws_public_endpoint_url: ws://x/public
        ws_market_endpoint_url: ws://x/market
        paths:
          segments: /seg
          sealed:   /sealed
        collector_max_runtime_s: 60
        storage:
          endpoint:          http://minio:9000
          bucket:            b
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    SkeletonConfig cfg = SkeletonConfig.load(yaml);

    // depth poll carries its extra query params.
    assertEquals("1000", cfg.restPolls().get(0).params().get("limit"));
    // exchangeInfo (non-per-symbol) is bucketed under the reserved global symbol.
    assertTrue(
        cfg.effectiveSubscriptions()
            .contains(
                new SkeletonConfig.Subscription(SkeletonConfig.GLOBAL_SYMBOL, "exchangeInfo")),
        "non-per-symbol poll should appear under the global symbol");
    // per-symbol depth poll still fans out per symbol.
    assertTrue(
        cfg.effectiveSubscriptions().contains(new SkeletonConfig.Subscription("btcusdt", "depth")));
  }

  @Test
  void sealGraceSecondsParsedWhenPresent(@TempDir Path tmp) throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: vps-fra-1
        subscriptions:
          - symbol: btcusdt
            stream: trade
        ws_public_endpoint_url: ws://x/public
        ws_market_endpoint_url: ws://x/market
        seal_grace_seconds: 25
        paths:
          segments: /seg
          sealed:   /sealed
        collector_max_runtime_s: 60
        storage:
          endpoint:          http://minio:9000
          bucket:            b
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    assertEquals(25, SkeletonConfig.load(yaml).sealGraceSeconds());
  }

  @Test
  void parsesBroadcastsAndRestPolls(@TempDir Path tmp) throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: vps-fra-1
        subscriptions:
          - symbol: btcusdt
            stream: trade
        broadcasts:
          - "!forceOrder@arr"
        rest_base_url: http://mock-rest:9101
        rest_api_key: test-key
        rest_polls:
          - stream: openInterest
            endpoint: /fapi/v1/openInterest
            cadence_seconds: 60
            per_symbol: true
          - stream: exchangeInfo
            endpoint: /fapi/v1/exchangeInfo
            cadence_seconds: 3600
            per_symbol: false
        ws_public_endpoint_url: ws://x/public
        ws_market_endpoint_url: ws://x/market
        paths:
          segments: /seg
          sealed:   /sealed
        collector_max_runtime_s: 60
        storage:
          endpoint:          http://minio:9000
          bucket:            b
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    SkeletonConfig cfg = SkeletonConfig.load(yaml);

    assertEquals(List.of("!forceOrder@arr"), cfg.broadcasts());
    assertEquals("http://mock-rest:9101", cfg.restBaseUrl());
    assertEquals("test-key", cfg.restApiKey());
    assertEquals(2, cfg.restPolls().size());

    SkeletonConfig.RestPoll oi = cfg.restPolls().get(0);
    assertEquals("openInterest", oi.stream());
    assertEquals("/fapi/v1/openInterest", oi.endpoint());
    assertEquals(60, oi.cadenceSeconds());
    assertTrue(oi.perSymbol());

    SkeletonConfig.RestPoll ei = cfg.restPolls().get(1);
    assertEquals("exchangeInfo", ei.stream());
    assertEquals(3600, ei.cadenceSeconds());
    assertFalse(ei.perSymbol());
  }

  @Test
  void missingBroadcastsAndRestPollsDefaultToEmpty(@TempDir Path tmp) throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: x
        subscriptions:
          - symbol: btcusdt
            stream: trade
        ws_public_endpoint_url: ws://x/public
        ws_market_endpoint_url: ws://x/market
        paths:
          segments: /seg
          sealed:   /sealed
        collector_max_runtime_s: 60
        storage:
          endpoint:          http://minio:9000
          bucket:            b
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    SkeletonConfig cfg = SkeletonConfig.load(yaml);

    assertEquals(List.of(), cfg.broadcasts());
    assertEquals(List.of(), cfg.restPolls());
  }

  @Test
  void symbolsReturnsDistinctSymbolsInInsertionOrder() {
    SkeletonConfig cfg =
        configWith(
            List.of(
                new SkeletonConfig.Subscription("btcusdt", "trade"),
                new SkeletonConfig.Subscription("ethusdt", "trade"),
                new SkeletonConfig.Subscription("btcusdt", "aggTrade")),
            List.of(),
            List.of());

    assertEquals(List.of("btcusdt", "ethusdt"), new ArrayList<>(cfg.symbols()));
  }

  @Test
  void effectiveSubscriptionsWithExplicitOnly() {
    SkeletonConfig cfg =
        configWith(
            List.of(
                new SkeletonConfig.Subscription("btcusdt", "trade"),
                new SkeletonConfig.Subscription("ethusdt", "aggTrade")),
            List.of(),
            List.of());

    assertEquals(2, cfg.effectiveSubscriptions().size());
  }

  @Test
  void effectiveSubscriptionsFansBroadcastsAcrossSymbols() {
    SkeletonConfig cfg =
        configWith(
            List.of(
                new SkeletonConfig.Subscription("btcusdt", "trade"),
                new SkeletonConfig.Subscription("ethusdt", "trade")),
            List.of("!forceOrder@arr"),
            List.of());

    List<SkeletonConfig.Subscription> eff = cfg.effectiveSubscriptions();
    assertEquals(4, eff.size());
    assertTrue(eff.contains(new SkeletonConfig.Subscription("btcusdt", "forceOrder")));
    assertTrue(eff.contains(new SkeletonConfig.Subscription("ethusdt", "forceOrder")));
  }

  @Test
  void effectiveSubscriptionsFansPerSymbolRestPollsAcrossSymbols() {
    SkeletonConfig cfg =
        configWith(
            List.of(
                new SkeletonConfig.Subscription("btcusdt", "trade"),
                new SkeletonConfig.Subscription("ethusdt", "trade")),
            List.of(),
            List.of(new SkeletonConfig.RestPoll("openInterest", "/oi", 60, true, Map.of())));

    List<SkeletonConfig.Subscription> eff = cfg.effectiveSubscriptions();
    assertEquals(4, eff.size());
    assertTrue(eff.contains(new SkeletonConfig.Subscription("btcusdt", "openInterest")));
    assertTrue(eff.contains(new SkeletonConfig.Subscription("ethusdt", "openInterest")));
  }

  @Test
  void effectiveSubscriptionsPlacesNonPerSymbolRestPollsUnderGlobalSymbol() {
    SkeletonConfig cfg =
        configWith(
            List.of(new SkeletonConfig.Subscription("btcusdt", "trade")),
            List.of(),
            List.of(new SkeletonConfig.RestPoll("exchangeInfo", "/ei", 3600, false, Map.of())));

    List<SkeletonConfig.Subscription> eff = cfg.effectiveSubscriptions();
    assertEquals(2, eff.size());
    assertTrue(
        eff.contains(
            new SkeletonConfig.Subscription(SkeletonConfig.GLOBAL_SYMBOL, "exchangeInfo")));
  }

  @Test
  void effectiveSubscriptionsThrowsOnUnknownBroadcast() {
    SkeletonConfig cfg =
        configWith(
            List.of(new SkeletonConfig.Subscription("btcusdt", "trade")),
            List.of("!unknownStream@arr"),
            List.of());

    assertThrows(IllegalArgumentException.class, cfg::effectiveSubscriptions);
  }

  private static SkeletonConfig configWith(
      List<SkeletonConfig.Subscription> subs,
      List<String> broadcasts,
      List<SkeletonConfig.RestPoll> restPolls) {
    return new SkeletonConfig(
        "n",
        subs,
        broadcasts,
        "u",
        "k",
        restPolls,
        "ws://p",
        "ws://m",
        new SkeletonConfig.Paths(Path.of("/seg"), Path.of("/sealed")),
        60,
        10,
        new SkeletonConfig.Storage("u", "b", "a", "s", "r", true));
  }
}
