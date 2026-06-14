package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        symbol:  btcusdt
        stream:  trade
        ws_endpoint_url: ws://mock-binance-ws:9001/ws
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
    assertEquals("btcusdt", cfg.symbol());
    assertEquals("trade", cfg.stream());
    assertEquals("ws://mock-binance-ws:9001/ws", cfg.wsEndpointUrl());
    assertEquals(Path.of("/data/cryptopanner/segments"), cfg.paths().segments());
    assertEquals(Path.of("/data/cryptopanner/sealed"), cfg.paths().sealed());
    assertEquals(120, cfg.collectorMaxRuntimeS());
    assertEquals("http://minio:9000", cfg.storage().endpoint());
    assertEquals("cryptopanner-prod", cfg.storage().bucket());
    assertEquals(true, cfg.storage().pathStyleAccess());
  }
}
