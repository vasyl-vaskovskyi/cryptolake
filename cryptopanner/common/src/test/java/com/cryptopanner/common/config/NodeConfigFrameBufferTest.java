package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeConfigFrameBufferTest {

  private NodeConfig load(String yaml, @TempDir Path dir) throws Exception {
    Path f = dir.resolve("config.yaml");
    Files.writeString(f, yaml);
    return NodeConfig.load(f);
  }

  @Test
  void frameBufferWindowDefaultsTo5Seconds(@TempDir Path dir) throws Exception {
    String yaml =
        """
        node_id: test-node
        symbols: [btcusdt]
        streams:
          per_symbol: [trade, depth@100ms]
        paths:
          segments: /tmp/segments
          sealed:   /tmp/sealed
        storage:
          endpoint: http://localhost:9000
          bucket:   test-bucket
        collector:
          ws_public_endpoint_url: ws://localhost/public
          ws_market_endpoint_url: ws://localhost/market
          seal_grace_window: 10s
        """;
    NodeConfig cfg = load(yaml, dir);
    assertEquals(5, cfg.frameBufferSeconds());
  }

  @Test
  void frameBufferWindowCanBeOverriddenInConfig(@TempDir Path dir) throws Exception {
    String yaml =
        """
        node_id: test-node
        symbols: [btcusdt]
        streams:
          per_symbol: [trade, depth@100ms]
        paths:
          segments: /tmp/segments
          sealed:   /tmp/sealed
        storage:
          endpoint: http://localhost:9000
          bucket:   test-bucket
        collector:
          ws_public_endpoint_url: ws://localhost/public
          ws_market_endpoint_url: ws://localhost/market
          seal_grace_window: 10s
          frame_buffer_window: 15s
        """;
    NodeConfig cfg = load(yaml, dir);
    assertEquals(15, cfg.frameBufferSeconds());
  }
}
