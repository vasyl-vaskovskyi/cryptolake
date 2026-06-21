package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeConfigValidationTest {

  /** A minimal-but-valid §15.b config; individual tests mutate one thing to make it invalid. */
  private static String validYaml() {
    return """
        node_id: vps-fra-1
        symbols: [btcusdt]
        streams:
          per_symbol: [trade]
        paths:
          segments: /data/cryptopanner/segments
          sealed:   /data/cryptopanner/sealed
        storage:
          endpoint: http://localhost:9000
          bucket:   cryptopanner-test
        collector:
          ws_public_endpoint_url: wss://x/public/stream
          ws_market_endpoint_url: wss://x/market/stream
          rest_base_url:          https://fapi.binance.com
          seal_grace_window:  10s
          connection_max_age: 23h
          rotation_window:    "HH:10-HH:50"
        deploy:
          forbidden_window:   "HH:50-HH:15"
          recommended_window: "HH:15-HH:45"
        """;
  }

  private NodeConfig load(Path dir, String yaml) throws Exception {
    Path f = dir.resolve("config.yaml");
    Files.writeString(f, yaml);
    return NodeConfig.load(f);
  }

  @Test
  void validConfigLoadsWithoutError(@TempDir Path dir) {
    assertDoesNotThrow(() -> load(dir, validYaml()));
  }

  @Test
  void rejectsMissingNodeId(@TempDir Path dir) {
    String yaml = validYaml().replace("node_id: vps-fra-1\n", "");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(e.getMessage().contains("node_id"), e.getMessage());
  }

  @Test
  void rejectsEmptySymbols(@TempDir Path dir) {
    String yaml = validYaml().replace("symbols: [btcusdt]", "symbols: []");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(e.getMessage().contains("symbols"), e.getMessage());
  }

  @Test
  void rejectsMissingSegmentsPath(@TempDir Path dir) {
    String yaml = validYaml().replace("  segments: /data/cryptopanner/segments\n", "");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(e.getMessage().contains("paths.segments"), e.getMessage());
  }

  @Test
  void rejectsMissingStorageBucket(@TempDir Path dir) {
    String yaml = validYaml().replace("  bucket:   cryptopanner-test\n", "");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(e.getMessage().contains("storage.bucket"), e.getMessage());
  }

  @Test
  void rejectsMalformedDurationNamingTheKey(@TempDir Path dir) {
    String yaml = validYaml().replace("seal_grace_window:  10s", "seal_grace_window:  10x");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(e.getMessage().contains("seal_grace_window"), e.getMessage());
  }

  @Test
  void rejectsRecommendedWindowOverlappingForbidden(@TempDir Path dir) {
    // recommended HH:05-HH:45 overlaps forbidden HH:50-HH:15 at minutes 05..14 → contradiction.
    String yaml =
        validYaml()
            .replace("recommended_window: \"HH:15-HH:45\"", "recommended_window: \"HH:05-HH:45\"");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> load(dir, yaml));
    assertTrue(
        e.getMessage().contains("recommended_window")
            && e.getMessage().contains("forbidden_window"),
        e.getMessage());
  }
}
