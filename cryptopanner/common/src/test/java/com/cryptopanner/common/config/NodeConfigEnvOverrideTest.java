package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeConfigEnvOverrideTest {

  private static final String YAML =
      """
      node_id: vps-fra-1
      symbols: [btcusdt]
      streams:
        per_symbol: [trade]
      paths:
        segments: /data/cryptopanner/segments
        sealed:   /data/cryptopanner/sealed
      storage:
        endpoint:   http://localhost:9000
        bucket:     cryptopanner-test
        access_key: placeholder
        secret_key: placeholder
      collector:
        ws_public_endpoint_url: wss://x/public/stream
        ws_market_endpoint_url: wss://x/market/stream
        rest_base_url:          https://fapi.binance.com
        seal_grace_window:      10s
      """;

  private NodeConfig load(Path dir, Map<String, String> env) throws Exception {
    Path f = dir.resolve("config.yaml");
    Files.writeString(f, YAML);
    return NodeConfig.load(f, env);
  }

  @Test
  void overridesLeafKeyFromEnv(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir, Map.of("CRYPTOPANNER_STORAGE_SECRET_KEY", "supersecret-from-env"));
    assertEquals("supersecret-from-env", cfg.storage().secretKey());
    assertEquals("placeholder", cfg.storage().accessKey(), "untouched keys keep their YAML value");
  }

  @Test
  void overridesNestedScalarFromEnv(@TempDir Path dir) throws Exception {
    NodeConfig cfg =
        load(
            dir,
            Map.of(
                "CRYPTOPANNER_COLLECTOR_REST_BASE_URL", "http://mock-binance:8080",
                "CRYPTOPANNER_COLLECTOR_SEAL_GRACE_WINDOW", "20s"));
    assertEquals("http://mock-binance:8080", cfg.restBaseUrl());
    assertEquals(20, cfg.sealGraceSeconds(), "overridden duration string is re-parsed");
  }

  @Test
  void noEnvLeavesYamlValuesIntact(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir, Map.of());
    assertEquals("placeholder", cfg.storage().secretKey());
    assertEquals("https://fapi.binance.com", cfg.restBaseUrl());
  }

  @Test
  void unrelatedEnvVarsAreIgnored(@TempDir Path dir) throws Exception {
    NodeConfig cfg = load(dir, Map.of("PATH", "/usr/bin", "CRYPTOPANNER_NOT_A_REAL_KEY", "x"));
    assertEquals("placeholder", cfg.storage().secretKey());
  }
}
