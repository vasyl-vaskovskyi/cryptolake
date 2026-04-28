package com.cryptolake.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that COLLECTOR_ID and TOPIC_PREFIX env vars override YAML values in BinanceExchangeConfig.
 */
class BackupConfigOverrideTest {

  private Path configPath;

  @BeforeEach
  void setUp() throws URISyntaxException {
    URL url = getClass().getResource("/fixtures/config_valid.yaml");
    assertThat(url).isNotNull();
    configPath = Path.of(url.toURI());
    YamlConfigLoader.envSupplier = System::getenv;
  }

  @AfterEach
  void tearDown() {
    YamlConfigLoader.envSupplier = System::getenv;
  }

  @Test
  void collectorIdEnvVarOverridesYaml() {
    // Inject COLLECTOR_ID into the env supplier
    Map<String, String> fakeEnv = new HashMap<>();
    fakeEnv.put("COLLECTOR_ID", "foo");
    YamlConfigLoader.envSupplier = () -> fakeEnv;

    AppConfig config = YamlConfigLoader.load(configPath);
    assertThat(config.exchanges().binance().collectorId()).isEqualTo("foo");
  }

  @Test
  void topicPrefixEnvVarOverridesYaml() {
    // Inject TOPIC_PREFIX into the env supplier
    Map<String, String> fakeEnv = new HashMap<>();
    fakeEnv.put("TOPIC_PREFIX", "backup.");
    YamlConfigLoader.envSupplier = () -> fakeEnv;

    AppConfig config = YamlConfigLoader.load(configPath);
    assertThat(config.exchanges().binance().topicPrefix()).isEqualTo("backup.");
  }

  @Test
  void absentEnvVarsUseYamlValues() {
    // No COLLECTOR_ID / TOPIC_PREFIX in env — YAML values (or defaults) should be used
    YamlConfigLoader.envSupplier = () -> Map.of();

    AppConfig config = YamlConfigLoader.load(configPath);
    // Default collectorId from BinanceExchangeConfig compact constructor
    assertThat(config.exchanges().binance().collectorId()).isNotBlank();
    // Default topicPrefix is ""
    assertThat(config.exchanges().binance().topicPrefix()).isEqualTo("");
  }
}
