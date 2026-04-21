// ports: tests/unit/test_config.py::TestConfigLoading::test_load_valid_config
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_all_streams_enabled
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_depth_snapshot_override
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_writer_defaults
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_retention_minimum_rejected
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_env_override
// ports: tests/unit/test_config.py::TestConfigLoading::test_host_data_dir_alias_overrides_writer_base_dir
// ports: tests/unit/test_config.py::TestConfigLoading::test_config_missing_file_raises
// ports: tests/unit/test_config.py::TestConfigLoading::test_env_override_does_not_bleed_os_environ
package com.cryptolake.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class YamlConfigLoaderTest {

  private Path fixturesDir;

  @BeforeEach
  void setUp() throws URISyntaxException {
    URL url = getClass().getResource("/fixtures/config_valid.yaml");
    assertThat(url).isNotNull();
    fixturesDir = Path.of(url.toURI()).getParent();
    // Reset the env supplier to system default before each test
    YamlConfigLoader.envSupplier = System::getenv;
  }

  @AfterEach
  void tearDown() {
    // Restore the supplier after each test
    YamlConfigLoader.envSupplier = System::getenv;
  }

  @Test
  void loadValidConfig() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_load_valid_config
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), Map.of());
    assertThat(config.exchanges().binance().enabled()).isTrue();
    assertThat(config.exchanges().binance().symbols()).containsExactly("btcusdt", "ethusdt",
        "solusdt");
    assertThat(config.redpanda().retentionHours()).isEqualTo(48);
  }

  @Test
  void allStreamsEnabled() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_all_streams_enabled
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), Map.of());
    StreamsConfig streams = config.exchanges().binance().streams();
    assertThat(streams.trades()).isTrue();
    assertThat(streams.depth()).isTrue();
    assertThat(streams.bookticker()).isTrue();
    assertThat(streams.fundingRate()).isTrue();
    assertThat(streams.liquidations()).isTrue();
    assertThat(streams.openInterest()).isTrue();
  }

  @Test
  void depthSnapshotOverride() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_depth_snapshot_override
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), Map.of());
    assertThat(config.exchanges().binance().depth().snapshotOverrides()).containsEntry("btcusdt",
        "1m");
    assertThat(config.exchanges().binance().depth().snapshotInterval()).isEqualTo("5m");
  }

  @Test
  void writerDefaultsApplied() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_writer_defaults
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), Map.of());
    assertThat(config.writer().compression()).isEqualTo("zstd");
    assertThat(config.writer().compressionLevel()).isEqualTo(3);
    assertThat(config.writer().rotation()).isEqualTo("hourly");
  }

  @Test
  void retentionBelowMinimumRejected() throws IOException {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_retention_minimum_rejected
    Path tmp = Files.createTempFile("bad_config", ".yaml");
    Files.writeString(
        tmp,
        "database:\n"
            + "  url: \"postgresql+psycopg://x:x@localhost:5432/x\"\n"
            + "exchanges:\n"
            + "  binance:\n"
            + "    enabled: true\n"
            + "    symbols: [btcusdt]\n"
            + "redpanda:\n"
            + "  brokers: [\"localhost:9092\"]\n"
            + "  retention_hours: 6\n");
    try {
      assertThatThrownBy(() -> YamlConfigLoader.load(tmp, Map.of()))
          .isInstanceOf(CryptoLakeConfigException.class)
          .hasMessageContaining("retentionHours");
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  @Test
  void envOverrideBeatsYaml() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_env_override
    Map<String, String> overrides =
        EnvOverrides.normalize(Map.of("REDPANDA__BROKERS", "broker1:9092,broker2:9092"));
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), overrides);
    assertThat(config.redpanda().brokers()).containsExactly("broker1:9092", "broker2:9092");
  }

  @Test
  void hostDataDirAlias() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_host_data_dir_alias_overrides_writer_base_dir
    Map<String, String> overrides =
        EnvOverrides.normalize(Map.of("HOST_DATA_DIR", "/tmp/archive"));
    AppConfig config = YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), overrides);
    assertThat(config.writer().baseDir()).isEqualTo("/tmp/archive");
  }

  @Test
  void missingFileThrows() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_missing_file_raises
    assertThatThrownBy(() -> YamlConfigLoader.load(Path.of("/nonexistent/config.yaml"), Map.of()))
        .isInstanceOf(CryptoLakeConfigException.class)
        .hasMessageContaining("not found");
  }

  @Test
  void explicitEnvOverrideIsolatesFromSystemEnv() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_env_override_does_not_bleed_os_environ
    // Inject a fake System.getenv that contains a "leaked" value.
    // When we use the 2-arg load(..., envOverrides) overload, only the explicit overrides apply.
    Map<String, String> fakeEnv = new HashMap<>();
    fakeEnv.put("REDPANDA__BROKERS", "leaked:9092");
    // Override the env supplier to return this fake env (design §11 Q3 option b)
    YamlConfigLoader.envSupplier = () -> fakeEnv;

    // Use explicit overrides that do NOT include REDPANDA__BROKERS
    Map<String, String> explicitOverrides =
        EnvOverrides.normalize(Map.of("WRITER__COMPRESSION_LEVEL", "5"));
    AppConfig config =
        YamlConfigLoader.load(fixturesDir.resolve("config_valid.yaml"), explicitOverrides);

    assertThat(config.writer().compressionLevel()).isEqualTo(5);
    // The fake env REDPANDA__BROKERS should NOT have been applied — 2-arg overload is isolated
    assertThat(config.redpanda().brokers()).containsExactly("redpanda:9092");
  }
}
