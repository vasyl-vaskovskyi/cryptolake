// ports: tests/unit/test_config.py::TestConfigLoading::test_config_symbols_are_lowercase
// ports: tests/unit/test_config.py::TestConfigLoading::test_disabled_stream_no_depth_snapshot
// ports: tests/unit/test_config.py::TestConfigLoading::test_enabled_stream_list
// ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_auto_includes_depth_snapshot
// ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_no_duplicate_depth_snapshot
// ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_none_by_default
package com.cryptolake.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class BinanceExchangeConfigTest {

  @Test
  void symbolsAreLowercased() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_config_symbols_are_lowercase
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("BTCUSDT", "ETHUSDT"), null, null, null, null, null);
    for (String s : cfg.symbols()) {
      assertThat(s).isEqualTo(s.toLowerCase());
    }
    assertThat(cfg.symbols()).containsExactly("btcusdt", "ethusdt");
  }

  @Test
  void disabledStreamNoDepthSnapshot() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_disabled_stream_no_depth_snapshot
    // When depth is disabled, depth_snapshot should not appear
    StreamsConfig streams =
        new StreamsConfig(true, false, true, false, false, false); // no depth
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("btcusdt"), streams, null, null, null, null);
    List<String> enabled = cfg.getEnabledStreams();
    assertThat(enabled).doesNotContain("depth");
    assertThat(enabled).doesNotContain("depth_snapshot");
  }

  @Test
  void enabledStreamList() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_enabled_stream_list
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("btcusdt"), new StreamsConfig(), null, null, null,
            null);
    List<String> enabled = cfg.getEnabledStreams();
    assertThat(enabled)
        .containsExactly(
            "trades", "depth", "depth_snapshot", "bookticker", "funding_rate", "liquidations",
            "open_interest");
  }

  @Test
  void writerOverrideAutoIncludesSnapshot() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_auto_includes_depth_snapshot
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("btcusdt"), null, List.of("trades", "depth"), null,
            null, null);
    assertThat(cfg.writerStreamsOverride()).contains("depth_snapshot");
    assertThat(cfg.writerStreamsOverride()).containsExactly("trades", "depth", "depth_snapshot");
  }

  @Test
  void writerOverrideNoDuplicateSnapshot() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_no_duplicate_depth_snapshot
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("btcusdt"), null,
            List.of("depth", "depth_snapshot"), null, null, null);
    long count = cfg.writerStreamsOverride().stream().filter("depth_snapshot"::equals).count();
    assertThat(count).isEqualTo(1);
  }

  @Test
  void writerOverrideNullByDefault() {
    // ports: tests/unit/test_config.py::TestConfigLoading::test_writer_streams_override_none_by_default
    BinanceExchangeConfig cfg =
        new BinanceExchangeConfig(
            true, null, null, null, List.of("btcusdt"), null, null, null, null, null);
    assertThat(cfg.writerStreamsOverride()).isNull();
  }
}
