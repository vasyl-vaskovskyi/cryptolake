package com.cryptolake.writer.rotate;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FilePaths}.
 *
 * <p>Ports: Python's {@code test_file_paths.py} — naming conventions (Tier 5 M1, M15, I6).
 */
class FilePathsTest {

  // ports: Tier 5 M1 — symbol is lowercased
  @Test
  void buildFilePath_uppercaseSymbol_lowercasedInPath() {
    Path path =
        FilePaths.buildFilePath("/data", "binance", "BTCUSDT", "trades", "2024-01-15", 14, null);

    assertThat(path.toString()).contains("btcusdt");
    assertThat(path.toString()).doesNotContain("BTCUSDT");
  }

  // ports: design §2.4 — canonical path structure
  @Test
  void buildFilePath_primary_canonicalStructure() {
    Path path =
        FilePaths.buildFilePath("/archive", "binance", "btcusdt", "trades", "2024-01-15", 14, null);

    assertThat(path.toString())
        .isEqualTo("/archive/binance/btcusdt/trades/2024-01-15/hour-14.jsonl.zst");
  }

  // ports: Tier 5 M15 — late file naming
  @Test
  void buildFilePath_withLateSeq_lateNamePattern() {
    Path path =
        FilePaths.buildFilePath("/archive", "binance", "btcusdt", "trades", "2024-01-15", 14, 2);

    assertThat(path.toString())
        .isEqualTo("/archive/binance/btcusdt/trades/2024-01-15/hour-14.late-2.jsonl.zst");
  }

  // ports: Tier 5 I6 — sidecar path is data path + ".sha256"
  @Test
  void sidecarPath_appendsSha256Extension() {
    Path dataPath = Path.of("/archive/binance/btcusdt/trades/2024-01-15/hour-14.jsonl.zst");

    Path sidecar = FilePaths.sidecarPath(dataPath);

    assertThat(sidecar.toString())
        .isEqualTo("/archive/binance/btcusdt/trades/2024-01-15/hour-14.jsonl.zst.sha256");
  }

  // ports: Tier 5 M15 — late seq 1
  @Test
  void buildFilePath_lateSeq1_lateHyphen1() {
    Path path = FilePaths.buildFilePath("/d", "okx", "ethusdt", "depth", "2024-06-01", 0, 1);

    assertThat(path.getFileName().toString()).isEqualTo("hour-0.late-1.jsonl.zst");
  }

  // ports: design §4.5 — backfill path includes "backfill" prefix
  @Test
  void buildBackfillFilePath_hasBackfillSegment() {
    Path path =
        FilePaths.buildBackfillFilePath("/data", "binance", "BTCUSDT", "trades", "2024-01-15", 8);

    assertThat(path.toString()).startsWith("/data/backfill/binance/btcusdt/trades/");
  }
}
