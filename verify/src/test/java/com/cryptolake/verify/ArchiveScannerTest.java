package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.verify.archive.ArchiveFile;
import com.cryptolake.verify.archive.ArchiveScanner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** new — gate 5 ordering + filter semantics */
class ArchiveScannerTest {

  @TempDir Path tmpDir;

  private void createArchive(
      String exchange, String symbol, String stream, String date, String filename)
      throws IOException {
    Path dir = tmpDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);
    Files.createDirectories(dir);
    Files.writeString(dir.resolve(filename), "");
  }

  @Test
  void lexOrderMatchesPython() throws IOException {
    // new — gate 5 ordering: sorted by path string
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-0.jsonl.zst");
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-1.jsonl.zst");
    createArchive("binance", "btcusdt", "depth", "2026-04-23", "hour-0.jsonl.zst");

    List<ArchiveFile> files = ArchiveScanner.scan(tmpDir, "2026-04-23", null, null, null);
    assertThat(files).hasSize(3);
    // depth comes before trades lexicographically
    assertThat(files.get(0).stream()).isEqualTo("depth");
    assertThat(files.get(1).stream()).isEqualTo("trades");
    assertThat(files.get(2).stream()).isEqualTo("trades");
  }

  @Test
  void filtersByExchangeSymbolStream() throws IOException {
    // new — Click option semantics
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-0.jsonl.zst");
    createArchive("binance", "ethusdt", "trades", "2026-04-23", "hour-0.jsonl.zst");

    List<ArchiveFile> files = ArchiveScanner.scan(tmpDir, "2026-04-23", null, "btcusdt", null);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).symbol()).isEqualTo("btcusdt");
  }

  @Test
  void recursivelyDiscoversBaseLateBackfill() throws IOException {
    // new — Tier 5 I7
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-0.jsonl.zst");
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-0.late-1.jsonl.zst");
    createArchive("binance", "btcusdt", "trades", "2026-04-23", "hour-0.backfill-1.jsonl.zst");

    List<ArchiveFile> files = ArchiveScanner.scan(tmpDir, "2026-04-23", null, null, null);
    assertThat(files).hasSize(3);
  }
}
