package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HourMergerTest {

  /**
   * Uses {@code ticker} (non-ID-bearing) so the merge succeeds on opaque content; sequence analysis
   * is covered by the trade-specific test below and by {@link SequenceAnalyzerTest}.
   */
  @Test
  void mergesMinutesInOrder(@TempDir Path tmp) throws IOException {
    Path segments = tmp.resolve("segments");
    Path sealed = tmp.resolve("sealed");
    Path minDir = segments.resolve("btcusdt/ticker/2026-06-14");
    Files.createDirectories(minDir);
    writeMinute(minDir, "minute-14-00.jsonl.zst", "a1\na2\n");
    writeMinute(minDir, "minute-14-01.jsonl.zst", "b1\n");
    writeMinute(minDir, "minute-14-02.jsonl.zst", "c1\nc2\nc3\n");

    HourMerger merger = new HourMerger(segments, sealed);
    HourMerger.Result result =
        merger.mergeHour("btcusdt", "ticker", Instant.parse("2026-06-14T14:00:00Z"));

    Path expectedFile = sealed.resolve("btcusdt/ticker/2026-06-14/hour-14.jsonl.zst");
    assertTrue(Files.exists(expectedFile));
    assertTrue(Files.exists(expectedFile.resolveSibling("hour-14.jsonl.zst.sha256")));

    byte[] decompressed = decompress(Files.readAllBytes(expectedFile));
    assertEquals("a1\na2\nb1\nc1\nc2\nc3\n", new String(decompressed));
    assertEquals(6, result.recordCount());
    assertEquals(List.of(0, 1, 2), result.minutesPresent());
    assertNull(result.sequence(), "ticker is not ID-bearing — sequence analysis must be null");
  }

  /**
   * §12.k / "explicit gap surfacing": a stream that captured nothing for the hour (no segments
   * directory at all) must seal a zero-record hour rather than throwing — so the Sealer surfaces
   * the empty hour as a manifest instead of hard-failing the whole run.
   */
  @Test
  void emptyHourWithNoSegmentsDirectoryStillSeals(@TempDir Path tmp) throws IOException {
    Path segments = tmp.resolve("segments");
    Path sealed = tmp.resolve("sealed");
    // Deliberately create no segments directory for this (symbol, stream).

    HourMerger.Result result =
        new HourMerger(segments, sealed)
            .mergeHour("btcusdt", "openInterest", Instant.parse("2026-06-14T14:00:00Z"));

    assertEquals(0, result.recordCount());
    assertTrue(result.minutesPresent().isEmpty());
    Path expectedFile = sealed.resolve("btcusdt/openInterest/2026-06-14/hour-14.jsonl.zst");
    assertTrue(Files.exists(expectedFile), "empty hour must still seal a zero-record file");
    assertTrue(Files.exists(expectedFile.resolveSibling("hour-14.jsonl.zst.sha256")));
  }

  @Test
  void detectsSequenceGapForTrade(@TempDir Path tmp) throws IOException {
    Path segments = tmp.resolve("segments");
    Path sealed = tmp.resolve("sealed");
    Path minDir = segments.resolve("btcusdt/trade/2026-06-14");
    Files.createDirectories(minDir);
    // IDs 100, 101 then 104, 105 — gap covers 102..103.
    writeMinute(
        minDir,
        "minute-14-00.jsonl.zst",
        "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":100}}\n"
            + "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":101}}\n");
    writeMinute(
        minDir,
        "minute-14-01.jsonl.zst",
        "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":104}}\n"
            + "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":105}}\n");

    HourMerger.Result result =
        new HourMerger(segments, sealed)
            .mergeHour("btcusdt", "trade", Instant.parse("2026-06-14T14:00:00Z"));

    assertNotNull(result.sequence());
    assertEquals(100, result.sequence().firstId());
    assertEquals(105, result.sequence().lastId());
    assertEquals(1, result.sequence().gaps().size());
    SequenceAnalyzer.Gap g = result.sequence().gaps().get(0);
    assertEquals(102, g.from());
    assertEquals(103, g.to());
    assertEquals(2, g.count());
    // No backfiller configured → outcomes are NOT_ATTEMPTED, attempts list empty.
    assertEquals(List.of(RestBackfiller.Outcome.NOT_ATTEMPTED), result.gapOutcomes());
    assertTrue(result.backfillAttempts().isEmpty());
  }

  private static void writeMinute(Path dir, String name, String content) throws IOException {
    byte[] z = Zstd.compress(content.getBytes(), 3);
    Files.write(dir.resolve(name), z);
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
