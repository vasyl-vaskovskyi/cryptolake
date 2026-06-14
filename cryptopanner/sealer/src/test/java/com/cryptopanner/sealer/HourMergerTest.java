package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  @Test
  void mergesMinutesInOrder(@TempDir Path tmp) throws IOException {
    Path segments = tmp.resolve("segments");
    Path sealed = tmp.resolve("sealed");
    Path minDir = segments.resolve("btcusdt/trade/2026-06-14");
    Files.createDirectories(minDir);
    writeMinute(minDir, "minute-14-00.jsonl.zst", "a1\na2\n");
    writeMinute(minDir, "minute-14-01.jsonl.zst", "b1\n");
    writeMinute(minDir, "minute-14-02.jsonl.zst", "c1\nc2\nc3\n");

    HourMerger merger = new HourMerger(segments, sealed);
    HourMerger.Result result =
        merger.mergeHour("btcusdt", "trade", Instant.parse("2026-06-14T14:00:00Z"));

    Path expectedFile = sealed.resolve("btcusdt/trade/2026-06-14/hour-14.jsonl.zst");
    assertTrue(Files.exists(expectedFile));
    assertTrue(Files.exists(expectedFile.resolveSibling("hour-14.jsonl.zst.sha256")));

    byte[] decompressed = decompress(Files.readAllBytes(expectedFile));
    assertEquals("a1\na2\nb1\nc1\nc2\nc3\n", new String(decompressed));
    assertEquals(6, result.recordCount());
    assertEquals(List.of(0, 1, 2), result.minutesPresent());
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
