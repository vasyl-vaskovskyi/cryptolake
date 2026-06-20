package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MinuteSegmentWriterTest {

  @Test
  void bucketsAndRotatesByEventTime(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade")) {
      w.accept("frame-a-1\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("frame-a-2\n".getBytes(), Instant.parse("2026-06-14T14:23:59Z"));
      // A later event time rotates to the next minute.
      w.accept("frame-b-1\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z"));
    }

    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 = base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    assertTrue(Files.exists(minute23), "minute-14-23 file missing");
    assertTrue(Files.exists(minute24), "minute-14-24 file missing");
    assertTrue(
        Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst.sha256")));

    assertEquals("frame-a-1\nframe-a-2\n", new String(decompress(Files.readAllBytes(minute23))));
    assertEquals("frame-b-1\n", new String(decompress(Files.readAllBytes(minute24))));
  }

  @Test
  void lateFrameIsKeptInCurrentMinuteAndCounted(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade")) {
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      // Advance to minute 24 — this seals minute 23.
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z"));
      // Straggler whose minute (23) is already sealed: kept in the current open minute, counted.
      w.accept("late\n".getBytes(), Instant.parse("2026-06-14T14:23:30Z"));

      assertEquals(1, w.lateFrames());
    }

    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 = base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    // The straggler is never discarded; it lands in the current open minute (24), not minute 23.
    assertEquals("a\n", new String(decompress(Files.readAllBytes(minute23))));
    assertEquals("b\nlate\n", new String(decompress(Files.readAllBytes(minute24))));
  }

  @Test
  void sameMinuteEventsAppendWithoutRotation(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade")) {
      w.accept("x\n".getBytes(), Instant.parse("2026-06-14T14:23:01Z"));
      w.accept("y\n".getBytes(), Instant.parse("2026-06-14T14:23:45Z"));
      assertEquals(0, w.lateFrames());
    }
    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    assertEquals("x\ny\n", new String(decompress(Files.readAllBytes(minute23))));
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
