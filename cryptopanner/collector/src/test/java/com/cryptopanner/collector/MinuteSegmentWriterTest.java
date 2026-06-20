package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MinuteSegmentWriterTest {

  private static final Duration GRACE = Duration.ofSeconds(10);

  @Test
  void minuteIsHeldOpenUntilCloseGracePasses(@TempDir Path base) throws IOException {
    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE)) {
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z")); // minute 24 now the max

      // Before minute 23's deadline (14:24:00 + 10s = 14:24:10) nothing is sealed.
      w.sealElapsed(Instant.parse("2026-06-14T14:24:05Z"));
      assertFalse(Files.exists(minute23), "minute 23 sealed too early");

      // After the grace deadline, minute 23 seals; minute 24 (the max) stays open.
      w.sealElapsed(Instant.parse("2026-06-14T14:24:11Z"));
      assertTrue(Files.exists(minute23), "minute 23 should seal after close+grace");
      assertFalse(
          Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst")),
          "the latest minute stays open as a straggler target");
    }
    // close() seals everything that remains.
    assertTrue(Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst")));
    assertEquals("a\n", new String(decompress(Files.readAllBytes(minute23))));
  }

  @Test
  void lateFrameWithinGraceLandsInItsCorrectMinute(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE)) {
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z"));
      // Minute 23 has not been sealed yet, so a straggler for it lands in the CORRECT file.
      w.accept("late\n".getBytes(), Instant.parse("2026-06-14T14:23:30Z"));

      assertEquals(0, w.lateFrames(), "a straggler caught within grace is not 'late'");
    }

    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 = base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    assertEquals("a\nlate\n", new String(decompress(Files.readAllBytes(minute23))));
    assertEquals("b\n", new String(decompress(Files.readAllBytes(minute24))));
  }

  @Test
  void stragglerAfterSealIsKeptInCurrentMinuteAndCounted(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE)) {
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z"));
      w.sealElapsed(Instant.parse("2026-06-14T14:24:11Z")); // seals minute 23
      // Now minute 23 is sealed: its straggler is kept in the current open minute (24), counted.
      w.accept("late\n".getBytes(), Instant.parse("2026-06-14T14:23:30Z"));

      assertEquals(1, w.lateFrames());
    }

    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 = base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    assertEquals("a\n", new String(decompress(Files.readAllBytes(minute23))));
    assertEquals("b\nlate\n", new String(decompress(Files.readAllBytes(minute24))));
  }

  @Test
  void sameMinuteEventsAppendWithoutSealing(@TempDir Path base) throws IOException {
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE)) {
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
