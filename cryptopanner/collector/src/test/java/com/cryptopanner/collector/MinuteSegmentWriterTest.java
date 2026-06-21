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
  void minuteSealsGraceMonotonicNanosAfterItsEndIsObserved(@TempDir Path base) throws IOException {
    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    long graceNanos = GRACE.toNanos();
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE)) {
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z")); // minute 24 now the max

      // First tick observes minute 23 has ended (wall >= 14:24:00); grace starts at mono=0.
      w.sealElapsed(Instant.parse("2026-06-14T14:24:01Z"), 0L);
      assertFalse(Files.exists(minute23), "grace not yet elapsed");

      // Less than grace of monotonic time elapsed — still open, even if wall jumped far ahead.
      w.sealElapsed(Instant.parse("2026-06-14T14:30:00Z"), graceNanos - 1);
      assertFalse(Files.exists(minute23), "an NTP wall jump must not seal before monotonic grace");

      // Grace of monotonic time elapsed → minute 23 seals; minute 24 (the max) stays open.
      w.sealElapsed(Instant.parse("2026-06-14T14:24:01Z"), graceNanos);
      assertTrue(Files.exists(minute23), "minute 23 should seal after monotonic grace");
      assertFalse(
          Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst")),
          "the latest minute stays open as a straggler target");
    }
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
      long graceNanos = GRACE.toNanos();
      w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("b\n".getBytes(), Instant.parse("2026-06-14T14:24:01Z"));
      w.sealElapsed(Instant.parse("2026-06-14T14:24:01Z"), 0L); // observe minute 23 ended
      w.sealElapsed(Instant.parse("2026-06-14T14:24:01Z"), graceNanos); // seals minute 23
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

  @Test
  void acceptAfterCloseIsANoOpNotAnError(@TempDir Path base) throws IOException {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", GRACE);
    w.accept("a\n".getBytes(), Instant.parse("2026-06-14T14:23:10Z"));
    w.close(); // seals everything and clears the open buffers

    // A frame racing in after close() (the shutdown window) must not throw — regression for the
    // TreeMap.lastEntry() NPE seen when a post-close straggler hit the empty open map.
    w.accept("late\n".getBytes(), Instant.parse("2026-06-14T14:23:30Z"));
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
