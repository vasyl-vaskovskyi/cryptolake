package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MinuteSegmentWriterTest {

  /** A clock that returns whatever the caller sets via {@link #set(Instant)}. */
  static final class StubClock extends Clock {
    private volatile Instant now = Instant.parse("2026-06-14T14:23:00Z");

    void set(Instant i) {
      this.now = i;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId z) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  @Test
  void writesAndRotatesAtMinuteBoundary(@TempDir Path base) throws IOException {
    StubClock clock = new StubClock();
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", clock)) {
      clock.set(Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("frame-a-1\n".getBytes());
      w.accept("frame-a-2\n".getBytes());

      // Cross the minute boundary.
      clock.set(Instant.parse("2026-06-14T14:24:01Z"));
      w.accept("frame-b-1\n".getBytes());
    }

    Path minute23 = base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 = base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    assertTrue(Files.exists(minute23), "minute-14-23 file missing");
    assertTrue(Files.exists(minute24), "minute-14-24 file missing");
    assertTrue(
        Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst.sha256")));

    byte[] decompressed23 = decompress(Files.readAllBytes(minute23));
    assertEquals("frame-a-1\nframe-a-2\n", new String(decompressed23));
    byte[] decompressed24 = decompress(Files.readAllBytes(minute24));
    assertEquals("frame-b-1\n", new String(decompressed24));
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
