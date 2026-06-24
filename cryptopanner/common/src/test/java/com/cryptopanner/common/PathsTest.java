package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PathsTest {

  @Test
  void minuteSegment_buildsCanonicalUtcPath() {
    Path base = Path.of("/data/cryptopanner/segments");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    Path actual = Paths.minuteSegment(base, "btcusdt", "trade", t);
    assertEquals(
        Path.of("/data/cryptopanner/segments/btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst"),
        actual);
  }

  @Test
  void minuteSegment_shadowVariantInsertsShadowInfix() {
    Path base = Path.of("/data/cryptopanner/segments");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    Path actual = Paths.minuteSegment(base, "btcusdt", "trade", t, true);
    assertEquals(
        Path.of(
            "/data/cryptopanner/segments/btcusdt/trade/2026-06-14/minute-14-23.shadow.jsonl.zst"),
        actual);
  }

  @Test
  void minuteSegment_shadowFalseMatchesPrimaryPath() {
    Path base = Path.of("/data/cryptopanner/segments");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    assertEquals(
        Paths.minuteSegment(base, "btcusdt", "trade", t),
        Paths.minuteSegment(base, "btcusdt", "trade", t, false));
  }

  @Test
  void hourSealed_buildsCanonicalUtcPath() {
    Path base = Path.of("/data/cryptopanner/sealed");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    Path actual = Paths.hourSealed(base, "btcusdt", "trade", t);
    assertEquals(
        Path.of("/data/cryptopanner/sealed/btcusdt/trade/2026-06-14/hour-14.jsonl.zst"), actual);
  }

  @Test
  void s3Key_buildsCanonicalKey() {
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    String actual = Paths.s3Key("vps-fra-1", "btcusdt", "trade", t);
    assertEquals("vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst", actual);
  }

  /**
   * Per-node independence (master spec §3.d): two nodes capturing the SAME symbol/stream/hour write
   * to distinct, node-prefixed keys, so independent archives never collide in shared S3. This is
   * the structural guarantee the (otherwise low-value) two-node soak would have exercised.
   */
  @Test
  void s3Key_distinctNodesNeverCollideForSameData() {
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    String a = Paths.s3Key("vps-fra-1", "btcusdt", "trade", t);
    String b = Paths.s3Key("vps-tyo-1", "btcusdt", "trade", t);
    assertNotEquals(a, b, "different nodes must not share an S3 key for identical data");
    assertTrue(a.startsWith("vps-fra-1/"), a);
    assertTrue(b.startsWith("vps-tyo-1/"), b);
  }
}
