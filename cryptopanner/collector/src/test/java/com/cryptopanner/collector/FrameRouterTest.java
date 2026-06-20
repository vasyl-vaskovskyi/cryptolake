package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.Paths;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FrameRouterTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();
  private static final Instant RECEIVED = Instant.parse("2026-06-20T14:23:15Z");

  private static String readEnvelope(Path file) throws IOException {
    byte[] zstd = Files.readAllBytes(file);
    byte[] out = new byte[(int) Zstd.decompressedSize(zstd)];
    Zstd.decompress(out, zstd);
    return new String(out);
  }

  @Test
  void tradeIsWrappedAndBucketedByEventTimeNotReceiveTime(@TempDir Path base) throws IOException {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@trade", w));
    long tradeTimeMs = 1_750_000_000_000L; // a minute far from RECEIVED's 2026-06-20 14:23
    String raw =
        "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1750000000999,\"T\":"
            + tradeTimeMs
            + ",\"t\":1}}";

    router.handle(raw, RECEIVED);
    w.close();

    Path byEvent = Paths.minuteSegment(base, "btcusdt", "trade", Instant.ofEpochMilli(tradeTimeMs));
    Path byReceive = Paths.minuteSegment(base, "btcusdt", "trade", RECEIVED);
    assertTrue(Files.exists(byEvent), "frame should be bucketed by trade time T");
    assertFalse(Files.exists(byReceive), "frame must NOT be bucketed by receive time");

    JsonNode env = MAPPER.readTree(readEnvelope(byEvent).strip());
    assertEquals("ws_frame", env.get("envelope").asText());
    assertEquals(RECEIVED.toString(), env.get("received_at").asText());
    assertEquals(raw, env.get("raw").asText());
    assertEquals(1, router.framesWritten());
  }

  @Test
  void forceOrderRoutesBySymbolAndBucketsByEventTime(@TempDir Path base) throws IOException {
    MinuteSegmentWriter fo = new MinuteSegmentWriter(base, "btcusdt", "forceOrder");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@forceOrder", fo));
    long eventMs = 1_750_000_000_000L;
    String raw =
        "{\"stream\":\"!forceOrder@arr\",\"data\":{\"e\":\"forceOrder\",\"E\":"
            + eventMs
            + ",\"o\":{\"s\":\"BTCUSDT\",\"T\":1750000000001}}}";

    router.handle(raw, RECEIVED);
    fo.close();

    Path byEvent =
        Paths.minuteSegment(base, "btcusdt", "forceOrder", Instant.ofEpochMilli(eventMs));
    assertTrue(Files.exists(byEvent));
    assertEquals(raw, MAPPER.readTree(readEnvelope(byEvent).strip()).get("raw").asText());
  }

  @Test
  void depthBucketsByEventTimeEWithSuffixStreamName(@TempDir Path base) throws IOException {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "depth@100ms");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@depth@100ms", w));
    long eMs = 1_750_000_000_000L;
    String raw =
        "{\"stream\":\"btcusdt@depth@100ms\",\"data\":{\"e\":\"depthUpdate\",\"E\":"
            + eMs
            + ",\"U\":1,\"u\":2,\"pu\":0}}";

    router.handle(raw, RECEIVED);
    w.close();

    assertTrue(
        Files.exists(
            Paths.minuteSegment(base, "btcusdt", "depth@100ms", Instant.ofEpochMilli(eMs))),
        "depth must bucket by E even with an @-suffixed stream name");
  }

  @Test
  void aggTradeBucketsByTradeTimeT(@TempDir Path base) throws IOException {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "aggTrade");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@aggTrade", w));
    long tMs = 1_750_000_000_000L;
    String raw =
        "{\"stream\":\"btcusdt@aggTrade\",\"data\":{\"e\":\"aggTrade\",\"E\":1750000000999,\"T\":"
            + tMs
            + ",\"a\":42}}";

    router.handle(raw, RECEIVED);
    w.close();

    assertTrue(
        Files.exists(Paths.minuteSegment(base, "btcusdt", "aggTrade", Instant.ofEpochMilli(tMs))),
        "aggTrade must bucket by trade time T");
  }

  @Test
  void missingEventTimeFallsBackToReceiveTimeAndCounts(@TempDir Path base) throws IOException {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@trade", w));
    String raw = "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"t\":1}}"; // no T, no E

    router.handle(raw, RECEIVED);
    w.close();

    assertTrue(Files.exists(Paths.minuteSegment(base, "btcusdt", "trade", RECEIVED)));
    assertEquals(1, router.unparseableFrames());
    assertEquals(1, router.framesWritten());
  }

  @Test
  void unknownStreamIsNotWritten(@TempDir Path base) {
    MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade");
    FrameRouter router = new FrameRouter(MAPPER, Map.of("btcusdt@trade", w));

    router.handle("{\"stream\":\"ethusdt@trade\",\"data\":{\"E\":1,\"T\":1}}", RECEIVED);

    assertEquals(0, router.framesWritten());
  }
}
