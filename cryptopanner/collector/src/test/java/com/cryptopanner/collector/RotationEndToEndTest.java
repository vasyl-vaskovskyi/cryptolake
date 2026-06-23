package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.collector.testutil.TinyWsServer;
import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.FsHeavyLock;
import com.cryptopanner.common.Paths;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end rotation through the real conductor + real {@link LiveShadowSession} against a {@link
 * TinyWsServer} — the live composition previously only exercised behind the {@code ShadowSession}
 * seam. Drives open → probe → real sealed shadow minute → cutover → promote in one process, the
 * substance of what the soak (§14.e) validates without the docker stack.
 */
class RotationEndToEndTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private long ms(String iso) {
    return Instant.parse(iso).toEpochMilli();
  }

  private String tradeFrame(long id, long t) {
    return "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":" + id + ",\"T\":" + t + "}}";
  }

  private String tradeEnvelope(long id, long t) {
    return CaptureEnvelope.wsFrame(mapper, tradeFrame(id, t), Instant.EPOCH) + "\n";
  }

  private TreeSet<Long> tradeIds(Path zst) throws Exception {
    TreeSet<Long> ids = new TreeSet<>();
    for (String line : DurableSegment.readLines(zst)) {
      ids.add(CaptureEnvelope.unwrap(mapper, mapper.readTree(line)).get("data").get("t").asLong());
    }
    return ids;
  }

  @Test
  void liveRotationCutsOverPromotesAndHandsOffTheNewPrimary(@TempDir Path dir) throws Exception {
    Path segments = dir.resolve("segments");
    Path lock = dir.resolve(".fs-heavy.lock");
    Path rotations = dir.resolve("deploy/rotations.jsonl");

    // Primary already captured minute 14-23 (trades 1,2). The shadow will capture 2,3 → edge
    // straddle → PASS.
    Instant minute23 = Instant.parse("2026-06-14T14:23:00Z");
    Path primary = Paths.minuteSegment(segments, "btcusdt", "trade", minute23);
    DurableSegment.writeLines(
        primary,
        List.of(
            tradeEnvelope(1, ms("2026-06-14T14:23:05Z")),
            tradeEnvelope(2, ms("2026-06-14T14:23:10Z"))));

    try (TinyWsServer server =
        TinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                tradeFrame(2, ms("2026-06-14T14:23:10Z")),
                tradeFrame(3, ms("2026-06-14T14:23:20Z")),
                // a frame in minute 24 makes minute 23 the non-latest bucket, so it can seal
                tradeFrame(4, ms("2026-06-14T14:24:05Z"))))) {

      URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
      boolean[] closePrimaryRan = {false};
      ShadowSession[] handedOff = {null};

      java.util.function.Supplier<ShadowSession> opener =
          () -> {
            try {
              return new LiveShadowSession(
                  List.of(new LiveShadowSession.SocketSpec(uri, List.of("btcusdt@trade"))),
                  List.of(new LiveShadowSession.WriterSpec("btcusdt", "trade")),
                  segments,
                  Duration.ofMillis(10), // tiny grace so the sealed minute appears within a tick
                  mapper,
                  "cryptopanner/test+shadow",
                  Duration.ofSeconds(3),
                  () -> false,
                  () -> false,
                  () -> closePrimaryRan[0] = true,
                  (s, clients) -> handedOff[0] = s);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };

      WsConnectionManager mgr =
          new WsConnectionManager(
              opener,
              mapper,
              lock,
              rotations,
              () -> Optional.of(Duration.ofHours(23)),
              () -> "rot-e2e",
              () -> Instant.parse("2026-06-14T14:24:30Z"),
              WsConnectionManager.Config.defaults("collector-a"));

      WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

      assertEquals(WsConnectionManager.Status.COMPLETED, out.status(), "live rotation completes");
      assertEquals("PASS", out.verifyResult());
      assertEquals(
          new TreeSet<>(List.of(1L, 2L, 3L)),
          tradeIds(primary),
          "the overlap minute merges into the primary as the union");
      assertFalse(
          Files.exists(primary.resolveSibling("minute-14-23.shadow.jsonl.zst")),
          "the shadow segment is consumed by the merge");
      assertTrue(closePrimaryRan[0], "the old primary is retired on cutover");
      assertNotNull(handedOff[0], "the promoted shadow is handed off as the new primary");

      JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
      assertEquals("rot-e2e", ev.get("rotation_id").asText());
      assertEquals("PASS", ev.get("verify_result").asText());
      assertEquals(
          Optional.empty(), FsHeavyLock.heldBy(lock), "fs-heavy lock released after cutover");

      handedOff[0].close();
    }
  }
}
