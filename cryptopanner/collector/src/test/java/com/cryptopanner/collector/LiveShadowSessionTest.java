package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.collector.testutil.TinyWsServer;
import com.cryptopanner.common.DurableSegment;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LiveShadowSessionTest {

  private void writeShadow(Path segments, String symbol, String stream, String mm)
      throws Exception {
    Path shadow =
        segments
            .resolve(symbol)
            .resolve(stream)
            .resolve("2026-06-14")
            .resolve("minute-14-" + mm + ".shadow.jsonl.zst");
    DurableSegment.writeLines(shadow, List.of("x\n"));
  }

  @Test
  void scanReturnsOldestUnseenShadowMinutePairedWithPrimarySiblings(@TempDir Path segments)
      throws Exception {
    writeShadow(segments, "btcusdt", "trade", "23");
    writeShadow(segments, "btcusdt", "aggTrade", "23");
    writeShadow(segments, "btcusdt", "trade", "24"); // a later minute also present

    Set<String> seen = new HashSet<>();
    Optional<OverlapMinute> first = LiveShadowSession.scanSealedShadowMinute(segments, seen);

    assertTrue(first.isPresent());
    assertEquals(23, first.get().minuteOfHour(), "oldest sealed shadow minute first");
    assertEquals(2, first.get().pairs().size(), "both streams of minute 23 are paired");
    for (SegmentPair p : first.get().pairs()) {
      assertTrue(
          p.shadowFile().getFileName().toString().endsWith(".shadow.jsonl.zst"),
          "shadow side carries the infix");
      assertEquals(
          "minute-14-23.jsonl.zst",
          p.primaryFile().getFileName().toString(),
          "primary sibling drops the infix");
    }

    // Once minute 23 is recorded as seen, the next scan advances to minute 24.
    Optional<OverlapMinute> second = LiveShadowSession.scanSealedShadowMinute(segments, seen);
    assertTrue(second.isPresent());
    assertEquals(24, second.get().minuteOfHour());
  }

  @Test
  void scanReturnsEmptyWhenNoShadowSegmentsExist(@TempDir Path segments) throws Exception {
    assertEquals(
        Optional.empty(), LiveShadowSession.scanSealedShadowMinute(segments, new HashSet<>()));
  }

  @Test
  void opensRealShadowSocketProbesLiveAndWritesShadowSegment(@TempDir Path segments)
      throws Exception {
    try (TinyWsServer server =
        TinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":1,\"T\":1749902600000}}"))) {

      URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
      LiveShadowSession session =
          new LiveShadowSession(
              List.of(new LiveShadowSession.SocketSpec(uri, List.of("btcusdt@trade"))),
              List.of(new LiveShadowSession.WriterSpec("btcusdt", "trade")),
              segments,
              Duration.ofMillis(10), // tiny seal grace so close() seals promptly
              new ObjectMapper(),
              "cryptopanner/test+shadow",
              Duration.ofSeconds(5),
              () -> false,
              () -> false,
              () -> {},
              (s, clients) -> {});

      assertTrue(session.probeLiveness(), "shadow saw a frame → live");
      session.close(); // seals the open minute as a .shadow segment

      try (Stream<Path> walk = Files.walk(segments)) {
        assertTrue(
            walk.anyMatch(p -> p.getFileName().toString().endsWith(".shadow.jsonl.zst")),
            "the shadow connection's frame is sealed into a .shadow segment");
      }
    }
  }

  @Test
  void promoteRetiresOldPrimaryThenHandsOffTheNewPrimary(@TempDir Path segments) throws Exception {
    try (TinyWsServer server =
        TinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":1,\"T\":1749902600000}}"))) {

      URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
      boolean[] closePrimaryRan = {false};
      ShadowSession[] handedOff = {null};
      @SuppressWarnings("unchecked")
      List<?>[] handedClients = new List<?>[1];

      LiveShadowSession session =
          new LiveShadowSession(
              List.of(new LiveShadowSession.SocketSpec(uri, List.of("btcusdt@trade"))),
              List.of(new LiveShadowSession.WriterSpec("btcusdt", "trade")),
              segments,
              Duration.ofMillis(10),
              new ObjectMapper(),
              "cryptopanner/test+shadow",
              Duration.ofSeconds(5),
              () -> false,
              () -> false,
              () -> closePrimaryRan[0] = true,
              (s, clients) -> {
                // The old primary must already be retired by the time the new one is handed off.
                assertTrue(closePrimaryRan[0], "closePrimary runs before the hand-off");
                handedOff[0] = s;
                handedClients[0] = clients;
              });

      assertTrue(session.probeLiveness());
      session.promote();

      assertTrue(closePrimaryRan[0], "promote retires the old primary");
      assertEquals(session, handedOff[0], "the session hands itself off as the new primary");
      assertEquals(1, handedClients[0].size(), "the new primary's live clients are handed off");
      session.close();
    }
  }
}
