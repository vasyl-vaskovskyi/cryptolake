package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.collector.testutil.TinyWsServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BinanceWsClientTest {

  private TinyWsServer server;

  @BeforeEach
  void setUp() throws Exception {
    server =
        TinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":1}}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":2}}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":3}}"));
  }

  @AfterEach
  void tearDown() throws Exception {
    server.close();
  }

  @Test
  void connectsSubscribesAndEmitsFrames() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), (raw, receivedAt) -> seen.add(raw));

    client.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (seen.size() < 3 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    client.stop();

    assertEquals(3, seen.size());
    assertTrue(seen.get(0).contains("\"t\":1"));
    assertTrue(seen.get(2).contains("\"t\":3"));
  }

  @Test
  void tracksActivityWhileReceivingFrames() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), (raw, receivedAt) -> seen.add(raw));

    client.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (seen.size() < 3 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    long idle = client.idleNanos();
    client.stop();

    assertEquals(3, seen.size());
    assertTrue(
        idle < TimeUnit.SECONDS.toNanos(2),
        "idle time should be small right after receiving frames, was " + idle + "ns");
  }

  @Test
  void deliversReceiveTimestampWithEachFrame() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<Instant> stamps = new CopyOnWriteArrayList<>();
    Instant before = Instant.now();
    BinanceWsClient client =
        new BinanceWsClient(
            uri, List.of("btcusdt@trade"), (raw, receivedAt) -> stamps.add(receivedAt));

    client.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (stamps.size() < 3 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    client.stop();
    Instant after = Instant.now();

    assertEquals(3, stamps.size());
    for (Instant ts : stamps) {
      assertNotNull(ts);
      assertFalse(ts.isBefore(before), "receive time before start");
      assertFalse(ts.isAfter(after), "receive time after stop");
    }
  }

  @Test
  void tracksConnectionAgeFromAck() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), (raw, receivedAt) -> {});
    assertTrue(client.currentConnectionAge().isEmpty(), "no age before connect");

    client.start(); // blocks until ACK
    java.util.Optional<java.time.Duration> age = client.currentConnectionAge();
    client.stop();

    assertTrue(age.isPresent(), "age available once subscribed");
    assertTrue(
        age.get().compareTo(java.time.Duration.ofSeconds(5)) < 0,
        "a freshly-acked connection is young: " + age.get());
  }

  @Test
  void shadowClientSendsDistinctUserAgentHeader() throws Exception {
    // The rotation shadow connection must identify itself with a distinct User-Agent (§5.2 step 1)
    // so Binance-side and our own audit can tell the two overlapping connections apart.
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    BinanceWsClient client =
        new BinanceWsClient(
            uri, List.of("btcusdt@trade"), (raw, receivedAt) -> {}, "cryptopanner/test+shadow");

    client.start();
    String request = server.awaitHandshakeRequest(java.time.Duration.ofSeconds(5));
    client.stop();

    assertTrue(
        request.toLowerCase().contains("user-agent: cryptopanner/test+shadow"),
        "handshake should carry the shadow User-Agent, was:\n" + request);
  }

  @Test
  void emitsStructuredWsConnectEventOnAck(@TempDir java.nio.file.Path dir) throws Exception {
    java.nio.file.Path logFile = dir.resolve("logs/cryptopanner-collector@a.jsonl");
    com.cryptopanner.common.StructuredLog log =
        new com.cryptopanner.common.StructuredLog(logFile, "cryptopanner-collector", "a");
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), (raw, receivedAt) -> {}).withLog(log);

    client.start(); // blocks until ACK → ws_connect
    client.stop();

    var lines = java.nio.file.Files.readAllLines(logFile);
    assertTrue(
        lines.stream().anyMatch(l -> l.contains("\"event\":\"ws_connect\"")),
        "ws_connect (§11.e) emitted on SUBSCRIBE ACK, was:\n" + lines);
  }

  @Test
  void countsUnexpectedBinaryFrameAndKeepsStreamFlowing() throws Exception {
    // Binance fstream is text-only; a binary frame is anomalous. It must be counted (for §13
    // metrics) and NOT silently stall the socket — the text frame after it must still arrive.
    server.close();
    server =
        TinyWsServer.startFrames(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                TinyWsServer.Frame.text("{\"result\":null,\"id\":1}"),
                TinyWsServer.Frame.binary(new byte[] {0x00, 0x01, 0x02, 0x03}),
                TinyWsServer.Frame.text("{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":7}}")));

    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), (raw, receivedAt) -> seen.add(raw));

    client.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (seen.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    long binaryCount = client.binaryFramesUnexpected();
    client.stop();

    assertEquals(1, binaryCount, "the binary frame must be counted");
    assertEquals(1, seen.size(), "the text frame after the binary frame must still be delivered");
    assertTrue(seen.get(0).contains("\"t\":7"));
  }
}
