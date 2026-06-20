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
}
