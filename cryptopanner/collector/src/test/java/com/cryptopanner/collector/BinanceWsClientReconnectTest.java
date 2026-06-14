package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.*;

import com.cryptopanner.collector.testutil.ReconnectingTinyWsServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinanceWsClientReconnectTest {

  private ReconnectingTinyWsServer server;

  @BeforeEach
  void setUp() throws Exception {
    // Each session: ACK + 2 frames, then server closes the connection.
    server =
        ReconnectingTinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":1}}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":2}}"));
  }

  @AfterEach
  void tearDown() throws Exception {
    server.close();
  }

  @Test
  void reconnectsAfterServerCloseAndResubscribes() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client = new BinanceWsClient(uri, List.of("btcusdt@trade"), seen::add);

    client.start();

    // Wait for >= 4 frames across at least 2 sessions (2 frames per session).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (seen.size() < 4 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    client.stop();

    assertTrue(seen.size() >= 4, "expected >= 4 frames across reconnects, got " + seen.size());
    assertTrue(server.sessionCount() >= 2, "expected >= 2 sessions, got " + server.sessionCount());
  }

  @Test
  void stopCancelsReconnect() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client = new BinanceWsClient(uri, List.of("btcusdt@trade"), seen::add);

    client.start();

    // Wait for the first session to complete (2 frames).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (seen.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    // Stop immediately — must not reconnect.
    client.stop();

    int sessionsAtStop = server.sessionCount();
    // Give reconnect window (backoff is at least 750ms after jitter on attempt 0).
    Thread.sleep(2000);

    // Session count must not have grown by more than 1 after stop.
    int sessionsAfterWait = server.sessionCount();
    assertTrue(
        sessionsAfterWait <= sessionsAtStop + 1,
        "expected no reconnects after stop, sessions before="
            + sessionsAtStop
            + " after="
            + sessionsAfterWait);
  }

  @Test
  void computeBackoffMillisFollowsSchedule() {
    // Attempt 0 → base 1 000 ms, attempt 5 → base 32 000 ms, attempt 6+ → base 60 000 ms.
    long base0 = BinanceWsClient.computeBackoffMillis(0);
    long base5 = BinanceWsClient.computeBackoffMillis(5);
    long base6 = BinanceWsClient.computeBackoffMillis(6);
    long base10 = BinanceWsClient.computeBackoffMillis(10);

    // ±25% jitter around nominal: each value must be in [nominal*0.75, nominal*1.25].
    assertInRange("attempt 0", base0, 750, 1250);
    assertInRange("attempt 5", base5, 24_000, 40_000);
    // Attempts 6 and 10 are both capped at 60 s.
    assertInRange("attempt 6", base6, 45_000, 75_000);
    assertInRange("attempt 10", base10, 45_000, 75_000);
  }

  private static void assertInRange(String label, long actual, long lo, long hi) {
    assertTrue(
        actual >= lo && actual <= hi,
        label + ": expected in [" + lo + ", " + hi + "] but was " + actual);
  }
}
