package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.RestPoller;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PollerSchedulerTest {

  private HttpServer server;
  private URI baseUrl;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
    baseUrl = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void firesImmediatelyAndOnCadence() throws InterruptedException {
    AtomicInteger hits = new AtomicInteger();
    server.createContext("/ep", ex -> count(ex, hits));
    RestPoller poller =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/ep", Map.of(), b -> {});

    try (PollerScheduler sched = new PollerScheduler()) {
      sched.add(poller, 1, "test");
      sched.start();
      Thread.sleep(2500);
    }

    assertTrue(hits.get() >= 3, "expected >= 3 hits in 2.5s at 1s cadence, got " + hits.get());
  }

  @Test
  void closeStopsFurtherPolls() throws InterruptedException {
    AtomicInteger hits = new AtomicInteger();
    server.createContext("/ep", ex -> count(ex, hits));
    RestPoller poller =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/ep", Map.of(), b -> {});

    PollerScheduler sched = new PollerScheduler();
    sched.add(poller, 1, "test");
    sched.start();
    Thread.sleep(1200);
    sched.close();
    int afterClose = hits.get();

    Thread.sleep(1500);
    assertEquals(afterClose, hits.get(), "no further polls after close()");
  }

  @Test
  void multiplePollersAllFire() throws InterruptedException {
    AtomicInteger aHits = new AtomicInteger();
    AtomicInteger bHits = new AtomicInteger();
    server.createContext("/a", ex -> count(ex, aHits));
    server.createContext("/b", ex -> count(ex, bHits));

    RestPoller pa =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/a", Map.of(), x -> {});
    RestPoller pb =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/b", Map.of(), x -> {});

    try (PollerScheduler sched = new PollerScheduler()) {
      sched.add(pa, 1, "a");
      sched.add(pb, 1, "b");
      sched.start();
      Thread.sleep(1500);
    }

    assertTrue(aHits.get() >= 2, "endpoint /a expected >= 2 hits, got " + aHits.get());
    assertTrue(bHits.get() >= 2, "endpoint /b expected >= 2 hits, got " + bHits.get());
  }

  @Test
  void retriesOnFailureFasterThanCadence() throws InterruptedException {
    AtomicInteger hits = new AtomicInteger();
    server.createContext("/fail", ex -> failCount(ex, hits));
    RestPoller poller =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/fail", Map.of(), b -> {});

    try (PollerScheduler sched = new PollerScheduler()) {
      sched.add(poller, 30, "fail"); // 30s cadence: without retry only the initial hit fires
      sched.start();
      Thread.sleep(3500);
    }

    assertTrue(
        hits.get() >= 2,
        "expected retries (>=2 hits) within 3.5s despite a 30s cadence, got " + hits.get());
  }

  @Test
  void retryDelayIsExponentialCappedAtCadenceWithJitter() {
    assertEquals(1, PollerScheduler.retryDelaySeconds(0, 30));
    assertInRange(PollerScheduler.retryDelaySeconds(3, 30), 6, 10); // base 8 ±25%
    assertInRange(PollerScheduler.retryDelaySeconds(10, 30), 22, 38); // capped at cadence 30 ±25%
    assertInRange(PollerScheduler.retryDelaySeconds(6, 5), 4, 6); // capped at cadence 5 ±25%
  }

  private static void assertInRange(long actual, long lo, long hi) {
    assertTrue(actual >= lo && actual <= hi, "expected [" + lo + "," + hi + "] but was " + actual);
  }

  private static void failCount(HttpExchange ex, AtomicInteger counter) throws IOException {
    counter.incrementAndGet();
    byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(500, body.length);
    ex.getResponseBody().write(body);
    ex.close();
  }

  private static void count(HttpExchange ex, AtomicInteger counter) throws IOException {
    counter.incrementAndGet();
    byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, body.length);
    ex.getResponseBody().write(body);
    ex.close();
  }
}
