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

  private static void count(HttpExchange ex, AtomicInteger counter) throws IOException {
    counter.incrementAndGet();
    byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, body.length);
    ex.getResponseBody().write(body);
    ex.close();
  }
}
