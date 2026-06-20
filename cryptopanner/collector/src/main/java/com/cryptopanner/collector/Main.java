package com.cryptopanner.collector;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.common.RestPoller;
import com.cryptopanner.common.StreamRouting;
import com.cryptopanner.common.config.SkeletonConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);
    ObjectMapper mapper = EnvelopeCodec.newMapper();

    // Per-(symbol,stream) writers. Keyed by "<symbol>@<stream>" for direct lookup from
    // wrapper.stream
    // and from broadcast-fanout writers (e.g. "btcusdt@forceOrder").
    Map<String, MinuteSegmentWriter> writers = new HashMap<>();
    for (SkeletonConfig.Subscription s : cfg.subscriptions()) {
      String key = s.symbol() + "@" + s.stream();
      writers.put(key, new MinuteSegmentWriter(cfg.paths().segments(), s.symbol(), s.stream()));
    }
    // Broadcast fan-out writers: one (symbol, forceOrder) writer per configured symbol.
    if (cfg.broadcasts().contains(FrameRouter.FORCE_ORDER_BROADCAST)) {
      for (String symbol : cfg.symbols()) {
        String key = symbol + "@" + FrameRouter.FORCE_ORDER_STREAM;
        writers.computeIfAbsent(
            key,
            k ->
                new MinuteSegmentWriter(
                    cfg.paths().segments(), symbol, FrameRouter.FORCE_ORDER_STREAM));
      }
    }
    // REST-poll fan-out writers: one (symbol, restPoll.stream) per configured symbol.
    for (SkeletonConfig.RestPoll p : cfg.restPolls()) {
      if (!p.perSymbol()) continue; // non-per-symbol REST polls need a global path scheme — TODO.
      for (String symbol : cfg.symbols()) {
        String key = symbol + "@" + p.stream();
        writers.computeIfAbsent(
            key, k -> new MinuteSegmentWriter(cfg.paths().segments(), symbol, p.stream()));
      }
    }

    // Partition subscriptions + broadcasts by routed socket (§8.a).
    Map<StreamRouting.Socket, List<String>> bySocket = new EnumMap<>(StreamRouting.Socket.class);
    for (SkeletonConfig.Subscription s : cfg.subscriptions()) {
      StreamRouting.Socket sock = StreamRouting.forStreamType(s.stream());
      bySocket.computeIfAbsent(sock, k -> new ArrayList<>()).add(s.symbol() + "@" + s.stream());
    }
    for (String b : cfg.broadcasts()) {
      StreamRouting.Socket sock = StreamRouting.forBroadcast(b);
      bySocket.computeIfAbsent(sock, k -> new ArrayList<>()).add(b);
    }

    // Shared frame router — parses, routes, wraps in a ws_frame envelope, and buckets by server
    // event time. MinuteSegmentWriter.accept() is synchronized, safe for concurrent sockets.
    FrameRouter router = new FrameRouter(mapper, writers);
    BiConsumer<String, Instant> onFrame = router::handle;

    // Open one BinanceWsClient per non-empty socket group.
    List<BinanceWsClient> clients = new ArrayList<>();

    List<String> publicSubs = bySocket.get(StreamRouting.Socket.PUBLIC);
    if (publicSubs != null) {
      BinanceWsClient c =
          new BinanceWsClient(URI.create(cfg.wsPublicEndpointUrl()), publicSubs, onFrame);
      c.start();
      clients.add(c);
      System.out.println("[collector] /public connected with " + publicSubs);
    }

    List<String> marketSubs = bySocket.get(StreamRouting.Socket.MARKET);
    if (marketSubs != null) {
      BinanceWsClient c =
          new BinanceWsClient(URI.create(cfg.wsMarketEndpointUrl()), marketSubs, onFrame);
      c.start();
      clients.add(c);
      System.out.println("[collector] /market connected with " + marketSubs);
    }

    // REST pollers — one per (rest_poll, configured symbol). Cadences pinned per spec §7.b in
    // production; the skeleton config can override for fast smoke tests.
    PollerScheduler scheduler = new PollerScheduler();
    if (!cfg.restPolls().isEmpty()) {
      if (cfg.restBaseUrl() == null) {
        throw new IllegalStateException("rest_polls configured but rest_base_url is missing");
      }
      HttpClient httpClient = RestPoller.newHttpClient();
      URI baseUrl = URI.create(cfg.restBaseUrl());
      for (SkeletonConfig.RestPoll p : cfg.restPolls()) {
        if (!p.perSymbol()) continue;
        for (String symbol : cfg.symbols()) {
          MinuteSegmentWriter writer = writers.get(symbol + "@" + p.stream());
          Consumer<byte[]> sink =
              b -> {
                try {
                  writer.accept(b, Instant.now());
                } catch (Exception e) {
                  System.err.println(
                      "[collector] rest sink write failed for "
                          + symbol
                          + "@"
                          + p.stream()
                          + ": "
                          + e.getMessage());
                }
              };
          RestPoller poller =
              new RestPoller(
                  httpClient, mapper, baseUrl, p.endpoint(), Map.of("symbol", symbol), sink);
          scheduler.add(poller, p.cadenceSeconds(), symbol + "@" + p.stream());
        }
      }
      scheduler.start();
    }

    System.out.println(
        "[collector] started; running for "
            + cfg.collectorMaxRuntimeS()
            + "s; sockets="
            + clients.size());

    // Single idempotent close path used by both the timed-exit and the SIGTERM hook so buffered
    // minute segments are never lost on signal — durability invariant (master spec §3.b).
    AtomicBoolean closed = new AtomicBoolean(false);
    Runnable closeAll =
        () -> {
          if (!closed.compareAndSet(false, true)) return;
          System.out.println(
              "[collector] stopping after "
                  + router.framesWritten()
                  + " frames ("
                  + router.unparseableFrames()
                  + " unparseable)");
          scheduler.close();
          for (BinanceWsClient c : clients) {
            c.stop();
          }
          for (MinuteSegmentWriter w : writers.values()) {
            try {
              w.close();
            } catch (IOException e) {
              System.err.println("[collector] writer close failed: " + e.getMessage());
            }
          }
          System.out.println("[collector] done");
        };

    Runtime.getRuntime().addShutdownHook(new Thread(closeAll, "collector-shutdown"));

    Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);
    closeAll.run();
  }
}
