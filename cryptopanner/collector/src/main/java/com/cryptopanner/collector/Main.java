package com.cryptopanner.collector;

import com.cryptopanner.common.StreamRouting;
import com.cryptopanner.common.config.SkeletonConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);
    ObjectMapper mapper = new ObjectMapper();

    // Build per-(symbol,stream) writers.
    Map<String, MinuteSegmentWriter> writers = new HashMap<>();
    for (SkeletonConfig.Subscription s : cfg.subscriptions()) {
      String key = s.symbol() + "@" + s.stream();
      writers.put(
          key,
          new MinuteSegmentWriter(
              cfg.paths().segments(), s.symbol(), s.stream(), Clock.systemUTC()));
    }

    // Partition subscriptions by routed socket (§8.a).
    Map<StreamRouting.Socket, List<String>> bySocket = new EnumMap<>(StreamRouting.Socket.class);
    for (SkeletonConfig.Subscription s : cfg.subscriptions()) {
      StreamRouting.Socket sock = StreamRouting.forStreamType(s.stream());
      bySocket.computeIfAbsent(sock, k -> new ArrayList<>()).add(s.symbol() + "@" + s.stream());
    }

    // Shared frame consumer — writers.accept() is synchronized, safe for concurrent sockets.
    AtomicLong framesSeen = new AtomicLong();
    Consumer<String> onFrame =
        frame -> {
          try {
            String streamName = mapper.readTree(frame).get("stream").asText();
            MinuteSegmentWriter w = writers.get(streamName);
            if (w == null) {
              System.err.println("[collector] unknown stream in wrapper: " + streamName);
              return;
            }
            w.accept((frame + "\n").getBytes(StandardCharsets.UTF_8));
            long n = framesSeen.incrementAndGet();
            if (n % 100 == 0) {
              System.out.println("[collector] frames seen: " + n);
            }
          } catch (Exception e) {
            System.err.println("[collector] write error: " + e.getMessage());
          }
        };

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

    System.out.println(
        "[collector] started; running for "
            + cfg.collectorMaxRuntimeS()
            + "s; sockets="
            + clients.size());

    Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);

    System.out.println("[collector] stopping after " + framesSeen.get() + " frames");
    for (BinanceWsClient c : clients) {
      c.stop();
    }
    for (MinuteSegmentWriter w : writers.values()) {
      w.close();
    }
    System.out.println("[collector] done");
  }
}
