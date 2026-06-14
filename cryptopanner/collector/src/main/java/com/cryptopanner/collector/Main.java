package com.cryptopanner.collector;

import com.cryptopanner.common.config.SkeletonConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);
    ObjectMapper mapper = new ObjectMapper();

    Map<String, MinuteSegmentWriter> writers = new HashMap<>();
    for (SkeletonConfig.Subscription s : cfg.subscriptions()) {
      String key = s.symbol() + "@" + s.stream();
      writers.put(
          key,
          new MinuteSegmentWriter(
              cfg.paths().segments(), s.symbol(), s.stream(), Clock.systemUTC()));
    }
    List<String> streams = writers.keySet().stream().sorted().toList();

    AtomicLong framesSeen = new AtomicLong();
    BinanceWsClient client =
        new BinanceWsClient(
            URI.create(cfg.wsEndpointUrl()),
            streams,
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
            });

    client.start();
    System.out.println(
        "[collector] started; running for "
            + cfg.collectorMaxRuntimeS()
            + "s; streams="
            + streams);

    Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);

    System.out.println("[collector] stopping after " + framesSeen.get() + " frames");
    client.stop();
    for (MinuteSegmentWriter w : writers.values()) {
      w.close();
    }
    System.out.println("[collector] done");
  }
}
