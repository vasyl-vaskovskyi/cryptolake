package com.cryptopanner.collector;

import com.cryptopanner.common.config.SkeletonConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);

    AtomicLong framesSeen = new AtomicLong();
    MinuteSegmentWriter writer =
        new MinuteSegmentWriter(
            cfg.paths().segments(), cfg.symbol(), cfg.stream(), Clock.systemUTC());

    BinanceWsClient client =
        new BinanceWsClient(
            URI.create(cfg.wsEndpointUrl()),
            List.of(cfg.symbol() + "@" + cfg.stream()),
            frame -> {
              try {
                writer.accept((frame + "\n").getBytes(StandardCharsets.UTF_8));
                long n = framesSeen.incrementAndGet();
                if (n % 100 == 0) {
                  System.out.println("[collector] frames seen: " + n);
                }
              } catch (Exception e) {
                System.err.println("[collector] write error: " + e.getMessage());
              }
            });

    client.start();
    System.out.println("[collector] started; running for " + cfg.collectorMaxRuntimeS() + "s");

    Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);

    System.out.println("[collector] stopping after " + framesSeen.get() + " frames");
    client.stop();
    writer.close();
    System.out.println("[collector] done");
  }
}
