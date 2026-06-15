package com.cryptopanner.sealer;

import com.cryptopanner.common.config.SkeletonConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);

    String dateStr = required(args, "--date");
    int hour = Integer.parseInt(required(args, "--hour"));
    Instant hourStart =
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0)).toInstant(ZoneOffset.UTC);

    ObjectMapper mapper = new ObjectMapper();
    RestBackfiller backfiller = null;
    if (cfg.restBaseUrl() != null) {
      backfiller =
          new RestBackfiller(
              RestBackfiller.newHttpClient(),
              URI.create(cfg.restBaseUrl()),
              cfg.restApiKey(),
              mapper);
      System.out.println("[sealer] REST backfill enabled via " + cfg.restBaseUrl());
    }
    HourMerger merger =
        new HourMerger(cfg.paths().segments(), cfg.paths().sealed(), mapper, backfiller);
    System.out.println(
        "[sealer] sealing hour "
            + hour
            + " of "
            + dateStr
            + " for "
            + cfg.effectiveSubscriptions().size()
            + " subscription(s)");

    int failures = 0;
    for (SkeletonConfig.Subscription sub : cfg.effectiveSubscriptions()) {
      String symbol = sub.symbol();
      String stream = sub.stream();
      try {
        HourMerger.Result result = merger.mergeHour(symbol, stream, hourStart);
        System.out.println(
            "[sealer] "
                + symbol
                + "@"
                + stream
                + ": merged "
                + result.recordCount()
                + " records into "
                + result.file());

        Path manifestPath =
            result.file().resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
        ManifestWriter.write(
            manifestPath, cfg.nodeId(), symbol, stream, hourStart, result, Instant.now());
        System.out.println("[sealer] " + symbol + "@" + stream + ": manifest " + manifestPath);
      } catch (Exception e) {
        System.err.println("[sealer] " + symbol + "@" + stream + ": FAILED — " + e.getMessage());
        failures++;
      }
    }

    if (failures > 0) {
      System.err.println("[sealer] " + failures + " subscription(s) failed");
      System.exit(1);
    }
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing required flag: " + flag);
  }
}
