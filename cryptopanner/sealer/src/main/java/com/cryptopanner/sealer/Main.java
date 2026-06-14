package com.cryptopanner.sealer;

import com.cryptopanner.common.config.SkeletonConfig;
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
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0))
            .toInstant(ZoneOffset.UTC);

    HourMerger merger = new HourMerger(cfg.paths().segments(), cfg.paths().sealed());
    System.out.println("[sealer] merging hour " + hour + " of " + dateStr);
    HourMerger.Result result = merger.mergeHour(cfg.symbol(), cfg.stream(), hourStart);
    System.out.println("[sealer] merged " + result.recordCount() + " records into " + result.file());

    Path manifestPath =
        result
            .file()
            .resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
    ManifestWriter.write(
        manifestPath,
        cfg.nodeId(),
        cfg.symbol(),
        cfg.stream(),
        hourStart,
        result,
        Instant.now());
    System.out.println("[sealer] manifest written: " + manifestPath);
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing required flag: " + flag);
  }
}
