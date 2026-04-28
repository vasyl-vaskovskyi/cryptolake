package com.cryptolake.backfill.harness;

import com.cryptolake.backfill.BackfillMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;

/**
 * Gate-4 metric skeleton harness for the backfill scheduler.
 *
 * <p>Same shape as writer/collector/consolidation harnesses. Registers all backfill meters, scrapes
 * their names (canonical: sorted, app-level only, no _max), and writes to the output file.
 */
public final class MetricSkeletonDump {

  private MetricSkeletonDump() {}

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: MetricSkeletonDump <output-file>");
      System.exit(1);
    }
    Path outFile = Path.of(args[0]);
    Files.createDirectories(outFile.getParent());

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    new BackfillMetrics(registry);

    String scrape = registry.scrape();

    TreeSet<String> metricNames = new TreeSet<>();
    for (String line : scrape.split("\n")) {
      if (line.startsWith("#") || line.isBlank()) {
        continue;
      }
      String name = line.split("[{\\s]")[0];
      if (name.endsWith("_max")) {
        continue;
      }
      if (name.startsWith("backfill_")) {
        metricNames.add(name);
      }
    }

    StringBuilder sb = new StringBuilder();
    for (String name : metricNames) {
      sb.append(name).append('\n');
    }

    Files.writeString(outFile, sb.toString(), StandardCharsets.UTF_8);
    System.out.println(
        "MetricSkeletonDump: wrote " + metricNames.size() + " metrics to " + outFile);
  }
}
