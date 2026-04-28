package com.cryptolake.consolidation.harness;

import com.cryptolake.consolidation.scheduler.ConsolidationMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Gate-4 metric skeleton harness for the consolidation scheduler.
 *
 * <p>Registers all consolidation meters into a fresh {@link PrometheusMeterRegistry}, scrapes their
 * names, canonicalizes (sorted, stripped of {@code _max} and values), and writes to the output
 * file.
 *
 * <p>Same shape as writer and collector harnesses. Exit 0 = success.
 */
public final class MetricSkeletonDump {

  private static final Pattern VALUE_PATTERN = Pattern.compile("^([a-z_]+(?:\\{[^}]*\\})?)\\s+.*$");

  private MetricSkeletonDump() {}

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: MetricSkeletonDump <output-file>");
      System.exit(1);
    }
    Path outFile = Path.of(args[0]);
    Files.createDirectories(outFile.getParent());

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    // Register all consolidation meters
    new ConsolidationMetrics(registry);

    String scrape = registry.scrape();

    // Canonicalize: keep only metric name lines (not #HELP/#TYPE), remove _max, sort
    TreeSet<String> metricNames = new TreeSet<>();
    for (String line : scrape.split("\n")) {
      if (line.startsWith("#")) {
        continue;
      }
      if (line.isBlank()) {
        continue;
      }
      // Extract metric name (before whitespace or {)
      String name = line.split("[{\\s]")[0];
      if (name.endsWith("_max")) {
        continue;
      }
      // Only include app-level consolidation_* metrics
      if (name.startsWith("consolidation_")) {
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
