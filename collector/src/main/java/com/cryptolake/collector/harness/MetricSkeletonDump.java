package com.cryptolake.collector.harness;

import com.cryptolake.collector.metrics.CollectorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Harness that instantiates {@link CollectorMetrics}, exercises each meter with sample label
 * values, scrapes the Prometheus registry, canonicalizes the output, and writes it to {@code
 * args[0]}.
 *
 * <p>Used by the Gradle {@code :collector:dumpMetricSkeleton} task (gate 4 metric-parity check).
 * Mirrors the writer's {@code MetricSkeletonDump} pattern exactly.
 */
public final class MetricSkeletonDump {

  private static final Pattern LINE_PATTERN = Pattern.compile("^(\\w+)(\\{[^}]*\\})?\\s+.*");
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("(\\w+)=\"[^\"]*\"");

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: MetricSkeletonDump <output-path>");
      System.exit(1);
    }
    Path outPath = Path.of(args[0]);
    Files.createDirectories(outPath.getParent());

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    CollectorMetrics metrics = new CollectorMetrics(registry);

    // Exercise all 9 meters with canonical label values
    String ex = "binance";
    String sym = "btcusdt";
    String streamD = "depth";
    String streamT = "trades";

    // #1 messages_produced_total
    metrics.messagesProduced(ex, sym, streamD).increment(1);
    metrics.messagesProduced(ex, sym, streamT).increment(1);

    // #2 ws_connections_active (gauge)
    metrics.setWsConnectionsActive(ex, 1);

    // #3 ws_reconnects_total
    metrics.wsReconnects(ex).increment(1);

    // #4 gaps_detected_total
    metrics.gapsDetected(ex, sym, streamD, "pu_chain_break").increment(1);

    // #5 exchange_latency_ms (histogram)
    metrics.exchangeLatencyMs(ex, sym, streamD).record(5.0);

    // #6 snapshots_taken_total
    metrics.snapshotsTaken(ex, sym).increment(1);

    // #7 snapshots_failed_total
    metrics.snapshotsFailed(ex, sym).increment(1);

    // #8 messages_dropped_total
    metrics.messagesDropped(ex, sym, streamD).increment(1);

    // #9 heartbeats_emitted_total
    metrics.heartbeatsEmitted(ex, sym, streamD, "alive").increment(1);

    String scrape = registry.scrape();
    String canonical = canonicalize(scrape);
    Files.writeString(outPath, canonical);
    System.out.println("MetricSkeletonDump: wrote " + outPath);
  }

  /**
   * Canonicalizes Prometheus text-format output (mirrors writer's MetricSkeletonDump.canonicalize).
   */
  static String canonicalize(String scrape) {
    TreeSet<String> seen = new TreeSet<>();
    for (String raw : scrape.split("\n")) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) continue;
      Matcher m = LINE_PATTERN.matcher(line);
      if (!m.matches()) continue;
      String name = m.group(1);
      if (!name.startsWith("collector_")) continue;
      // Strip Micrometer-only _max lines — Python doesn't emit these (Tier 5 H5)
      if (name.endsWith("_max")) continue;
      String labelsBlock = m.group(2);
      String canonical;
      if (labelsBlock == null || labelsBlock.isEmpty()) {
        canonical = name;
      } else {
        List<String> keys = new ArrayList<>();
        Matcher km = LABEL_KEY_PATTERN.matcher(labelsBlock);
        while (km.find()) {
          keys.add(km.group(1));
        }
        keys.sort(String::compareTo);
        canonical = name + "{" + String.join(",", keys) + "}";
      }
      seen.add(canonical);
    }
    StringBuilder sb = new StringBuilder();
    for (String line : seen) {
      sb.append(line).append('\n');
    }
    return sb.toString();
  }
}
