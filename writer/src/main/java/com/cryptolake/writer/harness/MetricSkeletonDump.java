package com.cryptolake.writer.harness;

import com.cryptolake.writer.metrics.WriterMetrics;
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
 * Harness that instantiates WriterMetrics, exercises each meter with sample label values, scrapes
 * the Prometheus registry, canonicalises the output to {@code name{sorted,label,keys}} (values
 * stripped, app-level lines only), and writes the result to the path supplied as {@code args[0]}.
 *
 * <p>Used by the Gradle {@code :writer:dumpMetricSkeleton} task (gate 4 metric-parity check).
 */
public final class MetricSkeletonDump {

  // Pattern to parse a Prometheus text-format line: name, labels, value
  // Matches:  metric_name{label="value",...} value [timestamp]
  // or:       metric_name value [timestamp]   (no labels)
  private static final Pattern LINE_PATTERN = Pattern.compile("^(\\w+)(\\{[^}]*\\})?\\s+.*");

  // Pattern to extract individual label keys from a {k="v",...} block
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("(\\w+)=\"[^\"]*\"");

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: MetricSkeletonDump <output-path>");
      System.exit(1);
    }
    Path outPath = Path.of(args[0]);
    Files.createDirectories(outPath.getParent());

    // Build registry and register all meters
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);

    // Exercise each meter so it appears in the scrape output.
    // Only touch meters that correspond to the 32 lines in parity-fixtures/metrics/writer.txt.
    // Extra Java-only meters (messagesSkipped, filesRotated, writeErrors, gapEnvelopesSuppressed,
    // gapCoalesced, hoursSealedToday, hoursSealedPreviousDay) are intentionally NOT exercised so
    // they stay unregistered and absent from the skeleton.

    String ex = "binance";
    String sym = "btcusdt";
    String stream = "trades";

    // Counters with exchange/symbol/stream labels
    metrics.messagesConsumed(ex, sym, stream).increment(1);
    metrics.sessionGapsDetected(ex, sym, stream).increment(1);
    metrics.bytesWritten(ex, sym, stream).increment(1);
    metrics.gapRecordsWritten(ex, sym, stream, "checkpoint_lost").increment(1);

    // Counters — no labels
    metrics.pgCommitFailures().increment(1);
    metrics.kafkaCommitFailures().increment(1);
    metrics.failoverTotal().increment(1);
    metrics.failoverRecordsTotal().increment(1);
    metrics.switchbackTotal().increment(1);

    // Gauges — with labels (lazy registration via set*)
    metrics.setCompressionRatio(ex, sym, stream, 3.5);
    metrics.setConsumerLag(ex, stream, 0L);

    // Gauges — no labels (registered at construction; just set a value)
    metrics.setDiskUsageBytes(0L);
    metrics.setDiskUsagePct(0.0);
    metrics.setFailoverActive(false);
    metrics.setGapPendingSize(0);

    // Histograms
    metrics.flushDurationMs(ex, sym, stream).record(1.0);
    metrics.failoverDurationSeconds().record(1.0);

    // Scrape and canonicalise
    String scrape = registry.scrape();
    String canonical = canonicalize(scrape);

    Files.writeString(outPath, canonical);
    System.out.println("MetricSkeletonDump: wrote " + outPath);
  }

  /**
   * Canonicalises Prometheus text-format output.
   *
   * <ul>
   *   <li>Skips comment lines ({@code #}) and empty lines.
   *   <li>Keeps only lines whose metric name starts with {@code writer_}.
   *   <li>Strips values (and optional timestamps) — retains only {@code name{sorted_label_keys}}.
   *   <li>Sorts label keys alphabetically within the braces.
   *   <li>Deduplicates (histogram buckets with different {@code le} values collapse to one line).
   *   <li>Sorts all output lines alphabetically.
   * </ul>
   */
  static String canonicalize(String scrape) {
    TreeSet<String> seen = new TreeSet<>();
    for (String raw : scrape.split("\n")) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      Matcher m = LINE_PATTERN.matcher(line);
      if (!m.matches()) {
        continue;
      }
      String name = m.group(1);
      if (!name.startsWith("writer_")) {
        continue;
      }
      // Strip Micrometer-specific histogram twins that have no Python equivalent:
      // Python's prometheus_client exposes _count/_sum/_bucket but NOT _max.
      // Keeping _max on the Java side would cause spurious gate-4 diffs.
      if (name.endsWith("_max")) {
        continue;
      }
      String labelsBlock = m.group(2); // may be null
      String canonical;
      if (labelsBlock == null || labelsBlock.isEmpty()) {
        canonical = name;
      } else {
        // Extract label keys, sort them, reassemble without values
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
