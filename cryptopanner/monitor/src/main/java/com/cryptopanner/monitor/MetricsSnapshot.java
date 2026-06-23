package com.cryptopanner.monitor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * Parsed subset of a Node Agent's {@code GET /metrics} OpenMetrics text (master spec §11.c). The
 * Monitor reads the <em>current</em> gauge/counter values for alert conditions whose signal is not
 * carried in {@code /status} (upload backlog, connection age). It is not a historical store — each
 * scrape replaces the snapshot.
 *
 * <p>Lines beginning {@code #} (HELP/TYPE) and blank lines are skipped. Labelled series ({@code
 * name{label="x"} value}) are summed under their bare metric name, which is the right aggregate for
 * the counters and the per-slot gauges the Monitor inspects.
 */
public record MetricsSnapshot(Map<String, Double> values) {

  public static MetricsSnapshot parse(String text) {
    Map<String, Double> values = new LinkedHashMap<>();
    for (String raw : text.split("\n")) {
      String line = raw.strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int sp = line.lastIndexOf(' ');
      if (sp < 0) {
        continue;
      }
      String key = line.substring(0, sp).strip();
      String valueText = line.substring(sp + 1).strip();
      int brace = key.indexOf('{');
      String name = brace < 0 ? key : key.substring(0, brace);
      double value;
      try {
        value = Double.parseDouble(valueText);
      } catch (NumberFormatException e) {
        continue;
      }
      values.merge(name, value, Double::sum);
    }
    return new MetricsSnapshot(Map.copyOf(values));
  }

  /** Current value of a metric by bare name, summed across label sets. */
  public OptionalDouble gauge(String name) {
    Double v = values.get(name);
    return v == null ? OptionalDouble.empty() : OptionalDouble.of(v);
  }

  public OptionalDouble sealedFilesPendingUpload() {
    return gauge("cryptopanner_sealed_files_pending_upload");
  }

  public OptionalDouble currentConnectionAgeSeconds() {
    return gauge("cryptopanner_current_connection_age_seconds");
  }
}
