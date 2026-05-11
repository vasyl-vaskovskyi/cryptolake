package com.cryptolake.verify.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link GapRecord}s from per-collector Kafka outage journal files.
 *
 * <p>Journal files live at {@code ${dataDir}/cryptolake/<collectorId>/kafka_outage.json} (written
 * by the collector's {@code KafkaOutageJournal}). Each file is a single JSON object with one field:
 * {@code outage_started_at_ns} (long nanoseconds since epoch). The file's presence indicates an
 * active (unresolved) outage; it is deleted when the outage clears.
 *
 * <p>For every such file whose {@code outage_started_at_ns} timestamp overlaps the {@link
 * AuditScope}, one {@link GapRecord} is emitted with {@code source="kafka_outage"}, {@code startMs}
 * derived from the file timestamp, and {@code endMs=now()} (because the outage is still active — if
 * it had cleared, the file would have been deleted).
 *
 * <p>Graceful degradation: if {@code ${dataDir}/cryptolake/} does not exist, or any individual
 * outage file is missing or unparseable, the source returns an empty list (or partial results)
 * without throwing.
 */
public final class KafkaOutageGapSource implements GapSource {

  private static final Logger log = LoggerFactory.getLogger(KafkaOutageGapSource.class);

  private static final String SOURCE_LABEL = "kafka_outage";

  private final Path dataDir;
  private final ObjectMapper mapper;
  private final Supplier<Instant> nowSupplier;

  /**
   * Constructs a {@code KafkaOutageGapSource}.
   *
   * @param dataDir host data directory (e.g. {@code /data}); outage files live at {@code
   *     ${dataDir}/cryptolake/<collectorId>/kafka_outage.json}
   * @param mapper shared {@link ObjectMapper}
   * @param nowSupplier injectable "now" for deterministic tests; use {@code Instant::now} in
   *     production
   */
  public KafkaOutageGapSource(Path dataDir, ObjectMapper mapper, Supplier<Instant> nowSupplier) {
    this.dataDir = dataDir;
    this.mapper = mapper;
    this.nowSupplier = nowSupplier;
  }

  @Override
  public String name() {
    return "KafkaOutageGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    Path cryptolakeDir = dataDir.resolve("cryptolake");
    if (!Files.exists(cryptolakeDir)) {
      return List.of();
    }

    List<GapRecord> result = new ArrayList<>();
    List<Path> collectorDirs = new ArrayList<>();
    try (var stream = Files.list(cryptolakeDir)) {
      stream.filter(Files::isDirectory).forEach(collectorDirs::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list cryptolake dirs under " + cryptolakeDir, e);
    }

    Instant now = nowSupplier.get();

    for (Path collectorDir : collectorDirs) {
      String collectorId = collectorDir.getFileName().toString();
      Path outageFile = collectorDir.resolve("kafka_outage.json");
      if (!Files.exists(outageFile)) {
        continue;
      }
      try {
        String content = Files.readString(outageFile, StandardCharsets.UTF_8).strip();
        if (content.isEmpty()) {
          continue;
        }
        ObjectNode node = (ObjectNode) mapper.readTree(content);
        if (!node.has("outage_started_at_ns")) {
          continue;
        }
        long outageNs = node.get("outage_started_at_ns").asLong();
        long outageMs = outageNs / 1_000_000L;
        long nowMs = now.toEpochMilli();

        // Overlap check: the outage window is [outageMs, nowMs].
        // Skip if the outage start is after the scope end.
        if (outageMs > scope.endMs()) {
          continue;
        }
        // Skip if "now" is before the scope start (degenerate: shouldn't happen in practice).
        if (nowMs < scope.startMs()) {
          continue;
        }

        // Clamp endMs to scope.endMs so downstream filters never see a record extending
        // beyond the audit window. `now()` may be later than the audit window if the audit
        // is run with --until in the past on an outage that's still active.
        long endMs = Math.min(nowMs, scope.endMs());

        String detail = "collector_id=" + collectorId + "; outage_started_at_ns=" + outageNs;

        result.addAll(fanOut(scope, "kafka_producer_outage", outageMs, endMs, detail));

      } catch (Exception e) {
        log.warn(
            "kafka_outage_gap_source_read_failed",
            "path",
            outageFile.toString(),
            "error",
            e.getMessage());
      }
    }

    return result;
  }

  // ── private helpers ───────────────────────────────────────────────────────

  /**
   * Fans out a single system-wide Kafka outage event into one {@link GapRecord} per {@code (symbol,
   * stream)} tuple in the scope. When the scope has no symbols or streams, falls back to a single
   * record with empty fields (preserving backward-compatible system-wide behaviour).
   *
   * <p>When fan-out actually expands to a non-empty symbol/stream, the detail string is annotated
   * with {@code ; fanout=true} so consumers can tell the record was derived from a system-wide
   * event.
   */
  private List<GapRecord> fanOut(
      AuditScope scope, String reason, long startMs, long endMs, String baseDetail) {
    List<String> symbols =
        (scope.symbols() == null || scope.symbols().isEmpty()) ? List.of("") : scope.symbols();
    List<String> streams =
        (scope.streams() == null || scope.streams().isEmpty()) ? List.of("") : scope.streams();
    String exchange =
        (scope.exchanges() != null && scope.exchanges().size() == 1)
            ? scope.exchanges().get(0)
            : "";

    boolean isFanout =
        symbols.size() > 1
            || streams.size() > 1
            || (symbols.size() == 1 && !symbols.get(0).isEmpty())
            || (streams.size() == 1 && !streams.get(0).isEmpty());
    String detail = isFanout ? baseDetail + "; fanout=true" : baseDetail;

    List<GapRecord> out = new ArrayList<>();
    for (String sym : symbols) {
      for (String str : streams) {
        out.add(new GapRecord(SOURCE_LABEL, exchange, sym, str, startMs, endMs, reason, detail));
      }
    }
    return out;
  }
}
