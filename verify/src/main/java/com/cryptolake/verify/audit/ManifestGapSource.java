package com.cryptolake.verify.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link GapRecord}s from per-stream daily manifest files.
 *
 * <p>Manifest files are written by the consolidation module at {@code
 * <baseDir>/<exchange>/<symbol>/<stream>/<date>/<date>.manifest.json}. Each manifest is a JSON
 * object that includes a {@code missing_hours} field — a JSON array of integer hours (0..23) that
 * were absent when the consolidation ran.
 *
 * <p>For every missing hour that falls within the {@link AuditScope} time window, one {@link
 * GapRecord} is emitted with:
 *
 * <ul>
 *   <li>{@code source="manifest"}
 *   <li>{@code exchange/symbol/stream} from the path
 *   <li>{@code startMs} = hour start epoch ms
 *   <li>{@code endMs} = {@code startMs + 3_599_999} (matching {@link MissingHourGapSource})
 *   <li>{@code reason="missing_hour"}
 *   <li>{@code detail="manifest=<date>; hour=<h>"}
 * </ul>
 *
 * <p>Graceful degradation: dates with no manifest file are silently skipped. Manifests that fail to
 * parse are silently skipped.
 */
public final class ManifestGapSource implements GapSource {

  private static final Logger log = LoggerFactory.getLogger(ManifestGapSource.class);

  private static final String SOURCE_LABEL = "manifest";
  private static final String REASON = "missing_hour";
  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final long HOUR_MS = 3_600_000L;

  private final Path baseDir;
  private final ObjectMapper mapper;

  /**
   * Constructs a {@code ManifestGapSource}.
   *
   * @param baseDir archive root directory (e.g. {@code /data/archive})
   * @param mapper shared {@link ObjectMapper}
   */
  public ManifestGapSource(Path baseDir, ObjectMapper mapper) {
    this.baseDir = baseDir;
    this.mapper = mapper;
  }

  @Override
  public String name() {
    return "ManifestGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    if (!Files.exists(baseDir)) {
      return List.of();
    }

    List<GapRecord> result = new ArrayList<>();
    List<String> dates = datesInScope(scope);

    List<Path> exchangeDirs = new ArrayList<>();
    try (var stream = Files.list(baseDir)) {
      stream.filter(Files::isDirectory).forEach(exchangeDirs::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list exchange dirs under " + baseDir, e);
    }

    for (Path exchangeDir : exchangeDirs) {
      String exchange = exchangeDir.getFileName().toString();
      List<Path> symbolDirs = listDirs(exchangeDir);

      for (Path symbolDir : symbolDirs) {
        String symbol = symbolDir.getFileName().toString();
        List<Path> streamDirs = listDirs(symbolDir);

        for (Path streamDir : streamDirs) {
          String stream = streamDir.getFileName().toString();

          for (String date : dates) {
            Path dateDir = streamDir.resolve(date);
            if (!Files.isDirectory(dateDir)) {
              continue;
            }
            Path manifestFile = dateDir.resolve(date + ".manifest.json");
            if (!Files.exists(manifestFile)) {
              continue;
            }
            readManifest(manifestFile, date, exchange, symbol, stream, scope, result);
          }
        }
      }
    }

    return result;
  }

  // ── private helpers ───────────────────────────────────────────────────────

  private void readManifest(
      Path manifestFile,
      String date,
      String exchange,
      String symbol,
      String stream,
      AuditScope scope,
      List<GapRecord> result) {
    try {
      JsonNode root = mapper.readTree(manifestFile.toFile());
      JsonNode missingHours = root.path("missing_hours");
      if (missingHours.isMissingNode() || !missingHours.isArray()) {
        return;
      }

      long dayStartMs = dateStartMs(date);

      for (JsonNode hourNode : missingHours) {
        if (!hourNode.isInt()) {
          continue;
        }
        int h = hourNode.intValue();
        if (h < 0 || h > 23) {
          continue;
        }
        long hourStartMs = dayStartMs + (long) h * HOUR_MS;
        long hourEndMs = hourStartMs + HOUR_MS - 1; // 3_599_999 ms inclusive end

        // Skip if outside the scope time window
        if (hourEndMs < scope.startMs() || hourStartMs > scope.endMs()) {
          continue;
        }

        String detail = "manifest=" + date + "; hour=" + h;
        result.add(
            new GapRecord(
                SOURCE_LABEL, exchange, symbol, stream, hourStartMs, hourEndMs, REASON, detail));
      }
    } catch (Exception e) {
      log.warn(
          "manifest_gap_source_read_failed",
          "path",
          manifestFile.toString(),
          "error",
          e.getMessage());
    }
  }

  /** Returns all {@code YYYY-MM-DD} dates covered by the scope (UTC). */
  private static List<String> datesInScope(AuditScope scope) {
    LocalDate start = Instant.ofEpochMilli(scope.startMs()).atZone(ZoneOffset.UTC).toLocalDate();
    LocalDate end = Instant.ofEpochMilli(scope.endMs()).atZone(ZoneOffset.UTC).toLocalDate();
    List<String> dates = new ArrayList<>();
    for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
      dates.add(d.format(DATE_FMT));
    }
    return dates;
  }

  /** Returns the UTC epoch millis for midnight at the start of the given date. */
  private static long dateStartMs(String date) {
    return LocalDate.parse(date, DATE_FMT).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  /** Lists direct subdirectories of {@code dir}; returns empty list if dir doesn't exist. */
  private static List<Path> listDirs(Path dir) {
    List<Path> dirs = new ArrayList<>();
    if (!Files.exists(dir)) {
      return dirs;
    }
    try (var stream = Files.list(dir)) {
      stream.filter(Files::isDirectory).forEach(dirs::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list dirs under " + dir, e);
    }
    return dirs;
  }
}
