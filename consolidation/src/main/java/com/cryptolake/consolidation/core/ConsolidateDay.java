package com.cryptolake.consolidation.core;

import com.cryptolake.common.util.Sha256;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the full daily consolidation: discover → merge → write → verify → cleanup.
 *
 * <p>Algorithm: For each hour 0–23, parse and merge the hour's source files (or synthesize a {@code
 * missing_hour} gap envelope), stream the result into the daily zstd file, then release the parsed
 * batch. Hourly source files are deleted ONLY after verification passes.
 *
 * <p>Spec invariants honored: §Idempotency (skip if daily file exists), §Streaming Writer (peak
 * memory bounded to one hour), §Cleanup (delete-after-verify).
 *
 * <p>Thread safety: single-shot per (exchange, symbol, stream, date) tuple; no shared mutation.
 */
public final class ConsolidateDay {

  private static final Logger log = LoggerFactory.getLogger(ConsolidateDay.class);

  private ConsolidateDay() {}

  /** Consolidation result. */
  public record ConsolidateResult(
      boolean success,
      long totalRecords,
      long dataRecords,
      long gapRecords,
      int missingHours,
      int sourceFilesCount) {
    public static ConsolidateResult empty() {
      return new ConsolidateResult(true, 0L, 0L, 0L, 0, 0);
    }
  }

  /**
   * Runs the daily consolidation for one (exchange, symbol, stream, date).
   *
   * @param baseDir archive base directory
   * @param exchange exchange name
   * @param symbol symbol name
   * @param stream stream type
   * @param date date (YYYY-MM-DD)
   * @param mapper shared {@link ObjectMapper}
   * @return consolidation result
   * @throws IOException on I/O failure
   */
  public static ConsolidateResult run(
      Path baseDir, String exchange, String symbol, String stream, String date, ObjectMapper mapper)
      throws IOException {

    Path dateDir = baseDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);
    if (!Files.exists(dateDir)) {
      return ConsolidateResult.empty();
    }

    // Idempotency (spec §Idempotency): skip if the daily file already exists,
    // so a re-run cannot clobber a sealed archive with synthesized gaps.
    Path dailyPath = dateDir.resolve(date + ".jsonl.zst");
    if (Files.exists(dailyPath)) {
      log.info(
          "consolidation_skipped_existing",
          "exchange",
          exchange,
          "symbol",
          symbol,
          "stream",
          stream,
          "date",
          date);
      return ConsolidateResult.empty();
    }

    Map<Integer, HourFileGroup> groups = HourFileDiscovery.discover(dateDir);
    String sessionId = "consolidation-" + Instant.now();

    List<Integer> missingHourList = new ArrayList<>();
    List<Path> allOriginalFiles = new ArrayList<>();
    List<Path> allSidecars = new ArrayList<>();
    Map<Integer, DailyFileWriter.HourCounts> hourCounts = new HashMap<>();

    DailyFileWriter.WriteStats stats;
    try (DailyFileWriter writer = DailyFileWriter.open(dailyPath)) {
      // Stream hours 0–23 in order. Each hour's parsed envelopes are eligible for GC
      // before the next iteration begins (spec §"Streaming Writer").
      for (int h = 0; h < 24; h++) {
        HourFileGroup group = groups.get(h);
        List<JsonNode> hourBatch;

        if (group == null || group.isEmpty()) {
          missingHourList.add(h);
          log.info(
              "missing_hour_synthesized",
              "exchange",
              exchange,
              "symbol",
              symbol,
              "stream",
              stream,
              "date",
              date,
              "hour",
              h);
          byte[] gapBytes =
              MissingHourGapFactory.create(exchange, symbol, stream, date, h, sessionId, mapper);
          hourBatch = List.of(mapper.readTree(gapBytes));
        } else {
          hourBatch = HourMerger.merge(h, group, stream, mapper);
          collectSourceFiles(group, allOriginalFiles, allSidecars);
        }

        DailyFileWriter.HourCounts counts = writer.appendHour(hourBatch, mapper);
        hourCounts.put(h, counts);
      }
      stats = writer.stats();
    }

    // SHA-256 sidecar
    Path sidecarPath = dateDir.resolve(date + ".jsonl.zst.sha256");
    String hex = Sha256.hexFile(dailyPath);
    Files.writeString(
        sidecarPath, hex + "  " + dailyPath.getFileName() + "\n", StandardCharsets.UTF_8);

    // Manifest
    ManifestRecord manifest =
        buildManifest(
            exchange,
            symbol,
            stream,
            date,
            dailyPath.getFileName().toString(),
            hex,
            stats,
            missingHourList,
            groups,
            hourCounts,
            allOriginalFiles);
    Path manifestPath = dateDir.resolve(date + ".manifest.json");
    Files.writeString(
        manifestPath,
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest) + "\n",
        StandardCharsets.UTF_8);

    // Verify before cleanup (delete-after-verify discipline)
    DailyFileVerifier.VerifyResult verifyResult =
        DailyFileVerifier.verify(dailyPath, stats.totalRecords(), sidecarPath, stream, mapper);

    if (!verifyResult.success()) {
      log.error(
          "consolidation_verify_failed",
          "exchange",
          exchange,
          "symbol",
          symbol,
          "stream",
          stream,
          "date",
          date,
          "error",
          verifyResult.errorMessage());
      return new ConsolidateResult(
          false,
          stats.totalRecords(),
          stats.dataRecords(),
          stats.gapRecords(),
          missingHourList.size(),
          allOriginalFiles.size());
    }

    HourlyCleanup.delete(allOriginalFiles, allSidecars);

    return new ConsolidateResult(
        true,
        stats.totalRecords(),
        stats.dataRecords(),
        stats.gapRecords(),
        missingHourList.size(),
        allOriginalFiles.size());
  }

  private static void collectSourceFiles(
      HourFileGroup group, List<Path> originalFiles, List<Path> sidecars) {
    if (group.base() != null) {
      originalFiles.add(group.base());
      sidecars.add(group.base().resolveSibling(group.base().getFileName() + ".sha256"));
    }
    for (Path f : group.late()) {
      originalFiles.add(f);
      sidecars.add(f.resolveSibling(f.getFileName() + ".sha256"));
    }
    for (Path f : group.backfill()) {
      originalFiles.add(f);
      sidecars.add(f.resolveSibling(f.getFileName() + ".sha256"));
    }
  }

  private static ManifestRecord buildManifest(
      String exchange,
      String symbol,
      String stream,
      String date,
      String dailyFileName,
      String sha256,
      DailyFileWriter.WriteStats stats,
      List<Integer> missingHours,
      Map<Integer, HourFileGroup> groups,
      Map<Integer, DailyFileWriter.HourCounts> hourCounts,
      List<Path> allOriginalFiles) {

    LinkedHashMap<String, ManifestRecord.HourSummary> hours = new LinkedHashMap<>();
    for (int h = 0; h < 24; h++) {
      DailyFileWriter.HourCounts counts = hourCounts.get(h);
      if (counts == null || (counts.dataRecords() == 0 && counts.gapRecords() == 0)) {
        continue;
      }
      HourFileGroup group = groups.get(h);
      List<String> sources = new ArrayList<>();
      if (group != null) {
        if (group.base() != null) sources.add(group.base().getFileName().toString());
        group.late().forEach(f -> sources.add(f.getFileName().toString()));
        group.backfill().forEach(f -> sources.add(f.getFileName().toString()));
      }
      String status = group != null ? group.status() : "missing";
      hours.put(
          String.valueOf(h),
          new ManifestRecord.HourSummary(
              status, counts.dataRecords(), counts.gapRecords(), sources));
    }

    List<String> sourceFileNames = new ArrayList<>();
    for (Path f : allOriginalFiles) {
      sourceFileNames.add(f.getFileName().toString());
    }

    return new ManifestRecord(
        1,
        exchange,
        symbol,
        stream,
        date,
        Instant.now().toString(),
        dailyFileName,
        sha256,
        stats.totalRecords(),
        stats.dataRecords(),
        stats.gapRecords(),
        hours,
        missingHours,
        sourceFileNames);
  }
}
