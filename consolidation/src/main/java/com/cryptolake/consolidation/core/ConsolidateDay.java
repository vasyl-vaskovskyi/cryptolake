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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the full daily consolidation: discover → merge → write → verify → cleanup.
 *
 * <p>Ports {@code consolidate_day} from {@code consolidate.py:303-440}.
 *
 * <p>Algorithm: For each hour 0–23, merge the hour files. For missing hours, emit a {@code
 * missing_hour} gap envelope (Tier 1 §5). Write the daily JSONL.zst, write the sidecar, verify, and
 * clean up hourlies ONLY if verify passes (Tier 1 §4 indirect).
 *
 * <p>Thread safety: single-shot per (exchange, symbol, stream, date) tuple; no shared mutation.
 */
public final class ConsolidateDay {

  private static final Logger log = LoggerFactory.getLogger(ConsolidateDay.class);

  private ConsolidateDay() {}

  /**
   * Consolidation result.
   *
   * <p>Tier 2 §12 — record.
   */
  public record ConsolidateResult(
      boolean success, long totalRecords, long dataRecords, long gapRecords, int missingHours) {
    public static ConsolidateResult empty() {
      return new ConsolidateResult(true, 0L, 0L, 0L, 0);
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
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
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

    Map<Integer, HourFileGroup> groups = HourFileDiscovery.discover(dateDir);
    String sessionId = "consolidation-" + UUID.randomUUID();

    // Build per-hour envelope lists
    List<List<JsonNode>> hourBatches = new ArrayList<>();
    List<Integer> missingHourList = new ArrayList<>();
    List<Path> allOriginalFiles = new ArrayList<>();
    List<Path> allSidecars = new ArrayList<>();

    // Process hours 0–23 in order
    for (int h = 0; h < 24; h++) {
      HourFileGroup group = groups.get(h);
      if (group == null || group.isEmpty()) {
        // Missing hour — emit a gap envelope (Tier 1 §5)
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
        JsonNode gapNode = mapper.readTree(gapBytes);
        hourBatches.add(List.of(gapNode));
      } else {
        List<JsonNode> merged = HourMerger.merge(h, group, stream, mapper);
        hourBatches.add(merged);

        // Track original files for cleanup
        if (group.base() != null) {
          allOriginalFiles.add(group.base());
          Path sidecar = group.base().resolveSibling(group.base().getFileName() + ".sha256");
          allSidecars.add(sidecar);
        }
        for (Path f : group.late()) {
          allOriginalFiles.add(f);
          allSidecars.add(f.resolveSibling(f.getFileName() + ".sha256"));
        }
        for (Path f : group.backfill()) {
          allOriginalFiles.add(f);
          allSidecars.add(f.resolveSibling(f.getFileName() + ".sha256"));
        }
      }
    }

    // Write daily file
    Path dailyPath = dateDir.resolve(date + ".jsonl.zst");
    DailyFileWriter.WriteStats stats = DailyFileWriter.write(dailyPath, hourBatches, mapper);

    // Write SHA-256 sidecar
    Path sidecarPath = dateDir.resolve(date + ".jsonl.zst.sha256");
    String hex = Sha256.hexFile(dailyPath);
    String sidecarContent = hex + "  " + dailyPath.getFileName() + "\n";
    Files.writeString(sidecarPath, sidecarContent, StandardCharsets.UTF_8);

    // Build manifest
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
            hourBatches,
            allOriginalFiles,
            mapper);
    Path manifestPath = dateDir.resolve(date + ".manifest.json");
    Files.writeString(
        manifestPath,
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest) + "\n",
        StandardCharsets.UTF_8);

    // Verify daily file (Tier 1 §4 indirect — only delete hourlies after verify passes)
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
      // Do NOT delete hourlies on verify failure (Tier 1 §4 indirect)
      return new ConsolidateResult(
          false,
          stats.totalRecords(),
          stats.dataRecords(),
          stats.gapRecords(),
          missingHourList.size());
    }

    // Cleanup hourly files — ONLY after verify passes
    HourlyCleanup.delete(allOriginalFiles, allSidecars);

    return new ConsolidateResult(
        true,
        stats.totalRecords(),
        stats.dataRecords(),
        stats.gapRecords(),
        missingHourList.size());
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
      List<List<JsonNode>> hourBatches,
      List<Path> allOriginalFiles,
      ObjectMapper mapper) {

    // Build per-hour summaries
    java.util.LinkedHashMap<String, ManifestRecord.HourSummary> hours =
        new java.util.LinkedHashMap<>();
    for (int h = 0; h < 24; h++) {
      List<JsonNode> batch = hourBatches.get(h);
      if (batch == null || batch.isEmpty()) {
        continue;
      }
      long dataRecs = batch.stream().filter(e -> !"gap".equals(e.path("type").asText())).count();
      long gapRecs = batch.stream().filter(e -> "gap".equals(e.path("type").asText())).count();
      HourFileGroup group = groups.get(h);
      List<String> sources = new ArrayList<>();
      if (group != null) {
        if (group.base() != null) sources.add(group.base().getFileName().toString());
        group.late().forEach(f -> sources.add(f.getFileName().toString()));
        group.backfill().forEach(f -> sources.add(f.getFileName().toString()));
      }
      String status = missingHours.contains(h) ? "missing" : "present";
      hours.put(
          String.valueOf(h), new ManifestRecord.HourSummary(status, dataRecs, gapRecs, sources));
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
        Instant.now().toString(), // Tier 5 F1
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
