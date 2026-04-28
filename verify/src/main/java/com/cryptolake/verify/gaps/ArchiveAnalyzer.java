package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Analyzes archive directories to identify missing hours and backfillable gaps.
 *
 * <p>Ports {@code analyze_archive} and {@code find_backfillable_gaps} from {@code gaps.py}. Uses
 * the archive directory structure to detect which hours are missing for a given
 * exchange/symbol/stream/date.
 *
 * <p>Thread safety: stateless utility.
 */
public final class ArchiveAnalyzer {

  private static final Pattern HOUR_PATTERN =
      Pattern.compile("^hour-(\\d{1,2})(?:\\.(?:late|backfill)-\\d+)?\\.jsonl\\.zst$");

  private ArchiveAnalyzer() {}

  /**
   * Finds missing hours for the given archive dimensions.
   *
   * @param baseDir archive base directory
   * @param exchange exchange name
   * @param symbol symbol name
   * @param stream stream type
   * @param date date (YYYY-MM-DD)
   * @return map of stream → list of missing hour indices (0–23)
   * @throws IOException on directory traversal failure
   */
  public static Map<String, List<Integer>> findGaps(
      Path baseDir, String exchange, String symbol, String stream, String date) throws IOException {
    Map<String, List<Integer>> result = new LinkedHashMap<>();
    Path dateDir = baseDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);

    if (!Files.exists(dateDir)) {
      List<Integer> allMissing = new ArrayList<>();
      for (int h = 0; h < 24; h++) {
        allMissing.add(h);
      }
      result.put(stream, allMissing);
      return result;
    }

    // Collect present hours
    boolean[] present = new boolean[24];
    try (var files = Files.list(dateDir)) {
      files.forEach(
          f -> {
            Matcher m = HOUR_PATTERN.matcher(f.getFileName().toString());
            if (m.matches()) {
              int h = Integer.parseInt(m.group(1));
              if (h >= 0 && h < 24) {
                present[h] = true;
              }
            }
          });
    }

    List<Integer> missing = new ArrayList<>();
    for (int h = 0; h < 24; h++) {
      if (!present[h]) {
        missing.add(h);
      }
    }
    result.put(stream, missing);
    return result;
  }

  /**
   * Returns the subset of missing hours that are backfillable (stream in {@link
   * GapStreams#BACKFILLABLE}).
   *
   * @param baseDir archive base directory
   * @param exchange exchange name
   * @param symbol symbol name
   * @param stream stream type
   * @param date date (YYYY-MM-DD)
   * @param mapper shared {@link ObjectMapper}
   * @return list of missing hour indices that can be backfilled
   * @throws IOException on I/O failure
   */
  public static List<Integer> findBackfillable(
      Path baseDir, String exchange, String symbol, String stream, String date, ObjectMapper mapper)
      throws IOException {
    if (!GapStreams.BACKFILLABLE.contains(stream)) {
      return List.of();
    }
    Map<String, List<Integer>> gaps = findGaps(baseDir, exchange, symbol, stream, date);
    return gaps.getOrDefault(stream, List.of());
  }

  /** Returns all archive files for the given date dir, sorted. */
  public static List<Path> listArchiveFiles(Path dateDir) throws IOException {
    if (!Files.exists(dateDir)) {
      return List.of();
    }
    List<Path> files = new ArrayList<>();
    try (var stream = Files.list(dateDir)) {
      stream.filter(f -> f.getFileName().toString().endsWith(".jsonl.zst")).forEach(files::add);
    }
    files.sort(Comparator.comparing(p -> p.getFileName().toString()));
    return files;
  }
}
