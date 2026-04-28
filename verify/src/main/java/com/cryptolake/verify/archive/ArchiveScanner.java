package com.cryptolake.verify.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Scans the archive directory tree for matching {@code .jsonl.zst} files.
 *
 * <p>Ports the rglob block in {@code verify.py:297-301}. Returns a sorted list of {@link
 * ArchiveFile} records matching the optional exchange/symbol/stream filters.
 *
 * <p>Tier 5 I7 — uses {@link Files#walk} with try-with-resources.
 *
 * <p>Thread safety: stateless utility; each call opens its own stream.
 */
public final class ArchiveScanner {

  // Matches: <anything>/<date>/hour-<N>[.<late|backfill>-<seq>].jsonl.zst
  private static final Pattern FILE_PATTERN =
      Pattern.compile(
          ".*/([^/]+)/([^/]+)/([^/]+)/([^/]+)/hour-\\d+(?:\\.(?:late|backfill)-\\d+)?\\.jsonl\\.zst$");

  private ArchiveScanner() {}

  /**
   * Walks {@code baseDir} and returns all matching archive files, sorted by path string (matches
   * Python's {@code sorted(set(...))} over Path objects).
   *
   * @param baseDir base archive directory
   * @param date required date filter (YYYY-MM-DD)
   * @param exchange optional exchange filter (null = no filter)
   * @param symbol optional symbol filter (null = no filter)
   * @param stream optional stream filter (null = no filter)
   * @return sorted list of matching archive files
   * @throws IOException if directory traversal fails
   */
  public static List<ArchiveFile> scan(
      Path baseDir, String date, String exchange, String symbol, String stream) throws IOException {
    List<ArchiveFile> results = new ArrayList<>();
    if (!Files.exists(baseDir)) {
      return results;
    }
    // Tier 5 I7: try-with-resources on the walk stream
    try (var walk = Files.walk(baseDir)) {
      walk.filter(Files::isRegularFile)
          .forEach(
              path -> {
                String pathStr = path.toString();
                var matcher = FILE_PATTERN.matcher(pathStr);
                if (!matcher.matches()) {
                  return;
                }
                // Decompose relative path: exchange/symbol/stream/date/filename
                // (at least 4 segments from base, depth ≥ 4)
                Path rel;
                try {
                  rel = baseDir.relativize(path);
                } catch (IllegalArgumentException e) {
                  return;
                }
                if (rel.getNameCount() < 4) {
                  return;
                }
                String fileExchange = rel.getName(0).toString();
                String fileSymbol = rel.getName(1).toString();
                String fileStream = rel.getName(2).toString();
                String fileDate = rel.getName(3).toString();

                // Date filter
                if (!date.equals(fileDate)) {
                  return;
                }
                // Optional filters
                if (exchange != null && !exchange.equals(fileExchange)) {
                  return;
                }
                if (symbol != null && !symbol.equals(fileSymbol)) {
                  return;
                }
                if (stream != null && !stream.equals(fileStream)) {
                  return;
                }
                results.add(new ArchiveFile(path, fileExchange, fileSymbol, fileStream, fileDate));
              });
    }
    // Sort by path string — matches Python's sorted() over Path objects (lex order)
    results.sort(Comparator.comparing(f -> f.path().toString()));
    return results;
  }
}
