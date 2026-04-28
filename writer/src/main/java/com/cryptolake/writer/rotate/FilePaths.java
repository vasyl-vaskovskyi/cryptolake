package com.cryptolake.writer.rotate;

import java.nio.file.Path;
import java.util.Locale;

/**
 * Pure-function utilities for building archive file paths.
 *
 * <p>Ports Python's {@code file_rotator.py} path helpers (design §4.5; design §2.4). All methods
 * are static and have no side effects.
 *
 * <p>Symbol is lowercased at every call site (Tier 5 M1). {@code date} is in {@code YYYY-MM-DD}
 * format (Tier 5 F3). Late-sequence naming follows {@code hour-H.late-N.jsonl.zst} (Tier 5 M15).
 */
public final class FilePaths {

  private FilePaths() {}

  /**
   * Builds the canonical archive file path.
   *
   * <p>Pattern: {@code {baseDir}/{exchange}/{symbol}/{stream}/{date}/hour-{hour}.jsonl.zst} or
   * {@code hour-{hour}.late-{lateSeq}.jsonl.zst} when {@code lateSeq != null} (Tier 5 M15).
   *
   * @param baseDir base archive directory
   * @param exchange exchange name (e.g. {@code "binance"})
   * @param symbol symbol in LOWERCASE (caller's responsibility — Tier 5 M1)
   * @param stream stream type (e.g. {@code "trades"})
   * @param date date string {@code YYYY-MM-DD} (Tier 5 F3)
   * @param hour UTC hour 0–23
   * @param lateSeq null for primary file; positive integer for late-arrival files (Tier 5 M15)
   */
  public static Path buildFilePath(
      String baseDir,
      String exchange,
      String symbol,
      String stream,
      String date,
      int hour,
      Integer lateSeq) {
    String sym = symbol.toLowerCase(Locale.ROOT); // Tier 5 M1
    String filename =
        (lateSeq == null)
            ? "hour-" + hour + ".jsonl.zst"
            : "hour-" + hour + ".late-" + lateSeq + ".jsonl.zst"; // Tier 5 M15
    return Path.of(baseDir, exchange, sym, stream, date, filename);
  }

  /**
   * Builds the backfill file path.
   *
   * <p>Pattern: {@code
   * {baseDir}/backfill/{exchange}/{symbol}/{stream}/{date}/hour-{hour}.jsonl.zst}
   */
  public static Path buildBackfillFilePath(
      String baseDir, String exchange, String symbol, String stream, String date, int hour) {
    String sym = symbol.toLowerCase(Locale.ROOT);
    String filename = "hour-" + hour + ".jsonl.zst";
    return Path.of(baseDir, "backfill", exchange, sym, stream, date, filename);
  }

  /**
   * Returns the SHA-256 sidecar path for a given data file: {@code dataPath + ".sha256"} (Tier 5
   * I6).
   */
  public static Path sidecarPath(Path dataPath) {
    return dataPath.resolveSibling(dataPath.getFileName() + ".sha256");
  }
}
