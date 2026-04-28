package com.cryptolake.verify.validation;

import com.cryptolake.verify.archive.DecompressAndParse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Walks archived heartbeat envelopes to detect coverage gaps post-hoc.
 *
 * <p>For each {@code (symbol, stream)}, finds any window {@code > 30s} with no heartbeat from
 * EITHER source AND no data envelope. Reports as ERROR if no covering gap envelope exists.
 *
 * <p>This catches the case where the writer itself was down during the silence (so neither source's
 * silence triggered a synthetic gap at the time), but the gap is inferable post-hoc from archived
 * heartbeats.
 *
 * <p>Spec §12 Q2 resolution: heartbeats are archived at {@code
 * ${baseDir}/{exchange}/{symbol}/heartbeat/{date}/hour-NN.jsonl.zst}. If the heartbeat archive is
 * absent for a given {@code (symbol, stream, date)}, the walker returns no errors for that stream
 * (cannot infer silence without heartbeat evidence).
 *
 * <p>Task A4.2.
 */
public final class HeartbeatTimelineWalker {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatTimelineWalker.class);

  /** Silence threshold: gap > this → potential error. */
  public static final long SILENCE_THRESHOLD_NS = 30_000_000_000L; // 30 s

  private HeartbeatTimelineWalker() {}

  /**
   * Walks the heartbeat archives for the given {@code (exchange, symbol, stream, date)} and reports
   * any uncovered silence windows.
   *
   * <p>A silence window is: a continuous period with no heartbeat AND no data envelope from either
   * source. If any gap envelope in {@code gapEnvelopes} covers the window, it is not reported.
   *
   * @param baseDir archive base directory (e.g. {@code /data})
   * @param exchange exchange name (e.g. {@code "binance"})
   * @param symbol symbol (e.g. {@code "btcusdt"})
   * @param stream stream name (e.g. {@code "depth"})
   * @param date date string (e.g. {@code "2026-04-28"})
   * @param dataEnvelopes all data envelopes for this {@code (symbol, stream, date)} from all
   *     sources (used to distinguish "no data" from "data present but no heartbeat")
   * @param gapEnvelopes all gap envelopes for this {@code (symbol, stream, date)}
   * @param mapper Jackson mapper for parsing archive files
   * @return list of error strings (empty if no uncovered silence found or if heartbeat archives are
   *     absent)
   */
  public static List<String> walk(
      Path baseDir,
      String exchange,
      String symbol,
      String stream,
      String date,
      List<JsonNode> dataEnvelopes,
      List<JsonNode> gapEnvelopes,
      ObjectMapper mapper) {

    // Load heartbeat archives for both primary and backup sources
    List<JsonNode> primaryHeartbeats =
        loadHeartbeats(baseDir, exchange, symbol, stream, date, false, mapper);
    List<JsonNode> backupHeartbeats =
        loadHeartbeats(baseDir, exchange, symbol, stream, date, true, mapper);

    if (primaryHeartbeats.isEmpty() && backupHeartbeats.isEmpty()) {
      // No heartbeat archives — cannot detect silence post-hoc (heartbeat archival may not be
      // enabled yet, or this is a new deployment). Return no errors.
      return Collections.emptyList();
    }

    // Merge heartbeats from both sources and sort by received_at
    List<JsonNode> allHeartbeats = new ArrayList<>();
    allHeartbeats.addAll(primaryHeartbeats);
    allHeartbeats.addAll(backupHeartbeats);
    allHeartbeats.sort(java.util.Comparator.comparingLong(n -> n.path("received_at").asLong(0)));

    // Collect all timestamps (data + heartbeat) in chronological order
    List<Long> allTimestamps = new ArrayList<>();
    for (JsonNode hb : allHeartbeats) {
      allTimestamps.add(hb.path("received_at").asLong(0));
    }
    for (JsonNode data : dataEnvelopes) {
      allTimestamps.add(data.path("received_at").asLong(0));
    }
    Collections.sort(allTimestamps);

    if (allTimestamps.isEmpty()) {
      return Collections.emptyList();
    }

    // Find silence windows: gaps between consecutive timestamps > SILENCE_THRESHOLD_NS
    List<String> errors = new ArrayList<>();
    long prev = allTimestamps.get(0);
    for (int i = 1; i < allTimestamps.size(); i++) {
      long curr = allTimestamps.get(i);
      long gap = curr - prev;
      if (gap > SILENCE_THRESHOLD_NS) {
        // Check if any gap envelope covers this silence window
        boolean covered = isCoveredByGapEnvelope(gapEnvelopes, prev, curr);
        if (!covered) {
          errors.add(
              "heartbeat_silence_uncovered: "
                  + symbol
                  + "/"
                  + stream
                  + " gap_start="
                  + prev
                  + " gap_end="
                  + curr
                  + " duration_ms="
                  + (gap / 1_000_000L));
          log.debug(
              "uncovered_heartbeat_silence",
              "symbol",
              symbol,
              "stream",
              stream,
              "gap_ms",
              gap / 1_000_000L);
        }
      }
      prev = curr;
    }
    return Collections.unmodifiableList(errors);
  }

  // ── private helpers ───────────────────────────────────────────────────────

  /**
   * Loads heartbeat envelopes from the heartbeat archive directory for a given {@code (exchange,
   * symbol, stream, date)}.
   *
   * <p>Path pattern: {@code
   * ${baseDir}/${exchange}/${symbol}/heartbeat/${stream}/${date}/hour-*.jsonl.zst}
   */
  private static List<JsonNode> loadHeartbeats(
      Path baseDir,
      String exchange,
      String symbol,
      String stream,
      String date,
      boolean backup,
      ObjectMapper mapper) {
    String prefix = backup ? "backup." : "";
    // Heartbeat archive path: baseDir/binance/{symbol}/heartbeat/{stream}/{date}/
    Path dir =
        baseDir
            .resolve(prefix + exchange)
            .resolve(symbol)
            .resolve("heartbeat")
            .resolve(stream)
            .resolve(date);

    if (!Files.isDirectory(dir)) {
      return Collections.emptyList();
    }

    List<JsonNode> heartbeats = new ArrayList<>();
    try (var stream2 = Files.list(dir)) {
      stream2
          .filter(p -> p.getFileName().toString().endsWith(".jsonl.zst"))
          .sorted()
          .forEach(
              archiveFile -> {
                try {
                  heartbeats.addAll(DecompressAndParse.parse(archiveFile, mapper));
                } catch (Exception e) {
                  log.debug(
                      "heartbeat_archive_parse_error",
                      "file",
                      archiveFile,
                      "error",
                      e.getMessage());
                }
              });
    } catch (IOException e) {
      log.debug("heartbeat_archive_list_error", "dir", dir, "error", e.getMessage());
    }
    return heartbeats;
  }

  /**
   * Returns {@code true} if any gap envelope's window covers the silence period {@code [gapStart,
   * gapEnd]}.
   */
  private static boolean isCoveredByGapEnvelope(
      List<JsonNode> gapEnvelopes, long silenceStart, long silenceEnd) {
    for (JsonNode gap : gapEnvelopes) {
      long envGapStart = gap.path("gap_start_ts").asLong(Long.MAX_VALUE);
      long envGapEnd = gap.path("gap_end_ts").asLong(0);
      // Envelope covers if it overlaps with the silence window
      if (envGapStart <= silenceEnd && envGapEnd >= silenceStart) {
        return true;
      }
    }
    return false;
  }
}
