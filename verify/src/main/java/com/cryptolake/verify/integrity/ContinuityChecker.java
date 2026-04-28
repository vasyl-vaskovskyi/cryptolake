package com.cryptolake.verify.integrity;

import com.cryptolake.verify.archive.DecompressAndParse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Shared driver for stream continuity checks.
 *
 * <p>Streams records from each {@code .jsonl.zst} file, deduplicates in-memory by natural key, and
 * dispatches to the per-stream checker.
 *
 * <p>Tier 5 I2 — streaming decompress (no full-file load for large archives).
 *
 * <p>Thread safety: stateless per call.
 */
public final class ContinuityChecker {

  /** Streams for which continuity checking is supported. */
  public static final Set<String> CHECKABLE_STREAMS = Set.of("trades", "depth", "bookticker");

  private ContinuityChecker() {}

  /**
   * Runs a continuity check for the given stream across all provided files.
   *
   * @param stream stream type (e.g. "trades", "depth", "bookticker")
   * @param files archive files to check (sorted order)
   * @param mapper shared {@link ObjectMapper}
   * @return {@link IntegrityCheckResult}
   * @throws IOException on I/O failure
   */
  public static IntegrityCheckResult run(String stream, List<Path> files, ObjectMapper mapper)
      throws IOException {

    // Collect all envelopes from all files for this stream
    List<JsonNode> allEnvelopes = new ArrayList<>();
    for (Path file : files) {
      allEnvelopes.addAll(DecompressAndParse.parse(file, mapper));
    }

    Iterator<JsonNode> records = allEnvelopes.iterator();

    return switch (stream) {
      case "trades" -> TradesContinuity.check(records, mapper);
      case "depth" -> DepthContinuity.check(records, mapper);
      case "bookticker" -> BooktickerContinuity.check(records, mapper);
      default -> new IntegrityCheckResult(allEnvelopes.size(), List.of());
    };
  }
}
