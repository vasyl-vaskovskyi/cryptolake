package com.cryptolake.consolidation.core;

import com.cryptolake.verify.archive.DecompressAndParse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Merges all source files for a single hour into a sorted list of envelopes.
 *
 * <p>Ports {@code merge_hour} from {@code consolidate.py:125-149}. Fast path: single base file with
 * no extras returns as-is. Otherwise: parse all sources, separate data from gap envelopes, sort
 * data by natural key, splice gaps by {@code gap_start_ts}.
 *
 * <p>Thread safety: stateless per call; all state is local.
 */
public final class HourMerger {

  private HourMerger() {}

  /**
   * Merges all files for the given hour group into an ordered envelope list.
   *
   * @param hour hour index (0–23) for logging
   * @param group discovered file group
   * @param stream stream type (for {@link DataSortKey})
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return merged, sorted list of envelopes (data + spliced gaps)
   * @throws IOException on decompression failure
   */
  public static List<JsonNode> merge(
      int hour, HourFileGroup group, String stream, ObjectMapper mapper) throws IOException {

    // Fast path: single base file, no late or backfill files — already sorted by writer
    if (group.base() != null && group.late().isEmpty() && group.backfill().isEmpty()) {
      return DecompressAndParse.parse(group.base(), mapper);
    }

    // Parse all sources
    List<JsonNode> dataEnvelopes = new ArrayList<>();
    List<JsonNode> gapEnvelopes = new ArrayList<>();

    // Order: base first, then late (seq ascending), then backfill (seq ascending)
    List<java.nio.file.Path> allSources = new ArrayList<>();
    if (group.base() != null) {
      allSources.add(group.base());
    }
    allSources.addAll(group.late());
    allSources.addAll(group.backfill());

    for (java.nio.file.Path source : allSources) {
      for (JsonNode env : DecompressAndParse.parse(source, mapper)) {
        if ("gap".equals(env.path("type").asText())) {
          gapEnvelopes.add(env);
        } else {
          dataEnvelopes.add(env);
        }
      }
    }

    // Sort data by stream natural key (Tier 5 M5; Tier 5 E1 — DataSortKey returns long)
    dataEnvelopes.sort(Comparator.comparingLong(e -> DataSortKey.of(e, stream, mapper)));

    // Sort gaps by gap_start_ts
    gapEnvelopes.sort(Comparator.comparingLong(e -> e.path("gap_start_ts").asLong(0L)));

    // Splice gaps into the data stream by received_at / gap_start_ts
    // Matches consolidate.py:135-149: gaps are inserted before the first data record
    // whose received_at >= gap_start_ts
    List<JsonNode> result = new ArrayList<>();
    int gi = 0;
    for (JsonNode data : dataEnvelopes) {
      long dataTs = data.path("received_at").asLong(0L);
      while (gi < gapEnvelopes.size()) {
        long gapStartTs = gapEnvelopes.get(gi).path("gap_start_ts").asLong(Long.MAX_VALUE);
        if (gapStartTs <= dataTs) {
          result.add(gapEnvelopes.get(gi));
          gi++;
        } else {
          break;
        }
      }
      result.add(data);
    }
    // Append any remaining gaps
    while (gi < gapEnvelopes.size()) {
      result.add(gapEnvelopes.get(gi++));
    }

    return result;
  }
}
