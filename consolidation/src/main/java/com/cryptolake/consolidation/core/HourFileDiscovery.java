package com.cryptolake.consolidation.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Discovers and classifies hour archive files in a date directory.
 *
 * <p>Ports {@code discover_hour_files} from {@code consolidate.py:22-24}. Classifies files into
 * base, late, and backfill groups; sorts late and backfill by their sequence number (Tier 5 M15).
 *
 * <p>Thread safety: stateless utility.
 */
public final class HourFileDiscovery {

  // Matches: hour-{H}.jsonl.zst
  private static final Pattern RE_BASE = Pattern.compile("^hour-(\\d{1,2})\\.jsonl\\.zst$");

  // Matches: hour-{H}.late-{seq}.jsonl.zst (Tier 5 M15)
  private static final Pattern RE_LATE =
      Pattern.compile("^hour-(\\d{1,2})\\.late-(\\d+)\\.jsonl\\.zst$");

  // Matches: hour-{H}.backfill-{seq}.jsonl.zst
  private static final Pattern RE_BACKFILL =
      Pattern.compile("^hour-(\\d{1,2})\\.backfill-(\\d+)\\.jsonl\\.zst$");

  private HourFileDiscovery() {}

  /**
   * Discovers and groups all hour files in {@code dateDir}.
   *
   * @param dateDir date directory path ({@code .../exchange/symbol/stream/YYYY-MM-DD/})
   * @return map from hour index (0–23) to {@link HourFileGroup}
   * @throws IOException on directory traversal failure
   */
  public static Map<Integer, HourFileGroup> discover(Path dateDir) throws IOException {
    Map<Integer, Path> baseFiles = new HashMap<>();
    Map<Integer, List<long[]>> lateFiles = new HashMap<>(); // hour → [[seq, idx]]
    Map<Integer, List<long[]>> backfillFiles = new HashMap<>();
    Map<Integer, Map<Long, Path>> latePaths = new HashMap<>();
    Map<Integer, Map<Long, Path>> backfillPaths = new HashMap<>();

    if (!Files.exists(dateDir)) {
      return Map.of();
    }

    try (var stream = Files.list(dateDir)) {
      stream.forEach(
          f -> {
            String name = f.getFileName().toString();
            Matcher mBase = RE_BASE.matcher(name);
            Matcher mLate = RE_LATE.matcher(name);
            Matcher mBackfill = RE_BACKFILL.matcher(name);

            if (mBase.matches()) {
              int h = Integer.parseInt(mBase.group(1));
              baseFiles.put(h, f);
            } else if (mLate.matches()) {
              int h = Integer.parseInt(mLate.group(1));
              long seq = Long.parseLong(mLate.group(2));
              lateFiles.computeIfAbsent(h, k -> new ArrayList<>()).add(new long[] {seq});
              latePaths.computeIfAbsent(h, k -> new HashMap<>()).put(seq, f);
            } else if (mBackfill.matches()) {
              int h = Integer.parseInt(mBackfill.group(1));
              long seq = Long.parseLong(mBackfill.group(2));
              backfillFiles.computeIfAbsent(h, k -> new ArrayList<>()).add(new long[] {seq});
              backfillPaths.computeIfAbsent(h, k -> new HashMap<>()).put(seq, f);
            }
          });
    }

    // Collect all hours that appear in any list
    java.util.Set<Integer> allHours = new java.util.HashSet<>();
    allHours.addAll(baseFiles.keySet());
    allHours.addAll(lateFiles.keySet());
    allHours.addAll(backfillFiles.keySet());

    Map<Integer, HourFileGroup> result = new HashMap<>();
    for (int h : allHours) {
      Path base = baseFiles.get(h);

      // Sort late by seq ascending (Tier 5 M15)
      List<Long> lateSeqs = new ArrayList<>();
      if (lateFiles.containsKey(h)) {
        for (long[] arr : lateFiles.get(h)) {
          lateSeqs.add(arr[0]);
        }
      }
      lateSeqs.sort(Comparator.naturalOrder());
      List<Path> lateSorted = new ArrayList<>();
      if (latePaths.containsKey(h)) {
        for (long seq : lateSeqs) {
          lateSorted.add(latePaths.get(h).get(seq));
        }
      }

      // Sort backfill by seq ascending
      List<Long> backfillSeqs = new ArrayList<>();
      if (backfillFiles.containsKey(h)) {
        for (long[] arr : backfillFiles.get(h)) {
          backfillSeqs.add(arr[0]);
        }
      }
      backfillSeqs.sort(Comparator.naturalOrder());
      List<Path> backfillSorted = new ArrayList<>();
      if (backfillPaths.containsKey(h)) {
        for (long seq : backfillSeqs) {
          backfillSorted.add(backfillPaths.get(h).get(seq));
        }
      }

      result.put(h, new HourFileGroup(base, lateSorted, backfillSorted));
    }

    return result;
  }
}
