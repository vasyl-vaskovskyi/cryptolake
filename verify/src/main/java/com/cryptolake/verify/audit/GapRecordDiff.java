package com.cryptolake.verify.audit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Exact-tuple diff between file-sourced gap records and PG-state gap records.
 *
 * <p>Only {@link RuntimeOnlyReasons#PERSISTENT_CLASS} reasons participate in the diff. Runtime-only
 * reasons are stripped from both sides before comparison — they are never expected in PG state and
 * never fire a divergence alert.
 *
 * <p>Two records match when their {@link GapRecord.DiffKey} is identical (exchange, symbol, stream,
 * startMs, endMs, reason). Duplicate keys on the state side are collapsed by keeping the first
 * occurrence.
 *
 * @deprecated Use {@link GapRecordReconciler} for the new interval-overlap + caused-by matching.
 *     This class is kept until R4 migrates the last caller in {@code AuditBackfillCommand}.
 */
@Deprecated
public final class GapRecordDiff {

  public record DiffResult(
      List<GapRecord> matched, List<GapRecord> onlyInFiles, List<GapRecord> onlyInState) {

    public boolean isClean() {
      return onlyInFiles.isEmpty() && onlyInState.isEmpty();
    }
  }

  public static DiffResult diff(List<GapRecord> files, List<GapRecord> state) {
    List<GapRecord> filesPersistent =
        files.stream()
            .filter(g -> RuntimeOnlyReasons.PERSISTENT_CLASS.contains(g.reason()))
            .toList();
    List<GapRecord> statePersistent =
        state.stream()
            .filter(g -> RuntimeOnlyReasons.PERSISTENT_CLASS.contains(g.reason()))
            .toList();

    Map<GapRecord.DiffKey, GapRecord> stateByKey =
        statePersistent.stream().collect(Collectors.toMap(GapRecord::diffKey, g -> g, (a, b) -> a));

    List<GapRecord> matched = new ArrayList<>();
    List<GapRecord> onlyInFiles = new ArrayList<>();
    Set<GapRecord.DiffKey> matchedKeys = new HashSet<>();

    for (GapRecord f : filesPersistent) {
      if (stateByKey.containsKey(f.diffKey())) {
        matched.add(f);
        matchedKeys.add(f.diffKey());
      } else {
        onlyInFiles.add(f);
      }
    }

    List<GapRecord> onlyInState =
        statePersistent.stream().filter(g -> !matchedKeys.contains(g.diffKey())).toList();

    return new DiffResult(matched, onlyInFiles, onlyInState);
  }

  private GapRecordDiff() {}
}
