package com.cryptopanner.collector;

import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.EquivalenceChecker;
import com.cryptopanner.common.OverlapMerger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * Executes the cutover of a single rotation/deploy overlap minute (design doc §5.2 step 5): verify
 * the primary vs shadow segment, and on PASS (or a forced cutover) merge them into the primary,
 * delete the shadow, and append a rotation event. The caller (WsConnectionManager) owns the WS
 * lifecycle, the {@code .fs-heavy.lock}, and the minute-boundary timing; this is the deterministic,
 * file-level heart that composes EquivalenceChecker + OverlapMerger + DurableSegment +
 * RotationsLog.
 *
 * <p>On a non-forced equivalence FAIL nothing is changed (the caller re-verifies next minute); on a
 * forced cutover the merge proceeds and the event records {@code verify_result: "FORCED"}.
 */
public final class RotationExecutor {

  private final ObjectMapper mapper;
  private final EquivalenceChecker equivalence;
  private final OverlapMerger merger;

  public RotationExecutor(ObjectMapper mapper) {
    this.mapper = mapper;
    this.equivalence = new EquivalenceChecker(mapper);
    this.merger = new OverlapMerger(mapper);
  }

  /** Result of a minute cutover: the recorded verify result, merged size, and any divergences. */
  public record Outcome(String verifyResult, int mergedRecordCount, List<String> divergences) {}

  public Outcome cutoverMinute(
      String stream,
      Path primaryFile,
      Path shadowFile,
      Path rotationsLog,
      String rotationId,
      String reason,
      double oldConnectionAgeHours,
      Instant promotedAt,
      int minuteOfHour,
      boolean forceOnFail)
      throws IOException {
    List<String> primaryLines = DurableSegment.readLines(primaryFile);
    List<String> shadowLines = DurableSegment.readLines(shadowFile);

    EquivalenceChecker.Result eq = equivalence.check(stream, primaryLines, shadowLines);
    if (!eq.pass() && !forceOnFail) {
      // Deferred fail: leave both files in place and record nothing — caller re-verifies next
      // minute.
      return new Outcome("FAIL", 0, List.of());
    }
    String verifyResult = eq.pass() ? "PASS" : "FORCED";

    OverlapMerger.Merged merged = merger.merge(stream, primaryLines, shadowLines);
    DurableSegment.writeLines(primaryFile, merged.lines());
    Files.deleteIfExists(shadowFile);
    Files.deleteIfExists(shadowFile.resolveSibling(shadowFile.getFileName() + ".sha256"));

    RotationsLog.append(
        rotationsLog,
        mapper,
        new RotationsLog.RotationEvent(
            rotationId,
            reason,
            oldConnectionAgeHours,
            promotedAt,
            List.of(minuteOfHour),
            verifyResult));

    return new Outcome(verifyResult, merged.lines().size(), merged.divergences());
  }
}
