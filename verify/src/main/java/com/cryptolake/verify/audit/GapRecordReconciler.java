package com.cryptolake.verify.audit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Matches file-side gap records against state-side gap records using interval-overlap and
 * reason-equivalence (see {@link ReasonCausedBy}).
 *
 * <p>Replaces the old exact-tuple diff, which could not bridge the 1-to-N relationship between a
 * single state event (one PG row for a planned restart) and the N file-side gap envelopes it
 * produces — one per active stream.
 *
 * <h3>Matching algorithm</h3>
 *
 * <p>For each file record {@code F}, find the first state record {@code S} satisfying all of:
 *
 * <ol>
 *   <li>Same (exchange, symbol, stream) tuple.
 *   <li>{@link ReasonCausedBy#explains(String, String) ReasonCausedBy.explains(S.reason, F.reason)}
 *       is {@code true}.
 *   <li>The time windows overlap within {@code toleranceMs}: {@code F.endMs >= S.startMs -
 *       toleranceMs && F.startMs <= S.endMs + toleranceMs}.
 * </ol>
 *
 * <p>File records with no match are {@link ReconcileResult#unexplained() unexplained} — they fail
 * the audit gate. State records that were never used to explain any file record are {@link
 * ReconcileResult#orphanState() orphanState} — this is informational only; a fast restart that
 * emits no gap envelope is expected behaviour.
 *
 * <p>Note: the old "RUNTIME_ONLY = silently ignored" carve-out is GONE here. Every file gap must
 * either match a cause or be flagged.
 */
public final class GapRecordReconciler {

  /** Default time tolerance for window-overlap check: ±2 s. */
  public static final long DEFAULT_TOLERANCE_MS = 2_000L;

  /**
   * A file record that was successfully matched to a state record.
   *
   * @param file the file-side {@link GapRecord}
   * @param state the state-side {@link GapRecord} that explains it
   */
  public record ExplainedRecord(GapRecord file, GapRecord state) {}

  /**
   * The output of {@link #reconcile}.
   *
   * @param explained file records that were matched to a state cause
   * @param unexplained file records with no matching state record — these fail the audit gate
   * @param orphanState state records that were never used to explain a file record — informational
   *     only; a restart so fast it produced no gap envelope is fine
   */
  public record ReconcileResult(
      List<ExplainedRecord> explained, List<GapRecord> unexplained, List<GapRecord> orphanState) {

    /**
     * Returns {@code true} iff there are no unexplained file records. Orphan state records do
     * <em>not</em> fail the gate.
     */
    public boolean isClean() {
      return unexplained.isEmpty();
    }
  }

  /**
   * Reconciles {@code files} against {@code state} using {@link #DEFAULT_TOLERANCE_MS}.
   *
   * @see #reconcile(List, List, long)
   */
  public static ReconcileResult reconcile(List<GapRecord> files, List<GapRecord> state) {
    return reconcile(files, state, DEFAULT_TOLERANCE_MS);
  }

  /**
   * Reconciles {@code files} against {@code state} with a custom time tolerance.
   *
   * @param files file-side gap records (source {@code "file.*"})
   * @param state state-side gap records (source {@code "pg.*"}, {@code "ledger"}, etc.)
   * @param toleranceMs milliseconds of slack added to both ends of each state window when testing
   *     overlap with a file window
   * @return a {@link ReconcileResult} with three disjoint lists
   */
  public static ReconcileResult reconcile(
      List<GapRecord> files, List<GapRecord> state, long toleranceMs) {
    List<ExplainedRecord> explained = new ArrayList<>();
    List<GapRecord> unexplained = new ArrayList<>();
    // Track which state records have been used (by index) to compute orphans.
    Set<Integer> usedStateIndices = new HashSet<>();

    for (GapRecord f : files) {
      boolean matched = false;
      for (int i = 0; i < state.size(); i++) {
        GapRecord s = state.get(i);
        if (matches(f, s, toleranceMs)) {
          explained.add(new ExplainedRecord(f, s));
          usedStateIndices.add(i);
          matched = true;
          break;
        }
      }
      if (!matched) {
        unexplained.add(f);
      }
    }

    List<GapRecord> orphanState = new ArrayList<>();
    for (int i = 0; i < state.size(); i++) {
      if (!usedStateIndices.contains(i)) {
        orphanState.add(state.get(i));
      }
    }

    return new ReconcileResult(
        List.copyOf(explained), List.copyOf(unexplained), List.copyOf(orphanState));
  }

  /**
   * Returns {@code true} iff state record {@code s} can explain file record {@code f}.
   *
   * <p>All three conditions must hold:
   *
   * <ol>
   *   <li>Same (exchange, symbol, stream) tuple.
   *   <li>{@link ReasonCausedBy#explains} returns {@code true} for (s.reason, f.reason).
   *   <li>Time windows overlap: {@code f.endMs >= s.startMs - toleranceMs && f.startMs <= s.endMs +
   *       toleranceMs}.
   * </ol>
   */
  private static boolean matches(GapRecord f, GapRecord s, long toleranceMs) {
    // 1. Tuple check
    if (!f.exchange().equals(s.exchange())
        || !f.symbol().equals(s.symbol())
        || !f.stream().equals(s.stream())) {
      return false;
    }
    // 2. Reason-equivalence check
    if (!ReasonCausedBy.explains(s.reason(), f.reason())) {
      return false;
    }
    // 3. Interval-overlap check with tolerance
    return f.endMs() >= s.startMs() - toleranceMs && f.startMs() <= s.endMs() + toleranceMs;
  }

  private GapRecordReconciler() {}
}
