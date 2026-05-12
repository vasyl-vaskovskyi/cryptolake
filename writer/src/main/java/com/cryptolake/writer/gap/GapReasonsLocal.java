package com.cryptolake.writer.gap;

/**
 * Writer-private string constants for coverage-filter metric labels (design §2.8).
 *
 * <p>Unrelated to the gap-envelope vocabulary in {@link com.cryptolake.common.envelope.GapReason} —
 * these labels describe how a gap record was <em>handled</em> by the coverage filter, not the cause
 * of the gap.
 */
public final class GapReasonsLocal {

  private GapReasonsLocal() {}

  /** Gap produced because a gap on one source was covered by the other source. */
  public static final String COVERED = "covered";

  /** Gap parked in the coverage filter (pending grace period). */
  public static final String PARKED = "parked";

  /** Gap suppressed because both sources reported a gap (no coverage). */
  public static final String UNCOVERED = "uncovered";

  /** Gap coalesced with a later arriving gap for the same key. */
  public static final String COALESCED = "coalesced";
}
