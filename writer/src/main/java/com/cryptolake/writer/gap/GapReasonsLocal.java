package com.cryptolake.writer.gap;

/**
 * Writer-private helpers on top of {@link com.cryptolake.common.envelope.GapReasons}.
 *
 * <p>Provides string constants used by the writer for coverage-filter reason labels and metrics
 * (design §2.8). No enum — reasons are strings for archival compatibility (Tier 5 M6).
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
