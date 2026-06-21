package com.cryptopanner.collector;

/**
 * Pure decision logic for a WS rotation overlap (design doc §5.2, §5.5). The {@code
 * WsConnectionManager} drives the actual shadow open / liveness probe / lock / merge I/O
 * (soak-tested per §14.e); the branching that decides <em>whether and how</em> to cut over lives
 * here so it can be unit-tested exhaustively.
 */
public final class RotationDecider {

  private RotationDecider() {}

  public enum Action {
    /** Equivalence not yet passing but still within the retry budget — re-verify next minute. */
    WAIT,
    /** Shadow is half-open — close it and retry the rotation later. */
    RETRY_SHADOW,
    /** Equivalence passed — cut over cleanly at the next minute boundary. */
    CUTOVER,
    /** Retry budget exhausted — cut over anyway (verify_result FORCED), 24h cliff cannot wait. */
    FORCE_CUTOVER,
    /**
     * Old connection dropped mid-overlap — promote the shadow now (reason SCHEDULED, early=true).
     */
    EARLY_CUTOVER,
    /** Both connections dropped — abandon the rotation (verify_result ABORTED). */
    ABORT
  }

  /**
   * Liveness probe (§5.2 step 2): the shadow is healthy iff it saw a high-frequency frame in time.
   */
  public static boolean shadowHealthy(boolean sawHighFreqFrame) {
    return sawHighFreqFrame;
  }

  /**
   * Decides the cutover action during the overlap-verify phase.
   *
   * @param equivalencePass whether the latest equivalence check passed
   * @param priorConsecutiveFails equivalence failures before this evaluation
   * @param maxFails failure budget before a forced cutover (default 3)
   * @param oldConnectionDropped the old primary WS connection dropped during overlap
   * @param bothConnectionsDropped both connections dropped during overlap
   */
  public static Action decideCutover(
      boolean equivalencePass,
      int priorConsecutiveFails,
      int maxFails,
      boolean oldConnectionDropped,
      boolean bothConnectionsDropped) {
    if (bothConnectionsDropped) {
      return Action.ABORT;
    }
    if (oldConnectionDropped) {
      return Action.EARLY_CUTOVER;
    }
    if (equivalencePass) {
      return Action.CUTOVER;
    }
    return (priorConsecutiveFails + 1 >= maxFails) ? Action.FORCE_CUTOVER : Action.WAIT;
  }
}
