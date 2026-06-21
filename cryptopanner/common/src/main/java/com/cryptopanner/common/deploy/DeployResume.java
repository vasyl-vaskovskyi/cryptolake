package com.cryptopanner.common.deploy;

/**
 * Decides how {@code cryptopanner-deploy resume} should proceed after a crash, from the last
 * recorded {@link DeployState} marker (design doc §4.5). Before the active-slot flip the deploy can
 * be aborted (old slot is intact); at or after it, the only safe direction is forward.
 */
public final class DeployResume {

  private DeployResume() {}

  public enum Action {
    /** No deploy in progress — nothing to resume. */
    FRESH,
    /**
     * Crash before the point of no return — operator may abort (revert to OLD) or resume forward.
     */
    ABORTABLE,
    /**
     * Crash at/after the active-slot flip — resume is always forward; there is no path back to OLD.
     */
    FORWARD_ONLY,
    /** The deploy already reached PROMOTED. */
    COMPLETE
  }

  public static Action decide(DeployState lastMarker) {
    if (lastMarker == null) {
      return Action.FRESH;
    }
    if (lastMarker == DeployState.PROMOTED) {
      return Action.COMPLETE;
    }
    return lastMarker.isPastPointOfNoReturn() ? Action.FORWARD_ONLY : Action.ABORTABLE;
  }
}
