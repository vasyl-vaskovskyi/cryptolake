package com.cryptopanner.common.deploy;

/**
 * The ordered markers a JAR deploy promote records to {@code deploy/history.jsonl} (design doc
 * §4.4). {@link #ACTIVE_SLOT_FLIPPED} is the point of no return: a crash before it may be aborted
 * (the old slot restarts via {@code Restart=always}); at or after it, resume is always forward
 * (§4.5).
 */
public enum DeployState {
  PROMOTING_STARTED,
  OLD_STOPPED,
  ACTIVE_SLOT_FLIPPED,
  NEW_ROLE_FLIPPED,
  OVERLAP_MERGED,
  STAGING_DRAINED,
  PROMOTED;

  /** Whether this marker is at or beyond the active-slot flip — the irreversible cutover (§4.5). */
  public boolean isPastPointOfNoReturn() {
    return ordinal() >= ACTIVE_SLOT_FLIPPED.ordinal();
  }
}
