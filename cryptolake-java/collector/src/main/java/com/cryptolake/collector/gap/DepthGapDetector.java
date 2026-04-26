package com.cryptolake.collector.gap;

/**
 * Validates depth diff pu-chain after synchronization with a snapshot.
 *
 * <p>Exact port of {@code DepthGapDetector} from {@code src/collector/gap_detector.py}. All
 * update-ID fields are {@code long} — never {@code int} (Tier 5 E1).
 *
 * <p>Thread safety: caller-confined (single virtual thread per socket). Not shared across sockets.
 */
public final class DepthGapDetector {

  /** Sentinel meaning "no value seen yet" — distinguishes from a genuine {@code 0}. */
  private static final long UNSET = Long.MIN_VALUE;

  private boolean synced = false;
  private long lastUpdateId = UNSET; // snapshot's lastUpdateId (sync point)
  private long lastU = UNSET; // last accepted diff's u

  /**
   * Sets the synchronization point from a depth snapshot's {@code lastUpdateId}.
   *
   * <p>Resets {@code lastU} so the next diff is treated as the first-after-sync.
   */
  public void setSyncPoint(long lastUpdateId) {
    this.synced = true;
    this.lastUpdateId = lastUpdateId;
    this.lastU = UNSET;
  }

  /**
   * Validates a depth diff ({@code U}, {@code u}, {@code pu}) against the pu-chain.
   *
   * <p>Returns:
   * <ul>
   *   <li>{@link DiffValidationResult#ok()} — diff is valid and accepted
   *   <li>{@link DiffValidationResult#stale()} — diff is before the sync point (discard silently)
   *   <li>{@link DiffValidationResult#invalid(String)} — not yet synced, or no sync diff found
   *   <li>{@link DiffValidationResult#puBreak(String)} — pu chain break (trigger resync)
   * </ul>
   */
  public DiffValidationResult validateDiff(long U, long u, long pu) {
    if (!synced) {
      return DiffValidationResult.invalid("no_sync_point");
    }

    // First diff after sync: find where this diff anchors to the snapshot
    if (lastU == UNSET) {
      if (lastUpdateId == UNSET) {
        return DiffValidationResult.invalid("no_sync_point");
      }
      long lid = lastUpdateId;
      if (u < lid) {
        return DiffValidationResult.staleDiff();
      }
      // Accept if U <= lid+1 AND u >= lid+1 (Python condition)
      if (U <= lid + 1 && u >= lid + 1) {
        lastU = u;
        return DiffValidationResult.ok();
      }
      return DiffValidationResult.invalid("no_sync_diff_found");
    }

    // Subsequent diffs: pu must equal the previous u
    if (pu != lastU) {
      return DiffValidationResult.puBreak("pu_chain_break");
    }
    lastU = u;
    return DiffValidationResult.ok();
  }

  /** Resets state completely — call on reconnect or resync trigger. */
  public void reset() {
    synced = false;
    lastUpdateId = UNSET;
    lastU = UNSET;
  }

  /** For testing: whether this detector is currently synced. */
  public boolean isSynced() {
    return synced;
  }
}
