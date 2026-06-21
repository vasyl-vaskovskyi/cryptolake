package com.cryptopanner.deploy;

import com.cryptopanner.common.deploy.DeployState;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes the ordered promote steps still to run (design doc §4.4), for a fresh promote or a
 * resume after a crash. Steps are idempotent (§4.5), so resuming re-runs only the markers after the
 * last one recorded in {@code history.jsonl}.
 */
public final class PromotePlan {

  private PromotePlan() {}

  /** Markers still to execute given the last recorded one ({@code null} = fresh promote). */
  public static List<DeployState> remainingSteps(DeployState lastMarker) {
    List<DeployState> out = new ArrayList<>();
    for (DeployState s : DeployState.values()) {
      if (lastMarker == null || s.ordinal() > lastMarker.ordinal()) {
        out.add(s);
      }
    }
    return out;
  }
}
