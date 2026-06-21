package com.cryptopanner.agent;

import java.time.Duration;

/**
 * Classifies a component's liveness from its heartbeat age and systemd active flag (master spec
 * §11.b). The thresholds are configurable (§15.b {@code agent.heartbeat}); defaults are 15s
 * (degraded) and 60s (stuck) against a 5s touch cadence.
 */
public final class HeartbeatState {

  private HeartbeatState() {}

  /**
   * @param heartbeatAge time since the heartbeat file was last touched, or {@code null} if there is
   *     no heartbeat file
   * @param systemdActive whether systemd reports the unit active
   * @param degradedThreshold mtime above which a component is {@code degraded}
   * @param stuckThreshold mtime above which an active component is {@code stuck}
   */
  public static ComponentState classify(
      Duration heartbeatAge,
      boolean systemdActive,
      Duration degradedThreshold,
      Duration stuckThreshold) {
    if (!systemdActive) {
      return ComponentState.DOWN;
    }
    if (heartbeatAge == null || heartbeatAge.compareTo(stuckThreshold) > 0) {
      return ComponentState.STUCK; // active but not progressing (no/stale heartbeat)
    }
    if (heartbeatAge.compareTo(degradedThreshold) <= 0) {
      return ComponentState.RUNNING;
    }
    return ComponentState.DEGRADED;
  }
}
