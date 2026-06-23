package com.cryptopanner.monitor;

import java.util.List;

/**
 * The Monitor's latest view of one node, rebuilt each scrape cycle and served by {@link
 * MonitorServer} to {@code /api/nodes} and {@code /dashboard} (§11.d). Carries reachability, the
 * last parsed {@link StatusSnapshot} (null when unreachable), whether the node's restart breaker is
 * tripped, and the names of currently-active alert types (for dashboard banners/highlights).
 */
public record NodeView(
    String id,
    boolean reachable,
    String error,
    StatusSnapshot status,
    boolean circuitBreakerTripped,
    List<String> activeAlerts) {

  public static NodeView unreachable(String id, String error) {
    return new NodeView(id, false, error, null, false, List.of());
  }

  public static NodeView of(
      String id, StatusSnapshot status, boolean breakerTripped, List<String> activeAlerts) {
    return new NodeView(id, true, null, status, breakerTripped, List.copyOf(activeAlerts));
  }

  /** A deploy or WS rotation is mid-flight on this node (surfaces as a dashboard banner). */
  public boolean activityInProgress() {
    if (status == null) {
      return false;
    }
    boolean deploy = status.deployState() != null && !"IDLE".equals(status.deployState());
    boolean rotation = status.rotationState() != null && !"IDLE".equals(status.rotationState());
    return deploy || rotation;
  }

  /** Highlighted at the top of the dashboard: unreachable or breaker-tripped. */
  public boolean needsAttention() {
    return !reachable || circuitBreakerTripped;
  }
}
