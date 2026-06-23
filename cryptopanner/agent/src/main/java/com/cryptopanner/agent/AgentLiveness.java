package com.cryptopanner.agent;

import java.time.Duration;
import java.util.Optional;

/**
 * Synthesizes the systemd-active flag from heartbeat freshness for the agent's no-systemd test mode
 * (master spec §14.j). When the node runs without systemd — local dev processes or docker
 * containers — {@code systemctl is-active} is unavailable, so {@link HeartbeatState#classify} would
 * report every component {@code down}. In that mode the agent instead treats a component as
 * "active" iff its heartbeat file exists and is no older than the stuck threshold; feeding that
 * into the unchanged {@link HeartbeatState#classify} then yields {@code running} / {@code degraded}
 * by age and {@code down} for an absent or stale heartbeat.
 *
 * <p>Production (systemd present) is unaffected — the agent keeps using {@link Systemctl#isActive}.
 */
public final class AgentLiveness {

  private AgentLiveness() {}

  /**
   * True when a heartbeat exists and is within the stuck threshold (the no-systemd active flag).
   */
  public static boolean heartbeatActive(Optional<Duration> heartbeatAge, Duration stuckThreshold) {
    return heartbeatAge.isPresent() && heartbeatAge.get().compareTo(stuckThreshold) <= 0;
  }
}
