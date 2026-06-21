package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class HeartbeatStateTest {

  private static final Duration DEGRADED = Duration.ofSeconds(15);
  private static final Duration STUCK = Duration.ofSeconds(60);

  @Test
  void inactiveSystemdIsDownRegardlessOfHeartbeat() {
    assertEquals(
        ComponentState.DOWN,
        HeartbeatState.classify(Duration.ofSeconds(1), false, DEGRADED, STUCK));
  }

  @Test
  void freshHeartbeatIsRunning() {
    assertEquals(
        ComponentState.RUNNING,
        HeartbeatState.classify(Duration.ofSeconds(2), true, DEGRADED, STUCK));
  }

  @Test
  void boundaryAtDegradedThresholdIsStillRunning() {
    assertEquals(ComponentState.RUNNING, HeartbeatState.classify(DEGRADED, true, DEGRADED, STUCK));
  }

  @Test
  void betweenDegradedAndStuckIsDegraded() {
    assertEquals(
        ComponentState.DEGRADED,
        HeartbeatState.classify(Duration.ofSeconds(30), true, DEGRADED, STUCK));
  }

  @Test
  void pastStuckThresholdWhileActiveIsStuck() {
    assertEquals(
        ComponentState.STUCK,
        HeartbeatState.classify(Duration.ofSeconds(120), true, DEGRADED, STUCK));
  }

  @Test
  void missingHeartbeatWhileActiveIsStuck() {
    assertEquals(ComponentState.STUCK, HeartbeatState.classify(null, true, DEGRADED, STUCK));
  }
}
