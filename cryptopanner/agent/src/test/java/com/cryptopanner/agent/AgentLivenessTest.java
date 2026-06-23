package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * §14.j test mode: without systemd (dev processes or docker containers), the agent must derive
 * component liveness from heartbeat freshness, so {@code /status} reports real states instead of a
 * blanket {@code down}. {@link AgentLiveness#heartbeatActive} synthesizes the systemd-active flag
 * that {@link HeartbeatState#classify} consumes.
 */
class AgentLivenessTest {

  private static final Duration DEGRADED = Duration.ofSeconds(15);
  private static final Duration STUCK = Duration.ofSeconds(60);

  @Test
  void freshHeartbeatIsActive() {
    assertTrue(AgentLiveness.heartbeatActive(Optional.of(Duration.ofSeconds(2)), STUCK));
  }

  @Test
  void absentHeartbeatIsNotActive() {
    assertFalse(AgentLiveness.heartbeatActive(Optional.empty(), STUCK));
  }

  @Test
  void heartbeatOlderThanStuckIsNotActive() {
    assertFalse(AgentLiveness.heartbeatActive(Optional.of(Duration.ofSeconds(90)), STUCK));
  }

  @Test
  void heartbeatExactlyAtStuckIsStillActive() {
    assertTrue(AgentLiveness.heartbeatActive(Optional.of(STUCK), STUCK));
  }

  // The synthesized flag must feed HeartbeatState.classify into the right states across the range.
  @Test
  void composedWithClassifyYieldsRunningDegradedDown() {
    // fresh → running
    assertEquals(ComponentState.RUNNING, classifyHeartbeatOnly(Optional.of(Duration.ofSeconds(2))));
    // between degraded and stuck → degraded
    assertEquals(
        ComponentState.DEGRADED, classifyHeartbeatOnly(Optional.of(Duration.ofSeconds(30))));
    // stale → down (no systemd to say "stuck but alive")
    assertEquals(ComponentState.DOWN, classifyHeartbeatOnly(Optional.of(Duration.ofSeconds(90))));
    // absent → down
    assertEquals(ComponentState.DOWN, classifyHeartbeatOnly(Optional.empty()));
  }

  private static ComponentState classifyHeartbeatOnly(Optional<Duration> age) {
    boolean active = AgentLiveness.heartbeatActive(age, STUCK);
    return HeartbeatState.classify(age.orElse(null), active, DEGRADED, STUCK);
  }
}
