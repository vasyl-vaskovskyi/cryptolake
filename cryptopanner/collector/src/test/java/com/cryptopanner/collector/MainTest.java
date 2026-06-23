package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MainTest {

  @Test
  void nonPositiveMaxRuntimeRunsUntilSignal() {
    assertTrue(Main.runsUntilSignal(0), "0 means run until SIGTERM (production)");
    assertTrue(Main.runsUntilSignal(-1), "negative also means run until SIGTERM");
  }

  @Test
  void positiveMaxRuntimeUsesATimedRun() {
    assertFalse(Main.runsUntilSignal(120), "a positive runtime is the bounded smoke-test mode");
  }

  @Test
  void heartbeatFileMatchesThePerSlotPathTheAgentReads() {
    // The Node Agent's StatusBuilder reads /tmp/cryptopanner-collector@<slot>.heartbeat (§11.b);
    // the collector must write exactly that path or the agent sees it as permanently down.
    assertEquals(
        java.nio.file.Path.of("/tmp/cryptopanner-collector@a.heartbeat"), Main.heartbeatFile("a"));
    assertEquals(
        java.nio.file.Path.of("/tmp/cryptopanner-collector@b.heartbeat"), Main.heartbeatFile("b"));
  }

  @Test
  void slotDefaultsToAWhenUnset() {
    assertEquals("a", Main.resolveSlot(null));
    assertEquals("a", Main.resolveSlot("  "));
    assertEquals("b", Main.resolveSlot("b"));
  }

  @Test
  void rotationStatusFileMatchesThePerSlotPathTheAgentReads() {
    assertEquals(
        java.nio.file.Path.of("/tmp/cryptopanner-collector@a.rotation.json"),
        Main.rotationStatusFile("a"));
  }

  @Test
  void metricsTextIncludesTheSpecNamedRotationAndConnectionAgeMetrics() {
    String m = Main.metricsText(100, 0, 2, 1, 0, 5, 76800);
    // §11.c requires these two by name, in addition to the existing capture counters.
    assertTrue(
        m.contains("# TYPE cryptopanner_rotation_events_total counter\n")
            && m.contains("cryptopanner_rotation_events_total 5\n"),
        "rotation_events_total counter required by §11.c, was:\n" + m);
    assertTrue(
        m.contains("# TYPE cryptopanner_current_connection_age_seconds gauge\n")
            && m.contains("cryptopanner_current_connection_age_seconds 76800\n"),
        "current_connection_age_seconds gauge required by §11.c, was:\n" + m);
    // existing capture metrics still present
    assertTrue(m.contains("cryptopanner_frames_written_total 100\n"));
    assertTrue(m.contains("cryptopanner_late_frames_total 2\n"));
  }
}
