package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RotationDeciderTest {

  private static final int MAX_FAILS = 3;

  @Test
  void shadowHealthyOnlyWhenAHighFreqFrameSeen() {
    assertTrue(RotationDecider.shadowHealthy(true));
    assertFalse(RotationDecider.shadowHealthy(false), "no frame within probe window = half-open");
  }

  @Test
  void equivalencePassCutsOver() {
    assertEquals(
        RotationDecider.Action.CUTOVER,
        RotationDecider.decideCutover(true, 0, MAX_FAILS, false, false));
  }

  @Test
  void equivalenceFailBelowThresholdWaits() {
    // one fail so far (becomes the 1st); re-verify next minute.
    assertEquals(
        RotationDecider.Action.WAIT,
        RotationDecider.decideCutover(false, 0, MAX_FAILS, false, false));
  }

  @Test
  void equivalenceFailAtThresholdForcesCutover() {
    // two prior fails + this one = 3 → can't defer past the 24h cliff.
    assertEquals(
        RotationDecider.Action.FORCE_CUTOVER,
        RotationDecider.decideCutover(false, 2, MAX_FAILS, false, false));
  }

  @Test
  void oldConnectionDropTriggersEarlyCutoverEvenIfNotVerified() {
    assertEquals(
        RotationDecider.Action.EARLY_CUTOVER,
        RotationDecider.decideCutover(false, 0, MAX_FAILS, true, false));
  }

  @Test
  void bothConnectionsDropAbortsAndOutranksEverything() {
    assertEquals(
        RotationDecider.Action.ABORT,
        RotationDecider.decideCutover(true, 0, MAX_FAILS, true, true));
  }
}
