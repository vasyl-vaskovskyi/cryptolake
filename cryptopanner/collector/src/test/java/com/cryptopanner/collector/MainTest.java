package com.cryptopanner.collector;

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
}
