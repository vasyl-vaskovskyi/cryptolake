package com.cryptolake.collector.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BackpressureGate}.
 *
 * <p>Ports tests from design §8.1 table.
 */
class BackpressureGateTest {

  @Test
  // ports: (new) BackpressureGateTest::dropsBelowThresholdDoNotPause
  void dropsBelowThresholdDoNotPause() {
    BackpressureGate gate = new BackpressureGate(5);
    for (int i = 0; i < 4; i++) {
      gate.onDrop();
    }
    assertThat(gate.shouldPause()).isFalse();
  }

  @Test
  // ports: (new) BackpressureGateTest::thresholdReachedPauses
  void thresholdReachedPauses() {
    BackpressureGate gate = new BackpressureGate(5);
    for (int i = 0; i < 5; i++) {
      gate.onDrop();
    }
    assertThat(gate.shouldPause()).isTrue();
  }

  @Test
  // ports: (new) BackpressureGateTest::recoveryClearsCounter
  void recoveryClearsCounter() {
    BackpressureGate gate = new BackpressureGate(3);
    gate.onDrop();
    gate.onDrop();
    gate.onDrop();
    assertThat(gate.shouldPause()).isTrue();
    gate.onRecovery();
    assertThat(gate.shouldPause()).isFalse();
  }
}
