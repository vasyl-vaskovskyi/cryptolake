// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_gap_reason_values
// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_checkpoint_lost_is_valid_gap_reason
package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class GapReasonsTest {

  @Test
  void validSetContainsExpected() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_gap_reason_values
    assertThat(GapReasons.VALID).contains("ws_disconnect");
    assertThat(GapReasons.VALID).contains("pu_chain_break");
    assertThat(GapReasons.VALID).contains("session_seq_skip");
    assertThat(GapReasons.VALID).contains("buffer_overflow");
    assertThat(GapReasons.VALID).contains("snapshot_poll_miss");
    assertThat(GapReasons.VALID).contains("collector_restart");
    assertThat(GapReasons.VALID).contains("restart_gap");
    assertThat(GapReasons.VALID).contains("recovery_depth_anchor");
  }

  @Test
  void checkpointLostValid() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_checkpoint_lost_is_valid_gap_reason
    assertThat(GapReasons.VALID).contains("checkpoint_lost");
  }

  @Test
  void requireValidThrowsOnInvalid() {
    assertThatThrownBy(() -> GapReasons.requireValid("not_a_valid_reason"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not_a_valid_reason");
  }

  @Test
  void requireValidPassesOnKnownReason() {
    // should not throw
    GapReasons.requireValid("ws_disconnect");
    GapReasons.requireValid("missing_hour");
  }
}
