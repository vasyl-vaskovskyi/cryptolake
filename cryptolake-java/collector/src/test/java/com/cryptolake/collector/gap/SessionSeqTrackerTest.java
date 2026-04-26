package com.cryptolake.collector.gap;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SessionSeqTracker}.
 *
 * <p>Ports {@code test_gap_detector.py::SessionSeqTracker} tests.
 */
class SessionSeqTrackerTest {

  private SessionSeqTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new SessionSeqTracker();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_session_seq_sequential_ok
  void sequentialOk() {
    assertThat(tracker.check(0)).isEmpty();
    assertThat(tracker.check(1)).isEmpty();
    assertThat(tracker.check(2)).isEmpty();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_session_seq_gap_detected
  void gapDetected() {
    tracker.check(0);
    var gap = tracker.check(5); // skipped 1-4
    assertThat(gap).isPresent();
    assertThat(gap.get().expected()).isEqualTo(1L);
    assertThat(gap.get().actual()).isEqualTo(5L);
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_session_seq_first_message_ok
  void firstMessageOk() {
    // First message: any seq is accepted
    assertThat(tracker.check(42)).isEmpty();
    // Second must be 43
    assertThat(tracker.check(43)).isEmpty();
    var gap = tracker.check(50);
    assertThat(gap).isPresent();
    assertThat(gap.get().expected()).isEqualTo(44L);
  }
}
