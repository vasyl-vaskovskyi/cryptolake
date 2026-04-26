package com.cryptolake.collector.gap;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DepthGapDetector}.
 *
 * <p>Ports {@code test_gap_detector.py} from Python test suite.
 */
class DepthGapDetectorTest {

  private DepthGapDetector detector;

  @BeforeEach
  void setUp() {
    detector = new DepthGapDetector();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_first_diff_after_sync_accepted
  void firstDiffAfterSyncAccepted() {
    detector.setSyncPoint(100L);
    // U <= lid+1 AND u >= lid+1: U=100, u=105, lid=100
    DiffValidationResult result = detector.validateDiff(100L, 105L, 99L);
    assertThat(result.valid()).isTrue();
    assertThat(result.isGap()).isFalse();
    assertThat(result.isStale()).isFalse();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_subsequent_diff_pu_chain_valid
  void subsequentDiffPuChainValid() {
    detector.setSyncPoint(100L);
    detector.validateDiff(100L, 105L, 99L); // first diff establishes lastU=105
    // Second diff: pu must equal 105
    DiffValidationResult result = detector.validateDiff(106L, 110L, 105L);
    assertThat(result.valid()).isTrue();
    assertThat(result.isGap()).isFalse();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_pu_chain_break_detected
  void puChainBreakDetected() {
    detector.setSyncPoint(100L);
    detector.validateDiff(100L, 105L, 99L);
    // pu should be 105 but we send 104 — chain break
    DiffValidationResult result = detector.validateDiff(106L, 110L, 104L);
    assertThat(result.valid()).isFalse();
    assertThat(result.isGap()).isTrue();
    assertThat(result.reason()).isEqualTo("pu_chain_break");
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_stale_diff_before_sync_rejected
  void staleDiffBeforeSyncRejected() {
    detector.setSyncPoint(100L);
    // u < lid (99 < 100) → stale
    DiffValidationResult result = detector.validateDiff(98L, 99L, 97L);
    assertThat(result.valid()).isFalse();
    assertThat(result.isStale()).isTrue();
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_no_sync_point_rejects
  void noSyncPointRejects() {
    DiffValidationResult result = detector.validateDiff(100L, 105L, 99L);
    assertThat(result.valid()).isFalse();
    assertThat(result.reason()).contains("no_sync_point");
  }

  @Test
  // ports: tests/unit/collector/test_gap_detector.py::test_reset_clears_state
  void resetClearsState() {
    detector.setSyncPoint(100L);
    detector.validateDiff(100L, 105L, 99L);
    detector.reset();
    // After reset: no sync point
    DiffValidationResult result = detector.validateDiff(100L, 105L, 99L);
    assertThat(result.valid()).isFalse();
    assertThat(result.reason()).contains("no_sync_point");
  }
}
