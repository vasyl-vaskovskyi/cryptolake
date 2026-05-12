package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReason;
import java.util.List;
import org.junit.jupiter.api.Test;

class GapRecordReconcilerTest {

  private static GapRecord fileRecord(String stream, long startMs, long endMs, GapReason reason) {
    return new GapRecord("file.envelope", "binance", "btcusdt", stream, startMs, endMs, reason, "");
  }

  private static GapRecord stateRecord(String stream, long startMs, long endMs, GapReason reason) {
    return new GapRecord(
        "pg.component_runtime", "binance", "btcusdt", stream, startMs, endMs, reason, "");
  }

  // --- Case 1: restart explains ws_disconnect ---

  @Test
  void restartGapExplainsWsDisconnect() {
    // File: ws_disconnect in [100, 200]; State: restart_gap in [50, 300] — fully covers
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));
    var state = List.of(stateRecord("depth", 50, 300, GapReason.RESTART_GAP));

    var result = GapRecordReconciler.reconcile(file, state);

    assertThat(result.explained()).hasSize(1);
    assertThat(result.unexplained()).isEmpty();
    assertThat(result.orphanState()).isEmpty();
    assertThat(result.isClean()).isTrue();
  }

  // --- Case 2: tolerance ±2s ---

  @Test
  void toleranceAllowsOverlapWithin2s() {
    // State ends at 198; file ends at 200. Overlap check: F.startMs(100) <= S.endMs(198) + 2000 ✓
    // and F.endMs(200) >= S.startMs(50) - 2000 ✓ — should match with default tolerance
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));
    var state = List.of(stateRecord("depth", 50, 198, GapReason.RESTART_GAP));

    var withDefault = GapRecordReconciler.reconcile(file, state);
    assertThat(withDefault.explained()).hasSize(1);
    assertThat(withDefault.unexplained()).isEmpty();
  }

  @Test
  void zeroToleranceRejectsNonOverlap() {
    // State window [50, 98] ends before file window [100, 200] starts — no overlap at 0ms tolerance
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));
    var state = List.of(stateRecord("depth", 50, 98, GapReason.RESTART_GAP));

    var withZero = GapRecordReconciler.reconcile(file, state, 0L);
    assertThat(withZero.unexplained()).hasSize(1);
    assertThat(withZero.explained()).isEmpty();
    assertThat(withZero.orphanState()).hasSize(1);
  }

  // --- Case 3: unexplained — no matching state ---

  @Test
  void noStateRecordsProducesUnexplained() {
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));

    var result = GapRecordReconciler.reconcile(file, List.of());

    assertThat(result.unexplained()).hasSize(1);
    assertThat(result.explained()).isEmpty();
    assertThat(result.orphanState()).isEmpty();
    assertThat(result.isClean()).isFalse();
  }

  // --- Case 4: orphan state — restart with no file consequence ---

  @Test
  void stateWithNoFileConsequenceIsOrphan() {
    var state = List.of(stateRecord("depth", 100, 200, GapReason.RESTART_GAP));

    var result = GapRecordReconciler.reconcile(List.of(), state);

    assertThat(result.explained()).isEmpty();
    assertThat(result.unexplained()).isEmpty();
    assertThat(result.orphanState()).hasSize(1);
    // orphan state does NOT fail the gate
    assertThat(result.isClean()).isTrue();
  }

  // --- Case 5: reason categorisation — missing_hour doesn't explain ws_disconnect ---

  @Test
  void missingHourDoesNotExplainWsDisconnect() {
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));
    var state = List.of(stateRecord("depth", 50, 300, GapReason.MISSING_HOUR));

    var result = GapRecordReconciler.reconcile(file, state);

    assertThat(result.unexplained()).hasSize(1);
    assertThat(result.explained()).isEmpty();
    assertThat(result.orphanState()).hasSize(1);
    assertThat(result.isClean()).isFalse();
  }

  // --- Case 6: tuple mismatch — different stream ---

  @Test
  void tupleMismatchOnStreamProducesUnexplainedAndOrphan() {
    // File is depth; state is bookticker — same reason range but different stream
    var file = List.of(fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT));
    var state = List.of(stateRecord("bookticker", 100, 200, GapReason.RESTART_GAP));

    var result = GapRecordReconciler.reconcile(file, state);

    assertThat(result.unexplained()).hasSize(1);
    assertThat(result.explained()).isEmpty();
    assertThat(result.orphanState()).hasSize(1);
  }

  // --- Case 7: one state per stream explains many files (the headline 1-to-N test) ---

  @Test
  void oneStateRecordPerStreamExplainsCorrespondingFileRecord() {
    // Three file ws_disconnect records across three streams
    var files =
        List.of(
            fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT),
            fileRecord("bookticker", 100, 200, GapReason.WS_DISCONNECT),
            fileRecord("aggTrade", 100, 200, GapReason.WS_DISCONNECT));
    // Three state restart_gap records — one per stream, post-fan-out
    var states =
        List.of(
            stateRecord("depth", 50, 300, GapReason.RESTART_GAP),
            stateRecord("bookticker", 50, 300, GapReason.RESTART_GAP),
            stateRecord("aggTrade", 50, 300, GapReason.RESTART_GAP));

    var result = GapRecordReconciler.reconcile(files, states);

    assertThat(result.explained()).hasSize(3);
    assertThat(result.unexplained()).isEmpty();
    assertThat(result.orphanState()).isEmpty();
    assertThat(result.isClean()).isTrue();
  }

  // --- Additional: verify ExplainedRecord links correct file and state ---

  @Test
  void explainedRecordLinksFiileAndState() {
    var file = fileRecord("depth", 100, 200, GapReason.WS_DISCONNECT);
    var state = stateRecord("depth", 50, 300, GapReason.RESTART_GAP);

    var result = GapRecordReconciler.reconcile(List.of(file), List.of(state));

    assertThat(result.explained()).hasSize(1);
    var explained = result.explained().get(0);
    assertThat(explained.file()).isEqualTo(file);
    assertThat(explained.state()).isEqualTo(state);
  }

  // --- Additional: a single state record can explain multiple file records for the same stream ---

  @Test
  void singleStateCanExplainMultipleFileRecordsForSameStream() {
    // Two ws_disconnect gaps in different sub-windows within one restart window
    var file1 = fileRecord("depth", 100, 150, GapReason.WS_DISCONNECT);
    var file2 = fileRecord("depth", 160, 200, GapReason.WS_DISCONNECT);
    var state = stateRecord("depth", 50, 300, GapReason.RESTART_GAP);

    var result = GapRecordReconciler.reconcile(List.of(file1, file2), List.of(state));

    assertThat(result.explained()).hasSize(2);
    assertThat(result.unexplained()).isEmpty();
    // State was used — but it should still NOT appear in orphanState
    assertThat(result.orphanState()).isEmpty();
  }
}
