package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class GapRecordDiffTest {

  static GapRecord file(long s, long e, String reason) {
    return new GapRecord("file.envelope", "binance", "btcusdt", "depth", s, e, reason, "");
  }

  static GapRecord state(long s, long e, String reason) {
    return new GapRecord("pg.component_runtime", "binance", "btcusdt", "depth", s, e, reason, "");
  }

  @Test
  void matchOnExactTuple() {
    var d =
        GapRecordDiff.diff(
            List.of(file(100, 200, "collector_restart")),
            List.of(state(100, 200, "collector_restart")));
    assertThat(d.matched()).hasSize(1);
    assertThat(d.onlyInFiles()).isEmpty();
    assertThat(d.onlyInState()).isEmpty();
  }

  @Test
  void runtimeOnlyReasonExcludedFromDiff() {
    var d = GapRecordDiff.diff(List.of(file(100, 200, "ws_disconnect")), List.of());
    assertThat(d.matched()).isEmpty();
    assertThat(d.onlyInFiles()).isEmpty(); // excluded — does NOT fire alert
    assertThat(d.onlyInState()).isEmpty();
  }

  @Test
  void persistentClassMissingStateFires() {
    var d = GapRecordDiff.diff(List.of(file(100, 200, "collector_restart")), List.of());
    assertThat(d.onlyInFiles()).hasSize(1);
  }

  @Test
  void persistentClassOrphanStateFires() {
    var d = GapRecordDiff.diff(List.of(), List.of(state(100, 200, "collector_restart")));
    assertThat(d.onlyInState()).hasSize(1);
  }
}
