package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.consolidation.core.ConsolidateDay.ConsolidateResult;
import org.junit.jupiter.api.Test;

class ConsolidateResultTest {

  @Test
  void emptyHasZeroSourceFilesCount() {
    assertThat(ConsolidateResult.empty().sourceFilesCount()).isZero();
  }

  @Test
  void carriesSourceFilesCount() {
    ConsolidateResult r = new ConsolidateResult(true, 100L, 99L, 1L, 0, 7);
    assertThat(r.sourceFilesCount()).isEqualTo(7);
  }
}
