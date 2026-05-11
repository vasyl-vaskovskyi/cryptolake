package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReasons;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimeOnlyReasonsTest {

  @Test
  void unionPartitionsAllValidReasons() {
    Set<String> union = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    union.addAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    // Bidirectional equality: also catches typos that introduce a string not in VALID.
    assertThat(union).containsExactlyInAnyOrderElementsOf(GapReasons.VALID);
    Set<String> overlap = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    overlap.retainAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    assertThat(overlap).isEmpty();
  }
}
