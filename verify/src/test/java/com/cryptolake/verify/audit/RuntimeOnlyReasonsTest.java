package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReasons;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimeOnlyReasonsTest {

  @Test
  void unionCoversAllValidReasons() {
    Set<String> union = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    union.addAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    assertThat(union).containsAll(GapReasons.VALID);
    // and no overlap
    Set<String> overlap = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    overlap.retainAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    assertThat(overlap).isEmpty();
  }
}
