package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.consolidation.core.HourFileGroup;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class HourFileGroupTest {

  @Test
  void statusMissingWhenEmpty() {
    HourFileGroup g = new HourFileGroup(null, List.of(), List.of());
    assertThat(g.status()).isEqualTo("missing");
  }

  @Test
  void statusBackfilledWhenOnlyBackfillFiles() {
    HourFileGroup g =
        new HourFileGroup(null, List.of(), List.of(Path.of("hour-05.backfill-1.jsonl.zst")));
    assertThat(g.status()).isEqualTo("backfilled");
  }

  @Test
  void statusPresentWhenBaseFilePresent() {
    HourFileGroup g = new HourFileGroup(Path.of("hour-05.jsonl.zst"), List.of(), List.of());
    assertThat(g.status()).isEqualTo("present");
  }

  @Test
  void statusPresentWhenBaseAndLateAndBackfill() {
    HourFileGroup g =
        new HourFileGroup(
            Path.of("hour-05.jsonl.zst"),
            List.of(Path.of("hour-05.late-1.jsonl.zst")),
            List.of(Path.of("hour-05.backfill-1.jsonl.zst")));
    assertThat(g.status()).isEqualTo("present");
  }
}
