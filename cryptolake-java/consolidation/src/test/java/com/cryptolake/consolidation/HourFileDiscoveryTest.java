package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.consolidation.core.HourFileDiscovery;
import com.cryptolake.consolidation.core.HourFileGroup;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** new — hour file classification tests */
class HourFileDiscoveryTest {

  @TempDir Path tmpDir;

  @Test
  void classifiesBaseLateBackfill() throws IOException {
    // new
    Files.writeString(tmpDir.resolve("hour-5.jsonl.zst"), "");
    Files.writeString(tmpDir.resolve("hour-5.late-1.jsonl.zst"), "");
    Files.writeString(tmpDir.resolve("hour-5.backfill-1.jsonl.zst"), "");

    Map<Integer, HourFileGroup> groups = HourFileDiscovery.discover(tmpDir);
    assertThat(groups).containsKey(5);
    HourFileGroup g = groups.get(5);
    assertThat(g.base()).isNotNull();
    assertThat(g.late()).hasSize(1);
    assertThat(g.backfill()).hasSize(1);
  }

  @Test
  void lateAndBackfillSortedBySeq() throws IOException {
    // new — Tier 5 M15
    Files.writeString(tmpDir.resolve("hour-3.late-2.jsonl.zst"), "");
    Files.writeString(tmpDir.resolve("hour-3.late-1.jsonl.zst"), "");

    Map<Integer, HourFileGroup> groups = HourFileDiscovery.discover(tmpDir);
    HourFileGroup g = groups.get(3);
    assertThat(g.late().get(0).getFileName().toString()).contains("late-1");
    assertThat(g.late().get(1).getFileName().toString()).contains("late-2");
  }
}
